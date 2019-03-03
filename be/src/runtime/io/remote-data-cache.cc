// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/io/remote-data-cache.h"
#include "util/hdfs-util.h"
#include "util/thread-group.h"

#include <boost/algorithm/string.hpp>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// Configurations for remote read cache.
// Expected to be a list "<data-dir>:<data-size>,<data-dir>:<data-size>,..."
DEFINE_string(data_cache_config, "", "XXX");
DEFINE_int64(data_cache_block_size, 1 << 20, "XXX");
DEFINE_int32(data_cache_high_water_mark, 90, "XXX");
/*
  #define POSIX_FADV_NORMAL     0
  #define POSIX_FADV_RANDOM     1
  #define POSIX_FADV_SEQUENTIAL 2
  #define POSIX_FADV_WILLNEED   3
*/
DEFINE_int32(data_cache_prefetch_policy, 2, "XXX");

#define PAGE_SIZE (1L << 12)

RemoteDataCache::RemoteDataCache() {
  vector<string> all_cache_configs;
  split(all_cache_configs, FLAGS_remote_data_cache_config,
      boost::algorithm::is_any_of(","), boost::algorithm::token_compress_on);
  for (const string& config : all_cache_configs) {
    size_t offset = config.find(":");
    if (offset != std::string::npos) {
      const string& dir_path = FLAGS_data_cache_config.substr(0, offset);
      const string& capacity_str = FLAGS_data_cache_config.substr(offset+1);
      bool is_percent;
      int64_t capacity = ParseUtil::ParseMemSpec(capacity_str, &is_percent, 0);
      CHECK(!is_percent);
      if (capacity > 0) {
        VLOG_QUERY << "Adding partition " << dir_path " with capacity "
                   << PrettyPrinter::PrintBytes(capacity);
        partitions_.emplace_back(dir_path, capacity);
      }
    }
  }
  eviction_threads_.reset(new ThreadPool<int>("remote-data-cache", "evictor", partitions_.size(),
      partitions_.size() * 10, boost::bind(&RemoteDataCache::Evict, this, _1, _2)));
}

Status RemoteDataCache::Init() {
  for (const Partition& partition : partitions_) {
    RETURN_IF_ERROR(FileSystemUtil::VerifyIsDirectory(partition.dir_path()));
    int64_t available_bytes;
    RETURN_IF_ERROR(
        FileSystemUtil::GetSpaceAvailable(partition.dir_path(), &available_bytes));
    if (available_bytes < partition.capacity()) {
      const string& err = Substitute("Insufficient space for $0. Required $1. Only $2 "
          "available", partition.dir_path(), PrettyPrinter::PrintBytes(partition.capacity()),
          PrettyPrinter::PrintBytes(available_bytes));
      LOG(ERROR) << err;
      return Status(err);
    }
  }
  RETURN_IF_ERROR(eviction_threads_->Init());
  return Status::OK();
}

Status RemoteDataCache::Read(const HdfsFileReader& hdfs_file_reader,
    const std::string& filename, int64_t mtime, int64_t offset, int bytes_to_read,
    int* bytes_read, uint8_t* buffer) {
  /*
   * - Hash the filename and mtime to generate an int64_t
   * uint64_t FastHash64(const void* buf, int64_t len, uint64_t seed)
   * - Hash mod # partitions to find the partition
   * - Call read on that partition
   */
}

void RemoteDataCache::Evict(int thread_id, const EvictionWork& eviction_work) {
  DCHECK_LT(eviction_work.partition_idx, partitions_.size());
  partitions_[eviction_work.partition_idx]->Evict();
}

RemoteDataCache::Partition::Partition(string dir_path, int64_t capacity)
  : dir_path_(dir_path), capacity_(capacity) {
  if (FLAGS_data_cache_high_water_mark <= 0 || FLAGS_data_cache_high_water_mark > 100) {
    FLAGS_data_cache_high_water_mark = 90;
  }
  high_water_mark_ = capacity_ * FLAGS_data_cache_high_water_mark / 100;
}

FileCache* RemoteDataCache::Partition::Lookup(const string& filename, int64_t mtime,
    int64_t key_hash, bool create) {
  unique_lock<mutex> partition_lock(lock_);
  auto file_cache_iter = file_cache_map_.find(key_hash);
  if (file_cache_iter == file_cache_map_.end()) {
    int64_t size;
    if (!create || !GetSizeMtime(hdfs_fs, filename, mtime, &size).ok()) {
      return nullptr;
    }
    file_cache_map_.emplace(key_hash, FileCache(filename, mtime, size));
    file_cache_iter = file_cache_map_.find(filename);
  }
  FileCache* file_cache = &(file_cache_iter->second);
  if (UNLIKELY(file_cache->pathname().find(filename) == string::npos ||
          file_cache->mtime() != mtime)) {
    return nullptr;
  }
  return file_cache;
}


void RemoteDataCache::Partition::Read(const hdfsFS& hdfs_fs, const string& filename,
    int64_t mtime, int64_t key_hash, const FileSpan& read_range,
    vector<FileSpan>* miss_list) {
  FileCache* file_cache = Lookup(hdfs_fs, dir_path_, filename, mtime, key_hash, true);
  if (UNLIKELY(file_cache == nullptr)) {
    miss_list->emplace_back(read_range);
    return;
  }
  file_cache->Read(read_range, buffer, miss_list);
}

void RemoteDataCache::Partition::Write(const string& filename, int64_t mtime,
    int64_t key_hash, const FileSpan& read_range, const uint8_t* buffer) {
  FileCache* file_cache = Lookup(hdfs_fs, dir_path_, filename, mtime, key_hash, true);
  if (UNLIKELY(file_cache == nullptr)) return;
  int64_t bytes_written = file_cache->Write(read_range, buffer);

  unique_lock<SpinLock> partition_lock(lock_);
  usage_ += bytes_written;
  if (usage_ > high_water_mark_ && !eviction_requested_) {
    eviction_requested_ = true;    
  }
}

void RemoteDataCache::Partition::Evict() {
  std::unique_lock<SpinLock> partition_lock(lock_);
  FileCacheMap::iter iter = file_cache_map_.find(last_evicted_file_key_);
  int64_t bytes_to_free = usage_ - high_water_mark_;
  while (bytes_to_free > 0) {
    if (iter == file_cache_map_.end()) {
      iter = file_cache_map_.begin();
      last_evicted_file_key_ = iter->first;
    }
    while (bytes_to_free > 0 && iter != file_cache_map_.end()) {
      FileCache* file_cache = &(last_evicted_file_->second);
      partition_lock->unlock();
      int64_t bytes_freed = file_cache->Evict(bytes_to_free);
      partition_lock->lock();
      usage_ -= bytes_freed;
      bytes_to_free -= bytes_freed;
      ++iter;
    }
  }
  last_evicted_file_key_ = iter->first;
}

FileCache::FileCache(const string& dir_path, const string& filename,
   int64_t mtime, int64_t size)
  : pathname_(dir_path + "/" + filename), mtime_(mtime),
    accessed_bits_(BitUtil::RoundUp(size, FLAGS_data_cache_block_size)) { }

FileCache::~FileCache() {
  close(fd_);
  unlink(pathname_.c_str());
}

void FileCache::Create(const string& dir_path, const string& filename, int64_t mtime,
    int64_t size, std::unique_ptr<FileCache>* file_cache_ptr) {
  file_cache_ptr->reset();
  std::unique_ptr<FileCache> file_cache(new FileCache(dir_path, filename, mtime, size));

  // Create a file for caching file data.
  file_cache->fd = open(file_cache->pathname().c_str(), O_RDWR | O_TRUNC | O_CREAT, 0600);
  if (UNLIKELY(file_cache->fd < 0)) return;

  // Extend to create a sparse file.
  off_t offset = lseek(file_cache->fd, BitUtil::RoundUp(size, PAGE_SIZE), SEEK_SET);
  if (UNLIKELY(offset < 0)) return;

  *file_cache_ptr = move(file_cache);
}

void FileCache::Read(const FileRange& read_range, uint8_t* buffer,
    vector<FileSpan>* miss_list) {
  unique_lock<SpinLock> lock(lock_);
  posix_fadvise(fd_, read_range.offset, read_range.len, FLAGS_data_cache_prefetch_policy);
  const int64_t range_start = read_range.offset;
  const int64_t range_end = read_range.offset + read_range.len;
  int64_t cur = range_start;
  while (cur < range_end) {
    int64_t data_start = lseek(fd_, cur, SEEK_DATA);
    int64_t data_end = min<int64_t>(range_end, lseek(fd_, data_start, SEEK_HOLE));
    if (data_start < range_end) {
      int64_t bytes_to_read = data_end - data_start;
      lseek(fd, data_start, SEEK_SET);
      int64_t bytes_read = read(fd_, buffer + data_start - range_start, bytes_to_read);
      if (UNLIKELY(bytes_read < bytes_to_read)) {
        string err = string(strerror(errno));
        VLOG_QUERY << "Failed to read from " << pathname_ << " at offset " << data_start
                   << " for " << bytes_to_read << " bytes: " << err;
        miss_list->emplace_back({cur, range_end - cur});
        return;
      }
      for (int64_t i = data_start / FLAGS_data_cache_block_size;
           i < data_end / FLAGS_data_cache_block_size; ++i) {
        access_bits_->Set(i, true);
      }
    }
    if (data_start > cur) {
      miss_list->emplace_back({cur, min<int64_t>(range_end, data_start) - cur});
    }
    cur = data_end;
  }
}

int64_t FileCache::Write(const FileRange& read_range, uint8_t* buffer) {
  /*
   * Round up the starting offset to a page offset
   * Round down the ending offset to a page offset
   * data_start = lseek(offset, SEEK_DATA)
   * if (data_start > start_offset)
   * -> insert x amount of stuff
   * -> count those pages as added
   *
   * 
   */
  int64_t total_bytes_written = 0;
  unique_lock<SpinLock> lock(lock_);
  const int64_t range_start = BitUtil::RoundUp(read_range.offset, PAGE_SIZE);
  const int64_t range_end =
      BitUtil::RoundDown(read_range.offset + read_range.len, PAGE_SIZE);
  int64_t cur = range_start;
  while (cur < range_end) {
    int64_t data_start = lseek(fd_, cur, SEEK_DATA);
    int64_t data_end = lseek(fd_, data_start, SEEK_HOLE);
    DCHECK_EQ(data_start % PAGE_SIZE, 0);
    DCHECK_EQ(data_end % PAGE_SIZE, 0);
    if (data_start > cur) {
      int64_t bytes_to_write = min<int64_t>(range_end, data_start) - cur;
      lseek(fd, cur, SEEK_SET);
      DCHECK_EQ(cur % PAGE_SIZE, 0);
      int64_t bytes_written = write(fd_, buffer + cur - range_start, bytes_to_write);
      if (bytes_written < bytes_to_write) {
        string err = string(strerror(errno));
        VLOG_QUERY << "Failed to write to " << pathname_ << " at offset " << cur
                   << " for " << bytes_to_write << " bytes: " << err;
        return;
      }
      total_bytes_written += bytes_written;
    }
    cur = data_end;
  }
  return total_bytes_written;
}
