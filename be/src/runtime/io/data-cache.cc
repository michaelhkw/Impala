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

#include "runtime/io/data-cache.h"

#include <boost/algorithm/string.hpp>
#include <errno.h>
#include <fcntl.h>
#include <mutex>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "kudu/util/cache.h"
#include "util/bit-util.h"
#include "util/filesystem-util.h"
#include "util/hash-util.h"
#include "util/impalad-metrics.h"
#include "util/parse-util.h"
#include "util/pretty-printer.h"
#include "util/uid-util.h"

#ifndef FALLOC_FL_PUNCH_HOLE
#include <linux/falloc.h>
#endif

#include "common/names.h"

// TODO: Evaluate the option of exposing the cache via mmap() and pinning similar to HDFS
// caching. This has the advantage of not needing to copy out the data. On the other hand,
// pinning may make it more complicated for eviction if there are too many pinned entries.

using kudu::Slice;
using kudu::faststring;
using namespace impala;
using namespace impala::io;

DEFINE_int64(data_cache_file_max_size, 4L << 40,
    "(Advanced) The maximum size which a cache file can grow to before data stops being "
    "appended to it.");
DEFINE_bool(data_cache_checksum, true,
    "(Advanced) Enable checksumming for the cached buffer.");
DEFINE_bool(data_cache_pseudo_read, false, "XXX");

static const int64_t PAGE_SIZE = 1L << 12;

DataCache::Partition::Partition(const string& path, int64_t capacity)
  : path_(path), capacity_(max<int64_t>(capacity, PAGE_SIZE)),
    meta_cache_(NewLRUCache(kudu::DRAM_CACHE, capacity_, path_)) {
}

DataCache::Partition::~Partition() {
  if (!closed_) Close();
}

Status DataCache::Partition::CreateCacheFile() {
  const string& path = path_ + "/" + PrintId(GenerateUUID());
  int64_t fd = open(path.c_str(), O_CREAT | O_TRUNC | O_RDWR, S_IRUSR | S_IWUSR);
  if (UNLIKELY(fd < 0)) {
    return Status(Substitute("Failed to create file $0: $1", path, string(strerror(errno))));
  }
  // Unlink the file so the disk space is recycled once the process exits.
  unlink(path.c_str());
  CacheFile cache_file;
  cache_file.path = path;
  cache_file.fd = fd;
  cache_file.allow_append = true;
  cache_file.current_offset = 0;
  cache_files_.emplace_back(std::move(cache_file));
  LOG(INFO) << "Created cache file " << path;
  return Status::OK();
}

Status DataCache::Partition::Init() {
  RETURN_IF_ERROR(FileSystemUtil::VerifyIsDirectory(path_));
  uint64_t available_bytes;
  RETURN_IF_ERROR(FileSystemUtil::GetSpaceAvailable(path_, &available_bytes));
  if (available_bytes < capacity_) {
    const string& err = Substitute("Insufficient space for $0. Required $1. Only $2 is "
        "available", path_, PrettyPrinter::PrintBytes(capacity_),
        PrettyPrinter::PrintBytes(available_bytes));
    LOG(ERROR) << err;
    return Status(err);
  }
  RETURN_IF_ERROR(CreateCacheFile());
  return Status::OK();
}

void DataCache::Partition::Close() {
  if (closed_) return;
  closed_ = true;
  for (const CacheFile& cache_file : cache_files_) close(cache_file.fd);
}

int64_t DataCache::Partition::Lookup(const faststring& key, const string& filename,
    int64_t mtime, int64_t offset, int64_t bytes_to_read, uint8_t* buffer) {
  DCHECK(!closed_);
  kudu::Cache::Handle* handle = meta_cache_->Lookup(key, kudu::Cache::EXPECT_IN_CACHE);
  if (handle == nullptr) return 0;

  const Slice& value_slice = meta_cache_->Value(handle);
  DCHECK_EQ(value_slice.size(), sizeof(CacheEntry));
  const CacheEntry* entry = reinterpret_cast<const CacheEntry*>(value_slice.data());
  const CacheFile* cache_file = entry->file;
  bytes_to_read = min(entry->len, bytes_to_read);

  posix_fadvise(cache_file->fd, entry->offset, bytes_to_read, POSIX_FADV_SEQUENTIAL);
  int64_t bytes_read = pread(cache_file->fd, buffer, bytes_to_read, entry->offset);
  if (UNLIKELY(bytes_read != bytes_to_read)) {
    LOG(ERROR) << Substitute("Failed to read from $0 at offset $1 for $2: $3",
        cache_file->path, entry->offset, PrettyPrinter::PrintBytes(bytes_to_read),
        bytes_read < 0 ? string(strerror(errno)) :
        Substitute("only read $0 bytes", PrettyPrinter::PrintBytes(bytes_read)));
    bytes_read = 0;
  } else if (FLAGS_data_cache_checksum) {
    uint64_t crc_hash = Checksum(entry, buffer);
    VLOG(2) << Substitute("Reading file $0 offset $1 len $2 checksum $3 "
         "buffer checksum: $4", entry->file->path, entry->offset, entry->len,
         entry->crc_hash, crc_hash);
    if (bytes_read == entry->len) {
      if (UNLIKELY(crc_hash != entry->crc_hash)) {
        LOG(ERROR) << Substitute("Read checksum mismatch for file $0 mtime $1 offset $2."
            " Expected $3, Got $4.", filename, mtime, offset, entry->crc_hash, crc_hash);
        bytes_read = 0;
      }
    } else {
      VLOG_QUERY << Substitute("XXX: filename: $0 mtime: $1 offset: $2 bytes_to_read: $3"
          "entry len: $4 bytes_read: $5", filename, mtime, offset, bytes_to_read,
          entry->len, bytes_read);
    }
  }
  // Don't release the cache handle until read() completes.
  meta_cache_->Release(handle);
  return bytes_read;
}

bool DataCache::Partition::Store(const faststring& key, const uint8_t* buffer,
    int64_t store_len) {
  DCHECK(!closed_);
  const int64_t charge_len = BitUtil::RoundUp(store_len, PAGE_SIZE);
  if (charge_len > capacity_) return false;

  // Check for existing entry. If it exists already, bail out early.
  kudu::Cache::Handle* handle = meta_cache_->Lookup(key, kudu::Cache::EXPECT_IN_CACHE);
  if (handle != nullptr) {
    if (FLAGS_data_cache_checksum) {
      const Slice& value_slice = meta_cache_->Value(handle);
      DCHECK_EQ(value_slice.size(), sizeof(CacheEntry));
      const CacheEntry* entry = reinterpret_cast<const CacheEntry*>(value_slice.data());
      uint64_t crc_hash = Checksum(entry, buffer);
      VLOG(2) << Substitute("Storing duplicated file $0 offset $1 len $2 checksum $3 "
           "buffer checksum: $4", entry->file->path, entry->offset, entry->len,
           entry->crc_hash, crc_hash);
      if (UNLIKELY(crc_hash != entry->crc_hash)) {
        if (entry->len == store_len) {
          LOG(ERROR) << Substitute("Write checksum mismatch for file $0 offset $1 "
              "entry len: $2 store_len: $3 Expected $4, Got $5.", entry->file->path,
              entry->offset, entry->len, store_len, entry->crc_hash, crc_hash);
        } else {
          LOG(INFO) << Substitute("Write Checksum mismatch for file $0 offset $1 "
              "entry len: $2 store_len: $3 Expected $4, Got $5.", entry->file->path,
              entry->offset, entry->len, store_len, entry->crc_hash, crc_hash);
        }
      }
    }
    // TODO: handle cases in which 'store_len' is longer than existing entry;
    meta_cache_->Release(handle);
    return false;
  }

  CacheFile* cache_file;
  int64_t insertion_offset;
  {
    std::unique_lock<SpinLock> partition_lock(lock_);

    // If there is an insert for the same key in progress, bail out early.
    if (pending_insert_set_.find(key.ToString()) != pending_insert_set_.end()) {
      return false;
    }

    DCHECK(!cache_files_.empty());
    cache_file = &(cache_files_.back());
    if (UNLIKELY(!cache_file->allow_append)) {
      if (!CreateCacheFile().ok()) return false;
      cache_file = &(cache_files_.back());
    }

    // At this point, we are committed to inserting 'key' into the cache.
    pending_insert_set_.insert(key.ToString());
    insertion_offset = cache_file->current_offset;
    DCHECK_EQ(insertion_offset % PAGE_SIZE, 0);
    cache_file->current_offset += charge_len;
    if (cache_file->current_offset > FLAGS_data_cache_file_max_size) {
      cache_file->allow_append = false;
    }
  }

  // Allocate a cache handle
  CacheEntry* entry = nullptr;
  bool inserted = false;
  ssize_t bytes_written = 0;
  kudu::Cache::PendingHandle* pending_handle =
      meta_cache_->Allocate(key, sizeof(CacheEntry), charge_len);
  if (UNLIKELY(pending_handle == nullptr)) {
    goto done;
  }

  // Update the cache entry.  
  entry = reinterpret_cast<CacheEntry*>(meta_cache_->MutableValue(pending_handle));
  entry->file = cache_file;
  entry->offset = insertion_offset;
  entry->len = store_len;
  if (FLAGS_data_cache_checksum) {
    entry->crc_hash = Checksum(entry, buffer);
  } else {
    entry->crc_hash = 0;
  }
  VLOG(2) << Substitute("Storing file $0 offset $1 len $2 checksum $3 ",
       entry->file->path, entry->offset, entry->len, entry->crc_hash);
  bytes_written = pwrite(cache_file->fd, buffer, store_len, insertion_offset);
  if (bytes_written < store_len) {
    LOG(ERROR) << Substitute("Failed to write to $0 at offset $1 for $2: $3",
        cache_file->path, cache_file->current_offset, store_len,
        string(strerror(errno)));
    meta_cache_->Free(pending_handle);
    goto done;
  }

  // Add the new entry.
  handle = meta_cache_->Insert(pending_handle, this);
  meta_cache_->Release(handle);
  inserted = true;
  ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_TOTAL_BYTES->Increment(charge_len);

done:
  std::unique_lock<SpinLock> partition_lock(lock_);
  pending_insert_set_.erase(key.ToString());
  return inserted;
}

void DataCache::Partition::EvictedEntry(Slice key, Slice value) {
  if (closed_) return;
  DCHECK_EQ(value.size(), sizeof(CacheEntry));
  const CacheEntry* entry = reinterpret_cast<const CacheEntry*>(value.data());
  int64_t eviction_len =
      BitUtil::RoundUp(entry->offset + entry->len, PAGE_SIZE) - entry->offset;
  int res = fallocate(entry->file->fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
      entry->offset, eviction_len);
  LOG(INFO) << Substitute("Punching holes in $0 at offset $1 for $2: $3",
      entry->file->path, entry->offset, PrettyPrinter::PrintBytes(eviction_len),
      string(strerror(errno)));
  if (res < 0) {
    LOG(ERROR) << Substitute("Failed to punch holes in $0 at offset $1 for $2: $3",
        entry->file->path, entry->offset, PrettyPrinter::PrintBytes(eviction_len),
        string(strerror(errno)));
  }
  ImpaladMetrics::IO_MGR_REMOTE_DATA_CACHE_TOTAL_BYTES->Increment(-eviction_len);
}

uint64_t DataCache::Partition::Checksum(const CacheEntry* entry, const uint8_t* buffer) {
  /*
  const string& path = entry->file->path;
  uint64_t h = HashUtil::FastHash64(path.data(), path.size(), 0xcafe);
  h = HashUtil::FastHash64(&entry->offset, sizeof(entry->offset), h);
  h = HashUtil::FastHash64(&entry->len, sizeof(entry->len), h);
  */
  return HashUtil::FastHash64(buffer, entry->len, 0xcafebeef);
}

DataCache::DataCache(const string& config_str) {
  vector<string> all_cache_configs;
  split(all_cache_configs, config_str,
      boost::algorithm::is_any_of(","), boost::algorithm::token_compress_on);
  for (const string& cache_config : all_cache_configs) {
    vector<string> dir_capacity;
    split(dir_capacity, cache_config,
        boost::algorithm::is_any_of(":"), boost::algorithm::token_compress_on);
    if (dir_capacity.size() == 2) {
      const string& dir_path = dir_capacity[0];
      const string& capacity_str = dir_capacity[1];
      bool is_percent;
      int64_t capacity = ParseUtil::ParseMemSpec(capacity_str, &is_percent, 0);
      CHECK(!is_percent);
      if (capacity > 0) {
        LOG(INFO) << "Adding partition " << dir_path << " with capacity "
                  << capacity; //PrettyPrinter::PrintBytes(capacity);
        std::unique_ptr<Partition> partition = make_unique<Partition>(dir_path, capacity);
        partitions_.emplace_back(move(partition));
      }
    }
  }
}

Status DataCache::Init() {
  for (auto& partition : partitions_) {
    RETURN_IF_ERROR(partition->Init());
  }
  return Status::OK();
}

void DataCache::Close() {
  for (auto& partition : partitions_) {
    partition->Close();
  }
}

uint64_t DataCache::Hash(const faststring& key) {
  return HashUtil::FastHash64(key.data(), key.size(), 0);
}

void DataCache::ConstructCacheKey(const string& filename, int64_t mtime,
    int64_t offset, unique_ptr<faststring>* key_ptr) {
  key_ptr->reset(new faststring(filename.size() + sizeof(mtime) + sizeof(offset)));
  (*key_ptr)->append(filename);
  (*key_ptr)->append(&mtime, sizeof(mtime));
  (*key_ptr)->append(&offset, sizeof(offset));
}

int64_t DataCache::Lookup(const string& filename, int64_t mtime, int64_t offset,
    int64_t bytes_to_read, uint8_t* buffer) {
  if (partitions_.empty()) return 0;
  unique_ptr<faststring> key;
  ConstructCacheKey(filename, mtime, offset, &key);
  int idx = Hash(*key.get()) % partitions_.size();
  int64_t bytes_read = partitions_[idx]->Lookup(*key.get(), filename, mtime, offset,
      bytes_to_read, buffer);
  stringstream ss;
  ss << std::hex << reinterpret_cast<int64_t>(buffer);
  VLOG(2) << Substitute("Looking up $0 mtime: $1 offset: $2 bytes_to_read: $3 "
       "bytes_read: $4 buffer: $5", filename, mtime, offset, bytes_to_read,
       bytes_read, ss.str());
  if (FLAGS_data_cache_pseudo_read) bytes_read = 0;
  return bytes_read;
}

bool DataCache::Store(const string& filename, int64_t mtime, int64_t offset,
    const uint8_t* buffer, int64_t store_len) {
  if (partitions_.empty()) return false;
  unique_ptr<faststring> key;
  ConstructCacheKey(filename, mtime, offset, &key);
  int idx = Hash(*key.get()) % partitions_.size();
  bool stored = partitions_[idx]->Store(*key.get(), buffer, store_len);

  stringstream ss;
  ss << std::hex << reinterpret_cast<int64_t>(buffer);
  VLOG(2) << Substitute("Storing $0 mtime: $1 offset: $2 bytes_to_read: $3 buffer: $4 stored: $5",
       filename, mtime, offset, store_len, ss.str(), stored);
  return stored;
}
