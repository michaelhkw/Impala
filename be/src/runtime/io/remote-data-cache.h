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

#pragma once

#include <condition_variable>
#include <mutex>

#include "common/status.h"
#include "util/bitmap.h"
#include "util/lru-cache.h"

namespace impala {

class ThreadPool;

namespace io {

struct FileSpan {
  int64_t offset = 0;
  int64_t len = 0;
};

struct EvictionWork {
  int partition_idx;
};

class FileCache {
 public:
  static FileCache* Create(const std::string& filename, int64_t mtime, int64_t size);

  void Read(const FileRange& read_range, uint8_t* buffer, vector<FileSpan>* miss_list);

  int64_t Write(const FileRange& read_range, uint8_t* buffer);

  const std::string& pathname() const { return pathname_; }

  int64_t mtime() const { return mtime_; }

  void Evict();

 private:

  FileCache(const std::string& dir_path, const std::string& filename, int64_t mtime,
      int64_t size);

  ~FileCache();

  /// XXX
  const std::string pathname_;

  /// XXX
  const int64_t mtime;

  SpinLock lock_;

  int fd_;

  Bitmap access_bits_;

  int64_t last_eviction_offset_ = 0;
};

class RemoteDataCache {
 public:

  /// Need to parse a list of paths and their usage quota
  RemoteDataCache();

  /// Need to check the validity of the paths and space are available
  Status Init();

  /// Try reading from cache.
  void Read(const hdfsFS& hdfs_fs, const std::string& filename, int64_t mtime,
      const FileSpan& read_range, uint8_t* out_buffer, std::vector<FileSpan>* miss_list);

  /// XXX
  Status Write(const std::string& filename, int64_t mtime, const FileSpan& read_range,
      const uint8_t* out_buffer);

  class Partition {
   public:
    ///
    Partition(std::string dir_path, int64_t capacity);

    /// XXX
    void Read(const hdfsFS& hdfs_fs, const std::string& filename,
        int64_t mtime, int64_t key_hash, FileSpan& read_range,
        std::vector<FileSpan>* miss_list);

    /// XXX
    void Write(const std::string& filename, int64_t mtime, int64_t key_hash,
        const FileSpan& read_range, const uint8_t* out_buffer);

    /// XXX
    void Evict(int64_t);

    /// XXX
    const std::string& dir_path() const { return dir_path_; }

    /// XXX
    const int64_t capacity() const { return capacity_; }

   private:
    /// XXX
    const std::string dir_path_;

    /// XXX in blocks
    const int64_t capacity_;

    ///
    int64_t high_water_mark_;

    /// XXX
    std::mutex lock_;

    /// XXX
    int64_t usage_ = 0;

    /// XXX
    bool eviction_requested_ = false;

    /// Use a map to guranatee the eviction clock pointer is consistent
    typedef std::map<int64_t, FileCache> FileCacheMap;
    FileCacheMap file_cache_map_;

    /// XXX
    int64_t last_evicted_file_key_ = 0;

    /// XXX
    FileCache* Lookup(const std::string& filename, int64_t mtime,
        int64_t key_hash, bool create);

    /// XXX
    void Evict();
  };

 private:
  std::vector<Partition> partitions_;

  std::unique_ptr<ThreadGroup<EvictionWork>> eviction_threads_;

  /// XXX
  void Evict(int thread_id, const EvictionWork& partition);
};

} //namespace io
} //namespace impala
