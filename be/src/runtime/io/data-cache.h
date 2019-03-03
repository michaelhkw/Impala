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

#ifndef IMPALA_RUNTIME_IO_DATA_CACHE_H
#define IMPALA_RUNTIME_IO_DATA_CACHE_H

#include <string>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>

#include "common/status.h"
#include "util/spinlock.h"
#include "kudu/util/cache.h"
#include "kudu/util/faststring.h"
#include "kudu/util/slice.h"

namespace kudu {
class Cache;
} // kudu

/// DataCache is an implemenation of a data cache backed by the local storage.
/// It implicitly relies on the OS page cache management to shuffle data between
/// memory and the storage device.
///
/// A cache key is a tuple of (file's name, file's modification time, file offset).
/// If the key is present in the cache, it maps to an entry which is a tuple of
/// (backing file, offset in the backing file, length of the cached data, CRC checksum).
///
/// A data cache is divided into one or more partitions based on the configuration
/// string passed to the ctor. The configuration string specifies a list of directories
/// and their corresponding storage consumption limits.
///
/// For each partition, there is a meta-data cache (backed by an LRU cache implementation
/// in Kudu).
///
/// There are one or more sparse
/// files in each partition to back the cached data. Each New cache entries are written to
/// one of the sparse files by 
///
/// Each partition creates one or more sparse files in
/// the directory specified in the configuration string. There is also a quota on storage
/// space consumed for each partition.
/// specified by t has a quota on the storage space consumed.
/// space 

namespace impala {
namespace io {

struct CacheFile {
  std::string path;
  int fd = -1;
  bool allow_append = false;
  int64_t current_offset = 0;
};

struct CacheEntry {
  CacheFile* file = nullptr;
  int64_t offset = 0;
  int64_t len = 0;
  uint64_t crc_hash = 0;
};

class DataCache{
 public:

  /// An instance of DataCache is created based on the configuration string passed
  /// in the ctor. The configuration string is a list of tuple <directory>:<quota>
  /// delimited by comma. <directory> specifies the
  DataCache(const std::string& config);

  Status Init();

  void Close();

  int64_t Lookup(const std:: string& filename, int64_t mtime, int64_t offset,
      int64_t bytes_to_read, uint8_t* buffer);

  bool Store(const std::string& filename, int64_t mtime, int64_t offset,
      const uint8_t* buffer, int64_t store_len);

 private:

  class Partition : public kudu::Cache::EvictionCallback {
   public:
    /// store the path of the directory
    Partition(const std::string& path, int64_t capacity);

    ~Partition();

    Status Init();

    void Close();

    int64_t Lookup(const kudu::faststring& key, const std::string& filename, int64_t mtime,
        int64_t offset, int64_t bytes_to_read, uint8_t* buffer);

    /// XXX
    bool Store(const kudu::faststring& key, const uint8_t* buffer, int64_t store_len);

    /// XXX
    void EvictedEntry(kudu::Slice key, kudu::Slice value) override;

  private:

    const std::string path_;

    const int64_t capacity_;

    /// Protects cached_items' contents
    SpinLock lock_;

    /// XXX
    bool closed_ = false;

    /// XXX
    std::unordered_set<std::string> pending_insert_set_;

    /// XXX
    std::vector<CacheFile> cache_files_;

    /// XXX
    std::unique_ptr<kudu::Cache> meta_cache_;

    /// XXX
    Status CreateCacheFile();

    /// XXX
    static uint64_t Checksum(const CacheEntry* entry, const uint8_t* buffer);
  };

  /// XXX
  std::vector<std::unique_ptr<Partition>> partitions_;

  /// XXX
  static uint64_t Hash(const kudu::faststring& key);

  /// XXX
  static void ConstructCacheKey(const string& filename, int64_t mtime, int64_t offset,
      std::unique_ptr<kudu::faststring>* key);
};

} // namespace io
} // namespace impala

#endif // IMPALA_RUNTIME_IO_DATA_CACHE_H
