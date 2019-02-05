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

#ifndef IMPALA_EXEC_PARQUET_FOOTER_CACHE_H
#define IMPALA_EXEC_PARQUET_FOOTER_CACHE_H

#include <condition_variable>
#include <mutex>

#include "exec/parquet/parquet-common.h"
#include "runtime/io/request-ranges.h"
#include "util/spinlock.h"

namespace impala {

class ParquetFooterCache {
 public:

  /// XXX
  ParquetFooterCache(int64_t capacity, int num_partitions)
    : cache_partitions_(num_partitions) {
    const int64_t capacity_per_partition = capacity / num_partitions;
    DCHECK_GE(capacity_per_partition, sizeof (parquet::FileMetaData));
    for (int i = 0; i < num_partitions; ++i) {
      cache_partitions_[i].available_capacity_ = capacity_per_partition;
    }
  }

  /// XXX
  const parquet::FileMetaData* Probe(const std::string& fname);

  /// XXX
  void Insert(const std::string& fname, const parquet::FileMetaData& footer);

  /// XXX
  void Release(const std::string& fname);

 private:

  typedef std::list<std::string> LruListType;

  class Entry {
   public:

    Entry(const parquet::FileMetaData& footer, LruListType::iterator lru_entry)
      : ref_cnt_(1), size_(sizeof(footer)), footer_(footer),  lru_entry_(lru_entry) {}

    /// XXX
    int64_t size() const { return size_; }

    /// XXX
    const parquet::FileMetaData& footer() const {
      DCHECK_GT(ref_cnt_, 0);
      DCHECK_GT(size_, 0);
      return footer_;
    }

    ///
    void Acquire(LruListType* lru_list);

    ///
    void Release(const std::string& fnane, LruListType* lru_list);

   private:
    /// XXX
    int64_t ref_cnt_;

    /// XXX
    int64_t size_;

    /// File metadata thrift object
    parquet::FileMetaData footer_;

    /// Iterator to this element's location in the LRU list. This only points to a
    /// valid location when in_use is true. For error-checking, this is set to
    /// lru_list.end() when in_use is false.
    LruListType::iterator lru_entry_;
  };

  class Partition {
   public:
    const parquet::FileMetaData* Probe(const std::string& fname);

    void Insert(const std::string& fname, const parquet::FileMetaData& footer);

    void Release(const std::string& fname);

   private:
    friend class ParquetFooterCache;

    std::mutex lock_;

    int64_t available_capacity_;

    // A map of filename to a binary blob
    std::unordered_map<std::string, Entry> map_;

    // XXX
    LruListType lru_list_;
  };

  /// XXX
  std::vector<Partition> cache_partitions_;
};

} // namespace impala

#endif
