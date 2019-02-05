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

#include "exec/parquet/parquet-footer-cache.h"

#include "common/names.h"
#include "util/hash-util.h"

using parquet::FileMetaData;

namespace impala {

void ParquetFooterCache::Entry::Acquire(LruListType* lru_list) {
  if (ref_cnt_ == 0) {
    DCHECK(lru_entry_ != lru_list->end());
    lru_list->erase(lru_entry_);
    lru_entry_ = lru_list->end();
  }
  ++ref_cnt_;
}

void ParquetFooterCache::Entry::Release(const string& fname, LruListType* lru_list) {
  DCHECK_GT(ref_cnt_, 0);
  --ref_cnt_;
  if (ref_cnt_ == 0) {
    lru_list->emplace_back(fname);
    lru_entry_ = --lru_list->end();
    DCHECK_EQ(*lru_entry_, fname);
  }
}

const parquet::FileMetaData* ParquetFooterCache::Partition::Probe(const string& fname) {
  std::unique_lock<std::mutex> lock(lock_);
  auto iter = map_.find(fname);
  if (iter != map_.end()) {
    iter->second.Acquire(&lru_list_);
    return &(iter->second.footer());
  }
  return nullptr;
}

/// XXX
const parquet::FileMetaData* ParquetFooterCache::Probe(const string& fname) {
  int index = HashUtil::Hash(fname.data(), fname.size(), 0) % cache_partitions_.size();
  Partition& partition = cache_partitions_[index];
  return partition.Probe(fname);
}

void ParquetFooterCache::Partition::Release(const string& fname) {
  std::unique_lock<std::mutex> lock(lock_);
  auto iter = map_.find(fname);
  DCHECK(iter != map_.end());
  iter->second.Release(fname, &lru_list_);
}

void ParquetFooterCache::Release(const string& fname) {
  int index = HashUtil::Hash(fname.data(), fname.size(), 0) % cache_partitions_.size();
  Partition& partition = cache_partitions_[index];
  partition.Release(fname);
}

void ParquetFooterCache::Partition::Insert(const string& fname, const FileMetaData& footer) {
  std::unique_lock<std::mutex> lock(lock_);
  auto iter = map_.find(fname);
  if (iter == map_.end()) {
    const int64_t footer_size = sizeof(footer);
    while (available_capacity_ < footer_size && !lru_list_.empty()) {
      auto evicted_iter = map_.find(lru_list_.front());
      DCHECK(evicted_iter != map_.end());
      available_capacity_ += iter->second.size();
      map_.erase(evicted_iter);
      lru_list_.pop_front();
    }
    if (available_capacity_ >= footer_size) {
      map_.emplace(fname, Entry(footer, lru_list_.end()));
      available_capacity_ -= footer_size;
    }
  }
}

void ParquetFooterCache::Insert(const string& fname, const FileMetaData& footer) {
  int index = HashUtil::Hash(fname.data(), fname.size(), 0) % cache_partitions_.size();
  Partition& partition = cache_partitions_[index];
  partition.Insert(fname, footer);
}

}
