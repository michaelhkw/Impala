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

#include <boost/bind.hpp>
#include <sys/sysinfo.h>

#include "runtime/io/data-cache.h"
#include "runtime/test-env.h"
#include "service/fe-support.h"
#include "testutil/gtest-util.h"
#include "util/filesystem-util.h"
#include "util/thread.h"

#include "common/names.h"

#define TEMP_BUFFER_SIZE (4096)
#define TEST_BUFFER_SIZE (8192)
#define NUM_TEST_DIRS    (16)
#define NUM_THREADS      (16)

DECLARE_bool(cache_force_single_shard);

/*
 * - Rotates files very quickly
 * - Check metrics size
 */

namespace impala {
namespace io {

class DataCacheTest : public testing::Test {
 public:
  const uint8_t* test_buffer() {
    return reinterpret_cast<const uint8_t*>(test_buffer_);
  }

  const std::vector<string>& data_cache_dirs() {
    return data_cache_dirs_;
  }

  // XXX
  void ThreadCb(const string& fname_prefix, DataCache* cache, int iterations,
      int max_offset, int* num_misses) {
    vector<int64_t> offsets;
    for (int i = 0; i < iterations; ++i) {
      int offset = rand() % max_offset;
      const string& fname = Substitute("$0f$1", fname_prefix, offset);
      int64_t mtime = 12345;
      cache->Store(fname, mtime, offset, test_buffer() + offset, TEMP_BUFFER_SIZE);
    }
    for (int offset : offsets) {
      const string& fname = Substitute("$0f$1", fname_prefix, offset);
      int64_t mtime = 12345;
      uint8_t buffer[TEMP_BUFFER_SIZE];
      memset(buffer, 0, TEMP_BUFFER_SIZE);
      int found = cache->Lookup(fname, mtime, offset, TEMP_BUFFER_SIZE, buffer);
      ASSERT_TRUE(found == 0 || found == TEMP_BUFFER_SIZE);
      if (found == TEMP_BUFFER_SIZE) {
        ASSERT_EQ(0, memcmp(buffer, test_buffer() + offset, TEMP_BUFFER_SIZE));
      } else {
        ++(*num_misses);
      }
    }
  }

 protected:
  DataCacheTest() {
    // Create a buffer of random characters.
    for (int i = 0; i < TEST_BUFFER_SIZE; ++i) {
      test_buffer_[i] = '!' + (rand() % 93);
    }
  }

  virtual void SetUp() {
    test_env_.reset(new TestEnv());
    ASSERT_OK(test_env_->Init());
    for (int i = 0; i < NUM_TEST_DIRS; ++i) {
      const string& path = Substitute("/tmp/data-cache-test.$0", i);
      ASSERT_OK(FileSystemUtil::RemoveAndCreateDirectory(path));
      data_cache_dirs_.push_back(path);
    }
  }

  virtual void TearDown() {
    ASSERT_OK(FileSystemUtil::RemovePaths(data_cache_dirs_));
    test_env_.reset();
  }

 private:
  std::unique_ptr<TestEnv> test_env_;
  char test_buffer_[TEST_BUFFER_SIZE];
  vector<string> data_cache_dirs_;
};

TEST_F(DataCacheTest, TestBasics) {
  // Force a single shard to avoid imbalance between shards in the Kudu LRU cache which
  // may lead to eviction. This allows the capacity of the cache is utilized to the limit.
  FLAGS_cache_force_single_shard = true;

  DataCache cache(data_cache_dirs()[0]+":4096KB");
  ASSERT_OK(cache.Init());

  uint8_t buffer[TEMP_BUFFER_SIZE];
  // Read and then insert a range of offsets. Expected all misses in the first iteration
  // and all hits in the second iteration.
  for (int i = 0; i < 2; ++i) {
    for (int offset = 0; offset < 1024; ++offset) {
      const string& fname = Substitute("f$0", offset);
      int64_t mtime = 12345;
      int expected_bytes = i * TEMP_BUFFER_SIZE;
      memset(buffer, 0, TEMP_BUFFER_SIZE);
      ASSERT_EQ(expected_bytes,
          cache.Lookup(fname, mtime, offset, TEMP_BUFFER_SIZE, buffer)) << offset;
      if (i == 0) {
        ASSERT_TRUE(cache.Store(fname, mtime, offset, test_buffer() + offset,
            TEMP_BUFFER_SIZE));
      } else {
        ASSERT_EQ(0, memcmp(test_buffer() + offset, buffer, TEMP_BUFFER_SIZE));
      }
    }
  }

  // Read a range of offsets which should miss in the cache.
  for (int offset = 1024; offset < TEST_BUFFER_SIZE; ++offset) {
    const string& fname = Substitute("f$0", offset);
    int64_t mtime = 12345;
    ASSERT_EQ(0, cache.Lookup(fname, mtime, offset, TEMP_BUFFER_SIZE, buffer));
  }

  // Read the same same range inserted previously and they should still all in the cache.
  for (int offset = 0; offset < 1024; ++offset) {
    string fname = Substitute("f$0", offset);
    int64_t mtime = 12345;
    memset(buffer, 0, TEMP_BUFFER_SIZE);
    ASSERT_EQ(TEMP_BUFFER_SIZE,
        cache.Lookup(fname, mtime, offset, TEMP_BUFFER_SIZE + 10, buffer));
    ASSERT_EQ(0, memcmp(test_buffer() + offset, buffer, TEMP_BUFFER_SIZE));
    ASSERT_EQ(TEMP_BUFFER_SIZE - 10,
        cache.Lookup(fname, mtime, offset, TEMP_BUFFER_SIZE - 10, buffer));
    ASSERT_EQ(0, memcmp(test_buffer() + offset, buffer, TEMP_BUFFER_SIZE - 10));
  }

  // Check that an insertion larger than the cache size will fail.
  ASSERT_FALSE(cache.Store("foobar", 1234, 5678, test_buffer(), TEMP_BUFFER_SIZE * 5000));
}

TEST_F(DataCacheTest, Eviction) {
  // Force a single shard to avoid imbalance between shards in the Kudu LRU cache which
  // may lead to eviction. This allows the capacity of the cache is utilized to the limit.
  FLAGS_cache_force_single_shard = true;
  DataCache cache(data_cache_dirs()[0]+":4096KB");
  ASSERT_OK(cache.Init());

  uint8_t buffer[TEMP_BUFFER_SIZE];

  // Read and then insert a range of offsets. Expected all misses in the first iteration
  // and all hits in the second iteration.
  for (int i = 0; i < 2; ++i) {
    for (int offset = 0; offset < 1025; ++offset) {
      const string& fname = Substitute("f$0", offset);
      int64_t mtime = 12345;
      ASSERT_EQ(0, cache.Lookup(fname, mtime, offset, TEMP_BUFFER_SIZE, buffer));
      ASSERT_TRUE(cache.Store(fname, mtime, offset, test_buffer() + offset,
          TEMP_BUFFER_SIZE));
    }
  }

  const int64_t large_buffer_size = TEST_BUFFER_SIZE * 512;
  unique_ptr<uint8_t> large_buffer(reinterpret_cast<uint8_t*>(malloc(large_buffer_size)));
  for (int64_t offset = 0; offset < large_buffer_size; offset += TEST_BUFFER_SIZE) {
    memcpy(large_buffer.get() + offset, test_buffer(), TEST_BUFFER_SIZE);
  }
  ASSERT_TRUE(cache.Store("foobar", 12345, 0, large_buffer.get(), large_buffer_size));
  // All should miss
  for (int offset = 0; offset < 1025; ++offset) {
    const string& fname = Substitute("f$0", offset);
    int64_t mtime = 12345;
    ASSERT_EQ(0, cache.Lookup(fname, mtime, offset, TEMP_BUFFER_SIZE, buffer));
  }
  unique_ptr<uint8_t> temp_buffer(reinterpret_cast<uint8_t*>(malloc(large_buffer_size)));
  ASSERT_EQ(large_buffer_size,
      cache.Lookup("foobar", 12345, 0, large_buffer_size, temp_buffer.get()));
  ASSERT_EQ(0, memcmp(temp_buffer.get(), large_buffer.get(), large_buffer_size));

  // XXX: check the actual file size
}

TEST_F(DataCacheTest, MultiThreaded) {
  // Force a single shard to avoid imbalance between shards in the Kudu LRU cache which
  // may lead to eviction. This allows the capacity of the cache is utilized to the limit.
  FLAGS_cache_force_single_shard = true;
  DataCache cache(data_cache_dirs()[1]+":4096KB");
  ASSERT_OK(cache.Init());

  // This first part tests multiple threads inserting and reading back a set of ranges
  // which will fit into the cache. No misses should be expected.
  vector<unique_ptr<Thread>> threads;
  int num_misses[NUM_THREADS];
  for (int i = 0; i < NUM_THREADS; ++i) {
    unique_ptr<Thread> thread;
    num_misses[i] = 0;
    ASSERT_OK(Thread::Create("data-cache-test", Substitute("thread-$0", i),
        boost::bind(&DataCacheTest::ThreadCb, this, "", &cache, 2048, 1024,
        &num_misses[i]), &thread));
    threads.emplace_back(std::move(thread));
  }
  for (int i = 0; i < NUM_THREADS; ++i) {
    threads[i]->Join();
    ASSERT_EQ(0, num_misses[i]);
  }

  // This second part tests multiple threads inserting and reading back a set of ranges
  // which may not completely fit into the cache. This exercises the eviction path.
  threads.resize(0);
  for (int i = 0; i < NUM_THREADS; ++i) {
    unique_ptr<Thread> thread;
    num_misses[i] = 0;
    ASSERT_OK(Thread::Create("data-cache-test", Substitute("thread-$0", i),
        boost::bind(&DataCacheTest::ThreadCb, this, Substitute("Thread-$0", i),
        &cache, 2048, 1024, &num_misses[i]), &thread));
    threads.emplace_back(std::move(thread));
  }
  for (int i = 0; i < NUM_THREADS; ++i) {
    threads[i]->Join();
  }
}

TEST_F(DataCacheTest, MultiPartitions) {
  string data_cache_config;
  for (const string& data_cache_dir : data_cache_dirs()) {
    if (!data_cache_config.empty()) {
      data_cache_config += ",";
    }
    data_cache_config += data_cache_dir + ":4096KB";
  }
  DataCache cache(data_cache_config);
  ASSERT_OK(cache.Init());

  // This test uses multiple threads to insert into and then read back from
  // a cache with multiple partitions.
  vector<unique_ptr<Thread>> threads;
  int num_misses[NUM_THREADS];
  for (int i = 0; i < NUM_THREADS; ++i) {
    unique_ptr<Thread> thread;
    num_misses[i] = 0;
    ASSERT_OK(Thread::Create("data-cache-test", Substitute("thread-$0", i),
        boost::bind(&DataCacheTest::ThreadCb, this, Substitute("Thread-$0", i),
        &cache, 2048, 1024, &num_misses[i]), &thread));
    threads.emplace_back(std::move(thread));
  }
  for (int i = 0; i < NUM_THREADS; ++i) {
    threads[i]->Join();
  }
}

TEST_F(DataCacheTest, LargeFootprint) {
  // Force a single shard to avoid imbalance between shards in the Kudu LRU cache which
  // may lead to eviction. This allows the capacity of the cache is utilized to the limit.
  FLAGS_cache_force_single_shard = true;
  struct sysinfo info;
  ASSERT_EQ(0, sysinfo(&info));
  ASSERT_GT(info.totalram, 0);
  DataCache cache(data_cache_dirs()[0]+":"+std::to_string(info.totalram));
  ASSERT_OK(cache.Init());

  const int64_t footprint = info.totalram / 8;
  for (int64_t i = 0; i < footprint / TEST_BUFFER_SIZE; ++i) {
    const string& fname = Substitute("f$0", i);
    ASSERT_TRUE(cache.Store(fname, 12345, 0, test_buffer(), TEST_BUFFER_SIZE));
  }
  uint8_t buffer[TEST_BUFFER_SIZE];
  for (int64_t i = 0; i < footprint / TEST_BUFFER_SIZE; ++i) {
    const string& fname = Substitute("f$0", i);
    memset(buffer, 0, TEST_BUFFER_SIZE);
    ASSERT_EQ(TEST_BUFFER_SIZE, cache.Lookup(fname, 12345, 0, TEST_BUFFER_SIZE, buffer));
    ASSERT_EQ(0, memcmp(buffer, test_buffer(), TEST_BUFFER_SIZE));
  }
}

} // namespace io
} // namespace impala

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  int rand_seed = time(NULL);
  LOG(INFO) << "rand_seed: " << rand_seed;
  srand(rand_seed);
  return RUN_ALL_TESTS();
}
