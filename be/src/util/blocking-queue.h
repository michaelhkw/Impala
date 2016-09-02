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


#ifndef IMPALA_UTIL_BLOCKING_QUEUE_H
#define IMPALA_UTIL_BLOCKING_QUEUE_H

#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/scoped_ptr.hpp>
#include <deque>
#include <unistd.h>

#include "common/compiler-util.h"
#include "util/stopwatch.h"
#include "util/time.h"

namespace impala {

/// Fixed capacity FIFO queue, where both BlockingGet() and BlockingPut() operations block
/// if the queue is empty or full, respectively.
///
/// FIFO is made up of a 'read_list_' that BlockingGet() consumes from and a 'write_list_'
/// that BlockingPut() enqueues into. They are protected by 'read_lock_' and 'write_lock_'
/// respectively. If both locks need to be held at the same time, 'read_lock_' must be
/// held before 'write_lock_'.
template <typename T>
class BlockingQueue {

 public:
  BlockingQueue(size_t max_elements)
    : shutdown_(false),
      max_elements_(max_elements),
      total_get_wait_time_(0),
      total_put_wait_time_(0) {
    DCHECK_GT(max_elements_, 0);
    read_list_.reset(new std::deque<T>());
    write_list_.reset(new std::deque<T>());
  }

  /// Get an element from the queue, waiting indefinitely for one to become available.
  /// Returns false if we were shut down prior to getting the element, and there
  /// are no more elements available.
  bool BlockingGet(T* out) {
    boost::unique_lock<boost::mutex> read_lock(read_lock_);

    if (UNLIKELY(read_list_->empty())) {
      MonotonicStopWatch timer;
      {
        // Block off writers while swapping 'read_list_' with 'write_list_'.
        boost::unique_lock<boost::mutex> write_lock(write_lock_);

        // Note that it's intentional to signal the writer while holding 'write_lock_' to
        // avoid the race in which the writer may be signalled between when it checks the
        // queue size and when it calls wait(). Calling notify_all() here may trigger
        // thundering herd and lead to contention (e.g. InitTuple() in scanner).
        put_cv_.notify_one();

        while (write_list_->empty()) {
          DCHECK(read_list_->empty());
          if (shutdown_) return false;
          // Sleep with read lock held to block off other readers which cannot
          // make progress anyway.
          timer.Start();
          get_cv_.wait(write_lock);
          timer.Stop();
        }
        DCHECK(!write_list_->empty());
        write_list_.swap(read_list_);
      }
      total_get_wait_time_ += timer.ElapsedTime();
    }

    DCHECK(read_list_.get() != write_list_.get());
    DCHECK(!read_list_->empty());
    *out = read_list_->front();
    read_list_->pop_front();
    return true;
  }

  /// Puts an element into the queue, waiting indefinitely until there is space.
  /// If the queue is shut down, returns false.
  bool BlockingPut(const T& val) {
    MonotonicStopWatch timer;
    boost::unique_lock<boost::mutex> write_lock(write_lock_);

    while (GetSize() >= max_elements_ && !shutdown_) {
      timer.Start();
      put_cv_.wait(write_lock);
      timer.Stop();
    }
    total_put_wait_time_ += timer.ElapsedTime();
    if (UNLIKELY(shutdown_)) return false;

    DCHECK_LT(write_list_->size(), max_elements_);
    write_list_->push_back(val);
    write_lock.unlock();
    get_cv_.notify_one();
    return true;
  }

  /// Puts an element into the queue, waiting until 'timeout_micros' elapses, if there is
  /// no space. If the queue is shut down, or if the timeout elapsed without being able to
  /// put the element, returns false.
  bool BlockingPutWithTimeout(const T& val, int64_t timeout_micros) {
    MonotonicStopWatch timer;
    boost::unique_lock<boost::mutex> write_lock(write_lock_);
    boost::system_time wtime = boost::get_system_time() +
        boost::posix_time::microseconds(timeout_micros);
    bool notified = true;
    while (GetSize() >= max_elements_ && !shutdown_ && notified) {
      timer.Start();
      // Wait until we're notified or until the timeout expires.
      notified = put_cv_.timed_wait(write_lock, wtime);
      timer.Stop();
    }
    total_put_wait_time_ += timer.ElapsedTime();
    // If the list is still full or if the the queue has been shutdown, return false.
    // NOTE: We don't check 'notified' here as it appears that pthread condition variables
    // have a weird behavior in which they can return ETIMEDOUT from timed_wait even if
    // another thread did in fact signal
    if (GetSize() >= max_elements_ || shutdown_) return false;
    DCHECK_LT(write_list_->size(), max_elements_);
    write_list_->push_back(val);
    write_lock.unlock();
    get_cv_.notify_one();
    return true;
  }

  /// Shut down the queue. Wakes up all threads waiting on BlockingGet or BlockingPut.
  void Shutdown() {
    {
      boost::lock_guard<boost::mutex> write_lock(write_lock_);
      shutdown_ = true;
    }

    get_cv_.notify_all();
    put_cv_.notify_all();
  }

  uint32_t GetSize() const {
    // Reading queue size is racy by nature so no lock is held. It's racy because
    // the queue's size could have changed once the lock is dropped.
    return read_list_->size() + write_list_->size();
  }

  uint64_t total_get_wait_time() const {
    // Same reasoning as GetSize() for not holding lock here.
    return total_get_wait_time_;
  }

  uint64_t total_put_wait_time() const {
    // Same reasoning as GetSize() for not holding lock here.
    return total_put_wait_time_;
  }

 private:
  /// True if the BlockingQueue is being shut down. Guarded by 'write_lock_'.
  bool shutdown_;

  /// Maximum number of elements in 'read_list_' + 'write_list_'.
  const int max_elements_;

  /// BlockingGet() callers wait on this.
  boost::condition_variable get_cv_;

  /// BlockingPut()/BlockingPutWithTimeout() callers wait on this.
  boost::condition_variable put_cv_;

  /// Guards against concurrent access to 'read_list_'.
  boost::mutex read_lock_;

  /// Guards against concurrent access to 'write_list_'.
  /// Please see comments at the beginning of the file for lock ordering.
  boost::mutex write_lock_;

  /// The queue of items to be consumed by BlockingGet(). Guarded by 'read_lock_'.
  boost::scoped_ptr<std::deque<T> > read_list_;

  /// The queue for items enqueued by BlockingPut(). Guarded by 'write_lock_'.
  boost::scoped_ptr<std::deque<T> > write_list_;

  /// Total amount of time threads blocked in BlockingGet(). Guarded by 'read_lock_'.
  uint64_t total_get_wait_time_;

  /// Total amount of time threads blocked in BlockingPut(). Guarded by 'write_lock_'.
  uint64_t total_put_wait_time_;
};

}

#endif
