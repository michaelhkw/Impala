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
#include "util/condition-variable.h"
#include "util/stopwatch.h"
#include "util/time.h"

namespace impala {

/// Fixed capacity FIFO queue, where both BlockingGet() and BlockingPut() operations block
/// if the queue is empty or full, respectively.
///
/// FIFO is made up of a 'get_list_' that BlockingGet() consumes from and a 'put_list_'
/// that BlockingPut() enqueues into. They are protected by 'get_lock_' and 'put_lock_'
/// respectively. If both locks need to be held at the same time, 'get_lock_' must be
/// held before 'put_lock_'. When the 'get_list_' is empty, the caller of BlockingGet()
/// will atomically swap the 'put_list_' with 'get_list_'. The swapping happens with both
/// the 'get_lock_' and 'put_lock_' held.
template <typename T>
class CACHE_ALIGNED BlockingQueue {
 public:
  BlockingQueue(size_t max_elements)
    : shutdown_(false),
      max_elements_(max_elements),
      total_put_wait_time_(0),
      total_get_wait_time_(0) {
    DCHECK_GT(max_elements_, 0);
    // Make sure that 'put_cv_' doesn't share the same cache line as 'get_lock_'.
    DCHECK_NE(offsetof(BlockingQueue, put_cv_) / 64,
        offsetof(BlockingQueue, get_lock_) / 64);
    put_list_.reset(new std::deque<T>());
    get_list_.reset(new std::deque<T>());
  }

  /// Gets an element from the queue, waiting indefinitely for one to become available.
  /// Returns false if we were shut down prior to getting the element, and there
  /// are no more elements available.
  bool BlockingGet(T* out) {
    boost::unique_lock<boost::mutex> read_lock(get_lock_);

    if (UNLIKELY(get_list_->empty())) {
      MonotonicStopWatch timer;
      {
        // Block off writers while swapping 'get_list_' with 'put_list_'.
        boost::unique_lock<boost::mutex> write_lock(put_lock_);

        // Note that it's intentional to signal the writer while holding 'put_lock_' to
        // avoid the race in which the writer may be signalled between when it checks
        // the queue size and when it calls wait(). Calling NotitfyAll() here may trigger
        // thundering herd and lead to contention (e.g. InitTuple() in scanner).
        put_cv_.NotifyOne();

        while (put_list_->empty()) {
          DCHECK(get_list_->empty());
          if (UNLIKELY(shutdown_)) return false;
          // Sleep with 'get_lock_' held to block off other readers which cannot
          // make progress anyway.
          timer.Start();
          get_cv_.Wait(write_lock);
          timer.Stop();
        }
        DCHECK(!put_list_->empty());
        put_list_.swap(get_list_);
      }
      total_get_wait_time_ += timer.ElapsedTime();
    }

    DCHECK(get_list_.get() != put_list_.get());
    DCHECK(!get_list_->empty());
    *out = get_list_->front();
    get_list_->pop_front();
    read_lock.unlock();
    return true;
  }

  /// Puts an element into the queue, waiting indefinitely until there is space.
  /// If the queue is shut down, returns false.
  bool BlockingPut(const T& val) {
    MonotonicStopWatch timer;
    boost::unique_lock<boost::mutex> write_lock(put_lock_);

    while (GetSize() >= max_elements_ && !shutdown_) {
      timer.Start();
      put_cv_.Wait(write_lock);
      timer.Stop();
    }
    total_put_wait_time_ += timer.ElapsedTime();
    if (UNLIKELY(shutdown_)) return false;

    DCHECK_LT(put_list_->size(), max_elements_);
    put_list_->push_back(val);
    write_lock.unlock();
    get_cv_.NotifyOne();
    if (GetSize() < max_elements_) put_cv_.NotifyOneIfWaiting();
    return true;
  }

  /// Puts an element into the queue, waiting until 'timeout_micros' elapses, if there is
  /// no space. If the queue is shut down, or if the timeout elapsed without being able to
  /// put the element, returns false.
  bool BlockingPutWithTimeout(const T& val, int64_t timeout_micros) {
    MonotonicStopWatch timer;
    boost::unique_lock<boost::mutex> write_lock(put_lock_);
    boost::system_time wtime = boost::get_system_time() +
        boost::posix_time::microseconds(timeout_micros);
    const struct timespec timeout = boost::detail::to_timespec(wtime);
    bool notified = true;
    while (GetSize() >= max_elements_ && !shutdown_ && notified) {
      timer.Start();
      // Wait until we're notified or until the timeout expires.
      notified = put_cv_.TimedWait(write_lock, &timeout);
      timer.Stop();
    }
    total_put_wait_time_ += timer.ElapsedTime();
    // If the list is still full or if the the queue has been shut down, return false.
    // NOTE: We don't check 'notified' here as it appears that pthread condition variables
    // have a weird behavior in which they can return ETIMEDOUT from timed_wait even if
    // another thread did in fact signal
    if (GetSize() >= max_elements_ || shutdown_) return false;
    DCHECK_LT(put_list_->size(), max_elements_);
    put_list_->push_back(val);
    write_lock.unlock();
    get_cv_.NotifyOne();
    return true;
  }

  /// Shut down the queue. Wakes up all threads waiting on BlockingGet or BlockingPut.
  void Shutdown() {
    {
      // No need to hold 'get_lock_' here. BlockingGet() may sleep with 'get_lock_' so
      // it may delay the caller here if the lock is acquired.
      boost::lock_guard<boost::mutex> write_lock(put_lock_);
      shutdown_ = true;
    }

    get_cv_.NotifyAll();
    put_cv_.NotifyAll();
  }

  uint32_t GetSize() const {
    // Reading queue size is racy by nature so no lock is held. It's racy because
    // the queue's size could have changed once the lock is dropped.
    return get_list_->size() + put_list_->size();
  }

  int64_t total_get_wait_time() const {
    // Same reasoning as GetSize() for not holding lock here.
    return total_get_wait_time_;
  }

  int64_t total_put_wait_time() const {
    // Same reasoning as GetSize() for not holding lock here.
    return total_put_wait_time_;
  }

 private:
  /// True if the BlockingQueue is being shut down. Guarded by 'put_lock_'.
  bool shutdown_;

  /// Maximum number of elements in 'get_list_' + 'put_list_'.
  const int max_elements_;

  /// Guards against concurrent access to 'put_list_'.
  /// Please see comments at the beginning of the file for lock ordering.
  boost::mutex put_lock_;

  /// The queue for items enqueued by BlockingPut(). Guarded by 'put_lock_'.
  boost::scoped_ptr<std::deque<T> > put_list_;

  /// Total amount of time threads blocked in BlockingPut(). Guarded by 'put_lock_'.
  int64_t total_put_wait_time_;

  /// BlockingPut()/BlockingPutWithTimeout() callers wait on this.
  ConditionVariable put_cv_;

  /// Guards against concurrent access to 'get_list_'.
  boost::mutex get_lock_;

  /// The queue of items to be consumed by BlockingGet(). Guarded by 'get_lock_'.
  boost::scoped_ptr<std::deque<T> > get_list_;

  /// Total amount of time a thread blocked in BlockingGet(). Guarded by 'get_lock_'.
  /// Note that a caller of BlockingGet() may sleep with 'get_lock_' held and this
  /// variable doesn't include the time which other threads block waiting for 'get_lock_'.
  int64_t total_get_wait_time_;

  /// BlockingGet() callers wait on this.
  ConditionVariable get_cv_;
};

}

#endif
