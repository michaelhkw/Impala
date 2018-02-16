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

#ifndef IMPALA_SERVICE_POOL_H
#define IMPALA_SERVICE_POOL_H

#include <string>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/rpc/rpc_service.h"
#include "kudu/rpc/service_queue.h"
#include "kudu/util/status.h"
#include "util/histogram-metric.h"
#include "util/spinlock.h"
#include "util/thread.h"

namespace impala {
class MemTracker;

/// A pool of threads that handle new incoming RPC calls.
/// Also includes a queue that calls get pushed onto for handling by the pool.
class ImpalaServicePool : public kudu::rpc::RpcService {
 public:
  /// 'service_queue_depth' is the maximum number of requests that may be queued for this
  /// service before clients begin to see rejection errors.
  ///
  /// 'service' contains an interface implementation that will handle RPCs.
  ///
  /// 'service_mem_tracker' is the MemTracker for tracking the memory usage of RPC
  /// payloads in the service queue.
  ImpalaServicePool(const scoped_refptr<kudu::MetricEntity>& entity,
      size_t service_queue_length, kudu::rpc::ServiceIf* service,
      MemTracker* service_mem_tracker);

  virtual ~ImpalaServicePool();

  /// Start up the thread pool.
  virtual Status Init(int num_threads);

  /// Shut down the queue and the thread pool.
  virtual void Shutdown();

  kudu::rpc::RpcMethodInfo* LookupMethod(const kudu::rpc::RemoteMethod& method) override;

  virtual kudu::Status
      QueueInboundCall(gscoped_ptr<kudu::rpc::InboundCall> call) OVERRIDE;

  const std::string service_name() const;

 private:
  void RunThread();
  void RejectTooBusy(kudu::rpc::InboundCall* c);

  /// Respond with failure to the incoming call in 'call' with 'error_code' and 'status'
  /// and release the payload memory from 'mem_tracker_'. Takes ownership of 'call'.
  void FailAndReleaseRpc(const kudu::rpc::ErrorStatusPB::RpcErrorCodePB& error_code,
      const kudu::Status& status, kudu::rpc::InboundCall* call);

  /// Synchronizes accesses to 'service_mem_tracker_' to avoid over consumption.
  SpinLock mem_tracker_lock_;

  /// Tracks memory of inbound calls in 'service_queue_'.
  MemTracker* const service_mem_tracker_;

  /// Reference to the implementation of the RPC handlers. Not owned.
  kudu::rpc::ServiceIf* const service_;

  /// The set of service threads started to process incoming RPC calls.
  std::vector<std::unique_ptr<Thread>> threads_;

  /// The pending RPCs to be dequeued by the service threads.
  kudu::rpc::LifoServiceQueue service_queue_;

  /// TODO: Display these metrics in the debug webpage. IMPALA-6269
  /// Number of RPCs that timed out while waiting in the service queue.
  AtomicInt32 rpcs_timed_out_in_queue_;

  /// Number of RPCs that were rejected due to the queue being full.
  AtomicInt32 rpcs_queue_overflow_;

  /// Dummy histogram needed to call InboundCall::RecordHandlingStarted() to set
  /// appropriate internal KRPC state. Unused otherwise.
  /// TODO: Consider displaying this histogram in the debug webpage. IMPALA-6269
  scoped_refptr<kudu::Histogram> unused_histogram_;

  /// Protects against concurrent Shutdown() operations.
  /// TODO: This seems implausible given our current usage pattern.
  /// Consider removing lock.
  boost::mutex shutdown_lock_;
  bool closing_ = false;

  DISALLOW_COPY_AND_ASSIGN(ImpalaServicePool);
};

} // namespace impala

#endif  // IMPALA_SERVICE_POOL_H
