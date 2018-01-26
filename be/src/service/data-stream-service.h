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

#ifndef IMPALA_SERVICE_DATA_STREAM_SERVICE_H
#define IMPALA_SERVICE_DATA_STREAM_SERVICE_H

#include "gen-cpp/data_stream_service.service.h"

#include <mutex>
#include <queue>

#include "rpc/rpc-mgr.h"
#include "runtime/mem-tracker.h"
#include "util/spinlock.h"

namespace kudu {
namespace rpc {
class RpcContext;
} // namespace rpc
} // namespace kudu

namespace impala {

class MemPool;
class RowBatch;
class RpcMgr;

class RecycledBatchesQueue {
 public:
  void Insert(std::unique_ptr<RowBatch> batch) {
    std::unique_lock<SpinLock> l(lock_);
    recycled_batches_.push(std::move(batch));
  }

  void DrainQueue() {
    std::unique_lock<SpinLock> l(lock_);
    while (!recycled_batches_.empty()) recycled_batches_.pop();
  }

 private:
  SpinLock lock_;
  std::queue<std::unique_ptr<RowBatch>> recycled_batches_;
};

/// This is singleton class which provides data transmission services between fragment
/// instances. The client for this service is implemented in KrpcDataStreamSender.
/// The processing of incoming requests is implemented in KrpcDataStreamRecvr.
/// KrpcDataStreamMgr is responsible for routing the incoming requests to the
/// appropriate receivers.
class DataStreamService : public DataStreamServiceIf {
 public:
  DataStreamService(RpcMgr* rpc_mgr);

  /// XXX
  Status Init(RpcMgr* rpc_mgr);

  /// XXX
  virtual void Shutdown() override;

  /// Notifies the receiver to close the data stream specified in 'request'.
  /// The receiver replies to the client with a status serialized in 'response'.
  virtual void EndDataStream(const EndDataStreamRequestPB* request,
      EndDataStreamResponsePB* response, kudu::rpc::RpcContext* context);

  /// Sends a row batch to the receiver specified in 'request'.
  /// The receiver replies to the client with a status serialized in 'response'.
  virtual void TransmitData(const TransmitDataRequestPB* request,
      TransmitDataResponsePB* response, kudu::rpc::RpcContext* context);

  /// XXX
  void DrainQueue();

  /// XXX
  void AcquireTupleData(int64_t tid, const RowBatch& src, MemPool* tuple_data_pool);

 private:
  /// XXX
  const int num_svc_threads_;

  /// XXX
  std::vector<std::unique_ptr<RecycledBatchesQueue>> recycled_batches_queues_;

  /// XXX
  MemTracker mem_tracker_;
};

} // namespace impala
#endif // IMPALA_SERVICE_DATA_STREAM_SERVICE_H
