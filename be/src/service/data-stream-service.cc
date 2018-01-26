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

#include "service/data-stream-service.h"

#include "common/status.h"
#include "exec/kudu-util.h"
#include "kudu/rpc/rpc_context.h"
#include "rpc/rpc-mgr.h"
#include "runtime/krpc-data-stream-mgr.h"
#include "runtime/exec-env.h"
#include "runtime/row-batch.h"
#include "testutil/fault-injection-util.h"

#include "gutil/walltime.h"

#include "gen-cpp/data_stream_service.pb.h"

#include "common/names.h"

using kudu::rpc::RpcContext;

DEFINE_int32(datastream_service_queue_depth, 1024, "Size of datastream service queue");
DEFINE_int32(datastream_service_num_svc_threads, 0, "Number of datastream service "
    "processing threads. If left at default value 0, it will be set to number of CPU "
    "cores.");

namespace impala {

DataStreamService::DataStreamService(RpcMgr* rpc_mgr)
  : DataStreamServiceIf(rpc_mgr->metric_entity(), rpc_mgr->result_tracker()),
    num_svc_threads_(FLAGS_datastream_service_num_svc_threads > 0 ?
        FLAGS_datastream_service_num_svc_threads : CpuInfo::num_cores()),
    mem_tracker_(-1, "Data Stream Service", ExecEnv::GetInstance()->process_mem_tracker()) {
  for (int i = 0; i < num_svc_threads_; ++i) {
    recycled_batches_queues_.emplace_back(new RecycledBatchesQueue());
  }
}

Status DataStreamService::Init(RpcMgr* rpc_mgr) {
  return rpc_mgr->RegisterService(num_svc_threads_,
      FLAGS_datastream_service_queue_depth, this,
      boost::bind(&DataStreamService::DrainQueue, this), 60 * MILLIS_PER_SEC);
}

inline void DataStreamService::DrainQueue() {
  const int qid = Thread::current_tid() % num_svc_threads_;
  recycled_batches_queues_[qid]->DrainQueue();
}

void DataStreamService::EndDataStream(const EndDataStreamRequestPB* request,
    EndDataStreamResponsePB* response, RpcContext* rpc_context) {
  DrainQueue();
  // CloseSender() is guaranteed to eventually respond to this RPC so we don't do it here.
  ExecEnv::GetInstance()->KrpcStreamMgr()->CloseSender(request, response, rpc_context);
}

void DataStreamService::TransmitData(const TransmitDataRequestPB* request,
    TransmitDataResponsePB* response, RpcContext* rpc_context) {
  FAULT_INJECTION_RPC_DELAY(RPC_TRANSMITDATA);
  DrainQueue();
  // AddData() is guaranteed to eventually respond to this RPC so we don't do it here.
  ExecEnv::GetInstance()->KrpcStreamMgr()->AddData(request, response, rpc_context);
}

void DataStreamService::AcquireTupleData(
    int64_t tid, const RowBatch& src, MemPool* tuple_data_pool) {
  if (tuple_data_pool->total_reserved_bytes() > 0) {
    std::unique_ptr<RowBatch> dest =
        make_unique<RowBatch>(src.row_desc(), src.capacity(), &mem_tracker_);
    dest->tuple_data_pool()->AcquireData(tuple_data_pool, false);
    recycled_batches_queues_[tid % num_svc_threads_]->Insert(move(dest));
  }
}

void DataStreamService::Shutdown() {
  for (auto& q : recycled_batches_queues_) q->DrainQueue();
}

}
