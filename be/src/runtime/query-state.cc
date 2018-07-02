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

#include "runtime/query-state.h"

#include <boost/thread/lock_guard.hpp>
#include <boost/thread/locks.hpp>

#include "common/thread-debug-info.h"
#include "exec/kudu-util.h"
#include "exprs/expr.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/util/status.h"
#include "rpc/rpc-mgr.h"
#include "rpc/rpc-mgr.inline.h"
#include "runtime/backend-client.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/bufferpool/reservation-util.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/initial-reservations.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-exec-mgr.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/impalad-metrics.h"
#include "util/thread.h"

#include "gen-cpp/control_service.pb.h"
#include "gen-cpp/control_service.proxy.h"

using kudu::rpc::RpcController;
using kudu::rpc::RpcSidecar;

#include "common/names.h"

DEFINE_int32(report_status_retry_interval_ms, 100,
    "The interval in milliseconds to wait before retrying a failed status report RPC to "
    "the coordinator.");

using namespace impala;

QueryState::ScopedRef::ScopedRef(const TUniqueId& query_id) {
  DCHECK(ExecEnv::GetInstance()->query_exec_mgr() != nullptr);
  query_state_ = ExecEnv::GetInstance()->query_exec_mgr()->GetQueryState(query_id);
}

QueryState::ScopedRef::~ScopedRef() {
  if (query_state_ == nullptr) return;
  ExecEnv::GetInstance()->query_exec_mgr()->ReleaseQueryState(query_state_);
}

QueryState::QueryState(const TQueryCtx& query_ctx, const string& request_pool)
  : query_ctx_(query_ctx),
    exec_resource_refcnt_(0),
    refcnt_(0),
    is_cancelled_(0),
    query_spilled_(0) {
  if (query_ctx_.request_pool.empty()) {
    // fix up pool name for tests
    DCHECK(!request_pool.empty());
    const_cast<TQueryCtx&>(query_ctx_).request_pool = request_pool;
  }
  TQueryOptions& query_options =
      const_cast<TQueryOptions&>(query_ctx_.client_request.query_options);
  // max_errors does not indicate how many errors in total have been recorded, but rather
  // how many are distinct. It is defined as the sum of the number of generic errors and
  // the number of distinct other errors.
  if (query_options.max_errors <= 0) {
    query_options.max_errors = 100;
  }
  if (query_options.batch_size <= 0) {
    query_options.__set_batch_size(DEFAULT_BATCH_SIZE);
  }
  InitMemTrackers();
}

void QueryState::ReleaseExecResources() {
  DCHECK(!released_exec_resources_);
  // Clean up temporary files.
  if (file_group_ != nullptr) file_group_->Close();
  // Release any remaining reservation.
  if (initial_reservations_ != nullptr) initial_reservations_->ReleaseResources();
  if (buffer_reservation_ != nullptr) buffer_reservation_->Close();
  if (desc_tbl_ != nullptr) desc_tbl_->ReleaseResources();
  // Mark the query as finished on the query MemTracker so that admission control will
  // not consider the whole query memory limit to be "reserved".
  query_mem_tracker_->set_query_exec_finished();
  // At this point query execution should not be consuming any resources but some tracked
  // memory may still be used by the ClientRequestState for result caching. The query
  // MemTracker will be closed later when this QueryState is torn down.
  released_exec_resources_ = true;
}

QueryState::~QueryState() {
  DCHECK_EQ(refcnt_.Load(), 0);
  DCHECK_EQ(exec_resource_refcnt_.Load(), 0);
  DCHECK(released_exec_resources_);
  if (query_mem_tracker_ != nullptr) {
    // Disconnect the query MemTracker hierarchy from the global hierarchy. After this
    // point nothing must touch this query's MemTracker and all tracked memory associated
    // with the query must be released. The whole query subtree of MemTrackers can
    // therefore be safely destroyed.
    query_mem_tracker_->CloseAndUnregisterFromParent();
  }
}

Status QueryState::Init(const TExecQueryFInstancesParams& rpc_params) {
  // Decremented in QueryExecMgr::StartQueryHelper() on success or by the caller of
  // Init() on failure. We need to do this before any returns because Init() always
  // returns a resource refcount to its caller.
  AcquireExecResourceRefcount();

  // Starting a new query creates threads and consumes a non-trivial amount of memory.
  // If we are already starved for memory, fail as early as possible to avoid consuming
  // more resources.
  ExecEnv* exec_env = ExecEnv::GetInstance();
  MemTracker* process_mem_tracker = exec_env->process_mem_tracker();
  if (process_mem_tracker->LimitExceeded()) {
    string msg = Substitute(
        "Query $0 could not start because the backend Impala daemon "
        "is over its memory limit", PrintId(query_id()));
    RETURN_IF_ERROR(process_mem_tracker->MemLimitExceeded(NULL, msg, 0));
  }

  RETURN_IF_ERROR(InitBufferPoolState());

  // don't copy query_ctx, it's large and we already did that in the c'tor
  rpc_params_.__set_coord_state_idx(rpc_params.coord_state_idx);
  TExecQueryFInstancesParams& non_const_params =
      const_cast<TExecQueryFInstancesParams&>(rpc_params);
  rpc_params_.fragment_ctxs.swap(non_const_params.fragment_ctxs);
  rpc_params_.__isset.fragment_ctxs = true;
  rpc_params_.fragment_instance_ctxs.swap(non_const_params.fragment_instance_ctxs);
  rpc_params_.__isset.fragment_instance_ctxs = true;

  // Claim the query-wide minimum reservation. Do this last so that we don't need
  // to handle releasing it if a later step fails.
  initial_reservations_ = obj_pool_.Add(new InitialReservations(&obj_pool_,
      buffer_reservation_, query_mem_tracker_,
      rpc_params.initial_mem_reservation_total_claims));
  RETURN_IF_ERROR(
      initial_reservations_->Init(query_id(), rpc_params.min_mem_reservation_bytes));
  return Status::OK();
}

void QueryState::InitMemTrackers() {
  const string& pool = query_ctx_.request_pool;
  int64_t bytes_limit = -1;
  if (query_options().__isset.mem_limit && query_options().mem_limit > 0) {
    bytes_limit = query_options().mem_limit;
    VLOG_QUERY << "Using query memory limit from query options: "
               << PrettyPrinter::Print(bytes_limit, TUnit::BYTES);
  }
  query_mem_tracker_ =
      MemTracker::CreateQueryMemTracker(query_id(), query_options(), pool, &obj_pool_);
}

Status QueryState::InitBufferPoolState() {
  ExecEnv* exec_env = ExecEnv::GetInstance();
  int64_t mem_limit = query_mem_tracker_->lowest_limit();
  int64_t max_reservation;
  if (query_options().__isset.buffer_pool_limit
      && query_options().buffer_pool_limit > 0) {
    max_reservation = query_options().buffer_pool_limit;
  } else if (mem_limit == -1) {
    // No query mem limit. The process-wide reservation limit is the only limit on
    // reservations.
    max_reservation = numeric_limits<int64_t>::max();
  } else {
    DCHECK_GE(mem_limit, 0);
    max_reservation = ReservationUtil::GetReservationLimitFromMemLimit(mem_limit);
  }
  VLOG_QUERY << "Buffer pool limit for " << PrintId(query_id()) << ": " << max_reservation;

  buffer_reservation_ = obj_pool_.Add(new ReservationTracker);
  buffer_reservation_->InitChildTracker(
      NULL, exec_env->buffer_reservation(), query_mem_tracker_, max_reservation);

  // TODO: once there's a mechanism for reporting non-fragment-local profiles,
  // should make sure to report this profile so it's not going into a black hole.
  RuntimeProfile* dummy_profile = RuntimeProfile::Create(&obj_pool_, "dummy");
  // Only create file group if spilling is enabled.
  if (query_options().scratch_limit != 0 && !query_ctx_.disable_spilling) {
    file_group_ = obj_pool_.Add(
        new TmpFileMgr::FileGroup(exec_env->tmp_file_mgr(), exec_env->disk_io_mgr(),
            dummy_profile, query_id(), query_options().scratch_limit));
  }
  return Status::OK();
}

FragmentInstanceState* QueryState::GetFInstanceState(const TUniqueId& instance_id) {
  VLOG_FILE << "GetFInstanceState(): instance_id=" << PrintId(instance_id);
  if (!instances_prepared_promise_.Get().ok()) return nullptr;
  auto it = fis_map_.find(instance_id);
  return it != fis_map_.end() ? it->second : nullptr;
}

void QueryState::ReportExecStatus(bool done, const Status& status,
    FragmentInstanceState* fis) {
  ReportExecStatusAux(done, status, fis, true);
}

void QueryState::ReportExecStatusAux(bool done, const Status& status,
    FragmentInstanceState* fis, bool instances_started) {
  // if we're reporting an error, we're done
  DCHECK(status.ok() || done);
  // if this is not for a specific fragment instance, we're reporting an error
  DCHECK(fis != nullptr || !status.ok());
  DCHECK(fis == nullptr || fis->IsPrepared());

  // This will send a report even if we are cancelled.  If the query completed correctly
  // but fragments still need to be cancelled (e.g. limit reached), the coordinator will
  // be waiting for a final report and profile.
  ReportExecStatusRequestPB req;
  req.set_protocol_version(ImpalaInternalServiceVersionPB::V1);
  UniqueIdPB* query_id_pb = req.mutable_query_id();
  query_id_pb->set_lo(query_id().lo);
  query_id_pb->set_hi(query_id().hi);

  DCHECK(rpc_params().__isset.coord_state_idx);
  req.set_coord_state_idx(rpc_params().coord_state_idx);
  status.ToProto(req.mutable_status());

  RpcController rpc_controller;
  if (fis != nullptr) {
    // create status for 'fis'
    FragmentInstanceExecStatusPB* instance_status = req.add_instance_exec_status();
    UniqueIdPB* finstance_id_pb = instance_status->mutable_fragment_instance_id();
    finstance_id_pb->set_lo(fis->instance_id().lo);
    finstance_id_pb->set_hi(fis->instance_id().hi);
    status.ToProto(instance_status->mutable_status());
    instance_status->set_done(done);
    instance_status->set_current_state(fis->current_state());

    // Serialize the profile in Thrift and then send it as a sidecar. We keep the
    // runtime profile as Thrift object as Impala client still communicates with
    // Impala server with Thrift RPC.
    //
    // Note the ownership of the Thrift profile buffer is transferred to RPC layer
    // and it is freed after the RPC payload is sent. If serialization of the profile
    // to RPC sidecar fails, we will proceed without the profile so that the coordinator
    // can still get the status instead of hitting IMPALA-2990.
    DCHECK(fis->profile() != nullptr);
    unique_ptr<kudu::faststring> thrift_profile_buffer = make_unique<kudu::faststring>();
    Status serialize_status =
        fis->profile()->SerializeToFaststring(thrift_profile_buffer.get());
    if (serialize_status.ok()) {
      int sidecar_idx;
      unique_ptr<RpcSidecar> sidecar =
          RpcSidecar::FromFaststring(move(thrift_profile_buffer));
      if (rpc_controller.AddOutboundSidecar(move(sidecar), &sidecar_idx).ok()) {
        instance_status->set_thrift_profile_sidecar_idx(sidecar_idx);
      }
    } else {
      const string err = Substitute("Failed to serialize $0profile for query ID $1: $2",
          done ? "final" : "", PrintId(query_id()), serialize_status.GetDetail());
      LOG(ERROR) << err;
    }

    // Only send updates to insert status if fragment is finished, the coordinator waits
    // until query execution is done to use them anyhow.
    RuntimeState* state = fis->runtime_state();
    if (done) state->dml_exec_state()->ToProto(req.mutable_insert_exec_status());

    // Send new errors to coordinator
    state->GetUnreportedErrors(&req);
  }

  RpcMgr* rpc_mgr = ExecEnv::GetInstance()->rpc_mgr();
  unique_ptr<ControlServiceProxy> proxy;
  rpc_mgr->GetProxy(query_ctx().coord_krpc_address, &proxy);

  ReportExecStatusResponsePB resp;
  kudu::Status rpc_status = proxy->ReportExecStatus(req, &resp, &rpc_controller);
  Status result_status(resp.status());

  if ((!rpc_status.ok() || !result_status.ok()) && instances_started) {
    // TODO: should we try to keep rpc_status for the final report? (but the final
    // report, following this Cancel(), may not succeed anyway.)
    // TODO: not keeping an error status here means that all instances might
    // abort with CANCELLED status, despite there being an error
    // TODO: Fix IMPALA-2990. Cancelling fragment instances without sending the
    // ReporExecStatus RPC may cause query to hang as the coordinator may not be aware
    // of the cancellation. Remove the log statements once IMPALA-2990 is fixed.
    if (!rpc_status.ok() && !RpcMgr::IsServerTooBusy(rpc_controller)) {
      LOG(ERROR) << "Cancelling fragment instances due to failure to reach the "
                 << "coordinator. (" << FromKuduStatus(rpc_status, "").GetDetail()
                 << "). Query " << PrintId(query_id()) << " may hang. See IMPALA-2990.";
    } else if (!result_status.ok()) {
      // If the ReportExecStatus RPC succeeded in reaching the coordinator and we get
      // back a non-OK status, it means that the coordinator expects us to cancel the
      // fragment instances for this query.
      LOG(INFO) << "Cancelling fragment instances as directed by the coordinator. "
                << "Returned status: " << result_status.GetDetail();
    }
    Cancel();
  }
}

Status QueryState::WaitForPrepare() {
  return instances_prepared_promise_.Get();
}

void QueryState::StartFInstances() {
  VLOG_QUERY << "StartFInstances(): query_id=" << PrintId(query_id())
      << " #instances=" << rpc_params_.fragment_instance_ctxs.size();
  DCHECK_GT(refcnt_.Load(), 0);
  DCHECK_GT(exec_resource_refcnt_.Load(), 0) << "Should have been taken in Init()";

  // set up desc tbl
  DCHECK(query_ctx().__isset.desc_tbl);
  Status status = DescriptorTbl::Create(&obj_pool_, query_ctx().desc_tbl, &desc_tbl_);
  if (!status.ok()) {
    discard_result(instances_prepared_promise_.Set(status));
    ReportExecStatusAux(true, status, nullptr, false);
    return;
  }
  VLOG_QUERY << "descriptor table for query=" << PrintId(query_id())
             << "\n" << desc_tbl_->DebugString();

  Status thread_create_status;
  DCHECK_GT(rpc_params_.fragment_ctxs.size(), 0);
  TPlanFragmentCtx* fragment_ctx = &rpc_params_.fragment_ctxs[0];
  int fragment_ctx_idx = 0;
  fragment_events_start_time_ = MonotonicStopWatch::Now();
  for (const TPlanFragmentInstanceCtx& instance_ctx: rpc_params_.fragment_instance_ctxs) {
    // determine corresponding TPlanFragmentCtx
    if (fragment_ctx->fragment.idx != instance_ctx.fragment_idx) {
      ++fragment_ctx_idx;
      DCHECK_LT(fragment_ctx_idx, rpc_params_.fragment_ctxs.size());
      fragment_ctx = &rpc_params_.fragment_ctxs[fragment_ctx_idx];
      // we expect fragment and instance contexts to follow the same order
      DCHECK_EQ(fragment_ctx->fragment.idx, instance_ctx.fragment_idx);
    }
    FragmentInstanceState* fis = obj_pool_.Add(
        new FragmentInstanceState(this, *fragment_ctx, instance_ctx));

    // start new thread to execute instance
    refcnt_.Add(1); // decremented in ExecFInstance()
    AcquireExecResourceRefcount(); // decremented in ExecFInstance()
    string thread_name = Substitute("$0 (finst:$1)",
        FragmentInstanceState::FINST_THREAD_NAME_PREFIX,
        PrintId(instance_ctx.fragment_instance_id));
    unique_ptr<Thread> t;
    thread_create_status = Thread::Create(FragmentInstanceState::FINST_THREAD_GROUP_NAME,
        thread_name, [this, fis]() { this->ExecFInstance(fis); }, &t, true);
    if (!thread_create_status.ok()) {
      // Undo refcnt increments done immediately prior to Thread::Create(). The
      // reference counts were both greater than zero before the increments, so
      // neither of these decrements will free any structures.
      ReleaseExecResourceRefcount();
      ExecEnv::GetInstance()->query_exec_mgr()->ReleaseQueryState(this);
      break;
    }
    // Fragment instance successfully started
    fis_map_.emplace(fis->instance_id(), fis);
    // update fragment_map_
    vector<FragmentInstanceState*>& fis_list = fragment_map_[instance_ctx.fragment_idx];
    fis_list.push_back(fis);
    t->Detach();
  }

  // don't return until every instance is prepared and record the first non-OK
  // (non-CANCELLED if available) status (including any error from thread creation
  // above).
  Status prepare_status = thread_create_status;
  for (auto entry: fis_map_) {
    Status instance_status = entry.second->WaitForPrepare();
    // don't wipe out an error in one instance with the resulting CANCELLED from
    // the remaining instances
    if (!instance_status.ok() && (prepare_status.ok() || prepare_status.IsCancelled())) {
      prepare_status = instance_status;
    }
  }
  discard_result(instances_prepared_promise_.Set(prepare_status));
  // If this is aborting due to failure in thread creation, report status to the
  // coordinator to start query cancellation. (Other errors are reported by the
  // fragment instance itself.)
  if (!thread_create_status.ok()) {
    ReportExecStatusAux(true, thread_create_status, nullptr, true);
  }
}

void QueryState::AcquireExecResourceRefcount() {
  DCHECK(!released_exec_resources_);
  exec_resource_refcnt_.Add(1);
}

void QueryState::ReleaseExecResourceRefcount() {
  int32_t new_val = exec_resource_refcnt_.Add(-1);
  DCHECK_GE(new_val, 0);
  if (new_val == 0) ReleaseExecResources();
}

void QueryState::ExecFInstance(FragmentInstanceState* fis) {
  GetThreadDebugInfo()->SetInstanceId(fis->instance_id());

  ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT->Increment(1L);
  ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS->Increment(1L);
  VLOG_QUERY << "Executing instance. instance_id=" << PrintId(fis->instance_id())
      << " fragment_idx=" << fis->instance_ctx().fragment_idx
      << " per_fragment_instance_idx=" << fis->instance_ctx().per_fragment_instance_idx
      << " coord_state_idx=" << rpc_params().coord_state_idx
      << " #in-flight="
      << ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT->GetValue();
  Status status = fis->Exec();
  ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT->Increment(-1L);
  VLOG_QUERY << "Instance completed. instance_id=" << PrintId(fis->instance_id())
      << " #in-flight="
      << ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT->GetValue()
      << " status=" << status;
  // initiate cancellation if nobody has done so yet
  if (!status.ok()) Cancel();
  // decrement refcount taken in StartFInstances()
  ReleaseExecResourceRefcount();
  // decrement refcount taken in StartFInstances()
  ExecEnv::GetInstance()->query_exec_mgr()->ReleaseQueryState(this);
}

void QueryState::Cancel() {
  VLOG_QUERY << "Cancel: query_id=" << PrintId(query_id());
  (void) instances_prepared_promise_.Get();
  if (!is_cancelled_.CompareAndSwap(0, 1)) return;
  for (auto entry: fis_map_) entry.second->Cancel();
}

void QueryState::PublishFilter(const TPublishFilterParams& params) {
  if (!instances_prepared_promise_.Get().ok()) return;
  DCHECK_EQ(fragment_map_.count(params.dst_fragment_idx), 1);
  for (FragmentInstanceState* fis : fragment_map_[params.dst_fragment_idx]) {
    fis->PublishFilter(params);
  }
}

Status QueryState::StartSpilling(RuntimeState* runtime_state, MemTracker* mem_tracker) {
  // Return an error message with the root cause of why spilling is disabled.
  if (query_options().scratch_limit == 0) {
    return mem_tracker->MemLimitExceeded(
        runtime_state, "Could not free memory by spilling to disk: scratch_limit is 0");
  } else if (query_ctx_.disable_spilling) {
    return mem_tracker->MemLimitExceeded(runtime_state,
        "Could not free memory by spilling to disk: spilling was disabled by planner. "
        "Re-enable spilling by setting the query option DISABLE_UNSAFE_SPILLS=false");
  }
  // 'file_group_' must be non-NULL for spilling to be enabled.
  DCHECK(file_group_ != nullptr);
  if (query_spilled_.CompareAndSwap(0, 1)) {
    ImpaladMetrics::NUM_QUERIES_SPILLED->Increment(1);
  }
  return Status::OK();
}
