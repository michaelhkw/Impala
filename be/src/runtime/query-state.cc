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
#include "kudu/util/monotime.h"
#include "kudu/util/status.h"
#include "rpc/rpc-mgr.h"
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
#include "runtime/scanner-mem-limiter.h"
#include "service/control-service.h"
#include "util/debug-util.h"
#include "util/impalad-metrics.h"
#include "util/thread.h"

#include "gen-cpp/control_service.pb.h"
#include "gen-cpp/control_service.proxy.h"

using kudu::MonoDelta;
using kudu::rpc::RpcController;
using kudu::rpc::RpcSidecar;

#include "common/names.h"

DEFINE_int32(status_report_interval_ms, 5000,
    "interval between profile reports; in milliseconds");
DEFINE_int32(report_status_retry_interval_ms, 100,
    "The interval in milliseconds to wait before retrying a failed status report RPC to "
    "the coordinator.");
DECLARE_int32(backend_client_rpc_timeout_ms);
DECLARE_int64(rpc_max_message_size);

using namespace impala;

QueryState::ScopedRef::ScopedRef(const TUniqueId& query_id) {
  DCHECK(ExecEnv::GetInstance()->query_exec_mgr() != nullptr);
  query_state_ = ExecEnv::GetInstance()->query_exec_mgr()->GetQueryState(query_id);
}

QueryState::ScopedRef::~ScopedRef() {
  if (query_state_ == nullptr) return;
  ExecEnv::GetInstance()->query_exec_mgr()->ReleaseQueryState(query_state_);
}

QueryState::QueryState(
    const TQueryCtx& query_ctx, int64_t mem_limit, const string& request_pool)
  : query_ctx_(query_ctx),
    backend_resource_refcnt_(0),
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
  query_mem_tracker_ = MemTracker::CreateQueryMemTracker(
      query_id(), mem_limit, query_ctx_.request_pool, &obj_pool_);
}

void QueryState::ReleaseBackendResources() {
  DCHECK(!released_backend_resources_);
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
  released_backend_resources_ = true;
}

QueryState::~QueryState() {
  DCHECK_EQ(refcnt_.Load(), 0);
  DCHECK_EQ(backend_resource_refcnt_.Load(), 0);
  if (query_mem_tracker_ != nullptr) {
    // Disconnect the query MemTracker hierarchy from the global hierarchy. After this
    // point nothing must touch this query's MemTracker and all tracked memory associated
    // with the query must be released. The whole query subtree of MemTrackers can
    // therefore be safely destroyed.
    query_mem_tracker_->CloseAndUnregisterFromParent();
  }
}

Status QueryState::Init(const TExecQueryFInstancesParams& exec_rpc_params) {
  // Decremented in QueryExecMgr::StartQueryHelper() on success or by the caller of
  // Init() on failure. We need to do this before any returns because Init() always
  // returns a resource refcount to its caller.
  AcquireBackendResourceRefcount();

  // Starting a new query creates threads and consumes a non-trivial amount of memory.
  // If we are already starved for memory, fail as early as possible to avoid consuming
  // more resources.
  ExecEnv* exec_env = ExecEnv::GetInstance();
  MemTracker* process_mem_tracker = exec_env->process_mem_tracker();
  if (process_mem_tracker->LimitExceeded(MemLimit::HARD)) {
    string msg = Substitute(
        "Query $0 could not start because the backend Impala daemon "
        "is over its memory limit", PrintId(query_id()));
    RETURN_IF_ERROR(process_mem_tracker->MemLimitExceeded(NULL, msg, 0));
  }

  RETURN_IF_ERROR(InitBufferPoolState());

  // Initialize the RPC proxy once and report any error.
  RETURN_IF_ERROR(ControlService::GetProxy(query_ctx().coord_krpc_address,
      query_ctx().coord_address.hostname, &proxy_));

  // don't copy query_ctx, it's large and we already did that in the c'tor
  exec_rpc_params_.__set_coord_state_idx(exec_rpc_params.coord_state_idx);
  TExecQueryFInstancesParams& non_const_params =
      const_cast<TExecQueryFInstancesParams&>(exec_rpc_params);
  exec_rpc_params_.fragment_ctxs.swap(non_const_params.fragment_ctxs);
  exec_rpc_params_.__isset.fragment_ctxs = true;
  exec_rpc_params_.fragment_instance_ctxs.swap(non_const_params.fragment_instance_ctxs);
  exec_rpc_params_.__isset.fragment_instance_ctxs = true;

  instances_prepared_barrier_.reset(
      new CountingBarrier(exec_rpc_params_.fragment_instance_ctxs.size()));
  instances_finished_barrier_.reset(
      new CountingBarrier(exec_rpc_params_.fragment_instance_ctxs.size()));

  // Claim the query-wide minimum reservation. Do this last so that we don't need
  // to handle releasing it if a later step fails.
  initial_reservations_ = obj_pool_.Add(new InitialReservations(&obj_pool_,
      buffer_reservation_, query_mem_tracker_,
      exec_rpc_params.initial_mem_reservation_total_claims));
  RETURN_IF_ERROR(
      initial_reservations_->Init(query_id(), exec_rpc_params.min_mem_reservation_bytes));
  scanner_mem_limiter_ = obj_pool_.Add(new ScannerMemLimiter);
  return Status::OK();
}

Status QueryState::InitBufferPoolState() {
  ExecEnv* exec_env = ExecEnv::GetInstance();
  int64_t mem_limit = query_mem_tracker_->GetLowestLimit(MemLimit::HARD);
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
  VLOG(2) << "Buffer pool limit for " << PrintId(query_id()) << ": " << max_reservation;

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

const char* QueryState::BackendExecStateToString(const BackendExecState& state) {
  static const unordered_map<BackendExecState, const char*> exec_state_to_str{
      {BackendExecState::PREPARING, "PREPARING"},
      {BackendExecState::EXECUTING, "EXECUTING"},
      {BackendExecState::FINISHED, "FINISHED"},
      {BackendExecState::CANCELLED, "CANCELLED"},
      {BackendExecState::ERROR, "ERROR"}};

  return exec_state_to_str.at(state);
}

void QueryState::UpdateBackendExecState() {
  DFAKE_SCOPED_LOCK(backend_exec_state_lock_);
  {
    BackendExecState cur_state = backend_exec_state_;
    unique_lock<SpinLock> l(status_lock_);
    // We shouldn't call this function if we're already in a terminal state.
    DCHECK(cur_state == BackendExecState::PREPARING ||
        cur_state == BackendExecState::EXECUTING)
            << " Current State: " << BackendExecStateToString(cur_state)
            << " | Current Status: " << overall_status_.GetDetail();
    if (overall_status_.IsCancelled()) {
      // Received cancellation - go to CANCELLED state.
      backend_exec_state_ = BackendExecState::CANCELLED;
    } else if (!overall_status_.ok()) {
      // Error while executing - go to ERROR state.
      backend_exec_state_ = BackendExecState::ERROR;
    } else {
      // Transition to the next state in the lifecycle.
      backend_exec_state_ = cur_state == BackendExecState::PREPARING ?
          BackendExecState::EXECUTING : BackendExecState::FINISHED;
    }
  }
  // Send one last report if the query has reached the terminal state.
  if (IsTerminalState()) {
    VLOG_QUERY << "UpdateBackendExecState(): last report for " << PrintId(query_id());
    ReportExecStatus();
  }
}

FragmentInstanceState* QueryState::GetFInstanceState(const TUniqueId& instance_id) {
  VLOG_FILE << "GetFInstanceState(): instance_id=" << PrintId(instance_id);
  if (!WaitForPrepare().ok()) return nullptr;
  auto it = fis_map_.find(instance_id);
  return it != fis_map_.end() ? it->second : nullptr;
}

void QueryState::ConstructReport(bool instances_started,
    ReportExecStatusRequestPB* report, TRuntimeProfileForest* profiles_forest) {
  report->Clear();
  TUniqueIdToUniqueIdPB(query_id(), report->mutable_query_id());
  DCHECK(exec_rpc_params().__isset.coord_state_idx);
  report->set_coord_state_idx(exec_rpc_params().coord_state_idx);
  {
    std::unique_lock<SpinLock> l(status_lock_);
    overall_status_.ToProto(report->mutable_overall_status());
    if (IsValidFInstanceId(failed_finstance_id_)) {
      TUniqueIdToUniqueIdPB(failed_finstance_id_, report->mutable_fragment_instance_id());
    }
  }

  if (instances_started) {
    for (const auto& entry : fis_map_) {
      FragmentInstanceState* fis = entry.second;

      // If this fragment instance has already sent its last report, skip it.
      if (fis->final_report_sent()) {
        DCHECK(fis->IsDone());
        continue;
      }

      // Update the status and profiles of this fragment instance.
      FragmentInstanceExecStatusPB* instance_status = report->add_instance_exec_status();
      profiles_forest->profile_trees.emplace_back();
      fis->GetStatusReport(instance_status, &profiles_forest->profile_trees.back());
    }
  }
}

void QueryState::ReportExecStatus() {
  bool instances_started = fis_map_.size() > 0;

  // This will send a report even if we are cancelled.  If the query completed correctly
  // but fragments still need to be cancelled (e.g. limit reached), the coordinator will
  // be waiting for a final report and profile.
  ReportExecStatusRequestPB report;

  // Gather the statuses and profiles of the fragment instances.
  TRuntimeProfileForest profiles_forest;
  ConstructReport(instances_started, &report, &profiles_forest);

  // Serialize the runtime profile with Thrift to 'profile_buf'. Note that the
  // serialization output is owned by 'serializer' so this must be alive until RPC
  // is done.
  ThriftSerializer serializer(true);
  uint8_t* profile_buf = nullptr;
  uint32_t profile_len = 0;
  Status serialize_status =
      serializer.SerializeToBuffer(&profiles_forest, &profile_len, &profile_buf);
  if (UNLIKELY(!serialize_status.ok() ||
          profile_len > FLAGS_rpc_max_message_size ||
          !DebugAction(query_options(), "REPORT_EXEC_STATUS_PROFILE").ok())) {
    profile_buf = nullptr;
    LOG(ERROR) << Substitute("Failed to create $0profile for query $1: "
        "status=$2 len=$3", IsTerminalState() ? "final " : "", PrintId(query_id()),
        serialize_status.ok() ? "OK" : serialize_status.GetDetail(), profile_len);
  }

  // Try to send the RPC 3 times before failing. Sleep for 100ms between retries.
  // It's safe to retry the RPC as the coordinator handles duplicate RPC messages.
  Status rpc_status;
  Status result_status;
  for (int i = 0; i < 3; ++i) {
    RpcController rpc_controller;

    // The profile is a thrift structure serialized to a string and sent as a sidecar.
    // We keep the runtime profile as Thrift object as Impala client still communicates
    // with Impala server with Thrift RPC.
    //
    // Note that the sidecar is created with faststring so the ownership of the Thrift
    // profile buffer is transferred to RPC layer and it is freed after the RPC payload
    // is sent. If serialization of the profile to RPC sidecar fails, we will proceed
    // without the profile so that the coordinator can still get the status instead of
    // hitting IMPALA-2990.
    if (profile_buf != nullptr) {
      unique_ptr<kudu::faststring> sidecar_buf = make_unique<kudu::faststring>();
      sidecar_buf->assign_copy(profile_buf, profile_len);
      unique_ptr<RpcSidecar> sidecar = RpcSidecar::FromFaststring(move(sidecar_buf));

      int sidecar_idx;
      kudu::Status sidecar_status =
          rpc_controller.AddOutboundSidecar(move(sidecar), &sidecar_idx);
      if (LIKELY(sidecar_status.ok())) {
        report.set_thrift_profiles_sidecar_idx(sidecar_idx);
      } else {
        LOG(DFATAL) <<
            FromKuduStatus(sidecar_status, "Failed to add sidecar").GetDetail();
      }
    }

    rpc_controller.set_timeout(
        MonoDelta::FromMilliseconds(FLAGS_backend_client_rpc_timeout_ms));
    ReportExecStatusResponsePB resp;
    rpc_status = FromKuduStatus(proxy_->ReportExecStatus(report, &resp, &rpc_controller),
        "ReportExecStatus() RPC failed");
    result_status = Status(resp.status());
    if (rpc_status.ok()) break;
    //TODO: Consider exponential backoff.
    if (i < 2) SleepForMs(FLAGS_report_status_retry_interval_ms);
    LOG(WARNING) <<
        Substitute("Retrying ReportExecStatus() RPC for query $0", PrintId(query_id()));
  }

  if ((!rpc_status.ok() || !result_status.ok()) && instances_started) {
    // TODO: should we try to keep rpc_status for the final report? (but the final
    // report, following this Cancel(), may not succeed anyway.)
    // TODO: not keeping an error status here means that all instances might
    // abort with CANCELLED status, despite there being an error
    // TODO: Fix IMPALA-2990. Cancelling fragment instances without sending the
    // ReporExecStatus RPC may cause query to hang as the coordinator may not be aware
    // of the cancellation. Remove the log statements once IMPALA-2990 is fixed.
    if (!rpc_status.ok()) {
      LOG(ERROR) << "Cancelling fragment instances due to failure to reach the "
                 << "coordinator. (" << rpc_status.GetDetail()
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

void QueryState::ErrorDuringPrepare(const Status& status, const TUniqueId& finst_id) {
  {
    std::unique_lock<SpinLock> l(status_lock_);
    if (!HasErrorStatus()) {
      overall_status_ = status;
      failed_finstance_id_ = finst_id;
    }
  }
  discard_result(instances_prepared_barrier_->Notify());
}

void QueryState::ErrorDuringExecute(const Status& status, const TUniqueId& finst_id) {
  {
    std::unique_lock<SpinLock> l(status_lock_);
    if (!HasErrorStatus()) {
      overall_status_ = status;
      failed_finstance_id_ = finst_id;
    }
  }
  instances_finished_barrier_->NotifyRemaining();
}

Status QueryState::WaitForPrepare() {
  instances_prepared_barrier_->Wait();
  unique_lock<SpinLock> l(status_lock_);
  return overall_status_;
}

void QueryState::WaitForFinish() {
  instances_finished_barrier_->Wait();
}

bool QueryState::WaitForFinishOrTimeout(int32_t timeout_ms) {
  bool timed_out = false;
  instances_finished_barrier_->Wait(timeout_ms, &timed_out);
  return !timed_out;
}

bool QueryState::StartFInstances() {
  VLOG(2) << "StartFInstances(): query_id=" << PrintId(query_id())
          << " #instances=" << exec_rpc_params_.fragment_instance_ctxs.size();
  DCHECK_GT(refcnt_.Load(), 0);
  DCHECK_GT(backend_resource_refcnt_.Load(), 0) << "Should have been taken in Init()";

  DCHECK_GT(exec_rpc_params_.fragment_ctxs.size(), 0);
  TPlanFragmentCtx* fragment_ctx = &exec_rpc_params_.fragment_ctxs[0];
  int num_unstarted_instances = exec_rpc_params_.fragment_instance_ctxs.size();
  int fragment_ctx_idx = 0;

  // set up desc tbl
  DCHECK(query_ctx().__isset.desc_tbl);
  Status start_finstances_status =
      DescriptorTbl::Create(&obj_pool_, query_ctx().desc_tbl, &desc_tbl_);
  if (UNLIKELY(!start_finstances_status.ok())) goto error;
  VLOG(2) << "descriptor table for query=" << PrintId(query_id())
          << "\n" << desc_tbl_->DebugString();

  fragment_events_start_time_ = MonotonicStopWatch::Now();
  for (const TPlanFragmentInstanceCtx& instance_ctx :
           exec_rpc_params_.fragment_instance_ctxs) {
    // determine corresponding TPlanFragmentCtx
    if (fragment_ctx->fragment.idx != instance_ctx.fragment_idx) {
      ++fragment_ctx_idx;
      DCHECK_LT(fragment_ctx_idx, exec_rpc_params_.fragment_ctxs.size());
      fragment_ctx = &exec_rpc_params_.fragment_ctxs[fragment_ctx_idx];
      // we expect fragment and instance contexts to follow the same order
      DCHECK_EQ(fragment_ctx->fragment.idx, instance_ctx.fragment_idx);
    }
    FragmentInstanceState* fis = obj_pool_.Add(
        new FragmentInstanceState(this, *fragment_ctx, instance_ctx));

    // start new thread to execute instance
    refcnt_.Add(1); // decremented in ExecFInstance()
    AcquireBackendResourceRefcount(); // decremented in ExecFInstance()

    // Add the fragment instance ID to the 'fis_map_'. Has to happen before the thread is
    // spawned or we may race with users of 'fis_map_'.
    fis_map_.emplace(fis->instance_id(), fis);

    // Update fragment_map_. Has to happen before the thread is spawned below or
    // we may race with users of 'fragment_map_'.
    vector<FragmentInstanceState*>& fis_list = fragment_map_[instance_ctx.fragment_idx];
    fis_list.push_back(fis);

    string thread_name = Substitute("$0 (finst:$1)",
        FragmentInstanceState::FINST_THREAD_NAME_PREFIX,
        PrintId(instance_ctx.fragment_instance_id));
    unique_ptr<Thread> t;

    // Inject thread creation failures through debug actions if enabled.
    Status debug_action_status = DebugAction(query_options(), "FIS_FAIL_THREAD_CREATION");
    start_finstances_status = !debug_action_status.ok() ? debug_action_status :
        Thread::Create(FragmentInstanceState::FINST_THREAD_GROUP_NAME, thread_name,
            [this, fis]() { this->ExecFInstance(fis); }, &t, true);
    if (!start_finstances_status.ok()) {
      fis_map_.erase(fis->instance_id());
      fis_list.pop_back();
      // Undo refcnt increments done immediately prior to Thread::Create(). The
      // reference counts were both greater than zero before the increments, so
      // neither of these decrements will free any structures.
      ReleaseBackendResourceRefcount();
      ExecEnv::GetInstance()->query_exec_mgr()->ReleaseQueryState(this);
      goto error;
    }
    t->Detach();
    --num_unstarted_instances;
  }
  return true;

error:
  // This point is reached if there were general errors to start query fragment instances.
  // Wait for all running fragment instances to finish preparing and report status to the
  // coordinator to start query cancellation.
  {
    // Prioritize general errors as a query killing error, even over an error
    // during Prepare() for a FIS. Overwrite any existing value in 'overall_status_'.
    std::unique_lock<SpinLock> l(status_lock_);
    overall_status_ = start_finstances_status;
    failed_finstance_id_ = TUniqueId();
  }
  // Updates the barrier for all unstarted fragment instances.
  for (int i = 0; i < num_unstarted_instances; ++i) {
    DonePreparing();
  }
  // Block until all the already started fragment instances finish Prepare()-ing before
  // reporting the error.
  discard_result(WaitForPrepare());
  UpdateBackendExecState();
  DCHECK(IsTerminalState());
  return false;
}

void QueryState::MonitorFInstances() {
  // Wait for all fragment instances to finish preparing.
  discard_result(WaitForPrepare());
  UpdateBackendExecState();
  if (IsTerminalState()) goto done;

  // Once all fragment instances finished preparing successfully, start periodic
  // reporting back to the coordinator.
  DCHECK(backend_exec_state_ == BackendExecState::EXECUTING)
      << BackendExecStateToString(backend_exec_state_);
  if (FLAGS_status_report_interval_ms > 0) {
    while (!WaitForFinishOrTimeout(FLAGS_status_report_interval_ms)) {
      ReportExecStatus();
    }
  } else {
    WaitForFinish();
  }
  UpdateBackendExecState();
  DCHECK(IsTerminalState());

done:
  if (backend_exec_state_ == BackendExecState::FINISHED) {
    for (const auto& entry : fis_map_) {
      DCHECK(entry.second->IsDone());
    }
  } else {
    DCHECK_EQ(is_cancelled_.Load(), 1);
  }
}

void QueryState::AcquireBackendResourceRefcount() {
  DCHECK(!released_backend_resources_);
  backend_resource_refcnt_.Add(1);
}

void QueryState::ReleaseBackendResourceRefcount() {
  int32_t new_val = backend_resource_refcnt_.Add(-1);
  DCHECK_GE(new_val, 0);
  if (new_val == 0) ReleaseBackendResources();
}

void QueryState::ExecFInstance(FragmentInstanceState* fis) {
  GetThreadDebugInfo()->SetInstanceId(fis->instance_id());

  ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT->Increment(1L);
  ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS->Increment(1L);
  VLOG_QUERY << "Executing instance. instance_id=" << PrintId(fis->instance_id())
      << " fragment_idx=" << fis->instance_ctx().fragment_idx
      << " per_fragment_instance_idx=" << fis->instance_ctx().per_fragment_instance_idx
      << " coord_state_idx=" << exec_rpc_params().coord_state_idx
      << " #in-flight="
      << ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT->GetValue();
  Status status = fis->Exec();
  ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT->Increment(-1L);
  VLOG_QUERY << "Instance completed. instance_id=" << PrintId(fis->instance_id())
      << " #in-flight="
      << ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT->GetValue()
      << " status=" << status;

  // Don't cancel other fragments here as the final report for "fis" may not have been
  // sent yet. Cancellation will happen in ReportExecStatus() after sending the final
  // report to the coordinator. Otherwise, the coordinator fragment may mark the status
  // of this backend as "CANCELLED", masking the original error.

  // decrement refcount taken in StartFInstances()
  ReleaseBackendResourceRefcount();
  // decrement refcount taken in StartFInstances()
  ExecEnv::GetInstance()->query_exec_mgr()->ReleaseQueryState(this);
}

void QueryState::Cancel() {
  VLOG_QUERY << "Cancel: query_id=" << PrintId(query_id());
  discard_result(WaitForPrepare());
  if (!is_cancelled_.CompareAndSwap(0, 1)) return;
  for (auto entry: fis_map_) entry.second->Cancel();
}

void QueryState::PublishFilter(const TPublishFilterParams& params) {
  if (!WaitForPrepare().ok()) return;
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
