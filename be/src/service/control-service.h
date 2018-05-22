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

#ifndef IMPALA_SERVICE_CONTROL_SERVICE_H
#define IMPALA_SERVICE_CONTROL_SERVICE_H

#include "gen-cpp/control_service.service.h"

#include "common/status.h"

namespace kudu {
namespace rpc {
class RpcContext;
} // namespace rpc
} // namespace kudu

namespace impala {

class MemTracker;
class MetricGroup;

/// This singleton class implements service for managing execution of queries in Impala.
class ControlService : public ControlServiceIf {
  public:
   ControlService(MetricGroup* metric_group);

   /// Initializes the service by registering it with the singleton RPC manager.
   /// This mustn't be called until RPC manager has been initialized.
   Status Init();

   /// Returns true iff the 'remote_user' in 'context' is authorized to access
   /// ControlService. On denied access, the RPC is replied to with an error message.
   /// Authorization is enforced only when Kerberos is enabled.
   virtual bool Authorize(const google::protobuf::Message* req,
       google::protobuf::Message* resp, kudu::rpc::RpcContext* context) override;

   /// Updates the coordinator of the query status of the backend encoded in 'req'.
   virtual void ReportExecStatus(const ReportExecStatusRequestPB *req,
       ReportExecStatusResponsePB *resp, kudu::rpc::RpcContext *context) override;

  private:
   /// Tracks the memory usage of payload in the service queue.
   std::unique_ptr<MemTracker> mem_tracker_;

   /// Helper for serializing 'status' as part of 'response'. Also releases memory
   /// of the RPC payload previously accounted towards the internal memory tracker.
   template<typename ResponsePBType>
   void RespondAndReleaseRpc(const Status& status, ResponsePBType* response,
       kudu::rpc::RpcContext* rpc_context);
};

} // namespace impala

#endif // IMPALA_SERVICE_CONTROL_SERVICE_H
