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

namespace kudu { namespace rpc { class RpcContext; } }

namespace impala {

class RpcMgr;

/// Handles data flow between execution nodes: transmitting tuples and filters.
class DataStreamService : public DataStreamServiceIf {
 public:
  DataStreamService(RpcMgr* rpc_mgr);

  virtual void EndDataStream(const EndDataStreamRequestPB* request,
      EndDataStreamResponsePB* response, kudu::rpc::RpcContext* context);

  virtual void TransmitData(const TransmitDataRequestPB* request,
      TransmitDataResponsePB* response, kudu::rpc::RpcContext* context);
};

}

#endif
