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

#include "rpc/rpc-mgr.inline.h"
#include "common/init.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_header.pb.h"
#include "rpc/rpc_test.proxy.h"
#include "rpc/rpc_test.service.h"
#include "testutil/gtest-util.h"
#include "util/network-util.h"

#include <functional>

#include "common/names.h"

using namespace impala;

using kudu::rpc::ServiceIf;
using kudu::rpc::RpcController;
using kudu::rpc::RpcContext;
using kudu::rpc::ErrorStatusPB;

using namespace std;

DECLARE_int32(num_reactor_threads);
DECLARE_int32(num_acceptor_threads);
DECLARE_int32(krpc_port);

namespace impala {

static int32_t SERVICE_PORT = FindUnusedEphemeralPort(nullptr);

class RpcMgrTest : public testing::Test {
 protected:
  RpcMgr rpc_mgr_;

  virtual void SetUp() {
    ASSERT_OK(rpc_mgr_.Init());
  }

  virtual void TearDown() {
    rpc_mgr_.UnregisterServices();
  }
};

class PingServiceImpl : public PingServiceIf {
 public:
  // 'cb' is a callback used by tests to inject custom behaviour into the RPC handler.
  PingServiceImpl(const scoped_refptr<kudu::MetricEntity>& entity,
      const scoped_refptr<kudu::rpc::ResultTracker> tracker,
      std::function<void(RpcContext*)> cb =
          [](RpcContext* ctx) { ctx->RespondSuccess(); })
    : PingServiceIf(entity, tracker), cb_(cb) {}

  virtual void Ping(
      const PingRequestPB* request, PingResponsePB* response, RpcContext* context) {
    response->set_int_response(42);
    cb_(context);
  }

 private:
  std::function<void(RpcContext*)> cb_;
};

TEST_F(RpcMgrTest, ServiceSmokeTest) {
  // Test that a service can be started, and will respond to requests.
  unique_ptr<ServiceIf> impl(
      new PingServiceImpl(rpc_mgr_.metric_entity(), rpc_mgr_.result_tracker()));
  ASSERT_OK(rpc_mgr_.RegisterService(10, 1024, move(impl)));

  FLAGS_num_acceptor_threads = 2;
  FLAGS_num_reactor_threads = 2;
  ASSERT_OK(rpc_mgr_.StartServices(SERVICE_PORT));

  unique_ptr<PingServiceProxy> proxy;
  ASSERT_OK(rpc_mgr_.GetProxy<PingServiceProxy>(
      MakeNetworkAddress("localhost", SERVICE_PORT), &proxy));

  PingRequestPB request;
  PingResponsePB response;
  RpcController controller;
  proxy->Ping(request, &response, &controller);
  ASSERT_EQ(response.int_response(), 42);
  rpc_mgr_.UnregisterServices();
}
}

IMPALA_TEST_MAIN();
