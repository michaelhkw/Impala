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

#include "statestore/statestore-subscriber.h"

#include <sstream>
#include <utility>

#include <boost/algorithm/string/join.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread/lock_options.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "common/status.h"
#include "statestore/failure-detector.h"
#include "gen-cpp/StatestoreService_types.h"
#include "rpc/rpc-trace.h"
#include "rpc/thrift-util.h"
#include "statestore/statestore-service-client-wrapper.h"
#include "util/container-util.h"
#include "util/collection-metrics.h"
#include "util/debug-util.h"
#include "util/metrics.h"
#include "util/openssl-util.h"
#include "util/collection-metrics.h"
#include "util/time.h"

#include "common/names.h"

using boost::posix_time::seconds;
using boost::shared_lock;
using boost::shared_mutex;
using boost::try_to_lock;
using std::string;

using namespace apache::thrift;
using namespace apache::thrift::transport;
using namespace strings;

DEFINE_int32(statestore_subscriber_timeout_seconds, 30, "The amount of time (in seconds)"
     " that may elapse before the connection with the statestore is considered lost.");
DEFINE_int32(statestore_subscriber_cnxn_attempts, 10, "The number of times to retry an "
    "RPC connection to the statestore during initial registration. "
    "A setting of 0 means retry indefinitely");
DEFINE_int32(statestore_subscriber_cnxn_retry_interval_ms, 3000, "The interval, in ms, "
    "to wait between attempts to make an RPC connection to the statestore.");
DEFINE_int64_hidden(statestore_subscriber_recovery_grace_period_ms, 30000L, "Period "
    "after the last successful subscription attempt until the subscriber will be "
    "considered fully recovered. After a successful reconnect attempt, updates to the "
    "cluster membership will only become effective after this period has elapsed.");

DECLARE_string(debug_actions);
DECLARE_string(ssl_client_ca_certificate);
DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_string(ssl_private_key_password_cmd);
DECLARE_string(ssl_cipher_list);
DECLARE_string(ssl_minimum_version);

namespace impala {

// Used to identify the statestore in the failure detector
const string STATESTORE_ID = "STATESTORE";

// Template for metrics that measure the processing time for individual topics.
const string CALLBACK_METRIC_PATTERN = "statestore-subscriber.topic-$0.processing-time-s";

// Template for metrics that measure the interval between updates for individual topics.
const string UPDATE_INTERVAL_METRIC_PATTERN = "statestore-subscriber.topic-$0.update-interval";

// Duration, in ms, to sleep between attempts to reconnect to the
// statestore after a failure.
const int32_t SLEEP_INTERVAL_MS = 5000;

typedef ClientConnection<StatestoreServiceClientWrapper> StatestoreServiceConn;

// Proxy class for the subscriber heartbeat thrift API, which
// translates RPCs into method calls on the local subscriber object.
class StatestoreSubscriberThriftIf : public StatestoreSubscriberIf {
 public:
  StatestoreSubscriberThriftIf(StatestoreSubscriber* subscriber)
      : subscriber_(subscriber) { DCHECK(subscriber != NULL); }
  virtual void UpdateState(TUpdateStateResponse& response,
                           const TUpdateStateRequest& params) {
    TUniqueId registration_id;
    if (params.__isset.registration_id) {
      registration_id = params.registration_id;
    }

    subscriber_->UpdateState(params.topic_deltas, registration_id,
        &response.topic_updates, &response.skipped).ToThrift(&response.status);
    // Make sure Thrift thinks the field is set.
    response.__set_skipped(response.skipped);
  }

  virtual void Heartbeat(THeartbeatResponse& response, const THeartbeatRequest& request) {
    subscriber_->Heartbeat(request.registration_id);
  }

 private:
  StatestoreSubscriber* subscriber_;
};

StatestoreSubscriber::StatestoreSubscriber(const string& subscriber_id,
    const TNetworkAddress& heartbeat_address, const TNetworkAddress& statestore_address,
    MetricGroup* metrics)
    : subscriber_id_(subscriber_id),
      statestore_address_(statestore_address),
      thrift_iface_(new StatestoreSubscriberThriftIf(this)),
      failure_detector_(new TimeoutFailureDetector(
          seconds(FLAGS_statestore_subscriber_timeout_seconds),
          seconds(FLAGS_statestore_subscriber_timeout_seconds / 2))),
      client_cache_(new StatestoreClientCache(1, 0, 0, 0, "",
                !FLAGS_ssl_client_ca_certificate.empty())),
      metrics_(metrics->GetOrCreateChildGroup("statestore-subscriber")),
      heartbeat_address_(heartbeat_address) {
  connected_to_statestore_metric_ =
      metrics_->AddProperty("statestore-subscriber.connected", false);
  connection_failure_metric_ =
    metrics_->AddCounter("statestore-subscriber.num-connection-failures", 0);
  last_registration_duration_metric_ = metrics_->AddDoubleGauge(
      "statestore-subscriber.last-recovery-duration", 0.0);
  last_registration_time_metric_ = metrics_->AddProperty<string>(
      "statestore-subscriber.last-recovery-time", "N/A");
  topic_update_interval_metric_ = StatsMetric<double>::CreateAndRegister(metrics_,
      "statestore-subscriber.topic-update-interval-time");
  topic_update_duration_metric_ = StatsMetric<double>::CreateAndRegister(metrics_,
      "statestore-subscriber.topic-update-duration");
  heartbeat_interval_metric_ = StatsMetric<double>::CreateAndRegister(metrics_,
      "statestore-subscriber.heartbeat-interval-time");
  registration_id_metric_ = metrics->AddProperty<string>(
      "statestore-subscriber.registration-id", "N/A");
  client_cache_->InitMetrics(metrics, "statestore-subscriber.statestore");
}

Status StatestoreSubscriber::AddTopic(const Statestore::TopicId& topic_id,
    bool is_transient, bool populate_min_subscriber_topic_version,
    string filter_prefix, const UpdateCallback& callback) {
  lock_guard<shared_mutex> exclusive_lock(lock_);
  if (IsRegistered()) return Status("Subscriber already started, can't add new topic");
  TopicRegistration& registration = topic_registrations_[topic_id];
  registration.callbacks.push_back(callback);
  if (registration.processing_time_metric == nullptr) {
    registration.processing_time_metric = StatsMetric<double>::CreateAndRegister(metrics_,
        CALLBACK_METRIC_PATTERN, topic_id);
    registration.update_interval_metric = StatsMetric<double>::CreateAndRegister(metrics_,
        UPDATE_INTERVAL_METRIC_PATTERN, topic_id);
    registration.update_interval_timer.Start();
  }
  registration.is_transient = is_transient;
  registration.populate_min_subscriber_topic_version =
      populate_min_subscriber_topic_version;
  registration.filter_prefix = std::move(filter_prefix);
  return Status::OK();
}

Status StatestoreSubscriber::Register() {
  static int32_t attempt = 0; // used for debug actions only
  if (UNLIKELY(attempt++ == 0)) {
    RETURN_IF_ERROR(DebugAction(FLAGS_debug_actions, "REGISTER_SUBSCRIBER_FIRST_ATTEMPT"));
  }
  Status client_status;
  StatestoreServiceConn client(client_cache_.get(), statestore_address_, &client_status);
  RETURN_IF_ERROR(client_status);

  TRegisterSubscriberRequest request;
  for (const auto& registration : topic_registrations_) {
    TTopicRegistration thrift_topic;
    thrift_topic.topic_name = registration.first;
    thrift_topic.is_transient = registration.second.is_transient;
    thrift_topic.populate_min_subscriber_topic_version =
        registration.second.populate_min_subscriber_topic_version;
    thrift_topic.__set_filter_prefix(registration.second.filter_prefix);
    request.topic_registrations.push_back(thrift_topic);
  }

  request.subscriber_location = heartbeat_address_;
  request.subscriber_id = subscriber_id_;
  TRegisterSubscriberResponse response;
  RETURN_IF_ERROR(client.DoRpc(&StatestoreServiceClientWrapper::RegisterSubscriber,
      request, &response));

  Status status = Status(response.status);
  if (status.ok()) {
    connected_to_statestore_metric_->SetValue(true);
    last_registration_ms_.Store(MonotonicMillis());
  }
  if (response.__isset.registration_id) {
    lock_guard<mutex> l(registration_id_lock_);
    registration_id_ = response.registration_id;
    const string& registration_string = PrintId(registration_id_);
    registration_id_metric_->SetValue(registration_string);
    VLOG(1) << "Subscriber registration ID: " << registration_string;
  } else {
    VLOG(1) << "No subscriber registration ID received from statestore";
  }
  heartbeat_interval_timer_.Start();
  return status;
}

Status StatestoreSubscriber::Start() {
  {
    // Take the lock to ensure that, if a topic-update is received during registration
    // (perhaps because Register() has succeeded, but we haven't finished setting up state
    // on the client side), UpdateState() will reject the message.
    lock_guard<shared_mutex> exclusive_lock(lock_);
    LOG(INFO) << "Starting statestore subscriber " << subscriber_id_;

    // Backend must be started before registration
    boost::shared_ptr<TProcessor> processor(
        new StatestoreSubscriberProcessor(thrift_iface_));
    boost::shared_ptr<TProcessorEventHandler> event_handler(
        new RpcEventHandler("statestore-subscriber", metrics_));
    processor->setEventHandler(event_handler);

    ThriftServerBuilder builder(
        "StatestoreSubscriber", processor, heartbeat_address_.port);
    if (IsInternalTlsConfigured()) {
      SSLProtocol ssl_version;
      RETURN_IF_ERROR(
          SSLProtoVersions::StringToProtocol(FLAGS_ssl_minimum_version, &ssl_version));
      LOG(INFO) << "Enabling SSL for Statestore subscriber";
      builder.ssl(FLAGS_ssl_server_certificate, FLAGS_ssl_private_key)
          .pem_password_cmd(FLAGS_ssl_private_key_password_cmd)
          .ssl_version(ssl_version)
          .cipher_list(FLAGS_ssl_cipher_list);
    }

    ThriftServer* server;
    RETURN_IF_ERROR(builder.Build(&server));
    heartbeat_server_.reset(server);
    RETURN_IF_ERROR(heartbeat_server_->Start());
    heartbeat_address_.port = heartbeat_server_->port();
  }

  // Kick off the thread which will do the initial registration.
  RETURN_IF_ERROR(Thread::Create("statestore-subscriber", "registration-thread",
      &StatestoreSubscriber::RegistrationLoop, this, &registration_thread_));

  // Wait till initial registration is done.
  Status status = registration_status_.Get();
  if (status.ok()) {
    LOG(INFO) << Substitute("Statestore subscriber $0 initial registration succeeded",
        subscriber_id_);
  } else{
    LOG(INFO) << Substitute("Statestore subscriber $0 initial registration failed: $1",
        subscriber_id_, status.GetDetail());
  }
  return status;
}

void StatestoreSubscriber::RegistrationLoop() {
  failure_detector_->UpdateHeartbeat(STATESTORE_ID, true);

  // The following loop is the workhorse for making all registration calls to statestore.
  // Registration with statestore needs to happen in two scenarios:
  //
  // - initial startup of a statestore subscriber
  // - statestore has failed from the subscriber's persepective after initial registration
  //   (due to too many missing heartbeats)
  //
  // Every few seconds, the registration thread wakes up and checks if any of the above
  // scenerios is true and if so, try to connect and register with statestore.
  while (true) {
    const bool is_registered = IsRegistered();
    if (!is_registered ||
        failure_detector_->GetPeerState(STATESTORE_ID) == FailureDetector::FAILED) {
      // Take the class-wide lock_ to ensure mutual exclusion with any operations in flight.
      lock_guard<shared_mutex> exclusive_lock(lock_);
      MonotonicStopWatch registration_timer;
      registration_timer.Start();
      connected_to_statestore_metric_->SetValue(false);
      if (is_registered) {
        connection_failure_metric_->Increment(1);
        LOG(INFO) << Substitute("$0: connection with statestore lost, entering "
            "recovery mode", subscriber_id_);
      }
      uint32_t attempt_count = 1;
      bool connected = false;
      while (!connected) {
        LOG(INFO) << Substitute("Subscriber $0 trying to $1 with statestore, attempt: $2",
            subscriber_id_, is_registered ? "re-register" : "register", attempt_count++);

        Status status = Register();
        connected = status.ok();
        if (connected) {
          // Make sure to update failure detector so that we don't immediately fail on the
          // next loop while we're waiting for heartbeat messages to resume.
          failure_detector_->UpdateHeartbeat(STATESTORE_ID, true);
          if (!is_registered) {
            registration_status_.Set(status);
          } else {
            LOG(INFO) << "Reconnected to statestore. Exiting recovery mode";
          }
        } else {
          // Failed to register with statestore. Retry after sleeping for some time.
          LOG(INFO) << Substitute("Failed to $0 with statestore: $1",
              is_registered ? "re-register" : "register", status.GetDetail());
          // If it's initial registration, give up after a certain number of retries.
          if (!is_registered && FLAGS_statestore_subscriber_cnxn_attempts > 0 &&
              attempt_count >= FLAGS_statestore_subscriber_cnxn_attempts) {
            // Initial registration failed. Break out of the loop.
            registration_status_.Set(status);
            break;
          }
          SleepForMs(FLAGS_statestore_subscriber_cnxn_retry_interval_ms);
        }
        last_registration_duration_metric_->SetValue(
            registration_timer.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
      }
      // When we're successful in re-registering, we don't do anything
      // to re-send our updates to the statestore. It is the
      // responsibility of individual clients to post missing updates
      // back to the statestore. This saves a lot of complexity where
      // we would otherwise have to cache updates here.
      last_registration_time_metric_->SetValue(CurrentTimeString());
    }
    // No work needs to be done. Sleep for a bit and check again later.
    SleepForMs(SLEEP_INTERVAL_MS);
  }
}

Status StatestoreSubscriber::CheckRegistrationId(const RegistrationId& registration_id) {
  {
    lock_guard<mutex> r(registration_id_lock_);
    // If this subscriber has just started, the registration_id_ may not have been set
    // despite the statestore starting to send updates. The 'unset' RegistrationId is 0:0,
    // so we can differentiate between a) an early message from an eager statestore, and
    // b) a message that's targeted to a previous registration.
    if (registration_id_ != TUniqueId() && registration_id != registration_id_) {
      return Status(Substitute("Unexpected registration ID: $0, was expecting $1",
          PrintId(registration_id), PrintId(registration_id_)));
    }
  }

  return Status::OK();
}

void StatestoreSubscriber::Heartbeat(const RegistrationId& registration_id) {
  const Status& status = CheckRegistrationId(registration_id);
  if (status.ok()) {
    heartbeat_interval_metric_->Update(
        heartbeat_interval_timer_.Reset() / (1000.0 * 1000.0 * 1000.0));
    failure_detector_->UpdateHeartbeat(STATESTORE_ID, true);
  } else {
    VLOG_RPC << "Heartbeat: " << status.GetDetail();
  }
}

Status StatestoreSubscriber::UpdateState(const TopicDeltaMap& incoming_topic_deltas,
    const RegistrationId& registration_id, vector<TTopicDelta>* subscriber_topic_updates,
    bool* skipped) {
  RETURN_IF_ERROR(CheckRegistrationId(registration_id));

  // Put the updates into ascending order of topic name to match the lock acquisition
  // order of TopicRegistration::update_lock.
  vector<const TTopicDelta*> deltas_to_process;
  for (auto& delta : incoming_topic_deltas) deltas_to_process.push_back(&delta.second);
  sort(deltas_to_process.begin(), deltas_to_process.end(),
      [](const TTopicDelta* left, const TTopicDelta* right) {
        return left->topic_name < right->topic_name;
      });
  // Unique locks to hold the 'update_lock' for each entry in 'deltas_to_process'. Locks
  // are held until we finish processing the update to prevent any races with concurrent
  // updates for the same topic.
  vector<unique_lock<mutex>> topic_update_locks(deltas_to_process.size());

  // We don't want to block here because this is an RPC, and delaying the return causes
  // the statestore to delay sending further messages. The only time that lock_ might be
  // taken exclusively is if the subscriber is recovering, and has the lock held during
  // RecoveryModeChecker(). In this case we skip all topics and don't update any metrics.
  //
  // UpdateState() may run concurrently with itself in two cases:
  // a) disjoint sets of topics are being updated. In that case the updates can proceed
  // concurrently.
  // b) another update for the same topics is still being processed (i.e. is still in
  // UpdateState()). This could happen only when the subscriber has re-registered, and
  // the statestore is still sending an update for the previous registration. In this
  // case, we notices that the per-topic 'update_lock' is held, skip processing all
  // of the topic updates and set *skipped = true so that the statestore will retry this
  // update in the future.
  //
  // TODO: Consider returning an error in this case so that the statestore will eventually
  // stop sending updates even if re-registration fails.
  shared_lock<shared_mutex> l(lock_, try_to_lock);
  if (!l.owns_lock()) {
    *skipped = true;
    return Status::OK();
  }

  // First, acquire all the topic locks and update the interval metrics
  // Record the time we received the update before doing any processing to avoid including
  // processing time in the interval metrics.
  for (int i = 0; i < deltas_to_process.size(); ++i) {
    const TTopicDelta& delta = *deltas_to_process[i];
    auto it = topic_registrations_.find(delta.topic_name);
    // Skip updates to unregistered topics.
    if (it == topic_registrations_.end()) {
      LOG(ERROR) << "Unexpected delta update for unregistered topic: "
                 << delta.topic_name;
      continue;
    }
    TopicRegistration& registration = it->second;
    unique_lock<mutex> ul(registration.update_lock, try_to_lock);
    if (!ul.owns_lock()) {
      // Statestore sent out concurrent topic updates. Avoid blocking the RPC by skipping
      // the topic.
      LOG(ERROR) << "Could not acquire lock for topic " << delta.topic_name << ". "
                 << "Skipping update.";
      *skipped = true;
      return Status::OK();
    }
    double interval =
        registration.update_interval_timer.ElapsedTime() / (1000.0 * 1000.0 * 1000.0);
    registration.update_interval_metric->Update(interval);
    topic_update_interval_metric_->Update(interval);

    // Hold onto lock until we've finished processing the update.
    topic_update_locks[i].swap(ul);
  }

  MonotonicStopWatch sw;
  sw.Start();
  // Second, do the actual processing of topic updates that we validated and acquired
  // locks for above.
  for (int i = 0; i < deltas_to_process.size(); ++i) {
    if (!topic_update_locks[i].owns_lock()) continue;

    const TTopicDelta& delta = *deltas_to_process[i];
    auto it = topic_registrations_.find(delta.topic_name);
    DCHECK(it != topic_registrations_.end());
    TopicRegistration& registration = it->second;
    if (delta.is_delta && registration.current_topic_version != -1
      && delta.from_version != registration.current_topic_version) {
      // Received a delta update for the wrong version. Log an error and send back the
      // expected version to the statestore to request a new update with the correct
      // version range.
      LOG(ERROR) << "Unexpected delta update to topic '" << delta.topic_name << "' of "
                 << "version range (" << delta.from_version << ":"
                 << delta.to_version << "]. Expected delta start version: "
                 << registration.current_topic_version;

      subscriber_topic_updates->push_back(TTopicDelta());
      TTopicDelta& update = subscriber_topic_updates->back();
      update.topic_name = delta.topic_name;
      update.__set_from_version(registration.current_topic_version);
      continue;
    }
    // The topic version in the update is valid, process the update.
    MonotonicStopWatch update_callback_sw;
    update_callback_sw.Start();
    for (const UpdateCallback& callback : registration.callbacks) {
      callback(incoming_topic_deltas, subscriber_topic_updates);
    }
    update_callback_sw.Stop();
    registration.current_topic_version = delta.to_version;
    registration.processing_time_metric->Update(
        sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
  }

  // Third and finally, reset the interval timers so they correctly measure the
  // time between RPCs, excluding processing time.
  for (int i = 0; i < deltas_to_process.size(); ++i) {
    if (!topic_update_locks[i].owns_lock()) continue;

    const TTopicDelta& delta = *deltas_to_process[i];
    auto it = topic_registrations_.find(delta.topic_name);
    DCHECK(it != topic_registrations_.end());
    TopicRegistration& registration = it->second;
    registration.update_interval_timer.Reset();
  }
  sw.Stop();
  topic_update_duration_metric_->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
  return Status::OK();
}

bool StatestoreSubscriber::IsInPostRecoveryGracePeriod() const {
  bool has_failed_before = connection_failure_metric_->GetValue() > 0;
  bool in_grace_period = MilliSecondsSinceLastRegistration()
      < FLAGS_statestore_subscriber_recovery_grace_period_ms;
  return has_failed_before && in_grace_period;
}

}
