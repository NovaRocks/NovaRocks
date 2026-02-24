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
#include "compat.h"

#include <atomic>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <butil/endpoint.h>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include "internal_service.pb.h"
#include "lake_service.pb.h"
#include "status.pb.h"

#include "Data_types.h"
#include "InternalService_types.h"
#include "StatusCode_types.h"

namespace {

enum class AttachmentProtocol {
    Binary,
    Compact,
    Json,
};

AttachmentProtocol parse_attachment_protocol(const std::string& s) {
    std::string lower;
    lower.reserve(s.size());
    for (char ch : s) {
        lower.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
    }
    if (lower == "compact") return AttachmentProtocol::Compact;
    if (lower == "json") return AttachmentProtocol::Json;
    return AttachmentProtocol::Binary;
}

std::shared_ptr<apache::thrift::protocol::TProtocol> make_protocol(
        AttachmentProtocol proto,
        const std::shared_ptr<apache::thrift::transport::TTransport>& transport) {
    switch (proto) {
    case AttachmentProtocol::Compact:
        return std::make_shared<apache::thrift::protocol::TCompactProtocol>(transport);
    case AttachmentProtocol::Json:
        return std::make_shared<apache::thrift::protocol::TJSONProtocol>(transport);
    case AttachmentProtocol::Binary:
    default:
        return std::make_shared<apache::thrift::protocol::TBinaryProtocol>(transport);
    }
}

template <typename ThriftStruct>
bool thrift_deserialize(const std::string& bytes,
                        AttachmentProtocol proto,
                        ThriftStruct* out,
                        std::string* err) {
    if (out == nullptr) {
        if (err) *err = "out is null";
        return false;
    }
    try {
        auto transport = std::make_shared<apache::thrift::transport::TMemoryBuffer>(
                reinterpret_cast<uint8_t*>(const_cast<char*>(bytes.data())),
                static_cast<uint32_t>(bytes.size()));
        auto protocol = make_protocol(proto, transport);
        out->read(protocol.get());
        return true;
    } catch (const std::exception& e) {
        if (err) *err = e.what();
        return false;
    }
}

template <typename ThriftStruct>
bool thrift_serialize(const ThriftStruct& in,
                      AttachmentProtocol proto,
                      std::string* out,
                      std::string* err) {
    if (out == nullptr) {
        if (err) *err = "out is null";
        return false;
    }
    try {
        auto transport = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
        auto protocol = make_protocol(proto, transport);
        in.write(protocol.get());
        *out = transport->getBufferAsString();
        return true;
    } catch (const std::exception& e) {
        if (err) *err = e.what();
        return false;
    }
}

void status_ok(starrocks::StatusPB* status) {
    status->set_status_code(static_cast<int32_t>(starrocks::TStatusCode::OK));
}

void status_err(starrocks::StatusPB* status, starrocks::TStatusCode::type code, const std::string& msg) {
    status->set_status_code(static_cast<int32_t>(code));
    status->add_error_msgs(msg);
}

struct UniqueIdKey {
    int64_t hi;
    int64_t lo;

    bool operator==(const UniqueIdKey& other) const { return hi == other.hi && lo == other.lo; }
};

std::string format_unique_id(const UniqueIdKey& id) {
    return std::to_string(id.hi) + ":" + std::to_string(id.lo);
}

struct ResultPacket {
    std::string thrift_bytes;
    int64_t packet_seq = 0;
    bool eos = true;
};

ResultPacket make_empty_eos_packet(AttachmentProtocol proto, int64_t packet_seq, std::string* err) {
    starrocks::TResultBatch batch;
    batch.is_compressed = false;
    batch.packet_seq = packet_seq;

    std::string bytes;
    (void)thrift_serialize(batch, proto, &bytes, err);

    ResultPacket pkt;
    pkt.thrift_bytes = std::move(bytes);
    pkt.packet_seq = packet_seq;
    pkt.eos = true;
    return pkt;
}

std::string take_rust_buf_string(NovaRocksRustBuf* buf) {
    if (buf == nullptr) {
        return "";
    }
    std::string out;
    if (buf->ptr != nullptr && buf->len > 0) {
        out.assign(reinterpret_cast<char*>(buf->ptr), buf->len);
    }
    if (buf->ptr != nullptr) {
        novarocks_rs_free_buf(buf->ptr, buf->len);
        buf->ptr = nullptr;
        buf->len = 0;
    }
    return out;
}

class InternalServiceImpl final : public starrocks::PInternalService {
public:
    explicit InternalServiceImpl(NovaRocksCompatConfig cfg) : cfg_(cfg) {}

    void exec_plan_fragment(google::protobuf::RpcController* controller,
                            const starrocks::PExecPlanFragmentRequest* request,
                            starrocks::PExecPlanFragmentResult* response,
                            google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto* cntl = static_cast<brpc::Controller*>(controller);
        std::string proto_str = request != nullptr && request->has_attachment_protocol()
                                        ? request->attachment_protocol()
                                        : "binary";
        auto proto = parse_attachment_protocol(proto_str);

        std::string attachment = cntl->request_attachment().to_string();

        if (proto != AttachmentProtocol::Binary) {
            status_err(response->mutable_status(), starrocks::TStatusCode::INVALID_ARGUMENT,
                       "only attachment_protocol=binary is supported for now");
            return;
        }

        starrocks::TExecPlanFragmentParams params;
        std::string err;
        if (!thrift_deserialize(attachment, proto, &params, &err)) {
            status_err(response->mutable_status(), starrocks::TStatusCode::INTERNAL_ERROR,
                       "failed to deserialize TExecPlanFragmentParams: " + err);
            return;
        }
        if (!params.__isset.params) {
            status_err(response->mutable_status(), starrocks::TStatusCode::INVALID_ARGUMENT,
                       "missing fragment_instance_id in TExecPlanFragmentParams.params");
            return;
        }

        int32_t rc = novarocks_rs_submit_exec_plan_fragment(
                reinterpret_cast<const uint8_t*>(attachment.data()), attachment.size());
        if (rc != 0) {
            status_err(response->mutable_status(), starrocks::TStatusCode::INTERNAL_ERROR,
                       "rust submit_exec_plan_fragment failed");
            return;
        }

        status_ok(response->mutable_status());
    }

    void exec_batch_plan_fragments(google::protobuf::RpcController* controller,
                                   const ::starrocks::PExecBatchPlanFragmentsRequest* request,
                                   ::starrocks::PExecBatchPlanFragmentsResult* response,
                                   ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto* cntl = static_cast<brpc::Controller*>(controller);
        std::string proto_str = request != nullptr && request->has_attachment_protocol()
                                        ? request->attachment_protocol()
                                        : "binary";
        auto proto = parse_attachment_protocol(proto_str);

        std::string attachment = cntl->request_attachment().to_string();

        if (proto != AttachmentProtocol::Binary) {
            status_err(response->mutable_status(), starrocks::TStatusCode::INVALID_ARGUMENT,
                       "only attachment_protocol=binary is supported for now");
            return;
        }

        starrocks::TExecBatchPlanFragmentsParams batch;
        std::string err;
        if (!thrift_deserialize(attachment, proto, &batch, &err)) {
            status_err(response->mutable_status(), starrocks::TStatusCode::INTERNAL_ERROR,
                       "failed to deserialize TExecBatchPlanFragmentsParams: " + err);
            return;
        }
        (void)batch;

        int32_t rc = novarocks_rs_submit_exec_batch_plan_fragments(
                reinterpret_cast<const uint8_t*>(attachment.data()), attachment.size());
        if (rc != 0) {
            status_err(response->mutable_status(), starrocks::TStatusCode::INTERNAL_ERROR,
                       "rust submit_exec_batch_plan_fragments failed");
            return;
        }
        status_ok(response->mutable_status());
    }

    void fetch_data(google::protobuf::RpcController* controller,
                    const starrocks::PFetchDataRequest* request,
                    starrocks::PFetchDataResult* response,
                    google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto* cntl = static_cast<brpc::Controller*>(controller);
        UniqueIdKey finst_id{0, 0};
        if (request != nullptr) {
            finst_id.hi = request->finst_id().hi();
            finst_id.lo = request->finst_id().lo();
        }

        ResultPacket pkt;
        std::string err;
        NovaRocksRustBuf batch_buf;
        batch_buf.ptr = nullptr;
        batch_buf.len = 0;
        NovaRocksRustBuf err_buf;
        err_buf.ptr = nullptr;
        err_buf.len = 0;
        int64_t packet_seq = 0;
        bool eos = true;
        int32_t rc = novarocks_rs_fetch_result_batch(
                finst_id.hi, finst_id.lo, &packet_seq, &eos, &batch_buf, &err_buf);

        auto take_err = [&]() -> std::string {
            std::string msg;
            if (err_buf.ptr != nullptr && err_buf.len > 0) {
                msg.assign(reinterpret_cast<char*>(err_buf.ptr), err_buf.len);
            }
            if (err_buf.ptr != nullptr) {
                novarocks_rs_free_buf(err_buf.ptr, err_buf.len);
                err_buf.ptr = nullptr;
                err_buf.len = 0;
            }
            if (msg.empty()) {
                msg = "unknown error";
            }
            return msg;
        };
        
        if (rc != 0) {
            std::string msg = take_err();
            starrocks::TStatusCode::type code = starrocks::TStatusCode::INTERNAL_ERROR;
            switch (rc) {
            case 1: // NOT_FOUND
                code = starrocks::TStatusCode::NOT_FOUND;
                break;
            case 2: // CANCELLED
                code = starrocks::TStatusCode::CANCELLED;
                break;
            case 4: // TIMEOUT
                code = starrocks::TStatusCode::TIMEOUT;
                break;
            case 3: // FAILED
            default:
                code = starrocks::TStatusCode::INTERNAL_ERROR;
                break;
            }
            std::cerr << "[WARN] fetch_data failed, finst_id=" << format_unique_id(finst_id)
                      << " rc=" << rc << " msg=" << msg << std::endl;
            status_err(response->mutable_status(), code, msg);
            return;
        }
        
        // Handle success
        if (batch_buf.ptr != nullptr && batch_buf.len > 0) {
            pkt.thrift_bytes.assign(reinterpret_cast<char*>(batch_buf.ptr), batch_buf.len);
            pkt.packet_seq = packet_seq;
            pkt.eos = eos;
        }
        if (batch_buf.ptr != nullptr) {
            novarocks_rs_free_buf(batch_buf.ptr, batch_buf.len);
        }
        
        if (pkt.thrift_bytes.empty()) {
            // Got success but empty data, return eos packet
            pkt = make_empty_eos_packet(AttachmentProtocol::Binary, /*packet_seq=*/packet_seq, &err);
            if (!err.empty()) {
                std::cerr << "[ERROR] fetch_data build empty packet failed: " << err << std::endl;
            }
            pkt.eos = eos;
        }

        std::cerr << "[DEBUG] fetch_data called, remote=" << cntl->remote_side()
                  << " finst_id=" << format_unique_id(finst_id) << " packet_seq=" << pkt.packet_seq
                  << " eos=" << (pkt.eos ? "true" : "false") << " attachment_bytes=" << pkt.thrift_bytes.size()
                  << std::endl;

        status_ok(response->mutable_status());
        response->set_packet_seq(pkt.packet_seq);
        response->set_eos(pkt.eos);
        cntl->response_attachment().append(pkt.thrift_bytes);
    }

    void cancel_plan_fragment(google::protobuf::RpcController* controller,
                              const starrocks::PCancelPlanFragmentRequest* request,
                              starrocks::PCancelPlanFragmentResult* response,
                              google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto* cntl = static_cast<brpc::Controller*>(controller);
        UniqueIdKey finst_id{0, 0};
        if (request != nullptr) {
            finst_id.hi = request->finst_id().hi();
            finst_id.lo = request->finst_id().lo();
        }
        novarocks_rs_cancel(finst_id.hi, finst_id.lo);
        std::cerr << "[INFO] cancel_plan_fragment called, remote=" << cntl->remote_side()
                  << " finst_id=" << format_unique_id(finst_id) << std::endl;
        status_ok(response->mutable_status());
    }

    void trigger_profile_report(google::protobuf::RpcController* controller,
                                const starrocks::PTriggerProfileReportRequest* request,
                                starrocks::PTriggerProfileReportResult* response,
                                google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto* cntl = static_cast<brpc::Controller*>(controller);
        if (request == nullptr) {
            status_err(response->mutable_status(), starrocks::TStatusCode::INVALID_ARGUMENT,
                       "missing request");
            return;
        }

        // StarRocks FE does not actively call trigger_profile_report in production. Runtime
        // profiles are pushed by BE via reportExecStatus, so this RPC is treated as a no-op.
        std::cerr << "[INFO] trigger_profile_report ignored, remote=" << cntl->remote_side()
                  << " instances=" << request->instance_ids_size() << std::endl;
        status_ok(response->mutable_status());
    }

private:
    NovaRocksCompatConfig cfg_;
};

class LakeServiceImpl final : public starrocks::LakeService {
public:
    LakeServiceImpl() = default;

    void publish_version(google::protobuf::RpcController* controller,
                         const starrocks::PublishVersionRequest* request,
                         starrocks::PublishVersionResponse* response,
                         google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto* cntl = static_cast<brpc::Controller*>(controller);
        if (request == nullptr || response == nullptr) {
            if (response != nullptr) {
                response->clear_failed_tablets();
                status_err(response->mutable_status(), starrocks::TStatusCode::INVALID_ARGUMENT,
                           "missing publish_version request/response");
            }
            cntl->SetFailed("missing publish_version request/response");
            return;
        }

        std::string err;
        if (!invoke_publish_version(*request, response, &err)) {
            response->clear_failed_tablets();
            status_err(response->mutable_status(), starrocks::TStatusCode::INTERNAL_ERROR, err);
            cntl->SetFailed(err);
            std::cerr << "[WARN] lake publish_version failed, remote=" << cntl->remote_side() << " err=" << err
                      << std::endl;
            return;
        }

        if (!response->has_status()) {
            status_ok(response->mutable_status());
        }
        std::cerr << "[DEBUG] lake publish_version, remote=" << cntl->remote_side()
                  << " tablets=" << request->tablet_ids_size() << " txn_infos=" << request->txn_infos_size()
                  << " failed_tablets=" << response->failed_tablets_size()
                  << std::endl;
    }

    void aggregate_publish_version(google::protobuf::RpcController* controller,
                                   const starrocks::AggregatePublishVersionRequest* request,
                                   starrocks::PublishVersionResponse* response,
                                   google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto* cntl = static_cast<brpc::Controller*>(controller);
        if (request == nullptr || response == nullptr) {
            if (response != nullptr) {
                response->clear_failed_tablets();
                status_err(response->mutable_status(), starrocks::TStatusCode::INVALID_ARGUMENT,
                           "missing aggregate_publish_version request/response");
            }
            cntl->SetFailed("missing aggregate_publish_version request/response");
            return;
        }
        response->clear_failed_tablets();
        status_ok(response->mutable_status());

        for (const auto& sub_request : request->publish_reqs()) {
            starrocks::PublishVersionResponse sub_response;
            std::string err;
            if (!invoke_publish_version(sub_request, &sub_response, &err)) {
                for (auto tablet_id : sub_request.tablet_ids()) {
                    response->add_failed_tablets(tablet_id);
                }
                status_err(response->mutable_status(), starrocks::TStatusCode::INTERNAL_ERROR, err);
                continue;
            }
            for (auto tablet_id : sub_response.failed_tablets()) {
                response->add_failed_tablets(tablet_id);
            }
            for (const auto& it : sub_response.compaction_scores()) {
                (*response->mutable_compaction_scores())[it.first] = it.second;
            }
            for (const auto& it : sub_response.tablet_row_nums()) {
                (*response->mutable_tablet_row_nums())[it.first] = it.second;
            }
            for (const auto& it : sub_response.tablet_metas()) {
                (*response->mutable_tablet_metas())[it.first].CopyFrom(it.second);
            }
            for (const auto& it : sub_response.tablet_ranges()) {
                (*response->mutable_tablet_ranges())[it.first].CopyFrom(it.second);
            }
        }
        std::cerr << "[DEBUG] lake aggregate_publish_version, remote=" << cntl->remote_side()
                  << " sub_requests=" << request->publish_reqs_size()
                  << " failed_tablets=" << response->failed_tablets_size()
                  << std::endl;
    }

    void publish_log_version(google::protobuf::RpcController* controller,
                             const starrocks::PublishLogVersionRequest* request,
                             starrocks::PublishLogVersionResponse* response,
                             google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto* cntl = static_cast<brpc::Controller*>(controller);
        if (request == nullptr || response == nullptr) {
            if (response != nullptr) {
                response->clear_failed_tablets();
            }
            cntl->SetFailed("missing publish_log_version request/response");
            return;
        }
        response->clear_failed_tablets();

        if (request->tablet_ids_size() == 0) {
            return;
        }
        if (!request->has_version() || request->version() <= 0) {
            cntl->SetFailed("publish_log_version missing positive version");
            return;
        }
        if (!request->has_txn_info() && !request->has_txn_id()) {
            cntl->SetFailed("publish_log_version requires txn_info or txn_id");
            return;
        }

        starrocks::PublishVersionRequest publish_req;
        for (auto tablet_id : request->tablet_ids()) {
            publish_req.add_tablet_ids(tablet_id);
        }
        publish_req.set_base_version(request->version() - 1);
        publish_req.set_new_version(request->version());
        if (request->has_txn_info()) {
            *publish_req.add_txn_infos() = request->txn_info();
        } else {
            publish_req.add_txn_ids(request->txn_id());
        }

        starrocks::PublishVersionResponse publish_resp;
        std::string err;
        if (!invoke_publish_version(publish_req, &publish_resp, &err)) {
            for (auto tablet_id : request->tablet_ids()) {
                response->add_failed_tablets(tablet_id);
            }
            cntl->SetFailed(err);
            return;
        }
        for (auto tablet_id : publish_resp.failed_tablets()) {
            response->add_failed_tablets(tablet_id);
        }
        std::cerr << "[DEBUG] lake publish_log_version, remote=" << cntl->remote_side()
                  << " tablets=" << request->tablet_ids_size() << " version=" << request->version()
                  << " failed_tablets=" << response->failed_tablets_size()
                  << std::endl;
    }

    void publish_log_version_batch(google::protobuf::RpcController* controller,
                                   const starrocks::PublishLogVersionBatchRequest* request,
                                   starrocks::PublishLogVersionResponse* response,
                                   google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto* cntl = static_cast<brpc::Controller*>(controller);
        if (request == nullptr || response == nullptr) {
            if (response != nullptr) {
                response->clear_failed_tablets();
            }
            cntl->SetFailed("missing publish_log_version_batch request/response");
            return;
        }
        response->clear_failed_tablets();

        if (request->tablet_ids_size() == 0) {
            return;
        }

        const int version_cnt = request->versions_size();
        if (version_cnt <= 0) {
            cntl->SetFailed("publish_log_version_batch requires versions");
            return;
        }

        std::vector<starrocks::TxnInfoPB> txn_infos;
        txn_infos.reserve(version_cnt);
        if (request->txn_infos_size() > 0) {
            if (request->txn_infos_size() != version_cnt) {
                cntl->SetFailed("publish_log_version_batch txn_infos size mismatches versions");
                return;
            }
            for (const auto& info : request->txn_infos()) {
                if (!info.has_txn_id() || info.txn_id() <= 0) {
                    cntl->SetFailed("publish_log_version_batch txn_info has invalid txn_id");
                    return;
                }
                txn_infos.push_back(info);
            }
        } else {
            if (request->txn_ids_size() != version_cnt) {
                cntl->SetFailed("publish_log_version_batch txn_ids size mismatches versions");
                return;
            }
            for (int i = 0; i < request->txn_ids_size(); ++i) {
                auto txn_id = request->txn_ids(i);
                if (txn_id <= 0) {
                    cntl->SetFailed("publish_log_version_batch has non-positive txn_id");
                    return;
                }
                starrocks::TxnInfoPB info;
                info.set_txn_id(txn_id);
                txn_infos.push_back(std::move(info));
            }
        }

        for (int i = 0; i < version_cnt; ++i) {
            auto version = request->versions(i);
            if (version <= 0) {
                for (auto tablet_id : request->tablet_ids()) {
                    response->add_failed_tablets(tablet_id);
                }
                cntl->SetFailed("publish_log_version_batch has non-positive version");
                continue;
            }

            starrocks::PublishVersionRequest publish_req;
            for (auto tablet_id : request->tablet_ids()) {
                publish_req.add_tablet_ids(tablet_id);
            }
            publish_req.set_base_version(version - 1);
            publish_req.set_new_version(version);
            *publish_req.add_txn_infos() = txn_infos[i];

            starrocks::PublishVersionResponse publish_resp;
            std::string err;
            if (!invoke_publish_version(publish_req, &publish_resp, &err)) {
                for (auto tablet_id : request->tablet_ids()) {
                    response->add_failed_tablets(tablet_id);
                }
                cntl->SetFailed(err);
                continue;
            }
            for (auto tablet_id : publish_resp.failed_tablets()) {
                response->add_failed_tablets(tablet_id);
            }
        }
        std::cerr << "[DEBUG] lake publish_log_version_batch, remote=" << cntl->remote_side()
                  << " tablets=" << request->tablet_ids_size()
                  << " txn_infos=" << request->txn_infos_size()
                  << " versions=" << request->versions_size()
                  << " failed_tablets=" << response->failed_tablets_size()
                  << std::endl;
    }

    void abort_txn(google::protobuf::RpcController* controller,
                   const starrocks::AbortTxnRequest* request,
                   starrocks::AbortTxnResponse* response,
                   google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto* cntl = static_cast<brpc::Controller*>(controller);
        if (request == nullptr || response == nullptr) {
            if (response != nullptr) {
                response->clear_failed_tablets();
            }
            cntl->SetFailed("missing abort_txn request/response");
            return;
        }
        std::string err;
        if (!invoke_abort_txn(*request, response, &err)) {
            response->clear_failed_tablets();
            cntl->SetFailed(err);
            std::cerr << "[WARN] lake abort_txn failed, remote=" << cntl->remote_side() << " err=" << err
                      << std::endl;
            return;
        }
        std::cerr << "[DEBUG] lake abort_txn, remote=" << cntl->remote_side()
                  << " tablets=" << request->tablet_ids_size() << " txn_infos=" << request->txn_infos_size()
                  << " failed_tablets=" << response->failed_tablets_size()
                  << std::endl;
    }

    void delete_tablet(google::protobuf::RpcController* controller,
                       const starrocks::DeleteTabletRequest* request,
                       starrocks::DeleteTabletResponse* response,
                       google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto* cntl = static_cast<brpc::Controller*>(controller);
        if (request == nullptr || response == nullptr) {
            if (response != nullptr) {
                status_err(response->mutable_status(), starrocks::TStatusCode::INVALID_ARGUMENT,
                           "missing delete_tablet request/response");
            }
            cntl->SetFailed("missing delete_tablet request/response");
            return;
        }
        if (request->tablet_ids_size() == 0) {
            status_err(response->mutable_status(), starrocks::TStatusCode::INVALID_ARGUMENT, "missing tablet_ids");
            cntl->SetFailed("missing tablet_ids");
            return;
        }

        std::string err;
        if (!invoke_delete_tablet(*request, response, &err)) {
            status_err(response->mutable_status(), starrocks::TStatusCode::INTERNAL_ERROR, err);
            response->clear_failed_tablets();
            for (auto tablet_id : request->tablet_ids()) {
                response->add_failed_tablets(tablet_id);
            }
            cntl->SetFailed(err);
            std::cerr << "[WARN] lake delete_tablet failed, remote=" << cntl->remote_side() << " err=" << err
                      << std::endl;
            return;
        }
        if (!response->has_status()) {
            status_ok(response->mutable_status());
        }
        if (response->status().status_code() != static_cast<int32_t>(starrocks::TStatusCode::OK) &&
            response->failed_tablets_size() == 0) {
            for (auto tablet_id : request->tablet_ids()) {
                response->add_failed_tablets(tablet_id);
            }
        }
        std::cerr << "[DEBUG] lake delete_tablet, remote=" << cntl->remote_side()
                  << " tablets=" << request->tablet_ids_size()
                  << " failed_tablets=" << response->failed_tablets_size()
                  << std::endl;
    }

    void drop_table(google::protobuf::RpcController* controller,
                    const starrocks::DropTableRequest* request,
                    starrocks::DropTableResponse* response,
                    google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto* cntl = static_cast<brpc::Controller*>(controller);
        if (request == nullptr || response == nullptr) {
            if (response != nullptr) {
                status_err(response->mutable_status(), starrocks::TStatusCode::INVALID_ARGUMENT,
                           "missing drop_table request/response");
            }
            cntl->SetFailed("missing drop_table request/response");
            return;
        }

        std::string err;
        if (!invoke_drop_table(*request, response, &err)) {
            status_err(response->mutable_status(), starrocks::TStatusCode::INTERNAL_ERROR, err);
            cntl->SetFailed(err);
            std::cerr << "[WARN] lake drop_table failed, remote=" << cntl->remote_side() << " err=" << err
                      << std::endl;
            return;
        }
        if (!response->has_status()) {
            status_ok(response->mutable_status());
        }
        std::cerr << "[DEBUG] lake drop_table, remote=" << cntl->remote_side()
                  << " tablet_id=" << request->tablet_id()
                  << " path=" << (request->has_path() ? request->path() : "")
                  << std::endl;
    }

    void delete_data(google::protobuf::RpcController* controller,
                     const starrocks::DeleteDataRequest* request,
                     starrocks::DeleteDataResponse* response,
                     google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto* cntl = static_cast<brpc::Controller*>(controller);
        if (request == nullptr || response == nullptr) {
            if (response != nullptr) {
                response->clear_failed_tablets();
            }
            cntl->SetFailed("missing delete_data request/response");
            return;
        }

        std::string err;
        if (!invoke_delete_data(*request, response, &err)) {
            response->clear_failed_tablets();
            for (const auto tablet_id : request->tablet_ids()) {
                response->add_failed_tablets(tablet_id);
            }
            cntl->SetFailed(err);
            std::cerr << "[WARN] lake delete_data failed, remote=" << cntl->remote_side() << " err=" << err
                      << std::endl;
            return;
        }
        std::cerr << "[DEBUG] lake delete_data, remote=" << cntl->remote_side()
                  << " tablets=" << request->tablet_ids_size()
                  << " txn_id=" << (request->has_txn_id() ? request->txn_id() : 0)
                  << " failed_tablets=" << response->failed_tablets_size()
                  << std::endl;
    }

    void get_tablet_stats(google::protobuf::RpcController* controller,
                          const starrocks::TabletStatRequest* request,
                          starrocks::TabletStatResponse* response,
                          google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto* cntl = static_cast<brpc::Controller*>(controller);
        if (request == nullptr || response == nullptr) {
            if (response != nullptr) {
                response->clear_tablet_stats();
            }
            cntl->SetFailed("missing get_tablet_stats request/response");
            return;
        }
        if (request->tablet_infos_size() == 0) {
            response->clear_tablet_stats();
            cntl->SetFailed("missing tablet_infos");
            return;
        }

        std::string err;
        if (!invoke_get_tablet_stats(*request, response, &err)) {
            response->clear_tablet_stats();
            cntl->SetFailed(err);
            std::cerr << "[WARN] lake get_tablet_stats failed, remote=" << cntl->remote_side() << " err=" << err
                      << std::endl;
            return;
        }
        std::cerr << "[DEBUG] lake get_tablet_stats, remote=" << cntl->remote_side()
                  << " tablets=" << request->tablet_infos_size()
                  << " tablet_stats=" << response->tablet_stats_size()
                  << std::endl;
    }

    void vacuum(google::protobuf::RpcController* controller,
                const starrocks::VacuumRequest* request,
                starrocks::VacuumResponse* response,
                google::protobuf::Closure* done) override {
        static std::mutex s_mtx;
        static std::unordered_set<int64_t> s_vacuuming_partitions;

        brpc::ClosureGuard guard(done);
        auto* cntl = static_cast<brpc::Controller*>(controller);
        if (request == nullptr || response == nullptr) {
            if (response != nullptr) {
                status_err(response->mutable_status(), starrocks::TStatusCode::INVALID_ARGUMENT,
                           "missing vacuum request/response");
            }
            cntl->SetFailed("missing vacuum request/response");
            return;
        }

        bool tracked_partition = false;
        const int64_t partition_id = request->partition_id();
        if (partition_id > 0) {
            std::lock_guard<std::mutex> lock(s_mtx);
            if (!s_vacuuming_partitions.insert(partition_id).second) {
                cntl->SetFailed("duplicated vacuum request of partition " + std::to_string(partition_id));
                return;
            }
            tracked_partition = true;
        }

        auto on_return = [&]() {
            if (!tracked_partition) {
                return;
            }
            std::lock_guard<std::mutex> lock(s_mtx);
            s_vacuuming_partitions.erase(partition_id);
        };
        struct ScopeExit {
            std::function<void()> fn;
            ~ScopeExit() {
                if (fn) fn();
            }
        } defer{on_return};

        std::string err;
        if (!invoke_vacuum(*request, response, &err)) {
            status_err(response->mutable_status(), starrocks::TStatusCode::INTERNAL_ERROR, err);
            cntl->SetFailed(err);
            std::cerr << "[WARN] lake vacuum failed, remote=" << cntl->remote_side() << " err=" << err << std::endl;
            return;
        }
        if (!response->has_status()) {
            status_ok(response->mutable_status());
        }
        std::cerr << "[DEBUG] lake vacuum, remote=" << cntl->remote_side()
                  << " partition_id=" << (request->has_partition_id() ? request->partition_id() : 0)
                  << " tablets=" << request->tablet_infos_size()
                  << " vacuumed_files=" << (response->has_vacuumed_files() ? response->vacuumed_files() : 0)
                  << " vacuumed_file_size="
                  << (response->has_vacuumed_file_size() ? response->vacuumed_file_size() : 0)
                  << " vacuumed_version=" << (response->has_vacuumed_version() ? response->vacuumed_version() : 0)
                  << std::endl;
    }

private:
    static bool invoke_publish_version(const starrocks::PublishVersionRequest& request,
                                       starrocks::PublishVersionResponse* response,
                                       std::string* err) {
        if (response == nullptr) {
            if (err != nullptr) {
                *err = "publish_version response is null";
            }
            return false;
        }

        std::string request_bytes;
        if (!request.SerializeToString(&request_bytes)) {
            if (err != nullptr) {
                *err = "serialize PublishVersionRequest failed";
            }
            return false;
        }

        NovaRocksRustBuf resp_buf{nullptr, 0};
        NovaRocksRustBuf err_buf{nullptr, 0};
        int32_t rc = novarocks_rs_lake_publish_version(reinterpret_cast<const uint8_t*>(request_bytes.data()),
                                                      request_bytes.size(), &resp_buf, &err_buf);
        std::string rust_err = take_rust_buf_string(&err_buf);
        if (rc != 0) {
            take_rust_buf_string(&resp_buf);
            if (err != nullptr) {
                *err = rust_err.empty() ? "rust lake publish_version failed" : rust_err;
            }
            return false;
        }
        if (resp_buf.ptr == nullptr || resp_buf.len == 0) {
            // Empty protobuf payload is a valid default message.
            response->Clear();
            take_rust_buf_string(&resp_buf);
            return true;
        }
        bool ok = response->ParseFromArray(resp_buf.ptr, static_cast<int>(resp_buf.len));
        take_rust_buf_string(&resp_buf);
        if (!ok) {
            if (err != nullptr) {
                *err = "parse PublishVersionResponse from rust buffer failed";
            }
            return false;
        }
        return true;
    }

    static bool invoke_abort_txn(const starrocks::AbortTxnRequest& request,
                                 starrocks::AbortTxnResponse* response,
                                 std::string* err) {
        if (response == nullptr) {
            if (err != nullptr) {
                *err = "abort_txn response is null";
            }
            return false;
        }

        std::string request_bytes;
        if (!request.SerializeToString(&request_bytes)) {
            if (err != nullptr) {
                *err = "serialize AbortTxnRequest failed";
            }
            return false;
        }

        NovaRocksRustBuf resp_buf{nullptr, 0};
        NovaRocksRustBuf err_buf{nullptr, 0};
        int32_t rc = novarocks_rs_lake_abort_txn(reinterpret_cast<const uint8_t*>(request_bytes.data()),
                                               request_bytes.size(), &resp_buf, &err_buf);
        std::string rust_err = take_rust_buf_string(&err_buf);
        if (rc != 0) {
            take_rust_buf_string(&resp_buf);
            if (err != nullptr) {
                *err = rust_err.empty() ? "rust lake abort_txn failed" : rust_err;
            }
            return false;
        }
        if (resp_buf.ptr == nullptr || resp_buf.len == 0) {
            // Empty protobuf payload is a valid default message.
            response->Clear();
            take_rust_buf_string(&resp_buf);
            return true;
        }
        bool ok = response->ParseFromArray(resp_buf.ptr, static_cast<int>(resp_buf.len));
        take_rust_buf_string(&resp_buf);
        if (!ok) {
            if (err != nullptr) {
                *err = "parse AbortTxnResponse from rust buffer failed";
            }
            return false;
        }
        return true;
    }

    static bool invoke_delete_tablet(const starrocks::DeleteTabletRequest& request,
                                     starrocks::DeleteTabletResponse* response,
                                     std::string* err) {
        if (response == nullptr) {
            if (err != nullptr) {
                *err = "delete_tablet response is null";
            }
            return false;
        }

        std::string request_bytes;
        if (!request.SerializeToString(&request_bytes)) {
            if (err != nullptr) {
                *err = "serialize DeleteTabletRequest failed";
            }
            return false;
        }

        NovaRocksRustBuf resp_buf{nullptr, 0};
        NovaRocksRustBuf err_buf{nullptr, 0};
        int32_t rc = novarocks_rs_lake_delete_tablet(reinterpret_cast<const uint8_t*>(request_bytes.data()),
                                                    request_bytes.size(), &resp_buf, &err_buf);
        std::string rust_err = take_rust_buf_string(&err_buf);
        if (rc != 0) {
            take_rust_buf_string(&resp_buf);
            if (err != nullptr) {
                *err = rust_err.empty() ? "rust lake delete_tablet failed" : rust_err;
            }
            return false;
        }
        if (resp_buf.ptr == nullptr || resp_buf.len == 0) {
            response->Clear();
            take_rust_buf_string(&resp_buf);
            return true;
        }
        bool ok = response->ParseFromArray(resp_buf.ptr, static_cast<int>(resp_buf.len));
        take_rust_buf_string(&resp_buf);
        if (!ok) {
            if (err != nullptr) {
                *err = "parse DeleteTabletResponse from rust buffer failed";
            }
            return false;
        }
        return true;
    }

    static bool invoke_delete_data(const starrocks::DeleteDataRequest& request,
                                   starrocks::DeleteDataResponse* response,
                                   std::string* err) {
        if (response == nullptr) {
            if (err != nullptr) {
                *err = "delete_data response is null";
            }
            return false;
        }

        std::string request_bytes;
        if (!request.SerializeToString(&request_bytes)) {
            if (err != nullptr) {
                *err = "serialize DeleteDataRequest failed";
            }
            return false;
        }

        NovaRocksRustBuf resp_buf{nullptr, 0};
        NovaRocksRustBuf err_buf{nullptr, 0};
        int32_t rc = novarocks_rs_lake_delete_data(reinterpret_cast<const uint8_t*>(request_bytes.data()),
                                                  request_bytes.size(), &resp_buf, &err_buf);
        std::string rust_err = take_rust_buf_string(&err_buf);
        if (rc != 0) {
            take_rust_buf_string(&resp_buf);
            if (err != nullptr) {
                *err = rust_err.empty() ? "rust lake delete_data failed" : rust_err;
            }
            return false;
        }
        if (resp_buf.ptr == nullptr || resp_buf.len == 0) {
            response->Clear();
            take_rust_buf_string(&resp_buf);
            return true;
        }
        bool ok = response->ParseFromArray(resp_buf.ptr, static_cast<int>(resp_buf.len));
        take_rust_buf_string(&resp_buf);
        if (!ok) {
            if (err != nullptr) {
                *err = "parse DeleteDataResponse from rust buffer failed";
            }
            return false;
        }
        return true;
    }

    static bool invoke_get_tablet_stats(const starrocks::TabletStatRequest& request,
                                        starrocks::TabletStatResponse* response,
                                        std::string* err) {
        if (response == nullptr) {
            if (err != nullptr) {
                *err = "get_tablet_stats response is null";
            }
            return false;
        }

        std::string request_bytes;
        if (!request.SerializeToString(&request_bytes)) {
            if (err != nullptr) {
                *err = "serialize TabletStatRequest failed";
            }
            return false;
        }

        NovaRocksRustBuf resp_buf{nullptr, 0};
        NovaRocksRustBuf err_buf{nullptr, 0};
        int32_t rc = novarocks_rs_lake_get_tablet_stats(reinterpret_cast<const uint8_t*>(request_bytes.data()),
                                                        request_bytes.size(), &resp_buf, &err_buf);
        std::string rust_err = take_rust_buf_string(&err_buf);
        if (rc != 0) {
            take_rust_buf_string(&resp_buf);
            if (err != nullptr) {
                *err = rust_err.empty() ? "rust lake get_tablet_stats failed" : rust_err;
            }
            return false;
        }
        if (resp_buf.ptr == nullptr || resp_buf.len == 0) {
            response->Clear();
            take_rust_buf_string(&resp_buf);
            return true;
        }
        bool ok = response->ParseFromArray(resp_buf.ptr, static_cast<int>(resp_buf.len));
        take_rust_buf_string(&resp_buf);
        if (!ok) {
            if (err != nullptr) {
                *err = "parse TabletStatResponse from rust buffer failed";
            }
            return false;
        }
        return true;
    }

    static bool invoke_vacuum(const starrocks::VacuumRequest& request,
                              starrocks::VacuumResponse* response,
                              std::string* err) {
        if (response == nullptr) {
            if (err != nullptr) {
                *err = "vacuum response is null";
            }
            return false;
        }

        std::string request_bytes;
        if (!request.SerializeToString(&request_bytes)) {
            if (err != nullptr) {
                *err = "serialize VacuumRequest failed";
            }
            return false;
        }

        NovaRocksRustBuf resp_buf{nullptr, 0};
        NovaRocksRustBuf err_buf{nullptr, 0};
        int32_t rc = novarocks_rs_lake_vacuum(reinterpret_cast<const uint8_t*>(request_bytes.data()),
                                             request_bytes.size(), &resp_buf, &err_buf);
        std::string rust_err = take_rust_buf_string(&err_buf);
        if (rc != 0) {
            take_rust_buf_string(&resp_buf);
            if (err != nullptr) {
                *err = rust_err.empty() ? "rust lake vacuum failed" : rust_err;
            }
            return false;
        }
        if (resp_buf.ptr == nullptr || resp_buf.len == 0) {
            response->Clear();
            take_rust_buf_string(&resp_buf);
            return true;
        }
        bool ok = response->ParseFromArray(resp_buf.ptr, static_cast<int>(resp_buf.len));
        take_rust_buf_string(&resp_buf);
        if (!ok) {
            if (err != nullptr) {
                *err = "parse VacuumResponse from rust buffer failed";
            }
            return false;
        }
        return true;
    }

    static bool invoke_drop_table(const starrocks::DropTableRequest& request,
                                  starrocks::DropTableResponse* response,
                                  std::string* err) {
        if (response == nullptr) {
            if (err != nullptr) {
                *err = "drop_table response is null";
            }
            return false;
        }

        std::string request_bytes;
        if (!request.SerializeToString(&request_bytes)) {
            if (err != nullptr) {
                *err = "serialize DropTableRequest failed";
            }
            return false;
        }

        NovaRocksRustBuf resp_buf{nullptr, 0};
        NovaRocksRustBuf err_buf{nullptr, 0};
        int32_t rc = novarocks_rs_lake_drop_table(reinterpret_cast<const uint8_t*>(request_bytes.data()),
                                                 request_bytes.size(), &resp_buf, &err_buf);
        std::string rust_err = take_rust_buf_string(&err_buf);
        if (rc != 0) {
            take_rust_buf_string(&resp_buf);
            if (err != nullptr) {
                *err = rust_err.empty() ? "rust lake drop_table failed" : rust_err;
            }
            return false;
        }
        if (resp_buf.ptr == nullptr || resp_buf.len == 0) {
            response->Clear();
            take_rust_buf_string(&resp_buf);
            return true;
        }
        bool ok = response->ParseFromArray(resp_buf.ptr, static_cast<int>(resp_buf.len));
        take_rust_buf_string(&resp_buf);
        if (!ok) {
            if (err != nullptr) {
                *err = "parse DropTableResponse from rust buffer failed";
            }
            return false;
        }
        return true;
    }
};

std::unique_ptr<brpc::Server> g_brpc_server;
std::unique_ptr<InternalServiceImpl> g_internal_service;
std::unique_ptr<LakeServiceImpl> g_lake_service;
std::atomic<bool> g_brpc_started{false};

} // namespace

int novarocks_compat_start_brpc(const NovaRocksCompatConfig* cfg, std::string* err) {
    if (cfg == nullptr) {
        if (err) *err = "cfg is null";
        return 1;
    }
    if (g_brpc_started.exchange(true)) {
        if (err) *err = "brpc already started";
        return 2;
    }

    try {
        const char* host = (cfg->host != nullptr && cfg->host[0] != '\0') ? cfg->host : "0.0.0.0";
        butil::EndPoint endpoint;
        if (butil::str2endpoint(host, static_cast<int>(cfg->brpc_port), &endpoint) != 0) {
            if (err) *err = "invalid host/port for brpc";
            g_brpc_started.store(false);
            return 3;
        }

        if (cfg->debug_exec_batch_plan_json) {
            std::cerr << "[DEBUG] start config debug_exec_batch_plan_json="
                      << static_cast<int>(cfg->debug_exec_batch_plan_json) << std::endl;
        }

        auto server = std::make_unique<brpc::Server>();
        auto service = std::make_unique<InternalServiceImpl>(*cfg);
        auto lake_service = std::make_unique<LakeServiceImpl>();

        if (server->AddService(service.get(), brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            if (err) *err = "failed to add PInternalService";
            g_brpc_started.store(false);
            return 4;
        }
        if (server->AddService(lake_service.get(), brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            if (err) *err = "failed to add LakeService";
            g_brpc_started.store(false);
            return 4;
        }

        brpc::ServerOptions options;
        options.idle_timeout_sec = -1; // keep connections alive unless explicitly closed

        if (server->Start(endpoint, &options) != 0) {
            if (err) *err = "failed to start brpc server";
            g_brpc_started.store(false);
            return 5;
        }

        g_internal_service = std::move(service);
        g_lake_service = std::move(lake_service);
        g_brpc_server = std::move(server);
        if (err) err->clear();
        return 0;
    } catch (const std::exception& e) {
        if (err) *err = e.what();
        g_brpc_started.store(false);
        g_brpc_server.reset();
        g_internal_service.reset();
        g_lake_service.reset();
        return 6;
    }
}

void novarocks_compat_stop_brpc() {
    if (!g_brpc_started.exchange(false)) {
        return;
    }
    if (g_brpc_server) {
        g_brpc_server->Stop(0);
        g_brpc_server->Join();
    }
    g_lake_service.reset();
    g_internal_service.reset();
    g_brpc_server.reset();
}
