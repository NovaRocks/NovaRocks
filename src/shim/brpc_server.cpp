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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <deque>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <brpc/closure_guard.h>
#include <brpc/channel.h>
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

struct UniqueIdKeyHash {
    size_t operator()(const UniqueIdKey& id) const {
        const auto hi = static_cast<uint64_t>(id.hi);
        const auto lo = static_cast<uint64_t>(id.lo);
        return std::hash<uint64_t>{}((hi << 1) ^ lo);
    }
};

std::string format_unique_id(const UniqueIdKey& id) {
    const auto hi = static_cast<uint64_t>(id.hi);
    const auto lo = static_cast<uint64_t>(id.lo);
    std::ostringstream oss;
    oss << std::hex << std::nouppercase << std::setfill('0')
        << std::setw(8) << static_cast<uint32_t>(hi >> 32) << "-"
        << std::setw(4) << static_cast<uint16_t>(hi >> 16) << "-"
        << std::setw(4) << static_cast<uint16_t>(hi) << "-"
        << std::setw(4) << static_cast<uint16_t>(lo >> 48) << "-"
        << std::setw(12) << (lo & 0x0000FFFFFFFFFFFFULL);
    return oss.str();
}

struct ResultPacket {
    std::string thrift_bytes;
    int64_t packet_seq = 0;
    bool eos = true;
};

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

void init_compat_buf(NovaRocksRustBuf* buf) {
    if (buf == nullptr) {
        return;
    }
    buf->ptr = nullptr;
    buf->len = 0;
}

void write_compat_buf(const std::string& bytes, NovaRocksRustBuf* out) {
    if (out == nullptr) {
        return;
    }
    init_compat_buf(out);
    if (bytes.empty()) {
        return;
    }
    auto* ptr = static_cast<uint8_t*>(std::malloc(bytes.size()));
    if (ptr == nullptr) {
        return;
    }
    std::memcpy(ptr, bytes.data(), bytes.size());
    out->ptr = ptr;
    out->len = bytes.size();
}

using RustUnaryRpcFn = int32_t (*)(const uint8_t*, size_t, NovaRocksRustBuf*, NovaRocksRustBuf*);

template <typename Request, typename Response>
bool invoke_rust_unary_rpc(const Request& request,
                           Response* response,
                           RustUnaryRpcFn func,
                           const char* rpc_name,
                           std::string* err) {
    if (response == nullptr) {
        if (err != nullptr) {
            *err = std::string(rpc_name) + " response is null";
        }
        return false;
    }

    std::string request_bytes;
    if (!request.SerializeToString(&request_bytes)) {
        if (err != nullptr) {
            *err = std::string("serialize ") + rpc_name + " request failed";
        }
        return false;
    }

    NovaRocksRustBuf resp_buf{nullptr, 0};
    NovaRocksRustBuf err_buf{nullptr, 0};
    int32_t rc = func(reinterpret_cast<const uint8_t*>(request_bytes.data()),
                      request_bytes.size(),
                      &resp_buf,
                      &err_buf);
    std::string rust_err = take_rust_buf_string(&err_buf);
    if (rc != 0) {
        take_rust_buf_string(&resp_buf);
        if (err != nullptr) {
            *err = rust_err.empty() ? std::string("rust ") + rpc_name + " failed" : rust_err;
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
            *err = std::string("parse ") + rpc_name + " response from rust buffer failed";
        }
        return false;
    }
    return true;
}

template <typename Request, typename Response>
int32_t invoke_internal_brpc_client(
        const char* host,
        uint16_t port,
        const uint8_t* ptr,
        size_t len,
        NovaRocksRustBuf* out_resp,
        NovaRocksRustBuf* out_err,
        const char* rpc_name,
        void (starrocks::PInternalService_Stub::*method)(
                google::protobuf::RpcController*,
                const Request*,
                Response*,
                google::protobuf::Closure*),
        const std::function<void(brpc::Controller*, const Request&)>& configure = nullptr) {
    init_compat_buf(out_resp);
    init_compat_buf(out_err);

    if (host == nullptr || host[0] == '\0') {
        write_compat_buf(std::string(rpc_name) + " destination host is empty", out_err);
        return 2;
    }
    if (port == 0) {
        write_compat_buf(std::string(rpc_name) + " destination port must be positive", out_err);
        return 2;
    }
    if (ptr == nullptr) {
        write_compat_buf(std::string(rpc_name) + " request ptr is null", out_err);
        return 2;
    }

    Request request;
    if (!request.ParseFromArray(ptr, static_cast<int>(len))) {
        write_compat_buf(std::string("decode ") + rpc_name + " request failed", out_err);
        return 2;
    }

    std::string endpoint = std::string(host) + ":" + std::to_string(port);
    static std::mutex s_channel_mu;
    static std::unordered_map<std::string, std::shared_ptr<brpc::Channel>> s_channels;
    std::shared_ptr<brpc::Channel> channel;
    {
        std::lock_guard<std::mutex> guard(s_channel_mu);
        auto it = s_channels.find(endpoint);
        if (it != s_channels.end()) {
            channel = it->second;
        } else {
            brpc::ChannelOptions options;
            options.connect_timeout_ms = 2000;
            options.timeout_ms = 600000;
            options.max_retry = 3;

            channel = std::make_shared<brpc::Channel>();
            if (channel->Init(endpoint.c_str(), &options) != 0) {
                write_compat_buf(std::string(rpc_name) + " channel init failed: " + endpoint,
                                 out_err);
                return 1;
            }
            s_channels.emplace(endpoint, channel);
        }
    }

    if (channel == nullptr) {
        write_compat_buf(std::string(rpc_name) + " channel cache returned null: " + endpoint,
                         out_err);
        return 1;
    }

    starrocks::PInternalService_Stub stub(channel.get());
    brpc::Controller cntl;
    cntl.set_timeout_ms(600000);
    if (configure) {
        configure(&cntl, request);
    }
    Response response;
    (stub.*method)(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        write_compat_buf(std::string(rpc_name) + " request failed: " + cntl.ErrorText(), out_err);
        return 1;
    }

    std::string response_bytes;
    if (!response.SerializeToString(&response_bytes)) {
        write_compat_buf(std::string("serialize ") + rpc_name + " response failed", out_err);
        return 1;
    }
    write_compat_buf(response_bytes, out_resp);
    return 0;
}

class QueryRpcPool {
public:
    explicit QueryRpcPool(size_t thread_count) {
        const size_t actual_threads = std::max<size_t>(1, thread_count);
        workers_.reserve(actual_threads);
        for (size_t i = 0; i < actual_threads; ++i) {
            workers_.emplace_back([this]() { worker_loop(); });
        }
    }

    ~QueryRpcPool() { shutdown(); }

    bool submit(std::function<void()> task) {
        if (!task) {
            return false;
        }

        {
            std::lock_guard<std::mutex> lock(mu_);
            if (stopped_) {
                return false;
            }
            tasks_.push_back(std::move(task));
        }
        cv_.notify_one();
        return true;
    }

    void shutdown() {
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (stopped_) {
                return;
            }
            stopped_ = true;
        }
        cv_.notify_all();
        for (auto& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
        workers_.clear();
    }

private:
    void worker_loop() {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(mu_);
                cv_.wait(lock, [this]() { return stopped_ || !tasks_.empty(); });
                if (tasks_.empty()) {
                    if (stopped_) {
                        return;
                    }
                    continue;
                }
                task = std::move(tasks_.front());
                tasks_.pop_front();
            }

            try {
                task();
            } catch (const std::exception& e) {
                std::cerr << "[ERROR] query rpc pool task failed: " << e.what() << std::endl;
            } catch (...) {
                std::cerr << "[ERROR] query rpc pool task failed: unknown exception" << std::endl;
            }
        }
    }

    std::mutex mu_;
    std::condition_variable cv_;
    std::deque<std::function<void()>> tasks_;
    std::vector<std::thread> workers_;
    bool stopped_ = false;
};

std::unique_ptr<QueryRpcPool> g_query_rpc_pool;

struct FetchTryOutcome {
    enum class Kind {
        Ready,
        NotReady,
        Error,
    };

    Kind kind = Kind::Error;
    ResultPacket packet;
    starrocks::TStatusCode::type status_code = starrocks::TStatusCode::INTERNAL_ERROR;
    std::string message;
};

void finish_fetch_response(brpc::Controller* cntl,
                           starrocks::PFetchDataResult* response,
                           const ResultPacket& pkt) {
    if (response == nullptr) {
        return;
    }
    status_ok(response->mutable_status());
    response->set_packet_seq(pkt.packet_seq);
    response->set_eos(pkt.eos);
    if (cntl != nullptr && !pkt.thrift_bytes.empty()) {
        cntl->response_attachment().append(pkt.thrift_bytes);
    }
}

void finish_fetch_error(starrocks::PFetchDataResult* response,
                        starrocks::TStatusCode::type code,
                        const std::string& msg) {
    if (response != nullptr) {
        status_err(response->mutable_status(), code, msg);
    }
}

FetchTryOutcome try_fetch_result_packet(UniqueIdKey finst_id) {
    FetchTryOutcome outcome;

    NovaRocksRustBuf batch_buf{nullptr, 0};
    NovaRocksRustBuf err_buf{nullptr, 0};
    int64_t packet_seq = 0;
    bool eos = true;
    int32_t rc = novarocks_rs_try_fetch_result_batch(
            finst_id.hi, finst_id.lo, &packet_seq, &eos, &batch_buf, &err_buf);

    auto take_err = [&]() -> std::string {
        std::string msg = take_rust_buf_string(&err_buf);
        if (msg.empty()) {
            msg = "unknown error";
        }
        return msg;
    };

    if (rc == 4) {
        outcome.kind = FetchTryOutcome::Kind::NotReady;
        if (batch_buf.ptr != nullptr) {
            novarocks_rs_free_buf(batch_buf.ptr, batch_buf.len);
        }
        return outcome;
    }

    if (rc != 0) {
        outcome.kind = FetchTryOutcome::Kind::Error;
        outcome.message = take_err();
        switch (rc) {
        case 1:
            outcome.status_code = starrocks::TStatusCode::NOT_FOUND;
            break;
        case 2:
            outcome.status_code = starrocks::TStatusCode::CANCELLED;
            break;
        case 3:
        default:
            outcome.status_code = starrocks::TStatusCode::INTERNAL_ERROR;
            break;
        }
        if (batch_buf.ptr != nullptr) {
            novarocks_rs_free_buf(batch_buf.ptr, batch_buf.len);
        }
        return outcome;
    }

    outcome.kind = FetchTryOutcome::Kind::Ready;
    outcome.packet.packet_seq = packet_seq;
    outcome.packet.eos = eos;
    if (batch_buf.ptr != nullptr && batch_buf.len > 0) {
        outcome.packet.thrift_bytes.assign(reinterpret_cast<char*>(batch_buf.ptr), batch_buf.len);
    }
    if (batch_buf.ptr != nullptr) {
        novarocks_rs_free_buf(batch_buf.ptr, batch_buf.len);
    }
    take_rust_buf_string(&err_buf);

    if (outcome.packet.thrift_bytes.empty() && !outcome.packet.eos) {
        outcome.kind = FetchTryOutcome::Kind::Error;
        outcome.status_code = starrocks::TStatusCode::INTERNAL_ERROR;
        outcome.message = "non-eos fetch result missing batch attachment";
    }

    return outcome;
}

struct FetchWaiter {
    UniqueIdKey finst_id;
    brpc::Controller* cntl;
    starrocks::PFetchDataResult* response;
    google::protobuf::Closure* done;
    std::chrono::steady_clock::time_point deadline;
};

void complete_fetch_waiter(FetchWaiter waiter, const FetchTryOutcome& outcome) {
    brpc::ClosureGuard guard(waiter.done);

    switch (outcome.kind) {
    case FetchTryOutcome::Kind::Ready:
        std::cerr << "[DEBUG] fetch_data called, remote="
                  << (waiter.cntl != nullptr ? waiter.cntl->remote_side() : butil::EndPoint())
                  << " finst_id=" << format_unique_id(waiter.finst_id)
                  << " packet_seq=" << outcome.packet.packet_seq
                  << " eos=" << (outcome.packet.eos ? "true" : "false")
                  << " attachment_bytes=" << outcome.packet.thrift_bytes.size() << std::endl;
        finish_fetch_response(waiter.cntl, waiter.response, outcome.packet);
        return;
    case FetchTryOutcome::Kind::NotReady:
        finish_fetch_error(waiter.response,
                           starrocks::TStatusCode::INTERNAL_ERROR,
                           "fetch waiter completed without ready result");
        return;
    case FetchTryOutcome::Kind::Error:
        std::cerr << "[WARN] fetch_data failed, finst_id=" << format_unique_id(waiter.finst_id)
                  << " msg=" << outcome.message << std::endl;
        finish_fetch_error(waiter.response, outcome.status_code, outcome.message);
        return;
    }
}

void complete_fetch_waiter_timeout(FetchWaiter waiter) {
    brpc::ClosureGuard guard(waiter.done);
    std::string msg = "timeout waiting for fetch result";
    std::cerr << "[WARN] fetch_data waiter timed out, finst_id=" << format_unique_id(waiter.finst_id)
              << " msg=" << msg << std::endl;
    finish_fetch_error(waiter.response, starrocks::TStatusCode::TIMEOUT, msg);
}

void complete_fetch_waiter_shutdown(FetchWaiter waiter) {
    brpc::ClosureGuard guard(waiter.done);
    finish_fetch_error(waiter.response,
                       starrocks::TStatusCode::SERVICE_UNAVAILABLE,
                       "fetch waiter registry is stopping");
}

class FetchWaiterRegistry {
public:
    explicit FetchWaiterRegistry(QueryRpcPool* pool) : pool_(pool) {
        cleaner_ = std::thread([this]() { cleaner_loop(); });
    }

    ~FetchWaiterRegistry() { shutdown(); }

    bool register_waiter(FetchWaiter waiter, std::string* err) {
        const UniqueIdKey finst_id = waiter.finst_id;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (stopped_) {
                if (err != nullptr) {
                    *err = "fetch waiter registry is stopping";
                }
                return false;
            }
            waiters_[waiter.finst_id].push_back(std::move(waiter));
        }

        cv_.notify_one();
        schedule_drain(finst_id);
        return true;
    }

    void notify_ready(UniqueIdKey finst_id) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (stopped_) {
                return;
            }
            ready_notified_.insert(finst_id);
        }
        schedule_drain(finst_id, /*consume_ready_hint=*/true);
    }

    void shutdown() {
        std::vector<FetchWaiter> pending;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (stopped_) {
                return;
            }
            stopped_ = true;
            for (auto& [finst_id, queue] : waiters_) {
                (void)finst_id;
                while (!queue.empty()) {
                    pending.push_back(std::move(queue.front()));
                    queue.pop_front();
                }
            }
            waiters_.clear();
            draining_.clear();
            ready_notified_.clear();
        }

        cv_.notify_all();
        if (cleaner_.joinable()) {
            cleaner_.join();
        }
        for (auto& waiter : pending) {
            complete_fetch_waiter_shutdown(std::move(waiter));
        }
    }

private:
    using WaiterQueue = std::deque<FetchWaiter>;

    void schedule_drain(UniqueIdKey finst_id, bool consume_ready_hint = false) {
        bool should_submit = false;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (stopped_) {
                return;
            }
            if (draining_.find(finst_id) != draining_.end()) {
                return;
            }
            draining_.insert(finst_id);
            if (consume_ready_hint) {
                ready_notified_.erase(finst_id);
            }
            should_submit = true;
        }
        if (!should_submit) {
            return;
        }

        auto* pool = pool_;
        if (pool == nullptr) {
            fail_waiters_for_id(finst_id, starrocks::TStatusCode::SERVICE_UNAVAILABLE,
                                "query rpc pool is unavailable");
            clear_draining(finst_id);
            return;
        }

        if (!pool->submit([this, finst_id]() { drain_ready(finst_id); })) {
            fail_waiters_for_id(finst_id,
                                starrocks::TStatusCode::SERVICE_UNAVAILABLE,
                                "query rpc pool is stopping");
            clear_draining(finst_id);
        }
    }

    void clear_draining(UniqueIdKey finst_id) {
        std::lock_guard<std::mutex> lock(mu_);
        draining_.erase(finst_id);
        ready_notified_.erase(finst_id);
    }

    void fail_waiters_for_id(UniqueIdKey finst_id,
                             starrocks::TStatusCode::type code,
                             const std::string& message) {
        std::vector<FetchWaiter> pending;
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto it = waiters_.find(finst_id);
            if (it != waiters_.end()) {
                while (!it->second.empty()) {
                    pending.push_back(std::move(it->second.front()));
                    it->second.pop_front();
                }
                waiters_.erase(it);
            }
            ready_notified_.erase(finst_id);
        }

        for (auto& waiter : pending) {
            brpc::ClosureGuard guard(waiter.done);
            finish_fetch_error(waiter.response, code, message);
        }
    }

    bool pop_waiter(UniqueIdKey finst_id, FetchWaiter* waiter) {
        std::lock_guard<std::mutex> lock(mu_);
        if (stopped_) {
            draining_.erase(finst_id);
            ready_notified_.erase(finst_id);
            return false;
        }
        auto it = waiters_.find(finst_id);
        if (it == waiters_.end() || it->second.empty()) {
            draining_.erase(finst_id);
            ready_notified_.erase(finst_id);
            return false;
        }
        *waiter = std::move(it->second.front());
        it->second.pop_front();
        if (it->second.empty()) {
            waiters_.erase(it);
        }
        return true;
    }

    void requeue_waiter_front(FetchWaiter waiter) {
        bool complete_shutdown = false;
        bool reschedule = false;
        const UniqueIdKey finst_id = waiter.finst_id;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (stopped_) {
                draining_.erase(finst_id);
                ready_notified_.erase(finst_id);
                complete_shutdown = true;
            } else {
                waiters_[finst_id].push_front(std::move(waiter));
                draining_.erase(finst_id);
                reschedule = ready_notified_.erase(finst_id) > 0;
            }
        }
        if (complete_shutdown) {
            complete_fetch_waiter_shutdown(std::move(waiter));
        } else if (reschedule) {
            schedule_drain(finst_id, /*consume_ready_hint=*/false);
        }
    }

    void drain_ready(UniqueIdKey finst_id) {
        while (true) {
            FetchWaiter waiter;
            if (!pop_waiter(finst_id, &waiter)) {
                return;
            }

            if (std::chrono::steady_clock::now() >= waiter.deadline) {
                complete_fetch_waiter_timeout(std::move(waiter));
                continue;
            }

            FetchTryOutcome outcome = try_fetch_result_packet(finst_id);
            if (outcome.kind == FetchTryOutcome::Kind::NotReady) {
                requeue_waiter_front(std::move(waiter));
                return;
            }

            complete_fetch_waiter(std::move(waiter), outcome);
        }
    }

    void cleaner_loop() {
        std::unique_lock<std::mutex> lock(mu_);
        while (!stopped_) {
            cv_.wait_for(lock, std::chrono::seconds(1));
            if (stopped_) {
                break;
            }

            std::vector<FetchWaiter> expired;
            const auto now = std::chrono::steady_clock::now();
            for (auto it = waiters_.begin(); it != waiters_.end();) {
                WaiterQueue kept;
                while (!it->second.empty()) {
                    FetchWaiter waiter = std::move(it->second.front());
                    it->second.pop_front();
                    if (now >= waiter.deadline) {
                        expired.push_back(std::move(waiter));
                    } else {
                        kept.push_back(std::move(waiter));
                    }
                }
                it->second.swap(kept);
                if (it->second.empty()) {
                    draining_.erase(it->first);
                    ready_notified_.erase(it->first);
                    it = waiters_.erase(it);
                } else {
                    ++it;
                }
            }

            lock.unlock();
            for (auto& waiter : expired) {
                complete_fetch_waiter_timeout(std::move(waiter));
            }
            lock.lock();
        }
    }

    QueryRpcPool* pool_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::unordered_map<UniqueIdKey, WaiterQueue, UniqueIdKeyHash> waiters_;
    std::unordered_set<UniqueIdKey, UniqueIdKeyHash> draining_;
    std::unordered_set<UniqueIdKey, UniqueIdKeyHash> ready_notified_;
    std::thread cleaner_;
    bool stopped_ = false;
};

std::unique_ptr<FetchWaiterRegistry> g_fetch_waiter_registry;

class InternalServiceImpl final : public starrocks::PInternalService {
public:
    explicit InternalServiceImpl(NovaRocksCompatConfig cfg) : cfg_(cfg) {}

    void exec_plan_fragment(google::protobuf::RpcController* controller,
                            const starrocks::PExecPlanFragmentRequest* request,
                            starrocks::PExecPlanFragmentResult* response,
                            google::protobuf::Closure* done) override {
        auto* cntl = static_cast<brpc::Controller*>(controller);
        std::string proto_str = request != nullptr && request->has_attachment_protocol()
                                        ? request->attachment_protocol()
                                        : "binary";
        auto proto = parse_attachment_protocol(proto_str);
        std::string attachment =
                cntl != nullptr ? cntl->request_attachment().to_string() : std::string();

        submit_query_rpc_task(
                "exec_plan_fragment",
                response,
                done,
                [proto, attachment = std::move(attachment), response]() mutable {
                    run_exec_plan_fragment(proto, std::move(attachment), response);
                });
    }

    void exec_batch_plan_fragments(google::protobuf::RpcController* controller,
                                   const ::starrocks::PExecBatchPlanFragmentsRequest* request,
                                   ::starrocks::PExecBatchPlanFragmentsResult* response,
                                   ::google::protobuf::Closure* done) override {
        auto* cntl = static_cast<brpc::Controller*>(controller);
        std::string proto_str = request != nullptr && request->has_attachment_protocol()
                                        ? request->attachment_protocol()
                                        : "binary";
        auto proto = parse_attachment_protocol(proto_str);
        std::string attachment =
                cntl != nullptr ? cntl->request_attachment().to_string() : std::string();

        submit_query_rpc_task(
                "exec_batch_plan_fragments",
                response,
                done,
                [proto, attachment = std::move(attachment), response]() mutable {
                    run_exec_batch_plan_fragments(proto, std::move(attachment), response);
                });
    }

    void fetch_data(google::protobuf::RpcController* controller,
                    const starrocks::PFetchDataRequest* request,
                    starrocks::PFetchDataResult* response,
                    google::protobuf::Closure* done) override {
        auto* cntl = static_cast<brpc::Controller*>(controller);
        UniqueIdKey finst_id{0, 0};
        if (request != nullptr) {
            finst_id.hi = request->finst_id().hi();
            finst_id.lo = request->finst_id().lo();
        }
        if (request == nullptr || response == nullptr || cntl == nullptr) {
            brpc::ClosureGuard guard(done);
            finish_fetch_error(response,
                               starrocks::TStatusCode::INVALID_ARGUMENT,
                               "missing fetch_data request/controller/response");
            return;
        }

        FetchTryOutcome outcome = try_fetch_result_packet(finst_id);
        if (outcome.kind != FetchTryOutcome::Kind::NotReady || done == nullptr) {
            brpc::ClosureGuard guard(done);
            if (outcome.kind == FetchTryOutcome::Kind::Ready) {
                std::cerr << "[DEBUG] fetch_data called, remote=" << cntl->remote_side()
                          << " finst_id=" << format_unique_id(finst_id)
                          << " packet_seq=" << outcome.packet.packet_seq
                          << " eos=" << (outcome.packet.eos ? "true" : "false")
                          << " attachment_bytes=" << outcome.packet.thrift_bytes.size() << std::endl;
                finish_fetch_response(cntl, response, outcome.packet);
            } else if (outcome.kind == FetchTryOutcome::Kind::Error) {
                std::cerr << "[WARN] fetch_data failed, finst_id=" << format_unique_id(finst_id)
                          << " msg=" << outcome.message << std::endl;
                finish_fetch_error(response, outcome.status_code, outcome.message);
            } else {
                finish_fetch_error(response,
                                   starrocks::TStatusCode::SERVICE_UNAVAILABLE,
                                   "fetch_data waiter requires async closure");
            }
            return;
        }

        auto* registry = g_fetch_waiter_registry.get();
        if (registry == nullptr) {
            brpc::ClosureGuard guard(done);
            finish_fetch_error(response,
                               starrocks::TStatusCode::SERVICE_UNAVAILABLE,
                               "fetch waiter registry is unavailable");
            return;
        }

        int64_t timeout_ms = novarocks_rs_fetch_wait_timeout_ms(finst_id.hi, finst_id.lo);
        if (timeout_ms <= 0) {
            timeout_ms = 300000;
        }

        FetchWaiter waiter{
                finst_id,
                cntl,
                response,
                done,
                std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms)};
        std::string err;
        if (!registry->register_waiter(std::move(waiter), &err)) {
            brpc::ClosureGuard guard(done);
            finish_fetch_error(response,
                               starrocks::TStatusCode::SERVICE_UNAVAILABLE,
                               err.empty() ? "fetch waiter registry rejected request" : err);
        }
    }

    void cancel_plan_fragment(google::protobuf::RpcController* controller,
                              const starrocks::PCancelPlanFragmentRequest* request,
                              starrocks::PCancelPlanFragmentResult* response,
                              google::protobuf::Closure* done) override {
        auto* cntl = static_cast<brpc::Controller*>(controller);
        UniqueIdKey finst_id{0, 0};
        if (request != nullptr) {
            finst_id.hi = request->finst_id().hi();
            finst_id.lo = request->finst_id().lo();
        }

        submit_query_rpc_task("cancel_plan_fragment",
                              response,
                              done,
                              [cntl, finst_id, response]() {
                                  run_cancel_plan_fragment(cntl, finst_id, response);
                              });
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

    void transmit_chunk(google::protobuf::RpcController* controller,
                        const starrocks::PTransmitChunkParams* request,
                        starrocks::PTransmitChunkResult* response,
                        google::protobuf::Closure* done) override {
        auto* cntl = static_cast<brpc::Controller*>(controller);
        submit_query_rpc_task(
                "transmit_chunk",
                response,
                done,
                [cntl, request, response]() { run_transmit_chunk(cntl, request, response); });
    }

    void transmit_runtime_filter(google::protobuf::RpcController* controller,
                                 const starrocks::PTransmitRuntimeFilterParams* request,
                                 starrocks::PTransmitRuntimeFilterResult* response,
                                 google::protobuf::Closure* done) override {
        auto* cntl = static_cast<brpc::Controller*>(controller);
        submit_query_rpc_task("transmit_runtime_filter",
                              response,
                              done,
                              [cntl, request, response]() {
                                  run_transmit_runtime_filter(cntl, request, response);
                              });
    }

    void lookup(google::protobuf::RpcController* controller,
                const starrocks::PLookUpRequest* request,
                starrocks::PLookUpResponse* response,
                google::protobuf::Closure* done) override {
        auto* cntl = static_cast<brpc::Controller*>(controller);
        submit_query_rpc_task("lookup",
                              response,
                              done,
                              [cntl, request, response]() { run_lookup(cntl, request, response); });
    }

private:
    template <typename Response>
    static void finish_query_rpc_error(Response* response,
                                       const char* rpc_name,
                                       starrocks::TStatusCode::type code,
                                       const std::string& msg) {
        std::cerr << "[WARN] " << rpc_name << " failed: " << msg << std::endl;
        if (response != nullptr) {
            status_err(response->mutable_status(), code, msg);
        }
    }

    template <typename Response>
    static void submit_query_rpc_task(const char* rpc_name,
                                      Response* response,
                                      google::protobuf::Closure* done,
                                      std::function<void()> task) {
        if (done == nullptr) {
            try {
                task();
            } catch (const std::exception& e) {
                finish_query_rpc_error(response,
                                       rpc_name,
                                       starrocks::TStatusCode::INTERNAL_ERROR,
                                       std::string("unexpected exception: ") + e.what());
            } catch (...) {
                finish_query_rpc_error(response,
                                       rpc_name,
                                       starrocks::TStatusCode::INTERNAL_ERROR,
                                       "unexpected unknown exception");
            }
            return;
        }

        auto* pool = g_query_rpc_pool.get();
        if (pool == nullptr) {
            finish_query_rpc_error(response,
                                   rpc_name,
                                   starrocks::TStatusCode::SERVICE_UNAVAILABLE,
                                   std::string(rpc_name) + " query rpc pool is unavailable");
            done->Run();
            return;
        }

        std::function<void()> wrapped = [rpc_name, response, done, task = std::move(task)]() mutable {
            brpc::ClosureGuard guard(done);
            try {
                task();
            } catch (const std::exception& e) {
                finish_query_rpc_error(response,
                                       rpc_name,
                                       starrocks::TStatusCode::INTERNAL_ERROR,
                                       std::string("unexpected exception: ") + e.what());
            } catch (...) {
                finish_query_rpc_error(response,
                                       rpc_name,
                                       starrocks::TStatusCode::INTERNAL_ERROR,
                                       "unexpected unknown exception");
            }
        };
        if (!pool->submit(std::move(wrapped))) {
            finish_query_rpc_error(response,
                                   rpc_name,
                                   starrocks::TStatusCode::SERVICE_UNAVAILABLE,
                                   std::string(rpc_name) + " query rpc pool is stopping");
            done->Run();
        }
    }

    static void run_exec_plan_fragment(AttachmentProtocol proto,
                                       std::string attachment,
                                       starrocks::PExecPlanFragmentResult* response) {
        if (response == nullptr) {
            return;
        }
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

    static void run_exec_batch_plan_fragments(AttachmentProtocol proto,
                                              std::string attachment,
                                              starrocks::PExecBatchPlanFragmentsResult* response) {
        if (response == nullptr) {
            return;
        }
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

    static void run_cancel_plan_fragment(brpc::Controller* cntl,
                                         UniqueIdKey finst_id,
                                         starrocks::PCancelPlanFragmentResult* response) {
        novarocks_rs_cancel(finst_id.hi, finst_id.lo);
        if (cntl != nullptr) {
            std::cerr << "[INFO] cancel_plan_fragment called, remote=" << cntl->remote_side()
                      << " finst_id=" << format_unique_id(finst_id) << std::endl;
        } else {
            std::cerr << "[INFO] cancel_plan_fragment called, finst_id="
                      << format_unique_id(finst_id) << std::endl;
        }
        if (response != nullptr) {
            status_ok(response->mutable_status());
        }
    }

    static void run_transmit_chunk(brpc::Controller* cntl,
                                   const starrocks::PTransmitChunkParams* request,
                                   starrocks::PTransmitChunkResult* response) {
        if (request == nullptr || response == nullptr) {
            if (response != nullptr) {
                status_err(response->mutable_status(), starrocks::TStatusCode::INVALID_ARGUMENT,
                           "missing transmit_chunk request/response");
            }
            if (cntl != nullptr) {
                cntl->SetFailed("missing transmit_chunk request/response");
            }
            return;
        }

        std::string err;
        if (!invoke_transmit_chunk(*request, response, &err)) {
            status_err(response->mutable_status(), starrocks::TStatusCode::INTERNAL_ERROR, err);
            if (cntl != nullptr) {
                cntl->SetFailed(err);
            }
            return;
        }
        if (!response->has_status()) {
            status_ok(response->mutable_status());
        }
    }

    static void run_transmit_runtime_filter(
            brpc::Controller* cntl,
            const starrocks::PTransmitRuntimeFilterParams* request,
            starrocks::PTransmitRuntimeFilterResult* response) {
        if (request == nullptr || response == nullptr) {
            if (response != nullptr) {
                status_err(response->mutable_status(), starrocks::TStatusCode::INVALID_ARGUMENT,
                           "missing transmit_runtime_filter request/response");
            }
            if (cntl != nullptr) {
                cntl->SetFailed("missing transmit_runtime_filter request/response");
            }
            return;
        }

        std::string err;
        if (!invoke_transmit_runtime_filter(*request, response, &err)) {
            status_err(response->mutable_status(), starrocks::TStatusCode::INTERNAL_ERROR, err);
            if (cntl != nullptr) {
                cntl->SetFailed(err);
            }
            return;
        }
        if (!response->has_status()) {
            status_ok(response->mutable_status());
        }
    }

    static void run_lookup(brpc::Controller* cntl,
                           const starrocks::PLookUpRequest* request,
                           starrocks::PLookUpResponse* response) {
        if (request == nullptr || response == nullptr) {
            if (response != nullptr) {
                status_err(response->mutable_status(), starrocks::TStatusCode::INVALID_ARGUMENT,
                           "missing lookup request/response");
            }
            if (cntl != nullptr) {
                cntl->SetFailed("missing lookup request/response");
            }
            return;
        }

        std::string err;
        if (!invoke_lookup(*request, response, &err)) {
            status_err(response->mutable_status(), starrocks::TStatusCode::INTERNAL_ERROR, err);
            if (cntl != nullptr) {
                cntl->SetFailed(err);
            }
            return;
        }
        if (!response->has_status()) {
            status_ok(response->mutable_status());
        }
    }

    static bool invoke_transmit_chunk(const starrocks::PTransmitChunkParams& request,
                                      starrocks::PTransmitChunkResult* response,
                                      std::string* err) {
        return invoke_rust_unary_rpc(
                request, response, novarocks_rs_transmit_chunk, "transmit_chunk", err);
    }

    static bool invoke_transmit_runtime_filter(
            const starrocks::PTransmitRuntimeFilterParams& request,
            starrocks::PTransmitRuntimeFilterResult* response,
            std::string* err) {
        return invoke_rust_unary_rpc(request,
                                     response,
                                     novarocks_rs_transmit_runtime_filter,
                                     "transmit_runtime_filter",
                                     err);
    }

    static bool invoke_lookup(const starrocks::PLookUpRequest& request,
                              starrocks::PLookUpResponse* response,
                              std::string* err) {
        return invoke_rust_unary_rpc(request, response, novarocks_rs_lookup, "lookup", err);
    }

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

        std::string err;
        if (!invoke_publish_log_version(*request, response, &err)) {
            for (auto tablet_id : request->tablet_ids()) {
                response->add_failed_tablets(tablet_id);
            }
            cntl->SetFailed(err);
            return;
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
        std::string err;
        if (!invoke_publish_log_version_batch(*request, response, &err)) {
            for (auto tablet_id : request->tablet_ids()) {
                response->add_failed_tablets(tablet_id);
            }
            cntl->SetFailed(err);
            return;
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

    void compact(google::protobuf::RpcController* controller,
                 const starrocks::CompactRequest* request,
                 starrocks::CompactResponse* response,
                 google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto* cntl = static_cast<brpc::Controller*>(controller);
        if (request == nullptr || response == nullptr) {
            if (response != nullptr) {
                response->clear_failed_tablets();
                status_err(response->mutable_status(), starrocks::TStatusCode::INVALID_ARGUMENT,
                           "missing compact request/response");
            }
            cntl->SetFailed("missing compact request/response");
            return;
        }
        response->clear_failed_tablets();

        if (request->tablet_ids_size() == 0) {
            status_err(response->mutable_status(), starrocks::TStatusCode::INVALID_ARGUMENT, "missing tablet_ids");
            cntl->SetFailed("missing tablet_ids");
            return;
        }
        if (!request->has_txn_id()) {
            status_err(response->mutable_status(), starrocks::TStatusCode::INVALID_ARGUMENT, "missing txn_id");
            cntl->SetFailed("missing txn_id");
            return;
        }
        if (!request->has_version()) {
            status_err(response->mutable_status(), starrocks::TStatusCode::INVALID_ARGUMENT, "missing version");
            cntl->SetFailed("missing version");
            return;
        }

        std::string err;
        if (!invoke_compact(*request, response, &err)) {
            response->clear_failed_tablets();
            for (auto tablet_id : request->tablet_ids()) {
                response->add_failed_tablets(tablet_id);
            }
            status_err(response->mutable_status(), starrocks::TStatusCode::INTERNAL_ERROR, err);
            cntl->SetFailed(err);
            std::cerr << "[WARN] lake compact failed, remote=" << cntl->remote_side()
                      << " txn_id=" << request->txn_id() << " version=" << request->version()
                      << " err=" << err << std::endl;
            return;
        }
        if (!response->has_status()) {
            status_ok(response->mutable_status());
        }
        std::cerr << "[DEBUG] lake compact, remote=" << cntl->remote_side()
                  << " tablets=" << request->tablet_ids_size() << " txn_id=" << request->txn_id()
                  << " version=" << request->version()
                  << " failed_tablets=" << response->failed_tablets_size()
                  << std::endl;
    }

    void aggregate_compact(google::protobuf::RpcController* controller,
                           const starrocks::AggregateCompactRequest* request,
                           starrocks::CompactResponse* response,
                           google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto* cntl = static_cast<brpc::Controller*>(controller);
        if (request == nullptr || response == nullptr) {
            if (response != nullptr) {
                response->clear_failed_tablets();
                status_err(response->mutable_status(), starrocks::TStatusCode::INVALID_ARGUMENT,
                           "missing aggregate_compact request/response");
            }
            cntl->SetFailed("missing aggregate_compact request/response");
            return;
        }
        response->Clear();
        status_ok(response->mutable_status());

        if (request->requests_size() == 0) {
            cntl->SetFailed("empty requests");
            return;
        }
        if (request->compute_nodes_size() != request->requests_size()) {
            cntl->SetFailed("compute nodes size not equal to requests size");
            return;
        }

        int64_t total_input_size = 0;
        for (int i = 0; i < request->requests_size(); ++i) {
            const auto& compute_node = request->compute_nodes(i);
            if (!compute_node.has_host() || !compute_node.has_brpc_port()) {
                status_err(response->mutable_status(), starrocks::TStatusCode::INTERNAL_ERROR,
                           "compute node missing host/port");
                return;
            }

            starrocks::CompactRequest node_request = request->requests(i);
            // Keep publish path compatible without requiring combined txn log writer.
            node_request.set_skip_write_txnlog(false);

            brpc::Channel channel;
            std::string endpoint = compute_node.host() + ":" + std::to_string(compute_node.brpc_port());
            if (channel.Init(endpoint.c_str(), nullptr) != 0) {
                status_err(response->mutable_status(), starrocks::TStatusCode::INTERNAL_ERROR,
                           "aggregate_compact channel init failed: " + endpoint);
                return;
            }
            starrocks::LakeService_Stub stub(&channel);
            brpc::Controller node_cntl;
            if (node_request.has_timeout_ms()) {
                node_cntl.set_timeout_ms(node_request.timeout_ms());
            }
            starrocks::CompactResponse node_response;
            stub.compact(&node_cntl, &node_request, &node_response, nullptr);

            if (node_cntl.Failed()) {
                status_err(response->mutable_status(), starrocks::TStatusCode::INTERNAL_ERROR,
                           "aggregate_compact call compact failed: " + std::string(node_cntl.ErrorText()));
                return;
            }
            if (node_response.has_status() &&
                node_response.status().status_code() != static_cast<int32_t>(starrocks::TStatusCode::OK)) {
                std::string err = node_response.status().error_msgs_size() > 0
                                          ? node_response.status().error_msgs(0)
                                          : "aggregate_compact sub compact returned non-OK status";
                status_err(response->mutable_status(), starrocks::TStatusCode::INTERNAL_ERROR, err);
                return;
            }
            if (node_response.failed_tablets_size() > 0) {
                std::string err = "aggregate_compact sub compact returned failed_tablets";
                status_err(response->mutable_status(), starrocks::TStatusCode::INTERNAL_ERROR, err);
                return;
            }

            if (node_response.has_success_compaction_input_file_size()) {
                total_input_size += node_response.success_compaction_input_file_size();
            }
            for (const auto& stat : node_response.compact_stats()) {
                response->add_compact_stats()->CopyFrom(stat);
            }
        }

        response->set_success_compaction_input_file_size(total_input_size);
        std::cerr << "[DEBUG] lake aggregate_compact, remote=" << cntl->remote_side()
                  << " sub_requests=" << request->requests_size()
                  << " partition_id=" << (request->has_partition_id() ? request->partition_id() : 0)
                  << std::endl;
    }

    void abort_compaction(google::protobuf::RpcController* controller,
                          const starrocks::AbortCompactionRequest* request,
                          starrocks::AbortCompactionResponse* response,
                          google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto* cntl = static_cast<brpc::Controller*>(controller);
        if (request == nullptr || response == nullptr) {
            if (response != nullptr) {
                status_err(response->mutable_status(), starrocks::TStatusCode::INVALID_ARGUMENT,
                           "missing abort_compaction request/response");
            }
            cntl->SetFailed("missing abort_compaction request/response");
            return;
        }
        if (!request->has_txn_id()) {
            status_err(response->mutable_status(), starrocks::TStatusCode::INVALID_ARGUMENT, "missing txn_id");
            cntl->SetFailed("missing txn_id");
            return;
        }

        std::string err;
        if (!invoke_abort_compaction(*request, response, &err)) {
            status_err(response->mutable_status(), starrocks::TStatusCode::INTERNAL_ERROR, err);
            cntl->SetFailed(err);
            std::cerr << "[WARN] lake abort_compaction failed, remote=" << cntl->remote_side()
                      << " txn_id=" << request->txn_id() << " err=" << err << std::endl;
            return;
        }
        if (!response->has_status()) {
            status_ok(response->mutable_status());
        }
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

    static bool invoke_publish_log_version(const starrocks::PublishLogVersionRequest& request,
                                           starrocks::PublishLogVersionResponse* response,
                                           std::string* err) {
        if (response == nullptr) {
            if (err != nullptr) {
                *err = "publish_log_version response is null";
            }
            return false;
        }

        std::string request_bytes;
        if (!request.SerializeToString(&request_bytes)) {
            if (err != nullptr) {
                *err = "serialize PublishLogVersionRequest failed";
            }
            return false;
        }

        NovaRocksRustBuf resp_buf{nullptr, 0};
        NovaRocksRustBuf err_buf{nullptr, 0};
        int32_t rc = novarocks_rs_lake_publish_log_version(
                reinterpret_cast<const uint8_t*>(request_bytes.data()), request_bytes.size(), &resp_buf, &err_buf);
        std::string rust_err = take_rust_buf_string(&err_buf);
        if (rc != 0) {
            take_rust_buf_string(&resp_buf);
            if (err != nullptr) {
                *err = rust_err.empty() ? "rust lake publish_log_version failed" : rust_err;
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
                *err = "parse PublishLogVersionResponse from rust buffer failed";
            }
            return false;
        }
        return true;
    }

    static bool invoke_publish_log_version_batch(const starrocks::PublishLogVersionBatchRequest& request,
                                                 starrocks::PublishLogVersionResponse* response,
                                                 std::string* err) {
        if (response == nullptr) {
            if (err != nullptr) {
                *err = "publish_log_version_batch response is null";
            }
            return false;
        }

        std::string request_bytes;
        if (!request.SerializeToString(&request_bytes)) {
            if (err != nullptr) {
                *err = "serialize PublishLogVersionBatchRequest failed";
            }
            return false;
        }

        NovaRocksRustBuf resp_buf{nullptr, 0};
        NovaRocksRustBuf err_buf{nullptr, 0};
        int32_t rc = novarocks_rs_lake_publish_log_version_batch(
                reinterpret_cast<const uint8_t*>(request_bytes.data()), request_bytes.size(), &resp_buf, &err_buf);
        std::string rust_err = take_rust_buf_string(&err_buf);
        if (rc != 0) {
            take_rust_buf_string(&resp_buf);
            if (err != nullptr) {
                *err = rust_err.empty() ? "rust lake publish_log_version_batch failed" : rust_err;
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
                *err = "parse PublishLogVersionResponse from rust buffer failed";
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

    static bool invoke_compact(const starrocks::CompactRequest& request,
                               starrocks::CompactResponse* response,
                               std::string* err) {
        if (response == nullptr) {
            if (err != nullptr) {
                *err = "compact response is null";
            }
            return false;
        }

        std::string request_bytes;
        if (!request.SerializeToString(&request_bytes)) {
            if (err != nullptr) {
                *err = "serialize CompactRequest failed";
            }
            return false;
        }

        NovaRocksRustBuf resp_buf{nullptr, 0};
        NovaRocksRustBuf err_buf{nullptr, 0};
        int32_t rc = novarocks_rs_lake_compact(reinterpret_cast<const uint8_t*>(request_bytes.data()),
                                               request_bytes.size(), &resp_buf, &err_buf);
        std::string rust_err = take_rust_buf_string(&err_buf);
        if (rc != 0) {
            take_rust_buf_string(&resp_buf);
            if (err != nullptr) {
                *err = rust_err.empty() ? "rust lake compact failed" : rust_err;
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
                *err = "parse CompactResponse from rust buffer failed";
            }
            return false;
        }
        return true;
    }

    static bool invoke_abort_compaction(const starrocks::AbortCompactionRequest& request,
                                        starrocks::AbortCompactionResponse* response,
                                        std::string* err) {
        if (response == nullptr) {
            if (err != nullptr) {
                *err = "abort_compaction response is null";
            }
            return false;
        }

        std::string request_bytes;
        if (!request.SerializeToString(&request_bytes)) {
            if (err != nullptr) {
                *err = "serialize AbortCompactionRequest failed";
            }
            return false;
        }

        NovaRocksRustBuf resp_buf{nullptr, 0};
        NovaRocksRustBuf err_buf{nullptr, 0};
        int32_t rc = novarocks_rs_lake_abort_compaction(reinterpret_cast<const uint8_t*>(request_bytes.data()),
                                                        request_bytes.size(), &resp_buf, &err_buf);
        std::string rust_err = take_rust_buf_string(&err_buf);
        if (rc != 0) {
            take_rust_buf_string(&resp_buf);
            if (err != nullptr) {
                *err = rust_err.empty() ? "rust lake abort_compaction failed" : rust_err;
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
                *err = "parse AbortCompactionResponse from rust buffer failed";
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

int32_t novarocks_compat_transmit_chunk(const char* host,
                                        uint16_t port,
                                        const uint8_t* ptr,
                                        size_t len,
                                        NovaRocksRustBuf* out_resp,
                                        NovaRocksRustBuf* out_err) {
    return invoke_internal_brpc_client<starrocks::PTransmitChunkParams,
                                       starrocks::PTransmitChunkResult>(
            host,
            port,
            ptr,
            len,
            out_resp,
            out_err,
            "transmit_chunk",
            &starrocks::PInternalService_Stub::transmit_chunk);
}

int32_t novarocks_compat_transmit_runtime_filter(const char* host,
                                                 uint16_t port,
                                                 const uint8_t* ptr,
                                                 size_t len,
                                                 NovaRocksRustBuf* out_resp,
                                                 NovaRocksRustBuf* out_err) {
    return invoke_internal_brpc_client<starrocks::PTransmitRuntimeFilterParams,
                                       starrocks::PTransmitRuntimeFilterResult>(
            host,
            port,
            ptr,
            len,
            out_resp,
            out_err,
            "transmit_runtime_filter",
            &starrocks::PInternalService_Stub::transmit_runtime_filter,
            [](brpc::Controller* cntl, const starrocks::PTransmitRuntimeFilterParams& request) {
                if (request.has_transmit_timeout_ms()) {
                    cntl->set_timeout_ms(request.transmit_timeout_ms());
                }
            });
}

int32_t novarocks_compat_lookup(const char* host,
                                uint16_t port,
                                const uint8_t* ptr,
                                size_t len,
                                NovaRocksRustBuf* out_resp,
                                NovaRocksRustBuf* out_err) {
    return invoke_internal_brpc_client<starrocks::PLookUpRequest, starrocks::PLookUpResponse>(
            host,
            port,
            ptr,
            len,
            out_resp,
            out_err,
            "lookup",
            &starrocks::PInternalService_Stub::lookup);
}

extern "C" void novarocks_compat_notify_fetch_ready(int64_t finst_id_hi, int64_t finst_id_lo) {
    auto* registry = g_fetch_waiter_registry.get();
    if (registry == nullptr) {
        return;
    }
    registry->notify_ready(UniqueIdKey{finst_id_hi, finst_id_lo});
}

void novarocks_compat_free_buf(uint8_t* ptr, size_t /*len*/) {
    std::free(ptr);
}

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

        size_t query_rpc_threads = static_cast<size_t>(cfg->internal_service_query_rpc_thread_num);
        if (query_rpc_threads == 0) {
            query_rpc_threads = std::thread::hardware_concurrency();
        }
        if (query_rpc_threads == 0) {
            query_rpc_threads = 1;
        }

        auto server = std::make_unique<brpc::Server>();
        auto service = std::make_unique<InternalServiceImpl>(*cfg);
        auto lake_service = std::make_unique<LakeServiceImpl>();
        auto query_rpc_pool = std::make_unique<QueryRpcPool>(query_rpc_threads);
        auto fetch_waiter_registry = std::make_unique<FetchWaiterRegistry>(query_rpc_pool.get());

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
        g_query_rpc_pool = std::move(query_rpc_pool);
        g_fetch_waiter_registry = std::move(fetch_waiter_registry);
        g_brpc_server = std::move(server);
        std::cerr << "[INFO] query rpc pool started, threads=" << query_rpc_threads << std::endl;
        if (err) err->clear();
        return 0;
    } catch (const std::exception& e) {
        if (err) *err = e.what();
        g_brpc_started.store(false);
        g_brpc_server.reset();
        g_internal_service.reset();
        g_lake_service.reset();
        g_fetch_waiter_registry.reset();
        g_query_rpc_pool.reset();
        return 6;
    }
}

void novarocks_compat_stop_brpc() {
    if (!g_brpc_started.exchange(false)) {
        return;
    }
    if (g_brpc_server) {
        g_brpc_server->Stop(0);
    }
    if (g_fetch_waiter_registry) {
        g_fetch_waiter_registry->shutdown();
    }
    if (g_query_rpc_pool) {
        g_query_rpc_pool->shutdown();
    }
    if (g_brpc_server) {
        g_brpc_server->Join();
    }
    g_fetch_waiter_registry.reset();
    g_query_rpc_pool.reset();
    g_lake_service.reset();
    g_internal_service.reset();
    g_brpc_server.reset();
}
