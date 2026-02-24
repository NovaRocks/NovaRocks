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
#include <cstring>
#include <iostream>
#include <string>

int novarocks_compat_start_brpc(const NovaRocksCompatConfig* cfg, std::string* err);
void novarocks_compat_stop_brpc();

namespace {
std::atomic<bool> g_started{false};

void write_err(char* err_buf, int err_buf_len, const char* msg) {
    if (err_buf == nullptr || err_buf_len <= 0) {
        return;
    }
    std::strncpy(err_buf, msg, static_cast<size_t>(err_buf_len - 1));
    err_buf[err_buf_len - 1] = '\0';
}
} // namespace

int novarocks_compat_start(const NovaRocksCompatConfig* cfg, char* err_buf, int err_buf_len) {
    if (cfg == nullptr) {
        write_err(err_buf, err_buf_len, "cfg is null");
        return 1;
    }
    if (g_started.exchange(true)) {
        write_err(err_buf, err_buf_len, "already started");
        return 2;
    }

    std::string err;
    std::cerr << "[INFO] starting brpc on host=" << (cfg->host ? cfg->host : "0.0.0.0")
              << " port=" << cfg->brpc_port << std::endl;
    int rc = novarocks_compat_start_brpc(cfg, &err);
    if (rc != 0) {
        write_err(err_buf, err_buf_len, err.c_str());
        g_started.store(false);
        return rc;
    }

    write_err(err_buf, err_buf_len, "");
    return 0;
}

void novarocks_compat_stop(void) {
    if (!g_started.exchange(false)) {
        return;
    }
    std::cerr << "[INFO] stopping services" << std::endl;
    novarocks_compat_stop_brpc();
}
