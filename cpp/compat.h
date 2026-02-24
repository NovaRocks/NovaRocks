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
#pragma once

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct NovaRocksCompatConfig {
    const char* host;           // e.g. "0.0.0.0"
    uint16_t heartbeat_port;    // (unused, kept for ABI compatibility)
    uint16_t brpc_port;         // brpc PInternalService
    uint8_t debug_exec_batch_plan_json;
    uint8_t log_level;          // 0=INFO, 1=WARNING, 2=ERROR, 3=FATAL
} NovaRocksCompatConfig;

// Starts brpc server only (heartbeat moved to Rust).
// Returns 0 on success, non-zero on failure. If failure, err_buf is filled with a short message.
int novarocks_compat_start(const NovaRocksCompatConfig* cfg, char* err_buf, int err_buf_len);

// Stops RPC servers (stub in Phase 0 scaffolding).
void novarocks_compat_stop(void);

typedef struct NovaRocksRustBuf {
    uint8_t* ptr;
    size_t len;
} NovaRocksRustBuf;

typedef struct NovaRocksUniqueId {
    int64_t hi;
    int64_t lo;
} NovaRocksUniqueId;

// --- Rust engine FFI ---

// Executes `TExecBatchPlanFragmentsParams` from request attachment (Thrift BINARY).
int32_t novarocks_rs_submit_exec_batch_plan_fragments(const uint8_t* ptr, size_t len);

// Executes `TExecPlanFragmentParams` from request attachment (Thrift BINARY).
int32_t novarocks_rs_submit_exec_plan_fragment(const uint8_t* ptr, size_t len);

// Returns:
// - 0: OK (a result batch is returned; may be EOS)
// - 1: NOT_FOUND
// - 2: CANCELLED
// - 3: FAILED
// - 4: TIMEOUT
int32_t novarocks_rs_fetch_result_batch(int64_t finst_id_hi,
                                      int64_t finst_id_lo,
                                      int64_t* out_packet_seq,
                                      bool* out_eos,
                                      NovaRocksRustBuf* out_batch,
                                      NovaRocksRustBuf* out_err);

int32_t novarocks_rs_cancel(int64_t finst_id_hi, int64_t finst_id_lo);

// lake_service.proto PublishVersionRequest -> PublishVersionResponse (protobuf bytes).
// Returns:
// - 0: OK
// - 1: execution failed (out_err set)
// - 2: invalid request/decode failure (out_err set)
int32_t novarocks_rs_lake_publish_version(const uint8_t* ptr,
                                        size_t len,
                                        NovaRocksRustBuf* out_resp,
                                        NovaRocksRustBuf* out_err);

// lake_service.proto AbortTxnRequest -> AbortTxnResponse (protobuf bytes).
// Returns:
// - 0: OK
// - 1: execution failed (out_err set)
// - 2: invalid request/decode failure (out_err set)
int32_t novarocks_rs_lake_abort_txn(const uint8_t* ptr,
                                  size_t len,
                                  NovaRocksRustBuf* out_resp,
                                  NovaRocksRustBuf* out_err);

// lake_service.proto DropTableRequest -> DropTableResponse (protobuf bytes).
// Returns:
// - 0: OK
// - 1: execution failed (out_err set)
// - 2: invalid request/decode failure (out_err set)
int32_t novarocks_rs_lake_drop_table(const uint8_t* ptr,
                                   size_t len,
                                   NovaRocksRustBuf* out_resp,
                                   NovaRocksRustBuf* out_err);

// lake_service.proto DeleteTabletRequest -> DeleteTabletResponse (protobuf bytes).
// Returns:
// - 0: OK
// - 1: execution failed (out_err set)
// - 2: invalid request/decode failure (out_err set)
int32_t novarocks_rs_lake_delete_tablet(const uint8_t* ptr,
                                      size_t len,
                                      NovaRocksRustBuf* out_resp,
                                      NovaRocksRustBuf* out_err);

// lake_service.proto DeleteDataRequest -> DeleteDataResponse (protobuf bytes).
// Returns:
// - 0: OK
// - 1: execution failed (out_err set)
// - 2: invalid request/decode failure (out_err set)
int32_t novarocks_rs_lake_delete_data(const uint8_t* ptr,
                                     size_t len,
                                     NovaRocksRustBuf* out_resp,
                                     NovaRocksRustBuf* out_err);

// lake_service.proto TabletStatRequest -> TabletStatResponse (protobuf bytes).
// Returns:
// - 0: OK
// - 1: execution failed (out_err set)
// - 2: invalid request/decode failure (out_err set)
int32_t novarocks_rs_lake_get_tablet_stats(const uint8_t* ptr,
                                         size_t len,
                                         NovaRocksRustBuf* out_resp,
                                         NovaRocksRustBuf* out_err);

// lake_service.proto VacuumRequest -> VacuumResponse (protobuf bytes).
// Returns:
// - 0: OK
// - 1: execution failed (out_err set)
// - 2: invalid request/decode failure (out_err set)
int32_t novarocks_rs_lake_vacuum(const uint8_t* ptr,
                               size_t len,
                               NovaRocksRustBuf* out_resp,
                               NovaRocksRustBuf* out_err);

void novarocks_rs_free_buf(uint8_t* ptr, size_t len);

#ifdef __cplusplus
}
#endif
