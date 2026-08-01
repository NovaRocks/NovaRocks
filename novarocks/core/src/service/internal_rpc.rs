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
use std::collections::HashMap;

use crate::common::ids::SlotId;
use crate::proto;
use crate::runtime::exchange;
use crate::runtime::lookup::{
    decode_column_ipc, encode_column_ipc, execute_position_lookup_request,
};
use crate::runtime::query_context::QueryId;
fn ok_common_status() -> proto::common::Status {
    proto::common::Status {
        code: 0,
        message: String::new(),
    }
}

fn error_common_status(message: impl Into<String>) -> proto::common::Status {
    proto::common::Status {
        code: 1,
        message: message.into(),
    }
}

pub fn handle_transmit_chunk(
    params: proto::novarocks::ExchangeRequest,
) -> proto::novarocks::ExchangeResponse {
    let mut response = proto::novarocks::ExchangeResponse {
        ack_sequence: params.sequence,
        status: Some(ok_common_status()),
    };

    let decode_start = std::time::Instant::now();
    let key = exchange::ExchangeKey {
        finst_id_hi: params.finst_id_hi,
        finst_id_lo: params.finst_id_lo,
        node_id: params.node_id,
    };
    let chunks = match exchange::decode_chunks_for_sender(
        key,
        params.sender_id,
        params.be_number,
        &params.payload,
    ) {
        Ok(v) => v,
        Err(err) => {
            response.status = Some(error_common_status(format!(
                "exchange decode failed: {err}"
            )));
            return response;
        }
    };
    let decode_ns = decode_start.elapsed().as_nanos();

    exchange::push_chunks_with_stats(
        key,
        params.sender_id,
        params.be_number,
        chunks,
        params.eos,
        params.payload.len(),
        decode_ns,
    );
    response
}

pub fn handle_lookup(req: proto::filter::LookupRequest) -> proto::filter::LookupResponse {
    let mut response = proto::filter::LookupResponse {
        status: Some(ok_common_status()),
        columns: Vec::new(),
    };

    let Some(query_id) = req.query_id.as_ref() else {
        response.status = Some(error_common_status("missing query_id for lookup"));
        return response;
    };
    let query_id = QueryId {
        hi: query_id.hi,
        lo: query_id.lo,
    };
    let tuple_id = req.request_tuple_id;

    let mut request_columns = HashMap::new();
    for col in req.request_columns {
        let slot_id = col.slot_id;
        if col.data.is_empty() {
            response.status = Some(error_common_status(format!(
                "lookup request column {} missing data",
                slot_id
            )));
            return response;
        }
        let slot_id = match SlotId::try_from(slot_id) {
            Ok(v) => v,
            Err(err) => {
                response.status = Some(error_common_status(err));
                return response;
            }
        };
        let array = match decode_column_ipc(&col.data) {
            Ok(arr) => arr,
            Err(err) => {
                response.status = Some(error_common_status(err));
                return response;
            }
        };
        request_columns.insert(slot_id, array);
    }

    match execute_position_lookup_request(query_id, tuple_id, request_columns) {
        Ok(columns) => {
            for (slot_id, array) in columns {
                let data = match encode_column_ipc(&array) {
                    Ok(v) => v,
                    Err(err) => {
                        response.status = Some(error_common_status(err));
                        return response;
                    }
                };
                response.columns.push(proto::filter::Column {
                    slot_id: slot_id.as_u32() as i32,
                    data_size: data.len() as i64,
                    data,
                });
            }
        }
        Err(err) => {
            response.status = Some(error_common_status(err));
        }
    }
    response
}

pub fn handle_lookup_close(query_id: QueryId, lookup_node_id: i32) -> Result<(), String> {
    crate::runtime::query_context::query_context_manager()
        .complete_lookup_fetcher(query_id, lookup_node_id)
}

#[cfg(test)]
mod native_runtime_filter_mode_tests {
    use super::*;

    fn submit_native_fragment(query_id: QueryId) -> Result<(), String> {
        crate::service::native_fragment_service_test_fixture::submit_exec_plan_fragment_native(
            crate::service::native_fragment_service_test_fixture::values_submission_for_test(
                query_id,
                crate::common::types::UniqueId {
                    hi: query_id.hi + 1,
                    lo: query_id.lo + 1,
                },
                81,
                crate::exec::fragment::sink::FragmentSinkProgram::Noop,
            ),
        )
    }
}
