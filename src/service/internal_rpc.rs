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
use crate::novarocks_logging::warn;
use crate::runtime::exchange;
use crate::runtime::lookup::{decode_column_ipc, encode_column_ipc, execute_lookup_request};
use crate::runtime::query_context::{QueryId, query_context_manager, query_expire_durations};
use crate::service::grpc_proto as proto;

fn ok_status() -> proto::starrocks::StatusPb {
    proto::starrocks::StatusPb {
        status_code: 0,
        error_msgs: Vec::new(),
    }
}

fn error_status(message: impl Into<String>) -> proto::starrocks::StatusPb {
    proto::starrocks::StatusPb {
        status_code: 1,
        error_msgs: vec![message.into()],
    }
}

pub(crate) fn handle_transmit_chunk(
    params: proto::starrocks::PTransmitChunkParams,
) -> proto::starrocks::PTransmitChunkResult {
    let mut response = proto::starrocks::PTransmitChunkResult {
        status: Some(ok_status()),
        receive_timestamp: None,
        receiver_post_process_time: None,
    };

    let Some(finst_id) = params.finst_id.as_ref() else {
        response.status = Some(error_status("missing finst_id for transmit_chunk"));
        return response;
    };
    let Some(node_id) = params.node_id else {
        response.status = Some(error_status("missing node_id for transmit_chunk"));
        return response;
    };
    let Some(sender_id) = params.sender_id else {
        response.status = Some(error_status("missing sender_id for transmit_chunk"));
        return response;
    };
    let Some(be_number) = params.be_number else {
        response.status = Some(error_status("missing be_number for transmit_chunk"));
        return response;
    };
    let Some(eos) = params.eos else {
        response.status = Some(error_status("missing eos for transmit_chunk"));
        return response;
    };
    let Some(_sequence) = params.sequence else {
        response.status = Some(error_status("missing sequence for transmit_chunk"));
        return response;
    };
    let Some(payload) = params.chunks.first().and_then(|chunk| chunk.data.as_ref()) else {
        response.status = Some(error_status("missing chunks[0].data for transmit_chunk"));
        return response;
    };

    let decode_start = std::time::Instant::now();
    let key = exchange::ExchangeKey {
        finst_id_hi: finst_id.hi,
        finst_id_lo: finst_id.lo,
        node_id,
    };
    let chunks = match exchange::decode_chunks_for_sender(key, sender_id, be_number, payload) {
        Ok(v) => v,
        Err(err) => {
            response.status = Some(error_status(format!("exchange decode failed: {err}")));
            return response;
        }
    };
    let decode_ns = decode_start.elapsed().as_nanos();

    exchange::push_chunks_with_stats(
        key,
        sender_id,
        be_number,
        chunks,
        eos,
        payload.len(),
        decode_ns,
    );
    response
}

pub(crate) fn handle_transmit_runtime_filter(
    params: proto::starrocks::PTransmitRuntimeFilterParams,
) -> proto::starrocks::PTransmitRuntimeFilterResult {
    let Some(filter_id) = params.filter_id else {
        return proto::starrocks::PTransmitRuntimeFilterResult {
            status: Some(error_status(
                "missing filter_id for transmit_runtime_filter",
            )),
            filter_id: Some(0),
        };
    };
    let mut response = proto::starrocks::PTransmitRuntimeFilterResult {
        status: Some(ok_status()),
        filter_id: Some(filter_id),
    };

    let Some(query_id) = params.query_id.as_ref() else {
        response.status = Some(error_status("missing query_id for transmit_runtime_filter"));
        return response;
    };
    let query_id = QueryId {
        hi: query_id.hi,
        lo: query_id.lo,
    };

    let Some(payload) = params.data.as_ref() else {
        response.status = Some(error_status(format!(
            "missing runtime filter payload: query_id={} filter_id={}",
            query_id, filter_id
        )));
        return response;
    };
    if payload.is_empty() {
        response.status = Some(error_status(format!(
            "runtime filter payload is empty: query_id={} filter_id={}",
            query_id, filter_id
        )));
        return response;
    }

    if params.is_partial.unwrap_or(false) {
        let Some(worker) = query_context_manager().get_or_create_runtime_filter_worker(query_id)
        else {
            let (delivery_expire, query_expire) = query_expire_durations(None);
            let _ = query_context_manager().ensure_context(
                query_id,
                false,
                delivery_expire,
                query_expire,
            );
            let _ = query_context_manager().enqueue_pending_runtime_filter(
                query_id,
                filter_id,
                params.build_be_number.unwrap_or(0),
                payload.to_vec(),
            );
            return response;
        };
        let build_be_number = params.build_be_number.unwrap_or(0);
        if let Err(err) = worker.receive_partial(filter_id, payload, build_be_number) {
            warn!(
                "receive_partial_runtime_filter failed: query_id={} filter_id={} err={}",
                query_id, filter_id, err
            );
            response.status = Some(error_status(err));
        }
        return response;
    }

    let Some(hub) = query_context_manager().get_runtime_filter_hub(query_id) else {
        response.status = Some(error_status(format!(
            "runtime filter hub not found: query_id={}",
            query_id
        )));
        return response;
    };

    if let Err(err) = hub.receive_remote_filter(filter_id, payload) {
        warn!(
            "receive_remote_filter failed: query_id={} filter_id={} err={}",
            query_id, filter_id, err
        );
        response.status = Some(error_status(err));
    }
    response
}

pub(crate) fn handle_lookup(
    req: proto::starrocks::PLookUpRequest,
) -> proto::starrocks::PLookUpResponse {
    let mut response = proto::starrocks::PLookUpResponse {
        status: Some(ok_status()),
        columns: Vec::new(),
    };

    let Some(query_id) = req.query_id.as_ref() else {
        response.status = Some(error_status("missing query_id for lookup"));
        return response;
    };
    let query_id = QueryId {
        hi: query_id.hi,
        lo: query_id.lo,
    };
    let Some(tuple_id) = req.request_tuple_id else {
        response.status = Some(error_status("missing request_tuple_id for lookup"));
        return response;
    };

    let mut request_columns = HashMap::new();
    for col in req.request_columns {
        let Some(slot_id) = col.slot_id else {
            response.status = Some(error_status("lookup request column missing slot_id"));
            return response;
        };
        if col.data.as_ref().map_or(true, |data| data.is_empty()) {
            response.status = Some(error_status(format!(
                "lookup request column {} missing data",
                slot_id
            )));
            return response;
        }
        let slot_id = match SlotId::try_from(slot_id) {
            Ok(v) => v,
            Err(err) => {
                response.status = Some(error_status(err));
                return response;
            }
        };
        let data = col
            .data
            .as_ref()
            .expect("checked non-empty request column data");
        let array = match decode_column_ipc(data) {
            Ok(arr) => arr,
            Err(err) => {
                response.status = Some(error_status(err));
                return response;
            }
        };
        request_columns.insert(slot_id, array);
    }

    match execute_lookup_request(query_id, tuple_id, request_columns) {
        Ok(columns) => {
            for (slot_id, array) in columns {
                let data = match encode_column_ipc(&array) {
                    Ok(v) => v,
                    Err(err) => {
                        response.status = Some(error_status(err));
                        return response;
                    }
                };
                response.columns.push(proto::starrocks::PColumn {
                    slot_id: Some(slot_id.as_u32() as i32),
                    data_size: Some(data.len() as i64),
                    data: Some(data),
                });
            }
        }
        Err(err) => {
            response.status = Some(error_status(err));
        }
    }
    response
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use arrow::array::{ArrayRef, Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use tempfile::tempdir;

    use super::{
        decode_column_ipc, encode_column_ipc, handle_lookup, handle_transmit_chunk,
        handle_transmit_runtime_filter,
    };
    use crate::cache::CacheOptions;
    use crate::common::ids::SlotId;
    use crate::descriptors;
    use crate::exec::chunk::{Chunk, field_with_slot_id};
    use crate::exec::expr::ExprId;
    use crate::exec::node::RuntimeFilterProbeSpec;
    use crate::exec::node::scan::RowPositionScanConfig;
    use crate::exec::row_position::RowPositionDescriptor;
    use crate::exec::runtime_filter::{RuntimeInFilter, encode_starrocks_in_filter};
    use crate::fs::scan_context::FileScanRange;
    use crate::runtime::exchange;
    use crate::runtime::query_context::{QueryId, query_context_manager};
    use crate::service::grpc_proto as proto;
    use crate::service::internal_rpc_client;
    use crate::types;

    fn unique_id(hi: i64, lo: i64) -> proto::starrocks::PUniqueId {
        proto::starrocks::PUniqueId { hi, lo }
    }

    fn ok_status(status: Option<&proto::starrocks::StatusPb>) -> bool {
        status.map(|s| s.status_code).unwrap_or_default() == 0
    }

    fn int_type_desc() -> types::TTypeDesc {
        types::TTypeDesc {
            types: Some(vec![types::TTypeNode {
                type_: types::TTypeNodeType::SCALAR,
                scalar_type: Some(types::TScalarType {
                    type_: types::TPrimitiveType::INT,
                    len: None,
                    precision: None,
                    scale: None,
                }),
                struct_fields: None,
                is_named: None,
            }]),
        }
    }

    fn bigint_type_desc() -> types::TTypeDesc {
        types::TTypeDesc {
            types: Some(vec![types::TTypeNode {
                type_: types::TTypeNodeType::SCALAR,
                scalar_type: Some(types::TScalarType {
                    type_: types::TPrimitiveType::BIGINT,
                    len: None,
                    precision: None,
                    scale: None,
                }),
                struct_fields: None,
                is_named: None,
            }]),
        }
    }

    fn lookup_desc_tbl(tuple_id: i32) -> descriptors::TDescriptorTable {
        descriptors::TDescriptorTable {
            slot_descriptors: Some(vec![
                descriptors::TSlotDescriptor {
                    id: Some(1),
                    parent: Some(tuple_id),
                    slot_type: Some(int_type_desc()),
                    column_pos: None,
                    byte_offset: None,
                    null_indicator_byte: None,
                    null_indicator_bit: None,
                    col_name: Some("_row_source_id".to_string()),
                    slot_idx: None,
                    is_materialized: Some(true),
                    is_output_column: Some(true),
                    is_nullable: Some(false),
                    col_unique_id: None,
                    col_physical_name: None,
                },
                descriptors::TSlotDescriptor {
                    id: Some(2),
                    parent: Some(tuple_id),
                    slot_type: Some(int_type_desc()),
                    column_pos: None,
                    byte_offset: None,
                    null_indicator_byte: None,
                    null_indicator_bit: None,
                    col_name: Some("_scan_range_id".to_string()),
                    slot_idx: None,
                    is_materialized: Some(true),
                    is_output_column: Some(true),
                    is_nullable: Some(false),
                    col_unique_id: None,
                    col_physical_name: None,
                },
                descriptors::TSlotDescriptor {
                    id: Some(3),
                    parent: Some(tuple_id),
                    slot_type: Some(bigint_type_desc()),
                    column_pos: None,
                    byte_offset: None,
                    null_indicator_byte: None,
                    null_indicator_bit: None,
                    col_name: Some("_row_id".to_string()),
                    slot_idx: None,
                    is_materialized: Some(true),
                    is_output_column: Some(true),
                    is_nullable: Some(false),
                    col_unique_id: None,
                    col_physical_name: None,
                },
                descriptors::TSlotDescriptor {
                    id: Some(4),
                    parent: Some(tuple_id),
                    slot_type: Some(int_type_desc()),
                    column_pos: None,
                    byte_offset: None,
                    null_indicator_byte: None,
                    null_indicator_bit: None,
                    col_name: Some("v".to_string()),
                    slot_idx: None,
                    is_materialized: Some(true),
                    is_output_column: Some(true),
                    is_nullable: Some(false),
                    col_unique_id: None,
                    col_physical_name: None,
                },
            ]),
            table_descriptors: None,
            tuple_descriptors: Vec::new(),
            is_cached: None,
        }
    }

    #[test]
    fn test_handle_transmit_chunk_delivers_payload_and_eos() {
        let finst_id = unique_id(11, 22);
        let key = exchange::ExchangeKey {
            finst_id_hi: finst_id.hi,
            finst_id_lo: finst_id.lo,
            node_id: 7,
        };
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![field_with_slot_id(
                Field::new("v", DataType::Int32, false),
                SlotId::new(1),
            )])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
        )
        .expect("record batch");
        let chunk = Chunk::try_new(batch).expect("chunk");
        exchange::register_expected_chunk_schema(key, 1, chunk.chunk_schema_ref())
            .expect("register expected chunk schema");
        let payload = exchange::encode_chunks(&[chunk], true).expect("encode chunks");

        let response = handle_transmit_chunk(proto::starrocks::PTransmitChunkParams {
            finst_id: Some(finst_id),
            node_id: Some(7),
            sender_id: Some(3),
            be_number: Some(9),
            eos: Some(true),
            sequence: Some(42),
            chunks: vec![proto::starrocks::ChunkPb {
                data: Some(payload),
                data_size: Some(0),
                ..Default::default()
            }],
            ..Default::default()
        });

        assert!(ok_status(response.status.as_ref()));
        let snapshot =
            exchange::snapshot_receiver_state(key).expect("receiver snapshot after transmit_chunk");
        assert_eq!(snapshot.queued_chunks, 1);
        assert_eq!(snapshot.queued_rows, 3);
        assert_eq!(snapshot.finished_senders, 1);
        exchange::cancel_exchange_key(key);
    }

    #[test]
    fn test_handle_transmit_runtime_filter_partial_merge_broadcasts_on_completion() {
        let _hook_guard = internal_rpc_client::test_hook_lock();
        internal_rpc_client::clear_test_hooks();

        let query_id = QueryId { hi: 100, lo: 200 };
        query_context_manager()
            .ensure_context(
                query_id,
                false,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .expect("ensure query context");
        query_context_manager()
            .set_runtime_filter_params(
                query_id,
                crate::runtime_filter::TRuntimeFilterParams {
                    id_to_prober_params: Some(BTreeMap::from([(
                        7,
                        vec![crate::runtime_filter::TRuntimeFilterProberParams {
                            fragment_instance_id: None,
                            fragment_instance_address: Some(types::TNetworkAddress::new(
                                "probe-host".to_string(),
                                9010,
                            )),
                        }],
                    )])),
                    runtime_filter_builder_number: Some(BTreeMap::from([(7, 2)])),
                    runtime_filter_max_size: None,
                    skew_join_runtime_filters: None,
                },
            )
            .expect("set runtime filter params");

        let sent = Arc::new(Mutex::new(Vec::new()));
        let sent_capture = Arc::clone(&sent);
        internal_rpc_client::set_transmit_runtime_filter_hook(move |host, port, params| {
            sent_capture
                .lock()
                .expect("sent lock")
                .push((host.to_string(), port, params));
            Ok(proto::starrocks::PTransmitRuntimeFilterResult {
                status: Some(proto::starrocks::StatusPb {
                    status_code: 0,
                    error_msgs: Vec::new(),
                }),
                filter_id: Some(7),
            })
        });

        let filter =
            RuntimeInFilter::empty(7, SlotId::new(11), &DataType::Int32).expect("empty in filter");
        let payload = encode_starrocks_in_filter(&filter).expect("encode runtime filter");

        let first =
            handle_transmit_runtime_filter(proto::starrocks::PTransmitRuntimeFilterParams {
                is_partial: Some(true),
                query_id: Some(unique_id(query_id.hi, query_id.lo)),
                filter_id: Some(7),
                build_be_number: Some(1),
                data: Some(payload.clone()),
                ..Default::default()
            });
        let second =
            handle_transmit_runtime_filter(proto::starrocks::PTransmitRuntimeFilterParams {
                is_partial: Some(true),
                query_id: Some(unique_id(query_id.hi, query_id.lo)),
                filter_id: Some(7),
                build_be_number: Some(2),
                data: Some(payload),
                ..Default::default()
            });

        assert!(ok_status(first.status.as_ref()));
        assert!(ok_status(second.status.as_ref()));
        let sent = sent.lock().expect("sent lock");
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, "probe-host");
        assert_eq!(sent[0].1, 9010);
        assert_eq!(sent[0].2.is_partial, Some(false));
        internal_rpc_client::clear_test_hooks();
    }

    #[test]
    fn test_handle_transmit_runtime_filter_final_delivery_updates_probe() {
        let query_id = QueryId { hi: 300, lo: 400 };
        query_context_manager()
            .ensure_context(
                query_id,
                false,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .expect("ensure query context");
        let hub = Arc::new(crate::runtime::runtime_filter_hub::RuntimeFilterHub::new(
            crate::exec::pipeline::dependency::DependencyManager::new(),
        ));
        hub.register_probe_specs(
            88,
            &[RuntimeFilterProbeSpec {
                filter_id: 7,
                expr_id: ExprId(0),
                slot_id: SlotId::new(11),
            }],
        );
        let probe = hub.register_probe(88);
        query_context_manager()
            .with_context_mut(query_id, |ctx| {
                ctx.set_runtime_filter_hub(Arc::clone(&hub));
                Ok(())
            })
            .expect("install runtime filter hub");

        let filter =
            RuntimeInFilter::empty(7, SlotId::new(11), &DataType::Int32).expect("empty in filter");
        let payload = encode_starrocks_in_filter(&filter).expect("encode runtime filter");
        let response =
            handle_transmit_runtime_filter(proto::starrocks::PTransmitRuntimeFilterParams {
                is_partial: Some(false),
                query_id: Some(unique_id(query_id.hi, query_id.lo)),
                filter_id: Some(7),
                data: Some(payload),
                ..Default::default()
            });

        assert!(ok_status(response.status.as_ref()));
        let snapshot = probe.snapshot();
        assert_eq!(snapshot.in_filters().len(), 1);
        assert_eq!(snapshot.in_filters()[0].filter_id(), 7);
    }

    #[test]
    fn test_handle_lookup_returns_encoded_columns() {
        let query_id = QueryId { hi: 500, lo: 600 };
        let tuple_id = 1;
        query_context_manager()
            .ensure_context(
                query_id,
                false,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .expect("ensure query context");
        query_context_manager()
            .set_cache_options(
                query_id,
                CacheOptions::from_query_options(None).expect("default cache options"),
            )
            .expect("set cache options");
        query_context_manager()
            .with_context_mut(query_id, |ctx| {
                ctx.desc_tbl = Some(lookup_desc_tbl(tuple_id));
                Ok(())
            })
            .expect("set descriptor table");
        query_context_manager()
            .register_row_pos_descs(
                query_id,
                HashMap::from([(
                    tuple_id,
                    RowPositionDescriptor {
                        row_position_type: descriptors::TRowPositionType::ICEBERG_V3_ROW_POSITION,
                        row_source_slot: SlotId::new(1),
                        fetch_ref_slots: vec![SlotId::new(2), SlotId::new(3)],
                        lookup_ref_slots: Vec::new(),
                    },
                )]),
            )
            .expect("register row position descriptors");

        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("lookup.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef],
        )
        .expect("build parquet batch");
        let file = std::fs::File::create(&path).expect("create parquet file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("parquet writer");
        writer.write(&batch).expect("write parquet batch");
        writer.close().expect("close parquet writer");

        query_context_manager()
            .register_glm_scan_ranges(
                query_id,
                SlotId::new(1),
                RowPositionScanConfig {
                    file_format: descriptors::THdfsFileFormat::PARQUET,
                    case_sensitive: true,
                    batch_size: Some(1024),
                    enable_file_metacache: false,
                    enable_file_pagecache: false,
                    oss_config: None,
                },
                vec![FileScanRange {
                    path: path.to_string_lossy().to_string(),
                    file_len: std::fs::metadata(&path).expect("metadata").len(),
                    offset: 0,
                    length: std::fs::metadata(&path).expect("metadata").len(),
                    scan_range_id: 9,
                    first_row_id: Some(0),
                    external_datacache: None,
                }],
            )
            .expect("register glm scan ranges");

        let scan_range = encode_column_ipc(&(Arc::new(Int32Array::from(vec![9])) as ArrayRef))
            .expect("encode scan_range_id column");
        let row_id = encode_column_ipc(&(Arc::new(Int64Array::from(vec![1])) as ArrayRef))
            .expect("encode row_id column");
        let response = handle_lookup(proto::starrocks::PLookUpRequest {
            query_id: Some(unique_id(query_id.hi, query_id.lo)),
            lookup_node_id: Some(77),
            request_tuple_id: Some(tuple_id),
            request_columns: vec![
                proto::starrocks::PColumn {
                    slot_id: Some(2),
                    data_size: Some(scan_range.len() as i64),
                    data: Some(scan_range),
                },
                proto::starrocks::PColumn {
                    slot_id: Some(3),
                    data_size: Some(row_id.len() as i64),
                    data: Some(row_id),
                },
            ],
            lookup_slots: Vec::new(),
        });

        assert!(ok_status(response.status.as_ref()));
        assert_eq!(response.columns.len(), 1);
        assert_eq!(response.columns[0].slot_id, Some(4));
        let data = response.columns[0]
            .data
            .as_ref()
            .expect("lookup response column data");
        let values = decode_column_ipc(data).expect("decode lookup response column");
        let values = values
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 lookup response");
        assert_eq!(values.values(), &[20]);
    }
}
