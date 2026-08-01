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

mod common;
mod generic;
mod iceberg_metadata;
mod native_starrocks;
mod starrocks;
mod variant_path;

use super::context::NativePlanDecodeContext;
use super::error::{NativeFragmentDecodeError, NativeFragmentLeafDecodeError};
use super::node::DecodedNode;
use novarocks::exec::expr::ExprArena;
use novarocks::protocol::{FieldPath, ProtocolErrorKind};
use novarocks_protocol::plan;

pub(crate) fn lower_scan_node(
    node: &plan::DistributedNode,
    _physical: &plan::PlanNode,
    scan: &plan::ScanNode,
    path: FieldPath,
    ctx: &NativePlanDecodeContext,
    arena: &mut ExprArena,
) -> Result<DecodedNode, NativeFragmentDecodeError> {
    if !scan.dict_columns.is_empty() {
        return Err(NativeFragmentDecodeError::unsupported(
            path.clone().field("dict_columns"),
            "ScanNode dict_columns are not supported by native lowering yet",
        ));
    }
    let table = scan.table.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(path.clone().field("table"), "ScanNode table missing")
    })?;
    let source = table.source.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("table").field("source"),
            "ScanNode table source missing",
        )
    })?;
    let source = source.kind.as_ref().ok_or_else(|| {
        NativeFragmentDecodeError::missing(
            path.clone().field("table").field("source").field("kind"),
            "ScanNode table source kind missing",
        )
    })?;
    let source_path = path.clone().field("table").field("source");
    let output_columns = common::decode_scan_output_columns(scan, path.clone())?;
    match source {
        plan::scan_source::Kind::IcebergDataFiles(_) => {
            Err(NativeFragmentDecodeError::unsupported(
                source_path.field("iceberg_data_files"),
                "legacy IcebergDataFiles must be materialized as ConnectorReadSource before native decoding",
            ))
        }
        plan::scan_source::Kind::IcebergMetadataTable(source) => {
            reject_variant_columns_for_source(scan, "IcebergMetadataTable")
                .map_err(|error| error.into_native(path.clone()))?;
            iceberg_metadata::lower_iceberg_metadata_scan(
                node,
                scan,
                source,
                &output_columns,
                ctx,
                arena,
            )
            .map_err(|error| error.into_native(source_path.field("iceberg_metadata_table")))
        }
        plan::scan_source::Kind::IcebergVersionTable(_) => {
            Err(NativeFragmentDecodeError::unsupported(
                source_path.field("iceberg_version_table"),
                "IcebergVersionTable native scan source is not implemented",
            ))
        }
        plan::scan_source::Kind::IcebergMvTargetState(_) => {
            Err(NativeFragmentDecodeError::unsupported(
                source_path.field("iceberg_mv_target_state"),
                "IcebergMvTargetState native scan source is not implemented",
            ))
        }
        plan::scan_source::Kind::IcebergMvTargetLocator(_) => {
            Err(NativeFragmentDecodeError::unsupported(
                source_path.field("iceberg_mv_target_locator"),
                "IcebergMvTargetLocator native scan source is not implemented",
            ))
        }
        plan::scan_source::Kind::StarrocksTable(source) => {
            reject_variant_columns_for_source(scan, "StarRocksTable")
                .map_err(|error| error.into_native(path.clone()))?;
            starrocks::validate_starrocks_output_columns(&output_columns, source)?;
            starrocks::lower_starrocks_scan(node, scan, source, &output_columns, ctx, arena)
                .map_err(|error| error.into_native(source_path.field("starrocks_table")))
        }
        plan::scan_source::Kind::ConnectorRead(source) => {
            let variant_path_plan = variant_path::parse_native_scan_variant_path_columns(
                scan,
                table,
                output_columns.columns(),
            )
            .map_err(|error| error.into_native(path.clone()))?;
            generic::lower_connector_read_scan(
                node,
                scan,
                source,
                &output_columns,
                variant_path_plan,
                ctx,
                arena,
            )
            .map_err(|error| error.into_native(source_path.field("connector_read")))
        }
    }
}

fn reject_variant_columns_for_source(
    scan: &plan::ScanNode,
    source_name: &str,
) -> Result<(), NativeFragmentLeafDecodeError> {
    if scan.variant_columns.is_empty() {
        return Ok(());
    }
    Err(NativeFragmentLeafDecodeError::at_field(
        ProtocolErrorKind::Unsupported,
        "variant_columns",
        format!("{source_name} native scan does not support variant_columns"),
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::{Arc, Mutex};

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use novarocks_spi::connector::{
        ConnectorBatchReader, ConnectorCancellation, ConnectorError, ConnectorErrorKind,
        ConnectorExecutionBinding, ConnectorExecutionBindingKey, ConnectorExecutionResolver,
        ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorOpenReaderRequest,
        ConnectorProviderId, ConnectorReadExecution, ConnectorSplit,
    };
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::super::context::NativePlanDecodeContext;
    use super::super::node::{decode_node, decode_node_with_runtime_filters};
    use super::super::runtime_filter_binding::NativeRuntimeFilterDecodeLedger;
    use crate::native::type_decode::encode_type;
    use novarocks::common::ids::SlotId;
    use novarocks::connector::ConnectorRegistry;
    use novarocks::exec::expr::{ExprArena, ExprNode};
    use novarocks::exec::node::ExecNodeKind;
    use novarocks::exec::node::runtime_filter::{ArtifactMembershipSchema, NullSemantics};
    use novarocks::exec::node::scan::ScanMorsel;
    use novarocks::protocol::ProtocolErrorKind;
    use novarocks::runtime::query_options::{QueryOptions, QueryOptionsParts};
    use novarocks_protocol::{common, expr, novarocks as native_proto, plan};

    struct TestNotCancelled;

    impl ConnectorCancellation for TestNotCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn test_decode_context() -> NativePlanDecodeContext {
        NativePlanDecodeContext::default().with_connector_cancellation(Arc::new(TestNotCancelled))
    }

    fn type_desc(data_type: &DataType) -> common::TypeDesc {
        encode_type(data_type).expect("encode type")
    }

    fn output_column(column_id: u32, name: &str, data_type: DataType) -> common::OutputColumn {
        common::OutputColumn {
            column_id,
            name: name.to_string(),
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            is_internal: false,
        }
    }

    fn column_def(name: &str, data_type: DataType) -> plan::ColumnDef {
        plan::ColumnDef {
            name: name.to_string(),
            data_type: Some(type_desc(&data_type)),
            nullable: true,
            write_default_json: None,
            logical_type: None,
        }
    }

    fn schema_field(field_id: i32, name: &str) -> plan::IcebergSchemaFieldDef {
        plan::IcebergSchemaFieldDef {
            field_id,
            name: name.to_string(),
            initial_default_json: None,
            write_default_json: None,
            children: Vec::new(),
        }
    }

    fn table_info() -> plan::IcebergTableInfo {
        plan::IcebergTableInfo {
            catalog: "rest".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            table_uuid: None,
            current_snapshot_id: Some(1),
            schema_id: 7,
            location: "s3://bucket/warehouse/db/t".to_string(),
            schema: Some(plan::IcebergSchemaDef {
                fields: vec![schema_field(10, "id"), schema_field(11, "flag")],
            }),
            serialized_metadata: None,
            serialized_metadata_rows: None,
        }
    }

    fn variant_table_info() -> plan::IcebergTableInfo {
        plan::IcebergTableInfo {
            schema: Some(plan::IcebergSchemaDef {
                fields: vec![schema_field(101, "v")],
            }),
            ..table_info()
        }
    }

    fn scan_node(source: plan::scan_source::Kind) -> plan::DistributedNode {
        let columns = vec![output_column(1, "id", DataType::Int64)];
        scan_node_with(columns, Vec::new(), Vec::new(), source)
    }

    fn expected_connector_schema_ipc() -> Vec<u8> {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, true)]);
        let mut writer = StreamWriter::try_new(Vec::new(), &schema).expect("schema IPC writer");
        writer.finish().expect("finish schema IPC writer");
        writer.get_ref().clone()
    }

    fn scan_node_with(
        columns: Vec<common::OutputColumn>,
        predicates: Vec<expr::Expr>,
        required_columns: Vec<String>,
        source: plan::scan_source::Kind,
    ) -> plan::DistributedNode {
        plan::DistributedNode {
            node_id: 10,
            fragment_id: 0,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns: columns.clone(),
                kind: Some(plan::plan_node::Kind::Scan(plan::ScanNode {
                    database: "db".to_string(),
                    table: Some(plan::TableDef {
                        name: "t".to_string(),
                        columns: vec![
                            column_def("id", DataType::Int64),
                            column_def("flag", DataType::Boolean),
                        ],
                        iceberg_row_lineage_metadata_columns: Vec::new(),
                        source: Some(plan::ScanSource { kind: Some(source) }),
                    }),
                    alias: None,
                    columns,
                    predicates,
                    required_columns,
                    dict_columns: Vec::new(),
                    variant_columns: Vec::new(),
                    mv_rewritten_from: None,
                })),
            })),
        }
    }

    struct EmptyConnectorReader;

    impl ConnectorBatchReader for EmptyConnectorReader {
        fn next_batch(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
            Ok(None)
        }

        fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    struct NativeCarrierTestRead {
        key: ConnectorExecutionBindingKey,
    }

    impl ConnectorReadExecution for NativeCarrierTestRead {
        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.key
        }

        fn open_reader(
            &self,
            _split: &ConnectorSplit,
            _request: ConnectorOpenReaderRequest,
        ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
            Ok(Box::new(EmptyConnectorReader))
        }
    }

    struct SchemaRecordingRead {
        key: ConnectorExecutionBindingKey,
        expected_schema: Arc<Mutex<Option<Arc<Schema>>>>,
    }

    impl ConnectorReadExecution for SchemaRecordingRead {
        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.key
        }

        fn open_reader(
            &self,
            _split: &ConnectorSplit,
            request: ConnectorOpenReaderRequest,
        ) -> Result<Box<dyn ConnectorBatchReader>, ConnectorError> {
            *self.expected_schema.lock().expect("expected schema lock") =
                Some(request.expected_schema);
            Ok(Box::new(EmptyConnectorReader))
        }
    }

    struct TestExecutionResolver {
        binding: Option<Arc<ConnectorExecutionBinding>>,
    }

    impl ConnectorExecutionResolver for TestExecutionResolver {
        fn resolve(
            &self,
            key: &ConnectorExecutionBindingKey,
        ) -> Result<Arc<ConnectorExecutionBinding>, ConnectorError> {
            self.binding
                .as_ref()
                .filter(|binding| binding.key() == key)
                .cloned()
                .ok_or_else(|| {
                    ConnectorError::new(ConnectorErrorKind::NotFound, "test binding is absent")
                })
        }
    }

    fn connector_read_resolver(instance_id: &str) -> Arc<dyn ConnectorExecutionResolver> {
        let instance_id = ConnectorInstanceId::parse(instance_id).expect("instance ID");
        let key = ConnectorExecutionBindingKey {
            instance_id,
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
        };
        let binding = ConnectorExecutionBinding::try_new(
            ConnectorProviderId::parse("test").expect("provider ID"),
            key.clone(),
            Arc::new(NativeCarrierTestRead { key }),
        )
        .expect("execution binding");
        Arc::new(TestExecutionResolver {
            binding: Some(Arc::new(binding)),
        })
    }

    fn schema_recording_connector_read_resolver(
        instance_id: &str,
        expected_schema: Arc<Mutex<Option<Arc<Schema>>>>,
    ) -> Arc<dyn ConnectorExecutionResolver> {
        let instance_id = ConnectorInstanceId::parse(instance_id).expect("instance ID");
        let key = ConnectorExecutionBindingKey {
            instance_id,
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
        };
        let binding = ConnectorExecutionBinding::try_new(
            ConnectorProviderId::parse("test").expect("provider ID"),
            key.clone(),
            Arc::new(SchemaRecordingRead {
                key,
                expected_schema,
            }),
        )
        .expect("execution binding");
        Arc::new(TestExecutionResolver {
            binding: Some(Arc::new(binding)),
        })
    }

    #[test]
    fn native_connector_read_carrier_resolves_the_typed_host_and_executes_its_split() {
        let node = scan_node(plan::scan_source::Kind::ConnectorRead(
            plan::ConnectorReadSource {
                instance_id: "test.native".to_string(),
                instance_incarnation: vec![7; 16],
                scan_payload: vec![0],
                splits: vec![plan::ConnectorReadSplit {
                    split_id: "split-1".to_string(),
                    split_payload: vec![1, 2, 3],
                    estimated_bytes: Some(3),
                }],
                max_batch_rows: 128,
                max_batch_bytes: 4096,
                max_handle_payload_bytes: 1024,
                max_total_payload_bytes: 4096,
                expected_schema_ipc: expected_connector_schema_ipc(),
            },
        ));
        let context = test_decode_context()
            .with_execution_resolver(connector_read_resolver("test.native"))
            .with_query_id(novarocks_types::QueryId::new(7, 9));
        let decoded = decode_node(&node, &mut ExprArena::default(), &context)
            .expect("decode ConnectorReadSource");
        let ExecNodeKind::Scan(scan) = decoded.node.kind else {
            panic!("expected decoded scan node");
        };
        let op = scan
            .source()
            .bind(
                context
                    .captured_ranges_for_test(node.node_id)
                    .expect("scan decode captures ranges"),
            )
            .expect("bind generic connector source");
        let rows = op
            .execute_iter(
                ScanMorsel::ConnectorSplit {
                    index: 0,
                    row_position: None,
                },
                None,
                None,
            )
            .expect("open typed reader")
            .collect::<Result<Vec<_>, _>>()
            .expect("read typed split");
        assert!(rows.is_empty());
    }

    #[test]
    fn native_connector_read_carrier_preserves_field_metadata_for_provider_readers() {
        let mut metadata = HashMap::new();
        metadata.insert(PARQUET_FIELD_ID_META_KEY.to_string(), "42".to_string());
        let schema = Schema::new(vec![
            Field::new("renamed_amount", DataType::Float64, true).with_metadata(metadata),
        ]);
        let mut writer = StreamWriter::try_new(Vec::new(), &schema).expect("schema IPC writer");
        writer.finish().expect("finish schema IPC writer");
        let node = scan_node_with(
            vec![output_column(1, "renamed_amount", DataType::Float64)],
            Vec::new(),
            Vec::new(),
            plan::scan_source::Kind::ConnectorRead(plan::ConnectorReadSource {
                instance_id: "test.native".to_string(),
                instance_incarnation: vec![7; 16],
                scan_payload: vec![0],
                splits: vec![plan::ConnectorReadSplit {
                    split_id: "split-1".to_string(),
                    split_payload: vec![1],
                    estimated_bytes: Some(1),
                }],
                max_batch_rows: 128,
                max_batch_bytes: 4096,
                max_handle_payload_bytes: 1024,
                max_total_payload_bytes: 4096,
                expected_schema_ipc: writer.get_ref().clone(),
            }),
        );
        let recorded = Arc::new(Mutex::new(None));
        let context = test_decode_context()
            .with_execution_resolver(schema_recording_connector_read_resolver(
                "test.native",
                Arc::clone(&recorded),
            ))
            .with_query_id(novarocks_types::QueryId::new(7, 12));
        let decoded = decode_node(&node, &mut ExprArena::default(), &context)
            .expect("decode ConnectorReadSource");
        let ExecNodeKind::Scan(scan) = decoded.node.kind else {
            panic!("expected decoded scan node");
        };
        scan.source()
            .bind(
                context
                    .captured_ranges_for_test(node.node_id)
                    .expect("scan decode captures ranges"),
            )
            .expect("bind generic connector source")
            .execute_iter(
                ScanMorsel::ConnectorSplit {
                    index: 0,
                    row_position: None,
                },
                None,
                None,
            )
            .expect("open typed reader")
            .collect::<Result<Vec<_>, _>>()
            .expect("read typed split");
        let recorded = recorded
            .lock()
            .expect("expected schema lock")
            .clone()
            .expect("provider expected schema");
        assert_eq!(
            recorded.field(0).metadata().get(PARQUET_FIELD_ID_META_KEY),
            Some(&"42".to_string())
        );
    }

    #[test]
    fn native_connector_read_carrier_rejects_zero_batch_budget() {
        let node = scan_node(plan::scan_source::Kind::ConnectorRead(
            plan::ConnectorReadSource {
                instance_id: "test.native".to_string(),
                instance_incarnation: vec![7; 16],
                scan_payload: Vec::new(),
                splits: Vec::new(),
                max_batch_rows: 0,
                max_batch_bytes: 4096,
                max_handle_payload_bytes: 1024,
                max_total_payload_bytes: 4096,
                expected_schema_ipc: expected_connector_schema_ipc(),
            },
        ));
        let context = test_decode_context()
            .with_execution_resolver(connector_read_resolver("test.native"))
            .with_query_id(novarocks_types::QueryId::new(7, 10));
        let error = decode_node(&node, &mut ExprArena::default(), &context)
            .expect_err("zero batch budget must fail native decoding");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(protocol.kind(), ProtocolErrorKind::OutOfRange);
        assert!(
            protocol
                .path()
                .to_string()
                .ends_with("connector_read.max_batch_rows")
        );
    }

    #[test]
    fn native_connector_read_carrier_keeps_splits_opaque_to_core() {
        let node = scan_node(plan::scan_source::Kind::ConnectorRead(
            plan::ConnectorReadSource {
                instance_id: "test.native".to_string(),
                instance_incarnation: vec![7; 16],
                scan_payload: vec![0],
                splits: vec![plan::ConnectorReadSplit {
                    split_id: "file-1".to_string(),
                    split_payload: vec![1, 2, 3],
                    estimated_bytes: Some(3),
                }],
                max_batch_rows: 128,
                max_batch_bytes: 4096,
                max_handle_payload_bytes: 1024,
                max_total_payload_bytes: 4096,
                expected_schema_ipc: expected_connector_schema_ipc(),
            },
        ));
        let context = test_decode_context()
            .with_execution_resolver(connector_read_resolver("test.native"))
            .with_query_id(novarocks_types::QueryId::new(7, 11));
        let decoded = decode_node(&node, &mut ExprArena::default(), &context)
            .expect("decode ConnectorReadSource with opaque provider splits");
        let ExecNodeKind::Scan(scan) = decoded.node.kind else {
            panic!("expected decoded scan node");
        };
        let op = scan
            .source()
            .bind(
                context
                    .captured_ranges_for_test(node.node_id)
                    .expect("scan decode captures ranges"),
            )
            .expect("bind generic connector source");
        let morsels = op.build_morsels().expect("build generic connector morsels");
        assert!(matches!(
            &morsels.morsels[..],
            [ScanMorsel::ConnectorSplit { index: 0, .. }]
        ));
    }

    #[test]
    fn native_connector_read_carrier_rejects_unknown_instance_without_transport_fallback() {
        let node = scan_node(plan::scan_source::Kind::ConnectorRead(
            plan::ConnectorReadSource {
                instance_id: "unknown.native".to_string(),
                instance_incarnation: vec![7; 16],
                scan_payload: Vec::new(),
                splits: vec![plan::ConnectorReadSplit {
                    split_id: "file-1".to_string(),
                    split_payload: Vec::new(),
                    estimated_bytes: Some(100),
                }],
                max_batch_rows: 128,
                max_batch_bytes: 4096,
                max_handle_payload_bytes: 1024,
                max_total_payload_bytes: 4096,
                expected_schema_ipc: expected_connector_schema_ipc(),
            },
        ));
        let context = test_decode_context()
            .with_execution_resolver(Arc::new(TestExecutionResolver { binding: None }))
            .with_query_id(novarocks_types::QueryId::new(7, 12));
        let error = decode_node(&node, &mut ExprArena::default(), &context)
            .expect_err("unknown instances must not be materialized by native decoding");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(protocol.kind(), ProtocolErrorKind::InvalidValue);
        assert!(
            protocol
                .path()
                .to_string()
                .ends_with("connector_read.instance_id")
        );
    }

    fn assert_scan_column_type_error(
        columns: Vec<common::OutputColumn>,
        required_columns: Vec<String>,
        expected_index: usize,
    ) {
        let node = scan_node_with(
            columns,
            Vec::new(),
            required_columns,
            iceberg_metadata_table_source(),
        );
        let error = decode_node(&node, &mut ExprArena::default(), &test_decode_context())
            .expect_err("invalid scan column type must fail");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(
            protocol.path().to_string(),
            format!("plan_fragment.root.payload.physical.scan.columns[{expected_index}].type")
        );
        assert_eq!(protocol.kind(), ProtocolErrorKind::InvalidValue);
    }

    fn variant_scan_node() -> plan::DistributedNode {
        variant_scan_node_with_source_ids(1, 1)
    }

    fn variant_scan_node_with_source_ids(
        variant_source_column_id: u32,
        scan_source_column_id: u32,
    ) -> plan::DistributedNode {
        let output_columns = vec![output_column(2, "__nr_var_v_0", DataType::Int64)];
        let scan_columns = vec![
            output_column(scan_source_column_id, "v", DataType::LargeBinary),
            output_column(2, "__nr_var_v_0", DataType::Int64),
        ];
        plan::DistributedNode {
            node_id: 10,
            fragment_id: 0,
            tuple_ids: Vec::new(),
            nullable_tuple_ids: Vec::new(),
            limit: -1,
            runtime_filter_binding_ids: Vec::new(),
            children: Vec::new(),
            payload: Some(plan::distributed_node::Payload::Physical(plan::PlanNode {
                output_columns,
                kind: Some(plan::plan_node::Kind::Scan(plan::ScanNode {
                    database: "db".to_string(),
                    table: Some(plan::TableDef {
                        name: "t".to_string(),
                        columns: vec![
                            column_def("v", DataType::LargeBinary),
                            column_def("__nr_var_v_0", DataType::Int64),
                        ],
                        iceberg_row_lineage_metadata_columns: Vec::new(),
                        source: Some(plan::ScanSource {
                            kind: Some(plan::scan_source::Kind::IcebergDataFiles(
                                plan::IcebergDataFiles {
                                    table: Some(variant_table_info()),
                                    files: Vec::new(),
                                    binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
                                },
                            )),
                        }),
                    }),
                    alias: None,
                    columns: scan_columns,
                    predicates: Vec::new(),
                    required_columns: vec!["__nr_var_v_0".to_string()],
                    dict_columns: Vec::new(),
                    variant_columns: vec![plan::ScanVariantColumn {
                        source_column_id: variant_source_column_id,
                        source_column: "v".to_string(),
                        synthetic_column_id: 2,
                        synthetic_column: "__nr_var_v_0".to_string(),
                        canonical_path: "$.a.b".to_string(),
                        requested_type: Some(type_desc(&DataType::Int64)),
                        strict: true,
                    }],
                    mv_rewritten_from: None,
                })),
            })),
        }
    }

    fn column_ref(column_id: u32, name: &str, data_type: DataType) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            kind: Some(expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id,
                qualifier: None,
                column: Some(name.to_string()),
            })),
        }
    }

    fn membership_consumer_binding(binding_id: u32, node_id: i32) -> plan::RuntimeFilterBinding {
        let schema = ArtifactMembershipSchema::new(&DataType::Int64, NullSemantics::NeverMatches)
            .expect("membership schema");
        plan::RuntimeFilterBinding {
            binding_id,
            channel_id: 9,
            node_id,
            apply_point: i32::from(plan::RuntimeFilterApplyPoint::NodeInput),
            expression: Some(column_ref(1, "id", DataType::Int64)),
            contract: Some(plan::RuntimeFilterContract {
                kind: Some(plan::runtime_filter_contract::Kind::Membership(
                    plan::RuntimeFilterMembershipContract {
                        canonical_schema: schema.canonical_bytes().to_vec(),
                        schema_digest: schema.digest().bytes().to_vec(),
                    },
                )),
            }),
            reduction: Some(plan::RuntimeFilterReductionContract {
                kind: Some(plan::runtime_filter_reduction_contract::Kind::SetUnion(
                    true,
                )),
            }),
            role: Some(plan::runtime_filter_binding::Role::Consumer(
                plan::RuntimeFilterConsumerRole {
                    capabilities: vec![
                        i32::from(plan::RuntimeFilterArtifactCapability::Membership),
                        i32::from(plan::RuntimeFilterArtifactCapability::EmptyDomain),
                    ],
                    activation: Some(plan::RuntimeFilterConsumerActivation {
                        kind: Some(
                            plan::runtime_filter_consumer_activation::Kind::BlockingSnapshot(true),
                        ),
                    }),
                    target: Some(plan::runtime_filter_consumer_role::Target::SourceBoundary(
                        true,
                    )),
                },
            )),
        }
    }

    fn lower_delta_scan_with_binding(
        node: &mut plan::DistributedNode,
    ) -> super::super::node::DecodedNode {
        node.runtime_filter_binding_ids = vec![1];
        let table = plan::RuntimeFilterBindingTable {
            fragment_id: node.fragment_id,
            bindings: vec![membership_consumer_binding(1, node.node_id)],
        };
        let mut ledger = NativeRuntimeFilterDecodeLedger::decode(node.fragment_id, Some(&table))
            .expect("decode delta-scan consumer table");
        let lowered = decode_node_with_runtime_filters(
            node,
            &mut ExprArena::default(),
            &test_decode_context(),
            &mut ledger,
        )
        .expect("lower delta scan with dormant leaf-local consumer");
        ledger.finish().expect("delta-scan consumer consumed");
        lowered
    }

    fn file_range() -> native_proto::ScanRangeParams {
        native_proto::ScanRangeParams {
            range: Some(native_proto::ScanRange {
                kind: Some(native_proto::scan_range::Kind::File(
                    native_proto::FileScanRange {
                        file_format: "PARQUET".to_string(),
                        full_path: Some("s3://bucket/warehouse/db/t/data-1.parquet".to_string()),
                        relative_path: None,
                        table_id: None,
                        offset: 0,
                        length: 10,
                        file_length: 10,
                        delete_files: Vec::new(),
                        deletion_vector_descriptor: None,
                        first_row_id: None,
                        data_sequence_number: None,
                        modification_time: None,
                        datacache_options: None,
                        included_positions: Vec::new(),
                        serialized_split: None,
                        use_iceberg_jni_metadata_reader: false,
                        change_op: None,
                        file_pruning_min_max_values: HashMap::new(),
                    },
                )),
            }),
            volume_id: None,
            empty: None,
            has_more: None,
        }
    }

    fn starrocks_source() -> plan::scan_source::Kind {
        plan::scan_source::Kind::StarrocksTable(plan::StarRocksTableSource {
            catalog_name: "default_catalog".to_string(),
            db_id: 10,
            table_id: 20,
            schema_id: 30,
            storage_columns: vec![
                plan::StarRocksColumnStorageMeta {
                    name: "id".to_string(),
                    unique_id: 0,
                    default_value: None,
                },
                plan::StarRocksColumnStorageMeta {
                    name: "flag".to_string(),
                    unique_id: 12,
                    default_value: Some("false".to_string()),
                },
            ],
            current_schema: Some(plan::StarRocksTabletSchema {
                schema_id: 30,
                keys_type: plan::StarRocksKeysType::StarrocksKeysTypeDuplicate as i32,
                num_short_key_columns: Some(1),
                sort_key_idxes: vec![0],
                sort_key_unique_ids: vec![0],
                columns: vec![
                    plan::StarRocksColumnSchema {
                        unique_id: 0,
                        name: Some("id".to_string()),
                        physical_type: "BIGINT".to_string(),
                        is_key: Some(true),
                        aggregation: None,
                        nullable: Some(false),
                        default_value: None,
                        precision: None,
                        scale: None,
                        visible: Some(true),
                        children: vec![],
                    },
                    plan::StarRocksColumnSchema {
                        unique_id: 12,
                        name: Some("flag".to_string()),
                        physical_type: "BOOLEAN".to_string(),
                        is_key: Some(false),
                        aggregation: None,
                        nullable: Some(false),
                        default_value: Some("false".to_string()),
                        precision: None,
                        scale: None,
                        visible: Some(true),
                        children: vec![],
                    },
                ],
            }),
        })
    }

    fn starrocks_range(
        tablet_id: i64,
        partition_id: i64,
        version: i64,
    ) -> native_proto::ScanRangeParams {
        native_proto::ScanRangeParams {
            range: Some(native_proto::ScanRange {
                kind: Some(native_proto::scan_range::Kind::StarrocksTablet(
                    native_proto::StarRocksTabletScanRange {
                        tablet_id,
                        partition_id,
                        version,
                    },
                )),
            }),
            volume_id: None,
            empty: Some(false),
            has_more: Some(false),
        }
    }

    fn starrocks_empty_range(
        kind: Option<native_proto::scan_range::Kind>,
    ) -> native_proto::ScanRangeParams {
        native_proto::ScanRangeParams {
            range: kind.map(|kind| native_proto::ScanRange { kind: Some(kind) }),
            volume_id: None,
            empty: Some(true),
            has_more: Some(false),
        }
    }

    #[test]
    fn native_starrocks_scan_decode_defers_tablet_resolution_without_registry_mutation() {
        let tablet_id = 8_700_000_000_000_001;
        let query_id = novarocks_types::QueryId::new(8_700_000_000_000_002, 8_700_000_000_000_003);
        let node = scan_node(starrocks_source());
        let connectors = Arc::new(ConnectorRegistry::new());
        let ctx = test_decode_context()
            .with_connector_registry(connectors)
            .with_query_id(query_id)
            .with_scan_ranges(10, vec![starrocks_range(tablet_id, 100, 7)]);
        let mut arena = ExprArena::default();
        decode_node(&node, &mut arena, &ctx)
            .expect("valid StarRocks scan decode must defer tablet-path resolution");
    }

    fn int_literal(value: i64) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Int64)),
            nullable: false,
            kind: Some(expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::IntValue(value)),
                }),
            })),
        }
    }

    fn greater_than(left: expr::Expr, right: expr::Expr) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Boolean)),
            nullable: true,
            kind: Some(expr::expr::Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
                op: expr::BinaryOp::Gt as i32,
                left: Some(Box::new(left)),
                right: Some(Box::new(right)),
            }))),
        }
    }

    fn equals(left: expr::Expr, right: expr::Expr) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Boolean)),
            nullable: true,
            kind: Some(expr::expr::Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
                op: expr::BinaryOp::Eq as i32,
                left: Some(Box::new(left)),
                right: Some(Box::new(right)),
            }))),
        }
    }

    fn not_equals(left: expr::Expr, right: expr::Expr) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&DataType::Boolean)),
            nullable: true,
            kind: Some(expr::expr::Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
                op: expr::BinaryOp::Ne as i32,
                left: Some(Box::new(left)),
                right: Some(Box::new(right)),
            }))),
        }
    }

    fn iceberg_metadata_table_source() -> plan::scan_source::Kind {
        plan::scan_source::Kind::IcebergMetadataTable(plan::IcebergMetadataTable {
            table: Some(table_info()),
            metadata_table_type: plan::IcebergMetadataTableType::Snapshots as i32,
            serialized_table: "{}".to_string(),
            cloud_properties: HashMap::new(),
            metadata_payload: None,
        })
    }

    fn file_range_with_deletion_vector() -> native_proto::ScanRangeParams {
        let mut range = file_range();
        let Some(native_proto::scan_range::Kind::File(file)) =
            range.range.as_mut().and_then(|range| range.kind.as_mut())
        else {
            panic!("expected file range");
        };
        file.deletion_vector_descriptor = Some(native_proto::DeletionVectorDescriptor {
            storage_type: Some("PUFFIN".to_string()),
            path_or_inline_dv: Some("s3://bucket/warehouse/db/t/delete-1.puffin".to_string()),
            offset: Some(12),
            size_in_bytes: Some(34),
            cardinality: Some(2),
        });
        range
    }

    fn file_range_with_change_op_and_pruning() -> native_proto::ScanRangeParams {
        let mut range = file_range();
        let Some(native_proto::scan_range::Kind::File(file)) =
            range.range.as_mut().and_then(|range| range.kind.as_mut())
        else {
            panic!("expected file range");
        };
        file.change_op = Some(novarocks::exec::change_op::CHANGE_OP_DELETE.into());
        file.file_pruning_min_max_values = HashMap::from([(
            0,
            native_proto::FilePruningMinMaxValue {
                value_kind: 2,
                has_null: true,
                all_null: false,
                min_int_value: Some(10),
                max_int_value: Some(20),
                min_float_value: None,
                max_float_value: None,
            },
        )]);
        range
    }

    #[test]
    fn rejects_legacy_iceberg_data_file_scan_before_provider_binding() {
        let node = scan_node(plan::scan_source::Kind::IcebergDataFiles(
            plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            },
        ));
        let ctx = test_decode_context()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_query_options(Some(QueryOptions::from_parts(QueryOptionsParts {
                connector_io_tasks_per_scan_operator: Some(1),
                ..Default::default()
            })))
            .with_scan_ranges(10, vec![file_range()]);
        let error = decode_node(&node, &mut ExprArena::default(), &ctx)
            .expect_err("legacy Iceberg source must not bypass ConnectorReadSource");
        assert!(
            error
                .to_string()
                .contains("legacy IcebergDataFiles must be materialized as ConnectorReadSource")
        );
    }

    #[test]
    fn iceberg_metadata_invalid_column_type_uses_scan_columns_wire_path() {
        let mut invalid = output_column(1, "id", DataType::Int64);
        invalid.r#type = Some(common::TypeDesc::default());
        assert_scan_column_type_error(vec![invalid], Vec::new(), 0);
    }

    #[test]
    fn lowers_iceberg_metadata_scan_predicate_to_scan_conjunct() {
        let node = scan_node_with(
            vec![output_column(1, "snapshot_id", DataType::Int64)],
            vec![greater_than(
                column_ref(1, "snapshot_id", DataType::Int64),
                int_literal(0),
            )],
            Vec::new(),
            iceberg_metadata_table_source(),
        );
        let ctx = test_decode_context()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_scan_ranges(10, vec![file_range()]);
        let mut arena = ExprArena::default();

        let lowered =
            decode_node(&node, &mut arena, &ctx).expect("lower metadata scan with predicate");
        let ExecNodeKind::Scan(scan) = lowered.node.kind else {
            panic!("expected Scan");
        };
        assert!(scan.conjunct_predicate().is_some());
    }

    #[test]
    fn legacy_iceberg_scan_rejects_before_scan_range_validation() {
        let node = scan_node(plan::scan_source::Kind::IcebergDataFiles(
            plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            },
        ));
        let ctx =
            test_decode_context().with_connector_registry(Arc::new(ConnectorRegistry::default()));
        let mut arena = ExprArena::default();
        let err = decode_node(&node, &mut arena, &ctx).unwrap_err();
        assert!(
            err.contains("legacy IcebergDataFiles must be materialized as ConnectorReadSource"),
            "err={err}"
        );
    }

    #[test]
    fn legacy_iceberg_scan_rejects_even_with_predicate_only_required_columns() {
        let node = scan_node_with(
            vec![output_column(1, "id", DataType::Int64)],
            vec![column_ref(2, "flag", DataType::Boolean)],
            vec!["id".to_string(), "flag".to_string()],
            plan::scan_source::Kind::IcebergDataFiles(plan::IcebergDataFiles {
                table: Some(table_info()),
                files: Vec::new(),
                binding: plan::IcebergDataFileBinding::ExplicitFiles as i32,
            }),
        );
        let ctx = test_decode_context()
            .with_connector_registry(Arc::new(ConnectorRegistry::default()))
            .with_scan_ranges(10, vec![file_range()]);
        let mut arena = ExprArena::default();
        let err = decode_node(&node, &mut arena, &ctx).unwrap_err();
        assert!(
            err.contains("legacy IcebergDataFiles must be materialized as ConnectorReadSource"),
            "err={err}"
        );
    }
}
