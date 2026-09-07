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

//! Test-only connector write fixtures shared by the plan decoder and the
//! fragment service.
//!
//! The stubs here stand in for a provider generation: they mint and recover
//! provider write values through the SPI's own adapter, so nothing in this
//! module can forge a value the real seam would refuse. They deliberately
//! provide no commit authority, because a backend never has one.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use novarocks_proto_codec::connector_write::{
    ConnectorWriteCodecError, ConnectorWriteFragmentEncoder, ConnectorWriteHandleDecoder,
    ValidatedWriterHandle,
};
use novarocks_proto_codec::{FieldPath, arrow_physical};
use novarocks_proto_models::connector_write as write_dto;
use novarocks_proto_models::{catalog as catalog_dto, plan};
use novarocks_spi::connector::write_stack::{
    ConnectorBatchWriter, ConnectorCommitFragment, ConnectorOpenWriterRequest,
    ConnectorWriteExecution, ConnectorWriterHandle, ProviderWriteRuntime, RootWriteResultSchema,
    WriteRuntimeAdapter, WriterMultiplexSchema,
};
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, CatalogWriteExecution, ConnectorError, ConnectorErrorKind,
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId,
};
use novarocks_types::{QueryExecutionId, UniqueId};

use crate::connector::ConnectorExecutionWriteBinding;

pub(crate) const TEST_WRITE_CATALOG: &str = "write_catalog";

#[derive(Debug)]
pub(crate) struct StubProviderRuntime {
    descriptor: ConnectorInstanceDescriptor,
    catalog_handle: CatalogHandle,
}

impl ProviderWriteRuntime for StubProviderRuntime {
    type CommitHandle = ();
    type WriterHandle = write_dto::IcebergWriterHandle;
    type CommitFragment = write_dto::ConnectorCommitFragment;

    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }
}

pub(crate) fn test_write_catalog_handle() -> CatalogHandle {
    CatalogHandle::new(
        ConnectorInstanceId::try_from_canonical(TEST_WRITE_CATALOG).expect("canonical instance id"),
        CatalogVersion::from_bytes([9; 32]),
    )
}

pub(crate) fn test_write_adapter() -> WriteRuntimeAdapter<StubProviderRuntime> {
    let handle = test_write_catalog_handle();
    WriteRuntimeAdapter::new(Arc::new(StubProviderRuntime {
        descriptor: ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider id"),
            instance_id: handle.catalog_name().clone(),
        },
        catalog_handle: handle,
    }))
}

struct StubHandleDecoder {
    adapter: WriteRuntimeAdapter<StubProviderRuntime>,
}

impl ConnectorWriteHandleDecoder for StubHandleDecoder {
    fn owner(&self) -> &str {
        TEST_WRITE_CATALOG
    }

    fn decode_writer_handle(
        &self,
        handle: &ValidatedWriterHandle,
    ) -> Result<ConnectorWriterHandle, ConnectorWriteCodecError> {
        Ok(self.adapter.wrap_writer_handle(handle.iceberg().clone()))
    }
}

struct StubFragmentEncoder {
    adapter: WriteRuntimeAdapter<StubProviderRuntime>,
}

impl ConnectorWriteFragmentEncoder for StubFragmentEncoder {
    fn owner(&self) -> &str {
        TEST_WRITE_CATALOG
    }

    fn encode_commit_fragment(
        &self,
        fragment: &ConnectorCommitFragment,
    ) -> Result<write_dto::ConnectorCommitFragment, ConnectorWriteCodecError> {
        self.adapter
            .commit_fragment(fragment)
            .cloned()
            .map_err(|error| {
                ConnectorWriteCodecError::invalid(
                    TEST_WRITE_CATALOG,
                    FieldPath::root("commit_fragment"),
                    error.to_string(),
                )
            })
    }
}

/// Records every writer it opened, so a test can prove each driver received its
/// own writer and its own physical context.
pub(crate) struct RecordingWriteExecution {
    catalog_handle: CatalogHandle,
    opened: Mutex<Vec<(u32, u32, u32)>>,
    terminals: Arc<Mutex<WriterTerminals>>,
}

/// How the writers this execution handed out ended.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct WriterTerminals {
    pub(crate) finished: usize,
    pub(crate) aborted: usize,
    pub(crate) appended_rows: usize,
}

impl RecordingWriteExecution {
    pub(crate) fn new() -> Self {
        Self {
            catalog_handle: test_write_catalog_handle(),
            opened: Mutex::new(Vec::new()),
            terminals: Arc::new(Mutex::new(WriterTerminals::default())),
        }
    }

    /// `(driver_id, writer_ordinal, write_target_ordinal)` per opened writer.
    pub(crate) fn opened(&self) -> Vec<(u32, u32, u32)> {
        self.opened.lock().expect("opened writers").clone()
    }
}

struct RecordingWriter {
    terminals: Arc<Mutex<WriterTerminals>>,
}

#[async_trait::async_trait]
impl ConnectorBatchWriter for RecordingWriter {
    async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        self.terminals
            .lock()
            .expect("writer terminals")
            .appended_rows += batch.num_rows();
        Ok(())
    }

    async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
        self.terminals.lock().expect("writer terminals").finished += 1;
        Ok(Vec::new())
    }

    async fn abort(&mut self) -> Result<(), ConnectorError> {
        self.terminals.lock().expect("writer terminals").aborted += 1;
        Ok(())
    }
}

#[async_trait::async_trait]
impl ConnectorWriteExecution for RecordingWriteExecution {
    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }

    async fn open_writer(
        &self,
        request: ConnectorOpenWriterRequest,
    ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
        self.opened.lock().expect("opened writers").push((
            request.physical.driver_id(),
            request.physical.writer_ordinal(),
            request.target.get(),
        ));
        Ok(Box::new(RecordingWriter {
            terminals: Arc::clone(&self.terminals),
        }))
    }
}

/// The catalog-scoped capability member of the role binding. It carries the
/// handle and nothing else; a write binding is a complete group by
/// construction.
struct UnusedCatalogWriteExecution {
    catalog_handle: CatalogHandle,
}

impl CatalogWriteExecution for UnusedCatalogWriteExecution {
    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }
}

pub(crate) fn test_write_binding(
    execution: Arc<RecordingWriteExecution>,
) -> ConnectorExecutionWriteBinding {
    ConnectorExecutionWriteBinding::new(
        Arc::new(UnusedCatalogWriteExecution {
            catalog_handle: test_write_catalog_handle(),
        }),
        execution,
        Arc::new(StubHandleDecoder {
            adapter: test_write_adapter(),
        }),
        Arc::new(StubFragmentEncoder {
            adapter: test_write_adapter(),
        }),
    )
}

struct NeverCancelled;

impl novarocks_spi::connector::ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

struct NoVendedStorage;

impl novarocks_spi::connector::ConnectorStorageResolver for NoVendedStorage {
    fn resolve_vended_s3(
        &self,
        _: &novarocks_spi::connector::StorageAccessRequest,
    ) -> Result<novarocks_spi::connector::ResolvedVendedS3Access, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "test fixture has no vended storage lease",
        ))
    }
}

pub(crate) fn never_cancelled() -> Arc<dyn novarocks_spi::connector::ConnectorCancellation> {
    Arc::new(NeverCancelled)
}

pub(crate) fn test_request_context() -> novarocks_spi::connector::ConnectorRequestContext {
    novarocks_spi::connector::ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(60),
        never_cancelled(),
        novarocks_spi::connector::MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
        novarocks_spi::connector::MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    )
    .expect("request context")
}

/// A typed runtime whose write resolver answers only for the test catalog.
pub(crate) fn test_write_scan_runtime(
    execution_id: QueryExecutionId,
    fragment_instance_id: UniqueId,
    execution: Arc<RecordingWriteExecution>,
) -> crate::fragment::decode::plan::context::TypedScanRuntime {
    let binding = test_write_binding(execution);
    let expected = test_write_catalog_handle();
    let queues = novarocks_execution::connector::SplitQueueRegistry::new().open_attempt(
        novarocks_execution::connector::TaskAttemptKey::new(execution_id, fragment_instance_id),
        novarocks_execution::connector::SplitQueueConfig::default(),
    );
    let session = novarocks_spi::connector::read_stack::ConnectorSession::try_new(
        "write-test",
        "novarocks",
        "UTC",
        "en_US",
        std::time::SystemTime::UNIX_EPOCH,
    )
    .expect("session");
    crate::fragment::decode::plan::context::TypedScanRuntime::new(
        execution_id,
        Arc::new(|_| Err("no query-leased test read runtime".to_owned())),
        Arc::new(move |handle| {
            if handle == &expected {
                Ok(binding.clone())
            } else {
                Err(format!(
                    "no query-leased catalog runtime exists for {}",
                    handle.catalog_name().as_str()
                ))
            }
        }),
        queues,
        session,
        Arc::new(|| Ok(None)),
        Arc::new(crate::fragment::ingress::TypedReadAttemptContext::new()),
        Arc::new(NoVendedStorage),
    )
}

// --------------------------------------------------------------- wire builders

pub(crate) fn wire_catalog_handle(name: &str) -> catalog_dto::CatalogHandle {
    catalog_dto::CatalogHandle {
        catalog_name: name.to_string(),
        version: vec![9; 32],
    }
}

pub(crate) fn iceberg_writer_handle(table_uuid: String) -> write_dto::ConnectorWriterHandle {
    write_dto::ConnectorWriterHandle {
        handle: Some(write_dto::connector_writer_handle::Handle::Iceberg(
            write_dto::IcebergWriterHandle {
                branch: write_dto::IcebergWriteBranch::Data as i32,
                table: Some(write_dto::IcebergWriteTableFacts {
                    table_uuid,
                    namespace: "db".to_string(),
                    table_name: "t".to_string(),
                    table_location: "s3://bucket/db/t".to_string(),
                    data_location: "s3://bucket/db/t/data".to_string(),
                    target_ref: "main".to_string(),
                    base_snapshot_id: None,
                    base_sequence_number: 0,
                    schema_id: 0,
                    default_partition_spec_id: 0,
                    format_version: 2,
                }),
                output: Some(write_dto::IcebergWriterOutput {
                    file_format: write_dto::IcebergFileFormat::Parquet as i32,
                    compression: write_dto::IcebergCompression::Zstd as i32,
                    parquet_row_group_size_bytes: None,
                }),
                data: Some(write_dto::IcebergDataBranchRecipe {
                    input_schema_json: None,
                    partition_source_column_names: Vec::new(),
                    partition_column_names: Vec::new(),
                    transform_exprs: Vec::new(),
                    row_lineage: false,
                }),
                old_deletes: BTreeMap::new(),
                equality: None,
            },
        )),
    }
}

pub(crate) fn table_writer_payload(
    output_expr: novarocks_proto_models::expr::Expr,
    target_schema: Vec<novarocks_proto_models::common::OutputColumn>,
) -> plan::TableWriterNode {
    plan::TableWriterNode {
        catalog_handle: Some(wire_catalog_handle(TEST_WRITE_CATALOG)),
        write_target_ordinal: 0,
        handle: Some(iceberg_writer_handle(
            "9c2f1f66-1f0f-4c9a-9a1a-3f1d2c0b7a11".to_string(),
        )),
        input: Some(plan::ConnectorWriteInputBinding {
            kind: Some(plan::connector_write_input_binding::Kind::RootOutputByOrdinal(true)),
        }),
        writer_ordinal: 0,
        output_exprs: vec![output_expr],
        target_schema,
        writer_multiplex_schema: Some(writer_multiplex_schema(&WriterMultiplexSchema::empty())),
        partial_aggregate_plan: Some(plan::WriterPartialAggregatePlan { calls: Vec::new() }),
    }
}

pub(crate) fn writer_multiplex_schema(
    schema: &WriterMultiplexSchema,
) -> plan::WriterMultiplexSchema {
    let slot_ids = schema.slot_ids();
    let (columns, schema_metadata) = arrow_physical::encode_schema(
        schema.arrow_schema().as_ref(),
        &slot_ids,
        true,
        FieldPath::root("writer_multiplex_schema"),
    )
    .expect("fixture relation schema");
    plan::WriterMultiplexSchema {
        contract_version: schema.contract_version(),
        columns,
        schema_metadata,
    }
}

pub(crate) fn root_result_schema(schema: &RootWriteResultSchema) -> plan::RootWriteResultSchema {
    let slot_ids = schema.slot_ids();
    let arrow_schema = schema.arrow_schema();
    let (columns, schema_metadata) = arrow_physical::encode_schema(
        arrow_schema.as_ref(),
        &slot_ids,
        true,
        FieldPath::root("root_result_schema"),
    )
    .expect("fixture relation schema");
    plan::RootWriteResultSchema {
        contract_version: schema.contract_version(),
        columns,
        schema_metadata,
    }
}

pub(crate) fn writer_node(
    node_id: i32,
    writer: plan::TableWriterNode,
    children: Vec<plan::DistributedNode>,
) -> plan::DistributedNode {
    plan::DistributedNode {
        node_id,
        fragment_id: 1,
        tuple_ids: Vec::new(),
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children,
        payload: Some(plan::distributed_node::Payload::TableWriter(writer)),
    }
}

pub(crate) fn finish_node(
    node_id: i32,
    expected_target_ordinals: Vec<u32>,
    children: Vec<plan::DistributedNode>,
) -> plan::DistributedNode {
    plan::DistributedNode {
        node_id,
        fragment_id: 1,
        tuple_ids: Vec::new(),
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children,
        payload: Some(plan::distributed_node::Payload::TableFinish(
            plan::TableFinishNode {
                expected_target_ordinals,
                writer_multiplex_schema: Some(writer_multiplex_schema(
                    &WriterMultiplexSchema::empty(),
                )),
                root_result_schema: Some(root_result_schema(&RootWriteResultSchema::new())),
                final_aggregate_plan: Some(plan::WriterFinalAggregatePlan {
                    calls: Vec::new(),
                    unpivot: None,
                }),
            },
        )),
    }
}
