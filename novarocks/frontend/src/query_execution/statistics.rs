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

//! Frontend ownership for ordinary distributed statistics execution.
//!
//! Connector control freezes the exact scan inputs, aggregate functions, and
//! artifact identities. SQL then plans only ordinary scan/aggregate/exchange/
//! unpivot/result operators. This module retains the frozen expectations and
//! validates the Root result stream before the provider session may finish.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, BinaryArray, Int32Array, ListArray, MapArray, StringArray};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorControlResolver, ConnectorReadSelector,
    ConnectorRequestContext, StatisticsArtifactDraft, StatisticsArtifactIdentity,
    StatisticsDataVersion, StatisticsRequiredAggregation,
};

use crate::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, DistributedQueryRequest,
    build_statistics_query_request_with_execution,
};

const MAX_STATISTICS_ROOT_ROWS: usize = 4096;

fn charge_statistics_body_bytes(current: usize, body_bytes: usize) -> Result<usize, String> {
    let charged = current
        .checked_add(body_bytes)
        .ok_or_else(|| "statistics Root body budget overflow".to_string())?;
    if charged > novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_RESULT_BODY_BYTES {
        return Err("statistics Root body budget exceeded".into());
    }
    Ok(charged)
}

fn artifact_input_fields_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Int32, false)))
}

fn artifact_properties_type() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, false),
            ])),
            false,
        )),
        false,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsExecutionMode {
    SynchronousWait,
    ProcessJobAttempt,
}

impl StatisticsExecutionMode {
    pub const fn statement_cancellation_terminates_execution(self) -> bool {
        matches!(self, Self::SynchronousWait)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StatisticsExecutionPolicy {
    mode: StatisticsExecutionMode,
    attempt_timeout: Duration,
}

impl StatisticsExecutionPolicy {
    pub fn try_new(
        mode: StatisticsExecutionMode,
        attempt_timeout: Duration,
    ) -> Result<Self, DistributedQueryError> {
        if attempt_timeout.is_zero() {
            return Err(contract_violation(
                "statistics attempt timeout must be greater than zero",
            ));
        }
        Ok(Self {
            mode,
            attempt_timeout,
        })
    }

    pub const fn mode(self) -> StatisticsExecutionMode {
        self.mode
    }

    pub const fn attempt_timeout(self) -> Duration {
        self.attempt_timeout
    }
}

/// Move-only execution contract retained by the FE coordinator until Root EOF
/// and every required execution terminal have both been observed.
pub struct StatisticsCollectionProgram {
    table: novarocks_spi::connector::ConnectorTableHandle,
    data_version: StatisticsDataVersion,
    read_version_ordinal: i64,
    required: Vec<StatisticsRequiredAggregation>,
    policy: StatisticsExecutionPolicy,
}

impl StatisticsCollectionProgram {
    pub fn try_new(
        table: novarocks_spi::connector::ConnectorTableHandle,
        data_version: StatisticsDataVersion,
        read_version_ordinal: Option<i64>,
        required: Vec<StatisticsRequiredAggregation>,
        policy: StatisticsExecutionPolicy,
    ) -> Result<Self, DistributedQueryError> {
        if required.is_empty() {
            return Err(contract_violation(
                "empty statistics requirements must bypass distributed execution",
            ));
        }
        let read_version_ordinal = read_version_ordinal.ok_or_else(|| {
            contract_violation("statistics execution has no exact read version ordinal")
        })?;
        let identities = required
            .iter()
            .map(|requirement| requirement.artifact().clone())
            .collect::<BTreeSet<_>>();
        if identities.len() != required.len() {
            return Err(contract_violation(
                "statistics requirements contain duplicate artifact identities",
            ));
        }
        let mut inputs = BTreeMap::<usize, (&str, &DataType, bool)>::new();
        for requirement in &required {
            let input = requirement.input();
            if let Some((name, data_type, nullable)) = inputs.insert(
                input.ordinal(),
                (input.name(), input.data_type(), input.nullable()),
            ) && (name != input.name()
                || data_type != input.data_type()
                || nullable != input.nullable())
            {
                return Err(contract_violation(
                    "statistics requirements disagree on a scan column ordinal",
                ));
            }
        }
        Ok(Self {
            table,
            data_version,
            read_version_ordinal,
            required,
            policy,
        })
    }

    pub const fn policy(&self) -> StatisticsExecutionPolicy {
        self.policy
    }

    pub fn table(&self) -> &novarocks_spi::connector::ConnectorTableHandle {
        &self.table
    }

    pub fn data_version(&self) -> &StatisticsDataVersion {
        &self.data_version
    }

    pub const fn read_version_ordinal(&self) -> i64 {
        self.read_version_ordinal
    }

    pub fn required_aggregations(&self) -> &[StatisticsRequiredAggregation] {
        &self.required
    }

    pub fn scan_columns(&self) -> Vec<novarocks_spi::connector::StatisticsScanColumn> {
        let mut columns = self
            .required
            .iter()
            .map(|requirement| requirement.input().clone())
            .collect::<Vec<_>>();
        columns.sort_by_key(|column| column.ordinal());
        columns.dedup_by_key(|column| column.ordinal());
        columns
    }

    pub fn result_decoder(&self) -> StatisticsRootResultDecoder {
        StatisticsRootResultDecoder::new(
            self.required
                .iter()
                .map(|requirement| requirement.artifact().clone()),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatisticsRelationIdentity {
    catalog: String,
    namespace: String,
    table: String,
}

impl StatisticsRelationIdentity {
    pub fn try_new(
        catalog: impl Into<String>,
        namespace: impl Into<String>,
        table: impl Into<String>,
    ) -> Result<Self, DistributedQueryError> {
        let identity = Self {
            catalog: catalog.into(),
            namespace: namespace.into(),
            table: table.into(),
        };
        if identity.catalog.is_empty() || identity.namespace.is_empty() || identity.table.is_empty()
        {
            return Err(contract_violation(
                "statistics collection relation identity is incomplete",
            ));
        }
        Ok(identity)
    }

    fn fqn(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.namespace, self.table)
    }
}

pub struct PreparedStatisticsCollectionRequest {
    encoding: crate::query_execution::post_compile::NativeFragmentEncodingInput,
    program: StatisticsCollectionProgram,
    execution: crate::common::admitted_query_context::QueryExecutionContext,
}

impl PreparedStatisticsCollectionRequest {
    pub fn encoding_view(
        &self,
    ) -> crate::query_execution::native_fragment::NativeFragmentEncodingView<'_> {
        self.encoding.encoding_view()
    }

    pub fn finish(
        self,
        native_attachment: crate::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> Result<DistributedQueryRequest, DistributedQueryError> {
        build_statistics_query_request_with_execution(
            self.encoding,
            native_attachment,
            None,
            self.program,
            &self.execution,
        )
    }
}

/// Frontend planning authorities used to turn one provider-frozen statistics
/// program into an ordinary distributed query.
pub struct StatisticsPlanningServices<'a> {
    controls: &'a dyn ConnectorControlResolver,
    typed_connector_control: &'a Arc<crate::connector::ConnectorControlHost>,
    functions: &'a dyn novarocks_sql::compiler::SqlFunctionCatalog,
}

impl<'a> StatisticsPlanningServices<'a> {
    pub fn new(
        controls: &'a dyn ConnectorControlResolver,
        typed_connector_control: &'a Arc<crate::connector::ConnectorControlHost>,
        functions: &'a dyn novarocks_sql::compiler::SqlFunctionCatalog,
    ) -> Self {
        Self {
            controls,
            typed_connector_control,
            functions,
        }
    }
}

pub fn prepare_statistics_collection_request(
    services: StatisticsPlanningServices<'_>,
    execution: &crate::common::admitted_query_context::QueryExecutionContext,
    context: ConnectorRequestContext,
    identity: &StatisticsRelationIdentity,
    program: StatisticsCollectionProgram,
    planning_lease: ConnectorControlPlanningLease,
) -> Result<PreparedStatisticsCollectionRequest, DistributedQueryError> {
    let StatisticsPlanningServices {
        controls,
        typed_connector_control,
        functions,
    } = services;
    if execution.topology().targets().is_empty() {
        return Err(DistributedQueryError::new(
            DistributedQueryErrorKind::Rejected,
            "statistics collection requires at least one live backend",
        ));
    }
    if planning_lease.binding().descriptor().instance_id != *program.table().owner() {
        return Err(contract_violation(
            "statistics collection planning lease does not own its resolved table handle",
        ));
    }
    if planning_lease.binding().descriptor().instance_id.as_str() != identity.catalog {
        return Err(contract_violation(format!(
            "statistics collection of `{}` was planned through connector instance `{}`",
            identity.fqn(),
            planning_lease.binding().descriptor().instance_id.as_str()
        )));
    }
    let table_bindings = Arc::new(
        crate::catalog_application::query_bindings::QueryTableBindingStore::try_new()
            .map_err(contract_violation)?,
    );
    let source_binding =
        admit_statistics_scan_binding(table_bindings.as_ref(), identity, &program, planning_lease)?;
    let distributed = novarocks_sql::planning::dml::build_statistics_connector_plan(
        novarocks_sql::planning::dml::StatisticsConnectorScan {
            binding: source_binding,
            catalog: identity.catalog.clone(),
            namespace: identity.namespace.clone(),
            table: identity.table.clone(),
            version_ordinal: program.read_version_ordinal(),
            columns: program.scan_columns(),
        },
        program.required_aggregations(),
        functions,
        execution.optimizer_settings(),
    )
    .map_err(contract_violation)?;
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed,
        controls,
        &context,
        Some(table_bindings.as_ref()),
        None,
        crate::query_execution::compiler::scan_preparation_options(
            typed_connector_control,
            execution.optimizer_settings(),
        )
        .map_err(contract_violation)?,
    )
    .map_err(contract_violation)?;
    Ok(PreparedStatisticsCollectionRequest {
        encoding: crate::query_execution::post_compile::NativeFragmentEncodingInput::new(prepared),
        program,
        execution: execution.clone(),
    })
}

fn admit_statistics_scan_binding(
    bindings: &crate::catalog_application::query_bindings::QueryTableBindingStore,
    identity: &StatisticsRelationIdentity,
    program: &StatisticsCollectionProgram,
    planning_lease: ConnectorControlPlanningLease,
) -> Result<novarocks_sql::binding::SqlTableBindingId, DistributedQueryError> {
    let input_schema = Arc::new(Schema::new(
        program
            .scan_columns()
            .iter()
            .map(|column| Field::new(column.name(), column.data_type().clone(), column.nullable()))
            .collect::<Vec<_>>(),
    ));
    let scan_identity =
        novarocks_sql::planning::query_execution::FrozenConnectorScanIdentity::try_new(
            identity.catalog.as_str(),
            identity.namespace.as_str(),
            identity.table.as_str(),
        )
        .map_err(contract_violation)?;
    let version_ordinal = program.read_version_ordinal();
    let key = crate::catalog_application::query_bindings::QueryTableBindingKey::snapshot(
        &identity.catalog,
        &identity.namespace,
        &identity.table,
        version_ordinal,
    );
    bindings
        .resolve_or_insert_with_id(key, |binding| {
            Ok(crate::catalog_application::query_bindings::QueryTableBinding {
                resolved: novarocks_sql::planning::query_execution::pinned_version_resolved_analyzer_table(
                    &scan_identity,
                    input_schema.clone(),
                    binding,
                    version_ordinal,
                ),
                statistics_pin: None,
                admission: crate::catalog_application::query_bindings::QueryTableBindingAdmission::Exact(
                    planning_lease.clone(),
                ),
                scan_materialization: Some(
                    crate::catalog_application::query_bindings::QueryScanMaterialization {
                        table: program.table().clone(),
                        catalog_handle: planning_lease
                            .binding()
                            .catalog_handle()
                            .map_err(|error| error.to_string())?
                            .clone(),
                        schema: input_schema.clone(),
                        selector: ConnectorReadSelector::SnapshotId(version_ordinal),
                        statistics_pin: None,
                        planning_lease: planning_lease.clone(),
                    },
                ),
                mv_target_read: None,
                write_target_admission: None,
                frozen_snapshot_materializations: BTreeMap::new(),
                admitted_change_scans: BTreeMap::new(),
            })
        })
        .map_err(contract_violation)
}

/// Streaming decoder for the ordinary Root Result relation.
///
/// Identity and body are data-plane values. Properties are intentionally empty
/// for ANALYZE; the provider session validates the compact body and derives
/// provider metadata while consuming `finish`.
pub struct StatisticsRootResultDecoder {
    expected: BTreeSet<StatisticsArtifactIdentity>,
    observed: BTreeMap<StatisticsArtifactIdentity, StatisticsArtifactDraft>,
    body_bytes: usize,
    root_eof: bool,
    execution_succeeded: bool,
}

impl StatisticsRootResultDecoder {
    fn new(expected: impl IntoIterator<Item = StatisticsArtifactIdentity>) -> Self {
        Self {
            expected: expected.into_iter().collect(),
            observed: BTreeMap::new(),
            body_bytes: 0,
            root_eof: false,
            execution_succeeded: false,
        }
    }

    pub fn apply_chunk(
        &mut self,
        chunk: &novarocks_execution::exec::chunk::Chunk,
    ) -> Result<(), String> {
        if self.root_eof {
            return Err("statistics Root emitted a trailing batch after EOF".into());
        }
        let batch = &chunk.batch;
        let schema = batch.schema();
        let expected_schema = Schema::new(vec![
            Field::new("input_fields", artifact_input_fields_type(), false),
            Field::new("blob_type", DataType::Utf8, false),
            Field::new("body", DataType::Binary, true),
            Field::new("properties", artifact_properties_type(), false),
        ]);
        if schema.as_ref() != &expected_schema {
            return Err(format!(
                "statistics Root schema mismatch: expected {expected_schema:?}, received {schema:?}"
            ));
        }
        if self
            .observed
            .len()
            .checked_add(batch.num_rows())
            .is_none_or(|rows| rows > MAX_STATISTICS_ROOT_ROWS)
        {
            return Err("statistics Root row budget exceeded".into());
        }
        let input_fields = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| "statistics Root input_fields is not List<Int32>".to_string())?;
        let blob_types = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| "statistics Root blob_type is not Utf8".to_string())?;
        let bodies = batch
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| "statistics Root body is not Binary".to_string())?;
        let properties = batch
            .column(3)
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| "statistics Root properties is not Map<Utf8, Utf8>".to_string())?;
        for row in 0..batch.num_rows() {
            if input_fields.is_null(row)
                || blob_types.is_null(row)
                || bodies.is_null(row)
                || properties.is_null(row)
            {
                return Err("statistics Root artifact row contains a null value".into());
            }
            let fields = input_fields.value(row);
            let fields = fields
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "statistics Root input_fields item is not Int32".to_string())?;
            if fields.null_count() != 0 {
                return Err("statistics Root input_fields contains a null item".into());
            }
            let field_ids = fields.values().to_vec();
            if field_ids.iter().collect::<BTreeSet<_>>().len() != field_ids.len() {
                return Err("statistics Root input_fields contains a duplicate field ID".into());
            }
            let property_offsets = properties.value_offsets();
            let property_count = usize::try_from(property_offsets[row + 1] - property_offsets[row])
                .map_err(|_| "statistics Root properties offset is invalid".to_string())?;
            if property_count != 0 {
                return Err("ANALYZE statistics Root properties must be empty".into());
            }
            let identity = StatisticsArtifactIdentity::try_new(field_ids, blob_types.value(row))
                .map_err(|error| error.to_string())?;
            if !self.expected.contains(&identity) {
                return Err(format!(
                    "statistics Root emitted unexpected artifact identity {identity:?}"
                ));
            }
            if self.observed.contains_key(&identity) {
                return Err(format!(
                    "statistics Root emitted duplicate artifact identity {identity:?}"
                ));
            }
            self.body_bytes =
                charge_statistics_body_bytes(self.body_bytes, bodies.value(row).len())?;
            let draft = StatisticsArtifactDraft::try_new(
                identity.input_fields().to_vec(),
                identity.blob_type(),
                bytes::Bytes::copy_from_slice(bodies.value(row)),
                BTreeMap::new(),
            )
            .map_err(|error| error.to_string())?;
            self.observed.insert(identity, draft);
        }
        Ok(())
    }

    pub fn observe_root_eof(&mut self) -> Result<(), String> {
        if std::mem::replace(&mut self.root_eof, true) {
            return Err("statistics Root emitted duplicate EOF".into());
        }
        Ok(())
    }

    pub fn observe_execution_success(&mut self) -> Result<(), String> {
        if std::mem::replace(&mut self.execution_succeeded, true) {
            return Err("statistics execution success was observed twice".into());
        }
        Ok(())
    }

    pub fn finish(self) -> Result<Vec<StatisticsArtifactDraft>, String> {
        if !self.root_eof {
            return Err("statistics Root EOF was not observed".into());
        }
        if !self.execution_succeeded {
            return Err("statistics execution did not reach all-success".into());
        }
        let observed = self.observed.keys().cloned().collect::<BTreeSet<_>>();
        if observed != self.expected {
            let missing = self.expected.difference(&observed).collect::<Vec<_>>();
            return Err(format!(
                "statistics Root artifact membership is incomplete; missing {missing:?}"
            ));
        }
        Ok(self.observed.into_values().collect())
    }
}

fn contract_violation(message: impl Into<String>) -> DistributedQueryError {
    DistributedQueryError::new(DistributedQueryErrorKind::ContractViolation, message)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, BinaryArray, Int32Builder, ListBuilder, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use novarocks_execution::exec::chunk::Chunk;
    use novarocks_spi::connector::StatisticsArtifactIdentity;
    use novarocks_types::SlotId;

    use super::{StatisticsRootResultDecoder, charge_statistics_body_bytes};

    fn identity(field_id: i32, blob_type: &str) -> StatisticsArtifactIdentity {
        StatisticsArtifactIdentity::try_new(vec![field_id], Arc::<str>::from(blob_type))
            .expect("identity")
    }

    fn chunk(rows: &[(&[i32], &str, &[u8], &[(&str, &str)])]) -> Chunk {
        let schema = Arc::new(Schema::new(vec![
            Field::new("input_fields", super::artifact_input_fields_type(), false),
            Field::new("blob_type", DataType::Utf8, false),
            Field::new("body", DataType::Binary, true),
            Field::new("properties", super::artifact_properties_type(), false),
        ]));
        let mut fields = ListBuilder::new(Int32Builder::new()).with_field(Arc::new(Field::new(
            "item",
            DataType::Int32,
            false,
        )));
        for (field_ids, _, _, _) in rows {
            for field_id in *field_ids {
                fields.values().append_value(*field_id);
            }
            fields.append(true);
        }
        let fields = Arc::new(fields.finish()) as ArrayRef;
        let types = Arc::new(StringArray::from_iter_values(
            rows.iter().map(|(_, blob_type, _, _)| *blob_type),
        )) as ArrayRef;
        let bodies = Arc::new(BinaryArray::from_iter_values(
            rows.iter().map(|(_, _, body, _)| *body),
        )) as ArrayRef;
        let mut properties = arrow::array::MapBuilder::new(
            Some(arrow::array::MapFieldNames {
                entry: "entries".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            arrow::array::StringBuilder::new(),
            arrow::array::StringBuilder::new(),
        )
        .with_keys_field(Arc::new(Field::new("key", DataType::Utf8, false)))
        .with_values_field(Arc::new(Field::new("value", DataType::Utf8, false)));
        for (_, _, _, row_properties) in rows {
            for (key, value) in *row_properties {
                properties.keys().append_value(*key);
                properties.values().append_value(*value);
            }
            properties.append(true).expect("empty properties row");
        }
        let properties = Arc::new(properties.finish()) as ArrayRef;
        let slot_ids = [
            SlotId::new(1),
            SlotId::new(2),
            SlotId::new(3),
            SlotId::new(4),
        ];
        let chunk_schema =
            novarocks_execution::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                schema.as_ref(),
                &slot_ids,
            )
            .expect("chunk schema");
        Chunk::try_new_with_chunk_schema(
            RecordBatch::try_new(schema, vec![fields, types, bodies, properties]).expect("batch"),
            chunk_schema,
        )
        .expect("chunk")
    }

    #[test]
    fn statistics_root_decoder_requires_eof_success_and_exact_membership() {
        let mut decoder = StatisticsRootResultDecoder::new([
            identity(1, "apache-datasketches-theta-v1"),
            identity(2, "apache-datasketches-theta-v1"),
        ]);
        decoder
            .apply_chunk(&chunk(&[(
                &[1],
                "apache-datasketches-theta-v1",
                b"one",
                &[],
            )]))
            .expect("first batch");
        decoder
            .apply_chunk(&chunk(&[(
                &[2],
                "apache-datasketches-theta-v1",
                b"two",
                &[],
            )]))
            .expect("second batch");
        assert!(decoder.finish().unwrap_err().contains("EOF"));

        let mut decoder = StatisticsRootResultDecoder::new([
            identity(1, "apache-datasketches-theta-v1"),
            identity(2, "apache-datasketches-theta-v1"),
        ]);
        decoder
            .apply_chunk(&chunk(&[
                (&[1], "apache-datasketches-theta-v1", b"one", &[]),
                (&[2], "apache-datasketches-theta-v1", b"two", &[]),
            ]))
            .expect("batch");
        decoder.observe_root_eof().expect("EOF");
        assert!(decoder.finish().unwrap_err().contains("all-success"));

        let mut decoder = StatisticsRootResultDecoder::new([
            identity(1, "apache-datasketches-theta-v1"),
            identity(2, "apache-datasketches-theta-v1"),
        ]);
        decoder
            .apply_chunk(&chunk(&[
                (&[1], "apache-datasketches-theta-v1", b"one", &[]),
                (&[2], "apache-datasketches-theta-v1", b"two", &[]),
            ]))
            .expect("batch");
        decoder.observe_root_eof().expect("EOF");
        decoder.observe_execution_success().expect("all-success");
        assert_eq!(decoder.finish().expect("complete").len(), 2);
    }

    #[test]
    fn statistics_root_decoder_rejects_unknown_duplicate_missing_and_trailing() {
        let expected = identity(1, "apache-datasketches-theta-v1");

        let mut unknown = StatisticsRootResultDecoder::new([expected.clone()]);
        assert!(
            unknown
                .apply_chunk(&chunk(&[(
                    &[2],
                    "apache-datasketches-theta-v1",
                    b"body",
                    &[]
                )]))
                .unwrap_err()
                .contains("unexpected")
        );

        let mut duplicate = StatisticsRootResultDecoder::new([expected.clone()]);
        duplicate
            .apply_chunk(&chunk(&[(
                &[1],
                "apache-datasketches-theta-v1",
                b"body",
                &[],
            )]))
            .expect("first");
        assert!(
            duplicate
                .apply_chunk(&chunk(&[(
                    &[1],
                    "apache-datasketches-theta-v1",
                    b"body",
                    &[]
                )]))
                .unwrap_err()
                .contains("duplicate")
        );

        let mut missing = StatisticsRootResultDecoder::new([expected.clone()]);
        missing.observe_root_eof().expect("EOF");
        missing.observe_execution_success().expect("all-success");
        assert!(missing.finish().unwrap_err().contains("incomplete"));

        let mut trailing = StatisticsRootResultDecoder::new([expected]);
        trailing.observe_root_eof().expect("EOF");
        assert!(
            trailing
                .apply_chunk(&chunk(&[]))
                .unwrap_err()
                .contains("trailing")
        );
    }

    #[test]
    fn statistics_root_decoder_preserves_composite_identity_and_rejects_properties() {
        let composite = StatisticsArtifactIdentity::try_new(
            vec![7, 9],
            Arc::<str>::from("future-composite-v1"),
        )
        .expect("composite identity");
        let mut decoder = StatisticsRootResultDecoder::new([composite]);
        decoder
            .apply_chunk(&chunk(&[(&[7, 9], "future-composite-v1", b"body", &[])]))
            .expect("composite artifact");
        decoder.observe_root_eof().expect("EOF");
        decoder.observe_execution_success().expect("all-success");
        let drafts = decoder.finish().expect("complete");
        assert_eq!(drafts[0].identity().input_fields(), &[7, 9]);

        let mut decoder =
            StatisticsRootResultDecoder::new([identity(1, "apache-datasketches-theta-v1")]);
        assert!(
            decoder
                .apply_chunk(&chunk(&[(
                    &[1],
                    "apache-datasketches-theta-v1",
                    b"body",
                    &[("ndv", "1")],
                )]))
                .unwrap_err()
                .contains("properties must be empty")
        );
    }

    #[test]
    fn statistics_root_body_budget_admits_every_maximal_theta_requirement() {
        const MAX_COMPACT_THETA_BYTES: usize = 65_560;
        let mut charged = 0;
        for _ in 0..novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_COLUMNS {
            charged = charge_statistics_body_bytes(charged, MAX_COMPACT_THETA_BYTES)
                .expect("all maximal Theta bodies fit the result budget");
        }
        assert_eq!(
            charged,
            novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_COLUMNS * MAX_COMPACT_THETA_BYTES
        );
    }
}
