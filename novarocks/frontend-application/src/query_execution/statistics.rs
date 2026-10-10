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

use novarocks_native_adapter::root_record_assembly::{RootRecordAssembly, RootRecordDomain};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, BinaryArray, Int32Array, ListArray, MapArray, StringArray};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorReadSelector, ConnectorRequestContext,
    StatisticsArtifactDraft, StatisticsArtifactIdentity, StatisticsDataVersion,
    StatisticsRequiredAggregation,
};

use crate::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, DistributedQueryRequest,
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
    /// A process-owned statistics job has reached its collection query
    /// attempt. This is intentionally not the business job identity and not
    /// the provider publication identity.
    BackgroundCollectionAttempt,
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
        let mut inputs =
            BTreeMap::<usize, (&str, &novarocks_type_contract::FunctionValueType)>::new();
        for requirement in &required {
            let input = requirement.input();
            if let Some((name, value_type)) =
                inputs.insert(input.ordinal(), (input.name(), input.value_type()))
                && (name != input.name() || value_type != input.value_type())
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

    pub(crate) fn result_decoder_with_capacity(
        &self,
        binding: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    ) -> Result<StatisticsRootResultDecoder, String> {
        let retention =
            crate::query_execution::internal_result_cpu::InternalResultRetention::try_new(binding)?;
        let mut decoder = self.result_decoder();
        decoder.retention = Some(retention);
        Ok(decoder)
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

/// Frontend authorities one statistics collection is planned with.
///
/// There are only two, because a completed plan negotiates its own read
/// through the provider-read protocol: the control that read is frozen
/// through, and the catalog the collection's aggregates are bound in.
pub struct CompletedStatisticsPlanningServices<'a> {
    constant_policy: novarocks_functions::ConstantPolicy,
    typed_connector_control: &'a Arc<novarocks_catalog_application::ConnectorControlHost>,
    functions: &'a novarocks_functions::EngineFunctionCatalog,
    static_plan_carrier: &'a crate::query_execution::package_freeze::StaticPlanCarrier,
}

impl<'a> CompletedStatisticsPlanningServices<'a> {
    pub const fn new(
        typed_connector_control: &'a Arc<novarocks_catalog_application::ConnectorControlHost>,
        functions: &'a novarocks_functions::EngineFunctionCatalog,
        constant_policy: novarocks_functions::ConstantPolicy,
        static_plan_carrier: &'a crate::query_execution::package_freeze::StaticPlanCarrier,
    ) -> Self {
        Self {
            constant_policy,
            typed_connector_control,
            functions,
            static_plan_carrier,
        }
    }
}

/// Plan one statistics collection as a completed physical plan.
///
/// A collection is not a statement: the provider says what must be measured,
/// so there is no text to parse, no name to resolve and no shape to choose.
/// What it *is* is an ordinary provider read followed by ordinary aggregation,
/// and both halves go through exactly the contracts a statement's go through
/// -- the read is negotiated and frozen through the provider-read protocol,
/// and what that produces is the same validated `PhysicalPlan`, put on the
/// wire by the same encoder.
///
/// The read is frozen before the plan is built because the plan addresses its
/// scan by the occurrence the freeze accounted for. Building the plan first
/// would mean naming an occurrence nothing had been frozen for yet.
pub fn prepare_completed_statistics_collection(
    services: CompletedStatisticsPlanningServices<'_>,
    execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
    context: ConnectorRequestContext,
    identity: &StatisticsRelationIdentity,
    program: StatisticsCollectionProgram,
    planning_lease: ConnectorControlPlanningLease,
) -> Result<DistributedQueryRequest, DistributedQueryError> {
    use novarocks_query_application::preparation::{
        CompletedPhysicalPlanCandidate, CompletedPlanWithAccess, ReadAccessSink,
    };

    let CompletedStatisticsPlanningServices {
        typed_connector_control,
        functions,
        constant_policy,
        static_plan_carrier,
    } = services;
    let live = execution.topology().targets().len();
    if live == 0 {
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
    let bindings = Arc::new(
        crate::catalog_application::query_bindings::QueryTableBindingStore::try_new()
            .map_err(contract_violation)?,
    );
    let binding =
        admit_statistics_scan_binding(bindings.as_ref(), identity, &program, planning_lease)?;
    let scan = novarocks_sql::planning::dml::StatisticsConnectorScan {
        binding,
        catalog: identity.catalog.clone(),
        namespace: identity.namespace.clone(),
        table: identity.table.clone(),
        version_ordinal: program.read_version_ordinal(),
        columns: program.scan_columns(),
    };

    // Freeze the read. The capability this leaves is deposited as it is taken,
    // so the plan and what performs its read are accounted for together.
    let need = novarocks_sql::planning::dml::statistics_provider_read_need(&scan)
        .map_err(contract_violation)?;
    let session =
        crate::query_execution::compiler::typed_connector_session().map_err(contract_violation)?;
    let sink = ReadAccessSink::new();
    let fact = crate::query_execution::provider_read_facts::freeze_one_read(
        &need,
        typed_connector_control.as_ref(),
        bindings.as_ref(),
        &session,
        &context,
        &sink.deposits(),
    )
    .map_err(contract_violation)?;
    let access = sink
        .try_into_access()
        .map_err(|(error, _returned)| contract_violation(error.to_string()))?;

    let completion_control =
        crate::query_execution::planning::sql_compile_control_from_execution(execution);
    let plan = novarocks_sql::planning::dml::build_final_statistics_connector_plan(
        scan,
        program.required_aggregations(),
        functions,
        execution.optimizer_settings(),
        novarocks_sql::planning::dml::DmlFinalPlanContext::new(
            crate::query_execution::physical_encoding::mint_plan_version(),
            crate::query_execution::contract::completed_plan_dop_domain(None)
                .map_err(contract_violation)?,
            novarocks_sql::planning::dml::DmlFinalizedProviderReadSet::try_new([
                novarocks_sql::planning::dml::DmlFinalizedProviderRead {
                    fact,
                    read_budget: statistics_scan_read_budget(),
                },
            ])
            .map_err(contract_violation)?,
            static_plan_carrier.sql_emission_mode(),
        ),
        execution
            .sql_semantics()
            .sql_mode()
            .decimal_overflow_policy(),
        execution.sql_semantics().sql_mode().allow_throw_exception(),
        constant_policy,
        &completion_control,
    )
    .map_err(DistributedQueryError::from_compile)?;
    let version = plan.plan().version();
    let candidate = CompletedPhysicalPlanCandidate::for_sql_program(plan, &completion_control)
        .and_then(|candidate| {
            candidate.freeze_root_output(
                novarocks_result_contract::FrozenRootOutput::InternalFacts(
                    novarocks_result_contract::InternalResultDomain::StatisticsArtifactV1,
                ),
            )
        })
        .map_err(|error| contract_violation(error.to_string()))?;
    let output =
        novarocks_query_application::preparation::OutputContract::from_completed_candidate(
            novarocks_query_application::api::QueryExecutionKind::Statistics,
            &candidate,
        )
        .map_err(contract_violation)?;
    let paired = CompletedPlanWithAccess::try_pair(candidate, access)
        .map_err(|(error, _returned)| contract_violation(error.to_string()))?;
    let encoded = crate::query_execution::physical_encoding::encode_completed_plan(
        paired,
        functions,
        static_plan_carrier,
        constant_policy,
        None,
        execution.sql_semantics().sql_mode().allow_throw_exception(),
        &completion_control,
    )
    .map_err(DistributedQueryError::from_encode)?;
    let (template, candidate) = encoded.into_attempt_template_with_candidate(version);
    let description =
        novarocks_query_application::preparation::FrozenExecutionDescription::for_completed_plan(
            novarocks_query_application::api::QueryExecutionKind::Statistics,
            candidate,
            template
                .attempt_scheduling_facts()
                .map_err(contract_violation)?
                .fragments
                .iter()
                .flat_map(|fragment| fragment.scans.iter().map(|scan| scan.scan))
                .collect(),
            output,
            novarocks_query_application::coordination::ExecutionEffect::None,
            novarocks_query_application::coordination::RecoveryMode::NoRecovery,
            Vec::new(),
            novarocks_query_application::preparation::FrozenCostEstimate::unknown(
                novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected,
            ),
            novarocks_query_application::preparation::ExecutionResourceRequirements::unknown(
                novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected,
            ),
        )
        .map_err(contract_violation)?;
    let options = crate::query_execution::contract::synthetic_statement_query_options(execution);
    crate::query_execution::contract::build_request_from_finalized_execution(
        crate::query_execution::post_compile::FinalizedDistributedExecution::for_completed_plan(
            description,
            template,
        ),
        Some(options),
        crate::query_execution::contract::DistributedQueryIntent::Statistics,
        execution,
        Some(program),
    )
}

/// How much one collection's scan may return in a batch.
///
/// A collection has no session to lower this, so it states the contract's own
/// maximum, which means "unconstrained" rather than a number someone chose.
const fn statistics_scan_read_budget() -> novarocks_physical_plan::ScanReadBudget {
    novarocks_physical_plan::ScanReadBudget {
        max_batch_rows: novarocks_physical_plan::MAX_SCAN_BATCH_ROWS,
        max_batch_bytes: novarocks_physical_plan::MAX_SCAN_BATCH_BYTES,
    }
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
            .map(|column| {
                column
                    .value_type()
                    .try_to_field(column.name())
                    .map_err(|error| contract_violation(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?,
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
                source_metadata: None,
                // A collection measures one snapshot, so the binding offers
                // exactly that one. Offering it as the current read as well
                // would let a freeze resolve to whatever is current by then,
                // which is a different table from the one the evidence will
                // be stamped with.
                scan_materialization: None,
                mv_target_read: None,
                write_target_admission: None,
                frozen_cohort_read: None,
                frozen_snapshot_materializations: BTreeMap::from([(
                    version_ordinal,
                    crate::catalog_application::query_bindings::QueryScanMaterialization {
                        table: program.table().clone(),
                        catalog_handle: planning_lease
                            .binding()
                            .catalog_handle()
                            .map_err(|error| error.to_string())?
                            .clone(),
                        schema: input_schema.clone(),
                        selector: ConnectorReadSelector::SnapshotId(version_ordinal),
                        mv_partition_selection: None,
                        statistics_pin: None,
                        planning_lease: planning_lease.clone(),
                    },
                )]),
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
    relay_assembly: RootRecordAssembly,
    relayed_record_count: u64,
    expected: BTreeSet<StatisticsArtifactIdentity>,
    observed: BTreeMap<StatisticsArtifactIdentity, StatisticsArtifactDraft>,
    body_bytes: usize,
    root_eof: bool,
    execution_succeeded: bool,
    retention: Option<crate::query_execution::internal_result_cpu::InternalResultRetention>,
}

impl StatisticsRootResultDecoder {
    #[cfg(test)]
    pub(crate) fn for_test_with_capacity(
        expected: impl IntoIterator<Item = StatisticsArtifactIdentity>,
        binding: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    ) -> Self {
        let retention =
            crate::query_execution::internal_result_cpu::InternalResultRetention::try_new(binding)
                .unwrap();
        let mut decoder = Self::new(expected);
        decoder.retention = Some(retention);
        decoder
    }
    fn new(expected: impl IntoIterator<Item = StatisticsArtifactIdentity>) -> Self {
        Self {
            relayed_record_count: 0,
            relay_assembly: RootRecordAssembly::new(RootRecordDomain::Statistics, 32 * 1024 * 1024),
            expected: expected.into_iter().collect(),
            observed: BTreeMap::new(),
            body_bytes: 0,
            root_eof: false,
            execution_succeeded: false,
            retention: None,
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
        // The Root relation is these four columns in this order. Only the
        // body's nullability is left to the plan: it is whatever the
        // aggregates the provider asked for return, and an aggregate that
        // cannot return null produces a column that says so. A null body is
        // refused per row below either way, so the plan is free to state the
        // stronger fact without the decoder having to predict it.
        let expected_schema = Schema::new(vec![
            Field::new("input_fields", artifact_input_fields_type(), false),
            Field::new("blob_type", DataType::Utf8, false),
            Field::new(
                "body",
                DataType::Binary,
                schema
                    .fields()
                    .get(2)
                    .is_some_and(|field| field.is_nullable()),
            ),
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
            let property_offsets = properties.value_offsets();
            let property_count = usize::try_from(property_offsets[row + 1] - property_offsets[row])
                .map_err(|_| "statistics Root properties offset is invalid".to_string())?;
            if property_count != 0 {
                return Err("ANALYZE statistics Root properties must be empty".into());
            }
            self.apply_artifact(
                fields.values().to_vec(),
                blob_types.value(row),
                bodies.value(row),
            )?;
        }
        Ok(())
    }

    /// Apply one complete StatisticsArtifactV1 record relayed from the
    /// Backend, under the same identity, membership and body rules.
    /// Validate complete local domain consumption before the coordinator
    /// records End. This count is relation rows, not affected write rows.
    pub(crate) fn check_relay_end(&self, output_rows: u64) -> Result<(), String> {
        self.relay_assembly.finish()?;
        if output_rows != self.relayed_record_count {
            return Err("internal root End differs from decoded record count".into());
        }
        Ok(())
    }

    /// Accept a relayed body into the prepaid 32 MiB assembly share. A complete
    /// record is validated before this call returns and its receipt may finish.
    pub(crate) fn apply_relay_body(&mut self, body: &[u8]) -> Result<(), String> {
        if self.root_eof {
            return Err("statistics Root emitted a trailing body after EOF".into());
        }
        let mut assembly = std::mem::replace(
            &mut self.relay_assembly,
            RootRecordAssembly::new(RootRecordDomain::Statistics, 32 * 1024 * 1024),
        );
        let result = assembly.push(body, |record| {
            self.apply_record(record)?;
            self.relayed_record_count = self
                .relayed_record_count
                .checked_add(1)
                .ok_or("internal root record count overflow")?;
            Ok(())
        });
        self.relay_assembly = assembly;
        result
    }

    pub fn apply_record(&mut self, record: &[u8]) -> Result<(), String> {
        if self.root_eof {
            return Err("statistics Root emitted a trailing record after EOF".into());
        }
        if self.observed.len() >= MAX_STATISTICS_ROOT_ROWS {
            return Err("statistics Root row budget exceeded".into());
        }
        let view =
            novarocks_native_adapter::root_statistics_codec::StatisticsArtifactRecordView::parse(
                record,
            )
            .map_err(|error| format!("statistics Root record: {error}"))?;
        self.apply_artifact(view.field_ids().collect(), view.blob_type(), view.body())
    }

    fn apply_artifact(
        &mut self,
        field_ids: Vec<i32>,
        blob_type: &str,
        body: &[u8],
    ) -> Result<(), String> {
        if field_ids.iter().collect::<BTreeSet<_>>().len() != field_ids.len() {
            return Err("statistics Root input_fields contains a duplicate field ID".into());
        }
        let identity = StatisticsArtifactIdentity::try_new(field_ids, blob_type)
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
        self.body_bytes = charge_statistics_body_bytes(self.body_bytes, body.len())?;
        let draft = StatisticsArtifactDraft::try_new(
            identity.input_fields().to_vec(),
            identity.blob_type(),
            bytes::Bytes::copy_from_slice(body),
            BTreeMap::new(),
        )
        .map_err(|error| error.to_string())?;
        let draft = match &self.retention {
            Some(retention) => draft.attach_guard(retention.spi_guard()),
            None => draft,
        };
        self.observed.insert(identity, draft);
        Ok(())
    }

    pub fn observe_root_eof(&mut self) -> Result<(), String> {
        self.relay_assembly.finish()?;
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

    fn collection_requirement(
        value_type: novarocks_type_contract::FunctionValueType,
        field_id: i32,
    ) -> novarocks_spi::connector::StatisticsRequiredAggregation {
        novarocks_spi::connector::StatisticsRequiredAggregation::try_new(
            novarocks_spi::connector::StatisticsScanColumn::try_new(0, "v", value_type)
                .expect("checked scan column"),
            "$test_stat",
            identity(field_id, "test/blob"),
        )
        .expect("checked statistics requirement")
    }

    fn collection_program(
        required: Vec<novarocks_spi::connector::StatisticsRequiredAggregation>,
    ) -> Result<super::StatisticsCollectionProgram, super::DistributedQueryError> {
        use bytes::Bytes;
        use novarocks_spi::connector::{
            ConnectorInstanceId, ConnectorTableHandle, StatisticsDataVersion,
        };

        super::StatisticsCollectionProgram::try_new(
            ConnectorTableHandle::try_new(
                ConnectorInstanceId::parse("statistics_value_type_test").expect("instance id"),
                Bytes::from_static(b"table"),
            )
            .expect("checked table handle"),
            StatisticsDataVersion::try_new(Bytes::from_static(b"data-v1"))
                .expect("checked data version"),
            Some(7),
            required,
            super::StatisticsExecutionPolicy::try_new(
                super::StatisticsExecutionMode::SynchronousWait,
                std::time::Duration::from_secs(30),
            )
            .expect("bounded attempt policy"),
        )
    }

    #[test]
    fn statistics_program_keeps_exact_tagged_scan_columns() {
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};

        for (carrier, logical) in [
            (DataType::FixedSizeBinary(16), ValueLogicalType::Uuid),
            (DataType::LargeBinary, ValueLogicalType::Variant),
        ] {
            let authored = FunctionValueType::try_with_logical_type(carrier, true, logical)
                .expect("authored source type");
            let field = authored.try_to_field("v").expect("tagged source field");
            let source = FunctionValueType::try_from_field(&field).expect("exact field projection");
            assert_eq!(source, authored);
            let program = collection_program(vec![
                collection_requirement(source.clone(), 1),
                collection_requirement(source.clone(), 2),
            ])
            .expect("same ordinal and exact type can serve distinct artifacts");
            assert_eq!(program.required_aggregations().len(), 2);
            assert_eq!(program.read_version_ordinal(), 7);
            let columns = program.scan_columns();
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0].ordinal(), 0);
            assert_eq!(columns[0].value_type(), &source);
            assert_eq!(
                columns[0]
                    .value_type()
                    .try_to_field("v")
                    .expect("scan schema"),
                field
            );
        }
    }

    #[test]
    fn statistics_program_rejects_same_ordinal_with_a_different_root_domain() {
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};

        for (carrier, logical) in [
            (DataType::FixedSizeBinary(16), ValueLogicalType::Uuid),
            (DataType::LargeBinary, ValueLogicalType::Variant),
        ] {
            let semantic = FunctionValueType::try_with_logical_type(carrier.clone(), true, logical)
                .expect("authored semantic source");
            let physical = FunctionValueType::new(carrier, true);
            for (first, second) in [(semantic.clone(), physical.clone()), (physical, semantic)] {
                let error = collection_program(vec![
                    collection_requirement(first, 1),
                    collection_requirement(second, 2),
                ])
                .err()
                .expect("same carrier and nullability cannot hide a root conflict");
                assert_eq!(
                    error.kind(),
                    super::DistributedQueryErrorKind::ContractViolation
                );
                assert_eq!(
                    error.message(),
                    "statistics requirements disagree on a scan column ordinal"
                );
            }
        }
    }

    #[test]
    fn statistics_scan_field_admission_rejects_unknown_and_wrong_tags() {
        use novarocks_type_contract::{
            FunctionValueType, NR_LOGICAL_TYPE_KEY, ValueLogicalType, ValueTypeError,
        };

        for (carrier, tag, expected) in [
            (
                DataType::FixedSizeBinary(16),
                "unknown",
                ValueTypeError::UnknownLogicalMetadata,
            ),
            (
                DataType::LargeBinary,
                "uuid",
                ValueTypeError::InvalidLogicalCarrier(ValueLogicalType::Uuid),
            ),
            (
                DataType::FixedSizeBinary(16),
                "variant",
                ValueTypeError::InvalidLogicalCarrier(ValueLogicalType::Variant),
            ),
        ] {
            let field = Field::new("v", carrier, true)
                .with_metadata([(NR_LOGICAL_TYPE_KEY.to_owned(), tag.to_owned())].into());
            assert_eq!(FunctionValueType::try_from_field(&field), Err(expected));
        }
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

    /// The Backend statistics encoder's records for `chunk`, cut into tiny
    /// bodies and reassembled.
    fn statistics_records(chunk: &Chunk) -> Vec<Vec<u8>> {
        use novarocks_native_adapter::root_record_assembly::{
            RootRecordAssembly, RootRecordDomain,
        };
        use novarocks_native_adapter::root_statistics_codec::{
            StatisticsArtifactEncoder, StatisticsCodecStatus, StatisticsCodecTotals,
        };
        let mut encoder = StatisticsArtifactEncoder::try_new(
            chunk.batch.clone(),
            StatisticsCodecTotals::default(),
        )
        .expect("statistics relation");
        let mut stream = Vec::new();
        let mut output = vec![0_u8; 9];
        loop {
            let turn = encoder.step(&mut output).expect("encodable artifacts");
            stream.extend_from_slice(&output[..turn.emitted_bytes]);
            if turn.status == StatisticsCodecStatus::InputComplete {
                break;
            }
        }
        let mut assembly = RootRecordAssembly::new(RootRecordDomain::Statistics, 1 << 20);
        let mut records = Vec::new();
        for body in stream.chunks(4) {
            assembly
                .push(body, |record| {
                    records.push(record.to_vec());
                    Ok(())
                })
                .unwrap();
        }
        assembly.finish().unwrap();
        records
    }

    #[test]
    fn last_artifact_body_clone_retains_window_after_decoder_and_artifacts_exit() {
        let (_control, root, binding, capacity) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let mut decoder = StatisticsRootResultDecoder::new([identity(1, "theta-v1")]);
        decoder.retention = Some(
            crate::query_execution::internal_result_cpu::InternalResultRetention::try_new(&binding)
                .unwrap(),
        );
        for record in statistics_records(&chunk(&[(&[1], "theta-v1", b"one", &[])])) {
            decoder.apply_relay_body(&record).unwrap();
        }
        decoder.observe_root_eof().unwrap();
        decoder.observe_execution_success().unwrap();
        let artifacts = decoder.finish().unwrap();
        let body = artifacts[0].body().clone();
        drop(binding);
        root.owner.complete();
        root.business.release();
        drop(artifacts);
        assert_eq!(body.as_ref(), b"one");
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        drop(body);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[test]
    fn relayed_body_end_refuses_partial_record_and_preserves_all_success_gate() {
        let record = statistics_records(&chunk(&[(&[1], "theta-v1", b"one", &[])])).remove(0);
        let expected = StatisticsArtifactIdentity::try_new(vec![1], "theta-v1").unwrap();
        let mut decoder = StatisticsRootResultDecoder::new([expected.clone()]);
        decoder.apply_relay_body(&record[..7]).unwrap();
        assert!(
            decoder
                .observe_root_eof()
                .unwrap_err()
                .contains("unfinished")
        );
        assert!(!decoder.root_eof);
        decoder.apply_relay_body(&record[7..]).unwrap();
        assert!(decoder.check_relay_end(2).is_err());
        assert!(!decoder.root_eof);
        decoder.check_relay_end(1).unwrap();
        decoder.observe_root_eof().unwrap();
        assert!(decoder.finish().unwrap_err().contains("all-success"));
        let mut decoder = StatisticsRootResultDecoder::new([expected]);
        decoder.apply_relay_body(&record).unwrap();
        decoder.observe_root_eof().unwrap();
        decoder.observe_execution_success().unwrap();
        assert_eq!(decoder.finish().unwrap().len(), 1);
    }

    #[test]
    fn relayed_statistics_records_match_the_arrow_relation() {
        let rows = chunk(&[
            (&[1], "apache-datasketches-theta-v1", b"one", &[]),
            (&[2, 3], "apache-datasketches-theta-v1", b"two", &[]),
        ]);
        let expected = || {
            StatisticsRootResultDecoder::new([
                StatisticsArtifactIdentity::try_new(vec![1], "apache-datasketches-theta-v1")
                    .unwrap(),
                StatisticsArtifactIdentity::try_new(vec![2, 3], "apache-datasketches-theta-v1")
                    .unwrap(),
            ])
        };
        let mut from_chunk = expected();
        from_chunk.apply_chunk(&rows).unwrap();
        from_chunk.observe_root_eof().unwrap();
        from_chunk.observe_execution_success().unwrap();
        let mut from_records = expected();
        for record in statistics_records(&rows) {
            for body in record.chunks(3) {
                from_records.apply_relay_body(body).unwrap();
            }
        }
        from_records.observe_root_eof().unwrap();
        from_records.observe_execution_success().unwrap();
        assert_eq!(from_records.finish().unwrap(), from_chunk.finish().unwrap());
        // Duplicates and unexpected identities are refused on the record path too.
        let mut duplicate = expected();
        let records = statistics_records(&rows);
        duplicate.apply_record(&records[0]).unwrap();
        assert!(duplicate.apply_record(&records[0]).is_err());
        let mut unexpected = StatisticsRootResultDecoder::new([identity(9, "x")]);
        assert!(unexpected.apply_record(&records[0]).is_err());
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
