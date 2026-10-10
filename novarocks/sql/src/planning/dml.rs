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

//! SQL-owned DML syntax and planning entrypoints.
//!
//! Application code may retain provider leases, write fences, and execution
//! lifecycle state, but it must not reach into parser or optimizer internals.
//! This submodule is the deliberately narrow handoff for those DML-specific
//! facts.  Its module hook is installed by the SQL facade integration wave.

pub use crate::analyzer::iceberg_ref::{IcebergRefSuffix, split_ref_suffix};
pub use crate::planner::distributed::write::ConnectorWriteInputBinding;

const ICEBERG_FILE_PATH_COLUMN: &str = "_file";
const ICEBERG_ROW_POSITION_COLUMN: &str = "_pos";
const ICEBERG_ROW_ID_COLUMN: &str = "_row_id";
const ICEBERG_LAST_UPDATED_SEQUENCE_COLUMN: &str = "_last_updated_sequence_number";

/// One application-admitted, immutable statistics observation. It contains no
/// resolver, table handle, lease, or callback, so SQL cannot retry against a
/// newer connector generation while compiling the paired request.
#[derive(Clone, Debug)]
pub enum DmlStatisticsEvidence {
    Available {
        binding: crate::binding::SqlTableBindingId,
        label: String,
        columns: Vec<novarocks_types::schema::ColumnDef>,
        evidence: novarocks_spi::connector::StatisticsEvidence,
    },
    Missing {
        binding: crate::binding::SqlTableBindingId,
        label: String,
        reason: String,
    },
    Fatal {
        binding: crate::binding::SqlTableBindingId,
        label: String,
        failure: DmlStatisticsFailure,
    },
}

/// An admission-time contradiction between connector evidence and the frozen
/// table binding.  These failures are carried into SQL unchanged; SQL never
/// retries a catalog or statistics lookup to replace them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DmlStatisticsFailure {
    BindingMissing,
    OwnerMismatch,
    IncarnationMismatch,
    DataVersionMismatch,
    CorruptEvidence(String),
}

/// SQL-owned opaque statistics carrier for one compile request.
#[derive(Clone, Debug)]
pub struct DmlStatisticsSnapshot(pub(crate) crate::optimizer::stats_input::SqlStatisticsSnapshot);

impl Default for DmlStatisticsSnapshot {
    fn default() -> Self {
        Self::empty()
    }
}

impl DmlStatisticsSnapshot {
    /// Construct an intentionally empty snapshot for an already SQL-owned
    /// logical input. Empty means evidence is unavailable, never that a table
    /// has zero rows.
    pub fn empty() -> Self {
        Self(crate::optimizer::stats_input::SqlStatisticsSnapshot::empty())
    }

    /// Seal admission-frozen connector evidence into the compiler's private
    /// statistics representation. The public input is immutable data only;
    /// provider handles, leases, and resolver callbacks cannot cross this
    /// boundary.
    pub fn from_evidence(entries: impl IntoIterator<Item = DmlStatisticsEvidence>) -> Self {
        use crate::optimizer::stats_input::{
            BaseTableStatistics, SqlTableStatisticsEvidence, StatsMissingReason,
        };

        let mut snapshot = crate::optimizer::stats_input::SqlStatisticsSnapshot::empty();
        for entry in entries {
            match entry {
                DmlStatisticsEvidence::Available {
                    binding,
                    label,
                    columns,
                    evidence,
                } => snapshot.insert(
                    binding,
                    SqlTableStatisticsEvidence {
                        label,
                        statistics: evidence_to_base_statistics(&evidence, &columns),
                    },
                ),
                DmlStatisticsEvidence::Missing {
                    binding,
                    label,
                    reason,
                } => snapshot.insert(
                    binding,
                    SqlTableStatisticsEvidence {
                        label,
                        statistics: BaseTableStatistics::missing(
                            StatsMissingReason::CatalogLoadError(reason),
                        ),
                    },
                ),
                DmlStatisticsEvidence::Fatal {
                    binding, failure, ..
                } => snapshot.insert_fatal(binding, match_failure(failure)),
            }
        }
        Self(snapshot)
    }
}

fn match_failure(
    failure: DmlStatisticsFailure,
) -> crate::optimizer::stats_input::SqlStatisticsFatalError {
    use crate::optimizer::stats_input::SqlStatisticsFatalError;

    match failure {
        DmlStatisticsFailure::BindingMissing => SqlStatisticsFatalError::BindingMissing,
        DmlStatisticsFailure::OwnerMismatch => SqlStatisticsFatalError::OwnerMismatch,
        DmlStatisticsFailure::IncarnationMismatch => SqlStatisticsFatalError::IncarnationMismatch,
        DmlStatisticsFailure::DataVersionMismatch => SqlStatisticsFatalError::DataVersionMismatch,
        DmlStatisticsFailure::CorruptEvidence(message) => {
            SqlStatisticsFatalError::CorruptEvidence(message)
        }
    }
}

/// Maps one connector answer into optimizer input, metric by metric.
///
/// Nothing here can discard the whole answer. Each metric is admitted only if
/// it describes the queried version's rows, and its numeric nature becomes a
/// confidence rather than a veto: a value that bounds the truth, or estimates
/// it in both directions, is still better input than the missing-stats
/// fallback. What it must never do is claim to be exact.
fn evidence_to_base_statistics(
    evidence: &novarocks_spi::connector::StatisticsEvidence,
    columns: &[novarocks_types::schema::ColumnDef],
) -> crate::optimizer::stats_input::BaseTableStatistics {
    use crate::optimizer::statistics::Confidence;
    use crate::optimizer::stats_input::{
        BaseColumnStatistics, BaseTableStatistics, StatValue, StatsMissingReason, StatsSource,
    };
    use novarocks_spi::connector::{
        StatisticsMetric, StatisticsMetricObservation, StatisticsMetricSource,
        StatisticsMetricState, StatisticsMetricValue, StatisticsNumericNature,
    };

    fn metric_source(source: &StatisticsMetricSource) -> StatsSource {
        match source {
            StatisticsMetricSource::ProviderArtifact => StatsSource::IcebergPuffin,
            StatisticsMetricSource::CurrentManifest => StatsSource::IcebergManifest,
            StatisticsMetricSource::VisibleRowScan | StatisticsMetricSource::Provider(_) => {
                StatsSource::ConnectorEstimate
            }
        }
    }

    /// Only a value that is exact on a basis identical to the queried version
    /// earns `Exact`. Bounds and sketch estimates are real but inexact, which
    /// is precisely what `Estimated` means here.
    fn metric_confidence(observation: &StatisticsMetricObservation) -> Confidence {
        match observation.numeric_nature() {
            StatisticsNumericNature::Exact => Confidence::Exact,
            StatisticsNumericNature::UpperBound
            | StatisticsNumericNature::LowerBound
            | StatisticsNumericNature::TwoSidedApproximate => Confidence::Estimated,
        }
    }

    // Admission is per metric: a value measured on another basis describes
    // other rows, so it is skipped without touching its neighbours.
    let admitted = |metric: StatisticsMetric| -> Option<&StatisticsMetricObservation> {
        match evidence.metrics().get(&metric) {
            Some(StatisticsMetricState::Available(observation))
                if observation.describes_queried_rows() =>
            {
                Some(observation)
            }
            _ => None,
        }
    };
    let metric_u64 = |observation: Option<&StatisticsMetricObservation>| match observation
        .map(StatisticsMetricObservation::value)
    {
        Some(StatisticsMetricValue::U64(value)) => Some(*value),
        Some(StatisticsMetricValue::I64(value)) => u64::try_from(*value).ok(),
        Some(StatisticsMetricValue::F64(value))
            if value.is_finite() && *value >= 0.0 && *value <= u64::MAX as f64 =>
        {
            Some(*value as u64)
        }
        _ => None,
    };
    let metric_f64 = |observation: Option<&StatisticsMetricObservation>,
                      data_type: Option<&arrow::datatypes::DataType>| {
        const MAX_CONSERVATIVE_EXACT_INTEGER: u128 = 1_u128 << 53;
        let value = match observation.map(StatisticsMetricObservation::value) {
            Some(StatisticsMetricValue::U64(value)) => {
                (u128::from(*value) <= MAX_CONSERVATIVE_EXACT_INTEGER).then_some(*value as f64)?
            }
            Some(StatisticsMetricValue::I64(value)) => (value.unsigned_abs() as u128
                <= MAX_CONSERVATIVE_EXACT_INTEGER)
                .then_some(*value as f64)?,
            Some(StatisticsMetricValue::F64(value)) => *value,
            Some(StatisticsMetricValue::Bytes(value))
                if matches!(data_type, Some(arrow::datatypes::DataType::FixedSizeBinary(width)) if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH)
                    && value.len()
                        == usize::try_from(novarocks_types::largeint::LARGEINT_BYTE_WIDTH)
                            .ok()? =>
            {
                let value = novarocks_types::largeint::i128_from_be_bytes(value).ok()?;
                (value.unsigned_abs() <= MAX_CONSERVATIVE_EXACT_INTEGER).then_some(value as f64)?
            }
            _ => return None,
        };
        value.is_finite().then_some(value)
    };
    // The table-level source label reports where the row count came from; each
    // column value still carries its own confidence below.
    let row_count_observation = admitted(StatisticsMetric::RowCount);
    let source = row_count_observation
        .map(|observation| metric_source(observation.source()))
        .unwrap_or(StatsSource::ConnectorEstimate);

    let row_count = metric_u64(row_count_observation);
    let row_count_stat = match (row_count, row_count_observation) {
        (Some(value), Some(observation)) => StatValue::known(
            value,
            metric_confidence(observation),
            metric_source(observation.source()),
        ),
        _ => StatValue::missing(StatsMissingReason::ColumnNotReported("row_count".into())),
    };
    let mut base_columns = std::collections::HashMap::new();
    for column in columns {
        let name = column.name.to_ascii_lowercase();
        let key = std::sync::Arc::<str>::from(column.name.as_str());
        let missing = || StatsMissingReason::ColumnNotReported(name.clone());
        // A derived value is only as trustworthy as its weakest input, so the
        // nulls fraction takes the lower confidence of the two counts it
        // divides.
        let null_count_observation = admitted(StatisticsMetric::NullCount {
            column: std::sync::Arc::clone(&key),
        });
        let null_count = metric_u64(null_count_observation);
        let nulls_fraction = match (null_count, null_count_observation, row_count) {
            (Some(nulls), Some(observation), Some(rows)) if rows > 0 => StatValue::known(
                nulls as f64 / rows as f64,
                metric_confidence(observation).min(row_count_stat.confidence()),
                metric_source(observation.source()),
            ),
            (Some(0), Some(observation), Some(0)) => StatValue::known(
                0.0,
                metric_confidence(observation).min(row_count_stat.confidence()),
                metric_source(observation.source()),
            ),
            _ => StatValue::missing(missing()),
        };
        let column_stat =
            |metric: StatisticsMetric, data_type: Option<&arrow::datatypes::DataType>| {
                let observation = admitted(metric);
                match (metric_f64(observation, data_type), observation) {
                    (Some(value), Some(observation)) => StatValue::known(
                        value,
                        metric_confidence(observation),
                        metric_source(observation.source()),
                    ),
                    _ => StatValue::missing(missing()),
                }
            };
        base_columns.insert(
            name.clone(),
            BaseColumnStatistics {
                nulls_fraction,
                average_row_size: column_stat(
                    StatisticsMetric::AverageSize {
                        column: std::sync::Arc::clone(&key),
                    },
                    None,
                ),
                min_value: column_stat(
                    StatisticsMetric::Minimum {
                        column: std::sync::Arc::clone(&key),
                    },
                    Some(&column.data_type),
                ),
                max_value: column_stat(
                    StatisticsMetric::Maximum {
                        column: std::sync::Arc::clone(&key),
                    },
                    Some(&column.data_type),
                ),
                // A Theta NDV is approximate by construction, which is what
                // `Estimated` is for — it is not a reason to withhold the only
                // distinct-count evidence the table has. Admission still runs
                // first, so a value whose basis rows differ from the queried
                // ones never arrives here.
                ndv: column_stat(
                    StatisticsMetric::ThetaNdv {
                        column: std::sync::Arc::clone(&key),
                    },
                    None,
                ),
            },
        );
    }
    BaseTableStatistics {
        row_count: row_count_stat,
        columns: base_columns,
        source,
    }
}

/// Parse exactly one DML statement through NovaRocks' native parser.
///
/// DML callers receive the parser-owned syntax tree directly.  There is no
/// normalizer or secondary parser boundary on this path.
pub fn parse_raw_statement(sql: &str) -> Result<novarocks_parser::ast::Statement, String> {
    let mut statements = novarocks_parser::parse(sql).map_err(|error| error.to_string())?;
    match statements.len() {
        1 => Ok(statements.remove(0)),
        0 => Err("DML admission requires exactly one statement".to_string()),
        _ => Err("DML admission requires exactly one statement".to_string()),
    }
}

/// The SQL-visible kind of terminal row-mutation writer.  This maps one
/// provider-signed Arrow input shape to its immutable SQL write contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DmlWriteSinkMode {
    Data,
    RowLineageData,
    PositionDeletes,
    DeletionVectors,
    EqualityDeletes,
}

impl From<DmlWriteSinkMode> for crate::planner::distributed::write::contract::SqlWriteSinkMode {
    fn from(value: DmlWriteSinkMode) -> Self {
        match value {
            DmlWriteSinkMode::Data => Self::Data,
            DmlWriteSinkMode::RowLineageData => Self::RowLineageData,
            DmlWriteSinkMode::PositionDeletes => Self::PositionDeletes,
            DmlWriteSinkMode::DeletionVectors => Self::DeletionVectors,
            DmlWriteSinkMode::EqualityDeletes => Self::EqualityDeletes,
        }
    }
}

/// One provider-signed input field retained in a SQL terminal contract.
#[derive(Clone, Debug, PartialEq)]
pub struct DmlWriteTargetField {
    pub token: novarocks_spi::connector::ConnectorWriteFieldToken,
    pub column: novarocks_types::schema::ColumnDef,
    pub is_hidden: bool,
}

/// Immutable SQL facts for an already admitted write target.  The binding is
/// only an opaque request-local token; no provider table or writer handle can
/// cross into this value.
#[derive(Clone, Debug, PartialEq)]
pub struct DmlWriteTarget {
    pub binding: crate::binding::SqlTableBindingId,
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub fields: Vec<DmlWriteTargetField>,
}

/// Opaque SQL terminal write contract.  Application code can construct it
/// from its admitted binding facts but cannot inspect or mutate the private
/// planner contract afterwards.
#[derive(Clone, Debug)]
pub struct DmlWritePlanInput(crate::planner::distributed::write::contract::SqlWritePlanInput);

/// One provider-sealed write target admitted for final plan completion.
pub struct DmlFinalizedWriteTarget {
    pub ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
    pub handle: novarocks_spi::connector::ConnectorEncodedPayload,
}

/// Value-only completion input keyed by the exact target ordinal.
pub struct DmlFinalizedWriteTargetSet(
    pub(crate) crate::planner::distributed::write::contract::FinalizedWriteTargetSet,
);

impl DmlFinalizedWriteTargetSet {
    pub fn try_new(
        targets: impl IntoIterator<Item = DmlFinalizedWriteTarget>,
    ) -> Result<Self, String> {
        Ok(Self(
            crate::planner::distributed::write::contract::FinalizedWriteTargetSet::try_new(
                targets
                    .into_iter()
                    .map(|target| (target.ordinal, target.handle)),
            )?,
        ))
    }

    pub fn target_count(&self) -> usize {
        self.0.len()
    }
}

/// One already-negotiated provider read. The fact owns its exact physical scan
/// occurrence; live readers, leases and credentials remain in the application
/// runtime sidecar.
pub struct DmlFinalizedProviderRead {
    pub fact: crate::compiler::ProviderReadFact,
    pub read_budget: novarocks_physical_plan::ScanReadBudget,
}

/// Closed provider-read fact set consumed exactly once by final lowering.
pub struct DmlFinalizedProviderReadSet(crate::compiler::FinalizedProviderReadSet);

impl DmlFinalizedProviderReadSet {
    pub fn try_new(
        reads: impl IntoIterator<Item = DmlFinalizedProviderRead>,
    ) -> Result<Self, String> {
        crate::compiler::FinalizedProviderReadSet::try_from_facts(
            reads.into_iter().map(|read| (read.fact, read.read_budget)),
        )
        .map(Self)
        .map_err(|error| error.to_string())
    }

    pub fn empty() -> Self {
        Self(
            crate::compiler::FinalizedProviderReadSet::try_from_facts([])
                .expect("an empty finalized provider-read set is valid"),
        )
    }

    fn into_optional(self) -> Option<crate::compiler::FinalizedProviderReadSet> {
        (!self.0.is_empty()).then_some(self.0)
    }
}

/// Frozen inputs shared by every SQL-owned final write-plan constructor.
/// Neither value may be inferred from process state or live topology.
pub struct DmlFinalPlanContext {
    emission_mode: crate::compiler::SqlPhysicalEmissionMode,
    version: novarocks_physical_plan::PlanVersionId,
    dop_domain: novarocks_physical_plan::PipelineDopDomain,
    provider_reads: DmlFinalizedProviderReadSet,
}

impl DmlFinalPlanContext {
    pub fn new(
        version: novarocks_physical_plan::PlanVersionId,
        dop_domain: novarocks_physical_plan::PipelineDopDomain,
        provider_reads: DmlFinalizedProviderReadSet,
        emission_mode: crate::compiler::SqlPhysicalEmissionMode,
    ) -> Self {
        Self {
            version,
            dop_domain,
            provider_reads,
            emission_mode,
        }
    }

    fn into_parts(
        self,
    ) -> (
        novarocks_physical_plan::PlanVersionId,
        novarocks_physical_plan::PipelineDopDomain,
        Option<crate::compiler::FinalizedProviderReadSet>,
        crate::compiler::SqlPhysicalEmissionMode,
    ) {
        (
            self.version,
            self.dop_domain,
            self.provider_reads.into_optional(),
            self.emission_mode,
        )
    }
}

/// Complete immutable application contribution for one final write plan.
/// Static provider reads and write handles are consumed together so no SQL
/// write constructor can publish a plan before either contract is closed.
pub struct DmlFinalWritePlanContext {
    plan: DmlFinalPlanContext,
    targets: DmlFinalizedWriteTargetSet,
}

impl DmlFinalWritePlanContext {
    pub fn emission_mode(&self) -> crate::compiler::SqlPhysicalEmissionMode {
        self.plan.emission_mode
    }
    pub fn new(plan: DmlFinalPlanContext, targets: DmlFinalizedWriteTargetSet) -> Self {
        Self { plan, targets }
    }

    fn into_parts(self) -> (DmlFinalPlanContext, DmlFinalizedWriteTargetSet) {
        (self.plan, self.targets)
    }
}

impl DmlWritePlanInput {
    /// What the provider calls each field this target accepts, by the token it
    /// issued for it.
    ///
    /// The plan names a written field by its token, because a name is the
    /// provider's and a plan restating it is a second place for it to be
    /// wrong. The provider matches its own schema by name, so the pairing is
    /// read back here -- from the same contract the plan's tokens come from,
    /// rather than from a second description of the same target.
    pub fn accepted_field_names(&self) -> Vec<([u8; 32], Box<str>)> {
        self.0
            .contract
            .target
            .fields
            .iter()
            .map(|field| {
                (
                    field.token.to_bytes(),
                    Box::<str>::from(field.column.name.as_str()),
                )
            })
            .collect()
    }

    pub fn try_new(
        mode: DmlWriteSinkMode,
        target: DmlWriteTarget,
        input_columns: Vec<novarocks_types::schema::ColumnDef>,
        input: ConnectorWriteInputBinding,
    ) -> Result<Self, String> {
        use crate::planner::distributed::write::contract::{
            SqlWriteSinkContract, SqlWriteSinkTargetContract, SqlWriteTargetField,
        };
        use crate::planner::table::SqlTableIdentity;

        let target = SqlWriteSinkTargetContract::try_new(
            target.binding,
            SqlTableIdentity {
                catalog: target.catalog,
                namespace: target.namespace,
                table: target.table,
            },
            target
                .fields
                .into_iter()
                .map(|field| SqlWriteTargetField {
                    token: field.token,
                    column: field.column,
                    is_hidden: field.is_hidden,
                })
                .collect(),
        )?;
        Ok(Self(
            crate::planner::distributed::write::contract::SqlWritePlanInput {
                contract: SqlWriteSinkContract::try_new(mode.into(), target, input_columns)?,
                input,
                root_output_exprs: None,
            },
        ))
    }
}

fn final_lowering_error(
    error: crate::planner::distributed::build::ContractLoweringError,
) -> crate::compiler::SqlCompileError {
    match error {
        crate::planner::distributed::build::ContractLoweringError::Control(error) => error.into(),
        other => crate::compiler::SqlCompileError::Compilation(other.to_string()),
    }
}

fn final_plan_publication_error(
    error: crate::planner::distributed::build::SqlPublicationError,
) -> crate::compiler::SqlCompileError {
    match error {
        crate::planner::distributed::build::SqlPublicationError::Construction(error) => {
            final_plan_construction_error(error)
        }
        crate::planner::distributed::build::SqlPublicationError::Support(error) => {
            error.into_compile_error()
        }
    }
}

fn final_plan_construction_error(
    error: novarocks_physical_plan::PlanConstructionError,
) -> crate::compiler::SqlCompileError {
    match error {
        novarocks_physical_plan::PlanConstructionError::Constants(
            novarocks_physical_plan::ConstantReferenceError::Control(error),
        ) => error.into(),
        other => crate::compiler::SqlCompileError::Compilation(other.to_string()),
    }
}

/// Build the final write contract for one already-frozen connector source.
///
/// The source reads exactly one provider-frozen cohort, so the occurrence the
/// plan addresses that scan by is the one occurrence the finalized read set
/// accounts for. Taking it from anywhere else would let the plan name a read
/// nothing was frozen for.
#[allow(
    clippy::too_many_arguments,
    reason = "Frozen write facts and the request's read-only control remain separate inputs."
)]
pub fn build_final_frozen_connector_write_plan(
    source: crate::planning::query_execution::FrozenConnectorScanPlan,
    sink: DmlWritePlanInput,
    write_target_ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
    statistics: &[novarocks_spi::connector::StatisticsRequiredAggregation],
    functions: std::sync::Arc<dyn crate::compiler::SqlFunctionCatalog>,
    settings: &crate::compiler::SessionOptimizerSettings,
    final_write: DmlFinalWritePlanContext,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    root_allow_throw_exception: bool,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &crate::compiler::SqlCompileControl,
) -> Result<crate::compiler::SqlAuthoredPhysicalPlan, crate::compiler::SqlCompileError> {
    let scan_occurrence = final_write
        .plan
        .provider_reads
        .0
        .single_occurrence()
        .map_err(|error| error.to_string())?;
    let physical = source
        .finalize_provider_read_occurrence(scan_occurrence)
        .map_err(|error| format!("finalize frozen connector scan occurrence: {error}"))?;
    let target_schema =
        crate::planner::distributed::write::sink::ConnectorWritePlanInput::target_schema_from_sql_write_plan_input(&sink.0)?;
    let auxiliary = crate::planner::distributed::write::auxiliary::plan_writer_statistics(
        &[
            crate::planner::distributed::write::auxiliary::WriterStatisticsTargetInput {
                target: write_target_ordinal,
                input_schema: target_schema.as_ref(),
                requirements: statistics,
            },
        ],
        functions.as_ref(),
        decimal_overflow_policy,
        constant_policy,
        control,
    )?;
    complete_connector_write_plan(
        physical,
        &crate::optimizer::stats_input::QueryStatsSnapshot::empty(),
        sink.0,
        write_target_ordinal,
        &auxiliary,
        settings,
        final_write,
        functions,
        root_allow_throw_exception,
        constant_policy,
        control,
    )
}

/// One write, optimized and waiting for the reads it states.
///
/// A write is not driven need-by-need the way a statement is: the owner that
/// sealed its target compiles it itself. It still performs ordinary provider
/// reads, and the plan addresses each scan by the occurrence that read will be
/// accounted for under, so the two halves are separated here -- the needs
/// leave, the facts come back, and the plan is lowered against them.
pub struct DmlWriteCompletion {
    emission_mode: crate::compiler::SqlPhysicalEmissionMode,
    constant_policy: novarocks_functions::ConstantPolicy,
    root_allow_throw_exception: bool,
    functions: std::sync::Arc<dyn crate::compiler::SqlFunctionCatalog>,
    query_statistics: crate::optimizer::stats_input::QueryStatsSnapshot,
    physical: crate::planner::physical::PhysicalPlanNode,
    sink: DmlWritePlanInput,
    write_target_ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
    auxiliary: crate::planner::distributed::write::auxiliary::WriterAuxiliaryPlan,
}

/// Optimize one admitted write and state the provider reads it performs.
pub fn begin_final_connector_write_plan(
    request: crate::compiler::SqlOptimizeRequest<'_>,
    sink: DmlWritePlanInput,
    write_target_ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
    statistics: &[novarocks_spi::connector::StatisticsRequiredAggregation],
    settings: &crate::compiler::SessionOptimizerSettings,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    emission_mode: crate::compiler::SqlPhysicalEmissionMode,
) -> Result<
    (DmlWriteCompletion, Box<[crate::compiler::ProviderReadNeed]>),
    crate::compiler::SqlCompileError,
> {
    let constant_policy = request.constant_policy();
    let control = request.control().clone();
    let compiled = crate::compiler::SqlCompiler::optimize(request)?
        .into_optimized_output()
        .map_err(|_| "connector write intent did not produce optimized SQL facts".to_string())?;
    let physical = crate::planner::optimizer_bridge::to_physical_plan(&compiled.optimized_tree)?;
    let target_schema =
        crate::planner::distributed::write::sink::ConnectorWritePlanInput::target_schema_from_sql_write_plan_input(&sink.0)?;
    let auxiliary = crate::planner::distributed::write::auxiliary::plan_writer_statistics(
        &[
            crate::planner::distributed::write::auxiliary::WriterStatisticsTargetInput {
                target: write_target_ordinal,
                input_schema: target_schema.as_ref(),
                requirements: statistics,
            },
        ],
        compiled.function_catalog.as_ref(),
        decimal_overflow_policy,
        constant_policy,
        &control,
    )?;
    // Runtime filters are placed before the reads are stated, because a filter
    // a scan applies is part of what that scan asks its provider for.
    let mut physical = physical;
    crate::planner::physical::runtime_filter_placement::place_runtime_filters(
        &mut physical,
        settings,
    );
    let (physical, needs) = crate::compiler::collect_provider_needs(
        physical,
        0,
        settings.connector_static_predicate_pushdown_enabled(),
        &control,
    )?;
    Ok((
        DmlWriteCompletion {
            emission_mode,
            constant_policy,
            root_allow_throw_exception: compiled.root_allow_throw_exception,
            functions: compiled.function_catalog,
            query_statistics: compiled.statistics.snapshot,
            physical,
            sink,
            write_target_ordinal,
            auxiliary,
        },
        needs,
    ))
}

impl DmlWriteCompletion {
    /// Lower this write against the reads its scans were frozen with.
    pub fn finish(
        self,
        version: novarocks_physical_plan::PlanVersionId,
        dop_domain: novarocks_physical_plan::PipelineDopDomain,
        reads: DmlFinalizedProviderReadSet,
        targets: DmlFinalizedWriteTargetSet,
        control: &crate::compiler::SqlCompileControl,
    ) -> Result<crate::compiler::SqlAuthoredPhysicalPlan, crate::compiler::SqlCompileError> {
        let mut draft = crate::planner::distributed::build::lower_final_physical_write_plan(
            &self.physical,
            version,
            dop_domain,
            crate::planner::distributed::build::FinalWriteLowering {
                reads: reads.into_optional(),
                write: self.sink.0,
                write_target_ordinal: self.write_target_ordinal,
                auxiliary: &self.auxiliary,
                targets: targets.0,
            },
            self.functions,
            self.root_allow_throw_exception,
            self.constant_policy,
            self.emission_mode,
            control,
        )
        .map_err(final_lowering_error)?;
        self.query_statistics.annotate_final_plan(&mut draft);
        draft
            .finish_with_dependency_observer_observed(control)
            .map_err(final_plan_publication_error)
    }
}

/// Compile an admitted write to the staged final physical contract.
pub fn compile_final_connector_write_plan(
    request: crate::compiler::SqlOptimizeRequest<'_>,
    sink: DmlWritePlanInput,
    write_target_ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
    statistics: &[novarocks_spi::connector::StatisticsRequiredAggregation],
    settings: &crate::compiler::SessionOptimizerSettings,
    final_write: DmlFinalWritePlanContext,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
) -> Result<crate::compiler::SqlAuthoredPhysicalPlan, crate::compiler::SqlCompileError> {
    let constant_policy = request.constant_policy();
    let control = request.control().clone();
    let compiled = crate::compiler::SqlCompiler::optimize(request)?
        .into_optimized_output()
        .map_err(|_| "connector write intent did not produce optimized SQL facts".to_string())?;
    let physical = crate::planner::optimizer_bridge::to_physical_plan(&compiled.optimized_tree)?;
    let target_schema =
        crate::planner::distributed::write::sink::ConnectorWritePlanInput::target_schema_from_sql_write_plan_input(&sink.0)?;
    let auxiliary = crate::planner::distributed::write::auxiliary::plan_writer_statistics(
        &[
            crate::planner::distributed::write::auxiliary::WriterStatisticsTargetInput {
                target: write_target_ordinal,
                input_schema: target_schema.as_ref(),
                requirements: statistics,
            },
        ],
        compiled.function_catalog.as_ref(),
        decimal_overflow_policy,
        constant_policy,
        &control,
    )?;
    complete_connector_write_plan(
        physical,
        &compiled.statistics.snapshot,
        sink.0,
        write_target_ordinal,
        &auxiliary,
        settings,
        final_write,
        compiled.function_catalog,
        compiled.root_allow_throw_exception,
        constant_policy,
        &control,
    )
}

#[allow(clippy::too_many_arguments)]
fn complete_connector_write_plan(
    mut physical: crate::planner::physical::PhysicalPlanNode,
    query_statistics: &crate::optimizer::stats_input::QueryStatsSnapshot,
    sink: crate::planner::distributed::write::contract::SqlWritePlanInput,
    write_target_ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
    auxiliary: &crate::planner::distributed::write::auxiliary::WriterAuxiliaryPlan,
    settings: &crate::compiler::SessionOptimizerSettings,
    final_write: DmlFinalWritePlanContext,
    functions: std::sync::Arc<dyn crate::compiler::SqlFunctionCatalog>,
    root_allow_throw_exception: bool,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &crate::compiler::SqlCompileControl,
) -> Result<crate::compiler::SqlAuthoredPhysicalPlan, crate::compiler::SqlCompileError> {
    crate::planner::physical::runtime_filter_placement::place_runtime_filters(
        &mut physical,
        settings,
    );
    let (final_context, finalized_targets) = final_write.into_parts();
    let (version, dop_domain, reads, emission_mode) = final_context.into_parts();
    let mut draft = crate::planner::distributed::build::lower_final_physical_write_plan(
        &physical,
        version,
        dop_domain,
        crate::planner::distributed::build::FinalWriteLowering {
            reads,
            write: sink,
            write_target_ordinal,
            auxiliary,
            targets: finalized_targets.0,
        },
        functions,
        root_allow_throw_exception,
        constant_policy,
        emission_mode,
        control,
    )
    .map_err(final_lowering_error)?;
    query_statistics.annotate_final_plan(&mut draft);
    draft
        .finish_with_dependency_observer_observed(control)
        .map_err(final_plan_publication_error)
}

/// One internal DML read, optimized and waiting for its provider facts.
pub struct DmlReadCompletion {
    emission_mode: crate::compiler::SqlPhysicalEmissionMode,
    constant_policy: novarocks_functions::ConstantPolicy,
    root_allow_throw_exception: bool,
    functions: std::sync::Arc<dyn crate::compiler::SqlFunctionCatalog>,
    root_semantics: crate::compiler::root_output::RootOutputSemantics,
    query_statistics: crate::optimizer::stats_input::QueryStatsSnapshot,
    physical: crate::planner::physical::PhysicalPlanNode,
}

pub fn begin_final_dml_read_plan(
    request: crate::compiler::SqlOptimizeRequest<'_>,
    settings: &crate::compiler::SessionOptimizerSettings,
    emission_mode: crate::compiler::SqlPhysicalEmissionMode,
) -> Result<
    (DmlReadCompletion, Box<[crate::compiler::ProviderReadNeed]>),
    crate::compiler::SqlCompileError,
> {
    let constant_policy = request.constant_policy();
    let control = request.control().clone();
    let compiled = crate::compiler::SqlCompiler::optimize(request)?
        .into_optimized_output()
        .map_err(|_| "DML read intent did not produce optimized SQL facts".to_string())?;
    let mut physical =
        crate::planner::optimizer_bridge::to_physical_plan(&compiled.optimized_tree)?;
    crate::planner::physical::runtime_filter_placement::place_runtime_filters(
        &mut physical,
        settings,
    );
    let (physical, needs) = crate::compiler::collect_provider_needs(
        physical,
        0,
        settings.connector_static_predicate_pushdown_enabled(),
        &control,
    )?;
    Ok((
        DmlReadCompletion {
            emission_mode,
            constant_policy,
            root_allow_throw_exception: compiled.root_allow_throw_exception,
            functions: compiled.function_catalog,
            root_semantics: compiled.root_semantics,
            physical,
            query_statistics: compiled.statistics.snapshot,
        },
        needs,
    ))
}

impl DmlReadCompletion {
    pub fn finish(
        self,
        version: novarocks_physical_plan::PlanVersionId,
        dop_domain: novarocks_physical_plan::PipelineDopDomain,
        reads: DmlFinalizedProviderReadSet,
        control: &crate::compiler::SqlCompileControl,
    ) -> Result<crate::compiler::SqlAuthoredPhysicalPlan, crate::compiler::SqlCompileError> {
        let mut draft =
            crate::planner::distributed::build::lower_final_physical_plan_with_root_semantics(
                &self.physical,
                version,
                dop_domain,
                reads.into_optional(),
                self.root_semantics,
                self.functions,
                self.root_allow_throw_exception,
                self.constant_policy,
                self.emission_mode,
                control,
            )
            .map_err(final_lowering_error)?;
        self.query_statistics.annotate_final_plan(&mut draft);
        draft
            .finish_with_dependency_observer_observed(control)
            .map_err(final_plan_publication_error)
    }
}

/// SQL-owned, immutable CTAS source artifact. Its optimizer graph never
/// leaves this module; application code may inspect only the source schema,
/// stable capture fingerprint, and sealed write plan derived from it.
#[derive(Clone, Debug)]
pub struct DmlCtasSourcePlan {
    emission_mode: crate::compiler::SqlPhysicalEmissionMode,
    constant_policy: novarocks_functions::ConstantPolicy,
    root_allow_throw_exception: bool,
    private_output_domain: bool,
    output_domains: Box<[novarocks_physical_plan::ResultValueDomain]>,
    query_statistics: crate::optimizer::stats_input::QueryStatsSnapshot,
    optimized: crate::optimizer::OptimizedOperatorNode,
    function_catalog: std::sync::Arc<dyn crate::compiler::SqlFunctionCatalog>,
}

/// One source output field exposed to CTAS target admission.
#[derive(Clone, Debug, PartialEq)]
pub struct DmlSourceColumn {
    pub domain: novarocks_physical_plan::ResultValueDomain,
    pub name: String,
    pub data_type: arrow::datatypes::DataType,
    pub nullable: bool,
}

impl DmlCtasSourcePlan {
    /// Complete declared private domains survive even when Arrow fields omit markers.
    pub fn has_private_output_domain(&self) -> bool {
        self.private_output_domain
    }

    pub fn output_columns(&self) -> Vec<DmlSourceColumn> {
        self.optimized
            .output_columns
            .iter()
            .zip(self.output_domains.iter().copied())
            .map(|(column, domain)| DmlSourceColumn {
                domain,
                name: column.name.clone(),
                data_type: column.value_type.data_type.clone(),
                nullable: column.value_type.nullable,
            })
            .collect()
    }

    /// Versioned digest of the frozen in-memory optimizer artifact used to
    /// bind CTAS source preparation to exactly one compilation.
    pub fn capture_fingerprint(&self) -> [u8; 32] {
        use sha2::{Digest, Sha256};

        let material = format!(
            "{:#?}\n{:#?}\n{}",
            self.optimized, self.output_domains, self.private_output_domain
        );
        let root_allow = [u8::from(self.root_allow_throw_exception)];
        let mut digest = Sha256::new();
        for part in [
            b"novarocks.ctas-optimized-capture.v2".as_slice(),
            material.as_bytes(),
            root_allow.as_slice(),
        ] {
            digest.update((part.len() as u64).to_be_bytes());
            digest.update(part);
        }
        digest.finalize().into()
    }
}

/// Compile one CTAS source into an opaque SQL artifact. The source must be
/// optimized, but no distributed sink is selected until the application has
/// completed target admission.
pub fn compile_ctas_source(
    request: crate::compiler::SqlOptimizeRequest<'_>,
    emission_mode: crate::compiler::SqlPhysicalEmissionMode,
) -> Result<DmlCtasSourcePlan, crate::compiler::SqlCompileError> {
    let constant_policy = request.constant_policy();
    let compiled = crate::compiler::SqlCompiler::optimize(request)?
        .into_optimized_output()
        .map_err(|_| "CTAS source did not produce optimized SQL facts".to_string())?;
    let output_domains = compiled
        .root_semantics
        .domains(&compiled.optimized_tree.output_columns)?
        .into_boxed_slice();
    let private_output_domain = compiled.root_semantics.has_private_persistence_domain();
    Ok(DmlCtasSourcePlan {
        emission_mode,
        constant_policy,
        root_allow_throw_exception: compiled.root_allow_throw_exception,
        private_output_domain,
        output_domains,
        query_statistics: compiled.statistics.snapshot,
        optimized: compiled.optimized_tree,
        function_catalog: compiled.function_catalog,
    })
}

/// Attach the admitted CTAS sink, then state the source reads before lowering
/// the completed writer graph.
pub fn begin_final_ctas_connector_write_plan(
    source: &DmlCtasSourcePlan,
    sink: DmlWritePlanInput,
    write_target_ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
    statistics: &[novarocks_spi::connector::StatisticsRequiredAggregation],
    settings: &crate::compiler::SessionOptimizerSettings,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    control: &crate::compiler::SqlCompileControl,
) -> Result<
    (DmlWriteCompletion, Box<[crate::compiler::ProviderReadNeed]>),
    crate::compiler::SqlCompileError,
> {
    let mut physical = crate::planner::optimizer_bridge::to_physical_plan(&source.optimized)?;
    let target_schema =
        crate::planner::distributed::write::sink::ConnectorWritePlanInput::target_schema_from_sql_write_plan_input(&sink.0)?;
    let auxiliary = crate::planner::distributed::write::auxiliary::plan_writer_statistics(
        &[
            crate::planner::distributed::write::auxiliary::WriterStatisticsTargetInput {
                target: write_target_ordinal,
                input_schema: target_schema.as_ref(),
                requirements: statistics,
            },
        ],
        source.function_catalog.as_ref(),
        decimal_overflow_policy,
        source.constant_policy,
        control,
    )?;
    crate::planner::physical::runtime_filter_placement::place_runtime_filters(
        &mut physical,
        settings,
    );
    let (physical, needs) = crate::compiler::collect_provider_needs(
        physical,
        0,
        settings.connector_static_predicate_pushdown_enabled(),
        control,
    )?;
    Ok((
        DmlWriteCompletion {
            emission_mode: source.emission_mode,
            constant_policy: source.constant_policy,
            root_allow_throw_exception: source.root_allow_throw_exception,
            functions: source.function_catalog.clone(),
            query_statistics: source.query_statistics.clone(),
            physical,
            sink,
            write_target_ordinal,
            auxiliary,
        },
        needs,
    ))
}

/// Provider route facts that SQL binds to a change-stream producer. Exact
/// producer-output occurrences are frozen upstream; neither SQL nor the
/// application reconstructs identity from display names.
#[derive(Clone, Debug)]
pub struct DmlChangeStreamRoute {
    pub route_id: novarocks_spi::connector::ConnectorWriteRouteId,
    /// The begin session's dense, query-local ordinal for the logical write
    /// target this branch feeds. It is the branch's whole identity: nothing
    /// downstream recovers a cohort, an operation, or a placement from it.
    pub write_target_ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
    pub accepted_effects: Vec<novarocks_spi::connector::ConnectorRowMutationEffect>,
    /// Provider-signed bindings from target-field token to the exact producer
    /// output occurrence. SQL validates these ordinals against the completed
    /// producer; it never recovers them from display names.
    pub input_ordinals: Vec<novarocks_spi::connector::ConnectorMutationRouteInput>,
    pub partition_input_tokens: Vec<novarocks_spi::connector::ConnectorWriteFieldToken>,
    pub sink: DmlWritePlanInput,
}

/// Provider-selected ordinary aggregate requirements for exactly one routed
/// change-stream write target. The target key is explicit so SQL can prove
/// that the requirements cover the same closed target set as the router.
#[derive(Clone, Debug)]
pub struct DmlChangeStreamStatisticsTarget {
    pub write_target_ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
    pub requirements: Vec<novarocks_spi::connector::StatisticsRequiredAggregation>,
}

/// SQL-only specification of a generated row-mutation producer.
#[derive(Clone, Debug)]
pub enum DmlChangeStreamKind {
    Update {
        target_columns: Vec<novarocks_types::schema::ColumnDef>,
    },
    Merge {
        target_columns: Vec<novarocks_types::schema::ColumnDef>,
        matched_update: bool,
        matched_delete: bool,
        not_matched_insert: bool,
    },
}

/// Optional duplicate-match assertion installed immediately before change
/// event expansion.  It is a pure SQL physical constraint; lifecycle and
/// connector fencing remain application-owned.
#[derive(Clone, Debug)]
pub struct DmlPreExpandKeyedAssert {
    pub key_column_name: String,
    pub key_label: String,
    pub message_prefix: String,
}

/// A request that consumes one immutable compile input and a fully admitted,
/// provider-signed SQL write route set.
pub struct DmlChangeStreamCompileRequest<'a> {
    pub optimize_request: crate::compiler::SqlOptimizeRequest<'a>,
    pub kind: DmlChangeStreamKind,
    pub routes: Vec<DmlChangeStreamRoute>,
    pub statistics_targets: Vec<DmlChangeStreamStatisticsTarget>,
    pub pre_expand_keyed_assert: Option<DmlPreExpandKeyedAssert>,
    pub shape: DmlWritePlanShape,
}

/// Staged final-plan request, kept separate from the current production
/// carrier so callers cannot select a partial migration at runtime.
pub struct DmlFinalChangeStreamCompileRequest<'a> {
    pub optimize_request: crate::compiler::SqlOptimizeRequest<'a>,
    pub kind: DmlChangeStreamKind,
    pub routes: Vec<DmlChangeStreamRoute>,
    pub statistics_targets: Vec<DmlChangeStreamStatisticsTarget>,
    pub pre_expand_keyed_assert: Option<DmlPreExpandKeyedAssert>,
    pub shape: DmlWritePlanShape,
    pub final_write: DmlFinalWritePlanContext,
}

pub(crate) struct DmlFinalChangeStreamSealContext {
    pub(crate) pre_expand_keyed_assert: Option<DmlPreExpandKeyedAssert>,
    pub(crate) shape: DmlWritePlanShape,
    pub(crate) final_write: DmlFinalWritePlanContext,
}

/// Which terminal shape a sealed write plan should take.
///
/// Both exist only while callers move from the terminal-sink form to the NCP-6
/// dataflow form one at a time. A caller states it explicitly rather than
/// inheriting a default, because the two shapes need different frontend and
/// backend handling and a silently wrong default would surface as a runtime
/// failure rather than a compile error.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DmlWritePlanShape {
    /// The writer is an ordinary node whose rows gather into one Root finish
    /// fragment ending in the ordinary result sink.
    #[default]
    Dataflow,
}

/// Optimizer policy for generated mutation change streams.  This stays in the
/// SQL facade so callers do not reproduce a physical-plan safety rule.
pub fn dml_change_stream_optimizer_settings() -> crate::optimizer::options::SessionOptimizerSettings
{
    // A generated mutation plan carries before/after rows over independent
    // branches. A query runtime filter can describe only one branch, so it
    // must not suppress locator rows required by a DELETE route.
    crate::optimizer::options::SessionOptimizerSettings {
        enable_global_runtime_filter: Some(false),
        ..Default::default()
    }
}

/// Return deterministic SQL-owned optimizer settings material for a CTAS
/// execution digest. The exact canonicalization remains private to SQL.
pub fn optimizer_settings_stable_digest_material(
    settings: &crate::compiler::SessionOptimizerSettings,
) -> Vec<u8> {
    settings.stable_digest_material()
}

/// Staged final change-stream contract and its typed writer destinations.
pub struct DmlFinalChangeStreamPlan {
    physical_plan: crate::compiler::SqlAuthoredPhysicalPlan,
    writer_routes: Vec<DmlFinalChangeStreamWriterRoute>,
}

/// An optimized change stream whose provider reads have been stated but not
/// frozen. The application supplies their exact facts before lowering.
pub struct DmlChangeStreamCompletion {
    emission_mode: crate::compiler::SqlPhysicalEmissionMode,
    constant_policy: novarocks_functions::ConstantPolicy,
    root_allow_throw_exception: bool,
    functions: std::sync::Arc<dyn crate::compiler::SqlFunctionCatalog>,
    query_statistics: crate::optimizer::stats_input::QueryStatsSnapshot,
    physical: crate::planner::physical::PhysicalPlanNode,
    dag: crate::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec,
    auxiliary: crate::planner::distributed::write::auxiliary::WriterAuxiliaryPlan,
}

impl DmlChangeStreamCompletion {
    pub fn finish(
        self,
        final_write: DmlFinalWritePlanContext,
        control: &crate::compiler::SqlCompileControl,
    ) -> Result<DmlFinalChangeStreamPlan, crate::compiler::SqlCompileError> {
        let (final_context, finalized_targets) = final_write.into_parts();
        let (version, dop_domain, reads, emission_mode) = final_context.into_parts();
        if emission_mode != self.emission_mode {
            return Err(
                "change-stream completion mode differs from its final write context"
                    .to_string()
                    .into(),
            );
        }
        let mut draft = crate::planner::distributed::build::lower_final_change_stream_write_plan(
            &self.physical,
            version,
            dop_domain,
            crate::planner::distributed::build::FinalChangeStreamWriteLowering {
                reads,
                dag: self.dag,
                auxiliary: &self.auxiliary,
                targets: finalized_targets.0,
            },
            self.functions,
            self.root_allow_throw_exception,
            self.constant_policy,
            self.emission_mode,
            control,
        )
        .map_err(final_lowering_error)?;
        self.query_statistics.annotate_final_plan(&mut draft);
        let physical_plan = draft
            .finish_with_dependency_observer_observed(control)
            .map_err(final_plan_publication_error)?;
        let writer_routes = completed_change_stream_writer_routes(physical_plan.plan())?;
        Ok(DmlFinalChangeStreamPlan {
            physical_plan,
            writer_routes,
        })
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn begin_final_change_stream_producer_with_effect_ordinal(
    producer: crate::optimizer::OptimizedOperatorNode,
    query_statistics: crate::optimizer::stats_input::QueryStatsSnapshot,
    routes: Vec<DmlChangeStreamRoute>,
    statistics_targets: Vec<DmlChangeStreamStatisticsTarget>,
    effect_output_ordinal: usize,
    functions: std::sync::Arc<dyn crate::compiler::SqlFunctionCatalog>,
    pre_expand_keyed_assert: Option<DmlPreExpandKeyedAssert>,
    shape: DmlWritePlanShape,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    root_allow_throw_exception: bool,
    constant_policy: novarocks_functions::ConstantPolicy,
    emission_mode: crate::compiler::SqlPhysicalEmissionMode,
    control: &crate::compiler::SqlCompileControl,
) -> Result<
    (
        DmlChangeStreamCompletion,
        Box<[crate::compiler::ProviderReadNeed]>,
    ),
    crate::compiler::SqlCompileError,
> {
    let crate::optimizer::operator::Operator::PhysicalChangeEventExpand(expand) = &producer.op
    else {
        return Err(
            "change-stream producer root must be the native ChangeEventExpand"
                .to_string()
                .into(),
        );
    };
    let effect_output = producer
        .output_columns
        .get(effect_output_ordinal)
        .ok_or_else(|| "change-stream effect output ordinal is out of bounds".to_string())?;
    if effect_output.column_id != expand.effect_column_id {
        return Err("change-stream effect output ordinal does not identify the native ChangeEventExpand effect".to_string().into());
    }
    let auxiliary = plan_change_stream_writer_statistics(
        &routes,
        statistics_targets,
        functions.as_ref(),
        decimal_overflow_policy,
        constant_policy,
        control,
    )?;
    let dag = bind_route_layout(&producer.output_columns, routes, effect_output_ordinal)?;
    let mut physical = crate::planner::optimizer_bridge::to_physical_plan(&producer)?;
    let settings = dml_change_stream_optimizer_settings();
    match shape {
        DmlWritePlanShape::Dataflow => {}
    }
    if let Some(assertion) = pre_expand_keyed_assert {
        crate::planner::pipeline::insert_pre_expand_keyed_assert(
            &mut physical,
            &crate::planner::physical::PreExpandKeyedAssertSpec {
                key_column_name: assertion.key_column_name,
                key_label: assertion.key_label,
                message_prefix: assertion.message_prefix,
            },
        )?;
    }
    crate::planner::physical::runtime_filter_placement::place_runtime_filters(
        &mut physical,
        &settings,
    );
    let (physical, needs) = crate::compiler::collect_provider_needs(
        physical,
        0,
        settings.connector_static_predicate_pushdown_enabled(),
        control,
    )?;
    Ok((
        DmlChangeStreamCompletion {
            emission_mode,
            constant_policy,
            root_allow_throw_exception,
            functions,
            query_statistics,
            physical,
            dag,
            auxiliary,
        },
        needs,
    ))
}

#[derive(Clone, Debug)]
pub struct DmlFinalChangeStreamWriterRoute {
    pub route_id: novarocks_spi::connector::ConnectorWriteRouteId,
    pub write_target_ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
    pub accepted_effects: Vec<novarocks_spi::connector::ConnectorRowMutationEffect>,
    pub writer_fragment_id: novarocks_physical_plan::FragmentId,
}

impl DmlFinalChangeStreamPlan {
    pub fn physical_plan(&self) -> &novarocks_physical_plan::PhysicalPlan {
        self.physical_plan.plan()
    }

    pub fn into_parts(
        self,
    ) -> (
        crate::compiler::SqlAuthoredPhysicalPlan,
        Vec<DmlFinalChangeStreamWriterRoute>,
    ) {
        (self.physical_plan, self.writer_routes)
    }
}

/// Optimize a generated change stream and state every provider read before
/// its writer graph is lowered against the frozen provider facts.
pub fn begin_final_dml_change_stream(
    request: DmlChangeStreamCompileRequest<'_>,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    emission_mode: crate::compiler::SqlPhysicalEmissionMode,
) -> Result<
    (
        DmlChangeStreamCompletion,
        Box<[crate::compiler::ProviderReadNeed]>,
    ),
    crate::compiler::SqlCompileError,
> {
    let constant_policy = request.optimize_request.constant_policy();
    let control = request.optimize_request.control().clone();
    let compiled = crate::compiler::SqlCompiler::optimize(request.optimize_request)?
        .into_optimized_output()
        .map_err(|_| "change-stream intent did not produce an optimized SQL plan".to_string())?;
    let producer = match request.kind {
        DmlChangeStreamKind::Update { target_columns } => {
            build_update_change_event_expand(compiled.optimized_tree, &target_columns, &control)?
        }
        DmlChangeStreamKind::Merge {
            target_columns,
            matched_update,
            matched_delete,
            not_matched_insert,
        } => build_merge_change_event_expand(
            compiled.optimized_tree,
            &target_columns,
            matched_update,
            matched_delete,
            not_matched_insert,
            &control,
        )?,
    };
    let effect_output_ordinal = producer
        .output_columns
        .len()
        .checked_sub(1)
        .ok_or_else(|| "change-stream producer has no effect output occurrence".to_string())?;
    begin_final_change_stream_producer_with_effect_ordinal(
        producer,
        compiled.statistics.snapshot,
        request.routes,
        request.statistics_targets,
        effect_output_ordinal,
        compiled.function_catalog,
        request.pre_expand_keyed_assert,
        request.shape,
        decimal_overflow_policy,
        compiled.root_allow_throw_exception,
        constant_policy,
        emission_mode,
        &control,
    )
}

/// Compile a generated change stream into the staged final physical contract.
pub fn compile_final_dml_change_stream(
    request: DmlFinalChangeStreamCompileRequest<'_>,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
) -> Result<DmlFinalChangeStreamPlan, crate::compiler::SqlCompileError> {
    let constant_policy = request.optimize_request.constant_policy();
    let control = request.optimize_request.control().clone();
    let compiled = crate::compiler::SqlCompiler::optimize(request.optimize_request)?
        .into_optimized_output()
        .map_err(|_| "change-stream intent did not produce an optimized SQL plan".to_string())?;
    let producer = match request.kind {
        DmlChangeStreamKind::Update { target_columns } => {
            build_update_change_event_expand(compiled.optimized_tree, &target_columns, &control)?
        }
        DmlChangeStreamKind::Merge {
            target_columns,
            matched_update,
            matched_delete,
            not_matched_insert,
        } => build_merge_change_event_expand(
            compiled.optimized_tree,
            &target_columns,
            matched_update,
            matched_delete,
            not_matched_insert,
            &control,
        )?,
    };
    seal_final_change_stream_producer(
        producer,
        compiled.statistics.snapshot,
        request.routes,
        request.statistics_targets,
        compiled.function_catalog,
        DmlFinalChangeStreamSealContext {
            pre_expand_keyed_assert: request.pre_expand_keyed_assert,
            shape: request.shape,
            final_write: request.final_write,
        },
        decimal_overflow_policy,
        compiled.root_allow_throw_exception,
        constant_policy,
        &control,
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "The producer, signed routes, statistics, authored policy and request control are distinct compiler inputs."
)]
pub(crate) fn seal_final_change_stream_producer(
    producer: crate::optimizer::OptimizedOperatorNode,
    query_statistics: crate::optimizer::stats_input::QueryStatsSnapshot,
    routes: Vec<DmlChangeStreamRoute>,
    statistics_targets: Vec<DmlChangeStreamStatisticsTarget>,
    functions: std::sync::Arc<dyn crate::compiler::SqlFunctionCatalog>,
    context: DmlFinalChangeStreamSealContext,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    root_allow_throw_exception: bool,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &crate::compiler::SqlCompileControl,
) -> Result<DmlFinalChangeStreamPlan, crate::compiler::SqlCompileError> {
    let crate::optimizer::operator::Operator::PhysicalChangeEventExpand(expand) = &producer.op
    else {
        return Err(
            "change-stream producer root must be the native ChangeEventExpand"
                .to_string()
                .into(),
        );
    };
    let effect_output_ordinal = producer
        .output_columns
        .len()
        .checked_sub(1)
        .ok_or_else(|| "change-stream producer has no effect output occurrence".to_string())?;
    if producer.output_columns[effect_output_ordinal].column_id != expand.effect_column_id {
        return Err(
            "change-stream producer effect must be its final ordered output occurrence"
                .to_string()
                .into(),
        );
    }
    seal_final_change_stream_producer_with_effect_ordinal(
        producer,
        query_statistics,
        routes,
        statistics_targets,
        effect_output_ordinal,
        functions,
        context,
        decimal_overflow_policy,
        root_allow_throw_exception,
        constant_policy,
        control,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn seal_final_change_stream_producer_with_effect_ordinal(
    producer: crate::optimizer::OptimizedOperatorNode,
    query_statistics: crate::optimizer::stats_input::QueryStatsSnapshot,
    routes: Vec<DmlChangeStreamRoute>,
    statistics_targets: Vec<DmlChangeStreamStatisticsTarget>,
    effect_output_ordinal: usize,
    functions: std::sync::Arc<dyn crate::compiler::SqlFunctionCatalog>,
    context: DmlFinalChangeStreamSealContext,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    root_allow_throw_exception: bool,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &crate::compiler::SqlCompileControl,
) -> Result<DmlFinalChangeStreamPlan, crate::compiler::SqlCompileError> {
    let crate::optimizer::operator::Operator::PhysicalChangeEventExpand(expand) = &producer.op
    else {
        return Err(
            "change-stream producer root must be the native ChangeEventExpand"
                .to_string()
                .into(),
        );
    };
    let effect_output = producer
        .output_columns
        .get(effect_output_ordinal)
        .ok_or_else(|| "change-stream effect output ordinal is out of bounds".to_string())?;
    if effect_output.column_id != expand.effect_column_id {
        return Err(
            "change-stream effect output ordinal does not identify the native ChangeEventExpand effect"
                .to_string().into());
    }
    let auxiliary = plan_change_stream_writer_statistics(
        &routes,
        statistics_targets,
        functions.as_ref(),
        decimal_overflow_policy,
        constant_policy,
        control,
    )?;
    let dag = bind_route_layout(&producer.output_columns, routes, effect_output_ordinal)?;
    let keyed_assert = context.pre_expand_keyed_assert.map(|assertion| {
        crate::planner::physical::PreExpandKeyedAssertSpec {
            key_column_name: assertion.key_column_name,
            key_label: assertion.key_label,
            message_prefix: assertion.message_prefix,
        }
    });
    let mut physical = crate::planner::optimizer_bridge::to_physical_plan(&producer)?;
    let settings = dml_change_stream_optimizer_settings();
    match context.shape {
        DmlWritePlanShape::Dataflow => {}
    }
    if let Some(keyed_assert) = keyed_assert {
        crate::planner::pipeline::insert_pre_expand_keyed_assert(&mut physical, &keyed_assert)?;
    }
    crate::planner::physical::runtime_filter_placement::place_runtime_filters(
        &mut physical,
        &settings,
    );
    let (final_context, finalized_targets) = context.final_write.into_parts();
    let (version, dop_domain, reads, emission_mode) = final_context.into_parts();
    let mut draft = crate::planner::distributed::build::lower_final_change_stream_write_plan(
        &physical,
        version,
        dop_domain,
        crate::planner::distributed::build::FinalChangeStreamWriteLowering {
            reads,
            dag,
            auxiliary: &auxiliary,
            targets: finalized_targets.0,
        },
        functions,
        root_allow_throw_exception,
        constant_policy,
        emission_mode,
        control,
    )
    .map_err(final_lowering_error)?;
    query_statistics.annotate_final_plan(&mut draft);
    let physical_plan = draft
        .finish_with_dependency_observer_observed(control)
        .map_err(final_plan_publication_error)?;
    let writer_routes = completed_change_stream_writer_routes(physical_plan.plan())?;
    Ok(DmlFinalChangeStreamPlan {
        physical_plan,
        writer_routes,
    })
}

fn completed_change_stream_writer_routes(
    plan: &novarocks_physical_plan::PhysicalPlan,
) -> Result<Vec<DmlFinalChangeStreamWriterRoute>, String> {
    let mut routers = plan.fragments().values().filter_map(|fragment| {
        let novarocks_physical_plan::FragmentSink::Router { routes, .. } = fragment.sink() else {
            return None;
        };
        Some(routes)
    });
    let routes = routers
        .next()
        .ok_or_else(|| "completed change-stream plan has no router sink".to_string())?;
    if routers.next().is_some() {
        return Err("completed change-stream plan has multiple router sinks".to_string());
    }
    routes
        .iter()
        .map(|route| {
            let edge = plan.edges().get(&route.edge).ok_or_else(|| {
                format!(
                    "completed change-stream route references absent edge {}",
                    route.edge.get()
                )
            })?;
            Ok(DmlFinalChangeStreamWriterRoute {
                route_id: route.route_id,
                write_target_ordinal: route.write_target_ordinal,
                accepted_effects: route.accepted_effects.to_vec(),
                writer_fragment_id: edge.destination.fragment,
            })
        })
        .collect()
}

fn plan_change_stream_writer_statistics(
    routes: &[DmlChangeStreamRoute],
    statistics_targets: Vec<DmlChangeStreamStatisticsTarget>,
    functions: &dyn crate::compiler::SqlFunctionCatalog,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<
    crate::planner::distributed::write::auxiliary::WriterAuxiliaryPlan,
    crate::compiler::SqlCompileError,
> {
    use std::collections::{BTreeMap, BTreeSet};

    let route_targets = routes
        .iter()
        .map(|route| route.write_target_ordinal)
        .collect::<BTreeSet<_>>();
    if route_targets.len() != routes.len() {
        return Err("change-stream statistics routes contain a duplicate target ordinal".into());
    }

    let mut requirements_by_target = BTreeMap::new();
    for target in statistics_targets {
        if requirements_by_target
            .insert(target.write_target_ordinal, target.requirements)
            .is_some()
        {
            return Err(format!(
                "change-stream statistics repeat write target {}",
                target.write_target_ordinal.get()
            )
            .into());
        }
    }
    let statistics_target_set = requirements_by_target
        .keys()
        .copied()
        .collect::<BTreeSet<_>>();
    if statistics_target_set != route_targets {
        let missing = route_targets
            .difference(&statistics_target_set)
            .map(|target| target.get())
            .collect::<Vec<_>>();
        let extraneous = statistics_target_set
            .difference(&route_targets)
            .map(|target| target.get())
            .collect::<Vec<_>>();
        return Err(format!(
            "change-stream statistics target membership differs from routes; missing={missing:?}, extraneous={extraneous:?}"
        ).into());
    }

    let target_schemas = routes
        .iter()
        .map(|route| {
            crate::planner::distributed::write::sink::ConnectorWritePlanInput::target_schema_from_sql_write_plan_input(
                &route.sink.0,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let inputs = routes
        .iter()
        .zip(target_schemas.iter())
        .map(|(route, schema)| {
            let requirements = requirements_by_target
                .get(&route.write_target_ordinal)
                .ok_or_else(|| {
                    format!(
                        "change-stream statistics lost write target {} after membership validation",
                        route.write_target_ordinal.get()
                    )
                })?;
            Ok(
                crate::planner::distributed::write::auxiliary::WriterStatisticsTargetInput {
                    target: route.write_target_ordinal,
                    input_schema: schema.as_ref(),
                    requirements,
                },
            )
        })
        .collect::<Result<Vec<_>, String>>()?;
    crate::planner::distributed::write::auxiliary::plan_writer_statistics(
        &inputs,
        functions,
        decimal_overflow_policy,
        constant_policy,
        control,
    )
}

fn bind_route_layout(
    output_columns: &[crate::analysis::OutputColumn],
    routes: Vec<DmlChangeStreamRoute>,
    effect_output_ordinal: usize,
) -> Result<crate::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec, String> {
    use crate::planner::distributed::write::change_stream::{
        ChangeStreamWriteLayoutRequest, ChangeStreamWriteLayoutRoute,
        bind_change_stream_write_layout,
    };

    let routes = routes
        .into_iter()
        .map(|route| {
            Ok(ChangeStreamWriteLayoutRoute {
                route_id: route.route_id,
                write_target_ordinal: route.write_target_ordinal,
                accepted_effects: route.accepted_effects,
                input_ordinals: route.input_ordinals,
                partition_input_tokens: route.partition_input_tokens,
                sink: route.sink.0,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    bind_change_stream_write_layout(ChangeStreamWriteLayoutRequest {
        producer_output_columns: output_columns,
        effect_output_ordinal,
        routes,
    })
}

fn build_update_change_event_expand(
    optimized_tree: crate::optimizer::OptimizedOperatorNode,
    target_columns: &[novarocks_types::schema::ColumnDef],
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<crate::optimizer::OptimizedOperatorNode, crate::compiler::SqlCompileError> {
    control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)?;
    let mut arena = clone_scalar_arena(&optimized_tree, "MOR UPDATE")?;
    let child_outputs = optimized_tree.output_columns.clone();
    let row_id = output_column_by_name(&child_outputs, "__nr_row_id", "UPDATE row id")?;
    let distributed = distribute_producer(optimized_tree, row_id.column_id);
    let (file, pos, targets, row_id, last_sequence, effect) =
        allocate_change_outputs(&distributed, target_columns);
    let mut assignments = vec![
        output_expr(
            &mut arena,
            &child_outputs,
            "__nr_file",
            "UPDATE old file",
            file.column_id,
            control,
        )?,
        output_expr(
            &mut arena,
            &child_outputs,
            "__nr_pos",
            "UPDATE old row position",
            pos.column_id,
            control,
        )?,
    ];
    for (name, output) in &targets {
        let new_name = format!("__nr_new_{name}");
        let expr = match maybe_output_column_by_name(&child_outputs, &new_name)? {
            Some(column) => intern_column(&mut arena, &column, control)?,
            None => child_expr(
                &mut arena,
                &child_outputs,
                name,
                "UPDATE unchanged target column",
                control,
            )?,
        };
        assignments.push(crate::optimizer::operator::ChangeEventOutputExpr {
            output_column_id: output.column_id,
            expr: Some(expr),
        });
    }
    assignments.push(output_expr(
        &mut arena,
        &child_outputs,
        "__nr_row_id",
        "UPDATE old row id",
        row_id.column_id,
        control,
    )?);
    // Unassigned sequence outputs are NULL and inherit the actual commit sequence.
    build_change_expand(
        distributed,
        arena,
        change_output_columns(&file, &pos, &targets, &row_id, &last_sequence, &effect),
        effect.column_id,
        vec![crate::optimizer::operator::ChangeEventSpec {
            predicate: None,
            effect: novarocks_spi::connector::ConnectorRowMutationEffect::Replace,
            assignments,
        }],
    )
    .map_err(crate::compiler::SqlCompileError::Compilation)
}

#[allow(clippy::too_many_arguments)]
fn build_merge_change_event_expand(
    optimized_tree: crate::optimizer::OptimizedOperatorNode,
    target_columns: &[novarocks_types::schema::ColumnDef],
    matched_update: bool,
    matched_delete: bool,
    not_matched_insert: bool,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<crate::optimizer::OptimizedOperatorNode, crate::compiler::SqlCompileError> {
    control.checkpoint(novarocks_type_contract::CompilePhase::Validate, 0)?;
    let mut arena = clone_scalar_arena(&optimized_tree, "MOR MERGE")?;
    let child_outputs = optimized_tree.output_columns.clone();
    let assert_key =
        output_column_by_name(&child_outputs, "__nr_merge_assert_key", "MERGE assert key")?;
    let distributed = distribute_producer(optimized_tree, assert_key.column_id);
    let (file, pos, targets, row_id, last_sequence, effect) =
        allocate_change_outputs(&distributed, target_columns);
    let mut delete_assignments = vec![
        output_expr(
            &mut arena,
            &child_outputs,
            "__nr_file",
            "MERGE old file",
            file.column_id,
            control,
        )?,
        output_expr(
            &mut arena,
            &child_outputs,
            "__nr_pos",
            "MERGE old row position",
            pos.column_id,
            control,
        )?,
    ];
    let mut reuse_assignments = vec![
        output_expr(
            &mut arena,
            &child_outputs,
            "__nr_file",
            "MERGE old file",
            file.column_id,
            control,
        )?,
        output_expr(
            &mut arena,
            &child_outputs,
            "__nr_pos",
            "MERGE old row position",
            pos.column_id,
            control,
        )?,
    ];
    let mut fresh_assignments = Vec::with_capacity(targets.len());
    for (name, output) in &targets {
        delete_assignments.push(output_expr(
            &mut arena,
            &child_outputs,
            name,
            "MERGE old target column",
            output.column_id,
            control,
        )?);
        let new_name = format!("__nr_new_{name}");
        let reuse = match maybe_output_column_by_name(&child_outputs, &new_name)? {
            Some(column) => intern_column(&mut arena, &column, control)?,
            None => child_expr(
                &mut arena,
                &child_outputs,
                name,
                "MERGE unchanged target column",
                control,
            )?,
        };
        reuse_assignments.push(crate::optimizer::operator::ChangeEventOutputExpr {
            output_column_id: output.column_id,
            expr: Some(reuse),
        });
        let insert_name = format!("__nr_ins_{name}");
        if let Some(column) = maybe_output_column_by_name(&child_outputs, &insert_name)? {
            fresh_assignments.push(crate::optimizer::operator::ChangeEventOutputExpr {
                output_column_id: output.column_id,
                expr: Some(intern_column(&mut arena, &column, control)?),
            });
        }
    }
    reuse_assignments.push(output_expr(
        &mut arena,
        &child_outputs,
        "__nr_row_id",
        "MERGE old row id",
        row_id.column_id,
        control,
    )?);
    // Replace and Insert leave the sequence unassigned for commit-time inheritance.
    let mut events = Vec::new();
    if matched_update {
        events.push(crate::optimizer::operator::ChangeEventSpec {
            predicate: Some(merge_action_predicate(
                &mut arena,
                &child_outputs,
                1,
                control,
            )?),
            effect: novarocks_spi::connector::ConnectorRowMutationEffect::Replace,
            assignments: reuse_assignments,
        });
    }
    if matched_delete {
        events.push(crate::optimizer::operator::ChangeEventSpec {
            predicate: Some(merge_action_predicate(
                &mut arena,
                &child_outputs,
                2,
                control,
            )?),
            effect: novarocks_spi::connector::ConnectorRowMutationEffect::Delete,
            assignments: delete_assignments,
        });
    }
    if not_matched_insert {
        events.push(crate::optimizer::operator::ChangeEventSpec {
            predicate: Some(merge_action_predicate(
                &mut arena,
                &child_outputs,
                3,
                control,
            )?),
            effect: novarocks_spi::connector::ConnectorRowMutationEffect::Insert,
            assignments: fresh_assignments,
        });
    }
    if events.is_empty() {
        return Err("MOR MERGE change-stream expand requires at least one event"
            .to_string()
            .into());
    }
    build_change_expand(
        distributed,
        arena,
        change_output_columns(&file, &pos, &targets, &row_id, &last_sequence, &effect),
        effect.column_id,
        events,
    )
    .map_err(crate::compiler::SqlCompileError::Compilation)
}

fn clone_scalar_arena(
    optimized_tree: &crate::optimizer::OptimizedOperatorNode,
    operation: &str,
) -> Result<crate::optimizer::scalar::ScalarArena, String> {
    optimized_tree
        .execution_props
        .scalar_arena
        .as_deref()
        .cloned()
        .ok_or_else(|| format!("{operation} physical plan is missing scalar arena"))
}

fn distribute_producer(
    optimized_tree: crate::optimizer::OptimizedOperatorNode,
    key: crate::column_id::ColumnId,
) -> crate::optimizer::OptimizedOperatorNode {
    let stats = optimized_tree.stats.clone();
    let output_columns = optimized_tree.output_columns.clone();
    crate::optimizer::OptimizedOperatorNode {
        op: crate::optimizer::operator::Operator::PhysicalDistribution(
            crate::optimizer::operator::PhysicalDistributionOp {
                spec: crate::optimizer::property::DistributionSpec::shuffle_agg([key]),
            },
        ),
        children: vec![optimized_tree],
        stats,
        explain_stats: crate::optimizer::optimized_tree::OptimizerExplainStats::default(),
        output_columns,
        execution_props: crate::optimizer::optimized_tree::PlanExecutionProps::default(),
    }
}

type ChangeOutputs = (
    crate::analysis::OutputColumn,
    crate::analysis::OutputColumn,
    Vec<(String, crate::analysis::OutputColumn)>,
    crate::analysis::OutputColumn,
    crate::analysis::OutputColumn,
    crate::analysis::OutputColumn,
);

fn allocate_change_outputs(
    node: &crate::optimizer::OptimizedOperatorNode,
    target_columns: &[novarocks_types::schema::ColumnDef],
) -> ChangeOutputs {
    let mut next = max_physical_column_id(node) + 1;
    let mut allocate =
        |name: &str, data_type: arrow::datatypes::DataType, nullable: bool, is_internal: bool| {
            let output = crate::analysis::OutputColumn {
                column_id: crate::column_id::ColumnId(next),
                name: name.to_string(),
                value_type: novarocks_type_contract::FunctionValueType::new(data_type, nullable),

                is_internal,
            };
            next += 1;
            output
        };
    let file = allocate(
        ICEBERG_FILE_PATH_COLUMN,
        arrow::datatypes::DataType::Utf8,
        true,
        true,
    );
    let pos = allocate(
        ICEBERG_ROW_POSITION_COLUMN,
        arrow::datatypes::DataType::Int64,
        true,
        true,
    );
    let targets = target_columns
        .iter()
        .map(|column| {
            (
                column.name.clone(),
                allocate(
                    &column.name,
                    column.data_type.clone(),
                    // An event may leave this field unassigned, and a MERGE
                    // source may supply a nullable value even when the sink
                    // rejects null rows. This relation describes the event
                    // stream, not the sink's acceptance constraint.
                    true,
                    false,
                ),
            )
        })
        .collect();
    let row_id = allocate(
        ICEBERG_ROW_ID_COLUMN,
        arrow::datatypes::DataType::Int64,
        true,
        true,
    );
    let last_sequence = allocate(
        ICEBERG_LAST_UPDATED_SEQUENCE_COLUMN,
        arrow::datatypes::DataType::Int64,
        true,
        true,
    );
    let effect = allocate(
        crate::common::change_stream::ROW_MUTATION_EFFECT_COLUMN,
        arrow::datatypes::DataType::Int8,
        false,
        true,
    );
    (file, pos, targets, row_id, last_sequence, effect)
}

fn change_output_columns(
    file: &crate::analysis::OutputColumn,
    pos: &crate::analysis::OutputColumn,
    targets: &[(String, crate::analysis::OutputColumn)],
    row_id: &crate::analysis::OutputColumn,
    last_sequence: &crate::analysis::OutputColumn,
    effect: &crate::analysis::OutputColumn,
) -> Vec<crate::analysis::OutputColumn> {
    // The order is the provider's signed input shape: a row-lineage input is
    // its data fields -- the target's own columns and the v3 lineage they
    // carry forward -- and then the `_file`/`_pos` row identity. Each branch
    // names its columns by their position in that shape and the router reads
    // this producer at those positions, so emitting the identity first would
    // hand the delete branch a target column where it expects a file path.
    let mut columns = Vec::with_capacity(targets.len() + 6);
    columns.extend(targets.iter().map(|(_, column)| column.clone()));
    columns.push(row_id.clone());
    columns.push(last_sequence.clone());
    columns.push(file.clone());
    columns.push(pos.clone());
    columns.push(effect.clone());
    columns
}

fn output_expr(
    arena: &mut crate::optimizer::scalar::ScalarArena,
    columns: &[crate::analysis::OutputColumn],
    name: &str,
    label: &str,
    output_column_id: crate::column_id::ColumnId,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<crate::optimizer::operator::ChangeEventOutputExpr, crate::compiler::SqlCompileError> {
    Ok(crate::optimizer::operator::ChangeEventOutputExpr {
        output_column_id,
        expr: Some(child_expr(arena, columns, name, label, control)?),
    })
}

fn intern_column(
    arena: &mut crate::optimizer::scalar::ScalarArena,
    column: &crate::analysis::OutputColumn,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<crate::optimizer::scalar::ScalarId, crate::compiler::SqlCompileError> {
    arena.intern_observed(
        crate::optimizer::scalar::ScalarNode::ColumnRef(column.column_id),
        column.value_type.clone(),
        control,
    )
}

fn child_expr(
    arena: &mut crate::optimizer::scalar::ScalarArena,
    columns: &[crate::analysis::OutputColumn],
    name: &str,
    label: &str,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<crate::optimizer::scalar::ScalarId, crate::compiler::SqlCompileError> {
    let column = output_column_by_name(columns, name, label)?;
    intern_column(arena, &column, control)
}

fn merge_action_predicate(
    arena: &mut crate::optimizer::scalar::ScalarArena,
    columns: &[crate::analysis::OutputColumn],
    action: i32,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<crate::optimizer::scalar::ScalarId, crate::compiler::SqlCompileError> {
    let action_expr = child_expr(arena, columns, "__nr_merge_action", "MERGE action", control)?;
    let literal = arena.intern_observed(
        crate::optimizer::scalar::ScalarNode::Literal(crate::optimizer::scalar::HashableLiteral(
            crate::analysis::LiteralValue::Int(i64::from(action)),
        )),
        novarocks_type_contract::FunctionValueType::new(arrow::datatypes::DataType::Int64, false),
        control,
    )?;
    arena.intern_observed(
        crate::optimizer::scalar::ScalarNode::BinaryOp {
            op: crate::common::BinOp::Eq,
            left: action_expr,
            right: literal,
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        },
        novarocks_type_contract::FunctionValueType::new(arrow::datatypes::DataType::Boolean, false),
        control,
    )
}

pub(crate) fn build_change_expand(
    child: crate::optimizer::OptimizedOperatorNode,
    arena: crate::optimizer::scalar::ScalarArena,
    output_columns: Vec<crate::analysis::OutputColumn>,
    effect_column_id: crate::column_id::ColumnId,
    events: Vec<crate::optimizer::operator::ChangeEventSpec>,
) -> Result<crate::optimizer::OptimizedOperatorNode, String> {
    let stats = child.stats.clone();
    let mut root = crate::optimizer::OptimizedOperatorNode {
        op: crate::optimizer::operator::Operator::PhysicalChangeEventExpand(
            crate::optimizer::operator::ChangeEventExpandOp {
                events,
                output_columns: output_columns.clone(),
                effect_column_id,
            },
        ),
        children: vec![child],
        stats,
        explain_stats: crate::optimizer::optimized_tree::OptimizerExplainStats::default(),
        output_columns,
        execution_props: crate::optimizer::optimized_tree::PlanExecutionProps::default(),
    };
    crate::optimizer::optimized_tree::attach_scalar_arena(&mut root, std::sync::Arc::new(arena));
    Ok(root)
}

fn output_column_by_name(
    columns: &[crate::analysis::OutputColumn],
    name: &str,
    label: &str,
) -> Result<crate::analysis::OutputColumn, String> {
    maybe_output_column_by_name(columns, name)?.ok_or_else(|| {
        format!("MOR UPDATE change-stream {label} column `{name}` not found in producer output")
    })
}

fn maybe_output_column_by_name(
    columns: &[crate::analysis::OutputColumn],
    name: &str,
) -> Result<Option<crate::analysis::OutputColumn>, String> {
    let mut matches = columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(name));
    let Some(column) = matches.next() else {
        return Ok(None);
    };
    if matches.next().is_some() {
        return Err(format!(
            "MOR UPDATE change-stream producer column `{name}` is ambiguous"
        ));
    }
    Ok(Some(column.clone()))
}

fn max_physical_column_id(node: &crate::optimizer::OptimizedOperatorNode) -> u32 {
    node.output_columns
        .iter()
        .map(|column| column.column_id.0)
        .chain(node.children.iter().map(max_physical_column_id))
        .max()
        .unwrap_or(0)
}

/// Immutable scan facts used by Frontend's ordinary ANALYZE data plane.
///
/// The relation is named, not synthetic: a collection measures one real table
/// at one exact version, and `version_ordinal` is that version. The binding was
/// admitted by Core from an exact provider lease pinned to the same version, so
/// SQL only turns these facts into a sealed statistics distributed plan.
#[derive(Clone, Debug)]
pub struct StatisticsConnectorScan {
    pub binding: crate::binding::SqlTableBindingId,
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub version_ordinal: i64,
    pub columns: Vec<novarocks_spi::connector::StatisticsScanColumn>,
}

/// The provider-read occurrence one ANALYZE attempt scans.
///
/// A collection reads exactly one relation, so the occurrence that addresses
/// it is a constant rather than something an allocator has to hand out.
pub const STATISTICS_SCAN_OCCURRENCE: novarocks_physical_plan::ProviderReadOccurrenceId =
    novarocks_physical_plan::ProviderReadOccurrenceId::new(0);

/// State what one ANALYZE attempt needs from the provider it measures.
///
/// The projection is the collection's own scan projection in order, and the
/// version is the one the evidence will be stamped with -- the same two facts
/// the plan is built from, so the read that is frozen and the scan that is
/// planned cannot describe different relations.
pub fn statistics_provider_read_need(
    scan: &StatisticsConnectorScan,
) -> Result<crate::compiler::ProviderReadNeed, String> {
    crate::compiler::ProviderReadNeed::for_program(
        STATISTICS_SCAN_OCCURRENCE,
        scan.binding,
        crate::compiler::ProviderReadRelationNeed::Data {
            relation: novarocks_types::naming::TableIdentity::new(
                &scan.catalog,
                &scan.namespace,
                &scan.table,
            ),
            version: crate::compiler::ProviderReadVersionNeed::Snapshot(scan.version_ordinal),
        },
        scan.columns
            .iter()
            .map(|column| (Box::<str>::from(column.name()), column.value_type().clone())),
    )
    .map_err(|error| error.to_string())
}

/// Build the final ANALYZE contract from one exact provider-read occurrence.
///
/// This deliberately constructs the complete physical shape directly. An
/// ANALYZE attempt is not user SQL: the provider says what must be measured,
/// so there is no logical aggregate for the optimizer to discover and no
/// provider-specific sink for the distributed planner to install. The shape
/// is always
///
/// `Scan -> Local Aggregate -> Gather -> Global Aggregate -> Unpivot -> Result`
///
/// and the long-form Root relation is exactly
/// `(input_fields List<Int32>, blob_type Utf8, body Binary,
/// properties Map<Utf8, Utf8>)`. The generic Unpivot constants carry the
/// frozen identity without giving Execution any statistics semantics.
pub fn build_final_statistics_connector_plan(
    scan: StatisticsConnectorScan,
    required: &[novarocks_spi::connector::StatisticsRequiredAggregation],
    functions: &dyn crate::compiler::SqlFunctionCatalog,
    settings: &crate::compiler::SessionOptimizerSettings,
    final_context: DmlFinalPlanContext,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    root_allow_throw_exception: bool,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &crate::compiler::SqlCompileControl,
) -> Result<crate::compiler::SqlAuthoredPhysicalPlan, crate::compiler::SqlCompileError> {
    if required.is_empty() {
        return Err(
            "empty ANALYZE requirements must bypass distributed planning and execution"
                .to_string()
                .into(),
        );
    }
    // This generated source has no preceding analyzed catalog owner. Capture
    // once before authoring its first binding, and retain that same owner.
    control.check()?;
    let functions = functions.snapshot();
    control.check()?;
    let (version, dop_domain, reads, emission_mode) = final_context.into_parts();
    let reads = reads.ok_or_else(|| {
        "ANALYZE final planning requires exactly one finalized provider read".to_string()
    })?;
    let scan_occurrence = reads
        .single_occurrence()
        .map_err(|error| error.to_string())?;
    let mut physical = build_statistics_connector_physical(
        scan,
        required,
        functions.as_ref(),
        scan_occurrence,
        decimal_overflow_policy,
        constant_policy,
        control,
    )?;
    crate::planner::physical::runtime_filter_placement::place_runtime_filters(
        &mut physical,
        settings,
    );
    let draft = crate::planner::distributed::build::lower_final_physical_plan_with_provider_reads(
        &physical,
        version,
        dop_domain,
        reads,
        functions,
        root_allow_throw_exception,
        constant_policy,
        emission_mode,
        control,
    )
    .map_err(final_lowering_error)?;
    draft
        .finish_with_dependency_observer_observed(control)
        .map_err(final_plan_publication_error)
}

fn build_statistics_connector_physical(
    scan: StatisticsConnectorScan,
    required: &[novarocks_spi::connector::StatisticsRequiredAggregation],
    functions: &dyn crate::compiler::SqlFunctionCatalog,
    scan_occurrence: novarocks_physical_plan::ProviderReadOccurrenceId,
    decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<crate::planner::physical::PhysicalPlanNode, crate::compiler::SqlCompileError> {
    if required.is_empty() {
        return Err(
            "empty ANALYZE requirements must bypass distributed planning and execution"
                .to_string()
                .into(),
        );
    }
    let mut factory = crate::column_id::ColumnRefFactory::new();
    let scan_columns = scan
        .columns
        .iter()
        .map(|column| {
            let column_id =
                factory.create(None, column.name().to_string(), column.value_type().clone());
            crate::analysis::OutputColumn {
                column_id,
                name: column.name().to_string(),
                value_type: column.value_type().clone(),

                is_internal: false,
            }
        })
        .collect::<Vec<_>>();
    let scan_index_by_ordinal = scan
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| (column.ordinal(), index))
        .collect::<std::collections::BTreeMap<_, _>>();
    if scan_index_by_ordinal.len() != scan.columns.len() {
        return Err("ANALYZE scan contains duplicate provider column ordinals"
            .to_string()
            .into());
    }
    let physical_scan: crate::planner::physical::PhysicalScanNode =
        crate::planner::payload::PlanScanNode {
            database: scan.namespace.clone(),
            table: crate::planner::table::TableDef {
                name: scan.table.clone(),
                columns: scan
                    .columns
                    .iter()
                    .map(|column| {
                        novarocks_types::schema::ColumnDef::from_value_type(
                            column.name().to_string(),
                            column.value_type().clone(),
                            None,
                        )
                        .map_err(|error| error.to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: crate::planner::table::ScanSource::Sql(
                    crate::planner::table::SqlScanSource::new(
                        scan.binding,
                        crate::planner::table::SqlTableIdentity {
                            catalog: scan.catalog,
                            namespace: scan.namespace,
                            table: scan.table,
                        },
                        // The collection reads the relation as of exactly
                        // the version its evidence is stamped with. Naming
                        // the current version instead would measure rows
                        // the answer does not describe.
                        crate::planner::table::SqlScanKind::Data {
                            version: crate::planner::table::SqlTableVersionSelector::Snapshot(
                                scan.version_ordinal,
                            ),
                        },
                    ),
                ),
            },
            alias: None,
            columns: scan_columns.clone(),
            predicates: Vec::new(),
            required_columns: None,
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }
        .into();
    let physical_scan = physical_scan
        .finalize_provider_read_occurrence(scan_occurrence)
        .map_err(|error| format!("finalize ANALYZE scan occurrence: {error}"))?;
    let scan = crate::planner::physical::PhysicalPlanNode {
        kind: crate::planner::physical::PhysicalPlanKind::Scan(physical_scan),
        children: Vec::new(),
        output_columns: scan_columns,
        stats: crate::planner::physical::PhysicalPlanStats {
            output_row_count: 0.0,
            row_count_confidence: crate::planner::physical::PlannerConfidence::Fallback,
            column_statistics: std::collections::HashMap::new(),
            cost_estimate: None,
            broadcast_decision: None,
        },
        probe_runtime_filters: Vec::new(),
    };
    let mut local_calls = Vec::with_capacity(required.len());
    let mut global_calls = Vec::with_capacity(required.len());
    let mut final_columns = Vec::with_capacity(required.len());
    let mut partial_columns = Vec::with_capacity(required.len());
    // The one body column the Unpivot stacks every aggregate into says what
    // those aggregates say. Stating it independently would make the Root
    // relation claim a nullability no function it reads produces, which is
    // the same column described twice and only accidentally alike.
    let mut body_nullable = false;
    for (index, requirement) in required.iter().enumerate() {
        let input = scan
            .output_columns
            .get(
                *scan_index_by_ordinal
                    .get(&requirement.input().ordinal())
                    .ok_or_else(|| {
                        format!(
                            "ANALYZE aggregate input ordinal {} is absent from its pinned scan",
                            requirement.input().ordinal()
                        )
                    })?,
            )
            .ok_or_else(|| {
                format!(
                    "ANALYZE aggregate input `{}` is absent from its pinned scan",
                    requirement.input().name()
                )
            })?;
        if &input.value_type != requirement.input().value_type() {
            return Err(format!(
                "ANALYZE aggregate input `{}` does not match its pinned scan type",
                requirement.input().name()
            )
            .into());
        }
        let args = vec![crate::analysis::TypedExpr {
            kind: crate::analysis::ExprKind::ColumnRef {
                column_id: input.column_id,
                qualifier: None,
                column: input.name.clone(),
            },
            value_type: input.value_type.clone(),
        }];
        let resolved = crate::functions::resolve_sql_aggregate_binding(
            functions,
            requirement.function_name(),
            &args,
            &[],
            true,
            constant_policy,
            control,
        )
        .map_err(|error| match error {
            novarocks_functions::FunctionBindingError::Control(error) => {
                crate::compiler::SqlCompileError::from(error)
            }
            error => crate::compiler::SqlCompileError::Compilation(format!(
                "resolve trusted ANALYZE aggregate `{}` for {:?}: {error}",
                requirement.function_name(),
                requirement.input().data_type()
            )),
        })?;
        let resolved = crate::binding::SqlFunctionBinding::new(resolved, decimal_overflow_policy);
        let result_type = crate::functions::aggregate_result_type(&resolved);
        if result_type.logical_type != novarocks_type_contract::ValueLogicalType::Physical
            || result_type.data_type != arrow::datatypes::DataType::Binary
        {
            return Err(
                "ANALYZE aggregate must produce an exact Physical Binary artifact body".into(),
            );
        }
        let partial_output_id = factory.create(
            None,
            format!("analyze_state_{index}"),
            crate::functions::aggregate_selection(&resolved)
                .intermediate_type
                .clone(),
        );
        let result_nullable = crate::functions::aggregate_result_type(&resolved).nullable;
        let final_output_id = factory.create(
            None,
            format!("analyze_body_{index}"),
            crate::functions::aggregate_result_type(&resolved).clone(),
        );
        body_nullable |= result_nullable;
        let output_name = format!("analyze_body_{index}");
        final_columns.push(crate::analysis::OutputColumn {
            column_id: final_output_id,
            name: output_name.clone(),
            value_type: crate::functions::aggregate_result_type(&resolved).clone(),

            is_internal: true,
        });
        partial_columns.push(crate::analysis::OutputColumn {
            column_id: partial_output_id,
            name: format!("analyze_state_{index}"),
            value_type: crate::functions::aggregate_selection(&resolved)
                .intermediate_type
                .clone(),

            is_internal: true,
        });
        let result_type = crate::functions::aggregate_result_type(&resolved)
            .data_type
            .clone();
        // Both phases retain the original logical update source. The merge
        // lowerer selects the materialized state channel independently.
        let source =
            crate::binding::AggregateArgumentSource::logical_update(args, Vec::new(), resolved);
        local_calls.push(crate::planner::payload::AggregateCall {
            name: requirement.function_name().to_string(),
            distinct: false,
            result_type: result_type.clone(),
            source: source.clone(),
            output_column_id: partial_output_id,
        });
        global_calls.push(crate::planner::payload::AggregateCall {
            name: requirement.function_name().to_string(),
            distinct: false,
            result_type,
            source,
            output_column_id: final_output_id,
        });
    }

    let stats = crate::planner::physical::PhysicalPlanStats {
        output_row_count: 1.0,
        row_count_confidence: crate::planner::physical::PlannerConfidence::Exact,
        column_statistics: std::collections::HashMap::new(),
        cost_estimate: None,
        broadcast_decision: None,
    };
    let local = crate::planner::physical::PhysicalPlanNode {
        kind: crate::planner::physical::PhysicalPlanKind::HashAggregate(Box::new(
            crate::planner::physical::PhysicalHashAggregateNode {
                mode: crate::planner::physical::AggMode::Local,
                group_by: Vec::new(),
                aggregates: local_calls,
                is_merge: vec![false; required.len()],
                output_layout: crate::planner::physical::AggregateOutputLayout::new(
                    Vec::new(),
                    partial_columns.clone(),
                ),
                output_columns: partial_columns.clone(),
                topn_runtime_filter_builds: Vec::new(),
            },
        )),
        children: vec![scan],
        output_columns: partial_columns.clone(),
        stats: stats.clone(),
        probe_runtime_filters: Vec::new(),
    };
    let gather = crate::planner::physical::PhysicalPlanNode {
        kind: crate::planner::physical::PhysicalPlanKind::Redistribute(
            crate::planner::physical::RedistributeNode {
                mode: crate::planner::physical::RedistributeMode::Gather,
                partition_exprs: Vec::new(),
                output_columns: partial_columns.clone(),
            },
        ),
        children: vec![local],
        output_columns: partial_columns,
        stats: stats.clone(),
        probe_runtime_filters: Vec::new(),
    };
    let global = crate::planner::physical::PhysicalPlanNode {
        kind: crate::planner::physical::PhysicalPlanKind::HashAggregate(Box::new(
            crate::planner::physical::PhysicalHashAggregateNode {
                mode: crate::planner::physical::AggMode::Global,
                group_by: Vec::new(),
                aggregates: global_calls,
                is_merge: vec![true; required.len()],
                output_layout: crate::planner::physical::AggregateOutputLayout::new(
                    Vec::new(),
                    final_columns.clone(),
                ),
                output_columns: final_columns.clone(),
                topn_runtime_filter_builds: Vec::new(),
            },
        )),
        children: vec![gather],
        output_columns: final_columns.clone(),
        stats: stats.clone(),
        probe_runtime_filters: Vec::new(),
    };

    let input_fields_type = crate::analysis::UnpivotConstant::Int32List(Vec::new()).data_type();
    let properties_type = crate::analysis::UnpivotConstant::Utf8Map(Vec::new()).data_type();
    let input_fields = factory.create(
        None,
        "input_fields".to_string(),
        novarocks_type_contract::FunctionValueType::new(input_fields_type.clone(), false),
    );
    let blob_type = factory.create(
        None,
        "blob_type".to_string(),
        novarocks_type_contract::FunctionValueType::new(arrow::datatypes::DataType::Utf8, false),
    );
    let body = factory.create(
        None,
        "body".to_string(),
        novarocks_type_contract::FunctionValueType::new(
            arrow::datatypes::DataType::Binary,
            body_nullable,
        ),
    );
    let properties = factory.create(
        None,
        "properties".to_string(),
        novarocks_type_contract::FunctionValueType::new(properties_type.clone(), false),
    );
    let root_columns = vec![
        crate::analysis::OutputColumn {
            column_id: input_fields,
            name: "input_fields".to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(input_fields_type, false),

            is_internal: true,
        },
        crate::analysis::OutputColumn {
            column_id: blob_type,
            name: "blob_type".to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Utf8,
                false,
            ),

            is_internal: true,
        },
        crate::analysis::OutputColumn {
            column_id: body,
            name: "body".to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(
                arrow::datatypes::DataType::Binary,
                body_nullable,
            ),

            is_internal: true,
        },
        crate::analysis::OutputColumn {
            column_id: properties,
            name: "properties".to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(properties_type, false),

            is_internal: true,
        },
    ];
    let value_mappings = required
        .iter()
        .zip(final_columns.iter())
        .map(
            |(requirement, aggregate)| crate::planner::payload::PlanUnpivotValueMapping {
                input_value_column_id: aggregate.column_id,
                constants: vec![
                    crate::analysis::UnpivotConstant::Int32List(
                        requirement.artifact().input_fields().to_vec(),
                    ),
                    crate::analysis::UnpivotConstant::Scalar(crate::analysis::TypedExpr {
                        kind: crate::analysis::ExprKind::Literal(
                            crate::analysis::LiteralValue::String(
                                requirement.artifact().blob_type().to_string(),
                            ),
                        ),
                        value_type: novarocks_type_contract::FunctionValueType::new(
                            arrow::datatypes::DataType::Utf8,
                            false,
                        ),
                    }),
                    crate::analysis::UnpivotConstant::Utf8Map(Vec::new()),
                ],
            },
        )
        .collect::<Vec<_>>();
    // Emit the domain's frozen four-column order directly. A reorder Project
    // would put the allocation-producing Unpivot before the root's last edge.
    let unpivot = crate::planner::payload::PlanUnpivotNode::try_new(
        &final_columns,
        Vec::new(),
        body,
        vec![input_fields, blob_type, properties],
        value_mappings,
        root_columns.clone(),
        4096,
        novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES,
        constant_policy,
        control,
    )?;
    let physical = crate::planner::physical::PhysicalPlanNode {
        kind: crate::planner::physical::PhysicalPlanKind::Unpivot(unpivot),
        children: vec![global],
        output_columns: root_columns,
        stats,
        probe_runtime_filters: Vec::new(),
    };
    Ok(physical)
}

#[cfg(test)]
pub(crate) fn statistics_final_context_for_test() -> DmlFinalPlanContext {
    tests::statistics_final_context()
}

#[cfg(test)]
mod tests {
    use super::{
        DmlChangeStreamRoute, DmlChangeStreamStatisticsTarget, DmlStatisticsSnapshot,
        DmlWritePlanInput, dml_change_stream_optimizer_settings, evidence_to_base_statistics,
        optimizer_settings_stable_digest_material, plan_change_stream_writer_statistics,
    };
    use crate::compiler::SessionOptimizerSettings;
    use crate::optimizer::statistics::Confidence;
    use crate::optimizer::stats_input::StatsSource;
    use novarocks_spi::connector::{
        StatisticsBasisRelation, StatisticsDataVersion, StatisticsEvidence,
        StatisticsEvidenceRevision, StatisticsMetric, StatisticsMetricObservation,
        StatisticsMetricSource, StatisticsMetricState, StatisticsMetricValue,
        StatisticsNumericNature, StatisticsRowCoverage,
    };

    fn mor_match_producer() -> crate::optimizer::OptimizedOperatorNode {
        use crate::optimizer::operator::{Operator, ValuesOp};
        use arrow::datatypes::DataType;
        let output_columns = [
            ("__nr_file", DataType::Utf8),
            ("__nr_pos", DataType::Int64),
            ("__nr_row_id", DataType::Int64),
            ("__nr_merge_assert_key", DataType::Int64),
            ("__nr_merge_action", DataType::Int64),
            ("id", DataType::Int64),
            ("__nr_new_id", DataType::Int64),
            ("__nr_ins_id", DataType::Int64),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, (name, data_type))| crate::analysis::OutputColumn {
            column_id: crate::column_id::ColumnId(index as u32 + 1),
            name: name.into(),
            value_type: novarocks_type_contract::FunctionValueType::new(data_type, true),
            is_internal: true,
        })
        .collect::<Vec<_>>();
        crate::optimizer::OptimizedOperatorNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: output_columns.clone(),
            }),
            children: vec![],
            stats: Default::default(),
            explain_stats: Default::default(),
            output_columns,
            execution_props: crate::optimizer::optimized_tree::PlanExecutionProps {
                scalar_arena: Some(std::sync::Arc::new(
                    crate::optimizer::scalar::ScalarArena::new(),
                )),
                ..Default::default()
            },
        }
    }

    fn assert_replace_inherits_sequence_and_preserves_row_id(
        producer: &crate::optimizer::OptimizedOperatorNode,
    ) {
        use crate::optimizer::operator::Operator;
        use novarocks_spi::connector::ConnectorRowMutationEffect;
        let Operator::PhysicalChangeEventExpand(expand) = &producer.op else {
            panic!("expected change-event expand");
        };
        let sequence = producer
            .output_columns
            .iter()
            .find(|output| output.name == super::ICEBERG_LAST_UPDATED_SEQUENCE_COLUMN)
            .expect("sequence output");
        assert!(sequence.value_type.nullable);
        let row_id = producer
            .output_columns
            .iter()
            .find(|output| output.name == super::ICEBERG_ROW_ID_COLUMN)
            .expect("row ID output");
        let replaced = expand
            .events
            .iter()
            .find(|event| event.effect == ConnectorRowMutationEffect::Replace)
            .expect("Replace event");
        assert!(
            !replaced
                .assignments
                .iter()
                .any(|assignment| assignment.output_column_id == sequence.column_id),
            "Replace must inherit its sequence rather than stamp an admission-time value"
        );
        let row_id_expr = replaced
            .assignments
            .iter()
            .find(|assignment| assignment.output_column_id == row_id.column_id)
            .and_then(|assignment| assignment.expr)
            .expect("Replace must retain row ID");
        assert!(
            matches!(producer.execution_props.scalar_arena.as_ref().unwrap().node(row_id_expr),
            crate::optimizer::scalar::ScalarNode::ColumnRef(id) if *id == crate::column_id::ColumnId(3))
        );
    }

    #[test]
    fn mor_update_replace_inherits_actual_commit_sequence() {
        let columns = [novarocks_types::schema::ColumnDef {
            name: "id".into(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        }];
        let producer = super::build_update_change_event_expand(
            mor_match_producer(),
            &columns,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        assert_replace_inherits_sequence_and_preserves_row_id(&producer);
    }

    #[test]
    fn mor_merge_replace_inherits_actual_commit_sequence() {
        let columns = [novarocks_types::schema::ColumnDef {
            name: "id".into(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        }];
        let producer = super::build_merge_change_event_expand(
            mor_match_producer(),
            &columns,
            true,
            true,
            true,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        assert_replace_inherits_sequence_and_preserves_row_id(&producer);
        let crate::optimizer::operator::Operator::PhysicalChangeEventExpand(expand) = &producer.op
        else {
            unreachable!()
        };
        assert_eq!(expand.events.len(), 3);
    }

    #[test]
    fn observed_plan_publication_preserves_control_and_ordinary_diagnostics() {
        use novarocks_physical_plan::{ConstantReferenceError, PlanConstructionError};
        use novarocks_type_contract::CompileControlError;
        for (cause, expected) in [
            (
                CompileControlError::Cancelled,
                crate::compiler::SqlCompileError::Cancelled,
            ),
            (
                CompileControlError::DeadlineExceeded,
                crate::compiler::SqlCompileError::DeadlineExceeded,
            ),
            (
                CompileControlError::ResourceExhausted,
                crate::compiler::SqlCompileError::ResourceExhausted,
            ),
        ] {
            assert_eq!(
                super::final_plan_construction_error(PlanConstructionError::Constants(
                    ConstantReferenceError::Control(cause),
                )),
                expected,
            );
        }
        let ordinary = PlanConstructionError::Constants(ConstantReferenceError::InvalidConsumer(
            "cancelled is only diagnostic text",
        ));
        let expected = ordinary.to_string();
        assert_eq!(
            super::final_plan_construction_error(ordinary),
            crate::compiler::SqlCompileError::Compilation(expected),
        );
    }

    fn statistics_scan_with_type(
        value_type: novarocks_type_contract::FunctionValueType,
    ) -> super::StatisticsConnectorScan {
        super::StatisticsConnectorScan {
            binding: crate::binding::SqlTableBindingId::new_for_test(1),
            catalog: "iceberg".into(),
            namespace: "db".into(),
            table: "t".into(),
            version_ordinal: 42,
            columns: vec![
                novarocks_spi::connector::StatisticsScanColumn::try_new(0, "u", value_type)
                    .unwrap(),
            ],
        }
    }

    #[test]
    fn statistics_provider_need_preserves_uuid_and_physical_fixed16_identity() {
        use novarocks_spi::connector::read_stack::value::ConnectorValueType;
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
        for (value_type, connector_type) in [
            (
                FunctionValueType::new(arrow::datatypes::DataType::FixedSizeBinary(16), true),
                ConnectorValueType::Fixed { length: 16 },
            ),
            (
                FunctionValueType::try_with_logical_type(
                    arrow::datatypes::DataType::FixedSizeBinary(16),
                    true,
                    ValueLogicalType::Uuid,
                )
                .unwrap(),
                ConnectorValueType::Uuid,
            ),
        ] {
            let scan = statistics_scan_with_type(value_type.clone());
            let need = super::statistics_provider_read_need(&scan).unwrap();
            assert_eq!(need.columns()[0].engine_type(), &value_type);
            assert_eq!(need.columns()[0].connector_type(), connector_type);
        }
    }

    #[test]
    fn statistics_pinned_scan_rejects_equal_carrier_with_different_root_domain() {
        use novarocks_spi::connector::{
            StatisticsArtifactIdentity, StatisticsRequiredAggregation, StatisticsScanColumn,
        };
        use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
        let source = FunctionValueType::try_with_logical_type(
            arrow::datatypes::DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .unwrap();
        let scan = statistics_scan_with_type(source);
        let requirement = StatisticsRequiredAggregation::try_new(
            StatisticsScanColumn::try_new(
                0,
                "u",
                FunctionValueType::new(arrow::datatypes::DataType::FixedSizeBinary(16), true),
            )
            .unwrap(),
            "$test",
            StatisticsArtifactIdentity::try_new(vec![7], "test/blob").unwrap(),
        )
        .unwrap();
        let functions = crate::functions::build_builtin_engine_function_catalog().unwrap();
        let error = super::build_statistics_connector_physical(
            scan,
            &[requirement],
            &functions,
            novarocks_physical_plan::ProviderReadOccurrenceId::new(37),
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not match its pinned scan type"),
            "{error}"
        );
    }

    pub(super) fn statistics_final_context() -> super::DmlFinalPlanContext {
        use novarocks_physical_plan::{
            ExactInputVersion, PipelineDopDomain, PlanVersionId, ProviderColumnReference,
            ProviderReadReference, ScanReadBudget, ValueType,
        };
        use novarocks_spi::connector::read_stack::value::ConnectorValueType;
        use novarocks_spi::connector::read_stack::{ConnectorReadBinding, ConnectorReadWorkSource};
        use novarocks_spi::connector::{
            CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
            ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceDescriptor,
            ConnectorInstanceId, ConnectorProviderId, ConnectorReadRelationPayload,
        };

        let sql_binding = crate::binding::SqlTableBindingId::new_for_test(1);
        let need = crate::compiler::ProviderReadNeed::exact_projection_for_test(
            sql_binding,
            crate::compiler::ProviderReadRelationNeed::Data {
                relation: novarocks_types::naming::TableIdentity::new("iceberg", "db", "t"),
                version: crate::compiler::ProviderReadVersionNeed::Snapshot(42),
            },
            Box::from([crate::compiler::ProviderReadColumnNeed::for_test(
                0,
                "id",
                ValueType {
                    logical_type: novarocks_type_contract::ValueLogicalType::Physical,
                    data_type: arrow::datatypes::DataType::Int64,
                    nullable: true,
                },
                ConnectorValueType::BigInt,
            )]),
        );
        let provider_id = ConnectorProviderId::parse("iceberg").expect("provider id");
        let instance_id = ConnectorInstanceId::parse("statistics").expect("instance id");
        let binding = ConnectorReadBinding::new(
            ConnectorInstanceDescriptor {
                provider_id,
                instance_id: instance_id.clone(),
            },
            CatalogHandle::new(instance_id, CatalogVersion::from_bytes([3; 32])),
        );
        let encoded = |category| {
            ConnectorEncodedPayload::new(
                ConnectorEnvelopeHeader::new(
                    binding.descriptor().provider_id.clone(),
                    binding.catalog_handle().clone(),
                    category,
                    ConnectorCodecRevision::try_new(1).expect("codec revision"),
                ),
                vec![category as u8 + 1].into(),
            )
        };
        let contract = crate::compiler::ProviderReadStaticContract {
            sql_binding,
            request: crate::compiler::ProviderReadRequestBinding::from_need(&need),
            read: ProviderReadReference {
                binding: binding.clone(),
                input_version: ExactInputVersion::try_new([9]).expect("input version"),
                relation: ConnectorReadRelationPayload::new(
                    need.relation().relation_kind(),
                    encoded(ConnectorCodecCategory::ReadTable),
                    encoded(ConnectorCodecCategory::ReadView),
                ),
            },
            work_source: ConnectorReadWorkSource::RuntimeSplits,
            selection_digest: [8; 32],
            schema: Box::from([crate::compiler::ProviderReadColumnFact::new(
                0,
                ProviderColumnReference {
                    column_payload: encoded(ConnectorCodecCategory::ReadColumn),
                },
                need.columns()[0].engine_type().clone(),
            )]),
            predicates: Box::default(),
            limit: crate::compiler::ProviderReadLimitFact::NotRequested,
            provided_properties: crate::compiler::ProviderReadProperties::unconstrained(),
            coverage_evidence: Box::default(),
        };
        super::DmlFinalPlanContext::new(
            PlanVersionId::try_new([31; 16]).expect("plan version"),
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            super::DmlFinalizedProviderReadSet(
                crate::compiler::FinalizedProviderReadSet::single_for_test(
                    sql_binding,
                    novarocks_physical_plan::ProviderReadOccurrenceId::new(37),
                    contract,
                    ScanReadBudget {
                        max_batch_rows: 1024,
                        max_batch_bytes: 1 << 20,
                    },
                ),
            ),
            crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
        )
    }

    #[test]
    fn analyze_statistics_uses_ordinary_two_phase_aggregate_unpivot_and_result_sink() {
        use novarocks_functions::{AggregateOverloadMetadata, FunctionVisibility};
        use novarocks_physical_plan::{AggregatePhase, FragmentSink, NodeKind};
        use novarocks_spi::connector::{
            StatisticsArtifactIdentity, StatisticsRequiredAggregation, StatisticsScanColumn,
        };

        let functions = crate::functions::test_exact_aggregate_catalog(
            "$test_blob_aggregate",
            FunctionVisibility::Hidden,
            [AggregateOverloadMetadata::try_new(
                "test/blob-aggregate/i64/v1",
                [arrow::datatypes::DataType::Int64],
                arrow::datatypes::DataType::Binary,
                arrow::datatypes::DataType::Binary,
                "test/blob-state/v1",
            )
            .expect("aggregate overload")],
        );
        let requirement = StatisticsRequiredAggregation::try_new(
            StatisticsScanColumn::try_new(
                0,
                "id",
                novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int64,
                    true,
                ),
            )
            .expect("scan column"),
            "$test_blob_aggregate",
            StatisticsArtifactIdentity::try_new(vec![7], "test-blob-v1")
                .expect("artifact identity"),
        )
        .expect("requirement");

        let scan_source = super::StatisticsConnectorScan {
            binding: crate::binding::SqlTableBindingId::new_for_test(1),
            catalog: "iceberg".into(),
            namespace: "db".into(),
            table: "t".into(),
            version_ordinal: 42,
            columns: vec![
                StatisticsScanColumn::try_new(
                    0,
                    "id",
                    novarocks_type_contract::FunctionValueType::new(
                        arrow::datatypes::DataType::Int64,
                        true,
                    ),
                )
                .expect("scan column"),
            ],
        };
        let physical = super::build_statistics_connector_physical(
            scan_source.clone(),
            std::slice::from_ref(&requirement),
            &functions,
            novarocks_physical_plan::ProviderReadOccurrenceId::new(37),
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .expect("ANALYZE physical source");
        // Unpivot -> Global -> Gather -> Local -> Scan is the production
        // two-phase tree. The materializer must remain the direct root.
        assert!(matches!(
            &physical.kind,
            crate::planner::physical::PhysicalPlanKind::Unpivot(_)
        ));
        assert_eq!(physical.children.len(), 1);
        let global_node = &physical.children[0];
        assert_eq!(global_node.children.len(), 1);
        let gather_node = &global_node.children[0];
        let crate::planner::physical::PhysicalPlanKind::Redistribute(gather) = &gather_node.kind
        else {
            panic!("expected Gather between aggregate phases");
        };
        assert_eq!(
            gather.mode,
            crate::planner::physical::RedistributeMode::Gather
        );
        assert_eq!(gather_node.children.len(), 1);
        let local_node = &gather_node.children[0];
        assert_eq!(local_node.children.len(), 1);
        assert!(matches!(
            &local_node.children[0].kind,
            crate::planner::physical::PhysicalPlanKind::Scan(_)
        ));
        let crate::planner::physical::PhysicalPlanKind::HashAggregate(global_source) =
            &global_node.kind
        else {
            panic!("expected Global aggregate");
        };
        let crate::planner::physical::PhysicalPlanKind::HashAggregate(local_source) =
            &local_node.kind
        else {
            panic!("expected Local aggregate");
        };
        assert_eq!(
            global_source.mode,
            crate::planner::physical::AggMode::Global
        );
        assert_eq!(local_source.mode, crate::planner::physical::AggMode::Local);
        assert_eq!(global_source.is_merge, [true]);
        assert_eq!(local_source.is_merge, [false]);
        let local_call = &local_source.aggregates[0];
        let global_call = &global_source.aggregates[0];
        let (local_args, local_order, local_binding) = local_call
            .source
            .logical_parts()
            .expect("Local authors a certified logical update");
        let (global_args, global_order, global_binding) = global_call
            .source
            .logical_parts()
            .expect("Global transfers the certified update source");
        assert!(std::ptr::eq(
            local_binding.resolved(),
            global_binding.resolved()
        ));
        assert_eq!(
            local_binding.decimal_overflow_policy(),
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull
        );
        assert!(local_order.is_empty() && global_order.is_empty());
        assert_eq!(local_args.len(), 1);
        assert_eq!(global_args.len(), 1);
        let scan_input = &local_node.children[0].output_columns[0];
        for argument in [&local_args[0], &global_args[0]] {
            assert_eq!(argument.value_type, scan_input.value_type);
            assert_eq!(
                argument.value_type,
                requirement.input().value_type().clone()
            );
            let crate::analysis::ExprKind::ColumnRef {
                column_id, column, ..
            } = &argument.kind
            else {
                panic!("logical update must read the original scan column");
            };
            assert_eq!(*column_id, scan_input.column_id);
            assert_eq!(column, "id");
            assert_ne!(*column_id, local_call.output_column_id);
        }
        assert_eq!(
            local_node.output_columns[0].value_type.data_type,
            arrow::datatypes::DataType::Binary
        );
        assert_eq!(global_call.result_type, arrow::datatypes::DataType::Binary);

        let authored = super::build_final_statistics_connector_plan(
            scan_source,
            std::slice::from_ref(&requirement),
            &functions,
            &SessionOptimizerSettings::default(),
            statistics_final_context(),
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            false,
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .expect("ANALYZE plan");
        let plan = authored.plan();

        assert_eq!(plan.fragments().len(), 2);
        assert!(matches!(
            plan.fragments()
                .get(&plan.result_port().expect("result port").fragment)
                .expect("result fragment")
                .sink(),
            FragmentSink::Result
        ));
        let mut scan = 0;
        let mut local = 0;
        let mut exchange = 0;
        let mut global = 0;
        let mut unpivot = 0;
        let result_fragment = plan
            .fragments()
            .get(&plan.result_port().expect("result port").fragment)
            .expect("result fragment");
        assert!(
            matches!(
                result_fragment
                    .nodes()
                    .get(&result_fragment.root())
                    .expect("result root")
                    .kind,
                NodeKind::Unpivot { .. }
            ),
            "the materializer must be the direct root upstream"
        );
        for fragment in plan.fragments().values() {
            for node in fragment.nodes().values() {
                if let NodeKind::Aggregate { calls, .. } = &node.kind {
                    let control = crate::compiler::SqlCompileControl::unbounded();
                    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
                        &control,
                        novarocks_type_contract::CompilePhase::Validate,
                    )
                    .unwrap();
                    for (ordinal, call) in calls.iter().enumerate() {
                        let source = authored
                            .checked_aggregate_source_observed(
                                fragment,
                                node,
                                novarocks_physical_plan::PhysicalCallSite::Aggregate {
                                    node: node.id,
                                    call: u32::try_from(ordinal).unwrap(),
                                },
                                call,
                                &mut work,
                            )
                            .expect("ANALYZE retains its exact logical source journal");
                        assert_eq!(source.phase(), call.binding.phase);
                        let request = source.captured().request();
                        assert_eq!(request.logical_argument_count, 1);
                        let novarocks_functions::FunctionArgument::Value {
                            value_type,
                            constant,
                        } = &request.arguments[0]
                        else {
                            panic!("ANALYZE logical input is its original scan value");
                        };
                        assert_eq!(value_type, requirement.input().value_type());
                        assert!(constant.is_none());
                    }
                    work.finish().unwrap();
                }
                match &node.kind {
                    NodeKind::Scan { occurrence, .. } => {
                        scan += 1;
                        assert_eq!(
                            *occurrence,
                            novarocks_physical_plan::ProviderReadOccurrenceId::new(37),
                            "ANALYZE must preserve the occurrence frozen by its provider fact"
                        );
                    }
                    NodeKind::Aggregate { calls, .. }
                        if calls.iter().all(|call| {
                            matches!(call.binding.phase, AggregatePhase::Partial { .. })
                        }) =>
                    {
                        local += 1;
                        assert_eq!(calls[0].arguments.len(), 1);
                        assert_eq!(
                            fragment
                                .expressions()
                                .get(calls[0].arguments[0])
                                .unwrap()
                                .ty,
                            requirement.input().value_type().clone(),
                            "Partial runtime channel reads the original logical input"
                        );
                    }
                    NodeKind::Aggregate { calls, .. }
                        if calls.iter().all(|call| {
                            matches!(call.binding.phase, AggregatePhase::Final { .. })
                        }) =>
                    {
                        global += 1;
                        assert_eq!(calls[0].arguments.len(), 1);
                        assert_eq!(
                            fragment
                                .expressions()
                                .get(calls[0].arguments[0])
                                .unwrap()
                                .ty,
                            calls[0].binding.intermediate_type,
                            "Final runtime channel reads the actual materialized state"
                        );
                        assert_eq!(
                            calls[0].binding.function.argument_types[0],
                            novarocks_type_contract::FunctionArgumentType::Value(
                                requirement.input().value_type().clone()
                            ),
                            "Final still carries the original logical signature"
                        );
                    }
                    NodeKind::ExchangeSource { .. } => exchange += 1,
                    NodeKind::Unpivot { spec } => {
                        unpivot += 1;
                        assert_eq!(
                            spec.max_output_bytes,
                            novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES
                                as u64
                        );
                    }
                    _ => {}
                }
            }
        }
        assert_eq!((scan, local, exchange, global, unpivot), (1, 1, 1, 1, 1));
        assert_eq!(
            plan.result_port()
                .expect("result port")
                .fields
                .iter()
                .map(|field| {
                    (
                        field.name.as_ref(),
                        field.ty.data_type.clone(),
                        field.ty.nullable,
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                (
                    "input_fields",
                    crate::analysis::UnpivotConstant::Int32List(Vec::new()).data_type(),
                    false,
                ),
                ("blob_type", arrow::datatypes::DataType::Utf8, false),
                ("body", arrow::datatypes::DataType::Binary, true),
                (
                    "properties",
                    crate::analysis::UnpivotConstant::Utf8Map(Vec::new()).data_type(),
                    false,
                ),
            ]
        );
    }

    fn version(token: &'static [u8]) -> StatisticsDataVersion {
        StatisticsDataVersion::try_new(bytes::Bytes::from_static(token)).expect("data version")
    }

    fn observed(
        value: StatisticsMetricValue,
        basis: StatisticsDataVersion,
        nature: StatisticsNumericNature,
        relation: StatisticsBasisRelation,
    ) -> StatisticsMetricState {
        StatisticsMetricState::Available(StatisticsMetricObservation::new(
            value,
            basis,
            StatisticsMetricSource::CurrentManifest,
            nature,
            relation,
        ))
    }

    fn column(name: &str) -> novarocks_types::schema::ColumnDef {
        novarocks_types::schema::ColumnDef {
            name: name.to_string(),
            data_type: arrow::datatypes::DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        }
    }

    /// One answer can mix an exact count, a directional bound, and a value
    /// measured on an older basis. None of them may change how the others are
    /// admitted or labelled, and the whole answer must survive.
    #[test]
    fn a_mixed_answer_is_admitted_per_metric_without_cross_contamination() {
        let queried = version(b"data-v1");
        let evidence = StatisticsEvidence::try_new(
            queried.clone(),
            StatisticsEvidenceRevision::try_new(bytes::Bytes::from_static(b"rev-1"))
                .expect("revision"),
            StatisticsRowCoverage::AllVisibleRows,
            std::collections::BTreeMap::from([
                (
                    StatisticsMetric::RowCount,
                    observed(
                        StatisticsMetricValue::U64(100),
                        queried.clone(),
                        StatisticsNumericNature::Exact,
                        StatisticsBasisRelation::Identical,
                    ),
                ),
                (
                    StatisticsMetric::Maximum {
                        column: std::sync::Arc::from("k"),
                    },
                    observed(
                        StatisticsMetricValue::F64(9.0),
                        queried.clone(),
                        StatisticsNumericNature::UpperBound,
                        StatisticsBasisRelation::Identical,
                    ),
                ),
                (
                    StatisticsMetric::Minimum {
                        column: std::sync::Arc::from("k"),
                    },
                    observed(
                        StatisticsMetricValue::F64(1.0),
                        version(b"data-v0"),
                        StatisticsNumericNature::Exact,
                        StatisticsBasisRelation::BasisIsSuperset,
                    ),
                ),
            ]),
        )
        .expect("evidence");

        let statistics = evidence_to_base_statistics(&evidence, &[column("k")]);

        // The exact current count keeps full confidence...
        assert_eq!(statistics.row_count.known_value(), Some(&100));
        assert_eq!(statistics.row_count.confidence(), Confidence::Exact);
        assert_eq!(statistics.source, StatsSource::IcebergManifest);

        let k = statistics.columns.get("k").expect("column statistics");
        // ...the bound beside it is admitted but not called exact...
        assert_eq!(k.max_value.known_value(), Some(&9.0));
        assert_eq!(k.max_value.confidence(), Confidence::Estimated);
        // ...and the one measured on another basis is skipped, without
        // taking the rest of the answer down with it.
        assert_eq!(k.min_value.known_value(), None);
    }

    /// A distinct count is the one statistic Puffin actually owns, and it is
    /// always approximate. Withholding it left the optimizer with no NDV at all
    /// for lake tables; `Estimated` is the honest way to hand it over.
    #[test]
    fn an_approximate_distinct_count_reaches_the_optimizer_as_estimated() {
        let queried = version(b"data-v1");
        let evidence = StatisticsEvidence::try_new(
            queried.clone(),
            StatisticsEvidenceRevision::try_new(bytes::Bytes::from_static(b"rev-1"))
                .expect("revision"),
            StatisticsRowCoverage::AllVisibleRows,
            std::collections::BTreeMap::from([(
                StatisticsMetric::ThetaNdv {
                    column: std::sync::Arc::from("k"),
                },
                observed(
                    StatisticsMetricValue::F64(42.0),
                    queried,
                    StatisticsNumericNature::TwoSidedApproximate,
                    StatisticsBasisRelation::Identical,
                ),
            )]),
        )
        .expect("evidence");

        let statistics = evidence_to_base_statistics(&evidence, &[column("k")]);
        let k = statistics.columns.get("k").expect("column statistics");
        assert_eq!(k.ndv.known_value(), Some(&42.0));
        assert_eq!(k.ndv.confidence(), Confidence::Estimated);
    }

    /// A sketch measured on rows the query will not read is not evidence about
    /// this query, however recent it is.
    #[test]
    fn a_distinct_count_measured_on_other_rows_is_not_admitted() {
        let queried = version(b"data-v1");
        let evidence = StatisticsEvidence::try_new(
            queried,
            StatisticsEvidenceRevision::try_new(bytes::Bytes::from_static(b"rev-1"))
                .expect("revision"),
            StatisticsRowCoverage::AllVisibleRows,
            std::collections::BTreeMap::from([(
                StatisticsMetric::ThetaNdv {
                    column: std::sync::Arc::from("k"),
                },
                observed(
                    StatisticsMetricValue::F64(42.0),
                    version(b"data-v0"),
                    StatisticsNumericNature::TwoSidedApproximate,
                    StatisticsBasisRelation::BasisIsSubset,
                ),
            )]),
        )
        .expect("evidence");

        let statistics = evidence_to_base_statistics(&evidence, &[column("k")]);
        let k = statistics.columns.get("k").expect("column statistics");
        assert_eq!(k.ndv.known_value(), None);
    }

    /// A compaction changes the snapshot without changing the rows, so its
    /// statistics still describe what the query will read.
    #[test]
    fn a_distinct_count_from_a_rewrite_only_ancestor_is_admitted() {
        let queried = version(b"data-v1");
        let evidence = StatisticsEvidence::try_new(
            queried,
            StatisticsEvidenceRevision::try_new(bytes::Bytes::from_static(b"rev-1"))
                .expect("revision"),
            StatisticsRowCoverage::AllVisibleRows,
            std::collections::BTreeMap::from([(
                StatisticsMetric::ThetaNdv {
                    column: std::sync::Arc::from("k"),
                },
                observed(
                    StatisticsMetricValue::F64(42.0),
                    version(b"data-v0"),
                    StatisticsNumericNature::TwoSidedApproximate,
                    StatisticsBasisRelation::Identical,
                ),
            )]),
        )
        .expect("evidence");

        let statistics = evidence_to_base_statistics(&evidence, &[column("k")]);
        let k = statistics.columns.get("k").expect("column statistics");
        assert_eq!(k.ndv.known_value(), Some(&42.0));
        assert_eq!(k.ndv.confidence(), Confidence::Estimated);
    }

    /// The old whole-evidence gate dropped every metric as soon as delete files
    /// made one of them inexact. Bounds must now reach the optimizer.
    #[test]
    fn an_inexact_answer_is_no_longer_discarded_wholesale() {
        let queried = version(b"data-v1");
        let evidence = StatisticsEvidence::try_new(
            queried.clone(),
            StatisticsEvidenceRevision::try_new(bytes::Bytes::from_static(b"rev-1"))
                .expect("revision"),
            StatisticsRowCoverage::AllVisibleRows,
            std::collections::BTreeMap::from([(
                StatisticsMetric::RowCount,
                observed(
                    StatisticsMetricValue::U64(100),
                    queried,
                    StatisticsNumericNature::UpperBound,
                    StatisticsBasisRelation::Identical,
                ),
            )]),
        )
        .expect("evidence");

        let statistics = evidence_to_base_statistics(&evidence, &[]);
        assert_eq!(statistics.row_count.known_value(), Some(&100));
        assert_eq!(statistics.row_count.confidence(), Confidence::Estimated);
    }

    #[test]
    fn integer_bounds_are_not_silently_rounded_at_the_optimizer_boundary() {
        let queried = version(b"data-v1");
        let unsafe_integer = (1_i64 << 53) + 1;
        let evidence = StatisticsEvidence::try_new(
            queried.clone(),
            StatisticsEvidenceRevision::try_new(bytes::Bytes::from_static(b"rev-1"))
                .expect("revision"),
            StatisticsRowCoverage::AllVisibleRows,
            std::collections::BTreeMap::from([(
                StatisticsMetric::Minimum {
                    column: std::sync::Arc::from("k"),
                },
                observed(
                    StatisticsMetricValue::I64(unsafe_integer),
                    queried,
                    StatisticsNumericNature::Exact,
                    StatisticsBasisRelation::Identical,
                ),
            )]),
        )
        .expect("evidence");

        let statistics = evidence_to_base_statistics(&evidence, &[column("k")]);
        assert_eq!(
            statistics
                .columns
                .get("k")
                .expect("column statistics")
                .min_value
                .known_value(),
            None,
            "an exact integer that f64 cannot represent must remain missing"
        );
    }

    #[test]
    fn optimizer_settings_digest_material_is_stable_across_rule_order_and_duplicates() {
        let unordered = SessionOptimizerSettings {
            disabled_rules: vec![
                "RuleB".to_string(),
                "RuleA".to_string(),
                "RuleB".to_string(),
            ],
            ..Default::default()
        };
        let canonical = SessionOptimizerSettings {
            disabled_rules: vec!["RuleA".to_string(), "RuleB".to_string()],
            ..Default::default()
        };

        assert_eq!(
            optimizer_settings_stable_digest_material(&unordered),
            optimizer_settings_stable_digest_material(&canonical)
        );
    }

    #[test]
    fn empty_statistics_snapshot_is_the_default_without_implying_zero_rows() {
        let _snapshot = DmlStatisticsSnapshot::default();
    }

    #[test]
    fn change_stream_facade_disables_global_runtime_filters() {
        assert_eq!(
            dml_change_stream_optimizer_settings().enable_global_runtime_filter,
            Some(false)
        );
    }

    fn statistics_route(target: u32) -> DmlChangeStreamRoute {
        DmlChangeStreamRoute {
            route_id: novarocks_spi::connector::ConnectorWriteRouteId::from_bytes([
                u8::try_from(target + 1).expect("small target");
                32
            ]),
            write_target_ordinal:
                novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(target)
                    .expect("bounded target"),
            accepted_effects: vec![
                novarocks_spi::connector::ConnectorRowMutationEffect::Insert,
            ],
            input_ordinals: Vec::new(),
            partition_input_tokens: Vec::new(),
            sink: DmlWritePlanInput(
                crate::planner::distributed::write::contract::test_support::simple_sql_write_plan_input(
                    crate::planner::distributed::write::ConnectorWriteInputBinding::RootOutputByOrdinal,
                ),
            ),
        }
    }

    fn empty_statistics_target(target: u32) -> DmlChangeStreamStatisticsTarget {
        DmlChangeStreamStatisticsTarget {
            write_target_ordinal:
                novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(target)
                    .expect("bounded target"),
            requirements: Vec::new(),
        }
    }

    #[test]
    fn change_stream_statistics_requires_exact_target_membership() {
        let functions = crate::functions::builtin_sql_function_catalog();

        let missing = plan_change_stream_writer_statistics(
            &[statistics_route(0), statistics_route(1)],
            vec![empty_statistics_target(0)],
            functions,
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .expect_err("missing target must fail");
        assert!(
            missing.to_string().contains("missing=[1], extraneous=[]"),
            "unexpected error: {missing}"
        );

        let duplicate = plan_change_stream_writer_statistics(
            &[statistics_route(0)],
            vec![empty_statistics_target(0), empty_statistics_target(0)],
            functions,
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .expect_err("duplicate target must fail");
        assert!(
            duplicate.to_string().contains("repeat write target 0"),
            "unexpected error: {duplicate}"
        );

        let extraneous = plan_change_stream_writer_statistics(
            &[statistics_route(0)],
            vec![empty_statistics_target(0), empty_statistics_target(1)],
            functions,
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .expect_err("extraneous target must fail");
        assert!(
            extraneous
                .to_string()
                .contains("missing=[], extraneous=[1]"),
            "unexpected error: {extraneous}"
        );
    }

    #[test]
    fn change_stream_statistics_accepts_explicit_empty_requirements_for_every_route() {
        let auxiliary = plan_change_stream_writer_statistics(
            &[statistics_route(0), statistics_route(1)],
            vec![empty_statistics_target(0), empty_statistics_target(1)],
            crate::functions::builtin_sql_function_catalog(),
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .expect("exact empty requirements are a valid ordinary mutation plan");

        assert!(auxiliary.schema().auxiliary_channels().is_empty());
        assert!(
            auxiliary
                .partial_for(
                    novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(0)
                        .expect("bounded target")
                )
                .expect("known target")
                .calls()
                .is_empty()
        );
        assert!(auxiliary.final_plan().calls().is_empty());
        assert!(auxiliary.final_plan().unpivot().is_none());
    }

    #[test]
    fn change_stream_statistics_production_helper_plans_nonempty_requirements() {
        use novarocks_functions::{AggregateOverloadMetadata, FunctionVisibility};
        use novarocks_spi::connector::{
            StatisticsArtifactIdentity, StatisticsRequiredAggregation, StatisticsScanColumn,
        };

        let functions = crate::functions::test_exact_aggregate_catalog(
            "$test_change_stream_blob",
            FunctionVisibility::Hidden,
            [AggregateOverloadMetadata::try_new(
                "test/change-stream-blob/i64/v1",
                [arrow::datatypes::DataType::Int64],
                arrow::datatypes::DataType::Binary,
                arrow::datatypes::DataType::Binary,
                "test/change-stream-blob-state/v1",
            )
            .expect("aggregate overload")],
        );
        let requirement = |target: u32| {
            StatisticsRequiredAggregation::try_new(
                StatisticsScanColumn::try_new(
                    0,
                    "order_id",
                    novarocks_type_contract::FunctionValueType::new(
                        arrow::datatypes::DataType::Int64,
                        false,
                    ),
                )
                .expect("scan column"),
                "$test_change_stream_blob",
                StatisticsArtifactIdentity::try_new(
                    vec![i32::try_from(target + 1).expect("field id")],
                    "test-change-stream-blob-v1",
                )
                .expect("artifact identity"),
            )
            .expect("requirement")
        };

        let auxiliary = plan_change_stream_writer_statistics(
            &[statistics_route(0), statistics_route(1)],
            vec![
                DmlChangeStreamStatisticsTarget {
                    write_target_ordinal:
                        novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(0)
                            .expect("target"),
                    requirements: vec![requirement(0)],
                },
                DmlChangeStreamStatisticsTarget {
                    write_target_ordinal:
                        novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(1)
                            .expect("target"),
                    requirements: vec![requirement(1)],
                },
            ],
            &functions,
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .expect("production helper plans routed statistics");

        for target in [0, 1] {
            assert_eq!(
                auxiliary
                    .partial_for(
                        novarocks_spi::connector::write_stack::WriteTargetOrdinal::try_new(target)
                            .expect("target")
                    )
                    .expect("known target")
                    .calls()
                    .len(),
                1
            );
        }
        assert_eq!(auxiliary.schema().auxiliary_channels().len(), 1);
        assert_eq!(auxiliary.final_plan().calls().len(), 1);
        assert_eq!(
            auxiliary
                .final_plan()
                .unpivot()
                .expect("artifact unpivot")
                .mappings()
                .len(),
            2
        );
    }
    #[test]
    fn actual_dml_output_interning_preserves_control_categories() {
        use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
        struct Refuse {
            cause: CompileControlError,
            at_entry: bool,
            seen: std::sync::atomic::AtomicBool,
        }
        impl PureCompileControl for Refuse {
            fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
                if (self.at_entry && units == 0) || (!self.at_entry && units == 256) {
                    self.seen.store(true, std::sync::atomic::Ordering::SeqCst);
                    return Err(self.cause);
                }
                Ok(())
            }
        }
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at_entry in [true, false] {
                let control = Refuse {
                    cause,
                    at_entry,
                    seen: std::sync::atomic::AtomicBool::new(false),
                };
                let data_type = arrow::datatypes::DataType::Struct(
                    (0..300)
                        .map(|i| {
                            arrow::datatypes::Field::new(
                                format!("f{i}"),
                                arrow::datatypes::DataType::Int64,
                                false,
                            )
                        })
                        .collect(),
                );
                let column = crate::analysis::OutputColumn {
                    column_id: crate::column_id::ColumnId(1),
                    name: "payload".into(),
                    value_type: novarocks_type_contract::FunctionValueType::new(data_type, false),
                    is_internal: false,
                };
                let mut arena = crate::optimizer::scalar::ScalarArena::new();
                let result = super::output_expr(
                    &mut arena,
                    &[column],
                    "payload",
                    "DML payload",
                    crate::column_id::ColumnId(2),
                    &control,
                );
                let node =
                    crate::optimizer::scalar::ScalarNode::ColumnRef(crate::column_id::ColumnId(99));
                let ty = novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int64,
                    false,
                );
                let first = arena.intern(node.clone(), ty.clone());
                let expected = crate::optimizer::scalar::ScalarArena::new().intern(node, ty);
                assert_eq!(first, expected, "refused insertion cannot publish a scalar");
                assert!(
                    control.seen.load(std::sync::atomic::Ordering::SeqCst),
                    "the original control must observe the refused work"
                );
                assert!(matches!(
                    (cause, result),
                    (
                        CompileControlError::Cancelled,
                        Err(crate::compiler::SqlCompileError::Cancelled)
                    ) | (
                        CompileControlError::DeadlineExceeded,
                        Err(crate::compiler::SqlCompileError::DeadlineExceeded)
                    ) | (
                        CompileControlError::ResourceExhausted,
                        Err(crate::compiler::SqlCompileError::ResourceExhausted)
                    )
                ));
            }
        }
    }
    #[test]
    fn actual_update_selected_new_column_skips_unused_fallback_interning() {
        use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
        struct RefuseUnusedWide;
        impl PureCompileControl for RefuseUnusedWide {
            fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
                if units == 256 {
                    return Err(CompileControlError::ResourceExhausted);
                }
                Ok(())
            }
        }
        for include_unused_old in [false, true] {
            let source = |id, name: &str, ty| crate::analysis::OutputColumn {
                column_id: crate::column_id::ColumnId(id),
                name: name.into(),
                value_type: novarocks_type_contract::FunctionValueType::new(ty, true),
                is_internal: false,
            };
            let mut columns = vec![
                source(1, "__nr_file", arrow::datatypes::DataType::Utf8),
                source(2, "__nr_pos", arrow::datatypes::DataType::Int64),
                source(3, "__nr_row_id", arrow::datatypes::DataType::Int64),
                source(4, "__nr_new_k", arrow::datatypes::DataType::Int64),
            ];
            if include_unused_old {
                columns.push(source(
                    5,
                    "k",
                    arrow::datatypes::DataType::Struct(
                        (0..300)
                            .map(|i| {
                                arrow::datatypes::Field::new(
                                    format!("f{i}"),
                                    arrow::datatypes::DataType::Int64,
                                    false,
                                )
                            })
                            .collect(),
                    ),
                ));
            }
            let mut node = crate::optimizer::OptimizedOperatorNode {
                op: crate::optimizer::operator::Operator::PhysicalValues(
                    crate::optimizer::operator::ValuesOp {
                        rows: vec![],
                        columns: columns.clone(),
                    },
                ),
                children: vec![],
                output_columns: columns,
                stats: Default::default(),
                explain_stats: Default::default(),
                execution_props: Default::default(),
            };
            crate::optimizer::optimized_tree::attach_scalar_arena(
                &mut node,
                std::sync::Arc::new(crate::optimizer::scalar::ScalarArena::new()),
            );
            let expanded =
                super::build_update_change_event_expand(node, &[column("k")], &RefuseUnusedWide)
                    .expect("selected new value skips missing or refused old value");
            let crate::optimizer::operator::Operator::PhysicalChangeEventExpand(expand) =
                &expanded.op
            else {
                panic!("expected change-event expansion");
            };
            let target = expanded
                .output_columns
                .iter()
                .find(|column| column.name == "k")
                .unwrap();
            let expression = expand.events[0]
                .assignments
                .iter()
                .find(|assignment| assignment.output_column_id == target.column_id)
                .unwrap()
                .expr
                .unwrap();
            assert!(matches!(
                expanded
                    .execution_props
                    .scalar_arena
                    .as_ref()
                    .unwrap()
                    .node(expression),
                crate::optimizer::scalar::ScalarNode::ColumnRef(crate::column_id::ColumnId(4))
            ));
        }
    }

    #[test]
    fn dml_read_and_ctas_keep_actual_analyzed_root_allow_across_move_handoffs() {
        use crate::compiler::{
            SqlAnalyzeRequest, SqlCompileControl, SqlCompileIntent, SqlCompiler,
            SqlOptimizeRequest, SqlPlannerTableSnapshot, SqlPlanningEnvironment, SqlSessionContext,
            SqlStatementInput,
        };
        let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
        let catalog = SqlPlannerTableSnapshot::new(&catalog);
        let statistics = DmlStatisticsSnapshot::empty();
        let constant_policy = novarocks_functions::ConstantPolicy {
            max_rows: 73,
            ..crate::constant::test_constant_policy()
        };
        for (session_mode, sql, expected) in [
            ("32", "SELECT 1", false),
            ("ALLOW_THROW_EXCEPTION", "SELECT 1", true),
            ("ERROR_IF_OVERFLOW", "SELECT 1", false),
            (
                "32",
                "SELECT /*+ SET_VAR(sql_mode='ALLOW_THROW_EXCEPTION') */ 1",
                true,
            ),
            (
                "ALLOW_THROW_EXCEPTION",
                "SELECT /*+ SET_VAR(sql_mode=32) */ 1",
                false,
            ),
            (
                "32",
                "SELECT x FROM (SELECT /*+ SET_VAR(sql_mode='ALLOW_THROW_EXCEPTION') */ 1 AS x) s",
                false,
            ),
            (
                "ALLOW_THROW_EXCEPTION",
                "SELECT x FROM (SELECT /*+ SET_VAR(sql_mode=32) */ 1 AS x) s",
                true,
            ),
        ] {
            let request = |intent| {
                let analyzed = SqlCompiler::analyze(SqlAnalyzeRequest::new(
                    SqlStatementInput::sql(sql),
                    intent,
                    SqlSessionContext {
                        sql_semantics: crate::sql_mode::SqlSemanticSettings::default()
                            .with_sql_mode(crate::sql_mode::SqlMode::from_assignment(session_mode)),
                        current_catalog: None,
                        current_database: "default".into(),
                        optimizer_settings: SessionOptimizerSettings::default(),
                    },
                    SqlPlanningEnvironment::Distributed,
                    &catalog,
                    crate::functions::builtin_sql_function_catalog(),
                    crate::compiler::noop_constant_evaluator(),
                    None,
                    constant_policy,
                    crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
                    SqlCompileControl::unbounded(),
                ))
                .unwrap()
                .into_pending()
                .unwrap();
                assert_eq!(analyzed.root_allow_throw_exception(), expected);
                let request =
                    SqlOptimizeRequest::new(analyzed, &statistics, SqlCompileControl::unbounded());
                assert_eq!(request.root_allow_throw_exception(), expected);
                request
            };
            let (completion, reads) = super::begin_final_dml_read_plan(
                request(SqlCompileIntent::DmlInternalRead),
                &SessionOptimizerSettings::default(),
                crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
            )
            .unwrap();
            assert!(reads.is_empty());
            assert_eq!(completion.root_allow_throw_exception, expected);
            assert_eq!(completion.constant_policy, constant_policy);
            let source = super::compile_ctas_source(
                request(SqlCompileIntent::Query),
                crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
            )
            .unwrap();
            assert_eq!(source.root_allow_throw_exception, expected);
            assert_eq!(source.constant_policy, constant_policy);
            assert_eq!(source.output_columns().len(), 1);
        }
    }

    #[test]
    fn ctas_capture_fingerprint_binds_root_allow_for_identical_actual_optimized_sources() {
        use crate::compiler::{
            SqlAnalyzeRequest, SqlCompileControl, SqlCompileIntent, SqlCompiler,
            SqlOptimizeRequest, SqlPlannerTableSnapshot, SqlPlanningEnvironment, SqlSessionContext,
            SqlStatementInput,
        };
        let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
        let catalog = SqlPlannerTableSnapshot::new(&catalog);
        let statistics = DmlStatisticsSnapshot::empty();
        let sources = ["32", "ALLOW_THROW_EXCEPTION"].map(|session_mode| {
            let analyzed = SqlCompiler::analyze(SqlAnalyzeRequest::new(
                SqlStatementInput::sql("SELECT 1"),
                SqlCompileIntent::Query,
                SqlSessionContext {
                    sql_semantics: crate::sql_mode::SqlSemanticSettings::default()
                        .with_sql_mode(crate::sql_mode::SqlMode::from_assignment(session_mode)),
                    current_catalog: None,
                    current_database: "default".into(),
                    optimizer_settings: SessionOptimizerSettings::default(),
                },
                SqlPlanningEnvironment::Distributed,
                &catalog,
                crate::functions::builtin_sql_function_catalog(),
                crate::compiler::noop_constant_evaluator(),
                None,
                crate::constant::test_constant_policy(),
                crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
                SqlCompileControl::unbounded(),
            ))
            .unwrap()
            .into_pending()
            .unwrap();
            super::compile_ctas_source(
                SqlOptimizeRequest::new(analyzed, &statistics, SqlCompileControl::unbounded()),
                crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
            )
            .unwrap()
        });
        assert!(!sources[0].root_allow_throw_exception);
        assert!(sources[1].root_allow_throw_exception);
        // The previous capture material was identical for these two admitted
        // statements, even though their frozen terminal exception modes differ.
        assert_eq!(
            format!("{:#?}", sources[0].optimized),
            format!("{:#?}", sources[1].optimized)
        );
        assert_ne!(
            sources[0].capture_fingerprint(),
            sources[1].capture_fingerprint()
        );
        for source in &sources {
            assert_eq!(
                source.capture_fingerprint(),
                source.clone().capture_fingerprint()
            );
        }
    }

    #[test]
    fn m07_ctas_source_keeps_exact_producer_domains_until_target_admission() {
        use crate::compiler::*;
        use novarocks_physical_plan::ResultValueDomain as Domain;
        for (sql, expected) in [
            (
                "select percentile_hash(cast(1 as double)) as v",
                Domain::Percentile,
            ),
            ("select to_bitmap(1) as v", Domain::Bitmap),
            ("select bitmap_to_binary(to_bitmap(1)) as v", Domain::Plain),
        ] {
            let catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
            let snapshot = SqlPlannerTableSnapshot::new(&catalog);
            let control = SqlCompileControl::unbounded();
            let analyzed = SqlCompiler::analyze(SqlAnalyzeRequest::new(
                SqlStatementInput::sql(sql),
                SqlCompileIntent::IcebergWrite {
                    root_distribution: RootDistributionRequirement::Any,
                },
                SqlSessionContext {
                    sql_semantics: Default::default(),
                    current_catalog: None,
                    current_database: "default".into(),
                    optimizer_settings: Default::default(),
                },
                SqlPlanningEnvironment::Distributed,
                &snapshot,
                builtin_sql_function_catalog(),
                noop_constant_evaluator(),
                None,
                crate::constant::test_constant_policy(),
                crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
                control.clone(),
            ))
            .unwrap()
            .into_pending()
            .unwrap();
            let source = super::compile_ctas_source(
                SqlOptimizeRequest::new(analyzed, &DmlStatisticsSnapshot::empty(), control),
                crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
            )
            .unwrap();
            let columns = source.output_columns();
            assert_eq!(columns.len(), 1, "{sql}");
            assert_eq!(columns[0].domain, expected, "{sql}");
            assert_eq!(
                source.has_private_output_domain(),
                expected == Domain::Percentile
            );
            if expected != Domain::Plain {
                let mut erased = source.clone();
                erased.output_domains[0] = Domain::Plain;
                assert_ne!(source.capture_fingerprint(), erased.capture_fingerprint());
            }
        }
    }

    #[test]
    fn m07_ctas_complete_private_declaration_survives_unmarked_multi_column_source() {
        use crate::compiler::*;
        use arrow::datatypes::{DataType as D, Field};
        use novarocks_types::schema::{ColumnDef, SqlType as T};
        use std::sync::Arc;
        for logical in [T::Object, T::Percentile] {
            let mut catalog = crate::planning::catalog::PlannerMemoryCatalog::default();
            crate::planning::catalog::register_test_connector_read_table(
                &mut catalog,
                "default",
                "t",
                vec![ColumnDef {
                    name: "state".into(),
                    data_type: D::List(Arc::new(Field::new("item", D::Binary, true))),
                    nullable: true,
                    write_default: None,
                    logical_type: Some(T::Array(Box::new(logical))),
                }],
            )
            .unwrap();
            let snapshot = SqlPlannerTableSnapshot::new(&catalog);
            let control = SqlCompileControl::unbounded();
            let analyzed = SqlCompiler::analyze(SqlAnalyzeRequest::new(
                SqlStatementInput::sql("select state, 1 as plain from t"),
                SqlCompileIntent::IcebergWrite {
                    root_distribution: RootDistributionRequirement::Any,
                },
                SqlSessionContext {
                    sql_semantics: Default::default(),
                    current_catalog: None,
                    current_database: "default".into(),
                    optimizer_settings: Default::default(),
                },
                SqlPlanningEnvironment::Distributed,
                &snapshot,
                builtin_sql_function_catalog(),
                noop_constant_evaluator(),
                None,
                crate::constant::test_constant_policy(),
                crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
                control.clone(),
            ))
            .unwrap()
            .into_pending()
            .unwrap();
            let source = super::compile_ctas_source(
                SqlOptimizeRequest::new(
                    analyzed,
                    &DmlStatisticsSnapshot::from_evidence([
                        super::DmlStatisticsEvidence::Missing {
                            binding: crate::binding::SqlTableBindingId::new_for_test(1),
                            label: "test_catalog.test_db.test_table".into(),
                            reason: "fixture has no published statistics".into(),
                        },
                    ]),
                    control,
                ),
                crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
            )
            .unwrap();
            assert_eq!(source.output_columns().len(), 2);
            assert_eq!(
                source.output_columns()[0].domain,
                novarocks_physical_plan::ResultValueDomain::Plain
            );
            assert!(source.has_private_output_domain());
            let mut erased = source.clone();
            erased.private_output_domain = false;
            assert_ne!(source.capture_fingerprint(), erased.capture_fingerprint());
        }
    }
}
