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

impl DmlWritePlanInput {
    pub fn try_new(
        mode: DmlWriteSinkMode,
        target: DmlWriteTarget,
        input_columns: Vec<novarocks_types::schema::ColumnDef>,
        input: crate::plan_read::ConnectorWriteInputBinding,
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

/// Seal one application-admitted frozen connector source into an immutable
/// distributed write plan.
///
/// The writer is an ordinary node emitting the write relation, and every writer
/// gathers into one Root finish fragment whose terminal is the ordinary result
/// sink. The physical source and write contract remain opaque outside SQL;
/// provider bindings, leases, and provenance stay retained by the application
/// that admitted them.
pub fn build_frozen_connector_write_dataflow_plan(
    source: crate::planning::query_execution::FrozenConnectorScanPlan,
    sink: DmlWritePlanInput,
    write_target_ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
    statistics: &[novarocks_spi::connector::StatisticsRequiredAggregation],
    functions: &dyn crate::compiler::SqlFunctionCatalog,
    settings: &crate::compiler::SessionOptimizerSettings,
) -> Result<crate::plan_read::DistributedPlan, String> {
    let physical = source.into_physical();
    let target_schema =
        crate::planner::distributed::write::sink::ConnectorWritePlanInput::target_schema_from_sql_write_plan_input(&sink.0);
    let auxiliary = crate::planner::distributed::write::auxiliary::plan_writer_statistics(
        &[
            crate::planner::distributed::write::auxiliary::WriterStatisticsTargetInput {
                target: write_target_ordinal,
                input_schema: target_schema.as_ref(),
                requirements: statistics,
            },
        ],
        functions,
    )?;
    crate::planner::pipeline::build_sql_write_dataflow_plan_with_auxiliary_settings(
        physical,
        sink.0,
        write_target_ordinal,
        &auxiliary,
        settings,
    )
}

/// Compile an immutable SQL request into a sealed connector-write plan.
/// Application code supplies only the already-admitted request context and
/// opaque write contract; optimizer and physical planner artifacts do not
/// cross this boundary.
pub fn compile_connector_write_dataflow_plan(
    request: crate::compiler::SqlOptimizeRequest<'_>,
    sink: DmlWritePlanInput,
    write_target_ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
    statistics: &[novarocks_spi::connector::StatisticsRequiredAggregation],
    settings: &crate::compiler::SessionOptimizerSettings,
) -> Result<crate::plan_read::DistributedPlan, String> {
    let compiled = crate::compiler::SqlCompiler::optimize(request)
        .map_err(|error| error.to_string())?
        .into_optimized_output()
        .map_err(|_| "connector write intent did not produce optimized SQL facts".to_string())?;
    let physical = crate::planner::optimizer_bridge::to_physical_plan(&compiled.optimized_tree)?;
    let target_schema =
        crate::planner::distributed::write::sink::ConnectorWritePlanInput::target_schema_from_sql_write_plan_input(&sink.0);
    let auxiliary = crate::planner::distributed::write::auxiliary::plan_writer_statistics(
        &[
            crate::planner::distributed::write::auxiliary::WriterStatisticsTargetInput {
                target: write_target_ordinal,
                input_schema: target_schema.as_ref(),
                requirements: statistics,
            },
        ],
        compiled.function_catalog.as_ref(),
    )?;
    crate::planner::pipeline::build_sql_write_dataflow_plan_with_auxiliary_settings(
        physical,
        sink.0,
        write_target_ordinal,
        &auxiliary,
        settings,
    )
}

/// Compile one immutable query request directly to its sealed distributed
/// plan. This is the read-side counterpart to the DML write entrypoints and
/// keeps optimized/scalar graphs inside SQL.
pub fn compile_query_distributed_plan(
    request: crate::compiler::SqlOptimizeRequest<'_>,
) -> Result<crate::plan_read::DistributedPlan, String> {
    crate::compiler::SqlCompiler::optimize(request)
        .map_err(|error| error.to_string())?
        .into_distributed_plan()
        .map_err(|error| error.to_string())
}

/// SQL-owned, immutable CTAS source artifact. Its optimizer graph never
/// leaves this module; application code may inspect only the source schema,
/// stable capture fingerprint, and sealed write plan derived from it.
#[derive(Clone, Debug)]
pub struct DmlCtasSourcePlan {
    optimized: crate::optimizer::OptimizedOperatorNode,
    function_catalog: std::sync::Arc<dyn crate::compiler::SqlFunctionCatalog>,
}

/// One source output field exposed to CTAS target admission.
#[derive(Clone, Debug, PartialEq)]
pub struct DmlSourceColumn {
    pub name: String,
    pub data_type: arrow::datatypes::DataType,
    pub nullable: bool,
}

impl DmlCtasSourcePlan {
    pub fn output_columns(&self) -> Vec<DmlSourceColumn> {
        self.optimized
            .output_columns
            .iter()
            .map(|column| DmlSourceColumn {
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
            })
            .collect()
    }

    /// Versioned digest of the frozen in-memory optimizer artifact used to
    /// bind CTAS source preparation to exactly one compilation.
    pub fn capture_fingerprint(&self) -> [u8; 32] {
        use sha2::{Digest, Sha256};

        let material = format!("{:#?}", self.optimized);
        let mut digest = Sha256::new();
        for part in [
            b"novarocks.ctas-optimized-capture.v1".as_slice(),
            material.as_bytes(),
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
) -> Result<DmlCtasSourcePlan, String> {
    let compiled = crate::compiler::SqlCompiler::optimize(request)
        .map_err(|error| error.to_string())?
        .into_optimized_output()
        .map_err(|_| "CTAS source did not produce optimized SQL facts".to_string())?;
    Ok(DmlCtasSourcePlan {
        optimized: compiled.optimized_tree,
        function_catalog: compiled.function_catalog,
    })
}

/// Attach an already admitted CTAS write schema to its frozen source and
/// return the sealed distributed write plan.
pub fn build_ctas_connector_write_dataflow_plan(
    source: &DmlCtasSourcePlan,
    target_schema: arrow::datatypes::SchemaRef,
    write_target_ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
    statistics: &[novarocks_spi::connector::StatisticsRequiredAggregation],
    settings: &crate::compiler::SessionOptimizerSettings,
) -> Result<crate::plan_read::DistributedPlan, String> {
    let physical = crate::planner::optimizer_bridge::to_physical_plan(&source.optimized)?;
    let auxiliary = crate::planner::distributed::write::auxiliary::plan_writer_statistics(
        &[
            crate::planner::distributed::write::auxiliary::WriterStatisticsTargetInput {
                target: write_target_ordinal,
                input_schema: target_schema.as_ref(),
                requirements: statistics,
            },
        ],
        source.function_catalog.as_ref(),
    )?;
    crate::planner::pipeline::build_connector_write_dataflow_plan_with_auxiliary_settings(
        physical,
        crate::planner::distributed::write::sink::ConnectorWritePlanInput {
            target_schema,
            input: crate::planner::distributed::write::contract::ConnectorWriteInputBinding::RootOutputByOrdinal,
            root_output_exprs: None,
        },
        write_target_ordinal,
        &auxiliary,
        settings,
    )
}

/// Test-only sealed connector-write fixture for application encoder tests.
/// The distributed graph remains opaque; callers receive only its read model.
#[doc(hidden)]
pub fn native_encoder_test_fixture_plan() -> Result<crate::plan_read::DistributedPlan, String> {
    crate::planner::distributed::native_encoder_test_fixture_plan()
}

/// Provider route facts that SQL binds to a change-stream producer.  Field
/// names are resolved against the sealed producer output inside SQL, never by
/// the application against an optimizer tree.
#[derive(Clone, Debug)]
pub struct DmlChangeStreamRoute {
    pub route_id: novarocks_spi::connector::ConnectorWriteRouteId,
    /// The begin session's dense, query-local ordinal for the logical write
    /// target this branch feeds. It is the branch's whole identity: nothing
    /// downstream recovers a cohort, an operation, or a placement from it.
    pub write_target_ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
    pub accepted_effects: Vec<novarocks_spi::connector::ConnectorRowMutationEffect>,
    pub input_fields: Vec<DmlChangeStreamRouteField>,
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

#[derive(Clone, Debug)]
pub struct DmlChangeStreamRouteField {
    pub token: novarocks_spi::connector::ConnectorWriteFieldToken,
    pub output_name: String,
}

/// SQL-only specification of a generated row-mutation producer.
#[derive(Clone, Debug)]
pub enum DmlChangeStreamKind {
    Update {
        target_columns: Vec<novarocks_types::schema::ColumnDef>,
        new_sequence_number: i64,
    },
    Merge {
        target_columns: Vec<novarocks_types::schema::ColumnDef>,
        new_sequence_number: i64,
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

/// Read-only routing facts needed by Core when it binds a prepared write
/// operation to fragment cohorts.  SQL keeps the mutable writer topology and
/// all physical graph state private.
#[derive(Clone, Debug)]
pub struct DmlChangeStreamWriterRoute {
    pub route_id: novarocks_spi::connector::ConnectorWriteRouteId,
    pub write_target_ordinal: novarocks_spi::connector::write_stack::WriteTargetOrdinal,
    pub accepted_effects: Vec<novarocks_spi::connector::ConnectorRowMutationEffect>,
    pub writer_fragment_id: crate::plan_read::FragmentId,
}

/// Sealed SQL plan plus the minimal immutable routing projection Core needs
/// for normal fragment preparation and provider-session registration.
pub struct DmlChangeStreamPlan {
    distributed_plan: crate::plan_read::DistributedPlan,
    writer_routes: Vec<DmlChangeStreamWriterRoute>,
}

impl DmlChangeStreamPlan {
    pub fn distributed_plan(&self) -> &crate::plan_read::DistributedPlan {
        &self.distributed_plan
    }

    pub fn into_parts(
        self,
    ) -> (
        crate::plan_read::DistributedPlan,
        Vec<DmlChangeStreamWriterRoute>,
    ) {
        (self.distributed_plan, self.writer_routes)
    }
}

/// Compile a generated UPDATE/MERGE change-stream query directly into a
/// sealed distributed write plan.  No optimized tree, scalar arena, physical
/// graph, or draft distributed plan escapes SQL.
pub fn compile_dml_change_stream(
    request: DmlChangeStreamCompileRequest<'_>,
) -> Result<DmlChangeStreamPlan, String> {
    let compiled = crate::compiler::SqlCompiler::optimize(request.optimize_request)
        .map_err(|error| error.to_string())?
        .into_optimized_output()
        .map_err(|_| "change-stream intent did not produce an optimized SQL plan".to_string())?;

    let producer = match request.kind {
        DmlChangeStreamKind::Update {
            target_columns,
            new_sequence_number,
        } => build_update_change_event_expand(
            compiled.optimized_tree,
            &target_columns,
            new_sequence_number,
        )?,
        DmlChangeStreamKind::Merge {
            target_columns,
            new_sequence_number,
            matched_update,
            matched_delete,
            not_matched_insert,
        } => build_merge_change_event_expand(
            compiled.optimized_tree,
            &target_columns,
            new_sequence_number,
            matched_update,
            matched_delete,
            not_matched_insert,
        )?,
    };
    seal_change_stream_producer(
        producer,
        request.routes,
        request.statistics_targets,
        request.pre_expand_keyed_assert,
        request.shape,
        compiled.function_catalog.as_ref(),
    )
}

/// Seal an SQL-owned generated change-stream producer after a specialized
/// compiler terminal has applied its immutable transformation.  This stays
/// crate-private: public callers submit only value-only terminal contexts and
/// receive [`DmlChangeStreamPlan`], never an optimizer tree or draft DAG.
pub(crate) fn seal_change_stream_producer(
    producer: crate::optimizer::OptimizedOperatorNode,
    routes: Vec<DmlChangeStreamRoute>,
    statistics_targets: Vec<DmlChangeStreamStatisticsTarget>,
    pre_expand_keyed_assert: Option<DmlPreExpandKeyedAssert>,
    shape: DmlWritePlanShape,
    functions: &dyn crate::compiler::SqlFunctionCatalog,
) -> Result<DmlChangeStreamPlan, String> {
    seal_change_stream_producer_with_effect_column(
        producer,
        routes,
        statistics_targets,
        crate::common::ROW_MUTATION_EFFECT_COLUMN,
        pre_expand_keyed_assert,
        shape,
        functions,
    )
}

/// Seal a specialized SQL-owned change-stream producer that uses a
/// terminal-specific effect output.  The effect name never crosses the public
/// boundary; the returned plan remains opaque to application code.
pub(crate) fn seal_change_stream_producer_with_effect_column(
    producer: crate::optimizer::OptimizedOperatorNode,
    routes: Vec<DmlChangeStreamRoute>,
    statistics_targets: Vec<DmlChangeStreamStatisticsTarget>,
    effect_output_name: &str,
    pre_expand_keyed_assert: Option<DmlPreExpandKeyedAssert>,
    shape: DmlWritePlanShape,
    functions: &dyn crate::compiler::SqlFunctionCatalog,
) -> Result<DmlChangeStreamPlan, String> {
    let auxiliary = plan_change_stream_writer_statistics(&routes, statistics_targets, functions)?;
    let dag = bind_route_layout(&producer.output_columns, routes, effect_output_name)?;
    let keyed_assert = pre_expand_keyed_assert.map(|assertion| {
        crate::planner::physical::PreExpandKeyedAssertSpec {
            key_column_name: assertion.key_column_name,
            key_label: assertion.key_label,
            message_prefix: assertion.message_prefix,
        }
    });
    let physical = crate::planner::optimizer_bridge::to_physical_plan(&producer)?;
    let settings = dml_change_stream_optimizer_settings();
    let planned = match shape {
        DmlWritePlanShape::Dataflow => {
            crate::planner::pipeline::build_sql_change_stream_dataflow_plan_with_auxiliary_settings(
                physical,
                dag,
                keyed_assert,
                &auxiliary,
                &settings,
            )?
        }
    };
    let writer_routes = planned
        .topology
        .writer_routes
        .iter()
        .map(|route| DmlChangeStreamWriterRoute {
            route_id: route.route_id,
            write_target_ordinal: route.write_target_ordinal,
            accepted_effects: route.accepted_effects.clone(),
            writer_fragment_id: route.writer_fragment_id,
        })
        .collect();
    Ok(DmlChangeStreamPlan {
        distributed_plan: planned.distributed_plan,
        writer_routes,
    })
}

fn plan_change_stream_writer_statistics(
    routes: &[DmlChangeStreamRoute],
    statistics_targets: Vec<DmlChangeStreamStatisticsTarget>,
    functions: &dyn crate::compiler::SqlFunctionCatalog,
) -> Result<crate::planner::distributed::write::auxiliary::WriterAuxiliaryPlan, String> {
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
            ));
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
        ));
    }

    let target_schemas = routes
        .iter()
        .map(|route| {
            crate::planner::distributed::write::sink::ConnectorWritePlanInput::target_schema_from_sql_write_plan_input(
                &route.sink.0,
            )
        })
        .collect::<Vec<_>>();
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
    crate::planner::distributed::write::auxiliary::plan_writer_statistics(&inputs, functions)
}

fn bind_route_layout(
    output_columns: &[crate::analysis::OutputColumn],
    routes: Vec<DmlChangeStreamRoute>,
    effect_output_name: &str,
) -> Result<crate::planner::distributed::write::change_stream::ChangeStreamWriteDagSpec, String> {
    use crate::planner::distributed::write::change_stream::{
        ChangeStreamWriteLayoutRequest, ChangeStreamWriteLayoutRoute,
        bind_change_stream_write_layout,
    };

    let effect_output_ordinal = output_columns
        .iter()
        .position(|column| column.name == effect_output_name)
        .ok_or_else(|| "row-mutation producer has no logical effect output".to_string())?;
    let routes = routes
        .into_iter()
        .map(|route| {
            let input_ordinals = route
                .input_fields
                .into_iter()
                .map(|field| {
                    output_columns
                        .iter()
                        .position(|column| column.name.eq_ignore_ascii_case(&field.output_name))
                        .ok_or_else(|| {
                            format!(
                                "row-mutation producer has no output for Provider route field `{}`",
                                field.output_name
                            )
                        })
                        .and_then(|ordinal| {
                            u32::try_from(ordinal).map_err(|_| {
                                "row-mutation producer output ordinal exceeds u32".to_string()
                            })
                        })
                        .map(|ordinal| {
                            novarocks_spi::connector::ConnectorMutationRouteInput::new(
                                field.token,
                                ordinal,
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ChangeStreamWriteLayoutRoute {
                route_id: route.route_id,
                write_target_ordinal: route.write_target_ordinal,
                accepted_effects: route.accepted_effects,
                input_ordinals,
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
    new_sequence_number: i64,
) -> Result<crate::optimizer::OptimizedOperatorNode, String> {
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
        )?,
        output_expr(
            &mut arena,
            &child_outputs,
            "__nr_pos",
            "UPDATE old row position",
            pos.column_id,
        )?,
    ];
    for (name, output) in &targets {
        let new_name = format!("__nr_new_{name}");
        let expr = maybe_output_column_by_name(&child_outputs, &new_name)?
            .map(|column| intern_column(&mut arena, &column))
            .transpose()?
            .unwrap_or(child_expr(
                &mut arena,
                &child_outputs,
                name,
                "UPDATE unchanged target column",
            )?);
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
    )?);
    let sequence = arena.intern(
        crate::optimizer::scalar::ScalarNode::Literal(crate::optimizer::scalar::HashableLiteral(
            crate::analysis::LiteralValue::Int(new_sequence_number),
        )),
        arrow::datatypes::DataType::Int64,
        false,
    );
    assignments.push(crate::optimizer::operator::ChangeEventOutputExpr {
        output_column_id: last_sequence.column_id,
        expr: Some(sequence),
    });
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
}

#[allow(clippy::too_many_arguments)]
fn build_merge_change_event_expand(
    optimized_tree: crate::optimizer::OptimizedOperatorNode,
    target_columns: &[novarocks_types::schema::ColumnDef],
    new_sequence_number: i64,
    matched_update: bool,
    matched_delete: bool,
    not_matched_insert: bool,
) -> Result<crate::optimizer::OptimizedOperatorNode, String> {
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
        )?,
        output_expr(
            &mut arena,
            &child_outputs,
            "__nr_pos",
            "MERGE old row position",
            pos.column_id,
        )?,
    ];
    let mut reuse_assignments = vec![
        output_expr(
            &mut arena,
            &child_outputs,
            "__nr_file",
            "MERGE old file",
            file.column_id,
        )?,
        output_expr(
            &mut arena,
            &child_outputs,
            "__nr_pos",
            "MERGE old row position",
            pos.column_id,
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
        )?);
        let new_name = format!("__nr_new_{name}");
        let reuse = maybe_output_column_by_name(&child_outputs, &new_name)?
            .map(|column| intern_column(&mut arena, &column))
            .transpose()?
            .unwrap_or(child_expr(
                &mut arena,
                &child_outputs,
                name,
                "MERGE unchanged target column",
            )?);
        reuse_assignments.push(crate::optimizer::operator::ChangeEventOutputExpr {
            output_column_id: output.column_id,
            expr: Some(reuse),
        });
        let insert_name = format!("__nr_ins_{name}");
        if let Some(column) = maybe_output_column_by_name(&child_outputs, &insert_name)? {
            fresh_assignments.push(crate::optimizer::operator::ChangeEventOutputExpr {
                output_column_id: output.column_id,
                expr: Some(intern_column(&mut arena, &column)?),
            });
        }
    }
    reuse_assignments.push(output_expr(
        &mut arena,
        &child_outputs,
        "__nr_row_id",
        "MERGE old row id",
        row_id.column_id,
    )?);
    let sequence = arena.intern(
        crate::optimizer::scalar::ScalarNode::Literal(crate::optimizer::scalar::HashableLiteral(
            crate::analysis::LiteralValue::Int(new_sequence_number),
        )),
        arrow::datatypes::DataType::Int64,
        false,
    );
    reuse_assignments.push(crate::optimizer::operator::ChangeEventOutputExpr {
        output_column_id: last_sequence.column_id,
        expr: Some(sequence),
    });
    let mut events = Vec::new();
    if matched_update {
        events.push(crate::optimizer::operator::ChangeEventSpec {
            predicate: Some(merge_action_predicate(&mut arena, &child_outputs, 1)?),
            effect: novarocks_spi::connector::ConnectorRowMutationEffect::Replace,
            assignments: reuse_assignments,
        });
    }
    if matched_delete {
        events.push(crate::optimizer::operator::ChangeEventSpec {
            predicate: Some(merge_action_predicate(&mut arena, &child_outputs, 2)?),
            effect: novarocks_spi::connector::ConnectorRowMutationEffect::Delete,
            assignments: delete_assignments,
        });
    }
    if not_matched_insert {
        events.push(crate::optimizer::operator::ChangeEventSpec {
            predicate: Some(merge_action_predicate(&mut arena, &child_outputs, 3)?),
            effect: novarocks_spi::connector::ConnectorRowMutationEffect::Insert,
            assignments: fresh_assignments,
        });
    }
    if events.is_empty() {
        return Err("MOR MERGE change-stream expand requires at least one event".to_string());
    }
    build_change_expand(
        distributed,
        arena,
        change_output_columns(&file, &pos, &targets, &row_id, &last_sequence, &effect),
        effect.column_id,
        events,
    )
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
                data_type,
                nullable,
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
                    column.nullable,
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
    let mut columns = Vec::with_capacity(targets.len() + 6);
    columns.push(file.clone());
    columns.push(pos.clone());
    columns.extend(targets.iter().map(|(_, column)| column.clone()));
    columns.push(row_id.clone());
    columns.push(last_sequence.clone());
    columns.push(effect.clone());
    columns
}

fn output_expr(
    arena: &mut crate::optimizer::scalar::ScalarArena,
    columns: &[crate::analysis::OutputColumn],
    name: &str,
    label: &str,
    output_column_id: crate::column_id::ColumnId,
) -> Result<crate::optimizer::operator::ChangeEventOutputExpr, String> {
    Ok(crate::optimizer::operator::ChangeEventOutputExpr {
        output_column_id,
        expr: Some(child_expr(arena, columns, name, label)?),
    })
}

fn intern_column(
    arena: &mut crate::optimizer::scalar::ScalarArena,
    column: &crate::analysis::OutputColumn,
) -> Result<crate::optimizer::scalar::ScalarId, String> {
    Ok(arena.intern(
        crate::optimizer::scalar::ScalarNode::ColumnRef(column.column_id),
        column.data_type.clone(),
        column.nullable,
    ))
}

fn child_expr(
    arena: &mut crate::optimizer::scalar::ScalarArena,
    columns: &[crate::analysis::OutputColumn],
    name: &str,
    label: &str,
) -> Result<crate::optimizer::scalar::ScalarId, String> {
    let column = output_column_by_name(columns, name, label)?;
    intern_column(arena, &column)
}

fn merge_action_predicate(
    arena: &mut crate::optimizer::scalar::ScalarArena,
    columns: &[crate::analysis::OutputColumn],
    action: i32,
) -> Result<crate::optimizer::scalar::ScalarId, String> {
    let action_expr = child_expr(arena, columns, "__nr_merge_action", "MERGE action")?;
    let literal = arena.intern(
        crate::optimizer::scalar::ScalarNode::Literal(crate::optimizer::scalar::HashableLiteral(
            crate::analysis::LiteralValue::Int(i64::from(action)),
        )),
        arrow::datatypes::DataType::Int64,
        false,
    );
    Ok(arena.intern(
        crate::optimizer::scalar::ScalarNode::BinaryOp {
            op: crate::common::BinOp::Eq,
            left: action_expr,
            right: literal,
        },
        arrow::datatypes::DataType::Boolean,
        false,
    ))
}

fn build_change_expand(
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

/// Build the SQL-owned physical and distributed ANALYZE program from a pinned
/// connector scan and provider-selected ordinary aggregate calls.
///
/// This deliberately constructs the complete physical shape directly. An
/// ANALYZE attempt is not user SQL, so there is no logical aggregate for the
/// optimizer to discover or provider-specific sink for the distributed
/// planner to install:
///
/// `Scan -> Local Aggregate -> Gather -> Global Aggregate -> Unpivot -> Result`
///
/// The long-form Root relation is exactly
/// `(input_fields List<Int32>, blob_type Utf8, body Binary,
/// properties Map<Utf8, Utf8>)`. The generic Unpivot constants carry the
/// frozen identity without giving Execution any statistics semantics.
pub fn build_statistics_connector_plan(
    scan: StatisticsConnectorScan,
    required: &[novarocks_spi::connector::StatisticsRequiredAggregation],
    functions: &dyn crate::compiler::SqlFunctionCatalog,
    settings: &crate::compiler::SessionOptimizerSettings,
) -> Result<crate::plan_read::DistributedPlan, String> {
    if required.is_empty() {
        return Err(
            "empty ANALYZE requirements must bypass distributed planning and execution".to_string(),
        );
    }
    let mut factory = crate::column_id::ColumnRefFactory::new();
    let scan_columns = scan
        .columns
        .iter()
        .map(|column| {
            let column_id = factory.create(
                None,
                column.name().to_string(),
                column.data_type().clone(),
                column.nullable(),
            );
            crate::analysis::OutputColumn {
                column_id,
                name: column.name().to_string(),
                data_type: column.data_type().clone(),
                nullable: column.nullable(),
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
        return Err("ANALYZE scan contains duplicate provider column ordinals".to_string());
    }
    let scan = crate::planner::physical::PhysicalPlanNode {
        kind: crate::planner::physical::PhysicalPlanKind::Scan(
            crate::planner::payload::PlanScanNode {
                database: scan.namespace.clone(),
                table: crate::planner::table::TableDef {
                    name: scan.table.clone(),
                    columns: scan
                        .columns
                        .iter()
                        .map(|column| novarocks_types::schema::ColumnDef {
                            name: column.name().to_string(),
                            data_type: column.data_type().clone(),
                            nullable: column.nullable(),
                            write_default: None,
                            logical_type: None,
                        })
                        .collect(),
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
            },
        ),
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
    let mut calls = Vec::with_capacity(required.len());
    let mut final_columns = Vec::with_capacity(required.len());
    let mut partial_columns = Vec::with_capacity(required.len());
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
        if input.data_type != *requirement.input().data_type()
            || input.nullable != requirement.input().nullable()
        {
            return Err(format!(
                "ANALYZE aggregate input `{}` does not match its pinned scan type",
                requirement.input().name()
            ));
        }
        let resolved = functions
            .resolve_aggregate_trusted(
                requirement.function_name(),
                std::slice::from_ref(requirement.input().data_type()),
            )
            .map_err(|error| {
                format!(
                    "resolve trusted ANALYZE aggregate `{}` for {:?}: {error}",
                    requirement.function_name(),
                    requirement.input().data_type()
                )
            })?;
        let output_id = factory.create(
            None,
            format!("analyze_body_{index}"),
            resolved.output_type.clone(),
            true,
        );
        let output_name = format!("analyze_body_{index}");
        final_columns.push(crate::analysis::OutputColumn {
            column_id: output_id,
            name: output_name.clone(),
            data_type: resolved.output_type.clone(),
            nullable: true,
            is_internal: true,
        });
        partial_columns.push(crate::analysis::OutputColumn {
            column_id: output_id,
            name: output_name,
            data_type: resolved.intermediate_type.clone(),
            nullable: true,
            is_internal: true,
        });
        calls.push(crate::planner::payload::AggregateCall {
            name: requirement.function_name().to_string(),
            args: vec![crate::analysis::TypedExpr {
                kind: crate::analysis::ExprKind::ColumnRef {
                    column_id: input.column_id,
                    qualifier: None,
                    column: input.name.clone(),
                },
                data_type: input.data_type.clone(),
                nullable: input.nullable,
            }],
            distinct: false,
            result_type: resolved.output_type.clone(),
            order_by: Vec::new(),
            output_column_id: output_id,
            resolved,
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
                aggregates: calls.clone(),
                is_merge: vec![false; calls.len()],
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
                aggregates: calls,
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
        input_fields_type.clone(),
        false,
    );
    let blob_type = factory.create(
        None,
        "blob_type".to_string(),
        arrow::datatypes::DataType::Utf8,
        false,
    );
    let body = factory.create(
        None,
        "body".to_string(),
        arrow::datatypes::DataType::Binary,
        true,
    );
    let properties = factory.create(
        None,
        "properties".to_string(),
        properties_type.clone(),
        false,
    );
    let root_columns = vec![
        crate::analysis::OutputColumn {
            column_id: input_fields,
            name: "input_fields".to_string(),
            data_type: input_fields_type,
            nullable: false,
            is_internal: true,
        },
        crate::analysis::OutputColumn {
            column_id: blob_type,
            name: "blob_type".to_string(),
            data_type: arrow::datatypes::DataType::Utf8,
            nullable: false,
            is_internal: true,
        },
        crate::analysis::OutputColumn {
            column_id: body,
            name: "body".to_string(),
            data_type: arrow::datatypes::DataType::Binary,
            nullable: true,
            is_internal: true,
        },
        crate::analysis::OutputColumn {
            column_id: properties,
            name: "properties".to_string(),
            data_type: properties_type,
            nullable: false,
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
                        data_type: arrow::datatypes::DataType::Utf8,
                        nullable: false,
                    }),
                    crate::analysis::UnpivotConstant::Utf8Map(Vec::new()),
                ],
            },
        )
        .collect::<Vec<_>>();
    let unpivot = crate::planner::payload::PlanUnpivotNode::try_new(
        &final_columns,
        Vec::new(),
        body,
        vec![input_fields, blob_type, properties],
        value_mappings,
        root_columns.clone(),
        4096,
        novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES,
    )?;
    let physical = crate::planner::physical::PhysicalPlanNode {
        kind: crate::planner::physical::PhysicalPlanKind::Unpivot(unpivot),
        children: vec![global],
        output_columns: root_columns,
        stats,
        probe_runtime_filters: Vec::new(),
    };
    crate::planner::pipeline::build_distributed_plan_with_settings(physical, settings)
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

    #[test]
    fn analyze_statistics_uses_ordinary_two_phase_aggregate_unpivot_and_result_sink() {
        use crate::planner::distributed::{DistributedNode, DistributedNodeKind};
        use crate::planner::physical::AggMode;
        use novarocks_functions::{
            AggregateOverloadMetadata, EngineFunctionCatalogBuilder, FunctionDefinition,
            FunctionVisibility, FunctionVolatility,
        };
        use novarocks_spi::connector::{
            StatisticsArtifactIdentity, StatisticsRequiredAggregation, StatisticsScanColumn,
        };

        let definition = FunctionDefinition::try_new_exact_aggregate(
            "$test_blob_aggregate",
            FunctionVisibility::Hidden,
            FunctionVolatility::Immutable,
            [AggregateOverloadMetadata::try_new(
                "test/blob-aggregate/i64/v1",
                [arrow::datatypes::DataType::Int64],
                arrow::datatypes::DataType::Binary,
                arrow::datatypes::DataType::Binary,
                "test/blob-state/v1",
            )
            .expect("aggregate overload")],
        )
        .expect("aggregate definition");
        let mut functions = EngineFunctionCatalogBuilder::new();
        functions.register(definition).expect("register aggregate");
        let functions = functions.seal().expect("function catalog");
        let requirement = StatisticsRequiredAggregation::try_new(
            StatisticsScanColumn::try_new(0, "id", arrow::datatypes::DataType::Int64, true)
                .expect("scan column"),
            "$test_blob_aggregate",
            StatisticsArtifactIdentity::try_new(vec![7], "test-blob-v1")
                .expect("artifact identity"),
        )
        .expect("requirement");

        let plan = super::build_statistics_connector_plan(
            super::StatisticsConnectorScan {
                binding: crate::binding::SqlTableBindingId::new_for_test(1),
                catalog: "iceberg".into(),
                namespace: "db".into(),
                table: "t".into(),
                version_ordinal: 42,
                columns: vec![
                    StatisticsScanColumn::try_new(0, "id", arrow::datatypes::DataType::Int64, true)
                        .expect("scan column"),
                ],
            },
            &[requirement],
            &functions,
            &SessionOptimizerSettings::default(),
        )
        .expect("ANALYZE plan");

        assert_eq!(plan.fragments().len(), 2);
        assert!(matches!(
            plan.fragments()
                .iter()
                .find(|fragment| fragment.fragment_id == plan.root_fragment_id())
                .expect("Root fragment")
                .sink,
            crate::planner::distributed::DataSink::Result
        ));
        fn visit(
            node: &DistributedNode,
            scan: &mut usize,
            local: &mut usize,
            exchange: &mut usize,
            global: &mut usize,
            unpivot: &mut usize,
        ) {
            match &node.payload {
                DistributedNodeKind::Scan(_) => *scan += 1,
                DistributedNodeKind::HashAggregate(aggregate) => match aggregate.mode {
                    AggMode::Local => *local += 1,
                    AggMode::Global => *global += 1,
                    _ => {}
                },
                DistributedNodeKind::Exchange(_) => *exchange += 1,
                DistributedNodeKind::Unpivot(node) => {
                    *unpivot += 1;
                    assert_eq!(
                        node.max_output_bytes,
                        novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_RESULT_BATCH_BYTES
                    );
                    assert_eq!(
                        node.output_columns
                            .iter()
                            .map(|column| {
                                (
                                    column.name.as_str(),
                                    column.data_type.clone(),
                                    column.nullable,
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
                _ => {}
            }
            for child in &node.children {
                visit(child, scan, local, exchange, global, unpivot);
            }
        }
        let (mut scan, mut local, mut exchange, mut global, mut unpivot) = (0, 0, 0, 0, 0);
        for fragment in plan.fragments() {
            visit(
                &fragment.root,
                &mut scan,
                &mut local,
                &mut exchange,
                &mut global,
                &mut unpivot,
            );
        }
        assert_eq!((scan, local, exchange, global, unpivot), (1, 1, 1, 1, 1));
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
            input_fields: Vec::new(),
            partition_input_tokens: Vec::new(),
            sink: DmlWritePlanInput(
                crate::planner::distributed::write::contract::test_support::simple_sql_write_plan_input(
                    crate::plan_read::ConnectorWriteInputBinding::RootOutputByOrdinal,
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
        )
        .expect_err("missing target must fail");
        assert!(
            missing.contains("missing=[1], extraneous=[]"),
            "unexpected error: {missing}"
        );

        let duplicate = plan_change_stream_writer_statistics(
            &[statistics_route(0)],
            vec![empty_statistics_target(0), empty_statistics_target(0)],
            functions,
        )
        .expect_err("duplicate target must fail");
        assert!(
            duplicate.contains("repeat write target 0"),
            "unexpected error: {duplicate}"
        );

        let extraneous = plan_change_stream_writer_statistics(
            &[statistics_route(0)],
            vec![empty_statistics_target(0), empty_statistics_target(1)],
            functions,
        )
        .expect_err("extraneous target must fail");
        assert!(
            extraneous.contains("missing=[], extraneous=[1]"),
            "unexpected error: {extraneous}"
        );
    }

    #[test]
    fn change_stream_statistics_accepts_explicit_empty_requirements_for_every_route() {
        let auxiliary = plan_change_stream_writer_statistics(
            &[statistics_route(0), statistics_route(1)],
            vec![empty_statistics_target(0), empty_statistics_target(1)],
            crate::functions::builtin_sql_function_catalog(),
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
        use novarocks_functions::{
            AggregateOverloadMetadata, EngineFunctionCatalogBuilder, FunctionDefinition,
            FunctionVisibility, FunctionVolatility,
        };
        use novarocks_spi::connector::{
            StatisticsArtifactIdentity, StatisticsRequiredAggregation, StatisticsScanColumn,
        };

        let definition = FunctionDefinition::try_new_exact_aggregate(
            "$test_change_stream_blob",
            FunctionVisibility::Hidden,
            FunctionVolatility::Immutable,
            [AggregateOverloadMetadata::try_new(
                "test/change-stream-blob/i64/v1",
                [arrow::datatypes::DataType::Int64],
                arrow::datatypes::DataType::Binary,
                arrow::datatypes::DataType::Binary,
                "test/change-stream-blob-state/v1",
            )
            .expect("aggregate overload")],
        )
        .expect("aggregate definition");
        let mut functions = EngineFunctionCatalogBuilder::new();
        functions.register(definition).expect("register aggregate");
        let functions = functions.seal().expect("function catalog");
        let requirement = |target: u32| {
            StatisticsRequiredAggregation::try_new(
                StatisticsScanColumn::try_new(
                    0,
                    "order_id",
                    arrow::datatypes::DataType::Int64,
                    false,
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
}
