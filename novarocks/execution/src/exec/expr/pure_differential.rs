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

//! Test-only differential harness: pure kernel catalog vs legacy `ExprArena`.
//!
//! Every pure owner lane must reproduce the legacy evaluator exactly. This
//! module turns that obligation into one call per case:
//!
//! - [`assert_scalar_matches_v1`] resolves a scalar call exactly as SQL does
//!   (`builtin_engine_function_catalog().resolve_bound_user`, the same entry
//!   `SqlFunctionCatalog::resolve_scalar_binding` uses), prepares the selected
//!   overload through the catalog's own pure attachment
//!   (`prepare_fresh_selected`), and evaluates the legacy path the way the
//!   native adapter lowers it: an `ExprNode::FunctionCall` whose legacy kind is
//!   looked up by the canonical wire name of the resolved function identity,
//!   over slot references and literal nodes, typed with the frozen result.
//! - [`aggregate::assert_aggregate_matches_v1`] does the same for plain grouped
//!   aggregation, single phase and Partial -> Final over several partitions.
//! - A selected overload without an installed pure owner is reported as
//!   [`DifferentialFailure::MissingPureImplementation`] with its overload id,
//!   so lanes can use the harness as a to-do list; [`pure_owner_inventory`]
//!   lists every such overload of the builtin catalogue.
//!
//! # Error rule (UEA-5E E-D13 and the required-error oracle)
//!
//! A single call over a fixed input with every selected row demanded has no
//! legal narrowing: every selected row is required for the output. E-D13 then
//! leaves exactly one admissible relation between the two paths, which the
//! harness checks per selection (all rows, then random sparse selections):
//!
//! 1. Legacy batch succeeds => the pure kernel reports no row error, and the
//!    result type, NULLs and values are equal row for row. A pure row error
//!    here would be a newly visible data error, which E-D13 forbids.
//! 2. Legacy batch fails => the pure kernel reports at least one row error (a
//!    required error must propagate; a swallowed error is the "all errors
//!    eaten" implementation the required-error oracle exists to catch). The
//!    legacy error is then attributed with single-row legacy probes - the
//!    "local expression row-mask probe" the spec admits for a fixed input and
//!    demand domain: a probe fails <=> the pure kernel reports an error at that
//!    row, and successful probes must equal the pure value. A batch failure
//!    that no single-row probe reproduces is reported as unattributable.
//! 3. Rows outside a sparse selection are not demanded: their data errors are
//!    eliminable and must not appear. The legacy reference for a sparse
//!    selection is therefore the legacy call over only the gathered rows.
//!
//! With [`ErrorMessageCheck::LegacyContainsPure`] (the default) each attributed
//! legacy diagnostic must also contain the pure row error message, as the
//! original arithmetic oracle requires. Outer pure failures (control,
//! resource, internal or invalid-program) are never data errors and always
//! fail the comparison. Aggregates have no row-error channel (owners declare
//! `NotRowEvaluated`), so the aggregate harness requires the same outcome
//! class: both succeed with equal results, or both fail with an operational
//! data failure.
//!
//! # Usage
//!
//! The name is the one SQL resolves (after analyzer rewrites), and columns are
//! supplied with the selected, already-coerced argument types; a coercing
//! overload is refused with [`DifferentialFailure::CoercedArgument`] naming the
//! type to supply. A lane test is typically:
//!
//! ```text
//! let utf8 = FunctionValueType::new(DataType::Utf8, true);
//! let values = InputGenerator::new(seed).column(&utf8, 256, &InputProfile::default());
//! assert_scalar_matches_v1(ScalarDiffSpec::new("unhex").typed_column(utf8, values));
//! assert_aggregate_matches_v1(
//!     AggregateDiffSpec::new("avg").column(values).grouped(group_ids, groups),
//! );
//! ```

pub(crate) mod aggregate;
pub(crate) mod generate;
pub(crate) mod temporal;

use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ArrayRef, AsArray, Int8Array, UInt64Array};
use arrow::compute::take;
use arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, Schema,
};
use arrow::record_batch::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use novarocks_functions::builtin::catalogue::builtin_engine_function_catalog;
use novarocks_functions::{
    CallArgumentUses, CallEffectInput, ConstantPolicy, ConstantPool, ConstantValue,
    EngineFunctionCatalog, EvaluatedArgument, FunctionArgument, FunctionArgumentType,
    FunctionBindingRequest, FunctionId, FunctionKind as CatalogKind, FunctionOverloadId,
    FunctionResultType, FunctionSpecializationFailure, KernelEvaluationControl, KernelFailure,
    PreparedPureKernel, PreparedScalarKernel, PureCallPreparation, PureKernelAbi,
    ResolvedFunctionBinding, RowDataError, ScalarEvaluationInstance, ScopedExpressionEffects,
    Selection,
};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationDomainId, ExpressionEffectContext, ExpressionUseId, FunctionValueType,
    PureCompileControl, SemanticParameterId, SemanticParameterKey, SemanticParameterRef,
    SemanticParameterValue, SemanticParameters, ValueLogicalType,
};
use novarocks_types::SlotId;

use self::generate::InputGenerator;
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{FunctionKind as LegacyKind, lookup_function};
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use crate::runtime::runtime_state::RuntimeErrorState;

/// Mismatch lines retained per selection; the count is always reported.
const MAX_REPORTED_MISMATCHES: usize = 12;
/// Slot of the anchor column that sizes an all-constant legacy chunk.
const ANCHOR_SLOT: u32 = 60_000;

// ---------------------------------------------------------------------------
// Controls and constants
// ---------------------------------------------------------------------------

/// Unbounded compile and evaluation control. Waiting returns immediately so a
/// sleeping kernel cannot stall a differential run.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct HarnessControl;

impl PureCompileControl for HarnessControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

impl KernelEvaluationControl for HarnessControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }

    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        Ok(())
    }
}

/// Generous admission limits for harness constants.
pub(crate) fn constant_policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 4096,
        max_logical_elements: 1_000_000,
        max_retained_buffer_bytes: 4_194_304,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1_048_576,
        max_library_validation_work: 16_777_216,
        max_library_validation_bytes: 16_777_216,
    }
}

/// Admit one SQL constant from a one-row array of exactly `value_type`.
pub(crate) fn constant(value_type: FunctionValueType, single_row: ArrayRef) -> ConstantValue {
    assert_eq!(single_row.len(), 1, "a constant is authored from one row");
    let field = Arc::new(
        value_type
            .try_to_field("constant")
            .expect("constant value type is valid"),
    );
    ConstantPool::try_new(
        field,
        value_type,
        single_row.to_data(),
        constant_policy(),
        CompilePhase::FunctionSpecialization,
        &HarnessControl,
    )
    .expect("constant pool admits the authored value")
    .value(0)
    .expect("constant pool row 0 exists")
}

pub(crate) fn harness_context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(0),
        domain: EvaluationDomainId::new(0),
        demand: EvaluationDemand::Value,
    }
}

// ---------------------------------------------------------------------------
// Specification
// ---------------------------------------------------------------------------

/// One call argument: an evaluated column, or a SQL constant that is offered
/// to overload resolution (as `FunctionArgument::Value { constant: Some }`),
/// lowered as a legacy literal node and evaluated as `EvaluatedArgument::Constant`.
#[derive(Clone, Debug)]
pub(crate) enum DiffArgument {
    Column {
        value_type: FunctionValueType,
        values: ArrayRef,
    },
    Constant(ConstantValue),
}

impl DiffArgument {
    pub(crate) fn value_type(&self) -> &FunctionValueType {
        match self {
            Self::Column { value_type, .. } => value_type,
            Self::Constant(value) => value.value_type(),
        }
    }

    pub(crate) fn request(&self) -> FunctionArgument {
        match self {
            Self::Column { value_type, .. } => FunctionArgument::Value {
                value_type: value_type.clone(),
                constant: None,
            },
            Self::Constant(value) => FunctionArgument::Value {
                value_type: value.value_type().clone(),
                constant: Some(value.clone()),
            },
        }
    }

    pub(crate) fn evaluated(&self) -> EvaluatedArgument<'_> {
        match self {
            Self::Column { values, .. } => EvaluatedArgument::Column(values),
            Self::Constant(value) => EvaluatedArgument::Constant(value),
        }
    }
}

/// Statement semantics shared by both paths. `sql_mode` contributes two
/// independent facts: ALLOW_THROW_EXCEPTION (a statement parameter, and the
/// legacy arena flag) and ERROR_IF_OVERFLOW (the per-call decimal overflow
/// policy). The semantic parameter table has no separate sql_mode key.
#[derive(Clone, Debug)]
pub(crate) struct DiffSemantics {
    pub allow_throw_exception: bool,
    pub decimal_overflow_policy: DecimalOverflowPolicy,
    /// Session time zone; when present it is both a statement parameter and
    /// the legacy arena session zone.
    pub time_zone: Option<String>,
    /// Further statement parameters (statement start, group_concat settings).
    pub extra: Vec<SemanticParameterValue>,
}

impl Default for DiffSemantics {
    fn default() -> Self {
        Self {
            allow_throw_exception: false,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            time_zone: None,
            extra: Vec::new(),
        }
    }
}

impl DiffSemantics {
    /// The statement parameter table with ids 1.. in insertion order.
    fn parameter_table(
        &self,
    ) -> Result<
        (
            SemanticParameters,
            Vec<(SemanticParameterKey, SemanticParameterId)>,
        ),
        String,
    > {
        let mut values = vec![SemanticParameterValue::AllowThrowException(
            self.allow_throw_exception,
        )];
        if let Some(zone) = &self.time_zone {
            values.push(SemanticParameterValue::TimeZone(zone.as_str().into()));
        }
        values.extend(self.extra.iter().cloned());
        let mut keys = Vec::with_capacity(values.len());
        let mut entries = Vec::with_capacity(values.len());
        for (index, value) in values.into_iter().enumerate() {
            let key = value.key();
            if keys.iter().any(|(existing, _)| *existing == key) {
                return Err(format!("semantic parameter {key:?} is specified twice"));
            }
            let id = SemanticParameterId::new(index as u32 + 1);
            keys.push((key, id));
            entries.push((id, value));
        }
        let table = SemanticParameters::try_new(entries).map_err(|error| error.to_string())?;
        Ok((table, keys))
    }

    /// Exact references to the parameters an owner declares it depends on.
    fn environment(
        &self,
        keys: &[(SemanticParameterKey, SemanticParameterId)],
        dependencies: &[SemanticParameterKey],
    ) -> Result<Vec<SemanticParameterRef>, DifferentialFailure> {
        dependencies
            .iter()
            .map(|key| {
                keys.iter()
                    .find(|(candidate, _)| candidate == key)
                    .map(|(_, id)| SemanticParameterRef {
                        id: *id,
                        expected_key: *key,
                    })
                    .ok_or(DifferentialFailure::MissingSemanticParameter(*key))
            })
            .collect()
    }

    fn group_concat_max_len(&self) -> Option<i64> {
        self.extra.iter().find_map(|value| match value {
            SemanticParameterValue::GroupConcatMaxLen(value) => Some(*value),
            _ => None,
        })
    }
}

/// Float equality. `Exact` compares IEEE bits (so -0.0 differs from 0.0)
/// except that any NaN equals any NaN, because the payload is not observable.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum FloatComparison {
    Exact,
    Tolerance { absolute: f64, relative: f64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ErrorMessageCheck {
    /// Each attributed legacy diagnostic contains the pure row message.
    LegacyContainsPure,
    /// Only the error rows are compared.
    Ignore,
}

/// How constants reach the legacy arena. The native adapter lowers a SQL
/// literal to `ExprNode::Literal`; a carrier without a literal form (for
/// example a timestamp) falls back to the typed constant-pool node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LegacyConstantForm {
    Literal,
    Pool,
}

/// One scalar differential case. Build with [`ScalarDiffSpec::new`].
#[derive(Clone, Debug)]
pub(crate) struct ScalarDiffSpec {
    pub name: String,
    pub arguments: Vec<DiffArgument>,
    /// Row count for an all-constant call; ignored when a column is present.
    pub constant_rows: usize,
    pub semantics: DiffSemantics,
    pub expected_result_type: Option<FunctionValueType>,
    pub float_comparison: FloatComparison,
    pub error_messages: ErrorMessageCheck,
    pub legacy_constants: LegacyConstantForm,
    /// Random sparse selections checked after the full selection.
    pub sparse_selections: usize,
    pub selection_seed: u64,
    /// Exact original source expression; binding arguments remain the resolved
    /// normal call channels. The compiled host never reuses their cached values.
    pub temporal_source: Option<temporal::SourceExpression>,
}

impl ScalarDiffSpec {
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            arguments: Vec::new(),
            constant_rows: 4,
            semantics: DiffSemantics::default(),
            expected_result_type: None,
            float_comparison: FloatComparison::Exact,
            error_messages: ErrorMessageCheck::LegacyContainsPure,
            legacy_constants: LegacyConstantForm::Literal,
            sparse_selections: 3,
            selection_seed: 0x5EED,
            temporal_source: None,
        }
    }

    /// A nullable Physical column of the array's own carrier type.
    pub(crate) fn column(self, values: ArrayRef) -> Self {
        let value_type = FunctionValueType::new(values.data_type().clone(), true);
        self.typed_column(value_type, values)
    }

    pub(crate) fn typed_column(mut self, value_type: FunctionValueType, values: ArrayRef) -> Self {
        assert!(
            novarocks_type_contract::arrow_data_types_exact(
                values.data_type(),
                &value_type.data_type
            ),
            "column carrier {:?} differs from its declared type {:?}",
            values.data_type(),
            value_type.data_type
        );
        self.arguments
            .push(DiffArgument::Column { value_type, values });
        self
    }

    pub(crate) fn constant(mut self, value: ConstantValue) -> Self {
        self.arguments.push(DiffArgument::Constant(value));
        self
    }

    /// A constant authored from a one-row array; nullable iff the row is NULL.
    pub(crate) fn constant_array(self, single_row: ArrayRef) -> Self {
        let value_type = FunctionValueType::new(
            single_row.data_type().clone(),
            single_row
                .logical_nulls()
                .is_some_and(|nulls| nulls.is_null(0)),
        );
        self.constant(constant(value_type, single_row))
    }

    pub(crate) fn constant_rows(mut self, rows: usize) -> Self {
        self.constant_rows = rows;
        self
    }

    pub(crate) fn semantics(mut self, semantics: DiffSemantics) -> Self {
        self.semantics = semantics;
        self
    }

    pub(crate) fn allow_throw_exception(mut self, allow: bool) -> Self {
        self.semantics.allow_throw_exception = allow;
        self
    }

    pub(crate) fn decimal_overflow(mut self, policy: DecimalOverflowPolicy) -> Self {
        self.semantics.decimal_overflow_policy = policy;
        self
    }

    pub(crate) fn time_zone(mut self, zone: impl Into<String>) -> Self {
        self.semantics.time_zone = Some(zone.into());
        self
    }

    pub(crate) fn parameter(mut self, value: SemanticParameterValue) -> Self {
        self.semantics.extra.push(value);
        self
    }

    pub(crate) fn expect_result_type(mut self, expected: FunctionValueType) -> Self {
        self.expected_result_type = Some(expected);
        self
    }

    pub(crate) fn float_comparison(mut self, comparison: FloatComparison) -> Self {
        self.float_comparison = comparison;
        self
    }

    pub(crate) fn ignore_error_messages(mut self) -> Self {
        self.error_messages = ErrorMessageCheck::Ignore;
        self
    }

    pub(crate) fn legacy_constants(mut self, form: LegacyConstantForm) -> Self {
        self.legacy_constants = form;
        self
    }

    pub(crate) fn sparse_selections(mut self, count: usize, seed: u64) -> Self {
        self.sparse_selections = count;
        self.selection_seed = seed;
        self
    }

    pub(crate) fn temporal_source(mut self, source: temporal::SourceExpression) -> Self {
        self.temporal_source = Some(source);
        self
    }

    fn rows(&self) -> Result<usize, DifferentialFailure> {
        let mut rows = None;
        for argument in &self.arguments {
            if let DiffArgument::Column { values, .. } = argument {
                match rows {
                    None => rows = Some(values.len()),
                    Some(existing) if existing != values.len() => {
                        return Err(DifferentialFailure::InvalidSpec(format!(
                            "argument columns have different lengths {existing} and {}",
                            values.len()
                        )));
                    }
                    Some(_) => {}
                }
            }
        }
        Ok(rows.unwrap_or(self.constant_rows))
    }
}

// ---------------------------------------------------------------------------
// Outcomes
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub(crate) struct ScalarDiffSummary {
    pub function: FunctionId,
    pub overload: FunctionOverloadId,
    pub legacy_name: String,
    pub result_type: FunctionValueType,
    pub rows: usize,
    /// Full selection plus each sparse selection.
    pub selections: usize,
    /// Selections whose legacy batch failed and was attributed row by row.
    pub legacy_batch_errors: usize,
    /// Pure row errors matched to single-row legacy failures.
    pub attributed_row_errors: usize,
    /// Rows whose equal result was a successful SQL NULL.
    pub null_results: usize,
}

/// Whether the legacy side has an implementation the harness can call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum LegacyStatus {
    Available,
    Unavailable(String),
}

#[derive(Clone, Debug)]
pub(crate) enum DifferentialFailure {
    InvalidSpec(String),
    /// SQL overload resolution refused the call.
    Resolution {
        name: String,
        error: String,
    },
    /// The selected overload coerces an argument; the spec must supply the
    /// already-coerced type, as SQL inserts the coercion before the call.
    CoercedArgument {
        position: usize,
        supplied: FunctionValueType,
        selected: String,
    },
    ResultTypePin {
        expected: FunctionValueType,
        selected: String,
    },
    /// The to-do item for pure owner lanes.
    MissingPureImplementation {
        name: String,
        function: FunctionId,
        overload: FunctionOverloadId,
        legacy: LegacyStatus,
    },
    UnsupportedPureAbi {
        overload: FunctionOverloadId,
        abi: String,
    },
    MissingSemanticParameter(SemanticParameterKey),
    Specialization(String),
    LegacyUnavailable {
        name: String,
        reason: String,
    },
    Mismatch {
        name: String,
        overload: FunctionOverloadId,
        details: Vec<String>,
    },
}

impl DifferentialFailure {
    pub(crate) fn missing_overload(&self) -> Option<&FunctionOverloadId> {
        match self {
            Self::MissingPureImplementation { overload, .. } => Some(overload),
            _ => None,
        }
    }
}

impl fmt::Display for DifferentialFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSpec(message) => write!(f, "invalid differential spec: {message}"),
            Self::Resolution { name, error } => {
                write!(f, "SQL resolution of `{name}` failed: {error}")
            }
            Self::CoercedArgument {
                position,
                supplied,
                selected,
            } => write!(
                f,
                "argument {position} supplied as {supplied:?} but the selected overload binds {selected}; \
                 supply the coerced type"
            ),
            Self::ResultTypePin { expected, selected } => write!(
                f,
                "selected result type {selected} differs from the pinned {expected:?}"
            ),
            Self::MissingPureImplementation {
                name,
                function,
                overload,
                legacy,
            } => write!(
                f,
                "MissingPureImplementation: `{name}` ({}) overload `{}` has no installed pure owner; legacy: {legacy:?}",
                function.as_str(),
                overload.as_str()
            ),
            Self::UnsupportedPureAbi { overload, abi } => write!(
                f,
                "overload `{}` is installed with ABI {abi}, which this harness does not drive",
                overload.as_str()
            ),
            Self::MissingSemanticParameter(key) => write!(
                f,
                "the pure owner depends on semantic parameter {key:?}; add it to the spec"
            ),
            Self::Specialization(message) => write!(f, "pure specialization failed: {message}"),
            Self::LegacyUnavailable { name, reason } => {
                write!(f, "legacy path cannot evaluate `{name}`: {reason}")
            }
            Self::Mismatch {
                name,
                overload,
                details,
            } => {
                writeln!(
                    f,
                    "pure `{name}` overload `{}` differs from legacy:",
                    overload.as_str()
                )?;
                for detail in details {
                    writeln!(f, "  - {detail}")?;
                }
                Ok(())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Scalar entry points
// ---------------------------------------------------------------------------

/// Run every selection of `spec` and panic with a complete report on any
/// difference, including a missing pure owner.
#[track_caller]
pub(crate) fn assert_scalar_matches_v1(spec: ScalarDiffSpec) -> ScalarDiffSummary {
    match run_scalar_differential(&spec) {
        Ok(summary) => summary,
        Err(failure) => panic!("{failure}"),
    }
}

pub(crate) fn run_scalar_differential(
    spec: &ScalarDiffSpec,
) -> Result<ScalarDiffSummary, DifferentialFailure> {
    let rows = spec.rows()?;
    let catalog = builtin_engine_function_catalog();
    let bound = resolve_like_sql(catalog, &spec.name, CatalogKind::Scalar, &spec.arguments)?;
    let legacy_name = legacy_wire_name(&bound.function_id);
    let legacy_kind = legacy_name
        .as_ref()
        .ok()
        .and_then(|legacy_name| scalar_legacy_implementation(legacy_name));
    let legacy_status = match (&legacy_name, legacy_kind) {
        (Ok(_), Some(_)) => LegacyStatus::Available,
        (Ok(legacy_name), None) => {
            LegacyStatus::Unavailable(format!("no legacy function kind for `{legacy_name}`"))
        }
        (Err(failure), _) => LegacyStatus::Unavailable(failure.to_string()),
    };
    // A missing owner is reported before any other requirement of the spec.
    let (abi, dependencies) = installed_declaration(
        catalog,
        &spec.name,
        &bound,
        CatalogKind::Scalar,
        legacy_status,
    )?;
    let legacy_name = legacy_name?;
    require_selected_arguments(&spec.name, &bound, &spec.arguments)?;
    let FunctionResultType::Scalar(result_type) = bound.selected.result_type.clone() else {
        return Err(DifferentialFailure::InvalidSpec(format!(
            "`{}` selected a relation result",
            spec.name
        )));
    };
    if let Some(expected) = &spec.expected_result_type
        && expected != &result_type
    {
        return Err(DifferentialFailure::ResultTypePin {
            expected: expected.clone(),
            selected: format!("{result_type:?}"),
        });
    }
    if abi == PureKernelAbi::ControlIntrinsicV1 {
        return temporal::run(spec, &bound, &legacy_name, legacy_kind, rows, &result_type);
    }
    if !matches!(
        abi,
        PureKernelAbi::ScalarV1 | PureKernelAbi::ScalarInvocationV1
    ) {
        return Err(DifferentialFailure::UnsupportedPureAbi {
            overload: bound.selected.overload.clone(),
            abi: format!("{abi:?}"),
        });
    }
    let Some(legacy_kind) = legacy_kind else {
        return Err(DifferentialFailure::LegacyUnavailable {
            name: legacy_name,
            reason: "lookup_function has no legacy kind".into(),
        });
    };
    if matches!(
        bound.function_id.as_str(),
        "builtin.scalar/regexp_count/v1" | "builtin.scalar/to_base64/v1"
    ) {
        return temporal::run_native_source_scalar(
            spec,
            &bound,
            &legacy_name,
            legacy_kind,
            rows,
            &result_type,
        );
    }
    let prepared = prepare_scalar(catalog, spec, &bound, &dependencies)?;
    let call = ScalarCall {
        spec,
        legacy_kind,
        result_type: &result_type,
        rows,
    };
    let mut summary = ScalarDiffSummary {
        function: bound.function_id.clone(),
        overload: bound.selected.overload.clone(),
        legacy_name,
        result_type: result_type.clone(),
        rows,
        selections: 0,
        legacy_batch_errors: 0,
        attributed_row_errors: 0,
        null_results: 0,
    };
    let mut details = Vec::new();
    call.check_selection(&prepared, None, "all rows", &mut summary, &mut details);
    if rows > 1 {
        let mut generator = InputGenerator::new(spec.selection_seed);
        for index in 0..spec.sparse_selections {
            let selected = generator.selection(rows, 0.5);
            let label = format!(
                "sparse selection #{index} ({} of {rows} rows)",
                selected.len()
            );
            call.check_selection(
                &prepared,
                Some(&selected),
                &label,
                &mut summary,
                &mut details,
            );
        }
    }
    if details.is_empty() {
        Ok(summary)
    } else {
        Err(DifferentialFailure::Mismatch {
            name: spec.name.clone(),
            overload: bound.selected.overload,
            details,
        })
    }
}

/// Resolution and owner status only: the cheap to-do probe for a call shape.
pub(crate) fn scalar_pure_owner_status(
    name: &str,
    arguments: &[DiffArgument],
) -> Result<(FunctionId, FunctionOverloadId), DifferentialFailure> {
    let catalog = builtin_engine_function_catalog();
    let bound = resolve_like_sql(catalog, name, CatalogKind::Scalar, arguments)?;
    let legacy = legacy_wire_name(&bound.function_id)
        .ok()
        .and_then(|legacy| scalar_legacy_implementation(&legacy).map(|_| ()))
        .map_or_else(
            || LegacyStatus::Unavailable("no legacy function kind".into()),
            |_| LegacyStatus::Available,
        );
    installed_declaration(catalog, name, &bound, CatalogKind::Scalar, legacy)?;
    Ok((bound.function_id, bound.selected.overload))
}

/// One row of [`pure_owner_inventory`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OwnerInventoryRow {
    pub name: String,
    pub kind: CatalogKind,
    pub overload: FunctionOverloadId,
    pub installed: bool,
}

/// Every declared overload of the builtin catalogue with its pure owner
/// status, in catalogue order. Definitions without a binding declaration have
/// no overload identity and are omitted.
pub(crate) fn pure_owner_inventory() -> Vec<OwnerInventoryRow> {
    let catalog = builtin_engine_function_catalog();
    let mut rows = Vec::new();
    for definition in catalog.definitions() {
        let Some(declaration) = definition.binding_declaration() else {
            continue;
        };
        for overload in declaration.overloads() {
            let installed = catalog
                .pure_overload_declaration_observed(
                    declaration.function_id(),
                    definition.kind(),
                    &overload.identity,
                    &HarnessControl,
                )
                .is_ok();
            rows.push(OwnerInventoryRow {
                name: definition.canonical_name().to_string(),
                kind: definition.kind(),
                overload: overload.identity.clone(),
                installed,
            });
        }
    }
    rows
}

// ---------------------------------------------------------------------------
// Shared resolution and preparation
// ---------------------------------------------------------------------------

/// Resolve exactly as `SqlFunctionCatalog::resolve_scalar_binding` (and its
/// aggregate twin) does: user visibility, no expected result type, one
/// logical argument per supplied argument.
pub(crate) fn resolve_like_sql(
    catalog: &EngineFunctionCatalog,
    name: &str,
    kind: CatalogKind,
    arguments: &[DiffArgument],
) -> Result<ResolvedFunctionBinding, DifferentialFailure> {
    let request_arguments = arguments
        .iter()
        .map(DiffArgument::request)
        .collect::<Vec<_>>();
    catalog
        .resolve_bound_user(
            name,
            kind,
            FunctionBindingRequest {
                expected_result_type: None,
                arguments: &request_arguments,
                logical_argument_count: request_arguments.len(),
            },
            &HarnessControl,
        )
        .map_err(|error| DifferentialFailure::Resolution {
            name: name.to_string(),
            error: error.to_string(),
        })
}

/// SQL materializes argument coercions before the call, so both paths must
/// receive the selected argument types; a spec supplies them already coerced.
pub(crate) fn require_selected_arguments(
    name: &str,
    bound: &ResolvedFunctionBinding,
    arguments: &[DiffArgument],
) -> Result<(), DifferentialFailure> {
    if bound.selected.argument_types.len() != arguments.len() {
        return Err(DifferentialFailure::InvalidSpec(format!(
            "`{name}` selected {} argument channels for {} arguments",
            bound.selected.argument_types.len(),
            arguments.len()
        )));
    }
    for (position, (selected, argument)) in bound
        .selected
        .argument_types
        .iter()
        .zip(arguments)
        .enumerate()
    {
        let exact = matches!(selected, FunctionArgumentType::Value(value) if value == argument.value_type());
        if !exact {
            return Err(DifferentialFailure::CoercedArgument {
                position,
                supplied: argument.value_type().clone(),
                selected: format!("{selected:?}"),
            });
        }
    }
    Ok(())
}

/// The native wire v1 name of a resolved identity, exactly as plan-codec
/// derives it (`builtin.<kind>/<name>/v1` or `parametric.<kind>/<name>/v1`).
pub(crate) fn legacy_wire_name(function: &FunctionId) -> Result<String, DifferentialFailure> {
    let identity = function.as_str();
    identity
        .strip_prefix("builtin.")
        .or_else(|| identity.strip_prefix("parametric."))
        .and_then(|identity| identity.split_once('/').map(|(_, rest)| rest))
        .and_then(|identity| identity.strip_suffix("/v1"))
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .ok_or_else(|| DifferentialFailure::LegacyUnavailable {
            name: identity.to_string(),
            reason: "identity has no native wire v1 name".into(),
        })
}

/// The installed ABI and declared environment dependencies of the selected
/// overload, or the missing-owner to-do item.
pub(crate) fn installed_declaration(
    catalog: &EngineFunctionCatalog,
    name: &str,
    bound: &ResolvedFunctionBinding,
    kind: CatalogKind,
    legacy: LegacyStatus,
) -> Result<(PureKernelAbi, Vec<SemanticParameterKey>), DifferentialFailure> {
    match catalog.pure_overload_declaration_observed(
        &bound.function_id,
        kind,
        &bound.selected.overload,
        &HarnessControl,
    ) {
        Ok(declaration) => Ok((
            declaration.implementation().abi,
            declaration.effects().environment_dependencies.to_vec(),
        )),
        Err(FunctionSpecializationFailure::MissingPureImplementation(overload)) => {
            Err(DifferentialFailure::MissingPureImplementation {
                name: name.to_string(),
                function: bound.function_id.clone(),
                overload,
                legacy,
            })
        }
        Err(other) => Err(DifferentialFailure::Specialization(other.to_string())),
    }
}

pub(crate) fn specialization_failure(
    name: &str,
    bound: &ResolvedFunctionBinding,
    failure: FunctionSpecializationFailure,
) -> DifferentialFailure {
    match failure {
        FunctionSpecializationFailure::MissingPureImplementation(overload) => {
            DifferentialFailure::MissingPureImplementation {
                name: name.to_string(),
                function: bound.function_id.clone(),
                overload,
                legacy: LegacyStatus::Unavailable("not probed".into()),
            }
        }
        other => DifferentialFailure::Specialization(other.to_string()),
    }
}

enum PreparedScalarDiff {
    Kernel(Arc<dyn PreparedScalarKernel>),
    Invocation(Arc<dyn novarocks_functions::PreparedInvocationScalarKernel>),
}
impl PreparedScalarDiff {
    fn contract(&self) -> &Arc<novarocks_functions::ScalarCallContract> {
        match self {
            Self::Kernel(p) => p.contract(),
            Self::Invocation(p) => p.contract(),
        }
    }
}
enum PureScalarOutcome {
    Selected(PureSelection),
    InvocationData(novarocks_functions::ScalarInvocationData),
}

fn prepare_scalar(
    catalog: &EngineFunctionCatalog,
    spec: &ScalarDiffSpec,
    bound: &ResolvedFunctionBinding,
    dependencies: &[SemanticParameterKey],
) -> Result<PreparedScalarDiff, DifferentialFailure> {
    let (parameters, keys) = spec
        .semantics
        .parameter_table()
        .map_err(DifferentialFailure::InvalidSpec)?;
    let environment = spec.semantics.environment(&keys, dependencies)?;
    let request_arguments = spec
        .arguments
        .iter()
        .map(DiffArgument::request)
        .collect::<Vec<_>>();
    let declaration = catalog
        .pure_overload_declaration_observed(
            &bound.function_id,
            CatalogKind::Scalar,
            &bound.selected.overload,
            &HarnessControl,
        )
        .map_err(|failure| specialization_failure(&spec.name, bound, failure))?;
    let selected_control = declaration
        .selected_argument_control_observed(
            &bound.function_id,
            &bound.selected,
            request_arguments.len(),
            &HarnessControl,
        )
        .map_err(|error| specialization_failure(&spec.name, bound, error.into()))?;
    let no_arguments = selected_control == novarocks_type_contract::ArgumentControl::NoArguments;
    let uses = (0..spec.arguments.len())
        .map(|index| {
            if no_arguments {
                None
            } else {
                Some(ExpressionUseId::new(index as u32 + 1))
            }
        })
        .collect::<Vec<_>>();
    let selected = Arc::new(bound.selected.clone());
    let context = harness_context();
    let input = CallEffectInput {
        context,
        argument_uses: CallArgumentUses::SelectedChannels(&uses),
        function_id: &bound.function_id,
        kind: CatalogKind::Scalar,
        selected: selected.as_ref(),
        request: FunctionBindingRequest {
            expected_result_type: None,
            arguments: &request_arguments,
            logical_argument_count: request_arguments.len(),
        },
        environment: &environment,
        parameters: &parameters,
        decimal_overflow_policy: spec.semantics.decimal_overflow_policy,
        proof_scope: CallProofScope::Unconditional,
    };
    let specialization = catalog
        .prepare_fresh_selected(
            input,
            Arc::clone(&selected),
            PureCallPreparation::Scalar {
                arguments: ScopedExpressionEffects::pure_value(context),
            },
            &HarnessControl,
        )
        .map_err(|failure| specialization_failure(&spec.name, bound, failure))?;
    match specialization.into_prepared() {
        PreparedPureKernel::Scalar(kernel) => Ok(PreparedScalarDiff::Kernel(kernel)),
        PreparedPureKernel::ScalarInvocation(kernel) => Ok(PreparedScalarDiff::Invocation(kernel)),
        _ => Err(DifferentialFailure::UnsupportedPureAbi {
            overload: bound.selected.overload.clone(),
            abi: "non-scalar prepared kernel".into(),
        }),
    }
}

// ---------------------------------------------------------------------------
// Scalar evaluation and comparison
// ---------------------------------------------------------------------------

// Mirrors the native adapter's actual node producer. Array literals are
// lowered to ArrayExpr, while ordinary scalar calls use their registered kind.
#[derive(Clone, Copy)]
enum ScalarLegacyImplementation {
    Function(LegacyKind),
    ArrayLiteral,
}
fn scalar_legacy_implementation(name: &str) -> Option<ScalarLegacyImplementation> {
    if name == "__array_literal" {
        Some(ScalarLegacyImplementation::ArrayLiteral)
    } else {
        lookup_function(name).map(ScalarLegacyImplementation::Function)
    }
}
struct ScalarCall<'a> {
    spec: &'a ScalarDiffSpec,
    legacy_kind: ScalarLegacyImplementation,
    result_type: &'a FunctionValueType,
    rows: usize,
}

/// Pure outcome of one selection, with owned compact values.
pub(crate) struct PureSelection {
    pub values: ArrayRef,
    pub errors: Vec<RowDataError>,
}

impl ScalarCall<'_> {
    fn check_selection(
        &self,
        prepared: &PreparedScalarDiff,
        rows: Option<&[usize]>,
        label: &str,
        summary: &mut ScalarDiffSummary,
        details: &mut Vec<String>,
    ) {
        summary.selections += 1;
        let selected_rows = rows.map_or_else(|| (0..self.rows).collect::<Vec<_>>(), <[_]>::to_vec);
        let pure = match self.evaluate_pure(prepared, rows) {
            Ok(PureScalarOutcome::Selected(pure)) => pure,
            Ok(PureScalarOutcome::InvocationData(data)) => {
                // Whole Data is compared to the actual legacy batch, never
                // attributed to row 0 or normalized into a bounded row error.
                let correct_domain = data.batch_rows() == self.rows
                    && data.source_len() == selected_rows.len()
                    && selected_rows
                        .iter()
                        .enumerate()
                        .all(|(ordinal, row)| data.source_row(ordinal) == Some(*row))
                    && Arc::ptr_eq(data.contract(), prepared.contract());
                if !correct_domain {
                    details.push(format!(
                        "{label}: invocation Data has an unrelated source contract/domain"
                    ));
                }
                match self.evaluate_legacy(rows) {
                    Err(original)
                        if !original.starts_with(LEGACY_PANIC_PREFIX)
                            && original == data.message() =>
                    {
                        summary.legacy_batch_errors += 1;
                    }
                    Err(original) => details.push(format!(
                        "{label}: whole Data differs: legacy `{original}`, pure `{}`",
                        data.message()
                    )),
                    Ok(_) => details.push(format!(
                        "{label}: pure whole Data where the actual legacy batch succeeded: {}",
                        data.message()
                    )),
                }
                return;
            }
            Err(failure) => {
                details.push(format!("{label}: pure outer failure: {failure}"));
                return;
            }
        };
        let legacy = self.evaluate_legacy(rows);
        let mut found = Vec::new();
        let outcome = compare_selection(
            SelectionComparison {
                result_type: self.result_type,
                floats: self.spec.float_comparison,
                messages: self.spec.error_messages,
                rows: &selected_rows,
            },
            legacy,
            |row| self.evaluate_legacy(Some(&[row])),
            &pure,
            &mut found,
        );
        summary.null_results += outcome.null_results;
        summary.attributed_row_errors += outcome.attributed_row_errors;
        if outcome.legacy_batch_error {
            summary.legacy_batch_errors += 1;
        }
        let total = found.len();
        for line in found.into_iter().take(MAX_REPORTED_MISMATCHES) {
            details.push(format!("{label}: {line}"));
        }
        if total > MAX_REPORTED_MISMATCHES {
            details.push(format!(
                "{label}: ... {} further mismatches",
                total - MAX_REPORTED_MISMATCHES
            ));
        }
    }

    fn evaluate_pure(
        &self,
        prepared: &PreparedScalarDiff,
        rows: Option<&[usize]>,
    ) -> Result<PureScalarOutcome, String> {
        let arguments = if prepared.contract().value_argument_types().len() == 0 {
            Vec::new()
        } else {
            self.spec
                .arguments
                .iter()
                .map(DiffArgument::evaluated)
                .collect::<Vec<_>>()
        };
        let selection = match rows {
            None => Selection::all(self.rows),
            Some(rows) => {
                Selection::try_sparse(self.rows, rows).map_err(|error| error.to_string())?
            }
        };
        let run = catch_unwind(AssertUnwindSafe(|| {
            let host = Some(
                crate::exec::operators::compiled_aggregate::differential_aggregate_state_allocator(
                    crate::runtime::mem_tracker::MemTracker::new_root(
                        "pure-differential-scalar-state",
                    ),
                ),
            );
            let output = match prepared {
                PreparedScalarDiff::Kernel(p) => {
                    let mut instance =
                        ScalarEvaluationInstance::instantiate_with_allocator(Arc::clone(p), host)
                            .map_err(|error| error.to_string())?;
                    instance
                        .evaluate(selection, &arguments, &HarnessControl)
                        .map_err(|error| error.to_string())?
                }
                PreparedScalarDiff::Invocation(p) => {
                    let mut instance = novarocks_functions::InvocationScalarEvaluationInstance::instantiate_with_allocator(Arc::clone(p), host)
                        .map_err(|error| error.to_string())?;
                    match instance.evaluate(
                        selection,
                        &arguments,
                        novarocks_functions::ScalarInvocationActivation::Activated,
                        &HarnessControl,
                    ) {
                        Ok(output) => output,
                        Err(novarocks_functions::ScalarInvocationFailure::Data(data)) => {
                            return Ok(PureScalarOutcome::InvocationData(data));
                        }
                        Err(novarocks_functions::ScalarInvocationFailure::Kernel(cause)) => {
                            return Err(cause.to_string());
                        }
                    }
                }
            };
            let (_, values, errors) = output.into_parts();
            Ok(PureScalarOutcome::Selected(PureSelection {
                values,
                errors: errors.into_vec(),
            }))
        }));
        run.unwrap_or_else(|panic| Err(format!("pure kernel panicked: {}", panic_message(&panic))))
    }

    /// The legacy call over the given original rows (all rows when `None`).
    fn evaluate_legacy(&self, rows: Option<&[usize]>) -> Result<ArrayRef, String> {
        let run = catch_unwind(AssertUnwindSafe(|| self.evaluate_legacy_inner(rows)));
        run.unwrap_or_else(|panic| Err(format!("{LEGACY_PANIC_PREFIX}{}", panic_message(&panic))))
    }

    fn evaluate_legacy_inner(&self, rows: Option<&[usize]>) -> Result<ArrayRef, String> {
        let spec = self.spec;
        let num_rows = rows.map_or(self.rows, <[_]>::len);
        let indices =
            rows.map(|rows| UInt64Array::from_iter_values(rows.iter().map(|row| *row as u64)));
        let mut arena = ExprArena::default();
        arena.set_allow_throw_exception(spec.semantics.allow_throw_exception);
        arena.set_session_time_zone(spec.semantics.time_zone.clone());
        arena.bind_runtime_error(Arc::new(RuntimeErrorState::default()));
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        let mut slots = Vec::new();
        let mut arguments = Vec::with_capacity(spec.arguments.len());
        for (index, argument) in spec.arguments.iter().enumerate() {
            let argument = if index == 0 {
                spec.temporal_source
                    .as_ref()
                    .map_or(argument, |source| &source.input)
            } else {
                argument
            };
            let mut id = match argument {
                DiffArgument::Column { value_type, values } => {
                    let values = match (rows, &indices) {
                        (None, _) => Arc::clone(values),
                        (Some([]), _) => values.slice(0, 0),
                        (Some(rows), _)
                            if rows.iter().enumerate().all(|(ordinal, row)| {
                                rows[0].checked_add(ordinal) == Some(*row)
                            }) =>
                        {
                            // Keep the original carrier for a contiguous host chunk.
                            // An unnecessary Arrow take can reject valid retained
                            // dictionary backing before the legacy function runs.
                            values.slice(rows[0], rows.len())
                        }
                        (_, Some(indices)) => take(values.as_ref(), indices, None)
                            .map_err(|error| format!("harness gather failed: {error}"))?,
                        (Some(_), None) => unreachable!("selection indices authored above"),
                    };
                    let slot = SlotId::new(index as u32 + 1);
                    fields.push(
                        value_type
                            .try_to_field(format!("arg{index}"))
                            .map_err(|error| error.to_string())?,
                    );
                    columns.push(values);
                    slots.push(slot);
                    arena.push_typed(ExprNode::SlotId(slot), value_type.data_type.clone())
                }
                DiffArgument::Constant(value) => {
                    legacy_constant(&mut arena, value, spec.legacy_constants)
                }
            };
            if index == 0
                && let Some(source) = &spec.temporal_source
            {
                id = source.wrap_legacy(&mut arena, id, spec.semantics.decimal_overflow_policy);
            }
            arguments.push(id);
        }
        if columns.is_empty() {
            fields.push(arrow::datatypes::Field::new(
                "anchor",
                DataType::Int8,
                false,
            ));
            columns.push(Arc::new(Int8Array::from(vec![0; num_rows])) as ArrayRef);
            slots.push(SlotId::new(ANCHOR_SLOT));
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns)
            .map_err(|error| format!("harness batch failed: {error}"))?;
        let chunk_schema = ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &slots)?;
        let chunk = Chunk::new_with_chunk_schema(batch, chunk_schema);
        let node = match self.legacy_kind {
            ScalarLegacyImplementation::Function(kind) => ExprNode::FunctionCall {
                kind,
                args: arguments,
            },
            ScalarLegacyImplementation::ArrayLiteral => ExprNode::ArrayExpr {
                elements: arguments,
            },
        };
        let call = arena.push_typed(node, self.result_type.data_type.clone());
        arena.eval(call, &chunk)
    }
}

pub(crate) const LEGACY_PANIC_PREFIX: &str = "legacy evaluator panicked: ";

pub(crate) fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|text| (*text).to_string()))
        .unwrap_or_else(|| "non-string panic payload".into())
}

fn legacy_constant(
    arena: &mut ExprArena,
    value: &ConstantValue,
    form: LegacyConstantForm,
) -> ExprId {
    let data_type = value.value_type().data_type.clone();
    if form == LegacyConstantForm::Literal
        && let Some(literal) = legacy_literal(value)
    {
        return arena.push_typed(ExprNode::Literal(literal), data_type);
    }
    arena.push_typed(ExprNode::Constant(value.clone()), data_type)
}

/// The literal the native adapter would author for this constant, if any.
pub(crate) fn legacy_literal(value: &ConstantValue) -> Option<LiteralValue> {
    let array = value.pool().array();
    let row = value.ordinal() as usize;
    if array.is_null(row) {
        return Some(LiteralValue::Null);
    }
    Some(match (array.data_type(), value.value_type().logical_type) {
        (DataType::Int8, ValueLogicalType::Physical) => {
            LiteralValue::Int8(array.as_primitive::<Int8Type>().value(row))
        }
        (DataType::Int16, ValueLogicalType::Physical) => {
            LiteralValue::Int16(array.as_primitive::<Int16Type>().value(row))
        }
        (DataType::Int32, ValueLogicalType::Physical) => {
            LiteralValue::Int32(array.as_primitive::<Int32Type>().value(row))
        }
        (DataType::Int64, ValueLogicalType::Physical) => {
            LiteralValue::Int64(array.as_primitive::<Int64Type>().value(row))
        }
        (DataType::Float32, ValueLogicalType::Physical) => {
            LiteralValue::Float32(array.as_primitive::<Float32Type>().value(row))
        }
        (DataType::Float64, ValueLogicalType::Physical) => {
            LiteralValue::Float64(array.as_primitive::<Float64Type>().value(row))
        }
        (DataType::Boolean, ValueLogicalType::Physical) => {
            LiteralValue::Bool(array.as_boolean().value(row))
        }
        (DataType::Utf8, ValueLogicalType::Physical) => {
            LiteralValue::Utf8(array.as_string::<i32>().value(row).to_string())
        }
        (DataType::Binary, ValueLogicalType::Physical) => {
            LiteralValue::Binary(array.as_binary::<i32>().value(row).to_vec())
        }
        (DataType::Date32, ValueLogicalType::Physical) => {
            LiteralValue::Date32(array.as_primitive::<Date32Type>().value(row))
        }
        (DataType::Decimal128(precision, scale), ValueLogicalType::Physical) => {
            LiteralValue::Decimal128 {
                value: array.as_primitive::<Decimal128Type>().value(row),
                precision: *precision,
                scale: *scale,
            }
        }
        (DataType::Decimal256(precision, scale), ValueLogicalType::Physical) => {
            LiteralValue::Decimal256 {
                value: array.as_primitive::<Decimal256Type>().value(row),
                precision: *precision,
                scale: *scale,
            }
        }
        (DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt) => {
            let bytes: [u8; 16] = array.as_fixed_size_binary().value(row).try_into().ok()?;
            LiteralValue::LargeInt(i128::from_be_bytes(bytes))
        }
        _ => return None,
    })
}

/// Facts shared by every row of one selection comparison.
#[derive(Clone, Copy)]
pub(crate) struct SelectionComparison<'a> {
    pub result_type: &'a FunctionValueType,
    pub floats: FloatComparison,
    pub messages: ErrorMessageCheck,
    /// Original batch row of each selected ordinal.
    pub rows: &'a [usize],
}

#[derive(Debug, Default)]
pub(crate) struct SelectionOutcome {
    pub legacy_batch_error: bool,
    pub attributed_row_errors: usize,
    pub null_results: usize,
}

/// Apply the module-level error rule to one selection. `legacy_batch` is the
/// legacy call over exactly the selected rows; `legacy_row` evaluates the
/// legacy call over one original row. Mismatch lines are appended to `found`.
pub(crate) fn compare_selection(
    facts: SelectionComparison<'_>,
    legacy_batch: Result<ArrayRef, String>,
    mut legacy_row: impl FnMut(usize) -> Result<ArrayRef, String>,
    pure: &PureSelection,
    found: &mut Vec<String>,
) -> SelectionOutcome {
    let mut outcome = SelectionOutcome::default();
    let selected = facts.rows.len();
    if pure.values.len() != selected {
        found.push(format!(
            "pure produced {} values for {selected} selected rows",
            pure.values.len()
        ));
        return outcome;
    }
    let pure_error = |ordinal: usize| {
        pure.errors
            .iter()
            .find(|error| error.selected_ordinal() == ordinal)
    };
    match legacy_batch {
        Ok(legacy) => {
            if legacy.len() != selected {
                found.push(format!(
                    "legacy produced {} values for {selected} selected rows",
                    legacy.len()
                ));
                return outcome;
            }
            if !check_types(facts.result_type, &legacy, &pure.values, found) {
                return outcome;
            }
            for error in &pure.errors {
                let row = facts.rows.get(error.selected_ordinal()).copied();
                found.push(format!(
                    "row {row:?}: pure raised `{}` where the legacy batch succeeded with {}",
                    error.message(),
                    render(&legacy, error.selected_ordinal())
                ));
            }
            for ordinal in 0..selected {
                if pure_error(ordinal).is_some() {
                    continue;
                }
                match compare_value(&legacy, ordinal, &pure.values, ordinal, facts.floats) {
                    Ok(true) => outcome.null_results += 1,
                    Ok(false) => {}
                    Err(difference) => {
                        found.push(format!("row {}: {difference}", facts.rows[ordinal]))
                    }
                }
            }
        }
        Err(batch_error) => {
            outcome.legacy_batch_error = true;
            if batch_error.starts_with(LEGACY_PANIC_PREFIX) {
                found.push(batch_error);
                return outcome;
            }
            if pure.errors.is_empty() {
                found.push(format!(
                    "required error swallowed: the legacy batch failed with `{batch_error}` but pure reported no row error"
                ));
            }
            let mut attributed = 0usize;
            for (ordinal, row) in facts.rows.iter().copied().enumerate() {
                match (legacy_row(row), pure_error(ordinal)) {
                    (Err(legacy), Some(error)) => {
                        attributed += 1;
                        outcome.attributed_row_errors += 1;
                        if legacy.starts_with(LEGACY_PANIC_PREFIX) {
                            found.push(format!("row {row}: {legacy}"));
                        } else if facts.messages == ErrorMessageCheck::LegacyContainsPure
                            && !legacy.contains(error.message())
                        {
                            found.push(format!(
                                "row {row}: legacy diagnostic `{legacy}` does not contain pure row error `{}`",
                                error.message()
                            ));
                        }
                    }
                    (Err(legacy), None) => {
                        attributed += 1;
                        found.push(format!(
                            "row {row}: required error swallowed: legacy `{legacy}`, pure {}",
                            render(&pure.values, ordinal)
                        ));
                    }
                    (Ok(legacy), Some(error)) => found.push(format!(
                        "row {row}: pure raised `{}` but the single-row legacy call returned {}",
                        error.message(),
                        render(&legacy, 0)
                    )),
                    (Ok(legacy), None) => {
                        if legacy.len() != 1 {
                            found.push(format!(
                                "row {row}: single-row legacy call returned {} values",
                                legacy.len()
                            ));
                            continue;
                        }
                        if !check_types(facts.result_type, &legacy, &pure.values, found) {
                            continue;
                        }
                        match compare_value(&legacy, 0, &pure.values, ordinal, facts.floats) {
                            Ok(true) => outcome.null_results += 1,
                            Ok(false) => {}
                            Err(difference) => found.push(format!("row {row}: {difference}")),
                        }
                    }
                }
            }
            if attributed == 0 {
                found.push(format!(
                    "unattributable legacy batch error `{batch_error}`: no single-row legacy call fails"
                ));
            }
        }
    }
    outcome
}

/// Exact carrier equality, plus the non-nullable promise of the result.
fn check_types(
    result_type: &FunctionValueType,
    legacy: &ArrayRef,
    pure: &ArrayRef,
    found: &mut Vec<String>,
) -> bool {
    let mut equal = true;
    if !novarocks_type_contract::arrow_data_types_exact(pure.data_type(), &result_type.data_type) {
        found.push(format!(
            "pure carrier {:?} differs from the selected result {:?}",
            pure.data_type(),
            result_type.data_type
        ));
        equal = false;
    }
    if !novarocks_type_contract::arrow_data_types_exact(legacy.data_type(), pure.data_type()) {
        found.push(format!(
            "result type: legacy {:?}, pure {:?}",
            legacy.data_type(),
            pure.data_type()
        ));
        equal = false;
    }
    if !result_type.nullable && legacy.null_count() > 0 {
        found.push(format!(
            "legacy produced {} NULLs for a non-nullable selected result",
            legacy.null_count()
        ));
    }
    equal
}

/// `Ok(true)` for an equal NULL, `Ok(false)` for an equal value.
pub(crate) fn compare_value(
    legacy: &ArrayRef,
    legacy_row: usize,
    pure: &ArrayRef,
    pure_row: usize,
    floats: FloatComparison,
) -> Result<bool, String> {
    let differ = || {
        format!(
            "legacy {} vs pure {}",
            render(legacy, legacy_row),
            render(pure, pure_row)
        )
    };
    // NullArray has no physical validity bitmap; its carrier still denotes
    // SQL NULL at every row, including empty/all-NULL aggregate results.
    match (
        legacy.data_type() == &DataType::Null || legacy.is_null(legacy_row),
        pure.data_type() == &DataType::Null || pure.is_null(pure_row),
    ) {
        (true, true) => return Ok(true),
        (true, false) | (false, true) => return Err(differ()),
        (false, false) => {}
    }
    let equal = match (legacy.data_type(), pure.data_type()) {
        (DataType::Float64, DataType::Float64) => floats_equal(
            legacy.as_primitive::<Float64Type>().value(legacy_row),
            pure.as_primitive::<Float64Type>().value(pure_row),
            floats,
        ),
        (DataType::Float32, DataType::Float32) => floats_equal(
            f64::from(legacy.as_primitive::<Float32Type>().value(legacy_row)),
            f64::from(pure.as_primitive::<Float32Type>().value(pure_row)),
            floats,
        ),
        _ => legacy.slice(legacy_row, 1).to_data() == pure.slice(pure_row, 1).to_data(),
    };
    if equal { Ok(false) } else { Err(differ()) }
}

fn floats_equal(left: f64, right: f64, comparison: FloatComparison) -> bool {
    if left.is_nan() || right.is_nan() {
        return left.is_nan() && right.is_nan();
    }
    match comparison {
        FloatComparison::Exact => left.to_bits() == right.to_bits(),
        FloatComparison::Tolerance { absolute, relative } => {
            if left == right {
                return true;
            }
            if left.is_infinite() || right.is_infinite() {
                return false;
            }
            (left - right).abs() <= absolute + relative * left.abs().max(right.abs())
        }
    }
}

/// Human-readable value; floats keep the sign of zero and NaN visible.
pub(crate) fn render(array: &ArrayRef, row: usize) -> String {
    if row >= array.len() {
        return format!("<row {row} out of {}>", array.len());
    }
    if array.is_null(row) {
        return "NULL".into();
    }
    match array.data_type() {
        DataType::Float64 => format!("{:?}", array.as_primitive::<Float64Type>().value(row)),
        DataType::Float32 => format!("{:?}", array.as_primitive::<Float32Type>().value(row)),
        _ => ArrayFormatter::try_new(array.as_ref(), &FormatOptions::default())
            .map(|formatter| format!("{:?}", formatter.value(row).to_string()))
            .unwrap_or_else(|error| format!("<unrenderable: {error}>")),
    }
}

#[cfg(test)]
#[path = "pure_differential_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "pure_differential_a1_tests.rs"]
mod a1_tests;

#[cfg(test)]
#[path = "pure_differential_s1_tests.rs"]
mod s1_tests;

#[cfg(test)]
#[path = "pure_differential_s2_tests.rs"]
mod s2_tests;

#[path = "pure_differential_regexp_replace_tests.rs"]
mod regexp_replace_tests;

#[path = "pure_differential_distinct_numeric_tests.rs"]
mod distinct_numeric_tests;

#[path = "pure_differential_numeric_elementary_tests.rs"]
mod numeric_elementary_tests;

#[path = "pure_differential_regexp_extract_tests.rs"]
mod regexp_extract_tests;

#[path = "pure_differential/any_value_tests.rs"]
mod any_value_tests;

#[path = "pure_differential_nullif_tests.rs"]
mod nullif_tests;

#[path = "pure_differential/by_tests.rs"]
mod by_tests;

#[cfg(test)]
#[path = "pure_differential/n_tests.rs"]
mod n_tests;

#[cfg(test)]
#[path = "pure_differential_s1_parts_tests.rs"]
mod s1_parts_tests;

#[cfg(test)]
#[path = "pure_differential_count_distinct_tests.rs"]
mod count_distinct_tests;

#[cfg(test)]
#[path = "pure_differential_numeric_unary_tests.rs"]
mod numeric_unary_tests;

#[cfg(test)]
#[path = "pure_differential_extrema_tests.rs"]
mod extrema_tests;

#[cfg(test)]
#[path = "pure_differential_rounding_tests.rs"]
mod rounding_tests;

#[cfg(test)]
#[path = "pure_differential_abs_tests.rs"]
mod abs_tests;

#[cfg(test)]
#[path = "pure_differential_string_case_tests.rs"]
mod string_case_tests;

#[cfg(test)]
#[path = "pure_differential/to_date_tests.rs"]
mod to_date_tests;

#[cfg(test)]
#[path = "pure_differential_xx_hash3_128_tests.rs"]
mod xx_hash3_128_tests;

#[cfg(test)]
#[path = "pure_differential/sec_to_time_tests.rs"]
mod sec_to_time_tests;

#[cfg(test)]
#[path = "pure_differential_crc32_shared_tests.rs"]
mod crc32_shared_tests;

#[cfg(test)]
#[path = "pure_differential_regexp_position_tests.rs"]
mod regexp_position_tests;

#[cfg(test)]
#[path = "pure_differential/concat_tests.rs"]
mod concat_tests;

#[cfg(test)]
#[path = "pure_differential_md5_family_tests.rs"]
mod md5_family_tests;

#[cfg(test)]
#[path = "pure_differential_split_tests.rs"]
mod split_tests;

#[cfg(test)]
#[path = "pure_differential_null_or_empty_tests.rs"]
mod null_or_empty_tests;

#[cfg(test)]
#[path = "pure_differential/array_tests.rs"]
mod array_tests;

#[path = "pure_differential_shift_shared_tests.rs"]
mod shift_shared_tests;

#[path = "pure_differential_bitwise_shared_tests.rs"]
mod bitwise_shared_tests;

#[path = "pure_differential_collection_construct_access_tests.rs"]
mod collection_construct_access_tests;

#[path = "pure_differential_array_element_access_tests.rs"]
mod array_element_access_tests;

#[path = "pure_differential/unixtime_tests.rs"]
mod unixtime_tests;

#[path = "pure_differential/time_slice_tests.rs"]
mod time_slice_tests;

#[path = "pure_differential_array_append_tests.rs"]
mod array_append_tests;

#[path = "pure_differential/mod_pmod_shared_tests.rs"]
mod mod_pmod_shared_tests;

#[path = "pure_differential/numeric_binary_shared_tests.rs"]
mod numeric_binary_shared_tests;

mod round_disagreement_tests;
mod round_expanded_tests;

#[cfg(test)]
#[path = "pure_differential_array_append_multi_tests.rs"]
mod array_append_multi_tests;

#[cfg(test)]
#[path = "pure_differential_map_size_tests.rs"]
mod map_size_tests;

#[cfg(test)]
#[path = "pure_differential_map_keys_values_tests.rs"]
mod map_keys_values_tests;

#[path = "pure_differential_time_text_tests.rs"]
mod time_text_tests;

mod string_measure_shared_tests;

#[path = "pure_differential/regexp_count_tests.rs"]
mod regexp_count_tests;

#[path = "pure_differential_sha2_shared_tests.rs"]
mod sha2_shared_differential;

#[path = "pure_differential_sm3_shared_tests.rs"]
mod sm3_shared_differential;

#[cfg(test)]
#[path = "pure_differential/aggregate_count_original_diff_tests.rs"]
mod aggregate_count_original_diff_tests;

pub(crate) mod aggregate_count_generic_diff_tests;

#[path = "pure_differential_parse_url_tests.rs"]
mod parse_url_tests;

#[cfg(test)]
#[path = "pure_differential_cardinality_dedup_tests.rs"]
mod cardinality_dedup_tests;

#[path = "pure_differential_to_base64_tests.rs"]
mod to_base64_tests;

#[path = "pure_differential_calendar_leaf_dedup_tests.rs"]
mod calendar_leaf_dedup_tests;

#[path = "pure_differential_to_binary_metadata_tests.rs"]
mod to_binary_metadata_tests;

#[path = "pure_differential_array_match_tests.rs"]
mod array_match_tests;

#[path = "pure_differential_array_difference_tests.rs"]
mod array_difference_tests;

#[path = "pure_differential/string_reverse_shared_tests.rs"]
mod string_reverse_shared_tests;

#[path = "pure_differential/append_trailing_shared_tests.rs"]
mod append_trailing_shared_tests;

#[path = "pure_differential_map_entries_tests.rs"]
mod map_entries_tests;

#[cfg(test)]
#[path = "pure_differential_ndv_hll_tests.rs"]
mod ndv_hll_tests;

#[cfg(test)]
#[path = "pure_differential_exact_percentile_tests.rs"]
mod exact_percentile_tests;

#[cfg(test)]
#[path = "pure_differential_ndv_invocation_data_tests.rs"]
mod ndv_invocation_data_tests;

#[cfg(test)]
#[path = "pure_differential_ds_hll_family_tests.rs"]
mod ds_hll_family_tests;

#[cfg(test)]
#[path = "pure_differential_exact_percentile_rate_tests.rs"]
mod exact_percentile_rate_tests;

#[path = "pure_differential_field_tests.rs"]
mod field_tests;

#[cfg(test)]
#[path = "pure_differential_approx_percentile_tests.rs"]
mod approx_percentile_tests;

#[cfg(test)]
#[path = "pure_differential_percentile_hash_native_n1_tests.rs"]
mod percentile_hash_native_n1_tests;

#[cfg(test)]
#[path = "pure_differential_bitmap_to_string_tests.rs"]
mod bitmap_to_string_tests;

#[path = "pure_differential_hll_hash_tests.rs"]
mod hll_hash_tests;

#[path = "pure_differential_bitmap_union_int_tests.rs"]
mod bitmap_union_int_tests;

#[path = "pure_differential_map_agg_tests.rs"]
mod pure_differential_map_agg_tests;

#[path = "pure_differential_hll_payload_aggregate_tests.rs"]
mod hll_payload_aggregate_tests;

#[path = "pure_differential_percentile_approx_raw_tests.rs"]
mod pure_differential_percentile_approx_raw_tests;

#[cfg(test)]
#[path = "pure_differential_bitmap_agg_tests.rs"]
mod bitmap_agg_tests;

#[path = "pure_differential_array_struct_subfield_tests.rs"]
mod array_struct_subfield_tests;

#[cfg(test)]
#[path = "pure_differential_percentile_union_tests.rs"]
mod percentile_union_tests;

#[cfg(test)]
#[path = "pure_differential_left_right_shared_tests.rs"]
mod left_right_shared_tests;

#[cfg(test)]
#[path = "pure_differential_split_part_shared_tests.rs"]
mod split_part_shared_tests;

#[cfg(test)]
#[path = "pure_differential_approx_top_k_tests.rs"]
mod approx_top_k_tests;

#[cfg(test)]
#[path = "pure_differential_approx_top_k_full_any_error_tests.rs"]
mod approx_top_k_full_any_error_tests;

#[cfg(test)]
#[path = "pure_differential_parse_json_tests.rs"]
mod parse_json_tests;
