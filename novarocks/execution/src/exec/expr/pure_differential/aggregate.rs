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

//! Grouped plain-aggregate differential: pure aggregate handles vs the legacy
//! aggregate kernel set.
//!
//! Both paths run the same two shapes over the same group ids:
//! - Single: one state per group, every row updated once.
//! - Partial -> Final: rows are dealt to `partitions` partitions by a seeded
//!   draw; each partition aggregates only the groups it sees, emits its state
//!   column, and one Final merges every partition's states by global group.
//!   The pure Partial consumes a sparse `Selection` of the original batch,
//!   the legacy partial the gathered rows, exactly as each path is fed.
//!
//! Legacy aggregation follows the native adapter: multi-argument input is
//! packed into one struct column with fields `f{i}`, and `count_if` with two
//! arguments consumes only its second argument. The final results are
//! compared for type, NULLs and values; aggregate owners have no row-error
//! channel, so a failure must occur on both sides as an operational data
//! failure. Intermediate states are implementation formats and are not
//! compared; only their final interpretation is.

use std::num::NonZeroUsize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StructArray, UInt64Array};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Fields};
use novarocks_functions::builtin::catalogue::{
    builtin_engine_function_catalog, resolved_aggregate_signature_from_binding,
};
use novarocks_functions::{
    AggregateInputBatch, AggregateKernelPhase, AggregatePreparationOptions, AggregateStateColumn,
    CallArgumentUses, CallEffectInput, EngineFunctionCatalog, EvaluatedArgument,
    FunctionBindingRequest, FunctionId, FunctionKind as CatalogKind, FunctionOverloadId,
    FunctionResultType, KernelFailure, PreparedAggregateHandle, PreparedPureKernel,
    PureCallPreparation, PureKernelAbi, ResolvedAggregateSignature, ResolvedFunctionBinding,
    ScopedExpressionEffects, SelectedAggregateMergeInput, SelectedAggregateUpdateInput, Selection,
};
use novarocks_type_contract::{
    CallProofScope, EvaluationDemand, EvaluationDomainId, ExpressionEffectContext, ExpressionUseId,
    FunctionValueType,
};

use super::generate::InputGenerator;
use super::{
    DiffArgument, DiffSemantics, DifferentialFailure, ErrorMessageCheck, FloatComparison,
    HarnessControl, LegacyStatus, compare_value, harness_context, installed_declaration,
    panic_message, render, require_selected_arguments, resolve_like_sql, specialization_failure,
};
use crate::exec::expr::ExprId;
use crate::exec::expr::agg::{
    AggKernelSet, AggStateArena, AggStatePtr, build_kernel_set, test_builtin_execution_function_set,
};
use crate::exec::node::aggregate::{AggFunction, AggOrderSpec, AggTypeSignature};
use crate::runtime::mem_tracker::MemTracker;

/// One grouped aggregate case. Build with [`AggregateDiffSpec::new`].
#[derive(Clone, Debug)]
pub(crate) struct AggregateDiffSpec {
    pub name: String,
    /// Logical arguments; empty for `count(*)`.
    pub arguments: Vec<DiffArgument>,
    /// Row count when every argument is a constant or there is none.
    pub constant_rows: usize,
    /// Group id per row; `None` is one global group.
    pub group_ids: Option<Vec<usize>>,
    /// Number of groups; groups without rows are emitted from empty states.
    pub groups: usize,
    pub partitions: usize,
    pub partition_seed: u64,
    /// Match an immutable original transcript that partitions by row ordinal.
    pub round_robin_partitions: bool,
    pub semantics: DiffSemantics,
    pub original_state: Option<novarocks_type_contract::AggregateStateInterpretation>,
    pub float_comparison: FloatComparison,
    pub error_messages: ErrorMessageCheck,
    /// Require the lossless whole-invocation Data transport for legacy errors.
    pub require_invocation_data_errors: bool,
    /// Only explicitly frozen original panic payloads can match.
    pub expected_panic_payload: Option<String>,
}

impl AggregateDiffSpec {
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            arguments: Vec::new(),
            constant_rows: 0,
            group_ids: None,
            groups: 1,
            partitions: 3,
            partition_seed: 0xA66,
            round_robin_partitions: false,
            semantics: DiffSemantics::default(),
            original_state: None,
            float_comparison: FloatComparison::Exact,
            error_messages: ErrorMessageCheck::LegacyContainsPure,
            require_invocation_data_errors: false,
            expected_panic_payload: None,
        }
    }

    pub(crate) fn column(self, values: ArrayRef) -> Self {
        let value_type = FunctionValueType::new(values.data_type().clone(), true);
        self.typed_column(value_type, values)
    }

    pub(crate) fn typed_column(mut self, value_type: FunctionValueType, values: ArrayRef) -> Self {
        self.arguments
            .push(DiffArgument::Column { value_type, values });
        self
    }

    pub(crate) fn constant(mut self, value: novarocks_functions::ConstantValue) -> Self {
        self.arguments.push(DiffArgument::Constant(value));
        self
    }

    /// Rows for `count(*)` or an all-constant call.
    pub(crate) fn constant_rows(mut self, rows: usize) -> Self {
        self.constant_rows = rows;
        self
    }

    /// Group every row by `group_ids`; `groups` may exceed the largest id to
    /// include empty groups.
    pub(crate) fn grouped(mut self, group_ids: Vec<usize>, groups: usize) -> Self {
        assert!(
            group_ids.iter().all(|group| *group < groups),
            "group id outside 0..{groups}"
        );
        self.group_ids = Some(group_ids);
        self.groups = groups;
        self
    }

    pub(crate) fn partitions(mut self, partitions: usize, seed: u64) -> Self {
        assert!(partitions > 0, "at least one partition");
        self.partitions = partitions;
        self.partition_seed = seed;
        self
    }

    pub(crate) fn round_robin_partitions(mut self, count: usize) -> Self {
        assert!(count > 0);
        self.partitions = count;
        self.round_robin_partitions = true;
        self
    }

    /// The test producer authors the same original state options for both paths.
    pub(crate) fn original_state_interpretation(
        mut self,
        distinct: bool,
        order: Vec<novarocks_type_contract::AggregateStateOrderKey>,
    ) -> Self {
        self.original_state = Some(novarocks_type_contract::AggregateStateInterpretation {
            distinct,
            order_keys: order.into_boxed_slice(),
        });
        self
    }
    fn logical_argument_count(&self) -> Result<usize, DifferentialFailure> {
        self.arguments
            .len()
            .checked_sub(
                self.original_state
                    .as_ref()
                    .map_or(0, |state| state.order_keys.len()),
            )
            .ok_or_else(|| {
                DifferentialFailure::InvalidSpec(
                    "state interpretation has more order keys than channels".into(),
                )
            })
    }

    pub(crate) fn semantics(mut self, semantics: DiffSemantics) -> Self {
        self.semantics = semantics;
        self
    }

    pub(crate) fn float_comparison(mut self, comparison: FloatComparison) -> Self {
        self.float_comparison = comparison;
        self
    }

    pub(crate) fn invocation_data_errors(mut self) -> Self {
        self.require_invocation_data_errors = true;
        self
    }

    pub(crate) fn ignore_error_messages(mut self) -> Self {
        self.error_messages = ErrorMessageCheck::Ignore;
        self
    }

    /// Opt in to one exact original library panic, without treating it as a
    /// successful NULL or an equivalent Operational data failure.
    pub(crate) fn expected_panic_payload(mut self, payload: &str) -> Self {
        self.expected_panic_payload = Some(payload.into());
        self
    }

    fn rows(&self) -> Result<usize, DifferentialFailure> {
        let mut rows = None;
        for argument in &self.arguments {
            if let DiffArgument::Column { values, .. } = argument {
                match rows {
                    None => rows = Some(values.len()),
                    Some(existing) if existing != values.len() => {
                        return Err(DifferentialFailure::InvalidSpec(
                            "aggregate argument columns have different lengths".into(),
                        ));
                    }
                    Some(_) => {}
                }
            }
        }
        let rows = rows.unwrap_or(self.constant_rows);
        if let Some(groups) = &self.group_ids
            && groups.len() != rows
        {
            return Err(DifferentialFailure::InvalidSpec(format!(
                "{} group ids for {rows} rows",
                groups.len()
            )));
        }
        Ok(rows)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AggregateDiffSummary {
    pub function: FunctionId,
    pub overload: FunctionOverloadId,
    pub result_type: FunctionValueType,
    pub pure_state_type: FunctionValueType,
    pub legacy_intermediate_type: DataType,
    pub rows: usize,
    pub groups: usize,
    pub partitions: usize,
    /// Shapes in which both paths failed with an equivalent data failure.
    pub matched_failures: usize,
    /// Shapes where both paths panic with the same complete payload. This is
    /// distinct from an equivalent Operational data failure.
    pub matched_panics: usize,
    pub null_results: usize,
}

#[track_caller]
pub(crate) fn assert_aggregate_matches_v1(spec: AggregateDiffSpec) -> AggregateDiffSummary {
    match run_aggregate_differential(&spec) {
        Ok(summary) => summary,
        Err(failure) => panic!("{failure}"),
    }
}

/// Layout of rows into groups and partitions shared by both paths.
struct Layout {
    rows: usize,
    groups: usize,
    group_ids: Vec<usize>,
    /// Per partition: original rows, local group of each row, and the global
    /// group of each local group in first-appearance order.
    partitions: Vec<PartitionLayout>,
}

struct PartitionLayout {
    rows: Vec<usize>,
    local_groups: Vec<usize>,
    globals: Vec<usize>,
}

impl Layout {
    fn new(spec: &AggregateDiffSpec, rows: usize) -> Self {
        let group_ids = spec.group_ids.clone().unwrap_or_else(|| vec![0; rows]);
        let mut generator = InputGenerator::new(spec.partition_seed);
        let mut partitions = (0..spec.partitions)
            .map(|_| PartitionLayout {
                rows: Vec::new(),
                local_groups: Vec::new(),
                globals: Vec::new(),
            })
            .collect::<Vec<_>>();
        for (row, group) in group_ids.iter().copied().enumerate() {
            let index = if spec.round_robin_partitions {
                row % spec.partitions
            } else {
                generator.rng().below(spec.partitions as u64) as usize
            };
            let partition = &mut partitions[index];
            let local = match partition.globals.iter().position(|global| *global == group) {
                Some(local) => local,
                None => {
                    partition.globals.push(group);
                    partition.globals.len() - 1
                }
            };
            partition.rows.push(row);
            partition.local_groups.push(local);
        }
        Self {
            rows,
            groups: spec.groups,
            group_ids,
            partitions,
        }
    }
}

fn resolve_aggregate_spec(
    catalog: &EngineFunctionCatalog,
    spec: &AggregateDiffSpec,
) -> Result<ResolvedFunctionBinding, DifferentialFailure> {
    if spec.original_state.is_none() {
        return resolve_like_sql(catalog, &spec.name, CatalogKind::Aggregate, &spec.arguments);
    }
    let arguments = spec
        .arguments
        .iter()
        .map(DiffArgument::request)
        .collect::<Vec<_>>();
    catalog
        .resolve_bound_user(
            &spec.name,
            CatalogKind::Aggregate,
            FunctionBindingRequest {
                arguments: &arguments,
                logical_argument_count: spec.logical_argument_count()?,
                expected_result_type: None,
            },
            &HarnessControl,
        )
        .map_err(|error| DifferentialFailure::Resolution {
            name: spec.name.clone(),
            error: error.to_string(),
        })
}

pub(crate) fn run_aggregate_differential(
    spec: &AggregateDiffSpec,
) -> Result<AggregateDiffSummary, DifferentialFailure> {
    let rows = spec.rows()?;
    let catalog = builtin_engine_function_catalog();
    let bound = resolve_aggregate_spec(catalog, spec)?;
    let FunctionResultType::Scalar(result_type) = bound.selected.result_type.clone() else {
        return Err(DifferentialFailure::InvalidSpec(
            "aggregate selected a relation result".into(),
        ));
    };
    let pure_state_type = bound
        .selected
        .aggregate
        .as_ref()
        .map(|aggregate| aggregate.intermediate_type.clone())
        .ok_or_else(|| DifferentialFailure::InvalidSpec("aggregate has no state".into()))?;
    let signature = resolved_aggregate_signature_from_binding(bound.clone())
        .map_err(|error| DifferentialFailure::InvalidSpec(error.to_string()))?;
    let legacy = LegacyAggregate::try_new(spec, &signature);
    let legacy_status = match &legacy {
        Ok(_) => LegacyStatus::Available,
        Err(error) => LegacyStatus::Unavailable(error.clone()),
    };
    let (abi, dependencies) = installed_declaration(
        catalog,
        &spec.name,
        &bound,
        CatalogKind::Aggregate,
        legacy_status,
    )?;
    require_selected_arguments(&spec.name, &bound, &spec.arguments)?;
    if !matches!(
        abi,
        PureKernelAbi::AggregateV1 | PureKernelAbi::AggregateWindowV1
    ) {
        return Err(DifferentialFailure::UnsupportedPureAbi {
            overload: bound.selected.overload.clone(),
            abi: format!("{abi:?}"),
        });
    }
    let legacy = legacy.map_err(|reason| DifferentialFailure::LegacyUnavailable {
        name: spec.name.clone(),
        reason,
    })?;
    let pure = PureAggregate::try_new(catalog, spec, &bound, &pure_state_type, &dependencies)?;
    let layout = Layout::new(spec, rows);
    let mut summary = AggregateDiffSummary {
        function: bound.function_id.clone(),
        overload: bound.selected.overload.clone(),
        result_type: result_type.clone(),
        pure_state_type,
        legacy_intermediate_type: signature.intermediate_type.clone(),
        rows,
        groups: layout.groups,
        partitions: spec.partitions,
        matched_failures: 0,
        matched_panics: 0,
        null_results: 0,
    };
    let mut details = Vec::new();
    for (shape, legacy_result, pure_result) in [
        (
            "single phase",
            guard_legacy(|| legacy.single(spec, &layout)),
            guard_pure(|| pure.single(spec, &layout)),
        ),
        (
            "partial -> final",
            guard_legacy(|| legacy.two_phase(spec, &layout)),
            guard_pure(|| pure.two_phase(spec, &layout)),
        ),
    ] {
        compare_shape(
            spec,
            &result_type,
            shape,
            layout.groups,
            legacy_result,
            pure_result,
            &mut summary,
            &mut details,
        );
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

fn guard_legacy(run: impl FnOnce() -> Result<ArrayRef, String>) -> Result<ArrayRef, String> {
    catch_unwind(AssertUnwindSafe(run)).unwrap_or_else(|panic| {
        Err(format!(
            "{}{}",
            super::LEGACY_PANIC_PREFIX,
            panic_message(&panic)
        ))
    })
}

enum GuardedPureResult {
    Finished(Result<ArrayRef, novarocks_functions::EvaluationFailure>),
    Panicked(String),
}
fn guard_pure(
    run: impl FnOnce() -> Result<ArrayRef, novarocks_functions::EvaluationFailure>,
) -> GuardedPureResult {
    match catch_unwind(AssertUnwindSafe(run)) {
        Ok(result) => GuardedPureResult::Finished(result),
        Err(panic) => GuardedPureResult::Panicked(panic_message(&panic)),
    }
}

#[allow(clippy::too_many_arguments)]
fn compare_shape(
    spec: &AggregateDiffSpec,
    result_type: &FunctionValueType,
    shape: &str,
    groups: usize,
    legacy: Result<ArrayRef, String>,
    pure: GuardedPureResult,
    summary: &mut AggregateDiffSummary,
    details: &mut Vec<String>,
) {
    let pure = match pure {
        GuardedPureResult::Finished(result) => result,
        GuardedPureResult::Panicked(payload) => {
            match legacy {
                Err(message)
                    if message.strip_prefix(super::LEGACY_PANIC_PREFIX)
                        == Some(payload.as_str())
                        && spec.expected_panic_payload.as_deref() == Some(payload.as_str()) =>
                {
                    summary.matched_panics += 1;
                }
                Err(message) => details.push(format!(
                    "{shape}: panic mismatch: legacy `{message}`, pure `{payload}`"
                )),
                Ok(_) => details.push(format!(
                    "{shape}: pure panicked with `{payload}` but legacy completed"
                )),
            }
            return;
        }
    };
    match (legacy, pure) {
        (Ok(legacy), Ok(pure)) => {
            if legacy.len() != groups || pure.len() != groups {
                details.push(format!(
                    "{shape}: {groups} groups, legacy emitted {}, pure emitted {}",
                    legacy.len(),
                    pure.len()
                ));
                return;
            }
            if !novarocks_type_contract::arrow_data_types_exact(pure.data_type(), &result_type.data_type)
                || !novarocks_type_contract::arrow_data_types_exact(legacy.data_type(), pure.data_type())
            {
                details.push(format!(
                    "{shape}: result type legacy {:?}, pure {:?}, selected {:?}",
                    legacy.data_type(),
                    pure.data_type(),
                    result_type.data_type
                ));
                return;
            }
            if !result_type.nullable && (legacy.null_count() > 0 || pure.null_count() > 0) {
                details.push(format!(
                    "{shape}: NULL in a non-nullable result (legacy {}, pure {})",
                    legacy.null_count(),
                    pure.null_count()
                ));
            }
            for group in 0..groups {
                match compare_value(&legacy, group, &pure, group, spec.float_comparison) {
                    Ok(true) => summary.null_results += 1,
                    Ok(false) => {}
                    Err(difference) => details.push(format!("{shape}: group {group}: {difference}")),
                }
            }
        }
        (Err(legacy), Err(novarocks_functions::EvaluationFailure::Kernel(KernelFailure::Operational(diagnostic)))) => {
            if legacy.starts_with(super::LEGACY_PANIC_PREFIX) {
                details.push(format!("{shape}: {legacy}"));
            } else if spec.require_invocation_data_errors {
                details.push(format!(
                    "{shape}: legacy `{legacy}` requires whole invocation Data, pure returned Kernel Operational `{}`",
                    diagnostic.message()
                ));
            } else if spec.error_messages == ErrorMessageCheck::LegacyContainsPure
                && !legacy.contains(diagnostic.message())
            {
                details.push(format!(
                    "{shape}: legacy diagnostic `{legacy}` does not contain pure `{}`",
                    diagnostic.message()
                ));
            } else {
                summary.matched_failures += 1;
            }
        }
        (Err(legacy), Err(novarocks_functions::EvaluationFailure::InvocationData(data))) => {
            if legacy.starts_with(super::LEGACY_PANIC_PREFIX) || legacy != data.message() {
                details.push(format!("{shape}: whole invocation Data differs: legacy `{legacy}`, pure `{data}`"));
            } else { summary.matched_failures += 1; }
        }
        (Err(legacy), Err(other)) => details.push(format!(
            "{shape}: legacy failed with `{legacy}`, pure outer failure `{other}` is not a data failure"
        )),
        (Err(legacy), Ok(pure)) => details.push(format!(
            "{shape}: required error swallowed: legacy failed with `{legacy}`, pure emitted {}",
            (0..pure.len().min(4))
                .map(|row| render(&pure, row))
                .collect::<Vec<_>>()
                .join(", ")
        )),
        (Ok(legacy), Err(failure)) => details.push(format!(
            "{shape}: pure failed with `{failure}` where legacy emitted {}",
            (0..legacy.len().min(4))
                .map(|row| render(&legacy, row))
                .collect::<Vec<_>>()
                .join(", ")
        )),
    }
}

// ---------------------------------------------------------------------------
// Legacy path
// ---------------------------------------------------------------------------

struct LegacyAggregate {
    update: AggKernelSet,
    merge: AggKernelSet,
    /// Which logical arguments form the legacy input, in order.
    channels: Vec<usize>,
}

impl LegacyAggregate {
    fn try_new(
        spec: &AggregateDiffSpec,
        signature: &ResolvedAggregateSignature,
    ) -> Result<Self, String> {
        let channels = if spec.name == "count_if" && spec.arguments.len() == 2 {
            vec![1]
        } else {
            (0..spec.arguments.len()).collect()
        };
        let input_type = match channels.as_slice() {
            [] => None,
            [single] => Some(spec.arguments[*single].value_type().data_type.clone()),
            many => Some(DataType::Struct(Fields::from(
                many.iter()
                    .enumerate()
                    .map(|(index, channel)| {
                        Field::new(
                            format!("f{index}"),
                            spec.arguments[*channel].value_type().data_type.clone(),
                            true,
                        )
                    })
                    .collect::<Vec<_>>(),
            ))),
        };
        let group_concat_max_len = (spec.name == "group_concat" || spec.name == "string_agg")
            .then(|| spec.semantics.group_concat_max_len())
            .flatten();
        let has_input = input_type.is_some();
        let function = |merge: bool| AggFunction {
            name: spec.name.clone(),
            inputs: if has_input || merge {
                vec![ExprId(0)]
            } else {
                Vec::new()
            },
            input_is_intermediate: merge,
            types: Some(AggTypeSignature {
                intermediate_type: Some(signature.intermediate_type.clone()),
                output_type: Some(signature.output_type.clone()),
                input_arg_type: signature.argument_types.first().cloned(),
            }),
            order: AggOrderSpec {
                group_concat_max_len,
                is_distinct: spec
                    .original_state
                    .as_ref()
                    .is_some_and(|state| state.distinct),
                is_asc_order: spec.original_state.as_ref().map_or_else(Vec::new, |state| {
                    state.order_keys.iter().map(|key| key.ascending).collect()
                }),
                nulls_first: spec.original_state.as_ref().map_or_else(Vec::new, |state| {
                    state.order_keys.iter().map(|key| key.nulls_first).collect()
                }),
            },
        };
        let functions = test_builtin_execution_function_set();
        let update = build_kernel_set(
            &functions,
            &[function(false)],
            &[input_type],
            std::slice::from_ref(signature),
        )?;
        let merge = build_kernel_set(
            &functions,
            &[function(true)],
            &[Some(signature.intermediate_type.clone())],
            std::slice::from_ref(signature),
        )?;
        Ok(Self {
            update,
            merge,
            channels,
        })
    }

    /// The packed legacy input over `rows` (all rows when `None`).
    fn input(
        &self,
        spec: &AggregateDiffSpec,
        rows: Option<&[usize]>,
        all: usize,
    ) -> Result<Option<ArrayRef>, String> {
        let count = rows.map_or(all, <[_]>::len);
        let indices =
            rows.map(|rows| UInt64Array::from_iter_values(rows.iter().map(|row| *row as u64)));
        let mut columns = Vec::with_capacity(self.channels.len());
        for channel in &self.channels {
            let column = match &spec.arguments[*channel] {
                DiffArgument::Column { values, .. } => match &indices {
                    Some(indices) => {
                        take(values.as_ref(), indices, None).map_err(|e| e.to_string())?
                    }
                    None => Arc::clone(values),
                },
                DiffArgument::Constant(value) => {
                    let repeat = UInt64Array::from(vec![u64::from(value.ordinal()); count]);
                    take(value.pool().array().as_ref(), &repeat, None).map_err(|e| e.to_string())?
                }
            };
            columns.push(column);
        }
        Ok(match columns.len() {
            0 => None,
            1 => columns.pop(),
            _ => {
                let fields = columns
                    .iter()
                    .enumerate()
                    .map(|(index, column)| {
                        Field::new(format!("f{index}"), column.data_type().clone(), true)
                    })
                    .collect::<Vec<_>>();
                Some(Arc::new(
                    StructArray::try_new(Fields::from(fields), columns, None)
                        .map_err(|error| error.to_string())?,
                ) as ArrayRef)
            }
        })
    }

    fn single(&self, spec: &AggregateDiffSpec, layout: &Layout) -> Result<ArrayRef, String> {
        let input = self.input(spec, None, layout.rows)?;
        let mut arena = LegacyArena::new();
        let states = LegacyStates::new(&self.update, &mut arena, layout.groups)?;
        let pointers = layout
            .group_ids
            .iter()
            .map(|group| states.pointers[*group])
            .collect::<Vec<_>>();
        let entry = &self.update.entries[0];
        entry.update_batch(
            &pointers,
            AggregateInputBatch::try_new(input.as_ref(), layout.rows).map_err(|e| e.to_string())?,
        )?;
        entry.build_array(&states.pointers, false)
    }

    fn two_phase(&self, spec: &AggregateDiffSpec, layout: &Layout) -> Result<ArrayRef, String> {
        let mut arena = LegacyArena::new();
        let mut partials = Vec::with_capacity(layout.partitions.len());
        for partition in &layout.partitions {
            if partition.globals.is_empty() {
                continue;
            }
            let input = self.input(spec, Some(&partition.rows), layout.rows)?;
            let states = LegacyStates::new(&self.update, &mut arena, partition.globals.len())?;
            let pointers = partition
                .local_groups
                .iter()
                .map(|local| states.pointers[*local])
                .collect::<Vec<_>>();
            let entry = &self.update.entries[0];
            entry.update_batch(
                &pointers,
                AggregateInputBatch::try_new(input.as_ref(), partition.rows.len())
                    .map_err(|e| e.to_string())?,
            )?;
            partials.push((partition, entry.build_array(&states.pointers, true)?));
        }
        let states = LegacyStates::new(&self.merge, &mut arena, layout.groups)?;
        let entry = &self.merge.entries[0];
        for (partition, partial) in &partials {
            let pointers = partition
                .globals
                .iter()
                .map(|global| states.pointers[*global])
                .collect::<Vec<_>>();
            entry.merge_batch(
                &pointers,
                AggregateInputBatch::try_new(Some(partial), partial.len())
                    .map_err(|e| e.to_string())?,
            )?;
        }
        entry.build_array(&states.pointers, false)
    }
}

/// Initialized legacy states that are dropped exactly once.
struct LegacyStates<'a> {
    kernels: &'a AggKernelSet,
    pointers: Vec<AggStatePtr>,
}

/// A tracked state arena, as the aggregate operator owns one: some legacy
/// states (for example UTF-8 extrema) refuse to initialize without a tracker.
struct LegacyArena {
    arena: AggStateArena,
    tracker: Arc<MemTracker>,
}

impl LegacyArena {
    fn new() -> Self {
        let tracker = MemTracker::new_root("pure-differential-legacy-aggregate");
        let mut arena = AggStateArena::new(4096);
        arena.set_mem_tracker(Arc::clone(&tracker));
        Self { arena, tracker }
    }
}

impl<'a> LegacyStates<'a> {
    fn new(
        kernels: &'a AggKernelSet,
        arena: &mut LegacyArena,
        count: usize,
    ) -> Result<Self, String> {
        let entry = &kernels.entries[0];
        let mut states = Self {
            kernels,
            pointers: Vec::with_capacity(count),
        };
        for _ in 0..count {
            let pointer = arena
                .arena
                .try_alloc(kernels.layout.total_size, entry.state_align())?;
            entry.init_state_with_tracker(pointer, Arc::clone(&arena.tracker))?;
            states.pointers.push(pointer);
        }
        Ok(states)
    }
}

impl Drop for LegacyStates<'_> {
    fn drop(&mut self) {
        for pointer in &self.pointers {
            self.kernels.entries[0].drop_state(*pointer);
        }
    }
}

// ---------------------------------------------------------------------------
// Pure path
// ---------------------------------------------------------------------------

struct PureAggregate {
    single: PreparedAggregateHandle,
    partial: PreparedAggregateHandle,
    last: PreparedAggregateHandle,
}

fn state_context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(u32::MAX),
        domain: EvaluationDomainId::new(u32::MAX - 1),
        demand: EvaluationDemand::Value,
    }
}

impl PureAggregate {
    fn try_new(
        catalog: &EngineFunctionCatalog,
        spec: &AggregateDiffSpec,
        bound: &ResolvedFunctionBinding,
        state_type: &FunctionValueType,
        dependencies: &[novarocks_type_contract::SemanticParameterKey],
    ) -> Result<Self, DifferentialFailure> {
        let prepare =
            |phase| prepare_aggregate(catalog, spec, bound, state_type, dependencies, phase);
        Ok(Self {
            single: prepare(AggregateKernelPhase::Single)?,
            partial: prepare(AggregateKernelPhase::Partial)?,
            last: prepare(AggregateKernelPhase::Final)?,
        })
    }

    fn column(
        handle: &PreparedAggregateHandle,
        groups: usize,
    ) -> Result<AggregateStateColumn, KernelFailure> {
        let mut column = AggregateStateColumn::try_new(
            handle.clone(),
            crate::exec::operators::compiled_aggregate::differential_aggregate_state_allocator(
                MemTracker::new_root("pure-differential-aggregate-state"),
            ),
            NonZeroUsize::new(64).expect("nonzero block"),
        )?;
        for _ in 0..groups {
            column.push(&HarnessControl)?;
        }
        Ok(column)
    }

    fn update(
        handle: &PreparedAggregateHandle,
        spec: &AggregateDiffSpec,
        column: &mut AggregateStateColumn,
        selection: Selection<'_>,
        mapping: &[usize],
    ) -> Result<(), novarocks_functions::EvaluationFailure> {
        let contract = Arc::clone(handle.contract());
        let arguments = spec
            .arguments
            .iter()
            .map(DiffArgument::evaluated)
            .collect::<Vec<_>>();
        let input = SelectedAggregateUpdateInput::try_new(
            &contract,
            selection,
            &arguments[..contract.call().logical_argument_count()],
            &arguments[contract.call().logical_argument_count()..],
            &HarnessControl,
        )?;
        column
            .prepare_update_batch_evaluation(mapping, input, &HarnessControl)?
            .run(&HarnessControl)
    }

    fn single(
        &self,
        spec: &AggregateDiffSpec,
        layout: &Layout,
    ) -> Result<ArrayRef, novarocks_functions::EvaluationFailure> {
        let mut column = Self::column(&self.single, layout.groups)?;
        Self::update(
            &self.single,
            spec,
            &mut column,
            Selection::all(layout.rows),
            &layout.group_ids,
        )?;
        column.emit_evaluation(
            &(0..layout.groups).collect::<Vec<_>>(),
            layout.groups,
            &HarnessControl,
        )
    }

    fn two_phase(
        &self,
        spec: &AggregateDiffSpec,
        layout: &Layout,
    ) -> Result<ArrayRef, novarocks_functions::EvaluationFailure> {
        let mut partials = Vec::with_capacity(layout.partitions.len());
        for partition in &layout.partitions {
            if partition.globals.is_empty() {
                continue;
            }
            let mut column = Self::column(&self.partial, partition.globals.len())?;
            let selection = Selection::try_sparse(layout.rows, &partition.rows)
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            Self::update(
                &self.partial,
                spec,
                &mut column,
                selection,
                &partition.local_groups,
            )?;
            let locals = (0..partition.globals.len()).collect::<Vec<_>>();
            partials.push((
                partition,
                column.emit_evaluation(&locals, locals.len(), &HarnessControl)?,
            ));
        }
        let mut column = Self::column(&self.last, layout.groups)?;
        let contract = Arc::clone(self.last.contract());
        for (partition, states) in &partials {
            let input = SelectedAggregateMergeInput::try_new(
                &contract,
                Selection::all(states.len()),
                EvaluatedArgument::Column(states),
                &HarnessControl,
            )?;
            column
                .prepare_merge_batch_evaluation(&partition.globals, input, &HarnessControl)?
                .run(&HarnessControl)?;
        }
        column.emit_evaluation(
            &(0..layout.groups).collect::<Vec<_>>(),
            layout.groups,
            &HarnessControl,
        )
    }
}

fn prepare_aggregate(
    catalog: &EngineFunctionCatalog,
    spec: &AggregateDiffSpec,
    bound: &ResolvedFunctionBinding,
    state_type: &FunctionValueType,
    dependencies: &[novarocks_type_contract::SemanticParameterKey],
    phase: AggregateKernelPhase,
) -> Result<PreparedAggregateHandle, DifferentialFailure> {
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
    let uses = (0..spec.arguments.len())
        .map(|index| Some(ExpressionUseId::new(index as u32 + 1)))
        .collect::<Vec<_>>();
    let selected = Arc::new(bound.selected.clone());
    let context = harness_context();
    let argument_uses = if phase.consumes_logical_arguments() {
        CallArgumentUses::SelectedChannels(&uses)
    } else {
        CallArgumentUses::AggregateMerge {
            phase,
            state_context: state_context(),
            state_input_type: state_type,
        }
    };
    let input = CallEffectInput {
        context,
        argument_uses,
        function_id: &bound.function_id,
        kind: CatalogKind::Aggregate,
        selected: selected.as_ref(),
        request: FunctionBindingRequest {
            expected_result_type: None,
            arguments: &request_arguments,
            logical_argument_count: spec.logical_argument_count()?,
        },
        environment: &environment,
        parameters: &parameters,
        decimal_overflow_policy: spec.semantics.decimal_overflow_policy,
        proof_scope: CallProofScope::Domain(context.domain),
    };
    let prepared = catalog
        .prepare_fresh_selected(
            input,
            Arc::clone(&selected),
            PureCallPreparation::Aggregate {
                arguments: ScopedExpressionEffects::pure_value(context),
                options: AggregatePreparationOptions {
                    state_interpretation: spec.original_state.clone().map(Arc::new),
                    phase,
                    distinct: phase.consumes_logical_arguments()
                        && spec
                            .original_state
                            .as_ref()
                            .is_some_and(|state| state.distinct),
                    order_keys: if phase.consumes_logical_arguments() {
                        spec.original_state.as_ref().map_or_else(
                            || Arc::from([]),
                            |state| {
                                state
                                    .order_keys
                                    .iter()
                                    .map(|key| novarocks_functions::AggregateOrderKey {
                                        ascending: key.ascending,
                                        nulls_first: key.nulls_first,
                                    })
                                    .collect::<Vec<_>>()
                                    .into()
                            },
                        )
                    } else {
                        Arc::from([])
                    },
                    state_input_type: (!phase.consumes_logical_arguments())
                        .then(|| state_type.clone()),
                },
            },
            &HarnessControl,
        )
        .map_err(|failure| specialization_failure(&spec.name, bound, failure))?;
    match prepared.into_prepared() {
        PreparedPureKernel::Aggregate(handle) => Ok(handle),
        _ => Err(DifferentialFailure::UnsupportedPureAbi {
            overload: bound.selected.overload.clone(),
            abi: "non-aggregate prepared kernel".into(),
        }),
    }
}

/// Resolution and owner status only: the cheap to-do probe for an aggregate.
pub(crate) fn aggregate_pure_owner_status(
    spec: &AggregateDiffSpec,
) -> Result<(FunctionId, FunctionOverloadId), DifferentialFailure> {
    let catalog = builtin_engine_function_catalog();
    let bound = resolve_aggregate_spec(catalog, spec)?;
    let signature = resolved_aggregate_signature_from_binding(bound.clone())
        .map_err(|error| DifferentialFailure::InvalidSpec(error.to_string()))?;
    let legacy = match LegacyAggregate::try_new(spec, &signature) {
        Ok(_) => LegacyStatus::Available,
        Err(error) => LegacyStatus::Unavailable(error),
    };
    installed_declaration(catalog, &spec.name, &bound, CatalogKind::Aggregate, legacy)?;
    Ok((bound.function_id, bound.selected.overload))
}

#[cfg(test)]
mod panic_comparison_tests {
    use super::*;
    use arrow::array::Int64Array;
    use novarocks_functions::KernelDiagnostic;
    const PAYLOAD: &str = "frozen original library panic";
    fn legacy_panic() -> Result<ArrayRef, String> {
        guard_legacy(|| panic!("{PAYLOAD}"))
    }
    fn pure_panic(payload: &str) -> GuardedPureResult {
        guard_pure(|| panic!("{payload}"))
    }
    fn compare(
        spec: AggregateDiffSpec,
        legacy: Result<ArrayRef, String>,
        pure: GuardedPureResult,
    ) -> (AggregateDiffSummary, Vec<String>) {
        let result = FunctionValueType::new(DataType::Int64, true);
        let mut summary = AggregateDiffSummary {
            function: FunctionId::try_new("builtin.aggregate/min_n/v1").unwrap(),
            overload: FunctionOverloadId::try_new("builtin.aggregate/min_n/derived-v1").unwrap(),
            result_type: result.clone(),
            pure_state_type: FunctionValueType::new(DataType::Binary, true),
            legacy_intermediate_type: DataType::Binary,
            rows: 0,
            groups: 1,
            partitions: 1,
            matched_failures: 0,
            matched_panics: 0,
            null_results: 0,
        };
        let mut details = Vec::new();
        compare_shape(
            &spec,
            &result,
            "independent panic guard",
            1,
            legacy,
            pure,
            &mut summary,
            &mut details,
        );
        (summary, details)
    }
    #[test]
    fn aggregate_same_panic_without_explicit_opt_in_is_rejected() {
        let (summary, details) = compare(
            AggregateDiffSpec::new("min_n"),
            legacy_panic(),
            pure_panic(PAYLOAD),
        );
        assert_eq!(summary.matched_panics, 0);
        assert_eq!(summary.matched_failures, 0);
        assert_eq!(details.len(), 1);
        assert!(details[0].contains("panic mismatch"));
    }
    #[test]
    fn aggregate_opt_in_requires_both_complete_payloads_and_frozen_payload_to_match() {
        for payload in [
            "different library panic",
            "frozen original library panic with tail",
        ] {
            let (summary, details) = compare(
                AggregateDiffSpec::new("min_n").expected_panic_payload(PAYLOAD),
                legacy_panic(),
                pure_panic(payload),
            );
            assert_eq!(summary.matched_panics, 0);
            assert_eq!(summary.matched_failures, 0);
            assert_eq!(details.len(), 1);
        }
        let (summary, details) = compare(
            AggregateDiffSpec::new("min_n").expected_panic_payload("different frozen payload"),
            legacy_panic(),
            pure_panic(PAYLOAD),
        );
        assert_eq!(summary.matched_panics, 0);
        assert_eq!(details.len(), 1);
    }
    #[test]
    fn aggregate_legacy_success_and_pure_panic_are_never_equivalent() {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let (summary, details) = compare(
            AggregateDiffSpec::new("min_n").expected_panic_payload(PAYLOAD),
            Ok(values),
            pure_panic(PAYLOAD),
        );
        assert_eq!(summary.matched_panics, 0);
        assert_eq!(summary.matched_failures, 0);
        assert_eq!(details.len(), 1);
        assert!(details[0].contains("legacy completed"));
    }
    #[test]
    fn aggregate_legacy_panic_and_pure_operational_are_never_equivalent() {
        let (summary, details) = compare(
            AggregateDiffSpec::new("min_n").expected_panic_payload(PAYLOAD),
            legacy_panic(),
            GuardedPureResult::Finished(Err(novarocks_functions::EvaluationFailure::Kernel(
                KernelFailure::Operational(KernelDiagnostic::new(PAYLOAD)),
            ))),
        );
        assert_eq!(summary.matched_panics, 0);
        assert_eq!(summary.matched_failures, 0);
        assert_eq!(details.len(), 1);
        assert!(details[0].contains(super::super::LEGACY_PANIC_PREFIX));
    }
    #[test]
    fn aggregate_exact_opted_in_panic_increments_only_separate_panic_evidence() {
        let (summary, details) = compare(
            AggregateDiffSpec::new("min_n").expected_panic_payload(PAYLOAD),
            legacy_panic(),
            pure_panic(PAYLOAD),
        );
        assert_eq!(summary.matched_panics, 1);
        assert_eq!(summary.matched_failures, 0);
        assert_eq!(summary.null_results, 0);
        assert!(details.is_empty());
    }
}

// This test-only port consumes authored encoded states through both actual
// merge lifecycles; it never decodes or computes the payload in the harness.
pub(super) fn assert_encoded_aggregate_matches_v1(spec: AggregateDiffSpec, encoded: ArrayRef) {
    let catalog = builtin_engine_function_catalog();
    let bound = resolve_aggregate_spec(catalog, &spec).unwrap();
    let signature = resolved_aggregate_signature_from_binding(bound.clone()).unwrap();
    let FunctionResultType::Scalar(result_type) = &bound.selected.result_type else {
        panic!("scalar aggregate result")
    };
    let state_type = bound
        .selected
        .aggregate
        .as_ref()
        .unwrap()
        .intermediate_type
        .clone();
    let legacy = LegacyAggregate::try_new(&spec, &signature).unwrap();
    let groups = encoded.len();
    for intermediate in [false, true] {
        let phase = if intermediate {
            AggregateKernelPhase::Intermediate
        } else {
            AggregateKernelPhase::Final
        };
        let handle = prepare_aggregate(catalog, &spec, &bound, &state_type, &[], phase).unwrap();
        let legacy_result = guard_legacy(|| {
            let mut arena = LegacyArena::new();
            let states = LegacyStates::new(&legacy.merge, &mut arena, groups)?;
            let entry = &legacy.merge.entries[0];
            entry.merge_batch(
                &states.pointers,
                AggregateInputBatch::try_new(Some(&encoded), groups).map_err(|e| e.to_string())?,
            )?;
            entry.build_array(&states.pointers, intermediate)
        });
        let pure_result = guard_pure(|| {
            let mut column = PureAggregate::column(&handle, groups)?;
            let input = SelectedAggregateMergeInput::try_new(
                handle.contract(),
                Selection::all(groups),
                EvaluatedArgument::Column(&encoded),
                &HarnessControl,
            )?;
            column
                .prepare_merge_batch_evaluation(
                    &(0..groups).collect::<Vec<_>>(),
                    input,
                    &HarnessControl,
                )?
                .run(&HarnessControl)?;
            column.emit_evaluation(&(0..groups).collect::<Vec<_>>(), groups, &HarnessControl)
        });
        let expected_type = if intermediate {
            &state_type
        } else {
            result_type
        };
        let mut summary = AggregateDiffSummary {
            function: bound.function_id.clone(),
            overload: bound.selected.overload.clone(),
            result_type: expected_type.clone(),
            pure_state_type: state_type.clone(),
            legacy_intermediate_type: signature.intermediate_type.clone(),
            rows: groups,
            groups,
            partitions: 1,
            matched_failures: 0,
            matched_panics: 0,
            null_results: 0,
        };
        let mut details = Vec::new();
        compare_shape(
            &spec,
            expected_type,
            "authored encoded state",
            groups,
            legacy_result,
            pure_result,
            &mut summary,
            &mut details,
        );
        assert!(
            details.is_empty(),
            "encoded state phase {phase:?} mismatch: {details:?}"
        );
    }
}
