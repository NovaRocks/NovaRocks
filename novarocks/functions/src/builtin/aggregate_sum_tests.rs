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

//! R-SUM-exact through the installed owner and the erased state column: the
//! value is the exact sum, and the only overflow is the result's, reported
//! when the final value is built, independently of order, batch split and
//! Partial/Final split.

use super::*;
use arrow_array::{Decimal128Array, Decimal256Array, FixedSizeBinaryArray, Float64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationRegionId, ExpressionEffectContext, ExpressionUseId, PureCompileControl,
    SemanticParameters, ValueLogicalType,
};
use std::{num::NonZeroUsize, time::Duration};

struct CompileControl;
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
struct RuntimeControl;
impl KernelEvaluationControl for RuntimeControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("SUM never waits")
    }
}

fn context() -> novarocks_type_contract::RelationEffectContext {
    novarocks_type_contract::RelationEffectContext::call(
        novarocks_type_contract::RelationCallId::new(0),
    )
}

fn catalog() -> PureEngineFunctionCatalog {
    let original = crate::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            original
                .definition("sum", FunctionKind::Aggregate)
                .unwrap()
                .clone(),
        )
        .unwrap();
    // Independent inventory of the one installed SUM owner.
    builder
        .seal_pure([InstalledPureKernel {
            function: FunctionId::try_new("builtin.aggregate/sum/v1").unwrap(),
            kind: FunctionKind::Aggregate,
            implementation: PureImplementationDeclaration {
                overload: FunctionOverloadId::try_new("builtin.aggregate/sum/derived-v1").unwrap(),
                implementation: PureImplementationId::try_new("builtin.aggregate/sum/selected-v1")
                    .unwrap(),
                abi: PureKernelAbi::AggregateWindowV1,
            },
            aggregate_state_format: Some(
                AggregateStateFormatIdentity::try_new("novarocks/sum/state-v2").unwrap(),
            ),
        }])
        .unwrap()
}

struct Fixture {
    catalog: PureEngineFunctionCatalog,
    id: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    args: Vec<FunctionArgument>,
    uses: Vec<Option<ExpressionUseId>>,
    parameters: SemanticParameters,
    policy: DecimalOverflowPolicy,
    state: FunctionValueType,
}

impl Fixture {
    fn new(source: FunctionValueType, policy: DecimalOverflowPolicy) -> Self {
        let catalog = catalog();
        let args = vec![FunctionArgument::Value {
            value_type: source,
            constant: None,
        }];
        let bound = catalog
            .metadata()
            .resolve_bound_user(
                "sum",
                FunctionKind::Aggregate,
                FunctionBindingRequest {
                    arguments: &args,
                    logical_argument_count: 1,
                    expected_result_type: None,
                },
                &CompileControl,
            )
            .unwrap();
        let state = bound
            .selected
            .aggregate
            .as_ref()
            .unwrap()
            .intermediate_type
            .clone();
        Self {
            catalog,
            id: bound.function_id,
            selected: Arc::new(bound.selected),
            args,
            uses: vec![Some(ExpressionUseId::new(1))],
            parameters: SemanticParameters::try_new([]).unwrap(),
            policy,
            state,
        }
    }
    fn result(&self) -> &FunctionValueType {
        match &self.selected.result_type {
            FunctionResultType::Scalar(value) => value,
            _ => panic!("scalar SUM result"),
        }
    }
    fn prepare(
        &self,
        phase: AggregateKernelPhase,
        distinct: bool,
    ) -> Result<PreparedAggregateHandle, FunctionSpecializationFailure> {
        let mut input = CallEffectInput {
            function_id: &self.id,
            kind: FunctionKind::Aggregate,
            selected: &self.selected,
            request: FunctionBindingRequest {
                arguments: &self.args,
                logical_argument_count: 1,
                expected_result_type: None,
            },
            argument_uses: CallArgumentUses::SelectedChannels(&self.uses),
            context: (context()).into(),
            parameters: &self.parameters,
            environment: &[],
            decimal_overflow_policy: self.policy,
            proof_scope: CallProofScope::Relation(context().identity),
        };
        if !phase.consumes_logical_arguments() {
            input.argument_uses = CallArgumentUses::AggregateMerge {
                phase,
                state_context: (ExpressionEffectContext {
                    use_id: ExpressionUseId::new(u32::MAX),
                    region: EvaluationRegionId::new(u32::MAX - 1),
                    demand: EvaluationDemand::Value,
                })
                .into(),
                state_input_type: &self.state,
            };
        }
        let prepared = self.catalog.prepare_fresh(
            input,
            self.selected.clone(),
            PureCallPreparation::Aggregate {
                arguments: ScopedRelationalEffects::pure_value(context()),
                options: AggregatePreparationOptions {
                    state_interpretation: None,
                    phase,
                    distinct,
                    order_keys: Arc::from([]),
                    state_input_type: (!phase.consumes_logical_arguments())
                        .then(|| self.state.clone()),
                },
            },
            &CompileControl,
        )?;
        let PreparedPureKernel::Aggregate(handle) = prepared.into_prepared() else {
            panic!("SUM prepares an aggregate handle")
        };
        Ok(handle)
    }
    fn handle(&self, phase: AggregateKernelPhase) -> PreparedAggregateHandle {
        self.prepare(phase, false)
            .unwrap_or_else(|error| panic!("SUM {phase:?} prepares: {error}"))
    }
}

/// One group's state column driven through the erased handle.
struct Group {
    handle: PreparedAggregateHandle,
    column: AggregateStateColumn,
}

impl Group {
    fn new(handle: PreparedAggregateHandle) -> Self {
        let mut column = AggregateStateColumn::try_new(
            handle.clone(),
            Arc::new(UnaccountedAggregateStateAllocator),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap();
        column.push(&RuntimeControl).unwrap();
        Self { handle, column }
    }
    fn update(&mut self, values: &ArrayRef) -> Result<(), KernelFailure> {
        let contract = self.handle.contract().clone();
        let mapping = vec![0; values.len()];
        let arguments = [EvaluatedArgument::Column(values)];
        let input = SelectedAggregateUpdateInput::try_new(
            &contract,
            Selection::all(values.len()),
            &arguments,
            &[],
            &RuntimeControl,
        )?;
        self.column
            .prepare_update_batch(&mapping, input, &RuntimeControl)?
            .run(&RuntimeControl)
    }
    fn merge(&mut self, states: &ArrayRef) -> Result<(), KernelFailure> {
        let contract = self.handle.contract().clone();
        let mapping = vec![0; states.len()];
        let input = SelectedAggregateMergeInput::try_new(
            &contract,
            Selection::all(states.len()),
            EvaluatedArgument::Column(states),
            &RuntimeControl,
        )?;
        self.column
            .prepare_merge_batch(&mapping, input, &RuntimeControl)?
            .run(&RuntimeControl)
    }
    fn emit(&self) -> Result<ArrayRef, KernelFailure> {
        self.column.emit(&[0], 1, &RuntimeControl)
    }
}

fn int64s(values: &[Option<i64>]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

fn bigint(array: &ArrayRef) -> Option<i64> {
    let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
    (!array.is_null(0)).then(|| array.value(0))
}

fn is_numeric_data(result: Result<ArrayRef, KernelFailure>) -> bool {
    use novarocks_failure_contract::FailureProjection;
    match result {
        Err(error) => {
            assert_eq!(
                error.failure_fact(),
                DataFailureReason::NumericOutOfRange.failure_fact()
            );
            true
        }
        Ok(_) => false,
    }
}

/// Single-phase SUM of `values`, fed as one batch per `split` boundary.
fn single(fixture: &Fixture, batches: &[ArrayRef]) -> Result<ArrayRef, KernelFailure> {
    let mut group = Group::new(fixture.handle(AggregateKernelPhase::Single));
    for batch in batches {
        group.update(batch)?;
    }
    group.emit()
}

/// Two Partial groups over `left` and `right`, merged by one Final.
fn two_phase(
    fixture: &Fixture,
    left: &ArrayRef,
    right: &ArrayRef,
) -> Result<ArrayRef, KernelFailure> {
    let mut states = Vec::new();
    for part in [left, right] {
        let mut partial = Group::new(fixture.handle(AggregateKernelPhase::Partial));
        partial.update(part)?;
        // A partial emission never range-checks the exact sum.
        let state = partial.emit()?;
        assert_eq!(state.data_type(), fixture.state.data_type());
        states.push(state);
    }
    let mut last = Group::new(fixture.handle(AggregateKernelPhase::Final));
    for state in &states {
        last.merge(state)?;
    }
    last.emit()
}

fn permutations<T: Copy>(values: &[T]) -> Vec<Vec<T>> {
    if values.len() <= 1 {
        return vec![values.to_vec()];
    }
    let mut all = Vec::new();
    for index in 0..values.len() {
        let mut rest = values.to_vec();
        let first = rest.remove(index);
        for mut tail in permutations(&rest) {
            tail.insert(0, first);
            all.push(tail);
        }
    }
    all
}

#[test]
fn sum_types_follow_the_exact_ruling_for_every_input_domain() {
    let largeint = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    for (source, result, state, domain) in [
        (
            DataType::Boolean,
            DataType::Int64,
            DataType::Decimal128(38, 0),
            SumDomain::Integer,
        ),
        (
            DataType::Int8,
            DataType::Int64,
            DataType::Decimal128(38, 0),
            SumDomain::Integer,
        ),
        (
            DataType::Int16,
            DataType::Int64,
            DataType::Decimal128(38, 0),
            SumDomain::Integer,
        ),
        (
            DataType::Int32,
            DataType::Int64,
            DataType::Decimal128(38, 0),
            SumDomain::Integer,
        ),
        (
            DataType::Int64,
            DataType::Int64,
            DataType::Decimal128(38, 0),
            SumDomain::Integer,
        ),
        (
            DataType::Float32,
            DataType::Float64,
            DataType::Float64,
            SumDomain::Float,
        ),
        (
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            SumDomain::Float,
        ),
        (
            DataType::Decimal128(20, 2),
            DataType::Decimal128(38, 2),
            DataType::Decimal256(76, 2),
            SumDomain::Decimal,
        ),
    ] {
        let fixture = Fixture::new(
            FunctionValueType::from_legacy_declared_projection(source.clone(), true),
            DecimalOverflowPolicy::ReportError,
        );
        assert_eq!(fixture.result().data_type().clone(), result, "{source:?}");
        assert!(fixture.result().nullable());
        assert_eq!(fixture.state.data_type().clone(), state, "{source:?}");
        assert!(fixture.state.nullable());
        for phase in [
            AggregateKernelPhase::Single,
            AggregateKernelPhase::Partial,
            AggregateKernelPhase::Intermediate,
            AggregateKernelPhase::Final,
        ] {
            let handle = fixture.handle(phase);
            assert_eq!(handle.contract().phase(), phase);
            assert_eq!(
                handle.contract().state_format().as_str(),
                "novarocks/sum/state-v2"
            );
        }
        let expected = if domain == SumDomain::Float {
            SumAccumulation::Unordered
        } else {
            SumAccumulation::Exact
        };
        assert_eq!(domain.accumulation(), expected);
    }
    let fixture = Fixture::new(largeint.clone(), DecimalOverflowPolicy::ReportError);
    assert_eq!(fixture.result(), &largeint);
    assert_eq!(
        fixture.state,
        FunctionValueType::new(DataType::Decimal256(76, 0), true)
    );
    fixture.handle(AggregateKernelPhase::Final);
}

#[test]
fn decimal256_and_distinct_sum_have_no_exact_owner() {
    let fixture = Fixture::new(
        FunctionValueType::new(DataType::Decimal256(50, 5), true),
        DecimalOverflowPolicy::ReportError,
    );
    let error = fixture
        .prepare(AggregateKernelPhase::Single, false)
        .unwrap_err();
    assert!(error.to_string().contains("DECIMAL256"), "{error}");
    let fixture = Fixture::new(
        FunctionValueType::new(DataType::Int64, true),
        DecimalOverflowPolicy::ReportError,
    );
    let error = fixture
        .prepare(AggregateKernelPhase::Partial, true)
        .unwrap_err();
    assert!(error.to_string().contains("DISTINCT"), "{error}");
}

#[test]
fn bigint_sum_is_exact_in_every_order_batch_split_and_phase_split() {
    let fixture = Fixture::new(
        FunctionValueType::new(DataType::Int64, true),
        DecimalOverflowPolicy::ReportError,
    );
    // A running BIGINT add would overflow at MAX + 1 in some orders; the
    // exact sum is MAX in all of them.
    for values in permutations(&[Some(i64::MAX), Some(1), Some(-1), None]) {
        for split in 0..=values.len() {
            let (left, right) = values.split_at(split);
            let (left, right) = (int64s(left), int64s(right));
            assert_eq!(
                bigint(&single(&fixture, &[left.clone(), right.clone()]).unwrap()),
                Some(i64::MAX),
                "{values:?} split at {split}"
            );
            assert_eq!(
                bigint(&two_phase(&fixture, &left, &right).unwrap()),
                Some(i64::MAX),
                "{values:?} split at {split}"
            );
        }
    }
}

#[test]
fn bigint_sum_overflow_is_numeric_data_of_the_final_value_only() {
    let fixture = Fixture::new(
        FunctionValueType::new(DataType::Int64, true),
        DecimalOverflowPolicy::ReportError,
    );
    for values in [
        vec![Some(i64::MAX), Some(1)],
        vec![Some(i64::MIN), Some(-1)],
        vec![Some(i64::MAX), Some(i64::MAX), Some(i64::MAX)],
    ] {
        let batch = int64s(&values);
        // Every update succeeds; only building the BIGINT result fails.
        let mut group = Group::new(fixture.handle(AggregateKernelPhase::Single));
        group.update(&batch).unwrap();
        assert!(is_numeric_data(group.emit()), "{values:?}");
        assert!(is_numeric_data(two_phase(&fixture, &batch, &int64s(&[]))));
    }
    // The partial state carries the exact out-of-range sum.
    let mut partial = Group::new(fixture.handle(AggregateKernelPhase::Partial));
    partial.update(&int64s(&[Some(i64::MAX), Some(1)])).unwrap();
    let state = partial.emit().unwrap();
    let state = state.as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!(state.value(0), i128::from(i64::MAX) + 1);
    assert_eq!(
        bigint(&single(&fixture, &[int64s(&[Some(i64::MIN), Some(i64::MAX)])]).unwrap()),
        Some(-1)
    );
}

#[test]
fn a_group_without_a_value_sums_to_null_in_every_phase() {
    let fixture = Fixture::new(
        FunctionValueType::new(DataType::Int64, true),
        DecimalOverflowPolicy::ReportError,
    );
    assert_eq!(bigint(&single(&fixture, &[]).unwrap()), None);
    assert_eq!(
        bigint(&single(&fixture, &[int64s(&[None, None])]).unwrap()),
        None
    );
    assert_eq!(
        bigint(&two_phase(&fixture, &int64s(&[None]), &int64s(&[])).unwrap()),
        None
    );
}

fn decimal38(values: &[Option<i128>]) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values.to_vec())
            .with_precision_and_scale(38, 0)
            .unwrap(),
    )
}

#[test]
fn decimal_sum_checks_38_digits_and_follows_the_frozen_overflow_policy() {
    let max = 99_999_999_999_999_999_999_999_999_999_999_999_999_i128;
    for policy in [
        DecimalOverflowPolicy::ReportError,
        DecimalOverflowPolicy::OutputNull,
    ] {
        let fixture = Fixture::new(
            FunctionValueType::new(DataType::Decimal128(38, 0), true),
            policy,
        );
        let result = |batches: &[ArrayRef]| single(&fixture, batches);
        let value = |array: ArrayRef| {
            let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            (!array.is_null(0)).then(|| array.value(0))
        };
        // 10^38 - 1 fits; the order of a transient excess does not matter.
        for values in permutations(&[Some(max), Some(1), Some(-1)]) {
            assert_eq!(value(result(&[decimal38(&values)]).unwrap()), Some(max));
        }
        // 10^38 fits i128 but not 38 digits; 2 * (10^38 - 1) fits neither.
        for values in [
            vec![Some(max), Some(1)],
            vec![Some(max), Some(max)],
            vec![Some(-max), Some(-1)],
        ] {
            match policy {
                DecimalOverflowPolicy::ReportError => {
                    assert!(is_numeric_data(result(&[decimal38(&values)])), "{values:?}")
                }
                DecimalOverflowPolicy::OutputNull => {
                    assert_eq!(value(result(&[decimal38(&values)]).unwrap()), None)
                }
            }
        }
        // The Decimal256 partial state carries the excess exactly.
        let left = decimal38(&[Some(max), Some(max)]);
        let right = decimal38(&[Some(-max)]);
        assert_eq!(
            value(two_phase(&fixture, &left, &right).unwrap()),
            Some(max)
        );
    }
}

#[test]
fn largeint_sum_is_exact_and_reports_overflow_when_the_result_is_built() {
    let largeint = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let fixture = Fixture::new(largeint, DecimalOverflowPolicy::OutputNull);
    let values = |values: &[i128]| -> ArrayRef {
        Arc::new(
            FixedSizeBinaryArray::try_from_iter(values.iter().map(|value| value.to_be_bytes()))
                .unwrap(),
        )
    };
    let result = two_phase(&fixture, &values(&[i128::MAX, 1]), &values(&[-1])).unwrap();
    let result = result
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(
        i128::from_be_bytes(result.value(0).try_into().unwrap()),
        i128::MAX
    );
    // LARGEINT has no NULL-on-overflow policy: the overflow is an error.
    assert!(is_numeric_data(single(
        &fixture,
        &[values(&[i128::MAX, 1])]
    )));
    let mut partial = Group::new(fixture.handle(AggregateKernelPhase::Partial));
    partial.update(&values(&[i128::MAX, i128::MAX])).unwrap();
    let state = partial.emit().unwrap();
    assert!(state.as_any().is::<Decimal256Array>());
}

#[test]
fn floating_sum_declares_unordered_accumulation() {
    let fixture = Fixture::new(
        FunctionValueType::new(DataType::Float64, true),
        DecimalOverflowPolicy::ReportError,
    );
    let values: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.5), None, Some(2.25)]));
    let result = single(&fixture, &[values]).unwrap();
    let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(result.value(0), 3.75);
    assert_eq!(SumDomain::Float.accumulation(), SumAccumulation::Unordered);
}
