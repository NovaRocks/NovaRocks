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

//! SUM, MIN and MAX OVER through the installed owners and the generic adapter.
//! The oracle is the plain installed aggregate run once per frame over exactly
//! that frame's rows through the erased state column: each row's value, NULL
//! and failure must be that aggregate's, including empty frames and SUM's
//! finalize-time overflow.

use super::*;
use crate::*;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, ExpressionUseId,
    SemanticParameters, ValueLogicalType, WindowBound, WindowFrame, WindowFrameUnits,
};
use std::{num::NonZeroUsize, sync::Mutex, time::Duration};

#[derive(Default)]
struct CompileControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "compile callback after primary");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "runtime callback after primary");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if at == *stop => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("aggregate OVER never waits")
    }
}

fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original refusal"),
        internal("original refusal"),
        KernelFailure::Operational(KernelDiagnostic::new("original refusal")),
        KernelFailure::InstanceFailed,
    ]
}

fn context() -> novarocks_type_contract::RelationEffectContext {
    novarocks_type_contract::RelationEffectContext::call(
        novarocks_type_contract::RelationCallId::new(41),
    )
}

struct Fixture {
    catalog: EngineFunctionCatalog,
    function: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    uses: Vec<Option<ExpressionUseId>>,
    parameters: SemanticParameters,
    policy: DecimalOverflowPolicy,
}

impl Fixture {
    fn new(name: &str, source: FunctionValueType, policy: DecimalOverflowPolicy) -> Self {
        let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
        let arguments = vec![FunctionArgument::Value {
            value_type: source,
            constant: None,
        }];
        let resolved = catalog
            .resolve_bound_user(
                name,
                FunctionKind::Aggregate,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: 1,
                    expected_result_type: None,
                },
                &CompileControl::default(),
            )
            .unwrap_or_else(|error| panic!("{name} binds: {error}"));
        Self {
            catalog,
            function: resolved.function_id,
            selected: Arc::new(resolved.selected),
            arguments,
            uses: vec![Some(ExpressionUseId::new(11))],
            parameters: SemanticParameters::try_new([]).unwrap(),
            policy,
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: (context()).into(),
            argument_uses: crate::CallArgumentUses::SelectedChannels(&self.uses),
            function_id: &self.function,
            kind: FunctionKind::Aggregate,
            selected: &self.selected,
            request: FunctionBindingRequest {
                arguments: &self.arguments,
                logical_argument_count: 1,
                expected_result_type: None,
            },
            environment: &[],
            parameters: &self.parameters,
            decimal_overflow_policy: self.policy,
            proof_scope: CallProofScope::Relation(context().identity),
        }
    }
    fn aggregate_options(distinct: bool) -> AggregatePreparationOptions {
        AggregatePreparationOptions {
            state_interpretation: None,
            phase: AggregateKernelPhase::Single,
            distinct,
            order_keys: Arc::from([]),
            state_input_type: None,
        }
    }
    fn try_window(
        &self,
        frame: Option<WindowFrame<u64>>,
        ignore_nulls: bool,
        distinct: bool,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedWindowKernel>, FunctionSpecializationFailure> {
        let prepared = self.catalog.prepare_fresh_selected(
            self.input(),
            self.selected.clone(),
            PureCallPreparation::AggregateWindow {
                arguments: ScopedRelationalEffects::pure_value(context()),
                options: AggregateWindowPreparationOptions {
                    aggregate: Self::aggregate_options(distinct),
                    window: WindowCallOptions::try_new(
                        frame,
                        ignore_nulls,
                        &CompileControl::default(),
                    )
                    .unwrap(),
                },
            },
            control,
        )?;
        assert_eq!(
            prepared.implementation().abi,
            PureKernelAbi::AggregateWindowV1
        );
        match prepared.into_prepared() {
            PreparedPureKernel::Window(kernel) => Ok(kernel),
            _ => panic!("aggregate OVER prepares a window kernel"),
        }
    }
    fn window(&self) -> Arc<dyn PreparedWindowKernel> {
        self.try_window(None, false, false, &CompileControl::default())
            .unwrap_or_else(|error| panic!("aggregate OVER prepares: {error}"))
    }
    fn aggregate(&self) -> PreparedAggregateHandle {
        let prepared = self
            .catalog
            .prepare_fresh_selected(
                self.input(),
                self.selected.clone(),
                PureCallPreparation::Aggregate {
                    arguments: ScopedRelationalEffects::pure_value(context()),
                    options: Self::aggregate_options(false),
                },
                &CompileControl::default(),
            )
            .unwrap_or_else(|error| panic!("plain aggregate prepares: {error}"));
        let PreparedPureKernel::Aggregate(handle) = prepared.into_prepared() else {
            panic!("plain aggregate prepares an aggregate handle")
        };
        handle
    }
}

fn ranges(values: &[(usize, usize)]) -> Vec<WindowRowRange> {
    values
        .iter()
        .map(|&(start, end)| WindowRowRange { start, end })
        .collect()
}

/// One peer group covering the partition: the adapter reads only frames.
fn whole(rows: usize) -> Vec<WindowRowRange> {
    if rows == 0 {
        Vec::new()
    } else {
        ranges(&[(0, rows)])
    }
}

fn begin<'a>(
    kernel: &'a Arc<dyn PreparedWindowKernel>,
    arguments: &'a [EvaluatedArgument<'a>],
    peers: &'a [WindowRowRange],
    frames: &'a [WindowRowRange],
    control: &dyn KernelEvaluationControl,
) -> Result<WindowEvaluationPartition<'a>, KernelFailure> {
    let full = FullPartitionWindowInput::try_new(
        kernel.contract(),
        frames.len(),
        arguments,
        &[],
        &Control::default(),
    )
    .unwrap();
    let input = WindowPartitionInput::try_new(full, peers, frames, &Control::default()).unwrap();
    WindowEvaluationPartition::begin(kernel.clone(), input, control)
}

/// Every row's window value, or the partition's failure.
fn run_window(
    kernel: &Arc<dyn PreparedWindowKernel>,
    values: &ArrayRef,
    frames: &[WindowRowRange],
) -> Result<ArrayRef, KernelFailure> {
    let control = Control::default();
    let rows = frames.len();
    let arguments = [EvaluatedArgument::Column(values)];
    let peers = whole(rows);
    let mut partition = begin(kernel, &arguments, &peers, frames, &control)?;
    // One inline state per row and nothing else is retained.
    assert_eq!(
        partition.retained_bytes().unwrap(),
        partition.retained_upper_bound()
    );
    let output = partition.evaluate(Selection::all(rows), rows, &control)?;
    partition.finish(&control)?;
    let (_, values, errors) = output.into_parts();
    assert!(errors.is_empty());
    Ok(values)
}

/// The plain aggregate of exactly the frame's rows, in partition order.
fn oracle(
    handle: &PreparedAggregateHandle,
    values: &ArrayRef,
    frame: WindowRowRange,
) -> Result<ArrayRef, KernelFailure> {
    let control = Control::default();
    let mut column = AggregateStateColumn::try_new(
        handle.clone(),
        Arc::new(UnaccountedAggregateStateAllocator),
        NonZeroUsize::new(1).unwrap(),
    )?;
    column.push(&control)?;
    let rows = values.slice(frame.start, frame.end - frame.start);
    if !rows.is_empty() {
        let contract = handle.contract().clone();
        let mapping = vec![0; rows.len()];
        let arguments = [EvaluatedArgument::Column(&rows)];
        let input = SelectedAggregateUpdateInput::try_new(
            &contract,
            Selection::all(rows.len()),
            &arguments,
            &[],
            &control,
        )?;
        column
            .prepare_update_batch(&mapping, input, &control)?
            .run(&control)?;
    }
    column.emit(&[0], 1, &control)
}

/// The window against the per-frame oracle. A partition fails exactly when
/// one of its frames fails, with that frame's own failure.
fn check(
    fixture: &Fixture,
    window: &Arc<dyn PreparedWindowKernel>,
    values: &ArrayRef,
    frames: &[WindowRowRange],
) -> Result<ArrayRef, KernelFailure> {
    let handle = fixture.aggregate();
    let expected = frames
        .iter()
        .map(|frame| oracle(&handle, values, *frame))
        .collect::<Vec<_>>();
    let actual = run_window(window, values, frames);
    match expected.iter().find_map(|value| value.as_ref().err()) {
        Some(error) => {
            use novarocks_failure_contract::FailureProjection;
            let actual = actual.as_ref().unwrap_err();
            assert_eq!(actual.failure_fact(), error.failure_fact(), "{frames:?}");
            assert_eq!(actual.to_string(), error.to_string(), "{frames:?}");
        }
        None => {
            let actual = actual.as_ref().unwrap();
            assert_eq!(actual.len(), frames.len());
            for (row, expected) in expected.iter().enumerate() {
                assert_eq!(
                    actual.slice(row, 1).as_ref(),
                    expected.as_ref().unwrap().as_ref(),
                    "row {row} of {frames:?}"
                );
            }
        }
    }
    actual
}

/// A small deterministic generator for frames and values.
struct Lcg(u64);
impl Lcg {
    fn next(&mut self, bound: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((self.0 >> 33) as usize) % bound
    }
    fn pick<T: Copy>(&mut self, values: &[T]) -> T {
        values[self.next(values.len())]
    }
    /// A value from `values`, or NULL for about a quarter of the rows.
    fn cell<T: Copy>(&mut self, values: &[T]) -> Option<T> {
        (self.next(4) != 0).then(|| self.pick(values))
    }
}

/// Arbitrary frames, about a fifth of them empty.
fn random_frames(rows: usize, seed: u64) -> Vec<WindowRowRange> {
    let mut random = Lcg(seed);
    (0..rows)
        .map(|_| {
            let start = random.next(rows + 1);
            let end = if random.next(5) == 0 {
                start
            } else {
                start + random.next(rows + 1 - start)
            };
            WindowRowRange { start, end }
        })
        .collect()
}

/// The frame tables the SQL frame clauses produce, stated independently.
fn shaped_frames(rows: usize) -> Vec<Vec<WindowRowRange>> {
    let table = |bounds: &dyn Fn(usize) -> (usize, usize)| {
        (0..rows)
            .map(|row| {
                let (start, end) = bounds(row);
                WindowRowRange { start, end }
            })
            .collect::<Vec<_>>()
    };
    vec![
        // ROWS UNBOUNDED PRECEDING .. CURRENT ROW
        table(&|row| (0, row + 1)),
        // ROWS CURRENT ROW .. CURRENT ROW: one value always fits its result
        table(&|row| (row, row + 1)),
        // ROWS 2 PRECEDING .. 2 FOLLOWING
        table(&|row| (row.saturating_sub(2), (row + 3).min(rows))),
        // ROWS CURRENT ROW .. UNBOUNDED FOLLOWING
        table(&|row| (row, rows)),
        // The whole partition
        table(&|_| (0, rows)),
        // RANGE UNBOUNDED PRECEDING .. CURRENT ROW over three-row peer groups
        table(&|row| (0, ((row / 3 + 1) * 3).min(rows))),
        // ROWS UNBOUNDED PRECEDING .. 1 PRECEDING: the first frame is empty
        table(&|row| (0, row)),
        // ROWS 1 FOLLOWING .. 2 FOLLOWING: the last frame is empty
        table(&|row| ((row + 1).min(rows), (row + 3).min(rows))),
    ]
}

const ROWS: usize = 23;

fn nullable(array: &ArrayRef) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(array.data_type().clone(), true)
}

fn largeint_type() -> FunctionValueType {
    FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap()
}

fn largeints(values: &[Option<i128>]) -> ArrayRef {
    Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            values.iter().map(|value| value.map(i128::to_be_bytes)),
            16,
        )
        .unwrap(),
    )
}

fn decimals(values: &[Option<i128>], precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values.to_vec())
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

fn int64s(values: &[Option<i64>]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

/// Largest DECIMAL(38) magnitude.
const MAX38: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

/// One input carrier: its value type and its partition values.
struct Source {
    label: &'static str,
    value_type: FunctionValueType,
    values: ArrayRef,
}

fn sources(seed: u64) -> Vec<Source> {
    let mut random = Lcg(seed);
    let r = &mut random;
    let small = int64s(
        &(0..ROWS)
            .map(|_| r.cell(&[-50, -7, -1, 0, 1, 3, 12, 49]))
            .collect::<Vec<_>>(),
    );
    let large = int64s(
        &(0..ROWS)
            .map(|_| r.cell(&[i64::MAX, i64::MIN, 1, -1, i64::MAX - 3, i64::MIN + 2]))
            .collect::<Vec<_>>(),
    );
    let int32: ArrayRef = Arc::new(Int32Array::from(
        (0..ROWS)
            .map(|_| r.cell(&[i32::MAX, i32::MIN, -5, 0, 8]))
            .collect::<Vec<_>>(),
    ));
    let boolean: ArrayRef = Arc::new(BooleanArray::from(
        (0..ROWS)
            .map(|_| r.cell(&[true, false]))
            .collect::<Vec<_>>(),
    ));
    let float: ArrayRef = Arc::new(Float64Array::from(
        (0..ROWS)
            .map(|_| {
                r.cell(&[
                    f64::NAN,
                    -0.0,
                    0.0,
                    1.5,
                    -2.25,
                    f64::INFINITY,
                    f64::NEG_INFINITY,
                    1e300,
                ])
            })
            .collect::<Vec<_>>(),
    ));
    let decimal = decimals(
        &(0..ROWS)
            .map(|_| r.cell(&[-99_999_999, -125, 0, 1, 250, 99_999_999]))
            .collect::<Vec<_>>(),
        10,
        2,
    );
    let wide_decimal = decimals(
        &(0..ROWS)
            .map(|_| r.cell(&[MAX38, -MAX38, 1, -1, MAX38 - 5]))
            .collect::<Vec<_>>(),
        38,
        0,
    );
    let largeint = largeints(
        &(0..ROWS)
            .map(|_| r.cell(&[i128::MAX, i128::MIN, 1, -1, 7]))
            .collect::<Vec<_>>(),
    );
    let date: ArrayRef = Arc::new(Date32Array::from(
        (0..ROWS)
            .map(|_| r.cell(&[-719_162, -1, 0, 19_000, 2_932_896]))
            .collect::<Vec<_>>(),
    ));
    let plain = |label, values: ArrayRef| Source {
        label,
        value_type: nullable(&values),
        values,
    };
    vec![
        plain("bigint", small),
        plain("bigint near its range", large),
        plain("int", int32),
        plain("boolean", boolean),
        plain("double", float),
        plain("decimal(10,2)", decimal),
        plain("decimal(38,0) near its range", wide_decimal),
        Source {
            label: "largeint",
            value_type: largeint_type(),
            values: largeint,
        },
        plain("date", date),
    ]
}

#[test]
fn sum_min_max_over_any_frame_equal_the_plain_aggregate_of_that_frame() {
    let mut outcomes = std::collections::BTreeMap::<(&str, &str, bool), usize>::new();
    for seed in 1..=3 {
        for source in sources(seed) {
            for name in ["sum", "min", "max"] {
                if name == "sum" && source.label == "date" {
                    continue;
                }
                // Only a DECIMAL result consults the frozen overflow policy.
                let policies: &[DecimalOverflowPolicy] = if source.label.starts_with("decimal") {
                    &[
                        DecimalOverflowPolicy::ReportError,
                        DecimalOverflowPolicy::OutputNull,
                    ]
                } else {
                    &[DecimalOverflowPolicy::ReportError]
                };
                for &policy in policies {
                    let fixture = Fixture::new(name, source.value_type.clone(), policy);
                    let window = fixture.window();
                    let mut tables = shaped_frames(ROWS);
                    tables.extend((0..8).map(|offset| random_frames(ROWS, seed * 100 + offset)));
                    for frames in tables {
                        let result = check(&fixture, &window, &source.values, &frames);
                        *outcomes
                            .entry((name, source.label, result.is_ok()))
                            .or_default() += 1;
                    }
                }
            }
        }
    }
    // Both outcomes actually ran where a result can overflow its type.
    for label in [
        "bigint near its range",
        "largeint",
        "decimal(38,0) near its range",
    ] {
        assert!(outcomes.contains_key(&("sum", label, true)), "{label}");
        assert!(outcomes.contains_key(&("sum", label, false)), "{label}");
    }
    assert!(!outcomes.contains_key(&("min", "largeint", false)));
}

fn assert_same_numeric_failure(actual: KernelFailure, expected: &KernelFailure) {
    use novarocks_failure_contract::FailureProjection;
    use std::error::Error;
    assert_eq!(
        actual.failure_fact(),
        DataFailureReason::NumericOutOfRange.failure_fact()
    );
    assert_eq!(actual.failure_fact(), expected.failure_fact());
    assert_eq!(actual.to_string(), expected.to_string());
    let mut source = actual.source();
    let mut conversion = false;
    while let Some(error) = source {
        conversion |= error.is::<std::num::TryFromIntError>();
        source = error.source();
    }
    assert!(conversion);
}

#[test]
fn sum_overflow_fails_only_a_frame_whose_own_result_overflows() {
    let values = int64s(&[Some(i64::MAX), Some(1), Some(-1)]);
    let fixture = Fixture::new("sum", nullable(&values), DecimalOverflowPolicy::ReportError);
    let window = fixture.window();
    let overflow = oracle(
        &fixture.aggregate(),
        &values,
        WindowRowRange { start: 0, end: 2 },
    )
    .unwrap_err();
    // The running frame [0, 2) holds MAX + 1.
    let running = ranges(&[(0, 1), (0, 2), (0, 3)]);
    assert_same_numeric_failure(
        check(&fixture, &window, &values, &running).unwrap_err(),
        &overflow,
    );
    // The same exact running state passes MAX + 1 between two frames; no
    // frame ends there, so nothing overflows.
    let skipping = ranges(&[(0, 1), (0, 3), (0, 3)]);
    let output = check(&fixture, &window, &values, &skipping).unwrap();
    assert_eq!(
        output.as_any().downcast_ref::<Int64Array>().unwrap(),
        &Int64Array::from(vec![i64::MAX; 3])
    );
    let current = ranges(&[(0, 1), (1, 2), (2, 3)]);
    check(&fixture, &window, &values, &current).unwrap();
    // The overflow is required partition work, not an output-demand effect.
    let arguments = [EvaluatedArgument::Column(&values)];
    let peers = whole(3);
    assert_same_numeric_failure(
        begin(&window, &arguments, &peers, &running, &Control::default())
            .map(|_| ())
            .unwrap_err(),
        &overflow,
    );

    // DECIMAL follows the frozen policy for that frame alone.
    let values = decimals(&[Some(MAX38), Some(1), Some(-1)], 38, 0);
    let fixture = Fixture::new("sum", nullable(&values), DecimalOverflowPolicy::OutputNull);
    let output = check(&fixture, &fixture.window(), &values, &running).unwrap();
    assert_eq!(
        output.as_any().downcast_ref::<Decimal128Array>().unwrap(),
        &Decimal128Array::from(vec![Some(MAX38), None, Some(MAX38)])
            .with_precision_and_scale(38, 0)
            .unwrap()
    );
    let fixture = Fixture::new("sum", nullable(&values), DecimalOverflowPolicy::ReportError);
    let actual = check(&fixture, &fixture.window(), &values, &running).unwrap_err();
    use novarocks_failure_contract::FailureProjection;
    assert_eq!(
        actual.failure_fact(),
        DataFailureReason::NumericOutOfRange.failure_fact()
    );
    assert_eq!(actual.to_string(), "sum result overflows DECIMAL(38)");
}

#[test]
fn empty_and_all_null_frames_emit_null_and_values_ignore_null_inputs() {
    let values = int64s(&[None, Some(5), None, Some(-3)]);
    let frames = ranges(&[(0, 0), (0, 1), (1, 3), (0, 4)]);
    for (name, hand) in [
        ("sum", [None, None, Some(5), Some(2)]),
        ("min", [None, None, Some(5), Some(-3)]),
        ("max", [None, None, Some(5), Some(5)]),
    ] {
        let fixture = Fixture::new(name, nullable(&values), DecimalOverflowPolicy::ReportError);
        let output = check(&fixture, &fixture.window(), &values, &frames).unwrap();
        assert_eq!(
            output.as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(hand.to_vec()),
            "{name}"
        );
    }
    // A partition of no rows emits no rows.
    let empty = int64s(&[]);
    let fixture = Fixture::new("sum", nullable(&empty), DecimalOverflowPolicy::ReportError);
    assert_eq!(run_window(&fixture.window(), &empty, &[]).unwrap().len(), 0);
}

#[test]
fn sparse_output_emits_selected_rows_after_complete_partition_work() {
    let values = int64s(&[Some(4), None, Some(-2), Some(10)]);
    let fixture = Fixture::new("sum", nullable(&values), DecimalOverflowPolicy::ReportError);
    let window = fixture.window();
    let frames = ranges(&[(0, 1), (0, 2), (1, 4), (0, 4)]);
    let arguments = [EvaluatedArgument::Column(&values)];
    let peers = whole(4);
    let control = Control::default();
    let mut partition = begin(&window, &arguments, &peers, &frames, &control).unwrap();
    let rows = [1, 3];
    let output = partition
        .evaluate(Selection::try_sparse(4, &rows).unwrap(), 2, &control)
        .unwrap();
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        &Int64Array::from(vec![Some(4), Some(12)])
    );
    partition.finish(&control).unwrap();
    // The retained bound is one inline state per row; no row count can
    // overflow it silently.
    assert!(
        window.partition_retained_upper_bound(4).unwrap()
            < window.partition_retained_upper_bound(5).unwrap()
    );
    assert_eq!(
        window.partition_retained_upper_bound(usize::MAX),
        Err(KernelFailure::ResourceExhausted)
    );
}

fn refusal(result: Result<Arc<dyn PreparedWindowKernel>, FunctionSpecializationFailure>) -> String {
    match result {
        Err(FunctionSpecializationFailure::Kernel(KernelFailure::InvalidProgram(message))) => {
            message.message().to_owned()
        }
        other => panic!("expected an explicit refusal, got {other:?}"),
    }
}

#[test]
fn unsupported_aggregate_over_shapes_are_refused_by_name() {
    let control = CompileControl::default();
    let text: ArrayRef = Arc::new(StringArray::from(vec![Some("b"), None]));
    for name in ["min", "max"] {
        let fixture = Fixture::new(name, nullable(&text), DecimalOverflowPolicy::ReportError);
        assert_eq!(
            refusal(fixture.try_window(None, false, false, &control)),
            "MIN/MAX OVER a UTF-8 value has no inline window state"
        );
    }
    let values = int64s(&[Some(1)]);
    let sum = Fixture::new("sum", nullable(&values), DecimalOverflowPolicy::ReportError);
    assert_eq!(
        refusal(sum.try_window(None, true, false, &control)),
        "aggregate OVER IGNORE NULLS is unsupported"
    );
    let min = Fixture::new("min", nullable(&values), DecimalOverflowPolicy::ReportError);
    assert_eq!(
        refusal(min.try_window(None, false, true, &control)),
        "aggregate OVER DISTINCT has no window adapter"
    );
    let ties = WindowFrame {
        units: WindowFrameUnits::Rows,
        start: WindowBound::UnboundedPreceding,
        end: WindowBound::CurrentRow,
        exclusion: novarocks_type_contract::WindowFrameExclusion::Ties,
    };
    assert_eq!(
        refusal(min.try_window(Some(ties), false, false, &control)),
        "aggregate OVER frame exclusion is unsupported"
    );
    // The scalar aggregate and OVER path share each exact installed owner.
    let catalog = &sum.catalog;
    for name in ["sum", "min", "max", "avg"] {
        let fixture = Fixture::new(name, nullable(&values), DecimalOverflowPolicy::ReportError);
        let installed = catalog.pure_overload_declaration_observed(
            &fixture.function,
            FunctionKind::Aggregate,
            &fixture.selected.overload,
            &control,
        );
        assert_eq!(
            installed.unwrap().implementation().abi,
            PureKernelAbi::AggregateWindowV1
        );
    }
}

fn compile_cause(error: FunctionSpecializationFailure) -> CompileControlError {
    match error {
        FunctionSpecializationFailure::Control(cause) => cause,
        FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled) => {
            CompileControlError::Cancelled
        }
        FunctionSpecializationFailure::Kernel(KernelFailure::DeadlineExceeded) => {
            CompileControlError::DeadlineExceeded
        }
        FunctionSpecializationFailure::Kernel(KernelFailure::ResourceExhausted) => {
            CompileControlError::ResourceExhausted
        }
        other => panic!("unexpected cause {other:?}"),
    }
}

#[test]
fn every_preparation_callback_keeps_the_original_compile_cause() {
    let values = int64s(&[Some(1)]);
    for name in ["sum", "min", "max"] {
        let fixture = Fixture::new(name, nullable(&values), DecimalOverflowPolicy::ReportError);
        let baseline = CompileControl::default();
        fixture.try_window(None, false, false, &baseline).unwrap();
        let trace = baseline.trace.lock().unwrap().clone();
        assert!(trace.len() > 2);
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
                CompileControlError::from_domain(novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel),
            ] {
                let control = CompileControl {
                    trace: Mutex::default(),
                    refusal: Some((at, cause.clone())),
                };
                assert_eq!(
                    compile_cause(
                        fixture
                            .try_window(None, false, false, &control)
                            .unwrap_err()
                    ),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn every_window_lifecycle_callback_keeps_the_original_cause_and_poisons_the_partition() {
    let values = int64s(&[None, Some(7), Some(9), Some(4)]);
    let fixture = Fixture::new("sum", nullable(&values), DecimalOverflowPolicy::ReportError);
    let window = fixture.window();
    let arguments = [EvaluatedArgument::Column(&values)];
    let peers = whole(4);
    // A running frame, a repeated frame, a refolded frame and a running
    // extension: every fold path runs under the observed control.
    let frames = ranges(&[(0, 1), (0, 1), (1, 3), (0, 4)]);
    let rows = [0, 2];
    let selected = Selection::try_sparse(4, &rows).unwrap();
    for stage in 0..4 {
        let baseline = Control::default();
        if stage == 0 {
            begin(&window, &arguments, &peers, &frames, &baseline).unwrap();
        } else {
            let mut partition =
                begin(&window, &arguments, &peers, &frames, &Control::default()).unwrap();
            match stage {
                1 => {
                    partition.evaluate(selected, 2, &baseline).unwrap();
                }
                2 => {
                    assert!(partition.evaluate(selected, 1, &baseline).is_err());
                }
                _ => partition.finish(&baseline).unwrap(),
            }
        }
        let trace = baseline.trace.lock().unwrap().clone();
        assert!(trace.len() >= 2);
        for at in 0..trace.len() {
            for cause in causes() {
                let control = Control {
                    trace: Mutex::default(),
                    refusal: Some((at, cause.clone())),
                };
                if stage == 0 {
                    assert_eq!(
                        begin(&window, &arguments, &peers, &frames, &control)
                            .map(|_| ())
                            .unwrap_err(),
                        cause
                    );
                } else {
                    let mut partition =
                        begin(&window, &arguments, &peers, &frames, &Control::default()).unwrap();
                    let result = match stage {
                        1 => partition.evaluate(selected, 2, &control).map(|_| ()),
                        2 => partition.evaluate(selected, 1, &control).map(|_| ()),
                        _ => partition.finish(&control),
                    };
                    assert_eq!(result.unwrap_err(), cause);
                    let stopped = Control::default();
                    assert_eq!(
                        partition.evaluate(selected, 2, &stopped).unwrap_err(),
                        KernelFailure::InstanceFailed
                    );
                    assert_eq!(
                        partition.finish(&stopped).unwrap_err(),
                        KernelFailure::InstanceFailed
                    );
                    assert!(stopped.trace.lock().unwrap().is_empty());
                }
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

fn admit_extrema_profile(
    fixture: &Fixture,
    lifecycle: PureCallLifecycle,
    control: &dyn PureCompileControl,
) -> Result<(), FunctionBindingError> {
    let declaration = fixture
        .catalog
        .pure_overload_declaration_observed(
            &fixture.function,
            FunctionKind::Aggregate,
            &fixture.selected.overload,
            &CompileControl::default(),
        )
        .unwrap();
    declaration.admit_selected_lifecycle_observed(&fixture.selected, 1, lifecycle, control)
}

#[test]
fn extrema_static_utf8_refusal_is_only_for_the_aggregate_window_lifecycle() {
    for name in ["min", "max"] {
        let text = Fixture::new(
            name,
            FunctionValueType::new(DataType::Utf8, true),
            DecimalOverflowPolicy::ReportError,
        );
        admit_extrema_profile(
            &text,
            PureCallLifecycle::Aggregate,
            &CompileControl::default(),
        )
        .unwrap();
        let _original_group_aggregate = text.aggregate();
        assert!(
            matches!(admit_extrema_profile(&text,PureCallLifecycle::AggregateWindow,
            &CompileControl::default()),Err(FunctionBindingError::UnavailableImplementation(overload))
            if overload==text.selected.overload)
        );
        let fixed = Fixture::new(
            name,
            FunctionValueType::new(DataType::Int64, true),
            DecimalOverflowPolicy::ReportError,
        );
        admit_extrema_profile(
            &fixed,
            PureCallLifecycle::AggregateWindow,
            &CompileControl::default(),
        )
        .unwrap();
        fixed
            .try_window(None, false, false, &CompileControl::default())
            .unwrap();
    }
}

#[test]
fn extrema_static_window_refusal_preserves_all_original_control_causes() {
    let fixture = Fixture::new(
        "min",
        FunctionValueType::new(DataType::Utf8, true),
        DecimalOverflowPolicy::ReportError,
    );
    let baseline = CompileControl::default();
    assert!(matches!(
        admit_extrema_profile(&fixture, PureCallLifecycle::AggregateWindow, &baseline),
        Err(FunctionBindingError::UnavailableImplementation(_))
    ));
    let trace = baseline.trace.lock().unwrap().clone();
    for stop in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = CompileControl {
                trace: Mutex::default(),
                refusal: Some((stop, cause.clone())),
            };
            assert!(
                matches!(admit_extrema_profile(&fixture,PureCallLifecycle::AggregateWindow,&control),
                Err(FunctionBindingError::Control(actual)) if actual==cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
        }
    }
}
