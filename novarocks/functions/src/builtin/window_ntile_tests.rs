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

use super::super::window_ntile_owner::{effects, operation};
use super::*;
use crate::{
    CallEffectInput, ConstantPolicy, ConstantPool, ConstantValue, EngineFunctionCatalog,
    EngineFunctionCatalogBuilder, EvaluatedArgument, FullPartitionWindowInput, FunctionArgument,
    FunctionBindingRequest, FunctionBindingSelection, FunctionId, FunctionKind, FunctionOverloadId,
    FunctionResultType, FunctionSpecializationFailure, InstalledPureKernel, KernelDiagnostic,
    PreparedPureKernel, PureCallPreparation, PureImplementationDeclaration, PureImplementationId,
    PureKernelAbi, PurePreparationSource, ScopedRelationalEffects, WindowCallOptions,
    WindowEvaluationPartition, WindowRowRange,
};
use arrow_array::{Int32Array, Int64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy,
    ExpressionUseId, FunctionInstanceState, FunctionIntrinsicRowError, FunctionNullBehavior,
    FunctionValueType, PureCompileControl, SemanticParameters, WindowBound, WindowFrame,
    WindowFrameExclusion, WindowFrameUnits,
};
use std::{sync::Mutex, time::Duration};
const ARGUMENT_USES: [Option<ExpressionUseId>; 1] = [Some(ExpressionUseId::new(11))];
#[derive(Default)]
struct CompileControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "compile callback after primary cause");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
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
            assert!(at <= *stop, "runtime callback after primary cause");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("ntile never waits")
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

fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 64,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 65536,
        max_library_validation_work: 4 * 1024 * 1024,
        max_library_validation_bytes: 4 * 1024 * 1024,
    }
}
fn constant(array: ArrayRef, nullable: bool, ordinal: u32) -> ConstantValue {
    let ty =
        FunctionValueType::from_legacy_declared_projection(array.data_type().clone(), nullable);
    ConstantPool::try_new(
        Arc::new(ty.try_to_field("original-buckets").unwrap()),
        ty,
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &CompileControl::default(),
    )
    .unwrap()
    .value(ordinal)
    .unwrap()
}
fn cv(value: Option<i64>, nullable: bool) -> ConstantValue {
    constant(Arc::new(Int64Array::from(vec![value])), nullable, 0)
}
struct Fixture {
    catalog: EngineFunctionCatalog,
    id: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    parameters: SemanticParameters,
    policy: DecimalOverflowPolicy,
}
impl Fixture {
    fn new(
        value: Option<ConstantValue>,
        source: FunctionValueType,
        policy: DecimalOverflowPolicy,
    ) -> Self {
        let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
        let arguments = vec![FunctionArgument::Value {
            value_type: source,
            constant: value,
        }];
        let bound = catalog
            .resolve_bound_user(
                "ntile",
                FunctionKind::Window,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: 1,
                    expected_result_type: None,
                },
                &CompileControl::default(),
            )
            .unwrap();
        Self {
            catalog,
            id: bound.function_id,
            selected: Arc::new(bound.selected),
            arguments,
            parameters: SemanticParameters::try_new([]).unwrap(),
            policy,
        }
    }
    fn positive(buckets: i64, policy: DecimalOverflowPolicy) -> Self {
        let value = cv(Some(buckets), false);
        Self::new(Some(value.clone()), value.value_type().clone(), policy)
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: (context()).into(),
            argument_uses: crate::CallArgumentUses::SelectedChannels(&ARGUMENT_USES),
            function_id: &self.id,
            kind: FunctionKind::Window,
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
    fn prepare(
        &self,
        options: WindowCallOptions,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedWindowKernel>, FunctionSpecializationFailure> {
        let prepared = self.catalog.prepare_fresh_selected(
            self.input(),
            self.selected.clone(),
            PureCallPreparation::Window {
                arguments: ScopedRelationalEffects::pure_value(context()),
                options,
            },
            control,
        )?;
        assert_eq!(prepared.implementation().abi, PureKernelAbi::WindowV1);
        assert!(Arc::ptr_eq(
            prepared.call_contract().selected_owner(),
            &self.selected
        ));
        match prepared.into_prepared() {
            PreparedPureKernel::Window(value) => Ok(value),
            _ => panic!("actual Window attachment"),
        }
    }
    fn value(&self) -> &ConstantValue {
        match &self.arguments[0] {
            FunctionArgument::Value {
                constant: Some(value),
                ..
            } => value,
            _ => panic!("constant fixture"),
        }
    }
}
fn options(frame: Option<WindowFrame<u64>>, ignore: bool) -> WindowCallOptions {
    WindowCallOptions::try_new(frame, ignore, &CompileControl::default()).unwrap()
}
fn geometry<'a>(
    prepared: &'a Arc<dyn PreparedWindowKernel>,
    rows: usize,
    arguments: &'a [EvaluatedArgument<'a>],
    peers: &'a [WindowRowRange],
    frames: &'a [WindowRowRange],
) -> WindowPartitionInput<'a> {
    let full = FullPartitionWindowInput::try_new(
        prepared.contract(),
        rows,
        arguments,
        &[],
        &Control::default(),
    )
    .unwrap();
    WindowPartitionInput::try_new(full, peers, frames, &Control::default()).unwrap()
}
fn values(output: &SelectedValues<'_>) -> Vec<i64> {
    assert_eq!(output.values().null_count(), 0);
    assert!(output.errors().is_empty());
    output
        .values()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
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
        other => panic!("unexpected compile refusal {other:?}"),
    }
}

#[test]
fn ntile_independent_quotient_remainder_oracles_policies_and_exact_installed_contract() {
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for (buckets, hand) in [
            (3, vec![1, 1, 1, 1, 2, 2, 2, 3, 3, 3]),
            (4, vec![1, 1, 2, 2, 3, 4]),
            (9, vec![1, 2, 3]),
            (i64::MAX, vec![1, 2, 3]),
            (1, vec![1, 1, 1]),
        ] {
            let f = Fixture::positive(buckets, policy);
            let prepared = f
                .prepare(options(None, false), &CompileControl::default())
                .unwrap();
            let call = prepared.contract().call();
            assert_eq!(call.decimal_overflow_policy(), policy);
            assert_eq!(call.effects().argument_control, ArgumentControl::Window);
            assert_eq!(
                call.effects().instance_state,
                FunctionInstanceState::WindowPartition
            );
            assert_eq!(
                call.effects().own_row_error,
                FunctionIntrinsicRowError::NotRowEvaluated
            );
            assert_eq!(
                call.effects().null_behavior,
                FunctionNullBehavior::CalledOnNull
            );
            assert!(call.effects().environment.is_empty());
            assert!(prepared.contract().result_type().nullable());
            let args = [EvaluatedArgument::Constant(f.value())];
            let peers = [WindowRowRange {
                start: 0,
                end: hand.len(),
            }];
            let frames = vec![WindowRowRange { start: 0, end: 0 }; hand.len()];
            let input = geometry(&prepared, hand.len(), &args, &peers, &frames);
            let mut part =
                WindowEvaluationPartition::begin(prepared.clone(), input, &Control::default())
                    .unwrap();
            assert_eq!(
                values(
                    &part
                        .evaluate(Selection::all(hand.len()), hand.len(), &Control::default())
                        .unwrap()
                ),
                hand
            );
            assert!(part.retained_bytes().unwrap() <= part.retained_upper_bound());
            part.finish(&Control::default()).unwrap();
        }
    }
    assert_eq!(operation("ntile"), Some(()));
    for name in ["NTILE", "session_number", "rank"] {
        assert_eq!(operation(name), None);
    }
}

#[test]
fn ntile_frozen_subset_keeps_actual_installed_attachment_and_selected_arc() {
    let f = Fixture::positive(3, DecimalOverflowPolicy::ReportError);
    let opts = options(None, true);
    let fresh = f.prepare(opts, &CompileControl::default()).unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            f.catalog
                .definition("ntile", FunctionKind::Window)
                .unwrap()
                .clone(),
        )
        .unwrap();
    let subset = builder
        .seal_pure([InstalledPureKernel {
            function: FunctionId::try_new("builtin.window/ntile/v1").unwrap(),
            kind: FunctionKind::Window,
            implementation: PureImplementationDeclaration {
                overload: FunctionOverloadId::try_new(
                    "builtin.window/ntile/(i64)->i64;strict;legacy",
                )
                .unwrap(),
                implementation: PureImplementationId::try_new("builtin.window/ntile/selected-v1")
                    .unwrap(),
                abi: PureKernelAbi::WindowV1,
            },
            aggregate_state_format: None,
        }])
        .unwrap();
    let frozen = subset
        .prepare_frozen(
            f.input(),
            f.selected.clone(),
            fresh.contract().call().effects(),
            PureCallPreparation::Window {
                arguments: ScopedRelationalEffects::pure_value(context()),
                options: opts,
            },
            &CompileControl::default(),
        )
        .unwrap();
    assert_eq!(frozen.source(), PurePreparationSource::Frozen);
    let prepared = match frozen.into_prepared() {
        PreparedPureKernel::Window(value) => value,
        _ => panic!("Window"),
    };
    assert!(Arc::ptr_eq(
        prepared.contract().call().selected_owner(),
        &f.selected
    ));
    assert_eq!(*prepared.contract().options(), opts);
    assert_eq!(
        prepared.contract().call().effects(),
        fresh.contract().call().effects()
    );
    assert_eq!(
        effects().observable_effects,
        novarocks_type_contract::ObservableEffects::NONE
    );
    let args = [EvaluatedArgument::Constant(f.value())];
    let peers = [WindowRowRange { start: 0, end: 6 }];
    let frames = [WindowRowRange { start: 0, end: 0 }; 6];
    let mut part = WindowEvaluationPartition::begin(
        prepared.clone(),
        geometry(&prepared, 6, &args, &peers, &frames),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        values(
            &part
                .evaluate(Selection::all(6), 6, &Control::default())
                .unwrap()
        ),
        [1, 1, 2, 2, 3, 3]
    );
}

#[test]
fn ntile_original_slice_scalar_compact_and_nonzero_cv_addresses_allow_split_repeated_demand() {
    let backing: ArrayRef = Arc::new(Int64Array::from(vec![99, 3, 3, 3, 3, 3, 3, 77]));
    let original = constant(backing.clone(), false, 1);
    let f = Fixture::new(
        Some(original.clone()),
        original.value_type().clone(),
        DecimalOverflowPolicy::OutputNull,
    );
    let prepared = f
        .prepare(options(None, false), &CompileControl::default())
        .unwrap();
    assert_eq!(f.value().ordinal(), 1);
    assert_eq!(
        f.value().pool().backing_identity(),
        original.pool().backing_identity()
    );
    let sliced = backing.slice(1, 6);
    let scalar: ArrayRef = Arc::new(Int64Array::from(vec![3]));
    let dense = SelectedValues::try_new(
        Selection::all(6),
        sliced.data_type(),
        sliced.clone(),
        Box::default(),
    )
    .unwrap();
    let peers = [WindowRowRange { start: 0, end: 6 }];
    let frames = [WindowRowRange { start: 0, end: 0 }; 6];
    for arg in [
        EvaluatedArgument::Column(&sliced),
        EvaluatedArgument::Scalar(&scalar),
        EvaluatedArgument::Constant(&original),
        EvaluatedArgument::SelectedColumn(&dense),
    ] {
        let args = [arg];
        let input = geometry(&prepared, 6, &args, &peers, &frames);
        let mut a =
            WindowEvaluationPartition::begin(prepared.clone(), input, &Control::default()).unwrap();
        let mut b =
            WindowEvaluationPartition::begin(prepared.clone(), input, &Control::default()).unwrap();
        let rows = [1, 4, 5];
        let selected = Selection::try_sparse(6, &rows).unwrap();
        for turn in 0..3 {
            let part = if turn == 1 { &mut b } else { &mut a };
            let out = part.evaluate(selected, 3, &Control::default()).unwrap();
            assert_eq!(out.selection().row(0), Some(1));
            assert_eq!(values(&out), [1, 3, 3]);
        }
        let rows = [0, 2];
        assert_eq!(
            values(
                &a.evaluate(
                    Selection::try_sparse(6, &rows).unwrap(),
                    2,
                    &Control::default()
                )
                .unwrap()
            ),
            [1, 2]
        );
        let rows = [3, 5];
        assert_eq!(
            values(
                &a.evaluate(
                    Selection::try_sparse(6, &rows).unwrap(),
                    2,
                    &Control::default()
                )
                .unwrap()
            ),
            [2, 3]
        );
        a.finish(&Control::default()).unwrap();
        b.finish(&Control::default()).unwrap();
    }
}

#[test]
fn ntile_empty_singleton_full_setup_and_frame_options_preserve_original_ignored_membership() {
    let f = Fixture::positive(3, DecimalOverflowPolicy::OutputNull);
    for units in [
        WindowFrameUnits::Rows,
        WindowFrameUnits::Range,
        WindowFrameUnits::Groups,
    ] {
        for ignore in [false, true] {
            let frame = WindowFrame {
                units,
                start: WindowBound::UnboundedPreceding,
                end: WindowBound::CurrentRow,
                exclusion: WindowFrameExclusion::NoOthers,
            };
            let opts = options(Some(frame), ignore);
            let prepared = f.prepare(opts, &CompileControl::default()).unwrap();
            assert_eq!(*prepared.contract().options(), opts);
            let args = [EvaluatedArgument::Constant(f.value())];
            let empty = geometry(&prepared, 0, &args, &[], &[]);
            let control = Control::default();
            let mut part =
                WindowEvaluationPartition::begin(prepared.clone(), empty, &control).unwrap();
            assert!(!control.trace.lock().unwrap().is_empty());
            assert_eq!(
                values(
                    &part
                        .evaluate(Selection::all(0), 0, &Control::default())
                        .unwrap()
                ),
                Vec::<i64>::new()
            );
            part.finish(&Control::default()).unwrap();
            let peers = [WindowRowRange { start: 0, end: 1 }];
            let frames = [WindowRowRange { start: 0, end: 0 }];
            let mut part = WindowEvaluationPartition::begin(
                prepared.clone(),
                geometry(&prepared, 1, &args, &peers, &frames),
                &Control::default(),
            )
            .unwrap();
            assert_eq!(
                values(
                    &part
                        .evaluate(
                            Selection::try_sparse(1, &[]).unwrap(),
                            0,
                            &Control::default()
                        )
                        .unwrap()
                ),
                Vec::<i64>::new()
            );
            assert_eq!(
                values(
                    &part
                        .evaluate(Selection::all(1), 1, &Control::default())
                        .unwrap()
                ),
                [1]
            );
            part.finish(&Control::default()).unwrap();
        }
    }
}

#[test]
fn ntile_null_dynamic_nonpositive_and_uncast_sources_are_explicit_prepare_refusals() {
    for value in [None, Some(0), Some(-1), Some(i64::MIN)] {
        let value = cv(value, true);
        let f = Fixture::new(
            Some(value.clone()),
            value.value_type().clone(),
            DecimalOverflowPolicy::OutputNull,
        );
        assert!(
            f.prepare(options(None, false), &CompileControl::default())
                .is_err()
        );
    }
    let f = Fixture::new(
        None,
        FunctionValueType::new(DataType::Int64, false),
        DecimalOverflowPolicy::OutputNull,
    );
    assert!(
        f.prepare(options(None, false), &CompileControl::default())
            .is_err()
    );
    let narrow = constant(Arc::new(Int32Array::from(vec![3])), false, 0);
    let f = Fixture::new(
        Some(narrow.clone()),
        narrow.value_type().clone(),
        DecimalOverflowPolicy::OutputNull,
    );
    // Original resolver may select a coercion, but CPU prepare never casts raw source.
    assert!(
        matches!(&f.selected.argument_types[0],crate::FunctionArgumentType::Value(ty) if ty.data_type()==&DataType::Int64)
    );
    assert!(
        f.prepare(options(None, false), &CompileControl::default())
            .is_err()
    );
    let f = Fixture::positive(3, DecimalOverflowPolicy::OutputNull);
    let mut stale = (*f.selected).clone();
    stale.result_type = FunctionResultType::Scalar(FunctionValueType::new(DataType::Int64, false));
    let selected = Arc::new(stale);
    let mut input = f.input();
    input.selected = &selected;
    assert!(
        f.catalog
            .prepare_fresh_selected(
                input,
                selected.clone(),
                PureCallPreparation::Window {
                    arguments: ScopedRelationalEffects::pure_value(context()),
                    options: options(None, false)
                },
                &CompileControl::default()
            )
            .is_err()
    );
    let wrong = constant(Arc::new(Int64Array::from(vec![3])), true, 0);
    let arguments = [FunctionArgument::Value {
        value_type: FunctionValueType::new(DataType::Int64, false),
        constant: Some(wrong),
    }];
    let mut input = f.input();
    input.request.arguments = &arguments;
    assert!(
        f.catalog
            .prepare_fresh_selected(
                input,
                f.selected.clone(),
                PureCallPreparation::Window {
                    arguments: ScopedRelationalEffects::pure_value(context()),
                    options: options(None, false)
                },
                &CompileControl::default()
            )
            .is_err()
    );
}

#[test]
fn ntile_every_actual_compile_prefix_preserves_three_causes_success_and_ordinary_refusals() {
    let valid = Fixture::positive(3, DecimalOverflowPolicy::OutputNull);
    let bad = Fixture::positive(0, DecimalOverflowPolicy::OutputNull);
    let dynamic = Fixture::new(
        None,
        FunctionValueType::new(DataType::Int64, false),
        DecimalOverflowPolicy::OutputNull,
    );
    let excluded = options(
        Some(WindowFrame {
            units: WindowFrameUnits::Rows,
            start: WindowBound::UnboundedPreceding,
            end: WindowBound::CurrentRow,
            exclusion: WindowFrameExclusion::CurrentRow,
        }),
        false,
    );
    for (f, opts, success) in [
        (&valid, options(None, false), true),
        (&bad, options(None, false), false),
        (&dynamic, options(None, false), false),
        (&valid, excluded, false),
    ] {
        let baseline = CompileControl::default();
        assert_eq!(f.prepare(opts, &baseline).is_ok(), success);
        let trace = baseline.trace.lock().unwrap().clone();
        assert!(trace.len() >= 2);
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = CompileControl {
                    trace: Mutex::default(),
                    refusal: Some((at, cause.clone())),
                };
                assert_eq!(compile_cause(f.prepare(opts, &control).unwrap_err()), cause);
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn ntile_private_and_actual_window_every_runtime_prefix_keep_seven_causes_and_failed_latch() {
    let f = Fixture::positive(3, DecimalOverflowPolicy::OutputNull);
    let prepared = f
        .prepare(options(None, false), &CompileControl::default())
        .unwrap();
    let args = [EvaluatedArgument::Constant(f.value())];
    let peers = [WindowRowRange { start: 0, end: 6 }];
    let frames = [WindowRowRange { start: 0, end: 0 }; 6];
    let input = geometry(&prepared, 6, &args, &peers, &frames);
    let rows = [1, 4, 5];
    let selected = Selection::try_sparse(6, &rows).unwrap();
    for stage in 0..4 {
        let run = |control: &Control| {
            if stage == 0 {
                return prepared.clone().begin_partition(input, control).map(|_| ());
            }
            let mut part = prepared
                .clone()
                .begin_partition(input, &Control::default())
                .unwrap();
            let result = match stage {
                1 => part.evaluate(selected, 3, control).map(|_| ()),
                2 => part.evaluate(Selection::all(7), 7, control).map(|_| ()),
                _ => part.finish(control),
            };
            if result.is_err() {
                let clean = Control::default();
                assert_eq!(
                    part.evaluate(selected, 3, &clean).unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert_eq!(
                    part.finish(&clean).unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(clean.trace.lock().unwrap().is_empty());
            }
            result
        };
        let baseline = Control::default();
        assert_eq!(run(&baseline).is_ok(), stage != 2);
        let trace = baseline.trace.lock().unwrap().clone();
        for at in 0..trace.len() {
            for cause in causes() {
                let control = Control {
                    trace: Mutex::default(),
                    refusal: Some((at, cause.clone())),
                };
                assert_eq!(run(&control).unwrap_err(), cause);
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
    for selection in [
        Selection::all(6),
        selected,
        Selection::try_sparse(6, &[]).unwrap(),
    ] {
        let run = |control: &Control| {
            let mut part =
                WindowEvaluationPartition::begin(prepared.clone(), input, &Control::default())
                    .unwrap();
            let result = part.evaluate(selection, selection.len(), control);
            if result.is_err() {
                assert_eq!(
                    part.evaluate(selection, selection.len(), control)
                        .unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert_eq!(
                    part.finish(control).unwrap_err(),
                    KernelFailure::InstanceFailed
                );
            }
            result
        };
        let baseline = Control::default();
        run(&baseline).unwrap();
        let trace = baseline.trace.lock().unwrap().clone();
        for at in 0..trace.len() {
            for cause in causes() {
                let control = Control {
                    trace: Mutex::default(),
                    refusal: Some((at, cause.clone())),
                };
                assert_eq!(run(&control).unwrap_err(), cause);
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn ntile_actual_selected_output_quantum_samples_primary_causes_and_layout_before_allocation() {
    let f = Fixture::positive(3, DecimalOverflowPolicy::OutputNull);
    let prepared = f
        .prepare(options(None, false), &CompileControl::default())
        .unwrap();
    let args = [EvaluatedArgument::Constant(f.value())];
    let peers = [WindowRowRange { start: 0, end: 320 }];
    let frames = [WindowRowRange { start: 0, end: 0 }; 320];
    let input = geometry(&prepared, 320, &args, &peers, &frames);
    let run = |control: &Control| {
        let mut part =
            WindowEvaluationPartition::begin(prepared.clone(), input, &Control::default()).unwrap();
        part.evaluate(Selection::all(320), 320, control)
    };
    let baseline = Control::default();
    let output = run(&baseline).unwrap();
    let mut hand = vec![1; 107];
    hand.extend(vec![2; 107]);
    hand.extend(vec![3; 106]);
    assert_eq!(values(&output), hand);
    let trace = baseline.trace.lock().unwrap().clone();
    let quantum = trace.iter().position(|units| *units == 256).unwrap();
    for at in [0, quantum, trace.len() - 1] {
        for cause in causes() {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert_eq!(run(&control).unwrap_err(), cause);
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
    assert_eq!(
        output_capacity(usize::MAX),
        Err(KernelFailure::ResourceExhausted)
    );
    assert_eq!(partition_sizes(10, 3), Ok((3, 4, 1, 4)));
    assert_eq!(partition_sizes(3, i64::MAX), Ok((0, 1, 3, 3)));
    // Direct private extent exercise; not a fabricated huge partition fixture.
    if let Ok(rows) = usize::try_from(i64::MAX) {
        assert_eq!(
            partition_sizes(rows, 1),
            Err(KernelFailure::ResourceExhausted)
        );
    }
    assert_eq!(
        prepared.partition_retained_upper_bound(0).unwrap(),
        prepared.partition_retained_upper_bound(320).unwrap()
    );
}

#[test]
fn ntile_complete_required_inputs_foreign_contract_and_host_capacity_fail_before_output() {
    let f = Fixture::positive(3, DecimalOverflowPolicy::OutputNull);
    let prepared = f
        .prepare(options(None, false), &CompileControl::default())
        .unwrap();
    let other = f
        .prepare(options(None, false), &CompileControl::default())
        .unwrap();
    let args = [EvaluatedArgument::Constant(f.value())];
    let peers = [WindowRowRange { start: 0, end: 3 }];
    let frames = [WindowRowRange { start: 0, end: 0 }; 3];
    let input = geometry(&other, 3, &args, &peers, &frames);
    assert!(matches!(
        prepared.clone().begin_partition(input, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let nulls: ArrayRef = Arc::new(Int64Array::from(vec![Some(3), Some(3), None]));
    let arguments = [EvaluatedArgument::Column(&nulls)];
    assert!(matches!(
        FullPartitionWindowInput::try_new(
            prepared.contract(),
            3,
            &arguments,
            &[],
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let errors = SelectedValues::try_new(
        Selection::all(3),
        nulls.data_type(),
        nulls.clone(),
        vec![crate::RowDataError::new(2, "required bucket input")].into_boxed_slice(),
    )
    .unwrap();
    let arguments = [EvaluatedArgument::SelectedColumn(&errors)];
    assert!(
        FullPartitionWindowInput::try_new(
            prepared.contract(),
            3,
            &arguments,
            &[],
            &Control::default()
        )
        .is_err()
    );
    let wrong: ArrayRef = Arc::new(Int32Array::from(vec![3, 3, 3]));
    let arguments = [EvaluatedArgument::Column(&wrong)];
    assert!(
        FullPartitionWindowInput::try_new(
            prepared.contract(),
            3,
            &arguments,
            &[],
            &Control::default()
        )
        .is_err()
    );
    let input = geometry(&prepared, 3, &args, &peers, &frames);
    let mut part =
        WindowEvaluationPartition::begin(prepared.clone(), input, &Control::default()).unwrap();
    assert!(matches!(
        part.evaluate(Selection::all(3), 2, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert_eq!(
        part.finish(&Control::default()).unwrap_err(),
        KernelFailure::InstanceFailed
    );
}
