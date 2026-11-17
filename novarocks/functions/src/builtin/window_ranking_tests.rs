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

use super::super::window_ranking_owner::{effects, operation};
use super::*;
use crate::{
    CallEffectInput, EngineFunctionCatalog, EngineFunctionCatalogBuilder, FullPartitionWindowInput,
    FunctionArgument, FunctionBindingRequest, FunctionBindingSelection, FunctionId, FunctionKind,
    FunctionOverloadId, FunctionResultType, FunctionSpecializationFailure, InstalledPureKernel,
    KernelDiagnostic, PreparedPureKernel, PureCallPreparation, PureImplementationDeclaration,
    PureImplementationId, PureKernelAbi, PurePreparationSource, ScopedRelationalEffects,
    WindowCallOptions, WindowEvaluationPartition, WindowRowRange,
};
use arrow_array::{Float64Array, Int64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy,
    EvaluationDemand, EvaluationRegionId, ExpressionEffectContext, ExpressionUseId,
    FunctionInstanceState, FunctionIntrinsicRowError, FunctionNullBehavior, PureCompileControl,
    SemanticParameters, WindowBound, WindowFrame, WindowFrameExclusion, WindowFrameUnits,
};
use std::{sync::Mutex, time::Duration};

const NAMES: [&str; 5] = [
    "row_number",
    "rank",
    "dense_rank",
    "cume_dist",
    "percent_rank",
];
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
        panic!("ranking never waits")
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
    id: crate::FunctionId,
    selected: Arc<FunctionBindingSelection>,
    parameters: SemanticParameters,
    policy: DecimalOverflowPolicy,
}
impl Fixture {
    fn new(name: &str, policy: DecimalOverflowPolicy) -> Self {
        let catalog = super::super::catalogue::build_builtin_engine_function_catalog().unwrap();
        let bound = catalog
            .resolve_bound_user(
                name,
                FunctionKind::Window,
                Self::request(),
                &CompileControl::default(),
            )
            .unwrap();
        Self {
            catalog,
            id: bound.function_id,
            selected: Arc::new(bound.selected),
            parameters: SemanticParameters::try_new([]).unwrap(),
            policy,
        }
    }
    fn request() -> FunctionBindingRequest<'static> {
        FunctionBindingRequest {
            arguments: &[],
            logical_argument_count: 0,
            expected_result_type: None,
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: (context()).into(),
            argument_uses: crate::CallArgumentUses::SelectedChannels(&[]),
            function_id: &self.id,
            kind: FunctionKind::Window,
            selected: &self.selected,
            request: Self::request(),
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
            _ => panic!("actual window attachment"),
        }
    }
}
fn options(frame: Option<WindowFrame<u64>>, ignore: bool) -> WindowCallOptions {
    WindowCallOptions::try_new(frame, ignore, &CompileControl::default()).unwrap()
}
fn geometry<'a>(
    prepared: &'a Arc<dyn PreparedWindowKernel>,
    rows: usize,
    peers: &'a [WindowRowRange],
    frames: &'a [WindowRowRange],
) -> WindowPartitionInput<'a> {
    let full =
        FullPartitionWindowInput::try_new(prepared.contract(), rows, &[], &[], &Control::default())
            .unwrap();
    WindowPartitionInput::try_new(full, peers, frames, &Control::default()).unwrap()
}
fn assert_values(output: &SelectedValues<'_>, expected: &[f64]) {
    assert_eq!(output.values().len(), expected.len());
    assert_eq!(output.values().null_count(), 0);
    assert!(output.errors().is_empty());
    if let Some(array) = output.values().as_any().downcast_ref::<Int64Array>() {
        assert_eq!(
            array.values().iter().copied().collect::<Vec<_>>(),
            expected.iter().map(|x| *x as i64).collect::<Vec<_>>()
        );
    } else {
        let array = output
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(
            array
                .values()
                .iter()
                .map(|x| x.to_bits())
                .collect::<Vec<_>>(),
            expected.iter().map(|x| x.to_bits()).collect::<Vec<_>>()
        );
    }
}
fn expected(name: &str) -> Vec<f64> {
    match name {
        "row_number" => vec![1., 2., 3., 4., 5., 6.],
        "rank" => vec![1., 1., 3., 4., 4., 4.],
        "dense_rank" => vec![1., 1., 2., 3., 3., 3.],
        "cume_dist" => vec![2. / 6., 2. / 6., 3. / 6., 1., 1., 1.],
        "percent_rank" => vec![0., 0., 2. / 5., 3. / 5., 3. / 5., 3. / 5.],
        _ => unreachable!(),
    }
}

#[test]
fn installed_five_rankings_preserve_independent_peer_values_and_exact_effects_under_both_policies()
{
    let peers = [
        WindowRowRange { start: 0, end: 2 },
        WindowRowRange { start: 2, end: 3 },
        WindowRowRange { start: 3, end: 6 },
    ];
    let frames = [WindowRowRange { start: 0, end: 6 }; 6];
    for name in NAMES {
        assert!(operation(name).is_some());
        for policy in [
            DecimalOverflowPolicy::OutputNull,
            DecimalOverflowPolicy::ReportError,
        ] {
            let fixture = Fixture::new(name, policy);
            let prepared = fixture
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
            assert_eq!(
                call.effects().proof_scope,
                CallProofScope::Relation(context().identity)
            );
            assert!(prepared.contract().result_type().nullable());
            let input = geometry(&prepared, 6, &peers, &frames);
            let mut partition =
                WindowEvaluationPartition::begin(prepared.clone(), input, &Control::default())
                    .unwrap();
            let output = partition
                .evaluate(Selection::all(6), 6, &Control::default())
                .unwrap();
            assert_values(&output, &expected(name));
            assert!(partition.retained_bytes().unwrap() <= partition.retained_upper_bound());
            partition.finish(&Control::default()).unwrap();
        }
    }
    assert!(operation("ntile").is_none());
}

#[test]
fn sparse_repeated_split_outputs_and_interleaved_partitions_use_original_geometry_without_shared_state()
 {
    let peers = [
        WindowRowRange { start: 0, end: 2 },
        WindowRowRange { start: 2, end: 3 },
        WindowRowRange { start: 3, end: 6 },
    ];
    let frames = [WindowRowRange { start: 2, end: 2 }; 6];
    for name in NAMES {
        let fixture = Fixture::new(name, DecimalOverflowPolicy::OutputNull);
        let prepared = fixture
            .prepare(options(None, false), &CompileControl::default())
            .unwrap();
        let input = geometry(&prepared, 6, &peers, &frames);
        let mut a =
            WindowEvaluationPartition::begin(prepared.clone(), input, &Control::default()).unwrap();
        let mut b =
            WindowEvaluationPartition::begin(prepared.clone(), input, &Control::default()).unwrap();
        let rows = [1, 4, 5];
        let selected = Selection::try_sparse(6, &rows).unwrap();
        for turn in 0..3 {
            let partition = if turn == 1 { &mut b } else { &mut a };
            let out = partition
                .evaluate(selected, 3, &Control::default())
                .unwrap();
            assert_eq!(out.selection().row(0), Some(1));
            assert_values(
                &out,
                &[expected(name)[1], expected(name)[4], expected(name)[5]],
            );
        }
        let first = [0, 2];
        let second = [3, 5];
        assert_values(
            &a.evaluate(
                Selection::try_sparse(6, &first).unwrap(),
                2,
                &Control::default(),
            )
            .unwrap(),
            &[expected(name)[0], expected(name)[2]],
        );
        assert_values(
            &a.evaluate(
                Selection::try_sparse(6, &second).unwrap(),
                2,
                &Control::default(),
            )
            .unwrap(),
            &[expected(name)[3], expected(name)[5]],
        );
        a.finish(&Control::default()).unwrap();
        b.finish(&Control::default()).unwrap();
    }
}

#[test]
fn empty_singleton_and_guarded_empty_outputs_complete_real_begin_and_finish_without_full_output_storage()
 {
    for name in NAMES {
        let fixture = Fixture::new(name, DecimalOverflowPolicy::OutputNull);
        let prepared = fixture
            .prepare(options(None, false), &CompileControl::default())
            .unwrap();
        let empty = geometry(&prepared, 0, &[], &[]);
        let control = Control::default();
        let mut partition =
            WindowEvaluationPartition::begin(prepared.clone(), empty, &control).unwrap();
        assert!(!control.trace.lock().unwrap().is_empty());
        assert_values(
            &partition
                .evaluate(Selection::all(0), 0, &Control::default())
                .unwrap(),
            &[],
        );
        partition.finish(&Control::default()).unwrap();
        let peers = [WindowRowRange { start: 0, end: 1 }];
        let frames = [WindowRowRange { start: 0, end: 0 }];
        let singleton = geometry(&prepared, 1, &peers, &frames);
        let mut partition =
            WindowEvaluationPartition::begin(prepared.clone(), singleton, &Control::default())
                .unwrap();
        assert_values(
            &partition
                .evaluate(
                    Selection::try_sparse(1, &[]).unwrap(),
                    0,
                    &Control::default(),
                )
                .unwrap(),
            &[],
        );
        assert_values(
            &partition
                .evaluate(Selection::all(1), 1, &Control::default())
                .unwrap(),
            &[if name == "percent_rank" { 0. } else { 1. }],
        );
        partition.finish(&Control::default()).unwrap();
    }
}

#[test]
fn explicit_rows_range_groups_and_ignore_nulls_are_retained_while_ranking_ignores_membership() {
    let peers = [
        WindowRowRange { start: 0, end: 2 },
        WindowRowRange { start: 2, end: 3 },
        WindowRowRange { start: 3, end: 6 },
    ];
    let frames = [WindowRowRange { start: 1, end: 1 }; 6];
    for name in NAMES {
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
                let fixture = Fixture::new(name, DecimalOverflowPolicy::ReportError);
                let prepared = fixture
                    .prepare(options(Some(frame), ignore), &CompileControl::default())
                    .unwrap();
                assert_eq!(*prepared.contract().options(), options(Some(frame), ignore));
                let input = geometry(&prepared, 6, &peers, &frames);
                let mut partition = prepared
                    .clone()
                    .begin_partition(input, &Control::default())
                    .unwrap();
                assert_values(
                    &partition
                        .evaluate(Selection::all(6), 6, &Control::default())
                        .unwrap(),
                    &expected(name),
                );
                partition.finish(&Control::default()).unwrap();
            }
        }
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
        other => panic!("unexpected refusal {other:?}"),
    }
}
#[test]
fn installed_compile_success_and_unsupported_exclusion_preserve_every_original_control_prefix() {
    for name in NAMES {
        let fixture = Fixture::new(name, DecimalOverflowPolicy::OutputNull);
        for exclusion in [
            WindowFrameExclusion::NoOthers,
            WindowFrameExclusion::CurrentRow,
            WindowFrameExclusion::Group,
            WindowFrameExclusion::Ties,
        ] {
            let frame = WindowFrame {
                units: WindowFrameUnits::Rows,
                start: WindowBound::UnboundedPreceding,
                end: WindowBound::CurrentRow,
                exclusion,
            };
            let options = options(Some(frame), false);
            let baseline = CompileControl::default();
            let outcome = fixture.prepare(options, &baseline);
            if exclusion == WindowFrameExclusion::NoOthers {
                assert!(outcome.is_ok());
            } else {
                assert!(matches!(
                    outcome,
                    Err(FunctionSpecializationFailure::Kernel(
                        KernelFailure::InvalidProgram(_)
                    ))
                ));
            }
            let trace = baseline.trace.lock().unwrap().clone();
            assert!(trace.len() > 2);
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
                    assert_eq!(
                        compile_cause(fixture.prepare(options, &control).unwrap_err()),
                        cause
                    );
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                }
            }
        }
    }
}

#[test]
fn frozen_owner_uses_exact_same_selected_signature_policy_and_rejects_stale_arguments_results_and_scope()
 {
    for name in NAMES {
        let fixture = Fixture::new(name, DecimalOverflowPolicy::ReportError);
        let options = options(None, true);
        let fresh = fixture
            .prepare(options, &CompileControl::default())
            .unwrap();
        // Clone the original definition: its installed attachment Arc is shared,
        // not a newly constructed resolver/owner with merely equal signatures.
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(
                fixture
                    .catalog
                    .definition(name, FunctionKind::Window)
                    .unwrap()
                    .clone(),
            )
            .unwrap();
        let signature = match name {
            "row_number" | "rank" | "dense_rank" => "()->i64;strict;legacy",
            "cume_dist" | "percent_rank" => "()->f64;strict;legacy",
            _ => unreachable!("five explicit installed names"),
        };
        // Independent expected manifest for this one exact attachment; this
        // subset seal makes no assertion about the whole builtin catalogue.
        let subset = builder
            .seal_pure([InstalledPureKernel {
                function: FunctionId::try_new(format!("builtin.window/{name}/v1")).unwrap(),
                kind: FunctionKind::Window,
                implementation: PureImplementationDeclaration {
                    overload: FunctionOverloadId::try_new(format!(
                        "builtin.window/{name}/{signature}"
                    ))
                    .unwrap(),
                    implementation: PureImplementationId::try_new(format!(
                        "builtin.window/{name}/selected-v1"
                    ))
                    .unwrap(),
                    abi: PureKernelAbi::WindowV1,
                },
                aggregate_state_format: None,
            }])
            .unwrap();
        let frozen = subset
            .prepare_frozen(
                fixture.input(),
                fixture.selected.clone(),
                fresh.contract().call().effects(),
                PureCallPreparation::Window {
                    arguments: ScopedRelationalEffects::pure_value(context()),
                    options,
                },
                &CompileControl::default(),
            )
            .unwrap();
        assert_eq!(frozen.source(), PurePreparationSource::Frozen);
        let frozen = match frozen.into_prepared() {
            PreparedPureKernel::Window(value) => value,
            _ => panic!("actual same installed Window attachment"),
        };
        assert!(Arc::ptr_eq(
            frozen.contract().call().selected_owner(),
            &fixture.selected
        ));
        assert_eq!(
            frozen.contract().call().effects(),
            fresh.contract().call().effects()
        );
        assert_eq!(*frozen.contract().options(), options);
        let arguments = [FunctionArgument::Value {
            value_type: crate::FunctionValueType::new(DataType::Int64, false),
            constant: None,
        }];
        let mut input = fixture.input();
        input.request.arguments = &arguments;
        input.request.logical_argument_count = 1;
        assert!(
            fixture
                .catalog
                .prepare_fresh_selected(
                    input,
                    fixture.selected.clone(),
                    PureCallPreparation::Window {
                        arguments: ScopedRelationalEffects::pure_value(context()),
                        options
                    },
                    &CompileControl::default()
                )
                .is_err()
        );
        let mut selected = fixture.selected.as_ref().clone();
        selected.result_type =
            FunctionResultType::Scalar(crate::FunctionValueType::new(DataType::Int64, false));
        let selected = Arc::new(selected);
        let mut input = fixture.input();
        input.selected = &selected;
        assert!(
            fixture
                .catalog
                .prepare_fresh_selected(
                    input,
                    selected.clone(),
                    PureCallPreparation::Window {
                        arguments: ScopedRelationalEffects::pure_value(context()),
                        options
                    },
                    &CompileControl::default()
                )
                .is_err()
        );
        let mut input = fixture.input();
        input.proof_scope = CallProofScope::Region(EvaluationRegionId::new(99));
        assert!(
            fixture
                .catalog
                .prepare_fresh_selected(
                    input,
                    fixture.selected.clone(),
                    PureCallPreparation::Window {
                        arguments: ScopedRelationalEffects::pure_value(context()),
                        options
                    },
                    &CompileControl::default()
                )
                .is_err()
        );
    }
    assert_eq!(effects().environment_dependencies.len(), 0);
}

#[test]
fn actual_begin_selected_emission_ordinary_shape_and_finish_keep_all_seven_causes_and_failed_latch()
{
    let peers = [
        WindowRowRange { start: 0, end: 2 },
        WindowRowRange { start: 2, end: 3 },
        WindowRowRange { start: 3, end: 6 },
    ];
    let frames = [WindowRowRange { start: 0, end: 6 }; 6];
    let selected_rows = [1, 4, 5];
    for name in NAMES {
        let fixture = Fixture::new(name, DecimalOverflowPolicy::OutputNull);
        let prepared = fixture
            .prepare(options(None, false), &CompileControl::default())
            .unwrap();
        let input = geometry(&prepared, 6, &peers, &frames);
        for stage in 0..4 {
            let selection = Selection::try_sparse(6, &selected_rows).unwrap();
            let baseline = Control::default();
            if stage == 0 {
                prepared.clone().begin_partition(input, &baseline).unwrap();
            } else {
                let mut part = prepared
                    .clone()
                    .begin_partition(input, &Control::default())
                    .unwrap();
                match stage {
                    1 => {
                        assert_values(
                            &part.evaluate(selection, 3, &baseline).unwrap(),
                            &[expected(name)[1], expected(name)[4], expected(name)[5]],
                        );
                    }
                    2 => {
                        assert!(matches!(
                            part.evaluate(Selection::all(7), 7, &baseline),
                            Err(KernelFailure::InvalidProgram(_))
                        ));
                    }
                    _ => part.finish(&baseline).unwrap(),
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
                        assert!(
                            matches!(prepared.clone().begin_partition(input,&control),Err(error) if error==cause)
                        );
                    } else {
                        let mut part = prepared
                            .clone()
                            .begin_partition(input, &Control::default())
                            .unwrap();
                        let result = match stage {
                            1 => part.evaluate(selection, 3, &control).map(|_| ()),
                            2 => part.evaluate(Selection::all(7), 7, &control).map(|_| ()),
                            _ => part.finish(&control),
                        };
                        assert_eq!(result.unwrap_err(), cause);
                        let clean = Control::default();
                        assert!(matches!(
                            part.evaluate(selection, 3, &clean),
                            Err(KernelFailure::InstanceFailed)
                        ));
                        assert!(matches!(
                            part.finish(&clean),
                            Err(KernelFailure::InstanceFailed)
                        ));
                        assert!(clean.trace.lock().unwrap().is_empty());
                    }
                    assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                }
            }
        }
    }
}

#[test]
fn genuine_wide_peer_arithmetic_and_selected_writes_cross_quantum_without_eager_partition_output() {
    let rows = 320;
    let peers: Vec<_> = (0..rows)
        .map(|row| WindowRowRange {
            start: row,
            end: row + 1,
        })
        .collect();
    let frames = vec![
        WindowRowRange {
            start: 0,
            end: rows
        };
        rows
    ];
    let fixture = Fixture::new("percent_rank", DecimalOverflowPolicy::OutputNull);
    let prepared = fixture
        .prepare(options(None, false), &CompileControl::default())
        .unwrap();
    let input = geometry(&prepared, rows, &peers, &frames);
    let selected_rows = [0, 319];
    let selection = Selection::try_sparse(rows, &selected_rows).unwrap();
    let baseline = Control::default();
    let mut partition = prepared
        .clone()
        .begin_partition(input, &Control::default())
        .unwrap();
    assert_values(
        &partition.evaluate(selection, 2, &baseline).unwrap(),
        &[0., 1.],
    );
    let trace = baseline.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    // These are actual cursor advances, arithmetic and selected writes; no
    // synthetic setup scan or cooperation claim inside an opaque algorithm.
    let points = [
        0,
        trace.iter().position(|units| *units == 256).unwrap(),
        trace.len() - 1,
    ];
    for at in points {
        for cause in causes() {
            let mut partition = prepared
                .clone()
                .begin_partition(input, &Control::default())
                .unwrap();
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert!(matches!(partition.evaluate(selection,2,&control),Err(error) if error==cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
    assert_eq!(
        prepared.partition_retained_upper_bound(rows).unwrap(),
        prepared.partition_retained_upper_bound(0).unwrap()
    );
    assert_eq!(
        output_capacity(usize::MAX),
        Err(KernelFailure::ResourceExhausted)
    );
}

#[test]
fn original_contract_geometry_and_host_capacity_are_required_without_replacement_or_default_options()
 {
    let fixture = Fixture::new("rank", DecimalOverflowPolicy::OutputNull);
    let a = fixture
        .prepare(options(None, false), &CompileControl::default())
        .unwrap();
    let b = fixture
        .prepare(options(None, false), &CompileControl::default())
        .unwrap();
    let peers = [WindowRowRange { start: 0, end: 2 }];
    let frames = [WindowRowRange { start: 0, end: 2 }; 2];
    let foreign = geometry(&b, 2, &peers, &frames);
    let baseline = Control::default();
    assert!(matches!(
        a.clone().begin_partition(foreign, &baseline),
        Err(KernelFailure::InvalidProgram(_))
    ));
    let trace = baseline.trace.lock().unwrap().clone();
    assert!(trace.last().is_some_and(|units| *units > 0));
    for at in 0..trace.len() {
        for cause in causes() {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(a.clone().begin_partition(foreign, &control), Err(error) if error == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
    let input = geometry(&a, 2, &peers, &frames);
    let mut partition = a
        .clone()
        .begin_partition(input, &Control::default())
        .unwrap();
    assert!(matches!(
        partition.evaluate(Selection::all(2), 1, &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert!(matches!(
        partition.finish(&Control::default()),
        Err(KernelFailure::InstanceFailed)
    ));
    let full =
        FullPartitionWindowInput::try_new(a.contract(), 2, &[], &[], &Control::default()).unwrap();
    assert!(matches!(
        WindowPartitionInput::try_new(
            full,
            &[WindowRowRange { start: 1, end: 2 }],
            &frames,
            &Control::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert!(matches!(
        WindowPartitionInput::try_new(full, &peers, &frames[..1], &Control::default()),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert!(matches!(
        WindowCallOptions::try_new(
            Some(WindowFrame {
                units: WindowFrameUnits::Range,
                start: WindowBound::Preceding(1),
                end: WindowBound::CurrentRow,
                exclusion: WindowFrameExclusion::NoOthers,
            }),
            false,
            &CompileControl::default()
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert!(matches!(
        fixture.catalog.prepare_fresh_selected(
            fixture.input(),
            fixture.selected.clone(),
            PureCallPreparation::Scalar {
                arguments: crate::ScopedExpressionEffects::pure_value(ExpressionEffectContext {
                    use_id: ExpressionUseId::new(900),
                    region: EvaluationRegionId::new(901),
                    demand: EvaluationDemand::Value
                })
            },
            &CompileControl::default()
        ),
        Err(FunctionSpecializationFailure::InvalidInput(_))
    ));
}
