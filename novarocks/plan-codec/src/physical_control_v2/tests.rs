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

use super::*;
use crate::test_constants as checked;
use arrow::datatypes::DataType;
use novarocks_expression_contract::{BinaryOperator, BoundFunction, ExprKind, ValueType};
use novarocks_type_contract::{
    DecimalOverflowPolicy, ExpressionDefinitionMembership, FunctionArgumentEvaluation,
    FunctionArgumentType, FunctionFailureBehavior, FunctionId, FunctionIntrinsicRowError,
    FunctionKind, FunctionOverloadId, FunctionVolatility, control_argument_semantics,
};
use std::sync::Mutex;

#[test]
fn nullif_control_requires_the_frozen_equality_identity() {
    assert_eq!(
        decode_shape(&encode_shape(ControlShape::NullIf)).unwrap(),
        ControlShape::NullIf
    );
    let mut shape = encode_shape(ControlShape::NullIf);
    let Some(wire::control_shape::Kind::NullIf(frozen)) = shape.kind.as_mut() else {
        panic!("expected a NULLIF control shape");
    };
    frozen.equality_function = novarocks_type_contract::OperatorFunction::NotEqual
        .function_id_text()
        .into();
    assert!(matches!(
        decode_shape(&shape),
        Err(ControlCodecError::InvalidShape(
            "NULLIF requires its exact equality identity"
        ))
    ));
    let guard = RegionGuard {
        owner: ExpressionUseId::new(17),
        kind: GuardKind::NullIfAfterLeft,
    };
    assert_eq!(decode_guard(&encode_guard(guard)).unwrap(), guard);
}

#[derive(Default)]
struct Control {
    failure: Option<(CompilePhase, CompileControlError)>,
    observations: Mutex<Vec<(CompilePhase, u32)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK);
        self.observations.lock().unwrap().push((phase, units));
        if units > 0
            && let Some((failed_phase, failure)) = self.failure.clone()
            && phase == failed_phase
        {
            Err(failure)
        } else {
            Ok(())
        }
    }
}

/// Actual definition and CV sources outlive every projection. No fragment or
/// global root catalogue participates in this carrier-neutral grammar fixture.
pub(super) struct Definitions {
    nodes: Vec<ExprNode>,
    pools: novarocks_constant_contract::ConstantPools,
}
impl Default for Definitions {
    fn default() -> Self {
        Self {
            nodes: Vec::new(),
            pools: novarocks_constant_contract::ConstantPools::empty(),
        }
    }
}
impl std::ops::Deref for Definitions {
    type Target = [ExprNode];
    fn deref(&self) -> &Self::Target {
        &self.nodes
    }
}
impl ExpressionDefinitionMembership<ExprId> for Definitions {
    fn definition_count(&self) -> usize {
        self.nodes.len()
    }
    fn contains_definition(&self, id: ExprId) -> bool {
        (id.get() as usize) < self.nodes.len()
    }
}
impl Definitions {
    pub(super) fn add(&mut self, kind: ExprKind) -> ExprId {
        let id = ExprId::new(self.nodes.len() as u32);
        self.nodes.push(ExprNode {
            id,
            lambda_scope: None,
            ty: ValueType::new(DataType::Boolean, false),
            kind,
        });
        id
    }
    pub(super) fn boolean(&mut self, value: bool) -> ExprId {
        let ty = ValueType::new(DataType::Boolean, false);
        let cv = checked::boolean(value)(&ty);
        let kind = checked::insert(&mut self.pools, cv);
        self.add(kind)
    }
    pub(super) fn flow(
        &self,
        regions: Vec<ExpressionEvaluationRegion>,
        uses: Vec<ExpressionInvocation<ExprId>>,
    ) -> ExpressionControlFlow<ExprId> {
        ExpressionControlFlow::try_new(
            regions,
            uses,
            self,
            CompilePhase::Validate,
            &Control::default(),
        )
        .unwrap()
    }
}
fn region() -> ExpressionEvaluationRegion {
    ExpressionEvaluationRegion {
        id: EvaluationRegionId::new(u32::MAX),
        parent: None,
        guard: None,
    }
}

fn invocation(
    id: u32,
    definition: ExprId,
    region: EvaluationRegionId,
    demand: EvaluationDemand,
    control: ControlShape,
    arguments: &[u32],
) -> ExpressionInvocation<ExprId> {
    ExpressionInvocation {
        context: ExpressionEffectContext {
            use_id: ExpressionUseId::new(id),
            region,
            demand,
        },
        definition,
        control,
        arguments: arguments
            .iter()
            .copied()
            .map(ExpressionUseId::new)
            .collect(),
    }
}
fn binary_fixture() -> (Definitions, ExpressionControlFlow<ExprId>) {
    let mut definitions = Definitions::default();
    let left = definitions.boolean(true);
    let right = definitions.boolean(false);
    let root = definitions.add(ExprKind::Binary {
        allow_throw_exception: None,
        left,
        op: BinaryOperator::Eq,
        right,
        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
    });
    let r = region();
    let flow = definitions.flow(
        vec![r],
        vec![
            invocation(
                0,
                root,
                r.id,
                EvaluationDemand::Value,
                ControlShape::Eager,
                &[u32::MAX, 7],
            ),
            invocation(
                u32::MAX,
                left,
                r.id,
                EvaluationDemand::Value,
                ControlShape::Eager,
                &[],
            ),
            invocation(
                7,
                right,
                r.id,
                EvaluationDemand::Value,
                ControlShape::Eager,
                &[],
            ),
        ],
    );
    (definitions, flow)
}
fn shared_definition_fixture() -> (Definitions, ExpressionControlFlow<ExprId>) {
    let mut definitions = Definitions::default();
    definitions.boolean(true);
    definitions.boolean(false);
    let shared = definitions.boolean(true);
    let r = region();
    let flow = definitions.flow(
        vec![r],
        vec![
            invocation(
                0,
                shared,
                r.id,
                EvaluationDemand::Value,
                ControlShape::Eager,
                &[],
            ),
            invocation(
                u32::MAX,
                shared,
                r.id,
                EvaluationDemand::TruthOnly,
                ControlShape::Eager,
                &[],
            ),
        ],
    );
    (definitions, flow)
}

const SEARCHED_CASE: ControlShape = ControlShape::Case {
    simple: false,
    arms: 1,
    has_else: true,
};

fn branch_fixture(shape: ControlShape) -> (Definitions, ExpressionControlFlow<ExprId>) {
    let mut builder = Definitions::default();
    let arguments = [
        builder.boolean(true),
        builder.boolean(true),
        builder.boolean(false),
    ];
    let ty = ValueType::new(DataType::Boolean, false);
    let kind = if shape == SEARCHED_CASE {
        ExprKind::Case {
            ieee_float_equality: false,
            operand: None,
            when_then: Box::from([(arguments[0], arguments[1])]),
            else_expr: Some(arguments[2]),
        }
    } else {
        ExprKind::FunctionCall {
            function: BoundFunction {
                function_id: FunctionId::try_new("fixture/control-codec/branch").unwrap(),
                overload: FunctionOverloadId::try_new("fixture/control-codec/boolean-branch")
                    .unwrap(),
                kind: FunctionKind::Scalar,
                argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                    vec![FunctionArgumentType::Value(ty.clone()); 3].into_boxed_slice(),
                ),
                result_type: ty.clone(),

                legacy_metadata: Some(novarocks_expression_contract::LegacyBindingMetadata {
                    semantic_parameters: Box::default(),
                    volatility: FunctionVolatility::Immutable,
                    argument_evaluation: FunctionArgumentEvaluation::Eager,
                    failure_behavior: FunctionFailureBehavior::Propagate,
                    intrinsic_row_error: FunctionIntrinsicRowError::NoRowError,
                }),
            },
            args: Box::from(arguments),
        }
    };
    let root = builder.add(kind);
    // The exact selected owner remains a compiler obligation. The fixture
    // requests its control shape independently of its name or legacy flag.
    let region = EvaluationRegionId::new(u32::MAX);
    let mut regions = vec![ExpressionEvaluationRegion {
        id: region,
        parent: None,
        guard: None,
    }];
    let mut uses = vec![invocation(
        0,
        root,
        region,
        EvaluationDemand::Value,
        shape,
        &[7, 8, u32::MAX],
    )];
    for (ordinal, (id, definition)) in [7, 8, u32::MAX].into_iter().zip(arguments).enumerate() {
        let (demand, guard) =
            control_argument_semantics(shape, 3, ordinal, EvaluationDemand::Value).unwrap();
        let child_region = if let Some(kind) = guard {
            let child = EvaluationRegionId::new(ordinal as u32);
            regions.push(ExpressionEvaluationRegion {
                id: child,
                parent: Some(region),
                guard: Some(RegionGuard {
                    owner: ExpressionUseId::new(0),
                    kind,
                }),
            });
            child
        } else {
            region
        };
        uses.push(invocation(
            id,
            definition,
            child_region,
            demand,
            ControlShape::Eager,
            &[],
        ));
    }
    let flow = ExpressionControlFlow::try_new(
        regions,
        uses,
        &builder,
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    (builder, flow)
}
fn many_roots_fixture(count: usize) -> (Definitions, ExpressionControlFlow<ExprId>) {
    let mut definitions = Definitions::default();
    let leaf = definitions.boolean(true);
    let r = region();
    let flow = definitions.flow(
        vec![r],
        (0..count)
            .map(|i| {
                invocation(
                    i as u32,
                    leaf,
                    r.id,
                    EvaluationDemand::Value,
                    ControlShape::Eager,
                    &[],
                )
            })
            .collect(),
    );
    (definitions, flow)
}

#[test]
fn typed_control_roundtrip_preserves_presence_and_ordered_children() {
    let (definitions, flow) = binary_fixture();
    let control = Control::default();
    let encoded = encode_expression_control(&flow, &control).unwrap();
    assert_eq!(encoded.regions[0].id, u32::MAX);
    assert_eq!(encoded.uses[0].id, 0);
    assert_eq!(encoded.uses[0].region_id, Some(u32::MAX));
    assert_eq!(encoded.uses[0].argument_use_ids, [u32::MAX, 7]);
    assert_eq!(
        encoded
            .uses
            .iter()
            .find(|item| item.id == u32::MAX)
            .unwrap()
            .definition_id,
        Some(0)
    );
    let decoded = decode_expression_control(&definitions, &encoded, &control).unwrap();
    assert_eq!(decoded, flow);
    assert!(
        control
            .observations
            .lock()
            .unwrap()
            .iter()
            .any(|(phase, _)| *phase == CompilePhase::Encode)
    );
}

#[test]
fn shared_definition_retains_independent_value_and_truth_only_occurrences() {
    let (definitions, flow) = shared_definition_fixture();
    let encoded = encode_expression_control(&flow, &Control::default()).unwrap();
    let duplicates = encoded
        .uses
        .iter()
        .filter(|item| item.definition_id == Some(2))
        .collect::<Vec<_>>();
    assert_eq!(duplicates.len(), 2);
    assert_ne!(duplicates[0].id, duplicates[1].id);
    assert!(
        duplicates
            .iter()
            .any(|item| item.demand == wire::EvaluationDemand::Value as i32)
    );
    assert!(
        duplicates
            .iter()
            .any(|item| item.demand == wire::EvaluationDemand::TruthOnly as i32)
    );
    assert_eq!(
        decode_expression_control(&definitions, &encoded, &Control::default()).unwrap(),
        flow
    );
}

#[test]
fn actual_case_and_explicit_if_preserve_guard_owner_ordinal_and_demand() {
    for shape in [ControlShape::If, SEARCHED_CASE] {
        let (definitions, flow) = branch_fixture(shape);
        let encoded = encode_expression_control(&flow, &Control::default()).unwrap();
        for region in encoded
            .regions
            .iter()
            .filter(|region| region.guard.is_some())
        {
            assert_eq!(region.parent_region_id, Some(u32::MAX));
            assert_eq!(region.guard.as_ref().unwrap().owner_use_id, Some(0));
        }
        assert_eq!(
            encoded
                .uses
                .iter()
                .find(|item| item.id == 7)
                .unwrap()
                .demand,
            wire::EvaluationDemand::TruthOnly as i32
        );
        assert_eq!(
            decode_expression_control(&definitions, &encoded, &Control::default()).unwrap(),
            flow
        );
    }
}

#[test]
fn ordered_regexp_roundtrip_checks_predecessor_guards_and_value_demand() {
    use wire::region_guard::Kind::OrderedRegexpAfterArgumentOrdinal;

    let control = Control::default();
    let (definitions, flow) = branch_fixture(ControlShape::OrderedRegexp);
    let encoded = encode_expression_control(&flow, &control).unwrap();
    for (region, predecessor) in encoded
        .regions
        .iter()
        .filter(|region| region.guard.is_some())
        .zip([0, 1])
    {
        assert_eq!(region.parent_region_id, Some(u32::MAX));
        let guard = region.guard.as_ref().unwrap();
        assert_eq!(guard.owner_use_id, Some(0));
        assert_eq!(
            guard.kind,
            Some(OrderedRegexpAfterArgumentOrdinal(predecessor))
        );
    }
    assert_eq!(
        decode_expression_control(&definitions, &encoded, &control).unwrap(),
        flow
    );

    let mut forged = encoded.clone();
    forged
        .regions
        .iter_mut()
        .find_map(|region| region.guard.as_mut())
        .unwrap()
        .kind = Some(OrderedRegexpAfterArgumentOrdinal(1));
    assert_eq!(
        decode_expression_control(&definitions, &forged, &control).unwrap_err(),
        ControlCodecError::Flow(ExpressionControlFlowError::InvalidGuard)
    );

    let mut forged = encoded;
    forged
        .uses
        .iter_mut()
        .find(|use_| use_.id == 8)
        .unwrap()
        .demand = wire::EvaluationDemand::TruthOnly as i32;
    assert_eq!(
        decode_expression_control(&definitions, &forged, &control).unwrap_err(),
        ControlCodecError::Flow(ExpressionControlFlowError::InvalidDemand)
    );
}

#[test]
fn every_control_and_guard_variant_roundtrips() {
    for shape in [
        ControlShape::Eager,
        ControlShape::NoArguments,
        ControlShape::TypeOnly,
        ControlShape::LambdaBody,
        ControlShape::Conjunction,
        ControlShape::Disjunction,
        ControlShape::If,
        ControlShape::Coalesce,
        ControlShape::Try,
        ControlShape::OrderedRegexp,
        ControlShape::Between { negated: false },
        ControlShape::Between { negated: true },
        ControlShape::Membership { negated: false },
        ControlShape::Membership { negated: true },
        ControlShape::Case {
            simple: true,
            arms: u32::MAX,
            has_else: true,
        },
        ControlShape::HigherOrder {
            body_ordinal: u32::MAX,
            body_demand: EvaluationDemand::TruthOnly,
        },
        ControlShape::HigherOrder {
            body_ordinal: 0,
            body_demand: EvaluationDemand::Value,
        },
    ] {
        assert_eq!(decode_shape(&encode_shape(shape)).unwrap(), shape);
    }
    for shape in [
        novarocks_type_contract::TemporalSourceShape::FormatOrdinary,
        novarocks_type_contract::TemporalSourceShape::FormatUtf8Override,
        novarocks_type_contract::TemporalSourceShape::SecondsDirect,
        novarocks_type_contract::TemporalSourceShape::SecondsCastString,
        novarocks_type_contract::TemporalSourceShape::SecondsCastOther,
        novarocks_type_contract::TemporalSourceShape::SecondsRoundtrip,
    ] {
        let shape = ControlShape::TemporalSource(shape);
        assert_eq!(decode_shape(&encode_shape(shape)).unwrap(), shape);
    }
    for owner in [0, u32::MAX] {
        for kind in [
            GuardKind::IfThen,
            GuardKind::IfElse,
            GuardKind::CaseElse,
            GuardKind::TryBody,
            GuardKind::LambdaInvocation,
            GuardKind::TemporalInvocationNull,
            GuardKind::TemporalAfterSource { ordinal: u32::MAX },
            GuardKind::BetweenAfterSource { ordinal: u32::MAX },
            GuardKind::MembershipAfterSource { ordinal: u32::MAX },
            GuardKind::OrderedRegexpAfterArgument { ordinal: 0 },
            GuardKind::CoalesceAfterNull { ordinal: u32::MAX },
            GuardKind::CaseWhen { arm: 0 },
            GuardKind::CaseThen { arm: u32::MAX },
        ] {
            let guard = RegionGuard {
                owner: ExpressionUseId::new(owner),
                kind,
            };
            assert_eq!(decode_guard(&encode_guard(guard)).unwrap(), guard);
        }
    }
}

#[test]
fn absent_mandatory_ids_and_oneofs_never_default_to_zero_or_eager() {
    let (definitions, flow) = binary_fixture();
    let encoded = encode_expression_control(&flow, &Control::default()).unwrap();
    type MissingFieldMutation = (&'static str, fn(&mut wire::ExpressionControl));
    let mutations: &[MissingFieldMutation] = &[
        ("use region is missing", |dto| dto.uses[0].region_id = None),
        ("use definition is missing", |dto| {
            dto.uses[0].definition_id = None
        }),
        ("use control is missing", |dto| dto.uses[0].control = None),
        ("control kind is missing", |dto| {
            dto.uses[0].control.as_mut().unwrap().kind = None
        }),
    ];
    for (message, mutate) in mutations {
        let mut forged = encoded.clone();
        mutate(&mut forged);
        assert_eq!(
            decode_expression_control(&definitions, &forged, &Control::default()).unwrap_err(),
            ControlCodecError::InvalidShape(message)
        );
    }
    let (definitions, flow) = branch_fixture(SEARCHED_CASE);
    let encoded = encode_expression_control(&flow, &Control::default()).unwrap();
    for (missing_owner, message) in [
        (true, "guard owner is missing"),
        (false, "guard kind is missing"),
    ] {
        let mut forged = encoded.clone();
        let guard = forged
            .regions
            .iter_mut()
            .find_map(|region| region.guard.as_mut())
            .unwrap();
        if missing_owner {
            guard.owner_use_id = None;
        } else {
            guard.kind = None;
        }
        assert_eq!(
            decode_expression_control(&definitions, &forged, &Control::default()).unwrap_err(),
            ControlCodecError::InvalidShape(message)
        );
    }
}

#[test]
fn unspecified_and_unknown_enums_fail_closed() {
    let (definitions, flow) = binary_fixture();
    let encoded = encode_expression_control(&flow, &Control::default()).unwrap();
    for unknown in [0, -1, i32::MAX] {
        let mut forged = encoded.clone();
        forged.uses[0].demand = unknown;
        assert_eq!(
            decode_expression_control(&definitions, &forged, &Control::default()).unwrap_err(),
            ControlCodecError::InvalidShape("unknown or unspecified evaluation demand")
        );
        forged = encoded.clone();
        forged.uses[0].control = Some(wire::ControlShape {
            kind: Some(wire::control_shape::Kind::Simple(unknown)),
        });
        assert_eq!(
            decode_expression_control(&definitions, &forged, &Control::default()).unwrap_err(),
            ControlCodecError::InvalidShape("unknown or unspecified control shape")
        );
        let higher = wire::ControlShape {
            kind: Some(wire::control_shape::Kind::HigherOrder(
                wire::HigherOrderControl {
                    body_ordinal: 0,
                    body_demand: unknown,
                },
            )),
        };
        assert_eq!(
            decode_shape(&higher).unwrap_err(),
            ControlCodecError::InvalidShape("unknown or unspecified evaluation demand")
        );
        let guard = wire::RegionGuard {
            owner_use_id: Some(0),
            kind: Some(wire::region_guard::Kind::Simple(unknown)),
        };
        assert_eq!(
            decode_guard(&guard).unwrap_err(),
            ControlCodecError::InvalidShape("unknown or unspecified guard kind")
        );
    }
}

#[test]
fn duplicate_dangling_shared_and_reordered_use_graphs_keep_typed_rejections() {
    let (definitions, flow) = binary_fixture();
    let encoded = encode_expression_control(&flow, &Control::default()).unwrap();
    let mut forged = encoded.clone();
    forged.regions.push(forged.regions[0]);
    assert_eq!(
        decode_expression_control(&definitions, &forged, &Control::default()).unwrap_err(),
        ControlCodecError::Flow(ExpressionControlFlowError::DuplicateIdentity)
    );
    forged = encoded.clone();
    forged.uses.push(forged.uses[0].clone());
    assert_eq!(
        decode_expression_control(&definitions, &forged, &Control::default()).unwrap_err(),
        ControlCodecError::Flow(ExpressionControlFlowError::DuplicateIdentity)
    );
    for field in [0, 1, 2] {
        forged = encoded.clone();
        match field {
            0 => forged.uses[0].definition_id = Some(12345),
            1 => forged.uses[0].region_id = Some(12345),
            _ => forged.uses[0].argument_use_ids[0] = 12345,
        }
        assert_eq!(
            decode_expression_control(&definitions, &forged, &Control::default()).unwrap_err(),
            ControlCodecError::Flow(ExpressionControlFlowError::InvalidReference)
        );
    }
    forged = encoded.clone();
    forged.uses[0].argument_use_ids = vec![7, 7];
    assert_eq!(
        decode_expression_control(&definitions, &forged, &Control::default()).unwrap_err(),
        ControlCodecError::Flow(ExpressionControlFlowError::SharedUse)
    );
    // Exact ordered-definition and independent-root correspondence now belongs
    // to the checked unit author; see ordered_correspondence public tests.
}

#[test]
fn each_projection_and_validation_phase_observes_midwork_typed_control_failures() {
    let (definitions, flow) = many_roots_fixture(300);
    let encoded = encode_expression_control(&flow, &Control::default()).unwrap();
    for phase in [
        CompilePhase::Encode,
        CompilePhase::Decode,
        CompilePhase::Validate,
    ] {
        for failure in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                failure: Some((phase, failure.clone())),
                ..Control::default()
            };
            let error = if phase == CompilePhase::Encode {
                encode_expression_control(&flow, &control).unwrap_err()
            } else if phase == CompilePhase::Decode {
                decode_expression_control(&definitions, &encoded, &control).unwrap_err()
            } else {
                // Validation borrows this exact caller stream; it does not
                // create a new phase or finish after the primary refusal.
                let mut work = CompileCheckpoints::try_new(&control, phase).unwrap();
                decode_expression_control_observed(
                    &definitions,
                    &encoded,
                    4 * 1024 * 1024,
                    unbounded(),
                    &mut |_| Ok(()),
                    &mut work,
                )
                .unwrap_err()
            };
            assert_eq!(error, ControlCodecError::Control(failure));
            let observations = control.observations.lock().unwrap();
            assert!(observations.contains(&(phase, 256)));
            assert!(observations.iter().all(|(_, units)| *units <= 256));
        }
    }
}

#[test]
fn count_and_edge_limits_are_checked_before_malformed_dto_projection() {
    let (definitions, _) = binary_fixture();
    let too_many = ControlCodecError::Flow(ExpressionControlFlowError::TooManyItems);
    for count in [MAX_CONTROL_USE_REFERENCES, MAX_CONTROL_USE_REFERENCES + 1] {
        let dto = wire::ExpressionControl {
            uses: vec![wire::ExpressionUse::default(); count],
            ..Default::default()
        };
        let error = decode_expression_control(&definitions, &dto, &Control::default()).unwrap_err();
        if count > MAX_CONTROL_USE_REFERENCES {
            assert_eq!(error, too_many);
        } else {
            assert_eq!(
                error,
                ControlCodecError::InvalidShape("use region is missing")
            );
        }
    }
    for dto in [
        wire::ExpressionControl {
            regions: vec![wire::EvaluationRegion::default(); MAX_CONTROL_DEFINITIONS + 1],
            ..Default::default()
        },
        wire::ExpressionControl {
            uses: vec![wire::ExpressionUse {
                argument_use_ids: vec![0; MAX_CONTROL_USE_REFERENCES],
                ..Default::default()
            }],
            ..Default::default()
        },
    ] {
        assert_eq!(
            decode_expression_control(&definitions, &dto, &Control::default()).unwrap_err(),
            too_many
        );
    }
    // Equality at the reference bound reaches mandatory-field validation. This
    // malformed probe does not assert that its graph is a valid program.
    let dto = wire::ExpressionControl {
        uses: vec![wire::ExpressionUse {
            argument_use_ids: vec![0; MAX_CONTROL_USE_REFERENCES - 1],
            ..Default::default()
        }],
        ..Default::default()
    };
    assert_eq!(
        decode_expression_control(&definitions, &dto, &Control::default()).unwrap_err(),
        ControlCodecError::InvalidShape("use region is missing")
    );
}

#[test]
fn valid_reference_budget_boundary_roundtrips_and_one_more_edge_fails_before_projection() {
    let count = MAX_CONTROL_USE_REFERENCES / 2 - 1;
    let mut builder = Definitions::default();
    let leaf = builder.boolean(true);
    let root = builder.add(ExprKind::Conjunction {
        args: vec![leaf; count].into_boxed_slice(),
    });
    let region = EvaluationRegionId::new(u32::MAX);
    let arguments = (1..=count as u32).collect::<Vec<_>>();
    let mut uses = vec![invocation(
        0,
        root,
        region,
        EvaluationDemand::Value,
        ControlShape::Conjunction,
        &arguments,
    )];
    uses.extend(arguments.iter().map(|id| {
        invocation(
            *id,
            leaf,
            region,
            EvaluationDemand::Value,
            ControlShape::Eager,
            &[],
        )
    }));
    // A second independent root makes uses + edges exactly the even bound.
    uses.push(invocation(
        u32::MAX,
        leaf,
        region,
        EvaluationDemand::Value,
        ControlShape::Eager,
        &[],
    ));
    assert_eq!(uses.len() + arguments.len(), MAX_CONTROL_USE_REFERENCES);
    let flow = ExpressionControlFlow::try_new(
        vec![ExpressionEvaluationRegion {
            id: region,
            parent: None,
            guard: None,
        }],
        uses,
        &builder,
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    let mut encoded = encode_expression_control(&flow, &Control::default()).unwrap();
    assert_eq!(
        encoded.uses.len()
            + encoded
                .uses
                .iter()
                .map(|item| item.argument_use_ids.len())
                .sum::<usize>(),
        MAX_CONTROL_USE_REFERENCES
    );
    assert_eq!(
        decode_expression_control(&builder, &encoded, &Control::default()).unwrap(),
        flow
    );
    encoded.uses[0].argument_use_ids.push(u32::MAX);
    let control = Control::default();
    assert_eq!(
        decode_expression_control(&builder, &encoded, &control).unwrap_err(),
        ControlCodecError::Flow(ExpressionControlFlowError::TooManyItems)
    );
    assert_eq!(
        *control.observations.lock().unwrap(),
        [(CompilePhase::Decode, 0), (CompilePhase::Decode, 0)]
    );
}

#[test]
fn region_count_equality_reaches_shape_checks_but_overflow_does_not_project() {
    let (definitions, _) = binary_fixture();
    let dto = wire::ExpressionControl {
        regions: vec![wire::EvaluationRegion::default(); MAX_CONTROL_DEFINITIONS],
        ..Default::default()
    };
    assert_eq!(
        decode_expression_control(&definitions, &dto, &Control::default()).unwrap_err(),
        ControlCodecError::Flow(ExpressionControlFlowError::DuplicateIdentity)
    );
    // These equality probes are malformed DTOs, not accepted programs. Each
    // over-limit count must reject before projecting a single dynamic item.
    for dto in [
        wire::ExpressionControl {
            regions: vec![wire::EvaluationRegion::default(); MAX_CONTROL_DEFINITIONS + 1],
            ..Default::default()
        },
        wire::ExpressionControl {
            uses: vec![wire::ExpressionUse::default(); MAX_CONTROL_USE_REFERENCES + 1],
            ..Default::default()
        },
    ] {
        let control = Control::default();
        assert_eq!(
            decode_expression_control(&definitions, &dto, &control).unwrap_err(),
            ControlCodecError::Flow(ExpressionControlFlowError::TooManyItems)
        );
        assert_eq!(
            *control.observations.lock().unwrap(),
            [(CompilePhase::Decode, 0), (CompilePhase::Decode, 0)]
        );
    }
}

#[path = "failure_tail_tests.rs"]
mod failure_tail_tests;
