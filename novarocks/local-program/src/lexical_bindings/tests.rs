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
use crate::{
    BindingRequirements, CompileProfile, ImmutableExpressions, KernelAbiVersion, LocalProgramGraph,
    ProgramControlFlow, ProgramEvaluationRegion, ProgramExpressionArena, ProgramExpressionUse,
    ProgramNode, ProgramNodeId, ProgramRootControlBindings, ProgramRootUseBinding,
    ProgramTypedExpressions, StaticExprNode, StaticFunctionKind, StaticLayout, StaticLiteral,
    StaticValues,
};
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use novarocks_functions::*;
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationRegionId, ExpressionEffectContext, ExpressionUseId, FunctionEffectDeclaration,
    FunctionFailureBehavior, FunctionInstanceState, FunctionIntrinsicRowError, FunctionKind,
    FunctionNullBehavior, FunctionVolatility, GuardKind, ObservableEffects, RegionGuard,
    SemanticParameters,
};
use std::{collections::HashMap, num::NonZeroUsize, sync::Mutex};

const FUNCTION: &str = "fixture/lexical/higher-owner-v1";
const OVERLOAD: &str = "fixture/lexical/int-body-v1";
const IMPLEMENTATION: &str = "fixture/lexical/cpu-v1";
const ROOT_DOMAIN: EvaluationRegionId = EvaluationRegionId::new(7);
const OUTER_DOMAIN: EvaluationRegionId = EvaluationRegionId::new(8);
const INNER_DOMAIN: EvaluationRegionId = EvaluationRegionId::new(9);
#[derive(Default)]
struct Control {
    fail: Option<(u32, CompileControlError)>,
    units: Mutex<usize>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        *self.units.lock().unwrap() += units as usize;
        if let Some((at, failure)) = self.fail.clone()
            && at == units
        {
            Err(failure)
        } else {
            Ok(())
        }
    }
}
fn int() -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, false)
}
fn fid() -> FunctionId {
    FunctionId::try_new(FUNCTION).unwrap()
}
fn oid() -> FunctionOverloadId {
    FunctionOverloadId::try_new(OVERLOAD).unwrap()
}
fn base() -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: FunctionIntrinsicRowError::NoRowError,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::CalledOnNull,
        argument_control: ArgumentControl::HigherOrder {
            body_ordinal: 1,
            body_demand: EvaluationDemand::Value,
        },
        instance_state: FunctionInstanceState::None,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::default(),
    }
}
fn facts(domain: EvaluationRegionId) -> CallEffects {
    let base = base();
    CallEffects {
        value_stability: base.value_stability,
        own_row_error: base.own_row_error,
        failure_behavior: base.failure_behavior,
        null_behavior: base.null_behavior,
        argument_control: base.argument_control,
        instance_state: base.instance_state,
        observable_effects: base.observable_effects,
        environment: Box::default(),
        proof_scope: CallProofScope::Region(domain),
    }
}
fn signature() -> Box<[FunctionArgumentType]> {
    Box::from([
        FunctionArgumentType::Value(int()),
        FunctionArgumentType::Lambda {
            parameter_types: novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                Box::from([int()]),
            ),
            result_type: int(),
        },
    ])
}
struct Owner {
    declaration: FunctionBindingDeclaration,
    code: [PureImplementationDeclaration; 1],
}
impl Owner {
    fn new() -> Self {
        Self {
            declaration: FunctionBindingDeclaration::try_new_complete(
                fid(),
                FunctionKind::Scalar,
                [FunctionOverloadDeclaration::from_effects(
                    oid(),
                    "INT64, lambda(INT64)->INT64",
                    "INT64",
                    None,
                    base(),
                )],
            )
            .unwrap(),
            code: [PureImplementationDeclaration {
                overload: oid(),
                implementation: PureImplementationId::try_new(IMPLEMENTATION).unwrap(),
                abi: PureKernelAbi::HigherOrderV1,
            }],
        }
    }
}
impl PureFunctionMetadataOwner for Owner {
    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }
    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.code
    }
}
impl FunctionBindingResolver for Owner {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        let selected = FunctionBindingSelection {
            overload: oid(),
            argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                signature(),
            ),
            result_type: FunctionResultType::Scalar(int()),
            aggregate: None,
        };
        self.validate_selected(&selected, request, _control)?;
        Ok(selected)
    }
    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        if selected.overload != oid()
            || selected.argument_types.as_slice() != signature().as_ref()
            || selected.result_type != FunctionResultType::Scalar(int())
            || selected.aggregate.is_some()
            || request.logical_argument_count != 2
            || request
                .arguments
                .iter()
                .map(FunctionArgument::argument_type)
                .collect::<Box<[_]>>()
                != signature()
        {
            return Err(FunctionBindingError::InvalidBinding(
                "fixture signature differs".into(),
            ));
        }
        Ok(())
    }
}
impl FunctionEffectOwner for Owner {
    type Error = FunctionBindingError;
    fn declaration(
        &self,
        function: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if *function != fid() {
            return Err(FunctionBindingError::UnknownFunction);
        }
        self.declaration.effect_declaration(&selected.overload)
    }
    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        control
            .checkpoint(CompilePhase::FunctionSpecialization, 0)
            .map_err(FunctionEffectOwnerError::Control)?;
        self.declaration(input.function_id, input.selected)?;
        self.validate_selected(input.selected, input.request, control)?;
        if !input.environment.is_empty() {
            return Err(
                FunctionBindingError::InvalidBinding("fixture has no environment".into()).into(),
            );
        }
        Ok(facts(input.context.region))
    }
}
#[derive(Debug)]
struct Prepared {
    contract: Arc<HigherOrderCallContract>,
    body: Arc<LambdaBodyContract>,
}
impl PreparedHigherOrderKernel for Prepared {
    fn contract(&self) -> &Arc<HigherOrderCallContract> {
        &self.contract
    }
    fn body_contract(&self) -> &Arc<LambdaBodyContract> {
        &self.body
    }
    fn instance_retained_upper_bound(&self) -> usize {
        0
    }
    fn create_instance(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn HigherOrderKernelInstance>, KernelFailure> {
        control.checkpoint(0)?;
        // Closure construction supplies no runtime expansion backing.
        Err(KernelFailure::ResourceExhausted)
    }
}
impl PureHigherOrderImplementation for Owner {
    fn prepare_higher_order(
        &self,
        _: CallEffectInput<'_>,
        contract: Arc<HigherOrderCallContract>,
        body: Arc<LambdaBodyContract>,
        _: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedHigherOrderKernel>, KernelFailure> {
        Ok(Arc::new(Prepared { contract, body }))
    }
}
fn catalog() -> PureEngineFunctionCatalog {
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            FunctionDefinition::try_new_pure_higher_order(
                "lexical_fixture",
                FunctionVisibility::Public,
                Arc::new(Owner::new()),
            )
            .unwrap(),
        )
        .unwrap();
    builder
        .seal_pure([InstalledPureKernel {
            function: fid(),
            kind: FunctionKind::Scalar,
            implementation: PureImplementationDeclaration {
                overload: oid(),
                implementation: PureImplementationId::try_new(IMPLEMENTATION).unwrap(),
                abi: PureKernelAbi::HigherOrderV1,
            },
            aggregate_state_format: None,
        }])
        .unwrap()
}
fn occurrence(id: u32) -> ProgramUseRef {
    ProgramUseRef {
        arena: ProgramExpressionArena::Main,
        use_id: ExpressionUseId::new(id),
    }
}
fn input() -> ProgramLexicalSource {
    ProgramLexicalSource::Input(ProgramChannelSite::Layout {
        node: ProgramNodeId::new(0),
        role: ProgramChannelLayoutRole::NodeOutput,
        ordinal: 0,
    })
}
fn parameter(edge: u32) -> ProgramLexicalSource {
    ProgramLexicalSource::Parameter {
        lambda: occurrence(edge),
        ordinal: 0,
    }
}
fn common(edge: u32, ordinal: u32) -> ProgramLexicalSource {
    ProgramLexicalSource::Common {
        lambda: occurrence(edge),
        ordinal,
    }
}
fn capture(edge: u32) -> ProgramLexicalSource {
    ProgramLexicalSource::Capture {
        lambda: occurrence(edge),
        ordinal: 0,
    }
}
fn slot(use_id: u32, source: ProgramLexicalSource) -> ProgramSlotBinding {
    ProgramSlotBinding {
        occurrence: occurrence(use_id),
        source,
    }
}
fn lambda(edge: u32, captures: &[ProgramLexicalSource]) -> ProgramLambdaBinding {
    ProgramLambdaBinding {
        edge: occurrence(edge),
        captures: captures.into(),
    }
}
fn use_(
    id: u32,
    definition: usize,
    domain: EvaluationRegionId,
    control: ControlShape,
    args: &[u32],
) -> ProgramExpressionUse {
    ProgramExpressionUse {
        context: ExpressionEffectContext {
            use_id: ExpressionUseId::new(id),
            region: domain,
            demand: EvaluationDemand::Value,
        },
        definition: ProgramExprId::new(definition),
        control,
        arguments: args.iter().map(|id| ExpressionUseId::new(*id)).collect(),
    }
}
fn eager(id: u32, definition: usize, domain: EvaluationRegionId) -> ProgramExpressionUse {
    use_(id, definition, domain, ControlShape::Eager, &[])
}
fn higher(
    id: u32,
    definition: usize,
    domain: EvaluationRegionId,
    args: &[u32],
) -> ProgramExpressionUse {
    use_(
        id,
        definition,
        domain,
        ControlShape::HigherOrder {
            body_ordinal: 1,
            body_demand: EvaluationDemand::Value,
        },
        args,
    )
}
fn lit() -> StaticExprKind {
    StaticExprKind::Literal(StaticLiteral::Int64(4))
}
fn ref_(slot: u32) -> StaticExprKind {
    StaticExprKind::SlotId(SlotId::new(slot))
}
fn lambda_def(body: usize, param: u32, common: &[(u32, usize)]) -> StaticExprKind {
    StaticExprKind::LambdaFunction {
        body: ProgramExprId::new(body),
        arg_slots: vec![SlotId::new(param)],
        common_sub_exprs: common
            .iter()
            .map(|(slot, def)| (SlotId::new(*slot), ProgramExprId::new(*def)))
            .collect(),
        is_nondeterministic: false,
    }
}
fn call(value: usize, lambda: usize) -> StaticExprKind {
    StaticExprKind::FunctionCall {
        kind: StaticFunctionKind::ArrayMap,
        args: vec![ProgramExprId::new(value), ProgramExprId::new(lambda)],
    }
}
struct Fixture {
    channels: ProgramTypedChannels,
    lambdas: Vec<ProgramLambdaBinding>,
    slots: Vec<ProgramSlotBinding>,
}
fn build(
    definitions: Vec<StaticExprKind>,
    uses: Vec<ProgramExpressionUse>,
    roots: &[(usize, u32)],
    domains: Vec<ProgramEvaluationRegion>,
    lambdas: Vec<ProgramLambdaBinding>,
    slots: Vec<ProgramSlotBinding>,
) -> Fixture {
    build_project(
        definitions,
        uses,
        roots,
        domains,
        lambdas,
        slots,
        None,
        None,
    )
}
// Keep independent source, control, closure and output axes explicit in fixtures.
fn build_project(
    definitions: Vec<StaticExprKind>,
    uses: Vec<ProgramExpressionUse>,
    roots: &[(usize, u32)],
    domains: Vec<ProgramEvaluationRegion>,
    lambdas: Vec<ProgramLambdaBinding>,
    slots: Vec<ProgramSlotBinding>,
    projected_slots: Option<Vec<SlotId>>,
    output_indices: Option<Vec<usize>>,
) -> Fixture {
    let types = definitions
        .iter()
        .map(|definition| match definition {
            StaticExprKind::LambdaFunction { arg_slots, .. } => FunctionArgumentType::Lambda {
                parameter_types: vec![int(); arg_slots.len()].into(),
                result_type: int(),
            },
            _ => FunctionArgumentType::Value(int()),
        })
        .collect::<Vec<_>>();
    let expressions = Arc::new(
        ImmutableExpressions::try_new(
            definitions
                .into_iter()
                .map(|definition| StaticExprNode::new(definition, DataType::Int64, None))
                .collect(),
            false,
            HashMap::new(),
            None,
        )
        .unwrap(),
    );
    let source_schema = Arc::new(Schema::new(vec![Field::new(
        "source",
        DataType::Int64,
        false,
    )]));
    let source_layout =
        StaticLayout::try_new(source_schema.clone(), Arc::from([SlotId::new(1)])).unwrap();
    let values = StaticValues::try_new(
        RecordBatch::try_new(source_schema, vec![Arc::new(Int64Array::from(vec![4, 2]))]).unwrap(),
        source_layout.clone(),
    )
    .unwrap();
    let expr_slots = projected_slots.unwrap_or_else(|| {
        (0..roots.len())
            .map(|i| SlotId::new(10 + i as u32))
            .collect()
    });
    let visible_slots = output_indices
        .as_ref()
        .map(|indices| indices.iter().map(|i| expr_slots[*i]).collect::<Vec<_>>())
        .unwrap_or_else(|| expr_slots.clone());
    let output_count = visible_slots.len();
    let output = StaticLayout::try_new(
        Arc::new(Schema::new(
            (0..output_count)
                .map(|i| Field::new(format!("output{i}"), DataType::Int64, false))
                .collect::<Vec<_>>(),
        )),
        visible_slots.into(),
    )
    .unwrap();
    let profile = CompileProfile::new(
        NonZeroUsize::new(1).unwrap(),
        None,
        output.identity().unwrap(),
        KernelAbiVersion::CURRENT,
    );
    let program = LocalProgramGraph::try_new(
        vec![
            ProgramNode::new(1, ProgramNodeKind::Values { values }, source_layout),
            ProgramNode::new(
                2,
                ProgramNodeKind::Project {
                    input: ProgramNodeId::new(0),
                    is_subordinate: false,
                    validate_final_result_input: false,
                    exprs: roots
                        .iter()
                        .map(|(definition, _)| ProgramExprId::new(*definition))
                        .collect(),
                    expr_slot_ids: expr_slots,
                    expr_slot_schemas: None,
                    output_indices,
                },
                output,
            ),
        ],
        ProgramNodeId::new(1),
        expressions,
        profile,
        BindingRequirements::try_new(vec![]).unwrap(),
    )
    .unwrap();
    let flow =
        ProgramControlFlow::try_new(domains, uses, types.len(), &Control::default()).unwrap();
    let mut calls = Vec::new();
    let catalog = catalog();
    for invocation in flow.uses().values() {
        if !matches!(invocation.control, ControlShape::HigherOrder { .. }) {
            continue;
        }
        let edge = &flow.uses()[&invocation.arguments[1]];
        let body = &flow.uses()[edge.arguments.last().unwrap()];
        let arguments = [
            FunctionArgument::Value {
                value_type: int(),
                constant: None,
            },
            FunctionArgument::Lambda {
                parameter_types: novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                    Box::from([int()]),
                ),
                result_type: int(),
            },
        ];
        let selected = Arc::new(FunctionBindingSelection {
            overload: oid(),
            argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                signature(),
            ),
            result_type: FunctionResultType::Scalar(int()),
            aggregate: None,
        });
        let parameters = SemanticParameters::default();
        let function = fid();
        let uses = invocation
            .arguments
            .iter()
            .copied()
            .map(Some)
            .collect::<Vec<_>>();
        let input = CallEffectInput {
            context: invocation.context,
            argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(&uses),
            function_id: &function,
            kind: FunctionKind::Scalar,
            selected: &selected,
            request: FunctionBindingRequest {
                expected_result_type: None,
                arguments: &arguments,
                logical_argument_count: 2,
            },
            environment: &[],
            parameters: &parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            proof_scope: CallProofScope::Region(invocation.context.region),
        };
        let count = lambdas
            .iter()
            .find(|lambda| lambda.edge.use_id == edge.context.use_id)
            .unwrap()
            .captures
            .len();
        let token = catalog
            .prepare_frozen(
                input,
                selected.clone(),
                &facts(invocation.context.region),
                PureCallPreparation::HigherOrder(HigherOrderPreparationOptions {
                    arguments: ScopedExpressionEffects::pure_value(invocation.context),
                    body_context: body.context,
                    body_effects: ScopedExpressionEffects::pure_value(body.context),
                    capture_types: vec![int(); count].into(),
                }),
                &Control::default(),
            )
            .unwrap();
        calls.push((
            ProgramCallSite::Expression(occurrence(invocation.context.use_id.get())),
            token,
        ));
    }
    let snapshot = ProgramRootControlBindings::try_new(
        program,
        BTreeMap::from([(ProgramExpressionArena::Main, flow)]),
        roots
            .iter()
            .enumerate()
            .map(|(index, (_, id))| ProgramRootUseBinding {
                site: ProgramExpressionRootSite::Node {
                    node: ProgramNodeId::new(1),
                    role: ProgramNodeExpressionRole::ProjectOutput {
                        expression: index as u32,
                    },
                },
                use_id: ExpressionUseId::new(*id),
            })
            .collect(),
        &Control::default(),
    )
    .unwrap();
    let calls = crate::ProgramResolvedCalls::try_new(snapshot, calls, &Control::default()).unwrap();
    let expressions = ProgramTypedExpressions::try_new(
        calls,
        BTreeMap::from([(ProgramExpressionArena::Main, types)]),
        &Control::default(),
    )
    .unwrap();
    let mut channels = vec![(
        ProgramChannelSite::Layout {
            node: ProgramNodeId::new(0),
            role: ProgramChannelLayoutRole::NodeOutput,
            ordinal: 0,
        },
        int(),
    )];
    for ordinal in 0..output_count {
        channels.push((
            ProgramChannelSite::Layout {
                node: ProgramNodeId::new(1),
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal: ordinal as u32,
            },
            int(),
        ));
    }
    Fixture {
        channels: ProgramTypedChannels::try_new(expressions, channels, &Control::default())
            .unwrap(),
        lambdas,
        slots,
    }
}
fn domains(nested: bool, inner_owner: u32) -> Vec<ProgramEvaluationRegion> {
    let mut out = vec![
        ProgramEvaluationRegion {
            id: ROOT_DOMAIN,
            parent: None,
            guard: None,
        },
        ProgramEvaluationRegion {
            id: OUTER_DOMAIN,
            parent: Some(ROOT_DOMAIN),
            guard: Some(RegionGuard {
                owner: ExpressionUseId::new(0),
                kind: GuardKind::LambdaInvocation,
            }),
        },
    ];
    if nested {
        out.push(ProgramEvaluationRegion {
            id: INNER_DOMAIN,
            parent: Some(OUTER_DOMAIN),
            guard: Some(RegionGuard {
                owner: ExpressionUseId::new(inner_owner),
                kind: GuardKind::LambdaInvocation,
            }),
        });
    }
    out
}
fn parameter_fixture() -> Fixture {
    build(
        vec![ref_(99), lit(), lambda_def(0, 99, &[]), call(1, 2)],
        vec![
            higher(0, 3, ROOT_DOMAIN, &[1, 2]),
            eager(1, 1, ROOT_DOMAIN),
            use_(2, 2, OUTER_DOMAIN, ControlShape::LambdaBody, &[3]),
            eager(3, 0, OUTER_DOMAIN),
        ],
        &[(3, 0)],
        domains(false, 0),
        vec![lambda(2, &[])],
        vec![slot(3, parameter(2))],
    )
}
fn capture_fixture() -> Fixture {
    build(
        vec![ref_(1), lit(), lambda_def(0, 99, &[]), call(1, 2)],
        vec![
            higher(0, 3, ROOT_DOMAIN, &[1, 2]),
            eager(1, 1, ROOT_DOMAIN),
            use_(2, 2, OUTER_DOMAIN, ControlShape::LambdaBody, &[3]),
            eager(3, 0, OUTER_DOMAIN),
        ],
        &[(3, 0)],
        domains(false, 0),
        vec![lambda(2, &[input()])],
        vec![slot(3, capture(2))],
    )
}
fn common_fixture() -> Fixture {
    build(
        vec![
            ref_(99),
            ref_(100),
            ref_(101),
            lit(),
            lambda_def(2, 99, &[(100, 0), (101, 1)]),
            call(3, 4),
        ],
        vec![
            higher(0, 5, ROOT_DOMAIN, &[1, 2]),
            eager(1, 3, ROOT_DOMAIN),
            use_(2, 4, OUTER_DOMAIN, ControlShape::LambdaBody, &[3, 4, 5]),
            eager(3, 0, OUTER_DOMAIN),
            eager(4, 1, OUTER_DOMAIN),
            eager(5, 2, OUTER_DOMAIN),
        ],
        &[(5, 0)],
        domains(false, 0),
        vec![lambda(2, &[])],
        vec![
            slot(3, parameter(2)),
            slot(4, common(2, 0)),
            slot(5, common(2, 1)),
        ],
    )
}
fn nested_fixture(common_parent: bool, shadow: bool) -> Fixture {
    let (captured, outer_common, outer_args, body, inner_edge, inner_body, inner_owner) =
        if common_parent {
            (100, vec![(100, 1)], vec![3, 4], 4, 6, 7, 4)
        } else {
            (99, vec![], vec![3], 3, 5, 6, 3)
        };
    let parent_source = if common_parent {
        common(2, 0)
    } else {
        parameter(2)
    };
    let captures = if shadow { vec![] } else { vec![parent_source] };
    let param_slot = if shadow { captured } else { 101 };
    let mut uses = vec![
        higher(0, 5, ROOT_DOMAIN, &[1, 2]),
        eager(1, 1, ROOT_DOMAIN),
        use_(2, 4, OUTER_DOMAIN, ControlShape::LambdaBody, &outer_args),
    ];
    if common_parent {
        uses.push(eager(3, 1, OUTER_DOMAIN));
    }
    uses.extend([
        higher(body, 3, OUTER_DOMAIN, &[body + 1, inner_edge]),
        eager(body + 1, 1, OUTER_DOMAIN),
        use_(
            inner_edge,
            2,
            INNER_DOMAIN,
            ControlShape::LambdaBody,
            &[inner_body],
        ),
        eager(inner_body, 0, INNER_DOMAIN),
    ]);
    build(
        vec![
            ref_(captured),
            lit(),
            lambda_def(0, param_slot, &[]),
            call(1, 2),
            lambda_def(3, 99, &outer_common),
            call(1, 4),
        ],
        uses,
        &[(5, 0)],
        domains(true, inner_owner),
        vec![lambda(2, &[]), lambda(inner_edge, &captures)],
        vec![slot(
            inner_body,
            if shadow {
                parameter(inner_edge)
            } else {
                capture(inner_edge)
            },
        )],
    )
}
fn construct(fixture: Fixture) -> Result<ProgramLexicalBindings, ProgramLexicalBindingError> {
    ProgramLexicalBindings::try_new(
        fixture.channels,
        fixture.lambdas,
        fixture.slots,
        &Control::default(),
    )
}
#[test]
fn parameters_input_captures_and_ordered_common_locals_use_real_same_snapshot() {
    for fixture in [parameter_fixture(), capture_fixture(), common_fixture()] {
        let expected = fixture.channels.clone();
        let checked = construct(fixture).unwrap();
        assert!(std::ptr::eq(
            checked
                .channels()
                .expressions()
                .resolved_calls()
                .snapshot()
                .program()
                .expressions()
                .as_ref(),
            expected
                .expressions()
                .resolved_calls()
                .snapshot()
                .program()
                .expressions()
                .as_ref()
        ));
        assert_eq!(checked.lambdas().len(), 1);
    }
}
#[test]
fn nested_capture_parameters_common_and_shadowing_are_lexically_exact() {
    construct(nested_fixture(false, false)).unwrap();
    construct(nested_fixture(true, false)).unwrap();
    construct(nested_fixture(false, true)).unwrap();
    let mut fixture = nested_fixture(false, false);
    fixture.slots[0].source = parameter(2);
    assert_eq!(
        construct(fixture).unwrap_err(),
        ProgramLexicalBindingError::WrongScope
    );
    let mut fixture = nested_fixture(false, false);
    fixture.lambdas[1].captures[0] = input();
    assert_eq!(
        construct(fixture).unwrap_err(),
        ProgramLexicalBindingError::WrongScope
    );
}
#[test]
fn ordinary_input_slot_is_not_an_unbound_capture() {
    let fixture = build(
        vec![ref_(1)],
        vec![eager(0, 0, ROOT_DOMAIN)],
        &[(0, 0)],
        vec![ProgramEvaluationRegion {
            id: ROOT_DOMAIN,
            parent: None,
            guard: None,
        }],
        vec![],
        vec![slot(0, input())],
    );
    let checked = construct(fixture).unwrap();
    assert!(checked.lambdas().is_empty());
    assert_eq!(checked.slots()[&occurrence(0)], input());
}
#[test]
fn missing_extra_duplicate_or_foreign_occurrences_never_reuse_bindings() {
    let mut fixture = capture_fixture();
    fixture.slots.clear();
    assert_eq!(
        construct(fixture).unwrap_err(),
        ProgramLexicalBindingError::IncompleteCoverage
    );
    let mut fixture = capture_fixture();
    fixture.slots.push(slot(1, input()));
    assert_eq!(
        construct(fixture).unwrap_err(),
        ProgramLexicalBindingError::IncompleteCoverage
    );
    let mut fixture = capture_fixture();
    fixture.slots.push(fixture.slots[0]);
    assert_eq!(
        construct(fixture).unwrap_err(),
        ProgramLexicalBindingError::DuplicateBinding
    );
    let mut fixture = capture_fixture();
    fixture.lambdas[0].edge = occurrence(u32::MAX);
    assert_eq!(
        construct(fixture).unwrap_err(),
        ProgramLexicalBindingError::InvalidLambda
    );
    let mut fixture = capture_fixture();
    fixture.slots[0].occurrence.arena = ProgramExpressionArena::Sink;
    assert_eq!(
        construct(fixture).unwrap_err(),
        ProgramLexicalBindingError::IncompleteCoverage
    );
}
#[test]
fn forward_common_wrong_slot_source_and_capture_bypass_are_rejected() {
    let mut fixture = common_fixture();
    fixture.slots[0].source = common(2, 1);
    assert_eq!(
        construct(fixture).unwrap_err(),
        ProgramLexicalBindingError::ForwardCommon
    );
    let mut fixture = parameter_fixture();
    fixture.slots[0].source = ProgramLexicalSource::Parameter {
        lambda: occurrence(2),
        ordinal: 1,
    };
    assert_eq!(
        construct(fixture).unwrap_err(),
        ProgramLexicalBindingError::InvalidSource
    );
    let mut fixture = common_fixture();
    fixture.slots[1].source = parameter(2);
    assert_eq!(
        construct(fixture).unwrap_err(),
        ProgramLexicalBindingError::WrongSlot
    );
    let mut fixture = capture_fixture();
    fixture.slots[0].source = input();
    assert_eq!(
        construct(fixture).unwrap_err(),
        ProgramLexicalBindingError::WrongScope
    );
    let mut fixture = capture_fixture();
    fixture.lambdas[0].captures[0] = ProgramLexicalSource::Input(ProgramChannelSite::Layout {
        node: ProgramNodeId::new(1),
        role: ProgramChannelLayoutRole::NodeOutput,
        ordinal: 0,
    });
    assert_eq!(
        construct(fixture).unwrap_err(),
        ProgramLexicalBindingError::WrongScope
    );
}
#[test]
fn new_closure_records_share_one_bound_and_observe_typed_outer_control() {
    for units in [0, 256] {
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let uses = (0..100).map(|id| eager(id, 0, ROOT_DOMAIN)).collect();
            let roots = (0..100).map(|id| (0, id)).collect::<Vec<_>>();
            let fixture = build(
                vec![ref_(1)],
                uses,
                &roots,
                vec![ProgramEvaluationRegion {
                    id: ROOT_DOMAIN,
                    parent: None,
                    guard: None,
                }],
                vec![],
                (0..100).map(|id| slot(id, input())).collect(),
            );
            let control = Control {
                fail: Some((units, error.clone())),
                ..Control::default()
            };
            assert_eq!(
                ProgramLexicalBindings::try_new(
                    fixture.channels,
                    fixture.lambdas,
                    fixture.slots,
                    &control
                )
                .unwrap_err(),
                ProgramLexicalBindingError::Control(error)
            );
        }
    }
    let fixture = parameter_fixture();
    assert_eq!(
        ProgramLexicalBindings::try_new(
            fixture.channels,
            vec![],
            vec![slot(3, parameter(2)); MAX_CONTROL_USE_REFERENCES + 1],
            &Control::default()
        )
        .unwrap_err(),
        ProgramLexicalBindingError::TooManyItems
    );
}

#[test]
fn same_lambda_definition_has_distinct_parameter_frames_for_each_real_use() {
    let second_domain = ProgramEvaluationRegion {
        id: INNER_DOMAIN,
        parent: Some(ROOT_DOMAIN),
        guard: Some(RegionGuard {
            owner: ExpressionUseId::new(4),
            kind: GuardKind::LambdaInvocation,
        }),
    };
    let mut domains = domains(false, 0);
    domains.push(second_domain);
    let fixture = build(
        vec![ref_(99), lit(), lambda_def(0, 99, &[]), call(1, 2)],
        vec![
            higher(0, 3, ROOT_DOMAIN, &[1, 2]),
            eager(1, 1, ROOT_DOMAIN),
            use_(2, 2, OUTER_DOMAIN, ControlShape::LambdaBody, &[3]),
            eager(3, 0, OUTER_DOMAIN),
            higher(4, 3, ROOT_DOMAIN, &[5, 6]),
            eager(5, 1, ROOT_DOMAIN),
            use_(6, 2, INNER_DOMAIN, ControlShape::LambdaBody, &[7]),
            eager(7, 0, INNER_DOMAIN),
        ],
        &[(3, 0), (3, 4)],
        domains,
        vec![lambda(2, &[]), lambda(6, &[])],
        vec![slot(3, parameter(2)), slot(7, parameter(6))],
    );
    construct(Fixture {
        channels: fixture.channels.clone(),
        lambdas: fixture.lambdas.clone(),
        slots: fixture.slots.clone(),
    })
    .unwrap();
    let mut wrong = fixture;
    wrong.slots[1].source = parameter(2);
    assert_eq!(
        construct(wrong).unwrap_err(),
        ProgramLexicalBindingError::WrongScope
    );
}

#[test]
fn unused_capture_and_capture_that_shadows_inner_parameter_are_rejected() {
    let unused = build(
        vec![lit(), lambda_def(0, 99, &[]), call(0, 1)],
        vec![
            higher(0, 2, ROOT_DOMAIN, &[1, 2]),
            eager(1, 0, ROOT_DOMAIN),
            use_(2, 1, OUTER_DOMAIN, ControlShape::LambdaBody, &[3]),
            eager(3, 0, OUTER_DOMAIN),
        ],
        &[(2, 0)],
        domains(false, 0),
        vec![lambda(2, &[input()])],
        vec![],
    );
    assert_eq!(
        construct(unused).unwrap_err(),
        ProgramLexicalBindingError::UnusedCapture
    );
    let shadowed = build(
        vec![
            ref_(99),
            lit(),
            lambda_def(0, 99, &[]),
            call(1, 2),
            lambda_def(3, 99, &[]),
            call(1, 4),
        ],
        vec![
            higher(0, 5, ROOT_DOMAIN, &[1, 2]),
            eager(1, 1, ROOT_DOMAIN),
            use_(2, 4, OUTER_DOMAIN, ControlShape::LambdaBody, &[3]),
            higher(3, 3, OUTER_DOMAIN, &[4, 5]),
            eager(4, 1, OUTER_DOMAIN),
            use_(5, 2, INNER_DOMAIN, ControlShape::LambdaBody, &[6]),
            eager(6, 0, INNER_DOMAIN),
        ],
        &[(5, 0)],
        domains(true, 3),
        vec![lambda(2, &[]), lambda(5, &[parameter(2)])],
        vec![slot(6, capture(5))],
    );
    assert_eq!(
        construct(shadowed).unwrap_err(),
        ProgramLexicalBindingError::ShadowedSource
    );
}

fn projected(expression: u32) -> ProgramLexicalSource {
    ProgramLexicalSource::Projected {
        node: ProgramNodeId::new(1),
        expression,
    }
}
#[test]
fn prior_unprojected_computation_and_same_slot_replacement_have_exact_sources() {
    for produced in [1, 20] {
        let fixture = build_project(
            vec![lit(), ref_(produced)],
            vec![eager(0, 0, ROOT_DOMAIN), eager(1, 1, ROOT_DOMAIN)],
            &[(0, 0), (1, 1)],
            vec![ProgramEvaluationRegion {
                id: ROOT_DOMAIN,
                parent: None,
                guard: None,
            }],
            vec![],
            vec![slot(1, projected(0))],
            Some(vec![SlotId::new(produced), SlotId::new(10)]),
            Some(vec![1]),
        );
        assert!(
            fixture
                .channels
                .slot_type(
                    ProgramNodeId::new(1),
                    ProgramChannelLayoutRole::NodeOutput,
                    SlotId::new(produced)
                )
                .is_none()
        );
        construct(fixture).unwrap();
    }
    let stale = build_project(
        vec![lit(), ref_(1)],
        vec![eager(0, 0, ROOT_DOMAIN), eager(1, 1, ROOT_DOMAIN)],
        &[(0, 0), (1, 1)],
        vec![ProgramEvaluationRegion {
            id: ROOT_DOMAIN,
            parent: None,
            guard: None,
        }],
        vec![],
        vec![slot(1, input())],
        Some(vec![SlotId::new(1), SlotId::new(10)]),
        Some(vec![1]),
    );
    assert_eq!(
        construct(stale).unwrap_err(),
        ProgramLexicalBindingError::ShadowedSource
    );
    let future = build_project(
        vec![ref_(1), lit()],
        vec![eager(0, 0, ROOT_DOMAIN), eager(1, 1, ROOT_DOMAIN)],
        &[(0, 0), (1, 1)],
        vec![ProgramEvaluationRegion {
            id: ROOT_DOMAIN,
            parent: None,
            guard: None,
        }],
        vec![],
        vec![slot(0, projected(1))],
        Some(vec![SlotId::new(20), SlotId::new(1)]),
        Some(vec![0]),
    );
    assert_eq!(
        construct(future).unwrap_err(),
        ProgramLexicalBindingError::WrongScope
    );
}
#[test]
fn nested_capture_transitively_retains_the_actual_prior_projected_producer() {
    let fixture = build_project(
        vec![
            ref_(1),
            lit(),
            lambda_def(0, 101, &[]),
            call(1, 2),
            lambda_def(3, 99, &[]),
            call(1, 4),
        ],
        vec![
            eager(8, 1, ROOT_DOMAIN),
            higher(0, 5, ROOT_DOMAIN, &[1, 2]),
            eager(1, 1, ROOT_DOMAIN),
            use_(2, 4, OUTER_DOMAIN, ControlShape::LambdaBody, &[3]),
            higher(3, 3, OUTER_DOMAIN, &[4, 5]),
            eager(4, 1, OUTER_DOMAIN),
            use_(5, 2, INNER_DOMAIN, ControlShape::LambdaBody, &[6]),
            eager(6, 0, INNER_DOMAIN),
        ],
        &[(1, 8), (5, 0)],
        domains(true, 3),
        vec![lambda(2, &[projected(0)]), lambda(5, &[capture(2)])],
        vec![slot(6, capture(5))],
        Some(vec![SlotId::new(1), SlotId::new(10)]),
        Some(vec![1]),
    );
    let checked = construct(fixture).unwrap();
    assert_eq!(checked.lambdas()[&occurrence(2)].captures[0], projected(0));
    assert_eq!(checked.lambdas()[&occurrence(5)].captures[0], capture(2));
}

// These neutral-constructor probes observe actual request ports and finite
// holders. They do not stand in for BE tracker or Native capture publication.
struct LexicalCreationHold(Arc<std::sync::atomic::AtomicUsize>);
impl novarocks_type_contract::ValueTypeStorageHold for LexicalCreationHold {}
impl Drop for LexicalCreationHold {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
fn lexical_creation_storage(
    exits: &Arc<std::sync::atomic::AtomicUsize>,
) -> ValueTypeStorageLifetime {
    ValueTypeStorageLifetime::held(Arc::new(LexicalCreationHold(exits.clone())))
}
#[test]
fn lexical_created_maps_keep_retained_owner_after_temporary_and_creator_exit() {
    let fixture = parameter_fixture();
    let expected = construct(parameter_fixture()).unwrap();
    let temporary_exits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let retained_exits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let temporary_events = std::cell::RefCell::new(Vec::new());
    let retained_events = std::cell::RefCell::new(Vec::new());
    let result = {
        let mut tr = |layout: std::alloc::Layout| -> Result<(), u64> {
            temporary_events.borrow_mut().push(layout.size());
            Ok(())
        };
        let mut to = |bytes: usize| -> Result<(), u64> {
            temporary_events.borrow_mut().push(bytes);
            Ok(())
        };
        let mut rr = |layout: std::alloc::Layout| -> Result<(), u64> {
            retained_events.borrow_mut().push(layout.size());
            Ok(())
        };
        let mut ro = |bytes: usize| -> Result<(), u64> {
            retained_events.borrow_mut().push(bytes);
            Ok(())
        };
        let mut temporary = novarocks_type_contract::ValueSchemaCreation::new(
            lexical_creation_storage(&temporary_exits),
            &mut tr,
            &mut to,
        );
        let mut retained = novarocks_type_contract::ValueSchemaCreation::new(
            lexical_creation_storage(&retained_exits),
            &mut rr,
            &mut ro,
        );
        ProgramLexicalBindings::try_new_with_creator_in(
            fixture.channels,
            fixture.lambdas,
            fixture.slots,
            ValueTypeStorageLifetime::pure(),
            &mut temporary,
            &mut retained,
            &Control::default(),
        )
        .unwrap()
    };
    assert_eq!(result.lambdas(), expected.lambdas());
    assert_eq!(result.slots(), expected.slots());
    assert!(!temporary_events.borrow().is_empty());
    assert!(!retained_events.borrow().is_empty());
    assert_eq!(temporary_exits.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(retained_exits.load(std::sync::atomic::Ordering::SeqCst), 0);
    let late = result.clone();
    drop(result);
    assert_eq!(retained_exits.load(std::sync::atomic::Ordering::SeqCst), 0);
    drop(late);
    assert_eq!(retained_exits.load(std::sync::atomic::Ordering::SeqCst), 1);
}
#[test]
fn lexical_first_creator_cause_is_preserved_without_temporary_or_callback_tail() {
    let fixture = parameter_fixture();
    let calls = std::cell::Cell::new(0usize);
    let mut reject = |_: std::alloc::Layout| -> Result<(), u64> {
        panic!("first insertion uses opaque geometry")
    };
    let mut opaque = |_: usize| -> Result<(), u64> {
        assert_eq!(calls.replace(calls.get() + 1), 0);
        Err(91)
    };
    let mut temporary_request = |_: std::alloc::Layout| -> Result<(), u64> {
        panic!("temporary callback after original refusal")
    };
    let mut temporary_opaque =
        |_: usize| -> Result<(), u64> { panic!("temporary callback after original refusal") };
    let mut temporary = novarocks_type_contract::ValueSchemaCreation::new(
        ValueTypeStorageLifetime::pure(),
        &mut temporary_request,
        &mut temporary_opaque,
    );
    let mut retained = novarocks_type_contract::ValueSchemaCreation::new(
        ValueTypeStorageLifetime::pure(),
        &mut reject,
        &mut opaque,
    );
    let error = ProgramLexicalBindings::try_new_with_creator_in(
        fixture.channels,
        fixture.lambdas,
        fixture.slots,
        ValueTypeStorageLifetime::pure(),
        &mut temporary,
        &mut retained,
        &Control::default(),
    )
    .unwrap_err();
    assert!(matches!(error, ProgramCreationFailure::Creator(91)));
    assert_eq!(calls.get(), 1);
}
