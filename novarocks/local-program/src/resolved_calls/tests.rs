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

//! Real sealed-catalogue tokens and actual local occurrence forests. These
//! tests verify correspondence, not complete expression/capture/runtime proof.

use super::*;
use crate::{
    BindingRequirements, CompileProfile, ImmutableExpressions, KernelAbiVersion, LocalProgramGraph,
    ProgramControlFlow, ProgramEvaluationDomain, ProgramExpressionUse, ProgramNode,
    ProgramRootUseBinding, StaticExprNode, StaticFunctionKind, StaticLiteral, StaticValues,
};
use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use novarocks_functions::*;
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, DecimalOverflowPolicy, DomainGuard,
    EvaluationDomainId, ExpressionEffectContext, FunctionEffectDeclaration,
    FunctionFailureBehavior, FunctionInstanceState, FunctionIntrinsicRowError, FunctionKind,
    FunctionNullBehavior, FunctionVolatility, GuardKind, ObservableEffects, SemanticParameters,
};
use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

const FUNCTION: &str = "fixture/local-resolved/function-v1";
const TYPE_FUNCTION: &str = "fixture/local-resolved/type-function-v1";
const HIGHER_FUNCTION: &str = "fixture/local-resolved/higher-function-v1";
const CONTROL_FUNCTION: &str = "fixture/local-resolved/control-v1";
const SCALAR: &str = "fixture/local-resolved/compare-v1";
const TYPE_ONLY: &str = "fixture/local-resolved/type-v1";
const HIGHER: &str = "fixture/local-resolved/higher-v1";
const IF: &str = "fixture/local-resolved/if-v1";
const DOMAIN: EvaluationDomainId = EvaluationDomainId::new(7);

#[derive(Default)]
struct Control {
    failure: Option<(u32, CompileControlError)>,
    work: Mutex<Vec<(CompilePhase, u32)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        self.work.lock().unwrap().push((phase, units));
        if let Some((at, failure)) = self.failure.clone()
            && at == units
        {
            Err(failure)
        } else {
            Ok(())
        }
    }
}
struct Runtime;
impl KernelEvaluationControl for Runtime {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        Ok(())
    }
}
fn fid(value: &str) -> FunctionId {
    FunctionId::try_new(value).unwrap()
}
fn oid(value: &str) -> FunctionOverloadId {
    FunctionOverloadId::try_new(value).unwrap()
}
fn code(value: &str) -> PureImplementationId {
    PureImplementationId::try_new(value).unwrap()
}
fn result_type() -> FunctionValueType {
    FunctionValueType::new(DataType::Boolean, true)
}
fn input_type() -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, false)
}
fn effects(control: ArgumentControl) -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: FunctionIntrinsicRowError::NoRowError,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: if control == ArgumentControl::If {
            FunctionNullBehavior::ControlDefined
        } else {
            FunctionNullBehavior::CalledOnNull
        },
        argument_control: control,
        instance_state: FunctionInstanceState::None,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::default(),
    }
}
fn argument_types(overload: &FunctionOverloadId) -> Vec<FunctionArgument> {
    let value = || FunctionArgument::Value {
        value_type: input_type(),
        constant: None,
    };
    if *overload == oid(HIGHER) {
        vec![
            value(),
            FunctionArgument::Lambda {
                parameter_types: novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                    Box::from([input_type()]),
                ),
                result_type: result_type(),
            },
        ]
    } else if *overload == oid(IF) {
        vec![
            FunctionArgument::Value {
                value_type: result_type(),
                constant: None
            };
            3
        ]
    } else {
        vec![value(), value()]
    }
}
struct Owner {
    declaration: FunctionBindingDeclaration,
    implementations: Vec<PureImplementationDeclaration>,
    instances: Arc<AtomicUsize>,
    explicit_type_only: Option<[FunctionValueType; 2]>,
    explicit_higher_parameters: Option<Box<[FunctionValueType]>>,
}
impl Owner {
    fn new(control_owner: bool) -> Self {
        Self::for_overload(if control_owner { IF } else { SCALAR })
    }
    fn for_overload(overload: &str) -> Self {
        let (function, control, abi) = match overload {
            SCALAR => (FUNCTION, ArgumentControl::Eager, PureKernelAbi::ScalarV1),
            TYPE_ONLY => (
                TYPE_FUNCTION,
                ArgumentControl::TypeOnly,
                PureKernelAbi::ScalarV1,
            ),
            HIGHER => (
                HIGHER_FUNCTION,
                ArgumentControl::HigherOrder {
                    body_ordinal: 1,
                    body_demand: EvaluationDemand::Value,
                },
                PureKernelAbi::HigherOrderV1,
            ),
            IF => (
                CONTROL_FUNCTION,
                ArgumentControl::If,
                PureKernelAbi::ControlIntrinsicV1,
            ),
            _ => unreachable!("fixture exact owner"),
        };
        let controls = [(overload, control, abi)];
        Self {
            declaration: FunctionBindingDeclaration::try_new_complete(
                fid(function),
                FunctionKind::Scalar,
                controls.iter().map(|(overload, control, _)| {
                    FunctionOverloadDeclaration::from_effects(
                        oid(overload),
                        "fixture exact channels",
                        "BOOLEAN",
                        None,
                        effects(*control),
                    )
                }),
            )
            .unwrap(),
            implementations: controls
                .iter()
                .map(|(overload, _, abi)| PureImplementationDeclaration {
                    overload: oid(overload),
                    implementation: code(overload),
                    abi: *abi,
                })
                .collect(),
            instances: Arc::new(AtomicUsize::new(0)),
            explicit_type_only: None,
            explicit_higher_parameters: None,
        }
    }
    fn arguments(&self, overload: &FunctionOverloadId) -> Vec<FunctionArgument> {
        if *overload == oid(TYPE_ONLY)
            && let Some(types) = &self.explicit_type_only
        {
            types
                .iter()
                .map(|ty| FunctionArgument::Value {
                    value_type: ty.clone(),
                    constant: None,
                })
                .collect()
        } else if *overload == oid(HIGHER)
            && let Some(types) = &self.explicit_higher_parameters
        {
            vec![
                FunctionArgument::Value {
                    value_type: input_type(),
                    constant: None,
                },
                FunctionArgument::Lambda {
                    parameter_types:
                        novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                            types.clone(),
                        ),
                    result_type: result_type(),
                },
            ]
        } else {
            argument_types(overload)
        }
    }
    fn frozen(
        &self,
        context: ExpressionEffectContext,
        selected: &FunctionBindingSelection,
    ) -> CallEffects {
        let base = self
            .declaration
            .effect_declaration(&selected.overload)
            .unwrap();
        CallEffects {
            value_stability: base.value_stability,
            own_row_error: base.own_row_error,
            failure_behavior: base.failure_behavior,
            null_behavior: base.null_behavior,
            argument_control: base.argument_control,
            instance_state: base.instance_state,
            observable_effects: base.observable_effects,
            environment: Box::default(),
            proof_scope: CallProofScope::Domain(context.domain),
        }
    }
}
impl PureFunctionMetadataOwner for Owner {
    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }
    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
}
impl FunctionBindingResolver for Owner {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        let overload = self
            .declaration
            .overloads()
            .first()
            .unwrap()
            .identity
            .clone();
        let selected = FunctionBindingSelection {
            overload,
            argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                request
                    .arguments
                    .iter()
                    .map(FunctionArgument::argument_type)
                    .collect::<Box<[_]>>(),
            ),
            result_type: FunctionResultType::Scalar(result_type()),
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
        self.declaration.effect_declaration(&selected.overload)?;
        let expected = self.arguments(&selected.overload);
        let mut arguments_equal = request.arguments.len() == expected.len();
        if arguments_equal {
            for (actual, expected) in request.arguments.iter().zip(&expected) {
                if !actual.equals_observed(
                    expected,
                    novarocks_type_contract::CompilePhase::Validate,
                    _control,
                )? {
                    arguments_equal = false;
                    break;
                }
            }
        }
        if !arguments_equal
            || request.logical_argument_count != expected.len()
            || selected.argument_types.as_slice()
                != expected
                    .iter()
                    .map(FunctionArgument::argument_type)
                    .collect::<Box<[_]>>()
                    .as_ref()
            || selected.result_type != FunctionResultType::Scalar(result_type())
            || selected.aggregate.is_some()
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
        if function != self.declaration.function_id() {
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
        Ok(self.frozen(input.context, input.selected))
    }
}
impl PureScalarImplementation for Owner {
    fn prepare_scalar(
        &self,
        _: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        _: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure> {
        Ok(Arc::new(Scalar {
            contract,
            instances: self.instances.clone(),
        }))
    }
}
#[derive(Debug)]
struct Scalar {
    contract: Arc<ScalarCallContract>,
    instances: Arc<AtomicUsize>,
}
impl PreparedScalarKernel for Scalar {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }
    fn instance_retained_upper_bound(&self) -> usize {
        0
    }
    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        self.instances.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(ScalarInstance))
    }
}
struct ScalarInstance;
impl ScalarKernelInstance for ScalarInstance {
    fn retained_bytes(&self) -> usize {
        0
    }
    fn evaluate<'input>(
        &mut self,
        input: ScalarCallInput<'_, 'input>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'input>, KernelFailure> {
        let mut output = Vec::new();
        for ordinal in 0..input.selection().len() {
            control.checkpoint(1)?;
            if input.contract().effects().argument_control == ArgumentControl::TypeOnly {
                output.push(true);
                continue;
            }
            let read = |argument: EvaluatedArgument<'_>| {
                let array = argument
                    .array()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let row = input.selection().row(ordinal).unwrap();
                array.value(argument.value_row(ordinal, row))
            };
            output.push(read(input.arguments()[0]) > read(input.arguments()[1]));
        }
        SelectedValues::try_new(
            input.selection(),
            &DataType::Boolean,
            Arc::new(BooleanArray::from(output)),
            Box::default(),
        )
        .map_err(|_| KernelFailure::Internal(KernelDiagnostic::new("fixture output differs")))
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
        Ok(Arc::new(Higher {
            contract,
            body,
            instances: self.instances.clone(),
        }))
    }
}
#[derive(Debug)]
struct Higher {
    contract: Arc<HigherOrderCallContract>,
    body: Arc<LambdaBodyContract>,
    instances: Arc<AtomicUsize>,
}
impl PreparedHigherOrderKernel for Higher {
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
        self.instances.fetch_add(1, Ordering::Relaxed);
        // These construction tests deliberately supply no expansion backing.
        // Instance refusal is an outer resource fault, never a data recipe.
        Err(KernelFailure::ResourceExhausted)
    }
}
struct FixtureCatalog {
    pure: PureEngineFunctionCatalog,
    owners: BTreeMap<&'static str, Arc<Owner>>,
}
fn catalogue() -> (FixtureCatalog, Arc<Owner>, Arc<Owner>) {
    catalogue_with_owner(Arc::new(Owner::new(false)))
}
fn catalogue_with_owner(owner: Arc<Owner>) -> (FixtureCatalog, Arc<Owner>, Arc<Owner>) {
    let controller = Arc::new(Owner::new(true));
    let mut type_owner = Owner::for_overload(TYPE_ONLY);
    type_owner.explicit_type_only = owner.explicit_type_only.clone();
    type_owner.instances = owner.instances.clone();
    let type_owner = Arc::new(type_owner);
    let mut higher_owner = Owner::for_overload(HIGHER);
    higher_owner.explicit_higher_parameters = owner.explicit_higher_parameters.clone();
    higher_owner.instances = owner.instances.clone();
    let higher_owner = Arc::new(higher_owner);
    let owners = BTreeMap::from([
        (SCALAR, owner.clone()),
        (TYPE_ONLY, type_owner),
        (HIGHER, higher_owner),
        (IF, controller.clone()),
    ]);
    let mut builder = EngineFunctionCatalogBuilder::new();
    for (overload, owner) in &owners {
        let definition = if *overload == IF {
            FunctionDefinition::try_new_pure_control(
                "local_fixture_control",
                FunctionVisibility::Public,
                owner.clone(),
            )
        } else if *overload == HIGHER {
            FunctionDefinition::try_new_pure_higher_order(
                "local_fixture_higher",
                FunctionVisibility::Public,
                owner.clone(),
            )
        } else {
            FunctionDefinition::try_new_pure_scalar(
                if *overload == TYPE_ONLY {
                    "local_fixture_type"
                } else {
                    "local_fixture_cpu"
                },
                FunctionVisibility::Public,
                owner.clone(),
            )
        }
        .unwrap();
        builder.register(definition).unwrap();
    }
    // Independent installed facts enumerate every exact owner, rather than
    // copying declarations into an automatically self-fulfilling manifest.
    let rows = [
        (FUNCTION, SCALAR, PureKernelAbi::ScalarV1),
        (TYPE_FUNCTION, TYPE_ONLY, PureKernelAbi::ScalarV1),
        (HIGHER_FUNCTION, HIGHER, PureKernelAbi::HigherOrderV1),
        (CONTROL_FUNCTION, IF, PureKernelAbi::ControlIntrinsicV1),
    ];
    let pure = builder
        .seal_pure(
            rows.into_iter()
                .map(|(function, overload, abi)| InstalledPureKernel {
                    function: fid(function),
                    kind: FunctionKind::Scalar,
                    implementation: PureImplementationDeclaration {
                        overload: oid(overload),
                        implementation: code(overload),
                        abi,
                    },
                    aggregate_state_format: None,
                }),
        )
        .unwrap();
    (FixtureCatalog { pure, owners }, owner, controller)
}
fn token(
    catalog: &FixtureCatalog,
    _owner: &Owner,
    overload: &str,
    context: ExpressionEffectContext,
    uses: &[Option<ExpressionUseId>],
    body: Option<ExpressionEffectContext>,
    fresh: bool,
) -> PureCallSpecialization {
    let owner = &catalog.owners[overload];
    let arguments = owner.arguments(&oid(overload));
    let selected = Arc::new(FunctionBindingSelection {
        overload: oid(overload),
        argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
            arguments
                .iter()
                .map(FunctionArgument::argument_type)
                .collect::<Box<[_]>>(),
        ),
        result_type: FunctionResultType::Scalar(result_type()),
        aggregate: None,
    });
    let parameters = SemanticParameters::default();
    let input = CallEffectInput {
        context,
        argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(uses),
        function_id: owner.declaration.function_id(),
        kind: FunctionKind::Scalar,
        selected: &selected,
        request: FunctionBindingRequest {
            expected_result_type: None,
            arguments: &arguments,
            logical_argument_count: arguments.len(),
        },
        environment: &[],
        parameters: &parameters,
        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        proof_scope: CallProofScope::Domain(context.domain),
    };
    let options = if overload == HIGHER {
        let body_context = body.unwrap();
        PureCallPreparation::HigherOrder(HigherOrderPreparationOptions {
            arguments: ScopedExpressionEffects::pure_value(context),
            body_context,
            body_effects: ScopedExpressionEffects::pure_value(body_context),
            capture_types: Box::default(),
        })
    } else if overload == IF {
        PureCallPreparation::ControlIntrinsic {
            arguments: ScopedExpressionEffects::pure_value(context),
        }
    } else {
        PureCallPreparation::Scalar {
            arguments: ScopedExpressionEffects::pure_value(context),
        }
    };
    if fresh {
        catalog
            .pure
            .prepare_fresh(input, selected.clone(), options, &Control::default())
            .unwrap()
    } else {
        catalog
            .pure
            .prepare_frozen(
                input,
                selected.clone(),
                &owner.frozen(context, &selected),
                options,
                &Control::default(),
            )
            .unwrap()
    }
}
fn context(
    use_id: u32,
    domain: EvaluationDomainId,
    demand: EvaluationDemand,
) -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(use_id),
        domain,
        demand,
    }
}
fn invocation(
    use_id: u32,
    definition: usize,
    control: ControlShape,
    arguments: &[u32],
) -> ProgramExpressionUse {
    ProgramExpressionUse {
        context: context(use_id, DOMAIN, EvaluationDemand::Value),
        definition: crate::ProgramExprId::new(definition),
        control,
        arguments: arguments
            .iter()
            .map(|id| ExpressionUseId::new(*id))
            .collect(),
    }
}
fn arena(nodes: Vec<(StaticExprKind, DataType)>) -> Arc<ImmutableExpressions> {
    Arc::new(
        ImmutableExpressions::try_new(
            nodes
                .into_iter()
                .map(|(kind, ty)| StaticExprNode::new(kind, ty, None))
                .collect(),
            false,
            HashMap::new(),
            None,
        )
        .unwrap(),
    )
}
fn scalar_arena() -> Arc<ImmutableExpressions> {
    arena(vec![
        (
            StaticExprKind::Literal(StaticLiteral::Int64(4)),
            DataType::Int64,
        ),
        (
            StaticExprKind::Literal(StaticLiteral::Int64(2)),
            DataType::Int64,
        ),
        (
            StaticExprKind::FunctionCall {
                kind: StaticFunctionKind::Abs,
                args: vec![crate::ProgramExprId::new(0), crate::ProgramExprId::new(1)],
            },
            DataType::Boolean,
        ),
    ])
}
fn project_program(arena: Arc<ImmutableExpressions>, definitions: &[usize]) -> LocalProgramGraph {
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
    let layout = StaticLayout::try_new(
        Arc::new(Schema::new(
            (0..definitions.len())
                .map(|i| Field::new(format!("v{i}"), DataType::Boolean, true))
                .collect::<Vec<_>>(),
        )),
        (0..definitions.len())
            .map(|i| SlotId::new(u32::try_from(i + 10).unwrap()))
            .collect::<Vec<_>>()
            .into(),
    )
    .unwrap();
    let profile = CompileProfile::new(
        NonZeroUsize::new(1).unwrap(),
        None,
        layout.identity().unwrap(),
        KernelAbiVersion::CURRENT,
    );
    LocalProgramGraph::try_new(
        vec![
            ProgramNode::new(10, ProgramNodeKind::Values { values }, source_layout),
            ProgramNode::new(
                20,
                ProgramNodeKind::Project {
                    input: ProgramNodeId::new(0),
                    is_subordinate: false,
                    validate_final_result_input: false,
                    exprs: definitions
                        .iter()
                        .map(|id| crate::ProgramExprId::new(*id))
                        .collect(),
                    expr_slot_ids: layout.slots().to_vec(),
                    expr_slot_schemas: None,
                    output_indices: None,
                },
                layout,
            ),
        ],
        ProgramNodeId::new(1),
        arena,
        profile,
        BindingRequirements::try_new(vec![]).unwrap(),
    )
    .unwrap()
}
fn root(expression: u32) -> ProgramExpressionRootSite {
    ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(1),
        role: ProgramNodeExpressionRole::ProjectOutput { expression },
    }
}
fn scope(use_id: u32) -> ProgramCallSite {
    ProgramCallSite::Expression(ProgramUseRef {
        arena: ProgramExpressionArena::Main,
        use_id: ExpressionUseId::new(use_id),
    })
}
fn snapshot(
    program: LocalProgramGraph,
    uses: Vec<ProgramExpressionUse>,
    roots: Vec<(ProgramExpressionRootSite, u32)>,
    domains: Vec<ProgramEvaluationDomain>,
) -> ProgramRootControlBindings {
    let flow = ProgramControlFlow::try_new(
        domains,
        uses,
        program.expressions().nodes().len(),
        &Control::default(),
    )
    .unwrap();
    ProgramRootControlBindings::try_new(
        program,
        BTreeMap::from([(ProgramExpressionArena::Main, flow)]),
        roots
            .into_iter()
            .map(|(site, id)| ProgramRootUseBinding {
                site,
                use_id: ExpressionUseId::new(id),
            })
            .collect(),
        &Control::default(),
    )
    .unwrap()
}
fn root_domain() -> ProgramEvaluationDomain {
    ProgramEvaluationDomain {
        id: DOMAIN,
        parent: None,
        guard: None,
    }
}
fn scalar_snapshot() -> ProgramRootControlBindings {
    snapshot(
        project_program(scalar_arena(), &[2]),
        vec![
            invocation(0, 2, ControlShape::Eager, &[1, 2]),
            invocation(1, 0, ControlShape::Eager, &[]),
            invocation(2, 1, ControlShape::Eager, &[]),
        ],
        vec![(root(0), 0)],
        vec![root_domain()],
    )
}
fn scalar_token(catalog: &FixtureCatalog, owner: &Owner, use_id: u32) -> PureCallSpecialization {
    token(
        catalog,
        owner,
        SCALAR,
        context(use_id, DOMAIN, EvaluationDemand::Value),
        &[Some(ExpressionUseId::new(1)), Some(ExpressionUseId::new(2))],
        None,
        false,
    )
}

#[test]
fn frozen_token_keeps_same_snapshot_owner_contract_effects_and_real_scalar_cpu() {
    let (catalog, owner, _) = catalogue();
    let snapshot = scalar_snapshot();
    let expected_arena = snapshot.program().expressions().clone();
    let call = scalar_token(&catalog, &owner, 0);
    let expected = call.clone();
    let resolved =
        ProgramResolvedCalls::try_new(snapshot, vec![(scope(0), call)], &Control::default())
            .unwrap();
    assert!(Arc::ptr_eq(
        resolved.snapshot().program().expressions(),
        &expected_arena
    ));
    let entry = &resolved.calls()[&scope(0)];
    assert_eq!(entry.implementation(), expected.implementation());
    assert_eq!(entry.effects(), expected.effects());
    assert!(
        entry.call_contract().selected().result_type == FunctionResultType::Scalar(result_type())
    );
    let ProgramStateTemplate::Scalar {
        scope: actual_scope,
        kernel,
    } = entry.state_template()
    else {
        panic!("expected scalar scope")
    };
    assert_eq!(actual_scope.root, root(0));
    assert_eq!(actual_scope.occurrence.use_id, ExpressionUseId::new(0));
    assert_eq!(owner.instances.load(Ordering::Relaxed), 0);
    let mut instance = ScalarEvaluationInstance::instantiate(kernel.clone()).unwrap();
    let left: ArrayRef = Arc::new(Int64Array::from(vec![4, 1]));
    let right: ArrayRef = Arc::new(Int64Array::from(vec![2, 2]));
    let selection = Selection::all(2);
    let arguments = [
        EvaluatedArgument::Column(&left),
        EvaluatedArgument::Column(&right),
    ];
    let output = instance.evaluate(selection, &arguments, &Runtime).unwrap();
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap(),
        &BooleanArray::from(vec![true, false])
    );
    assert_eq!(owner.instances.load(Ordering::Relaxed), 1);
}

#[test]
fn frozen_only_missing_duplicate_and_foreign_sites_fail_without_instantiation() {
    let (catalog, owner, _) = catalogue();
    let fresh = token(
        &catalog,
        &owner,
        SCALAR,
        context(0, DOMAIN, EvaluationDemand::Value),
        &[Some(ExpressionUseId::new(1)), Some(ExpressionUseId::new(2))],
        None,
        true,
    );
    assert_eq!(
        ProgramResolvedCalls::try_new(
            scalar_snapshot(),
            vec![(scope(0), fresh)],
            &Control::default()
        )
        .unwrap_err(),
        ProgramResolvedCallsError::WrongSource
    );
    assert_eq!(
        ProgramResolvedCalls::try_new(scalar_snapshot(), vec![], &Control::default()).unwrap_err(),
        ProgramResolvedCallsError::MissingSite(scope(0))
    );
    let call = scalar_token(&catalog, &owner, 0);
    assert_eq!(
        ProgramResolvedCalls::try_new(
            scalar_snapshot(),
            vec![(scope(0), call.clone()), (scope(0), call.clone())],
            &Control::default()
        )
        .unwrap_err(),
        ProgramResolvedCallsError::DuplicateSite
    );
    assert_eq!(
        ProgramResolvedCalls::try_new(
            scalar_snapshot(),
            vec![(scope(0), call.clone()), (scope(u32::MAX), call)],
            &Control::default()
        )
        .unwrap_err(),
        ProgramResolvedCallsError::InvalidSite
    );
    assert_eq!(owner.instances.load(Ordering::Relaxed), 0);
}

#[test]
fn changed_actual_order_and_occurrence_context_cannot_reuse_token_as_evidence() {
    let (catalog, owner, _) = catalogue();
    let wrong_order = snapshot(
        project_program(scalar_arena(), &[2]),
        vec![
            invocation(0, 2, ControlShape::Eager, &[1, 2]),
            invocation(1, 1, ControlShape::Eager, &[]),
            invocation(2, 0, ControlShape::Eager, &[]),
        ],
        vec![(root(0), 0)],
        vec![root_domain()],
    );
    assert_eq!(
        ProgramResolvedCalls::try_new(
            wrong_order,
            vec![(scope(0), scalar_token(&catalog, &owner, 0))],
            &Control::default()
        )
        .unwrap_err(),
        ProgramResolvedCallsError::WrongArguments
    );
    let wrong_use = scalar_token(&catalog, &owner, u32::MAX);
    assert_eq!(
        ProgramResolvedCalls::try_new(
            scalar_snapshot(),
            vec![(scope(0), wrong_use)],
            &Control::default()
        )
        .unwrap_err(),
        ProgramResolvedCallsError::WrongContext
    );
    let wrong_domain = token(
        &catalog,
        &owner,
        SCALAR,
        context(0, EvaluationDomainId::new(8), EvaluationDemand::Value),
        &[Some(ExpressionUseId::new(1)), Some(ExpressionUseId::new(2))],
        None,
        false,
    );
    assert_eq!(
        ProgramResolvedCalls::try_new(
            scalar_snapshot(),
            vec![(scope(0), wrong_domain)],
            &Control::default()
        )
        .unwrap_err(),
        ProgramResolvedCallsError::WrongContext
    );
    let wrong_demand = token(
        &catalog,
        &owner,
        SCALAR,
        context(0, DOMAIN, EvaluationDemand::TruthOnly),
        &[Some(ExpressionUseId::new(1)), Some(ExpressionUseId::new(2))],
        None,
        false,
    );
    assert_eq!(
        ProgramResolvedCalls::try_new(
            scalar_snapshot(),
            vec![(scope(0), wrong_demand)],
            &Control::default()
        )
        .unwrap_err(),
        ProgramResolvedCallsError::WrongContext
    );
}

#[test]
fn shared_definition_keeps_distinct_sparse_occurrences_and_host_root_scopes() {
    let (catalog, owner, _) = catalogue();
    let snapshot = snapshot(
        project_program(scalar_arena(), &[2, 2]),
        vec![
            invocation(0, 2, ControlShape::Eager, &[1, 2]),
            invocation(1, 0, ControlShape::Eager, &[]),
            invocation(2, 1, ControlShape::Eager, &[]),
            invocation(u32::MAX, 2, ControlShape::Eager, &[3, 4]),
            invocation(3, 0, ControlShape::Eager, &[]),
            invocation(4, 1, ControlShape::Eager, &[]),
        ],
        vec![(root(0), 0), (root(1), u32::MAX)],
        vec![root_domain()],
    );
    let left = scalar_token(&catalog, &owner, 0);
    let right = token(
        &catalog,
        &owner,
        SCALAR,
        context(u32::MAX, DOMAIN, EvaluationDemand::Value),
        &[Some(ExpressionUseId::new(3)), Some(ExpressionUseId::new(4))],
        None,
        false,
    );
    let resolved = ProgramResolvedCalls::try_new(
        snapshot.clone(),
        vec![(scope(0), left.clone()), (scope(u32::MAX), right.clone())],
        &Control::default(),
    )
    .unwrap();
    assert_eq!(resolved.calls().len(), 2);
    let ProgramStateTemplate::Scalar {
        scope: left_scope, ..
    } = resolved.calls()[&scope(0)].state_template()
    else {
        unreachable!()
    };
    let ProgramStateTemplate::Scalar {
        scope: right_scope, ..
    } = resolved.calls()[&scope(u32::MAX)].state_template()
    else {
        unreachable!()
    };
    assert_eq!(left_scope.root, root(0));
    assert_eq!(right_scope.root, root(1));
    assert_eq!(
        ProgramResolvedCalls::try_new(
            snapshot,
            vec![(scope(0), right), (scope(u32::MAX), left)],
            &Control::default()
        )
        .unwrap_err(),
        ProgramResolvedCallsError::WrongContext
    );
}

#[test]
fn type_only_does_not_require_dead_child_call_attachments_and_cannot_be_eager() {
    let (catalog, owner, _) = catalogue();
    let arena = arena(vec![
        (
            StaticExprKind::Literal(StaticLiteral::Int64(1)),
            DataType::Int64,
        ),
        (
            StaticExprKind::FunctionCall {
                kind: StaticFunctionKind::Math("legacy_dead"),
                args: vec![crate::ProgramExprId::new(0)],
            },
            DataType::Int64,
        ),
        (
            StaticExprKind::FunctionCall {
                kind: StaticFunctionKind::Abs,
                args: vec![crate::ProgramExprId::new(0), crate::ProgramExprId::new(1)],
            },
            DataType::Boolean,
        ),
    ]);
    let snapshot = snapshot(
        project_program(arena, &[2]),
        vec![invocation(0, 2, ControlShape::TypeOnly, &[])],
        vec![(root(0), 0)],
        vec![root_domain()],
    );
    let call = token(
        &catalog,
        &owner,
        TYPE_ONLY,
        context(0, DOMAIN, EvaluationDemand::Value),
        &[None, None],
        None,
        false,
    );
    let resolved = ProgramResolvedCalls::try_new(
        snapshot.clone(),
        vec![(scope(0), call)],
        &Control::default(),
    )
    .unwrap();
    assert_eq!(resolved.calls().len(), 1);
    assert_eq!(
        ProgramResolvedCalls::try_new(
            snapshot,
            vec![(scope(0), scalar_token(&catalog, &owner, 0))],
            &Control::default()
        )
        .unwrap_err(),
        ProgramResolvedCallsError::WrongControl
    );
}

fn higher_snapshot() -> ProgramRootControlBindings {
    higher_snapshot_parameters(1)
}
fn higher_snapshot_parameters(count: usize) -> ProgramRootControlBindings {
    higher_snapshot_locals(count, false, false)
}
fn higher_snapshot_locals(
    count: usize,
    has_common: bool,
    wrong_order: bool,
) -> ProgramRootControlBindings {
    let mut nodes = vec![
        (
            StaticExprKind::Literal(StaticLiteral::Int64(2)),
            DataType::Int64,
        ),
        (
            StaticExprKind::Literal(StaticLiteral::Bool(true)),
            DataType::Boolean,
        ),
        (
            StaticExprKind::LambdaFunction {
                body: crate::ProgramExprId::new(1),
                arg_slots: (0..count).map(|i| SlotId::new(99 + i as u32)).collect(),
                common_sub_exprs: if has_common {
                    vec![(SlotId::new(200), crate::ProgramExprId::new(2))]
                } else {
                    vec![]
                },
                is_nondeterministic: false,
            },
            DataType::Boolean,
        ),
        (
            StaticExprKind::FunctionCall {
                kind: StaticFunctionKind::ArrayMap,
                args: vec![crate::ProgramExprId::new(0), crate::ProgramExprId::new(2)],
            },
            DataType::Boolean,
        ),
    ];
    if has_common {
        nodes.insert(
            2,
            (
                StaticExprKind::FunctionCall {
                    kind: StaticFunctionKind::Abs,
                    args: vec![crate::ProgramExprId::new(0), crate::ProgramExprId::new(0)],
                },
                DataType::Boolean,
            ),
        );
        let StaticExprKind::FunctionCall { args, .. } = &mut nodes[4].0 else {
            unreachable!()
        };
        args[1] = crate::ProgramExprId::new(3);
    }
    let arena = arena(nodes);
    let edge_uses: &[u32] = if has_common {
        if wrong_order { &[3, 4] } else { &[4, 3] }
    } else {
        &[3]
    };
    let mut edge = invocation(
        2,
        if has_common { 3 } else { 2 },
        ControlShape::LambdaBody,
        edge_uses,
    );
    edge.context.domain = EvaluationDomainId::new(8);
    let mut body = invocation(3, 1, ControlShape::Eager, &[]);
    body.context.domain = EvaluationDomainId::new(8);
    let mut uses = vec![
        invocation(
            0,
            if has_common { 4 } else { 3 },
            ControlShape::HigherOrder {
                body_ordinal: 1,
                body_demand: EvaluationDemand::Value,
            },
            &[1, 2],
        ),
        invocation(1, 0, ControlShape::Eager, &[]),
        edge,
        body,
    ];
    if has_common {
        for mut child in [
            invocation(4, 2, ControlShape::Eager, &[5, 6]),
            invocation(5, 0, ControlShape::Eager, &[]),
            invocation(6, 0, ControlShape::Eager, &[]),
        ] {
            child.context.domain = EvaluationDomainId::new(8);
            uses.push(child);
        }
    }
    snapshot(
        project_program(arena, &[if has_common { 4 } else { 3 }]),
        uses,
        vec![(root(0), 0)],
        vec![
            root_domain(),
            ProgramEvaluationDomain {
                id: EvaluationDomainId::new(8),
                parent: Some(DOMAIN),
                guard: Some(DomainGuard {
                    owner: ExpressionUseId::new(0),
                    kind: GuardKind::LambdaInvocation,
                }),
            },
        ],
    )
}
#[test]
fn lambda_local_calls_require_exact_frozen_tokens_and_order_before_body() {
    let (catalog, owner, _) = catalogue();
    let higher = token(
        &catalog,
        &owner,
        HIGHER,
        context(0, DOMAIN, EvaluationDemand::Value),
        &[Some(ExpressionUseId::new(1)), Some(ExpressionUseId::new(2))],
        Some(context(
            3,
            EvaluationDomainId::new(8),
            EvaluationDemand::Value,
        )),
        false,
    );
    let common = token(
        &catalog,
        &owner,
        SCALAR,
        context(4, EvaluationDomainId::new(8), EvaluationDemand::Value),
        &[Some(ExpressionUseId::new(5)), Some(ExpressionUseId::new(6))],
        None,
        false,
    );
    let resolved = ProgramResolvedCalls::try_new(
        higher_snapshot_locals(1, true, false),
        vec![(scope(0), higher.clone()), (scope(4), common.clone())],
        &Control::default(),
    )
    .unwrap();
    assert_eq!(resolved.calls().len(), 2);
    assert_eq!(
        resolved.calls()[&scope(4)].call_contract().context().domain,
        EvaluationDomainId::new(8)
    );
    assert_eq!(
        ProgramResolvedCalls::try_new(
            higher_snapshot_locals(1, true, false),
            vec![(scope(0), higher.clone())],
            &Control::default(),
        )
        .unwrap_err(),
        ProgramResolvedCallsError::MissingSite(scope(4)),
    );
    // Ordered intrinsic children are now checked before any call token is
    // attached. Keep the actual sources/guards and change only edge order.
    let snapshot = higher_snapshot_locals(1, true, false);
    let flow = &snapshot.flows()[&ProgramExpressionArena::Main];
    let mut uses = flow.uses().values().cloned().collect::<Vec<_>>();
    uses.iter_mut()
        .find(|invocation| invocation.context.use_id == ExpressionUseId::new(2))
        .unwrap()
        .arguments
        .reverse();
    let wrong = ProgramControlFlow::try_new(
        flow.domains().values().copied().collect(),
        uses,
        snapshot.program().expressions().nodes().len(),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        ProgramRootControlBindings::try_new(
            snapshot.program().clone(),
            BTreeMap::from([(ProgramExpressionArena::Main, wrong)]),
            snapshot
                .bindings()
                .iter()
                .map(|(site, use_id)| ProgramRootUseBinding {
                    site: *site,
                    use_id: *use_id
                })
                .collect(),
            &Control::default(),
        )
        .unwrap_err(),
        crate::ProgramRootBindingError::WrongArguments,
    );
    assert_eq!(owner.instances.load(Ordering::Relaxed), 0);
}

#[test]
fn higher_order_keeps_actual_body_edge_and_inner_root_but_does_not_claim_capture_closure() {
    let (catalog, owner, _) = catalogue();
    let body = context(3, EvaluationDomainId::new(8), EvaluationDemand::Value);
    let prepare = |body| {
        token(
            &catalog,
            &owner,
            HIGHER,
            context(0, DOMAIN, EvaluationDemand::Value),
            &[Some(ExpressionUseId::new(1)), Some(ExpressionUseId::new(2))],
            Some(body),
            false,
        )
    };
    let resolved = ProgramResolvedCalls::try_new(
        higher_snapshot(),
        vec![(scope(0), prepare(body))],
        &Control::default(),
    )
    .unwrap();
    let ProgramStateTemplate::HigherOrder {
        scope: actual,
        kernel,
    } = resolved.calls()[&scope(0)].state_template()
    else {
        unreachable!()
    };
    assert_eq!(actual.root, root(0));
    assert_eq!(kernel.body_contract().context(), body);
    let wrong = context(
        u32::MAX,
        EvaluationDomainId::new(8),
        EvaluationDemand::Value,
    );
    assert_eq!(
        ProgramResolvedCalls::try_new(
            higher_snapshot(),
            vec![(scope(0), prepare(wrong))],
            &Control::default()
        )
        .unwrap_err(),
        ProgramResolvedCallsError::WrongBody
    );
    assert_eq!(owner.instances.load(Ordering::Relaxed), 0);
}

#[test]
fn intrinsic_controller_attachment_retains_guards_without_scalar_instance() {
    let (catalog, _, owner) = catalogue();
    let arena = arena(vec![
        (
            StaticExprKind::Literal(StaticLiteral::Bool(true)),
            DataType::Boolean,
        ),
        (
            StaticExprKind::Literal(StaticLiteral::Bool(false)),
            DataType::Boolean,
        ),
        (
            StaticExprKind::FunctionCall {
                kind: StaticFunctionKind::If,
                args: vec![
                    crate::ProgramExprId::new(0),
                    crate::ProgramExprId::new(0),
                    crate::ProgramExprId::new(1),
                ],
            },
            DataType::Boolean,
        ),
    ]);
    let mut condition = invocation(1, 0, ControlShape::Eager, &[]);
    condition.context.demand = EvaluationDemand::TruthOnly;
    let mut then = invocation(2, 0, ControlShape::Eager, &[]);
    then.context.domain = EvaluationDomainId::new(8);
    let mut otherwise = invocation(3, 1, ControlShape::Eager, &[]);
    otherwise.context.domain = EvaluationDomainId::new(9);
    let domains = vec![
        root_domain(),
        ProgramEvaluationDomain {
            id: EvaluationDomainId::new(8),
            parent: Some(DOMAIN),
            guard: Some(DomainGuard {
                owner: ExpressionUseId::new(0),
                kind: GuardKind::IfThen,
            }),
        },
        ProgramEvaluationDomain {
            id: EvaluationDomainId::new(9),
            parent: Some(DOMAIN),
            guard: Some(DomainGuard {
                owner: ExpressionUseId::new(0),
                kind: GuardKind::IfElse,
            }),
        },
    ];
    let snapshot = snapshot(
        project_program(arena, &[2]),
        vec![
            invocation(0, 2, ControlShape::If, &[1, 2, 3]),
            condition,
            then,
            otherwise,
        ],
        vec![(root(0), 0)],
        domains,
    );
    let call = token(
        &catalog,
        &owner,
        IF,
        context(0, DOMAIN, EvaluationDemand::Value),
        &[
            Some(ExpressionUseId::new(1)),
            Some(ExpressionUseId::new(2)),
            Some(ExpressionUseId::new(3)),
        ],
        None,
        false,
    );
    let resolved =
        ProgramResolvedCalls::try_new(snapshot, vec![(scope(0), call)], &Control::default())
            .unwrap();
    assert!(matches!(
        resolved.calls()[&scope(0)].state_template(),
        ProgramStateTemplate::ControlIntrinsic { .. }
    ));
    assert_eq!(
        resolved.snapshot().flows()[&ProgramExpressionArena::Main]
            .domains()
            .len(),
        3
    );
    assert_eq!(owner.instances.load(Ordering::Relaxed), 0);
}

#[test]
fn entry_and_midwork_failures_are_typed_and_declaration_count_is_bounded() {
    let (catalog, owner, _) = catalogue();
    let call = scalar_token(&catalog, &owner, 0);
    let mut uses = Vec::new();
    let mut roots = Vec::new();
    let mut calls = Vec::new();
    for i in 0..100u32 {
        let id = i * 3;
        uses.push(invocation(id, 2, ControlShape::Eager, &[id + 1, id + 2]));
        uses.push(invocation(id + 1, 0, ControlShape::Eager, &[]));
        uses.push(invocation(id + 2, 1, ControlShape::Eager, &[]));
        roots.push((root(i), id));
        calls.push((
            scope(id),
            token(
                &catalog,
                &owner,
                SCALAR,
                context(id, DOMAIN, EvaluationDemand::Value),
                &[
                    Some(ExpressionUseId::new(id + 1)),
                    Some(ExpressionUseId::new(id + 2)),
                ],
                None,
                false,
            ),
        ));
    }
    let snapshot = snapshot(
        project_program(scalar_arena(), &vec![2; 100]),
        uses,
        roots,
        vec![root_domain()],
    );
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in [0, 256] {
            let control = Control {
                failure: Some((at, failure.clone())),
                ..Default::default()
            };
            assert_eq!(
                ProgramResolvedCalls::try_new(snapshot.clone(), calls.clone(), &control)
                    .unwrap_err(),
                ProgramResolvedCallsError::Control(failure.clone())
            );
            assert_eq!(
                control.work.lock().unwrap().last(),
                Some(&(CompilePhase::LowerProgram, at))
            );
        }
    }
    assert_eq!(
        ProgramResolvedCalls::try_new(
            scalar_snapshot(),
            vec![(scope(0), call); MAX_CONTROL_USE_REFERENCES + 1],
            &Control::default()
        )
        .unwrap_err(),
        ProgramResolvedCallsError::TooManyItems
    );
    assert_eq!(owner.instances.load(Ordering::Relaxed), 0);
}

/// Real frozen catalogue fixtures shared only by the complete-type tests.
pub(crate) enum TypedCallFixture {
    Scalar,
    TypeOnly,
    HigherOrder,
    HigherOrderTwoParameters,
    TruthOnly,
    TypedTypeOnly([FunctionValueType; 2]),
}
pub(crate) fn typed_call_fixture(kind: TypedCallFixture) -> ProgramResolvedCalls {
    match kind {
        TypedCallFixture::Scalar => {
            let (catalog, owner, _) = catalogue();
            ProgramResolvedCalls::try_new(
                scalar_snapshot(),
                vec![(scope(0), scalar_token(&catalog, &owner, 0))],
                &Control::default(),
            )
            .unwrap()
        }
        TypedCallFixture::HigherOrder | TypedCallFixture::HigherOrderTwoParameters => {
            let count = if matches!(kind, TypedCallFixture::HigherOrderTwoParameters) {
                2
            } else {
                1
            };
            let mut owner = Owner::new(false);
            if count == 2 {
                owner.explicit_higher_parameters = Some(Box::from([input_type(), result_type()]));
            }
            let (catalog, owner, _) = catalogue_with_owner(Arc::new(owner));
            let call = token(
                &catalog,
                &owner,
                HIGHER,
                context(0, DOMAIN, EvaluationDemand::Value),
                &[Some(ExpressionUseId::new(1)), Some(ExpressionUseId::new(2))],
                Some(context(
                    3,
                    EvaluationDomainId::new(8),
                    EvaluationDemand::Value,
                )),
                false,
            );
            ProgramResolvedCalls::try_new(
                higher_snapshot_parameters(count),
                vec![(scope(0), call)],
                &Control::default(),
            )
            .unwrap()
        }
        TypedCallFixture::TypeOnly
        | TypedCallFixture::TypedTypeOnly(_)
        | TypedCallFixture::TruthOnly => {
            let (types, truth) = match kind {
                TypedCallFixture::TypedTypeOnly(types) => (types, false),
                TypedCallFixture::TruthOnly => ([input_type(), input_type()], true),
                _ => ([input_type(), input_type()], false),
            };
            let mut owner = Owner::new(false);
            owner.explicit_type_only = Some(types.clone());
            let (catalog, owner, _) = catalogue_with_owner(Arc::new(owner));
            let definitions = arena(vec![
                (
                    StaticExprKind::SlotId(SlotId::new(1)),
                    types[0].data_type().clone(),
                ),
                (
                    StaticExprKind::SlotId(SlotId::new(2)),
                    types[1].data_type().clone(),
                ),
                (
                    StaticExprKind::FunctionCall {
                        kind: StaticFunctionKind::Abs,
                        args: vec![crate::ProgramExprId::new(0), crate::ProgramExprId::new(1)],
                    },
                    DataType::Boolean,
                ),
            ]);
            let source_schema = Arc::new(Schema::new(
                types
                    .iter()
                    .enumerate()
                    .map(|(i, ty)| {
                        Field::new(format!("typed{i}"), ty.data_type().clone(), ty.nullable())
                    })
                    .collect::<Vec<_>>(),
            ));
            // NULL source values are unused by TypeOnly. The exact source
            // schema and selected types, not their payload, bind these channels.
            let source_layout = StaticLayout::try_new(
                source_schema.clone(),
                Arc::from([SlotId::new(1), SlotId::new(2)]),
            )
            .unwrap();
            let values = StaticValues::try_new(
                RecordBatch::try_new(
                    source_schema,
                    types
                        .iter()
                        .map(|ty| arrow_array::new_null_array(ty.data_type(), 0))
                        .collect(),
                )
                .unwrap(),
                source_layout.clone(),
            )
            .unwrap();
            let output_layout = if truth {
                source_layout.clone()
            } else {
                StaticLayout::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        "output",
                        DataType::Boolean,
                        true,
                    )])),
                    Arc::from([SlotId::new(10)]),
                )
                .unwrap()
            };
            let output_kind = if truth {
                ProgramNodeKind::Filter {
                    input: ProgramNodeId::new(0),
                    predicates: vec![crate::ProgramExprId::new(2)].into_boxed_slice(),
                }
            } else {
                ProgramNodeKind::Project {
                    input: ProgramNodeId::new(0),
                    is_subordinate: false,
                    validate_final_result_input: false,
                    exprs: vec![crate::ProgramExprId::new(2)],
                    expr_slot_ids: vec![SlotId::new(10)],
                    expr_slot_schemas: None,
                    output_indices: None,
                }
            };
            let profile = CompileProfile::new(
                NonZeroUsize::new(1).unwrap(),
                None,
                output_layout.identity().unwrap(),
                KernelAbiVersion::CURRENT,
            );
            let program = LocalProgramGraph::try_new(
                vec![
                    ProgramNode::new(10, ProgramNodeKind::Values { values }, source_layout),
                    ProgramNode::new(20, output_kind, output_layout),
                ],
                ProgramNodeId::new(1),
                definitions,
                profile,
                BindingRequirements::try_new(vec![]).unwrap(),
            )
            .unwrap();
            let demand = if truth {
                EvaluationDemand::TruthOnly
            } else {
                EvaluationDemand::Value
            };
            let mut use_ = invocation(0, 2, ControlShape::TypeOnly, &[]);
            use_.context.demand = demand;
            let site = if truth {
                ProgramExpressionRootSite::Node {
                    node: ProgramNodeId::new(1),
                    role: ProgramNodeExpressionRole::FilterPredicate { predicate: 0 },
                }
            } else {
                root(0)
            };
            let snapshot = snapshot(program, vec![use_], vec![(site, 0)], vec![root_domain()]);
            let call = token(
                &catalog,
                &owner,
                TYPE_ONLY,
                context(0, DOMAIN, demand),
                &[None, None],
                None,
                false,
            );
            ProgramResolvedCalls::try_new(snapshot, vec![(scope(0), call)], &Control::default())
                .unwrap()
        }
    }
}

#[test]
fn created_call_table_drops_scratch_and_keeps_finite_final_hold() {
    struct Hold;
    impl novarocks_type_contract::ValueTypeStorageHold for Hold {}
    let (catalog, owner, _) = catalogue();
    let snapshot = scalar_snapshot();
    let token = scalar_token(&catalog, &owner, 0);
    let expected = token.clone();
    let temporary_hold = Arc::new(Hold);
    let retained_hold = Arc::new(Hold);
    let temporary_weak = Arc::downgrade(&temporary_hold);
    let retained_weak = Arc::downgrade(&retained_hold);
    let temporary_requests = AtomicUsize::new(0);
    let retained_requests = AtomicUsize::new(0);
    let mut request = |_: std::alloc::Layout| {
        temporary_requests.fetch_add(1, Ordering::Relaxed);
        Ok::<_, ()>(())
    };
    let mut opaque = |_: usize| {
        temporary_requests.fetch_add(1, Ordering::Relaxed);
        Ok::<_, ()>(())
    };
    let mut retained_request = |_: std::alloc::Layout| {
        retained_requests.fetch_add(1, Ordering::Relaxed);
        Ok::<_, ()>(())
    };
    let mut retained_opaque = |_: usize| {
        retained_requests.fetch_add(1, Ordering::Relaxed);
        Ok::<_, ()>(())
    };
    let mut temporary = ValueSchemaCreation::new(
        ValueTypeStorageLifetime::held(temporary_hold.clone()),
        &mut request,
        &mut opaque,
    );
    let mut retained = ValueSchemaCreation::new(
        ValueTypeStorageLifetime::held(retained_hold.clone()),
        &mut retained_request,
        &mut retained_opaque,
    );
    let resolved = ProgramResolvedCalls::try_new_with_creator_in(
        snapshot,
        vec![(scope(0), token)],
        &mut temporary,
        &mut retained,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(
        resolved.calls()[&scope(0)].implementation(),
        expected.implementation()
    );
    assert_eq!(resolved.calls()[&scope(0)].effects(), expected.effects());
    assert!(temporary_requests.load(Ordering::Relaxed) > 0);
    assert!(retained_requests.load(Ordering::Relaxed) > 0);
    drop(temporary);
    drop(retained);
    drop(temporary_hold);
    drop(retained_hold);
    assert!(temporary_weak.upgrade().is_none());
    assert!(retained_weak.upgrade().is_some());
    drop(resolved);
    assert!(retained_weak.upgrade().is_none());
}

#[test]
fn created_call_table_each_real_request_preserves_host_and_has_no_tail() {
    let (catalog, owner, _) = catalogue();
    let source = scalar_snapshot();
    let token = scalar_token(&catalog, &owner, 0);
    let run = |fail_at: Option<usize>| {
        let control = Control::default();
        let next = AtomicUsize::new(0);
        let refused = AtomicUsize::new(usize::MAX);
        let cause = Arc::new(());
        let observe = || {
            let at = next.fetch_add(1, Ordering::Relaxed);
            if Some(at) == fail_at {
                refused.store(control.work.lock().unwrap().len(), Ordering::Relaxed);
                Err(cause.clone())
            } else {
                Ok(())
            }
        };
        let mut request = |_: std::alloc::Layout| observe();
        let mut opaque = |_: usize| observe();
        let mut retained_request = |_: std::alloc::Layout| observe();
        let mut retained_opaque = |_: usize| observe();
        let mut temporary =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut opaque);
        let mut retained = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut retained_request,
            &mut retained_opaque,
        );
        let result = ProgramResolvedCalls::try_new_with_creator_in(
            source.clone(),
            vec![(scope(0), token.clone())],
            &mut temporary,
            &mut retained,
            &control,
        );
        if let Some(at) = fail_at {
            assert!(
                matches!(result, Err(ProgramCreationFailure::Creator(actual))
                if Arc::ptr_eq(&actual, &cause))
            );
            assert_eq!(next.load(Ordering::Relaxed), at + 1);
            assert_eq!(
                control.work.lock().unwrap().len(),
                refused.load(Ordering::Relaxed)
            );
        } else {
            assert!(result.is_ok());
        }
        next.load(Ordering::Relaxed)
    };
    let total = run(None);
    assert!(
        total > 2,
        "original scratch and final tree/Arc are actual constructors"
    );
    for at in 0..total {
        run(Some(at));
    }
}

#[test]
fn created_call_table_every_control_boundary_keeps_first_cause_and_no_tail() {
    struct Compile<'a> {
        next: AtomicUsize,
        fail: Option<(usize, CompileControlError)>,
        requests: &'a AtomicUsize,
        refused_requests: AtomicUsize,
    }
    impl PureCompileControl for Compile<'_> {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            let at = self.next.fetch_add(1, Ordering::Relaxed);
            if let Some((boundary, error)) = self.fail.clone()
                && at == boundary
            {
                self.refused_requests
                    .store(self.requests.load(Ordering::Relaxed), Ordering::Relaxed);
                Err(error)
            } else {
                Ok(())
            }
        }
    }
    let (catalog, owner, _) = catalogue();
    let source = scalar_snapshot();
    let token = scalar_token(&catalog, &owner, 0);
    let run = |fail: Option<(usize, CompileControlError)>| {
        let requests = AtomicUsize::new(0);
        let control = Compile {
            next: AtomicUsize::new(0),
            fail: fail.clone(),
            requests: &requests,
            refused_requests: AtomicUsize::new(usize::MAX),
        };
        let observe = || {
            requests.fetch_add(1, Ordering::Relaxed);
            Ok::<_, ()>(())
        };
        let mut request = |_: std::alloc::Layout| observe();
        let mut opaque = |_: usize| observe();
        let mut retained_request = |_: std::alloc::Layout| observe();
        let mut retained_opaque = |_: usize| observe();
        let mut temporary =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut opaque);
        let mut retained = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut retained_request,
            &mut retained_opaque,
        );
        let result = ProgramResolvedCalls::try_new_with_creator_in(
            source.clone(),
            vec![(scope(0), token.clone())],
            &mut temporary,
            &mut retained,
            &control,
        );
        if let Some((at, cause)) = fail {
            assert!(
                matches!(result, Err(ProgramCreationFailure::Control(actual)) if actual == cause)
            );
            assert_eq!(control.next.load(Ordering::Relaxed), at + 1);
            assert_eq!(
                requests.load(Ordering::Relaxed),
                control.refused_requests.load(Ordering::Relaxed)
            );
        } else {
            assert!(result.is_ok());
        }
        control.next.load(Ordering::Relaxed)
    };
    let total = run(None);
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 0..total {
            run(Some((at, cause.clone())));
        }
    }
}

#[test]
fn canonical_map64_carrier_consistency_keeps_type_and_control_refusals() {
    let map = |mut data_type: DataType| {
        for _ in 1..64 {
            data_type = DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(Field::new("key", DataType::Int32, false)),
                            Arc::new(Field::new("value", data_type, false)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            );
        }
        data_type
    };
    let left = map(DataType::Utf8);
    let right = map(DataType::LargeUtf8);
    let good = Control::default();
    let mut work = CompileCheckpoints::try_new(&good, CompilePhase::LowerProgram).unwrap();
    check_arrow(&left, &left, &mut work).unwrap();
    assert_eq!(
        check_arrow(&left, &right, &mut work),
        Err(ProgramResolvedCallsError::TypeMismatch)
    );
    work.finish().unwrap();
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Control {
            failure: Some((256, cause.clone())),
            ..Control::default()
        };
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
        assert_eq!(
            check_arrow(&left, &left, &mut work),
            Err(ProgramResolvedCallsError::Control(cause))
        );
    }
}
