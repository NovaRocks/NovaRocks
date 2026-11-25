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

//! Binding and hosted execution of the Trino array relation overload.

use std::sync::Arc;

use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompilePhase,
    FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior, ObservableEffects,
    PureCompileControl,
};

use super::array_relation::{Operation, PreparedRelation};
use super::catalogue::{BuiltinDynamicScalarResolver, BuiltinScalarResolver};
use crate::AggregateStateAllocator;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_control::{compile_failure, invalid};
use crate::{
    CallEffectInput, FunctionBindingDeclaration, FunctionBindingError, FunctionBindingRequest,
    FunctionBindingResolver, FunctionBindingSelection, FunctionCatalogError, FunctionDefinition,
    FunctionEffectOwner, FunctionEffectOwnerError, FunctionFailureBehavior, FunctionId,
    FunctionIntrinsicRowError, FunctionKind, FunctionVisibility, FunctionVolatility,
    KernelEvaluationControl, KernelFailure, PreparedScalarKernel, PureFunctionMetadataOwner,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi, PureScalarImplementation,
    ScalarCallContract, ScalarCallInput, ScalarKernelInstance, SelectedValues,
};

pub(super) fn operation(name: &str) -> Option<Operation> {
    Some(match name {
        "contains" => Operation::Contains,
        "array_position" => Operation::Position,
        "array_remove" => Operation::Remove,
        "contains_sequence" => Operation::ContainsSequence,
        "arrays_overlap" => Operation::Overlap,
        "array_distinct" => Operation::Distinct,
        "array_intersect" => Operation::Intersect,
        _ => return None,
    })
}

pub(super) fn effects(operation: Operation) -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: match operation.relation() {
            novarocks_type_contract::KeyRelation::Equal => FunctionIntrinsicRowError::MayRaise,
            novarocks_type_contract::KeyRelation::Identical => {
                FunctionIntrinsicRowError::NoRowError
            }
        },
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::Strict,
        argument_control: ArgumentControl::Eager,
        instance_state: FunctionInstanceState::None,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::new([]),
    }
}

pub(super) fn definition(
    name: &str,
    declaration: FunctionBindingDeclaration,
    resolver: BuiltinScalarResolver,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    create_definition(name, declaration, Resolver::Fixed(Arc::new(resolver)))
}

pub(super) fn dynamic_definition(
    name: &str,
    declaration: FunctionBindingDeclaration,
    resolver: BuiltinDynamicScalarResolver,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    create_definition(name, declaration, Resolver::Dynamic(Arc::new(resolver)))
}

fn create_definition(
    name: &str,
    declaration: FunctionBindingDeclaration,
    resolver: Resolver,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let owner = Arc::new(ArrayRelationOwner::new(name, declaration, resolver)?);
    FunctionDefinition::try_new_pure_scalar(name, FunctionVisibility::Public, owner).map_err(
        |error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin array relation pure owner",
            value: error.to_string().into(),
        },
    )
}

enum Resolver {
    Fixed(Arc<BuiltinScalarResolver>),
    Dynamic(Arc<BuiltinDynamicScalarResolver>),
}

impl Resolver {
    fn author(&self) -> &dyn FunctionBindingResolver {
        match self {
            Self::Fixed(resolver) => resolver.as_ref(),
            Self::Dynamic(resolver) => resolver.as_ref(),
        }
    }
}

struct ArrayRelationOwner {
    resolver: Resolver,
    operation: Operation,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
}
impl ArrayRelationOwner {
    fn new(
        name: &str,
        declaration: FunctionBindingDeclaration,
        resolver: Resolver,
    ) -> Result<Self, FunctionCatalogError> {
        let operation =
            operation(name).ok_or_else(|| FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin array relation name",
                value: name.into(),
            })?;
        let function = format!("builtin.scalar/{name}/v1");
        let expected = effects(operation);
        if declaration.function_id().as_str() != function
            || declaration.kind() != FunctionKind::Scalar
            || declaration.overloads().len() != 1
            || declaration.overloads().iter().any(|overload| {
                overload.aggregate.is_some() || overload.effects.as_ref() != Some(&expected)
            })
        {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin array relation pure declaration",
                value: declaration.function_id().as_str().into(),
            });
        }
        let implementation =
            PureImplementationId::try_new(format!("builtin.scalar/{name}/selected-v1"))?;
        let implementations = declaration
            .overloads()
            .iter()
            .map(|overload| PureImplementationDeclaration {
                overload: overload.identity.clone(),
                implementation: implementation.clone(),
                abi: PureKernelAbi::ScalarV1,
            })
            .collect();
        Ok(Self {
            resolver,
            operation,
            declaration,
            implementations,
        })
    }

    fn call_effects(&self, scope: CallProofScope) -> CallEffects {
        let base = effects(self.operation);
        CallEffects {
            value_stability: base.value_stability,
            own_row_error: base.own_row_error,
            failure_behavior: base.failure_behavior,
            null_behavior: base.null_behavior,
            argument_control: base.argument_control,
            instance_state: base.instance_state,
            observable_effects: base.observable_effects,
            environment: Box::new([]),
            proof_scope: scope,
        }
    }
}

impl FunctionBindingResolver for ArrayRelationOwner {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.resolver.author().resolve(request, control)
    }

    fn select_at_overload_observed(
        &self,
        overload: &crate::FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.resolver
            .author()
            .select_at_overload_observed(overload, request, control)
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        self.resolver
            .author()
            .validate_selected(selected, request, control)
    }
}

impl PureFunctionMetadataOwner for ArrayRelationOwner {
    fn binding_resolver_owner(self: Arc<Self>) -> crate::FunctionBindingResolverOwner {
        match &self.resolver {
            Resolver::Fixed(resolver) => {
                crate::FunctionBindingResolverOwner::fixed_scalar(Arc::clone(resolver))
            }
            Resolver::Dynamic(resolver) => {
                crate::FunctionBindingResolverOwner::dynamic_scalar(Arc::clone(resolver))
            }
        }
    }

    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }

    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
}

impl FunctionEffectOwner for ArrayRelationOwner {
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
        self.refine_selected_core(input, control, || {
            self.validate_selected(input.selected, input.request, control)
        })
    }
    fn validate_and_refine_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &crate::CheckedCallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, crate::CheckedOwnerFailure<FunctionEffectOwnerError<Self::Error>>>
    {
        self.refine_selected_core(input, control, || {
            checked.require_binding_author(self.resolver.author(), input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}

impl PureScalarImplementation for ArrayRelationOwner {
    fn prepare_scalar(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure> {
        self.prepare_selected_core(input, contract, control, || {
            self.validate_selected(input.selected, input.request, control)
        })
    }
    fn prepare_scalar_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &crate::CheckedCallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedScalarKernel>, crate::CheckedOwnerFailure<KernelFailure>> {
        self.prepare_selected_core(input, contract, control, || {
            checked.require_binding_author(self.resolver.author(), input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}

#[derive(Debug)]
struct PreparedArrayRelation {
    contract: Arc<ScalarCallContract>,
    relation: Arc<PreparedRelation>,
}
impl PreparedScalarKernel for PreparedArrayRelation {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }

    fn instance_retained_upper_bound(&self) -> usize {
        size_of::<ArrayRelationInstance>() + HostAggregateAllocator::metadata_allocation_bytes()
    }

    fn instance_inline_allocation_bytes(&self) -> usize {
        size_of::<ArrayRelationInstance>()
    }

    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        Err(invalid(
            "array relation requires its scalar allocation host",
        ))
    }

    fn create_instance_with_allocator(
        &self,
        host: Option<Arc<dyn AggregateStateAllocator>>,
    ) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        let host = host
            .filter(|host| host.opaque_allocation_host().is_some())
            .ok_or_else(|| invalid("array relation requires its scalar allocation host"))?;
        let allocator = HostAggregateAllocator::try_new(Arc::clone(&host))?;
        Ok(Box::new(ArrayRelationInstance {
            host,
            allocator,
            relation: Arc::clone(&self.relation),
        }))
    }
}

struct ArrayRelationInstance {
    relation: Arc<PreparedRelation>,
    host: Arc<dyn AggregateStateAllocator>,
    allocator: HostAggregateAllocator,
}
impl ScalarKernelInstance for ArrayRelationInstance {
    fn evaluate<'a>(
        &mut self,
        input: ScalarCallInput<'_, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        self.relation
            .evaluate(input, control, &self.host, &self.allocator)
    }

    fn retained_bytes(&self) -> usize {
        size_of::<Self>() + self.allocator.metadata_bytes()
    }
}

impl ArrayRelationOwner {
    fn refine_selected_core(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<CallEffects, FunctionEffectOwnerError<FunctionBindingError>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        let result = (|| {
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            if input.function_id != self.declaration.function_id()
                || input.kind != FunctionKind::Scalar
            {
                return Err(FunctionBindingError::UnknownFunction.into());
            }
            if !input.environment.is_empty()
                || !matches!(input.proof_scope, CallProofScope::Unconditional)
                    && input.proof_scope != input.context.proof_scope()
            {
                return Err(FunctionBindingError::InvalidBinding(
                    "array relation has no environment dependencies and requires an exact proof scope"
                        .into(),
                )
                .into());
            }
            work.flush().map_err(FunctionEffectOwnerError::Control)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
                other => FunctionEffectOwnerError::Owner(other),
            })?;
            let result = self.call_effects(input.proof_scope);
            Ok(result)
        })();
        if matches!(&result, Err(FunctionEffectOwnerError::Control(_))) {
            return result;
        }
        work.finish().map_err(FunctionEffectOwnerError::Control)?;
        result
    }
    fn prepare_selected_core(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        let result: Result<Arc<dyn PreparedScalarKernel>, KernelFailure> = (|| {
            work.step().map_err(compile_failure)?;
            if input.function_id != self.declaration.function_id()
                || input.kind != FunctionKind::Scalar
                || contract.function_id() != input.function_id
                || !std::ptr::eq(contract.selected(), input.selected)
                || novarocks_type_contract::CallEffectContext::from(contract.context())
                    != input.context
                || contract.decimal_overflow_policy() != input.decimal_overflow_policy
                || contract.effects() != &self.call_effects(input.proof_scope)
                || !input.environment.is_empty()
            {
                return Err(invalid(
                    "array relation preparation differs from its exact checked call",
                ));
            }
            work.flush().map_err(compile_failure)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => compile_failure(error),
                _ => invalid("array relation preparation has a stale selected binding"),
            })?;
            let relation = PreparedRelation::prepare(self.operation, &contract, || {
                work.step().map_err(compile_failure)
            })?;
            work.step().map_err(compile_failure)?;
            Ok(Arc::new(PreparedArrayRelation {
                contract,
                relation: Arc::new(relation),
            }) as Arc<dyn PreparedScalarKernel>)
        })();
        if result.as_ref().is_err_and(KernelFailure::is_interruption) {
            return result;
        }
        work.finish().map_err(compile_failure)?;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, ArrayRef, BooleanArray, ListArray, types::Int64Type};
    use novarocks_function_contract::kernel_failure::ARRAY_ELEMENT_COMPARISON_INDETERMINATE;
    use novarocks_type_contract::{
        DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId, ExpressionEffectContext,
        ExpressionUseId, SemanticParameters,
    };

    use crate::builtin::array_invocation_test_support::{Control, Host};
    use crate::{
        CallArgumentUses, EvaluatedArgument, FunctionArgument, FunctionArgumentType,
        FunctionValueType, ScalarEvaluationInstance, ScopedExpressionEffects, Selection,
        specialize_scalar,
    };

    #[test]
    fn array_relation_owner_executes_selected_rows_and_keeps_result_charged() {
        let probe: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some([Some(7)]),
            Some([Some(7)]),
            Some([Some(7)]),
        ]));
        let inner = ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some([Some(7)]),
            Some([None]),
            Some([None]),
            Some([Some(7)]),
        ]);
        let field = Arc::new(arrow_schema::Field::new(
            "item",
            inner.data_type().clone(),
            true,
        ));
        let left: ArrayRef = Arc::new(ListArray::new(
            field,
            arrow_buffer::OffsetBuffer::from_lengths([2, 0, 2]),
            Arc::new(inner),
            None,
        ));

        let (_, signatures) = super::super::registry::builtin_scalar_declarations()
            .into_iter()
            .find(|(name, _)| name == "contains")
            .unwrap();
        let (declaration, resolver) = super::super::catalogue::scalar_definition_parts(
            "contains",
            &signatures,
            FunctionKind::Scalar,
        )
        .unwrap();
        let owner =
            ArrayRelationOwner::new("contains", declaration, Resolver::Fixed(Arc::new(resolver)))
                .unwrap();
        let original = [&left, &probe].map(|array| FunctionArgument::Value {
            value_type: FunctionValueType::new(array.data_type().clone(), true),
            constant: None,
        });
        let selected = Arc::new(
            owner
                .resolve(
                    FunctionBindingRequest {
                        expected_result_type: None,
                        arguments: &original,
                        logical_argument_count: 2,
                    },
                    crate::binding_test_control(),
                )
                .unwrap(),
        );
        let arguments = selected
            .argument_types
            .iter()
            .map(|argument| {
                let FunctionArgumentType::Value(value) = argument else {
                    panic!("array relation has a non-value argument")
                };
                FunctionArgument::Value {
                    value_type: value.clone(),
                    constant: None,
                }
            })
            .collect::<Vec<_>>();
        let context = ExpressionEffectContext {
            use_id: ExpressionUseId::new(1),
            region: EvaluationRegionId::new(4),
            demand: EvaluationDemand::Value,
        };
        let parameters = SemanticParameters::try_new([]).unwrap();
        let uses = [Some(ExpressionUseId::new(2)), Some(ExpressionUseId::new(3))];
        let prepared = specialize_scalar(
            &owner,
            CallEffectInput {
                context: context.into(),
                argument_uses: CallArgumentUses::SelectedChannels(&uses),
                function_id: owner.declaration.function_id(),
                kind: FunctionKind::Scalar,
                selected: &selected,
                request: FunctionBindingRequest {
                    expected_result_type: None,
                    arguments: &arguments,
                    logical_argument_count: 2,
                },
                environment: &[],
                parameters: &parameters,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                proof_scope: CallProofScope::Unconditional,
            },
            selected.clone(),
            ScopedExpressionEffects::pure_value(context),
            crate::binding_test_control(),
        )
        .unwrap()
        .into_prepared();

        let host = Host::with_limit(usize::MAX);
        let mut instance =
            ScalarEvaluationInstance::instantiate_with_allocator(prepared, Some(host.clone()))
                .unwrap();
        let input = [
            EvaluatedArgument::Column(&left),
            EvaluatedArgument::Column(&probe),
        ];
        let selected = Selection::try_sparse(3, &[0, 2]).unwrap();
        let output = instance
            .evaluate(selected, &input, &Control::default())
            .unwrap();
        let values = output
            .values()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), [Some(true), None]);
        let [error] = output.errors() else {
            panic!("missing selected comparison refusal")
        };
        assert_eq!(error.selected_ordinal(), 1);
        assert_eq!(
            error.projected_fact().reason(),
            ARRAY_ELEMENT_COMPARISON_INDETERMINATE
        );
        drop(instance);
        assert_ne!(host.live_blocks(), 0, "result still owns its allocation");
        drop(output);
        host.released();
    }
}
