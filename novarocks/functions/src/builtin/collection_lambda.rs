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
//! Trino collection lambdas: contextual parameters and exact overload ownership.

mod kernel;

pub use kernel::prepare_kernel;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_schema::{DataType, FieldRef, Fields};
use novarocks_type_contract::{
    ArgumentControl, ArrayCommonTypeFailure, BorrowedTypedValueBinding, CallEffects,
    CallProofScope, CompileCheckpoints, CompilePhase, EvaluationDemand, FunctionEffectDeclaration,
    FunctionId, FunctionInstanceState, FunctionIntrinsicRowError, FunctionNullBehavior,
    FunctionOverloadId, FunctionParameterTypes, FunctionValueType, KeyRelation, ObservableEffects,
    PureCompileControl, ValueConstructionFailure, ValueConstructionRequest, ValueKind, ValueLayout,
    ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
};

use super::binding_control;
use crate::kernel_control::{compile_failure, invalid};
use crate::{
    CallEffectInput, CheckedCallEffectInput, CheckedOwnerFailure, EngineFunctionCatalogBuilder,
    FunctionArgument, FunctionArgumentType, FunctionBindingDeclaration, FunctionBindingError,
    FunctionBindingRequest, FunctionBindingResolver, FunctionBindingResolverOwner,
    FunctionBindingSelection, FunctionCatalogError, FunctionDefinition, FunctionEffectOwner,
    FunctionEffectOwnerError, FunctionFailureBehavior, FunctionKind, FunctionOverloadDeclaration,
    FunctionResultType, FunctionSemantics, FunctionVisibility, FunctionVolatility,
    HigherOrderBindingShape, HigherOrderCallContract, KernelFailure, LambdaBodyContract,
    PreparedHigherOrderKernel, PureFunctionMetadataOwner, PureHigherOrderImplementation,
    PureImplementationDeclaration, PureImplementationId, PureKernelAbi,
};

#[derive(Clone, Copy)]
enum ResultShape {
    Collection,
    Match,
    ListOfBody,
    MapKeyOfBody,
    MapValueOfBody,
}

#[derive(Clone, Copy)]
pub struct CollectionLambdaSignature {
    name: &'static str,
    map: bool,
    arity: usize,
    arguments: &'static str,
    result: &'static str,
    fixed_body: Option<FixedBody>,
    result_shape: ResultShape,
}

#[derive(Clone, Copy)]
enum FixedBody {
    Boolean,
    Integer,
}

impl FixedBody {
    fn carrier(self) -> DataType {
        match self {
            Self::Boolean => DataType::Boolean,
            Self::Integer => DataType::Int32,
        }
    }
}

impl CollectionLambdaSignature {
    pub const fn name(self) -> &'static str {
        self.name
    }

    pub const fn arity(self) -> usize {
        self.arity
    }

    pub fn accepts_collection(self, source: &FunctionValueType) -> bool {
        source.logical_type() == ValueLogicalType::Physical
            && if self.map {
                matches!(source.data_type(), DataType::Map(_, _) | DataType::Null)
            } else {
                matches!(source.data_type(), DataType::List(_) | DataType::Null)
            }
    }

    fn function(self) -> FunctionId {
        FunctionId::try_new(format!("builtin.scalar/{}/v1", self.name))
            .expect("valid collection lambda function identity")
    }

    pub fn overload(self) -> FunctionOverloadId {
        FunctionOverloadId::try_new(format!("builtin.scalar/{}/lambda-v1", self.name))
            .expect("valid collection lambda overload identity")
    }

    pub(super) fn owns_overload(self, overload: &FunctionOverloadId) -> bool {
        overload
            .as_str()
            .strip_prefix("builtin.scalar/")
            .and_then(|identity| identity.strip_suffix("/lambda-v1"))
            == Some(self.name)
    }

    fn owns_function(self, function: &FunctionId) -> bool {
        function
            .as_str()
            .strip_prefix("builtin.scalar/")
            .and_then(|identity| identity.strip_suffix("/v1"))
            == Some(self.name)
    }

    pub(super) fn effects(self) -> FunctionEffectDeclaration {
        let own_row_error = if matches!(self.fixed_body, Some(FixedBody::Integer))
            || matches!(self.result_shape, ResultShape::MapKeyOfBody)
        {
            FunctionIntrinsicRowError::MayRaise
        } else {
            FunctionIntrinsicRowError::NoRowError
        };
        FunctionEffectDeclaration {
            value_stability: FunctionVolatility::Immutable,
            own_row_error,
            failure_behavior: FunctionFailureBehavior::Propagate,
            null_behavior: FunctionNullBehavior::ControlDefined,
            argument_control: ArgumentControl::HigherOrder {
                body_ordinal: 1,
                body_demand: EvaluationDemand::Value,
            },
            instance_state: FunctionInstanceState::None,
            observable_effects: ObservableEffects::NONE,
            environment_dependencies: Box::new([]),
        }
    }

    pub(super) fn declaration(self) -> FunctionOverloadDeclaration {
        let effects = self.effects();
        FunctionOverloadDeclaration {
            identity: self.overload(),
            argument_pattern: self.arguments.into(),
            result_pattern: self.result.into(),
            semantics: FunctionSemantics::from_effects(&effects),
            effects: Some(effects),
            aggregate: None,
        }
    }

    pub(super) fn public_signature(self) -> crate::FunctionPublicSignature {
        let arguments = self
            .arguments
            .strip_prefix('(')
            .and_then(|arguments| arguments.strip_suffix(')'))
            .expect("collection lambda declaration encloses its argument list");
        crate::FunctionPublicSignature::new(
            arguments.replacen(",function(", ", function(", 1),
            self.result,
            "",
        )
    }

    pub(super) fn implementation(
        self,
    ) -> Result<PureImplementationDeclaration, FunctionCatalogError> {
        Ok(PureImplementationDeclaration {
            overload: self.overload(),
            implementation: PureImplementationId::try_new(format!(
                "builtin.scalar/{}/lambda-selected-v1",
                self.name,
            ))?,
            abi: PureKernelAbi::HigherOrderV1,
        })
    }
}

pub fn signature(name: &str) -> Option<CollectionLambdaSignature> {
    use FixedBody::{Boolean, Integer};
    use ResultShape::{Collection, ListOfBody, MapKeyOfBody, MapValueOfBody, Match};

    let (name, map, arity, arguments, result, fixed_body, result_shape) = match name {
        "transform" => (
            "transform",
            false,
            1,
            "(array(T),function(T,U))",
            "array(U)",
            None,
            ListOfBody,
        ),
        "filter" => (
            "filter",
            false,
            1,
            "(array(T),function(T,boolean))",
            "array(T)",
            Some(Boolean),
            Collection,
        ),
        "array_sort" => (
            "array_sort",
            false,
            2,
            "(array(T),function(T,T,integer))",
            "array(T)",
            Some(Integer),
            Collection,
        ),
        "any_match" => (
            "any_match",
            false,
            1,
            "(array(T),function(T,boolean))",
            "boolean",
            Some(Boolean),
            Match,
        ),
        "all_match" => (
            "all_match",
            false,
            1,
            "(array(T),function(T,boolean))",
            "boolean",
            Some(Boolean),
            Match,
        ),
        "map_filter" => (
            "map_filter",
            true,
            2,
            "(map(K,V),function(K,V,boolean))",
            "map(K,V)",
            Some(Boolean),
            Collection,
        ),
        "transform_keys" => (
            "transform_keys",
            true,
            2,
            "(map(K1,V),function(K1,V,K2))",
            "map(K2,V)",
            None,
            MapKeyOfBody,
        ),
        "transform_values" => (
            "transform_values",
            true,
            2,
            "(map(K,V1),function(K,V1,V2))",
            "map(K,V2)",
            None,
            MapValueOfBody,
        ),
        _ => return None,
    };
    Some(CollectionLambdaSignature {
        name,
        map,
        arity,
        arguments,
        result,
        fixed_body,
        result_shape,
    })
}

pub(super) type PrepareCollectionLambda =
    fn(
        CollectionLambdaSignature,
        Arc<HigherOrderCallContract>,
        Arc<LambdaBodyContract>,
        &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedHigherOrderKernel>, KernelFailure>;

pub(super) fn register(
    builder: &mut EngineFunctionCatalogBuilder,
    prepare: Option<PrepareCollectionLambda>,
) -> Result<(), FunctionCatalogError> {
    for name in [
        "transform",
        "filter",
        "map_filter",
        "transform_keys",
        "transform_values",
        "any_match",
        "all_match",
    ] {
        let signature = signature(name).expect("registered collection lambda signature");
        let declaration = FunctionBindingDeclaration::try_new(
            signature.function(),
            FunctionKind::Scalar,
            [signature.declaration()],
        )
        .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
            subject: "collection lambda binding declaration",
            value: error.to_string().into(),
        })?
        .with_public_signatures([(signature.overload(), vec![signature.public_signature()])])
        .map_err(super::catalogue::public_signature_error)?;
        let owner = Arc::new(CollectionLambdaOwner {
            signature,
            implementations: prepare
                .map(|_| signature.implementation())
                .transpose()?
                .into_iter()
                .collect(),
            declaration,
            prepare,
        });
        let definition = if prepare.is_some() {
            FunctionDefinition::try_new_pure_higher_order(name, FunctionVisibility::Public, owner)
                .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin collection lambda pure owner",
                value: error.to_string().into(),
            })?
        } else {
            FunctionDefinition::bound_with_owner(
                name,
                FunctionVisibility::Public,
                owner.declaration.clone(),
                FunctionBindingResolverOwner::collection_lambda(owner),
                None,
            )?
        };
        builder.register(definition)?;
    }
    Ok(())
}

type BoundLambdaArguments<'a> = (
    &'a FunctionValueType,
    &'a FunctionParameterTypes,
    &'a FunctionValueType,
);

impl CollectionLambdaSignature {
    fn argument_facts<'a>(
        self,
        request: FunctionBindingRequest<'a>,
    ) -> Result<BoundLambdaArguments<'a>, FunctionBindingError> {
        let [
            FunctionArgument::Value { value_type, .. },
            FunctionArgument::Lambda {
                parameter_types,
                result_type,
            },
        ] = request.arguments
        else {
            return Err(FunctionBindingError::NoMatchingOverload);
        };
        if request.logical_argument_count != 2
            || parameter_types.len() != self.arity
            || !self.accepts_collection(value_type)
        {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        Ok((value_type, parameter_types, result_type))
    }

    pub(super) fn begin(
        self,
        ordinary_ordinal: usize,
        ordinary: &FunctionArgument,
        lambda_ordinal: usize,
        lambda_arity: usize,
        control: &dyn PureCompileControl,
    ) -> Result<HigherOrderBindingShape, FunctionBindingError> {
        binding_control::scope(control, |work| {
            work.step()?;
            let FunctionArgument::Value { value_type, .. } = ordinary else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            if ordinary_ordinal != 0
                || lambda_ordinal != 1
                || lambda_arity != self.arity
                || !self.accepts_collection(value_type)
            {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            self.input_shape(value_type, work)
        })
    }

    fn input_shape(
        self,
        source: &FunctionValueType,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<HigherOrderBindingShape, FunctionBindingError> {
        let collection_target = self.collection_target(source, work)?;
        let storage = ValueTypeStorageLifetime::pure();
        let child = |ordinal, work: &mut CompileCheckpoints<'_>| {
            let mut observe = |request| observe_construction(request, work);
            collection_target
                .child_at_with_construction(ordinal, storage.clone(), Some(&mut observe))
                .map_err(construction_error)?
                .ok_or_else(|| binding_error("collection lambda has no declared child"))
        };
        let first = child(0, work)?;
        let parameter_types = if self.map {
            [first, child(1, work)?].into()
        } else if self.arity == 2 {
            [first.clone_observed(|| work.step())?, first].into()
        } else {
            [first].into()
        };
        Ok(HigherOrderBindingShape {
            overload: self.overload(),
            collection_target,
            parameter_types,
        })
    }

    fn collection_target(
        self,
        source: &FunctionValueType,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FunctionValueType, FunctionBindingError> {
        if source.data_type() != &DataType::Null {
            return Ok(source.clone_observed(|| work.step())?);
        }
        let unknown = FunctionValueType::new(DataType::Null, true);
        if self.map {
            make_map_type(
                &FunctionValueType::new(DataType::Null, false),
                &unknown,
                true,
                MapFields::CANONICAL,
                work,
            )
        } else {
            make_list_type(&unknown, true, work)
        }
    }

    pub(super) fn resolve(
        self,
        request: FunctionBindingRequest<'_>,
        installed: bool,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        if installed {
            self.select(request, control)
        } else {
            // Keep the staging rejection until this owner has its actual ABI.
            control.checkpoint(CompilePhase::FunctionSpecialization, 1)?;
            self.argument_facts(request)?;
            Err(FunctionBindingError::UnavailableImplementation(
                self.overload(),
            ))
        }
    }

    pub(super) fn select(
        self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| {
            let (source, parameters, body) = self.argument_facts(request)?;
            let source = self.collection_target(source, work)?;
            self.validate_arguments(&source, parameters, body, work)?;
            let result = self.result_type(&source, body, work)?;
            let selected = FunctionBindingSelection {
                overload: self.overload(),
                argument_types: [
                    FunctionArgumentType::Value(source),
                    FunctionArgumentType::Lambda {
                        parameter_types: parameters.clone(),
                        result_type: body.clone_observed(|| work.step())?,
                    },
                ]
                .into(),
                result_type: FunctionResultType::Scalar(result),
                aggregate: None,
            };
            Ok(selected)
        })
    }

    fn validate_arguments(
        self,
        source: &FunctionValueType,
        parameters: &FunctionParameterTypes,
        body: &FunctionValueType,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), FunctionBindingError> {
        let source = source.typed_binding().borrowed();
        let mut observe = || work.step().map_err(FunctionBindingError::from);
        for (ordinal, parameter) in parameters.iter().enumerate() {
            let child_ordinal = if self.map { ordinal } else { 0 };
            let expected = source
                .child_at(child_ordinal)
                .ok_or(FunctionBindingError::NoMatchingOverload)?;
            if !expected
                .exactly_equals_observed(parameter.typed_binding().borrowed(), &mut observe)?
            {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
        }
        if let Some(fixed) = self.fixed_body
            && (body.logical_type() != ValueLogicalType::Physical
                || *body.data_type() != fixed.carrier())
        {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        if matches!(self.result_shape, ResultShape::MapKeyOfBody)
            && body.data_type() != &DataType::Null
            && KeyRelation::Identical
                .validate_type_observed(body.value_type(), |_| work.step())?
                .is_err()
        {
            return Err(FunctionBindingError::UnavailableImplementation(
                self.overload(),
            ));
        }
        Ok(())
    }

    fn result_type(
        self,
        source: &FunctionValueType,
        body: &FunctionValueType,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FunctionValueType, FunctionBindingError> {
        let output = match self.result_shape {
            ResultShape::Collection => source.clone_observed(|| work.step())?,
            ResultShape::Match => {
                FunctionValueType::new(DataType::Boolean, source.nullable() || body.nullable())
            }
            ResultShape::ListOfBody => make_list_type(body, source.nullable(), work)?,
            ResultShape::MapKeyOfBody | ResultShape::MapValueOfBody => {
                let replace_key = matches!(self.result_shape, ResultShape::MapKeyOfBody);
                let original = MapFields::from_carrier(source.data_type())
                    .ok_or(FunctionBindingError::NoMatchingOverload)?;
                let keep_ordinal = usize::from(replace_key);
                let storage = ValueTypeStorageLifetime::pure();
                let mut observe = |request| observe_construction(request, work);
                let kept = source
                    .child_at_with_construction(keep_ordinal, storage.clone(), Some(&mut observe))
                    .map_err(construction_error)?
                    .ok_or_else(|| binding_error("collection lambda Map has no declared child"))?;
                if replace_key {
                    let key = body
                        .with_nullable_with_construction(false, storage, Some(&mut observe))
                        .map_err(construction_error)?;
                    make_map_type(
                        &key,
                        &kept,
                        source.nullable(),
                        original.with_sorted(false),
                        work,
                    )?
                } else {
                    make_map_type(&kept, body, source.nullable(), original, work)?
                }
            }
        };
        Ok(output)
    }

    pub(super) fn validate_selected(
        self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        binding_control::scope(control, |work| {
            if !self.owns_overload(&selected.overload) {
                return Err(FunctionBindingError::UnknownOverload(
                    selected.overload.clone(),
                ));
            }
            let (source, parameters, body) = self.argument_facts(request)?;
            self.validate_arguments(source, parameters, body, work)?;
            let [
                FunctionArgumentType::Value(selected_source),
                FunctionArgumentType::Lambda {
                    parameter_types: selected_parameters,
                    result_type: selected_body,
                },
            ] = selected.argument_types.as_ref()
            else {
                return Err(binding_error(
                    "selected collection lambda has different argument channels",
                ));
            };
            let mut observe = || work.step().map_err(FunctionBindingError::from);
            if selected.aggregate.is_some()
                || selected_parameters.len() != parameters.len()
                || !selected_source.exactly_equals_checked_observed(source, &mut observe)?
                || !selected_body.exactly_equals_checked_observed(body, &mut observe)?
            {
                return Err(binding_error(
                    "selected collection lambda has different argument types",
                ));
            }
            for (selected, source) in selected_parameters.iter().zip(parameters.iter()) {
                if !selected.exactly_equals_checked_observed(source, &mut observe)? {
                    return Err(binding_error(
                        "selected collection lambda has different parameter types",
                    ));
                }
            }
            let FunctionResultType::Scalar(output) = &selected.result_type else {
                return Err(binding_error(
                    "selected collection lambda has a relation result",
                ));
            };
            if !self.matches_result(source, body, output, work)? {
                return Err(binding_error(
                    "selected collection lambda has a different result type",
                ));
            }
            Ok(())
        })
    }

    // The original frozen author borrows existing fields and children. It
    // needs no new schema allocation or substitute creator to prove them.
    fn matches_result(
        self,
        source: &FunctionValueType,
        body: &FunctionValueType,
        output: &FunctionValueType,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<bool, FunctionBindingError> {
        let mut observe = || work.step().map_err(FunctionBindingError::from);
        if output.logical_type() != ValueLogicalType::Physical {
            return Ok(false);
        }
        let source_view = source.typed_binding().borrowed();
        let body_view = body.typed_binding().borrowed();
        let output_view = output.typed_binding().borrowed();
        match self.result_shape {
            ResultShape::Collection => output_view.exactly_equals_observed(source_view, observe),
            ResultShape::Match => {
                observe()?;
                Ok(output.data_type() == &DataType::Boolean
                    && output.nullable() == (source.nullable() || body.nullable()))
            }
            ResultShape::ListOfBody => {
                let DataType::List(item) = output.data_type() else {
                    return Ok(false);
                };
                observe()?;
                Ok(output.nullable() == source.nullable()
                    && item.name() == "item"
                    && required_child(output_view, 0)?
                        .exactly_equals_observed(body_view, observe)?)
            }
            ResultShape::MapKeyOfBody | ResultShape::MapValueOfBody => {
                let replace_key = matches!(self.result_shape, ResultShape::MapKeyOfBody);
                let Some(source_fields) = MapFields::from_carrier(source.data_type()) else {
                    return Ok(false);
                };
                let Some(output_fields) = MapFields::from_carrier(output.data_type()) else {
                    return Ok(false);
                };
                observe()?;
                if output.nullable() != source.nullable()
                    || output_fields
                        != source_fields.with_sorted(!replace_key && source_fields.sorted)
                {
                    return Ok(false);
                }
                let changed = usize::from(!replace_key);
                let kept = usize::from(replace_key);
                if !required_child(output_view, kept)?
                    .exactly_equals_observed(required_child(source_view, kept)?, &mut observe)?
                {
                    return Ok(false);
                }
                let result = required_child(output_view, changed)?;
                if replace_key {
                    result.same_physical_binding_ignoring_root_nullability_observed(
                        body_view, observe,
                    )
                } else {
                    result.exactly_equals_observed(body_view, observe)
                }
            }
        }
    }

    pub(super) fn refine(
        self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<CallEffects, FunctionEffectOwnerError<FunctionBindingError>> {
        let result = binding_control::scope(control, |work| {
            work.step()?;
            if !self.owns_function(input.function_id)
                || !self.owns_overload(&input.selected.overload)
                || input.kind != FunctionKind::Scalar
            {
                return Err(FunctionBindingError::UnknownFunction);
            }
            if !input.environment.is_empty()
                || (!matches!(input.proof_scope, CallProofScope::Unconditional)
                    && input.proof_scope != input.context.proof_scope())
            {
                return Err(binding_error(
                    "collection lambda requires its exact proof scope and empty environment",
                ));
            }
            work.flush()?;
            validate()?;
            let base = self.effects();
            Ok(CallEffects {
                value_stability: base.value_stability,
                own_row_error: base.own_row_error,
                failure_behavior: base.failure_behavior,
                null_behavior: base.null_behavior,
                argument_control: base.argument_control,
                instance_state: base.instance_state,
                observable_effects: base.observable_effects,
                environment: Box::new([]),
                proof_scope: input.proof_scope,
            })
        });
        result.map_err(effect_binding_error)
    }

    pub(super) fn prepare_exact(
        self,
        prepare: Option<PrepareCollectionLambda>,
        input: CallEffectInput<'_>,
        contract: Arc<HigherOrderCallContract>,
        body: Arc<LambdaBodyContract>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<Arc<dyn PreparedHigherOrderKernel>, KernelFailure> {
        control
            .checkpoint(CompilePhase::FunctionSpecialization, 1)
            .map_err(compile_failure)?;
        let call = contract.call();
        if !self.owns_function(input.function_id)
            || !self.owns_overload(&input.selected.overload)
            || input.kind != FunctionKind::Scalar
            || call.function_id() != input.function_id
            || !std::ptr::eq(call.selected(), input.selected)
            || call.context() != input.context
            || call.decimal_overflow_policy() != input.decimal_overflow_policy
            || call.effects().argument_control != self.effects().argument_control
            || call.effects().null_behavior != FunctionNullBehavior::ControlDefined
            || !Arc::ptr_eq(body.call(), &contract)
        {
            return Err(invalid(
                "collection lambda preparation differs from its exact checked call",
            ));
        }
        validate().map_err(|error| match error {
            FunctionBindingError::Control(error) => compile_failure(error),
            _ => invalid("collection lambda preparation has a stale selected binding"),
        })?;
        let prepare =
            prepare.ok_or_else(|| invalid("collection lambda kernel is not installed"))?;
        prepare(self, contract, body, control)
    }
}

pub(crate) struct CollectionLambdaOwner {
    signature: CollectionLambdaSignature,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
    prepare: Option<PrepareCollectionLambda>,
}

impl FunctionBindingResolver for CollectionLambdaOwner {
    fn begin_higher_order_binding(
        &self,
        ordinary_ordinal: usize,
        ordinary: &FunctionArgument,
        lambda_ordinal: usize,
        lambda_arity: usize,
        control: &dyn PureCompileControl,
    ) -> Result<HigherOrderBindingShape, FunctionBindingError> {
        self.signature.begin(
            ordinary_ordinal,
            ordinary,
            lambda_ordinal,
            lambda_arity,
            control,
        )
    }

    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.signature
            .resolve(request, self.prepare.is_some(), control)
    }

    fn select_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        control.checkpoint(CompilePhase::FunctionSpecialization, 1)?;
        if !self.signature.owns_overload(overload) {
            return Err(FunctionBindingError::UnknownOverload(overload.clone()));
        }
        self.signature.select(request, control)
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        self.signature.validate_selected(selected, request, control)
    }
}

impl PureFunctionMetadataOwner for CollectionLambdaOwner {
    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }

    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }

    fn binding_resolver_owner(self: Arc<Self>) -> FunctionBindingResolverOwner {
        FunctionBindingResolverOwner::collection_lambda(self)
    }
}

impl FunctionEffectOwner for CollectionLambdaOwner {
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
        self.signature.refine(input, control, || {
            self.validate_selected(input.selected, input.request, control)
        })
    }

    fn validate_and_refine_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &CheckedCallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, CheckedOwnerFailure<FunctionEffectOwnerError<Self::Error>>> {
        self.signature
            .refine(input, control, || {
                checked.require_binding_author(self, input)
            })
            .map_err(CheckedOwnerFailure::Original)
    }
}

impl PureHigherOrderImplementation for CollectionLambdaOwner {
    fn prepare_higher_order(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<HigherOrderCallContract>,
        body: Arc<LambdaBodyContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedHigherOrderKernel>, KernelFailure> {
        self.signature
            .prepare_exact(self.prepare, input, contract, body, control, || {
                self.validate_selected(input.selected, input.request, control)
            })
    }

    fn prepare_higher_order_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &CheckedCallEffectInput<'_>,
        contract: Arc<HigherOrderCallContract>,
        body: Arc<LambdaBodyContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedHigherOrderKernel>, CheckedOwnerFailure<KernelFailure>> {
        self.signature
            .prepare_exact(self.prepare, input, contract, body, control, || {
                checked.require_binding_author(self, input)
            })
            .map_err(CheckedOwnerFailure::Original)
    }
}

fn binding_error(message: &'static str) -> FunctionBindingError {
    FunctionBindingError::InvalidBinding(message.into())
}

fn effect_binding_error(
    error: FunctionBindingError,
) -> FunctionEffectOwnerError<FunctionBindingError> {
    match error {
        FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
        other => FunctionEffectOwnerError::Owner(other),
    }
}

fn construction_error(
    error: ValueConstructionFailure<FunctionBindingError>,
) -> FunctionBindingError {
    match error {
        ValueConstructionFailure::Observer(error) => error,
        other => FunctionBindingError::InvalidBinding(format!("{other:?}").into()),
    }
}

fn observe_construction(
    request: ValueConstructionRequest,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FunctionBindingError> {
    match request {
        ValueConstructionRequest::Work(amount) => {
            for _ in 0..amount {
                work.step()?;
            }
        }
        ValueConstructionRequest::Exact(_) | ValueConstructionRequest::OpaqueUpper(_) => {
            work.step()?
        }
    }
    Ok(())
}

fn make_list_type(
    item: &FunctionValueType,
    nullable: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FunctionValueType, FunctionBindingError> {
    let mut observe = |request| observe_construction(request, work);
    novarocks_type_contract::list_value_type_with_construction(
        item,
        nullable,
        ValueTypeStorageLifetime::pure(),
        Some(&mut observe),
    )
    .map_err(|error| match error {
        ArrayCommonTypeFailure::Construction(error) => construction_error(error),
        ArrayCommonTypeFailure::Rule(_) => binding_error("invalid collection lambda result List"),
    })
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct MapFields<'a> {
    entries: &'a str,
    key: &'a str,
    value: &'a str,
    sorted: bool,
}

impl<'a> MapFields<'a> {
    const CANONICAL: MapFields<'static> = MapFields {
        entries: "entries",
        key: "key",
        value: "value",
        sorted: false,
    };

    fn from_carrier(data_type: &'a DataType) -> Option<Self> {
        let DataType::Map(entries, sorted) = data_type else {
            return None;
        };
        let DataType::Struct(fields) = entries.data_type() else {
            return None;
        };
        let [key, value] = fields.as_ref() else {
            return None;
        };
        Some(Self {
            entries: entries.name(),
            key: key.name(),
            value: value.name(),
            sorted: *sorted,
        })
    }

    fn with_sorted(self, sorted: bool) -> Self {
        Self { sorted, ..self }
    }
}

fn make_map_type(
    key: &FunctionValueType,
    value: &FunctionValueType,
    nullable: bool,
    fields: MapFields<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FunctionValueType, FunctionBindingError> {
    use super::scalar_construction::ScalarConstruction;
    let storage = ValueTypeStorageLifetime::pure();
    let mut observe = |request| observe_construction(request, work);
    let mut creation = ScalarConstruction::new(storage.clone(), Some(&mut observe));
    let key_carrier = creation
        .cloned_data_type(key.data_type())
        .map_err(construction_error)?;
    let value_carrier = creation
        .cloned_data_type(value.data_type())
        .map_err(construction_error)?;
    let children = [
        creation
            .field(fields.key, key_carrier, false)
            .map_err(construction_error)?,
        creation
            .field(fields.value, value_carrier, value.nullable())
            .map_err(construction_error)?,
    ];
    creation
        .array::<FieldRef>(children.len())
        .map_err(construction_error)?;
    creation
        .arc_slice::<FieldRef>(children.len())
        .map_err(construction_error)?;
    let carrier = DataType::Map(
        creation
            .field(
                fields.entries,
                DataType::Struct(Fields::from(Vec::from(children))),
                false,
            )
            .map_err(construction_error)?,
        fields.sorted,
    );
    let layout =
        ValueLayout::try_new_with_construction(&carrier, storage.clone(), creation.observer())
            .map_err(construction_error)?;
    let value = ValueTypeRef::try_new_with_construction(
        ValueKind::Map {
            key: key.value_type().clone(),
            value: value.value_type().clone(),
        },
        nullable,
        storage,
        creation.observer(),
    )
    .map_err(construction_error)?;
    FunctionValueType::try_from_value(value, layout).map_err(Into::into)
}

fn required_child(
    value: BorrowedTypedValueBinding<'_>,
    ordinal: usize,
) -> Result<BorrowedTypedValueBinding<'_>, FunctionBindingError> {
    value
        .child_at(ordinal)
        .ok_or_else(|| binding_error("collection lambda has no declared child"))
}
