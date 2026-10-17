// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Exact ordinary-scalar source validation. No kernel is prepared or discarded.
use super::{
    Construction, Error, NativeDecodeResources, NativeOriginalRequests, frozen_validation_failure,
    invalid, step,
};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::expr;
use novarocks_type_contract::{FunctionValueType, ValueSchemaCreationScope};

impl NativeOriginalRequests {
    pub(super) fn with_catalog_request_scope<T>(
        &self,
        id: u32,
        scope: &mut dyn ValueSchemaCreationScope<Construction>,
        resources: &mut NativeDecodeResources<'_>,
        consume: impl FnOnce(
            novarocks_functions::FunctionBindingRequest<'_>,
            &mut dyn ValueSchemaCreationScope<Construction>,
        ) -> Result<T, Error>,
    ) -> Result<T, Error> {
        use novarocks_physical_plan::CallRequestMaterializationFailure as F;
        let original = self.request(id, resources)?;
        let request = novarocks_physical_plan::materialize_call_request_with_creator_in(
            original,
            &self.pools,
            scope,
            &mut resources.work,
            novarocks_functions::MAX_CALL_EFFECT_ARGUMENTS,
        )
        .map_err(|cause| match cause {
            F::Control(cause) => Error::from(Construction::Control(cause)),
            F::Creator(cause) => Error::from(cause),
            F::Owner(cause) => {
                Error::invalid_value(FieldPath::root("expr").field("original_request_id"), cause)
            }
        })?;
        // The original request's expected target is borrowed, never reconstructed
        // from a selected result. Release these temporary headers after consume.
        consume(request.request(), scope)
    }

    pub(crate) fn validate_scalar_actual_arguments(
        &self,
        request: u32,
        actual: &[expr::Expr],
        types: &novarocks_plan_codec::execution_type_table::DecodedExecutionTypeTable,
        resources: &mut NativeDecodeResources<'_>,
        path: FieldPath,
    ) -> Result<(), Error> {
        use novarocks_physical_plan::StaticFunctionArgument as Argument;
        let original = self.request(request, resources)?;
        if original.arguments.len() != actual.len() {
            return Err(invalid(
                "scalar actual arguments differ from its original request",
            ));
        }
        step(resources)?;
        for (at, (expected, actual)) in original.arguments.iter().zip(actual).enumerate() {
            let path = path.clone().index(at);
            match expected {
                Argument::Value {
                    value_type,
                    constant,
                } => {
                    if matches!(&actual.kind, Some(expr::expr::Kind::Lambda(_))) {
                        return Err(invalid("scalar value argument is an actual lambda"));
                    }
                    let desc = actual.r#type.as_ref().ok_or_else(|| {
                        Error::missing(
                            path.clone().field("type"),
                            "scalar argument complete type is absent",
                        )
                    })?;
                    let ty = crate::fragment_type_decode::resolve_type(
                        types,
                        desc,
                        path.clone().field("type"),
                        resources,
                    )?;
                    if ty.nullable() != actual.nullable
                        || !value_type.exactly_equals_checked_observed(ty, || step(resources))?
                    {
                        return Err(invalid(
                            "scalar actual value differs from its complete original argument",
                        ));
                    }
                    let reference = match &actual.kind {
                        Some(expr::expr::Kind::ConstantReference(reference)) => {
                            Some(novarocks_physical_plan::ConstantReference {
                                pool: novarocks_physical_plan::ConstantPoolId::new(
                                    reference.pool_id.ok_or_else(|| {
                                        invalid("scalar actual constant pool is absent")
                                    })?,
                                ),
                                ordinal: reference.row_ordinal,
                            })
                        }
                        Some(expr::expr::Kind::Literal(_)) => {
                            return Err(invalid("scalar strong argument has an inline literal"));
                        }
                        _ => None,
                    };
                    step(resources)?;
                    // A captured constant is an exact source-address fact.
                    // None carries no such fact: SQL may elide a Nested or
                    // identity cast into a physical Constant while retaining
                    // the original nonconstant request. Never promote that
                    // runtime representation into a binding constant below.
                    if constant.is_some() && reference != *constant {
                        return Err(invalid(
                            "scalar constant address differs from its original request",
                        ));
                    }
                    if let Some(expr::expr::Kind::ConstantReference(reference)) = &actual.kind {
                        let _actual_value = self.constant(reference, value_type, resources)?;
                    }
                }
                Argument::Lambda {
                    parameter_types,
                    result_type,
                } => {
                    let Some(expr::expr::Kind::Lambda(lambda)) = &actual.kind else {
                        return Err(invalid(
                            "scalar original lambda has no actual lambda source",
                        ));
                    };
                    if lambda.params.len() != parameter_types.len() {
                        return Err(invalid(
                            "scalar lambda parameter count differs from its original request",
                        ));
                    }
                    step(resources)?;
                    for (at, (expected, param)) in
                        parameter_types.iter().zip(&lambda.params).enumerate()
                    {
                        let path = path.clone().field("lambda").field("params").index(at);
                        let desc = param.r#type.as_ref().ok_or_else(|| {
                            Error::missing(
                                path.clone().field("type"),
                                "scalar lambda parameter complete type is absent",
                            )
                        })?;
                        let ty = crate::fragment_type_decode::resolve_type(
                            types,
                            desc,
                            path.field("type"),
                            resources,
                        )?;
                        if param.nullable != ty.nullable()
                            || !expected.exactly_equals_checked_observed(ty, || step(resources))?
                        {
                            return Err(invalid(
                                "scalar lambda parameter differs from its complete original source",
                            ));
                        }
                    }
                    let body = lambda.body.as_ref().ok_or_else(|| {
                        Error::missing(path.clone().field("body"), "scalar lambda body is absent")
                    })?;
                    let desc = body.r#type.as_ref().ok_or_else(|| {
                        Error::missing(
                            path.clone().field("body").field("type"),
                            "scalar lambda result complete type is absent",
                        )
                    })?;
                    let ty = crate::fragment_type_decode::resolve_type(
                        types,
                        desc,
                        path.field("body").field("type"),
                        resources,
                    )?;
                    if body.nullable != ty.nullable()
                        || !result_type.exactly_equals_checked_observed(ty, || step(resources))?
                    {
                        return Err(invalid(
                            "scalar lambda result differs from its complete original source",
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

// This header role borrows only one real DTO and the execution type namespace.
// Original requests and pools stay with their existing reader/source owner.
fn binding_source_bytes(
    raw: &novarocks_proto_models::physical_package_v2::FunctionBindingDefinition,
    types: &novarocks_plan_codec::execution_type_table::DecodedExecutionTypeTable,
) -> Result<usize, Error> {
    let mut bytes = std::mem::size_of_val(raw)
        .checked_add(std::mem::size_of_val(types))
        .ok_or_else(super::arithmetic)?;
    bytes = bytes
        .checked_add(types.retained_source_bytes_upper_bound())
        .ok_or_else(super::arithmetic)?;
    let mut add = |n| -> Result<(), Error> {
        bytes = bytes.checked_add(n).ok_or_else(super::arithmetic)?;
        Ok(())
    };
    add(raw.function_id.capacity())?;
    add(raw.overload_id.capacity())?;
    add(std::alloc::Layout::array::<
        novarocks_proto_models::physical_package_v2::FunctionArgumentType,
    >(raw.arguments.capacity())
    .map_err(|_| super::arithmetic())?
    .size())?;
    for argument in &raw.arguments {
        if let Some(
            novarocks_proto_models::physical_package_v2::function_argument_type::Kind::Lambda(
                lambda,
            ),
        ) = &argument.kind
        {
            add(
                std::alloc::Layout::array::<u32>(lambda.parameter_value_type_ids.capacity())
                    .map_err(|_| super::arithmetic())?
                    .size(),
            )?;
        }
    }
    if let Some(
        novarocks_proto_models::physical_package_v2::function_binding_definition::Result::Relation(
            relation,
        ),
    ) = &raw.result
    {
        add(
            std::alloc::Layout::array::<u32>(relation.value_type_ids.capacity())
                .map_err(|_| super::arithmetic())?
                .size(),
        )?;
    }
    Ok(bytes)
}
/// The existing default lowering actor, after full selected-source validation.
/// This carries no state, prepared kernel, capability or type proof by itself.
pub(crate) enum NativeScalarActor {
    Registered(novarocks_execution::exec::expr::function::FunctionKind),
    ArrayLiteral,
    CarrierCast,
    RegexpCount(novarocks_type_contract::RegexpCountPatternSource),
}
pub(crate) fn validate_default_scalar(
    expression: &expr::Expr,
    call: &expr::FunctionCall,
    declared: &FunctionValueType,
    types: &novarocks_plan_codec::execution_type_table::DecodedExecutionTypeTable,
    requests: &NativeOriginalRequests,
    catalog: &novarocks_functions::EngineFunctionCatalog,
    resources: &mut NativeDecodeResources<'_>,
    path: FieldPath,
) -> Result<NativeScalarActor, Error> {
    use novarocks_functions::{FunctionBindingSelection, FunctionKind, FunctionResultType};
    use novarocks_plan_codec::{
        host_projection_v2::ProjectionFailure, physical_binding_v2::MaterializedFunctionBinding,
    };
    let raw = call.frozen_binding.as_ref().ok_or_else(|| {
        Error::missing(
            path.clone().field("frozen_binding"),
            "ordinary scalar complete frozen binding is absent",
        )
    })?;
    // The inherited hard argument bound and Layout overflow precede constructors.
    if raw.arguments.len() > novarocks_functions::MAX_CALL_EFFECT_ARGUMENTS {
        return Err(Construction::Control(
            novarocks_type_contract::CompileControlError::ResourceExhausted,
        )
        .into());
    }
    std::alloc::Layout::array::<novarocks_type_contract::FunctionArgumentType>(raw.arguments.len())
        .map_err(|_| super::arithmetic())?;
    let source_bytes = binding_source_bytes(raw, types)?;
    let header=novarocks_plan_codec::physical_binding_v2::preflight_execution_binding_header_input_derived_in(raw,types,source_bytes,&mut resources.work)
        .map_err(|cause|match cause {
            novarocks_plan_codec::physical_binding_v2::BindingCodecError::Control(cause)=>Error::from(Construction::Control(cause)),
            other=>Error::invalid_value(path.clone().field("frozen_binding"),other),
        })?;
    let original = expression.original_definition_id.ok_or_else(|| {
        Error::missing(
            path.clone(),
            "ordinary scalar original definition is absent",
        )
    })?;
    if raw.id != original {
        return Err(invalid(
            "scalar frozen binding belongs to a different original definition",
        ));
    }
    let request = call.original_request_id.ok_or_else(|| {
        Error::missing(
            path.clone().field("original_request_id"),
            "ordinary scalar original request reference is absent",
        )
    })?;
    step(resources)?;
    requests.validate_definition(
        request,
        novarocks_physical_plan::PhysicalCallDefinition::Expression(
            novarocks_physical_plan::ExprId::new(original),
        ),
        resources,
    )?;
    requests.validate_scalar_actual_arguments(
        request,
        &call.args,
        types,
        resources,
        path.clone().field("args"),
    )?;
    resources.work.flush().map_err(Construction::Control)?;
    use novarocks_type_contract::ValueSchemaCreationFactory;
    let factory = crate::native_type_construction::NativeTypeFactory::new(resources.creation);
    let mut temporary_prefix = 0usize;
    let mut quote_prefix = |layout: std::alloc::Layout| -> Result<(), Construction> {
        let additional =
            temporary_prefix
                .checked_add(layout.size())
                .ok_or(Construction::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            ))?;
        // This is the actual observed constructor layout, added once to the
        // temporary operation overlap. It is not retained source or a grant.
        header
            .coexisting_source_and_request_bytes_upper_bound
            .checked_add(additional)
            .ok_or(Construction::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            ))?;
        temporary_prefix = additional;
        Ok(())
    };
    let mut temporary =
        factory
            .begin_scope_observed(&mut quote_prefix)
            .map_err(|cause| match cause {
                novarocks_type_contract::ValueSchemaScopeBeginFailure::Observer(cause) => {
                    Error::from(cause)
                }
                novarocks_type_contract::ValueSchemaScopeBeginFailure::Creator(cause) => {
                    Error::from(cause)
                }
            })?;
    let materialized =
        novarocks_plan_codec::physical_binding_v2::materialize_execution_binding_with_creator_in(
            raw,
            types,
            &mut temporary,
            &mut resources.work,
        )
        .map_err(|cause| match cause {
            ProjectionFailure::Host(cause) => Error::from(cause),
            ProjectionFailure::Codec(
                novarocks_plan_codec::physical_binding_v2::BindingCodecError::Control(cause),
            ) => Error::from(Construction::Control(cause)),
            ProjectionFailure::Codec(cause) => {
                Error::invalid_value(path.clone().field("frozen_binding"), cause)
            }
        })?;
    let MaterializedFunctionBinding::Scalar(bound) = materialized else {
        return Err(invalid("ordinary scalar frozen binding is not scalar"));
    };
    if bound.kind != FunctionKind::Scalar
        || !declared.exactly_equals_checked_observed(&bound.result_type, || step(resources))?
    {
        return Err(invalid(
            "scalar result differs from its complete frozen declaration",
        ));
    }
    let native_name = novarocks_physical_plan::native_v1_function_name(&bound.function_id)
        .map_err(|_| {
            Error::invalid_value(
                path.clone().field("function_name"),
                "frozen function identity has no canonical native v1 name",
            )
        })?;
    let mut same = call.function_name.len() == native_name.len();
    step(resources)?;
    if same {
        for (actual, expected) in call.function_name.bytes().zip(native_name.bytes()) {
            step(resources)?;
            if !actual.eq_ignore_ascii_case(&expected) {
                same = false;
                break;
            }
        }
    }
    if !same {
        return Err(invalid(
            "default scalar name differs from its exact frozen function identity",
        ));
    }
    let definition = catalog
        .definition_by_id(&bound.function_id)
        .ok_or_else(|| invalid("default scalar frozen installed declaration is absent"))?;
    step(resources)?;
    if definition.kind() != FunctionKind::Scalar {
        return Err(invalid(
            "default scalar installed declaration is not scalar",
        ));
    }
    let selected = FunctionBindingSelection {
        overload: bound.overload,
        argument_types: bound.argument_types,
        result_type: FunctionResultType::Scalar(bound.result_type),
        aggregate: None,
    };
    let original_control = resources.creation;
    requests.with_catalog_request_scope(
        request,
        &mut temporary,
        resources,
        |request, temporary| {
            catalog
                .validate_frozen_selection_with_creator_in(
                    &bound.function_id,
                    FunctionKind::Scalar,
                    &selected,
                    request,
                    temporary,
                    original_control,
                )
                .map_err(|cause| {
                    frozen_validation_failure(cause, path.clone().field("frozen_binding"))
                })
        },
    )?;
    let actor = if bound.function_id.as_str()
        == novarocks_functions::builtin::value_conversion::VALUE_CONVERSION_FUNCTION_ID
    {
        let [novarocks_type_contract::FunctionArgumentType::Value(source)] =
            selected.argument_types.as_ref()
        else {
            return Err(invalid(
                "default value conversion requires one actual value channel",
            ));
        };
        let novarocks_functions::FunctionResultType::Scalar(target) = &selected.result_type else {
            return Err(invalid(
                "default value conversion requires its exact scalar target",
            ));
        };
        // Full original request/constant/target and selected-overload checks
        // succeeded above. Support only existing default CAST operations; a
        // matching Arrow carrier alone never establishes a logical domain.
        if definition.canonical_name()
            != novarocks_functions::builtin::value_conversion::VALUE_CONVERSION_NAME
        {
            return Err(invalid(
                "default opaque conversion differs from its installed owner",
            ));
        }
        use novarocks_functions::builtin::value_conversion as conversion;
        if conversion::is_default_scalar_carrier_cast_overload(&selected.overload) {
            step(resources)?;
            return Ok(NativeScalarActor::CarrierCast);
        }
        let kind = if conversion::is_default_percentile_binary_keep_overload(&selected.overload)
            && conversion::is_default_percentile_binary_keep(source, target)
        {
            novarocks_execution::exec::expr::function::FunctionKind::PercentileToBinary
        } else if conversion::is_default_json_text_keep_overload(&selected.overload)
            && conversion::is_default_json_text_keep(source, target)
        {
            novarocks_execution::exec::expr::function::FunctionKind::JsonToText
        } else {
            return Err(Error::unsupported(
                path.clone(),
                "default host does not support this frozen value-domain conversion",
            ));
        };
        step(resources)?;
        NativeScalarActor::Registered(kind)
    } else if native_name == "__array_literal" {
        if call.function_name != native_name {
            return Err(invalid(
                "native ARRAY constructor requires its canonical spelling",
            ));
        }
        // This original constructor is ExprNode::ArrayExpr, whose evaluator
        // calls the same installed array_literal_core as the catalogue owner.
        // It is not a FunctionKind alias or an additional execution recipe.
        step(resources)?;
        if definition.canonical_name() != "__array_literal" {
            return Err(invalid(
                "frozen ARRAY declaration differs from its actual constructor owner",
            ));
        }
        NativeScalarActor::ArrayLiteral
    } else {
        // Reuse the existing registry for the actual runner, including its aliases.
        // This creates no prepared kernel or proof from a matching output carrier.
        resources.work.flush().map_err(Construction::Control)?;
        let kind = novarocks_execution::exec::expr::function::FUNCTION_REGISTRY
            .get(native_name)
            .copied()
            .ok_or_else(|| {
                Error::unsupported(
                    path.clone().field("function_name"),
                    format!("unsupported native scalar function '{}'", native_name),
                )
            })?;
        step(resources)?;
        // Installed aliases can share the same original runner while their
        // declaration name remains distinct (e.g. __map_element_at). Compare
        // the actual registry actor, not the runner's public display name.
        resources.work.flush().map_err(Construction::Control)?;
        let declaration_actor = novarocks_execution::exec::expr::function::FUNCTION_REGISTRY
            .get(definition.canonical_name())
            .copied();
        step(resources)?;
        if declaration_actor != Some(kind) {
            return Err(invalid(
                "frozen scalar declaration differs from the actual default runner owner",
            ));
        }
        NativeScalarActor::Registered(kind)
    };
    if bound.function_id.as_str() == "builtin.scalar/regexp_count/v1" {
        let [
            _,
            novarocks_type_contract::FunctionArgumentType::Value(pattern_type),
        ] = selected.argument_types.as_ref()
        else {
            return Err(invalid(
                "regexp_count native source requires two exact value arguments",
            ));
        };
        let [_, pattern] = call.args.as_slice() else {
            return Err(invalid(
                "regexp_count native source requires two actual definitions",
            ));
        };
        let source =
            novarocks_type_contract::RegexpCountPatternSource::from_native_v1_emitted_shape(
                pattern_type,
                matches!(pattern.kind, Some(expr::expr::Kind::ConstantReference(_))),
            )
            .map_err(invalid)?;
        if !matches!(
            actor,
            NativeScalarActor::Registered(
                novarocks_execution::exec::expr::function::FunctionKind::String("regexp_count")
            )
        ) {
            return Err(invalid(
                "regexp_count source differs from its validated runner",
            ));
        }
        return Ok(NativeScalarActor::RegexpCount(source));
    }
    Ok(actor)
}
