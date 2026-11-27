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

//! Exact internal value conversions selected by the original FE type owner.
//!
//! Every family explicitly declares its domains and conversion semantics.
//! Common coercion is exact-selection only; ordinary carrier casts stay separate.
//! Binding requires an explicit complete target; there is no default target,
//! name-based source classification or decimal error-policy default. The
//! structural binder uses the existing bounded FVT validation, not a resource
//! admission or cooperative compilation receipt.

use super::binding_control;
mod typed;
use novarocks_type_contract::{CompileCheckpoints, PureCompileControl};
use std::sync::Arc;

use arrow_schema::DataType;
#[cfg(test)]
use arrow_schema::Field;
#[cfg(test)]
use novarocks_type_contract::NR_LOGICAL_TYPE_KEY;
use novarocks_type_contract::{
    ArgumentControl, FunctionEffectDeclaration, FunctionInstanceState, FunctionNullBehavior,
    ObservableEffects, ValueLogicalType, arrow_data_types_exact,
};

use crate::{
    FunctionArgument, FunctionArgumentType, FunctionBindingDeclaration, FunctionBindingError,
    FunctionBindingRequest, FunctionBindingResolver, FunctionBindingSelection,
    FunctionCatalogError, FunctionDefinition, FunctionFailureBehavior, FunctionId,
    FunctionIntrinsicRowError, FunctionKind, FunctionOverloadDeclaration, FunctionOverloadId,
    FunctionResultType, FunctionValueType, FunctionVolatility,
};

pub const VALUE_CONVERSION_NAME: &str = "__value_domain_conversion";
pub const VALUE_CONVERSION_FUNCTION_ID: &str = "builtin.scalar/value_domain_conversion/v1";

/// Only the original ARRAY binder may explicitly select this declared family.
/// Automatic domain resolution deliberately never proposes it.
pub const ARRAY_COMMON_COERCION: &str =
    "builtin.scalar/value_domain_conversion/array_common_coercion/v1";
pub const TEMPORAL_WRITE_ASSIGNMENT: &str =
    "builtin.scalar/value_domain_conversion/nested_temporal_write_assignment/v1";

/// Only a checked writer assignment can request this shape-preserving path.
pub fn temporal_write_assignment_matches(
    source: &FunctionValueType,
    target: &FunctionValueType,
    control: &dyn PureCompileControl,
) -> Result<bool, FunctionBindingError> {
    binding_control::scope(control, |work| {
        pair_matches_observed(TEMPORAL_WRITE_ASSIGNMENT, source, target, work)
    })
}

/// The numeric operator binder selects this family only for its original
/// DECIMAL argument and explicit REAL or DOUBLE target.
pub const OPERATOR_DECIMAL_FLOAT_COERCION: &str =
    "builtin.scalar/value_domain_conversion/operator_decimal_float_coercion/v1";

pub(super) const OPAQUE_BINARY: &str =
    "builtin.scalar/value_domain_conversion/opaque_to_binary_same_structure/v1";
pub(super) const SIGNED_LARGEINT: &str =
    "builtin.scalar/value_domain_conversion/signed_to_largeint/v1";
pub(super) const BOOLEAN_LARGEINT: &str =
    "builtin.scalar/value_domain_conversion/boolean_to_largeint/v1";
pub(super) const DECIMAL128_LARGEINT: &str =
    "builtin.scalar/value_domain_conversion/decimal128_to_largeint/v1";
pub(super) const DECIMAL256_LARGEINT: &str =
    "builtin.scalar/value_domain_conversion/decimal256_to_largeint/v1";
pub(super) const FLOAT32_LARGEINT: &str =
    "builtin.scalar/value_domain_conversion/float32_to_largeint_null_invalid/v1";
pub(super) const FLOAT64_LARGEINT: &str =
    "builtin.scalar/value_domain_conversion/float64_to_largeint_null_invalid/v1";
pub(super) const LARGEINT_SIGNED: &str =
    "builtin.scalar/value_domain_conversion/largeint_to_signed_null_overflow/v1";
pub(super) const LARGEINT_DECIMAL: &str =
    "builtin.scalar/value_domain_conversion/largeint_to_decimal_exact/v1";
pub(super) const LARGEINT_FLOAT: &str =
    "builtin.scalar/value_domain_conversion/largeint_to_float_round/v1";
pub(super) const LARGEINT_TEXT: &str =
    "builtin.scalar/value_domain_conversion/largeint_to_utf8_text/v1";
pub(super) const UTF8_LARGEINT: &str =
    "builtin.scalar/value_domain_conversion/utf8_to_largeint_null_invalid/v1";
pub(super) const NULL_LIFT: &str =
    "builtin.scalar/value_domain_conversion/null_to_typed_nullable/v1";

pub(super) const TEMPORAL: &str = "builtin.scalar/value_domain_conversion/temporal/v1";
pub(super) const TEMPORAL_WIDEN: &str = "builtin.scalar/value_domain_conversion/temporal_widen/v1";
pub(super) const TEMPORAL_ZONE: &str = "builtin.scalar/value_domain_conversion/temporal_zone/v1";
pub(super) const TEMPORAL_CLOCK: &str = "builtin.scalar/value_domain_conversion/temporal_clock/v1";
pub(super) const TRY_TEMPORAL: &str = "builtin.scalar/value_domain_conversion/try_temporal/v1";
pub(super) const TRY_TEMPORAL_ZONE: &str =
    "builtin.scalar/value_domain_conversion/try_temporal_zone/v1";
pub(super) const TRY_TEMPORAL_CLOCK: &str =
    "builtin.scalar/value_domain_conversion/try_temporal_clock/v1";

pub(super) fn temporal_overload(id: &str) -> Option<(crate::temporal_cast::Dependencies, bool)> {
    use crate::temporal_cast::Dependencies as D;
    Some(match id {
        TEMPORAL | TEMPORAL_WIDEN => (D::None, false),
        TEMPORAL_ZONE => (D::Zone, false),
        TEMPORAL_CLOCK => (D::Clock, false),
        TRY_TEMPORAL => (D::None, true),
        TRY_TEMPORAL_ZONE => (D::Zone, true),
        TRY_TEMPORAL_CLOCK => (D::Clock, true),
        _ => return None,
    })
}

pub fn temporal_cast_overload(
    source: &FunctionValueType,
    target: &FunctionValueType,
    safe: bool,
) -> Option<&'static str> {
    if !safe
        && let (Some(source), Some(target)) = (
            source.value_type().temporal(),
            target.value_type().temporal(),
        )
        && source.is_lossless_widening_to(target)
    {
        return Some(TEMPORAL_WIDEN);
    }
    use crate::temporal_cast::Dependencies as D;
    Some(
        match (
            crate::temporal_cast::Pair::bind(source, target)?.dependencies,
            safe,
        ) {
            (D::None, false) => TEMPORAL,
            (D::Zone, false) => TEMPORAL_ZONE,
            (D::Clock, false) => TEMPORAL_CLOCK,
            (D::None, true) => TRY_TEMPORAL,
            (D::Zone, true) => TRY_TEMPORAL_ZONE,
            (D::Clock, true) => TRY_TEMPORAL_CLOCK,
        },
    )
}

/// Selected temporal dependencies are also consumed by SQL and EXL binding
/// authors before the unit projects the original parameter IDs.
pub fn temporal_cast_dependencies(
    id: &str,
) -> &'static [novarocks_type_contract::SemanticParameterKey] {
    temporal_overload(id).map_or(&[], |(dependencies, _)| dependencies.keys())
}

const AUTOMATIC_OVERLOADS: &[&str] = &[
    OPAQUE_BINARY,
    SIGNED_LARGEINT,
    BOOLEAN_LARGEINT,
    DECIMAL128_LARGEINT,
    DECIMAL256_LARGEINT,
    FLOAT32_LARGEINT,
    FLOAT64_LARGEINT,
    LARGEINT_SIGNED,
    LARGEINT_DECIMAL,
    LARGEINT_FLOAT,
    LARGEINT_TEXT,
    UTF8_LARGEINT,
    NULL_LIFT,
    TEMPORAL,
    TEMPORAL_WIDEN,
    TEMPORAL_ZONE,
    TEMPORAL_CLOCK,
];

/// Base effects of the hidden value conversions. Overflow NULL is a
/// result of the signed narrowing overload, not suppression of a child,
/// control, resource, contract or lifecycle failure. Opaque byte values share
/// their original carrier; signed integers extend exactly; floats use the existing i128 `as` rounding;
/// text parse failure is an original successful NULL, never a row error.
pub(super) fn effects() -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: FunctionIntrinsicRowError::NoRowError,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::Strict,
        argument_control: ArgumentControl::OrderedNull,
        instance_state: FunctionInstanceState::None,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::new([]),
    }
}

/// The nominal decimal family refines its frozen overflow policy later.
/// Strict ARRAY common coercion owns conversion Data under every scalar policy;
/// its dedicated preparation never accepts that policy as a semantic switch.
pub(super) fn overload_effects(id: &str) -> FunctionEffectDeclaration {
    if let Some((dependencies, safe)) = temporal_overload(id) {
        let mut base = effects();
        base.environment_dependencies = dependencies.keys().into();
        if safe {
            base.failure_behavior = FunctionFailureBehavior::ReturnsNull;
        } else if id != TEMPORAL_WIDEN {
            base.own_row_error = FunctionIntrinsicRowError::MayRaise;
        }
        return base;
    }

    let mut effects = effects();
    if matches!(
        id,
        LARGEINT_DECIMAL
            | DECIMAL128_LARGEINT
            | DECIMAL256_LARGEINT
            | ARRAY_COMMON_COERCION
            | TEMPORAL_WRITE_ASSIGNMENT
    ) {
        effects.own_row_error = FunctionIntrinsicRowError::MayRaise;
    }
    effects
}

pub(super) mod largeint_decimal;

/// Install the exact internal binder and all of its pure scalar families.
pub fn value_conversion_definition() -> Result<FunctionDefinition, FunctionCatalogError> {
    let (declaration, resolver) = definition_parts()?;
    super::value_conversion_owner::definition(declaration, resolver)
}

pub(super) fn definition_parts()
-> Result<(FunctionBindingDeclaration, ValueConversionResolver), FunctionCatalogError> {
    let invalid = |error: &dyn std::fmt::Display| FunctionCatalogError::InvalidStableIdentity {
        subject: "internal value-domain conversion declaration",
        value: error.to_string().into(),
    };
    let overloads = [
        (OPAQUE_BINARY, "Hll/Bitmap/Object/Percentile/Variant->PhysicalBinary, or identical container shape with those opaque children->PhysicalBinary", "explicit exact target; original bytes and NULL preserved; no inverse domain establishment"),
        (SIGNED_LARGEINT, "Physical(Int8|Int16|Int32|Int64)->LargeInt(FixedSizeBinary16)", "explicit exact LargeInt target; signed i128 extension; NULL preserved"),
        (BOOLEAN_LARGEINT, "Physical Boolean->LargeInt(FixedSizeBinary16)", "explicit exact target; false is zero, true is one; NULL preserved"),
        (DECIMAL128_LARGEINT, "Physical Decimal128->LargeInt(FixedSizeBinary16)", "explicit exact target; truncate toward zero; frozen decimal overflow policy; NULL preserved"),
        (DECIMAL256_LARGEINT, "Physical Decimal256->LargeInt(FixedSizeBinary16)", "explicit exact target; truncate toward zero; frozen decimal overflow policy; NULL preserved"),
        (FLOAT32_LARGEINT, "Physical Float32->LargeInt(FixedSizeBinary16)", "explicit nullable exact target; truncate toward zero; non-finite or out-of-range values return NULL"),
        (FLOAT64_LARGEINT, "Physical Float64->LargeInt(FixedSizeBinary16)", "explicit nullable exact target; truncate toward zero; non-finite or out-of-range values return NULL"),
        (LARGEINT_SIGNED, "LargeInt(FixedSizeBinary16)->Physical(Int8|Int16|Int32|Int64)", "explicit nullable exact target; out-of-range value returns NULL; NULL preserved"),
        (LARGEINT_DECIMAL, "LargeInt(FixedSizeBinary16)->Physical(Decimal128|Decimal256)", "explicit exact decimal target; checked integer scaling; negative scale truncates toward zero; frozen decimal overflow policy; NULL preserved"),
        (LARGEINT_FLOAT, "LargeInt(FixedSizeBinary16)->Physical(Float32|Float64)", "explicit exact target; existing i128 as float rounding; NULL preserved"),
        (LARGEINT_TEXT, "LargeInt(FixedSizeBinary16)->Physical(Utf8)", "explicit exact target; original signed big-endian i128 text; NULL preserved"),
        (UTF8_LARGEINT, "Physical Utf8->LargeInt(FixedSizeBinary16)", "explicit nullable exact LargeInt target; trim then signed i128 parse; invalid or out-of-range text returns NULL; NULL preserved"),
        (NULL_LIFT, "Physical Null(nullable=true)->explicit complete nullable target", "evaluate child eagerly; strict NULL protocol produces typed NULL without row implementation"),
        (TEMPORAL, "exact temporal source and target; strict, independent of statement time", "checked selected temporal conversion, no statement dependencies"),
        (TEMPORAL_WIDEN, "same datetime family and unchanged or wider fractional precision", "exact datetime value unchanged; no statement dependencies or row errors"),
        (TEMPORAL_ZONE, "exact temporal source and target; strict, depends on the effective time zone", "checked selected temporal conversion with captured time zone"),
        (TEMPORAL_CLOCK, "exact temporal source and target; strict, depends on effective zone and start", "checked selected temporal conversion with captured start and zone"),
        (TRY_TEMPORAL, "exact temporal source and nullable target; own row capture, independent of statement time", "capture only own temporal row Data"),
        (TRY_TEMPORAL_ZONE, "exact temporal source and nullable target; own row capture with effective time zone", "capture only own temporal row Data with captured time zone"),
        (TRY_TEMPORAL_CLOCK, "exact temporal source and nullable target; own row capture with effective zone and start", "capture only own temporal row Data with captured start and zone"),
        (ARRAY_COMMON_COERCION, "original complete source->independently inferred ARRAY common element target; exact selection only", "strict Trino common coercion; preserve source NULL/domain/layout obligations; conversion Data propagates; never an automatic domain candidate"),
        (TEMPORAL_WRITE_ASSIGNMENT, "exact same-shape Iceberg Array/Map/Row with temporal precision changes; exact selection only", "round reachable temporal leaves once; retain target geometry; conversion Data propagates"),
        (OPERATOR_DECIMAL_FLOAT_COERCION, "Physical Decimal128(1<=p<=38,0<=s<=p)->Physical(Float32|Float64); exact selection only", "explicit operand target; round directly to its IEEE width; preserve NULL"),
    ]
    .into_iter()
    .map(|(id, argument_pattern, result_pattern)| {
        Ok(FunctionOverloadDeclaration::from_effects(
            FunctionOverloadId::try_new(id).map_err(|error| invalid(&error))?,
            argument_pattern,
            result_pattern,
            None,
            overload_effects(id),
        ))
    })
    .collect::<Result<Vec<_>, FunctionCatalogError>>()?;
    let declaration = FunctionBindingDeclaration::try_new_complete(
        FunctionId::try_new(VALUE_CONVERSION_FUNCTION_ID).map_err(|error| invalid(&error))?,
        FunctionKind::Scalar,
        overloads,
    )
    .map_err(|error| invalid(&error))?;
    Ok((declaration, ValueConversionResolver))
}

pub(crate) struct ValueConversionResolver;

/// Check an assignment through this exact implementation-owner binder. The
/// returned selection still needs the registered function identity, FE
/// materialization and installed kernel coverage before execution.
pub fn resolve_value_conversion(
    source: &FunctionValueType,
    target: &FunctionValueType,
    control: &dyn PureCompileControl,
) -> Result<FunctionBindingSelection, FunctionBindingError> {
    binding_control::scope(control, |work| {
        binding_control::value_type(source, work)?;
        binding_control::value_type(target, work)?;
        let arguments = [FunctionArgument::Value {
            value_type: source.clone(),
            constant: None,
        }];
        work.flush()?;
        ValueConversionResolver.resolve(
            FunctionBindingRequest {
                arguments: &arguments,
                logical_argument_count: 1,
                expected_result_type: Some(target),
            },
            work.control(),
        )
    })
}

/// Author one explicit ARRAY common coercion from the installed declaration.
/// Automatic nominal conversion selection never chooses this family.
pub fn resolve_array_common_coercion_binding(
    catalog: &crate::EngineFunctionCatalog,
    argument: &FunctionArgument,
    target: &FunctionValueType,
    control: &dyn PureCompileControl,
) -> Result<crate::ResolvedFunctionBinding, FunctionBindingError> {
    binding_control::scope(control, |work| {
        let definition = catalog
            .definition(VALUE_CONVERSION_NAME, FunctionKind::Scalar)
            .ok_or(FunctionBindingError::UnknownFunction)?;
        work.step()?;
        let declaration = definition
            .binding_declaration()
            .ok_or(FunctionBindingError::MissingBindingDeclaration)?;
        let mut exact = None;
        for overload in declaration.overloads() {
            work.step()?;
            if overload.identity.as_str() == ARRAY_COMMON_COERCION {
                exact = Some(&overload.identity);
                break;
            }
        }
        let overload = exact.ok_or(FunctionBindingError::MissingBindingDeclaration)?;
        work.flush()?;
        let bound = catalog.resolve_bound_trusted_at_overload(
            declaration.function_id(),
            FunctionKind::Scalar,
            overload,
            FunctionBindingRequest {
                arguments: std::slice::from_ref(argument),
                logical_argument_count: 1,
                expected_result_type: Some(target),
            },
            work.control(),
        )?;
        work.flush()?;
        Ok(bound)
    })
}

/// The original declaration's family decision, before any selection backing.
/// Authorization borrows this decision; real publication uses selection above.
pub(super) fn recipe_family(
    source: &FunctionValueType,
    target: &FunctionValueType,
) -> Result<&'static str, FunctionBindingError> {
    let arguments = [FunctionArgument::Value {
        value_type: source.clone(),
        constant: None,
    }];
    let (source, target) = inputs(FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: 1,
        expected_result_type: Some(target),
    })?;
    AUTOMATIC_OVERLOADS
        .iter()
        .copied()
        .find(|id| pair_matches(id, source, target))
        .ok_or(FunctionBindingError::NoMatchingOverload)
}

fn selection(
    id: &str,
    source: &FunctionValueType,
    target: &FunctionValueType,
) -> Result<FunctionBindingSelection, FunctionBindingError> {
    Ok(FunctionBindingSelection {
        overload: FunctionOverloadId::try_new(id)
            .map_err(|_| invalid("invalid conversion overload identity"))?,
        argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
            vec![FunctionArgumentType::Value(source.clone())].into_boxed_slice(),
        ),
        result_type: FunctionResultType::Scalar(target.clone()),
        aggregate: None,
    })
}

/// Same-domain carrier coercion remains with the existing type owner. A
/// decided logical-domain change must be admitted by the exact conversion
/// owner; untyped NULL does not establish a domain and follows the variable.
pub(super) fn assignment_domains_authorized_with_construction<E>(
    source: &FunctionValueType,
    target: &FunctionValueType,
    creation: &mut super::scalar_construction::ScalarConstruction<'_, E>,
) -> Result<bool, super::scalar_construction::Failure<E>> {
    match conversion_intermediate_type_with_construction(source, target, creation) {
        Ok(_) => Ok(true),
        Err(super::scalar_construction::OwnerFailure::Binding(_)) => Ok(false),
        Err(super::scalar_construction::OwnerFailure::Construction(error)) => match error {
            super::scalar_construction::Failure::ValueType(_)
            | super::scalar_construction::Failure::Rule(_) => Ok(false),
            error => Err(error),
        },
    }
}
/// Return the exact first conversion result for a two-stage assignment. The
/// source-owned field names, nullability and metadata survive this first stage;
/// an ordinary same-domain carrier CAST may then reach `target`. `None` means
/// no decided domain changes. Physical NULL has an explicit lift binding too:
/// the eager child is evaluated before the strict typed-NULL result protocol.
/// The FE still supplies its actual decimal policy
/// to that CAST; this helper does not choose one or install either kernel.
pub fn conversion_intermediate_type(
    source: &FunctionValueType,
    target: &FunctionValueType,
) -> Result<Option<FunctionValueType>, FunctionBindingError> {
    conversion_intermediate_type_with_construction(
        source,
        target,
        &mut super::scalar_construction::ScalarConstruction::<std::convert::Infallible>::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            None,
        ),
    )
    .map_err(super::scalar_construction::OwnerFailure::into_pure)
}
pub(super) fn conversion_intermediate_type_with_construction<E>(
    source: &FunctionValueType,
    target: &FunctionValueType,
    creation: &mut super::scalar_construction::ScalarConstruction<'_, E>,
) -> Result<Option<FunctionValueType>, super::scalar_construction::OwnerFailure<E>> {
    source
        .validate()
        .map_err(|_| invalid("invalid assignment source type"))?;
    target
        .validate()
        .map_err(|_| invalid("invalid assignment target type"))?;
    if source.nullable() && !target.nullable() {
        return Err(invalid("assignment target cannot narrow source nullability").into());
    }
    if source.data_type() == &DataType::Null && source.logical_type() == ValueLogicalType::Physical
    {
        recipe_family(source, target)?;
        return Ok(Some(target.clone()));
    }
    if source.logical_type() == target.logical_type()
        && typed::domains_preserved(source.value_type(), target.value_type())
    {
        return Ok(None);
    }
    if recipe_family(source, target).is_ok() {
        return Ok(Some(target.clone()));
    }
    let intermediate = if source.logical_type() == ValueLogicalType::LargeInt
        && source.data_type() == &DataType::FixedSizeBinary(16)
        && target.logical_type() == ValueLogicalType::Physical
        && target.data_type() == &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        && target.nullable()
    {
        // Original LARGEINT -> DATETIME is checked i128 -> i64, followed by
        // the existing numeric calendar grammar. Construct the intermediate
        // with this caller's actual owner; no same-carrier nominal erasure or
        // new conversion family is authorized by this composition.
        creation.value(DataType::Int64, true, ValueLogicalType::Physical)?
    } else {
        typed::same_shape_intermediate_with_construction(source, target, creation)?
    };
    recipe_family(source, &intermediate)?;
    if intermediate.logical_type() != target.logical_type()
        || (intermediate.nullable() && !target.nullable())
        || !typed::domains_preserved(intermediate.value_type(), target.value_type())
        || !(arrow_data_types_exact(intermediate.data_type(), target.data_type())
            || arrow_cast::can_cast_types(intermediate.data_type(), target.data_type()))
    {
        return Err(invalid(
            "conversion intermediate cannot reach the selected same-domain carrier target",
        )
        .into());
    }
    Ok(Some(intermediate))
}

fn invalid(message: &str) -> FunctionBindingError {
    FunctionBindingError::InvalidBinding(message.into())
}

fn inputs<'a>(
    request: FunctionBindingRequest<'a>,
) -> Result<(&'a FunctionValueType, &'a FunctionValueType), FunctionBindingError> {
    let (source, target) = input_shape(request)?;
    source
        .validate()
        .map_err(|_| invalid("invalid conversion source type"))?;
    target
        .validate()
        .map_err(|_| invalid("invalid conversion target type"))?;
    Ok((source, target))
}

fn input_shape<'a>(
    request: FunctionBindingRequest<'a>,
) -> Result<(&'a FunctionValueType, &'a FunctionValueType), FunctionBindingError> {
    if request.logical_argument_count != 1 || request.arguments.len() != 1 {
        return Err(FunctionBindingError::NoMatchingOverload);
    }
    let FunctionArgument::Value {
        value_type: source, ..
    } = &request.arguments[0]
    else {
        return Err(FunctionBindingError::NoMatchingOverload);
    };
    let target = request.expected_result_type.ok_or_else(|| {
        invalid("internal value-domain conversion requires an explicit complete target")
    })?;
    if source.nullable() && !target.nullable() {
        return Err(invalid(
            "conversion target cannot narrow source nullability",
        ));
    }
    Ok((source, target))
}

fn signed(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

pub(super) fn decimal_to_largeint_can_overflow(data_type: &DataType) -> bool {
    match data_type {
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            // The complete signed range contains every coefficient with at most 38 digits.
            i16::from(*precision) - i16::from(*scale) > 38
        }
        _ => true,
    }
}

/// Match the exact selected family directly. No resolution replay is used by
/// frozen validation. In particular FixedSizeBinary16 is not a numeric source.
fn pair_matches(id: &str, source: &FunctionValueType, target: &FunctionValueType) -> bool {
    if let Some((_, safe)) = temporal_overload(id) {
        return (!safe || target.nullable())
            && (!source.nullable() || target.nullable())
            && temporal_cast_overload(source, target, safe) == Some(id);
    }
    match id {
        OPAQUE_BINARY => typed::same_shape_pair(source, target, &mut || Ok(()))
            .ok()
            .flatten()
            .is_some_and(|changed| changed),
        SIGNED_LARGEINT => {
            source.logical_type() == ValueLogicalType::Physical
                && signed(source.data_type())
                && target.logical_type() == ValueLogicalType::LargeInt
                && target.data_type() == &DataType::FixedSizeBinary(16)
        }
        BOOLEAN_LARGEINT | DECIMAL128_LARGEINT | DECIMAL256_LARGEINT | FLOAT32_LARGEINT
        | FLOAT64_LARGEINT => {
            source.logical_type() == ValueLogicalType::Physical
                && target.logical_type() == ValueLogicalType::LargeInt
                && target.data_type() == &DataType::FixedSizeBinary(16)
                && match (id, source.data_type()) {
                    (BOOLEAN_LARGEINT, DataType::Boolean) => true,
                    (DECIMAL128_LARGEINT, DataType::Decimal128(..))
                    | (DECIMAL256_LARGEINT, DataType::Decimal256(..)) => {
                        target.nullable() || !decimal_to_largeint_can_overflow(source.data_type())
                    }
                    (FLOAT32_LARGEINT, DataType::Float32)
                    | (FLOAT64_LARGEINT, DataType::Float64) => target.nullable(),
                    _ => false,
                }
        }
        LARGEINT_SIGNED => {
            source.logical_type() == ValueLogicalType::LargeInt
                && source.data_type() == &DataType::FixedSizeBinary(16)
                && target.logical_type() == ValueLogicalType::Physical
                && signed(target.data_type())
                && target.nullable()
        }
        LARGEINT_DECIMAL => {
            source.logical_type() == ValueLogicalType::LargeInt
                && source.data_type() == &DataType::FixedSizeBinary(16)
                && target.logical_type() == ValueLogicalType::Physical
                && largeint_decimal::DecimalRecipe::try_new(target.data_type())
                    .is_some_and(|recipe| target.nullable() || !recipe.can_overflow())
        }
        LARGEINT_FLOAT => {
            source.logical_type() == ValueLogicalType::LargeInt
                && source.data_type() == &DataType::FixedSizeBinary(16)
                && target.logical_type() == ValueLogicalType::Physical
                && matches!(target.data_type(), DataType::Float32 | DataType::Float64)
        }
        LARGEINT_TEXT => {
            source.logical_type() == ValueLogicalType::LargeInt
                && source.data_type() == &DataType::FixedSizeBinary(16)
                && target.logical_type() == ValueLogicalType::Physical
                && target.data_type() == &DataType::Utf8
        }
        UTF8_LARGEINT => {
            source.logical_type() == ValueLogicalType::Physical
                && source.data_type() == &DataType::Utf8
                && target.logical_type() == ValueLogicalType::LargeInt
                && target.data_type() == &DataType::FixedSizeBinary(16)
                && target.nullable()
        }
        OPERATOR_DECIMAL_FLOAT_COERCION => operator_decimal_float_types(source, target).is_some(),
        NULL_LIFT => {
            source.logical_type() == ValueLogicalType::Physical
                && source.data_type() == &DataType::Null
                && source.nullable()
                && target.nullable()
        }
        _ => false,
    }
}

pub(super) fn operator_decimal_float_types(
    source: &FunctionValueType,
    target: &FunctionValueType,
) -> Option<(u8, u8, crate::cast::TrinoFloatWidth)> {
    if source.logical_type() != ValueLogicalType::Physical
        || target.logical_type() != ValueLogicalType::Physical
        || (source.nullable() && !target.nullable())
    {
        return None;
    }
    let DataType::Decimal128(precision, scale) = *source.data_type() else {
        return None;
    };
    if !(1..=38).contains(&precision) || !(0..=precision as i8).contains(&scale) {
        return None;
    }
    let width = match target.data_type() {
        DataType::Float32 => crate::cast::TrinoFloatWidth::F32,
        DataType::Float64 => crate::cast::TrinoFloatWidth::F64,
        _ => return None,
    };
    Some((precision, scale as u8, width))
}

fn input_pair_observed<'a>(
    request: FunctionBindingRequest<'a>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(&'a FunctionValueType, &'a FunctionValueType), FunctionBindingError> {
    binding_control::request_types(request, work)?;
    input_shape(request)
}

fn require_selected_pair_observed(
    overload: &FunctionOverloadId,
    source: &FunctionValueType,
    target: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FunctionBindingError> {
    if !pair_matches_observed(overload.as_str(), source, target, work)? {
        return Err(invalid(
            "frozen value-domain conversion differs from its exact source, target or overload",
        ));
    }
    Ok(())
}

impl FunctionBindingResolver for ValueConversionResolver {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| {
            binding_control::request_types(request, work)?;
            let (source, target) = input_shape(request)?;
            for &id in AUTOMATIC_OVERLOADS {
                work.step()?;
                if pair_matches_observed(id, source, target, work)? {
                    return selection(id, source, target);
                }
            }
            Err(FunctionBindingError::NoMatchingOverload)
        })
    }
    fn select_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| {
            let (source, target) = input_pair_observed(request, work)?;
            require_selected_pair_observed(overload, source, target, work)?;
            selection(overload.as_str(), source, target)
        })
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        binding_control::scope(control, |work| {
            let (source, target) = input_pair_observed(request, work)?;
            let arguments_match = match selected.argument_types.as_ref() {
                [FunctionArgumentType::Value(actual)] => {
                    binding_control::exact_type(actual, source, work)?
                }
                _ => false,
            };
            let result_matches = match &selected.result_type {
                FunctionResultType::Scalar(actual) => {
                    binding_control::exact_type(actual, target, work)?
                }
                _ => false,
            };
            if selected.aggregate.is_some() || !arguments_match || !result_matches {
                return Err(invalid(
                    "frozen value-domain conversion differs from its exact source, target or overload",
                ));
            }
            require_selected_pair_observed(&selected.overload, source, target, work)
        })
    }
}

fn pair_matches_observed(
    id: &str,
    source: &FunctionValueType,
    target: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, FunctionBindingError> {
    work.step()?;
    if id == TEMPORAL_WRITE_ASSIGNMENT {
        return super::value_conversion_kernel::temporal_assignment::matches(
            source,
            target,
            &mut || work.step().map_err(FunctionBindingError::from),
        );
    }
    if id == ARRAY_COMMON_COERCION {
        return novarocks_type_contract::array_common_coercion_types_valid_observed(
            source,
            target,
            || work.step().map_err(FunctionBindingError::from),
        );
    }
    if id != OPAQUE_BINARY {
        return Ok(pair_matches(id, source, target));
    }
    Ok(
        typed::same_shape_pair(source, target, &mut || work.step().map_err(Into::into))?
            .is_some_and(|changed| changed),
    )
}

#[cfg(test)]
mod tests;

/// The sole unchanged computation for a validated SameShape/Keep operation.
/// This clones an existing Arc: no ArrayData, Field, array/header or buffer is
/// constructed. It grants no conversion capability; callers must establish
/// their own exact selected/source contract before selecting this operation.
pub fn share_same_structure_keep(array: &arrow_array::ArrayRef) -> arrow_array::ArrayRef {
    Arc::clone(array)
}

/// The current default host's exact original opaque CAST leaf. Other opaque
/// containers and widths remain outside this opaque host port. This is a
/// support check, not an overload resolver or proof.
pub fn is_default_percentile_binary_keep(
    source: &FunctionValueType,
    target: &FunctionValueType,
) -> bool {
    source.logical_type() == ValueLogicalType::Percentile
        && source.data_type() == &DataType::Binary
        && target.logical_type() == ValueLogicalType::Physical
        && matches!(
            target.value_type().kind(),
            novarocks_type_contract::ValueKind::Binary
        )
        && target.data_type() == &DataType::Binary
        && source.nullable() == target.nullable()
}
pub fn is_default_percentile_binary_keep_overload(overload: &FunctionOverloadId) -> bool {
    overload.as_str() == OPAQUE_BINARY
}

/// These exact installed conversions reuse the original default scalar CAST.
/// The caller must first validate the complete frozen request and selection.
/// Opaque structure conversions do not enter this dispatch by carrier.
pub fn is_default_scalar_carrier_cast_overload(overload: &FunctionOverloadId) -> bool {
    matches!(
        overload.as_str(),
        SIGNED_LARGEINT
            | LARGEINT_SIGNED
            | LARGEINT_DECIMAL
            | LARGEINT_FLOAT
            | LARGEINT_TEXT
            | UTF8_LARGEINT
            | NULL_LIFT
    )
}
