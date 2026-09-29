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

//! Function call resolver: given `(name, arg_types)`, find the best
//! matching [`Signature`] in the registry and return its instantiated
//! parameter and return types.
//!
//! Resolution proceeds in passes that mirror the structure of
//! StarRocks' `FunctionSet.getFunction`:
//!
//! 1. **Strict match.** Every parameter spec must `anchor_matches` the
//!    corresponding argument type — no implicit casting, no type variables
//!    needed. Picks the first registered signature that strict-matches.
//! 2. **Polymorphic match.** If no strict match was found, try unifying
//!    each spec against the argument type, allowing `Any(name)` variants
//!    to bind. The first signature whose every spec unifies wins; its
//!    parameter and return types are then realised by substituting the
//!    bindings.
//! 3. **Limited concrete cast match.** Only signatures that opt in to
//!    argument coercion can use an explicit anchor cast. The current policy
//!    only accepts integral or NULL arguments for an `Int32` target; it is
//!    not a general implicit-cast pass.
//! 4. **Polymorphic widening.** Signatures that opt in to widening can
//!    merge repeated `Any(name)` bindings through `wider_type`.

use arrow::datatypes::DataType;
use novarocks_functions::{FunctionResolutionError, ResolvedFunctionSignature};

use super::registry;
use super::signature::{BindMode, Bindings, Signature, TypeSpec, anchor_matches, realize, unify};

pub(crate) type ResolvedScalarFunction = ResolvedFunctionSignature;

/// Why a function call could not be resolved against the registry.
///
/// Callers use this to decide whether to surface an error or retain a legacy
/// hand-written `infer_*` fallback. A caller that supports argument binding
/// must inspect `NoMatchingSignature::binding_enforced` before taking a
/// fallback path, so an opt-in signature policy cannot be bypassed.
pub(crate) type ResolveError = FunctionResolutionError;

/// Resolve a scalar function call to its instantiated signature.
///
/// When `enforce_argument_binding` is true, callers must bind each argument
/// to `argument_types`. `UnknownFunction` still permits the legacy
/// `infer_*` fallback; callers must inspect `NoMatchingSignature` before
/// using that fallback for a registered function.
pub(crate) fn resolve_scalar_function_signature(
    name: &str,
    arg_types: &[DataType],
) -> Result<ResolvedScalarFunction, ResolveError> {
    resolve_scalar_function_signature_with_overload(name, arg_types).map(|(_, resolved)| resolved)
}

pub(crate) fn resolve_scalar_function_signature_with_overload(
    name: &str,
    arg_types: &[DataType],
) -> Result<(usize, ResolvedScalarFunction), ResolveError> {
    let candidates = registry::scalar_signatures(name).ok_or(ResolveError::UnknownFunction)?;

    // Pass 1: strict — every spec anchor-matches the concrete argument.
    for (index, sig) in candidates.iter().enumerate() {
        if strict_matches(sig, arg_types) {
            return resolved_signature(sig, arg_types, &Bindings::default())
                .map(|resolved| (index, resolved));
        }
    }

    // Pass 2: polymorphic-strict — `Any(name)` binds with equality.
    // Same name occurring twice must bind to the same concrete type.
    for (index, sig) in candidates.iter().enumerate() {
        let mut bindings = Bindings::default();
        if polymorphic_matches(sig, arg_types, &mut bindings, BindMode::Strict) {
            return resolved_signature(sig, arg_types, &bindings).map(|resolved| (index, resolved));
        }
    }

    // Pass 3: lossless integer widening. A position that declares BIGINT
    // describes what the executor reads, not what the caller must already
    // have written; an INT reaching it loses nothing on the way up, and the
    // caller coerces every argument to the selected types anyway. Without
    // this, a narrower integer -- which is what an expression like `idx % 3`
    // or a column of a smaller width produces -- had to be spelled out as its
    // own overload for every function that takes a count, an offset or a
    // position. Strictly widening only: a spec never accepts an actual it
    // could not hold.
    for (index, sig) in candidates.iter().enumerate() {
        if integer_widening_matches(sig, arg_types) {
            return resolved_signature(sig, arg_types, &Bindings::default())
                .map(|resolved| (index, resolved));
        }
    }

    // Pass 4: limited concrete casts for signatures that explicitly require
    // the resulting parameter targets to be enforced by the caller.
    for (index, sig) in candidates.iter().enumerate() {
        if concrete_cast_matches(sig, arg_types) {
            return resolved_signature(sig, arg_types, &Bindings::default())
                .map(|resolved| (index, resolved));
        }
    }

    // Pass 4: polymorphic-widening. Only
    // signatures explicitly registered with `with_widening()` opt in
    // — e.g. `coalesce(Any("T"), ...) -> Any("T")`. Structural
    // polymorphic signatures like `array_append(List<T>, T) -> List<T>`
    // are deliberately excluded so a mismatched element type fails the
    // resolver instead of silently widening through the type variable.
    for (index, sig) in candidates.iter().enumerate() {
        if !sig.widening {
            continue;
        }
        let mut bindings = Bindings::default();
        if polymorphic_matches(sig, arg_types, &mut bindings, BindMode::Widening) {
            return resolved_signature(sig, arg_types, &bindings).map(|resolved| (index, resolved));
        }
    }

    Err(ResolveError::NoMatchingSignature {
        candidates: candidates.len(),
        binding_enforced: binding_enforced_for_arity(candidates, arg_types.len()),
    })
}

/// Validate one catalog-selected overload without running overload selection.
/// This is the BE-side path for a frozen binding identity.
pub(crate) fn resolve_scalar_function_signature_at_overload(
    name: &str,
    overload_index: usize,
    arg_types: &[DataType],
) -> Result<ResolvedScalarFunction, ResolveError> {
    let candidates = registry::scalar_signatures(name).ok_or(ResolveError::UnknownFunction)?;
    let signature = candidates
        .get(overload_index)
        .ok_or_else(|| ResolveError::BadSignature("selected overload index is unknown".into()))?;

    if strict_matches(signature, arg_types) {
        return resolved_signature(signature, arg_types, &Bindings::default());
    }
    let mut bindings = Bindings::default();
    if polymorphic_matches(signature, arg_types, &mut bindings, BindMode::Strict) {
        return resolved_signature(signature, arg_types, &bindings);
    }
    if concrete_cast_matches(signature, arg_types) {
        return resolved_signature(signature, arg_types, &Bindings::default());
    }
    if signature.widening {
        let mut bindings = Bindings::default();
        if polymorphic_matches(signature, arg_types, &mut bindings, BindMode::Widening) {
            return resolved_signature(signature, arg_types, &bindings);
        }
    }
    Err(ResolveError::NoMatchingSignature {
        candidates: 1,
        binding_enforced: signature.argument_binding.is_enforced(),
    })
}

fn binding_enforced_for_arity(candidates: &[Signature], n_args: usize) -> bool {
    let mut has_relevant_candidate = false;
    let mut binding_enforced = false;

    for signature in candidates.iter().filter(|sig| check_arity(sig, n_args)) {
        has_relevant_candidate = true;
        binding_enforced |= signature.argument_binding.is_enforced();
    }

    if has_relevant_candidate {
        binding_enforced
    } else {
        candidates
            .iter()
            .all(|signature| signature.argument_binding.is_enforced())
    }
}

/// Resolve a scalar function call to its return type.
///
/// This compatibility wrapper preserves the existing return-type-only API.
/// It retains the underlying `ResolveError`, so callers using a legacy
/// fallback can still honor `NoMatchingSignature::binding_enforced`.
#[allow(
    dead_code,
    reason = "Retained for staged SQL planner migration consumers and test helpers."
)]
pub(crate) fn resolve_scalar_function(
    name: &str,
    arg_types: &[DataType],
) -> Result<DataType, ResolveError> {
    resolve_scalar_function_signature(name, arg_types).map(|resolved| resolved.return_type)
}

fn resolved_signature(
    sig: &Signature,
    arg_types: &[DataType],
    bindings: &Bindings,
) -> Result<ResolvedScalarFunction, ResolveError> {
    let return_type = realize(&sig.ret, bindings).map_err(ResolveError::BadSignature)?;
    let argument_types = arg_types
        .iter()
        .enumerate()
        .map(|(idx, actual)| realize_argument_type(signature_spec_at(sig, idx), bindings, actual))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ResolvedScalarFunction {
        return_type,
        argument_types,
        enforce_argument_binding: sig.argument_binding.is_enforced(),
    })
}

fn realize_argument_type(
    spec: &TypeSpec,
    bindings: &Bindings,
    actual: &DataType,
) -> Result<DataType, ResolveError> {
    match spec {
        TypeSpec::AnyDecimal128 | TypeSpec::AnyType => Ok(actual.clone()),
        _ => realize(spec, bindings).map_err(ResolveError::BadSignature),
    }
}

/// True iff every `arg_types[i]` `anchor_matches` `sig.args[i]` (with
/// variadic tails handled).
fn strict_matches(sig: &Signature, arg_types: &[DataType]) -> bool {
    if !check_arity(sig, arg_types.len()) {
        return false;
    }
    for (idx, dt) in arg_types.iter().enumerate() {
        let spec = signature_spec_at(sig, idx);
        if !anchor_matches(spec, dt) {
            return false;
        }
    }
    true
}

/// True iff every spec unifies (anchor- or variable-binding) with the
/// corresponding argument, under the given `BindMode`.
fn polymorphic_matches(
    sig: &Signature,
    arg_types: &[DataType],
    bindings: &mut Bindings,
    mode: BindMode,
) -> bool {
    if !check_arity(sig, arg_types.len()) {
        return false;
    }
    for (idx, dt) in arg_types.iter().enumerate() {
        let spec = signature_spec_at(sig, idx);
        if !unify(spec, dt, bindings, mode) {
            return false;
        }
    }
    true
}

/// True iff every argument either anchor-matches its spec or is an integer
/// that the spec's own integer type can hold without loss.
fn integer_widening_matches(sig: &Signature, arg_types: &[DataType]) -> bool {
    if !check_arity(sig, arg_types.len()) {
        return false;
    }
    let mut widened_any = false;
    let matched = arg_types.iter().enumerate().all(|(idx, actual)| {
        let spec = signature_spec_at(sig, idx);
        if anchor_matches(spec, actual) {
            return true;
        }
        if integer_widens_losslessly(spec, actual) {
            widened_any = true;
            return true;
        }
        false
    });
    // A signature that matched without widening anything was already decided
    // by an earlier pass; only reach here for one this pass actually rescued.
    matched && widened_any
}

fn integer_widens_losslessly(spec: &TypeSpec, actual: &DataType) -> bool {
    let width = |data_type: &DataType| match data_type {
        DataType::Int8 => Some(1u8),
        DataType::Int16 => Some(2),
        DataType::Int32 => Some(4),
        DataType::Int64 => Some(8),
        _ => None,
    };
    let spec_width = match spec {
        TypeSpec::Int8 => 1u8,
        TypeSpec::Int16 => 2,
        TypeSpec::Int32 => 4,
        TypeSpec::Int64 => 8,
        _ => return false,
    };
    width(actual).is_some_and(|actual_width| actual_width < spec_width)
}

fn concrete_cast_matches(sig: &Signature, arg_types: &[DataType]) -> bool {
    if !sig.argument_binding.is_enforced() || !check_arity(sig, arg_types.len()) {
        return false;
    }
    arg_types.iter().enumerate().all(|(idx, actual)| {
        let spec = signature_spec_at(sig, idx);
        anchor_matches(spec, actual) || implicit_anchor_cast_target(spec, actual).is_some()
    })
}

/// Return the target of the resolver's intentionally limited concrete cast.
/// This is not a general range check or implicit-casting framework.
///
/// An integer position on an opt-in signature absorbs the values SQL reads as
/// a whole number: the integers, a boolean, an approximate or exact decimal
/// (truncated toward zero), and a string (NULL when it does not spell one).
/// Spelling each of those as its own overload would multiply every function
/// that takes a count, an offset, a position or an interval by the number of
/// ways a caller can write a number, and the caller coerces every argument to
/// the selected types anyway. The cast is still the ordinary one, so a value
/// the target cannot hold becomes NULL rather than a different number.
fn implicit_anchor_cast_target(spec: &TypeSpec, actual: &DataType) -> Option<DataType> {
    let target = match spec {
        TypeSpec::Int32 => DataType::Int32,
        TypeSpec::Int64 => DataType::Int64,
        _ => return None,
    };
    reads_as_whole_number(actual).then_some(target)
}

fn reads_as_whole_number(actual: &DataType) -> bool {
    matches!(
        actual,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Boolean
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Null
    )
}

fn check_arity(sig: &Signature, n_args: usize) -> bool {
    if sig.variadic {
        // At least the non-variadic prefix must be present; the last spec
        // covers all trailing positions.
        !sig.args.is_empty() && n_args >= sig.args.len() - 1
    } else {
        sig.args.len() == n_args
    }
}

fn signature_spec_at(sig: &Signature, idx: usize) -> &super::signature::TypeSpec {
    if sig.variadic && idx >= sig.args.len() - 1 {
        sig.args
            .last()
            .expect("variadic signature must have at least one spec")
    } else {
        &sig.args[idx]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::datatypes::{Field, TimeUnit};

    fn list_of(item: DataType) -> DataType {
        DataType::List(Arc::new(Field::new("item", item, true)))
    }

    #[test]
    fn resolve_unknown_function_returns_unknown_function_error() {
        let r = resolve_scalar_function("definitely_not_a_real_function", &[DataType::Int64]);
        assert_eq!(r, Err(ResolveError::UnknownFunction));
    }

    #[test]
    fn resolve_upper_returns_utf8() {
        let r = resolve_scalar_function("upper", &[DataType::Utf8]);
        assert_eq!(r, Ok(DataType::Utf8));
    }

    #[test]
    fn resolve_upper_with_wrong_arity_does_not_strict_match() {
        // `upper()` has only the `(Utf8) -> Utf8` signature; passing two
        // args should give NoMatchingSignature, not Ok.
        let r = resolve_scalar_function("upper", &[DataType::Utf8, DataType::Utf8]);
        assert!(matches!(r, Err(ResolveError::NoMatchingSignature { .. })));
    }

    #[test]
    fn time_slice_declares_exact_rewritten_arities_and_preserves_value_type() {
        assert_eq!(
            resolve_scalar_function(
                "time_slice",
                &[DataType::Utf8, DataType::Int64, DataType::Utf8]
            ),
            Ok(DataType::Utf8)
        );
        assert_eq!(
            resolve_scalar_function(
                "time_slice",
                &[
                    DataType::Date32,
                    DataType::Int64,
                    DataType::Utf8,
                    DataType::Utf8,
                ]
            ),
            Ok(DataType::Date32)
        );
        assert!(matches!(
            resolve_scalar_function("time_slice", &[DataType::Utf8, DataType::Int64]),
            Err(ResolveError::NoMatchingSignature { .. })
        ));
        assert!(matches!(
            resolve_scalar_function(
                "time_slice",
                &[
                    DataType::Utf8,
                    DataType::Int64,
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Utf8,
                ]
            ),
            Err(ResolveError::NoMatchingSignature { .. })
        ));
    }

    #[test]
    fn resolve_concat_is_variadic() {
        // `concat(...)` accepts any number of Utf8 args.
        for n in 1..5 {
            let args = vec![DataType::Utf8; n];
            assert_eq!(
                resolve_scalar_function("concat", &args),
                Ok(DataType::Utf8),
                "concat with {n} args"
            );
        }
    }

    #[test]
    fn resolve_abs_picks_per_input_type_signature() {
        for (input, output) in [
            (DataType::Int8, DataType::Int16),
            (DataType::Int16, DataType::Int32),
            (DataType::Int32, DataType::Int64),
            (DataType::Int64, DataType::FixedSizeBinary(16)),
            (DataType::FixedSizeBinary(16), DataType::FixedSizeBinary(16)),
            (DataType::Float32, DataType::Float32),
            (DataType::Float64, DataType::Float64),
            (DataType::Decimal128(18, 3), DataType::Decimal128(18, 3)),
        ] {
            let resolved = resolve_scalar_function_signature("abs", std::slice::from_ref(&input))
                .expect("ABS must select the exact input signature");
            assert_eq!(resolved.argument_types, [input]);
            assert_eq!(resolved.return_type, output);
        }
    }

    #[test]
    fn resolve_array_append_propagates_element_type() {
        // `array_append(List<T>, T) -> List<T>` — polymorphic.
        let r =
            resolve_scalar_function("array_append", &[list_of(DataType::Int64), DataType::Int64]);
        assert_eq!(r, Ok(list_of(DataType::Int64)));
    }

    #[test]
    fn resolve_array_append_rejects_mismatched_element_type() {
        // `array_append(List<Int64>, Utf8)` should not match — T is bound
        // to Int64 by the first arg, second arg violates the binding.
        let r =
            resolve_scalar_function("array_append", &[list_of(DataType::Int64), DataType::Utf8]);
        assert!(matches!(r, Err(ResolveError::NoMatchingSignature { .. })));
    }

    #[test]
    fn resolve_coalesce_widens_through_cast_match() {
        // `coalesce(Int8, Int64)` → Int64 via Pass 3 (widening cast).
        // Strict and polymorphic-strict both fail (T can't be Int8 and
        // Int64 at once), so this exercises the widening pass.
        let r = resolve_scalar_function("coalesce", &[DataType::Int8, DataType::Int64]);
        assert_eq!(r, Ok(DataType::Int64));
    }

    #[test]
    fn resolve_if_widens_then_and_else() {
        // `if(Boolean, Int8, Int64)` → Int64.
        let r =
            resolve_scalar_function("if", &[DataType::Boolean, DataType::Int8, DataType::Int64]);
        assert_eq!(r, Ok(DataType::Int64));
    }

    #[test]
    fn resolve_ifnull_widens_arguments() {
        // `ifnull(Int8, Float64)` → Float64 (wider).
        let r = resolve_scalar_function("ifnull", &[DataType::Int8, DataType::Float64]);
        assert_eq!(r, Ok(DataType::Float64));
    }

    #[test]
    fn resolve_coalesce_with_identical_args_no_widening_needed() {
        // `coalesce(Int64, Int64, Int64)` resolves at Pass 2 (strict
        // polymorphic) without reaching Pass 3.
        let r = resolve_scalar_function(
            "coalesce",
            &[DataType::Int64, DataType::Int64, DataType::Int64],
        );
        assert_eq!(r, Ok(DataType::Int64));
    }

    #[test]
    fn resolve_now_returns_datetime_with_no_args() {
        let r = resolve_scalar_function("now", &[]);
        assert_eq!(r, Ok(DataType::Timestamp(TimeUnit::Microsecond, None)));
    }

    #[test]
    fn resolve_assert_true_2arg_returns_boolean() {
        // The 2-arg form assert_true(bool, varchar) -> bool must resolve to Boolean.
        let r = resolve_scalar_function("assert_true", &[DataType::Boolean, DataType::Utf8]);
        assert_eq!(
            r,
            Ok(DataType::Boolean),
            "assert_true(bool, varchar) must resolve to Boolean"
        );
    }

    #[test]
    fn resolve_join_row_key_accepts_binary_object_ids_and_returns_utf8() {
        let r = resolve_scalar_function(
            "join_row_key",
            &[
                DataType::Binary,
                DataType::Int64,
                DataType::LargeBinary,
                DataType::Int64,
            ],
        );
        assert_eq!(r, Ok(DataType::Utf8));
    }

    #[test]
    fn resolve_join_row_key_rejects_utf8_object_id_salts() {
        let r = resolve_scalar_function(
            "join_row_key",
            &[
                DataType::Utf8,
                DataType::Int64,
                DataType::Utf8,
                DataType::Int64,
            ],
        );
        assert!(r.is_err());
    }

    #[test]
    fn resolved_signature_preserves_any_decimal128_argument_precision_and_scale() {
        let actual = DataType::Decimal128(18, 4);
        let signature = Signature::new(vec![TypeSpec::AnyDecimal128], TypeSpec::Boolean);

        let resolved = resolved_signature(
            &signature,
            std::slice::from_ref(&actual),
            &Bindings::default(),
        )
        .expect("AnyDecimal128 argument targets should preserve precision and scale");

        assert_eq!(resolved.argument_types, vec![actual]);
    }

    #[test]
    fn resolve_substring_exposes_int32_argument_targets() {
        let resolved = resolve_scalar_function_signature(
            "substring",
            &[DataType::Utf8, DataType::Int64, DataType::Int16],
        )
        .expect("substring integer arguments should use the opt-in cast match");

        assert_eq!(resolved.return_type, DataType::Utf8);
        assert_eq!(
            resolved.argument_types,
            vec![DataType::Utf8, DataType::Int32, DataType::Int32]
        );
        assert!(resolved.enforce_argument_binding);
    }

    #[test]
    fn resolve_substring_null_offset_targets_int32() {
        let resolved =
            resolve_scalar_function_signature("substring", &[DataType::Utf8, DataType::Null])
                .expect("NULL should coerce to the signature target");

        assert_eq!(
            resolved.argument_types,
            vec![DataType::Utf8, DataType::Int32]
        );
    }

    #[test]
    fn resolve_substring_reports_enforced_no_match() {
        let err = resolve_scalar_function_signature(
            "substring",
            &[DataType::Utf8, list_of(DataType::Int32)],
        )
        .expect_err("an offset that is not a number must not fall through to legacy inference");

        assert!(matches!(
            err,
            ResolveError::NoMatchingSignature {
                candidates: 2,
                binding_enforced: true,
            }
        ));
    }

    #[test]
    fn resolve_substring_offset_spelled_as_a_string_targets_int32() {
        let resolved =
            resolve_scalar_function_signature("substring", &[DataType::Utf8, DataType::Utf8])
                .expect("an offset opted in to coercion reads a string as a number");

        assert_eq!(
            resolved.argument_types,
            vec![DataType::Utf8, DataType::Int32]
        );
    }

    #[test]
    fn resolve_date_add_interval_spelled_as_a_float_targets_int64() {
        let resolved =
            resolve_scalar_function_signature("date_add", &[DataType::Utf8, DataType::Float64])
                .expect("an interval opted in to coercion reads a float as a number");

        assert_eq!(
            resolved.argument_types,
            vec![DataType::Utf8, DataType::Int64]
        );
        assert_eq!(
            resolved.return_type,
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn resolve_date_add_keeps_a_datetime_first_argument_over_the_string_overload() {
        let resolved = resolve_scalar_function_signature(
            "date_add",
            &[
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Utf8,
            ],
        )
        .expect("coercion applies to the interval, not to the shifted value");

        assert_eq!(
            resolved.argument_types,
            vec![
                DataType::Timestamp(TimeUnit::Microsecond, None),
                DataType::Int64
            ]
        );
    }

    #[test]
    fn resolve_encode_row_id_accepts_a_mixed_argument_list() {
        let resolved = resolve_scalar_function_signature(
            "encode_row_id",
            &[DataType::Utf8, DataType::Int64, DataType::Int32],
        )
        .expect("a per-argument hash does not bind every position to one type");

        assert_eq!(resolved.return_type, DataType::Binary);
    }

    #[test]
    fn resolve_substring_wrong_arity_reports_enforced_no_match() {
        let err = resolve_scalar_function_signature("substring", &[DataType::Utf8])
            .expect_err("an enforced function with no matching arity must not fall back");

        assert!(matches!(
            err,
            ResolveError::NoMatchingSignature {
                candidates: 2,
                binding_enforced: true,
            }
        ));
    }

    #[test]
    fn non_opt_in_signature_keeps_legacy_no_match_policy() {
        let err = resolve_scalar_function_signature("upper", &[DataType::Int64])
            .expect_err("upper(Int64) is normalized by the analyzer, not resolver cast match");

        assert!(matches!(
            err,
            ResolveError::NoMatchingSignature {
                binding_enforced: false,
                ..
            }
        ));
    }

    #[test]
    fn binding_enforcement_only_considers_candidates_for_the_call_arity() {
        let candidates = vec![
            Signature::new(vec![TypeSpec::Utf8], TypeSpec::Utf8),
            Signature::new(vec![TypeSpec::Utf8, TypeSpec::Int32], TypeSpec::Utf8)
                .with_argument_coercion(),
        ];

        assert!(
            !binding_enforced_for_arity(&candidates, 1),
            "an unrelated enforced two-argument overload must not block the one-argument legacy fallback"
        );
        assert!(
            binding_enforced_for_arity(&candidates, 2),
            "the matching-arity enforced overload must still block legacy fallback"
        );
    }

    #[test]
    fn field_signature_requires_one_frozen_type_and_two_arguments() {
        for ty in [
            DataType::Utf8,
            DataType::Int64,
            DataType::Float64,
            DataType::Decimal256(50, 2),
            DataType::FixedSizeBinary(16),
        ] {
            let resolved =
                resolve_scalar_function_signature("field", &[ty.clone(), ty.clone(), ty.clone()])
                    .unwrap();
            assert_eq!(resolved.argument_types, vec![ty.clone(), ty.clone(), ty]);
            assert_eq!(resolved.return_type, DataType::Int32);
        }
        assert!(resolve_scalar_function_signature("field", &[DataType::Int64]).is_err());
        assert!(
            resolve_scalar_function_signature("field", &[DataType::Int64, DataType::Float64])
                .is_err()
        );
    }
}
