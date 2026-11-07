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

use super::binding_control::{self, ScalarCarriers, ScalarTypes};
use crate::{FunctionResolutionError, FunctionValueType, ResolvedFunctionSignature};
use arrow_schema::DataType;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

use super::registry;
use super::scalar_construction::{ResolutionFailure, borrow_creator, with_resolution_creation};
use super::signature::{BindMode, Bindings, Signature, TypeSpec, anchor_matches, realize, unify};

pub type ResolvedScalarFunction = ResolvedFunctionSignature;

/// Why a function call could not be resolved against the registry.
///
/// Callers use this to decide whether to surface an error or retain a legacy
/// hand-written `infer_*` fallback. A caller that supports argument binding
/// must inspect `NoMatchingSignature::binding_enforced` before taking a
/// fallback path, so an opt-in signature policy cannot be bypassed.
pub type ResolveError = FunctionResolutionError;

/// Resolve a scalar function call to its instantiated signature.
///
/// When `enforce_argument_binding` is true, callers must bind each argument
/// to `argument_types`. `UnknownFunction` still permits the legacy
/// `infer_*` fallback; callers must inspect `NoMatchingSignature` before
/// using that fallback for a registered function.
pub fn resolve_scalar_function_signature(
    name: &str,
    arg_types: &[DataType],
) -> Result<ResolvedScalarFunction, ResolveError> {
    resolve_scalar_function_signature_with_overload(name, arg_types).map(|(_, resolved)| resolved)
}

pub fn resolve_scalar_function_signature_with_overload(
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
        if let Some(bindings) = integer_widening_bindings(sig, arg_types) {
            return resolved_signature(sig, arg_types, &bindings).map(|resolved| (index, resolved));
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
pub fn resolve_scalar_function_signature_at_overload(
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
    if let Some(bindings) = integer_widening_bindings(signature, arg_types) {
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

/// Exact value-domain signature instantiated by the builtin declaration owner.
/// The legacy DataType resolver is only a carrier projection; executable
/// binding uses this path for both selection and selected-overload validation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedScalarValueSignature {
    pub argument_types: Vec<FunctionValueType>,
    pub return_type: FunctionValueType,
    pub enforce_argument_binding: bool,
}

/// One accepted original pass. Argument authorization and its temporary
/// realization/Bindings retain their original algorithm and remain outside
/// this partial publication migration. The result is a borrowed recipe.
pub(super) struct ScalarValueSignatureDecision<'a> {
    pub(super) argument_types: Vec<FunctionValueType>,
    signature: &'a Signature,
    bindings: Bindings,
    // The original pass's return legality/physical hard-bound check still
    // constructs this temporary. It is not a borrowed source or new proof.
    // Removing it requires the sole layout preflight to support a borrowed
    // template; this partial checkpoint does not guess physical depth.
    checked_return_type: FunctionValueType,
}
impl ScalarValueSignatureDecision<'_> {
    pub(super) fn result_recipe(
        &self,
        nullable: bool,
    ) -> super::scalar_type_recipe::ScalarTypeRecipe<'_> {
        super::scalar_type_recipe::ScalarTypeRecipe::new(
            &self.signature.ret,
            &self.bindings,
            nullable,
        )
    }
    pub(super) fn diagnostic_return_type(&self) -> FunctionValueType {
        self.checked_return_type.clone()
    }
    fn materialize(
        self,
        _work: &mut CompileCheckpoints<'_>,
    ) -> Result<ResolvedScalarValueSignature, ResolveError> {
        Ok(ResolvedScalarValueSignature {
            argument_types: self.argument_types,
            return_type: self.checked_return_type,
            enforce_argument_binding: self.signature.argument_binding.is_enforced(),
        })
    }
}

pub fn resolve_scalar_value_signature_with_overload(
    name: &str,
    arguments: &[FunctionValueType],
    control: &dyn PureCompileControl,
) -> Result<(usize, ResolvedScalarValueSignature), ResolveError> {
    resolve_scalar_value_signature_view_with_overload(
        name,
        ScalarTypes::from_values(arguments),
        control,
    )
}

pub(super) fn resolve_scalar_value_signature_view_with_overload(
    name: &str,
    arguments: ScalarTypes<'_>,
    control: &dyn PureCompileControl,
) -> Result<(usize, ResolvedScalarValueSignature), ResolveError> {
    value_resolution_scope(control, |work| {
        let candidates = registry::scalar_signatures(name).ok_or(ResolveError::UnknownFunction)?;
        let carriers = value_carriers(arguments, work)?;
        for pass in 0..5 {
            work.step()?;
            for (index, signature) in candidates.iter().enumerate() {
                work.step()?;
                if let Some(resolved) =
                    value_signature_in_pass(signature, arguments, carriers, pass, work)?
                {
                    return Ok((index, resolved));
                }
            }
        }
        let mut relevant = false;
        let mut enforced = false;
        let mut all_enforced = true;
        for signature in candidates {
            work.step()?;
            all_enforced &= signature.argument_binding.is_enforced();
            if check_arity(signature, arguments.len()) {
                relevant = true;
                enforced |= signature.argument_binding.is_enforced();
            }
        }
        Err(ResolveError::NoMatchingSignature {
            candidates: candidates.len(),
            binding_enforced: if relevant { enforced } else { all_enforced },
        })
    })
}

pub fn resolve_scalar_value_signature_at_overload(
    name: &str,
    overload_index: usize,
    arguments: &[FunctionValueType],
    control: &dyn PureCompileControl,
) -> Result<ResolvedScalarValueSignature, ResolveError> {
    resolve_scalar_value_signature_view_at_overload(
        name,
        overload_index,
        ScalarTypes::from_values(arguments),
        control,
    )
}

pub(super) fn resolve_scalar_value_signature_view_at_overload(
    name: &str,
    overload_index: usize,
    arguments: ScalarTypes<'_>,
    control: &dyn PureCompileControl,
) -> Result<ResolvedScalarValueSignature, ResolveError> {
    decide_scalar_value_signature_at_overload(
        name,
        overload_index,
        arguments,
        control,
        |decision, work| decision.materialize(work),
    )
}
pub(super) fn scalar_value_decision_at_overload(
    name: &str,
    overload_index: usize,
    arguments: ScalarTypes<'_>,
    control: &dyn PureCompileControl,
) -> Result<ScalarValueSignatureDecision<'static>, ResolveError> {
    decide_scalar_value_signature_at_overload(
        name,
        overload_index,
        arguments,
        control,
        |decision, _| Ok(decision),
    )
}

fn decide_scalar_value_signature_at_overload<T>(
    name: &str,
    overload_index: usize,
    arguments: ScalarTypes<'_>,
    control: &dyn PureCompileControl,
    consume: impl FnOnce(
        ScalarValueSignatureDecision<'static>,
        &mut CompileCheckpoints<'_>,
    ) -> Result<T, ResolveError>,
) -> Result<T, ResolveError> {
    decide_scalar_value_signature_with_creation::<T, std::convert::Infallible>(
        name,
        overload_index,
        arguments,
        control,
        None,
        |decision, work| consume(decision, work).map_err(Into::into),
    )
    .map_err(ResolutionFailure::into_pure)
}
pub(super) fn scalar_value_decision_at_overload_with_creator_in<H>(
    name: &str,
    overload_index: usize,
    arguments: ScalarTypes<'_>,
    creation: &mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>,
    control: &dyn PureCompileControl,
) -> Result<ScalarValueSignatureDecision<'static>, ResolutionFailure<H>> {
    decide_scalar_value_signature_with_creation(
        name,
        overload_index,
        arguments,
        control,
        Some(creation),
        |decision, _| Ok(decision),
    )
}
fn decide_scalar_value_signature_with_creation<T, H>(
    name: &str,
    overload_index: usize,
    arguments: ScalarTypes<'_>,
    control: &dyn PureCompileControl,
    mut creation: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
    consume: impl FnOnce(
        ScalarValueSignatureDecision<'static>,
        &mut CompileCheckpoints<'_>,
    ) -> Result<T, ResolutionFailure<H>>,
) -> Result<T, ResolutionFailure<H>> {
    value_resolution_scope_with_creation(control, |work| {
        let candidates = registry::scalar_signatures(name).ok_or(ResolveError::UnknownFunction)?;
        let signature = candidates.get(overload_index).ok_or_else(|| {
            ResolveError::BadSignature("selected overload index is unknown".into())
        })?;
        let carriers = value_carriers(arguments, work)?;
        for pass in 0..5 {
            work.step()?;
            if let Some(resolved) = value_signature_decision_with_creation(
                signature,
                arguments,
                carriers,
                pass,
                work,
                borrow_creator(&mut creation),
            )? {
                return consume(resolved, work);
            }
        }
        Err(ResolveError::NoMatchingSignature {
            candidates: 1,
            binding_enforced: signature.argument_binding.is_enforced(),
        }
        .into())
    })
}

fn binding_to_resolution(error: crate::FunctionBindingError) -> ResolveError {
    match error {
        crate::FunctionBindingError::Control(error) => ResolveError::Control(error),
        other => ResolveError::BadSignature(other.to_string()),
    }
}

fn value_resolution_scope<T>(
    control: &dyn PureCompileControl,
    body: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, ResolveError>,
) -> Result<T, ResolveError> {
    value_resolution_scope_with_creation::<T, std::convert::Infallible>(control, |work| {
        body(work).map_err(Into::into)
    })
    .map_err(ResolutionFailure::into_pure)
}
fn value_resolution_scope_with_creation<T, H>(
    control: &dyn PureCompileControl,
    body: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, ResolutionFailure<H>>,
) -> Result<T, ResolutionFailure<H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = body(&mut work);
    work.finish_result(result, ResolutionFailure::from)
}

fn value_carriers<'a>(
    arguments: ScalarTypes<'a>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ScalarCarriers<'a>, ResolveError> {
    for argument in arguments.iter() {
        work.step()?;
        binding_control::value_type(argument, work).map_err(binding_to_resolution)?;
    }
    Ok(ScalarCarriers::from_values(arguments))
}

fn carrier_matches_with_creation<H>(
    sig: &Signature,
    args: ScalarCarriers<'_>,
    pass: u8,
    work: &mut CompileCheckpoints<'_>,
    mut creation: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
) -> Result<bool, ResolutionFailure<H>> {
    if !check_arity(sig, args.len())
        || (pass == 3 && !sig.argument_binding.is_enforced())
        || (pass == 4 && !sig.widening)
    {
        return Ok(false);
    }
    let mut bindings = Bindings::default();
    let mut widened = false;
    for (index, actual) in args.iter().enumerate() {
        work.step()?;
        let spec = signature_spec_at(sig, index);
        // The declaration's recursive matcher is still a legacy opaque helper;
        // the actual argument/pass traversal belongs to this controlled owner.
        work.flush()?;
        let matched = match pass {
            0 => anchor_matches(spec, actual),
            1 => with_resolution_creation(borrow_creator(&mut creation), work, |creation| {
                super::signature::unify_with_construction(
                    spec,
                    actual,
                    &mut bindings,
                    BindMode::Strict,
                    creation,
                )
            })?,
            2 => {
                if integer_widens_losslessly(spec, actual)
                    || integer_widens_to_map_key(sig, index, actual, &bindings, args)
                {
                    widened = true;
                    true
                } else {
                    with_resolution_creation(borrow_creator(&mut creation), work, |creation| {
                        super::signature::unify_with_construction(
                            spec,
                            actual,
                            &mut bindings,
                            BindMode::Strict,
                            creation,
                        )
                    })?
                }
            }
            3 => {
                anchor_matches(spec, actual) || implicit_anchor_cast_target(spec, actual).is_some()
            }
            4 => with_resolution_creation(borrow_creator(&mut creation), work, |creation| {
                super::signature::unify_with_construction(
                    spec,
                    actual,
                    &mut bindings,
                    BindMode::Widening,
                    creation,
                )
            })?,
            _ => unreachable!("closed resolver pass"),
        };
        work.step()?;
        if !matched {
            return Ok(false);
        }
    }
    Ok(pass != 2 || widened)
}

fn value_signature_in_pass(
    signature: &Signature,
    arguments: ScalarTypes<'_>,
    carriers: ScalarCarriers<'_>,
    pass: u8,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<ResolvedScalarValueSignature>, ResolveError> {
    value_signature_decision_in_pass(signature, arguments, carriers, pass, work)?
        .map(|decision| decision.materialize(work))
        .transpose()
}

fn value_signature_decision_in_pass<'a>(
    signature: &'a Signature,
    arguments: ScalarTypes<'_>,
    carriers: ScalarCarriers<'_>,
    pass: u8,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<ScalarValueSignatureDecision<'a>>, ResolveError> {
    value_signature_decision_with_creation::<std::convert::Infallible>(
        signature, arguments, carriers, pass, work, None,
    )
    .map_err(ResolutionFailure::into_pure)
}
fn value_signature_decision_with_creation<'a, H>(
    signature: &'a Signature,
    arguments: ScalarTypes<'_>,
    carriers: ScalarCarriers<'_>,
    pass: u8,
    work: &mut CompileCheckpoints<'_>,
    mut creation: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
) -> Result<Option<ScalarValueSignatureDecision<'a>>, ResolutionFailure<H>> {
    let carrier_match = carrier_matches_with_creation(
        signature,
        carriers,
        pass,
        work,
        borrow_creator(&mut creation),
    )?;
    if !carrier_match {
        return Ok(None);
    }
    let mode = if pass == 4 {
        BindMode::Widening
    } else {
        BindMode::Strict
    };
    let mut bindings = Bindings::default();
    for (index, actual) in arguments.iter().enumerate() {
        work.step()?;
        work.flush()?;
        let matched = if pass == 2
            && value_widens_to_map_key(signature, index, actual, &bindings, carriers)
        {
            true
        } else {
            with_resolution_creation(borrow_creator(&mut creation), work, |creation| {
                super::signature::unify_value_with_construction(
                    signature_spec_at(signature, index),
                    actual,
                    &mut bindings,
                    mode,
                    creation,
                )
            })?
        };
        work.step()?;
        if !matched {
            return Ok(None);
        }
    }
    with_resolution_creation(borrow_creator(&mut creation), work, |creation| {
        creation.array::<FunctionValueType>(arguments.len())
    })?;
    let mut argument_types = Vec::with_capacity(arguments.len());
    for (index, actual) in arguments.iter().enumerate() {
        work.step()?;
        // Recursive realization/domain recipes retain their existing policies;
        // their opaque internal allocation work is not certified by this step.
        work.flush()?;
        let target = match signature_spec_at(signature, index) {
            TypeSpec::AnyType | TypeSpec::AnyDecimal128 => actual.clone(),
            spec => with_resolution_creation(borrow_creator(&mut creation), work, |creation| {
                super::signature::realize_value_with_construction(
                    spec,
                    &bindings,
                    actual.nullable(),
                    creation,
                )
            })?,
        };
        binding_control::value_type(&target, work).map_err(binding_to_resolution)?;
        work.flush()?;
        let authorized =
            with_resolution_creation(borrow_creator(&mut creation), work, |creation| {
                super::value_conversion::assignment_domains_authorized_with_construction(
                    actual, &target, creation,
                )
            })?;
        work.step()?;
        if !authorized {
            return Ok(None);
        }
        argument_types.push(target);
    }
    work.flush()?;
    let checked_return_type =
        with_resolution_creation(borrow_creator(&mut creation), work, |creation| {
            super::scalar_type_recipe::ScalarTypeRecipe::new(&signature.ret, &bindings, true)
                .materialize_with_construction(creation)
        })?;
    binding_control::value_type(&checked_return_type, work).map_err(binding_to_resolution)?;
    Ok(Some(ScalarValueSignatureDecision {
        argument_types,
        signature,
        bindings,
        checked_return_type,
    }))
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
pub fn resolve_scalar_function(
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
fn integer_widening_bindings(sig: &Signature, arg_types: &[DataType]) -> Option<Bindings> {
    if !check_arity(sig, arg_types.len()) {
        return None;
    }
    let mut widened_any = false;
    let mut bindings = Bindings::default();
    for (idx, actual) in arg_types.iter().enumerate() {
        let spec = signature_spec_at(sig, idx);
        if integer_widens_losslessly(spec, actual)
            || integer_widens_to_map_key(
                sig,
                idx,
                actual,
                &bindings,
                ScalarCarriers::from_data_types(arg_types),
            )
        {
            widened_any = true;
        } else if !unify(spec, actual, &mut bindings, BindMode::Strict) {
            return None;
        }
    }
    // The original strict/polymorphic passes retain precedence. A fixed
    // signed anchor must actually widen; generic siblings keep strict T/D.
    widened_any.then_some(bindings)
}

/// Only the declared Map<K, V> plus probe K role anchors this conversion.
/// The original Map key and binding stay fixed. Scalar repeated T, List<T>,
/// undecided NULL maps and later bindings never authorize a generic widening.
/// This inspects one actual Map key in constant work, without a prefix scan.
fn integer_widens_to_map_key(
    signature: &Signature,
    index: usize,
    actual: &DataType,
    bindings: &Bindings,
    arguments: ScalarCarriers<'_>,
) -> bool {
    if index != 1 {
        return false;
    }
    let TypeSpec::Any(name) = signature_spec_at(signature, index) else {
        return false;
    };
    let TypeSpec::Map(key, _) = signature_spec_at(signature, 0) else {
        return false;
    };
    if !matches!(key.as_ref(), TypeSpec::Any(key_name) if key_name == name) {
        return false;
    }
    let Some(DataType::Map(entries, _)) = arguments.get(0) else {
        return false;
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return false;
    };
    if fields.len() != 2 {
        return false;
    }
    let Some(bound) = bindings.lookup(name) else {
        return false;
    };
    let anchor = match bound {
        DataType::Int8 => TypeSpec::Int8,
        DataType::Int16 => TypeSpec::Int16,
        DataType::Int32 => TypeSpec::Int32,
        DataType::Int64 => TypeSpec::Int64,
        _ => return false,
    };
    // The right-hand side is now a primitive; this cannot walk a nested key.
    fields[0].data_type() == &bound && integer_widens_losslessly(&anchor, actual)
}

fn value_widens_to_map_key(
    signature: &Signature,
    index: usize,
    actual: &FunctionValueType,
    bindings: &Bindings,
    arguments: ScalarCarriers<'_>,
) -> bool {
    let TypeSpec::Any(name) = signature_spec_at(signature, index) else {
        return false;
    };
    let Some(bound) = bindings.lookup_value_borrowed(name) else {
        return false;
    };
    actual.logical_type() == novarocks_type_contract::ValueLogicalType::Physical
        && bound.logical_type() == novarocks_type_contract::ValueLogicalType::Physical
        && integer_widens_to_map_key(signature, index, actual.data_type(), bindings, arguments)
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
        TypeSpec::LargeInt => 16,
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

    use arrow_schema::{Field, TimeUnit};

    fn list_of(item: DataType) -> DataType {
        DataType::List(Arc::new(Field::new("item", item, true)))
    }

    #[test]
    fn lossless_anchor_widening_keeps_strict_generic_siblings_and_selected_replay() {
        struct Control;
        impl PureCompileControl for Control {
            fn checkpoint(
                &self,
                _: CompilePhase,
                _: u32,
            ) -> Result<(), novarocks_type_contract::CompileControlError> {
                Ok(())
            }
        }
        let json = FunctionValueType::try_with_logical_type(
            DataType::Utf8,
            true,
            novarocks_type_contract::ValueLogicalType::Json,
        )
        .unwrap();
        let plain = FunctionValueType::new(DataType::Utf8, false);
        for (name, arguments) in [
            (
                "lead",
                vec![
                    json.clone(),
                    FunctionValueType::new(DataType::Int32, false),
                    plain.clone(),
                ],
            ),
            (
                "lag",
                vec![
                    json.clone(),
                    FunctionValueType::new(DataType::Int16, true),
                    plain.clone(),
                ],
            ),
            (
                "array_slice",
                vec![
                    FunctionValueType::new(list_of(DataType::Int32), true),
                    FunctionValueType::new(DataType::Int32, false),
                ],
            ),
            (
                "session_number",
                vec![
                    FunctionValueType::new(DataType::Int32, true),
                    FunctionValueType::new(DataType::Int32, false),
                ],
            ),
            (
                "__map_element_at",
                vec![
                    FunctionValueType::new(
                        DataType::Map(
                            Arc::new(Field::new(
                                "entries",
                                DataType::Struct(
                                    vec![
                                        Field::new("key", DataType::Int64, false),
                                        Field::new("value", DataType::Utf8, true),
                                    ]
                                    .into(),
                                ),
                                false,
                            )),
                            false,
                        ),
                        true,
                    ),
                    FunctionValueType::new(DataType::Int32, true),
                ],
            ),
        ] {
            let carriers = arguments
                .iter()
                .map(|ty| ty.data_type().clone())
                .collect::<Vec<_>>();
            let (carrier_index, carrier) =
                resolve_scalar_function_signature_with_overload(name, &carriers).unwrap();
            let (index, resolved) =
                resolve_scalar_value_signature_with_overload(name, &arguments, &Control).unwrap();
            assert_eq!(index, carrier_index, "{name}");
            assert_eq!(carrier.argument_types[1], DataType::Int64);
            assert_eq!(
                resolved.argument_types[0], arguments[0],
                "generic source stays exact: {name}"
            );
            assert_eq!(
                resolved.argument_types[1],
                FunctionValueType::new(DataType::Int64, arguments[1].nullable())
            );
            if arguments.len() == 3 {
                assert_eq!(
                    resolved.argument_types[2], arguments[2],
                    "independent D domain stays exact"
                );
            }
            assert_eq!(
                resolve_scalar_function_signature_at_overload(name, index, &carriers).unwrap(),
                carrier
            );
            assert_eq!(
                resolve_scalar_value_signature_at_overload(name, index, &arguments, &Control)
                    .unwrap(),
                resolved
            );
        }
        let narrow_map = FunctionValueType::new(
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Int32, false),
                            Field::new("value", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        );
        assert!(
            resolve_scalar_value_signature_with_overload(
                "__map_element_at",
                &[narrow_map, FunctionValueType::new(DataType::Int64, false)],
                &Control
            )
            .is_err(),
            "probe cannot narrow or change the existing map-key binding"
        );
        let repeated = Signature::new(
            vec![TypeSpec::Any("T"), TypeSpec::Any("T"), TypeSpec::Int64],
            TypeSpec::Any("T"),
        );
        for arguments in [
            [DataType::Int8, DataType::Int16, DataType::Int32],
            [DataType::Int16, DataType::Int8, DataType::Int32],
        ] {
            assert!(
                integer_widening_bindings(&repeated, &arguments).is_none(),
                "ordinary repeated T stays strict in both orders"
            );
        }
        let list = FunctionValueType::new(list_of(DataType::Int64), false);
        let item = FunctionValueType::new(DataType::Int32, false);
        assert!(
            resolve_scalar_function_signature_with_overload(
                "array_append",
                &[list.data_type().clone(), DataType::Int32]
            )
            .is_err(),
            "List<T> does not grant the Map key role"
        );
        assert!(
            resolve_scalar_value_signature_with_overload("array_append", &[list, item], &Control)
                .is_err()
        );
        assert!(
            resolve_scalar_value_signature_with_overload(
                "lead",
                &[json.clone(), plain.clone()],
                &Control
            )
            .is_err(),
            "text offset cannot widen"
        );
        assert!(
            resolve_scalar_value_signature_with_overload("lead", &[plain, json], &Control).is_err(),
            "nominal JSON cannot become a signed offset"
        );
    }

    #[test]
    fn json_producers_declare_their_value_domain_before_public_output_projection() {
        struct Control;
        impl PureCompileControl for Control {
            fn checkpoint(
                &self,
                _: CompilePhase,
                _: u32,
            ) -> Result<(), novarocks_type_contract::CompileControlError> {
                Ok(())
            }
        }
        let text = FunctionValueType::new(DataType::Utf8, false);
        let integer = FunctionValueType::new(DataType::Int64, false);
        for (name, arguments) in [
            ("parse_json", vec![text.clone()]),
            ("json_object", vec![text.clone(), integer.clone()]),
            ("json_array", vec![integer]),
        ] {
            let (overload, resolved) =
                resolve_scalar_value_signature_with_overload(name, &arguments, &Control).unwrap();
            assert_eq!(resolved.return_type.data_type(), &DataType::Utf8);
            assert_eq!(
                resolved.return_type.logical_type(),
                novarocks_type_contract::ValueLogicalType::Json,
                "the installed function declaration owns its nominal result"
            );
            assert_eq!(
                resolve_scalar_value_signature_at_overload(name, overload, &arguments, &Control)
                    .unwrap(),
                resolved,
                "selected-overload replay preserves the same complete declaration"
            );
            let (_, coalesce) = resolve_scalar_value_signature_with_overload(
                "coalesce",
                &[
                    resolved.return_type,
                    FunctionValueType::new(DataType::Null, true),
                ],
                &Control,
            )
            .unwrap();
            assert_eq!(
                coalesce.return_type.logical_type(),
                novarocks_type_contract::ValueLogicalType::Json
            );
        }
        let (_, upper) =
            resolve_scalar_value_signature_with_overload("upper", &[text], &Control).unwrap();
        assert_eq!(
            upper.return_type.logical_type(),
            novarocks_type_contract::ValueLogicalType::Physical,
            "the same Utf8 carrier does not turn a text producer into JSON"
        );
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
    fn slice_functions_bind_only_their_declared_temporal_domains() {
        let timestamp = DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None);
        for (name, domain) in [("time_slice", timestamp), ("date_slice", DataType::Date32)] {
            for arity in [3, 4] {
                let mut args = vec![domain.clone(), DataType::Int32, DataType::Utf8];
                if arity == 4 {
                    args.push(DataType::Utf8);
                }
                let result = resolve_scalar_function_signature(name, &args).unwrap();
                assert_eq!(result.return_type, domain);
                assert_eq!(result.argument_types, args);
                assert!(result.enforce_argument_binding);
            }
            for bad in [DataType::Utf8, DataType::Boolean, DataType::Float64] {
                assert!(matches!(
                    resolve_scalar_function(name, &[bad, DataType::Int32, DataType::Utf8]),
                    Err(ResolveError::NoMatchingSignature { .. })
                ));
            }
            assert!(resolve_scalar_function(name, &[domain.clone(), DataType::Int32]).is_err());
            assert!(
                resolve_scalar_function(
                    name,
                    &[
                        domain,
                        DataType::Int32,
                        DataType::Utf8,
                        DataType::Utf8,
                        DataType::Utf8
                    ]
                )
                .is_err()
            );
        }
    }

    #[test]
    fn resolve_concat_is_variadic() {
        assert!(resolve_scalar_function("concat", &[]).is_err());
        // CONCAT requires one Utf8 argument before its repeated tail.
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
