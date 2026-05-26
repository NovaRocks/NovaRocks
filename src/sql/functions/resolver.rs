//! Function call resolver: given `(name, arg_types)`, find the best
//! matching [`Signature`] in the registry and return its instantiated
//! return type.
//!
//! Resolution proceeds in two passes that mirror the structure of
//! StarRocks' `FunctionSet.getFunction`:
//!
//! 1. **Strict match.** Every parameter spec must `anchor_matches` the
//!    corresponding argument type — no implicit casting, no type variables
//!    needed. Picks the first registered signature that strict-matches.
//! 2. **Polymorphic match.** If no strict match was found, try unifying
//!    each spec against the argument type, allowing `Any(name)` variants
//!    to bind. The first signature whose every spec unifies wins; its
//!    return type is then realised by substituting the bindings.
//!
//! `cast match` (StarRocks' third pass) is not yet implemented — Step A
//! deliberately leaves implicit widening to the legacy `infer_*` path. A
//! caller that fails to resolve here is free to fall back.

use arrow::datatypes::DataType;

use super::registry;
use super::signature::{BindMode, Bindings, Signature, anchor_matches, realize, unify};

/// Why a function call could not be resolved against the registry.
///
/// Callers (analyzer / codegen) use this to decide whether to surface an
/// error or fall back to the legacy hand-written `infer_*` path.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ResolveError {
    /// The function name is not registered. The caller should fall back to
    /// the legacy path — Step A only covers a subset of all known SQL
    /// functions.
    UnknownFunction,
    /// The function name is registered but no signature matches the given
    /// argument types. The caller should also fall back, because cast
    /// match (which Step A does not implement) might still succeed.
    NoMatchingSignature {
        /// All registered signatures for this name, for diagnostic output.
        candidates: usize,
    },
    /// The signature matched but its return type referenced an unbound
    /// type variable — a registry bug, not a user error. Bubble up.
    BadSignature(String),
}

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolveError::UnknownFunction => write!(f, "function not registered"),
            ResolveError::NoMatchingSignature { candidates } => write!(
                f,
                "no matching signature among {candidates} registered candidates"
            ),
            ResolveError::BadSignature(msg) => write!(f, "bad signature: {msg}"),
        }
    }
}

/// Resolve a scalar function call to its return type.
///
/// Returns `Err(ResolveError::UnknownFunction)` for names not yet
/// registered, so callers can transparently fall back to the legacy
/// `infer_*` path during the gradual Step A → Step B migration.
pub(crate) fn resolve_scalar_function(
    name: &str,
    arg_types: &[DataType],
) -> Result<DataType, ResolveError> {
    let candidates = registry::scalar_signatures(name).ok_or(ResolveError::UnknownFunction)?;

    // Pass 1: strict — every spec anchor-matches the concrete argument.
    for sig in candidates {
        if strict_matches(sig, arg_types) {
            return realize(&sig.ret, &Bindings::default()).map_err(ResolveError::BadSignature);
        }
    }

    // Pass 2: polymorphic-strict — `Any(name)` binds with equality.
    // Same name occurring twice must bind to the same concrete type.
    for sig in candidates {
        let mut bindings = Bindings::default();
        if polymorphic_matches(sig, arg_types, &mut bindings, BindMode::Strict) {
            return realize(&sig.ret, &bindings).map_err(ResolveError::BadSignature);
        }
    }

    // Pass 3: polymorphic-widening (StarRocks "cast match"). Only
    // signatures explicitly registered with `with_widening()` opt in
    // — e.g. `coalesce(Any("T"), ...) -> Any("T")`. Structural
    // polymorphic signatures like `array_append(List<T>, T) -> List<T>`
    // are deliberately excluded so a mismatched element type fails the
    // resolver instead of silently widening through the type variable.
    for sig in candidates {
        if !sig.widening {
            continue;
        }
        let mut bindings = Bindings::default();
        if polymorphic_matches(sig, arg_types, &mut bindings, BindMode::Widening) {
            return realize(&sig.ret, &bindings).map_err(ResolveError::BadSignature);
        }
    }

    Err(ResolveError::NoMatchingSignature {
        candidates: candidates.len(),
    })
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
        // `abs` has multiple signatures; we expect strict-match to pick
        // the one whose input type matches the actual argument.
        assert_eq!(
            resolve_scalar_function("abs", &[DataType::Int64]),
            Ok(DataType::Int64)
        );
        assert_eq!(
            resolve_scalar_function("abs", &[DataType::Float64]),
            Ok(DataType::Float64)
        );
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
}
