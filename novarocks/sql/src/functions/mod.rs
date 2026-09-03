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

//! Single-source function signature registry.
//!
//! Before this module landed, analyzer and codegen each carried their own
//! private "given a function name and argument types, what is the return
//! type?" logic — analyzer in [`crate::analyzer::functions`] and the now
//! retired legacy FE Thrift expression emitter. The two copies were drifting
//! (the emitter side, for example, recognised `parse_url -> Utf8` while the
//! analyzer did not), and adding a new SQL function meant patching both sides
//! at once.
//!
//! This module follows StarRocks' [`functions.py`] approach: every supported
//! scalar function (and operator) is described once, by a [`Signature`] of
//! parameter types and a return type. Resolving a call is then a lookup
//! against that table (`strict → polymorphic → cast`), and both analyzer
//! and codegen share the same answer.
//!
//! Step A of the migration deliberately covers only the high-frequency
//! function families (string / numeric / condition / a few array helpers).
//! Anything not yet registered here falls through to the legacy
//! hand-written `infer_*` paths so existing behaviour is preserved.
//!
//! [`functions.py`]: https://github.com/StarRocks/starrocks/blob/main/gensrc/script/functions.py

pub(crate) mod registry;
pub(crate) mod resolver;
pub(crate) mod signature;

use std::sync::{Arc, LazyLock};

use arrow::datatypes::DataType;
use novarocks_functions::{
    EngineFunctionCatalog, EngineFunctionCatalogBuilder, FunctionCatalogError,
    FunctionDefinition, FunctionKind, FunctionResolutionError, FunctionSignatureResolver,
    FunctionVisibility, ResolvedFunctionSignature,
};

#[cfg(test)]
pub(crate) use resolver::resolve_scalar_function;
pub(crate) use resolver::{
    ResolveError, ResolvedScalarFunction, resolve_scalar_function_signature,
};

/// Evaluation stability of a scalar function call.
///
/// This is SQL semantic metadata, not an optimizer-local policy.  It is
/// intentionally carried by the immutable function catalog so that analysis,
/// lambda validation, CSE, predicate derivation, and aggregate pushdown make
/// the same decision.
pub(crate) use novarocks_functions::FunctionVolatility;

impl crate::compiler::SqlFunctionCatalog for EngineFunctionCatalog {
    fn resolve_scalar_signature(
        &self,
        name: &str,
        arg_types: &[DataType],
    ) -> Result<ResolvedScalarFunction, ResolveError> {
        self.resolve_user(name, FunctionKind::Scalar, arg_types)
    }

    fn volatility(&self, name: &str) -> FunctionVolatility {
        self.definition(name, FunctionKind::Scalar)
            .map(FunctionDefinition::volatility)
            .unwrap_or_default()
    }
}

struct BuiltinScalarResolver {
    canonical_name: Box<str>,
}

impl FunctionSignatureResolver for BuiltinScalarResolver {
    fn resolve(
        &self,
        argument_types: &[DataType],
    ) -> Result<ResolvedFunctionSignature, FunctionResolutionError> {
        resolve_scalar_function_signature(&self.canonical_name, argument_types)
            .map(|resolved| ResolvedFunctionSignature {
                return_type: resolved.return_type,
                argument_types: resolved.argument_types,
                enforce_argument_binding: resolved.enforce_argument_binding,
            })
            .map_err(|error| match error {
                ResolveError::UnknownFunction => FunctionResolutionError::UnknownFunction,
                ResolveError::HiddenFunction => FunctionResolutionError::HiddenFunction,
                ResolveError::NoMatchingSignature {
                    candidates,
                    binding_enforced,
                } => FunctionResolutionError::NoMatchingSignature {
                    candidates,
                    binding_enforced,
                },
                ResolveError::BadSignature(message) => {
                    FunctionResolutionError::BadSignature(message)
                }
            })
    }
}

pub fn contribute_builtin_functions(
    builder: &mut EngineFunctionCatalogBuilder,
) -> Result<(), FunctionCatalogError> {
    for (name, signatures) in registry::builtin_scalar_declarations() {
        let resolver = Arc::new(BuiltinScalarResolver {
            canonical_name: name.clone().into_boxed_str(),
        });
        builder.register(FunctionDefinition::try_new(
            &name,
            FunctionKind::Scalar,
            FunctionVisibility::Public,
            builtin_function_volatility(&name),
            signatures,
            resolver,
        )?)?;
    }
    Ok(())
}

pub fn build_builtin_engine_function_catalog() -> Result<EngineFunctionCatalog, FunctionCatalogError>
{
    let mut builder = EngineFunctionCatalogBuilder::new();
    contribute_builtin_functions(&mut builder)?;
    builder.seal()
}

static BUILTIN_ENGINE_FUNCTION_CATALOG: LazyLock<EngineFunctionCatalog> = LazyLock::new(|| {
    build_builtin_engine_function_catalog().expect("builtin engine function catalog must be valid")
});

pub fn builtin_sql_function_catalog() -> &'static dyn crate::compiler::SqlFunctionCatalog {
    &*BUILTIN_ENGINE_FUNCTION_CATALOG
}

pub fn builtin_engine_function_catalog() -> &'static EngineFunctionCatalog {
    &BUILTIN_ENGINE_FUNCTION_CATALOG
}

/// Canonical set of volatile builtins.  Keep this list here rather than in
/// analyzer and optimizer copies.  The historical analyzer list was a strict
/// subset; SQLX-1 deliberately adopts the optimizer's full safety set.
///
/// "Volatile" covers two kinds of non-constant builtin, and both have to be
/// denied for the same reason: the optimizer must not evaluate them itself.
///
/// - Non-deterministic *value*: `rand`, `random`, `uuid` and the clock family
///   return a different answer per evaluation.
/// - Non-reproducible *side effect*: `sleep` returns a constant `true`, but its
///   whole observable behavior is the delay it imposes on the evaluating
///   thread. Classifying it `Immutable` let `FoldConstant` evaluate
///   `sleep(10)` on the frontend during logical normalization, which blocked
///   the planner for the sleep duration and then shipped a bare `true` to the
///   backends — the delay disappeared from execution entirely. This matches
///   the reference engine, which groups `sleep` with `rand`/`random`/`uuid`
///   rather than with the clock functions.
pub(crate) fn builtin_function_volatility(name: &str) -> FunctionVolatility {
    match name.to_ascii_lowercase().as_str() {
        "rand" | "random" | "uuid" | "sleep" | "now" | "current_timestamp" | "current_date"
        | "curdate" | "current_time" | "curtime" | "localtime" | "localtimestamp"
        | "utc_timestamp" | "utc_time" => FunctionVolatility::Volatile,
        _ => FunctionVolatility::Immutable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlx1_function_builtin_snapshot_has_canonical_volatility_set() {
        let catalog = builtin_sql_function_catalog();
        for name in [
            "rand",
            "random",
            "uuid",
            "sleep",
            "now",
            "current_timestamp",
            "current_date",
            "curdate",
            "current_time",
            "curtime",
            "localtime",
            "localtimestamp",
            "utc_timestamp",
            "utc_time",
        ] {
            assert_eq!(
                catalog.volatility(name),
                FunctionVolatility::Volatile,
                "{name}"
            );
        }
        assert_eq!(catalog.volatility("lower"), FunctionVolatility::Immutable);
    }

    #[test]
    fn sqlx1_function_snapshot_resolves_registered_signature() {
        let resolved = builtin_sql_function_catalog()
            .resolve_scalar_signature("lower", &[DataType::Utf8])
            .expect("registered function resolves through snapshot");
        assert_eq!(resolved.return_type, DataType::Utf8);
    }

    #[test]
    fn builtin_bundle_is_canonical_and_reproducible() {
        let first = build_builtin_engine_function_catalog().expect("first catalog");
        let second = build_builtin_engine_function_catalog().expect("second catalog");
        assert_eq!(first.digest(), second.digest());
        assert!(!first.definitions().is_empty());
        assert!(first.definitions().windows(2).all(|pair| {
            (pair[0].canonical_name(), pair[0].kind())
                < (pair[1].canonical_name(), pair[1].kind())
        }));
    }
}
