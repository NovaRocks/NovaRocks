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

use arrow::datatypes::DataType;

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
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) enum FunctionVolatility {
    #[default]
    Immutable,
    Volatile,
}

impl FunctionVolatility {
    pub(crate) const fn is_volatile(self) -> bool {
        matches!(self, Self::Volatile)
    }
}

#[derive(Debug, Default)]
pub(crate) struct BuiltinSqlFunctionCatalog;

impl crate::compiler::SqlFunctionCatalog for BuiltinSqlFunctionCatalog {
    fn resolve_scalar_signature(
        &self,
        name: &str,
        arg_types: &[DataType],
    ) -> Result<ResolvedScalarFunction, ResolveError> {
        resolve_scalar_function_signature(name, arg_types)
    }

    fn volatility(&self, name: &str) -> FunctionVolatility {
        builtin_function_volatility(name)
    }
}

static BUILTIN_SQL_FUNCTION_CATALOG: BuiltinSqlFunctionCatalog = BuiltinSqlFunctionCatalog;

pub fn builtin_sql_function_catalog() -> &'static dyn crate::compiler::SqlFunctionCatalog {
    &BUILTIN_SQL_FUNCTION_CATALOG
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
}
