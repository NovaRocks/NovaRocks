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
    AggregateOverloadDeclaration, AggregateOverloadIdentity, AggregateSignatureResolver,
    AggregateStateFormatIdentity, EngineFunctionCatalog, EngineFunctionCatalogBuilder,
    FunctionCatalogError, FunctionDefinition, FunctionKind, FunctionResolutionError,
    FunctionSignatureResolver, FunctionVisibility, ResolvedAggregateSignature,
    ResolvedFunctionSignature,
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
    fn snapshot(&self) -> Arc<dyn crate::compiler::SqlFunctionCatalog> {
        Arc::new(self.clone())
    }

    fn resolve_scalar_signature(
        &self,
        name: &str,
        arg_types: &[DataType],
    ) -> Result<ResolvedScalarFunction, ResolveError> {
        self.resolve_user(name, FunctionKind::Scalar, arg_types)
    }

    fn contains_aggregate(&self, name: &str) -> bool {
        self.definition(name, FunctionKind::Aggregate).is_some()
    }

    fn resolve_aggregate_signature(
        &self,
        name: &str,
        arg_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        self.resolve_aggregate_user(name, arg_types)
    }

    fn resolve_aggregate_update_signature(
        &self,
        name: &str,
        logical_arg_types: &[DataType],
        update_arg_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        self.resolve_aggregate_update_user(name, logical_arg_types, update_arg_types)
    }

    fn resolve_aggregate_trusted(
        &self,
        name: &str,
        arg_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        EngineFunctionCatalog::resolve_aggregate_trusted(self, name, arg_types)
    }

    fn volatility(&self, name: &str) -> FunctionVolatility {
        self.definition(name, FunctionKind::Scalar)
            .map(FunctionDefinition::volatility)
            .unwrap_or_default()
    }
}

#[derive(Clone, Copy)]
struct AggregateDeclaration {
    name: &'static str,
    signature: &'static str,
    min_args: usize,
    max_args: usize,
}

impl AggregateDeclaration {
    const fn exact(name: &'static str, args: usize, signature: &'static str) -> Self {
        Self {
            name,
            signature,
            min_args: args,
            max_args: args,
        }
    }

    const fn ranged(
        name: &'static str,
        min_args: usize,
        max_args: usize,
        signature: &'static str,
    ) -> Self {
        Self {
            name,
            signature,
            min_args,
            max_args,
        }
    }
}

struct BuiltinAggregateResolver {
    declaration: AggregateDeclaration,
}

impl AggregateSignatureResolver for BuiltinAggregateResolver {
    fn resolve_aggregate(
        &self,
        argument_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        let declaration = self.declaration;
        if !(declaration.min_args..=declaration.max_args).contains(&argument_types.len()) {
            return Err(FunctionResolutionError::NoMatchingSignature {
                candidates: 1,
                binding_enforced: true,
            });
        }
        self.resolve_update_signature(&builtin_overload_identity(declaration)?, argument_types)
    }

    fn supports_ordered_update_channels(&self) -> bool {
        builtin_supports_ordered_update_channels(self.declaration.name)
    }

    fn resolve_update_signature(
        &self,
        selected_overload: &AggregateOverloadIdentity,
        update_argument_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        let declaration = self.declaration;
        let overload = builtin_overload_identity(declaration)?;
        if selected_overload != &overload {
            return Err(FunctionResolutionError::BadSignature(format!(
                "builtin aggregate `{}` has no selected overload `{}`",
                declaration.name,
                selected_overload.as_str()
            )));
        }
        if !builtin_supports_ordered_update_channels(declaration.name)
            && !(declaration.min_args..=declaration.max_args).contains(&update_argument_types.len())
        {
            return Err(FunctionResolutionError::BadSignature(format!(
                "builtin aggregate `{}` does not support additional update channels",
                declaration.name
            )));
        }
        let (output_type, intermediate_type) =
            novarocks_types::aggregate::infer_agg_function_types(
                declaration.name,
                update_argument_types,
                false,
            )
            .map_err(FunctionResolutionError::BadSignature)?;
        let intermediate_type = intermediate_type.ok_or_else(|| {
            FunctionResolutionError::BadSignature(format!(
                "aggregate `{}` has no intermediate type",
                declaration.name
            ))
        })?;
        Ok(ResolvedAggregateSignature {
            overload,
            argument_types: update_argument_types.to_vec(),
            intermediate_type,
            output_type,
            state_format: AggregateStateFormatIdentity::try_new(format!(
                "novarocks/{}/state-v1",
                declaration.name
            ))
            .map_err(|error| FunctionResolutionError::BadSignature(error.to_string()))?,
        })
    }
}

fn builtin_supports_ordered_update_channels(name: &str) -> bool {
    matches!(
        name,
        "array_agg" | "array_agg_distinct" | "array_unique_agg" | "group_concat" | "string_agg"
    )
}

fn builtin_overload_identity(
    declaration: AggregateDeclaration,
) -> Result<AggregateOverloadIdentity, FunctionResolutionError> {
    AggregateOverloadIdentity::try_new(format!("builtin/{}/v1", declaration.name))
        .map_err(|error| FunctionResolutionError::BadSignature(error.to_string()))
}

const ONE_ARG_AGGREGATES: &[&str] = &[
    "any_value",
    "approx_count_distinct",
    "array_agg",
    "array_agg_distinct",
    "array_unique_agg",
    "avg",
    "bitmap_agg",
    "bitmap_union",
    "bitmap_union_count",
    "bitmap_union_int",
    "bool_and",
    "bool_or",
    "booland_agg",
    "boolor_agg",
    "count_distinct_state",
    "count_distinct_state_merge",
    "count_if",
    "dict_merge",
    "ds_hll_count_distinct_merge",
    "ds_hll_count_distinct_union",
    "hll_raw_agg",
    "hll_union",
    "hll_union_agg",
    "max",
    "min",
    "multi_distinct_count",
    "multi_distinct_sum",
    "ndv",
    "percentile_union",
    "sum",
    "sum_map",
    "variance",
    "variance_pop",
    "variance_samp",
    "var_pop",
    "var_samp",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "std",
    "approx_count_distinct_state_merge",
    "avg_state_merge",
    "bool_and_state_merge",
    "bool_or_state_merge",
    "count_state_merge",
    "max_state_merge",
    "min_state_merge",
    "sum_state_merge",
];

const STATE_ONE_ARG_AGGREGATES: &[&str] = &[
    "approx_count_distinct_state",
    "avg_state",
    "bool_and_state",
    "bool_or_state",
    "count_state",
    "max_state",
    "min_state",
    "sum_state",
];

const SIGNED_STATE_ONE_ARG_AGGREGATES: &[&str] = &[
    "approx_count_distinct_state_signed",
    "avg_state_signed",
    "bool_and_state_signed",
    "bool_or_state_signed",
    "count_distinct_state_signed",
    "count_state_signed",
    "max_state_signed",
    "min_state_signed",
    "sum_state_signed",
];

fn builtin_aggregate_declarations() -> Vec<AggregateDeclaration> {
    let mut declarations = Vec::new();
    declarations.extend(
        ONE_ARG_AGGREGATES
            .iter()
            .copied()
            .map(|name| AggregateDeclaration::exact(name, 1, "(any)->derived")),
    );
    declarations.extend(
        STATE_ONE_ARG_AGGREGATES
            .iter()
            .copied()
            .map(|name| AggregateDeclaration::exact(name, 1, "(any)->binary")),
    );
    declarations.extend(
        SIGNED_STATE_ONE_ARG_AGGREGATES
            .iter()
            .copied()
            .map(|name| AggregateDeclaration::exact(name, 1, "(row(value,change_op))->binary")),
    );
    declarations.extend([
        AggregateDeclaration::ranged("count", 0, 1, "()->i64 | (any)->i64"),
        AggregateDeclaration::ranged("group_concat", 2, usize::MAX, "(any,utf8...)->utf8"),
        AggregateDeclaration::ranged("string_agg", 2, usize::MAX, "(any,utf8...)->utf8"),
        AggregateDeclaration::exact("map_agg", 2, "(any,any)->map"),
        AggregateDeclaration::exact("max_by", 2, "(any,any)->any"),
        AggregateDeclaration::exact("min_by", 2, "(any,any)->any"),
        AggregateDeclaration::exact("min_n", 2, "(any,i64)->list<any>"),
        AggregateDeclaration::exact("max_n", 2, "(any,i64)->list<any>"),
        AggregateDeclaration::exact("corr", 2, "(any,any)->f64"),
        AggregateDeclaration::exact("covar_pop", 2, "(any,any)->f64"),
        AggregateDeclaration::exact("covar_samp", 2, "(any,any)->f64"),
        AggregateDeclaration::exact("percentile_cont", 2, "(any,f64)->any"),
        AggregateDeclaration::exact("percentile_disc", 2, "(any,f64)->any"),
        AggregateDeclaration::exact("percentile_disc_lc", 2, "(any,f64)->any"),
        AggregateDeclaration::ranged("percentile_approx", 2, 3, "(any,f64[,i64])->f64"),
        AggregateDeclaration::ranged(
            "percentile_approx_weighted",
            3,
            4,
            "(any,i64,f64[,i64])->f64",
        ),
        AggregateDeclaration::ranged("approx_top_k", 1, 3, "(any[,i64[,i64]])->list<struct>"),
        AggregateDeclaration::ranged("ds_hll_count_distinct", 1, 3, "(any[,i64[,utf8]])->i64"),
        AggregateDeclaration::ranged(
            "approx_count_distinct_hll_sketch",
            1,
            3,
            "(any[,i64[,utf8]])->i64",
        ),
        AggregateDeclaration::ranged("mann_whitney_u_test", 2, 4, "(any,bool[,utf8[,i64]])->utf8"),
    ]);
    declarations.sort_unstable_by_key(|declaration| declaration.name);
    declarations
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
    for declaration in builtin_aggregate_declarations() {
        let overload = AggregateOverloadDeclaration::try_new(
            format!("builtin/{}/v1", declaration.name),
            declaration.signature,
            "derived",
            "derived",
            format!("novarocks/{}/state-v1", declaration.name),
        )?;
        builder.register(FunctionDefinition::try_new_parametric_aggregate(
            declaration.name,
            FunctionVisibility::Public,
            FunctionVolatility::Immutable,
            [overload],
            Arc::new(BuiltinAggregateResolver { declaration }),
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

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn test_resolved_aggregate(
    name: &str,
    argument_types: &[DataType],
    distinct: bool,
) -> ResolvedAggregateSignature {
    let executable_name =
        novarocks_types::aggregate::mangle_distinct_aggregate_name(name, distinct);
    builtin_sql_function_catalog()
        .resolve_aggregate_trusted(&executable_name, argument_types)
        .unwrap_or_else(|error| {
            panic!("test aggregate `{executable_name}` must resolve exactly: {error}")
        })
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn test_function_catalog_snapshot() -> Arc<dyn crate::compiler::SqlFunctionCatalog> {
    builtin_sql_function_catalog().snapshot()
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
            (pair[0].canonical_name(), pair[0].kind()) < (pair[1].canonical_name(), pair[1].kind())
        }));
    }

    #[test]
    fn aggregate_resolution_is_catalog_backed_and_exact() {
        let catalog = build_builtin_engine_function_catalog().expect("builtin catalog");
        let resolved = catalog
            .resolve_aggregate_user("count", &[])
            .expect("count star resolves");
        assert_eq!(resolved.overload.as_str(), "builtin/count/v1");
        assert!(resolved.argument_types.is_empty());
        assert_eq!(resolved.intermediate_type, DataType::Int64);
        assert_eq!(resolved.output_type, DataType::Int64);
        assert_eq!(resolved.state_format.as_str(), "novarocks/count/state-v1");
        assert!(matches!(
            catalog.resolve_aggregate_user("map_agg", &[DataType::Int64]),
            Err(FunctionResolutionError::NoMatchingSignature { .. })
        ));
        assert_eq!(
            catalog.resolve_aggregate_user("not_an_aggregate", &[]),
            Err(FunctionResolutionError::UnknownFunction)
        );

        let std_user = catalog
            .resolve_aggregate_user("std", &[DataType::Int64])
            .expect("std alias resolves for user SQL");
        let std_trusted = catalog
            .resolve_aggregate_trusted("std", &[DataType::Int64])
            .expect("std alias resolves for trusted planning");
        assert_eq!(std_user, std_trusted);
        assert_eq!(std_user.overload.as_str(), "builtin/std/v1");
        assert_eq!(std_user.intermediate_type, DataType::Binary);
        assert_eq!(std_user.output_type, DataType::Float64);
        assert_eq!(std_user.state_format.as_str(), "novarocks/std/state-v1");
    }

    #[test]
    fn ordered_update_resolution_does_not_widen_logical_overloads() {
        let catalog = build_builtin_engine_function_catalog().expect("builtin catalog");
        assert!(matches!(
            catalog.resolve_aggregate_user("array_agg", &[DataType::Utf8, DataType::Int64]),
            Err(FunctionResolutionError::NoMatchingSignature { .. })
        ));

        let resolved = catalog
            .resolve_aggregate_update_user(
                "array_agg",
                &[DataType::Utf8],
                &[DataType::Utf8, DataType::Int64],
            )
            .expect("selected array_agg overload accepts one physical ORDER BY channel");
        assert_eq!(resolved.overload.as_str(), "builtin/array_agg/v1");
        assert_eq!(resolved.argument_types, [DataType::Utf8, DataType::Int64]);
        let DataType::Struct(fields) = resolved.intermediate_type else {
            panic!("ordered array_agg must expose a Struct intermediate");
        };
        assert_eq!(fields.len(), 2);

        assert!(matches!(
            catalog.resolve_aggregate_update_user(
                "sum",
                &[DataType::Int64],
                &[DataType::Int64, DataType::Utf8],
            ),
            Err(FunctionResolutionError::BadSignature(message))
                if message.contains("does not support function ORDER BY update channels")
        ));
    }

    #[test]
    fn analyzer_macros_are_not_published_as_executable_aggregate_overloads() {
        let catalog = build_builtin_engine_function_catalog().expect("builtin catalog");
        for name in ["ds_hll_accumulate", "ds_hll_combine", "ds_hll_estimate"] {
            assert!(
                catalog.definition(name, FunctionKind::Aggregate).is_none(),
                "{name} is analyzer syntax, not an executable aggregate"
            );
        }
        assert!(
            catalog
                .definition("ds_hll_count_distinct_state", FunctionKind::Aggregate)
                .is_none(),
            "ds_hll_count_distinct_state is an executable scalar"
        );
        assert!(
            catalog
                .definition("ds_hll_count_distinct_state", FunctionKind::Scalar)
                .is_some()
        );
        assert!(
            catalog
                .definition("every", FunctionKind::Aggregate)
                .is_none(),
            "EVERY is normalized to the executable BOOL_AND aggregate"
        );
        assert!(
            catalog
                .definition("bool_and", FunctionKind::Aggregate)
                .is_some()
        );
    }

    #[test]
    fn hidden_aggregate_is_rejected_by_sql_user_resolution() {
        let declaration = AggregateDeclaration::exact("$hidden_stat", 1, "(any)->i64");
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(
                FunctionDefinition::try_new_parametric_aggregate(
                    declaration.name,
                    FunctionVisibility::Hidden,
                    FunctionVolatility::Immutable,
                    [AggregateOverloadDeclaration::try_new(
                        "builtin/$hidden_stat/v1",
                        declaration.signature,
                        "derived",
                        "derived",
                        "novarocks/$hidden_stat/state-v1",
                    )
                    .expect("hidden overload")],
                    Arc::new(BuiltinAggregateResolver { declaration }),
                )
                .expect("hidden definition"),
            )
            .expect("register hidden definition");
        let catalog = builder.seal().expect("hidden catalog");
        assert!(crate::compiler::SqlFunctionCatalog::contains_aggregate(
            &catalog,
            "$hidden_stat"
        ));
        assert_eq!(
            crate::compiler::SqlFunctionCatalog::resolve_aggregate_signature(
                &catalog,
                "$hidden_stat",
                &[DataType::Int64]
            ),
            Err(FunctionResolutionError::HiddenFunction)
        );
    }
}
