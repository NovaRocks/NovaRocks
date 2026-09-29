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

mod intrinsic;
pub(crate) mod registry;
pub(crate) use intrinsic::{BuiltinDisposition, builtin_disposition};
pub(crate) mod resolver;
pub(crate) mod signature;

use std::sync::{Arc, LazyLock};

use arrow::datatypes::DataType;
use novarocks_functions::{
    AggregateBindingDeclaration, AggregateOverloadIdentity, AggregateSignatureResolver,
    AggregateStateFormatIdentity, EngineFunctionCatalog, EngineFunctionCatalogBuilder,
    FunctionArgument, FunctionArgumentEvaluation, FunctionArgumentType, FunctionBindingDeclaration,
    FunctionBindingError, FunctionBindingRequest, FunctionBindingResolver,
    FunctionBindingSelection, FunctionCatalogError, FunctionDefinition, FunctionFailureBehavior,
    FunctionId, FunctionKind, FunctionOverloadDeclaration, FunctionOverloadId,
    FunctionResolutionError, FunctionResultType, FunctionSemantics, FunctionValueType,
    FunctionVisibility, ResolvedAggregateSignature, ResolvedFunctionBinding,
};

#[cfg(test)]
use novarocks_functions::{AggregateOverloadDeclaration, AggregateOverloadMetadata};

#[cfg(test)]
pub(crate) use resolver::resolve_scalar_function;
pub(crate) use resolver::{ResolveError, ResolvedScalarFunction};

/// Evaluation stability of a scalar function call.
///
/// This is SQL semantic metadata, not an optimizer-local policy.  It is
/// intentionally carried by the immutable function catalog so that analysis,
/// lambda validation, CSE, predicate derivation, and aggregate pushdown make
/// the same decision.
pub(crate) use novarocks_functions::FunctionVolatility;

/// Semantic result domains declared by the closed built-in function identity.
/// Utf8 itself never establishes JSON provenance; external or shadowing functions
/// retain their own declared physical domain.
pub(crate) fn scalar_output_logical_type(
    binding: &ResolvedFunctionBinding,
) -> Option<novarocks_types::schema::SqlType> {
    if binding.kind != FunctionKind::Scalar {
        return None;
    }
    match binding.function_id.as_str() {
        "builtin.scalar/parse_json/v1"
        | "builtin.scalar/json_object/v1"
        | "builtin.scalar/json_array/v1"
        | "builtin.scalar/to_json/v1"
        | "builtin.scalar/json_query/v1" => Some(novarocks_types::schema::SqlType::Json),
        _ => None,
    }
}

pub(crate) fn aggregate_result_type(binding: &ResolvedFunctionBinding) -> &FunctionValueType {
    match &binding.selected.result_type {
        FunctionResultType::Scalar(result) => result,
        FunctionResultType::Relation(_) => {
            unreachable!("aggregate binding cannot produce a relation")
        }
    }
}

pub(crate) fn aggregate_selection(
    binding: &ResolvedFunctionBinding,
) -> &novarocks_functions::AggregateBindingSelection {
    binding
        .selected
        .aggregate
        .as_ref()
        .expect("aggregate binding must carry intermediate state")
}

impl crate::compiler::SqlFunctionCatalog for EngineFunctionCatalog {
    fn snapshot(&self) -> Arc<dyn crate::compiler::SqlFunctionCatalog> {
        Arc::new(self.clone())
    }

    fn resolve_scalar_signature(
        &self,
        name: &str,
        arg_types: &[DataType],
    ) -> Result<ResolvedScalarFunction, ResolveError> {
        let arguments = arg_types
            .iter()
            .cloned()
            .map(|data_type| FunctionArgument::Value {
                value_type: FunctionValueType::new(data_type, true),
                constant: None,
            })
            .collect::<Vec<_>>();
        let bound = self
            .resolve_bound_user(
                name,
                FunctionKind::Scalar,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: arguments.len(),
                },
            )
            .map_err(|error| match error {
                FunctionBindingError::UnknownFunction => ResolveError::UnknownFunction,
                FunctionBindingError::HiddenFunction => ResolveError::HiddenFunction,
                FunctionBindingError::NoMatchingOverload => ResolveError::NoMatchingSignature {
                    candidates: self
                        .definition(name, FunctionKind::Scalar)
                        .map(|definition| definition.canonical_signatures().len())
                        .unwrap_or_default(),
                    binding_enforced: true,
                },
                other => ResolveError::BadSignature(other.to_string()),
            })?;
        let FunctionResultType::Scalar(result) = bound.selected.result_type else {
            return Err(ResolveError::BadSignature(
                "scalar function selected a relation result".into(),
            ));
        };
        let argument_types = bound
            .selected
            .argument_types
            .into_vec()
            .into_iter()
            .map(|argument| match argument {
                FunctionArgumentType::Value(value) => Ok(value.data_type),
                FunctionArgumentType::Lambda { .. } => Err(ResolveError::BadSignature(
                    "legacy scalar signature cannot represent a lambda argument".into(),
                )),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ResolvedScalarFunction {
            return_type: result.data_type,
            argument_types,
            enforce_argument_binding: true,
        })
    }

    fn resolve_scalar_binding(
        &self,
        name: &str,
        arguments: &[FunctionArgument],
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_user(
            name,
            FunctionKind::Scalar,
            FunctionBindingRequest {
                arguments,
                logical_argument_count: arguments.len(),
            },
        )
    }

    fn resolve_window_binding(
        &self,
        name: &str,
        arguments: &[FunctionArgument],
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_user(
            name,
            FunctionKind::Window,
            FunctionBindingRequest {
                arguments,
                logical_argument_count: arguments.len(),
            },
        )
    }

    fn resolve_table_binding(
        &self,
        name: &str,
        arguments: &[FunctionArgument],
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_user(
            name,
            FunctionKind::Table,
            FunctionBindingRequest {
                arguments,
                logical_argument_count: arguments.len(),
            },
        )
    }

    fn contains_aggregate(&self, name: &str) -> bool {
        self.definition(name, FunctionKind::Aggregate).is_some()
    }

    fn resolve_aggregate_binding(
        &self,
        name: &str,
        logical_argument_count: usize,
        arguments: &[FunctionArgument],
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_user(
            name,
            FunctionKind::Aggregate,
            FunctionBindingRequest {
                arguments,
                logical_argument_count,
            },
        )
    }

    fn resolve_aggregate_binding_trusted(
        &self,
        name: &str,
        logical_argument_count: usize,
        arguments: &[FunctionArgument],
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_trusted(
            name,
            FunctionKind::Aggregate,
            FunctionBindingRequest {
                arguments,
                logical_argument_count,
            },
        )
    }

    fn resolve_aggregate_signature(
        &self,
        name: &str,
        arg_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        resolve_bound_aggregate(self, name, arg_types, arg_types, false)
    }

    fn resolve_aggregate_update_signature(
        &self,
        name: &str,
        logical_arg_types: &[DataType],
        update_arg_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        resolve_bound_aggregate(self, name, logical_arg_types, update_arg_types, false)
    }

    fn resolve_aggregate_trusted(
        &self,
        name: &str,
        arg_types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        resolve_bound_aggregate(self, name, arg_types, arg_types, true)
    }

    fn volatility(&self, name: &str) -> FunctionVolatility {
        self.definition(name, FunctionKind::Scalar)
            .or_else(|| self.definition(name, FunctionKind::Window))
            .map(FunctionDefinition::volatility)
            .unwrap_or_default()
    }
}

fn builtin_window_only(name: &str) -> bool {
    matches!(
        name,
        "row_number"
            | "rank"
            | "dense_rank"
            | "cume_dist"
            | "percent_rank"
            | "ntile"
            | "lag"
            | "lead"
            | "first_value"
            | "last_value"
            | "session_number"
    )
}

fn resolve_bound_aggregate(
    catalog: &EngineFunctionCatalog,
    name: &str,
    logical_arg_types: &[DataType],
    update_arg_types: &[DataType],
    trusted: bool,
) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
    let arguments = update_arg_types
        .iter()
        .cloned()
        .map(|data_type| FunctionArgument::Value {
            value_type: FunctionValueType::new(data_type, true),
            constant: None,
        })
        .collect::<Vec<_>>();
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: logical_arg_types.len(),
    };
    let binding = if trusted {
        catalog.resolve_bound_trusted(name, FunctionKind::Aggregate, request)
    } else {
        catalog.resolve_bound_user(name, FunctionKind::Aggregate, request)
    }
    .map_err(|error| match error {
        FunctionBindingError::UnknownFunction => FunctionResolutionError::UnknownFunction,
        FunctionBindingError::HiddenFunction => FunctionResolutionError::HiddenFunction,
        FunctionBindingError::NoMatchingOverload => FunctionResolutionError::NoMatchingSignature {
            candidates: 1,
            binding_enforced: true,
        },
        other => FunctionResolutionError::BadSignature(other.to_string()),
    })?;
    resolved_aggregate_signature_from_binding(binding)
}

pub(crate) fn resolved_aggregate_signature_from_binding(
    binding: ResolvedFunctionBinding,
) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
    let FunctionResultType::Scalar(output) = binding.selected.result_type else {
        return Err(FunctionResolutionError::BadSignature(
            "aggregate function selected a relation result".into(),
        ));
    };
    let aggregate = binding.selected.aggregate.ok_or_else(|| {
        FunctionResolutionError::BadSignature(
            "aggregate function selected no intermediate state".into(),
        )
    })?;
    let argument_types = binding
        .selected
        .argument_types
        .into_vec()
        .into_iter()
        .map(|argument| match argument {
            FunctionArgumentType::Value(value) => Ok(value.data_type),
            FunctionArgumentType::Lambda { .. } => Err(FunctionResolutionError::BadSignature(
                "aggregate update channel cannot be a lambda".into(),
            )),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ResolvedAggregateSignature {
        overload: AggregateOverloadIdentity::try_new(binding.selected.overload.as_str())
            .map_err(|error| FunctionResolutionError::BadSignature(error.to_string()))?,
        argument_types,
        intermediate_type: aggregate.intermediate_type.data_type,
        output_type: output.data_type,
        state_format: aggregate.state_format,
    })
}

pub(crate) fn resolve_sql_aggregate_binding(
    catalog: &dyn crate::compiler::SqlFunctionCatalog,
    name: &str,
    args: &[crate::analysis::TypedExpr],
    order_by: &[crate::analysis::SortItem],
    trusted: bool,
) -> Result<ResolvedFunctionBinding, String> {
    let arguments = args
        .iter()
        .map(crate::analysis::function_argument)
        .chain(
            order_by
                .iter()
                .map(|item| crate::analysis::function_argument(&item.expr)),
        )
        .collect::<Vec<_>>();
    let exact = if trusted {
        catalog.resolve_aggregate_binding_trusted(name, args.len(), &arguments)
    } else {
        catalog.resolve_aggregate_binding(name, args.len(), &arguments)
    }
    .map_err(|error| error.to_string())?;
    Ok(exact)
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
        if !builtin_aggregate_logical_arguments_match(declaration.name, argument_types) {
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
        if !builtin_aggregate_logical_arguments_match(declaration.name, update_argument_types) {
            return Err(FunctionResolutionError::BadSignature(format!(
                "builtin aggregate `{}` update arguments do not match its logical type contract",
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

impl FunctionBindingResolver for BuiltinAggregateResolver {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        let argument_types = request
            .arguments
            .iter()
            .map(|argument| match argument {
                FunctionArgument::Value { value_type, .. } => Ok(value_type.data_type.clone()),
                FunctionArgument::Lambda { .. } => Err(FunctionBindingError::NoMatchingOverload),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let logical_types = &argument_types[..request.logical_argument_count];
        let logical = self
            .resolve_aggregate(logical_types)
            .map_err(binding_resolution_error)?;
        let resolved = if request.logical_argument_count == argument_types.len() {
            logical
        } else {
            self.resolve_update_signature(&logical.overload, &argument_types)
                .map_err(binding_resolution_error)?
        };
        let argument_types = request
            .arguments
            .iter()
            .map(FunctionArgument::argument_type)
            .collect();
        Ok(FunctionBindingSelection {
            overload: FunctionOverloadId::try_new(resolved.overload.as_str())
                .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))?,
            argument_types,
            result_type: FunctionResultType::Scalar(FunctionValueType::new(
                resolved.output_type,
                builtin_aggregate_output_nullable(self.declaration.name),
            )),
            aggregate: Some(novarocks_functions::AggregateBindingSelection {
                intermediate_type: FunctionValueType::new(
                    resolved.intermediate_type,
                    builtin_aggregate_intermediate_nullable(self.declaration.name),
                ),
                state_format: resolved.state_format,
            }),
        })
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
    ) -> Result<(), FunctionBindingError> {
        let argument_types = request
            .arguments
            .iter()
            .map(|argument| match argument {
                FunctionArgument::Value { value_type, .. } => Ok(value_type.data_type.clone()),
                FunctionArgument::Lambda { .. } => Err(FunctionBindingError::NoMatchingOverload),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let logical_types = &argument_types[..request.logical_argument_count];
        let declaration = self.declaration;
        if !(declaration.min_args..=declaration.max_args).contains(&logical_types.len())
            || !builtin_aggregate_logical_arguments_match(declaration.name, logical_types)
        {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        let selected_overload = AggregateOverloadIdentity::try_new(selected.overload.as_str())
            .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))?;
        let resolved = AggregateSignatureResolver::resolve_update_signature(
            self,
            &selected_overload,
            &argument_types,
        )
        .map_err(binding_resolution_error)?;
        let expected = FunctionBindingSelection {
            overload: selected.overload.clone(),
            argument_types: request
                .arguments
                .iter()
                .map(FunctionArgument::argument_type)
                .collect(),
            result_type: FunctionResultType::Scalar(FunctionValueType::new(
                resolved.output_type,
                builtin_aggregate_output_nullable(self.declaration.name),
            )),
            aggregate: Some(novarocks_functions::AggregateBindingSelection {
                intermediate_type: FunctionValueType::new(
                    resolved.intermediate_type,
                    builtin_aggregate_intermediate_nullable(self.declaration.name),
                ),
                state_format: resolved.state_format,
            }),
        };
        if &expected == selected {
            Ok(())
        } else {
            Err(FunctionBindingError::InvalidBinding(
                "selected aggregate overload differs from exact registry resolution".into(),
            ))
        }
    }
}

/// Aggregates that answer with a number even for a group that saw no value.
///
/// Only the ones that really do. `approx_count_distinct`, `ndv` and
/// `bitmap_union_count` count the members of a union, and each of their
/// executors deliberately answers NULL for an empty union rather than 0 --
/// `bitmap_union_int_finalize_returns_null_for_empty_group` pins that. They
/// were listed here anyway, so a query that filtered every row away published
/// a non-nullable column and then delivered a NULL in it.
fn builtin_aggregate_output_nullable(name: &str) -> bool {
    !matches!(
        name,
        "count"
            | "count_if"
            | "multi_distinct_count"
            | "ds_hll_count_distinct"
            | "ds_hll_count_distinct_merge"
            | "approx_count_distinct_hll_sketch"
            | "count_state_signed"
            | "sum_state_signed"
            | "avg_state_signed"
            | "min_state_signed"
            | "max_state_signed"
            | "bool_or_state_signed"
            | "bool_and_state_signed"
    )
}

fn builtin_aggregate_intermediate_nullable(name: &str) -> bool {
    builtin_aggregate_output_nullable(name)
}

fn builtin_supports_ordered_update_channels(name: &str) -> bool {
    matches!(
        name,
        "array_agg" | "array_agg_distinct" | "array_unique_agg" | "group_concat" | "string_agg"
    )
}

fn builtin_aggregate_logical_arguments_match(name: &str, argument_types: &[DataType]) -> bool {
    if name != "dict_merge" {
        return true;
    }
    let [value_type, threshold_type] = argument_types else {
        return false;
    };
    let value_matches = matches!(value_type, DataType::Utf8)
        || matches!(value_type, DataType::List(item) if matches!(item.data_type(), DataType::Utf8));
    let threshold_matches = matches!(
        threshold_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    );
    value_matches && threshold_matches
}

/// How one builtin aggregate's single overload is spelled.
///
/// It names the family its function names, the way a scalar overload does: a
/// bare `builtin/count/v1` cannot be proven to belong to the aggregate `count`
/// rather than to any other function of that name. The backend spells this
/// same identity for itself when it registers implementations, so that sealing
/// compares two independently written sets rather than one copied twice.
/// Whether two argument types are the same but for what their nested fields
/// admit. See the caller for why that is not part of a type's identity here.
fn same_argument_up_to_nested_nullability(
    types: (
        &novarocks_functions::FunctionArgumentType,
        &novarocks_functions::FunctionArgumentType,
    ),
) -> bool {
    use novarocks_functions::FunctionArgumentType;
    match types {
        (FunctionArgumentType::Value(left), FunctionArgumentType::Value(right)) => {
            left.nullable == right.nullable
                && crate::literal::arrow_type_equals_ignoring_metadata(
                    &left.data_type,
                    &right.data_type,
                )
        }
        (left, right) => left == right,
    }
}

fn builtin_aggregate_overload(name: &str) -> String {
    format!("builtin.aggregate/{name}/derived-v1")
}

fn builtin_overload_identity(
    declaration: AggregateDeclaration,
) -> Result<AggregateOverloadIdentity, FunctionResolutionError> {
    AggregateOverloadIdentity::try_new(builtin_aggregate_overload(declaration.name))
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
    "ds_hll_count_distinct_merge",
    "ds_hll_count_distinct_union",
    "hll_raw_agg",
    "hll_union",
    "hll_union_agg",
    "max",
    "min",
    "multi_distinct_sum",
    "multi_distinct_avg",
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
        AggregateDeclaration::ranged("multi_distinct_count", 1, usize::MAX, "(any...)->i64"),
        AggregateDeclaration::exact("dict_merge", 2, "(utf8|list<utf8>,i8|i16|i32|i64)->utf8"),
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

// These generic declarations still freeze an exact installed input domain.
// Unsupported types must fail binding, before an optimizer can remove the call.
// ARRAY equality and hash share this recursive carrier domain. This is a
// selected-overload admission fact, not a runtime or optimizer name dispatch.
fn builtin_array_equality_item(ty: &DataType) -> bool {
    match ty {
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::Date32
        | DataType::Decimal128(..)
        | DataType::Timestamp(_, None) => true,
        DataType::List(item) => builtin_array_equality_item(item.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .all(|field| builtin_array_equality_item(field.data_type())),
        DataType::Map(entries, _) => {
            matches!(entries.data_type(),DataType::Struct(fields) if fields.len()==2)
                && builtin_array_equality_item(entries.data_type())
        }
        _ => novarocks_types::largeint::is_largeint_data_type(ty),
    }
}

fn validate_builtin_selected_domain(
    name: &str,
    arguments: &[DataType],
) -> Result<(), FunctionBindingError> {
    // Empty constructors ignore arguments. Value consumers must have every
    // argument they actually read, and an input carrier implemented by that
    // consumer. Do not infer this domain from the legacy Any declaration.
    let binary_or_null = |ty: &DataType| matches!(ty, DataType::Binary | DataType::Null);
    let text_or_bytes = |ty: &DataType| {
        matches!(
            ty,
            DataType::Null
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Binary
                | DataType::LargeBinary
        )
    };
    let numeric = |ty: &DataType| {
        matches!(
            ty,
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal128(..)
        ) || novarocks_types::largeint::is_largeint_data_type(ty)
    };
    let consumed = match name {
        "bitmap_and" | "bitmap_has_any" => {
            arguments.len() >= 2 && arguments.iter().take(2).all(binary_or_null)
        }
        "bitmap_count" | "bitmap_to_binary" | "bitmap_to_base64" => {
            arguments.first().is_some_and(binary_or_null)
        }
        "bitmap_from_binary" | "bitmap_from_string" => arguments.first().is_some_and(text_or_bytes),
        "percentile_hash" => arguments.first().is_some_and(numeric),
        "hll_hash" => arguments.first().is_some_and(|ty| {
            matches!(
                ty,
                DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Date32
                    | DataType::Timestamp(..)
                    | DataType::Decimal128(..)
                    | DataType::FixedSizeBinary(_)
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Binary
                    | DataType::LargeBinary
            )
        }),
        "to_bitmap" => arguments.first().is_some_and(|ty| {
            matches!(
                ty,
                DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Binary
                    | DataType::LargeBinary
            )
        }),
        "array_cum_sum" | "array_difference" => {
            arguments.len() == 1
                && matches!(&arguments[0], DataType::List(item) if matches!(item.data_type(),
                DataType::Boolean | DataType::Int8 | DataType::Int16 | DataType::Int32
                    | DataType::Int64 | DataType::Float32 | DataType::Float64
                    | DataType::Decimal128(..)))
        }
        _ => true,
    };
    if !consumed {
        return Err(FunctionBindingError::NoMatchingOverload);
    }
    let ordered_item = |ty: &DataType| {
        matches!(
            ty,
            DataType::Null
                | DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Utf8
                | DataType::Date32
                | DataType::Decimal128(..)
                | DataType::Timestamp(_, None)
        ) || novarocks_types::largeint::is_largeint_data_type(ty)
    };
    let ordered_list =
        |ty: &DataType| matches!(ty, DataType::List(item) if ordered_item(item.data_type()));
    // The shared comparator supports one closed primitive item domain. SORTBY
    // compares only key lists; arbitrary output values remain admitted.
    let bool_item = |ty: &DataType| arrow::compute::can_cast_types(ty, &DataType::Boolean);
    let shape = match name {
        "arrays_zip" => !arguments.is_empty() && arguments.iter().all(|ty| matches!(ty,DataType::List(_) | DataType::Null)),
        "map_entries" => arguments.len()==1 && matches!(&arguments[0],DataType::Map(_, _)),
        "array_contains" | "array_position" | "array_remove" | "array_distinct" =>
            arguments.first().is_some_and(|ty| matches!(ty,DataType::List(item) if builtin_array_equality_item(item.data_type()))),
        "all_match" | "any_match" => arguments.first().is_some_and(|ty| matches!(ty,DataType::List(item)
            if bool_item(item.data_type()) || novarocks_types::largeint::is_largeint_data_type(item.data_type()))),
        "array_filter" => arguments.get(1).is_some_and(|ty| matches!(ty,DataType::List(item) if bool_item(item.data_type()))),
        "array_flatten" => arguments.len()==1 && matches!(&arguments[0],DataType::List(outer) if matches!(outer.data_type(),DataType::List(_))),
        "array_repeat" => arguments.len()==2 && arrow::compute::can_cast_types(&arguments[1],&DataType::Int64),
        "distinct_map_keys" => arguments.first().is_some_and(|ty| matches!(ty,DataType::Map(entries,_) if
            matches!(entries.data_type(),DataType::Struct(fields) if fields.len()==2 &&
                ((ordered_item(fields[0].data_type()) && *fields[0].data_type()!=DataType::Null)
                    || matches!(fields[0].data_type(),DataType::Decimal256(..)))))),
        "array_contains_all" | "array_contains_seq" => match arguments {
            [DataType::List(left), DataType::List(right)] => {
                if arrow::compute::can_cast_types(right.data_type(),left.data_type()) {
                    builtin_array_equality_item(left.data_type())
                } else {
                    builtin_array_equality_item(right.data_type())
                        && arrow::compute::can_cast_types(left.data_type(),right.data_type())
                }
            },
            _ => false,
        },
        _ => true,
    };
    if !shape {
        return Err(FunctionBindingError::NoMatchingOverload);
    }
    let ordered = match name {
        "array_sort" | "array_top_n" | "array_min" | "array_max" => {
            arguments.first().is_some_and(ordered_list)
        }
        "array_sortby" => arguments.iter().skip(1).all(ordered_list),
        _ => true,
    };
    if !ordered {
        return Err(FunctionBindingError::NoMatchingOverload);
    }
    let admitted = |ty: &DataType| match name {
        "field" => {
            matches!(
                ty,
                DataType::Null
                    | DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Decimal128(..)
                    | DataType::Decimal256(..)
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Date32
                    | DataType::Timestamp(..)
            ) || novarocks_types::largeint::is_largeint_data_type(ty)
        }
        "mv_group_row_id" => matches!(
            ty,
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Date32
                | DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
                | DataType::Utf8
                | DataType::Decimal128(..)
        ),
        "encode_sort_key" | "encode_row_id" => {
            matches!(
                ty,
                DataType::Null
                    | DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Date32
                    | DataType::Timestamp(..)
                    | DataType::Utf8
                    | DataType::Binary
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Decimal128(..)
            ) || matches!(ty, DataType::FixedSizeBinary(16))
        }
        _ => true,
    };
    if arguments.iter().all(admitted) {
        Ok(())
    } else {
        Err(FunctionBindingError::NoMatchingOverload)
    }
}

struct BuiltinScalarResolver {
    canonical_name: Box<str>,
    overloads: Box<[FunctionOverloadId]>,
}

fn builtin_scalar_function_id(
    name: &str,
    kind: FunctionKind,
) -> Result<FunctionId, FunctionCatalogError> {
    let family = if kind == FunctionKind::Window {
        "window"
    } else {
        "scalar"
    };
    let value = format!("builtin.{family}/{name}/v1");
    FunctionId::try_new(&value).map_err(|_| FunctionCatalogError::InvalidStableIdentity {
        subject: "builtin scalar function",
        value: value.into(),
    })
}

fn builtin_scalar_overload_id(
    name: &str,
    canonical_signature: &str,
    kind: FunctionKind,
) -> Result<FunctionOverloadId, FunctionCatalogError> {
    let family = if kind == FunctionKind::Window {
        "window"
    } else {
        "scalar"
    };
    let value = format!("builtin.{family}/{name}/{canonical_signature}");
    FunctionOverloadId::try_new(&value).map_err(|_| FunctionCatalogError::InvalidStableIdentity {
        subject: "builtin scalar overload",
        value: value.into(),
    })
}

fn builtin_scalar_semantics(name: &str) -> FunctionSemantics {
    let argument_evaluation = match name {
        "case" | "coalesce" | "if" | "ifnull" | "nullif" | "nvl" => {
            FunctionArgumentEvaluation::ShortCircuit
        }
        _ => FunctionArgumentEvaluation::Eager,
    };
    FunctionSemantics {
        volatility: builtin_function_volatility(name),
        argument_evaluation,
        failure_behavior: FunctionFailureBehavior::Propagate,
        intrinsic_row_error: match builtin_disposition(name) {
            Some(BuiltinDisposition::InstalledScalar(fact)) => fact,
            Some(BuiltinDisposition::WindowBoundary) => {
                novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated
            }
            _ => unreachable!("only explicitly admitted builtin implementations have semantics"),
        },
    }
}

fn scalar_request_types(
    request: FunctionBindingRequest<'_>,
) -> Result<Vec<DataType>, FunctionBindingError> {
    request
        .arguments
        .iter()
        .map(|argument| match argument {
            FunctionArgument::Value { value_type, .. } => Ok(value_type.data_type.clone()),
            FunctionArgument::Lambda { .. } => Err(FunctionBindingError::NoMatchingOverload),
        })
        .collect()
}

fn scalar_result_nullable(name: &str, request: FunctionBindingRequest<'_>) -> bool {
    let value_nullable = |index: usize| match request.arguments.get(index) {
        Some(FunctionArgument::Value { value_type, .. }) => value_type.nullable,
        Some(FunctionArgument::Lambda { result_type, .. }) => result_type.nullable,
        None => false,
    };
    match name {
        // FIELD returns zero for NULL or absence, and a non-NULL first-match index otherwise.
        "field" => false,
        // A NULL predicate fails the assertion; successful evaluations are true.
        "assert_true"
        | "mv_group_row_id"
        | "state_all_zero"
        | "count_state_visible"
        | "count_distinct_state_visible"
        | "approx_count_distinct_state_visible"
        | "count_state_union"
        | "count_distinct_state_union"
        | "approx_count_distinct_state_union"
        | "avg_state_union"
        | "sum_state_union"
        | "min_state_union"
        | "max_state_union"
        | "bool_or_state_union"
        | "bool_and_state_union" => false,
        // These four decide their own result from the branches they choose
        // between, so their nullability really is their arguments'.
        "coalesce" | "ifnull" | "nvl" => (0..request.logical_argument_count).all(value_nullable),
        "if" => (1..request.logical_argument_count).any(value_nullable),
        "case" => (1..request.logical_argument_count)
            .step_by(2)
            .any(value_nullable),
        // A function that is total -- one that answers for every value of
        // its declared argument types -- passes its arguments' nullability
        // through. Everything else is nullable.
        name if TOTAL_SCALAR_FUNCTIONS.contains(&name) => {
            (0..request.logical_argument_count).any(value_nullable)
        }
        _ => true,
    }
}

/// Scalar functions that answer for every value of their argument types, and
/// so return NULL only where an argument was already NULL.
///
/// This used to be stated the other way round: results were non-null unless
/// the function appeared on a list of exceptions. But a scalar function in
/// this engine returns NULL for any input outside its domain -- an unparsable
/// bitmap, a decimal that overflows, a string operation whose result is too
/// long, `substring_index(s, d, 0)`, a time before zero -- and that describes
/// most of the string, date and bitmap families rather than a handful of
/// names. An exception list of that shape could never be finished, and every
/// name missing from it was a column the planner promised could not be null
/// and then filled with nulls.
///
/// Stated this way each entry is a claim someone made deliberately, and being
/// wrong about a name that is *absent* costs only an optimization. The
/// aggregate side already defaults the same way.
const TOTAL_SCALAR_FUNCTIONS: &[&str] = &[
    // Sign manipulation answers for every number it accepts; overflow is an
    // error here, not a null.
    "abs",
    "negative",
    "positive",
    "sign",
    // Measuring a string cannot fail.
    "bit_length",
    "char_length",
    "character_length",
    "length",
    "octet_length",
    // The join key hashes every non-null pair and propagates null inputs.
    "join_row_key",
    // Case folding is defined for every string.
    "lcase",
    "lower",
    "ucase",
    "upper",
    // A null test is the one thing that is never null.
    "isnull",
];

fn binding_resolution_error(error: ResolveError) -> FunctionBindingError {
    match error {
        ResolveError::UnknownFunction => FunctionBindingError::UnknownFunction,
        ResolveError::HiddenFunction => FunctionBindingError::HiddenFunction,
        ResolveError::NoMatchingSignature { .. } => FunctionBindingError::NoMatchingOverload,
        ResolveError::BadSignature(message) => FunctionBindingError::InvalidBinding(message.into()),
    }
}

impl FunctionBindingResolver for BuiltinScalarResolver {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        let argument_types = scalar_request_types(request)?;
        let (index, resolved) = resolver::resolve_scalar_function_signature_with_overload(
            &self.canonical_name,
            &argument_types,
        )
        .map_err(binding_resolution_error)?;
        validate_builtin_selected_domain(&self.canonical_name, &resolved.argument_types)?;
        let selected_argument_types = request
            .arguments
            .iter()
            .zip(resolved.argument_types)
            .map(|(argument, data_type)| match argument {
                FunctionArgument::Value { value_type, .. } => FunctionArgumentType::Value(
                    FunctionValueType::new(data_type, value_type.nullable),
                ),
                FunctionArgument::Lambda { .. } => unreachable!("scalar registry has no lambdas"),
            })
            .collect();
        Ok(FunctionBindingSelection {
            overload: self.overloads[index].clone(),
            argument_types: selected_argument_types,
            result_type: FunctionResultType::Scalar(FunctionValueType::new(
                resolved.return_type,
                scalar_result_nullable(&self.canonical_name, request),
            )),
            aggregate: None,
        })
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
    ) -> Result<(), FunctionBindingError> {
        let overload_index = self
            .overloads
            .iter()
            .position(|overload| overload == &selected.overload)
            .ok_or_else(|| {
                FunctionBindingError::InvalidBinding(
                    "selected scalar overload is not declared by this function".into(),
                )
            })?;
        let argument_types = scalar_request_types(request)?;
        let resolved = resolver::resolve_scalar_function_signature_at_overload(
            &self.canonical_name,
            overload_index,
            &argument_types,
        )
        .map_err(binding_resolution_error)?;
        validate_builtin_selected_domain(&self.canonical_name, &resolved.argument_types)?;
        let expected = FunctionBindingSelection {
            overload: selected.overload.clone(),
            argument_types: request
                .arguments
                .iter()
                .map(FunctionArgument::argument_type)
                .collect(),
            result_type: FunctionResultType::Scalar(FunctionValueType::new(
                resolved.return_type,
                scalar_result_nullable(&self.canonical_name, request),
            )),
            aggregate: None,
        };
        if &expected == selected {
            return Ok(());
        }
        // What a nested field admits is not part of a type's identity across
        // this boundary -- a map read from Iceberg has non-null keys while the
        // same type declared from SQL says they may be null, which is what
        // `literal::arrow_type_equals_ignoring_metadata` exists to say. So a
        // binding whose arguments differ only there is the same binding.
        if expected.overload == selected.overload
            && expected.result_type == selected.result_type
            && expected.aggregate == selected.aggregate
            && expected.argument_types.len() == selected.argument_types.len()
            && expected
                .argument_types
                .iter()
                .zip(selected.argument_types.iter())
                .all(same_argument_up_to_nested_nullability)
        {
            return Ok(());
        }
        // Name the part that differs: the whole selection does not fit in one
        // error line, and every field of it can drift for its own reason.
        let differing = if expected.argument_types != selected.argument_types {
            let ordinal = expected
                .argument_types
                .iter()
                .zip(selected.argument_types.iter())
                .position(|(registry, plan)| registry != plan);
            match ordinal {
                Some(ordinal) => format!(
                    "argument {ordinal}: plan {:?}, registry {:?}",
                    selected.argument_types[ordinal], expected.argument_types[ordinal]
                ),
                None => format!(
                    "argument count: plan {}, registry {}",
                    selected.argument_types.len(),
                    expected.argument_types.len()
                ),
            }
        } else if expected.result_type != selected.result_type {
            format!(
                "result type: registry {:?}, plan {:?}",
                expected.result_type, selected.result_type
            )
        } else {
            format!(
                "aggregate state: registry {:?}, plan {:?}",
                expected.aggregate, selected.aggregate
            )
        };
        Err(FunctionBindingError::InvalidBinding(
            format!("selected scalar overload differs from exact registry resolution: {differing}")
                .into(),
        ))
    }
}

const BUILTIN_UNNEST_FUNCTION_ID: &str = "builtin.table/unnest/v1";
const BUILTIN_UNNEST_OVERLOAD_ID: &str = "builtin.table/unnest/array-variadic-v1";

struct BuiltinUnnestResolver;

fn bind_builtin_unnest(
    request: FunctionBindingRequest<'_>,
) -> Result<FunctionBindingSelection, FunctionBindingError> {
    if request.logical_argument_count != request.arguments.len() || request.arguments.is_empty() {
        return Err(FunctionBindingError::NoMatchingOverload);
    }
    let mut argument_types = Vec::with_capacity(request.arguments.len());
    let mut result_columns = Vec::with_capacity(request.arguments.len());
    for argument in request.arguments {
        let FunctionArgument::Value { value_type, .. } = argument else {
            return Err(FunctionBindingError::NoMatchingOverload);
        };
        let DataType::List(item) = &value_type.data_type else {
            return Err(FunctionBindingError::NoMatchingOverload);
        };
        argument_types.push(FunctionArgumentType::Value(value_type.clone()));
        // The current UNNEST operator exposes nullable output slots so a
        // lateral left join can null-extend them without changing its binding.
        result_columns.push(FunctionValueType::new(item.data_type().clone(), true));
    }
    Ok(FunctionBindingSelection {
        overload: FunctionOverloadId::try_new(BUILTIN_UNNEST_OVERLOAD_ID)
            .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))?,
        argument_types: argument_types.into_boxed_slice(),
        result_type: FunctionResultType::Relation(result_columns.into_boxed_slice()),
        aggregate: None,
    })
}

impl FunctionBindingResolver for BuiltinUnnestResolver {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        bind_builtin_unnest(request)
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
    ) -> Result<(), FunctionBindingError> {
        if selected.overload.as_str() != BUILTIN_UNNEST_OVERLOAD_ID {
            return Err(FunctionBindingError::UnknownOverload(
                selected.overload.clone(),
            ));
        }
        let expected = bind_builtin_unnest(request)?;
        if selected == &expected {
            Ok(())
        } else {
            Err(FunctionBindingError::InvalidBinding(
                "selected UNNEST binding differs from its declared overload".into(),
            ))
        }
    }
}

const DYNAMIC_SCALAR_FUNCTIONS: &[&str] = &[
    "__array_literal",
    "__array_struct_subfield",
    "__iceberg_transform_void",
    "__struct_subfield",
    "array_avg",
    "array_cum_sum",
    "array_difference",
    "array_flatten",
    "array_generate",
    "array_intersect",
    "array_map",
    "array_repeat",
    "array_sort_lambda",
    "array_sum",
    "arrays_zip",
    "greatest",
    "least",
    "map",
    "map_concat",
    "map_entries",
    "map_from_arrays",
    "map_apply",
    "md5sum_numeric",
    "named_struct",
    "null_or_empty",
    "round",
    "row",
    "str_to_map",
    "struct",
    "truncate",
    "transform_keys",
    "transform_values",
    "try_variant_get",
    "variant_get",
    "xx_hash3_128",
];

struct BuiltinDynamicScalarResolver {
    canonical_name: Box<str>,
    overload: FunctionOverloadId,
}

fn dynamic_argument_data_types(request: FunctionBindingRequest<'_>) -> Vec<DataType> {
    request
        .arguments
        .iter()
        .map(|argument| match argument {
            FunctionArgument::Value { value_type, .. } => value_type.data_type.clone(),
            FunctionArgument::Lambda { result_type, .. } => result_type.data_type.clone(),
        })
        .collect()
}

fn utf8_constant(argument: Option<&FunctionArgument>) -> Option<&str> {
    match argument {
        Some(FunctionArgument::Value {
            constant: Some(novarocks_functions::FunctionLiteral::Utf8(value)),
            ..
        }) => Some(value),
        _ => None,
    }
}

fn int64_constant(argument: Option<&FunctionArgument>) -> Option<i64> {
    match argument {
        Some(FunctionArgument::Value {
            constant: Some(novarocks_functions::FunctionLiteral::Int64(value)),
            ..
        }) => Some(*value),
        _ => None,
    }
}

fn struct_field_type(data_type: &DataType, field_name: &str) -> Option<DataType> {
    let DataType::Struct(fields) = data_type else {
        return None;
    };
    fields
        .iter()
        .find(|field| field.name().eq_ignore_ascii_case(field_name))
        .map(|field| field.data_type().clone())
}

fn list_type(item_type: DataType) -> DataType {
    DataType::List(Arc::new(arrow::datatypes::Field::new(
        "item", item_type, true,
    )))
}

fn list_item_type(data_type: &DataType) -> Option<DataType> {
    match data_type {
        DataType::List(item) => Some(item.data_type().clone()),
        _ => None,
    }
}

fn map_key_value_types(data_type: &DataType) -> Option<(DataType, DataType)> {
    let DataType::Map(entries, _) = data_type else {
        return None;
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return None;
    };
    (fields.len() == 2).then(|| (fields[0].data_type().clone(), fields[1].data_type().clone()))
}

fn map_type(key_type: DataType, value_type: DataType) -> DataType {
    DataType::Map(
        Arc::new(arrow::datatypes::Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(arrow::datatypes::Field::new("key", key_type, true)),
                    Arc::new(arrow::datatypes::Field::new("value", value_type, true)),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

/// Type-only projection of owner-bound dynamic scalar rules. This exists for
/// legacy explain/type inspection callers; executable SQL always resolves the
/// full literal-aware binding below.
pub(crate) fn dynamic_scalar_data_type(
    name: &str,
    argument_types: &[DataType],
) -> Option<DataType> {
    let widen_all = |types: &[DataType]| {
        types
            .iter()
            .cloned()
            .reduce(|left, right| novarocks_types::wider_type(&left, &right))
            .unwrap_or(DataType::Null)
    };
    Some(match name {
        "__array_literal" => list_type(
            argument_types
                .iter()
                .cloned()
                .reduce(|left, right| novarocks_types::wider_type(&left, &right))
                .unwrap_or(DataType::Null),
        ),
        "__array_struct_subfield" => DataType::Null,
        "__iceberg_transform_void" => DataType::Null,
        "__struct_subfield" => DataType::Null,
        "array_avg" => match argument_types.first().and_then(list_item_type) {
            Some(DataType::Decimal128(_, scale)) => {
                let scale = if scale <= 6 {
                    scale + 6
                } else if scale <= 12 {
                    12
                } else {
                    scale
                };
                DataType::Decimal128(38, scale)
            }
            Some(_) => DataType::Float64,
            None => DataType::Null,
        },
        "array_cum_sum" | "array_difference" => {
            list_type(match argument_types.first().and_then(list_item_type) {
                Some(
                    DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64,
                ) => DataType::Int64,
                Some(DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _)) => {
                    DataType::Float64
                }
                Some(other) => other,
                None => DataType::Null,
            })
        }
        "array_flatten" => match argument_types.first() {
            Some(DataType::List(outer)) => match outer.data_type() {
                DataType::List(inner) => list_type(inner.data_type().clone()),
                _ => argument_types[0].clone(),
            },
            _ => DataType::Null,
        },
        "array_generate" => list_type(novarocks_type_contract::array_generate_item_type(
            argument_types,
        )?),
        "array_intersect" => list_type(
            argument_types
                .iter()
                .filter_map(list_item_type)
                .reduce(|left, right| novarocks_types::wider_type(&left, &right))
                .unwrap_or(DataType::Null),
        ),
        "array_repeat" => list_type(argument_types.first().cloned().unwrap_or(DataType::Null)),
        "array_sum" => match argument_types.first().and_then(list_item_type) {
            Some(
                DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64,
            ) => DataType::Int64,
            Some(DataType::Float32 | DataType::Float64 | DataType::Utf8 | DataType::LargeUtf8) => {
                DataType::Float64
            }
            Some(DataType::Decimal128(_, scale)) => DataType::Decimal128(38, scale),
            Some(DataType::FixedSizeBinary(width))
                if width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
            {
                DataType::FixedSizeBinary(width)
            }
            _ => DataType::Null,
        },
        "arrays_zip" => list_type(DataType::Struct(
            argument_types
                .iter()
                .enumerate()
                .map(|(index, data_type)| {
                    Arc::new(arrow::datatypes::Field::new(
                        format!("col{}", index + 1),
                        list_item_type(data_type).unwrap_or(DataType::Null),
                        true,
                    ))
                })
                .collect::<Vec<_>>()
                .into(),
        )),
        "greatest" | "least" => {
            let result = widen_all(argument_types);
            if result == DataType::Date32 {
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
            } else {
                result
            }
        }
        "map" => {
            let keys = argument_types
                .iter()
                .step_by(2)
                .cloned()
                .collect::<Vec<_>>();
            let values = argument_types
                .iter()
                .skip(1)
                .step_by(2)
                .cloned()
                .collect::<Vec<_>>();
            map_type(widen_all(&keys), widen_all(&values))
        }
        "map_concat" => {
            let mut entries = argument_types.iter().filter_map(map_key_value_types);
            let Some((mut key, mut value)) = entries.next() else {
                return Some(DataType::Null);
            };
            for (next_key, next_value) in entries {
                key = novarocks_types::wider_type(&key, &next_key);
                value = novarocks_types::wider_type(&value, &next_value);
            }
            map_type(key, value)
        }
        "map_entries" => argument_types
            .first()
            .and_then(|data_type| match data_type {
                DataType::Map(entries, _) => Some(list_type(entries.data_type().clone())),
                _ => None,
            })
            .unwrap_or(DataType::Null),
        "map_from_arrays" => match (argument_types.first(), argument_types.get(1)) {
            (Some(DataType::List(keys)), Some(DataType::List(values))) => {
                map_type(keys.data_type().clone(), values.data_type().clone())
            }
            _ => DataType::Null,
        },
        "md5sum_numeric" | "xx_hash3_128" => {
            DataType::FixedSizeBinary(novarocks_types::largeint::LARGEINT_BYTE_WIDTH)
        }
        "named_struct" => DataType::Struct(
            argument_types
                .iter()
                .skip(1)
                .step_by(2)
                .enumerate()
                .map(|(index, data_type)| {
                    Arc::new(arrow::datatypes::Field::new(
                        format!("col{}", index + 1),
                        data_type.clone(),
                        true,
                    ))
                })
                .collect::<Vec<_>>()
                .into(),
        ),
        "null_or_empty" => DataType::Boolean,
        "round" | "truncate" => match argument_types.first() {
            Some(DataType::Decimal128(_, scale)) => DataType::Decimal128(38, *scale),
            _ if argument_types.len() >= 2 => DataType::Float64,
            _ => DataType::Int64,
        },
        "row" | "struct" => DataType::Struct(
            argument_types
                .iter()
                .enumerate()
                .map(|(index, data_type)| {
                    Arc::new(arrow::datatypes::Field::new(
                        format!("col{}", index + 1),
                        data_type.clone(),
                        true,
                    ))
                })
                .collect::<Vec<_>>()
                .into(),
        ),
        "str_to_map" => map_type(DataType::Utf8, DataType::Utf8),
        "try_variant_get" | "variant_get" => DataType::LargeBinary,
        "array_map" | "array_sort_lambda" | "map_apply" | "transform_keys" | "transform_values" => {
            return None;
        }
        _ => return None,
    })
}

fn bind_dynamic_scalar_result(
    name: &str,
    request: FunctionBindingRequest<'_>,
) -> Result<FunctionValueType, FunctionBindingError> {
    let argument_types = dynamic_argument_data_types(request);
    validate_builtin_selected_domain(name, &argument_types)?;
    let mut result = match name {
        "array_generate" => list_type(
            novarocks_type_contract::array_generate_item_type(&argument_types)
                .ok_or(FunctionBindingError::NoMatchingOverload)?,
        ),
        "array_map" => {
            let Some(FunctionArgument::Lambda { result_type, .. }) = request.arguments.first()
            else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            // A bare NULL is a value of whatever the position holds, exactly
            // as `TypeSpec::List` reads it in a declared signature. Demanding
            // a List here made `array_map(f, [1, 2], NULL)` a binding error
            // instead of NULL.
            if request.arguments.len() < 2
                || request.arguments[1..].iter().any(|argument| {
                    !matches!(
                        argument,
                        FunctionArgument::Value {
                            value_type: FunctionValueType {
                                data_type: DataType::List(_) | DataType::Null,
                                ..
                            },
                            ..
                        }
                    )
                })
            {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                result_type.data_type.clone(),
                true,
            )))
        }
        "array_sort_lambda" => {
            let [
                FunctionArgument::Value { value_type, .. },
                FunctionArgument::Lambda { .. },
            ] = request.arguments
            else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            if !matches!(value_type.data_type, DataType::List(_)) {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            value_type.data_type.clone()
        }
        "__array_literal" => {
            // The element type is the one every element fits in, not the one
            // the first element happens to have. Taking the first meant
            // `[1, 300]` was an array of TINYINT and the 300 became NULL, and
            // `[1, 2.5]` was an array of integers with the 2.5 truncated --
            // silently, since nothing downstream can tell a narrowed literal
            // from a NULL the query asked for.
            let item_type = argument_types
                .iter()
                .cloned()
                .reduce(|left, right| novarocks_types::wider_type(&left, &right))
                .unwrap_or(DataType::Null);
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item", item_type, true,
            )))
        }
        "__struct_subfield" => {
            let Some(field_name) = utf8_constant(request.arguments.get(1)) else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            struct_field_type(
                argument_types.first().unwrap_or(&DataType::Null),
                field_name,
            )
            .ok_or(FunctionBindingError::NoMatchingOverload)?
        }
        "__array_struct_subfield" => {
            let Some(field_name) = utf8_constant(request.arguments.get(1)) else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            let Some(DataType::List(item)) = argument_types.first() else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            let field_type = struct_field_type(item.data_type(), field_name)
                .ok_or(FunctionBindingError::NoMatchingOverload)?;
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item", field_type, true,
            )))
        }
        "named_struct" => {
            if request.arguments.is_empty() || !request.arguments.len().is_multiple_of(2) {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            let mut fields = Vec::with_capacity(request.arguments.len() / 2);
            for pair in request.arguments.chunks_exact(2) {
                let Some(field_name) = utf8_constant(pair.first()) else {
                    return Err(FunctionBindingError::NoMatchingOverload);
                };
                let FunctionArgument::Value { value_type, .. } = &pair[1] else {
                    return Err(FunctionBindingError::NoMatchingOverload);
                };
                fields.push(Arc::new(arrow::datatypes::Field::new(
                    field_name,
                    value_type.data_type.clone(),
                    true,
                )));
            }
            DataType::Struct(fields.into())
        }
        "map_apply" | "transform_keys" | "transform_values" => {
            let [
                FunctionArgument::Lambda { result_type, .. },
                FunctionArgument::Value { value_type, .. },
            ] = request.arguments
            else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            if !matches!(value_type.data_type, DataType::Map(_, _))
                || !matches!(result_type.data_type, DataType::Map(_, _))
            {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            result_type.data_type.clone()
        }
        other => dynamic_scalar_data_type(other, &argument_types)
            .ok_or(FunctionBindingError::UnknownFunction)?,
    };
    if matches!(name, "round" | "truncate")
        && let DataType::Decimal128(precision, scale) = result
        && let Some(decimal_places) = int64_constant(request.arguments.get(1))
    {
        result = DataType::Decimal128(precision, (decimal_places as i8).max(0).min(scale));
    }
    if matches!(name, "variant_get" | "try_variant_get") {
        if !(2..=3).contains(&request.arguments.len())
            || utf8_constant(request.arguments.get(1)).is_none()
        {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        if request.arguments.len() == 3 {
            let Some(target) = utf8_constant(request.arguments.get(2)) else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            result = novarocks_types::value::variant::variant_get_target_type(target)
                .map_err(|message| FunctionBindingError::InvalidBinding(message.into()))?;
        }
    }
    let nullable = match name {
        "__array_literal" | "map" | "named_struct" | "row" | "struct" => false,
        "__struct_subfield" | "__array_struct_subfield" => true,
        _ => scalar_result_nullable(name, request),
    };
    Ok(FunctionValueType::new(result, nullable))
}

impl FunctionBindingResolver for BuiltinDynamicScalarResolver {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        if request.logical_argument_count != request.arguments.len() {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        Ok(FunctionBindingSelection {
            overload: self.overload.clone(),
            argument_types: request
                .arguments
                .iter()
                .map(FunctionArgument::argument_type)
                .collect(),
            result_type: FunctionResultType::Scalar(bind_dynamic_scalar_result(
                &self.canonical_name,
                request,
            )?),
            aggregate: None,
        })
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
    ) -> Result<(), FunctionBindingError> {
        if selected.overload != self.overload {
            return Err(FunctionBindingError::UnknownOverload(
                selected.overload.clone(),
            ));
        }
        let expected = FunctionBindingSelection {
            overload: self.overload.clone(),
            argument_types: request
                .arguments
                .iter()
                .map(FunctionArgument::argument_type)
                .collect(),
            result_type: FunctionResultType::Scalar(bind_dynamic_scalar_result(
                &self.canonical_name,
                request,
            )?),
            aggregate: None,
        };
        if selected == &expected {
            Ok(())
        } else {
            Err(FunctionBindingError::InvalidBinding(
                "selected dynamic scalar binding differs from its declared overload".into(),
            ))
        }
    }
}

pub fn contribute_builtin_functions(
    builder: &mut EngineFunctionCatalogBuilder,
) -> Result<(), FunctionCatalogError> {
    for name in DYNAMIC_SCALAR_FUNCTIONS {
        if !matches!(
            builtin_disposition(name),
            Some(BuiltinDisposition::InstalledScalar(_))
        ) {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "unclassified dynamic scalar implementation",
                value: (*name).into(),
            });
        }
        let function_id =
            FunctionId::try_new(format!("builtin.scalar/{name}/v1")).map_err(|error| {
                FunctionCatalogError::InvalidStableIdentity {
                    subject: "dynamic scalar function",
                    value: error.to_string().into(),
                }
            })?;
        let overload = FunctionOverloadId::try_new(format!("builtin.scalar/{name}/dynamic-v1"))
            .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
                subject: "dynamic scalar overload",
                value: error.to_string().into(),
            })?;
        let declaration = FunctionBindingDeclaration::try_new(
            function_id,
            FunctionKind::Scalar,
            builtin_scalar_semantics(name),
            [FunctionOverloadDeclaration {
                identity: overload.clone(),
                argument_pattern: "owner-derived".into(),
                result_pattern: "owner-derived".into(),
                aggregate: None,
            }],
        )
        .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
            subject: "dynamic scalar binding declaration",
            value: error.to_string().into(),
        })?;
        builder.register(FunctionDefinition::try_new_bound(
            name,
            FunctionVisibility::Public,
            declaration,
            Arc::new(BuiltinDynamicScalarResolver {
                canonical_name: (*name).into(),
                overload,
            }),
        )?)?;
    }
    for (name, signatures) in registry::builtin_scalar_declarations() {
        let kind = match builtin_disposition(&name) {
            Some(BuiltinDisposition::InstalledScalar(_)) => FunctionKind::Scalar,
            Some(BuiltinDisposition::WindowBoundary) => FunctionKind::Window,
            Some(
                BuiltinDisposition::AggregateBoundary
                | BuiltinDisposition::LoweredOnly
                | BuiltinDisposition::Unavailable,
            ) => continue,
            None => {
                return Err(FunctionCatalogError::InvalidStableIdentity {
                    subject: "unclassified builtin implementation",
                    value: name.into(),
                });
            }
        };
        let overloads = signatures
            .iter()
            .map(|signature| builtin_scalar_overload_id(&name, signature, kind))
            .collect::<Result<Vec<_>, _>>()?;
        let resolver = Arc::new(BuiltinScalarResolver {
            canonical_name: name.clone().into_boxed_str(),
            overloads: overloads.clone().into_boxed_slice(),
        });
        let declaration = FunctionBindingDeclaration::try_new(
            builtin_scalar_function_id(&name, kind)?,
            kind,
            builtin_scalar_semantics(&name),
            overloads
                .into_iter()
                .zip(&signatures)
                .map(|(identity, signature)| FunctionOverloadDeclaration {
                    identity,
                    argument_pattern: signature.clone().into_boxed_str(),
                    result_pattern: signature.clone().into_boxed_str(),
                    aggregate: None,
                }),
        )
        .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin scalar binding declaration",
            value: error.to_string().into(),
        })?;
        builder.register(FunctionDefinition::try_new_bound(
            &name,
            FunctionVisibility::Public,
            declaration,
            resolver,
        )?)?;
    }
    for declaration in builtin_aggregate_declarations() {
        let function_id = FunctionId::try_new(format!("builtin.aggregate/{}/v1", declaration.name))
            .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin aggregate function",
                value: error.to_string().into(),
            })?;
        let overload_id = FunctionOverloadId::try_new(builtin_aggregate_overload(declaration.name))
            .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin aggregate overload",
                value: error.to_string().into(),
            })?;
        let state_format = AggregateStateFormatIdentity::try_new(format!(
            "novarocks/{}/state-v1",
            declaration.name
        ))
        .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin aggregate state format",
            value: error.to_string().into(),
        })?;
        let binding_declaration = FunctionBindingDeclaration::try_new(
            function_id,
            FunctionKind::Aggregate,
            FunctionSemantics {
                volatility: FunctionVolatility::Immutable,
                argument_evaluation: FunctionArgumentEvaluation::Eager,
                failure_behavior: FunctionFailureBehavior::Propagate,
                intrinsic_row_error:
                    novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated,
            },
            [FunctionOverloadDeclaration {
                identity: overload_id,
                argument_pattern: declaration.signature.into(),
                result_pattern: "derived".into(),
                aggregate: Some(AggregateBindingDeclaration {
                    intermediate_pattern: "derived".into(),
                    state_format,
                }),
            }],
        )
        .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
            subject: "builtin aggregate binding declaration",
            value: error.to_string().into(),
        })?;
        // One resolver in both roles: it answers binding questions and it is
        // the typed signature contract an aggregate is resolved through.
        let resolver = Arc::new(BuiltinAggregateResolver { declaration });
        builder.register(FunctionDefinition::try_new_bound_aggregate(
            declaration.name,
            FunctionVisibility::Public,
            binding_declaration,
            Arc::clone(&resolver) as Arc<dyn novarocks_functions::FunctionBindingResolver>,
            resolver as Arc<dyn novarocks_functions::AggregateSignatureResolver>,
        )?)?;
    }
    let unnest_declaration = FunctionBindingDeclaration::try_new(
        FunctionId::try_new(BUILTIN_UNNEST_FUNCTION_ID).map_err(|error| {
            FunctionCatalogError::InvalidStableIdentity {
                subject: "builtin table function",
                value: error.to_string().into(),
            }
        })?,
        FunctionKind::Table,
        FunctionSemantics {
            volatility: FunctionVolatility::Immutable,
            argument_evaluation: FunctionArgumentEvaluation::Eager,
            failure_behavior: FunctionFailureBehavior::Propagate,
            intrinsic_row_error: novarocks_type_contract::FunctionIntrinsicRowError::NoRowError,
        },
        [FunctionOverloadDeclaration {
            identity: FunctionOverloadId::try_new(BUILTIN_UNNEST_OVERLOAD_ID).map_err(|error| {
                FunctionCatalogError::InvalidStableIdentity {
                    subject: "builtin table function overload",
                    value: error.to_string().into(),
                }
            })?,
            argument_pattern: "(Array<T>...)".into(),
            result_pattern: "Relation<T...>".into(),
            aggregate: None,
        }],
    )
    .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
        subject: "builtin table function binding declaration",
        value: error.to_string().into(),
    })?;
    builder.register(FunctionDefinition::try_new_bound(
        "unnest",
        FunctionVisibility::Public,
        unnest_declaration,
        Arc::new(BuiltinUnnestResolver),
    )?)?;
    Ok(())
}

pub fn build_builtin_engine_function_catalog() -> Result<EngineFunctionCatalog, FunctionCatalogError>
{
    let mut builder = EngineFunctionCatalogBuilder::new();
    contribute_builtin_functions(&mut builder)?;
    builder.seal_bound()
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
) -> crate::binding::SqlFunctionBinding {
    let executable_name =
        novarocks_types::aggregate::mangle_distinct_aggregate_name(name, distinct);
    let arguments = argument_types
        .iter()
        .cloned()
        .map(|data_type| FunctionArgument::Value {
            value_type: FunctionValueType::new(data_type, true),
            constant: None,
        })
        .collect::<Vec<_>>();
    let exact = builtin_engine_function_catalog()
        .resolve_bound_trusted(
            &executable_name,
            FunctionKind::Aggregate,
            FunctionBindingRequest {
                arguments: &arguments,
                logical_argument_count: arguments.len(),
            },
        )
        .unwrap_or_else(|error| {
            panic!("test aggregate `{executable_name}` must resolve exactly: {error}")
        });
    exact.into()
}

#[cfg(any(test, feature = "test-support"))]
pub(crate) fn test_function_catalog_snapshot() -> Arc<dyn crate::compiler::SqlFunctionCatalog> {
    builtin_sql_function_catalog().snapshot()
}

#[cfg(test)]
struct TestExactAggregateBindingResolver {
    overloads: Box<[AggregateOverloadMetadata]>,
}

#[cfg(test)]
impl TestExactAggregateBindingResolver {
    fn resolve_exact(
        &self,
        request: FunctionBindingRequest<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        if request.logical_argument_count != request.arguments.len() {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        let argument_types = request
            .arguments
            .iter()
            .map(|argument| match argument {
                FunctionArgument::Value { value_type, .. } => Ok(value_type.data_type.clone()),
                FunctionArgument::Lambda { .. } => Err(FunctionBindingError::NoMatchingOverload),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let overload = self
            .overloads
            .iter()
            .find(|overload| overload.argument_types.as_ref() == argument_types.as_slice())
            .ok_or(FunctionBindingError::NoMatchingOverload)?;
        Ok(FunctionBindingSelection {
            overload: FunctionOverloadId::try_new(overload.identity.as_str())
                .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))?,
            argument_types: request
                .arguments
                .iter()
                .map(FunctionArgument::argument_type)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            result_type: FunctionResultType::Scalar(FunctionValueType::new(
                overload.output_type.clone(),
                true,
            )),
            aggregate: Some(novarocks_functions::AggregateBindingSelection {
                intermediate_type: FunctionValueType::new(overload.intermediate_type.clone(), true),
                state_format: overload.state_format.clone(),
            }),
        })
    }
}

#[cfg(test)]
impl FunctionBindingResolver for TestExactAggregateBindingResolver {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.resolve_exact(request)
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
    ) -> Result<(), FunctionBindingError> {
        if &self.resolve_exact(request)? == selected {
            Ok(())
        } else {
            Err(FunctionBindingError::InvalidBinding(
                "selected test aggregate binding differs from its exact declaration".into(),
            ))
        }
    }
}

/// Build a test-only catalog whose custom aggregate has the same exact,
/// identity-bearing contract required from production contributors.
#[cfg(test)]
pub(crate) fn test_exact_aggregate_catalog(
    name: &str,
    visibility: FunctionVisibility,
    overloads: impl IntoIterator<Item = AggregateOverloadMetadata>,
) -> EngineFunctionCatalog {
    let overloads = overloads.into_iter().collect::<Vec<_>>();
    let declaration = FunctionBindingDeclaration::try_new(
        FunctionId::try_new(format!("test.aggregate/{name}/v1"))
            .expect("test aggregate function identity"),
        FunctionKind::Aggregate,
        FunctionSemantics {
            volatility: FunctionVolatility::Immutable,
            argument_evaluation: FunctionArgumentEvaluation::Eager,
            failure_behavior: FunctionFailureBehavior::Propagate,
            intrinsic_row_error:
                novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated,
        },
        overloads
            .iter()
            .map(|overload| FunctionOverloadDeclaration {
                identity: FunctionOverloadId::try_new(overload.identity.as_str())
                    .expect("test aggregate overload identity"),
                argument_pattern: format!("exact:{}:arguments", overload.identity.as_str()).into(),
                result_pattern: format!("exact:{}:output", overload.identity.as_str()).into(),
                aggregate: Some(AggregateBindingDeclaration {
                    intermediate_pattern: format!(
                        "exact:{}:intermediate",
                        overload.identity.as_str()
                    )
                    .into(),
                    state_format: overload.state_format.clone(),
                }),
            }),
    )
    .expect("test aggregate binding declaration");
    let overloads = overloads.into_boxed_slice();
    let definition = FunctionDefinition::try_new_bound_aggregate(
        name,
        visibility,
        declaration,
        Arc::new(TestExactAggregateBindingResolver {
            overloads: overloads.clone(),
        }),
        novarocks_functions::exact_aggregate_signature_contract(overloads.into_vec()),
    )
    .expect("test aggregate definition");
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(definition)
        .expect("register test aggregate");
    builder.seal_bound().expect("test aggregate catalog")
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

    fn value_argument(
        data_type: DataType,
        nullable: bool,
        constant: Option<novarocks_functions::FunctionLiteral>,
    ) -> FunctionArgument {
        FunctionArgument::Value {
            value_type: FunctionValueType::new(data_type, nullable),
            constant,
        }
    }

    fn resolve_exact_scalar(
        catalog: &EngineFunctionCatalog,
        name: &str,
        arguments: &[FunctionArgument],
    ) -> ResolvedFunctionBinding {
        catalog
            .resolve_bound_user(
                name,
                FunctionKind::Scalar,
                FunctionBindingRequest {
                    arguments,
                    logical_argument_count: arguments.len(),
                },
            )
            .unwrap_or_else(|error| panic!("{name} must bind exactly: {error}"))
    }

    fn scalar_result(binding: &ResolvedFunctionBinding) -> &FunctionValueType {
        let FunctionResultType::Scalar(result) = &binding.selected.result_type else {
            panic!("scalar binding must have a scalar result")
        };
        result
    }

    #[test]
    fn json_output_domain_uses_bound_identity_not_shadowed_function_spelling() {
        let builtin = build_builtin_engine_function_catalog().unwrap();
        let args = [value_argument(DataType::Utf8, false, None)];
        let bound = resolve_exact_scalar(&builtin, "json_object", &args);
        assert_eq!(
            scalar_output_logical_type(&bound),
            Some(novarocks_types::schema::SqlType::Json)
        );

        let signatures = registry::builtin_scalar_declarations()
            .into_iter()
            .find(|(name, _)| name == "json_object")
            .unwrap()
            .1;
        let overloads = (0..signatures.len())
            .map(|index| {
                FunctionOverloadId::try_new(format!("test.shadow.json_object/{index}/v1")).unwrap()
            })
            .collect::<Vec<_>>();
        let declaration = FunctionBindingDeclaration::try_new(
            FunctionId::try_new("test.shadow/json_object/v1").unwrap(),
            FunctionKind::Scalar,
            builtin_scalar_semantics("json_object"),
            overloads
                .iter()
                .cloned()
                .zip(&signatures)
                .map(|(identity, signature)| FunctionOverloadDeclaration {
                    identity,
                    argument_pattern: signature.clone().into_boxed_str(),
                    result_pattern: signature.clone().into_boxed_str(),
                    aggregate: None,
                }),
        )
        .unwrap();
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(
                FunctionDefinition::try_new_bound(
                    "json_object",
                    FunctionVisibility::Public,
                    declaration,
                    Arc::new(BuiltinScalarResolver {
                        canonical_name: "json_object".into(),
                        overloads: overloads.into_boxed_slice(),
                    }),
                )
                .unwrap(),
            )
            .unwrap();
        let shadow = builder.seal().unwrap();
        let bound = resolve_exact_scalar(&shadow, "json_object", &args);
        assert_eq!(scalar_result(&bound).data_type, DataType::Utf8);
        assert_eq!(scalar_output_logical_type(&bound), None);
    }

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
    fn exact_scalar_nullability_is_owned_by_the_binding() {
        let catalog = build_builtin_engine_function_catalog().expect("builtin catalog");
        let nonnull = [value_argument(DataType::Int64, false, None)];
        let nullable = [value_argument(DataType::Int64, true, None)];
        assert!(!scalar_result(&resolve_exact_scalar(&catalog, "abs", &nonnull)).nullable);
        assert!(scalar_result(&resolve_exact_scalar(&catalog, "abs", &nullable)).nullable);

        let coalesce = [
            value_argument(DataType::Int64, true, None),
            value_argument(DataType::Int64, false, None),
        ];
        assert!(!scalar_result(&resolve_exact_scalar(&catalog, "coalesce", &coalesce)).nullable);
        let all_nullable = [
            value_argument(DataType::Int64, true, None),
            value_argument(DataType::Int64, true, None),
        ];
        assert!(scalar_result(&resolve_exact_scalar(&catalog, "coalesce", &all_nullable)).nullable);

        let count = catalog
            .resolve_bound_user(
                "count",
                FunctionKind::Aggregate,
                FunctionBindingRequest {
                    arguments: &nonnull,
                    logical_argument_count: 1,
                },
            )
            .expect("count binding");
        let sum = catalog
            .resolve_bound_user(
                "sum",
                FunctionKind::Aggregate,
                FunctionBindingRequest {
                    arguments: &nonnull,
                    logical_argument_count: 1,
                },
            )
            .expect("sum binding");
        assert!(!aggregate_result_type(&count).nullable);
        assert!(aggregate_result_type(&sum).nullable);
    }

    #[test]
    fn literal_dependent_dynamic_scalars_freeze_exact_result_shapes() {
        let catalog = build_builtin_engine_function_catalog().expect("builtin catalog");
        let named = [
            value_argument(
                DataType::Utf8,
                false,
                Some(novarocks_functions::FunctionLiteral::Utf8("left".into())),
            ),
            value_argument(DataType::Int64, false, None),
            value_argument(
                DataType::Utf8,
                false,
                Some(novarocks_functions::FunctionLiteral::Utf8("right".into())),
            ),
            value_argument(DataType::Boolean, true, None),
        ];
        let named = resolve_exact_scalar(&catalog, "named_struct", &named);
        let DataType::Struct(fields) = &scalar_result(&named).data_type else {
            panic!("named_struct must bind a struct result")
        };
        assert_eq!(fields[0].name(), "left");
        assert_eq!(fields[0].data_type(), &DataType::Int64);
        assert_eq!(fields[1].name(), "right");
        assert_eq!(fields[1].data_type(), &DataType::Boolean);

        let rounded = resolve_exact_scalar(
            &catalog,
            "round",
            &[
                value_argument(DataType::Decimal128(18, 6), false, None),
                value_argument(
                    DataType::Int64,
                    false,
                    Some(novarocks_functions::FunctionLiteral::Int64(2)),
                ),
            ],
        );
        assert_eq!(
            scalar_result(&rounded).data_type,
            DataType::Decimal128(38, 2)
        );

        let variant = resolve_exact_scalar(
            &catalog,
            "variant_get",
            &[
                value_argument(DataType::LargeBinary, false, None),
                value_argument(
                    DataType::Utf8,
                    false,
                    Some(novarocks_functions::FunctionLiteral::Utf8("$.x".into())),
                ),
                value_argument(
                    DataType::Utf8,
                    false,
                    Some(novarocks_functions::FunctionLiteral::Utf8("BIGINT".into())),
                ),
            ],
        );
        assert_eq!(scalar_result(&variant).data_type, DataType::Int64);
    }

    #[test]
    fn exact_table_binding_freezes_unnest_relation_columns() {
        let catalog = build_builtin_engine_function_catalog().expect("builtin catalog");
        let list = DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Int64,
            false,
        )));
        let arguments = [value_argument(list.clone(), false, None)];
        let binding = catalog
            .resolve_bound_user(
                "unnest",
                FunctionKind::Table,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: 1,
                },
            )
            .expect("UNNEST binding");
        assert_eq!(binding.function_id.as_str(), BUILTIN_UNNEST_FUNCTION_ID);
        assert_eq!(binding.kind, FunctionKind::Table);
        assert_eq!(
            binding.semantics.intrinsic_row_error,
            novarocks_type_contract::FunctionIntrinsicRowError::NoRowError
        );
        catalog
            .validate_bound(
                &binding,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: 1,
                },
            )
            .expect("exact installed table binding");
        assert_eq!(
            binding.selected.argument_types.as_ref(),
            &[FunctionArgumentType::Value(FunctionValueType::new(
                list, false
            ))]
        );
        assert_eq!(
            binding.selected.result_type,
            FunctionResultType::Relation(
                vec![FunctionValueType::new(DataType::Int64, true)].into_boxed_slice()
            )
        );
    }

    #[test]
    fn aggregate_resolution_is_catalog_backed_and_exact() {
        let catalog = build_builtin_engine_function_catalog().expect("builtin catalog");
        let resolved = resolve_bound_aggregate(&catalog, "count", &[], &[], false)
            .expect("count star resolves");
        assert_eq!(
            resolved.overload.as_str(),
            "builtin.aggregate/count/derived-v1"
        );
        assert!(resolved.argument_types.is_empty());
        assert_eq!(resolved.intermediate_type, DataType::Int64);
        assert_eq!(resolved.output_type, DataType::Int64);
        assert_eq!(resolved.state_format.as_str(), "novarocks/count/state-v1");
        assert!(matches!(
            resolve_bound_aggregate(
                &catalog,
                "map_agg",
                &[DataType::Int64],
                &[DataType::Int64],
                false,
            ),
            Err(FunctionResolutionError::NoMatchingSignature { .. })
        ));
        assert_eq!(
            resolve_bound_aggregate(&catalog, "not_an_aggregate", &[], &[], false),
            Err(FunctionResolutionError::UnknownFunction)
        );

        let std_user = resolve_bound_aggregate(
            &catalog,
            "std",
            &[DataType::Int64],
            &[DataType::Int64],
            false,
        )
        .expect("std alias resolves for user SQL");
        let std_trusted = resolve_bound_aggregate(
            &catalog,
            "std",
            &[DataType::Int64],
            &[DataType::Int64],
            true,
        )
        .expect("std alias resolves for trusted planning");
        assert_eq!(std_user, std_trusted);
        assert_eq!(
            std_user.overload.as_str(),
            "builtin.aggregate/std/derived-v1"
        );
        assert_eq!(std_user.intermediate_type, DataType::Binary);
        assert_eq!(std_user.output_type, DataType::Float64);
        assert_eq!(std_user.state_format.as_str(), "novarocks/std/state-v1");
    }

    #[test]
    fn ordered_update_resolution_does_not_widen_logical_overloads() {
        let catalog = build_builtin_engine_function_catalog().expect("builtin catalog");
        assert!(matches!(
            resolve_bound_aggregate(
                &catalog,
                "array_agg",
                &[DataType::Utf8, DataType::Int64],
                &[DataType::Utf8, DataType::Int64],
                false,
            ),
            Err(FunctionResolutionError::NoMatchingSignature { .. })
        ));

        let resolved = resolve_bound_aggregate(
            &catalog,
            "array_agg",
            &[DataType::Utf8],
            &[DataType::Utf8, DataType::Int64],
            false,
        )
        .expect("selected array_agg overload accepts one physical ORDER BY channel");
        assert_eq!(
            resolved.overload.as_str(),
            "builtin.aggregate/array_agg/derived-v1"
        );
        assert_eq!(resolved.argument_types, [DataType::Utf8, DataType::Int64]);
        let DataType::Struct(fields) = resolved.intermediate_type else {
            panic!("ordered array_agg must expose a Struct intermediate");
        };
        assert_eq!(fields.len(), 2);

        assert!(matches!(
            resolve_bound_aggregate(
                &catalog,
                "sum",
                &[DataType::Int64],
                &[DataType::Int64, DataType::Utf8],
                false,
            ),
            Err(FunctionResolutionError::NoMatchingSignature { .. })
                | Err(FunctionResolutionError::BadSignature(_))
        ));
    }

    #[test]
    fn variadic_distinct_count_and_dict_merge_keep_their_logical_arities() {
        let catalog = build_builtin_engine_function_catalog().expect("builtin catalog");

        let distinct = resolve_bound_aggregate(
            &catalog,
            "multi_distinct_count",
            &[DataType::Int64, DataType::Utf8, DataType::Boolean],
            &[DataType::Int64, DataType::Utf8, DataType::Boolean],
            false,
        )
        .expect("multi-column distinct count resolves");
        assert_eq!(
            distinct.argument_types,
            [DataType::Int64, DataType::Utf8, DataType::Boolean]
        );
        assert!(matches!(
            resolve_bound_aggregate(&catalog, "multi_distinct_count", &[], &[], false),
            Err(FunctionResolutionError::NoMatchingSignature { .. })
        ));

        let dict = resolve_bound_aggregate(
            &catalog,
            "dict_merge",
            &[DataType::Utf8, DataType::Int64],
            &[DataType::Utf8, DataType::Int64],
            false,
        )
        .expect("dict_merge logical value and threshold arguments resolve");
        assert_eq!(dict.argument_types, [DataType::Utf8, DataType::Int64]);
        assert!(
            catalog
                .resolve_selected_aggregate_update_trusted(
                    "dict_merge",
                    &dict.overload,
                    &[DataType::Boolean, DataType::Float64],
                )
                .is_err(),
            "selected update resolution must preserve dict_merge's logical type contract"
        );
        let list_utf8 = DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Utf8,
            true,
        )));
        for threshold_type in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            resolve_bound_aggregate(
                &catalog,
                "dict_merge",
                &[list_utf8.clone(), threshold_type.clone()],
                &[list_utf8.clone(), threshold_type.clone()],
                false,
            )
            .unwrap_or_else(|error| {
                panic!("dict_merge must accept {list_utf8:?}, {threshold_type:?}: {error}")
            });
        }
        assert!(matches!(
            resolve_bound_aggregate(
                &catalog,
                "dict_merge",
                &[DataType::Utf8],
                &[DataType::Utf8],
                false,
            ),
            Err(FunctionResolutionError::NoMatchingSignature { .. })
        ));
        assert!(matches!(
            resolve_bound_aggregate(
                &catalog,
                "dict_merge",
                &[DataType::Boolean, DataType::Float64],
                &[DataType::Boolean, DataType::Float64],
                false,
            ),
            Err(FunctionResolutionError::NoMatchingSignature { .. })
        ));
        let unsupported_list = DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Int64,
            true,
        )));
        assert!(matches!(
            resolve_bound_aggregate(
                &catalog,
                "dict_merge",
                &[unsupported_list.clone(), DataType::Int64],
                &[unsupported_list, DataType::Int64],
                false,
            ),
            Err(FunctionResolutionError::NoMatchingSignature { .. })
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

    #[test]
    fn abs_exact_bindings_freeze_input_and_promoted_output_widths() {
        let catalog = build_builtin_engine_function_catalog().expect("builtin catalog");
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
            for nullable in [false, true] {
                let arguments = [value_argument(input.clone(), nullable, None)];
                let binding = resolve_exact_scalar(&catalog, "abs", &arguments);
                assert_eq!(scalar_result(&binding).data_type, output);
                assert_eq!(scalar_result(&binding).nullable, nullable);
                assert_eq!(
                    binding.selected.argument_types.as_ref(),
                    &[FunctionArgumentType::Value(FunctionValueType::new(
                        input.clone(),
                        nullable
                    ))]
                );
                catalog
                    .validate_bound(
                        &binding,
                        FunctionBindingRequest {
                            arguments: &arguments,
                            logical_argument_count: 1,
                        },
                    )
                    .expect("the frozen ABS profile must validate without changing the input type");

                let negative = resolver::resolve_scalar_function_signature(
                    "negative",
                    std::slice::from_ref(&input),
                )
                .unwrap();
                assert_eq!(negative.return_type, input);
                assert!(matches!(
                    builtin_disposition("negative"),
                    Some(BuiltinDisposition::LoweredOnly)
                ));
            }
        }
    }

    #[test]
    fn abs_exact_binding_rejects_a_stale_same_width_integer_result() {
        let catalog = build_builtin_engine_function_catalog().expect("builtin catalog");
        for input in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            let arguments = [value_argument(input.clone(), true, None)];
            let mut binding = resolve_exact_scalar(&catalog, "abs", &arguments);
            binding.selected.result_type =
                FunctionResultType::Scalar(FunctionValueType::new(input, true));
            assert!(
                catalog
                    .validate_bound(
                        &binding,
                        FunctionBindingRequest {
                            arguments: &arguments,
                            logical_argument_count: 1,
                        }
                    )
                    .is_err(),
                "a stale same-width result must fail exact catalog validation before encoding"
            );
        }
    }
    #[test]
    fn selected_builtin_row_effects_follow_exact_implementation_contract() {
        use novarocks_type_contract::FunctionIntrinsicRowError as Own;
        let catalog = build_builtin_engine_function_catalog().unwrap();
        for (name, arguments, expected) in [
            (
                "lower",
                vec![value_argument(DataType::Utf8, true, None)],
                Own::NoRowError,
            ),
            (
                "parse_json",
                vec![value_argument(DataType::Utf8, true, None)],
                Own::NoRowError,
            ),
            (
                "assert_true",
                vec![value_argument(DataType::Boolean, true, None)],
                Own::MayRaise,
            ),
            (
                "bar",
                vec![
                    value_argument(DataType::Float64, false, None),
                    value_argument(DataType::Float64, false, None),
                    value_argument(DataType::Float64, false, None),
                    value_argument(DataType::Int64, false, None),
                ],
                Own::MayRaise,
            ),
            (
                "count_state_visible",
                vec![value_argument(DataType::Binary, false, None)],
                Own::MayRaise,
            ),
        ] {
            let bound = resolve_exact_scalar(&catalog, name, &arguments);
            assert_eq!(bound.semantics.intrinsic_row_error, expected, "{name}");
            catalog
                .validate_bound(
                    &bound,
                    FunctionBindingRequest {
                        arguments: &arguments,
                        logical_argument_count: arguments.len(),
                    },
                )
                .unwrap();
        }
    }

    #[test]
    fn variadic_typed_encoders_close_unsupported_shape_before_optimization() {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        for name in ["mv_group_row_id", "encode_sort_key", "encode_row_id"] {
            let arguments = [value_argument(
                DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "item",
                    DataType::Int32,
                    true,
                ))),
                true,
                None,
            )];
            assert!(
                catalog
                    .resolve_bound_user(
                        name,
                        FunctionKind::Scalar,
                        FunctionBindingRequest {
                            arguments: &arguments,
                            logical_argument_count: 1
                        }
                    )
                    .is_err(),
                "{name}"
            );
        }
        let arguments = [
            value_argument(DataType::Int16, true, None),
            value_argument(DataType::Utf8, true, None),
        ];
        for name in ["mv_group_row_id", "encode_sort_key", "encode_row_id"] {
            let bound = resolve_exact_scalar(&catalog, name, &arguments);
            assert_eq!(
                bound.selected.argument_types.as_ref(),
                arguments
                    .iter()
                    .map(FunctionArgument::argument_type)
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                bound.semantics.intrinsic_row_error,
                novarocks_type_contract::FunctionIntrinsicRowError::NoRowError
            );
        }
        // Fingerprint intentionally ignores complex inputs in its installed owner.
        let complex = [value_argument(
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Int32,
                true,
            ))),
            true,
            None,
        )];
        let bound = resolve_exact_scalar(&catalog, "encode_fingerprint_sha256", &complex);
        assert_eq!(
            bound.semantics.intrinsic_row_error,
            novarocks_type_contract::FunctionIntrinsicRowError::NoRowError
        );
    }
    #[test]
    fn field_exact_binding_rejects_containers_and_preserves_comparable_profiles() {
        use arrow::datatypes::{Field, Fields, TimeUnit};
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let item = Arc::new(Field::new("item", DataType::Int64, true));
        let fields: Fields = vec![
            Arc::new(Field::new("key", DataType::Int64, false)),
            Arc::new(Field::new("value", DataType::Utf8, true)),
        ]
        .into();
        for ty in [
            DataType::List(item),
            DataType::Struct(fields.clone()),
            DataType::Map(
                Arc::new(Field::new("entries", DataType::Struct(fields), false)),
                false,
            ),
        ] {
            let arguments = [
                value_argument(ty.clone(), true, None),
                value_argument(ty, true, None),
            ];
            assert!(
                catalog
                    .resolve_bound_user(
                        "field",
                        FunctionKind::Scalar,
                        FunctionBindingRequest {
                            arguments: &arguments,
                            logical_argument_count: 2
                        }
                    )
                    .is_err()
            );
        }
        for ty in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int64,
            DataType::Decimal128(38, 9),
            DataType::Decimal256(60, 9),
            DataType::FixedSizeBinary(16),
            DataType::Utf8,
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ] {
            let arguments = [
                value_argument(ty.clone(), true, None),
                value_argument(ty, true, None),
            ];
            let bound = resolve_exact_scalar(&catalog, "field", &arguments);
            assert_eq!(
                bound.semantics.intrinsic_row_error,
                novarocks_type_contract::FunctionIntrinsicRowError::NoRowError
            );
            catalog
                .validate_bound(
                    &bound,
                    FunctionBindingRequest {
                        arguments: &arguments,
                        logical_argument_count: 2,
                    },
                )
                .unwrap();
        }
    }
    #[test]
    fn array_ordering_binding_closes_comparator_domain_and_preserves_sortby_values() {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let list = |ty| DataType::List(Arc::new(arrow::datatypes::Field::new("item", ty, true)));
        for ty in [
            list(DataType::Int64),
            DataType::Map(
                Arc::new(arrow::datatypes::Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(arrow::datatypes::Field::new("key", DataType::Int64, true)),
                            Arc::new(arrow::datatypes::Field::new("value", DataType::Int64, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            DataType::Struct(
                vec![Arc::new(arrow::datatypes::Field::new(
                    "x",
                    DataType::Int64,
                    true,
                ))]
                .into(),
            ),
            DataType::Decimal256(60, 2),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
        ] {
            for name in ["array_sort", "array_min", "array_max", "array_top_n"] {
                let mut arguments = vec![value_argument(list(ty.clone()), true, None)];
                if name == "array_top_n" {
                    arguments.push(value_argument(DataType::Int64, false, None));
                }
                assert!(
                    catalog
                        .resolve_bound_user(
                            name,
                            FunctionKind::Scalar,
                            FunctionBindingRequest {
                                arguments: &arguments,
                                logical_argument_count: arguments.len()
                            }
                        )
                        .is_err(),
                    "{name}"
                );
            }
            let arguments = [
                value_argument(list(DataType::Utf8), true, None),
                value_argument(list(ty), true, None),
            ];
            assert!(
                catalog
                    .resolve_bound_user(
                        "array_sortby",
                        FunctionKind::Scalar,
                        FunctionBindingRequest {
                            arguments: &arguments,
                            logical_argument_count: 2
                        }
                    )
                    .is_err()
            );
        }
        for ty in [
            DataType::Null,
            DataType::Int8,
            DataType::Date32,
            DataType::Decimal128(38, 2),
            DataType::FixedSizeBinary(16),
        ] {
            let arguments = [value_argument(list(ty), true, None)];
            let bound = resolve_exact_scalar(&catalog, "array_sort", &arguments);
            catalog
                .validate_bound(
                    &bound,
                    FunctionBindingRequest {
                        arguments: &arguments,
                        logical_argument_count: 1,
                    },
                )
                .unwrap();
        }
        // A nested output list needs no comparison: only the independent keys do.
        let arguments = [
            value_argument(list(list(DataType::Int64)), true, None),
            value_argument(list(DataType::Int64), true, None),
        ];
        let bound = resolve_exact_scalar(&catalog, "array_sortby", &arguments);
        catalog
            .validate_bound(
                &bound,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: 2,
                },
            )
            .unwrap();
    }

    #[test]
    fn object_value_binding_rejects_unimplemented_carriers_and_missing_inputs() {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let list = DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Int64,
            true,
        )));
        for name in [
            "hll_hash",
            "percentile_hash",
            "to_bitmap",
            "bitmap_count",
            "bitmap_to_binary",
            "bitmap_from_binary",
            "bitmap_from_string",
            "bitmap_and",
        ] {
            for types in [vec![], vec![list.clone()], vec![list.clone(), list.clone()]] {
                let arguments = types
                    .into_iter()
                    .map(|ty| value_argument(ty, true, None))
                    .collect::<Vec<_>>();
                assert!(
                    catalog
                        .resolve_bound_user(
                            name,
                            FunctionKind::Scalar,
                            FunctionBindingRequest {
                                arguments: &arguments,
                                logical_argument_count: arguments.len()
                            }
                        )
                        .is_err(),
                    "{name}"
                );
            }
        }
        for (name, ty) in [
            ("hll_hash", DataType::Date32),
            ("hll_hash", DataType::FixedSizeBinary(16)),
            ("percentile_hash", DataType::Decimal128(38, 2)),
            ("percentile_hash", DataType::FixedSizeBinary(16)),
            ("to_bitmap", DataType::UInt64),
            ("to_bitmap", DataType::LargeBinary),
            ("bitmap_count", DataType::Null),
            ("bitmap_from_binary", DataType::LargeUtf8),
        ] {
            let arguments = [value_argument(ty, true, None)];
            let bound = resolve_exact_scalar(&catalog, name, &arguments);
            assert_eq!(
                bound.semantics.intrinsic_row_error,
                novarocks_type_contract::FunctionIntrinsicRowError::NoRowError
            );
            catalog
                .validate_bound(
                    &bound,
                    FunctionBindingRequest {
                        arguments: &arguments,
                        logical_argument_count: 1,
                    },
                )
                .unwrap();
        }
        let arguments = [
            value_argument(DataType::Binary, true, None),
            value_argument(DataType::Binary, true, None),
        ];
        for name in ["bitmap_and", "bitmap_has_any"] {
            let bound = resolve_exact_scalar(&catalog, name, &arguments);
            catalog
                .validate_bound(
                    &bound,
                    FunctionBindingRequest {
                        arguments: &arguments,
                        logical_argument_count: 2,
                    },
                )
                .unwrap();
        }
        // These installed constructors consume no row values, including no input.
        for name in ["bitmap_empty", "percentile_empty"] {
            let bound = resolve_exact_scalar(&catalog, name, &[]);
            catalog
                .validate_bound(
                    &bound,
                    FunctionBindingRequest {
                        arguments: &[],
                        logical_argument_count: 0,
                    },
                )
                .unwrap();
        }
    }

    #[test]
    fn dynamic_array_numeric_binding_closes_the_installed_output_domain() {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let list = |ty| DataType::List(Arc::new(arrow::datatypes::Field::new("item", ty, true)));
        for name in ["array_cum_sum", "array_difference"] {
            for ty in [
                DataType::Null,
                DataType::Utf8,
                DataType::Decimal256(60, 2),
                list(DataType::Int64),
            ] {
                let arguments = [value_argument(list(ty), true, None)];
                assert!(
                    catalog
                        .resolve_bound_user(
                            name,
                            FunctionKind::Scalar,
                            FunctionBindingRequest {
                                arguments: &arguments,
                                logical_argument_count: 1
                            }
                        )
                        .is_err(),
                    "{name}"
                );
            }
            for (input, output) in [
                (DataType::Boolean, DataType::Int64),
                (DataType::Int16, DataType::Int64),
                (DataType::Float32, DataType::Float64),
                (DataType::Decimal128(38, 2), DataType::Float64),
            ] {
                let arguments = [value_argument(list(input), true, None)];
                let bound = resolve_exact_scalar(&catalog, name, &arguments);
                assert_eq!(
                    bound.selected.result_type,
                    FunctionResultType::Scalar(FunctionValueType::new(list(output), true))
                );
                catalog
                    .validate_bound(
                        &bound,
                        FunctionBindingRequest {
                            arguments: &arguments,
                            logical_argument_count: 1,
                        },
                    )
                    .unwrap();
            }
        }
    }
    #[test]
    fn selected_collection_shapes_preserve_recursive_equality_and_masks() {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let list = |ty| DataType::List(Arc::new(arrow::datatypes::Field::new("item", ty, true)));
        for name in [
            "array_contains",
            "array_position",
            "array_remove",
            "array_distinct",
        ] {
            let mut arguments = vec![value_argument(
                list(DataType::Decimal256(60, 2)),
                true,
                None,
            )];
            if name != "array_distinct" {
                arguments.push(value_argument(DataType::Decimal256(60, 2), true, None));
            }
            assert!(
                catalog
                    .resolve_bound_user(
                        name,
                        FunctionKind::Scalar,
                        FunctionBindingRequest {
                            arguments: &arguments,
                            logical_argument_count: arguments.len()
                        }
                    )
                    .is_err(),
                "{name}"
            );
        }
        for name in ["all_match", "any_match", "array_filter"] {
            let mut arguments = vec![value_argument(list(DataType::Int64), true, None)];
            if name == "array_filter" {
                arguments.push(value_argument(list(list(DataType::Boolean)), true, None));
            } else {
                arguments[0] = value_argument(list(list(DataType::Boolean)), true, None);
            }
            assert!(
                catalog
                    .resolve_bound_user(
                        name,
                        FunctionKind::Scalar,
                        FunctionBindingRequest {
                            arguments: &arguments,
                            logical_argument_count: arguments.len()
                        }
                    )
                    .is_err(),
                "{name}"
            );
            let mut arguments = vec![value_argument(list(DataType::Int8), true, None)];
            if name == "array_filter" {
                arguments.push(value_argument(list(DataType::Int8), true, None));
            }
            let bound = resolve_exact_scalar(&catalog, name, &arguments);
            catalog
                .validate_bound(
                    &bound,
                    FunctionBindingRequest {
                        arguments: &arguments,
                        logical_argument_count: arguments.len(),
                    },
                )
                .unwrap();
        }
        for name in ["array_contains", "array_position", "array_remove"] {
            let item = list(DataType::FixedSizeBinary(16));
            let arguments = [
                value_argument(list(item.clone()), true, None),
                value_argument(item, true, None),
            ];
            let bound = resolve_exact_scalar(&catalog, name, &arguments);
            catalog
                .validate_bound(
                    &bound,
                    FunctionBindingRequest {
                        arguments: &arguments,
                        logical_argument_count: 2,
                    },
                )
                .unwrap();
        }
        for (name, types) in [
            ("array_flatten", vec![list(DataType::Int64)]),
            ("array_repeat", vec![DataType::Int64]),
            ("arrays_zip", vec![]),
            ("arrays_zip", vec![DataType::Int64]),
            ("map_entries", vec![DataType::Int64]),
        ] {
            let arguments = types
                .into_iter()
                .map(|ty| value_argument(ty, true, None))
                .collect::<Vec<_>>();
            assert!(
                catalog
                    .resolve_bound_user(
                        name,
                        FunctionKind::Scalar,
                        FunctionBindingRequest {
                            arguments: &arguments,
                            logical_argument_count: arguments.len()
                        }
                    )
                    .is_err(),
                "{name}"
            );
        }
        let arguments = [
            value_argument(list(DataType::Date32), true, None),
            value_argument(list(DataType::Utf8), true, None),
        ];
        let bound = resolve_exact_scalar(&catalog, "arrays_overlap", &arguments);
        assert_eq!(
            bound.semantics.intrinsic_row_error,
            novarocks_type_contract::FunctionIntrinsicRowError::MayRaise
        );
    }
    #[test]
    fn selected_array_domain_is_checked_after_argument_widening() {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let list = |ty| DataType::List(Arc::new(arrow::datatypes::Field::new("item", ty, true)));
        for name in ["array_contains", "array_position", "array_remove"] {
            let arguments = [
                value_argument(list(DataType::Int64), true, None),
                value_argument(DataType::Decimal256(60, 2), true, None),
            ];
            assert!(
                catalog
                    .resolve_bound_user(
                        name,
                        FunctionKind::Scalar,
                        FunctionBindingRequest {
                            arguments: &arguments,
                            logical_argument_count: 2
                        }
                    )
                    .is_err(),
                "{name}"
            );
        }
    }
    #[test]
    fn selected_array_ordering_retains_null_only_and_empty_profiles() {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let list = |ty| DataType::List(Arc::new(arrow::datatypes::Field::new("item", ty, true)));
        for name in ["array_sort", "array_min", "array_max", "array_top_n"] {
            let mut arguments = vec![value_argument(list(DataType::Null), true, None)];
            if name == "array_top_n" {
                arguments.push(value_argument(DataType::Int64, false, None));
            }
            let bound = resolve_exact_scalar(&catalog, name, &arguments);
            catalog
                .validate_bound(
                    &bound,
                    FunctionBindingRequest {
                        arguments: &arguments,
                        logical_argument_count: arguments.len(),
                    },
                )
                .unwrap();
            assert_eq!(
                bound.semantics.intrinsic_row_error,
                if name == "array_top_n" {
                    novarocks_type_contract::FunctionIntrinsicRowError::MayRaise
                } else {
                    novarocks_type_contract::FunctionIntrinsicRowError::NoRowError
                }
            );
        }
        let arguments = [
            value_argument(list(list(DataType::Int64)), true, None),
            value_argument(list(DataType::Null), true, None),
        ];
        let bound = resolve_exact_scalar(&catalog, "array_sortby", &arguments);
        catalog
            .validate_bound(
                &bound,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: 2,
                },
            )
            .unwrap();
        assert_eq!(scalar_result(&bound).data_type, list(list(DataType::Int64)));
    }
}
