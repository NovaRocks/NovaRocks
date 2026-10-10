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

//! Builtin declarations and exact binding owned by the function implementation.

use std::sync::{Arc, LazyLock};

use crate::{
    AggregateBindingDeclaration, AggregateOverloadIdentity, AggregateSignatureResolver,
    AggregateStateFormatIdentity, EngineFunctionCatalog, EngineFunctionCatalogBuilder,
    FunctionArgument, FunctionArgumentEvaluation, FunctionArgumentType, FunctionBindingDeclaration,
    FunctionBindingError, FunctionBindingRequest, FunctionBindingResolver,
    FunctionBindingSelection, FunctionCatalogError, FunctionDefinition, FunctionFailureBehavior,
    FunctionId, FunctionKind, FunctionOverloadDeclaration, FunctionOverloadId,
    FunctionResolutionError, FunctionResultType, FunctionSemantics, FunctionValueType,
    FunctionVisibility, ResolvedAggregateSignature, ResolvedFunctionBinding,
};
use arrow_schema::DataType;

use super::binding_control;
use super::intrinsic::{BuiltinDisposition, builtin_disposition};
use super::resolver::ResolveError;
use super::signature::{field_value_type, merge_value_types, value_field};
use super::{registry, resolver};
use crate::FunctionVolatility;
use novarocks_type_contract::{CompileCheckpoints, PureCompileControl, ValueLogicalType};
pub fn resolved_aggregate_signature_from_binding(
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

pub fn resolve_bound_aggregate(
    catalog: &EngineFunctionCatalog,
    name: &str,
    logical_arg_types: &[DataType],
    update_arg_types: &[DataType],
    trusted: bool,
    control: &dyn PureCompileControl,
) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
    let mut work = CompileCheckpoints::try_new(
        control,
        novarocks_type_contract::CompilePhase::FunctionSpecialization,
    )?;
    let result = (|| {
        let mut arguments = Vec::with_capacity(update_arg_types.len());
        for data_type in update_arg_types {
            work.step()?;
            arguments.push(FunctionArgument::Value {
                value_type: FunctionValueType::new(data_type.clone(), true),
                constant: None,
            });
        }
        let request = FunctionBindingRequest {
            expected_result_type: None,
            arguments: &arguments,
            logical_argument_count: logical_arg_types.len(),
        };
        work.flush()?;
        let binding = if trusted {
            catalog.resolve_bound_trusted(name, FunctionKind::Aggregate, request, work.control())
        } else {
            catalog.resolve_bound_user(name, FunctionKind::Aggregate, request, work.control())
        }
        .map_err(|error| match error {
            FunctionBindingError::Control(error) => FunctionResolutionError::Control(error),
            FunctionBindingError::UnknownFunction => FunctionResolutionError::UnknownFunction,
            FunctionBindingError::HiddenFunction => FunctionResolutionError::HiddenFunction,
            FunctionBindingError::NoMatchingOverload => {
                FunctionResolutionError::NoMatchingSignature {
                    candidates: 1,
                    binding_enforced: true,
                }
            }
            other => FunctionResolutionError::BadSignature(other.to_string()),
        })?;
        // Projection preserves the legacy carrier result; its returned channels
        // are visited here, while deep Arrow clones remain library operations.
        for _ in &binding.selected.argument_types {
            work.step()?;
        }
        resolved_aggregate_signature_from_binding(binding)
    })();
    if matches!(result, Err(FunctionResolutionError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
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

pub(super) struct BuiltinAggregateResolver {
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

    fn state_argument_contract(
        &self,
        selected_overload: &AggregateOverloadIdentity,
    ) -> Result<novarocks_type_contract::AggregateStateArgumentContract, FunctionResolutionError>
    {
        if selected_overload != &builtin_overload_identity(self.declaration)? {
            return Err(FunctionResolutionError::BadSignature(
                "builtin aggregate state contract references a foreign overload".into(),
            ));
        }
        Ok(builtin_aggregate_state_argument_contract(
            self.declaration.name,
        ))
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
        let (output_type, intermediate_type) = crate::aggregate_types::infer_agg_function_types(
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
            state_format: AggregateStateFormatIdentity::try_new(builtin_aggregate_state_format(
                declaration.name,
            ))
            .map_err(|error| FunctionResolutionError::BadSignature(error.to_string()))?,
        })
    }
}

// The original implementation declaration authors state interpretation facts;
// downstream planners/codecs never infer them from names or carrier types.
fn builtin_aggregate_state_argument_contract(
    name: &str,
) -> novarocks_type_contract::AggregateStateArgumentContract {
    use novarocks_type_contract::AggregateStateArgumentContract;
    match name {
        "count" | "min" | "max" => AggregateStateArgumentContract::ValueRootNullabilityIndependent,
        _ => AggregateStateArgumentContract::ExactSignature,
    }
}

impl BuiltinAggregateResolver {
    fn bind_value_selection(
        &self,
        request: FunctionBindingRequest<'_>,
        selected: Option<&FunctionOverloadId>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        request_types_with_constants(request, work)?;
        let values = binding_control::scalar_types(request, work)?;
        let mut argument_types = Vec::with_capacity(values.len());
        for value in &values {
            work.step()?;
            argument_types.push(value.data_type.clone());
        }
        let logical_types = &argument_types[..request.logical_argument_count];
        let declaration = self.declaration;
        if !(declaration.min_args..=declaration.max_args).contains(&logical_types.len())
            || !builtin_aggregate_logical_arguments_match(declaration.name, logical_types)
        {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        let name = declaration.name;
        // Numeric readers never reinterpret another semantic value domain as
        // their carrier. LARGEINT is explicitly admitted only by its owner.
        if matches!(
            name,
            "sum"
                | "multi_distinct_sum"
                | "avg"
                | "multi_distinct_avg"
                | "corr"
                | "covar_pop"
                | "covar_samp"
                | "var_pop"
                | "var_samp"
                | "variance"
                | "variance_pop"
                | "variance_samp"
                | "stddev"
                | "stddev_pop"
                | "stddev_samp"
                | "std"
        ) {
            for value in &values[..request.logical_argument_count] {
                work.step()?;
                let largeint = matches!(name, "sum" | "multi_distinct_sum")
                    && value.logical_type == ValueLogicalType::LargeInt;
                if !largeint
                    && (value.logical_type != ValueLogicalType::Physical
                        || matches!(value.data_type, DataType::FixedSizeBinary(_)))
                {
                    return Err(FunctionBindingError::NoMatchingOverload);
                }
            }
        }
        work.flush()?;
        // Legacy carrier derivation remains an opaque owner operation.
        let resolved = if let Some(selected) = selected {
            let overload = AggregateOverloadIdentity::try_new(selected.as_str())
                .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))?;
            self.resolve_update_signature(&overload, &argument_types)
                .map_err(binding_resolution_error)?
        } else {
            let logical = self
                .resolve_aggregate(logical_types)
                .map_err(binding_resolution_error)?;
            if request.logical_argument_count == argument_types.len() {
                logical
            } else {
                self.resolve_update_signature(&logical.overload, &argument_types)
                    .map_err(binding_resolution_error)?
            }
        };
        work.step()?;
        let mut output = FunctionValueType::new(
            resolved.output_type,
            builtin_aggregate_output_nullable(name),
        );
        let mut intermediate = FunctionValueType::new(
            resolved.intermediate_type,
            builtin_aggregate_intermediate_nullable(name),
        );
        if let Some(first) = values.first() {
            match name {
                "min" | "max" | "any_value" | "array_unique_agg" | "sum_map" => {
                    output = first.clone();
                    intermediate = first.clone();
                }
                "max_by" | "min_by" | "percentile_cont" | "percentile_disc"
                | "percentile_disc_lc" => output = first.clone(),
                // An exact LARGEINT SUM keeps its result type; its state is
                // the wider physical intermediate.
                "sum" | "multi_distinct_sum"
                    if first.logical_type == ValueLogicalType::LargeInt =>
                {
                    output = first.clone();
                }
                "array_agg" | "array_agg_distinct" => {
                    output.data_type = DataType::List(Arc::new(value_field("item", first, true)));
                    // The implementation's state Fields/ORDER BY channels
                    // remain the separately derived transport representation.
                }
                "min_n" | "max_n" => {
                    output.data_type = DataType::List(Arc::new(value_field("item", first, true)))
                }
                _ => {}
            }
        }
        if name == "map_agg" && values.len() >= 2 {
            output.data_type = full_map_type(&values[0], &values[1]);
            intermediate.data_type = output.data_type.clone();
        }
        output.nullable = builtin_aggregate_output_nullable(name);
        intermediate.nullable = builtin_aggregate_intermediate_nullable(name);
        binding_control::value_type(&output, work)?;
        binding_control::value_type(&intermediate, work)?;
        Ok(FunctionBindingSelection {
            overload: FunctionOverloadId::try_new(resolved.overload.as_str())
                .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))?,
            argument_types: binding_control::argument_types(request, work)?,
            result_type: FunctionResultType::Scalar(output),
            aggregate: Some(crate::AggregateBindingSelection {
                state_argument_contract: builtin_aggregate_state_argument_contract(
                    declaration.name,
                ),
                intermediate_type: intermediate,
                state_format: resolved.state_format,
            }),
        })
    }
}

impl FunctionBindingResolver for BuiltinAggregateResolver {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| {
            self.bind_value_selection(request, None, work)
        })
    }
    fn select_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| {
            self.bind_value_selection(request, Some(overload), work)
        })
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        binding_control::scope(control, |work| {
            let expected = self.bind_value_selection(request, Some(&selected.overload), work)?;
            if binding_control::same_selection(&expected, selected, work)? {
                Ok(())
            } else {
                Err(FunctionBindingError::InvalidBinding(
                    "selected aggregate overload differs from exact registry resolution".into(),
                ))
            }
        })
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
/// admit. This legacy nullability allowance never erases root or nested
/// logical identities; provider-only decoration is still ignored.
fn same_argument_up_to_nested_nullability(
    types: (&crate::FunctionArgumentType, &crate::FunctionArgumentType),
) -> bool {
    use crate::FunctionArgumentType;
    match types {
        (FunctionArgumentType::Value(left), FunctionArgumentType::Value(right)) => {
            left.nullable == right.nullable
                && left.logical_type == right.logical_type
                && novarocks_type_contract::preserves_nested_logical_identity(
                    &left.data_type,
                    &right.data_type,
                )
                && novarocks_type_contract::arrow_type_equals_ignoring_metadata(
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

/// How one builtin aggregate's state format is spelled. SUM's exact state is
/// its second format: the first carried the result type as its intermediate.
pub(crate) fn builtin_aggregate_state_format(name: &str) -> String {
    match name {
        "sum" => "novarocks/sum/state-v2".to_owned(),
        name => format!("novarocks/{name}/state-v1"),
    }
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
        _ => novarocks_type_contract::is_largeint_data_type(ty),
    }
}

fn validate_builtin_selected_domain(
    name: &str,
    arguments: &[DataType],
    work: &mut CompileCheckpoints<'_>,
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
        ) || novarocks_type_contract::is_largeint_data_type(ty)
    };
    let consumed = match name {
        "bitmap_and" | "bitmap_has_any" => {
            arguments.len() >= 2 && observed_all(arguments.iter().take(2), binary_or_null, work)?
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
        ) || novarocks_type_contract::is_largeint_data_type(ty)
    };
    let ordered_list =
        |ty: &DataType| matches!(ty, DataType::List(item) if ordered_item(item.data_type()));
    // The shared comparator supports one closed primitive item domain. SORTBY
    // compares only key lists; arbitrary output values remain admitted.
    let bool_item = |ty: &DataType| arrow_cast::can_cast_types(ty, &DataType::Boolean);
    let shape = match name {
        "arrays_zip" => !arguments.is_empty() && observed_all(arguments.iter(),|ty| matches!(ty,DataType::List(_) | DataType::Null),work)?,
        "map_entries" => arguments.len()==1 && matches!(&arguments[0],DataType::Map(_, _)),
        "array_contains" | "array_position" | "array_remove" | "array_distinct" =>
            arguments.first().is_some_and(|ty| matches!(ty,DataType::List(item) if builtin_array_equality_item(item.data_type()))),
        "all_match" | "any_match" => arguments.first().is_some_and(|ty| matches!(ty,DataType::List(item)
            if bool_item(item.data_type()) || novarocks_type_contract::is_largeint_data_type(item.data_type()))),
        "array_filter" => arguments.get(1).is_some_and(|ty| matches!(ty,DataType::List(item) if bool_item(item.data_type()))),
        "array_flatten" => arguments.len()==1 && matches!(&arguments[0],DataType::List(outer) if matches!(outer.data_type(),DataType::List(_))),
        "array_repeat" => arguments.len()==2 && arrow_cast::can_cast_types(&arguments[1],&DataType::Int64),
        "distinct_map_keys" => arguments.first().is_some_and(|ty| matches!(ty,DataType::Map(entries,_) if
            matches!(entries.data_type(),DataType::Struct(fields) if fields.len()==2 &&
                ((ordered_item(fields[0].data_type()) && *fields[0].data_type()!=DataType::Null)
                    || matches!(fields[0].data_type(),DataType::Decimal256(..)))))),
        "array_contains_all" | "array_contains_seq" => match arguments {
            [DataType::List(left), DataType::List(right)] => {
                if arrow_cast::can_cast_types(right.data_type(),left.data_type()) {
                    builtin_array_equality_item(left.data_type())
                } else {
                    builtin_array_equality_item(right.data_type())
                        && arrow_cast::can_cast_types(left.data_type(),right.data_type())
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
        "array_sortby" => observed_all(arguments.iter().skip(1), ordered_list, work)?,
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
            ) || novarocks_type_contract::is_largeint_data_type(ty)
        }
        "mv_group_row_id" => matches!(
            ty,
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Date32
                | DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
                | DataType::Utf8
                | DataType::Decimal128(..)
        ),
        // ENCODE_ROW_ID is the installed fingerprint alias. Its owner ignores
        // unencoded carriers; ENCODE_SORT_KEY instead rejects them.
        "encode_fingerprint_sha256" | "encode_row_id" => true,
        "encode_sort_key" => {
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
    if observed_all(arguments.iter(), admitted, work)? {
        Ok(())
    } else {
        Err(FunctionBindingError::NoMatchingOverload)
    }
}

pub(super) struct BuiltinScalarResolver {
    function_id: FunctionId,
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

fn scalar_result_nullable(
    name: &str,
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, FunctionBindingError> {
    let value_nullable = |index: usize| match request.arguments.get(index) {
        Some(FunctionArgument::Value { value_type, .. }) => value_type.nullable,
        Some(FunctionArgument::Lambda { result_type, .. }) => result_type.nullable,
        None => false,
    };
    let first_value = match request.arguments.first() {
        Some(FunctionArgument::Value { value_type, .. }) => Some(value_type),
        _ => None,
    };
    scalar_result_nullable_from_types(
        name,
        request.logical_argument_count,
        first_value,
        value_nullable,
        work,
    )
}

/// The original nullability author over already-authored type facts. Neither
/// adapter constructs constant facts or evaluates data to supply these loans.
fn scalar_result_nullable_from_types(
    name: &str,
    logical_argument_count: usize,
    first_value: Option<&FunctionValueType>,
    value_nullable: impl Fn(usize) -> bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, FunctionBindingError> {
    Ok(match name {
        // POSITIVE uses the math output conversion, which turns non-finite
        // Float32/Float64 values into SQL NULL. Only admitted signed integers
        // and decimals prove a finite Float64 result for every non-NULL value.
        "positive" => match first_value {
            Some(value_type)
                if value_type.logical_type == ValueLogicalType::Physical
                    && matches!(
                        value_type.data_type,
                        DataType::Int8
                            | DataType::Int16
                            | DataType::Int32
                            | DataType::Int64
                            | DataType::Decimal128(_, _)
                    ) =>
            {
                value_type.nullable
            }
            _ => true,
        },
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
        "coalesce" | "ifnull" | "nvl" => {
            nullable_arguments(0..logical_argument_count, true, &value_nullable, work)?
        }
        "if" => nullable_arguments(1..logical_argument_count, false, &value_nullable, work)?,
        "case" => nullable_arguments(
            (1..logical_argument_count).step_by(2),
            false,
            &value_nullable,
            work,
        )?,
        // A function that is total -- one that answers for every value of
        // its declared argument types -- passes its arguments' nullability
        // through. Everything else is nullable.
        name if TOTAL_SCALAR_FUNCTIONS.contains(&name) => {
            nullable_arguments(0..logical_argument_count, false, &value_nullable, work)?
        }
        _ => true,
    })
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
        ResolveError::Control(error) => FunctionBindingError::Control(error),
        ResolveError::BadSignature(message) => FunctionBindingError::InvalidBinding(message.into()),
    }
}

fn builtin_fixed_result_domain(function_id: &FunctionId) -> Option<ValueLogicalType> {
    match function_id.as_str() {
        "builtin.scalar/parse_json/v1"
        | "builtin.scalar/json_object/v1"
        | "builtin.scalar/json_array/v1"
        | "builtin.scalar/to_json/v1"
        | "builtin.scalar/json_query/v1" => Some(ValueLogicalType::Json),
        _ => None,
    }
}

impl BuiltinScalarResolver {
    /// Validate the complete already-selected type profile through the same
    /// original fixed-overload signature and nullability authors. This does
    /// not resolve a new function binding, build a request, inspect constants,
    /// prepare a kernel, or certify emitted source-role facts.
    pub(super) fn check_instantiated_type_profile_observed(
        &self,
        selected: &FunctionBindingSelection,
        logical_argument_count: usize,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        binding_control::scope(control, |work| {
            work.step()?;
            if selected.aggregate.is_some()
                || selected.argument_types.len() != logical_argument_count
            {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            let index = self.overload_index_observed(&selected.overload, work)?;
            let mut types = Vec::with_capacity(logical_argument_count);
            for argument in &selected.argument_types {
                work.step()?;
                let FunctionArgumentType::Value(value_type) = argument else {
                    return Err(FunctionBindingError::NoMatchingOverload);
                };
                types.push(value_type.clone());
            }
            work.flush()?;
            let resolved = resolver::resolve_scalar_value_signature_at_overload(
                &self.canonical_name,
                index,
                &types,
                work.control(),
            )
            .map_err(binding_resolution_error)?;
            if resolved.argument_types.len() != types.len() {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            for (actual, required) in types.iter().zip(&resolved.argument_types) {
                if !binding_control::exact_type(actual, required, work)? {
                    return Err(FunctionBindingError::NoMatchingOverload);
                }
            }
            let value_nullable = |index: usize| types.get(index).is_some_and(|ty| ty.nullable);
            let nullable = scalar_result_nullable_from_types(
                &self.canonical_name,
                logical_argument_count,
                types.first(),
                value_nullable,
                work,
            )?;
            let mut expected = resolved.return_type;
            expected.nullable = nullable;
            let FunctionResultType::Scalar(actual) = &selected.result_type else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            if !binding_control::exact_type(actual, &expected, work)? {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            Ok(())
        })
    }

    fn overload_index_observed(
        &self,
        overload: &FunctionOverloadId,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<usize, FunctionBindingError> {
        let mut index = None;
        for (ordinal, candidate) in self.overloads.iter().enumerate() {
            work.step()?;
            if candidate == overload {
                index = Some(ordinal);
                break;
            }
        }
        index.ok_or_else(|| {
            FunctionBindingError::InvalidBinding(
                "selected scalar overload is not declared by this function".into(),
            )
        })
    }

    /// Instantiate exactly one declared overload through the original scalar
    /// signature author. Validation and late selection share this same body.
    fn selection_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        request_types_with_constants(request, work)?;
        let index = self.overload_index_observed(overload, work)?;
        let argument_types = binding_control::scalar_types(request, work)?;
        work.flush()?;
        let resolved = resolver::resolve_scalar_value_signature_at_overload(
            &self.canonical_name,
            index,
            &argument_types,
            work.control(),
        )
        .map_err(binding_resolution_error)?;
        self.selection(index, resolved, request, work)
    }

    fn selection(
        &self,
        index: usize,
        mut resolved: resolver::ResolvedScalarValueSignature,
        request: FunctionBindingRequest<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        let mut carriers = Vec::with_capacity(resolved.argument_types.len());
        for value in &resolved.argument_types {
            work.step()?;
            carriers.push(value.data_type.clone());
        }
        validate_builtin_selected_domain(&self.canonical_name, &carriers, work)?;
        if matches!(
            self.function_id.as_str(),
            "builtin.scalar/get_json_bool/v1"
                | "builtin.scalar/get_variant_bool/v1"
                | "builtin.scalar/get_json_int/v1"
                | "builtin.scalar/get_variant_int/v1"
                | "builtin.scalar/get_json_double/v1"
                | "builtin.scalar/get_variant_double/v1"
                | "builtin.scalar/get_json_string/v1"
                | "builtin.scalar/get_variant_string/v1"
                | "builtin.scalar/get_json_object/v1"
                | "builtin.scalar/json_query/v1"
                | "builtin.scalar/json_extract/v1"
                | "builtin.scalar/json_exists/v1"
                | "builtin.scalar/json_length/v1"
                | "builtin.scalar/json_keys/v1"
                | "builtin.scalar/variant_typeof/v1"
        ) {
            let Some(document) = resolved.argument_types.first() else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            let admitted = document.data_type == DataType::Null
                || match document.logical_type {
                    ValueLogicalType::Physical => {
                        matches!(document.data_type, DataType::Utf8 | DataType::LargeUtf8)
                    }
                    ValueLogicalType::Json | ValueLogicalType::Variant => true,
                    _ => false,
                };
            if !admitted {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
        }

        // These result identities belong to these exact registered owners.
        // No display name or Utf8 carrier establishes a JSON value domain.
        if let Some(logical_type) = builtin_fixed_result_domain(&self.function_id) {
            resolved.return_type = FunctionValueType::try_with_logical_type(
                resolved.return_type.data_type,
                true,
                logical_type,
            )
            .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))?;
        }
        if self.function_id.as_str() == "builtin.scalar/array_sortby/v1" {
            let Some(FunctionArgument::Value { value_type, .. }) = request.arguments.first() else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            resolved.argument_types[0] = value_type.clone();
            resolved.return_type = value_type.clone();
        }
        if matches!(
            self.function_id.as_str(),
            "builtin.scalar/cardinality/v1"
                | "builtin.scalar/map_size/v1"
                | "builtin.scalar/map_keys/v1"
                | "builtin.scalar/map_values/v1"
        ) {
            let ([FunctionArgument::Value { value_type, .. }], [target]) =
                (request.arguments, resolved.argument_types.as_mut_slice())
            else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            // Each sole List/Map signature has independent unconstrained child
            // variables. Counting offsets needs no conversion of those values.
            // Preserve the actual fields and sorted-map fact instead of adding
            // a nested cast merely to regenerate TypeSpec's field template.
            let same_container = value_type.logical_type == ValueLogicalType::Physical
                && target.logical_type == ValueLogicalType::Physical
                && matches!(
                    (&value_type.data_type, &target.data_type),
                    (DataType::List(_), DataType::List(_))
                        | (DataType::Map(_, _), DataType::Map(_, _))
                );
            work.step()?;
            if same_container {
                // These two root DataType variants clone one FieldRef Arc;
                // they do not clone the nested field/metadata tree.
                *target = value_type.clone();
                work.step()?;
            }
        }
        resolved.return_type.nullable =
            scalar_result_nullable(&self.canonical_name, request, work)?;
        binding_control::value_type(&resolved.return_type, work)?;
        let mut argument_types = Vec::with_capacity(resolved.argument_types.len());
        for value in resolved.argument_types {
            work.step()?;
            argument_types.push(FunctionArgumentType::Value(value));
        }
        Ok(FunctionBindingSelection {
            overload: self.overloads[index].clone(),
            argument_types: argument_types.into_boxed_slice(),
            result_type: FunctionResultType::Scalar(resolved.return_type),
            aggregate: None,
        })
    }
}

impl FunctionBindingResolver for BuiltinScalarResolver {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| {
            request_types_with_constants(request, work)?;
            let argument_types = binding_control::scalar_types(request, work)?;
            work.flush()?;
            let (index, resolved) = resolver::resolve_scalar_value_signature_with_overload(
                &self.canonical_name,
                &argument_types,
                work.control(),
            )
            .map_err(binding_resolution_error)?;
            self.selection(index, resolved, request, work)
        })
    }

    fn select_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| {
            self.selection_at_overload_observed(overload, request, work)
        })
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        binding_control::scope(control, |work| {
            let expected =
                self.selection_at_overload_observed(&selected.overload, request, work)?;
            if binding_control::same_selection(&expected, selected, work)? {
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
                && nested_arguments_match(&expected.argument_types, &selected.argument_types, work)?
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
                format!(
                    "selected scalar overload differs from exact registry resolution: {differing}"
                )
                .into(),
            ))
        })
    }
}

pub(super) const BUILTIN_UNNEST_FUNCTION_ID: &str = "builtin.table/unnest/v1";
pub(super) const BUILTIN_UNNEST_OVERLOAD_ID: &str = "builtin.table/unnest/array-variadic-v1";

pub(super) struct BuiltinUnnestResolver;

fn bind_builtin_unnest(
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FunctionBindingSelection, FunctionBindingError> {
    if request.logical_argument_count != request.arguments.len() || request.arguments.is_empty() {
        return Err(FunctionBindingError::NoMatchingOverload);
    }
    let mut argument_types = Vec::with_capacity(request.arguments.len());
    let mut result_columns = Vec::with_capacity(request.arguments.len());
    for argument in request.arguments {
        work.step()?;
        let FunctionArgument::Value { value_type, .. } = argument else {
            return Err(FunctionBindingError::NoMatchingOverload);
        };
        let DataType::List(item) = &value_type.data_type else {
            return Err(FunctionBindingError::NoMatchingOverload);
        };
        argument_types.push(FunctionArgumentType::Value(value_type.clone()));
        // The current UNNEST operator exposes nullable output slots so a
        // lateral left join can null-extend them without changing its binding.
        if value_type.logical_type != ValueLogicalType::Physical {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        let mut result = field_value_type(item).ok_or(FunctionBindingError::NoMatchingOverload)?;
        result.nullable = true;
        result_columns.push(result);
    }
    Ok(FunctionBindingSelection {
        overload: FunctionOverloadId::try_new(BUILTIN_UNNEST_OVERLOAD_ID)
            .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))?,
        argument_types: argument_types.into_boxed_slice(),
        result_type: FunctionResultType::Relation(result_columns.into_boxed_slice()),
        aggregate: None,
    })
}

impl BuiltinUnnestResolver {
    fn selection_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        request_types_with_constants(request, work)?;
        if overload.as_str() != BUILTIN_UNNEST_OVERLOAD_ID {
            return Err(FunctionBindingError::UnknownOverload(overload.clone()));
        }
        bind_builtin_unnest(request, work)
    }
}

impl FunctionBindingResolver for BuiltinUnnestResolver {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| {
            request_types_with_constants(request, work)?;
            bind_builtin_unnest(request, work)
        })
    }

    fn select_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| {
            self.selection_at_overload_observed(overload, request, work)
        })
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        binding_control::scope(control, |work| {
            let expected =
                self.selection_at_overload_observed(&selected.overload, request, work)?;
            if binding_control::same_selection(selected, &expected, work)? {
                Ok(())
            } else {
                Err(FunctionBindingError::InvalidBinding(
                    "selected UNNEST binding differs from its declared overload".into(),
                ))
            }
        })
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

/// Names whose result shape is bound from precise arguments or constants.
pub fn dynamic_scalar_names() -> &'static [&'static str] {
    DYNAMIC_SCALAR_FUNCTIONS
}

pub(super) struct BuiltinDynamicScalarResolver {
    function_id: FunctionId,
    canonical_name: Box<str>,
    overload: FunctionOverloadId,
}

fn dynamic_argument_data_types(
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<DataType>, FunctionBindingError> {
    let mut types = Vec::with_capacity(request.arguments.len());
    for argument in request.arguments {
        work.step()?;
        types.push(match argument {
            FunctionArgument::Value { value_type, .. } => value_type.data_type.clone(),
            FunctionArgument::Lambda { result_type, .. } => result_type.data_type.clone(),
        });
    }
    Ok(types)
}

/// Direct family entrypoints retain the same exact source gate as the shared
/// catalogue, including constants whose payload is not read by this family.
pub(super) fn request_types_with_constants(
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FunctionBindingError> {
    binding_control::request_types(request, work)?;
    for argument in request.arguments {
        constant_source(Some(argument), work)?;
        work.step()?;
    }
    Ok(())
}

pub(super) fn constant_source<'a>(
    argument: Option<&'a FunctionArgument>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<&'a crate::ConstantValue>, FunctionBindingError> {
    match argument {
        Some(FunctionArgument::Value {
            value_type,
            constant: Some(value),
        }) => {
            if !binding_control::exact_type(value_type, value.value_type(), work)? {
                return Err(FunctionBindingError::InvalidBinding(
                    "constant source type differs from its argument type".into(),
                ));
            }
            Ok(Some(value))
        }
        _ => Ok(None),
    }
}

fn utf8_constant<'a>(
    argument: Option<&'a FunctionArgument>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<&'a str>, FunctionBindingError> {
    let Some(value) = constant_source(argument, work)? else {
        return Ok(None);
    };
    work.flush()?;
    let text = value.utf8_observed(
        novarocks_type_contract::CompilePhase::FunctionSpecialization,
        work.control(),
    )?;
    Ok(text)
}

fn struct_field_type(
    data_type: &DataType,
    field_name: &str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<DataType>, FunctionBindingError> {
    let DataType::Struct(fields) = data_type else {
        return Ok(None);
    };
    for field in fields {
        work.step()?;
        if field.name().eq_ignore_ascii_case(field_name) {
            return Ok(Some(field.data_type().clone()));
        }
    }
    Ok(None)
}

fn list_type(item_type: DataType) -> DataType {
    DataType::List(Arc::new(arrow_schema::Field::new("item", item_type, true)))
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
        Arc::new(arrow_schema::Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(arrow_schema::Field::new("key", key_type, true)),
                    Arc::new(arrow_schema::Field::new("value", value_type, true)),
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
pub fn dynamic_scalar_data_type(name: &str, argument_types: &[DataType]) -> Option<DataType> {
    let widen_all = |types: &[DataType]| {
        types
            .iter()
            .cloned()
            .reduce(|left, right| novarocks_type_contract::wider_type(&left, &right))
            .unwrap_or(DataType::Null)
    };
    Some(match name {
        "__array_literal" => list_type(
            argument_types
                .iter()
                .cloned()
                .reduce(|left, right| novarocks_type_contract::wider_type(&left, &right))
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
                .reduce(|left, right| novarocks_type_contract::wider_type(&left, &right))
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
                if width == novarocks_type_contract::LARGEINT_BYTE_WIDTH =>
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
                    Arc::new(arrow_schema::Field::new(
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
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
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
                key = novarocks_type_contract::wider_type(&key, &next_key);
                value = novarocks_type_contract::wider_type(&value, &next_value);
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
            DataType::FixedSizeBinary(novarocks_type_contract::LARGEINT_BYTE_WIDTH)
        }
        "named_struct" => DataType::Struct(
            argument_types
                .iter()
                .skip(1)
                .step_by(2)
                .enumerate()
                .map(|(index, data_type)| {
                    Arc::new(arrow_schema::Field::new(
                        format!("col{}", index + 1),
                        data_type.clone(),
                        true,
                    ))
                })
                .collect::<Vec<_>>()
                .into(),
        ),
        "null_or_empty" => DataType::Boolean,
        "row" | "struct" => DataType::Struct(
            argument_types
                .iter()
                .enumerate()
                .map(|(index, data_type)| {
                    Arc::new(arrow_schema::Field::new(
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

fn full_map_type(key: &FunctionValueType, value: &FunctionValueType) -> DataType {
    DataType::Map(
        Arc::new(arrow_schema::Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(value_field("key", key, true)),
                    Arc::new(value_field("value", value, true)),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

fn fold_value_types<'a>(
    values: impl Iterator<Item = &'a FunctionValueType>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FunctionValueType, FunctionBindingError> {
    let mut result = FunctionValueType::new(DataType::Null, true);
    for value in values {
        work.step()?;
        result = merge_value_types(&result, value, true)
            .ok_or(FunctionBindingError::NoMatchingOverload)?;
    }
    Ok(result)
}

fn full_list_item(ty: &FunctionValueType) -> Result<FunctionValueType, FunctionBindingError> {
    if ty.logical_type != ValueLogicalType::Physical {
        return Err(FunctionBindingError::NoMatchingOverload);
    }
    match &ty.data_type {
        DataType::List(item) => {
            field_value_type(item).ok_or(FunctionBindingError::NoMatchingOverload)
        }
        DataType::Null => Ok(FunctionValueType::new(DataType::Null, true)),
        _ => Err(FunctionBindingError::NoMatchingOverload),
    }
}

fn full_map_items(
    ty: &FunctionValueType,
) -> Result<(FunctionValueType, FunctionValueType), FunctionBindingError> {
    if ty.logical_type != ValueLogicalType::Physical {
        return Err(FunctionBindingError::NoMatchingOverload);
    }
    let DataType::Map(entries, _) = &ty.data_type else {
        return Err(FunctionBindingError::NoMatchingOverload);
    };
    let DataType::Struct(fields) = entries.data_type() else {
        return Err(FunctionBindingError::NoMatchingOverload);
    };
    if fields.len() != 2 {
        return Err(FunctionBindingError::NoMatchingOverload);
    }
    Ok((
        field_value_type(&fields[0]).ok_or(FunctionBindingError::NoMatchingOverload)?,
        field_value_type(&fields[1]).ok_or(FunctionBindingError::NoMatchingOverload)?,
    ))
}

fn bind_dynamic_scalar_result(
    function_id: &FunctionId,
    name: &str,
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FunctionValueType, FunctionBindingError> {
    if matches!(name, "round" | "truncate") {
        return super::rounding_binding::bind_result(name, request, work);
    }
    if function_id.as_str() == "builtin.scalar/__array_literal/v1"
        && request.arguments.is_empty()
        && let Some(expected) = request.expected_result_type
    {
        if expected.logical_type != ValueLogicalType::Physical
            || expected.nullable
            || !matches!(expected.data_type, DataType::List(_))
        {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        binding_control::value_type(expected, work)?;
        return Ok(expected.clone());
    }
    let argument_types = dynamic_argument_data_types(request, work)?;
    validate_builtin_selected_domain(name, &argument_types, work)?;
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
                || !observed_all(
                    request.arguments[1..].iter(),
                    |argument| {
                        matches!(
                            argument,
                            FunctionArgument::Value {
                                value_type: FunctionValueType {
                                    logical_type:
                                        novarocks_type_contract::ValueLogicalType::Physical,
                                    data_type: DataType::List(_) | DataType::Null,
                                    ..
                                },
                                ..
                            }
                        )
                    },
                    work,
                )?
            {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            DataType::List(Arc::new(arrow_schema::Field::new(
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
            let mut item_type = None;
            for argument in &argument_types {
                work.step()?;
                item_type = Some(match item_type {
                    None => argument.clone(),
                    Some(left) => novarocks_type_contract::wider_type(&left, argument),
                });
            }
            let item_type = item_type.unwrap_or(DataType::Null);
            DataType::List(Arc::new(arrow_schema::Field::new("item", item_type, true)))
        }
        "__struct_subfield" => {
            let Some(field_name) = utf8_constant(request.arguments.get(1), work)? else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            struct_field_type(
                argument_types.first().unwrap_or(&DataType::Null),
                field_name,
                work,
            )?
            .ok_or(FunctionBindingError::NoMatchingOverload)?
        }
        "__array_struct_subfield" => {
            let Some(field_name) = utf8_constant(request.arguments.get(1), work)? else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            let Some(DataType::List(item)) = argument_types.first() else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            let field_type = struct_field_type(item.data_type(), field_name, work)?
                .ok_or(FunctionBindingError::NoMatchingOverload)?;
            DataType::List(Arc::new(arrow_schema::Field::new("item", field_type, true)))
        }
        "named_struct" => {
            if request.arguments.is_empty() || !request.arguments.len().is_multiple_of(2) {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            let mut fields = Vec::with_capacity(request.arguments.len() / 2);
            for pair in request.arguments.chunks_exact(2) {
                work.step()?;
                let Some(field_name) = utf8_constant(pair.first(), work)? else {
                    return Err(FunctionBindingError::NoMatchingOverload);
                };
                let FunctionArgument::Value { value_type, .. } = &pair[1] else {
                    return Err(FunctionBindingError::NoMatchingOverload);
                };
                fields.push(Arc::new(arrow_schema::Field::new(
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
        other => {
            work.flush()?;
            let result = dynamic_scalar_data_type(other, &argument_types);
            work.step()?;
            result.ok_or(FunctionBindingError::UnknownFunction)?
        }
    };
    if matches!(name, "variant_get" | "try_variant_get") {
        if !(2..=3).contains(&request.arguments.len())
            || utf8_constant(request.arguments.get(1), work)?.is_none()
        {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        if request.arguments.len() == 3 {
            let Some(target) = utf8_constant(request.arguments.get(2), work)? else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            result = novarocks_type_contract::variant_get_target_type(target)
                .map_err(|message| FunctionBindingError::InvalidBinding(message.into()))?;
        }
    }
    let nullable = match name {
        "__array_literal" | "map" | "named_struct" | "row" | "struct" => false,
        "__struct_subfield" | "__array_struct_subfield" => true,
        _ => scalar_result_nullable(name, request, work)?,
    };
    let value = |index: usize| match request.arguments.get(index) {
        Some(FunctionArgument::Value { value_type, .. }) => Ok(value_type),
        _ => Err(FunctionBindingError::NoMatchingOverload),
    };
    let mut full_result = FunctionValueType::new(result, nullable);
    match name {
        "__array_literal" => {
            let arguments = dynamic_values(request, work)?;
            let item = fold_value_types(arguments.iter().copied(), work)?;
            full_result.data_type = DataType::List(Arc::new(value_field("item", &item, true)));
        }
        "array_repeat" => {
            if value(1)?.logical_type != ValueLogicalType::Physical {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            full_result.data_type = DataType::List(Arc::new(value_field("item", value(0)?, true)));
        }
        "array_intersect" => {
            let items = dynamic_values(request, work)?
                .into_iter()
                .map(|value| {
                    work.step()?;
                    full_list_item(value)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let item = fold_value_types(items.iter(), work)?;
            full_result.data_type = DataType::List(Arc::new(value_field("item", &item, true)));
        }
        "array_flatten" => {
            let outer = full_list_item(value(0)?)?;
            full_result.data_type = DataType::List(Arc::new(value_field(
                "item",
                &full_list_item(&outer)?,
                true,
            )));
        }
        "array_map" => {
            let Some(FunctionArgument::Lambda {
                parameter_types,
                result_type,
            }) = request.arguments.first()
            else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            let items = request.arguments[1..]
                .iter()
                .map(|argument| {
                    work.step()?;
                    match argument {
                        FunctionArgument::Value { value_type, .. } => full_list_item(value_type),
                        _ => Err(FunctionBindingError::NoMatchingOverload),
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            if items.len() != parameter_types.len() {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            for (item, parameter) in items.iter().zip(parameter_types) {
                work.step()?;
                // Preserve the existing lambda source-domain policy. Recursive
                // carrier equality remains an opaque legacy operation here.
                if item.data_type != DataType::Null
                    && (item.logical_type != parameter.logical_type
                        || item.data_type != parameter.data_type)
                {
                    return Err(FunctionBindingError::NoMatchingOverload);
                }
            }
            full_result.data_type =
                DataType::List(Arc::new(value_field("item", result_type, true)));
        }
        "array_sort_lambda" => {
            let _ = full_list_item(value(0)?)?;
            full_result = value(0)?.clone();
        }
        "arrays_zip" => {
            let items = dynamic_values(request, work)?
                .into_iter()
                .map(|value| {
                    work.step()?;
                    full_list_item(value)
                })
                .collect::<Result<Vec<_>, _>>()?;
            full_result.data_type = DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                DataType::Struct(
                    items
                        .iter()
                        .enumerate()
                        .map(|(index, item)| {
                            work.step()?;
                            Ok(Arc::new(value_field(
                                &format!("col{}", index + 1),
                                item,
                                true,
                            )))
                        })
                        .collect::<Result<Vec<_>, FunctionBindingError>>()?
                        .into(),
                ),
                true,
            )));
        }
        "greatest" | "least" => {
            full_result = fold_value_types(dynamic_values(request, work)?.into_iter(), work)?;
            if full_result.data_type == DataType::Date32 {
                full_result.data_type =
                    DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None);
            }
        }
        "map" => {
            let arguments = dynamic_values(request, work)?;
            let key = fold_value_types(arguments.iter().step_by(2).copied(), work)?;
            let value = fold_value_types(arguments.iter().skip(1).step_by(2).copied(), work)?;
            full_result.data_type = full_map_type(&key, &value);
        }
        "map_from_arrays" => {
            full_result.data_type =
                full_map_type(&full_list_item(value(0)?)?, &full_list_item(value(1)?)?)
        }
        "map_concat" => {
            let pairs = dynamic_values(request, work)?
                .into_iter()
                .map(|value| {
                    work.step()?;
                    full_map_items(value)
                })
                .collect::<Result<Vec<_>, _>>()?;
            if pairs.is_empty() {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            full_result.data_type = full_map_type(
                &fold_value_types(pairs.iter().map(|pair| &pair.0), work)?,
                &fold_value_types(pairs.iter().map(|pair| &pair.1), work)?,
            );
        }
        "map_entries" => {
            let (key, value) = full_map_items(value(0)?)?;
            let DataType::Map(entries, _) = full_map_type(&key, &value) else {
                unreachable!("map constructor");
            };
            full_result.data_type = DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                entries.data_type().clone(),
                true,
            )));
        }
        "map_apply" | "transform_keys" | "transform_values" => {
            let _ = full_map_items(value(1)?)?;
            let Some(FunctionArgument::Lambda { result_type, .. }) = request.arguments.first()
            else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            let _ = full_map_items(result_type)?;
            full_result = result_type.clone();
        }
        "named_struct" | "row" | "struct" => {
            let fields = if name == "named_struct" {
                request
                    .arguments
                    .chunks_exact(2)
                    .map(|pair| {
                        work.step()?;
                        let name = utf8_constant(pair.first(), work)?
                            .ok_or(FunctionBindingError::NoMatchingOverload)?;
                        let FunctionArgument::Value { value_type, .. } = &pair[1] else {
                            return Err(FunctionBindingError::NoMatchingOverload);
                        };
                        Ok(Arc::new(value_field(name, value_type, true)))
                    })
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                dynamic_values(request, work)?
                    .iter()
                    .enumerate()
                    .map(|(index, ty)| {
                        work.step()?;
                        Ok(Arc::new(value_field(
                            &format!("col{}", index + 1),
                            ty,
                            true,
                        )))
                    })
                    .collect::<Result<Vec<_>, FunctionBindingError>>()?
            };
            full_result.data_type = DataType::Struct(fields.into());
        }
        "__struct_subfield" | "__array_struct_subfield" => {
            let name = utf8_constant(request.arguments.get(1), work)?
                .ok_or(FunctionBindingError::NoMatchingOverload)?;
            if value(1)?.logical_type != ValueLogicalType::Physical {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            let source = if name.is_empty() {
                return Err(FunctionBindingError::NoMatchingOverload);
            } else if self_array_subfield(function_id) {
                full_list_item(value(0)?)?
            } else {
                value(0)?.clone()
            };
            if source.logical_type != ValueLogicalType::Physical {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            let DataType::Struct(fields) = &source.data_type else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            let mut field = None;
            for candidate in fields {
                work.step()?;
                if candidate.name().eq_ignore_ascii_case(name) {
                    field = Some(candidate);
                    break;
                }
            }
            let field = field.ok_or(FunctionBindingError::NoMatchingOverload)?;
            let projected =
                field_value_type(field).ok_or(FunctionBindingError::NoMatchingOverload)?;
            if self_array_subfield(function_id) {
                full_result.data_type =
                    DataType::List(Arc::new(value_field("item", &projected, true)));
            } else {
                full_result = projected;
            }
        }
        "array_sum" | "array_avg" | "array_cum_sum" | "array_difference" => {
            let item = full_list_item(value(0)?)?;
            let largeint = name == "array_sum" && item.logical_type == ValueLogicalType::LargeInt;
            if !largeint
                && (item.logical_type != ValueLogicalType::Physical
                    || matches!(item.data_type, DataType::FixedSizeBinary(_)))
            {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            if largeint {
                full_result.logical_type = ValueLogicalType::LargeInt;
            }
        }
        "variant_get" | "try_variant_get" => {
            if value(0)?.logical_type != ValueLogicalType::Variant
                && value(0)?.data_type != DataType::Null
            {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            if value(1)?.logical_type != ValueLogicalType::Physical
                || (request.arguments.len() == 3
                    && value(2)?.logical_type != ValueLogicalType::Physical)
            {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            if request.arguments.len() == 2 {
                full_result.logical_type = ValueLogicalType::Variant;
            }
        }
        "md5sum_numeric" | "xx_hash3_128" => full_result.logical_type = ValueLogicalType::LargeInt,
        "array_generate" => {
            let values = dynamic_values(request, work)?;
            if !observed_all(
                values.into_iter(),
                |ty| ty.logical_type == ValueLogicalType::Physical,
                work,
            )? {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
        }
        _ => {}
    }
    full_result.nullable = nullable;
    binding_control::value_type(&full_result, work)?;
    Ok(full_result)
}

fn self_array_subfield(function_id: &FunctionId) -> bool {
    function_id.as_str() == "builtin.scalar/__array_struct_subfield/v1"
}

impl BuiltinDynamicScalarResolver {
    fn bind_selection_observed(
        &self,
        request: FunctionBindingRequest<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        if !matches!(self.canonical_name.as_ref(), "round" | "truncate") {
            request_types_with_constants(request, work)?;
        }
        if request.logical_argument_count != request.arguments.len() {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        let result =
            bind_dynamic_scalar_result(&self.function_id, &self.canonical_name, request, work)?;
        let argument_types = if self.function_id.as_str() == "builtin.scalar/__array_literal/v1" {
            let item = full_list_item(&result)?;
            request
                .arguments
                .iter()
                .map(|argument| {
                    let FunctionArgument::Value {
                        value_type: source, ..
                    } = argument
                    else {
                        return Err(FunctionBindingError::NoMatchingOverload);
                    };
                    let target = FunctionValueType {
                        nullable: source.nullable,
                        ..item.clone()
                    };
                    work.step()?;
                    super::value_conversion::conversion_intermediate_type(source, &target)?;
                    work.step()?;
                    Ok(FunctionArgumentType::Value(target))
                })
                .collect::<Result<Vec<_>, FunctionBindingError>>()?
        } else {
            binding_control::argument_types(request, work)?.into_vec()
        };
        Ok(FunctionBindingSelection {
            overload: self.overload.clone(),
            argument_types: argument_types.into(),
            result_type: FunctionResultType::Scalar(result),
            aggregate: None,
        })
    }
}

impl FunctionBindingResolver for BuiltinDynamicScalarResolver {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| self.bind_selection_observed(request, work))
    }

    fn select_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| {
            if overload != &self.overload {
                return Err(FunctionBindingError::UnknownOverload(overload.clone()));
            }
            self.bind_selection_observed(request, work)
        })
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        binding_control::scope(control, |work| {
            request_types_with_constants(request, work)?;
            if request.logical_argument_count != request.arguments.len() {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            if selected.overload != self.overload {
                return Err(FunctionBindingError::UnknownOverload(
                    selected.overload.clone(),
                ));
            }
            let request = if self.function_id.as_str() == "builtin.scalar/__array_literal/v1"
                && request.arguments.is_empty()
            {
                let FunctionResultType::Scalar(result) = &selected.result_type else {
                    return Err(FunctionBindingError::NoMatchingOverload);
                };
                if let Some(expected) = request.expected_result_type
                    && !binding_control::exact_type(expected, result, work)?
                {
                    return Err(FunctionBindingError::InvalidBinding(
                        "selected empty array differs from its explicit result constraint".into(),
                    ));
                }
                FunctionBindingRequest {
                    expected_result_type: Some(result),
                    ..request
                }
            } else {
                request
            };
            work.flush()?;
            let expected = self.resolve(request, work.control())?;
            if binding_control::same_selection(selected, &expected, work)? {
                Ok(())
            } else {
                Err(FunctionBindingError::InvalidBinding(
                    "selected dynamic scalar binding differs from its declared overload".into(),
                ))
            }
        })
    }
}

pub(super) fn scalar_definition_parts(
    name: &str,
    signatures: &[String],
    kind: FunctionKind,
) -> Result<(FunctionBindingDeclaration, BuiltinScalarResolver), FunctionCatalogError> {
    let overloads = signatures
        .iter()
        .map(|signature| builtin_scalar_overload_id(name, signature, kind))
        .collect::<Result<Vec<_>, _>>()?;
    let function_id = builtin_scalar_function_id(name, kind)?;
    let resolver = BuiltinScalarResolver {
        function_id: function_id.clone(),
        canonical_name: name.into(),
        overloads: overloads.clone().into_boxed_slice(),
    };
    let result_domain = builtin_fixed_result_domain(&function_id);
    let declaration = FunctionBindingDeclaration::try_new(
        function_id,
        kind,
        overloads
            .into_iter()
            .zip(signatures)
            .map(|(identity, signature)| {
                let effects = match name {
                    name if super::window_ranking_owner::operation(name).is_some() => {
                        Some(super::window_ranking_owner::effects())
                    }
                    name if super::window_value_owner::operation(name).is_some() => {
                        Some(super::window_value_owner::effects())
                    }
                    name if super::window_ntile_owner::operation(name).is_some() => {
                        Some(super::window_ntile_owner::effects())
                    }
                    name if super::window_offset_owner::operation(name).is_some() => {
                        Some(super::window_offset_owner::effects())
                    }
                    "abs" => Some(super::abs_owner::effects()),
                    "nullif" => Some(super::nullif_owner::effects()),
                    name if super::calendar_time_text_owner::operation(name).is_some() => {
                        Some(super::calendar_time_text_owner::effects(
                            super::calendar_time_text_owner::operation(name)
                                .expect("matched TIME source owner"),
                        ))
                    }
                    name if super::control_owner::operation(name).is_some() => {
                        Some(super::control_owner::effects(
                            super::control_owner::operation(name)
                                .expect("matched control operation"),
                        ))
                    }
                    "crc32" => Some(super::crc32_owner::effects()),
                    "bitmap_to_string" => Some(super::bitmap_to_string_owner::effects()),
                    "parse_json" => Some(super::parse_json_owner::effects()),
                    "percentile_hash" => Some(super::percentile_hash_owner::effects()),
                    "hll_hash" => Some(super::hll_hash_owner::effects()),
                    name if super::string_measure_owner::operation(name).is_some() => {
                        Some(super::string_measure_owner::effects())
                    }
                    name if super::string_case_owner::operation(name).is_some() => {
                        Some(super::string_case_owner::effects())
                    }
                    name if super::string_concat_owner::operation(name).is_some() => {
                        Some(super::string_concat_owner::effects())
                    }
                    name if super::string_find_in_set_owner::operation(name).is_some() => {
                        Some(super::string_find_in_set_owner::effects())
                    }
                    "field" => Some(super::string_field_owner::effects()),
                    name if super::string_trim_owner::operation(name).is_some() => {
                        Some(super::string_trim_owner::effects())
                    }
                    name if super::string_translate_owner::operation(name).is_some() => {
                        Some(super::string_translate_owner::effects())
                    }
                    name if super::string_url_encode_owner::operation(name).is_some() => {
                        Some(super::string_url_encode_owner::effects())
                    }
                    name if super::string_url_decode_owner::operation(name).is_some() => {
                        Some(super::string_url_decode_owner::effects())
                    }
                    name if super::string_substring_owner::operation(name).is_some() => {
                        Some(super::string_substring_owner::effects())
                    }
                    name if super::string_substring_index_owner::operation(name).is_some() => {
                        Some(super::string_substring_index_owner::effects())
                    }
                    name if super::string_left_right_owner::operation(name).is_some() => {
                        Some(super::string_left_right_owner::effects())
                    }
                    name if super::string_repeat_owner::operation(name).is_some() => {
                        Some(super::string_repeat_owner::effects())
                    }
                    name if super::string_locate_owner::operation(name).is_some() => {
                        Some(super::string_locate_owner::effects())
                    }
                    name if super::string_initcap_owner::operation(name).is_some() => {
                        Some(super::string_initcap_owner::effects())
                    }
                    name if super::string_pad_owner::operation(name).is_some() => {
                        Some(super::string_pad_owner::effects())
                    }
                    name if super::string_replace_owner::operation(name).is_some() => {
                        Some(super::string_replace_owner::effects())
                    }
                    name if super::string_split_part_owner::operation(name).is_some() => {
                        Some(super::string_split_part_owner::effects())
                    }
                    name if super::string_append_trailing_owner::operation(name).is_some() => {
                        Some(super::string_append_trailing_owner::effects())
                    }
                    name if super::string_sha2_owner::operation(name).is_some() => {
                        Some(super::string_sha2_owner::effects())
                    }
                    name if super::string_concat_ws_owner::operation(name).is_some() => {
                        Some(super::string_concat_ws_owner::effects())
                    }
                    name if super::string_from_base64_owner::operation(name).is_some() => {
                        Some(super::string_from_base64_owner::effects())
                    }
                    name if super::string_hex_owner::operation(name).is_some() => {
                        Some(super::string_hex_owner::effects())
                    }
                    "md5sum" => Some(super::md5sum_owner::effects()),
                    "split" => Some(super::string_split_owner::effects()),
                    "__map_element_at" => Some(super::map_element_at_owner::effects()),
                    "__array_element_at" => Some(super::array_element_at_owner::effects()),
                    "array_append" => Some(super::array_append_owner::effects()),
                    name if super::calendar_slice_owner::operation(name).is_some() => {
                        Some(super::calendar_slice_owner::effects())
                    }
                    name if super::calendar_unixtime_owner::operation(name).is_some() => {
                        Some(super::calendar_unixtime_owner::effects())
                    }
                    name if super::string_md5_owner::operation(name).is_some() => {
                        Some(super::string_md5_owner::effects())
                    }
                    name if super::string_sm3_owner::operation(name).is_some() => {
                        Some(super::string_sm3_owner::effects())
                    }
                    name if super::calendar_day_number_owner::operation(name).is_some() => {
                        Some(super::calendar_day_number_owner::effects())
                    }
                    name if super::makedate_owner::operation(name).is_some() => {
                        Some(super::makedate_owner::effects())
                    }
                    name if super::map_projection_owner::operation(name).is_some() => {
                        Some(super::map_projection_owner::effects())
                    }
                    name if super::map_size_owner::operation(name) => {
                        Some(super::map_size_owner::effects())
                    }
                    name if super::ds_hll_state_owner::operation(name) => {
                        Some(super::ds_hll_state_owner::effects())
                    }
                    name if super::percentile_approx_raw_owner::operation(name) => {
                        Some(super::percentile_approx_raw_owner::effects())
                    }
                    name if super::array_match_owner::operation(name).is_some() => {
                        Some(super::array_match_owner::effects())
                    }
                    name if super::collection_cardinality_owner::operation(name).is_some() => {
                        Some(super::collection_cardinality_owner::effects())
                    }
                    name if super::calendar_period_diff_owner::operation(name).is_some() => {
                        Some(super::calendar_period_diff_owner::effects())
                    }
                    name if super::calendar_diff_owner::operation(name).is_some() => {
                        Some(super::calendar_diff_owner::effects())
                    }
                    name if super::calendar_extended_owner::operation(name).is_some() => {
                        Some(super::calendar_extended_owner::effects(
                            super::calendar_extended_owner::operation(name)
                                .expect("matched operation"),
                        ))
                    }
                    name if super::calendar_parts_owner::operation(name).is_some() => {
                        Some(super::calendar_parts_owner::effects())
                    }
                    name if super::string_extended_owner::operation(name).is_some() => {
                        Some(super::string_extended_owner::effects(
                            super::string_extended_owner::operation(name)
                                .expect("matched operation"),
                        ))
                    }
                    name if super::date_owner::operation(name).is_some() => {
                        Some(super::date_owner::effects())
                    }
                    name if super::calendar_to_date_owner::operation(name) => {
                        Some(super::calendar_to_date_owner::effects())
                    }
                    name if super::calendar_sec_to_time_owner::operation(name).is_some() => {
                        Some(super::calendar_sec_to_time_owner::effects())
                    }
                    "regexp_count" => Some(super::regexp_count_owner::effects()),
                    "parse_url" => Some(super::string_parse_url_owner::effects()),
                    "to_base64" => Some(super::string_to_base64_owner::effects()),
                    "regexp_position" => Some(super::regexp_position_owner::effects()),
                    name if super::string_reverse_owner::operation(name).is_some() => {
                        Some(super::string_reverse_owner::effects())
                    }
                    "dround" => Some(super::dround_owner::effects()),
                    name if super::bit_shift_owner::operation(name).is_some() => {
                        Some(super::bit_shift_owner::effects())
                    }
                    name if super::bitwise_owner::operation(name).is_some() => {
                        Some(super::bitwise_owner::effects())
                    }
                    "rand" | "random" => Some(super::rand_owner::effects()),
                    name if super::numeric_elementary_owner::is_installed(name) => {
                        Some(super::numeric_elementary_owner::effects())
                    }
                    name if super::numeric_mod_owner::operation(name).is_some() => {
                        Some(super::numeric_mod_owner::effects())
                    }
                    name if super::numeric_binary_owner::operation(name).is_some() => {
                        Some(super::numeric_binary_owner::effects())
                    }
                    name if super::numeric_unary_owner::operation(name).is_some() => {
                        Some(super::numeric_unary_owner::effects())
                    }
                    _ => None,
                };
                FunctionOverloadDeclaration {
                    semantics: effects
                        .as_ref()
                        .map(FunctionSemantics::from_effects)
                        .unwrap_or_else(|| builtin_scalar_semantics(name)),
                    effects,
                    identity,
                    argument_pattern: signature.clone().into_boxed_str(),
                    result_pattern: match result_domain {
                        Some(domain) => format!(
                            "{signature};root={}",
                            domain.metadata_value().expect("declared semantic root")
                        )
                        .into_boxed_str(),
                        None => signature.clone().into_boxed_str(),
                    },
                    aggregate: None,
                }
            }),
    )
    .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
        subject: "builtin scalar binding declaration",
        value: error.to_string().into(),
    })?;
    Ok((declaration, resolver))
}

pub(super) fn dynamic_definition_parts(
    name: &str,
) -> Result<(FunctionBindingDeclaration, BuiltinDynamicScalarResolver), FunctionCatalogError> {
    if !matches!(
        builtin_disposition(name),
        Some(BuiltinDisposition::InstalledScalar(_))
    ) {
        return Err(FunctionCatalogError::InvalidStableIdentity {
            subject: "unclassified dynamic scalar implementation",
            value: name.into(),
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
    let pure_effects = match name {
        "map_entries" => Some(super::map_entries_owner::effects()),
        name if super::array_difference_owner::operation(name).is_some() => {
            Some(super::array_difference_owner::effects())
        }
        "__array_literal" => Some(super::array_literal_owner::effects()),
        "md5sum_numeric" => Some(super::md5sum_numeric_owner::effects()),
        "null_or_empty" => Some(super::string_null_or_empty_owner::effects()),
        "truncate" => Some(super::truncate_owner::effects()),
        "round" => Some(super::round_owner::effects()),
        name if super::scalar_extrema_owner::operation(name) => {
            Some(super::scalar_extrema_owner::effects())
        }
        name if super::xx_hash3_128_owner::operation(name) => {
            Some(super::xx_hash3_128_owner::effects())
        }
        _ => None,
    };
    let overload_declaration = if let Some(effects) = pure_effects {
        FunctionOverloadDeclaration::from_effects(
            overload.clone(),
            "owner-derived",
            "owner-derived",
            None,
            effects,
        )
    } else {
        FunctionOverloadDeclaration {
            effects: None,
            semantics: builtin_scalar_semantics(name),
            identity: overload.clone(),
            argument_pattern: "owner-derived".into(),
            result_pattern: "owner-derived".into(),
            aggregate: None,
        }
    };
    let declaration = FunctionBindingDeclaration::try_new(
        function_id.clone(),
        FunctionKind::Scalar,
        [overload_declaration],
    )
    .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
        subject: "dynamic scalar binding declaration",
        value: error.to_string().into(),
    })?;
    let resolver = BuiltinDynamicScalarResolver {
        function_id,
        canonical_name: name.into(),
        overload,
    };
    Ok((declaration, resolver))
}

pub fn contribute_builtin_functions(
    builder: &mut EngineFunctionCatalogBuilder,
) -> Result<(), FunctionCatalogError> {
    builder.register(super::value_conversion::value_conversion_definition()?)?;
    for name in DYNAMIC_SCALAR_FUNCTIONS {
        let (declaration, resolver) = dynamic_definition_parts(name)?;
        let definition = match *name {
            "map_entries" => super::map_entries_owner::definition(name, declaration, resolver)?,
            name if super::array_difference_owner::operation(name).is_some() => {
                super::array_difference_owner::definition(name, declaration, resolver)?
            }
            "__array_literal" => {
                super::array_literal_owner::definition(name, declaration, resolver)?
            }
            "md5sum_numeric" => {
                super::md5sum_numeric_owner::definition(name, declaration, resolver)?
            }
            "null_or_empty" => {
                super::string_null_or_empty_owner::definition(name, declaration, resolver)?
            }
            "truncate" => super::truncate_owner::definition(declaration, resolver)?,
            "round" => super::round_owner::definition(declaration, resolver)?,
            name if super::scalar_extrema_owner::operation(name) => {
                super::scalar_extrema_owner::definition(name, declaration, resolver)?
            }
            name if super::xx_hash3_128_owner::operation(name) => {
                super::xx_hash3_128_owner::definition(name, declaration, resolver)?
            }
            _ => FunctionDefinition::try_new_bound(
                name,
                FunctionVisibility::Public,
                declaration,
                Arc::new(resolver),
            )?,
        };
        builder.register(definition)?;
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
        let (declaration, resolver) = scalar_definition_parts(&name, &signatures, kind)?;
        let definition = match name.as_str() {
            name if super::window_ranking_owner::operation(name).is_some() => {
                super::window_ranking_owner::definition(name, declaration, resolver)?
            }
            name if super::window_value_owner::operation(name).is_some() => {
                super::window_value_owner::definition(name, declaration, resolver)?
            }
            name if super::window_ntile_owner::operation(name).is_some() => {
                super::window_ntile_owner::definition(name, declaration, resolver)?
            }
            name if super::window_offset_owner::operation(name).is_some() => {
                super::window_offset_owner::definition(name, declaration, resolver)?
            }
            "abs" => super::abs_owner::definition(declaration, resolver)?,
            "nullif" => super::nullif_owner::definition(&name, declaration, resolver)?,
            name if super::calendar_time_text_owner::operation(name).is_some() => {
                super::calendar_time_text_owner::definition(name, declaration, resolver)?
            }
            name if super::control_owner::operation(name).is_some() => {
                super::control_owner::definition(name, declaration, resolver)?
            }
            "crc32" => super::crc32_owner::definition(declaration, resolver)?,
            "bitmap_to_string" => super::bitmap_to_string_owner::definition(declaration, resolver)?,
            "parse_json" => super::parse_json_owner::definition(declaration, resolver)?,
            "percentile_hash" => super::percentile_hash_owner::definition(declaration, resolver)?,
            "hll_hash" => super::hll_hash_owner::definition(declaration, resolver)?,
            name if super::string_measure_owner::operation(name).is_some() => {
                super::string_measure_owner::definition(name, declaration, resolver)?
            }
            name if super::string_case_owner::operation(name).is_some() => {
                super::string_case_owner::definition(name, declaration, resolver)?
            }
            name if super::string_concat_owner::operation(name).is_some() => {
                super::string_concat_owner::definition(name, declaration, resolver)?
            }
            name if super::string_find_in_set_owner::operation(name).is_some() => {
                super::string_find_in_set_owner::definition(name, declaration, resolver)?
            }
            "field" => super::string_field_owner::definition(&name, declaration, resolver)?,
            name if super::string_trim_owner::operation(name).is_some() => {
                super::string_trim_owner::definition(name, declaration, resolver)?
            }
            name if super::string_translate_owner::operation(name).is_some() => {
                super::string_translate_owner::definition(name, declaration, resolver)?
            }
            name if super::string_url_encode_owner::operation(name).is_some() => {
                super::string_url_encode_owner::definition(name, declaration, resolver)?
            }
            name if super::string_url_decode_owner::operation(name).is_some() => {
                super::string_url_decode_owner::definition(name, declaration, resolver)?
            }
            name if super::string_substring_owner::operation(name).is_some() => {
                super::string_substring_owner::definition(name, declaration, resolver)?
            }
            name if super::string_substring_index_owner::operation(name).is_some() => {
                super::string_substring_index_owner::definition(name, declaration, resolver)?
            }
            name if super::string_left_right_owner::operation(name).is_some() => {
                super::string_left_right_owner::definition(name, declaration, resolver)?
            }
            name if super::string_repeat_owner::operation(name).is_some() => {
                super::string_repeat_owner::definition(name, declaration, resolver)?
            }
            name if super::string_locate_owner::operation(name).is_some() => {
                super::string_locate_owner::definition(name, declaration, resolver)?
            }
            name if super::string_initcap_owner::operation(name).is_some() => {
                super::string_initcap_owner::definition(name, declaration, resolver)?
            }
            name if super::string_pad_owner::operation(name).is_some() => {
                super::string_pad_owner::definition(name, declaration, resolver)?
            }
            name if super::string_replace_owner::operation(name).is_some() => {
                super::string_replace_owner::definition(name, declaration, resolver)?
            }
            name if super::string_split_part_owner::operation(name).is_some() => {
                super::string_split_part_owner::definition(name, declaration, resolver)?
            }
            name if super::string_append_trailing_owner::operation(name).is_some() => {
                super::string_append_trailing_owner::definition(name, declaration, resolver)?
            }
            name if super::string_sha2_owner::operation(name).is_some() => {
                super::string_sha2_owner::definition(name, declaration, resolver)?
            }
            name if super::string_concat_ws_owner::operation(name).is_some() => {
                super::string_concat_ws_owner::definition(name, declaration, resolver)?
            }
            name if super::string_from_base64_owner::operation(name).is_some() => {
                super::string_from_base64_owner::definition(name, declaration, resolver)?
            }
            name if super::string_hex_owner::operation(name).is_some() => {
                super::string_hex_owner::definition(name, declaration, resolver)?
            }
            "md5sum" => super::md5sum_owner::definition(&name, declaration, resolver)?,
            "split" => super::string_split_owner::definition(&name, declaration, resolver)?,
            "__map_element_at" => {
                super::map_element_at_owner::definition(&name, declaration, resolver)?
            }
            "__array_element_at" => {
                super::array_element_at_owner::definition(&name, declaration, resolver)?
            }
            "array_append" => super::array_append_owner::definition(&name, declaration, resolver)?,
            name if super::calendar_slice_owner::operation(name).is_some() => {
                super::calendar_slice_owner::definition(name, declaration, resolver)?
            }
            name if super::calendar_unixtime_owner::operation(name).is_some() => {
                super::calendar_unixtime_owner::definition(name, declaration, resolver)?
            }
            name if super::string_md5_owner::operation(name).is_some() => {
                super::string_md5_owner::definition(name, declaration, resolver)?
            }
            name if super::string_sm3_owner::operation(name).is_some() => {
                super::string_sm3_owner::definition(name, declaration, resolver)?
            }
            name if super::calendar_day_number_owner::operation(name).is_some() => {
                super::calendar_day_number_owner::definition(name, declaration, resolver)?
            }
            name if super::makedate_owner::operation(name).is_some() => {
                super::makedate_owner::definition(name, declaration, resolver)?
            }
            name if super::map_projection_owner::operation(name).is_some() => {
                super::map_projection_owner::definition(name, declaration, resolver)?
            }
            name if super::map_size_owner::operation(name) => {
                super::map_size_owner::definition(name, declaration, resolver)?
            }
            name if super::ds_hll_state_owner::operation(name) => {
                super::ds_hll_state_owner::definition(name, declaration, resolver)?
            }
            name if super::percentile_approx_raw_owner::operation(name) => {
                super::percentile_approx_raw_owner::definition(name, declaration, resolver)?
            }
            name if super::array_match_owner::operation(name).is_some() => {
                super::array_match_owner::definition(name, declaration, resolver)?
            }
            name if super::collection_cardinality_owner::operation(name).is_some() => {
                super::collection_cardinality_owner::definition(name, declaration, resolver)?
            }
            name if super::calendar_period_diff_owner::operation(name).is_some() => {
                super::calendar_period_diff_owner::definition(name, declaration, resolver)?
            }
            name if super::calendar_diff_owner::operation(name).is_some() => {
                super::calendar_diff_owner::definition(name, declaration, resolver)?
            }
            name if super::calendar_extended_owner::operation(name).is_some() => {
                super::calendar_extended_owner::definition(name, declaration, resolver)?
            }
            name if super::string_extended_owner::operation(name).is_some() => {
                super::string_extended_owner::definition(name, declaration, resolver)?
            }
            name if super::calendar_parts_owner::operation(name).is_some() => {
                super::calendar_parts_owner::definition(name, declaration, resolver)?
            }
            name if super::date_owner::operation(name).is_some() => {
                super::date_owner::definition(name, declaration, resolver)?
            }
            name if super::calendar_to_date_owner::operation(name) => {
                super::calendar_to_date_owner::definition(name, declaration, resolver)?
            }
            name if super::calendar_sec_to_time_owner::operation(name).is_some() => {
                super::calendar_sec_to_time_owner::definition(name, declaration, resolver)?
            }
            "regexp_count" => super::regexp_count_owner::definition(&name, declaration, resolver)?,
            name if super::string_parse_url_owner::operation(name).is_some() => {
                super::string_parse_url_owner::definition(name, declaration, resolver)?
            }
            name if super::string_to_base64_owner::operation(name).is_some() => {
                super::string_to_base64_owner::definition(name, declaration, resolver)?
            }
            "regexp_position" => super::regexp_position_owner::definition(declaration, resolver)?,
            name if super::string_reverse_owner::operation(name).is_some() => {
                super::string_reverse_owner::definition(name, declaration, resolver)?
            }
            "dround" => super::dround_owner::definition(declaration, resolver)?,
            name if super::bit_shift_owner::operation(name).is_some() => {
                super::bit_shift_owner::definition(name, declaration, resolver)?
            }
            name if super::bitwise_owner::operation(name).is_some() => {
                super::bitwise_owner::definition(name, declaration, resolver)?
            }
            "rand" | "random" => super::rand_owner::definition(&name, declaration, resolver)?,
            name if super::numeric_elementary_owner::is_installed(name) => {
                super::numeric_elementary_owner::definition(name, declaration, resolver)?
            }
            name if super::numeric_mod_owner::operation(name).is_some() => {
                super::numeric_mod_owner::definition(name, declaration, resolver)?
            }
            name if super::numeric_binary_owner::operation(name).is_some() => {
                super::numeric_binary_owner::definition(name, declaration, resolver)?
            }
            name if super::numeric_unary_owner::operation(name).is_some() => {
                super::numeric_unary_owner::definition(name, declaration, resolver)?
            }
            _ => FunctionDefinition::try_new_bound(
                &name,
                FunctionVisibility::Public,
                declaration,
                Arc::new(resolver),
            )?,
        };
        builder.register(definition)?;
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
        let state_format =
            AggregateStateFormatIdentity::try_new(builtin_aggregate_state_format(declaration.name))
                .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
                    subject: "builtin aggregate state format",
                    value: error.to_string().into(),
                })?;
        let binding_declaration = FunctionBindingDeclaration::try_new(
            function_id,
            FunctionKind::Aggregate,
            [FunctionOverloadDeclaration {
                effects: match declaration.name {
                    "group_concat" | "string_agg" => Some(super::aggregate_concat_owner::effects()),
                    "array_agg" | "array_agg_distinct" | "array_unique_agg" => {
                        Some(super::aggregate_array_owner::effects())
                    }
                    "bitmap_union_int" | "bitmap_agg" => {
                        Some(super::aggregate_bitmap_union_int_owner::effects())
                    }
                    "hll_union" | "hll_raw_agg" | "hll_union_agg" => {
                        Some(super::aggregate_hll_payload_owner::effects())
                    }
                    "ndv" | "approx_count_distinct" => Some(super::aggregate_hll_owner::effects()),
                    "ds_hll_count_distinct"
                    | "approx_count_distinct_hll_sketch"
                    | "ds_hll_count_distinct_merge"
                    | "ds_hll_count_distinct_union" => {
                        Some(super::aggregate_ds_hll_owner::effects())
                    }
                    "map_agg" => Some(super::aggregate_map_owner::effects()),
                    "count" => Some(super::aggregate_count_owner::effects()),
                    "multi_distinct_count" => {
                        Some(super::aggregate_count_distinct_owner::effects())
                    }
                    "any_value" => Some(super::aggregate_any_value_owner::effects()),
                    "percentile_approx" | "percentile_approx_weighted" | "percentile_union" => {
                        Some(super::aggregate_approx_percentile_owner::effects())
                    }
                    "percentile_cont" | "percentile_disc" | "percentile_disc_lc" => {
                        Some(super::aggregate_percentile_owner::effects())
                    }
                    "approx_top_k" => Some(super::aggregate_top_k_owner::effects()),
                    "max_by" | "min_by" => Some(super::aggregate_by_owner::effects()),
                    "min_n" | "max_n" => Some(super::aggregate_n_owner::effects()),
                    "sum" => Some(super::aggregate_sum_owner::effects()),
                    name if super::aggregate_distinct_numeric_kernel::operation(name).is_some() => {
                        Some(super::aggregate_distinct_numeric_owner::effects())
                    }
                    name if super::aggregate_basic::operation(name).is_some() => {
                        Some(super::aggregate_basic_owner::effects())
                    }
                    name if super::aggregate_extrema_owner::operation(name).is_some() => {
                        Some(super::aggregate_extrema_owner::effects())
                    }
                    _ => None,
                },
                semantics: FunctionSemantics {
                    volatility: FunctionVolatility::Immutable,
                    argument_evaluation: FunctionArgumentEvaluation::Eager,
                    failure_behavior: FunctionFailureBehavior::Propagate,
                    intrinsic_row_error:
                        novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated,
                },
                identity: overload_id,
                argument_pattern: declaration.signature.into(),
                result_pattern: "derived".into(),
                aggregate: Some(AggregateBindingDeclaration {
                    state_argument_contract: builtin_aggregate_state_argument_contract(
                        declaration.name,
                    ),
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
        if matches!(
            declaration.name,
            "array_agg" | "array_agg_distinct" | "array_unique_agg"
        ) {
            builder.register(super::aggregate_array_owner::definition(
                declaration.name,
                binding_declaration,
                resolver,
            )?)?;
            continue;
        }
        if matches!(declaration.name, "group_concat" | "string_agg") {
            builder.register(super::aggregate_concat_owner::definition(
                declaration.name,
                binding_declaration,
                resolver,
            )?)?;
            continue;
        }
        if declaration.name == "approx_top_k" {
            builder.register(super::aggregate_top_k_owner::definition(
                declaration.name,
                binding_declaration,
                resolver,
            )?)?;
            continue;
        }
        if matches!(declaration.name, "min_n" | "max_n") {
            builder.register(super::aggregate_n_owner::definition(
                declaration.name,
                binding_declaration,
                resolver,
            )?)?;
            continue;
        }
        if matches!(
            declaration.name,
            "percentile_approx" | "percentile_approx_weighted" | "percentile_union"
        ) {
            builder.register(super::aggregate_approx_percentile_owner::definition(
                declaration.name,
                binding_declaration,
                resolver,
            )?)?;
            continue;
        }
        if declaration.name == "map_agg" {
            builder.register(super::aggregate_map_owner::definition(
                declaration.name,
                binding_declaration,
                resolver,
            )?)?;
            continue;
        }
        if matches!(
            declaration.name,
            "percentile_cont" | "percentile_disc" | "percentile_disc_lc"
        ) {
            builder.register(super::aggregate_percentile_owner::definition(
                declaration.name,
                binding_declaration,
                resolver,
            )?)?;
            continue;
        }
        if matches!(
            declaration.name,
            "ds_hll_count_distinct"
                | "approx_count_distinct_hll_sketch"
                | "ds_hll_count_distinct_merge"
                | "ds_hll_count_distinct_union"
        ) {
            builder.register(super::aggregate_ds_hll_owner::definition(
                declaration.name,
                binding_declaration,
                resolver,
            )?)?;
            continue;
        }
        if matches!(declaration.name, "max_by" | "min_by") {
            builder.register(super::aggregate_by_owner::definition(
                declaration.name,
                binding_declaration,
                resolver,
            )?)?;
            continue;
        }
        if declaration.name == "any_value" {
            builder.register(super::aggregate_any_value_owner::definition(
                declaration.name,
                binding_declaration,
                resolver,
            )?)?;
            continue;
        }
        if declaration.name == "multi_distinct_count" {
            builder.register(super::aggregate_count_distinct_owner::definition(
                declaration.name,
                binding_declaration,
                resolver,
            )?)?;
            continue;
        }
        if matches!(
            declaration.name,
            "hll_union" | "hll_raw_agg" | "hll_union_agg"
        ) {
            builder.register(super::aggregate_hll_payload_owner::definition(
                declaration.name,
                binding_declaration,
                resolver,
            )?)?;
            continue;
        }
        if matches!(declaration.name, "bitmap_union_int" | "bitmap_agg") {
            builder.register(super::aggregate_bitmap_union_int_owner::definition(
                declaration.name,
                binding_declaration,
                resolver,
            )?)?;
            continue;
        }
        if matches!(declaration.name, "ndv" | "approx_count_distinct") {
            builder.register(super::aggregate_hll_owner::definition(
                declaration.name,
                binding_declaration,
                resolver,
            )?)?;
            continue;
        }
        if declaration.name == "count" {
            builder.register(super::aggregate_count_owner::definition(
                declaration.name,
                binding_declaration,
                resolver,
            )?)?;
            continue;
        }
        if declaration.name == "sum" {
            builder.register(super::aggregate_sum_owner::definition(
                declaration.name,
                binding_declaration,
                resolver,
            )?)?;
            continue;
        }
        if super::aggregate_extrema_owner::operation(declaration.name).is_some() {
            builder.register(super::aggregate_extrema_owner::definition(
                declaration.name,
                binding_declaration,
                resolver,
            )?)?;
            continue;
        }
        if super::aggregate_distinct_numeric_kernel::operation(declaration.name).is_some() {
            builder.register(super::aggregate_distinct_numeric_owner::definition(
                declaration.name,
                binding_declaration,
                resolver,
            )?)?;
            continue;
        }
        if super::aggregate_basic::operation(declaration.name).is_some() {
            builder.register(super::aggregate_basic_owner::definition(
                declaration.name,
                binding_declaration,
                resolver,
            )?)?;
            continue;
        }
        builder.register(FunctionDefinition::try_new_bound_aggregate(
            declaration.name,
            FunctionVisibility::Public,
            binding_declaration,
            Arc::clone(&resolver) as Arc<dyn crate::FunctionBindingResolver>,
            resolver as Arc<dyn crate::AggregateSignatureResolver>,
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
        [FunctionOverloadDeclaration {
            effects: Some(super::table_unnest_owner::effects()),
            semantics: FunctionSemantics {
                volatility: FunctionVolatility::Immutable,
                argument_evaluation: FunctionArgumentEvaluation::Eager,
                failure_behavior: FunctionFailureBehavior::Propagate,
                intrinsic_row_error: novarocks_type_contract::FunctionIntrinsicRowError::NoRowError,
            },
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
    builder.register(super::table_unnest_owner::definition(
        unnest_declaration,
        BuiltinUnnestResolver,
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
pub fn builtin_function_volatility(name: &str) -> FunctionVolatility {
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
    fn original_builtin_state_law_matches_signature_port_declaration_and_selection() {
        use novarocks_type_contract::AggregateStateArgumentContract;

        let catalog = build_builtin_engine_function_catalog().unwrap();
        for (name, expected) in [
            (
                "count",
                AggregateStateArgumentContract::ValueRootNullabilityIndependent,
            ),
            (
                "min",
                AggregateStateArgumentContract::ValueRootNullabilityIndependent,
            ),
            (
                "max",
                AggregateStateArgumentContract::ValueRootNullabilityIndependent,
            ),
            ("sum", AggregateStateArgumentContract::ExactSignature),
        ] {
            let args = [value_argument(DataType::Int64, true, None)];
            let binding = catalog
                .resolve_bound_user(
                    name,
                    FunctionKind::Aggregate,
                    FunctionBindingRequest {
                        expected_result_type: None,
                        arguments: &args,
                        logical_argument_count: 1,
                    },
                    crate::binding_test_control(),
                )
                .unwrap();
            let definition = catalog.definition_by_id(&binding.function_id).unwrap();
            let overload =
                AggregateOverloadIdentity::try_new(binding.selected.overload.as_str()).unwrap();
            let original = definition.aggregate_resolver.as_ref().unwrap();
            assert_eq!(
                original.state_argument_contract(&overload).unwrap(),
                expected
            );
            assert_eq!(
                binding
                    .selected
                    .aggregate
                    .as_ref()
                    .unwrap()
                    .state_argument_contract,
                expected
            );
            let declared = definition
                .binding_declaration()
                .unwrap()
                .overloads()
                .iter()
                .find(|candidate| candidate.identity == binding.selected.overload)
                .unwrap();
            assert_eq!(
                declared.aggregate.as_ref().unwrap().state_argument_contract,
                expected
            );
            let foreign = AggregateOverloadIdentity::try_new("test/foreign-state-law/v1").unwrap();
            assert!(matches!(
                original.state_argument_contract(&foreign),
                Err(FunctionResolutionError::BadSignature(_))
            ));
        }
    }

    fn value_argument(
        data_type: DataType,
        nullable: bool,
        constant: Option<crate::ConstantValue>,
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
                    expected_result_type: None,
                    arguments,
                    logical_argument_count: arguments.len(),
                },
                crate::binding_test_control(),
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
    fn positive_result_nullability_preserves_nonfinite_float_and_total_numeric_domains() {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        for data_type in [DataType::Float32, DataType::Float64] {
            for nullable in [false, true] {
                let arguments = [value_argument(data_type.clone(), nullable, None)];
                let selected = resolve_exact_scalar(&catalog, "positive", &arguments);
                assert_eq!(scalar_result(&selected).data_type, DataType::Float64);
                assert!(scalar_result(&selected).nullable);
            }
        }
        for data_type in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Decimal128(38, -2),
        ] {
            for nullable in [false, true] {
                let arguments = [value_argument(data_type.clone(), nullable, None)];
                let selected = resolve_exact_scalar(&catalog, "positive", &arguments);
                assert_eq!(scalar_result(&selected).data_type, DataType::Float64);
                assert_eq!(scalar_result(&selected).nullable, nullable);
            }
        }
        let float = [value_argument(DataType::Float64, false, None)];
        assert!(!scalar_result(&resolve_exact_scalar(&catalog, "abs", &float)).nullable);
        assert!(!scalar_result(&resolve_exact_scalar(&catalog, "sign", &float)).nullable);
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
                    expected_result_type: None,
                    arguments: &nonnull,
                    logical_argument_count: 1,
                },
                crate::binding_test_control(),
            )
            .expect("count binding");
        let sum = catalog
            .resolve_bound_user(
                "sum",
                FunctionKind::Aggregate,
                FunctionBindingRequest {
                    expected_result_type: None,
                    arguments: &nonnull,
                    logical_argument_count: 1,
                },
                crate::binding_test_control(),
            )
            .expect("sum binding");
        assert!(!scalar_result(&count).nullable);
        assert!(scalar_result(&sum).nullable);
    }

    #[test]
    fn literal_dependent_dynamic_scalars_freeze_exact_result_shapes() {
        let catalog = build_builtin_engine_function_catalog().expect("builtin catalog");
        let named = [
            value_argument(
                DataType::Utf8,
                false,
                Some(constant_binding_tests::utf8("left", false)),
            ),
            value_argument(DataType::Int64, false, None),
            value_argument(
                DataType::Utf8,
                false,
                Some(constant_binding_tests::utf8("right", false)),
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
                    Some(constant_binding_tests::i64(2, false)),
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
                FunctionArgument::Value {
                    value_type: FunctionValueType::try_with_logical_type(
                        DataType::LargeBinary,
                        false,
                        ValueLogicalType::Variant,
                    )
                    .unwrap(),
                    constant: None,
                },
                value_argument(
                    DataType::Utf8,
                    false,
                    Some(constant_binding_tests::utf8("$.x", false)),
                ),
                value_argument(
                    DataType::Utf8,
                    false,
                    Some(constant_binding_tests::utf8("BIGINT", false)),
                ),
            ],
        );
        assert_eq!(scalar_result(&variant).data_type, DataType::Int64);
    }

    #[test]
    fn rounding_catalogue_keeps_actual_source_domains_in_fresh_and_frozen_bindings() {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let encoded = DataType::Dictionary(
            Box::new(DataType::UInt8),
            Box::new(DataType::Decimal128(18, 6)),
        );
        // An encoded Decimal uses ROUND's cast path, not its outer Decimal
        // branch. TRUNCATE's NumericArrayView does not decode that carrier.
        let cases = [
            (DataType::UInt64, true, false, DataType::Int64),
            (DataType::Float16, true, false, DataType::Int64),
            (DataType::Boolean, true, false, DataType::Int64),
            (DataType::Utf8, true, false, DataType::Int64),
            (DataType::Decimal256(76, 6), true, false, DataType::Int64),
            (encoded, true, false, DataType::Int64),
            (DataType::Date32, false, false, DataType::Int64),
            (DataType::Float32, true, true, DataType::Int64),
            (DataType::Null, true, true, DataType::Int64),
            (
                DataType::Decimal128(18, 6),
                true,
                true,
                DataType::Decimal128(38, 6),
            ),
        ];
        for (source, round, truncate, expected) in cases {
            for (name, admitted) in [("round", round), ("truncate", truncate)] {
                let arguments = [value_argument(source.clone(), true, None)];
                let request = FunctionBindingRequest {
                    expected_result_type: None,
                    arguments: &arguments,
                    logical_argument_count: 1,
                };
                let result = catalog.resolve_bound_user(
                    name,
                    FunctionKind::Scalar,
                    request,
                    crate::binding_test_control(),
                );
                if !admitted {
                    assert!(result.is_err(), "{name} must reject {source:?}");
                    continue;
                }
                let binding = result.unwrap();
                assert_eq!(
                    scalar_result(&binding),
                    &FunctionValueType::new(expected.clone(), true)
                );
                assert_eq!(
                    binding.selected.argument_types.as_ref(),
                    &[FunctionArgumentType::Value(FunctionValueType::new(
                        source.clone(),
                        true,
                    ))]
                );
                catalog
                    .validate_bound(&binding, request, crate::binding_test_control())
                    .unwrap();
            }
        }
    }

    #[test]
    fn rounding_catalogue_has_one_result_author_and_refuses_forged_frozen_scale() {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        for name in ["round", "truncate"] {
            for (digits, scale) in [(2, 2), (-1, 0), (128, 0), (256, 0), (258, 2)] {
                let arguments = [
                    value_argument(DataType::Decimal128(18, 6), false, None),
                    value_argument(
                        DataType::Int64,
                        false,
                        Some(constant_binding_tests::i64(digits, false)),
                    ),
                ];
                let expected_override = FunctionValueType::new(DataType::Int32, false);
                let request = FunctionBindingRequest {
                    expected_result_type: Some(&expected_override),
                    arguments: &arguments,
                    logical_argument_count: 2,
                };
                let mut binding = catalog
                    .resolve_bound_user(
                        name,
                        FunctionKind::Scalar,
                        request,
                        crate::binding_test_control(),
                    )
                    .unwrap();
                assert_eq!(
                    scalar_result(&binding),
                    &FunctionValueType::new(DataType::Decimal128(38, scale), true)
                );
                catalog
                    .validate_bound(&binding, request, crate::binding_test_control())
                    .unwrap();
                binding.selected.result_type = FunctionResultType::Scalar(FunctionValueType::new(
                    DataType::Decimal128(38, 5),
                    true,
                ));
                assert!(
                    catalog
                        .validate_bound(&binding, request, crate::binding_test_control())
                        .is_err(),
                    "{name} must refuse a foreign result scale"
                );
            }
        }
    }

    #[test]
    fn rounding_catalogue_checks_shared_carrier_grammar_before_cast_authoring() {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let field = |ty, nullable| Arc::new(arrow_schema::Field::new("v", ty, nullable));
        let invalid_sources = [
            DataType::Decimal128(0, 0),
            DataType::Decimal256(77, 0),
            DataType::Dictionary(Box::new(DataType::Float64), Box::new(DataType::Float64)),
            DataType::RunEndEncoded(field(DataType::Int32, true), field(DataType::Float64, true)),
            DataType::Union(
                [
                    (7, field(DataType::Float64, true)),
                    (7, field(DataType::Utf8, true)),
                ]
                .into_iter()
                .collect(),
                arrow_schema::UnionMode::Dense,
            ),
            // Even an unselected child is required to be a valid carrier.
            DataType::Union(
                [
                    (7, field(DataType::Float64, true)),
                    (1, field(DataType::Decimal128(0, 0), true)),
                ]
                .into_iter()
                .collect(),
                arrow_schema::UnionMode::Dense,
            ),
        ];
        for source in invalid_sources {
            let arguments = [value_argument(source, true, None)];
            let result = catalog.resolve_bound_user(
                "round",
                FunctionKind::Scalar,
                FunctionBindingRequest {
                    expected_result_type: None,
                    arguments: &arguments,
                    logical_argument_count: 1,
                },
                crate::binding_test_control(),
            );
            assert!(matches!(
                result,
                Err(FunctionBindingError::InvalidBinding(_))
            ));
        }
    }

    #[test]
    fn exact_table_binding_freezes_unnest_relation_columns() {
        let catalog = build_builtin_engine_function_catalog().expect("builtin catalog");
        let list = DataType::List(Arc::new(arrow_schema::Field::new(
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
                    expected_result_type: None,
                    arguments: &arguments,
                    logical_argument_count: 1,
                },
                crate::binding_test_control(),
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
                    expected_result_type: None,
                    arguments: &arguments,
                    logical_argument_count: 1,
                },
                crate::binding_test_control(),
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
        let resolved = resolve_bound_aggregate(
            &catalog,
            "count",
            &[],
            &[],
            false,
            crate::binding_test_control(),
        )
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
                crate::binding_test_control()
            ),
            Err(FunctionResolutionError::NoMatchingSignature { .. })
        ));
        assert_eq!(
            resolve_bound_aggregate(
                &catalog,
                "not_an_aggregate",
                &[],
                &[],
                false,
                crate::binding_test_control()
            ),
            Err(FunctionResolutionError::UnknownFunction)
        );

        let std_user = resolve_bound_aggregate(
            &catalog,
            "std",
            &[DataType::Int64],
            &[DataType::Int64],
            false,
            crate::binding_test_control(),
        )
        .expect("std alias resolves for user SQL");
        let std_trusted = resolve_bound_aggregate(
            &catalog,
            "std",
            &[DataType::Int64],
            &[DataType::Int64],
            true,
            crate::binding_test_control(),
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
                crate::binding_test_control()
            ),
            Err(FunctionResolutionError::NoMatchingSignature { .. })
        ));

        let resolved = resolve_bound_aggregate(
            &catalog,
            "array_agg",
            &[DataType::Utf8],
            &[DataType::Utf8, DataType::Int64],
            false,
            crate::binding_test_control(),
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
                crate::binding_test_control()
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
            crate::binding_test_control(),
        )
        .expect("multi-column distinct count resolves");
        assert_eq!(
            distinct.argument_types,
            [DataType::Int64, DataType::Utf8, DataType::Boolean]
        );
        assert!(matches!(
            resolve_bound_aggregate(
                &catalog,
                "multi_distinct_count",
                &[],
                &[],
                false,
                crate::binding_test_control()
            ),
            Err(FunctionResolutionError::NoMatchingSignature { .. })
        ));

        let dict = resolve_bound_aggregate(
            &catalog,
            "dict_merge",
            &[DataType::Utf8, DataType::Int64],
            &[DataType::Utf8, DataType::Int64],
            false,
            crate::binding_test_control(),
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
        let list_utf8 = DataType::List(Arc::new(arrow_schema::Field::new(
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
                crate::binding_test_control(),
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
                crate::binding_test_control()
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
                crate::binding_test_control()
            ),
            Err(FunctionResolutionError::NoMatchingSignature { .. })
        ));
        let unsupported_list = DataType::List(Arc::new(arrow_schema::Field::new(
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
                crate::binding_test_control()
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
                let source_type = if input == DataType::FixedSizeBinary(16) {
                    FunctionValueType::try_with_logical_type(
                        input.clone(),
                        nullable,
                        ValueLogicalType::LargeInt,
                    )
                    .unwrap()
                } else {
                    FunctionValueType::new(input.clone(), nullable)
                };
                let arguments = [FunctionArgument::Value {
                    value_type: source_type.clone(),
                    constant: None,
                }];
                let binding = resolve_exact_scalar(&catalog, "abs", &arguments);
                assert_eq!(scalar_result(&binding).data_type, output);
                assert_eq!(scalar_result(&binding).nullable, nullable);
                assert_eq!(
                    binding.selected.argument_types.as_ref(),
                    &[FunctionArgumentType::Value(source_type)]
                );
                catalog
                    .validate_bound(
                        &binding,
                        FunctionBindingRequest {
                            expected_result_type: None,
                            arguments: &arguments,
                            logical_argument_count: 1,
                        },
                        crate::binding_test_control(),
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
                            expected_result_type: None,
                            arguments: &arguments,
                            logical_argument_count: 1,
                        },
                        crate::binding_test_control()
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
                    value_argument(DataType::Int64, false, None),
                    value_argument(DataType::Int64, false, None),
                    value_argument(DataType::Int64, false, None),
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
                        expected_result_type: None,
                        arguments: &arguments,
                        logical_argument_count: arguments.len(),
                    },
                    crate::binding_test_control(),
                )
                .unwrap();
        }
    }

    #[test]
    fn variadic_typed_encoders_close_unsupported_shape_before_optimization() {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        for name in ["mv_group_row_id", "encode_sort_key"] {
            let arguments = [value_argument(
                DataType::List(Arc::new(arrow_schema::Field::new(
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
                            expected_result_type: None,
                            arguments: &arguments,
                            logical_argument_count: 1
                        },
                        crate::binding_test_control()
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
            DataType::List(Arc::new(arrow_schema::Field::new(
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
    fn fingerprint_alias_preserves_ignored_container_selected_profiles() {
        use arrow_schema::{Field, Fields};
        use novarocks_type_contract::FunctionIntrinsicRowError;
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let element = Arc::new(
            Field::new("element", DataType::Int32, true).with_metadata(
                [("PARQUET:field_id".to_string(), "5".to_string())]
                    .into_iter()
                    .collect(),
            ),
        );
        let fields: Fields = vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", DataType::Utf8, true)),
        ]
        .into();
        for ignored in [
            DataType::List(element),
            DataType::Struct(fields.clone()),
            DataType::Map(
                Arc::new(Field::new("entries", DataType::Struct(fields), false)),
                false,
            ),
        ] {
            let arguments = [
                value_argument(DataType::Int64, false, None),
                value_argument(DataType::Utf8, true, None),
                value_argument(ignored, true, None),
            ];
            for name in ["encode_row_id", "encode_fingerprint_sha256"] {
                let bound = resolve_exact_scalar(&catalog, name, &arguments);
                assert_eq!(
                    bound.selected.argument_types.as_ref(),
                    arguments
                        .iter()
                        .map(FunctionArgument::argument_type)
                        .collect::<Vec<_>>()
                );
                assert_eq!(scalar_result(&bound).data_type, DataType::Binary);
                assert_eq!(
                    bound.semantics.intrinsic_row_error,
                    FunctionIntrinsicRowError::NoRowError
                );
                catalog
                    .validate_bound(
                        &bound,
                        FunctionBindingRequest {
                            expected_result_type: None,
                            arguments: &arguments,
                            logical_argument_count: arguments.len(),
                        },
                        crate::binding_test_control(),
                    )
                    .unwrap();
            }
            assert!(
                catalog
                    .resolve_bound_user(
                        "encode_sort_key",
                        FunctionKind::Scalar,
                        FunctionBindingRequest {
                            expected_result_type: None,
                            arguments: &arguments,
                            logical_argument_count: arguments.len()
                        },
                        crate::binding_test_control()
                    )
                    .is_err()
            );
        }
    }

    #[test]
    fn field_exact_binding_rejects_containers_and_preserves_comparable_profiles() {
        use arrow_schema::{Field, Fields, TimeUnit};
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
                            expected_result_type: None,
                            arguments: &arguments,
                            logical_argument_count: 2
                        },
                        crate::binding_test_control()
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
                        expected_result_type: None,
                        arguments: &arguments,
                        logical_argument_count: 2,
                    },
                    crate::binding_test_control(),
                )
                .unwrap();
        }
    }
    #[test]
    fn array_ordering_binding_closes_comparator_domain_and_preserves_sortby_values() {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let list = |ty| DataType::List(Arc::new(arrow_schema::Field::new("item", ty, true)));
        for ty in [
            list(DataType::Int64),
            DataType::Map(
                Arc::new(arrow_schema::Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(arrow_schema::Field::new("key", DataType::Int64, true)),
                            Arc::new(arrow_schema::Field::new("value", DataType::Int64, true)),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            DataType::Struct(
                vec![Arc::new(arrow_schema::Field::new(
                    "x",
                    DataType::Int64,
                    true,
                ))]
                .into(),
            ),
            DataType::Decimal256(60, 2),
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
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
                                expected_result_type: None,
                                arguments: &arguments,
                                logical_argument_count: arguments.len()
                            },
                            crate::binding_test_control()
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
                            expected_result_type: None,
                            arguments: &arguments,
                            logical_argument_count: 2
                        },
                        crate::binding_test_control()
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
                        expected_result_type: None,
                        arguments: &arguments,
                        logical_argument_count: 1,
                    },
                    crate::binding_test_control(),
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
                    expected_result_type: None,
                    arguments: &arguments,
                    logical_argument_count: 2,
                },
                crate::binding_test_control(),
            )
            .unwrap();
    }

    #[test]
    fn object_value_binding_rejects_unimplemented_carriers_and_missing_inputs() {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let list = DataType::List(Arc::new(arrow_schema::Field::new(
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
                                expected_result_type: None,
                                arguments: &arguments,
                                logical_argument_count: arguments.len()
                            },
                            crate::binding_test_control()
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
                        expected_result_type: None,
                        arguments: &arguments,
                        logical_argument_count: 1,
                    },
                    crate::binding_test_control(),
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
                        expected_result_type: None,
                        arguments: &arguments,
                        logical_argument_count: 2,
                    },
                    crate::binding_test_control(),
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
                        expected_result_type: None,
                        arguments: &[],
                        logical_argument_count: 0,
                    },
                    crate::binding_test_control(),
                )
                .unwrap();
        }
    }

    #[test]
    fn dynamic_array_numeric_binding_closes_the_installed_output_domain() {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let list = |ty| DataType::List(Arc::new(arrow_schema::Field::new("item", ty, true)));
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
                                expected_result_type: None,
                                arguments: &arguments,
                                logical_argument_count: 1
                            },
                            crate::binding_test_control()
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
                            expected_result_type: None,
                            arguments: &arguments,
                            logical_argument_count: 1,
                        },
                        crate::binding_test_control(),
                    )
                    .unwrap();
            }
        }
    }
    #[test]
    fn selected_collection_shapes_preserve_recursive_equality_and_masks() {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let list = |ty| DataType::List(Arc::new(arrow_schema::Field::new("item", ty, true)));
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
                            expected_result_type: None,
                            arguments: &arguments,
                            logical_argument_count: arguments.len()
                        },
                        crate::binding_test_control()
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
                            expected_result_type: None,
                            arguments: &arguments,
                            logical_argument_count: arguments.len()
                        },
                        crate::binding_test_control()
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
                        expected_result_type: None,
                        arguments: &arguments,
                        logical_argument_count: arguments.len(),
                    },
                    crate::binding_test_control(),
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
                        expected_result_type: None,
                        arguments: &arguments,
                        logical_argument_count: 2,
                    },
                    crate::binding_test_control(),
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
                            expected_result_type: None,
                            arguments: &arguments,
                            logical_argument_count: arguments.len()
                        },
                        crate::binding_test_control()
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
        let list = |ty| DataType::List(Arc::new(arrow_schema::Field::new("item", ty, true)));
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
                            expected_result_type: None,
                            arguments: &arguments,
                            logical_argument_count: 2
                        },
                        crate::binding_test_control()
                    )
                    .is_err(),
                "{name}"
            );
        }
    }
    #[test]
    fn selected_array_ordering_retains_null_only_and_empty_profiles() {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let list = |ty| DataType::List(Arc::new(arrow_schema::Field::new("item", ty, true)));
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
                        expected_result_type: None,
                        arguments: &arguments,
                        logical_argument_count: arguments.len(),
                    },
                    crate::binding_test_control(),
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
                    expected_result_type: None,
                    arguments: &arguments,
                    logical_argument_count: 2,
                },
                crate::binding_test_control(),
            )
            .unwrap();
        assert_eq!(scalar_result(&bound).data_type, list(list(DataType::Int64)));
    }
}

#[cfg(test)]
mod logical_argument_identity_tests {
    use super::same_argument_up_to_nested_nullability;
    use crate::{FunctionArgumentType, FunctionValueType};
    use arrow_schema::{DataType, Field};
    use novarocks_type_contract::{NR_LOGICAL_TYPE_KEY, ValueLogicalType};
    use std::sync::Arc;

    fn value(ty: FunctionValueType) -> FunctionArgumentType {
        FunctionArgumentType::Value(ty)
    }
    fn list(nullable: bool, logical: Option<&str>, decorated: bool) -> FunctionArgumentType {
        let mut metadata = std::collections::HashMap::new();
        if let Some(logical) = logical {
            metadata.insert(NR_LOGICAL_TYPE_KEY.to_string(), logical.to_string());
        }
        if decorated {
            metadata.insert("iceberg.field.id".to_string(), "27".to_string());
        }
        value(FunctionValueType::new(
            DataType::List(Arc::new(
                Field::new("item", DataType::Utf8, nullable).with_metadata(metadata),
            )),
            false,
        ))
    }
    #[test]
    fn aggregate_argument_nullability_allowance_keeps_root_logical_identity() {
        for (carrier, logical) in [
            (DataType::Utf8, ValueLogicalType::Json),
            (DataType::LargeBinary, ValueLogicalType::Variant),
            (DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt),
            (DataType::Binary, ValueLogicalType::Hll),
        ] {
            let physical = value(FunctionValueType::new(carrier.clone(), false));
            let semantic =
                value(FunctionValueType::try_with_logical_type(carrier, false, logical).unwrap());
            assert!(!same_argument_up_to_nested_nullability((
                &physical, &semantic
            )));
            assert!(!same_argument_up_to_nested_nullability((
                &semantic, &physical
            )));
            assert!(same_argument_up_to_nested_nullability((
                &semantic, &semantic
            )));
        }
        let integer = value(
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                false,
                ValueLogicalType::LargeInt,
            )
            .unwrap(),
        );
        let uuid = value(
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                false,
                ValueLogicalType::Uuid,
            )
            .unwrap(),
        );
        assert!(!same_argument_up_to_nested_nullability((&integer, &uuid)));
    }
    #[test]
    fn aggregate_argument_nullability_allowance_keeps_nested_json_and_provider_rules() {
        let json = list(false, Some("json"), false);
        let nullable_json = list(true, Some("json"), true);
        let physical = list(true, None, false);
        assert!(same_argument_up_to_nested_nullability((
            &json,
            &nullable_json
        )));
        assert!(same_argument_up_to_nested_nullability((
            &nullable_json,
            &json
        )));
        assert!(!same_argument_up_to_nested_nullability((&json, &physical)));
        assert!(!same_argument_up_to_nested_nullability((&physical, &json)));
        let unknown = list(false, Some("not_a_declared_logical_type"), false);
        assert!(!same_argument_up_to_nested_nullability((
            &unknown, &unknown
        )));
    }
}

#[cfg(test)]
#[path = "value_binding_tests.rs"]
mod value_binding_tests;

fn nullable_arguments(
    indices: impl Iterator<Item = usize>,
    all: bool,
    nullable: &impl Fn(usize) -> bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, FunctionBindingError> {
    for index in indices {
        work.step()?;
        let value = nullable(index);
        if value != all {
            return Ok(value);
        }
    }
    Ok(all)
}

fn dynamic_values<'a>(
    request: FunctionBindingRequest<'a>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<&'a FunctionValueType>, FunctionBindingError> {
    let mut result = Vec::with_capacity(request.arguments.len());
    for argument in request.arguments {
        work.step()?;
        match argument {
            FunctionArgument::Value { value_type, .. } => result.push(value_type),
            _ => return Err(FunctionBindingError::NoMatchingOverload),
        }
    }
    Ok(result)
}

fn nested_arguments_match(
    left: &[FunctionArgumentType],
    right: &[FunctionArgumentType],
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, FunctionBindingError> {
    for pair in left.iter().zip(right) {
        work.step()?;
        if !same_argument_up_to_nested_nullability(pair) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn observed_all<T>(
    values: impl Iterator<Item = T>,
    mut test: impl FnMut(T) -> bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, FunctionBindingError> {
    for value in values {
        work.step()?;
        if !test(value) {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
#[path = "constant_binding_tests.rs"]
pub(super) mod constant_binding_tests;

/// Construct the actual private DS scalar definition for cross-crate runtime
/// tests before public catalogue registration. No production caller enables
/// this feature; binding, metadata and installed declarations keep one author.
#[cfg(feature = "test-support")]
pub fn ds_hll_scalar_private_test_catalog() -> EngineFunctionCatalog {
    let original = build_builtin_engine_function_catalog().unwrap();
    let (name, signatures) = registry::builtin_scalar_declarations()
        .into_iter()
        .find(|(name, _)| name == "ds_hll_count_distinct_state")
        .expect("original DS scalar declaration");
    let (declaration, resolver) =
        scalar_definition_parts(&name, &signatures, FunctionKind::Scalar).unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(super::ds_hll_state_owner::definition(&name, declaration, resolver).unwrap())
        .unwrap();
    // The compact child fixture also borrows the actual installed IF owner.
    builder
        .register(
            original
                .definition("if", FunctionKind::Scalar)
                .unwrap()
                .clone(),
        )
        .unwrap();
    builder.seal().unwrap()
}

#[cfg(test)]
pub(super) fn ndv_invocation_data_test_catalog() -> EngineFunctionCatalog {
    let original = build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    for declaration in builtin_aggregate_declarations()
        .into_iter()
        .filter(|item| matches!(item.name, "ndv" | "approx_count_distinct"))
    {
        let raw = original
            .definition(declaration.name, FunctionKind::Aggregate)
            .unwrap()
            .binding_declaration()
            .unwrap();
        let binding = FunctionBindingDeclaration::try_new(
            raw.function_id().clone(),
            raw.kind(),
            raw.overloads().iter().cloned().map(|mut overload| {
                overload.effects = Some(super::aggregate_hll_owner::effects());
                overload
            }),
        )
        .unwrap();
        let resolver = Arc::new(BuiltinAggregateResolver { declaration });
        builder
            .register(
                super::aggregate_hll_owner::definition(declaration.name, binding, resolver)
                    .unwrap(),
            )
            .unwrap();
    }
    builder.seal().unwrap()
}

#[cfg(test)]
pub(super) fn ds_hll_host_test_catalog() -> EngineFunctionCatalog {
    let original = build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    for declaration in builtin_aggregate_declarations().into_iter().filter(|item| {
        matches!(
            item.name,
            "ds_hll_count_distinct"
                | "approx_count_distinct_hll_sketch"
                | "ds_hll_count_distinct_merge"
                | "ds_hll_count_distinct_union"
        )
    }) {
        let raw = original
            .definition(declaration.name, FunctionKind::Aggregate)
            .unwrap()
            .binding_declaration()
            .unwrap();
        let binding = FunctionBindingDeclaration::try_new(
            raw.function_id().clone(),
            raw.kind(),
            raw.overloads().iter().cloned().map(|mut overload| {
                overload.effects = Some(super::aggregate_ds_hll_owner::effects());
                overload
            }),
        )
        .unwrap();
        builder
            .register(
                super::aggregate_ds_hll_owner::definition(
                    declaration.name,
                    binding,
                    Arc::new(BuiltinAggregateResolver { declaration }),
                )
                .unwrap(),
            )
            .unwrap();
    }
    builder.seal().unwrap()
}

#[cfg(test)]
pub(super) fn bitmap_union_int_test_catalog() -> EngineFunctionCatalog {
    let original = build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    for declaration in builtin_aggregate_declarations()
        .into_iter()
        .filter(|item| matches!(item.name, "bitmap_union_int" | "bitmap_agg"))
    {
        let raw = original
            .definition(declaration.name, FunctionKind::Aggregate)
            .unwrap()
            .binding_declaration()
            .unwrap();
        let binding = FunctionBindingDeclaration::try_new(
            raw.function_id().clone(),
            raw.kind(),
            raw.overloads().iter().cloned().map(|mut overload| {
                overload.effects = Some(super::aggregate_bitmap_union_int_owner::effects());
                overload
            }),
        )
        .unwrap();
        let resolver = Arc::new(BuiltinAggregateResolver { declaration });
        builder
            .register(
                super::aggregate_bitmap_union_int_owner::definition(
                    declaration.name,
                    binding,
                    resolver,
                )
                .unwrap(),
            )
            .unwrap();
    }
    builder.seal().unwrap()
}

#[cfg(test)]
pub(super) fn hll_payload_aggregate_test_catalog() -> EngineFunctionCatalog {
    let original = build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    for declaration in builtin_aggregate_declarations()
        .into_iter()
        .filter(|item| matches!(item.name, "hll_union" | "hll_raw_agg" | "hll_union_agg"))
    {
        let raw = original
            .definition(declaration.name, FunctionKind::Aggregate)
            .unwrap()
            .binding_declaration()
            .unwrap();
        let binding = FunctionBindingDeclaration::try_new(
            raw.function_id().clone(),
            raw.kind(),
            raw.overloads().iter().cloned().map(|mut overload| {
                overload.effects = Some(super::aggregate_hll_payload_owner::effects());
                overload
            }),
        )
        .unwrap();
        let resolver = Arc::new(BuiltinAggregateResolver { declaration });
        builder
            .register(
                super::aggregate_hll_payload_owner::definition(declaration.name, binding, resolver)
                    .unwrap(),
            )
            .unwrap();
    }
    builder.seal().unwrap()
}

// Test-only attachment to the original immutable binding and resolver authors.
#[cfg(test)]
pub(super) fn map_agg_private_catalog_for_test() -> EngineFunctionCatalog {
    let native = build_builtin_engine_function_catalog().unwrap();
    let original = native
        .definition("map_agg", FunctionKind::Aggregate)
        .unwrap()
        .binding_declaration()
        .unwrap();
    let declaration = FunctionBindingDeclaration::try_new(
        original.function_id().clone(),
        original.kind(),
        original.overloads().iter().cloned().map(|mut overload| {
            overload.effects = Some(super::aggregate_map_owner::effects());
            overload
        }),
    )
    .unwrap();
    let declaration_source = builtin_aggregate_declarations()
        .into_iter()
        .find(|declaration| declaration.name == "map_agg")
        .unwrap();
    let resolver = Arc::new(BuiltinAggregateResolver {
        declaration: declaration_source,
    });
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(super::aggregate_map_owner::definition("map_agg", declaration, resolver).unwrap())
        .unwrap();
    builder.seal_bound().unwrap()
}

/// Actual complete original declaration and original binding/metadata authors.
/// This test-only factory enables no public registration or binding fallback.
#[cfg(feature = "test-support")]
pub fn percentile_raw_private_test_catalog() -> EngineFunctionCatalog {
    let (name, signatures) = registry::builtin_scalar_declarations()
        .into_iter()
        .find(|(name, _)| name == "percentile_approx_raw")
        .expect("original ANY2 declaration");
    let (declaration, resolver) =
        scalar_definition_parts(&name, &signatures, FunctionKind::Scalar).unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            super::percentile_approx_raw_owner::definition(&name, declaration, resolver).unwrap(),
        )
        .unwrap();
    builder.seal().unwrap()
}

/// Private runtime test catalogue. Public production BY definitions retain
/// AggregateV1 until the complete original invocation schedule is validated.
#[cfg(any(test, feature = "test-support"))]
pub fn by_window_private_test_catalog() -> EngineFunctionCatalog {
    let original = build_builtin_engine_function_catalog().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    for declaration in builtin_aggregate_declarations()
        .into_iter()
        .filter(|item| matches!(item.name, "max_by" | "min_by"))
    {
        let binding = original
            .definition(declaration.name, FunctionKind::Aggregate)
            .unwrap()
            .binding_declaration()
            .unwrap()
            .clone();
        let name = declaration.name;
        let resolver = Arc::new(BuiltinAggregateResolver { declaration });
        builder
            .register(
                super::aggregate_by_owner::private_window_definition_for_test(
                    name, binding, resolver,
                )
                .unwrap(),
            )
            .unwrap();
    }
    builder.seal().unwrap()
}

/// Private Union capability for original full-domain binding/runtime probes.
/// This attachment enables no public production registration.
#[cfg(any(test, feature = "test-support"))]
pub fn percentile_union_private_test_catalog() -> EngineFunctionCatalog {
    let original = build_builtin_engine_function_catalog().unwrap();
    let raw = original
        .definition("percentile_union", FunctionKind::Aggregate)
        .unwrap()
        .binding_declaration()
        .unwrap();
    let binding = FunctionBindingDeclaration::try_new(
        raw.function_id().clone(),
        raw.kind(),
        raw.overloads().iter().cloned().map(|mut overload| {
            overload.effects = Some(super::aggregate_approx_percentile_owner::effects());
            overload
        }),
    )
    .unwrap();
    let declaration = builtin_aggregate_declarations()
        .into_iter()
        .find(|item| item.name == "percentile_union")
        .unwrap();
    let resolver = Arc::new(BuiltinAggregateResolver { declaration });
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            super::aggregate_approx_percentile_owner::definition(
                "percentile_union",
                binding,
                resolver,
            )
            .unwrap(),
        )
        .unwrap();
    builder.seal().unwrap()
}

/// Cross-crate transport probes borrow the REAL ARRAY dynamic declaration and
/// resolver. This private catalogue is not a public supported capability.
#[cfg(feature = "test-support")]
pub fn array_projection_diagnostic_private_test_catalog() -> EngineFunctionCatalog {
    let (raw, resolver) = dynamic_definition_parts("__array_struct_subfield").unwrap();
    let declaration = FunctionBindingDeclaration::try_new(
        raw.function_id().clone(),
        raw.kind(),
        raw.overloads().iter().cloned().map(|mut overload| {
            overload.effects = Some(super::array_invocation_diagnostic_probe_owner::effects());
            overload
        }),
    )
    .unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            super::array_invocation_diagnostic_probe_owner::definition(
                "__array_struct_subfield",
                declaration,
                resolver,
            )
            .unwrap(),
        )
        .unwrap();
    let original = build_builtin_engine_function_catalog().unwrap();
    builder
        .register(
            original
                .definition("if", FunctionKind::Scalar)
                .unwrap()
                .clone(),
        )
        .unwrap();
    builder.seal().unwrap()
}
