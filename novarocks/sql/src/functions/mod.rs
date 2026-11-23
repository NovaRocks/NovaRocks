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

//! SQL application adapter for the Functions-owned builtin catalogue.
//!
//! Syntax admission, analyzer argument extraction and the SQL catalogue trait
//! remain here. Declarations, overload selection and result type policies live
//! in `novarocks_functions::builtin`; this adapter owns no second resolver.

pub(crate) use novarocks_functions::builtin::intrinsic::{BuiltinDisposition, builtin_disposition};
pub(crate) use novarocks_functions::builtin::resolver;
#[cfg(test)]
#[path = "intrinsic.rs"]
mod intrinsic_integration_tests;
#[cfg(test)]
mod selected_preparation_tests;
pub use novarocks_functions::builtin::catalogue::{
    build_builtin_engine_function_catalog, builtin_engine_function_catalog,
    contribute_builtin_functions,
};
pub(crate) use novarocks_functions::builtin::catalogue::{
    builtin_function_volatility, resolve_bound_aggregate,
};

use std::sync::Arc;

use arrow::datatypes::DataType;
#[cfg(test)]
use novarocks_functions::{
    AggregateBindingDeclaration, AggregateOverloadMetadata, EngineFunctionCatalogBuilder,
    FunctionArgumentEvaluation, FunctionBindingDeclaration, FunctionBindingResolver,
    FunctionBindingSelection, FunctionFailureBehavior, FunctionId, FunctionOverloadDeclaration,
    FunctionOverloadId, FunctionSemantics,
};
use novarocks_functions::{
    EngineFunctionCatalog, FunctionArgument, FunctionArgumentType, FunctionBindingError,
    FunctionBindingRequest, FunctionDefinition, FunctionKind, FunctionResolutionError,
    FunctionResultType, FunctionValueType, FunctionVisibility, ResolvedAggregateSignature,
    ResolvedFunctionBinding, aggregate_types::DistinctAggregate,
};
use novarocks_type_contract::{ValueKind, trino_can_coerce_scalar};

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

#[cfg(test)]
fn declared_output_logical_type(
    result: &FunctionValueType,
) -> Option<novarocks_types::schema::SqlType> {
    use novarocks_type_contract::ValueLogicalType as V;
    use novarocks_types::schema::SqlType as S;
    match result.logical_type() {
        V::Object => Some(S::Object),
        V::Percentile => Some(S::Percentile),
        V::Uuid => Some(S::Uuid),
        domain => crate::analyzer::sql_logical_projection(domain),
    }
}

#[cfg(test)]
pub(crate) fn scalar_output_logical_type(
    binding: &ResolvedFunctionBinding,
) -> Option<novarocks_types::schema::SqlType> {
    if binding.kind != FunctionKind::Scalar {
        return None;
    }
    let FunctionResultType::Scalar(result) = &binding.selected.result_type else {
        return None;
    };
    declared_output_logical_type(result)
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

    fn scalar_time_dependencies(
        &self,
        binding: &ResolvedFunctionBinding,
    ) -> &[novarocks_type_contract::SemanticParameterKey] {
        self.definition_by_id(&binding.function_id)
            .and_then(|definition| definition.binding_declaration())
            .and_then(|declaration| {
                declaration
                    .effect_declaration(&binding.selected.overload)
                    .ok()
            })
            .map_or(&[], |effects| effects.environment_dependencies.as_ref())
    }

    fn select_exact_overload_observed(
        &self,
        function: &novarocks_functions::FunctionId,
        kind: novarocks_functions::FunctionKind,
        overload: &novarocks_functions::FunctionOverloadId,
        request: novarocks_functions::FunctionBindingRequest<'_>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        Arc<novarocks_functions::FunctionBindingSelection>,
        novarocks_functions::FunctionBindingError,
    > {
        EngineFunctionCatalog::select_exact_overload_observed(
            self, function, kind, overload, request, control,
        )
    }

    fn pure_overload_declaration_observed<'a>(
        &'a self,
        function_id: &novarocks_functions::FunctionId,
        kind: novarocks_functions::FunctionKind,
        overload: &novarocks_functions::FunctionOverloadId,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::PureOverloadDeclaration<'a>,
        novarocks_functions::FunctionSpecializationFailure,
    > {
        EngineFunctionCatalog::pure_overload_declaration_observed(
            self,
            function_id,
            kind,
            overload,
            control,
        )
    }

    fn prepare_fresh_selected(
        &self,
        input: novarocks_functions::CallEffectInput<'_>,
        selected: Arc<novarocks_functions::FunctionBindingSelection>,
        options: novarocks_functions::PureCallPreparation,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::PureCallSpecialization,
        novarocks_functions::FunctionSpecializationFailure,
    > {
        EngineFunctionCatalog::prepare_fresh_selected(self, input, selected, options, control)
    }

    fn resolve_scalar_signature(
        &self,
        name: &str,
        arg_types: &[DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedScalarFunction, ResolveError> {
        let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
            control,
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
        )?;
        let result = (|| {
            if arg_types.len() > novarocks_functions::MAX_CALL_EFFECT_ARGUMENTS {
                return Err(novarocks_type_contract::CompileControlError::ResourceExhausted.into());
            }
            let mut arguments = Vec::with_capacity(arg_types.len());
            for data_type in arg_types {
                work.step()?;
                arguments.push(FunctionArgument::Value {
                    value_type: FunctionValueType::new(data_type.clone(), true),
                    constant: None,
                });
            }
            work.flush()?;
            let bound = self
                .resolve_bound_user(
                    name,
                    FunctionKind::Scalar,
                    FunctionBindingRequest {
                        expected_result_type: None,
                        arguments: &arguments,
                        logical_argument_count: arguments.len(),
                    },
                    control,
                )
                .map_err(|error| match error {
                    FunctionBindingError::Control(error) => ResolveError::Control(error),
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
                .iter()
                .map(|argument| {
                    work.step()?;
                    match argument {
                        FunctionArgumentType::Value(value) => Ok(value.data_type().clone()),
                        FunctionArgumentType::Lambda { .. } => Err(ResolveError::BadSignature(
                            "legacy scalar signature cannot represent a lambda argument".into(),
                        )),
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(ResolvedScalarFunction {
                return_type: result.data_type().clone(),
                argument_types,
                enforce_argument_binding: true,
            })
        })();
        if matches!(result, Err(ResolveError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }

    fn recognizes_scalar_name(
        &self,
        name: &str,
        _arg_types: &[DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<bool, ResolveError> {
        let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
            control,
            novarocks_type_contract::CompilePhase::FunctionSpecialization,
        )?;
        let known = self.definition(name, FunctionKind::Scalar).is_some();
        work.step()?;
        work.finish()?;
        Ok(known)
    }

    fn resolve_scalar_binding(
        &self,
        name: &str,
        arguments: &[FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_user(
            name,
            FunctionKind::Scalar,
            FunctionBindingRequest {
                expected_result_type: None,
                arguments,
                logical_argument_count: arguments.len(),
            },
            control,
        )
    }

    fn resolve_scalar_operator_binding(
        &self,
        operator: novarocks_type_contract::OperatorFunction,
        request: FunctionBindingRequest<'_>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_trusted_by_id(
            &operator.function_id(),
            FunctionKind::Scalar,
            request,
            control,
        )
    }

    fn finish_scalar_binding(
        &self,
        function: &novarocks_functions::FunctionId,
        overload: &novarocks_functions::FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_trusted_at_overload(
            function,
            FunctionKind::Scalar,
            overload,
            request,
            control,
        )
    }

    fn resolve_scalar_binding_with_expected_result(
        &self,
        name: &str,
        arguments: &[FunctionArgument],
        expected: &FunctionValueType,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_user(
            name,
            FunctionKind::Scalar,
            FunctionBindingRequest {
                arguments,
                logical_argument_count: arguments.len(),
                expected_result_type: Some(expected),
            },
            control,
        )
    }

    fn resolve_array_common_coercion_binding(
        &self,
        argument: &FunctionArgument,
        target: &FunctionValueType,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        novarocks_functions::builtin::value_conversion::resolve_array_common_coercion_binding(
            self, argument, target, control,
        )
    }

    fn resolve_value_conversion_binding(
        &self,
        argument: &FunctionArgument,
        target: &FunctionValueType,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_trusted(
            novarocks_functions::builtin::value_conversion::VALUE_CONVERSION_NAME,
            FunctionKind::Scalar,
            FunctionBindingRequest {
                arguments: std::slice::from_ref(argument),
                logical_argument_count: 1,
                expected_result_type: Some(target),
            },
            control,
        )
    }

    fn resolve_like_with_escape_binding(
        &self,
        arguments: &[FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_trusted(
            novarocks_functions::native_like::ESCAPE_FUNCTION_NAME,
            FunctionKind::Scalar,
            FunctionBindingRequest {
                arguments,
                logical_argument_count: arguments.len(),
                expected_result_type: None,
            },
            control,
        )
    }

    fn resolve_json_canonical_key_binding(
        &self,
        argument: &FunctionArgument,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_trusted(
            novarocks_functions::json_canonical::FUNCTION_NAME,
            FunctionKind::Scalar,
            FunctionBindingRequest {
                arguments: std::slice::from_ref(argument),
                logical_argument_count: 1,
                expected_result_type: None,
            },
            control,
        )
    }

    fn resolve_window_binding(
        &self,
        name: &str,
        arguments: &[FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_user(
            name,
            FunctionKind::Window,
            FunctionBindingRequest {
                expected_result_type: None,
                arguments,
                logical_argument_count: arguments.len(),
            },
            control,
        )
    }

    fn resolve_table_binding(
        &self,
        name: &str,
        arguments: &[FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_user(
            name,
            FunctionKind::Table,
            FunctionBindingRequest {
                expected_result_type: None,
                arguments,
                logical_argument_count: arguments.len(),
            },
            control,
        )
    }

    fn contains_aggregate(&self, name: &str) -> bool {
        self.definition(name, FunctionKind::Aggregate)
            .is_some_and(|definition| definition.visibility() == FunctionVisibility::Public)
    }

    fn resolve_distinct_aggregate(
        &self,
        aggregate: DistinctAggregate,
        request: FunctionBindingRequest<'_>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        if matches!(aggregate, DistinctAggregate::Sum | DistinctAggregate::Avg) {
            require_sql_aggregate_input(request.arguments, DataType::Float64)?;
        }
        EngineFunctionCatalog::resolve_distinct_aggregate(self, aggregate, request, control)
    }

    fn resolve_aggregate_binding(
        &self,
        name: &str,
        logical_argument_count: usize,
        arguments: &[FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        if let Some(declaration) = self
            .definition(name, FunctionKind::Aggregate)
            .and_then(FunctionDefinition::binding_declaration)
        {
            let domain = match declaration.function_id().as_str() {
                "builtin.aggregate/sum/v1" | "builtin.aggregate/avg/v1" => Some(DataType::Float64),
                "builtin.aggregate/bool_or/v1"
                | "builtin.aggregate/bool_and/v1"
                | "builtin.aggregate/boolor_agg/v1"
                | "builtin.aggregate/booland_agg/v1"
                | "builtin.aggregate/every/v1"
                | "builtin.aggregate/count_if/v1" => Some(DataType::Boolean),
                _ => None,
            };
            if let Some(domain) = domain {
                require_sql_aggregate_input(arguments, domain)?;
            }
        }
        self.resolve_bound_user(
            name,
            FunctionKind::Aggregate,
            FunctionBindingRequest {
                expected_result_type: None,
                arguments,
                logical_argument_count,
            },
            control,
        )
    }

    fn resolve_aggregate_binding_trusted(
        &self,
        name: &str,
        logical_argument_count: usize,
        arguments: &[FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.resolve_bound_trusted(
            name,
            FunctionKind::Aggregate,
            FunctionBindingRequest {
                expected_result_type: None,
                arguments,
                logical_argument_count,
            },
            control,
        )
    }

    fn resolve_aggregate_signature(
        &self,
        name: &str,
        arg_types: &[DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        resolve_bound_aggregate(self, name, arg_types, arg_types, false, control)
    }

    fn resolve_aggregate_update_signature(
        &self,
        name: &str,
        logical_arg_types: &[DataType],
        update_arg_types: &[DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        resolve_bound_aggregate(
            self,
            name,
            logical_arg_types,
            update_arg_types,
            false,
            control,
        )
    }

    fn resolve_aggregate_trusted(
        &self,
        name: &str,
        arg_types: &[DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        resolve_bound_aggregate(self, name, arg_types, arg_types, true, control)
    }

    fn volatility(&self, name: &str) -> FunctionVolatility {
        self.definition(name, FunctionKind::Scalar)
            .or_else(|| self.definition(name, FunctionKind::Window))
            .map(FunctionDefinition::volatility)
            .unwrap_or_default()
    }
}

pub(crate) fn resolve_sql_aggregate_binding(
    catalog: &dyn crate::compiler::SqlFunctionCatalog,
    name: &str,
    args: &[crate::analysis::TypedExpr],
    order_by: &[crate::analysis::SortItem],
    trusted: bool,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
    bind_aggregate_arguments(args, order_by, constant_policy, control, |request| {
        if trusted {
            catalog.resolve_aggregate_binding_trusted(
                name,
                request.logical_argument_count,
                request.arguments,
                control,
            )
        } else {
            catalog.resolve_aggregate_binding(
                name,
                request.logical_argument_count,
                request.arguments,
                control,
            )
        }
    })
}

// Derived aggregate metadata can describe internal carriers. Public argument
// matching still requires the original SQL value to belong to the input domain.
fn require_sql_aggregate_input(
    arguments: &[FunctionArgument],
    domain: DataType,
) -> Result<(), FunctionBindingError> {
    if let Some(FunctionArgument::Value { value_type, .. }) = arguments.first()
        && !trino_can_coerce_scalar(value_type.value_type().kind(), &ValueKind::Scalar(domain))
    {
        return Err(FunctionBindingError::NoMatchingOverload);
    }
    Ok(())
}

pub(crate) fn resolve_sql_aggregate_call(
    catalog: &dyn crate::compiler::SqlFunctionCatalog,
    name: &str,
    distinct: bool,
    args: &[crate::analysis::TypedExpr],
    order_by: &[crate::analysis::SortItem],
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
    bind_aggregate_arguments(args, order_by, constant_policy, control, |request| {
        match public_aggregate_target(name, distinct) {
            PublicAggregateTarget::Named(executable) => catalog.resolve_aggregate_binding(
                executable,
                request.logical_argument_count,
                request.arguments,
                control,
            ),
            PublicAggregateTarget::Distinct(aggregate) => {
                catalog.resolve_distinct_aggregate(aggregate, request, control)
            }
        }
    })
}

enum PublicAggregateTarget<'a> {
    Named(&'a str),
    Distinct(DistinctAggregate),
}

// Design: ADR-0246 (docs/adr/ADR-0246-trino-public-functions-and-internal-aggregate-identities.md)
fn public_aggregate_target(name: &str, distinct: bool) -> PublicAggregateTarget<'_> {
    match (distinct, name) {
        (true, "count") => PublicAggregateTarget::Distinct(DistinctAggregate::Count),
        (true, "sum") => PublicAggregateTarget::Distinct(DistinctAggregate::Sum),
        (true, "avg") => PublicAggregateTarget::Distinct(DistinctAggregate::Avg),
        (true, "array_agg") => PublicAggregateTarget::Named("array_agg_distinct"),
        _ => PublicAggregateTarget::Named(name),
    }
}

fn bind_aggregate_arguments(
    args: &[crate::analysis::TypedExpr],
    order_by: &[crate::analysis::SortItem],
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &dyn novarocks_type_contract::PureCompileControl,
    bind: impl FnOnce(
        FunctionBindingRequest<'_>,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError>,
) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
    let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
        control,
        novarocks_type_contract::CompilePhase::FunctionSpecialization,
    )?;
    let result = (|| {
        let count = args
            .len()
            .checked_add(order_by.len())
            .filter(|count| *count <= novarocks_functions::MAX_CALL_EFFECT_ARGUMENTS)
            .ok_or(novarocks_type_contract::CompileControlError::ResourceExhausted)?;
        let mut arguments = Vec::with_capacity(count);
        for argument in args.iter().chain(order_by.iter().map(|item| &item.expr)) {
            work.step()?;
            work.flush()?;
            arguments.push(crate::analysis::function_argument(
                argument,
                constant_policy,
                work.control(),
            )?);
            work.flush()?;
        }
        work.flush()?;
        bind(FunctionBindingRequest {
            expected_result_type: None,
            logical_argument_count: args.len(),
            arguments: &arguments,
        })
    })();
    if matches!(result, Err(FunctionBindingError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

pub fn builtin_sql_function_catalog() -> &'static dyn crate::compiler::SqlFunctionCatalog {
    builtin_engine_function_catalog()
}

#[cfg(test)]
pub(crate) fn test_function_catalog_snapshot() -> Arc<dyn crate::compiler::SqlFunctionCatalog> {
    builtin_sql_function_catalog().snapshot()
}

#[cfg(test)]
pub(crate) fn test_resolved_aggregate(
    name: &str,
    argument_types: &[DataType],
    distinct: bool,
) -> crate::binding::SqlFunctionBinding {
    let name = name.to_ascii_lowercase();
    let control = crate::compiler::SqlCompileControl::unbounded();
    let arguments = argument_types
        .iter()
        .cloned()
        .map(|data_type| FunctionArgument::Value {
            value_type: FunctionValueType::new(data_type, true),
            constant: None,
        })
        .collect::<Vec<_>>();
    let request = FunctionBindingRequest {
        expected_result_type: None,
        arguments: &arguments,
        logical_argument_count: arguments.len(),
    };
    let catalog = builtin_engine_function_catalog();
    let exact = match public_aggregate_target(&name, distinct) {
        PublicAggregateTarget::Named(executable) => {
            catalog.resolve_bound_trusted(executable, FunctionKind::Aggregate, request, &control)
        }
        PublicAggregateTarget::Distinct(aggregate) => {
            catalog.resolve_distinct_aggregate(aggregate, request, &control)
        }
    }
    .unwrap_or_else(|error| panic!("test aggregate `{name}` must resolve exactly: {error}"));
    crate::binding::SqlFunctionBinding::new(
        exact,
        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
    )
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
                FunctionArgument::Value { value_type, .. } => Ok(value_type.data_type().clone()),
                FunctionArgument::Lambda { .. } => Err(FunctionBindingError::NoMatchingOverload),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let overload = self
            .overloads
            .iter()
            .find(|overload| overload.argument_types.as_ref() == argument_types.as_slice())
            .ok_or(FunctionBindingError::NoMatchingOverload)?;
        self.selection(request, overload)
    }

    fn selection(
        &self,
        request: FunctionBindingRequest<'_>,
        overload: &AggregateOverloadMetadata,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        Ok(FunctionBindingSelection {
            overload: FunctionOverloadId::try_new(overload.identity.as_str())
                .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))?,
            argument_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                request
                    .arguments
                    .iter()
                    .map(FunctionArgument::argument_type)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            result_type: FunctionResultType::Scalar(FunctionValueType::new(
                overload.output_type.clone(),
                true,
            )),
            aggregate: Some(novarocks_functions::AggregateBindingSelection {
                state_argument_contract:
                    novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
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
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.resolve_exact(request)
    }

    fn select_at_overload_observed(
        &self,
        supplied: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = (|| {
            let mut exact = None;
            for overload in &self.overloads {
                let matches = overload.identity.as_str() == supplied.as_str();
                work.step()?;
                if matches {
                    exact = Some(overload);
                    break;
                }
            }
            let overload = exact.ok_or(FunctionBindingError::NoMatchingOverload)?;
            let same_arity = request.logical_argument_count == request.arguments.len()
                && request.arguments.len() == overload.argument_types.len();
            work.step()?;
            if !same_arity {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
            for (argument, expected) in request.arguments.iter().zip(&overload.argument_types) {
                let FunctionArgument::Value { value_type, .. } = argument else {
                    return Err(FunctionBindingError::NoMatchingOverload);
                };
                let exact = novarocks_type_contract::arrow_data_types_exact_observed(
                    value_type.data_type(),
                    expected,
                    || work.step().map_err(FunctionBindingError::from),
                )?;
                if !exact {
                    return Err(FunctionBindingError::NoMatchingOverload);
                }
            }
            work.flush()?;
            let selected = self.selection(request, overload);
            work.step()?;
            selected
        })();
        if matches!(&result, Err(FunctionBindingError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        if &self.select_at_overload_observed(&selected.overload, request, _control)? == selected {
            Ok(())
        } else {
            Err(FunctionBindingError::InvalidBinding(
                "selected test aggregate binding differs from its exact declaration".into(),
            ))
        }
    }
}

/// ANALYZE planning fixtures need a genuinely installed Binary-state/Binary-output
/// aggregate. Reuse the existing typed Theta CPU owner and its complete effects,
/// overloads and state contract; a metadata-only synthetic blob is not executable.
/// This helper is test-only and adds no registration to the product catalogue.
#[cfg(test)]
pub(crate) fn test_installed_blob_aggregate_catalog() -> EngineFunctionCatalog {
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            novarocks_connector_iceberg_functions::iceberg_theta_pure_definition()
                .expect("installed test aggregate owner"),
        )
        .expect("register installed test aggregate");
    builder.seal_bound().expect("test aggregate catalogue")
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
        overloads
            .iter()
            .map(|overload| FunctionOverloadDeclaration {
                effects: None,
                semantics: FunctionSemantics {
                    volatility: FunctionVolatility::Immutable,
                    argument_evaluation: FunctionArgumentEvaluation::Eager,
                    failure_behavior: FunctionFailureBehavior::Propagate,
                    intrinsic_row_error:
                        novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated,
                },
                identity: FunctionOverloadId::try_new(overload.identity.as_str())
                    .expect("test aggregate overload identity"),
                argument_pattern: format!("exact:{}:arguments", overload.identity.as_str()).into(),
                result_pattern: format!("exact:{}:output", overload.identity.as_str()).into(),
                aggregate: Some(AggregateBindingDeclaration {
                    state_argument_contract:
                        novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
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

#[cfg(test)]
mod tests {
    use super::*;

    struct ShadowBindingResolver(FunctionBindingSelection);
    impl FunctionBindingResolver for ShadowBindingResolver {
        fn resolve(
            &self,
            _: FunctionBindingRequest<'_>,
            _control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<FunctionBindingSelection, FunctionBindingError> {
            Ok(self.0.clone())
        }
        fn validate_selected(
            &self,
            selected: &FunctionBindingSelection,
            _: FunctionBindingRequest<'_>,
            _control: &dyn novarocks_type_contract::PureCompileControl,
        ) -> Result<(), FunctionBindingError> {
            if selected == &self.0 {
                Ok(())
            } else {
                Err(FunctionBindingError::NoMatchingOverload)
            }
        }
    }

    fn value_argument(
        data_type: DataType,
        nullable: bool,
        constant: Option<novarocks_functions::ConstantValue>,
    ) -> FunctionArgument {
        FunctionArgument::Value {
            value_type: FunctionValueType::new(data_type, nullable),
            constant,
        }
    }

    fn test_utf8_constant(value: &str) -> novarocks_functions::ConstantValue {
        let ty = FunctionValueType::new(DataType::Utf8, false);
        novarocks_functions::ConstantValue::from_utf8(
            Arc::new(ty.try_to_field("literal").unwrap()),
            ty,
            value,
            crate::constant::test_constant_policy(),
            novarocks_type_contract::CompilePhase::Validate,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap()
    }

    fn test_i64_constant(value: i64) -> novarocks_functions::ConstantValue {
        let ty = FunctionValueType::new(DataType::Int64, false);
        novarocks_functions::ConstantValue::from_i64(
            Arc::new(ty.try_to_field("literal").unwrap()),
            ty,
            value,
            crate::constant::test_constant_policy(),
            novarocks_type_contract::CompilePhase::Validate,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap()
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
                &crate::compiler::SqlCompileControl::unbounded(),
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
    fn json_output_domain_uses_bound_result_not_shadowed_function_spelling() {
        let builtin = build_builtin_engine_function_catalog().unwrap();
        let args = [value_argument(DataType::Utf8, false, None)];
        let bound = resolve_exact_scalar(&builtin, "json_object", &args);
        assert_eq!(
            scalar_output_logical_type(&bound),
            Some(novarocks_types::schema::SqlType::Json)
        );

        let overload = FunctionOverloadId::try_new("test.shadow.json_object/0/v1").unwrap();
        let mut selection = bound.selected.clone();
        selection.overload = overload.clone();
        selection.result_type =
            FunctionResultType::Scalar(FunctionValueType::new(DataType::Utf8, false));
        let declaration = FunctionBindingDeclaration::try_new(
            FunctionId::try_new("test.shadow/json_object/v1").unwrap(),
            FunctionKind::Scalar,
            [FunctionOverloadDeclaration {
                effects: None,
                semantics: bound.semantics,
                identity: overload,
                argument_pattern: "(varchar...)->varchar".into(),
                result_pattern: "varchar".into(),
                aggregate: None,
            }],
        )
        .unwrap();
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder
            .register(
                FunctionDefinition::try_new_bound(
                    "json_object",
                    FunctionVisibility::Public,
                    declaration,
                    Arc::new(ShadowBindingResolver(selection)),
                )
                .unwrap(),
            )
            .unwrap();
        let shadow = builder.seal().unwrap();
        let bound = resolve_exact_scalar(&shadow, "json_object", &args);
        assert_eq!(scalar_result(&bound).data_type().clone(), DataType::Utf8);
        assert_eq!(scalar_output_logical_type(&bound), None);
    }

    #[test]
    fn sqlx1_function_builtin_snapshot_has_canonical_volatility_set() {
        use novarocks_type_contract::SemanticParameterKey::{StatementStartUtc, TimeZone};

        let catalog = builtin_sql_function_catalog();
        for name in ["rand", "random", "uuid", "sleep"] {
            assert_eq!(
                catalog.volatility(name),
                FunctionVolatility::Volatile,
                "{name}"
            );
        }
        assert_eq!(catalog.volatility("lower"), FunctionVolatility::Immutable);

        let definitions = builtin_engine_function_catalog();
        for name in ["now", "current_date"] {
            assert_eq!(catalog.volatility(name), FunctionVolatility::Immutable);
            let declaration = definitions
                .definition(name, FunctionKind::Scalar)
                .unwrap()
                .binding_declaration()
                .unwrap();
            let [overload] = declaration.overloads() else {
                panic!("{name} has one clock overload");
            };
            let effects = declaration.effect_declaration(&overload.identity).unwrap();
            assert_eq!(
                effects.environment_dependencies.as_ref(),
                &[StatementStartUtc, TimeZone],
                "{name}"
            );
        }

        for name in [
            "current_timestamp",
            "current_time",
            "localtime",
            "localtimestamp",
            "curdate",
            "curtime",
        ] {
            assert!(
                definitions.definition(name, FunctionKind::Scalar).is_none(),
                "{name}"
            );
        }
    }

    #[test]
    fn sqlx1_function_snapshot_resolves_registered_signature() {
        let resolved = builtin_sql_function_catalog()
            .resolve_scalar_signature(
                "lower",
                &[DataType::Utf8],
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .expect("registered function resolves through snapshot");
        assert_eq!(resolved.return_type, DataType::Utf8);
    }

    #[test]
    fn hidden_aggregate_is_rejected_by_sql_user_resolution() {
        let catalog = test_exact_aggregate_catalog(
            "$hidden_stat",
            FunctionVisibility::Hidden,
            [AggregateOverloadMetadata::try_new(
                "test/$hidden_stat/v1",
                [DataType::Int64],
                DataType::Int64,
                DataType::Int64,
                "test/$hidden_stat/state-v1",
            )
            .unwrap()],
        );
        assert!(!crate::compiler::SqlFunctionCatalog::contains_aggregate(
            &catalog,
            "$hidden_stat"
        ));
        assert_eq!(
            crate::compiler::SqlFunctionCatalog::resolve_aggregate_signature(
                &catalog,
                "$hidden_stat",
                &[DataType::Int64],
                &crate::compiler::SqlCompileControl::unbounded()
            ),
            Err(FunctionResolutionError::HiddenFunction)
        );
    }

    #[test]
    fn m07_variant_output_domain_respects_literal_selected_result() {
        use novarocks_types::schema::SqlType;

        let catalog = build_builtin_engine_function_catalog().unwrap();
        for name in ["variant_get", "try_variant_get"] {
            let mut arguments = vec![
                FunctionArgument::Value {
                    value_type: FunctionValueType::try_with_logical_type(
                        DataType::LargeBinary,
                        true,
                        novarocks_type_contract::ValueLogicalType::Variant,
                    )
                    .unwrap(),
                    constant: None,
                },
                value_argument(DataType::Utf8, false, Some(test_utf8_constant("$.value"))),
            ];
            let binding = resolve_exact_scalar(&catalog, name, &arguments);
            assert_eq!(
                scalar_result(&binding).data_type().clone(),
                DataType::LargeBinary
            );
            assert_eq!(scalar_output_logical_type(&binding), Some(SqlType::Variant));
            let mut changed = binding.clone();
            changed.selected.result_type =
                FunctionResultType::Scalar(FunctionValueType::new(DataType::Binary, true));
            assert_eq!(scalar_output_logical_type(&changed), None);
            arguments.push(value_argument(
                DataType::Utf8,
                false,
                Some(test_utf8_constant("BIGINT")),
            ));
            let binding = resolve_exact_scalar(&catalog, name, &arguments);
            assert_eq!(scalar_result(&binding).data_type().clone(), DataType::Int64);
            assert_eq!(scalar_output_logical_type(&binding), None);
        }
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

        let signatures = novarocks_functions::builtin::registry::builtin_scalar_declarations()
            .into_iter()
            .find(|(name, _)| name == "json_object")
            .unwrap()
            .1;
        let overloads = (0..signatures.len())
            .map(|index| {
                FunctionOverloadId::try_new(format!("test.shadow.json_object/{index}/v1")).unwrap()
            })
            .collect::<Vec<_>>();
        let mut selection = bound.selected.clone();
        selection.overload = overloads[0].clone();
        selection.result_type =
            FunctionResultType::Scalar(FunctionValueType::new(DataType::Utf8, false));
        let declaration = FunctionBindingDeclaration::try_new(
            FunctionId::try_new("test.shadow/json_object/v1").unwrap(),
            FunctionKind::Scalar,
            overloads
                .iter()
                .cloned()
                .zip(&signatures)
                .map(|(identity, signature)| FunctionOverloadDeclaration {
                    effects: None,
                    semantics: bound.semantics,
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
                    Arc::new(ShadowBindingResolver(selection)),
                )
                .unwrap(),
            )
            .unwrap();
        let shadow = builder.seal().unwrap();
        let bound = resolve_exact_scalar(&shadow, "json_object", &args);
        assert_eq!(scalar_result(&bound).data_type().clone(), DataType::Utf8);
        assert_eq!(scalar_output_logical_type(&bound), None);
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
        assert!(!scalar_result(&resolve_exact_scalar(&catalog, "abs", &nonnull)).nullable());
        assert!(scalar_result(&resolve_exact_scalar(&catalog, "abs", &nullable)).nullable());

        let coalesce = [
            value_argument(DataType::Int64, true, None),
            value_argument(DataType::Int64, false, None),
        ];
        assert!(!scalar_result(&resolve_exact_scalar(&catalog, "coalesce", &coalesce)).nullable());
        let all_nullable = [
            value_argument(DataType::Int64, true, None),
            value_argument(DataType::Int64, true, None),
        ];
        assert!(
            scalar_result(&resolve_exact_scalar(&catalog, "coalesce", &all_nullable)).nullable()
        );

        let count = catalog
            .resolve_bound_user(
                "count",
                FunctionKind::Aggregate,
                FunctionBindingRequest {
                    expected_result_type: None,
                    arguments: &nonnull,
                    logical_argument_count: 1,
                },
                &crate::compiler::SqlCompileControl::unbounded(),
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
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .expect("sum binding");
        assert!(!aggregate_result_type(&count).nullable());
        assert!(aggregate_result_type(&sum).nullable());
    }

    #[test]
    fn literal_dependent_dynamic_scalars_freeze_exact_result_shapes() {
        let catalog = build_builtin_engine_function_catalog().expect("builtin catalog");
        let named = [
            value_argument(DataType::Utf8, false, Some(test_utf8_constant("left"))),
            value_argument(DataType::Int64, false, None),
            value_argument(DataType::Utf8, false, Some(test_utf8_constant("right"))),
            value_argument(DataType::Boolean, true, None),
        ];
        let named = catalog
            .resolve_bound_trusted(
                "named_struct",
                FunctionKind::Scalar,
                FunctionBindingRequest {
                    expected_result_type: None,
                    arguments: &named,
                    logical_argument_count: named.len(),
                },
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .unwrap();
        let DataType::Struct(fields) = &scalar_result(&named).data_type() else {
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
                value_argument(DataType::Int64, false, Some(test_i64_constant(2))),
            ],
        );
        assert_eq!(
            scalar_result(&rounded).data_type().clone(),
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
                        novarocks_type_contract::ValueLogicalType::Variant,
                    )
                    .unwrap(),
                    constant: None,
                },
                value_argument(DataType::Utf8, false, Some(test_utf8_constant("$.x"))),
                value_argument(DataType::Utf8, false, Some(test_utf8_constant("BIGINT"))),
            ],
        );
        assert_eq!(scalar_result(&variant).data_type().clone(), DataType::Int64);
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
                    expected_result_type: None,
                    arguments: &arguments,
                    logical_argument_count: 1,
                },
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .expect("UNNEST binding");
        assert_eq!(binding.function_id.as_str(), "builtin.table/unnest/v1");
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
                &crate::compiler::SqlCompileControl::unbounded(),
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
            FunctionResultType::Relation(novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                vec![FunctionValueType::new(DataType::Int64, true)].into_boxed_slice()
            ))
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
            &crate::compiler::SqlCompileControl::unbounded(),
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
                &crate::compiler::SqlCompileControl::unbounded(),
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
                &crate::compiler::SqlCompileControl::unbounded()
            ),
            Err(FunctionResolutionError::UnknownFunction)
        );

        let std_user = resolve_bound_aggregate(
            &catalog,
            "std",
            &[DataType::Int64],
            &[DataType::Int64],
            false,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .expect("std alias resolves for user SQL");
        let std_trusted = resolve_bound_aggregate(
            &catalog,
            "std",
            &[DataType::Int64],
            &[DataType::Int64],
            true,
            &crate::compiler::SqlCompileControl::unbounded(),
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
                &crate::compiler::SqlCompileControl::unbounded(),
            ),
            Err(FunctionResolutionError::NoMatchingSignature { .. })
        ));

        let resolved = resolve_bound_aggregate(
            &catalog,
            "array_agg",
            &[DataType::Utf8],
            &[DataType::Utf8, DataType::Int64],
            false,
            &crate::compiler::SqlCompileControl::unbounded(),
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
                &crate::compiler::SqlCompileControl::unbounded(),
            ),
            Err(FunctionResolutionError::NoMatchingSignature { .. })
                | Err(FunctionResolutionError::BadSignature(_))
        ));
    }

    #[test]
    fn abs_exact_bindings_freeze_input_and_promoted_output_widths() {
        let catalog = build_builtin_engine_function_catalog().expect("builtin catalog");
        let control = crate::compiler::SqlCompileControl::unbounded();
        let negate = novarocks_type_contract::OperatorFunction::Negation.function_id();
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
                let input_type = if input == DataType::FixedSizeBinary(16) {
                    FunctionValueType::try_with_logical_type(
                        input.clone(),
                        nullable,
                        novarocks_type_contract::ValueLogicalType::LargeInt,
                    )
                    .unwrap()
                } else {
                    FunctionValueType::new(input.clone(), nullable)
                };
                let arguments = [FunctionArgument::Value {
                    value_type: input_type.clone(),
                    constant: None,
                }];
                let request = FunctionBindingRequest {
                    expected_result_type: None,
                    arguments: &arguments,
                    logical_argument_count: 1,
                };
                let binding = resolve_exact_scalar(&catalog, "abs", &arguments);
                assert_eq!(scalar_result(&binding).data_type().clone(), output);
                assert_eq!(scalar_result(&binding).nullable(), nullable);
                assert_eq!(
                    binding.selected.argument_types.as_ref(),
                    &[FunctionArgumentType::Value(input_type.clone())]
                );
                catalog
                    .validate_bound(&binding, request, &control)
                    .expect("the frozen ABS profile must validate without changing the input type");

                let negation = catalog
                    .resolve_bound_trusted_by_id(&negate, FunctionKind::Scalar, request, &control)
                    .expect("the exact negation overload for this input");
                assert_eq!(scalar_result(&negation), &input_type);
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
                        &crate::compiler::SqlCompileControl::unbounded(),
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
                    &crate::compiler::SqlCompileControl::unbounded(),
                )
                .unwrap();
        }
    }

    #[test]
    fn array_ordering_binding_closes_comparator_domain() {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let list = |ty| DataType::List(Arc::new(arrow::datatypes::Field::new("item", ty, true)));
        for ty in [
            list(DataType::Int64),
            DataType::Map(
                Arc::new(arrow::datatypes::Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Arc::new(arrow::datatypes::Field::new("key", DataType::Int64, false)),
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
            for name in ["array_sort", "array_min", "array_max"] {
                let arguments = vec![value_argument(list(ty.clone()), true, None)];
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
                            &crate::compiler::SqlCompileControl::unbounded(),
                        )
                        .is_err(),
                    "{name}"
                );
            }
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
                    &crate::compiler::SqlCompileControl::unbounded(),
                )
                .unwrap();
        }
    }

    #[test]
    fn selected_collection_shapes_preserve_recursive_equality() {
        let catalog = build_builtin_engine_function_catalog().unwrap();
        let list = |ty| DataType::List(Arc::new(arrow::datatypes::Field::new("item", ty, true)));
        for name in [
            "contains",
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
                        &crate::compiler::SqlCompileControl::unbounded(),
                    )
                    .is_err(),
                "{name}"
            );
        }
        for name in ["contains", "array_position", "array_remove"] {
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
                    &crate::compiler::SqlCompileControl::unbounded(),
                )
                .unwrap();
        }
        for (name, types) in [
            ("flatten", vec![list(DataType::Int64)]),
            ("repeat", vec![DataType::Int64]),
            ("zip", vec![]),
            ("zip", vec![DataType::Int64]),
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
                        &crate::compiler::SqlCompileControl::unbounded(),
                    )
                    .is_err(),
                "{name}"
            );
        }
        let arguments = [
            value_argument(list(DataType::Date32), true, None),
            value_argument(list(DataType::Date32), true, None),
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
        for name in ["contains", "array_position", "array_remove"] {
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
                        &crate::compiler::SqlCompileControl::unbounded(),
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
        for name in ["array_sort", "array_min", "array_max"] {
            let arguments = vec![value_argument(list(DataType::Null), true, None)];
            let bound = resolve_exact_scalar(&catalog, name, &arguments);
            catalog
                .validate_bound(
                    &bound,
                    FunctionBindingRequest {
                        expected_result_type: None,
                        arguments: &arguments,
                        logical_argument_count: arguments.len(),
                    },
                    &crate::compiler::SqlCompileControl::unbounded(),
                )
                .unwrap();
            assert_eq!(
                bound.semantics.intrinsic_row_error,
                novarocks_type_contract::FunctionIntrinsicRowError::NoRowError
            );
        }
    }
}

#[cfg(test)]
mod overload_declaration_tests;
