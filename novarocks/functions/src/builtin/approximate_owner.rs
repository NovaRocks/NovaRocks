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

use super::{
    approx_distinct_kernel::{ApproxDistinctKernel, ApproxProfile},
    approx_histogram_kernel::ApproxHistogramKernel,
    binding_control,
};
use crate::aggregate_types::{AggregateTypeArguments, AggregateTypeRecipe};
use crate::kernel_control::{compile_failure, invalid};
use crate::*;
use arrow_schema::DataType;
use novarocks_type_contract::{
    AggregateStateArgumentContract, ArgumentControl, CallEffects, CallProofScope,
    CompileCheckpoints, CompilePhase, FunctionEffectDeclaration, FunctionInstanceState,
    FunctionNullBehavior, ObservableEffects, PureCompileControl, ValueLogicalType,
};
use std::sync::Arc;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Family {
    Distinct,
    Percentile,
    Frequency,
}

impl Family {
    fn name(self) -> &'static str {
        match self {
            Self::Distinct => "approx_distinct",
            Self::Percentile => "approx_percentile",
            Self::Frequency => "approx_most_frequent",
        }
    }

    fn overloads(self) -> &'static [Overload] {
        match self {
            Self::Distinct => DISTINCT,
            Self::Percentile => PERCENTILE,
            Self::Frequency => FREQUENCY,
        }
    }
}

#[derive(Clone, Copy)]
enum Input {
    Unknown,
    Boolean,
    AnyHash,
    Bigint,
    Real,
    Double,
    Varchar,
    DoubleArray,
}

impl Input {
    fn public_sql(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Boolean => "boolean",
            Self::AnyHash => "T",
            Self::Bigint => "bigint",
            Self::Real => "real",
            Self::Double => "double",
            Self::Varchar => "varchar",
            Self::DoubleArray => "array(double)",
        }
    }

    fn accepts(self, value: &DataType) -> bool {
        match self {
            Self::Unknown => value == &DataType::Null,
            Self::Boolean => value == &DataType::Boolean,
            Self::AnyHash => !matches!(value, DataType::Null | DataType::Boolean),
            Self::Bigint => value == &DataType::Int64,
            Self::Real => value == &DataType::Float32,
            Self::Double => value == &DataType::Float64,
            Self::Varchar => value == &DataType::Utf8,
            Self::DoubleArray => {
                matches!(value, DataType::List(field) if field.data_type() == &DataType::Float64)
            }
        }
    }
}

#[derive(Clone, Copy)]
enum Output {
    Count,
    Value,
    Array,
    Map,
}

impl Output {
    fn type_recipe(self, args: AggregateTypeArguments<'_>) -> AggregateTypeRecipe<'_> {
        match self {
            Self::Count => AggregateTypeRecipe::Scalar(DataType::Int64),
            Self::Value => AggregateTypeRecipe::Argument(args, 0),
            Self::Array => AggregateTypeRecipe::List(args, 0),
            Self::Map => AggregateTypeRecipe::Map(args, 1, 0),
        }
    }

    fn nullable(self) -> bool {
        !matches!(self, Self::Count)
    }

    fn pattern(self) -> &'static str {
        match self {
            Self::Count => "i64",
            Self::Value => "T",
            Self::Array => "list<T>",
            Self::Map => "map<T,i64>",
        }
    }
}

struct Overload {
    name: &'static str,
    signature: &'static str,
    inputs: &'static [Input],
    output: Output,
    state: &'static str,
    installed: bool,
}

impl Overload {
    fn public_signature(&self) -> FunctionPublicSignature {
        let inputs = self
            .inputs
            .iter()
            .map(|input| input.public_sql())
            .collect::<Vec<_>>();
        let result = match self.output {
            Output::Count => "bigint".to_owned(),
            Output::Value => inputs[0].to_owned(),
            Output::Array => format!("array({})", inputs[0]),
            Output::Map => format!("map({},bigint)", inputs[1]),
        };
        FunctionPublicSignature::new(inputs.join(", "), result, "")
    }

    fn identity(&self, family: Family) -> Result<FunctionOverloadId, FunctionBindingError> {
        FunctionOverloadId::try_new(format!("builtin.aggregate/{}/{}", family.name(), self.name))
            .map_err(binding_error)
    }

    fn matches_identity(&self, family: Family, identity: &str) -> bool {
        let segments = identity.split('/');
        segments.eq(["builtin.aggregate", family.name(), self.name])
    }

    fn state_format(
        &self,
        family: Family,
    ) -> Result<AggregateStateFormatIdentity, FunctionBindingError> {
        AggregateStateFormatIdentity::try_new(format!("novarocks/{}/{}", family.name(), self.state))
            .map_err(binding_error)
    }

    fn matches_state(&self, family: Family, identity: &AggregateStateFormatIdentity) -> bool {
        identity
            .as_str()
            .split('/')
            .eq(["novarocks", family.name(), self.state])
    }

    fn declaration(
        &self,
        family: Family,
    ) -> Result<FunctionOverloadDeclaration, FunctionBindingError> {
        Ok(FunctionOverloadDeclaration {
            identity: self.identity(family)?,
            argument_pattern: self.signature.into(),
            result_pattern: self.output.pattern().into(),
            semantics: FunctionSemantics {
                volatility: FunctionVolatility::Immutable,
                argument_evaluation: FunctionArgumentEvaluation::Eager,
                failure_behavior: FunctionFailureBehavior::Propagate,
                intrinsic_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
            },
            effects: Some(effects()),
            aggregate: Some(AggregateBindingDeclaration {
                intermediate_pattern: "binary".into(),
                state_format: self.state_format(family)?,
                state_argument_contract: AggregateStateArgumentContract::ExactSignature,
            }),
        })
    }

    fn accepts(&self, args: AggregateTypeArguments<'_>, logical_count: usize) -> bool {
        logical_count == self.inputs.len()
            && self.inputs.iter().enumerate().all(|(index, input)| {
                args.data_type(index)
                    .is_some_and(|value| input.accepts(value))
            })
    }
}

macro_rules! overload {
    ($name:literal, $pattern:literal, [$($input:ident),+], $output:ident, $state:literal, $installed:literal) => {
        Overload {
            name: $name, signature: $pattern, inputs: &[$(Input::$input),+],
            output: Output::$output, state: $state, installed: $installed,
        }
    };
}

const DISTINCT: &[Overload] = &[
    overload!(
        "unknown-v1",
        "(unknown)->i64",
        [Unknown],
        Count,
        "unknown-v2",
        true
    ),
    overload!(
        "boolean-v1",
        "(bool)->i64",
        [Boolean],
        Count,
        "boolean-set-v2",
        true
    ),
    overload!(
        "default-hll-v1",
        "(any)->i64",
        [AnyHash],
        Count,
        "hll-xxhash64-parameterized-v2",
        true
    ),
    overload!(
        "unknown-error-v1",
        "(unknown,f64)->i64",
        [Unknown, Double],
        Count,
        "unknown-v2",
        true
    ),
    overload!(
        "boolean-error-v1",
        "(bool,f64)->i64",
        [Boolean, Double],
        Count,
        "boolean-set-v2",
        true
    ),
    overload!(
        "explicit-error-hll-v1",
        "(any,f64)->i64",
        [AnyHash, Double],
        Count,
        "hll-xxhash64-parameterized-v2",
        true
    ),
];

const PERCENTILE: &[Overload] = &[
    overload!(
        "bigint-scalar-v1",
        "(i64,f64)->i64",
        [Bigint, Double],
        Value,
        "tdigest-binary64-v1",
        true
    ),
    overload!(
        "real-scalar-v1",
        "(f32,f64)->f32",
        [Real, Double],
        Value,
        "tdigest-binary64-v1",
        true
    ),
    overload!(
        "double-scalar-v1",
        "(f64,f64)->f64",
        [Double, Double],
        Value,
        "tdigest-binary64-v1",
        true
    ),
    overload!(
        "bigint-weighted-scalar-v1",
        "(i64,f64,f64)->i64",
        [Bigint, Double, Double],
        Value,
        "tdigest-binary64-v1",
        true
    ),
    overload!(
        "real-weighted-scalar-v1",
        "(f32,f64,f64)->f32",
        [Real, Double, Double],
        Value,
        "tdigest-binary64-v1",
        true
    ),
    overload!(
        "double-weighted-scalar-v1",
        "(f64,f64,f64)->f64",
        [Double, Double, Double],
        Value,
        "tdigest-binary64-v1",
        true
    ),
    overload!(
        "bigint-array-v1",
        "(i64,list<f64>)->list<i64>",
        [Bigint, DoubleArray],
        Array,
        "tdigest-binary64-v1",
        true
    ),
    overload!(
        "real-array-v1",
        "(f32,list<f64>)->list<f32>",
        [Real, DoubleArray],
        Array,
        "tdigest-binary64-v1",
        true
    ),
    overload!(
        "double-array-v1",
        "(f64,list<f64>)->list<f64>",
        [Double, DoubleArray],
        Array,
        "tdigest-binary64-v1",
        true
    ),
    overload!(
        "bigint-weighted-array-v1",
        "(i64,f64,list<f64>)->list<i64>",
        [Bigint, Double, DoubleArray],
        Array,
        "tdigest-binary64-v1",
        true
    ),
    overload!(
        "real-weighted-array-v1",
        "(f32,f64,list<f64>)->list<f32>",
        [Real, Double, DoubleArray],
        Array,
        "tdigest-binary64-v1",
        true
    ),
    overload!(
        "double-weighted-array-v1",
        "(f64,f64,list<f64>)->list<f64>",
        [Double, Double, DoubleArray],
        Array,
        "tdigest-binary64-v1",
        true
    ),
    overload!(
        "bigint-accuracy-v1",
        "(i64,f64,f64,f64)->i64",
        [Bigint, Double, Double, Double],
        Value,
        "quantile-digest-v1",
        false
    ),
    overload!(
        "real-accuracy-v1",
        "(f32,f64,f64,f64)->f32",
        [Real, Double, Double, Double],
        Value,
        "quantile-digest-v1",
        false
    ),
    overload!(
        "double-accuracy-v1",
        "(f64,f64,f64,f64)->f64",
        [Double, Double, Double, Double],
        Value,
        "quantile-digest-v1",
        false
    ),
];

const FREQUENCY: &[Overload] = &[
    overload!(
        "bigint-v1",
        "(i64,i64,i64)->map<i64,i64>",
        [Bigint, Bigint, Bigint],
        Map,
        "space-saving-v1",
        true
    ),
    overload!(
        "varchar-v1",
        "(i64,varchar,i64)->map<varchar,i64>",
        [Bigint, Varchar, Bigint],
        Map,
        "space-saving-v1",
        true
    ),
];

fn effects() -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::CalledOnNull,
        argument_control: ArgumentControl::Aggregate,
        instance_state: FunctionInstanceState::AggregateInstance,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::new([]),
    }
}

fn matches_scalar(value: &FunctionValueType, data_type: DataType) -> bool {
    value.data_type() == &data_type
        && !value.nullable()
        && value.logical_type() == ValueLogicalType::Physical
}

fn binding_error(error: impl std::fmt::Display) -> FunctionBindingError {
    FunctionBindingError::InvalidBinding(error.to_string().into())
}

fn catalogue_error(error: impl std::fmt::Display) -> FunctionCatalogError {
    FunctionCatalogError::InvalidStableIdentity {
        subject: "builtin approximate aggregate owner",
        value: error.to_string().into(),
    }
}

pub(super) fn definitions() -> Result<[FunctionDefinition; 3], FunctionCatalogError> {
    Ok([
        definition(Family::Distinct, ApproxDistinctKernel::prepare)?,
        definition(Family::Percentile, |contract, _| {
            ApproxHistogramKernel::percentile(contract)
        })?,
        definition(Family::Frequency, |contract, _| {
            ApproxHistogramKernel::frequency(contract)
        })?,
    ])
}

fn definition<K: PreparedAggregateKernel>(
    family: Family,
    prepare: PrepareKernel<K>,
) -> Result<FunctionDefinition, FunctionCatalogError> {
    let name = family.name();
    let function_id =
        FunctionId::try_new(format!("builtin.aggregate/{name}/v1")).map_err(catalogue_error)?;
    let overloads = family.overloads();
    let declarations = overloads
        .iter()
        .map(|overload| overload.declaration(family))
        .collect::<Result<Vec<_>, _>>()
        .map_err(catalogue_error)?;
    let public = declarations
        .iter()
        .zip(overloads)
        .map(|(declaration, overload)| {
            (
                declaration.identity.clone(),
                vec![overload.public_signature()],
            )
        })
        .collect::<Vec<_>>();
    let declaration =
        FunctionBindingDeclaration::try_new(function_id, FunctionKind::Aggregate, declarations)
            .map_err(catalogue_error)?
            .with_public_signatures(public)
            .map_err(super::catalogue::public_signature_error)?;
    let mut declared_only = Vec::new();
    let mut implementations = Vec::new();
    for overload in overloads {
        let identity = overload.identity(family).map_err(catalogue_error)?;
        if overload.installed {
            implementations.push(PureImplementationDeclaration {
                overload: identity,
                implementation: PureImplementationId::try_new(format!(
                    "builtin.aggregate/{name}/selected-{}",
                    overload.name
                ))?,
                abi: PureKernelAbi::AggregateV1,
            });
        } else {
            declared_only.push(identity);
        }
    }
    FunctionDefinition::try_new_pure_aggregate_with_declared_only(
        name,
        FunctionVisibility::Public,
        Arc::new(ApproximateOwner {
            binding: Arc::new(ApproximateBinding { family }),
            declaration,
            implementations: implementations.into_boxed_slice(),
            prepare,
        }),
        &declared_only,
    )
    .map_err(catalogue_error)
}

type PrepareKernel<K> =
    fn(Arc<AggregateCallContract>, FunctionBindingRequest<'_>) -> Result<K, KernelFailure>;

struct ApproximateOwner<K> {
    binding: Arc<ApproximateBinding>,
    declaration: FunctionBindingDeclaration,
    implementations: Box<[PureImplementationDeclaration]>,
    prepare: PrepareKernel<K>,
}

pub(crate) struct ApproximateBinding {
    family: Family,
}

impl ApproximateBinding {
    fn decision<'a>(
        &self,
        request: FunctionBindingRequest<'a>,
        required: Option<&FunctionOverloadId>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(&'static Overload, AggregateTypeArguments<'a>), FunctionBindingError> {
        let args = AggregateTypeArguments::Values(request.arguments);
        for argument in request.arguments {
            work.step()?;
            let FunctionArgument::Value { value_type, .. } = argument else {
                return Err(FunctionBindingError::NoMatchingOverload);
            };
            if self.family != Family::Distinct
                && value_type.logical_type() != ValueLogicalType::Physical
            {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
        }
        if self.family == Family::Distinct
            && request.logical_argument_count == 2
            && args
                .value_type(1)
                .is_some_and(|value| value.logical_type() != ValueLogicalType::Physical)
        {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        for overload in self.family.overloads() {
            work.step()?;
            if required.is_none_or(|id| overload.matches_identity(self.family, id.as_str()))
                && overload.accepts(args, request.logical_argument_count)
            {
                return Ok((overload, args));
            }
        }
        Err(
            required.map_or(FunctionBindingError::NoMatchingOverload, |id| {
                FunctionBindingError::UnknownOverload(id.clone())
            }),
        )
    }

    fn materialize(
        &self,
        request: FunctionBindingRequest<'_>,
        required: Option<&FunctionOverloadId>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        let (overload, args) = self.decision(request, required, work)?;
        let result = overload
            .output
            .type_recipe(args)
            .materialize_value_pure(overload.output.nullable(), ValueLogicalType::Physical)?;
        binding_control::value_type(&result, work)?;
        Ok(FunctionBindingSelection {
            overload: overload.identity(self.family)?,
            argument_types: binding_control::argument_types(request, work)?,
            result_type: FunctionResultType::Scalar(result),
            aggregate: Some(AggregateBindingSelection {
                intermediate_type: FunctionValueType::new(DataType::Binary, false),
                state_format: overload.state_format(self.family)?,
                state_argument_contract: AggregateStateArgumentContract::ExactSignature,
            }),
        })
    }
}

impl FunctionBindingResolver for ApproximateBinding {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| self.materialize(request, None, work))
    }

    fn select_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        binding_control::scope(control, |work| {
            self.materialize(request, Some(overload), work)
        })
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        binding_control::scope(control, |work| {
            let (overload, args) = self.decision(request, Some(&selected.overload), work)?;
            let (Some(aggregate), FunctionResultType::Scalar(result)) =
                (&selected.aggregate, &selected.result_type)
            else {
                return Err(binding_error(
                    "approximate aggregate requires both selected result and state",
                ));
            };
            if !overload.matches_state(self.family, &aggregate.state_format)
                || aggregate.state_argument_contract
                    != AggregateStateArgumentContract::ExactSignature
                || !matches_scalar(&aggregate.intermediate_type, DataType::Binary)
                || !overload.output.type_recipe(args).matches_value_observed(
                    result.typed_binding().borrowed(),
                    overload.output.nullable(),
                    ValueLogicalType::Physical,
                    work,
                )?
                || request.arguments.len() != selected.argument_types.len()
            {
                return Err(binding_error(
                    "approximate aggregate state or result differs from its binding",
                ));
            }
            for (argument, original) in selected.argument_types.iter().zip(request.arguments) {
                work.step()?;
                let (
                    FunctionArgumentType::Value(argument),
                    FunctionArgument::Value {
                        value_type: original,
                        ..
                    },
                ) = (argument, original)
                else {
                    return Err(binding_error(
                        "approximate aggregate requires its original value channels",
                    ));
                };
                if !argument.exactly_equals_checked_observed(original, || {
                    work.step().map_err(FunctionBindingError::from)
                })? {
                    return Err(binding_error(
                        "approximate aggregate argument differs from its selected source",
                    ));
                }
            }
            Ok(())
        })
    }
}

impl<K: Send + Sync> FunctionBindingResolver for ApproximateOwner<K> {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.binding.resolve(request, control)
    }

    fn select_at_overload_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.binding
            .select_at_overload_observed(overload, request, control)
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        self.binding.validate_selected(selected, request, control)
    }
}

impl<K: Send + Sync> ApproximateOwner<K> {
    fn signature(
        &self,
        selected: Option<&AggregateOverloadIdentity>,
        types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        let family = self.binding.family;
        let args = AggregateTypeArguments::Arrow(types);
        let overload = family
            .overloads()
            .iter()
            .find(|overload| {
                selected.is_none_or(|id| overload.matches_identity(family, id.as_str()))
                    && overload.accepts(args, types.len())
            })
            .ok_or(FunctionResolutionError::NoMatchingSignature {
                candidates: family.overloads().len(),
                binding_enforced: true,
            })?;
        let as_resolution =
            |error: FunctionBindingError| FunctionResolutionError::BadSignature(error.to_string());
        let overload_id = overload
            .identity(family)
            .and_then(|id| AggregateOverloadIdentity::try_new(id.as_str()).map_err(binding_error))
            .map_err(as_resolution)?;
        Ok(ResolvedAggregateSignature {
            overload: overload_id,
            argument_types: types.to_vec(),
            intermediate_type: DataType::Binary,
            output_type: overload.output.type_recipe(args).materialize_arrow(),
            state_format: overload.state_format(family).map_err(as_resolution)?,
        })
    }

    fn validate_profile(
        &self,
        selected: &FunctionBindingSelection,
        logical_count: usize,
    ) -> Result<(), FunctionBindingError> {
        let installed = self
            .implementations
            .iter()
            .any(|item| item.overload == selected.overload);
        if !installed || selected.argument_types.len() != logical_count {
            return Err(FunctionBindingError::UnavailableImplementation(
                selected.overload.clone(),
            ));
        }
        if self.binding.family == Family::Distinct
            && !matches!(selected.argument_types.first(), Some(FunctionArgumentType::Value(value)) if ApproxProfile::prepare(value).is_some())
        {
            return Err(FunctionBindingError::UnavailableImplementation(
                selected.overload.clone(),
            ));
        }
        Ok(())
    }
}

impl<K: Send + Sync> AggregateSignatureResolver for ApproximateOwner<K> {
    fn resolve_aggregate(
        &self,
        types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        self.signature(None, types)
    }
    fn resolve_update_signature(
        &self,
        overload: &AggregateOverloadIdentity,
        types: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        self.signature(Some(overload), types)
    }
    fn produces_null(&self) -> bool {
        self.binding.family != Family::Distinct
    }
}

impl<K: Send + Sync> PureFunctionMetadataOwner for ApproximateOwner<K> {
    fn binding_resolver_owner(self: Arc<Self>) -> FunctionBindingResolverOwner {
        FunctionBindingResolverOwner::borrowed_approximate(self.binding.clone())
    }
    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }
    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
    fn admit_selected_profile_observed(
        &self,
        selected: &FunctionBindingSelection,
        logical_argument_count: usize,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        control.checkpoint(CompilePhase::FunctionSpecialization, 0)?;
        self.validate_profile(selected, logical_argument_count)
    }
}

impl<K: Send + Sync> ApproximateOwner<K> {
    fn call_effects(&self, proof_scope: CallProofScope) -> CallEffects {
        let base = effects();
        CallEffects {
            value_stability: base.value_stability,
            own_row_error: base.own_row_error,
            failure_behavior: base.failure_behavior,
            null_behavior: base.null_behavior,
            argument_control: base.argument_control,
            instance_state: base.instance_state,
            observable_effects: base.observable_effects,
            environment: Box::new([]),
            proof_scope,
        }
    }
}
impl<K: Send + Sync> FunctionEffectOwner for ApproximateOwner<K> {
    type Error = FunctionBindingError;
    fn declaration(
        &self,
        id: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if id != self.declaration.function_id() {
            return Err(FunctionBindingError::UnknownFunction);
        }
        self.declaration.effect_declaration(&selected.overload)
    }
    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        self.refine_core(input, control, || {
            self.binding
                .validate_selected(input.selected, input.request, control)
        })
    }
    fn validate_and_refine_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &crate::CheckedCallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, crate::CheckedOwnerFailure<FunctionEffectOwnerError<Self::Error>>>
    {
        self.refine_core(input, control, || {
            checked.require_binding_author(self.binding.as_ref(), input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}
impl<K: PreparedAggregateKernel> PureAggregateImplementation for ApproximateOwner<K> {
    type Kernel = K;
    fn prepare_aggregate(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<K>, KernelFailure> {
        self.prepare_core(input, contract, control, || {
            self.binding
                .validate_selected(input.selected, input.request, control)
        })
    }
    fn prepare_aggregate_checked(
        &self,
        input: CallEffectInput<'_>,
        checked: &crate::CheckedCallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<Self::Kernel>, crate::CheckedOwnerFailure<KernelFailure>> {
        self.prepare_core(input, contract, control, || {
            checked.require_binding_author(self.binding.as_ref(), input)
        })
        .map_err(crate::CheckedOwnerFailure::Original)
    }
}

impl<K: Send + Sync> ApproximateOwner<K> {
    fn refine_core(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<CallEffects, FunctionEffectOwnerError<FunctionBindingError>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        let result = (|| {
            let exact = input.function_id == self.declaration.function_id()
                && input.kind == FunctionKind::Aggregate;
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            if !exact {
                return Err(FunctionBindingError::UnknownFunction.into());
            }
            let scope = input.environment.is_empty()
                && (input.proof_scope == CallProofScope::Unconditional
                    || input.proof_scope == input.context.proof_scope());
            work.step().map_err(FunctionEffectOwnerError::Control)?;
            if !scope {
                return Err(FunctionBindingError::InvalidBinding(
                    "approximate aggregate requires its original scope and no environment".into(),
                )
                .into());
            }
            work.flush().map_err(FunctionEffectOwnerError::Control)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => FunctionEffectOwnerError::Control(error),
                other => FunctionEffectOwnerError::Owner(other),
            })?;
            Ok(self.call_effects(input.proof_scope))
        })();
        work.finish_result(result, FunctionEffectOwnerError::Control)
    }
    fn prepare_core(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
        validate: impl FnOnce() -> Result<(), FunctionBindingError>,
    ) -> Result<Arc<K>, KernelFailure> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(compile_failure)?;
        let result = (|| {
            let call = contract.call();
            let exact = input.function_id == self.declaration.function_id()
                && input.kind == FunctionKind::Aggregate
                && call.function_id() == input.function_id
                && std::ptr::eq(call.selected(), input.selected)
                && call.context() == input.context
                && call.decimal_overflow_policy() == input.decimal_overflow_policy
                && call.effects() == &self.call_effects(input.proof_scope)
                && call.logical_argument_count() == input.request.logical_argument_count;
            work.step().map_err(compile_failure)?;
            if !exact {
                return Err(invalid(
                    "approximate aggregate preparation differs from its exact checked call",
                ));
            }
            self.validate_profile(input.selected, input.request.logical_argument_count)
                .map_err(|_| invalid("approximate aggregate preparation requires its installed unordered profile"))?;
            if !contract.order_keys().is_empty() {
                return Err(invalid(
                    "approximate aggregate lifecycle does not implement function ORDER BY",
                ));
            }
            work.flush().map_err(compile_failure)?;
            validate().map_err(|error| match error {
                FunctionBindingError::Control(error) => compile_failure(error),
                _ => invalid("approximate aggregate preparation has a stale selected binding"),
            })?;
            for (actual, selected) in input
                .request
                .arguments
                .iter()
                .zip(&input.selected.argument_types)
            {
                let (
                    FunctionArgument::Value {
                        value_type: actual, ..
                    },
                    FunctionArgumentType::Value(selected),
                ) = (actual, selected)
                else {
                    work.step().map_err(compile_failure)?;
                    return Err(invalid(
                        "approximate aggregate requires canonical value arguments",
                    ));
                };
                if !actual.exactly_equals_observed::<KernelFailure>(selected, || {
                    work.step().map_err(compile_failure)
                })? {
                    return Err(invalid(
                        "approximate aggregate request differs from its exact selected source",
                    ));
                }
            }
            work.flush().map_err(compile_failure)?;
            let prepared = Arc::new((self.prepare)(contract, input.request)?);
            work.flush().map_err(compile_failure)?;
            Ok(prepared)
        })();
        work.finish_result(result, compile_failure)
    }
}
