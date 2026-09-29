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

//! Explicit function declarations and exact, carrier-neutral binding.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use crate::{
    AggregateOverloadDeclaration, AggregateStateFormatIdentity, EngineFunctionCatalog,
    EngineFunctionCatalogBuilder, FunctionArgumentEvaluation, FunctionArgumentType,
    FunctionCatalogError, FunctionDefinition, FunctionFailureBehavior, FunctionId, FunctionKind,
    FunctionOverloadId, FunctionValueType, FunctionVisibility, FunctionVolatility, digest_text,
};

/// Semantics declared by the implementation owner, never inferred from a name.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FunctionSemantics {
    pub volatility: FunctionVolatility,
    pub argument_evaluation: FunctionArgumentEvaluation,
    pub failure_behavior: FunctionFailureBehavior,
    pub intrinsic_row_error: novarocks_type_contract::FunctionIntrinsicRowError,
}

/// The aggregate state contract remains separate from its Arrow carrier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateBindingDeclaration {
    pub intermediate_pattern: Box<str>,
    pub state_format: AggregateStateFormatIdentity,
}

/// One stable overload family. Patterns describe the owner's binder contract;
/// the catalog does not implement a second pattern language or rank candidates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FunctionOverloadDeclaration {
    pub identity: FunctionOverloadId,
    pub argument_pattern: Box<str>,
    pub result_pattern: Box<str>,
    pub aggregate: Option<AggregateBindingDeclaration>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FunctionBindingDeclaration {
    function_id: FunctionId,
    kind: FunctionKind,
    semantics: FunctionSemantics,
    overloads: Box<[FunctionOverloadDeclaration]>,
}

impl FunctionBindingDeclaration {
    pub fn try_new(
        function_id: FunctionId,
        kind: FunctionKind,
        semantics: FunctionSemantics,
        overloads: impl IntoIterator<Item = FunctionOverloadDeclaration>,
    ) -> Result<Self, FunctionBindingError> {
        if !semantics.intrinsic_row_error.is_valid_for_kind(kind) {
            return Err(invalid(
                "intrinsic row-error fact differs from the function kind",
            ));
        }
        let mut overloads = overloads.into_iter().collect::<Vec<_>>();
        if overloads.is_empty() {
            return Err(invalid("function has no declared overloads"));
        }
        overloads.sort_unstable_by(|left, right| left.identity.cmp(&right.identity));
        let mut patterns = BTreeSet::new();
        for (index, overload) in overloads.iter().enumerate() {
            if index > 0 && overloads[index - 1].identity == overload.identity {
                return Err(FunctionBindingError::DuplicateOverload(
                    overload.identity.clone(),
                ));
            }
            validate_pattern(&overload.argument_pattern)?;
            validate_pattern(&overload.result_pattern)?;
            if !patterns.insert(overload.argument_pattern.as_ref()) {
                return Err(invalid(
                    "multiple overloads declare the same argument pattern",
                ));
            }
            if (kind == FunctionKind::Aggregate) != overload.aggregate.is_some() {
                return Err(invalid(
                    "aggregate state declaration differs from the function kind",
                ));
            }
            if let Some(aggregate) = &overload.aggregate {
                validate_pattern(&aggregate.intermediate_pattern)?;
            }
        }
        Ok(Self {
            function_id,
            kind,
            semantics,
            overloads: overloads.into_boxed_slice(),
        })
    }

    pub fn function_id(&self) -> &FunctionId {
        &self.function_id
    }
    pub const fn kind(&self) -> FunctionKind {
        self.kind
    }
    pub const fn semantics(&self) -> FunctionSemantics {
        self.semantics
    }
    pub fn overloads(&self) -> &[FunctionOverloadDeclaration] {
        &self.overloads
    }

    fn overload(
        &self,
        identity: &FunctionOverloadId,
    ) -> Result<&FunctionOverloadDeclaration, FunctionBindingError> {
        self.overloads
            .binary_search_by(|candidate| candidate.identity.cmp(identity))
            .map(|index| &self.overloads[index])
            .map_err(|_| FunctionBindingError::UnknownOverload(identity.clone()))
    }
}

fn validate_pattern(pattern: &str) -> Result<(), FunctionBindingError> {
    if pattern.trim().is_empty() || pattern.len() > u16::MAX as usize {
        return Err(invalid(
            "function signature pattern is empty or exceeds 65535 bytes",
        ));
    }
    Ok(())
}

/// Compile-time scalar values required by literal-dependent binders. The
/// argument's exact Arrow type specifies widths, decimal scales and time units.
/// `None` on FunctionArgument::Value means nonconstant; `Some(Null)` is a
/// constant NULL. Lambdas cannot carry a scalar constant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FunctionLiteral {
    Null,
    Boolean(bool),
    Int64(i64),
    LargeInt(i128),
    UInt64(u64),
    Float64Bits(u64),
    Decimal128(i128),
    Utf8(Box<str>),
    Binary(Box<[u8]>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FunctionArgument {
    Value {
        value_type: FunctionValueType,
        constant: Option<FunctionLiteral>,
    },
    Lambda {
        parameter_types: Box<[FunctionValueType]>,
        result_type: FunctionValueType,
    },
}

impl FunctionArgument {
    pub fn argument_type(&self) -> FunctionArgumentType {
        match self {
            Self::Value { value_type, .. } => FunctionArgumentType::Value(value_type.clone()),
            Self::Lambda {
                parameter_types,
                result_type,
            } => FunctionArgumentType::Lambda {
                parameter_types: parameter_types.clone(),
                result_type: result_type.clone(),
            },
        }
    }

    fn matches_type(&self, expected: &FunctionArgumentType) -> bool {
        match (self, expected) {
            // A parameter that accepts null accepts a value that never writes
            // one, inside a nested type as much as at the top: a map's keys are
            // never null and the signature that takes a map says they may be.
            (Self::Value { value_type, .. }, FunctionArgumentType::Value(expected)) => {
                novarocks_type_contract::fits_nested_nullability(
                    &value_type.data_type,
                    &expected.data_type,
                ) && (expected.nullable || !value_type.nullable)
            }
            (
                Self::Lambda {
                    parameter_types,
                    result_type,
                },
                FunctionArgumentType::Lambda {
                    parameter_types: expected_parameters,
                    result_type: expected_result,
                },
            ) => parameter_types == expected_parameters && result_type == expected_result,
            _ => false,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct FunctionBindingRequest<'a> {
    /// Logical arguments followed by aggregate-owned ORDER BY update channels.
    pub arguments: &'a [FunctionArgument],
    /// Equals arguments.len() for every non-aggregate function.
    pub logical_argument_count: usize,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum FunctionResultType {
    Scalar(FunctionValueType),
    /// Only the function's produced columns, excluding outer pass-throughs.
    Relation(Box<[FunctionValueType]>),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AggregateBindingSelection {
    pub intermediate_type: FunctionValueType,
    pub state_format: AggregateStateFormatIdentity,
}

/// Concrete instantiation of a declared overload, including explicit coercion
/// targets for values and lambda bodies/parameters. The caller must materialize
/// these coercions before exact validation. A value cannot coerce to a lambda,
/// and a lambda cannot gain or lose parameters through coercion.
/// No process handle or implementation pointer is part of this value.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FunctionBindingSelection {
    pub overload: FunctionOverloadId,
    pub argument_types: Box<[FunctionArgumentType]>,
    pub result_type: FunctionResultType,
    pub aggregate: Option<AggregateBindingSelection>,
}

/// FE resolution and BE selected-binding validation are distinct operations.
/// Implementations must validate the requested overload directly; validation
/// must not call resolution to choose a possibly different candidate.
pub trait FunctionBindingResolver: Send + Sync {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError>;

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
    ) -> Result<(), FunctionBindingError>;
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ResolvedFunctionBinding {
    pub function_id: FunctionId,
    pub kind: FunctionKind,
    pub semantics: FunctionSemantics,
    pub logical_argument_count: usize,
    pub selected: FunctionBindingSelection,
}

#[derive(Clone)]
pub(crate) struct FunctionBindingDefinition {
    pub(crate) declaration: FunctionBindingDeclaration,
    resolver: Arc<dyn FunctionBindingResolver>,
}

impl FunctionBindingDefinition {
    pub(crate) const fn new(
        declaration: FunctionBindingDeclaration,
        resolver: Arc<dyn FunctionBindingResolver>,
    ) -> Self {
        Self {
            declaration,
            resolver,
        }
    }
}

/// Derive an aggregate's binding contract from the overloads it already
/// declares.
///
/// An aggregate overload states its identity, what it takes, what it returns
/// and what its state looks like - which is everything a binding declaration
/// holds. Deriving it means the two halves of one function cannot disagree,
/// and that registering an aggregate cannot leave it resolvable through only
/// one of them.
pub(crate) fn parametric_aggregate_binding(
    canonical_name: &str,
    volatility: crate::FunctionVolatility,
    overloads: &[crate::AggregateOverloadDeclaration],
    aggregate_resolver: Arc<dyn crate::AggregateSignatureResolver>,
) -> Result<FunctionBindingDefinition, FunctionCatalogError> {
    let invalid_identity = |error: &dyn fmt::Display| FunctionCatalogError::InvalidStableIdentity {
        subject: "parametric aggregate binding declaration",
        value: error.to_string().into(),
    };
    let function_id = FunctionId::try_new(format!("parametric.aggregate/{canonical_name}/v1"))
        .map_err(|error| invalid_identity(&error))?;
    let declared = overloads
        .iter()
        .map(|overload| {
            Ok(FunctionOverloadDeclaration {
                identity: FunctionOverloadId::try_new(overload.identity.as_str())
                    .map_err(|error| invalid_identity(&error))?,
                argument_pattern: overload.argument_pattern.clone(),
                result_pattern: overload.output_pattern.clone(),
                aggregate: Some(AggregateBindingDeclaration {
                    intermediate_pattern: overload.intermediate_pattern.clone(),
                    state_format: overload.state_format.clone(),
                }),
            })
        })
        .collect::<Result<Vec<_>, FunctionCatalogError>>()?;
    let declaration = FunctionBindingDeclaration::try_new(
        function_id,
        crate::FunctionKind::Aggregate,
        FunctionSemantics {
            volatility,
            argument_evaluation: FunctionArgumentEvaluation::Eager,
            failure_behavior: FunctionFailureBehavior::Propagate,
            intrinsic_row_error:
                novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated,
        },
        declared,
    )
    .map_err(|error| invalid_identity(&error))?;
    Ok(FunctionBindingDefinition::new(
        declaration,
        Arc::new(ParametricAggregateBindingResolver { aggregate_resolver }),
    ))
}

/// Answers binding questions for an aggregate through the same typed contract
/// its signatures are resolved with, so the two can never disagree.
struct ParametricAggregateBindingResolver {
    aggregate_resolver: Arc<dyn crate::AggregateSignatureResolver>,
}

impl ParametricAggregateBindingResolver {
    fn argument_types(
        request: FunctionBindingRequest<'_>,
    ) -> Result<Vec<arrow_schema::DataType>, FunctionBindingError> {
        request
            .arguments
            .iter()
            .map(|argument| match argument {
                FunctionArgument::Value { value_type, .. } => Ok(value_type.data_type.clone()),
                FunctionArgument::Lambda { .. } => Err(FunctionBindingError::NoMatchingOverload),
            })
            .collect()
    }

    fn selection(
        &self,
        request: FunctionBindingRequest<'_>,
        resolved: &crate::ResolvedAggregateSignature,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        let nullable = self.aggregate_resolver.produces_null();
        Ok(FunctionBindingSelection {
            overload: FunctionOverloadId::try_new(resolved.overload.as_str())
                .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))?,
            argument_types: request
                .arguments
                .iter()
                .map(FunctionArgument::argument_type)
                .collect(),
            result_type: FunctionResultType::Scalar(FunctionValueType::new(
                resolved.output_type.clone(),
                nullable,
            )),
            aggregate: Some(crate::AggregateBindingSelection {
                intermediate_type: FunctionValueType::new(
                    resolved.intermediate_type.clone(),
                    nullable,
                ),
                state_format: resolved.state_format.clone(),
            }),
        })
    }
}

impl FunctionBindingResolver for ParametricAggregateBindingResolver {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        let argument_types = Self::argument_types(request)?;
        let logical = self
            .aggregate_resolver
            .resolve_aggregate(&argument_types[..request.logical_argument_count])
            .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))?;
        let resolved = if request.logical_argument_count == argument_types.len() {
            logical
        } else {
            self.aggregate_resolver
                .resolve_update_signature(&logical.overload, &argument_types)
                .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))?
        };
        self.selection(request, &resolved)
    }

    /// Validate the overload that was selected, rather than resolving a fresh
    /// one and accepting whatever comes back.
    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
    ) -> Result<(), FunctionBindingError> {
        let argument_types = Self::argument_types(request)?;
        let selected_overload =
            crate::AggregateOverloadIdentity::try_new(selected.overload.as_str())
                .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))?;
        let resolved = self
            .aggregate_resolver
            .resolve_update_signature(&selected_overload, &argument_types)
            .map_err(|error| FunctionBindingError::InvalidBinding(error.to_string().into()))?;
        if &self.selection(request, &resolved)? == selected {
            Ok(())
        } else {
            Err(FunctionBindingError::InvalidBinding(
                "selected aggregate overload differs from its typed contract".into(),
            ))
        }
    }
}

impl fmt::Debug for FunctionBindingDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FunctionBindingDefinition")
            .field("declaration", &self.declaration)
            .finish_non_exhaustive()
    }
}

impl FunctionDefinition {
    /// Register an explicit binding contract. Legacy resolution APIs reject
    /// this definition rather than discarding its selected identity.
    /// Register a non-aggregate function from its binding declaration.
    ///
    /// An aggregate is refused here on purpose. Resolving one needs a typed
    /// signature contract that this constructor has no way to obtain, and
    /// building an aggregate without one produced a definition that named the
    /// function everywhere but could not be resolved anywhere - which is not a
    /// failure any caller can see until something tries to resolve it. Use
    /// `try_new_bound_aggregate`, which cannot be called without one.
    pub fn try_new_bound(
        canonical_name: impl AsRef<str>,
        visibility: FunctionVisibility,
        declaration: FunctionBindingDeclaration,
        resolver: Arc<dyn FunctionBindingResolver>,
    ) -> Result<Self, FunctionCatalogError> {
        if declaration.kind == crate::FunctionKind::Aggregate {
            return Err(FunctionCatalogError::InvalidStableIdentity {
                subject: "aggregate function without a typed signature contract",
                value: canonical_name.as_ref().into(),
            });
        }
        Self::bound(canonical_name, visibility, declaration, resolver, None)
    }

    /// Register an aggregate from its binding declaration and the contract that
    /// resolves its typed signature.
    pub fn try_new_bound_aggregate(
        canonical_name: impl AsRef<str>,
        visibility: FunctionVisibility,
        declaration: FunctionBindingDeclaration,
        resolver: Arc<dyn FunctionBindingResolver>,
        aggregate_resolver: Arc<dyn crate::AggregateSignatureResolver>,
    ) -> Result<Self, FunctionCatalogError> {
        Self::bound(
            canonical_name,
            visibility,
            declaration,
            resolver,
            Some(aggregate_resolver),
        )
    }

    fn bound(
        canonical_name: impl AsRef<str>,
        visibility: FunctionVisibility,
        declaration: FunctionBindingDeclaration,
        resolver: Arc<dyn FunctionBindingResolver>,
        aggregate_resolver: Option<Arc<dyn crate::AggregateSignatureResolver>>,
    ) -> Result<Self, FunctionCatalogError> {
        let canonical_name = canonical_name.as_ref();
        super::validate_canonical_name(canonical_name)?;
        let aggregate_overloads = declaration
            .overloads
            .iter()
            .filter_map(|overload| {
                overload.aggregate.as_ref().map(|aggregate| {
                    AggregateOverloadDeclaration::try_new(
                        overload.identity.as_str(),
                        &overload.argument_pattern,
                        &aggregate.intermediate_pattern,
                        &overload.result_pattern,
                        aggregate.state_format.as_str(),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let canonical_signatures = declaration
            .overloads
            .iter()
            .map(|overload| overload.argument_pattern.clone())
            .collect();
        Ok(Self {
            canonical_name: canonical_name.into(),
            kind: declaration.kind,
            visibility,
            volatility: declaration.semantics.volatility,
            canonical_signatures,
            aggregate_overloads: aggregate_overloads.into_boxed_slice(),
            exact_aggregate_overloads: Box::default(),
            aggregate_resolver,
            resolver: None,
            binding: Some(FunctionBindingDefinition {
                declaration,
                resolver,
            }),
        })
    }

    pub fn binding_declaration(&self) -> Option<&FunctionBindingDeclaration> {
        self.binding.as_ref().map(|binding| &binding.declaration)
    }
}

impl EngineFunctionCatalogBuilder {
    /// Final-plan consumers require every entry to have an explicit identity.
    /// An incomplete migration cannot silently invent identities or semantics.
    pub fn seal_bound(self) -> Result<EngineFunctionCatalog, FunctionCatalogError> {
        for definition in self.definitions.values() {
            if definition.binding.is_none() {
                return Err(FunctionCatalogError::MissingBindingDeclaration {
                    name: definition.canonical_name.clone(),
                    kind: definition.kind,
                });
            }
        }
        self.seal()
    }
}

impl EngineFunctionCatalog {
    pub fn definition_by_id(&self, identity: &FunctionId) -> Option<&FunctionDefinition> {
        self.identities
            .get(identity)
            .map(|index| &self.definitions[*index])
    }

    pub fn resolve_bound_user(
        &self,
        name: &str,
        kind: FunctionKind,
        request: FunctionBindingRequest<'_>,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        let definition = self
            .definition(name, kind)
            .ok_or(FunctionBindingError::UnknownFunction)?;
        if definition.visibility == FunctionVisibility::Hidden {
            return Err(FunctionBindingError::HiddenFunction);
        }
        resolve_definition(definition, request)
    }

    pub fn resolve_bound_trusted(
        &self,
        name: &str,
        kind: FunctionKind,
        request: FunctionBindingRequest<'_>,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        let definition = self
            .definition(name, kind)
            .ok_or(FunctionBindingError::UnknownFunction)?;
        resolve_definition(definition, request)
    }

    /// Check one frozen binding and its already-coerced argument expressions.
    /// The executable implementation is selected by identity by its owner.
    pub fn validate_bound(
        &self,
        bound: &ResolvedFunctionBinding,
        request: FunctionBindingRequest<'_>,
    ) -> Result<(), FunctionBindingError> {
        let definition = self
            .definition_by_id(&bound.function_id)
            .ok_or(FunctionBindingError::UnknownFunction)?;
        let binding = exact_definition(definition)?;
        if bound.kind != binding.declaration.kind
            || bound.semantics != binding.declaration.semantics
        {
            return Err(invalid(
                "frozen function kind or semantics differ from the declaration",
            ));
        }
        if bound.logical_argument_count != request.logical_argument_count {
            return Err(invalid(
                "frozen logical argument count differs from the expression",
            ));
        }
        validate_selection(&binding.declaration, &bound.selected, request)?;
        if !request
            .arguments
            .iter()
            .zip(bound.selected.argument_types.iter())
            .all(|(argument, expected)| argument.matches_type(expected))
        {
            return Err(invalid(
                "frozen argument types differ from the already-coerced expressions",
            ));
        }
        binding.resolver.validate_selected(&bound.selected, request)
    }
}

fn exact_definition(
    definition: &FunctionDefinition,
) -> Result<&FunctionBindingDefinition, FunctionBindingError> {
    definition
        .binding
        .as_ref()
        .ok_or(FunctionBindingError::MissingBindingDeclaration)
}

fn resolve_definition(
    definition: &FunctionDefinition,
    request: FunctionBindingRequest<'_>,
) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
    let binding = exact_definition(definition)?;
    validate_request(binding.declaration.kind, request)?;
    let selected = binding.resolver.resolve(request)?;
    validate_selection(&binding.declaration, &selected, request)?;
    Ok(ResolvedFunctionBinding {
        function_id: binding.declaration.function_id.clone(),
        kind: binding.declaration.kind,
        semantics: binding.declaration.semantics,
        logical_argument_count: request.logical_argument_count,
        selected,
    })
}

fn validate_request(
    kind: FunctionKind,
    request: FunctionBindingRequest<'_>,
) -> Result<(), FunctionBindingError> {
    if request.logical_argument_count > request.arguments.len()
        || (kind != FunctionKind::Aggregate
            && request.logical_argument_count != request.arguments.len())
    {
        return Err(invalid(
            "logical argument count differs from the function kind or update channels",
        ));
    }
    if request.arguments[request.logical_argument_count..]
        .iter()
        .any(|argument| matches!(argument, FunctionArgument::Lambda { .. }))
    {
        return Err(invalid(
            "aggregate ORDER BY update channels must be values, not lambdas",
        ));
    }
    Ok(())
}

fn validate_selection(
    declaration: &FunctionBindingDeclaration,
    selected: &FunctionBindingSelection,
    request: FunctionBindingRequest<'_>,
) -> Result<(), FunctionBindingError> {
    validate_request(declaration.kind, request)?;
    let overload = declaration.overload(&selected.overload)?;
    if selected.argument_types.len() != request.arguments.len() {
        return Err(invalid(
            "selected signature argument count differs from the request",
        ));
    }
    for (argument, selected_type) in request.arguments.iter().zip(&selected.argument_types) {
        match (argument, selected_type) {
            (FunctionArgument::Value { .. }, FunctionArgumentType::Value(_)) => {}
            (
                FunctionArgument::Lambda {
                    parameter_types, ..
                },
                FunctionArgumentType::Lambda {
                    parameter_types: selected_parameters,
                    ..
                },
            ) if parameter_types.len() == selected_parameters.len() => {}
            _ => {
                return Err(invalid(
                    "selected argument shape or lambda arity differs from the request",
                ));
            }
        }
    }
    if (declaration.kind == FunctionKind::Table)
        != matches!(selected.result_type, FunctionResultType::Relation(_))
    {
        return Err(invalid(
            "selected result shape differs from the function kind",
        ));
    }
    match (&overload.aggregate, &selected.aggregate) {
        (None, None) => {}
        (Some(declared), Some(resolved)) => {
            if resolved.state_format != declared.state_format {
                return Err(invalid(
                    "aggregate state format differs from its declaration",
                ));
            }
        }
        _ => {
            return Err(invalid(
                "selected aggregate state contract differs from the declaration",
            ));
        }
    }
    Ok(())
}

pub(crate) fn digest_binding_definition(
    hasher: &mut Sha256,
    binding: Option<&FunctionBindingDefinition>,
) {
    let Some(binding) = binding else {
        hasher.update([0]);
        return;
    };
    hasher.update([1]);
    let declaration = &binding.declaration;
    digest_text(hasher, declaration.function_id.as_str());
    hasher.update([match declaration.semantics.argument_evaluation {
        FunctionArgumentEvaluation::Eager => 1,
        FunctionArgumentEvaluation::ShortCircuit => 2,
    }]);
    hasher.update([match declaration.semantics.failure_behavior {
        FunctionFailureBehavior::Propagate => 1,
        FunctionFailureBehavior::ReturnsNull => 2,
    }]);
    hasher.update(
        u32::try_from(declaration.overloads.len())
            .expect("overload count fits u32")
            .to_be_bytes(),
    );
    hasher.update([match declaration.semantics.intrinsic_row_error {
        novarocks_type_contract::FunctionIntrinsicRowError::NoRowError => 1,
        novarocks_type_contract::FunctionIntrinsicRowError::MayRaise => 2,
        novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated => 3,
    }]);
    for overload in &declaration.overloads {
        digest_text(hasher, overload.identity.as_str());
        digest_text(hasher, &overload.argument_pattern);
        digest_text(hasher, &overload.result_pattern);
        if let Some(aggregate) = &overload.aggregate {
            hasher.update([1]);
            digest_text(hasher, &aggregate.intermediate_pattern);
            digest_text(hasher, aggregate.state_format.as_str());
        } else {
            hasher.update([0]);
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FunctionBindingError {
    UnknownFunction,
    HiddenFunction,
    MissingBindingDeclaration,
    UnknownOverload(FunctionOverloadId),
    DuplicateOverload(FunctionOverloadId),
    NoMatchingOverload,
    AmbiguousOverload,
    InvalidBinding(Box<str>),
}

fn invalid(message: &str) -> FunctionBindingError {
    FunctionBindingError::InvalidBinding(message.into())
}

impl fmt::Display for FunctionBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownFunction => formatter.write_str("function is not registered"),
            Self::HiddenFunction => formatter.write_str("function is hidden from user SQL"),
            Self::MissingBindingDeclaration => {
                formatter.write_str("function has no exact binding declaration")
            }
            Self::UnknownOverload(identity) => write!(
                formatter,
                "unknown selected overload `{}`",
                identity.as_str()
            ),
            Self::DuplicateOverload(identity) => write!(
                formatter,
                "duplicate overload identity `{}`",
                identity.as_str()
            ),
            Self::NoMatchingOverload => formatter.write_str("no matching declared overload"),
            Self::AmbiguousOverload => {
                formatter.write_str("multiple declared overloads match ambiguously")
            }
            Self::InvalidBinding(message) => {
                write!(formatter, "invalid function binding: {message}")
            }
        }
    }
}

impl std::error::Error for FunctionBindingError {}

#[cfg(test)]
mod tests;
