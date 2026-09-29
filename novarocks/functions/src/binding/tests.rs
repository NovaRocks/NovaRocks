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

use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_schema::{DataType, Field};

use super::*;

fn identity(value: &str) -> FunctionOverloadId {
    FunctionOverloadId::try_new(value).unwrap()
}

fn value_type(data_type: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType {
        data_type,
        nullable,
    }
}

fn argument(data_type: DataType, nullable: bool) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: value_type(data_type, nullable),
        constant: None,
    }
}

fn literal_argument(constant: FunctionLiteral) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: value_type(DataType::Utf8, false),
        constant: Some(constant),
    }
}

fn request(arguments: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
    FunctionBindingRequest {
        arguments,
        logical_argument_count: arguments.len(),
    }
}

fn semantics() -> FunctionSemantics {
    FunctionSemantics {
        volatility: FunctionVolatility::Immutable,
        argument_evaluation: FunctionArgumentEvaluation::Eager,
        failure_behavior: FunctionFailureBehavior::Propagate,
        intrinsic_row_error: novarocks_type_contract::FunctionIntrinsicRowError::NoRowError,
    }
}

fn overload(id: &str, pattern: &str) -> FunctionOverloadDeclaration {
    FunctionOverloadDeclaration {
        identity: identity(id),
        argument_pattern: pattern.into(),
        result_pattern: "T".into(),
        aggregate: None,
    }
}

fn declaration(
    kind: FunctionKind,
    overloads: Vec<FunctionOverloadDeclaration>,
) -> FunctionBindingDeclaration {
    let mut semantics = semantics();
    if matches!(kind, FunctionKind::Aggregate | FunctionKind::Window) {
        semantics.intrinsic_row_error =
            novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated;
    }
    FunctionBindingDeclaration::try_new(
        FunctionId::try_new("test/function/v1").unwrap(),
        kind,
        semantics,
        overloads,
    )
    .unwrap()
}

#[derive(Default)]
struct EchoResolver {
    resolutions: AtomicUsize,
    validations: AtomicUsize,
}

impl FunctionBindingResolver for EchoResolver {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.resolutions.fetch_add(1, Ordering::Relaxed);
        let [FunctionArgument::Value { value_type, .. }] = request.arguments else {
            return Err(FunctionBindingError::NoMatchingOverload);
        };
        Ok(FunctionBindingSelection {
            overload: identity("test/echo/T/v1"),
            argument_types: vec![FunctionArgumentType::Value(value_type.clone())]
                .into_boxed_slice(),
            result_type: FunctionResultType::Scalar(value_type.clone()),
            aggregate: None,
        })
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
    ) -> Result<(), FunctionBindingError> {
        self.validations.fetch_add(1, Ordering::Relaxed);
        let [FunctionArgument::Value { value_type, .. }] = request.arguments else {
            return Err(FunctionBindingError::NoMatchingOverload);
        };
        if selected.overload != identity("test/echo/T/v1")
            || selected.result_type != FunctionResultType::Scalar(value_type.clone())
        {
            return Err(invalid(
                "selected echo signature does not match its concrete input",
            ));
        }
        Ok(())
    }
}

fn catalog(
    resolver: Arc<dyn FunctionBindingResolver>,
    declaration: FunctionBindingDeclaration,
) -> EngineFunctionCatalog {
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            FunctionDefinition::try_new_bound(
                "echo",
                FunctionVisibility::Public,
                declaration,
                resolver,
            )
            .unwrap(),
        )
        .unwrap();
    builder.seal_bound().unwrap()
}

/// An aggregate needs the typed signature contract it is resolved through, so
/// it cannot be registered through the non-aggregate constructor.
fn aggregate_catalog(
    resolver: Arc<AggregateResolver>,
    declaration: FunctionBindingDeclaration,
) -> EngineFunctionCatalog {
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            FunctionDefinition::try_new_bound_aggregate(
                "echo",
                FunctionVisibility::Public,
                declaration,
                Arc::clone(&resolver) as Arc<dyn FunctionBindingResolver>,
                resolver as Arc<dyn crate::AggregateSignatureResolver>,
            )
            .unwrap(),
        )
        .unwrap();
    builder.seal_bound().unwrap()
}

fn echo_catalog(resolver: Arc<EchoResolver>) -> EngineFunctionCatalog {
    catalog(
        resolver,
        declaration(
            FunctionKind::Scalar,
            vec![overload("test/echo/T/v1", "(T)")],
        ),
    )
}

#[test]
fn parametric_identity_is_stable_and_validation_never_resolves_again() {
    let resolver = Arc::new(EchoResolver::default());
    let catalog = echo_catalog(Arc::clone(&resolver));
    let integers = [argument(DataType::Int64, false)];
    let strings = [argument(DataType::Utf8, true)];
    let integer = catalog
        .resolve_bound_user("ECHO", FunctionKind::Scalar, request(&integers))
        .unwrap();
    let string = catalog
        .resolve_bound_user("echo", FunctionKind::Scalar, request(&strings))
        .unwrap();
    assert_eq!(integer.function_id, string.function_id);
    assert_eq!(integer.selected.overload, string.selected.overload);
    assert_ne!(
        integer.selected.argument_types,
        string.selected.argument_types
    );
    catalog
        .validate_bound(&integer, request(&integers))
        .unwrap();
    catalog.validate_bound(&string, request(&strings)).unwrap();
    assert_eq!(resolver.resolutions.load(Ordering::Relaxed), 2);
    assert_eq!(resolver.validations.load(Ordering::Relaxed), 2);
}

#[test]
fn frozen_identity_kind_types_and_semantics_fail_closed() {
    let catalog = echo_catalog(Arc::new(EchoResolver::default()));
    let args = [argument(DataType::Decimal128(12, 3), true)];
    let bound = catalog
        .resolve_bound_user("echo", FunctionKind::Scalar, request(&args))
        .unwrap();
    let assert_rejected =
        |changed| assert!(catalog.validate_bound(&changed, request(&args)).is_err());
    let mut changed = bound.clone();
    changed.function_id = FunctionId::try_new("missing/function/v1").unwrap();
    assert_rejected(changed);
    let mut changed = bound.clone();
    changed.selected.overload = identity("test/different/v1");
    assert_rejected(changed);
    let mut changed = bound.clone();
    changed.kind = FunctionKind::Window;
    assert_rejected(changed);
    let mut changed = bound.clone();
    changed.selected.argument_types[0] =
        FunctionArgumentType::Value(value_type(DataType::Decimal128(12, 3), false));
    assert_rejected(changed);
    let mut changed = bound.clone();
    changed.selected.result_type =
        FunctionResultType::Scalar(value_type(DataType::Decimal128(12, 2), true));
    assert_rejected(changed);
    let mut changed = bound.clone();
    changed.semantics.volatility = FunctionVolatility::Stable;
    assert_rejected(changed);
    let mut changed = bound.clone();
    changed.semantics.argument_evaluation = FunctionArgumentEvaluation::ShortCircuit;
    assert_rejected(changed);
    let mut changed = bound;
    changed.semantics.failure_behavior = FunctionFailureBehavior::ReturnsNull;
    assert_rejected(changed);
}

struct NamedStructResolver;

impl NamedStructResolver {
    fn selection(
        request: FunctionBindingRequest<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        let [
            name,
            FunctionArgument::Value {
                value_type: field_type,
                ..
            },
        ] = request.arguments
        else {
            return Err(FunctionBindingError::NoMatchingOverload);
        };
        let FunctionArgument::Value {
            constant: Some(FunctionLiteral::Utf8(name)),
            ..
        } = name
        else {
            return Err(invalid("field name must be a compile-time string"));
        };
        let field = Field::new(
            name.as_ref(),
            field_type.data_type.clone(),
            field_type.nullable,
        );
        Ok(FunctionBindingSelection {
            overload: identity("test/named-struct/T/v1"),
            argument_types: request
                .arguments
                .iter()
                .map(FunctionArgument::argument_type)
                .collect(),
            result_type: FunctionResultType::Scalar(value_type(
                DataType::Struct(vec![Arc::new(field)].into()),
                false,
            )),
            aggregate: None,
        })
    }
}

impl FunctionBindingResolver for NamedStructResolver {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        Self::selection(request)
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
    ) -> Result<(), FunctionBindingError> {
        if selected != &Self::selection(request)? {
            return Err(invalid(
                "selected field schema differs from its literal argument",
            ));
        }
        Ok(())
    }
}

#[test]
fn literal_dependent_binding_distinguishes_constant_null_nonconstant_and_changed_field_name() {
    let catalog = catalog(
        Arc::new(NamedStructResolver),
        declaration(
            FunctionKind::Scalar,
            vec![overload("test/named-struct/T/v1", "(constant string, T)")],
        ),
    );
    let mut args = [
        argument(DataType::Utf8, false),
        argument(DataType::Int64, false),
    ];
    assert!(
        catalog
            .resolve_bound_user("echo", FunctionKind::Scalar, request(&args))
            .is_err()
    );
    args[0] = literal_argument(FunctionLiteral::Null);
    assert!(
        catalog
            .resolve_bound_user("echo", FunctionKind::Scalar, request(&args))
            .is_err()
    );
    args[0] = literal_argument(FunctionLiteral::Utf8("field_a".into()));
    let bound = catalog
        .resolve_bound_user("echo", FunctionKind::Scalar, request(&args))
        .unwrap();
    catalog.validate_bound(&bound, request(&args)).unwrap();
    args[0] = literal_argument(FunctionLiteral::Utf8("field_b".into()));
    assert!(catalog.validate_bound(&bound, request(&args)).is_err());
}

#[test]
fn catalog_digest_covers_explicit_binding_contract_and_ignores_registration_order() {
    let first = overload("test/echo/T/v1", "(T)");
    let second = overload("test/echo/zero/v1", "()");
    let base = declaration(FunctionKind::Scalar, vec![first.clone(), second.clone()]);
    let reversed = declaration(FunctionKind::Scalar, vec![second, first]);
    let digest = |declaration| catalog(Arc::new(EchoResolver::default()), declaration).digest();
    let baseline = digest(base.clone());
    assert_eq!(baseline, digest(reversed));
    for field in 0..8 {
        let mut changed = base.clone();
        match field {
            0 => changed.function_id = FunctionId::try_new("test/function/v2").unwrap(),
            1 => changed.overloads[0].identity = identity("test/echo/T/v2"),
            2 => changed.semantics.volatility = FunctionVolatility::Stable,
            3 => changed.semantics.argument_evaluation = FunctionArgumentEvaluation::ShortCircuit,
            4 => changed.semantics.failure_behavior = FunctionFailureBehavior::ReturnsNull,
            5 => changed.overloads[0].argument_pattern = "(U)".into(),
            6 => changed.overloads[0].result_pattern = "U".into(),
            7 => {
                changed.semantics.intrinsic_row_error =
                    novarocks_type_contract::FunctionIntrinsicRowError::MayRaise
            }
            _ => unreachable!(),
        }
        assert_ne!(baseline, digest(changed), "field {field}");
    }
}

#[test]
fn declarations_reject_duplicate_identity_ambiguous_patterns_and_wrong_state_kind() {
    let make = |overloads| {
        FunctionBindingDeclaration::try_new(
            FunctionId::try_new("test/function/v1").unwrap(),
            FunctionKind::Scalar,
            semantics(),
            overloads,
        )
    };
    let first = overload("test/echo/T/v1", "(T)");
    assert!(matches!(
        make(vec![first.clone(), first.clone()]),
        Err(FunctionBindingError::DuplicateOverload(_))
    ));
    assert!(make(vec![first.clone(), overload("other/v1", "(T)")]).is_err());
    let mut aggregate = first;
    aggregate.aggregate = Some(AggregateBindingDeclaration {
        intermediate_pattern: "binary".into(),
        state_format: AggregateStateFormatIdentity::try_new("state/v1").unwrap(),
    });
    assert!(make(vec![aggregate]).is_err());
    assert!(make(Vec::new()).is_err());
}

#[test]
fn duplicate_function_identity_is_rejected_atomically() {
    let declaration = declaration(
        FunctionKind::Scalar,
        vec![overload("test/echo/T/v1", "(T)")],
    );
    let mut builder = EngineFunctionCatalogBuilder::new();
    let definition = |name| {
        FunctionDefinition::try_new_bound(
            name,
            FunctionVisibility::Public,
            declaration.clone(),
            Arc::new(EchoResolver::default()),
        )
        .unwrap()
    };
    builder.register(definition("first")).unwrap();
    assert!(matches!(
        builder.register(definition("second")),
        Err(FunctionCatalogError::DuplicateFunctionIdentity { .. })
    ));
    assert_eq!(builder.seal_bound().unwrap().definitions().len(), 1);
}

#[test]
fn canonical_name_is_bounded_before_catalog_digest() {
    let declaration = declaration(
        FunctionKind::Scalar,
        vec![overload("test/echo/T/v1", "(T)")],
    );
    assert!(matches!(
        FunctionDefinition::try_new_bound(
            "x".repeat(u16::MAX as usize + 1),
            FunctionVisibility::Public,
            declaration,
            Arc::new(EchoResolver::default())
        ),
        Err(FunctionCatalogError::InvalidCanonicalName { .. })
    ));
}

#[test]
fn hidden_bound_functions_require_trusted_resolution_and_legacy_calls_do_not_discard_identity() {
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            FunctionDefinition::try_new_bound(
                "hidden",
                FunctionVisibility::Hidden,
                declaration(
                    FunctionKind::Scalar,
                    vec![overload("test/echo/T/v1", "(T)")],
                ),
                Arc::new(EchoResolver::default()),
            )
            .unwrap(),
        )
        .unwrap();
    let catalog = builder.seal_bound().unwrap();
    let args = [argument(DataType::Int64, false)];
    assert_eq!(
        catalog.resolve_bound_user("hidden", FunctionKind::Scalar, request(&args)),
        Err(FunctionBindingError::HiddenFunction)
    );
    assert!(
        catalog
            .resolve_bound_trusted("hidden", FunctionKind::Scalar, request(&args))
            .is_ok()
    );
    assert!(
        catalog
            .resolve_trusted("hidden", FunctionKind::Scalar, &[DataType::Int64])
            .is_err()
    );
}

struct AggregateResolver;

impl crate::AggregateSignatureResolver for AggregateResolver {
    fn resolve_aggregate(
        &self,
        argument_types: &[DataType],
    ) -> Result<crate::ResolvedAggregateSignature, crate::FunctionResolutionError> {
        Ok(crate::ResolvedAggregateSignature {
            overload: crate::AggregateOverloadIdentity::try_new("test/aggregate/T/v1").unwrap(),
            argument_types: argument_types.to_vec(),
            intermediate_type: DataType::Binary,
            output_type: DataType::Int64,
            state_format: AggregateStateFormatIdentity::try_new("state/v1").unwrap(),
        })
    }
}

impl FunctionBindingResolver for AggregateResolver {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        if request.logical_argument_count != 1 || request.arguments.is_empty() {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        Ok(FunctionBindingSelection {
            overload: identity("test/aggregate/T/v1"),
            argument_types: request
                .arguments
                .iter()
                .map(FunctionArgument::argument_type)
                .collect(),
            result_type: FunctionResultType::Scalar(value_type(DataType::Int64, true)),
            aggregate: Some(AggregateBindingSelection {
                intermediate_type: value_type(DataType::Binary, false),
                state_format: AggregateStateFormatIdentity::try_new("state/v1").unwrap(),
            }),
        })
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
    ) -> Result<(), FunctionBindingError> {
        if request.logical_argument_count != 1
            || selected
                .aggregate
                .as_ref()
                .map(|aggregate| &aggregate.intermediate_type)
                != Some(&value_type(DataType::Binary, false))
        {
            return Err(invalid(
                "aggregate logical arity or selected intermediate differs",
            ));
        }
        Ok(())
    }
}

#[test]
fn aggregate_binding_preserves_state_format_intermediate_nullability_and_logical_arity() {
    let mut overload = overload("test/aggregate/T/v1", "(T; order_by...)");
    overload.aggregate = Some(AggregateBindingDeclaration {
        intermediate_pattern: "binary not null".into(),
        state_format: AggregateStateFormatIdentity::try_new("state/v1").unwrap(),
    });
    let catalog = aggregate_catalog(
        Arc::new(AggregateResolver),
        declaration(FunctionKind::Aggregate, vec![overload]),
    );
    let args = [
        argument(DataType::Int64, true),
        argument(DataType::Utf8, true),
    ];
    let request = FunctionBindingRequest {
        arguments: &args,
        logical_argument_count: 1,
    };
    let bound = catalog
        .resolve_bound_user("echo", FunctionKind::Aggregate, request)
        .unwrap();
    catalog.validate_bound(&bound, request).unwrap();
    let mut changed = bound.clone();
    changed.selected.aggregate.as_mut().unwrap().state_format =
        AggregateStateFormatIdentity::try_new("state/v2").unwrap();
    assert!(catalog.validate_bound(&changed, request).is_err());
    let mut changed = bound.clone();
    changed
        .selected
        .aggregate
        .as_mut()
        .unwrap()
        .intermediate_type
        .nullable = true;
    assert!(catalog.validate_bound(&changed, request).is_err());
    let changed_request = FunctionBindingRequest {
        logical_argument_count: 2,
        ..request
    };
    assert!(catalog.validate_bound(&bound, changed_request).is_err());
    assert!(
        catalog
            .resolve_bound_user("echo", FunctionKind::Aggregate, changed_request)
            .is_err()
    );
}

#[test]
fn bound_catalog_rejects_unmigrated_definitions_without_inventing_identities() {
    struct LegacyResolver;
    impl crate::FunctionSignatureResolver for LegacyResolver {
        fn resolve(
            &self,
            arguments: &[DataType],
        ) -> Result<crate::ResolvedFunctionSignature, crate::FunctionResolutionError> {
            Ok(crate::ResolvedFunctionSignature {
                argument_types: arguments.to_vec(),
                return_type: DataType::Int64,
                enforce_argument_binding: true,
            })
        }
    }
    let definition = FunctionDefinition::try_new(
        "legacy",
        FunctionKind::Aggregate,
        FunctionVisibility::Public,
        FunctionVolatility::Immutable,
        ["(int64)->int64"],
        Arc::new(LegacyResolver),
    )
    .unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(definition.clone()).unwrap();
    assert!(matches!(
        builder.seal_bound(),
        Err(FunctionCatalogError::MissingBindingDeclaration { .. })
    ));
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(definition).unwrap();
    let catalog = builder.seal().unwrap();
    let args = [argument(DataType::Int64, false)];
    assert_eq!(
        catalog.resolve_bound_user("legacy", FunctionKind::Aggregate, request(&args)),
        Err(FunctionBindingError::MissingBindingDeclaration)
    );
}

struct FixedResolver(FunctionBindingSelection);

impl FunctionBindingResolver for FixedResolver {
    fn resolve(
        &self,
        _request: FunctionBindingRequest<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        Ok(self.0.clone())
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        _request: FunctionBindingRequest<'_>,
    ) -> Result<(), FunctionBindingError> {
        if selected != &self.0 {
            return Err(invalid("selected fixed signature differs"));
        }
        Ok(())
    }
}

#[test]
fn worker_validation_requires_explicitly_coerced_argument_types() {
    let target = value_type(DataType::Int32, false);
    let selected = FunctionBindingSelection {
        overload: identity("test/echo/T/v1"),
        argument_types: vec![FunctionArgumentType::Value(target.clone())].into_boxed_slice(),
        result_type: FunctionResultType::Scalar(target),
        aggregate: None,
    };
    let catalog = catalog(
        Arc::new(FixedResolver(selected)),
        declaration(
            FunctionKind::Scalar,
            vec![overload("test/echo/T/v1", "(int32)")],
        ),
    );
    let original = [argument(DataType::Int64, false)];
    let coerced = [argument(DataType::Int32, false)];
    let bound = catalog
        .resolve_bound_user("echo", FunctionKind::Scalar, request(&original))
        .unwrap();
    assert!(catalog.validate_bound(&bound, request(&original)).is_err());
    catalog.validate_bound(&bound, request(&coerced)).unwrap();
}

fn higher_order_catalog(arguments: &[FunctionArgument]) -> EngineFunctionCatalog {
    let result = value_type(
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
        false,
    );
    let selected = FunctionBindingSelection {
        overload: identity("test/array-map/T-U/v1"),
        argument_types: arguments
            .iter()
            .map(FunctionArgument::argument_type)
            .collect(),
        result_type: FunctionResultType::Scalar(result),
        aggregate: None,
    };
    catalog(
        Arc::new(FixedResolver(selected)),
        declaration(
            FunctionKind::Scalar,
            vec![overload(
                "test/array-map/T-U/v1",
                "(lambda(T)->U, array<T>)",
            )],
        ),
    )
}

fn higher_order_arguments() -> [FunctionArgument; 2] {
    [
        FunctionArgument::Lambda {
            parameter_types: vec![value_type(DataType::Int64, true)].into_boxed_slice(),
            result_type: value_type(DataType::Utf8, false),
        },
        argument(
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            false,
        ),
    ]
}

#[test]
fn higher_order_binding_rejects_scalar_impersonation_in_both_directions() {
    let arguments = higher_order_arguments();
    let catalog = higher_order_catalog(&arguments);
    let bound = catalog
        .resolve_bound_user("echo", FunctionKind::Scalar, request(&arguments))
        .unwrap();
    catalog.validate_bound(&bound, request(&arguments)).unwrap();

    // The scalar has the lambda body's exact result type and nullability.
    let mut scalar_arguments = arguments.clone();
    scalar_arguments[0] = argument(DataType::Utf8, false);
    assert!(
        catalog
            .validate_bound(&bound, request(&scalar_arguments))
            .is_err()
    );
    assert!(
        catalog
            .resolve_bound_user("echo", FunctionKind::Scalar, request(&scalar_arguments))
            .is_err()
    );

    let scalar_catalog = higher_order_catalog(&scalar_arguments);
    let scalar_bound = scalar_catalog
        .resolve_bound_user("echo", FunctionKind::Scalar, request(&scalar_arguments))
        .unwrap();
    assert!(
        scalar_catalog
            .validate_bound(&scalar_bound, request(&arguments))
            .is_err()
    );
    assert!(
        scalar_catalog
            .resolve_bound_user("echo", FunctionKind::Scalar, request(&arguments))
            .is_err()
    );
}

#[test]
fn higher_order_binding_freezes_lambda_arity_parameter_types_and_result_type() {
    let arguments = higher_order_arguments();
    let catalog = higher_order_catalog(&arguments);
    let bound = catalog
        .resolve_bound_user("echo", FunctionKind::Scalar, request(&arguments))
        .unwrap();
    for (parameters, result) in [
        (vec![], value_type(DataType::Utf8, false)),
        (
            vec![value_type(DataType::Int64, true); 2],
            value_type(DataType::Utf8, false),
        ),
        (
            vec![value_type(DataType::Int32, true)],
            value_type(DataType::Utf8, false),
        ),
        (
            vec![value_type(DataType::Int64, false)],
            value_type(DataType::Utf8, false),
        ),
        (
            vec![value_type(DataType::Int64, true)],
            value_type(DataType::Int64, false),
        ),
        (
            vec![value_type(DataType::Int64, true)],
            value_type(DataType::Utf8, true),
        ),
    ] {
        let mut changed = arguments.clone();
        let changed_arity = parameters.len() != 1;
        changed[0] = FunctionArgument::Lambda {
            parameter_types: parameters.into_boxed_slice(),
            result_type: result,
        };
        assert!(catalog.validate_bound(&bound, request(&changed)).is_err());
        if changed_arity {
            assert!(
                catalog
                    .resolve_bound_user("echo", FunctionKind::Scalar, request(&changed))
                    .is_err()
            );
        }
    }
}

#[test]
fn aggregate_order_by_update_channels_cannot_be_lambdas() {
    let arguments = [
        argument(DataType::Int64, true),
        higher_order_arguments()[0].clone(),
    ];
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: 1,
    };
    assert!(validate_request(FunctionKind::Aggregate, request).is_err());
}

#[test]
fn undeclared_selected_overload_is_rejected_during_resolution() {
    let target = value_type(DataType::Int64, false);
    let selected = FunctionBindingSelection {
        overload: identity("test/undeclared/v1"),
        argument_types: vec![FunctionArgumentType::Value(target.clone())].into_boxed_slice(),
        result_type: FunctionResultType::Scalar(target),
        aggregate: None,
    };
    let catalog = catalog(
        Arc::new(FixedResolver(selected)),
        declaration(
            FunctionKind::Scalar,
            vec![overload("test/echo/T/v1", "(int64)")],
        ),
    );
    let args = [argument(DataType::Int64, false)];
    assert!(matches!(
        catalog.resolve_bound_user("echo", FunctionKind::Scalar, request(&args)),
        Err(FunctionBindingError::UnknownOverload(_))
    ));
}

#[test]
fn table_and_window_bindings_have_distinct_result_shapes() {
    let scalar = value_type(DataType::Int64, false);
    let relation = vec![scalar.clone(), value_type(DataType::Utf8, true)].into_boxed_slice();
    for kind in [FunctionKind::Table, FunctionKind::Window] {
        let result_type = if kind == FunctionKind::Table {
            FunctionResultType::Relation(relation.clone())
        } else {
            FunctionResultType::Scalar(scalar.clone())
        };
        let selected = FunctionBindingSelection {
            overload: identity("test/result/v1"),
            argument_types: Box::default(),
            result_type,
            aggregate: None,
        };
        let catalog = catalog(
            Arc::new(FixedResolver(selected)),
            declaration(kind, vec![overload("test/result/v1", "()")]),
        );
        let bound = catalog
            .resolve_bound_user("echo", kind, request(&[]))
            .unwrap();
        catalog.validate_bound(&bound, request(&[])).unwrap();
        let mut changed = bound.clone();
        changed.selected.result_type = if kind == FunctionKind::Table {
            FunctionResultType::Scalar(scalar.clone())
        } else {
            FunctionResultType::Relation(relation.clone())
        };
        assert!(catalog.validate_bound(&changed, request(&[])).is_err());
        let mut changed = bound;
        changed.kind = if kind == FunctionKind::Table {
            FunctionKind::Window
        } else {
            FunctionKind::Table
        };
        assert!(catalog.validate_bound(&changed, request(&[])).is_err());
    }
}

#[test]
fn intrinsic_row_error_is_independent_of_catching_and_closed_by_function_kind() {
    use novarocks_type_contract::FunctionIntrinsicRowError as Own;
    for kind in [
        FunctionKind::Scalar,
        FunctionKind::Table,
        FunctionKind::Aggregate,
        FunctionKind::Window,
    ] {
        for own in [Own::NoRowError, Own::MayRaise, Own::NotRowEvaluated] {
            let mut semantics = semantics();
            semantics.failure_behavior = FunctionFailureBehavior::ReturnsNull;
            semantics.intrinsic_row_error = own;
            let mut selected = overload("test/intrinsic/T/v1", "(T)");
            if kind == FunctionKind::Aggregate {
                selected.aggregate = Some(AggregateBindingDeclaration {
                    intermediate_pattern: "binary".into(),
                    state_format: AggregateStateFormatIdentity::try_new("test/intrinsic/state-v1")
                        .unwrap(),
                });
            }
            let result = FunctionBindingDeclaration::try_new(
                FunctionId::try_new("test/intrinsic/v1").unwrap(),
                kind,
                semantics,
                [selected],
            );
            assert_eq!(
                result.is_ok(),
                own.is_valid_for_kind(kind),
                "{kind:?}/{own:?}"
            );
        }
    }
    let declaration = declaration(
        FunctionKind::Scalar,
        vec![overload("test/echo/T/v1", "(T)")],
    );
    let catalog = catalog(Arc::new(EchoResolver::default()), declaration);
    let arguments = [argument(DataType::Int32, false)];
    let original = catalog
        .resolve_bound_user("echo", FunctionKind::Scalar, request(&arguments))
        .unwrap();
    let mut forged = original.clone();
    forged.semantics.intrinsic_row_error = Own::MayRaise;
    assert!(
        catalog
            .validate_bound(&forged, request(&arguments))
            .is_err()
    );
}
