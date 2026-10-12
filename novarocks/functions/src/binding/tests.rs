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
    FunctionValueType::new(data_type, nullable)
}

fn argument(data_type: DataType, nullable: bool) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: value_type(data_type, nullable),
        constant: None,
    }
}

fn literal_argument(constant: crate::ConstantValue) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: constant.value_type().clone(),
        constant: Some(constant),
    }
}

fn constant_policy() -> crate::ConstantPolicy {
    crate::ConstantPolicy {
        max_rows: 4096,
        max_array_nodes: 64,
        max_logical_elements: 16384,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 64,
        max_dictionary_depth: 8,
        max_metadata_bytes: 65536,
        max_library_validation_work: 4 << 20,
        max_library_validation_bytes: 4 << 20,
    }
}
fn text_constant(text: Option<&str>) -> crate::ConstantValue {
    let ty = value_type(DataType::Utf8, text.is_none());
    let field = Arc::new(ty.try_to_field("fixture").unwrap());
    match text {
        Some(text) => crate::ConstantValue::from_utf8(
            field,
            ty,
            text,
            constant_policy(),
            CompilePhase::FunctionSpecialization,
            crate::binding_test_control(),
        ),
        None => crate::ConstantValue::null(
            field,
            ty,
            constant_policy(),
            CompilePhase::FunctionSpecialization,
            crate::binding_test_control(),
        ),
    }
    .unwrap()
}

fn request(arguments: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
    FunctionBindingRequest {
        expected_result_type: None,
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
        effects: None,
        semantics: semantics(),
        identity: identity(id),
        argument_pattern: pattern.into(),
        result_pattern: "T".into(),
        aggregate: None,
    }
}

fn declaration(
    kind: FunctionKind,
    mut overloads: Vec<FunctionOverloadDeclaration>,
) -> FunctionBindingDeclaration {
    if matches!(kind, FunctionKind::Aggregate | FunctionKind::Window) {
        for overload in &mut overloads {
            overload.semantics.intrinsic_row_error =
                novarocks_type_contract::FunctionIntrinsicRowError::NotRowEvaluated;
        }
    }
    FunctionBindingDeclaration::try_new(
        FunctionId::try_new("test/function/v1").unwrap(),
        kind,
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
        _control: &dyn novarocks_type_contract::PureCompileControl,
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
        _control: &dyn novarocks_type_contract::PureCompileControl,
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
        .resolve_bound_user(
            "ECHO",
            FunctionKind::Scalar,
            request(&integers),
            crate::binding_test_control(),
        )
        .unwrap();
    let string = catalog
        .resolve_bound_user(
            "echo",
            FunctionKind::Scalar,
            request(&strings),
            crate::binding_test_control(),
        )
        .unwrap();
    assert_eq!(integer.function_id, string.function_id);
    assert_eq!(integer.selected.overload, string.selected.overload);
    assert_ne!(
        integer.selected.argument_types,
        string.selected.argument_types
    );
    catalog
        .validate_bound(&integer, request(&integers), crate::binding_test_control())
        .unwrap();
    catalog
        .validate_bound(&string, request(&strings), crate::binding_test_control())
        .unwrap();
    assert_eq!(resolver.resolutions.load(Ordering::Relaxed), 2);
    assert_eq!(resolver.validations.load(Ordering::Relaxed), 2);
}

#[test]
fn frozen_identity_kind_types_and_semantics_fail_closed() {
    let catalog = echo_catalog(Arc::new(EchoResolver::default()));
    let args = [argument(DataType::Decimal128(12, 3), true)];
    let bound = catalog
        .resolve_bound_user(
            "echo",
            FunctionKind::Scalar,
            request(&args),
            crate::binding_test_control(),
        )
        .unwrap();
    let assert_rejected = |changed| {
        assert!(
            catalog
                .validate_bound(&changed, request(&args), crate::binding_test_control())
                .is_err()
        )
    };
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
        control: &dyn PureCompileControl,
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
            constant: Some(value),
            ..
        } = name
        else {
            return Err(invalid("field name must be a compile-time string"));
        };
        let Some(name) = value.utf8_observed(CompilePhase::FunctionSpecialization, control)? else {
            return Err(invalid("field name must be a compile-time string"));
        };
        let field = Field::new(name, field_type.data_type.clone(), field_type.nullable);
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
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        Self::selection(request, control)
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        if selected != &Self::selection(request, control)? {
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
            .resolve_bound_user(
                "echo",
                FunctionKind::Scalar,
                request(&args),
                crate::binding_test_control()
            )
            .is_err()
    );
    args[0] = literal_argument(text_constant(None));
    assert!(
        catalog
            .resolve_bound_user(
                "echo",
                FunctionKind::Scalar,
                request(&args),
                crate::binding_test_control()
            )
            .is_err()
    );
    args[0] = literal_argument(text_constant(Some("field_a")));
    let bound = catalog
        .resolve_bound_user(
            "echo",
            FunctionKind::Scalar,
            request(&args),
            crate::binding_test_control(),
        )
        .unwrap();
    catalog
        .validate_bound(&bound, request(&args), crate::binding_test_control())
        .unwrap();
    args[0] = literal_argument(text_constant(Some("field_b")));
    assert!(
        catalog
            .validate_bound(&bound, request(&args), crate::binding_test_control())
            .is_err()
    );
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
            2 => changed.overloads[0].semantics.volatility = FunctionVolatility::Stable,
            3 => {
                changed.overloads[0].semantics.argument_evaluation =
                    FunctionArgumentEvaluation::ShortCircuit
            }
            4 => {
                changed.overloads[0].semantics.failure_behavior =
                    FunctionFailureBehavior::ReturnsNull
            }
            5 => changed.overloads[0].argument_pattern = "(U)".into(),
            6 => changed.overloads[0].result_pattern = "U".into(),
            7 => {
                changed.overloads[0].semantics.intrinsic_row_error =
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
        state_argument_contract:
            novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
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
        catalog.resolve_bound_user(
            "hidden",
            FunctionKind::Scalar,
            request(&args),
            crate::binding_test_control()
        ),
        Err(FunctionBindingError::HiddenFunction)
    );
    assert!(
        catalog
            .resolve_bound_trusted(
                "hidden",
                FunctionKind::Scalar,
                request(&args),
                crate::binding_test_control()
            )
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
        _control: &dyn novarocks_type_contract::PureCompileControl,
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
                state_argument_contract:
                    novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
                intermediate_type: value_type(DataType::Binary, false),
                state_format: AggregateStateFormatIdentity::try_new("state/v1").unwrap(),
            }),
        })
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
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
        state_argument_contract:
            novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
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
        expected_result_type: None,
        arguments: &args,
        logical_argument_count: 1,
    };
    let bound = catalog
        .resolve_bound_user(
            "echo",
            FunctionKind::Aggregate,
            request,
            crate::binding_test_control(),
        )
        .unwrap();
    catalog
        .validate_bound(&bound, request, crate::binding_test_control())
        .unwrap();
    let mut changed = bound.clone();
    changed.selected.aggregate.as_mut().unwrap().state_format =
        AggregateStateFormatIdentity::try_new("state/v2").unwrap();
    assert!(
        catalog
            .validate_bound(&changed, request, crate::binding_test_control())
            .is_err()
    );
    let mut changed = bound.clone();
    changed
        .selected
        .aggregate
        .as_mut()
        .unwrap()
        .intermediate_type
        .nullable = true;
    assert!(
        catalog
            .validate_bound(&changed, request, crate::binding_test_control())
            .is_err()
    );
    let changed_request = FunctionBindingRequest {
        expected_result_type: None,
        logical_argument_count: 2,
        ..request
    };
    assert!(
        catalog
            .validate_bound(&bound, changed_request, crate::binding_test_control())
            .is_err()
    );
    assert!(
        catalog
            .resolve_bound_user(
                "echo",
                FunctionKind::Aggregate,
                changed_request,
                crate::binding_test_control()
            )
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
        catalog.resolve_bound_user(
            "legacy",
            FunctionKind::Aggregate,
            request(&args),
            crate::binding_test_control()
        ),
        Err(FunctionBindingError::MissingBindingDeclaration)
    );
}

struct FixedResolver(FunctionBindingSelection);

#[test]
fn selected_overload_semantics_are_exact_with_conservative_name_metadata() {
    let mut stable = overload("test/stable/v1", "(Int32)");
    stable.semantics.volatility = FunctionVolatility::Stable;
    let mut volatile = overload("test/volatile/v1", "(Int64)");
    volatile.semantics.volatility = FunctionVolatility::Volatile;
    volatile.semantics.intrinsic_row_error =
        novarocks_type_contract::FunctionIntrinsicRowError::MayRaise;
    volatile.semantics.argument_evaluation = FunctionArgumentEvaluation::ShortCircuit;
    for (chosen, data_type) in [(&stable, DataType::Int32), (&volatile, DataType::Int64)] {
        let declaration = declaration(FunctionKind::Scalar, vec![stable.clone(), volatile.clone()]);
        assert_eq!(declaration.volatility(), FunctionVolatility::Volatile);
        let ty = value_type(data_type.clone(), false);
        let selection = FunctionBindingSelection {
            overload: chosen.identity.clone(),
            argument_types: Box::from([FunctionArgumentType::Value(ty.clone())]),
            result_type: FunctionResultType::Scalar(ty),
            aggregate: None,
        };
        let catalog = catalog(Arc::new(FixedResolver(selection)), declaration);
        let args = [argument(data_type, false)];
        let bound = catalog
            .resolve_bound_user(
                "echo",
                FunctionKind::Scalar,
                request(&args),
                crate::binding_test_control(),
            )
            .unwrap();
        assert_eq!(bound.semantics, chosen.semantics);
        catalog
            .validate_bound(&bound, request(&args), crate::binding_test_control())
            .unwrap();
        let mut forged = bound;
        forged.semantics = if chosen == &stable {
            volatile.semantics
        } else {
            stable.semantics
        };
        assert!(
            catalog
                .validate_bound(&forged, request(&args), crate::binding_test_control())
                .is_err()
        );
    }
    let first = declaration(FunctionKind::Scalar, vec![stable.clone(), volatile.clone()]);
    let digest = catalog(Arc::new(EchoResolver::default()), first).digest();
    stable.semantics.volatility = FunctionVolatility::Immutable;
    let changed = declaration(FunctionKind::Scalar, vec![stable, volatile]);
    assert_eq!(changed.volatility(), FunctionVolatility::Volatile);
    assert_ne!(
        digest,
        catalog(Arc::new(EchoResolver::default()), changed).digest()
    );
}

impl FunctionBindingResolver for FixedResolver {
    fn resolve(
        &self,
        _request: FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        Ok(self.0.clone())
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        _request: FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
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
        .resolve_bound_user(
            "echo",
            FunctionKind::Scalar,
            request(&original),
            crate::binding_test_control(),
        )
        .unwrap();
    assert!(
        catalog
            .validate_bound(&bound, request(&original), crate::binding_test_control())
            .is_err()
    );
    catalog
        .validate_bound(&bound, request(&coerced), crate::binding_test_control())
        .unwrap();
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
        .resolve_bound_user(
            "echo",
            FunctionKind::Scalar,
            request(&arguments),
            crate::binding_test_control(),
        )
        .unwrap();
    catalog
        .validate_bound(&bound, request(&arguments), crate::binding_test_control())
        .unwrap();

    // The scalar has the lambda body's exact result type and nullability.
    let mut scalar_arguments = arguments.clone();
    scalar_arguments[0] = argument(DataType::Utf8, false);
    assert!(
        catalog
            .validate_bound(
                &bound,
                request(&scalar_arguments),
                crate::binding_test_control()
            )
            .is_err()
    );
    assert!(
        catalog
            .resolve_bound_user(
                "echo",
                FunctionKind::Scalar,
                request(&scalar_arguments),
                crate::binding_test_control()
            )
            .is_err()
    );

    let scalar_catalog = higher_order_catalog(&scalar_arguments);
    let scalar_bound = scalar_catalog
        .resolve_bound_user(
            "echo",
            FunctionKind::Scalar,
            request(&scalar_arguments),
            crate::binding_test_control(),
        )
        .unwrap();
    assert!(
        scalar_catalog
            .validate_bound(
                &scalar_bound,
                request(&arguments),
                crate::binding_test_control()
            )
            .is_err()
    );
    assert!(
        scalar_catalog
            .resolve_bound_user(
                "echo",
                FunctionKind::Scalar,
                request(&arguments),
                crate::binding_test_control()
            )
            .is_err()
    );
}

#[test]
fn higher_order_binding_freezes_lambda_arity_parameter_types_and_result_type() {
    let arguments = higher_order_arguments();
    let catalog = higher_order_catalog(&arguments);
    let bound = catalog
        .resolve_bound_user(
            "echo",
            FunctionKind::Scalar,
            request(&arguments),
            crate::binding_test_control(),
        )
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
        assert!(
            catalog
                .validate_bound(&bound, request(&changed), crate::binding_test_control())
                .is_err()
        );
        if changed_arity {
            assert!(
                catalog
                    .resolve_bound_user(
                        "echo",
                        FunctionKind::Scalar,
                        request(&changed),
                        crate::binding_test_control()
                    )
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
        expected_result_type: None,
        arguments: &arguments,
        logical_argument_count: 1,
    };
    assert!(
        validate_request(
            FunctionKind::Aggregate,
            request,
            &mut CompileCheckpoints::try_new(
                crate::binding_test_control(),
                CompilePhase::FunctionSpecialization
            )
            .unwrap()
        )
        .is_err()
    );
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
        catalog.resolve_bound_user(
            "echo",
            FunctionKind::Scalar,
            request(&args),
            crate::binding_test_control()
        ),
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
            .resolve_bound_user("echo", kind, request(&[]), crate::binding_test_control())
            .unwrap();
        catalog
            .validate_bound(&bound, request(&[]), crate::binding_test_control())
            .unwrap();
        let mut changed = bound.clone();
        changed.selected.result_type = if kind == FunctionKind::Table {
            FunctionResultType::Scalar(scalar.clone())
        } else {
            FunctionResultType::Relation(relation.clone())
        };
        assert!(
            catalog
                .validate_bound(&changed, request(&[]), crate::binding_test_control())
                .is_err()
        );
        let mut changed = bound;
        changed.kind = if kind == FunctionKind::Table {
            FunctionKind::Window
        } else {
            FunctionKind::Table
        };
        assert!(
            catalog
                .validate_bound(&changed, request(&[]), crate::binding_test_control())
                .is_err()
        );
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
                    state_argument_contract:
                        novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
                    intermediate_pattern: "binary".into(),
                    state_format: AggregateStateFormatIdentity::try_new("test/intrinsic/state-v1")
                        .unwrap(),
                });
            }
            selected.semantics = semantics;
            let result = FunctionBindingDeclaration::try_new(
                FunctionId::try_new("test/intrinsic/v1").unwrap(),
                kind,
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
        .resolve_bound_user(
            "echo",
            FunctionKind::Scalar,
            request(&arguments),
            crate::binding_test_control(),
        )
        .unwrap();
    let mut forged = original.clone();
    forged.semantics.intrinsic_row_error = Own::MayRaise;
    assert!(
        catalog
            .validate_bound(&forged, request(&arguments), crate::binding_test_control())
            .is_err()
    );
}

#[test]
fn frozen_bindings_refuse_root_and_nested_logical_identity_drift() {
    use novarocks_type_contract::{NR_LOGICAL_TYPE_KEY, ValueLogicalType};
    let declaration = declaration(
        FunctionKind::Scalar,
        vec![overload("test/echo/T/v1", "(T)")],
    );
    let catalog = catalog(Arc::new(EchoResolver::default()), declaration);
    let typed =
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap();
    let arguments = [FunctionArgument::Value {
        value_type: typed,
        constant: None,
    }];
    let bound = catalog
        .resolve_bound_user(
            "echo",
            FunctionKind::Scalar,
            request(&arguments),
            crate::binding_test_control(),
        )
        .unwrap();
    let mut forged = bound.clone();
    forged.selected.argument_types = Box::from([FunctionArgumentType::Value(
        FunctionValueType::new(DataType::Utf8, false),
    )]);
    assert!(
        catalog
            .validate_bound(&forged, request(&arguments), crate::binding_test_control())
            .is_err()
    );
    let malformed = [FunctionArgument::Value {
        value_type: FunctionValueType {
            data_type: DataType::Int64,
            nullable: false,
            logical_type: ValueLogicalType::Json,
        },
        constant: None,
    }];
    assert!(
        catalog
            .resolve_bound_user(
                "echo",
                FunctionKind::Scalar,
                request(&malformed),
                crate::binding_test_control()
            )
            .is_err()
    );
    let nested = [argument(
        DataType::List(Arc::new(
            Field::new("item", DataType::Utf8, true)
                .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), "unknown".into())].into()),
        )),
        false,
    )];
    assert!(
        catalog
            .resolve_bound_user(
                "echo",
                FunctionKind::Scalar,
                request(&nested),
                crate::binding_test_control()
            )
            .is_err()
    );
    let mut invalid = bound.clone();
    invalid.selected.result_type = FunctionResultType::Scalar(FunctionValueType {
        data_type: DataType::Int64,
        nullable: false,
        logical_type: ValueLogicalType::Variant,
    });
    assert!(
        catalog
            .validate_bound(&invalid, request(&arguments), crate::binding_test_control())
            .is_err()
    );
}

#[test]
fn binding_type_gate_observes_original_control_before_resolver() {
    struct StopAtBatch {
        error: CompileControlError,
        observations: std::sync::Mutex<Vec<u32>>,
    }
    impl PureCompileControl for StopAtBatch {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::FunctionSpecialization);
            self.observations.lock().unwrap().push(units);
            if units == 256 {
                Err(self.error)
            } else {
                Ok(())
            }
        }
    }
    let nested = DataType::Struct(
        (0..320)
            .map(|i| Arc::new(Field::new(format!("field_{i}"), DataType::Int64, false)))
            .collect(),
    );
    let arguments = [argument(nested, false)];
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let resolver = Arc::new(EchoResolver::default());
        let catalog = catalog(
            resolver.clone(),
            declaration(FunctionKind::Scalar, vec![overload("test/echo/T/v1", "T")]),
        );
        let control = StopAtBatch {
            error,
            observations: Default::default(),
        };
        assert_eq!(
            catalog.resolve_bound_user("echo", FunctionKind::Scalar, request(&arguments), &control),
            Err(FunctionBindingError::Control(error))
        );
        assert_eq!(resolver.resolutions.load(Ordering::Relaxed), 0);
        let observed = control.observations.lock().unwrap();
        assert_eq!(observed[0], 0);
        assert_eq!(
            observed.iter().copied().find(|units| *units != 0),
            Some(256)
        );
    }
}

#[test]
fn resolver_control_failure_survives_later_wrapper_observation() {
    struct Owner(CompileControlError);
    impl FunctionBindingResolver for Owner {
        fn resolve(
            &self,
            _: FunctionBindingRequest<'_>,
            control: &dyn PureCompileControl,
        ) -> Result<FunctionBindingSelection, FunctionBindingError> {
            // This is the callee's first failure, after the engine flushed its tail.
            control.checkpoint(CompilePhase::FunctionSpecialization, 7)?;
            Err(self.0.into())
        }
        fn validate_selected(
            &self,
            _: &FunctionBindingSelection,
            _: FunctionBindingRequest<'_>,
            _: &dyn PureCompileControl,
        ) -> Result<(), FunctionBindingError> {
            unreachable!()
        }
    }
    struct Observe(std::sync::Mutex<Vec<u32>>);
    impl PureCompileControl for Observe {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut observations = self.0.lock().unwrap();
            if observations.last() == Some(&7) {
                return Err(CompileControlError::ResourceExhausted);
            }
            observations.push(units);
            Ok(())
        }
    }
    let arguments = [argument(DataType::Int64, false)];
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let catalog = catalog(
            Arc::new(Owner(error)),
            declaration(FunctionKind::Scalar, vec![overload("test/echo/T/v1", "T")]),
        );
        let control = Observe(Default::default());
        assert_eq!(
            catalog.resolve_bound_trusted(
                "echo",
                FunctionKind::Scalar,
                request(&arguments),
                &control
            ),
            Err(FunctionBindingError::Control(error))
        );
        assert_eq!(control.0.lock().unwrap().last(), Some(&7));
    }
}

struct ConstantTraceControl {
    trace: std::sync::Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl ConstantTraceControl {
    fn new(stop: Option<(usize, CompileControlError)>) -> Self {
        Self {
            trace: Default::default(),
            stop,
        }
    }
}
impl PureCompileControl for ConstantTraceControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        if let Some((at, _)) = self.stop {
            assert!(trace.len() < at, "no callback after primary refusal");
        }
        trace.push((phase, units));
        match self.stop {
            Some((at, error)) if trace.len() == at => Err(error),
            _ => Ok(()),
        }
    }
}
fn check_constant_prefixes(
    call: impl Fn(&dyn PureCompileControl) -> Result<bool, FunctionBindingError>,
    expected: bool,
) {
    let baseline = ConstantTraceControl::new(None);
    assert_eq!(call(&baseline).unwrap(), expected);
    let trace = baseline.trace.into_inner().unwrap();
    assert_eq!(trace.first().unwrap().1, 0);
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 1..=trace.len() {
            let control = ConstantTraceControl::new(Some((at, error)));
            assert_eq!(call(&control), Err(FunctionBindingError::Control(error)));
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
        }
    }
}
fn pooled_integer(rows: Vec<Option<i64>>, ordinal: u32) -> FunctionArgument {
    use arrow_array::{Array, Int64Array};
    let ty = value_type(DataType::Int64, true);
    let pool = crate::ConstantPool::try_new(
        Arc::new(ty.try_to_field("source").unwrap()),
        ty,
        Int64Array::from(rows).to_data(),
        constant_policy(),
        CompilePhase::FunctionSpecialization,
        crate::binding_test_control(),
    )
    .unwrap();
    literal_argument(pool.value(ordinal).unwrap())
}
#[test]
fn constant_arguments_compare_selected_values_without_pool_or_ordinal_identity() {
    let left = pooled_integer(vec![Some(-99), Some(42), None], 1);
    let right = pooled_integer(vec![Some(42), Some(7), Some(8)], 0);
    check_constant_prefixes(
        |control| left.equals_observed(&right, CompilePhase::FunctionSpecialization, control),
        true,
    );
    let changed = pooled_integer(vec![Some(99), Some(43)], 1);
    check_constant_prefixes(
        |control| left.equals_observed(&changed, CompilePhase::FunctionSpecialization, control),
        false,
    );
    let null = pooled_integer(vec![Some(42), None], 1);
    let second_null = pooled_integer(vec![None, Some(-9)], 0);
    check_constant_prefixes(
        |control| null.equals_observed(&second_null, CompilePhase::FunctionSpecialization, control),
        true,
    );
    let nonconstant = argument(DataType::Int64, true);
    check_constant_prefixes(
        |control| null.equals_observed(&nonconstant, CompilePhase::FunctionSpecialization, control),
        false,
    );
    check_constant_prefixes(
        |control| {
            nonconstant.equals_observed(&nonconstant, CompilePhase::FunctionSpecialization, control)
        },
        true,
    );
}
#[test]
fn constant_argument_request_type_mismatch_refuses_before_resolver_and_observes_tail() {
    let constant = match pooled_integer(vec![Some(42)], 0) {
        FunctionArgument::Value {
            constant: Some(value),
            ..
        } => value,
        _ => unreachable!(),
    };
    for ty in [
        value_type(DataType::Int64, false),
        value_type(DataType::Float64, true),
    ] {
        let arguments = [FunctionArgument::Value {
            value_type: ty,
            constant: Some(constant.clone()),
        }];
        let resolver = Arc::new(EchoResolver::default());
        let catalog = catalog(
            resolver.clone(),
            declaration(FunctionKind::Scalar, vec![overload("test/echo/T/v1", "T")]),
        );
        let call = |control: &dyn PureCompileControl| {
            catalog.resolve_bound_user("echo", FunctionKind::Scalar, request(&arguments), control)
        };
        let baseline = ConstantTraceControl::new(None);
        assert!(matches!(
            call(&baseline),
            Err(FunctionBindingError::InvalidBinding(_))
        ));
        assert_eq!(resolver.resolutions.load(Ordering::Relaxed), 0);
        let trace = baseline.trace.into_inner().unwrap();
        assert!(trace.len() >= 2, "ordinary refusal must observe completion");
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 1..=trace.len() {
                let control = ConstantTraceControl::new(Some((at, error)));
                assert_eq!(call(&control), Err(FunctionBindingError::Control(error)));
                assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
                assert_eq!(resolver.resolutions.load(Ordering::Relaxed), 0);
            }
        }
    }
}
#[test]
fn argument_observed_comparison_keeps_full_lambda_and_nominal_types() {
    let physical = value_type(DataType::FixedSizeBinary(16), true);
    let largeint = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        novarocks_type_contract::ValueLogicalType::LargeInt,
    )
    .unwrap();
    let lambda = |ty: FunctionValueType| FunctionArgument::Lambda {
        parameter_types: vec![ty.clone()].into_boxed_slice(),
        result_type: ty,
    };
    let left = lambda(largeint.clone());
    let same = lambda(largeint);
    let changed = lambda(physical);
    check_constant_prefixes(
        |control| left.equals_observed(&same, CompilePhase::FunctionSpecialization, control),
        true,
    );
    check_constant_prefixes(
        |control| left.equals_observed(&changed, CompilePhase::FunctionSpecialization, control),
        false,
    );
    let field = |metadata: &str| {
        value_type(
            DataType::Struct(
                vec![Arc::new(
                    Field::new("child", DataType::Int64, true).with_metadata(
                        [("provider".to_owned(), metadata.to_owned())]
                            .into_iter()
                            .collect(),
                    ),
                )]
                .into(),
            ),
            true,
        )
    };
    check_constant_prefixes(
        |control| {
            lambda(field("original")).equals_observed(
                &lambda(field("changed")),
                CompilePhase::FunctionSpecialization,
                control,
            )
        },
        false,
    );
}

#[test]
fn catalog_nonconstant_argument_keeps_concrete_neutral_carrier() {
    let arg = FunctionArgument::Value {
        value_type: value_type(DataType::Int64, false),
        constant: None,
    };
    assert_eq!(
        arg.argument_type(),
        FunctionArgumentType::Value(value_type(DataType::Int64, false))
    );
    let args = [arg];
    let request = FunctionBindingRequest {
        arguments: &args,
        logical_argument_count: 1,
        expected_result_type: None,
    };
    // Both paths borrow the same concrete neutral data; no adapter or clone.
    let neutral: novarocks_function_contract::FunctionBindingRequest<'_> = request;
    let copied = neutral;
    assert!(std::ptr::eq(request.arguments, copied.arguments));
    assert!(copied.expected_result_type.is_none());
    let resolver = Arc::new(EchoResolver::default());
    let catalog = catalog(
        resolver.clone(),
        declaration(FunctionKind::Scalar, vec![overload("test/echo/T/v1", "T")]),
    );
    let binding = catalog
        .resolve_bound_user(
            "echo",
            FunctionKind::Scalar,
            request,
            crate::binding_test_control(),
        )
        .unwrap();
    catalog
        .validate_bound(&binding, request, crate::binding_test_control())
        .unwrap();
    assert_eq!(resolver.resolutions.load(Ordering::Relaxed), 1);
}

#[test]
fn c16_p1_scalar_invocation_data_declaration_is_positive_checked_and_canonical() {
    let base = declaration(
        FunctionKind::Scalar,
        vec![
            overload("test/echo/T/v1", "(T)"),
            overload("test/echo/zero/v1", "()"),
        ],
    );
    let marked = base
        .clone()
        .with_scalar_invocation_data_overloads([
            identity("test/echo/zero/v1"),
            identity("test/echo/T/v1"),
        ])
        .unwrap();
    let reversed = base
        .clone()
        .with_scalar_invocation_data_overloads([
            identity("test/echo/T/v1"),
            identity("test/echo/zero/v1"),
        ])
        .unwrap();
    assert_eq!(marked, reversed);
    assert!(
        marked
            .has_scalar_invocation_data(&identity("test/echo/T/v1"))
            .unwrap()
    );
    assert!(
        !base
            .has_scalar_invocation_data(&identity("test/echo/T/v1"))
            .unwrap()
    );
    assert!(matches!(
        base.clone()
            .with_scalar_invocation_data_overloads([identity("unknown/v1")]),
        Err(FunctionBindingError::UnknownOverload(_))
    ));
    assert!(matches!(
        base.clone().with_scalar_invocation_data_overloads([
            identity("test/echo/T/v1"),
            identity("test/echo/T/v1")
        ]),
        Err(FunctionBindingError::DuplicateOverload(_))
    ));
    let table = declaration(FunctionKind::Table, vec![overload("test/echo/T/v1", "(T)")]);
    assert!(matches!(
        table.with_scalar_invocation_data_overloads([identity("test/echo/T/v1")]),
        Err(FunctionBindingError::InvalidBinding(_))
    ));
    let mut catching = overload("test/echo/T/v1", "(T)");
    catching.semantics.failure_behavior = FunctionFailureBehavior::ReturnsNull;
    assert!(matches!(
        declaration(FunctionKind::Scalar, vec![catching])
            .with_scalar_invocation_data_overloads([identity("test/echo/T/v1")]),
        Err(FunctionBindingError::InvalidBinding(_))
    ));
    assert_ne!(
        catalog(Arc::new(EchoResolver::default()), base).digest(),
        catalog(Arc::new(EchoResolver::default()), marked).digest()
    );
}

#[test]
fn c16_p1_scalar_invocation_data_survives_original_resolution_and_frozen_validation() {
    let declaration = declaration(
        FunctionKind::Scalar,
        vec![overload("test/echo/T/v1", "(T)")],
    )
    .with_scalar_invocation_data_overloads([identity("test/echo/T/v1")])
    .unwrap();
    let catalog = catalog(Arc::new(EchoResolver::default()), declaration);
    let arguments = [argument(DataType::Int64, false)];
    let bound = catalog
        .resolve_bound_user(
            "echo",
            FunctionKind::Scalar,
            request(&arguments),
            crate::binding_test_control(),
        )
        .unwrap();
    assert!(bound.declared_scalar_invocation_data);
    assert_eq!(bound.admitted_execution_abi, None);
    assert_eq!(
        bound.semantics.intrinsic_row_error,
        novarocks_type_contract::FunctionIntrinsicRowError::NoRowError
    );
    catalog
        .validate_bound(&bound, request(&arguments), crate::binding_test_control())
        .unwrap();
    let mut dropped = bound.clone();
    dropped.declared_scalar_invocation_data = false;
    assert_ne!(bound, dropped);
    assert!(
        matches!(catalog.validate_bound(&dropped, request(&arguments), crate::binding_test_control()), Err(FunctionBindingError::InvalidBinding(message)) if message.as_ref() == "frozen scalar invocation Data fact differs from the declaration")
    );
    catalog
        .validate_legacy_bound_observed(
            &mut dropped,
            request(&arguments),
            crate::binding_test_control(),
        )
        .unwrap();
    assert_eq!(bound, dropped);
    assert_eq!(dropped.admitted_execution_abi, None);
    // Reconstruction occupies the original checked identity/semantics point,
    // before the old count check. A failed later check grants no admission.
    dropped.declared_scalar_invocation_data = false;
    dropped.logical_argument_count = 0;
    assert!(
        matches!(catalog.validate_legacy_bound_observed(&mut dropped, request(&arguments), crate::binding_test_control()), Err(FunctionBindingError::InvalidBinding(message)) if message.as_ref() == "frozen logical argument count differs from the expression")
    );
    assert!(dropped.declared_scalar_invocation_data);
    assert_eq!(dropped.admitted_execution_abi, None);
    dropped.function_id = FunctionId::try_new("unknown/function/v1").unwrap();
    dropped.declared_scalar_invocation_data = false;
    assert_eq!(
        catalog.validate_legacy_bound_observed(
            &mut dropped,
            request(&arguments),
            crate::binding_test_control()
        ),
        Err(FunctionBindingError::UnknownFunction)
    );
    assert!(!dropped.declared_scalar_invocation_data);
}
