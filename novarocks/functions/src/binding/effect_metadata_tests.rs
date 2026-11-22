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

use super::*;
use crate::{AggregateSignatureResolver, FunctionResolutionError, ResolvedAggregateSignature};
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, EvaluationDemand, FunctionEffectDeclaration, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionNullBehavior, ObservableEffects, SemanticParameterKey,
};

// These ordinary catalogue resolver ports are deliberately never invoked by
// metadata tests. They are not installed Pure owners or CPU implementations.
struct MetadataResolver;
impl FunctionBindingResolver for MetadataResolver {
    fn resolve(
        &self,
        _: FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        panic!("metadata catalogue must not resolve a call")
    }
    fn validate_selected(
        &self,
        _: &FunctionBindingSelection,
        _: FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        panic!("metadata catalogue must not validate a call")
    }
}
impl AggregateSignatureResolver for MetadataResolver {
    fn resolve_aggregate(
        &self,
        _: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        panic!("metadata catalogue must not resolve an aggregate")
    }
}
fn identity(value: &str) -> FunctionOverloadId {
    FunctionOverloadId::try_new(value).unwrap()
}
fn effects(kind: FunctionKind) -> FunctionEffectDeclaration {
    let (argument_control, instance_state, own_row_error) = match kind {
        FunctionKind::Scalar => (
            ArgumentControl::Eager,
            FunctionInstanceState::None,
            FunctionIntrinsicRowError::NoRowError,
        ),
        FunctionKind::Aggregate => (
            ArgumentControl::Aggregate,
            FunctionInstanceState::AggregateInstance,
            FunctionIntrinsicRowError::NotRowEvaluated,
        ),
        FunctionKind::Window => (
            ArgumentControl::Window,
            FunctionInstanceState::WindowPartition,
            FunctionIntrinsicRowError::NotRowEvaluated,
        ),
        FunctionKind::Table => (
            ArgumentControl::Table,
            FunctionInstanceState::TableInstance,
            FunctionIntrinsicRowError::NoRowError,
        ),
    };
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::CalledOnNull,
        argument_control,
        instance_state,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::new([]),
    }
}
fn overload(kind: FunctionKind, effects: FunctionEffectDeclaration) -> FunctionOverloadDeclaration {
    FunctionOverloadDeclaration::from_effects(
        identity("fixture/effect-metadata/overload-v1"),
        "(T, U)",
        "T",
        (kind == FunctionKind::Aggregate).then(|| AggregateBindingDeclaration {
            state_argument_contract:
                novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
            intermediate_pattern: "state<T>".into(),
            state_format: AggregateStateFormatIdentity::try_new("fixture/effect-metadata/state-v1")
                .unwrap(),
        }),
        effects,
    )
}
fn declaration(
    kind: FunctionKind,
    effects: FunctionEffectDeclaration,
) -> FunctionBindingDeclaration {
    FunctionBindingDeclaration::try_new_complete(
        FunctionId::try_new("fixture/effect-metadata/function-v1").unwrap(),
        kind,
        [overload(kind, effects)],
    )
    .unwrap()
}
fn catalog(declaration: FunctionBindingDeclaration) -> EngineFunctionCatalog {
    let resolver = Arc::new(MetadataResolver);
    let definition = if declaration.kind() == FunctionKind::Aggregate {
        FunctionDefinition::try_new_bound_aggregate(
            "effect_metadata",
            FunctionVisibility::Public,
            declaration,
            resolver.clone(),
            resolver,
        )
        .unwrap()
    } else {
        FunctionDefinition::try_new_bound(
            "effect_metadata",
            FunctionVisibility::Public,
            declaration,
            resolver,
        )
        .unwrap()
    };
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(definition).unwrap();
    builder.seal_bound().unwrap()
}
fn digest(kind: FunctionKind, effects: FunctionEffectDeclaration) -> [u8; 32] {
    catalog(declaration(kind, effects)).digest()
}
fn assert_same_projection_distinct_digest(
    left: FunctionEffectDeclaration,
    right: FunctionEffectDeclaration,
) {
    assert_eq!(
        FunctionSemantics::from_effects(&left),
        FunctionSemantics::from_effects(&right)
    );
    assert_ne!(
        digest(FunctionKind::Scalar, left),
        digest(FunctionKind::Scalar, right)
    );
}
fn keys() -> [SemanticParameterKey; 5] {
    [
        SemanticParameterKey::StatementStartUtc,
        SemanticParameterKey::TimeZone,
        SemanticParameterKey::AllowThrowException,
        SemanticParameterKey::GroupConcatLegacy,
        SemanticParameterKey::GroupConcatMaxLen,
    ]
}

#[test]
fn full_factory_preserves_source_and_only_projects_four_legacy_fields() {
    let mut source = effects(FunctionKind::Scalar);
    source.argument_control = ArgumentControl::HigherOrder {
        body_ordinal: 1,
        body_demand: EvaluationDemand::TruthOnly,
    };
    source.null_behavior = FunctionNullBehavior::ControlDefined;
    source.instance_state = FunctionInstanceState::ScalarInstance;
    source.observable_effects = ObservableEffects {
        rng_sampling: true,
        warnings: true,
        controlled_wait: true,
    };
    source.environment_dependencies = Box::from([SemanticParameterKey::TimeZone]);
    let overload = overload(FunctionKind::Scalar, source.clone());
    assert_eq!(overload.effects.as_ref(), Some(&source));
    assert_eq!(
        overload.semantics,
        FunctionSemantics {
            volatility: source.value_stability,
            argument_evaluation: FunctionArgumentEvaluation::Eager,
            failure_behavior: source.failure_behavior,
            intrinsic_row_error: source.own_row_error
        }
    );
    let declared = declaration(FunctionKind::Scalar, source.clone());
    assert_eq!(
        declared.effect_declaration(&overload.identity).unwrap(),
        &source
    );
    declared.validate_complete_effects().unwrap();
    let sealed = catalog(declared);
    let actual = sealed
        .definition("EFFECT_METADATA", FunctionKind::Scalar)
        .unwrap()
        .binding_declaration()
        .unwrap();
    assert_eq!(
        actual.effect_declaration(&overload.identity).unwrap(),
        &source
    );
}

#[test]
fn null_state_and_every_observable_bit_change_real_catalogue_digest_with_identical_old_four() {
    let base = effects(FunctionKind::Scalar);
    for null_behavior in [
        FunctionNullBehavior::Strict,
        FunctionNullBehavior::ControlDefined,
    ] {
        let mut changed = base.clone();
        changed.null_behavior = null_behavior;
        assert_same_projection_distinct_digest(base.clone(), changed);
    }
    let mut state = base.clone();
    state.instance_state = FunctionInstanceState::ScalarInstance;
    assert_same_projection_distinct_digest(base.clone(), state);
    let mut seen = BTreeSet::new();
    for bits in 0..8 {
        let mut changed = base.clone();
        changed.observable_effects = ObservableEffects {
            rng_sampling: bits & 1 != 0,
            warnings: bits & 2 != 0,
            controlled_wait: bits & 4 != 0,
        };
        assert_eq!(
            FunctionSemantics::from_effects(&base),
            FunctionSemantics::from_effects(&changed)
        );
        assert!(
            seen.insert(digest(FunctionKind::Scalar, changed)),
            "observable combination {bits} collided"
        );
    }
    let mut seen = BTreeSet::new();
    for null_behavior in [
        FunctionNullBehavior::Strict,
        FunctionNullBehavior::CalledOnNull,
        FunctionNullBehavior::ControlDefined,
    ] {
        let mut changed = base.clone();
        changed.null_behavior = null_behavior;
        assert!(seen.insert(digest(FunctionKind::Scalar, changed)));
    }
}

#[test]
fn exact_control_variants_and_higher_order_ordinal_demand_survive_lossy_projection() {
    let mut base = effects(FunctionKind::Scalar);
    base.null_behavior = FunctionNullBehavior::ControlDefined;
    for controls in [
        vec![
            ArgumentControl::Eager,
            ArgumentControl::TypeOnly,
            ArgumentControl::HigherOrder {
                body_ordinal: 0,
                body_demand: EvaluationDemand::Value,
            },
            ArgumentControl::HigherOrder {
                body_ordinal: 1,
                body_demand: EvaluationDemand::Value,
            },
            ArgumentControl::HigherOrder {
                body_ordinal: 0,
                body_demand: EvaluationDemand::TruthOnly,
            },
            ArgumentControl::HigherOrder {
                body_ordinal: 1,
                body_demand: EvaluationDemand::TruthOnly,
            },
        ],
        vec![
            ArgumentControl::If,
            ArgumentControl::Coalesce,
            ArgumentControl::SimpleCase,
            ArgumentControl::SearchedCase,
        ],
    ] {
        let mut seen = BTreeSet::new();
        let mut projection = None;
        for control in controls {
            let mut changed = base.clone();
            changed.argument_control = control;
            let current = FunctionSemantics::from_effects(&changed);
            if let Some(projection) = projection {
                assert_eq!(current, projection);
            }
            projection = Some(current);
            assert!(
                seen.insert(digest(FunctionKind::Scalar, changed)),
                "exact control {control:?} collided"
            );
        }
    }
    // Relational protocols are tested only with their valid kind/state/row facts.
    for kind in [
        FunctionKind::Aggregate,
        FunctionKind::Window,
        FunctionKind::Table,
    ] {
        let base = effects(kind);
        let declared = declaration(kind, base.clone());
        assert_eq!(
            declared
                .effect_declaration(&identity("fixture/effect-metadata/overload-v1"))
                .unwrap(),
            &base
        );
        let mut changed = base.clone();
        changed.null_behavior = FunctionNullBehavior::ControlDefined;
        assert_eq!(
            FunctionSemantics::from_effects(&base),
            FunctionSemantics::from_effects(&changed)
        );
        assert_ne!(digest(kind, base), digest(kind, changed));
    }
}

#[test]
fn all_environment_keys_and_dependency_membership_change_digest_but_reorder_is_canonical() {
    let base = effects(FunctionKind::Scalar);
    let baseline = digest(FunctionKind::Scalar, base.clone());
    let mut seen = BTreeSet::new();
    for key in keys() {
        let mut changed = base.clone();
        changed.environment_dependencies = Box::from([key]);
        assert_eq!(
            FunctionSemantics::from_effects(&base),
            FunctionSemantics::from_effects(&changed)
        );
        let current = digest(FunctionKind::Scalar, changed);
        assert_ne!(current, baseline);
        assert!(seen.insert(current));
    }
    let mut ordered = base.clone();
    ordered.environment_dependencies = keys().into();
    let mut reversed = ordered.clone();
    reversed.environment_dependencies.reverse();
    let first = declaration(FunctionKind::Scalar, ordered);
    let second = declaration(FunctionKind::Scalar, reversed);
    assert_eq!(first, second);
    assert_eq!(
        first
            .effect_declaration(&identity("fixture/effect-metadata/overload-v1"))
            .unwrap()
            .environment_dependencies
            .as_ref(),
        keys().as_slice()
    );
    assert_eq!(catalog(first).digest(), catalog(second).digest());
    let mut pair = base;
    pair.environment_dependencies = Box::from([
        SemanticParameterKey::StatementStartUtc,
        SemanticParameterKey::TimeZone,
    ]);
    let pair_digest = digest(FunctionKind::Scalar, pair.clone());
    pair.environment_dependencies[1] = SemanticParameterKey::AllowThrowException;
    assert_ne!(pair_digest, digest(FunctionKind::Scalar, pair));
}

#[test]
fn value_stability_row_error_and_failure_variants_remain_full_valid_source_facts() {
    let base = effects(FunctionKind::Scalar);
    let baseline = digest(FunctionKind::Scalar, base.clone());
    for volatility in [FunctionVolatility::Stable, FunctionVolatility::Volatile] {
        let mut changed = base.clone();
        changed.value_stability = volatility;
        assert_ne!(baseline, digest(FunctionKind::Scalar, changed));
    }
    let mut changed = base.clone();
    changed.own_row_error = FunctionIntrinsicRowError::MayRaise;
    assert_ne!(baseline, digest(FunctionKind::Scalar, changed));
    let mut changed = base;
    changed.failure_behavior = FunctionFailureBehavior::ReturnsNull;
    assert_ne!(baseline, digest(FunctionKind::Scalar, changed));
}

#[test]
fn legacy_none_remains_missing_and_exact_lookup_distinguishes_unknown_from_incomplete() {
    let base = effects(FunctionKind::Scalar);
    let full = overload(FunctionKind::Scalar, base);
    let mut legacy = full.clone();
    legacy.effects = None;
    let legacy_id = legacy.identity.clone();
    let make = |overloads| {
        FunctionBindingDeclaration::try_new(
            FunctionId::try_new("fixture/effect-metadata/function-v1").unwrap(),
            FunctionKind::Scalar,
            overloads,
        )
    };
    let declaration = make(vec![legacy.clone()]).unwrap();
    assert!(declaration.overloads()[0].effects.is_none());
    assert_eq!(
        declaration.validate_complete_effects(),
        Err(FunctionBindingError::MissingEffectDeclaration(
            legacy_id.clone()
        ))
    );
    assert_eq!(
        declaration.effect_declaration(&legacy_id),
        Err(FunctionBindingError::MissingEffectDeclaration(
            legacy_id.clone()
        ))
    );
    let unknown = identity("fixture/effect-metadata/unknown");
    assert_eq!(
        declaration.effect_declaration(&unknown),
        Err(FunctionBindingError::UnknownOverload(unknown.clone()))
    );
    assert_eq!(
        FunctionBindingDeclaration::try_new_complete(
            declaration.function_id().clone(),
            FunctionKind::Scalar,
            [legacy.clone()]
        ),
        Err(FunctionBindingError::MissingEffectDeclaration(
            legacy_id.clone()
        ))
    );
    let legacy_catalog = catalog(declaration);
    assert!(
        legacy_catalog
            .definition("effect_metadata", FunctionKind::Scalar)
            .unwrap()
            .binding_declaration()
            .unwrap()
            .overloads()[0]
            .effects
            .is_none()
    );
    let complete = make(vec![full.clone()]).unwrap();
    assert_eq!(complete.overloads()[0].semantics, legacy.semantics);
    assert_ne!(legacy_catalog.digest(), catalog(complete).digest());
    let mut second = full;
    second.identity = identity("fixture/effect-metadata/overload-v2");
    second.argument_pattern = "(U)".into();
    second.effects.as_mut().unwrap().null_behavior = FunctionNullBehavior::Strict;
    let mixed = make(vec![second.clone(), legacy]).unwrap();
    assert_eq!(
        mixed
            .effect_declaration(&second.identity)
            .unwrap()
            .null_behavior,
        FunctionNullBehavior::Strict
    );
    assert_eq!(
        mixed.effect_declaration(&legacy_id),
        Err(FunctionBindingError::MissingEffectDeclaration(legacy_id))
    );
}

#[test]
fn invalid_projection_kind_control_null_and_duplicate_dependencies_fail_before_catalogue() {
    let base = effects(FunctionKind::Scalar);
    let id = FunctionId::try_new("fixture/effect-metadata/function-v1").unwrap();
    let check = |kind, overload| {
        assert!(matches!(
            FunctionBindingDeclaration::try_new(id.clone(), kind, [overload]),
            Err(FunctionBindingError::InvalidBinding(_))
        ))
    };
    for field in 0..4 {
        let mut changed = overload(FunctionKind::Scalar, base.clone());
        match field {
            0 => changed.semantics.volatility = FunctionVolatility::Volatile,
            1 => changed.semantics.argument_evaluation = FunctionArgumentEvaluation::ShortCircuit,
            2 => changed.semantics.failure_behavior = FunctionFailureBehavior::ReturnsNull,
            3 => changed.semantics.intrinsic_row_error = FunctionIntrinsicRowError::MayRaise,
            _ => unreachable!(),
        };
        check(FunctionKind::Scalar, changed);
    }
    for control in [
        ArgumentControl::If,
        ArgumentControl::Coalesce,
        ArgumentControl::SimpleCase,
        ArgumentControl::SearchedCase,
    ] {
        let mut changed = base.clone();
        changed.argument_control = control;
        check(
            FunctionKind::Scalar,
            overload(FunctionKind::Scalar, changed),
        );
    }
    for control in [
        ArgumentControl::Aggregate,
        ArgumentControl::Window,
        ArgumentControl::Table,
    ] {
        let mut changed = base.clone();
        changed.argument_control = control;
        check(
            FunctionKind::Scalar,
            overload(FunctionKind::Scalar, changed),
        );
    }
    for state in [
        FunctionInstanceState::AggregateInstance,
        FunctionInstanceState::WindowPartition,
        FunctionInstanceState::TableInstance,
    ] {
        let mut changed = base.clone();
        changed.instance_state = state;
        check(
            FunctionKind::Scalar,
            overload(FunctionKind::Scalar, changed),
        );
    }
    for kind in [
        FunctionKind::Aggregate,
        FunctionKind::Window,
        FunctionKind::Table,
    ] {
        let mut changed = effects(kind);
        changed.null_behavior = FunctionNullBehavior::Strict;
        check(kind, overload(kind, changed));
    }
    let mut changed = base.clone();
    changed.own_row_error = FunctionIntrinsicRowError::NotRowEvaluated;
    check(
        FunctionKind::Scalar,
        overload(FunctionKind::Scalar, changed),
    );
    let mut duplicate = base.clone();
    duplicate.environment_dependencies = Box::from([
        SemanticParameterKey::TimeZone,
        SemanticParameterKey::TimeZone,
    ]);
    check(
        FunctionKind::Scalar,
        overload(FunctionKind::Scalar, duplicate),
    );
    for mutation in 0..3 {
        let mut changed = base.clone();
        changed.argument_control = ArgumentControl::TypeOnly;
        match mutation {
            0 => changed.own_row_error = FunctionIntrinsicRowError::MayRaise,
            1 => changed.instance_state = FunctionInstanceState::ScalarInstance,
            2 => changed.observable_effects.warnings = true,
            _ => unreachable!(),
        };
        check(
            FunctionKind::Scalar,
            overload(FunctionKind::Scalar, changed),
        );
    }
    check(
        FunctionKind::Aggregate,
        overload(FunctionKind::Aggregate, base),
    );
}

#[test]
fn canonical_overload_order_does_not_merge_exact_effect_identity_or_drop_patterns() {
    let first = overload(FunctionKind::Scalar, effects(FunctionKind::Scalar));
    let mut second = first.clone();
    second.identity = identity("fixture/effect-metadata/overload-v2");
    second.argument_pattern = "(U)".into();
    second.effects.as_mut().unwrap().null_behavior = FunctionNullBehavior::Strict;
    let make = |overloads| {
        FunctionBindingDeclaration::try_new_complete(
            FunctionId::try_new("fixture/effect-metadata/function-v1").unwrap(),
            FunctionKind::Scalar,
            overloads,
        )
        .unwrap()
    };
    let forward = make(vec![first.clone(), second.clone()]);
    let reverse = make(vec![second.clone(), first.clone()]);
    assert_eq!(catalog(forward.clone()).digest(), catalog(reverse).digest());
    assert_eq!(
        forward
            .effect_declaration(&first.identity)
            .unwrap()
            .null_behavior,
        FunctionNullBehavior::CalledOnNull
    );
    assert_eq!(
        forward
            .effect_declaration(&second.identity)
            .unwrap()
            .null_behavior,
        FunctionNullBehavior::Strict
    );
    let mut changed = second;
    changed.result_pattern = "U".into();
    assert_ne!(
        catalog(forward.clone()).digest(),
        catalog(make(vec![first.clone(), changed.clone()])).digest()
    );
    changed.result_pattern = "T".into();
    changed.argument_pattern = "(U,V)".into();
    assert_ne!(
        catalog(forward).digest(),
        catalog(make(vec![first, changed])).digest()
    );
}

#[test]
fn state_argument_contract_changes_catalog_digest_with_identical_types_and_effects() {
    let original = declaration(FunctionKind::Aggregate, effects(FunctionKind::Aggregate));
    let mut changed = original.clone();
    changed.overloads[0]
        .aggregate
        .as_mut()
        .unwrap()
        .state_argument_contract =
        novarocks_type_contract::AggregateStateArgumentContract::ValueRootNullabilityIndependent;
    assert_eq!(original.function_id, changed.function_id);
    assert_eq!(original.overloads[0].effects, changed.overloads[0].effects);
    assert_eq!(
        original.overloads[0].argument_pattern,
        changed.overloads[0].argument_pattern
    );
    assert_eq!(
        original.overloads[0].result_pattern,
        changed.overloads[0].result_pattern
    );
    assert_ne!(catalog(original).digest(), catalog(changed).digest());
}
