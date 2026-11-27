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

//! Original per-occurrence claim properties at their neutral unit owner.
use super::support::request as original_request;
use super::support::*;
use novarocks_expression_contract::*;
use novarocks_failure_contract::{FailureClass, FailureProjection};
use novarocks_type_contract::*;

fn original_signature(
    name: &str,
    arguments: Vec<FunctionArgumentType>,
    result: ValueType,
) -> BoundFunction {
    BoundFunction {
        function_id: FunctionId::try_new(name).unwrap(),
        overload: FunctionOverloadId::try_new("fixture/frozen-calls/exact-overload").unwrap(),
        kind: FunctionKind::Scalar,
        argument_types: TypedFactSlice::from_pure_boxed(arguments.into_boxed_slice()),
        result_type: result,
        legacy_metadata: Some(LegacyBindingMetadata {
            volatility: FunctionVolatility::Immutable,
            argument_evaluation: FunctionArgumentEvaluation::Eager,
            failure_behavior: FunctionFailureBehavior::Propagate,
            intrinsic_row_error: FunctionIntrinsicRowError::NoRowError,
            semantic_parameters: Box::default(),
        }),
    }
}

fn repeated(count: usize) -> BoundExprSet {
    let control = Control::good();
    let mut builder = BoundExprSetBuilder::new(limits());
    let signature = original_signature("fixture/frozen-calls/exact-id", vec![], boolean(true));
    let request = original_request(&signature);
    let definition = builder
        .add(
            None,
            boolean(true),
            ExprKind::FunctionCall {
                function: signature,
                args: Box::default(),
            },
        )
        .unwrap();
    builder.request(definition, request).unwrap();
    builder
        .definition_control(
            definition,
            DefinitionControl {
                shape: ControlShape::Eager,
                temporal_sources: None,
            },
        )
        .unwrap();
    for _ in 0..count {
        builder
            .root(definition, EvaluationDemand::Value, RootScope::AllPorts)
            .unwrap();
    }
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let unit = with_call_claims(
        builder.seal(vec![], &mut work).unwrap(),
        ArgumentControl::Eager,
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    unit
}
fn copy_fixture_parts(unit: &BoundExprSet) -> UnitParts {
    // This module explicitly authors pure construction headers only. The
    // recursive source/type handles below retain their own original backing.
    UnitParts::new(
        unit.ports().into(),
        unit.definitions().into(),
        unit.roots().into(),
        unit.root_use_ids().into(),
        unit.control_flow().clone(),
        unit.claims().clone(),
        unit.requests().clone(),
    )
}
fn reconstruct(parts: UnitParts) -> Result<BoundExprSet, UnitError> {
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate)?;
    let result = BoundExprSet::try_from_parts_in(parts, limits(), &mut work);
    if matches!(&result, Err(UnitError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn invalid_message(result: Result<BoundExprSet, UnitError>, expected: &str) {
    let error = result.unwrap_err();
    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    assert!(
        matches!(&error, UnitError::Invalid { message, .. } if message.contains(expected)),
        "{error:?}"
    );
}
#[test]
fn missing_extra_duplicate_foreign_fragment_context_scope_and_control_fail_closed() {
    let unit = repeated(2);
    let id = *unit.claims().keys().next().unwrap();
    let mut missing = copy_fixture_parts(&unit);
    missing.claims.remove(&id);
    invalid_message(reconstruct(missing), "lacks a call claim");
    let mut extra = copy_fixture_parts(&unit);
    let mut foreign = extra.claims[&id].clone();
    foreign.context.use_id = ExpressionUseId::new(123);
    extra.claims.insert(foreign.context.use_id, foreign);
    invalid_message(reconstruct(extra), "foreign or non-call use");
    let edits: [fn(&mut ExpressionCallClaim); 3] = [
        |claim: &mut ExpressionCallClaim| claim.context.demand = EvaluationDemand::TruthOnly,
        |claim: &mut ExpressionCallClaim| claim.context.region = EvaluationRegionId::new(123),
        |claim: &mut ExpressionCallClaim| {
            claim.effects.argument_control = ArgumentControl::TypeOnly
        },
    ];
    for edit in edits {
        let mut parts = copy_fixture_parts(&unit);
        edit(parts.claims.get_mut(&id).unwrap());
        invalid_message(reconstruct(parts), "context/control differs");
    }
    let mut scope = copy_fixture_parts(&unit);
    scope.claims.get_mut(&id).unwrap().effects.proof_scope =
        CallProofScope::Region(EvaluationRegionId::new(123));
    invalid_message(reconstruct(scope), "foreign region");
    // The immutable map cannot represent duplicate claim keys. Its remaining
    // duplicate identity law is checked at the actual root-use correspondence.
    let mut duplicate_use = copy_fixture_parts(&unit);
    duplicate_use.root_uses[1] = duplicate_use.root_uses[0];
    invalid_message(reconstruct(duplicate_use), "exact independent invocation");
    let mut builder = BoundExprSetBuilder::new(limits());
    let signature = original_signature("fixture/frozen-calls/exact-id", vec![], boolean(true));
    let original = original_request(&signature);
    let definition = builder
        .add(
            None,
            boolean(true),
            ExprKind::FunctionCall {
                function: signature,
                args: Box::default(),
            },
        )
        .unwrap();
    builder.request(definition, original.clone()).unwrap();
    assert!(
        matches!(builder.request(definition, original), Err(UnitError::Invalid { message, .. }) if message.contains("already has an original request"))
    );
    // Fragment/snapshot identity belongs to the physical property proof and
    // remains tested there; neutral units have no invented FragmentId.
}
#[test]
fn malformed_full_effect_shape_and_duplicate_environment_are_rejected() {
    let unit = repeated(1);
    let id = *unit.claims().keys().next().unwrap();
    let mutations: [fn(&mut CallEffects); 2] = [
        |effects: &mut CallEffects| effects.instance_state = FunctionInstanceState::TableInstance,
        |effects: &mut CallEffects| {
            effects.own_row_error = FunctionIntrinsicRowError::NotRowEvaluated
        },
    ];
    for mutate in mutations {
        let mut parts = copy_fixture_parts(&unit);
        mutate(&mut parts.claims.get_mut(&id).unwrap().effects);
        assert!(matches!(
            reconstruct(parts),
            Err(UnitError::Effects(EffectContractError::KindMismatch))
        ));
    }
    let mut parts = copy_fixture_parts(&unit);
    parts.claims.get_mut(&id).unwrap().effects.environment = Box::from([
        SemanticParameterRef {
            id: SemanticParameterId::new(0),
            expected_key: SemanticParameterKey::TimeZone,
        },
        SemanticParameterRef {
            id: SemanticParameterId::new(u32::MAX),
            expected_key: SemanticParameterKey::TimeZone,
        },
    ]);
    assert!(matches!(
        reconstruct(parts),
        Err(UnitError::Effects(
            EffectContractError::InvalidEnvironmentReference
        ))
    ));
}
#[test]
fn type_only_parent_retains_static_child_binding_without_runtime_child_claim() {
    let control = Control::good();
    let mut builder = BoundExprSetBuilder::new(limits());
    let child_signature =
        original_signature("fixture/frozen-calls/exact-id", vec![], boolean(true));
    let child_request = original_request(&child_signature);
    let child = builder
        .add(
            None,
            boolean(true),
            ExprKind::FunctionCall {
                function: child_signature,
                args: Box::default(),
            },
        )
        .unwrap();
    builder.request(child, child_request).unwrap();
    let parent_signature = original_signature(
        "fixture/frozen-calls/inspect-type",
        vec![FunctionArgumentType::Value(boolean(true))],
        boolean(true),
    );
    let parent_request = original_request(&parent_signature);
    let parent = builder
        .add(
            None,
            boolean(true),
            ExprKind::FunctionCall {
                function: parent_signature,
                args: Box::from([child]),
            },
        )
        .unwrap();
    builder.request(parent, parent_request).unwrap();
    builder
        .definition_control(
            parent,
            DefinitionControl {
                shape: ControlShape::TypeOnly,
                temporal_sources: None,
            },
        )
        .unwrap();
    builder
        .root(parent, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let unit = with_call_claims(
        builder.seal(vec![], &mut work).unwrap(),
        ArgumentControl::TypeOnly,
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    assert_eq!(unit.definitions().len(), 2);
    assert_eq!(unit.requests().len(), 2);
    assert_eq!(unit.control_flow().uses().len(), 1);
    assert_eq!(unit.claims().len(), 1);
    let invocation = unit.control_flow().uses().values().next().unwrap();
    assert!(invocation.arguments.is_empty());
    assert_eq!(invocation.definition, parent);
    let ExprKind::FunctionCall {
        function: actual,
        args,
    } = &unit.definition(parent).unwrap().kind
    else {
        panic!("actual retained parent")
    };
    assert_eq!(
        actual.argument_types.as_slice(),
        &[FunctionArgumentType::Value(boolean(true))]
    );
    assert_eq!(args.as_ref(), &[child]);
    let mut parts = unit.into_parts();
    let mut invented = parts.claims.values().next().unwrap().clone();
    invented.context.use_id = ExpressionUseId::new(u32::MAX);
    invented.effects.argument_control = ArgumentControl::Eager;
    parts.claims.insert(invented.context.use_id, invented);
    invalid_message(reconstruct(parts), "foreign or non-call use");
}
#[test]
fn scalar_claim_cannot_borrow_another_guarded_occurrences_proof() {
    let control = Control::good();
    let mut builder = BoundExprSetBuilder::new(limits());
    let signature = original_signature("fixture/frozen-calls/exact-id", vec![], boolean(true));
    let original = original_request(&signature);
    let call = builder
        .add(
            None,
            boolean(true),
            ExprKind::FunctionCall {
                function: signature,
                args: Box::default(),
            },
        )
        .unwrap();
    builder.request(call, original).unwrap();
    builder
        .definition_control(
            call,
            DefinitionControl {
                shape: ControlShape::Eager,
                temporal_sources: None,
            },
        )
        .unwrap();
    let value = novarocks_constant_contract::ConstantValue::from_boolean(
        std::sync::Arc::new(boolean(true).try_to_field("fixture").unwrap()),
        boolean(true),
        false,
        policy(),
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    let mut pools = novarocks_constant_contract::ConstantPools::empty();
    let pool = novarocks_constant_contract::ConstantPoolId::new(1);
    let reference = novarocks_constant_contract::ConstantReference {
        pool,
        ordinal: value.ordinal(),
    };
    pools.insert(pool, value.pool().clone()).unwrap();
    let otherwise = builder
        .add(None, boolean(true), ExprKind::Constant(reference))
        .unwrap();
    let case = builder
        .add(
            None,
            boolean(true),
            ExprKind::Case {
                ieee_float_equality: false,
                operand: None,
                when_then: Box::from([(call, call)]),
                else_expr: Some(otherwise),
            },
        )
        .unwrap();
    builder
        .root(case, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let unit = with_call_claims(
        builder.seal(vec![], &mut work).unwrap(),
        ArgumentControl::Eager,
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    let root = &unit.control_flow().uses()[&unit.root_use_ids()[0]];
    let first = &unit.claims()[&root.arguments[0]];
    let then = &unit.claims()[&root.arguments[1]];
    assert_eq!(first.context.demand, EvaluationDemand::Value);
    assert_eq!(then.context.demand, EvaluationDemand::Value);
    assert_ne!(first.context.region, then.context.region);
    let first_proof = first.effects.proof_scope;
    let then_id = then.context.use_id;
    let mut parts = unit.into_parts();
    parts.claims.get_mut(&then_id).unwrap().effects.proof_scope = first_proof;
    invalid_message(reconstruct(parts), "foreign region");
    assert_eq!(
        pools.entries()[&pool]
            .value(0)
            .unwrap()
            .try_boolean()
            .unwrap(),
        Some(false)
    );
}

fn repeated_under(count: usize, control: &Control) -> Result<BoundExprSet, UnitError> {
    let mut builder = BoundExprSetBuilder::new(limits());
    let signature = original_signature("fixture/frozen-calls/exact-id", vec![], boolean(true));
    let original = original_request(&signature);
    let definition = builder.add(
        None,
        boolean(true),
        ExprKind::FunctionCall {
            function: signature,
            args: Box::default(),
        },
    )?;
    builder.request(definition, original)?;
    builder.definition_control(
        definition,
        DefinitionControl {
            shape: ControlShape::Eager,
            temporal_sources: None,
        },
    )?;
    for _ in 0..count {
        builder.root(definition, EvaluationDemand::Value, RootScope::AllPorts)?;
    }
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = (|| {
        with_call_claims(
            builder.seal(vec![], &mut work)?,
            ArgumentControl::Eager,
            &mut work,
        )
    })();
    if matches!(&result, Err(UnitError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
#[test]
fn actual_300_scalar_source_observes_quantum_and_each_original_primary_prefix() {
    let control = Control::good();
    let unit = repeated_under(300, &control).unwrap();
    assert_eq!(unit.claims().len(), 300);
    let trace = control.trace();
    assert!(
        trace
            .iter()
            .all(|(phase, units)| *phase == CompilePhase::LowerProgram && *units <= 256)
    );
    assert!(trace.iter().any(|(_, units)| *units == 256));
    for (at, _) in trace
        .iter()
        .enumerate()
        .filter(|(at, (_, units))| *at == 0 || *at + 1 == trace.len() || *units == 256)
    {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control::at(at, cause.clone());
            assert!(
                matches!(repeated_under(300, &control), Err(UnitError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

// Transferred from physical-plan's old raw self-output Values fixture. A unit
// must reject an undeclared Value before node/property admission can see it.
#[test]
fn value_outside_declared_root_port_is_rejected_before_call_claim_correspondence() {
    let unit = repeated(1);
    let mut parts = copy_fixture_parts(&unit);
    parts.definitions[0].kind = ExprKind::input(ValueId::new(17));
    parts.claims.clear();
    parts.requests.clear();
    invalid_message(reconstruct(parts), "value is outside its root input scope");
}
