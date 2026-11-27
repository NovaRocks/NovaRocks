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

use super::support::*;
use novarocks_expression_contract::*;
use novarocks_type_contract::*;
use std::sync::Arc;

#[test]
fn zero_port_checked_constant_keeps_dense_definition_and_root_demand() {
    let (pools, reference) = i64_source(42);
    let mut builder = BoundExprSetBuilder::new(limits());
    let id = builder
        .add(None, integer(false), ExprKind::Constant(reference))
        .unwrap();
    assert_eq!(id.get(), 0);
    let root = builder
        .root(id, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let unit = no_calls(builder, vec![], &Control::good()).unwrap();
    assert_eq!(unit.root(root).unwrap().definition, id);
    assert_eq!(unit.ports().len(), 0);
    assert_eq!(
        pools.entries()[&reference.pool]
            .value(reference.ordinal)
            .unwrap()
            .try_i64()
            .unwrap(),
        Some(42)
    );
    assert_eq!(
        unit.control_flow().uses()[&unit.root_use(root).unwrap()]
            .context
            .demand,
        EvaluationDemand::Value
    );
}
#[test]
fn two_ports_and_synthetic_scope_preserve_exact_values_and_root_ordinals() {
    let ty = boolean(true);
    let mut builder = BoundExprSetBuilder::new(limits());
    let a = builder
        .add(None, ty.clone(), ExprKind::input(ValueId::new(9)))
        .unwrap();
    let b = builder
        .add(None, ty.clone(), ExprKind::input(ValueId::new(71)))
        .unwrap();
    let both = builder
        .add(
            None,
            ty.clone(),
            ExprKind::Conjunction {
                args: Box::new([a, b]),
            },
        )
        .unwrap();
    let left = builder
        .root(a, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let right = builder
        .root(b, EvaluationDemand::TruthOnly, RootScope::Port(1))
        .unwrap();
    let combined = builder
        .root(both, EvaluationDemand::TruthOnly, RootScope::AllPorts)
        .unwrap();
    let unit = no_calls(
        builder,
        vec![
            ExprPort {
                values: Box::new([(ValueId::new(9), ty.clone())]),
            },
            ExprPort {
                values: Box::new([(ValueId::new(71), ty)]),
            },
        ],
        &Control::good(),
    )
    .unwrap();
    assert_eq!([left.get(), right.get(), combined.get()], [0, 1, 2]);
    assert_eq!(unit.roots()[1].scope, RootScope::Port(1));
    assert_eq!(unit.roots()[2].scope, RootScope::AllPorts);
    let use_id = unit.root_use(combined).unwrap();
    assert_eq!(unit.control_flow().uses()[&use_id].arguments.len(), 2);
}
#[test]
fn lambda_reservation_precedes_body_and_is_not_required_to_be_topological() {
    let ty = integer(false);
    let mut builder = BoundExprSetBuilder::new(limits());
    let lambda = builder.reserve().unwrap();
    let body = builder
        .add(
            Some(lambda),
            ty.clone(),
            ExprKind::LambdaParameter { lambda, ordinal: 0 },
        )
        .unwrap();
    builder
        .insert(
            lambda,
            None,
            ty.clone(),
            ExprKind::Lambda {
                parameter_types: Box::new([ty.clone()]),
                body,
            },
        )
        .unwrap();
    let f = function(
        "fixture/unit/lambda",
        vec![FunctionArgumentType::Lambda {
            parameter_types: TypedFactSlice::from_pure_boxed(Box::new([ty.clone()])),
            result_type: ty.clone(),
        }],
        ty.clone(),
    );
    let request = request(&f);
    let call = builder
        .add(
            None,
            ty,
            ExprKind::FunctionCall {
                function: f,
                args: Box::new([lambda]),
            },
        )
        .unwrap();
    builder.request(call, request).unwrap();
    builder
        .definition_control(
            call,
            DefinitionControl {
                shape: ControlShape::HigherOrder {
                    body_ordinal: 0,
                    body_demand: EvaluationDemand::Value,
                },
                temporal_sources: None,
            },
        )
        .unwrap();
    builder
        .root(call, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let sealed = builder.seal(vec![], &mut work).unwrap();
    let unit = with_call_claims(
        sealed,
        ArgumentControl::HigherOrder {
            body_ordinal: 0,
            body_demand: EvaluationDemand::Value,
        },
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    assert_eq!([lambda.get(), body.get(), call.get()], [0, 1, 2]);
    assert_eq!(unit.definitions()[1].lambda_scope, Some(lambda));
}
#[test]
fn original_storage_survives_seal_finish_and_parts_roundtrip_until_actual_drop() {
    struct Hold;
    impl ValueTypeStorageHold for Hold {}
    let hold = Arc::new(Hold);
    let weak = Arc::downgrade(&hold);
    let (pools, reference) = i64_source(1);
    let mut builder = BoundExprSetBuilder::new(limits())
        .with_storage_lifetime(ValueTypeStorageLifetime::held(hold.clone()))
        .unwrap();
    let id = builder
        .add(None, integer(false), ExprKind::Constant(reference))
        .unwrap();
    builder
        .root(id, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let unit = no_calls(builder, vec![], &Control::good()).unwrap();
    drop(hold);
    assert!(weak.upgrade().is_some());
    let parts = unit.into_parts();
    assert!(weak.upgrade().is_some());
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
    let unit = BoundExprSet::try_from_parts_in(parts, limits(), &mut work).unwrap();
    work.finish().unwrap();
    assert!(weak.upgrade().is_some());
    drop(unit);
    assert!(weak.upgrade().is_none());
    drop(pools);
}

#[test]
fn dense_definitions_keep_complete_typed_source_and_repeated_ordered_children() {
    use arrow_schema::{DataType, Field};
    let child = Arc::new(
        Field::new("authored-child", DataType::Int64, false).with_metadata(
            std::collections::HashMap::from([("provider".into(), "exact".into())]),
        ),
    );
    // The original type author first normalizes the declared layout. The unit
    // must retain that exact checked source, including its actual nested Arc.
    let ty = ValueType::new(DataType::List(Arc::clone(&child)), true);
    let expected_type = ty.clone();
    let DataType::List(child) = ty.data_type() else {
        unreachable!()
    };
    let child = Arc::clone(child);
    let mut b = BoundExprSetBuilder::new(limits());
    let value = b
        .add(None, ty.clone(), ExprKind::input(ValueId::new(9)))
        .unwrap();
    let f = function(
        "fixture/unit/ordered",
        vec![FunctionArgumentType::Value(ty.clone()); 3],
        ty.clone(),
    );
    let original_request = request(&f);
    let id = b
        .add(
            None,
            ty.clone(),
            ExprKind::FunctionCall {
                function: f,
                args: Box::new([value, value, value]),
            },
        )
        .unwrap();
    b.request(id, original_request).unwrap();
    b.definition_control(
        id,
        DefinitionControl {
            shape: ControlShape::Eager,
            temporal_sources: None,
        },
    )
    .unwrap();
    b.root(id, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    let sealed = b
        .seal(
            vec![ExprPort {
                values: Box::new([(ValueId::new(9), ty)]),
            }],
            &mut work,
        )
        .unwrap();
    let unit = with_call_claims(sealed, ArgumentControl::Eager, &mut work).unwrap();
    work.finish().unwrap();
    let DataType::List(stored) = unit.definition(value).unwrap().ty.data_type() else {
        panic!("source carrier changed")
    };
    assert!(Arc::ptr_eq(&child, stored));
    assert_eq!(unit.definition(value).unwrap().ty, expected_type);
    let ExprKind::FunctionCall { args, .. } = &unit.definition(id).unwrap().kind else {
        panic!("definition kind changed")
    };
    assert_eq!(args.as_ref(), [value, value, value]);
    let invocation = &unit.control_flow().uses()[&unit.root_use_ids()[0]];
    assert_eq!(invocation.arguments.len(), 3);
    assert_ne!(invocation.arguments[0], invocation.arguments[1]);
    assert_ne!(invocation.arguments[1], invocation.arguments[2]);
}

#[test]
fn seal_and_finish_borrow_the_original_phase_and_leave_its_footer_to_the_caller() {
    let make = |wrong_scope| {
        let mut b = BoundExprSetBuilder::new(limits());
        let id = b
            .add(None, boolean(true), ExprKind::input(ValueId::new(9)))
            .unwrap();
        b.root(
            id,
            EvaluationDemand::Value,
            RootScope::Port(u32::from(wrong_scope)),
        )
        .unwrap();
        b
    };
    let ports = || {
        vec![ExprPort {
            values: Box::new([(ValueId::new(9), boolean(true))]),
        }]
    };
    for wrong in [false, true] {
        let c = Control::good();
        let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
        let entry = c.trace();
        assert_eq!(entry, [(CompilePhase::Decode, 0)]);
        let result = (|| {
            make(wrong)
                .seal(ports(), &mut work)?
                .finish(std::collections::BTreeMap::new(), &mut work)
        })();
        assert_eq!(result.is_err(), wrong);
        let before = c.trace();
        assert!(
            before
                .iter()
                .all(|(phase, _)| *phase == CompilePhase::Decode)
        );
        work.finish().unwrap();
        let after = c.trace();
        assert_eq!(&after[..before.len()], before);
        assert!(after.len() > before.len());
        // The original footer may refuse ordinary completion. The unit never
        // consumes it, and no later checkpoint follows that refusal.
        let index = after.len() - 1;
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let c = Control::at(index, cause.clone());
            let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
            let result = (|| {
                make(wrong)
                    .seal(ports(), &mut work)?
                    .finish(std::collections::BTreeMap::new(), &mut work)
            })();
            assert_eq!(result.is_err(), wrong);
            assert_eq!(work.finish(), Err(cause));
            assert_eq!(c.trace(), after);
        }
    }
}

#[test]
fn constrained_scalar_type_edit_is_atomic_and_preserves_the_call_identity() {
    let mut b = BoundExprSetBuilder::new(limits());
    let original = function("fixture/unit/rebind", vec![], integer(false));
    let mut revised = original.clone();
    revised.result_type = integer(true);
    let id = b
        .add(
            None,
            integer(false),
            ExprKind::FunctionCall {
                function: original.clone(),
                args: Box::default(),
            },
        )
        .unwrap();
    let c = Control::good();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Validate).unwrap();
    assert!(
        b.types_mut()
            .expression_type(id, &integer(false), integer(true), &mut work)
            .is_err()
    );
    let mut wrong = revised.clone();
    wrong.function_id = FunctionId::try_new("fixture/unit/different").unwrap();
    assert!(
        b.types_mut()
            .scalar_signature(id, &original, wrong, &mut work)
            .is_err()
    );
    assert_eq!(b.definition(id).unwrap().ty, integer(false));
    b.types_mut()
        .scalar_signature(id, &original, revised.clone(), &mut work)
        .unwrap();
    assert_eq!(b.definition(id).unwrap().ty, integer(true));
    let ExprKind::FunctionCall {
        function: actual, ..
    } = &b.definition(id).unwrap().kind
    else {
        panic!("call lifecycle changed")
    };
    assert_eq!(actual, &revised);
    b.request(id, request(&revised)).unwrap();
    b.definition_control(
        id,
        DefinitionControl {
            shape: ControlShape::Eager,
            temporal_sources: None,
        },
    )
    .unwrap();
    b.root(id, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let sealed = b.seal(vec![], &mut work).unwrap();
    with_call_claims(sealed, ArgumentControl::Eager, &mut work).unwrap();
    work.finish().unwrap();
}
