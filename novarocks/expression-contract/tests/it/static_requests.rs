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

// Proposed tests/it/static_requests.rs; add `mod static_requests;` in main.rs.
// Full scalar properties move from physical-plan's retired fragment arena.
use super::support::{Control as SetupControl, claim, function, limits};
use arrow_array::{Array, Int64Array};
use arrow_schema::{DataType, Field};
use novarocks_constant_contract::{
    ConstantPolicy, ConstantPool, ConstantPoolId, ConstantPools, ConstantReference, ConstantValue,
};
use novarocks_expression_contract::*;
use novarocks_failure_contract::{FailureClass, FailureProjection};
use novarocks_function_contract::FunctionArgument;
use novarocks_type_contract::*;
use std::sync::{Arc, Mutex};

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
struct ScopeControl {
    phase: CompilePhase,
    stop: Option<(usize, CompileControlError)>,
    trace: Mutex<Vec<(CompilePhase, u32)>>,
}
impl ScopeControl {
    fn new(phase: CompilePhase, stop: Option<(usize, CompileControlError)>) -> Self {
        Self {
            phase,
            stop,
            trace: Mutex::default(),
        }
    }
}
impl PureCompileControl for ScopeControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, self.phase);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        if let Some((at, _)) = self.stop {
            assert!(trace.len() < at, "callback after original refusal");
        }
        trace.push((phase, units));
        match &self.stop {
            Some((at, cause)) if *at == trace.len() => Err(cause.clone()),
            _ => Ok(()),
        }
    }
}
fn integer(nullable: bool) -> ValueType {
    ValueType::new(DataType::Int64, nullable)
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 32,
        max_logical_elements: 128,
        max_retained_buffer_bytes: 64 * 1024,
        max_type_depth: 16,
        max_type_nodes: 128,
        max_dictionary_depth: 8,
        max_metadata_bytes: 4096,
        max_library_validation_work: 1_000_000,
        max_library_validation_bytes: 1024 * 1024,
    }
}
fn request(arguments: Vec<FunctionArgument<ConstantReference>>) -> PhysicalCallRequest {
    PhysicalCallRequest {
        logical_argument_count: arguments.len(),
        arguments: arguments.into_boxed_slice(),
        expected_result_type: None,
    }
}
fn value(ty: ValueType) -> FunctionArgument<ConstantReference> {
    FunctionArgument::Value {
        value_type: ty,
        constant: None,
    }
}
fn nested(root_nullable: bool, child_nullable: bool, metadata: &str) -> ValueType {
    ValueType::new(
        DataType::Struct(
            vec![Arc::new(
                Field::new(
                    format!("original.child.{metadata}"),
                    DataType::Int64,
                    child_nullable,
                )
                .with_metadata(
                    [("source".to_owned(), metadata.to_owned())]
                        .into_iter()
                        .collect::<std::collections::HashMap<_, _>>(),
                ),
            )]
            .into(),
        ),
        root_nullable,
    )
}
fn original_request(types: &[FunctionArgumentType]) -> PhysicalCallRequest {
    request(
        types
            .iter()
            .map(|ty| match ty {
                FunctionArgumentType::Value(ty) => value(ty.clone()),
                FunctionArgumentType::Lambda {
                    parameter_types,
                    result_type,
                } => FunctionArgument::Lambda {
                    parameter_types: parameter_types.clone(),
                    result_type: result_type.clone(),
                },
            })
            .collect(),
    )
}

// Valid emitted definitions are independent of candidate static requests. A
// TypeOnly parent retains every Lambda definition but has no invented runtime
// child use/control/claim. Real NULL backing stays with the caller through seal.
fn scalar_unit(types: Vec<FunctionArgumentType>) -> (BoundExprSet, ConstantPools) {
    let mut builder = BoundExprSetBuilder::new(limits());
    let mut arguments = Vec::new();
    let mut ports = Vec::new();
    let mut pools = ConstantPools::empty();
    for (ordinal, ty) in types.iter().enumerate() {
        let argument = match ty {
            FunctionArgumentType::Value(ty) => {
                let value = ValueId::new(ordinal as u32);
                ports.push((value, ty.clone()));
                builder
                    .add(None, ty.clone(), ExprKind::Value(value))
                    .unwrap()
            }
            FunctionArgumentType::Lambda {
                parameter_types,
                result_type,
            } => {
                let lambda = builder.reserve().unwrap();
                let constant = ConstantValue::null(
                    Arc::new(result_type.try_to_field("original.lambda.body").unwrap()),
                    result_type.clone(),
                    policy(),
                    CompilePhase::Validate,
                    &SetupControl::good(),
                )
                .unwrap();
                let pool = ConstantPoolId::new(ordinal as u32);
                let reference = ConstantReference {
                    pool,
                    ordinal: constant.ordinal(),
                };
                pools.insert(pool, constant.pool().clone()).unwrap();
                let body = builder
                    .add(
                        Some(lambda),
                        result_type.clone(),
                        ExprKind::Constant(reference),
                    )
                    .unwrap();
                builder
                    .insert(
                        lambda,
                        None,
                        result_type.clone(),
                        ExprKind::Lambda {
                            parameter_types: parameter_types.to_vec().into_boxed_slice(),
                            body,
                        },
                    )
                    .unwrap();
                lambda
            }
        };
        arguments.push(argument);
    }
    let binding = function(
        "fixture/static-request/component",
        types.clone(),
        integer(false),
    );
    let call = builder
        .add(
            None,
            integer(false),
            ExprKind::FunctionCall {
                function: binding,
                args: arguments.into_boxed_slice(),
            },
        )
        .unwrap();
    builder.request(call, original_request(&types)).unwrap();
    builder
        .definition_control(
            call,
            DefinitionControl {
                shape: ControlShape::TypeOnly,
                temporal_sources: None,
            },
        )
        .unwrap();
    builder
        .root(call, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    let control = SetupControl::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let sealed = builder
        .seal(
            vec![ExprPort {
                values: ports.into_boxed_slice(),
            }],
            &mut work,
        )
        .unwrap();
    let claims = sealed
        .uses()
        .iter()
        .map(|(&id, invocation)| (id, claim(invocation.context, ArgumentControl::TypeOnly)))
        .collect();
    let unit = sealed.finish(claims, &mut work).unwrap();
    work.finish().unwrap();
    (unit, pools)
}
fn parts_with_request(
    types: &[FunctionArgumentType],
    original: &PhysicalCallRequest,
) -> (UnitParts, ConstantPools) {
    let (unit, pools) = scalar_unit(types.to_vec());
    let call = unit
        .definitions()
        .iter()
        .find(|n| matches!(n.kind, ExprKind::FunctionCall { .. }))
        .unwrap()
        .id;
    let mut parts = unit.into_parts();
    parts.requests.insert(call, original.clone());
    (parts, pools)
}
fn validate_parts(parts: UnitParts, control: &ScopeControl) -> Result<BoundExprSet, UnitError> {
    let mut work = CompileCheckpoints::try_new(control, control.phase)?;
    let result = BoundExprSet::try_from_parts_in(parts, limits(), &mut work);
    if matches!(&result, Err(UnitError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn prefixes(
    make: impl Fn() -> (UnitParts, ConstantPools),
    error_message: Option<&str>,
    phase: CompilePhase,
) {
    let control = ScopeControl::new(phase, None);
    let (parts, _sources) = make();
    let result = validate_parts(parts, &control);
    match error_message {
        None => assert!(result.is_ok(), "{result:?}"),
        Some(message) => {
            let error = result.unwrap_err();
            assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
            assert!(error.to_string().contains(message), "{error}");
        }
    }
    let trace = control.trace.into_inner().unwrap();
    assert_eq!(trace.first(), Some(&(phase, 0)));
    assert!(trace.len() >= 2);
    for at in 1..=trace.len() {
        for cause in CAUSES {
            let control = ScopeControl::new(phase, Some((at, cause.clone())));
            let (parts, _sources) = make();
            assert!(
                matches!(validate_parts(parts, &control), Err(UnitError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
        }
    }
}

fn type_only_source() -> BoundExprSet {
    let mut builder = BoundExprSetBuilder::new(limits());
    let child_function = function("fixture/static-request/hidden", vec![], integer(false));
    let child = builder
        .add(
            None,
            integer(false),
            ExprKind::FunctionCall {
                function: child_function,
                args: Box::default(),
            },
        )
        .unwrap();
    builder.request(child, request(vec![])).unwrap();
    let parent_function = function(
        "fixture/static-request/type-only",
        vec![FunctionArgumentType::Value(integer(false))],
        integer(false),
    );
    let parent = builder
        .add(
            None,
            integer(false),
            ExprKind::FunctionCall {
                function: parent_function,
                args: Box::from([child]),
            },
        )
        .unwrap();
    builder
        .request(parent, request(vec![value(integer(false))]))
        .unwrap();
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
    let control = SetupControl::good();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let sealed = builder.seal(vec![], &mut work).unwrap();
    assert_eq!(sealed.uses().len(), 1);
    assert_eq!(sealed.uses().values().next().unwrap().definition, parent);
    assert!(!sealed.uses().values().any(|u| u.definition == child));
    assert!(sealed.uses().values().next().unwrap().arguments.is_empty());
    let claims = sealed
        .uses()
        .iter()
        .map(|(&id, u)| (id, claim(u.context, ArgumentControl::TypeOnly)))
        .collect();
    let unit = sealed.finish(claims, &mut work).unwrap();
    work.finish().unwrap();
    unit
}

#[test]
fn static_requests_cover_dead_and_type_only_definitions_without_runtime_use_fallback() {
    for phase in [CompilePhase::Validate, CompilePhase::LowerProgram] {
        prefixes(
            || (type_only_source().into_parts(), ConstantPools::empty()),
            None,
            phase,
        );
        for missing in [ExprId::new(0), ExprId::new(1)] {
            prefixes(
                || {
                    let mut parts = type_only_source().into_parts();
                    assert!(parts.requests.remove(&missing).is_some());
                    (parts, ConstantPools::empty())
                },
                Some("function definition lacks its original request"),
                phase,
            );
        }
        prefixes(
            || {
                let mut parts = type_only_source().into_parts();
                parts
                    .requests
                    .insert(ExprId::new(u32::MAX), request(vec![]));
                (parts, ConstantPools::empty())
            },
            Some("original requests include a foreign or non-call definition"),
            phase,
        );
    }
}

#[test]
fn static_requests_keep_original_none_some_constraint_and_reference_address() {
    // The historical MAX/3 address is now backed by its actual pool/ordinal.
    let array = Int64Array::from(vec![11, 22, 33, 44]);
    let pool = ConstantPool::try_new(
        Arc::new(Field::new("original.constant", DataType::Int64, false)),
        integer(false),
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &SetupControl::good(),
    )
    .unwrap();
    let address = ConstantReference {
        pool: ConstantPoolId::new(u32::MAX),
        ordinal: 3,
    };
    let mut pools = ConstantPools::empty();
    pools.insert(address.pool, pool.clone()).unwrap();
    for constraint in [None, Some(integer(true))] {
        for constant in [None, Some(address)] {
            let original = PhysicalCallRequest {
                expected_result_type: constraint.clone(),
                ..request(vec![FunctionArgument::Value {
                    value_type: integer(false),
                    constant,
                }])
            };
            let types = vec![FunctionArgumentType::Value(integer(true))];
            let (parts, _lambda_sources) = parts_with_request(&types, &original);
            let control = ScopeControl::new(CompilePhase::LowerProgram, None);
            let unit = validate_parts(parts, &control).unwrap();
            let retained = unit.requests().values().next().unwrap();
            assert_eq!(retained, &original);
            assert_eq!(retained.request().expected_result_type, constraint.as_ref());
            assert!(std::ptr::eq(
                retained.request().arguments,
                retained.arguments.as_ref()
            ));
            assert!(
                matches!(retained.arguments[0], FunctionArgument::Value { constant: actual, .. } if actual == constant)
            );
            let setup = SetupControl::good();
            let mut work = CompileCheckpoints::try_new(&setup, CompilePhase::Validate).unwrap();
            let materialized =
                materialize_call_request_observed(retained, &pools, &mut work, 1).unwrap();
            match (
                retained.expected_result_type.as_ref(),
                materialized.request().expected_result_type,
            ) {
                (None, None) => {}
                (Some(original), Some(actual)) => assert!(std::ptr::eq(original, actual)),
                _ => panic!("materialization changed the optional result constraint"),
            }
            if let novarocks_function_contract::FunctionArgument::Value {
                constant: actual, ..
            } = &materialized.request().arguments[0]
            {
                match (constant, actual) {
                    (None, None) => {}
                    (Some(expected), Some(actual)) => {
                        assert_eq!(actual.ordinal(), expected.ordinal);
                        assert!(Arc::ptr_eq(actual.pool().array(), pool.array()));
                        assert_eq!(actual.try_i64().unwrap(), Some(44));
                    }
                    _ => panic!("materialization changed None/Some"),
                }
            } else {
                panic!("value request changed channel");
            }
            work.finish().unwrap();
            prefixes(
                || parts_with_request(&types, &original),
                None,
                CompilePhase::Validate,
            );
        }
    }
}

#[test]
fn static_requests_value_nullable_covariance_and_lambda_complete_exactness_stay_distinct() {
    for phase in [CompilePhase::Validate, CompilePhase::LowerProgram] {
        let wider = vec![FunctionArgumentType::Value(nested(true, true, "original"))];
        let narrow = request(vec![value(nested(false, false, "original"))]);
        prefixes(|| parts_with_request(&wider, &narrow), None, phase);
        let strict = vec![FunctionArgumentType::Value(nested(
            false, false, "original",
        ))];
        for actual in [
            nested(true, false, "original"),
            nested(false, true, "original"),
        ] {
            let wrong = request(vec![value(actual)]);
            prefixes(
                || parts_with_request(&strict, &wrong),
                Some("original request argument type differs"),
                phase,
            );
        }
        let nominal = ValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::LargeInt,
        )
        .unwrap();
        let lambda_types = vec![FunctionArgumentType::Lambda {
            parameter_types: TypedFactSlice::from_pure_boxed(Box::from([
                nominal.clone(),
                nested(false, false, "original"),
            ])),
            result_type: integer(true),
        }];
        let lambda = |nominal, metadata: &str, result| FunctionArgument::Lambda {
            parameter_types: TypedFactSlice::from_pure_boxed(Box::from([
                nominal,
                nested(false, false, metadata),
            ])),
            result_type: result,
        };
        let correct = request(vec![lambda(nominal.clone(), "original", integer(true))]);
        prefixes(|| parts_with_request(&lambda_types, &correct), None, phase);
        for wrong in [
            request(vec![lambda(
                ValueType::new(DataType::FixedSizeBinary(16), false),
                "original",
                integer(true),
            )]),
            request(vec![lambda(nominal.clone(), "foreign", integer(true))]),
            request(vec![lambda(nominal.clone(), "original", integer(false))]),
            request(vec![value(integer(true))]),
        ] {
            prefixes(
                || parts_with_request(&lambda_types, &wrong),
                Some("original request argument type differs"),
                phase,
            );
        }
        let missing = request(vec![]);
        prefixes(
            || parts_with_request(&lambda_types, &missing),
            Some("original request argument counts differ"),
            phase,
        );
    }
    // The original caller-owned test has exactly one nested parameter. Keep
    // that complete source shape and every original mutation on LowerProgram.
    let single_types = vec![FunctionArgumentType::Lambda {
        parameter_types: TypedFactSlice::from_pure_boxed(Box::from([nested(
            false, false, "original",
        )])),
        result_type: integer(true),
    }];
    let single = |metadata: &str, result| FunctionArgument::Lambda {
        parameter_types: TypedFactSlice::from_pure_boxed(Box::from([nested(
            false, false, metadata,
        )])),
        result_type: result,
    };
    let original = request(vec![single("original", integer(true))]);
    prefixes(
        || parts_with_request(&single_types, &original),
        None,
        CompilePhase::LowerProgram,
    );
    let wrong_count = request(vec![]);
    prefixes(
        || parts_with_request(&single_types, &wrong_count),
        Some("original request argument counts differ"),
        CompilePhase::LowerProgram,
    );
    for wrong in [
        request(vec![value(integer(true))]),
        request(vec![single("foreign", integer(true))]),
        request(vec![single("original", integer(false))]),
    ] {
        prefixes(
            || parts_with_request(&single_types, &wrong),
            Some("original request argument type differs"),
            CompilePhase::LowerProgram,
        );
    }
}
