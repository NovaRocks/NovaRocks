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
use crate::{CallRequestError, PhysicalCallRequest, PhysicalCallSite};
use novarocks_function_contract::FunctionArgument;
use novarocks_type_contract::{CompilePhase, PureCompileControl};
use std::sync::Mutex;

const PHASE: CompilePhase = CompilePhase::Validate;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, PHASE);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        if let Some((at, _)) = self.refusal {
            assert!(trace.len() < at, "callback after original refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((at, cause)) if trace.len() == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn integer() -> ValueType {
    ValueType::new(DataType::Int64, true)
}
fn request(arguments: Vec<FunctionArgument<crate::ConstantReference>>) -> PhysicalCallRequest {
    PhysicalCallRequest {
        logical_argument_count: arguments.len(),
        arguments: arguments.into_boxed_slice(),
        expected_result_type: None,
    }
}
fn mixed_request() -> PhysicalCallRequest {
    let mut request = request(vec![
        FunctionArgument::Value {
            value_type: integer(),
            constant: Some(crate::ConstantReference {
                pool: crate::ConstantPoolId::new(u32::MAX),
                ordinal: 1,
            }),
        },
        FunctionArgument::Lambda {
            parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
                integer(),
                integer(),
            ])),
            result_type: integer(),
        },
    ]);
    request.expected_result_type = Some(integer());
    request
}
fn finish(
    operation: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<(), CallRequestError>,
    control: &dyn PureCompileControl,
) -> Result<(), CallRequestError> {
    let mut work = CompileCheckpoints::try_new(control, PHASE)?;
    let result = operation(&mut work);
    if matches!(result, Err(CallRequestError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn source(
    requests: &[(PhysicalCallSite, PhysicalCallRequest)],
    capacity: usize,
    control: &dyn PureCompileControl,
) -> Result<(), CallRequestError> {
    finish(
        |work| validate_call_request_source_observed(requests, capacity, work),
        control,
    )
}
fn checked_empty_requests(count: usize) -> crate::Fragment {
    use novarocks_type_contract::*;
    let mut builder = crate::FragmentBuilder::new(crate::FragmentId::new(9));
    let input = crate::NodeId::new(99);
    builder
        .add_values(input, Box::from([Box::default()]), Box::default())
        .unwrap();
    let owner = crate::NodeId::new(0);
    let binding = crate::BoundFunction::from_exact_signature(
        FunctionId::try_new("fixture/resource-request/window").unwrap(),
        FunctionOverloadId::try_new("fixture/resource-request/window-zero").unwrap(),
        FunctionKind::Window,
        TypedFactSlice::from_pure_boxed(Box::default()),
        integer(),
    );
    let mut calls = Vec::new();
    let mut outputs = Vec::new();
    let mut requests = Vec::new();
    let mut claims = Vec::new();
    for ordinal in 0..count {
        let ordinal = u32::try_from(ordinal).unwrap();
        let output = builder
            .add_value(
                integer(),
                crate::ValueOrigin::NodeOutput {
                    node: owner,
                    output_ordinal: ordinal,
                },
            )
            .unwrap();
        outputs.push(output);
        calls.push(crate::WindowCallSpec {
            binding: binding.clone(),
            distinct: false,
            arguments: Box::default(),
            order_by: Box::default(),
            frame: None,
            ignore_nulls: false,
            aggregate_binding: None,
            output,
        });
        let site = crate::PhysicalCallSite::Window {
            node: owner,
            call: ordinal,
        };
        requests.push((site, request(Vec::new())));
        let context = RelationEffectContext::call(RelationCallId::new(ordinal));
        claims.push(crate::FrozenPhysicalCall {
            site,
            context,
            materialized_writer_input: None,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            effects: CallEffects {
                value_stability: FunctionVolatility::Immutable,
                own_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
                failure_behavior: FunctionFailureBehavior::Propagate,
                null_behavior: FunctionNullBehavior::CalledOnNull,
                argument_control: ArgumentControl::Window,
                instance_state: FunctionInstanceState::WindowPartition,
                observable_effects: ObservableEffects::NONE,
                environment: Box::default(),
                proof_scope: CallProofScope::Relation(context.identity),
            },
        });
    }
    let properties = builder.node_output_properties(input).unwrap().clone();
    builder
        .insert_node_unchecked(crate::PhysicalNode {
            id: owner,
            inputs: Box::from([input]),
            required_inputs: Box::from([properties.clone()]),
            output_properties: properties,
            output: crate::OutputPort {
                node: owner,
                columns: outputs.into_boxed_slice(),
            },
            expressions: None,
            kind: crate::NodeKind::Window(crate::WindowSpec {
                partition_by: Box::default(),
                order_by: Box::default(),
                calls: calls.into_boxed_slice(),
            }),
        })
        .unwrap();
    builder
        .finish_structure(
            owner,
            crate::FragmentSink::Noop,
            crate::PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            crate::PlanLimits::FROZEN,
            &Control::default(),
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap()
        .with_calls_observed(claims, &Control::default())
        .unwrap()
        .with_call_requests_observed(requests, &Control::default())
        .unwrap()
}

fn prefixes(
    operation: impl Fn(&dyn PureCompileControl) -> Result<(), CallRequestError>,
    expected: Result<(), CallRequestError>,
) {
    let baseline = Control::default();
    assert_eq!(operation(&baseline), expected);
    let trace = baseline.trace.into_inner().unwrap();
    assert_eq!(trace.first(), Some(&(PHASE, 0)));
    for at in 1..=trace.len() {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert_eq!(operation(&control), Err(CallRequestError::Control(cause)));
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
        }
    }
}

#[test]
fn request_owned_arguments_and_lambda_types_preserve_the_sole_resource_numerics() {
    let request = mixed_request();
    let mut expected =
        ResourceUsage::limited(MAX_FRAGMENT_DYNAMIC_ITEMS, MAX_FRAGMENT_DYNAMIC_BYTES);
    let mut errors = ValidationContext::new();
    add_call_request_usage(&request, &mut expected, &mut errors, &mut |_| {
        Ok::<(), std::convert::Infallible>(())
    })
    .unwrap();
    assert!(errors.is_empty());
    assert_eq!(expected.items, 9);
    assert_eq!(
        expected.bytes,
        2 * std::mem::size_of::<FunctionArgument<crate::ConstantReference>>()
            + 2 * std::mem::size_of::<ValueType>()
    );
    let mut actual = ResourceUsage::limited(MAX_FRAGMENT_DYNAMIC_ITEMS, MAX_FRAGMENT_DYNAMIC_BYTES);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
    add_call_request_usage(&request, &mut actual, &mut errors, &mut |opaque| {
        if opaque { work.flush() } else { work.step() }
    })
    .unwrap();
    work.finish().unwrap();
    assert_eq!(
        (actual.items, actual.bytes),
        (expected.items, expected.bytes)
    );
    assert_eq!(
        control
            .trace
            .lock()
            .unwrap()
            .iter()
            .map(|(_, units)| usize::try_from(*units).unwrap())
            .sum::<usize>(),
        9
    );
}

#[test]
fn request_source_capacity_and_owned_extents_refuse_before_the_unadmitted_walk() {
    let requests = vec![(
        PhysicalCallSite::Table {
            node: crate::NodeId::new(0),
        },
        mixed_request(),
    )];
    let control = Control::default();
    assert_eq!(
        source(&requests, usize::MAX, &control),
        Err(CallRequestError::Control(
            CompileControlError::ResourceExhausted
        ))
    );
    assert_eq!(*control.trace.lock().unwrap(), vec![(PHASE, 0)]);
    let mut usage = ResourceUsage::limited(1, MAX_FRAGMENT_DYNAMIC_BYTES);
    let mut errors = ValidationContext::new();
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
    add_call_request_usage(&requests[0].1, &mut usage, &mut errors, &mut |opaque| {
        if opaque { work.flush() } else { work.step() }
    })
    .unwrap();
    assert!(usage.exhausted());
    assert_eq!(*control.trace.lock().unwrap(), vec![(PHASE, 0)]);
    assert!(errors.is_empty());
    // The real source Vec capacity coexists with destination headers. No type
    // or backing scratch is reached after its numerical primary refusal.
}

#[test]
fn request_resource_success_preserves_control_prefix_and_type_author_refuses_invalid_requests() {
    let requests = vec![(
        PhysicalCallSite::Table {
            node: crate::NodeId::new(u32::MAX),
        },
        mixed_request(),
    )];
    prefixes(
        |control| source(&requests, requests.capacity(), control),
        Ok(()),
    );
    assert!(
        ValueType::try_with_logical_type(
            DataType::FixedSizeBinary(-1),
            true,
            novarocks_type_contract::ValueLogicalType::Physical
        )
        .is_err()
    );
    let fragment = checked_empty_requests(2);
    prefixes(
        |control| {
            finish(
                |work| validate_call_request_table_observed(fragment.call_requests(), work),
                control,
            )
        },
        Ok(()),
    );
}

#[test]
fn request_table_and_source_wide_owned_loops_have_real_quantum_without_a_type_internal_claim() {
    let fragment = checked_empty_requests(320);
    let requests: Vec<_> = fragment
        .call_requests()
        .entries()
        .iter()
        .map(|(key, request)| (*key, request.clone()))
        .collect();
    for table in [false, true] {
        let operation = |control: &dyn PureCompileControl| {
            if table {
                finish(
                    |work| validate_call_request_table_observed(fragment.call_requests(), work),
                    control,
                )
            } else {
                source(&requests, requests.capacity(), control)
            }
        };
        let baseline = Control::default();
        operation(&baseline).unwrap();
        let trace = baseline.trace.into_inner().unwrap();
        assert_eq!(trace, vec![(PHASE, 0), (PHASE, 256), (PHASE, 64)]);
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((2, cause.clone())),
            };
            assert_eq!(operation(&control), Err(CallRequestError::Control(cause)));
            assert_eq!(*control.trace.lock().unwrap(), trace[..2]);
        }
    }
    let mut usage = ResourceUsage::limited(MAX_FRAGMENT_DYNAMIC_ITEMS, MAX_FRAGMENT_DYNAMIC_BYTES);
    let mut errors = ValidationContext::new();
    add_call_request_table_usage(fragment.call_requests(), &mut usage, &mut errors);
    assert!(errors.is_empty());
    assert_eq!(usage.items, 320);
    assert_eq!(
        usage.bytes,
        320 * std::mem::size_of::<(PhysicalCallSite, PhysicalCallRequest)>()
    );
}
