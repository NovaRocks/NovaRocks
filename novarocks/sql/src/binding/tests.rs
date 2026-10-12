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
use arrow::datatypes::{DataType, Field};
use novarocks_functions::{AggregateStateFormatIdentity, FunctionId, FunctionOverloadId};
use novarocks_type_contract::{
    CompileControlError, DecimalOverflowPolicy, FunctionArgumentEvaluation,
    FunctionFailureBehavior, FunctionIntrinsicRowError, FunctionKind, FunctionVolatility,
    ValueLogicalType,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

fn actual() -> SqlFunctionBinding {
    crate::functions::test_resolved_aggregate("sum", &[DataType::Int64], false)
}
fn changed(
    binding: &SqlFunctionBinding,
    mutate: impl FnOnce(&mut novarocks_functions::ResolvedFunctionBinding),
) -> SqlFunctionBinding {
    let mut resolved = binding.resolved().clone();
    mutate(&mut resolved);
    SqlFunctionBinding::new(resolved, binding.decimal_overflow_policy())
}
fn control() -> crate::compiler::SqlCompileControl {
    crate::compiler::SqlCompileControl::unbounded()
}
fn exact(left: &SqlFunctionBinding, right: &SqlFunctionBinding) -> bool {
    left.equals_observed(right, CompilePhase::Validate, &control())
        .unwrap()
}

#[test]
fn borrowed_binding_author_preserves_original_scalar_interner_field_order() {
    let binding = actual();
    let mut fields = Vec::new();
    visit_fields(&binding, |field| {
        fields.push(match field {
            BindingField::Number(_) => 0,
            BindingField::Bytes(_) => 1,
            BindingField::ValueType(_) => 2,
        });
        Ok::<_, std::convert::Infallible>(())
    })
    .unwrap();
    // One value argument, scalar result and the actual aggregate state recipe.
    assert_eq!(fields, [0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 2, 0, 2, 0, 2, 1]);
    let mut numbers = Vec::new();
    visit_fields(&binding, |field| {
        if let BindingField::Number(value) = field {
            numbers.push(value);
        }
        Ok::<_, std::convert::Infallible>(())
    })
    .unwrap();
    assert_eq!(numbers[0], DecimalOverflowPolicy::OutputNull as u128);
    assert_eq!(numbers[6], 1); // Actual logical argument count.
    assert_eq!(*numbers.last().unwrap(), 1); // Actual aggregate state present.
}

#[test]
fn observed_binding_identity_retains_policy_overload_arguments_results_and_aggregate_state() {
    let binding = actual();
    assert!(exact(&binding, &binding.clone()));
    let other_allocation = SqlFunctionBinding::new(
        binding.resolved().clone(),
        binding.decimal_overflow_policy(),
    );
    assert!(exact(&binding, &other_allocation));
    assert_eq!(
        binding
            .fingerprint_observed(CompilePhase::Encode, &control())
            .unwrap(),
        other_allocation
            .fingerprint_observed(CompilePhase::Encode, &control())
            .unwrap()
    );
    let variants = [
        SqlFunctionBinding::new(
            binding.resolved().clone(),
            DecimalOverflowPolicy::ReportError,
        ),
        changed(&binding, |b| {
            b.function_id = FunctionId::try_new("foreign.aggregate/sum/v1").unwrap()
        }),
        changed(&binding, |b| {
            b.selected.overload =
                FunctionOverloadId::try_new("foreign.aggregate/sum/overload-v1").unwrap()
        }),
        changed(&binding, |b| {
            b.semantics.volatility = FunctionVolatility::Volatile
        }),
        changed(&binding, |b| b.kind = FunctionKind::Scalar),
        changed(&binding, |b| {
            b.semantics.argument_evaluation = FunctionArgumentEvaluation::ShortCircuit
        }),
        changed(&binding, |b| {
            b.semantics.failure_behavior = FunctionFailureBehavior::ReturnsNull
        }),
        changed(&binding, |b| {
            b.semantics.intrinsic_row_error = FunctionIntrinsicRowError::MayRaise
        }),
        changed(&binding, |b| b.logical_argument_count += 1),
        changed(&binding, |b| {
            b.selected.argument_types[0] =
                FunctionArgumentType::Value(FunctionValueType::new(DataType::Int32, true))
        }),
        changed(&binding, |b| {
            b.selected.result_type =
                FunctionResultType::Scalar(FunctionValueType::new(DataType::Int32, true))
        }),
        changed(&binding, |b| {
            b.selected.result_type = FunctionResultType::Relation(
                vec![FunctionValueType::new(DataType::Int64, true)].into_boxed_slice(),
            )
        }),
        changed(&binding, |b| {
            let state = b.selected.aggregate.as_mut().unwrap();
            state.intermediate_type.nullable = !state.intermediate_type.nullable;
        }),
        changed(&binding, |b| {
            b.selected.aggregate.as_mut().unwrap().state_format =
                AggregateStateFormatIdentity::try_new("foreign/sum-state/v1").unwrap()
        }),
        changed(&binding, |b| b.selected.aggregate = None),
    ];
    for other in variants {
        assert!(!exact(&binding, &other));
        assert!(!exact(&other, &binding));
    }
}

fn nested(reverse: bool, altered: bool) -> FunctionValueType {
    let mut metadata = HashMap::new();
    let entries = [
        ("first", "one"),
        ("second", if altered { "different" } else { "two" }),
    ];
    if reverse {
        for (key, value) in entries.iter().rev() {
            metadata.insert((*key).into(), (*value).into());
        }
    } else {
        for (key, value) in entries {
            metadata.insert(key.into(), value.into());
        }
    }
    let mut ty = DataType::Int64;
    for depth in 0..24 {
        ty = DataType::List(Arc::new(
            Field::new(format!("child_{depth}"), ty, true).with_metadata(metadata.clone()),
        ));
    }
    FunctionValueType::new(ty, true)
}

#[test]
fn observed_binding_types_cover_lambda_relation_and_deep_metadata_without_order_identity() {
    let base = actual();
    let make = |ty: FunctionValueType| {
        changed(&base, |b| {
            b.selected.argument_types = vec![FunctionArgumentType::Lambda {
                parameter_types: vec![ty.clone()].into_boxed_slice(),
                result_type: ty.clone(),
            }]
            .into_boxed_slice();
            b.selected.result_type =
                FunctionResultType::Relation(vec![ty.clone()].into_boxed_slice());
            b.selected.aggregate.as_mut().unwrap().intermediate_type = ty;
        })
    };
    let first = make(nested(false, false));
    let reordered = make(nested(true, false));
    assert!(exact(&first, &reordered));
    assert_eq!(
        first
            .fingerprint_observed(CompilePhase::Validate, &control())
            .unwrap(),
        reordered
            .fingerprint_observed(CompilePhase::Validate, &control())
            .unwrap()
    );
    assert!(!exact(&first, &make(nested(false, true))));
    let physical = changed(&base, |b| {
        b.selected.argument_types[0] =
            FunctionArgumentType::Value(FunctionValueType::new(DataType::FixedSizeBinary(16), true))
    });
    let uuid = changed(&physical, |b| {
        b.selected.argument_types[0] = FunctionArgumentType::Value(
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                true,
                ValueLogicalType::Uuid,
            )
            .unwrap(),
        )
    });
    assert!(!exact(&physical, &uuid));
    let mut nullable = nested(false, false);
    nullable.nullable = false;
    assert!(!exact(&first, &make(nullable)));
}

struct Recording {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<usize>,
    cause: CompileControlError,
}
impl PureCompileControl for Recording {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        trace.push((phase, units));
        if self.stop == Some(trace.len() - 1) {
            Err(self.cause)
        } else {
            Ok(())
        }
    }
}

#[test]
fn binding_observation_preserves_every_original_control_prefix_and_false_success_tails() {
    let first = changed(&actual(), |b| {
        b.selected.argument_types = (0..320)
            .map(|_| FunctionArgumentType::Value(FunctionValueType::new(DataType::Int64, true)))
            .collect::<Vec<_>>()
            .into_boxed_slice();
    });
    let last_mismatch = changed(&first, |b| {
        b.selected.aggregate.as_mut().unwrap().state_format =
            AggregateStateFormatIdentity::try_new("foreign/sum-state/v1").unwrap()
    });
    for (phase, operation) in [
        (CompilePhase::Encode, 0),
        (CompilePhase::Decode, 1),
        (CompilePhase::Validate, 2),
    ] {
        let run = |c: &Recording| match operation {
            0 => first.fingerprint_observed(phase, c).map(|_| true),
            1 => first.equals_observed(&first, phase, c),
            _ => first.equals_observed(&last_mismatch, phase, c),
        };
        let recording = Recording {
            trace: Default::default(),
            stop: None,
            cause: CompileControlError::Cancelled,
        };
        assert_eq!(run(&recording).unwrap(), operation != 2);
        let trace = recording.trace.into_inner().unwrap();
        assert_eq!(trace[0], (phase, 0));
        assert!(trace.iter().any(|(_, units)| *units == 256));
        assert!(trace.iter().all(|(p, units)| *p == phase && *units <= 256));
        for stop in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let refused = Recording {
                    trace: Default::default(),
                    stop: Some(stop),
                    cause,
                };
                assert!(matches!(run(&refused),Err(error) if error==SqlCompileError::from(cause)));
                assert_eq!(*refused.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}

#[test]
fn c16_p1_scalar_invocation_data_is_part_of_observed_binding_without_abi_admission() {
    let baseline = actual();
    let positive = changed(&baseline, |b| b.declared_scalar_invocation_data = true);
    assert_ne!(baseline.resolved(), positive.resolved());
    assert!(!exact(&baseline, &positive));
    assert_ne!(
        baseline
            .fingerprint_observed(CompilePhase::Encode, &control())
            .unwrap(),
        positive
            .fingerprint_observed(CompilePhase::Encode, &control())
            .unwrap()
    );
    assert!(exact(&positive, &positive.clone()));
    assert_eq!(
        baseline.admitted_execution_abi,
        positive.admitted_execution_abi
    );
    let mut count = 0;
    visit_fields(&baseline, |_| {
        count += 1;
        Ok::<_, std::convert::Infallible>(())
    })
    .unwrap();
    let mut positive_count = 0;
    visit_fields(&positive, |_| {
        positive_count += 1;
        Ok::<_, std::convert::Infallible>(())
    })
    .unwrap();
    assert_eq!(positive_count, count + 1);
}
