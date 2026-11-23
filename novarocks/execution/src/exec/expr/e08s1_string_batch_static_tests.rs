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
//! Exact static profiles borrow ONE original CPU predicate, never dynamic length values.
use super::string_batch_sql_source_tests::{NAMES, calls};
use arrow::datatypes::DataType;
use novarocks_functions::{
    FunctionArgument, FunctionArgumentType, FunctionBindingError, FunctionResultType,
    FunctionValueType, PureCallLifecycle, ResolvedFunctionBinding,
};
use novarocks_sql::compiler::{SqlCompileControl, builtin_sql_function_catalog};
use novarocks_type_contract::{
    CharacterKind, CharacterType, CompileControlError, CompilePhase, PureCompileControl, ValueKind,
    ValueLayout, ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
};
use std::sync::Mutex;
fn full_binding(name: &str, mask: usize) -> ResolvedFunctionBinding {
    // Exact test inputs use original declared argument types; the original
    // resolver remains the nullability/result/overload author.
    let physical = |ty| FunctionValueType::new(ty, false);
    let types = match name {
        "chr" => vec![physical(DataType::Int64)],
        "codepoint" => {
            let storage = ValueTypeStorageLifetime::pure();
            let character = CharacterType::new(CharacterKind::Varchar, 1).unwrap();
            let value = ValueTypeRef::try_new(
                ValueKind::BoundedCharacter(character),
                false,
                storage.clone(),
            )
            .unwrap();
            let layout = ValueLayout::try_new(&DataType::Utf8, storage).unwrap();
            vec![FunctionValueType::try_from_value(value, layout).unwrap()]
        }
        "length" => vec![physical(DataType::Utf8)],
        "lpad" | "rpad" => vec![
            physical(DataType::Utf8),
            physical(DataType::Int64),
            physical(DataType::Utf8),
        ],
        _ => unreachable!(),
    };
    let arguments = types
        .into_iter()
        .enumerate()
        .map(|(index, ty)| FunctionArgument::Value {
            value_type: ty.with_nullable(mask & (1 << index) != 0).unwrap(),
            constant: None,
        })
        .collect::<Vec<_>>();
    builtin_sql_function_catalog()
        .snapshot()
        .resolve_scalar_binding(name, &arguments, &SqlCompileControl::unbounded())
        .unwrap()
}

fn replace_argument_type(
    types: &mut novarocks_type_contract::FunctionArgumentTypes,
    index: usize,
    replacement: FunctionArgumentType,
) {
    let mut revised = types.to_vec();
    revised[index] = replacement;
    *types =
        novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(revised.into_boxed_slice());
}

#[test]
fn e08s1_string_batch_static_full_families_nullable_axes_and_original_dynamic_caps_stay_delayed() {
    let original = builtin_sql_function_catalog().snapshot();
    let scoped = original.snapshot_for_scalar_presence();
    let control = SqlCompileControl::unbounded();
    {
        for index in 0..3 {
            for (_, binding) in calls(index) {
                scoped
                    .admit_bound_lifecycle_observed(&binding, PureCallLifecycle::Scalar, &control)
                    .unwrap();
            }
        }
    }
    for name in NAMES {
        let first = full_binding(name, 0);
        let arity = first.logical_argument_count;
        for mask in 0..(1 << arity) {
            let binding = full_binding(name, mask);
            scoped
                .admit_bound_lifecycle_observed(&binding, PureCallLifecycle::Scalar, &control)
                .unwrap();
            original
                .admit_bound_lifecycle_observed(&binding, PureCallLifecycle::Scalar, &control)
                .unwrap();
        }
    }
}
#[test]
fn e08s1_string_batch_static_exact_arity_nominal_result_and_nullability_refusal() {
    let original = builtin_sql_function_catalog().snapshot();
    let scoped = original.snapshot_for_scalar_presence();
    let control = SqlCompileControl::unbounded();
    for name in NAMES {
        let binding = full_binding(name, 0);
        let mut wrong = Vec::new();
        let wrong_carrier = if name == "length" {
            DataType::Boolean
        } else {
            DataType::Binary
        };
        for index in 0..binding.selected.argument_types.len() {
            let mut bad = binding.clone();
            replace_argument_type(
                &mut bad.selected.argument_types,
                index,
                FunctionArgumentType::Value(FunctionValueType::new(wrong_carrier.clone(), true)),
            );
            wrong.push(bad);
            if matches!(&binding.selected.argument_types[index],FunctionArgumentType::Value(v) if *v.data_type()==DataType::Utf8)
            {
                let mut bad = binding.clone();
                replace_argument_type(
                    &mut bad.selected.argument_types,
                    index,
                    FunctionArgumentType::Value(
                        FunctionValueType::try_with_logical_type(
                            DataType::Utf8,
                            true,
                            ValueLogicalType::Json,
                        )
                        .unwrap(),
                    ),
                );
                wrong.push(bad);
                let mut bad = binding.clone();
                replace_argument_type(
                    &mut bad.selected.argument_types,
                    index,
                    FunctionArgumentType::Value(FunctionValueType::new(DataType::LargeUtf8, true)),
                );
                wrong.push(bad);
            }
        }
        let mut bad = binding.clone();
        let FunctionResultType::Scalar(v) = &mut bad.selected.result_type else {
            unreachable!()
        };
        *v = v
            .with_nullable(!v.nullable())
            .expect("valid test root nullability");
        wrong.push(bad);
        let mut bad = binding.clone();
        let FunctionResultType::Scalar(v) = &mut bad.selected.result_type else {
            unreachable!()
        };
        *v = FunctionValueType::new(DataType::Binary, v.nullable());
        wrong.push(bad);
        let mut bad = binding.clone();
        bad.selected.result_type = FunctionResultType::Relation(
            novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(Box::new([
                FunctionValueType::new(DataType::Utf8, true),
            ])),
        );
        wrong.push(bad);
        let mut bad = binding.clone();
        bad.selected.argument_types =
            novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(Box::new([]));
        bad.logical_argument_count = 0;
        wrong.push(bad);
        let mut bad = binding.clone();
        let mut t = bad.selected.argument_types.to_vec();
        t.push(t[0].clone());
        bad.selected.argument_types =
            novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(t.into_boxed_slice());
        bad.logical_argument_count += 1;
        wrong.push(bad);
        for stale in wrong {
            // Corrupted selected metadata probes retain the real overload. They
            // are not manually fabricated Physical input plans.
            original
                .admit_bound_lifecycle_observed(&stale, PureCallLifecycle::Scalar, &control)
                .unwrap();
            assert!(
                matches!(scoped.admit_bound_lifecycle_observed(&stale,PureCallLifecycle::Scalar,&control),Err(FunctionBindingError::UnavailableImplementation(id)) if id==binding.selected.overload),
                "{name}"
            );
        }
    }
}
struct Trace {
    events: Mutex<Vec<(CompilePhase, u32)>>,
    refuse: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Trace {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut events = self.events.lock().unwrap();
        let index = events.len();
        if let Some((stop, _)) = self.refuse {
            assert!(index <= stop, "callback after first cause")
        }
        events.push((phase, units));
        if let Some((stop, cause)) = self.refuse.clone()
            && index == stop
        {
            return Err(cause);
        }
        Ok(())
    }
}
#[test]
fn e08s1_string_batch_static_all_three_compile_causes_keep_actual_prefix_and_original_no_work() {
    let original = builtin_sql_function_catalog().snapshot();
    let scoped = original.snapshot_for_scalar_presence();
    for name in NAMES {
        let binding = full_binding(name, 0);
        let c = Trace {
            events: Mutex::new(Vec::new()),
            refuse: None,
        };
        scoped
            .admit_bound_lifecycle_observed(&binding, PureCallLifecycle::Scalar, &c)
            .unwrap();
        let expected = c.events.into_inner().unwrap();
        assert!(!expected.is_empty());
        for stop in 0..expected.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let c = Trace {
                    events: Mutex::new(Vec::new()),
                    refuse: Some((stop, cause.clone())),
                };
                original
                    .admit_bound_lifecycle_observed(&binding, PureCallLifecycle::Scalar, &c)
                    .unwrap();
                assert!(c.events.lock().unwrap().is_empty());
                assert!(
                    matches!(scoped.admit_bound_lifecycle_observed(&binding,PureCallLifecycle::Scalar,&c),Err(FunctionBindingError::Control(actual)) if actual==cause)
                );
                assert_eq!(*c.events.lock().unwrap(), expected[..=stop]);
            }
        }
    }
}
