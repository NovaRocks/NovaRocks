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
//! Frozen before extraction through the original aggregate shell and state ABI.
use super::*;
use arrow::array::*;
use arrow::compute::{cast, take};
use arrow::datatypes::Field;
use serde_json::{Value, json};
use std::mem::MaybeUninit;

struct States {
    spec: AggSpec,
    slots: Vec<Box<MaybeUninit<MannWhitneyState>>>,
}
impl States {
    fn new(spec: &AggSpec, count: usize, tracker: &Arc<MemTracker>) -> Self {
        let mut slots = Vec::new();
        for _ in 0..count {
            let mut slot = Box::new(MaybeUninit::<MannWhitneyState>::uninit());
            MannWhitneyUTestAgg
                .init_state_with_tracker(spec, slot.as_mut_ptr().cast(), Some(tracker.clone()))
                .unwrap();
            slots.push(slot);
        }
        Self {
            spec: spec.clone(),
            slots,
        }
    }
    fn pointers(&mut self) -> Vec<AggStatePtr> {
        self.slots
            .iter_mut()
            .map(|slot| slot.as_mut_ptr() as AggStatePtr)
            .collect()
    }
}
impl Drop for States {
    fn drop(&mut self) {
        for slot in &mut self.slots {
            MannWhitneyUTestAgg.drop_state(&self.spec, slot.as_mut_ptr().cast());
        }
    }
}
fn input(columns: Vec<ArrayRef>) -> ArrayRef {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, column)| Field::new(format!("f{i}"), column.data_type().clone(), true))
        .collect::<Vec<_>>();
    Arc::new(StructArray::new(fields.into(), columns, None))
}
fn spec(input: &ArrayRef) -> AggSpec {
    let function = AggFunction {
        name: "mann_whitney_u_test".into(),
        types: Some(crate::exec::node::aggregate::AggTypeSignature {
            intermediate_type: Some(DataType::Binary),
            output_type: Some(DataType::Utf8),
            input_arg_type: Some(
                input
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap()
                    .column(0)
                    .data_type()
                    .clone(),
            ),
        }),
        ..Default::default()
    };
    MannWhitneyUTestAgg
        .build_spec_from_type(&function, Some(input.data_type()), false)
        .unwrap()
}
fn evaluate(columns: Vec<ArrayRef>, groups: &[usize], count: usize, partitions: usize) -> Value {
    let input = input(columns);
    let spec = spec(&input);
    let tracker = MemTracker::new_root("mann-whitney-contract");
    let result = (|| -> Result<Value, String> {
        let mut final_states = States::new(&spec, count, &tracker);
        let final_ptrs = final_states.pointers();
        if partitions == 1 {
            let pointers = groups
                .iter()
                .map(|group| final_ptrs[*group])
                .collect::<Vec<_>>();
            MannWhitneyUTestAgg.update_batch(&spec, 0, &pointers, &AggInputView::Any(&input))?;
        } else {
            for partition in 0..partitions {
                let rows = (0..groups.len())
                    .filter(|row| row % partitions == partition)
                    .collect::<Vec<_>>();
                let indices =
                    UInt64Array::from(rows.iter().map(|row| *row as u64).collect::<Vec<_>>());
                let gathered = take(input.as_ref(), &indices, None).unwrap();
                let mut partial = States::new(&spec, count, &tracker);
                let partial_ptrs = partial.pointers();
                let pointers = rows
                    .iter()
                    .map(|row| partial_ptrs[groups[*row]])
                    .collect::<Vec<_>>();
                MannWhitneyUTestAgg.update_batch(
                    &spec,
                    0,
                    &pointers,
                    &AggInputView::Any(&gathered),
                )?;
                let encoded = MannWhitneyUTestAgg.build_array(&spec, 0, &partial_ptrs, true)?;
                let binary = encoded.as_any().downcast_ref::<BinaryArray>().unwrap();
                MannWhitneyUTestAgg.merge_batch(
                    &spec,
                    0,
                    &final_ptrs,
                    &AggInputView::Binary(binary),
                )?;
            }
        }
        let encoded = MannWhitneyUTestAgg.build_array(&spec, 0, &final_ptrs, true)?;
        let binary = encoded.as_any().downcast_ref::<BinaryArray>().unwrap();
        let states = (0..binary.len())
            .map(|row| hex::encode(binary.value(row)))
            .collect::<Vec<_>>();
        let output = MannWhitneyUTestAgg.build_array(&spec, 0, &final_ptrs, false)?;
        let strings = output.as_any().downcast_ref::<StringArray>().unwrap();
        let values = (0..strings.len())
            .map(|row| (!strings.is_null(row)).then(|| strings.value(row).to_owned()))
            .collect::<Vec<_>>();
        Ok(
            json!({"type":format!("{:?}",output.data_type()),"intermediate_type":format!("{:?}",encoded.data_type()),"values":values,"states":states,"tracked_bytes":tracker.current()}),
        )
    })();
    assert_eq!(
        tracker.current(),
        0,
        "every original state must be destroyed"
    );
    match result {
        Ok(value) => json!({"ok":value,"after_drop":tracker.current()}),
        Err(message) => json!({"error":message,"after_drop":tracker.current()}),
    }
}
fn record(
    records: &mut Vec<Value>,
    name: &str,
    columns: Vec<ArrayRef>,
    groups: &[usize],
    count: usize,
) {
    let types = columns
        .iter()
        .map(|c| format!("{:?}", c.data_type()))
        .collect::<Vec<_>>();
    for partitions in [1, 3] {
        records.push(json!({"name":name,"types":types,"partitions":partitions,"outcome":evaluate(columns.clone(),groups,count,partitions)}));
    }
}
fn boolean(values: Vec<Option<bool>>) -> ArrayRef {
    Arc::new(BooleanArray::from(values))
}
fn text(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}
fn integers(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}
fn transcript() -> Value {
    let mut records = Vec::new();
    let base = integers(vec![Some(1), Some(3), Some(2), Some(4), None, Some(-7)]);
    let treatment = integers(vec![Some(0), Some(-1), Some(0), Some(3), None, Some(0)]);
    let value_types = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal128(20, 0),
        DataType::Decimal128(20, 2),
        DataType::Decimal128(38, -2),
    ];
    let treatment_types = [
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ];
    let mut numeric = value_types
        .iter()
        .map(|ty| cast(base.as_ref(), ty).unwrap())
        .collect::<Vec<_>>();
    numeric.push(
        novarocks_functions::largeint::array_from_i128(&[
            Some(1),
            Some(3),
            Some(2),
            Some(4),
            None,
            Some(-(1_i128 << 72)),
        ])
        .unwrap(),
    );
    for (vi, values) in numeric.iter().enumerate() {
        for (ti, ty) in treatment_types.iter().enumerate() {
            let treatments = cast(treatment.as_ref(), ty).unwrap();
            for arity in 2..=4 {
                let mut columns = vec![values.clone(), treatments.clone()];
                if arity >= 3 {
                    columns.push(text(vec![Some("TwO-SiDeD"); 6]));
                }
                if arity == 4 {
                    columns
                        .push(cast(integers(vec![Some(0); 6]).as_ref(), &DataType::Int16).unwrap());
                }
                record(
                    &mut records,
                    &format!("numeric-{vi}-treatment-{ti}-arity-{arity}"),
                    columns,
                    &[0, 0, 0, 0, 0, 0],
                    2,
                );
            }
        }
    }
    record(
        &mut records,
        "grouped",
        vec![
            base.clone(),
            cast(treatment.as_ref(), &DataType::Boolean).unwrap(),
        ],
        &[0, 1, 2, 0, 1, 2],
        4,
    );
    record(
        &mut records,
        "empty",
        vec![
            Arc::new(Float64Array::from(Vec::<f64>::new())),
            boolean(vec![]),
        ],
        &[],
        2,
    );
    record(
        &mut records,
        "identical",
        vec![
            integers(vec![Some(1); 3]),
            boolean(vec![Some(false), Some(true), Some(false)]),
        ],
        &[0; 3],
        1,
    );
    record(
        &mut records,
        "single-sided",
        vec![
            integers(vec![Some(1), Some(2)]),
            boolean(vec![Some(false); 2]),
        ],
        &[0; 2],
        1,
    );
    record(
        &mut records,
        "nonfinite",
        vec![
            Arc::new(Float64Array::from(vec![
                f64::NAN,
                f64::INFINITY,
                f64::NEG_INFINITY,
                1.0,
                2.0,
                3.0,
            ])),
            boolean(vec![
                Some(false),
                Some(true),
                Some(false),
                Some(false),
                Some(true),
                Some(true),
            ]),
        ],
        &[0; 6],
        2,
    );
    record(
        &mut records,
        "all-null-values",
        vec![text(vec![None; 2]), boolean(vec![Some(false), Some(true)])],
        &[0; 2],
        2,
    );
    record(
        &mut records,
        "null-physical",
        vec![
            Arc::new(NullArray::new(2)),
            boolean(vec![Some(false), Some(true)]),
        ],
        &[0; 2],
        1,
    );
    record(
        &mut records,
        "nonnumeric-null-treatment",
        vec![text(vec![Some("x"), Some("y")]), boolean(vec![None; 2])],
        &[0; 2],
        1,
    );
    record(
        &mut records,
        "nonnumeric-data-error",
        vec![
            text(vec![Some("x"), None]),
            boolean(vec![Some(false), Some(true)]),
        ],
        &[0; 2],
        1,
    );
    record(
        &mut records,
        "unsupported-reader-null-treatment",
        vec![
            Arc::new(UInt64Array::from(vec![Some(1), None])),
            boolean(vec![None; 2]),
        ],
        &[0; 2],
        1,
    );
    record(
        &mut records,
        "unsupported-reader-null-value",
        vec![
            Arc::new(UInt64Array::from(vec![None::<u64>; 2])),
            boolean(vec![Some(false), Some(true)]),
        ],
        &[0; 2],
        1,
    );
    record(
        &mut records,
        "invalid-alternative-before-null",
        vec![
            text(vec![Some("x")]),
            boolean(vec![None]),
            text(vec![Some(" BAD ")]),
        ],
        &[0],
        1,
    );
    record(
        &mut records,
        "negative-continuity-before-null",
        vec![
            text(vec![None]),
            boolean(vec![None]),
            text(vec![None]),
            integers(vec![Some(-1)]),
        ],
        &[0],
        1,
    );
    record(
        &mut records,
        "later-parameters-ignored",
        vec![
            base.clone(),
            cast(treatment.as_ref(), &DataType::Boolean).unwrap(),
            text(vec![
                Some("less"),
                Some("invalid"),
                Some("greater"),
                None,
                Some("less"),
                Some("less"),
            ]),
            integers(vec![Some(0), Some(-1), Some(2), None, Some(0), Some(0)]),
        ],
        &[0; 6],
        1,
    );
    record(
        &mut records,
        "merge-parameters-mismatch",
        vec![
            base.clone(),
            cast(treatment.as_ref(), &DataType::Boolean).unwrap(),
            text(vec![
                Some("less"),
                Some("greater"),
                Some("less"),
                Some("less"),
                Some("greater"),
                Some("less"),
            ]),
        ],
        &[0; 6],
        1,
    );
    for alternative in ["two-sided", "less", "greater"] {
        for ty in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
        ] {
            record(
                &mut records,
                &format!("alternative-{alternative}-continuity-{ty:?}"),
                vec![
                    base.clone(),
                    cast(treatment.as_ref(), &DataType::Boolean).unwrap(),
                    text(vec![Some(alternative); 6]),
                    cast(integers(vec![Some(2); 6]).as_ref(), &ty).unwrap(),
                ],
                &[0; 6],
                1,
            );
        }
    }
    let tracker = MemTracker::new_root("mann-whitney-codec");
    let mut codec = Vec::new();
    for (name, bytes) in [
        ("unknown", vec![0; 17]),
        ("known-empty", {
            let mut b = vec![0; 17];
            b[0] = 1;
            b[1] = 1;
            b
        }),
        ("trailing", {
            let mut b = vec![0; 20];
            b[0] = 2;
            b
        }),
        ("short", vec![0; 16]),
    ] {
        let result =
            MannWhitneyState::deserialize_in(&bytes, AggregateAllocator::new(tracker.clone()));
        let observed = match result {
            Ok(mut state) => {
                json!({"uninitialized":state.is_uninitialized(),"text":state.build_result(),"serialized":hex::encode(state.serialize())})
            }
            Err(error) => json!({"error":error.to_string()}),
        };
        assert_eq!(tracker.current(), 0);
        codec.push(json!({"name":name,"input":hex::encode(bytes),"observed":observed}));
    }
    let mut outcomes = Vec::new();
    for profile in &mut records {
        let outcome = profile["outcome"].take();
        let index = match outcomes.iter().position(|value| value == &outcome) {
            Some(index) => index,
            None => {
                outcomes.push(outcome);
                outcomes.len() - 1
            }
        };
        profile["outcome"] = json!(index);
    }
    json!({"profiles":records,"outcomes":outcomes,"codec":codec})
}
#[test]
fn mann_whitney_contract_frozen_before_extraction() {
    let actual = transcript();
    let original: Value =
        serde_json::from_str(include_str!("mann_whitney_original_contract.json")).unwrap();
    assert_eq!(actual, original);
}
