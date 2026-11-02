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

//! Canonical typed Map keys are non-null; raw Arrow flexibility is separate.

use super::*;
use arrow_array::{Int32Array, MapArray, StructArray};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::{collections::HashMap, sync::Mutex};

const PHASE: CompilePhase = CompilePhase::FunctionSpecialization;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];

struct TraceControl {
    calls: Mutex<Vec<(CompilePhase, u32)>>,
    refuse: Option<(usize, CompileControlError)>,
}
impl TraceControl {
    fn new(refuse: Option<(usize, CompileControlError)>) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            refuse,
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.calls.lock().unwrap().clone()
    }
}
impl PureCompileControl for TraceControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut calls = self.calls.lock().unwrap();
        let at = calls.len();
        if let Some((refused_at, _)) = self.refuse {
            assert!(
                at <= refused_at,
                "control was retried after its first refusal"
            );
        }
        calls.push((phase, units));
        match self.refuse.clone() {
            Some((refused_at, cause)) if at == refused_at => Err(cause),
            _ => Ok(()),
        }
    }
}

fn policy() -> ConstantPolicy {
    // Finite test admission of already allocated arrays; this is not a MEM grant.
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 64,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1024 * 1024,
        max_library_validation_work: 1024 * 1024,
        max_library_validation_bytes: 1024 * 1024,
    }
}

fn map_fixture(rows: usize) -> (Arc<Field>, FunctionValueType, MapArray) {
    assert!(rows >= 2);
    let key = Arc::new(
        Field::new("key", DataType::Int32, false).with_metadata(HashMap::from([(
            "source-key".into(),
            "canonical-key-source".into(),
        )])),
    );
    let value = Arc::new(Field::new("value", DataType::Int32, true));
    let children = vec![key, value].into();
    let keys: ArrayRef = Arc::new(Int32Array::from(
        (0..rows.min(3))
            .map(|row| Some(row as i32))
            .collect::<Vec<_>>(),
    ));
    let values: ArrayRef = Arc::new(Int32Array::from(
        (0..rows.min(3))
            .map(|row| {
                if row == 1 {
                    None
                } else {
                    Some(100 + row as i32)
                }
            })
            .collect::<Vec<_>>(),
    ));
    let entries = StructArray::new(children, vec![keys, values], None);
    let entries_field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    let offsets = OffsetBuffer::new(ScalarBuffer::from(
        (0..=rows).map(|row| row.min(3) as i32).collect::<Vec<_>>(),
    ));
    let array = MapArray::try_new(entries_field, offsets, entries, None, false).unwrap();
    let field = Arc::new(
        Field::new("source_map", array.data_type().clone(), false).with_metadata(HashMap::from([
            ("source-root".into(), "original-field".into()),
        ])),
    );
    let ty = FunctionValueType::try_from_field(&field).unwrap();
    (field, ty, array)
}

fn pool(
    field: &Arc<Field>,
    ty: &FunctionValueType,
    array: &MapArray,
    control: &dyn PureCompileControl,
) -> Result<ConstantPool, ConstantError> {
    ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        array.to_data(),
        policy(),
        PHASE,
        control,
    )
}

fn kernel_cause(cause: CompileControlError) -> KernelFailure {
    KernelFailure::CompileControl(cause.clone())
}

fn validate_type(
    ty: &FunctionValueType,
    control: &dyn PureCompileControl,
) -> Result<(), KernelFailure> {
    // This public type port borrows a caller-owned scope. Its caller supplies
    // the entry and completion, rather than inventing a tail in the port itself.
    let mut work = CompileCheckpoints::try_new(control, PHASE).map_err(kernel_cause)?;
    let result = validate_function_value_type_observed(ty, &mut work);
    if matches!(
        result,
        Err(KernelFailure::Cancelled
            | KernelFailure::DeadlineExceeded
            | KernelFailure::ResourceExhausted)
    ) {
        return result;
    }
    work.finish().map_err(kernel_cause)?;
    result
}

fn assert_exact_type(actual: &FunctionValueType, expected: &FunctionValueType) {
    assert_eq!(actual.logical_type(), expected.logical_type());
    assert_eq!(actual.nullable(), expected.nullable());
    let control = TraceControl::new(None);
    let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
    assert!(
        novarocks_type_contract::arrow_data_types_exact_borrowed_observed::<KernelFailure>(
            actual.data_type(),
            expected.data_type(),
            || work.step().map_err(kernel_cause),
        )
        .unwrap()
    );
    work.finish().unwrap();
}

fn invalid_map_types() -> Vec<DataType> {
    let children = vec![
        Arc::new(Field::new("key", DataType::Int32, false)),
        Arc::new(Field::new("value", DataType::Int32, true)),
    ];
    let entries = |carrier, nullable| {
        DataType::Map(Arc::new(Field::new("entries", carrier, nullable)), false)
    };
    let mut three = children.clone();
    three.push(Arc::new(Field::new("extra", DataType::Int32, true)));
    vec![
        entries(DataType::Struct(children.clone().into()), true),
        entries(DataType::Int32, false),
        entries(DataType::Struct(children[..1].to_vec().into()), false),
        entries(DataType::Struct(three.into()), false),
    ]
}

#[test]
fn carrier_map_canonical_key_and_nullable_value_admit_checked_selected_constant() {
    let (field, ty, array) = map_fixture(3);
    array.to_data().validate_full().unwrap();
    validate_type(&ty, &TraceControl::new(None)).unwrap();
    let pool = pool(&field, &ty, &array, &TraceControl::new(None)).unwrap();
    assert!(Arc::ptr_eq(pool.field_ref(), &field));
    assert_exact_type(pool.value_type(), &ty);
    let selected = pool.value(1).unwrap();
    assert_eq!(selected.ordinal(), 1);
    assert_exact_type(selected.value_type(), &ty);
    let stored = selected
        .pool()
        .array()
        .as_any()
        .downcast_ref::<MapArray>()
        .unwrap();
    assert!(!stored.is_null(1));
    assert_eq!(stored.value_length(1), 1);
    let entry = stored.value_offsets()[1] as usize;
    assert!(!stored.keys().is_null(entry));
    assert!(stored.values().is_null(entry));
    assert_eq!(
        stored
            .keys()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(2),
        2
    );
    let DataType::Map(entries, ordered) = selected.value_type().data_type() else {
        panic!("expected Map");
    };
    assert!(!ordered);
    assert!(!entries.is_nullable());
    let DataType::Struct(children) = entries.data_type() else {
        panic!("expected entries Struct");
    };
    assert!(!children[0].is_nullable());
    assert!(children[0].metadata().is_empty());
    assert_eq!(
        selected.field().metadata().get("source-root").unwrap(),
        "original-field"
    );
}

#[test]
fn carrier_map_nullable_key_does_not_relax_entries_or_two_child_shape() {
    for ty in invalid_map_types() {
        assert!(
            FunctionValueType::try_with_logical_type(
                ty,
                false,
                novarocks_type_contract::ValueLogicalType::Physical
            )
            .is_err()
        );
    }
}

#[test]
fn carrier_map_original_control_prefixes_cover_pool_and_full_type_ordinary_tails() {
    let (field, ty, array) = map_fixture(3);
    let valid = TraceControl::new(None);
    pool(&field, &ty, &array, &valid).unwrap();
    let expected = valid.trace();
    for at in 0..expected.len() {
        for cause in CAUSES {
            let control = TraceControl::new(Some((at, cause.clone())));
            assert!(
                matches!(pool(&field, &ty, &array, &control), Err(ConstantError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), expected[..=at]);
        }
    }
    // Invalid type graphs are refused at construction. The observed public
    // type port receives only complete valid immutable bindings.
    for (valid, target) in [(true, &ty)] {
        let recording = TraceControl::new(None);
        let result = validate_type(target, &recording);
        assert_eq!(result.is_ok(), valid);
        let expected = recording.trace();
        for at in 0..expected.len() {
            for cause in CAUSES {
                let control = TraceControl::new(Some((at, cause.clone())));
                assert_eq!(validate_type(target, &control), Err(kernel_cause(cause)));
                assert_eq!(control.trace(), expected[..=at]);
            }
        }
    }
    let invalid_field = Arc::new(field.as_ref().clone().with_data_type(DataType::Int64));
    let recording = TraceControl::new(None);
    assert!(matches!(
        pool(&invalid_field, &ty, &array, &recording),
        Err(ConstantError::Invalid(_))
    ));
    let expected = recording.trace();
    assert!(expected.last().unwrap().1 > 0);
    for at in 0..expected.len() {
        for cause in CAUSES {
            let control = TraceControl::new(Some((at, cause.clone())));
            assert!(
                matches!(pool(&invalid_field, &ty, &array, &control), Err(ConstantError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), expected[..=at]);
        }
    }
}

#[test]
fn carrier_map_actual_wide_rows_observe_quantum_and_keep_nullable_value() {
    let (field, ty, array) = map_fixture(320);
    let recording = TraceControl::new(None);
    let backing = pool(&field, &ty, &array, &recording).unwrap();
    let expected = recording.trace();
    assert!(expected.iter().any(|(_, units)| *units == 256));
    assert_eq!(backing.resource_facts().rows, 320);
    assert!(
        backing
            .array()
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap()
            .values()
            .is_null(1)
    );
    for (at, (_, units)) in expected.iter().enumerate() {
        if *units != 256 {
            continue;
        }
        for cause in CAUSES {
            let control = TraceControl::new(Some((at, cause.clone())));
            assert!(
                matches!(pool(&field, &ty, &array, &control), Err(ConstantError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), expected[..=at]);
        }
    }
}

#[test]
fn nullable_map_key_is_rejected_by_sdk_and_checked_typed_author() {
    let fields = vec![
        Arc::new(Field::new("key", DataType::Int32, true)),
        Arc::new(Field::new("value", DataType::Int32, true)),
    ]
    .into();
    let entries = StructArray::new(
        fields,
        vec![
            Arc::new(Int32Array::from(vec![None])),
            Arc::new(Int32Array::from(vec![1])),
        ],
        None,
    );
    let data_type = DataType::Map(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        false,
    );
    let error = MapArray::try_new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(vec![0, 1].into()),
        entries,
        None,
        false,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        arrow_schema::ArrowError::InvalidArgumentError(_)
    ));
    assert!(error.to_string().contains("keys field cannot be nullable"));
    assert!(
        FunctionValueType::try_with_logical_type(
            data_type,
            false,
            novarocks_type_contract::ValueLogicalType::Physical
        )
        .is_err()
    );
}
