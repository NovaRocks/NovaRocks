// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::{ConstantError, ConstantPolicy, ConstantPool, ConstantValue};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Int8Array, Int64Array, LargeStringArray, StringArray,
    StringViewArray, types::Int8Type,
};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl, ValueLogicalType,
};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refuse: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        trace.push((phase, units));
        match self.refuse.clone() {
            Some((at, error)) if trace.len() == at + 1 => Err(error),
            _ => Ok(()),
        }
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 8,
        max_array_nodes: 16,
        max_logical_elements: 1 << 20,
        max_retained_buffer_bytes: 2_000_000,
        max_type_depth: 8,
        max_type_nodes: 16,
        max_dictionary_depth: 2,
        max_metadata_bytes: 500_000,
        max_library_validation_work: 5_000_000,
        max_library_validation_bytes: 5_000_000,
    }
}
fn pool(array: ArrayRef, logical: ValueLogicalType) -> ConstantPool {
    let ty =
        FunctionValueType::try_with_logical_type(array.data_type().clone(), true, logical).unwrap();
    let field = ty
        .try_to_field("actual.source")
        .unwrap()
        .with_data_type(array.data_type().clone())
        .with_metadata(
            ty.try_to_field("actual.source")
                .unwrap()
                .metadata()
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .chain([("provider.fact".to_owned(), "preserved".to_owned())])
                .collect::<std::collections::HashMap<String, String>>(),
        );
    ConstantPool::try_new(
        Arc::new(field),
        ty,
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap()
}
fn array(ty: &DataType, values: Vec<Option<&str>>) -> ArrayRef {
    match ty {
        DataType::Utf8 => Arc::new(StringArray::from(values)),
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(values)),
        DataType::Utf8View => Arc::new(StringViewArray::from(values)),
        _ => unreachable!(),
    }
}
fn read<'a>(value: &'a ConstantValue, control: &Control) -> Result<Option<&'a str>, ConstantError> {
    value.try_utf8_borrowed_observed(CompilePhase::FunctionSpecialization, control)
}

#[test]
fn exact_utf8_carriers_borrow_selected_nonzero_ordinal_and_slice_without_reauthoring() {
    for ty in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        let source = array(
            &ty,
            vec![
                Some("unselected"),
                None,
                Some("selected λ long external view"),
                Some("tail"),
            ],
        );
        let pool = pool(source.slice(1, 3), ValueLogicalType::Physical);
        let value = pool.value(1).unwrap();
        let expected = value.try_utf8().unwrap().unwrap();
        let observed = read(&value, &Control::default()).unwrap().unwrap();
        assert_eq!(observed, "selected λ long external view");
        assert_eq!(observed.as_ptr(), expected.as_ptr());
        assert_eq!(value.ordinal(), 1);
        assert!(Arc::ptr_eq(value.pool().field_ref(), pool.field_ref()));
        assert_eq!(value.value_type(), pool.value_type());
        assert_eq!(value.field().metadata()["provider.fact"], "preserved");
        assert_eq!(
            read(&pool.value(0).unwrap(), &Control::default()).unwrap(),
            None
        );
    }
}

#[test]
fn borrowed_utf8_distinguishes_null_from_wrong_nominal_carrier_and_dictionary() {
    let json = pool(
        Arc::new(StringArray::from(vec![Some("{}"), None])),
        ValueLogicalType::Json,
    );
    let integers = pool(
        Arc::new(Int64Array::from(vec![Some(7), None])),
        ValueLogicalType::Physical,
    );
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(0), None]),
        Arc::new(StringArray::from(vec!["text"])),
    )
    .unwrap();
    let dictionary = pool(Arc::new(dictionary), ValueLogicalType::Physical);
    for pool in [&json, &integers, &dictionary] {
        for ordinal in [0, 1] {
            assert!(matches!(
                read(&pool.value(ordinal).unwrap(), &Control::default()),
                Err(ConstantError::Invalid(_)),
            ));
        }
    }
    let strings = pool(
        Arc::new(StringArray::from(vec![None::<&str>, Some("")])),
        ValueLogicalType::Physical,
    );
    assert_eq!(
        read(&strings.value(0).unwrap(), &Control::default()).unwrap(),
        None
    );
    assert_eq!(
        read(&strings.value(1).unwrap(), &Control::default()).unwrap(),
        Some("")
    );
}

#[test]
fn borrowed_text_boundaries_do_not_rescan_payload_or_create_length_dependent_work() {
    for len in [0, 12, 13, 1023, 1024, 1025, 65_536] {
        let text = "x".repeat(len);
        for ty in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            let pool = pool(
                array(&ty, vec![Some("unused"), Some(&text)]),
                ValueLogicalType::Physical,
            );
            let value = pool.value(1).unwrap();
            let control = Control::default();
            assert_eq!(read(&value, &control).unwrap(), Some(text.as_str()));
            assert_eq!(
                *control.trace.lock().unwrap(),
                vec![
                    (CompilePhase::FunctionSpecialization, 0),
                    (CompilePhase::FunctionSpecialization, 3)
                ],
            );
        }
    }
}

#[test]
fn borrowed_utf8_all_original_refusals_preserve_exact_prefix_on_success_and_ordinary_tail() {
    let strings = pool(
        Arc::new(StringArray::from(vec![Some("chosen"), None])),
        ValueLogicalType::Physical,
    );
    let integers = pool(
        Arc::new(Int64Array::from(vec![Some(7)])),
        ValueLogicalType::Physical,
    );
    for value in [
        strings.value(0).unwrap(),
        strings.value(1).unwrap(),
        integers.value(0).unwrap(),
    ] {
        let control = Control::default();
        let _ = read(&value, &control);
        let trace = control.trace.into_inner().unwrap();
        assert_eq!(trace.len(), 2);
        assert!(trace[1].1 > 0);
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    refuse: Some((at, cause.clone())),
                    ..Control::default()
                };
                assert!(
                    matches!(read(&value, &control), Err(ConstantError::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}
