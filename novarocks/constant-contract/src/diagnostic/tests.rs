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

use super::*;
use crate::{ConstantPolicy, ConstantPool};
use arrow_array::types::{Int8Type, Int32Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, Date32Array, Decimal128Array, DictionaryArray, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int32Array, Int64Array, ListArray, RunArray,
    StringArray, StructArray,
};
use arrow_schema::Field;
use novarocks_type_contract::{CompileControlError, FunctionValueType};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

const PHASE: CompilePhase = CompilePhase::FunctionSpecialization;

struct Control {
    refusal: Option<(usize, CompileControlError)>,
    calls: Mutex<Vec<(CompilePhase, u32)>>,
}
impl Control {
    fn good() -> Self {
        Self {
            refusal: None,
            calls: Mutex::new(Vec::new()),
        }
    }
    fn refusing(at: usize, cause: CompileControlError) -> Self {
        Self {
            refusal: Some((at, cause)),
            calls: Mutex::new(Vec::new()),
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.calls.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut calls = self.calls.lock().unwrap();
        let at = calls.len();
        calls.push((phase, units));
        match self.refusal.clone() {
            Some((index, cause)) if index == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn causes() -> [CompileControlError; 3] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ]
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1_000_000,
        max_array_nodes: 4096,
        max_logical_elements: 100_000_000,
        max_retained_buffer_bytes: 64 * 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1024 * 1024,
        max_library_validation_work: 100_000_000,
        max_library_validation_bytes: 100_000_000,
    }
}
fn typed_pool(
    array: ArrayRef,
    value_type: FunctionValueType,
    metadata: HashMap<String, String>,
) -> ConstantPool {
    let mut field = value_type
        .try_to_field("source")
        .unwrap()
        .with_data_type(array.data_type().clone());
    field.metadata_mut().extend(metadata);
    ConstantPool::try_new(
        Arc::new(field),
        value_type,
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap()
}
fn pool(array: ArrayRef, nullable: bool) -> ConstantPool {
    let ty = FunctionValueType::new(array.data_type().clone(), nullable);
    typed_pool(array, ty, HashMap::new())
}
fn diagnostic(value: &ConstantValue) -> String {
    format(value, PHASE, &Control::good()).unwrap()
}
fn assert_all_callbacks(value: &ConstantValue, ordinary_error: bool) -> Vec<(CompilePhase, u32)> {
    let baseline = Control::good();
    let result = format(value, PHASE, &baseline);
    if ordinary_error {
        assert!(matches!(result, Err(ConstantError::Source(_))));
    } else {
        result.unwrap();
    }
    let trace = baseline.trace();
    assert!(!trace.is_empty());
    assert!(
        trace
            .iter()
            .all(|(phase, units)| *phase == PHASE && *units <= 256)
    );
    for at in 0..trace.len() {
        for cause in causes() {
            let control = Control::refusing(at, cause.clone());
            assert!(
                matches!(format(value, PHASE, &control), Err(ConstantError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
    trace
}

#[test]
fn diagnostic_selects_nonzero_ordinal_and_exact_largeint_without_unused_pool_output() {
    let unused = "unselected".repeat(40_000);
    let p = pool(
        Arc::new(StringArray::from(vec![unused.as_str(), "chosen雪", "last"])),
        false,
    );
    assert_eq!(diagnostic(&p.value(1).unwrap()), "chosen雪");
    assert_eq!(p.value(1).unwrap().ordinal(), 1);
    let values = [i128::MAX.to_be_bytes(), i128::MIN.to_be_bytes()];
    let array =
        FixedSizeBinaryArray::try_from_iter(values.iter().map(|value| value.as_slice())).unwrap();
    let ty = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let p = typed_pool(Arc::new(array), ty, HashMap::new());
    assert_eq!(
        diagnostic(&p.value(1).unwrap()),
        "-170141183460469231731687303715884105728"
    );
    assert_eq!(
        p.field()
            .metadata()
            .get(novarocks_type_contract::NR_LOGICAL_TYPE_KEY)
            .unwrap(),
        "largeint"
    );
}

#[test]
fn diagnostic_keeps_typed_null_json_and_provider_metadata() {
    let metadata = HashMap::from([("provider.id".to_owned(), "42".to_owned())]);
    let ty = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        ValueLogicalType::Json,
    )
    .unwrap();
    let p = typed_pool(
        Arc::new(arrow_array::LargeBinaryArray::from(vec![
            Some(b"\x01\x20".as_slice()),
            None,
        ])),
        ty.clone(),
        metadata,
    );
    assert_eq!(diagnostic(&p.value(0).unwrap()), "0120");
    assert_eq!(diagnostic(&p.value(1).unwrap()), "NULL");
    assert_eq!(p.value_type(), &ty);
    assert_eq!(p.field().metadata().get("provider.id").unwrap(), "42");
    let p = pool(Arc::new(Int64Array::from(vec![None, Some(7)])), true);
    assert_eq!(diagnostic(&p.value(0).unwrap()), "NULL");
    assert_all_callbacks(&p.value(0).unwrap(), false);
}

#[test]
fn diagnostic_preserves_float_nan_payloads_and_both_zero_signs() {
    let p = pool(
        Arc::new(Float32Array::from(vec![
            f32::from_bits(0x7f80_0001),
            -0.0,
            0.0,
        ])),
        false,
    );
    assert_eq!(diagnostic(&p.value(0).unwrap()), "NaN(Float32,0x7f800001)");
    assert_eq!(diagnostic(&p.value(1).unwrap()), "-0.0");
    assert_eq!(diagnostic(&p.value(2).unwrap()), "0.0");
    let p = pool(
        Arc::new(Float64Array::from(vec![
            f64::from_bits(0x7ff0_0000_0000_0001),
            -0.0,
            0.0,
        ])),
        false,
    );
    assert_eq!(
        diagnostic(&p.value(0).unwrap()),
        "NaN(Float64,0x7ff0000000000001)"
    );
    assert_eq!(diagnostic(&p.value(1).unwrap()), "-0.0");
    assert_eq!(diagnostic(&p.value(2).unwrap()), "0.0");
    assert_all_callbacks(&p.value(0).unwrap(), false);
}

#[test]
fn diagnostic_dictionary_renumbering_run_encoding_and_selected_nested_values() {
    let unused = "unused".repeat(60_000);
    let a = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![0, 1]),
        Arc::new(StringArray::from(vec![unused.as_str(), "selected"])),
    )
    .unwrap();
    let b = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![1, 0]),
        Arc::new(StringArray::from(vec!["selected", unused.as_str()])),
    )
    .unwrap();
    assert_eq!(
        diagnostic(&pool(Arc::new(a), false).value(1).unwrap()),
        "selected"
    );
    assert_eq!(
        diagnostic(&pool(Arc::new(b), false).value(1).unwrap()),
        "selected"
    );
    let run = RunArray::<Int32Type>::try_new(
        &Int32Array::from(vec![1, 4]),
        &StringArray::from(vec!["other", "selected"]),
    )
    .unwrap();
    assert_eq!(
        diagnostic(&pool(Arc::new(run), false).value(3).unwrap()),
        "selected"
    );
    let lists = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(99)]),
        Some(vec![Some(1), None, Some(3)]),
    ]);
    let value = pool(Arc::new(lists), false).value(1).unwrap();
    assert_eq!(diagnostic(&value), "[1, NULL, 3]");
    assert_all_callbacks(&value, false);
}

#[test]
fn diagnostic_observes_wide_actual_fields_and_ordinary_date_failure_tail() {
    let fields = (0..320)
        .map(|n| Arc::new(Field::new(format!("field{n}"), DataType::Int64, false)))
        .collect::<Vec<_>>();
    let arrays = (0..320)
        .map(|n| Arc::new(Int64Array::from(vec![n])) as ArrayRef)
        .collect::<Vec<_>>();
    let array = StructArray::new(fields.into(), arrays, None);
    let metadata = (0..320)
        .map(|n| (format!("provider.{n}"), format!("value{n}")))
        .collect();
    let ty = FunctionValueType::new(array.data_type().clone(), false);
    let p = typed_pool(Arc::new(array), ty, metadata);
    let value = p.value(0).unwrap();
    let control = Control::good();
    let text = format(&value, PHASE, &control).unwrap();
    assert!(text.starts_with("{field0: 0, field1: 1"));
    assert!(text.ends_with("field319: 319}"));
    assert_eq!(p.field().metadata().len(), 320);
    let trace = control.trace();
    assert!(trace.iter().any(|(_, units)| *units == 256));
    for at in [
        0,
        trace.iter().position(|(_, units)| *units == 256).unwrap(),
        trace.len() - 1,
    ] {
        for cause in causes() {
            let refusal = Control::refusing(at, cause.clone());
            assert!(
                matches!(format(&value, PHASE, &refusal), Err(ConstantError::Control(actual)) if actual == cause)
            );
            assert_eq!(refusal.trace(), trace[..=at]);
        }
    }
    let p = pool(Arc::new(Date32Array::from(vec![0, i32::MAX])), false);
    let value = p.value(1).unwrap();
    let error = format(&value, PHASE, &Control::good()).unwrap_err();
    assert!(
        matches!(error, ConstantError::Source(message) if message.detail().contains("Failed to convert"))
    );
    let trace = assert_all_callbacks(&value, true);
    assert!(trace.len() > 1);
}

#[test]
fn diagnostic_long_selected_unicode_text_observes_each_pass_and_never_publishes_partial() {
    let selected = "雪☃éa".repeat(40_000);
    let p = pool(
        Arc::new(StringArray::from(vec!["unused", selected.as_str()])),
        false,
    );
    let value = p.value(1).unwrap();
    let control = Control::good();
    assert_eq!(format(&value, PHASE, &control).unwrap(), selected);
    let trace = control.trace();
    let quanta = trace
        .iter()
        .enumerate()
        .filter(|(_, (_, n))| *n == 256)
        .map(|(i, _)| i)
        .collect::<Vec<_>>();
    assert!(quanta.len() >= selected.len() / 256);
    for at in [
        0,
        quanta[0],
        quanta[quanta.len() / 2],
        *quanta.last().unwrap(),
        trace.len() - 1,
    ] {
        for cause in causes() {
            let refusal = Control::refusing(at, cause.clone());
            assert!(
                matches!(format(&value, PHASE, &refusal), Err(ConstantError::Control(actual)) if actual == cause)
            );
            assert_eq!(refusal.trace(), trace[..=at]);
        }
    }
}

#[test]
fn diagnostic_exact_count_covers_decimal_scale_extremes_without_a_default_bound() {
    let array = Decimal128Array::from(vec![-9])
        .with_precision_and_scale(38, i8::MIN)
        .unwrap();
    let value = pool(Arc::new(array), false).value(0).unwrap();
    assert_eq!(diagnostic(&value), format!("-9{}", "0".repeat(128)));
    assert_all_callbacks(&value, false);
}

#[test]
fn diagnostic_writer_checks_growth_overflow_unicode_and_first_primary_failure() {
    let control = Control::good();
    let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
    let mut writer = ObservedWriter::output(&mut work, 5);
    writer.write_str("é雪").unwrap();
    assert_eq!(writer.output.as_deref(), Some("é雪"));
    assert!(writer.write_str("a").is_err());
    assert!(matches!(writer.error, Some(ConstantError::Limit(_))));
    let length = writer.length;
    assert!(writer.write_str("later").is_err());
    assert_eq!(writer.length, length);
    drop(writer);
    let before = control.trace();
    let mut writer = ObservedWriter::counting(&mut work);
    writer.length = usize::MAX;
    assert!(writer.write_str("x").is_err());
    assert!(matches!(
        writer.error,
        Some(ConstantError::Limit("constant diagnostic length overflow"))
    ));
    drop(writer);
    assert_eq!(control.trace(), before);

    for cause in causes() {
        let refusal = Control::refusing(1, cause.clone());
        let mut work = CompileCheckpoints::try_new(&refusal, PHASE).unwrap();
        let mut writer = ObservedWriter::output(&mut work, 5);
        // The mandatory pre-growth callback fails before copying any bytes.
        assert!(writer.write_str("é").is_err());
        assert_eq!(writer.output.as_deref(), Some(""));
        assert!(matches!(&writer.error, Some(ConstantError::Control(actual)) if actual == &cause));
        assert!(writer.write_str("exceeds bound").is_err());
        assert!(matches!(&writer.error, Some(ConstantError::Control(actual)) if actual == &cause));
        assert_eq!(refusal.trace().len(), 2);
    }
    let refusal = Control::refusing(1, CompileControlError::Cancelled);
    let mut work = CompileCheckpoints::try_new(&refusal, PHASE).unwrap();
    let mut writer = ObservedWriter::output(&mut work, 0);
    assert!(writer.write_str("x").is_err());
    assert!(matches!(writer.error, Some(ConstantError::Limit(_))));
    assert!(writer.write_str("x").is_err());
    assert_eq!(refusal.trace().len(), 1);
}

#[test]
fn actual_diagnostic_backing_refuses_unrepresentable_string_before_allocation() {
    use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
    let error = super::reserve_output(usize::MAX).unwrap_err();
    assert_eq!(error.failure_fact().class(), FailureClass::Representation);
    let facts = error.failure_fact().representation().unwrap();
    assert_eq!(facts.boundary, RepresentationBoundary::SingleOutput);
    assert_eq!(facts.required, Some(usize::MAX as u64));
    assert_eq!(facts.extent_limit, Some(isize::MAX as u64));
    let mut source: &(dyn std::error::Error + 'static) = &error;
    while !source.is::<std::alloc::LayoutError>() {
        source = source.source().expect("actual Rust string layout refusal");
    }
    assert!(super::reserve_output(0).unwrap().is_empty());
    assert!(super::reserve_output(64).unwrap().capacity() >= 64);
}
