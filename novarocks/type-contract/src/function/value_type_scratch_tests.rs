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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use crate::{
    CompileCheckpoints, CompileControlError, CompilePhase, MAX_VALUE_TYPE_NODES,
    NR_LOGICAL_TYPE_KEY, PureCompileControl, ValueLogicalType, ValueTypeError, ValueTypeVisit,
    owned_resources::type_validation::TypeValidationScratch,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Debug, PartialEq)]
enum Error {
    Value(ValueTypeError),
    Control(CompileControlError),
}
impl From<ValueTypeError> for Error {
    fn from(value: ValueTypeError) -> Self {
        Self::Value(value)
    }
}
impl From<CompileControlError> for Error {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}
fn event(visit: ValueTypeVisit<'_>) -> (u8, usize) {
    match visit {
        ValueTypeVisit::TypeNode(ty) => (0, ty as *const DataType as usize),
        ValueTypeVisit::ChildEdge(ty) => (1, ty as *const DataType as usize),
        ValueTypeVisit::Field(field) => (2, field as *const Field as usize),
    }
}

#[test]
fn invalid_declared_root_is_rejected_before_a_binding_exists() {
    let result = FunctionValueType::try_from_legacy_declared_projection(
        DataType::List(Arc::new(Field::new("nested", DataType::Int32, true))),
        true,
        ValueLogicalType::Json,
    );
    assert_eq!(
        result,
        Err(ValueTypeError::InvalidLogicalCarrier(
            ValueLogicalType::Json
        ))
    );
}

#[test]
fn shared_nested_fields_keep_original_law_and_exact_borrowed_occurrence_events() {
    let shared = Arc::new(
        Field::new("json", DataType::LargeBinary, true).with_metadata(HashMap::from([
            (NR_LOGICAL_TYPE_KEY.to_owned(), "json".to_owned()),
            ("provider".to_owned(), "same-original-field".to_owned()),
        ])),
    );
    let valid = FunctionValueType::from_legacy_declared_projection(
        DataType::Struct(vec![shared.clone(), shared.clone()].into()),
        true,
    );
    let bad = FunctionValueType::new(
        DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::List(Arc::new(
                Field::new("bad", DataType::Int32, true).with_metadata(HashMap::from([(
                    NR_LOGICAL_TYPE_KEY.to_owned(),
                    "json".to_owned(),
                )])),
            ))),
        ),
        false,
    );
    for ty in [&valid, &bad] {
        let mut original = Vec::new();
        let expected =
            crate::validate_value_type_structure_observed::<Error>(ty.data_type(), |v| {
                original.push(event(v));
                Ok(())
            });
        let mut scratch: TypeValidationScratch<'_> = [None; MAX_VALUE_TYPE_NODES];
        let mut actual = Vec::new();
        let result = ty.validate_with_scratch_observed::<Error>(&mut scratch, |v| {
            actual.push(event(v));
            Ok(())
        });
        assert_eq!(result, expected);
        assert_eq!(result, ty.validate().map_err(Error::Value));
        assert_eq!(actual, original);
        assert_eq!(
            actual.iter().filter(|entry| entry.0 == 2).count(),
            if std::ptr::eq(ty, &valid) { 2 } else { 1 }
        );
    }
}

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after original refusal");
        }
        trace.push((phase, units));
        match self.stop.clone() {
            Some((stop, error)) if at == stop => Err(error),
            _ => Ok(()),
        }
    }
}
fn run(ty: &FunctionValueType, control: &Control) -> Result<(), Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    // This caller owns the actual scratch initialization; no allocation or
    // resource admission certificate is inferred from this test harness.
    let mut scratch: TypeValidationScratch<'_> = [None; MAX_VALUE_TYPE_NODES];
    let result =
        ty.validate_with_scratch_observed(&mut scratch, |_| work.step().map_err(Error::from));
    if matches!(result, Err(Error::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

#[test]
fn actual_nested_success_and_ordinary_tail_preserve_every_three_cause_control_prefix() {
    let valid = FunctionValueType::from_legacy_declared_projection(
        DataType::Struct(
            (0..320)
                .map(|i| Arc::new(Field::new(format!("field_{i}"), DataType::Utf8, true)))
                .collect::<Vec<_>>()
                .into(),
        ),
        false,
    );
    let bad_source = Field::new(
        "unknown-root",
        DataType::List(Arc::new(
            Field::new("unknown", DataType::Utf8, true).with_metadata(HashMap::from([(
                NR_LOGICAL_TYPE_KEY.to_owned(),
                "foreign".to_owned(),
            )])),
        )),
        true,
    );
    assert_eq!(
        FunctionValueType::try_from_legacy_field_projection(&bad_source),
        Err(ValueTypeError::UnknownLogicalMetadata)
    );
    for ty in [&valid] {
        let control = Control::default();
        assert_eq!(run(ty, &control), ty.validate().map_err(Error::Value));
        let trace = control.trace.lock().unwrap().clone();
        if std::ptr::eq(ty, &valid) {
            assert!(trace.iter().any(|(_, units)| *units == 256));
        }
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    stop: Some((at, cause.clone())),
                    ..Control::default()
                };
                assert_eq!(run(ty, &control), Err(Error::Control(cause)));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}
