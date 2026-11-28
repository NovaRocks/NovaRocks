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
use crate::{CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl};
use arrow_schema::{UnionFields, UnionMode};
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
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after primary refusal");
        }
        trace.push((phase, units));
        match self.stop.clone() {
            Some((stop, error)) if at == stop => Err(error),
            _ => Ok(()),
        }
    }
}
fn event(visit: ValueTypeVisit<'_>) -> (u8, usize) {
    match visit {
        ValueTypeVisit::TypeNode(ty) => (0, ty as *const DataType as usize),
        ValueTypeVisit::ChildEdge(ty) => (1, ty as *const DataType as usize),
        ValueTypeVisit::Field(field) => (2, field as *const Field as usize),
    }
}
fn compare(root: &DataType) {
    let mut vector = Vec::new();
    let mut borrowed = Vec::new();
    let first = validate_value_type_structure_observed::<Error>(root, |visit| {
        vector.push(event(visit));
        Ok(())
    });
    let mut scratch = [None; crate::MAX_ARROW_TYPE_PATH_DEPTH];
    let second =
        validate_value_type_structure_with_scratch_observed::<Error>(root, &mut scratch, |visit| {
            borrowed.push(event(visit));
            Ok(())
        });
    assert_eq!(first, second);
    assert_eq!(vector, borrowed);
}
#[test]
fn borrowed_scratch_uses_the_same_carrier_grammar_and_event_order() {
    let child = Arc::new(Field::new("child", DataType::Utf8, true));
    let union = UnionFields::try_new(
        vec![3, 9],
        vec![
            Arc::clone(&child),
            Arc::new(Field::new("n", DataType::Int64, true)),
        ],
    )
    .unwrap();
    for root in [
        DataType::Int32,
        DataType::List(Arc::clone(&child)),
        DataType::LargeList(Arc::clone(&child)),
        DataType::ListView(Arc::clone(&child)),
        DataType::LargeListView(Arc::clone(&child)),
        DataType::FixedSizeList(Arc::clone(&child), 3),
        DataType::Map(Arc::clone(&child), true),
        DataType::Struct(vec![Arc::clone(&child), Arc::clone(&child)].into()),
        DataType::Union(union, UnionMode::Dense),
        DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::List(Arc::clone(&child))),
        ),
        DataType::RunEndEncoded(
            Arc::new(Field::new("ends", DataType::Int16, false)),
            Arc::clone(&child),
        ),
    ] {
        compare(&root);
    }
}
#[test]
fn borrowed_scratch_preserves_original_limit_and_logical_failure_events() {
    let child = Arc::new(Field::new("child", DataType::Int32, true));
    compare(&DataType::Struct(vec![child; 4096].into()));
    let mut root = DataType::Int32;
    for _ in 0..MAX_VALUE_TYPE_DEPTH + 1 {
        root = DataType::List(Arc::new(Field::new("child", root, true)));
    }
    compare(&root);
    compare(&DataType::List(Arc::new(
        Field::new("foreign", DataType::Utf8, true).with_metadata(HashMap::from([(
            NR_LOGICAL_TYPE_KEY.into(),
            "unknown".into(),
        )])),
    )));
}
#[test]
fn borrowed_scratch_every_control_prefix_matches_the_original_walk() {
    let root = DataType::Struct(
        (0..320)
            .map(|i| Arc::new(Field::new(format!("field_{i}"), DataType::Int32, true)))
            .collect::<Vec<_>>()
            .into(),
    );
    let run = |control: &Control, scratch_backend: bool| -> Result<(), Error> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
        let mut scratch = [None; crate::MAX_ARROW_TYPE_PATH_DEPTH];
        let observe = |_: ValueTypeVisit<'_>| work.step().map_err(Error::from);
        let result = if scratch_backend {
            validate_value_type_structure_with_scratch_observed(&root, &mut scratch, observe)
        } else {
            validate_value_type_structure_observed(&root, observe)
        };
        if matches!(&result, Err(Error::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    };
    let control = Control::default();
    run(&control, false).unwrap();
    let trace = control.trace.lock().unwrap().clone();
    assert!(trace.iter().any(|(_, units)| *units == 256));
    for at in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for scratch in [false, true] {
                let control = Control {
                    trace: Mutex::new(Vec::new()),
                    stop: Some((at, cause.clone())),
                };
                assert_eq!(run(&control, scratch), Err(Error::Control(cause.clone())));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn layout_scratch_observes_unknown_annotations_without_authoring_semantics() {
    let field = Arc::new(
        Field::new("source", DataType::Utf8, true).with_metadata(HashMap::from([(
            NR_LOGICAL_TYPE_KEY.into(),
            "unknown".into(),
        )])),
    );
    let root = DataType::List(Arc::clone(&field));
    let mut scratch = [None; crate::MAX_ARROW_TYPE_PATH_DEPTH];
    let mut trace = Vec::new();
    let mut attributes = 0;
    validate_arrow_layout_structure_with_scratch_observed::<Error>(&root, &mut scratch, |visit| {
        trace.push(event(visit));
        if let ValueTypeVisit::Field(source) = visit {
            assert!(std::ptr::eq(source, field.as_ref()));
            assert_eq!(source.metadata()[NR_LOGICAL_TYPE_KEY], "unknown");
            attributes += 1;
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(attributes, 1);
    assert_eq!(
        validate_value_type_structure_observed::<Error>(&root, |_| Ok(())),
        Err(Error::Value(ValueTypeError::UnknownLogicalMetadata))
    );
    for at in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let mut scratch = [None; crate::MAX_ARROW_TYPE_PATH_DEPTH];
            let mut prefix = Vec::new();
            let result = validate_arrow_layout_structure_with_scratch_observed::<Error>(
                &root,
                &mut scratch,
                |visit| {
                    let ordinal = prefix.len();
                    assert!(ordinal <= at, "callback after original refusal");
                    prefix.push(event(visit));
                    if ordinal == at {
                        Err(Error::Control(cause.clone()))
                    } else {
                        Ok(())
                    }
                },
            );
            assert_eq!(result, Err(Error::Control(cause)));
            assert_eq!(prefix, trace[..=at]);
        }
    }
}
