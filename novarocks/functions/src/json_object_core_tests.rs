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
use arrow_array::{Array, BooleanArray, Int64Array, StringArray, UInt32Array};

struct Port {
    events: Vec<char>,
    stop: Option<usize>,
    reads: usize,
}
impl Port {
    fn event(&mut self, event: char) -> Result<(), &'static str> {
        let at = self.events.len();
        self.events.push(event);
        if self.stop == Some(at) {
            Err("original non-cloned control")
        } else {
            Ok(())
        }
    }
}
impl ScalarReadFailureSink for Port {
    type Error = JsonObjectFailure<&'static str>;
    fn read_data(&mut self, failure: ScalarReadFailure<'_>) -> Self::Error {
        OriginalData(failure.to_string())
    }
    fn observe(&mut self, event: ScalarReadObservation) -> Result<(), Self::Error> {
        match event {
            ScalarReadObservation::Step => self.event('s'),
            ScalarReadObservation::OpaqueBoundary => self.event('b'),
        }
        .map_err(Control)
    }
    fn reserve_scalar_copy(&mut self, _: usize, _: usize, _: usize) -> Result<(), Self::Error> {
        Ok(())
    }
}
impl JsonObjectPort for Port {
    type Control = &'static str;
    fn step(&mut self) -> Result<(), Self::Control> {
        self.event('s')
    }
    fn boundary(&mut self) -> Result<(), Self::Control> {
        self.event('b')
    }
    fn read_scalar(
        &mut self,
        array: &ArrayRef,
        row: usize,
    ) -> Result<Option<AggScalarValue>, JsonObjectFailure<Self::Control>> {
        self.reads += 1;
        scalar_from_array_with_failure(array, row, self)
    }
    fn debug_formatting_failure(&mut self) -> Self::Control {
        "unexpected Debug destination fault"
    }
}
fn port() -> Port {
    Port {
        events: Vec::new(),
        stop: None,
        reads: 0,
    }
}

enum Address {
    Column,
    Scalar,
    Selected,
}
struct Arguments {
    arrays: Vec<ArrayRef>,
    addresses: Vec<Address>,
}
impl JsonObjectArguments for Arguments {
    fn len(&self) -> usize {
        self.arrays.len()
    }
    fn array(&self, index: usize) -> &ArrayRef {
        &self.arrays[index]
    }
    fn value_row(&self, index: usize, ordinal: usize, row: usize) -> usize {
        match self.addresses[index] {
            Address::Column => row,
            Address::Scalar => 0,
            Address::Selected => ordinal,
        }
    }
}
fn output(result: Result<ArrayRef, JsonObjectFailure<&'static str>>) -> ArrayRef {
    match result {
        Ok(out) => out,
        Err(OriginalData(message)) => panic!("{message}"),
        Err(Control(cause)) => panic!("{cause}"),
    }
}

#[test]
fn json_object_core_independent_sparse_column_scalar_selected_addresses_and_odd_tail() {
    let arguments = Arguments {
        arrays: vec![
            Arc::new(StringArray::from(vec!["unused", "k", "unused2", "z"])),
            Arc::new(StringArray::from(vec!["{"])),
            Arc::new(StringArray::from(vec!["x", "y"])),
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(BooleanArray::from(vec![true])),
        ],
        addresses: vec![
            Address::Column,
            Address::Scalar,
            Address::Selected,
            Address::Selected,
            Address::Scalar,
        ],
    };
    let selection = Selection::try_sparse(4, &[1, 3]).unwrap();
    let out = output(evaluate_selected(&arguments, selection, &mut port()));
    let values = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        values,
        &StringArray::from(vec![
            r#"{"k": "{","x": 10,"true": null}"#,
            r#"{"z": "{","y": 20,"true": null}"#
        ])
    );
    let empty = Selection::try_sparse(4, &[]).unwrap();
    assert_eq!(
        output(evaluate_selected(&arguments, empty, &mut port())).len(),
        0
    );
}

#[test]
fn json_object_core_null_key_suppresses_unsupported_reader_without_early_data() {
    let arguments: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![None::<&str>])),
        Arc::new(UInt32Array::from(vec![1])),
    ];
    let mut reader = port();
    let out = output(evaluate_selected(
        arguments.as_slice(),
        Selection::all(1),
        &mut reader,
    ));
    assert!(out.is_null(0));
    assert_eq!(reader.reads, 1);
    let arguments: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["k"])),
        Arc::new(UInt32Array::from(vec![1])),
    ];
    assert!(
        matches!(evaluate_selected(arguments.as_slice(), Selection::all(1), &mut port()), Err(OriginalData(message)) if message == "unsupported scalar type: UInt32")
    );
}

#[test]
fn json_object_core_parse_and_fallback_every_callback_preserves_control_prefix() {
    let arguments: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["k", "k"])),
        Arc::new(StringArray::from(vec![r#"{"z":[true,"é"],"a":null}"#, "{"])),
    ];
    let mut original = port();
    let out = output(evaluate_selected(
        arguments.as_slice(),
        Selection::all(2),
        &mut original,
    ));
    let values = out.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        values,
        &StringArray::from(vec![
            r#"{"k": {"a": null, "z": [true, "é"]}}"#,
            r#"{"k": "{"}"#
        ])
    );
    for stop in 0..original.events.len() {
        let mut interrupted = Port {
            stop: Some(stop),
            ..port()
        };
        let result = evaluate_selected(arguments.as_slice(), Selection::all(2), &mut interrupted);
        assert!(matches!(
            result,
            Err(Control("original non-cloned control"))
        ));
        assert_eq!(interrupted.events, original.events[..=stop]);
    }
}
