// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::{
    exec::{chunk::ArrayPathStep, pipeline::operator::ConstructionFailure},
    runtime::mem_tracker::MemTracker,
};
use arrow::{
    array::{Array, Int32Array, StringArray},
    datatypes::Field,
};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Control {
    calls: AtomicUsize,
    stop: usize,
}
impl Control {
    fn new(stop: usize) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            stop,
        }
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), novarocks_functions::KernelFailure> {
        let next = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if next == self.stop {
            Err(novarocks_functions::KernelFailure::DeadlineExceeded)
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), novarocks_functions::KernelFailure> {
        panic!("canonical decode never waits")
    }
}
fn owner() -> Box<dyn InputConstructionOwner> {
    let factory = crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        MemTracker::new_root("canonical decoder construction test"),
        Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default()),
    ).unwrap();
    let declaration = crate::exec::chunk::typed_fixture::layout(
        Arc::new(arrow::datatypes::Schema::empty()),
        &[],
        &[],
    );
    let schema = factory.create_compiled_chunk_schema(&declaration).unwrap();
    factory.begin_typed_construction(&schema).unwrap()
}
fn blob(marker: u8, value: &[u8]) -> Vec<u8> {
    let mut out = vec![marker];
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value);
    out
}
fn frame(value: Option<&[u8]>) -> Vec<u8> {
    match value {
        None => vec![0],
        Some(value) => {
            let mut out = vec![1];
            out.extend_from_slice(&(value.len() as u32).to_le_bytes());
            out.extend_from_slice(value);
            out
        }
    }
}
fn nested(marker: u8, values: &[Option<&[u8]>], count: usize) -> Vec<u8> {
    let mut out = vec![marker];
    out.extend_from_slice(&(count as u32).to_le_bytes());
    for value in values {
        out.extend_from_slice(&frame(*value));
    }
    out
}
fn fixed(marker: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![marker];
    out.extend_from_slice(payload);
    out
}

#[test]
fn governed_canonical_nested_export_has_independent_values_nulls_and_selection_order() {
    let owner = owner();
    let short = blob(8, b"alpha");
    let long_value = "文x".repeat(40_000);
    let long = blob(8, long_value.as_bytes());
    let populated = nested(16, &[Some(&short), None, Some(&long)], 3);
    let empty = nested(16, &[], 0);
    let ty = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
    let rows = [
        Some(populated.as_slice()),
        None,
        Some(empty.as_slice()),
        Some(populated.as_slice()),
    ];
    let output =
        decode_group_key_rows_construction(&ty, &rows, owner.as_ref(), &Control::new(usize::MAX))
            .unwrap();
    drop(populated);
    drop(empty);
    drop(short);
    drop(long);
    assert_eq!(output.data_type(), &ty);
    let lists = output.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(lists.value_offsets(), &[0, 3, 3, 3, 6]);
    assert!(lists.is_null(1));
    assert!(!lists.is_null(2));
    let strings = lists
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        strings.iter().collect::<Vec<_>>(),
        vec![
            Some("alpha"),
            None,
            Some(long_value.as_str()),
            Some("alpha"),
            None,
            Some(long_value.as_str())
        ]
    );
}

#[test]
fn governed_canonical_export_preserves_struct_map_parameters_and_largeint_bytes() {
    let owner = owner();
    let control = Control::new(usize::MAX);
    let decimal = fixed(14, &12345_i128.to_le_bytes());
    let largeint = fixed(15, &(-1234567_i128).to_le_bytes());
    let fields: Fields = vec![
        Field::new("amount", DataType::Decimal128(18, 3), true),
        Field::new("wide", DataType::FixedSizeBinary(16), false),
    ]
    .into();
    let row = nested(17, &[Some(&decimal), Some(&largeint)], 2);
    let ty = DataType::Struct(fields);
    let output = decode_group_key_rows_construction(
        &ty,
        &[None, Some(&row), Some(&row)],
        owner.as_ref(),
        &control,
    )
    .unwrap();
    let structs = output.as_any().downcast_ref::<StructArray>().unwrap();
    assert!(structs.is_null(0));
    assert_eq!(structs.data_type(), &ty);
    assert_eq!(
        structs
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(1),
        12345
    );
    assert_eq!(
        structs
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(2),
        (-1234567_i128).to_be_bytes()
    );
    let key = fixed(4, &7_i32.to_le_bytes());
    let value = blob(20, b"payload");
    let map_row = nested(18, &[Some(&key), Some(&value)], 1);
    let entry = Arc::new(Field::new(
        "entries",
        DataType::Struct(
            vec![
                Field::new("key", DataType::Int32, false),
                Field::new("value", DataType::Binary, true),
            ]
            .into(),
        ),
        false,
    ));
    let map_ty = DataType::Map(entry, true);
    let map = decode_group_key_rows_construction(
        &map_ty,
        &[Some(&map_row), None],
        owner.as_ref(),
        &control,
    )
    .unwrap();
    let map = map.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(map.data_type(), &map_ty);
    assert_eq!(map.value_offsets(), &[0, 1, 1]);
    assert!(map.is_null(1));
    assert_eq!(
        map.keys()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        7
    );
    assert_eq!(
        map.values()
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap()
            .value(0),
        b"payload"
    );
}

#[test]
fn governed_canonical_zero_field_struct_keeps_nonzero_rows_and_root_nulls() {
    let row = nested(17, &[], 0);
    let owner = owner();
    let output = decode_group_key_rows_construction(
        &DataType::Struct(Fields::empty()),
        &[Some(&row), None, Some(&row)],
        owner.as_ref(),
        &Control::new(usize::MAX),
    )
    .unwrap();
    assert_eq!(output.len(), 3);
    assert!(output.is_null(1));
    assert!(!output.is_null(2));
}

struct Refusals<'a> {
    inner: &'a dyn InputConstructionOwner,
    request: AtomicUsize,
    opaque: AtomicUsize,
    observe: AtomicUsize,
    stop_request: usize,
    stop_opaque: usize,
    stop_observe: usize,
}
impl<'a> Refusals<'a> {
    fn new(
        inner: &'a dyn InputConstructionOwner,
        stop_request: usize,
        stop_opaque: usize,
        stop_observe: usize,
    ) -> Self {
        Self {
            inner,
            request: AtomicUsize::new(0),
            opaque: AtomicUsize::new(0),
            observe: AtomicUsize::new(0),
            stop_request,
            stop_opaque,
            stop_observe,
        }
    }
    fn refusal() -> ConstructionFailure {
        ConstructionFailure::from(ExecutionFailure::from(
            novarocks_functions::KernelFailure::DeadlineExceeded,
        ))
    }
}
impl InputConstructionOwner for Refusals<'_> {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.inner.storage_lifetime()
    }
    fn request(&self, layout: Layout) -> Result<(), ConstructionFailure> {
        if self.request.fetch_add(1, Ordering::SeqCst) + 1 == self.stop_request {
            return Err(Self::refusal());
        }
        self.inner.request(layout)
    }
    fn observe_opaque_upper_bound(&self, bytes: usize) -> Result<(), ConstructionFailure> {
        if self.opaque.fetch_add(1, Ordering::SeqCst) + 1 == self.stop_opaque {
            return Err(Self::refusal());
        }
        self.inner.observe_opaque_upper_bound(bytes)
    }
    fn observe(
        &self,
        occurrence: usize,
        path: &[ArrayPathStep],
        step: ArrayNormalizationStep<'_>,
    ) -> Result<(), ConstructionFailure> {
        if self.observe.fetch_add(1, Ordering::SeqCst) + 1 == self.stop_observe {
            return Err(Self::refusal());
        }
        self.inner.observe(occurrence, path, step)
    }
}

#[test]
fn governed_canonical_each_actual_creator_and_control_refusal_stays_original() {
    let owner = owner();
    let value = blob(8, b"independent payload");
    let row = nested(16, &[Some(&value), None], 2);
    let ty = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
    let rows = [Some(row.as_slice()), None];
    let baseline = Refusals::new(owner.as_ref(), usize::MAX, usize::MAX, usize::MAX);
    let control = Control::new(usize::MAX);
    decode_group_key_rows_construction(&ty, &rows, &baseline, &control).unwrap();
    let requests = baseline.request.load(Ordering::SeqCst);
    let opaque = baseline.opaque.load(Ordering::SeqCst);
    let observes = baseline.observe.load(Ordering::SeqCst);
    let checks = control.calls.load(Ordering::SeqCst);
    assert!(requests > 0 && opaque > 0 && observes > 0 && checks > 0);
    let expected = ExecutionFailure::from(novarocks_functions::KernelFailure::DeadlineExceeded);
    for stop in 1..=requests {
        let refusing = Refusals::new(owner.as_ref(), stop, usize::MAX, usize::MAX);
        let err =
            decode_group_key_rows_construction(&ty, &rows, &refusing, &Control::new(usize::MAX))
                .unwrap_err();
        assert_eq!(err, expected);
        assert_eq!(refusing.request.load(Ordering::SeqCst), stop);
    }
    for stop in 1..=opaque {
        let refusing = Refusals::new(owner.as_ref(), usize::MAX, stop, usize::MAX);
        assert_eq!(
            decode_group_key_rows_construction(&ty, &rows, &refusing, &Control::new(usize::MAX))
                .unwrap_err(),
            expected
        );
        assert_eq!(refusing.opaque.load(Ordering::SeqCst), stop);
    }
    for stop in 1..=observes {
        let refusing = Refusals::new(owner.as_ref(), usize::MAX, usize::MAX, stop);
        assert_eq!(
            decode_group_key_rows_construction(&ty, &rows, &refusing, &Control::new(usize::MAX))
                .unwrap_err(),
            expected
        );
        assert_eq!(refusing.observe.load(Ordering::SeqCst), stop);
    }
    for stop in 1..=checks {
        let stopped = Control::new(stop);
        assert_eq!(
            decode_group_key_rows_construction(&ty, &rows, owner.as_ref(), &stopped).unwrap_err(),
            expected
        );
        assert_eq!(stopped.calls.load(Ordering::SeqCst), stop);
    }
}

#[test]
fn canonical_utf8_validation_checks_inside_long_values_and_preserves_split_codepoints() {
    let owner = owner();
    let long = "a文🪨".repeat(30_000);
    let control = Control::new(usize::MAX);
    let resource = Governed {
        owner: owner.as_ref(),
        control: &control,
    };
    assert_eq!(utf8(long.as_bytes(), &resource).unwrap(), long);
    assert!(control.calls.load(Ordering::SeqCst) > 2);
    let stop = Control::new(2);
    let resource = Governed {
        owner: owner.as_ref(),
        control: &stop,
    };
    assert_eq!(
        utf8(long.as_bytes(), &resource).unwrap_err(),
        ExecutionFailure::from(novarocks_functions::KernelFailure::DeadlineExceeded)
    );
    assert_eq!(stop.calls.load(Ordering::SeqCst), 2);
}
