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
use arrow::{
    array::*,
    datatypes::DataType,
    ipc::{
        self,
        writer::{IpcWriteOptions, StreamWriter},
    },
    record_batch::RecordBatch,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use novarocks_type_contract::{CompileControlError, NR_LOGICAL_TYPE_KEY};
use std::sync::Mutex;

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
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
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 4096,
        max_array_nodes: 4096,
        max_logical_elements: 1 << 20,
        max_retained_buffer_bytes: 1 << 24,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 0,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 28,
        max_library_validation_bytes: 1 << 28,
    }
}

fn limits() -> RecursiveStreamProjectionLimits {
    RecursiveStreamProjectionLimits {
        max_input_bytes: 1 << 24,

        batch: RecursiveBatchProjectionLimits {
            flat: crate::ipc_flat_batch_v2::FlatBatchProjectionLimits {
                max_metadata_bytes: 1 << 20,
                max_body_bytes: 1 << 24,
                max_rows: 4096,
                max_buffer_descriptors: 16384,
                max_view_validation_bytes: 1 << 24,
            },
            max_field_nodes: 4096,
            max_total_rows: 1 << 20,
            max_geometry_request_bytes: 1 << 20,
        },
    }
}
fn reader_limits() -> RecursiveReaderProjectionLimits {
    RecursiveReaderProjectionLimits {
        max_new_allocation_request_bytes: 1 << 28,
        max_coexisting_source_and_request_bytes: 1 << 29,
        max_cumulative_library_work: 1 << 30,
    }
}
fn verifier() -> VerifierOptions {
    VerifierOptions {
        max_depth: 67,
        max_tables: 65536,
        max_apparent_size: 1 << 26,
        ignore_missing_null_terminator: false,
    }
}
fn fixture(array: ArrayRef, field: &Arc<Field>) -> Vec<u8> {
    let schema = Arc::new(Schema::new([Arc::clone(field)]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();
    let options = IpcWriteOptions::try_new(8, false, ipc::MetadataVersion::V5).unwrap();
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new_with_options(&mut bytes, &schema, options).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    crate::ipc_flat_stream_v2::standard_test_record_frame(&bytes).to_vec()
}
fn nested() -> ArrayRef {
    let json = Arc::new(
        Field::new("document", DataType::Utf8, true).with_metadata(HashMap::from([
            (NR_LOGICAL_TYPE_KEY.into(), "json".into()),
            ("provider".into(), "actual-child".into()),
        ])),
    );
    let entries: ArrayRef = Arc::new(StructArray::new(
        vec![json, Arc::new(Field::new("view", DataType::Utf8View, true))].into(),
        vec![
            Arc::new(StringArray::from(vec![Some("{}"), Some("[1]"), None])),
            Arc::new(StringViewArray::from(vec![
                "hidden payload exceeds inline",
                "selected payload exceeds inline",
                "third",
            ])),
        ],
        None,
    ));
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", entries.data_type().clone(), true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 3])),
        entries,
        Some(NullBuffer::from(vec![false, true])),
    ))
}
fn source(bytes: &Vec<u8>) -> usize {
    bytes.capacity() + (1 << 20)
}
fn stream<'a, 'f>(bytes: &'a [u8], field: &'f Field) -> RecursiveConstantStream<'a, 'f> {
    preflight_recursive_constant_stream(bytes, field, limits(), &verifier(), &Control::default())
        .unwrap()
}
fn materialize(
    stream: &RecursiveConstantStream<'_, '_>,
    field: &Arc<Field>,
    source: usize,
    reader: RecursiveReaderProjectionLimits,
    control: &Control,
) -> Result<ConstantPool, RecursiveReaderError> {
    stream.materialize_pool(
        Arc::clone(field),
        FunctionValueType::new(field.data_type().clone(), true),
        source,
        policy(),
        reader,
        control,
    )
}
#[test]
fn standard_nested_reader_retains_original_field_and_selected_values() {
    let array = nested();
    let field = Arc::new(
        Field::new("original", array.data_type().clone(), true)
            .with_metadata(HashMap::from([("identity".into(), "retained".into())])),
    );
    let bytes = fixture(Arc::clone(&array), &field);
    let checked = stream(&bytes, &field);
    let pool = materialize(
        &checked,
        &field,
        source(&bytes),
        reader_limits(),
        &Control::default(),
    )
    .unwrap();
    assert!(Arc::ptr_eq(pool.field_ref(), &field));
    assert_eq!(pool.array().to_data(), array.to_data());
    assert!(
        pool.value(0)
            .unwrap()
            .is_null_observed(CompilePhase::Decode, &Control::default())
            .unwrap()
    );
    assert_eq!(pool.value(1).unwrap().ordinal(), 1);
    assert_eq!(pool.resource_facts().array_nodes, 4);
}
#[test]
fn recursive_reader_exact_request_work_limits_and_original_source_are_required() {
    let array = nested();
    let field = Arc::new(Field::new("original", array.data_type().clone(), true));
    let bytes = fixture(array, &field);
    let checked = stream(&bytes, &field);
    let ty = FunctionValueType::new(field.data_type().clone(), true);
    let facts = checked
        .preflight_reader_resources(
            &ty,
            source(&bytes),
            policy(),
            reader_limits(),
            &Control::default(),
        )
        .unwrap();
    let exact = RecursiveReaderProjectionLimits {
        max_new_allocation_request_bytes: facts.new_allocation_request_bytes_upper_bound,
        max_coexisting_source_and_request_bytes: facts
            .coexisting_source_and_request_bytes_upper_bound,
        max_cumulative_library_work: facts.cumulative_library_work_upper_bound,
    };
    materialize(&checked, &field, source(&bytes), exact, &Control::default()).unwrap();
    for failed in [
        RecursiveReaderProjectionLimits {
            max_new_allocation_request_bytes: exact.max_new_allocation_request_bytes - 1,
            ..exact
        },
        RecursiveReaderProjectionLimits {
            max_coexisting_source_and_request_bytes: exact.max_coexisting_source_and_request_bytes
                - 1,
            ..exact
        },
        RecursiveReaderProjectionLimits {
            max_cumulative_library_work: exact.max_cumulative_library_work - 1,
            ..exact
        },
    ] {
        assert!(
            materialize(
                &checked,
                &field,
                source(&bytes),
                failed,
                &Control::default()
            )
            .is_err()
        );
    }
    let foreign = Arc::new(field.as_ref().clone());
    assert!(
        materialize(
            &checked,
            &foreign,
            source(&bytes),
            exact,
            &Control::default()
        )
        .is_err()
    );
    assert!(
        materialize(
            &checked,
            &field,
            bytes.len() - 1,
            exact,
            &Control::default()
        )
        .is_err()
    );
    assert!(
        checked
            .preflight_reader_resources(
                &FunctionValueType::new(DataType::Int64, true),
                source(&bytes),
                policy(),
                exact,
                &Control::default()
            )
            .is_err()
    );
}
#[test]
fn recursive_reader_every_original_callback_and_ordinary_tail_preserves_primary_cause() {
    let array = nested();
    let field = Arc::new(Field::new("original", array.data_type().clone(), true));
    let bytes = fixture(array, &field);
    let checked = stream(&bytes, &field);
    for successful in [true, false] {
        let reader = if successful {
            reader_limits()
        } else {
            RecursiveReaderProjectionLimits {
                max_new_allocation_request_bytes: 0,
                ..reader_limits()
            }
        };
        let control = Control::default();
        assert_eq!(
            materialize(&checked, &field, source(&bytes), reader, &control).is_ok(),
            successful
        );
        let trace = control.trace.lock().unwrap().clone();
        assert_eq!(trace.first(), Some(&(CompilePhase::Decode, 0)));
        for at in 0..trace.len() {
            for cause in CAUSES {
                let control = Control {
                    trace: Mutex::new(Vec::new()),
                    stop: Some((at, cause.clone())),
                };
                assert!(
                    matches!(materialize(&checked, &field, source(&bytes), reader, &control),
                Err(RecursiveReaderError::Projection(FlatPoolResourceError::Control(actual))) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}
#[test]
fn recursive_reader_rejects_trailing_data_and_validates_payload_below_null_parent() {
    let array: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1])),
        Arc::new(StringArray::from(vec!["hidden"])),
        Some(NullBuffer::from(vec![false])),
    ));
    let field = Arc::new(Field::new("original", array.data_type().clone(), true));
    let mut bytes = fixture(array, &field);
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(
        preflight_recursive_constant_stream(
            &trailing,
            &field,
            limits(),
            &verifier(),
            &Control::default()
        )
        .is_err()
    );
    {
        let checked = stream(&bytes, &field);
        let node = checked.checked.nodes.last().unwrap();
        let descriptor = checked.batch.buffers().unwrap().get(node.buffer_start + 2);
        let index =
            checked.body.as_ptr() as usize - bytes.as_ptr() as usize + descriptor.offset() as usize;
        bytes[index] = 255;
    }
    let checked = stream(&bytes, &field);
    assert!(matches!(
        materialize(
            &checked,
            &field,
            source(&bytes),
            reader_limits(),
            &Control::default()
        ),
        Err(RecursiveReaderError::Arrow(_))
    ));
}

#[test]
fn recursive_framing_every_original_callback_and_ordinary_tail_preserves_primary_cause() {
    let array = nested();
    let field = Arc::new(Field::new("original", array.data_type().clone(), true));
    let bytes = fixture(array, &field);
    for successful in [true, false] {
        let mut input = bytes.clone();
        if !successful {
            input.push(0);
        }
        let control = Control::default();
        assert_eq!(
            preflight_recursive_constant_stream(&input, &field, limits(), &verifier(), &control)
                .is_ok(),
            successful
        );
        let trace = control.trace.lock().unwrap().clone();
        assert_eq!(trace.first(), Some(&(CompilePhase::Decode, 0)));
        for at in 0..trace.len() {
            for cause in CAUSES {
                let control = Control {
                    trace: Mutex::new(Vec::new()),
                    stop: Some((at, cause.clone())),
                };
                assert!(matches!(
                    preflight_recursive_constant_stream(&input, &field, limits(), &verifier(), &control),
                    Err(TypeCodecError::Control(actual)) if actual == cause
                ));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

mod borrowed_tests {
    include!("ipc_recursive_stream_v2/borrowed_reader_tests.rs");
}

#[path = "ipc_recursive_stream_v2/prepared_tests.rs"]
mod prepared_tests;
