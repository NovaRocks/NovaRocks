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
    datatypes::{DataType, Field, Schema},
    ipc::{
        self,
        reader::StreamReader,
        writer::{IpcWriteOptions, StreamWriter},
    },
    record_batch::RecordBatch,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use novarocks_constant_contract::ConstantPolicy;
use novarocks_type_contract::{
    CompileControlError, FunctionValueType, NR_LOGICAL_TYPE_KEY, ValueLogicalType,
};
use std::{
    collections::HashMap,
    io::Cursor,
    sync::{Arc, Mutex},
};

#[path = "prepared_tests.rs"]
mod prepared_tests;

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
struct Control {
    phase: CompilePhase,
    refusal: Option<(usize, CompileControlError)>,
    trace: Mutex<Vec<(CompilePhase, u32)>>,
}
impl Control {
    fn good(phase: CompilePhase) -> Self {
        Self {
            phase,
            refusal: None,
            trace: Mutex::new(Vec::new()),
        }
    }
    fn refusing(at: usize, cause: CompileControlError) -> Self {
        Self {
            phase: CompilePhase::Encode,
            refusal: Some((at, cause)),
            trace: Mutex::new(Vec::new()),
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, self.phase);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((refusal, _)) = self.refusal {
            assert!(at <= refusal, "callback after primary refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((refusal, cause)) if refusal == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 4096,
        max_array_nodes: 4096,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 8 * 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 8,
        max_metadata_bytes: 65536,
        max_library_validation_work: 32 * 1024 * 1024,
        max_library_validation_bytes: 32 * 1024 * 1024,
    }
}
fn limits() -> RecursivePoolWriteLimits {
    RecursivePoolWriteLimits {
        flat: FlatPoolWriteLimits {
            max_rows: 4096,
            max_buffer_descriptors: 16384,
            max_body_bytes: 8 * 1024 * 1024,
            max_encoded_stream_bytes: 16 * 1024 * 1024,
            schema: ipc_schema_v2::IpcSchemaProjectionLimits {
                max_field_occurrences: 4096,
                max_type_occurrences: 4096,
                max_string_bytes: 65536,
                max_flatbuffer_bytes: 4 * 1024 * 1024,
            },
            max_new_allocation_request_bytes: 128 * 1024 * 1024,
            max_coexisting_source_and_request_bytes: 256 * 1024 * 1024,
            max_cumulative_library_work: 1024 * 1024 * 1024,
        },
        max_field_nodes: 4096,
        max_total_rows: 65536,
    }
}
fn pool(array: ArrayRef) -> ConstantPool {
    let ty = FunctionValueType::new(array.data_type().clone(), true);
    let field = Arc::new(
        Field::new("original", array.data_type().clone(), true).with_metadata(HashMap::from([(
            "provider".into(),
            "original-field".into(),
        )])),
    );
    ConstantPool::try_new(
        field,
        ty,
        array.to_data(),
        policy(),
        CompilePhase::Decode,
        &Control::good(CompilePhase::Decode),
    )
    .unwrap()
}
fn invoice(pool: &ConstantPool) -> usize {
    // Conservative complete fixture owner invoice; no formal account claim.
    1024 * 1024 + pool.resource_facts().retained_buffer_capacity_bytes as usize
}
fn standard(pool: &ConstantPool) -> Vec<u8> {
    let schema = Arc::new(Schema::new([Arc::clone(pool.field_ref())]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(pool.array())]).unwrap();
    let options = IpcWriteOptions::try_new(8, false, ipc::MetadataVersion::V5).unwrap();
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new_with_options(&mut bytes, &schema, options).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    bytes
}
fn batch_parts(bytes: &[u8]) -> (ipc::RecordBatch<'_>, &[u8]) {
    assert_eq!(&bytes[..4], &[255; 4]);
    let first = usize::try_from(i32::from_le_bytes(bytes[4..8].try_into().unwrap())).unwrap();
    let second = 8 + first;
    assert_eq!(&bytes[second..second + 4], &[255; 4]);
    let length = usize::try_from(i32::from_le_bytes(
        bytes[second + 4..second + 8].try_into().unwrap(),
    ))
    .unwrap();
    let message = ipc::root_as_message(&bytes[second + 8..second + 8 + length]).unwrap();
    let batch = message.header_as_record_batch().unwrap();
    let body_start = second + 8 + length;
    let body = &bytes[body_start..body_start + usize::try_from(message.bodyLength()).unwrap()];
    assert_eq!(
        &bytes[body_start + body.len()..],
        &[255, 255, 255, 255, 0, 0, 0, 0]
    );
    (batch, body)
}
// Derive descriptor roles from the public Arrow layout and the original
// arrays, without consulting either writer's geometry or descriptor lengths.
// A validity entry keeps its exact source bytes, including unused tail bits.
#[derive(Default)]
struct SourceWireOracle {
    nodes: Vec<(usize, usize)>,
    buffers: Vec<Option<(usize, Vec<u8>)>>,
    variadic_counts: Vec<i64>,
}
impl SourceWireOracle {
    fn visit(&mut self, array: &ArrayRef) {
        match array.data_type() {
            DataType::Boolean
            | DataType::Int32
            | DataType::Int64
            | DataType::Float64
            | DataType::Utf8
            | DataType::Utf8View
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::Map(_, _)
            | DataType::Struct(_) => {}
            other => panic!("no independent recursive IPC fixture oracle for {other:?}"),
        }
        let node = self.nodes.len();
        self.nodes.push((array.len(), array.null_count()));
        let layout = arrow::array::layout(array.data_type());
        if layout.can_contain_null_mask {
            let bitmap = match array.nulls() {
                // Public bit slicing preserves a byte-aligned source tail;
                // an unaligned slice packs only the declared source bits.
                Some(nulls) => nulls.inner().sliced().as_slice().to_vec(),
                None => vec![0xff; array.len().div_ceil(8)],
            };
            assert_eq!(bitmap.len(), array.len().div_ceil(8));
            self.buffers.push(Some((node, bitmap)));
        }
        self.buffers.extend((0..layout.buffers.len()).map(|_| None));
        if layout.variadic {
            let data = array.to_data();
            let count = data
                .buffers()
                .len()
                .checked_sub(layout.buffers.len())
                .unwrap();
            self.variadic_counts.push(i64::try_from(count).unwrap());
            self.buffers.extend((0..count).map(|_| None));
        }
        match array.data_type() {
            DataType::Struct(_) => {
                let structure = array.as_any().downcast_ref::<StructArray>().unwrap();
                for child in structure.columns() {
                    self.visit(child);
                }
            }
            DataType::List(_) => {
                let list = array.as_any().downcast_ref::<ListArray>().unwrap();
                let offsets = list.value_offsets();
                let start = usize::try_from(offsets[0]).unwrap();
                let end = usize::try_from(*offsets.last().unwrap()).unwrap();
                self.visit(&list.values().slice(start, end - start));
            }
            DataType::LargeList(_) => {
                let list = array.as_any().downcast_ref::<LargeListArray>().unwrap();
                let offsets = list.value_offsets();
                let start = usize::try_from(offsets[0]).unwrap();
                let end = usize::try_from(*offsets.last().unwrap()).unwrap();
                self.visit(&list.values().slice(start, end - start));
            }
            DataType::Map(_, _) => {
                let map = array.as_any().downcast_ref::<MapArray>().unwrap();
                let offsets = map.value_offsets();
                let start = usize::try_from(offsets[0]).unwrap();
                let end = usize::try_from(*offsets.last().unwrap()).unwrap();
                let entries: ArrayRef = Arc::new(map.entries().slice(start, end - start));
                self.visit(&entries);
            }
            _ => {}
        }
    }
}
fn agree_with_standard(pool: &ConstantPool, bytes: &[u8]) -> ArrayRef {
    let original = standard(pool);
    let (left, lb) = batch_parts(bytes);
    let (right, rb) = batch_parts(&original);
    assert_eq!(left.length(), right.length());
    let ln = left.nodes().unwrap();
    let rn = right.nodes().unwrap();
    let mut source = SourceWireOracle::default();
    source.visit(pool.array());
    assert_eq!(ln.len(), source.nodes.len());
    assert_eq!(ln.len(), rn.len());
    for ((a, b), &(length, nulls)) in ln.iter().zip(rn.iter()).zip(&source.nodes) {
        assert_eq!((a.length(), a.null_count()), (b.length(), b.null_count()));
        assert_eq!(
            (
                usize::try_from(a.length()).unwrap(),
                usize::try_from(a.null_count()).unwrap()
            ),
            (length, nulls)
        );
    }
    for batch in [left, right] {
        let variadic = batch
            .variadicBufferCounts()
            .map(|counts| counts.iter().collect::<Vec<_>>())
            .unwrap_or_default();
        assert_eq!(variadic, source.variadic_counts);
    }
    let ld = left.buffers().unwrap();
    let rd = right.buffers().unwrap();
    assert_eq!(ld.len(), rd.len());
    assert_eq!(ld.len(), source.buffers.len());
    for ((a, b), role) in ld.iter().zip(rd.iter()).zip(&source.buffers) {
        let astart = usize::try_from(a.offset()).unwrap();
        let bstart = usize::try_from(b.offset()).unwrap();
        let length = usize::try_from(a.length()).unwrap();
        let actual = &lb[astart..astart + length];
        if let Some((node, bitmap)) = role {
            assert_eq!(length, bitmap.len());
            assert_eq!(actual, bitmap.as_slice());
            if source.nodes[*node].1 == 0 {
                // Arrow 60.1 omits only the all-valid bitmap. Our frozen
                // writer still emits the complete, independently checked bits.
                assert_eq!(b.length(), 0);
                continue;
            }
        }
        assert_eq!(a.length(), b.length());
        assert_eq!(actual, &rb[bstart..bstart + length]);
    }
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None).unwrap();
    assert!(
        novarocks_type_contract::arrow_fields_exact_observed::<
            novarocks_constant_contract::ConstantError,
        >(pool.field(), reader.schema().field(0), || Ok(()))
        .unwrap()
    );
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), pool.data().len());
    assert!(reader.next().is_none());
    Arc::clone(batch.column(0))
}
#[test]
fn actual_recursive_writer_all_valid_child_slice_keeps_source_bitmap_tail_bits() {
    for (offsets, expected, expected_values) in [
        // Child [8, 10) starts at a byte boundary; unused source bits survive.
        (vec![0, 8, 10, 12], 0x0b, [8, 9]),
        // Child [1, 3) is repacked, so unused destination bits are zero.
        (vec![0, 1, 3, 12], 0x03, [1, 2]),
    ] {
        // Author every source validity bit explicitly, including the tail.
        let values = Int32Array::new(
            ScalarBuffer::from((0..12).collect::<Vec<i32>>()),
            Some(NullBuffer::new(arrow_buffer::BooleanBuffer::new(
                arrow_buffer::Buffer::from(vec![0xfe_u8, 0x0b]),
                0,
                12,
            ))),
        );
        assert_eq!(values.len(), 12);
        assert_eq!(values.null_count(), 2);
        assert_eq!(values.nulls().unwrap().inner().values(), &[0xfe_u8, 0x0b]);
        let array = ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Arc::new(values),
            None,
        )
        .slice(1, 1);
        let original = pool(Arc::new(array));
        let source = original
            .array()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(source.len(), 1);
        assert_eq!(
            source.value_offsets(),
            &[expected_values[0], expected_values[1] + 1]
        );
        assert_eq!(source.values().len(), 12);
        assert_eq!(source.values().null_count(), 2);
        let row = source.value(0);
        let row = row.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(row.len(), 2);
        assert_eq!(row.null_count(), 0);
        assert_eq!(row.values().as_ref(), &expected_values);
        let bytes = encode_recursive_pool(
            &original,
            invoice(&original),
            limits(),
            &Control::good(CompilePhase::Encode),
        )
        .unwrap();
        let output = agree_with_standard(&original, &bytes);
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .values()
                .null_count(),
            0
        );
        let output = output.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output.value_offsets(), &[0, 2]);
        assert_eq!(output.values().len(), 2);
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .as_ref(),
            &expected_values
        );
        // List validity + offsets precede the primitive child's validity.
        let (batch, body) = batch_parts(&bytes);
        let validity = batch.buffers().unwrap().get(2);
        assert_eq!(validity.length(), 1);
        assert_eq!(body[usize::try_from(validity.offset()).unwrap()], expected);
    }
}

fn list_fixture(rows: usize) -> ConstantPool {
    let offsets = (0..=rows)
        .map(|i| i32::try_from(i * 2).unwrap())
        .collect::<Vec<_>>();
    let values: ArrayRef = Arc::new(Int32Array::from_iter_values(
        0..i32::try_from(rows * 2).unwrap(),
    ));
    let array = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, false)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        values,
        None,
    );
    pool(Arc::new(array))
}

#[test]
fn actual_recursive_writer_list_rebases_sliced_offsets_and_keeps_hidden_null_payload() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![10, 11, 22, 23, 24, 25, 26, 99]));
    let array = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, false)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 4, 5, 7, 8])),
        values,
        Some(NullBuffer::from(vec![true, false, true, true, true])),
    )
    .slice(1, 3);
    let original = pool(Arc::new(array));
    let bytes = encode_recursive_pool(
        &original,
        invoice(&original),
        limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    let output = agree_with_standard(&original, &bytes);
    let output = output.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(output.value_offsets(), &[0, 2, 3, 5]);
    assert!(output.is_null(0));
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[22, 23, 24, 25, 26]
    );
    assert_eq!(original.value(2).unwrap().ordinal(), 2);
}

#[test]
fn actual_recursive_writer_struct_largelist_map_nested_spans_match_independent_arrow() {
    let strings: ArrayRef = Arc::new(StringArray::from(vec![
        Some("a"),
        Some("hidden"),
        Some("c"),
        None,
    ]));
    let list = LargeListArray::new(
        Arc::new(Field::new("entry", DataType::Utf8, true)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0_i64, 1, 2, 3, 4])),
        strings,
        Some(NullBuffer::from(vec![true, false, true, true])),
    );
    let fields = vec![
        Arc::new(Field::new("list", list.data_type().clone(), true)),
        Arc::new(Field::new("flag", DataType::Boolean, true)),
    ];
    let array = StructArray::new(
        fields.into(),
        vec![
            Arc::new(list),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])),
        ],
        Some(NullBuffer::from(vec![true, false, true, true])),
    )
    .slice(1, 2);
    let original = pool(Arc::new(array));
    let bytes = encode_recursive_pool(
        &original,
        invoice(&original),
        limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    let output = agree_with_standard(&original, &bytes);
    let output = output.as_any().downcast_ref::<StructArray>().unwrap();
    assert!(output.is_null(0));
    let list = output
        .column(0)
        .as_any()
        .downcast_ref::<LargeListArray>()
        .unwrap();
    assert_eq!(list.value_offsets(), &[0, 1, 2]);
    assert_eq!(
        list.values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "hidden"
    );
    let entries = StructArray::new(
        vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("value", DataType::Int64, true)),
        ]
        .into(),
        vec![
            Arc::new(StringArray::from(vec!["a", "hidden", "c", "d"])),
            Arc::new(Int64Array::from(vec![Some(1), Some(-99), None, Some(4)])),
        ],
        None,
    );
    let map = MapArray::new(
        Arc::new(Field::new("entries", entries.data_type().clone(), false)),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2, 4])),
        entries,
        Some(NullBuffer::from(vec![true, false, true])),
        false,
    )
    .slice(1, 2);
    let original = pool(Arc::new(map));
    let bytes = encode_recursive_pool(
        &original,
        invoice(&original),
        limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    let output = agree_with_standard(&original, &bytes);
    let output = output.as_any().downcast_ref::<MapArray>().unwrap();
    assert!(output.is_null(0));
    assert_eq!(output.value_offsets(), &[0, 1, 3]);
    assert_eq!(
        output
            .keys()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "hidden"
    );
}

#[test]
fn actual_recursive_writer_nested_views_preserve_all_headers_and_nominal_metadata() {
    let text = "external-string-long-enough-for-a-view";
    let views = StringViewArray::from(vec![Some(text), None, Some("small")]);
    let json = Field::new("json", DataType::Utf8, true).with_metadata(HashMap::from([
        (
            NR_LOGICAL_TYPE_KEY.into(),
            ValueLogicalType::Json.metadata_value().unwrap().into(),
        ),
        ("provider".into(), "original".into()),
    ]));
    let array = StructArray::new(
        vec![
            Arc::new(Field::new("views", DataType::Utf8View, true)),
            Arc::new(json),
        ]
        .into(),
        vec![
            Arc::new(views),
            Arc::new(StringArray::from(vec![
                Some("{\"a\":1}"),
                None,
                Some("null"),
            ])),
        ],
        None,
    );
    let original = pool(Arc::new(array));
    let facts = preflight_recursive_pool_write(
        &original,
        invoice(&original),
        limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    assert_eq!((facts.field_nodes, facts.view_fields), (3, 1));
    let bytes = encode_recursive_pool(
        &original,
        invoice(&original),
        limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    let output = agree_with_standard(&original, &bytes);
    let output = output.as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(
        output
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(0),
        text
    );
    assert!(output.column(0).is_null(1));
}

#[test]
fn actual_recursive_writer_rejects_unsupported_children_even_empty_or_null() {
    let unsupported = [
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 1),
        DataType::ListView(Arc::new(Field::new("item", DataType::Int32, true))),
        DataType::LargeListView(Arc::new(Field::new("item", DataType::Int32, true))),
        DataType::RunEndEncoded(
            Arc::new(Field::new("runs", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Utf8, true)),
        ),
        DataType::Union(
            arrow::datatypes::UnionFields::try_new(
                [0],
                [Field::new("value", DataType::Int32, true)],
            )
            .unwrap(),
            arrow::datatypes::UnionMode::Sparse,
        ),
    ];
    for ty in unsupported {
        for rows in [0, 1] {
            let child = new_null_array(&ty, rows);
            let array = StructArray::new(
                vec![Arc::new(Field::new("unsupported", ty.clone(), true))].into(),
                vec![child],
                Some(NullBuffer::new_null(rows)),
            );
            let original = pool(Arc::new(array));
            let c = Control::good(CompilePhase::Encode);
            assert!(
                matches!(
                    encode_recursive_pool(&original, invoice(&original), limits(), &c),
                    Err(TypeCodecError::InvalidShape(_))
                ),
                "{ty:?}"
            );
            assert!(c.trace().len() >= 2);
        }
    }
}

#[test]
fn actual_recursive_writer_combined_caps_accept_exact_and_refuse_one_over() {
    let original = list_fixture(3);
    let source = invoice(&original);
    let facts = preflight_recursive_pool_write(
        &original,
        source,
        limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    let mut exact = limits();
    exact.max_field_nodes = facts.field_nodes;
    exact.max_total_rows = facts.total_rows;
    exact.flat.max_rows = facts.flat.rows;
    exact.flat.max_buffer_descriptors = facts.flat.buffer_descriptors;
    exact.flat.max_body_bytes = facts.flat.body_bytes;
    exact.flat.max_encoded_stream_bytes = facts.flat.encoded_stream_bytes_upper_bound;
    exact.flat.max_new_allocation_request_bytes =
        facts.flat.new_allocation_request_bytes_upper_bound;
    exact.flat.max_coexisting_source_and_request_bytes =
        facts.flat.coexisting_source_and_request_bytes_upper_bound;
    exact.flat.max_cumulative_library_work = facts.flat.cumulative_library_work_upper_bound;
    assert!(
        encode_recursive_pool(
            &original,
            source,
            exact,
            &Control::good(CompilePhase::Encode)
        )
        .is_ok()
    );
    for slot in 0..9 {
        let mut small = exact;
        match slot {
            0 => small.max_field_nodes -= 1,
            1 => small.max_total_rows -= 1,
            2 => small.flat.max_rows -= 1,
            3 => small.flat.max_buffer_descriptors -= 1,
            4 => small.flat.max_body_bytes -= 1,
            5 => small.flat.max_encoded_stream_bytes -= 1,
            6 => small.flat.max_new_allocation_request_bytes -= 1,
            7 => small.flat.max_coexisting_source_and_request_bytes -= 1,
            _ => small.flat.max_cumulative_library_work -= 1,
        };
        assert!(matches!(
            encode_recursive_pool(
                &original,
                source,
                small,
                &Control::good(CompilePhase::Encode)
            ),
            Err(TypeCodecError::InvalidShape(_))
        ));
    }
}

fn assert_prefixes(
    pool: &ConstantPool,
    bounds: RecursivePoolWriteLimits,
    trace: &[(CompilePhase, u32)],
    positions: impl IntoIterator<Item = usize> + Clone,
) {
    for cause in CAUSES {
        for at in positions.clone() {
            let c = Control::refusing(at, cause.clone());
            assert!(
                matches!(encode_recursive_pool(pool,invoice(pool),bounds,&c),Err(TypeCodecError::Control(actual)) if actual==cause)
            );
            assert_eq!(c.trace(), trace[..=at]);
        }
    }
}
#[test]
fn actual_recursive_writer_original_controls_stop_at_entry_quantum_and_publication() {
    let small = list_fixture(2);
    let c = Control::good(CompilePhase::Encode);
    encode_recursive_pool(&small, invoice(&small), limits(), &c).unwrap();
    let trace = c.trace();
    assert_prefixes(&small, limits(), &trace, 0..trace.len());
    let mut bad = limits();
    bad.max_total_rows = 1;
    let c = Control::good(CompilePhase::Encode);
    assert!(matches!(
        encode_recursive_pool(&small, invoice(&small), bad, &c),
        Err(TypeCodecError::InvalidShape(_))
    ));
    let trace = c.trace();
    assert_prefixes(&small, bad, &trace, 0..trace.len());
    // Exercise 320 real parent offsets without inflating the checked pool's
    // complete-child logical-elements bound by a wide child allocation.
    let mut offsets = vec![0; 321];
    offsets[320] = 1;
    let wide = pool(Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, false)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(Int32Array::from(vec![71])),
        None,
    )));
    let c = Control::good(CompilePhase::Encode);
    encode_recursive_pool(&wide, invoice(&wide), limits(), &c).unwrap();
    let trace = c.trace();
    let mut positions = vec![0, trace.len() - 1];
    positions.extend(
        trace
            .iter()
            .enumerate()
            .filter_map(|(i, (_, units))| (*units == 256).then_some(i)),
    );
    assert!(positions.len() > 2);
    positions.sort_unstable();
    positions.dedup();
    assert_prefixes(&wide, limits(), &trace, positions);
}

#[path = "owned_tests.rs"]
mod owned_tests;

#[test]
fn pure_recursive_writer_without_invoice_matches_receiving_stream() {
    let item = Arc::new(Field::new("item", DataType::Int32, true));
    let array: ArrayRef = Arc::new(ListArray::new(
        item,
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 2, 2, 3])),
        Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
        Some(NullBuffer::from(vec![true, false, true])),
    ));
    let field = Arc::new(Field::new("source", array.data_type().clone(), true));
    let ty = FunctionValueType::new(array.data_type().clone(), true);
    let pool = ConstantPool::try_new(
        field,
        ty,
        array.to_data(),
        policy(),
        CompilePhase::Decode,
        &Control::good(CompilePhase::Decode),
    )
    .unwrap();
    assert!(pool.source_retained_bytes().is_none());
    let control = Control::good(CompilePhase::Encode);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
    let pure = prepare_recursive_pool_write_pure_in(&pool, &mut work)
        .unwrap()
        .emit_in(&mut work)
        .unwrap();
    work.finish().unwrap();
    let receiving = encode_recursive_pool(
        &pool,
        invoice(&pool),
        limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    assert_eq!(pure, receiving);
    let mut reader = StreamReader::try_new(Cursor::new(pure), None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.column(0).as_ref(), pool.array().as_ref());
}
