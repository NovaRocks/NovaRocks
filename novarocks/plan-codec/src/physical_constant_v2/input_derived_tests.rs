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
use arrow::datatypes::{DataType, Field};
use novarocks_type_contract::owned_resources::{
    layout::arc_layout,
    metadata_materialization::{MaterializedFieldNamespace, MetadataFieldLoan},
};
use novarocks_type_contract::{
    TypedValueBinding, ValueKind, ValueLayout, ValueSchemaCreation, ValueTypeRef,
    ValueTypeStorageLifetime,
};
use std::{
    alloc::Layout,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

struct Control(AtomicBool);
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        assert!(
            !self.0.load(Ordering::SeqCst),
            "callback after original host refusal"
        );
        Ok(())
    }
}
struct NullSource {
    record: wire::IpcConstantPool,
    ty: FunctionValueType,
    field: Arc<Field>,
    metadata: MaterializedFieldNamespace,
    invoice: usize,
}
fn null_source(rows: i64) -> NullSource {
    let semantic_request = ValueTypeRef::storage_allocation_layout().unwrap();
    let semantic = ValueTypeRef::try_new(
        ValueKind::Scalar(DataType::Null),
        true,
        ValueTypeStorageLifetime::pure(),
    )
    .unwrap();
    let layout_request = ValueLayout::storage_allocation_layout().unwrap();
    let layout = ValueLayout::try_new(&DataType::Null, ValueTypeStorageLifetime::pure()).unwrap();
    let ty = FunctionValueType::try_from_typed_binding(
        TypedValueBinding::try_new(semantic, layout).unwrap(),
    )
    .unwrap();
    let field_request = arc_layout(Layout::new::<Field>()).unwrap();
    let field = Arc::new(Field::new("source", DataType::Null, true));
    let namespace_request = arc_layout(Layout::array::<MetadataFieldLoan>(0).unwrap()).unwrap();
    let metadata = MaterializedFieldNamespace::from_original_loans(Arc::from([]));
    // A safe official FlatBuffer writer authors the actual batch header. NULL
    // rows have no payload buffers; no byte-derived row estimate is involved.
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let nodes = builder.create_vector(&[arrow::ipc::FieldNode::new(rows, rows)]);
    let buffers = builder.create_vector::<arrow::ipc::Buffer>(&[]);
    let batch = arrow::ipc::RecordBatch::create(
        &mut builder,
        &arrow::ipc::RecordBatchArgs {
            length: rows,
            nodes: Some(nodes),
            buffers: Some(buffers),
            compression: None,
            variadicBufferCounts: None,
        },
    );
    let message = arrow::ipc::Message::create(
        &mut builder,
        &arrow::ipc::MessageArgs {
            version: arrow::ipc::MetadataVersion::V5,
            header_type: arrow::ipc::MessageHeader::RecordBatch,
            header: Some(batch.as_union_value()),
            bodyLength: 0,
            custom_metadata: None,
        },
    );
    arrow::ipc::finish_message_buffer(&mut builder, message);
    let bytes = builder.finished_data();
    let padded = bytes.len().next_multiple_of(8);
    let mut ipc = vec![0xff; 4];
    ipc.extend_from_slice(&i32::try_from(padded).unwrap().to_le_bytes());
    ipc.extend_from_slice(bytes);
    ipc.resize(8 + padded, 0);
    let record = wire::IpcConstantPool {
        id: 0,
        value_type_id: Some(0),
        source_field_id: Some(0),
        compression: wire::IpcCompression::Uncompressed as i32,
        arrow_ipc: ipc,
    };
    // The actual fixture author records every retained holder/backing above.
    // No decoded floor or generous source envelope substitutes for this invoice.
    let invoice = record.arrow_ipc.capacity()
        + std::mem::size_of_val(&record)
        + field_request.size()
        + field.name().capacity()
        + semantic_request.size()
        + layout_request.size()
        + namespace_request.size()
        + std::mem::size_of_val(&ty)
        + std::mem::size_of_val(&metadata);
    NullSource {
        record,
        ty,
        field,
        metadata,
        invoice,
    }
}

#[test]
fn input_derived_reader_uses_real_null_header_rows_and_decodes_the_same_small_values() {
    for rows in [3i64, 1 << 40] {
        let source = null_source(rows);
        let control = Control(AtomicBool::new(false));
        let mut exact = |_: Layout| Ok::<(), Arc<&str>>(());
        let mut opaque = |_: usize| Ok::<(), Arc<&str>>(());
        let mut creation =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let prepared = prepare_constant_record_from_sources_input_derived_with_creator_in(
            &source.record,
            &source.ty,
            &source.field,
            ValueTypeStorageLifetime::pure(),
            &source.metadata,
            source.invoice,
            source.invoice,
            &mut creation,
            &mut work,
        )
        .unwrap();
        assert!(prepared.reader_request_upper_bounds().2 >= rows as usize);
        if rows == 3 {
            let pool = prepared.materialize_in(&mut creation, &mut work).unwrap();
            assert_eq!(pool.resource_facts().rows, 3);
            assert_eq!(pool.resource_facts().retained_buffer_capacity_bytes, 0);
        }
        work.finish().unwrap();
    }
}

#[test]
fn input_derived_reader_retains_first_actual_construction_refusal_without_tail() {
    let source = null_source(3);
    let control = Control(AtomicBool::new(false));
    let cause = Arc::new("original tracker refusal");
    let mut exact = |_: Layout| {
        assert!(!control.0.swap(true, Ordering::SeqCst));
        Err(cause.clone())
    };
    let mut opaque = |_: usize| Ok::<(), Arc<&str>>(());
    let mut creation =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let result = prepare_constant_record_from_sources_input_derived_with_creator_in(
        &source.record,
        &source.ty,
        &source.field,
        ValueTypeStorageLifetime::pure(),
        &source.metadata,
        source.invoice,
        source.invoice,
        &mut creation,
        &mut work,
    );
    match result {
        Err(ProjectionFailure::Host(actual)) => assert!(Arc::ptr_eq(&actual, &cause)),
        _ => panic!("first construction refusal was not preserved"),
    }
}
