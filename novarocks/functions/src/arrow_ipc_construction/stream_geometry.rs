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

//! Function runtime adapters for the neutral Arrow construction geometry.

use crate::kernel_input::EvaluationCheckpoints;
use crate::{KernelEvaluationControl, KernelFailure};
use arrow_array::{Array, RecordBatch};
use arrow_schema::Schema;
pub use neutral::{IpcStreamBatchFacts, IpcStreamSchemaFacts};
use novarocks_arrow_batch_codec::construction::{self as neutral, ConstructionWork};
use novarocks_type_contract::ValueSchemaCreation;
pub type IpcConstructionError<H> = neutral::IpcConstructionError<H, KernelFailure>;

struct KernelGeometryWork<'a, 'b>(&'a mut EvaluationCheckpoints<'b>);
impl ConstructionWork for KernelGeometryWork<'_, '_> {
    type Error = KernelFailure;
    fn step(&mut self) -> Result<(), Self::Error> {
        self.0.step()
    }
    fn flush(&mut self) -> Result<(), Self::Error> {
        self.0.flush()
    }
    fn source_metadata_bytes(&mut self, source: &dyn Array) -> Result<usize, Self::Error> {
        crate::array_backing_geometry::source_metadata_bytes(source, self.0)
    }
    fn source_metadata_and_slots(
        &mut self,
        source: &dyn Array,
    ) -> Result<(usize, usize), Self::Error> {
        let mut observer = SourceBufferSlots::default();
        let bytes = crate::array_backing_geometry::observe_source_metadata(
            source,
            false,
            self.0,
            &mut observer,
        )?;
        Ok((bytes, observer.slots))
    }
}
#[derive(Default)]
struct SourceBufferSlots {
    slots: usize,
}
impl<'a> crate::array_backing_geometry::BorrowedSourceObservation<'a> for SourceBufferSlots {
    fn node(
        &mut self,
        node: crate::array_backing_geometry::BorrowedSourceNode<'a>,
        _work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        // Null contributes zero data slots and no invented validity. Other
        // actual nodes contribute their real buffers plus an existing mask.
        self.slots = self
            .slots
            .checked_add(node.data_buffers)
            .and_then(|slots| slots.checked_add(usize::from(node.array.nulls().is_some())))
            .ok_or(KernelFailure::ResourceExhausted)?;
        Ok(())
    }
}

pub fn observe_stream_writer_schema_construction<'a, H>(
    schema: &'a Schema,
    creation: &mut ValueSchemaCreation<'_, H>,
    control: &dyn KernelEvaluationControl,
) -> Result<IpcStreamSchemaFacts<'a>, IpcConstructionError<H>> {
    control
        .checkpoint(0)
        .map_err(IpcConstructionError::Control)?;
    let mut checkpoints = EvaluationCheckpoints::new(control);
    neutral::observe_stream_writer_schema_construction(
        schema,
        creation,
        &mut KernelGeometryWork(&mut checkpoints),
    )
}

pub fn observe_stream_writer_batch_construction<'a, H>(
    batch: &'a RecordBatch,
    schema: &IpcStreamSchemaFacts<'_>,
    creation: &mut ValueSchemaCreation<'_, H>,
    control: &dyn KernelEvaluationControl,
) -> Result<IpcStreamBatchFacts<'a>, IpcConstructionError<H>> {
    control
        .checkpoint(0)
        .map_err(IpcConstructionError::Control)?;
    let mut checkpoints = EvaluationCheckpoints::new(control);
    neutral::observe_stream_writer_batch_construction(
        batch,
        schema,
        creation,
        &mut KernelGeometryWork(&mut checkpoints),
    )
}

pub fn observe_stream_logical_size_construction<'a, H>(
    batch: &'a RecordBatch,
    facts: &IpcStreamBatchFacts<'_>,
    creation: &mut ValueSchemaCreation<'_, H>,
    control: &dyn KernelEvaluationControl,
) -> Result<(), IpcConstructionError<H>> {
    control
        .checkpoint(0)
        .map_err(IpcConstructionError::Control)?;
    let mut checkpoints = EvaluationCheckpoints::new(control);
    neutral::observe_stream_logical_size_construction(
        batch,
        facts,
        creation,
        &mut KernelGeometryWork(&mut checkpoints),
    )
}

/// Runtime adapter to the neutral schema-free batch operation model.
pub fn observe_unit_batch_construction<'a, H>(
    batch: &'a RecordBatch,
    creation: &mut ValueSchemaCreation<'_, H>,
    control: &dyn KernelEvaluationControl,
) -> Result<IpcStreamBatchFacts<'a>, IpcConstructionError<H>> {
    control
        .checkpoint(0)
        .map_err(IpcConstructionError::Control)?;
    let mut checkpoints = EvaluationCheckpoints::new(control);
    neutral::observe_unit_batch_construction(
        batch,
        creation,
        &mut KernelGeometryWork(&mut checkpoints),
    )
}

#[cfg(test)]
use arrow_array::types::Int32Type;
#[cfg(test)]
use arrow_array::{
    LargeListViewArray, ListViewArray, RunArray, StringArray, StringViewArray, UnionArray,
};
#[cfg(test)]
use arrow_schema::{DataType, Field};
#[cfg(test)]
mod retained_carrier_tests {
    use super::*;
    use arrow_array::{ArrayRef, Int32Array};
    use arrow_buffer::ScalarBuffer;
    use arrow_schema::UnionFields;
    use novarocks_type_contract::ValueTypeStorageLifetime;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    struct Control(AtomicBool);
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            if self.0.load(Ordering::SeqCst) {
                Err(KernelFailure::Cancelled)
            } else {
                Ok(())
            }
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            panic!("writer does not wait");
        }
    }
    fn sources() -> Vec<ArrayRef> {
        let fields = UnionFields::try_new(
            vec![4, 9],
            vec![
                Field::new("number", DataType::Int32, false),
                Field::new("text", DataType::Utf8, true),
            ],
        )
        .unwrap();
        let dense: ArrayRef = Arc::new(
            UnionArray::try_new(
                fields.clone(),
                ScalarBuffer::from(vec![4_i8, 9, 4]),
                Some(ScalarBuffer::from(vec![0_i32, 0, 1])),
                vec![
                    Arc::new(Int32Array::from(vec![7, 8])),
                    Arc::new(StringArray::from(vec![Some("text")])),
                ],
            )
            .unwrap(),
        );
        let sparse: ArrayRef = Arc::new(
            UnionArray::try_new(
                fields,
                ScalarBuffer::from(vec![4_i8, 9, 4]),
                None,
                vec![
                    Arc::new(Int32Array::from(vec![7, 8, 9])),
                    Arc::new(StringArray::from(vec![Some("x"), Some("y"), None])),
                ],
            )
            .unwrap(),
        );
        let view: ArrayRef = Arc::new(StringViewArray::from(vec![
            "a genuinely external view value",
            "short",
        ]));
        let list_view: ArrayRef = Arc::new(
            ListViewArray::try_new(
                Arc::new(Field::new("item", DataType::Int32, false)),
                ScalarBuffer::from(vec![2_i32, 0, 1]),
                ScalarBuffer::from(vec![2_i32, 1, 2]),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
                None,
            )
            .unwrap(),
        );
        let large_view: ArrayRef = Arc::new(
            LargeListViewArray::try_new(
                Arc::new(Field::new("item", DataType::Int32, false)),
                ScalarBuffer::from(vec![2_i64, 0, 1]),
                ScalarBuffer::from(vec![2_i64, 1, 2]),
                Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
                None,
            )
            .unwrap(),
        );
        let run: ArrayRef = Arc::new(
            RunArray::<Int32Type>::try_new(
                &Int32Array::from(vec![2, 5]),
                &StringArray::from(vec!["first", "second"]),
            )
            .unwrap(),
        );
        vec![dense, sparse, view, list_view, large_view, run]
    }
    #[test]
    fn original_stream_writer_model_preserves_native_union_view_and_run_carriers() {
        for array in sources() {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                array.data_type().clone(),
                true,
            )]));
            let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
            let control = Control(AtomicBool::new(false));
            let requests = AtomicUsize::new(0);
            let mut exact = |_| {
                requests.fetch_add(1, Ordering::SeqCst);
                Ok::<_, std::convert::Infallible>(())
            };
            let mut opaque = |_| {
                requests.fetch_add(1, Ordering::SeqCst);
                Ok::<_, std::convert::Infallible>(())
            };
            let mut creation =
                ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
            let facts =
                observe_stream_writer_schema_construction(schema.as_ref(), &mut creation, &control)
                    .unwrap();
            observe_stream_writer_batch_construction(&batch, &facts, &mut creation, &control)
                .unwrap();
            assert!(requests.load(Ordering::SeqCst) > 0);
            let mut bytes = Vec::new();
            let mut writer =
                arrow_ipc::writer::StreamWriter::try_new(&mut bytes, schema.as_ref()).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
            drop(writer);
            let decoded =
                arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
                    .unwrap()
                    .next()
                    .unwrap()
                    .unwrap();
            assert_eq!(decoded.num_rows(), batch.num_rows());
            assert_eq!(
                decoded.columns()[0].data_type(),
                batch.columns()[0].data_type()
            );
            assert_eq!(decoded.columns()[0].to_data(), batch.columns()[0].to_data());
        }
    }
    #[test]
    fn original_stream_writer_batch_creator_refusal_wins_before_pending_control() {
        let array = sources().remove(0);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        let control = Control(AtomicBool::new(false));
        let mut exact = |_| Ok::<_, u32>(());
        let mut opaque = |_| Ok::<_, u32>(());
        let mut creation =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
        let facts =
            observe_stream_writer_schema_construction(schema.as_ref(), &mut creation, &control)
                .unwrap();
        let calls = AtomicUsize::new(0);
        let mut fail = |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            control.0.store(true, Ordering::SeqCst);
            Err::<(), _>(71_u32)
        };
        let mut failing =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut fail);
        assert!(matches!(
            observe_stream_writer_batch_construction(&batch, &facts, &mut failing, &control),
            Err(IpcConstructionError::Creator(71))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
