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

//! Immutable batch sequence with creator-owned headers and no SQL VALUES quota.
use crate::StaticLayout;
use arrow_array::RecordBatch;
use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl, ValueSchemaCreation,
    ValueTypeStorageLifetime,
};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct StaticBatchSequence {
    batches: Arc<[RecordBatch]>,
    layout: StaticLayout,
    retained_bytes: usize,
    rows: usize,
    storage: ValueTypeStorageLifetime,
}
#[derive(Debug)]
pub enum BatchSequenceCreationFailure<H> {
    Invalid(&'static str),
    Control(CompileControlError),
    Creator(H),
    Resource(LayoutResourceError),
}
impl<H> From<CompileControlError> for BatchSequenceCreationFailure<H> {
    fn from(e: CompileControlError) -> Self {
        Self::Control(e)
    }
}
impl<H> From<LayoutResourceError> for BatchSequenceCreationFailure<H> {
    fn from(e: LayoutResourceError) -> Self {
        Self::Resource(e)
    }
}
impl StaticBatchSequence {
    pub fn try_new_with_creator<H>(
        batches: Arc<[RecordBatch]>,
        layout: StaticLayout,
        source: ValueTypeStorageLifetime,
        creation: &mut ValueSchemaCreation<'_, H>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, BatchSequenceCreationFailure<H>> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        let mut retained_bytes = 0usize;
        let mut rows = 0usize;
        for batch in batches.iter() {
            work.flush()?;
            if batch.schema().as_ref() != layout.schema().as_ref() || batch.num_rows() == 0 {
                return Err(BatchSequenceCreationFailure::Invalid(
                    "system relation batch differs from its frozen layout or is empty",
                ));
            }
            retained_bytes = retained_bytes
                .checked_add(batch.get_array_memory_size())
                .ok_or(CompileControlError::ResourceExhausted)?;
            rows = rows
                .checked_add(batch.num_rows())
                .ok_or(CompileControlError::ResourceExhausted)?;
            work.step()?;
        }
        let original_storage = creation.storage_lifetime();
        let mut request = |layout| {
            work.flush()?;
            creation
                .request_layout(layout)
                .map_err(BatchSequenceCreationFailure::Creator)
        };
        let storage = original_storage
            .retaining_observed::<BatchSequenceCreationFailure<H>>(&source, &mut request)?;
        let storage = storage.retaining_observed::<BatchSequenceCreationFailure<H>>(
            &layout.storage_lifetime(),
            &mut request,
        )?;
        // The sequence borrows the already-created Arc backing; no whole-table copy is requested.
        work.finish()?;
        Ok(Self {
            batches,
            layout,
            retained_bytes,
            rows,
            storage,
        })
    }
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }
    pub fn layout(&self) -> &StaticLayout {
        &self.layout
    }
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
    pub fn rows(&self) -> usize {
        self.rows
    }
    pub fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.storage.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, RecordBatchOptions, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use novarocks_types::SlotId;
    struct Control;
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            Ok(())
        }
    }
    #[test]
    fn large_snapshot_keeps_original_batches_and_creator_layout_receipts() {
        let text = "x".repeat(16 * 1024 * 1024 + 1);
        let array = Arc::new(StringArray::from(vec![text.as_str()]));
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array.clone() as ArrayRef]).unwrap();
        let mut requests = Vec::new();
        let mut request = |layout: std::alloc::Layout| {
            requests.push(layout);
            Ok::<_, std::convert::Infallible>(())
        };
        let mut opaque = |_: usize| Ok::<_, std::convert::Infallible>(());
        let mut creator =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut opaque);
        let ty = novarocks_type_contract::FunctionValueType::new(DataType::Utf8, false);
        let layout = StaticLayout::try_new_typed_with_creator(
            schema,
            Arc::from([SlotId::new(0)]),
            vec![(ty.typed_binding().clone(), None)],
            ValueTypeStorageLifetime::pure(),
            |l| {
                creator
                    .request_layout(l)
                    .map_err(crate::LayoutCreationFailure::Creator)
            },
            &Control,
        )
        .unwrap();
        let sequence = StaticBatchSequence::try_new_with_creator(
            Arc::from([batch]),
            layout,
            ValueTypeStorageLifetime::pure(),
            &mut creator,
            &Control,
        )
        .unwrap();
        assert!(sequence.retained_bytes() > 16 * 1024 * 1024);
        assert_eq!(sequence.rows(), 1);
        assert_eq!(sequence.batches().len(), 1);
        assert!(Arc::ptr_eq(
            &sequence.batches()[0].columns()[0],
            &(array as ArrayRef)
        ));
        drop(creator);
        assert!(!requests.is_empty());
    }
    #[test]
    fn zero_column_batches_keep_nonzero_row_count() {
        let schema = Arc::new(Schema::empty());
        let layout = StaticLayout::try_new(schema.clone(), Arc::from([])).unwrap();
        let batch = RecordBatch::try_new_with_options(
            schema,
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(5001)),
        )
        .unwrap();
        let mut request = |_: std::alloc::Layout| Ok::<_, std::convert::Infallible>(());
        let mut opaque = |_: usize| Ok::<_, std::convert::Infallible>(());
        let mut creator =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut opaque);
        let sequence = StaticBatchSequence::try_new_with_creator(
            Arc::from([batch]),
            layout,
            ValueTypeStorageLifetime::pure(),
            &mut creator,
            &Control,
        )
        .unwrap();
        assert_eq!(sequence.rows(), 5001);
        assert_eq!(sequence.batches()[0].num_rows(), 5001);
    }
}
