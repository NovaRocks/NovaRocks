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

//! Literal column backing held by a program, before any task memory charge.

use std::fmt;
use std::sync::Arc;

use arrow_array::RecordBatch;

use crate::StaticLayout;

/// The frozen descriptor is capped at 16 MiB by task-codec. Values have an
/// independent cap because Arrow materialization can expand the wire input.
pub const MAX_STATIC_VALUES_BACKING_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct StaticValues {
    batch: Arc<RecordBatch>,
    layout: StaticLayout,
    retained_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticValuesError {
    SchemaMismatch,
    TooManyBytes,
}

impl fmt::Display for StaticValuesError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid static values backing: {self:?}")
    }
}

impl std::error::Error for StaticValuesError {}

impl StaticValues {
    pub fn try_new(batch: RecordBatch, layout: StaticLayout) -> Result<Self, StaticValuesError> {
        if batch.schema().as_ref() != layout.schema().as_ref() {
            return Err(StaticValuesError::SchemaMismatch);
        }
        let retained_bytes = batch.get_array_memory_size();
        if retained_bytes > MAX_STATIC_VALUES_BACKING_BYTES {
            return Err(StaticValuesError::TooManyBytes);
        }
        Ok(Self {
            batch: Arc::new(batch),
            layout,
            retained_bytes,
        })
    }

    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    pub fn layout(&self) -> &StaticLayout {
        &self.layout
    }

    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};
    use novarocks_types::SlotId;

    #[test]
    fn static_values_clone_shares_arrow_buffers_without_runtime_charge() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let layout = StaticLayout::try_new(schema, Arc::from([SlotId::new(7)])).unwrap();
        let values = StaticValues::try_new(batch, layout).unwrap();
        let cloned = values.clone();
        assert!(Arc::ptr_eq(&values.batch, &cloned.batch));
        assert_eq!(values.retained_bytes(), cloned.retained_bytes());
    }

    #[test]
    fn static_values_rejects_layout_mismatch() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let layout = StaticLayout::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)])),
            Arc::from([SlotId::new(7)]),
        )
        .unwrap();
        assert!(matches!(
            StaticValues::try_new(batch, layout),
            Err(StaticValuesError::SchemaMismatch)
        ));
    }
}
