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

use super::SystemTableError;
use arrow::datatypes::Schema;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::sync::Arc;

/// Ordered batches retain their original backing and explicit row counts.
/// Empty projections are valid and must not erase cardinality for COUNT(*).
#[derive(Clone, Debug)]
pub struct SystemTableSnapshot {
    pub batches: Arc<[RecordBatch]>,
    pub storage: ValueTypeStorageLifetime,
}

impl SystemTableSnapshot {
    pub fn try_new(
        batches: impl Into<Arc<[RecordBatch]>>,
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, SystemTableError> {
        let batches = batches.into();
        if batches.iter().any(|batch| batch.num_rows() == 0) {
            return Err(SystemTableError::Internal(
                "zero-row snapshots must contain no batches".into(),
            ));
        }
        if !batches.is_empty() && storage.is_pure() {
            return Err(SystemTableError::Internal(
                "system-table snapshot is missing its original storage lifetime".into(),
            ));
        }
        Ok(Self { batches, storage })
    }

    pub fn empty_projection_batch(row_count: usize) -> Result<RecordBatch, SystemTableError> {
        RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(row_count)),
        )
        .map_err(|error| {
            SystemTableError::Internal(format!("build zero-column system table batch: {error}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn empty_projection_preserves_nonempty_and_empty_cardinality() {
        for rows in [0, 1, 4096, 65536] {
            let batch = SystemTableSnapshot::empty_projection_batch(rows).unwrap();
            assert_eq!(batch.num_columns(), 0);
            assert_eq!(batch.num_rows(), rows);
        }
    }
    #[test]
    fn snapshot_requires_storage_and_uses_no_batches_for_zero_rows() {
        struct Hold;
        impl novarocks_type_contract::ValueTypeStorageHold for Hold {}
        let storage = ValueTypeStorageLifetime::held(Arc::new(Hold));
        assert!(
            SystemTableSnapshot::try_new(vec![], ValueTypeStorageLifetime::pure())
                .unwrap()
                .batches
                .is_empty()
        );
        assert!(
            SystemTableSnapshot::try_new(
                vec![SystemTableSnapshot::empty_projection_batch(0).unwrap()],
                storage.clone()
            )
            .is_err()
        );
        assert!(
            SystemTableSnapshot::try_new(
                vec![SystemTableSnapshot::empty_projection_batch(1).unwrap()],
                ValueTypeStorageLifetime::pure()
            )
            .is_err()
        );
        let snapshot = SystemTableSnapshot::try_new(
            vec![SystemTableSnapshot::empty_projection_batch(1).unwrap()],
            storage,
        )
        .unwrap();
        assert_eq!(snapshot.batches[0].num_rows(), 1);
        assert!(!snapshot.storage.is_pure());
    }
}
