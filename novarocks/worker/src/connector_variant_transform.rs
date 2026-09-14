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

//! Driver-local materialization of connector VARIANT path outputs.

use novarocks_execution::exec::chunk::ChunkSchema;
use novarocks_execution::exec::variant_read::{
    ParquetSlotKind, VariantPathSpec, convert_variant_columns, materialize_variant_path_columns,
};
use novarocks_types::SlotId;

use crate::connector_batch_transform::ConnectorBatchTransform;

/// Turn one connector scan's physical read columns into its declared output.
#[derive(Clone)]
pub struct ConnectorVariantPathTransform {
    read_slot_ids: Vec<SlotId>,
    output_slot_ids: Vec<SlotId>,
    output_fields: Vec<arrow::datatypes::Field>,
    specs: Vec<VariantPathSpec>,
    output_slot_kinds: Vec<ParquetSlotKind>,
}

impl ConnectorVariantPathTransform {
    /// Native decoding supplies immutable layouts; Worker owns batch execution.
    pub fn new(
        read_slot_ids: Vec<SlotId>,
        output_schema: &ChunkSchema,
        specs: Vec<VariantPathSpec>,
    ) -> Self {
        let output_slot_kinds = output_schema
            .slot_ids()
            .iter()
            .map(|slot_id| {
                if specs.iter().any(|spec| spec.source_slot_id == *slot_id) {
                    ParquetSlotKind::Variant
                } else {
                    ParquetSlotKind::Regular
                }
            })
            .collect();
        Self {
            read_slot_ids,
            output_slot_ids: output_schema.slot_ids().to_vec(),
            output_fields: output_schema
                .slots()
                .iter()
                .map(|slot| slot.field().clone())
                .collect(),
            specs,
            output_slot_kinds,
        }
    }

    fn apply(
        &self,
        batch: arrow::record_batch::RecordBatch,
    ) -> Result<arrow::record_batch::RecordBatch, String> {
        let batch = materialize_variant_path_columns(
            batch,
            &self.read_slot_ids,
            &self.output_slot_ids,
            &self.specs,
        )
        .and_then(|batch| convert_variant_columns(&self.output_slot_kinds, batch))?;
        arrow::record_batch::RecordBatch::try_new(
            std::sync::Arc::new(arrow::datatypes::Schema::new(self.output_fields.clone())),
            batch.columns().to_vec(),
        )
        .map_err(|error| format!("restore variant scan output schema failed: {error}"))
    }
}

impl ConnectorBatchTransform for ConnectorVariantPathTransform {
    fn transform(
        &self,
        batch: arrow::record_batch::RecordBatch,
    ) -> Result<arrow::record_batch::RecordBatch, String> {
        self.apply(batch)
    }
}
