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

//! Actual construction owners for runtime expression inputs and filtered outputs.

use crate::exec::chunk::{ArrayNormalizationStep, Chunk};
use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionOwner};
use crate::runtime::fragment::ExecutionResult;
use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::DataType;
use novarocks_types::SlotId;

impl Chunk {
    pub(crate) fn hydrate_for_evaluation(
        &self,
        owner: &dyn InputConstructionOwner,
        keep_encoded: impl Fn(SlotId, &DataType) -> bool,
    ) -> ExecutionResult<Self> {
        if self.chunk_schema().is_typed() {
            let storage = self.source_storage_lifetime_observed::<ConstructionFailure>(
                owner.storage_lifetime(),
                |layout| owner.request(layout),
            )?;
            return crate::exec::chunk::hydrate_dictionary_columns_owned_observed::<
                crate::runtime::fragment::ExecutionFailure,
                _,
                _,
                _,
            >(
                self,
                storage,
                |slot, _, _, data_type| keep_encoded(slot, data_type),
                |index, path, step| Ok(owner.observe(index, path, step)?),
                |layout| Ok(owner.request(layout)?),
            );
        }
        // An untyped reference source still uses the caller's actual creator
        // for every wrapper that its hydration allocates.
        owner.observe(
            0,
            &[],
            ArrayNormalizationStep::InspectConstructionWork {
                units: self.columns().len(),
            },
        )?;
        owner.request(std::alloc::Layout::array::<ArrayRef>(self.columns().len())
            .map_err(|_| ConstructionFailure::from(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
        owner.request(std::alloc::Layout::array::<crate::exec::chunk::ChunkSlotSchema>(self.columns().len())
            .map_err(|_| ConstructionFailure::from(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
        let mut result = crate::exec::chunk::hydrate_dictionary_columns_except(self, keep_encoded)?;
        owner.observe(0, &[], ArrayNormalizationStep::OpaqueOperationFinished)?;
        result.construction_storage = owner.storage_lifetime();
        result.array_type_storage = self.source_storage_lifetime_observed::<ConstructionFailure>(
            owner.storage_lifetime(),
            |layout| owner.request(layout),
        )?;
        result.retain_source_buffer_funding(self);
        Ok(result)
    }

    pub(crate) fn filter_for_evaluation(
        &self,
        mask: &BooleanArray,
        owner: &dyn InputConstructionOwner,
    ) -> ExecutionResult<Self> {
        owner.observe(
            0,
            &[],
            ArrayNormalizationStep::InspectConstructionWork { units: mask.len() },
        )?;
        owner.request(
            std::alloc::Layout::array::<ArrayRef>(self.columns().len()).map_err(|_| {
                ConstructionFailure::from(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
            })?,
        )?;
        let batch = arrow::compute::filter_record_batch(self.record_batch(), mask)
            .map_err(|error| format!("filter expression input: {error}"))?;
        owner.observe(0, &[], ArrayNormalizationStep::OpaqueOperationFinished)?;
        let source_storage = self.source_storage_lifetime_observed::<ConstructionFailure>(
            owner.storage_lifetime(),
            |layout| owner.request(layout),
        )?;
        let mut output = if self.chunk_schema().is_typed() {
            Self::try_new_typed_record_batch_owned_observed::<ConstructionFailure, _, _>(
                batch,
                self.chunk_schema_ref(),
                source_storage,
                owner.storage_lifetime(),
                |layout| owner.request(layout),
                |step| owner.observe(0, &[], step),
            )?
        } else {
            let mut output = Self::try_new_with_chunk_schema(batch, self.chunk_schema_ref())?;
            owner.observe(0, &[], ArrayNormalizationStep::CreateRecordBatchWrapper)?;
            output.construction_storage = owner.storage_lifetime();
            output.array_type_storage = source_storage;
            output
        };
        output.retain_source_buffer_funding(self);
        Ok(output)
    }
}
