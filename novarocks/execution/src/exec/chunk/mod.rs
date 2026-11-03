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

mod additional_buffers;
mod chunk_impl;
mod compiled_layout_metadata_request;
mod construction_failure;
mod filtered_output;
mod hydrate;
mod memory;
mod output_metadata;
mod required_dictionary_nulls;
mod root_array_storage;
mod root_chunk_storage;
mod root_schema_backing;
mod schema;
pub use construction_failure::{ChunkConstructionFailure, ChunkSchemaViolation};
mod slot_layout;
mod source_authority;
#[cfg(test)]
pub(crate) mod source_authority_tests;
#[cfg(test)]
mod tests;
pub mod type_compatibility;
mod typed_array_normalization;
#[cfg(test)]
pub(crate) mod typed_fixture;

pub(crate) use additional_buffers::AdditionalBufferLedger;
pub use chunk_impl::{Chunk, ChunkAccessFailure, ChunkBatchProjection};
pub use compiled_layout_metadata_request::{
    CompiledSchemaRequestFailure, original_compiled_schema_metadata_request,
};
pub(crate) use filtered_output::normalize_filter_mask_observed;
pub use hydrate::dictionary_hydration_required_observed;
pub use hydrate::hydrate_dictionary_columns_owned_observed;
pub use memory::record_batch_bytes;
pub(crate) use memory::{
    ChunkMemoryCharge, TransferredChunkBytes, record_batch_additional_bytes,
    record_batch_additional_bytes_observed, record_batch_shared_owner_bytes,
};
pub(crate) use output_metadata::OutputColumnMetadata;
pub(crate) use output_metadata::retain_column_capacity;
pub use root_array_storage::{
    ARROW_BUFFER_OWNER_METADATA_BOUND, RootArrayStorageError, RootArrayStorageLimits,
    borrowed_root_array_storage, borrowed_root_batch_storage,
};
pub use root_chunk_storage::{borrowed_root_chunk_schema_storage, borrowed_root_chunk_storage};
pub use schema::{ChunkFieldSchema, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
pub use slot_layout::SlotLayout;
pub(crate) use source_authority::ChunkSourceAuthorityRef;
pub use typed_array_normalization::{
    ArrayNormalizationStep, ArrayPathStep, normalize_execution_array,
    normalize_execution_array_observed, normalize_execution_field_observed,
};

pub(crate) use schema::validate_chunk_carrier_encoding_observed;

pub(crate) use hydrate::contains_dictionary;
