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

mod chunk_impl;
mod compiled_layout_metadata_request;
mod hydrate;
mod memory;
mod root_array_storage;
mod root_chunk_storage;
mod root_schema_backing;
mod schema;
mod slot_layout;
#[cfg(test)]
mod tests;
pub mod type_compatibility;

pub use chunk_impl::Chunk;
pub use compiled_layout_metadata_request::original_compiled_schema_metadata_request;
pub use hydrate::hydrate_dictionary_columns_except;
pub use memory::record_batch_bytes;
pub(crate) use memory::{
    ChunkMemoryLease, TransferredChunkBytes, record_batch_additional_bytes,
    record_batch_shared_owner_bytes,
};
pub use root_array_storage::{
    ARROW_BUFFER_OWNER_METADATA_BOUND, RootArrayStorageError, RootArrayStorageLimits,
    borrowed_root_array_storage, borrowed_root_batch_storage,
};
pub use root_chunk_storage::{borrowed_root_chunk_schema_storage, borrowed_root_chunk_storage};
pub use schema::{ChunkFieldSchema, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
pub use slot_layout::SlotLayout;
