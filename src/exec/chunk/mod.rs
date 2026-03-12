mod chunk_impl;
mod memory;
mod schema;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub use crate::testutil::chunk::field_with_slot_id;
pub use chunk_impl::Chunk;
pub use memory::record_batch_bytes;
pub use schema::{ChunkFieldSchema, ChunkSchema, ChunkSchemaRef, ChunkSlotSchema};
