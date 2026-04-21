//! Generic query result types for standalone SQL execution.
//!
//! These types live here (rather than in `crate::standalone`) so that
//! executors and coordinators under `crate::runtime` can reference the
//! result type without creating a dependency on the standalone module.

use arrow::datatypes::DataType;

use crate::exec::chunk::Chunk;

#[derive(Clone, Debug)]
pub struct QueryResultColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub logical_type: Option<crate::sql::SqlType>,
}

#[derive(Clone, Debug)]
pub struct QueryResult {
    pub columns: Vec<QueryResultColumn>,
    pub chunks: Vec<Chunk>,
}

impl QueryResult {
    pub fn row_count(&self) -> usize {
        self.chunks.iter().map(Chunk::len).sum()
    }

    pub fn into_chunks(self) -> Vec<Chunk> {
        self.chunks
    }
}
