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

//! Fragment dispatcher port and native submission DTO.

use crate::common::types::UniqueId;
use novarocks_execution::exec::chunk::{Chunk, ChunkSchemaRef};

/// Opaque data-plane batch returned by a fragment dispatcher.
///
/// The execution-layer `Chunk` remains owned by core. Role crates may route
/// this value through the query-execution contract but cannot inspect or
/// manufacture execution batches.
pub struct FetchedQueryBatch {
    chunk: Chunk,
}

impl FetchedQueryBatch {
    pub(crate) fn new(chunk: Chunk) -> Self {
        Self { chunk }
    }

    pub(crate) fn into_chunk(self) -> Chunk {
        self.chunk
    }
}

/// Borrowed opaque view of the root fetch schema.
#[derive(Clone, Copy)]
pub struct ExpectedOutputSchemaView<'a> {
    schema: &'a ChunkSchemaRef,
}

impl<'a> ExpectedOutputSchemaView<'a> {
    pub(crate) const fn new(schema: &'a ChunkSchemaRef) -> Self {
        Self { schema }
    }

    pub(crate) const fn chunk_schema(self) -> &'a ChunkSchemaRef {
        self.schema
    }
}

/// Decode one typed root-result payload into the opaque dispatcher value.
///
/// Native transports live in role crates, while Core retains the execution
/// batch representation and the canonical wire-to-chunk conversion.  This
/// keeps that conversion available without exposing `Chunk` construction to a
/// transport owner.
pub fn decode_fetched_query_batch(
    payload: &[u8],
    expected_output_schema: Option<ExpectedOutputSchemaView<'_>>,
) -> Result<FetchedQueryBatch, String> {
    let mut chunks = novarocks_execution::runtime::exchange::decode_root_result_chunks(
        payload,
        expected_output_schema.map(|view| view.chunk_schema()),
    )?;
    if chunks.len() != 1 {
        return Err(format!(
            "typed root result decoded {} chunks, expected 1",
            chunks.len()
        ));
    }
    Ok(FetchedQueryBatch::new(chunks.remove(0)))
}

/// Outcome of a single `fetch_result` call.
pub enum FetchOutcome {
    /// A result batch is available.
    Ready(FetchedQueryBatch),
    /// No chunk available yet; fragment is still running.
    NotReady,
    /// All chunks have been delivered; the root fragment is complete.
    Eof,
    /// Fragment execution failed.
    Err(String),
}

/// Result transport for an already-running native query.
///
/// Query startup belongs exclusively to the query lifecycle Stage/Start
/// barrier. Query lifecycle owns cancellation and terminal convergence after
/// that barrier has entered `Running`.
pub trait FragmentDispatcher: Send + Sync + 'static {
    /// Poll for the next result chunk from the root fragment on the given backend.
    fn fetch_result(
        &self,
        backend_idx: usize,
        finst_id: UniqueId,
        max_wait_ms: i64,
        expected_output_schema: Option<ExpectedOutputSchemaView<'_>>,
    ) -> Result<FetchOutcome, String>;

    /// Number of backends this dispatcher can route to.
    fn backend_count(&self) -> usize;
}

#[cfg(test)]
mod tests {
    use super::decode_fetched_query_batch;

    #[test]
    fn opaque_fetch_decode_requires_exactly_one_chunk() {
        let Err(error) = decode_fetched_query_batch(&[], None) else {
            panic!("empty payload is not a batch");
        };
        assert_eq!(error, "typed root result decoded 0 chunks, expected 1");
    }
}
