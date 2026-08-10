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
//! Spill-friendly chunks sorter wrapper.
//!
//! This wrapper provides a small helper surface for incremental candidate merge
//! during spill restore.

use crate::exec::chunk::Chunk;
use crate::exec::operators::sort::ChunksSorter;

/// Wrapper around a sorter used by spill restore merge.
pub(crate) struct SpillableChunksSorter {
    inner: Box<dyn ChunksSorter>,
}

impl SpillableChunksSorter {
    pub(crate) fn new(inner: Box<dyn ChunksSorter>) -> Self {
        Self { inner }
    }

    pub(crate) fn sort_chunks(&self, chunks: &[Chunk]) -> Result<Option<Chunk>, String> {
        self.inner.sort_chunks(chunks)
    }

    pub(crate) fn merge_candidate(
        &self,
        candidate: Option<Chunk>,
        chunk: Chunk,
    ) -> Result<Option<Chunk>, String> {
        let mut inputs = Vec::with_capacity(2);
        if let Some(prev) = candidate {
            inputs.push(prev);
        }
        inputs.push(chunk);
        self.sort_chunks(&inputs)
    }
}
