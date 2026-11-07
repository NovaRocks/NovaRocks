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

//! CTE (Common Table Expression) metadata types.
//!
//! The registry stores all non-recursive CTE definitions analyzed for the
//! current query. Lexical visibility is tracked separately by the analyzer.

use super::{AnalyzedOutputField as OutputColumn, ResolvedQuery};

/// Unique identifier for a CTE within a query.
pub(crate) use crate::common::CteId;

/// Accumulated registry of all non-recursive CTEs produced by the analyzer.
/// The planner turns these definitions into `CTEProduce` / `CTEAnchor`
/// structure; Cascades decides later whether to inline or reuse them.
#[derive(Clone, Debug, Default)]
pub(crate) struct CTERegistry {
    pub entries: Vec<CTEEntry>,
    next_id: CteId,
}

impl CTERegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a CTE and return its ID.
    pub fn register(
        &mut self,
        name: String,
        resolved_query: ResolvedQuery,
        output_columns: Vec<OutputColumn>,
    ) -> CteId {
        let id = self.next_id;
        self.next_id += 1;
        self.entries.push(CTEEntry {
            id,
            name,
            resolved_query,
            output_columns,
        });
        id
    }

    pub fn get(&self, id: CteId) -> Option<&CTEEntry> {
        self.entries.iter().find(|e| e.id == id)
    }
}

/// A single analyzed CTE definition in the current query scope.
#[derive(Clone, Debug)]
pub(crate) struct CTEEntry {
    pub id: CteId,
    #[allow(dead_code)] // kept for debugging and plan display
    pub name: String,
    pub resolved_query: ResolvedQuery,
    pub output_columns: Vec<OutputColumn>,
}
