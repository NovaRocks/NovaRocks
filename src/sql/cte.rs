//! CTE (Common Table Expression) metadata types.
//!
//! The registry stores all non-recursive CTE definitions analyzed for the
//! current query. Lexical visibility is tracked separately by the analyzer.

use crate::sql::ir::{OutputColumn, ResolvedQuery};

/// Unique identifier for a CTE within a query.
pub(crate) type CteId = u32;

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
    pub name: String,
    pub resolved_query: ResolvedQuery,
    pub output_columns: Vec<OutputColumn>,
}
