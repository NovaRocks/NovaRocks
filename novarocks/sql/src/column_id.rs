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

//! Global column identity for the SQL optimizer layer.
//!
//! Every column in a query plan receives a unique [`ColumnId`] allocated by
//! [`ColumnRefFactory`] during semantic analysis. Downstream layers —
//! distribution specs, equivalence classes, sort keys, output schemas —
//! reference columns by id, never by name strings.
//!
//! Display names (for EXPLAIN, error messages, and the MySQL wire output
//! schema) are stored in [`ColumnMeta`] inside the factory and looked up
//! when needed.
//!
//! Design reference: StarRocks `ColumnRefOperator` / `ColumnRefFactory`.

use std::fmt;

use arrow::datatypes::DataType;

// ---------------------------------------------------------------------------
// ColumnId
// ---------------------------------------------------------------------------

/// A globally unique column identifier within a single query planning session.
///
/// Invariant: `Project` and `Window` operators do **not** allocate new ids
/// for pass-through columns. Derived-table aliases are resolved in the analyzer
/// and represented through output metadata or ordinary Project adapters before
/// the optimizer sees the plan.
#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct ColumnId(pub u32);

impl ColumnId {
    /// Sentinel value used only during bootstrapping or when a real id is not
    /// yet available. Production code should never compare against this.
    pub const UNSET: ColumnId = ColumnId(0);

    /// Construct a `ColumnId` from a raw u32 for use in tests only.
    #[cfg(test)]
    pub(crate) fn new_for_test(id: u32) -> ColumnId {
        ColumnId(id)
    }
}

impl fmt::Debug for ColumnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "c{}", self.0)
    }
}

impl fmt::Display for ColumnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "c{}", self.0)
    }
}

// ---------------------------------------------------------------------------
// ColumnMeta
// ---------------------------------------------------------------------------

/// Metadata about a column, stored in the [`ColumnRefFactory`].
#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "Column metadata keeps the full schema identity for planner paths that are feature-gated in this target."
)]
pub(crate) struct ColumnMeta {
    pub id: ColumnId,
    pub name: String,
    pub qualifier: Option<String>,
    pub data_type: DataType,
    pub nullable: bool,
    /// SQL logical provenance for physically ambiguous scalar carriers.
    pub logical_type: Option<novarocks_types::schema::SqlType>,
    pub json_list_provenance: bool,
}

// ---------------------------------------------------------------------------
// ColumnRefFactory
// ---------------------------------------------------------------------------

/// Allocates globally unique [`ColumnId`]s for a single planning session.
///
/// The factory maintains a dense list of [`ColumnMeta`] entries indexed by
/// `(id.0 - 1)`. It is created at the start of query analysis and threaded
/// through analyzer → planner → optimizer → codegen.
///
/// Design reference: StarRocks `ColumnRefFactory.java`.
#[derive(Clone, Debug)]
pub(crate) struct ColumnRefFactory {
    next_id: u32,
    columns: Vec<ColumnMeta>,
}

impl ColumnRefFactory {
    pub(crate) fn new() -> Self {
        Self {
            next_id: 1,
            columns: Vec::new(),
        }
    }

    /// Allocate a new [`ColumnId`] for a column with the given metadata.
    pub(crate) fn create(
        &mut self,
        qualifier: Option<String>,
        name: String,
        data_type: DataType,
        nullable: bool,
    ) -> ColumnId {
        let id = ColumnId(self.next_id);
        self.next_id += 1;
        self.columns.push(ColumnMeta {
            id,
            name,
            qualifier,
            data_type,
            nullable,
            logical_type: None,
            json_list_provenance: false,
        });
        id
    }

    /// Reserve all ids below `next_id` so future `create()` calls cannot
    /// collide with ids allocated by a rewrite stage that manages its own
    /// column counter.
    pub(crate) fn reserve_until(&mut self, next_id: u32) {
        let next_id = next_id.max(1);
        while self.next_id < next_id {
            let id = ColumnId(self.next_id);
            self.next_id += 1;
            self.columns.push(ColumnMeta {
                id,
                name: format!("__reserved_col_{}", id.0),
                qualifier: None,
                data_type: DataType::Null,
                nullable: true,
                logical_type: None,
                json_list_provenance: false,
            });
        }
    }

    /// Look up metadata for a previously allocated [`ColumnId`].
    ///
    /// # Panics
    /// Panics if `id` was not allocated by this factory.
    pub(crate) fn get(&self, id: ColumnId) -> &ColumnMeta {
        assert!(
            id.0 >= 1 && (id.0 as usize) <= self.columns.len(),
            "ColumnId {} out of range (factory has {} columns)",
            id.0,
            self.columns.len()
        );
        &self.columns[(id.0 - 1) as usize]
    }

    /// Freeze semantic provenance alongside the query-local column identity.
    pub(crate) fn set_logical_type(
        &mut self,
        id: ColumnId,
        logical_type: Option<novarocks_types::schema::SqlType>,
    ) {
        self.get(id);
        self.columns[(id.0 - 1) as usize].logical_type = logical_type;
    }

    pub(crate) fn set_json_list_provenance(&mut self, id: ColumnId, proven: bool) {
        self.get(id);
        self.columns[(id.0 - 1) as usize].json_list_provenance = proven;
    }

    pub(crate) fn has_json_list_provenance(&self, id: ColumnId) -> bool {
        id.0.checked_sub(1)
            .and_then(|index| self.columns.get(index as usize))
            .is_some_and(|column| column.json_list_provenance)
    }

    pub(crate) fn logical_type(&self, id: ColumnId) -> Option<novarocks_types::schema::SqlType> {
        let index = id.0.checked_sub(1)? as usize;
        self.columns.get(index)?.logical_type.clone()
    }

    /// Return a human-readable display name for the column: `"qualifier.name"`
    /// or just `"name"`.
    #[allow(
        dead_code,
        reason = "Display-name construction is retained for planner diagnostics compiled in feature-specific targets."
    )]
    pub(crate) fn display_name(&self, id: ColumnId) -> String {
        let m = self.get(id);
        if let Some(q) = &m.qualifier {
            format!("{}.{}", q, m.name)
        } else {
            m.name.clone()
        }
    }

    /// Return just the column name (without qualifier).
    #[allow(
        dead_code,
        reason = "Retained for planner diagnostics and feature-gated rewrite paths."
    )]
    pub(crate) fn column_name(&self, id: ColumnId) -> &str {
        &self.get(id).name
    }

    /// Return the number of columns allocated so far.
    #[allow(
        dead_code,
        reason = "Retained for planner diagnostics and feature-gated rewrite paths."
    )]
    pub(crate) fn len(&self) -> usize {
        self.columns.len()
    }

    /// Returns the next `ColumnId` value that `create` would allocate, without
    /// allocating it. Used to seed downstream allocators (e.g. IMV rewrite)
    /// so they never collide with ids this factory has already handed out.
    #[allow(
        dead_code,
        reason = "Retained for planner diagnostics and feature-gated rewrite paths."
    )]
    pub(crate) fn peek_next_id(&self) -> u32 {
        self.next_id
    }
}

impl Default for ColumnRefFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_until_advances_future_allocations_without_sparse_metadata() {
        let mut factory = ColumnRefFactory::new();
        let first = factory.create(None, "a".to_string(), DataType::Int64, false);
        assert_eq!(first, ColumnId(1));

        factory.reserve_until(5);

        assert_eq!(factory.peek_next_id(), 5);
        assert_eq!(factory.len(), 4);
        assert_eq!(factory.column_name(ColumnId(3)), "__reserved_col_3");

        let next = factory.create(None, "b".to_string(), DataType::Utf8, true);
        assert_eq!(next, ColumnId(5));
        assert_eq!(factory.column_name(next), "b");
    }
}
