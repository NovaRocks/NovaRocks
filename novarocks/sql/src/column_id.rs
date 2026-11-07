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

#[cfg(test)]
use arrow::datatypes::DataType;
use std::fmt;

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
    /// Original value identity, changed only by explicit same-value handoff.
    pub value_origin: ColumnId,
    pub name: String,
    pub qualifier: Option<String>,
    pub value_type: novarocks_type_contract::FunctionValueType,
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
    columns: Vec<Option<ColumnMeta>>,
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
        value_type: novarocks_type_contract::FunctionValueType,
    ) -> ColumnId {
        let id = ColumnId(self.next_id);
        self.next_id += 1;
        self.columns.push(Some(ColumnMeta {
            id,
            value_origin: id,
            name,
            qualifier,
            json_list_provenance: false,
            value_type,
            logical_type: None,
        }));
        id
    }

    /// Reserve all ids below `next_id` so future `create()` calls cannot
    /// collide with ids allocated by a rewrite stage that manages its own
    /// column counter.
    pub(crate) fn reserve_until(&mut self, next_id: u32) {
        let next_id = next_id.max(1);
        while self.next_id < next_id {
            self.next_id += 1;
            // A reserved identity has no authored column or value domain.
            self.columns.push(None);
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
        self.columns[(id.0 - 1) as usize]
            .as_ref()
            .expect("reserved ColumnId has no source metadata")
    }

    /// Read the complete admitted schema identity. Scope-specific null padding
    /// belongs to the scope's borrowed projection, not this source record.
    pub(crate) fn value_type(
        &self,
        id: ColumnId,
    ) -> Option<&novarocks_type_contract::FunctionValueType> {
        self.columns
            .get(id.0.checked_sub(1)? as usize)
            .and_then(|column| column.as_ref().map(|column| &column.value_type))
    }

    pub(crate) fn set_logical_type(
        &mut self,
        id: ColumnId,
        logical_type: Option<novarocks_types::schema::SqlType>,
    ) {
        let index = id.0.checked_sub(1).expect("ColumnId starts at one") as usize;
        self.columns[index]
            .as_mut()
            .expect("reserved ColumnId has no source metadata")
            .logical_type = logical_type;
    }
    pub(crate) fn set_json_list_provenance(&mut self, id: ColumnId, value: bool) {
        let index = id.0.checked_sub(1).expect("ColumnId starts at one") as usize;
        self.columns[index]
            .as_mut()
            .expect("reserved ColumnId has no source metadata")
            .json_list_provenance = value;
    }
    pub(crate) fn has_json_list_provenance(&self, id: ColumnId) -> bool {
        id.0.checked_sub(1)
            .and_then(|i| self.columns.get(i as usize))
            .and_then(Option::as_ref)
            .is_some_and(|column| column.json_list_provenance)
    }

    pub(crate) fn borrowed_logical_type(
        &self,
        id: ColumnId,
    ) -> Option<&novarocks_types::schema::SqlType> {
        let index = id.0.checked_sub(1)? as usize;
        self.columns.get(index)?.as_ref()?.logical_type.as_ref()
    }

    pub(crate) fn logical_type(&self, id: ColumnId) -> Option<novarocks_types::schema::SqlType> {
        if let Some(logical) = self.borrowed_logical_type(id) {
            return Some(logical.clone());
        }
        use novarocks_type_contract::ValueLogicalType;
        use novarocks_types::schema::SqlType;
        match self.value_type(id)?.logical_type() {
            ValueLogicalType::Json => Some(SqlType::Json),
            ValueLogicalType::Hll => Some(SqlType::Hll),
            ValueLogicalType::Bitmap => Some(SqlType::Bitmap),
            ValueLogicalType::Variant => Some(SqlType::Variant),
            ValueLogicalType::LargeInt => Some(SqlType::LargeInt),
            ValueLogicalType::Uuid => Some(SqlType::Uuid),
            _ => None,
        }
    }

    /// Transfer already-established facts across a planner-proven same-value
    /// symbol rewrite. This does not derive a domain from a carrier or name.
    pub(crate) fn transfer_value_provenance(
        &mut self,
        source: ColumnId,
        target: ColumnId,
    ) -> Result<(), &'static str> {
        let original = self.get(source);
        let replacement = self.get(target);
        if original.value_type.data_type() != replacement.value_type.data_type() {
            return Err("same-value column rewrite changed its declared carrier");
        }
        if !original
            .value_type
            .same_physical_binding_ignoring_root_nullability_observed(
                &replacement.value_type,
                || Ok::<(), std::convert::Infallible>(()),
            )
            .expect("infallible checked type comparison")
        {
            return Err("same-value column rewrite changed its complete declared type");
        }
        if original.value_type.nullable() && !replacement.value_type.nullable() {
            return Err("same-value column rewrite narrowed its declared nullability");
        }
        if replacement.logical_type.is_some() && replacement.logical_type != original.logical_type {
            return Err("same-value column rewrite contains conflicting logical domains");
        }
        if replacement.json_list_provenance && !original.json_list_provenance {
            return Err("same-value column rewrite contains conflicting JSON list witnesses");
        }
        let logical = original.logical_type.clone();
        let json_list = original.json_list_provenance;
        self.set_logical_type(target, logical);
        self.set_json_list_provenance(target, json_list);
        Ok(())
    }

    /// Preserve a value occurrence only at an actual planner symbol replacement.
    /// CTE consumes copy type provenance while retaining distinct row bindings.
    pub(crate) fn transfer_value_occurrence(
        &mut self,
        source: ColumnId,
        target: ColumnId,
    ) -> Result<(), &'static str> {
        self.transfer_value_provenance(source, target)?;
        let origin = self.get(source).value_origin;
        self.columns[(target.0 - 1) as usize]
            .as_mut()
            .expect("authored target")
            .value_origin = origin;
        Ok(())
    }

    pub(crate) fn same_value_origin(&self, source: ColumnId, target: ColumnId) -> bool {
        self.get(source).value_origin == self.get(target).value_origin
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
        let first = factory.create(
            None,
            "a".to_string(),
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        );
        assert_eq!(first, ColumnId(1));

        factory.reserve_until(5);

        assert_eq!(factory.peek_next_id(), 5);
        assert_eq!(factory.len(), 4);
        assert!(factory.value_type(ColumnId(3)).is_none());

        let next = factory.create(
            None,
            "b".to_string(),
            novarocks_type_contract::FunctionValueType::new(DataType::Utf8, true),
        );
        assert_eq!(next, ColumnId(5));
        assert_eq!(factory.column_name(next), "b");
    }
    #[test]
    fn type_provenance_does_not_merge_distinct_relation_occurrences() {
        let mut factory = ColumnRefFactory::new();
        let ty = novarocks_type_contract::FunctionValueType::new(
            arrow::datatypes::DataType::Int64,
            true,
        );
        let producer = factory.create(None, "x".into(), ty.clone());
        let left = factory.create(Some("left".into()), "x".into(), ty.clone());
        let right = factory.create(Some("right".into()), "x".into(), ty.clone());
        factory.transfer_value_provenance(producer, left).unwrap();
        factory.transfer_value_provenance(producer, right).unwrap();
        assert!(!factory.same_value_origin(left, right));
        let replacement = factory.create(None, "x".into(), ty);
        factory
            .transfer_value_occurrence(left, replacement)
            .unwrap();
        assert!(factory.same_value_origin(left, replacement));
        assert!(!factory.same_value_origin(right, replacement));
    }

    #[test]
    fn same_value_provenance_preserves_nested_domains_and_root_nullability_rules() {
        let mut factory = ColumnRefFactory::new();
        let json = crate::test_value_types::string_structure(&[("payload", true, true)], false);
        let text = crate::test_value_types::string_structure(&[("payload", true, false)], false);
        assert_eq!(json.data_type(), text.data_type());
        let source = factory.create(None, "source".into(), json.clone());
        let widened = factory.create(None, "widened".into(), json.with_nullable(true).unwrap());
        let changed = factory.create(None, "changed".into(), text);
        assert!(factory.transfer_value_provenance(source, widened).is_ok());
        assert!(factory.transfer_value_provenance(source, changed).is_err());
        assert!(factory.transfer_value_provenance(widened, source).is_err());
    }
}
