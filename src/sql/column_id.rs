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
pub(crate) struct ColumnId(pub u32);

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
pub(crate) struct ColumnMeta {
    pub id: ColumnId,
    pub name: String,
    pub qualifier: Option<String>,
    pub data_type: DataType,
    pub nullable: bool,
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
    pub fn new() -> Self {
        Self {
            next_id: 1,
            columns: Vec::new(),
        }
    }

    /// Allocate a new [`ColumnId`] for a column with the given metadata.
    pub fn create(
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
        });
        id
    }

    /// Look up metadata for a previously allocated [`ColumnId`].
    ///
    /// # Panics
    /// Panics if `id` was not allocated by this factory.
    pub fn get(&self, id: ColumnId) -> &ColumnMeta {
        assert!(
            id.0 >= 1 && (id.0 as usize) <= self.columns.len(),
            "ColumnId {} out of range (factory has {} columns)",
            id.0,
            self.columns.len()
        );
        &self.columns[(id.0 - 1) as usize]
    }

    /// Return a human-readable display name for the column: `"qualifier.name"`
    /// or just `"name"`.
    pub fn display_name(&self, id: ColumnId) -> String {
        let m = self.get(id);
        if let Some(q) = &m.qualifier {
            format!("{}.{}", q, m.name)
        } else {
            m.name.clone()
        }
    }

    /// Return just the column name (without qualifier).
    pub fn column_name(&self, id: ColumnId) -> &str {
        &self.get(id).name
    }

    /// Return the number of columns allocated so far.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Returns the next `ColumnId` value that `create` would allocate, without
    /// allocating it. Used to seed downstream allocators (e.g. IMV rewrite)
    /// so they never collide with ids this factory has already handed out.
    pub fn peek_next_id(&self) -> u32 {
        self.next_id
    }
}

impl Default for ColumnRefFactory {
    fn default() -> Self {
        Self::new()
    }
}
