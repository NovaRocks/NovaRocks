use std::collections::HashMap;

use arrow::datatypes::DataType;

use crate::sql::catalog::{PhysicalTableLayout, TableDef};
use crate::sql::column_id::ColumnId;
use crate::types;

#[derive(Clone, Debug)]
pub(crate) struct ResolvedTable {
    #[allow(dead_code)]
    pub database: String,
    pub table: TableDef,
    pub physical_layout: Option<PhysicalTableLayout>,
    #[allow(dead_code)]
    pub alias: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct ColumnBinding {
    pub tuple_id: i32,
    pub slot_id: i32,
    pub data_type: DataType,
    pub type_desc: Option<types::TTypeDesc>,
    pub nullable: bool,
}

/// Tracks which columns are in scope for expression compilation.
/// Supports both unqualified and qualified (table.column) lookups.
pub(crate) struct ExprScope {
    /// (qualifier, column_name_lower) -> binding
    qualified: HashMap<(String, String), ColumnBinding>,
    /// column_name_lower -> binding (for unqualified lookup)
    unqualified: HashMap<String, ColumnBinding>,
    /// Ordered list of (column_name, binding) for wildcard expansion
    ordered: Vec<(String, ColumnBinding)>,
    /// G1: ColumnId -> binding (primary lookup, when populated).
    /// Used by the expression compiler so a `ColumnRef` with a real
    /// `ColumnId` resolves by id rather than by (qualifier, name) string —
    /// this is what lets the SELECT projection above a GROUPING SETS
    /// Aggregate still find `k1` even though the Aggregate's output slot
    /// is named `__repeat_group.k1`. String lookup remains as fallback for
    /// the call sites that have not yet been migrated to register
    /// ColumnIds.
    by_id: HashMap<ColumnId, ColumnBinding>,
}

impl ExprScope {
    pub fn new() -> Self {
        Self {
            qualified: HashMap::new(),
            unqualified: HashMap::new(),
            ordered: Vec::new(),
            by_id: HashMap::new(),
        }
    }

    pub fn add_column(&mut self, qualifier: Option<String>, name: String, binding: ColumnBinding) {
        let name_lower = name.to_lowercase();
        if let Some(q) = &qualifier {
            self.qualified
                .insert((q.to_lowercase(), name_lower.clone()), binding.clone());
        }
        self.unqualified.insert(name_lower.clone(), binding.clone());
        self.ordered.push((name_lower, binding));
    }

    /// G1 variant of `add_column` that also indexes the binding by
    /// `ColumnId`. Call this for slots whose `ColumnId` is known (e.g.
    /// group-by output slots emitted by `visit_hash_aggregate`).
    /// `ColumnId::UNSET` is ignored — those bindings stay name-indexed only.
    pub fn add_column_with_id(
        &mut self,
        column_id: ColumnId,
        qualifier: Option<String>,
        name: String,
        binding: ColumnBinding,
    ) {
        if column_id != ColumnId::UNSET {
            self.by_id.insert(column_id, binding.clone());
        }
        self.add_column(qualifier, name, binding);
    }

    /// Register a qualified alias for lookup without adding to the ordered
    /// column list.  Use this for secondary qualifiers (e.g. `ss.s_store_sk`
    /// when the unqualified `s_store_sk` is already registered).
    pub fn add_qualified_alias(&mut self, qualifier: String, name: String, binding: ColumnBinding) {
        let name_lower = name.to_lowercase();
        self.qualified
            .insert((qualifier.to_lowercase(), name_lower), binding);
    }

    /// G1: primary column lookup by `ColumnId`. Returns `None` when this
    /// scope does not (yet) have an id-indexed binding for the column.
    pub fn resolve_by_id(&self, column_id: ColumnId) -> Option<&ColumnBinding> {
        if column_id == ColumnId::UNSET {
            return None;
        }
        self.by_id.get(&column_id)
    }

    pub fn resolve_column(
        &self,
        qualifier: Option<&str>,
        name: &str,
    ) -> Result<&ColumnBinding, String> {
        let name_lower = name.to_lowercase();
        if let Some(q) = qualifier {
            let q_lower = q.to_lowercase();
            self.qualified
                .get(&(q_lower.clone(), name_lower.clone()))
                .ok_or_else(|| format!("Column '{}.{}' cannot be resolved.", q, name))
        } else {
            self.unqualified
                .get(&name_lower)
                .ok_or_else(|| format!("Column '{}' cannot be resolved.", name))
        }
    }

    /// Iterate all columns in declaration order. Used for SELECT *.
    pub fn iter_columns(&self) -> impl Iterator<Item = (&String, &ColumnBinding)> {
        self.ordered.iter().map(|(name, binding)| (name, binding))
    }

    /// Iterate all qualified lookup aliases.
    pub fn iter_qualified(&self) -> impl Iterator<Item = (&String, &String, &ColumnBinding)> {
        self.qualified
            .iter()
            .map(|((qualifier, name), binding)| (qualifier, name, binding))
    }

    /// Merge another scope into this one. Used for building JOIN output scopes.
    /// Qualified lookups are always added; unqualified lookups are added
    /// only if the column name is not already present (ambiguous columns
    /// require qualification).
    pub fn merge(&mut self, other: &ExprScope) {
        for ((qualifier, name), binding) in &other.qualified {
            self.qualified
                .insert((qualifier.clone(), name.clone()), binding.clone());
        }
        for (name, binding) in &other.unqualified {
            // For unqualified: skip if already present to avoid ambiguity
            self.unqualified
                .entry(name.clone())
                .or_insert_with(|| binding.clone());
        }
        for (name, binding) in &other.ordered {
            self.ordered.push((name.clone(), binding.clone()));
        }
        // G1 id index: copy other's id bindings. Same column id from both
        // children would mean the column is the same physical entity (e.g.
        // a USING-shared column or a colocate-passthrough), so overwriting
        // is correct.
        for (column_id, binding) in &other.by_id {
            self.by_id.insert(*column_id, binding.clone());
        }
    }
}
