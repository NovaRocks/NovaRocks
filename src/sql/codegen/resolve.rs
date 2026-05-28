use std::collections::HashMap;

use arrow::datatypes::DataType;

use crate::connector::scan_planning::{ScanHandle, Split};
use crate::sql::catalog::TableDef;
use crate::sql::column_id::ColumnId;
use crate::types;

#[derive(Clone, Debug)]
pub(crate) struct PlannedConnectorScan {
    pub(crate) scan: ScanHandle,
    pub(crate) splits: Vec<Split>,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedTable {
    #[allow(dead_code)]
    pub database: String,
    pub table: TableDef,
    pub planned_scan: Option<PlannedConnectorScan>,
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
    /// Both qualified and unqualified lookups use "left wins": entries already
    /// present in `self` are kept and the corresponding entry in `other` is
    /// dropped on the floor.
    ///
    /// "Left wins" is required for self-join correctness. When a scan is
    /// aliased (`t1 AS t2`), `visit_scan` registers each column under the
    /// alias AND the original table name (so `SELECT t1.k FROM t1 AS x`
    /// referencing the bare table name still works). In a self-join
    /// `t1 LEFT JOIN t1 t2`, both children therefore have a qualified
    /// `("t1", "k1")` entry — the left's is the true left binding; the
    /// right's is a back-compat alias pointing at the *right* side's slot.
    /// Unconditional insert would let the right's secondary registration
    /// silently shadow the left's primary, so a downstream `t1.k1`
    /// ColumnRef resolved to the right side's slot and unmatched-left rows
    /// in a LEFT OUTER join surfaced as all-NULL on the left columns
    /// (`join_range_direct_mapping` step 20).
    pub fn merge(&mut self, other: &ExprScope) {
        for ((qualifier, name), binding) in &other.qualified {
            self.qualified
                .entry((qualifier.clone(), name.clone()))
                .or_insert_with(|| binding.clone());
        }
        for (name, binding) in &other.unqualified {
            self.unqualified
                .entry(name.clone())
                .or_insert_with(|| binding.clone());
        }
        for (name, binding) in &other.ordered {
            self.ordered.push((name.clone(), binding.clone()));
        }
        // G1 id index: same column id from both children would mean the
        // column is the same physical entity (e.g. a USING-shared column
        // or a colocate-passthrough), which is incompatible with self-join
        // where left and right deliberately mint different ColumnIds. So
        // also "left wins" here.
        for (column_id, binding) in &other.by_id {
            self.by_id
                .entry(*column_id)
                .or_insert_with(|| binding.clone());
        }
    }
}
