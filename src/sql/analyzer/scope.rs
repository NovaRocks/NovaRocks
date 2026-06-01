use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use arrow::datatypes::DataType;

use crate::sql::analysis::LambdaParam;
use crate::sql::catalog::ColumnDef;
use crate::sql::column_id::{ColumnId, ColumnRefFactory};

/// Tracks column names and types visible at the current query level.
/// Similar to `ExprScope` in `resolve.rs` but without physical binding
/// (no tuple_id / slot_id).
#[derive(Clone)]
pub(super) struct AnalyzerScope {
    /// Shared factory for allocating globally unique ColumnIds.
    factory: Rc<RefCell<ColumnRefFactory>>,
    /// (qualifier_lower, col_name_lower) -> (ColumnId, DataType, nullable)
    qualified: HashMap<(String, String), (ColumnId, DataType, bool)>,
    /// col_name_lower -> (ColumnId, DataType, nullable)
    unqualified: HashMap<String, (ColumnId, DataType, bool)>,
    /// Ordered columns for SELECT * expansion:
    /// (qualifier, col_name, ColumnId, DataType, nullable)
    ordered: Vec<(Option<String>, String, ColumnId, DataType, bool)>,
    /// Lambda parameters visible in the current expression scope.
    lambda_params: HashMap<String, LambdaParam>,
    /// For column names that have a canonical qualifier — e.g. a USING-join
    /// column whose unqualified reference must resolve to a specific side's
    /// physical binding — this map records that qualifier. Callers can use
    /// `resolve_qualifier` to rewrite an unqualified `ColumnRef` to a
    /// qualified one before the codegen layer compiles it.
    canonical_qualifier: HashMap<String, String>,
    /// Synthetic expressions for column names that should evaluate to
    /// something other than a single column ref. Used for FULL OUTER USING
    /// columns, where unqualified `id` must evaluate to
    /// `COALESCE(left.id, right.id)` so that null-padding on either side
    /// still produces the correct merged value.
    computed_columns: HashMap<String, crate::sql::analysis::TypedExpr>,
    /// Logical type tags for columns whose Arrow `DataType` is ambiguous
    /// (for example JSON-as-Utf8 or BITMAP/HLL-as-Binary). The analyzer
    /// consults this side-table to reject or special-case semantics that
    /// depend on the original StarRocks logical type. Keyed by lower-cased
    /// (qualifier, column) and the unqualified column name.
    qualified_logical_types: HashMap<(String, String), crate::sql::SqlType>,
    unqualified_logical_types: HashMap<String, crate::sql::SqlType>,
}

impl AnalyzerScope {
    pub(super) fn new(factory: Rc<RefCell<ColumnRefFactory>>) -> Self {
        Self {
            factory,
            qualified: HashMap::new(),
            unqualified: HashMap::new(),
            ordered: Vec::new(),
            lambda_params: HashMap::new(),
            canonical_qualifier: HashMap::new(),
            computed_columns: HashMap::new(),
            qualified_logical_types: HashMap::new(),
            unqualified_logical_types: HashMap::new(),
        }
    }

    /// Return a reference to the shared ColumnRefFactory.
    pub(super) fn factory(&self) -> &Rc<RefCell<ColumnRefFactory>> {
        &self.factory
    }

    /// Look up the logical type tag for a column reference. Returns the
    /// StarRocks `SqlType` for tagged columns; `None` for columns whose Arrow
    /// type fully describes them.
    pub(super) fn logical_type_for(
        &self,
        qualifier: Option<&str>,
        name: &str,
    ) -> Option<crate::sql::SqlType> {
        let name_lower = name.to_lowercase();
        if let Some(q) = qualifier
            && let Some(t) = self
                .qualified_logical_types
                .get(&(q.to_lowercase(), name_lower.clone()))
        {
            return Some(t.clone());
        }
        self.unqualified_logical_types.get(&name_lower).cloned()
    }

    /// Convenience: if `expr` is a `ColumnRef`, return the column's logical
    /// type tag. Returns `None` for expressions that are not direct column
    /// references. Derived logical values are detected by their producer
    /// expressions where needed.
    pub(super) fn logical_type_of_expr(
        &self,
        expr: &crate::sql::analysis::TypedExpr,
    ) -> Option<crate::sql::SqlType> {
        if let crate::sql::analysis::ExprKind::ColumnRef {
            qualifier, column, ..
        } = &expr.kind
        {
            self.logical_type_for(qualifier.as_deref(), column)
        } else {
            None
        }
    }

    /// Return the canonical qualifier for an unqualified column name, if any.
    /// USING-clause joins record a canonical qualifier so that downstream
    /// resolution against codegen scopes (which contain both sides' bindings)
    /// picks the correct slot.
    pub(super) fn canonical_qualifier_for(&self, name: &str) -> Option<String> {
        self.canonical_qualifier.get(&name.to_lowercase()).cloned()
    }

    /// Return a synthetic expression for an unqualified column name, if any.
    /// FULL OUTER USING columns register a `COALESCE(left.col, right.col)`
    /// expression here so the analyzer rewrites unqualified references to
    /// the merged value.
    pub(super) fn computed_column_for(
        &self,
        name: &str,
    ) -> Option<&crate::sql::analysis::TypedExpr> {
        self.computed_columns.get(&name.to_lowercase())
    }

    /// Register all columns from a table (or subquery output).
    /// Returns the freshly-allocated `ColumnId`s in the same order as
    /// `columns` so callers (e.g. analyzer `Relation::Scan` construction)
    /// can record them and pass them down to the planner — this is what
    /// keeps the G1 ColumnId invariant ("scan output ids == analyzer ids")
    /// intact across the analyzer → planner boundary.
    pub(super) fn add_table(
        &mut self,
        qualifier: Option<&str>,
        columns: &[ColumnDef],
    ) -> Vec<ColumnId> {
        let mut ids = Vec::with_capacity(columns.len());
        for col in columns {
            let id = self.factory.borrow_mut().create(
                qualifier.map(|s| s.to_string()),
                col.name.clone(),
                col.data_type.clone(),
                col.nullable,
            );
            ids.push(id);
            self.insert_table_column_binding(qualifier, col, id);
        }
        ids
    }

    /// Register a table's columns using pre-allocated `ColumnId`s.
    ///
    /// Used when rebuilding a FROM-scope from a `Relation::Scan` whose
    /// `column_ids` were already minted during the initial analyzer pass —
    /// reusing them keeps the by-id lookup index continuous so downstream
    /// `ColumnRef` nodes (e.g. group-by keys carried through Repeat /
    /// Aggregate) still resolve via `resolve_by_id` instead of falling
    /// back to name-based lookup. The fallback path breaks for ROLLUP /
    /// CUBE / GROUPING SETS because the planner rewrites the group-by
    /// ColumnRef's qualifier to `__repeat_group`, so the bare-name entry
    /// is gone from the codegen scope.
    pub(super) fn add_table_with_ids(
        &mut self,
        qualifier: Option<&str>,
        columns: &[ColumnDef],
        column_ids: &[ColumnId],
    ) {
        assert_eq!(
            columns.len(),
            column_ids.len(),
            "add_table_with_ids: columns/column_ids length mismatch"
        );
        for (col, &id) in columns.iter().zip(column_ids.iter()) {
            self.insert_table_column_binding(qualifier, col, id);
        }
    }

    fn insert_table_column_binding(
        &mut self,
        qualifier: Option<&str>,
        col: &ColumnDef,
        id: ColumnId,
    ) {
        let name_lower = col.name.to_lowercase();
        if let Some(q) = qualifier {
            self.qualified.insert(
                (q.to_lowercase(), name_lower.clone()),
                (id, col.data_type.clone(), col.nullable),
            );
            if let Some(logical) = col.logical_type.clone() {
                self.qualified_logical_types
                    .insert((q.to_lowercase(), name_lower.clone()), logical);
            }
        }
        self.unqualified.insert(
            name_lower.clone(),
            (id, col.data_type.clone(), col.nullable),
        );
        if let Some(logical) = col.logical_type.clone() {
            self.unqualified_logical_types.insert(name_lower, logical);
        }
        self.ordered.push((
            qualifier.map(|s| s.to_lowercase()),
            col.name.clone(),
            id,
            col.data_type.clone(),
            col.nullable,
        ));
    }

    /// Register a single column (used for subquery output columns, etc.).
    pub(super) fn add_column(
        &mut self,
        qualifier: Option<&str>,
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> ColumnId {
        let name_lower = name.to_lowercase();
        let id = self.factory.borrow_mut().create(
            qualifier.map(|s| s.to_string()),
            name.to_string(),
            data_type.clone(),
            nullable,
        );
        if let Some(q) = qualifier {
            self.qualified.insert(
                (q.to_lowercase(), name_lower.clone()),
                (id, data_type.clone(), nullable),
            );
        }
        self.unqualified
            .insert(name_lower, (id, data_type.clone(), nullable));
        // Store original-case name in ordered for SELECT * display.
        self.ordered.push((
            qualifier.map(|s| s.to_lowercase()),
            name.to_string(),
            id,
            data_type,
            nullable,
        ));
        id
    }

    /// Register a single column with an already-allocated ColumnId.
    /// Used when constructing derived-table and CTE-consume output scopes
    /// from already analyzed query output.
    pub(super) fn add_column_with_id(
        &mut self,
        qualifier: Option<&str>,
        name: &str,
        column_id: ColumnId,
        data_type: DataType,
        nullable: bool,
    ) {
        let name_lower = name.to_lowercase();
        if let Some(q) = qualifier {
            self.qualified.insert(
                (q.to_lowercase(), name_lower.clone()),
                (column_id, data_type.clone(), nullable),
            );
        }
        self.unqualified
            .insert(name_lower, (column_id, data_type.clone(), nullable));
        self.ordered.push((
            qualifier.map(|s| s.to_lowercase()),
            name.to_string(),
            column_id,
            data_type,
            nullable,
        ));
    }

    pub(super) fn add_lambda_param(&mut self, param: LambdaParam) {
        self.lambda_params.insert(param.name.to_lowercase(), param);
    }

    pub(super) fn resolve_lambda_param(&self, name: &str) -> Option<LambdaParam> {
        self.lambda_params.get(&name.to_lowercase()).cloned()
    }

    /// Resolve a column reference. Returns `(ColumnId, DataType, nullable)`.
    ///
    /// Returns a spec-aligned error message when the column name is one of
    /// the two Iceberg V3 row-lineage reserved names but the table did not
    /// register them (i.e. it is not a V3 row-lineage table), so the user
    /// gets a clear diagnostic instead of a generic "cannot be resolved"
    /// message.
    pub(super) fn resolve(
        &self,
        qualifier: Option<&str>,
        name: &str,
    ) -> Result<(ColumnId, DataType, bool), String> {
        let name_lower = name.to_lowercase();
        if let Some(q) = qualifier {
            let q_lower = q.to_lowercase();
            if let Some(found) = self.qualified.get(&(q_lower.clone(), name_lower.clone())) {
                return Ok(found.clone());
            }
            return Err(reserved_name_error(name)
                .unwrap_or_else(|| format!("Column '{}.{}' cannot be resolved.", q, name)));
        }
        if let Some(found) = self.unqualified.get(&name_lower) {
            return Ok(found.clone());
        }
        Err(reserved_name_error(name)
            .unwrap_or_else(|| format!("Column '{}' cannot be resolved.", name)))
    }

    /// Register Iceberg V3 row-lineage reserved pseudo-columns. Unlike
    /// `add_table`, these go into the qualified/unqualified resolution maps
    /// **but not** into `ordered`, so `SELECT *` does not expand them. Users
    /// must reference them by name explicitly (`SELECT _row_id FROM t`).
    pub(super) fn add_iceberg_metadata_columns(
        &mut self,
        qualifier: &str,
        columns: &[crate::sql::catalog::ColumnDef],
    ) -> Vec<crate::sql::column_id::ColumnId> {
        let q_lower = qualifier.to_lowercase();
        let mut ids = Vec::with_capacity(columns.len());
        for col in columns {
            let name_lower = col.name.to_lowercase();
            let id = self.factory.borrow_mut().create(
                Some(qualifier.to_string()),
                col.name.clone(),
                col.data_type.clone(),
                col.nullable,
            );
            self.qualified.insert(
                (q_lower.clone(), name_lower.clone()),
                (id, col.data_type.clone(), col.nullable),
            );
            self.unqualified
                .insert(name_lower, (id, col.data_type.clone(), col.nullable));
            ids.push(id);
        }
        ids
    }

    /// Merge another scope into this one (for JOINs).
    pub(super) fn merge(&mut self, other: &AnalyzerScope) {
        for name in other.unqualified.keys() {
            if self.unqualified.contains_key(name)
                && !self.canonical_qualifier.contains_key(name)
                && let Some(qualifier) = self.unique_qualifier_for_column(name)
            {
                self.canonical_qualifier.insert(name.clone(), qualifier);
            }
        }
        for ((qualifier, name), (id, dt, nullable)) in &other.qualified {
            // Left wins: in `t1 LEFT JOIN t1 AS t2`, the aliased-side scan
            // also registers its columns under the original table name
            // (`t1`) for SQL like `SELECT t1.c FROM t1 AS x` that mixes
            // alias and table-name references. In a self-join the LEFT
            // scan's `("t1", "k1")` is the true left binding, and the
            // RIGHT scan's `("t1", "k1")` is the back-compat alias of its
            // own slot. An unconditional insert here let the right's
            // secondary registration silently shadow the left's primary,
            // so a downstream `t1.k1` ColumnRef bound to the right side's
            // ColumnId — and unmatched-left rows in a LEFT OUTER join
            // surfaced as all-NULL on the left columns
            // (`join_range_direct_mapping` step 20).
            self.qualified
                .entry((qualifier.clone(), name.clone()))
                .or_insert_with(|| (*id, dt.clone(), *nullable));
        }
        for (name, (id, dt, nullable)) in &other.unqualified {
            self.unqualified
                .entry(name.clone())
                .or_insert_with(|| (*id, dt.clone(), *nullable));
        }
        for entry in &other.ordered {
            self.ordered.push(entry.clone());
        }
        for (name, qual) in &other.canonical_qualifier {
            // Do not let an outer USING canonical qualifier rewrite a
            // subquery-local unqualified column with the same name.
            if self.unqualified.contains_key(name) {
                continue;
            }
            self.canonical_qualifier
                .entry(name.clone())
                .or_insert_with(|| qual.clone());
        }
        for (name, expr) in &other.computed_columns {
            // Inner wins: when merging an outer scope into an inner subquery scope,
            // the inner subquery's own bindings (or a chained-USING's later coalesce)
            // must not be shadowed by an outer scope's computed column with the
            // same name. Additionally, do NOT propagate an outer-scope computed
            // column whose name collides with a regular column already bound in
            // the inner scope: `computed_column_for` is consulted BEFORE the
            // normal `resolve()` path, so a leaked outer COALESCE would shadow
            // the inner's own real column (e.g. inner subquery `SELECT k1 FROM t1`
            // must resolve `k1` to `t1.k1`, not to an outer FULL OUTER USING
            // coalesce expression bound at the enclosing query).
            if self.computed_columns.contains_key(name) {
                continue;
            }
            if self.unqualified.contains_key(name) {
                continue;
            }
            self.computed_columns.insert(name.clone(), expr.clone());
        }
    }

    fn unique_qualifier_for_column(&self, name: &str) -> Option<String> {
        let mut found: Option<String> = None;
        for (qualifier, column) in self.qualified.keys() {
            if column != name {
                continue;
            }
            match &found {
                Some(existing) if existing != qualifier => return None,
                Some(_) => {}
                None => found = Some(qualifier.clone()),
            }
        }
        found
    }

    /// Iterate columns in declaration order (for SELECT * expansion).
    pub(super) fn iter_columns(
        &self,
    ) -> impl Iterator<Item = &(Option<String>, String, ColumnId, DataType, bool)> {
        self.ordered.iter()
    }

    /// Iterate columns that belong to a specific qualifier (for `table.*` expansion).
    pub(super) fn iter_qualified_columns(
        &self,
        qualifier: &str,
    ) -> impl Iterator<Item = &(Option<String>, String, ColumnId, DataType, bool)> {
        let q_lower = qualifier.to_lowercase();
        self.ordered
            .iter()
            .filter(move |(q, _, _, _, _)| q.as_deref() == Some(q_lower.as_str()))
    }

    /// Register `COALESCE(left.col, right.col)` for every USING column.
    ///
    /// For FULL OUTER JOIN with USING, the joined column is the merge of
    /// both sides: either side can be NULL-padded when the other side
    /// has no match. The standard SQL output column is COALESCE of both
    /// sides. We register that as a synthetic expression so that
    /// unqualified references in projection / ORDER BY / WHERE pick up
    /// the merged value instead of one side's potentially-null binding.
    ///
    /// `left_qual` / `right_qual` are the table qualifiers for the two
    /// USING-side columns. `dt` / `nullable` describe the joined output.
    ///
    /// Currently unused — the call site in `resolve_from.rs` skips the
    /// FULL OUTER USING transform because the BE-side FULL OUTER hash
    /// join drops the surviving-side columns when the SELECT projection
    /// references them via a COALESCE-wrapped scope. Once that BE bug is
    /// fixed, re-enable the call site to make `SELECT *` over
    /// `FULL OUTER ... USING(...)` produce a single merged column.
    #[allow(dead_code)]
    /// Remove any synthetic computed column (e.g. a FULL OUTER USING
    /// COALESCE) registered for `name`. Used when a later non-FULL-OUTER
    /// USING join changes which side owns the merged column, invalidating
    /// the prior COALESCE chain.
    pub(super) fn clear_computed_column(&mut self, name: &str) {
        self.computed_columns.remove(&name.to_lowercase());
    }

    pub(super) fn register_full_outer_using_coalesce(
        &mut self,
        using_cols: &[String],
        left_qual: &str,
        right_qual: &str,
    ) {
        use crate::sql::analysis::{ExprKind, TypedExpr};
        for col in using_cols {
            let col_lower = col.to_lowercase();
            let Some((left_id, dt, _)) = self
                .qualified
                .get(&(left_qual.to_lowercase(), col_lower.clone()))
            else {
                continue;
            };
            let left_id = *left_id;
            let dt = dt.clone();

            let right_id = self
                .qualified
                .get(&(right_qual.to_lowercase(), col_lower.clone()))
                .map(|(id, _, _)| *id)
                .unwrap_or(ColumnId::UNSET);

            // For chained FULL OUTER USING, the "left" of the new COALESCE
            // is the previous COALESCE expression so that
            // `(t1 FULL OUTER t2) FULL OUTER t3 USING(k1)` evaluates to
            // `COALESCE(COALESCE(t1.k1, t2.k1), t3.k1)`. Without this, the
            // second join overwrites the first's mapping and the inner
            // table's value is lost on outer rows.
            let left_ref = self
                .computed_columns
                .get(&col_lower)
                .cloned()
                .unwrap_or(TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: left_id,
                        qualifier: Some(left_qual.to_string()),
                        column: col_lower.clone(),
                    },
                    data_type: dt.clone(),
                    nullable: true,
                });
            let right_ref = TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: right_id,
                    qualifier: Some(right_qual.to_string()),
                    column: col_lower.clone(),
                },
                data_type: dt.clone(),
                nullable: true,
            };
            let coalesce = TypedExpr {
                kind: ExprKind::FunctionCall {
                    name: "coalesce".to_string(),
                    args: vec![left_ref, right_ref],
                    distinct: false,
                },
                data_type: dt,
                nullable: true,
            };
            self.computed_columns.insert(col_lower, coalesce);
        }
    }

    /// Apply USING-clause column deduplication and reordering.
    ///
    /// After a `JOIN ... USING (col1, col2, ...)`, SQL semantics require that:
    /// - each USING column appears exactly once in `SELECT *` output, and
    /// - the USING columns appear before the remaining non-USING columns.
    ///
    /// `merge()` adds both sides' columns to `ordered` in left-then-right
    /// order, which produces duplicates and the wrong column order for
    /// USING joins. This helper rewrites `ordered` to keep one occurrence
    /// of each USING name and to surface USING columns at the head of the
    /// list (in their USING-clause order). Qualified / unqualified lookup
    /// maps are left untouched so explicit `l.col` / `r.col` references
    /// still resolve.
    ///
    /// `prefer_right` selects which side's entry survives the dedup:
    /// false → keep the left (first) occurrence, used for INNER / LEFT /
    /// FULL OUTER joins where the left side is the primary qualifier
    /// (FULL OUTER additionally needs `register_full_outer_using_coalesce`
    /// to provide the COALESCE expression); true → keep the right
    /// occurrence, used for RIGHT joins.
    pub(super) fn apply_using_layout(&mut self, using_cols: &[String], prefer_right: bool) {
        if using_cols.is_empty() {
            return;
        }
        let names_lower: Vec<String> = using_cols.iter().map(|s| s.to_lowercase()).collect();
        if prefer_right {
            // Reverse-scan dedup: keep the *last* occurrence of each USING name.
            let mut keep_indices: std::collections::HashMap<String, usize> =
                std::collections::HashMap::new();
            for (idx, (_, name, _, _, _)) in self.ordered.iter().enumerate() {
                let n = name.to_lowercase();
                if names_lower.contains(&n) {
                    keep_indices.insert(n, idx);
                }
            }
            let mut i = 0;
            self.ordered.retain(|(_, name, _, _, _)| {
                let n = name.to_lowercase();
                let keep = if names_lower.contains(&n) {
                    keep_indices.get(&n) == Some(&i)
                } else {
                    true
                };
                i += 1;
                keep
            });
        } else {
            // Forward-scan dedup: keep the first occurrence of each USING name.
            let mut seen = std::collections::HashSet::new();
            self.ordered.retain(|(_, name, _, _, _)| {
                let n = name.to_lowercase();
                if names_lower.contains(&n) {
                    seen.insert(n)
                } else {
                    true
                }
            });
        }
        // Move USING columns to the front in USING-clause order.
        let mut front: Vec<(Option<String>, String, ColumnId, DataType, bool)> =
            Vec::with_capacity(names_lower.len());
        for using_name in &names_lower {
            if let Some(pos) = self
                .ordered
                .iter()
                .position(|(_, n, _, _, _)| n.to_lowercase() == *using_name)
            {
                front.push(self.ordered.remove(pos));
            }
        }
        front.append(&mut self.ordered);
        self.ordered = front;

        // For `prefer_right`, also override the unqualified entry so that
        // an unqualified `id1` in WHERE / SELECT / ORDER BY resolves to the
        // right-side binding (matching the column that wins in `ordered`).
        if prefer_right {
            for (_, name, id, dt, nullable) in self
                .ordered
                .iter()
                .take(names_lower.len())
                .cloned()
                .collect::<Vec<_>>()
            {
                self.unqualified
                    .insert(name.to_lowercase(), (id, dt, nullable));
            }
        }

        // Record a canonical qualifier for each USING column so that
        // unqualified references in expressions can be normalized to that
        // qualifier before codegen. Without this, codegen sees `id1`
        // unqualified and looks it up in its own merged ExprScope, which
        // picks left-first regardless of which side semantically owns the
        // column for the join type.
        let collected: Vec<(String, Option<String>)> = self
            .ordered
            .iter()
            .take(names_lower.len())
            .map(|(q, n, _, _, _)| (n.to_lowercase(), q.clone()))
            .collect();
        for (name, qual) in collected {
            if let Some(q) = qual {
                self.canonical_qualifier.insert(name, q);
            }
        }
    }

    /// Register columns only in the qualified map (not unqualified or ordered).
    /// Used when an alias is present and differs from the table name, so that
    /// both `alias.col` and `table.col` resolve but the duplicate does not
    /// appear in SELECT * expansion.
    pub(super) fn add_table_qualified_only(&mut self, qualifier: &str, columns: &[ColumnDef]) {
        let q_lower = qualifier.to_lowercase();
        for col in columns {
            let name_lower = col.name.to_lowercase();
            // Reuse existing ColumnId from the unqualified map if available,
            // since this is just an alias for the same column.
            let id = self
                .unqualified
                .get(&name_lower)
                .map(|(id, _, _)| *id)
                .unwrap_or_else(|| {
                    self.factory.borrow_mut().create(
                        Some(qualifier.to_string()),
                        col.name.clone(),
                        col.data_type.clone(),
                        col.nullable,
                    )
                });
            self.qualified.insert(
                (q_lower.clone(), name_lower),
                (id, col.data_type.clone(), col.nullable),
            );
        }
    }
}

/// Returns a spec-aligned error message when `name` is one of the two
/// Iceberg V3 row-lineage reserved column names but was not registered in
/// the scope (table is not V3 row-lineage). Returns `None` for other names.
fn reserved_name_error(name: &str) -> Option<String> {
    let lower = name.to_lowercase();
    if lower == "_row_id" || lower == "_last_updated_sequence_number" {
        Some(format!(
            "column \"{}\" is only available on Iceberg V3 row-lineage tables \
             (table is not Iceberg V3 with write.row-lineage=true)",
            lower
        ))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::ColumnDef;
    use arrow::datatypes::DataType;

    fn test_factory() -> Rc<RefCell<ColumnRefFactory>> {
        Rc::new(RefCell::new(ColumnRefFactory::new()))
    }

    fn col(name: &str, ty: DataType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: ty,
            nullable,
            write_default: None,
            logical_type: None,
        }
    }

    #[test]
    fn rejects_row_id_on_non_iceberg_table() {
        let mut scope = AnalyzerScope::new(test_factory());
        scope.add_table(Some("t"), &[col("id", DataType::Int64, false)]);
        let err = scope.resolve(None, "_row_id").expect_err("must fail");
        assert!(err.contains("only available on Iceberg V3 row-lineage tables"));
    }

    #[test]
    fn rejects_row_id_on_v2_iceberg_table_no_metadata_added() {
        let mut scope = AnalyzerScope::new(test_factory());
        scope.add_table(Some("ice"), &[col("id", DataType::Int64, false)]);
        // V2 path adds no row-lineage metadata columns.
        let err = scope.resolve(None, "_row_id").expect_err("must fail");
        assert!(err.contains("only available on Iceberg V3 row-lineage tables"));
    }

    #[test]
    fn accepts_row_id_on_v3_row_lineage_table() {
        let mut scope = AnalyzerScope::new(test_factory());
        scope.add_table(Some("ice"), &[col("id", DataType::Int64, false)]);
        scope.add_iceberg_metadata_columns(
            "ice",
            &[
                col("_row_id", DataType::Int64, false),
                col("_last_updated_sequence_number", DataType::Int64, false),
            ],
        );
        let (_id, ty, nullable) = scope.resolve(None, "_row_id").expect("ok");
        assert_eq!(ty, DataType::Int64);
        assert!(!nullable);
    }

    #[test]
    fn select_star_does_not_expose_row_lineage_pseudo_columns() {
        let mut scope = AnalyzerScope::new(test_factory());
        scope.add_table(Some("ice"), &[col("id", DataType::Int64, false)]);
        scope.add_iceberg_metadata_columns("ice", &[col("_row_id", DataType::Int64, false)]);
        let names: Vec<_> = scope
            .iter_columns()
            .map(|(_, n, _, _, _)| n.as_str())
            .collect();
        assert_eq!(names, vec!["id"]);
    }
}
