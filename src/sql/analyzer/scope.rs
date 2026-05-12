use std::collections::HashMap;

use arrow::datatypes::DataType;

use crate::sql::analysis::LambdaParam;
use crate::sql::catalog::ColumnDef;

/// Tracks column names and types visible at the current query level.
/// Similar to `ExprScope` in `resolve.rs` but without physical binding
/// (no tuple_id / slot_id).
#[derive(Clone)]
pub(super) struct AnalyzerScope {
    /// (qualifier_lower, col_name_lower) -> (DataType, nullable)
    qualified: HashMap<(String, String), (DataType, bool)>,
    /// col_name_lower -> (DataType, nullable)
    unqualified: HashMap<String, (DataType, bool)>,
    /// Ordered columns for SELECT * expansion:
    /// (qualifier, col_name, DataType, nullable)
    ordered: Vec<(Option<String>, String, DataType, bool)>,
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
}

impl AnalyzerScope {
    pub(super) fn new() -> Self {
        Self {
            qualified: HashMap::new(),
            unqualified: HashMap::new(),
            ordered: Vec::new(),
            lambda_params: HashMap::new(),
            canonical_qualifier: HashMap::new(),
            computed_columns: HashMap::new(),
        }
    }

    /// Return the canonical qualifier for an unqualified column name, if any.
    /// USING-clause joins record a canonical qualifier so that downstream
    /// resolution against codegen scopes (which contain both sides' bindings)
    /// picks the correct slot.
    pub(super) fn canonical_qualifier_for(&self, name: &str) -> Option<String> {
        self.canonical_qualifier
            .get(&name.to_lowercase())
            .cloned()
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
    pub(super) fn add_table(&mut self, qualifier: Option<&str>, columns: &[ColumnDef]) {
        for col in columns {
            let name_lower = col.name.to_lowercase();
            if let Some(q) = qualifier {
                self.qualified.insert(
                    (q.to_lowercase(), name_lower.clone()),
                    (col.data_type.clone(), col.nullable),
                );
            }
            self.unqualified
                .insert(name_lower, (col.data_type.clone(), col.nullable));
            // Store original-case name in ordered for SELECT * display.
            self.ordered.push((
                qualifier.map(|s| s.to_lowercase()),
                col.name.clone(),
                col.data_type.clone(),
                col.nullable,
            ));
        }
    }

    /// Register a single column (used for subquery output columns, etc.).
    pub(super) fn add_column(
        &mut self,
        qualifier: Option<&str>,
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) {
        let name_lower = name.to_lowercase();
        if let Some(q) = qualifier {
            self.qualified.insert(
                (q.to_lowercase(), name_lower.clone()),
                (data_type.clone(), nullable),
            );
        }
        self.unqualified
            .insert(name_lower, (data_type.clone(), nullable));
        // Store original-case name in ordered for SELECT * display.
        self.ordered.push((
            qualifier.map(|s| s.to_lowercase()),
            name.to_string(),
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

    /// Resolve a column reference. Returns a spec-aligned error message when
    /// the column name is one of the two Iceberg V3 row-lineage reserved names
    /// but the table did not register them (i.e. it is not a V3 row-lineage
    /// table), so the user gets a clear diagnostic instead of a generic
    /// "cannot be resolved" message.
    pub(super) fn resolve(
        &self,
        qualifier: Option<&str>,
        name: &str,
    ) -> Result<(DataType, bool), String> {
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
    ) {
        let q_lower = qualifier.to_lowercase();
        for col in columns {
            let name_lower = col.name.to_lowercase();
            self.qualified.insert(
                (q_lower.clone(), name_lower.clone()),
                (col.data_type.clone(), col.nullable),
            );
            self.unqualified
                .insert(name_lower, (col.data_type.clone(), col.nullable));
        }
    }

    /// Merge another scope into this one (for JOINs).
    pub(super) fn merge(&mut self, other: &AnalyzerScope) {
        for ((qualifier, name), (dt, nullable)) in &other.qualified {
            self.qualified
                .insert((qualifier.clone(), name.clone()), (dt.clone(), *nullable));
        }
        for (name, (dt, nullable)) in &other.unqualified {
            self.unqualified
                .entry(name.clone())
                .or_insert_with(|| (dt.clone(), *nullable));
        }
        for entry in &other.ordered {
            self.ordered.push(entry.clone());
        }
        for (name, qual) in &other.canonical_qualifier {
            self.canonical_qualifier.insert(name.clone(), qual.clone());
        }
        for (name, expr) in &other.computed_columns {
            self.computed_columns.insert(name.clone(), expr.clone());
        }
    }

    /// Iterate columns in declaration order (for SELECT * expansion).
    pub(super) fn iter_columns(
        &self,
    ) -> impl Iterator<Item = &(Option<String>, String, DataType, bool)> {
        self.ordered.iter()
    }

    /// Iterate columns that belong to a specific qualifier (for `table.*` expansion).
    pub(super) fn iter_qualified_columns(
        &self,
        qualifier: &str,
    ) -> impl Iterator<Item = &(Option<String>, String, DataType, bool)> {
        let q_lower = qualifier.to_lowercase();
        self.ordered
            .iter()
            .filter(move |(q, _, _, _)| q.as_deref() == Some(q_lower.as_str()))
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
    pub(super) fn register_full_outer_using_coalesce(
        &mut self,
        using_cols: &[String],
        left_qual: &str,
        right_qual: &str,
    ) {
        use crate::sql::analysis::{ExprKind, TypedExpr};
        for col in using_cols {
            let col_lower = col.to_lowercase();
            let Some((dt, _)) = self.qualified.get(&(left_qual.to_lowercase(), col_lower.clone()))
            else {
                continue;
            };
            let dt = dt.clone();
            let left_ref = TypedExpr {
                kind: ExprKind::ColumnRef {
                    qualifier: Some(left_qual.to_string()),
                    column: col_lower.clone(),
                },
                data_type: dt.clone(),
                nullable: true,
            };
            let right_ref = TypedExpr {
                kind: ExprKind::ColumnRef {
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
            for (idx, (_, name, _, _)) in self.ordered.iter().enumerate() {
                let n = name.to_lowercase();
                if names_lower.contains(&n) {
                    keep_indices.insert(n, idx);
                }
            }
            let mut i = 0;
            self.ordered.retain(|(_, name, _, _)| {
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
            self.ordered.retain(|(_, name, _, _)| {
                let n = name.to_lowercase();
                if names_lower.contains(&n) {
                    seen.insert(n)
                } else {
                    true
                }
            });
        }
        // Move USING columns to the front in USING-clause order.
        let mut front: Vec<(Option<String>, String, DataType, bool)> =
            Vec::with_capacity(names_lower.len());
        for using_name in &names_lower {
            if let Some(pos) = self
                .ordered
                .iter()
                .position(|(_, n, _, _)| n.to_lowercase() == *using_name)
            {
                front.push(self.ordered.remove(pos));
            }
        }
        front.extend(self.ordered.drain(..));
        self.ordered = front;

        // For `prefer_right`, also override the unqualified entry so that
        // an unqualified `id1` in WHERE / SELECT / ORDER BY resolves to the
        // right-side binding (matching the column that wins in `ordered`).
        if prefer_right {
            for (_, name, dt, nullable) in self
                .ordered
                .iter()
                .take(names_lower.len())
                .map(|e| e.clone())
                .collect::<Vec<_>>()
            {
                self.unqualified
                    .insert(name.to_lowercase(), (dt, nullable));
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
            .map(|(q, n, _, _)| (n.to_lowercase(), q.clone()))
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
            self.qualified.insert(
                (q_lower.clone(), name_lower),
                (col.data_type.clone(), col.nullable),
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

    fn col(name: &str, ty: DataType, nullable: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: ty,
            nullable,
            write_default: None,
        }
    }

    #[test]
    fn rejects_row_id_on_non_iceberg_table() {
        let mut scope = AnalyzerScope::new();
        scope.add_table(Some("t"), &[col("id", DataType::Int64, false)]);
        let err = scope.resolve(None, "_row_id").expect_err("must fail");
        assert!(err.contains("only available on Iceberg V3 row-lineage tables"));
    }

    #[test]
    fn rejects_row_id_on_v2_iceberg_table_no_metadata_added() {
        let mut scope = AnalyzerScope::new();
        scope.add_table(Some("ice"), &[col("id", DataType::Int64, false)]);
        // V2 path adds no row-lineage metadata columns.
        let err = scope.resolve(None, "_row_id").expect_err("must fail");
        assert!(err.contains("only available on Iceberg V3 row-lineage tables"));
    }

    #[test]
    fn accepts_row_id_on_v3_row_lineage_table() {
        let mut scope = AnalyzerScope::new();
        scope.add_table(Some("ice"), &[col("id", DataType::Int64, false)]);
        scope.add_iceberg_metadata_columns(
            "ice",
            &[
                col("_row_id", DataType::Int64, false),
                col("_last_updated_sequence_number", DataType::Int64, false),
            ],
        );
        let (ty, nullable) = scope.resolve(None, "_row_id").expect("ok");
        assert_eq!(ty, DataType::Int64);
        assert!(!nullable);
    }

    #[test]
    fn select_star_does_not_expose_row_lineage_pseudo_columns() {
        let mut scope = AnalyzerScope::new();
        scope.add_table(Some("ice"), &[col("id", DataType::Int64, false)]);
        scope.add_iceberg_metadata_columns("ice", &[col("_row_id", DataType::Int64, false)]);
        let names: Vec<_> = scope
            .iter_columns()
            .map(|(_, n, _, _)| n.as_str())
            .collect();
        assert_eq!(names, vec!["id"]);
    }
}
