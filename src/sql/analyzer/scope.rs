use std::collections::HashMap;

use arrow::datatypes::DataType;

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
}

impl AnalyzerScope {
    pub(super) fn new() -> Self {
        Self {
            qualified: HashMap::new(),
            unqualified: HashMap::new(),
            ordered: Vec::new(),
        }
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
