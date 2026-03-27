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
                .insert(name_lower.clone(), (col.data_type.clone(), col.nullable));
            self.ordered.push((
                qualifier.map(|s| s.to_lowercase()),
                name_lower,
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
            .insert(name_lower.clone(), (data_type.clone(), nullable));
        self.ordered.push((
            qualifier.map(|s| s.to_lowercase()),
            name_lower,
            data_type,
            nullable,
        ));
    }

    /// Resolve a column reference.
    pub(super) fn resolve(
        &self,
        qualifier: Option<&str>,
        name: &str,
    ) -> Result<(DataType, bool), String> {
        let name_lower = name.to_lowercase();
        if let Some(q) = qualifier {
            let q_lower = q.to_lowercase();
            self.qualified
                .get(&(q_lower.clone(), name_lower.clone()))
                .cloned()
                .ok_or_else(|| format!("column `{q}.{name}` not found"))
        } else {
            self.unqualified
                .get(&name_lower)
                .cloned()
                .ok_or_else(|| format!("column `{name}` not found"))
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
