use std::collections::HashMap;

use arrow::datatypes::DataType;

use crate::sql::catalog::TableDef;

#[derive(Clone, Debug)]
pub(crate) struct ResolvedTable {
    pub database: String,
    pub table: TableDef,
    pub alias: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct ColumnBinding {
    pub tuple_id: i32,
    pub slot_id: i32,
    pub data_type: DataType,
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
}

impl ExprScope {
    pub fn new() -> Self {
        Self {
            qualified: HashMap::new(),
            unqualified: HashMap::new(),
            ordered: Vec::new(),
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
                .ok_or_else(|| format!("column `{q}.{name}` not found"))
        } else {
            self.unqualified
                .get(&name_lower)
                .ok_or_else(|| format!("column `{name}` not found"))
        }
    }

    /// Iterate all columns in declaration order. Used for SELECT *.
    pub fn iter_columns(&self) -> impl Iterator<Item = (&String, &ColumnBinding)> {
        self.ordered.iter().map(|(name, binding)| (name, binding))
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
    }
}
