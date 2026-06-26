use std::collections::HashMap;

use arrow::datatypes::DataType;

use crate::connector::scan_planning::{ScanHandle, Split};
use crate::sql::catalog::TableDef;
use crate::sql::column_id::ColumnId;
use crate::thrift::types;

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
#[derive(Clone)]
pub(crate) struct ExprScope {
    /// Ordered list of (column_name, binding) for wildcard expansion
    ordered: Vec<(String, ColumnBinding)>,
    /// ColumnId -> binding. This is the only semantic column lookup.
    by_id: HashMap<ColumnId, ColumnBinding>,
    /// Explicit machine/internal columns that are not semantic SQL columns.
    internal_by_name: HashMap<String, ColumnBinding>,
}

impl ExprScope {
    pub fn new() -> Self {
        Self {
            ordered: Vec::new(),
            by_id: HashMap::new(),
            internal_by_name: HashMap::new(),
        }
    }

    pub fn add_column(&mut self, _qualifier: Option<String>, name: String, binding: ColumnBinding) {
        let name_lower = name.to_lowercase();
        self.ordered.push((name_lower, binding));
    }

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

    /// Register a `ColumnId` lookup for an existing output slot without adding
    /// another visible column name. Use this when an operator remaps a child's
    /// physical binding but keeps the same semantic column identity.
    pub fn add_id_alias(&mut self, column_id: ColumnId, binding: ColumnBinding) {
        if column_id != ColumnId::UNSET {
            self.by_id.insert(column_id, binding);
        }
    }

    pub fn resolve_by_id(&self, column_id: ColumnId) -> Option<&ColumnBinding> {
        if column_id == ColumnId::UNSET {
            return None;
        }
        self.by_id.get(&column_id)
    }

    pub fn add_internal_column(&mut self, name: String, binding: ColumnBinding) {
        self.internal_by_name.insert(name.to_lowercase(), binding);
    }

    pub fn resolve_internal_by_name(&self, name: &str) -> Result<&ColumnBinding, String> {
        let name_lower = name.to_lowercase();
        self.internal_by_name
            .get(&name_lower)
            .ok_or_else(|| format!("Internal column '{}' cannot be resolved.", name))
    }

    /// Iterate all columns in declaration order. Used for SELECT *.
    pub fn iter_columns(&self) -> impl Iterator<Item = (&String, &ColumnBinding)> {
        self.ordered.iter().map(|(name, binding)| (name, binding))
    }

    pub fn iter_id_bindings(&self) -> impl Iterator<Item = (&ColumnId, &ColumnBinding)> {
        self.by_id.iter()
    }

    /// Merge another scope into this one. Used for building JOIN output scopes.
    /// ColumnId entries use left-wins semantics, which keeps self-join sides
    /// distinct because each side mints different ColumnIds.
    pub fn merge(&mut self, other: &ExprScope) {
        for (name, binding) in &other.ordered {
            self.ordered.push((name.clone(), binding.clone()));
        }
        for (column_id, binding) in &other.by_id {
            self.by_id
                .entry(*column_id)
                .or_insert_with(|| binding.clone());
        }
        for (name, binding) in &other.internal_by_name {
            self.internal_by_name
                .entry(name.clone())
                .or_insert_with(|| binding.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(slot_id: i32, data_type: DataType) -> ColumnBinding {
        ColumnBinding {
            tuple_id: 1,
            slot_id,
            data_type,
            type_desc: None,
            nullable: false,
        }
    }

    #[test]
    fn p3_expr_scope_does_not_resolve_semantic_columns_by_name() {
        let mut scope = ExprScope::new();
        let column_id = ColumnId::new_for_test(10);
        scope.add_column_with_id(
            column_id,
            None,
            "a".to_string(),
            binding(20, DataType::Int32),
        );

        assert_eq!(
            scope
                .resolve_by_id(column_id)
                .expect("id binding should resolve")
                .slot_id,
            20
        );
        assert!(
            scope.resolve_internal_by_name("a").is_err(),
            "regular semantic columns must not be visible through the internal name channel"
        );
    }

    #[test]
    fn p3_internal_name_channel_is_explicit() {
        let mut scope = ExprScope::new();
        scope.add_column_with_id(
            ColumnId::new_for_test(10),
            None,
            "a".to_string(),
            binding(20, DataType::Int32),
        );
        scope.add_internal_column("__change_op".to_string(), binding(21, DataType::Int8));

        assert_eq!(
            scope
                .resolve_internal_by_name("__change_op")
                .expect("explicit internal binding should resolve")
                .slot_id,
            21
        );
        assert!(
            scope.resolve_internal_by_name("a").is_err(),
            "semantic columns are not implicit internal bindings"
        );
    }
}
