//! Per-rule context for the low-cardinality dictionary rewrite.
//!
//! Distinct from `crate::sql::optimizer::rewrite::context::RewriteContext`:
//! this struct lives for one application of
//! `LowCardinalityDictionaryRewriteRule` and tracks the dict-eligible
//! scan columns (keyed by `ScanColumnKey`).
//!
//! Per-subtree column visibility — i.e. "in this branch of the plan,
//! which output column name resolves to which dict column / snapshot"
//! — lives on the `DictScope` value that the rewriter threads up
//! through its recursive calls, *not* on this context. That separation
//! is what stops two scans with a column of the same name from
//! colliding in a single global map.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::sql::column_id::ColumnId;
use crate::sql::common::DictionarySnapshot;

/// Identifies a base-table column that is participating in dictionary
/// rewrite. `(database, table, column)` are all lowercased to match the
/// normalization the catalog applies.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ScanColumnKey {
    pub database: String,
    pub table: String,
    pub column: String,
}

impl ScanColumnKey {
    pub(crate) fn new(database: &str, table: &str, column: &str) -> Self {
        Self {
            database: database.to_ascii_lowercase(),
            table: table.to_ascii_lowercase(),
            column: column.to_ascii_lowercase(),
        }
    }
}

/// What the rewriter needs to know about a single dict-encoded column
/// that is currently visible in a subtree's output. `string_column` is
/// the name the column is published under in the current scope (may
/// differ from the base scan column name due to Project aliases);
/// `dict_column` is the synthetic Int32 slot the scan emits; `snapshot`
/// is the dictionary itself.
#[derive(Clone, Debug)]
pub(crate) struct DictBinding {
    pub dict_column: String,
    pub source_column_id: ColumnId,
    pub snapshot: Arc<DictionarySnapshot>,
}

/// Per-subtree map from the current scope's output column name to its
/// dict binding. Threaded as a return value alongside each rewritten
/// subtree so two scans of the same logical column name in different
/// branches cannot collide (each branch carries its own scope).
#[derive(Clone, Debug, Default)]
pub(crate) struct DictScope {
    bindings: BTreeMap<String, DictBinding>,
}

impl DictScope {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn insert(&mut self, output_name: String, binding: DictBinding) {
        self.bindings
            .insert(output_name.to_ascii_lowercase(), binding);
    }

    pub(crate) fn get(&self, output_name: &str) -> Option<&DictBinding> {
        self.bindings.get(&output_name.to_ascii_lowercase())
    }

    /// Resolve a column name that is EITHER the output (string) column
    /// name OR the synthesized dict column name. Returns the binding
    /// plus the output (source) name the binding is registered under.
    ///
    /// Used by the Task 8 Join / Union rewriters to recognize predicates
    /// and project items that were already rewritten in a prior pipeline
    /// iteration (where the column ref is on the dict slot directly).
    pub(crate) fn resolve_either(&self, name: &str) -> Option<(&str, &DictBinding)> {
        let lower = name.to_ascii_lowercase();
        if let Some(binding) = self.bindings.get(&lower) {
            return Some((
                self.bindings.get_key_value(&lower).unwrap().0.as_str(),
                binding,
            ));
        }
        for (k, b) in &self.bindings {
            if b.dict_column.eq_ignore_ascii_case(name) {
                return Some((k.as_str(), b));
            }
        }
        None
    }

    pub(crate) fn resolve_column_id(&self, column_id: ColumnId) -> Option<(&str, &DictBinding)> {
        if column_id == ColumnId::UNSET {
            return None;
        }
        self.bindings
            .iter()
            .find(|(_, binding)| binding.source_column_id == column_id)
            .map(|(name, binding)| (name.as_str(), binding))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }

    /// Iterate `(output_name, binding)` pairs registered in this scope.
    /// Used by Project propagation to emit sibling pass-through items for
    /// each dict slot visible at the input.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&String, &DictBinding)> {
        self.bindings.iter()
    }
}

/// Rule-global state accumulated by the collector and consumed by the
/// rewriter. Conservative by design: any scan-side dictionary column
/// that reaches a node the rewriter does not understand is decoded
/// before that node.
#[derive(Clone, Debug, Default)]
pub(crate) struct DictionaryRewriteContext {
    scan_columns: BTreeMap<ScanColumnKey, Arc<DictionarySnapshot>>,
    changed: bool,
}

impl DictionaryRewriteContext {
    /// Generate the synthetic dict column name for `table.column`. The
    /// name is shared between the scan-side hidden Int32 slot and any
    /// dict-column references inserted upstream.
    pub(crate) fn dict_column_name(table: &str, column: &str) -> String {
        format!(
            "__nr_dict_{}_{}",
            table.to_ascii_lowercase(),
            column.to_ascii_lowercase()
        )
    }

    pub(crate) fn register_scan_column(
        &mut self,
        key: ScanColumnKey,
        snapshot: DictionarySnapshot,
    ) {
        let snapshot = Arc::new(snapshot);
        self.scan_columns.insert(key, snapshot);
    }

    pub(crate) fn mark_changed(&mut self) {
        self.changed = true;
    }

    pub(crate) fn changed(&self) -> bool {
        self.changed
    }

    /// True if any scan column has a registered snapshot. Used as a
    /// fast-fail gate before running the rewriter.
    pub(crate) fn has_any_scan_column(&self) -> bool {
        !self.scan_columns.is_empty()
    }

    pub(crate) fn dict_eligible_columns_for_scan(
        &self,
        database: &str,
        table: &str,
    ) -> Vec<(String, Arc<DictionarySnapshot>)> {
        let database = database.to_ascii_lowercase();
        let table = table.to_ascii_lowercase();
        self.scan_columns
            .iter()
            .filter(|(key, _)| key.database == database && key.table == table)
            .map(|(key, snapshot)| (key.column.clone(), snapshot.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_snapshot(name: &str) -> DictBinding {
        use crate::engine::dictionary::model::{
            DictionaryOwner, DictionarySnapshot, DictionaryState, DictionaryWatermark,
        };
        use arrow::datatypes::DataType;
        DictBinding {
            dict_column: format!("__nr_dict_{name}"),
            source_column_id: ColumnId::new_for_test(10),
            snapshot: Arc::new(DictionarySnapshot {
                dictionary_id: 1,
                owner: DictionaryOwner::StarRocksTable {
                    database: "db".to_string(),
                    table: "t".to_string(),
                    db_id: 1,
                    table_id: 2,
                },
                column_id: Some(10),
                column_name: name.to_string(),
                data_type: DataType::Utf8,
                version: 1,
                watermark: DictionaryWatermark::Iceberg {
                    snapshot_id: None,
                    schema_id: 0,
                },
                values: vec![],
                null_id: 0,
                state: DictionaryState::Active,
                order_preserving: true,
            }),
        }
    }

    #[test]
    fn dict_column_name_lowercases() {
        assert_eq!(
            DictionaryRewriteContext::dict_column_name("Customer", "Name"),
            "__nr_dict_customer_name"
        );
    }

    #[test]
    fn scope_lookup_is_case_insensitive() {
        let mut scope = DictScope::new();
        scope.insert("Name".to_string(), fake_snapshot("name"));
        assert!(scope.get("name").is_some());
        assert!(scope.get("NAME").is_some());
        assert!(scope.get("other").is_none());
    }
}
