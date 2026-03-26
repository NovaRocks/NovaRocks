use crate::sql::plan::*;

use crate::sql::physical::nodes;
use crate::sql::physical::resolve::{ExprScope, ResolvedTable};

use super::EmitResult;

impl<'a> super::ThriftEmitter<'a> {
    pub(super) fn emit_values(&mut self, _node: ValuesNode) -> Result<EmitResult, String> {
        // For values node (SELECT without FROM), produce a scan on __dual__ table.
        // Look up the real __dual__ table from the catalog to get the actual parquet path.
        let scan_tuple_id = self.alloc_tuple();
        let scan_node_id = self.alloc_node();

        let dual_table = self
            .catalog
            .get_table(self.current_database, "__dual__")
            .or_else(|_| self.catalog.get_table("default", "__dual__"))
            .map_err(|_| "internal error: __dual__ table not found in catalog")?;

        // Register dual table columns as slots so the scan has a valid layout
        for (idx, col) in dual_table.columns.iter().enumerate() {
            let slot_id = self.alloc_slot();
            self.desc_builder.add_slot(
                slot_id,
                scan_tuple_id,
                &col.name,
                &col.data_type,
                col.nullable,
                idx as i32,
            );
        }
        self.desc_builder.add_tuple(scan_tuple_id);

        let resolved = ResolvedTable {
            database: self.current_database.to_string(),
            table: dual_table,
            alias: None,
        };

        let scan_plan_node =
            nodes::build_scan_node(scan_node_id, scan_tuple_id, &resolved, vec![]);
        self.scan_tables.push((scan_node_id, resolved));

        let scope = ExprScope::new();

        Ok(EmitResult {
            plan_nodes: vec![scan_plan_node],
            scope,
            tuple_ids: vec![scan_tuple_id],
        })
    }

    pub(super) fn emit_generate_series(&mut self, node: GenerateSeriesNode) -> Result<EmitResult, String> {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        // Generate the series values
        let mut values = Vec::new();
        let mut v = node.start;
        if node.step > 0 {
            while v <= node.end {
                values.push(v);
                v += node.step;
            }
        } else {
            while v >= node.end {
                values.push(v);
                v += node.step;
            }
        }

        // Build a parquet file
        let col_name = &node.column_name;
        let schema = Arc::new(Schema::new(vec![Field::new(
            col_name,
            ArrowDataType::Int64,
            false,
        )]));
        let col_array = Arc::new(Int64Array::from(values));
        let batch = RecordBatch::try_new(schema, vec![col_array])
            .map_err(|e| format!("build generate_series batch failed: {e}"))?;

        let dir = std::env::temp_dir().join("novarocks_generate_series");
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("create generate_series dir failed: {e}"))?;
        let path = dir.join(format!(
            "gs_{}_{}_{}_{}.parquet",
            node.start, node.end, node.step, self.next_node_id
        ));
        crate::sql::physical::write_parquet_to_path(&path, &batch)?;

        // Build a TableDef and emit as a scan
        let table_def = crate::sql::catalog::TableDef {
            name: node.alias.as_deref().unwrap_or("generate_series").to_string(),
            columns: vec![crate::sql::catalog::ColumnDef {
                name: col_name.clone(),
                data_type: ArrowDataType::Int64,
                nullable: false,
            }],
            storage: crate::sql::catalog::TableStorage::LocalParquetFile { path },
        };

        let scan_node = ScanNode {
            database: self.current_database.to_string(),
            table: table_def,
            alias: node.alias,
            columns: vec![crate::sql::ir::OutputColumn {
                name: col_name.clone(),
                data_type: ArrowDataType::Int64,
                nullable: false,
            }],
            predicates: vec![],
            required_columns: None,
        };

        self.emit_scan(scan_node)
    }
}
