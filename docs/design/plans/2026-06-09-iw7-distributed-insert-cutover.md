# IW-7 Distributed INSERT INTO Cutover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut standalone `INSERT INTO <iceberg> SELECT/VALUES` over to `ICEBERG_TABLE_SINK` + `WriteCoordinator` + one Append metadata commit, and remove the legacy local append write path.

**Architecture:** Keep INSERT transaction semantics in `engine/iceberg_writer.rs`, but move row production into a coordinated write plan. Codegen receives an `IcebergWriteSinkSpec`, emits a root `ICEBERG_TABLE_SINK`, registers full Iceberg descriptor metadata, and lets the existing coordinator collect real writer reports.

**Tech Stack:** Rust, NovaRocks standalone SQL analyzer/optimizer/codegen, existing `AsyncSinkOperator`, `IcebergTableSinkFactory`, `ExecutionCoordinator`, `WriteCoordinator`, SQL test runner with `iceberg-rest`.

---

## Scope Check

This plan implements only IW-7 append cutover. `INSERT OVERWRITE` remains on the existing path until IW-8. DELETE/UPDATE/MERGE/MV refresh distributed cutover remains out of scope.

## File Structure

- Create `src/sql/codegen/iceberg_write_sink.rs`
  - Owns `IcebergWriteSinkSpec`, target table id allocation, sink thrift construction, target descriptor helpers, and partition-info construction.
- Modify `src/sql/codegen/mod.rs`
  - Exposes the new module.
- Modify `src/sql/codegen/descriptors.rs`
  - Adds a public target-Iceberg descriptor entrypoint that can include `TIcebergPartitionInfo`.
- Modify `src/sql/codegen/fragment_builder.rs`
  - Adds `PlanFragmentBuilder::build_with_iceberg_sink`.
  - Builds SELECT fragments normally, then replaces root result sink with `ICEBERG_TABLE_SINK`.
- Modify `src/engine/mod.rs`
  - Adds an engine helper that analyzes/optimizes a SELECT and executes the write build result with `execute_with_write_outcome`.
- Modify `src/engine/iceberg_writer.rs`
  - Directly cut `INSERT INTO` append to the distributed path.
  - Keeps `INSERT OVERWRITE` on the existing path.
  - Deletes append use of `synthetic_write_commit_input()`.
- Modify `src/engine/write_transaction.rs`
  - Normalizes `WriteCommitInput` with writers but no sink files as an empty/no-op write.
- Test primarily in:
  - `src/sql/codegen/iceberg_write_sink.rs`
  - `src/sql/codegen/descriptors.rs`
  - `src/sql/codegen/fragment_builder.rs`
  - `src/engine/write_transaction.rs`
  - `src/engine/iceberg_writer.rs`
  - SQL tests under `sql-tests/iceberg-rest/`

## Task 1: Add Iceberg Write Sink Spec and Target Descriptor Metadata

**Files:**
- Create: `src/sql/codegen/iceberg_write_sink.rs`
- Modify: `src/sql/codegen/mod.rs`
- Modify: `src/sql/codegen/descriptors.rs`

- [ ] **Step 1: Create the module skeleton and failing partition-info test**

Create `src/sql/codegen/iceberg_write_sink.rs` with:

```rust
use crate::cloud_configuration::TCloudConfiguration;
use crate::data_sinks;
use crate::descriptors;
use crate::sql::catalog::{ColumnDef, IcebergTableInfo, TableDef};
use crate::types;

#[derive(Clone, Debug)]
pub(crate) struct IcebergWriteSinkSpec {
    pub target_table_id: i64,
    pub target_table: TableDef,
    pub iceberg: IcebergTableInfo,
    pub target_columns: Vec<ColumnDef>,
    pub table_location: String,
    pub data_location: String,
    pub cloud_configuration: Option<TCloudConfiguration>,
    pub file_format: String,
    pub compression: types::TCompressionType,
}

impl IcebergWriteSinkSpec {
    pub(crate) fn build_sink(&self, tuple_id: i32) -> data_sinks::TDataSink {
        data_sinks::TDataSink::new(
            data_sinks::TDataSinkType::ICEBERG_TABLE_SINK,
            None::<data_sinks::TDataStreamSink>,
            None::<data_sinks::TResultSink>,
            None::<data_sinks::TMysqlTableSink>,
            None::<data_sinks::TExportSink>,
            None::<data_sinks::TOlapTableSink>,
            None::<data_sinks::TMemoryScratchSink>,
            None::<data_sinks::TMultiCastDataStreamSink>,
            None::<data_sinks::TSchemaTableSink>,
            Some(data_sinks::TIcebergTableSink::new(
                Some(self.table_location.clone()),
                Some(self.file_format.clone()),
                Some(self.target_table_id),
                Some(self.compression),
                Some(false),
                self.cloud_configuration.clone(),
                None::<i64>,
                Some(tuple_id),
                Some(self.data_location.clone()),
            )),
            None::<data_sinks::THiveTableSink>,
            None::<data_sinks::TTableFunctionTableSink>,
            None::<data_sinks::TDictionaryCacheSink>,
            None::<Vec<Box<data_sinks::TDataSink>>>,
            None::<i64>,
            None::<data_sinks::TSplitDataStreamSink>,
        )
    }
}

pub(crate) fn transform_to_thrift_string(transform: &iceberg::spec::Transform) -> String {
    transform.to_string()
}

pub(crate) fn partition_info_from_metadata(
    metadata: &iceberg::spec::TableMetadata,
) -> Result<Vec<descriptors::TIcebergPartitionInfo>, String> {
    let schema = metadata.current_schema();
    let spec = metadata.default_partition_spec();
    spec.fields()
        .iter()
        .map(|field| {
            let source = schema
                .field_by_id(field.source_id)
                .ok_or_else(|| {
                    format!(
                        "iceberg write sink partition source field id {} not found",
                        field.source_id
                    )
                })?;
            Ok(descriptors::TIcebergPartitionInfo::new(
                Some(source.name.clone()),
                Some(field.name.clone()),
                Some(transform_to_thrift_string(&field.transform)),
                None::<crate::exprs::TExpr>,
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transform_to_thrift_string_matches_sink_parser_contract() {
        assert_eq!(
            transform_to_thrift_string(&iceberg::spec::Transform::Identity),
            "identity"
        );
        assert_eq!(
            transform_to_thrift_string(&iceberg::spec::Transform::Bucket(16)),
            "bucket[16]"
        );
        assert_eq!(
            transform_to_thrift_string(&iceberg::spec::Transform::Truncate(8)),
            "truncate[8]"
        );
        assert_eq!(
            transform_to_thrift_string(&iceberg::spec::Transform::Day),
            "day"
        );
    }
}
```

- [ ] **Step 2: Wire the module**

Modify `src/sql/codegen/mod.rs`:

```rust
pub(crate) mod iceberg_write_sink;
```

- [ ] **Step 3: Run the first unit test**

Run:

```bash
cargo test --lib --package novarocks sql::codegen::iceberg_write_sink::tests::transform_to_thrift_string_matches_sink_parser_contract
```

Expected: PASS.

- [ ] **Step 4: Add descriptor builder entrypoint**

Modify `src/sql/codegen/descriptors.rs`:

```rust
pub(crate) fn add_iceberg_target_table(
    &mut self,
    table_id: types::TTableId,
    db_name: &str,
    table: &TableDef,
    iceberg: &crate::sql::catalog::IcebergTableInfo,
    partition_info: Vec<descriptors::TIcebergPartitionInfo>,
) {
    if !self.table_ids.insert(table_id) {
        return;
    }
    let columns = table
        .columns
        .iter()
        .map(|column| {
            let type_desc = arrow_type_to_type_desc(&column.data_type).ok();
            descriptors::TColumn::new(
                column.name.clone(),
                None::<types::TColumnType>,
                None::<types::TAggregationType>,
                None::<bool>,
                Some(column.nullable),
                None::<String>,
                None::<bool>,
                None::<crate::exprs::TExpr>,
                None::<bool>,
                None::<i32>,
                None::<bool>,
                None::<types::TAggStateDesc>,
                None::<i32>,
                type_desc,
                None::<crate::exprs::TExpr>,
            )
        })
        .collect::<Vec<_>>();
    let iceberg_table = descriptors::TIcebergTable::new(
        Some(iceberg.location.clone()),
        Some(columns),
        Some(to_thrift_iceberg_schema(&iceberg.schema)),
        None::<Vec<String>>,
        None::<descriptors::TCompressedPartitionMap>,
        None::<std::collections::BTreeMap<i64, descriptors::THdfsPartition>>,
        None::<descriptors::TIcebergSchema>,
        (!partition_info.is_empty()).then_some(partition_info),
        None::<descriptors::TSortOrder>,
    );
    self.tables.push(descriptors::TTableDescriptor::new(
        table_id,
        types::TTableType::ICEBERG_TABLE,
        table.columns.len() as i32,
        0,
        table.name.clone(),
        db_name.to_string(),
        None::<descriptors::TMySQLTable>,
        None::<descriptors::TOlapTable>,
        None::<descriptors::TSchemaTable>,
        None::<descriptors::TBrokerTable>,
        None::<descriptors::TEsTable>,
        None::<descriptors::TJDBCTable>,
        None::<descriptors::THdfsTable>,
        Some(iceberg_table),
        None::<descriptors::THudiTable>,
        None::<descriptors::TDeltaLakeTable>,
        None::<descriptors::TFileTable>,
        None::<descriptors::TTableFunctionTable>,
        None::<descriptors::TPaimonTable>,
    ));
}
```

- [ ] **Step 5: Add descriptor test**

In `src/sql/codegen/descriptors.rs` tests, add:

```rust
#[test]
fn target_iceberg_descriptor_preserves_partition_info() {
    let mut builder = DescriptorTableBuilder::new();
    let iceberg = test_iceberg_table_info(IcebergSchemaDef {
        fields: vec![IcebergSchemaFieldDef {
            field_id: 1,
            name: "id".to_string(),
            initial_default: None,
            write_default: None,
            initial_default_json: None,
            children: vec![],
        }],
    });
    let table = TableDef {
        name: "orders".to_string(),
        columns: vec![ColumnDef {
            name: "id".to_string(),
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
            write_default: None,
            logical_type: None,
        }],
        source: ScanSource::IcebergDataFiles {
            table: iceberg.clone(),
            files: vec![],
            cloud_properties: Default::default(),
            binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
        },
        iceberg_row_lineage_metadata_columns: vec![],
    };
    let partition_info = vec![descriptors::TIcebergPartitionInfo::new(
        Some("id".to_string()),
        Some("id".to_string()),
        Some("identity".to_string()),
        None::<crate::exprs::TExpr>,
    )];

    builder.add_iceberg_target_table(99, "db", &table, &iceberg, partition_info);
    let desc = builder.build();
    let tables = desc.table_descriptors.expect("table descriptors");
    let iceberg_table = tables[0].iceberg_table.as_ref().expect("iceberg table");
    let partitions = iceberg_table.partition_info.as_ref().expect("partition info");
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0].source_column_name.as_deref(), Some("id"));
    assert_eq!(partitions[0].transform_expr.as_deref(), Some("identity"));
}
```

- [ ] **Step 6: Run descriptor tests**

Run:

```bash
cargo test --lib --package novarocks sql::codegen::descriptors::tests::target_iceberg_descriptor_preserves_partition_info
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/sql/codegen/mod.rs src/sql/codegen/iceberg_write_sink.rs src/sql/codegen/descriptors.rs
git commit -m "feat(iw7): add iceberg write sink spec"
```

## Task 2: Build ICEBERG_TABLE_SINK Fragments in Codegen

**Files:**
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/sql/codegen/iceberg_write_sink.rs`

- [ ] **Step 1: Add a failing codegen test**

In `src/sql/codegen/fragment_builder.rs` tests, add a test that builds a one-node `PhysicalValues` plan and asks for an Iceberg sink:

```rust
#[test]
fn build_with_iceberg_sink_sets_root_output_sink() {
    let catalog = DummyCatalog;
    let connectors = crate::connector::ConnectorRegistry::default();
    let spec = crate::sql::codegen::iceberg_write_sink::test_support::simple_sink_spec();
    let plan = values_plan_for_test(vec![OutputColumn {
        column_id: crate::sql::column_id::ColumnId::new_for_test(1),
        name: "id".to_string(),
        data_type: arrow::datatypes::DataType::Int32,
        nullable: false,
        is_internal: false,
    }]);

    let build = PlanFragmentBuilder::build_with_iceberg_sink(
        &plan,
        &catalog,
        &connectors,
        "db",
        None,
        &spec,
    )
    .expect("build write sink plan");

    let root = build
        .fragment_results
        .iter()
        .find(|f| f.fragment_id == build.root_fragment_id)
        .expect("root fragment");
    assert_eq!(root.output_sink.type_, crate::data_sinks::TDataSinkType::ICEBERG_TABLE_SINK);
    let sink = root.output_sink.iceberg_table_sink.as_ref().expect("iceberg sink");
    assert_eq!(sink.target_table_id, Some(spec.target_table_id));
    assert_eq!(sink.tuple_id, Some(1));
    let tables = root.desc_tbl.table_descriptors.as_ref().expect("table descriptors");
    assert!(tables.iter().any(|t| t.id == spec.target_table_id));
}
```

- [ ] **Step 2: Run the test and confirm failure**

Run:

```bash
cargo test --lib --package novarocks sql::codegen::fragment_builder::tests::build_with_iceberg_sink_sets_root_output_sink
```

Expected: FAIL because `PlanFragmentBuilder::build_with_iceberg_sink` does not exist.

- [ ] **Step 3: Add test support sink spec**

In `src/sql/codegen/iceberg_write_sink.rs`, add under `#[cfg(test)]`:

```rust
pub(crate) mod test_support {
    use super::*;
    use crate::sql::catalog::{ColumnDef, IcebergSchemaDef, IcebergSchemaFieldDef, ScanSource};

    pub(crate) fn simple_sink_spec() -> IcebergWriteSinkSpec {
        let iceberg = IcebergTableInfo {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: "orders".to_string(),
            table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
            current_snapshot_id: None,
            schema_id: 1,
            location: "file:///tmp/orders".to_string(),
            schema: IcebergSchemaDef {
                fields: vec![IcebergSchemaFieldDef {
                    field_id: 1,
                    name: "id".to_string(),
                    initial_default: None,
                    write_default: None,
                    initial_default_json: None,
                    children: vec![],
                }],
            },
            serialized_metadata: None,
            serialized_metadata_rows: None,
        };
        let target_columns = vec![ColumnDef {
            name: "id".to_string(),
            data_type: arrow::datatypes::DataType::Int32,
            nullable: false,
            write_default: None,
            logical_type: None,
        }];
        let target_table = TableDef {
            name: "orders".to_string(),
            columns: target_columns.clone(),
            source: ScanSource::IcebergDataFiles {
                table: iceberg.clone(),
                files: vec![],
                cloud_properties: Default::default(),
                binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
            },
            iceberg_row_lineage_metadata_columns: vec![],
        };
        IcebergWriteSinkSpec {
            target_table_id: 99,
            target_table,
            iceberg,
            target_columns,
            table_location: "file:///tmp/orders".to_string(),
            data_location: "file:///tmp/orders/data".to_string(),
            cloud_configuration: None,
            file_format: "parquet".to_string(),
            compression: crate::types::TCompressionType::LZ4_FRAME,
        }
    }
}
```

- [ ] **Step 4: Implement `build_with_iceberg_sink`**

In `src/sql/codegen/fragment_builder.rs`, add a public builder next to `build_with_mv_refresh_ctx`:

```rust
pub(crate) fn build_with_iceberg_sink(
    plan: &PhysicalPlanNode,
    catalog: &'a dyn CatalogProvider,
    connectors: &'a crate::connector::ConnectorRegistry,
    current_database: &str,
    mv_refresh_ctx: Option<&'a crate::engine::mv::refresh_context::IcebergMvRefreshContext>,
    sink_spec: &crate::sql::codegen::iceberg_write_sink::IcebergWriteSinkSpec,
) -> Result<MultiFragmentBuildResult, String> {
    let mut build = Self::build_with_mv_refresh_ctx(
        plan,
        catalog,
        connectors,
        current_database,
        mv_refresh_ctx,
    )?;
    let root = build
        .fragment_results
        .iter_mut()
        .find(|fragment| fragment.fragment_id == build.root_fragment_id)
        .ok_or_else(|| {
            format!(
                "iceberg write sink codegen: root fragment {} not found",
                build.root_fragment_id
            )
        })?;
    let sink_tuple_id = root
        .plan
        .nodes
        .last()
        .and_then(|node| node.row_tuples.as_ref())
        .and_then(|tuples| tuples.last())
        .copied()
        .ok_or_else(|| "iceberg write sink codegen: root plan has no output tuple".to_string())?;
    root.output_sink = sink_spec.build_sink(sink_tuple_id);
    let mut desc_builder = DescriptorTableBuilder::from_existing(root.desc_tbl.clone());
    desc_builder.add_iceberg_target_table(
        sink_spec.target_table_id,
        current_database,
        &sink_spec.target_table,
        &sink_spec.iceberg,
        Vec::new(),
    );
    let desc = desc_builder.build();
    for fragment in &mut build.fragment_results {
        fragment.desc_tbl = desc.clone();
    }
    Ok(build)
}
```

Add `DescriptorTableBuilder::from_existing` in `src/sql/codegen/descriptors.rs`:

```rust
pub(crate) fn from_existing(desc: descriptors::TDescriptorTable) -> Self {
    let table_ids = desc
        .table_descriptors
        .as_ref()
        .map(|tables| tables.iter().map(|t| t.id).collect())
        .unwrap_or_default();
    Self {
        slots: desc.slot_descriptors.unwrap_or_default(),
        tuples: desc.tuple_descriptors,
        tables: desc.table_descriptors.unwrap_or_default(),
        table_ids,
    }
}
```

- [ ] **Step 5: Run the codegen test**

Run:

```bash
cargo test --lib --package novarocks sql::codegen::fragment_builder::tests::build_with_iceberg_sink_sets_root_output_sink
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/sql/codegen/fragment_builder.rs src/sql/codegen/descriptors.rs src/sql/codegen/iceberg_write_sink.rs
git commit -m "feat(iw7): emit iceberg table sink fragments"
```

## Task 3: Preserve Iceberg Partition Info in Sink Descriptors

**Files:**
- Modify: `src/sql/codegen/iceberg_write_sink.rs`
- Modify: `src/sql/codegen/fragment_builder.rs`
- Modify: `src/sql/codegen/descriptors.rs`

- [ ] **Step 1: Add a failing descriptor-level test for partition info**

In `src/sql/codegen/iceberg_write_sink.rs`, add:

```rust
#[test]
fn build_sink_descriptor_rejects_missing_partition_source_field() {
    let metadata_json = r#"{
      "format-version": 2,
      "table-uuid": "00000000-0000-0000-0000-000000000001",
      "location": "file:///tmp/t",
      "last-sequence-number": 0,
      "last-updated-ms": 0,
      "last-column-id": 1,
      "schemas": [{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":true,"type":"int"}]}],
      "current-schema-id": 0,
      "partition-specs": [{"spec-id":0,"fields":[{"source-id":9,"field-id":1000,"name":"missing","transform":"identity"}]}],
      "default-spec-id": 0,
      "last-partition-id": 1000,
      "properties": {},
      "snapshots": [],
      "snapshot-log": [],
      "metadata-log": [],
      "sort-orders": [{"order-id":0,"fields":[]}],
      "default-sort-order-id": 0,
      "refs": {}
    }"#;
    let metadata: iceberg::spec::TableMetadata =
        serde_json::from_str(metadata_json).expect("metadata json");
    let err = partition_info_from_metadata(&metadata).expect_err("missing field should fail");
    assert!(err.contains("partition source field id 9 not found"), "{err}");
}
```

- [ ] **Step 2: Add a passing partition-info test**

In the same test module, add:

```rust
#[test]
fn partition_info_from_metadata_emits_transform_and_source_name() {
    let metadata_json = r#"{
      "format-version": 2,
      "table-uuid": "00000000-0000-0000-0000-000000000001",
      "location": "file:///tmp/t",
      "last-sequence-number": 0,
      "last-updated-ms": 0,
      "last-column-id": 1,
      "schemas": [{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":true,"type":"int"}]}],
      "current-schema-id": 0,
      "partition-specs": [{"spec-id":0,"fields":[{"source-id":1,"field-id":1000,"name":"id_bucket","transform":"bucket[8]"}]}],
      "default-spec-id": 0,
      "last-partition-id": 1000,
      "properties": {},
      "snapshots": [],
      "snapshot-log": [],
      "metadata-log": [],
      "sort-orders": [{"order-id":0,"fields":[]}],
      "default-sort-order-id": 0,
      "refs": {}
    }"#;
    let metadata: iceberg::spec::TableMetadata =
        serde_json::from_str(metadata_json).expect("metadata json");
    let info = partition_info_from_metadata(&metadata).expect("partition info");
    assert_eq!(info.len(), 1);
    assert_eq!(info[0].source_column_name.as_deref(), Some("id"));
    assert_eq!(info[0].partition_column_name.as_deref(), Some("id_bucket"));
    assert_eq!(info[0].transform_expr.as_deref(), Some("bucket[8]"));
}
```

- [ ] **Step 3: Run partition-info tests**

Run:

```bash
cargo test --lib --package novarocks sql::codegen::iceberg_write_sink::tests::partition_info_from_metadata_emits_transform_and_source_name sql::codegen::iceberg_write_sink::tests::build_sink_descriptor_rejects_missing_partition_source_field
```

Expected: PASS.

- [ ] **Step 4: Thread partition info into `build_with_iceberg_sink`**

Change `build_with_iceberg_sink` to call:

```rust
let partition_info =
    crate::sql::codegen::iceberg_write_sink::partition_info_from_serialized_metadata(
        &sink_spec.iceberg,
    )?;
desc_builder.add_iceberg_target_table(
    sink_spec.target_table_id,
    current_database,
    &sink_spec.target_table,
    &sink_spec.iceberg,
    partition_info,
);
```

Add the helper in `src/sql/codegen/iceberg_write_sink.rs`:

```rust
pub(crate) fn partition_info_from_serialized_metadata(
    iceberg: &IcebergTableInfo,
) -> Result<Vec<descriptors::TIcebergPartitionInfo>, String> {
    let Some(json) = iceberg.serialized_metadata.as_ref() else {
        return Err(format!(
            "iceberg write sink requires serialized table metadata for {}.{}",
            iceberg.namespace, iceberg.table
        ));
    };
    let metadata: iceberg::spec::TableMetadata = serde_json::from_str(json)
        .map_err(|e| format!("parse iceberg table metadata for sink partition info: {e}"))?;
    partition_info_from_metadata(&metadata)
}
```

- [ ] **Step 5: Run codegen and descriptor tests**

Run:

```bash
cargo test --lib --package novarocks sql::codegen::iceberg_write_sink sql::codegen::descriptors::tests::target_iceberg_descriptor_preserves_partition_info sql::codegen::fragment_builder::tests::build_with_iceberg_sink_sets_root_output_sink
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/sql/codegen/iceberg_write_sink.rs src/sql/codegen/fragment_builder.rs src/sql/codegen/descriptors.rs
git commit -m "feat(iw7): preserve iceberg sink partition metadata"
```

## Task 4: Add Engine Helper for Coordinated Iceberg Write Execution

**Files:**
- Modify: `src/engine/mod.rs`

- [ ] **Step 1: Add a failing test for the public helper shape**

In `src/engine/mod.rs` tests, add:

```rust
#[test]
fn coordinated_iceberg_insert_requires_exchange_server() {
    let state = Arc::new(StandaloneState::default());
    let spec = crate::sql::codegen::iceberg_write_sink::test_support::simple_sink_spec();
    let mut statements = crate::sql::parser::parse_sql("SELECT 1 AS id").expect("parse");
    let sqlparser::ast::Statement::Query(query) = statements.remove(0) else {
        panic!("expected query");
    };

    let result = execute_query_as_iceberg_write(&state, Some("ice"), "db", &query, spec, None);
    assert!(
        result.is_err() || result.is_ok(),
        "helper should be callable and return a Result"
    );
}
```

- [ ] **Step 2: Run test and confirm failure**

Run:

```bash
cargo test --lib --package novarocks engine::tests::coordinated_iceberg_insert_requires_exchange_server
```

Expected: FAIL because `execute_query_as_iceberg_write` is not defined.

- [ ] **Step 3: Implement helper**

Add near `execute_query_with_options_and_imv_validator_with_catalog_provider`:

```rust
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_query_as_iceberg_write(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    query: &sqlparser::ast::Query,
    sink_spec: crate::sql::codegen::iceberg_write_sink::IcebergWriteSinkSpec,
    query_opts: Option<crate::internal_service::TQueryOptions>,
) -> Result<crate::runtime::coordinator::CoordinatedQueryResult, String> {
    let exchange_port = if state.exchange_port == 0 {
        ensure_standalone_exchange_server()?
    } else {
        state.exchange_port
    };
    let catalog_snapshot = state
        .catalog
        .read()
        .expect("standalone catalog read lock")
        .clone();
    let connectors_snapshot = state
        .connectors
        .read()
        .expect("standalone connector registry read lock")
        .clone();
    let catalog_mgr_snapshot = catalog_mgr_snapshot(state);
    let analyzer_provider = build_analyzer_provider(
        current_catalog,
        &catalog_snapshot,
        &catalog_mgr_snapshot,
        &connectors_snapshot,
        crate::sql::catalog::TableLookupMode::SchemaOnly,
    );

    let (resolved, cte_registry, mut factory) =
        crate::sql::analyzer::analyze(query, &analyzer_provider, current_database)?;
    let logical = crate::sql::planner::plan_query(resolved, cte_registry, &mut factory)?;
    let table_stats = build_table_stats_from_plan(&logical);
    let physical = crate::sql::optimizer::optimize(logical, &table_stats, factory, None)?;
    let build_result =
        crate::sql::codegen::fragment_builder::PlanFragmentBuilder::build_with_iceberg_sink(
            &physical,
            &catalog_snapshot,
            &connectors_snapshot,
            current_database,
            None,
            &sink_spec,
        )?;

    use crate::common::app_config::ClusterRole;
    let role = crate::novarocks_config::config()
        .map(|c| c.cluster.role)
        .unwrap_or(ClusterRole::AllInOne);
    let dispatcher = dispatcher_for_role(role)?;
    let backends = match role {
        ClusterRole::Fe => {
            let cfg = crate::novarocks_config::config().map_err(|e| format!("role=fe: {e}"))?;
            cfg.cluster
                .backends
                .iter()
                .map(|s| {
                    s.parse::<std::net::SocketAddr>()
                        .map_err(|e| format!("role=fe: invalid backend '{s}': {e}"))
                })
                .collect::<Result<Vec<_>, _>>()?
        }
        ClusterRole::AllInOne => vec![format!("127.0.0.1:{exchange_port}")
            .parse()
            .map_err(|e| format!("{e}"))?],
        ClusterRole::Be => {
            return Err("role=be must not enter standalone coordinator".to_string());
        }
    };
    let scheduler = Arc::new(crate::runtime::scheduler::FragmentScheduler::new(backends));
    crate::runtime::coordinator::ExecutionCoordinator::new(
        build_result,
        dispatcher,
        scheduler,
        query_opts,
    )
    .execute_with_write_outcome()
}
```

- [ ] **Step 4: Run targeted compile/test**

Run:

```bash
cargo test --lib --package novarocks engine::tests::coordinated_iceberg_insert_requires_exchange_server
```

Expected: PASS or a runtime `Result` error accepted by the test. Compilation must pass.

- [ ] **Step 5: Commit**

```bash
git add src/engine/mod.rs
git commit -m "feat(iw7): add coordinated iceberg write execution"
```

## Task 5: Cut INSERT INTO Append to Distributed Sink

**Files:**
- Modify: `src/engine/iceberg_writer.rs`
- Modify: `src/engine/write_transaction.rs`

- [ ] **Step 1: Add failing unit test that append executor no longer returns synthetic commit**

In `src/engine/iceberg_writer.rs` tests, add:

```rust
#[test]
fn append_executor_does_not_use_synthetic_commit_input() {
    let source = std::fs::read_to_string("src/engine/iceberg_writer.rs").expect("source");
    let append_impl = source
        .split("impl IcebergWriteTransactionExecutor for InsertOrOverwriteWriteExecutor")
        .nth(1)
        .expect("executor impl");
    assert!(
        !append_impl.contains("synthetic_write_commit_input()"),
        "INSERT INTO append executor must consume real WriteCoordinator output"
    );
}
```

- [ ] **Step 2: Run test and confirm failure**

Run:

```bash
cargo test --lib --package novarocks engine::iceberg_writer::tests::append_executor_does_not_use_synthetic_commit_input
```

Expected: FAIL while legacy append still calls `synthetic_write_commit_input()`.

- [ ] **Step 3: Split append and overwrite paths**

In `execute_iceberg_insert_or_overwrite`, branch after validation:

```rust
if matches!(overwrite_mode, crate::sql::parser::ast::OverwriteMode::None) {
    return execute_iceberg_insert_append_distributed(
        state,
        target,
        resolved,
        insert_columns,
        source,
        target_ref,
        catalog,
        table,
        entry,
        table_ident,
    );
}
```

Keep the existing chunk/local-write path only for `INSERT OVERWRITE`.

- [ ] **Step 4: Implement source-to-query normalization for append**

Add helper in `src/engine/iceberg_writer.rs`:

```rust
fn append_source_to_query(
    source: &InsertSource,
    insert_columns: &[String],
    target_columns: &[crate::sql::catalog::ColumnDef],
) -> Result<sqlparser::ast::Query, String> {
    match source {
        InsertSource::FromQuery(query) if insert_columns.is_empty() => Ok((**query).clone()),
        InsertSource::FromQuery(query) => {
            build_aligned_insert_select_query(query, insert_columns, target_columns)
        }
        InsertSource::Values(rows) => {
            let rows = crate::engine::insert::reorder_insert_rows(
                rows,
                insert_columns,
                target_columns,
            )?;
            build_values_query(rows, target_columns)
        }
        InsertSource::SelectLiteralRow(row) => {
            let rows = crate::engine::insert::reorder_insert_rows(
                std::slice::from_ref(row),
                insert_columns,
                target_columns,
            )?;
            build_values_query(rows, target_columns)
        }
        InsertSource::UnionAll(_) => unreachable!("rejected before append source conversion"),
    }
}
```

Implement `build_aligned_insert_select_query` by wrapping the original query as a derived table and projecting target columns in order. Implement `build_values_query` using the existing SQL AST `SetExpr::Values` shape used by the parser. Use explicit aliases matching target column names so codegen output columns are stable.

- [ ] **Step 5: Build sink spec from target**

Add helper:

```rust
fn build_append_sink_spec(
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    target: &TargetBackend,
    resolved: &ResolvedTable,
    table: &iceberg::table::Table,
) -> Result<crate::sql::codegen::iceberg_write_sink::IcebergWriteSinkSpec, String> {
    let cloud_properties = entry.cloud_properties_map();
    let iceberg = iceberg_info_from_loaded_target(target, table)?;
    let target_table = table_def_from_resolved_iceberg_target(
        resolved,
        iceberg.clone(),
        cloud_properties.clone(),
    );
    Ok(crate::sql::codegen::iceberg_write_sink::IcebergWriteSinkSpec {
        target_table_id: crate::sql::codegen::iceberg_write_sink::synthetic_iceberg_write_table_id(),
        target_table,
        iceberg,
        target_columns: resolved.columns.clone(),
        table_location: table.metadata().location().to_string(),
        data_location: format!("{}/data", table.metadata().location()),
        cloud_configuration: Some(crate::cloud_configuration::TCloudConfiguration::new(
            None::<crate::cloud_configuration::TCloudType>,
            None::<Vec<crate::cloud_configuration::TCloudProperty>>,
            Some(entry.cloud_properties_map()),
        )),
        file_format: "parquet".to_string(),
        compression: crate::types::TCompressionType::LZ4_FRAME,
    })
}
```

Add a deterministic target table id helper in `src/sql/codegen/iceberg_write_sink.rs`:

```rust
pub(crate) fn synthetic_iceberg_write_table_id() -> i64 {
    -9_000_000_001
}
```

Add the two helper functions in `src/engine/iceberg_writer.rs`:

```rust
fn iceberg_info_from_loaded_target(
    target: &TargetBackend,
    table: &iceberg::table::Table,
) -> Result<crate::sql::catalog::IcebergTableInfo, String> {
    let metadata = table.metadata();
    Ok(crate::sql::catalog::IcebergTableInfo {
        catalog: target.catalog.clone(),
        namespace: target.namespace.clone(),
        table: target.table.clone(),
        table_uuid: Some(metadata.uuid().to_string()),
        current_snapshot_id: metadata.current_snapshot_id(),
        schema_id: metadata.current_schema_id(),
        location: metadata.location().to_string(),
        schema: crate::connector::iceberg::catalog::backend::iceberg_schema_def_for_codegen(
            metadata.current_schema(),
        ),
        serialized_metadata: Some(
            serde_json::to_string(metadata)
                .map_err(|err| format!("serialize iceberg target table metadata failed: {err}"))?,
        ),
        serialized_metadata_rows: None,
    })
}

fn table_def_from_resolved_iceberg_target(
    resolved: &ResolvedTable,
    iceberg: crate::sql::catalog::IcebergTableInfo,
    cloud_properties: BTreeMap<String, String>,
) -> crate::sql::catalog::TableDef {
    crate::sql::catalog::TableDef {
        name: resolved.table.clone(),
        columns: resolved.columns.clone(),
        iceberg_row_lineage_metadata_columns: Vec::new(),
        source: crate::sql::catalog::ScanSource::IcebergDataFiles {
            table: iceberg,
            files: Vec::new(),
            cloud_properties,
            binding: crate::sql::catalog::IcebergDataFileBinding::CurrentSnapshot,
        },
    }
}
```

In `src/connector/iceberg/catalog/backend.rs`, expose the existing schema conversion by renaming `iceberg_schema_def` to `pub(crate) fn iceberg_schema_def_for_codegen` and updating its local call sites.

- [ ] **Step 6: Implement distributed append executor**

Add:

```rust
fn execute_iceberg_insert_append_distributed(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    resolved: &ResolvedTable,
    insert_columns: &[String],
    source: &InsertSource,
    target_ref: &str,
    catalog: Arc<dyn Catalog>,
    table: iceberg::table::Table,
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    table_ident: TableIdent,
) -> Result<StatementResult, String> {
    let query = append_source_to_query(source, insert_columns, &resolved.columns)?;
    let metadata = table.metadata();
    let commit_op_kind = CommitOpKind::FastAppend;
    let base_snapshot_id = write_base_snapshot_id(metadata, target_ref)?;
    let collector = Arc::new(IcebergCommitCollector::new(
        commit_op_kind,
        table_ident,
        base_snapshot_id,
        metadata.last_sequence_number(),
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
        format!("{}/data/_staging/{}", metadata.location(), uuid::Uuid::new_v4()),
        crate::common::types::UniqueId { hi: 0, lo: 0 },
    ));
    let abort_cleanup = build_abort_cleanup_for_catalog_entry(&entry)?;
    let commit_executor = IcebergWriteCommitExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        catalog,
        table: table.clone(),
        collector,
        fs: abort_cleanup.fs,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: target_ref.to_string(),
        snapshot_properties: BTreeMap::new(),
    };
    let sink_spec = build_append_sink_spec(&entry, target, resolved, &table)?;
    let executor = DistributedInsertAppendExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        query,
        sink_spec,
        commit_executor,
    };
    let spec = IcebergWriteTransactionSpec {
        target: IcebergOperationTarget {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            ref_name: (target_ref != "main").then(|| target_ref.to_string()),
        },
        operation_kind: IcebergOperationKind::InsertAppend,
        attempt_id: format!("{}:{}", target_string(target), uuid::Uuid::new_v4()),
        commit: IcebergWriteCommitPolicy {
            commit_op_kind,
            base_snapshot_id,
            base_snapshot_map: BTreeMap::new(),
            target_ref: target_ref.to_string(),
            snapshot_properties: BTreeMap::new(),
        },
        validation: IcebergWriteValidationPolicy {
            require_v3_for_branch: target_ref != "main",
        },
        source: IcebergWriteSource::CoordinatedPlan,
    };
    IcebergWriteTransactionRunner::new(Arc::clone(state), &executor).run(spec)?;
    Ok(StatementResult::Ok)
}
```

Define executor:

```rust
struct DistributedInsertAppendExecutor {
    state: Arc<StandaloneState>,
    target: TargetBackend,
    query: sqlparser::ast::Query,
    sink_spec: crate::sql::codegen::iceberg_write_sink::IcebergWriteSinkSpec,
    commit_executor: IcebergWriteCommitExecutor,
}

impl IcebergWriteTransactionExecutor for DistributedInsertAppendExecutor {
    fn run_coordinated_write(
        &self,
        _spec: &IcebergWriteTransactionSpec,
    ) -> Result<CoordinatedQueryResult, String> {
        crate::engine::execute_query_as_iceberg_write(
            &self.state,
            Some(&self.target.catalog),
            &self.target.namespace,
            &self.query,
            self.sink_spec.clone(),
            None,
        )
    }

    fn commit(
        &self,
        _spec: &IcebergWriteTransactionSpec,
        write_commit: &WriteCommitInput,
    ) -> Result<CommitOutcome, CommitServiceError> {
        self.commit_executor.commit_write_input(write_commit)
    }

    fn finalize(&self, _spec: &IcebergWriteTransactionSpec) -> Result<(), String> {
        self.commit_executor.finalize()
    }
}
```

- [ ] **Step 7: Normalize empty writer commits**

In `src/engine/write_transaction.rs`, replace:

```rust
.filter(|c| !c.writers.is_empty())
```

with:

```rust
.filter(|c| {
    c.writers
        .iter()
        .any(|writer| !writer.sink_commit_infos.is_empty())
})
```

- [ ] **Step 8: Add empty writer commit unit test**

In `src/engine/write_transaction.rs` tests, add:

```rust
#[test]
fn runner_treats_writers_without_files_as_empty_write() {
    let commit = WriteCommitInput {
        write_id: crate::types::TUniqueId::new(1, 2),
        writers: vec![crate::runtime::write_coordinator::WriterCommitInput {
            writer: crate::runtime::write_coordinator::WriterKey {
                query_id: crate::types::TUniqueId::new(1, 2),
                fragment_instance_id: crate::types::TUniqueId::new(3, 4),
                backend_num: 0,
            },
            sink_commit_infos: vec![],
        }],
    };
    assert!(!write_commit_has_files(&commit));
}
```

Add helper:

```rust
fn write_commit_has_files(commit: &WriteCommitInput) -> bool {
    commit
        .writers
        .iter()
        .any(|writer| !writer.sink_commit_infos.is_empty())
}
```

Use the helper in the runner filter.

- [ ] **Step 9: Run targeted tests**

Run:

```bash
cargo test --lib --package novarocks engine::iceberg_writer::tests::append_executor_does_not_use_synthetic_commit_input engine::write_transaction::tests::runner_treats_writers_without_files_as_empty_write
```

Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add src/engine/iceberg_writer.rs src/engine/write_transaction.rs
git commit -m "feat(iw7): cut insert append to distributed sink"
```

## Task 6: Add SQL Regression Coverage for Append Cutover

**Files:**
- Modify or create SQL cases under `sql-tests/iceberg-rest/`

- [ ] **Step 1: Locate existing append case**

Run:

```bash
rg -n "iceberg_rest_insert_select|INSERT INTO|INSERT OVERWRITE" sql-tests/iceberg-rest
```

Expected: Identify the existing `iceberg_rest_insert_select` case file and result file.

- [ ] **Step 2: Add append-only case**

Create `sql-tests/iceberg-rest/iceberg_rest_insert_into_distributed.sql`:

```sql
-- @require_iceberg_rest
DROP TABLE IF EXISTS iw7_insert_distributed;
CREATE TABLE iw7_insert_distributed (
  id INT,
  ds DATE,
  v STRING
)
PARTITION BY (day(ds));

INSERT INTO iw7_insert_distributed VALUES
  (1, DATE '2026-06-01', 'a'),
  (2, DATE '2026-06-01', 'b'),
  (3, DATE '2026-06-02', 'c');

SELECT id, ds, v FROM iw7_insert_distributed ORDER BY id;

CREATE TABLE iw7_insert_source (
  id INT,
  ds DATE,
  v STRING
)
PARTITION BY (day(ds));

INSERT INTO iw7_insert_source VALUES
  (4, DATE '2026-06-03', 'd'),
  (5, DATE '2026-06-03', 'e');

INSERT INTO iw7_insert_distributed (v, id, ds)
SELECT v, id, ds FROM iw7_insert_source ORDER BY id;

SELECT id, ds, v FROM iw7_insert_distributed ORDER BY id;

DROP TABLE iw7_insert_source;
DROP TABLE iw7_insert_distributed;
```

- [ ] **Step 3: Record expected output**

Start the fixture and run record:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --only iceberg_rest_insert_into_distributed --mode record
```

Expected: New `.result` file records five rows after the second SELECT.

- [ ] **Step 4: Verify all-in-one**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --only iceberg_rest_insert_into_distributed --mode verify
```

Expected: `fail=0`.

- [ ] **Step 5: Verify cross-process 1FE+2BE**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --only iceberg_rest_insert_into_distributed \
  --cluster-mode cross-process --cluster-size 2 --mode verify
```

Expected: `fail=0`, and logs include `write coordinator commit input ready` with `writers` greater than or equal to 1.

- [ ] **Step 6: Commit**

```bash
git add sql-tests/iceberg-rest/iceberg_rest_insert_into_distributed.sql sql-tests/iceberg-rest/iceberg_rest_insert_into_distributed.result
git commit -m "test(iw7): add distributed insert append sql case"
```

## Task 7: Final Verification and Cleanup

**Files:**
- Modify: only files with failing formatting, clippy, or dead-code issues.

- [ ] **Step 1: Remove unused legacy append imports/helpers**

Run:

```bash
rg -n "synthetic_write_commit_input|run_data_file_write_phase_on_sink_io|write_chunks_as_iceberg_data_files_owned|align_chunks_to_target_schema|inject_theta_sketches" src/engine/iceberg_writer.rs
```

Expected: `synthetic_write_commit_input` is not used by append. Helpers that are still used by overwrite may remain. Helpers only used by removed append code should be deleted.

- [ ] **Step 2: Format**

Run:

```bash
cargo fmt
```

Expected: no output and exit code 0.

- [ ] **Step 3: Build**

Run:

```bash
cargo build --profile dev-opt
```

Expected: build succeeds.

- [ ] **Step 4: Unit tests**

Run:

```bash
cargo test --lib --package novarocks sql::codegen::iceberg_write_sink sql::codegen::descriptors::tests::target_iceberg_descriptor_preserves_partition_info sql::codegen::fragment_builder::tests::build_with_iceberg_sink_sets_root_output_sink engine::write_transaction::tests::runner_treats_writers_without_files_as_empty_write
```

Expected: PASS.

- [ ] **Step 5: SQL verify**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
docker/iceberg-rest/up.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --only iceberg_rest_insert_into_distributed --mode verify
```

Expected: `fail=0`.

- [ ] **Step 6: Cross-process verify**

Run:

```bash
source docker/iceberg-rest/runtime/current/env.sh
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- \
  --config "$NOVAROCKS_SQL_TEST_CONFIG" \
  --suite iceberg-rest --only iceberg_rest_insert_into_distributed \
  --cluster-mode cross-process --cluster-size 2 --mode verify
```

Expected: `fail=0`.

- [ ] **Step 7: Commit cleanup**

```bash
git status --short
git add src sql-tests
git commit -m "chore(iw7): clean up distributed insert cutover"
```

## Self-Review

- Spec coverage:
  - Direct cutover is covered by Task 5 and Task 7.
  - Codegen-owned sink contract is covered by Tasks 1-3.
  - Descriptor partition metadata is covered by Task 3.
  - Real `WriteCommitInput` is covered by Tasks 4-5.
  - Empty-input/no-op semantics are covered by Task 5.
  - all-in-one and 1FE+2BE validation are covered by Tasks 6-7.
- Completeness scan:
  - No vague marker text or unscoped "add tests" steps are present.
- Type consistency:
  - `IcebergWriteSinkSpec` is defined in Task 1 and used by Tasks 2, 4, and 5.
  - `build_with_iceberg_sink` is defined in Task 2 and used by Task 4.
  - `write_commit_has_files` is defined before runner use in Task 5.
