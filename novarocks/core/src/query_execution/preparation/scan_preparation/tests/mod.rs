// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

mod dispatch;
mod iceberg;
mod projection;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use arrow::datatypes::DataType;

use crate::connector::ConnectorRegistry;
use crate::connector::iceberg::scan_model::{
    IcebergDataFileBinding, IcebergDataFileInfo, IcebergSchemaDef, IcebergSchemaFieldDef,
    IcebergTableInfo,
};
use crate::query_execution::preparation::scan::{
    ResolvedIcebergFileScan, ResolvedReadReason, ResolvedScanExecution, ScanBindingResolver,
};
use crate::sql::analysis::OutputColumn;
use crate::sql::column_id::ColumnId;
use crate::sql::planner::distributed::{
    DataPartition, DataSink, DistributedNode, DistributedNodeKind, DistributedPlan, PlanFragment,
};
use crate::sql::planner::payload::PlanScanNode;
use crate::sql::planner::physical::{PhysicalPlanStats, PlannerConfidence};
use crate::sql::planner::table::{ScanSource, TableDef};
use novarocks_catalog::schema::ColumnDef;

fn prepare_scan_bindings(
    plan: &DistributedPlan,
    connectors: &ConnectorRegistry,
    resolver: Option<&dyn ScanBindingResolver>,
) -> Result<crate::query_execution::preparation::scan::ScanExecutionBindings, String> {
    let controls = crate::connector::FixtureControlResolver::new(connectors.clone());
    super::prepare_scan_bindings(
        plan,
        connectors,
        &controls,
        &crate::connector::test_request_context(),
        resolver,
        super::ScanPreparationOptions::default(),
    )
}

struct StaticResolver {
    execution: ResolvedScanExecution,
}

impl ScanBindingResolver for StaticResolver {
    fn resolve_scan(
        &self,
        _node_id: i32,
        _scan: &PlanScanNode,
    ) -> Result<Option<ResolvedScanExecution>, String> {
        Ok(Some(self.execution.clone()))
    }
}

fn column(id: u32, name: &str, data_type: DataType, nullable: bool) -> OutputColumn {
    OutputColumn {
        column_id: ColumnId::new_for_test(id),
        name: name.to_string(),
        data_type,
        nullable,
        is_internal: false,
    }
}

fn source_column(name: &str, data_type: DataType, nullable: bool) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        data_type,
        nullable,
        write_default: None,
        logical_type: None,
    }
}

fn iceberg_table() -> IcebergTableInfo {
    IcebergTableInfo {
        catalog: "test_catalog".to_string(),
        namespace: "test_db".to_string(),
        table: "test_table".to_string(),
        table_uuid: Some("00000000-0000-0000-0000-000000000001".to_string()),
        current_snapshot_id: Some(7),
        schema_id: 1,
        location: "s3://bucket/test_table".to_string(),
        schema: IcebergSchemaDef {
            fields: vec![
                IcebergSchemaFieldDef {
                    field_id: 1,
                    name: "id".to_string(),
                    initial_default: None,
                    write_default: None,
                    initial_default_json: None,
                    write_default_json: None,
                    children: Vec::new(),
                },
                IcebergSchemaFieldDef {
                    field_id: 3,
                    name: "category".to_string(),
                    initial_default: None,
                    write_default: None,
                    initial_default_json: None,
                    write_default_json: None,
                    children: Vec::new(),
                },
            ],
        },
        serialized_metadata: None,
        serialized_metadata_rows: None,
    }
}

fn data_file(path: &str) -> IcebergDataFileInfo {
    IcebergDataFileInfo {
        path: path.to_string(),
        size: 128,
        row_count: Some(10),
        column_stats: None,
        partition_spec_id: Some(0),
        partition_key: Some("Struct([])".to_string()),
        first_row_id: None,
        data_sequence_number: Some(1),
        ivm_change_op: None,
        included_positions: None,
        delete_files: Vec::new(),
        manifest_path: None,
        partition_values: Vec::new(),
    }
}

fn equality_delete_file(
    equality_column_names: Vec<&str>,
    equality_field_ids: Vec<i32>,
) -> crate::connector::iceberg::scan_model::IcebergDeleteFileInfo {
    crate::connector::iceberg::scan_model::IcebergDeleteFileInfo {
        path: "s3://bucket/eq-delete.parquet".to_string(),
        file_format: crate::connector::iceberg::scan_model::IcebergDeleteFileFormat::Parquet,
        file_content: crate::connector::iceberg::scan_model::IcebergDeleteFileContent::Equality,
        length: Some(1),
        content_offset: None,
        content_size_in_bytes: None,
        sequence_number: Some(2),
        partition_spec_id: Some(0),
        partition_key: Some("Struct([])".to_string()),
        equality_column_names: equality_column_names
            .into_iter()
            .map(str::to_string)
            .collect(),
        equality_field_ids,
    }
}

fn scan_node(node_id: i32, binding: IcebergDataFileBinding) -> DistributedNode {
    let output = column(1, "id", DataType::Int32, false);
    let table = TableDef {
        name: "ice_t".to_string(),
        columns: vec![source_column("id", DataType::Int32, false)],
        iceberg_row_lineage_metadata_columns: Vec::new(),
        source: ScanSource::IcebergDataFiles {
            table: iceberg_table(),
            files: vec![data_file("s3://bucket/explicit.parquet")],
            cloud_properties: BTreeMap::new(),
            binding,
        },
    };
    DistributedNode {
        node_id,
        fragment_id: 0,
        tuple_ids: vec![node_id],
        nullable_tuple_ids: Vec::new(),
        limit: -1,
        runtime_filter_binding_ids: Vec::new(),
        children: Vec::new(),
        stats: PhysicalPlanStats {
            output_row_count: 10.0,
            row_count_confidence: PlannerConfidence::Fallback,
            column_statistics: HashMap::new(),
            cost_estimate: None,
            broadcast_decision: None,
        },
        payload: DistributedNodeKind::Scan(PlanScanNode {
            database: "default".to_string(),
            table,
            alias: None,
            columns: vec![output],
            predicates: Vec::new(),
            required_columns: Some(vec!["id".to_string()]),
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }),
    }
}

fn plan(root: DistributedNode) -> DistributedPlan {
    crate::sql::planner::distributed::test_support::distributed_plan_for_test! {
        fragments: vec![PlanFragment {
            fragment_id: 0,
            root,
            data_partition: DataPartition::unpartitioned(),
            output_partition: DataPartition::unpartitioned(),
            sink: DataSink::Result,
            output_exprs: None,
            output_columns: vec![column(1, "id", DataType::Int32, false)],
            cte_id: None,
            cte_exchange_nodes: Vec::new(),
        }],
        root_fragment_id: 0,
        runtime_filter_graph: Default::default(),
        edges: Vec::new(),
    }
}

fn registry(files: Vec<IcebergDataFileInfo>) -> ConnectorRegistry {
    let registry = ConnectorRegistry::new();
    crate::connector::iceberg::provider::register_planned_files_fixture(
        &registry,
        "test_catalog",
        files,
        None,
    );
    registry
}

fn recording_registry(
    files: Vec<IcebergDataFileInfo>,
) -> (ConnectorRegistry, Arc<Mutex<Vec<Vec<usize>>>>) {
    let seen_projections = Arc::new(Mutex::new(Vec::new()));
    let registry = ConnectorRegistry::new();
    crate::connector::iceberg::provider::register_planned_files_fixture(
        &registry,
        "test_catalog",
        files,
        Some(Arc::clone(&seen_projections)),
    );
    (registry, seen_projections)
}

fn resolved_files(files: Vec<IcebergDataFileInfo>) -> ResolvedScanExecution {
    ResolvedScanExecution::IcebergFiles(ResolvedIcebergFileScan {
        table: iceberg_table(),
        files,
        binding: IcebergDataFileBinding::ExplicitFiles,
    })
}

fn resolved_delta() -> ResolvedScanExecution {
    ResolvedScanExecution::IcebergDelta(
        crate::query_execution::preparation::scan::ResolvedIcebergDeltaScan {
            runtime_plan: crate::query_execution::preparation::scan::IcebergDeltaScanRuntimePlan {
                table_location: "s3://bucket/test_table".to_string(),
                data_columns: Vec::new(),
                change_files: Vec::new(),
                delete_side: None,
            },
        },
    )
}

fn resolved_data_delta() -> ResolvedScanExecution {
    let mut delta = match resolved_delta() {
        ResolvedScanExecution::IcebergDelta(delta) => delta,
        ResolvedScanExecution::IcebergFiles(_) => unreachable!("fixture is delta"),
        ResolvedScanExecution::ConnectorRead => unreachable!("fixture is delta"),
    };
    delta.runtime_plan.change_files = vec![crate::connector::iceberg::delta::DeltaSourceFile {
        path: "s3://bucket/delta-added.parquet".to_string(),
        size: 128,
        role: crate::connector::iceberg::delta::DeltaSourceRole::DataFile,
        partition_spec_id: Some(0),
        partition_key: Some("Struct([])".to_string()),
        first_row_id: Some(100),
        data_sequence_number: Some(7),
        row_id_allow_list: None,
    }];
    ResolvedScanExecution::IcebergDelta(delta)
}

fn replace_scan_source(root: &mut DistributedNode, source: ScanSource) {
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("test root must be a scan");
    };
    scan.table.source = source;
}
