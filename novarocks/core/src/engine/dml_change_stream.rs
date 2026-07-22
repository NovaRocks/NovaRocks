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

use std::sync::Arc;

use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_iceberg_catalog};
use crate::coordinator::execution::CoordinatedQueryResult;
use crate::engine::StandaloneState;
use crate::runtime::query_options::QueryOptions;
use crate::sql::analysis::OutputColumn;
use crate::sql::common::ChangeStreamBranchKind;
use crate::sql::optimizer::OptimizedOperatorNode;
use crate::sql::planner::distributed::write::change_stream::{
    ChangeStreamWriteBranchSpec, ChangeStreamWriteDagSpec,
};
use crate::sql::planner::distributed::write::sink::IcebergWriteSinkSpec;

pub(crate) const DML_CHANGE_STREAM_DATA_ROUTE_COLUMN: &str = "__change_data_route";

pub(crate) struct DmlChangeStreamWritePlan {
    pub(crate) producer: OptimizedOperatorNode,
    pub(crate) dag: ChangeStreamWriteDagSpec,
    pub(crate) pre_expand_keyed_assert: Option<DmlPreExpandKeyedAssert>,
}

#[derive(Clone, Debug)]
pub(crate) struct DmlPreExpandKeyedAssert {
    pub(crate) key_column_name: String,
    pub(crate) key_label: String,
    pub(crate) message_prefix: String,
}

#[derive(Debug)]
pub(crate) struct DmlChangeStreamWriteExecution {
    pub(crate) result: CoordinatedQueryResult,
    pub(crate) commit_plan:
        crate::connector::iceberg::change_stream_routing::ChangeStreamWriterCommitPlan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DmlChangeStreamBranchSet {
    UpdateMor,
    Merge {
        matched_update: bool,
        matched_delete: bool,
        not_matched_insert: bool,
    },
}

#[derive(Clone, Debug, Default)]
struct DmlChangeStreamWriteBranchSinkSpecs {
    delete_dv: Option<IcebergWriteSinkSpec>,
    reuse_data: Option<IcebergWriteSinkSpec>,
    fresh_data: Option<IcebergWriteSinkSpec>,
    target_partition_source_columns: Vec<String>,
}

impl DmlChangeStreamBranchSet {
    fn branch_kinds(self) -> Vec<ChangeStreamBranchKind> {
        match self {
            Self::UpdateMor => vec![
                ChangeStreamBranchKind::DeleteDv,
                ChangeStreamBranchKind::ReuseData,
            ],
            Self::Merge {
                matched_update,
                matched_delete,
                not_matched_insert,
            } => {
                let mut branches = Vec::with_capacity(3);
                if matched_update || matched_delete {
                    branches.push(ChangeStreamBranchKind::DeleteDv);
                }
                if matched_update {
                    branches.push(ChangeStreamBranchKind::ReuseData);
                }
                if not_matched_insert {
                    branches.push(ChangeStreamBranchKind::FreshData);
                }
                branches
            }
        }
    }
}

pub(crate) fn build_dml_change_stream_write_plan(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    producer: OptimizedOperatorNode,
    branch_set: DmlChangeStreamBranchSet,
    target_ref: &str,
) -> Result<DmlChangeStreamWritePlan, String> {
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    let catalog = build_iceberg_catalog(&entry)?;
    let table_ident = iceberg::TableIdent::new(
        iceberg::NamespaceIdent::new(target.namespace.clone()),
        target.table.clone(),
    );
    let table = block_on_iceberg(async { catalog.load_table(&table_ident).await })?
        .map_err(|e| format!("load iceberg table {}: {e}", &table_ident))?;
    let resolved = {
        let registry = state.connectors.read().expect("connector registry read");
        let backend = registry.catalog_backend("iceberg")?;
        backend.load_table(&target.catalog, &target.namespace, &target.table)?
    };

    let branch_kinds = branch_set.branch_kinds();
    if branch_kinds.is_empty() {
        return Err("DML change-stream write requires at least one branch".to_string());
    }
    let mut sink_specs = DmlChangeStreamWriteBranchSinkSpecs {
        target_partition_source_columns: target_partition_source_column_names(table.metadata())?,
        ..Default::default()
    };
    if branch_kinds.contains(&ChangeStreamBranchKind::DeleteDv) {
        sink_specs.delete_dv = Some(
            crate::engine::mutation_flow::build_mor_deletion_vector_sink_spec(
                target, &resolved, &table, &entry, target_ref,
            )?,
        );
    }
    if branch_kinds.contains(&ChangeStreamBranchKind::ReuseData) {
        sink_specs.reuse_data = Some(
            crate::engine::iceberg_writer::build_row_lineage_data_sink_spec(
                target, &resolved, &table, &entry,
            )?,
        );
    }
    if branch_kinds.contains(&ChangeStreamBranchKind::FreshData) {
        let write_columns = crate::engine::iceberg_writer::iceberg_insert_columns_from_schema(
            table.metadata().current_schema(),
        )?;
        sink_specs.fresh_data = Some(crate::engine::iceberg_writer::build_insert_write_sink_spec(
            target,
            &resolved,
            &table,
            &entry,
            &write_columns,
        )?);
    }

    let dag = build_dml_change_stream_dag_from_sink_specs(
        branch_set,
        &producer.output_columns,
        sink_specs,
    )?;
    Ok(DmlChangeStreamWritePlan {
        producer,
        dag,
        pre_expand_keyed_assert: None,
    })
}

fn build_dml_change_stream_dag_from_sink_specs(
    branch_set: DmlChangeStreamBranchSet,
    producer_output_columns: &[OutputColumn],
    mut sink_specs: DmlChangeStreamWriteBranchSinkSpecs,
) -> Result<ChangeStreamWriteDagSpec, String> {
    let branch_kinds = branch_set.branch_kinds();
    if branch_kinds.is_empty() {
        return Err("DML change-stream write requires at least one branch".to_string());
    }
    let has_data_branch = branch_kinds.iter().any(|kind| {
        matches!(
            kind,
            ChangeStreamBranchKind::ReuseData | ChangeStreamBranchKind::FreshData
        )
    });
    let change_op_output_ordinal = output_ordinal_by_name(
        producer_output_columns,
        crate::exec::change_op::CHANGE_OP_COLUMN,
        "change-op column",
        OutputBindingKind::Internal,
    )?;
    let data_route_output_ordinal = if has_data_branch {
        Some(output_ordinal_by_name(
            producer_output_columns,
            DML_CHANGE_STREAM_DATA_ROUTE_COLUMN,
            "data-route column",
            OutputBindingKind::Internal,
        )?)
    } else {
        None
    };
    let data_partition_ordinals = if has_data_branch {
        target_partition_source_ordinals(
            producer_output_columns,
            &sink_specs.target_partition_source_columns,
        )?
    } else {
        Vec::new()
    };

    let mut branches = Vec::with_capacity(branch_kinds.len());
    for (idx, branch_kind) in branch_kinds.into_iter().enumerate() {
        let (sink_spec, output_partition_ordinals) = match branch_kind {
            ChangeStreamBranchKind::DeleteDv => {
                let sink_spec = sink_specs
                    .delete_dv
                    .take()
                    .ok_or_else(|| "DML change-stream DeleteDv sink spec is missing".to_string())?;
                let file_ordinal = output_ordinal_by_name(
                    producer_output_columns,
                    crate::exec::row_position::ICEBERG_FILE_PATH_COL,
                    "delete file column",
                    OutputBindingKind::Internal,
                )?;
                (sink_spec, vec![file_ordinal])
            }
            ChangeStreamBranchKind::ReuseData => {
                let sink_spec = sink_specs.reuse_data.take().ok_or_else(|| {
                    "DML change-stream ReuseData sink spec is missing".to_string()
                })?;
                (sink_spec, data_partition_ordinals.clone())
            }
            ChangeStreamBranchKind::FreshData => {
                let sink_spec = sink_specs.fresh_data.take().ok_or_else(|| {
                    "DML change-stream FreshData sink spec is missing".to_string()
                })?;
                (sink_spec, data_partition_ordinals.clone())
            }
        };
        let stream_output_ordinals =
            output_ordinals_for_sink_columns(producer_output_columns, &sink_spec.target_columns)?;
        branches.push(ChangeStreamWriteBranchSpec {
            branch_id: i32::try_from(idx).map_err(|_| {
                "DML change-stream branch id overflow while building DAG".to_string()
            })?,
            branch_kind,
            stream_output_ordinals,
            output_partition_ordinals,
            sink_spec,
        });
    }

    let dag = ChangeStreamWriteDagSpec {
        change_op_output_ordinal: Some(change_op_output_ordinal),
        data_route_output_ordinal,
        branches,
    };
    dag.validate()?;
    Ok(dag)
}

pub(crate) fn execute_dml_change_stream_write(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    mut plan: DmlChangeStreamWritePlan,
    query_opts: Option<&QueryOptions>,
) -> Result<DmlChangeStreamWriteExecution, String> {
    let crate::engine::PlannedIcebergChangeStreamWrite {
        prepared,
        native_bundle,
        commit_plan,
        #[cfg(test)]
        topology,
    } = plan_dml_change_stream_write(state, target, &mut plan)?;
    #[cfg(test)]
    if let Some(result) = crate::engine::observe_change_stream_write_build_for_test(&topology) {
        return dml_change_stream_write_execution(result, commit_plan);
    }
    let result = crate::engine::execute_planned_iceberg_change_stream_write(
        prepared,
        native_bundle,
        query_opts.cloned(),
    )?;
    dml_change_stream_write_execution(result, commit_plan)
}

pub(crate) fn plan_dml_change_stream_write(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    plan: &mut DmlChangeStreamWritePlan,
) -> Result<crate::engine::PlannedIcebergChangeStreamWrite, String> {
    let keyed_assert = plan.pre_expand_keyed_assert.as_ref().map(|keyed_assert| {
        crate::sql::planner::physical::PreExpandKeyedAssertSpec {
            key_column_name: keyed_assert.key_column_name.clone(),
            key_label: keyed_assert.key_label.clone(),
            message_prefix: keyed_assert.message_prefix.clone(),
        }
    });
    let planned = crate::engine::build_physical_plan_as_iceberg_change_stream_write(
        state,
        Some(&target.catalog),
        &target.namespace,
        &plan.producer,
        &mut plan.dag,
        None,
        keyed_assert,
    )?;
    Ok(planned)
}

fn dml_change_stream_write_execution(
    result: CoordinatedQueryResult,
    commit_plan: crate::connector::iceberg::change_stream_routing::ChangeStreamWriterCommitPlan,
) -> Result<DmlChangeStreamWriteExecution, String> {
    if let Some(abort) = result.write_abort.as_ref() {
        return Err(abort.reason.clone());
    }
    if result.write_commit.is_none() {
        return Err("DML change-stream write completed without writer commit".to_string());
    }
    Ok(DmlChangeStreamWriteExecution {
        result,
        commit_plan,
    })
}

fn target_partition_source_column_names(
    metadata: &iceberg::spec::TableMetadata,
) -> Result<Vec<String>, String> {
    let schema = metadata.current_schema();
    metadata
        .default_partition_spec()
        .fields()
        .iter()
        .map(|field| {
            let source = schema.field_by_id(field.source_id).ok_or_else(|| {
                format!(
                    "DML change-stream partition source field id {} not found in target schema",
                    field.source_id
                )
            })?;
            Ok(source.name.clone())
        })
        .collect()
}

fn target_partition_source_ordinals(
    output_columns: &[OutputColumn],
    source_columns: &[String],
) -> Result<Vec<usize>, String> {
    source_columns
        .iter()
        .map(|name| {
            output_ordinal_by_name(
                output_columns,
                name,
                "target partition source column",
                OutputBindingKind::UserVisible,
            )
        })
        .collect()
}

fn output_ordinals_for_sink_columns(
    output_columns: &[OutputColumn],
    sink_columns: &[novarocks_catalog::schema::ColumnDef],
) -> Result<Vec<usize>, String> {
    sink_columns
        .iter()
        .map(|column| {
            output_ordinal_by_name(
                output_columns,
                &column.name,
                "sink input column",
                binding_kind_for_sink_column(&column.name),
            )
        })
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OutputBindingKind {
    Internal,
    UserVisible,
}

fn binding_kind_for_sink_column(name: &str) -> OutputBindingKind {
    if is_reserved_internal_output_name(name) {
        OutputBindingKind::Internal
    } else {
        OutputBindingKind::UserVisible
    }
}

fn is_reserved_internal_output_name(name: &str) -> bool {
    name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_FILE_PATH_COL)
        || name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_POS_COL)
        || name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_ROW_ID_COL)
        || name.eq_ignore_ascii_case(crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL)
        || name.eq_ignore_ascii_case(crate::exec::change_op::CHANGE_OP_COLUMN)
        || name.eq_ignore_ascii_case(DML_CHANGE_STREAM_DATA_ROUTE_COLUMN)
}

fn output_ordinal_by_name(
    output_columns: &[OutputColumn],
    name: &str,
    label: &str,
    binding_kind: OutputBindingKind,
) -> Result<usize, String> {
    let mut matches = output_columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.name.eq_ignore_ascii_case(name));
    let (ordinal, column) = matches
        .next()
        .ok_or_else(|| format!("DML change-stream {label} `{name}` not found in plan output"))?;
    if matches.next().is_some() {
        return Err(format!(
            "DML change-stream {label} `{name}` is ambiguous in plan output"
        ));
    }
    match binding_kind {
        OutputBindingKind::Internal if !column.is_internal => {
            return Err(format!(
                "DML change-stream {label} `{name}` must be marked internal in plan output"
            ));
        }
        OutputBindingKind::UserVisible if column.is_internal => {
            return Err(format!(
                "DML change-stream {label} `{name}` must be user-visible in plan output"
            ));
        }
        OutputBindingKind::Internal | OutputBindingKind::UserVisible => {}
    }
    Ok(ordinal)
}
