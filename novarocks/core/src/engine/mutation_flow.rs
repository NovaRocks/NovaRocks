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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::compute::{cast, concat_batches, filter_record_batch};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use iceberg::Catalog;
use iceberg::arrow::schema_to_arrow_schema;

use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_iceberg_catalog};
use crate::connector::iceberg::commit::{
    CommitOpKind, CommitOutcome, CommitServiceError, CowUpdateRewriteSet, CowUpdateTouchedFile,
    IcebergCommitCollector, IcebergUpdateMode, ensure_iceberg_write_supported,
    select_iceberg_update_mode,
};
use crate::coordinator::execution::CoordinatedQueryResult;
use crate::coordinator::write::report::WriteCommitInput;
use crate::engine::write_transaction::{
    IcebergWriteCommitExecutor, IcebergWriteCommitPolicy, IcebergWriteSource,
    IcebergWriteTransactionExecutor, IcebergWriteTransactionRunner, IcebergWriteTransactionSpec,
    IcebergWriteValidationPolicy, write_commit_has_files,
};
use crate::engine::{StandaloneState, StatementResult};
use crate::meta::repository::iceberg_operation::{IcebergOperationKind, IcebergOperationTarget};
use crate::runtime::query_result::QueryResult;
use crate::sql::analyzer::iceberg_ref::{IcebergRefSuffix, split_ref_suffix};
use crate::sql::parser::ast::{
    InsertSource, MergeMatchedAction, MergeNotMatchedAction, MergeStmt, ObjectName, UpdateStmt,
};
use crate::sql::planner::distributed::write::sink::{IcebergWriteSinkMode, IcebergWriteSinkSpec};

pub(crate) fn execute_update_statement(
    state: &Arc<StandaloneState>,
    stmt: &UpdateStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    // Detect branch/tag suffix in the target table name.
    let (stripped_parts, ref_suffix) = split_ref_suffix(&stmt.table.parts);
    let effective_name;
    let table_name: &ObjectName = match ref_suffix {
        Some(IcebergRefSuffix::Tag(ref tag_name)) => {
            return Err(format!(
                "iceberg ref: tag '{tag_name}' is read-only; use a branch as DML target"
            ));
        }
        Some(IcebergRefSuffix::Branch(_)) => {
            effective_name = ObjectName {
                parts: stripped_parts,
            };
            &effective_name
        }
        None => &stmt.table,
    };
    let target_ref = match &ref_suffix {
        Some(IcebergRefSuffix::Branch(b)) => b.clone(),
        _ => "main".to_string(),
    };

    let target = crate::engine::backend_resolver::resolve_existing_table_target(
        state,
        table_name,
        current_catalog,
        current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "UPDATE only supports iceberg backends, got `{}`",
            target.backend_name
        ));
    }

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
    crate::engine::mv::iceberg_guard::reject_if_iceberg_mv_properties(
        &target,
        table.metadata().properties(),
        crate::engine::mv::iceberg_guard::IcebergMvUserMutation::Update,
    )?;

    // Branch writes require Iceberg v3 (row-lineage semantics).
    if target_ref != "main" {
        let fmt = table.metadata().format_version();
        if fmt != iceberg::spec::FormatVersion::V3 {
            return Err(format!(
                "iceberg ref: branch writes require Iceberg v3 tables (table {} is v{})",
                table_ident, fmt as u8,
            ));
        }
    }

    let target_columns = iceberg_table_columns(&table)?;
    let partition_columns = iceberg_partition_source_columns(&table)?;
    validate_update_assignments(&stmt.assignments, &target_columns, &partition_columns)?;

    let mode = select_iceberg_update_mode(&table)?;
    match mode {
        IcebergUpdateMode::CopyOnWrite => {
            let matched = materialize_update_matches(state, &target, stmt, current_catalog)?;
            if matched.row_ids.is_empty() {
                return Ok(StatementResult::Ok);
            }
            validate_unique_target_row_ids(&matched.row_ids)?;
            execute_cow_update(
                state,
                &target,
                catalog,
                table_ident,
                table,
                matched,
                &target_columns,
                entry,
                &target_ref,
            )
        }
        IcebergUpdateMode::MergeOnRead => execute_mor_update(
            state,
            &target,
            catalog,
            table_ident,
            table,
            stmt,
            current_catalog,
            &target_columns,
            entry,
            &target_ref,
        ),
    }
}

fn materialize_update_matches(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &UpdateStmt,
    current_catalog: Option<&str>,
) -> Result<MatchedUpdateBatch, String> {
    let target_alias = stmt.alias.as_deref().unwrap_or("__nr_t");
    // The match SELECT runs against the standalone analyzer with
    // `current_database = target.namespace` (so 1-part target name resolves
    // to the iceberg target). Source relations may live in a different
    // namespace; `mutation_source_to_sql` qualifies them with their
    // namespace so the analyzer can find them.
    let target_sql = format!("{} AS {}", target.table, target_alias);
    let assignments_sql = stmt
        .assignments
        .iter()
        .map(|assignment| (assignment.column.as_str(), assignment.value.to_string()))
        .collect::<Vec<_>>();
    let assignments_sql = assignments_sql
        .iter()
        .map(|(column, expr)| (*column, expr.as_str()))
        .collect::<Vec<_>>();
    let where_sql = stmt.where_clause.as_ref().map(|expr| expr.to_string());
    let source_sql = mutation_source_to_sql(state, &stmt.source, current_catalog, target)?;
    let match_sql = build_update_match_query_sql(
        &target_sql,
        target_alias,
        source_sql.as_deref(),
        &assignments_sql,
        where_sql.as_deref(),
    );
    execute_update_match_query(state, Some(&target.catalog), &match_sql, &target.namespace)
}

fn mutation_source_to_sql(
    state: &Arc<StandaloneState>,
    source: &Option<crate::sql::parser::ast::MutationSource>,
    current_catalog: Option<&str>,
    target: &crate::engine::backend_resolver::TargetBackend,
) -> Result<Option<String>, String> {
    match source {
        None => Ok(None),
        Some(source) => {
            mutation_source_relation_to_sql(state, source, current_catalog, target).map(Some)
        }
    }
}

fn mutation_source_relation_to_sql(
    state: &Arc<StandaloneState>,
    source: &crate::sql::parser::ast::MutationSource,
    current_catalog: Option<&str>,
    target: &crate::engine::backend_resolver::TargetBackend,
) -> Result<String, String> {
    use crate::sql::parser::ast::MutationSource;
    match source {
        MutationSource::Table { name, alias } => {
            // The match SELECT runs with `current_database = target.namespace`
            // and `current_catalog = Some(target.catalog)`. Resolve the source
            // against the user's surface name to get its concrete (catalog,
            // namespace, table). Emit a 1-part name when the source shares the
            // target's namespace+catalog (lets refresh follow the
            // current-catalog path), and a 2-part `<namespace>.<table>` name
            // otherwise so the standalone analyzer can find it directly.
            let resolved = crate::engine::backend_resolver::resolve_existing_table_target(
                state,
                name,
                current_catalog,
                &target.namespace,
            )?;
            let mut sql =
                if resolved.catalog == target.catalog && resolved.namespace == target.namespace {
                    resolved.table.clone()
                } else {
                    format!("{}.{}", resolved.namespace, resolved.table)
                };
            if let Some(alias) = alias {
                sql.push_str(" AS ");
                sql.push_str(alias);
            }
            Ok(sql)
        }
        MutationSource::Query { query, alias } => {
            let alias = alias
                .as_deref()
                .ok_or_else(|| "MERGE/UPDATE subquery source requires an alias".to_string())?;
            Ok(format!("({query}) AS {alias}"))
        }
    }
}

pub(crate) fn build_mor_deletion_vector_sink_spec(
    target: &crate::engine::backend_resolver::TargetBackend,
    resolved: &crate::connector::backend::ResolvedTable,
    table: &iceberg::table::Table,
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    target_ref: &str,
) -> Result<IcebergWriteSinkSpec, String> {
    let planned_snapshot_id = if target_ref == "main" {
        table.metadata().current_snapshot().map(|s| s.snapshot_id())
    } else {
        crate::engine::delete_flow::resolve_branch_head_snapshot_id(table.metadata(), target_ref)?
    };
    let mut sink_spec = crate::engine::iceberg_writer::build_position_delete_sink_spec(
        target, resolved, table, entry,
    )?;
    sink_spec.mode = IcebergWriteSinkMode::DeletionVectors;
    sink_spec.set_planned_snapshot_id(planned_snapshot_id)?;
    Ok(sink_spec)
}

#[allow(clippy::too_many_arguments)]
fn build_update_mor_change_stream_write_plan(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &UpdateStmt,
    current_catalog: Option<&str>,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
    target_ref: &str,
    new_sequence_number: i64,
) -> Result<crate::engine::dml_change_stream::DmlChangeStreamWritePlan, String> {
    let target_alias = stmt.alias.as_deref().unwrap_or("__nr_t");
    let source_sql = mutation_source_to_sql(state, &stmt.source, current_catalog, target)?;
    let where_sql = stmt.where_clause.as_ref().map(|expr| expr.to_string());
    let assignments_sql = update_assignment_projection_sql(&stmt.assignments, target_columns)?;
    let assignments_sql_refs = assignments_sql
        .iter()
        .map(|(column, expr)| (column.as_str(), expr.as_str()))
        .collect::<Vec<_>>();
    let target_sql = update_change_stream_target_sql(target, target_alias, target_ref);
    let match_sql = build_update_match_query_sql(
        &target_sql,
        target_alias,
        source_sql.as_deref(),
        &assignments_sql_refs,
        where_sql.as_deref(),
    );
    let mut query = parse_generated_query(&match_sql, "MOR UPDATE change-stream producer")?;
    if crate::engine::query_prep::has_time_travel_refs(&query) {
        crate::engine::query_prep::rewrite_time_travel_refs(
            state,
            Some(&target.catalog),
            &target.namespace,
            &mut query,
        )?;
    }

    let catalog_service_snapshot = crate::engine::catalog_service_snapshot(state);
    let connectors_snapshot = state
        .connectors
        .read()
        .expect("standalone connector registry read lock")
        .clone();
    let analyzer_provider = crate::engine::build_catalog_service_provider(
        Some(&target.catalog),
        &catalog_service_snapshot,
        &connectors_snapshot,
        crate::sql::catalog::TableLookupMode::SchemaOnly,
    );
    let planned = crate::engine::plan_query_for_iceberg_change_stream_refresh(
        &query,
        &analyzer_provider,
        &connectors_snapshot,
        &target.namespace,
        None,
        None,
        false,
    )?;
    let producer = build_update_mor_change_event_expand_plan(
        planned.optimized_tree,
        target_columns,
        new_sequence_number,
    )?;
    let mut plan = crate::engine::dml_change_stream::build_dml_change_stream_write_plan(
        state,
        target,
        producer,
        crate::engine::dml_change_stream::DmlChangeStreamBranchSet::UpdateMor,
        target_ref,
    )?;
    plan.pre_expand_keyed_assert =
        Some(crate::engine::dml_change_stream::DmlPreExpandKeyedAssert {
            key_column_name: "__nr_row_id".to_string(),
            key_label: crate::exec::row_position::ICEBERG_ROW_ID_COL.to_string(),
            message_prefix: "MOR UPDATE matched target row".to_string(),
        });
    Ok(plan)
}

fn update_assignment_projection_sql(
    assignments: &[crate::sql::parser::ast::UpdateAssignment],
    target_columns: &[novarocks_catalog::schema::ColumnDef],
) -> Result<Vec<(String, String)>, String> {
    assignments
        .iter()
        .map(|assignment| {
            let target_column = target_columns
                .iter()
                .find(|column| column.name.eq_ignore_ascii_case(&assignment.column))
                .ok_or_else(|| {
                    format!(
                        "UPDATE assignment references unknown target column `{}`",
                        assignment.column
                    )
                })?;
            Ok((
                target_column.name.clone(),
                crate::engine::iceberg_writer::target_cast_expr_sql(
                    &format!("({})", assignment.value),
                    target_column,
                )?,
            ))
        })
        .collect()
}

fn update_change_stream_target_sql(
    target: &crate::engine::backend_resolver::TargetBackend,
    target_alias: &str,
    target_ref: &str,
) -> String {
    let version_clause = if target_ref == "main" {
        String::new()
    } else {
        format!(" FOR VERSION AS OF {}", sql_string_literal(target_ref))
    };
    format!(
        "{}{} AS {}",
        qualify_iceberg_table(target),
        version_clause,
        target_alias
    )
}

fn build_update_mor_change_event_expand_plan(
    optimized_tree: crate::sql::optimizer::OptimizedOperatorNode,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
    new_sequence_number: i64,
) -> Result<crate::sql::optimizer::OptimizedOperatorNode, String> {
    use crate::sql::optimizer::operator::{
        ChangeEventExpandOp, ChangeEventOutputExpr, ChangeEventSpec, Operator,
        PhysicalDistributionOp,
    };
    use crate::sql::optimizer::optimized_tree::{
        OptimizedOperatorNode, OptimizerExplainStats, PlanExecutionProps,
    };
    use crate::sql::optimizer::property::DistributionSpec;
    use crate::sql::optimizer::scalar::{HashableLiteral, ScalarNode};

    let mut scalar_arena = optimized_tree
        .execution_props
        .scalar_arena
        .as_deref()
        .cloned()
        .ok_or_else(|| "MOR UPDATE physical plan is missing scalar arena".to_string())?;
    let child_outputs = optimized_tree.output_columns.clone();
    let row_id_input = output_column_by_name(&child_outputs, "__nr_row_id", "UPDATE row id")?;
    let hash_distribution = DistributionSpec::shuffle_agg([row_id_input.column_id]);

    let child_stats = optimized_tree.stats.clone();
    let distributed = OptimizedOperatorNode {
        op: Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: hash_distribution,
        }),
        children: vec![optimized_tree],
        stats: child_stats.clone(),
        explain_stats: OptimizerExplainStats::default(),
        output_columns: child_outputs.clone(),
        execution_props: PlanExecutionProps::default(),
    };

    let mut next_column_id = max_physical_column_id(&distributed) + 1;
    let mut alloc_output =
        |name: &str, data_type: arrow::datatypes::DataType, nullable: bool, is_internal: bool| {
            let column = crate::sql::analysis::OutputColumn {
                column_id: crate::sql::column_id::ColumnId(next_column_id),
                name: name.to_string(),
                data_type,
                nullable,
                is_internal,
            };
            next_column_id += 1;
            column
        };

    let file_output = alloc_output(
        crate::exec::row_position::ICEBERG_FILE_PATH_COL,
        arrow::datatypes::DataType::Utf8,
        true,
        true,
    );
    let pos_output = alloc_output(
        crate::exec::row_position::ICEBERG_ROW_POS_COL,
        arrow::datatypes::DataType::Int64,
        true,
        true,
    );
    let mut target_outputs = Vec::with_capacity(target_columns.len());
    for column in target_columns {
        target_outputs.push((
            column.name.clone(),
            alloc_output(
                &column.name,
                column.data_type.clone(),
                column.nullable,
                false,
            ),
        ));
    }
    let row_id_output = alloc_output(
        crate::exec::row_position::ICEBERG_ROW_ID_COL,
        arrow::datatypes::DataType::Int64,
        true,
        true,
    );
    let last_sequence_output = alloc_output(
        crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
        arrow::datatypes::DataType::Int64,
        true,
        true,
    );
    let change_op_output = alloc_output(
        crate::exec::change_op::CHANGE_OP_COLUMN,
        arrow::datatypes::DataType::Int8,
        false,
        true,
    );
    let data_route_output = alloc_output(
        crate::engine::dml_change_stream::DML_CHANGE_STREAM_DATA_ROUTE_COLUMN,
        arrow::datatypes::DataType::Int32,
        true,
        true,
    );

    let file_expr = child_column_expr(
        &mut scalar_arena,
        &child_outputs,
        "__nr_file",
        "UPDATE old file",
    )?;
    let pos_expr = child_column_expr(
        &mut scalar_arena,
        &child_outputs,
        "__nr_pos",
        "UPDATE old row position",
    )?;
    let row_id_expr = child_column_expr(
        &mut scalar_arena,
        &child_outputs,
        "__nr_row_id",
        "UPDATE old row id",
    )?;
    let new_sequence_expr = scalar_arena.intern(
        ScalarNode::Literal(HashableLiteral(crate::sql::analysis::LiteralValue::Int(
            new_sequence_number,
        ))),
        arrow::datatypes::DataType::Int64,
        false,
    );

    let mut delete_assignments = vec![
        ChangeEventOutputExpr {
            output_column_id: file_output.column_id,
            expr: Some(file_expr),
        },
        ChangeEventOutputExpr {
            output_column_id: pos_output.column_id,
            expr: Some(pos_expr),
        },
    ];
    let mut reuse_assignments = Vec::with_capacity(target_columns.len() + 2);
    for (name, output) in &target_outputs {
        let old_expr = child_column_expr(
            &mut scalar_arena,
            &child_outputs,
            name,
            "UPDATE old target column",
        )?;
        delete_assignments.push(ChangeEventOutputExpr {
            output_column_id: output.column_id,
            expr: Some(old_expr),
        });

        let new_name = format!("__nr_new_{name}");
        let new_expr = match maybe_output_column_by_name(&child_outputs, &new_name)? {
            Some(column) => scalar_arena.intern(
                ScalarNode::ColumnRef(column.column_id),
                column.data_type.clone(),
                column.nullable,
            ),
            None => child_column_expr(
                &mut scalar_arena,
                &child_outputs,
                name,
                "UPDATE unchanged target column",
            )?,
        };
        reuse_assignments.push(ChangeEventOutputExpr {
            output_column_id: output.column_id,
            expr: Some(new_expr),
        });
    }
    reuse_assignments.push(ChangeEventOutputExpr {
        output_column_id: row_id_output.column_id,
        expr: Some(row_id_expr),
    });
    reuse_assignments.push(ChangeEventOutputExpr {
        output_column_id: last_sequence_output.column_id,
        expr: Some(new_sequence_expr),
    });

    let mut output_columns = Vec::with_capacity(target_columns.len() + 6);
    output_columns.push(file_output);
    output_columns.push(pos_output);
    output_columns.extend(target_outputs.into_iter().map(|(_, column)| column));
    output_columns.push(row_id_output.clone());
    output_columns.push(last_sequence_output);
    output_columns.push(change_op_output.clone());
    output_columns.push(data_route_output.clone());

    let mut stats = child_stats;
    stats.output_row_count *= 2.0;
    let mut root = OptimizedOperatorNode {
        op: Operator::PhysicalChangeEventExpand(ChangeEventExpandOp {
            events: vec![
                ChangeEventSpec {
                    predicate: None,
                    branch_kind:
                        crate::sql::common::change_stream::ChangeStreamBranchKind::DeleteDv,
                    assignments: delete_assignments,
                },
                ChangeEventSpec {
                    predicate: None,
                    branch_kind:
                        crate::sql::common::change_stream::ChangeStreamBranchKind::ReuseData,
                    assignments: reuse_assignments,
                },
            ],
            output_columns: output_columns.clone(),
            change_op_column_id: change_op_output.column_id,
            data_route_column_id: Some(data_route_output.column_id),
        }),
        children: vec![distributed],
        stats,
        explain_stats: OptimizerExplainStats::default(),
        output_columns,
        execution_props: PlanExecutionProps::default(),
    };
    crate::sql::optimizer::optimized_tree::attach_scalar_arena(&mut root, Arc::new(scalar_arena));
    Ok(root)
}

fn build_merge_mor_change_event_expand_plan(
    optimized_tree: crate::sql::optimizer::OptimizedOperatorNode,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
    new_sequence_number: i64,
    matched_update: bool,
    matched_delete: bool,
    not_matched_insert: bool,
) -> Result<crate::sql::optimizer::OptimizedOperatorNode, String> {
    use crate::sql::common::BinOp;
    use crate::sql::optimizer::operator::{
        ChangeEventExpandOp, ChangeEventOutputExpr, ChangeEventSpec, Operator,
        PhysicalDistributionOp,
    };
    use crate::sql::optimizer::optimized_tree::{
        OptimizedOperatorNode, OptimizerExplainStats, PlanExecutionProps,
    };
    use crate::sql::optimizer::property::DistributionSpec;
    use crate::sql::optimizer::scalar::{HashableLiteral, ScalarNode};

    let mut scalar_arena = optimized_tree
        .execution_props
        .scalar_arena
        .as_deref()
        .cloned()
        .ok_or_else(|| "MOR MERGE physical plan is missing scalar arena".to_string())?;
    let child_outputs = optimized_tree.output_columns.clone();
    let assert_key_input =
        output_column_by_name(&child_outputs, "__nr_merge_assert_key", "MERGE assert key")?;
    let hash_distribution = DistributionSpec::shuffle_agg([assert_key_input.column_id]);

    let child_stats = optimized_tree.stats.clone();
    let distributed = OptimizedOperatorNode {
        op: Operator::PhysicalDistribution(PhysicalDistributionOp {
            spec: hash_distribution,
        }),
        children: vec![optimized_tree],
        stats: child_stats.clone(),
        explain_stats: OptimizerExplainStats::default(),
        output_columns: child_outputs.clone(),
        execution_props: PlanExecutionProps::default(),
    };

    let mut next_column_id = max_physical_column_id(&distributed) + 1;
    let mut alloc_output =
        |name: &str, data_type: arrow::datatypes::DataType, nullable: bool, is_internal: bool| {
            let column = crate::sql::analysis::OutputColumn {
                column_id: crate::sql::column_id::ColumnId(next_column_id),
                name: name.to_string(),
                data_type,
                nullable,
                is_internal,
            };
            next_column_id += 1;
            column
        };

    let file_output = alloc_output(
        crate::exec::row_position::ICEBERG_FILE_PATH_COL,
        arrow::datatypes::DataType::Utf8,
        true,
        true,
    );
    let pos_output = alloc_output(
        crate::exec::row_position::ICEBERG_ROW_POS_COL,
        arrow::datatypes::DataType::Int64,
        true,
        true,
    );
    let mut target_outputs = Vec::with_capacity(target_columns.len());
    for column in target_columns {
        target_outputs.push((
            column.name.clone(),
            alloc_output(
                &column.name,
                column.data_type.clone(),
                column.nullable,
                false,
            ),
        ));
    }
    let row_id_output = alloc_output(
        crate::exec::row_position::ICEBERG_ROW_ID_COL,
        arrow::datatypes::DataType::Int64,
        true,
        true,
    );
    let last_sequence_output = alloc_output(
        crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL,
        arrow::datatypes::DataType::Int64,
        true,
        true,
    );
    let change_op_output = alloc_output(
        crate::exec::change_op::CHANGE_OP_COLUMN,
        arrow::datatypes::DataType::Int8,
        false,
        true,
    );
    let data_route_output = alloc_output(
        crate::engine::dml_change_stream::DML_CHANGE_STREAM_DATA_ROUTE_COLUMN,
        arrow::datatypes::DataType::Int32,
        true,
        true,
    );

    let file_expr = child_column_expr(
        &mut scalar_arena,
        &child_outputs,
        "__nr_file",
        "MERGE old file",
    )?;
    let pos_expr = child_column_expr(
        &mut scalar_arena,
        &child_outputs,
        "__nr_pos",
        "MERGE old row position",
    )?;
    let row_id_expr = child_column_expr(
        &mut scalar_arena,
        &child_outputs,
        "__nr_row_id",
        "MERGE old row id",
    )?;
    let new_sequence_expr = scalar_arena.intern(
        ScalarNode::Literal(HashableLiteral(crate::sql::analysis::LiteralValue::Int(
            new_sequence_number,
        ))),
        arrow::datatypes::DataType::Int64,
        false,
    );

    let mut delete_assignments = vec![
        ChangeEventOutputExpr {
            output_column_id: file_output.column_id,
            expr: Some(file_expr),
        },
        ChangeEventOutputExpr {
            output_column_id: pos_output.column_id,
            expr: Some(pos_expr),
        },
    ];
    let mut reuse_assignments = Vec::with_capacity(target_columns.len() + 2);
    let mut fresh_assignments = Vec::with_capacity(target_columns.len());
    for (name, output) in &target_outputs {
        let old_expr = child_column_expr(
            &mut scalar_arena,
            &child_outputs,
            name,
            "MERGE old target column",
        )?;
        delete_assignments.push(ChangeEventOutputExpr {
            output_column_id: output.column_id,
            expr: Some(old_expr),
        });

        let new_name = format!("__nr_new_{name}");
        let reuse_expr = match maybe_output_column_by_name(&child_outputs, &new_name)? {
            Some(column) => scalar_arena.intern(
                ScalarNode::ColumnRef(column.column_id),
                column.data_type.clone(),
                column.nullable,
            ),
            None => child_column_expr(
                &mut scalar_arena,
                &child_outputs,
                name,
                "MERGE unchanged target column",
            )?,
        };
        reuse_assignments.push(ChangeEventOutputExpr {
            output_column_id: output.column_id,
            expr: Some(reuse_expr),
        });

        let insert_name = format!("__nr_ins_{name}");
        if let Some(column) = maybe_output_column_by_name(&child_outputs, &insert_name)? {
            let fresh_expr = scalar_arena.intern(
                ScalarNode::ColumnRef(column.column_id),
                column.data_type.clone(),
                column.nullable,
            );
            fresh_assignments.push(ChangeEventOutputExpr {
                output_column_id: output.column_id,
                expr: Some(fresh_expr),
            });
        }
    }
    reuse_assignments.push(ChangeEventOutputExpr {
        output_column_id: row_id_output.column_id,
        expr: Some(row_id_expr),
    });
    reuse_assignments.push(ChangeEventOutputExpr {
        output_column_id: last_sequence_output.column_id,
        expr: Some(new_sequence_expr),
    });

    let action_predicate = |arena: &mut crate::sql::optimizer::scalar::ScalarArena,
                            action: i32|
     -> Result<crate::sql::optimizer::scalar::ScalarId, String> {
        let action_expr =
            child_column_expr(arena, &child_outputs, "__nr_merge_action", "MERGE action")?;
        let literal = arena.intern(
            ScalarNode::Literal(HashableLiteral(crate::sql::analysis::LiteralValue::Int(
                i64::from(action),
            ))),
            arrow::datatypes::DataType::Int64,
            false,
        );
        Ok(arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Eq,
                left: action_expr,
                right: literal,
            },
            arrow::datatypes::DataType::Boolean,
            false,
        ))
    };

    let mut events = Vec::new();
    if matched_update {
        let predicate = action_predicate(&mut scalar_arena, MERGE_ACTION_MATCHED_UPDATE)?;
        events.push(ChangeEventSpec {
            predicate: Some(predicate),
            branch_kind: crate::sql::common::change_stream::ChangeStreamBranchKind::DeleteDv,
            assignments: delete_assignments.clone(),
        });
        events.push(ChangeEventSpec {
            predicate: Some(predicate),
            branch_kind: crate::sql::common::change_stream::ChangeStreamBranchKind::ReuseData,
            assignments: reuse_assignments,
        });
    }
    if matched_delete {
        events.push(ChangeEventSpec {
            predicate: Some(action_predicate(
                &mut scalar_arena,
                MERGE_ACTION_MATCHED_DELETE,
            )?),
            branch_kind: crate::sql::common::change_stream::ChangeStreamBranchKind::DeleteDv,
            assignments: delete_assignments,
        });
    }
    if not_matched_insert {
        events.push(ChangeEventSpec {
            predicate: Some(action_predicate(
                &mut scalar_arena,
                MERGE_ACTION_NOT_MATCHED_INSERT,
            )?),
            branch_kind: crate::sql::common::change_stream::ChangeStreamBranchKind::FreshData,
            assignments: fresh_assignments,
        });
    }
    if events.is_empty() {
        return Err("MOR MERGE change-stream expand requires at least one event".to_string());
    }

    let mut output_columns = Vec::with_capacity(target_columns.len() + 6);
    output_columns.push(file_output);
    output_columns.push(pos_output);
    output_columns.extend(target_outputs.into_iter().map(|(_, column)| column));
    output_columns.push(row_id_output);
    output_columns.push(last_sequence_output);
    output_columns.push(change_op_output.clone());
    output_columns.push(data_route_output.clone());

    let mut stats = child_stats;
    if matched_update {
        stats.output_row_count *= 2.0;
    }
    let mut root = OptimizedOperatorNode {
        op: Operator::PhysicalChangeEventExpand(ChangeEventExpandOp {
            events,
            output_columns: output_columns.clone(),
            change_op_column_id: change_op_output.column_id,
            data_route_column_id: Some(data_route_output.column_id),
        }),
        children: vec![distributed],
        stats,
        explain_stats: OptimizerExplainStats::default(),
        output_columns,
        execution_props: PlanExecutionProps::default(),
    };
    crate::sql::optimizer::optimized_tree::attach_scalar_arena(&mut root, Arc::new(scalar_arena));
    Ok(root)
}

fn output_column_by_name(
    columns: &[crate::sql::analysis::OutputColumn],
    name: &str,
    label: &str,
) -> Result<crate::sql::analysis::OutputColumn, String> {
    maybe_output_column_by_name(columns, name)?.ok_or_else(|| {
        format!("MOR UPDATE change-stream {label} column `{name}` not found in producer output")
    })
}

fn maybe_output_column_by_name(
    columns: &[crate::sql::analysis::OutputColumn],
    name: &str,
) -> Result<Option<crate::sql::analysis::OutputColumn>, String> {
    let mut matches = columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(name));
    let Some(column) = matches.next() else {
        return Ok(None);
    };
    if matches.next().is_some() {
        return Err(format!(
            "MOR UPDATE change-stream producer column `{name}` is ambiguous"
        ));
    }
    Ok(Some(column.clone()))
}

fn child_column_expr(
    scalar_arena: &mut crate::sql::optimizer::scalar::ScalarArena,
    columns: &[crate::sql::analysis::OutputColumn],
    name: &str,
    label: &str,
) -> Result<crate::sql::optimizer::scalar::ScalarId, String> {
    use crate::sql::optimizer::scalar::ScalarNode;

    let column = output_column_by_name(columns, name, label)?;
    Ok(scalar_arena.intern(
        ScalarNode::ColumnRef(column.column_id),
        column.data_type,
        column.nullable,
    ))
}

fn max_physical_column_id(node: &crate::sql::optimizer::OptimizedOperatorNode) -> u32 {
    node.output_columns
        .iter()
        .map(|column| column.column_id.0)
        .chain(node.children.iter().map(max_physical_column_id))
        .max()
        .unwrap_or(0)
}

fn parse_generated_query(sql: &str, context: &str) -> Result<sqlparser::ast::Query, String> {
    match crate::sql::parser::parse_sql_raw(sql)? {
        sqlparser::ast::Statement::Query(query) => Ok(*query),
        other => Err(format!("{context} generated non-query statement: {other}")),
    }
}

fn qualify_iceberg_table(target: &crate::engine::backend_resolver::TargetBackend) -> String {
    format!(
        "{}.{}.{}",
        sql_identifier(&target.catalog),
        sql_identifier(&target.namespace),
        sql_identifier(&target.table)
    )
}

fn qualify_column(alias: &str, column: &str) -> String {
    format!("{}.{}", sql_identifier(alias), sql_identifier(column))
}

fn sql_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn execute_mor_update(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn Catalog>,
    table_ident: iceberg::TableIdent,
    table: iceberg::table::Table,
    stmt: &UpdateStmt,
    current_catalog: Option<&str>,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    target_ref: &str,
) -> Result<StatementResult, String> {
    // For branch DML, read partition metadata at the branch head snapshot.
    let read_snapshot_id: Option<i64> = if target_ref != "main" {
        crate::engine::delete_flow::resolve_branch_head_snapshot_id(table.metadata(), target_ref)?
    } else {
        table.metadata().current_snapshot().map(|s| s.snapshot_id())
    };
    let metadata = table.metadata();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    // The old-row deletions are written as deletion vectors on the BE and
    // committed together with the BE-written replacement data files in one
    // RowDeltaDvFromFiles snapshot. The coordinator no longer materializes
    // positions or pre-loads referenced-file partitions.
    let collector = Arc::new(
        IcebergCommitCollector::new(
            CommitOpKind::RowDeltaDvFromFiles,
            table_ident,
            read_snapshot_id,
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            staging_dir,
            crate::common::types::UniqueId { hi: 0, lo: 0 },
        )
        .with_table_metadata(metadata.clone()),
    );
    let write = build_update_mor_change_stream_write_plan(
        state,
        target,
        stmt,
        current_catalog,
        target_columns,
        target_ref,
        metadata.last_sequence_number() + 1,
    )?;
    run_mor_update_change_stream_transaction(
        state,
        target,
        catalog,
        table,
        collector,
        entry,
        read_snapshot_id,
        target_ref,
        write,
    )?;
    Ok(StatementResult::Ok)
}

struct MorUpdateChangeStreamExecutor {
    state: Arc<StandaloneState>,
    target: crate::engine::backend_resolver::TargetBackend,
    write: Mutex<Option<crate::engine::dml_change_stream::DmlChangeStreamWritePlan>>,
    commit_executor: IcebergWriteCommitExecutor,
    commit_plan: Mutex<
        Option<crate::connector::iceberg::change_stream_routing::ChangeStreamWriterCommitPlan>,
    >,
}

struct MorMergeChangeStreamExecutor {
    state: Arc<StandaloneState>,
    target: crate::engine::backend_resolver::TargetBackend,
    write: Mutex<Option<crate::engine::dml_change_stream::DmlChangeStreamWritePlan>>,
    commit_executor: IcebergWriteCommitExecutor,
    commit_plan: Mutex<
        Option<crate::connector::iceberg::change_stream_routing::ChangeStreamWriterCommitPlan>,
    >,
}

impl IcebergWriteTransactionExecutor for MorUpdateChangeStreamExecutor {
    fn run_coordinated_write(
        &self,
        _spec: &IcebergWriteTransactionSpec,
    ) -> Result<CoordinatedQueryResult, String> {
        let mut plan = self
            .write
            .lock()
            .expect("MOR UPDATE change-stream plan lock poisoned")
            .take()
            .ok_or_else(|| "MOR UPDATE change-stream plan was already consumed".to_string())?;
        let planned = crate::engine::dml_change_stream::plan_dml_change_stream_write(
            &self.state,
            &self.target,
            &mut plan,
        )?;
        let crate::engine::PlannedIcebergChangeStreamWrite {
            prepared,
            native_bundle,
            commit_plan,
            #[cfg(test)]
            topology,
        } = planned;
        *self
            .commit_plan
            .lock()
            .expect("MOR UPDATE change-stream commit plan lock poisoned") = Some(commit_plan);
        #[cfg(test)]
        if let Some(result) = crate::engine::observe_change_stream_write_build_for_test(&topology) {
            return Ok(result);
        }
        let result = crate::engine::execute_planned_iceberg_change_stream_write(
            prepared,
            native_bundle,
            None,
        )?;
        if let Some(commit) = result.write_commit.as_ref()
            && !write_commit_has_files(commit)
        {
            if commit.writers.iter().any(|writer| writer.loaded_rows > 0) {
                return Err(
                    "MOR UPDATE change-stream write produced rows but no data or DV files"
                        .to_string(),
                );
            }
            return Ok(no_mutation_write_result());
        }
        Ok(result)
    }

    fn commit(
        &self,
        _spec: &IcebergWriteTransactionSpec,
        write_commit: &WriteCommitInput,
    ) -> Result<CommitOutcome, CommitServiceError> {
        let guard = self
            .commit_plan
            .lock()
            .expect("MOR UPDATE change-stream commit plan lock poisoned");
        let plan = guard.as_ref().ok_or_else(|| {
            CommitServiceError::known_uncommitted(
                "MOR UPDATE change-stream commit plan is missing; coordinated write did not complete"
                    .to_string(),
                crate::connector::iceberg::commit::CleanupAttempt::not_attempted(),
            )
        })?;
        self.commit_executor
            .commit_change_stream_write_input(write_commit, plan)
    }

    fn finalize(&self, _spec: &IcebergWriteTransactionSpec) -> Result<(), String> {
        self.commit_executor.finalize()
    }
}

impl IcebergWriteTransactionExecutor for MorMergeChangeStreamExecutor {
    fn run_coordinated_write(
        &self,
        _spec: &IcebergWriteTransactionSpec,
    ) -> Result<CoordinatedQueryResult, String> {
        let mut plan = self
            .write
            .lock()
            .expect("MOR MERGE change-stream plan lock poisoned")
            .take()
            .ok_or_else(|| "MOR MERGE change-stream plan was already consumed".to_string())?;
        let planned = crate::engine::dml_change_stream::plan_dml_change_stream_write(
            &self.state,
            &self.target,
            &mut plan,
        )?;
        let crate::engine::PlannedIcebergChangeStreamWrite {
            prepared,
            native_bundle,
            commit_plan,
            #[cfg(test)]
            topology,
        } = planned;
        *self
            .commit_plan
            .lock()
            .expect("MOR MERGE change-stream commit plan lock poisoned") = Some(commit_plan);
        #[cfg(test)]
        if let Some(result) = crate::engine::observe_change_stream_write_build_for_test(&topology) {
            return Ok(result);
        }
        let result = crate::engine::execute_planned_iceberg_change_stream_write(
            prepared,
            native_bundle,
            None,
        )?;
        if let Some(commit) = result.write_commit.as_ref()
            && !write_commit_has_files(commit)
        {
            if commit.writers.iter().any(|writer| writer.loaded_rows > 0) {
                return Err(
                    "MOR MERGE change-stream write produced rows but no data or DV files"
                        .to_string(),
                );
            }
            return Ok(no_mutation_write_result());
        }
        Ok(result)
    }

    fn commit(
        &self,
        _spec: &IcebergWriteTransactionSpec,
        write_commit: &WriteCommitInput,
    ) -> Result<CommitOutcome, CommitServiceError> {
        let guard = self
            .commit_plan
            .lock()
            .expect("MOR MERGE change-stream commit plan lock poisoned");
        let plan = guard.as_ref().ok_or_else(|| {
            CommitServiceError::known_uncommitted(
                "MOR MERGE change-stream commit plan is missing; coordinated write did not complete"
                    .to_string(),
                crate::connector::iceberg::commit::CleanupAttempt::not_attempted(),
            )
        })?;
        self.commit_executor
            .commit_change_stream_write_input(write_commit, plan)
    }

    fn finalize(&self, _spec: &IcebergWriteTransactionSpec) -> Result<(), String> {
        self.commit_executor.finalize()
    }
}

/// N-ary fold for MERGE branch writes: concatenate every part's
/// `WriteCommitInput.writers` into one transaction-wide `WriteCommitInput` so a
/// single collector drives one commit. Used by the folded multi-branch MERGE
/// executor (matched data/DV writers all share one collector). The first
/// part's `query_result` is kept (callers only need a sentinel here). If any
/// part reported a `write_abort`, the first such abort is propagated so the
/// transaction runner can clean up and discard the partial commit.
fn merge_all_write_commits(
    parts: Vec<CoordinatedQueryResult>,
) -> Result<CoordinatedQueryResult, String> {
    if parts.is_empty() {
        return Err("merge_all_write_commits requires at least one part".to_string());
    }
    let mut write_abort = None;
    let mut query_result = None;
    let mut merged_commit: Option<WriteCommitInput> = None;
    for part in parts {
        if write_abort.is_none() {
            write_abort = part.write_abort;
        }
        if query_result.is_none() {
            query_result = Some(part.query_result);
        }
        if let Some(commit) = part.write_commit {
            match merged_commit.as_mut() {
                Some(existing) => existing.writers.extend(commit.writers),
                None => merged_commit = Some(commit),
            }
        }
    }
    Ok(CoordinatedQueryResult {
        query_result: query_result.expect("non-empty parts => query_result set"),
        write_commit: merged_commit,
        write_abort,
        fragment_profiles: Vec::new(),
        runtime_filter_dormancy_proof: None,
    })
}

#[allow(clippy::too_many_arguments)]
fn run_mor_update_change_stream_transaction(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn Catalog>,
    table: iceberg::table::Table,
    collector: Arc<IcebergCommitCollector>,
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    base_snapshot_id: Option<i64>,
    target_ref: &str,
    write: crate::engine::dml_change_stream::DmlChangeStreamWritePlan,
) -> Result<(), String> {
    let abort_cleanup =
        crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
    let commit_executor = IcebergWriteCommitExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        catalog,
        table,
        collector,
        fs: abort_cleanup.fs,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: target_ref.to_string(),
        snapshot_properties: BTreeMap::new(),
    };
    let spec = IcebergWriteTransactionSpec {
        target: IcebergOperationTarget {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            ref_name: (target_ref != "main").then(|| target_ref.to_string()),
        },
        operation_kind: IcebergOperationKind::RowDelta,
        attempt_id: format!(
            "{}.{}.{}:mor-update-change-stream:{}",
            target.catalog,
            target.namespace,
            target.table,
            uuid::Uuid::new_v4()
        ),
        commit: IcebergWriteCommitPolicy {
            commit_op_kind: CommitOpKind::RowDeltaDvFromFiles,
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
    let executor = MorUpdateChangeStreamExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        write: Mutex::new(Some(write)),
        commit_executor,
        commit_plan: Mutex::new(None),
    };
    let runner = IcebergWriteTransactionRunner::new(Arc::clone(state), &executor);
    let _outcome = runner.run(spec)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_mor_merge_change_stream_transaction(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn Catalog>,
    table: iceberg::table::Table,
    collector: Arc<IcebergCommitCollector>,
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    base_snapshot_id: Option<i64>,
    target_ref: &str,
    write: crate::engine::dml_change_stream::DmlChangeStreamWritePlan,
) -> Result<(), String> {
    let abort_cleanup =
        crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
    let commit_executor = IcebergWriteCommitExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        catalog,
        table,
        collector,
        fs: abort_cleanup.fs,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: target_ref.to_string(),
        snapshot_properties: BTreeMap::new(),
    };
    let spec = IcebergWriteTransactionSpec {
        target: IcebergOperationTarget {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            ref_name: (target_ref != "main").then(|| target_ref.to_string()),
        },
        operation_kind: IcebergOperationKind::RowDelta,
        attempt_id: format!(
            "{}.{}.{}:mor-merge-change-stream:{}",
            target.catalog,
            target.namespace,
            target.table,
            uuid::Uuid::new_v4()
        ),
        commit: IcebergWriteCommitPolicy {
            commit_op_kind: CommitOpKind::RowDeltaDvFromFiles,
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
    let executor = MorMergeChangeStreamExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        write: Mutex::new(Some(write)),
        commit_executor,
        commit_plan: Mutex::new(None),
    };
    let runner = IcebergWriteTransactionRunner::new(Arc::clone(state), &executor);
    let _outcome = runner.run(spec)?;
    Ok(())
}

fn no_mutation_write_result() -> CoordinatedQueryResult {
    CoordinatedQueryResult {
        query_result: QueryResult::empty(),
        write_commit: None,
        write_abort: None,
        fragment_profiles: Vec::new(),
        runtime_filter_dormancy_proof: None,
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_cow_update(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn Catalog>,
    table_ident: iceberg::TableIdent,
    table: iceberg::table::Table,
    matched: MatchedUpdateBatch,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    target_ref: &str,
) -> Result<StatementResult, String> {
    if matched.row_ids.is_empty() {
        return Ok(StatementResult::Ok);
    }
    validate_unique_target_row_ids(&matched.row_ids)?;
    let metadata = table.metadata();
    // For branch DML, commit against the branch head snapshot.
    let base_snapshot_id: Option<i64> = if target_ref != "main" {
        crate::engine::delete_flow::resolve_branch_head_snapshot_id(metadata, target_ref)?
    } else {
        metadata.current_snapshot().map(|s| s.snapshot_id())
    };
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let collector = Arc::new(
        IcebergCommitCollector::new(
            CommitOpKind::CowUpdate,
            table_ident,
            base_snapshot_id,
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            staging_dir,
            crate::common::types::UniqueId { hi: 0, lo: 0 },
        )
        .with_table_metadata(metadata.clone()),
    );
    // Plan the distributed whole-file rewrite: one coordinated BE write per
    // touched data file, scoped to that file via an `ExplicitFiles`-bound scan.
    // The FE never reads/rewrites data; it only commits the BE-written files
    // through the unchanged `CowUpdateCommit`.
    let write = build_cow_update_distributed_write(
        state,
        target,
        &table,
        &matched,
        target_columns,
        &entry,
        base_snapshot_id,
    )?;
    run_cow_update_distributed_transaction(
        state,
        target,
        catalog,
        table,
        collector,
        entry,
        base_snapshot_id,
        target_ref,
        write,
    )?;
    Ok(StatementResult::Ok)
}

/// Per-touched-file plan for the distributed COW UPDATE rewrite. Each entry
/// describes one BE write scoped to exactly one old data file: the synthetic
/// `ExplicitFiles`-bound table to register before the write (and drop after),
/// the rewrite SELECT that re-emits every row of that file (replacing matched
/// rows with their new values and preserving `_row_id`), and the matched row
/// ids that live in this file (recorded on the resulting `CowUpdateTouchedFile`).
struct CowFileRewritePlan {
    old_file: String,
    namespace: String,
    synthetic_table_name: String,
    synthetic_table_def: crate::sql::planner::table::TableDef,
    rewrite_query: sqlparser::ast::Query,
    matched_row_ids: Vec<i64>,
}

/// Fully-planned distributed COW UPDATE write: the per-file rewrite plans, the
/// shared row-lineage data sink spec, and the commit-side rewrite-set identity.
struct CowUpdateDistributedWrite {
    file_plans: Vec<CowFileRewritePlan>,
    data_sink_spec: IcebergWriteSinkSpec,
    base_snapshot_id: i64,
    target_table_uuid: String,
    updated_row_ids: Vec<i64>,
}

#[allow(clippy::too_many_arguments)]
fn build_cow_update_distributed_write(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    table: &iceberg::table::Table,
    matched: &MatchedUpdateBatch,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    base_snapshot_id: Option<i64>,
) -> Result<CowUpdateDistributedWrite, String> {
    let base_snapshot_id =
        base_snapshot_id.ok_or_else(|| "COW UPDATE requires a current snapshot".to_string())?;
    let resolved = {
        let registry = state.connectors.read().expect("connector registry read");
        let backend = registry.catalog_backend("iceberg")?;
        backend.load_table(&target.catalog, &target.namespace, &target.table)?
    };
    let data_sink_spec = crate::engine::iceberg_writer::build_row_lineage_data_sink_spec(
        target, &resolved, table, entry,
    )?;

    // Index the snapshot's data files by path so each touched file inherits its
    // `first_row_id` / `data_sequence_number` / pre-existing delete files. The
    // BE scan computes `_row_id = first_row_id + _pos` and honors these deletes,
    // so the rewrite re-emits exactly the rows that were live in the file.
    let data_files =
        crate::connector::iceberg::catalog::registry::extract_data_files_with_stats_at(
            table,
            base_snapshot_id,
        )?;
    let mut data_file_by_path = std::collections::HashMap::with_capacity(data_files.len());
    for file in data_files {
        data_file_by_path.insert(file.path.clone(), file);
    }

    // Group matched rows by their owning data file, preserving the new-row batch
    // index so the rewrite query can project the replacement values.
    let mut matched_rows_by_file: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (idx, file_path) in matched.file_paths.iter().enumerate() {
        matched_rows_by_file
            .entry(file_path.clone())
            .or_default()
            .push(idx);
    }

    let new_sequence_number = table.metadata().last_sequence_number() + 1;
    let mut file_plans = Vec::with_capacity(matched_rows_by_file.len());
    for (old_file, matched_indices) in matched_rows_by_file {
        let data_file = data_file_by_path.get(&old_file).cloned().ok_or_else(|| {
            format!("COW UPDATE matched data file `{old_file}` is missing from snapshot metadata")
        })?;
        let synthetic_table_name = format!(
            "__nr_cow_{}_{}",
            target.table,
            uuid::Uuid::new_v4().simple()
        );
        let synthetic_table_def =
            build_cow_rewrite_synthetic_table_def(entry, target, &synthetic_table_name, data_file)?;
        let matched_row_ids = matched_indices
            .iter()
            .map(|idx| matched.row_ids[*idx])
            .collect::<Vec<_>>();
        let rewrite_query = build_cow_rewrite_query(
            target,
            &synthetic_table_name,
            matched,
            &matched_indices,
            target_columns,
            new_sequence_number,
        )?;
        file_plans.push(CowFileRewritePlan {
            old_file,
            namespace: target.namespace.clone(),
            synthetic_table_name,
            synthetic_table_def,
            rewrite_query,
            matched_row_ids,
        });
    }

    Ok(CowUpdateDistributedWrite {
        file_plans,
        data_sink_spec,
        base_snapshot_id,
        target_table_uuid: table.metadata().uuid().to_string(),
        updated_row_ids: matched.row_ids.clone(),
    })
}

/// Build a synthetic `ExplicitFiles`-bound `TableDef` over exactly one data
/// file. The single-file scan inherits the file's `first_row_id`,
/// `data_sequence_number`, and pre-existing delete files so the BE reads the
/// live rows and exposes the v3 row-lineage `_row_id` /
/// `_last_updated_sequence_number` virtual columns the rewrite query projects.
fn build_cow_rewrite_synthetic_table_def(
    entry: &crate::connector::iceberg::catalog::IcebergCatalogEntry,
    target: &crate::engine::backend_resolver::TargetBackend,
    synthetic_table_name: &str,
    data_file: crate::connector::iceberg::catalog::registry::DataFileWithStats,
) -> Result<crate::sql::planner::table::TableDef, String> {
    if data_file.first_row_id.is_none() {
        return Err(format!(
            "COW UPDATE requires first_row_id for iceberg data file `{}`",
            data_file.path
        ));
    }
    let loaded =
        crate::connector::iceberg::catalog::load_table(entry, &target.namespace, &target.table)?;
    let table_def = crate::connector::iceberg::catalog::build_iceberg_table_def_with_files(
        entry,
        &target.catalog,
        &target.namespace,
        &target.table,
        loaded,
        vec![data_file],
    )?;
    // The single-file scan must expose `_row_id` / `_last_updated_sequence_number`
    // for the rewrite projection; the table is v3 row-lineage (COW mode was
    // selected) and the file carries `first_row_id`, so the builder advertises
    // them. Guard against a silent drop.
    if !table_def
        .iceberg_row_lineage_metadata_columns
        .iter()
        .any(|c| crate::exec::row_position::is_iceberg_row_id(&c.name))
    {
        return Err(format!(
            "COW UPDATE synthetic scan for table {}.{} does not expose _row_id; \
             the data file lacks v3 row-lineage metadata",
            target.namespace, target.table
        ));
    }
    Ok(crate::sql::planner::table::TableDef {
        name: synthetic_table_name.to_string(),
        ..table_def
    })
}

/// Build the whole-file rewrite SELECT for one touched data file (approach
/// "drive-from-matched"): scan every live row of the file via the synthetic
/// `ExplicitFiles` table, LEFT JOIN the matched new rows that belong to this
/// file on `_row_id`, and project user columns (replacement value where
/// matched, original otherwise) plus `_row_id` and a conditional
/// `_last_updated_sequence_number`. Ordered by `_row_id` for deterministic
/// output. The matched new values come from the already-materialized
/// `matched.new_rows`, so this path is uniform for both UPDATE and
/// MERGE matched-UPDATE (no source re-join).
fn build_cow_rewrite_query(
    target: &crate::engine::backend_resolver::TargetBackend,
    synthetic_table_name: &str,
    matched: &MatchedUpdateBatch,
    matched_indices: &[usize],
    target_columns: &[novarocks_catalog::schema::ColumnDef],
    new_sequence_number: i64,
) -> Result<sqlparser::ast::Query, String> {
    if matched_indices.is_empty() {
        return Err("COW UPDATE rewrite query requires at least one matched row".to_string());
    }
    let scan_alias = "__nr_cow_t";
    let match_alias = "__nr_cow_m";
    let row_id_col = crate::exec::row_position::ICEBERG_ROW_ID_COL;
    let last_seq_col = crate::exec::row_position::ICEBERG_LAST_UPDATED_SEQ_COL;

    // VALUES relation of the matched new rows in this file: (_row_id, <user
    // columns...>). Values are typed literals read positionally from the
    // already-materialized `new_rows` batch (mirrors the MERGE MOR data sink).
    let mut value_rows = Vec::with_capacity(matched_indices.len());
    for &idx in matched_indices {
        let mut values = Vec::with_capacity(target_columns.len() + 1);
        values.push(matched.row_ids[idx].to_string());
        for target_column in target_columns {
            let col_idx = matched
                .new_rows
                .schema()
                .index_of(&target_column.name)
                .map_err(|_| {
                    format!(
                        "COW UPDATE new-row batch missing target column `{}`",
                        target_column.name
                    )
                })?;
            let literal =
                crate::sql::literal::literal_from_batch(matched.new_rows.column(col_idx), idx)?;
            values.push(literal_to_sql_for_values_target_column(
                &literal,
                target_column,
            )?);
        }
        value_rows.push(format!("({})", values.join(", ")));
    }
    let mut match_value_columns = Vec::with_capacity(target_columns.len() + 1);
    match_value_columns.push(sql_identifier(row_id_col));
    for target_column in target_columns {
        match_value_columns.push(sql_identifier(&target_column.name));
    }
    let values_sql = format!(
        "(VALUES {}) AS {}({})",
        value_rows.join(", "),
        sql_identifier(match_alias),
        match_value_columns.join(", ")
    );

    let matched_predicate = format!("{} IS NOT NULL", qualify_column(match_alias, row_id_col));

    let mut select_items = Vec::with_capacity(target_columns.len() + 2);
    for column in target_columns {
        // Replacement value where the row matched, original scan value
        // otherwise. The CASE result is cast to the target column type so the
        // sink sees the declared schema (mirrors the MOR/MERGE data sinks).
        let case_expr = format!(
            "CASE WHEN {matched_predicate} THEN {} ELSE {} END",
            qualify_column(match_alias, &column.name),
            qualify_column(scan_alias, &column.name),
        );
        select_items.push(format!(
            "{} AS {}",
            crate::engine::iceberg_writer::target_cast_expr_sql(&case_expr, column)?,
            sql_identifier(&column.name)
        ));
    }
    select_items.push(format!(
        "{} AS {}",
        qualify_column(scan_alias, row_id_col),
        sql_identifier(row_id_col)
    ));
    // Matched rows advance to the new sequence number; untouched rows keep the
    // per-row `_last_updated_sequence_number` the scan synthesized from the
    // file's data sequence number.
    select_items.push(format!(
        "CAST(CASE WHEN {matched_predicate} THEN {} ELSE {} END AS BIGINT) AS {}",
        new_sequence_number,
        qualify_column(scan_alias, last_seq_col),
        sql_identifier(last_seq_col)
    ));

    // Reference the synthetic table explicitly under `default_catalog` so a
    // session-level Iceberg current catalog cannot route it back through the
    // CatalogMgr entry (mirrors the time-travel rewrite).
    let scan_sql = format!(
        "{}.{}.{} AS {}",
        sql_identifier("default_catalog"),
        sql_identifier(&target.namespace),
        sql_identifier(synthetic_table_name),
        sql_identifier(scan_alias),
    );
    let sql = format!(
        "SELECT {} FROM {} LEFT JOIN {} ON {} = {} ORDER BY {}",
        select_items.join(", "),
        scan_sql,
        values_sql,
        qualify_column(scan_alias, row_id_col),
        qualify_column(match_alias, row_id_col),
        qualify_column(scan_alias, row_id_col),
    );
    parse_generated_query(&sql, "COW UPDATE rewrite")
}

fn literal_to_sql_for_values_target_column(
    literal: &crate::sql::parser::ast::Literal,
    target_column: &novarocks_catalog::schema::ColumnDef,
) -> Result<String, String> {
    let literal_sql = crate::engine::iceberg_writer::literal_to_sql_for_arrow_type(
        literal,
        &target_column.data_type,
    )?;
    if matches!(target_column.data_type, DataType::LargeBinary) {
        crate::engine::iceberg_writer::target_cast_expr_sql(&literal_sql, target_column)
    } else {
        Ok(literal_sql)
    }
}

struct DistributedCowUpdateExecutor {
    state: Arc<StandaloneState>,
    target: crate::engine::backend_resolver::TargetBackend,
    write: Mutex<Option<CowUpdateDistributedWrite>>,
    commit_executor: IcebergWriteCommitExecutor,
    cow_update_rewrite: Mutex<Option<CowUpdateRewriteSet>>,
}

impl IcebergWriteTransactionExecutor for DistributedCowUpdateExecutor {
    fn run_coordinated_write(
        &self,
        _spec: &IcebergWriteTransactionSpec,
    ) -> Result<CoordinatedQueryResult, String> {
        let write = self
            .write
            .lock()
            .expect("COW UPDATE write plan lock poisoned")
            .take()
            .ok_or_else(|| "COW UPDATE write plan was already consumed".to_string())?;
        if write.file_plans.is_empty() {
            return Ok(no_mutation_write_result());
        }

        let rewrite = run_cow_update_file_rewrites(
            &self.state,
            &self.target,
            write,
            self.commit_executor.table.metadata(),
            &self.commit_executor.collector,
            // Pure UPDATE appends no net-new data files; only a folded MERGE
            // not-matched INSERT (M3b) populates `appended_files`.
            Vec::new(),
        )?;

        let write_commit = rewrite.write_commit;
        *self
            .cow_update_rewrite
            .lock()
            .expect("COW UPDATE rewrite lock poisoned") = Some(rewrite.rewrite_set);

        Ok(CoordinatedQueryResult {
            query_result: QueryResult::empty(),
            write_commit: Some(write_commit),
            write_abort: None,
            fragment_profiles: Vec::new(),
            runtime_filter_dormancy_proof: None,
        })
    }

    fn commit(
        &self,
        _spec: &IcebergWriteTransactionSpec,
        write_commit: &WriteCommitInput,
    ) -> Result<CommitOutcome, CommitServiceError> {
        let commit_executor = IcebergWriteCommitExecutor {
            state: Arc::clone(&self.commit_executor.state),
            target: self.commit_executor.target.clone(),
            catalog: Arc::clone(&self.commit_executor.catalog),
            table: self.commit_executor.table.clone(),
            collector: Arc::clone(&self.commit_executor.collector),
            fs: self.commit_executor.fs.clone(),
            cleanup_path_mapper: self.commit_executor.cleanup_path_mapper.clone(),
            cow_update_rewrite: self
                .cow_update_rewrite
                .lock()
                .expect("COW UPDATE rewrite lock poisoned")
                .clone(),
            target_ref: self.commit_executor.target_ref.clone(),
            snapshot_properties: self.commit_executor.snapshot_properties.clone(),
        };
        commit_executor.commit_write_input(write_commit)
    }

    fn finalize(&self, _spec: &IcebergWriteTransactionSpec) -> Result<(), String> {
        self.commit_executor.finalize()
    }
}

/// One file's BE rewrite output: the replacement data-file paths (for the
/// `CowUpdateTouchedFile.new_files` mapping) and the coordinated write commit
/// that carries them.
struct CowFileRewriteOutput {
    paths: Vec<String>,
    write_commit: Option<WriteCommitInput>,
}

/// Fully-run COW UPDATE rewrite: the transaction-wide `WriteCommitInput` (every
/// touched file's replacement data files) and the commit-side rewrite-set
/// identity (touched files + any net-new appended INSERT data).
struct CowUpdateRewriteRun {
    write_commit: WriteCommitInput,
    rewrite_set: CowUpdateRewriteSet,
}

/// Run every per-file BE rewrite of a planned COW UPDATE into one shared
/// collector, returning the merged writer commit and the assembled
/// `CowUpdateRewriteSet`. Shared by the standalone COW UPDATE executor (no
/// appended files) and the folded MERGE executor (which passes the not-matched
/// INSERT's net-new data files as `appended_files`). The INSERT writers must be
/// injected into the SAME collector's flat `written` channel by the caller so
/// `CowUpdateCommit`'s written-set reconciliation (every written file is a
/// rewrite output or a declared appended file) holds.
fn run_cow_update_file_rewrites(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    write: CowUpdateDistributedWrite,
    metadata: &iceberg::spec::TableMetadata,
    collector: &Arc<IcebergCommitCollector>,
    appended_files: Vec<crate::connector::iceberg::commit::WrittenFile>,
) -> Result<CowUpdateRewriteRun, String> {
    let mut merged_commit: Option<WriteCommitInput> = None;
    let mut touched_data_files = Vec::with_capacity(write.file_plans.len());
    for plan in write.file_plans {
        let new_files = run_one_cow_file_rewrite(
            state,
            target,
            &plan,
            &write.data_sink_spec,
            metadata,
            collector,
        )?;
        // Merge this file's writer commits into the single transaction-wide
        // `WriteCommitInput`; the collector turns all of them into committed
        // data files in one `CowUpdateCommit`.
        if let Some(commit) = new_files.write_commit {
            match merged_commit.as_mut() {
                Some(existing) => existing.writers.extend(commit.writers),
                None => merged_commit = Some(commit),
            }
        }
        touched_data_files.push(CowUpdateTouchedFile {
            old_file: plan.old_file,
            new_files: new_files.paths,
            row_ids: plan.matched_row_ids,
        });
    }

    let write_commit = merged_commit.ok_or_else(|| {
        "COW UPDATE distributed rewrite produced no replacement data files".to_string()
    })?;
    if !write_commit_has_files(&write_commit) {
        return Err(
            "COW UPDATE distributed rewrite produced no replacement data files".to_string(),
        );
    }

    Ok(CowUpdateRewriteRun {
        write_commit,
        rewrite_set: CowUpdateRewriteSet {
            base_snapshot_id: write.base_snapshot_id,
            target_table_uuid: write.target_table_uuid,
            updated_row_ids: write.updated_row_ids,
            touched_data_files,
            appended_files,
        },
    })
}

/// Register the synthetic single-file table, run the scoped BE rewrite, and
/// always drop the synthetic table afterwards (even on error). The write's
/// reported data-file paths become this old file's `new_files`.
fn run_one_cow_file_rewrite(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    plan: &CowFileRewritePlan,
    data_sink_spec: &IcebergWriteSinkSpec,
    metadata: &iceberg::spec::TableMetadata,
    collector: &Arc<IcebergCommitCollector>,
) -> Result<CowFileRewriteOutput, String> {
    crate::engine::query_prep::register_synthetic_table_for_query(
        state,
        &plan.namespace,
        plan.synthetic_table_def.clone(),
    )?;
    let result = crate::engine::execute_query_as_iceberg_write(
        state,
        Some(&target.catalog),
        &target.namespace,
        &plan.rewrite_query,
        data_sink_spec.clone(),
        None,
        None,
    );
    let drop_result = crate::engine::query_prep::drop_local_table_registration_if_exists(
        state,
        &plan.namespace,
        &plan.synthetic_table_name,
    );
    let result = result?;
    drop_result?;

    if let Some(abort) = &result.write_abort {
        return Err(format!(
            "COW UPDATE rewrite for data file `{}` aborted: {}",
            plan.old_file, abort.reason
        ));
    }
    let write_commit = result.write_commit.filter(write_commit_has_files);
    let Some(commit) = write_commit else {
        return Err(format!(
            "COW UPDATE rewrite for data file `{}` produced no replacement data files \
             (rows={}, query={})",
            plan.old_file,
            result.query_result.row_count(),
            plan.rewrite_query
        ));
    };
    // Extract the replacement file paths from the writer reports. These go
    // through the same domain conversion the commit collector uses, so the
    // recorded `new_files` paths match
    // the collector's `written` paths exactly (CowUpdateCommit requires
    // bidirectional set equality).
    let mut paths = Vec::new();
    for writer in &commit.writers {
        let reports = crate::runtime::sink_commit::iceberg_commit_infos_to_writer_reports(
            writer.iceberg_commits.clone(),
            metadata,
        )?;
        for report in reports {
            let file = collector.convert_writer_report(report)?;
            paths.push(file.path);
        }
    }
    if paths.is_empty() {
        return Err(format!(
            "COW UPDATE rewrite for data file `{}` produced no replacement data files \
             (rows={}, query={})",
            plan.old_file,
            result.query_result.row_count(),
            plan.rewrite_query
        ));
    }
    Ok(CowFileRewriteOutput {
        paths,
        write_commit: Some(commit),
    })
}

#[allow(clippy::too_many_arguments)]
fn run_cow_update_distributed_transaction(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    catalog: Arc<dyn Catalog>,
    table: iceberg::table::Table,
    collector: Arc<IcebergCommitCollector>,
    entry: crate::connector::iceberg::catalog::IcebergCatalogEntry,
    base_snapshot_id: Option<i64>,
    target_ref: &str,
    write: CowUpdateDistributedWrite,
) -> Result<(), String> {
    let abort_cleanup =
        crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
    let commit_executor = IcebergWriteCommitExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        catalog,
        table,
        collector,
        fs: abort_cleanup.fs,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: target_ref.to_string(),
        snapshot_properties: BTreeMap::new(),
    };
    let spec = IcebergWriteTransactionSpec {
        target: IcebergOperationTarget {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            ref_name: (target_ref != "main").then(|| target_ref.to_string()),
        },
        operation_kind: IcebergOperationKind::RowDelta,
        attempt_id: format!(
            "{}.{}.{}:cow-update-distributed:{}",
            target.catalog,
            target.namespace,
            target.table,
            uuid::Uuid::new_v4()
        ),
        commit: IcebergWriteCommitPolicy {
            commit_op_kind: CommitOpKind::CowUpdate,
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
    let executor = DistributedCowUpdateExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        write: Mutex::new(Some(write)),
        commit_executor,
        cow_update_rewrite: Mutex::new(None),
    };
    let runner = IcebergWriteTransactionRunner::new(Arc::clone(state), &executor);
    let _outcome = runner.run(spec)?;
    Ok(())
}

struct MatchedUpdateBatch {
    row_ids: Vec<i64>,
    file_paths: Vec<String>,
    row_positions: Vec<i64>,
    old_rows: RecordBatch,
    new_rows: RecordBatch,
}

fn execute_update_match_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    sql: &str,
    current_database: &str,
) -> Result<MatchedUpdateBatch, String> {
    let statement = crate::sql::parser::parse_sql_raw(sql)?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("internal UPDATE match query was not a SELECT".to_string());
    };
    let result = crate::engine::execute_query_with_catalog_service(
        state,
        current_catalog,
        current_database,
        &query,
        None,
    )?;
    matched_update_batch_from_query_result(result)
}

fn matched_update_batch_from_query_result(
    result: QueryResult,
) -> Result<MatchedUpdateBatch, String> {
    let Some(first_chunk) = result.chunks.first() else {
        return empty_matched_update_batch();
    };
    let schema = first_chunk.batch.schema();
    let batches = result
        .chunks
        .iter()
        .map(|chunk| chunk.batch.clone())
        .collect::<Vec<_>>();
    let batch = concat_batches(&schema, batches.iter())
        .map_err(|e| format!("concatenate UPDATE match batches failed: {e}"))?;
    matched_update_batch_from_record_batch(&batch)
}

fn matched_update_batch_from_record_batch(
    batch: &RecordBatch,
) -> Result<MatchedUpdateBatch, String> {
    if batch.num_rows() == 0 {
        return empty_matched_update_batch();
    }

    let file_col = cast(required_column(batch, "__nr_file")?, &DataType::Utf8)
        .map_err(|e| format!("cast __nr_file to Utf8 failed: {e}"))?;
    let pos_col = cast(required_column(batch, "__nr_pos")?, &DataType::Int64)
        .map_err(|e| format!("cast __nr_pos to Int64 failed: {e}"))?;
    let row_id_col = cast(required_column(batch, "__nr_row_id")?, &DataType::Int64)
        .map_err(|e| format!("cast __nr_row_id to Int64 failed: {e}"))?;
    let file_arr = file_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "__nr_file was not Utf8 after cast".to_string())?;
    let pos_arr = pos_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "__nr_pos was not Int64 after cast".to_string())?;
    let row_id_arr = row_id_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| "__nr_row_id was not Int64 after cast".to_string())?;

    let mut file_paths = Vec::with_capacity(batch.num_rows());
    let mut row_positions = Vec::with_capacity(batch.num_rows());
    let mut row_ids = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        if file_arr.is_null(row) || pos_arr.is_null(row) || row_id_arr.is_null(row) {
            return Err("UPDATE match query produced null row identity columns".to_string());
        }
        file_paths.push(file_arr.value(row).to_string());
        row_positions.push(pos_arr.value(row));
        row_ids.push(row_id_arr.value(row));
    }

    let old_indices = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !field.name().starts_with("__nr_"))
        .map(|(idx, _)| idx)
        .collect::<Vec<_>>();
    let old_fields = old_indices
        .iter()
        .map(|idx| batch.schema().field(*idx).clone())
        .collect::<Vec<_>>();
    let old_schema = Arc::new(Schema::new(old_fields));
    let old_columns = old_indices
        .iter()
        .map(|idx| batch.column(*idx).clone())
        .collect::<Vec<_>>();
    let old_rows = RecordBatch::try_new(old_schema.clone(), old_columns)
        .map_err(|e| format!("build UPDATE old-row batch failed: {e}"))?;

    let mut new_columns = Vec::with_capacity(old_schema.fields().len());
    for (old_idx, field) in old_indices.iter().zip(old_schema.fields().iter()) {
        let new_name = format!("__nr_new_{}", field.name());
        let column = match batch.schema().index_of(&new_name) {
            Ok(idx) => cast(batch.column(idx), field.data_type()).map_err(|e| {
                format!(
                    "cast UPDATE assignment column `{new_name}` to {:?} failed: {e}",
                    field.data_type()
                )
            })?,
            Err(_) => batch.column(*old_idx).clone(),
        };
        new_columns.push(column);
    }
    let new_rows = RecordBatch::try_new(old_schema, new_columns)
        .map_err(|e| format!("build UPDATE new-row batch failed: {e}"))?;

    Ok(MatchedUpdateBatch {
        row_ids,
        file_paths,
        row_positions,
        old_rows,
        new_rows,
    })
}

fn empty_matched_update_batch() -> Result<MatchedUpdateBatch, String> {
    let schema = Arc::new(Schema::empty());
    let empty = RecordBatch::new_empty(schema);
    Ok(MatchedUpdateBatch {
        row_ids: Vec::new(),
        file_paths: Vec::new(),
        row_positions: Vec::new(),
        old_rows: empty.clone(),
        new_rows: empty,
    })
}

fn required_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef, String> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| format!("UPDATE match query missing `{name}` column"))?;
    Ok(batch.column(idx))
}

fn iceberg_table_columns(
    table: &iceberg::table::Table,
) -> Result<Vec<novarocks_catalog::schema::ColumnDef>, String> {
    let arrow_schema = schema_to_arrow_schema(table.metadata().current_schema())
        .map_err(|e| format!("convert iceberg schema to arrow schema failed: {e}"))?;
    let iceberg_schema = table.metadata().current_schema();
    arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let nested = iceberg_schema
                .field_by_name(field.name())
                .ok_or_else(|| format!("iceberg column `{}` missing from schema", field.name()))?;
            let data_type = match nested.field_type.as_ref() {
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Variant) => {
                    DataType::LargeBinary
                }
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Binary) => {
                    DataType::Binary
                }
                _ => field.data_type().clone(),
            };
            Ok(novarocks_catalog::schema::ColumnDef {
                name: field.name().clone(),
                data_type,
                nullable: field.is_nullable(),
                write_default: None,
                logical_type: None,
            })
        })
        .collect()
}

fn iceberg_partition_source_columns(table: &iceberg::table::Table) -> Result<Vec<String>, String> {
    let schema = table.metadata().current_schema();
    let mut out = Vec::new();
    for field in table.metadata().default_partition_spec().fields() {
        let source = schema.field_by_id(field.source_id).ok_or_else(|| {
            format!(
                "partition source field id {} is missing from iceberg schema",
                field.source_id
            )
        })?;
        out.push(source.name.clone());
    }
    Ok(out)
}

fn validate_update_assignments(
    assignments: &[crate::sql::parser::ast::UpdateAssignment],
    target_columns: &[novarocks_catalog::schema::ColumnDef],
    partition_columns: &[String],
) -> Result<(), String> {
    let target_names = target_columns
        .iter()
        .map(|c| c.name.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    let partition_names = partition_columns
        .iter()
        .map(|c| c.to_ascii_lowercase())
        .collect::<std::collections::HashSet<_>>();
    let mut seen = std::collections::HashSet::new();
    for assignment in assignments {
        let name = assignment.column.to_ascii_lowercase();
        if matches!(
            name.as_str(),
            "_row_id" | "_last_updated_sequence_number" | "_file" | "_pos"
        ) {
            return Err(format!(
                "UPDATE cannot assign reserved Iceberg metadata column `{}`",
                assignment.column
            ));
        }
        if !target_names.contains(&name) {
            return Err(format!(
                "UPDATE assignment references unknown target column `{}`",
                assignment.column
            ));
        }
        if partition_names.contains(&name) {
            return Err(format!(
                "UPDATE cannot modify Iceberg partition column `{}` in the first implementation",
                assignment.column
            ));
        }
        if !seen.insert(name) {
            return Err(format!(
                "UPDATE assignment lists target column `{}` more than once",
                assignment.column
            ));
        }
    }
    Ok(())
}

fn validate_unique_target_row_ids(row_ids: &[i64]) -> Result<(), String> {
    let mut seen = std::collections::HashSet::new();
    for row_id in row_ids {
        if !seen.insert(*row_id) {
            return Err(format!(
                "UPDATE source matched target row _row_id={} more than once; deduplicate the source before retrying",
                row_id
            ));
        }
    }
    Ok(())
}

fn build_update_match_query_sql(
    target_sql: &str,
    target_alias: &str,
    source_sql: Option<&str>,
    assignments_sql: &[(&str, &str)],
    where_sql: Option<&str>,
) -> String {
    let qualify = |column: &str| {
        if target_alias.is_empty() {
            column.to_string()
        } else {
            format!("{target_alias}.{column}")
        }
    };
    let star = if target_alias.is_empty() {
        "*".to_string()
    } else {
        format!("{target_alias}.*")
    };
    let mut select_items = vec![
        format!("{} AS __nr_file", qualify("_file")),
        format!("{} AS __nr_pos", qualify("_pos")),
        format!("{} AS __nr_row_id", qualify("_row_id")),
        format!(
            "{} AS __nr_last_updated_sequence_number",
            qualify("_last_updated_sequence_number")
        ),
        star,
    ];
    for (column, expr) in assignments_sql {
        select_items.push(format!("{expr} AS __nr_new_{column}"));
    }
    let mut sql = format!("SELECT {} FROM {target_sql}", select_items.join(", "));
    if let Some(source) = source_sql {
        sql.push_str(" CROSS JOIN ");
        sql.push_str(source);
    }
    if let Some(pred) = where_sql {
        sql.push_str(" WHERE ");
        sql.push_str(pred);
    }
    sql
}

// ---------------------------------------------------------------------------
// MERGE INTO
// ---------------------------------------------------------------------------

const MERGE_TARGET_DEFAULT_ALIAS: &str = "__nr_t";
const MERGE_SOURCE_DEFAULT_ALIAS: &str = "__nr_s";
const MERGE_ACTION_MATCHED_UPDATE: i32 = 1;
const MERGE_ACTION_MATCHED_DELETE: i32 = 2;
const MERGE_ACTION_NOT_MATCHED_INSERT: i32 = 3;

pub(crate) fn execute_merge_statement(
    state: &Arc<StandaloneState>,
    stmt: &MergeStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<StatementResult, String> {
    let target = crate::engine::backend_resolver::resolve_existing_table_target(
        state,
        &stmt.table,
        current_catalog,
        current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Err(format!(
            "MERGE only supports iceberg backends, got `{}`",
            target.backend_name
        ));
    }

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
    crate::engine::mv::iceberg_guard::reject_if_iceberg_mv_properties(
        &target,
        table.metadata().properties(),
        crate::engine::mv::iceberg_guard::IcebergMvUserMutation::Merge,
    )?;

    // Validate writability up front (resolvable default-sort-order, no variant
    // in partition spec / sort order) before any branch write. The folded
    // not-matched INSERT branch builds its own write plan and bypasses
    // `execute_iceberg_insert_or_overwrite`, which is where this check used to
    // run for the INSERT path; running it here keeps MERGE failing fast instead
    // of deep in codegen. Mirrors the INSERT/UPDATE entry call form.
    let _write_mode = ensure_iceberg_write_supported(&table)?;

    let target_columns = iceberg_table_columns(&table)?;
    let partition_columns = iceberg_partition_source_columns(&table)?;

    // The match SELECT is built against the v3 row-lineage target so the
    // matched-side path can reuse the UPDATE executor. Validate the v3
    // requirement up front instead of letting the executor surface it.
    let table_write_mode = select_iceberg_update_mode(&table)?;

    if let Some(clause) = stmt.matched.as_ref()
        && let MergeMatchedAction::Update { assignments } = &clause.action
    {
        validate_update_assignments(assignments, &target_columns, &partition_columns)?;
    }
    let insert_columns_resolved = if let Some(clause) = stmt.not_matched.as_ref() {
        Some(resolve_merge_insert_columns(
            &clause.action,
            &target_columns,
        )?)
    } else {
        None
    };

    let has_matched_update = matches!(
        stmt.matched.as_ref().map(|clause| &clause.action),
        Some(MergeMatchedAction::Update { .. })
    );
    let has_matched_delete = matches!(
        stmt.matched.as_ref().map(|clause| &clause.action),
        Some(MergeMatchedAction::Delete)
    );
    let has_not_matched_insert = stmt.not_matched.is_some();

    // MERGE matched-side DML is `main`-only here. Pin the unified transaction's
    // base snapshot to the freshly-loaded table's current snapshot; each
    // matched-branch builder derives its sink snapshot from its own
    // `load_table`, and under the single-writer assumption all observe the same
    // current snapshot (the commit's base-snapshot check fails fast otherwise).
    let target_ref = "main";

    let use_mor_change_stream =
        table_write_mode == IcebergUpdateMode::MergeOnRead || has_matched_delete;
    if use_mor_change_stream {
        if !has_matched_update && !has_matched_delete && !has_not_matched_insert {
            return Ok(StatementResult::Ok);
        }

        let base_snapshot_id = table.metadata().current_snapshot().map(|s| s.snapshot_id());
        let metadata = table.metadata();
        let staging_dir = format!(
            "{}/data/_staging/{}",
            metadata.location(),
            uuid::Uuid::new_v4()
        );
        let collector = Arc::new(
            IcebergCommitCollector::new(
                CommitOpKind::RowDeltaDvFromFiles,
                table_ident.clone(),
                base_snapshot_id,
                metadata.last_sequence_number(),
                metadata.current_schema().clone(),
                metadata.default_partition_spec().clone(),
                staging_dir,
                crate::common::types::UniqueId { hi: 0, lo: 0 },
            )
            .with_table_metadata(metadata.clone()),
        );
        let write = build_merge_mor_change_stream_write_plan(
            state,
            &target,
            stmt,
            current_catalog,
            &target_columns,
            insert_columns_resolved.as_deref(),
            target_ref,
            metadata.last_sequence_number() + 1,
        )?;
        run_mor_merge_change_stream_transaction(
            state,
            &target,
            catalog,
            table,
            collector,
            entry,
            base_snapshot_id,
            target_ref,
            write,
        )?;
        return Ok(StatementResult::Ok);
    }

    let match_rows = materialize_merge_match(
        state,
        &target,
        stmt,
        current_catalog,
        &target_columns,
        insert_columns_resolved.as_deref(),
    )?;

    // Build the not-matched INSERT branch only when there are unmatched rows to
    // insert. Its data files are FRESH (net-new rows, no preserved `_row_id`).
    let insert_branch = if stmt.not_matched.is_some() {
        let insert_columns = insert_columns_resolved
            .as_ref()
            .expect("not_matched populated => insert columns resolved");
        let insert_batch = match_rows.unmatched_insert_batch(&target_columns, insert_columns)?;
        if insert_batch.num_rows() > 0 {
            let insert_query = build_merge_unmatched_insert_query(
                state,
                &target,
                stmt,
                current_catalog,
                &target_columns,
                insert_columns,
            )?;
            let resolved = {
                let registry = state.connectors.read().expect("connector registry read");
                let backend = registry.catalog_backend("iceberg")?;
                backend.load_table(&target.catalog, &target.namespace, &target.table)?
            };
            let plan = crate::engine::iceberg_writer::build_insert_write_plan(
                &target,
                &resolved,
                &[],
                &InsertSource::FromQuery(Box::new(insert_query)),
                &table,
                &entry,
            )?;
            Some(plan)
        } else {
            None
        }
    } else {
        None
    };

    // Build the matched branch by table write mode. Cardinality
    // (at-most-one-match) is enforced here in the orchestrator before any write.
    let matched_branch = if let Some(clause) = stmt.matched.as_ref() {
        let matched = matched_update_batch_from_record_batch(&match_rows.matched_batch()?)?;
        if matched.row_ids.is_empty() {
            MergeMatchedBranch::None
        } else {
            validate_unique_target_row_ids(&matched.row_ids)?;
            match &clause.action {
                MergeMatchedAction::Update { .. } => {
                    let base_snapshot_id =
                        table.metadata().current_snapshot().map(|s| s.snapshot_id());
                    let write = build_cow_update_distributed_write(
                        state,
                        &target,
                        &table,
                        &matched,
                        &target_columns,
                        &entry,
                        base_snapshot_id,
                    )?;
                    MergeMatchedBranch::CowUpdate(write)
                }
                MergeMatchedAction::Delete => {
                    return Err(
                        "internal error: MERGE matched DELETE should use MOR change-stream path"
                            .to_string(),
                    );
                }
            }
        }
    } else {
        MergeMatchedBranch::None
    };

    let has_insert = insert_branch.is_some();
    // Choose the single commit op for the folded snapshot:
    // - COW table + matched-UPDATE present → CowUpdate (INSERT data appended).
    // - any DV/data fold (matched-UPDATE-MOR, matched-DELETE, ± INSERT)
    //   → RowDeltaDvFromFiles (INSERT data routed to the fresh channel).
    // - INSERT only, no matched → FastAppend.
    let commit_op_kind = match &matched_branch {
        MergeMatchedBranch::CowUpdate(_) => CommitOpKind::CowUpdate,
        MergeMatchedBranch::None => CommitOpKind::FastAppend,
    };

    if !has_insert && matches!(matched_branch, MergeMatchedBranch::None) {
        // Nothing matched and nothing to insert: no-op, no snapshot.
        return Ok(StatementResult::Ok);
    }

    let base_snapshot_id = table.metadata().current_snapshot().map(|s| s.snapshot_id());
    let metadata = table.metadata();
    let staging_dir = format!(
        "{}/data/_staging/{}",
        metadata.location(),
        uuid::Uuid::new_v4()
    );
    let collector = Arc::new(
        IcebergCommitCollector::new(
            commit_op_kind,
            table_ident.clone(),
            base_snapshot_id,
            metadata.last_sequence_number(),
            metadata.current_schema().clone(),
            metadata.default_partition_spec().clone(),
            staging_dir,
            crate::common::types::UniqueId { hi: 0, lo: 0 },
        )
        .with_table_metadata(metadata.clone()),
    );

    let abort_cleanup =
        crate::engine::iceberg_writer::build_abort_cleanup_for_catalog_entry(&entry)?;
    let commit_executor = IcebergWriteCommitExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        catalog,
        table,
        collector,
        fs: abort_cleanup.fs,
        cleanup_path_mapper: abort_cleanup.path_mapper,
        cow_update_rewrite: None,
        target_ref: target_ref.to_string(),
        snapshot_properties: BTreeMap::new(),
    };
    let spec = IcebergWriteTransactionSpec {
        target: IcebergOperationTarget {
            catalog: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            ref_name: None,
        },
        operation_kind: IcebergOperationKind::RowDelta,
        attempt_id: format!(
            "{}.{}.{}:merge-distributed:{}",
            target.catalog,
            target.namespace,
            target.table,
            uuid::Uuid::new_v4()
        ),
        commit: IcebergWriteCommitPolicy {
            commit_op_kind,
            base_snapshot_id,
            base_snapshot_map: BTreeMap::new(),
            target_ref: target_ref.to_string(),
            snapshot_properties: BTreeMap::new(),
        },
        validation: IcebergWriteValidationPolicy {
            require_v3_for_branch: false,
        },
        source: IcebergWriteSource::CoordinatedPlan,
    };
    let executor = DistributedMergeExecutor {
        state: Arc::clone(state),
        target: target.clone(),
        commit_op_kind,
        branches: Mutex::new(Some(MergeBranchSet {
            insert: insert_branch,
            matched: matched_branch,
        })),
        commit_executor,
        cow_update_rewrite: Mutex::new(None),
    };
    let runner = IcebergWriteTransactionRunner::new(Arc::clone(state), &executor);
    let _outcome = runner.run(spec)?;
    Ok(StatementResult::Ok)
}

/// Resolved target column ordering for `WHEN NOT MATCHED INSERT`. Each entry
/// maps a target column name to either an explicit value expression (sourced
/// from `INSERT (cols) VALUES (exprs)`) or a `NULL` default when the user did
/// not list the column. Validates that every named column exists, that the
/// list has no duplicates, and that no reserved row-lineage column is named.
struct MergeInsertColumns {
    columns: Vec<MergeInsertColumn>,
}

struct MergeInsertColumn {
    name: String,
    /// `Some(idx)` when the user supplied a value for this target column at
    /// position `idx` in the `VALUES` tuple. `None` means "no value
    /// supplied"; we project a NULL of the column's type instead.
    value_index: Option<usize>,
}

impl std::ops::Deref for MergeInsertColumns {
    type Target = [MergeInsertColumn];
    fn deref(&self) -> &[MergeInsertColumn] {
        &self.columns
    }
}

fn resolve_merge_insert_columns(
    action: &MergeNotMatchedAction,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
) -> Result<MergeInsertColumns, String> {
    let target_names_lower: Vec<String> = target_columns
        .iter()
        .map(|c| c.name.to_ascii_lowercase())
        .collect();

    // Empty `INSERT VALUES (...)` (no column list) means "values match target
    // schema in declaration order". Iceberg row-lineage columns (`_row_id`
    // etc.) are reserved/owned and never appear in the user-visible target
    // schema returned from `iceberg_table_columns`, so we don't have to
    // filter them here.
    if action.columns.is_empty() {
        if action.values.len() != target_columns.len() {
            return Err(format!(
                "MERGE WHEN NOT MATCHED INSERT VALUES count {} does not match target column count {}",
                action.values.len(),
                target_columns.len()
            ));
        }
        let columns = target_columns
            .iter()
            .enumerate()
            .map(|(idx, col)| MergeInsertColumn {
                name: col.name.clone(),
                value_index: Some(idx),
            })
            .collect();
        return Ok(MergeInsertColumns { columns });
    }

    let mut seen: HashSet<String> = HashSet::new();
    let mut by_target: HashMap<String, usize> = HashMap::new();
    for (idx, raw_name) in action.columns.iter().enumerate() {
        let lower = raw_name.to_ascii_lowercase();
        if matches!(
            lower.as_str(),
            "_row_id" | "_last_updated_sequence_number" | "_file" | "_pos"
        ) {
            return Err(format!(
                "MERGE INSERT cannot assign reserved Iceberg metadata column `{raw_name}`"
            ));
        }
        if !target_names_lower.contains(&lower) {
            return Err(format!(
                "MERGE INSERT references unknown target column `{raw_name}`"
            ));
        }
        if !seen.insert(lower.clone()) {
            return Err(format!(
                "MERGE INSERT lists target column `{raw_name}` more than once"
            ));
        }
        by_target.insert(lower, idx);
    }

    let columns = target_columns
        .iter()
        .map(|col| MergeInsertColumn {
            name: col.name.clone(),
            value_index: by_target.get(&col.name.to_ascii_lowercase()).copied(),
        })
        .collect();
    Ok(MergeInsertColumns { columns })
}

struct MergeMatchRows {
    /// The full RecordBatch from the MERGE match SELECT, with rows for both
    /// matched and unmatched cases. Filters for each side are derived from
    /// `__nr_match_kind` / `__nr_matched_apply` / `__nr_unmatched_apply`.
    full: RecordBatch,
}

impl MergeMatchRows {
    fn empty() -> Self {
        Self {
            full: RecordBatch::new_empty(Arc::new(Schema::empty())),
        }
    }

    fn matched_batch(&self) -> Result<RecordBatch, String> {
        if self.full.num_rows() == 0 {
            return Ok(self.full.clone());
        }
        let filter = self.row_filter("matched", "__nr_matched_apply")?;
        filter_record_batch(&self.full, &filter)
            .map_err(|e| format!("filter MERGE matched rows failed: {e}"))
    }

    fn unmatched_insert_batch(
        &self,
        target_columns: &[novarocks_catalog::schema::ColumnDef],
        insert_columns: &MergeInsertColumns,
    ) -> Result<RecordBatch, String> {
        let target_arrow_schema = arrow::datatypes::Schema::new(
            target_columns
                .iter()
                .map(|c| {
                    arrow::datatypes::Field::new(c.name.clone(), c.data_type.clone(), c.nullable)
                })
                .collect::<Vec<_>>(),
        );
        let target_arrow_schema = Arc::new(target_arrow_schema);
        if self.full.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(target_arrow_schema));
        }
        let filter = self.row_filter("unmatched", "__nr_unmatched_apply")?;
        let filtered = filter_record_batch(&self.full, &filter)
            .map_err(|e| format!("filter MERGE unmatched rows failed: {e}"))?;
        if filtered.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(target_arrow_schema));
        }

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_columns.len());
        for (target_col, insert_entry) in target_columns.iter().zip(insert_columns.iter()) {
            debug_assert_eq!(target_col.name, insert_entry.name);
            let column = match insert_entry.value_index {
                Some(_) => {
                    let projected_name = format!("__nr_ins_{}", target_col.name);
                    let idx = filtered.schema().index_of(&projected_name).map_err(|_| {
                        format!("MERGE INSERT projection missing column `{projected_name}`")
                    })?;
                    cast(filtered.column(idx), &target_col.data_type).map_err(|e| {
                        format!(
                            "cast MERGE INSERT column `{}` to {:?} failed: {e}",
                            target_col.name, target_col.data_type
                        )
                    })?
                }
                None => arrow::array::new_null_array(&target_col.data_type, filtered.num_rows()),
            };
            columns.push(column);
        }
        RecordBatch::try_new(target_arrow_schema, columns)
            .map_err(|e| format!("build MERGE INSERT batch failed: {e}"))
    }

    fn row_filter(&self, kind: &str, apply_col: &str) -> Result<BooleanArray, String> {
        let kind_col = cast(
            required_column(&self.full, "__nr_match_kind")?,
            &DataType::Utf8,
        )
        .map_err(|e| format!("cast __nr_match_kind to Utf8 failed: {e}"))?;
        let kind_arr = kind_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| "__nr_match_kind was not Utf8 after cast".to_string())?;
        let apply_col = cast(required_column(&self.full, apply_col)?, &DataType::Boolean)
            .map_err(|e| format!("cast {apply_col} to Boolean failed: {e}"))?;
        let apply_arr = apply_col
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| "MERGE apply column was not Boolean after cast".to_string())?;

        let mut bits = Vec::with_capacity(self.full.num_rows());
        for row in 0..self.full.num_rows() {
            if kind_arr.is_null(row) {
                bits.push(false);
                continue;
            }
            let matches_kind = kind_arr.value(row) == kind;
            let applies = !apply_arr.is_null(row) && apply_arr.value(row);
            bits.push(matches_kind && applies);
        }
        Ok(BooleanArray::from(bits))
    }
}

fn materialize_merge_match(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &MergeStmt,
    current_catalog: Option<&str>,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
    insert_columns: Option<&[MergeInsertColumn]>,
) -> Result<MergeMatchRows, String> {
    let target_alias = stmt
        .target_alias
        .clone()
        .unwrap_or_else(|| MERGE_TARGET_DEFAULT_ALIAS.to_string());
    let target_sql = format!("{} AS {}", target.table, target_alias);

    let source_table_sql =
        mutation_source_relation_to_sql(state, &stmt.source, current_catalog, target)?;
    // `mutation_source_to_sql` preserves the user-provided alias when present.
    // When the source carries no alias, inject `__nr_s` so the projection /
    // ON predicate can reference source columns deterministically.
    let source_sql = match &stmt.source {
        crate::sql::parser::ast::MutationSource::Table { alias, .. }
        | crate::sql::parser::ast::MutationSource::Query { alias, .. } => {
            if alias.is_some() {
                source_table_sql
            } else {
                format!("{source_table_sql} AS {MERGE_SOURCE_DEFAULT_ALIAS}")
            }
        }
    };

    let on_sql = stmt.on.to_string();
    let matched_predicate_sql = stmt
        .matched
        .as_ref()
        .and_then(|c| c.predicate.as_ref())
        .map(|expr| expr.to_string());
    let not_matched_predicate_sql = stmt
        .not_matched
        .as_ref()
        .and_then(|c| c.predicate.as_ref())
        .map(|expr| expr.to_string());

    let matched_assignments_sql = match stmt.matched.as_ref().map(|c| &c.action) {
        Some(MergeMatchedAction::Update { assignments }) => assignments
            .iter()
            .map(|a| {
                let target_column = target_columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(&a.column))
                    .ok_or_else(|| {
                        format!(
                            "MERGE UPDATE assignment references unknown target column `{}`",
                            a.column
                        )
                    })?;
                Ok((
                    target_column.name.clone(),
                    crate::engine::iceberg_writer::target_cast_expr_sql(
                        &format!("({})", a.value),
                        target_column,
                    )?,
                ))
            })
            .collect::<Result<Vec<_>, String>>()?,
        _ => Vec::new(),
    };
    let matched_assignments_sql_borrow: Vec<(&str, &str)> = matched_assignments_sql
        .iter()
        .map(|(c, e)| (c.as_str(), e.as_str()))
        .collect();

    let insert_values_sql: Vec<(String, String)> =
        match (insert_columns, stmt.not_matched.as_ref().map(|c| &c.action)) {
            (Some(cols), Some(action)) => cols
                .iter()
                .filter_map(|col| {
                    col.value_index.map(|idx| {
                        let target_column = target_columns
                            .iter()
                            .find(|target_column| {
                                target_column.name.eq_ignore_ascii_case(&col.name)
                            })
                            .expect("resolved MERGE INSERT column exists in target columns");
                        Ok((
                            col.name.clone(),
                            crate::engine::iceberg_writer::target_cast_expr_sql(
                                &format!("({})", action.values[idx]),
                                target_column,
                            )?,
                        ))
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            _ => Vec::new(),
        };
    let insert_values_sql_borrow: Vec<(&str, &str)> = insert_values_sql
        .iter()
        .map(|(c, e)| (c.as_str(), e.as_str()))
        .collect();

    let sql = build_merge_match_query_sql(
        &target_sql,
        &target_alias,
        &source_sql,
        &on_sql,
        matched_predicate_sql.as_deref(),
        not_matched_predicate_sql.as_deref(),
        target_columns,
        &matched_assignments_sql_borrow,
        &insert_values_sql_borrow,
        stmt.matched.as_ref().map(|clause| match clause.action {
            MergeMatchedAction::Update { .. } => MERGE_ACTION_MATCHED_UPDATE,
            MergeMatchedAction::Delete => MERGE_ACTION_MATCHED_DELETE,
        }),
        stmt.not_matched.is_some(),
    );

    let result = execute_merge_match_query(state, Some(&target.catalog), &sql, &target.namespace)?;
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
fn build_merge_mor_change_stream_write_plan(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &MergeStmt,
    current_catalog: Option<&str>,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
    insert_columns: Option<&[MergeInsertColumn]>,
    target_ref: &str,
    new_sequence_number: i64,
) -> Result<crate::engine::dml_change_stream::DmlChangeStreamWritePlan, String> {
    let target_alias = stmt
        .target_alias
        .clone()
        .unwrap_or_else(|| MERGE_TARGET_DEFAULT_ALIAS.to_string());
    let target_sql = update_change_stream_target_sql(target, &target_alias, target_ref);
    let source_table_sql =
        mutation_source_relation_to_sql(state, &stmt.source, current_catalog, target)?;
    let source_sql = match &stmt.source {
        crate::sql::parser::ast::MutationSource::Table { alias, .. }
        | crate::sql::parser::ast::MutationSource::Query { alias, .. } => {
            if alias.is_some() {
                source_table_sql
            } else {
                format!("{source_table_sql} AS {MERGE_SOURCE_DEFAULT_ALIAS}")
            }
        }
    };

    let matched_assignments_sql = match stmt.matched.as_ref().map(|c| &c.action) {
        Some(MergeMatchedAction::Update { assignments }) => assignments
            .iter()
            .map(|a| {
                let target_column = target_columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(&a.column))
                    .ok_or_else(|| {
                        format!(
                            "MERGE UPDATE assignment references unknown target column `{}`",
                            a.column
                        )
                    })?;
                Ok((
                    target_column.name.clone(),
                    crate::engine::iceberg_writer::target_cast_expr_sql(
                        &format!("({})", a.value),
                        target_column,
                    )?,
                ))
            })
            .collect::<Result<Vec<_>, String>>()?,
        _ => Vec::new(),
    };
    let matched_assignments_sql_borrow = matched_assignments_sql
        .iter()
        .map(|(c, e)| (c.as_str(), e.as_str()))
        .collect::<Vec<_>>();

    let insert_values_sql: Vec<(String, String)> =
        match (insert_columns, stmt.not_matched.as_ref().map(|c| &c.action)) {
            (Some(cols), Some(action)) => cols
                .iter()
                .filter_map(|col| {
                    col.value_index.map(|idx| {
                        let target_column = target_columns
                            .iter()
                            .find(|target_column| {
                                target_column.name.eq_ignore_ascii_case(&col.name)
                            })
                            .expect("resolved MERGE INSERT column exists in target columns");
                        Ok((
                            col.name.clone(),
                            crate::engine::iceberg_writer::target_cast_expr_sql(
                                &format!("({})", action.values[idx]),
                                target_column,
                            )?,
                        ))
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            _ => Vec::new(),
        };
    let insert_values_sql_borrow = insert_values_sql
        .iter()
        .map(|(c, e)| (c.as_str(), e.as_str()))
        .collect::<Vec<_>>();

    let matched_action = stmt.matched.as_ref().map(|clause| match clause.action {
        MergeMatchedAction::Update { .. } => MERGE_ACTION_MATCHED_UPDATE,
        MergeMatchedAction::Delete => MERGE_ACTION_MATCHED_DELETE,
    });
    let has_matched_update = matched_action == Some(MERGE_ACTION_MATCHED_UPDATE);
    let has_matched_delete = matched_action == Some(MERGE_ACTION_MATCHED_DELETE);
    let has_not_matched_insert = stmt.not_matched.is_some();
    let matched_predicate_sql = stmt
        .matched
        .as_ref()
        .and_then(|c| c.predicate.as_ref())
        .map(|expr| expr.to_string());
    let not_matched_predicate_sql = stmt
        .not_matched
        .as_ref()
        .and_then(|c| c.predicate.as_ref())
        .map(|expr| expr.to_string());

    let match_sql = build_merge_match_query_sql(
        &target_sql,
        &target_alias,
        &source_sql,
        &stmt.on.to_string(),
        matched_predicate_sql.as_deref(),
        not_matched_predicate_sql.as_deref(),
        target_columns,
        &matched_assignments_sql_borrow,
        &insert_values_sql_borrow,
        matched_action,
        has_not_matched_insert,
    );
    let mut query = parse_generated_query(&match_sql, "MOR MERGE change-stream producer")?;
    if crate::engine::query_prep::has_time_travel_refs(&query) {
        crate::engine::query_prep::rewrite_time_travel_refs(
            state,
            Some(&target.catalog),
            &target.namespace,
            &mut query,
        )?;
    }

    let catalog_service_snapshot = crate::engine::catalog_service_snapshot(state);
    let connectors_snapshot = state
        .connectors
        .read()
        .expect("standalone connector registry read lock")
        .clone();
    let analyzer_provider = crate::engine::build_catalog_service_provider(
        Some(&target.catalog),
        &catalog_service_snapshot,
        &connectors_snapshot,
        crate::sql::catalog::TableLookupMode::SchemaOnly,
    );
    let planned = crate::engine::plan_query_for_iceberg_change_stream_refresh(
        &query,
        &analyzer_provider,
        &connectors_snapshot,
        &target.namespace,
        None,
        None,
        false,
    )?;
    let producer = build_merge_mor_change_event_expand_plan(
        planned.optimized_tree,
        target_columns,
        new_sequence_number,
        has_matched_update,
        has_matched_delete,
        has_not_matched_insert,
    )?;
    let mut plan = crate::engine::dml_change_stream::build_dml_change_stream_write_plan(
        state,
        target,
        producer,
        crate::engine::dml_change_stream::DmlChangeStreamBranchSet::Merge {
            matched_update: has_matched_update,
            matched_delete: has_matched_delete,
            not_matched_insert: has_not_matched_insert,
        },
        target_ref,
    )?;
    if has_matched_update || has_matched_delete {
        plan.pre_expand_keyed_assert =
            Some(crate::engine::dml_change_stream::DmlPreExpandKeyedAssert {
                // Matched rows use the real target `_row_id`; unmatched rows use
                // a generated negative row number so fresh-only rows do not
                // collide under the same NULL key before expansion.
                key_column_name: "__nr_merge_assert_key".to_string(),
                key_label: crate::exec::row_position::ICEBERG_ROW_ID_COL.to_string(),
                message_prefix: "MOR MERGE matched target row".to_string(),
            });
    }
    Ok(plan)
}

fn execute_merge_match_query(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    sql: &str,
    current_database: &str,
) -> Result<MergeMatchRows, String> {
    let statement = crate::sql::parser::parse_sql_raw(sql)?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("internal MERGE match query was not a SELECT".to_string());
    };
    let result = crate::engine::execute_query_with_catalog_service(
        state,
        current_catalog,
        current_database,
        &query,
        None,
    )?;
    let Some(first_chunk) = result.chunks.first() else {
        return Ok(MergeMatchRows::empty());
    };
    let schema = first_chunk.batch.schema();
    let batches = result
        .chunks
        .iter()
        .map(|c| c.batch.clone())
        .collect::<Vec<_>>();
    let full = concat_batches(&schema, batches.iter())
        .map_err(|e| format!("concatenate MERGE match batches failed: {e}"))?;
    Ok(MergeMatchRows { full })
}

fn build_merge_match_query_sql(
    target_sql: &str,
    target_alias: &str,
    source_sql: &str,
    on_sql: &str,
    matched_predicate_sql: Option<&str>,
    not_matched_predicate_sql: Option<&str>,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
    matched_assignments_sql: &[(&str, &str)],
    insert_values_sql: &[(&str, &str)],
    matched_action: Option<i32>,
    has_not_matched_insert: bool,
) -> String {
    let quote_ident = |ident: &str| format!("`{}`", ident.replace('`', "``"));
    let qualify = |column: &str| {
        if target_alias.is_empty() {
            quote_ident(column)
        } else {
            format!("{target_alias}.{}", quote_ident(column))
        }
    };
    let row_id = qualify("_row_id");
    let nullable_target_column = |column: &str| {
        let value = qualify(column);
        format!("CASE WHEN {row_id} IS NOT NULL THEN {value} ELSE NULL END")
    };
    let matched_apply_expr = format!(
        "(CASE WHEN ({}) THEN TRUE ELSE FALSE END)",
        matched_predicate_sql.unwrap_or("TRUE")
    );
    let unmatched_apply_expr = format!(
        "(CASE WHEN ({}) THEN TRUE ELSE FALSE END)",
        not_matched_predicate_sql.unwrap_or("TRUE")
    );
    let mut action_cases = Vec::new();
    if let Some(action) = matched_action {
        action_cases.push(format!(
            "WHEN {row_id} IS NOT NULL AND ({}) THEN {action}",
            matched_predicate_sql.unwrap_or("TRUE")
        ));
    }
    if has_not_matched_insert {
        action_cases.push(format!(
            "WHEN {row_id} IS NULL AND ({}) THEN {MERGE_ACTION_NOT_MATCHED_INSERT}",
            not_matched_predicate_sql.unwrap_or("TRUE")
        ));
    }
    let action_expr = if action_cases.is_empty() {
        "0".to_string()
    } else {
        format!("CASE {} ELSE 0 END", action_cases.join(" "))
    };
    let target_select_items = target_columns
        .iter()
        .map(|column| {
            format!(
                "{} AS {}",
                nullable_target_column(&column.name),
                quote_ident(&column.name)
            )
        })
        .collect::<Vec<_>>();

    let mut select_items = vec![
        format!("{} AS __nr_file", nullable_target_column("_file")),
        format!("{} AS __nr_pos", nullable_target_column("_pos")),
        format!("{} AS __nr_row_id", nullable_target_column("_row_id")),
        format!(
            "{} AS __nr_last_updated_sequence_number",
            nullable_target_column("_last_updated_sequence_number")
        ),
        format!(
            "CASE WHEN {row_id} IS NOT NULL THEN {row_id} ELSE -ROW_NUMBER() OVER () END AS __nr_merge_assert_key"
        ),
        format!("({action_expr}) AS __nr_merge_action"),
        format!(
            "(CASE WHEN {} IS NOT NULL THEN 'matched' ELSE 'unmatched' END) AS __nr_match_kind",
            row_id
        ),
    ];
    select_items.extend(target_select_items);
    select_items.push(format!("{matched_apply_expr} AS __nr_matched_apply"));
    select_items.push(format!("{unmatched_apply_expr} AS __nr_unmatched_apply"));
    for (column, expr) in matched_assignments_sql {
        select_items.push(format!("({expr}) AS __nr_new_{column}"));
    }
    for (column, expr) in insert_values_sql {
        select_items.push(format!("({expr}) AS __nr_ins_{column}"));
    }

    format!(
        "SELECT {} FROM {} LEFT JOIN {} ON {}",
        select_items.join(", "),
        source_sql,
        target_sql,
        on_sql
    )
}

fn build_merge_unmatched_insert_query(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    stmt: &MergeStmt,
    current_catalog: Option<&str>,
    target_columns: &[novarocks_catalog::schema::ColumnDef],
    insert_columns: &MergeInsertColumns,
) -> Result<sqlparser::ast::Query, String> {
    let target_alias = stmt
        .target_alias
        .as_deref()
        .unwrap_or(MERGE_TARGET_DEFAULT_ALIAS);
    let source_table_sql =
        mutation_source_relation_to_sql(state, &stmt.source, current_catalog, target)?;
    let source_sql = match &stmt.source {
        crate::sql::parser::ast::MutationSource::Table { alias, .. }
        | crate::sql::parser::ast::MutationSource::Query { alias, .. } => {
            if alias.is_some() {
                source_table_sql
            } else {
                format!("{source_table_sql} AS {MERGE_SOURCE_DEFAULT_ALIAS}")
            }
        }
    };
    let not_matched = stmt
        .not_matched
        .as_ref()
        .ok_or_else(|| "MERGE unmatched INSERT write requires a not-matched clause".to_string())?;
    let select_items = target_columns
        .iter()
        .zip(insert_columns.iter())
        .map(|(target_column, insert_column)| {
            if target_column.name != insert_column.name {
                return Err(format!(
                    "MERGE INSERT column order mismatch: target `{}`, insert `{}`",
                    target_column.name, insert_column.name
                ));
            }
            let raw_expr = match insert_column.value_index {
                Some(idx) => format!("({})", not_matched.action.values[idx]),
                None => "NULL".to_string(),
            };
            let expr =
                crate::engine::iceberg_writer::target_cast_expr_sql(&raw_expr, target_column)?;
            Ok(format!("{expr} AS {}", sql_identifier(&target_column.name)))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let target_sql = format!(
        "{} AS {}",
        qualify_iceberg_table(target),
        sql_identifier(target_alias)
    );
    let mut predicates = vec![format!(
        "{} IS NULL",
        qualify_column(target_alias, crate::exec::row_position::ICEBERG_ROW_ID_COL)
    )];
    if let Some(predicate) = not_matched.predicate.as_ref() {
        predicates.push(format!("({predicate})"));
    }
    let sql = format!(
        "SELECT {} FROM {} LEFT JOIN {} ON {} WHERE {}",
        select_items.join(", "),
        source_sql,
        target_sql,
        stmt.on,
        predicates.join(" AND ")
    );
    parse_generated_query(&sql, "MERGE unmatched INSERT sink")
}

/// The matched-side write plan of a folded MERGE, by table write mode. `None`
/// when the statement has no matched clause (or the matched batch is empty).
enum MergeMatchedBranch {
    None,
    CowUpdate(CowUpdateDistributedWrite),
}

/// All active write branches of one MERGE statement, fed to
/// [`DistributedMergeExecutor`] so they share one collector and one commit.
struct MergeBranchSet {
    /// Not-matched INSERT plan (`build_insert_write_plan` output). Its files are
    /// FRESH (net-new rows, no preserved `_row_id`).
    insert: Option<(sqlparser::ast::Query, IcebergWriteSinkSpec)>,
    matched: MergeMatchedBranch,
}

/// Single multi-branch MERGE write executor: runs every active branch into one
/// shared `IcebergCommitCollector` and commits exactly once, so a MERGE lands
/// as ONE Iceberg snapshot. Routes fresh (INSERT) vs reuse (matched UPDATE
/// rewrite) row-lineage channels by branch shape — it KNOWS which branch
/// produced which files, so the commit-layer never has to content-sniff.
struct DistributedMergeExecutor {
    state: Arc<StandaloneState>,
    target: crate::engine::backend_resolver::TargetBackend,
    commit_op_kind: CommitOpKind,
    branches: Mutex<Option<MergeBranchSet>>,
    commit_executor: IcebergWriteCommitExecutor,
    /// Populated by `run_coordinated_write` for the COW fold so `commit` can
    /// carry the rewrite set (touched files + appended INSERT data) on the
    /// commit context. `None` for MOR / DELETE / INSERT-only folds.
    cow_update_rewrite: Mutex<Option<CowUpdateRewriteSet>>,
}

impl DistributedMergeExecutor {
    /// Run the not-matched INSERT branch on the BE and return its converted
    /// net-new data files. These rows are genuinely new (no preserved
    /// `_row_id`) so the caller routes them into a FRESH row-lineage channel.
    fn run_insert_branch(
        &self,
        query: &sqlparser::ast::Query,
        sink_spec: &IcebergWriteSinkSpec,
    ) -> Result<
        (
            CoordinatedQueryResult,
            Vec<crate::connector::iceberg::commit::WrittenFile>,
        ),
        String,
    > {
        let result = crate::engine::execute_query_as_iceberg_write(
            &self.state,
            Some(&self.target.catalog),
            &self.target.namespace,
            query,
            sink_spec.clone(),
            None,
            None,
        )?;
        if let Some(abort) = &result.write_abort {
            return Err(format!(
                "MERGE not-matched INSERT branch aborted: {}",
                abort.reason
            ));
        }
        // The orchestrator only builds the INSERT branch when the unmatched
        // batch is non-empty, so a file-less, non-aborted result is a real bug:
        // committing without the INSERT rows would silently drop them.
        let commit = result
            .write_commit
            .as_ref()
            .filter(|c| write_commit_has_files(c));
        let Some(commit) = commit else {
            return Err("MERGE not-matched INSERT branch produced no data files".to_string());
        };
        let mut files = Vec::new();
        for writer in &commit.writers {
            let reports = crate::runtime::sink_commit::iceberg_commit_infos_to_writer_reports(
                writer.iceberg_commits.clone(),
                self.commit_executor.table.metadata(),
            )?;
            for report in reports {
                files.push(
                    self.commit_executor
                        .collector
                        .convert_writer_report(report)?,
                );
            }
        }
        Ok((result, files))
    }
}

impl IcebergWriteTransactionExecutor for DistributedMergeExecutor {
    fn run_coordinated_write(
        &self,
        _spec: &IcebergWriteTransactionSpec,
    ) -> Result<CoordinatedQueryResult, String> {
        // Matched-branch staged files are recorded in the collector's AbortLog at commit() time
        // (deferred), consistent with the per-mode distributed executors. A mid-fold error here
        // returns Err, the runner skips commit(), and nothing is committed — the fold is atomic;
        // staged files from earlier branches are left for orphan cleanup (no partial snapshot).
        let branches = self
            .branches
            .lock()
            .expect("MERGE branch set lock poisoned")
            .take()
            .ok_or_else(|| "MERGE branch set was already consumed".to_string())?;

        // Matched-branch writer results that flow through the shared
        // `WriteCommitInput` (reuse channel). INSERT files are routed
        // separately per commit-op kind.
        let mut commit_parts: Vec<CoordinatedQueryResult> = Vec::new();

        match branches.matched {
            MergeMatchedBranch::None => {}
            MergeMatchedBranch::CowUpdate(write) => {
                // The per-file rewrite outputs land in the shared collector's
                // flat `written` channel via the returned `WriteCommitInput`.
                // The INSERT files (if any) are folded in below before the
                // rewrite set is finalized, so `CowUpdateCommit`'s written-set
                // reconciliation (every written file is a rewrite output or a
                // declared appended file) holds.
                let insert_files = match branches.insert.as_ref() {
                    Some((query, sink_spec)) => {
                        let (insert_result, files) = self.run_insert_branch(query, sink_spec)?;
                        commit_parts.push(insert_result);
                        files
                    }
                    None => Vec::new(),
                };
                let rewrite = run_cow_update_file_rewrites(
                    &self.state,
                    &self.target,
                    write,
                    self.commit_executor.table.metadata(),
                    &self.commit_executor.collector,
                    insert_files,
                )?;
                commit_parts.push(CoordinatedQueryResult {
                    query_result: QueryResult::empty(),
                    write_commit: Some(rewrite.write_commit),
                    write_abort: None,
                    fragment_profiles: Vec::new(),
                    runtime_filter_dormancy_proof: None,
                });
                *self
                    .cow_update_rewrite
                    .lock()
                    .expect("MERGE COW rewrite lock poisoned") = Some(rewrite.rewrite_set);
                // COW handled the INSERT branch inline; return now.
                return merge_all_write_commits(commit_parts);
            }
        }

        // INSERT branch for the non-COW folds (MOR / DELETE / INSERT-only).
        if let Some((query, sink_spec)) = branches.insert.as_ref() {
            let (insert_result, files) = self.run_insert_branch(query, sink_spec)?;
            match self.commit_op_kind {
                CommitOpKind::RowDeltaDvFromFiles => {
                    // Fresh INSERT data → dedicated appended channel so the
                    // commit allocates fresh `_row_id`s. Its writers must NOT
                    // also flow through the reuse `written` channel.
                    self.commit_executor.collector.inject_appended_files(files);
                }
                CommitOpKind::FastAppend => {
                    // INSERT-only MERGE: standard append, files flow through the
                    // reuse channel as a normal FastAppend.
                    let _ = files;
                    commit_parts.push(insert_result);
                }
                other => {
                    return Err(format!(
                        "MERGE not-matched INSERT fold does not support commit op {other:?}"
                    ));
                }
            }
        }

        merge_all_write_commits(commit_parts)
    }

    fn commit(
        &self,
        _spec: &IcebergWriteTransactionSpec,
        write_commit: &WriteCommitInput,
    ) -> Result<CommitOutcome, CommitServiceError> {
        let commit_executor = IcebergWriteCommitExecutor {
            state: Arc::clone(&self.commit_executor.state),
            target: self.commit_executor.target.clone(),
            catalog: Arc::clone(&self.commit_executor.catalog),
            table: self.commit_executor.table.clone(),
            collector: Arc::clone(&self.commit_executor.collector),
            fs: self.commit_executor.fs.clone(),
            cleanup_path_mapper: self.commit_executor.cleanup_path_mapper.clone(),
            cow_update_rewrite: self
                .cow_update_rewrite
                .lock()
                .expect("MERGE COW rewrite lock poisoned")
                .clone(),
            target_ref: self.commit_executor.target_ref.clone(),
            snapshot_properties: self.commit_executor.snapshot_properties.clone(),
        };
        commit_executor.commit_write_input(write_commit)
    }

    fn finalize(&self, _spec: &IcebergWriteTransactionSpec) -> Result<(), String> {
        self.commit_executor.finalize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use novarocks_catalog::schema::ColumnDef;

    fn col(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: true,
            write_default: None,
            logical_type: None,
        }
    }

    fn non_null_col(name: &str) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type: DataType::Int64,
            nullable: false,
            write_default: None,
            logical_type: None,
        }
    }

    fn iceberg_target() -> crate::engine::backend_resolver::TargetBackend {
        crate::engine::backend_resolver::TargetBackend {
            backend_name: "iceberg",
            catalog: "ice".to_string(),
            namespace: "db1".to_string(),
            table: "t".to_string(),
        }
    }

    fn optimizer_output_column(
        name: &str,
        column_id: u32,
        data_type: DataType,
        nullable: bool,
        is_internal: bool,
    ) -> crate::sql::analysis::OutputColumn {
        crate::sql::analysis::OutputColumn {
            column_id: crate::sql::column_id::ColumnId::new_for_test(column_id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal,
        }
    }

    fn update_mor_expand_child_plan_for_test() -> crate::sql::optimizer::OptimizedOperatorNode {
        use crate::sql::optimizer::operator::{Operator, ValuesOp};
        use crate::sql::optimizer::optimized_tree::{
            OptimizedOperatorNode, OptimizerExplainStats, PlanExecutionProps,
        };
        use crate::sql::optimizer::statistics::Statistics;

        let output_columns = vec![
            optimizer_output_column("__nr_file", 1, DataType::Utf8, false, true),
            optimizer_output_column("__nr_pos", 2, DataType::Int64, false, true),
            optimizer_output_column("__nr_row_id", 3, DataType::Int64, false, true),
            optimizer_output_column("id", 4, DataType::Int64, false, false),
            optimizer_output_column("qty", 5, DataType::Int64, true, false),
            optimizer_output_column("__nr_new_qty", 6, DataType::Int64, true, true),
        ];
        let mut node = OptimizedOperatorNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: Vec::new(),
                columns: output_columns.clone(),
            }),
            children: Vec::new(),
            stats: Statistics {
                output_row_count: 3.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            explain_stats: OptimizerExplainStats::default(),
            output_columns,
            execution_props: PlanExecutionProps::default(),
        };
        crate::sql::optimizer::optimized_tree::attach_scalar_arena(
            &mut node,
            Arc::new(crate::sql::optimizer::scalar::ScalarArena::new()),
        );
        node
    }

    fn merge_mor_expand_child_plan_for_test(
        include_insert_qty: bool,
    ) -> crate::sql::optimizer::OptimizedOperatorNode {
        use crate::sql::optimizer::operator::{Operator, ValuesOp};
        use crate::sql::optimizer::optimized_tree::{
            OptimizedOperatorNode, OptimizerExplainStats, PlanExecutionProps,
        };
        use crate::sql::optimizer::statistics::Statistics;

        let mut output_columns = vec![
            optimizer_output_column("__nr_file", 1, DataType::Utf8, true, true),
            optimizer_output_column("__nr_pos", 2, DataType::Int64, true, true),
            optimizer_output_column("__nr_row_id", 3, DataType::Int64, true, true),
            optimizer_output_column("__nr_merge_assert_key", 4, DataType::Int64, false, true),
            optimizer_output_column("__nr_merge_action", 5, DataType::Int64, false, true),
            optimizer_output_column("id", 6, DataType::Int64, true, false),
            optimizer_output_column("qty", 7, DataType::Int64, true, false),
            optimizer_output_column("__nr_new_qty", 8, DataType::Int64, true, true),
            optimizer_output_column("__nr_ins_id", 9, DataType::Int64, true, true),
        ];
        if include_insert_qty {
            output_columns.push(optimizer_output_column(
                "__nr_ins_qty",
                10,
                DataType::Int64,
                true,
                true,
            ));
        }

        let mut node = OptimizedOperatorNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: Vec::new(),
                columns: output_columns.clone(),
            }),
            children: Vec::new(),
            stats: Statistics {
                output_row_count: 5.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            explain_stats: OptimizerExplainStats::default(),
            output_columns,
            execution_props: PlanExecutionProps::default(),
        };
        crate::sql::optimizer::optimized_tree::attach_scalar_arena(
            &mut node,
            Arc::new(crate::sql::optimizer::scalar::ScalarArena::new()),
        );
        node
    }

    fn output_column_by_name_for_test<'a>(
        columns: &'a [crate::sql::analysis::OutputColumn],
        name: &str,
    ) -> &'a crate::sql::analysis::OutputColumn {
        columns
            .iter()
            .find(|column| column.name == name)
            .unwrap_or_else(|| panic!("missing output column {name}"))
    }

    fn assignment_expr_for_output(
        event: &crate::sql::optimizer::operator::ChangeEventSpec,
        output_column_id: crate::sql::column_id::ColumnId,
    ) -> crate::sql::optimizer::scalar::ScalarId {
        event
            .assignments
            .iter()
            .find(|assignment| assignment.output_column_id == output_column_id)
            .unwrap_or_else(|| panic!("missing assignment for output {output_column_id:?}"))
            .expr
            .expect("assignment expression")
    }

    fn assert_assignment_is_column_ref(
        arena: &crate::sql::optimizer::scalar::ScalarArena,
        expr: crate::sql::optimizer::scalar::ScalarId,
        expected: u32,
    ) {
        assert_eq!(
            arena.node(expr),
            &crate::sql::optimizer::scalar::ScalarNode::ColumnRef(
                crate::sql::column_id::ColumnId::new_for_test(expected)
            )
        );
    }

    fn assert_assignment_is_int_literal(
        arena: &crate::sql::optimizer::scalar::ScalarArena,
        expr: crate::sql::optimizer::scalar::ScalarId,
        expected: i64,
    ) {
        assert_eq!(
            arena.node(expr),
            &crate::sql::optimizer::scalar::ScalarNode::Literal(
                crate::sql::optimizer::scalar::HashableLiteral(
                    crate::sql::analysis::LiteralValue::Int(expected)
                )
            )
        );
    }

    fn assert_no_assignment_for_output(
        event: &crate::sql::optimizer::operator::ChangeEventSpec,
        output_column_id: crate::sql::column_id::ColumnId,
    ) {
        assert!(
            event
                .assignments
                .iter()
                .all(|assignment| assignment.output_column_id != output_column_id),
            "unexpected assignment for output {output_column_id:?}"
        );
    }

    fn assert_event_predicate_matches_action(
        arena: &crate::sql::optimizer::scalar::ScalarArena,
        event: &crate::sql::optimizer::operator::ChangeEventSpec,
        expected_action: i32,
    ) {
        use crate::sql::common::BinOp;
        use crate::sql::optimizer::scalar::{HashableLiteral, ScalarNode};

        let predicate = event.predicate.expect("action predicate");
        let ScalarNode::BinaryOp { op, left, right } = arena.node(predicate) else {
            panic!("expected action equality predicate");
        };
        assert_eq!(*op, BinOp::Eq);

        let mut saw_action_column = false;
        let mut saw_action_literal = false;
        for child in [*left, *right] {
            match arena.node(child) {
                ScalarNode::ColumnRef(id)
                    if *id == crate::sql::column_id::ColumnId::new_for_test(5) =>
                {
                    saw_action_column = true;
                }
                ScalarNode::Literal(HashableLiteral(crate::sql::analysis::LiteralValue::Int(
                    value,
                ))) if *value == i64::from(expected_action) => {
                    saw_action_literal = true;
                }
                other => panic!("unexpected action predicate child: {other:?}"),
            }
        }
        assert!(saw_action_column, "predicate must read __nr_merge_action");
        assert!(saw_action_literal, "predicate must compare expected action");
    }

    fn branch_kinds_for_test(
        expand: &crate::sql::optimizer::operator::ChangeEventExpandOp,
    ) -> Vec<crate::sql::common::change_stream::ChangeStreamBranchKind> {
        expand
            .events
            .iter()
            .map(|event| event.branch_kind)
            .collect()
    }

    #[test]
    fn iceberg_table_columns_maps_variant_to_largebinary() {
        use iceberg::spec::{NestedField, PrimitiveType, Type};

        let iceberg_schema = Arc::new(
            iceberg::spec::Schema::builder()
                .with_schema_id(1)
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Variant)).into(),
                    NestedField::optional(3, "b", Type::Primitive(PrimitiveType::Binary)).into(),
                ])
                .build()
                .expect("schema"),
        );
        let metadata = iceberg::spec::TableMetadataBuilder::new(
            iceberg_schema.as_ref().clone(),
            iceberg::spec::PartitionSpec::unpartition_spec(),
            iceberg::spec::SortOrder::unsorted_order(),
            "file:///tmp/iv3_variant_columns".to_string(),
            iceberg::spec::FormatVersion::V3,
            std::collections::HashMap::new(),
        )
        .expect("builder")
        .build()
        .expect("metadata")
        .metadata;
        let table = iceberg::table::Table::builder()
            .identifier(iceberg::TableIdent::from_strs(["db", "t"]).expect("ident"))
            .file_io(iceberg::io::FileIO::new_with_fs())
            .metadata(metadata)
            .build()
            .expect("table");

        let columns = iceberg_table_columns(&table).expect("columns");
        assert_eq!(columns[0].data_type, DataType::Int64);
        assert_eq!(columns[1].data_type, DataType::LargeBinary);
        assert_eq!(columns[2].data_type, DataType::Binary);
    }

    #[test]
    fn update_mor_change_event_expand_plan_has_expected_shape() {
        use crate::sql::common::change_stream::ChangeStreamBranchKind;
        use crate::sql::optimizer::operator::Operator;
        use crate::sql::optimizer::property::{DistributionSpec, HashSource};
        use crate::sql::optimizer::scalar::{HashableLiteral, ScalarNode};

        let target_columns = vec![non_null_col("id"), col("qty")];
        let plan = build_update_mor_change_event_expand_plan(
            update_mor_expand_child_plan_for_test(),
            &target_columns,
            77,
        )
        .expect("MOR UPDATE expand plan");
        let Operator::PhysicalChangeEventExpand(expand) = &plan.op else {
            panic!("expected PhysicalChangeEventExpand");
        };
        assert_eq!(plan.children.len(), 1);
        let Operator::PhysicalDistribution(distribution) = &plan.children[0].op else {
            panic!("expected pre-expand PhysicalDistribution");
        };
        assert_eq!(
            distribution.spec,
            DistributionSpec::HashPartitioned {
                cols: vec![crate::sql::column_id::ColumnId::new_for_test(3)],
                source: HashSource::ShuffleAgg,
            }
        );

        assert_eq!(expand.events.len(), 2);
        assert_eq!(
            expand.events[0].branch_kind,
            ChangeStreamBranchKind::DeleteDv
        );
        assert_eq!(
            expand.events[1].branch_kind,
            ChangeStreamBranchKind::ReuseData
        );

        let file = output_column_by_name_for_test(&expand.output_columns, "_file");
        let pos = output_column_by_name_for_test(&expand.output_columns, "_pos");
        let id = output_column_by_name_for_test(&expand.output_columns, "id");
        let qty = output_column_by_name_for_test(&expand.output_columns, "qty");
        let row_id = output_column_by_name_for_test(&expand.output_columns, "_row_id");
        let seq =
            output_column_by_name_for_test(&expand.output_columns, "_last_updated_sequence_number");
        let change_op = output_column_by_name_for_test(&expand.output_columns, "__change_op");
        let route = output_column_by_name_for_test(&expand.output_columns, "__change_data_route");
        assert!(file.is_internal);
        assert!(pos.is_internal);
        assert!(!id.is_internal);
        assert!(!qty.is_internal);
        assert!(row_id.is_internal);
        assert!(seq.is_internal);
        assert!(change_op.is_internal);
        assert!(route.is_internal);
        assert_eq!(expand.change_op_column_id, change_op.column_id);
        assert_eq!(expand.data_route_column_id, Some(route.column_id));
        assert_eq!(plan.output_columns.len(), expand.output_columns.len());
        assert!(
            plan.output_columns
                .iter()
                .zip(&expand.output_columns)
                .all(|(left, right)| left.column_id == right.column_id
                    && left.name == right.name
                    && left.is_internal == right.is_internal)
        );

        let arena = plan
            .execution_props
            .scalar_arena
            .as_deref()
            .expect("scalar arena");
        let delete = &expand.events[0];
        assert_assignment_is_column_ref(
            arena,
            assignment_expr_for_output(delete, file.column_id),
            1,
        );
        assert_assignment_is_column_ref(
            arena,
            assignment_expr_for_output(delete, pos.column_id),
            2,
        );
        assert_assignment_is_column_ref(arena, assignment_expr_for_output(delete, id.column_id), 4);
        assert_assignment_is_column_ref(
            arena,
            assignment_expr_for_output(delete, qty.column_id),
            5,
        );

        let reuse = &expand.events[1];
        assert_assignment_is_column_ref(arena, assignment_expr_for_output(reuse, id.column_id), 4);
        assert_assignment_is_column_ref(arena, assignment_expr_for_output(reuse, qty.column_id), 6);
        assert_assignment_is_column_ref(
            arena,
            assignment_expr_for_output(reuse, row_id.column_id),
            3,
        );
        let seq_expr = assignment_expr_for_output(reuse, seq.column_id);
        assert_eq!(
            arena.node(seq_expr),
            &ScalarNode::Literal(HashableLiteral(crate::sql::analysis::LiteralValue::Int(77)))
        );
    }

    #[test]
    fn merge_mor_change_event_expand_matched_update_shape() {
        use crate::sql::common::change_stream::ChangeStreamBranchKind;
        use crate::sql::optimizer::operator::Operator;
        use crate::sql::optimizer::property::{DistributionSpec, HashSource};

        let target_columns = vec![non_null_col("id"), col("qty")];
        let plan = build_merge_mor_change_event_expand_plan(
            merge_mor_expand_child_plan_for_test(true),
            &target_columns,
            101,
            true,
            false,
            false,
        )
        .expect("MOR MERGE matched UPDATE expand plan");
        let Operator::PhysicalChangeEventExpand(expand) = &plan.op else {
            panic!("expected PhysicalChangeEventExpand");
        };
        let Operator::PhysicalDistribution(distribution) = &plan.children[0].op else {
            panic!("expected pre-expand PhysicalDistribution");
        };
        assert_eq!(
            distribution.spec,
            DistributionSpec::HashPartitioned {
                cols: vec![crate::sql::column_id::ColumnId::new_for_test(4)],
                source: HashSource::ShuffleAgg,
            }
        );

        assert_eq!(expand.events.len(), 2);
        assert_eq!(
            branch_kinds_for_test(expand),
            vec![
                ChangeStreamBranchKind::DeleteDv,
                ChangeStreamBranchKind::ReuseData,
            ]
        );

        let file = output_column_by_name_for_test(&expand.output_columns, "_file");
        let pos = output_column_by_name_for_test(&expand.output_columns, "_pos");
        let id = output_column_by_name_for_test(&expand.output_columns, "id");
        let qty = output_column_by_name_for_test(&expand.output_columns, "qty");
        let row_id = output_column_by_name_for_test(&expand.output_columns, "_row_id");
        let seq =
            output_column_by_name_for_test(&expand.output_columns, "_last_updated_sequence_number");
        let change_op = output_column_by_name_for_test(&expand.output_columns, "__change_op");
        let route = output_column_by_name_for_test(&expand.output_columns, "__change_data_route");
        assert!(file.is_internal);
        assert!(pos.is_internal);
        assert!(!id.is_internal);
        assert!(!qty.is_internal);
        assert!(row_id.is_internal);
        assert!(seq.is_internal);
        assert!(change_op.is_internal);
        assert!(route.is_internal);
        assert_eq!(expand.change_op_column_id, change_op.column_id);
        assert_eq!(expand.data_route_column_id, Some(route.column_id));

        let arena = plan
            .execution_props
            .scalar_arena
            .as_deref()
            .expect("scalar arena");
        let delete = &expand.events[0];
        assert_event_predicate_matches_action(arena, delete, MERGE_ACTION_MATCHED_UPDATE);
        assert_assignment_is_column_ref(
            arena,
            assignment_expr_for_output(delete, file.column_id),
            1,
        );
        assert_assignment_is_column_ref(
            arena,
            assignment_expr_for_output(delete, pos.column_id),
            2,
        );
        assert_assignment_is_column_ref(arena, assignment_expr_for_output(delete, id.column_id), 6);
        assert_assignment_is_column_ref(
            arena,
            assignment_expr_for_output(delete, qty.column_id),
            7,
        );

        let reuse = &expand.events[1];
        assert_event_predicate_matches_action(arena, reuse, MERGE_ACTION_MATCHED_UPDATE);
        assert_assignment_is_column_ref(arena, assignment_expr_for_output(reuse, id.column_id), 6);
        assert_assignment_is_column_ref(arena, assignment_expr_for_output(reuse, qty.column_id), 8);
        assert_assignment_is_column_ref(
            arena,
            assignment_expr_for_output(reuse, row_id.column_id),
            3,
        );
        assert_assignment_is_int_literal(
            arena,
            assignment_expr_for_output(reuse, seq.column_id),
            101,
        );
    }

    #[test]
    fn merge_mor_change_event_expand_matched_delete_shape() {
        use crate::sql::common::change_stream::ChangeStreamBranchKind;
        use crate::sql::optimizer::operator::Operator;

        let target_columns = vec![non_null_col("id"), col("qty")];
        let plan = build_merge_mor_change_event_expand_plan(
            merge_mor_expand_child_plan_for_test(true),
            &target_columns,
            101,
            false,
            true,
            false,
        )
        .expect("MOR MERGE matched DELETE expand plan");
        let Operator::PhysicalChangeEventExpand(expand) = &plan.op else {
            panic!("expected PhysicalChangeEventExpand");
        };
        assert_eq!(
            branch_kinds_for_test(expand),
            vec![ChangeStreamBranchKind::DeleteDv]
        );

        let arena = plan
            .execution_props
            .scalar_arena
            .as_deref()
            .expect("scalar arena");
        let delete = &expand.events[0];
        assert_event_predicate_matches_action(arena, delete, MERGE_ACTION_MATCHED_DELETE);
        assert_assignment_is_column_ref(
            arena,
            assignment_expr_for_output(
                delete,
                output_column_by_name_for_test(&expand.output_columns, "_file").column_id,
            ),
            1,
        );
        assert_assignment_is_column_ref(
            arena,
            assignment_expr_for_output(
                delete,
                output_column_by_name_for_test(&expand.output_columns, "_pos").column_id,
            ),
            2,
        );
        assert_assignment_is_column_ref(
            arena,
            assignment_expr_for_output(
                delete,
                output_column_by_name_for_test(&expand.output_columns, "id").column_id,
            ),
            6,
        );
        assert_assignment_is_column_ref(
            arena,
            assignment_expr_for_output(
                delete,
                output_column_by_name_for_test(&expand.output_columns, "qty").column_id,
            ),
            7,
        );
    }

    #[test]
    fn merge_mor_change_event_expand_fresh_only_omitted_insert_column_outputs_null() {
        use crate::sql::common::change_stream::ChangeStreamBranchKind;
        use crate::sql::optimizer::operator::Operator;

        let target_columns = vec![non_null_col("id"), col("qty")];
        let plan = build_merge_mor_change_event_expand_plan(
            merge_mor_expand_child_plan_for_test(false),
            &target_columns,
            101,
            false,
            false,
            true,
        )
        .expect("MOR MERGE fresh-only expand plan");
        let Operator::PhysicalChangeEventExpand(expand) = &plan.op else {
            panic!("expected PhysicalChangeEventExpand");
        };
        assert_eq!(
            branch_kinds_for_test(expand),
            vec![ChangeStreamBranchKind::FreshData]
        );

        let arena = plan
            .execution_props
            .scalar_arena
            .as_deref()
            .expect("scalar arena");
        let fresh = &expand.events[0];
        assert_event_predicate_matches_action(arena, fresh, MERGE_ACTION_NOT_MATCHED_INSERT);
        let id = output_column_by_name_for_test(&expand.output_columns, "id");
        let qty = output_column_by_name_for_test(&expand.output_columns, "qty");
        assert_assignment_is_column_ref(arena, assignment_expr_for_output(fresh, id.column_id), 9);
        // Omitted INSERT target columns intentionally have no event assignment;
        // ChangeEventExpand fills unassigned output slots with NULL.
        assert_no_assignment_for_output(fresh, qty.column_id);
    }

    #[test]
    fn merge_mor_change_event_expand_mixed_update_and_insert_shape() {
        use crate::sql::common::change_stream::ChangeStreamBranchKind;
        use crate::sql::optimizer::operator::Operator;

        let target_columns = vec![non_null_col("id"), col("qty")];
        let plan = build_merge_mor_change_event_expand_plan(
            merge_mor_expand_child_plan_for_test(true),
            &target_columns,
            101,
            true,
            false,
            true,
        )
        .expect("MOR MERGE update+insert expand plan");
        let Operator::PhysicalChangeEventExpand(expand) = &plan.op else {
            panic!("expected PhysicalChangeEventExpand");
        };
        assert_eq!(
            branch_kinds_for_test(expand),
            vec![
                ChangeStreamBranchKind::DeleteDv,
                ChangeStreamBranchKind::ReuseData,
                ChangeStreamBranchKind::FreshData,
            ]
        );

        let arena = plan
            .execution_props
            .scalar_arena
            .as_deref()
            .expect("scalar arena");
        assert_event_predicate_matches_action(
            arena,
            &expand.events[0],
            MERGE_ACTION_MATCHED_UPDATE,
        );
        assert_event_predicate_matches_action(
            arena,
            &expand.events[1],
            MERGE_ACTION_MATCHED_UPDATE,
        );
        assert_event_predicate_matches_action(
            arena,
            &expand.events[2],
            MERGE_ACTION_NOT_MATCHED_INSERT,
        );

        let fresh = &expand.events[2];
        assert_assignment_is_column_ref(
            arena,
            assignment_expr_for_output(
                fresh,
                output_column_by_name_for_test(&expand.output_columns, "id").column_id,
            ),
            9,
        );
        assert_assignment_is_column_ref(
            arena,
            assignment_expr_for_output(
                fresh,
                output_column_by_name_for_test(&expand.output_columns, "qty").column_id,
            ),
            10,
        );
    }

    #[test]
    fn merge_mor_change_event_expand_mixed_delete_and_insert_shape() {
        use crate::sql::common::change_stream::ChangeStreamBranchKind;
        use crate::sql::optimizer::operator::Operator;

        let target_columns = vec![non_null_col("id"), col("qty")];
        let plan = build_merge_mor_change_event_expand_plan(
            merge_mor_expand_child_plan_for_test(true),
            &target_columns,
            101,
            false,
            true,
            true,
        )
        .expect("MOR MERGE delete+insert expand plan");
        let Operator::PhysicalChangeEventExpand(expand) = &plan.op else {
            panic!("expected PhysicalChangeEventExpand");
        };
        assert_eq!(
            branch_kinds_for_test(expand),
            vec![
                ChangeStreamBranchKind::DeleteDv,
                ChangeStreamBranchKind::FreshData,
            ]
        );

        let arena = plan
            .execution_props
            .scalar_arena
            .as_deref()
            .expect("scalar arena");
        assert_event_predicate_matches_action(
            arena,
            &expand.events[0],
            MERGE_ACTION_MATCHED_DELETE,
        );
        assert_event_predicate_matches_action(
            arena,
            &expand.events[1],
            MERGE_ACTION_NOT_MATCHED_INSERT,
        );
    }

    #[test]
    fn cow_rewrite_query_rewrites_whole_file_and_preserves_row_id() {
        // Two matched rows (row_ids 7,9) for one touched file; the rewrite query
        // must scan the whole file via the synthetic ExplicitFiles table, LEFT
        // JOIN the matched new values on `_row_id`, project user columns
        // (replacement where matched, original scan value otherwise), preserve
        // `_row_id`, and bump `_last_updated_sequence_number` only for matched
        // rows.
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, true),
            arrow::datatypes::Field::new("v", DataType::Utf8, true),
        ]));
        let new_rows = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![2, 4])) as ArrayRef,
                Arc::new(StringArray::from(vec!["bb", "dd"])) as ArrayRef,
            ],
        )
        .expect("new rows");
        let old_rows = RecordBatch::new_empty(schema);
        let matched = MatchedUpdateBatch {
            row_ids: vec![7, 9],
            file_paths: vec!["f.parquet".to_string(), "f.parquet".to_string()],
            row_positions: vec![1, 3],
            old_rows,
            new_rows,
        };

        let query = build_cow_rewrite_query(
            &iceberg_target(),
            "__nr_cow_t_abc",
            &matched,
            &[0, 1],
            &[
                typed_col("id", DataType::Int64),
                typed_col("v", DataType::Utf8),
            ],
            42,
        )
        .expect("query");
        let sql = query.to_string();

        // Scans the synthetic ExplicitFiles table under default_catalog (so a
        // session iceberg catalog cannot reroute it), LEFT JOINs the matched
        // VALUES on `_row_id`, and orders by `_row_id`.
        assert!(sql.contains("`default_catalog`"), "{sql}");
        assert!(sql.contains("`__nr_cow_t_abc`"), "{sql}");
        assert!(sql.contains("LEFT JOIN"), "{sql}");
        assert!(sql.contains("VALUES"), "{sql}");
        // Whole-file rewrite: no outer WHERE filter (all rows re-emitted).
        assert!(!sql.contains(" WHERE "), "{sql}");
        // Conditional replacement on the match key, `_row_id` preserved from the
        // scan, and the new sequence number applied only to matched rows.
        assert!(sql.contains("CASE WHEN"), "{sql}");
        assert!(sql.contains("IS NOT NULL"), "{sql}");
        assert!(sql.contains("AS `_row_id`"), "{sql}");
        assert!(sql.contains("_last_updated_sequence_number"), "{sql}");
        assert!(sql.contains("42"), "{sql}");
        // Replacement values flow from the matched new_rows VALUES.
        assert!(sql.contains("'bb'"), "{sql}");
        assert!(sql.contains("'dd'"), "{sql}");
        assert!(sql.contains("ORDER BY"), "{sql}");
    }

    #[test]
    fn cow_rewrite_query_casts_variant_values_payloads() {
        let payload = [0x0c_u8, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03];
        let schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int64, true),
            arrow::datatypes::Field::new("v", DataType::LargeBinary, true),
        ]));
        let new_rows = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![10])) as ArrayRef,
                Arc::new(arrow::array::LargeBinaryArray::from_iter_values([
                    payload.as_slice()
                ])) as ArrayRef,
            ],
        )
        .expect("new rows");
        let old_rows = RecordBatch::new_empty(schema);
        let matched = MatchedUpdateBatch {
            row_ids: vec![7],
            file_paths: vec!["f.parquet".to_string()],
            row_positions: vec![1],
            old_rows,
            new_rows,
        };

        let query = build_cow_rewrite_query(
            &iceberg_target(),
            "__nr_cow_t_abc",
            &matched,
            &[0],
            &[
                typed_col("id", DataType::Int64),
                typed_col("v", DataType::LargeBinary),
            ],
            42,
        )
        .expect("query");
        let sql = query.to_string();

        assert!(sql.contains("CAST(X'0C000000010203' AS VARIANT)"), "{sql}");
        assert!(sql.contains("CASE WHEN"), "{sql}");
    }

    #[test]
    fn reject_reserved_update_columns() {
        let err = validate_update_assignments(
            &[crate::sql::parser::ast::UpdateAssignment {
                column: "_row_id".to_string(),
                value: sqlparser::ast::Expr::Value(
                    sqlparser::ast::Value::Number("1".to_string(), false).into(),
                ),
            }],
            &[col("id"), col("v")],
            &[],
        )
        .expect_err("must reject");
        assert!(err.contains("reserved Iceberg metadata column"), "{err}");
    }

    #[test]
    fn reject_partition_column_update() {
        let err = validate_update_assignments(
            &[crate::sql::parser::ast::UpdateAssignment {
                column: "id".to_string(),
                value: sqlparser::ast::Expr::Value(
                    sqlparser::ast::Value::Number("1".to_string(), false).into(),
                ),
            }],
            &[col("id"), col("v")],
            &["id".to_string()],
        )
        .expect_err("must reject");
        assert!(err.contains("partition column"), "{err}");
    }

    #[test]
    fn cow_host_duplicate_row_ids_are_rejected_before_rewrite() {
        let err = validate_unique_target_row_ids(&[7, 8, 7]).expect_err("duplicate");
        assert!(err.contains("_row_id=7"), "{err}");
    }

    #[test]
    fn update_match_query_projects_identity_columns() {
        let sql = build_update_match_query_sql(
            "ice.db1.t AS t",
            "t",
            Some("staging.s AS s"),
            &[("v", "s.v")],
            Some("t.id = s.id"),
        );
        assert!(sql.contains("t._row_id AS __nr_row_id"), "{sql}");
        assert!(sql.contains("s.v AS __nr_new_v"), "{sql}");
        assert!(sql.contains("WHERE t.id = s.id"), "{sql}");
    }

    #[test]
    fn update_change_stream_target_sql_pins_branch_read_snapshot() {
        let sql = update_change_stream_target_sql(&iceberg_target(), "t", "dev");
        assert!(sql.contains("FOR VERSION AS OF 'dev'"), "{sql}");
        assert!(sql.ends_with(" AS t"), "{sql}");
    }

    #[test]
    fn update_assignment_projection_casts_to_target_type() {
        let assignments = vec![crate::sql::parser::ast::UpdateAssignment {
            column: "v".to_string(),
            value: sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new("src_v")),
        }];
        let projected = update_assignment_projection_sql(
            &assignments,
            &[
                typed_col("id", DataType::Int64),
                typed_col("v", DataType::Int32),
            ],
        )
        .expect("assignment projection");

        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].0, "v");
        assert!(
            projected[0].1.contains("CAST((src_v) AS INT)"),
            "{:?}",
            projected
        );
    }

    #[test]
    fn update_change_stream_match_query_uses_casted_assignment_projection() {
        let assignments = vec![crate::sql::parser::ast::UpdateAssignment {
            column: "v".to_string(),
            value: sqlparser::ast::Expr::Identifier(sqlparser::ast::Ident::new("src_v")),
        }];
        let projected = update_assignment_projection_sql(
            &assignments,
            &[
                typed_col("id", DataType::Int64),
                typed_col("v", DataType::Int32),
            ],
        )
        .expect("assignment projection");
        let projected_refs = projected
            .iter()
            .map(|(column, expr)| (column.as_str(), expr.as_str()))
            .collect::<Vec<_>>();
        let target_sql = update_change_stream_target_sql(&iceberg_target(), "t", "main");
        let sql = build_update_match_query_sql(
            &target_sql,
            "t",
            Some("staging.s AS s"),
            &projected_refs,
            Some("t.id = s.id"),
        );
        assert!(sql.contains("CAST((src_v) AS INT) AS __nr_new_v"), "{sql}");
        assert!(sql.contains("t._row_id AS __nr_row_id"), "{sql}");
    }

    fn typed_col(name: &str, data_type: DataType) -> ColumnDef {
        ColumnDef {
            name: name.to_string(),
            data_type,
            nullable: false,
            write_default: None,
            logical_type: None,
        }
    }

    #[test]
    fn merge_match_query_projects_nullable_target_columns() {
        let sql = build_merge_match_query_sql(
            "ice.db1.t AS t",
            "t",
            "staging.s AS s",
            "t.id = s.id",
            None,
            None,
            &[col("id"), col("v")],
            &[("v", "s.v")],
            &[("id", "s.id"), ("v", "s.v")],
            Some(MERGE_ACTION_MATCHED_UPDATE),
            true,
        );

        assert!(!sql.contains("t.*"), "{sql}");
        assert!(
            sql.contains("CASE WHEN t.`_row_id` IS NOT NULL THEN t.`id` ELSE NULL END AS `id`"),
            "{sql}"
        );
        assert!(sql.contains("(s.v) AS __nr_new_v"), "{sql}");
        assert!(sql.contains("(s.id) AS __nr_ins_id"), "{sql}");
        assert!(sql.contains("AS __nr_merge_action"), "{sql}");
        assert!(sql.contains("AS __nr_merge_assert_key"), "{sql}");
    }

    #[test]
    fn merge_unmatched_insert_query_uses_distributed_append_shape() {
        let raw = crate::sql::parser::parse_sql_raw(
            "MERGE INTO t AS t \
             USING (SELECT 3 AS id, 4 AS v) AS s \
             ON t.id = s.id \
             WHEN NOT MATCHED AND s.id > 0 THEN INSERT (id) VALUES (s.id)",
        )
        .expect("parse MERGE");
        let stmt = crate::engine::statement::convert_sqlparser_merge_to_custom(&raw)
            .expect("convert MERGE");
        let target_columns = vec![col("id"), col("v")];
        let insert_columns = resolve_merge_insert_columns(
            &stmt.not_matched.as_ref().expect("not matched").action,
            &target_columns,
        )
        .expect("insert columns");
        let state = Arc::new(StandaloneState::default());

        let query = build_merge_unmatched_insert_query(
            &state,
            &iceberg_target(),
            &stmt,
            None,
            &target_columns,
            &insert_columns,
        )
        .expect("query");
        let sql = query.to_string();

        assert!(sql.contains("LEFT JOIN"), "{sql}");
        assert!(sql.contains("_row_id"), "{sql}");
        assert!(sql.contains("IS NULL"), "{sql}");
        assert!(sql.contains("CAST((s.id) AS BIGINT) AS `id`"), "{sql}");
        assert!(sql.contains("CAST(NULL AS BIGINT) AS `v`"), "{sql}");
        assert!(sql.contains("(s.id > 0)"), "{sql}");
    }
}
