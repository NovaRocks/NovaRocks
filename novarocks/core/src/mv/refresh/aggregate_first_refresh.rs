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

//! Aggregate MV first-refresh preparation.
//!
//! This module owns state-shaped query preparation and result materialization.
//! The standalone engine supplies analysis and query execution through an
//! invocation-local callback; lifecycle and Iceberg writes stay in the engine.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::exec::chunk::Chunk;
use crate::mv::aggregate_state::aggregate_sql_calls::AggregateSqlCalls;
use crate::mv::aggregate_state::mv_agg_state::{
    AggregateMvLayout, materialize_aggregate_result_chunks,
};
use crate::mv::model::{AggregateStateRole, VisibleAggregateOutput};
use crate::mv::refresh::pin::{RefreshSnapshotPin, inject_pin_as_for_version_as_of};
use crate::runtime::query_result::{QueryResult, record_batch_to_chunk};

pub(crate) struct AggregateStateRead {
    pub(crate) result: QueryResult,
    pub(crate) source_layout: AggregateMvLayout,
}

pub(crate) fn prepare_aggregate_first_refresh_chunks<F>(
    select_sql: &str,
    calls: &AggregateSqlCalls,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    read: &mut F,
) -> Result<Vec<Chunk>, String>
where
    F: FnMut(&str, &AggregateSqlCalls, sqlparser::ast::Query) -> Result<AggregateStateRead, String>,
{
    let read = read_aggregate_state(
        select_sql,
        calls,
        pin,
        current_catalog,
        current_database,
        read,
    )?;
    let target_layout = read.source_layout.clone();
    normalize_and_materialize_aggregate_read(read, calls, &target_layout, calls)
}

fn read_aggregate_state<F>(
    select_sql: &str,
    calls: &AggregateSqlCalls,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    read: &mut F,
) -> Result<AggregateStateRead, String>
where
    F: FnMut(&str, &AggregateSqlCalls, sqlparser::ast::Query) -> Result<AggregateStateRead, String>,
{
    let state_sql =
        crate::mv::aggregate_state::mv_shape::rewrite_select_sql_for_state(select_sql, calls)?;
    let mut state_query = parse_stored_select_query(&state_sql)?;
    inject_pin_as_for_version_as_of(
        &mut state_query,
        pin,
        &HashSet::new(),
        current_catalog,
        current_database,
    )?;
    read(select_sql, calls, state_query)
}

pub(crate) fn prepare_branch_union_aggregate_first_refresh_chunks<F>(
    select_sql: &str,
    branch_count: usize,
    first_branch_calls: &AggregateSqlCalls,
    pin: &RefreshSnapshotPin,
    current_catalog: Option<&str>,
    current_database: &str,
    read: &mut F,
) -> Result<Vec<Chunk>, String>
where
    F: FnMut(&str, &AggregateSqlCalls, sqlparser::ast::Query) -> Result<AggregateStateRead, String>,
{
    let branches = branch_union_first_refresh_branch_queries(select_sql, branch_count)?;
    let mut target_layout = None;
    let mut prepared = Vec::new();
    for (branch_index, (branch_query, branch_sql)) in branches.into_iter().enumerate() {
        let branch_id = i32::try_from(branch_index).map_err(|_| {
            format!(
                "iceberg branch UNION ALL aggregate first refresh branch index {branch_index} exceeds Int32"
            )
        })?;
        let branch_calls =
            crate::mv::aggregate_state::aggregate_sql_calls::extract_aggregate_sql_calls(
                &branch_query,
            )?;
        if branch_index == 0 && &branch_calls != first_branch_calls {
            return Err(
                "branch UNION ALL aggregate first branch calls drifted from the validated contract"
                    .to_string(),
            );
        }
        let branch_read = read_aggregate_state(
            &branch_sql,
            &branch_calls,
            pin,
            current_catalog,
            current_database,
            read,
        )?;
        let canonical_layout = target_layout
            .get_or_insert_with(|| branch_read.source_layout.clone())
            .clone();
        validate_aggregate_layout_compatibility(
            branch_index,
            &branch_calls,
            &branch_read.source_layout,
            first_branch_calls,
            &canonical_layout,
        )?;
        let branch_chunks = normalize_and_materialize_aggregate_read(
            branch_read,
            &branch_calls,
            &canonical_layout,
            first_branch_calls,
        )?;
        prepared.extend(append_branch_id_to_chunks(branch_chunks, branch_id)?);
    }
    Ok(prepared)
}

fn branch_union_first_refresh_branch_queries(
    select_sql: &str,
    branch_count: usize,
) -> Result<Vec<(sqlparser::ast::Query, String)>, String> {
    let query = parse_stored_select_query(select_sql).map_err(|error| {
        format!("iceberg branch UNION ALL aggregate first refresh SELECT parse error: {error}")
    })?;
    let mut branch_bodies = Vec::new();
    flatten_branch_union_all_set_expr(query.body.as_ref(), &mut branch_bodies)?;
    if branch_bodies.len() != branch_count {
        return Err(format!(
            "iceberg branch UNION ALL aggregate first refresh expected {branch_count} branches, found {}",
            branch_bodies.len()
        ));
    }
    branch_bodies
        .into_iter()
        .map(|body| {
            let mut branch_query = query.clone();
            branch_query.body = Box::new(body);
            let branch_sql = branch_query.to_string();
            Ok((branch_query, branch_sql))
        })
        .collect()
}

fn flatten_branch_union_all_set_expr(
    body: &sqlparser::ast::SetExpr,
    out: &mut Vec<sqlparser::ast::SetExpr>,
) -> Result<(), String> {
    match body {
        sqlparser::ast::SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            if !matches!(op, sqlparser::ast::SetOperator::Union)
                || !matches!(set_quantifier, sqlparser::ast::SetQuantifier::All)
            {
                return Err(
                    "iceberg branch UNION ALL aggregate first refresh supports UNION ALL only"
                        .to_string(),
                );
            }
            flatten_branch_union_all_set_expr(left, out)?;
            flatten_branch_union_all_set_expr(right, out)
        }
        sqlparser::ast::SetExpr::Query(query) => {
            flatten_branch_union_all_set_expr(query.body.as_ref(), out)
        }
        sqlparser::ast::SetExpr::Select(_) => {
            out.push(body.clone());
            Ok(())
        }
        _ => Err(
            "iceberg branch UNION ALL aggregate first refresh expects SELECT branches".to_string(),
        ),
    }
}

fn append_branch_id_to_chunks(chunks: Vec<Chunk>, branch_id: i32) -> Result<Vec<Chunk>, String> {
    chunks
        .into_iter()
        .map(|chunk| append_branch_id_to_chunk(chunk, branch_id))
        .collect()
}

fn append_branch_id_to_chunk(chunk: Chunk, branch_id: i32) -> Result<Chunk, String> {
    let mut fields = chunk
        .batch
        .schema()
        .fields()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    fields.push(Arc::new(arrow::datatypes::Field::new(
        crate::mv::persistence::schema::BRANCH_ID_COLUMN_NAME,
        arrow::datatypes::DataType::Int32,
        false,
    )));
    let mut columns = chunk.batch.columns().to_vec();
    columns.push(Arc::new(arrow::array::Int32Array::from(vec![
        branch_id;
        chunk
            .batch
            .num_rows(
            )
    ])));
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|error| {
        format!(
            "append branch id to branch UNION ALL aggregate first refresh chunk failed: {error}"
        )
    })?;
    record_batch_to_chunk(batch)
}

fn parse_stored_select_query(sql: &str) -> Result<sqlparser::ast::Query, String> {
    let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)
        .map_err(|error| format!("stored MV SELECT normalize error: {error}"))?;
    let statement = crate::sql::parser::parse_normalized_sql_raw(&normalized)
        .map_err(|error| format!("sql parser error: {error}"))?;
    let sqlparser::ast::Statement::Query(query) = statement else {
        return Err("stored MV SQL must be a SELECT query".to_string());
    };
    Ok(*query)
}

fn normalize_and_materialize_aggregate_read(
    mut read: AggregateStateRead,
    source_calls: &AggregateSqlCalls,
    target_layout: &AggregateMvLayout,
    target_calls: &AggregateSqlCalls,
) -> Result<Vec<Chunk>, String> {
    validate_aggregate_layout_compatibility(
        0,
        source_calls,
        &read.source_layout,
        target_calls,
        target_layout,
    )?;
    let source_names =
        aggregate_state_source_result_column_names(&read.source_layout, source_calls)?;
    let target_names = aggregate_state_result_column_names(target_layout, target_calls)?;
    if source_names.len() != target_names.len() {
        return Err(format!(
            "aggregate MV state result source/target slot count mismatch: source={} target={}",
            source_names.len(),
            target_names.len()
        ));
    }

    let metadata_names = read
        .result
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    let metadata_permutation = exact_name_permutation(
        &metadata_names,
        &source_names,
        "aggregate MV state result metadata",
    )?;
    let old_columns = std::mem::take(&mut read.result.columns);
    read.result.columns = metadata_permutation
        .iter()
        .zip(target_names.iter())
        .map(|(source_index, target_name)| {
            let mut column = old_columns[*source_index].clone();
            column.name.clone_from(target_name);
            column
        })
        .collect();

    read.result.chunks = read
        .result
        .chunks
        .into_iter()
        .map(|chunk| {
            let schema = chunk.batch.schema();
            let actual_names = schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>();
            let permutation = exact_name_permutation(
                &actual_names,
                &source_names,
                "aggregate MV state result chunk",
            )?;
            reorder_and_rename_chunk_columns(chunk, &target_names, &permutation)
        })
        .collect::<Result<Vec<_>, String>>()?;
    materialize_aggregate_result_chunks(read.result, target_layout)
}

fn aggregate_state_source_result_column_names(
    layout: &AggregateMvLayout,
    calls: &AggregateSqlCalls,
) -> Result<Vec<String>, String> {
    let mut names = aggregate_state_result_column_names(layout, calls)?;
    for (projection_index, output) in calls.visible_outputs.iter().enumerate() {
        if let VisibleAggregateOutput::GroupKey(group_key_index) = output {
            let group_key = calls.group_keys.get(*group_key_index).ok_or_else(|| {
                format!("aggregate MV state result group key index {group_key_index} out of range")
            })?;
            names[projection_index].clone_from(&group_key.output_name);
        }
    }
    Ok(names)
}

fn validate_aggregate_layout_compatibility(
    branch_index: usize,
    source_calls: &AggregateSqlCalls,
    source_layout: &AggregateMvLayout,
    target_calls: &AggregateSqlCalls,
    target_layout: &AggregateMvLayout,
) -> Result<(), String> {
    let mismatch = |dimension: &str| {
        Err(format!(
            "aggregate MV branch {branch_index} {dimension} mismatch with branch 0"
        ))
    };
    if source_calls.visible_outputs != target_calls.visible_outputs {
        return mismatch("visible output order");
    }
    if source_calls.group_keys.len() != target_calls.group_keys.len() {
        return mismatch("group-key count");
    }
    if source_calls.aggregates.len() != target_calls.aggregates.len() {
        return mismatch("aggregate count");
    }
    for (aggregate_index, (source, target)) in source_calls
        .aggregates
        .iter()
        .zip(target_calls.aggregates.iter())
        .enumerate()
    {
        if source.function != target.function {
            return mismatch(&format!("aggregate {aggregate_index} function"));
        }
        let input_kind_matches = matches!(
            (&source.input, &target.input),
            (
                crate::mv::aggregate_state::mv_shape::AggregateInput::Star,
                crate::mv::aggregate_state::mv_shape::AggregateInput::Star
            ) | (
                crate::mv::aggregate_state::mv_shape::AggregateInput::Expr(_),
                crate::mv::aggregate_state::mv_shape::AggregateInput::Expr(_)
            )
        );
        if !input_kind_matches {
            return mismatch(&format!("aggregate {aggregate_index} input kind"));
        }
    }
    if source_layout.visible_columns.len() != target_layout.visible_columns.len() {
        return mismatch("visible column count");
    }
    for (column_index, (source, target)) in source_layout
        .visible_columns
        .iter()
        .zip(target_layout.visible_columns.iter())
        .enumerate()
    {
        if source.data_type != target.data_type {
            return mismatch(&format!("visible column {column_index} Arrow type"));
        }
        if source.sql_type != target.sql_type {
            return mismatch(&format!("visible column {column_index} SQL type"));
        }
        if source.nullable != target.nullable {
            return mismatch(&format!("visible column {column_index} nullability"));
        }
        if source.source_index != target.source_index {
            return mismatch(&format!("visible column {column_index} source index"));
        }
    }
    if source_layout.state_columns.len() != target_layout.state_columns.len() {
        return mismatch("state column count");
    }
    for (column_index, (source, target)) in source_layout
        .state_columns
        .iter()
        .zip(target_layout.state_columns.iter())
        .enumerate()
    {
        if source.data_type != target.data_type {
            return mismatch(&format!("state column {column_index} Arrow type"));
        }
        if source.sql_type != target.sql_type {
            return mismatch(&format!("state column {column_index} SQL type"));
        }
        if source.nullable != target.nullable {
            return mismatch(&format!("state column {column_index} nullability"));
        }
        if source.visible_source_index != target.visible_source_index {
            return mismatch(&format!("state column {column_index} visible source index"));
        }
        if source.aggregate_index != target.aggregate_index {
            return mismatch(&format!("state column {column_index} aggregate index"));
        }
        if source.function != target.function {
            return mismatch(&format!("state column {column_index} function"));
        }
        if source.state_role != target.state_role {
            return mismatch(&format!("state column {column_index} role"));
        }
        if source.count_star != target.count_star {
            return mismatch(&format!("state column {column_index} count-star flag"));
        }
    }
    if source_layout.aggregate_input_types.len() != target_layout.aggregate_input_types.len() {
        return mismatch("aggregate input type count");
    }
    for (aggregate_index, (source, target)) in source_layout
        .aggregate_input_types
        .iter()
        .zip(target_layout.aggregate_input_types.iter())
        .enumerate()
    {
        if source != target {
            return mismatch(&format!("aggregate {aggregate_index} input type"));
        }
    }
    if source_layout.group_key_source_indexes != target_layout.group_key_source_indexes {
        return mismatch("group-key source indexes");
    }
    if source_layout.physical_columns.len() != target_layout.physical_columns.len() {
        return mismatch("physical column count");
    }
    for (column_index, (source, target)) in source_layout
        .physical_columns
        .iter()
        .zip(target_layout.physical_columns.iter())
        .enumerate()
    {
        if source.column.data_type != target.column.data_type {
            return mismatch(&format!("physical column {column_index} SQL type"));
        }
        if source.column.nullable != target.column.nullable {
            return mismatch(&format!("physical column {column_index} nullability"));
        }
        if source.column.aggregation != target.column.aggregation {
            return mismatch(&format!("physical column {column_index} aggregation role"));
        }
        if source.column.default != target.column.default {
            return mismatch(&format!("physical column {column_index} default"));
        }
        if source.visible != target.visible {
            return mismatch(&format!("physical column {column_index} visibility role"));
        }
        if source.is_key != target.is_key {
            return mismatch(&format!("physical column {column_index} key role"));
        }
    }
    Ok(())
}

fn exact_name_permutation(
    actual_names: &[&str],
    expected_names: &[String],
    label: &str,
) -> Result<Vec<usize>, String> {
    if actual_names.len() != expected_names.len() {
        return Err(format!(
            "{label} column count mismatch: actual={} expected={}",
            actual_names.len(),
            expected_names.len()
        ));
    }
    let mut used = vec![false; actual_names.len()];
    let mut permutation = Vec::with_capacity(expected_names.len());
    for expected_name in expected_names {
        let candidates = actual_names
            .iter()
            .enumerate()
            .filter(|(index, actual_name)| {
                !used[*index] && actual_name.eq_ignore_ascii_case(expected_name)
            })
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let [index] = candidates.as_slice() else {
            return Err(format!(
                "{label} requires exactly one column named `{expected_name}`, found {} in [{}]",
                candidates.len(),
                actual_names.join(", ")
            ));
        };
        used[*index] = true;
        permutation.push(*index);
    }
    Ok(permutation)
}

fn aggregate_state_result_column_names(
    layout: &AggregateMvLayout,
    calls: &AggregateSqlCalls,
) -> Result<Vec<String>, String> {
    let mut names = Vec::with_capacity(calls.visible_outputs.len() + layout.state_columns.len());
    for output in &calls.visible_outputs {
        match output {
            VisibleAggregateOutput::GroupKey(group_key_index) => {
                let visible_source_index = layout
                    .group_key_source_indexes
                    .get(*group_key_index)
                    .ok_or_else(|| {
                        format!(
                            "aggregate MV state result group key index {group_key_index} out of range"
                        )
                    })?;
                let visible = layout
                    .visible_columns
                    .get(*visible_source_index)
                    .ok_or_else(|| {
                        format!(
                            "aggregate MV state result visible source index {visible_source_index} out of range"
                        )
                    })?;
                names.push(visible.name.clone());
            }
            VisibleAggregateOutput::Aggregate(aggregate_index) => {
                let state_column = layout
                    .state_columns
                    .iter()
                    .find(|column| {
                        column.state_role == AggregateStateRole::Single
                            && column.aggregate_index == *aggregate_index
                    })
                    .ok_or_else(|| {
                        format!(
                            "aggregate MV state result missing state column for aggregate index {aggregate_index}"
                        )
                    })?;
                names.push(state_column.name.clone());
            }
        }
    }
    names.extend(
        layout
            .state_columns
            .iter()
            .filter(|column| column.state_role == AggregateStateRole::RetractionCount)
            .map(|column| column.name.clone()),
    );
    Ok(names)
}

fn reorder_and_rename_chunk_columns(
    chunk: Chunk,
    names: &[String],
    permutation: &[usize],
) -> Result<Chunk, String> {
    if chunk.batch.num_columns() != names.len() || permutation.len() != names.len() {
        return Err(format!(
            "aggregate MV state result chunk column count mismatch: columns={} names={} permutation={}",
            chunk.batch.num_columns(),
            names.len(),
            permutation.len()
        ));
    }
    if permutation
        .iter()
        .any(|source_index| *source_index >= chunk.batch.num_columns())
    {
        return Err(format!(
            "aggregate MV state result column permutation out of range: columns={} permutation={permutation:?}",
            chunk.batch.num_columns()
        ));
    }
    let source_schema = chunk.batch.schema();
    let fields = permutation
        .iter()
        .zip(names.iter())
        .map(|(source_index, name)| {
            Arc::new(
                source_schema
                    .field(*source_index)
                    .clone()
                    .with_name(name.clone()),
            )
        })
        .collect::<Vec<_>>();
    let columns = permutation
        .iter()
        .map(|source_index| chunk.batch.column(*source_index).clone())
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|error| format!("reorder aggregate MV state result columns failed: {error}"))?;
    record_batch_to_chunk(batch)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, BinaryArray, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use super::*;
    use crate::mv::aggregate_state::mv_agg_state::{AggregateStateColumn, AggregateVisibleColumn};
    use crate::mv::aggregate_state::physical_column::starrocks_physical_column;
    use crate::mv::aggregate_state::state_codec::encode_count_state;
    use crate::mv::model::{AggregateFunctionKind, AggregateStateRole};
    use crate::runtime::query_result::{QueryResultColumn, record_batch_to_chunk};
    use novarocks_catalog::schema::SqlType;

    fn parse_calls(sql: &str) -> AggregateSqlCalls {
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(sql)
            .expect("normalize aggregate select");
        let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized)
            .expect("parse aggregate select");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected query");
        };
        crate::mv::aggregate_state::aggregate_sql_calls::extract_aggregate_sql_calls(&query)
            .expect("extract aggregate calls")
    }

    fn count_layout(group_key: &str) -> AggregateMvLayout {
        let row_id = starrocks_physical_column(
            "__row_id__".to_string(),
            SqlType::String,
            false,
            false,
            true,
        );
        let group =
            starrocks_physical_column(group_key.to_string(), SqlType::String, true, true, false);
        let counter =
            starrocks_physical_column("c".to_string(), SqlType::BigInt, false, true, false);
        let state = starrocks_physical_column(
            "__agg_state_c".to_string(),
            SqlType::Binary,
            false,
            false,
            false,
        );
        AggregateMvLayout {
            row_id_column: row_id.clone(),
            visible_columns: vec![
                AggregateVisibleColumn {
                    name: group_key.to_string(),
                    data_type: DataType::Utf8,
                    sql_type: SqlType::String,
                    nullable: true,
                    source_index: 0,
                },
                AggregateVisibleColumn {
                    name: "c".to_string(),
                    data_type: DataType::Int64,
                    sql_type: SqlType::BigInt,
                    nullable: false,
                    source_index: 1,
                },
            ],
            state_columns: vec![AggregateStateColumn {
                name: "__agg_state_c".to_string(),
                data_type: DataType::Binary,
                sql_type: SqlType::Binary,
                nullable: false,
                visible_source_index: 1,
                aggregate_index: 0,
                function: AggregateFunctionKind::Count,
                state_role: AggregateStateRole::Single,
                count_star: true,
            }],
            aggregate_input_types: vec![None],
            group_key_source_indexes: vec![0],
            physical_columns: vec![row_id, group, counter, state],
        }
    }

    fn reordered_count_result() -> QueryResult {
        count_result("region", 2)
    }

    fn count_result(group_key: &str, count: i64) -> QueryResult {
        let state = encode_count_state(count);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("__agg_state_c", DataType::Binary, false),
                Field::new(group_key, DataType::Utf8, true),
            ])),
            vec![
                Arc::new(BinaryArray::from_vec(vec![state.as_slice()])),
                Arc::new(StringArray::from(vec![Some("east")])),
            ],
        )
        .expect("state-shaped result batch");
        QueryResult {
            columns: vec![
                QueryResultColumn {
                    name: "__agg_state_c".to_string(),
                    data_type: DataType::Binary,
                    nullable: false,
                    logical_type: Some(SqlType::Binary),
                },
                QueryResultColumn {
                    name: group_key.to_string(),
                    data_type: DataType::Utf8,
                    nullable: true,
                    logical_type: Some(SqlType::String),
                },
            ],
            chunks: vec![record_batch_to_chunk(batch).expect("chunk")],
        }
    }

    fn branch_select_sql() -> &'static str {
        "select region, count(*) as c from ice.sales.fact_a group by region \
         union all \
         select area, count(*) as c from ice.sales.fact_b group by area"
    }

    fn first_branch_calls(select_sql: &str) -> AggregateSqlCalls {
        let normalized = crate::sql::parser::dialect::normalize_for_raw_parse(select_sql)
            .expect("normalize union");
        let stmt = crate::sql::parser::parse_normalized_sql_raw(&normalized).expect("parse union");
        let sqlparser::ast::Statement::Query(query) = stmt else {
            panic!("expected union query");
        };
        let sqlparser::ast::SetExpr::SetOperation { left, .. } = query.body.as_ref() else {
            panic!("expected set operation");
        };
        let mut first_query = query.as_ref().clone();
        first_query.body = left.clone();
        crate::mv::aggregate_state::aggregate_sql_calls::extract_aggregate_sql_calls(&first_query)
            .expect("first calls")
    }

    fn branch_pin() -> RefreshSnapshotPin {
        RefreshSnapshotPin::from_entries_for_tests(&[
            ("ice.sales.fact_a", 41, "a-uuid"),
            ("ice.sales.fact_b", 42, "b-uuid"),
        ])
    }

    #[test]
    fn single_preparation_injects_exact_pin_and_normalizes_reordered_result() {
        let select_sql = "select region, count(*) as c from ice.sales.fact group by region";
        let calls = parse_calls(select_sql);
        let pin =
            RefreshSnapshotPin::from_entries_for_tests(&[("ice.sales.fact", 42, "fact-uuid")]);
        let mut reads = 0;
        let mut read =
            |visible_sql: &str, actual_calls: &AggregateSqlCalls, query: sqlparser::ast::Query| {
                reads += 1;
                assert_eq!(visible_sql, select_sql);
                assert_eq!(actual_calls, &calls);
                assert!(
                    query.to_string().contains("VERSION AS OF 42"),
                    "query={query}"
                );
                Ok(AggregateStateRead {
                    result: reordered_count_result(),
                    source_layout: count_layout("region"),
                })
            };

        let chunks = prepare_aggregate_first_refresh_chunks(
            select_sql,
            &calls,
            &pin,
            Some("ice"),
            "sales",
            &mut read,
        )
        .expect("prepare aggregate first refresh");

        assert_eq!(reads, 1);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].batch.num_rows(), 1);
        assert_eq!(chunks[0].batch.schema().field(0).name(), "__row_id__");
        assert_eq!(chunks[0].batch.schema().field(1).name(), "region");
        assert_eq!(chunks[0].batch.schema().field(2).name(), "c");
        assert_eq!(chunks[0].batch.schema().field(3).name(), "__agg_state_c");
    }

    #[test]
    fn single_preparation_matches_exact_qualified_group_key_alias() {
        let select_sql = "select f.region, count(*) as c from ice.sales.fact f group by f.region";
        let calls = parse_calls(select_sql);
        let pin =
            RefreshSnapshotPin::from_entries_for_tests(&[("ice.sales.fact", 42, "fact-uuid")]);

        let chunks = prepare_aggregate_first_refresh_chunks(
            select_sql,
            &calls,
            &pin,
            Some("ice"),
            "sales",
            &mut |_, _, state_query| {
                assert!(
                    state_query.to_string().contains("AS `f.region`"),
                    "query={state_query}"
                );
                Ok(AggregateStateRead {
                    result: count_result("f.region", 2),
                    source_layout: count_layout("region"),
                })
            },
        )
        .expect("prepare qualified aggregate first refresh");

        assert_eq!(chunks[0].batch.schema().field(1).name(), "region");
    }

    fn prepare_with_result(result: QueryResult) -> Result<Vec<Chunk>, String> {
        let select_sql = "select region, count(*) as c from ice.sales.fact group by region";
        let calls = parse_calls(select_sql);
        let pin =
            RefreshSnapshotPin::from_entries_for_tests(&[("ice.sales.fact", 42, "fact-uuid")]);
        let mut result = Some(result);
        prepare_aggregate_first_refresh_chunks(
            select_sql,
            &calls,
            &pin,
            Some("ice"),
            "sales",
            &mut |_, _, _| {
                Ok(AggregateStateRead {
                    result: result.take().expect("single read"),
                    source_layout: count_layout("region"),
                })
            },
        )
    }

    #[test]
    fn single_metadata_count_mismatch_fails_fast() {
        let mut result = reordered_count_result();
        result.columns.pop();

        let error = prepare_with_result(result).expect_err("metadata arity must be exact");

        assert!(error.contains("metadata column count mismatch"), "{error}");
    }

    #[test]
    fn single_missing_or_duplicate_metadata_name_fails_fast() {
        for names in [["unexpected", "region"], ["region", "region"]] {
            let mut result = reordered_count_result();
            for (column, name) in result.columns.iter_mut().zip(names) {
                column.name = name.to_string();
            }

            let error = prepare_with_result(result).expect_err("metadata names must be exact");

            assert!(
                error.contains("requires exactly one column named `__agg_state_c`")
                    || error.contains("requires exactly one column named `region`"),
                "{error}"
            );
        }
    }

    #[test]
    fn single_chunk_name_mismatch_fails_fast() {
        let mut result = reordered_count_result();
        let old = result.chunks.pop().expect("chunk");
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("unexpected", DataType::Binary, false),
                Field::new("region", DataType::Utf8, true),
            ])),
            old.batch.columns().to_vec(),
        )
        .expect("mismatched chunk");
        result
            .chunks
            .push(record_batch_to_chunk(batch).expect("chunk"));

        let error = prepare_with_result(result).expect_err("chunk names must be exact");

        assert!(
            error.contains("state result chunk requires exactly one"),
            "{error}"
        );
    }

    #[test]
    fn single_callback_failure_returns_error_without_materialization() {
        let select_sql = "select region, count(*) as c from ice.sales.fact group by region";
        let calls = parse_calls(select_sql);
        let pin =
            RefreshSnapshotPin::from_entries_for_tests(&[("ice.sales.fact", 42, "fact-uuid")]);

        let error = prepare_aggregate_first_refresh_chunks(
            select_sql,
            &calls,
            &pin,
            Some("ice"),
            "sales",
            &mut |_, _, _| Err("query read failed".to_string()),
        )
        .expect_err("callback failure must propagate");

        assert_eq!(error, "query read failed");
    }

    #[test]
    fn branch_aliases_normalize_to_first_layout_and_append_stable_ids() {
        let select_sql = branch_select_sql();
        let first_calls = first_branch_calls(select_sql);
        let pin = branch_pin();
        let mut reads = 0;
        let mut read =
            |visible_sql: &str, _: &AggregateSqlCalls, state_query: sqlparser::ast::Query| {
                let branch = reads;
                reads += 1;
                let source_name = if branch == 0 { "region" } else { "area" };
                let snapshot = if branch == 0 { 41 } else { 42 };
                assert!(visible_sql.contains(source_name), "sql={visible_sql}");
                assert!(
                    state_query
                        .to_string()
                        .contains(&format!("VERSION AS OF {snapshot}")),
                    "query={state_query}"
                );
                Ok(AggregateStateRead {
                    result: count_result(source_name, branch as i64 + 1),
                    source_layout: count_layout(source_name),
                })
            };

        let chunks = prepare_branch_union_aggregate_first_refresh_chunks(
            select_sql,
            2,
            &first_calls,
            &pin,
            Some("ice"),
            "sales",
            &mut read,
        )
        .expect("prepare branch aggregate first refresh");

        assert_eq!(reads, 2);
        assert_eq!(chunks.len(), 2);
        for (branch_id, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.batch.schema().field(1).name(), "region");
            let branch = chunk
                .batch
                .column(chunk.batch.num_columns() - 1)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .expect("branch id");
            assert_eq!(branch.value(0), branch_id as i32);
            assert!(!branch.is_null(0));
        }
    }

    #[test]
    fn branch_requires_union_all_and_exact_branch_count() {
        let first_calls =
            parse_calls("select region, count(*) as c from ice.sales.fact_a group by region");
        let pin = branch_pin();
        let mut read = |_: &str, _: &AggregateSqlCalls, _: sqlparser::ast::Query| {
            panic!("invalid branch shape must fail before reading")
        };

        let error = prepare_branch_union_aggregate_first_refresh_chunks(
            "select region, count(*) as c from ice.sales.fact_a group by region \
             union \
             select area, count(*) as c from ice.sales.fact_b group by area",
            2,
            &first_calls,
            &pin,
            Some("ice"),
            "sales",
            &mut read,
        )
        .expect_err("UNION DISTINCT must fail");
        assert!(error.contains("supports UNION ALL only"), "{error}");

        let error = prepare_branch_union_aggregate_first_refresh_chunks(
            branch_select_sql(),
            3,
            &first_calls,
            &pin,
            Some("ice"),
            "sales",
            &mut read,
        )
        .expect_err("branch count mismatch must fail");
        assert!(error.contains("expected 3 branches, found 2"), "{error}");
    }

    #[test]
    fn branch_rejects_first_branch_call_contract_drift_before_reading() {
        let select_sql = branch_select_sql();
        let wrong_calls =
            parse_calls("select region, sum(amount) as c from ice.sales.fact_a group by region");
        let pin = branch_pin();

        let error = prepare_branch_union_aggregate_first_refresh_chunks(
            select_sql,
            2,
            &wrong_calls,
            &pin,
            Some("ice"),
            "sales",
            &mut |_, _, _| panic!("contract drift must fail before reading"),
        )
        .expect_err("first branch contract drift must fail");

        assert!(error.contains("first branch calls drifted"), "{error}");
    }

    fn assert_branch_layout_mismatch(
        source_calls: &AggregateSqlCalls,
        source_layout: &AggregateMvLayout,
        target_calls: &AggregateSqlCalls,
        target_layout: &AggregateMvLayout,
        dimension: &str,
    ) {
        let error = validate_aggregate_layout_compatibility(
            1,
            source_calls,
            source_layout,
            target_calls,
            target_layout,
        )
        .expect_err("branch layout drift must fail");

        assert_eq!(
            error,
            format!("aggregate MV branch 1 {dimension} mismatch with branch 0")
        );
    }

    #[test]
    fn branch_reports_specific_layout_mismatch_dimensions() {
        let target_calls =
            parse_calls("select region, count(*) as c from ice.sales.fact_a group by region");
        let target_layout = count_layout("region");

        let reordered_calls =
            parse_calls("select count(*) as c, region from ice.sales.fact_b group by region");
        assert_branch_layout_mismatch(
            &reordered_calls,
            &target_layout,
            &target_calls,
            &target_layout,
            "visible output order",
        );

        let sum_calls =
            parse_calls("select region, sum(amount) as c from ice.sales.fact_b group by region");
        assert_branch_layout_mismatch(
            &sum_calls,
            &target_layout,
            &target_calls,
            &target_layout,
            "aggregate 0 function",
        );

        let count_expr_calls =
            parse_calls("select region, count(region) as c from ice.sales.fact_b group by region");
        assert_branch_layout_mismatch(
            &count_expr_calls,
            &target_layout,
            &target_calls,
            &target_layout,
            "aggregate 0 input kind",
        );

        let mut source_layout = target_layout.clone();
        source_layout.visible_columns[0].data_type = DataType::Int64;
        assert_branch_layout_mismatch(
            &target_calls,
            &source_layout,
            &target_calls,
            &target_layout,
            "visible column 0 Arrow type",
        );

        let mut source_layout = target_layout.clone();
        source_layout.visible_columns[0].sql_type = SqlType::BigInt;
        assert_branch_layout_mismatch(
            &target_calls,
            &source_layout,
            &target_calls,
            &target_layout,
            "visible column 0 SQL type",
        );

        let mut source_layout = target_layout.clone();
        source_layout.visible_columns[0].nullable = false;
        assert_branch_layout_mismatch(
            &target_calls,
            &source_layout,
            &target_calls,
            &target_layout,
            "visible column 0 nullability",
        );

        let mut source_layout = target_layout.clone();
        source_layout.state_columns[0].state_role = AggregateStateRole::RetractionCount;
        assert_branch_layout_mismatch(
            &target_calls,
            &source_layout,
            &target_calls,
            &target_layout,
            "state column 0 role",
        );

        let mut source_layout = target_layout.clone();
        source_layout.aggregate_input_types[0] = Some(DataType::Int64);
        assert_branch_layout_mismatch(
            &target_calls,
            &source_layout,
            &target_calls,
            &target_layout,
            "aggregate 0 input type",
        );

        let mut source_layout = target_layout.clone();
        source_layout.physical_columns[1].visible = false;
        assert_branch_layout_mismatch(
            &target_calls,
            &source_layout,
            &target_calls,
            &target_layout,
            "physical column 1 visibility role",
        );
    }

    #[test]
    fn branch_preparation_propagates_specific_layout_mismatch() {
        let select_sql = branch_select_sql();
        let first_calls = first_branch_calls(select_sql);
        let pin = branch_pin();
        let mut reads = 0;

        let error = prepare_branch_union_aggregate_first_refresh_chunks(
            select_sql,
            2,
            &first_calls,
            &pin,
            Some("ice"),
            "sales",
            &mut |_, _, _| {
                let branch = reads;
                reads += 1;
                let source_name = if branch == 0 { "region" } else { "area" };
                let mut layout = count_layout(source_name);
                if branch == 1 {
                    layout.visible_columns[0].nullable = false;
                }
                Ok(AggregateStateRead {
                    result: count_result(source_name, branch as i64 + 1),
                    source_layout: layout,
                })
            },
        )
        .expect_err("layout nullability drift must fail");

        assert_eq!(reads, 2);
        assert_eq!(
            error,
            "aggregate MV branch 1 visible column 0 nullability mismatch with branch 0"
        );
    }

    #[test]
    fn branch_callback_failure_returns_error_without_partial_output() {
        let select_sql = branch_select_sql();
        let first_calls = first_branch_calls(select_sql);
        let pin = branch_pin();
        let mut reads = 0;

        let error = prepare_branch_union_aggregate_first_refresh_chunks(
            select_sql,
            2,
            &first_calls,
            &pin,
            Some("ice"),
            "sales",
            &mut |_, _, _| {
                let branch = reads;
                reads += 1;
                if branch == 1 {
                    return Err("second branch read failed".to_string());
                }
                Ok(AggregateStateRead {
                    result: count_result("region", 1),
                    source_layout: count_layout("region"),
                })
            },
        )
        .expect_err("callback failure must abort the complete preparation");

        assert_eq!(reads, 2);
        assert_eq!(error, "second branch read failed");
    }
}
