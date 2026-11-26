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

//! SQL-owned physical layout for aggregate materialized-view state.
//!
//! This module turns SQL's one-shot analyzed aggregate facts into the target
//! table's physical columns and the execution-only runtime layout.  It keeps
//! semantic table columns and aggregate-function vocabulary in SQL;
//! the embedded [`MvAggregateRuntimeLayout`] contains complete value types and runtime facts.

use std::collections::HashSet;

use arrow::datatypes::DataType;
use novarocks_type_contract::FunctionValueType;
use novarocks_types::mv_aggregate_layout::{
    MvAggregateRuntimeKind, MvAggregateRuntimeLayout, MvAggregateStateColumn, MvAggregateStateRole,
    MvAggregateVisibleColumn,
};
use novarocks_types::naming::normalize_identifier;
use novarocks_types::schema::SqlType;

use crate::mv_refresh::AggregateFunctionKind;
use crate::planning::mv::SqlMvAggregateLayoutFacts;
use crate::semantic::TableColumnDef;

pub const MV_AGGREGATE_ROW_ID_COLUMN: &str = "__row_id__";
pub const MV_AGGREGATE_STATE_PREFIX: &str = "__agg_state_";
pub const MV_AGGREGATE_RETRACTION_COUNT_STATE_COLUMN: &str = "__agg_state___ivm_row_count";

/// One target-table column emitted by the aggregate MV physical-layout builder.
#[derive(Clone, Debug, PartialEq)]
pub struct SqlMvAggregatePhysicalColumn {
    column: TableColumnDef,
    visible: bool,
    is_key: bool,
}

impl SqlMvAggregatePhysicalColumn {
    pub fn new(column: TableColumnDef, visible: bool, is_key: bool) -> Self {
        Self {
            column,
            visible,
            is_key,
        }
    }

    pub fn column(&self) -> &TableColumnDef {
        &self.column
    }

    pub fn visible(&self) -> bool {
        self.visible
    }

    pub fn is_key(&self) -> bool {
        self.is_key
    }
}

/// SQL-owned target DDL together with its execution-only aggregate layout.
#[derive(Clone, Debug, PartialEq)]
pub struct SqlMvAggregatePhysicalLayout {
    row_id_column: SqlMvAggregatePhysicalColumn,
    physical_columns: Vec<SqlMvAggregatePhysicalColumn>,
    runtime_layout: MvAggregateRuntimeLayout,
}

impl SqlMvAggregatePhysicalLayout {
    pub fn row_id_column(&self) -> &SqlMvAggregatePhysicalColumn {
        &self.row_id_column
    }

    pub fn physical_columns(&self) -> &[SqlMvAggregatePhysicalColumn] {
        &self.physical_columns
    }

    pub fn runtime_layout(&self) -> &MvAggregateRuntimeLayout {
        &self.runtime_layout
    }
}

/// Build the aggregate MV target layout in one SQL-owned transaction.
///
/// Validation intentionally follows the historical builder order: input-count,
/// group-key indexes, visible type mapping, each aggregate's visible type, and
/// then state-column validation.  The hidden retraction-count state is appended
/// only after every explicit aggregate state has been accepted.
pub fn build_sql_mv_aggregate_physical_layout(
    facts: &SqlMvAggregateLayoutFacts,
) -> Result<SqlMvAggregatePhysicalLayout, String> {
    let calls = facts.calls();
    let output_columns = facts.output_columns();
    let aggregate_input_types = facts.aggregate_input_types();
    let group_key_source_indexes = facts.group_key_source_indexes();

    if aggregate_input_types.len() != calls.len() {
        return Err(format!(
            "aggregate MV input type metadata count mismatch: inputs={} aggregates={}",
            aggregate_input_types.len(),
            calls.len()
        ));
    }

    let row_id_column = physical_column(
        MV_AGGREGATE_ROW_ID_COLUMN.to_string(),
        SqlType::String,
        false,
        false,
        true,
    );
    let mut physical_columns = vec![row_id_column.clone()];
    for (group_key_index, source_index) in group_key_source_indexes.iter().enumerate() {
        if *source_index >= output_columns.len() {
            return Err(format!(
                "aggregate MV group key visible source index out of range: group_key_index={group_key_index} source_index={source_index} outputs={}",
                output_columns.len()
            ));
        }
    }

    let visible_columns = output_columns
        .iter()
        .enumerate()
        .map(|(source_index, column)| {
            let sql_type = mv_value_type_to_sql_type(&column.value_type)?;
            physical_columns.push(physical_column(
                column.name.clone(),
                sql_type,
                column.value_type.nullable(),
                true,
                false,
            ));
            Ok(MvAggregateVisibleColumn::new(
                column.name.clone(),
                column.value_type.clone(),
                source_index,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;

    let avg_count = calls
        .iter()
        .filter(|call| call.function() == AggregateFunctionKind::Avg)
        .count();
    let mut state_columns = Vec::with_capacity(calls.len() + avg_count + 1);
    for (aggregate_index, call) in calls.iter().enumerate() {
        let visible_source_index = call.visible_source_index();
        let visible = output_columns.get(visible_source_index).ok_or_else(|| {
            format!(
                "aggregate MV visible source index out of range: aggregate_index={aggregate_index} source_index={visible_source_index}"
            )
        })?;
        validate_aggregate_state_visible_type(
            call.function(),
            visible.value_type.data_type(),
            aggregate_input_types
                .get(aggregate_index)
                .and_then(Option::as_ref),
            call.output_name(),
        )?;

        let state_name = format!(
            "{MV_AGGREGATE_STATE_PREFIX}{}",
            sanitize_state_column_name(call.output_name())
        );
        let state_data_type = DataType::LargeBinary;
        if call.function() == AggregateFunctionKind::Avg {
            for (suffix, role) in [
                ("avg_sum", MvAggregateStateRole::AvgSum),
                ("avg_count", MvAggregateStateRole::AvgCount),
            ] {
                let state_name = format!("{state_name}_{suffix}");
                validate_state_column_type(call.function(), role, &state_data_type, &state_name)?;
                physical_columns.push(physical_column(
                    state_name.clone(),
                    SqlType::Binary,
                    false,
                    false,
                    false,
                ));
                state_columns.push(MvAggregateStateColumn::new(
                    state_name,
                    FunctionValueType::new(state_data_type.clone(), false),
                    visible_source_index,
                    aggregate_index,
                    runtime_kind(call.function()),
                    role,
                    false,
                ));
            }
        } else {
            validate_state_column_type(
                call.function(),
                MvAggregateStateRole::Single,
                &state_data_type,
                &state_name,
            )?;
            physical_columns.push(physical_column(
                state_name.clone(),
                SqlType::Binary,
                false,
                false,
                false,
            ));
            state_columns.push(MvAggregateStateColumn::new(
                state_name,
                FunctionValueType::new(state_data_type, false),
                visible_source_index,
                aggregate_index,
                runtime_kind(call.function()),
                MvAggregateStateRole::Single,
                call.count_star(),
            ));
        }
    }

    if !calls
        .iter()
        .any(|call| call.function() == AggregateFunctionKind::Count && call.count_star())
    {
        validate_state_column_type(
            AggregateFunctionKind::Count,
            MvAggregateStateRole::RetractionCount,
            &DataType::Int64,
            MV_AGGREGATE_RETRACTION_COUNT_STATE_COLUMN,
        )?;
        physical_columns.push(physical_column(
            MV_AGGREGATE_RETRACTION_COUNT_STATE_COLUMN.to_string(),
            SqlType::BigInt,
            false,
            false,
            false,
        ));
        state_columns.push(MvAggregateStateColumn::new(
            MV_AGGREGATE_RETRACTION_COUNT_STATE_COLUMN.to_string(),
            FunctionValueType::new(DataType::Int64, false),
            0,
            calls.len(),
            MvAggregateRuntimeKind::Count,
            MvAggregateStateRole::RetractionCount,
            true,
        ));
    }

    let runtime_layout = MvAggregateRuntimeLayout::try_new(
        MV_AGGREGATE_ROW_ID_COLUMN.to_string(),
        visible_columns,
        state_columns,
        aggregate_input_types.to_vec(),
        group_key_source_indexes.to_vec(),
    )?;
    Ok(SqlMvAggregatePhysicalLayout {
        row_id_column,
        physical_columns,
        runtime_layout,
    })
}

/// Project the complete source domain through the shared schema owner.
/// The runtime layout retains the original full value type independently of
/// this SQL DDL declaration.
pub fn mv_value_type_to_sql_type(value_type: &FunctionValueType) -> Result<SqlType, String> {
    novarocks_types::sql_type_from_value_type(value_type).map_err(|error| match error {
        novarocks_types::ColumnValueTypeError::UnrepresentableCarrier(carrier) => {
            format!("unsupported MV output type: {carrier}")
        }
        other => other.to_string(),
    })
}

/// Reject duplicate physical names using StarRocks identifier normalization.
pub fn validate_unique_aggregate_physical_column_names(
    physical_columns: &[SqlMvAggregatePhysicalColumn],
) -> Result<(), String> {
    let mut names = HashSet::with_capacity(physical_columns.len());
    for column in physical_columns {
        let normalized = normalize_identifier(&column.column.name)?;
        if !names.insert(normalized.clone()) {
            return Err(format!(
                "aggregate MV physical column name collision: hidden column name collision or duplicate physical column `{normalized}`"
            ));
        }
    }
    Ok(())
}

fn physical_column(
    name: String,
    data_type: SqlType,
    nullable: bool,
    visible: bool,
    is_key: bool,
) -> SqlMvAggregatePhysicalColumn {
    SqlMvAggregatePhysicalColumn::new(
        TableColumnDef {
            name,
            data_type,
            nullable,
            default: None,
        },
        visible,
        is_key,
    )
}

fn runtime_kind(function: AggregateFunctionKind) -> MvAggregateRuntimeKind {
    match function {
        AggregateFunctionKind::Count => MvAggregateRuntimeKind::Count,
        AggregateFunctionKind::Sum => MvAggregateRuntimeKind::Sum,
        AggregateFunctionKind::Avg => MvAggregateRuntimeKind::Avg,
        AggregateFunctionKind::Min => MvAggregateRuntimeKind::Min,
        AggregateFunctionKind::Max => MvAggregateRuntimeKind::Max,
        AggregateFunctionKind::BoolOr => MvAggregateRuntimeKind::BoolOr,
        AggregateFunctionKind::BoolAnd => MvAggregateRuntimeKind::BoolAnd,
        AggregateFunctionKind::CountDistinct => MvAggregateRuntimeKind::CountDistinct,
        AggregateFunctionKind::ApproxCountDistinct => MvAggregateRuntimeKind::ApproxCountDistinct,
    }
}

fn validate_state_column_type(
    function: AggregateFunctionKind,
    state_role: MvAggregateStateRole,
    data_type: &DataType,
    state_name: &str,
) -> Result<(), String> {
    match state_role {
        MvAggregateStateRole::RetractionCount => match data_type {
            DataType::Int64 => Ok(()),
            other => Err(format!(
                "aggregate MV retraction count state column `{state_name}` must be BIGINT, got {other:?}"
            )),
        },
        MvAggregateStateRole::Single => match data_type {
            DataType::Binary | DataType::LargeBinary => Ok(()),
            other => Err(format!(
                "expected VARBINARY state column type for `{state_name}` ({function:?}), got: {other:?}"
            )),
        },
        MvAggregateStateRole::AvgSum | MvAggregateStateRole::AvgCount => match data_type {
            DataType::Binary | DataType::LargeBinary => Ok(()),
            other => Err(format!(
                "expected VARBINARY AVG state column type for `{state_name}` ({function:?}), got: {other:?}"
            )),
        },
    }
}

fn validate_aggregate_state_visible_type(
    function: AggregateFunctionKind,
    visible_data_type: &DataType,
    input_data_type: Option<&DataType>,
    output_name: &str,
) -> Result<(), String> {
    match (function, visible_data_type) {
        (AggregateFunctionKind::Sum, DataType::Float32 | DataType::Float64) => Err(format!(
            "SUM state type is unsupported for aggregate `{output_name}` output: {visible_data_type:?}; FLOAT/DOUBLE inputs are not supported by SUM state"
        )),
        (AggregateFunctionKind::Avg, DataType::Decimal128(_, _)) => match input_data_type {
            Some(DataType::Decimal128(_, _)) => Ok(()),
            Some(other) => Err(format!(
                "AVG state type is unsupported for aggregate `{output_name}` input: {other:?}; DECIMAL AVG requires Decimal128 input scale metadata"
            )),
            None => Err(format!(
                "AVG state type is unsupported for aggregate `{output_name}` output: {visible_data_type:?}; DECIMAL AVG requires input scale metadata"
            )),
        },
        _ => Ok(()),
    }
}

fn sanitize_state_column_name(name: &str) -> String {
    let sanitized = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "agg".to_string()
    } else {
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{Field, Fields, TimeUnit};

    use super::*;

    #[test]
    fn mv_ddl_uses_authored_domains_instead_of_carrier_guesses() {
        use novarocks_type_contract::ValueLogicalType;
        let cases = [
            (DataType::LargeBinary, ValueLogicalType::Json, SqlType::Json),
            (
                DataType::LargeBinary,
                ValueLogicalType::Variant,
                SqlType::Variant,
            ),
            (DataType::Binary, ValueLogicalType::Hll, SqlType::Hll),
            (DataType::Binary, ValueLogicalType::Bitmap, SqlType::Bitmap),
            (
                DataType::FixedSizeBinary(16),
                ValueLogicalType::LargeInt,
                SqlType::LargeInt,
            ),
            (
                DataType::FixedSizeBinary(16),
                ValueLogicalType::Physical,
                SqlType::Binary,
            ),
        ];
        for (data_type, logical_type, expected) in cases {
            assert_eq!(
                mv_value_type_to_sql_type(
                    &FunctionValueType::try_from_legacy_declared_projection(
                        data_type,
                        true,
                        logical_type
                    )
                    .expect("valid temporary legacy value projection")
                )
                .unwrap(),
                expected
            );
        }
        assert_eq!(
            mv_value_type_to_sql_type(&crate::test_value_types::string_list(true, true, false))
                .unwrap(),
            SqlType::Array(Box::new(SqlType::Json)),
        );
    }

    #[test]
    fn mv_value_mapper_preserves_nested_shape_and_diagnostic_contract() {
        let map_entries = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("value", DataType::Int64, true)),
        ]));
        assert_eq!(
            mv_value_type_to_sql_type(&FunctionValueType::new(
                DataType::Map(Arc::new(Field::new("entries", map_entries, false)), false,),
                false
            ))
            .expect("MV map type"),
            SqlType::Map(Box::new(SqlType::String), Box::new(SqlType::BigInt))
        );
        assert_eq!(
            mv_value_type_to_sql_type(&FunctionValueType::new(DataType::Null, true)),
            Err("unsupported MV output type: Null".to_string())
        );
    }

    #[test]
    fn mv_value_mapper_preserves_scalar_contract() {
        let cases = [
            (DataType::Boolean, SqlType::Boolean),
            (DataType::Int8, SqlType::TinyInt),
            (DataType::Int16, SqlType::SmallInt),
            (DataType::Int32, SqlType::Int),
            (DataType::Int64, SqlType::BigInt),
            (
                DataType::FixedSizeBinary(novarocks_types::largeint::LARGEINT_BYTE_WIDTH),
                SqlType::Binary,
            ),
            (
                DataType::Decimal128(38, -2),
                SqlType::Decimal {
                    precision: 38,
                    scale: -2,
                },
            ),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                SqlType::DateTimeNs,
            ),
        ];

        for (arrow_type, expected) in cases {
            assert_eq!(
                mv_value_type_to_sql_type(&FunctionValueType::new(arrow_type.clone(), false))
                    .expect("supported scalar type"),
                expected,
                "Arrow type {arrow_type:?}"
            );
        }
    }

    #[test]
    fn mv_value_mapper_preserves_nested_shape_and_order() {
        let map_entries = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("value", DataType::Decimal128(20, -3), true)),
        ]));
        let nested = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            )),
            Arc::new(Field::new(
                "attrs",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Map(Arc::new(Field::new("entries", map_entries, false)), false),
                    true,
                ))),
                true,
            )),
        ]));

        assert_eq!(
            mv_value_type_to_sql_type(&FunctionValueType::new(nested, false))
                .expect("supported nested type"),
            SqlType::Struct(vec![
                ("ts".to_string(), SqlType::DateTimeNs),
                (
                    "attrs".to_string(),
                    SqlType::Array(Box::new(SqlType::Map(
                        Box::new(SqlType::String),
                        Box::new(SqlType::Decimal {
                            precision: 20,
                            scale: -3,
                        }),
                    ))),
                ),
            ])
        );
    }

    #[test]
    fn physical_column_validator_keeps_normalized_collision_error() {
        let columns = vec![
            physical_column(
                "Visible_Output".to_string(),
                SqlType::BigInt,
                false,
                true,
                false,
            ),
            physical_column(
                "`visible_output`".to_string(),
                SqlType::BigInt,
                false,
                true,
                false,
            ),
        ];
        assert_eq!(
            validate_unique_aggregate_physical_column_names(&columns),
            Err("aggregate MV physical column name collision: hidden column name collision or duplicate physical column `visible_output`".to_string())
        );
    }
}
