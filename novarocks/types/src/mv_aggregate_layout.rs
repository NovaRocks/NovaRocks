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

//! Runtime-only layout for aggregate materialized-view state.
//!
//! This is an immutable in-process contract between planning and execution.
//! It deliberately contains Arrow/runtime facts only; physical DDL and
//! durable contracts remain with their respective owners.

use arrow::datatypes::DataType;

/// Runtime aggregate operation used by aggregate state kernels.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MvAggregateRuntimeKind {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    BoolOr,
    BoolAnd,
    CountDistinct,
    ApproxCountDistinct,
}

/// Identifies a state column's role in an aggregate materialized view.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MvAggregateStateRole {
    /// The one opaque state column for an aggregate.
    Single,
    /// A hidden row-count used to decide whether a group was fully retracted.
    RetractionCount,
}

/// One visible output column in the runtime layout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvAggregateVisibleColumn {
    name: String,
    data_type: DataType,
    nullable: bool,
    source_index: usize,
}

impl MvAggregateVisibleColumn {
    pub fn new(name: String, data_type: DataType, nullable: bool, source_index: usize) -> Self {
        Self {
            name,
            data_type,
            nullable,
            source_index,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }

    pub fn source_index(&self) -> usize {
        self.source_index
    }
}

/// One hidden aggregate state column in the runtime layout.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvAggregateStateColumn {
    name: String,
    data_type: DataType,
    nullable: bool,
    visible_source_index: usize,
    aggregate_index: usize,
    aggregate_kind: MvAggregateRuntimeKind,
    state_role: MvAggregateStateRole,
    count_star: bool,
}

impl MvAggregateStateColumn {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: String,
        data_type: DataType,
        nullable: bool,
        visible_source_index: usize,
        aggregate_index: usize,
        aggregate_kind: MvAggregateRuntimeKind,
        state_role: MvAggregateStateRole,
        count_star: bool,
    ) -> Self {
        Self {
            name,
            data_type,
            nullable,
            visible_source_index,
            aggregate_index,
            aggregate_kind,
            state_role,
            count_star,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }

    pub fn visible_source_index(&self) -> usize {
        self.visible_source_index
    }

    pub fn aggregate_index(&self) -> usize {
        self.aggregate_index
    }

    pub fn aggregate_kind(&self) -> MvAggregateRuntimeKind {
        self.aggregate_kind
    }

    pub fn state_role(&self) -> MvAggregateStateRole {
        self.state_role
    }

    pub fn count_star(&self) -> bool {
        self.count_star
    }
}

/// Neutral ordering of the visible values in a state-shaped result batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MvAggregateVisibleOutput {
    GroupKey(usize),
    Aggregate(usize),
}

/// Immutable Arrow/runtime facts for aggregate materialized-view state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvAggregateRuntimeLayout {
    visible_columns: Vec<MvAggregateVisibleColumn>,
    state_columns: Vec<MvAggregateStateColumn>,
    aggregate_input_types: Vec<Option<DataType>>,
    group_key_source_indexes: Vec<usize>,
    visible_output_order: Vec<MvAggregateVisibleOutput>,
}

impl MvAggregateRuntimeLayout {
    pub fn try_new(
        visible_columns: Vec<MvAggregateVisibleColumn>,
        state_columns: Vec<MvAggregateStateColumn>,
        aggregate_input_types: Vec<Option<DataType>>,
        group_key_source_indexes: Vec<usize>,
    ) -> Result<Self, String> {
        for (index, column) in visible_columns.iter().enumerate() {
            if column.source_index != index {
                return Err(format!(
                    "aggregate MV visible column source index mismatch: position={index} source_index={}",
                    column.source_index
                ));
            }
        }

        let mut visible_output_order = vec![None; visible_columns.len()];
        for (group_key_index, &source_index) in group_key_source_indexes.iter().enumerate() {
            let slot = visible_output_order.get_mut(source_index).ok_or_else(|| {
                format!(
                    "aggregate MV group key visible source index out of range: group_key_index={group_key_index} source_index={source_index} outputs={}",
                    visible_columns.len()
                )
            })?;
            if slot
                .replace(MvAggregateVisibleOutput::GroupKey(group_key_index))
                .is_some()
            {
                return Err(format!(
                    "aggregate MV visible output is duplicated: group_key_index={group_key_index} source_index={source_index}"
                ));
            }
        }

        let aggregate_count = aggregate_input_types.len();
        let mut single_aggregate_indexes = vec![false; aggregate_count];
        let mut retraction_count_columns = 0usize;
        for state_column in &state_columns {
            match state_column.state_role {
                MvAggregateStateRole::Single => {
                    if state_column.aggregate_index >= aggregate_count {
                        return Err(format!(
                            "aggregate MV state aggregate index out of range: aggregate_index={} aggregates={aggregate_count}",
                            state_column.aggregate_index
                        ));
                    }
                    if !is_varbinary_data_type(&state_column.data_type) {
                        return Err(format!(
                            "aggregate MV Single state column must be Binary or LargeBinary: column={} data_type={:?}",
                            state_column.name, state_column.data_type
                        ));
                    }
                    if state_column.nullable {
                        return Err(format!(
                            "aggregate MV state column must be non-nullable: column={}",
                            state_column.name
                        ));
                    }
                    if std::mem::replace(
                        &mut single_aggregate_indexes[state_column.aggregate_index],
                        true,
                    ) {
                        return Err(format!(
                            "aggregate MV state aggregate index is duplicated: aggregate_index={}",
                            state_column.aggregate_index
                        ));
                    }
                    let slot = visible_output_order
                        .get_mut(state_column.visible_source_index)
                        .ok_or_else(|| {
                            format!(
                                "aggregate MV visible source index out of range: aggregate_index={} source_index={}",
                                state_column.aggregate_index, state_column.visible_source_index
                            )
                        })?;
                    if slot
                        .replace(MvAggregateVisibleOutput::Aggregate(
                            state_column.aggregate_index,
                        ))
                        .is_some()
                    {
                        return Err(format!(
                            "aggregate MV visible output is duplicated: aggregate_index={} source_index={}",
                            state_column.aggregate_index, state_column.visible_source_index
                        ));
                    }
                }
                MvAggregateStateRole::RetractionCount => {
                    retraction_count_columns += 1;
                    if retraction_count_columns > 1 {
                        return Err(
                            "aggregate MV layout has more than one retraction count state column"
                                .to_string(),
                        );
                    }
                    if state_column.visible_source_index >= visible_columns.len() {
                        return Err(format!(
                            "aggregate MV retraction count visible source index out of range: source_index={} outputs={}",
                            state_column.visible_source_index,
                            visible_columns.len()
                        ));
                    }
                    if state_column.aggregate_kind != MvAggregateRuntimeKind::Count
                        || !state_column.count_star
                        || state_column.aggregate_index != aggregate_count
                        || state_column.data_type != DataType::Int64
                        || state_column.nullable
                    {
                        return Err(format!(
                            "aggregate MV retraction count state has invalid runtime shape: column={}",
                            state_column.name
                        ));
                    }
                }
            }
        }

        if let Some(missing_index) = single_aggregate_indexes.iter().position(|present| !present) {
            return Err(format!(
                "aggregate MV state column is missing: aggregate_index={missing_index}"
            ));
        }

        let visible_output_order = visible_output_order
            .into_iter()
            .enumerate()
            .map(|(position, output)| {
                output.ok_or_else(|| {
                    format!("aggregate MV visible output is missing: source_index={position}")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            visible_columns,
            state_columns,
            aggregate_input_types,
            group_key_source_indexes,
            visible_output_order,
        })
    }

    pub fn visible_columns(&self) -> &[MvAggregateVisibleColumn] {
        &self.visible_columns
    }

    pub fn state_columns(&self) -> &[MvAggregateStateColumn] {
        &self.state_columns
    }

    pub fn aggregate_input_types(&self) -> &[Option<DataType>] {
        &self.aggregate_input_types
    }

    pub fn group_key_source_indexes(&self) -> &[usize] {
        &self.group_key_source_indexes
    }

    pub fn visible_output_order(&self) -> &[MvAggregateVisibleOutput] {
        &self.visible_output_order
    }
}

fn is_varbinary_data_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Binary | DataType::LargeBinary)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn visible(name: &str, source_index: usize) -> MvAggregateVisibleColumn {
        MvAggregateVisibleColumn::new(name.to_string(), DataType::Int64, false, source_index)
    }

    fn state(aggregate_index: usize, visible_source_index: usize) -> MvAggregateStateColumn {
        MvAggregateStateColumn::new(
            format!("state_{aggregate_index}"),
            DataType::LargeBinary,
            false,
            visible_source_index,
            aggregate_index,
            MvAggregateRuntimeKind::Sum,
            MvAggregateStateRole::Single,
            false,
        )
    }

    #[test]
    fn layout_derives_neutral_visible_output_order() {
        let layout = MvAggregateRuntimeLayout::try_new(
            vec![
                visible("sum_v", 0),
                visible("region", 1),
                visible("count_v", 2),
            ],
            vec![state(0, 0), state(1, 2)],
            vec![Some(DataType::Int64), Some(DataType::Int64)],
            vec![1],
        )
        .expect("valid runtime layout");

        assert_eq!(
            layout.visible_output_order(),
            [
                MvAggregateVisibleOutput::Aggregate(0),
                MvAggregateVisibleOutput::GroupKey(0),
                MvAggregateVisibleOutput::Aggregate(1),
            ]
        );
    }

    #[test]
    fn layout_rejects_duplicate_visible_output() {
        let error = MvAggregateRuntimeLayout::try_new(
            vec![visible("region", 0), visible("sum_v", 1)],
            vec![state(0, 0)],
            vec![Some(DataType::Int64)],
            vec![0],
        )
        .expect_err("duplicate output must fail");

        assert_eq!(
            error,
            "aggregate MV visible output is duplicated: aggregate_index=0 source_index=0"
        );
    }

    #[test]
    fn layout_rejects_invalid_retraction_count_shape() {
        let invalid_retraction = MvAggregateStateColumn::new(
            "row_count".to_string(),
            DataType::Int32,
            false,
            0,
            1,
            MvAggregateRuntimeKind::Count,
            MvAggregateStateRole::RetractionCount,
            true,
        );
        let error = MvAggregateRuntimeLayout::try_new(
            vec![visible("count_v", 0)],
            vec![state(0, 0), invalid_retraction],
            vec![None],
            vec![],
        )
        .expect_err("invalid retraction shape must fail");

        assert_eq!(
            error,
            "aggregate MV retraction count state has invalid runtime shape: column=row_count"
        );
    }
}
