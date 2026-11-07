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

use std::collections::HashSet;

use arrow::datatypes::DataType;

use crate::analysis::OutputColumn;
use crate::column_id::ColumnId;

#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum JoinRefreshMode {
    Full,
    AppendOnly,
    Coalesce,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinRefreshBranchSide {
    LeftDeltaRightSnapshot,
    LeftSnapshotRightDelta,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JoinRefreshBranchDescriptor {
    pub side: JoinRefreshBranchSide,
    pub action_column_id: ColumnId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JoinRefreshMvIdentity {
    pub catalog: String,
    pub database: String,
    pub name: String,
}

#[derive(Clone, Debug)]
pub(crate) struct JoinRefreshJoinKeyPair {
    pub left_column: OutputColumn,
    pub right_column: OutputColumn,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum JoinRefreshOutputSource {
    Payload(ColumnId),
    Action(ColumnId),
    JoinApplyKey(ColumnId),
}

#[derive(Clone, Debug)]
pub(crate) struct JoinRefreshOutputMapping {
    pub mv_output_column: OutputColumn,
    pub source: JoinRefreshOutputSource,
}

#[derive(Clone, Debug)]
pub(crate) struct JoinRefreshDescriptor {
    pub mode: JoinRefreshMode,
    pub mv_identity: JoinRefreshMvIdentity,
    pub left_occurrence_id: crate::compiler::SqlMvRelationOccurrenceId,
    pub right_occurrence_id: crate::compiler::SqlMvRelationOccurrenceId,
    pub left_base_fqn: String,
    pub right_base_fqn: String,
    pub left_row_id_column: OutputColumn,
    pub right_row_id_column: OutputColumn,
    pub action_column: OutputColumn,
    pub join_apply_key_column: OutputColumn,
    pub payload_columns: Vec<OutputColumn>,
    pub join_key_pairs: Vec<JoinRefreshJoinKeyPair>,
    pub output_mappings: Vec<JoinRefreshOutputMapping>,
    pub branches: Vec<JoinRefreshBranchDescriptor>,
    pub needs_target_locator: bool,
}

impl PartialEq for JoinRefreshJoinKeyPair {
    fn eq(&self, other: &Self) -> bool {
        output_column_eq(&self.left_column, &other.left_column)
            && output_column_eq(&self.right_column, &other.right_column)
    }
}

impl Eq for JoinRefreshJoinKeyPair {}

impl PartialEq for JoinRefreshOutputMapping {
    fn eq(&self, other: &Self) -> bool {
        output_column_eq(&self.mv_output_column, &other.mv_output_column)
            && self.source == other.source
    }
}

impl Eq for JoinRefreshOutputMapping {}

impl PartialEq for JoinRefreshDescriptor {
    fn eq(&self, other: &Self) -> bool {
        self.mode == other.mode
            && self.mv_identity == other.mv_identity
            && self.left_occurrence_id == other.left_occurrence_id
            && self.right_occurrence_id == other.right_occurrence_id
            && self.left_base_fqn == other.left_base_fqn
            && self.right_base_fqn == other.right_base_fqn
            && output_column_eq(&self.left_row_id_column, &other.left_row_id_column)
            && output_column_eq(&self.right_row_id_column, &other.right_row_id_column)
            && output_column_eq(&self.action_column, &other.action_column)
            && output_column_eq(&self.join_apply_key_column, &other.join_apply_key_column)
            && output_columns_eq(&self.payload_columns, &other.payload_columns)
            && self.join_key_pairs == other.join_key_pairs
            && self.output_mappings == other.output_mappings
            && self.branches == other.branches
            && self.needs_target_locator == other.needs_target_locator
    }
}

impl Eq for JoinRefreshDescriptor {}

impl JoinRefreshDescriptor {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.left_base_fqn.trim().is_empty() {
            return Err("join refresh descriptor requires left base FQN".to_string());
        }
        if self.right_base_fqn.trim().is_empty() {
            return Err("join refresh descriptor requires right base FQN".to_string());
        }
        if self.left_occurrence_id == self.right_occurrence_id {
            return Err(
                "join refresh descriptor requires distinct left and right bases".to_string(),
            );
        }
        if self.payload_columns.is_empty() {
            return Err("join refresh descriptor requires at least one payload column".to_string());
        }
        self.validate_identity()?;
        validate_row_id_column("left", &self.left_row_id_column)?;
        validate_row_id_column("right", &self.right_row_id_column)?;
        if self.join_key_pairs.is_empty() {
            return Err("join refresh descriptor requires at least one join key pair".to_string());
        }
        self.validate_join_key_pairs()?;
        self.validate_action_column()?;
        self.validate_join_apply_key_column()?;
        self.validate_output_mappings()?;
        if matches!(self.mode, JoinRefreshMode::Coalesce) && !self.needs_target_locator {
            return Err("coalescing join refresh requires target locator".to_string());
        }
        self.validate_branches()?;
        Ok(())
    }

    fn validate_identity(&self) -> Result<(), String> {
        if self.mv_identity.name.trim().is_empty() {
            return Err("join refresh descriptor requires MV name".to_string());
        }
        if self.mv_identity.catalog.trim().is_empty() {
            return Err("join refresh descriptor requires MV catalog".to_string());
        }
        if self.mv_identity.database.trim().is_empty() {
            return Err("join refresh descriptor requires MV database".to_string());
        }
        Ok(())
    }

    fn validate_join_key_pairs(&self) -> Result<(), String> {
        for pair in &self.join_key_pairs {
            if pair.left_column.column_id == pair.right_column.column_id {
                return Err(format!(
                    "join refresh descriptor join key pair cannot use the same column id on both sides: {}",
                    pair.left_column.column_id
                ));
            }
            if !pair
                .left_column
                .value_type
                .same_physical_binding_ignoring_root_nullability_observed(
                    &pair.right_column.value_type,
                    || Ok::<(), std::convert::Infallible>(()),
                )
                .expect("infallible checked join key comparison")
            {
                return Err(format!(
                    "join refresh descriptor join key pair type mismatch: left {} is {:?}, right {} is {:?}",
                    pair.left_column.column_id,
                    pair.left_column.value_type.data_type(),
                    pair.right_column.column_id,
                    pair.right_column.value_type.data_type()
                ));
            }
        }
        Ok(())
    }

    fn validate_action_column(&self) -> Result<(), String> {
        if !self
            .action_column
            .name
            .eq_ignore_ascii_case(crate::common::CHANGE_OP_COLUMN)
            || *self.action_column.value_type.data_type() != DataType::Int8
            || self.action_column.value_type.nullable()
            || !self.action_column.is_internal
        {
            return Err("join refresh descriptor has invalid action column".to_string());
        }
        Ok(())
    }

    fn validate_join_apply_key_column(&self) -> Result<(), String> {
        if !self
            .join_apply_key_column
            .name
            .eq_ignore_ascii_case(crate::planner::vocabulary::JOIN_APPLY_KEY_COLUMN_NAME)
            || *self.join_apply_key_column.value_type.data_type() != DataType::Utf8
            || !self.join_apply_key_column.is_internal
        {
            return Err("join refresh descriptor has invalid join apply-key column".to_string());
        }
        Ok(())
    }

    fn validate_output_mappings(&self) -> Result<(), String> {
        if self.output_mappings.is_empty() {
            return Err("join refresh descriptor requires at least one output mapping".to_string());
        }

        let mut seen_mv_output_ids = HashSet::new();
        let mut seen_mv_output_names = HashSet::new();
        let mut mapped_payload_columns = HashSet::new();
        let mut has_action_mapping = false;
        let mut has_join_apply_key_mapping = false;

        for mapping in &self.output_mappings {
            if !seen_mv_output_ids.insert(mapping.mv_output_column.column_id) {
                return Err(format!(
                    "join refresh descriptor has duplicate MV output column id {}",
                    mapping.mv_output_column.column_id
                ));
            }

            let normalized_name = mapping.mv_output_column.name.to_ascii_lowercase();
            if !seen_mv_output_names.insert(normalized_name) {
                return Err(format!(
                    "join refresh descriptor has duplicate MV output column name {}",
                    mapping.mv_output_column.name
                ));
            }

            let source_column = match mapping.source {
                JoinRefreshOutputSource::Payload(column_id) => {
                    let Some(column) = self
                        .payload_columns
                        .iter()
                        .find(|column| column.column_id == column_id)
                    else {
                        return Err(format!(
                            "join refresh descriptor output mapping references unknown payload column {column_id}"
                        ));
                    };
                    mapped_payload_columns.insert(column_id);
                    column
                }
                JoinRefreshOutputSource::Action(column_id) => {
                    if column_id != self.action_column.column_id {
                        return Err(format!(
                            "join refresh descriptor output mapping references unknown action column {column_id}"
                        ));
                    }
                    has_action_mapping = true;
                    &self.action_column
                }
                JoinRefreshOutputSource::JoinApplyKey(column_id) => {
                    if column_id != self.join_apply_key_column.column_id {
                        return Err(format!(
                            "join refresh descriptor output mapping references unknown join apply-key column {column_id}"
                        ));
                    }
                    has_join_apply_key_mapping = true;
                    &self.join_apply_key_column
                }
            };

            if !output_column_shape_matches(&mapping.mv_output_column, source_column) {
                return Err(format!(
                    "join refresh descriptor output mapping for MV output column {} does not match source column {}",
                    mapping.mv_output_column.column_id, source_column.column_id
                ));
            }
        }

        for column in &self.payload_columns {
            if !mapped_payload_columns.contains(&column.column_id) {
                return Err(format!(
                    "join refresh descriptor missing output mapping for payload column {}",
                    column.column_id
                ));
            }
        }
        if !has_action_mapping {
            return Err(format!(
                "join refresh descriptor missing output mapping for action column {}",
                self.action_column.column_id
            ));
        }
        if !has_join_apply_key_mapping {
            return Err(format!(
                "join refresh descriptor missing output mapping for join apply-key column {}",
                self.join_apply_key_column.column_id
            ));
        }

        Ok(())
    }

    fn validate_branches(&self) -> Result<(), String> {
        if matches!(self.mode, JoinRefreshMode::Full) {
            if !self.branches.is_empty() {
                return Err(
                    "full join refresh descriptor must not carry delta branches".to_string()
                );
            }
            return Ok(());
        }

        if self.branches.is_empty() {
            return Err("join refresh descriptor requires at least one branch".to_string());
        }

        for branch in &self.branches {
            if branch.action_column_id != self.action_column.column_id {
                return Err(format!(
                    "join refresh descriptor branch action column id {} does not match action column id {}",
                    branch.action_column_id, self.action_column.column_id
                ));
            }
        }

        if matches!(self.mode, JoinRefreshMode::Coalesce) {
            let left_delta_count = self
                .branches
                .iter()
                .filter(|branch| branch.side == JoinRefreshBranchSide::LeftDeltaRightSnapshot)
                .count();
            if left_delta_count != 1 {
                return Err(format!(
                    "coalescing join refresh requires exactly one LeftDeltaRightSnapshot branch, found {left_delta_count}"
                ));
            }

            let right_delta_count = self
                .branches
                .iter()
                .filter(|branch| branch.side == JoinRefreshBranchSide::LeftSnapshotRightDelta)
                .count();
            if right_delta_count != 1 {
                return Err(format!(
                    "coalescing join refresh requires exactly one LeftSnapshotRightDelta branch, found {right_delta_count}"
                ));
            }
        }

        Ok(())
    }
}

fn validate_row_id_column(side: &str, column: &OutputColumn) -> Result<(), String> {
    if !column
        .name
        .eq_ignore_ascii_case(crate::common::ICEBERG_ROW_ID_COL)
        || *column.value_type.data_type() != DataType::Int64
        || !column.is_internal
    {
        return Err(format!(
            "join refresh descriptor has invalid {side} row-id column"
        ));
    }
    Ok(())
}

fn output_column_shape_matches(output: &OutputColumn, source: &OutputColumn) -> bool {
    output.value_type == source.value_type && output.is_internal == source.is_internal
}

fn output_columns_eq(left: &[OutputColumn], right: &[OutputColumn]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| output_column_eq(left, right))
}

fn output_column_eq(left: &OutputColumn, right: &OutputColumn) -> bool {
    left.column_id == right.column_id
        && left.name == right.name
        && left.value_type == right.value_type
        && left.is_internal == right.is_internal
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_id_descriptor_accepts_nullable_provider_metadata() {
        let mut column = out(
            1,
            crate::common::ICEBERG_ROW_ID_COL,
            DataType::Int64,
            true,
            true,
        );
        assert!(validate_row_id_column("left", &column).is_ok());
        let mut desc = valid_descriptor();
        desc.left_row_id_column.value_type = column.value_type.clone();
        desc.right_row_id_column.value_type = column.value_type.clone();
        assert!(desc.validate().is_ok());
        column.value_type = novarocks_type_contract::FunctionValueType::new(DataType::Utf8, true);
        assert!(validate_row_id_column("left", &column).is_err());
        column.value_type = novarocks_type_contract::FunctionValueType::new(DataType::Int64, true);
        column.is_internal = false;
        assert!(validate_row_id_column("left", &column).is_err());
    }

    fn out(
        id: u32,
        name: &str,
        data_type: DataType,
        nullable: bool,
        is_internal: bool,
    ) -> OutputColumn {
        OutputColumn {
            name: name.to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(data_type, nullable),

            column_id: ColumnId(id),
            is_internal,
        }
    }

    fn valid_descriptor() -> JoinRefreshDescriptor {
        JoinRefreshDescriptor {
            mode: JoinRefreshMode::Coalesce,
            mv_identity: JoinRefreshMvIdentity {
                catalog: "ice".to_string(),
                database: "db".to_string(),
                name: "mv_join".to_string(),
            },
            left_occurrence_id: crate::compiler::SqlMvRelationOccurrenceId::new(7),
            right_occurrence_id: crate::compiler::SqlMvRelationOccurrenceId::new(42),
            left_base_fqn: "ice.db.left_t".to_string(),
            right_base_fqn: "ice.db.right_t".to_string(),
            left_row_id_column: out(
                1,
                crate::common::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
                true,
            ),
            right_row_id_column: out(
                2,
                crate::common::ICEBERG_ROW_ID_COL,
                DataType::Int64,
                false,
                true,
            ),
            action_column: out(
                3,
                crate::common::CHANGE_OP_COLUMN,
                DataType::Int8,
                false,
                true,
            ),
            join_apply_key_column: out(
                4,
                crate::planner::vocabulary::JOIN_APPLY_KEY_COLUMN_NAME,
                DataType::Utf8,
                false,
                true,
            ),
            payload_columns: vec![out(5, "k", DataType::Int64, false, false)],
            join_key_pairs: vec![JoinRefreshJoinKeyPair {
                left_column: out(6, "left_k", DataType::Int64, false, false),
                right_column: out(7, "right_k", DataType::Int64, false, false),
            }],
            output_mappings: vec![
                JoinRefreshOutputMapping {
                    mv_output_column: out(8, "mv_k", DataType::Int64, false, false),
                    source: JoinRefreshOutputSource::Payload(ColumnId(5)),
                },
                JoinRefreshOutputMapping {
                    mv_output_column: out(
                        9,
                        crate::common::CHANGE_OP_COLUMN,
                        DataType::Int8,
                        false,
                        true,
                    ),
                    source: JoinRefreshOutputSource::Action(ColumnId(3)),
                },
                JoinRefreshOutputMapping {
                    mv_output_column: out(
                        10,
                        crate::planner::vocabulary::JOIN_APPLY_KEY_COLUMN_NAME,
                        DataType::Utf8,
                        false,
                        true,
                    ),
                    source: JoinRefreshOutputSource::JoinApplyKey(ColumnId(4)),
                },
            ],
            branches: vec![
                JoinRefreshBranchDescriptor {
                    side: JoinRefreshBranchSide::LeftDeltaRightSnapshot,
                    action_column_id: ColumnId(3),
                },
                JoinRefreshBranchDescriptor {
                    side: JoinRefreshBranchSide::LeftSnapshotRightDelta,
                    action_column_id: ColumnId(3),
                },
            ],
            needs_target_locator: true,
        }
    }

    #[test]
    fn validates_coalescing_join_refresh_descriptor() {
        valid_descriptor()
            .validate()
            .expect("descriptor should validate");
    }

    #[test]
    fn valid_descriptor_carries_identity_join_keys_and_output_mappings() {
        let desc = valid_descriptor();
        assert_eq!(desc.mv_identity.name, "mv_join");
        assert_eq!(desc.join_key_pairs.len(), 1);
        assert_eq!(desc.output_mappings.len(), 3);
    }

    #[test]
    fn rejects_coalescing_descriptor_without_locator() {
        let mut desc = valid_descriptor();
        desc.needs_target_locator = false;
        let err = desc.validate().expect_err("coalesce requires locator");
        assert!(err.contains("requires target locator"));
    }

    #[test]
    fn rejects_descriptor_with_same_base_on_both_sides() {
        let mut desc = valid_descriptor();
        desc.right_occurrence_id = desc.left_occurrence_id;
        assert_invalid(desc, "requires distinct left and right bases");
    }

    #[test]
    fn rejects_descriptor_without_left_base_fqn() {
        let mut desc = valid_descriptor();
        desc.left_base_fqn = "  ".to_string();
        assert_invalid(desc, "requires left base");
    }

    #[test]
    fn rejects_descriptor_without_right_base_fqn() {
        let mut desc = valid_descriptor();
        desc.right_base_fqn.clear();
        assert_invalid(desc, "requires right base");
    }

    #[test]
    fn rejects_descriptor_without_payload_columns() {
        let mut desc = valid_descriptor();
        desc.payload_columns.clear();
        assert_invalid(desc, "requires at least one payload column");
    }

    #[test]
    fn rejects_descriptor_without_mv_name() {
        let mut desc = valid_descriptor();
        desc.mv_identity.name.clear();
        assert_invalid(desc, "requires MV name");
    }

    #[test]
    fn rejects_descriptor_without_join_key_pairs() {
        let mut desc = valid_descriptor();
        desc.join_key_pairs.clear();
        assert_invalid(desc, "requires at least one join key pair");
    }

    #[test]
    fn rejects_descriptor_without_output_mappings() {
        let mut desc = valid_descriptor();
        desc.output_mappings.clear();
        assert_invalid(desc, "requires at least one output mapping");
    }

    #[test]
    fn full_descriptor_allows_no_delta_branches() {
        let mut desc = valid_descriptor();
        desc.mode = JoinRefreshMode::Full;
        desc.branches.clear();
        desc.needs_target_locator = false;

        desc.validate().expect("full refresh descriptor");
    }

    #[test]
    fn full_descriptor_allows_nullable_join_key_mismatch() {
        let mut desc = valid_descriptor();
        desc.mode = JoinRefreshMode::Full;
        desc.branches.clear();
        desc.needs_target_locator = false;
        desc.join_key_pairs[0].left_column.value_type = desc.join_key_pairs[0]
            .left_column
            .value_type
            .with_nullable(true)
            .unwrap();
        desc.join_key_pairs[0].right_column.value_type = desc.join_key_pairs[0]
            .right_column
            .value_type
            .with_nullable(false)
            .unwrap();

        desc.validate().expect("full refresh descriptor");
    }

    #[test]
    fn full_descriptor_rejects_delta_branches() {
        let mut desc = valid_descriptor();
        desc.mode = JoinRefreshMode::Full;
        desc.needs_target_locator = false;

        assert_invalid(desc, "must not carry delta branches");
    }

    #[test]
    fn rejects_output_mapping_with_unknown_payload_column() {
        let mut desc = valid_descriptor();
        desc.output_mappings[0].source = JoinRefreshOutputSource::Payload(ColumnId(99));
        assert_invalid(desc, "unknown payload column");
    }

    #[test]
    fn rejects_output_mapping_with_unknown_action_column() {
        let mut desc = valid_descriptor();
        desc.output_mappings[1].source = JoinRefreshOutputSource::Action(ColumnId(99));
        assert_invalid(desc, "unknown action column");
    }

    #[test]
    fn rejects_output_mapping_with_unknown_join_apply_key_column() {
        let mut desc = valid_descriptor();
        desc.output_mappings[2].source = JoinRefreshOutputSource::JoinApplyKey(ColumnId(99));
        assert_invalid(desc, "unknown join apply-key column");
    }

    #[test]
    fn rejects_output_mappings_missing_payload_column_mapping() {
        let mut desc = valid_descriptor();
        desc.output_mappings.remove(0);
        assert_invalid(desc, "missing output mapping for payload column");
    }

    #[test]
    fn rejects_output_mappings_missing_action_column_mapping() {
        let mut desc = valid_descriptor();
        desc.output_mappings.remove(1);
        assert_invalid(desc, "missing output mapping for action column");
    }

    #[test]
    fn rejects_output_mappings_missing_join_apply_key_column_mapping() {
        let mut desc = valid_descriptor();
        desc.output_mappings.pop();
        assert_invalid(desc, "missing output mapping for join apply-key column");
    }

    #[test]
    fn rejects_output_mappings_with_duplicate_mv_output_column_id() {
        let mut desc = valid_descriptor();
        desc.output_mappings[1].mv_output_column.column_id =
            desc.output_mappings[0].mv_output_column.column_id;
        assert_invalid(desc, "duplicate MV output column id");
    }

    #[test]
    fn rejects_output_mappings_with_duplicate_mv_output_column_name() {
        let mut desc = valid_descriptor();
        desc.output_mappings[1].mv_output_column.name =
            desc.output_mappings[0].mv_output_column.name.to_uppercase();
        assert_invalid(desc, "duplicate MV output column name");
    }

    #[test]
    fn rejects_output_mapping_with_type_mismatch() {
        let mut desc = valid_descriptor();
        desc.output_mappings[0].mv_output_column.value_type =
            novarocks_type_contract::FunctionValueType::try_with_logical_type(
                DataType::Utf8,
                desc.output_mappings[0]
                    .mv_output_column
                    .value_type
                    .nullable(),
                desc.output_mappings[0]
                    .mv_output_column
                    .value_type
                    .logical_type(),
            )
            .unwrap();
        assert_invalid(desc, "does not match source column");
    }

    #[test]
    fn rejects_output_mapping_with_nullability_mismatch() {
        let mut desc = valid_descriptor();
        desc.output_mappings[0].mv_output_column.value_type = desc.output_mappings[0]
            .mv_output_column
            .value_type
            .with_nullable(true)
            .unwrap();
        assert_invalid(desc, "does not match source column");
    }

    #[test]
    fn rejects_output_mapping_with_internal_flag_mismatch() {
        let mut desc = valid_descriptor();
        desc.output_mappings[2].mv_output_column.is_internal = false;
        assert_invalid(desc, "does not match source column");
    }

    #[test]
    fn rejects_descriptor_with_invalid_left_row_id_column() {
        let mut desc = valid_descriptor();
        desc.left_row_id_column.value_type =
            novarocks_type_contract::FunctionValueType::try_with_logical_type(
                DataType::Utf8,
                desc.left_row_id_column.value_type.nullable(),
                desc.left_row_id_column.value_type.logical_type(),
            )
            .unwrap();
        assert_invalid(desc, "invalid left row-id column");
    }

    #[test]
    fn rejects_descriptor_with_invalid_right_row_id_column() {
        let mut desc = valid_descriptor();
        desc.right_row_id_column.name = crate::common::ICEBERG_ROW_POS_COL.to_string();
        assert_invalid(desc, "invalid right row-id column");
    }

    #[test]
    fn rejects_descriptor_with_non_internal_row_id_column() {
        let mut desc = valid_descriptor();
        desc.left_row_id_column.is_internal = false;
        assert_invalid(desc, "invalid left row-id column");
    }

    #[test]
    fn rejects_join_key_pair_with_type_mismatch() {
        let mut desc = valid_descriptor();
        desc.join_key_pairs[0].right_column.value_type =
            novarocks_type_contract::FunctionValueType::try_with_logical_type(
                DataType::Utf8,
                desc.join_key_pairs[0].right_column.value_type.nullable(),
                desc.join_key_pairs[0]
                    .right_column
                    .value_type
                    .logical_type(),
            )
            .unwrap();
        assert_invalid(desc, "join key pair type mismatch");
    }

    #[test]
    fn allows_join_key_pair_with_nullability_mismatch() {
        let mut desc = valid_descriptor();
        desc.join_key_pairs[0].right_column.value_type = desc.join_key_pairs[0]
            .right_column
            .value_type
            .with_nullable(true)
            .unwrap();

        desc.validate()
            .expect("join key nullability follows SQL equality semantics");
    }

    #[test]
    fn rejects_join_key_pair_with_same_column_id_on_both_sides() {
        let mut desc = valid_descriptor();
        desc.join_key_pairs[0].right_column.column_id =
            desc.join_key_pairs[0].left_column.column_id;
        assert_invalid(desc, "join key pair cannot use the same column id");
    }

    #[test]
    fn rejects_descriptor_with_invalid_action_column() {
        let mut desc = valid_descriptor();
        desc.action_column.value_type =
            novarocks_type_contract::FunctionValueType::try_with_logical_type(
                DataType::Int64,
                desc.action_column.value_type.nullable(),
                desc.action_column.value_type.logical_type(),
            )
            .unwrap();
        assert_invalid(desc, "invalid action column");
    }

    #[test]
    fn derived_join_key_descriptor_accepts_nullable_source_without_changing_its_type() {
        let mut desc = valid_descriptor();
        desc.join_apply_key_column.value_type =
            novarocks_type_contract::FunctionValueType::new(DataType::Utf8, true);
        for mapping in &mut desc.output_mappings {
            if matches!(mapping.source, JoinRefreshOutputSource::JoinApplyKey(_)) {
                mapping.mv_output_column.value_type = desc.join_apply_key_column.value_type.clone();
            }
        }
        assert!(desc.validate().is_ok());
        assert!(desc.join_apply_key_column.value_type.nullable());
    }

    #[test]
    fn rejects_descriptor_with_invalid_join_apply_key_column() {
        let mut desc = valid_descriptor();
        desc.join_apply_key_column.value_type =
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, true);
        assert_invalid(desc, "invalid join apply-key column");
    }

    #[test]
    fn rejects_descriptor_without_branches() {
        let mut desc = valid_descriptor();
        desc.branches.clear();
        assert_invalid(desc, "requires at least one branch");
    }

    #[test]
    fn rejects_descriptor_with_branch_action_column_mismatch() {
        let mut desc = valid_descriptor();
        desc.branches[0].action_column_id = ColumnId(99);
        assert_invalid(desc, "does not match action column id");
    }

    #[test]
    fn rejects_coalescing_descriptor_missing_left_delta_branch() {
        let mut desc = valid_descriptor();
        desc.branches.remove(0);
        assert_invalid(desc, "exactly one LeftDeltaRightSnapshot branch");
    }

    #[test]
    fn rejects_coalescing_descriptor_missing_right_delta_branch() {
        let mut desc = valid_descriptor();
        desc.branches.pop();
        assert_invalid(desc, "exactly one LeftSnapshotRightDelta branch");
    }

    #[test]
    fn rejects_coalescing_descriptor_duplicate_delta_side() {
        let mut desc = valid_descriptor();
        desc.branches[1].side = JoinRefreshBranchSide::LeftDeltaRightSnapshot;
        assert_invalid(desc, "exactly one LeftDeltaRightSnapshot branch");
    }

    fn assert_invalid(desc: JoinRefreshDescriptor, expected: &str) {
        let err = desc.validate().expect_err("descriptor should be invalid");
        assert!(
            err.contains(expected),
            "expected error to contain {expected:?}, got {err:?}"
        );
    }
}
