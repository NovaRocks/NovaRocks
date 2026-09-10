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

//! Stage-neutral leaf payloads shared by logical and physical planner IR.

use arrow::datatypes::DataType;

use crate::analysis::{OutputColumn, ProjectItem, SortItem, TypedExpr};
use crate::binding::SqlTableBindingId;
use crate::column_id::ColumnId;
use crate::common::{ScanVariantColumn, SqlTopNType};
use crate::planner::table::TableDef;

/// SQL-optimizer identity of one pre-rewrite scan occurrence.
///
/// The anchor column is allocated uniquely for that scan by one planning
/// session, so two aliases of the same binding remain distinct.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SqlScanOccurrence {
    binding: SqlTableBindingId,
    anchor: ColumnId,
}

impl SqlScanOccurrence {
    pub(crate) fn from_scan(
        binding: SqlTableBindingId,
        columns: &[crate::common::OutputColumn],
    ) -> Option<Self> {
        let anchor = columns.iter().map(|column| column.column_id).min()?;
        (anchor != ColumnId::UNSET).then_some(Self { binding, anchor })
    }

    pub const fn binding(self) -> SqlTableBindingId {
        self.binding
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MvRewriteInputSelection {
    occurrence: SqlScanOccurrence,
    publication_input_ordinal: usize,
}

impl MvRewriteInputSelection {
    pub const fn binding(self) -> SqlTableBindingId {
        self.occurrence.binding()
    }

    pub const fn occurrence(self) -> SqlScanOccurrence {
        self.occurrence
    }

    pub const fn publication_input_ordinal(self) -> usize {
        self.publication_input_ordinal
    }
}

/// Opaque evidence emitted only when the optimizer actually selects one MV
/// candidate. Public consumers may inspect the selected facts but cannot
/// construct or alter this marker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRewriteSelection {
    name: String,
    publication_id: Option<[u8; 16]>,
    definition_fingerprint: Option<[u8; 32]>,
    input_mapping: Vec<MvRewriteInputSelection>,
    publication_inputs: Vec<crate::compiler::SqlMvRewritePublicationRelation>,
    publication_target: Option<crate::compiler::SqlMvRewritePublicationRelation>,
}

impl MvRewriteSelection {
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn unverified(name: String) -> Self {
        Self {
            name,
            publication_id: None,
            definition_fingerprint: None,
            input_mapping: Vec::new(),
            publication_inputs: Vec::new(),
            publication_target: None,
        }
    }

    pub(crate) fn selected(
        name: String,
        publication_id: [u8; 16],
        definition_fingerprint: [u8; 32],
        input_mapping: Vec<(SqlScanOccurrence, usize)>,
        publication_inputs: Vec<crate::compiler::SqlMvRewritePublicationRelation>,
        publication_target: crate::compiler::SqlMvRewritePublicationRelation,
    ) -> Self {
        Self {
            name,
            publication_id: Some(publication_id),
            definition_fingerprint: Some(definition_fingerprint),
            input_mapping: input_mapping
                .into_iter()
                .map(
                    |(occurrence, publication_input_ordinal)| MvRewriteInputSelection {
                        occurrence,
                        publication_input_ordinal,
                    },
                )
                .collect(),
            publication_inputs,
            publication_target: Some(publication_target),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) const fn publication_id(&self) -> Option<[u8; 16]> {
        self.publication_id
    }

    pub(crate) const fn definition_fingerprint(&self) -> Option<[u8; 32]> {
        self.definition_fingerprint
    }

    pub(crate) fn input_mapping(&self) -> &[MvRewriteInputSelection] {
        &self.input_mapping
    }

    pub(crate) fn publication_inputs(&self) -> &[crate::compiler::SqlMvRewritePublicationRelation] {
        &self.publication_inputs
    }

    pub(crate) fn publication_target(
        &self,
    ) -> Option<&crate::compiler::SqlMvRewritePublicationRelation> {
        self.publication_target.as_ref()
    }
}

impl std::ops::Deref for MvRewriteSelection {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.name()
    }
}

impl std::fmt::Display for MvRewriteSelection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.name())
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct PlanScanNode {
    pub database: String,
    pub table: TableDef,
    pub alias: Option<String>,
    pub columns: Vec<OutputColumn>,
    pub predicates: Vec<TypedExpr>,
    pub required_columns: Option<Vec<String>>,
    pub variant_columns: Vec<ScanVariantColumn>,
    pub mv_rewritten_from: Option<MvRewriteSelection>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanFilterNode {
    pub predicate: TypedExpr,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanProjectNode {
    pub items: Vec<ProjectItem>,
    pub output_qualifier: Option<String>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanUnpivotNode {
    pub passthrough_columns: Vec<PlanUnpivotPassthroughColumn>,
    pub value_output_column_id: ColumnId,
    pub literal_output_column_ids: Vec<ColumnId>,
    pub value_mappings: Vec<PlanUnpivotValueMapping>,
    pub output_columns: Vec<OutputColumn>,
    pub max_output_rows: usize,
    pub max_output_bytes: usize,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanUnpivotPassthroughColumn {
    pub input_column_id: ColumnId,
    pub output_column_id: ColumnId,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanUnpivotValueMapping {
    pub input_value_column_id: ColumnId,
    pub constants: Vec<crate::analysis::UnpivotConstant>,
}

impl PlanUnpivotNode {
    #[expect(
        clippy::too_many_arguments,
        reason = "The typed unpivot contract keeps independently validated column roles explicit."
    )]
    #[allow(dead_code)]
    pub(crate) fn try_new(
        input_columns: &[OutputColumn],
        passthrough_columns: Vec<PlanUnpivotPassthroughColumn>,
        value_output_column_id: ColumnId,
        literal_output_column_ids: Vec<ColumnId>,
        value_mappings: Vec<PlanUnpivotValueMapping>,
        output_columns: Vec<OutputColumn>,
        max_output_rows: usize,
        max_output_bytes: usize,
    ) -> Result<Self, String> {
        let node = Self {
            passthrough_columns,
            value_output_column_id,
            literal_output_column_ids,
            value_mappings,
            output_columns,
            max_output_rows,
            max_output_bytes,
        };
        node.validate_against(input_columns)?;
        Ok(node)
    }

    pub(crate) fn validate_against(&self, input_columns: &[OutputColumn]) -> Result<(), String> {
        use std::collections::{HashMap, HashSet};

        const MAX_UNPIVOT_MAPPINGS: usize = 4_096;
        const MAX_UNPIVOT_CONSTANTS: usize = 16_384;
        const MAX_UNPIVOT_NESTED_ELEMENTS: usize = 4_096;
        const MAX_UNPIVOT_CONSTANT_BYTES: usize = 16 * 1024 * 1024;

        if self.max_output_rows == 0 {
            return Err("Unpivot max_output_rows must be greater than zero".to_string());
        }
        if self.max_output_bytes == 0 {
            return Err("Unpivot max_output_bytes must be greater than zero".to_string());
        }
        if self.value_mappings.is_empty() {
            return Err("Unpivot requires at least one value mapping".to_string());
        }
        if self.value_mappings.len() > MAX_UNPIVOT_MAPPINGS {
            return Err("Unpivot exceeds the value mapping limit".to_string());
        }

        let mut constant_count = 0usize;
        let mut nested_element_count = 0usize;
        let mut constant_bytes = 0usize;
        for (mapping_index, mapping) in self.value_mappings.iter().enumerate() {
            constant_count = constant_count
                .checked_add(mapping.constants.len())
                .ok_or_else(|| "Unpivot constant count overflowed".to_string())?;
            for (constant_index, constant) in mapping.constants.iter().enumerate() {
                match constant {
                    crate::analysis::UnpivotConstant::Scalar(expression) => {
                        if !matches!(expression.kind, crate::analysis::ExprKind::Literal(_)) {
                            return Err(format!(
                                "Unpivot mapping {mapping_index} scalar constant {constant_index} is not a literal expression"
                            ));
                        }
                        constant_bytes = constant_bytes
                            .checked_add(scalar_literal_retained_bytes(expression))
                            .ok_or_else(|| "Unpivot constant byte charge overflowed".to_string())?;
                    }
                    crate::analysis::UnpivotConstant::Int32List(values) => {
                        nested_element_count = nested_element_count
                            .checked_add(values.len())
                            .ok_or_else(|| "Unpivot nested element count overflowed".to_string())?;
                        constant_bytes = constant_bytes
                            .checked_add(values.len().saturating_mul(size_of::<i32>()))
                            .ok_or_else(|| "Unpivot constant byte charge overflowed".to_string())?;
                    }
                    crate::analysis::UnpivotConstant::Utf8Map(entries) => {
                        nested_element_count = nested_element_count
                            .checked_add(entries.len())
                            .ok_or_else(|| "Unpivot nested element count overflowed".to_string())?;
                        let mut previous = None;
                        for (entry_index, (key, value)) in entries.iter().enumerate() {
                            if key.is_empty() {
                                return Err(format!(
                                    "Unpivot mapping {mapping_index} map constant {constant_index} entry {entry_index} has an empty key"
                                ));
                            }
                            if previous.is_some_and(|previous: &str| previous >= key.as_str()) {
                                return Err(format!(
                                    "Unpivot mapping {mapping_index} map constant {constant_index} keys must be strictly increasing"
                                ));
                            }
                            constant_bytes = constant_bytes
                                .checked_add(key.len())
                                .and_then(|total| total.checked_add(value.len()))
                                .ok_or_else(|| {
                                    "Unpivot constant byte charge overflowed".to_string()
                                })?;
                            previous = Some(key.as_str());
                        }
                    }
                }
            }
        }
        if constant_count > MAX_UNPIVOT_CONSTANTS {
            return Err("Unpivot exceeds the constant count limit".to_string());
        }
        if nested_element_count > MAX_UNPIVOT_NESTED_ELEMENTS {
            return Err("Unpivot exceeds the nested element limit".to_string());
        }
        if constant_bytes > MAX_UNPIVOT_CONSTANT_BYTES {
            return Err("Unpivot exceeds the decoded constant byte limit".to_string());
        }

        let mut input_by_id = HashMap::with_capacity(input_columns.len());
        for column in input_columns {
            if input_by_id.insert(column.column_id, column).is_some() {
                return Err(format!(
                    "Unpivot input contains duplicate column id {}",
                    column.column_id
                ));
            }
        }
        let mut output_by_id = HashMap::with_capacity(self.output_columns.len());
        for column in &self.output_columns {
            if output_by_id.insert(column.column_id, column).is_some() {
                return Err(format!(
                    "Unpivot output contains duplicate column id {}",
                    column.column_id
                ));
            }
        }

        let mut assigned_outputs = HashSet::new();
        for mapping in &self.passthrough_columns {
            if !assigned_outputs.insert(mapping.output_column_id) {
                return Err(format!(
                    "Unpivot output column id {} has multiple producers",
                    mapping.output_column_id
                ));
            }
            let input = input_by_id.get(&mapping.input_column_id).ok_or_else(|| {
                format!(
                    "Unpivot passthrough input column id {} is not produced by its child",
                    mapping.input_column_id
                )
            })?;
            let output = output_by_id.get(&mapping.output_column_id).ok_or_else(|| {
                format!(
                    "Unpivot passthrough output column id {} is missing from output columns",
                    mapping.output_column_id
                )
            })?;
            require_exact_column_shape("passthrough", input, output)?;
        }

        if !assigned_outputs.insert(self.value_output_column_id) {
            return Err(format!(
                "Unpivot value output column id {} has multiple producers",
                self.value_output_column_id
            ));
        }
        let value_output = output_by_id
            .get(&self.value_output_column_id)
            .ok_or_else(|| {
                format!(
                    "Unpivot value output column id {} is missing from output columns",
                    self.value_output_column_id
                )
            })?;
        let mut value_nullable = false;
        for (index, mapping) in self.value_mappings.iter().enumerate() {
            let input = input_by_id
                .get(&mapping.input_value_column_id)
                .ok_or_else(|| {
                    format!(
                        "Unpivot value mapping {index} input column id {} is not produced by its child",
                        mapping.input_value_column_id
                    )
                })?;
            if input.data_type != value_output.data_type {
                return Err(format!(
                    "Unpivot value mapping {index} type mismatch: input {:?}, output {:?}",
                    input.data_type, value_output.data_type
                ));
            }
            value_nullable |= input.nullable;
            if mapping.constants.len() != self.literal_output_column_ids.len() {
                return Err(format!(
                    "Unpivot value mapping {index} literal count mismatch: expected {}, got {}",
                    self.literal_output_column_ids.len(),
                    mapping.constants.len()
                ));
            }
        }
        if value_output.nullable != value_nullable {
            return Err(format!(
                "Unpivot value output column id {} nullability mismatch: expected {}, got {}",
                self.value_output_column_id, value_nullable, value_output.nullable
            ));
        }

        let mut literal_outputs = HashSet::new();
        for (literal_index, output_id) in self.literal_output_column_ids.iter().enumerate() {
            if !literal_outputs.insert(*output_id) {
                return Err(format!(
                    "Unpivot has duplicate literal output column id {output_id}"
                ));
            }
            if !assigned_outputs.insert(*output_id) {
                return Err(format!(
                    "Unpivot output column id {output_id} has multiple producers"
                ));
            }
            let output = output_by_id.get(output_id).ok_or_else(|| {
                format!(
                    "Unpivot literal output column id {output_id} is missing from output columns"
                )
            })?;
            let mut nullable = false;
            for (mapping_index, mapping) in self.value_mappings.iter().enumerate() {
                let constant = &mapping.constants[literal_index];
                if constant.data_type() != output.data_type {
                    return Err(format!(
                        "Unpivot value mapping {mapping_index} constant {literal_index} type mismatch: constant {:?}, output {:?}",
                        constant.data_type(),
                        output.data_type
                    ));
                }
                nullable |= constant.nullable();
            }
            if output.nullable != nullable {
                return Err(format!(
                    "Unpivot literal output column id {output_id} nullability mismatch: expected {nullable}, got {}",
                    output.nullable
                ));
            }
        }

        if assigned_outputs.len() != output_by_id.len() {
            let extras = output_by_id
                .keys()
                .filter(|id| !assigned_outputs.contains(id))
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            return Err(format!(
                "Unpivot output columns contain unassigned column ids [{extras}]"
            ));
        }
        Ok(())
    }
}

fn scalar_literal_retained_bytes(expression: &TypedExpr) -> usize {
    match &expression.kind {
        crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::Decimal(value))
        | crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::String(value)) => {
            value.len()
        }
        crate::analysis::ExprKind::Literal(crate::analysis::LiteralValue::Binary(value)) => {
            value.len()
        }
        crate::analysis::ExprKind::Literal(_) => size_of::<crate::analysis::LiteralValue>(),
        _ => 0,
    }
}

fn require_exact_column_shape(
    role: &str,
    input: &OutputColumn,
    output: &OutputColumn,
) -> Result<(), String> {
    if input.data_type != output.data_type || input.nullable != output.nullable {
        Err(format!(
            "Unpivot {role} column shape mismatch: input id {} is {:?} nullable={}, output id {} is {:?} nullable={}",
            input.column_id,
            input.data_type,
            input.nullable,
            output.column_id,
            output.data_type,
            output.nullable
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod unpivot_tests {
    use super::*;
    use crate::analysis::{ExprKind, LiteralValue};

    fn column(id: u32, name: &str, data_type: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            data_type,
            nullable,
            is_internal: false,
        }
    }

    fn string_literal(value: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String(value.to_string())),
            data_type: DataType::Utf8,
            nullable: false,
        }
    }

    fn valid_node() -> Result<PlanUnpivotNode, String> {
        PlanUnpivotNode::try_new(
            &[
                column(1, "group", DataType::Utf8, false),
                column(2, "v1", DataType::Int64, true),
                column(3, "v2", DataType::Int64, false),
            ],
            vec![PlanUnpivotPassthroughColumn {
                input_column_id: ColumnId(1),
                output_column_id: ColumnId(11),
            }],
            ColumnId(13),
            vec![ColumnId(12)],
            vec![
                PlanUnpivotValueMapping {
                    input_value_column_id: ColumnId(2),
                    constants: vec![crate::analysis::UnpivotConstant::Scalar(string_literal(
                        "first",
                    ))],
                },
                PlanUnpivotValueMapping {
                    input_value_column_id: ColumnId(3),
                    constants: vec![crate::analysis::UnpivotConstant::Scalar(string_literal(
                        "second",
                    ))],
                },
            ],
            vec![
                column(11, "group", DataType::Utf8, false),
                column(12, "label", DataType::Utf8, false),
                column(13, "value", DataType::Int64, true),
            ],
            1024,
            1024 * 1024,
        )
    }

    #[test]
    fn validates_typed_unpivot_contract() {
        let node = valid_node().unwrap();
        assert_eq!(node.value_mappings.len(), 2);
    }

    #[test]
    fn rejects_value_type_drift() {
        let error = PlanUnpivotNode::try_new(
            &[column(1, "v", DataType::Int64, false)],
            Vec::new(),
            ColumnId(2),
            Vec::new(),
            vec![PlanUnpivotValueMapping {
                input_value_column_id: ColumnId(1),
                constants: Vec::new(),
            }],
            vec![column(2, "value", DataType::Utf8, false)],
            1,
            1024,
        )
        .unwrap_err();
        assert!(error.contains("type mismatch"), "{error}");
    }

    #[test]
    fn rejects_constant_type_drift() {
        let mut node = valid_node().unwrap();
        node.value_mappings[0].constants[0] = crate::analysis::UnpivotConstant::Int32List(vec![1]);
        let input = [
            column(1, "group", DataType::Utf8, false),
            column(2, "v1", DataType::Int64, true),
            column(3, "v2", DataType::Int64, false),
        ];
        let error = node.validate_against(&input).unwrap_err();
        assert!(error.contains("constant 0 type mismatch"), "{error}");
    }

    #[test]
    fn accepts_maximum_mapping_count_with_three_constants_each() {
        let list_type = crate::analysis::UnpivotConstant::Int32List(Vec::new()).data_type();
        let map_type = crate::analysis::UnpivotConstant::Utf8Map(Vec::new()).data_type();
        let mappings = (0..4_096)
            .map(|_| PlanUnpivotValueMapping {
                input_value_column_id: ColumnId(1),
                constants: vec![
                    crate::analysis::UnpivotConstant::Int32List(Vec::new()),
                    crate::analysis::UnpivotConstant::Scalar(string_literal("x")),
                    crate::analysis::UnpivotConstant::Utf8Map(Vec::new()),
                ],
            })
            .collect();
        let node = PlanUnpivotNode::try_new(
            &[column(1, "value", DataType::Binary, false)],
            Vec::new(),
            ColumnId(5),
            vec![ColumnId(2), ColumnId(3), ColumnId(4)],
            mappings,
            vec![
                column(2, "input_fields", list_type, false),
                column(3, "blob_type", DataType::Utf8, false),
                column(4, "properties", map_type, false),
                column(5, "body", DataType::Binary, false),
            ],
            4_096,
            32 * 1024 * 1024,
        )
        .expect("4,096 mappings with three constants each fit the 16,384 limit");

        assert_eq!(node.value_mappings.len(), 4_096);
        assert_eq!(
            node.value_mappings
                .iter()
                .map(|mapping| mapping.constants.len())
                .sum::<usize>(),
            12_288
        );
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanSortNode {
    pub items: Vec<SortItem>,
    pub analytic_partition_by: Vec<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
    pub offset: Option<i64>,
    pub partition_limit: Option<usize>,
    pub topn_type: Option<SqlTopNType>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanLimitNode {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanValuesNode {
    pub rows: Vec<Vec<TypedExpr>>,
    pub columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanRepeatNode {
    pub repeat_column_ref_list: Vec<Vec<String>>,
    pub repeat_column_ref_ids: Vec<Vec<ColumnId>>,
    pub grouping_ids: Vec<u64>,
    pub all_rollup_columns: Vec<String>,
    pub all_rollup_column_ids: Vec<ColumnId>,
    pub grouping_key_aliases: Vec<(String, String)>,
    pub grouping_fn_args: Vec<(String, Vec<String>)>,
    pub grouping_fn_arg_ids: Vec<Vec<ColumnId>>,
    pub grouping_fn_ids: Vec<(String, ColumnId)>,
    pub virtual_tuple_id: Option<i32>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanWindowNode {
    pub window_exprs: Vec<WindowExpr>,
    pub output_columns: Vec<OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanGenerateSeriesNode {
    pub start: i64,
    pub end: i64,
    pub step: i64,
    pub column_name: String,
    pub alias: Option<String>,
    pub output_column_id: ColumnId,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanTableFunctionNode {
    pub function_name: String,
    pub args: Vec<TypedExpr>,
    pub output_columns: Vec<OutputColumn>,
    pub alias: Option<String>,
    pub is_left_join: bool,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanRowCountAssertion {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanAssertOneRowNode {
    pub subquery_text: String,
    pub desired_num_rows: Option<i64>,
    pub assertion: PlanRowCountAssertion,
    pub group_key_column_ids: Vec<ColumnId>,
    pub group_key_labels: Vec<String>,
    pub keyed_message_prefix: Option<String>,
}

impl PlanAssertOneRowNode {
    pub(crate) fn global_at_most_one(subquery_text: impl Into<String>) -> Self {
        Self {
            subquery_text: subquery_text.into(),
            desired_num_rows: Some(1),
            assertion: PlanRowCountAssertion::Le,
            group_key_column_ids: Vec::new(),
            group_key_labels: Vec::new(),
            keyed_message_prefix: None,
        }
    }

    pub(crate) fn per_key_at_most_one(
        subquery_text: impl Into<String>,
        group_key_column_ids: Vec<ColumnId>,
        group_key_labels: Vec<String>,
        keyed_message_prefix: impl Into<String>,
    ) -> Self {
        Self {
            subquery_text: subquery_text.into(),
            desired_num_rows: Some(1),
            assertion: PlanRowCountAssertion::Le,
            group_key_column_ids,
            group_key_labels,
            keyed_message_prefix: Some(keyed_message_prefix.into()),
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanCTEAnchorNode {
    pub cte_id: crate::analysis::cte::CteId,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanCTEProduceNode {
    pub cte_id: crate::analysis::cte::CteId,
    pub output_columns: Vec<crate::analysis::OutputColumn>,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) struct PlanCTEConsumeNode {
    pub cte_id: crate::analysis::cte::CteId,
    pub alias: String,
    pub output_columns: Vec<crate::analysis::OutputColumn>,
    pub producer_column_ids: Vec<crate::column_id::ColumnId>,
}

/// A single window function expression with its OVER specification.
#[derive(Clone, Debug)]
pub(crate) struct WindowExpr {
    pub name: String,
    pub args: Vec<TypedExpr>,
    pub distinct: bool,
    pub function_order_by: Vec<SortItem>,
    pub aggregate_binding: Option<novarocks_functions::ResolvedAggregateSignature>,
    pub partition_by: Vec<TypedExpr>,
    pub order_by: Vec<SortItem>,
    pub window_frame: Option<crate::analysis::WindowFrame>,
    pub result_type: DataType,
    /// Display label only (EXPLAIN / output schema). Identity is now
    /// `output_column_id`. (G1: `output_name` downgraded from a binding key.)
    pub output_name: String,
    /// G1: globally-unique id of this window function's output column.
    /// TODO(G1 P2/P3): remove this allow once parent Project/window references
    /// are rebound by id and downstream binding consumes the populated field.
    #[allow(dead_code)]
    pub output_column_id: crate::column_id::ColumnId,
    /// `IGNORE NULLS` modifier. Currently honored by first_value / last_value
    /// / lead / lag; ignored for other window functions.
    pub ignore_nulls: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct AggregateCall {
    pub name: String,
    pub args: Vec<TypedExpr>,
    pub distinct: bool,
    pub result_type: DataType,
    pub order_by: Vec<SortItem>,
    pub resolved: novarocks_functions::ResolvedAggregateSignature,
    /// G1: id of THIS aggregate's output column. Planner-created calls are
    /// minted by `collect_aggregates`; rewrite paths should preserve existing
    /// ids or allocate ids for newly-defined aggregate outputs. Fixtures and
    /// transient adapters may use `UNSET` until they become executable
    /// bindings.
    pub output_column_id: crate::column_id::ColumnId,
}
