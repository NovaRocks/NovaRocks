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

//! Plan-time partition derivation: resolve the contract-level
//! `PartitionDerivationSpec` and record the outcome on `ImvPlanAnnotation`.
//!
//! P1 scope (umbrella spec §5.1 / D5): matches aggregate-state-merge shapes
//! only; the annotation is observability + P2 input — live pruning still
//! flows from plan-time manifest derivation, so this rule never changes the
//! plan and never fails the rewrite.

use crate::compiler::SqlCompileError;
use crate::compiler::mv_rewrite::{
    SqlImvExpressionKind, SqlImvPartitionDerivationField, SqlImvPartitionDerivationSpec,
    SqlImvPartitionTransform, SqlImvSchemaContract,
};
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::rewrite::context::RewriteContext;
use crate::optimizer::rewrite::phase::RewritePhase;
use crate::optimizer::rewrite::result::RewriteResult;
use crate::optimizer::rewrite::rule::{LogicalRewriteRule, RewriteTraversal};
use crate::planner::imv_rewrite::annotation::{ImvExtension, ImvPartitionAnnotation};

pub(crate) struct DerivePartitionSpecRule;

impl LogicalRewriteRule for DerivePartitionSpecRule {
    fn name(&self) -> &'static str {
        "DerivePartitionSpec"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::SemanticRewrite
    }

    fn traversal(&self) -> RewriteTraversal {
        RewriteTraversal::TopDown
    }

    fn matches(&self, _expr: &OptExpr, ctx: &RewriteContext) -> bool {
        ctx.extension::<ImvExtension>().is_some_and(|ext| {
            ext.annotation.partition.is_none() && ext.annotation.change_stream.has_aggregate()
        })
    }

    fn apply(
        &self,
        _expr: OptExpr,
        ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, SqlCompileError> {
        let ext = ctx
            .extension::<ImvExtension>()
            .ok_or_else(|| "DerivePartitionSpec requires ImvExtension".to_string())
            .map_err(SqlCompileError::Compilation)?
            .clone();

        let outcome = match resolve_partition_derivation_spec(&ext.snapshot.schema_contract) {
            Ok(None) => ImvPartitionAnnotation::Unpartitioned,
            Ok(Some(spec)) => ImvPartitionAnnotation::Derivable { specs: vec![spec] },
            Err(err) => ImvPartitionAnnotation::NotDerivable {
                reason: err.to_string(),
            },
        };

        let mut annotation = ext.annotation.clone();
        annotation.partition = Some(outcome);
        ctx.set_extension::<ImvExtension>(ImvExtension { annotation, ..ext });
        Ok(RewriteResult::Unchanged)
    }
}

/// Resolve an admitted SQL MV contract into plan-time partition facts.  This
/// deliberately preserves the historical warn-and-skip behaviour: malformed
/// contracts become `NotDerivable` annotations rather than rewrite failures.
fn resolve_partition_derivation_spec(
    contract: &SqlImvSchemaContract,
) -> Result<Option<SqlImvPartitionDerivationSpec>, String> {
    let Some(partition) = contract.target.partition.as_ref() else {
        return Ok(None);
    };
    if partition.fields.is_empty() {
        return Ok(None);
    }
    let mut fields = Vec::with_capacity(partition.fields.len());
    for partition_field in &partition.fields {
        let output_index = contract
            .target
            .visible_columns
            .iter()
            .position(|column| column.target_field_id == partition_field.source_target_field_id)
            .ok_or_else(|| {
                format!(
                    "aggregate target partition field {} has no visible column for target field id {:?}",
                    partition_field.partition_field_name, partition_field.source_target_field_id
                )
            })?;
        let lineage = contract.output_columns.get(output_index).ok_or_else(|| {
            format!(
                "aggregate target partition field {} has no output lineage",
                partition_field.partition_field_name
            )
        })?;
        let is_source_column = lineage.expression.kind == SqlImvExpressionKind::Column
            && lineage.expression.referenced_base_fields.len() == 1;
        if !is_source_column {
            return Err(format!(
                "aggregate target partition field {} requires pure column lineage",
                partition_field.partition_field_name
            ));
        }
        if partition_field.transform == SqlImvPartitionTransform::Void {
            return Err(format!(
                "aggregate target partition field {} uses unsupported void transform",
                partition_field.partition_field_name
            ));
        }
        fields.push(SqlImvPartitionDerivationField {
            partition_field_name: partition_field.partition_field_name.clone(),
            source_target_field_id: partition_field.source_target_field_id.clone(),
            output_index,
            transform: partition_field.transform.clone(),
        });
    }
    Ok(Some(SqlImvPartitionDerivationSpec {
        target_spec_id: partition.target_spec_id,
        fields,
    }))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;
    use crate::analysis::OutputColumn;
    use crate::column_id::ColumnId;
    use crate::optimizer::operator::{Operator, ValuesOp};
    use crate::planner::imv_rewrite::annotation::ImvPlanAnnotation;
    use crate::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor;
    use crate::planner::imv_rewrite::join_refresh_descriptor::{
        JoinRefreshBranchDescriptor, JoinRefreshBranchSide, JoinRefreshDescriptor,
        JoinRefreshJoinKeyPair, JoinRefreshMode, JoinRefreshMvIdentity, JoinRefreshOutputMapping,
        JoinRefreshOutputSource,
    };

    #[test]
    fn partition_derivation_preserves_existing_join_refresh_descriptor() {
        let descriptor = valid_join_refresh_descriptor();
        let mut ctx = RewriteContext::for_mv_refresh(Vec::<String>::new());
        ctx.set_extension::<ImvExtension>(ImvExtension {
            snapshot: crate::compiler::mv_rewrite::test_incremental_snapshot(),
            annotation: ImvPlanAnnotation {
                target_read:
                    crate::planner::imv_rewrite::annotation::TargetReadRequirement::ExistingRows,
                partition: None,
                change_stream: ImvChangeStreamDescriptor {
                    join_refresh: Some(descriptor.clone()),
                    ..Default::default()
                },
            },
        });

        DerivePartitionSpecRule
            .apply(empty_values_expr(), &mut ctx)
            .expect("partition derivation should run");

        let annotation = &ctx
            .extension::<ImvExtension>()
            .expect("extension should remain installed")
            .annotation;
        assert!(annotation.partition.is_some());
        assert_eq!(
            annotation.change_stream.join_refresh.as_ref(),
            Some(&descriptor)
        );
    }

    fn empty_values_expr() -> OptExpr {
        OptExpr::leaf(Operator::LogicalValues(ValuesOp {
            rows: Vec::new(),
            columns: Vec::new(),
        }))
    }

    fn valid_join_refresh_descriptor() -> JoinRefreshDescriptor {
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
            left_row_id_column: out(1, "_row_id", DataType::Int64, false, true),
            right_row_id_column: out(2, "_row_id", DataType::Int64, false, true),
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

    fn out(
        id: u32,
        name: &str,
        data_type: DataType,
        nullable: bool,
        is_internal: bool,
    ) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_string(),
            value_type: novarocks_type_contract::FunctionValueType::new(data_type, nullable),

            is_internal,
        }
    }
}
