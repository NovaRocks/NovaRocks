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
