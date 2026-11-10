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

//! Prune metadata outputs without inventing a cardinality column.
use crate::optimizer::operator::Operator;
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::pattern::{OpKind, Pattern};
use crate::optimizer::rewrite::context::RewriteContext;
use crate::optimizer::rewrite::phase::RewritePhase;
use crate::optimizer::rewrite::result::RewriteResult;
use crate::optimizer::rewrite::rule::LogicalRewriteRule;

pub(crate) struct PruneSystemTableColumns;
impl LogicalRewriteRule for PruneSystemTableColumns {
    fn name(&self) -> &'static str {
        "PruneSystemTableColumns"
    }
    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }
    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::SystemTable,
            children: vec![],
        }
    }
    fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        true
    }
    fn apply(
        &self,
        expr: OptExpr,
        _ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, crate::compiler::SqlCompileError> {
        let OptExpr {
            op,
            children,
            required_output_columns,
        } = expr;
        let Operator::LogicalSystemTable(mut node) = op else {
            unreachable!()
        };
        let Some(needed) = &required_output_columns else {
            return Ok(RewriteResult::Unchanged);
        };
        if node.columns.len() != node.required_columns.len() {
            return Err("system table column ordinals differ from its outputs".into());
        }
        if node
            .columns
            .iter()
            .all(|column| needed.contains(&column.column_id))
        {
            return Ok(RewriteResult::Unchanged);
        }
        let (columns, ordinals) = node
            .columns
            .into_iter()
            .zip(node.required_columns)
            .filter(|(column, _)| needed.contains(&column.column_id))
            .unzip();
        node.columns = columns;
        node.required_columns = ordinals;
        Ok(RewriteResult::Changed(OptExpr {
            op: Operator::LogicalSystemTable(node),
            children,
            required_output_columns,
        }))
    }
}
