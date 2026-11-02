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

//! PruneJoinColumns — Phase 2 rule for Join nodes.
//!
//! This is a documented NO-OP. Join has no own output metadata to prune;
//! its output is the concatenation of its two children's output schemas.
//! Column needs were propagated to the left and right children separately
//! by the Phase-1 tagging pass. Each child's own prune rule drops unused
//! columns from that child independently.
//!
//! Kept for architectural symmetry and to allow per-operator
//! `disable_optimizer_rules` control in the future.

use crate::compiler::SqlCompileError;
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::pattern::{OpKind, Pattern};
use crate::optimizer::rewrite::context::RewriteContext;
use crate::optimizer::rewrite::phase::RewritePhase;
use crate::optimizer::rewrite::result::RewriteResult;
use crate::optimizer::rewrite::rule::LogicalRewriteRule;

pub(crate) struct PruneJoinColumns;

impl LogicalRewriteRule for PruneJoinColumns {
    fn name(&self) -> &'static str {
        "PruneJoinColumns"
    }

    fn phase(&self) -> RewritePhase {
        RewritePhase::StructuralRewrite
    }

    fn pattern(&self) -> Pattern {
        Pattern::Op {
            kind: OpKind::Join,
            children: vec![Pattern::MultiLeaf],
        }
    }

    fn matches(&self, _expr: &OptExpr, _ctx: &RewriteContext) -> bool {
        true
    }

    fn apply(
        &self,
        _expr: OptExpr,
        _ctx: &mut RewriteContext,
    ) -> Result<RewriteResult, SqlCompileError> {
        // No-op: Join has no own output metadata to prune; column needs were
        // propagated to its children by the Phase-1 tagging pass. Kept for
        // architectural symmetry + per-operator disable_optimizer_rules control.
        Ok(RewriteResult::Unchanged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::JoinKind;
    use crate::optimizer::operator::{LogicalJoinOp, Operator, ValuesOp};
    use crate::optimizer::opt_expr::OptExpr;
    use crate::optimizer::rewrite::context::{RewriteConsumer, RewriteContext};

    fn ctx() -> RewriteContext<'static> {
        RewriteContext::new(
            RewriteConsumer::Query,
            crate::optimizer::options::SessionOptimizerSettings::default(),
            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
    }

    fn dummy_input() -> OptExpr {
        OptExpr::leaf(Operator::LogicalValues(ValuesOp {
            rows: vec![],
            columns: vec![],
        }))
    }

    #[test]
    fn prune_join_is_always_unchanged() {
        let expr = OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                naa_author: None,
                join_type: JoinKind::Inner,
                condition: None,
            }),
            vec![dummy_input(), dummy_input()],
        );

        let rule = PruneJoinColumns;

        // pattern gates the structural operator kind.
        assert!(
            crate::optimizer::rewrite::tree_binder::bind_tree(
                &rule.pattern(),
                &expr,
                crate::optimizer::test_optimizer_control()
            )
            .unwrap()
            .is_some()
        );

        // apply always returns Unchanged
        let result = rule.apply(expr, &mut ctx()).unwrap();
        assert!(
            matches!(result, RewriteResult::Unchanged),
            "PruneJoinColumns must always return Unchanged"
        );
    }
}
