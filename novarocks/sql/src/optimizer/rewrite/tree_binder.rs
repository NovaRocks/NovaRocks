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

//! Tree-side declarative Pattern matcher.
//!
//! The tree rewrite driver uses this matcher as a structural pre-gate before
//! calling each rule's field-level `matches` method. The migration invariants
//! are:
//! - driver traversal and fixed-point order stay unchanged;
//! - default `Pattern::Leaf` is a root wildcard, so holdout rules keep their
//!   legacy imperative matching behavior;
//! - for migrated rules, structural `pattern` matching plus field-level
//!   `matches` is equivalent to the old monolithic `matches` predicate;
//! - `apply` remains the semantic rewrite boundary and is not interpreted by
//!   the binder;
//! - `first_match_only` is degenerate on concrete trees because one node can
//!   produce at most one binding.
//!
//! Unlike the memo binder, an `OptExpr` child is one concrete subtree, so a
//! successful match produces at most one binding.

use crate::compiler::SqlCompileError;
use crate::optimizer::operator::Operator;
use crate::optimizer::opt_expr::OptExpr;
use crate::optimizer::pattern::{Pattern, op_kind};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

/// A successful tree pattern match.
///
/// `interiors` contains only nodes matched by `Pattern::Op`, stored in DFS
/// preorder. `Pattern::Leaf` and `Pattern::MultiLeaf` captures are not
/// represented here.
pub(crate) struct TreeBinding<'t> {
    root: &'t OptExpr,
    interiors: Vec<&'t OptExpr>,
}

impl<'t> TreeBinding<'t> {
    // These accessors support migrated rules and tests that need matched
    // interior nodes.
    /// Return the root expression matched by the pattern.
    #[allow(dead_code)]
    pub(crate) fn root(&self) -> &'t OptExpr {
        self.root
    }

    /// Return the operator for a matched `Pattern::Op` interior index.
    ///
    /// Panics if `i` is outside the matched interior list.
    #[allow(dead_code)]
    pub(crate) fn op(&self, i: usize) -> &'t Operator {
        &self.interiors[i].op
    }

    /// Return the node for a matched `Pattern::Op` interior index.
    ///
    /// Panics if `i` is outside the matched interior list.
    #[allow(dead_code)]
    pub(crate) fn node(&self, i: usize) -> &'t OptExpr {
        self.interiors[i]
    }

    /// Return the children for a matched `Pattern::Op` interior index.
    ///
    /// Panics if `i` is outside the matched interior list.
    #[allow(dead_code)]
    pub(crate) fn children(&self, i: usize) -> &'t [OptExpr] {
        &self.interiors[i].children
    }
}

#[allow(dead_code)]
pub(crate) fn bind_tree<'t>(
    pattern: &Pattern,
    expr: &'t OptExpr,
    control: &dyn PureCompileControl,
) -> Result<Option<TreeBinding<'t>>, SqlCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = bind_tree_observed(pattern, expr, &mut work)?;
    work.finish()?;
    Ok(result)
}

/// Reuse the rewrite traversal's same-request observation owner. The caller
/// owns entry and finish; the successful binding retains only tree references.
pub(crate) fn bind_tree_observed<'t>(
    pattern: &Pattern,
    expr: &'t OptExpr,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<TreeBinding<'t>>, SqlCompileError> {
    let mut interiors = Vec::new();
    if !match_pattern(pattern, expr, &mut interiors, work)? {
        return Ok(None);
    }
    Ok(Some(TreeBinding {
        root: expr,
        interiors,
    }))
}

fn match_pattern<'t>(
    pattern: &Pattern,
    expr: &'t OptExpr,
    interiors: &mut Vec<&'t OptExpr>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlCompileError> {
    work.step()?;
    match pattern {
        Pattern::Leaf | Pattern::MultiLeaf => Ok(true),
        Pattern::Op { kind, children } => {
            if op_kind(&expr.op) != Some(*kind) {
                return Ok(false);
            }

            work.step()?;
            interiors.push(expr);
            match_children(children, &expr.children, interiors, work)
        }
    }
}

fn match_children<'t>(
    patterns: &[Pattern],
    child_exprs: &'t [OptExpr],
    interiors: &mut Vec<&'t OptExpr>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlCompileError> {
    work.step()?;
    let has_multi_leaf_tail = matches!(patterns.last(), Some(Pattern::MultiLeaf));
    let fixed_patterns = if has_multi_leaf_tail {
        &patterns[..patterns.len() - 1]
    } else {
        patterns
    };

    for pattern in fixed_patterns {
        work.step()?;
        if matches!(pattern, Pattern::MultiLeaf) {
            return Ok(false);
        }
    }

    if has_multi_leaf_tail {
        if child_exprs.len() < fixed_patterns.len() {
            return Ok(false);
        }
    } else if child_exprs.len() != fixed_patterns.len() {
        return Ok(false);
    }

    for (pattern, child_expr) in fixed_patterns.iter().zip(child_exprs.iter()) {
        work.step()?;
        if !match_pattern(pattern, child_expr, interiors, work)? {
            return Ok(false);
        }
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::TreeBinding;
    use crate::common::{JoinKind, LiteralValue};
    use crate::optimizer::operator::{FilterOp, LogicalJoinOp, Operator, ScanOp};
    use crate::optimizer::opt_expr::OptExpr;
    use crate::optimizer::pattern::{OpKind, Pattern};
    use crate::optimizer::scalar::{HashableLiteral, ScalarArena, ScalarId, ScalarNode};
    use crate::planner::table::TableDef;
    use arrow::datatypes::DataType;

    fn bind_tree<'t>(pattern: &Pattern, expr: &'t OptExpr) -> Option<TreeBinding<'t>> {
        super::bind_tree(
            pattern,
            expr,
            crate::optimizer::rewrite::context::unbounded_rewrite_test_control(),
        )
        .unwrap()
    }

    fn bool_literal_scalar(arena: &mut ScalarArena) -> ScalarId {
        arena.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Bool(true))),
            novarocks_type_contract::FunctionValueType::new(DataType::Boolean, false),
        )
    }

    fn mk_scan() -> OptExpr {
        OptExpr::leaf(Operator::LogicalScan(ScanOp {
            database: "db".into(),
            table: TableDef {
                name: "t".into(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: crate::compiler::mv_rewrite::test_scan_source(
                    crate::planner::table::SqlScanKind::ConnectorRead,
                ),
            },
            alias: None,
            stats_ref: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }))
    }

    fn mk_filter(predicate: ScalarId, child: OptExpr) -> OptExpr {
        OptExpr::new(Operator::LogicalFilter(FilterOp { predicate }), vec![child])
    }

    fn mk_join(left: OptExpr, right: OptExpr) -> OptExpr {
        OptExpr::new(
            Operator::LogicalJoin(LogicalJoinOp {
                naa_author: None,
                join_type: JoinKind::Inner,
                condition: None,
            }),
            vec![left, right],
        )
    }

    #[test]
    fn leaf_root_matches_any_node() {
        let scan = mk_scan();

        let binding = bind_tree(&Pattern::Leaf, &scan).expect("leaf should match scan");

        assert!(ptr::eq(binding.root(), &scan));
    }

    #[test]
    fn root_multi_leaf_matches_any_node_without_interiors() {
        let scan = mk_scan();

        let binding = bind_tree(&Pattern::MultiLeaf, &scan).expect("multileaf should match scan");

        assert!(ptr::eq(binding.root(), &scan));
        assert!(binding.interiors.is_empty());
    }

    #[test]
    fn op_two_level_filter_scan_matches_and_exposes_interiors() {
        let mut arena = ScalarArena::new();
        let predicate = bool_literal_scalar(&mut arena);
        let filter = mk_filter(predicate, mk_scan());
        let pattern = Pattern::Op {
            kind: OpKind::Filter,
            children: vec![Pattern::Op {
                kind: OpKind::Scan,
                children: vec![Pattern::MultiLeaf],
            }],
        };

        let binding = bind_tree(&pattern, &filter).expect("filter scan pattern should match");

        assert!(matches!(binding.op(0), Operator::LogicalFilter(_)));
        assert!(matches!(binding.op(1), Operator::LogicalScan(_)));
        assert!(ptr::eq(binding.node(1), filter.child(0)));
    }

    #[test]
    fn non_matching_kind_returns_none() {
        let scan = mk_scan();
        let pattern = Pattern::Op {
            kind: OpKind::Filter,
            children: vec![Pattern::MultiLeaf],
        };

        assert!(bind_tree(&pattern, &scan).is_none());
    }

    #[test]
    fn exact_arity_mismatch_returns_none() {
        let mut arena = ScalarArena::new();
        let predicate = bool_literal_scalar(&mut arena);
        let filter = mk_filter(predicate, mk_scan());
        let pattern = Pattern::Op {
            kind: OpKind::Filter,
            children: vec![],
        };

        assert!(bind_tree(&pattern, &filter).is_none());
    }

    #[test]
    fn single_node_any_arity_via_multileaf() {
        let scan = mk_scan();
        let pattern = Pattern::Op {
            kind: OpKind::Scan,
            children: vec![Pattern::MultiLeaf],
        };

        let binding = bind_tree(&pattern, &scan).expect("scan with tail multileaf should match");

        assert!(matches!(binding.op(0), Operator::LogicalScan(_)));
    }

    #[test]
    fn tail_multi_leaf_fixed_prefix_shortage_returns_none() {
        let scan = mk_scan();
        let pattern = Pattern::Op {
            kind: OpKind::Scan,
            children: vec![Pattern::Leaf, Pattern::MultiLeaf],
        };

        assert!(bind_tree(&pattern, &scan).is_none());
    }

    #[test]
    fn nested_child_failure_returns_none() {
        let mut arena = ScalarArena::new();
        let predicate = bool_literal_scalar(&mut arena);
        let filter = mk_filter(predicate, mk_scan());
        let pattern = Pattern::Op {
            kind: OpKind::Filter,
            children: vec![Pattern::Op {
                kind: OpKind::Join,
                children: vec![Pattern::MultiLeaf],
            }],
        };

        assert!(bind_tree(&pattern, &filter).is_none());
    }

    #[test]
    fn non_tail_multi_leaf_rejected() {
        let join = mk_join(mk_scan(), mk_scan());
        let pattern = Pattern::Op {
            kind: OpKind::Join,
            children: vec![Pattern::MultiLeaf, Pattern::Leaf],
        };

        assert!(bind_tree(&pattern, &join).is_none());
    }

    #[derive(Clone, Copy)]
    enum Stop {
        Entry,
        Positive(u32),
        Finish,
    }

    struct Control {
        units: std::sync::Mutex<Vec<u32>>,
        stop: Option<(Stop, novarocks_type_contract::CompileControlError)>,
        fired: std::sync::atomic::AtomicBool,
    }
    impl novarocks_type_contract::PureCompileControl for Control {
        fn checkpoint(
            &self,
            phase: novarocks_type_contract::CompilePhase,
            units: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            use std::sync::atomic::Ordering;
            assert_eq!(phase, novarocks_type_contract::CompilePhase::Validate);
            let mut recorded = self.units.lock().unwrap();
            recorded.push(units);
            let total: u32 = recorded.iter().sum();
            if let Some((stop, error)) = self.stop.clone() {
                let hit = match stop {
                    Stop::Entry => units == 0,
                    Stop::Positive(limit) => units > 0 && total >= limit,
                    Stop::Finish => units > 0 && units < 256,
                };
                if hit && !self.fired.swap(true, Ordering::SeqCst) {
                    return Err(error);
                }
            }
            Ok(())
        }
    }
    impl Control {
        fn new(stop: Option<(Stop, novarocks_type_contract::CompileControlError)>) -> Self {
            Self {
                units: Default::default(),
                stop,
                fired: Default::default(),
            }
        }
    }

    fn wide_scan_union(count: usize) -> OptExpr {
        OptExpr::new(
            Operator::LogicalUnion(crate::optimizer::operator::UnionOp {
                all: true,
                output_columns: vec![],
                child_output_columns: vec![vec![]; count],
            }),
            (0..count).map(|_| mk_scan()).collect(),
        )
    }
    fn wide_scan_pattern(count: usize) -> Pattern {
        Pattern::Op {
            kind: OpKind::Union,
            children: (0..count)
                .map(|_| Pattern::Op {
                    kind: OpKind::Scan,
                    children: vec![Pattern::MultiLeaf],
                })
                .collect(),
        }
    }

    #[test]
    fn wide_actual_matching_preserves_preorder_and_borrowed_source_identity() {
        let tree = wide_scan_union(320);
        let pattern = wide_scan_pattern(320);
        let control = Control::new(None);
        let binding = super::bind_tree(&pattern, &tree, &control)
            .unwrap()
            .expect("all actual scan children match");
        assert!(ptr::eq(binding.root(), &tree));
        assert_eq!(binding.interiors.len(), 321);
        assert!(ptr::eq(binding.node(0), &tree));
        for (ordinal, child) in tree.children.iter().enumerate() {
            assert!(ptr::eq(binding.node(ordinal + 1), child));
            assert!(ptr::eq(binding.op(ordinal + 1), &child.op));
        }
        let units = control.units.lock().unwrap();
        assert_eq!(units[0], 0);
        assert_eq!(units.iter().sum::<u32>(), 1603);
        assert!(units.iter().all(|units| *units <= 256));
        drop(units);

        let mut wrong = wide_scan_pattern(320);
        let Pattern::Op { children, .. } = &mut wrong else {
            unreachable!()
        };
        children[319] = Pattern::Op {
            kind: OpKind::Filter,
            children: vec![Pattern::MultiLeaf],
        };
        let mismatch_control = Control::new(None);
        assert!(
            super::bind_tree(&wrong, &tree, &mismatch_control)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            mismatch_control.units.lock().unwrap().iter().sum::<u32>(),
            1601
        );
    }

    #[test]
    fn wildcard_and_multi_tail_do_not_traverse_captured_subtrees() {
        let tree = wide_scan_union(320);
        let owner = std::sync::Arc::new(Control::new(None));
        let weak = std::sync::Arc::downgrade(&owner);
        let binding = super::bind_tree(&Pattern::Leaf, &tree, owner.as_ref())
            .unwrap()
            .unwrap();
        assert!(binding.interiors.is_empty());
        assert_eq!(*owner.units.lock().unwrap(), vec![0, 1]);
        drop(owner);
        assert!(weak.upgrade().is_none());
        assert!(ptr::eq(binding.root(), &tree));

        let tail = Pattern::Op {
            kind: OpKind::Union,
            children: vec![
                Pattern::Op {
                    kind: OpKind::Scan,
                    children: vec![Pattern::MultiLeaf],
                },
                Pattern::MultiLeaf,
            ],
        };
        let control = Control::new(None);
        let binding = super::bind_tree(&tail, &tree, &control).unwrap().unwrap();
        assert_eq!(binding.interiors.len(), 2);
        assert!(ptr::eq(binding.node(1), &tree.children[0]));
        assert_eq!(*control.units.lock().unwrap(), vec![0, 8]);
    }

    #[test]
    fn tree_matching_preserves_one_shot_controls_at_entry_child_scan_and_interiors() {
        use crate::compiler::SqlCompileError;
        use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for stop in [
                Stop::Entry,
                Stop::Positive(256),
                Stop::Positive(512),
                Stop::Finish,
            ] {
                let tree = wide_scan_union(320);
                let pattern = wide_scan_pattern(320);
                let control = Control::new(Some((stop, error.clone())));
                let result = super::bind_tree(&pattern, &tree, &control);
                assert!(
                    matches!(result, Err(failure) if failure == SqlCompileError::from(error.clone()))
                );
                let before = control.units.lock().unwrap().clone();
                assert_eq!(before[0], 0);
                match stop {
                    Stop::Entry => assert_eq!(before, vec![0]),
                    Stop::Positive(limit) => {
                        assert_eq!(before.iter().sum::<u32>(), limit);
                        assert_eq!(before.last(), Some(&256));
                    }
                    Stop::Finish => assert_eq!(before.iter().sum::<u32>(), 1603),
                }
                // The same owner now permits progress. The first failure must
                // remain a typed failure, never a structural non-match.
                control.checkpoint(CompilePhase::Validate, 0).unwrap();
            }
            let tree = mk_scan();
            let mismatch = Pattern::Op {
                kind: OpKind::Filter,
                children: vec![],
            };
            for pattern in [&Pattern::Leaf, &mismatch] {
                let control = Control::new(Some((Stop::Entry, error.clone())));
                assert!(matches!(
                    super::bind_tree(pattern, &tree, &control),
                    Err(failure) if failure == SqlCompileError::from(error.clone())
                ));
                assert_eq!(*control.units.lock().unwrap(), vec![0]);
            }
        }
    }

    #[test]
    fn short_finish_failure_discards_both_match_and_non_match() {
        use crate::compiler::SqlCompileError;
        use novarocks_type_contract::CompileControlError;
        let tree = mk_scan();
        let mismatch = Pattern::Op {
            kind: OpKind::Filter,
            children: vec![],
        };
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for pattern in [&Pattern::Leaf, &mismatch] {
                let control = Control::new(Some((Stop::Finish, error.clone())));
                assert!(matches!(
                    super::bind_tree(pattern, &tree, &control),
                    Err(failure) if failure == SqlCompileError::from(error.clone())
                ));
                assert_eq!(*control.units.lock().unwrap(), vec![0, 1]);
            }
        }
    }
}
