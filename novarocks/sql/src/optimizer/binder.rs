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

//! Eager Memo-side Binder over a declarative [`Pattern`] (G5 A2).
//!
//! The Binder enumerates ALL matches of a `Pattern` rooted at a specific
//! logical expression `(group_id, expr_index)` in the memo. The memo is a
//! "group → multiple equivalent logical exprs → group" graph: a group holds
//! several equivalent `MExpr`s, and each `MExpr` references its children by
//! `GroupId`.
//!
//! ## Enumeration model (the byte-identity contract)
//!
//! - **Eager**: [`bind`] returns every matching [`Binding`] in one shot.
//! - **`Leaf` / `MultiLeaf` never enumerate child-group alternatives.** They
//!   only CAPTURE the child group id(s); the binder does not descend into a
//!   leaf's group. This is the fanout cap — only interior `Op` pattern nodes
//!   iterate a child group's `logical_exprs`.
//! - **`MultiLeaf`** is a variable-arity trailing tail: an `Op` whose last
//!   child-pattern is `MultiLeaf` matches an expr with `>= fixed_children` and
//!   the bound node records ALL of the expr's children groups (the fixed
//!   prefix plus the tail). `MultiLeaf` is only valid as the single trailing
//!   child-pattern.
//! - **Order**: interior `Op` nodes are recorded in DFS pre-order (root =
//!   index 0). Within a child group, alternatives are enumerated in
//!   `logical_exprs` insertion order. Across multiple interior-`Op` children
//!   the binder takes the cartesian product, with the deepest/rightmost
//!   position varying fastest. This order is deterministic and
//!   insertion-order-faithful.
//! - **`Op` matches KIND only** (`op_kind(&expr.op) == Some(kind)`); field
//!   predicates live in the rule's `apply_bound`. Arity must match exactly
//!   unless the last child-pattern is `MultiLeaf`.
//! - A `Leaf`/`MultiLeaf` ROOT yields one binding capturing the root expr
//!   (with no interior nodes) — this is the default-shim path; only an `Op`
//!   root enumerates structure.
//!
//! ## `op_equal` and group-mint interleaving (I2 invariant)
//!
//! `op_equal` (used by Cascades dedup / plan output) compares `&Operator`
//! values via their `Debug` representation. It never sees `MExpr.id` — the
//! binder's group-mint interleaving (which perturbs ids) therefore cannot
//! affect golden outputs or dedup decisions. This is structurally guaranteed:
//! `op_equal` takes `&Operator`, not `&MExpr`.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::compiler::SqlCompileError;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

use crate::optimizer::memo::{GroupId, MExpr, Memo};
use crate::optimizer::operator::Operator;
use crate::optimizer::pattern::{Pattern, op_kind};

/// Process-global counter: incremented once each time [`bind`] truncates
/// results at [`MAX_BINDINGS_PER_PATTERN`].  Used for observability and tests.
static BINDER_TRUNCATED: AtomicU64 = AtomicU64::new(0);

/// Returns the number of times [`bind`] has hit the `MAX_BINDINGS_PER_PATTERN`
/// cap since the last call, and resets the counter to zero.
///
/// Intended for tests and observability tooling only.
#[allow(dead_code)] // used by tests / future tasks
pub(crate) fn take_truncation_count() -> u64 {
    BINDER_TRUNCATED.swap(0, Ordering::Relaxed)
}

/// Upper bound on the number of bindings a single `bind` call collects. Once
/// reached the binder stops collecting and increments [`BINDER_TRUNCATED`].
pub(crate) const MAX_BINDINGS_PER_PATTERN: usize = 1024;

// The extra real tuple certifies that the existing root-output cap truncates.
// For valid finite Memo/Pattern inputs, matching is ordered concatenation and
// cartesian product without cross-child filters. Their first N tuples depend
// only on the first N tuples of each operand. Later required children must
// still match: an empty operand annihilates even an already-full prefix.
// This bounds tuple multiplicity, not tuple bytes, pattern depth or host memory.
const MAX_INTERMEDIATE_BINDINGS: usize = MAX_BINDINGS_PER_PATTERN + 1;

/// One bound interior `Op` node: the concrete memo expression it matched plus
/// the group ids of all the children that expression references (the full set,
/// including any `MultiLeaf` tail).
#[derive(Clone, Debug)]
struct BoundNode {
    /// Group holding the matched expression.
    group: GroupId,
    /// Index of the matched expression within `group.logical_exprs`.
    expr_index: usize,
    /// Child group ids of the matched expression, in order.
    children: Vec<GroupId>,
}

/// A single complete match of a [`Pattern`] over the memo.
///
/// Interior `Op` pattern nodes are stored in a DFS pre-order `Vec`; the root
/// is index 0. `Leaf`/`MultiLeaf` positions are NOT interior nodes — their
/// captured child groups appear in the parent interior node's `children`.
#[derive(Clone, Debug)]
pub(crate) struct Binding {
    /// `(group_id, expr_index)` of the root expression this binding matched.
    pub root: (GroupId, usize),
    /// Interior `Op` nodes in DFS pre-order; `interiors[0]` is the root.
    interiors: Vec<BoundNode>,
}

impl Binding {
    /// The root memo expression of this binding.
    pub fn root_mexpr<'m>(&self, memo: &'m Memo) -> &'m MExpr {
        let (group, idx) = self.root;
        &memo.groups[group].logical_exprs[idx]
    }

    /// The operator of the `i`-th interior `Op` node (DFS pre-order; 0 = root).
    pub fn op<'m>(&self, memo: &'m Memo, i: usize) -> &'m Operator {
        let node = &self.interiors[i];
        &memo.groups[node.group].logical_exprs[node.expr_index].op
    }

    /// The child groups of the `i`-th interior `Op` node (DFS pre-order;
    /// 0 = root), including any `MultiLeaf` tail groups.
    pub fn children(&self, i: usize) -> &[GroupId] {
        &self.interiors[i].children
    }
}

/// Enumerate every match of `pattern` rooted at the logical expression
/// `(group_id, expr_index)`. Returns all bindings, capped at
/// [`MAX_BINDINGS_PER_PATTERN`].
pub(crate) fn bind(
    pattern: &Pattern,
    memo: &Memo,
    group_id: GroupId,
    expr_index: usize,
    control: &dyn PureCompileControl,
) -> Result<Vec<Binding>, SqlCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let (bindings, truncated) = bind_observed(pattern, memo, group_id, expr_index, &mut work)?;
    work.finish()?;
    if truncated {
        BINDER_TRUNCATED.fetch_add(1, Ordering::Relaxed);
    }
    Ok(bindings)
}

fn bind_observed(
    pattern: &Pattern,
    memo: &Memo,
    group_id: GroupId,
    expr_index: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(Vec<Binding>, bool), SqlCompileError> {
    // A `Leaf`/`MultiLeaf` ROOT yields exactly one binding that captures the
    // root expr with no interior nodes. This is the default-shim path: the
    // default `Rule::pattern()` is `Pattern::Leaf`, and `apply_bound` only
    // reads `root_mexpr` (never `op`/`children`), so an empty `interiors` is
    // sufficient. Returning an empty `Vec` here would silently disable every
    // un-migrated rule.
    let kind = match pattern {
        Pattern::Op { kind, .. } => *kind,
        Pattern::Leaf | Pattern::MultiLeaf => {
            let bindings = match memo
                .groups
                .get(group_id)
                .and_then(|g| g.logical_exprs.get(expr_index))
            {
                Some(_) => vec![Binding {
                    root: (group_id, expr_index),
                    interiors: Vec::new(),
                }],
                None => Vec::new(),
            };
            work.step()?;
            return Ok((bindings, false));
        }
    };

    let group = match memo.groups.get(group_id) {
        Some(g) => g,
        None => return Ok((Vec::new(), false)),
    };
    let expr = match group.logical_exprs.get(expr_index) {
        Some(e) => e,
        None => return Ok((Vec::new(), false)),
    };
    work.step()?;
    if op_kind(&expr.op) != Some(kind) {
        return Ok((Vec::new(), false));
    }

    // Match the root expr against the pattern, producing the DFS-preorder list
    // of interior `BoundNode`s for every combination of child alternatives.
    let mut out = Vec::new();
    let mut truncated = false;
    for interiors in match_expr(pattern, memo, group_id, expr_index, work)? {
        work.step()?;
        if out.len() >= MAX_BINDINGS_PER_PATTERN {
            truncated = true;
            break;
        }
        out.push(Binding {
            root: (group_id, expr_index),
            interiors,
        });
    }
    out.truncate(MAX_BINDINGS_PER_PATTERN);
    Ok((out, truncated))
}

/// Match an `Op` pattern against the expression at `(group_id, expr_index)`,
/// returning, for every combination of interior-child alternatives, the
/// DFS-preorder list of `BoundNode`s (this node first, then its interior
/// descendants left-to-right).
///
/// Precondition: `pattern` is `Pattern::Op` and the target expr's kind already
/// matches (verified by the caller / recursive child loop).
fn match_expr(
    pattern: &Pattern,
    memo: &Memo,
    group_id: GroupId,
    expr_index: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<Vec<BoundNode>>, SqlCompileError> {
    work.step()?;
    let child_patterns = match pattern {
        Pattern::Op { children, .. } => children,
        // Unreachable: only `Op` patterns reach here.
        Pattern::Leaf | Pattern::MultiLeaf => return Ok(Vec::new()),
    };
    let expr = &memo.groups[group_id].logical_exprs[expr_index];
    let expr_children = &expr.children;

    // Arity check. The last child-pattern may be `MultiLeaf`, which absorbs a
    // variable-arity trailing tail.
    let has_multileaf_tail = matches!(child_patterns.last(), Some(Pattern::MultiLeaf));
    if has_multileaf_tail {
        // `fixed` = child-patterns before the trailing MultiLeaf. The expr must
        // have at least that many children; the MultiLeaf captures the rest.
        let fixed = child_patterns.len() - 1;
        if expr_children.len() < fixed {
            return Ok(Vec::new());
        }
    } else if expr_children.len() != child_patterns.len() {
        return Ok(Vec::new());
    }

    // This node binds the expr and records ALL of its children groups (fixed
    // prefix plus any MultiLeaf tail).
    let mut captured_children = Vec::new();
    for &child in expr_children {
        captured_children.push(child);
        work.step()?;
    }
    let this_node = BoundNode {
        group: group_id,
        expr_index,
        children: captured_children,
    };

    // Recurse only into child positions that are themselves interior `Op`
    // patterns. Leaf/MultiLeaf positions capture without descending, so they
    // contribute nothing to enumeration. For each such interior child position,
    // enumerate that child group's logical exprs (in insertion order) and, for
    // each kind-matching alternative, its sub-bindings.
    let mut interior_child_results: Vec<Vec<Vec<BoundNode>>> = Vec::new();
    for (pos, child_pat) in child_patterns.iter().enumerate() {
        work.step()?;
        let child_kind = match child_pat {
            Pattern::Op { kind, .. } => *kind,
            // Leaf / MultiLeaf: non-enumerating capture, no descent.
            Pattern::Leaf | Pattern::MultiLeaf => continue,
        };
        let child_group = expr_children[pos];
        let mut alts: Vec<Vec<BoundNode>> = Vec::new();
        // Enumerate alternatives in `logical_exprs` insertion order.
        'alternatives: for (cidx, cexpr) in
            memo.groups[child_group].logical_exprs.iter().enumerate()
        {
            work.step()?;
            if op_kind(&cexpr.op) != Some(child_kind) {
                continue;
            }
            for sub in match_expr(child_pat, memo, child_group, cidx, work)? {
                alts.push(sub);
                work.step()?;
                if alts.len() == MAX_INTERMEDIATE_BINDINGS {
                    // Only this child's alternative enumeration is complete.
                    // Every remaining required child is still checked below.
                    break 'alternatives;
                }
            }
        }
        // A required interior child position with zero matching alternatives
        // means this node has no binding at all.
        if alts.is_empty() {
            return Ok(Vec::new());
        }
        interior_child_results.push(alts);
    }

    // Cartesian product across the interior child positions. DFS pre-order:
    // this node first, then each interior child's sub-bindings left-to-right.
    // The product is built so that the LAST (deepest/rightmost) interior child
    // varies fastest, preserving the insertion-order contract.
    let mut combos: Vec<Vec<BoundNode>> = vec![vec![this_node]];
    for alts in &interior_child_results {
        work.step()?;
        // Grow only as combinations are visited; do not reserve the entire
        // cartesian product before the caller can observe its work. Retain
        // only the same ordered prefix needed by the existing root-output cap;
        // variable-sized tuples still require independent host memory policy.
        let mut next: Vec<Vec<BoundNode>> = Vec::new();
        'prefixes: for prefix in &combos {
            work.step()?;
            for alt in alts {
                let mut combined = Vec::new();
                append_nodes_observed(&mut combined, prefix, work)?;
                append_nodes_observed(&mut combined, alt, work)?;
                next.push(combined);
                work.step()?;
                if next.len() == MAX_INTERMEDIATE_BINDINGS {
                    break 'prefixes;
                }
            }
        }
        combos = next;
    }
    Ok(combos)
}

fn append_nodes_observed(
    output: &mut Vec<BoundNode>,
    nodes: &[BoundNode],
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), SqlCompileError> {
    for node in nodes {
        let mut children = Vec::new();
        for &child in &node.children {
            children.push(child);
            work.step()?;
        }
        output.push(BoundNode {
            group: node.group,
            expr_index: node.expr_index,
            children,
        });
        work.step()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::JoinKind;
    use crate::optimizer::memo::Memo;
    use crate::optimizer::operator::TopNPhase;
    use crate::optimizer::operator::{LogicalJoinOp, Operator, ScanOp, TopNOp, UnionOp};
    use crate::optimizer::pattern::{OpKind, Pattern};

    fn fixture_control() -> &'static crate::compiler::SqlCompileControl {
        static CONTROL: std::sync::OnceLock<crate::compiler::SqlCompileControl> =
            std::sync::OnceLock::new();
        CONTROL.get_or_init(crate::compiler::SqlCompileControl::unbounded)
    }

    // ----- inline construction helpers (do NOT depend on other test mods) ---

    /// Build an opaque "leaf" scan group and return its GroupId.
    fn mk_scan_group(memo: &mut Memo) -> GroupId {
        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(ScanOp {
                database: "db".into(),
                table: crate::planner::table::TableDef {
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
            }),
            children: vec![],
        };
        memo.new_group(expr)
    }

    fn mk_join_mexpr(memo: &mut Memo, children: Vec<GroupId>) -> MExpr {
        MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalJoin(LogicalJoinOp {
                naa_author: None,
                join_type: JoinKind::Inner,
                condition: None,
            }),
            children,
        }
    }

    fn mk_join_group(memo: &mut Memo, children: Vec<GroupId>) -> GroupId {
        let expr = mk_join_mexpr(memo, children);
        memo.new_group(expr)
    }

    fn mk_union_group(memo: &mut Memo, branches: Vec<GroupId>) -> GroupId {
        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalUnion(UnionOp {
                all: true,
                output_columns: vec![],
                child_output_columns: branches.iter().map(|_| vec![]).collect(),
            }),
            children: branches,
        };
        memo.new_group(expr)
    }

    fn mk_topn_group(memo: &mut Memo, child: GroupId) -> GroupId {
        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalTopN(TopNOp {
                items: vec![],
                limit: Some(10),
                offset: Some(0),
                phase: TopNPhase::Final,
                is_split: false,
            }),
            children: vec![child],
        };
        memo.new_group(expr)
    }

    // ----- tests ------------------------------------------------------------

    /// (A⋈B)⋈C with a single inner-join alternative. Pattern
    /// `Op(Join,[Op(Join,[Leaf,Leaf]),Leaf])` binds exactly once and records
    /// the grandchildren groups in order.
    #[test]
    fn binds_two_level_join_grandchildren_in_order() {
        let mut memo = Memo::new();
        let a = mk_scan_group(&mut memo);
        let b = mk_scan_group(&mut memo);
        let c = mk_scan_group(&mut memo);
        let inner = mk_join_group(&mut memo, vec![a, b]);
        let root_expr = mk_join_mexpr(&mut memo, vec![inner, c]);
        let root_group = memo.new_group(root_expr);

        let pattern = Pattern::Op {
            kind: OpKind::Join,
            children: vec![
                Pattern::Op {
                    kind: OpKind::Join,
                    children: vec![Pattern::Leaf, Pattern::Leaf],
                },
                Pattern::Leaf,
            ],
        };

        let bs = bind(&pattern, &memo, root_group, 0, fixture_control()).expect("binding");
        assert_eq!(bs.len(), 1);
        // interior 0 = root join, children = [inner, c]
        assert_eq!(bs[0].children(0), &[inner, c]);
        // interior 1 = inner join (DFS pre-order), children = [a, b]
        assert_eq!(bs[0].children(1), &[a, b]);
    }

    /// `Leaf` must NOT enumerate a child group's multiple alternatives. A
    /// binary join over leaf children binds once even when one child group has
    /// two equivalent exprs.
    #[test]
    fn leaf_does_not_multiply_over_child_alternatives() {
        let mut memo = Memo::new();
        let a = mk_scan_group(&mut memo);
        let b = mk_scan_group(&mut memo);
        // Add a SECOND alternative expr into group `a`.
        let alt = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(ScanOp {
                database: "db".into(),
                table: crate::planner::table::TableDef {
                    name: "t2".into(),
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
            }),
            children: vec![],
        };
        memo.add_expr_to_group(a, alt);

        let root_expr = mk_join_mexpr(&mut memo, vec![a, b]);
        let root_group = memo.new_group(root_expr);

        let pattern = Pattern::Op {
            kind: OpKind::Join,
            children: vec![Pattern::Leaf, Pattern::Leaf],
        };

        let bs = bind(&pattern, &memo, root_group, 0, fixture_control()).expect("binding");
        assert_eq!(bs.len(), 1, "Leaf must not enumerate group a's two exprs");
        assert_eq!(bs[0].children(0), &[a, b]);
    }

    /// An interior `Op` child DOES enumerate its group's alternatives, in
    /// insertion order. The inner join group has two alternatives ([a,b] then
    /// [b,a]); the pattern binds twice with `bs[0]` carrying the first-inserted.
    #[test]
    fn interior_op_enumerates_alternatives_in_insertion_order() {
        let mut memo = Memo::new();
        let a = mk_scan_group(&mut memo);
        let b = mk_scan_group(&mut memo);
        let c = mk_scan_group(&mut memo);
        // Inner group: first alternative [a,b], then [b,a].
        let inner = mk_join_group(&mut memo, vec![a, b]);
        let inner_alt = mk_join_mexpr(&mut memo, vec![b, a]);
        memo.add_expr_to_group(inner, inner_alt);

        let root_expr = mk_join_mexpr(&mut memo, vec![inner, c]);
        let root_group = memo.new_group(root_expr);

        let pattern = Pattern::Op {
            kind: OpKind::Join,
            children: vec![
                Pattern::Op {
                    kind: OpKind::Join,
                    children: vec![Pattern::Leaf, Pattern::Leaf],
                },
                Pattern::Leaf,
            ],
        };

        let bs = bind(&pattern, &memo, root_group, 0, fixture_control()).expect("binding");
        assert_eq!(bs.len(), 2);
        // interior 1 = inner join; first binding carries the first-inserted alt.
        assert_eq!(bs[0].children(1), &[a, b]);
        assert_eq!(bs[1].children(1), &[b, a]);
    }

    /// `MultiLeaf` captures a variable-arity trailing tail: a Union with 3
    /// branches under a TopN. Pattern `Op(TopN,[Op(Union,[MultiLeaf])])` binds
    /// once and the Union interior node records all 3 branch groups.
    #[test]
    fn multileaf_binds_all_union_branches() {
        let mut memo = Memo::new();
        let b0 = mk_scan_group(&mut memo);
        let b1 = mk_scan_group(&mut memo);
        let b2 = mk_scan_group(&mut memo);
        let union = mk_union_group(&mut memo, vec![b0, b1, b2]);
        let topn = mk_topn_group(&mut memo, union);

        let pattern = Pattern::Op {
            kind: OpKind::TopN,
            children: vec![Pattern::Op {
                kind: OpKind::Union,
                children: vec![Pattern::MultiLeaf],
            }],
        };

        let bs = bind(&pattern, &memo, topn, 0, fixture_control()).expect("binding");
        assert_eq!(bs.len(), 1);
        // interior 0 = TopN (1 child group = union), interior 1 = Union.
        assert_eq!(bs[0].children(1), &[b0, b1, b2]);
    }

    /// A `Leaf` root pattern (the default `Rule::pattern()`) must yield EXACTLY
    /// one binding that captures the root expr, so the default `apply_bound`
    /// shim — which only reads `root_mexpr` — fires for un-migrated rules.
    #[test]
    fn leaf_root_yields_one_binding_for_shim() {
        let mut memo = Memo::new();
        let g = mk_scan_group(&mut memo);
        let bs = bind(&Pattern::Leaf, &memo, g, 0, fixture_control()).expect("binding");
        assert_eq!(
            bs.len(),
            1,
            "Leaf root must yield exactly one root binding for the shim"
        );
        assert!(matches!(
            bs[0].root_mexpr(&memo).op,
            Operator::LogicalScan(_)
        ));
        // out-of-range root → no binding
        assert!(
            bind(&Pattern::Leaf, &memo, 999, 0, fixture_control())
                .expect("binding")
                .is_empty()
        );
    }

    /// A `MultiLeaf` root behaves identically to a `Leaf` root: one binding.
    #[test]
    fn multileaf_root_yields_one_binding_for_shim() {
        let mut memo = Memo::new();
        let g = mk_scan_group(&mut memo);
        assert_eq!(
            bind(&Pattern::MultiLeaf, &memo, g, 0, fixture_control())
                .expect("binding")
                .len(),
            1
        );
    }

    /// Normal (non-truncating) bind must NOT increment the truncation counter.
    #[test]
    fn truncation_counter_not_incremented_on_normal_bind() {
        // Drain any count accumulated by other tests running in the same process.
        let _ = take_truncation_count();

        let mut memo = Memo::new();
        let a = mk_scan_group(&mut memo);
        let b = mk_scan_group(&mut memo);
        let root_expr = mk_join_mexpr(&mut memo, vec![a, b]);
        let root_group = memo.new_group(root_expr);

        let pattern = Pattern::Op {
            kind: OpKind::Join,
            children: vec![Pattern::Leaf, Pattern::Leaf],
        };
        let bs = bind(&pattern, &memo, root_group, 0, fixture_control()).expect("binding");
        assert_eq!(bs.len(), 1);
        assert_eq!(
            take_truncation_count(),
            0,
            "truncation counter must be 0 for a normal (non-capped) bind"
        );
    }

    #[derive(Clone, Copy)]
    enum Stop {
        Never,
        Entry,
        AtUnits(u32),
        Finish,
    }

    struct Control {
        error: novarocks_type_contract::CompileControlError,
        stop: Stop,
        observations: std::sync::Mutex<Vec<u32>>,
    }

    impl Control {
        fn new(error: novarocks_type_contract::CompileControlError, stop: Stop) -> Self {
            Self {
                error,
                stop,
                observations: std::sync::Mutex::default(),
            }
        }
    }

    impl PureCompileControl for Control {
        fn checkpoint(
            &self,
            phase: CompilePhase,
            units: u32,
        ) -> Result<(), novarocks_type_contract::CompileControlError> {
            assert_eq!(phase, CompilePhase::Validate);
            assert!(units <= novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK);
            let mut observations = self.observations.lock().expect("observations");
            observations.push(units);
            let stop = match self.stop {
                Stop::Never => false,
                Stop::Entry => true,
                Stop::AtUnits(limit) => observations.iter().sum::<u32>() >= limit,
                Stop::Finish => {
                    observations.len() > 1
                        && units < novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK
                }
            };
            if stop {
                Err(self.error.clone())
            } else {
                Ok(())
            }
        }
    }

    fn control_errors() -> [novarocks_type_contract::CompileControlError; 3] {
        use novarocks_type_contract::CompileControlError;
        [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ]
    }

    fn cartesian_fixture(left_count: usize, right_count: usize) -> (Memo, GroupId, Pattern) {
        assert!(left_count > 0 && right_count > 0);
        let mut memo = Memo::new();
        let a = mk_scan_group(&mut memo);
        let b = mk_scan_group(&mut memo);
        let c = mk_scan_group(&mut memo);
        let d = mk_scan_group(&mut memo);
        let left = mk_join_group(&mut memo, vec![a, b]);
        let right = mk_join_group(&mut memo, vec![c, d]);
        for index in 1..left_count {
            let children = if index % 2 == 0 {
                vec![a, b]
            } else {
                vec![b, a]
            };
            let alternative = mk_join_mexpr(&mut memo, children);
            memo.add_expr_to_group(left, alternative);
        }
        for index in 1..right_count {
            let children = if index % 2 == 0 {
                vec![c, d]
            } else {
                vec![d, c]
            };
            let alternative = mk_join_mexpr(&mut memo, children);
            memo.add_expr_to_group(right, alternative);
        }
        let root = mk_join_group(&mut memo, vec![left, right]);
        let interior = Pattern::Op {
            kind: OpKind::Join,
            children: vec![Pattern::Leaf, Pattern::Leaf],
        };
        let pattern = Pattern::Op {
            kind: OpKind::Join,
            children: vec![interior.clone(), interior],
        };
        (memo, root, pattern)
    }

    #[test]
    fn controlled_cartesian_binding_preserves_exact_memo_identity_and_order() {
        let (memo, root, pattern) = cartesian_fixture(16, 20);
        let [left, right] = memo.groups[root].logical_exprs[0].children.as_slice() else {
            panic!("binary root")
        };
        let control = Control::new(control_errors()[0].clone(), Stop::Never);
        let bindings = bind(&pattern, &memo, root, 0, &control).expect("bindings");
        assert_eq!(bindings.len(), 320);
        for (index, binding) in bindings.iter().enumerate() {
            let left_index = index / 20;
            let right_index = index % 20;
            assert_eq!(binding.root, (root, 0));
            assert_eq!(binding.interiors.len(), 3);
            assert_eq!(binding.interiors[0].group, root);
            assert_eq!(binding.interiors[0].expr_index, 0);
            assert_eq!(binding.interiors[1].group, *left);
            assert_eq!(binding.interiors[1].expr_index, left_index);
            assert_eq!(binding.interiors[2].group, *right);
            assert_eq!(binding.interiors[2].expr_index, right_index);
            assert_eq!(binding.children(0), &[*left, *right]);
            assert_eq!(
                binding.children(1),
                memo.groups[*left].logical_exprs[left_index].children
            );
            assert_eq!(
                binding.children(2),
                memo.groups[*right].logical_exprs[right_index].children
            );
            assert!(std::ptr::eq(
                binding.root_mexpr(&memo),
                &memo.groups[root].logical_exprs[0]
            ));
            assert!(std::ptr::eq(
                binding.op(&memo, 1),
                &memo.groups[*left].logical_exprs[left_index].op
            ));
            assert!(std::ptr::eq(
                binding.op(&memo, 2),
                &memo.groups[*right].logical_exprs[right_index].op
            ));
        }
        assert!(control.observations.lock().unwrap().contains(&256));
    }

    #[test]
    fn binding_control_entry_precedes_leaf_invalid_and_nonmatching_roots() {
        let (memo, root, pattern) = cartesian_fixture(2, 2);
        let nonmatching = Pattern::Op {
            kind: OpKind::TopN,
            children: vec![Pattern::Leaf],
        };
        for error in control_errors() {
            for (pattern, group) in [
                (&pattern, root),
                (&Pattern::Leaf, 999),
                (&nonmatching, root),
            ] {
                let control = Control::new(error.clone(), Stop::Entry);
                assert_eq!(
                    bind(pattern, &memo, group, 0, &control).unwrap_err(),
                    SqlCompileError::from(error.clone())
                );
                assert_eq!(*control.observations.lock().unwrap(), vec![0]);
            }
        }
    }

    #[test]
    fn recursive_enumeration_and_cartesian_work_preserve_three_control_errors() {
        let (memo, root, pattern) = cartesian_fixture(16, 20);
        for error in control_errors() {
            // The first quantum stops recursive alternative enumeration; 1024
            // units reaches real cartesian copies after both child groups were
            // matched. Neither stop can escape as an empty/partial match set.
            for limit in [256, 1024] {
                let control = Control::new(error.clone(), Stop::AtUnits(limit));
                assert_eq!(
                    bind(&pattern, &memo, root, 0, &control).unwrap_err(),
                    SqlCompileError::from(error.clone())
                );
                let observed = control.observations.lock().unwrap();
                assert_eq!(observed[0], 0);
                assert_eq!(observed.iter().sum::<u32>(), limit);
                assert!(observed[1..].iter().all(|&units| units == 256));
            }
        }
    }

    #[test]
    fn binding_finish_failure_discards_real_matches_and_stays_typed() {
        let (memo, root, pattern) = cartesian_fixture(16, 20);
        for error in control_errors() {
            let control = Control::new(error.clone(), Stop::Finish);
            assert_eq!(
                bind(&pattern, &memo, root, 0, &control).unwrap_err(),
                SqlCompileError::from(error)
            );
            let observed = control.observations.lock().unwrap();
            assert_eq!(observed[0], 0);
            assert!(observed.len() > 2);
            assert!(
                observed[1..observed.len() - 1]
                    .iter()
                    .all(|&units| units == 256)
            );
            assert!(*observed.last().unwrap() < 256);
        }
    }

    // Exercise the exact root-output/truncation implementation without
    // changing the process-global observability counter. This keeps these
    // cardinality tests independent of its existing parallel drain test.
    fn checked_bind_result(pattern: &Pattern, memo: &Memo, root: GroupId) -> (Vec<Binding>, bool) {
        let mut work = CompileCheckpoints::try_new(fixture_control(), CompilePhase::Validate)
            .expect("fixture control");
        let result = bind_observed(pattern, memo, root, 0, &mut work).expect("bindings");
        work.finish().expect("final control");
        result
    }

    fn assert_binary_prefix(
        bindings: &[Binding],
        memo: &Memo,
        root: GroupId,
        right_count: usize,
        interior_offset: usize,
    ) {
        let [left, right] = memo.groups[root].logical_exprs[0].children.as_slice() else {
            panic!("binary join fixture")
        };
        for (index, binding) in bindings.iter().enumerate() {
            let left_index = index / right_count;
            let right_index = index % right_count;
            let left_node = &binding.interiors[interior_offset + 1];
            let right_node = &binding.interiors[interior_offset + 2];
            assert_eq!((left_node.group, left_node.expr_index), (*left, left_index));
            assert_eq!(
                (right_node.group, right_node.expr_index),
                (*right, right_index)
            );
            assert_eq!(
                left_node.children,
                memo.groups[*left].logical_exprs[left_index].children
            );
            assert_eq!(
                right_node.children,
                memo.groups[*right].logical_exprs[right_index].children
            );
            assert!(std::ptr::eq(
                binding.op(memo, interior_offset + 1),
                &memo.groups[*left].logical_exprs[left_index].op,
            ));
            assert!(std::ptr::eq(
                binding.op(memo, interior_offset + 2),
                &memo.groups[*right].logical_exprs[right_index].op,
            ));
        }
    }

    #[test]
    fn intermediate_prefix_keeps_exact_1024_and_real_1025_root_threshold() {
        for (left_count, right_count) in [(16, 20), (32, 32), (41, 25), (33, 33), (1100, 2)] {
            let (memo, root, pattern) = cartesian_fixture(left_count, right_count);
            let total = left_count * right_count;
            let (bindings, truncated) = checked_bind_result(&pattern, &memo, root);
            assert_eq!(bindings.len(), total.min(MAX_BINDINGS_PER_PATTERN));
            assert_eq!(truncated, total > MAX_BINDINGS_PER_PATTERN);
            assert_binary_prefix(&bindings, &memo, root, right_count, 0);
            let mut work =
                CompileCheckpoints::try_new(fixture_control(), CompilePhase::Validate).unwrap();
            let actual_intermediate = match_expr(&pattern, &memo, root, 0, &mut work).unwrap();
            work.finish().unwrap();
            assert_eq!(
                actual_intermediate.len(),
                total.min(MAX_INTERMEDIATE_BINDINGS)
            );
        }
    }

    #[test]
    fn full_early_child_prefix_does_not_hide_a_late_required_empty_child() {
        let (mut memo, binary, _) = cartesian_fixture(1100, 2);
        let children = memo.groups[binary].logical_exprs[0].children.clone();
        let nonmatching = mk_scan_group(&mut memo);
        let root = mk_union_group(&mut memo, vec![children[0], children[1], nonmatching]);
        let join_pattern = Pattern::Op {
            kind: OpKind::Join,
            children: vec![Pattern::Leaf, Pattern::Leaf],
        };
        let pattern = Pattern::Op {
            kind: OpKind::Union,
            children: vec![join_pattern.clone(), join_pattern.clone(), join_pattern],
        };
        let (bindings, truncated) = checked_bind_result(&pattern, &memo, root);
        assert!(bindings.is_empty());
        assert!(!truncated, "a child prefix is not a root truncation proof");
    }

    #[test]
    fn three_child_cartesian_prefix_is_right_fast_after_each_intermediate_cap() {
        for (left_count, right_count, third_count) in [(2, 3, 4), (3, 20, 30), (1100, 2, 3)] {
            let (mut memo, binary, _) = cartesian_fixture(left_count, right_count);
            let children = memo.groups[binary].logical_exprs[0].children.clone();
            let third_inputs = memo.groups[children[0]].logical_exprs[0].children.clone();
            let third = mk_join_group(&mut memo, third_inputs.clone());
            for _ in 1..third_count {
                let alternative = mk_join_mexpr(&mut memo, third_inputs.clone());
                memo.add_expr_to_group(third, alternative);
            }
            let root = mk_union_group(&mut memo, vec![children[0], children[1], third]);
            let join_pattern = Pattern::Op {
                kind: OpKind::Join,
                children: vec![Pattern::Leaf, Pattern::Leaf],
            };
            let pattern = Pattern::Op {
                kind: OpKind::Union,
                children: vec![join_pattern.clone(), join_pattern.clone(), join_pattern],
            };
            let total = left_count * right_count * third_count;
            let (bindings, truncated) = checked_bind_result(&pattern, &memo, root);
            assert_eq!(bindings.len(), total.min(MAX_BINDINGS_PER_PATTERN));
            assert_eq!(truncated, total > MAX_BINDINGS_PER_PATTERN);
            for (index, binding) in bindings.iter().enumerate() {
                let actual = binding
                    .interiors
                    .iter()
                    .map(|node| (node.group, node.expr_index))
                    .collect::<Vec<_>>();
                assert_eq!(
                    actual,
                    vec![
                        (root, 0),
                        (children[0], index / (right_count * third_count)),
                        (children[1], (index / third_count) % right_count),
                        (third, index % third_count),
                    ]
                );
                assert_eq!(binding.children(0), &[children[0], children[1], third]);
                assert!(std::ptr::eq(
                    binding.root_mexpr(&memo),
                    &memo.groups[root].logical_exprs[0]
                ));
                for ordinal in 1..4 {
                    let node = &binding.interiors[ordinal];
                    assert_eq!(
                        binding.children(ordinal),
                        memo.groups[node.group].logical_exprs[node.expr_index].children
                    );
                    assert!(std::ptr::eq(
                        binding.op(&memo, ordinal),
                        &memo.groups[node.group].logical_exprs[node.expr_index].op
                    ));
                }
            }
        }
    }

    #[test]
    fn nested_join_prefix_and_multileaf_capture_keep_real_source_identities() {
        let (mut memo, inner, inner_pattern) = cartesian_fixture(1100, 2);
        let root = mk_topn_group(&mut memo, inner);
        let pattern = Pattern::Op {
            kind: OpKind::TopN,
            children: vec![inner_pattern],
        };
        let (bindings, truncated) = checked_bind_result(&pattern, &memo, root);
        assert_eq!(bindings.len(), MAX_BINDINGS_PER_PATTERN);
        assert!(truncated);
        assert_binary_prefix(&bindings, &memo, inner, 2, 1);
        for binding in &bindings {
            assert_eq!(binding.root, (root, 0));
            assert_eq!(binding.interiors.len(), 4);
            assert_eq!(
                (binding.interiors[1].group, binding.interiors[1].expr_index),
                (inner, 0)
            );
        }

        // MultiLeaf contributes one opaque capture, even when its tail has
        // more entries than the tuple-prefix cap. Every tail group is retained.
        let a = mk_scan_group(&mut memo);
        let b = mk_scan_group(&mut memo);
        let tail = (0..1100)
            .map(|index| if index % 2 == 0 { a } else { b })
            .collect::<Vec<_>>();
        let union = mk_union_group(&mut memo, tail.clone());
        let topn = mk_topn_group(&mut memo, union);
        let multileaf = Pattern::Op {
            kind: OpKind::TopN,
            children: vec![Pattern::Op {
                kind: OpKind::Union,
                children: vec![Pattern::MultiLeaf],
            }],
        };
        let (captured, truncated) = checked_bind_result(&multileaf, &memo, topn);
        assert_eq!(captured.len(), 1);
        assert!(!truncated);
        assert_eq!(captured[0].children(1), tail);
        assert!(std::ptr::eq(
            captured[0].op(&memo, 1),
            &memo.groups[union].logical_exprs[0].op
        ));

        // A fixed interior prefix still enumerates; the trailing opaque tail
        // neither multiplies that count nor changes the source ordering.
        let left = memo.groups[inner].logical_exprs[0].children[0];
        let union = mk_union_group(&mut memo, vec![left, a, b, a]);
        let mixed = Pattern::Op {
            kind: OpKind::Union,
            children: vec![
                Pattern::Op {
                    kind: OpKind::Join,
                    children: vec![Pattern::Leaf, Pattern::Leaf],
                },
                Pattern::MultiLeaf,
            ],
        };
        let (captured, truncated) = checked_bind_result(&mixed, &memo, union);
        assert_eq!(captured.len(), MAX_BINDINGS_PER_PATTERN);
        assert!(truncated);
        for (index, binding) in captured.iter().enumerate() {
            assert_eq!(binding.children(0), &[left, a, b, a]);
            assert_eq!(
                (binding.interiors[1].group, binding.interiors[1].expr_index),
                (left, index)
            );
        }
    }
}
