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

use crate::sql::optimizer::memo::{GroupId, MExpr, Memo};
use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::pattern::{Pattern, op_kind};

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
) -> Vec<Binding> {
    // A `Leaf`/`MultiLeaf` ROOT yields exactly one binding that captures the
    // root expr with no interior nodes. This is the default-shim path: the
    // default `Rule::pattern()` is `Pattern::Leaf`, and `apply_bound` only
    // reads `root_mexpr` (never `op`/`children`), so an empty `interiors` is
    // sufficient. Returning an empty `Vec` here would silently disable every
    // un-migrated rule.
    let kind = match pattern {
        Pattern::Op { kind, .. } => *kind,
        Pattern::Leaf | Pattern::MultiLeaf => {
            return match memo
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
        }
    };

    let group = match memo.groups.get(group_id) {
        Some(g) => g,
        None => return Vec::new(),
    };
    let expr = match group.logical_exprs.get(expr_index) {
        Some(e) => e,
        None => return Vec::new(),
    };
    if op_kind(&expr.op) != Some(kind) {
        return Vec::new();
    }

    // Match the root expr against the pattern, producing the DFS-preorder list
    // of interior `BoundNode`s for every combination of child alternatives.
    let mut out = Vec::new();
    let mut truncated = false;
    for interiors in match_expr(pattern, memo, group_id, expr_index) {
        if out.len() >= MAX_BINDINGS_PER_PATTERN {
            truncated = true;
            break;
        }
        out.push(Binding {
            root: (group_id, expr_index),
            interiors,
        });
    }
    if truncated {
        BINDER_TRUNCATED.fetch_add(1, Ordering::Relaxed);
    }
    out.truncate(MAX_BINDINGS_PER_PATTERN);
    out
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
) -> Vec<Vec<BoundNode>> {
    let child_patterns = match pattern {
        Pattern::Op { children, .. } => children,
        // Unreachable: only `Op` patterns reach here.
        Pattern::Leaf | Pattern::MultiLeaf => return Vec::new(),
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
            return Vec::new();
        }
    } else if expr_children.len() != child_patterns.len() {
        return Vec::new();
    }

    // This node binds the expr and records ALL of its children groups (fixed
    // prefix plus any MultiLeaf tail).
    let this_node = BoundNode {
        group: group_id,
        expr_index,
        children: expr_children.clone(),
    };

    // Recurse only into child positions that are themselves interior `Op`
    // patterns. Leaf/MultiLeaf positions capture without descending, so they
    // contribute nothing to enumeration. For each such interior child position,
    // enumerate that child group's logical exprs (in insertion order) and, for
    // each kind-matching alternative, its sub-bindings.
    let mut interior_child_results: Vec<Vec<Vec<BoundNode>>> = Vec::new();
    for (pos, child_pat) in child_patterns.iter().enumerate() {
        let child_kind = match child_pat {
            Pattern::Op { kind, .. } => *kind,
            // Leaf / MultiLeaf: non-enumerating capture, no descent.
            Pattern::Leaf | Pattern::MultiLeaf => continue,
        };
        let child_group = expr_children[pos];
        let mut alts: Vec<Vec<BoundNode>> = Vec::new();
        // Enumerate alternatives in `logical_exprs` insertion order.
        for (cidx, cexpr) in memo.groups[child_group].logical_exprs.iter().enumerate() {
            if op_kind(&cexpr.op) != Some(child_kind) {
                continue;
            }
            for sub in match_expr(child_pat, memo, child_group, cidx) {
                alts.push(sub);
            }
        }
        // A required interior child position with zero matching alternatives
        // means this node has no binding at all.
        if alts.is_empty() {
            return Vec::new();
        }
        interior_child_results.push(alts);
    }

    // Cartesian product across the interior child positions. DFS pre-order:
    // this node first, then each interior child's sub-bindings left-to-right.
    // The product is built so that the LAST (deepest/rightmost) interior child
    // varies fastest, preserving the insertion-order contract.
    let mut combos: Vec<Vec<BoundNode>> = vec![vec![this_node]];
    for alts in &interior_child_results {
        let mut next: Vec<Vec<BoundNode>> = Vec::with_capacity(combos.len() * alts.len());
        for prefix in &combos {
            for alt in alts {
                let mut combined = prefix.clone();
                combined.extend(alt.iter().cloned());
                next.push(combined);
            }
        }
        combos = next;
    }
    combos
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::common::JoinKind;
    use crate::sql::optimizer::memo::Memo;
    use crate::sql::optimizer::operator::TopNPhase;
    use crate::sql::optimizer::operator::{LogicalJoinOp, Operator, ScanOp, TopNOp, UnionOp};
    use crate::sql::optimizer::pattern::{OpKind, Pattern};

    // ----- inline construction helpers (do NOT depend on other test mods) ---

    /// Build an opaque "leaf" scan group and return its GroupId.
    fn mk_scan_group(memo: &mut Memo) -> GroupId {
        let expr = MExpr {
            id: memo.next_expr_id(),
            op: Operator::LogicalScan(ScanOp {
                database: "db".into(),
                table: crate::sql::catalog::TableDef {
                    name: "t".into(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: crate::sql::catalog::ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 0,
                    },
                },
                alias: None,
                stats_ref: None,
                columns: vec![],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
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

        let bs = bind(&pattern, &memo, root_group, 0);
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
                table: crate::sql::catalog::TableDef {
                    name: "t2".into(),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: crate::sql::catalog::ScanSource::StarRocks {
                        db_id: 0,
                        table_id: 1,
                    },
                },
                alias: None,
                stats_ref: None,
                columns: vec![],
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
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

        let bs = bind(&pattern, &memo, root_group, 0);
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

        let bs = bind(&pattern, &memo, root_group, 0);
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

        let bs = bind(&pattern, &memo, topn, 0);
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
        let bs = bind(&Pattern::Leaf, &memo, g, 0);
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
        assert!(bind(&Pattern::Leaf, &memo, 999, 0).is_empty());
    }

    /// A `MultiLeaf` root behaves identically to a `Leaf` root: one binding.
    #[test]
    fn multileaf_root_yields_one_binding_for_shim() {
        let mut memo = Memo::new();
        let g = mk_scan_group(&mut memo);
        assert_eq!(bind(&Pattern::MultiLeaf, &memo, g, 0).len(), 1);
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
        let bs = bind(&pattern, &memo, root_group, 0);
        assert_eq!(bs.len(), 1);
        assert_eq!(
            take_truncation_count(),
            0,
            "truncation counter must be 0 for a normal (non-capped) bind"
        );
    }
}
