//! Helpers for `ApplyToWindow` (StarRocks WinMagic): physical table identity,
//! ColumnId -> (table, column) resolution, and structural expression equality
//! that ignores which scan instance a column came from.

use std::collections::HashMap;

use crate::sql::catalog::ScanSource;
use crate::sql::column_id::ColumnId;
use crate::sql::common::BinOp;
use crate::sql::optimizer::operator::{Operator, ScanOp};
use crate::sql::optimizer::opt_expr::OptExpr;
use crate::sql::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};

/// Physical identity of a scanned table. Two scans of the same physical table
/// (e.g. a self-join's two legs, or an outer table re-scanned in a subquery)
/// share one `TableIdentity`, even though their output ColumnIds differ.
#[allow(dead_code)]
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(super) enum TableIdentity {
    StarRocks {
        db_id: i64,
        table_id: i64,
    },
    Iceberg {
        catalog: String,
        namespace: String,
        table: String,
        table_uuid: Option<String>,
    },
}

impl TableIdentity {
    #[allow(dead_code)]
    pub(super) fn from_scan(scan: &ScanOp) -> Self {
        match &scan.table.source {
            ScanSource::StarRocks { db_id, table_id } => TableIdentity::StarRocks {
                db_id: *db_id,
                table_id: *table_id,
            },
            ScanSource::IcebergDataFiles { table, .. }
            | ScanSource::IcebergMetadataTable { table, .. }
            | ScanSource::IcebergDeltaTable { table, .. }
            | ScanSource::IcebergVersionTable { table, .. } => TableIdentity::Iceberg {
                catalog: table.catalog.clone(),
                namespace: table.namespace.clone(),
                table: table.table.clone(),
                table_uuid: table.table_uuid.clone(),
            },
            // MV-target-state scans never reach this in practice: ApplyToWindow's
            // operator whitelist (a later task) rejects any plan containing an
            // IcebergMvTargetState node before identity comparison runs. The __mv__
            // prefix is a belt-and-suspenders signal, not the sole guard — a same-named
            // user catalog colliding here is harmless because the whitelist fires first.
            ScanSource::IcebergMvTargetState(mv) => TableIdentity::Iceberg {
                catalog: format!("__mv__{}", mv.catalog),
                namespace: mv.database.clone(),
                table: mv.table.clone(),
                table_uuid: None,
            },
        }
    }
}

/// Collect the physical table identities of every `Scan` in `plan`, in
/// left-to-right order, WITH duplicates preserved (so callers can detect a
/// self-join / duplicate-table by comparing `Vec::len()` against the set size).
#[allow(dead_code)]
pub(super) fn collect_table_ids(plan: &OptExpr) -> Vec<TableIdentity> {
    let mut out = Vec::new();
    collect_table_ids_inner(plan, &mut out);
    out
}

fn collect_table_ids_inner(plan: &OptExpr, out: &mut Vec<TableIdentity>) {
    if let Operator::LogicalScan(s) = &plan.op {
        out.push(TableIdentity::from_scan(s));
        return;
    }

    if matches!(
        plan.op,
        Operator::LogicalJoin(_)
            | Operator::LogicalFilter(_)
            | Operator::LogicalProject(_)
            | Operator::LogicalAggregate(_)
            | Operator::LogicalSort(_)
            | Operator::LogicalWindow(_)
            | Operator::LogicalAssertOneRow(_)
            | Operator::LogicalApply(_)
    ) {
        for child in &plan.children {
            collect_table_ids_inner(child, out);
        }
    } else {
        // Nodes not reachable through a WinMagic-eligible plan (Limit, Union,
        // CTEAnchor/Produce/Consume, Values, Repeat, Decode, IMV markers, …)
        // contribute no Scan children. The operator whitelist (a later task)
        // rejects any plan containing them before these helpers are called.
    }
}

/// Build `ColumnId -> (TableIdentity, physical_column_name)` by walking every
/// `Scan` in `plan` and recording its output columns. A column produced by a
/// Project/Aggregate/Window (not a base scan column) is intentionally absent;
/// the predicate-identity check only compares base-table column references.
#[allow(dead_code)]
pub(super) fn collect_scan_column_map(
    plan: &OptExpr,
) -> HashMap<ColumnId, (TableIdentity, String)> {
    let mut map = HashMap::new();
    collect_scan_column_map_inner(plan, &mut map);
    map
}

fn collect_scan_column_map_inner(
    plan: &OptExpr,
    map: &mut HashMap<ColumnId, (TableIdentity, String)>,
) {
    if let Operator::LogicalScan(s) = &plan.op {
        let id = TableIdentity::from_scan(s);
        for c in &s.columns {
            map.insert(c.column_id, (id.clone(), c.name.clone()));
        }
        return;
    }

    if matches!(
        plan.op,
        Operator::LogicalJoin(_)
            | Operator::LogicalFilter(_)
            | Operator::LogicalProject(_)
            | Operator::LogicalAggregate(_)
            | Operator::LogicalSort(_)
            | Operator::LogicalWindow(_)
            | Operator::LogicalAssertOneRow(_)
            | Operator::LogicalApply(_)
    ) {
        for child in &plan.children {
            collect_scan_column_map_inner(child, map);
        }
    } else {
        // Nodes not reachable through a WinMagic-eligible plan (Limit, Union,
        // CTEAnchor/Produce/Consume, Values, Repeat, Decode, IMV markers, …)
        // contribute no Scan children. The operator whitelist (a later task)
        // rejects any plan containing them before these helpers are called.
    }
}

/// Structural equality of two expressions where a `ColumnRef` is compared by its
/// resolved physical `(TableIdentity, column_name)` rather than by `ColumnId`.
/// A ColumnRef whose id is absent from `map` only matches another ColumnRef with
/// the *same* `ColumnId`. Mirrors StarRocks `PredicateComparator.isIdentical`.
#[allow(dead_code)]
pub(super) fn expr_phys_eq(
    arena: &ScalarArena,
    a: ScalarId,
    b: ScalarId,
    map: &HashMap<ColumnId, (TableIdentity, String)>,
) -> bool {
    match (arena.node(a), arena.node(b)) {
        (ScalarNode::ColumnRef(ia), ScalarNode::ColumnRef(ib)) => {
            match (map.get(ia), map.get(ib)) {
                (Some(pa), Some(pb)) => pa == pb,
                _ => ia == ib,
            }
        }
        (
            ScalarNode::BinaryOp {
                left: la,
                op: oa,
                right: ra,
            },
            ScalarNode::BinaryOp {
                left: lb,
                op: ob,
                right: rb,
            },
        ) => {
            oa == ob
                && ((expr_phys_eq(arena, *la, *lb, map) && expr_phys_eq(arena, *ra, *rb, map))
                    || (matches!(oa, BinOp::Eq | BinOp::Ne)
                        && expr_phys_eq(arena, *la, *rb, map)
                        && expr_phys_eq(arena, *ra, *lb, map)))
        }
        (
            ScalarNode::FunctionCall {
                name: na,
                args: aa,
                distinct: da,
            },
            ScalarNode::FunctionCall {
                name: nb,
                args: ab,
                distinct: db,
            },
        ) => {
            na == nb
                && da == db
                && aa.len() == ab.len()
                && aa
                    .iter()
                    .zip(ab)
                    .all(|(x, y)| expr_phys_eq(arena, *x, *y, map))
        }
        (
            ScalarNode::IsNull {
                child: ea,
                negated: ga,
            },
            ScalarNode::IsNull {
                child: eb,
                negated: gb,
            },
        ) => ga == gb && expr_phys_eq(arena, *ea, *eb, map),
        (ScalarNode::Literal(la), ScalarNode::Literal(lb)) => la == lb,
        // Other / mixed kinds: conservative debug-structural equality.
        _ => arena.node(a) == arena.node(b),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use arrow::datatypes::DataType;

    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, JoinKind, LiteralValue, OutputColumn, TypedExpr};
    use crate::sql::catalog::{ScanSource, TableDef};
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::scalar::{self, ScalarArena};
    use crate::sql::planner::optimizer_bridge::plan::logical_plan_to_opt_expr;
    use crate::sql::planner::plan::{
        LogicalJoinNode, LogicalPlanNode, LogicalScanNode, PlanNodeKind,
    };

    fn make_scan(table_id: i64, cols: Vec<(ColumnId, &str)>) -> LogicalPlanNode {
        LogicalPlanNode::new(
            PlanNodeKind::Scan(LogicalScanNode {
                database: "default".to_string(),
                table: TableDef {
                    name: format!("t{table_id}"),
                    columns: vec![],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: ScanSource::StarRocks { db_id: 0, table_id },
                },
                alias: None,
                columns: cols
                    .into_iter()
                    .map(|(cid, name)| OutputColumn {
                        column_id: cid,
                        name: name.to_string(),
                        data_type: DataType::Int64,
                        nullable: false,
                        is_internal: false,
                    })
                    .collect(),
                predicates: vec![],
                required_columns: None,
                dict_columns: vec![],
                variant_columns: vec![],
                mv_rewritten_from: None,
            }),
            vec![],
            None,
        )
    }

    fn col_ref(cid: ColumnId, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: cid,
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn int_lit(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn binop(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn collect_table_ids(plan: &LogicalPlanNode) -> Vec<TableIdentity> {
        let mut arena = ScalarArena::new();
        let expr = logical_plan_to_opt_expr(plan, &mut arena);
        super::collect_table_ids(&expr)
    }

    fn collect_scan_column_map(
        plan: &LogicalPlanNode,
    ) -> HashMap<ColumnId, (TableIdentity, String)> {
        let mut arena = ScalarArena::new();
        let expr = logical_plan_to_opt_expr(plan, &mut arena);
        super::collect_scan_column_map(&expr)
    }

    fn expr_phys_eq(
        a: &TypedExpr,
        b: &TypedExpr,
        map: &HashMap<ColumnId, (TableIdentity, String)>,
    ) -> bool {
        let mut arena = ScalarArena::new();
        let a = crate::sql::planner::optimizer_bridge::scalar::intern_typed(&mut arena, a);
        let b = crate::sql::planner::optimizer_bridge::scalar::intern_typed(&mut arena, b);
        super::expr_phys_eq(&arena, a, b, map)
    }

    // -----------------------------------------------------------------
    // table_identity_from_starrocks_scan
    // -----------------------------------------------------------------
    #[test]
    fn table_identity_from_starrocks_scan() {
        let scan_node = LogicalScanNode {
            database: "default".to_string(),
            table: TableDef {
                name: "t".to_string(),
                columns: vec![],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::StarRocks {
                    db_id: 7,
                    table_id: 42,
                },
            },
            alias: None,
            columns: vec![],
            predicates: vec![],
            required_columns: None,
            dict_columns: vec![],
            variant_columns: vec![],
            mv_rewritten_from: None,
        };
        let plan = LogicalPlanNode::new(PlanNodeKind::Scan(scan_node), vec![], None);
        let mut arena = ScalarArena::new();
        let expr = logical_plan_to_opt_expr(&plan, &mut arena);
        let Operator::LogicalScan(scan) = &expr.op else {
            panic!("expected scan")
        };
        let id = TableIdentity::from_scan(scan);
        assert_eq!(
            id,
            TableIdentity::StarRocks {
                db_id: 7,
                table_id: 42
            }
        );
    }

    // -----------------------------------------------------------------
    // collect_table_ids_two_scans_under_join
    // -----------------------------------------------------------------
    #[test]
    fn collect_table_ids_two_scans_under_join() {
        let left = make_scan(1, vec![]);
        let right = make_scan(2, vec![]);
        let join = LogicalPlanNode::new(
            PlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Cross,
                condition: None,
            }),
            vec![left, right],
            None,
        );
        let ids = collect_table_ids(&join);
        assert_eq!(ids.len(), 2);
        let set: HashSet<_> = ids.iter().collect();
        assert_eq!(set.len(), 2);
        assert!(set.contains(&TableIdentity::StarRocks {
            db_id: 0,
            table_id: 1
        }));
        assert!(set.contains(&TableIdentity::StarRocks {
            db_id: 0,
            table_id: 2
        }));
    }

    // -----------------------------------------------------------------
    // collect_table_ids_self_join_detects_dup
    // -----------------------------------------------------------------
    #[test]
    fn collect_table_ids_self_join_detects_dup() {
        let left = make_scan(1, vec![]);
        let right = make_scan(1, vec![]);
        let join = LogicalPlanNode::new(
            PlanNodeKind::Join(LogicalJoinNode {
                join_type: JoinKind::Cross,
                condition: None,
            }),
            vec![left, right],
            None,
        );
        let ids = collect_table_ids(&join);
        assert_eq!(ids.len(), 2, "dup-preserving: should have 2 entries");
        let set: HashSet<_> = ids.iter().collect();
        assert_eq!(set.len(), 1, "both entries map to the same physical table");
    }

    // -----------------------------------------------------------------
    // column_to_table_map_resolves_scan_column
    // -----------------------------------------------------------------
    #[test]
    fn column_to_table_map_resolves_scan_column() {
        let cid = ColumnId(3);
        let plan = make_scan(5, vec![(cid, "l_partkey")]);
        let map = collect_scan_column_map(&plan);
        let entry = map.get(&cid).expect("ColumnId(3) must be in the map");
        assert_eq!(
            entry.0,
            TableIdentity::StarRocks {
                db_id: 0,
                table_id: 5
            }
        );
        assert_eq!(entry.1, "l_partkey");
    }

    // -----------------------------------------------------------------
    // expr_phys_eq_same_physical_column_diff_instance
    // -----------------------------------------------------------------
    #[test]
    fn expr_phys_eq_same_physical_column_diff_instance() {
        // Two separate scan instances of the same physical table (table_id=5),
        // each producing "l_partkey" under a DIFFERENT ColumnId.
        let cid_a = ColumnId(10);
        let cid_b = ColumnId(20);

        let mut map: HashMap<ColumnId, (TableIdentity, String)> = HashMap::new();
        let identity = TableIdentity::StarRocks {
            db_id: 0,
            table_id: 5,
        };
        map.insert(cid_a, (identity.clone(), "l_partkey".to_string()));
        map.insert(cid_b, (identity.clone(), "l_partkey".to_string()));

        let expr_a = col_ref(cid_a, "l_partkey");
        let expr_b = col_ref(cid_b, "l_partkey");

        // Same physical (table, col) → equal.
        assert!(
            expr_phys_eq(&expr_a, &expr_b, &map),
            "same physical column from two scan instances must compare equal"
        );

        // Different physical column → not equal.
        let cid_c = ColumnId(30);
        map.insert(cid_c, (identity.clone(), "l_suppkey".to_string()));
        let expr_c = col_ref(cid_c, "l_suppkey");
        assert!(
            !expr_phys_eq(&expr_a, &expr_c, &map),
            "different physical columns must not compare equal"
        );

        // Id absent from the map → falls back to ColumnId equality.
        let cid_missing = ColumnId(99);
        let expr_missing_1 = col_ref(cid_missing, "x");
        let expr_missing_2 = col_ref(cid_missing, "x");
        assert!(
            expr_phys_eq(&expr_missing_1, &expr_missing_2, &map),
            "same ColumnId absent from map must equal itself"
        );

        let cid_missing2 = ColumnId(100);
        let expr_missing3 = col_ref(cid_missing2, "y");
        assert!(
            !expr_phys_eq(&expr_missing_1, &expr_missing3, &map),
            "different ColumnIds absent from map must not be equal"
        );
    }

    // -----------------------------------------------------------------
    // expr_phys_eq_binary_op_structural
    // -----------------------------------------------------------------
    #[test]
    fn expr_phys_eq_binary_op_structural() {
        let map: HashMap<ColumnId, (TableIdentity, String)> = HashMap::new();

        // a Eq b == a Eq b
        let lhs = binop(int_lit(5), BinOp::Eq, int_lit(5));
        let rhs = binop(int_lit(5), BinOp::Eq, int_lit(5));
        assert!(
            expr_phys_eq(&lhs, &rhs, &map),
            "structurally equal BinaryOp with equal literals must be equal"
        );

        // a Eq b != a Lt b
        let lt_expr = binop(int_lit(5), BinOp::Lt, int_lit(5));
        assert!(
            !expr_phys_eq(&lhs, &lt_expr, &map),
            "different op must be unequal"
        );

        // Int(5) == Int(5), Int(5) != Int(6)
        let l5 = int_lit(5);
        let l5b = int_lit(5);
        let l6 = int_lit(6);
        assert!(expr_phys_eq(&l5, &l5b, &map), "same literal must be equal");
        assert!(
            !expr_phys_eq(&l5, &l6, &map),
            "different literals must be unequal"
        );
    }

    // -----------------------------------------------------------------
    // expr_phys_eq_commutative_eq
    // -----------------------------------------------------------------
    #[test]
    fn expr_phys_eq_commutative_eq() {
        // Two physical columns: (table_id=1, "a") and (table_id=1, "b").
        let cid_a = ColumnId(10);
        let cid_b = ColumnId(20);

        let mut map: HashMap<ColumnId, (TableIdentity, String)> = HashMap::new();
        let id1 = TableIdentity::StarRocks {
            db_id: 0,
            table_id: 1,
        };
        map.insert(cid_a, (id1.clone(), "a".to_string()));
        map.insert(cid_b, (id1.clone(), "b".to_string()));

        let a = col_ref(cid_a, "a");
        let b = col_ref(cid_b, "b");

        // `a = b` must equal `b = a` (Eq is commutative).
        let a_eq_b = binop(a.clone(), BinOp::Eq, b.clone());
        let b_eq_a = binop(b.clone(), BinOp::Eq, a.clone());
        assert!(
            expr_phys_eq(&a_eq_b, &b_eq_a, &map),
            "Eq must be commutative: a = b should match b = a"
        );

        // `a != b` must equal `b != a` (Ne is commutative).
        let a_ne_b = binop(a.clone(), BinOp::Ne, b.clone());
        let b_ne_a = binop(b.clone(), BinOp::Ne, a.clone());
        assert!(
            expr_phys_eq(&a_ne_b, &b_ne_a, &map),
            "Ne must be commutative: a != b should match b != a"
        );

        // `a < b` must NOT equal `b < a` (Lt is not commutative).
        let a_lt_b = binop(a.clone(), BinOp::Lt, b.clone());
        let b_lt_a = binop(b.clone(), BinOp::Lt, a.clone());
        assert!(
            !expr_phys_eq(&a_lt_b, &b_lt_a, &map),
            "Lt must NOT be commutative: a < b should not match b < a"
        );
    }
}
