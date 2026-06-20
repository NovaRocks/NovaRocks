use crate::sql::optimizer::memo::{GroupId, MExpr, Memo};
use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::opt_expr::OptExpr;

pub(crate) fn opt_expr_to_memo(expr: &OptExpr, memo: &mut Memo) -> GroupId {
    let children: Vec<GroupId> = expr
        .children
        .iter()
        .map(|child| opt_expr_to_memo(child, memo))
        .collect();
    let mexpr = MExpr {
        id: memo.next_expr_id(),
        op: expr.op.clone(),
        children,
    };
    let group_id = memo.new_group(mexpr);
    if let Operator::LogicalCTEProduce(op) = &expr.op {
        memo.cte_produce_groups.insert(op.cte_id, group_id);
    }
    group_id
}
