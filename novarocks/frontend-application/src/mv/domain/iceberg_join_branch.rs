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

use novarocks_parser::ast;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotWindow {
    pub from: i64,
    pub to: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BranchSide {
    Delta(SnapshotWindow),
    Snapshot(i64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinDeltaBranchPlan {
    pub(crate) left_base: novarocks_types::naming::TableIdentity,
    pub(crate) right_base: novarocks_types::naming::TableIdentity,
    pub(crate) left: BranchSide,
    pub(crate) right: BranchSide,
}

pub fn plan_join_delta_branches(
    left_base: &novarocks_types::naming::TableIdentity,
    right_base: &novarocks_types::naming::TableIdentity,
    left_window: SnapshotWindow,
    right_window: SnapshotWindow,
    left_has_changes: bool,
    right_has_changes: bool,
) -> Vec<JoinDeltaBranchPlan> {
    let mut plans = Vec::new();
    if left_has_changes {
        plans.push(JoinDeltaBranchPlan {
            left_base: left_base.clone(),
            right_base: right_base.clone(),
            left: BranchSide::Delta(left_window),
            right: BranchSide::Snapshot(right_window.from),
        });
    }
    if right_has_changes {
        plans.push(JoinDeltaBranchPlan {
            left_base: left_base.clone(),
            right_base: right_base.clone(),
            left: BranchSide::Snapshot(left_window.to),
            right: BranchSide::Delta(right_window),
        });
    }
    plans
}

#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub(crate) fn is_append_only_join_delta_eligible(query: &ast::Query) -> bool {
    let ast::SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    let [from] = select.from.as_slice() else {
        return false;
    };
    let [join] = from.joins.as_slice() else {
        return false;
    };
    matches!(
        join.operator,
        ast::JoinOperator::Inner | ast::JoinOperator::InnerExplicit | ast::JoinOperator::Cross
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    fn base(name: &str) -> novarocks_types::naming::TableIdentity {
        novarocks_types::naming::TableIdentity {
            catalog: "ice".to_string(),
            namespace: "ns".to_string(),
            table: name.to_string(),
        }
    }
    #[test]
    fn both_changed_uses_telescoping_order() {
        let left = base("left");
        let right = base("right");
        let plans = plan_join_delta_branches(
            &left,
            &right,
            SnapshotWindow { from: 10, to: 11 },
            SnapshotWindow { from: 20, to: 21 },
            true,
            true,
        );
        assert_eq!(plans.len(), 2);
        assert_eq!(
            plans[0].left,
            BranchSide::Delta(SnapshotWindow { from: 10, to: 11 })
        );
        assert_eq!(plans[0].right, BranchSide::Snapshot(20));
        assert_eq!(plans[1].left, BranchSide::Snapshot(11));
        assert_eq!(
            plans[1].right,
            BranchSide::Delta(SnapshotWindow { from: 20, to: 21 })
        );
    }
    #[test]
    fn only_left_changed_has_one_branch() {
        let left = base("left");
        let right = base("right");
        let plans = plan_join_delta_branches(
            &left,
            &right,
            SnapshotWindow { from: 10, to: 11 },
            SnapshotWindow { from: 20, to: 20 },
            true,
            false,
        );
        assert_eq!(plans.len(), 1);
        assert_eq!(
            plans[0].left,
            BranchSide::Delta(SnapshotWindow { from: 10, to: 11 })
        );
        assert_eq!(plans[0].right, BranchSide::Snapshot(20));
    }
    #[test]
    fn join_delta_append_only_join_type_eligibility() {
        assert!(is_append_only_join_delta_eligible(&parse_query(
            "select l.id from ice.ns.left l join ice.ns.right r on l.id = r.id"
        )));
        assert!(is_append_only_join_delta_eligible(&parse_query(
            "select l.id from ice.ns.left l inner join ice.ns.right r on l.id = r.id"
        )));
        assert!(is_append_only_join_delta_eligible(&parse_query(
            "select l.id from ice.ns.left l cross join ice.ns.right r"
        )));

        assert!(!is_append_only_join_delta_eligible(&parse_query(
            "select l.id from ice.ns.left l left join ice.ns.right r on l.id = r.id"
        )));
        assert!(!is_append_only_join_delta_eligible(&parse_query(
            "select l.id from ice.ns.left l right join ice.ns.right r on l.id = r.id"
        )));
        assert!(!is_append_only_join_delta_eligible(&parse_query(
            "select l.id from ice.ns.left l full outer join ice.ns.right r on l.id = r.id"
        )));
    }
    fn parse_query(sql: &str) -> ast::Query {
        let statements = novarocks_parser::parse(sql).expect("parse");
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("expected query");
        };
        query.clone()
    }
}
