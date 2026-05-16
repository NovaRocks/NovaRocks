#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotWindow {
    pub(crate) from: i64,
    pub(crate) to: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BranchSide {
    Delta(SnapshotWindow),
    Snapshot(i64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct JoinDeltaBranchPlan {
    pub(crate) left_base: crate::connector::starrocks::managed::model::IcebergTableRef,
    pub(crate) right_base: crate::connector::starrocks::managed::model::IcebergTableRef,
    pub(crate) left: BranchSide,
    pub(crate) right: BranchSide,
}

pub(crate) fn plan_join_delta_branches(
    left_base: &crate::connector::starrocks::managed::model::IcebergTableRef,
    right_base: &crate::connector::starrocks::managed::model::IcebergTableRef,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn base(name: &str) -> crate::connector::starrocks::managed::model::IcebergTableRef {
        crate::connector::starrocks::managed::model::IcebergTableRef {
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
}
