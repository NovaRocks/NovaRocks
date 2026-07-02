pub(crate) const CHANGE_OP_DELETE: i32 = -1;
pub(crate) const CHANGE_OP_INSERT: i32 = 1;
pub(crate) const DATA_ROUTE_REUSE: i32 = 1;
pub(crate) const DATA_ROUTE_FRESH: i32 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ChangeStreamBranchKind {
    DeleteDv,
    ReuseData,
    FreshData,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ChangeStreamRouteKey {
    pub(crate) change_op: i32,
    pub(crate) data_route: Option<i32>,
}

impl ChangeStreamBranchKind {
    pub(crate) fn route_key(self) -> ChangeStreamRouteKey {
        match self {
            Self::DeleteDv => ChangeStreamRouteKey {
                change_op: CHANGE_OP_DELETE,
                data_route: None,
            },
            Self::ReuseData => ChangeStreamRouteKey {
                change_op: CHANGE_OP_INSERT,
                data_route: Some(DATA_ROUTE_REUSE),
            },
            Self::FreshData => ChangeStreamRouteKey {
                change_op: CHANGE_OP_INSERT,
                data_route: Some(DATA_ROUTE_FRESH),
            },
        }
    }
}

pub(crate) fn branch_kind_from_thrift(
    value: crate::thrift::data_sinks::TIcebergChangeStreamRouterBranchKind,
) -> Result<ChangeStreamBranchKind, String> {
    match value {
        crate::thrift::data_sinks::TIcebergChangeStreamRouterBranchKind::DELETE_DV => {
            Ok(ChangeStreamBranchKind::DeleteDv)
        }
        crate::thrift::data_sinks::TIcebergChangeStreamRouterBranchKind::REUSE_DATA => {
            Ok(ChangeStreamBranchKind::ReuseData)
        }
        crate::thrift::data_sinks::TIcebergChangeStreamRouterBranchKind::FRESH_DATA => {
            Ok(ChangeStreamBranchKind::FreshData)
        }
        _ => Err(format!(
            "unsupported Iceberg change-stream router branch kind {}",
            value.0
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_kind_maps_to_canonical_route_key() {
        assert_eq!(
            ChangeStreamBranchKind::DeleteDv.route_key(),
            ChangeStreamRouteKey {
                change_op: -1,
                data_route: None,
            }
        );
        assert_eq!(
            ChangeStreamBranchKind::ReuseData.route_key(),
            ChangeStreamRouteKey {
                change_op: 1,
                data_route: Some(1),
            }
        );
        assert_eq!(
            ChangeStreamBranchKind::FreshData.route_key(),
            ChangeStreamRouteKey {
                change_op: 1,
                data_route: Some(2),
            }
        );
    }

    #[test]
    fn from_thrift_accepts_known_branch_kinds() {
        assert_eq!(
            branch_kind_from_thrift(
                crate::thrift::data_sinks::TIcebergChangeStreamRouterBranchKind::DELETE_DV
            )
            .expect("DELETE_DV"),
            ChangeStreamBranchKind::DeleteDv
        );
        assert_eq!(
            branch_kind_from_thrift(
                crate::thrift::data_sinks::TIcebergChangeStreamRouterBranchKind::REUSE_DATA
            )
            .expect("REUSE_DATA"),
            ChangeStreamBranchKind::ReuseData
        );
        assert_eq!(
            branch_kind_from_thrift(
                crate::thrift::data_sinks::TIcebergChangeStreamRouterBranchKind::FRESH_DATA
            )
            .expect("FRESH_DATA"),
            ChangeStreamBranchKind::FreshData
        );
    }

    #[test]
    fn from_thrift_rejects_unknown_branch_kind_without_panic() {
        let err = branch_kind_from_thrift(
            crate::thrift::data_sinks::TIcebergChangeStreamRouterBranchKind(99),
        )
        .expect_err("unknown branch kind");
        assert!(err.contains("unsupported Iceberg change-stream router branch kind 99"));
    }
}
