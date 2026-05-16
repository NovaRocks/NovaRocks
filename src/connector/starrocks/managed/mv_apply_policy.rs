use super::mv_shape::{AggregateFunctionKind, AggregateMvShape, IncrementalMvShape};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MvApplyPolicy {
    Incremental,
    FullRefresh { reason: String },
    Unsupported { reason: String },
}

pub(crate) fn apply_policy_for_change(
    shape: &IncrementalMvShape,
    has_inserts: bool,
    has_deletes: bool,
    row_identity_available: bool,
) -> MvApplyPolicy {
    match shape {
        IncrementalMvShape::ProjectionFilter(_) => {
            if has_deletes && !row_identity_available {
                MvApplyPolicy::FullRefresh {
                    reason: "projection/filter MV DELETE without base row identity requires full refresh"
                        .to_string(),
                }
            } else {
                MvApplyPolicy::Incremental
            }
        }
        IncrementalMvShape::Aggregate(aggregate) => {
            aggregate_policy(aggregate, has_inserts, has_deletes)
        }
        IncrementalMvShape::JoinProjectionFilter(_) => MvApplyPolicy::Unsupported {
            reason: "join projection/filter IMV refresh is not supported by the legacy managed MV apply policy".to_string(),
        },
    }
}

fn aggregate_policy(
    aggregate: &AggregateMvShape,
    _has_inserts: bool,
    has_deletes: bool,
) -> MvApplyPolicy {
    if has_deletes
        && aggregate.aggregates.iter().any(|call| {
            matches!(
                call.function,
                AggregateFunctionKind::Min | AggregateFunctionKind::Max
            )
        })
    {
        return MvApplyPolicy::FullRefresh {
            reason: "MIN/MAX aggregate cannot retract DELETE state incrementally".to_string(),
        };
    }
    MvApplyPolicy::Incremental
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::starrocks::managed::mv_shape::{
        AggregateCallShape, AggregateFunctionKind, AggregateInput, AggregateMvShape,
        IncrementalMvShape, JoinProjectionFilterMvShape, ProjectionFilterMvShape,
    };

    fn object_name() -> sqlparser::ast::ObjectName {
        sqlparser::ast::ObjectName(vec![
            sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new("ice")),
            sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new("ns")),
            sqlparser::ast::ObjectNamePart::Identifier(sqlparser::ast::Ident::new("orders")),
        ])
    }

    fn projection_shape() -> IncrementalMvShape {
        IncrementalMvShape::ProjectionFilter(ProjectionFilterMvShape {
            base_table: object_name(),
        })
    }

    fn aggregate_shape(function: AggregateFunctionKind) -> IncrementalMvShape {
        IncrementalMvShape::Aggregate(AggregateMvShape {
            base_table: object_name(),
            group_keys: Vec::new(),
            aggregates: vec![AggregateCallShape {
                output_name: "a".to_string(),
                function,
                input: AggregateInput::Star,
            }],
            visible_outputs: Vec::new(),
        })
    }

    fn join_shape() -> IncrementalMvShape {
        IncrementalMvShape::JoinProjectionFilter(JoinProjectionFilterMvShape {
            left_table: object_name(),
            left_alias: "l".to_string(),
            right_table: object_name(),
            right_alias: "r".to_string(),
            join_keys: Vec::new(),
        })
    }

    #[test]
    fn projection_delete_without_row_identity_falls_back_to_full_refresh() {
        assert_eq!(
            apply_policy_for_change(&projection_shape(), false, true, false),
            MvApplyPolicy::FullRefresh {
                reason:
                    "projection/filter MV DELETE without base row identity requires full refresh"
                        .to_string(),
            }
        );
    }

    #[test]
    fn projection_delete_with_row_identity_is_incremental() {
        assert_eq!(
            apply_policy_for_change(&projection_shape(), false, true, true),
            MvApplyPolicy::Incremental
        );
    }

    #[test]
    fn projection_mixed_insert_delete_remains_incremental_red_path() {
        assert_eq!(
            apply_policy_for_change(&projection_shape(), true, true, true),
            MvApplyPolicy::Incremental
        );
    }

    #[test]
    fn sum_delete_is_incremental() {
        assert_eq!(
            apply_policy_for_change(
                &aggregate_shape(AggregateFunctionKind::Sum),
                false,
                true,
                false,
            ),
            MvApplyPolicy::Incremental
        );
    }

    #[test]
    fn max_delete_falls_back_to_full_refresh() {
        assert_eq!(
            apply_policy_for_change(
                &aggregate_shape(AggregateFunctionKind::Max),
                false,
                true,
                false,
            ),
            MvApplyPolicy::FullRefresh {
                reason: "MIN/MAX aggregate cannot retract DELETE state incrementally".to_string(),
            }
        );
    }

    #[test]
    fn join_shape_is_unsupported_by_apply_policy() {
        let policy = apply_policy_for_change(&join_shape(), true, true, true);
        match policy {
            MvApplyPolicy::Unsupported { reason } => {
                assert!(reason.contains("join projection/filter IMV"), "reason={reason}");
            }
            other => panic!("expected unsupported policy, got {other:?}"),
        }
    }
}
