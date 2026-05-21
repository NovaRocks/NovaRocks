use super::mv_shape::IncrementalMvShape;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum MvApplyPolicy {
    Incremental,
    FullRefresh { reason: String },
    Unsupported { reason: String },
}

pub(crate) fn apply_policy_for_change(
    shape: &IncrementalMvShape,
    _has_inserts: bool,
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
        // IVM-P5 Phase 5: MIN/MAX no longer forces a full refresh on DELETE.
        // Phase 4 wired the detail-map state through merge / negate /
        // derive-visible, so DELETE deltas are handled incrementally.
        IncrementalMvShape::Aggregate(_) => MvApplyPolicy::Incremental,
        IncrementalMvShape::JoinProjectionFilter(_) => MvApplyPolicy::Unsupported {
            reason: "join projection/filter IMV refresh is not supported by the legacy managed MV apply policy".to_string(),
        },
        IncrementalMvShape::JoinAggregate(_) => MvApplyPolicy::Unsupported {
            reason:
                "join aggregate IMV refresh is not supported by the legacy managed MV apply policy"
                    .to_string(),
        },
    }
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
    fn max_delete_is_incremental_after_phase5() {
        // IVM-P5 Phase 5: DELETE on MIN/MAX no longer falls back to full
        // refresh. The detail-map state (Phase 2-4) merges DELETE deltas
        // incrementally via key-wise count subtraction.
        assert_eq!(
            apply_policy_for_change(
                &aggregate_shape(AggregateFunctionKind::Max),
                false,
                true,
                false,
            ),
            MvApplyPolicy::Incremental
        );
    }

    #[test]
    fn min_delete_is_incremental_after_phase5() {
        assert_eq!(
            apply_policy_for_change(
                &aggregate_shape(AggregateFunctionKind::Min),
                false,
                true,
                false,
            ),
            MvApplyPolicy::Incremental
        );
    }

    #[test]
    fn join_shape_is_unsupported_by_apply_policy() {
        let policy = apply_policy_for_change(&join_shape(), true, true, true);
        match policy {
            MvApplyPolicy::Unsupported { reason } => {
                assert!(
                    reason.contains("join projection/filter IMV"),
                    "reason={reason}"
                );
            }
            other => panic!("expected unsupported policy, got {other:?}"),
        }
    }
}
