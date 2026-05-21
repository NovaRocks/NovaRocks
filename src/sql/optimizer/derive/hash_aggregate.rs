//! HashAggregate output / required derivation. Five modes:
//! Single / Local / Global / DistinctLocal / DistinctGlobal.

use crate::sql::analysis::TypedExpr;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::{AggMode, PhysicalHashAggregateOp};
use crate::sql::optimizer::property::{DistributionSpec, OrderingSpec, PhysicalPropertySet};

use super::{DeriveOutput, DeriveRequired};

fn typed_expr_to_column_id(expr: &TypedExpr) -> Option<ColumnId> {
    match &expr.kind {
        crate::sql::analysis::ExprKind::ColumnRef { column_id, .. } => Some(*column_id),
        _ => None,
    }
}
fn typed_exprs_to_column_ids(exprs: &[TypedExpr]) -> Vec<ColumnId> {
    exprs.iter().filter_map(typed_expr_to_column_id).collect()
}

impl DeriveOutput for PhysicalHashAggregateOp {
    fn derive_output(&self, _children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
        // For Local / Single the group_by may contain non-ColumnRef
        // expressions (e.g. `GROUP BY mod(k, 2)`); reading the column id
        // from `output_columns` instead of the typed group_by exprs is the
        // only way to recover the planner-minted ColumnId for those
        // synthesised slots so the Local-emitted distribution matches what
        // Global asks for. Mirrors the G1 fix that previously lived in
        // search.rs::output_properties.
        let cols: Vec<ColumnId> = self
            .output_columns
            .iter()
            .take(self.group_by.len())
            .map(|oc| oc.column_id)
            .filter(|id| *id != ColumnId::UNSET)
            .collect();
        if cols.is_empty() {
            PhysicalPropertySet::gather()
        } else {
            PhysicalPropertySet {
                distribution: DistributionSpec::HashPartitioned(cols),
                ordering: OrderingSpec::Any,
            }
        }
    }
}

impl DeriveRequired for PhysicalHashAggregateOp {
    fn derive_required(
        &self,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        match self.mode {
            AggMode::Single => {
                if self.group_by.is_empty() {
                    vec![PhysicalPropertySet::gather()]
                } else {
                    vec![PhysicalPropertySet::any()]
                }
            }
            AggMode::Local => vec![PhysicalPropertySet::any()],
            AggMode::Global => {
                let cols = typed_exprs_to_column_ids(&self.group_by);
                if cols.is_empty() {
                    vec![PhysicalPropertySet::gather()]
                } else {
                    vec![PhysicalPropertySet {
                        distribution: DistributionSpec::HashPartitioned(cols),
                        ordering: OrderingSpec::Any,
                    }]
                }
            }
            AggMode::DistinctGlobal => {
                let cols = typed_exprs_to_column_ids(&self.group_by);
                if cols.is_empty() {
                    vec![PhysicalPropertySet::gather()]
                } else {
                    vec![PhysicalPropertySet {
                        distribution: DistributionSpec::HashPartitioned(cols),
                        ordering: OrderingSpec::Any,
                    }]
                }
            }
            AggMode::DistinctLocal => vec![PhysicalPropertySet::any()],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, OutputColumn, TypedExpr};
    use crate::sql::column_id::ColumnId;

    #[test]
    fn output_properties_hash_agg_with_group_by() {
        let col_ref = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(3),
                qualifier: Some("t".into()),
                column: "city".into(),
            },
            data_type: arrow::datatypes::DataType::Utf8,
            nullable: false,
        };
        let op = PhysicalHashAggregateOp {
            mode: AggMode::Single,
            group_by: vec![col_ref],
            aggregates: vec![],
            output_columns: vec![OutputColumn {
                column_id: ColumnId(3),
                name: "city".into(),
                data_type: arrow::datatypes::DataType::Utf8,
                nullable: false,
            }],
            is_merge: vec![],
        };
        let props = op.derive_output(&[]);
        match &props.distribution {
            DistributionSpec::HashPartitioned(cols) => {
                assert_eq!(cols.len(), 1);
                assert_eq!(cols[0], ColumnId(3));
            }
            other => panic!("expected HashPartitioned, got {:?}", other),
        }
    }

    #[test]
    fn distinct_global_requires_hash_on_group_by() {
        let col_g = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(4),
                qualifier: None,
                column: "g".into(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        };
        let col_x = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(5),
                qualifier: None,
                column: "x".into(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        };
        let op = PhysicalHashAggregateOp {
            mode: AggMode::DistinctGlobal,
            group_by: vec![col_g, col_x],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        };
        let reqs = op.derive_required(&PhysicalPropertySet::any(), 1);
        assert_eq!(reqs.len(), 1);
        match &reqs[0].distribution {
            DistributionSpec::HashPartitioned(cols) => {
                assert_eq!(cols.len(), 2, "Hash on both g and x");
            }
            other => panic!("expected HashPartitioned, got {:?}", other),
        }
    }

    #[test]
    fn distinct_local_requires_any() {
        let op = PhysicalHashAggregateOp {
            mode: AggMode::DistinctLocal,
            group_by: vec![],
            aggregates: vec![],
            output_columns: vec![],
            is_merge: vec![],
        };
        let reqs = op.derive_required(&PhysicalPropertySet::gather(), 1);
        assert_eq!(reqs.len(), 1);
        assert!(matches!(reqs[0].distribution, DistributionSpec::Any));
    }
}
