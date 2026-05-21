//! Window operator. Output Hash(partition_cols) iff all PARTITION BY entries
//! resolve to column-refs; else Any. Required Hash(partition_cols) or Gather.

use crate::sql::analysis::TypedExpr;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::operator::PhysicalWindowOp;
use crate::sql::optimizer::property::{DistributionSpec, OrderingSpec, PhysicalPropertySet};

use super::{DeriveOutput, DeriveRequired};

fn typed_expr_to_column_id(expr: &TypedExpr) -> Option<ColumnId> {
    match &expr.kind {
        crate::sql::analysis::ExprKind::ColumnRef { column_id, .. } => Some(*column_id),
        _ => None,
    }
}

impl DeriveOutput for PhysicalWindowOp {
    fn derive_output(&self, _children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
        let mut partition_cols = Vec::new();
        let mut all_columns = true;
        for we in &self.window_exprs {
            for pbe in &we.partition_by {
                if let Some(col) = typed_expr_to_column_id(pbe) {
                    if !partition_cols.contains(&col) {
                        partition_cols.push(col);
                    }
                } else {
                    all_columns = false;
                }
            }
        }
        if !all_columns || partition_cols.is_empty() {
            PhysicalPropertySet::any()
        } else {
            PhysicalPropertySet {
                distribution: DistributionSpec::HashPartitioned(partition_cols),
                ordering: OrderingSpec::Any,
            }
        }
    }
}

impl DeriveRequired for PhysicalWindowOp {
    fn derive_required(
        &self,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        let mut partition_cols = Vec::new();
        for we in &self.window_exprs {
            for pbe in &we.partition_by {
                if let Some(col) = typed_expr_to_column_id(pbe)
                    && !partition_cols.contains(&col)
                {
                    partition_cols.push(col);
                }
            }
        }
        if partition_cols.is_empty() {
            vec![PhysicalPropertySet::gather()]
        } else {
            vec![PhysicalPropertySet {
                distribution: DistributionSpec::HashPartitioned(partition_cols),
                ordering: OrderingSpec::Any,
            }]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::plan::WindowExpr;

    #[test]
    fn output_properties_window_propagates_partition_distribution() {
        let col_c0 = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(2),
                qualifier: None,
                column: "c0".into(),
            },
            data_type: arrow::datatypes::DataType::Int64,
            nullable: false,
        };
        let window_expr = WindowExpr {
            name: "max".into(),
            args: vec![],
            partition_by: vec![col_c0.clone()],
            order_by: vec![],
            window_frame: None,
            ignore_nulls: false,
            distinct: false,
            output_name: "win".into(),
            result_type: arrow::datatypes::DataType::Int64,
        };
        let op = PhysicalWindowOp {
            window_exprs: vec![window_expr],
            output_columns: vec![],
        };
        let props = op.derive_output(&[]);
        match &props.distribution {
            DistributionSpec::HashPartitioned(cols) => {
                assert_eq!(cols.len(), 1);
                assert_eq!(cols[0], ColumnId(2));
            }
            other => panic!("expected HashPartitioned([c0]), got {:?}", other),
        }
    }

    #[test]
    fn output_properties_window_without_partition_by_is_any() {
        let window_expr = WindowExpr {
            name: "row_number".into(),
            args: vec![],
            partition_by: vec![],
            order_by: vec![],
            window_frame: None,
            ignore_nulls: false,
            distinct: false,
            output_name: "win".into(),
            result_type: arrow::datatypes::DataType::Int64,
        };
        let op = PhysicalWindowOp {
            window_exprs: vec![window_expr],
            output_columns: vec![],
        };
        let props = op.derive_output(&[]);
        assert_eq!(props.distribution, DistributionSpec::Any);
    }
}
