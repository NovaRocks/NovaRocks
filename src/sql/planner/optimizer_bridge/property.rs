//! Planner-owned bridges from analyzer ordering metadata to optimizer properties.

use crate::sql::analysis::{ExprKind, SortItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::property::{OrderingSpec, SortKey};

pub(crate) fn typed_expr_to_column_id(expr: &TypedExpr) -> Option<ColumnId> {
    match &expr.kind {
        ExprKind::ColumnRef { column_id, .. } if *column_id != ColumnId::UNSET => Some(*column_id),
        _ => None,
    }
}

pub(crate) fn ordering_spec_from_sort_items(items: &[SortItem]) -> OrderingSpec {
    let mut keys = Vec::with_capacity(items.len());
    for item in items {
        let Some(column) = typed_expr_to_column_id(&item.expr) else {
            return OrderingSpec::Any;
        };
        keys.push(SortKey {
            column,
            asc: item.asc,
            nulls_first: item.nulls_first,
        });
    }
    OrderingSpec::from_sort_keys(keys)
}

pub(crate) fn window_sort_items(
    partition_by: &[TypedExpr],
    order_by: &[SortItem],
) -> Vec<SortItem> {
    let mut items = Vec::with_capacity(partition_by.len() + order_by.len());
    for expr in partition_by {
        items.push(SortItem {
            expr: expr.clone(),
            asc: true,
            nulls_first: true,
        });
    }
    items.extend(order_by.iter().cloned());
    items
}

pub(crate) fn window_ordering_spec(
    partition_by: &[TypedExpr],
    order_by: &[SortItem],
) -> OrderingSpec {
    ordering_spec_from_sort_items(&window_sort_items(partition_by, order_by))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    fn column_expr(column_id: ColumnId, column: &str, data_type: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id,
                qualifier: None,
                column: column.to_string(),
            },
            data_type,
            nullable: false,
        }
    }

    #[test]
    fn ordering_spec_from_sort_items_returns_any_for_non_column_expr() {
        let spec = ordering_spec_from_sort_items(&[SortItem {
            expr: TypedExpr {
                kind: ExprKind::Literal(crate::sql::analysis::LiteralValue::Int(1)),
                data_type: DataType::Int64,
                nullable: false,
            },
            asc: true,
            nulls_first: true,
        }]);

        assert_eq!(spec, OrderingSpec::Any);
    }

    #[test]
    fn window_ordering_places_partition_keys_before_order_keys() {
        let partition = column_expr(ColumnId(10), "k", DataType::Int32);
        let order = column_expr(ColumnId(20), "ts", DataType::Int64);

        let spec = window_ordering_spec(
            &[partition],
            &[SortItem {
                expr: order,
                asc: false,
                nulls_first: false,
            }],
        );

        assert_eq!(
            spec,
            OrderingSpec::Required(vec![
                SortKey {
                    column: ColumnId(10),
                    asc: true,
                    nulls_first: true,
                },
                SortKey {
                    column: ColumnId(20),
                    asc: false,
                    nulls_first: false,
                },
            ])
        );
    }
}
