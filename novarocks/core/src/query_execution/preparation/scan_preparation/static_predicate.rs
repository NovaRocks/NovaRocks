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

//! Safe lowering from Core scan conjuncts to the connector-neutral static
//! predicate SPI. This module deliberately does not negotiate dispositions or
//! alter scan residuals; that boundary is owned by scan preparation.

use arrow::datatypes::DataType;
use novarocks_spi::connector::{
    ConnectorScalarType, ConnectorScalarValue, ConnectorStaticComparisonOp,
    ConnectorStaticPredicate, ConnectorStaticPredicateColumn, ConnectorStaticPredicateId,
    ConnectorStaticPredicateKind, MAX_CONNECTOR_STATIC_PREDICATES, validate_static_predicates,
};

use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
use crate::sql::planner::payload::PlanScanNode;

/// Lower independently numbered top-level scan conjuncts that have an exact,
/// type-preserving SPI representation. The returned ID is the source
/// predicate's original ordinal; rejected entries deliberately leave a gap.
///
/// This function does not split `AND`: `PlanScanNode::predicates` is already
/// the ordered top-level conjunct list, and one disposition must correspond to
/// exactly one entry from that list.
pub(super) fn lower_static_connector_predicates(
    scan: &PlanScanNode,
    connector_schema_fields: &[&str],
) -> Vec<ConnectorStaticPredicate> {
    let mut lowered = Vec::new();
    for (ordinal, predicate) in scan.predicates.iter().enumerate() {
        if lowered.len() == MAX_CONNECTOR_STATIC_PREDICATES {
            break;
        }
        let Ok(ordinal) = u32::try_from(ordinal) else {
            break;
        };
        let Some(predicate) = lower_static_predicate(
            scan,
            connector_schema_fields,
            ConnectorStaticPredicateId(ordinal),
            predicate,
        ) else {
            continue;
        };

        // The SPI validator also owns cumulative literal-payload accounting.
        // If this one candidate would exceed a hard budget, retain it only in
        // Core's residual path and continue considering later zero/smaller
        // predicates.
        let mut candidate_set = lowered.clone();
        candidate_set.push(predicate.clone());
        if validate_static_predicates(&candidate_set).is_ok() {
            lowered.push(predicate);
        }
    }
    lowered
}

fn lower_static_predicate(
    scan: &PlanScanNode,
    connector_schema_fields: &[&str],
    id: ConnectorStaticPredicateId,
    predicate: &TypedExpr,
) -> Option<ConnectorStaticPredicate> {
    match &unnest(predicate).kind {
        ExprKind::BinaryOp { left, op, right } => {
            let (column, op, literal) =
                if let Some(column) = lower_column(scan, connector_schema_fields, left) {
                    (column, comparison_op(*op)?, right.as_ref())
                } else {
                    let column = lower_column(scan, connector_schema_fields, right)?;
                    (
                        column,
                        comparison_op(reverse_comparison(*op))?,
                        left.as_ref(),
                    )
                };
            Some(ConnectorStaticPredicate {
                id,
                column: column.column,
                kind: ConnectorStaticPredicateKind::Comparison {
                    op,
                    literal: lower_literal(literal, column.data_type)?,
                },
            })
        }
        ExprKind::IsNull { expr, negated } => {
            let column = lower_column(scan, connector_schema_fields, expr)?;
            Some(ConnectorStaticPredicate {
                id,
                column: column.column,
                kind: if *negated {
                    ConnectorStaticPredicateKind::IsNotNull
                } else {
                    ConnectorStaticPredicateKind::IsNull
                },
            })
        }
        ExprKind::InList {
            expr,
            list,
            negated,
        } if !negated && !list.is_empty() => {
            let column = lower_column(scan, connector_schema_fields, expr)?;
            let literals = list
                .iter()
                .map(|literal| lower_literal(literal, column.data_type))
                .collect::<Option<Vec<_>>>()?;
            Some(ConnectorStaticPredicate {
                id,
                column: column.column,
                kind: ConnectorStaticPredicateKind::In { literals },
            })
        }
        _ => None,
    }
}

#[derive(Clone)]
struct LoweredColumn {
    column: ConnectorStaticPredicateColumn,
    data_type: ConnectorScalarType,
}

fn lower_column(
    scan: &PlanScanNode,
    connector_schema_fields: &[&str],
    expr: &TypedExpr,
) -> Option<LoweredColumn> {
    let ExprKind::ColumnRef { column_id, .. } = &unnest(expr).kind else {
        return None;
    };
    if scan
        .variant_columns
        .iter()
        .any(|variant| variant.synthetic_column_id == *column_id)
    {
        return None;
    }

    // Map identity through the scan output, then resolve the physical table
    // position. Do not use a bare expression name: aliases and metadata names
    // are not provider schema identity.
    let output = scan
        .columns
        .iter()
        .find(|output| output.column_id == *column_id && !output.is_internal)?;
    if output.data_type != expr.data_type || output.nullable != expr.nullable {
        return None;
    }
    let source = scan
        .table
        .columns
        .iter()
        .find(|source| source.name.eq_ignore_ascii_case(&output.name))?;
    if source.data_type != output.data_type
        || source.nullable != output.nullable
        || source.logical_type.is_some()
    {
        return None;
    }

    let data_type = static_data_type(&source.data_type)?;
    // The SPI ordinal addresses the connector's stable table schema, not the
    // analyzer-visible TableDef. The latter may hide or re-expose internal
    // columns (for example an MV apply key), changing its local order without
    // changing physical field identity.
    let mut ordinals = connector_schema_fields
        .iter()
        .enumerate()
        .filter(|(_, name)| name.eq_ignore_ascii_case(&source.name));
    let (ordinal, _) = ordinals.next()?;
    if ordinals.next().is_some() {
        return None;
    }
    Some(LoweredColumn {
        column: ConnectorStaticPredicateColumn {
            field_ordinal: u32::try_from(ordinal).ok()?,
            data_type,
            nullable: source.nullable,
        },
        data_type,
    })
}

fn lower_literal(
    expr: &TypedExpr,
    expected_type: ConnectorScalarType,
) -> Option<ConnectorScalarValue> {
    let expr = unnest(expr);
    if expr.nullable {
        return None;
    }
    if static_data_type(&expr.data_type)? != expected_type {
        return None;
    }
    let ExprKind::Literal(literal) = &expr.kind else {
        return None;
    };
    match (expected_type, literal) {
        (ConnectorScalarType::Boolean, LiteralValue::Bool(value)) => {
            Some(ConnectorScalarValue::Boolean(*value))
        }
        (ConnectorScalarType::Int8, LiteralValue::Int(value)) => {
            i8::try_from(*value).ok().map(ConnectorScalarValue::Int8)
        }
        (ConnectorScalarType::Int16, LiteralValue::Int(value)) => {
            i16::try_from(*value).ok().map(ConnectorScalarValue::Int16)
        }
        (ConnectorScalarType::Int32, LiteralValue::Int(value)) => {
            i32::try_from(*value).ok().map(ConnectorScalarValue::Int32)
        }
        (ConnectorScalarType::Int64, LiteralValue::Int(value)) => {
            Some(ConnectorScalarValue::Int64(*value))
        }
        (ConnectorScalarType::Date32, LiteralValue::Int(value)) => {
            i32::try_from(*value).ok().map(ConnectorScalarValue::Date32)
        }
        (ConnectorScalarType::TimestampMicros, LiteralValue::Int(value)) => {
            Some(ConnectorScalarValue::TimestampMicros(*value))
        }
        (ConnectorScalarType::TimestampNanos, LiteralValue::Int(value)) => {
            Some(ConnectorScalarValue::TimestampNanos(*value))
        }
        (ConnectorScalarType::Utf8, LiteralValue::String(value)) => {
            Some(ConnectorScalarValue::Utf8(value.clone()))
        }
        (ConnectorScalarType::Binary, LiteralValue::Binary(value)) => {
            Some(ConnectorScalarValue::Binary(value.clone()))
        }
        _ => None,
    }
}

fn static_data_type(data_type: &DataType) -> Option<ConnectorScalarType> {
    match data_type {
        DataType::Boolean => Some(ConnectorScalarType::Boolean),
        DataType::Int8 => Some(ConnectorScalarType::Int8),
        DataType::Int16 => Some(ConnectorScalarType::Int16),
        DataType::Int32 => Some(ConnectorScalarType::Int32),
        DataType::Int64 => Some(ConnectorScalarType::Int64),
        DataType::Date32 => Some(ConnectorScalarType::Date32),
        DataType::Binary => Some(ConnectorScalarType::Binary),
        // Core has no session collation contract and no cross-provider
        // timestamp semantic proof at this boundary. Preserve those
        // predicates as residuals even though the SPI vocabulary reserves
        // timezone-free timestamp and UTF-8 literals for a later producer.
        _ => None,
    }
}

fn comparison_op(op: BinOp) -> Option<ConnectorStaticComparisonOp> {
    match op {
        BinOp::Eq => Some(ConnectorStaticComparisonOp::Eq),
        BinOp::Ne => Some(ConnectorStaticComparisonOp::Ne),
        BinOp::Lt => Some(ConnectorStaticComparisonOp::Lt),
        BinOp::Le => Some(ConnectorStaticComparisonOp::Le),
        BinOp::Gt => Some(ConnectorStaticComparisonOp::Gt),
        BinOp::Ge => Some(ConnectorStaticComparisonOp::Ge),
        _ => None,
    }
}

fn reverse_comparison(op: BinOp) -> BinOp {
    match op {
        BinOp::Lt => BinOp::Gt,
        BinOp::Le => BinOp::Ge,
        BinOp::Gt => BinOp::Lt,
        BinOp::Ge => BinOp::Le,
        other => other,
    }
}

fn unnest(mut expr: &TypedExpr) -> &TypedExpr {
    while let ExprKind::Nested(inner) = &expr.kind {
        expr = inner;
    }
    expr
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::OutputColumn;
    use crate::sql::column_id::ColumnId;
    use crate::sql::planner::table::{ScanSource, TableDef};
    use arrow::datatypes::DataType;
    use novarocks_catalog::schema::ColumnDef;

    fn column(id: u32, name: &str, data_type: DataType, nullable: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId::new_for_test(id),
                qualifier: None,
                column: name.to_string(),
            },
            data_type,
            nullable,
        }
    }

    fn int(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn comparison(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
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

    fn scan(predicates: Vec<TypedExpr>) -> PlanScanNode {
        PlanScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "t".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "ignored".to_string(),
                        data_type: DataType::Boolean,
                        nullable: false,
                        write_default: None,
                        logical_type: None,
                    },
                    ColumnDef {
                        name: "value".to_string(),
                        data_type: DataType::Int32,
                        nullable: true,
                        write_default: None,
                        logical_type: None,
                    },
                ],
                iceberg_row_lineage_metadata_columns: vec![],
                source: ScanSource::ConnectorPinned,
            },
            alias: None,
            columns: vec![OutputColumn {
                column_id: ColumnId::new_for_test(7),
                name: "value".to_string(),
                data_type: DataType::Int32,
                nullable: true,
                is_internal: false,
            }],
            predicates,
            required_columns: None,
            variant_columns: vec![],
            mv_rewritten_from: None,
        }
    }

    #[test]
    fn lower_preserves_top_level_ordinals_and_reverses_literal_comparisons() {
        let value = || column(7, "value", DataType::Int32, true);
        let predicates = vec![
            comparison(value(), BinOp::Lt, int(10)),
            comparison(int(3), BinOp::Le, value()),
            TypedExpr {
                kind: ExprKind::InList {
                    expr: Box::new(value()),
                    list: vec![int(4), int(9)],
                    negated: false,
                },
                data_type: DataType::Boolean,
                nullable: true,
            },
        ];

        let actual = lower_static_connector_predicates(&scan(predicates), &["ignored", "value"]);
        assert_eq!(actual.len(), 3);
        assert_eq!(
            actual
                .iter()
                .map(|predicate| predicate.id)
                .collect::<Vec<_>>(),
            vec![
                ConnectorStaticPredicateId(0),
                ConnectorStaticPredicateId(1),
                ConnectorStaticPredicateId(2),
            ]
        );
        assert_eq!(actual[0].column.field_ordinal, 1);
        assert!(matches!(
            actual[0].kind,
            ConnectorStaticPredicateKind::Comparison {
                op: ConnectorStaticComparisonOp::Lt,
                literal: ConnectorScalarValue::Int32(10)
            }
        ));
        assert!(matches!(
            actual[1].kind,
            ConnectorStaticPredicateKind::Comparison {
                op: ConnectorStaticComparisonOp::Ge,
                literal: ConnectorScalarValue::Int32(3)
            }
        ));
        assert!(matches!(
            actual[2].kind,
            ConnectorStaticPredicateKind::In { ref literals }
                if literals == &vec![
                    ConnectorScalarValue::Int32(4),
                    ConnectorScalarValue::Int32(9),
                ]
        ));
    }

    #[test]
    fn lower_rejects_nullable_in_literal_and_keeps_the_original_ordinal_gap() {
        let value = || column(7, "value", DataType::Int32, true);
        let null = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Null),
            data_type: DataType::Int32,
            nullable: true,
        };
        let predicates = vec![
            TypedExpr {
                kind: ExprKind::InList {
                    expr: Box::new(value()),
                    list: vec![int(1), null],
                    negated: false,
                },
                data_type: DataType::Boolean,
                nullable: true,
            },
            TypedExpr {
                kind: ExprKind::IsNull {
                    expr: Box::new(value()),
                    negated: false,
                },
                data_type: DataType::Boolean,
                nullable: false,
            },
        ];

        let actual = lower_static_connector_predicates(&scan(predicates), &["ignored", "value"]);
        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0].id, ConnectorStaticPredicateId(1));
        assert!(matches!(
            actual[0].kind,
            ConnectorStaticPredicateKind::IsNull
        ));
    }

    #[test]
    fn lower_leaves_an_over_budget_in_list_unsent_but_keeps_later_safe_atoms() {
        let value = || column(7, "value", DataType::Int32, true);
        let too_many = (0..=1024).map(int).collect::<Vec<_>>();
        let predicates = vec![
            TypedExpr {
                kind: ExprKind::InList {
                    expr: Box::new(value()),
                    list: too_many,
                    negated: false,
                },
                data_type: DataType::Boolean,
                nullable: true,
            },
            TypedExpr {
                kind: ExprKind::IsNull {
                    expr: Box::new(value()),
                    negated: true,
                },
                data_type: DataType::Boolean,
                nullable: false,
            },
        ];

        let actual = lower_static_connector_predicates(&scan(predicates), &["ignored", "value"]);
        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0].id, ConnectorStaticPredicateId(1));
        assert!(matches!(
            actual[0].kind,
            ConnectorStaticPredicateKind::IsNotNull
        ));
    }

    #[test]
    fn lower_uses_connector_schema_ordinal_when_visible_columns_are_reordered() {
        let value = column(7, "value", DataType::Int32, true);
        let mut scan = scan(vec![comparison(value, BinOp::Eq, int(10))]);
        scan.table.columns.reverse();

        let actual = lower_static_connector_predicates(&scan, &["ignored", "value"]);

        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0].column.field_ordinal, 1);
    }
}
