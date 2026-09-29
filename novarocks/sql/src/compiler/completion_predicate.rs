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

//! Pure lowering of exact scan predicate occurrences into provider needs.
//!
//! A conjunct is offered only when its complete SQL meaning has an exact typed
//! tuple-domain representation. Every other conjunct remains an engine scan
//! residual. Column identity arrives as an exact `ColumnId -> projection
//! ordinal` map built while the provider projection is frozen; this module
//! never resolves a column by a display name.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use novarocks_spi::connector::read_stack::{
    Bound, ConnectorValue, ConnectorValueType, Constraint, Domain, MAX_CONNECTOR_VALUE_BYTES,
    Range, TupleDomain, ValueSet,
};

use super::completion::{ProviderPredicateOccurrenceId, ProviderReadPredicateNeed};
use crate::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
use crate::column_id::ColumnId;
use crate::planner::payload::PlanScanNode;

#[derive(Clone, Copy)]
pub(super) struct ProviderPredicateColumn {
    pub(super) ordinal: u32,
    pub(super) value_type: ConnectorValueType,
}

pub(super) fn lower_provider_predicates(
    scan: &PlanScanNode,
    columns: &BTreeMap<ColumnId, ProviderPredicateColumn>,
) -> Box<[ProviderReadPredicateNeed]> {
    scan.predicates
        .iter()
        .enumerate()
        .filter_map(|(ordinal, predicate)| {
            let occurrence = u32::try_from(ordinal).ok()?;
            let summary = lower_conjunct(scan, columns, predicate)?;
            Some(ProviderReadPredicateNeed::new(
                ProviderPredicateOccurrenceId::new(occurrence),
                Constraint::of_summary(summary),
            ))
        })
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

fn lower_conjunct(
    scan: &PlanScanNode,
    columns: &BTreeMap<ColumnId, ProviderPredicateColumn>,
    predicate: &TypedExpr,
) -> Option<TupleDomain<u32>> {
    let (column, domain) = lower_atom(scan, columns, predicate)?;
    TupleDomain::with_column_domains(BTreeMap::from([(column, domain)])).ok()
}

fn lower_atom(
    scan: &PlanScanNode,
    columns: &BTreeMap<ColumnId, ProviderPredicateColumn>,
    predicate: &TypedExpr,
) -> Option<(u32, Domain)> {
    match &unnest(predicate).kind {
        ExprKind::BinaryOp {
            left, op, right, ..
        } => {
            let (column, op, literal) = if let Some(column) = lower_column(scan, columns, left) {
                (column, *op, right.as_ref())
            } else {
                (
                    lower_column(scan, columns, right)?,
                    reverse_comparison(*op),
                    left.as_ref(),
                )
            };
            let value = lower_literal(literal, column.value_type)?;
            Some((
                column.ordinal,
                comparison_domain(column.value_type, op, value)?,
            ))
        }
        ExprKind::IsNull { expr, negated } => {
            let column = lower_column(scan, columns, expr)?;
            Some((
                column.ordinal,
                if *negated {
                    Domain::not_null(column.value_type)
                } else {
                    Domain::only_null(column.value_type)
                },
            ))
        }
        ExprKind::InList {
            expr,
            list,
            negated,
        } if !negated && !list.is_empty() => {
            let column = lower_column(scan, columns, expr)?;
            let values = list
                .iter()
                .map(|literal| lower_literal(literal, column.value_type))
                .collect::<Option<Vec<_>>>()?;
            let values = ValueSet::of_values(column.value_type, values).ok()?;
            Some((column.ordinal, Domain::new(values, false)))
        }
        _ => None,
    }
}

fn lower_column(
    scan: &PlanScanNode,
    columns: &BTreeMap<ColumnId, ProviderPredicateColumn>,
    expression: &TypedExpr,
) -> Option<ProviderPredicateColumn> {
    let ExprKind::ColumnRef { column_id, .. } = &unnest(expression).kind else {
        return None;
    };
    if scan
        .variant_columns
        .iter()
        .any(|column| column.synthetic_column_id == *column_id)
    {
        return None;
    }
    let output = scan
        .columns
        .iter()
        .find(|output| output.column_id == *column_id)?;
    if output.data_type != expression.data_type || output.nullable != expression.nullable {
        return None;
    }
    columns.get(column_id).copied()
}

fn comparison_domain(
    value_type: ConnectorValueType,
    op: BinOp,
    value: ConnectorValue,
) -> Option<Domain> {
    let values = match op {
        BinOp::Eq => ValueSet::of_values(value_type, vec![value]).ok()?,
        BinOp::Ne => ValueSet::of_ranges(
            value_type,
            vec![
                Range::try_new(
                    value_type,
                    Bound::Unbounded,
                    Bound::Exclusive(value.clone()),
                )
                .ok()?,
                Range::try_new(value_type, Bound::Exclusive(value), Bound::Unbounded).ok()?,
            ],
        )
        .ok()?,
        BinOp::Lt => single_range(value_type, Bound::Unbounded, Bound::Exclusive(value))?,
        BinOp::Le => single_range(value_type, Bound::Unbounded, Bound::Inclusive(value))?,
        BinOp::Gt => single_range(value_type, Bound::Exclusive(value), Bound::Unbounded)?,
        BinOp::Ge => single_range(value_type, Bound::Inclusive(value), Bound::Unbounded)?,
        BinOp::EqForNull
        | BinOp::Add
        | BinOp::Sub
        | BinOp::Mul
        | BinOp::Div
        | BinOp::Mod
        | BinOp::And
        | BinOp::Or => return None,
    };
    Some(Domain::new(values, false))
}

fn single_range(value_type: ConnectorValueType, low: Bound, high: Bound) -> Option<ValueSet> {
    ValueSet::of_ranges(
        value_type,
        vec![Range::try_new(value_type, low, high).ok()?],
    )
    .ok()
}

fn lower_literal(expression: &TypedExpr, expected: ConnectorValueType) -> Option<ConnectorValue> {
    let expression = unnest(expression);
    if expression.nullable || exact_predicate_value_type(&expression.data_type)? != expected {
        return None;
    }
    let ExprKind::Literal(literal) = &expression.kind else {
        return None;
    };
    let value = match (expected, literal) {
        (ConnectorValueType::Boolean, LiteralValue::Bool(value)) => ConnectorValue::Boolean(*value),
        (ConnectorValueType::TinyInt, LiteralValue::Int(value)) => {
            ConnectorValue::TinyInt(i8::try_from(*value).ok()?)
        }
        (ConnectorValueType::SmallInt, LiteralValue::Int(value)) => {
            ConnectorValue::SmallInt(i16::try_from(*value).ok()?)
        }
        (ConnectorValueType::Integer, LiteralValue::Int(value)) => {
            ConnectorValue::Integer(i32::try_from(*value).ok()?)
        }
        (ConnectorValueType::BigInt, LiteralValue::Int(value)) => ConnectorValue::BigInt(*value),
        (ConnectorValueType::Double, LiteralValue::Float(value)) if !value.is_nan() => {
            ConnectorValue::Double(*value)
        }
        (ConnectorValueType::Date, LiteralValue::Int(value)) => {
            ConnectorValue::Date(i32::try_from(*value).ok()?)
        }
        (ConnectorValueType::TimeMicros, LiteralValue::Int(value)) => {
            ConnectorValue::TimeMicros(*value)
        }
        (ConnectorValueType::TimestampMillis, LiteralValue::Int(value)) => {
            ConnectorValue::TimestampMillis(*value)
        }
        (ConnectorValueType::TimestampMicros, LiteralValue::Int(value)) => {
            ConnectorValue::TimestampMicros(*value)
        }
        (ConnectorValueType::TimestampNanos, LiteralValue::Int(value)) => {
            ConnectorValue::TimestampNanos(*value)
        }
        (ConnectorValueType::TimestampTzMicros, LiteralValue::Int(value)) => {
            ConnectorValue::TimestampTzMicros(*value)
        }
        (ConnectorValueType::TimestampTzNanos, LiteralValue::Int(value)) => {
            ConnectorValue::TimestampTzNanos(*value)
        }
        (ConnectorValueType::Varchar, LiteralValue::String(value)) => {
            ConnectorValue::Varchar(Arc::from(value.as_str()))
        }
        (ConnectorValueType::Varbinary, LiteralValue::Binary(value)) => {
            ConnectorValue::Varbinary(Arc::from(value.as_slice()))
        }
        _ => return None,
    };
    (value.payload_bytes() <= MAX_CONNECTOR_VALUE_BYTES).then_some(value)
}

fn exact_predicate_value_type(data_type: &DataType) -> Option<ConnectorValueType> {
    match data_type {
        DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, Some(zone))
            if !zone.eq_ignore_ascii_case("UTC") =>
        {
            None
        }
        DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Struct(_)
        | DataType::Map(_, _) => None,
        _ => super::completion::provider_connector_type_for_engine(data_type),
    }
}
const fn reverse_comparison(op: BinOp) -> BinOp {
    match op {
        BinOp::Lt => BinOp::Gt,
        BinOp::Le => BinOp::Ge,
        BinOp::Gt => BinOp::Lt,
        BinOp::Ge => BinOp::Le,
        BinOp::Eq
        | BinOp::Ne
        | BinOp::EqForNull
        | BinOp::Add
        | BinOp::Sub
        | BinOp::Mul
        | BinOp::Div
        | BinOp::Mod
        | BinOp::And
        | BinOp::Or => op,
    }
}

fn unnest(mut expression: &TypedExpr) -> &TypedExpr {
    while let ExprKind::Nested(inner) = &expression.kind {
        expression = inner;
    }
    expression
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroU64};

    use arrow::datatypes::DataType;

    use super::*;
    use crate::analysis::OutputColumn;
    use crate::binding::{SqlTableBindingId, SqlTableBindingScopeId};
    use crate::planner::table::{
        ScanSource, SqlScanKind, SqlScanSource, SqlTableIdentity, TableDef,
    };

    fn column() -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(1),
            name: "k".to_string(),
            data_type: DataType::Int32,
            nullable: false,
            is_internal: false,
        }
    }

    fn column_ref() -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(1),
                qualifier: None,
                column: "k".to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn integer(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    fn comparison(op: BinOp, left: TypedExpr, right: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    fn scan(predicates: Vec<TypedExpr>) -> PlanScanNode {
        let binding = SqlTableBindingId::new(
            SqlTableBindingScopeId::new(NonZeroU64::new(1).expect("scope")),
            NonZeroU32::new(1).expect("ordinal"),
        );
        PlanScanNode {
            database: "db".to_string(),
            table: TableDef {
                name: "t".to_string(),
                columns: vec![novarocks_types::schema::ColumnDef {
                    name: "k".to_string(),
                    data_type: DataType::Int32,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::Sql(SqlScanSource::new(
                    binding,
                    SqlTableIdentity {
                        catalog: "iceberg".to_string(),
                        namespace: "db".to_string(),
                        table: "t".to_string(),
                    },
                    SqlScanKind::Data {
                        version: crate::planner::table::SqlTableVersionSelector::Current,
                    },
                )),
            },
            alias: None,
            columns: vec![column()],
            predicates,
            required_columns: None,
            variant_columns: Vec::new(),
            mv_rewritten_from: None,
        }
    }

    fn columns() -> BTreeMap<ColumnId, ProviderPredicateColumn> {
        BTreeMap::from([(
            ColumnId(1),
            ProviderPredicateColumn {
                ordinal: 0,
                value_type: ConnectorValueType::Integer,
            },
        )])
    }

    #[test]
    fn exact_occurrences_keep_original_predicate_ordinals() {
        let unsupported = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Bool(true)),
            data_type: DataType::Boolean,
            nullable: false,
        };
        let scan = scan(vec![
            comparison(BinOp::Ge, column_ref(), integer(7)),
            unsupported,
            comparison(BinOp::Lt, integer(10), column_ref()),
        ]);

        let offered = lower_provider_predicates(&scan, &columns());

        assert_eq!(offered.len(), 2);
        assert_eq!(offered[0].occurrence().get(), 0);
        assert_eq!(offered[1].occurrence().get(), 2);
        assert_eq!(offered[0].constraint().summary().columns().count(), 1);
        assert!(offered[0].constraint().expression().is_constant_true());
    }

    #[test]
    fn column_identity_is_not_recovered_from_expression_name() {
        let expression = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(9),
                qualifier: None,
                column: "k".to_string(),
            },
            data_type: DataType::Int32,
            nullable: false,
        };
        let scan = scan(vec![comparison(BinOp::Eq, expression, integer(1))]);

        assert!(lower_provider_predicates(&scan, &columns()).is_empty());
    }

    #[test]
    fn negated_in_list_remains_an_engine_residual() {
        let predicate = TypedExpr {
            kind: ExprKind::InList {
                expr: Box::new(column_ref()),
                list: vec![integer(1), integer(2)],
                negated: true,
            },
            data_type: DataType::Boolean,
            nullable: true,
        };

        assert!(lower_provider_predicates(&scan(vec![predicate]), &columns()).is_empty());
    }
}
