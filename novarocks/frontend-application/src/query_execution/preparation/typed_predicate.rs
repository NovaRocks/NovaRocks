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

//! Lowering from SQL scan conjuncts to the typed connector predicate algebra.
//!
//! The result is one `TupleDomain` keyed by the connector's own column
//! handles, plus the ordinals of every conjunct that has no exact domain
//! representation. A conjunct is either represented exactly or left in
//! `residual_ordinals`: there is no partial, widened, or truncated domain,
//! because a domain that is weaker than the SQL predicate it replaced would
//! return rows the query must not see.
//!
//! Accepted shapes mirror `novarocks_sql::planning::query_execution`'s static
//! predicate lowering exactly -- `col <op> literal`, `literal <op> col`,
//! `col IS [NOT] NULL`, and non-negated `col IN (literals)` -- so the two
//! producers cannot disagree about which conjuncts a connector may see.
// Design: ADR-0123 (docs/adr/ADR-0123-task-update-watermark-retry-delivery.md)

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use novarocks_spi::connector::read_stack::{
    Bound, ConnectorReadColumnHandle, ConnectorValue, ConnectorValueType, Domain,
    MAX_CONNECTOR_DECIMAL_PRECISION, MAX_CONNECTOR_VALUE_BYTES, Range, TupleDomain, ValueSet,
};
use novarocks_sql::plan_read::{BinOp, ExprKind, LiteralValue, PlanScanNode, TypedExpr};

/// What one scan's ordered top-level conjunct list lowered to.
pub(crate) struct LoweredScanPredicate {
    /// The per-column conjunction that may be offered to the connector.
    pub(crate) summary: TupleDomain<ConnectorReadColumnHandle>,
    /// Ordinals into `PlanScanNode::predicates` the engine must still
    /// evaluate itself, in ascending order.
    pub(crate) residual_ordinals: Vec<usize>,
}

/// Lower the scan's ordered top-level conjuncts into one typed summary domain.
///
/// `bindings` and `value_types` are keyed by the scan output column name
/// exactly as `PlanScanNode::columns` spells it; the caller owns the
/// connector-side name resolution, so lookups here are exact and a name this
/// scan does not output simply leaves its conjunct residual.
///
/// This function does not split `AND`: `PlanScanNode::predicates` is already
/// the ordered top-level conjunct list, and one disposition must correspond to
/// exactly one entry in it.
pub(crate) fn lower_scan_predicates(
    scan: &PlanScanNode,
    bindings: &BTreeMap<String, ConnectorReadColumnHandle>,
    value_types: &BTreeMap<String, ConnectorValueType>,
) -> LoweredScanPredicate {
    let mut summary = TupleDomain::all();
    let mut residual_ordinals = Vec::new();
    for (ordinal, predicate) in scan.predicates.iter().enumerate() {
        let Some(conjunct) = lower_conjunct(scan, bindings, value_types, predicate) else {
            residual_ordinals.push(ordinal);
            continue;
        };
        // The wire bounds live in the SPI constructors: `intersect` rejects a
        // tuple domain that would exceed the column limit, and a bound that
        // cannot be ordered (a NaN literal reached through a column already in
        // the summary) fails here rather than silently taking a position. In
        // either case the conjunct stays engine work and the summary keeps the
        // exact value it had before this conjunct -- never a truncated domain.
        match summary.intersect(&conjunct) {
            Ok(intersected) => summary = intersected,
            Err(_) => residual_ordinals.push(ordinal),
        }
    }
    LoweredScanPredicate {
        summary,
        residual_ordinals,
    }
}

/// The exact typed counterpart of an engine column type.
///
/// Only lossless, order-preserving mappings are listed. A type with no exact
/// counterpart returns `None`: widening it, rounding it, or normalizing a time
/// zone would make the connector filter on a value the engine never asked for.
pub(crate) fn connector_value_type(data_type: &DataType) -> Option<ConnectorValueType> {
    match data_type {
        DataType::Boolean => Some(ConnectorValueType::Boolean),
        // Eight-bit columns exist in the engine and nowhere in Iceberg, so a
        // column of this type is always one the engine derived.
        DataType::Int8 => Some(ConnectorValueType::TinyInt),
        DataType::Int16 => Some(ConnectorValueType::SmallInt),
        DataType::Int32 => Some(ConnectorValueType::Integer),
        DataType::Int64 => Some(ConnectorValueType::BigInt),
        DataType::Float32 => Some(ConnectorValueType::Real),
        DataType::Float64 => Some(ConnectorValueType::Double),
        DataType::Decimal128(precision, scale) => {
            // Reject out-of-range parameters here so the wire encoder cannot
            // be handed a decimal type its decoder would refuse.
            if *precision == 0
                || *precision > MAX_CONNECTOR_DECIMAL_PRECISION
                || *scale < 0
                || *scale > *precision as i8
            {
                return None;
            }
            Some(ConnectorValueType::Decimal {
                precision: *precision,
                scale: *scale,
            })
        }
        DataType::Date32 => Some(ConnectorValueType::Date),
        DataType::Time64(TimeUnit::Microsecond) => Some(ConnectorValueType::TimeMicros),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Some(ConnectorValueType::TimestampMicros)
        }
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            Some(ConnectorValueType::TimestampMillis)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Some(ConnectorValueType::TimestampNanos),
        // A zoned timestamp is exact only when the engine already states UTC.
        // Any other zone would need a conversion the engine cannot prove, so
        // it has no typed counterpart at all.
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) if is_utc(zone) => {
            Some(ConnectorValueType::TimestampTzMicros)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, Some(zone)) if is_utc(zone) => {
            Some(ConnectorValueType::TimestampTzNanos)
        }
        DataType::Utf8 => Some(ConnectorValueType::Varchar),
        DataType::Binary => Some(ConnectorValueType::Varbinary),
        DataType::FixedSizeBinary(length) => u32::try_from(*length)
            .ok()
            .filter(|length| *length > 0)
            .map(|length| ConnectorValueType::Fixed { length }),
        _ => None,
    }
}

/// The typed counterpart of a scan output column's engine type.
///
/// An output column is not a predicate operand: it only has to be projected
/// and read. So a type with no comparable counterpart is typed
/// [`ConnectorValueType::NonComparable`] rather than refused, which states
/// exactly that no comparison is possible over it and nothing more. The
/// column's real shape stays where it already was, on the plan node's own
/// output column, and the fragment decoder agrees the two orders.
///
/// A type that is *scalar but inexact* — a zoned timestamp in a zone the
/// engine cannot convert, an out-of-range decimal — is still refused: calling
/// it non-comparable would hide a column the connector cannot read correctly
/// behind a word about comparison.
pub(crate) fn scan_output_value_type(data_type: &DataType) -> Option<ConnectorValueType> {
    if let Some(value_type) = connector_value_type(data_type) {
        return Some(value_type);
    }
    match data_type {
        DataType::Struct(_)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _)
        // The carrier of VARIANT and of any other value the engine keeps as an
        // opaque blob. Plain `Binary` stays VARBINARY: it is comparable.
        | DataType::LargeBinary => Some(ConnectorValueType::NonComparable),
        _ => None,
    }
}

fn is_utc(zone: &str) -> bool {
    zone.eq_ignore_ascii_case("UTC")
}

/// Lower one conjunct into a single-column tuple domain, or reject it.
fn lower_conjunct(
    scan: &PlanScanNode,
    bindings: &BTreeMap<String, ConnectorReadColumnHandle>,
    value_types: &BTreeMap<String, ConnectorValueType>,
    predicate: &TypedExpr,
) -> Option<TupleDomain<ConnectorReadColumnHandle>> {
    let (column, domain) = lower_atom(scan, bindings, value_types, predicate)?;
    // A one-column map is always inside the column bound; the caller's
    // `intersect` is what enforces the bound across conjuncts.
    TupleDomain::with_column_domains(BTreeMap::from([(column, domain)])).ok()
}

fn lower_atom(
    scan: &PlanScanNode,
    bindings: &BTreeMap<String, ConnectorReadColumnHandle>,
    value_types: &BTreeMap<String, ConnectorValueType>,
    predicate: &TypedExpr,
) -> Option<(ConnectorReadColumnHandle, Domain)> {
    match &unnest(predicate).kind {
        ExprKind::BinaryOp { left, op, right } => {
            let (column, op, literal) =
                if let Some(column) = lower_column(scan, bindings, value_types, left) {
                    (column, *op, right.as_ref())
                } else {
                    // `literal <op> col` states the same fact with the operands
                    // swapped, so the operator is reversed rather than dropped.
                    let column = lower_column(scan, bindings, value_types, right)?;
                    (column, reverse_comparison(*op), left.as_ref())
                };
            let value = lower_literal(literal, column.value_type)?;
            let domain = comparison_domain(column.value_type, op, value)?;
            Some((column.column, domain))
        }
        ExprKind::IsNull { expr, negated } => {
            let column = lower_column(scan, bindings, value_types, expr)?;
            let domain = if *negated {
                Domain::not_null(column.value_type)
            } else {
                Domain::only_null(column.value_type)
            };
            Some((column.column, domain))
        }
        // `NOT IN` is a negation over an unknown-valued set and an empty list
        // is not a value set at all; both stay engine work.
        ExprKind::InList {
            expr,
            list,
            negated,
        } if !negated && !list.is_empty() => {
            let column = lower_column(scan, bindings, value_types, expr)?;
            let values = list
                .iter()
                .map(|literal| lower_literal(literal, column.value_type))
                .collect::<Option<Vec<_>>>()?;
            // `of_values` rejects a list longer than the wire's discrete-value
            // budget. Rejecting keeps the conjunct residual; truncating would
            // silently narrow the query's own IN list.
            let values = ValueSet::of_values(column.value_type, values).ok()?;
            Some((column.column, Domain::new(values, false)))
        }
        _ => None,
    }
}

/// The domain a comparison against one non-null literal describes.
///
/// Every comparison excludes `NULL`, so `null_allowed` is always false here;
/// nullability only enters through `IS [NOT] NULL`.
fn comparison_domain(
    value_type: ConnectorValueType,
    op: BinOp,
    value: ConnectorValue,
) -> Option<Domain> {
    let values = match op {
        BinOp::Eq => ValueSet::of_values(value_type, vec![value]).ok()?,
        BinOp::Ne => {
            // `col <> v` is everything strictly below or above `v`. An
            // unorderable type has no such split, so `Range::try_new` rejects
            // it and the conjunct stays engine work.
            let below = Range::try_new(
                value_type,
                Bound::Unbounded,
                Bound::Exclusive(value.clone()),
            )
            .ok()?;
            let above =
                Range::try_new(value_type, Bound::Exclusive(value), Bound::Unbounded).ok()?;
            ValueSet::of_ranges(value_type, vec![below, above]).ok()?
        }
        BinOp::Lt => single_range(value_type, Bound::Unbounded, Bound::Exclusive(value))?,
        BinOp::Le => single_range(value_type, Bound::Unbounded, Bound::Inclusive(value))?,
        BinOp::Gt => single_range(value_type, Bound::Exclusive(value), Bound::Unbounded)?,
        BinOp::Ge => single_range(value_type, Bound::Inclusive(value), Bound::Unbounded)?,
        // `<=>` is null-safe equality: it is a different predicate from `=`
        // even against a non-null literal, and the SQL-side static lowering
        // rejects it too. Arithmetic and logical operators are not predicates.
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
    let range = Range::try_new(value_type, low, high).ok()?;
    ValueSet::of_ranges(value_type, vec![range]).ok()
}

/// One scan output column resolved to its connector handle and exact type.
struct LoweredColumn {
    column: ConnectorReadColumnHandle,
    value_type: ConnectorValueType,
}

fn lower_column(
    scan: &PlanScanNode,
    bindings: &BTreeMap<String, ConnectorReadColumnHandle>,
    value_types: &BTreeMap<String, ConnectorValueType>,
    expr: &TypedExpr,
) -> Option<LoweredColumn> {
    let ExprKind::ColumnRef { column_id, .. } = &unnest(expr).kind else {
        return None;
    };
    // A variant column is a synthetic projection over a source column, not a
    // provider column: its identity has no connector binding at all.
    if scan
        .variant_columns
        .iter()
        .any(|variant| variant.synthetic_column_id == *column_id)
    {
        return None;
    }

    // Map identity through the scan output rather than through the expression
    // name: aliases and metadata names are not provider schema identity.
    let output = scan
        .columns
        .iter()
        .find(|output| output.column_id == *column_id && !output.is_internal)?;
    if output.data_type != expr.data_type || output.nullable != expr.nullable {
        return None;
    }
    let column = bindings.get(&output.name)?;
    let value_type = *value_types.get(&output.name)?;
    Some(LoweredColumn {
        column: column.clone(),
        value_type,
    })
}

fn lower_literal(expr: &TypedExpr, expected: ConnectorValueType) -> Option<ConnectorValue> {
    let expr = unnest(expr);
    // A nullable literal position cannot be proven non-null, and `NULL` is not
    // a `ConnectorValue` at all: nullability lives in the `Domain`.
    if expr.nullable {
        return None;
    }
    // The literal's own exact type must be the column's exact type. There is no
    // implicit widening, unit change, or time-zone normalization here.
    if connector_value_type(&expr.data_type)? != expected {
        return None;
    }
    let ExprKind::Literal(literal) = &expr.kind else {
        return None;
    };
    let value = match expected {
        // A column with no comparable counterpart has no literal to compare
        // against, so no conjunct over it can be pushed down.
        ConnectorValueType::NonComparable => return None,
        ConnectorValueType::Boolean => match literal {
            LiteralValue::Bool(value) => ConnectorValue::Boolean(*value),
            _ => return None,
        },
        // An eight-bit column is engine-derived. The literal a query writes
        // against it is an ordinary integer literal, so it is accepted only
        // when it actually fits.
        ConnectorValueType::TinyInt => match literal {
            LiteralValue::Int(value) => ConnectorValue::TinyInt(i8::try_from(*value).ok()?),
            _ => return None,
        },
        ConnectorValueType::SmallInt => match literal {
            LiteralValue::Int(value) => ConnectorValue::SmallInt(i16::try_from(*value).ok()?),
            _ => return None,
        },
        ConnectorValueType::Integer => match literal {
            LiteralValue::Int(value) => ConnectorValue::Integer(i32::try_from(*value).ok()?),
            _ => return None,
        },
        ConnectorValueType::BigInt => match literal {
            LiteralValue::Int(value) => ConnectorValue::BigInt(*value),
            _ => return None,
        },
        ConnectorValueType::Double => match literal {
            LiteralValue::Float(value) => {
                // NaN has no position in a range, so a domain built from it
                // could not be intersected. Keep the predicate as engine work.
                if value.is_nan() {
                    return None;
                }
                ConnectorValue::Double(*value)
            }
            _ => return None,
        },
        ConnectorValueType::Date => match literal {
            LiteralValue::Int(value) => ConnectorValue::Date(i32::try_from(*value).ok()?),
            _ => return None,
        },
        ConnectorValueType::TimeMicros => match literal {
            LiteralValue::Int(value) => ConnectorValue::TimeMicros(*value),
            _ => return None,
        },
        ConnectorValueType::TimestampMicros => match literal {
            LiteralValue::Int(value) => ConnectorValue::TimestampMicros(*value),
            _ => return None,
        },
        ConnectorValueType::TimestampMillis => match literal {
            LiteralValue::Int(value) => ConnectorValue::TimestampMillis(*value),
            _ => return None,
        },
        ConnectorValueType::TimestampTzMicros => match literal {
            LiteralValue::Int(value) => ConnectorValue::TimestampTzMicros(*value),
            _ => return None,
        },
        ConnectorValueType::TimestampNanos => match literal {
            LiteralValue::Int(value) => ConnectorValue::TimestampNanos(*value),
            _ => return None,
        },
        ConnectorValueType::TimestampTzNanos => match literal {
            LiteralValue::Int(value) => ConnectorValue::TimestampTzNanos(*value),
            _ => return None,
        },
        ConnectorValueType::Varchar => match literal {
            LiteralValue::String(value) => ConnectorValue::Varchar(Arc::from(value.as_str())),
            _ => return None,
        },
        ConnectorValueType::Varbinary => match literal {
            LiteralValue::Binary(value) => ConnectorValue::Varbinary(Arc::from(value.as_slice())),
            _ => return None,
        },
        // No SQL literal reaches these exactly:
        // - `Real`: a SQL float literal is an f64, and narrowing it to f32
        //   rounds, so the connector would filter on a different value.
        // - `Decimal`: `LiteralValue::Decimal` is unparsed text; turning it
        //   into an unscaled i128 at the column's precision and scale is a
        //   conversion, not a reading of what the query stated.
        // - `Uuid` / `Fixed`: the SQL dialect has no literal of that type.
        ConnectorValueType::Real
        | ConnectorValueType::Decimal { .. }
        | ConnectorValueType::Uuid
        | ConnectorValueType::Fixed { .. } => return None,
    };
    // The wire caps one scalar. An oversized literal stays engine work rather
    // than crossing the boundary truncated.
    if value.payload_bytes() > MAX_CONNECTOR_VALUE_BYTES {
        return None;
    }
    Some(value)
}

/// The operator that states the same fact with the operands swapped.
const fn reverse_comparison(op: BinOp) -> BinOp {
    match op {
        BinOp::Lt => BinOp::Gt,
        BinOp::Le => BinOp::Ge,
        BinOp::Gt => BinOp::Lt,
        BinOp::Ge => BinOp::Le,
        // Symmetric or not a comparison at all; `comparison_domain` decides.
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

fn unnest(mut expr: &TypedExpr) -> &TypedExpr {
    while let ExprKind::Nested(inner) = &expr.kind {
        expr = inner;
    }
    expr
}

#[cfg(test)]
pub(super) mod test_support {
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_codec::connector_common::encode_connector_payload_message;
    use novarocks_proto_codec::connector_read::ValidatedColumnHandle;
    use novarocks_proto_models::connector_read as dto;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorCodecCategory, ConnectorCodecRevision,
        ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorInstanceId, ConnectorProviderId,
    };
    use novarocks_sql::plan_read::{
        BinOp, DistributedNodeKind, ExprKind, LiteralValue, OutputColumn, PlanScanNode, TypedExpr,
    };
    use novarocks_sql::test_support::{NativeScanFixture, native_scan_plan};

    use arrow::datatypes::DataType;
    use novarocks_sql::plan_read::ColumnId;

    /// A sealed scan whose `TableDef` is real; tests replace only the output
    /// columns and predicates, which are the inputs this lowering reads.
    pub(crate) fn base_scan() -> PlanScanNode {
        let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergAllColumns)
            .expect("sealed ordinary iceberg scan fixture");
        plan.fragments()
            .iter()
            .find_map(|fragment| match &fragment.root.payload {
                DistributedNodeKind::Scan(scan) => Some(scan.clone()),
                _ => None,
            })
            .expect("the ordinary iceberg fixture has exactly one scan")
    }

    pub(crate) fn scan(columns: Vec<OutputColumn>, predicates: Vec<TypedExpr>) -> PlanScanNode {
        let mut scan = base_scan();
        scan.columns = columns;
        scan.predicates = predicates;
        scan.variant_columns = Vec::new();
        scan.required_columns = None;
        scan
    }

    pub(crate) fn output(id: u32, name: &str, data_type: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: ColumnId(id),
            name: name.to_owned(),
            data_type,
            nullable,
            is_internal: false,
        }
    }

    pub(crate) fn column_ref(
        id: u32,
        name: &str,
        data_type: DataType,
        nullable: bool,
    ) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: name.to_owned(),
            },
            data_type,
            nullable,
        }
    }

    pub(crate) fn int_literal(value: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(value)),
            data_type: DataType::Int32,
            nullable: false,
        }
    }

    pub(crate) fn text_literal(value: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String(value.to_owned())),
            data_type: DataType::Utf8,
            nullable: false,
        }
    }

    pub(crate) fn binary(left: TypedExpr, op: BinOp, right: TypedExpr) -> TypedExpr {
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

    pub(crate) fn is_null(expr: TypedExpr, negated: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::IsNull {
                expr: Box::new(expr),
                negated,
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    pub(crate) fn in_list(expr: TypedExpr, list: Vec<TypedExpr>, negated: bool) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::InList {
                expr: Box::new(expr),
                list,
                negated,
            },
            data_type: DataType::Boolean,
            nullable: true,
        }
    }

    /// A validated provider-neutral column envelope. The lowering under test
    /// reads only its canonical bytes.
    pub(crate) fn column_handle(field_id: i32, name: &str) -> ValidatedColumnHandle {
        let payload = ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                ConnectorProviderId::parse("fixture").expect("provider id"),
                CatalogHandle::new(
                    ConnectorInstanceId::parse("typed_predicate_fixture").expect("instance id"),
                    CatalogVersion::from_bytes([1; 32]),
                ),
                ConnectorCodecCategory::ReadColumn,
                ConnectorCodecRevision::try_new(1).expect("codec revision"),
            ),
            bytes::Bytes::from(format!("{field_id}:{name}")),
        );
        ValidatedColumnHandle::parse(
            dto::ColumnHandle {
                provider_payload: Some(encode_connector_payload_message(&payload)),
            },
            FieldPath::root("column_handle"),
        )
        .expect("valid column handle")
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, TimeUnit};

    use super::*;

    #[test]
    fn only_lossless_connector_type_mappings_are_offered() {
        assert_eq!(
            connector_value_type(&DataType::Int64),
            Some(ConnectorValueType::BigInt)
        );
        assert_eq!(
            connector_value_type(&DataType::Int16),
            Some(ConnectorValueType::SmallInt)
        );
        assert_eq!(
            connector_value_type(&DataType::Timestamp(TimeUnit::Millisecond, None)),
            Some(ConnectorValueType::TimestampMillis)
        );
        assert_eq!(
            scan_output_value_type(&DataType::LargeBinary),
            Some(ConnectorValueType::NonComparable)
        );
    }
}
