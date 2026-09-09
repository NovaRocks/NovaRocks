//! Predicate conversion between the generated DTOs and the SPI algebra.
//!
//! A `TupleDomain` is keyed by [`ValidatedColumnHandle`], a structurally
//! validated carrier over the closed column-handle oneof. Protocol never
//! interprets which provider a column belongs to; ordering is by canonical
//! bytes so a decoded predicate is deterministic.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_proto_models::connector_read as dto;
use novarocks_spi::connector::read_stack::{
    Bound, ConnectorExpression, ConnectorFunctionName, ConnectorValueType, Domain, Range,
    TupleDomain, ValueSet,
};
use novarocks_spi::connector::{ConnectorCodecCategory, ConnectorEncodedPayload};
use prost::Message;

use crate::{FieldPath, ProtocolError};

use super::value::{decode_value, decode_value_type, encode_value, encode_value_type};
use super::{
    MAX_EXPRESSION_DEPTH, MAX_EXPRESSION_NODES, MAX_NAME_BYTES, MAX_TUPLE_DOMAIN_COLUMNS,
    MAX_VALUE_SET_RANGES, bounded_text, inconsistent, invalid, invalid_enum, missing, out_of_range,
};

/// A structurally validated column handle.
///
/// Equality and ordering use the canonical encoding of the closed oneof, so
/// the same column is the same key regardless of who built the message, and no
/// provider interpretation is required to use it as a predicate key.
#[derive(Clone, Debug)]
pub struct ValidatedColumnHandle {
    raw: dto::ColumnHandle,
    provider_payload: ConnectorEncodedPayload,
    canonical: Arc<[u8]>,
}

impl ValidatedColumnHandle {
    pub fn parse(raw: dto::ColumnHandle, path: FieldPath) -> Result<Self, ProtocolError> {
        let provider_payload = crate::connector_common::decode_embedded_connector_payload(
            raw.provider_payload.as_ref(),
            ConnectorCodecCategory::ReadColumn,
            super::MAX_SPLIT_ENCODED_BYTES,
            path.field("provider_payload"),
        )?;
        let canonical = Arc::from(raw.encode_to_vec());
        Ok(Self {
            raw,
            provider_payload,
            canonical,
        })
    }

    pub const fn as_proto(&self) -> &dto::ColumnHandle {
        &self.raw
    }

    pub fn into_proto(self) -> dto::ColumnHandle {
        self.raw
    }

    /// The canonical bytes used for ordering and same-message comparison.
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical
    }

    pub const fn provider_payload(&self) -> &ConnectorEncodedPayload {
        &self.provider_payload
    }
}

impl PartialEq for ValidatedColumnHandle {
    fn eq(&self, other: &Self) -> bool {
        self.canonical == other.canonical
    }
}

impl Eq for ValidatedColumnHandle {}

impl PartialOrd for ValidatedColumnHandle {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ValidatedColumnHandle {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.canonical.cmp(&other.canonical)
    }
}

fn decode_bound(
    raw: &dto::Bound,
    value_type: ConnectorValueType,
    path: FieldPath,
) -> Result<Bound, ProtocolError> {
    let kind = dto::BoundKind::try_from(raw.kind)
        .map_err(|_| invalid_enum(path.clone().field("kind"), "unknown bound kind"))?;
    match kind {
        dto::BoundKind::Unspecified => Err(invalid_enum(
            path.field("kind"),
            "bound kind must be specified",
        )),
        dto::BoundKind::Unbounded => {
            if raw.value.is_some() {
                return Err(inconsistent(
                    path.field("value"),
                    "an unbounded bound must not carry a value",
                ));
            }
            Ok(Bound::Unbounded)
        }
        bounded => {
            let value = raw.value.as_ref().ok_or_else(|| {
                missing(
                    path.clone().field("value"),
                    "a bounded bound requires a value",
                )
            })?;
            let value = decode_value(value, value_type, path.field("value"))?;
            Ok(match bounded {
                dto::BoundKind::Inclusive => Bound::Inclusive(value),
                dto::BoundKind::Exclusive => Bound::Exclusive(value),
                dto::BoundKind::Unspecified | dto::BoundKind::Unbounded => {
                    unreachable!("handled above")
                }
            })
        }
    }
}

fn encode_bound(bound: &Bound) -> dto::Bound {
    match bound {
        Bound::Unbounded => dto::Bound {
            kind: dto::BoundKind::Unbounded as i32,
            value: None,
        },
        Bound::Inclusive(value) => dto::Bound {
            kind: dto::BoundKind::Inclusive as i32,
            value: Some(encode_value(value)),
        },
        Bound::Exclusive(value) => dto::Bound {
            kind: dto::BoundKind::Exclusive as i32,
            value: Some(encode_value(value)),
        },
    }
}

fn decode_value_set(raw: &dto::ValueSet, path: FieldPath) -> Result<ValueSet, ProtocolError> {
    let value_type = raw.value_type.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("value_type"),
            "value set requires its exact type",
        )
    })?;
    let value_type = decode_value_type(value_type, path.clone().field("value_type"))?;
    if raw.ranges.len() > MAX_VALUE_SET_RANGES {
        return Err(out_of_range(
            path.clone().field("ranges"),
            "value set range count exceeds the hard limit",
        ));
    }
    let mut ranges = Vec::with_capacity(raw.ranges.len());
    for (index, range) in raw.ranges.iter().enumerate() {
        let range_path = path.clone().field("ranges").index(index);
        let low = range.low.as_ref().ok_or_else(|| {
            missing(
                range_path.clone().field("low"),
                "range requires a low bound",
            )
        })?;
        let high = range.high.as_ref().ok_or_else(|| {
            missing(
                range_path.clone().field("high"),
                "range requires a high bound",
            )
        })?;
        let low = decode_bound(low, value_type, range_path.clone().field("low"))?;
        let high = decode_bound(high, value_type, range_path.clone().field("high"))?;
        ranges.push(
            Range::try_new(value_type, low, high)
                .map_err(|error| invalid(range_path, error.message().to_owned()))?,
        );
    }
    ValueSet::of_ranges(value_type, ranges)
        .map_err(|error| invalid(path.field("ranges"), error.message().to_owned()))
}

fn encode_value_set(values: &ValueSet) -> dto::ValueSet {
    dto::ValueSet {
        value_type: Some(encode_value_type(values.value_type())),
        ranges: values
            .ranges()
            .iter()
            .map(|range| dto::Range {
                low: Some(encode_bound(range.low())),
                high: Some(encode_bound(range.high())),
            })
            .collect(),
    }
}

fn decode_domain(raw: &dto::Domain, path: FieldPath) -> Result<Domain, ProtocolError> {
    let values = raw
        .values
        .as_ref()
        .ok_or_else(|| missing(path.clone().field("values"), "domain requires a value set"))?;
    let values = decode_value_set(values, path.clone().field("values"))?;
    if !values.value_type().is_comparable() {
        return Err(invalid(
            path.field("values"),
            "a domain cannot be expressed over a non-comparable column type",
        ));
    }
    Ok(Domain::new(values, raw.null_allowed))
}

fn encode_domain(domain: &Domain) -> dto::Domain {
    dto::Domain {
        values: Some(encode_value_set(domain.values())),
        null_allowed: domain.null_allowed(),
    }
}

/// Decode a tuple domain, enforcing the unsatisfiable/constrained split and
/// rejecting duplicate column keys.
pub fn decode_tuple_domain(
    raw: &dto::TupleDomain,
    path: FieldPath,
) -> Result<TupleDomain<ValidatedColumnHandle>, ProtocolError> {
    if raw.none {
        if !raw.column_domains.is_empty() {
            return Err(inconsistent(
                path.field("column_domains"),
                "an unsatisfiable tuple domain must carry no column domains",
            ));
        }
        return Ok(TupleDomain::none());
    }
    if raw.column_domains.len() > MAX_TUPLE_DOMAIN_COLUMNS {
        return Err(out_of_range(
            path.field("column_domains"),
            "tuple domain column count exceeds the hard limit",
        ));
    }
    let mut domains = BTreeMap::new();
    for (index, entry) in raw.column_domains.iter().enumerate() {
        let entry_path = path.clone().field("column_domains").index(index);
        let column = entry.column.clone().ok_or_else(|| {
            missing(
                entry_path.clone().field("column"),
                "column domain requires a column handle",
            )
        })?;
        let column = ValidatedColumnHandle::parse(column, entry_path.clone().field("column"))?;
        let domain = entry.domain.as_ref().ok_or_else(|| {
            missing(
                entry_path.clone().field("domain"),
                "column domain requires a domain",
            )
        })?;
        let domain = decode_domain(domain, entry_path.clone().field("domain"))?;
        if domains.insert(column, domain).is_some() {
            return Err(inconsistent(
                entry_path.field("column"),
                "tuple domain contains a duplicate column",
            ));
        }
    }
    TupleDomain::with_column_domains(domains)
        .map_err(|error| out_of_range(path, error.message().to_owned()))
}

/// Encode a tuple domain in canonical column order.
pub fn encode_tuple_domain(domain: &TupleDomain<ValidatedColumnHandle>) -> dto::TupleDomain {
    match domain.domains() {
        None => dto::TupleDomain {
            none: true,
            column_domains: Vec::new(),
        },
        Some(domains) => dto::TupleDomain {
            none: false,
            column_domains: domains
                .iter()
                .map(|(column, domain)| dto::ColumnDomain {
                    column: Some(column.as_proto().clone()),
                    domain: Some(encode_domain(domain)),
                })
                .collect(),
        },
    }
}

/// Decode a residual expression, enforcing node-count and depth bounds.
pub fn decode_connector_expression(
    raw: &dto::ConnectorExpression,
    path: FieldPath,
) -> Result<ConnectorExpression, ProtocolError> {
    let mut nodes = 0_usize;
    decode_expression_node(raw, path, 1, &mut nodes)
}

fn decode_expression_node(
    raw: &dto::ConnectorExpression,
    path: FieldPath,
    depth: usize,
    nodes: &mut usize,
) -> Result<ConnectorExpression, ProtocolError> {
    if depth > MAX_EXPRESSION_DEPTH {
        return Err(out_of_range(
            path,
            "expression depth exceeds the hard limit",
        ));
    }
    *nodes += 1;
    if *nodes > MAX_EXPRESSION_NODES {
        return Err(out_of_range(
            path,
            "expression node count exceeds the hard limit",
        ));
    }
    let node = raw
        .node
        .as_ref()
        .ok_or_else(|| missing(path.clone(), "expression node must be present"))?;
    match node {
        dto::connector_expression::Node::Constant(constant) => {
            let value_type = constant.value_type.as_ref().ok_or_else(|| {
                missing(
                    path.clone().field("constant").field("value_type"),
                    "constant requires its exact type",
                )
            })?;
            let value_type = decode_value_type(
                value_type,
                path.clone().field("constant").field("value_type"),
            )?;
            let value = match constant.value.as_ref() {
                None => None,
                Some(value) => Some(decode_value(
                    value,
                    value_type,
                    path.field("constant").field("value"),
                )?),
            };
            Ok(ConnectorExpression::Constant { value, value_type })
        }
        dto::connector_expression::Node::Variable(variable) => {
            bounded_text(
                &variable.name,
                MAX_NAME_BYTES,
                path.clone().field("variable").field("name"),
                false,
            )?;
            let value_type = variable.value_type.as_ref().ok_or_else(|| {
                missing(
                    path.clone().field("variable").field("value_type"),
                    "variable requires its exact type",
                )
            })?;
            let value_type =
                decode_value_type(value_type, path.field("variable").field("value_type"))?;
            Ok(ConnectorExpression::Variable {
                name: Arc::from(variable.name.as_str()),
                value_type,
            })
        }
        dto::connector_expression::Node::FieldDereference(dereference) => {
            let target = dereference.target.as_ref().ok_or_else(|| {
                missing(
                    path.clone().field("field_dereference").field("target"),
                    "field dereference requires a target",
                )
            })?;
            let value_type = dereference.value_type.as_ref().ok_or_else(|| {
                missing(
                    path.clone().field("field_dereference").field("value_type"),
                    "field dereference requires its exact type",
                )
            })?;
            let value_type = decode_value_type(
                value_type,
                path.clone().field("field_dereference").field("value_type"),
            )?;
            let target = decode_expression_node(
                target,
                path.field("field_dereference").field("target"),
                depth + 1,
                nodes,
            )?;
            Ok(ConnectorExpression::FieldDereference {
                target: Box::new(target),
                field_index: dereference.field_index,
                value_type,
            })
        }
        dto::connector_expression::Node::Call(call) => {
            let function =
                ConnectorFunctionName::try_new(&call.function_name).map_err(|error| {
                    invalid(
                        path.clone().field("call").field("function_name"),
                        error.message().to_owned(),
                    )
                })?;
            let value_type = call.value_type.as_ref().ok_or_else(|| {
                missing(
                    path.clone().field("call").field("value_type"),
                    "call requires its exact type",
                )
            })?;
            let value_type =
                decode_value_type(value_type, path.clone().field("call").field("value_type"))?;
            let mut arguments = Vec::with_capacity(call.arguments.len());
            for (index, argument) in call.arguments.iter().enumerate() {
                arguments.push(decode_expression_node(
                    argument,
                    path.clone().field("call").field("arguments").index(index),
                    depth + 1,
                    nodes,
                )?);
            }
            Ok(ConnectorExpression::Call {
                function,
                value_type,
                arguments,
            })
        }
    }
}

/// Encode a residual expression as its unique generated representation.
pub fn encode_connector_expression(expression: &ConnectorExpression) -> dto::ConnectorExpression {
    let node = match expression {
        ConnectorExpression::Constant { value, value_type } => {
            dto::connector_expression::Node::Constant(dto::ConstantExpression {
                value_type: Some(encode_value_type(*value_type)),
                value: value.as_ref().map(encode_value),
            })
        }
        ConnectorExpression::Variable { name, value_type } => {
            dto::connector_expression::Node::Variable(dto::VariableExpression {
                name: name.to_string(),
                value_type: Some(encode_value_type(*value_type)),
            })
        }
        ConnectorExpression::FieldDereference {
            target,
            field_index,
            value_type,
        } => dto::connector_expression::Node::FieldDereference(Box::new(
            dto::FieldDereferenceExpression {
                target: Some(Box::new(encode_connector_expression(target))),
                field_index: *field_index,
                value_type: Some(encode_value_type(*value_type)),
            },
        )),
        ConnectorExpression::Call {
            function,
            value_type,
            arguments,
        } => dto::connector_expression::Node::Call(dto::CallExpression {
            function_name: function.as_str().to_owned(),
            value_type: Some(encode_value_type(*value_type)),
            arguments: arguments.iter().map(encode_connector_expression).collect(),
        }),
    };
    dto::ConnectorExpression { node: Some(node) }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use novarocks_spi::connector::read_stack::ConnectorValue;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorCodecRevision, ConnectorEnvelopeHeader,
        ConnectorInstanceId, ConnectorProviderId,
    };

    use super::*;

    fn root() -> FieldPath {
        FieldPath::root("tuple_domain")
    }

    fn column(field_id: i32) -> dto::ColumnHandle {
        let payload = ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                ConnectorProviderId::parse("iceberg").unwrap(),
                CatalogHandle::new(
                    ConnectorInstanceId::try_from_canonical("lake").unwrap(),
                    CatalogVersion::from_bytes([7; 32]),
                ),
                ConnectorCodecCategory::ReadColumn,
                ConnectorCodecRevision::try_new(1).unwrap(),
            ),
            Bytes::copy_from_slice(&field_id.to_be_bytes()),
        );
        dto::ColumnHandle {
            provider_payload: Some(crate::connector_common::encode_connector_payload_message(
                &payload,
            )),
        }
    }

    fn big_int_domain(low: i64) -> Domain {
        let range = Range::try_new(
            ConnectorValueType::BigInt,
            Bound::Inclusive(ConnectorValue::BigInt(low)),
            Bound::Unbounded,
        )
        .expect("valid range");
        Domain::new(
            ValueSet::of_ranges(ConnectorValueType::BigInt, vec![range]).expect("valid set"),
            false,
        )
    }

    /// A column with no comparable counterpart can be projected but never
    /// constrained. There is no `Value` arm for it, so the only way one could
    /// reach a predicate is an all-or-none domain, which this refuses.
    #[test]
    fn a_domain_over_a_non_comparable_column_type_is_refused() {
        let raw = dto::Domain {
            values: Some(dto::ValueSet {
                value_type: Some(super::super::value::encode_value_type(
                    ConnectorValueType::NonComparable,
                )),
                ranges: Vec::new(),
            }),
            null_allowed: true,
        };
        let error = decode_domain(&raw, root()).expect_err("a non-comparable domain");
        assert_eq!(error.kind(), crate::ProtocolErrorKind::InvalidValue);
        assert!(
            error.to_string().contains("non-comparable"),
            "unexpected detail: {error}"
        );
    }

    #[test]
    fn tuple_domains_round_trip_in_canonical_column_order() {
        let mut domains = BTreeMap::new();
        for field_id in [7_i32, 2, 5] {
            let handle = ValidatedColumnHandle::parse(column(field_id), root()).expect("valid");
            domains.insert(handle, big_int_domain(i64::from(field_id)));
        }
        let tuple = TupleDomain::with_column_domains(domains).expect("bounded");
        let encoded = encode_tuple_domain(&tuple);
        let decoded = decode_tuple_domain(&encoded, root()).expect("valid");
        assert_eq!(decoded, tuple);
        let reencoded = encode_tuple_domain(&decoded);
        assert_eq!(encoded, reencoded);
    }

    #[test]
    fn an_unsatisfiable_tuple_domain_must_be_empty() {
        let none = dto::TupleDomain {
            none: true,
            column_domains: Vec::new(),
        };
        assert!(decode_tuple_domain(&none, root()).expect("valid").is_none());

        let contradictory = dto::TupleDomain {
            none: true,
            column_domains: vec![dto::ColumnDomain {
                column: Some(column(1)),
                domain: Some(encode_domain(&big_int_domain(1))),
            }],
        };
        assert_eq!(
            decode_tuple_domain(&contradictory, root())
                .expect_err("contradictory")
                .kind(),
            crate::ProtocolErrorKind::InconsistentFields
        );
    }

    #[test]
    fn duplicate_columns_are_rejected() {
        let duplicated = dto::TupleDomain {
            none: false,
            column_domains: vec![
                dto::ColumnDomain {
                    column: Some(column(1)),
                    domain: Some(encode_domain(&big_int_domain(1))),
                },
                dto::ColumnDomain {
                    column: Some(column(1)),
                    domain: Some(encode_domain(&big_int_domain(2))),
                },
            ],
        };
        assert_eq!(
            decode_tuple_domain(&duplicated, root())
                .expect_err("duplicate")
                .kind(),
            crate::ProtocolErrorKind::InconsistentFields
        );
    }

    #[test]
    fn an_absent_column_handle_variant_is_a_missing_field() {
        let empty = dto::ColumnHandle {
            provider_payload: None,
        };
        assert_eq!(
            ValidatedColumnHandle::parse(empty, root())
                .expect_err("absent")
                .kind(),
            crate::ProtocolErrorKind::MissingField
        );
    }

    #[test]
    fn an_unbounded_bound_must_not_carry_a_value() {
        let raw = dto::Bound {
            kind: dto::BoundKind::Unbounded as i32,
            value: Some(encode_value(&ConnectorValue::BigInt(1))),
        };
        assert_eq!(
            decode_bound(&raw, ConnectorValueType::BigInt, root())
                .expect_err("stray value")
                .kind(),
            crate::ProtocolErrorKind::InconsistentFields
        );
    }

    #[test]
    fn expressions_round_trip_and_bound_their_depth() {
        let expression = ConnectorExpression::Call {
            function: ConnectorFunctionName::try_new("$like").expect("name"),
            value_type: ConnectorValueType::Boolean,
            arguments: vec![ConnectorExpression::Variable {
                name: Arc::from("v0"),
                value_type: ConnectorValueType::Varchar,
            }],
        };
        let encoded = encode_connector_expression(&expression);
        assert_eq!(
            decode_connector_expression(&encoded, FieldPath::root("expression")).expect("valid"),
            expression
        );

        let mut deep = ConnectorExpression::Constant {
            value: Some(ConnectorValue::BigInt(1)),
            value_type: ConnectorValueType::BigInt,
        };
        for _ in 0..MAX_EXPRESSION_DEPTH {
            deep = ConnectorExpression::Call {
                function: ConnectorFunctionName::try_new("$negate").expect("name"),
                value_type: ConnectorValueType::BigInt,
                arguments: vec![deep],
            };
        }
        let encoded_deep = encode_connector_expression(&deep);
        assert_eq!(
            decode_connector_expression(&encoded_deep, FieldPath::root("expression"))
                .expect_err("too deep")
                .kind(),
            crate::ProtocolErrorKind::OutOfRange
        );
    }
}
