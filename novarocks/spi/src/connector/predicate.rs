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

//! Provider-neutral, planning-time static predicate contract.
//!
//! This module deliberately has no Core expression, runtime-filter, connector,
//! or file-format types. A predicate disposition describes a guarantee for one
//! independently numbered top-level SQL conjunct in one scan attempt.

use std::collections::{BTreeMap, BTreeSet};

use super::{ConnectorError, ConnectorErrorKind, ConnectorScalarType, ConnectorScalarValue};

pub const MAX_CONNECTOR_STATIC_PREDICATES: usize = 1024;
pub const MAX_CONNECTOR_STATIC_IN_LITERALS: usize = 1024;
pub const MAX_CONNECTOR_STATIC_VARIABLE_LITERAL_BYTES: usize = 64 * 1024;
pub const MAX_CONNECTOR_STATIC_LITERAL_PAYLOAD_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ConnectorStaticPredicateId(pub u32);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorStaticPredicateColumn {
    /// Stable ordinal in the table schema addressed by this scan request.
    pub field_ordinal: u32,
    pub data_type: ConnectorScalarType,
    pub nullable: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectorStaticComparisonOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectorStaticPredicateKind {
    Comparison {
        op: ConnectorStaticComparisonOp,
        literal: ConnectorScalarValue,
    },
    IsNull,
    IsNotNull,
    In {
        literals: Vec<ConnectorScalarValue>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorStaticPredicate {
    pub id: ConnectorStaticPredicateId,
    pub column: ConnectorStaticPredicateColumn,
    pub kind: ConnectorStaticPredicateKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectorPredicateDispositionKind {
    Exact,
    PruningOnly,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorPredicateDisposition {
    pub predicate_id: ConnectorStaticPredicateId,
    pub kind: ConnectorPredicateDispositionKind,
}

/// Validate one static request before it crosses the SPI boundary.
pub fn validate_static_predicates(
    predicates: &[ConnectorStaticPredicate],
) -> Result<(), ConnectorError> {
    if predicates.len() > MAX_CONNECTOR_STATIC_PREDICATES {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector static predicate count exceeds the hard limit",
        ));
    }

    let mut ids = BTreeSet::new();
    let mut total_payload = 0_usize;
    for predicate in predicates {
        if !ids.insert(predicate.id) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector static predicate IDs must be unique",
            ));
        }
        validate_predicate_kind(predicate, &mut total_payload)?;
    }
    Ok(())
}

/// Validate a provider disposition response and normalize it to request order.
// Design: ADR-0018 (docs/adr/ADR-0018-static-connector-predicate-disposition.md)
pub fn normalize_predicate_dispositions(
    predicates: &[ConnectorStaticPredicate],
    dispositions: &[ConnectorPredicateDisposition],
) -> Result<Vec<ConnectorPredicateDisposition>, ConnectorError> {
    validate_static_predicates(predicates)?;
    if predicates.len() != dispositions.len() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "connector predicate disposition response is not total",
        ));
    }

    let request_ids = predicates
        .iter()
        .map(|predicate| predicate.id)
        .collect::<BTreeSet<_>>();
    let mut by_id = BTreeMap::new();
    for disposition in dispositions {
        if !request_ids.contains(&disposition.predicate_id) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector predicate disposition response contains an unknown predicate ID",
            ));
        }
        if by_id
            .insert(disposition.predicate_id, *disposition)
            .is_some()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "connector predicate disposition response contains a duplicate predicate ID",
            ));
        }
    }

    predicates
        .iter()
        .map(|predicate| {
            by_id.get(&predicate.id).copied().ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "connector predicate disposition response omits a predicate ID",
                )
            })
        })
        .collect()
}

fn validate_predicate_kind(
    predicate: &ConnectorStaticPredicate,
    total_payload: &mut usize,
) -> Result<(), ConnectorError> {
    let validate_literal =
        |literal: &ConnectorScalarValue, total_payload: &mut usize| -> Result<(), ConnectorError> {
            if literal.data_type() != predicate.column.data_type {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector static predicate literal type differs from its column type",
                ));
            }
            if let Some(variable_bytes) = literal.variable_payload_bytes()
                && variable_bytes > MAX_CONNECTOR_STATIC_VARIABLE_LITERAL_BYTES
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "connector static predicate variable literal exceeds the hard limit",
                ));
            }
            *total_payload = total_payload.saturating_add(literal.payload_bytes());
            if *total_payload > MAX_CONNECTOR_STATIC_LITERAL_PAYLOAD_BYTES {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "connector static predicate literal payload exceeds the hard limit",
                ));
            }
            Ok(())
        };

    match &predicate.kind {
        ConnectorStaticPredicateKind::Comparison { literal, .. } => {
            validate_literal(literal, total_payload)
        }
        ConnectorStaticPredicateKind::IsNull | ConnectorStaticPredicateKind::IsNotNull => Ok(()),
        ConnectorStaticPredicateKind::In { literals } => {
            if literals.is_empty() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "connector static predicate IN literal list must not be empty",
                ));
            }
            if literals.len() > MAX_CONNECTOR_STATIC_IN_LITERALS {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "connector static predicate IN literal count exceeds the hard limit",
                ));
            }
            for literal in literals {
                validate_literal(literal, total_payload)?;
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int_predicate(id: u32) -> ConnectorStaticPredicate {
        ConnectorStaticPredicate {
            id: ConnectorStaticPredicateId(id),
            column: ConnectorStaticPredicateColumn {
                field_ordinal: 0,
                data_type: ConnectorScalarType::Int32,
                nullable: false,
            },
            kind: ConnectorStaticPredicateKind::Comparison {
                op: ConnectorStaticComparisonOp::Eq,
                literal: ConnectorScalarValue::Int32(7),
            },
        }
    }

    #[test]
    fn dispositions_are_normalized_to_predicate_order() {
        let predicates = vec![int_predicate(4), int_predicate(9)];
        let actual = normalize_predicate_dispositions(
            &predicates,
            &[
                ConnectorPredicateDisposition {
                    predicate_id: ConnectorStaticPredicateId(9),
                    kind: ConnectorPredicateDispositionKind::Unsupported,
                },
                ConnectorPredicateDisposition {
                    predicate_id: ConnectorStaticPredicateId(4),
                    kind: ConnectorPredicateDispositionKind::PruningOnly,
                },
            ],
        )
        .expect("valid response");
        assert_eq!(actual[0].predicate_id, ConnectorStaticPredicateId(4));
        assert_eq!(actual[1].predicate_id, ConnectorStaticPredicateId(9));
    }

    #[test]
    fn malformed_disposition_response_is_corrupt_data() {
        let predicates = vec![int_predicate(4)];
        let error = normalize_predicate_dispositions(&predicates, &[]).expect_err("missing ID");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }
}
