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

use std::collections::BTreeMap;

use novarocks_spi::connector::read_stack::{Bound, Domain, TupleDomain};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::iceberg::expr::{
    BinaryExpression, BoundPredicate, BoundReference, PredicateOperator, SetExpression,
    UnaryExpression,
};
use crate::iceberg::spec::{Datum, PrimitiveType, SchemaRef, Type};
use crate::iceberg::{Error, ErrorKind, Result};
use crate::typed_read::IcebergColumnHandle;

#[derive(Debug)]
pub(crate) enum DeleteCondition {
    AllRows,
    Never,
    /// Each column is a conjunction in the complete predicate. A column's
    /// alternatives retain the complete set of disjoint ranges and NULL.
    Fields(Box<[DeleteFieldCondition]>),
}

#[derive(Debug)]
pub(crate) struct DeleteFieldCondition {
    pub(crate) field_id: i32,
    pub(crate) primitive: PrimitiveType,
    pub(crate) integer_domain: Option<crate::scalar_integer_domain::ScalarIntegerDomain>,
    pub(crate) alternatives: Box<[Box<[BoundPredicate]>]>,
}

#[derive(Debug)]
pub(crate) struct IcebergDeleteConditionFact {
    pub(crate) table_uuid: Uuid,
    pub(crate) snapshot_id: i64,
    pub(crate) metadata_identity: String,
    pub(crate) partition_proof_digest: [u8; 32],
    pub(crate) digest: [u8; 32],
    pub(crate) condition: DeleteCondition,
    pub(crate) _retention: Option<novarocks_spi::connector::ConnectorPayloadRetentionGuard>,
}

impl DeleteCondition {
    pub(crate) fn from_enforced(
        schema: &SchemaRef,
        enforced: &TupleDomain<IcebergColumnHandle>,
    ) -> Result<Self> {
        let Some(domains) = enforced.domains() else {
            return Ok(Self::Never);
        };
        let mut fields = BTreeMap::new();
        for (column, domain) in domains {
            if !column.is_base_column() {
                return Err(invalid("Filtered delete requires a bound base column"));
            }
            let field_id = column.base_field_id();
            let field = schema
                .as_struct()
                .field_by_id(field_id)
                .ok_or_else(|| invalid("Filtered delete field is absent from its source schema"))?;
            let Type::Primitive(primitive) = field.field_type.as_ref() else {
                return Err(invalid("Filtered delete field is not primitive"));
            };
            let accessor = schema
                .accessor_by_field_id(field_id)
                .ok_or_else(|| invalid("Filtered delete has no original field accessor"))?;
            let term = BoundReference::new(field.name.clone(), field.clone(), accessor);
            let integer_domain = column.scalar_integer_domain();
            if integer_domain.is_some_and(|declared| declared.value_type() != domain.value_type()) {
                return Err(invalid(
                    "Filtered delete domain differs from its declared scalar integer type",
                ));
            }
            let alternatives = field_alternatives(&term, primitive, domain)?;
            if alternatives.is_empty() {
                return Ok(Self::Never);
            }
            if fields
                .insert(
                    field_id,
                    DeleteFieldCondition {
                        field_id,
                        primitive: primitive.clone(),
                        integer_domain,
                        alternatives,
                    },
                )
                .is_some()
            {
                return Err(invalid("Filtered delete repeats one source field"));
            }
        }
        if fields.is_empty() {
            Ok(Self::AllRows)
        } else {
            Ok(Self::Fields(fields.into_values().collect()))
        }
    }

    pub(crate) fn digest(&self) -> Result<[u8; 32]> {
        let mut digest = Sha256::new();
        digest.update(b"novarocks.iceberg.delete-condition.v1\0");
        match self {
            Self::AllRows => digest.update([0]),
            Self::Never => digest.update([1]),
            Self::Fields(fields) => {
                digest.update([2]);
                digest.update((fields.len() as u64).to_be_bytes());
                for field in fields {
                    digest.update(field.field_id.to_be_bytes());
                    let declaration = serde_json::to_vec(&field.primitive)
                        .map_err(|error| invalid(error.to_string()))?;
                    digest_bytes(&mut digest, &declaration);
                    digest_bytes(
                        &mut digest,
                        field
                            .integer_domain
                            .map_or("", |domain| domain.name())
                            .as_bytes(),
                    );
                    digest.update((field.alternatives.len() as u64).to_be_bytes());
                    for arm in &field.alternatives {
                        digest.update((arm.len() as u64).to_be_bytes());
                        for leaf in arm {
                            digest_leaf(&mut digest, leaf)?;
                        }
                    }
                }
            }
        }
        Ok(digest.finalize().into())
    }
}

fn field_alternatives(
    term: &BoundReference,
    primitive: &PrimitiveType,
    domain: &Domain,
) -> Result<Box<[Box<[BoundPredicate]>]>> {
    let mut alternatives = Vec::new();
    let values = domain.values();
    if values.is_all() && domain.null_allowed() {
        return Ok(vec![Vec::new().into_boxed_slice()].into_boxed_slice());
    }
    if domain.null_allowed() {
        alternatives.push(
            vec![BoundPredicate::Unary(UnaryExpression::new(
                PredicateOperator::IsNull,
                term.clone(),
            ))]
            .into_boxed_slice(),
        );
    }
    if values.is_all() {
        alternatives.push(
            vec![BoundPredicate::Unary(UnaryExpression::new(
                PredicateOperator::NotNull,
                term.clone(),
            ))]
            .into_boxed_slice(),
        );
    } else if let Some(points) = values.discrete_values() {
        let literals = points
            .iter()
            .map(|value| exact_datum(value, primitive))
            .collect::<Result<Vec<_>>>()?;
        match literals.as_slice() {
            [] => {}
            [literal] => alternatives.push(
                vec![BoundPredicate::Binary(BinaryExpression::new(
                    PredicateOperator::Eq,
                    term.clone(),
                    literal.clone(),
                ))]
                .into_boxed_slice(),
            ),
            _ => alternatives.push(
                vec![BoundPredicate::Set(SetExpression::new(
                    PredicateOperator::In,
                    term.clone(),
                    literals.into_iter().collect(),
                ))]
                .into_boxed_slice(),
            ),
        }
    } else {
        for range in values.ranges() {
            let mut conjunction = Vec::new();
            for (bound, inclusive, exclusive) in [
                (
                    range.low(),
                    PredicateOperator::GreaterThanOrEq,
                    PredicateOperator::GreaterThan,
                ),
                (
                    range.high(),
                    PredicateOperator::LessThanOrEq,
                    PredicateOperator::LessThan,
                ),
            ] {
                let (operator, value) = match bound {
                    Bound::Unbounded => continue,
                    Bound::Inclusive(value) => (inclusive, value),
                    Bound::Exclusive(value) => (exclusive, value),
                };
                conjunction.push(BoundPredicate::Binary(BinaryExpression::new(
                    operator,
                    term.clone(),
                    exact_datum(value, primitive)?,
                )));
            }
            alternatives.push(conjunction.into_boxed_slice());
        }
    }
    Ok(alternatives.into_boxed_slice())
}

fn exact_datum(
    value: &novarocks_spi::connector::read_stack::ConnectorValue,
    primitive: &PrimitiveType,
) -> Result<Datum> {
    super::conflict_scope::connector_native_datum(value, primitive)
        .filter(|datum| datum.data_type() == primitive)
        .ok_or_else(|| {
            invalid("Filtered delete literal does not represent its original field type")
        })
}

fn digest_bytes(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

fn digest_leaf(digest: &mut Sha256, leaf: &BoundPredicate) -> Result<()> {
    match leaf {
        BoundPredicate::Unary(leaf) => digest.update((leaf.op() as u32).to_be_bytes()),
        BoundPredicate::Binary(leaf) => {
            digest.update((leaf.op() as u32).to_be_bytes());
            digest_bytes(digest, leaf.literal().to_bytes()?.as_ref());
        }
        BoundPredicate::Set(leaf) => {
            digest.update((leaf.op() as u32).to_be_bytes());
            let mut values = leaf
                .literals()
                .iter()
                .map(Datum::to_bytes)
                .collect::<Result<Vec<_>>>()?;
            values.sort_unstable();
            digest.update((values.len() as u64).to_be_bytes());
            for value in values {
                digest_bytes(digest, value.as_ref());
            }
        }
        _ => {
            return Err(invalid(
                "Filtered delete contains an unsupported predicate leaf",
            ));
        }
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::iceberg::spec::{NestedField, Schema};
    use novarocks_spi::connector::read_stack::{ConnectorValue, ConnectorValueType};

    fn schema(id: i32, name: &str, kind: PrimitiveType) -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::optional(
                    id,
                    name,
                    Type::Primitive(kind),
                ))])
                .build()
                .unwrap(),
        )
    }

    fn condition(
        id: i32,
        name: &str,
        kind: PrimitiveType,
        domain: Domain,
    ) -> Result<DeleteCondition> {
        let schema = schema(id, name, kind);
        let column = IcebergColumnHandle::base_column_of(&schema, id).unwrap();
        let domain = TupleDomain::with_column_domains(BTreeMap::from([(column, domain)])).unwrap();
        DeleteCondition::from_enforced(&schema, &domain)
    }

    #[test]
    fn digest_binds_field_id_and_value_but_survives_rename() {
        let make = |id, name, value| {
            condition(
                id,
                name,
                PrimitiveType::Long,
                Domain::single_value(ConnectorValue::BigInt(value)).unwrap(),
            )
            .unwrap()
            .digest()
            .unwrap()
        };
        let original = make(1, "original", i64::MAX);
        assert_eq!(original, make(1, "renamed", i64::MAX));
        assert_ne!(original, make(2, "original", i64::MAX));
        assert_ne!(original, make(1, "original", i64::MAX - 1));
        let int = condition(
            1,
            "original",
            PrimitiveType::Int,
            Domain::single_value(ConnectorValue::Integer(1)).unwrap(),
        )
        .unwrap()
        .digest()
        .unwrap();
        assert_ne!(int, make(1, "original", 1));
    }

    #[test]
    fn preserves_all_rows_no_rows_and_only_null() {
        let schema = schema(1, "id", PrimitiveType::Long);
        assert!(matches!(
            DeleteCondition::from_enforced(&schema, &TupleDomain::all()).unwrap(),
            DeleteCondition::AllRows
        ));
        assert!(matches!(
            DeleteCondition::from_enforced(&schema, &TupleDomain::none()).unwrap(),
            DeleteCondition::Never
        ));
        let result = condition(
            1,
            "id",
            PrimitiveType::Long,
            Domain::new(
                novarocks_spi::connector::read_stack::ValueSet::none(ConnectorValueType::BigInt),
                true,
            ),
        )
        .unwrap();
        let DeleteCondition::Fields(fields) = result else {
            panic!("expected a field predicate")
        };
        let [BoundPredicate::Unary(leaf)] = fields[0].alternatives[0].as_ref() else {
            panic!("expected a NULL predicate")
        };
        assert_eq!(leaf.op(), PredicateOperator::IsNull);
    }

    #[test]
    fn exact_literals_do_not_use_sdk_casts() {
        assert!(exact_datum(&ConnectorValue::Integer(1), &PrimitiveType::Long).is_err());
        assert!(
            exact_datum(
                &ConnectorValue::TimestampNanos(1),
                &PrimitiveType::Timestamp
            )
            .is_err()
        );
        let micros = exact_datum(
            &ConnectorValue::TimestampMicros(-1),
            &PrimitiveType::Timestamp,
        )
        .unwrap();
        assert_eq!(micros, Datum::timestamp_micros(-1));
    }
}
