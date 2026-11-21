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

//! Inclusive conflict predicates for row-delta publication.

// Design: ADR-0244 (docs/adr/ADR-0244-iceberg-row-dml-conflict-validation.md)

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::mem::size_of;
use std::sync::Arc;

use arrow::datatypes::Date32Type;
use arrow::temporal_conversions::timestamp_us_to_datetime;
use novarocks_spi::connector::read_stack::{Bound, ConnectorValue, Domain, TupleDomain};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorPayloadRetentionGuard,
};
use uuid::Uuid;

use super::conflict::{CandidateFile, MayMatch};
use super::write_stack::domain::IcebergArtifactPartition;
use crate::iceberg::expr::{
    BinaryExpression, Bind, BoundPredicate, BoundReference, Predicate, PredicateOperator,
    Reference, SetExpression, UnaryExpression,
};
use crate::iceberg::spec::{
    DataFile, DataFileFormat, Datum, Literal, PartitionField, PrimitiveLiteral, PrimitiveType,
    SchemaRef, TableMetadata, Transform, Type,
};
use crate::typed_read::{IcebergColumnHandle, IcebergMetadataColumn};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RewrittenTargetBinding {
    pub(crate) table_uuid: Uuid,
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) preparation_digest: [u8; 32],
}

#[derive(Debug)]
pub(crate) struct IcebergRewrittenTargetFact {
    pub(crate) binding: RewrittenTargetBinding,
    schema: SchemaRef,
    scan: TupleDomain<IcebergColumnHandle>,
}

impl IcebergRewrittenTargetFact {
    pub(crate) fn new(
        binding: RewrittenTargetBinding,
        schema: SchemaRef,
        enforced: &TupleDomain<IcebergColumnHandle>,
        unenforced: &TupleDomain<IcebergColumnHandle>,
    ) -> Result<Self, ConnectorError> {
        let data_columns = |column: &IcebergColumnHandle| {
            IcebergMetadataColumn::from_field_id(column.base_field_id()).is_none()
        };
        let scan = enforced
            .filter_columns(data_columns)
            .intersect(&unenforced.filter_columns(data_columns))?;
        Ok(Self {
            binding,
            schema,
            scan,
        })
    }
}

#[derive(Debug)]
pub(crate) struct WrittenPartitionCollector {
    spec_id: i32,
    partitioned: bool,
    fields: usize,
    identities: Vec<(usize, i32)>,
}

impl WrittenPartitionCollector {
    pub(crate) fn new(metadata: &TableMetadata) -> Self {
        let spec = metadata.default_partition_spec();
        let identities = spec
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| {
                field.transform == Transform::Identity
                    && metadata
                        .partition_specs_iter()
                        .all(|spec| spec.fields().contains(field))
            })
            .map(|(index, field)| (index, field.source_id))
            .collect();
        Self {
            spec_id: spec.spec_id(),
            partitioned: !spec.is_unpartitioned(),
            fields: spec.fields().len(),
            identities,
        }
    }

    // Prepare each record without changing the receiving state until every
    // fragment and its compact encoding have been admitted.
    pub(crate) fn observe(
        &self,
        existing: &WrittenPartitionDomain,
        pending: &mut WrittenPartitionDomain,
        partition: Option<&IcebergArtifactPartition>,
        author: &ConnectorPayloadRetentionGuard,
    ) -> Result<(), ConnectorError> {
        if existing.all || pending.all {
            return Ok(());
        }
        let Some(partition) = partition.filter(|partition| {
            self.partitioned
                && partition.partition_spec_id() == self.spec_id
                && partition.descriptor().values.len() == self.fields
        }) else {
            pending.widen();
            return Ok(());
        };

        for &(index, field_id) in &self.identities {
            let value = partition.descriptor().values[index].datum_bytes.as_deref();
            if existing
                .values
                .get(&field_id)
                .is_some_and(|column| column.contains(value))
                || pending
                    .values
                    .get(&field_id)
                    .is_some_and(|column| column.contains(value))
            {
                continue;
            }
            // The encoded key and typed value can coexist during composition.
            // Their payment comes from the original receiving author and moves
            // with the typed value if it still contributes to conflict scope.
            let bytes = value
                .map_or(0, <[u8]>::len)
                .checked_mul(2)
                .and_then(|bytes| {
                    bytes.checked_add(
                        size_of::<(Box<[u8]>, Datum, ConnectorPayloadRetentionGuard)>(),
                    )
                })
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "Iceberg written partition value size overflow",
                    )
                })?;
            let receipt = author.reserve_owned_bytes(bytes)?;
            let column = pending.values.entry(field_id).or_default();
            match value {
                Some(bytes) => {
                    column.non_null.insert(bytes.into(), receipt);
                }
                None => column.null = Some(receipt),
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub(crate) struct WrittenPartitionDomain {
    all: bool,
    values: BTreeMap<i32, WrittenValues>,
}

impl WrittenPartitionDomain {
    fn widen(&mut self) {
        self.all = true;
        self.values.clear();
    }

    pub(crate) fn apply(&mut self, record: Self) {
        if record.all {
            self.widen();
        } else {
            for (field_id, values) in record.values {
                let column = self.values.entry(field_id).or_default();
                column.non_null.extend(values.non_null);
                if values.null.is_some() {
                    column.null = values.null;
                }
            }
        }
    }
}

#[derive(Debug, Default)]
struct WrittenValues {
    non_null: BTreeMap<Box<[u8]>, ConnectorPayloadRetentionGuard>,
    null: Option<ConnectorPayloadRetentionGuard>,
}

impl WrittenValues {
    fn contains(&self, value: Option<&[u8]>) -> bool {
        match value {
            Some(bytes) => self.non_null.contains_key(bytes),
            None => self.null.is_some(),
        }
    }
}

#[derive(Debug)]
pub(crate) enum ConflictScope {
    All,
    Never,
    Domain(ConflictDomain),
}

#[derive(Debug)]
pub(crate) struct ConflictDomain {
    columns: BTreeMap<i32, ColumnPredicate>,
    // Actual backing is destroyed before its original payment.
    _written_receipts: Vec<ConnectorPayloadRetentionGuard>,
}

#[derive(Debug)]
enum ColumnPredicate {
    // Each arm is a conjunction of at most two range bounds, or one NULL,
    // equality or IN predicate. The arms themselves are disjunctive.
    Projectable(Vec<Vec<BoundPredicate>>),
    // The vendor's scalar binder has no accessor for Variant. Its presence
    // predicate still uses exact field-ID metrics. Variant is unpartitionable.
    VariantPresence {
        name: Arc<str>,
        op: PredicateOperator,
    },
}

impl ColumnPredicate {
    fn projectable_arms_may_match(&self, eval: impl Fn(&BoundPredicate) -> bool) -> bool {
        match self {
            Self::Projectable(arms) => arms.iter().any(|arm| arm.iter().all(&eval)),
            Self::VariantPresence { .. } => true,
        }
    }
}

impl ConflictScope {
    pub(crate) fn compose(
        fact: &IcebergRewrittenTargetFact,
        written: WrittenPartitionDomain,
        source: &TableMetadata,
    ) -> Result<Self, ConnectorError> {
        let Some(scan) = fact.scan.domains() else {
            return Ok(Self::Never);
        };

        // Reduce stable primitive field IDs after intersecting the original
        // handle domains, before applying the Trino type restrictions.
        let mut scan_by_id = BTreeMap::<i32, Domain>::new();
        for (column, domain) in scan {
            let id = column
                .field_id_path()
                .last()
                .copied()
                .unwrap_or(column.base_field_id());
            match scan_by_id.entry(id) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(domain.clone());
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let intersection = entry.get().intersect(domain)?;
                    if intersection.is_none() {
                        return Ok(Self::Never);
                    }
                    entry.insert(intersection);
                }
            }
        }

        let mut columns = BTreeMap::new();
        let mut receipts = Vec::new();
        for (id, values) in written.values {
            let Some(field) = source.current_schema().field_by_id(id) else {
                continue;
            };
            let Type::Primitive(primitive) = field.field_type.as_ref() else {
                continue;
            };
            let scan_domain = scan_by_id.remove(&id);
            let mut datums = Vec::new();
            let mut value_receipts = Vec::new();
            let mut comparable = true;
            for (bytes, receipt) in values.non_null {
                let datum = Datum::try_from_bytes(&bytes, primitive.clone()).map_err(|error| {
                    ConnectorError::new(ConnectorErrorKind::Internal, format!(
                        "Cannot decode admitted Iceberg identity partition for field {id}: {error}"
                    ))
                })?;
                let belongs = scan_domain
                    .as_ref()
                    .map_or(Some(true), |domain| domain_contains_datum(domain, &datum));
                match belongs {
                    Some(true) => {
                        datums.push(datum);
                        value_receipts.push(receipt);
                    }
                    Some(false) => {}
                    None => comparable = false,
                }
            }
            let null_allowed =
                values.null.is_some() && scan_domain.as_ref().is_none_or(Domain::null_allowed);
            if !comparable {
                if let Some(domain) = scan_domain {
                    scan_by_id.insert(id, domain);
                }
                continue;
            }
            if datums.is_empty() && !null_allowed {
                return Ok(Self::Never);
            }
            if !datums.is_empty() && *primitive == PrimitiveType::Uuid {
                continue;
            }
            let schema = if fact.schema.field_by_id(id).is_some() {
                &fact.schema
            } else {
                source.current_schema()
            };
            let Some(term) = bind_field(schema, id) else {
                continue;
            };
            let Type::Primitive(under_query) = term.field().field_type.as_ref() else {
                continue;
            };
            // A historical read can be narrower than the current write schema.
            // Its original scan still constrains conflict detection when the
            // written values cannot be safely interpreted under that schema.
            if primitive != under_query {
                if let Some(domain) = scan_domain {
                    scan_by_id.insert(id, domain);
                }
                continue;
            }
            let mut arms = Vec::new();
            if null_allowed {
                arms.push(vec![unary(&term, PredicateOperator::IsNull)]);
                receipts.extend(values.null);
            }
            add_discrete(&mut arms, &term, datums);
            receipts.extend(value_receipts);
            columns.insert(id, ColumnPredicate::Projectable(arms));
        }

        for (id, domain) in scan_by_id {
            if let Some(field) = fact.schema.field_by_id(id)
                && matches!(field.field_type.as_ref(), Type::Variant(_))
            {
                let nullable = field_nullable(&fact.schema, id).unwrap_or(true);
                let op = match (
                    domain.values().is_none(),
                    domain.values().is_all(),
                    domain.null_allowed(),
                ) {
                    (true, _, true) if !nullable => return Ok(Self::Never),
                    (true, _, true) => PredicateOperator::IsNull,
                    (_, true, false) if nullable => PredicateOperator::NotNull,
                    _ => continue,
                };
                columns.insert(
                    id,
                    ColumnPredicate::VariantPresence {
                        name: fact
                            .schema
                            .name_by_field_id(id)
                            .unwrap_or(&field.name)
                            .into(),
                        op,
                    },
                );
                continue;
            }
            let Some(term) = bind_field(&fact.schema, id) else {
                continue;
            };
            let Some(arms) = scan_arms(&fact.schema, id, &term, &domain) else {
                continue;
            };
            if arms.is_empty() {
                return Ok(Self::Never);
            }
            columns.insert(id, ColumnPredicate::Projectable(arms));
        }

        if columns.is_empty() {
            Ok(Self::All)
        } else {
            Ok(Self::Domain(ConflictDomain {
                columns,
                _written_receipts: receipts,
            }))
        }
    }
}

fn bind_field(schema: &SchemaRef, id: i32) -> Option<BoundReference> {
    let name = schema.name_by_field_id(id)?;
    Reference::new(name).bind(Arc::clone(schema), true).ok()
}

fn unary(term: &BoundReference, op: PredicateOperator) -> BoundPredicate {
    BoundPredicate::Unary(UnaryExpression::new(op, term.clone()))
}

fn binary(term: &BoundReference, op: PredicateOperator, datum: Datum) -> BoundPredicate {
    BoundPredicate::Binary(BinaryExpression::new(op, term.clone(), datum))
}

fn add_discrete(
    arms: &mut Vec<Vec<BoundPredicate>>,
    term: &BoundReference,
    mut values: Vec<Datum>,
) {
    if values.iter().any(Datum::is_nan) {
        // A NaN never participates in ordered metrics. Give it the same
        // inclusive count-based test whether it is alone or in a larger set.
        arms.push(vec![unary(term, PredicateOperator::IsNan)]);
        values.retain(|value| !value.is_nan());
    }
    match values.len() {
        0 => {}
        1 => arms.push(vec![binary(
            term,
            PredicateOperator::Eq,
            values.pop().unwrap(),
        )]),
        _ => {
            let set = SetExpression::new(
                PredicateOperator::In,
                term.clone(),
                values.into_iter().collect(),
            );
            match set.literals().iter().next() {
                Some(only) if set.literals().len() == 1 => {
                    arms.push(vec![binary(term, PredicateOperator::Eq, only.clone())]);
                }
                _ => arms.push(vec![BoundPredicate::Set(set)]),
            }
        }
    }
}

fn scan_arms(
    schema: &SchemaRef,
    id: i32,
    term: &BoundReference,
    domain: &Domain,
) -> Option<Vec<Vec<BoundPredicate>>> {
    let nullable = field_nullable(schema, id)?;
    let mut arms = Vec::new();
    if domain.null_allowed() && nullable {
        arms.push(vec![unary(term, PredicateOperator::IsNull)]);
    }
    let primitive = match term.field().field_type.as_ref() {
        Type::Primitive(primitive) => Some(primitive),
        Type::Variant(_) => None,
        Type::Struct(_) | Type::List(_) | Type::Map(_) => return None,
    };
    let values = domain.values();
    if values.is_all() {
        if domain.null_allowed() || !nullable {
            return None;
        }
        arms.push(vec![unary(term, PredicateOperator::NotNull)]);
        return Some(arms);
    }
    if values.is_none() {
        return Some(arms);
    }
    let primitive = primitive.filter(|primitive| **primitive != PrimitiveType::Uuid)?;
    let mut singletons = Vec::new();
    for range in values.ranges() {
        if let Some(value) = range.single_value() {
            singletons.push(connector_datum(value, primitive)?);
            continue;
        }
        if beyond_representable(range.low(), true) || beyond_representable(range.high(), false) {
            continue;
        }
        let mut bounds = Vec::new();
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
            match bound {
                Bound::Unbounded => {}
                Bound::Inclusive(value) => {
                    bounds.push(binary(term, inclusive, connector_datum(value, primitive)?))
                }
                Bound::Exclusive(value) => {
                    bounds.push(binary(term, exclusive, connector_datum(value, primitive)?))
                }
            }
        }
        arms.push(bounds);
    }
    add_discrete(&mut arms, term, singletons);
    Some(arms)
}

fn beyond_representable(bound: &Bound, lower: bool) -> bool {
    let Bound::Exclusive(value) = bound else {
        return false;
    };
    let (value, min, max) = match value {
        ConnectorValue::TinyInt(v) => (i128::from(*v), i128::from(i8::MIN), i128::from(i8::MAX)),
        ConnectorValue::SmallInt(v) => (i128::from(*v), i128::from(i16::MIN), i128::from(i16::MAX)),
        ConnectorValue::Integer(v) | ConnectorValue::Date(v) => {
            (i128::from(*v), i128::from(i32::MIN), i128::from(i32::MAX))
        }
        ConnectorValue::BigInt(v)
        | ConnectorValue::TimestampMicros(v)
        | ConnectorValue::TimestampTzMicros(v)
        | ConnectorValue::TimestampNanos(v)
        | ConnectorValue::TimestampTzNanos(v) => {
            (i128::from(*v), i128::from(i64::MIN), i128::from(i64::MAX))
        }
        _ => return false,
    };
    if lower { value == max } else { value == min }
}

fn field_nullable(schema: &SchemaRef, id: i32) -> Option<bool> {
    let name = schema.name_by_field_id(id)?;
    if !schema.field_by_id(id)?.required {
        return Some(true);
    }
    // Test for a containing optional field as well as the leaf. Iceberg's
    // vendor binder checks the leaf's required flag alone for unary predicates.
    Some(name.match_indices('.').any(|(end, _)| {
        schema
            .field_by_name(&name[..end])
            .is_some_and(|field| !field.required)
    }))
}

fn connector_datum(value: &ConnectorValue, primitive: &PrimitiveType) -> Option<Datum> {
    let original = match value {
        ConnectorValue::Boolean(v) => Datum::bool(*v),
        ConnectorValue::TinyInt(v) => Datum::int(*v),
        ConnectorValue::SmallInt(v) => Datum::int(*v),
        ConnectorValue::Integer(v) => Datum::int(*v),
        ConnectorValue::BigInt(v) => Datum::long(*v),
        ConnectorValue::Real(v) => Datum::float(*v),
        ConnectorValue::Double(v) => Datum::double(*v),
        ConnectorValue::Decimal {
            unscaled, scale, ..
        } => {
            let PrimitiveType::Decimal { scale: target, .. } = primitive else {
                return None;
            };
            if i8::try_from(*target).ok()? != *scale {
                return None;
            }
            return Datum::try_from_bytes(&unscaled.to_be_bytes(), primitive.clone()).ok();
        }
        ConnectorValue::Date(v) => Datum::date(*v),
        ConnectorValue::TimeMicros(v) => Datum::time_micros(*v).ok()?,
        ConnectorValue::TimestampMicros(v) => Datum::timestamp_micros(*v),
        ConnectorValue::TimestampMillis(v) => Datum::timestamp_micros(v.checked_mul(1000)?),
        ConnectorValue::TimestampTzMicros(v) => Datum::timestamptz_micros(*v),
        ConnectorValue::TimestampNanos(v) => Datum::timestamp_nanos(*v),
        ConnectorValue::TimestampTzNanos(v) => Datum::timestamptz_nanos(*v),
        ConnectorValue::Temporal { .. } => return None,
        ConnectorValue::Varchar(v) => Datum::string(v),
        ConnectorValue::Varbinary(v) => Datum::binary(v.iter().copied()),
        ConnectorValue::Uuid(v) => Datum::uuid(Uuid::from_bytes(*v)),
        ConnectorValue::Fixed(v) => Datum::fixed(v.iter().copied()),
    };
    if original.data_type() == primitive {
        Some(original)
    } else {
        original.to(&Type::Primitive(primitive.clone())).ok()
    }
}

fn domain_contains_datum(domain: &Domain, value: &Datum) -> Option<bool> {
    if domain.values().is_all() {
        return Some(true);
    }
    if domain.values().is_none() {
        return Some(false);
    }
    // The shared domain algebra does not represent ordered NaN ranges.
    if value.is_nan() {
        return None;
    }
    for range in domain.values().ranges() {
        let comparator = |bound: &Bound, high: bool| {
            let (value_bound, inclusive) = match bound {
                Bound::Unbounded => return Some(true),
                Bound::Inclusive(value) => (value, true),
                Bound::Exclusive(value) => (value, false),
            };
            let bound = connector_datum(value_bound, value.data_type())?;
            // Domain composition uses the connector's numeric ordering, where
            // equal signed zero values intersect. Manifest bounds use Iceberg
            // ordering in the separate evaluator below.
            let order = match (value.literal(), bound.literal()) {
                (PrimitiveLiteral::Float(left), PrimitiveLiteral::Float(right)) => {
                    left.0.partial_cmp(&right.0)
                }
                (PrimitiveLiteral::Double(left), PrimitiveLiteral::Double(right)) => {
                    left.0.partial_cmp(&right.0)
                }
                _ => value.partial_cmp(&bound),
            }?;
            Some(if order.is_eq() {
                inclusive
            } else if high {
                order.is_lt()
            } else {
                order.is_gt()
            })
        };
        if comparator(range.low(), false)? && comparator(range.high(), true)? {
            return Some(true);
        }
    }
    Some(false)
}

impl fmt::Display for ConflictScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::All => f.write_str("true"),
            Self::Never => f.write_str("false"),
            Self::Domain(domain) => {
                for (column_index, predicates) in domain.columns.values().enumerate() {
                    if column_index != 0 {
                        f.write_str(" AND ")?;
                    }
                    let arms = match predicates {
                        ColumnPredicate::Projectable(arms) => arms,
                        ColumnPredicate::VariantPresence { name, op } => {
                            write!(f, "{name} {op}")?;
                            continue;
                        }
                    };
                    f.write_str("(")?;
                    for (arm_index, arm) in arms.iter().enumerate() {
                        if arm_index != 0 {
                            f.write_str(" OR ")?;
                        }
                        for (leaf_index, leaf) in arm.iter().enumerate() {
                            if leaf_index != 0 {
                                f.write_str(" AND ")?;
                            }
                            display_leaf(f, leaf)?;
                        }
                    }
                    f.write_str(")")?;
                }
                Ok(())
            }
        }
    }
}

fn display_leaf(f: &mut fmt::Formatter<'_>, predicate: &BoundPredicate) -> fmt::Result {
    // The vendor's date/timestamp formatter expects chrono's narrower
    // representable range. Preserve the signed literal in a conflict
    // diagnostic even when the value lies outside that range.
    let can_format = |datum: &Datum| match (datum.data_type(), datum.literal()) {
        (PrimitiveType::Date, PrimitiveLiteral::Int(days)) => {
            Date32Type::to_naive_date_opt(*days).is_some()
        }
        (PrimitiveType::Timestamp | PrimitiveType::Timestamptz, PrimitiveLiteral::Long(micros)) => {
            timestamp_us_to_datetime(*micros).is_some()
        }
        _ => true,
    };
    match predicate {
        BoundPredicate::Binary(expression) if !can_format(expression.literal()) => write!(
            f,
            "{} {} {:?}",
            expression.term(),
            expression.op(),
            expression.literal(),
        ),
        BoundPredicate::Set(expression) if !expression.literals().iter().all(can_format) => write!(
            f,
            "{} {} {:?}",
            expression.term(),
            expression.op(),
            expression.literals(),
        ),
        _ => write!(f, "{predicate}"),
    }
}

impl MayMatch for ConflictScope {
    fn may_match(&self, candidate: &CandidateFile<'_>) -> Result<bool, ConnectorError> {
        let domain = match self {
            Self::All => return Ok(true),
            Self::Never => return Ok(false),
            Self::Domain(domain) => domain,
        };
        let file = candidate.file;
        if file.file_format() != DataFileFormat::Puffin
            && (file.record_count() == 0
                || !domain.columns.iter().all(|(id, column)| match column {
                    ColumnPredicate::VariantPresence { op, .. } => {
                        unary_metrics_may_match(*op, *id, file)
                    }
                    ColumnPredicate::Projectable(_) => {
                        column.projectable_arms_may_match(|leaf| metrics_may_match(leaf, file))
                    }
                }))
        {
            return Ok(false);
        }
        let Some(spec) = candidate.metadata.partition_spec_by_id(candidate.spec_id) else {
            return Ok(true);
        };
        Ok(domain.columns.iter().all(|(id, column)| {
            let fields = spec
                .fields()
                .iter()
                .zip(file.partition().fields())
                .filter(|(field, _)| field.source_id == *id);
            column.projectable_arms_may_match(|leaf| {
                fields
                    .clone()
                    .all(|(field, value)| partition_may_match(field, leaf, value.as_ref()))
            })
        }))
    }
}

fn partition_may_match(
    field: &PartitionField,
    leaf: &BoundPredicate,
    value: Option<&Literal>,
) -> bool {
    if !projection_arithmetic_is_safe(&field.transform, leaf) {
        return true;
    }
    match field.transform.project(&field.name, leaf) {
        Ok(Some(predicate)) => {
            projected_may_match(&predicate, value)
                || legacy_time_partition(&field.transform, leaf, value)
                    .is_some_and(|legacy| projected_may_match(&predicate, Some(&legacy)))
        }
        Ok(None) | Err(_) => true,
    }
}

fn legacy_time_partition(
    transform: &Transform,
    leaf: &BoundPredicate,
    value: Option<&Literal>,
) -> Option<Literal> {
    // Iceberg Java 1.12.0 ProjectionUtil.fixInclusiveTimeProjection accepts
    // the original partition number and its historical +1 encoding for
    // negative timestamp transforms. The vendor already covers Year/Month
    // and Day(Timestamp), but omits Hour and Day(Timestamptz). Test the legacy
    // encoding's original value against the same vendor-authored predicate.
    let omitted = match transform {
        Transform::Hour => true,
        Transform::Day => match leaf {
            BoundPredicate::Binary(expression) => {
                expression.literal().data_type() == &PrimitiveType::Timestamptz
            }
            BoundPredicate::Set(expression) => expression
                .literals()
                .iter()
                .next()
                .is_some_and(|datum| datum.data_type() == &PrimitiveType::Timestamptz),
            _ => false,
        },
        _ => false,
    };
    match (omitted, value) {
        (true, Some(Literal::Primitive(PrimitiveLiteral::Int(value)))) if *value <= 0 => value
            .checked_sub(1)
            .map(|earlier| Literal::Primitive(PrimitiveLiteral::Int(earlier))),
        _ => None,
    }
}

fn projection_arithmetic_is_safe(transform: &Transform, leaf: &BoundPredicate) -> bool {
    if !matches!(
        transform,
        Transform::Truncate(_)
            | Transform::Day
            | Transform::Hour
            | Transform::Year
            | Transform::Month
    ) {
        return true;
    }
    let safe_datum = |datum: &Datum, adjust| {
        // Guard the vendored public projection's unchecked exclusive shifts
        // and truncate operations before asking it to author the predicate.
        let (value, minimum, maximum) = match datum.literal() {
            PrimitiveLiteral::Int(value) => (
                i128::from(*value),
                i128::from(i32::MIN),
                i128::from(i32::MAX),
            ),
            PrimitiveLiteral::Long(value) => (
                i128::from(*value),
                i128::from(i64::MIN),
                i128::from(i64::MAX),
            ),
            PrimitiveLiteral::Int128(value) => (*value, i128::MIN, i128::MAX),
            _ => return true,
        };
        let Some(value) = value.checked_add(adjust) else {
            return false;
        };
        if value < minimum || value > maximum {
            return false;
        }
        if let Transform::Truncate(width) = transform {
            let width = i128::from(*width);
            return width != 0
                && (!(minimum == i128::from(i32::MIN)) || width <= i128::from(i32::MAX))
                && value
                    .checked_sub(value.rem_euclid(width))
                    .is_some_and(|value| value >= minimum);
        }
        if matches!(transform, Transform::Year | Transform::Month)
            && datum.data_type() == &PrimitiveType::Date
        {
            return Date32Type::to_naive_date_opt(value as i32).is_some();
        }
        true
    };
    match leaf {
        BoundPredicate::Binary(expression) => {
            let adjustment = match expression.op() {
                PredicateOperator::LessThan => -1,
                PredicateOperator::GreaterThan => 1,
                _ => 0,
            };
            safe_datum(expression.literal(), adjustment)
        }
        BoundPredicate::Set(expression) => expression
            .literals()
            .iter()
            .all(|datum| safe_datum(datum, 0)),
        _ => true,
    }
}

fn projected_may_match(predicate: &Predicate, value: Option<&Literal>) -> bool {
    match predicate {
        Predicate::AlwaysTrue => true,
        Predicate::AlwaysFalse => false,
        Predicate::And(expression) => expression
            .inputs()
            .iter()
            .all(|p| projected_may_match(p, value)),
        Predicate::Or(expression) => expression
            .inputs()
            .iter()
            .any(|p| projected_may_match(p, value)),
        Predicate::Unary(expression) => match expression.op() {
            PredicateOperator::IsNull => value.is_none(),
            PredicateOperator::NotNull => value.is_some(),
            PredicateOperator::IsNan => match value {
                Some(Literal::Primitive(value)) => value.is_nan(),
                None => false,
                _ => true,
            },
            _ => true,
        },
        Predicate::Binary(expression) => {
            let Some(Literal::Primitive(value)) = value else {
                return value.is_some();
            };
            compare_partition(value, expression.literal().literal()).is_none_or(|order| {
                match expression.op() {
                    PredicateOperator::Eq => order.is_eq(),
                    PredicateOperator::LessThan => order.is_lt(),
                    PredicateOperator::LessThanOrEq => order.is_le(),
                    PredicateOperator::GreaterThan => order.is_gt(),
                    PredicateOperator::GreaterThanOrEq => order.is_ge(),
                    _ => true,
                }
            })
        }
        Predicate::Set(expression) if expression.op() == PredicateOperator::In => {
            let Some(Literal::Primitive(value)) = value else {
                return value.is_some();
            };
            expression.literals().iter().any(|candidate| {
                compare_partition(value, candidate.literal()).is_none_or(|order| order.is_eq())
            })
        }
        _ => true,
    }
}

fn compare_partition(
    left: &PrimitiveLiteral,
    right: &PrimitiveLiteral,
) -> Option<std::cmp::Ordering> {
    if std::mem::discriminant(left) == std::mem::discriminant(right) {
        left.partial_cmp(right)
    } else {
        // Manifests may carry a wider primitive after type promotion. A
        // projection bound in another primitive representation proves nothing.
        None
    }
}

fn metrics_may_match(leaf: &BoundPredicate, file: &DataFile) -> bool {
    // Iceberg Java 1.12.0 InclusiveEvalVisitor checks term counts before
    // evaluating typed, potentially truncated manifest bounds. The persisted
    // upper bound already includes the writer's conservative truncation.
    match leaf {
        BoundPredicate::Unary(expression) => {
            unary_metrics_may_match(expression.op(), expression.term().field().id, file)
        }
        BoundPredicate::Binary(expression) => {
            let literal = expression.literal();
            let Some((lower, upper)) =
                metric_bounds(file, expression.term().field().id, literal.data_type())
            else {
                return false;
            };
            let compare_lower = lower
                .as_deref()
                .and_then(|bound| bound.partial_cmp(literal));
            let compare_upper = upper
                .as_deref()
                .and_then(|bound| bound.partial_cmp(literal));
            match expression.op() {
                PredicateOperator::Eq => {
                    !compare_lower.is_some_and(|order| order.is_gt())
                        && !compare_upper.is_some_and(|order| order.is_lt())
                }
                PredicateOperator::LessThan => !compare_lower.is_some_and(|order| order.is_ge()),
                PredicateOperator::LessThanOrEq => {
                    !compare_lower.is_some_and(|order| order.is_gt())
                }
                PredicateOperator::GreaterThan => !compare_upper.is_some_and(|order| order.is_le()),
                PredicateOperator::GreaterThanOrEq => {
                    !compare_upper.is_some_and(|order| order.is_lt())
                }
                _ => true,
            }
        }
        BoundPredicate::Set(expression) => {
            // Match the Java evaluator's metrics short-circuit. The complete
            // set still participates in inclusive partition projection.
            const IN_PREDICATE_LIMIT: usize = 200;
            let literals = expression.literals();
            let Some(first) = literals.iter().next() else {
                return false;
            };
            let Some((lower, upper)) =
                metric_bounds(file, expression.term().field().id, first.data_type())
            else {
                return false;
            };
            let Some(lower) = lower.as_deref() else {
                return true;
            };
            literals.len() > IN_PREDICATE_LIMIT
                || literals.iter().any(|literal| {
                    lower.partial_cmp(literal).is_none_or(|order| order.is_le())
                        && upper
                            .as_deref()
                            .and_then(|bound| bound.partial_cmp(literal))
                            .is_none_or(|order| order.is_ge())
                })
        }
        _ => true,
    }
}

fn unary_metrics_may_match(op: PredicateOperator, id: i32, file: &DataFile) -> bool {
    let nulls = file.null_value_counts().get(&id);
    let values = file.value_counts().get(&id);
    let all_null = values.is_some() && values == nulls;
    match op {
        PredicateOperator::IsNull => nulls != Some(&0),
        PredicateOperator::NotNull => !all_null,
        PredicateOperator::IsNan => !all_null && file.nan_value_counts().get(&id) != Some(&0),
        _ => true,
    }
}

type MetricBounds<'a> = (Option<Cow<'a, Datum>>, Option<Cow<'a, Datum>>);

fn metric_bounds<'a>(
    file: &'a DataFile,
    id: i32,
    primitive: &PrimitiveType,
) -> Option<MetricBounds<'a>> {
    if let Some(count) = file.value_counts().get(&id)
        && (file.null_value_counts().get(&id) == Some(count)
            || file.nan_value_counts().get(&id) == Some(count))
    {
        return None;
    }
    let decode = |datum: &'a Datum| {
        // Older writers could place NaNs in bounds. An upper NaN in the Java
        // comparator is above every finite literal and cannot narrow it.
        if datum.is_nan() {
            return None;
        }
        if datum.data_type() == primitive {
            Some(Cow::Borrowed(datum))
        } else {
            let bytes = datum.to_bytes().ok()?;
            Datum::try_from_bytes(bytes.as_ref(), primitive.clone())
                .ok()
                .map(Cow::Owned)
        }
    };
    Some((
        file.lower_bounds().get(&id).and_then(decode),
        file.upper_bounds().get(&id).and_then(decode),
    ))
}

#[cfg(test)]
pub(crate) mod tests;
