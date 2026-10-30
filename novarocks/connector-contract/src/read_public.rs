// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Complete immutable source facts exposed to pure provider compilation.
use crate::owned_copy::{ObservedCopy, OwnedCopy, PlainCopy};
use crate::{
    ConnectorError, ConnectorErrorKind, ConnectorReadArtifactCoverage, ConnectorReadDistribution,
    ConnectorReadMetadataKind, ConnectorReadStaticFacts, MAX_CONNECTOR_RECIPE_COLUMNS,
    MAX_STATIC_SCAN_RETAINED_BYTES, ScanColumnId,
};
use crate::{PureProviderCompileError, WriterOwnedResourceFacts};
use arrow_schema::{DataType, Field, Schema};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use std::sync::Arc;

#[derive(Clone)]
pub struct ConnectorReadPublicFacts {
    source: ConnectorReadStaticFacts<ScanColumnId>,
    metadata_kind: Option<ConnectorReadMetadataKind>,
    schema: Arc<Schema>,
    logical_types: Arc<[ValueLogicalType]>,
    charged_bytes: usize,
    typed: Option<typed::TypedReadSchemas>,
    metadata_materializations: Option<novarocks_type_contract::owned_resources::metadata_materialization::SchemaMetadataMaterializations>,
}
impl PartialEq for ConnectorReadPublicFacts {
    fn eq(&self, other: &Self) -> bool {
        self.source == other.source
            && self.metadata_kind == other.metadata_kind
            && self.typed == other.typed
            && self.logical_types == other.logical_types
            && novarocks_type_contract::arrow_schemas_exact(&self.schema, &other.schema)
    }
}
impl Eq for ConnectorReadPublicFacts {}

impl ConnectorReadPublicFacts {
    pub fn try_new(
        source: ConnectorReadStaticFacts<ScanColumnId>,
        metadata_kind: Option<ConnectorReadMetadataKind>,
        schema: Schema,
        logical_types: Vec<ValueLogicalType>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_core(
            source,
            metadata_kind,
            &schema,
            logical_types,
            &mut PlainCopy,
        )
    }

    /// Copy a detached schema through the original read laws and owned-copy
    /// author. The caller owns the truthful source invoice, admission and
    /// checkpoints, including the entry and ordinary/success footer.
    pub fn try_new_from_borrowed_schema_observed(
        source: ConnectorReadStaticFacts<ScanColumnId>,
        metadata_kind: Option<ConnectorReadMetadataKind>,
        schema: &Schema,
        logical_types: Vec<ValueLogicalType>,
        source_retained_bytes: usize,
        admit: &mut impl FnMut(&WriterOwnedResourceFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, PureProviderCompileError<ConnectorError>> {
        let mut context = ObservedCopy::new(source_retained_bytes, admit, work)?;
        Self::try_new_core(source, metadata_kind, schema, logical_types, &mut context)
    }

    /// Optional source receipts follow the SAME original count/materialize
    /// body. They add no schema validity law or live execution capability.
    pub fn try_new_from_borrowed_schema_with_materializations_observed(
        source: ConnectorReadStaticFacts<ScanColumnId>,
        metadata_kind: Option<ConnectorReadMetadataKind>,
        schema: &Schema,
        logical_types: Vec<ValueLogicalType>,
        source_retained_bytes: usize,
        admit: &mut impl FnMut(&WriterOwnedResourceFacts) -> Result<(), CompileControlError>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, PureProviderCompileError<ConnectorError>> {
        let mut original = ObservedCopy::new(source_retained_bytes, admit, work)?;
        let mut context =
            crate::metadata_materialization_copy::MetadataMaterializationCopy::new(&mut original);
        let mut value =
            Self::try_new_core(source, metadata_kind, schema, logical_types, &mut context)?;
        value.metadata_materializations = Some(context.finish()?);
        Ok(value)
    }
    pub fn metadata_materializations(&self) -> Option<&novarocks_type_contract::owned_resources::metadata_materialization::SchemaMetadataMaterializations>{
        self.metadata_materializations.as_ref()
    }

    fn try_new_core<O: OwnedCopy>(
        source: ConnectorReadStaticFacts<ScanColumnId>,
        metadata_kind: Option<ConnectorReadMetadataKind>,
        schema: &Schema,
        logical_types: Vec<ValueLogicalType>,
        context: &mut O,
    ) -> Result<Self, O::Error> {
        validate_header(&source, metadata_kind.as_ref(), schema, &logical_types)
            .map_err(O::Error::from)?;
        let metadata_buckets = crate::schema::preflight_owned_schema(schema, context)?;
        context.array::<ValueLogicalType>(logical_types.len(), 1)?;
        context.arc_slice::<ValueLogicalType>(logical_types.len())?;
        context.source_floor(context.add(
            size_of::<Vec<ValueLogicalType>>(),
            context.mul(logical_types.capacity(), size_of::<ValueLogicalType>())?,
        )?)?;
        context.work(context.add(
            context.mul(logical_types.len(), 2 * size_of::<ValueLogicalType>())?,
            128,
        )?)?;
        let distribution_keys = match source.properties().distribution() {
            ConnectorReadDistribution::Hash { keys, .. }
            | ConnectorReadDistribution::BucketShuffle { keys, .. } => keys.len(),
            _ => 0,
        };
        context.work(context.add(
            context.mul(
                context.add(distribution_keys, source.properties().ordering().len())?,
                128,
            )?,
            256,
        )?)?;
        context.source_floor(context.add(
            size_of::<ConnectorReadStaticFacts<ScanColumnId>>(),
            context.add(
                source.input_version().as_bytes().len(),
                source.coverage_evidence().len(),
            )?,
        )?)?;
        let mut budget = SchemaBudget::default();
        budget.add(
            std::mem::size_of::<Self>()
                + source.input_version().as_bytes().len()
                + source.coverage_evidence().len()
                + metadata_kind.as_ref().map_or(0, |kind| kind.as_str().len()),
        )?;
        budget.metadata_observed(schema.metadata(), context)?;
        for (field, logical) in schema.fields().iter().zip(&logical_types) {
            completed_law(
                logical
                    .validate_carrier(field.data_type())
                    .map_err(|error| invalid(error.to_string())),
                context,
            )?;
            let nodes = validate_nested_observed(field.data_type(), context)?;
            preflight_metadata_validation(field.metadata(), context)?;
            let root_label = (|| -> Result<(), ConnectorError> {
                if field
                    .metadata()
                    .contains_key(novarocks_type_contract::NR_LOGICAL_TYPE_KEY)
                    && novarocks_type_contract::field_logical_type(field)
                        .map_err(|error| invalid(error.to_string()))?
                        != *logical
                {
                    return Err(invalid(
                        "read root field label differs from its explicit logical identity",
                    ));
                }
                Ok(())
            })();
            completed_law(root_label, context)?;
            budget.field_observed(field, context)?;
            budget.data_type(field.data_type(), nodes, context)?;
        }
        let tail = (|| -> Result<(), ConnectorError> {
            let check_column = |column: &ScanColumnId| {
                if column.index() >= schema.fields().len() {
                    Err(invalid(
                        "read property names an absent schema column ordinal",
                    ))
                } else {
                    Ok(())
                }
            };
            match source.properties().distribution() {
                ConnectorReadDistribution::Hash { keys, .. }
                | ConnectorReadDistribution::BucketShuffle { keys, .. } => {
                    for key in keys.iter() {
                        check_column(key)?;
                    }
                    budget.add(keys.len() * std::mem::size_of::<ScanColumnId>())?;
                }
                _ => {}
            }
            for key in source.properties().ordering() {
                check_column(key.column())?;
            }
            budget.add(std::mem::size_of_val(source.properties().ordering()))?;
            if let ConnectorReadArtifactCoverage::Exact {
                source_selection_digest,
                content_digest,
                evidence,
            } = source.artifact_coverage()
            {
                if *source_selection_digest == [0; 32]
                    || *content_digest == [0; 32]
                    || evidence.is_empty()
                    || evidence.len() > crate::MAX_READ_COVERAGE_EVIDENCE_BYTES
                {
                    return Err(invalid("read artifact coverage has invalid exact evidence"));
                }
                budget.add(evidence.len())?;
            }
            Ok(())
        })();
        completed_law(tail, context)?;
        // No schema backing is copied until all original read laws succeed.
        // The count pass returns no partial Schema or public facts owner.
        let counted = crate::schema::owned_schema_core(schema, metadata_buckets, context)?;
        let schema = if context.materializes() {
            counted
        } else {
            context.begin_copy()?;
            crate::schema::owned_schema_core(schema, metadata_buckets, context)?
        }
        .ok_or_else(|| O::Error::from(invalid("read schema copy did not materialize")))?;
        context.flush()?;
        let logical_types = logical_types.into_boxed_slice();
        context.step()?;
        context.flush()?;
        let logical_types = logical_types.into();
        context.step()?;
        context.flush()?;
        Ok(Self {
            source,
            metadata_kind,
            schema,
            logical_types,
            charged_bytes: budget.bytes,
            typed: None,
            metadata_materializations: None,
        })
    }

    pub const fn source(&self) -> &ConnectorReadStaticFacts<ScanColumnId> {
        &self.source
    }
    pub const fn metadata_kind(&self) -> Option<&ConnectorReadMetadataKind> {
        self.metadata_kind.as_ref()
    }
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
    pub fn logical_types(&self) -> &[ValueLogicalType] {
        &self.logical_types
    }
    pub const fn charged_bytes(&self) -> usize {
        self.charged_bytes
    }
    pub fn matches_value_type(&self, ordinal: usize, ty: &FunctionValueType) -> bool {
        if let Some(typed) = &self.typed {
            return typed
                .execution
                .binding(ordinal)
                .is_some_and(|binding| binding == ty);
        }
        self.schema.fields().get(ordinal).is_some_and(|field| {
            self.logical_types[ordinal] == ty.logical_type()
                && field.is_nullable() == ty.nullable()
                && novarocks_type_contract::arrow_data_types_exact(
                    field.data_type(),
                    ty.data_type(),
                )
        })
    }
}

fn validate_header(
    source: &ConnectorReadStaticFacts<ScanColumnId>,
    metadata_kind: Option<&ConnectorReadMetadataKind>,
    schema: &Schema,
    logical_types: &[ValueLogicalType],
) -> Result<(), ConnectorError> {
    if schema.fields().len() > MAX_CONNECTOR_RECIPE_COLUMNS {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "read public schema exceeds the frozen column count limit",
        ));
    }
    if schema.fields().is_empty() || schema.fields().len() != logical_types.len() {
        return Err(invalid("read public schema has an invalid column count"));
    }
    if metadata_kind.is_some() && source.coverage_evidence().is_empty() {
        return Err(invalid("metadata read has no frozen coverage evidence"));
    }
    Ok(())
}

fn completed_law<T, O: OwnedCopy>(
    result: Result<T, ConnectorError>,
    context: &mut O,
) -> Result<T, O::Error> {
    context.step()?;
    context.flush()?;
    result.map_err(Into::into)
}

fn preflight_metadata_validation<O: OwnedCopy>(
    metadata: &arrow_schema::Metadata,
    context: &mut O,
) -> Result<(), O::Error> {
    // Metadata iteration follows Arrow 60's BTree cursor. Logical-label
    // probing and byte comparisons retain their separate opaque work quote;
    // this observation does not grant the source's retained backing.
    context.metadata_iteration(metadata.len(), 1)?;
    context.work(context.add(context.mul(metadata.len(), 128)?, 128)?)
}

enum LogicalWalkError<E> {
    Law(novarocks_type_contract::ValueTypeError),
    Observer(E),
}
impl<E> From<novarocks_type_contract::ValueTypeError> for LogicalWalkError<E> {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::Law(error)
    }
}
fn validate_nested_observed<O: OwnedCopy>(
    root: &DataType,
    context: &mut O,
) -> Result<usize, O::Error> {
    use novarocks_type_contract::*;
    if context.source_invoice().is_none() {
        validate_nested_logical_types(root).map_err(value_type_error)?;
        return Ok(0);
    }
    // The sole fixed-scratch grammar avoids a per-root worst-case heap Vec.
    // This admits its actual fixed inline initialization and closed walker
    // work; it is neither a stack grant nor a second type checker.
    context.work(context.add(
        context.mul(
            MAX_VALUE_TYPE_NODES,
            size_of::<Option<(&DataType, usize)>>() + 128,
        )?,
        128,
    )?)?;
    context.flush()?;
    let mut scratch = [None; MAX_VALUE_TYPE_NODES];
    context.step()?;
    context.flush()?;
    let mut nodes = 0usize;
    let result = validate_value_type_structure_with_scratch_observed(
        root,
        &mut scratch,
        |event| -> Result<(), LogicalWalkError<O::Error>> {
            match event {
                ValueTypeVisit::TypeNode(_) => {
                    nodes += 1;
                }
                ValueTypeVisit::ChildEdge(_) => {}
                ValueTypeVisit::Field(field) => {
                    preflight_metadata_validation(field.metadata(), context)
                        .map_err(LogicalWalkError::Observer)?;
                }
            }
            context.step().map_err(LogicalWalkError::Observer)
        },
    );
    match result {
        Err(LogicalWalkError::Observer(error)) => Err(error),
        other => completed_law(
            other.map_err(|error| match error {
                LogicalWalkError::Law(error) => value_type_error(error),
                LogicalWalkError::Observer(_) => unreachable!(),
            }),
            context,
        )
        .map(|()| nodes),
    }
}

fn preflight_validation_stack<O: OwnedCopy>(nodes: usize, context: &mut O) -> Result<(), O::Error> {
    if context.source_invoice().is_none() {
        return Ok(());
    }
    // Original Vec<&DataType>: initial one slot, then the locked minimum-four
    // doubling growth. Actual node visits from the sole logical walker bound
    // pending entries. Every possible request is counted before scratch init.
    context.array::<&DataType>(1, 1)?;
    let mut capacity = 1usize;
    while capacity < nodes {
        capacity = if capacity == 1 {
            4
        } else {
            context.mul(capacity, 2)?
        };
        context.array::<&DataType>(capacity, 1)?;
        context.work(context.mul(capacity, 2 * size_of::<&DataType>())?)?;
    }
    context.work(context.add(context.mul(nodes, 128)?, 128)?)
}

fn push_validation<'a, O: OwnedCopy>(
    pending: &mut Vec<&'a DataType>,
    value: &'a DataType,
    context: &mut O,
) -> Result<(), O::Error> {
    if pending.len() == pending.capacity() {
        context.flush()?;
        let result = pending.try_reserve(1);
        context.reserve_exit(result)?;
    }
    pending.push(value);
    context.step()
}

#[derive(Default)]
struct SchemaBudget {
    bytes: usize,
}
impl SchemaBudget {
    fn add(&mut self, bytes: usize) -> Result<(), ConnectorError> {
        self.bytes = self.bytes.checked_add(bytes).ok_or_else(exhausted)?;
        if self.bytes > MAX_STATIC_SCAN_RETAINED_BYTES {
            return Err(exhausted());
        }
        Ok(())
    }
    fn metadata(&mut self, metadata: &arrow_schema::Metadata) -> Result<(), ConnectorError> {
        use novarocks_type_contract::*;
        if metadata.len() > MAX_ARROW_FIELD_METADATA_ENTRIES {
            return Err(exhausted());
        }
        let mut text = 0usize;
        for (key, value) in metadata {
            if key.len() > MAX_ARROW_FIELD_METADATA_KEY_BYTES
                || value.len() > MAX_ARROW_FIELD_METADATA_VALUE_BYTES
            {
                return Err(exhausted());
            }
            text = text
                .checked_add(key.len())
                .and_then(|n| n.checked_add(value.len()))
                .ok_or_else(exhausted)?;
        }
        if text > MAX_ARROW_FIELD_METADATA_BYTES {
            return Err(exhausted());
        }
        // A conservative collection charge, separate from E02's generated
        // decode/compile resource model. Owned backing cannot retain source capacity.
        let backing =
            novarocks_type_contract::owned_resources::arrow_metadata::retained_backing_upper_bound(
                metadata.len(),
            )
            .map_err(|_| exhausted())?;
        self.add(text.checked_add(backing).ok_or_else(exhausted)?)
    }
    fn field(&mut self, field: &Field) -> Result<(), ConnectorError> {
        if field.name().len() > novarocks_type_contract::MAX_ARROW_FIELD_NAME_BYTES {
            return Err(exhausted());
        }
        self.add(
            std::mem::size_of::<Field>() + field.name().len() + 4 * std::mem::size_of::<usize>(),
        )?;
        self.metadata(field.metadata())
    }
    fn metadata_observed<O: OwnedCopy>(
        &mut self,
        metadata: &arrow_schema::Metadata,
        context: &mut O,
    ) -> Result<(), O::Error> {
        preflight_metadata_validation(metadata, context)?;
        completed_law(self.metadata(metadata), context)
    }
    fn attributes(
        &mut self,
        metadata: &std::collections::HashMap<String, String>,
    ) -> Result<(), ConnectorError> {
        use novarocks_type_contract::*;
        if metadata.len() > MAX_ARROW_FIELD_METADATA_ENTRIES {
            return Err(exhausted());
        }
        let mut text = 0usize;
        for (key, value) in metadata {
            if key.len() > MAX_ARROW_FIELD_METADATA_KEY_BYTES
                || value.len() > MAX_ARROW_FIELD_METADATA_VALUE_BYTES
            {
                return Err(exhausted());
            }
            text = text
                .checked_add(key.len())
                .and_then(|n| n.checked_add(value.len()))
                .ok_or_else(exhausted)?;
        }
        if text > MAX_ARROW_FIELD_METADATA_BYTES {
            return Err(exhausted());
        }
        // A conservative collection charge, separate from E02's generated
        // decode/compile resource model. Owned backing cannot retain source capacity.
        self.add(
            text + metadata.len()
                * 4
                * (2 * std::mem::size_of::<String>() + std::mem::size_of::<usize>()),
        )
    }
    fn attributes_with_bound_observed<O: OwnedCopy>(
        &mut self,
        metadata: &std::collections::HashMap<String, String>,
        source_work_bound: Option<usize>,
        context: &mut O,
    ) -> Result<(), O::Error> {
        context.metadata_iteration_with_bound(metadata.len(), 1, source_work_bound)?;
        context.work(context.add(context.mul(metadata.len(), 128)?, 128)?)?;
        completed_law(self.attributes(metadata), context)
    }
    fn field_observed<O: OwnedCopy>(
        &mut self,
        field: &Field,
        context: &mut O,
    ) -> Result<(), O::Error> {
        preflight_metadata_validation(field.metadata(), context)?;
        context.source_floor(context.add(
            size_of::<Field>(),
            context.add(
                field.name().capacity(),
                context.mul(field.metadata().len(), size_of::<(String, String)>())?,
            )?,
        )?)?;
        completed_law(self.field(field), context)
    }
    fn data_type<O: OwnedCopy>(
        &mut self,
        root: &DataType,
        nodes: usize,
        context: &mut O,
    ) -> Result<(), O::Error> {
        // The logical preflight already bounded depth/nodes before this walk.
        preflight_validation_stack(nodes, context)?;
        context.flush()?;
        let mut pending = Vec::new();
        let reserved = pending.try_reserve_exact(1);
        context.reserve_exit(reserved)?;
        push_validation(&mut pending, root, context)?;
        context.step()?;
        context.flush()?;
        while let Some(ty) = pending.pop() {
            context.step()?;
            self.add(2 * std::mem::size_of::<DataType>())?;
            match ty {
                DataType::List(f)
                | DataType::LargeList(f)
                | DataType::ListView(f)
                | DataType::LargeListView(f)
                | DataType::FixedSizeList(f, _)
                | DataType::Map(f, _) => {
                    self.field_observed(f, context)?;
                    push_validation(&mut pending, f.data_type(), context)?;
                }
                DataType::Struct(fields) => {
                    for field in fields {
                        self.field_observed(field, context)?;
                        push_validation(&mut pending, field.data_type(), context)?;
                    }
                }
                DataType::Union(fields, _) => {
                    for (_, field) in fields.iter() {
                        self.field_observed(field, context)?;
                        push_validation(&mut pending, field.data_type(), context)?;
                    }
                }
                DataType::Dictionary(key, value) => {
                    push_validation(&mut pending, key, context)?;
                    push_validation(&mut pending, value, context)?;
                }
                DataType::RunEndEncoded(ends, values) => {
                    self.field_observed(ends, context)?;
                    self.field_observed(values, context)?;
                    push_validation(&mut pending, ends.data_type(), context)?;
                    push_validation(&mut pending, values.data_type(), context)?;
                }
                DataType::Timestamp(_, Some(zone)) => {
                    if zone.len() > novarocks_type_contract::MAX_ARROW_TIMESTAMP_TIMEZONE_BYTES {
                        return Err(exhausted().into());
                    }
                    self.add(zone.len())?;
                }
                _ => {}
            }
        }
        Ok(())
    }
}
fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}
fn value_type_error(error: novarocks_type_contract::ValueTypeError) -> ConnectorError {
    use novarocks_type_contract::ValueTypeError;
    let kind = match error {
        ValueTypeError::TooDeep | ValueTypeError::TooManyNodes => {
            ConnectorErrorKind::ResourceExhausted
        }
        ValueTypeError::UnknownLogicalMetadata
        | ValueTypeError::InvalidLogicalCarrier(_)
        | ValueTypeError::InvalidType(_) => ConnectorErrorKind::InvalidRequest,
    };
    ConnectorError::new(kind, error.to_string())
}
fn exhausted() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "read public schema exceeds the frozen structural byte limit",
    )
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::{ConnectorReadInputVersion, ConnectorReadProperties};
    pub(crate) fn source() -> ConnectorReadStaticFacts<ScanColumnId> {
        ConnectorReadStaticFacts::try_new(
            ConnectorReadInputVersion::try_new([9]).unwrap(),
            [7; 32],
            ConnectorReadProperties::try_new(ConnectorReadDistribution::Unconstrained, vec![])
                .unwrap(),
            ConnectorReadArtifactCoverage::NoArtifactInputs,
            vec![],
        )
        .unwrap()
    }
    pub(crate) fn public() -> ConnectorReadPublicFacts {
        ConnectorReadPublicFacts::try_new(
            source(),
            None,
            Schema::new(vec![Field::new("v", DataType::Int64, false)]),
            vec![ValueLogicalType::Physical],
        )
        .unwrap()
    }
    fn physical_schema(schema: Schema) -> Result<ConnectorReadPublicFacts, ConnectorError> {
        let logical_types = vec![ValueLogicalType::Physical; schema.fields().len()];
        ConnectorReadPublicFacts::try_new(source(), None, schema, logical_types)
    }

    #[test]
    fn schema_count_exhaustion_stays_distinct_from_invalid_shape() {
        let fields = (0..MAX_CONNECTOR_RECIPE_COLUMNS)
            .map(|index| Field::new(format!("v{index}"), DataType::Int64, false))
            .collect::<Vec<_>>();
        assert!(physical_schema(Schema::new(fields.clone())).is_ok());
        let mut over = fields;
        over.push(Field::new("over", DataType::Int64, false));
        assert_eq!(
            physical_schema(Schema::new(over)).unwrap_err().kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(
            physical_schema(Schema::empty()).unwrap_err().kind(),
            ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(
            ConnectorReadPublicFacts::try_new(
                source(),
                None,
                Schema::new(vec![Field::new("v", DataType::Int64, false)]),
                vec![],
            )
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn nested_depth_and_node_exhaustion_keep_resource_classification() {
        use novarocks_type_contract::{MAX_VALUE_TYPE_DEPTH, MAX_VALUE_TYPE_NODES};
        let mut nested = DataType::Int64;
        for _ in 1..MAX_VALUE_TYPE_DEPTH {
            nested = DataType::List(Arc::new(Field::new("item", nested, true)));
        }
        assert!(physical_schema(Schema::new(vec![Field::new("v", nested.clone(), false)])).is_ok());
        let over = DataType::List(Arc::new(Field::new("item", nested, true)));
        assert_eq!(
            physical_schema(Schema::new(vec![Field::new("v", over, false)]))
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        for (children, expected) in [
            (MAX_VALUE_TYPE_NODES - 1, None),
            (
                MAX_VALUE_TYPE_NODES,
                Some(ConnectorErrorKind::ResourceExhausted),
            ),
        ] {
            let fields = (0..children)
                .map(|index| Field::new(format!("v{index}"), DataType::Int64, true))
                .collect::<Vec<_>>();
            let schema = Schema::new(vec![Field::new(
                "v",
                DataType::Struct(fields.into()),
                false,
            )]);
            assert_eq!(
                physical_schema(schema).err().map(|error| error.kind()),
                expected
            );
        }
    }

    #[test]
    fn invalid_logical_identity_remains_an_invalid_request() {
        assert_eq!(
            ConnectorReadPublicFacts::try_new(
                source(),
                None,
                Schema::new(vec![Field::new("v", DataType::Int64, false)]),
                vec![ValueLogicalType::Json],
            )
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        let child =
            Field::new("item", DataType::Utf8, true).with_metadata(std::collections::HashMap::<
                String,
                String,
            >::from([(
                novarocks_type_contract::NR_LOGICAL_TYPE_KEY.into(),
                "unknown".into(),
            )]));
        assert_eq!(
            physical_schema(Schema::new(vec![Field::new(
                "v",
                DataType::List(Arc::new(child)),
                false,
            )]))
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn aggregate_schema_retention_accepts_exact_limit_and_refuses_one_extra_byte() {
        let mut fields = (0..MAX_CONNECTOR_RECIPE_COLUMNS)
            .map(|index| {
                Field::new(format!("v{index}"), DataType::Int64, false).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "k".into(),
                        String::new(),
                    )]),
                )
            })
            .collect::<Vec<_>>();
        let baseline = physical_schema(Schema::new(fields.clone())).unwrap();
        let mut remaining = MAX_STATIC_SCAN_RETAINED_BYTES - baseline.charged_bytes();
        for field in &mut fields {
            let value_bytes =
                remaining.min(novarocks_type_contract::MAX_ARROW_FIELD_METADATA_VALUE_BYTES);
            *field =
                field
                    .clone()
                    .with_metadata(std::collections::HashMap::<String, String>::from([(
                        "k".into(),
                        "x".repeat(value_bytes),
                    )]));
            remaining -= value_bytes;
        }
        assert_eq!(
            remaining, 0,
            "individual metadata limits permit the aggregate boundary"
        );
        let exact = physical_schema(Schema::new(fields.clone())).unwrap();
        assert_eq!(exact.charged_bytes(), MAX_STATIC_SCAN_RETAINED_BYTES);
        let last = fields.last_mut().unwrap();
        assert!(last.metadata()["k"].is_empty());
        *last = last
            .clone()
            .with_metadata(std::collections::HashMap::<String, String>::from([(
                "k".into(),
                "x".into(),
            )]));
        assert_eq!(
            physical_schema(Schema::new(fields)).unwrap_err().kind(),
            ConnectorErrorKind::ResourceExhausted
        );
    }
    #[test]
    fn public_schema_preserves_full_physical_and_explicit_logical_identity() {
        let mut metadata = std::collections::HashMap::with_capacity(10_000);
        let mut value = String::with_capacity(10_000);
        value.push_str("value");
        metadata.insert("provider".into(), value);
        let field = Field::new("json", DataType::Utf8, true).with_metadata(metadata.clone());
        let schema = Schema::new_with_metadata(vec![field], metadata);
        let facts =
            ConnectorReadPublicFacts::try_new(source(), None, schema, vec![ValueLogicalType::Json])
                .unwrap();
        assert_eq!(facts.schema().metadata().len(), 1);
        assert_eq!(
            facts.schema().metadata()["provider"].capacity(),
            "value".len()
        );
        assert_eq!(facts.schema().fields()[0].metadata().len(), 1);
        assert_eq!(
            facts.schema().fields()[0].metadata()["provider"].capacity(),
            "value".len()
        );
        assert!(
            facts.matches_value_type(
                0,
                &FunctionValueType::try_with_logical_type(
                    DataType::Utf8,
                    true,
                    ValueLogicalType::Json
                )
                .unwrap()
            )
        );
        assert!(!facts.matches_value_type(0, &FunctionValueType::new(DataType::Utf8, true)));
        let field =
            Field::new("v", DataType::Utf8, true).with_metadata(std::collections::HashMap::<
                String,
                String,
            >::from([(
                novarocks_type_contract::NR_LOGICAL_TYPE_KEY.into(),
                "json".into(),
            )]));
        assert!(
            ConnectorReadPublicFacts::try_new(
                source(),
                None,
                Schema::new(vec![field]),
                vec![ValueLogicalType::Physical]
            )
            .is_err()
        );
    }
    #[test]
    fn public_field_limits_have_near_and_over_evidence() {
        use novarocks_type_contract::*;
        for (name_len, valid) in [
            (MAX_ARROW_FIELD_NAME_BYTES, true),
            (MAX_ARROW_FIELD_NAME_BYTES + 1, false),
        ] {
            let schema = Schema::new(vec![Field::new(
                "v".repeat(name_len),
                DataType::Int64,
                false,
            )]);
            assert_eq!(
                ConnectorReadPublicFacts::try_new(
                    source(),
                    None,
                    schema,
                    vec![ValueLogicalType::Physical]
                )
                .is_ok(),
                valid
            );
        }
        for (value_len, valid) in [
            (MAX_ARROW_FIELD_METADATA_VALUE_BYTES, true),
            (MAX_ARROW_FIELD_METADATA_VALUE_BYTES + 1, false),
        ] {
            let field =
                Field::new("v", DataType::Int64, false).with_metadata(std::collections::HashMap::<
                    String,
                    String,
                >::from([(
                    "key".into(),
                    "x".repeat(value_len),
                )]));
            assert_eq!(
                ConnectorReadPublicFacts::try_new(
                    source(),
                    None,
                    Schema::new(vec![field]),
                    vec![ValueLogicalType::Physical]
                )
                .is_ok(),
                valid
            );
        }
        for (count, valid) in [
            (MAX_ARROW_FIELD_METADATA_ENTRIES, true),
            (MAX_ARROW_FIELD_METADATA_ENTRIES + 1, false),
        ] {
            let metadata: std::collections::HashMap<String, String> =
                (0..count).map(|i| (format!("k{i}"), "v".into())).collect();
            let field = Field::new("v", DataType::Int64, false).with_metadata(metadata);
            assert_eq!(
                ConnectorReadPublicFacts::try_new(
                    source(),
                    None,
                    Schema::new(vec![field]),
                    vec![ValueLogicalType::Physical]
                )
                .is_ok(),
                valid
            );
        }
    }
    #[test]
    fn source_properties_and_metadata_need_exact_schema_coverage() {
        let props = crate::ConnectorReadProperties::try_new(
            ConnectorReadDistribution::Unconstrained,
            vec![crate::ConnectorReadOrderingKey::new(
                ScanColumnId::new(1),
                crate::ConnectorReadSortDirection::Ascending,
                crate::ConnectorReadNullOrdering::First,
            )],
        )
        .unwrap();
        let source = ConnectorReadStaticFacts::try_new(
            ConnectorReadInputVersion::try_new([9]).unwrap(),
            [7; 32],
            props,
            ConnectorReadArtifactCoverage::NoArtifactInputs,
            vec![],
        )
        .unwrap();
        assert!(
            ConnectorReadPublicFacts::try_new(
                source,
                None,
                Schema::new(vec![Field::new("v", DataType::Int64, false)]),
                vec![ValueLogicalType::Physical]
            )
            .is_err()
        );
        assert!(
            ConnectorReadPublicFacts::try_new(
                self::source(),
                Some(ConnectorReadMetadataKind::try_new("$files").unwrap()),
                Schema::new(vec![Field::new("v", DataType::Int64, false)]),
                vec![ValueLogicalType::Physical]
            )
            .is_err()
        );
    }
}

#[cfg(test)]
#[path = "read_public/owned_tests.rs"]
mod owned_tests;

impl std::fmt::Debug for ConnectorReadPublicFacts {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        out.debug_struct("ConnectorReadPublicFacts")
            .field("source", &self.source)
            .field("metadata_kind", &self.metadata_kind)
            .field("schema", &self.schema)
            .field("logical_types", &self.logical_types)
            .field("charged_bytes", &self.charged_bytes)
            .finish()
    }
}

mod typed;
pub use typed::TypedReadCreationFailure;
