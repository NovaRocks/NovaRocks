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

//! Canonical, bounded conversion between runtime values and private generated
//! persistence DTOs. No function in this module performs I/O.

pub mod keyed;
mod model;
mod wire;

#[cfg(test)]
mod tests;

pub use model::*;

use prost::Message;

use crate::persistence::generated as proto;
use crate::persistence::identity::{
    AggregateIdentity, ApplyKeyIdentity, BranchIdentity, ComputationIdentity, DocumentRevision,
    FieldIdentity, IdentityError, ObjectIdentity, OutputIdentity, PartitionSpecVersion,
    SchemaVersion, StateSlotIdentity,
};
use crate::persistence::validation::{
    PersistenceDecodeBudget, ValidationError, validate_configuration, validate_definition,
    validate_interpretation, verify_computation_identity,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodedDocument {
    bytes: Vec<u8>,
    revision: DocumentRevision,
}

impl EncodedDocument {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    pub fn revision(&self) -> DocumentRevision {
        self.revision
    }
}

#[derive(Debug)]
pub enum PersistenceCodecError {
    ResourceBudget {
        resource: &'static str,
        maximum: usize,
        actual: usize,
    },
    MalformedWire(String),
    UnknownFormatVersion {
        document: &'static str,
        version: u32,
    },
    MissingField(&'static str),
    UnknownEnum {
        field: &'static str,
        value: i32,
    },
    InvalidIdentity(IdentityError),
    InvalidDocument(ValidationError),
    ProtobufDecode(prost::DecodeError),
}

/// These reasons belong to the persistence codec, not a repository or compiler.
pub mod failure_reason {
    use std::num::NonZeroU16;
    pub const RESOURCE_BUDGET: NonZeroU16 = NonZeroU16::new(930).unwrap();
    pub const MALFORMED_WIRE: NonZeroU16 = NonZeroU16::new(931).unwrap();
    pub const UNKNOWN_FORMAT: NonZeroU16 = NonZeroU16::new(932).unwrap();
    pub const MISSING_FIELD: NonZeroU16 = NonZeroU16::new(933).unwrap();
    pub const UNKNOWN_ENUM: NonZeroU16 = NonZeroU16::new(934).unwrap();
    pub const INVALID_IDENTITY: NonZeroU16 = NonZeroU16::new(935).unwrap();
    pub const INVALID_DOCUMENT: NonZeroU16 = NonZeroU16::new(936).unwrap();
    pub const PROTOBUF_DECODE: NonZeroU16 = NonZeroU16::new(937).unwrap();
}
impl novarocks_failure_contract::FailureProjection for PersistenceCodecError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass as C, FailureFact, FailureReason, domain};
        let (class, reason) = match self {
            Self::ResourceBudget { .. } => (C::Resource, failure_reason::RESOURCE_BUDGET),
            Self::MalformedWire(_) => (C::Data, failure_reason::MALFORMED_WIRE),
            Self::UnknownFormatVersion { .. } => (C::Unsupported, failure_reason::UNKNOWN_FORMAT),
            Self::MissingField(_) => (C::Data, failure_reason::MISSING_FIELD),
            Self::UnknownEnum { .. } => (C::Data, failure_reason::UNKNOWN_ENUM),
            Self::InvalidIdentity(_) => (C::Data, failure_reason::INVALID_IDENTITY),
            Self::InvalidDocument(_) => (C::Data, failure_reason::INVALID_DOCUMENT),
            Self::ProtobufDecode(_) => (C::Data, failure_reason::PROTOBUF_DECODE),
        };
        FailureFact::new(class, FailureReason::new(domain::METADATA, reason))
    }
}

impl std::fmt::Display for PersistenceCodecError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ResourceBudget {
                resource,
                maximum,
                actual,
            } => write!(
                formatter,
                "MV persistence {resource} uses {actual} bytes/items, exceeding the limit {maximum}"
            ),
            Self::MalformedWire(message) => write!(formatter, "malformed MV protobuf: {message}"),
            Self::UnknownFormatVersion { document, version } => write!(
                formatter,
                "unsupported MV {document} format version {version}"
            ),
            Self::MissingField(field) => {
                write!(
                    formatter,
                    "MV persistence document is missing required field {field}"
                )
            }
            Self::UnknownEnum { field, value } => {
                write!(
                    formatter,
                    "MV persistence field {field} has unknown enum value {value}"
                )
            }
            Self::InvalidIdentity(error) => write!(formatter, "invalid MV identity: {error}"),
            Self::InvalidDocument(error) => write!(formatter, "invalid MV document: {error}"),
            Self::ProtobufDecode(error) => {
                write!(formatter, "failed to decode MV protobuf: {error}")
            }
        }
    }
}

impl std::error::Error for PersistenceCodecError {}

impl From<ValidationError> for PersistenceCodecError {
    fn from(value: ValidationError) -> Self {
        Self::InvalidDocument(value)
    }
}

impl From<IdentityError> for PersistenceCodecError {
    fn from(value: IdentityError) -> Self {
        Self::InvalidIdentity(value)
    }
}

pub fn build_definition(
    created_at_ms: u64,
    query: QuerySource,
    relation_occurrences: Vec<RelationOccurrence>,
    outputs: Vec<OutputDefinition>,
) -> Result<DefinitionDocument, PersistenceCodecError> {
    let mut document = DefinitionDocument {
        created_at_ms,
        query,
        relation_occurrences,
        outputs,
        computation_identity: ComputationIdentity::from_canonical_bytes(&[]),
    };
    preflight_definition_source(&document)?;
    canonicalize_definition(&mut document);
    validate_definition(&document)?;
    document.computation_identity = compute_definition_identity(&document);
    Ok(document)
}

pub fn encode_definition(
    document: &DefinitionDocument,
) -> Result<EncodedDocument, PersistenceCodecError> {
    preflight_definition_source(document)?;
    let mut canonical = document.clone();
    canonicalize_definition(&mut canonical);
    validate_definition(&canonical)?;
    verify_computation_identity(
        canonical.computation_identity,
        compute_definition_identity(&canonical),
    )?;
    encode_message(definition_to_proto(&canonical))
}

pub fn decode_definition(
    bytes: &[u8],
    budget: PersistenceDecodeBudget,
) -> Result<DefinitionDocument, PersistenceCodecError> {
    wire::preflight(bytes, wire::Schema::DefinitionDocument, budget)?;
    let dto =
        proto::DefinitionDocument::decode(bytes).map_err(PersistenceCodecError::ProtobufDecode)?;
    require_version("definition", dto.format_version)?;
    let document = definition_from_proto(dto)?;
    validate_definition(&document)?;
    verify_computation_identity(
        document.computation_identity,
        compute_definition_identity(&document),
    )?;
    let mut canonical = document.clone();
    canonicalize_definition(&mut canonical);
    ensure_canonical(bytes, definition_to_proto(&canonical))?;
    Ok(document)
}

pub fn encode_interpretation(
    document: &InterpretationDocument,
) -> Result<EncodedDocument, PersistenceCodecError> {
    preflight_interpretation_source(document)?;
    let mut canonical = document.clone();
    canonicalize_interpretation(&mut canonical);
    validate_interpretation(&canonical)?;
    encode_message(interpretation_to_proto(&canonical))
}

pub fn decode_interpretation(
    bytes: &[u8],
    budget: PersistenceDecodeBudget,
) -> Result<InterpretationDocument, PersistenceCodecError> {
    wire::preflight(bytes, wire::Schema::InterpretationDocument, budget)?;
    let dto = proto::InterpretationDocument::decode(bytes)
        .map_err(PersistenceCodecError::ProtobufDecode)?;
    require_version("interpretation", dto.format_version)?;
    let document = interpretation_from_proto(dto)?;
    validate_interpretation(&document)?;
    let mut canonical = document.clone();
    canonicalize_interpretation(&mut canonical);
    ensure_canonical(bytes, interpretation_to_proto(&canonical))?;
    Ok(document)
}

pub fn encode_configuration(
    document: &ConfigurationDocument,
) -> Result<EncodedDocument, PersistenceCodecError> {
    preflight_source(0, 5)?;
    validate_configuration(document)?;
    encode_message(configuration_to_proto(document))
}

pub fn decode_configuration(
    bytes: &[u8],
    budget: PersistenceDecodeBudget,
) -> Result<ConfigurationDocument, PersistenceCodecError> {
    wire::preflight(bytes, wire::Schema::ConfigurationDocument, budget)?;
    let dto = proto::ConfigurationDocument::decode(bytes)
        .map_err(PersistenceCodecError::ProtobufDecode)?;
    require_version("configuration", dto.format_version)?;
    let document = configuration_from_proto(dto)?;
    validate_configuration(&document)?;
    ensure_canonical(bytes, configuration_to_proto(&document))?;
    Ok(document)
}

fn encode_message(message: impl Message) -> Result<EncodedDocument, PersistenceCodecError> {
    let encoded_len = message.encoded_len();
    let maximum = PersistenceDecodeBudget::default().max_document_bytes;
    if encoded_len > maximum {
        return Err(PersistenceCodecError::ResourceBudget {
            resource: "encoded document",
            maximum,
            actual: encoded_len,
        });
    }
    let mut bytes = Vec::with_capacity(encoded_len);
    message
        .encode(&mut bytes)
        .expect("encoding a protobuf into a sufficiently sized Vec cannot fail");
    Ok(EncodedDocument {
        revision: DocumentRevision::from_canonical_bytes(&bytes),
        bytes,
    })
}

fn ensure_canonical(source: &[u8], message: impl Message) -> Result<(), PersistenceCodecError> {
    if message.encode_to_vec() != source {
        return Err(PersistenceCodecError::MalformedWire(
            "document does not use canonical field and set ordering".to_string(),
        ));
    }
    Ok(())
}

fn require_version(
    document: &'static str,
    version: Option<u32>,
) -> Result<(), PersistenceCodecError> {
    let version = version.ok_or(PersistenceCodecError::MissingField("format_version"))?;
    if version != MV_PERSISTENCE_FORMAT_VERSION {
        return Err(PersistenceCodecError::UnknownFormatVersion { document, version });
    }
    Ok(())
}

fn required<T>(value: Option<T>, field: &'static str) -> Result<T, PersistenceCodecError> {
    value.ok_or(PersistenceCodecError::MissingField(field))
}

fn enum_value<T>(
    value: Option<i32>,
    field: &'static str,
    convert: impl FnOnce(i32) -> Option<T>,
) -> Result<T, PersistenceCodecError> {
    let value = required(value, field)?;
    convert(value).ok_or(PersistenceCodecError::UnknownEnum { field, value })
}

fn canonicalize_definition(document: &mut DefinitionDocument) {
    for relation in &mut document.relation_occurrences {
        relation
            .fields
            .sort_by(|left, right| left.field_id.cmp(&right.field_id));
    }
    for output in &mut document.outputs {
        output.expression.source_fields.sort();
    }
}

fn canonicalize_interpretation(document: &mut InterpretationDocument) {
    document
        .outputs
        .sort_by(|left, right| left.output_id.cmp(&right.output_id));
    document
        .state_slots
        .sort_by(|left, right| left.slot_id.cmp(&right.slot_id));
    document
        .aggregates
        .sort_by(|left, right| left.aggregate_id.cmp(&right.aggregate_id));
    for aggregate in &mut document.aggregates {
        aggregate.source_fields.sort();
    }
    document
        .target
        .fields
        .sort_by(|left, right| left.logical_identity.cmp(&right.logical_identity));
}

fn compute_definition_identity(document: &DefinitionDocument) -> ComputationIdentity {
    let mut canonical = document.clone();
    canonicalize_definition(&mut canonical);
    let mut dto = definition_to_proto(&canonical);
    dto.format_version = None;
    dto.computation_identity = None;
    dto.created_at_ms = None;
    ComputationIdentity::from_canonical_bytes(&dto.encode_to_vec())
}

fn preflight_definition_source(document: &DefinitionDocument) -> Result<(), PersistenceCodecError> {
    let mut bytes = document.query.effective_sql.len()
        + document.query.resolution.default_catalog.len()
        + document.query.resolution.default_namespace.len();
    let mut items = document.relation_occurrences.len() + document.outputs.len();
    for relation in &document.relation_occurrences {
        bytes = bytes
            .saturating_add(relation.catalog_at_binding.len())
            .saturating_add(relation.namespace_at_binding.len())
            .saturating_add(relation.relation_at_binding.len())
            .saturating_add(relation.qualifier_at_binding.len())
            .saturating_add(relation.object_id.as_bytes().len())
            .saturating_add(relation.schema_version.as_bytes().len());
        items = items.saturating_add(relation.fields.len());
        for field in &relation.fields {
            bytes = bytes
                .saturating_add(field.field_id.as_bytes().len())
                .saturating_add(field.name_at_binding.len())
                .saturating_add(field.type_signature.len());
        }
    }
    for output in &document.outputs {
        bytes = bytes
            .saturating_add(output.output_id.as_bytes().len())
            .saturating_add(output.name.len())
            .saturating_add(output.type_signature.len())
            .saturating_add(
                output
                    .expression
                    .function_identity
                    .as_ref()
                    .map_or(0, String::len),
            );
        items = items.saturating_add(output.expression.source_fields.len());
        for reference in &output.expression.source_fields {
            bytes = bytes.saturating_add(reference.field_id.as_bytes().len());
        }
    }
    preflight_source(bytes, items)
}

fn preflight_interpretation_source(
    document: &InterpretationDocument,
) -> Result<(), PersistenceCodecError> {
    let mut bytes = document.target.object_id.as_bytes().len()
        + document.target.schema_version.as_bytes().len()
        + document.target.partition_spec_version.as_bytes().len();
    let mut items = document.outputs.len()
        + document.state_slots.len()
        + document.aggregates.len()
        + document.branches.len()
        + document.target.fields.len()
        + document.target.partition_fields.len()
        + document.apply_key.components.len();
    for output in &document.outputs {
        bytes = bytes
            .saturating_add(output.output_id.as_bytes().len())
            .saturating_add(output.target_field_id.as_bytes().len())
            .saturating_add(output.type_signature.len());
    }
    for slot in &document.state_slots {
        bytes = bytes
            .saturating_add(slot.slot_id.as_bytes().len())
            .saturating_add(slot.target_field_id.as_bytes().len())
            .saturating_add(slot.type_signature.len());
    }
    for component in &document.apply_key.components {
        bytes = bytes
            .saturating_add(component.logical_id.as_bytes().len())
            .saturating_add(component.target_field_id.as_bytes().len());
    }
    for aggregate in &document.aggregates {
        bytes = bytes
            .saturating_add(aggregate.aggregate_id.as_bytes().len())
            .saturating_add(aggregate.function_identity.len());
        items = items
            .saturating_add(aggregate.source_fields.len())
            .saturating_add(aggregate.state_slot_ids.len());
        for reference in &aggregate.source_fields {
            bytes = bytes.saturating_add(reference.field_id.as_bytes().len());
        }
        for id in &aggregate.state_slot_ids {
            bytes = bytes.saturating_add(id.as_bytes().len());
        }
    }
    for branch in &document.branches {
        bytes = bytes.saturating_add(branch.branch_id.as_bytes().len());
        items = items
            .saturating_add(branch.relation_occurrence_ids.len())
            .saturating_add(branch.output_ids.len());
        for id in &branch.output_ids {
            bytes = bytes.saturating_add(id.as_bytes().len());
        }
    }
    for field in &document.target.fields {
        bytes = bytes
            .saturating_add(field.logical_identity.as_bytes().len())
            .saturating_add(field.target_field_id.as_bytes().len())
            .saturating_add(field.type_signature.len());
    }
    for field in &document.target.partition_fields {
        bytes = bytes
            .saturating_add(field.partition_field_id.as_bytes().len())
            .saturating_add(field.source_target_field_id.as_bytes().len());
    }
    preflight_source(bytes, items)
}

fn preflight_source(variable_bytes: usize, items: usize) -> Result<(), PersistenceCodecError> {
    let budget = PersistenceDecodeBudget::default();
    // Reject grossly oversized variable content before cloning it into the
    // generated DTO. The exact encoded length, including tags and scalar
    // overhead, is checked by `encode_message` before allocating output bytes.
    if variable_bytes > budget.max_document_bytes {
        return Err(PersistenceCodecError::ResourceBudget {
            resource: "source document estimate",
            maximum: budget.max_document_bytes,
            actual: variable_bytes,
        });
    }
    if items > budget.max_items {
        return Err(PersistenceCodecError::ResourceBudget {
            resource: "source document items",
            maximum: budget.max_items,
            actual: items,
        });
    }
    Ok(())
}

fn definition_to_proto(document: &DefinitionDocument) -> proto::DefinitionDocument {
    proto::DefinitionDocument {
        created_at_ms: Some(document.created_at_ms),
        format_version: Some(MV_PERSISTENCE_FORMAT_VERSION),
        query: Some(proto::QuerySource {
            effective_sql: Some(document.query.effective_sql.clone()),
            dialect: Some(match document.query.dialect {
                QueryDialect::StarRocks => 1,
            }),
            resolution: Some(proto::ResolutionContext {
                default_catalog: Some(document.query.resolution.default_catalog.clone()),
                default_namespace: Some(document.query.resolution.default_namespace.clone()),
            }),
        }),
        relation_occurrences: document
            .relation_occurrences
            .iter()
            .map(|relation| proto::RelationOccurrence {
                occurrence_id: Some(relation.occurrence_id),
                catalog_at_binding: Some(relation.catalog_at_binding.clone()),
                namespace_at_binding: Some(relation.namespace_at_binding.clone()),
                relation_at_binding: Some(relation.relation_at_binding.clone()),
                qualifier_at_binding: Some(relation.qualifier_at_binding.clone()),
                object_id: Some(relation.object_id.as_bytes().to_vec()),
                schema_version: Some(relation.schema_version.as_bytes().to_vec()),
                fields: relation
                    .fields
                    .iter()
                    .map(|field| proto::SourceFieldBinding {
                        field_id: Some(field.field_id.as_bytes().to_vec()),
                        name_at_binding: Some(field.name_at_binding.clone()),
                        type_signature: Some(field.type_signature.clone()),
                        nullable: Some(field.nullable),
                    })
                    .collect(),
            })
            .collect(),
        outputs: document
            .outputs
            .iter()
            .map(|output| proto::OutputDefinition {
                output_id: Some(output.output_id.as_bytes().to_vec()),
                name: Some(output.name.clone()),
                type_signature: Some(output.type_signature.clone()),
                nullable: Some(output.nullable),
                expression: Some(proto::ExpressionShape {
                    kind: Some(match output.expression.kind {
                        ExpressionKind::Field => 1,
                        ExpressionKind::Literal => 2,
                        ExpressionKind::Cast => 3,
                        ExpressionKind::Function => 4,
                        ExpressionKind::Mixed => 5,
                    }),
                    function_identity: output.expression.function_identity.clone(),
                    source_fields: output
                        .expression
                        .source_fields
                        .iter()
                        .map(|reference| proto::SourceFieldReference {
                            occurrence_id: Some(reference.occurrence_id),
                            field_id: Some(reference.field_id.as_bytes().to_vec()),
                        })
                        .collect(),
                }),
            })
            .collect(),
        computation_identity: Some(document.computation_identity.as_bytes().to_vec()),
    }
}

fn definition_from_proto(
    dto: proto::DefinitionDocument,
) -> Result<DefinitionDocument, PersistenceCodecError> {
    let query = required(dto.query, "definition.query")?;
    let resolution = required(query.resolution, "definition.query.resolution")?;
    Ok(DefinitionDocument {
        created_at_ms: required(dto.created_at_ms, "definition.created_at_ms")?,
        query: QuerySource {
            effective_sql: required(query.effective_sql, "definition.query.effective_sql")?,
            dialect: enum_value(query.dialect, "definition.query.dialect", |value| {
                (value == 1).then_some(QueryDialect::StarRocks)
            })?,
            resolution: ResolutionContext {
                default_catalog: required(
                    resolution.default_catalog,
                    "definition.query.resolution.default_catalog",
                )?,
                default_namespace: required(
                    resolution.default_namespace,
                    "definition.query.resolution.default_namespace",
                )?,
            },
        },
        relation_occurrences: dto
            .relation_occurrences
            .into_iter()
            .map(relation_from_proto)
            .collect::<Result<_, _>>()?,
        outputs: dto
            .outputs
            .into_iter()
            .map(output_from_proto)
            .collect::<Result<_, _>>()?,
        computation_identity: ComputationIdentity::try_from_bytes(&required(
            dto.computation_identity,
            "definition.computation_identity",
        )?)?,
    })
}

fn relation_from_proto(
    value: proto::RelationOccurrence,
) -> Result<RelationOccurrence, PersistenceCodecError> {
    Ok(RelationOccurrence {
        occurrence_id: required(value.occurrence_id, "definition.relation.occurrence_id")?,
        catalog_at_binding: required(
            value.catalog_at_binding,
            "definition.relation.catalog_at_binding",
        )?,
        namespace_at_binding: required(
            value.namespace_at_binding,
            "definition.relation.namespace_at_binding",
        )?,
        relation_at_binding: required(
            value.relation_at_binding,
            "definition.relation.relation_at_binding",
        )?,
        qualifier_at_binding: required(
            value.qualifier_at_binding,
            "definition.relation.qualifier_at_binding",
        )?,
        object_id: ObjectIdentity::try_new(required(
            value.object_id,
            "definition.relation.object_id",
        )?)?,
        schema_version: SchemaVersion::try_new(required(
            value.schema_version,
            "definition.relation.schema_version",
        )?)?,
        fields: value
            .fields
            .into_iter()
            .map(|field| {
                Ok(SourceFieldBinding {
                    field_id: FieldIdentity::try_new(required(
                        field.field_id,
                        "definition.relation.field.field_id",
                    )?)?,
                    name_at_binding: required(
                        field.name_at_binding,
                        "definition.relation.field.name_at_binding",
                    )?,
                    type_signature: required(
                        field.type_signature,
                        "definition.relation.field.type_signature",
                    )?,
                    nullable: required(field.nullable, "definition.relation.field.nullable")?,
                })
            })
            .collect::<Result<_, PersistenceCodecError>>()?,
    })
}

fn output_from_proto(
    value: proto::OutputDefinition,
) -> Result<OutputDefinition, PersistenceCodecError> {
    let expression = required(value.expression, "definition.output.expression")?;
    Ok(OutputDefinition {
        output_id: OutputIdentity::try_new(required(
            value.output_id,
            "definition.output.output_id",
        )?)?,
        name: required(value.name, "definition.output.name")?,
        type_signature: required(value.type_signature, "definition.output.type_signature")?,
        nullable: required(value.nullable, "definition.output.nullable")?,
        expression: ExpressionShape {
            kind: enum_value(
                expression.kind,
                "definition.output.expression.kind",
                |value| {
                    Some(match value {
                        1 => ExpressionKind::Field,
                        2 => ExpressionKind::Literal,
                        3 => ExpressionKind::Cast,
                        4 => ExpressionKind::Function,
                        5 => ExpressionKind::Mixed,
                        _ => return None,
                    })
                },
            )?,
            function_identity: expression.function_identity,
            source_fields: expression
                .source_fields
                .into_iter()
                .map(|reference| {
                    Ok(SourceFieldReference {
                        occurrence_id: required(
                            reference.occurrence_id,
                            "definition.output.expression.source_field.occurrence_id",
                        )?,
                        field_id: FieldIdentity::try_new(required(
                            reference.field_id,
                            "definition.output.expression.source_field.field_id",
                        )?)?,
                    })
                })
                .collect::<Result<_, PersistenceCodecError>>()?,
        },
    })
}

fn interpretation_to_proto(document: &InterpretationDocument) -> proto::InterpretationDocument {
    proto::InterpretationDocument {
        format_version: Some(MV_PERSISTENCE_FORMAT_VERSION),
        definition_revision: Some(document.definition_revision.as_bytes().to_vec()),
        computation_identity: Some(document.computation_identity.as_bytes().to_vec()),
        outputs: document
            .outputs
            .iter()
            .map(|value| proto::OutputBinding {
                output_id: Some(value.output_id.as_bytes().to_vec()),
                target_field_id: Some(value.target_field_id.as_bytes().to_vec()),
                type_signature: Some(value.type_signature.clone()),
                nullable: Some(value.nullable),
            })
            .collect(),
        state_slots: document
            .state_slots
            .iter()
            .map(|value| proto::StateSlot {
                slot_id: Some(value.slot_id.as_bytes().to_vec()),
                target_field_id: Some(value.target_field_id.as_bytes().to_vec()),
                type_signature: Some(value.type_signature.clone()),
                nullable: Some(value.nullable),
                role: Some(match value.role {
                    StateRole::Single => 1,
                    StateRole::AvgSum => 2,
                    StateRole::AvgCount => 3,
                    StateRole::RetractionCount => 4,
                }),
                encoding: Some(match value.encoding {
                    StateEncoding::NativeColumnV1 => 1,
                }),
            })
            .collect(),
        apply_key: Some(proto::ApplyKey {
            kind: Some(match document.apply_key.kind {
                ApplyKeyKind::BaseRowId => 1,
                ApplyKeyKind::JoinRowKey => 2,
                ApplyKeyKind::GroupRowId => 3,
            }),
            components: document
                .apply_key
                .components
                .iter()
                .map(|component| proto::ApplyKeyComponent {
                    logical_id: Some(component.logical_id.as_bytes().to_vec()),
                    target_field_id: Some(component.target_field_id.as_bytes().to_vec()),
                })
                .collect(),
        }),
        aggregates: document
            .aggregates
            .iter()
            .map(|value| proto::AggregateInterpretation {
                aggregate_id: Some(value.aggregate_id.as_bytes().to_vec()),
                function_identity: Some(value.function_identity.clone()),
                source_fields: value
                    .source_fields
                    .iter()
                    .map(|reference| proto::SourceFieldReference {
                        occurrence_id: Some(reference.occurrence_id),
                        field_id: Some(reference.field_id.as_bytes().to_vec()),
                    })
                    .collect(),
                state_slot_ids: value
                    .state_slot_ids
                    .iter()
                    .map(|id| id.as_bytes().to_vec())
                    .collect(),
                branch_id: value.branch_id.as_ref().map(|id| id.as_bytes().to_vec()),
            })
            .collect(),
        branches: document
            .branches
            .iter()
            .map(|value| proto::BranchInterpretation {
                branch_id: Some(value.branch_id.as_bytes().to_vec()),
                relation_occurrence_ids: value.relation_occurrence_ids.clone(),
                output_ids: value
                    .output_ids
                    .iter()
                    .map(|id| id.as_bytes().to_vec())
                    .collect(),
            })
            .collect(),
        target: Some(proto::TargetBinding {
            object_id: Some(document.target.object_id.as_bytes().to_vec()),
            schema_version: Some(document.target.schema_version.as_bytes().to_vec()),
            partition_spec_version: Some(
                document.target.partition_spec_version.as_bytes().to_vec(),
            ),
            fields: document
                .target
                .fields
                .iter()
                .map(|value| proto::PhysicalFieldBinding {
                    kind: Some(match &value.logical_identity {
                        PhysicalFieldLogicalIdentity::Output(_) => 1,
                        PhysicalFieldLogicalIdentity::State(_) => 2,
                        PhysicalFieldLogicalIdentity::ApplyKey(_) => 3,
                        PhysicalFieldLogicalIdentity::Branch(_) => 4,
                    }),
                    logical_id: Some(value.logical_identity.as_bytes().to_vec()),
                    target_field_id: Some(value.target_field_id.as_bytes().to_vec()),
                    type_signature: Some(value.type_signature.clone()),
                    nullable: Some(value.nullable),
                })
                .collect(),
            partition_fields: document
                .target
                .partition_fields
                .iter()
                .map(|field| {
                    let (transform, transform_argument) = match field.transform {
                        TargetPartitionTransform::Identity => (1, None),
                        TargetPartitionTransform::Year => (2, None),
                        TargetPartitionTransform::Month => (3, None),
                        TargetPartitionTransform::Day => (4, None),
                        TargetPartitionTransform::Hour => (5, None),
                        TargetPartitionTransform::Bucket { num_buckets } => (6, Some(num_buckets)),
                        TargetPartitionTransform::Truncate { width } => (7, Some(width)),
                        TargetPartitionTransform::Void => (8, None),
                    };
                    proto::TargetPartitionFieldBinding {
                        partition_field_id: Some(field.partition_field_id.as_bytes().to_vec()),
                        source_target_field_id: Some(
                            field.source_target_field_id.as_bytes().to_vec(),
                        ),
                        transform: Some(transform),
                        transform_argument,
                    }
                })
                .collect(),
        }),
    }
}

fn interpretation_from_proto(
    dto: proto::InterpretationDocument,
) -> Result<InterpretationDocument, PersistenceCodecError> {
    let apply_key = required(dto.apply_key, "interpretation.apply_key")?;
    let target = required(dto.target, "interpretation.target")?;
    Ok(InterpretationDocument {
        definition_revision: DocumentRevision::try_from_bytes(&required(
            dto.definition_revision,
            "interpretation.definition_revision",
        )?)?,
        computation_identity: ComputationIdentity::try_from_bytes(&required(
            dto.computation_identity,
            "interpretation.computation_identity",
        )?)?,
        outputs: dto
            .outputs
            .into_iter()
            .map(|value| {
                Ok(OutputBinding {
                    output_id: OutputIdentity::try_new(required(
                        value.output_id,
                        "interpretation.output.output_id",
                    )?)?,
                    target_field_id: FieldIdentity::try_new(required(
                        value.target_field_id,
                        "interpretation.output.target_field_id",
                    )?)?,
                    type_signature: required(
                        value.type_signature,
                        "interpretation.output.type_signature",
                    )?,
                    nullable: required(value.nullable, "interpretation.output.nullable")?,
                })
            })
            .collect::<Result<_, PersistenceCodecError>>()?,
        state_slots: dto
            .state_slots
            .into_iter()
            .map(|value| {
                Ok(StateSlot {
                    slot_id: StateSlotIdentity::try_new(required(
                        value.slot_id,
                        "interpretation.state_slot.slot_id",
                    )?)?,
                    target_field_id: FieldIdentity::try_new(required(
                        value.target_field_id,
                        "interpretation.state_slot.target_field_id",
                    )?)?,
                    type_signature: required(
                        value.type_signature,
                        "interpretation.state_slot.type_signature",
                    )?,
                    nullable: required(value.nullable, "interpretation.state_slot.nullable")?,
                    role: enum_value(value.role, "interpretation.state_slot.role", |value| {
                        Some(match value {
                            1 => StateRole::Single,
                            2 => StateRole::AvgSum,
                            3 => StateRole::AvgCount,
                            4 => StateRole::RetractionCount,
                            _ => return None,
                        })
                    })?,
                    encoding: enum_value(
                        value.encoding,
                        "interpretation.state_slot.encoding",
                        |value| (value == 1).then_some(StateEncoding::NativeColumnV1),
                    )?,
                })
            })
            .collect::<Result<_, PersistenceCodecError>>()?,
        apply_key: ApplyKey {
            kind: enum_value(apply_key.kind, "interpretation.apply_key.kind", |value| {
                Some(match value {
                    1 => ApplyKeyKind::BaseRowId,
                    2 => ApplyKeyKind::JoinRowKey,
                    3 => ApplyKeyKind::GroupRowId,
                    _ => return None,
                })
            })?,
            components: apply_key
                .components
                .into_iter()
                .map(|component| {
                    Ok(ApplyKeyComponent {
                        logical_id: ApplyKeyIdentity::try_new(required(
                            component.logical_id,
                            "interpretation.apply_key.component.logical_id",
                        )?)?,
                        target_field_id: FieldIdentity::try_new(required(
                            component.target_field_id,
                            "interpretation.apply_key.component.target_field_id",
                        )?)?,
                    })
                })
                .collect::<Result<_, PersistenceCodecError>>()?,
        },
        aggregates: dto
            .aggregates
            .into_iter()
            .map(|value| {
                Ok(AggregateInterpretation {
                    aggregate_id: AggregateIdentity::try_new(required(
                        value.aggregate_id,
                        "interpretation.aggregate.aggregate_id",
                    )?)?,
                    function_identity: required(
                        value.function_identity,
                        "interpretation.aggregate.function_identity",
                    )?,
                    source_fields: value
                        .source_fields
                        .into_iter()
                        .map(|reference| {
                            Ok(SourceFieldReference {
                                occurrence_id: required(
                                    reference.occurrence_id,
                                    "interpretation.aggregate.source_field.occurrence_id",
                                )?,
                                field_id: FieldIdentity::try_new(required(
                                    reference.field_id,
                                    "interpretation.aggregate.source_field.field_id",
                                )?)?,
                            })
                        })
                        .collect::<Result<_, PersistenceCodecError>>()?,
                    state_slot_ids: value
                        .state_slot_ids
                        .into_iter()
                        .map(StateSlotIdentity::try_new)
                        .collect::<Result<_, _>>()?,
                    branch_id: value.branch_id.map(BranchIdentity::try_new).transpose()?,
                })
            })
            .collect::<Result<_, PersistenceCodecError>>()?,
        branches: dto
            .branches
            .into_iter()
            .map(|value| {
                Ok(BranchInterpretation {
                    branch_id: BranchIdentity::try_new(required(
                        value.branch_id,
                        "interpretation.branch.branch_id",
                    )?)?,
                    relation_occurrence_ids: value.relation_occurrence_ids,
                    output_ids: value
                        .output_ids
                        .into_iter()
                        .map(OutputIdentity::try_new)
                        .collect::<Result<_, _>>()?,
                })
            })
            .collect::<Result<_, PersistenceCodecError>>()?,
        target: TargetBinding {
            object_id: ObjectIdentity::try_new(required(
                target.object_id,
                "interpretation.target.object_id",
            )?)?,
            schema_version: SchemaVersion::try_new(required(
                target.schema_version,
                "interpretation.target.schema_version",
            )?)?,
            partition_spec_version: PartitionSpecVersion::try_new(required(
                target.partition_spec_version,
                "interpretation.target.partition_spec_version",
            )?)?,
            fields: target
                .fields
                .into_iter()
                .map(|value| {
                    let kind =
                        enum_value(value.kind, "interpretation.target.field.kind", |value| {
                            match value {
                                1..=4 => Some(value),
                                _ => None,
                            }
                        })?;
                    let logical_id =
                        required(value.logical_id, "interpretation.target.field.logical_id")?;
                    Ok(PhysicalFieldBinding {
                        logical_identity: match kind {
                            1 => PhysicalFieldLogicalIdentity::Output(OutputIdentity::try_new(
                                logical_id,
                            )?),
                            2 => PhysicalFieldLogicalIdentity::State(StateSlotIdentity::try_new(
                                logical_id,
                            )?),
                            3 => PhysicalFieldLogicalIdentity::ApplyKey(ApplyKeyIdentity::try_new(
                                logical_id,
                            )?),
                            4 => PhysicalFieldLogicalIdentity::Branch(BranchIdentity::try_new(
                                logical_id,
                            )?),
                            _ => unreachable!("physical field kind was validated"),
                        },
                        target_field_id: FieldIdentity::try_new(required(
                            value.target_field_id,
                            "interpretation.target.field.target_field_id",
                        )?)?,
                        type_signature: required(
                            value.type_signature,
                            "interpretation.target.field.type_signature",
                        )?,
                        nullable: required(value.nullable, "interpretation.target.field.nullable")?,
                    })
                })
                .collect::<Result<_, PersistenceCodecError>>()?,
            partition_fields: target
                .partition_fields
                .into_iter()
                .map(|value| {
                    let transform = enum_value(
                        value.transform,
                        "interpretation.target.partition_field.transform",
                        |value| match value {
                            1..=8 => Some(value),
                            _ => None,
                        },
                    )?;
                    let transform = match (transform, value.transform_argument) {
                        (1, None) => TargetPartitionTransform::Identity,
                        (2, None) => TargetPartitionTransform::Year,
                        (3, None) => TargetPartitionTransform::Month,
                        (4, None) => TargetPartitionTransform::Day,
                        (5, None) => TargetPartitionTransform::Hour,
                        (6, Some(num_buckets)) => TargetPartitionTransform::Bucket { num_buckets },
                        (7, Some(width)) => TargetPartitionTransform::Truncate { width },
                        (8, None) => TargetPartitionTransform::Void,
                        _ => {
                            return Err(PersistenceCodecError::MalformedWire(
                                "MV target partition transform argument is invalid".into(),
                            ));
                        }
                    };
                    Ok(TargetPartitionFieldBinding {
                        partition_field_id: FieldIdentity::try_new(required(
                            value.partition_field_id,
                            "interpretation.target.partition_field.partition_field_id",
                        )?)?,
                        source_target_field_id: FieldIdentity::try_new(required(
                            value.source_target_field_id,
                            "interpretation.target.partition_field.source_target_field_id",
                        )?)?,
                        transform,
                    })
                })
                .collect::<Result<_, PersistenceCodecError>>()?,
        },
    })
}

fn configuration_to_proto(document: &ConfigurationDocument) -> proto::ConfigurationDocument {
    proto::ConfigurationDocument {
        format_version: Some(MV_PERSISTENCE_FORMAT_VERSION),
        refresh_policy: Some(match document.refresh_policy {
            RefreshPolicy::Manual => 1,
            RefreshPolicy::AsyncOnChange => 2,
            RefreshPolicy::AsyncInterval => 3,
        }),
        paused: Some(document.paused),
        refresh_interval_ms: document.refresh_interval_ms,
        max_staleness_ms: document.max_staleness_ms,
    }
}

fn configuration_from_proto(
    dto: proto::ConfigurationDocument,
) -> Result<ConfigurationDocument, PersistenceCodecError> {
    Ok(ConfigurationDocument {
        refresh_policy: enum_value(
            dto.refresh_policy,
            "configuration.refresh_policy",
            |value| {
                Some(match value {
                    1 => RefreshPolicy::Manual,
                    2 => RefreshPolicy::AsyncOnChange,
                    3 => RefreshPolicy::AsyncInterval,
                    _ => return None,
                })
            },
        )?,
        paused: required(dto.paused, "configuration.paused")?,
        refresh_interval_ms: dto.refresh_interval_ms,
        max_staleness_ms: dto.max_staleness_ms,
    })
}
