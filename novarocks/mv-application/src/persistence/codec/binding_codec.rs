// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use std::collections::BTreeSet;

use bytes::Bytes;
use novarocks_spi::connector::{IcebergSourceTypeNode, ProviderSourceType};
use prost::Message;

use super::binding_model::*;
use super::{
    EncodedDocument, PersistenceCodecError, QueryDialect, SourceFieldReference, encode_message,
    enum_value, proto, required, wire,
};
use crate::persistence::{
    exact_revision::decode_fact,
    identity::{DocumentRevision, FieldIdentity, ObjectIdentity},
};

pub const BINDING_FORMAT_VERSION: u32 = 2;

pub fn encode_binding(value: &BindingDocument) -> Result<EncodedDocument, PersistenceCodecError> {
    validate_binding(value)?;
    let digests = value.digests();
    encode_message(proto::BindingDocument {
        format_version: Some(BINDING_FORMAT_VERSION),
        occurrences: value.occurrences.iter().map(encode_occurrence).collect(),
        outputs: value.outputs.iter().map(encode_output).collect(),
        dependencies: value.dependencies.iter().map(encode_dependency).collect(),
        digests: Some(proto::BindingDigests {
            resolution: Some(digests.resolution.as_bytes().to_vec()),
            dependencies: Some(digests.dependencies.as_bytes().to_vec()),
            outputs: Some(digests.outputs.as_bytes().to_vec()),
        }),
    })
}

pub fn decode_binding(bytes: &[u8]) -> Result<BindingDocument, PersistenceCodecError> {
    wire::preflight(bytes, wire::Schema::BindingDocument)?;
    let dto =
        proto::BindingDocument::decode(bytes).map_err(PersistenceCodecError::ProtobufDecode)?;
    let version = required(dto.format_version, "binding.format_version")?;
    if version != BINDING_FORMAT_VERSION {
        return Err(PersistenceCodecError::UnknownFormatVersion {
            document: "binding",
            version,
        });
    }
    let field = required(dto.digests, "binding.digests")?;
    let digests = BindingDigests {
        resolution: revision(field.resolution, "binding.digests.resolution")?,
        dependencies: revision(field.dependencies, "binding.digests.dependencies")?,
        outputs: revision(field.outputs, "binding.digests.outputs")?,
    };
    let value = BindingDocument {
        occurrences: dto
            .occurrences
            .into_iter()
            .map(decode_occurrence)
            .collect::<Result<_, _>>()?,
        outputs: dto
            .outputs
            .into_iter()
            .map(decode_output)
            .collect::<Result<_, _>>()?,
        dependencies: dto
            .dependencies
            .into_iter()
            .map(decode_dependency)
            .collect::<Result<_, _>>()?,
    };
    if digests != value.digests() || encode_binding(&value)?.as_bytes() != bytes {
        return Err(malformed(
            "binding facts have invalid digests or non-canonical framing",
        ));
    }
    Ok(value)
}

fn validate_binding(value: &BindingDocument) -> Result<(), PersistenceCodecError> {
    if value.occurrences.is_empty()
        || value.outputs.is_empty()
        || value.occurrences.len() > u16::MAX as usize
    {
        return Err(malformed(
            "binding lacks occurrences or outputs, or exceeds the publication occurrence representation",
        ));
    }

    let mut occurrence_ids = BTreeSet::new();
    for occurrence in &value.occurrences {
        if !occurrence_ids.insert(occurrence.occurrence_id) {
            return Err(malformed("duplicate binding occurrence ID"));
        }
        occurrence.data_version.validate()?;
        let object = decode_fact(occurrence.object_id.as_bytes())
            .map_err(|cause| malformed(cause.to_string()))?;
        if object.provider != occurrence.data_version.provider {
            return Err(malformed(
                "binding object and data-version providers differ",
            ));
        }
        if [
            &occurrence.facts.occurrence,
            &occurrence.facts.object,
            &occurrence.facts.field_paths,
            &occurrence.facts.field_types,
            &occurrence.facts.read_declarations,
        ]
        .into_iter()
        .any(|fact| fact.is_empty())
        {
            return Err(malformed("binding occurrence lacks a complete fact"));
        }
        for path in &occurrence.paths {
            validate_path(path)?;
        }
        if !occurrence
            .paths
            .windows(2)
            .all(|pair| path_ids(&pair[0]).cmp(path_ids(&pair[1])).is_lt())
        {
            return Err(malformed(
                "binding paths must have unique field identities in canonical order",
            ));
        }
    }

    for (position, output) in value.outputs.iter().enumerate() {
        if output.ordinal as usize != position
            || output.logical_type_identity.is_empty()
            || output.name.trim().is_empty()
        {
            return Err(malformed("binding output is malformed or out of order"));
        }
        if !output
            .source_fields
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        {
            return Err(malformed(
                "output field references must be unique and canonical",
            ));
        }
        for field in &output.source_fields {
            if !value.occurrences.iter().any(|occurrence| {
                occurrence.occurrence_id == field.occurrence_id
                    && occurrence.contains_field(&field.field_id)
            }) {
                return Err(malformed(
                    "output field reference is absent from the binding",
                ));
            }
        }
    }
    if value
        .dependencies
        .iter()
        .any(|item| item.canonical_bytes().is_empty())
        || !value
            .dependencies
            .windows(2)
            .all(|pair| pair[0].canonical_bytes() < pair[1].canonical_bytes())
    {
        return Err(malformed(
            "binding dependencies must have unique canonical facts",
        ));
    }
    Ok(())
}

fn path_ids(path: &BoundSourcePath) -> impl Iterator<Item = &FieldIdentity> {
    path.nodes.iter().map(|node| &node.field_id)
}

fn validate_path(path: &BoundSourcePath) -> Result<(), PersistenceCodecError> {
    let Some((leaf, ancestors)) = path.nodes.split_last() else {
        return Err(malformed("binding path must have at least one node"));
    };
    if ancestors
        .iter()
        .any(|node| node.kind != SourceNodeKind::Struct)
    {
        return Err(malformed("binding path may only descend through structs"));
    }
    let ProviderSourceType::Iceberg(ty) = &path.leaf_type;
    let leaf_kind = match ty.nodes().first() {
        Some(IcebergSourceTypeNode::Primitive(_)) => SourceNodeKind::Scalar,
        Some(IcebergSourceTypeNode::Struct { .. }) => SourceNodeKind::Struct,
        Some(IcebergSourceTypeNode::List { .. }) => SourceNodeKind::List,
        Some(IcebergSourceTypeNode::Map { .. }) => SourceNodeKind::Map,
        Some(IcebergSourceTypeNode::Field { .. }) | None => {
            return Err(malformed("binding has invalid leaf type"));
        }
    };
    if leaf_kind != leaf.kind || path.whole_subtree != (leaf_kind != SourceNodeKind::Scalar) {
        return Err(malformed(
            "binding leaf contract differs from its node kind",
        ));
    }
    Ok(())
}

fn encode_occurrence(value: &BindingOccurrence) -> proto::BindingOccurrence {
    proto::BindingOccurrence {
        occurrence_id: Some(value.occurrence_id),
        catalog_at_binding: Some(value.catalog_at_binding.clone()),
        namespace_at_binding: Some(value.namespace_at_binding.clone()),
        relation_at_binding: Some(value.relation_at_binding.clone()),
        qualifier_at_binding: Some(value.qualifier_at_binding.clone()),
        object_id: Some(value.object_id.as_bytes().to_vec()),
        selection: Some(match value.selection {
            ReadSelection::Current => 1,
            ReadSelection::Snapshot => 2,
            ReadSelection::Branch => 3,
            ReadSelection::Tag => 4,
        }),
        data_version: Some(value.data_version.to_proto()),
        object_fact: Some(value.facts.object.to_vec()),
        path_fact: Some(value.facts.field_paths.to_vec()),
        type_fact: Some(value.facts.field_types.to_vec()),
        declarations_fact: Some(value.facts.read_declarations.to_vec()),
        paths: value.paths.iter().map(encode_path).collect(),
        occurrence_fact: Some(value.facts.occurrence.to_vec()),
    }
}

fn decode_occurrence(
    value: proto::BindingOccurrence,
) -> Result<BindingOccurrence, PersistenceCodecError> {
    use super::keyed::DataVersionFormat;
    Ok(BindingOccurrence {
        occurrence_id: required(value.occurrence_id, "binding.occurrence_id")?,
        catalog_at_binding: required(value.catalog_at_binding, "binding.catalog_at_binding")?,
        namespace_at_binding: required(value.namespace_at_binding, "binding.namespace_at_binding")?,
        relation_at_binding: required(value.relation_at_binding, "binding.relation_at_binding")?,
        qualifier_at_binding: required(value.qualifier_at_binding, "binding.qualifier_at_binding")?,
        object_id: ObjectIdentity::try_new(required(value.object_id, "binding.object_id")?)?,
        selection: enum_value(value.selection, "binding.selection", |kind| {
            Some(match kind {
                1 => ReadSelection::Current,
                2 => ReadSelection::Snapshot,
                3 => ReadSelection::Branch,
                4 => ReadSelection::Tag,
                _ => return None,
            })
        })?,
        data_version: DataVersionFormat::from_proto(required(
            value.data_version,
            "binding.data_version",
        )?)?,
        facts: SourceBindingFacts {
            occurrence: take_bytes(value.occurrence_fact, "binding.occurrence_fact")?,
            object: take_bytes(value.object_fact, "binding.object_fact")?,
            field_paths: take_bytes(value.path_fact, "binding.path_fact")?,
            field_types: take_bytes(value.type_fact, "binding.type_fact")?,
            read_declarations: take_bytes(value.declarations_fact, "binding.declarations_fact")?,
        },
        paths: value
            .paths
            .into_iter()
            .map(decode_path)
            .collect::<Result<_, _>>()?,
    })
}

fn encode_path(value: &BoundSourcePath) -> proto::BoundSourcePath {
    proto::BoundSourcePath {
        nodes: value
            .nodes
            .iter()
            .map(|node| proto::BoundSourceNode {
                field_id: Some(node.field_id.as_bytes().to_vec()),
                required: Some(node.required),
                kind: Some(match node.kind {
                    SourceNodeKind::Scalar => 1,
                    SourceNodeKind::Struct => 2,
                    SourceNodeKind::List => 3,
                    SourceNodeKind::Map => 4,
                }),
            })
            .collect(),
        leaf_type: Some(value.leaf_type.canonical_bytes()),
        whole_subtree: Some(value.whole_subtree),
    }
}

fn decode_path(value: proto::BoundSourcePath) -> Result<BoundSourcePath, PersistenceCodecError> {
    Ok(BoundSourcePath {
        nodes: value
            .nodes
            .into_iter()
            .map(|node| {
                Ok(BoundSourceNode {
                    field_id: FieldIdentity::try_new(required(
                        node.field_id,
                        "binding.path.field_id",
                    )?)?,
                    required: required(node.required, "binding.path.required")?,
                    kind: enum_value(node.kind, "binding.path.kind", |kind| {
                        Some(match kind {
                            1 => SourceNodeKind::Scalar,
                            2 => SourceNodeKind::Struct,
                            3 => SourceNodeKind::List,
                            4 => SourceNodeKind::Map,
                            _ => return None,
                        })
                    })?,
                })
            })
            .collect::<Result<_, PersistenceCodecError>>()?,
        leaf_type: ProviderSourceType::from_canonical_bytes(&required(
            value.leaf_type,
            "binding.path.leaf_type",
        )?)
        .map_err(|cause| malformed(cause.to_string()))?,
        whole_subtree: required(value.whole_subtree, "binding.path.whole_subtree")?,
    })
}

fn encode_output(value: &BoundOutput) -> proto::BoundOutput {
    proto::BoundOutput {
        ordinal: Some(value.ordinal),
        name: Some(value.name.clone()),
        logical_type_identity: Some(value.logical_type_identity.to_vec()),
        source_fields: value
            .source_fields
            .iter()
            .map(|source| proto::SourceFieldReference {
                occurrence_id: Some(source.occurrence_id),
                field_id: Some(source.field_id.as_bytes().to_vec()),
            })
            .collect(),
    }
}

fn decode_output(value: proto::BoundOutput) -> Result<BoundOutput, PersistenceCodecError> {
    Ok(BoundOutput {
        ordinal: required(value.ordinal, "binding.output.ordinal")?,
        name: required(value.name, "binding.output.name")?,
        logical_type_identity: take_bytes(
            value.logical_type_identity,
            "binding.output.logical_type_identity",
        )?,
        source_fields: value
            .source_fields
            .into_iter()
            .map(|source| {
                Ok(SourceFieldReference {
                    occurrence_id: required(
                        source.occurrence_id,
                        "binding.output.source_field.occurrence_id",
                    )?,
                    field_id: FieldIdentity::try_new(required(
                        source.field_id,
                        "binding.output.source_field.field_id",
                    )?)?,
                })
            })
            .collect::<Result<_, PersistenceCodecError>>()?,
    })
}

fn encode_dependency(value: &BindingDependency) -> proto::BindingDependency {
    use proto::binding_dependency::Kind;
    let kind = match value {
        BindingDependency::View {
            identity,
            depth,
            exact_declaration,
            iceberg_version,
            dialect: _,
            ..
        } => Kind::View(proto::ViewDependency {
            identity: Some(identity.to_vec()),
            depth: Some(*depth),
            exact_declaration: Some(exact_declaration.to_vec()),
            iceberg_uuid: iceberg_version.map(|(uuid, _)| uuid.to_vec()),
            iceberg_version_id: iceberg_version.map(|(_, version)| version),
            dialect: Some(proto::QueryDialect::Trino as i32),
        }),
        BindingDependency::NestedMv {
            object_id,
            definition_revision,
            dialect: _,
            ..
        } => Kind::NestedMv(proto::NestedMvDependency {
            object_id: Some(object_id.as_bytes().to_vec()),
            definition_revision: Some(definition_revision.as_bytes().to_vec()),
            dialect: Some(proto::QueryDialect::Trino as i32),
        }),
    };
    proto::BindingDependency {
        kind: Some(kind),
        canonical: Some(value.canonical_bytes().to_vec()),
    }
}

fn decode_dependency(
    value: proto::BindingDependency,
) -> Result<BindingDependency, PersistenceCodecError> {
    use proto::binding_dependency::Kind;
    let dialect = |value| {
        enum_value(value, "binding.dependency.dialect", |kind| {
            (kind == proto::QueryDialect::Trino as i32).then_some(QueryDialect::Trino)
        })
    };
    let canonical = take_bytes(value.canonical, "binding.dependency.canonical")?;
    Ok(match required(value.kind, "binding.dependency.kind")? {
        Kind::View(view) => {
            let iceberg_version = match (view.iceberg_uuid, view.iceberg_version_id) {
                (None, None) => None,
                (Some(uuid), Some(id)) => Some((
                    uuid.try_into()
                        .map_err(|_| malformed("view identity must be a 16-byte UUID"))?,
                    id,
                )),
                _ => return Err(malformed("incomplete Iceberg view revision")),
            };
            BindingDependency::View {
                identity: take_bytes(view.identity, "binding.dependency.view.identity")?,
                depth: required(view.depth, "binding.dependency.view.depth")?,
                exact_declaration: take_bytes(
                    view.exact_declaration,
                    "binding.dependency.view.exact_declaration",
                )?,
                iceberg_version,
                dialect: dialect(view.dialect)?,
                canonical,
            }
        }
        Kind::NestedMv(mv) => BindingDependency::NestedMv {
            object_id: ObjectIdentity::try_new(required(
                mv.object_id,
                "binding.dependency.mv.object",
            )?)?,
            definition_revision: revision(
                mv.definition_revision,
                "binding.dependency.mv.definition_revision",
            )?,
            dialect: dialect(mv.dialect)?,
            canonical,
        },
    })
}

fn revision(
    value: Option<Vec<u8>>,
    field: &'static str,
) -> Result<DocumentRevision, PersistenceCodecError> {
    Ok(DocumentRevision::try_from_bytes(&required(value, field)?)?)
}

fn take_bytes(value: Option<Vec<u8>>, field: &'static str) -> Result<Bytes, PersistenceCodecError> {
    Ok(Bytes::from(required(value, field)?))
}

fn malformed(detail: impl Into<String>) -> PersistenceCodecError {
    PersistenceCodecError::MalformedWire(detail.into())
}
