// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use bytes::Bytes;
use novarocks_spi::connector::ProviderSourceType;
use novarocks_type_contract::CanonicalFactWriter;

use super::keyed::DataVersionFormat;
use super::{QueryDialect, SourceFieldReference};
use crate::persistence::identity::{
    DocumentRevision, FieldIdentity, ObjectIdentity, OutputIdentity,
};

/// Durable binding facts. Compact publications use this document's occurrence
/// order and native data-version formats, including for historical reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BindingDocument {
    pub occurrences: Vec<BindingOccurrence>,
    pub outputs: Vec<BoundOutput>,
    pub dependencies: Vec<BindingDependency>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BindingOccurrence {
    pub occurrence_id: u32,
    pub catalog_at_binding: String,
    pub namespace_at_binding: String,
    pub relation_at_binding: String,
    pub qualifier_at_binding: String,
    pub object_id: ObjectIdentity,
    pub selection: ReadSelection,
    pub data_version: DataVersionFormat,
    pub facts: SourceBindingFacts,
    pub paths: Vec<BoundSourcePath>,
}

/// Exact bytes from SQL (B1) and the provider (A1 through A4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceBindingFacts {
    pub occurrence: Bytes,
    pub object: Bytes,
    pub field_paths: Bytes,
    pub field_types: Bytes,
    pub read_declarations: Bytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadSelection {
    Current,
    Snapshot,
    Branch,
    Tag,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundSourcePath {
    pub nodes: Vec<BoundSourceNode>,
    pub leaf_type: ProviderSourceType,
    pub whole_subtree: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundSourceNode {
    pub field_id: FieldIdentity,
    /// Diagnostic name from the same resolved path. It does not enter A2 or A3.
    pub name_at_binding: String,
    pub required: bool,
    pub kind: SourceNodeKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceNodeKind {
    Scalar,
    Struct,
    List,
    Map,
}

impl BindingOccurrence {
    pub fn contains_field(&self, id: &FieldIdentity) -> bool {
        self.paths
            .iter()
            .any(|path| path.nodes.iter().any(|node| &node.field_id == id))
    }
}

/// B5's original SQL value identity with the statement's output placement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundOutput {
    pub ordinal: u32,
    pub name: String,
    pub logical_type_identity: Bytes,
    pub source_fields: Vec<SourceFieldReference>,
}

impl BoundOutput {
    pub fn identity(&self) -> OutputIdentity {
        output_identity(self.ordinal, &self.logical_type_identity)
    }
}

pub(crate) fn output_identity(ordinal: u32, logical_type_identity: &[u8]) -> OutputIdentity {
    let mut fact = CanonicalFactWriter::new("novarocks.mv.output-identity", 1);
    fact.u32(ordinal);
    fact.bytes(logical_type_identity);
    OutputIdentity::try_new(
        DocumentRevision::from_canonical_bytes(&fact.finish())
            .as_bytes()
            .to_vec(),
    )
    .expect("a fixed-size output digest is a valid opaque identity")
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BindingDependency {
    View {
        identity: Bytes,
        depth: u32,
        exact_declaration: Bytes,
        iceberg_version: Option<([u8; 16], i32)>,
        dialect: QueryDialect,
        canonical: Bytes,
    },
    NestedMv {
        object_id: ObjectIdentity,
        definition_revision: DocumentRevision,
        dialect: QueryDialect,
        canonical: Bytes,
    },
}

impl BindingDependency {
    pub fn canonical_bytes(&self) -> &[u8] {
        match self {
            Self::View { canonical, .. } | Self::NestedMv { canonical, .. } => canonical,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BindingDigests {
    pub resolution: DocumentRevision,
    pub dependencies: DocumentRevision,
    pub outputs: DocumentRevision,
}

impl BindingDocument {
    pub fn digests(&self) -> BindingDigests {
        let mut resolution = CanonicalFactWriter::new("novarocks.mv.binding-resolution", 1);
        resolution.sequence(self.occurrences.len());
        for item in &self.occurrences {
            for fact in [
                &item.facts.occurrence,
                &item.facts.object,
                &item.facts.field_paths,
                &item.facts.field_types,
                &item.facts.read_declarations,
            ] {
                resolution.bytes(fact);
            }
        }

        let mut dependencies = CanonicalFactWriter::new("novarocks.mv.binding-dependencies", 1);
        dependencies.sequence(self.dependencies.len());
        for item in &self.dependencies {
            dependencies.bytes(item.canonical_bytes());
        }

        let mut outputs = CanonicalFactWriter::new("novarocks.mv.binding-outputs", 1);
        outputs.sequence(self.outputs.len());
        for item in &self.outputs {
            outputs.u32(item.ordinal);
            outputs.text(&item.name);
            outputs.bytes(&item.logical_type_identity);
        }

        BindingDigests {
            resolution: DocumentRevision::from_canonical_bytes(&resolution.finish()),
            dependencies: DocumentRevision::from_canonical_bytes(&dependencies.finish()),
            outputs: DocumentRevision::from_canonical_bytes(&outputs.finish()),
        }
    }
}
