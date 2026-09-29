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

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use crate::iceberg::spec::{
    DataFileFormat, FormatVersion, Literal, ManifestStatus, PartitionSpec, PrimitiveLiteral,
    PrimitiveType, Schema, Struct, StructType, Type,
};

use super::{DeleteSemanticsError as Error, DeleteSemanticsErrorKind as Kind, FileMetrics, Result};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DataSequenceNumber(i64);

impl DataSequenceNumber {
    pub fn try_new(value: i64) -> Result<Self> {
        if value < 0 {
            return Err(Error::new(
                Kind::InvalidSequence,
                "negative Iceberg data sequence number",
            ));
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> i64 {
        self.0
    }
}

/// Facts before, or after, SDK inheritance. File sequence is intentionally not
/// an input: it is not a substitute for the data sequence used by deletes.
#[derive(Clone, Copy, Debug)]
pub struct EntrySequence {
    pub format_version: FormatVersion,
    pub status: ManifestStatus,
    pub data_sequence: Option<i64>,
    pub manifest_sequence: i64,
}

impl EntrySequence {
    /// Deleted entries are not live snapshot input. A change reader that needs
    /// one must call `required_sequence`, rather than filling in a fallback.
    pub fn live_sequence(self) -> Result<Option<DataSequenceNumber>> {
        if self.status == ManifestStatus::Deleted {
            return Ok(None);
        }
        self.required_sequence().map(Some)
    }

    pub fn required_sequence(self) -> Result<DataSequenceNumber> {
        let value = match self.data_sequence {
            Some(value) => value,
            None if self.format_version == FormatVersion::V1 => 0,
            None if self.status == ManifestStatus::Added => self.manifest_sequence,
            None => {
                return Err(Error::new(
                    Kind::MissingSequence,
                    "live Iceberg entry has no inherited data sequence number",
                ));
            }
        };
        DataSequenceNumber::try_new(value)
    }
}

/// Canonical scalar equality preserves signed zero and canonicalizes all NaN
/// payloads. The bound primitive type lives alongside these bytes/values.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CanonicalScalar {
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(u32),
    Double(u64),
    Decimal(i128),
    Uuid(u128),
    String(Arc<str>),
    Binary(Arc<[u8]>),
}

impl CanonicalScalar {
    pub fn from_literal(value: &PrimitiveLiteral, value_type: &PrimitiveType) -> Result<Self> {
        // Physical historical values, stored partition constants and current
        // data keys must converge on the same resolved field type.
        match (value, value_type) {
            (PrimitiveLiteral::Int(value), PrimitiveType::Long) => {
                return Ok(Self::Long(i64::from(*value)));
            }
            (PrimitiveLiteral::Float(value), PrimitiveType::Double) => {
                let value = f64::from(value.0);
                return Ok(Self::Double(if value.is_nan() {
                    f64::NAN.to_bits()
                } else {
                    value.to_bits()
                }));
            }
            _ => {}
        }
        if matches!(
            (value, value_type),
            (
                PrimitiveLiteral::Int(_),
                PrimitiveType::Timestamp | PrimitiveType::TimestampNs
            )
        ) {
            return Err(Error::new(
                Kind::UnsupportedPromotion,
                "date partition value requires an unsupported date-to-timestamp unit conversion",
            ));
        }
        if !value_type.compatible(value) {
            return Err(Error::new(
                Kind::InvalidFieldBinding,
                "Iceberg scalar does not match its resolved primitive type",
            ));
        }
        Ok(match value {
            PrimitiveLiteral::Boolean(v) => Self::Boolean(*v),
            PrimitiveLiteral::Int(v) => Self::Int(*v),
            PrimitiveLiteral::Long(v) => Self::Long(*v),
            PrimitiveLiteral::Float(v) => Self::Float(if v.is_nan() {
                f32::NAN.to_bits()
            } else {
                v.to_bits()
            }),
            PrimitiveLiteral::Double(v) => Self::Double(if v.is_nan() {
                f64::NAN.to_bits()
            } else {
                v.to_bits()
            }),
            PrimitiveLiteral::Int128(v) => Self::Decimal(*v),
            PrimitiveLiteral::UInt128(v) => Self::Uuid(*v),
            PrimitiveLiteral::String(v) => Self::String(Arc::from(v.as_str())),
            PrimitiveLiteral::Binary(v) => {
                if let PrimitiveType::Fixed(length) = value_type {
                    if *length != v.len() as u64 {
                        return Err(Error::new(
                            Kind::InvalidFieldBinding,
                            "Iceberg fixed scalar has the wrong length",
                        ));
                    }
                }
                Self::Binary(Arc::from(v.as_slice()))
            }
            PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin => {
                return Err(Error::new(
                    Kind::InvalidFieldBinding,
                    "Iceberg range sentinel is not a stored scalar",
                ));
            }
        })
    }
}

/// A typed stored tuple, not a transform re-evaluation or a debug string.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TypedPartition {
    spec_id: i32,
    fields: Arc<[(i32, PrimitiveType)]>,
    values: Arc<[Option<CanonicalScalar>]>,
    unpartitioned: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct PartitionTypeBinding {
    fields: Arc<[(i32, PrimitiveType)]>,
    unpartitioned: bool,
}

impl PartitionTypeBinding {
    pub(crate) fn validate(&self, partition: &TypedPartition) -> Result<()> {
        if self.fields != partition.fields || self.unpartitioned != partition.unpartitioned {
            return Err(Error::new(
                Kind::InvalidPartition,
                "partition type or global-scope proof differs from the pinned endpoint",
            ));
        }
        Ok(())
    }
}

impl TypedPartition {
    pub fn bind(spec: &PartitionSpec, schema: &Schema, tuple: &Struct) -> Result<Self> {
        let partition_type = spec
            .partition_type(schema)
            .map_err(|e| Error::new(Kind::InvalidPartition, e.to_string()))?;
        Self::bind_type(spec, &partition_type, tuple)
    }

    /// Bind stored values to the FE-frozen storage type, independently of the
    /// projected query schema. Historical partition sources may be dropped.
    pub fn bind_type(
        spec: &PartitionSpec,
        partition_type: &StructType,
        tuple: &Struct,
    ) -> Result<Self> {
        validate_partition_type(spec, partition_type)?;
        if spec.spec_id() < 0 || partition_type.fields().len() != tuple.fields().len() {
            return Err(Error::new(
                Kind::InvalidPartition,
                "Iceberg partition spec or tuple arity is invalid",
            ));
        }
        let mut fields = Vec::with_capacity(tuple.fields().len());
        let mut values = Vec::with_capacity(tuple.fields().len());
        for (field, value) in partition_type.fields().iter().zip(tuple.fields()) {
            let Type::Primitive(value_type) = field.field_type.as_ref() else {
                return Err(Error::new(
                    Kind::InvalidPartition,
                    "Iceberg partition field is not primitive",
                ));
            };
            let value = match value {
                None => None,
                Some(Literal::Primitive(value)) => {
                    Some(CanonicalScalar::from_literal(value, value_type)?)
                }
                Some(_) => {
                    return Err(Error::new(
                        Kind::InvalidPartition,
                        "Iceberg partition value is not primitive",
                    ));
                }
            };
            fields.push((field.id, value_type.clone()));
            values.push(value);
        }
        Ok(Self {
            spec_id: spec.spec_id(),
            fields: fields.into(),
            values: values.into(),
            unpartitioned: spec.is_unpartitioned(),
        })
    }

    pub const fn spec_id(&self) -> i32 {
        self.spec_id
    }
    pub fn fields(&self) -> &[(i32, PrimitiveType)] {
        &self.fields
    }
    pub fn values(&self) -> &[Option<CanonicalScalar>] {
        &self.values
    }
    pub const fn is_unpartitioned(&self) -> bool {
        self.unpartitioned
    }
}

fn validate_partition_type(spec: &PartitionSpec, partition_type: &StructType) -> Result<()> {
    if spec.fields().len() != partition_type.fields().len()
        || spec
            .fields()
            .iter()
            .zip(partition_type.fields())
            .any(|(declared, field)| {
                declared.field_id != field.id
                    || declared.name != field.name
                    || field.required
                    || !matches!(field.field_type.as_ref(), Type::Primitive(_))
            })
    {
        return Err(Error::new(
            Kind::InvalidPartition,
            "partition storage type does not match its ordered spec fields",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum EqualityScope {
    Global,
    Partition(TypedPartition),
}

/// IDs are sorted numerically, independent of manifest column order. Readers
/// bind their physical column positions to this same ordered ID/type tuple.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct EqualityFieldGroup(Arc<[(i32, PrimitiveType)]>);

impl EqualityFieldGroup {
    pub fn bind(ids: &[i32], schema: &Schema) -> Result<Self> {
        if ids.is_empty() {
            return Err(Error::new(
                Kind::InvalidFieldBinding,
                "equality delete has no equality field IDs",
            ));
        }
        let mut ordered = ids.to_vec();
        ordered.sort_unstable();
        if ordered.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(Error::new(
                Kind::InvalidFieldBinding,
                "equality delete repeats an equality field ID",
            ));
        }
        let fields = ordered
            .into_iter()
            .map(|id| {
                let field = schema.field_by_id(id).ok_or_else(|| {
                    Error::new(
                        Kind::InvalidFieldBinding,
                        format!("equality field ID {id} is absent from the bound schema"),
                    )
                })?;
                let Type::Primitive(primitive) = field.field_type.as_ref() else {
                    return Err(Error::new(
                        Kind::InvalidFieldBinding,
                        "equality field is not a primitive field",
                    ));
                };
                if matches!(primitive, PrimitiveType::Variant) {
                    return Err(Error::new(
                        Kind::InvalidFieldBinding,
                        "variant equality keys are not supported",
                    ));
                }
                Ok((id, primitive.clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self(fields.into()))
    }

    pub fn fields(&self) -> &[(i32, PrimitiveType)] {
        &self.0
    }

    pub fn validate_schema(&self, schema: &Schema) -> Result<()> {
        let rebound = Self::bind(
            &self.0.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            schema,
        )?;
        if self != &rebound {
            return Err(Error::new(
                Kind::InvalidFieldBinding,
                "equality field types differ from the pinned read schema",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum DeleteContentAddress {
    File(Arc<str>),
    PuffinBlob {
        path: Arc<str>,
        offset: u64,
        length: u64,
    },
}

impl DeleteContentAddress {
    pub fn file(path: impl Into<Arc<str>>) -> Result<Self> {
        let path = path.into();
        require_path(&path)?;
        Ok(Self::File(path))
    }

    pub fn puffin(
        path: impl Into<Arc<str>>,
        offset: i64,
        length: i64,
        file_size: u64,
    ) -> Result<Self> {
        let path = path.into();
        require_path(&path)?;
        let (offset, length) = (u64::try_from(offset), u64::try_from(length));
        let (Ok(offset), Ok(length)) = (offset, length) else {
            return Err(Error::new(
                Kind::InvalidAddress,
                "negative Puffin blob range",
            ));
        };
        if length == 0 || offset.checked_add(length).is_none_or(|end| end > file_size) {
            return Err(Error::new(
                Kind::InvalidAddress,
                "Puffin blob range is empty or outside its container",
            ));
        }
        Ok(Self::PuffinBlob {
            path,
            offset,
            length,
        })
    }

    pub fn path(&self) -> &str {
        match self {
            Self::File(path) | Self::PuffinBlob { path, .. } => path,
        }
    }
}

fn require_path(path: &str) -> Result<()> {
    if path.is_empty() {
        return Err(Error::new(
            Kind::InvalidAddress,
            "Iceberg exact path is empty",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DeleteFormat {
    Parquet,
    Avro,
    Orc,
    Puffin,
}

impl From<DataFileFormat> for DeleteFormat {
    fn from(value: DataFileFormat) -> Self {
        match value {
            DataFileFormat::Parquet => Self::Parquet,
            DataFileFormat::Avro => Self::Avro,
            DataFileFormat::Orc => Self::Orc,
            DataFileFormat::Puffin => Self::Puffin,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DeleteReadFacts {
    pub format: DeleteFormat,
    pub file_size: u64,
    pub record_count: u64,
    pub key_metadata: Arc<[u8]>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum DeleteKind {
    /// An exact target may be explicitly referenced or proved by path bounds.
    Position {
        exact_target: Option<Arc<str>>,
    },
    DeletionVector {
        exact_target: Arc<str>,
    },
    Equality(EqualityFieldGroup),
}

/// The complete identity of applying content. Metrics and manifest provenance
/// do not alter decoding/application, while count, format, scope and sequence
/// do. A physical address alone must never stand in for this identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DeleteApplication {
    address: DeleteContentAddress,
    kind: DeleteKind,
    sequence: DataSequenceNumber,
    partition: TypedPartition,
    read: DeleteReadFacts,
}

#[derive(Clone, Debug)]
pub struct DeleteFactParams {
    pub address: DeleteContentAddress,
    pub kind: DeleteKind,
    pub sequence: DataSequenceNumber,
    pub partition: TypedPartition,
    pub read: DeleteReadFacts,
    pub metrics: FileMetrics,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestEntryProvenance {
    pub manifest_path: Arc<str>,
    pub entry_ordinal: usize,
}

#[derive(Clone, Debug)]
pub struct DeleteFact {
    application: DeleteApplication,
    metrics: FileMetrics,
    provenance: Option<ManifestEntryProvenance>,
}

impl DeleteFact {
    /// Used by wire admission and by the raw observation normalizer. This does
    /// not perform whole-snapshot DV uniqueness validation.
    pub fn try_new(mut params: DeleteFactParams) -> Result<Self> {
        require_path(params.address.path())?;
        match (&params.kind, &params.address, params.read.format) {
            (
                DeleteKind::DeletionVector { exact_target },
                DeleteContentAddress::PuffinBlob { offset, length, .. },
                DeleteFormat::Puffin,
            ) => {
                require_path(exact_target)?;
                if *length == 0
                    || offset
                        .checked_add(*length)
                        .is_none_or(|end| end > params.read.file_size)
                {
                    return Err(Error::new(
                        Kind::InvalidAddress,
                        "Puffin blob range is outside its container",
                    ));
                }
            }
            (DeleteKind::Position { exact_target }, DeleteContentAddress::File(_), format)
                if format != DeleteFormat::Puffin =>
            {
                if let Some(target) = exact_target {
                    require_path(target)?;
                }
            }
            (DeleteKind::Equality(_), DeleteContentAddress::File(_), format)
                if format != DeleteFormat::Puffin => {}
            _ => {
                return Err(Error::new(
                    Kind::InvalidAddress,
                    "delete kind, format and content address disagree",
                ));
            }
        }
        if let DeleteKind::Position { exact_target } = &mut params.kind {
            if exact_target.is_none() {
                *exact_target = params.metrics.exact_position_target();
            }
        }
        Ok(Self {
            application: DeleteApplication {
                address: params.address,
                kind: params.kind,
                sequence: params.sequence,
                partition: params.partition,
                read: params.read,
            },
            metrics: params.metrics,
            provenance: None,
        })
    }

    pub fn application(&self) -> &DeleteApplication {
        &self.application
    }
    pub fn address(&self) -> &DeleteContentAddress {
        &self.application.address
    }
    pub fn kind(&self) -> &DeleteKind {
        &self.application.kind
    }
    pub fn sequence(&self) -> DataSequenceNumber {
        self.application.sequence
    }
    pub fn partition(&self) -> &TypedPartition {
        &self.application.partition
    }
    pub fn read(&self) -> &DeleteReadFacts {
        &self.application.read
    }
    /// Variable storage retained by the canonical expanded private descriptor.
    /// The descriptor's fixed layout is supplied by its runtime owner.
    pub fn descriptor_variable_bytes(&self) -> usize {
        let (fields, target) = match self.kind() {
            DeleteKind::Equality(group) => (group.fields().len(), 0),
            DeleteKind::Position { exact_target } => {
                (0, exact_target.as_ref().map_or(0, |path| path.len()))
            }
            DeleteKind::DeletionVector { exact_target } => (0, exact_target.len()),
        };
        self.address()
            .path()
            .len()
            .saturating_add(self.partition().to_json_string().len())
            .saturating_add(target)
            .saturating_add(fields.saturating_mul(std::mem::size_of::<i32>()))
            .saturating_add(self.read().key_metadata.len())
    }
    pub fn metrics(&self) -> &FileMetrics {
        &self.metrics
    }
    pub fn provenance(&self) -> Option<&ManifestEntryProvenance> {
        self.provenance.as_ref()
    }
    pub fn equality_scope(&self) -> EqualityScope {
        if self.partition().is_unpartitioned() {
            EqualityScope::Global
        } else {
            EqualityScope::Partition(self.partition().clone())
        }
    }
}

/// Raw entry multiplicity is retained until the manifest observation is closed.
/// The authoritative optional entry facts normalize the sequence. Raw file
/// facts contain no placeholder sequence that a caller could accidentally use.
#[derive(Clone, Debug)]
pub struct RawDeleteEntry {
    pub sequence: EntrySequence,
    pub file: RawDeleteFile,
}

#[derive(Clone, Debug)]
pub struct RawDeleteFile {
    pub address: DeleteContentAddress,
    pub kind: DeleteKind,
    pub partition: TypedPartition,
    pub read: DeleteReadFacts,
    pub metrics: FileMetrics,
}

#[derive(Clone, Debug)]
pub struct ManifestDeleteObservation {
    pub manifest_path: Arc<str>,
    pub entries: Vec<RawDeleteEntry>,
}

/// A closed, validated manifest observation; only constructors can establish
/// raw multiplicity or normalized-recall semantics.
#[derive(Clone, Debug)]
pub struct DeleteObservation {
    pub(crate) facts: Vec<Arc<DeleteFact>>,
    observed_manifest_count: usize,
}

impl DeleteObservation {
    pub fn from_manifests(
        manifests: impl IntoIterator<Item = ManifestDeleteObservation>,
    ) -> Result<Self> {
        let mut paths = HashSet::new();
        let mut dv_targets = HashSet::new();
        let mut facts = Vec::new();
        for manifest in manifests {
            require_path(&manifest.manifest_path)?;
            // Java's manifest-list path dedup occurs before reading entries.
            if !paths.insert(Arc::clone(&manifest.manifest_path)) {
                continue;
            }
            for (entry_ordinal, entry) in manifest.entries.into_iter().enumerate() {
                let Some(sequence) = entry.sequence.live_sequence()? else {
                    continue;
                };
                let mut fact = DeleteFact::try_new(DeleteFactParams {
                    address: entry.file.address,
                    kind: entry.file.kind,
                    sequence,
                    partition: entry.file.partition,
                    read: entry.file.read,
                    metrics: entry.file.metrics,
                })?;
                if let DeleteKind::DeletionVector { exact_target } = fact.kind() {
                    if !dv_targets.insert(Arc::clone(exact_target)) {
                        return Err(Error::new(
                            Kind::MultipleDeletionVectors,
                            format!("multiple live deletion vector entries target {exact_target}"),
                        ));
                    }
                }
                fact.provenance = Some(ManifestEntryProvenance {
                    manifest_path: Arc::clone(&manifest.manifest_path),
                    entry_ordinal,
                });
                facts.push(Arc::new(fact));
            }
        }
        Ok(Self {
            facts,
            observed_manifest_count: paths.len(),
        })
    }

    /// Only for facts already normalized by an earlier observation or wire
    /// validation. Identical application recalls are idempotent. Distinct DV
    /// applications for one target are still invalid; never use this entrance
    /// to hide raw duplicate manifest entries.
    pub fn from_normalized_recall(
        facts: impl IntoIterator<Item = Arc<DeleteFact>>,
    ) -> Result<Self> {
        let mut applications = HashSet::new();
        let mut dv_targets = HashSet::new();
        let mut retained = Vec::new();
        for fact in facts {
            if !applications.insert(fact.application().clone()) {
                continue;
            }
            if let DeleteKind::DeletionVector { exact_target } = fact.kind() {
                if !dv_targets.insert(Arc::clone(exact_target)) {
                    return Err(Error::new(
                        Kind::MultipleDeletionVectors,
                        "normalized recall contains distinct DVs for one target",
                    ));
                }
            }
            retained.push(fact);
        }
        Ok(Self {
            facts: retained,
            observed_manifest_count: 0,
        })
    }

    pub fn facts(&self) -> &[Arc<DeleteFact>] {
        &self.facts
    }
    pub const fn observed_manifest_count(&self) -> usize {
        self.observed_manifest_count
    }
}

#[derive(Clone, Debug)]
pub struct DataFileFact {
    path: Arc<str>,
    sequence: DataSequenceNumber,
    partition: TypedPartition,
    pub record_count: u64,
    pub metrics: FileMetrics,
}

impl DataFileFact {
    pub fn try_new(
        path: impl Into<Arc<str>>,
        sequence: DataSequenceNumber,
        partition: TypedPartition,
        record_count: u64,
        metrics: FileMetrics,
    ) -> Result<Self> {
        let path = path.into();
        require_path(&path)?;
        Ok(Self {
            path,
            sequence,
            partition,
            record_count,
            metrics,
        })
    }
    pub fn path(&self) -> &str {
        &self.path
    }
    pub const fn sequence(&self) -> DataSequenceNumber {
        self.sequence
    }
    pub fn partition(&self) -> &TypedPartition {
        &self.partition
    }
}

/// Minted once by the FE relation owner and transported to all sibling splits.
/// No random generation and no guessed/default identity exists in this module.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ReadObservationId([u8; 16]);

impl ReadObservationId {
    pub fn try_new(bytes: [u8; 16]) -> Result<Self> {
        if bytes == [0; 16] {
            return Err(Error::new(
                Kind::DomainMismatch,
                "read observation identity is absent",
            ));
        }
        Ok(Self(bytes))
    }
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PinnedEndpointFacts {
    table_uuid: uuid::Uuid,
    metadata_identity: Arc<str>,
    snapshot_id: i64,
    schema_json: Arc<str>,
    partition_spec_jsons: BTreeMap<i32, Arc<str>>,
    partition_type_jsons: BTreeMap<i32, Arc<str>>,
}

impl PinnedEndpointFacts {
    /// Metadata identity is supplied by the pinned metadata owner (normally
    /// the exact metadata-file location), never the table root or a BE lookup.
    pub fn try_new(
        table_uuid: uuid::Uuid,
        metadata_identity: impl Into<Arc<str>>,
        snapshot_id: i64,
        schema: &Schema,
        partition_specs: &[PartitionSpec],
    ) -> Result<Self> {
        let partition_types = partition_specs
            .iter()
            .map(|spec| {
                spec.partition_type(schema)
                    .map(|ty| (spec.spec_id(), ty))
                    .map_err(|e| Error::new(Kind::InvalidPartition, e.to_string()))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        Self::try_new_with_partition_types(
            table_uuid,
            metadata_identity,
            snapshot_id,
            schema,
            partition_specs,
            &partition_types,
        )
    }

    /// The FE supplies proven storage bindings; wire admission validates their
    /// shape and compares the complete domain to the independent relation.
    pub fn try_new_with_partition_types(
        table_uuid: uuid::Uuid,
        metadata_identity: impl Into<Arc<str>>,
        snapshot_id: i64,
        schema: &Schema,
        partition_specs: &[PartitionSpec],
        partition_types: &BTreeMap<i32, StructType>,
    ) -> Result<Self> {
        let metadata_identity = metadata_identity.into();
        if table_uuid.is_nil() || metadata_identity.is_empty() {
            return Err(Error::new(
                Kind::DomainMismatch,
                "pinned endpoint table or metadata identity is absent",
            ));
        }
        let schema_json = super::canonical_schema_json(schema)
            .map_err(|e| Error::new(Kind::InvalidFieldBinding, e.to_string()))?;
        let mut partition_spec_jsons = BTreeMap::new();
        let mut partition_type_jsons = BTreeMap::new();
        for spec in partition_specs {
            let partition_type = partition_types.get(&spec.spec_id()).ok_or_else(|| {
                Error::new(
                    Kind::InvalidPartition,
                    "pinned partition storage type is absent",
                )
            })?;
            validate_partition_type(spec, partition_type)?;
            for (source, field) in spec.fields().iter().zip(partition_type.fields()) {
                if let Some(query_field) = schema.field_by_id(source.source_id) {
                    // An older query can predate this spec's transform input
                    // type. Such a storage binding is authoritative only as
                    // part of the complete independently pinned FE domain.
                    if let Ok(expected) = source.transform.result_type(&query_field.field_type)
                        && expected != *field.field_type
                    {
                        return Err(Error::new(
                            Kind::InvalidPartition,
                            "partition storage type disagrees with its resolved query source",
                        ));
                    }
                }
            }
            let mut type_value = serde_json::to_value(partition_type)
                .map_err(|e| Error::new(Kind::InvalidPartition, e.to_string()))?;
            type_value.sort_all_objects();
            partition_type_jsons.insert(spec.spec_id(), Arc::from(type_value.to_string()));
            let json: Arc<str> = Arc::from(
                serde_json::to_string(spec)
                    .map_err(|e| Error::new(Kind::InvalidPartition, e.to_string()))?,
            );
            if partition_spec_jsons
                .insert(spec.spec_id(), json.clone())
                .is_some_and(|old| old != json)
            {
                return Err(Error::new(
                    Kind::InvalidPartition,
                    "pinned endpoint has conflicting partition specs",
                ));
            }
        }
        if partition_types.len() != partition_type_jsons.len() {
            return Err(Error::new(
                Kind::InvalidPartition,
                "partition storage types and specs have different inventories",
            ));
        }
        Ok(Self {
            table_uuid,
            metadata_identity,
            snapshot_id,
            schema_json: Arc::from(schema_json),
            partition_spec_jsons,
            partition_type_jsons,
        })
    }
    pub fn partition_type_jsons(&self) -> &BTreeMap<i32, Arc<str>> {
        &self.partition_type_jsons
    }
    pub fn partition_type(&self, spec_id: i32) -> Result<StructType> {
        let json = self.partition_type_jsons.get(&spec_id).ok_or_else(|| {
            Error::new(
                Kind::InvalidPartition,
                "partition storage type is absent from the pinned endpoint",
            )
        })?;
        serde_json::from_str(json).map_err(|e| Error::new(Kind::InvalidPartition, e.to_string()))
    }
    pub const fn table_uuid(&self) -> uuid::Uuid {
        self.table_uuid
    }
    pub fn metadata_identity(&self) -> &str {
        &self.metadata_identity
    }
    pub const fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }
    pub fn schema_json(&self) -> &str {
        &self.schema_json
    }
    pub fn partition_spec_jsons(&self) -> &BTreeMap<i32, Arc<str>> {
        &self.partition_spec_jsons
    }
    pub fn schema(&self) -> Result<Schema> {
        serde_json::from_str(&self.schema_json)
            .map_err(|e| Error::new(Kind::InvalidFieldBinding, e.to_string()))
    }

    /// Checks type/scope proof against the relation's frozen spec set. The
    /// caller cannot introduce a made-up unpartitioned spec on a delete alone.
    pub(crate) fn bind_partition_specs(&self) -> Result<BTreeMap<i32, PartitionTypeBinding>> {
        self.partition_spec_jsons
            .iter()
            .map(|(id, json)| {
                let spec: PartitionSpec = serde_json::from_str(json)
                    .map_err(|e| Error::new(Kind::InvalidPartition, e.to_string()))?;
                let partition_type = self.partition_type(*id)?;
                let fields = partition_type
                    .fields()
                    .iter()
                    .map(|field| {
                        let Type::Primitive(primitive) = field.field_type.as_ref() else {
                            return Err(Error::new(
                                Kind::InvalidPartition,
                                "partition field is not primitive",
                            ));
                        };
                        Ok((field.id, primitive.clone()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok((
                    *id,
                    PartitionTypeBinding {
                        fields: fields.into(),
                        unpartitioned: spec.is_unpartitioned(),
                    },
                ))
            })
            .collect()
    }
}

pub(crate) fn validate_partition_binding(
    bindings: &BTreeMap<i32, PartitionTypeBinding>,
    partition: &TypedPartition,
) -> Result<()> {
    bindings
        .get(&partition.spec_id())
        .ok_or_else(|| {
            Error::new(
                Kind::InvalidPartition,
                "partition spec is absent from the pinned endpoint",
            )
        })?
        .validate(partition)
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ReadDomain {
    observation: ReadObservationId,
    endpoint: PinnedEndpointFacts,
    expanded_descriptor_bytes: usize,
}

impl ReadDomain {
    pub fn new(observation: ReadObservationId, endpoint: PinnedEndpointFacts) -> Self {
        // Canonical expanded scheduling charge, independent of Arc sharing.
        // Precompute once: split charge must not re-walk the schema/spec maps.
        let expanded_descriptor_bytes = endpoint
            .partition_spec_jsons
            .values()
            .chain(endpoint.partition_type_jsons.values())
            .fold(
                size_of::<Self>()
                    .saturating_add(endpoint.metadata_identity.len())
                    .saturating_add(endpoint.schema_json.len()),
                |bytes, json| {
                    bytes
                        .saturating_add(size_of::<i32>() + size_of::<Arc<str>>())
                        .saturating_add(json.len())
                },
            );
        Self {
            observation,
            endpoint,
            expanded_descriptor_bytes,
        }
    }
    pub const fn expanded_descriptor_bytes(&self) -> usize {
        self.expanded_descriptor_bytes
    }
    pub const fn observation(&self) -> ReadObservationId {
        self.observation
    }
    pub fn endpoint(&self) -> &PinnedEndpointFacts {
        &self.endpoint
    }
}
