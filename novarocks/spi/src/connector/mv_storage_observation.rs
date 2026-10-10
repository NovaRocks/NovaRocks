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

//! Sealed, provider-neutral observations for materialized-view storage facts.
//!
//! A caller supplies a retained exact planning lease and the metadata loaded
//! through that lease. Implementations must project that already-sealed value;
//! they must not acquire a current generation, resolve a second table, or turn
//! this read-only observation into provider runtime IO. The consumer owns
//! durable decoding and policy. Providers own their handles and provenance
//! encodings.

use std::collections::{BTreeMap, HashSet};

use bytes::Bytes;

use super::{
    ConnectorCommittedVersion, ConnectorControlPlanningLease, ConnectorError, ConnectorErrorKind,
    ConnectorRequestContext, ConnectorTableIdentity, ConnectorTableMetadata,
    ConnectorTableObjectId,
};

pub const MAX_MV_OBSERVATION_FIELDS: usize = 4_096;
pub const MAX_MV_OBSERVATION_PARTITION_FIELDS: usize = 4_096;
pub const MAX_MV_OBSERVATION_REFS: usize = 1_024;
pub const MAX_MV_OBSERVATION_SNAPSHOTS: usize = 100_000;
pub const MAX_MV_LAKE_DESCRIPTOR_BYTES: usize = 64 * 1024;
pub const MAX_MV_LAKE_BASES: usize = 4_096;

const FIELD_FIXED_BYTES: usize = 32;
const PARTITION_FIELD_FIXED_BYTES: usize = 48;
const SNAPSHOT_BYTES: usize = 16;
const REF_FIXED_BYTES: usize = 16;
const MARKER_FIXED_BYTES: usize = 24;
const MAX_MV_SOURCE_FIELD_ID_BYTES: usize = 1024;

/// One source-schema field whose provider-owned identity is safe to persist
/// only through a consumer-owned document. The field's position in the
/// enclosing observation is a CREATE-time join coordinate, never an identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvObservedSourceField {
    provider_field_id: Bytes,
    name: String,
    type_signature: String,
    nullable: bool,
}

/// One partition field from the same exact provider metadata generation as
/// the physical schema. Both identities remain provider-opaque to consumers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvExactPartitionField {
    partition_field_id: Bytes,
    source_target_field_id: Bytes,
    transform: MvExactPartitionTransform,
}

impl MvExactPartitionField {
    pub fn try_new(
        partition_field_id: Bytes,
        source_target_field_id: Bytes,
        transform: MvExactPartitionTransform,
    ) -> Result<Self, ConnectorError> {
        if partition_field_id.is_empty()
            || source_target_field_id.is_empty()
            || partition_field_id.len() > MAX_MV_SOURCE_FIELD_ID_BYTES
            || source_target_field_id.len() > MAX_MV_SOURCE_FIELD_ID_BYTES
        {
            return corrupt("MV exact partition field has an empty or oversized identity");
        }
        if matches!(
            transform,
            MvExactPartitionTransform::Bucket { num_buckets: 0 }
                | MvExactPartitionTransform::Truncate { width: 0 }
        ) {
            return corrupt("MV exact partition field has an invalid transform parameter");
        }
        Ok(Self {
            partition_field_id,
            source_target_field_id,
            transform,
        })
    }

    pub const fn partition_field_id(&self) -> &Bytes {
        &self.partition_field_id
    }

    pub const fn source_target_field_id(&self) -> &Bytes {
        &self.source_target_field_id
    }

    pub const fn transform(&self) -> &MvExactPartitionTransform {
        &self.transform
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MvExactPartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket { num_buckets: u32 },
    Truncate { width: u32 },
    Void,
}

impl MvObservedSourceField {
    pub fn try_new(
        provider_field_id: Bytes,
        name: String,
        type_signature: String,
        nullable: bool,
    ) -> Result<Self, ConnectorError> {
        if provider_field_id.is_empty()
            || provider_field_id.len() > MAX_MV_SOURCE_FIELD_ID_BYTES
            || name.trim().is_empty()
            || type_signature.trim().is_empty()
        {
            return corrupt(
                "MV source field observation has an empty or oversized identity or schema fact",
            );
        }
        Ok(Self {
            provider_field_id,
            name,
            type_signature,
            nullable,
        })
    }

    pub const fn provider_field_id(&self) -> &Bytes {
        &self.provider_field_id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn type_signature(&self) -> &str {
        &self.type_signature
    }

    pub const fn nullable(&self) -> bool {
        self.nullable
    }
}

/// A provider projection from the same exact planning lease and metadata
/// generation that admitted one CREATE source. Frontend stamps SQL relation
/// occurrences onto this value; providers never receive or infer SQL aliases.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvCreateSourceObservation {
    table: ConnectorTableIdentity,
    object_id: ConnectorTableObjectId,
    schema_version: Bytes,
    fields: Vec<MvObservedSourceField>,
}

impl MvCreateSourceObservation {
    pub fn try_new(
        table: ConnectorTableIdentity,
        object_id: ConnectorTableObjectId,
        schema_version: Bytes,
        fields: Vec<MvObservedSourceField>,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        validate_context(context)?;
        validate_table(&table, "MV CREATE source")?;
        if schema_version.is_empty() || fields.is_empty() {
            return corrupt("MV CREATE source observation has no schema version or fields");
        }
        if fields.len() > MAX_MV_OBSERVATION_FIELDS {
            return exhausted("MV CREATE source observation exceeds the field limit");
        }
        let mut used = 0usize;
        reserve(
            &mut used,
            object_id.as_bytes().len(),
            context,
            "MV CREATE source",
        )?;
        reserve(&mut used, schema_version.len(), context, "MV CREATE source")?;
        let mut identities = HashSet::with_capacity(fields.len());
        for field in &fields {
            if !identities.insert(field.provider_field_id().as_ref()) {
                return corrupt(
                    "MV CREATE source observation has duplicate provider field identities",
                );
            }
            reserve(
                &mut used,
                FIELD_FIXED_BYTES
                    .saturating_add(field.provider_field_id().len())
                    .saturating_add(field.name().len())
                    .saturating_add(field.type_signature().len()),
                context,
                "MV CREATE source",
            )?;
        }
        validate_context(context)?;
        Ok(Self {
            table,
            object_id,
            schema_version,
            fields,
        })
    }

    pub const fn table(&self) -> &ConnectorTableIdentity {
        &self.table
    }

    pub const fn object_id(&self) -> &ConnectorTableObjectId {
        &self.object_id
    }

    pub const fn schema_version(&self) -> &Bytes {
        &self.schema_version
    }

    pub fn fields(&self) -> &[MvObservedSourceField] {
        &self.fields
    }
}

/// One target-schema field projected from sealed provider metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvObservedField {
    field_id: i32,
    name: String,
    type_signature: String,
    nullable: bool,
}

impl MvObservedField {
    pub fn new(field_id: i32, name: String, type_signature: String, nullable: bool) -> Self {
        Self {
            field_id,
            name,
            type_signature,
            nullable,
        }
    }

    pub const fn field_id(&self) -> i32 {
        self.field_id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn type_signature(&self) -> &str {
        &self.type_signature
    }

    pub const fn nullable(&self) -> bool {
        self.nullable
    }
}

/// Provider-neutral partition transform consumed by the frontend converter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MvObservedPartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket {
        num_buckets: u32,
    },
    Truncate {
        width: u32,
    },
    Void,
    /// Preserved exactly so the consumer can reject an unsupported transform.
    Unsupported(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvObservedPartitionField {
    partition_field_id: i32,
    partition_field_name: String,
    source_target_field_id: i32,
    source_column_name: String,
    transform: MvObservedPartitionTransform,
}

impl MvObservedPartitionField {
    pub fn new(
        partition_field_id: i32,
        partition_field_name: String,
        source_target_field_id: i32,
        source_column_name: String,
        transform: MvObservedPartitionTransform,
    ) -> Self {
        Self {
            partition_field_id,
            partition_field_name,
            source_target_field_id,
            source_column_name,
            transform,
        }
    }

    pub const fn partition_field_id(&self) -> i32 {
        self.partition_field_id
    }

    pub fn partition_field_name(&self) -> &str {
        &self.partition_field_name
    }

    pub const fn source_target_field_id(&self) -> i32 {
        self.source_target_field_id
    }

    pub fn source_column_name(&self) -> &str {
        &self.source_column_name
    }

    pub const fn transform(&self) -> &MvObservedPartitionTransform {
        &self.transform
    }
}

/// Default partition specification observed with a target schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvObservedPartitionSpec {
    spec_id: i32,
    fields: Vec<MvObservedPartitionField>,
}

impl MvObservedPartitionSpec {
    pub fn new(spec_id: i32, fields: Vec<MvObservedPartitionField>) -> Self {
        Self { spec_id, fields }
    }

    pub const fn spec_id(&self) -> i32 {
        self.spec_id
    }

    pub fn fields(&self) -> &[MvObservedPartitionField] {
        &self.fields
    }
}

/// Exact target facts observed immediately after creation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvCreatedTargetObservation {
    table: ConnectorTableIdentity,
    table_uuid: String,
    schema_id: i32,
    fields: Vec<MvObservedField>,
    partition: MvObservedPartitionSpec,
}

impl MvCreatedTargetObservation {
    pub fn try_new(
        table: ConnectorTableIdentity,
        table_uuid: String,
        schema_id: i32,
        fields: Vec<MvObservedField>,
        partition: MvObservedPartitionSpec,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        validate_context(context)?;
        validate_table(&table, "created MV target")?;
        require_non_empty(&table_uuid, "created MV target UUID")?;
        if schema_id < 0 {
            return corrupt("created MV target observation has a negative schema ID");
        }
        validate_fields_and_partition(&fields, &partition, context, "created MV target")?;
        Ok(Self {
            table,
            table_uuid,
            schema_id,
            fields,
            partition,
        })
    }

    pub const fn table(&self) -> &ConnectorTableIdentity {
        &self.table
    }
    pub fn table_uuid(&self) -> &str {
        &self.table_uuid
    }
    pub const fn schema_id(&self) -> i32 {
        self.schema_id
    }
    pub fn fields(&self) -> &[MvObservedField] {
        &self.fields
    }
    pub const fn partition(&self) -> &MvObservedPartitionSpec {
        &self.partition
    }
}

/// Exact physical schema facts from one retained metadata handle. Opaque
/// identities must use the same provider encoding as CREATE source bindings.
/// Each field carries its provider physical ordinal; consumers must not infer
/// identity or ordinal from a name, numeric field ID, or enumeration order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvSchemaValidationObservation {
    table: ConnectorTableIdentity,
    object_id: ConnectorTableObjectId,
    metadata_version: ConnectorCommittedVersion,
    schema_version: Bytes,
    partition_spec_version: Bytes,
    format_v3: bool,
    stored_row_lineage_enabled: bool,
    fields: Vec<(u32, MvObservedSourceField)>,
    partition_fields: Vec<MvExactPartitionField>,
}

impl MvSchemaValidationObservation {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        table: ConnectorTableIdentity,
        object_id: ConnectorTableObjectId,
        metadata_version: ConnectorCommittedVersion,
        schema_version: Bytes,
        partition_spec_version: Bytes,
        format_v3: bool,
        stored_row_lineage_enabled: bool,
        fields: Vec<(u32, MvObservedSourceField)>,
        partition_fields: Vec<MvExactPartitionField>,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        validate_context(context)?;
        validate_table(&table, "MV exact schema")?;
        metadata_version.validate()?;
        if schema_version.is_empty() || partition_spec_version.is_empty() || fields.is_empty() {
            return corrupt(
                "MV exact schema observation has missing schema, partition, or field facts",
            );
        }
        if fields.len() > MAX_MV_OBSERVATION_FIELDS {
            return exhausted("MV exact schema observation exceeds the field limit");
        }
        if partition_fields.len() > MAX_MV_OBSERVATION_PARTITION_FIELDS {
            return exhausted("MV exact schema observation exceeds the partition field limit");
        }
        let mut used = 0;
        for size in [
            object_id.as_bytes().len(),
            metadata_version.payload().len(),
            schema_version.len(),
            partition_spec_version.len(),
        ] {
            reserve(&mut used, size, context, "MV exact schema")?;
        }
        let mut ids = HashSet::new();
        let mut ordinals = HashSet::new();
        let mut names = HashSet::new();
        for (ordinal, field) in &fields {
            if !ids.insert(field.provider_field_id())
                || !ordinals.insert(*ordinal)
                || !names.insert(field.name().to_ascii_lowercase())
            {
                return corrupt(
                    "MV exact schema observation has duplicate field identity, ordinal, or name",
                );
            }
            reserve(
                &mut used,
                FIELD_FIXED_BYTES
                    .saturating_add(field.provider_field_id().len())
                    .saturating_add(field.name().len())
                    .saturating_add(field.type_signature().len()),
                context,
                "MV exact schema",
            )?;
        }
        let mut partition_ids = HashSet::with_capacity(partition_fields.len());
        for partition_field in &partition_fields {
            if !partition_ids.insert(partition_field.partition_field_id())
                || !ids.contains(partition_field.source_target_field_id())
            {
                return corrupt(
                    "MV exact schema observation has a duplicate partition identity or missing source field",
                );
            }
            reserve(
                &mut used,
                PARTITION_FIELD_FIXED_BYTES
                    .saturating_add(partition_field.partition_field_id().len())
                    .saturating_add(partition_field.source_target_field_id().len()),
                context,
                "MV exact schema",
            )?;
        }
        validate_context(context)?;
        Ok(Self {
            table,
            object_id,
            metadata_version,
            schema_version,
            partition_spec_version,
            format_v3,
            stored_row_lineage_enabled,
            fields,
            partition_fields,
        })
    }

    pub fn table(&self) -> &ConnectorTableIdentity {
        &self.table
    }
    pub fn object_id(&self) -> &ConnectorTableObjectId {
        &self.object_id
    }
    pub fn metadata_version(&self) -> &ConnectorCommittedVersion {
        &self.metadata_version
    }
    pub fn schema_version(&self) -> &Bytes {
        &self.schema_version
    }
    pub fn partition_spec_version(&self) -> &Bytes {
        &self.partition_spec_version
    }
    pub const fn is_format_v3(&self) -> bool {
        self.format_v3
    }
    pub const fn stored_row_lineage_enabled(&self) -> bool {
        self.stored_row_lineage_enabled
    }
    pub fn fields(&self) -> &[(u32, MvObservedSourceField)] {
        &self.fields
    }
    pub fn partition_fields(&self) -> &[MvExactPartitionField] {
        &self.partition_fields
    }
}

/// Bounded durable descriptor projection. The frontend parses this payload;
/// the SPI deliberately has no SQL, persistence, or descriptor dependency.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvLakeDescriptorProjection {
    package_id: String,
    inline_descriptor: String,
    content_hash: Option<String>,
}

impl MvLakeDescriptorProjection {
    pub fn try_new(
        package_id: String,
        inline_descriptor: String,
        content_hash: Option<String>,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        validate_context(context)?;
        require_non_empty(&package_id, "MV lake descriptor package ID")?;
        require_non_empty(&inline_descriptor, "MV lake descriptor inline payload")?;
        if inline_descriptor.len() > MAX_MV_LAKE_DESCRIPTOR_BYTES
            || inline_descriptor.len() > context.max_total_payload_bytes()
        {
            return exhausted("MV lake descriptor inline payload exceeds its bound");
        }
        if let Some(hash) = &content_hash {
            require_non_empty(hash, "MV lake descriptor content hash")?;
        }
        Ok(Self {
            package_id,
            inline_descriptor,
            content_hash,
        })
    }

    pub fn package_id(&self) -> &str {
        &self.package_id
    }
    pub fn inline_descriptor(&self) -> &str {
        &self.inline_descriptor
    }
    pub fn content_hash(&self) -> Option<&str> {
        self.content_hash.as_deref()
    }
}

/// Durable publication state discovered with an MV lake package.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MvLakePublicationObservation {
    NeverPublished,
    Published(MvPublishedRefreshObservation),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvPublishedRefreshObservation {
    pub target_snapshot_id: i64,
    pub publication_id: super::LakePublicationId,
    pub technique: MvPublishedRefreshTechnique,
    pub bases: Vec<MvPublishedBaseObservation>,
    pub definition_fingerprint: String,
    pub rows: i64,
    pub provenance_hash: String,
    pub waterline_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvPublishedBaseObservation {
    pub table_fqn: String,
    pub object_id: ConnectorTableObjectId,
    pub from_snapshot: Option<i64>,
    pub to_snapshot: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvPublishedRefreshTechnique {
    Incremental,
    Full,
    MetadataOnly,
}

impl MvPublishedRefreshObservation {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        target_snapshot_id: i64,
        publication_id: super::LakePublicationId,
        technique: MvPublishedRefreshTechnique,
        bases: Vec<MvPublishedBaseObservation>,
        definition_fingerprint: String,
        rows: i64,
        provenance_hash: String,
        waterline_hash: String,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        validate_context(context)?;
        if target_snapshot_id < 0 || rows < 0 {
            return corrupt("published MV lake facts contain a negative value");
        }
        require_non_empty(
            &definition_fingerprint,
            "published MV definition fingerprint",
        )?;
        require_non_empty(&provenance_hash, "published MV provenance hash")?;
        require_non_empty(&waterline_hash, "published MV waterline hash")?;
        if bases.len() > MAX_MV_LAKE_BASES {
            return exhausted("published MV lake facts exceed the base bound");
        }
        let mut names = HashSet::with_capacity(bases.len());
        let mut object_ids = HashSet::with_capacity(bases.len());
        let mut used = definition_fingerprint.len() + provenance_hash.len() + waterline_hash.len();
        for base in &bases {
            require_non_empty(&base.table_fqn, "published MV base table FQN")?;
            if base.to_snapshot < 0 || base.from_snapshot.is_some_and(|id| id < 0) {
                return corrupt("published MV lake facts have a negative base watermark");
            }
            if !names.insert(base.table_fqn.as_str()) || !object_ids.insert(base.object_id.clone())
            {
                return corrupt("published MV lake facts have duplicate base identity");
            }
            reserve(
                &mut used,
                base.table_fqn.len() + base.object_id.as_bytes().len(),
                context,
                "published MV lake facts",
            )?;
        }
        if used > context.max_total_payload_bytes() {
            return exhausted("published MV lake facts exceed the payload limit");
        }
        Ok(Self {
            target_snapshot_id,
            publication_id,
            technique,
            bases,
            definition_fingerprint,
            rows,
            provenance_hash,
            waterline_hash,
        })
    }
}

/// A fully bounded lake package projection. `None` means the table is not an
/// MV package; malformed durable state must instead be returned as CorruptData.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvLakePackageObservation {
    table: ConnectorTableIdentity,
    target_object_id: ConnectorTableObjectId,
    descriptor: MvLakeDescriptorProjection,
    current_target_snapshot: Option<MvLakeTargetSnapshotObservation>,
    publication: MvLakePublicationObservation,
}

/// Exact target snapshot metadata projected from the same sealed metadata
/// value as an MV lake package. It is a revision identity, not a refresh
/// watermark: a never-published target snapshot may legitimately appear.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MvLakeTargetSnapshotObservation {
    snapshot_id: i64,
    timestamp_ms: i64,
}

impl MvLakeTargetSnapshotObservation {
    pub fn try_new(snapshot_id: i64, timestamp_ms: i64) -> Result<Self, ConnectorError> {
        if snapshot_id < 0 || timestamp_ms < 0 {
            return corrupt("MV lake target snapshot observation has a negative value");
        }
        Ok(Self {
            snapshot_id,
            timestamp_ms,
        })
    }

    pub const fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    pub const fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }
}

impl MvLakePackageObservation {
    pub fn try_new(
        table: ConnectorTableIdentity,
        target_object_id: ConnectorTableObjectId,
        descriptor: MvLakeDescriptorProjection,
        current_target_snapshot: Option<MvLakeTargetSnapshotObservation>,
        publication: MvLakePublicationObservation,
    ) -> Result<Self, ConnectorError> {
        validate_table(&table, "MV lake package")?;
        Ok(Self {
            table,
            target_object_id,
            descriptor,
            current_target_snapshot,
            publication,
        })
    }
    pub const fn table(&self) -> &ConnectorTableIdentity {
        &self.table
    }
    pub const fn target_object_id(&self) -> &ConnectorTableObjectId {
        &self.target_object_id
    }
    pub const fn descriptor(&self) -> &MvLakeDescriptorProjection {
        &self.descriptor
    }
    pub const fn current_target_snapshot(&self) -> Option<MvLakeTargetSnapshotObservation> {
        self.current_target_snapshot
    }
    pub const fn publication(&self) -> &MvLakePublicationObservation {
        &self.publication
    }
}

/// A catalog discovery result is either complete or explicitly incomplete.
/// An implementation must never represent an unreadable candidate as an
/// absent MV package in a complete catalog result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MvLakeCatalogDiscovery {
    Complete(Vec<MvLakePackageOutcome>),
    Incomplete(MvLakeCatalogIncompleteReason),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MvLakeCatalogIncompleteReason {
    PaginationGap,
    GenerationDrift,
    DeadlineExceeded,
    BudgetExceeded,
    EnumerationFailed(ConnectorError),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MvLakePackageOutcome {
    Observed(MvLakePackageObservation),
    Failed(MvLakePackageFailure),
}

/// A package failure keeps its logical target and, when it was observed before
/// decoding failed, its opaque immutable identity so consumers can quarantine
/// exactly that target without treating the entire catalog as complete.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvLakePackageFailure {
    table: ConnectorTableIdentity,
    target_object_id: Option<ConnectorTableObjectId>,
    error: ConnectorError,
}

impl MvLakePackageFailure {
    pub fn try_new(
        table: ConnectorTableIdentity,
        target_object_id: Option<ConnectorTableObjectId>,
        error: ConnectorError,
    ) -> Result<Self, ConnectorError> {
        validate_table(&table, "MV lake package failure")?;
        Ok(Self {
            table,
            target_object_id,
            error,
        })
    }

    pub const fn table(&self) -> &ConnectorTableIdentity {
        &self.table
    }

    pub const fn target_object_id(&self) -> Option<&ConnectorTableObjectId> {
        self.target_object_id.as_ref()
    }

    pub const fn error(&self) -> &ConnectorError {
        &self.error
    }
}

/// Opaque physical identity and current snapshot projected from one exact base
/// metadata value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshBaseObservation {
    table: ConnectorTableIdentity,
    object_id: ConnectorTableObjectId,
    current_snapshot_id: Option<i64>,
}

impl MvRefreshBaseObservation {
    pub fn try_new(
        table: ConnectorTableIdentity,
        object_id: ConnectorTableObjectId,
        current_snapshot_id: Option<i64>,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        validate_context(context)?;
        validate_table(&table, "MV refresh base")?;
        if current_snapshot_id.is_some_and(|id| id < 0) {
            return corrupt("MV refresh base observation has a negative current snapshot ID");
        }
        Ok(Self {
            table,
            object_id,
            current_snapshot_id,
        })
    }
    pub const fn table(&self) -> &ConnectorTableIdentity {
        &self.table
    }
    pub const fn object_id(&self) -> &ConnectorTableObjectId {
        &self.object_id
    }
    pub const fn current_snapshot_id(&self) -> Option<i64> {
        self.current_snapshot_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvObservedRefreshMarker {
    pub publication_id: super::LakePublicationId,
}

/// Exact target facts required by refresh application.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshTargetObservation {
    table: ConnectorTableIdentity,
    table_uuid: String,
    schema_id: i32,
    partition: MvObservedPartitionSpec,
    current_snapshot_id: Option<i64>,
    ref_snapshot_ids: BTreeMap<String, i64>,
    field_ids: Vec<i32>,
    main_ancestor_snapshot_ids: Vec<i64>,
    snapshot_markers: BTreeMap<i64, MvObservedRefreshMarker>,
}

impl MvRefreshTargetObservation {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        table: ConnectorTableIdentity,
        table_uuid: String,
        schema_id: i32,
        partition: MvObservedPartitionSpec,
        current_snapshot_id: Option<i64>,
        ref_snapshot_ids: BTreeMap<String, i64>,
        field_ids: Vec<i32>,
        main_ancestor_snapshot_ids: Vec<i64>,
        snapshot_markers: BTreeMap<i64, MvObservedRefreshMarker>,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        validate_context(context)?;
        validate_table(&table, "MV refresh target")?;
        require_non_empty(&table_uuid, "MV refresh target UUID")?;
        if schema_id < 0 || current_snapshot_id.is_some_and(|id| id < 0) {
            return corrupt("MV refresh target observation has a negative schema or snapshot ID");
        }
        if ref_snapshot_ids.len() > MAX_MV_OBSERVATION_REFS {
            return exhausted("MV refresh target observation exceeds the ref bound");
        }
        if field_ids.len() > MAX_MV_OBSERVATION_FIELDS
            || main_ancestor_snapshot_ids.len() > MAX_MV_OBSERVATION_SNAPSHOTS
            || snapshot_markers.len() > MAX_MV_OBSERVATION_SNAPSHOTS
        {
            return exhausted("MV refresh target observation exceeds a collection bound");
        }
        let mut used = 0;
        let mut ids = HashSet::with_capacity(field_ids.len());
        for field_id in &field_ids {
            if !ids.insert(*field_id) {
                return corrupt("MV refresh target observation has duplicate field IDs");
            }
        }
        for (name, snapshot_id) in &ref_snapshot_ids {
            require_non_empty(name, "MV refresh target ref name")?;
            if *snapshot_id < 0 {
                return corrupt("MV refresh target ref has a negative snapshot ID");
            }
            reserve(
                &mut used,
                REF_FIXED_BYTES + name.len(),
                context,
                "MV refresh target refs",
            )?;
        }
        for snapshot_id in &main_ancestor_snapshot_ids {
            if *snapshot_id < 0 {
                return corrupt("MV refresh target lineage has a negative snapshot ID");
            }
            reserve(&mut used, 8, context, "MV refresh target lineage")?;
        }
        for snapshot_id in snapshot_markers.keys() {
            if *snapshot_id < 0 {
                return corrupt("MV refresh target marker has a negative snapshot ID");
            }
            reserve(
                &mut used,
                MARKER_FIXED_BYTES,
                context,
                "MV refresh target markers",
            )?;
        }
        Ok(Self {
            table,
            table_uuid,
            schema_id,
            partition,
            current_snapshot_id,
            ref_snapshot_ids,
            field_ids,
            main_ancestor_snapshot_ids,
            snapshot_markers,
        })
    }
    pub const fn table(&self) -> &ConnectorTableIdentity {
        &self.table
    }
    pub fn table_uuid(&self) -> &str {
        &self.table_uuid
    }
    pub const fn schema_id(&self) -> i32 {
        self.schema_id
    }
    pub const fn partition(&self) -> &MvObservedPartitionSpec {
        &self.partition
    }
    pub const fn current_snapshot_id(&self) -> Option<i64> {
        self.current_snapshot_id
    }
    pub fn ref_snapshot_ids(&self) -> &BTreeMap<String, i64> {
        &self.ref_snapshot_ids
    }
    pub fn field_ids(&self) -> &[i32] {
        &self.field_ids
    }
    pub fn main_ancestor_snapshot_ids(&self) -> &[i64] {
        &self.main_ancestor_snapshot_ids
    }
    pub fn snapshot_marker(&self, snapshot_id: i64) -> Option<&MvObservedRefreshMarker> {
        self.snapshot_markers.get(&snapshot_id)
    }

    /// All refresh markers carried by this bounded observation.
    ///
    /// The frontend converts these provider-neutral facts into its durable
    /// refresh application representation; it does not resolve any additional
    /// metadata while doing so.
    pub fn snapshot_markers(&self) -> &BTreeMap<i64, MvObservedRefreshMarker> {
        &self.snapshot_markers
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MvObservedSnapshot {
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
}

/// Exact declared values. The consumer, never a provider projection, supplies
/// defaults and policy clamping.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MvObservedMaintenancePolicy {
    pub maintenance_enabled: Option<bool>,
    pub expire_max_snapshot_age_ms: Option<i64>,
    pub expire_min_snapshots_to_keep: Option<u32>,
    pub target_file_size_bytes: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvMaintenanceMetadataObservation {
    current_snapshot_id: Option<i64>,
    snapshots: Vec<MvObservedSnapshot>,
    non_default_reference_count: usize,
    total_data_files: Option<u64>,
    total_delete_files: Option<u64>,
    total_files_size_bytes: Option<u64>,
    policy: MvObservedMaintenancePolicy,
}

impl MvMaintenanceMetadataObservation {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        current_snapshot_id: Option<i64>,
        snapshots: Vec<MvObservedSnapshot>,
        non_default_reference_count: usize,
        total_data_files: Option<u64>,
        total_delete_files: Option<u64>,
        total_files_size_bytes: Option<u64>,
        policy: MvObservedMaintenancePolicy,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        validate_context(context)?;
        if snapshots.len() > MAX_MV_OBSERVATION_SNAPSHOTS {
            return exhausted("MV maintenance observation exceeds the snapshot bound");
        }
        if current_snapshot_id.is_some_and(|id| id < 0) {
            return corrupt("MV maintenance observation has a negative current snapshot ID");
        }
        let mut ids = HashSet::with_capacity(snapshots.len());
        let mut used = 0;
        for snapshot in &snapshots {
            if snapshot.snapshot_id < 0 || !ids.insert(snapshot.snapshot_id) {
                return corrupt("MV maintenance observation has an invalid snapshot ID");
            }
            reserve(
                &mut used,
                SNAPSHOT_BYTES,
                context,
                "MV maintenance observation",
            )?;
        }
        if let Some(id) = current_snapshot_id
            && !ids.contains(&id)
        {
            return corrupt("MV maintenance observation current snapshot is not retained");
        }
        Ok(Self {
            current_snapshot_id,
            snapshots,
            non_default_reference_count,
            total_data_files,
            total_delete_files,
            total_files_size_bytes,
            policy,
        })
    }
    pub const fn current_snapshot_id(&self) -> Option<i64> {
        self.current_snapshot_id
    }
    pub fn snapshots(&self) -> &[MvObservedSnapshot] {
        &self.snapshots
    }
    pub const fn non_default_reference_count(&self) -> usize {
        self.non_default_reference_count
    }
    pub const fn total_data_files(&self) -> Option<u64> {
        self.total_data_files
    }
    pub const fn total_delete_files(&self) -> Option<u64> {
        self.total_delete_files
    }
    pub const fn total_files_size_bytes(&self) -> Option<u64> {
        self.total_files_size_bytes
    }
    pub const fn policy(&self) -> &MvObservedMaintenancePolicy {
        &self.policy
    }
}

/// Consumer-owned port physically published by SPI. An implementation must
/// answer from the supplied exact lease and sealed metadata only.
// Design: ADR-0086 (docs/adr/ADR-0086-frontend-mv-storage-observation-spi-relocation.md)
pub trait MvStorageObservationPort: Send + Sync {
    /// Projects opaque source identities from the same retained generation as
    /// `metadata`. A caller must reject this observation if its table or schema
    /// version does not exactly match that admitted metadata.
    fn observe_create_source(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvCreateSourceObservation, ConnectorError>;
    fn observe_created_target(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvCreatedTargetObservation, ConnectorError>;
    fn observe_schema_validation(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvSchemaValidationObservation, ConnectorError>;
    /// `Ok(None)` means not an MV package. Invalid durable properties are CorruptData.
    fn observe_lake_package(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<Option<MvLakePackageObservation>, ConnectorError>;
    fn observe_refresh_base(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvRefreshBaseObservation, ConnectorError>;
    fn observe_refresh_target(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvRefreshTargetObservation, ConnectorError>;
    fn observe_maintenance_metadata(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvMaintenanceMetadataObservation, ConnectorError>;
}

/// Fail-closed composition default.
#[derive(Clone, Copy, Debug, Default)]
pub struct UnavailableMvStorageObservationPort;

fn unavailable() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        "MV storage observation port is not installed",
    )
}

impl MvStorageObservationPort for UnavailableMvStorageObservationPort {
    fn observe_create_source(
        &self,
        _: &ConnectorControlPlanningLease,
        _: &ConnectorTableMetadata,
        _: ConnectorRequestContext,
    ) -> Result<MvCreateSourceObservation, ConnectorError> {
        Err(unavailable())
    }
    fn observe_created_target(
        &self,
        _: &ConnectorControlPlanningLease,
        _: &ConnectorTableMetadata,
        _: ConnectorRequestContext,
    ) -> Result<MvCreatedTargetObservation, ConnectorError> {
        Err(unavailable())
    }
    fn observe_schema_validation(
        &self,
        _: &ConnectorControlPlanningLease,
        _: &ConnectorTableMetadata,
        _: ConnectorRequestContext,
    ) -> Result<MvSchemaValidationObservation, ConnectorError> {
        Err(unavailable())
    }
    fn observe_lake_package(
        &self,
        _: &ConnectorControlPlanningLease,
        _: &ConnectorTableMetadata,
        _: ConnectorRequestContext,
    ) -> Result<Option<MvLakePackageObservation>, ConnectorError> {
        Err(unavailable())
    }
    fn observe_refresh_base(
        &self,
        _: &ConnectorControlPlanningLease,
        _: &ConnectorTableMetadata,
        _: ConnectorRequestContext,
    ) -> Result<MvRefreshBaseObservation, ConnectorError> {
        Err(unavailable())
    }
    fn observe_refresh_target(
        &self,
        _: &ConnectorControlPlanningLease,
        _: &ConnectorTableMetadata,
        _: ConnectorRequestContext,
    ) -> Result<MvRefreshTargetObservation, ConnectorError> {
        Err(unavailable())
    }
    fn observe_maintenance_metadata(
        &self,
        _: &ConnectorControlPlanningLease,
        _: &ConnectorTableMetadata,
        _: ConnectorRequestContext,
    ) -> Result<MvMaintenanceMetadataObservation, ConnectorError> {
        Err(unavailable())
    }
}

fn validate_context(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
    if context.is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "MV storage observation request was cancelled",
        ));
    }
    if std::time::Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "MV storage observation request deadline elapsed",
        ));
    }
    Ok(())
}

fn validate_table(table: &ConnectorTableIdentity, subject: &str) -> Result<(), ConnectorError> {
    if table.namespace.trim().is_empty() || table.table.trim().is_empty() {
        return corrupt(format!("{subject} has an empty table identity"));
    }
    Ok(())
}

fn validate_fields_and_partition(
    fields: &[MvObservedField],
    partition: &MvObservedPartitionSpec,
    context: &ConnectorRequestContext,
    subject: &str,
) -> Result<(), ConnectorError> {
    if fields.is_empty() {
        return corrupt(format!("{subject} observation has no schema fields"));
    }
    if fields.len() > MAX_MV_OBSERVATION_FIELDS
        || partition.fields.len() > MAX_MV_OBSERVATION_PARTITION_FIELDS
    {
        return exhausted(format!("{subject} observation exceeds a field bound"));
    }
    if partition.spec_id < 0 {
        return corrupt(format!("{subject} partition has a negative spec ID"));
    }
    let mut used = 0;
    let mut ids = HashSet::with_capacity(fields.len());
    let mut names = HashSet::with_capacity(fields.len());
    for field in fields {
        require_non_empty(&field.name, "MV observed field name")?;
        require_non_empty(&field.type_signature, "MV observed field type signature")?;
        if !ids.insert(field.field_id) || !names.insert(field.name.to_ascii_lowercase()) {
            return corrupt(format!("{subject} observation has duplicate schema fields"));
        }
        reserve(
            &mut used,
            FIELD_FIXED_BYTES + field.name.len() + field.type_signature.len(),
            context,
            subject,
        )?;
    }
    let mut partition_ids = HashSet::with_capacity(partition.fields.len());
    let mut partition_names = HashSet::with_capacity(partition.fields.len());
    for field in &partition.fields {
        require_non_empty(&field.partition_field_name, "MV partition field name")?;
        require_non_empty(&field.source_column_name, "MV partition source column name")?;
        if !partition_ids.insert(field.partition_field_id)
            || !partition_names.insert(field.partition_field_name.to_ascii_lowercase())
            || !ids.contains(&field.source_target_field_id)
        {
            return corrupt(format!(
                "{subject} partition is inconsistent with its schema"
            ));
        }
        match &field.transform {
            MvObservedPartitionTransform::Bucket { num_buckets: 0 }
            | MvObservedPartitionTransform::Truncate { width: 0 } => {
                return corrupt(format!(
                    "{subject} partition has a zero transform parameter"
                ));
            }
            MvObservedPartitionTransform::Unsupported(name) => {
                require_non_empty(name, "MV unsupported partition transform")?
            }
            _ => {}
        }
        reserve(
            &mut used,
            PARTITION_FIELD_FIXED_BYTES
                + field.partition_field_name.len()
                + field.source_column_name.len(),
            context,
            subject,
        )?;
    }
    Ok(())
}

fn require_non_empty(value: &str, subject: &str) -> Result<(), ConnectorError> {
    if value.trim().is_empty() {
        corrupt(format!("{subject} is empty"))
    } else {
        Ok(())
    }
}
fn reserve(
    used: &mut usize,
    additional: usize,
    context: &ConnectorRequestContext,
    subject: &str,
) -> Result<(), ConnectorError> {
    *used = used.checked_add(additional).ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            format!("{subject} payload accounting overflowed"),
        )
    })?;
    if *used > context.max_total_payload_bytes() {
        exhausted(format!(
            "{subject} exceeds the connector request payload budget"
        ))
    } else {
        Ok(())
    }
}
fn exhausted<T>(message: impl Into<String>) -> Result<T, ConnectorError> {
    Err(ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        message,
    ))
}
fn corrupt<T>(message: impl Into<String>) -> Result<T, ConnectorError> {
    Err(ConnectorError::new(
        ConnectorErrorKind::CorruptData,
        message,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use bytes::Bytes;

    use super::*;
    use crate::connector::ConnectorInstanceId;

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            crate::connector::ConnectorStopOwner::new().view(),
            1024,
            4096,
        )
        .unwrap()
    }
    fn table() -> ConnectorTableIdentity {
        ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse("iceberg.rest").unwrap(),
            namespace: Arc::from("db"),
            table: Arc::from("mv"),
        }
    }
    fn target_object_id() -> ConnectorTableObjectId {
        ConnectorTableObjectId::try_new(Bytes::from_static(b"target-uuid"))
            .expect("bounded target object ID")
    }
    fn fields() -> Vec<MvObservedField> {
        vec![MvObservedField::new(1, "id".into(), "bigint".into(), false)]
    }
    fn partition() -> MvObservedPartitionSpec {
        MvObservedPartitionSpec::new(
            0,
            vec![MvObservedPartitionField::new(
                100,
                "id".into(),
                1,
                "id".into(),
                MvObservedPartitionTransform::Identity,
            )],
        )
    }

    #[test]
    fn target_rejects_inconsistent_partition_and_cancelled_context() {
        let bad = MvObservedPartitionSpec::new(
            0,
            vec![MvObservedPartitionField::new(
                1,
                "id".into(),
                9,
                "id".into(),
                MvObservedPartitionTransform::Identity,
            )],
        );
        assert_eq!(
            MvCreatedTargetObservation::try_new(
                table(),
                "uuid".into(),
                1,
                fields(),
                bad,
                &context()
            )
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::CorruptData
        );
        let stop = crate::connector::ConnectorStopOwner::new();
        stop.request_stop();
        let cancelled = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            stop.view(),
            1024,
            4096,
        )
        .unwrap();
        assert_eq!(
            MvCreatedTargetObservation::try_new(
                table(),
                "uuid".into(),
                1,
                fields(),
                partition(),
                &cancelled
            )
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::Cancelled
        );
    }

    #[test]
    fn create_source_rejects_duplicate_opaque_field_identity() {
        let field = MvObservedSourceField::try_new(
            Bytes::from_static(b"provider-field"),
            "id".into(),
            "bigint".into(),
            false,
        )
        .expect("source field");
        let err = MvCreateSourceObservation::try_new(
            table(),
            target_object_id(),
            Bytes::from_static(b"schema-1"),
            vec![field.clone(), field],
            &context(),
        )
        .expect_err("duplicate provider identity must reject");
        assert_eq!(err.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn exact_schema_requires_unique_physical_identity_ordinal_and_name() {
        let field = |id, name: &str| {
            MvObservedSourceField::try_new(
                Bytes::from(vec![id]),
                name.to_owned(),
                "long".into(),
                false,
            )
            .unwrap()
        };
        let observe = |fields| {
            MvSchemaValidationObservation::try_new(
                table(),
                target_object_id(),
                ConnectorCommittedVersion::try_new(Bytes::from_static(b"metadata"), Some(11))
                    .unwrap(),
                Bytes::from_static(b"schema"),
                Bytes::from_static(b"spec"),
                true,
                true,
                fields,
                vec![],
                &context(),
            )
        };
        assert!(observe(vec![(0, field(1, "a")), (1, field(2, "b"))]).is_ok());
        for fields in [
            vec![(0, field(1, "a")), (1, field(1, "b"))],
            vec![(0, field(1, "a")), (0, field(2, "b"))],
            vec![(0, field(1, "a")), (1, field(2, "A"))],
        ] {
            assert_eq!(
                observe(fields).unwrap_err().kind(),
                ConnectorErrorKind::CorruptData
            );
        }
    }

    #[test]
    fn exact_partition_fields_require_unique_ids_and_same_schema_source() {
        let field = MvObservedSourceField::try_new(
            Bytes::from_static(b"source-field"),
            "id".into(),
            "long".into(),
            false,
        )
        .unwrap();
        let partition = |id: &'static [u8], source: &'static [u8]| {
            MvExactPartitionField::try_new(
                Bytes::from_static(id),
                Bytes::from_static(source),
                MvExactPartitionTransform::Void,
            )
            .unwrap()
        };
        let observe = |partition_fields| {
            MvSchemaValidationObservation::try_new(
                table(),
                target_object_id(),
                ConnectorCommittedVersion::try_new(Bytes::from_static(b"metadata"), Some(11))
                    .unwrap(),
                Bytes::from_static(b"schema"),
                Bytes::from_static(b"spec"),
                true,
                true,
                vec![(0, field.clone())],
                partition_fields,
                &context(),
            )
        };
        assert!(observe(vec![partition(b"partition", b"source-field")]).is_ok());
        assert_eq!(
            observe(vec![partition(b"partition", b"unknown")])
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
        assert_eq!(
            observe(vec![
                partition(b"partition", b"source-field"),
                partition(b"partition", b"source-field"),
            ])
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::CorruptData
        );
        assert_eq!(
            MvExactPartitionField::try_new(
                Bytes::from_static(b"partition"),
                Bytes::from_static(b"source-field"),
                MvExactPartitionTransform::Bucket { num_buckets: 0 },
            )
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn lake_projection_is_bounded_and_unavailable_is_unsupported() {
        let err = MvLakeDescriptorProjection::try_new(
            "pkg".into(),
            "x".repeat(MAX_MV_LAKE_DESCRIPTOR_BYTES + 1),
            None,
            &context(),
        )
        .unwrap_err();
        assert_eq!(err.kind(), ConnectorErrorKind::ResourceExhausted);
        assert_eq!(unavailable().kind(), ConnectorErrorKind::Unsupported);
    }

    #[test]
    fn lake_package_preserves_exact_current_target_snapshot() {
        let descriptor = MvLakeDescriptorProjection::try_new(
            "pkg".into(),
            "descriptor".into(),
            None,
            &context(),
        )
        .unwrap();
        let snapshot = MvLakeTargetSnapshotObservation::try_new(42, 1_700_000_042_000).unwrap();
        let package = MvLakePackageObservation::try_new(
            table(),
            target_object_id(),
            descriptor,
            Some(snapshot),
            MvLakePublicationObservation::NeverPublished,
        )
        .unwrap();

        assert_eq!(package.current_target_snapshot(), Some(snapshot));
        assert_eq!(package.target_object_id(), &target_object_id());
        assert_eq!(snapshot.snapshot_id(), 42);
        assert_eq!(snapshot.timestamp_ms(), 1_700_000_042_000);
        assert_eq!(
            MvLakeTargetSnapshotObservation::try_new(-1, 0)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
        assert_eq!(
            MvLakeTargetSnapshotObservation::try_new(1, -1)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn discovery_keeps_catalog_incomplete_distinct_from_one_package_failure() {
        let failure = MvLakePackageFailure::try_new(
            table(),
            Some(target_object_id()),
            ConnectorError::new(ConnectorErrorKind::CorruptData, "bad descriptor"),
        )
        .expect("stable package failure");
        let complete =
            MvLakeCatalogDiscovery::Complete(vec![MvLakePackageOutcome::Failed(failure.clone())]);
        assert!(matches!(complete, MvLakeCatalogDiscovery::Complete(_)));
        assert_eq!(failure.target_object_id(), Some(&target_object_id()));
        let incomplete =
            MvLakeCatalogDiscovery::Incomplete(MvLakeCatalogIncompleteReason::GenerationDrift);
        assert!(matches!(
            incomplete,
            MvLakeCatalogDiscovery::Incomplete(MvLakeCatalogIncompleteReason::GenerationDrift)
        ));
    }

    #[test]
    fn refresh_and_maintenance_reject_ambiguous_metadata() {
        let err = MvRefreshTargetObservation::try_new(
            table(),
            "uuid".into(),
            1,
            partition(),
            None,
            BTreeMap::new(),
            vec![1, 1],
            vec![],
            BTreeMap::new(),
            &context(),
        )
        .unwrap_err();
        assert_eq!(err.kind(), ConnectorErrorKind::CorruptData);
        let err = MvMaintenanceMetadataObservation::try_new(
            Some(1),
            vec![MvObservedSnapshot {
                snapshot_id: 2,
                timestamp_ms: 0,
            }],
            0,
            None,
            None,
            None,
            MvObservedMaintenancePolicy::default(),
            &context(),
        )
        .unwrap_err();
        assert_eq!(err.kind(), ConnectorErrorKind::CorruptData);
    }
}
