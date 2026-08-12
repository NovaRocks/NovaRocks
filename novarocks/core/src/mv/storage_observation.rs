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

//! Consumer-owned observation boundary for MV storage facts.
//!
//! This is an application-owned public port. Consumers retain an exact
//! connector planning lease while an adapter reads provider-specific storage,
//! then receive only validated neutral values.  It is not a Connector SPI
//! capability and must not expose concrete table handles or catalog entries.

use std::collections::{BTreeMap, HashSet};

use novarocks_catalog::identifier::normalize_identifier;
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorControlResolver, ConnectorError, ConnectorErrorKind,
    ConnectorInstanceId, ConnectorListNamespacesRequest, ConnectorListTablesRequest,
    ConnectorRequestContext, ConnectorTableIdentity, ConnectorTableMetadata, ConnectorTableRequest,
    ConnectorTableResolution, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
};

use crate::mv::persistence::{descriptor::MvDescriptorV1, schema::MvPartitionContract};

const MAX_MV_SCHEMA_VALIDATION_FIELDS: usize = 4_096;
const MAX_MV_SCHEMA_VALIDATION_PARTITION_FIELDS: usize = 4_096;
const MV_SCHEMA_VALIDATION_FIELD_BYTES: usize = 32;
const MV_SCHEMA_VALIDATION_PARTITION_FIELD_BYTES: usize = 48;

/// Exact target-schema facts observed immediately after CREATE/bootstrap.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvTargetCreationObservation {
    pub table: ConnectorTableIdentity,
    pub table_uuid: String,
    pub schema_id: i32,
    pub fields: Vec<MvObservedTargetField>,
    pub partition: MvPartitionContract,
}

impl MvTargetCreationObservation {
    pub fn try_new(
        table: ConnectorTableIdentity,
        table_uuid: String,
        schema_id: i32,
        fields: Vec<MvObservedTargetField>,
        partition: MvPartitionContract,
    ) -> Result<Self, ConnectorError> {
        validate_table_identity(&table, "created MV target")?;
        require_non_empty(&table_uuid, "created MV target UUID")?;
        if fields.is_empty() {
            return corrupt("created MV target observation has no schema fields");
        }

        let mut field_ids = HashSet::with_capacity(fields.len());
        let mut field_names = HashSet::with_capacity(fields.len());
        for field in &fields {
            require_non_empty(&field.name, "created MV target field name")?;
            require_non_empty(&field.type_signature, "created MV target field type")?;
            if !field_ids.insert(field.field_id) {
                return corrupt(format!(
                    "created MV target observation has duplicate field ID {}",
                    field.field_id
                ));
            }
            if !field_names.insert(field.name.as_str()) {
                return corrupt(format!(
                    "created MV target observation has duplicate field name `{}`",
                    field.name
                ));
            }
        }
        validate_partition_contract(&partition, &fields)?;

        Ok(Self {
            table,
            table_uuid,
            schema_id,
            fields,
            partition,
        })
    }
}

/// Exact current-schema facts consumed by the Core MV contract validator.
///
/// The provider-specific Server adapter constructs this value while retaining
/// the exact connector generation that loaded the source metadata. Core never
/// interprets the opaque table handle or provider schema values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvSchemaValidationObservation {
    table_uuid: String,
    schema_id: i32,
    format_v3: bool,
    stored_row_lineage_enabled: bool,
    fields: Vec<MvObservedTargetField>,
    partition: MvSchemaValidationPartitionContract,
}

impl MvSchemaValidationObservation {
    pub fn try_new(
        table_uuid: String,
        schema_id: i32,
        format_v3: bool,
        stored_row_lineage_enabled: bool,
        fields: Vec<MvObservedTargetField>,
        partition: MvSchemaValidationPartitionContract,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        validate_request_context(context)?;
        Self::try_new_with_payload_limit(
            table_uuid,
            schema_id,
            format_v3,
            stored_row_lineage_enabled,
            fields,
            partition,
            context.max_total_payload_bytes(),
        )
    }

    pub(crate) fn try_new_with_maximum_payload(
        table_uuid: String,
        schema_id: i32,
        format_v3: bool,
        stored_row_lineage_enabled: bool,
        fields: Vec<MvObservedTargetField>,
        partition: MvSchemaValidationPartitionContract,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_with_payload_limit(
            table_uuid,
            schema_id,
            format_v3,
            stored_row_lineage_enabled,
            fields,
            partition,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
    }

    fn try_new_with_payload_limit(
        table_uuid: String,
        schema_id: i32,
        format_v3: bool,
        stored_row_lineage_enabled: bool,
        fields: Vec<MvObservedTargetField>,
        partition: MvSchemaValidationPartitionContract,
        max_total_payload_bytes: usize,
    ) -> Result<Self, ConnectorError> {
        require_non_empty(&table_uuid, "MV schema validation table UUID")?;
        if schema_id < 0 {
            return corrupt("MV schema validation observation has a negative schema ID");
        }
        if fields.len() > MAX_MV_SCHEMA_VALIDATION_FIELDS {
            return exhausted("MV schema validation observation exceeds the field limit");
        }
        if partition.fields().len() > MAX_MV_SCHEMA_VALIDATION_PARTITION_FIELDS {
            return exhausted("MV schema validation observation exceeds the partition field limit");
        }

        let mut payload_bytes = 0;
        reserve_schema_validation_payload(
            &mut payload_bytes,
            table_uuid.len(),
            max_total_payload_bytes,
        )?;
        let mut field_ids = HashSet::with_capacity(fields.len());
        let mut field_names = HashSet::with_capacity(fields.len());
        for field in &fields {
            require_non_empty(&field.name, "MV schema validation field name")?;
            require_non_empty(
                &field.type_signature,
                "MV schema validation field type signature",
            )?;
            if !field_ids.insert(field.field_id) {
                return corrupt(format!(
                    "MV schema validation observation has duplicate field ID {}",
                    field.field_id
                ));
            }
            let normalized_name = field.name.to_ascii_lowercase();
            if !field_names.insert(normalized_name) {
                return corrupt(format!(
                    "MV schema validation observation has duplicate field name `{}`",
                    field.name
                ));
            }
            reserve_schema_validation_payload(
                &mut payload_bytes,
                MV_SCHEMA_VALIDATION_FIELD_BYTES
                    .saturating_add(field.name.len())
                    .saturating_add(field.type_signature.len()),
                max_total_payload_bytes,
            )?;
        }
        validate_schema_validation_partition_contract(&partition, &fields)?;
        for field in partition.fields() {
            reserve_schema_validation_payload(
                &mut payload_bytes,
                MV_SCHEMA_VALIDATION_PARTITION_FIELD_BYTES
                    .saturating_add(field.partition_field_name().len())
                    .saturating_add(field.source_column_name().len()),
                max_total_payload_bytes,
            )?;
        }

        Ok(Self {
            table_uuid,
            schema_id,
            format_v3,
            stored_row_lineage_enabled,
            fields,
            partition,
        })
    }

    pub fn table_uuid(&self) -> &str {
        &self.table_uuid
    }

    pub const fn schema_id(&self) -> i32 {
        self.schema_id
    }

    pub const fn is_format_v3(&self) -> bool {
        self.format_v3
    }

    /// Whether the table explicitly enables stored row-lineage values.
    pub const fn stored_row_lineage_enabled(&self) -> bool {
        self.stored_row_lineage_enabled
    }

    pub fn fields(&self) -> &[MvObservedTargetField] {
        &self.fields
    }

    pub const fn partition(&self) -> &MvSchemaValidationPartitionContract {
        &self.partition
    }
}

/// One field in an observed target schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvObservedTargetField {
    pub field_id: i32,
    pub name: String,
    pub type_signature: String,
    pub nullable: bool,
}

impl MvObservedTargetField {
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

/// Provider-neutral current default partition specification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvSchemaValidationPartitionContract {
    spec_id: i32,
    fields: Vec<MvSchemaValidationPartitionField>,
}

impl MvSchemaValidationPartitionContract {
    pub fn new(spec_id: i32, fields: Vec<MvSchemaValidationPartitionField>) -> Self {
        Self { spec_id, fields }
    }

    pub const fn spec_id(&self) -> i32 {
        self.spec_id
    }

    pub fn fields(&self) -> &[MvSchemaValidationPartitionField] {
        &self.fields
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvSchemaValidationPartitionField {
    partition_field_id: i32,
    partition_field_name: String,
    source_target_field_id: i32,
    source_column_name: String,
    transform: MvSchemaValidationPartitionTransform,
}

impl MvSchemaValidationPartitionField {
    pub fn new(
        partition_field_id: i32,
        partition_field_name: String,
        source_target_field_id: i32,
        source_column_name: String,
        transform: MvSchemaValidationPartitionTransform,
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

    pub const fn transform(&self) -> &MvSchemaValidationPartitionTransform {
        &self.transform
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MvSchemaValidationPartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket { num_buckets: u32 },
    Truncate { width: u32 },
    Void,
    Unsupported(String),
}

/// A discovered MV lake package, including its current publication state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvLakePackageObservation {
    pub table: ConnectorTableIdentity,
    pub descriptor: MvDescriptorV1,
    pub publication: MvLakePublication,
}

impl MvLakePackageObservation {
    pub fn try_new(
        table: ConnectorTableIdentity,
        descriptor: MvDescriptorV1,
        publication: MvLakePublication,
    ) -> Result<Self, ConnectorError> {
        validate_table_identity(&table, "MV lake package")?;
        validate_descriptor(&descriptor)?;
        if let MvLakePublication::Published(facts) = &publication {
            facts.validate()?;
        }
        Ok(Self {
            table,
            descriptor,
            publication,
        })
    }
}

/// The only publication states meaningful to lake recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MvLakePublication {
    NeverPublished,
    Published(MvPublishedLakeFacts),
}

/// Persisted refresh facts observed together with the lake package.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvPublishedLakeFacts {
    pub target_snapshot_id: i64,
    pub refresh_id: i64,
    pub mv_id: i64,
    pub token: String,
    pub technique: MvPublishedRefreshTechnique,
    pub bases: Vec<MvPublishedBaseFact>,
    pub definition_fingerprint: String,
    pub rows: i64,
    pub provenance_hash: String,
    pub waterline_hash: String,
}

impl MvPublishedLakeFacts {
    pub fn try_new(
        target_snapshot_id: i64,
        refresh_id: i64,
        mv_id: i64,
        token: String,
        technique: MvPublishedRefreshTechnique,
        bases: Vec<MvPublishedBaseFact>,
        definition_fingerprint: String,
        rows: i64,
        provenance_hash: String,
        waterline_hash: String,
    ) -> Result<Self, ConnectorError> {
        let facts = Self {
            target_snapshot_id,
            refresh_id,
            mv_id,
            token,
            technique,
            bases,
            definition_fingerprint,
            rows,
            provenance_hash,
            waterline_hash,
        };
        facts.validate()?;
        Ok(facts)
    }

    fn validate(&self) -> Result<(), ConnectorError> {
        if self.target_snapshot_id < 0 {
            return corrupt("published MV lake facts have a negative target snapshot ID");
        }
        if self.refresh_id < 0 {
            return corrupt("published MV lake facts have a negative refresh ID");
        }
        if self.mv_id < 0 {
            return corrupt("published MV lake facts have a negative MV ID");
        }
        require_non_empty(&self.token, "published MV refresh token")?;
        require_non_empty(
            &self.definition_fingerprint,
            "published MV definition fingerprint",
        )?;
        require_non_empty(&self.provenance_hash, "published MV provenance hash")?;
        require_non_empty(&self.waterline_hash, "published MV waterline hash")?;
        if self.rows < 0 {
            return corrupt("published MV lake facts have a negative row count");
        }

        let mut base_fqns = HashSet::with_capacity(self.bases.len());
        let mut base_uuids = HashSet::with_capacity(self.bases.len());
        for base in &self.bases {
            require_non_empty(&base.table_fqn, "published MV base table FQN")?;
            require_non_empty(&base.table_uuid, "published MV base table UUID")?;
            if !base_fqns.insert(base.table_fqn.as_str())
                || !base_uuids.insert(base.table_uuid.as_str())
            {
                return corrupt(format!(
                    "published MV lake facts have duplicate base identity `{}` ({})",
                    base.table_fqn, base.table_uuid
                ));
            }
            if base.to_snapshot < 0 || base.from_snapshot.is_some_and(|snapshot| snapshot < 0) {
                return corrupt(format!(
                    "published MV lake facts have a negative watermark for base `{}`",
                    base.table_fqn
                ));
            }
        }
        Ok(())
    }
}

/// One base-table identity and refresh watermark from MV provenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvPublishedBaseFact {
    pub table_fqn: String,
    pub table_uuid: String,
    pub from_snapshot: Option<i64>,
    pub to_snapshot: i64,
}

/// The published refresh technique, detached from provider provenance types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvPublishedRefreshTechnique {
    Incremental,
    Full,
    MetadataOnly,
}

/// Exact base-table identity pinned for one MV refresh attempt.
///
/// The UUID and current snapshot come from the same sealed metadata value
/// loaded through `table`'s retained exact connector generation. Keeping this
/// observation narrow prevents base pinning from becoming a general-purpose
/// provider metadata surface.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshBaseObservation {
    table: ConnectorTableIdentity,
    table_uuid: String,
    current_snapshot_id: Option<i64>,
}

impl MvRefreshBaseObservation {
    pub fn try_new(
        table: ConnectorTableIdentity,
        table_uuid: String,
        current_snapshot_id: Option<i64>,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        validate_request_context(context)?;
        validate_table_identity(&table, "MV refresh base")?;
        require_non_empty(&table_uuid, "MV refresh base table UUID")?;
        if let Some(snapshot_id) = current_snapshot_id
            && snapshot_id < 0
        {
            return corrupt("MV refresh base observation has a negative current snapshot ID");
        }
        Ok(Self {
            table,
            table_uuid,
            current_snapshot_id,
        })
    }

    pub const fn table(&self) -> &ConnectorTableIdentity {
        &self.table
    }

    pub fn table_uuid(&self) -> &str {
        &self.table_uuid
    }

    pub const fn current_snapshot_id(&self) -> Option<i64> {
        self.current_snapshot_id
    }
}

/// Maximum number of named refs carried by one refresh-target observation.
///
/// MV apply only ever consults `main` plus the refresh staging branches, so a
/// table whose ref count exceeds this bound is reported as corrupt rather than
/// silently truncated.
const MAX_MV_REFRESH_TARGET_REFS: usize = 1_024;

/// Exact refresh-time target facts consumed by the MV target apply path.
///
/// This is deliberately a distinct use case from `observe_schema_validation`:
/// apply needs snapshot and ref identity, which contract validation does not,
/// and does not need the per-field schema payload, which contract validation
/// does. Keeping them separate stops either observation from growing into a
/// general-purpose provider metadata dump.
///
/// Core owns MV watermark and staging-branch semantics, so snapshot and ref
/// identity are legitimate neutral facts. Storage layout facts that only the
/// physical writer needs — table location, sequence numbers, partition spec
/// objects — are deliberately absent: they belong to the Provider's own write
/// preparation, not to a Core-visible observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshTargetObservation {
    table: ConnectorTableIdentity,
    table_uuid: String,
    schema_id: i32,
    partition: MvPartitionContract,
    current_snapshot_id: Option<i64>,
    ref_snapshot_ids: BTreeMap<String, i64>,
    field_ids: Vec<i32>,
    main_ancestor_snapshot_ids: Vec<i64>,
    current_snapshot_is_empty_bootstrap: bool,
    snapshot_markers: BTreeMap<i64, MvObservedRefreshMarker>,
}

/// The MV refresh identity a target snapshot records.
///
/// The Provider decodes it from its own provenance encoding; Core only ever
/// compares it against the identity in its own refresh ledger.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvObservedRefreshMarker {
    pub refresh_id: i64,
    pub mv_id: i64,
    pub token: String,
}

impl MvRefreshTargetObservation {
    pub fn try_new(
        table: ConnectorTableIdentity,
        table_uuid: String,
        schema_id: i32,
        partition: MvPartitionContract,
        current_snapshot_id: Option<i64>,
        ref_snapshot_ids: BTreeMap<String, i64>,
        field_ids: Vec<i32>,
        main_ancestor_snapshot_ids: Vec<i64>,
        current_snapshot_is_empty_bootstrap: bool,
        snapshot_markers: BTreeMap<i64, MvObservedRefreshMarker>,
        context: &ConnectorRequestContext,
    ) -> Result<Self, ConnectorError> {
        validate_request_context(context)?;
        validate_table_identity(&table, "MV refresh target")?;
        require_non_empty(&table_uuid, "MV refresh target UUID")?;
        if schema_id < 0 {
            return corrupt("MV refresh target observation has a negative schema ID");
        }
        if ref_snapshot_ids.len() > MAX_MV_REFRESH_TARGET_REFS {
            return corrupt(format!(
                "MV refresh target observation carries {} refs, exceeding the {} bound",
                ref_snapshot_ids.len(),
                MAX_MV_REFRESH_TARGET_REFS
            ));
        }
        for (name, snapshot_id) in &ref_snapshot_ids {
            require_non_empty(name, "MV refresh target ref name")?;
            if *snapshot_id < 0 {
                return corrupt(format!(
                    "MV refresh target ref `{name}` has a negative snapshot ID"
                ));
            }
        }
        if let Some(snapshot_id) = current_snapshot_id
            && snapshot_id < 0
        {
            return corrupt("MV refresh target observation has a negative current snapshot ID");
        }

        for snapshot_id in main_ancestor_snapshot_ids.iter().copied() {
            if snapshot_id < 0 {
                return corrupt("MV refresh target lineage has a negative snapshot ID");
            }
        }
        for (snapshot_id, marker) in &snapshot_markers {
            if *snapshot_id < 0 {
                return corrupt("MV refresh target marker has a negative snapshot ID");
            }
            require_non_empty(&marker.token, "MV refresh target marker token")?;
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
            current_snapshot_is_empty_bootstrap,
            snapshot_markers,
        })
    }

    /// Target schema field IDs in schema order, positionally aligned with the
    /// Arrow schema in the neutral metadata.
    pub fn field_ids(&self) -> &[i32] {
        &self.field_ids
    }

    /// Is the current snapshot the empty bootstrap snapshot CREATE MV
    /// establishes before the first refresh publishes data?
    pub const fn current_snapshot_is_empty_bootstrap(&self) -> bool {
        self.current_snapshot_is_empty_bootstrap
    }

    /// `main`'s snapshot chain, newest first.
    pub fn main_ancestor_snapshot_ids(&self) -> &[i64] {
        &self.main_ancestor_snapshot_ids
    }

    /// Marker recorded by `snapshot_id`, if that snapshot carries one.
    pub fn snapshot_marker(&self, snapshot_id: i64) -> Option<&MvObservedRefreshMarker> {
        self.snapshot_markers.get(&snapshot_id)
    }

    pub fn table(&self) -> &ConnectorTableIdentity {
        &self.table
    }

    pub fn table_uuid(&self) -> &str {
        &self.table_uuid
    }

    pub const fn schema_id(&self) -> i32 {
        self.schema_id
    }

    pub const fn partition(&self) -> &MvPartitionContract {
        &self.partition
    }

    pub const fn current_snapshot_id(&self) -> Option<i64> {
        self.current_snapshot_id
    }

    /// Snapshot pinned by `ref_name`.
    ///
    /// `main` deliberately falls back to the current snapshot: a table whose
    /// `main` ref has not been materialized still has a current snapshot, and
    /// the MV apply path treats those as the same fact. Any other missing ref
    /// is reported as absent rather than silently resolved to `main`.
    pub fn snapshot_id_for_ref(&self, ref_name: &str) -> Option<i64> {
        self.ref_snapshot_ids.get(ref_name).copied().or_else(|| {
            (ref_name == "main")
                .then_some(self.current_snapshot_id)
                .flatten()
        })
    }

    pub fn ref_snapshot_ids(&self) -> &BTreeMap<String, i64> {
        &self.ref_snapshot_ids
    }
}

/// Maximum number of snapshots carried by one maintenance observation.
const MAX_MV_MAINTENANCE_SNAPSHOTS: usize = 100_000;

/// Two `i64` values per projected snapshot, charged against the payload budget.
const MV_MAINTENANCE_SNAPSHOT_BYTES: usize = 16;

/// Exact maintenance facts a provider can project from one metadata load.
///
/// Every value here is a pure metadata projection, so one already-loaded
/// metadata document answers the whole observation. Facts that need provider
/// runtime IO are deliberately absent: they have a different cost profile and
/// belong to a separate observation with its own failure modes.
///
/// Core owns retention and policy decisions, so snapshot identity, ref counts,
/// declared policy values, and the current-snapshot counters are legitimate
/// neutral facts. Provider-specific naming is not: the observation reports how
/// many references are not the provider's default one, never which ref that is.
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

/// One retained snapshot and the instant it was committed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MvObservedSnapshot {
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
}

/// Maintenance policy values exactly as the storage table declares them.
///
/// Each value is absent when the table declares nothing usable. Defaults and
/// clamping belong to the policy owner, never to the observation: an absent
/// value and a declared value must stay distinguishable across the boundary.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MvObservedMaintenancePolicy {
    pub maintenance_enabled: Option<bool>,
    pub expire_max_snapshot_age_ms: Option<i64>,
    pub expire_min_snapshots_to_keep: Option<u32>,
    pub target_file_size_bytes: Option<i64>,
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
        validate_request_context(context)?;
        if snapshots.len() > MAX_MV_MAINTENANCE_SNAPSHOTS {
            return exhausted("MV maintenance observation exceeds the snapshot limit");
        }
        if let Some(snapshot_id) = current_snapshot_id
            && snapshot_id < 0
        {
            return corrupt("MV maintenance observation has a negative current snapshot ID");
        }

        let mut payload_bytes = 0;
        let mut snapshot_ids = HashSet::with_capacity(snapshots.len());
        for snapshot in &snapshots {
            if snapshot.snapshot_id < 0 {
                return corrupt("MV maintenance observation has a negative snapshot ID");
            }
            if !snapshot_ids.insert(snapshot.snapshot_id) {
                return corrupt(format!(
                    "MV maintenance observation has duplicate snapshot ID {}",
                    snapshot.snapshot_id
                ));
            }
            reserve_maintenance_payload(
                &mut payload_bytes,
                MV_MAINTENANCE_SNAPSHOT_BYTES,
                context.max_total_payload_bytes(),
            )?;
        }
        // The retained-snapshot list is the only place a consumer can resolve a
        // snapshot's timestamp, so a current snapshot missing from it is
        // corrupt metadata rather than a fact worth forwarding.
        if let Some(snapshot_id) = current_snapshot_id
            && !snapshot_ids.contains(&snapshot_id)
        {
            return corrupt(format!(
                "MV maintenance observation current snapshot {snapshot_id} is not retained"
            ));
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

    /// Every snapshot the table still retains, including snapshots reachable
    /// only through a non-default reference.
    pub fn snapshots(&self) -> &[MvObservedSnapshot] {
        &self.snapshots
    }

    /// How many named references are not the provider's default one.
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

/// Observation port implemented by a composition-injected storage inspector.
///
/// The Core application loads metadata through the retained exact lease, then
/// gives the inspector that sealed metadata value.  The port deliberately has
/// no catalog entry, table handle downcast, or "current generation" lookup:
/// only the provider implementation may interpret its opaque table handle.
pub trait MvStorageObservationPort: Send + Sync {
    fn observe_created_target(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvTargetCreationObservation, ConnectorError>;

    fn observe_schema_validation(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvSchemaValidationObservation, ConnectorError>;

    fn observe_lake_package(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<Option<MvLakePackageObservation>, ConnectorError>;

    /// Observe a base table's UUID and current snapshot from one exact sealed
    /// metadata value.
    // Design: ADR-0060 (docs/adr/ADR-0060-exact-metadata-mv-refresh-base-pin.md)
    fn observe_refresh_base(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvRefreshBaseObservation, ConnectorError>;

    /// Observe the refresh-time facts of an MV target.
    ///
    /// Called on the same exact generation that loaded `metadata`, so the
    /// returned snapshot and ref identity are consistent with the Arrow schema
    /// and opaque handle the caller already holds.
    fn observe_refresh_target(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvRefreshTargetObservation, ConnectorError>;

    /// Observe the maintenance facts a provider can project from `metadata`
    /// alone.
    ///
    /// Scoped to pure metadata on purpose: it never triggers provider runtime
    /// IO, so a maintenance pass can gather these facts for every table at the
    /// cost of the metadata load it already performed.
    fn observe_maintenance_metadata(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<MvMaintenanceMetadataObservation, ConnectorError>;
}

/// Fail-closed default used until the Server composition root installs the
/// provider-specific storage inspector adapter.
#[derive(Clone, Copy, Debug, Default)]
pub struct UnavailableMvStorageObservationPort;

impl MvStorageObservationPort for UnavailableMvStorageObservationPort {
    fn observe_created_target(
        &self,
        _exact_lease: &ConnectorControlPlanningLease,
        _metadata: &ConnectorTableMetadata,
        _context: ConnectorRequestContext,
    ) -> Result<MvTargetCreationObservation, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "MV storage observation port is not installed",
        ))
    }

    fn observe_schema_validation(
        &self,
        _exact_lease: &ConnectorControlPlanningLease,
        _metadata: &ConnectorTableMetadata,
        _context: ConnectorRequestContext,
    ) -> Result<MvSchemaValidationObservation, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "MV schema validation observation port is not installed",
        ))
    }

    fn observe_lake_package(
        &self,
        _exact_lease: &ConnectorControlPlanningLease,
        _metadata: &ConnectorTableMetadata,
        _context: ConnectorRequestContext,
    ) -> Result<Option<MvLakePackageObservation>, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "MV storage observation port is not installed",
        ))
    }

    fn observe_refresh_base(
        &self,
        _exact_lease: &ConnectorControlPlanningLease,
        _metadata: &ConnectorTableMetadata,
        _context: ConnectorRequestContext,
    ) -> Result<MvRefreshBaseObservation, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "MV refresh base observation port is not installed",
        ))
    }

    fn observe_refresh_target(
        &self,
        _exact_lease: &ConnectorControlPlanningLease,
        _metadata: &ConnectorTableMetadata,
        _context: ConnectorRequestContext,
    ) -> Result<MvRefreshTargetObservation, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "MV refresh target observation port is not installed",
        ))
    }

    fn observe_maintenance_metadata(
        &self,
        _exact_lease: &ConnectorControlPlanningLease,
        _metadata: &ConnectorTableMetadata,
        _context: ConnectorRequestContext,
    ) -> Result<MvMaintenanceMetadataObservation, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "MV maintenance metadata observation port is not installed",
        ))
    }
}

/// Enumerate MV lake packages through durable attachment identities.
///
/// The application supplies the attachment-derived instance IDs. This helper
/// retains one exact generation while enumerating and loading every table,
/// then passes only the lease-loaded metadata to the injected observation
/// port. It never interprets an opaque provider handle.
pub fn discover_mv_lake_packages(
    controls: &dyn ConnectorControlResolver,
    instance_ids: impl IntoIterator<Item = ConnectorInstanceId>,
    observer: &dyn MvStorageObservationPort,
    context: ConnectorRequestContext,
) -> Result<Vec<MvLakePackageObservation>, ConnectorError> {
    validate_request_context(&context)?;
    let mut instance_ids = instance_ids.into_iter().collect::<Vec<_>>();
    instance_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    instance_ids.dedup();
    let mut budget = 0_usize;
    let mut packages = Vec::new();

    for instance_id in instance_ids {
        validate_request_context(&context)?;
        reserve_payload(&context, &mut budget, instance_id.as_str())?;
        let exact_lease = controls.acquire_current(&instance_id)?;
        if exact_lease.binding().descriptor().instance_id != instance_id {
            return corrupt("connector lease does not match MV discovery attachment identity");
        }
        let metadata = exact_lease.binding().metadata();
        let mut namespaces = metadata.list_namespaces(ConnectorListNamespacesRequest {
            instance_id: instance_id.clone(),
            context: context.clone(),
        })?;
        namespaces.sort_by(|left, right| left.namespace.cmp(&right.namespace));
        namespaces.dedup_by(|left, right| left.namespace == right.namespace);

        for namespace in namespaces {
            if namespace.instance_id != instance_id || namespace.namespace.trim().is_empty() {
                return corrupt("connector returned an invalid namespace during MV discovery");
            }
            if let Err(error) = normalize_identifier(namespace.namespace.as_ref()) {
                tracing::warn!(
                    instance = %instance_id.as_str(),
                    namespace = %namespace.namespace,
                    error,
                    "skip connector namespace outside the Native identifier contract during MV lake discovery"
                );
                continue;
            }
            reserve_payload(&context, &mut budget, namespace.namespace.as_ref())?;
            let mut tables = metadata.list_tables(ConnectorListTablesRequest {
                namespace: namespace.clone(),
                context: context.clone(),
            })?;
            tables.sort_by(|left, right| left.table.cmp(&right.table));
            tables.dedup_by(|left, right| left.table == right.table);

            for table in tables {
                validate_request_context(&context)?;
                if table.instance_id != instance_id
                    || table.namespace != namespace.namespace
                    || table.table.trim().is_empty()
                {
                    return corrupt("connector returned an invalid table during MV discovery");
                }
                reserve_payload(&context, &mut budget, table.table.as_ref())?;
                let loaded = match metadata.load_table(ConnectorTableRequest {
                    table: table.clone(),
                    resolution: ConnectorTableResolution::StrictBaseTable,
                    context: context.clone(),
                }) {
                    Ok(loaded) => loaded,
                    Err(error) => {
                        tracing::warn!(
                            instance = %instance_id.as_str(),
                            namespace = %table.namespace,
                            table = %table.table,
                            error = %error,
                            "skip unreadable connector table during MV lake discovery"
                        );
                        continue;
                    }
                };
                if loaded.identity != table {
                    return corrupt(
                        "connector loaded metadata for a different table during MV discovery",
                    );
                }
                if let Some(package) =
                    observer.observe_lake_package(&exact_lease, &loaded, context.clone())?
                {
                    if package.table != table {
                        return corrupt(format!(
                            "MV lake observer returned package metadata for `{}`.`{}`.`{}` while discovering `{}`.`{}`.`{}`",
                            package.table.instance_id.as_str(),
                            package.table.namespace,
                            package.table.table,
                            table.instance_id.as_str(),
                            table.namespace,
                            table.table,
                        ));
                    }
                    let expected_package_id = format!("{}.{}", table.namespace, table.table);
                    if package.descriptor.package_id != expected_package_id {
                        return corrupt(format!(
                            "descriptor package id mismatch for `{}`.`{}`.`{}`: expected `{expected_package_id}`, found `{}`",
                            table.instance_id.as_str(),
                            table.namespace,
                            table.table,
                            package.descriptor.package_id,
                        ));
                    }
                    packages.push(package);
                }
            }
        }
    }
    packages.sort_by(|left, right| {
        left.table
            .instance_id
            .as_str()
            .cmp(right.table.instance_id.as_str())
            .then(left.table.namespace.cmp(&right.table.namespace))
            .then(left.table.table.cmp(&right.table.table))
    });
    Ok(packages)
}

fn validate_request_context(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
    if context.cancellation().is_cancelled() {
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

fn reserve_payload(
    context: &ConnectorRequestContext,
    used: &mut usize,
    value: &str,
) -> Result<(), ConnectorError> {
    *used = used.checked_add(value.len()).ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "MV lake discovery payload accounting overflowed",
        )
    })?;
    if *used > context.max_total_payload_bytes() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "MV lake discovery names exceed the connector request payload budget",
        ));
    }
    Ok(())
}

fn validate_table_identity(
    table: &ConnectorTableIdentity,
    subject: &str,
) -> Result<(), ConnectorError> {
    if table.namespace.trim().is_empty() || table.table.trim().is_empty() {
        return corrupt(format!("{subject} has an empty table identity"));
    }
    Ok(())
}

fn validate_descriptor(descriptor: &MvDescriptorV1) -> Result<(), ConnectorError> {
    require_non_empty(&descriptor.package_id, "MV descriptor package ID")?;
    require_non_empty(&descriptor.logical_sql, "MV descriptor logical SQL")?;
    require_non_empty(&descriptor.dialect, "MV descriptor dialect")?;
    descriptor
        .to_canonical_json()
        .map_err(|err| ConnectorError::new(ConnectorErrorKind::CorruptData, err))?;
    descriptor
        .content_hash()
        .map_err(|err| ConnectorError::new(ConnectorErrorKind::CorruptData, err))?;
    Ok(())
}

fn validate_partition_contract(
    partition: &MvPartitionContract,
    fields: &[MvObservedTargetField],
) -> Result<(), ConnectorError> {
    let mut field_ids = HashSet::with_capacity(fields.len());
    for field in fields {
        field_ids.insert(field.field_id);
    }

    let mut partition_ids = HashSet::with_capacity(partition.fields.len());
    let mut partition_names = HashSet::with_capacity(partition.fields.len());
    for field in &partition.fields {
        require_non_empty(&field.partition_field_name, "MV partition field name")?;
        require_non_empty(&field.source_column_name, "MV partition source column name")?;
        if !partition_ids.insert(field.partition_field_id) {
            return corrupt(format!(
                "created MV target partition contract has duplicate partition field ID {}",
                field.partition_field_id
            ));
        }
        if !partition_names.insert(field.partition_field_name.as_str()) {
            return corrupt(format!(
                "created MV target partition contract has duplicate partition field name `{}`",
                field.partition_field_name
            ));
        }
        if !field_ids.contains(&field.source_target_field_id) {
            return corrupt(format!(
                "created MV target partition contract references missing target field ID {}",
                field.source_target_field_id
            ));
        }
        match &field.transform {
            crate::mv::persistence::schema::MvPartitionTransformContract::Bucket {
                num_buckets,
            } if *num_buckets == 0 => {
                return corrupt("created MV target partition contract has zero buckets");
            }
            crate::mv::persistence::schema::MvPartitionTransformContract::Truncate { width }
                if *width == 0 =>
            {
                return corrupt("created MV target partition contract has zero truncate width");
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_schema_validation_partition_contract(
    partition: &MvSchemaValidationPartitionContract,
    fields: &[MvObservedTargetField],
) -> Result<(), ConnectorError> {
    if partition.spec_id() < 0 {
        return corrupt("MV schema validation partition contract has a negative spec ID");
    }
    let field_ids = fields
        .iter()
        .map(MvObservedTargetField::field_id)
        .collect::<HashSet<_>>();
    let mut partition_ids = HashSet::with_capacity(partition.fields().len());
    let mut partition_names = HashSet::with_capacity(partition.fields().len());
    for field in partition.fields() {
        require_non_empty(
            field.partition_field_name(),
            "MV schema validation partition field name",
        )?;
        require_non_empty(
            field.source_column_name(),
            "MV schema validation partition source column name",
        )?;
        if !partition_ids.insert(field.partition_field_id()) {
            return corrupt(format!(
                "MV schema validation partition contract has duplicate field ID {}",
                field.partition_field_id()
            ));
        }
        if !partition_names.insert(field.partition_field_name().to_ascii_lowercase()) {
            return corrupt(format!(
                "MV schema validation partition contract has duplicate field name `{}`",
                field.partition_field_name()
            ));
        }
        if !field_ids.contains(&field.source_target_field_id()) {
            return corrupt(format!(
                "MV schema validation partition contract references missing target field ID {}",
                field.source_target_field_id()
            ));
        }
        match field.transform() {
            MvSchemaValidationPartitionTransform::Bucket { num_buckets: 0 } => {
                return corrupt(
                    "MV schema validation partition contract contains a zero bucket count",
                );
            }
            MvSchemaValidationPartitionTransform::Truncate { width: 0 } => {
                return corrupt(
                    "MV schema validation partition contract contains a zero truncate width",
                );
            }
            MvSchemaValidationPartitionTransform::Unsupported(name) => {
                require_non_empty(name, "MV schema validation unsupported partition transform")?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn require_non_empty(value: &str, subject: &str) -> Result<(), ConnectorError> {
    if value.trim().is_empty() {
        return corrupt(format!("{subject} is empty"));
    }
    Ok(())
}

fn reserve_schema_validation_payload(
    used: &mut usize,
    additional: usize,
    max_total_payload_bytes: usize,
) -> Result<(), ConnectorError> {
    *used = used.checked_add(additional).ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "MV schema validation payload accounting overflowed",
        )
    })?;
    if *used > max_total_payload_bytes {
        return exhausted("MV schema validation observation exceeds the payload limit");
    }
    Ok(())
}

fn reserve_maintenance_payload(
    used: &mut usize,
    additional: usize,
    max_total_payload_bytes: usize,
) -> Result<(), ConnectorError> {
    *used = used.checked_add(additional).ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "MV maintenance observation payload accounting overflowed",
        )
    })?;
    if *used > max_total_payload_bytes {
        return exhausted("MV maintenance observation exceeds the payload limit");
    }
    Ok(())
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

    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorInstanceId, ConnectorRequestContext, ConnectorTableIdentity,
    };

    use super::{
        BTreeMap, MvLakePackageObservation, MvLakePublication, MvMaintenanceMetadataObservation,
        MvObservedMaintenancePolicy, MvObservedSnapshot, MvObservedTargetField,
        MvPublishedBaseFact, MvPublishedLakeFacts, MvPublishedRefreshTechnique,
        MvRefreshBaseObservation, MvRefreshTargetObservation, MvSchemaValidationObservation,
        MvTargetCreationObservation,
    };
    use crate::mv::persistence::{
        descriptor::MvDescriptorV1,
        schema::{MvPartitionContract, MvPartitionFieldContract, MvPartitionTransformContract},
    };

    fn table() -> ConnectorTableIdentity {
        ConnectorTableIdentity {
            instance_id: ConnectorInstanceId::parse("iceberg.rest").unwrap(),
            namespace: Arc::from("db"),
            table: Arc::from("mv_target"),
        }
    }

    fn descriptor() -> MvDescriptorV1 {
        MvDescriptorV1 {
            descriptor_version: 1,
            package_id: "package-1".to_string(),
            logical_sql: "select 1".to_string(),
            dialect: "novarocks".to_string(),
            visible_columns: vec!["c1".to_string()],
            hidden_columns: vec![],
            base_dependencies: vec![],
            schema_contract: None,
            refresh_contract: None,
            created_at_ms: 1,
        }
    }

    fn target_fields() -> Vec<MvObservedTargetField> {
        vec![MvObservedTargetField {
            field_id: 1,
            name: "c1".to_string(),
            type_signature: "int".to_string(),
            nullable: false,
        }]
    }

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context(total_payload_bytes: usize) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(60),
            Arc::new(NeverCancelled),
            1,
            total_payload_bytes,
        )
        .unwrap()
    }

    fn published_facts() -> MvPublishedLakeFacts {
        MvPublishedLakeFacts::try_new(
            10,
            11,
            12,
            "refresh-token".to_string(),
            MvPublishedRefreshTechnique::Incremental,
            vec![MvPublishedBaseFact {
                table_fqn: "iceberg.db.base".to_string(),
                table_uuid: "base-uuid".to_string(),
                from_snapshot: Some(8),
                to_snapshot: 9,
            }],
            "definition-fingerprint".to_string(),
            1,
            "provenance-hash".to_string(),
            "waterline-hash".to_string(),
        )
        .unwrap()
    }

    #[test]
    fn target_observation_rejects_duplicate_schema_fields_and_invalid_partition_reference() {
        let mut duplicated = target_fields();
        duplicated.push(MvObservedTargetField {
            field_id: 1,
            name: "c2".to_string(),
            type_signature: "bigint".to_string(),
            nullable: true,
        });
        let err = MvTargetCreationObservation::try_new(
            table(),
            "target-uuid".to_string(),
            0,
            duplicated,
            MvPartitionContract {
                target_spec_id: 0,
                fields: vec![],
            },
        )
        .unwrap_err();
        assert_eq!(
            err.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );

        let err = MvTargetCreationObservation::try_new(
            table(),
            "target-uuid".to_string(),
            0,
            target_fields(),
            MvPartitionContract {
                target_spec_id: 0,
                fields: vec![MvPartitionFieldContract {
                    partition_field_id: 1000,
                    partition_field_name: "day_c1".to_string(),
                    source_target_field_id: 99,
                    source_column_name: "c1".to_string(),
                    transform: MvPartitionTransformContract::Day,
                }],
            },
        )
        .unwrap_err();
        assert_eq!(
            err.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn schema_validation_observation_is_bounded_and_exposes_only_neutral_facts() {
        let observed = MvSchemaValidationObservation::try_new(
            "target-uuid".to_string(),
            7,
            true,
            true,
            target_fields(),
            super::MvSchemaValidationPartitionContract::new(0, vec![]),
            &context(1_024),
        )
        .unwrap();
        assert_eq!(observed.table_uuid(), "target-uuid");
        assert_eq!(observed.schema_id(), 7);
        assert!(observed.is_format_v3());
        assert!(observed.stored_row_lineage_enabled());
        assert_eq!(observed.fields()[0].field_id(), 1);
        assert_eq!(observed.fields()[0].name(), "c1");
        assert_eq!(observed.fields()[0].type_signature(), "int");
        assert!(!observed.fields()[0].nullable());
        assert_eq!(observed.partition().spec_id(), 0);

        let payload_error = MvSchemaValidationObservation::try_new(
            "target-uuid".to_string(),
            7,
            true,
            true,
            target_fields(),
            super::MvSchemaValidationPartitionContract::new(0, vec![]),
            &context(1),
        )
        .unwrap_err();
        assert_eq!(
            payload_error.kind(),
            novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted
        );

        let duplicate_name_error = MvSchemaValidationObservation::try_new(
            "target-uuid".to_string(),
            7,
            true,
            true,
            vec![
                MvObservedTargetField::new(1, "c1".to_string(), "int".to_string(), false),
                MvObservedTargetField::new(2, "C1".to_string(), "long".to_string(), false),
            ],
            super::MvSchemaValidationPartitionContract::new(0, vec![]),
            &context(1_024),
        )
        .unwrap_err();
        assert_eq!(
            duplicate_name_error.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn published_facts_reject_duplicate_bases_negative_rows_and_missing_hashes() {
        let duplicate_base = MvPublishedLakeFacts::try_new(
            10,
            11,
            12,
            "refresh-token".to_string(),
            MvPublishedRefreshTechnique::Full,
            vec![
                MvPublishedBaseFact {
                    table_fqn: "iceberg.db.base".to_string(),
                    table_uuid: "base-uuid".to_string(),
                    from_snapshot: None,
                    to_snapshot: 9,
                },
                MvPublishedBaseFact {
                    table_fqn: "iceberg.db.base_renamed".to_string(),
                    table_uuid: "base-uuid".to_string(),
                    from_snapshot: None,
                    to_snapshot: 10,
                },
            ],
            "definition-fingerprint".to_string(),
            1,
            "provenance-hash".to_string(),
            "waterline-hash".to_string(),
        )
        .unwrap_err();
        assert_eq!(
            duplicate_base.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );

        let negative_rows = MvPublishedLakeFacts::try_new(
            10,
            11,
            12,
            "refresh-token".to_string(),
            MvPublishedRefreshTechnique::MetadataOnly,
            vec![],
            "definition-fingerprint".to_string(),
            -1,
            "provenance-hash".to_string(),
            "waterline-hash".to_string(),
        )
        .unwrap_err();
        assert_eq!(
            negative_rows.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );

        let missing_hash = MvPublishedLakeFacts::try_new(
            10,
            11,
            12,
            "refresh-token".to_string(),
            MvPublishedRefreshTechnique::MetadataOnly,
            vec![],
            "definition-fingerprint".to_string(),
            0,
            "".to_string(),
            "waterline-hash".to_string(),
        )
        .unwrap_err();
        assert_eq!(
            missing_hash.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn lake_package_accepts_never_published_and_validates_the_descriptor() {
        let observed = MvLakePackageObservation::try_new(
            table(),
            descriptor(),
            MvLakePublication::NeverPublished,
        )
        .unwrap();
        assert_eq!(observed.descriptor.package_id, "package-1");

        let observed = MvLakePackageObservation::try_new(
            table(),
            descriptor(),
            MvLakePublication::Published(published_facts()),
        )
        .unwrap();
        assert_eq!(observed.table.table.as_ref(), "mv_target");

        let mut invalid = descriptor();
        invalid.package_id.clear();
        let err =
            MvLakePackageObservation::try_new(table(), invalid, MvLakePublication::NeverPublished)
                .unwrap_err();
        assert_eq!(
            err.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );
    }

    fn partition() -> MvPartitionContract {
        MvPartitionContract {
            target_spec_id: 0,
            fields: vec![],
        }
    }

    fn refresh_target(
        current_snapshot_id: Option<i64>,
        refs: BTreeMap<String, i64>,
    ) -> Result<MvRefreshTargetObservation, novarocks_spi::connector::ConnectorError> {
        MvRefreshTargetObservation::try_new(
            table(),
            "uuid-1".to_string(),
            3,
            partition(),
            current_snapshot_id,
            refs,
            Vec::new(),
            Vec::new(),
            false,
            BTreeMap::new(),
            &context(4096),
        )
    }

    #[test]
    fn refresh_base_observation_requires_neutral_identity_uuid_and_snapshot() {
        let observed = MvRefreshBaseObservation::try_new(
            table(),
            "uuid-1".to_string(),
            Some(7),
            &context(4096),
        )
        .unwrap();
        assert_eq!(observed.table(), &table());
        assert_eq!(observed.table_uuid(), "uuid-1");
        assert_eq!(observed.current_snapshot_id(), Some(7));

        let empty_uuid =
            MvRefreshBaseObservation::try_new(table(), String::new(), None, &context(4096))
                .unwrap_err();
        assert_eq!(
            empty_uuid.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );

        let negative_snapshot = MvRefreshBaseObservation::try_new(
            table(),
            "uuid-1".to_string(),
            Some(-1),
            &context(4096),
        )
        .unwrap_err();
        assert_eq!(
            negative_snapshot.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn refresh_target_observation_rejects_corrupt_identity_and_snapshot_facts() {
        assert!(refresh_target(Some(7), BTreeMap::new()).is_ok());

        let err = MvRefreshTargetObservation::try_new(
            table(),
            String::new(),
            3,
            partition(),
            None,
            BTreeMap::new(),
            Vec::new(),
            Vec::new(),
            false,
            BTreeMap::new(),
            &context(4096),
        )
        .unwrap_err();
        assert_eq!(
            err.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );

        let err = MvRefreshTargetObservation::try_new(
            table(),
            "uuid-1".to_string(),
            -1,
            partition(),
            None,
            BTreeMap::new(),
            Vec::new(),
            Vec::new(),
            false,
            BTreeMap::new(),
            &context(4096),
        )
        .unwrap_err();
        assert_eq!(
            err.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );

        let err = refresh_target(Some(-5), BTreeMap::new()).unwrap_err();
        assert_eq!(
            err.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );

        let err =
            refresh_target(Some(7), BTreeMap::from([("staging".to_string(), -1)])).unwrap_err();
        assert_eq!(
            err.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );

        let err = refresh_target(Some(7), BTreeMap::from([(String::new(), 9)])).unwrap_err();
        assert_eq!(
            err.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn refresh_target_ref_lookup_falls_back_to_current_snapshot_only_for_main() {
        let observed =
            refresh_target(Some(7), BTreeMap::from([("mv_staging_42".to_string(), 11)])).unwrap();

        // An explicit ref wins.
        assert_eq!(observed.snapshot_id_for_ref("mv_staging_42"), Some(11));
        // `main` is not materialized as a ref here, so it resolves to current.
        assert_eq!(observed.snapshot_id_for_ref("main"), Some(7));
        // Any other missing ref stays absent rather than silently becoming main.
        assert_eq!(observed.snapshot_id_for_ref("mv_staging_99"), None);

        // A table with no current snapshot reports `main` as absent too.
        let empty = refresh_target(None, BTreeMap::new()).unwrap();
        assert_eq!(empty.snapshot_id_for_ref("main"), None);
    }

    fn maintenance_metadata(
        current_snapshot_id: Option<i64>,
        snapshots: Vec<MvObservedSnapshot>,
        payload_bytes: usize,
    ) -> Result<MvMaintenanceMetadataObservation, novarocks_spi::connector::ConnectorError> {
        MvMaintenanceMetadataObservation::try_new(
            current_snapshot_id,
            snapshots,
            2,
            Some(42),
            Some(7),
            Some(104_857_600),
            MvObservedMaintenancePolicy {
                maintenance_enabled: Some(false),
                expire_max_snapshot_age_ms: Some(900_000),
                expire_min_snapshots_to_keep: Some(0),
                target_file_size_bytes: None,
            },
            &context(payload_bytes),
        )
    }

    #[test]
    fn maintenance_observation_exposes_declared_facts_without_applying_policy_defaults() {
        let observed = maintenance_metadata(
            Some(11),
            vec![
                MvObservedSnapshot {
                    snapshot_id: 11,
                    timestamp_ms: 1_700_000_001_000,
                },
                MvObservedSnapshot {
                    snapshot_id: 22,
                    timestamp_ms: 1_700_000_002_000,
                },
            ],
            4_096,
        )
        .unwrap();

        assert_eq!(observed.current_snapshot_id(), Some(11));
        assert_eq!(observed.snapshots().len(), 2);
        assert_eq!(observed.snapshots()[1].snapshot_id, 22);
        assert_eq!(observed.snapshots()[1].timestamp_ms, 1_700_000_002_000);
        assert_eq!(observed.non_default_reference_count(), 2);
        assert_eq!(observed.total_data_files(), Some(42));
        assert_eq!(observed.total_delete_files(), Some(7));
        assert_eq!(observed.total_files_size_bytes(), Some(104_857_600));
        // Absent stays absent and a declared zero stays zero: substituting a
        // default here would erase the distinction the policy owner needs.
        assert_eq!(observed.policy().maintenance_enabled, Some(false));
        assert_eq!(observed.policy().expire_min_snapshots_to_keep, Some(0));
        assert_eq!(observed.policy().target_file_size_bytes, None);
    }

    #[test]
    fn maintenance_observation_rejects_corrupt_snapshot_identity_and_oversized_payloads() {
        let err = maintenance_metadata(Some(-1), Vec::new(), 4_096).unwrap_err();
        assert_eq!(
            err.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );

        let err = maintenance_metadata(
            None,
            vec![MvObservedSnapshot {
                snapshot_id: -3,
                timestamp_ms: 1,
            }],
            4_096,
        )
        .unwrap_err();
        assert_eq!(
            err.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );

        let err = maintenance_metadata(
            Some(11),
            vec![
                MvObservedSnapshot {
                    snapshot_id: 11,
                    timestamp_ms: 1,
                },
                MvObservedSnapshot {
                    snapshot_id: 11,
                    timestamp_ms: 2,
                },
            ],
            4_096,
        )
        .unwrap_err();
        assert_eq!(
            err.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );

        let err = maintenance_metadata(
            Some(11),
            vec![MvObservedSnapshot {
                snapshot_id: 11,
                timestamp_ms: 1,
            }],
            8,
        )
        .unwrap_err();
        assert_eq!(
            err.kind(),
            novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn maintenance_observation_rejects_a_current_snapshot_that_is_not_retained() {
        let err = maintenance_metadata(
            Some(12),
            vec![MvObservedSnapshot {
                snapshot_id: 11,
                timestamp_ms: 1,
            }],
            4_096,
        )
        .unwrap_err();
        assert_eq!(
            err.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );

        // A table that never published keeps no current snapshot; that is a
        // fact, not corruption.
        maintenance_metadata(None, Vec::new(), 4_096)
            .expect("an unpublished target observes no current snapshot");
    }
}
