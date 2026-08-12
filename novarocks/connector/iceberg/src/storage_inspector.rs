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

//! Server-composition-only projection of Iceberg MV storage facts.
//!
//! The inspector interprets an opaque table handle only while the caller
//! retains the exact control generation that loaded it. Its outputs contain
//! no Iceberg values, catalog clients, or application-owned MV types.

use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

use serde::Deserialize;

use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorError, ConnectorErrorKind, ConnectorRequestContext,
    ConnectorTableMetadata,
};

use crate::commit::mv_refresh_ref::{MV_ID_PROP, MV_REFRESH_ID_PROP, MV_REFRESH_TOKEN_PROP};
use crate::commit::{MvProvenanceV1, RefreshTechnique};
use crate::iceberg::spec::{FormatVersion, TableMetadata, Transform};
use crate::scan_model::IcebergTableInfo;

pub(crate) const MV_DESCRIPTOR_PACKAGE_ID_PROP: &str = "novarocks.mv.descriptor.package-id";
const MV_DESCRIPTOR_HASH_PROP: &str = "novarocks.mv.descriptor.hash";
const MV_DESCRIPTOR_INLINE_PROP: &str = "novarocks.mv.descriptor.inline";
const MAX_TARGET_FIELDS: usize = 4_096;
const MAX_PARTITION_FIELDS: usize = 4_096;
const MAX_TARGET_REFS: usize = 1_024;
const MAX_MAIN_ANCESTORS: usize = 100_000;
const MV_BOOTSTRAP_PROP: &str = "novarocks.mv.bootstrap";
const MV_BOOTSTRAP_OPERATION_ID_PROP: &str = "novarocks.bootstrap.empty.operation-id";
const MAX_PROVENANCE_BASES: usize = 16_384;
const MAX_MAINTENANCE_SNAPSHOTS: usize = 100_000;
/// Two `i64` values per projected snapshot, charged against the request budget.
const MAINTENANCE_SNAPSHOT_BYTES: usize = 16;

/// The branch every Iceberg table has by default. A ref with any other name is
/// a non-default reference as far as maintenance is concerned; this literal is
/// provider knowledge and never crosses the neutral observation boundary.
const DEFAULT_BRANCH_REF: &str = "main";

/// Iceberg snapshot-summary keys (string literals: the matching constants in
/// the vendored spec crate are private).
const TOTAL_DATA_FILES_SUMMARY_KEY: &str = "total-data-files";
const TOTAL_DELETE_FILES_SUMMARY_KEY: &str = "total-delete-files";
const TOTAL_FILES_SIZE_SUMMARY_KEY: &str = "total-files-size";

/// Table properties that declare maintenance policy values.
const MAINTENANCE_ENABLED_PROPERTY: &str = "novarocks.maintenance.enabled";
const EXPIRE_MAX_SNAPSHOT_AGE_PROPERTY: &str = "history.expire.max-snapshot-age-ms";
const EXPIRE_MIN_SNAPSHOTS_TO_KEEP_PROPERTY: &str = "history.expire.min-snapshots-to-keep";
const TARGET_FILE_SIZE_PROPERTY: &str = "write.target-file-size-bytes";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergStorageTargetObservation {
    pub table_uuid: String,
    pub schema_id: i32,
    pub format_v3: bool,
    pub explicit_row_lineage_enabled: bool,
    pub fields: Vec<IcebergStorageTargetField>,
    pub partition: IcebergStoragePartitionContract,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergStorageTargetField {
    pub field_id: i32,
    pub name: String,
    pub type_signature: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergStoragePartitionContract {
    pub target_spec_id: i32,
    pub fields: Vec<IcebergStoragePartitionField>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergStoragePartitionField {
    pub partition_field_id: i32,
    pub partition_field_name: String,
    pub source_target_field_id: i32,
    pub source_column_name: String,
    pub transform: IcebergStoragePartitionTransform,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IcebergStoragePartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket { num_buckets: u32 },
    Truncate { width: u32 },
    Void,
}

/// Narrow base-table facts projected from one frozen metadata document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergStorageRefreshBaseObservation {
    pub table_uuid: String,
    pub current_snapshot_id: Option<i64>,
}

/// Refresh-time facts of an MV target.
///
/// Distinct from [`IcebergStorageTargetObservation`]: apply needs snapshot and
/// ref identity but not the per-field schema payload. Storage layout facts the
/// physical writer needs — location, sequence numbers, partition spec objects —
/// stay inside this crate's write preparation and are deliberately absent here.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergStorageRefreshTargetObservation {
    pub table_uuid: String,
    pub schema_id: i32,
    pub partition: IcebergStoragePartitionContract,
    pub current_snapshot_id: Option<i64>,
    pub ref_snapshot_ids: BTreeMap<String, i64>,
    /// Target schema field IDs in schema order, positionally aligned with the
    /// Arrow schema the neutral metadata carries.
    pub field_ids: Vec<i32>,
    /// `main`'s snapshot chain, newest first. MV reconciliation classifies a
    /// staging snapshot by asking whether it is on this chain.
    pub main_ancestor_snapshot_ids: Vec<i64>,
    /// Is the current snapshot the empty bootstrap snapshot CREATE MV
    /// establishes before any refresh publishes data?
    pub current_snapshot_is_empty_bootstrap: bool,
    /// MV refresh marker carried by the current snapshot and by each ref tip,
    /// decoded from provider-private provenance. Snapshots without a marker are
    /// absent rather than present-and-empty.
    pub snapshot_markers: BTreeMap<i64, IcebergStorageRefreshMarker>,
}

/// The MV refresh identity a snapshot's provenance records.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergStorageRefreshMarker {
    pub refresh_id: i64,
    pub mv_id: i64,
    pub token: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergStorageLakePackageObservation {
    pub descriptor_properties: BTreeMap<String, String>,
    pub publication: IcebergStorageLakePublication,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IcebergStorageLakePublication {
    NeverPublished,
    Published(IcebergStoragePublishedFacts),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergStoragePublishedFacts {
    pub target_snapshot_id: i64,
    pub refresh_id: i64,
    pub mv_id: i64,
    pub token: String,
    pub technique: IcebergStorageRefreshTechnique,
    pub bases: Vec<IcebergStoragePublishedBaseFact>,
    pub definition_fingerprint: String,
    pub rows: i64,
    pub provenance_hash: String,
    pub waterline_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergStoragePublishedBaseFact {
    pub table_fqn: String,
    pub table_uuid: String,
    pub from_snapshot: Option<i64>,
    pub to_snapshot: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergStorageRefreshTechnique {
    Incremental,
    Full,
    MetadataOnly,
}

/// Pure `TableMetadata` projection of the facts MV maintenance policy needs.
///
/// Every value comes from the already-loaded metadata document. Facts that
/// require provider runtime IO — anything that has to read a manifest — are
/// deliberately absent: they carry a different cost profile and belong to a
/// separate observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergStorageMaintenanceMetadataObservation {
    pub current_snapshot_id: Option<i64>,
    /// Every snapshot the table still retains, sorted by snapshot ID.
    ///
    /// Projected from the snapshot set, never from the snapshot log: the log
    /// only records commits to the default branch, so a snapshot reachable
    /// only through another ref has a timestamp the log cannot report. A
    /// consumer that cannot resolve such a timestamp must treat retention as
    /// unresolved, so dropping those snapshots here would silently block it.
    pub snapshots: Vec<IcebergStorageSnapshotInfo>,
    /// Number of named refs other than the default branch.
    pub non_default_reference_count: usize,
    /// Current-snapshot summary counters. Absent when the table has no current
    /// snapshot, or the counter is missing or not a valid unsigned integer.
    pub total_data_files: Option<u64>,
    pub total_delete_files: Option<u64>,
    pub total_files_size_bytes: Option<u64>,
    /// Maintenance policy values exactly as the table declares them. Defaults
    /// and clamping belong to the policy owner, not to this projection.
    pub policy: IcebergStorageMaintenancePolicy,
}

/// One retained snapshot and the instant it was committed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IcebergStorageSnapshotInfo {
    pub snapshot_id: i64,
    pub timestamp_ms: i64,
}

/// Typed maintenance policy values declared by table properties.
///
/// Each value is absent when its property is missing or cannot be parsed.
/// This projection applies no default and no clamping: it reports only what
/// the table actually declares.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IcebergStorageMaintenancePolicy {
    /// `Some(false)` only when the property explicitly spells `false`
    /// (case-insensitively, ignoring surrounding whitespace); any other
    /// declared value is `Some(true)`.
    pub maintenance_enabled: Option<bool>,
    pub expire_max_snapshot_age_ms: Option<i64>,
    pub expire_min_snapshots_to_keep: Option<u32>,
    pub target_file_size_bytes: Option<i64>,
}

/// Stateless inspector installed only by the Server composition root.
#[derive(Clone, Copy, Debug, Default)]
pub struct IcebergStorageInspector;

impl IcebergStorageInspector {
    pub fn observe_created_target(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<IcebergStorageTargetObservation, ConnectorError> {
        let table = decoded_table(exact_lease, metadata, &context)?;
        target_observation(&table, &context)
    }

    pub fn observe_lake_package(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<Option<IcebergStorageLakePackageObservation>, ConnectorError> {
        let table = decoded_table(exact_lease, metadata, &context)?;
        lake_package_observation(&table, &context)
    }

    pub fn observe_refresh_base(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<IcebergStorageRefreshBaseObservation, ConnectorError> {
        let table = decoded_table(exact_lease, metadata, &context)?;
        refresh_base_observation(&table, &context)
    }

    pub fn observe_refresh_target(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<IcebergStorageRefreshTargetObservation, ConnectorError> {
        let table = decoded_table(exact_lease, metadata, &context)?;
        refresh_target_observation(&table, &context)
    }

    /// Project the maintenance facts carried by the table metadata itself.
    pub fn observe_maintenance_metadata(
        &self,
        exact_lease: &ConnectorControlPlanningLease,
        metadata: &ConnectorTableMetadata,
        context: ConnectorRequestContext,
    ) -> Result<IcebergStorageMaintenanceMetadataObservation, ConnectorError> {
        let table = decoded_table(exact_lease, metadata, &context)?;
        maintenance_metadata_observation(&table, &context)
    }
}

fn refresh_base_observation(
    table: &TableMetadata,
    context: &ConnectorRequestContext,
) -> Result<IcebergStorageRefreshBaseObservation, ConnectorError> {
    let table_uuid = table.uuid().to_string();
    let mut budget = 0_usize;
    reserve(context, &mut budget, &table_uuid)?;
    validate_context(context)?;
    Ok(IcebergStorageRefreshBaseObservation {
        table_uuid,
        current_snapshot_id: table.current_snapshot_id(),
    })
}

fn maintenance_metadata_observation(
    table: &TableMetadata,
    context: &ConnectorRequestContext,
) -> Result<IcebergStorageMaintenanceMetadataObservation, ConnectorError> {
    if table.snapshots().len() > MAX_MAINTENANCE_SNAPSHOTS {
        return Err(exhausted(
            "Iceberg MV maintenance metadata exceeds the inspection snapshot limit",
        ));
    }
    let mut budget = 0_usize;
    let mut snapshots = Vec::with_capacity(table.snapshots().len());
    // `snapshots()`, never `history()`: the snapshot log only records commits
    // to the default branch, so any snapshot reachable through another ref has
    // a real timestamp that the log simply does not carry.
    for snapshot in table.snapshots() {
        reserve_bytes(context, &mut budget, MAINTENANCE_SNAPSHOT_BYTES)?;
        snapshots.push(IcebergStorageSnapshotInfo {
            snapshot_id: snapshot.snapshot_id(),
            timestamp_ms: snapshot.timestamp_ms(),
        });
    }
    // The snapshot set is stored by ID, so iteration order is unspecified.
    // Sort for a deterministic observation; this ordering carries no retention
    // meaning of its own.
    snapshots.sort_by_key(|snapshot| snapshot.snapshot_id);

    let non_default_reference_count = table
        .refs()
        .keys()
        .filter(|name| name.as_str() != DEFAULT_BRANCH_REF)
        .count();

    let summary = table
        .current_snapshot()
        .map(|snapshot| &snapshot.summary().additional_properties);

    validate_context(context)?;
    Ok(IcebergStorageMaintenanceMetadataObservation {
        current_snapshot_id: table.current_snapshot_id(),
        snapshots,
        non_default_reference_count,
        total_data_files: summary
            .and_then(|summary| summary_u64(summary, TOTAL_DATA_FILES_SUMMARY_KEY)),
        total_delete_files: summary
            .and_then(|summary| summary_u64(summary, TOTAL_DELETE_FILES_SUMMARY_KEY)),
        total_files_size_bytes: summary
            .and_then(|summary| summary_u64(summary, TOTAL_FILES_SIZE_SUMMARY_KEY)),
        policy: maintenance_policy(table.properties()),
    })
}

fn summary_u64(summary: &HashMap<String, String>, key: &str) -> Option<u64> {
    summary
        .get(key)
        .and_then(|value| value.trim().parse::<u64>().ok())
}

fn maintenance_policy(properties: &HashMap<String, String>) -> IcebergStorageMaintenancePolicy {
    IcebergStorageMaintenancePolicy {
        maintenance_enabled: properties
            .get(MAINTENANCE_ENABLED_PROPERTY)
            .map(|value| !value.trim().eq_ignore_ascii_case("false")),
        expire_max_snapshot_age_ms: parsed_property(properties, EXPIRE_MAX_SNAPSHOT_AGE_PROPERTY),
        expire_min_snapshots_to_keep: parsed_property(
            properties,
            EXPIRE_MIN_SNAPSHOTS_TO_KEEP_PROPERTY,
        ),
        target_file_size_bytes: parsed_property(properties, TARGET_FILE_SIZE_PROPERTY),
    }
}

fn parsed_property<T: std::str::FromStr>(
    properties: &HashMap<String, String>,
    key: &str,
) -> Option<T> {
    properties
        .get(key)
        .and_then(|value| value.trim().parse::<T>().ok())
}

fn refresh_target_observation(
    table: &TableMetadata,
    context: &ConnectorRequestContext,
) -> Result<IcebergStorageRefreshTargetObservation, ConnectorError> {
    let schema = table.current_schema();
    let spec = table.default_partition_spec();
    if spec.fields().len() > MAX_PARTITION_FIELDS {
        return Err(exhausted(
            "Iceberg MV refresh target partition spec exceeds the inspection field limit",
        ));
    }
    let mut budget = 0_usize;
    let mut partition_fields = Vec::with_capacity(spec.fields().len());
    for field in spec.fields() {
        let source = schema.field_by_id(field.source_id).ok_or_else(|| {
            corrupt(format!(
                "Iceberg MV refresh target partition field {} references missing target field ID {}",
                field.name, field.source_id
            ))
        })?;
        reserve(context, &mut budget, &field.name)?;
        reserve(context, &mut budget, &source.name)?;
        partition_fields.push(IcebergStoragePartitionField {
            partition_field_id: field.field_id,
            partition_field_name: field.name.clone(),
            source_target_field_id: field.source_id,
            source_column_name: source.name.clone(),
            transform: partition_transform(&field.transform)?,
        });
    }

    let refs = table.refs();
    if refs.len() > MAX_TARGET_REFS {
        return Err(exhausted(
            "Iceberg MV refresh target exceeds the inspection ref limit",
        ));
    }
    let mut ref_snapshot_ids = BTreeMap::new();
    for (name, reference) in refs.iter() {
        reserve(context, &mut budget, name)?;
        ref_snapshot_ids.insert(name.clone(), reference.snapshot_id);
    }

    let current_snapshot_id = table
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id());

    // `main`'s parent chain, newest first. Bounded so a pathological history
    // cannot produce an unbounded observation.
    let mut main_ancestor_snapshot_ids = Vec::new();
    let mut cursor = current_snapshot_id;
    while let Some(snapshot_id) = cursor {
        if main_ancestor_snapshot_ids.len() >= MAX_MAIN_ANCESTORS {
            return Err(exhausted(
                "Iceberg MV refresh target snapshot history exceeds the inspection limit",
            ));
        }
        main_ancestor_snapshot_ids.push(snapshot_id);
        cursor = table
            .snapshot_by_id(snapshot_id)
            .and_then(|snapshot| snapshot.parent_snapshot_id());
    }

    // Markers for the snapshots MV reconciliation can reach: `main`'s current
    // snapshot and every ref tip. Core compares these identities against its own
    // ledger; it never parses the provider's provenance encoding.
    let mut snapshot_markers = BTreeMap::new();
    for snapshot_id in current_snapshot_id
        .into_iter()
        .chain(ref_snapshot_ids.values().copied())
    {
        if snapshot_markers.contains_key(&snapshot_id) {
            continue;
        }
        let Some(snapshot) = table.snapshot_by_id(snapshot_id) else {
            continue;
        };
        // The refresh marker is three discrete snapshot-summary properties, a
        // different encoding from the MvProvenanceV1 blob that publication facts
        // use. A staging snapshot carries the marker before it ever carries full
        // provenance, so decoding the wrong one silently loses the match.
        let props = &snapshot.summary().additional_properties;
        let (Some(refresh_id), Some(mv_id), Some(token)) = (
            props
                .get(MV_REFRESH_ID_PROP)
                .and_then(|value| value.parse::<i64>().ok()),
            props
                .get(MV_ID_PROP)
                .and_then(|value| value.parse::<i64>().ok()),
            props.get(MV_REFRESH_TOKEN_PROP),
        ) else {
            continue;
        };
        reserve(context, &mut budget, token)?;
        snapshot_markers.insert(
            snapshot_id,
            IcebergStorageRefreshMarker {
                refresh_id,
                mv_id,
                token: token.clone(),
            },
        );
    }

    let field_ids: Vec<i32> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.id)
        .collect();

    let current_snapshot_is_empty_bootstrap = table.current_snapshot().is_some_and(|snapshot| {
        let props = &snapshot.summary().additional_properties;
        snapshot.parent_snapshot_id().is_none()
            && props.get(MV_BOOTSTRAP_PROP).map(String::as_str) == Some("true")
            && props.contains_key(MV_BOOTSTRAP_OPERATION_ID_PROP)
    });

    let table_uuid = table.uuid().to_string();
    reserve(context, &mut budget, &table_uuid)?;
    validate_context(context)?;
    Ok(IcebergStorageRefreshTargetObservation {
        table_uuid,
        schema_id: table.current_schema_id(),
        partition: IcebergStoragePartitionContract {
            target_spec_id: spec.spec_id(),
            fields: partition_fields,
        },
        current_snapshot_id,
        ref_snapshot_ids,
        field_ids,
        main_ancestor_snapshot_ids,
        current_snapshot_is_empty_bootstrap,
        snapshot_markers,
    })
}

fn target_observation(
    table: &TableMetadata,
    context: &ConnectorRequestContext,
) -> Result<IcebergStorageTargetObservation, ConnectorError> {
    let schema = table.current_schema();
    if schema.as_struct().fields().len() > MAX_TARGET_FIELDS {
        return Err(exhausted(
            "Iceberg MV target schema exceeds the inspection field limit",
        ));
    }
    let mut budget = 0_usize;
    let fields = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| {
            reserve(context, &mut budget, &field.name)?;
            let type_signature = field.field_type.to_string();
            reserve(context, &mut budget, &type_signature)?;
            Ok(IcebergStorageTargetField {
                field_id: field.id,
                name: field.name.clone(),
                type_signature,
                nullable: !field.required,
            })
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    let spec = table.default_partition_spec();
    if spec.fields().len() > MAX_PARTITION_FIELDS {
        return Err(exhausted(
            "Iceberg MV target partition spec exceeds the inspection field limit",
        ));
    }
    let mut partition_fields = Vec::with_capacity(spec.fields().len());
    for field in spec.fields() {
        let source = schema.field_by_id(field.source_id).ok_or_else(|| {
            corrupt(format!(
                "Iceberg MV target partition field {} references missing target field ID {}",
                field.name, field.source_id
            ))
        })?;
        reserve(context, &mut budget, &field.name)?;
        reserve(context, &mut budget, &source.name)?;
        partition_fields.push(IcebergStoragePartitionField {
            partition_field_id: field.field_id,
            partition_field_name: field.name.clone(),
            source_target_field_id: field.source_id,
            source_column_name: source.name.clone(),
            transform: partition_transform(&field.transform)?,
        });
    }
    let table_uuid = table.uuid().to_string();
    reserve(context, &mut budget, &table_uuid)?;
    validate_context(context)?;
    Ok(IcebergStorageTargetObservation {
        table_uuid,
        schema_id: table.current_schema_id(),
        format_v3: matches!(table.format_version(), FormatVersion::V3),
        explicit_row_lineage_enabled: table
            .properties()
            .get("write.row-lineage")
            .is_some_and(|value| value.eq_ignore_ascii_case("true")),
        fields,
        partition: IcebergStoragePartitionContract {
            target_spec_id: spec.spec_id(),
            fields: partition_fields,
        },
    })
}

fn lake_package_observation(
    table: &TableMetadata,
    context: &ConnectorRequestContext,
) -> Result<Option<IcebergStorageLakePackageObservation>, ConnectorError> {
    let properties = table.properties();
    if !properties.contains_key(MV_DESCRIPTOR_PACKAGE_ID_PROP) {
        return Ok(None);
    }
    let mut budget = 0_usize;
    let mut descriptor_properties = BTreeMap::new();
    for key in [
        MV_DESCRIPTOR_PACKAGE_ID_PROP,
        MV_DESCRIPTOR_HASH_PROP,
        MV_DESCRIPTOR_INLINE_PROP,
    ] {
        if let Some(value) = properties.get(key) {
            reserve(context, &mut budget, key)?;
            reserve(context, &mut budget, value)?;
            descriptor_properties.insert(key.to_string(), value.clone());
        }
    }
    if !descriptor_properties.contains_key(MV_DESCRIPTOR_INLINE_PROP) {
        return Err(corrupt(
            "Iceberg MV package is missing its inline descriptor property",
        ));
    }
    let publication = match table.current_snapshot() {
        None => IcebergStorageLakePublication::NeverPublished,
        Some(snapshot) => match MvProvenanceV1::from_snapshot_summary(snapshot).map_err(corrupt)? {
            None => IcebergStorageLakePublication::NeverPublished,
            Some(provenance) => IcebergStorageLakePublication::Published(published_facts(
                snapshot.snapshot_id(),
                provenance,
                context,
                &mut budget,
            )?),
        },
    };
    validate_context(context)?;
    Ok(Some(IcebergStorageLakePackageObservation {
        descriptor_properties,
        publication,
    }))
}

#[derive(Deserialize)]
struct TableHandlePayload {
    namespace: String,
    table: String,
    table_info: Option<IcebergTableInfo>,
}

fn decoded_table(
    exact_lease: &ConnectorControlPlanningLease,
    metadata: &ConnectorTableMetadata,
    context: &ConnectorRequestContext,
) -> Result<TableMetadata, ConnectorError> {
    validate_context(context)?;
    if exact_lease.binding().descriptor().instance_id != metadata.identity.instance_id
        || metadata.table.owner() != &metadata.identity.instance_id
    {
        return Err(invalid(
            "Iceberg storage inspection metadata does not belong to the retained generation",
        ));
    }
    let payload: TableHandlePayload =
        serde_json::from_slice(metadata.table.payload()).map_err(|error| {
            corrupt(format!(
                "decode Iceberg table handle for storage inspection: {error}"
            ))
        })?;
    if payload.namespace != metadata.identity.namespace.as_ref()
        || payload.table != metadata.identity.table.as_ref()
    {
        return Err(corrupt(
            "Iceberg storage inspection table handle identity does not match loaded metadata",
        ));
    }
    let table_info = payload
        .table_info
        .ok_or_else(|| corrupt("Iceberg storage inspection handle has no frozen table metadata"))?;
    if table_info.namespace != payload.namespace || table_info.table != payload.table {
        return Err(corrupt(
            "Iceberg storage inspection frozen table identity is inconsistent",
        ));
    }
    let serialized = table_info.serialized_metadata.ok_or_else(|| {
        corrupt("Iceberg storage inspection handle has no serialized table metadata")
    })?;
    if serialized.len() > context.max_total_payload_bytes() {
        return Err(exhausted(
            "Iceberg storage inspection metadata exceeds the request payload budget",
        ));
    }
    serde_json::from_str(&serialized).map_err(|error| {
        corrupt(format!(
            "decode Iceberg storage inspection metadata: {error}"
        ))
    })
}

fn published_facts(
    target_snapshot_id: i64,
    provenance: MvProvenanceV1,
    context: &ConnectorRequestContext,
    budget: &mut usize,
) -> Result<IcebergStoragePublishedFacts, ConnectorError> {
    if provenance.bases.len() > MAX_PROVENANCE_BASES {
        return Err(exhausted(
            "Iceberg MV provenance exceeds the inspection base limit",
        ));
    }
    reserve(context, budget, &provenance.token)?;
    reserve(context, budget, &provenance.definition_fingerprint)?;
    let provenance_hash = provenance.content_hash().map_err(corrupt)?;
    let waterline_hash = provenance.waterline_hash().map_err(corrupt)?;
    reserve(context, budget, &provenance_hash)?;
    reserve(context, budget, &waterline_hash)?;
    let bases = provenance
        .bases
        .iter()
        .map(|base| {
            reserve(context, budget, &base.table_fqn)?;
            reserve(context, budget, &base.uuid)?;
            Ok(IcebergStoragePublishedBaseFact {
                table_fqn: base.table_fqn.clone(),
                table_uuid: base.uuid.clone(),
                from_snapshot: base.from_snapshot,
                to_snapshot: base.to_snapshot,
            })
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    Ok(IcebergStoragePublishedFacts {
        target_snapshot_id,
        refresh_id: provenance.refresh_id,
        mv_id: provenance.mv_id,
        token: provenance.token,
        technique: match provenance.technique {
            RefreshTechnique::Incremental => IcebergStorageRefreshTechnique::Incremental,
            RefreshTechnique::Full => IcebergStorageRefreshTechnique::Full,
            RefreshTechnique::MetadataOnly => IcebergStorageRefreshTechnique::MetadataOnly,
        },
        bases,
        definition_fingerprint: provenance.definition_fingerprint,
        rows: provenance.rows,
        provenance_hash,
        waterline_hash,
    })
}

fn partition_transform(
    transform: &Transform,
) -> Result<IcebergStoragePartitionTransform, ConnectorError> {
    match transform {
        Transform::Identity => Ok(IcebergStoragePartitionTransform::Identity),
        Transform::Year => Ok(IcebergStoragePartitionTransform::Year),
        Transform::Month => Ok(IcebergStoragePartitionTransform::Month),
        Transform::Day => Ok(IcebergStoragePartitionTransform::Day),
        Transform::Hour => Ok(IcebergStoragePartitionTransform::Hour),
        Transform::Bucket(num_buckets) => Ok(IcebergStoragePartitionTransform::Bucket {
            num_buckets: *num_buckets,
        }),
        Transform::Truncate(width) => {
            Ok(IcebergStoragePartitionTransform::Truncate { width: *width })
        }
        Transform::Void => Ok(IcebergStoragePartitionTransform::Void),
        Transform::Unknown => Err(corrupt(
            "Iceberg storage inspection cannot project an unknown partition transform",
        )),
    }
}

fn validate_context(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
    if context.cancellation().is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "Iceberg storage inspection request was cancelled",
        ));
    }
    if Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "Iceberg storage inspection request deadline elapsed",
        ));
    }
    Ok(())
}

fn reserve(
    context: &ConnectorRequestContext,
    budget: &mut usize,
    value: &str,
) -> Result<(), ConnectorError> {
    reserve_bytes(context, budget, value.len())
}

fn reserve_bytes(
    context: &ConnectorRequestContext,
    budget: &mut usize,
    additional: usize,
) -> Result<(), ConnectorError> {
    *budget = budget
        .checked_add(additional)
        .ok_or_else(|| exhausted("Iceberg storage inspection payload accounting overflowed"))?;
    if *budget > context.max_total_payload_bytes() {
        return Err(exhausted(
            "Iceberg storage inspection facts exceed the request payload budget",
        ));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

fn exhausted(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use novarocks_spi::connector::{ConnectorCancellation, ConnectorErrorKind};

    use crate::iceberg::spec::{
        FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
        SnapshotReference, SnapshotRetention, SortOrder, Summary, TableMetadataBuilder, Type,
    };

    use super::*;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context(max_total_payload_bytes: usize) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(10),
            Arc::new(NeverCancelled),
            max_total_payload_bytes.min(1024),
            max_total_payload_bytes,
        )
        .expect("context")
    }

    fn metadata(properties: HashMap<String, String>) -> TableMetadata {
        metadata_with_format(FormatVersion::V2, properties)
    }

    fn metadata_with_format(
        format_version: FormatVersion,
        properties: HashMap<String, String>,
    ) -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("schema");
        TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///storage-inspector-test".to_string(),
            format_version,
            properties,
        )
        .expect("metadata builder")
        .build()
        .expect("metadata")
        .metadata
    }

    #[test]
    fn target_projection_preserves_field_identity_and_nullability() {
        let observed = target_observation(&metadata(HashMap::new()), &context(4096))
            .expect("target observation");
        assert_eq!(observed.fields.len(), 2);
        assert_eq!(observed.fields[0].field_id, 1);
        assert_eq!(observed.fields[0].name, "id");
        assert!(!observed.fields[0].nullable);
        assert_eq!(observed.fields[1].name, "name");
        assert!(observed.fields[1].nullable);
        assert!(observed.partition.fields.is_empty());
        assert!(!observed.format_v3);
        assert!(!observed.explicit_row_lineage_enabled);
    }

    #[test]
    fn schema_validation_projection_requires_explicit_row_lineage_property() {
        let implicit = target_observation(
            &metadata_with_format(FormatVersion::V3, HashMap::new()),
            &context(4096),
        )
        .expect("implicit row lineage observation");
        assert!(implicit.format_v3);
        assert!(!implicit.explicit_row_lineage_enabled);

        let explicit = target_observation(
            &metadata_with_format(
                FormatVersion::V3,
                HashMap::from([("write.row-lineage".to_string(), "TRUE".to_string())]),
            ),
            &context(4096),
        )
        .expect("explicit row lineage observation");
        assert!(explicit.explicit_row_lineage_enabled);
    }

    #[test]
    fn lake_projection_is_absent_for_ordinary_table() {
        assert_eq!(
            lake_package_observation(&metadata(HashMap::new()), &context(4096))
                .expect("lake observation"),
            None
        );
    }

    #[test]
    fn lake_projection_requires_inline_descriptor() {
        let error = lake_package_observation(
            &metadata(HashMap::from([(
                MV_DESCRIPTOR_PACKAGE_ID_PROP.to_string(),
                "analytics.mv_orders".to_string(),
            )])),
            &context(4096),
        )
        .expect_err("missing inline descriptor");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn lake_projection_is_bounded_by_request_context() {
        let error = lake_package_observation(
            &metadata(HashMap::from([
                (
                    MV_DESCRIPTOR_PACKAGE_ID_PROP.to_string(),
                    "analytics.mv_orders".to_string(),
                ),
                (MV_DESCRIPTOR_INLINE_PROP.to_string(), "x".repeat(1024)),
            ])),
            &context(64),
        )
        .expect_err("payload limit");
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
    }

    #[test]
    fn unknown_partition_transform_fails_closed() {
        let error = partition_transform(&Transform::Unknown).expect_err("unknown transform");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn refresh_base_projection_keeps_uuid_and_snapshot_from_one_metadata_value() {
        let table = maintenance_metadata(
            vec![SnapshotFixture::new(11, 1_700_000_001_000).on_main()],
            HashMap::new(),
        );
        let observed =
            refresh_base_observation(&table, &context(4096)).expect("refresh base observation");
        assert_eq!(observed.table_uuid, table.uuid().to_string());
        assert_eq!(observed.current_snapshot_id, Some(11));
    }

    /// One snapshot to install, plus the ref (if any) that should point at it.
    ///
    /// A ref named `main` is what puts a snapshot into the snapshot log; every
    /// other ref leaves the snapshot reachable but unlogged.
    struct SnapshotFixture {
        snapshot_id: i64,
        timestamp_ms: i64,
        summary: Vec<(&'static str, &'static str)>,
        reference: Option<(&'static str, SnapshotRetention)>,
    }

    impl SnapshotFixture {
        fn new(snapshot_id: i64, timestamp_ms: i64) -> Self {
            Self {
                snapshot_id,
                timestamp_ms,
                summary: Vec::new(),
                reference: None,
            }
        }

        fn with_summary(mut self, summary: Vec<(&'static str, &'static str)>) -> Self {
            self.summary = summary;
            self
        }

        fn with_reference(mut self, name: &'static str, retention: SnapshotRetention) -> Self {
            self.reference = Some((name, retention));
            self
        }

        fn on_main(self) -> Self {
            self.with_reference("main", SnapshotRetention::branch(None, None, None))
        }
    }

    fn maintenance_metadata(
        snapshots: Vec<SnapshotFixture>,
        properties: HashMap<String, String>,
    ) -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema");
        let mut builder = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "file:///maintenance-inspector-test".to_string(),
            FormatVersion::V2,
            properties,
        )
        .expect("metadata builder");

        for (index, fixture) in snapshots.into_iter().enumerate() {
            let snapshot = Snapshot::builder()
                .with_snapshot_id(fixture.snapshot_id)
                .with_parent_snapshot_id(None)
                .with_sequence_number(index as i64 + 1)
                .with_timestamp_ms(fixture.timestamp_ms)
                .with_manifest_list(format!(
                    "file:///maintenance-inspector-test/metadata/snap-{}.avro",
                    fixture.snapshot_id
                ))
                .with_summary(Summary {
                    operation: Operation::Append,
                    additional_properties: fixture
                        .summary
                        .into_iter()
                        .map(|(key, value)| (key.to_string(), value.to_string()))
                        .collect(),
                })
                .build();
            builder = builder.add_snapshot(snapshot).expect("add snapshot");
            if let Some((name, retention)) = fixture.reference {
                builder = builder
                    .set_ref(name, SnapshotReference::new(fixture.snapshot_id, retention))
                    .expect("set ref");
            }
        }

        builder.build().expect("build").metadata
    }

    /// The regression this projection exists to prevent: the snapshot log only
    /// records the default branch, so projecting it would drop the timestamp of
    /// every snapshot reachable only through another ref.
    #[test]
    fn maintenance_projection_reports_snapshots_missing_from_the_snapshot_log() {
        let table = maintenance_metadata(
            vec![
                SnapshotFixture::new(11, 1_700_000_001_000).on_main(),
                SnapshotFixture::new(22, 1_700_000_002_000)
                    .with_reference("audit", SnapshotRetention::branch(None, None, None)),
                SnapshotFixture::new(33, 1_700_000_003_000).with_reference(
                    "release",
                    SnapshotRetention::Tag {
                        max_ref_age_ms: None,
                    },
                ),
            ],
            HashMap::new(),
        );

        // The fixture really does exercise the gap: the log knows only `main`.
        let logged: Vec<i64> = table
            .history()
            .iter()
            .map(|entry| entry.snapshot_id)
            .collect();
        assert_eq!(logged, vec![11]);

        let observed =
            maintenance_metadata_observation(&table, &context(4096)).expect("maintenance");
        assert_eq!(
            observed.snapshots,
            vec![
                IcebergStorageSnapshotInfo {
                    snapshot_id: 11,
                    timestamp_ms: 1_700_000_001_000,
                },
                IcebergStorageSnapshotInfo {
                    snapshot_id: 22,
                    timestamp_ms: 1_700_000_002_000,
                },
                IcebergStorageSnapshotInfo {
                    snapshot_id: 33,
                    timestamp_ms: 1_700_000_003_000,
                },
            ]
        );
        assert_eq!(observed.current_snapshot_id, Some(11));
    }

    #[test]
    fn maintenance_projection_counts_only_non_default_references() {
        let default_only = maintenance_metadata(
            vec![SnapshotFixture::new(11, 1_700_000_001_000).on_main()],
            HashMap::new(),
        );
        let observed =
            maintenance_metadata_observation(&default_only, &context(4096)).expect("maintenance");
        assert_eq!(observed.non_default_reference_count, 0);

        let with_extra_refs = maintenance_metadata(
            vec![
                SnapshotFixture::new(11, 1_700_000_001_000).on_main(),
                SnapshotFixture::new(22, 1_700_000_002_000)
                    .with_reference("audit", SnapshotRetention::branch(None, None, None)),
                SnapshotFixture::new(33, 1_700_000_003_000).with_reference(
                    "release",
                    SnapshotRetention::Tag {
                        max_ref_age_ms: None,
                    },
                ),
            ],
            HashMap::new(),
        );
        let observed = maintenance_metadata_observation(&with_extra_refs, &context(4096))
            .expect("maintenance");
        assert_eq!(observed.non_default_reference_count, 2);
    }

    #[test]
    fn maintenance_projection_has_no_summary_counters_without_a_current_snapshot() {
        let table = maintenance_metadata(Vec::new(), HashMap::new());
        let observed =
            maintenance_metadata_observation(&table, &context(4096)).expect("maintenance");
        assert_eq!(observed.current_snapshot_id, None);
        assert!(observed.snapshots.is_empty());
        assert_eq!(observed.non_default_reference_count, 0);
        assert_eq!(observed.total_data_files, None);
        assert_eq!(observed.total_delete_files, None);
        assert_eq!(observed.total_files_size_bytes, None);
    }

    #[test]
    fn maintenance_projection_reads_summary_counters_and_ignores_unparsable_values() {
        let missing = maintenance_metadata(
            vec![SnapshotFixture::new(11, 1_700_000_001_000).on_main()],
            HashMap::new(),
        );
        let observed =
            maintenance_metadata_observation(&missing, &context(4096)).expect("maintenance");
        assert_eq!(observed.total_data_files, None);
        assert_eq!(observed.total_delete_files, None);
        assert_eq!(observed.total_files_size_bytes, None);

        let unparsable = maintenance_metadata(
            vec![
                SnapshotFixture::new(11, 1_700_000_001_000)
                    .with_summary(vec![
                        ("total-data-files", "many"),
                        ("total-delete-files", "-1"),
                        ("total-files-size", ""),
                    ])
                    .on_main(),
            ],
            HashMap::new(),
        );
        let observed =
            maintenance_metadata_observation(&unparsable, &context(4096)).expect("maintenance");
        assert_eq!(observed.total_data_files, None);
        assert_eq!(observed.total_delete_files, None);
        assert_eq!(observed.total_files_size_bytes, None);

        let valid = maintenance_metadata(
            vec![
                SnapshotFixture::new(11, 1_700_000_001_000)
                    .with_summary(vec![
                        ("total-data-files", "42"),
                        ("total-delete-files", " 7 "),
                        ("total-files-size", "104857600"),
                    ])
                    .on_main(),
            ],
            HashMap::new(),
        );
        let observed =
            maintenance_metadata_observation(&valid, &context(4096)).expect("maintenance");
        assert_eq!(observed.total_data_files, Some(42));
        assert_eq!(observed.total_delete_files, Some(7));
        assert_eq!(observed.total_files_size_bytes, Some(104_857_600));
    }

    fn observed_policy(properties: Vec<(&str, &str)>) -> IcebergStorageMaintenancePolicy {
        let table = maintenance_metadata(
            vec![SnapshotFixture::new(11, 1_700_000_001_000).on_main()],
            properties
                .into_iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        );
        maintenance_metadata_observation(&table, &context(4096))
            .expect("maintenance")
            .policy
    }

    #[test]
    fn maintenance_policy_projection_distinguishes_absent_unparsable_and_declared_values() {
        assert_eq!(
            observed_policy(Vec::new()),
            IcebergStorageMaintenancePolicy::default()
        );

        let unparsable = observed_policy(vec![
            ("history.expire.max-snapshot-age-ms", "soon"),
            ("history.expire.min-snapshots-to-keep", "-1"),
            ("write.target-file-size-bytes", ""),
        ]);
        assert_eq!(unparsable.expire_max_snapshot_age_ms, None);
        assert_eq!(unparsable.expire_min_snapshots_to_keep, None);
        assert_eq!(unparsable.target_file_size_bytes, None);

        let declared = observed_policy(vec![
            ("history.expire.max-snapshot-age-ms", " 900000 "),
            ("history.expire.min-snapshots-to-keep", "3"),
            ("write.target-file-size-bytes", "268435456"),
        ]);
        assert_eq!(declared.expire_max_snapshot_age_ms, Some(900_000));
        assert_eq!(declared.expire_min_snapshots_to_keep, Some(3));
        assert_eq!(declared.target_file_size_bytes, Some(268_435_456));

        // The projection reports what the table declares. Clamping a zero up to
        // a usable minimum is the policy owner's job, not the observer's.
        let zeroed = observed_policy(vec![
            ("history.expire.max-snapshot-age-ms", "0"),
            ("history.expire.min-snapshots-to-keep", "0"),
            ("write.target-file-size-bytes", "0"),
        ]);
        assert_eq!(zeroed.expire_max_snapshot_age_ms, Some(0));
        assert_eq!(zeroed.expire_min_snapshots_to_keep, Some(0));
        assert_eq!(zeroed.target_file_size_bytes, Some(0));
    }

    #[test]
    fn maintenance_enabled_projection_treats_only_an_explicit_false_as_disabled() {
        assert_eq!(observed_policy(Vec::new()).maintenance_enabled, None);
        for disabled in ["false", "FALSE", " false ", "\tFalse\n"] {
            assert_eq!(
                observed_policy(vec![("novarocks.maintenance.enabled", disabled)])
                    .maintenance_enabled,
                Some(false),
                "`{disabled}` must read as disabled"
            );
        }
        for enabled in ["true", "TRUE", "", "no", "0"] {
            assert_eq!(
                observed_policy(vec![("novarocks.maintenance.enabled", enabled)])
                    .maintenance_enabled,
                Some(true),
                "`{enabled}` must read as enabled"
            );
        }
    }
}
