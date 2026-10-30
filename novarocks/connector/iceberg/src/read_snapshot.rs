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

//! One provider-owned manifest observation and compact delete closures.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use crate::delete_semantics::{
    DataFileFact, DeleteCandidateIndex, DeleteContentAddress, DeleteKind, DeleteObservation,
    DeleteReadFacts, EntrySequence, EqualityFieldGroup, FieldMetrics, FileMetrics, LoadView,
    ManifestDeleteObservation, POSITION_FILE_PATH_FIELD_ID, PinnedEndpointFacts, RawDeleteEntry,
    RawDeleteFile, ReadDomain, ReadObservationId, StatisticsPolicy, TypedPartition,
};
use crate::iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, Literal, ManifestFile, ManifestStatus, NestedField,
    PrimitiveLiteral, PrimitiveType, Schema, Struct, StructType, TableMetadata, Type,
};
use crate::iceberg::table::Table;
use crate::read_model::{
    IcebergDataFileMetadata, IcebergReadFile, IcebergReadSnapshot, iceberg_partition_key,
};
use crate::scan_model::IcebergColumnStats;

// Small metadata-only pruning work is bounded independently of the full suffix.
const MAX_STATS_DELETE_MEMBERS: usize = 256;
const MAX_STATS_FIELD_COMPARISONS: usize = 1024;
use sha2::{Digest, Sha256};
pub(crate) mod cow_capture;
use cow_capture::{CaptureScope, ReadFailure, active as check_cow_active};

/// Mint exactly once at the provider's pinned handle boundary. Standalone
/// snapshot callers intentionally create an independent observation.
pub(crate) fn mint_read_domain(
    metadata: &TableMetadata,
    snapshot_id: i64,
    schema: &Schema,
) -> Result<Arc<ReadDomain>, String> {
    let specs = metadata
        .partition_specs_iter()
        .map(|s| s.as_ref().clone())
        .collect::<Vec<_>>();
    let partition_types = frozen_partition_types(metadata, schema)?;
    let endpoint = PinnedEndpointFacts::try_new_with_partition_types(
        metadata.uuid(),
        metadata_identity(metadata)?,
        snapshot_id,
        schema,
        &specs,
        &partition_types,
    )
    .map_err(|e| e.to_string())?;
    let observation =
        ReadObservationId::try_new(*uuid::Uuid::now_v7().as_bytes()).map_err(|e| e.to_string())?;
    Ok(Arc::new(ReadDomain::new(observation, endpoint)))
}

/// Resolve storage partition types once from the pinned field-ID history.
/// A missing projected source does not erase its historical storage facts.
fn frozen_partition_types(
    metadata: &TableMetadata,
    query: &Schema,
) -> Result<BTreeMap<i32, StructType>, String> {
    let mut sources = BTreeMap::<i32, (PrimitiveType, PrimitiveType)>::new();
    for spec in metadata.partition_specs_iter() {
        for field in spec.fields() {
            if sources.contains_key(&field.source_id) {
                continue;
            }
            let types = partition_source_types(metadata, query, field.source_id)?;
            sources.insert(field.source_id, types);
        }
    }
    metadata
        .partition_specs_iter()
        .map(|spec| {
            let fields = spec
                .fields()
                .iter()
                .map(|field| {
                    let (query_source, storage_proof) = &sources[&field.source_id];
                    // The query may predate a transform introduced by a later
                    // spec (for example hour after Date -> Timestamp). Prove
                    // that spec's storage type from the retained legal chain;
                    // do not force its unrelated query projection to change.
                    let ty = match field
                        .transform
                        .result_type(&Type::Primitive(query_source.clone()))
                    {
                        Ok(ty) => ty,
                        Err(_) => field
                            .transform
                            .result_type(&Type::Primitive(storage_proof.clone()))
                            .map_err(|e| e.to_string())?,
                    };
                    Ok(Arc::new(NestedField::optional(
                        field.field_id,
                        field.name.clone(),
                        ty,
                    )))
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok((spec.spec_id(), StructType::new(fields)))
        })
        .collect()
}

/// A historical type failure never replaces its creating caller's control cause.
#[derive(Debug)]
pub(crate) enum PartitionSourceTypeFailure<E> {
    Semantic(String),
    Original(E),
}
impl<E> From<String> for PartitionSourceTypeFailure<E> {
    fn from(message: String) -> Self {
        Self::Semantic(message)
    }
}

pub(crate) fn partition_source_types(
    metadata: &TableMetadata,
    query: &Schema,
    source_id: i32,
) -> Result<(PrimitiveType, PrimitiveType), String> {
    partition_source_types_checked(metadata, query, source_id, || {
        Ok::<(), std::convert::Infallible>(())
    })
    .map_err(|cause| match cause {
        PartitionSourceTypeFailure::Semantic(message) => message,
        PartitionSourceTypeFailure::Original(never) => match never {},
    })
}

/// One exact retained-type policy shared by read domains and metadata authors.
/// Primitive declarations own no variable-size backing; each schema lookup is
/// observed before entry and caller failures retain their original identity.
pub(crate) fn partition_source_types_checked<E>(
    metadata: &TableMetadata,
    query: &Schema,
    source_id: i32,
    mut active: impl FnMut() -> Result<(), E>,
) -> Result<(PrimitiveType, PrimitiveType), PartitionSourceTypeFailure<E>> {
    let mut historical = None;
    for schema in metadata.schemas_iter() {
        active().map_err(PartitionSourceTypeFailure::Original)?;
        let Some(source) = schema.field_by_id(source_id) else {
            continue;
        };
        let Type::Primitive(ty) = source.field_type.as_ref() else {
            return Err(format!(
                "partition source {} is not primitive in retained schema {}",
                source_id,
                schema.schema_id()
            )
            .into());
        };
        historical = Some(match historical {
            None => ty.clone(),
            Some(previous) => legal_partition_type_join(metadata.format_version(), &previous, ty)
                .ok_or_else(|| {
                format!(
                    "partition source {} has incompatible retained types {previous:?} and {ty:?}",
                    source_id
                )
            })?,
        });
    }
    let historical = historical.ok_or_else(|| {
        format!(
            "partition source {} has no retained schema type evidence",
            source_id
        )
    })?;
    active().map_err(PartitionSourceTypeFailure::Original)?;
    let resolved = match query.field_by_id(source_id) {
        Some(source) => {
            let Type::Primitive(ty) = source.field_type.as_ref() else {
                return Err(format!(
                    "partition source {} is not primitive in the query schema",
                    source_id
                )
                .into());
            };
            legal_partition_type_join(metadata.format_version(), &historical, ty).ok_or_else(
                || {
                    format!(
                        "query partition source {} conflicts with retained schema types",
                        source_id
                    )
                },
            )?;
            ty.clone()
        }
        None => historical.clone(),
    };
    Ok((resolved, historical))
}

fn legal_partition_type_join(
    version: crate::iceberg::spec::FormatVersion,
    a: &PrimitiveType,
    b: &PrimitiveType,
) -> Option<PrimitiveType> {
    use PrimitiveType::*;
    if a == b {
        return Some(a.clone());
    }
    match (a, b) {
        (Int, Long) | (Long, Int) => Some(Long),
        (Float, Double) | (Double, Float) => Some(Double),
        (Date, Timestamp) | (Timestamp, Date)
            if version == crate::iceberg::spec::FormatVersion::V3 =>
        {
            Some(Timestamp)
        }
        (Date, TimestampNs) | (TimestampNs, Date)
            if version == crate::iceberg::spec::FormatVersion::V3 =>
        {
            Some(TimestampNs)
        }
        (
            Decimal {
                precision: p,
                scale: s,
            },
            Decimal {
                precision: q,
                scale: t,
            },
        ) if s == t => Some(Decimal {
            precision: (*p).max(*q),
            scale: *s,
        }),
        _ => None,
    }
}

fn metadata_identity(metadata: &TableMetadata) -> Result<String, String> {
    // This is a canonical fact identity, not the raw metadata object's digest.
    // SDK map/set iteration cannot change a pinned relation's identity.
    let bytes =
        crate::delete_semantics::canonical_metadata_json(metadata).map_err(|e| e.to_string())?;
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
}

pub async fn build_read_snapshot_at(
    table: &Table,
    snapshot_id: i64,
) -> Result<IcebergReadSnapshot, String> {
    build_read_snapshot_at_with_control(table, snapshot_id, None).await
}

pub(crate) async fn build_read_snapshot_at_with_control(
    table: &Table,
    snapshot_id: i64,
    control: Option<&dyn novarocks_spi::connector::ConnectorOperationControl>,
) -> Result<IcebergReadSnapshot, String> {
    let metadata = table.metadata();
    let snapshot = metadata
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| format!("snapshot {snapshot_id} not found"))?;
    let schema = snapshot
        .schema(metadata)
        .map_err(|e| format!("resolve snapshot schema: {e}"))?;
    let domain = mint_read_domain(metadata, snapshot_id, &schema)?;
    build_read_snapshot_in_domain(table, domain, control).await
}

/// Observe every distinct manifest exactly once. Facts remain bound to their
/// entry, including separate blobs and descriptions sharing one physical path.
pub(crate) async fn build_read_snapshot_in_domain(
    table: &Table,
    domain: Arc<ReadDomain>,
    control: Option<&dyn novarocks_spi::connector::ConnectorOperationControl>,
) -> Result<IcebergReadSnapshot, String> {
    let snapshot_id = domain.endpoint().snapshot_id();
    let files = observe_read_files::<DataFile>(table, domain, control, None)
        .await
        .map_err(|e| e.to_string())?;
    Ok(IcebergReadSnapshot {
        snapshot_id: Some(snapshot_id),
        files,
    })
}

/// COW needs the same validated read facts, but not the scan metadata graph.
#[cfg(test)]
pub(crate) async fn build_cow_read_files_at_with_control(
    table: &Table,
    snapshot_id: i64,
    control: Option<&dyn novarocks_spi::connector::ConnectorOperationControl>,
) -> Result<Vec<CowReadFile>, String> {
    let metadata = table.metadata();
    let snapshot = metadata
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| format!("snapshot {snapshot_id} not found"))?;
    let schema = snapshot
        .schema(metadata)
        .map_err(|e| format!("resolve snapshot schema: {e}"))?;
    let domain = mint_read_domain(metadata, snapshot_id, &schema)?;
    observe_read_files::<CowDataFileProjection>(table, domain, control, None)
        .await
        .map_err(|e| e.to_string())
}

/// Checked COW retains the original FE operation through its captured files.
pub(crate) async fn build_cow_read_files_at_with_original_scope(
    table: &Table,
    snapshot_id: i64,
    capture: &CaptureScope<'_>,
) -> Result<Vec<IcebergReadFile>, ReadFailure> {
    capture.active()?;
    let metadata = table.metadata();
    let snapshot = metadata
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| format!("snapshot {snapshot_id} not found"))?;
    let schema = snapshot
        .schema(metadata)
        .map_err(|e| format!("resolve snapshot schema: {e}"))?;
    let domain = mint_read_domain(metadata, snapshot_id, &schema)?;
    observe_read_files::<FrozenCowDataFileProjection>(table, domain, None, Some(capture)).await
}

pub(crate) async fn make_scoped_read_snapshot(
    table: Table,
    snapshot_id: i64,
    context: novarocks_spi::connector::ConnectorRequestContext,
    original: novarocks_spi::connector::ConnectorOriginalResultScope,
    kind: crate::catalog_control::statistics::scoped_cpu::scoped_snapshot::SnapshotReadKind,
    io: Arc<crate::catalog_control::statistics::scoped_cpu::scoped_snapshot::SnapshotIoGate>,
) -> Result<
    crate::catalog_control::statistics::scoped_cpu::scoped_snapshot::ScopedReadSnapshot,
    ReadFailure,
> {
    let capture = CaptureScope::new(&original)?;
    let metadata = table.metadata();
    let snapshot = metadata
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| format!("snapshot {snapshot_id} not found"))?;
    use crate::catalog_control::statistics::scoped_cpu::scoped_snapshot::{
        ReferencedSpecInventory, SnapshotReadKind,
    };
    let domain = match &kind {
        SnapshotReadKind::PreparedSource { domain } => {
            if domain.endpoint().snapshot_id() != snapshot_id {
                return Err("prepared snapshot differs from its original read domain"
                    .to_string()
                    .into());
            }
            domain.clone()
        }
        SnapshotReadKind::ChangeWindow { schema } => {
            mint_read_domain(metadata, snapshot_id, schema)?
        }
        _ => {
            let schema = snapshot.schema(metadata).map_err(|e| e.to_string())?;
            mint_read_domain(metadata, snapshot_id, &schema)?
        }
    };
    let mut referenced_specs = match &kind {
        SnapshotReadKind::PreparedSource { .. } => Some(
            ReferencedSpecInventory::for_original(&context)
                .map_err(|error| ReadFailure::Original(error.into()))?,
        ),
        _ => None,
    };
    let mut all_deletes = Vec::new();
    let files = match kind {
        SnapshotReadKind::ChangeWindow { .. } | SnapshotReadKind::PreparedSource { .. } => {
            // Preserve the original full endpoint projection and delete
            // applicability. Both endpoints use the SAME To schema above.
            observe_read_files_original::<DataFile>(
                &table,
                domain,
                Some(&context),
                Some(&capture),
                None,
                Some(io.as_ref()),
                referenced_specs.as_mut(),
            )
            .await?
        }
        SnapshotReadKind::Statistics => {
            observe_read_files_original::<FrozenCowDataFileProjection>(
                &table,
                domain,
                Some(&context),
                Some(&capture),
                None,
                Some(io.as_ref()),
                None,
            )
            .await?
        }
        SnapshotReadKind::Rewrite => {
            observe_read_files_original::<FrozenCowDataFileProjection>(
                &table,
                domain,
                Some(&context),
                Some(&capture),
                Some(&mut all_deletes),
                Some(io.as_ref()),
                None,
            )
            .await?
        }
    };
    Ok(
        crate::catalog_control::statistics::scoped_cpu::scoped_snapshot::ScopedReadSnapshot {
            files,
            all_deletes,
            referenced_specs,
            retention_guard: original.retention_guard(),
        },
    )
}

/// One common observation retains the original entry, delete and lineage checks.
/// Only the data-file storage projection differs between scan and COW callers.
async fn observe_read_files<P: ObservedDataFileProjection>(
    table: &Table,
    domain: Arc<ReadDomain>,
    control: Option<&dyn novarocks_spi::connector::ConnectorOperationControl>,
    capture: Option<&CaptureScope<'_>>,
) -> Result<Vec<P::File>, ReadFailure> {
    observe_read_files_with_delete_receiving::<P>(table, domain, control, capture, None).await
}

async fn observe_read_files_with_delete_receiving<P: ObservedDataFileProjection>(
    table: &Table,
    domain: Arc<ReadDomain>,
    control: Option<&dyn novarocks_spi::connector::ConnectorOperationControl>,
    capture: Option<&CaptureScope<'_>>,
    all_deletes: Option<&mut Vec<Arc<crate::delete_semantics::DeleteFact>>>,
) -> Result<Vec<P::File>, ReadFailure> {
    observe_read_files_original::<P>(table, domain, control, capture, all_deletes, None, None).await
}

/// Index by physical path, but retain the original descriptor as a borrowed
/// fact. A path match is not proof of equal inheritance/interpretation facts.
#[repr(transparent)]
struct ManifestReferenceKey<'a>(&'a ManifestFile);
impl PartialEq for ManifestReferenceKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0.manifest_path == other.0.manifest_path
    }
}
impl Eq for ManifestReferenceKey<'_> {}
impl std::hash::Hash for ManifestReferenceKey<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(&self.0.manifest_path, state);
    }
}
const CONFLICTING_MANIFEST_REFERENCE: &str =
    "Iceberg snapshot repeats one manifest path with conflicting descriptors";

async fn observe_read_files_original<P: ObservedDataFileProjection>(
    table: &Table,
    domain: Arc<ReadDomain>,
    control: Option<&dyn novarocks_spi::connector::ConnectorOperationControl>,
    capture: Option<&CaptureScope<'_>>,
    all_deletes: Option<&mut Vec<Arc<crate::delete_semantics::DeleteFact>>>,
    io: Option<&crate::catalog_control::statistics::scoped_cpu::scoped_snapshot::SnapshotIoGate>,
    mut referenced_specs: Option<&mut crate::catalog_control::statistics::scoped_cpu::scoped_snapshot::ReferencedSpecInventory>,
) -> Result<Vec<P::File>, ReadFailure> {
    check_read_control(control)?;
    check_cow_active(capture)?;
    let metadata = table.metadata();

    if domain.endpoint().table_uuid() != metadata.uuid()
        || domain.endpoint().metadata_identity() != metadata_identity(metadata)?
    {
        return Err("Iceberg read domain differs from the pinned table metadata"
            .to_string()
            .into());
    }
    let snapshot_id = domain.endpoint().snapshot_id();
    let snapshot = metadata
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| format!("snapshot {snapshot_id} not found"))?;

    let schema = domain.endpoint().schema().map_err(|e| e.to_string())?;

    let field_id_to_name: HashMap<i32, String> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| (f.id, f.name.clone()))
        .collect();
    let manifest_list = match io {
        Some(io) => {
            let bytes = io.read(snapshot.manifest_list()).await?;
            crate::manifest::decode_manifest_list(&bytes, metadata).map_err(|error| {
                ReadFailure::Original(crate::catalog_control::statistics::scoped_cpu::scoped_snapshot::raw_read_failure(error).into())
            })?
        }
        None => crate::manifest::load_manifest_list(snapshot, table.file_io(), metadata)
            .await
            .map_err(|e| format!("load manifest list: {e}"))?,
    };
    if let Some(inventory) = referenced_specs.as_deref_mut() {
        let gate = io.ok_or_else(|| {
            ReadFailure::Original(
                novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                    "prepared spec inventory requires the original snapshot gate",
                )
                .into(),
            )
        })?;
        // Exact manifest-list slot count, before any inventory allocation.
        // Repeated physical paths leave unused paid capacity, not extra reads.
        inventory
            .reserve_slots(gate, manifest_list.entries().len(), || {
                check_read_control(control)?;
                check_cow_active(capture)
            })
            .await?;
    }
    let mut seen_manifests: HashSet<ManifestReferenceKey<'_>> = HashSet::new();
    let mut delete_manifests = Vec::new();
    let mut observed_data = Vec::new();
    for manifest_file in manifest_list.entries() {
        check_read_control(control)?;
        check_cow_active(capture)?;

        let key = ManifestReferenceKey(manifest_file);
        if let Some(previous) = seen_manifests.get(&key) {
            if previous.0 != manifest_file {
                return Err(ReadFailure::Original(
                    novarocks_spi::connector::ConnectorError::new(
                        novarocks_spi::connector::ConnectorErrorKind::CorruptData,
                        CONFLICTING_MANIFEST_REFERENCE,
                    )
                    .into(),
                ));
            }
            continue;
        }
        seen_manifests.insert(key);
        let manifest = match io {
            Some(io) => {
                let bytes = io.read(&manifest_file.manifest_path).await?;
                crate::manifest::decode_manifest(&bytes, manifest_file).map_err(|error| {
                    ReadFailure::Original(crate::catalog_control::statistics::scoped_cpu::scoped_snapshot::raw_read_failure(error).into())
                })?
            }
            None => crate::manifest::load_manifest(manifest_file, table.file_io())
                .await
                .map_err(|e| {
                    crate::manifest::with_variant_statistics_hint(
                        format!("load manifest: {e}"),
                        metadata,
                    )
                })?,
        };
        check_read_control(control)?;
        check_cow_active(capture)?;
        let spec = metadata
            .partition_spec_by_id(manifest_file.partition_spec_id)
            .ok_or_else(|| {
                format!(
                    "manifest {} refers to unknown spec {}",
                    manifest_file.manifest_path, manifest_file.partition_spec_id
                )
            })?;
        if manifest.metadata().partition_spec != **spec {
            return Err(format!(
                "manifest {} partition spec differs from pinned metadata",
                manifest_file.manifest_path
            )
            .into());
        }

        let partition_type = domain
            .endpoint()
            .partition_type(spec.spec_id())
            .map_err(|e| e.to_string())?;
        if let Some(inventory) = referenced_specs.as_deref_mut() {
            // This must precede entry filtering. Empty manifests and manifests
            // containing only deleted/delete entries still depended on this
            // exact pinned header, storage binding and inheritance context.
            inventory.record(spec.spec_id())?;
        }
        let mut delete_entries = Vec::new();

        let mut next_first_row_id = manifest_file
            .first_row_id
            .map(|v| i64::try_from(v).map_err(|_| format!("manifest first_row_id too large: {v}")))
            .transpose()?;
        for entry in manifest.entries() {
            check_read_control(control)?;
            check_cow_active(capture)?;
            // Deleted entries are not live facts; unresolved sequence metadata
            // on them cannot become a read failure or an inheritance fallback.
            if entry.status == ManifestStatus::Deleted {
                continue;
            }
            let sequence = EntrySequence {
                format_version: manifest.metadata().format_version,
                status: entry.status,
                data_sequence: entry.sequence_number(),
                manifest_sequence: manifest_file.sequence_number,
            };
            let df = entry.data_file();
            let typed_partition = if let Some(capture) = capture {
                TypedPartition::bind_type_checked(
                    spec,
                    &partition_type,
                    df.partition(),
                    |_, _| capture.active(),
                    || capture.active(),
                )
                .map_err(|failure| match failure {
                    crate::delete_semantics::PartitionBindFailure::Semantic(error) => {
                        ReadFailure::Semantic(error.to_string())
                    }
                    crate::delete_semantics::PartitionBindFailure::Original(error) => error,
                })?
            } else {
                TypedPartition::bind_type(spec, &partition_type, df.partition())
                    .map_err(|e| e.to_string())?
            };
            let metrics = manifest_metrics_checked(df, &schema, capture)?;
            match df.content_type() {
                DataContentType::Data => {
                    let count = i64::try_from(df.record_count())
                        .map_err(|_| format!("record_count too large for {}", df.file_path()))?;
                    let first_row_id = df.first_row_id().or(next_first_row_id);
                    if let Some(next) = next_first_row_id.as_mut() {
                        *next = next.checked_add(count).ok_or_else(|| {
                            format!(
                                "first_row_id overflow for manifest {}",
                                manifest_file.manifest_path
                            )
                        })?;
                    }

                    let data = DataFileFact::try_new(
                        df.file_path(),
                        sequence.required_sequence().map_err(|e| e.to_string())?,
                        typed_partition,
                        df.record_count(),
                        metrics,
                    )
                    .map_err(|e| e.to_string())?;

                    let values = resolved_partition_values(df.partition(), &partition_type);
                    observed_data.push((
                        P::capture(df, &field_id_to_name, capture)?,
                        data,
                        values,
                        manifest_file.manifest_path.clone(),
                        first_row_id,
                    ));
                }
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    i64::try_from(df.file_size_in_bytes()).map_err(|_| {
                        format!("delete file size is unrepresentable: {}", df.file_path())
                    })?;
                    i64::try_from(df.record_count()).map_err(|_| {
                        format!("delete record count is unrepresentable: {}", df.file_path())
                    })?;

                    let (address, kind) = match (df.content_type(), df.file_format()) {
                        (DataContentType::PositionDeletes, DataFileFormat::Puffin) => {
                            let offset = df.content_offset().ok_or_else(|| {
                                format!("Puffin DV {} missing content_offset", df.file_path())
                            })?;
                            let size = df.content_size_in_bytes().ok_or_else(|| {
                                format!(
                                    "Puffin DV {} missing content_size_in_bytes",
                                    df.file_path()
                                )
                            })?;
                            let target = df.referenced_data_file().ok_or_else(|| {
                                format!("Puffin DV {} missing referenced_data_file", df.file_path())
                            })?;
                            (
                                DeleteContentAddress::puffin(
                                    df.file_path(),
                                    offset,
                                    size,
                                    df.file_size_in_bytes(),
                                ),
                                DeleteKind::DeletionVector {
                                    exact_target: target.into(),
                                },
                            )
                        }
                        (DataContentType::PositionDeletes, DataFileFormat::Parquet) => (
                            DeleteContentAddress::file(df.file_path()),
                            DeleteKind::Position {
                                exact_target: df.referenced_data_file().map(Arc::from),
                            },
                        ),
                        (DataContentType::EqualityDeletes, DataFileFormat::Parquet) => {
                            let ids = df.equality_ids().ok_or_else(|| {
                                format!("equality delete {} missing equality_ids", df.file_path())
                            })?;
                            let fields = EqualityFieldGroup::bind(&ids, &schema)
                                .map_err(|e| e.to_string())?;
                            (
                                DeleteContentAddress::file(df.file_path()),
                                DeleteKind::Equality(fields),
                            )
                        }
                        _ => {
                            return Err(format!(
                                "unsupported Iceberg delete format {:?}: {}",
                                df.file_format(),
                                df.file_path()
                            )
                            .into());
                        }
                    };
                    if df.key_metadata().is_some_and(|k| !k.is_empty()) {
                        return Err(format!(
                            "Iceberg encrypted delete file {} is unsupported",
                            df.file_path()
                        )
                        .into());
                    }
                    delete_entries.push(RawDeleteEntry {
                        sequence,
                        file: RawDeleteFile {
                            address: address.map_err(|e| e.to_string())?,
                            kind,
                            partition: typed_partition,
                            read: DeleteReadFacts {
                                format: df.file_format().into(),
                                file_size: df.file_size_in_bytes(),
                                record_count: df.record_count(),
                                key_metadata: Arc::from(df.key_metadata().unwrap_or_default()),
                            },
                            metrics,
                        },
                    });
                }
            }
        }

        delete_manifests.push(ManifestDeleteObservation {
            manifest_path: Arc::from(manifest_file.manifest_path.as_str()),
            entries: delete_entries,
        });
    }

    let observation =
        DeleteObservation::from_manifests_checked(delete_manifests, || check_cow_active(capture))
            .map_err(|e| match e {
            crate::delete_semantics::ConstructionFailure::Semantic(e) => {
                ReadFailure::Semantic(e.to_string())
            }
            crate::delete_semantics::ConstructionFailure::Original(e) => e,
        })?;
    if let Some(receiving) = all_deletes {
        if !receiving.is_empty() {
            return Err(ReadFailure::Semantic(
                "Rewrite delete receiver is occupied".into(),
            ));
        }

        receiving.extend(observation.facts().iter().cloned());
    }
    let index =
        DeleteCandidateIndex::try_new_checked(domain, observation, || check_cow_active(capture))
            .map_err(|e| match e {
                crate::delete_semantics::ConstructionFailure::Semantic(e) => {
                    ReadFailure::Semantic(e.to_string())
                }
                crate::delete_semantics::ConstructionFailure::Original(e) => e,
            })?;

    let mut files = Vec::with_capacity(observed_data.len());
    for (df, data, partition_values, manifest_path, first_row_id) in observed_data {
        check_read_control(control)?;
        check_cow_active(capture)?;

        let logical = index.for_data(&data).map_err(|e| e.to_string())?;
        let deletes = metadata_delete_load_view(&logical);
        files.push(df.into_file(
            FinalReadFileFacts {
                partition_spec_id: data.partition().spec_id(),
                data_sequence_number: data.sequence().get(),
                partition_values,
                manifest_path,
                first_row_id,
                deletes,
            },
            &field_id_to_name,
        )?);
    }
    if let Some(inventory) = referenced_specs {
        inventory.finish(|| {
            check_read_control(control)?;
            check_cow_active(capture)
        })?;
    }
    Ok(files)
}

/// The shared metadata-only policy for delete applicability and statistics pruning.
pub(crate) fn metadata_delete_load_view(logical: &crate::delete_semantics::DeleteSet) -> LoadView {
    // Large candidate suffixes remain compact conservative views under the
    // original metadata capture; unknown statistics never prove disjointness.
    let policy = if logical.data().metrics.fields().is_empty() {
        StatisticsPolicy::Disabled
    } else {
        StatisticsPolicy::MetadataBudget {
            max_candidate_members: MAX_STATS_DELETE_MEMBERS,
            max_field_comparisons: MAX_STATS_FIELD_COMPARISONS,
        }
    };
    logical.load_view(policy)
}

/// All final conversions still occur after complete manifest/delete validation.
struct FinalReadFileFacts {
    partition_spec_id: i32,
    data_sequence_number: i64,
    partition_values: Struct,
    manifest_path: String,
    first_row_id: Option<i64>,
    deletes: LoadView,
}

trait ObservedDataFileProjection: Sized {
    type File;

    fn capture(
        df: &DataFile,
        field_id_to_name: &HashMap<i32, String>,
        capture: Option<&CaptureScope<'_>>,
    ) -> Result<Self, ReadFailure>;

    fn into_file(
        self,
        facts: FinalReadFileFacts,
        field_id_to_name: &HashMap<i32, String>,
    ) -> Result<Self::File, String>;
}

impl ObservedDataFileProjection for DataFile {
    type File = IcebergReadFile;

    fn capture(
        df: &DataFile,
        _field_id_to_name: &HashMap<i32, String>,
        _capture: Option<&CaptureScope<'_>>,
    ) -> Result<Self, ReadFailure> {
        Ok(df.clone())
    }

    fn into_file(
        self,
        facts: FinalReadFileFacts,
        field_id_to_name: &HashMap<i32, String>,
    ) -> Result<Self::File, String> {
        Ok(IcebergReadFile {
            path: self.file_path().to_string(),
            size: i64::try_from(self.file_size_in_bytes())
                .map_err(|_| format!("file size too large: {}", self.file_path()))?,
            record_count: Some(
                i64::try_from(self.record_count()).map_err(|_| "record count too large")?,
            ),
            column_stats: column_stats(&self, field_id_to_name),
            partition_spec_id: Some(facts.partition_spec_id),
            partition_key: iceberg_partition_key(&facts.partition_values),
            partition_values: Some(facts.partition_values),
            manifest_path: Some(facts.manifest_path),
            first_row_id: facts.first_row_id,
            data_sequence_number: Some(facts.data_sequence_number),
            manifest: Arc::new(IcebergDataFileMetadata {
                file_format: self.file_format(),
                split_offsets: self.split_offsets().unwrap_or_default().to_vec(),
                key_metadata: self.key_metadata().unwrap_or_default().to_vec(),
                value_counts: self.value_counts().clone(),
                null_value_counts: self.null_value_counts().clone(),
                nan_value_counts: self.nan_value_counts().clone(),
                lower_bounds: self.lower_bounds().clone(),
                upper_bounds: self.upper_bounds().clone(),
            }),
            deletes: facts.deletes,
        })
    }
}

/// Only fields consumed by the COW manifest projection are copied while the
/// original SDK DataFile is borrowed. No DataFile or scan metadata is retained.
struct CowDataFileProjection {
    path: String,
    file_size_in_bytes: u64,
    record_count: u64,
    column_stats: Option<HashMap<String, IcebergColumnStats>>,
}

#[derive(Debug)]
pub(crate) struct CowReadFile {
    pub path: String,
    pub size: i64,
    pub record_count: Option<i64>,
    pub column_stats: Option<HashMap<String, IcebergColumnStats>>,
    pub partition_spec_id: Option<i32>,
    pub partition_key: Option<String>,
    pub partition_values: Option<Struct>,
    pub manifest_path: Option<String>,
    pub first_row_id: Option<i64>,
    pub data_sequence_number: Option<i64>,
    pub deletes: LoadView,
}

impl ObservedDataFileProjection for CowDataFileProjection {
    type File = CowReadFile;

    fn capture(
        df: &DataFile,
        field_id_to_name: &HashMap<i32, String>,
        capture: Option<&CaptureScope<'_>>,
    ) -> Result<Self, ReadFailure> {
        let output = Self {
            path: df.file_path().to_owned(),
            file_size_in_bytes: df.file_size_in_bytes(),
            record_count: df.record_count(),
            column_stats: column_stats_checked(df, field_id_to_name, capture)?,
        };
        check_cow_active(capture)?;
        Ok(output)
    }

    fn into_file(
        self,
        facts: FinalReadFileFacts,
        _field_id_to_name: &HashMap<i32, String>,
    ) -> Result<Self::File, String> {
        // Preserve the original final iteration's fallible conversion order.
        let size = i64::try_from(self.file_size_in_bytes)
            .map_err(|_| format!("file size too large: {}", self.path))?;
        let record_count =
            i64::try_from(self.record_count).map_err(|_| "record count too large")?;
        Ok(CowReadFile {
            path: self.path,
            size,
            record_count: Some(record_count),
            column_stats: self.column_stats,
            partition_spec_id: Some(facts.partition_spec_id),
            partition_key: iceberg_partition_key(&facts.partition_values),
            partition_values: Some(facts.partition_values),
            manifest_path: Some(facts.manifest_path),
            first_row_id: facts.first_row_id,
            data_sequence_number: Some(facts.data_sequence_number),
            deletes: facts.deletes,
        })
    }
}

/// Keep the exact typed manifest remainder required by frozen read authority.
/// Only public immutable fields of the original SDK entry are copied, and every
/// source retains its original owner; no complete SDK DataFile is cloned.
struct FrozenCowDataFileProjection {
    stats: CowDataFileProjection,
    manifest: Arc<IcebergDataFileMetadata>,
}
impl ObservedDataFileProjection for FrozenCowDataFileProjection {
    type File = IcebergReadFile;
    fn capture(
        df: &DataFile,
        names: &HashMap<i32, String>,
        capture: Option<&CaptureScope<'_>>,
    ) -> Result<Self, ReadFailure> {
        let stats = CowDataFileProjection::capture(df, names, capture)?;

        // Rebuild insert-only maps from the exact original pairs. This gives a
        // exact owned copy from their known lengths even if the SDK input
        // maps retained spare buckets or tombstones from prior construction.
        let counts = |map: &HashMap<i32, u64>| -> Result<HashMap<i32, u64>, ReadFailure> {
            let mut output = HashMap::with_capacity(map.len());
            for (&id, &value) in map {
                check_cow_active(capture)?;
                output.insert(id, value);
            }
            Ok(output)
        };
        let bounds = |map: &HashMap<i32,crate::iceberg::spec::Datum>| -> Result<HashMap<i32,crate::iceberg::spec::Datum>,ReadFailure> {
            let mut output = HashMap::with_capacity(map.len());
            for (&id,value) in map { check_cow_active(capture)?; output.insert(id,value.clone()); }
            Ok(output)
        };
        let manifest = Arc::new(IcebergDataFileMetadata {
            file_format: df.file_format(),
            split_offsets: df.split_offsets().unwrap_or_default().to_vec(),
            key_metadata: df.key_metadata().unwrap_or_default().to_vec(),
            value_counts: counts(df.value_counts())?,
            null_value_counts: counts(df.null_value_counts())?,
            nan_value_counts: counts(df.nan_value_counts())?,
            lower_bounds: bounds(df.lower_bounds())?,
            upper_bounds: bounds(df.upper_bounds())?,
        });
        check_cow_active(capture)?;
        Ok(Self { stats, manifest })
    }
    fn into_file(
        self,
        facts: FinalReadFileFacts,
        names: &HashMap<i32, String>,
    ) -> Result<Self::File, String> {
        let file = self.stats.into_file(facts, names)?;
        Ok(IcebergReadFile {
            path: file.path,
            size: file.size,
            record_count: file.record_count,
            column_stats: file.column_stats,
            partition_spec_id: file.partition_spec_id,
            partition_key: file.partition_key,
            partition_values: file.partition_values,
            manifest_path: file.manifest_path,
            first_row_id: file.first_row_id,
            data_sequence_number: file.data_sequence_number,
            manifest: self.manifest,
            deletes: file.deletes,
        })
    }
}

pub(crate) fn resolved_partition_values(
    values: &Struct,
    partition_type: &crate::iceberg::spec::StructType,
) -> Struct {
    Struct::from_iter(
        values
            .fields()
            .iter()
            .zip(partition_type.fields())
            .map(|(value, field)| match (value, field.field_type.as_ref()) {
                (Some(Literal::Primitive(value)), Type::Primitive(target)) => Some(
                    Literal::Primitive(resolved_partition_primitive(value, target).into_owned()),
                ),
                _ => value.clone(),
            }),
    )
}

/// The admitted lossless partition promotions; unrelated literals remain exact
/// and are rejected by their final typed consumer rather than coerced here.
pub(crate) fn resolved_partition_primitive<'a>(
    value: &'a PrimitiveLiteral,
    target: &PrimitiveType,
) -> std::borrow::Cow<'a, PrimitiveLiteral> {
    match (value, target) {
        (PrimitiveLiteral::Int(value), PrimitiveType::Long) => {
            std::borrow::Cow::Owned(PrimitiveLiteral::Long(i64::from(*value)))
        }
        (PrimitiveLiteral::Float(value), PrimitiveType::Double) => {
            std::borrow::Cow::Owned(PrimitiveLiteral::Double(f64::from(value.0).into()))
        }
        _ => std::borrow::Cow::Borrowed(value),
    }
}

fn manifest_metrics_checked(
    df: &DataFile,
    schema: &Schema,
    capture: Option<&CaptureScope<'_>>,
) -> Result<FileMetrics, ReadFailure> {
    manifest_metrics_for_metadata(df, schema, || check_cow_active(capture))
}

/// The same manifest metric binding under the caller's original control owner.
pub(crate) fn manifest_metrics_for_metadata<E>(
    df: &DataFile,
    schema: &Schema,
    mut active: impl FnMut() -> Result<(), E>,
) -> Result<FileMetrics, E> {
    let mut ids = std::collections::BTreeSet::new();
    for id in df
        .value_counts()
        .keys()
        .chain(df.null_value_counts().keys())
        .chain(df.nan_value_counts().keys())
        .chain(df.lower_bounds().keys())
        .chain(df.upper_bounds().keys())
    {
        active()?;
        ids.insert(*id);
    }
    let mut fields = BTreeMap::new();
    for id in ids {
        active()?;
        let value_type = match id {
            POSITION_FILE_PATH_FIELD_ID
                if df.content_type() == DataContentType::PositionDeletes =>
            {
                Some(PrimitiveType::String)
            }
            i if i == POSITION_FILE_PATH_FIELD_ID - 1
                && df.content_type() == DataContentType::PositionDeletes =>
            {
                Some(PrimitiveType::Long)
            }
            _ => schema
                .field_by_id(id)
                .and_then(|f| match f.field_type.as_ref() {
                    Type::Primitive(p) => Some(p.clone()),
                    _ => None,
                }),
        };
        let Some(resolved_type) = value_type else {
            continue;
        };
        fields.insert(
            id,
            FieldMetrics {
                resolved_type,
                value_count: df.value_counts().get(&id).copied(),
                null_count: df.null_value_counts().get(&id).copied(),
                nan_count: df.nan_value_counts().get(&id).copied(),
                lower_bound: df.lower_bounds().get(&id).cloned(),
                upper_bound: df.upper_bounds().get(&id).cloned(),
            }
            .bind_bounds(),
        );
    }
    active()?;
    FileMetrics::new_checked(fields, active)
}

fn column_stats(
    df: &DataFile,
    field_id_to_name: &HashMap<i32, String>,
) -> Option<HashMap<String, IcebergColumnStats>> {
    column_stats_checked(df, field_id_to_name, None)
        .unwrap_or_else(|_| unreachable!("ordinary stats has no fallible scope"))
}
fn column_stats_checked(
    df: &DataFile,
    field_id_to_name: &HashMap<i32, String>,
    capture: Option<&CaptureScope<'_>>,
) -> Result<Option<HashMap<String, IcebergColumnStats>>, ReadFailure> {
    let null_counts = df.null_value_counts();
    let value_counts = df.value_counts();
    let col_sizes = df.column_sizes();
    let lower = df.lower_bounds();
    let upper = df.upper_bounds();
    let has_any_stats = !null_counts.is_empty()
        || !value_counts.is_empty()
        || !col_sizes.is_empty()
        || !lower.is_empty()
        || !upper.is_empty();

    if has_any_stats {
        let mut all_ids = std::collections::HashSet::new();
        for id in null_counts
            .keys()
            .chain(value_counts.keys())
            .chain(col_sizes.keys())
            .chain(lower.keys())
            .chain(upper.keys())
        {
            check_cow_active(capture)?;
            all_ids.insert(*id);
        }

        let mut stats_map = HashMap::new();
        for &field_id in &all_ids {
            check_cow_active(capture)?;
            if let Some(column_name) = field_id_to_name.get(&field_id) {
                let lower_bound = lower
                    .get(&field_id)
                    .and_then(|datum| datum.to_bytes().ok())
                    .map(|bytes| bytes.to_vec());
                let upper_bound = upper
                    .get(&field_id)
                    .and_then(|datum| datum.to_bytes().ok())
                    .map(|bytes| bytes.to_vec());
                stats_map.insert(
                    column_name.clone(),
                    IcebergColumnStats {
                        field_id: Some(field_id),
                        null_count: null_counts
                            .get(&field_id)
                            .map(|&value| i64::try_from(value).unwrap_or(i64::MAX)),
                        value_count: value_counts
                            .get(&field_id)
                            .map(|&value| i64::try_from(value).unwrap_or(i64::MAX)),
                        column_size: col_sizes
                            .get(&field_id)
                            .map(|&value| i64::try_from(value).unwrap_or(i64::MAX)),
                        lower_bound,
                        upper_bound,
                    },
                );
            }
        }
        Ok(Some(stats_map))
    } else {
        Ok(None)
    }
}

fn check_read_control(
    control: Option<&dyn novarocks_spi::connector::ConnectorOperationControl>,
) -> Result<(), ReadFailure> {
    if let Some(control) = control {
        control
            .check_active()
            .map_err(|cause| ReadFailure::Original(cause.into()))?;
    }
    Ok(())
}

pub async fn build_current_read_snapshot(table: &Table) -> Result<IcebergReadSnapshot, String> {
    match table.metadata().current_snapshot() {
        Some(snapshot) => build_read_snapshot_at(table, snapshot.snapshot_id()).await,
        None => Ok(IcebergReadSnapshot {
            snapshot_id: None,
            files: Vec::new(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delete_semantics::{DeleteContentAddress, PositionSource, StatisticsDecision};
    use crate::iceberg::io::FileIO;
    use crate::iceberg::spec::{
        DataFileBuilder, FormatVersion, ManifestListWriter, ManifestWriterBuilder, NestedField,
        Operation, PartitionSpec, Snapshot, SortOrder, Summary, TableMetadataBuilder, Transform,
    };

    #[test]
    fn read_control_keeps_original_typed_fact_instead_of_semantic_text() {
        use novarocks_failure_contract::{FailureFact, FailureProjection, SharedFailureCause};
        use novarocks_spi::connector::{
            ConnectorCowBeginCause, ConnectorError, ConnectorErrorKind, ConnectorOperationControl,
        };
        use novarocks_type_contract::CompileControlError;
        #[derive(Debug)]
        struct CallerData;
        impl std::fmt::Display for CallerData {
            fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                panic!("caller source Display must not run");
            }
        }
        impl std::error::Error for CallerData {}
        impl FailureProjection for CallerData {
            fn failure_fact(&self) -> FailureFact {
                ConnectorErrorKind::CorruptData.failure_fact()
            }
        }
        struct Refused(ConnectorError);
        impl ConnectorOperationControl for Refused {
            fn check_active(&self) -> Result<(), ConnectorError> {
                Err(self.0.clone())
            }
        }
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
            CompileControlError::Source(SharedFailureCause::from_static_detail(
                CallerData,
                "safe caller data refusal",
            )),
        ] {
            let original = ConnectorError::compile_interruption(control);
            let checker = Refused(original.clone());
            let failure = check_read_control(Some(&checker)).unwrap_err();
            let ConnectorCowBeginCause::Provider(actual) = failure.into_cause() else {
                panic!("the original connector error must survive the read boundary");
            };
            assert_eq!(actual.failure_fact(), original.failure_fact());
            assert_eq!(actual, original);
        }
    }

    fn file(path: &str, content: DataContentType, partition: Struct) -> DataFileBuilder {
        let mut builder = DataFileBuilder::default();
        builder
            .file_path(path.to_string())
            .content(content)
            .file_format(DataFileFormat::Parquet)
            .partition(partition)
            .record_count(10)
            .file_size_in_bytes(1024);
        builder
    }
    fn data(path: &str) -> DataFile {
        file(path, DataContentType::Data, Struct::empty())
            .build()
            .unwrap()
    }
    fn position(path: &str, target: &str) -> DataFile {
        file(path, DataContentType::PositionDeletes, Struct::empty())
            .referenced_data_file(Some(target.to_string()))
            .build()
            .unwrap()
    }
    fn equality(path: &str) -> DataFile {
        file(path, DataContentType::EqualityDeletes, Struct::empty())
            .equality_ids(Some(vec![1]))
            .build()
            .unwrap()
    }
    fn dv(path: &str, target: &str, offset: i64, count: u64) -> DataFile {
        file(path, DataContentType::PositionDeletes, Struct::empty())
            .file_format(DataFileFormat::Puffin)
            .record_count(count)
            .referenced_data_file(Some(target.to_string()))
            .content_offset(Some(offset))
            .content_size_in_bytes(Some(32))
            .build()
            .unwrap()
    }

    async fn table(
        manifests: Vec<Vec<(DataFile, i64)>>,
        repeat: Option<usize>,
        partitioned: bool,
    ) -> Table {
        table_with_delete_spec(manifests, repeat, partitioned, false).await
    }

    async fn table_with_delete_spec(
        manifests: Vec<Vec<(DataFile, i64)>>,
        repeat: Option<usize>,
        partitioned: bool,
        separate_delete_spec: bool,
    ) -> Table {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "region",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap();
        table_with_schema_and_delete_spec(
            manifests,
            repeat,
            partitioned,
            separate_delete_spec,
            schema,
            Transform::Identity,
        )
        .await
    }

    async fn table_with_schema_and_delete_spec(
        manifests: Vec<Vec<(DataFile, i64)>>,
        repeat: Option<usize>,
        partitioned: bool,
        separate_delete_spec: bool,
        schema: Schema,
        partition_transform: Transform,
    ) -> Table {
        let io = FileIO::new_with_memory();
        let spec = if partitioned {
            PartitionSpec::builder(Arc::new(schema.clone()))
                .with_spec_id(0)
                .add_partition_field("region", "region_part", partition_transform)
                .unwrap()
                .build()
                .unwrap()
        } else {
            PartitionSpec::unpartition_spec()
        };
        let metadata = TableMetadataBuilder::new(
            schema,
            spec.into_unbound(),
            SortOrder::unsorted_order(),
            "memory:///table".to_string(),
            FormatVersion::V3,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let metadata = if separate_delete_spec {
            let spec = PartitionSpec::builder(metadata.current_schema().clone())
                .add_partition_field("region", "region_prefix", Transform::Truncate(2))
                .unwrap()
                .build()
                .unwrap();
            metadata
                .into_builder(None)
                .add_partition_spec(spec.into_unbound())
                .unwrap()
                .build()
                .unwrap()
                .metadata
        } else {
            metadata
        };
        let mut manifest_files = Vec::new();
        for (i, entries) in manifests.into_iter().enumerate() {
            let spec =
                if separate_delete_spec && entries[0].0.content_type() != DataContentType::Data {
                    metadata.partition_spec_by_id(1).unwrap()
                } else {
                    metadata.default_partition_spec()
                };
            let builder = ManifestWriterBuilder::new(
                io.new_output(format!("memory:///manifest-{i}.avro"))
                    .unwrap(),
                Some(77),
                metadata.current_schema().clone(),
                spec.as_ref().clone(),
            );
            let mut writer = if entries[0].0.content_type() == DataContentType::Data {
                builder.build_v3_data()
            } else {
                builder.build_v3_deletes()
            };
            for (file, sequence) in entries {
                writer.add_file(file, sequence).unwrap();
            }
            manifest_files.push(writer.write_manifest_file().await.unwrap());
        }
        if let Some(i) = repeat {
            manifest_files.push(manifest_files[i].clone());
        }
        let list_path = "memory:///list.avro";
        let mut list = ManifestListWriter::v3(
            io.new_output(list_path).unwrap().writer().await.unwrap(),
            77,
            None,
            7,
            Some(0),
        );
        list.add_manifests(manifest_files.into_iter()).unwrap();
        let added_rows = list.next_row_id().unwrap();
        list.close().await.unwrap();
        let snapshot = Snapshot::builder()
            .with_snapshot_id(77)
            .with_parent_snapshot_id(None)
            .with_sequence_number(7)
            .with_timestamp_ms(metadata.last_updated_ms())
            .with_manifest_list(list_path.to_string())
            .with_summary(Summary {
                operation: Operation::Overwrite,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(metadata.current_schema_id())
            .with_row_range(0, added_rows)
            .build();
        let metadata = metadata
            .into_builder(None)
            .add_snapshot(snapshot)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        Table::builder()
            .runtime(crate::iceberg::Runtime::try_current().expect("test runtime"))
            .file_io(io)
            .metadata(metadata)
            .identifier(crate::iceberg::TableIdent::from_strs(["db", "t"]).unwrap())
            .disable_cache()
            .build()
            .unwrap()
    }

    fn with_metadata(table: &Table, metadata: TableMetadata) -> Table {
        Table::builder()
            .runtime(crate::iceberg::Runtime::try_current().expect("test runtime"))
            .file_io(table.file_io().clone())
            .metadata(metadata)
            .identifier(crate::iceberg::TableIdent::from_strs(["db", "t"]).unwrap())
            .disable_cache()
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn avro_historical_snapshot_ignores_query_absence_of_unused_future_spec_source() {
        let original = table(vec![vec![(data("a"), 5)]], None, false).await;
        let mut fields = original
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .to_vec();
        fields.push(Arc::new(NestedField::optional(
            3,
            "future",
            Type::Primitive(PrimitiveType::Int),
        )));
        let future_schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(fields)
            .build()
            .unwrap();
        let future_spec = PartitionSpec::builder(Arc::new(future_schema.clone()))
            .add_partition_field("future", "future_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap();
        let metadata = original
            .metadata()
            .clone()
            .into_builder(None)
            .add_current_schema(future_schema)
            .unwrap()
            .add_partition_spec(future_spec.into_unbound())
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let evolved = with_metadata(&original, metadata);
        let read = build_read_snapshot_at(&evolved, 77).await.unwrap();
        assert_eq!(read.files.len(), 1);
        assert_eq!(read.files[0].path, "a");
        let domain = read.files[0].read_domain();
        assert!(domain.endpoint().schema().unwrap().field_by_id(3).is_none());
        assert_eq!(
            domain.endpoint().partition_type(1).unwrap().fields()[0]
                .field_type
                .as_ref(),
            &Type::Primitive(PrimitiveType::Int)
        );
        let dto = crate::typed_read::split::encode_read_domain(domain);
        assert_eq!(
            crate::typed_read::split::decode_read_domain(&dto).unwrap(),
            *domain
        );
        let mut absent = dto;
        absent.partition_type_jsons.clear();
        assert!(crate::typed_read::split::decode_read_domain(&absent).is_err());
    }

    #[tokio::test]
    async fn avro_dropped_partition_source_keeps_historical_type_and_promotes_old_tuple() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "region",
                    Type::Primitive(PrimitiveType::Int),
                )),
            ])
            .build()
            .unwrap();
        let physical = file(
            "old-int-data",
            DataContentType::Data,
            Struct::from_iter([Some(Literal::int(-7))]),
        )
        .build()
        .unwrap();
        let original = table_with_schema_and_delete_spec(
            vec![vec![(physical, 5)]],
            None,
            true,
            false,
            schema,
            Transform::Identity,
        )
        .await;
        let promoted = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "region",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .unwrap();
        let dropped = Schema::builder()
            .with_schema_id(2)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap();
        let metadata = original
            .metadata()
            .clone()
            .into_builder(None)
            .add_current_schema(promoted)
            .unwrap()
            .add_partition_spec(PartitionSpec::unpartition_spec().into_unbound())
            .unwrap()
            .set_default_partition_spec(-1)
            .unwrap()
            .add_current_schema(dropped)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let next = Snapshot::builder()
            .with_snapshot_id(78)
            .with_parent_snapshot_id(Some(77))
            .with_sequence_number(8)
            .with_timestamp_ms(metadata.last_updated_ms())
            .with_manifest_list(
                original
                    .metadata()
                    .snapshot_by_id(77)
                    .unwrap()
                    .manifest_list()
                    .to_string(),
            )
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(metadata.current_schema_id())
            .with_row_range(metadata.next_row_id(), 0)
            .build();
        let metadata = metadata
            .into_builder(None)
            .add_snapshot(next)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let encoded = serde_json::to_string(&metadata).unwrap();
        let evolved = with_metadata(&original, metadata);
        let read = build_read_snapshot_at(&evolved, 78).await.unwrap();
        let domain = read.files[0].read_domain();
        assert!(domain.endpoint().schema().unwrap().field_by_id(2).is_none());
        assert_eq!(
            read.files[0].partition_values.as_ref().unwrap(),
            &Struct::from_iter([Some(Literal::long(-7))])
        );
        assert_eq!(
            read.files[0]
                .logical_delete_set()
                .data()
                .partition()
                .values(),
            &[Some(crate::delete_semantics::CanonicalScalar::Long(-7))]
        );
        for _ in 0..16 {
            let metadata: TableMetadata = serde_json::from_str(&encoded).unwrap();
            let rebuilt = mint_read_domain(&metadata, 78, metadata.current_schema()).unwrap();
            assert_eq!(rebuilt.endpoint(), domain.endpoint());
        }
        let dto = crate::typed_read::split::encode_read_domain(domain);
        let decoded = crate::typed_read::split::decode_read_domain(&dto).unwrap();
        assert_eq!(&decoded, domain);
        // Exercise real FE split production and BE admission with the Avro facts.
        use crate::typed_read::delete_manager::{
            DeleteDomainBindings, DeleteEvaluationMode, DeleteManager,
        };
        use crate::typed_read::split_source::{
            IcebergPlannedDataFile, IcebergSplitSource, IcebergSplitSourceOptions,
        };
        use crate::typed_read::table_handle::{IcebergTableHandle, IcebergTableHandleParams};
        use novarocks_spi::connector::read_stack::{
            ConnectorSplitSource, DynamicFilterSnapshot, SchemaTableName, TupleDomain,
        };
        let mut params = IcebergTableHandleParams {
            schema_table_name: SchemaTableName::try_new("db", "t").unwrap(),
            snapshot_id: Some(78),
            read_domain: Some(domain.clone()),
            table_schema_json: domain.endpoint().schema_json().to_string(),
            spec_id: Some(evolved.metadata().default_partition_spec_id()),
            partition_spec_jsons: domain
                .endpoint()
                .partition_spec_jsons()
                .iter()
                .map(|(id, json)| (*id, json.to_string()))
                .collect(),
            format_version: 3,
            unenforced_predicate: TupleDomain::all(),
            enforced_predicate: TupleDomain::all(),
            limit: None,
            projected_columns: Default::default(),
            name_mapping_json: None,
            table_location: "memory:///table".to_owned(),
            storage_properties: Default::default(),
            pinned_data_files: None,
        };
        let handle = IcebergTableHandle::try_new(params.clone()).unwrap();
        let planned = IcebergPlannedDataFile {
            read_file: read.files[0].clone(),
            file_format: crate::typed_read::split::IcebergFileFormat::Parquet,
            split_offsets: Vec::new(),
            key_metadata: Vec::new(),
            file_statistics_domain: TupleDomain::all(),
            decryption_data: None,
        };
        let mut source = IcebergSplitSource::try_new(
            &handle,
            vec![planned],
            IcebergSplitSourceOptions::default(),
        )
        .unwrap();
        let splits = source
            .next_batch(8, &DynamicFilterSnapshot::all_complete())
            .unwrap()
            .into_splits();
        assert_eq!(splits.len(), 1);
        assert!(
            DeleteManager::preview_hidden_columns(
                &splits[0],
                &domain.endpoint().schema().unwrap(),
                &DeleteDomainBindings::Single(domain.clone()),
                &DeleteEvaluationMode::ExcludeDeleted
            )
            .unwrap()
            .is_empty()
        );
        params.snapshot_id = None;
        params.read_domain = None;
        let empty = IcebergTableHandle::try_new(params).unwrap();
        let empty =
            IcebergSplitSource::try_new(&empty, Vec::new(), IcebergSplitSourceOptions::default())
                .unwrap();
        assert!(empty.is_finished());
        let spec: PartitionSpec =
            serde_json::from_str(&domain.endpoint().partition_spec_jsons()[&0]).unwrap();
        let stored_type = decoded.endpoint().partition_type(0).unwrap();
        let tuple = crate::delete_semantics::decode_partition_data_json_with_type(
            &spec,
            &stored_type,
            &read.files[0]
                .logical_delete_set()
                .data()
                .partition()
                .to_json_string(),
        )
        .unwrap();
        assert_eq!(tuple, Struct::from_iter([Some(Literal::long(-7))]));
        let mut missing: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        for schema in missing["schemas"].as_array_mut().unwrap() {
            schema["fields"]
                .as_array_mut()
                .unwrap()
                .retain(|field| field["id"] != 2);
        }
        let missing: TableMetadata = serde_json::from_value(missing).unwrap();
        assert!(
            mint_read_domain(&missing, 78, missing.current_schema())
                .unwrap_err()
                .contains("no retained schema type evidence")
        );
        let mut incompatible: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        let historical_schema = incompatible["schemas"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|schema| schema["schema-id"] == 0)
            .unwrap();
        historical_schema["fields"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|field| field["id"] == 2)
            .unwrap()["type"] = serde_json::json!("string");
        let incompatible: TableMetadata = serde_json::from_value(incompatible).unwrap();
        assert!(
            mint_read_domain(&incompatible, 78, incompatible.current_schema())
                .unwrap_err()
                .contains("incompatible retained types")
        );
    }

    #[tokio::test]
    async fn avro_historical_date_partition_survives_later_legal_v3_timestamp_schema() {
        let schema = |ty| {
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(2, "region", Type::Primitive(ty))),
                ])
                .build()
                .unwrap()
        };
        let old = file(
            "old-date-data",
            DataContentType::Data,
            Struct::from_iter([Some(Literal::int(-7))]),
        )
        .build()
        .unwrap();
        let original = table_with_schema_and_delete_spec(
            vec![vec![(old, 5)]],
            None,
            true,
            false,
            schema(PrimitiveType::Date),
            Transform::Day,
        )
        .await;
        for future_type in [PrimitiveType::Timestamp, PrimitiveType::TimestampNs] {
            // day(date) and day(timestamp) retain the same stored day value.
            let future_schema = schema(future_type);
            let future_hour_spec = PartitionSpec::builder(Arc::new(future_schema.clone()))
                .add_partition_field("region", "future_hour", Transform::Hour)
                .unwrap()
                .build()
                .unwrap();
            let metadata = original
                .metadata()
                .clone()
                .into_builder(None)
                .add_current_schema(future_schema)
                .unwrap()
                .add_partition_spec(future_hour_spec.into_unbound())
                .unwrap()
                .build()
                .unwrap()
                .metadata;
            let evolved = with_metadata(&original, metadata);
            let read = build_read_snapshot_at(&evolved, 77).await.unwrap();
            assert_eq!(
                read.files[0]
                    .read_domain()
                    .endpoint()
                    .partition_type(1)
                    .unwrap()
                    .fields()[0]
                    .field_type
                    .as_ref(),
                &Type::Primitive(PrimitiveType::Int)
            );
            let wire = crate::typed_read::split::encode_read_domain(read.files[0].read_domain());
            assert_eq!(
                crate::typed_read::split::decode_read_domain(&wire).unwrap(),
                *read.files[0].read_domain()
            );
            assert_eq!(
                read.files[0].partition_values.as_ref().unwrap(),
                &Struct::from_iter([Some(Literal::int(-7))])
            );
            assert_eq!(
                read.files[0]
                    .read_domain()
                    .endpoint()
                    .schema()
                    .unwrap()
                    .field_by_id(2)
                    .unwrap()
                    .field_type
                    .as_ref(),
                &Type::Primitive(PrimitiveType::Date)
            );
        }
        assert_eq!(
            legal_partition_type_join(
                FormatVersion::V2,
                &PrimitiveType::Date,
                &PrimitiveType::Timestamp
            ),
            None
        );
        assert_eq!(
            legal_partition_type_join(
                FormatVersion::V3,
                &PrimitiveType::Date,
                &PrimitiveType::Timestamptz
            ),
            None
        );
        assert_eq!(
            legal_partition_type_join(
                FormatVersion::V3,
                &PrimitiveType::Date,
                &PrimitiveType::TimestamptzNs
            ),
            None
        );
        assert_eq!(
            legal_partition_type_join(
                FormatVersion::V3,
                &PrimitiveType::Timestamp,
                &PrimitiveType::TimestampNs
            ),
            None
        );
    }

    #[test]
    fn partition_history_join_is_commutative_and_rejects_unproven_type_changes() {
        assert_eq!(
            legal_partition_type_join(FormatVersion::V3, &PrimitiveType::Int, &PrimitiveType::Long),
            Some(PrimitiveType::Long)
        );
        assert_eq!(
            legal_partition_type_join(FormatVersion::V3, &PrimitiveType::Long, &PrimitiveType::Int),
            Some(PrimitiveType::Long)
        );
        assert_eq!(
            legal_partition_type_join(
                FormatVersion::V3,
                &PrimitiveType::Float,
                &PrimitiveType::Double
            ),
            Some(PrimitiveType::Double)
        );
        assert_eq!(
            legal_partition_type_join(
                FormatVersion::V3,
                &PrimitiveType::Int,
                &PrimitiveType::String
            ),
            None
        );
        assert_eq!(
            legal_partition_type_join(
                FormatVersion::V3,
                &PrimitiveType::Decimal {
                    precision: 8,
                    scale: 2
                },
                &PrimitiveType::Decimal {
                    precision: 9,
                    scale: 3
                }
            ),
            None
        );
    }

    #[tokio::test]
    async fn avro_same_commit_position_applies_and_equality_does_not() {
        let table = table(
            vec![
                vec![(data("a"), 5)],
                vec![(position("p", "a"), 5), (equality("e"), 5)],
            ],
            None,
            false,
        )
        .await;
        let snapshot = build_read_snapshot_at(&table, 77).await.unwrap();
        let read = &snapshot.files[0];
        assert_eq!(read.logical_delete_set().member_count(), 1);
        assert_eq!(read.deletes.members().next().unwrap().address().path(), "p");
        assert_eq!(read.deletes.cost().visited_members, 0);
        assert_eq!(read.deletes.decision(), StatisticsDecision::Disabled);
    }

    #[tokio::test]
    async fn avro_distinct_puffin_blobs_keep_their_entry_facts() {
        let table = table(
            vec![
                vec![(data("a"), 5), (data("b"), 5)],
                vec![
                    (dv("shared.puffin", "a", 4, 2), 5),
                    (dv("shared.puffin", "b", 64, 7), 6),
                ],
            ],
            None,
            false,
        )
        .await;
        let snapshot = build_read_snapshot_at(&table, 77).await.unwrap();
        for (read, offset, count) in [(&snapshot.files[0], 4, 2), (&snapshot.files[1], 64, 7)] {
            let member = read.deletes.members().next().unwrap();
            assert_eq!(member.read().record_count, count);
            assert!(
                matches!(member.address(),DeleteContentAddress::PuffinBlob{offset: found,..} if *found==offset)
            );
            assert!(matches!(
                read.logical_delete_set().position(),
                PositionSource::OneDv(_)
            ));
        }
    }

    /// Existing Avro owner, with entries whose inheritance is supplied by the
    /// list descriptor. Snapshot 77 retains existing rows from an expired
    /// predecessor, so each standalone historical descriptor is readable.
    async fn descriptor_context_table() -> (Table, ManifestFile) {
        let original = table(vec![vec![(data("a"), -1)]], None, false).await;
        let mut metadata = serde_json::to_value(original.metadata()).unwrap();
        metadata["last-sequence-number"] = serde_json::json!(8);
        metadata["next-row-id"] = serde_json::json!(110);
        let snapshot = &mut metadata["snapshots"][0];
        snapshot["sequence-number"] = serde_json::json!(8);
        snapshot["parent-snapshot-id"] = serde_json::json!(76);
        snapshot["first-row-id"] = serde_json::json!(110);
        snapshot["added-rows"] = serde_json::json!(0);
        let original = with_metadata(&original, serde_json::from_value(metadata).unwrap());
        let mut writer = ManifestWriterBuilder::new(
            original
                .file_io()
                .new_output("memory:///descriptor-context.avro")
                .unwrap(),
            None,
            original.metadata().current_schema().clone(),
            original
                .metadata()
                .default_partition_spec()
                .as_ref()
                .clone(),
        )
        .build_v3_data();
        writer.add_file(data("a"), -1).unwrap();
        let mut descriptor = writer.write_manifest_file().await.unwrap();
        descriptor.sequence_number = 6;
        descriptor.min_sequence_number = 6;
        descriptor.added_snapshot_id = 76;
        descriptor.first_row_id = Some(0);
        write_descriptor_references(&original, vec![descriptor.clone()]).await;
        (original, descriptor)
    }

    async fn write_descriptor_references(table: &Table, descriptors: Vec<ManifestFile>) {
        let snapshot = table.metadata().snapshot_by_id(77).unwrap();
        let mut writer = ManifestListWriter::v3(
            table
                .file_io()
                .new_output(snapshot.manifest_list())
                .unwrap()
                .writer()
                .await
                .unwrap(),
            77,
            Some(76),
            8,
            Some(110),
        );
        // All data first-row-ids are explicit before cloning. The SDK cannot
        // assign different row identities to two pending clones here.
        assert!(descriptors.iter().all(|entry| entry.first_row_id.is_some()));
        writer
            .add_manifests(descriptors.clone().into_iter())
            .unwrap();
        writer.close().await.unwrap();
        let actual =
            crate::manifest::load_manifest_list(snapshot, table.file_io(), table.metadata())
                .await
                .unwrap();
        assert_eq!(
            actual.entries(),
            descriptors.as_slice(),
            "SDK preserved both exact descriptor contexts"
        );
    }

    async fn descriptor_observation_error<P: ObservedDataFileProjection>(
        table: &Table,
    ) -> ReadFailure {
        let domain =
            mint_read_domain(table.metadata(), 77, table.metadata().current_schema()).unwrap();
        match observe_read_files::<P>(table, domain, None, None).await {
            Ok(_) => panic!("conflicting manifest reference was accepted"),
            Err(error) => error,
        }
    }
    fn assert_descriptor_conflict(error: ReadFailure) {
        let ReadFailure::Original(novarocks_spi::connector::ConnectorCowBeginCause::Provider(
            error,
        )) = error
        else {
            panic!("descriptor conflict lost its typed Provider cause");
        };
        assert_eq!(
            error.kind(),
            novarocks_spi::connector::ConnectorErrorKind::CorruptData
        );
        assert_eq!(error.message(), CONFLICTING_MANIFEST_REFERENCE);
    }

    #[tokio::test]
    async fn avro_exact_data_manifest_repeat_keeps_one_explicit_inherited_row_identity() {
        let (table, descriptor) = descriptor_context_table().await;
        write_descriptor_references(&table, vec![descriptor.clone(), descriptor]).await;
        let snapshot = build_read_snapshot_at(&table, 77).await.unwrap();
        assert_eq!(snapshot.files.len(), 1);
        assert_eq!(snapshot.files[0].first_row_id, Some(0));
        assert_eq!(snapshot.files[0].data_sequence_number, Some(6));
        let cow = compare_cow_extraction(&table).await;
        assert_eq!(cow.len(), 1);
        assert_eq!(cow[0].first_row_id, Some(0));
    }

    #[tokio::test]
    async fn avro_conflicting_manifest_inheritance_is_rejected_in_both_orders() {
        for field in 0..3 {
            let (table, descriptor) = descriptor_context_table().await;
            let mut changed = descriptor.clone();
            match field {
                0 => {
                    changed.sequence_number = 7;
                    changed.min_sequence_number = 7;
                }
                1 => changed.added_snapshot_id = 75,
                2 => changed.first_row_id = Some(100),
                _ => unreachable!(),
            }
            for single in [&descriptor, &changed] {
                write_descriptor_references(&table, vec![single.clone()]).await;
                let read = build_read_snapshot_at(&table, 77).await.unwrap();
                assert_eq!(
                    read.files.len(),
                    1,
                    "each complete context is independently readable"
                );
                assert_eq!(
                    read.files[0].data_sequence_number,
                    Some(single.sequence_number)
                );
                assert_eq!(
                    read.files[0].first_row_id,
                    single.first_row_id.map(|id| i64::try_from(id).unwrap())
                );
                assert_eq!(compare_cow_extraction(&table).await.len(), 1);
            }
            for reversed in [false, true] {
                let pair = if reversed {
                    vec![changed.clone(), descriptor.clone()]
                } else {
                    vec![descriptor.clone(), changed.clone()]
                };
                write_descriptor_references(&table, pair).await;
                assert_descriptor_conflict(descriptor_observation_error::<DataFile>(&table).await);
                assert_descriptor_conflict(
                    descriptor_observation_error::<FrozenCowDataFileProjection>(&table).await,
                );
            }
        }
    }

    #[tokio::test]
    async fn avro_conflicting_manifest_spec_rejects_without_reordering_original_header_failure() {
        let (table, descriptor) = descriptor_context_table().await;
        let mut changed = descriptor.clone();
        changed.partition_spec_id += 1;
        write_descriptor_references(&table, vec![descriptor.clone(), changed.clone()]).await;
        assert_descriptor_conflict(descriptor_observation_error::<DataFile>(&table).await);
        assert_descriptor_conflict(
            descriptor_observation_error::<FrozenCowDataFileProjection>(&table).await,
        );
        write_descriptor_references(&table, vec![changed, descriptor]).await;
        for error in [
            descriptor_observation_error::<DataFile>(&table).await,
            descriptor_observation_error::<FrozenCowDataFileProjection>(&table).await,
        ] {
            let ReadFailure::Semantic(message) = error else {
                panic!("first manifest header failure was replaced by a later conflict");
            };
            assert!(message.contains("unknown spec"), "{message}");
        }
    }

    #[tokio::test]
    async fn avro_first_manifest_decode_failure_precedes_a_later_descriptor_conflict() {
        let (table, descriptor) = descriptor_context_table().await;
        let mut changed = descriptor.clone();
        changed.sequence_number = 7;
        changed.min_sequence_number = 7;
        write_descriptor_references(&table, vec![descriptor.clone(), changed]).await;
        let mut output = table
            .file_io()
            .new_output(&descriptor.manifest_path)
            .unwrap()
            .writer()
            .await
            .unwrap();
        output
            .write(bytes::Bytes::from_static(b"invalid original manifest avro"))
            .await
            .unwrap();
        output.close().await.unwrap();
        for error in [
            descriptor_observation_error::<DataFile>(&table).await,
            descriptor_observation_error::<FrozenCowDataFileProjection>(&table).await,
        ] {
            let ReadFailure::Semantic(message) = error else {
                panic!("original decode failure was replaced by descriptor pre-scan");
            };
            assert!(message.contains("load manifest"), "{message}");
            assert!(!message.contains(CONFLICTING_MANIFEST_REFERENCE));
        }
    }

    #[tokio::test]
    async fn avro_repeated_manifest_reference_is_deduplicated_before_dv_validation() {
        let table = table(
            vec![
                vec![(data("a"), 5)],
                vec![(dv("shared.puffin", "a", 4, 2), 5)],
            ],
            Some(1),
            false,
        )
        .await;
        let snapshot = build_read_snapshot_at(&table, 77).await.unwrap();
        assert_eq!(snapshot.files[0].logical_delete_set().member_count(), 1);
    }

    #[tokio::test]
    async fn avro_duplicate_dv_entries_are_rejected_even_when_physically_equal() {
        for across_manifests in [false, true] {
            let delete = dv("shared.puffin", "a", 4, 2);
            let manifests = if across_manifests {
                vec![
                    vec![(data("a"), 5)],
                    vec![(delete.clone(), 5)],
                    vec![(delete, 5)],
                ]
            } else {
                vec![vec![(data("a"), 5)], vec![(delete.clone(), 5), (delete, 5)]]
            };
            let table = table(manifests, None, false).await;
            assert!(
                build_read_snapshot_at(&table, 77)
                    .await
                    .unwrap_err()
                    .contains("deletion vector")
            );
        }
    }

    #[tokio::test]
    async fn avro_equal_paths_preserve_distinct_equality_application_sequences() {
        let table = table(
            vec![
                vec![(data("a"), 5)],
                vec![(equality("shared"), 6), (equality("shared"), 7)],
            ],
            None,
            false,
        )
        .await;
        let snapshot = build_read_snapshot_at(&table, 77).await.unwrap();
        assert_eq!(
            snapshot.files[0]
                .deletes
                .members()
                .map(|f| f.sequence().get())
                .collect::<Vec<_>>(),
            vec![6, 7]
        );
    }

    #[tokio::test]
    async fn avro_exact_position_target_ignores_the_delete_partition_tuple() {
        let data = file(
            "a",
            DataContentType::Data,
            Struct::from_iter([Some(Literal::string("emea"))]),
        )
        .build()
        .unwrap();
        let delete = file(
            "p",
            DataContentType::PositionDeletes,
            Struct::from_iter([Some(Literal::string("apac"))]),
        )
        .referenced_data_file(Some("a".to_string()))
        .build()
        .unwrap();
        let table = table(vec![vec![(data, 5)], vec![(delete, 5)]], None, true).await;
        let snapshot = build_read_snapshot_at(&table, 77).await.unwrap();
        assert_eq!(snapshot.files[0].deletes.member_count(), 1);
        assert_ne!(
            snapshot.files[0]
                .deletes
                .members()
                .next()
                .unwrap()
                .partition(),
            snapshot.files[0].logical_delete_set().data().partition()
        );
    }

    #[tokio::test]
    async fn pinned_domain_is_shared_and_independent_observations_are_distinct() {
        let table = table(vec![vec![(data("a"), 5), (data("b"), 5)]], None, false).await;
        let domain =
            mint_read_domain(table.metadata(), 77, table.metadata().current_schema()).unwrap();
        let snapshot = build_read_snapshot_in_domain(&table, domain.clone(), None)
            .await
            .unwrap();
        assert!(
            snapshot
                .files
                .iter()
                .all(|f| Arc::ptr_eq(f.read_domain(), &domain))
        );
        let other =
            mint_read_domain(table.metadata(), 77, table.metadata().current_schema()).unwrap();
        assert_eq!(domain.endpoint(), other.endpoint());
        assert_ne!(domain.observation(), other.observation());
    }

    #[tokio::test]
    async fn avro_added_missing_sequence_inherits_manifest_sequence() {
        let table = table(
            vec![vec![(data("a"), 5)], vec![(position("p", "a"), -1)]],
            None,
            false,
        )
        .await;
        let snapshot = build_read_snapshot_at(&table, 77).await.unwrap();
        assert_eq!(
            snapshot.files[0]
                .deletes
                .members()
                .next()
                .unwrap()
                .sequence()
                .get(),
            7
        );
    }

    #[tokio::test]
    async fn avro_stale_dv_fails_instead_of_becoming_an_empty_closure() {
        let table = table(
            vec![vec![(data("a"), 5)], vec![(dv("puffin", "a", 4, 2), 4)]],
            None,
            false,
        )
        .await;
        assert!(
            build_read_snapshot_at(&table, 77)
                .await
                .unwrap_err()
                .contains("older")
        );
    }

    #[tokio::test]
    async fn avro_exact_target_ignores_delete_spec_for_positions_and_dvs() {
        for puffin in [false, true] {
            let values = Struct::from_iter([Some(Literal::string("emea"))]);
            let mut delete = file(
                "delete",
                DataContentType::PositionDeletes,
                Struct::from_iter([Some(Literal::string("ap"))]),
            );
            delete.referenced_data_file(Some("a".to_string()));
            if puffin {
                delete
                    .file_format(DataFileFormat::Puffin)
                    .content_offset(Some(4))
                    .content_size_in_bytes(Some(32));
            }
            let table = table_with_delete_spec(
                vec![
                    vec![(file("a", DataContentType::Data, values).build().unwrap(), 5)],
                    vec![(delete.build().unwrap(), 5)],
                ],
                None,
                true,
                true,
            )
            .await;
            let snapshot = build_read_snapshot_at(&table, 77).await.unwrap();
            let read = &snapshot.files[0];
            let fact = read.deletes.members().next().unwrap();
            assert_eq!(read.logical_delete_set().data().partition().spec_id(), 0);
            assert_eq!(fact.partition().spec_id(), 1);
            assert_eq!(read.deletes.member_count(), 1);
        }
    }

    #[tokio::test]
    async fn avro_no_statistics_keeps_shared_suffixes_without_member_visits() {
        let data_files = (0..32).map(|i| (data(&format!("data-{i}")), 5)).collect();
        let delete_files = (0..512)
            .map(|i| (equality(&format!("delete-{i}")), 6))
            .collect();
        let table = table(vec![data_files, delete_files], None, false).await;
        let snapshot = build_read_snapshot_at(&table, 77).await.unwrap();
        let first = &snapshot.files[0].logical_delete_set().equality()[0];
        assert_eq!(first.bucket().members().len(), 512);
        for file in &snapshot.files {
            assert_eq!(file.logical_delete_set().member_count(), 512);
            assert_eq!(file.deletes.cost().visited_members, 0);
            let view = &file.logical_delete_set().equality()[0];
            assert!(Arc::ptr_eq(first.bucket(), view.bucket()));
        }
    }

    #[tokio::test]
    async fn avro_statistics_prune_loads_without_changing_logical_identity() {
        use crate::iceberg::spec::Datum;
        for known_null_counts in [true, false] {
            let mut data = file("a", DataContentType::Data, Struct::empty());
            data.value_counts(HashMap::from([(1, 10)]))
                .lower_bounds(HashMap::from([(1, Datum::long(0))]))
                .upper_bounds(HashMap::from([(1, Datum::long(10))]));
            let mut delete = file("eq", DataContentType::EqualityDeletes, Struct::empty());
            delete
                .equality_ids(Some(vec![1]))
                .value_counts(HashMap::from([(1, 10)]))
                .lower_bounds(HashMap::from([(1, Datum::long(20))]))
                .upper_bounds(HashMap::from([(1, Datum::long(30))]));
            if known_null_counts {
                data.null_value_counts(HashMap::from([(1, 0)]));
                delete.null_value_counts(HashMap::from([(1, 0)]));
            }
            let table = table(
                vec![
                    vec![(data.build().unwrap(), 5)],
                    vec![(delete.build().unwrap(), 6)],
                ],
                None,
                false,
            )
            .await;
            let snapshot = build_read_snapshot_at(&table, 77).await.unwrap();
            let read = &snapshot.files[0];
            assert_eq!(read.logical_delete_set().member_count(), 1);
            assert_eq!(read.deletes.member_count(), usize::from(!known_null_counts));
            assert_eq!(read.deletes.cost().visited_members, 1);
        }
    }

    #[tokio::test]
    async fn metadata_content_identity_ignores_nested_map_insertion_order() {
        fn reverse_objects(value: &mut serde_json::Value) {
            match value {
                serde_json::Value::Object(object) => {
                    let mut members = std::mem::take(object).into_iter().collect::<Vec<_>>();
                    members.reverse();
                    for (key, mut value) in members {
                        reverse_objects(&mut value);
                        object.insert(key, value);
                    }
                }
                serde_json::Value::Array(values) => {
                    for value in values {
                        reverse_objects(value);
                    }
                }
                _ => {}
            }
        }
        let table = table(vec![vec![(data("a"), 5)]], None, false).await;
        let mut original = serde_json::to_value(table.metadata()).unwrap();
        original["properties"] = serde_json::json!({"z":"last","a":"first","m":"middle"});
        let mut reversed = original.clone();
        reverse_objects(&mut reversed);
        let a: TableMetadata = serde_json::from_value(original).unwrap();
        let b: TableMetadata = serde_json::from_value(reversed.clone()).unwrap();
        assert_eq!(
            metadata_identity(&a).unwrap(),
            metadata_identity(&b).unwrap()
        );
        reversed["properties"]["a"] = serde_json::json!("changed");
        let changed: TableMetadata = serde_json::from_value(reversed).unwrap();
        assert_ne!(
            metadata_identity(&a).unwrap(),
            metadata_identity(&changed).unwrap()
        );
    }

    #[tokio::test]
    async fn metadata_identity_is_stable_across_independent_sdk_map_and_set_rebuilds() {
        let table = table(vec![vec![(data("a"), 5)]], None, false).await;
        let mut value = serde_json::to_value(table.metadata()).unwrap();
        let mut schema = value["schemas"][0].clone();
        schema["fields"][1]["required"] = serde_json::json!(true);
        schema["identifier-field-ids"] = serde_json::json!([2, 1]);
        let mut newer = schema.clone();
        newer["schema-id"] = serde_json::json!(1);
        value["schemas"] = serde_json::json!([newer, schema]);
        value["current-schema-id"] = serde_json::json!(1);
        value["partition-specs"] = serde_json::json!([
            {"spec-id":1,"fields":[{"source-id":2,"field-id":1000,"name":"region_part","transform":"identity"}]},
            {"spec-id":0,"fields":[]}
        ]);
        value["last-partition-id"] = serde_json::json!(1000);
        value["sort-orders"] = serde_json::json!([
            {"order-id":1,"fields":[
                {"source-id":2,"transform":"identity","direction":"asc","null-order":"nulls-first"},
                {"source-id":1,"transform":"identity","direction":"desc","null-order":"nulls-last"}
            ]}, {"order-id":0,"fields":[]}
        ]);
        let mut next_snapshot = value["snapshots"][0].clone();
        next_snapshot["snapshot-id"] = serde_json::json!(78);
        next_snapshot["parent-snapshot-id"] = serde_json::json!(77);
        next_snapshot["sequence-number"] = serde_json::json!(8);
        next_snapshot["schema-id"] = serde_json::json!(1);
        next_snapshot["first-row-id"] = serde_json::json!(10);
        value["snapshots"]
            .as_array_mut()
            .unwrap()
            .push(next_snapshot);
        value["last-sequence-number"] = serde_json::json!(8);
        value["next-row-id"] = serde_json::json!(20);
        value["statistics"] = serde_json::json!([78, 77].map(|snapshot| serde_json::json!({
            "snapshot-id":snapshot,"statistics-path":format!("memory:///stats-{snapshot}.puffin"),
            "file-size-in-bytes":1024,"file-footer-size-in-bytes":128,
            "blob-metadata":[
                {"type":"first","snapshot-id":snapshot,"sequence-number":7,"fields":[2,1]},
                {"type":"second","snapshot-id":snapshot,"sequence-number":7,"fields":[1,2]}
            ]
        })));
        value["partition-statistics"] = serde_json::json!([78,77].map(|snapshot| serde_json::json!({
            "snapshot-id":snapshot,"statistics-path":format!("memory:///part-stats-{snapshot}.parquet"),"file-size-in-bytes":256
        })));
        value["encryption-keys"] = serde_json::json!([
            {"key-id":"second","encrypted-key-metadata":"Ag==","encrypted-by-id":null},
            {"key-id":"first","encrypted-key-metadata":"AQ==","encrypted-by-id":null}
        ]);
        let metadata: TableMetadata = serde_json::from_value(value.clone()).unwrap();
        let identity = metadata_identity(&metadata).unwrap();
        let expected_endpoint = PinnedEndpointFacts::try_new(
            metadata.uuid(),
            identity.clone(),
            78,
            metadata.current_schema(),
            &metadata
                .partition_specs_iter()
                .map(|spec| spec.as_ref().clone())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        for _ in 0..32 {
            for collection in [
                "schemas",
                "partition-specs",
                "sort-orders",
                "snapshots",
                "statistics",
                "partition-statistics",
                "encryption-keys",
            ] {
                value[collection].as_array_mut().unwrap().reverse();
            }
            for schema in value["schemas"].as_array_mut().unwrap() {
                schema["identifier-field-ids"]
                    .as_array_mut()
                    .unwrap()
                    .reverse();
            }
            // Deserialize anew to rebuild every SDK HashMap and HashSet.
            let rebuilt: TableMetadata = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(metadata_identity(&rebuilt).unwrap(), identity);
            let endpoint = PinnedEndpointFacts::try_new(
                rebuilt.uuid(),
                identity.clone(),
                78,
                rebuilt.current_schema(),
                &rebuilt
                    .partition_specs_iter()
                    .map(|spec| spec.as_ref().clone())
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            assert_eq!(endpoint, expected_endpoint);
            value = serde_json::to_value(rebuilt).unwrap();
        }
        value["statistics"][0]["blob-metadata"]
            .as_array_mut()
            .unwrap()
            .reverse();
        let changed: TableMetadata = serde_json::from_value(value).unwrap();
        assert_ne!(metadata_identity(&changed).unwrap(), identity);
    }

    #[test]
    fn v1_metadata_identifier_set_is_canonical_in_both_schema_projections() {
        let schema = Schema::builder()
            .with_fields([
                Arc::new(NestedField::required(
                    1,
                    "a",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    2,
                    "b",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .with_identifier_field_ids([2, 1])
            .build()
            .unwrap();
        let metadata = TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "memory:///legacy".into(),
            FormatVersion::V1,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        let expected = metadata_identity(&metadata).unwrap();
        for _ in 0..32 {
            let independently_decoded: TableMetadata =
                serde_json::from_slice(&serde_json::to_vec(&metadata).unwrap()).unwrap();
            assert_eq!(metadata_identity(&independently_decoded).unwrap(), expected);
        }
        let canonical: serde_json::Value = serde_json::from_slice(
            &crate::delete_semantics::canonical_metadata_json(&metadata).unwrap(),
        )
        .unwrap();
        assert_eq!(
            canonical["schema"]["identifier-field-ids"],
            serde_json::json!([1, 2])
        );
        assert_eq!(
            canonical["schemas"][0]["identifier-field-ids"],
            serde_json::json!([1, 2])
        );
    }

    fn assert_cow_data_file_parity(
        ordinary: &crate::manifest::DataFileWithStats,
        cow: &crate::manifest::DataFileWithStats,
    ) {
        assert_eq!(ordinary.path, cow.path);
        assert_eq!(ordinary.size, cow.size);
        assert_eq!(ordinary.record_count, cow.record_count);
        assert_eq!(
            serde_json::to_value(&ordinary.column_stats).unwrap(),
            serde_json::to_value(&cow.column_stats).unwrap()
        );
        assert_eq!(ordinary.partition_spec_id, cow.partition_spec_id);
        assert_eq!(ordinary.partition_key, cow.partition_key);
        assert_eq!(ordinary.partition_values, cow.partition_values);
        assert_eq!(ordinary.manifest_path, cow.manifest_path);
        assert_eq!(ordinary.partition_field_values, cow.partition_field_values);
        assert_eq!(ordinary.first_row_id, cow.first_row_id);
        assert_eq!(ordinary.data_sequence_number, cow.data_sequence_number);
        assert_eq!(ordinary.delete_files, cow.delete_files);
    }

    async fn compare_cow_extraction(table: &Table) -> Vec<crate::manifest::DataFileWithStats> {
        let ordinary = crate::manifest::extract_data_files_with_stats_at(table, 77)
            .await
            .unwrap();
        let cow = crate::manifest::extract_cow_data_files_with_stats_at(table, 77)
            .await
            .unwrap();
        assert_eq!(ordinary.len(), cow.len());
        for (ordinary, cow) in ordinary.iter().zip(&cow) {
            assert_cow_data_file_parity(ordinary, cow);
        }
        cow
    }

    #[tokio::test]
    async fn cow_extraction_matches_partition_statistics_and_attached_delete_facts() {
        use crate::iceberg::spec::Datum;
        let partition = || Struct::from_iter([Some(Literal::string("emea"))]);
        let mut a = file("a", DataContentType::Data, partition());
        a.value_counts(HashMap::from([(1, 10)]))
            .null_value_counts(HashMap::from([(1, 0)]))
            .column_sizes(HashMap::from([(1, 80)]))
            .lower_bounds(HashMap::from([(1, Datum::long(0))]))
            .upper_bounds(HashMap::from([(1, Datum::long(9))]))
            .key_metadata(Some(vec![9; 4096]))
            .split_offsets(Some(vec![0, 128, 512]));
        let b = file("b", DataContentType::Data, partition())
            .build()
            .unwrap();
        let mut position = file("position", DataContentType::PositionDeletes, partition());
        position.referenced_data_file(Some("a".to_owned()));
        let mut equality = file("equality", DataContentType::EqualityDeletes, partition());
        equality.equality_ids(Some(vec![1]));
        let mut vector = file("vector", DataContentType::PositionDeletes, partition());
        vector
            .file_format(DataFileFormat::Puffin)
            .referenced_data_file(Some("b".to_owned()))
            .content_offset(Some(4))
            .content_size_in_bytes(Some(32));
        let table = table(
            vec![
                vec![(a.build().unwrap(), 5), (b, 5)],
                vec![
                    (position.build().unwrap(), 5),
                    (equality.build().unwrap(), 6),
                ],
                vec![(vector.build().unwrap(), 5)],
            ],
            Some(1),
            true,
        )
        .await;
        let files = compare_cow_extraction(&table).await;
        assert_eq!(
            files.iter().map(|f| f.path.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        assert_eq!(files[0].delete_files.len(), 2);
        assert_eq!(files[1].delete_files.len(), 2);
        assert_eq!(files[0].partition_field_values[0].source_column, "region");
        assert_eq!(
            files[0].column_stats.as_ref().unwrap()["id"].lower_bound,
            Some(0_i64.to_le_bytes().to_vec())
        );
    }

    #[tokio::test]
    async fn cow_extraction_keeps_all_live_files_and_inherited_first_row_ids() {
        let mut a = file("before", DataContentType::Data, Struct::empty());
        a.record_count(3);
        let mut b = file("after", DataContentType::Data, Struct::empty());
        b.record_count(7);
        let table = table(
            vec![vec![(a.build().unwrap(), 5), (b.build().unwrap(), 5)]],
            None,
            false,
        )
        .await;
        let files = compare_cow_extraction(&table).await;
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].path, "before");
        assert_eq!(files[1].path, "after");
        assert_eq!(files[0].first_row_id, Some(0));
        assert_eq!(files[1].first_row_id, Some(3));
        assert_eq!(files[0].record_count, Some(3));
        assert_eq!(files[1].record_count, Some(7));
    }

    #[tokio::test]
    async fn cow_extraction_retains_complete_attached_delete_validation_and_error_priority() {
        for (variant, expected) in [
            (0, "older"),
            (1, "deletion vector"),
            (2, "encrypted"),
            (3, "unsupported Iceberg delete format"),
        ] {
            let deletes = match variant {
                0 => vec![(dv("puffin", "a", 4, 2), 4)],
                1 => vec![(dv("puffin", "a", 4, 2), 5), (dv("puffin", "a", 4, 2), 5)],
                2 => vec![(
                    file(
                        "encrypted",
                        DataContentType::PositionDeletes,
                        Struct::empty(),
                    )
                    .referenced_data_file(Some("a".to_owned()))
                    .key_metadata(Some(vec![1]))
                    .build()
                    .unwrap(),
                    5,
                )],
                _ => vec![(
                    file(
                        "unsupported",
                        DataContentType::PositionDeletes,
                        Struct::empty(),
                    )
                    .referenced_data_file(Some("a".to_owned()))
                    .file_format(DataFileFormat::Avro)
                    .build()
                    .unwrap(),
                    5,
                )],
            };
            let table = table(
                vec![vec![(data("a"), 5), (data("other"), 5)], deletes],
                None,
                false,
            )
            .await;
            // Both extractors observe all entries before final projection.
            let ordinary = match crate::manifest::extract_data_files_with_stats_at(&table, 77).await
            {
                Err(error) => error,
                Ok(_) => panic!("ordinary extraction accepted invalid attached delete"),
            };
            let cow = match crate::manifest::extract_cow_data_files_with_stats_at(&table, 77).await
            {
                Err(error) => error,
                Ok(_) => panic!("COW extraction accepted invalid attached delete"),
            };
            assert!(ordinary.contains(expected), "{ordinary}");
            assert_eq!(cow, ordinary);
        }
    }

    #[test]
    fn cow_borrowed_projection_owns_only_final_fields_and_keeps_raw_size_until_finish() {
        use crate::iceberg::spec::Datum;
        let original = file("borrowed", DataContentType::Data, Struct::empty())
            .file_size_in_bytes(u64::MAX)
            .value_counts(HashMap::from([(1, 10)]))
            .lower_bounds(HashMap::from([(1, Datum::long(0))]))
            .key_metadata(Some(vec![7; 8192]))
            .split_offsets(Some((0..1024).collect()))
            .build()
            .unwrap();
        let names = HashMap::from([(1, "id".to_owned())]);
        let projected = CowDataFileProjection::capture(&original, &names, None).unwrap();
        // Enumerating every stored field prevents silently retaining a DataFile
        // or scan metadata Arc in this projection in a later refactor.
        let CowDataFileProjection {
            path,
            file_size_in_bytes,
            record_count,
            column_stats: stats,
        } = projected;
        assert_eq!(path, original.file_path());
        assert_ne!(path.as_ptr(), original.file_path().as_ptr());
        assert_eq!(file_size_in_bytes, u64::MAX);
        assert_eq!(record_count, original.record_count());
        assert_eq!(
            serde_json::to_value(stats).unwrap(),
            serde_json::to_value(column_stats(&original, &names)).unwrap()
        );
        assert_eq!(original.key_metadata().unwrap(), &[7; 8192]);
        assert_eq!(original.split_offsets().unwrap().len(), 1024);
    }

    struct RejectFinalProjection;

    impl ObservedDataFileProjection for RejectFinalProjection {
        type File = ();

        fn capture(
            _df: &DataFile,
            _names: &HashMap<i32, String>,
            _capture: Option<&CaptureScope<'_>>,
        ) -> Result<Self, ReadFailure> {
            Ok(Self)
        }

        fn into_file(
            self,
            _facts: FinalReadFileFacts,
            _names: &HashMap<i32, String>,
        ) -> Result<Self::File, String> {
            Err("final projection canary".to_owned())
        }
    }

    #[tokio::test]
    async fn cow_observation_preserves_delete_failure_before_fallible_final_projection() {
        let valid = table(vec![vec![(data("a"), 5)]], None, false).await;
        let domain =
            mint_read_domain(valid.metadata(), 77, valid.metadata().current_schema()).unwrap();
        let canary = observe_read_files::<RejectFinalProjection>(&valid, domain, None, None)
            .await
            .map_err(|e| e.to_string())
            .unwrap_err();
        assert_eq!(canary, "final projection canary");

        let invalid = table(
            vec![vec![(data("a"), 5)], vec![(dv("puffin", "a", 4, 2), 4)]],
            None,
            false,
        )
        .await;
        let domain =
            mint_read_domain(invalid.metadata(), 77, invalid.metadata().current_schema()).unwrap();
        let actual = observe_read_files::<RejectFinalProjection>(&invalid, domain, None, None)
            .await
            .map_err(|e| e.to_string())
            .unwrap_err();
        assert!(actual.contains("older"), "{actual}");
        assert_ne!(actual, canary);
    }

    fn final_facts_from_read(file: IcebergReadFile) -> FinalReadFileFacts {
        FinalReadFileFacts {
            partition_spec_id: file.partition_spec_id.unwrap(),
            data_sequence_number: file.data_sequence_number.unwrap(),
            partition_values: file.partition_values.unwrap(),
            manifest_path: file.manifest_path.unwrap(),
            first_row_id: file.first_row_id,
            deletes: file.deletes,
        }
    }

    #[tokio::test]
    async fn cow_final_size_conversion_matches_original_without_early_rejection() {
        let table = table(vec![vec![(data("a"), 5)]], None, false).await;
        let mut snapshot = build_read_snapshot_at(&table, 77).await.unwrap();
        let read = snapshot.files.pop().unwrap();
        let original = file("a", DataContentType::Data, Struct::empty())
            .file_size_in_bytes(u64::MAX)
            .build()
            .unwrap();
        let names = HashMap::new();
        let captured = CowDataFileProjection::capture(&original, &names, None).unwrap();
        assert_eq!(captured.file_size_in_bytes, u64::MAX);
        let ordinary = original
            .into_file(final_facts_from_read(read.clone()), &names)
            .unwrap_err();
        let cow = captured
            .into_file(final_facts_from_read(read), &names)
            .unwrap_err();
        assert_eq!(ordinary, "file size too large: a");
        assert_eq!(cow, ordinary);
    }
}
