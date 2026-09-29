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
    DeleteReadFacts, EntrySequence, EqualityFieldGroup, FieldMetrics, FileMetrics,
    ManifestDeleteObservation, POSITION_FILE_PATH_FIELD_ID, PinnedEndpointFacts, RawDeleteEntry,
    RawDeleteFile, ReadDomain, ReadObservationId, StatisticsPolicy, TypedPartition,
};
use crate::iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, Literal, ManifestStatus, NestedField,
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
            let mut historical = None;
            for schema in metadata.schemas_iter() {
                let Some(source) = schema.field_by_id(field.source_id) else {
                    continue;
                };
                let Type::Primitive(ty) = source.field_type.as_ref() else {
                    return Err(format!(
                        "partition source {} is not primitive in retained schema {}",
                        field.source_id,
                        schema.schema_id()
                    ));
                };
                historical = Some(match historical {
                    None => ty.clone(),
                    Some(previous) => legal_partition_type_join(metadata.format_version(), &previous, ty).ok_or_else(|| format!("partition source {} has incompatible retained types {previous:?} and {ty:?}", field.source_id))?,
                });
            }
            let historical = historical.ok_or_else(|| {
                format!(
                    "partition source {} has no retained schema type evidence",
                    field.source_id
                )
            })?;
            let resolved = match query.field_by_id(field.source_id) {
                Some(source) => {
                    let Type::Primitive(ty) = source.field_type.as_ref() else {
                        return Err(format!(
                            "partition source {} is not primitive in the query schema",
                            field.source_id
                        ));
                    };
                    legal_partition_type_join(metadata.format_version(), &historical, ty)
                        .ok_or_else(|| {
                            format!(
                                "query partition source {} conflicts with retained schema types",
                                field.source_id
                            )
                        })?;
                    ty.clone()
                }
                None => historical.clone(),
            };
            sources.insert(field.source_id, (resolved, historical));
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
    check_read_control(control)?;
    let metadata = table.metadata();
    if domain.endpoint().table_uuid() != metadata.uuid()
        || domain.endpoint().metadata_identity() != metadata_identity(metadata)?
    {
        return Err("Iceberg read domain differs from the pinned table metadata".to_string());
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
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .map_err(|e| format!("load manifest list: {e}"))?;
    let mut seen_manifests = HashSet::new();
    let mut delete_manifests = Vec::new();
    let mut observed_data = Vec::new();
    for manifest_file in manifest_list.entries() {
        check_read_control(control)?;
        if !seen_manifests.insert(manifest_file.manifest_path.as_str()) {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .map_err(|e| format!("load manifest: {e}"))?;
        check_read_control(control)?;
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
            ));
        }
        let partition_type = domain
            .endpoint()
            .partition_type(spec.spec_id())
            .map_err(|e| e.to_string())?;
        let mut delete_entries = Vec::new();
        let mut next_first_row_id = manifest_file
            .first_row_id
            .map(|v| i64::try_from(v).map_err(|_| format!("manifest first_row_id too large: {v}")))
            .transpose()?;
        for entry in manifest.entries() {
            check_read_control(control)?;
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
            let typed_partition = TypedPartition::bind_type(spec, &partition_type, df.partition())
                .map_err(|e| e.to_string())?;
            let metrics = manifest_metrics(df, &schema);
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
                        df.clone(),
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
                            ));
                        }
                    };
                    if df.key_metadata().is_some_and(|k| !k.is_empty()) {
                        return Err(format!(
                            "Iceberg encrypted delete file {} is unsupported",
                            df.file_path()
                        ));
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
            manifest_path: manifest_file.manifest_path.clone().into(),
            entries: delete_entries,
        });
    }
    let observation =
        DeleteObservation::from_manifests(delete_manifests).map_err(|e| e.to_string())?;
    let index = DeleteCandidateIndex::try_new(domain, observation).map_err(|e| e.to_string())?;
    let mut files = Vec::with_capacity(observed_data.len());
    for (df, data, partition_values, manifest_path, first_row_id) in observed_data {
        check_read_control(control)?;
        let logical = index.for_data(&data).map_err(|e| e.to_string())?;
        // This bound is selected entirely from metadata. A large suffix costs
        // no per-member work and stays a compact, conservative view.
        let policy = if data.metrics.fields().is_empty() {
            StatisticsPolicy::Disabled
        } else {
            StatisticsPolicy::MetadataBudget {
                max_candidate_members: MAX_STATS_DELETE_MEMBERS,
                max_field_comparisons: MAX_STATS_FIELD_COMPARISONS,
            }
        };
        let deletes = logical.load_view(policy);
        files.push(IcebergReadFile {
            path: df.file_path().to_string(),
            size: i64::try_from(df.file_size_in_bytes())
                .map_err(|_| format!("file size too large: {}", df.file_path()))?,
            record_count: Some(
                i64::try_from(df.record_count()).map_err(|_| "record count too large")?,
            ),
            column_stats: column_stats(&df, &field_id_to_name),
            partition_spec_id: Some(data.partition().spec_id()),
            partition_key: iceberg_partition_key(&partition_values),
            partition_values: Some(partition_values),
            manifest_path: Some(manifest_path),
            first_row_id,
            data_sequence_number: Some(data.sequence().get()),
            manifest: Arc::new(IcebergDataFileMetadata {
                file_format: df.file_format(),
                split_offsets: df.split_offsets().unwrap_or_default().to_vec(),
                key_metadata: df.key_metadata().unwrap_or_default().to_vec(),
                value_counts: df.value_counts().clone(),
                null_value_counts: df.null_value_counts().clone(),
                nan_value_counts: df.nan_value_counts().clone(),
                lower_bounds: df.lower_bounds().clone(),
                upper_bounds: df.upper_bounds().clone(),
            }),
            deletes,
        });
    }
    Ok(IcebergReadSnapshot {
        snapshot_id: Some(snapshot_id),
        files,
    })
}

fn resolved_partition_values(
    values: &Struct,
    partition_type: &crate::iceberg::spec::StructType,
) -> Struct {
    Struct::from_iter(
        values
            .fields()
            .iter()
            .zip(partition_type.fields())
            .map(|(value, field)| match (value, field.field_type.as_ref()) {
                (
                    Some(Literal::Primitive(PrimitiveLiteral::Int(v))),
                    Type::Primitive(PrimitiveType::Long),
                ) => Some(Literal::Primitive(PrimitiveLiteral::Long(i64::from(*v)))),
                (
                    Some(Literal::Primitive(PrimitiveLiteral::Float(v))),
                    Type::Primitive(PrimitiveType::Double),
                ) => Some(Literal::double(f64::from(v.0))),
                _ => value.clone(),
            }),
    )
}

fn manifest_metrics(df: &DataFile, schema: &Schema) -> FileMetrics {
    let mut ids = std::collections::BTreeSet::new();
    ids.extend(df.value_counts().keys().copied());
    ids.extend(df.null_value_counts().keys().copied());
    ids.extend(df.nan_value_counts().keys().copied());
    ids.extend(df.lower_bounds().keys().copied());
    ids.extend(df.upper_bounds().keys().copied());
    let mut fields = BTreeMap::new();
    for id in ids {
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
    FileMetrics::new(fields)
}

fn column_stats(
    df: &DataFile,
    field_id_to_name: &HashMap<i32, String>,
) -> Option<HashMap<String, IcebergColumnStats>> {
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
        all_ids.extend(null_counts.keys());
        all_ids.extend(value_counts.keys());
        all_ids.extend(col_sizes.keys());
        all_ids.extend(lower.keys());
        all_ids.extend(upper.keys());

        let mut stats_map = HashMap::new();
        for &field_id in &all_ids {
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
        Some(stats_map)
    } else {
        None
    }
}

fn check_read_control(
    control: Option<&dyn novarocks_spi::connector::ConnectorOperationControl>,
) -> Result<(), String> {
    if let Some(control) = control {
        control.check_active().map_err(|e| e.to_string())?;
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
                None,
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
        let mut list =
            ManifestListWriter::v3(io.new_output(list_path).unwrap(), 77, None, 7, Some(0));
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
            .file_io(io)
            .metadata(metadata)
            .identifier(crate::iceberg::TableIdent::from_strs(["db", "t"]).unwrap())
            .disable_cache()
            .build()
            .unwrap()
    }

    fn with_metadata(table: &Table, metadata: TableMetadata) -> Table {
        Table::builder()
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
}
