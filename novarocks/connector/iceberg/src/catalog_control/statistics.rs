// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations
// under the License.

//! Exact-generation Iceberg statistics capability.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::DataType;
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorStatistics, ExternalMutationEffect, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome, StatisticsAccuracy,
    StatisticsCollection, StatisticsCollectionPlan, StatisticsCollectionRequest,
    StatisticsCoverage, StatisticsDataVersion, StatisticsEvidence, StatisticsEvidenceRevision,
    StatisticsMetric, StatisticsMetricState, StatisticsMetricValue, StatisticsMissing,
    StatisticsMissingKind, StatisticsProvenance, StatisticsPublishPreparationRequest,
    StatisticsPublishRequest, StatisticsReadRequest, StatisticsReader, StatisticsReceipt,
    StatisticsReconcileRequest, StatisticsScanColumn,
};

use crate::control_provider::{IcebergControlProvider, IcebergTablePayload};
use crate::manifest::{DataFileWithStats, extract_data_files_with_stats_at};
use crate::reconcile_payload::{
    ICEBERG_STATISTICS_EVIDENCE_VERSION, IcebergStatisticsEvidenceV1, decode_statistics_evidence,
    encode_statistics_evidence,
};
use crate::statistics_codec::{
    decode_provider_statistics, encode_provider_statistics, statistics_data_version,
    statistics_metric_column,
};
use crate::stats_assembler::{
    puffin_path_for_statistics_operation, read_provider_statistics_blob,
    write_puffin_with_provider_statistics,
};
use crate::stats_loader::StatsLoader;
use crate::theta_sketch::ThetaSketchHandle;

const STATISTICS_OPERATION_KIND: &str = "statistics-publish";
const VISIBLE_ROW_ARTIFACT_VERSION: u8 = 1;
const THETA_PARTIAL_WIRE_VERSION: u8 = 1;
const THETA_PARTIAL_WIRE_HEADER_BYTES: usize = 14;
const MAX_THETA_RETAINED_HASHES: usize = 1 << 12;

impl StatisticsReader for IcebergControlProvider {
    fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
        self.descriptor()
    }

    fn incarnation(&self) -> novarocks_spi::connector::ConnectorInstanceIncarnation {
        self.incarnation()
    }

    fn read_statistics(
        &self,
        request: StatisticsReadRequest,
    ) -> Result<StatisticsEvidence, ConnectorError> {
        validate_context(&request.context)?;
        let table = self.table_payload(&request.table)?;
        let table_info = base_table_info(&table, "statistics read")?;
        let expected = pinned_data_version(table_info)?;
        if request.data_version != expected {
            return Err(invalid(
                "Iceberg statistics request does not match its resolved table pin",
            ));
        }
        let snapshot_id = table_info.current_snapshot_id.ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::NotFound,
                "Iceberg table has no current snapshot for statistics",
            )
        })?;
        let physical = self
            .runtime()
            .load_table(&table.namespace, &table.table)
            .map_err(unavailable)?;
        let metadata = physical.table.metadata();
        ensure_current_version(metadata, &expected)?;
        let statistics_path = metadata
            .statistics_for_snapshot(snapshot_id)
            .map(|file| file.statistics_path.as_str());
        let evidence_path = statistics_path.unwrap_or("none");
        let revision = StatisticsEvidenceRevision::try_new(Bytes::from(format!(
            "iceberg/v1/{}/{snapshot_id}/{evidence_path}",
            table_info
                .table_uuid
                .as_deref()
                .expect("pinned data version requires a table UUID")
        )))?;

        if let Some(statistics_path) = statistics_path {
            let file_io = physical.table.file_io().clone();
            let path = statistics_path.to_string();
            let artifact = self
                .runtime()
                .resources()
                .catalog_runtime()
                .block_on(async move { read_provider_statistics_blob(&file_io, &path).await })
                .map_err(unavailable)?
                .map_err(corrupt)?;
            if let Some(artifact) = artifact {
                return Ok(StatisticsEvidence {
                    data_version: expected.clone(),
                    evidence_revision: revision,
                    coverage: StatisticsCoverage::Full,
                    accuracy: StatisticsAccuracy::Exact,
                    interval: None,
                    provenance: StatisticsProvenance::ProviderArtifact,
                    metrics: decode_provider_statistics(&artifact, &expected, &request.metrics)?,
                });
            }
        }

        let table_for_files = physical.table.clone();
        let files = self
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move {
                extract_data_files_with_stats_at(&table_for_files, snapshot_id).await
            })
            .map_err(unavailable)?
            .map_err(unavailable)?;
        let table_for_ndv = physical.table.clone();
        let ndv = self
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move {
                StatsLoader::load_ndv(
                    table_for_ndv.metadata(),
                    snapshot_id,
                    table_for_ndv.file_io(),
                )
                .await
            })
            .map_err(unavailable)?;
        let arrow_schema = crate::iceberg::arrow::schema_to_arrow_schema(metadata.current_schema())
            .map_err(|error| corrupt(format!("convert Iceberg statistics schema: {error}")))?;
        let field_ids = metadata
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| (field.name.to_ascii_lowercase(), field.id))
            .collect::<HashMap<_, _>>();
        let data_types = arrow_schema
            .fields()
            .iter()
            .map(|field| (field.name().to_ascii_lowercase(), field.data_type().clone()))
            .collect::<HashMap<_, _>>();
        let manifest_complete = files
            .iter()
            .all(|file| file.record_count.is_some() && file.delete_files.is_empty());
        let metrics = request
            .metrics
            .metrics()
            .iter()
            .cloned()
            .map(|metric| {
                let state = manifest_metric(&metric, &files, &data_types, &field_ids, &ndv);
                (metric, state)
            })
            .collect();
        Ok(StatisticsEvidence {
            data_version: expected,
            evidence_revision: revision,
            coverage: if manifest_complete {
                StatisticsCoverage::Full
            } else {
                StatisticsCoverage::Subset
            },
            accuracy: if manifest_complete {
                StatisticsAccuracy::Exact
            } else {
                StatisticsAccuracy::Approximate
            },
            interval: None,
            provenance: StatisticsProvenance::Manifest,
            metrics,
        })
    }
}

impl StatisticsCollection for IcebergControlProvider {
    fn descriptor(&self) -> &novarocks_spi::connector::ConnectorInstanceDescriptor {
        self.descriptor()
    }

    fn incarnation(&self) -> novarocks_spi::connector::ConnectorInstanceIncarnation {
        self.incarnation()
    }

    fn prepare_collection(
        &self,
        request: StatisticsCollectionRequest,
    ) -> Result<StatisticsCollectionPlan, ConnectorError> {
        validate_context(&request.context)?;
        let table = self.table_payload(&request.table)?;
        let table_info = base_table_info(&table, "statistics collection")?;
        let expected = pinned_data_version(table_info)?;
        if request.data_version != expected {
            return Err(invalid(
                "Iceberg statistics collection does not match its resolved table pin",
            ));
        }
        let mut projection = request
            .metrics
            .metrics()
            .iter()
            .filter_map(statistics_metric_column)
            .map(|column| {
                table_info
                    .schema
                    .fields
                    .iter()
                    .position(|field| field.name.eq_ignore_ascii_case(column))
                    .ok_or_else(|| {
                        invalid(format!(
                            "Iceberg statistics column `{column}` is absent from the pinned schema"
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        projection.sort_unstable();
        projection.dedup();
        let revision = StatisticsEvidenceRevision::try_new(Bytes::from(format!(
            "iceberg/v1/{}/{}/collection/{}",
            table_info
                .table_uuid
                .as_deref()
                .expect("pinned data version requires a table UUID"),
            table_info.current_snapshot_id.unwrap_or_default(),
            uuid::Uuid::from_bytes(request.operation_id.to_bytes())
        )))?;
        let columns = statistics_scan_layout(&table, &projection)?;
        let provider_payload = request.table.payload().clone();
        StatisticsCollectionPlan::try_new(
            request.table,
            request.data_version,
            revision,
            request.metrics,
            columns,
            provider_payload,
        )
    }

    fn prepare_publish(
        &self,
        request: StatisticsPublishPreparationRequest,
    ) -> Result<ExternalMutationEvidence, ConnectorError> {
        validate_context(&request.context)?;
        let table = self.table_payload(&request.table)?;
        let info = base_table_info(&table, "statistics publication")?;
        let expected = pinned_data_version(info)?;
        if request.result.evidence.data_version != expected {
            return Err(invalid(
                "Iceberg statistics publication does not match its resolved table pin",
            ));
        }
        let physical = self
            .runtime()
            .load_table(&table.namespace, &table.table)
            .map_err(unavailable)?;
        ensure_current_version(physical.table.metadata(), &expected)?;
        let snapshot_id = physical
            .table
            .metadata()
            .current_snapshot_id()
            .ok_or_else(|| invalid("cannot publish statistics for a table without a snapshot"))?;
        let path = puffin_path_for_statistics_operation(
            physical.table.metadata(),
            snapshot_id,
            request.operation_id.to_bytes(),
        );
        statistics_evidence(self, request.operation_id, &table, &expected, &path)
    }

    fn publish_statistics(
        &self,
        request: StatisticsPublishRequest,
    ) -> Result<ExternalMutationOutcome<StatisticsReceipt>, ConnectorError> {
        validate_context(&request.context)?;
        let table = self.table_payload(&request.table)?;
        let info = match base_table_info(&table, "statistics publication") {
            Ok(info) => info,
            Err(error) => return Ok(known_uncommitted(error)),
        };
        let expected = pinned_data_version(info)?;
        if request.result.evidence.data_version != expected
            || request.result.evidence.coverage != StatisticsCoverage::Full
            || request.result.evidence.accuracy != StatisticsAccuracy::Exact
            || request.result.evidence.provenance != StatisticsProvenance::VisibleRows
        {
            return Ok(known_uncommitted(invalid(
                "Iceberg statistics publication requires Full Exact visible-row evidence for the pinned table",
            )));
        }
        let provider_statistics = encode_provider_statistics(&request.result.evidence)?;
        let (artifact_version, theta) =
            decode_visible_row_artifact(request.result.provider_payload())?;
        if artifact_version != expected {
            return Ok(known_uncommitted(invalid(
                "statistics collection artifact does not match its resolved table pin",
            )));
        }

        let physical = self
            .runtime()
            .load_table(&table.namespace, &table.table)
            .map_err(unavailable)?;
        let metadata = physical.table.metadata();
        if let Err(error) = ensure_current_version(metadata, &expected) {
            return Ok(known_uncommitted(error));
        }
        let snapshot = metadata.current_snapshot().ok_or_else(|| {
            invalid("cannot publish statistics for a table without a current snapshot")
        })?;
        let snapshot_id = snapshot.snapshot_id();
        let sequence_number = snapshot.sequence_number();
        let field_ids = metadata
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| (field.name.to_ascii_lowercase(), field.id))
            .collect::<HashMap<_, _>>();
        let mut sketches = HashMap::new();
        for (column, sketch) in theta {
            let field_id = field_ids.get(&column.to_ascii_lowercase()).ok_or_else(|| {
                invalid(format!(
                    "statistics artifact column `{column}` is absent from the pinned Iceberg schema"
                ))
            })?;
            sketches.insert(*field_id, sketch);
        }
        let path = puffin_path_for_statistics_operation(
            metadata,
            snapshot_id,
            request.operation_id.to_bytes(),
        );
        let expected_evidence =
            statistics_evidence(self, request.operation_id, &table, &expected, &path)?;
        if request.evidence != expected_evidence {
            return Ok(known_uncommitted(invalid(
                "Iceberg statistics evidence does not match its pinned operation",
            )));
        }

        let file_io = physical.table.file_io().clone();
        let path_for_write = path.clone();
        let written = self
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move {
                write_puffin_with_provider_statistics(
                    &file_io,
                    &path_for_write,
                    snapshot_id,
                    sequence_number,
                    &sketches,
                    Some(&provider_statistics),
                )
                .await
            })
            .map_err(unavailable)?
            .map_err(unavailable)?;
        let Some(statistics_file) = written else {
            return statistics_receipt(
                self,
                request.operation_id,
                expected,
                request.result.evidence.evidence_revision,
                Bytes::from(path),
                ExternalMutationEffect::NoOp,
            );
        };
        let table_for_commit = physical.table.clone();
        let catalog = Arc::clone(self.runtime().catalog());
        let committed = self
            .runtime()
            .resources()
            .catalog_runtime()
            .block_on(async move {
                crate::commit::statistics::commit_statistics_file(
                    &table_for_commit,
                    catalog.as_ref(),
                    statistics_file,
                )
                .await
            });
        match committed {
            Ok(Ok(())) => {
                self.runtime()
                    .control_state()
                    .invalidate_table(&table.namespace, &table.table);
                statistics_receipt(
                    self,
                    request.operation_id,
                    expected,
                    request.result.evidence.evidence_revision,
                    Bytes::from(path),
                    ExternalMutationEffect::Applied,
                )
            }
            Ok(Err(error)) | Err(error) => Ok(ExternalMutationOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Internal,
                    format!("commit Iceberg statistics: {error}"),
                ),
                evidence: request.evidence,
            }),
        }
    }

    fn reconcile_statistics(
        &self,
        request: StatisticsReconcileRequest,
    ) -> Result<ExternalMutationOutcome<StatisticsReceipt>, ConnectorError> {
        validate_context(&request.context)?;
        if request.evidence.descriptor() != self.descriptor()
            || request.evidence.incarnation() != self.incarnation()
            || request.evidence.schema_version() != ICEBERG_STATISTICS_EVIDENCE_VERSION
            || request.evidence.operation_kind() != STATISTICS_OPERATION_KIND
        {
            return Err(invalid(
                "Iceberg statistics evidence does not match this exact generation",
            ));
        }
        let evidence = decode_statistics_evidence(request.evidence.provider_payload())
            .map_err(|error| invalid(format!("decode Iceberg statistics evidence: {error}")))?;
        let expected = StatisticsDataVersion::try_new(Bytes::from(evidence.data_version))?;
        if !self
            .runtime()
            .table_exists(&evidence.namespace, &evidence.table)
            .map_err(unavailable)?
        {
            return Ok(known_uncommitted(ConnectorError::new(
                ConnectorErrorKind::NotFound,
                "Iceberg table disappeared before statistics publication reconciled",
            )));
        }
        self.runtime()
            .control_state()
            .invalidate_table(&evidence.namespace, &evidence.table);
        let physical = self
            .runtime()
            .load_table(&evidence.namespace, &evidence.table)
            .map_err(unavailable)?;
        if let Err(error) = ensure_current_version(physical.table.metadata(), &expected) {
            return Ok(known_uncommitted(error));
        }
        if physical
            .table
            .metadata()
            .statistics_iter()
            .any(|file| file.statistics_path == evidence.statistics_path)
        {
            return statistics_receipt(
                self,
                request.evidence.operation_id(),
                expected,
                StatisticsEvidenceRevision::try_new(Bytes::from(format!(
                    "iceberg/statistics/v1/{}",
                    evidence.statistics_path
                )))?,
                Bytes::from(evidence.statistics_path),
                ExternalMutationEffect::Applied,
            );
        }
        Ok(known_uncommitted(invalid(
            "Iceberg statistics artifact is not registered in table metadata",
        )))
    }
}

impl ConnectorStatistics for IcebergControlProvider {
    fn collection(&self) -> Option<&dyn StatisticsCollection> {
        Some(self)
    }
}

fn base_table_info<'a>(
    table: &'a IcebergTablePayload,
    operation: &str,
) -> Result<&'a crate::scan_model::IcebergTableInfo, ConnectorError> {
    if table.metadata_table_type.is_some() {
        return Err(invalid(format!(
            "Iceberg {operation} requires a base table handle"
        )));
    }
    table.table_info.as_ref().ok_or_else(|| {
        invalid(format!(
            "Iceberg {operation} requires a resolved base table payload"
        ))
    })
}

fn pinned_data_version(
    info: &crate::scan_model::IcebergTableInfo,
) -> Result<StatisticsDataVersion, ConnectorError> {
    statistics_data_version(
        info.table_uuid
            .as_deref()
            .ok_or_else(|| corrupt("Iceberg table payload is missing its table UUID"))?,
        info.current_snapshot_id,
    )
}

fn ensure_current_version(
    metadata: &crate::iceberg::spec::TableMetadata,
    expected: &StatisticsDataVersion,
) -> Result<(), ConnectorError> {
    let current =
        statistics_data_version(&metadata.uuid().to_string(), metadata.current_snapshot_id())?;
    if &current != expected {
        return Err(invalid(
            "Iceberg table changed while statistics evidence was being processed",
        ));
    }
    Ok(())
}

fn statistics_scan_layout(
    table: &IcebergTablePayload,
    projection: &[usize],
) -> Result<Vec<StatisticsScanColumn>, ConnectorError> {
    let info = base_table_info(table, "statistics collection")?;
    let serialized = info.serialized_metadata.as_deref().ok_or_else(|| {
        corrupt("Iceberg statistics collection payload is missing serialized pinned metadata")
    })?;
    let metadata: crate::iceberg::spec::TableMetadata = serde_json::from_str(serialized)
        .map_err(|error| corrupt(format!("decode pinned Iceberg metadata: {error}")))?;
    if metadata.current_schema_id() != info.schema_id {
        return Err(corrupt(
            "Iceberg statistics metadata does not match its pinned schema ID",
        ));
    }
    let schema = crate::iceberg::arrow::schema_to_arrow_schema(metadata.current_schema())
        .map_err(|error| corrupt(format!("convert pinned Iceberg schema: {error}")))?;
    projection
        .iter()
        .map(|&ordinal| {
            let field = schema.fields().get(ordinal).ok_or_else(|| {
                invalid(format!(
                    "Iceberg statistics projection index {ordinal} is outside the pinned schema"
                ))
            })?;
            let data_type = match table
                .logical_type_columns
                .get(&field.name().to_ascii_lowercase())
                .map(String::as_str)
            {
                Some("bitmap") | Some("hll") => DataType::Binary,
                _ => field.data_type().clone(),
            };
            StatisticsScanColumn::try_new(
                ordinal,
                Arc::<str>::from(field.name().as_str()),
                data_type,
                field.is_nullable(),
            )
        })
        .collect()
}

fn manifest_metric(
    metric: &StatisticsMetric,
    files: &[DataFileWithStats],
    data_types: &HashMap<String, DataType>,
    field_ids: &HashMap<String, i32>,
    ndv: &HashMap<i32, f64>,
) -> StatisticsMetricState {
    match metric {
        StatisticsMetric::RowCount => files
            .iter()
            .try_fold(0_u64, |total, file| {
                total.checked_add(u64::try_from(file.record_count?).ok()?)
            })
            .map(|value| StatisticsMetricState::Available(StatisticsMetricValue::U64(value)))
            .unwrap_or_else(|| incomplete("Iceberg manifest does not report every row count")),
        StatisticsMetric::NullCount { column } => files
            .iter()
            .try_fold(0_u64, |total, file| {
                let count = column_stats(file, column)?.null_count?;
                total.checked_add(u64::try_from(count).ok()?)
            })
            .map(|value| StatisticsMetricState::Available(StatisticsMetricValue::U64(value)))
            .unwrap_or_else(|| {
                incomplete(format!(
                    "Iceberg manifest does not report an exact null count for `{column}`"
                ))
            }),
        StatisticsMetric::AverageSize { column } => {
            let total_rows = files.iter().try_fold(0_u64, |total, file| {
                total.checked_add(u64::try_from(file.record_count?).ok()?)
            });
            let total_size = files.iter().try_fold(0_u64, |total, file| {
                total.checked_add(u64::try_from(column_stats(file, column)?.column_size?).ok()?)
            });
            match (total_rows, total_size) {
                (Some(rows), Some(size)) => {
                    StatisticsMetricState::Available(StatisticsMetricValue::F64(if rows == 0 {
                        0.0
                    } else {
                        size as f64 / rows as f64
                    }))
                }
                _ => missing_column(column),
            }
        }
        StatisticsMetric::Minimum { column } | StatisticsMetric::Maximum { column } => {
            let lower = matches!(metric, StatisticsMetric::Minimum { .. });
            let Some(data_type) = data_types.get(&column.to_ascii_lowercase()) else {
                return missing_column(column);
            };
            let values = files.iter().map(|file| {
                let stats = column_stats(file, column)?;
                let bytes = if lower {
                    stats.lower_bound.as_deref()?
                } else {
                    stats.upper_bound.as_deref()?
                };
                decode_bound_to_f64(bytes, data_type).filter(|value| value.is_finite())
            });
            let reduced = values.fold(Some(None), |state, value| match (state, value) {
                (Some(None), Some(value)) => Some(Some(value)),
                (Some(Some(current)), Some(value)) => Some(Some(if lower {
                    current.min(value)
                } else {
                    current.max(value)
                })),
                _ => None,
            });
            match reduced.flatten() {
                Some(value) => StatisticsMetricState::Available(StatisticsMetricValue::F64(value)),
                None => missing_column(column),
            }
        }
        StatisticsMetric::ThetaNdv { column } => field_ids
            .get(&column.to_ascii_lowercase())
            .and_then(|field_id| ndv.get(field_id))
            .copied()
            .filter(|value| value.is_finite() && *value >= 0.0)
            .map(|value| StatisticsMetricState::Available(StatisticsMetricValue::F64(value)))
            .unwrap_or_else(|| missing_column(column)),
    }
}

fn column_stats<'a>(
    file: &'a DataFileWithStats,
    column: &str,
) -> Option<&'a crate::scan_model::IcebergColumnStats> {
    file.column_stats
        .as_ref()?
        .iter()
        .find_map(|(name, stats)| name.eq_ignore_ascii_case(column).then_some(stats))
}

fn decode_bound_to_f64(bytes: &[u8], data_type: &DataType) -> Option<f64> {
    match data_type {
        DataType::Boolean => match bytes {
            [0] => Some(0.0),
            [1] => Some(1.0),
            _ => None,
        },
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Date32
        | DataType::Time32(_) => Some(i32::from_le_bytes(bytes.try_into().ok()?) as f64),
        DataType::Int64
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Time64(_)
        | DataType::Duration(_) => Some(i64::from_le_bytes(bytes.try_into().ok()?) as f64),
        DataType::Float32 => Some(f32::from_le_bytes(bytes.try_into().ok()?) as f64),
        DataType::Float64 => Some(f64::from_le_bytes(bytes.try_into().ok()?)),
        DataType::Decimal128(_, scale) | DataType::Decimal256(_, scale) => {
            if bytes.is_empty() || bytes.len() > 16 {
                return None;
            }
            let mut padded = [if bytes[0] & 0x80 != 0 { 0xff } else { 0 }; 16];
            padded[16 - bytes.len()..].copy_from_slice(bytes);
            Some(i128::from_be_bytes(padded) as f64 / 10_f64.powi(*scale as i32))
        }
        _ => None,
    }
}

fn decode_visible_row_artifact(
    bytes: &[u8],
) -> Result<(StatisticsDataVersion, BTreeMap<String, ThetaSketchHandle>), ConnectorError> {
    let mut cursor = 0usize;
    let version = take(bytes, &mut cursor, 1)?[0];
    if version != VISIBLE_ROW_ARTIFACT_VERSION {
        return Err(corrupt(
            "statistics visible-row artifact has an unsupported version",
        ));
    }
    let data_version_len = u16::from_be_bytes(
        take(bytes, &mut cursor, 2)?
            .try_into()
            .expect("fixed field width"),
    ) as usize;
    let data_version = StatisticsDataVersion::try_new(Bytes::copy_from_slice(take(
        bytes,
        &mut cursor,
        data_version_len,
    )?))?;
    let count = u16::from_be_bytes(
        take(bytes, &mut cursor, 2)?
            .try_into()
            .expect("fixed field width"),
    ) as usize;
    let mut sketches = BTreeMap::new();
    for _ in 0..count {
        let name_len = u16::from_be_bytes(
            take(bytes, &mut cursor, 2)?
                .try_into()
                .expect("fixed field width"),
        ) as usize;
        let name = std::str::from_utf8(take(bytes, &mut cursor, name_len)?)
            .map_err(|_| corrupt("statistics artifact column is not UTF-8"))?;
        if name.is_empty() {
            return Err(corrupt("statistics artifact column name is empty"));
        }
        let sketch_len = u32::from_be_bytes(
            take(bytes, &mut cursor, 4)?
                .try_into()
                .expect("fixed field width"),
        ) as usize;
        let sketch = decode_theta_partial(take(bytes, &mut cursor, sketch_len)?)?;
        if sketches.insert(name.to_string(), sketch).is_some() {
            return Err(corrupt(
                "statistics artifact contains a duplicate Theta column",
            ));
        }
    }
    if cursor != bytes.len() {
        return Err(corrupt(
            "statistics visible-row artifact has trailing bytes",
        ));
    }
    Ok((data_version, sketches))
}

fn decode_theta_partial(bytes: &[u8]) -> Result<ThetaSketchHandle, ConnectorError> {
    if bytes.len() < THETA_PARTIAL_WIRE_HEADER_BYTES || bytes[0] != THETA_PARTIAL_WIRE_VERSION {
        return Err(corrupt("statistics Theta state is invalid"));
    }
    let lg_k = bytes[1];
    if !(5..=12).contains(&lg_k) {
        return Err(corrupt("statistics Theta state has an invalid lg_k"));
    }
    let theta = u64::from_be_bytes(bytes[2..10].try_into().expect("fixed field width"));
    let count = u32::from_be_bytes(bytes[10..14].try_into().expect("fixed field width")) as usize;
    if count > MAX_THETA_RETAINED_HASHES
        || bytes.len() != THETA_PARTIAL_WIRE_HEADER_BYTES + count * 8
    {
        return Err(corrupt("statistics Theta state has an invalid length"));
    }
    let hashes = bytes[THETA_PARTIAL_WIRE_HEADER_BYTES..]
        .chunks_exact(8)
        .map(|chunk| u64::from_be_bytes(chunk.try_into().expect("exact chunks")))
        .collect::<Vec<_>>();
    ThetaSketchHandle::from_compact_parts(lg_k, theta, hashes).map_err(corrupt)
}

fn take<'a>(bytes: &'a [u8], cursor: &mut usize, count: usize) -> Result<&'a [u8], ConnectorError> {
    let end = cursor
        .checked_add(count)
        .ok_or_else(|| corrupt("statistics artifact length overflow"))?;
    let value = bytes
        .get(*cursor..end)
        .ok_or_else(|| corrupt("statistics artifact is truncated"))?;
    *cursor = end;
    Ok(value)
}

fn statistics_evidence(
    provider: &IcebergControlProvider,
    operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
    table: &IcebergTablePayload,
    data_version: &StatisticsDataVersion,
    path: &str,
) -> Result<ExternalMutationEvidence, ConnectorError> {
    let payload = encode_statistics_evidence(&IcebergStatisticsEvidenceV1 {
        version: ICEBERG_STATISTICS_EVIDENCE_VERSION,
        namespace: table.namespace.clone(),
        table: table.table.clone(),
        data_version: data_version.as_bytes().to_vec(),
        statistics_path: path.to_string(),
    })
    .map_err(internal)?;
    ExternalMutationEvidence::try_new(
        ICEBERG_STATISTICS_EVIDENCE_VERSION,
        provider.descriptor().clone(),
        provider.incarnation(),
        operation_id,
        STATISTICS_OPERATION_KIND,
        Bytes::from(payload),
    )
}

fn statistics_receipt(
    provider: &IcebergControlProvider,
    operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
    data_version: StatisticsDataVersion,
    revision: StatisticsEvidenceRevision,
    payload: Bytes,
    effect: ExternalMutationEffect,
) -> Result<ExternalMutationOutcome<StatisticsReceipt>, ConnectorError> {
    Ok(ExternalMutationOutcome::KnownCommitted {
        effect,
        receipt: StatisticsReceipt::try_new(
            provider.descriptor().clone(),
            provider.incarnation(),
            operation_id,
            data_version,
            revision,
            payload,
        )?,
        finalization: ExternalMutationFinalization::Complete,
    })
}

fn known_uncommitted(error: ConnectorError) -> ExternalMutationOutcome<StatisticsReceipt> {
    ExternalMutationOutcome::KnownUncommitted {
        failure: ConnectorMutationFailure::new(failure_kind(error.kind()), error.to_string()),
    }
}

fn failure_kind(kind: ConnectorErrorKind) -> ConnectorMutationFailureKind {
    match kind {
        ConnectorErrorKind::InvalidRequest => ConnectorMutationFailureKind::InvalidRequest,
        ConnectorErrorKind::NotFound => ConnectorMutationFailureKind::NotFound,
        ConnectorErrorKind::PermissionDenied => ConnectorMutationFailureKind::PermissionDenied,
        ConnectorErrorKind::Unsupported => ConnectorMutationFailureKind::Unsupported,
        ConnectorErrorKind::Cancelled => ConnectorMutationFailureKind::Cancelled,
        ConnectorErrorKind::DeadlineExceeded => ConnectorMutationFailureKind::DeadlineExceeded,
        ConnectorErrorKind::ResourceExhausted => ConnectorMutationFailureKind::ResourceExhausted,
        ConnectorErrorKind::Unavailable => ConnectorMutationFailureKind::Unavailable,
        ConnectorErrorKind::CorruptData => ConnectorMutationFailureKind::CorruptData,
        ConnectorErrorKind::Internal => ConnectorMutationFailureKind::Internal,
    }
}

fn validate_context(
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    if context.cancellation().is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "connector request was cancelled",
        ));
    }
    if Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "connector request deadline elapsed",
        ));
    }
    Ok(())
}

fn missing_column(column: &str) -> StatisticsMetricState {
    StatisticsMetricState::Missing(StatisticsMissing {
        kind: StatisticsMissingKind::NotCollected,
        message: Arc::from(format!(
            "statistics for column `{column}` are not collected"
        )),
    })
}

fn incomplete(message: impl Into<Arc<str>>) -> StatisticsMetricState {
    StatisticsMetricState::Missing(StatisticsMissing {
        kind: StatisticsMissingKind::IncompleteEvidence,
        message: message.into(),
    })
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message.into())
}

fn unavailable(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, message.into())
}

fn internal(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Internal, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorInstanceIncarnation, ConnectorMutationOperationId, ConnectorProviderId,
        ConnectorRequestContext,
    };

    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::control_runtime::IcebergControlRuntime;
    use crate::resources::IcebergControlResources;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
            1024,
            4096,
        )
        .expect("context")
    }

    fn provider() -> (
        tokio::runtime::Runtime,
        tempfile::TempDir,
        IcebergControlProvider,
    ) {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(
                executor.handle().clone(),
            )),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(
                executor.handle().clone(),
            )),
        );
        let runtime = Arc::new(
            IcebergControlRuntime::try_new(
                IcebergCatalogControlState::new(configuration),
                IcebergControlResources::new(binding, executor.handle().clone()),
            )
            .expect("control runtime"),
        );
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            instance_id: ConnectorInstanceId::parse("ice").expect("instance"),
        };
        let provider = IcebergControlProvider::new(
            descriptor,
            ConnectorInstanceIncarnation::from_bytes([4; 16]),
            runtime,
        );
        (executor, warehouse, provider)
    }

    fn table_payload() -> IcebergTablePayload {
        IcebergTablePayload {
            namespace: "db".to_string(),
            table: "t".to_string(),
            table_info: None,
            metadata_columns: Vec::new(),
            metadata_table_type: None,
            prepared_files: Vec::new(),
            explicit_files: None,
            row_mutation_frozen_source: false,
            logical_type_columns: BTreeMap::new(),
            hidden_columns: Vec::new(),
        }
    }

    #[test]
    fn rejects_non_canonical_theta_state() {
        let mut bytes = vec![THETA_PARTIAL_WIRE_VERSION, 12];
        bytes.extend_from_slice(&u64::MAX.to_be_bytes());
        bytes.extend_from_slice(&2_u32.to_be_bytes());
        bytes.extend_from_slice(&9_u64.to_be_bytes());
        bytes.extend_from_slice(&9_u64.to_be_bytes());
        assert!(decode_theta_partial(&bytes).is_err());
    }

    #[test]
    fn manifest_row_count_requires_every_file() {
        let files = vec![DataFileWithStats {
            path: "data.parquet".to_string(),
            size: 1,
            record_count: None,
            column_stats: None,
            partition_spec_id: None,
            partition_key: None,
            partition_values: None,
            manifest_path: None,
            partition_field_values: Vec::new(),
            first_row_id: None,
            data_sequence_number: None,
            delete_files: Vec::new(),
        }];
        assert!(matches!(
            manifest_metric(
                &StatisticsMetric::RowCount,
                &files,
                &HashMap::new(),
                &HashMap::new(),
                &HashMap::new(),
            ),
            StatisticsMetricState::Missing(_)
        ));
    }

    #[test]
    fn response_loss_evidence_is_deterministic_and_exact_generation_bound() {
        let (_executor, _warehouse, provider) = provider();
        let operation_id = ConnectorMutationOperationId::new();
        let data_version =
            StatisticsDataVersion::try_new(Bytes::from_static(b"table-v1")).expect("data version");
        let first = statistics_evidence(
            &provider,
            operation_id,
            &table_payload(),
            &data_version,
            "s3://warehouse/db/t/metadata/stats.puffin",
        )
        .expect("evidence");
        let second = statistics_evidence(
            &provider,
            operation_id,
            &table_payload(),
            &data_version,
            "s3://warehouse/db/t/metadata/stats.puffin",
        )
        .expect("evidence replay");
        assert_eq!(first, second);
        let decoded = decode_statistics_evidence(first.provider_payload()).expect("decode");
        assert_eq!(decoded.namespace, "db");
        assert_eq!(decoded.table, "t");
        assert_eq!(
            decoded.statistics_path,
            "s3://warehouse/db/t/metadata/stats.puffin"
        );

        let foreign = ExternalMutationEvidence::try_new(
            ICEBERG_STATISTICS_EVIDENCE_VERSION,
            provider.descriptor().clone(),
            ConnectorInstanceIncarnation::from_bytes([5; 16]),
            operation_id,
            STATISTICS_OPERATION_KIND,
            first.provider_payload().clone(),
        )
        .expect("foreign evidence");
        let error = provider
            .reconcile_statistics(StatisticsReconcileRequest {
                evidence: foreign,
                context: context(),
            })
            .expect_err("foreign generation must be rejected before table access");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(error.to_string().contains("exact generation"));
    }

    #[test]
    fn malformed_reconcile_evidence_fails_before_catalog_access() {
        let (_executor, _warehouse, provider) = provider();
        let evidence = ExternalMutationEvidence::try_new(
            ICEBERG_STATISTICS_EVIDENCE_VERSION,
            provider.descriptor().clone(),
            provider.incarnation(),
            ConnectorMutationOperationId::new(),
            STATISTICS_OPERATION_KIND,
            Bytes::from_static(b"not-json"),
        )
        .expect("evidence envelope");
        let error = provider
            .reconcile_statistics(StatisticsReconcileRequest {
                evidence,
                context: context(),
            })
            .expect_err("malformed evidence must fail closed");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(
            error
                .to_string()
                .contains("decode Iceberg statistics evidence")
        );
    }
}
