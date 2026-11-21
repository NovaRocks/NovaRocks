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

//! Materialize the paid manifest entry for one local footer observation.

use novarocks_fs::ParquetMetadataInspection;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorPayloadRetentionGuard,
};

use crate::commit::RegisteredFile;
use crate::file_reader::field_identity::ResolvedParquetFields;
use crate::iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Schema};
use crate::parquet_metrics::analyze_footer_metrics;
use crate::schema_mapping::field_id_for_arrow_field;

use super::listing::CandidateFile;
use super::{commit_error, registration_error, unsupported_scan_carrier};

pub(super) fn build_registered_file(
    candidate: CandidateFile,
    footer: &ParquetMetadataInspection,
    resolved: &ResolvedParquetFields,
    schema: &Schema,
    spec_id: i32,
    author: &ConnectorPayloadRetentionGuard,
) -> Result<(RegisteredFile, Option<i32>), ConnectorError> {
    let metrics_plan = analyze_footer_metrics(footer.raw_metadata(), &resolved.leaves, schema)
        .map_err(metric_error)?;
    let (path, size, path_retention) = candidate.into_parts();
    i64::try_from(size)
        .map_err(|_| unsupported_scan_carrier("Iceberg file size does not fit the scan carrier"))?;
    let backing_bytes = metrics_plan
        .retained_bytes()
        .checked_add(path.path().len())
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg registration entry extent overflow",
            )
        })?;

    // The SDK's mutable builder and its built DataFile coexist. Both hold
    // the path and the metric backing through build().
    let file_retention = author.reserve_owned_bytes(backing_bytes)?;
    let builder_retention = author.reserve_owned_bytes(backing_bytes)?;
    let metrics = metrics_plan.materialize().map_err(metric_error)?;
    i64::try_from(metrics.record_count).map_err(|_| {
        unsupported_scan_carrier("Iceberg file record count does not fit the scan carrier")
    })?;
    let mut required_missing = None;
    for field in schema.as_struct().fields() {
        if !field.required {
            continue;
        }
        let mut bound_root = false;
        for physical in resolved.schema.fields() {
            if field_id_for_arrow_field(physical).map_err(commit_error)? == Some(field.id) {
                bound_root = true;
                break;
            }
        }
        if !bound_root || metrics.null_value_counts.get(&field.id) != Some(&0) {
            required_missing = Some(field.id);
            break;
        }
    }

    let mut builder = DataFileBuilder::default();
    builder
        .content(DataContentType::Data)
        .file_path(path.path().to_owned())
        .file_format(DataFileFormat::Parquet)
        .partition_spec_id(spec_id)
        .record_count(metrics.record_count)
        .file_size_in_bytes(size)
        .column_sizes(metrics.column_sizes)
        .value_counts(metrics.value_counts)
        .null_value_counts(metrics.null_value_counts)
        .lower_bounds(metrics.lower_bounds)
        .upper_bounds(metrics.upper_bounds);
    let file = builder
        .build()
        .map_err(|error| commit_error(error.to_string()))?;
    drop(builder);
    drop(builder_retention);
    let entry = RegisteredFile::try_new(file, path, path_retention, file_retention)
        .map_err(registration_error)?;
    Ok((entry, required_missing))
}

fn metric_error(error: ConnectorError) -> ConnectorError {
    if error.kind() == ConnectorErrorKind::ResourceExhausted {
        error
    } else {
        commit_error(error.message())
    }
}
