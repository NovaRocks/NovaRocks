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

use crate::iceberg::spec::{Datum, PrimitiveType, Type};
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;

use crate::iceberg::spec::{Literal, PartitionSpecRef, PrimitiveLiteral, Struct};
use base64::Engine;

use crate::delete_file::IcebergFileContent;

use super::types::WrittenFile;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IcebergColumnStats {
    pub column_sizes: BTreeMap<i32, i64>,
    pub value_counts: BTreeMap<i32, i64>,
    pub null_value_counts: BTreeMap<i32, i64>,
    pub nan_value_counts: BTreeMap<i32, i64>,
    pub lower_bounds: BTreeMap<i32, Vec<u8>>,
    pub upper_bounds: BTreeMap<i32, Vec<u8>>,
}

impl IcebergColumnStats {
    pub fn is_empty(&self) -> bool {
        self.column_sizes.is_empty()
            && self.value_counts.is_empty()
            && self.null_value_counts.is_empty()
            && self.nan_value_counts.is_empty()
            && self.lower_bounds.is_empty()
            && self.upper_bounds.is_empty()
    }
}

#[derive(Clone, Debug)]
pub struct IcebergPartitionReport {
    pub partition_path: String,
    pub null_fingerprint: String,
    pub partition_spec_id: i32,
    pub partition_values: Struct,
}

#[derive(Clone, Debug)]
pub struct IcebergWrittenFileReport {
    pub path: String,
    pub format: String,
    pub content: IcebergFileContent,
    pub record_count: i64,
    pub file_size_in_bytes: i64,
    pub partition: IcebergPartitionReport,
    pub split_offsets: Option<Vec<i64>>,
    pub column_stats: Option<IcebergColumnStats>,
    pub referenced_data_file: Option<String>,
    pub first_row_id: Option<i64>,
    pub equality_ids: Option<Vec<i32>>,
    pub key_metadata: Option<Vec<u8>>,
    pub content_offset: Option<i64>,
    pub content_size_in_bytes: Option<i64>,
    pub cardinality: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct IcebergWriterReport {
    pub file: IcebergWrittenFileReport,
    pub is_overwrite: Option<bool>,
    pub is_rewrite: Option<bool>,
}

pub fn writer_report_from_written_file(
    file: &WrittenFile,
    metadata: &crate::iceberg::spec::TableMetadata,
) -> Result<IcebergWriterReport, String> {
    let partition_spec = metadata
        .partition_spec_by_id(file.partition_spec_id)
        .ok_or_else(|| {
            format!(
                "iceberg written file `{}` references unknown partition spec id {}",
                file.path, file.partition_spec_id
            )
        })?;
    let (partition_path, null_fingerprint) =
        partition_path_from_struct(&file.partition_values, partition_spec)?;
    let content = match file.content {
        crate::iceberg::spec::DataContentType::Data => IcebergFileContent::Data,
        crate::iceberg::spec::DataContentType::PositionDeletes => {
            IcebergFileContent::PositionDeletes
        }
        crate::iceberg::spec::DataContentType::EqualityDeletes => {
            IcebergFileContent::EqualityDeletes
        }
    };
    Ok(IcebergWriterReport {
        file: IcebergWrittenFileReport {
            path: file.path.clone(),
            format: file.format.to_string(),
            content,
            record_count: u64_to_i64(file.record_count, "record_count")?,
            file_size_in_bytes: u64_to_i64(file.file_size_in_bytes, "file_size_in_bytes")?,
            partition: IcebergPartitionReport {
                partition_path,
                null_fingerprint,
                partition_spec_id: file.partition_spec_id,
                partition_values: file.partition_values.clone(),
            },
            split_offsets: (!file.split_offsets.is_empty()).then_some(file.split_offsets.clone()),
            column_stats: column_stats_from_written_file(file)?,
            referenced_data_file: file.referenced_data_file.clone(),
            first_row_id: file.first_row_id,
            equality_ids: file.equality_ids.clone(),
            key_metadata: file.key_metadata.clone(),
            content_offset: file.content_offset,
            content_size_in_bytes: file.content_size_in_bytes,
            cardinality: file
                .cardinality
                .map(|value| u64_to_i64(value, "cardinality"))
                .transpose()?,
        },
        is_overwrite: None,
        is_rewrite: None,
    })
}

fn column_stats_from_written_file(
    file: &WrittenFile,
) -> Result<Option<IcebergColumnStats>, String> {
    let stats = IcebergColumnStats {
        column_sizes: u64_stats_to_i64(&file.column_sizes, "column_sizes")?,
        value_counts: u64_stats_to_i64(&file.value_counts, "value_counts")?,
        null_value_counts: u64_stats_to_i64(&file.null_value_counts, "null_value_counts")?,
        nan_value_counts: u64_stats_to_i64(&file.nan_value_counts, "nan_value_counts")?,
        lower_bounds: datum_bounds_to_bytes(&file.lower_bounds, "lower_bounds")?,
        upper_bounds: datum_bounds_to_bytes(&file.upper_bounds, "upper_bounds")?,
    };
    Ok((!stats.is_empty()).then_some(stats))
}

fn u64_to_i64(value: u64, label: &str) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| format!("iceberg {label} {value} overflows i64"))
}

fn u64_stats_to_i64(stats: &HashMap<i32, u64>, label: &str) -> Result<BTreeMap<i32, i64>, String> {
    stats
        .iter()
        .map(|(field_id, value)| {
            u64_to_i64(*value, &format!("{label}[{field_id}]")).map(|value| (*field_id, value))
        })
        .collect()
}

fn datum_bounds_to_bytes(
    bounds: &HashMap<i32, crate::iceberg::spec::Datum>,
    label: &str,
) -> Result<BTreeMap<i32, Vec<u8>>, String> {
    bounds
        .iter()
        .map(|(field_id, datum)| {
            datum
                .to_bytes()
                .map(|bytes| (*field_id, bytes.to_vec()))
                .map_err(|e| {
                    format!("convert iceberg datum bound {label}[{field_id}] to bytes failed: {e}")
                })
        })
        .collect()
}

pub fn partition_path_from_struct(
    values: &Struct,
    partition_spec: &PartitionSpecRef,
) -> Result<(String, String), String> {
    if values.fields().len() != partition_spec.fields().len() {
        return Err(format!(
            "partition value count {} does not match partition spec field count {}",
            values.fields().len(),
            partition_spec.fields().len()
        ));
    }
    let mut path = String::new();
    let mut null_fingerprint = String::with_capacity(values.fields().len());
    for (value, field) in values.fields().iter().zip(partition_spec.fields().iter()) {
        null_fingerprint.push(if value.is_none() { '1' } else { '0' });
        path.push_str(&field.name);
        path.push('=');
        match value {
            Some(value) => path.push_str(&partition_literal_to_path_value(value)?),
            None => path.push_str("null"),
        }
        path.push('/');
    }
    Ok((path.trim_matches('/').to_string(), null_fingerprint))
}

fn partition_literal_to_path_value(value: &Literal) -> Result<String, String> {
    let Literal::Primitive(value) = value else {
        return Err("iceberg partition path only supports primitive literals".to_string());
    };
    Ok(match value {
        PrimitiveLiteral::Boolean(value) => {
            if *value {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        PrimitiveLiteral::Int(value) => value.to_string(),
        PrimitiveLiteral::Long(value) => value.to_string(),
        PrimitiveLiteral::Float(value) => value.0.to_string(),
        PrimitiveLiteral::Double(value) => value.0.to_string(),
        PrimitiveLiteral::String(value) => url_encode_partition_value(value),
        PrimitiveLiteral::Binary(value) => {
            let encoded = base64::engine::general_purpose::STANDARD.encode(value);
            url_encode_partition_value(&encoded)
        }
        PrimitiveLiteral::Int128(value) => value.to_string(),
        PrimitiveLiteral::UInt128(value) => value.to_string(),
        PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin => {
            return Err("iceberg partition path cannot encode sentinel bounds".to_string());
        }
    })
}

fn url_encode_partition_value(input: &str) -> String {
    url::form_urlencoded::byte_serialize(input.as_bytes()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_column_stats_reports_empty() {
        assert!(IcebergColumnStats::default().is_empty());
    }

    #[test]
    fn column_size_makes_stats_non_empty() {
        let mut stats = IcebergColumnStats::default();
        stats.column_sizes.insert(1, 42);

        assert!(!stats.is_empty());
    }

    #[test]
    fn nan_value_count_makes_stats_non_empty() {
        let mut stats = IcebergColumnStats::default();
        stats.nan_value_counts.insert(1, 2);

        assert!(!stats.is_empty());
    }

    #[test]
    fn writer_report_from_written_file_preserves_nan_value_counts() {
        let metadata = crate::iceberg::spec::TableMetadataBuilder::from_table_creation(
            crate::iceberg::TableCreation::builder()
                .name("t".to_string())
                .location("file:///warehouse/db/t".to_string())
                .schema(
                    crate::iceberg::spec::Schema::builder()
                        .with_schema_id(1)
                        .with_fields(vec![std::sync::Arc::new(
                            crate::iceberg::spec::NestedField::required(
                                1,
                                "id",
                                crate::iceberg::spec::Type::Primitive(
                                    crate::iceberg::spec::PrimitiveType::Double,
                                ),
                            ),
                        )])
                        .build()
                        .expect("schema"),
                )
                .partition_spec(crate::iceberg::spec::PartitionSpec::unpartition_spec())
                .format_version(crate::iceberg::spec::FormatVersion::V2)
                .build(),
        )
        .expect("table metadata builder")
        .build()
        .expect("table metadata")
        .metadata;
        let file = WrittenFile {
            path: "file:///warehouse/t/data/a.parquet".to_string(),
            format: crate::iceberg::spec::DataFileFormat::Parquet,
            content: crate::iceberg::spec::DataContentType::Data,
            partition_values: Struct::empty(),
            partition_spec_id: metadata.default_partition_spec_id(),
            record_count: 1,
            file_size_in_bytes: 12,
            split_offsets: Vec::new(),
            column_sizes: Default::default(),
            value_counts: Default::default(),
            null_value_counts: Default::default(),
            nan_value_counts: HashMap::from([(1, 1)]),
            lower_bounds: Default::default(),
            upper_bounds: Default::default(),
            key_metadata: None,
            referenced_data_file: None,
            equality_ids: None,
            first_row_id: None,
            content_offset: None,
            content_size_in_bytes: None,
            cardinality: None,
        };

        let report = writer_report_from_written_file(&file, &metadata).expect("writer report");
        let decoded = written_file_from_report(report.clone(), metadata.current_schema().as_ref())
            .expect("decode immutable output without a commit collector");
        assert_eq!(decoded.path, file.path);
        assert_eq!(decoded.partition_values, file.partition_values);
        assert_eq!(decoded.partition_spec_id, file.partition_spec_id);
        assert_eq!(decoded.record_count, file.record_count);
        assert_eq!(decoded.nan_value_counts, file.nan_value_counts);

        assert_eq!(
            report
                .file
                .column_stats
                .expect("column stats")
                .nan_value_counts
                .get(&1),
            Some(&1)
        );
    }

    #[test]
    fn writer_report_from_written_file_rejects_cardinality_overflow() {
        let metadata = crate::iceberg::spec::TableMetadataBuilder::from_table_creation(
            crate::iceberg::TableCreation::builder()
                .name("t".to_string())
                .location("file:///warehouse/db/t".to_string())
                .schema(
                    crate::iceberg::spec::Schema::builder()
                        .with_schema_id(1)
                        .with_fields(vec![std::sync::Arc::new(
                            crate::iceberg::spec::NestedField::required(
                                1,
                                "id",
                                crate::iceberg::spec::Type::Primitive(
                                    crate::iceberg::spec::PrimitiveType::Int,
                                ),
                            ),
                        )])
                        .build()
                        .expect("schema"),
                )
                .partition_spec(crate::iceberg::spec::PartitionSpec::unpartition_spec())
                .format_version(crate::iceberg::spec::FormatVersion::V2)
                .build(),
        )
        .expect("table metadata builder")
        .build()
        .expect("table metadata")
        .metadata;
        let file = WrittenFile {
            path: "file:///warehouse/t/dv-00000000.puffin".to_string(),
            format: crate::iceberg::spec::DataFileFormat::Puffin,
            content: crate::iceberg::spec::DataContentType::PositionDeletes,
            partition_values: Struct::empty(),
            partition_spec_id: metadata.default_partition_spec_id(),
            record_count: 3,
            file_size_in_bytes: 40,
            split_offsets: Vec::new(),
            column_sizes: Default::default(),
            value_counts: Default::default(),
            null_value_counts: Default::default(),
            nan_value_counts: Default::default(),
            lower_bounds: Default::default(),
            upper_bounds: Default::default(),
            key_metadata: None,
            referenced_data_file: Some("file:///warehouse/t/data-1.parquet".to_string()),
            equality_ids: None,
            first_row_id: None,
            content_offset: Some(4),
            content_size_in_bytes: Some(12),
            cardinality: Some(i64::MAX as u64 + 1),
        };

        let err = writer_report_from_written_file(&file, &metadata)
            .expect_err("cardinality overflow should fail");

        assert!(err.contains("cardinality"), "got: {err}");
    }
}

/// Decode immutable writer output facts without owning a commit or cleanup ledger.
pub(crate) fn written_file_from_report(
    report: IcebergWriterReport,
    schema: &crate::iceberg::spec::Schema,
) -> Result<WrittenFile, String> {
    use crate::delete_file::IcebergFileContent;
    use crate::iceberg::spec::{DataContentType, DataFileFormat};

    let IcebergWriterReport { file, .. } = report;
    let format_name = file.format.clone();
    let format = DataFileFormat::from_str(&format_name)
        .map_err(|e| format!("unsupported Iceberg writer report format `{format_name}`: {e}"))?;
    let content = match file.content {
        IcebergFileContent::Data => DataContentType::Data,
        IcebergFileContent::PositionDeletes => DataContentType::PositionDeletes,
        IcebergFileContent::EqualityDeletes => DataContentType::EqualityDeletes,
    };
    validate_puffin_dv_descriptor(
        format,
        content,
        file.referenced_data_file.as_deref(),
        file.content_offset,
        file.content_size_in_bytes,
        file.cardinality,
    )?;

    let partition = file.partition;
    let stats = file.column_stats.unwrap_or_default();
    let column_sizes = i64_map_to_u64(Some(stats.column_sizes), "column_sizes")?;
    let value_counts = i64_map_to_u64(Some(stats.value_counts), "value_counts")?;
    let null_value_counts = i64_map_to_u64(Some(stats.null_value_counts), "null_value_counts")?;
    let nan_value_counts = i64_map_to_u64(Some(stats.nan_value_counts), "nan_value_counts")?;
    let lower_bounds = decode_report_bounds(schema, Some(stats.lower_bounds), "lower_bounds")?;
    let upper_bounds = decode_report_bounds(schema, Some(stats.upper_bounds), "upper_bounds")?;
    let split_offsets = i64_vec_non_negative(file.split_offsets, "split_offsets")?;
    let first_row_id = i64_option_non_negative(file.first_row_id, "first_row_id")?;
    let content_offset = i64_option_non_negative(file.content_offset, "content_offset")?;
    let content_size_in_bytes =
        i64_option_non_negative(file.content_size_in_bytes, "content_size_in_bytes")?;

    let written = WrittenFile {
        path: file.path,
        format,
        content,
        partition_values: partition.partition_values,
        partition_spec_id: partition.partition_spec_id,
        record_count: i64_to_u64(file.record_count, "record_count")?,
        file_size_in_bytes: i64_to_u64(file.file_size_in_bytes, "file_size_in_bytes")?,
        split_offsets,
        column_sizes,
        value_counts,
        null_value_counts,
        nan_value_counts,
        lower_bounds,
        upper_bounds,
        key_metadata: file.key_metadata,
        referenced_data_file: file.referenced_data_file,
        equality_ids: file.equality_ids,
        first_row_id,
        content_offset,
        content_size_in_bytes,
        cardinality: file
            .cardinality
            .map(|c| i64_to_u64(c, "cardinality"))
            .transpose()?,
    };
    written
        .entry_identity()
        .map_err(|error| error.to_string())?;
    Ok(written)
}

fn decode_report_bounds(
    schema: &crate::iceberg::spec::Schema,
    bounds: Option<BTreeMap<i32, Vec<u8>>>,
    field: &str,
) -> Result<HashMap<i32, Datum>, String> {
    let mut out = HashMap::new();
    for (field_id, bytes) in bounds.unwrap_or_default() {
        // Iceberg writers may leave bounds for retired field-ids in a file
        // after a column is dropped. We cannot decode bytes without the
        // field's type, so skip unknown ids rather than failing the commit.
        // This matches the inject path (`data_file_to_written_file`), which
        // carries bounds through without validating against the schema.
        let Some(schema_field) = schema.field_by_id(field_id) else {
            continue;
        };
        let prim = match &*schema_field.field_type {
            Type::Primitive(p) => p.clone(),
            other => {
                return Err(format!(
                    "column stat {field} field id {field_id} has non-primitive type {other:?}"
                ));
            }
        };
        if matches!(prim, PrimitiveType::Variant) {
            continue;
        }
        let datum = Datum::try_from_bytes(&bytes, prim)
            .map_err(|e| format!("decode column stat {field}[{field_id}] failed: {e}"))?;
        out.insert(field_id, datum);
    }
    Ok(out)
}

fn validate_puffin_dv_descriptor(
    format: crate::iceberg::spec::DataFileFormat,
    content: crate::iceberg::spec::DataContentType,
    referenced_data_file: Option<&str>,
    content_offset: Option<i64>,
    content_size_in_bytes: Option<i64>,
    cardinality: Option<i64>,
) -> Result<(), String> {
    use crate::iceberg::spec::{DataContentType, DataFileFormat};

    if format != DataFileFormat::Puffin || content != DataContentType::PositionDeletes {
        return Ok(());
    }
    match referenced_data_file {
        Some(path) if !path.is_empty() => {}
        _ => {
            return Err(
                "Puffin position-delete DV requires non-empty referenced_data_file".to_string(),
            );
        }
    }
    match content_offset {
        Some(offset) if offset >= 0 => {}
        Some(offset) => {
            return Err(format!(
                "Puffin position-delete DV content_offset must be non-negative, got {offset}"
            ));
        }
        None => {
            return Err("Puffin position-delete DV requires content_offset".to_string());
        }
    }
    match content_size_in_bytes {
        Some(size) if size >= 0 => {}
        Some(size) => {
            return Err(format!(
                "Puffin position-delete DV content_size_in_bytes must be non-negative, got {size}"
            ));
        }
        None => {
            return Err("Puffin position-delete DV requires content_size_in_bytes".to_string());
        }
    }
    match cardinality {
        Some(value) if value >= 0 => Ok(()),
        Some(value) => Err(format!(
            "Puffin position-delete DV cardinality must be non-negative, got {value}"
        )),
        None => Err("Puffin position-delete DV requires cardinality".to_string()),
    }
}

/// Convert signed writer-report counts into the `WrittenFile`
/// `HashMap<i32, u64>` representation.
fn i64_to_u64(value: i64, field: &str) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| format!("iceberg {field} value {value} is negative"))
}

fn i64_option_non_negative(value: Option<i64>, field: &str) -> Result<Option<i64>, String> {
    if let Some(value) = value
        && value < 0
    {
        return Err(format!("iceberg {field} value {value} is negative"));
    }
    Ok(value)
}

fn i64_vec_non_negative(values: Option<Vec<i64>>, field: &str) -> Result<Vec<i64>, String> {
    values
        .unwrap_or_default()
        .into_iter()
        .enumerate()
        .map(|(idx, value)| {
            if value < 0 {
                Err(format!("iceberg {field}[{idx}] value {value} is negative"))
            } else {
                Ok(value)
            }
        })
        .collect()
}

fn i64_map_to_u64(
    map: Option<BTreeMap<i32, i64>>,
    field: &str,
) -> Result<HashMap<i32, u64>, String> {
    map.unwrap_or_default()
        .into_iter()
        .map(|(field_id, value)| {
            i64_to_u64(value, &format!("column stat {field}[{field_id}]"))
                .map(|value| (field_id, value))
        })
        .collect()
}
