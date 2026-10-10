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

//! `crate::iceberg::spec::DataFile` re-construction shared by the
//! commit-action implementations.
//!
//! `DataFile` fields are `pub(crate)` in iceberg-rust 0.9, so every
//! reconstruction goes through `DataFileBuilder`.

use crate::iceberg::spec::{DataFile, DataFileBuilder};

/// Clone a `DataFile`, overriding `first_row_id` with the given value.
///
/// `DataFile::partition_spec_id` is private in iceberg-rust, so callers must
/// pass the manifest-level partition spec id for the source file.
pub(super) fn clone_data_file_with_first_row_id(
    src: &DataFile,
    partition_spec_id: i32,
    first_row_id: Option<i64>,
) -> Result<DataFile, String> {
    let mut builder = DataFileBuilder::default();
    builder
        .content(src.content_type())
        .file_path(src.file_path().to_string())
        .file_format(src.file_format())
        .partition(src.partition().clone())
        .partition_spec_id(partition_spec_id)
        .record_count(src.record_count())
        .file_size_in_bytes(src.file_size_in_bytes())
        .column_sizes(src.column_sizes().clone())
        .value_counts(src.value_counts().clone())
        .null_value_counts(src.null_value_counts().clone())
        .nan_value_counts(src.nan_value_counts().clone())
        .lower_bounds(src.lower_bounds().clone())
        .upper_bounds(src.upper_bounds().clone())
        .key_metadata(src.key_metadata().map(|b| b.to_vec()))
        .split_offsets(src.split_offsets().map(|s| s.to_vec()))
        .equality_ids(src.equality_ids())
        .first_row_id(first_row_id)
        .referenced_data_file(src.referenced_data_file())
        .content_offset(src.content_offset())
        .content_size_in_bytes(src.content_size_in_bytes());
    if let Some(id) = src.sort_order_id() {
        builder.sort_order_id(id);
    }
    builder
        .build()
        .map_err(|e| format!("clone_data_file_with_first_row_id failed: {e}"))
}

/// Freeze every writer-provided physical fact. Logical DV identity includes
/// the blob range; statistics and row lineage are never inferred or dropped.
pub(crate) fn from_written_file(src: &super::WrittenFile) -> crate::iceberg::Result<DataFile> {
    src.entry_identity()?;
    let mut builder = DataFileBuilder::default();
    builder
        .content(src.content)
        .file_path(src.path.clone())
        .file_format(src.format)
        .partition(src.partition_values.clone())
        .partition_spec_id(src.partition_spec_id)
        .record_count(src.record_count)
        .file_size_in_bytes(src.file_size_in_bytes)
        .column_sizes(src.column_sizes.clone())
        .value_counts(src.value_counts.clone())
        .null_value_counts(src.null_value_counts.clone())
        .nan_value_counts(src.nan_value_counts.clone())
        .lower_bounds(src.lower_bounds.clone())
        .upper_bounds(src.upper_bounds.clone())
        .key_metadata(src.key_metadata.clone())
        .split_offsets((!src.split_offsets.is_empty()).then(|| src.split_offsets.clone()))
        .equality_ids(src.equality_ids.clone())
        .first_row_id(src.first_row_id)
        .referenced_data_file(src.referenced_data_file.clone())
        .content_offset(src.content_offset)
        .content_size_in_bytes(src.content_size_in_bytes);
    builder.build().map_err(|error| {
        crate::iceberg::Error::new(
            crate::iceberg::ErrorKind::DataInvalid,
            "Cannot freeze Iceberg writer file facts",
        )
        .with_source(error)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::model::EntryIdentity;
    use crate::iceberg::spec::{DataContentType, DataFileFormat, Datum, Struct};
    use std::collections::HashMap;

    fn written() -> super::super::WrittenFile {
        super::super::WrittenFile {
            path: "s3://bucket/data.parquet".into(),
            format: DataFileFormat::Parquet,
            content: DataContentType::Data,
            partition_values: Struct::empty(),
            partition_spec_id: 7,
            record_count: 3,
            file_size_in_bytes: 256,
            split_offsets: vec![4, 100],
            column_sizes: HashMap::from([(1, 48)]),
            value_counts: HashMap::from([(1, 3)]),
            null_value_counts: HashMap::from([(1, 1)]),
            nan_value_counts: HashMap::from([(1, 1)]),
            lower_bounds: HashMap::from([(1, Datum::long(2))]),
            upper_bounds: HashMap::from([(1, Datum::long(5))]),
            key_metadata: Some(vec![1, 2]),
            referenced_data_file: None,
            equality_ids: None,
            first_row_id: Some(91),
            content_offset: None,
            content_size_in_bytes: None,
            cardinality: None,
        }
    }

    #[test]
    fn frozen_writer_facts_preserve_statistics_row_lineage_and_delete_identity() {
        let source = written();
        let file = from_written_file(&source).unwrap();
        assert_eq!(file.first_row_id(), Some(91));
        assert_eq!(file.nan_value_counts(), &source.nan_value_counts);
        assert_eq!(file.column_sizes(), &source.column_sizes);
        assert_eq!(file.value_counts(), &source.value_counts);
        assert_eq!(file.null_value_counts(), &source.null_value_counts);
        assert_eq!(file.lower_bounds(), &source.lower_bounds);
        assert_eq!(file.upper_bounds(), &source.upper_bounds);
        assert_eq!(file.key_metadata(), source.key_metadata.as_deref());
        assert_eq!(file.split_offsets(), Some(source.split_offsets.as_slice()));
        assert_eq!(file.record_count(), source.record_count);
        assert_eq!(file.file_size_in_bytes(), source.file_size_in_bytes);
        let mut equality = source.clone();
        equality.content = DataContentType::EqualityDeletes;
        equality.equality_ids = Some(vec![2, 5]);
        assert_eq!(
            from_written_file(&equality).unwrap().equality_ids(),
            Some(vec![2, 5])
        );
        let mut dv = source;
        dv.path = "s3://bucket/shared.puffin".into();
        dv.format = DataFileFormat::Puffin;
        dv.content = DataContentType::PositionDeletes;
        dv.first_row_id = None;
        dv.referenced_data_file = Some("s3://bucket/data.parquet".into());
        dv.content_offset = Some(16);
        dv.content_size_in_bytes = Some(33);
        assert_eq!(
            EntryIdentity::try_from(&from_written_file(&dv).unwrap()).unwrap(),
            dv.entry_identity().unwrap()
        );
        dv.content_offset = None;
        assert!(from_written_file(&dv).is_err());
    }
}
