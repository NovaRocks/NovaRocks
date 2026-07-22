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

//! ADD FILES implementation for Iceberg tables.
//!
//! Registers existing parquet files from resolver-backed filesystem locations
//! into an Iceberg table's metadata without data movement.

use std::collections::HashMap;

use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableIdent};

use crate::connector::iceberg::catalog::registry::{
    IcebergCatalogEntry, block_on_iceberg, build_hadoop_catalog, load_table,
};
use crate::connector::iceberg::fs_io;
use crate::fs::object_store::ObjectStoreConfig;
use novarocks_catalog::identifier::normalize_identifier;

/// Execute ADD FILES: register parquet files from an S3 directory into an Iceberg table.
pub(crate) fn add_files(
    entry: &IcebergCatalogEntry,
    namespace: &str,
    table_name: &str,
    s3_directory: &str,
) -> Result<usize, String> {
    let loaded = load_table(entry, namespace, table_name)?;
    let object_store_config = fs_io::object_store_config_from_catalog_properties(&entry.properties)
        .map_err(|e| format!("parse Iceberg ADD FILES object-store catalog properties: {e}"))?;

    let files = list_parquet_files(s3_directory, object_store_config.as_ref())?;
    tracing::info!(
        "ADD FILES: found {} parquet files in {s3_directory}",
        files.len()
    );
    for (path, size) in &files {
        tracing::info!("  file: {path} ({size} bytes)");
    }
    if files.is_empty() {
        return Err(format!(
            "ADD FILES: no parquet files found under {s3_directory}"
        ));
    }

    let mut data_files = Vec::with_capacity(files.len());
    for (file_path, file_size) in &files {
        let record_count =
            read_parquet_record_count(file_path, *file_size, object_store_config.as_ref())?;
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(file_path.clone())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(*file_size)
            .record_count(record_count)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .build()
            .map_err(|e| format!("build DataFile failed: {e}"))?;
        data_files.push(data_file);
    }

    let count = data_files.len();

    let catalog = build_hadoop_catalog(entry)?;
    let ns = NamespaceIdent::new(normalize_identifier(namespace)?);
    let _ = block_on_iceberg(async { catalog.create_namespace(&ns, HashMap::new()).await });
    let table_ident = TableIdent::from_strs([
        normalize_identifier(namespace)?,
        normalize_identifier(table_name)?,
    ])
    .map_err(|e| format!("build table ident: {e}"))?;
    let metadata_location = loaded
        .table
        .metadata_location()
        .ok_or_else(|| "no metadata location for table".to_string())?
        .to_string();
    let _ = block_on_iceberg(async {
        catalog
            .register_table(&table_ident, metadata_location)
            .await
    });

    block_on_iceberg(async {
        let tx = Transaction::new(&loaded.table);
        let tx = tx
            .fast_append()
            .add_data_files(data_files)
            .apply(tx)
            .map_err(|e| format!("append files failed: {e}"))?;
        tx.commit(&catalog)
            .await
            .map_err(|e| format!("commit failed: {e}"))
    })
    .map_err(|e| format!("add_files runtime: {e}"))?
    .map_err(|e| format!("add_files failed: {e}"))?;

    tracing::info!("ADD FILES: registered {count} parquet files into {namespace}.{table_name}");
    Ok(count)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    use super::{list_parquet_files, read_parquet_record_count};

    #[test]
    fn list_parquet_files_uses_resolver_and_formats_file_locations() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("a.parquet"), b"data").expect("write parquet placeholder");
        std::fs::write(dir.path().join("_hidden.parquet"), b"hidden").expect("write hidden");
        std::fs::write(dir.path().join("notes.txt"), b"notes").expect("write notes");
        let directory = format!("file://{}", dir.path().display());

        let files = list_parquet_files(&directory, None).expect("list parquet files");

        assert_eq!(
            files,
            vec![(format!("file://{}/a.parquet", dir.path().display()), 4)]
        );
    }

    #[test]
    fn read_parquet_record_count_reads_local_file_footer_through_resolver() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("rows.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50]))],
        )
        .expect("record batch");
        let file = std::fs::File::create(&file_path).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("parquet writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close parquet writer");
        let file_size = std::fs::metadata(&file_path).expect("metadata").len();
        let file_uri = format!("file://{}", file_path.display());

        let row_count =
            read_parquet_record_count(&file_uri, file_size, None).expect("read record count");

        assert_eq!(row_count, 5);
    }
}

// ---------------------------------------------------------------------------
// File listing + parquet metadata
// ---------------------------------------------------------------------------

fn list_parquet_files(
    directory: &str,
    object_store_config: Option<&ObjectStoreConfig>,
) -> Result<Vec<(String, u64)>, String> {
    let access = fs_io::resolve_access_for_location(directory, object_store_config)
        .map_err(|e| format!("resolve ADD FILES directory {directory}: {e}"))?;
    let op = access.operator();

    let relative_directory = access.single_relative_path()?;
    let prefix = if relative_directory.ends_with('/') {
        relative_directory.to_string()
    } else {
        format!("{relative_directory}/")
    };

    block_on_iceberg(async {
        let entries = op
            .list(&prefix)
            .await
            .map_err(|e| format!("list {directory}: {e}"))?;

        let mut result = Vec::new();
        for entry in entries {
            let name = entry.name().to_string();
            if name.ends_with(".parquet") && !name.starts_with('.') && !name.starts_with('_') {
                let meta = op
                    .stat(entry.path())
                    .await
                    .map_err(|e| format!("stat {}: {e}", entry.path()))?;
                let full_path = fs_io::format_resolved_location(access.handle(), entry.path())?;
                result.push((full_path, meta.content_length()));
            }
        }
        Ok(result)
    })
    .map_err(|e| format!("list_parquet_files runtime: {e}"))?
}

fn read_parquet_record_count(
    path: &str,
    file_size: u64,
    object_store_config: Option<&ObjectStoreConfig>,
) -> Result<u64, String> {
    let access = fs_io::resolve_access_for_location(path, object_store_config)
        .map_err(|e| format!("resolve parquet file {path}: {e}"))?;
    let key = access.single_relative_path()?.to_string();
    let op = access.operator();

    block_on_iceberg(async {
        if file_size < 12 {
            return Err(format!("parquet file too small: {path}"));
        }
        // Parquet footer: last 8 bytes = [footer_len(4 LE), magic "PAR1"(4)]
        let tail = op
            .read_with(&key)
            .range(file_size - 8..file_size)
            .await
            .map_err(|e| format!("read footer tail: {e}"))?
            .to_bytes();
        if tail.len() < 8 || &tail[4..8] != b"PAR1" {
            return Err(format!("invalid parquet footer: {path}"));
        }
        let footer_len = u32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]) as u64;

        // Read the Thrift-encoded FileMetaData
        let footer_start = file_size - 8 - footer_len;
        let footer_bytes = op
            .read_with(&key)
            .range(footer_start..file_size - 8)
            .await
            .map_err(|e| format!("read footer: {e}"))?
            .to_bytes();

        // Build suffix bytes (footer_data + footer_len_bytes + magic) and parse
        let mut suffix_buf = Vec::with_capacity(footer_bytes.len() + 8);
        suffix_buf.extend_from_slice(&footer_bytes);
        suffix_buf.extend_from_slice(&tail);
        let suffix = bytes::Bytes::from(suffix_buf);

        use parquet::file::metadata::ParquetMetaDataReader;
        let mut reader = ParquetMetaDataReader::new();
        reader
            .try_parse_sized(&suffix, file_size)
            .map_err(|e| format!("parse parquet metadata: {e}"))?;
        let metadata = reader
            .finish()
            .map_err(|e| format!("finish parquet metadata: {e}"))?;
        Ok(metadata.file_metadata().num_rows() as u64)
    })
    .map_err(|e| format!("read_record_count runtime: {e}"))?
}
