//! ADD FILES implementation for Iceberg tables.
//!
//! Registers existing parquet files from S3/OSS into an Iceberg table's
//! metadata without data movement.

use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct};
use iceberg::transaction::{ApplyTransactionAction, Transaction};

use crate::fs::object_store::{ObjectStoreConfig, build_oss_operator};

use super::iceberg::{IcebergCatalogEntry, block_on_iceberg, load_table};

/// Execute ADD FILES: register parquet files from an S3 directory into an Iceberg table.
pub(crate) fn add_files(
    entry: &IcebergCatalogEntry,
    namespace: &str,
    table_name: &str,
    s3_directory: &str,
) -> Result<usize, String> {
    let loaded = load_table(entry, namespace, table_name)?;
    let s3_config = build_s3_config_from_properties(&entry.properties)?;

    let files = list_parquet_files(&s3_config, s3_directory)?;
    tracing::info!(
        "ADD FILES: found {} parquet files in {s3_directory}",
        files.len()
    );
    for (path, size) in &files {
        tracing::info!("  file: {path} ({size} bytes)");
    }
    if files.is_empty() {
        return Err(format!(
            "ADD FILES: no parquet files found in {s3_directory} (bucket={}, prefix from parse)",
            parse_s3_path(s3_directory).map(|(b,_)| b).unwrap_or_default()
        ));
    }

    let mut data_files = Vec::with_capacity(files.len());
    for (file_path, file_size) in &files {
        let record_count = read_parquet_record_count(&s3_config, file_path, *file_size)?;
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

    block_on_iceberg(async {
        let tx = Transaction::new(&loaded.table);
        let tx = tx
            .fast_append()
            .add_data_files(data_files)
            .apply(tx)
            .map_err(|e| format!("append files failed: {e}"))?;
        tx.commit(entry.catalog.as_ref())
            .await
            .map_err(|e| format!("commit failed: {e}"))
    })
    .map_err(|e| format!("add_files runtime: {e}"))?
    .map_err(|e| format!("add_files failed: {e}"))?;

    tracing::info!(
        "ADD FILES: registered {count} parquet files into {namespace}.{table_name}"
    );
    Ok(count)
}

// ---------------------------------------------------------------------------
// S3 config helpers
// ---------------------------------------------------------------------------

fn build_s3_config_from_properties(
    properties: &[(String, String)],
) -> Result<ObjectStoreConfig, String> {
    let mut map = std::collections::HashMap::new();
    for (k, v) in properties {
        map.insert(k.as_str(), v.as_str());
    }
    let endpoint = map
        .get("aws.s3.endpoint")
        .ok_or("ADD FILES requires aws.s3.endpoint")?;
    let ak = map
        .get("aws.s3.access_key")
        .ok_or("ADD FILES requires aws.s3.access_key")?;
    let sk = map
        .get("aws.s3.secret_key")
        .ok_or("ADD FILES requires aws.s3.secret_key")?;
    let path_style = map
        .get("aws.s3.enable_path_style_access")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    Ok(ObjectStoreConfig {
        endpoint: endpoint.to_string(),
        bucket: String::new(),
        root: String::new(),
        access_key_id: ak.to_string(),
        access_key_secret: sk.to_string(),
        session_token: None,
        enable_path_style_access: Some(path_style),
        region: map.get("aws.s3.region").map(|s| s.to_string()),
        retry_max_times: Some(3),
        retry_min_delay_ms: Some(100),
        retry_max_delay_ms: Some(2000),
        timeout_ms: Some(60000),
        io_timeout_ms: Some(60000),
    })
}

pub(crate) fn parse_s3_path(path: &str) -> Result<(String, String), String> {
    let stripped = path
        .strip_prefix("s3://")
        .or_else(|| path.strip_prefix("s3a://"))
        .or_else(|| path.strip_prefix("oss://"))
        .ok_or_else(|| format!("unsupported path scheme: {path}"))?;
    let slash = stripped
        .find('/')
        .ok_or_else(|| format!("path has no key: {path}"))?;
    Ok((
        stripped[..slash].to_string(),
        stripped[slash + 1..].to_string(),
    ))
}

// ---------------------------------------------------------------------------
// File listing + parquet metadata
// ---------------------------------------------------------------------------

fn list_parquet_files(
    base_config: &ObjectStoreConfig,
    directory: &str,
) -> Result<Vec<(String, u64)>, String> {
    let (bucket, prefix) = parse_s3_path(directory)?;
    let scheme = if directory.starts_with("oss://") {
        "oss"
    } else {
        "s3"
    };
    let mut cfg = base_config.clone();
    cfg.bucket = bucket.clone();
    let op = build_oss_operator(&cfg).map_err(|e| format!("build S3 operator: {e}"))?;

    let prefix = if prefix.ends_with('/') {
        prefix
    } else {
        format!("{prefix}/")
    };

    block_on_iceberg(async {
        let entries = op
            .list(&prefix)
            .await
            .map_err(|e| format!("list {directory}: {e}"))?;

        let mut result = Vec::new();
        for entry in entries {
            let name = entry.name().to_string();
            if name.ends_with(".parquet")
                && !name.starts_with('.')
                && !name.starts_with('_')
            {
                let meta = op
                    .stat(entry.path())
                    .await
                    .map_err(|e| format!("stat {}: {e}", entry.path()))?;
                let full_path = format!("{scheme}://{bucket}/{}", entry.path());
                result.push((full_path, meta.content_length()));
            }
        }
        Ok(result)
    })
    .map_err(|e| format!("list_parquet_files runtime: {e}"))?
}

fn read_parquet_record_count(
    base_config: &ObjectStoreConfig,
    s3_path: &str,
    file_size: u64,
) -> Result<u64, String> {
    let (bucket, key) = parse_s3_path(s3_path)?;
    let mut cfg = base_config.clone();
    cfg.bucket = bucket;
    let op = build_oss_operator(&cfg).map_err(|e| format!("build operator: {e}"))?;

    block_on_iceberg(async {
        if file_size < 12 {
            return Err(format!("parquet file too small: {s3_path}"));
        }
        // Parquet footer: last 8 bytes = [footer_len(4 LE), magic "PAR1"(4)]
        let tail = op
            .read_with(&key)
            .range(file_size - 8..file_size)
            .await
            .map_err(|e| format!("read footer tail: {e}"))?
            .to_bytes();
        if tail.len() < 8 || &tail[4..8] != b"PAR1" {
            return Err(format!("invalid parquet footer: {s3_path}"));
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
