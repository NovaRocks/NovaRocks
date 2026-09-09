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

mod fs_access_tooling;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::TryStreamExt;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::file::reader::{FileReader, SerializedFileReader};

use novarocks_fs::{
    ObjectStoreAccessContext, ObjectStoreCredentialProviderIdentity, ObjectStoreEndpointConfig,
    ObjectStoreProviderPool, ObjectStoreProviderPoolOptions, ObjectStoreSecretMaterial,
};
use novarocks_server::app_config::{NovaRocksConfig, load_from_env_or_default};
use novarocks_spi::connector::{
    CatalogCredentialPurpose, StaticCredentialReference, StorageAccessDomainId,
};

#[derive(Clone, Debug)]
struct ParquetProbe {
    path: String,
    num_rows: i64,
    num_row_groups: usize,
    created_by: Option<String>,
    schema: String,
    arrow_schema: Option<String>,
    arrow_schema_skip_meta: Option<String>,
    arrow_schema_error: Option<String>,
    arrow_schema_skip_meta_error: Option<String>,
}

fn probe_location_from_args(prefix: &str) -> Result<String> {
    let prefix = prefix.trim();
    if !prefix.is_empty() {
        return Ok(prefix.to_string());
    }
    anyhow::bail!("missing required --prefix")
}

fn parse_access_domain(value: &str) -> Result<StorageAccessDomainId> {
    let value = value.trim();
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        anyhow::bail!("--access-domain must be exactly 64 lowercase hexadecimal characters");
    }
    let mut bytes = [0_u8; 32];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(chunk).expect("ASCII checked");
        bytes[index] = u8::from_str_radix(text, 16).with_context(
            || "--access-domain must be exactly 64 lowercase hexadecimal characters",
        )?;
    }
    Ok(StorageAccessDomainId::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::parse_access_domain;

    #[test]
    fn access_domain_requires_lowercase_32_byte_hex() {
        assert!(parse_access_domain(&"ab".repeat(32)).is_ok());
        assert!(parse_access_domain(&"AB".repeat(32)).is_err());
        assert!(parse_access_domain(&"gg".repeat(32)).is_err());
        assert!(parse_access_domain(&"a".repeat(63)).is_err());
    }
}

fn probe_parquet_bytes(path: &str, bytes: Bytes) -> Result<ParquetProbe> {
    let reader = SerializedFileReader::new(bytes.clone())
        .with_context(|| format!("parquet open: {path}"))?;
    let metadata = reader.metadata();
    let file_meta = metadata.file_metadata();
    let arrow_schema = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .ok()
        .map(|builder| format!("{:?}", builder.schema()));
    let arrow_schema_error = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .err()
        .map(|e| e.to_string());
    let arrow_schema_skip_meta = ParquetRecordBatchReaderBuilder::try_new_with_options(
        bytes.clone(),
        ArrowReaderOptions::new().with_skip_arrow_metadata(true),
    )
    .ok()
    .map(|builder| format!("{:?}", builder.schema()));
    let arrow_schema_skip_meta_error = ParquetRecordBatchReaderBuilder::try_new_with_options(
        bytes,
        ArrowReaderOptions::new().with_skip_arrow_metadata(true),
    )
    .err()
    .map(|e| e.to_string());

    Ok(ParquetProbe {
        path: path.to_string(),
        num_rows: file_meta.num_rows(),
        num_row_groups: metadata.num_row_groups(),
        created_by: file_meta.created_by().map(|s: &str| s.to_string()),
        schema: format!("{:?}", file_meta.schema()),
        arrow_schema,
        arrow_schema_skip_meta,
        arrow_schema_error,
        arrow_schema_skip_meta_error,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    novarocks_server::logging::init();
    let mut args = std::env::args().skip(1);
    let mut config_path: Option<String> = None;
    let mut prefix: String = String::new();
    let mut credential_name: Option<String> = None;
    let mut credential_generation: Option<String> = None;
    let mut endpoint: Option<String> = None;
    let mut access_domain: Option<String> = None;
    let mut max_files: usize = 5;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" | "-c" => {
                config_path = Option::from(args.next().context("missing value for --config/-c")?);
            }
            "--prefix" => {
                prefix = args.next().context("missing value for --prefix")?;
            }
            "--credential-name" => {
                credential_name = Some(args.next().context("missing value for --credential-name")?);
            }
            "--credential-generation" => {
                credential_generation = Some(
                    args.next()
                        .context("missing value for --credential-generation")?,
                );
            }
            "--endpoint" => {
                endpoint = Some(args.next().context("missing value for --endpoint")?);
            }
            "--access-domain" => {
                access_domain = Some(args.next().context("missing value for --access-domain")?);
            }
            "--max-files" => {
                max_files = args
                    .next()
                    .context("missing value for --max-files")?
                    .parse()
                    .context("invalid --max-files (expected integer)")?;
            }
            "--help" | "-h" => {
                eprintln!(
                    "Usage: oss_parquet_probe [--config <path>] --prefix <s3://bucket/prefix> --credential-name <name> --credential-generation <generation> --endpoint <http(s)://host> --access-domain <64-hex> [--max-files <n>]"
                );
                eprintln!("  Default config path: $NOVAROCKS_CONFIG or ./novarocks.toml");
                std::process::exit(0);
            }
            other => anyhow::bail!("unknown arg: {other} (try --help)"),
        }
    }

    let location = probe_location_from_args(&prefix)?;
    let reference = StaticCredentialReference::try_new(
        credential_name
            .as_deref()
            .context("missing required --credential-name")?,
        credential_generation
            .as_deref()
            .context("missing required --credential-generation")?,
    )
    .map_err(anyhow::Error::msg)?;
    let endpoint = endpoint.context("missing required --endpoint")?;
    let access_domain = parse_access_domain(
        access_domain
            .as_deref()
            .context("missing required --access-domain")?,
    )?;
    let config = match config_path {
        Some(path) => NovaRocksConfig::load_from_file(std::path::Path::new(&path)),
        None => load_from_env_or_default(),
    }
    .context("load role-local credential registry")?;
    let registry = config
        .connector
        .credential_registry(config.cluster.role)
        .map_err(anyhow::Error::msg)?;
    let material = registry
        .resolve(CatalogCredentialPurpose::ObjectStoreData, &reference)
        .and_then(|material| material.as_s3())
        .context("configured role has no exact object-store-data S3 credential")?;
    let pool = ObjectStoreProviderPool::new(ObjectStoreProviderPoolOptions::default())
        .map_err(anyhow::Error::msg)?;
    let object_store_access = ObjectStoreAccessContext::new(
        ObjectStoreEndpointConfig {
            endpoint: endpoint.clone(),
            enable_path_style_access: None,
            region: None,
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        },
        ObjectStoreCredentialProviderIdentity::Static(reference),
        ObjectStoreSecretMaterial {
            access_key_id: material.access_key_id().clone(),
            access_key_secret: material.access_key_secret().clone(),
            session_token: material.session_token().cloned(),
        },
        &pool,
    );
    let access = fs_access_tooling::resolve_tool_location(
        &location,
        access_domain,
        Some(object_store_access),
    )
    .map_err(anyhow::Error::msg)?;
    let relative_path =
        fs_access_tooling::single_relative_path(&access, &location).map_err(anyhow::Error::msg)?;
    let list_prefix = fs_access_tooling::list_prefix(&relative_path);
    let op = access.operator();

    eprintln!(
        "[probe] endpoint={} authority={} prefix={} max_files={}",
        endpoint,
        access.authority().unwrap_or("<local>"),
        list_prefix,
        max_files
    );

    let mut files = Vec::new();
    let mut lister = op
        .lister_with(&list_prefix)
        .recursive(true)
        .await
        .context("opendal lister")?;
    while let Some(entry) = lister.try_next().await.context("opendal list next")? {
        let path = entry.path().to_string();
        if path.ends_with(".parquet") {
            files.push(path);
            if files.len() >= max_files {
                break;
            }
        }
    }
    if files.is_empty() {
        anyhow::bail!("no .parquet found under prefix={}", list_prefix);
    }

    for path in files {
        let data = op
            .read(&path)
            .await
            .with_context(|| format!("opendal read: {path}"))?;
        let probe = probe_parquet_bytes(&path, data.to_bytes())?;
        eprintln!(
            "[parquet] path={} rows={} row_groups={} created_by={}",
            probe.path,
            probe.num_rows,
            probe.num_row_groups,
            probe.created_by.as_deref().unwrap_or("<unknown>")
        );
        eprintln!("[parquet_schema] {}", probe.schema);
        if let Some(schema) = probe.arrow_schema.as_deref() {
            eprintln!("[arrow_schema] {}", schema);
        }
        if let Some(schema) = probe.arrow_schema_skip_meta.as_deref() {
            eprintln!("[arrow_schema_skip_meta] {}", schema);
        }
        if let Some(err) = probe.arrow_schema_error.as_deref() {
            eprintln!("[arrow_schema_error] {}", err);
        }
        if let Some(err) = probe.arrow_schema_skip_meta_error.as_deref() {
            eprintln!("[arrow_schema_skip_meta_error] {}", err);
        }
    }

    Ok(())
}
