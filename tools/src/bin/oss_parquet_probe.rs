mod fs_access_tooling;

use anyhow::{Context, Result};
use bytes::Bytes;
use futures::TryStreamExt;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::file::reader::{FileReader, SerializedFileReader};

use novarocks::fs::object_store::ObjectStoreConfig;
use novarocks::fs::object_store_credentials::{
    ObjectStoreCredentials, ObjectStoreCredentialsSource,
};
use novarocks::novarocks_config::{NovaRocksConfig, init_from_env_or_default, init_from_path};

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

fn object_store_config_from_standalone(app_cfg: &NovaRocksConfig) -> Result<ObjectStoreConfig> {
    let standalone = app_cfg
        .standalone_server
        .as_ref()
        .context("missing [standalone_server] config")?;
    let object_store = standalone
        .object_store
        .as_ref()
        .context("missing [standalone_server.object_store] config")?;
    let credentials = ObjectStoreCredentials::from_parts(
        ObjectStoreCredentialsSource::StandaloneConfig,
        object_store.endpoint.as_deref().unwrap_or_default(),
        object_store.access_key_id.as_deref().unwrap_or_default(),
        object_store
            .access_key_secret
            .as_deref()
            .unwrap_or_default(),
        object_store.region.as_deref(),
        object_store.enable_path_style_access,
    )
    .map_err(anyhow::Error::msg)?;
    Ok(credentials.to_object_store_config())
}

fn probe_location_from_args(app_cfg: &NovaRocksConfig, prefix: &str) -> Result<String> {
    let prefix = prefix.trim();
    if !prefix.is_empty() {
        return Ok(prefix.to_string());
    }
    let standalone = app_cfg
        .standalone_server
        .as_ref()
        .context("missing [standalone_server] config")?;
    standalone
        .warehouse_uri
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .context("missing --prefix and standalone_server.warehouse_uri")
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
    novarocks::novarocks_logging::init();
    let mut args = std::env::args().skip(1);
    let mut config_path: Option<String> = None;
    let mut prefix: String = String::new();
    let mut max_files: usize = 5;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" | "-c" => {
                config_path = Option::from(args.next().context("missing value for --config/-c")?);
            }
            "--prefix" => {
                prefix = args.next().context("missing value for --prefix")?;
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
                    "Usage: oss_parquet_probe [--config <path>] --prefix <s3://bucket/prefix> [--max-files <n>]"
                );
                eprintln!("  Default config path: $NOVAROCKS_CONFIG or ./novarocks.toml");
                std::process::exit(0);
            }
            other => anyhow::bail!("unknown arg: {other} (try --help)"),
        }
    }

    let app_cfg = match config_path.as_deref() {
        Some(p) => init_from_path(p).context("load config")?,
        None => init_from_env_or_default().context("load config")?,
    };
    let object_store_config = object_store_config_from_standalone(&app_cfg)?;
    let location = probe_location_from_args(&app_cfg, &prefix)?;
    let access = fs_access_tooling::resolve_tool_location(&location, Some(&object_store_config))
        .map_err(anyhow::Error::msg)?;
    let relative_path =
        fs_access_tooling::single_relative_path(&access, &location).map_err(anyhow::Error::msg)?;
    let list_prefix = fs_access_tooling::list_prefix(&relative_path);
    let op = access.operator();

    eprintln!(
        "[probe] endpoint={} authority={} prefix={} max_files={}",
        object_store_config.endpoint,
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
