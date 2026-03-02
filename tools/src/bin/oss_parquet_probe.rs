use anyhow::{Context, Result};
use bytes::Bytes;
use futures::TryStreamExt;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::file::reader::{FileReader, SerializedFileReader};

use novarocks::novarocks_config::{init_from_env_or_default, init_from_path};
use novarocks::novarocks_fs_oss::build_oss_operator;

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
                    "Usage: oss_parquet_probe [--config <path>] [--prefix <prefix>] [--max-files <n>]"
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
    let oss_cfg = app_cfg.oss_config().context("load oss config")?;
    let op = build_oss_operator(&oss_cfg).context("build oss operator")?;

    eprintln!(
        "[probe] endpoint={} bucket={} root={} prefix={} max_files={}",
        oss_cfg.endpoint, oss_cfg.bucket, oss_cfg.root, prefix, max_files
    );

    let mut files = Vec::new();
    let mut list_prefix = prefix.trim().to_string();
    if !list_prefix.is_empty() && !list_prefix.ends_with('/') {
        list_prefix.push('/');
    }
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
        anyhow::bail!("no .parquet found under prefix={}", prefix);
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
