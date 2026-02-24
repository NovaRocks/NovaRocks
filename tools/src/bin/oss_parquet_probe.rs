use anyhow::{Context, Result};

use novarocks::novarocks_config::{init_from_env_or_default, init_from_path};
use novarocks::novarocks_fs_opendal::{list_parquet_files, probe_parquet};
use novarocks::novarocks_fs_oss::build_oss_operator;

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

    let files = list_parquet_files(&op, &prefix, max_files)
        .await
        .context("list parquet files")?;
    if files.is_empty() {
        anyhow::bail!("no .parquet found under prefix={}", prefix);
    }

    for path in files {
        let probe = probe_parquet(&op, &path).await?;
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
