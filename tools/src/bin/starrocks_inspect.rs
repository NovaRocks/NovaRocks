use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use opendal::{ErrorKind, Operator};

use novarocks::formats::starrocks::metadata::load_tablet_snapshot;
use novarocks::formats::starrocks::plan::build_native_read_plan;
use novarocks::formats::starrocks::segment::{StarRocksSegmentFooter, decode_segment_footer};

const FS_S3A_ACCESS_KEY: &str = "fs.s3a.access.key";
const FS_S3A_SECRET_KEY: &str = "fs.s3a.secret.key";
const FS_S3A_SESSION_TOKEN: &str = "fs.s3a.session.token";
const FS_S3A_ENDPOINT: &str = "fs.s3a.endpoint";
const FS_S3A_ENDPOINT_REGION: &str = "fs.s3a.endpoint.region";
const FS_S3A_ENABLE_SSL: &str = "fs.s3a.connection.ssl.enabled";
const FS_S3A_PATH_STYLE: &str = "fs.s3a.path.style.access";

#[derive(Debug)]
struct InspectConfig {
    tablet_id: i64,
    version: i64,
    root: String,
    columns: Vec<String>,
    fs_options: BTreeMap<String, String>,
    skip_plan: bool,
}

fn print_help() {
    println!("starrocks_inspect");
    println!();
    println!("Inspect StarRocks tablet metadata and segment footers.");
    println!();
    println!("Options:");
    println!("  --tablet-id <id>          Tablet ID (default: 10035)");
    println!("  --version <v>             Tablet version (default: 4)");
    println!("  --root <path>             Tablet root path (default: /tmp/starrocks_tablet)");
    println!("  --columns <c1,c2,...>     Output columns for read-plan check (default: c1,c2,c3)");
    println!("  --fs-option <k=v>         FS option, repeatable (e.g. fs.s3a.endpoint=...)");
    println!("  --skip-plan               Skip read-plan building");
    println!("  -h, --help                Show this help");
    println!();
    println!("Example:");
    println!("  cargo run --manifest-path tools/Cargo.toml --bin starrocks_inspect -- \\");
    println!("    --tablet-id 10035 --version 4 --root /tmp/starrocks_tablet --columns c1,c2,c3");
}

fn parse_i64(value: &str, flag: &str) -> Result<i64, String> {
    value
        .parse::<i64>()
        .map_err(|e| format!("invalid value for {flag}: {value} ({e})"))
}

fn parse_args() -> Result<InspectConfig, String> {
    let mut cfg = InspectConfig {
        tablet_id: 10035,
        version: 4,
        root: "/tmp/starrocks_tablet".to_string(),
        columns: vec!["c1".to_string(), "c2".to_string(), "c3".to_string()],
        fs_options: BTreeMap::new(),
        skip_plan: false,
    };

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            "--tablet-id" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--tablet-id expects a value".to_string())?;
                cfg.tablet_id = parse_i64(&value, "--tablet-id")?;
            }
            "--version" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--version expects a value".to_string())?;
                cfg.version = parse_i64(&value, "--version")?;
            }
            "--root" => {
                cfg.root = args
                    .next()
                    .ok_or_else(|| "--root expects a value".to_string())?;
            }
            "--columns" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--columns expects a value".to_string())?;
                let parsed = value
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToString::to_string)
                    .collect::<Vec<_>>();
                if parsed.is_empty() {
                    return Err("--columns cannot be empty".to_string());
                }
                cfg.columns = parsed;
            }
            "--fs-option" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--fs-option expects key=value".to_string())?;
                let (k, v) = value
                    .split_once('=')
                    .ok_or_else(|| format!("invalid --fs-option (expected key=value): {value}"))?;
                let key = k.trim();
                if key.is_empty() {
                    return Err(format!("invalid --fs-option key: {value}"));
                }
                cfg.fs_options.insert(key.to_string(), v.trim().to_string());
            }
            "--skip-plan" => {
                cfg.skip_plan = true;
            }
            other => {
                return Err(format!("unknown argument: {other}"));
            }
        }
    }

    Ok(cfg)
}

fn decode_footers_from_snapshot_with_io(
    snapshot: &starust::formats::starrocks::metadata::StarRocksTabletSnapshot,
    tablet_root_path: &str,
    fs_options: &BTreeMap<String, String>,
) -> Result<Vec<StarRocksSegmentFooter>, String> {
    let root = TabletRoot::parse(tablet_root_path)?;
    let op = build_operator(&root, fs_options)?;
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| format!("create tokio runtime for inspect failed: {e}"))?;

    let mut out = Vec::with_capacity(snapshot.segment_files.len());
    for (idx, seg) in snapshot.segment_files.iter().enumerate() {
        let (mode, decode_bytes) = match (seg.bundle_file_offset, seg.segment_size) {
            (Some(offset), Some(size)) => {
                let start = u64::try_from(offset).map_err(|_| {
                    format!(
                        "invalid segment bundle offset: index={idx}, path={}, offset={offset}",
                        seg.path
                    )
                })?;
                let end = start.checked_add(size).ok_or_else(|| {
                    format!(
                        "segment range overflow: index={idx}, path={}, offset={offset}, size={size}",
                        seg.path
                    )
                })?;
                let bytes = read_range_bytes(&rt, &op, &seg.relative_path, start, end).map_err(|e| {
                    format!(
                        "read bundle segment range failed: index={idx}, path={}, range={}..{}, error={e}",
                        seg.path, start, end
                    )
                })?;
                ("bundle", bytes)
            }
            (None, Some(_)) | (None, None) => {
                let bytes = read_all_bytes(&rt, &op, &seg.relative_path).map_err(|e| {
                    format!(
                        "read standalone segment failed: index={idx}, path={}, error={e}",
                        seg.path
                    )
                })?;
                ("standalone", bytes)
            }
            (Some(offset), None) => {
                return Err(format!(
                    "missing segment_size for bundle segment: index={idx}, path={}, offset={offset}",
                    seg.path
                ));
            }
        };
        let footer = decode_segment_footer(&seg.path, &decode_bytes).map_err(|e| {
            format!(
                "decode segment footer failed: index={idx}, path={}, error={e}",
                seg.path
            )
        });
        let footer = footer?;
        println!(
            "decoded footer from segment[{idx}] mode={mode}: rel={}, path={}",
            seg.relative_path, seg.path
        );
        out.push(footer);
    }
    Ok(out)
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TabletRoot {
    Local { root_dir: String },
    ObjectStore { bucket: String, root_path: String },
}

impl TabletRoot {
    fn parse(tablet_root_path: &str) -> Result<Self, String> {
        let raw = tablet_root_path.trim().trim_end_matches('/');
        if raw.is_empty() {
            return Err("tablet_root_path is empty".to_string());
        }

        if let Some(rest) = raw
            .strip_prefix("s3://")
            .or_else(|| raw.strip_prefix("oss://"))
        {
            let (bucket, root_path) = split_bucket_and_root(rest)?;
            return Ok(Self::ObjectStore { bucket, root_path });
        }

        if raw.contains("://") {
            return Err(format!(
                "unsupported tablet_root_path scheme for inspect: {raw}"
            ));
        }
        Ok(Self::Local {
            root_dir: raw.to_string(),
        })
    }
}

fn split_bucket_and_root(value: &str) -> Result<(String, String), String> {
    let trimmed = value.trim_matches('/');
    let (bucket, root_path) = match trimmed.split_once('/') {
        Some((bucket, path)) => (bucket, path),
        None => (trimmed, ""),
    };
    if bucket.trim().is_empty() {
        return Err(format!("missing bucket in tablet_root_path: {value}"));
    }
    Ok((bucket.to_string(), root_path.trim_matches('/').to_string()))
}

fn build_operator(
    root: &TabletRoot,
    fs_options: &BTreeMap<String, String>,
) -> Result<Operator, String> {
    match root {
        TabletRoot::Local { root_dir } => {
            let builder = opendal::services::Fs::default().root(root_dir);
            let operator_builder =
                Operator::new(builder).map_err(|e| format!("init local operator failed: {e}"))?;
            Ok(operator_builder.finish())
        }
        TabletRoot::ObjectStore { bucket, root_path } => {
            build_s3_operator(bucket, root_path, fs_options)
        }
    }
}

fn build_s3_operator(
    bucket: &str,
    root_path: &str,
    fs_options: &BTreeMap<String, String>,
) -> Result<Operator, String> {
    let endpoint = fs_options
        .get(FS_S3A_ENDPOINT)
        .ok_or_else(|| "missing fs.s3a.endpoint for inspect".to_string())?;
    let endpoint = normalize_endpoint(endpoint, fs_options.get(FS_S3A_ENABLE_SSL))?;

    let mut builder = opendal::services::S3::default()
        .bucket(bucket)
        .endpoint(&endpoint);
    if !root_path.is_empty() {
        builder = builder.root(&format!("/{}", root_path));
    }
    if let Some(region) = fs_options
        .get(FS_S3A_ENDPOINT_REGION)
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
    {
        builder = builder.region(region);
    } else {
        builder = builder.region("us-east-1");
    }
    if let Some(access_key) = fs_options
        .get(FS_S3A_ACCESS_KEY)
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
    {
        builder = builder.access_key_id(access_key);
    }
    if let Some(secret_key) = fs_options
        .get(FS_S3A_SECRET_KEY)
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
    {
        builder = builder.secret_access_key(secret_key);
    }
    if let Some(token) = fs_options
        .get(FS_S3A_SESSION_TOKEN)
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
    {
        builder = builder.session_token(token);
    }

    let path_style = fs_options
        .get(FS_S3A_PATH_STYLE)
        .map(|v| is_true_value(v))
        .unwrap_or(false);
    if !path_style {
        builder = builder.enable_virtual_host_style();
    }

    let operator_builder =
        Operator::new(builder).map_err(|e| format!("init object store operator failed: {e}"))?;
    Ok(operator_builder.finish())
}

fn normalize_endpoint(raw_endpoint: &str, ssl_option: Option<&String>) -> Result<String, String> {
    let endpoint = raw_endpoint.trim();
    if endpoint.is_empty() {
        return Err("empty fs.s3a.endpoint for inspect".to_string());
    }
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        return Ok(endpoint.to_string());
    }
    let enable_ssl = ssl_option.map(|v| is_true_value(v)).unwrap_or(true);
    let scheme = if enable_ssl { "https" } else { "http" };
    Ok(format!("{scheme}://{endpoint}"))
}

fn is_true_value(value: &str) -> bool {
    value.trim().eq_ignore_ascii_case("true") || value.trim() == "1"
}

fn read_all_bytes(
    rt: &tokio::runtime::Runtime,
    op: &Operator,
    path: &str,
) -> Result<Vec<u8>, String> {
    rt.block_on(op.read(path)).map(|v| v.to_vec()).map_err(|e| {
        if e.kind() == ErrorKind::NotFound {
            format!("segment file not found: {}", path)
        } else {
            format!("read segment file failed: path={}, error={}", path, e)
        }
    })
}

fn read_range_bytes(
    rt: &tokio::runtime::Runtime,
    op: &Operator,
    path: &str,
    start: u64,
    end: u64,
) -> Result<Vec<u8>, String> {
    if end <= start {
        return Err(format!(
            "invalid read range for inspect: path={}, start={}, end={}",
            path, start, end
        ));
    }
    rt.block_on(op.read_with(path).range(start..end).into_future())
        .map(|v| v.to_vec())
        .map_err(|e| {
            if e.kind() == ErrorKind::NotFound {
                format!("segment file not found: {}", path)
            } else {
                format!(
                    "read segment file range failed: path={}, range={}..{}, error={}",
                    path, start, end, e
                )
            }
        })
}

fn main() {
    let cfg = parse_args().unwrap_or_else(|e| {
        eprintln!("argument error: {e}");
        eprintln!("run with --help for usage");
        std::process::exit(2);
    });

    let snapshot = load_tablet_snapshot(cfg.tablet_id, cfg.version, &cfg.root, &cfg.fs_options)
        .unwrap_or_else(|e| panic!("load_tablet_snapshot failed: {e}"));
    println!(
        "snapshot: tablet_id={}, version={}, segments={}",
        snapshot.tablet_id,
        snapshot.version,
        snapshot.segment_files.len()
    );
    for (idx, seg) in snapshot.segment_files.iter().enumerate() {
        println!(
            "segment[{idx}]: rel={}, offset={:?}, size={:?}",
            seg.relative_path, seg.bundle_file_offset, seg.segment_size
        );
    }

    let footers = decode_footers_from_snapshot_with_io(&snapshot, &cfg.root, &cfg.fs_options)
        .unwrap_or_else(|e| panic!("decode_footers_from_snapshot failed: {e}"));
    for (sidx, footer) in footers.iter().enumerate() {
        println!(
            "footer[{sidx}]: version={:?}, num_rows={:?}, columns={}",
            footer.version,
            footer.num_rows,
            footer.columns.len()
        );
        for (cidx, c) in footer.columns.iter().enumerate() {
            println!(
                "  col[{cidx}]: uid={:?}, logical_type={:?}, encoding={:?}, compression={:?}, ordinal_root={:?}, root_is_data={:?}",
                c.unique_id,
                c.logical_type,
                c.encoding,
                c.compression,
                c.ordinal_index_root_page
                    .as_ref()
                    .map(|p| (p.offset, p.size)),
                c.ordinal_index_root_is_data_page
            );
        }
    }

    if cfg.skip_plan {
        println!("read_plan: skipped (--skip-plan)");
        return;
    }

    let output_schema = Arc::new(Schema::new(
        cfg.columns
            .iter()
            .map(|name| Field::new(name, DataType::Int64, true))
            .collect::<Vec<_>>(),
    ));
    match build_native_read_plan(&snapshot, &footers, &output_schema) {
        Ok(plan) => {
            println!(
                "read_plan: tablet_id={}, version={}, estimated_rows={}, projected_columns={}",
                plan.tablet_id,
                plan.version,
                plan.estimated_rows,
                plan.projected_columns.len()
            );
            for col in &plan.projected_columns {
                println!(
                    "  plan_col: out_idx={}, name={}, uid={}, schema_type={}",
                    col.output_index, col.output_name, col.schema_unique_id, col.schema_type
                );
            }
        }
        Err(e) => {
            println!("read_plan: skipped (reason: {e})");
        }
    }
}
