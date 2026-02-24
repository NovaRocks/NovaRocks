use std::collections::BTreeMap;

use opendal::Operator;

use novarocks::formats::starrocks::writer::bundle_meta::{
    decode_bundle_metadata_from_bytes, decode_tablet_metadata_from_bundle_bytes,
};
use novarocks::formats::starrocks::writer::layout::bundle_meta_file_path;

const FS_S3A_ACCESS_KEY: &str = "fs.s3a.access.key";
const FS_S3A_SECRET_KEY: &str = "fs.s3a.secret.key";
const FS_S3A_ENDPOINT: &str = "fs.s3a.endpoint";
const FS_S3A_ENDPOINT_REGION: &str = "fs.s3a.endpoint.region";
const FS_S3A_PATH_STYLE: &str = "fs.s3a.path.style.access";

#[derive(Debug)]
struct ProbeConfig {
    root: String,
    version: i64,
    tablet_ids: Vec<i64>,
    fs_options: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RootPath {
    Local { root_dir: String },
    ObjectStore { bucket: String, root_path: String },
}

fn print_help() {
    println!("bundle_meta_probe");
    println!();
    println!("Inspect StarRocks bundle metadata file and dump tablet metadata summary.");
    println!();
    println!("Options:");
    println!("  --root <path>             Tablet root path, local or s3://bucket/root");
    println!("  --version <v>             Bundle version (default: 1)");
    println!("  --tablet-id <id>          Target tablet id (repeatable, optional)");
    println!("  --fs-option <k=v>         FS option, repeatable (s3 endpoint/ak/sk/path style)");
    println!("  -h, --help                Show this help");
}

fn parse_args() -> Result<ProbeConfig, String> {
    let mut cfg = ProbeConfig {
        root: String::new(),
        version: 1,
        tablet_ids: Vec::new(),
        fs_options: BTreeMap::new(),
    };

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--root" => {
                cfg.root = args
                    .next()
                    .ok_or_else(|| "--root expects a value".to_string())?;
            }
            "--version" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--version expects a value".to_string())?;
                cfg.version = raw
                    .parse::<i64>()
                    .map_err(|e| format!("invalid --version '{}': {}", raw, e))?;
            }
            "--tablet-id" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--tablet-id expects a value".to_string())?;
                let tid = raw
                    .parse::<i64>()
                    .map_err(|e| format!("invalid --tablet-id '{}': {}", raw, e))?;
                cfg.tablet_ids.push(tid);
            }
            "--fs-option" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--fs-option expects key=value".to_string())?;
                let (k, v) = value
                    .split_once('=')
                    .ok_or_else(|| format!("invalid --fs-option: {}", value))?;
                cfg.fs_options
                    .insert(k.trim().to_string(), v.trim().to_string());
            }
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            other => {
                return Err(format!("unknown argument: {}", other));
            }
        }
    }

    if cfg.root.trim().is_empty() {
        return Err("--root is required".to_string());
    }
    if cfg.version <= 0 {
        return Err("--version must be positive".to_string());
    }
    Ok(cfg)
}

fn parse_root(path: &str) -> Result<RootPath, String> {
    let raw = path.trim().trim_end_matches('/');
    if raw.is_empty() {
        return Err("empty root path".to_string());
    }
    if let Some(rest) = raw
        .strip_prefix("s3://")
        .or_else(|| raw.strip_prefix("oss://"))
    {
        let (bucket, root_path) = split_bucket_and_root(rest)?;
        return Ok(RootPath::ObjectStore { bucket, root_path });
    }
    if raw.contains("://") {
        return Err(format!("unsupported root path scheme: {}", raw));
    }
    Ok(RootPath::Local {
        root_dir: raw.to_string(),
    })
}

fn split_bucket_and_root(value: &str) -> Result<(String, String), String> {
    let trimmed = value.trim_matches('/');
    let (bucket, root_path) = match trimmed.split_once('/') {
        Some((bucket, path)) => (bucket.trim(), path.trim_matches('/')),
        None => (trimmed, ""),
    };
    if bucket.is_empty() {
        return Err(format!("invalid bucket in root path: {}", value));
    }
    Ok((bucket.to_string(), root_path.to_string()))
}

fn parse_bool(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Some(true),
        "false" | "0" | "no" => Some(false),
        _ => None,
    }
}

fn build_operator(root: &RootPath, fs_options: &BTreeMap<String, String>) -> Result<Operator, String> {
    match root {
        RootPath::Local { root_dir } => {
            let builder = opendal::services::Fs::default().root(root_dir);
            let op_builder =
                Operator::new(builder).map_err(|e| format!("init local operator failed: {}", e))?;
            Ok(op_builder.finish())
        }
        RootPath::ObjectStore { bucket, root_path } => {
            let endpoint = fs_options
                .get(FS_S3A_ENDPOINT)
                .map(|v| v.trim())
                .filter(|v| !v.is_empty())
                .ok_or_else(|| format!("missing fs option: {}", FS_S3A_ENDPOINT))?;
            let access_key = fs_options
                .get(FS_S3A_ACCESS_KEY)
                .map(|v| v.trim())
                .filter(|v| !v.is_empty())
                .ok_or_else(|| format!("missing fs option: {}", FS_S3A_ACCESS_KEY))?;
            let secret_key = fs_options
                .get(FS_S3A_SECRET_KEY)
                .map(|v| v.trim())
                .filter(|v| !v.is_empty())
                .ok_or_else(|| format!("missing fs option: {}", FS_S3A_SECRET_KEY))?;

            let mut builder = opendal::services::S3::default()
                .bucket(bucket)
                .root(root_path)
                .endpoint(endpoint)
                .access_key_id(access_key)
                .secret_access_key(secret_key);

            if let Some(region) = fs_options
                .get(FS_S3A_ENDPOINT_REGION)
                .map(|v| v.trim())
                .filter(|v| !v.is_empty())
            {
                builder = builder.region(region);
            }
            if let Some(path_style) = fs_options.get(FS_S3A_PATH_STYLE).and_then(|v| parse_bool(v))
            {
                if !path_style {
                    builder = builder.enable_virtual_host_style();
                }
            }
            let op_builder =
                Operator::new(builder).map_err(|e| format!("init s3 operator failed: {}", e))?;
            Ok(op_builder.finish())
        }
    }
}

fn to_relative_path(full_path: &str, root: &RootPath) -> Result<String, String> {
    match root {
        RootPath::Local { root_dir } => {
            let prefix = format!("{}/", root_dir.trim_end_matches('/'));
            if let Some(rest) = full_path.strip_prefix(&prefix) {
                return Ok(rest.to_string());
            }
            if full_path == root_dir {
                return Ok(String::new());
            }
            Err(format!(
                "path does not belong to local root: path={} root={}",
                full_path, root_dir
            ))
        }
        RootPath::ObjectStore { bucket, root_path } => {
            let raw = full_path
                .strip_prefix("s3://")
                .or_else(|| full_path.strip_prefix("oss://"))
                .ok_or_else(|| format!("unexpected object path: {}", full_path))?;
            let (path_bucket, key) = raw
                .split_once('/')
                .ok_or_else(|| format!("invalid object path: {}", full_path))?;
            if path_bucket != bucket {
                return Err(format!(
                    "bucket mismatch: path={} expected_bucket={}",
                    full_path, bucket
                ));
            }
            let root_trim = root_path.trim_matches('/');
            if root_trim.is_empty() {
                return Ok(key.trim_start_matches('/').to_string());
            }
            let prefix = format!("{}/", root_trim);
            let key = key.trim_start_matches('/');
            if let Some(rest) = key.strip_prefix(&prefix) {
                return Ok(rest.to_string());
            }
            if key == root_trim {
                return Ok(String::new());
            }
            Err(format!(
                "path does not belong to object root: path={} root={}",
                full_path, root_path
            ))
        }
    }
}

fn dump_tablet_meta(tablet_id: i64, meta: &novarocks::service::grpc_client::proto::starrocks::TabletMetadataPb) {
    println!(
        "tablet={} meta_version={:?} rowsets={} next_rowset_id={:?} commit_time={:?} gtid={:?}",
        tablet_id,
        meta.version,
        meta.rowsets.len(),
        meta.next_rowset_id,
        meta.commit_time,
        meta.gtid
    );
    for rowset in &meta.rowsets {
        println!(
            "  rowset id={:?} segments={} del_files={} num_rows={:?} num_dels={:?} version={:?}",
            rowset.id,
            rowset.segments.len(),
            rowset.del_files.len(),
            rowset.num_rows,
            rowset.num_dels,
            rowset.version
        );
        if !rowset.segments.is_empty() {
            println!("    segments={:?}", rowset.segments);
        }
        if !rowset.del_files.is_empty() {
            let mut names = Vec::with_capacity(rowset.del_files.len());
            for del in &rowset.del_files {
                names.push(format!(
                    "{}(origin={:?},op_offset={:?})",
                    del.name.clone().unwrap_or_else(|| "<none>".to_string()),
                    del.origin_rowset_id,
                    del.op_offset
                ));
            }
            println!("    del_files={}", names.join(", "));
        }
    }
    if let Some(delvec_meta) = meta.delvec_meta.as_ref() {
        let mut versions = delvec_meta
            .version_to_file
            .keys()
            .copied()
            .collect::<Vec<_>>();
        versions.sort_unstable();
        println!(
            "  delvec versions={} detail={:?}",
            versions.len(),
            versions
        );
    } else {
        println!("  delvec none");
    }
}

fn main() -> Result<(), String> {
    let cfg = parse_args()?;
    let root = parse_root(&cfg.root)?;
    let meta_path = bundle_meta_file_path(&cfg.root, cfg.version)?;
    let rel = to_relative_path(&meta_path, &root)?;

    let op = build_operator(&root, &cfg.fs_options)?;
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| format!("create tokio runtime failed: {}", e))?;
    let bytes = rt
        .block_on(op.read(&rel))
        .map_err(|e| format!("read bundle meta failed: path={} error={}", rel, e))?
        .to_vec();

    let (bundle, footer_offset) = decode_bundle_metadata_from_bytes(&bytes)?;
    println!(
        "bundle path={} bytes={} footer_offset={} tablet_pages={} schemas={}",
        meta_path,
        bytes.len(),
        footer_offset,
        bundle.tablet_meta_pages.len(),
        bundle.schemas.len()
    );

    let mut all_tablet_ids = bundle.tablet_meta_pages.keys().copied().collect::<Vec<_>>();
    all_tablet_ids.sort_unstable();
    println!("bundle tablet_ids={:?}", all_tablet_ids);

    let mut target_tablet_ids = if cfg.tablet_ids.is_empty() {
        all_tablet_ids
    } else {
        cfg.tablet_ids.clone()
    };
    target_tablet_ids.sort_unstable();
    target_tablet_ids.dedup();

    for tablet_id in target_tablet_ids {
        match decode_tablet_metadata_from_bundle_bytes(&bytes, tablet_id, cfg.version) {
            Ok(meta) => dump_tablet_meta(tablet_id, &meta),
            Err(err) => println!("tablet={} decode_error={}", tablet_id, err),
        }
    }
    Ok(())
}
