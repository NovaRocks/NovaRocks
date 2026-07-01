mod fs_access_tooling;

use std::collections::BTreeMap;

use novarocks::formats::starrocks::writer::bundle_meta::{
    decode_bundle_metadata_from_bytes, decode_tablet_metadata_from_bundle_bytes,
};
use novarocks::formats::starrocks::writer::layout::bundle_meta_file_path;

#[derive(Debug)]
struct ProbeConfig {
    root: String,
    version: i64,
    tablet_ids: Vec<i64>,
    fs_options: BTreeMap<String, String>,
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
    println!("  --fs-option <k=v>         FS option, repeatable (S3A endpoint/ak/sk/path style)");
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

fn dump_schema(
    schema: &novarocks::service::grpc_client::proto::starrocks::TabletSchemaPb,
) {
    println!(
        "schema id={:?} schema_version={:?} next_column_unique_id={:?} root_columns={}",
        schema.id,
        schema.schema_version,
        schema.next_column_unique_id,
        schema.column.len()
    );
    for column in &schema.column {
        dump_schema_column(column, 1);
    }
}

fn dump_schema_column(
    column: &novarocks::service::grpc_client::proto::starrocks::ColumnPb,
    depth: usize,
) {
    let indent = "  ".repeat(depth);
    println!(
        "{indent}col name={:?} unique_id={} type={} nullable={:?} key={:?} children={}",
        column.name,
        column.unique_id,
        column.r#type,
        column.is_nullable,
        column.is_key,
        column.children_columns.len()
    );
    for child in &column.children_columns {
        dump_schema_column(child, depth + 1);
    }
}

fn main() -> Result<(), String> {
    let cfg = parse_args()?;
    let object_store_config =
        fs_access_tooling::object_store_config_from_fs_options(&cfg.fs_options)?;
    let meta_path = bundle_meta_file_path(&cfg.root, cfg.version)?;
    let access =
        fs_access_tooling::resolve_tool_location(&meta_path, object_store_config.as_ref())?;
    let rel = fs_access_tooling::single_relative_path(&access, &meta_path)?;

    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| format!("create tokio runtime failed: {}", e))?;
    let op = access.operator();
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
            Ok(meta) => {
                dump_tablet_meta(tablet_id, &meta);
                if let Some(schema_id) = bundle.tablet_to_schema.get(&tablet_id) {
                    println!("tablet_to_schema tablet_id={} schema_id={}", tablet_id, schema_id);
                    if let Some(schema) = bundle.schemas.get(schema_id) {
                        dump_schema(schema);
                    } else {
                        println!("schema missing for schema_id={}", schema_id);
                    }
                } else {
                    println!("tablet_to_schema missing for tablet_id={}", tablet_id);
                }
            }
            Err(err) => println!("tablet={} decode_error={}", tablet_id, err),
        }
    }
    Ok(())
}
