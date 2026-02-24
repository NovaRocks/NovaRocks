use std::fs;

use prost::Message;

use novarocks::formats::starrocks::writer::layout::{txn_log_file_path, txn_log_file_path_with_load_id};
use novarocks::service::grpc_client::proto::starrocks::{PUniqueId, TxnLogPb};

#[derive(Debug)]
struct ProbeConfig {
    root: String,
    tablet_id: i64,
    txn_id: i64,
    load_id_hi: Option<i64>,
    load_id_lo: Option<i64>,
}

fn print_help() {
    println!("txn_log_probe");
    println!();
    println!("Dump op_write summary from a plain txn log protobuf file.");
    println!("This tool expects extracted raw .log payload (not MinIO xl.meta wrapper).");
    println!();
    println!("Options:");
    println!("  --root <path>       Tablet root path");
    println!("  --tablet-id <id>    Tablet id");
    println!("  --txn-id <id>       Txn id");
    println!("  --load-id-hi <id>   Optional load_id.hi for load-id txn log path");
    println!("  --load-id-lo <id>   Optional load_id.lo for load-id txn log path");
    println!("  -h, --help          Show help");
}

fn parse_args() -> Result<ProbeConfig, String> {
    let mut cfg = ProbeConfig {
        root: String::new(),
        tablet_id: 0,
        txn_id: 0,
        load_id_hi: None,
        load_id_lo: None,
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--root" => {
                cfg.root = args
                    .next()
                    .ok_or_else(|| "--root expects a value".to_string())?;
            }
            "--tablet-id" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--tablet-id expects a value".to_string())?;
                cfg.tablet_id = raw
                    .parse::<i64>()
                    .map_err(|e| format!("invalid --tablet-id '{}': {}", raw, e))?;
            }
            "--txn-id" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--txn-id expects a value".to_string())?;
                cfg.txn_id = raw
                    .parse::<i64>()
                    .map_err(|e| format!("invalid --txn-id '{}': {}", raw, e))?;
            }
            "--load-id-hi" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--load-id-hi expects a value".to_string())?;
                cfg.load_id_hi = Some(
                    raw.parse::<i64>()
                        .map_err(|e| format!("invalid --load-id-hi '{}': {}", raw, e))?,
                );
            }
            "--load-id-lo" => {
                let raw = args
                    .next()
                    .ok_or_else(|| "--load-id-lo expects a value".to_string())?;
                cfg.load_id_lo = Some(
                    raw.parse::<i64>()
                        .map_err(|e| format!("invalid --load-id-lo '{}': {}", raw, e))?,
                );
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
    if cfg.tablet_id <= 0 {
        return Err("--tablet-id must be positive".to_string());
    }
    if cfg.txn_id <= 0 {
        return Err("--txn-id must be positive".to_string());
    }
    if cfg.load_id_hi.is_some() ^ cfg.load_id_lo.is_some() {
        return Err("both --load-id-hi and --load-id-lo are required together".to_string());
    }
    Ok(cfg)
}

fn main() -> Result<(), String> {
    let cfg = parse_args()?;
    let path = if let (Some(hi), Some(lo)) = (cfg.load_id_hi, cfg.load_id_lo) {
        let load_id = PUniqueId { hi, lo };
        txn_log_file_path_with_load_id(&cfg.root, cfg.tablet_id, cfg.txn_id, &load_id)?
    } else {
        txn_log_file_path(&cfg.root, cfg.tablet_id, cfg.txn_id)?
    };
    let bytes = fs::read(&path).map_err(|e| format!("read log failed: path={} error={}", path, e))?;
    let txn_log =
        TxnLogPb::decode(bytes.as_slice()).map_err(|e| format!("decode txn log failed: {}", e))?;

    println!(
        "txn_log path={} tablet_id={:?} txn_id={:?} load_id={:?}",
        path, txn_log.tablet_id, txn_log.txn_id, txn_log.load_id
    );
    let Some(op_write) = txn_log.op_write.as_ref() else {
        println!("op_write: none");
        return Ok(());
    };
    println!(
        "op_write schema_key={:?} dels={} del_encryption_metas={}",
        op_write.schema_key,
        op_write.dels.len(),
        op_write.del_encryption_metas.len()
    );
    if !op_write.dels.is_empty() {
        println!("dels={:?}", op_write.dels);
    }

    if let Some(rowset) = op_write.rowset.as_ref() {
        println!(
            "rowset segments={} del_files={} num_rows={:?} num_dels={:?} segment_size={:?} bundle_offsets={:?}",
            rowset.segments.len(),
            rowset.del_files.len(),
            rowset.num_rows,
            rowset.num_dels,
            rowset.segment_size,
            rowset.bundle_file_offsets
        );
        if !rowset.segments.is_empty() {
            println!("rowset.segments={:?}", rowset.segments);
        }
        if !rowset.del_files.is_empty() {
            println!("rowset.del_files={:?}", rowset.del_files);
        }
    } else {
        println!("rowset: none");
    }
    Ok(())
}
