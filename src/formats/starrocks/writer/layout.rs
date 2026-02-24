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

use sha2::{Digest, Sha256};

use crate::formats::starrocks::writer::StarRocksWriteFormat;
use crate::service::grpc_client::proto::starrocks::PUniqueId;

pub const META_DIR: &str = "meta";
pub const DATA_DIR: &str = "data";
pub const LOG_DIR: &str = "log";
pub const BUNDLE_TABLET_ID: i64 = 0;

#[allow(dead_code)]
pub fn build_data_file_name(
    tablet_id: i64,
    version: i64,
    txn_id: i64,
    driver_id: i32,
    file_seq: u64,
    write_format: StarRocksWriteFormat,
) -> Result<String, String> {
    if txn_id <= 0 {
        return Err(format!(
            "invalid txn_id for data file name generation: {}",
            txn_id
        ));
    }
    let suffix = write_format_suffix(write_format);
    let seed = format!(
        "bundle_data_file:tablet={tablet_id}:version={version}:txn={txn_id}:driver={driver_id}:seq={file_seq}:suffix={suffix}"
    );
    let uuid = deterministic_uuid_v4_from_seed(&seed);
    Ok(format!("{:016x}_{}.{}", txn_id as u64, uuid, suffix))
}

pub fn build_txn_data_file_name(
    tablet_id: i64,
    txn_id: i64,
    driver_id: i32,
    file_seq: u64,
    write_format: StarRocksWriteFormat,
    load_id: Option<&PUniqueId>,
) -> Result<String, String> {
    if txn_id <= 0 {
        return Err(format!(
            "invalid txn_id for data file name generation: {}",
            txn_id
        ));
    }
    let suffix = write_format_suffix(write_format);
    let load_id_seed = if let Some(load_id) = load_id {
        format!("{}:{}", load_id.hi, load_id.lo)
    } else {
        "none".to_string()
    };
    let seed = format!(
        "txn_data_file:tablet={tablet_id}:txn={txn_id}:driver={driver_id}:seq={file_seq}:load={load_id_seed}:suffix={suffix}"
    );
    let uuid = deterministic_uuid_v4_from_seed(&seed);
    Ok(format!("{:016x}_{}.{}", txn_id as u64, uuid, suffix))
}

fn write_format_suffix(write_format: StarRocksWriteFormat) -> &'static str {
    match write_format {
        StarRocksWriteFormat::Native => "dat",
        StarRocksWriteFormat::Parquet => "parquet",
    }
}

fn deterministic_uuid_v4_from_seed(seed: &str) -> String {
    let digest = Sha256::digest(seed.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[0..16]);

    // Set RFC 4122 variant/version to standard UUIDv4 layout.
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;

    let hex = hex::encode(bytes);
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

pub fn bundle_meta_file_path(tablet_root_path: &str, version: i64) -> Result<String, String> {
    if version < 0 {
        return Err(format!("invalid negative version: {}", version));
    }
    join_tablet_path(
        tablet_root_path,
        &format!(
            "{META_DIR}/{:016X}_{:016X}.meta",
            BUNDLE_TABLET_ID as u64, version as u64
        ),
    )
}

pub fn txn_log_file_path(
    tablet_root_path: &str,
    tablet_id: i64,
    txn_id: i64,
) -> Result<String, String> {
    if tablet_id <= 0 {
        return Err(format!("invalid tablet_id for txn log path: {}", tablet_id));
    }
    if txn_id <= 0 {
        return Err(format!("invalid txn_id for txn log path: {}", txn_id));
    }
    join_tablet_path(
        tablet_root_path,
        &format!(
            "{LOG_DIR}/{:016X}_{:016X}.log",
            tablet_id as u64, txn_id as u64
        ),
    )
}

pub fn txn_log_file_path_with_load_id(
    tablet_root_path: &str,
    tablet_id: i64,
    txn_id: i64,
    load_id: &PUniqueId,
) -> Result<String, String> {
    if tablet_id <= 0 {
        return Err(format!(
            "invalid tablet_id for txn log path with load_id: {}",
            tablet_id
        ));
    }
    if txn_id <= 0 {
        return Err(format!(
            "invalid txn_id for txn log path with load_id: {}",
            txn_id
        ));
    }
    join_tablet_path(
        tablet_root_path,
        &format!(
            "{LOG_DIR}/{:016X}_{:016X}_{:016X}_{:016X}.log",
            tablet_id as u64, txn_id as u64, load_id.hi as u64, load_id.lo as u64
        ),
    )
}

pub fn combined_txn_log_file_path(tablet_root_path: &str, txn_id: i64) -> Result<String, String> {
    if txn_id <= 0 {
        return Err(format!(
            "invalid txn_id for combined txn log path: {}",
            txn_id
        ));
    }
    join_tablet_path(
        tablet_root_path,
        &format!("{LOG_DIR}/{:016X}.logs", txn_id as u64),
    )
}

pub fn join_tablet_path(tablet_root_path: &str, rel: &str) -> Result<String, String> {
    let root = tablet_root_path.trim().trim_end_matches('/');
    if root.is_empty() {
        return Err("tablet_root_path is empty".to_string());
    }
    let rel = rel.trim_start_matches('/');
    if rel.is_empty() {
        return Ok(root.to_string());
    }
    Ok(format!("{}/{}", root, rel))
}

#[cfg(test)]
mod tests {
    use super::{StarRocksWriteFormat, build_txn_data_file_name, txn_log_file_path_with_load_id};
    use crate::service::grpc_client::proto::starrocks::PUniqueId;

    #[test]
    fn txn_log_path_with_load_id_matches_starrocks_filename_pattern() {
        let path = txn_log_file_path_with_load_id(
            "/tmp/novarocks_lake_path_test",
            0x2741,
            0x37,
            &PUniqueId { hi: 0x11, lo: 0x22 },
        )
        .expect("build load-id path");
        assert!(
            path.ends_with(
                "log/0000000000002741_0000000000000037_0000000000000011_0000000000000022.log"
            ),
            "unexpected load-id path: {}",
            path
        );
    }

    #[test]
    fn build_txn_data_file_name_matches_txn_uuid_pattern() {
        let name = build_txn_data_file_name(0x2741, 0x37, 2, 9, StarRocksWriteFormat::Native, None)
            .expect("build txn data file name");
        assert!(
            name.starts_with("0000000000000037_"),
            "unexpected txn data file name: {}",
            name
        );
        assert!(name.ends_with(".dat"), "unexpected suffix: {}", name);

        let uuid = name
            .strip_prefix("0000000000000037_")
            .and_then(|v| v.strip_suffix(".dat"))
            .expect("strip txn uuid pattern");
        assert_eq!(uuid.len(), 36, "uuid length mismatch: {}", uuid);
        let bytes = uuid.as_bytes();
        assert_eq!(bytes[8], b'-', "uuid format mismatch: {}", uuid);
        assert_eq!(bytes[13], b'-', "uuid format mismatch: {}", uuid);
        assert_eq!(bytes[18], b'-', "uuid format mismatch: {}", uuid);
        assert_eq!(bytes[23], b'-', "uuid format mismatch: {}", uuid);
    }

    #[test]
    fn build_txn_data_file_name_is_stable_for_same_writer() {
        let first =
            build_txn_data_file_name(0x2741, 0x37, 2, 9, StarRocksWriteFormat::Native, None)
                .expect("build first txn data file name");
        let second =
            build_txn_data_file_name(0x2741, 0x37, 2, 9, StarRocksWriteFormat::Native, None)
                .expect("build second txn data file name");
        assert_eq!(
            first, second,
            "same writer should generate stable txn+uuid file name"
        );
    }

    #[test]
    fn build_txn_data_file_name_changes_with_load_id_seed() {
        let without_load =
            build_txn_data_file_name(0x2741, 0x37, 2, 9, StarRocksWriteFormat::Native, None)
                .expect("build txn data file name without load id");
        let with_load = build_txn_data_file_name(
            0x2741,
            0x37,
            2,
            9,
            StarRocksWriteFormat::Native,
            Some(&PUniqueId { hi: 0x11, lo: 0x22 }),
        )
        .expect("build txn data file name with load id");
        assert_ne!(
            without_load, with_load,
            "load_id should participate in deterministic uuid seed"
        );
    }
}
