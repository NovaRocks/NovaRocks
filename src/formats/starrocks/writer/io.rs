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

use std::fs;
use std::path::PathBuf;

use crate::formats::starrocks::fs_access::resolve_format_path;
use crate::fs::access::FsScheme;
use opendal::ErrorKind;

pub fn write_bytes(path: &str, bytes: Vec<u8>) -> Result<(), String> {
    reject_hdfs_path(path, "write_bytes")?;
    let access = resolve_format_path(path)?;
    match access.scheme() {
        FsScheme::Local => {
            let path_buf = PathBuf::from(path);
            if let Some(parent) = path_buf.parent() {
                fs::create_dir_all(parent)
                    .map_err(|e| format!("create parent dir failed: {}", e))?;
            }
            fs::write(path_buf, bytes).map_err(|e| format!("write file failed: {}", e))
        }
        FsScheme::ObjectStore => {
            let rel = access.single_relative_path()?.to_string();
            let write_result =
                crate::fs::object_store::oss_block_on(access.operator().write(&rel, bytes))?;
            write_result.map_err(|e| format!("write object failed: {}", e))?;
            Ok(())
        }
        FsScheme::Hdfs => Err(format!(
            "write_bytes does not support hdfs path yet: {}",
            path
        )),
    }
}

#[allow(dead_code)]
pub fn read_bytes(path: &str) -> Result<Vec<u8>, String> {
    reject_hdfs_path(path, "read_bytes")?;
    let access = resolve_format_path(path)?;
    match access.scheme() {
        FsScheme::Local => fs::read(path).map_err(|e| format!("read file failed: {}", e)),
        FsScheme::ObjectStore => {
            let rel = access.single_relative_path()?.to_string();
            let read_result = crate::fs::object_store::oss_block_on(access.operator().read(&rel))?;
            let bytes = read_result.map_err(|e| format!("read object failed: {}", e))?;
            Ok(bytes.to_vec())
        }
        FsScheme::Hdfs => Err(format!(
            "read_bytes does not support hdfs path yet: {}",
            path
        )),
    }
}

pub fn read_bytes_if_exists(path: &str) -> Result<Option<Vec<u8>>, String> {
    reject_hdfs_path(path, "read_bytes_if_exists")?;
    let access = resolve_format_path(path)?;
    match access.scheme() {
        FsScheme::Local => {
            let path_buf = PathBuf::from(path);
            if !path_buf.exists() {
                return Ok(None);
            }
            fs::read(path_buf)
                .map(Some)
                .map_err(|e| format!("read file failed: {}", e))
        }
        FsScheme::ObjectStore => {
            let rel = access.single_relative_path()?.to_string();
            match crate::fs::object_store::oss_block_on(access.operator().read(&rel))? {
                Ok(bytes) => Ok(Some(bytes.to_vec())),
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
                Err(e) => Err(format!("read object failed: {}", e)),
            }
        }
        FsScheme::Hdfs => Err(format!(
            "read_bytes_if_exists does not support hdfs path yet: {}",
            path
        )),
    }
}

pub fn delete_path_if_exists(path: &str) -> Result<(), String> {
    reject_hdfs_path(path, "delete_path_if_exists")?;
    let access = resolve_format_path(path)?;
    match access.scheme() {
        FsScheme::Local => {
            let path_buf = PathBuf::from(path);
            if !path_buf.exists() {
                return Ok(());
            }
            fs::remove_file(path_buf).map_err(|e| format!("delete file failed: {}", e))
        }
        FsScheme::ObjectStore => {
            let rel = access.single_relative_path()?.to_string();
            match crate::fs::object_store::oss_block_on(access.operator().delete(&rel))? {
                Ok(_) => Ok(()),
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
                Err(e) => Err(format!("delete object failed: {}", e)),
            }
        }
        FsScheme::Hdfs => Err(format!(
            "delete_path_if_exists does not support hdfs path yet: {}",
            path
        )),
    }
}

fn reject_hdfs_path(path: &str, function_name: &str) -> Result<(), String> {
    let trimmed = path.trim();
    if trimmed
        .split_once("://")
        .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case("hdfs"))
    {
        return Err(format!(
            "{function_name} does not support hdfs path yet: {path}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_helpers_round_trip_local_path() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let path = temp_dir
            .path()
            .join("nested")
            .join("payload.bin")
            .to_string_lossy()
            .to_string();

        assert_eq!(
            read_bytes_if_exists(&path).expect("read missing file"),
            None
        );

        write_bytes(&path, b"hello writer io".to_vec()).expect("write bytes");

        assert_eq!(read_bytes(&path).expect("read bytes"), b"hello writer io");
        assert_eq!(
            read_bytes_if_exists(&path).expect("read existing bytes"),
            Some(b"hello writer io".to_vec())
        );

        delete_path_if_exists(&path).expect("delete bytes");
        assert_eq!(
            read_bytes_if_exists(&path).expect("read deleted file"),
            None
        );
        delete_path_if_exists(&path).expect("delete missing file");
    }

    #[test]
    fn byte_helpers_use_format_path_resolver_for_object_store_credentials() {
        let _guard = crate::connector::starrocks::lake::context::lock_runtime_test_state();
        let path = "s3://missing-bucket/warehouse/tablet-1/1.meta";

        let err = read_bytes_if_exists(path).expect_err("missing runtime S3 config must fail");

        assert!(
            err.contains("missing S3 config for StarRocks object-store path="),
            "{err}"
        );
    }

    #[test]
    fn byte_helpers_reject_malformed_hdfs_with_function_specific_errors() {
        let path = "hdfs://nn:9000";

        let write_err = write_bytes(path, b"payload".to_vec()).expect_err("hdfs write must fail");
        assert!(
            write_err.contains("write_bytes does not support hdfs path yet"),
            "{write_err}"
        );

        let read_if_exists_err =
            read_bytes_if_exists(path).expect_err("hdfs read-if-exists must fail");
        assert!(
            read_if_exists_err.contains("read_bytes_if_exists does not support hdfs path yet"),
            "{read_if_exists_err}"
        );
    }
}
