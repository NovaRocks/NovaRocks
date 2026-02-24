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

use crate::fs::path::{ScanPathScheme, classify_scan_paths};
use opendal::ErrorKind;

pub fn write_bytes(path: &str, bytes: Vec<u8>) -> Result<(), String> {
    let scheme = classify_scan_paths([path])?;
    match scheme {
        ScanPathScheme::Local => {
            let path_buf = PathBuf::from(path);
            if let Some(parent) = path_buf.parent() {
                fs::create_dir_all(parent)
                    .map_err(|e| format!("create parent dir failed: {}", e))?;
            }
            fs::write(path_buf, bytes).map_err(|e| format!("write file failed: {}", e))
        }
        ScanPathScheme::Oss => {
            let (op, rel) = crate::fs::oss::resolve_oss_operator_and_path(path)?;
            let write_result = crate::fs::oss::oss_block_on(op.write(&rel, bytes))?;
            write_result.map_err(|e| format!("write object failed: {}", e))?;
            Ok(())
        }
    }
}

#[allow(dead_code)]
pub fn read_bytes(path: &str) -> Result<Vec<u8>, String> {
    let scheme = classify_scan_paths([path])?;
    match scheme {
        ScanPathScheme::Local => fs::read(path).map_err(|e| format!("read file failed: {}", e)),
        ScanPathScheme::Oss => {
            let (op, rel) = crate::fs::oss::resolve_oss_operator_and_path(path)?;
            let read_result = crate::fs::oss::oss_block_on(op.read(&rel))?;
            let bytes = read_result.map_err(|e| format!("read object failed: {}", e))?;
            Ok(bytes.to_vec())
        }
    }
}

pub fn read_bytes_if_exists(path: &str) -> Result<Option<Vec<u8>>, String> {
    let scheme = classify_scan_paths([path])?;
    match scheme {
        ScanPathScheme::Local => {
            let path_buf = PathBuf::from(path);
            if !path_buf.exists() {
                return Ok(None);
            }
            fs::read(path_buf)
                .map(Some)
                .map_err(|e| format!("read file failed: {}", e))
        }
        ScanPathScheme::Oss => {
            let (op, rel) = crate::fs::oss::resolve_oss_operator_and_path(path)?;
            match crate::fs::oss::oss_block_on(op.read(&rel))? {
                Ok(bytes) => Ok(Some(bytes.to_vec())),
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
                Err(e) => Err(format!("read object failed: {}", e)),
            }
        }
    }
}

pub fn delete_path_if_exists(path: &str) -> Result<(), String> {
    let scheme = classify_scan_paths([path])?;
    match scheme {
        ScanPathScheme::Local => {
            let path_buf = PathBuf::from(path);
            if !path_buf.exists() {
                return Ok(());
            }
            fs::remove_file(path_buf).map_err(|e| format!("delete file failed: {}", e))
        }
        ScanPathScheme::Oss => {
            let (op, rel) = crate::fs::oss::resolve_oss_operator_and_path(path)?;
            match crate::fs::oss::oss_block_on(op.delete(&rel))? {
                Ok(_) => Ok(()),
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
                Err(e) => Err(format!("delete object failed: {}", e)),
            }
        }
    }
}
