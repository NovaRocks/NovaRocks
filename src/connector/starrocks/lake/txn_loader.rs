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

use std::collections::HashSet;

use crate::connector::starrocks::lake::txn_log::{
    read_combined_txn_log_if_exists, read_txn_log_if_exists,
};
use crate::formats::starrocks::writer::layout::{
    combined_txn_log_file_path, txn_log_file_path, txn_log_file_path_with_load_id,
    txn_vlog_file_path,
};
use crate::service::grpc_client::proto::starrocks::{TxnInfoPb, TxnLogPb};

#[derive(Clone)]
pub(crate) struct LoadedTxnLog {
    pub(crate) log: TxnLogPb,
}

pub(crate) fn load_txn_logs_for_publish(
    tablet_root_path: &str,
    tablet_id: i64,
    txn_info: &TxnInfoPb,
) -> Result<Vec<LoadedTxnLog>, String> {
    let txn_id = txn_info
        .txn_id
        .ok_or_else(|| "publish_version txn_info missing txn_id".to_string())?;
    if !txn_info.load_ids.is_empty() {
        let mut logs = Vec::with_capacity(txn_info.load_ids.len());
        let mut seen_paths = HashSet::with_capacity(txn_info.load_ids.len());
        for load_id in &txn_info.load_ids {
            let path =
                txn_log_file_path_with_load_id(tablet_root_path, tablet_id, txn_id, load_id)?;
            if !seen_paths.insert(path.clone()) {
                continue;
            }
            if let Some(txn_log) = read_txn_log_if_exists(&path)? {
                logs.push(LoadedTxnLog { log: txn_log });
            }
        }
        if !logs.is_empty() {
            return Ok(logs);
        }
        let fallback_path = txn_log_file_path(tablet_root_path, tablet_id, txn_id)?;
        if let Some(txn_log) = read_txn_log_if_exists(&fallback_path)? {
            return Ok(vec![LoadedTxnLog { log: txn_log }]);
        }
        return Ok(Vec::new());
    }

    if txn_info.combined_txn_log.unwrap_or(false) {
        let combined_path = combined_txn_log_file_path(tablet_root_path, txn_id)?;
        if let Some(combined_log) = read_combined_txn_log_if_exists(&combined_path)? {
            let mut logs = Vec::new();
            for log in combined_log.txn_logs {
                if log.tablet_id == Some(tablet_id) {
                    logs.push(LoadedTxnLog { log });
                }
            }
            if !logs.is_empty() {
                return Ok(logs);
            }
        }

        // FE may mark combined_txn_log=true while writer persists per-tablet txn logs.
        // Fallback keeps publish path compatible with simplified sink writer.
        let tablet_path = txn_log_file_path(tablet_root_path, tablet_id, txn_id)?;
        if let Some(txn_log) = read_txn_log_if_exists(&tablet_path)? {
            return Ok(vec![LoadedTxnLog { log: txn_log }]);
        }
        return Ok(Vec::new());
    }

    let path = txn_log_file_path(tablet_root_path, tablet_id, txn_id)?;
    if let Some(txn_log) = read_txn_log_if_exists(&path)? {
        return Ok(vec![LoadedTxnLog { log: txn_log }]);
    }
    Ok(Vec::new())
}

pub(crate) fn load_txn_vlog_for_publish(
    tablet_root_path: &str,
    tablet_id: i64,
    version: i64,
) -> Result<Option<LoadedTxnLog>, String> {
    if version <= 0 {
        return Err(format!(
            "publish_version requires positive vlog version, got {version}"
        ));
    }
    let path = txn_vlog_file_path(tablet_root_path, tablet_id, version)?;
    let maybe_log = read_txn_log_if_exists(&path)?;
    Ok(maybe_log.map(|log| LoadedTxnLog { log }))
}
