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

use crate::connector::starrocks::lake::abort_policy::{
    AbortTxnLogSource, decide_abort_txn_log_source,
};
use crate::connector::starrocks::lake::context::get_tablet_runtime;
use crate::connector::starrocks::lake::txn_log::{
    read_combined_txn_log_if_exists, read_txn_log_if_exists,
};
use crate::formats::starrocks::writer::io::delete_path_if_exists;
use crate::formats::starrocks::writer::layout::{
    DATA_DIR, combined_txn_log_file_path, join_tablet_path, txn_log_file_path,
};
use crate::service::grpc_client::proto::starrocks::{TxnInfoPb, TxnLogPb};

pub(crate) fn abort_one_tablet(
    tablet_id: i64,
    txn_infos: &[TxnInfoPb],
    combined_logs_to_delete: &mut HashSet<String>,
) -> Result<(), String> {
    if tablet_id <= 0 {
        return Err(format!("abort_txn has non-positive tablet_id={tablet_id}"));
    }
    let runtime = get_tablet_runtime(tablet_id)?;

    for txn_info in txn_infos {
        let txn_id = txn_info
            .txn_id
            .ok_or_else(|| "abort_txn txn_info missing txn_id".to_string())?;
        match decide_abort_txn_log_source(txn_info) {
            AbortTxnLogSource::Combined => {
                let combined_log_path = combined_txn_log_file_path(&runtime.root_path, txn_id)?;
                if let Some(combined_log) = read_combined_txn_log_if_exists(&combined_log_path)? {
                    for txn_log in &combined_log.txn_logs {
                        if txn_log.tablet_id == Some(tablet_id) {
                            abort_one_txn_log(&runtime.root_path, tablet_id, txn_log)?;
                        }
                    }
                    combined_logs_to_delete.insert(combined_log_path);
                }
            }
            AbortTxnLogSource::PerTablet => {
                let log_path = txn_log_file_path(&runtime.root_path, tablet_id, txn_id)?;
                if let Some(txn_log) = read_txn_log_if_exists(&log_path)? {
                    abort_one_txn_log(&runtime.root_path, tablet_id, &txn_log)?;
                    delete_path_if_exists(&log_path)?;
                }
            }
        }
    }
    Ok(())
}

fn abort_one_txn_log(
    tablet_root_path: &str,
    tablet_id: i64,
    txn_log: &TxnLogPb,
) -> Result<(), String> {
    if let Some(op_write) = txn_log.op_write.as_ref() {
        if let Some(rowset) = op_write.rowset.as_ref() {
            for seg in &rowset.segments {
                let seg_path = join_tablet_path(tablet_root_path, &format!("{DATA_DIR}/{seg}"))?;
                delete_path_if_exists(&seg_path)?;
            }
        }
        for del_file in &op_write.dels {
            let del_path = join_tablet_path(tablet_root_path, &format!("{DATA_DIR}/{del_file}"))?;
            delete_path_if_exists(&del_path)?;
        }
        return Ok(());
    }

    if txn_log.op_compaction.is_some()
        || txn_log.op_schema_change.is_some()
        || txn_log.op_alter_metadata.is_some()
        || txn_log.op_replication.is_some()
    {
        return Err(format!(
            "abort_txn unsupported txn log operation: tablet_id={} txn_id={:?}",
            tablet_id, txn_log.txn_id
        ));
    }
    Ok(())
}
