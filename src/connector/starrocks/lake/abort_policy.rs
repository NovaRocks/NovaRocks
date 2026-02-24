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

use crate::service::grpc_client::proto::starrocks::TxnInfoPb;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AbortTxnLogSource {
    Combined,
    PerTablet,
}

pub(crate) fn should_skip_abort_cleanup(skip_cleanup: Option<bool>) -> bool {
    skip_cleanup.unwrap_or(false)
}

pub(crate) fn decide_abort_txn_log_source(txn_info: &TxnInfoPb) -> AbortTxnLogSource {
    if txn_info.combined_txn_log.unwrap_or(false) {
        AbortTxnLogSource::Combined
    } else {
        AbortTxnLogSource::PerTablet
    }
}

#[cfg(test)]
mod tests {
    use crate::service::grpc_client::proto::starrocks::TxnInfoPb;

    use super::{AbortTxnLogSource, decide_abort_txn_log_source, should_skip_abort_cleanup};

    #[test]
    fn skip_cleanup_defaults_to_false() {
        assert!(!should_skip_abort_cleanup(None));
    }

    #[test]
    fn decide_combined_source_from_txn_info() {
        let info = TxnInfoPb {
            txn_id: Some(1),
            commit_time: None,
            combined_txn_log: Some(true),
            txn_type: None,
            force_publish: None,
            rebuild_pindex: None,
            gtid: None,
            load_ids: Vec::new(),
        };
        assert_eq!(
            decide_abort_txn_log_source(&info),
            AbortTxnLogSource::Combined
        );
    }
}
