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

use crate::common::types::UniqueId;
use crate::novarocks_logging::info;
use crate::runtime::query_context::{QueryContextManager, query_context_manager};
use crate::runtime::{exchange, result_buffer};

pub fn cancel_runtime_fragment(finst_id: UniqueId) {
    cancel_with_manager(finst_id, query_context_manager());
}

pub(crate) fn cancel_with_manager(finst_id: UniqueId, mgr: std::sync::Arc<QueryContextManager>) {
    let cancel_reason = format!("query canceled by FE: finst={}", finst_id);
    let cancel_result = mgr.cancel_finst(finst_id, cancel_reason);
    let query_id = cancel_result.query_id;
    let mut target_finsts = cancel_result.finsts;
    if target_finsts.is_empty() {
        target_finsts.push(finst_id);
    }

    info!(
        target: "novarocks::exec",
        finst_id = %finst_id,
        query_id = ?query_id,
        canceled_fragments = target_finsts.len(),
        "cancel request received"
    );

    for id in &target_finsts {
        result_buffer::cancel(*id);
    }

    let cleanup: Vec<_> = target_finsts
        .iter()
        .map(|id| {
            let id = *id;
            std::thread::spawn(move || exchange::cancel_fragment(id.hi, id.lo))
        })
        .collect();
    for h in cleanup {
        let _ = h.join();
    }
}

#[cfg(test)]
mod tests {
    use super::cancel_runtime_fragment;
    use crate::common::types::UniqueId;
    use crate::runtime::{
        exchange::{ExchangeKey, set_expected_senders, snapshot_receiver_state},
        query_context::{QueryId, query_context_manager},
    };

    #[test]
    fn cancel_fans_out_to_query_fragment_peers() {
        let query_id = QueryId { hi: 7011, lo: 7012 };
        let finst_a = UniqueId { hi: 7013, lo: 7014 };
        let finst_b = UniqueId { hi: 7015, lo: 7016 };
        let key_a = ExchangeKey {
            finst_id_hi: finst_a.hi,
            finst_id_lo: finst_a.lo,
            node_id: 51,
        };
        let key_b = ExchangeKey {
            finst_id_hi: finst_b.hi,
            finst_id_lo: finst_b.lo,
            node_id: 52,
        };

        let mgr = query_context_manager();
        mgr.register_finst(finst_a, query_id);
        mgr.register_finst(finst_b, query_id);
        set_expected_senders(key_a, 1);
        set_expected_senders(key_b, 1);

        assert!(snapshot_receiver_state(key_a).is_some());
        assert!(snapshot_receiver_state(key_b).is_some());

        cancel_runtime_fragment(finst_a);

        assert!(
            snapshot_receiver_state(key_a).is_none(),
            "target finst receiver must be canceled"
        );
        assert!(
            snapshot_receiver_state(key_b).is_none(),
            "peer finst receiver must be canceled"
        );

        mgr.unregister_finst(finst_a);
        mgr.unregister_finst(finst_b);
    }
}
