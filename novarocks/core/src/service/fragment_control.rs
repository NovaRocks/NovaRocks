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
use crate::runtime::result_buffer;

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
        target: "novarocks_execution",
        finst_id = %finst_id,
        query_id = ?query_id,
        canceled_fragments = target_finsts.len(),
        "cancel request received"
    );

    for id in &target_finsts {
        result_buffer::cancel(*id);
    }
}

#[cfg(test)]
mod tests {
    use super::cancel_runtime_fragment;
    use crate::common::types::UniqueId;
    use crate::runtime::query_context::{QueryId, query_context_manager};

    #[test]
    fn cancel_fans_out_to_query_fragment_peers() {
        let query_id = QueryId::new(7011, 7012);
        let finst_a = UniqueId::new(7013, 7014);
        let finst_b = UniqueId::new(7015, 7016);

        let mgr = query_context_manager();
        mgr.register_finst(finst_a, query_id);
        mgr.register_finst(finst_b, query_id);

        cancel_runtime_fragment(finst_a);

        mgr.unregister_finst(finst_a);
        mgr.unregister_finst(finst_b);
    }
}
