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

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::exec::node::scan::ScanOp;
use crate::runtime::exchange::ExchangeKey;
use crate::runtime::fragment::io::ExchangeReceiverPort;

/// Instance-materialized exchange input for a single exchange-source node.
#[derive(Clone)]
pub struct ExchangeBinding {
    pub key: ExchangeKey,
    pub expected_senders: usize,
    pub receiver_port: Arc<dyn ExchangeReceiverPort>,
}

impl std::fmt::Debug for ExchangeBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ExchangeBinding")
            .field("key", &self.key)
            .field("expected_senders", &self.expected_senders)
            .finish_non_exhaustive()
    }
}

/// Per-node exchange bindings keyed by plan node_id. Exec-level; built in
/// `runtime::fragment` from the validated `FragmentInstanceSpec`.
#[derive(Clone, Debug, Default)]
pub struct ExchangeBindings(BTreeMap<i32, ExchangeBinding>);

impl ExchangeBindings {
    pub fn insert(&mut self, node_id: i32, binding: ExchangeBinding) {
        self.0.insert(node_id, binding);
    }
    pub fn get(&self, node_id: i32) -> Option<ExchangeBinding> {
        self.0.get(&node_id).cloned()
    }
}

/// Per-node scan bindings: the instance-local bound `ScanOp` keyed by node_id.
#[derive(Clone, Default)]
pub struct ScanBindings(BTreeMap<i32, Arc<dyn ScanOp>>);

impl ScanBindings {
    pub fn insert(&mut self, node_id: i32, op: Arc<dyn ScanOp>) {
        self.0.insert(node_id, op);
    }
    pub fn get(&self, node_id: i32) -> Option<Arc<dyn ScanOp>> {
        self.0.get(&node_id).cloned()
    }

    /// All instance-materialized scan operations owned by this fragment.
    ///
    /// The pipeline execution retains these terminal hooks so an external
    /// fragment abort can close connector readers even when every driver is
    /// parked and no further driver turn is scheduled.
    pub fn terminal_ops(&self) -> Vec<Arc<dyn ScanOp>> {
        self.0.values().cloned().collect()
    }
}

// `dyn ScanOp` is not `Debug`, so print only the bound node ids. Enough for
// `Result::expect_err`-style diagnostics without touching the ops.
impl std::fmt::Debug for ScanBindings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanBindings")
            .field("node_ids", &self.0.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::exchange::ExchangeKey;
    use crate::runtime::fragment::io::UnavailableExchangeReceiverPort;

    #[test]
    fn exchange_bindings_lookup_by_node_id() {
        let key = ExchangeKey {
            finst_id_hi: 1,
            finst_id_lo: 2,
            node_id: 7,
        };
        let mut b = ExchangeBindings::default();
        b.insert(
            7,
            ExchangeBinding {
                key,
                expected_senders: 3,
                receiver_port: Arc::new(UnavailableExchangeReceiverPort),
            },
        );
        let got = b.get(7).expect("binding for node 7");
        assert_eq!(got.expected_senders, 3);
        assert_eq!(got.key.node_id, 7);
        assert!(b.get(9).is_none());
    }
}
