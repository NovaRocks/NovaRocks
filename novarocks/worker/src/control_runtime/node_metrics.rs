// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Read-only observations of this same Node's actual admission ledger.

use crate::control_admission::Dimension;
use novarocks_control_runtime::transaction::RuntimeLedger;

/// Each field is independently sampled under the original ledger lock. These
/// are actual charged units, including retained and physically exiting holders.
/// They are neither an instantaneous cross-dimension sum nor exit receipts.
/// Queue/running counts, Root census and limits are absent: no actual read port
/// for those observations is inferred from these charged-resource samples.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerNodeMetricsSnapshot {
    pub active_tasks: u64,
    pub preparing_tasks: u64,
    pub preparing_bytes: u64,
    pub requests: u64,
    pub tickets: u64,
    pub contexts: u64,
}
impl WorkerNodeMetricsSnapshot {
    pub(super) fn sample(ledger: &RuntimeLedger) -> Self {
        Self {
            active_tasks: ledger.used(Dimension::ActiveTasks.id()),
            preparing_tasks: ledger.used(Dimension::PreparingTasks.id()),
            preparing_bytes: ledger.used(Dimension::PreparingBytes.id()),
            requests: ledger.used(Dimension::Requests.id()),
            tickets: ledger.used(Dimension::Tickets.id()),
            contexts: ledger.used(Dimension::Contexts.id()),
        }
    }
}
