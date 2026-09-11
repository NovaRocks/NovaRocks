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

//! Process-local Connector access retained by one logical execution.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_query_application::preparation::NegotiatedScanReceipt;
use novarocks_spi::connector::{
    CatalogProperties, ConnectorControlPlanningLease, ConnectorReadAttemptAccess,
};
use novarocks_sql::plan_read::FragmentId;

/// The single owner of everything that may open the immutable scans across
/// replacement attempts of one logical execution. It contains no attempt
/// identity, request-scoped credentials, or open split source; those are
/// acquired separately for each attempt.
pub(crate) struct LogicalExecutionAccessScope {
    entries: BTreeMap<(FragmentId, i32), Arc<ConnectorAttemptAccessEntry>>,
}

#[cfg(test)]
impl LogicalExecutionAccessScope {
    pub(super) fn get(
        &self,
        fragment_id: FragmentId,
        node_id: i32,
    ) -> Option<&ConnectorAttemptAccessEntry> {
        self.entries.get(&(fragment_id, node_id)).map(Arc::as_ref)
    }
}

pub(crate) type ConnectorAttemptAccessPlan = LogicalExecutionAccessScope;

/// Move-only lineage proofs reserved for final execution-description sealing.
/// Execution-side access never borrows them.
pub(crate) struct FrozenDescriptionInputs {
    receipts: Vec<NegotiatedScanReceipt>,
}

pub(super) struct FrozenDescriptionInputsBuilder {
    receipts: Vec<NegotiatedScanReceipt>,
}

pub(crate) struct ConnectorAttemptAccessEntry {
    catalog_properties: CatalogProperties,
    planning_lease: ConnectorControlPlanningLease,
    access: ConnectorReadAttemptAccess,
}

/// Builder used only by scan finalization. Callers cannot assemble an access
/// plan from unrelated negotiation and provider facts.
pub(super) struct ConnectorAttemptAccessPlanBuilder {
    entries: BTreeMap<(FragmentId, i32), Arc<ConnectorAttemptAccessEntry>>,
}

impl ConnectorAttemptAccessPlanBuilder {
    pub(super) fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    pub(super) fn insert(
        &mut self,
        fragment_id: FragmentId,
        node_id: i32,
        catalog_properties: CatalogProperties,
        generation_guard: ConnectorControlPlanningLease,
        access: ConnectorReadAttemptAccess,
        receipt: &NegotiatedScanReceipt,
    ) -> Result<(), String> {
        if self.entries.contains_key(&(fragment_id, node_id)) {
            return Err(format!(
                "duplicate connector attempt access fragment_id={fragment_id} node_id={node_id}"
            ));
        }
        if access.frozen().binding() != receipt.final_handle().binding() {
            return Err(
                "connector attempt access does not match the finalized negotiation binding"
                    .to_string(),
            );
        }
        if catalog_properties.handle()
            != generation_guard
                .binding()
                .catalog_handle()
                .map_err(|error| error.to_string())?
        {
            return Err(
                "connector attempt access catalog does not match its generation guard".to_string(),
            );
        }
        if receipt.outcome().node_id() != node_id {
            return Err(format!(
                "connector negotiation receipt node {} does not match fragment_id={fragment_id} node_id={node_id}",
                receipt.outcome().node_id()
            ));
        }
        self.entries.insert(
            (fragment_id, node_id),
            Arc::new(ConnectorAttemptAccessEntry {
                catalog_properties,
                planning_lease: generation_guard,
                access,
            }),
        );
        Ok(())
    }

    pub(super) fn finish(self) -> ConnectorAttemptAccessPlan {
        LogicalExecutionAccessScope {
            entries: self.entries,
        }
    }
}

impl FrozenDescriptionInputsBuilder {
    pub(super) fn new() -> Self {
        Self {
            receipts: Vec::new(),
        }
    }

    pub(super) fn push(&mut self, receipt: NegotiatedScanReceipt) {
        self.receipts.push(receipt);
    }

    pub(super) fn finish(self) -> FrozenDescriptionInputs {
        FrozenDescriptionInputs {
            receipts: self.receipts,
        }
    }
}

impl FrozenDescriptionInputs {
    /// P4.2 consumes these proofs when it seals `FrozenExecutionDescription`.
    #[allow(dead_code, reason = "P4.2 consumes the sealed description inputs")]
    pub(crate) fn into_receipts(self) -> Vec<NegotiatedScanReceipt> {
        self.receipts
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.receipts.len()
    }
}

impl LogicalExecutionAccessScope {
    pub(crate) fn share(
        &self,
        fragment_id: FragmentId,
        node_id: i32,
    ) -> Option<Arc<ConnectorAttemptAccessEntry>> {
        self.entries.get(&(fragment_id, node_id)).cloned()
    }

    pub(crate) fn iter(
        &self,
    ) -> impl Iterator<Item = (FragmentId, i32, &ConnectorAttemptAccessEntry)> {
        self.entries
            .iter()
            .map(|(&(fragment_id, node_id), entry)| (fragment_id, node_id, entry.as_ref()))
    }

    pub(crate) fn exactly_covers(&self, receipts: &[NegotiatedScanReceipt]) -> bool {
        self.entries.len() == receipts.len()
            && receipts.iter().all(|receipt| {
                self.entries.iter().any(|((_, node_id), entry)| {
                    *node_id == receipt.outcome().node_id()
                        && entry.access.frozen().binding() == receipt.final_handle().binding()
                })
            })
    }
}

impl ConnectorAttemptAccessEntry {
    pub(crate) const fn access(&self) -> &ConnectorReadAttemptAccess {
        &self.access
    }

    pub(crate) const fn catalog_properties(&self) -> &CatalogProperties {
        &self.catalog_properties
    }

    pub(crate) const fn planning_lease(&self) -> &ConnectorControlPlanningLease {
        &self.planning_lease
    }
}
