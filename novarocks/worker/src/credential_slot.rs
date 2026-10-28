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

//! One Worker's installed vended credentials for one query context.
//!
//! Native decoding remains outside this crate.  This owner receives already
//! validated SPI scope announcements, atomically replaces its local table, and
//! is the only place that resolves an attempt-local storage request.
//!
//! It holds no material. An execution node acquires its own under its own
//! catalog identity (CAD-1 D1), so what is installed here is which scopes this
//! attempt touches and how each one is acquired -- not a secret to hand out.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::RwLock;

use novarocks_execution_contract::task_execution::status::TaskFailureCategory;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorStorageResolver, CredentialLeaseDescriptor,
    CredentialLeaseId, CredentialLeaseProvider, ResolvedVendedS3Access, StorageAccessRequest,
    StorageCredentialScopePrefix,
};

use crate::HostRejection;

/// The announced scopes of one Worker query context.
#[derive(Default)]
pub struct QueryContextCredentialSlot {
    state: RwLock<CredentialSlotState>,
}

#[derive(Default)]
struct CredentialSlotState {
    closed: bool,
    leases: BTreeMap<CredentialLeaseId, CredentialLeaseDescriptor>,
}

impl fmt::Debug for QueryContextCredentialSlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.read().unwrap_or_else(|error| error.into_inner());
        formatter
            .debug_struct("QueryContextCredentialSlot")
            .field("closed", &state.closed)
            .field("lease_count", &state.leases.len())
            .finish()
    }
}

impl QueryContextCredentialSlot {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces every announced scope with one fully decoded rotation.
    ///
    /// A refusal leaves the prior table intact, so an invalid rotation cannot
    /// turn a still-live context into an empty credential authority. Closing
    /// and this final replacement share one lock; a closed instance cannot
    /// accept a rotation, even when validation began before closure.
    pub fn install(&self, announced: &[CredentialLeaseDescriptor]) -> Result<(), HostRejection> {
        let mut installed = BTreeMap::new();
        for lease in announced {
            if installed.insert(lease.lease_id(), lease.clone()).is_some() {
                return Err(HostRejection::from_domain(
                    TaskFailureCategory::Protocol,
                    RepeatedCredentialLease {
                        lease: lease.lease_id(),
                    },
                ));
            }
        }
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|error| error.into_inner());
        if state.closed {
            drop(state);
            return Err(HostRejection::from_domain(
                TaskFailureCategory::Protocol,
                ClosedCredentialSlot,
            ));
        }
        let previous = std::mem::replace(&mut state.leases, installed);
        drop(state);
        drop(previous);
        Ok(())
    }

    /// Drops every announced scope without changing this instance's admission
    /// state. An open slot may install again; a closed slot stays closed.
    pub fn clear(&self) {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|error| error.into_inner());
        let previous = std::mem::take(&mut state.leases);
        drop(state);
        drop(previous);
    }

    /// Permanently closes this instance and withdraws all announcements.
    /// Repeated closure is idempotent; neither clear nor install reopens it.
    pub fn close(&self) {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|error| error.into_inner());
        state.closed = true;
        let previous = std::mem::take(&mut state.leases);
        drop(state);
        drop(previous);
    }

    /// Resolves one process-local vended S3 target by exact catalog owner,
    /// unexpired lease, and longest matching scope prefix.
    /// Selects the announced scope that authorizes one location.
    ///
    /// No expiry filter: an announcement names a scope and an acquisition path,
    /// neither of which expires. The coordinator's own material may well have
    /// expired by the time this node reads, and CAD-1 acceptance 11 is exactly
    /// that case -- material expiry is not acquisition-capability expiry.
    pub fn resolve_vended_s3(
        &self,
        request: &StorageAccessRequest,
    ) -> Result<ResolvedVendedS3Access, ConnectorError> {
        let state = self.state.read().unwrap_or_else(|error| error.into_inner());
        if state.closed {
            return Err(vended_storage_access_denied());
        }
        let mut selected: Option<(&CredentialLeaseDescriptor, &StorageCredentialScopePrefix)> =
            None;
        for lease in state.leases.values() {
            if lease.provider() != CredentialLeaseProvider::S3 || lease.owner() != request.owner() {
                continue;
            }
            for prefix in lease.prefixes() {
                if !request.location().starts_with(prefix.as_str()) {
                    continue;
                }
                if selected
                    .is_none_or(|(_, current)| prefix.as_str().len() > current.as_str().len())
                {
                    selected = Some((lease, prefix));
                }
            }
        }
        let (lease, matched_prefix) = selected.ok_or_else(vended_storage_access_denied)?;
        Ok(ResolvedVendedS3Access::new(
            lease.storage_access_domain_id(),
            lease.lease_id(),
            lease.epoch(),
            matched_prefix.clone(),
            lease.renewal_path().cloned(),
            None,
            // An execution node is never seeded: it acquires for itself.
            None,
        ))
    }

    pub fn installed_epoch(&self, lease_id: CredentialLeaseId) -> Option<u64> {
        self.state
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .leases
            .get(&lease_id)
            .map(CredentialLeaseDescriptor::epoch)
    }

    pub fn installed_lease_count(&self) -> usize {
        self.state
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .leases
            .len()
    }
}

impl ConnectorStorageResolver for QueryContextCredentialSlot {
    fn resolve_vended_s3(
        &self,
        request: &StorageAccessRequest,
    ) -> Result<ResolvedVendedS3Access, ConnectorError> {
        Self::resolve_vended_s3(self, request)
    }
}

#[derive(Debug)]
struct ClosedCredentialSlot;
impl fmt::Display for ClosedCredentialSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("credential announcements cannot be installed after context closure")
    }
}
impl std::error::Error for ClosedCredentialSlot {}
impl novarocks_execution_contract::failure::FailureProjection for ClosedCredentialSlot {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::{FailureClass, FailureFact};
        FailureFact::new(
            FailureClass::Invariant,
            crate::failure_reason::WorkerFailureReason::DomainAfterSeal.reason(),
        )
    }
}

#[derive(Debug)]
struct RepeatedCredentialLease {
    lease: CredentialLeaseId,
}
impl fmt::Display for RepeatedCredentialLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "credential rotation repeats lease {:?}", self.lease)
    }
}
impl std::error::Error for RepeatedCredentialLease {}
impl novarocks_execution_contract::failure::FailureProjection for RepeatedCredentialLease {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::{FailureClass, FailureFact};
        FailureFact::new(
            FailureClass::Invariant,
            crate::failure_reason::WorkerFailureReason::CredentialLeaseRepeated.reason(),
        )
    }
}

fn vended_storage_access_denied() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        "vended storage access is unavailable for this query attempt",
    )
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::QueryContextCredentialSlot;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, CredentialLeaseDescriptor,
        CredentialLeaseId, CredentialLeaseProvider, CredentialRenewalPath,
        CredentialRoutingContext, StorageAccessDomainId, StorageAccessRequest,
        StorageCredentialScopePrefix,
    };

    fn owner() -> CatalogHandle {
        CatalogHandle::new(
            ConnectorInstanceId::parse("warehouse").expect("legal instance id"),
            CatalogVersion::from_bytes([7; 32]),
        )
    }

    fn lease_id(seed: u8) -> CredentialLeaseId {
        CredentialLeaseId::try_from_bytes([seed; 16]).expect("legal lease id")
    }

    fn prefix(value: &str) -> StorageCredentialScopePrefix {
        StorageCredentialScopePrefix::try_from_normalized(value).expect("legal prefix")
    }

    fn lease(seed: u8, epoch: u64, prefixes: &[&str]) -> CredentialLeaseDescriptor {
        announcing_lease(seed, epoch, prefixes, None)
    }

    fn routing(url: &str) -> CredentialRoutingContext {
        CredentialRoutingContext::try_new(url, [(Arc::from("x-project-id"), Arc::from("p1"))])
            .expect("legal routing context")
    }

    fn announcing_lease(
        seed: u8,
        epoch: u64,
        prefixes: &[&str],
        endpoint: Option<&str>,
    ) -> CredentialLeaseDescriptor {
        CredentialLeaseDescriptor::try_new(
            lease_id(seed),
            epoch,
            owner(),
            CredentialLeaseProvider::S3,
            prefixes.iter().copied().map(prefix).collect(),
            // Meaningful only to the coordinator's own consumption; an
            // execution node acquires its own material and never reads it.
            1,
            true,
            endpoint.map(|endpoint| CredentialRenewalPath::CredentialsEndpoint(routing(endpoint))),
            StorageAccessDomainId::from_bytes([8; 32]),
        )
        .expect("legal descriptor")
    }

    fn request(location: &str) -> StorageAccessRequest {
        StorageAccessRequest::try_new(owner(), location).expect("legal storage request")
    }

    fn assert_closed(rejection: crate::HostRejection) {
        use novarocks_execution_contract::{
            failure::{FailureClass, FailureProjection},
            task_execution::status::TaskFailureCategory,
        };
        assert_eq!(rejection.category(), TaskFailureCategory::Protocol);
        assert_eq!(rejection.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(
            rejection.failure_fact().reason(),
            crate::failure_reason::WorkerFailureReason::DomainAfterSeal.reason()
        );
        assert!(!rejection.is_closed_queue());
    }

    #[test]
    fn close_withdraws_every_announcement_and_refuses_future_rotations_for_this_instance() {
        let slot = QueryContextCredentialSlot::new();
        slot.install(&[
            lease(1, 1, &["s3://bucket/a"]),
            lease(2, 1, &["s3://bucket/b"]),
        ])
        .unwrap();
        slot.close();
        slot.close();
        assert_eq!(slot.installed_lease_count(), 0);
        assert_eq!(slot.installed_epoch(lease_id(1)), None);
        assert_eq!(slot.installed_epoch(lease_id(2)), None);
        assert!(slot.resolve_vended_s3(&request("s3://bucket/a/x")).is_err());
        assert!(slot.resolve_vended_s3(&request("s3://bucket/b/x")).is_err());
        assert_closed(
            slot.install(&[lease(1, 2, &["s3://bucket/a"])])
                .expect_err("closed slot refuses a valid later rotation"),
        );
        assert_closed(slot.install(&[]).expect_err("empty install cannot reopen"));

        let other = QueryContextCredentialSlot::new();
        other.install(&[lease(1, 1, &["s3://bucket/a"])]).unwrap();
        assert!(other.resolve_vended_s3(&request("s3://bucket/a/x")).is_ok());
    }

    #[test]
    fn clear_allows_replacement_while_open_and_does_not_reopen_after_close() {
        let slot = QueryContextCredentialSlot::new();
        slot.install(&[lease(1, 1, &["s3://bucket/a"])]).unwrap();
        slot.clear();
        slot.clear();
        assert_eq!(slot.installed_lease_count(), 0);
        assert!(slot.resolve_vended_s3(&request("s3://bucket/a/x")).is_err());
        slot.install(&[lease(2, 2, &["s3://bucket/b"])]).unwrap();
        assert_eq!(slot.installed_epoch(lease_id(2)), Some(2));
        assert!(slot.resolve_vended_s3(&request("s3://bucket/b/x")).is_ok());
        slot.close();
        slot.clear();
        assert_closed(
            slot.install(&[lease(3, 3, &["s3://bucket/c"])])
                .expect_err("clear must preserve permanent closure"),
        );
        assert_eq!(slot.installed_lease_count(), 0);
        assert!(slot.resolve_vended_s3(&request("s3://bucket/c/x")).is_err());
    }

    #[test]
    fn a_real_install_close_race_always_ends_closed_without_an_announcement() {
        for _ in 0..16 {
            let slot = Arc::new(QueryContextCredentialSlot::new());
            slot.install(&[lease(1, 1, &["s3://bucket/a"])]).unwrap();
            let start = Arc::new(Barrier::new(3));
            let installer = {
                let slot = slot.clone();
                let start = start.clone();
                thread::spawn(move || {
                    let rotation = [lease(2, 2, &["s3://bucket/b"])];
                    start.wait();
                    slot.install(&rotation)
                })
            };
            let closer = {
                let slot = slot.clone();
                let start = start.clone();
                thread::spawn(move || {
                    start.wait();
                    slot.close();
                })
            };
            start.wait();
            let installed = installer.join().unwrap();
            closer.join().unwrap();
            // Either installation committed first, or closure refused it.
            // Both actual threads have returned before checking the final state.
            if let Err(rejection) = installed {
                assert_closed(rejection);
            }
            assert_eq!(slot.installed_lease_count(), 0);
            assert_eq!(slot.installed_epoch(lease_id(2)), None);
            assert!(slot.resolve_vended_s3(&request("s3://bucket/a/x")).is_err());
            assert!(slot.resolve_vended_s3(&request("s3://bucket/b/x")).is_err());
            assert_closed(
                slot.install(&[lease(3, 3, &["s3://bucket/c"])])
                    .expect_err("a joined close is a permanent install barrier"),
            );
        }
    }

    #[test]
    fn a_selection_carries_the_acquisition_path_of_the_lease_it_selected() {
        // The path reaches the consumer through the selection, not through a
        // second lookup: a node that had to re-derive it could disagree with
        // the lease it is actually holding (CAD-1 D1).
        let slot = QueryContextCredentialSlot::new();
        slot.install(&[
            announcing_lease(
                1,
                1,
                &["s3://bucket/a"],
                Some("https://rest/v1/a/credentials"),
            ),
            lease(2, 1, &["s3://bucket/b"]),
        ])
        .expect("install");

        let announcing = slot
            .resolve_vended_s3(&request("s3://bucket/a/x"))
            .expect("selection");
        // The whole routing context travels, headers included: the consumer
        // must reach the catalog the coordinator observed (IRU-13a L15).
        assert_eq!(
            announcing.renewal_path(),
            Some(&CredentialRenewalPath::CredentialsEndpoint(routing(
                "https://rest/v1/a/credentials"
            )))
        );

        // Absence is the seeded-without-renewal shape, and it must stay
        // distinguishable from the announcing lease installed beside it.
        let silent = slot
            .resolve_vended_s3(&request("s3://bucket/b/x"))
            .expect("selection");
        assert_eq!(silent.renewal_path(), None);
    }

    #[test]
    fn an_execution_node_selection_carries_no_material() {
        // CAD-1 D1 made material stop travelling. A selection that still handed
        // one over would mean something on this node was seeded after all.
        let slot = QueryContextCredentialSlot::new();
        slot.install(&[lease(1, 1, &["s3://bucket/a"])])
            .expect("install");

        let resolved = slot
            .resolve_vended_s3(&request("s3://bucket/a/x"))
            .expect("selection");
        assert!(resolved.seed().is_none());
        // "access" alone would match the access-domain id, which is a
        // non-secret identity this selection legitimately carries.
        let rendered = format!("{resolved:?}");
        for token in ["access_key", "secret_access", "session_token"] {
            assert!(
                !rendered.contains(token),
                "a credential-shaped field appeared in a selection: {rendered}"
            );
        }
    }

    #[test]
    fn an_announcement_outlives_the_coordinator_material_that_produced_it() {
        // CAD-1 acceptance 11: the coordinator's own material is long expired
        // by the time this node reads, and that says nothing about whether this
        // node may still acquire. A selection must still be made.
        let slot = QueryContextCredentialSlot::new();
        slot.install(&[announcing_lease(
            1,
            1,
            &["s3://bucket/a"],
            Some("https://rest/v1/a/credentials"),
        )])
        .expect("install");

        assert!(slot.resolve_vended_s3(&request("s3://bucket/a/x")).is_ok());
    }

    #[test]
    fn a_rotation_replaces_the_whole_table_in_one_move() {
        let slot = QueryContextCredentialSlot::new();
        slot.install(&[
            lease(1, 1, &["s3://bucket/a"]),
            lease(2, 1, &["s3://bucket/b"]),
        ])
        .expect("initial install");
        slot.install(&[lease(1, 2, &["s3://bucket/a"])])
            .expect("rotation install");
        assert_eq!(slot.installed_lease_count(), 1);
        assert_eq!(slot.installed_epoch(lease_id(1)), Some(2));
        assert_eq!(slot.installed_epoch(lease_id(2)), None);
        assert!(slot.resolve_vended_s3(&request("s3://bucket/b/x")).is_err());
    }

    #[test]
    fn longest_matching_prefix_wins_and_foreign_location_is_refused() {
        let slot = QueryContextCredentialSlot::new();
        slot.install(&[
            lease(1, 1, &["s3://bucket"]),
            lease(2, 1, &["s3://bucket/deep/nested"]),
        ])
        .expect("install");
        let resolved = slot
            .resolve_vended_s3(&request("s3://bucket/deep/nested/file.parquet"))
            .expect("nested location resolves");
        assert_eq!(resolved.lease_id(), lease_id(2));
        assert_eq!(
            resolved.matched_prefix(),
            &prefix("s3://bucket/deep/nested")
        );
        assert!(
            slot.resolve_vended_s3(&request("s3://other/file.parquet"))
                .is_err()
        );
    }

    #[test]
    fn refused_rotation_leaves_the_installed_table_serving() {
        let slot = QueryContextCredentialSlot::new();
        slot.install(&[lease(1, 1, &["s3://bucket/a"])])
            .expect("install");
        let rejection = slot
            .install(&[
                lease(1, 2, &["s3://bucket/a"]),
                lease(1, 3, &["s3://bucket/a"]),
            ])
            .expect_err("a rotation repeating a lease id is refused");
        use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
        use std::error::Error;
        assert_eq!(rejection.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(
            rejection.failure_fact().reason(),
            crate::failure_reason::WorkerFailureReason::CredentialLeaseRepeated.reason()
        );
        let failure = rejection.into_failure();
        let repeated = failure
            .source()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<super::RepeatedCredentialLease>()
            .unwrap();
        assert_eq!(repeated.lease, lease_id(1));
        assert_eq!(slot.installed_epoch(lease_id(1)), Some(1));
        assert!(slot.resolve_vended_s3(&request("s3://bucket/a/x")).is_ok());
    }
}
