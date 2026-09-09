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

//! One query context's installed vended credentials.
//!
//! This is the backend half of the credential domain. It holds no authority
//! over epochs: the query context owner has already classified a rotation
//! against [`CredentialDomain`], so by the time anything here runs the only
//! remaining question is which scoped material a storage request may use.
//!
//! Two things about the shape are deliberate.
//!
//! A rotation replaces the whole table in one move. There is no prepare, no
//! commit, and no staging slot, because the two-step existed to make a
//! query-wide switch atomic across backends — and this protocol does not want
//! that: each backend's credential domain advances on its own, so the only
//! atomicity that matters is "one backend never serves half a rotation", which
//! a single replace under one lock already gives.
//!
//! The backend still mints nothing. It has no catalog client and no refresher;
//! it installs what the frontend sends and refuses everything else. That
//! division is ADR-0129's, and moving the carrier from the lifecycle stream to
//! a query-context domain does not move the principal.
//!
//! [`CredentialDomain`]: novarocks_execution::task_execution::domain::CredentialDomain

use std::collections::BTreeMap;
use std::fmt;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use novarocks_execution::task_execution::status::TaskFailureCategory;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorStorageResolver, CredentialLeaseDescriptor,
    CredentialLeaseId, CredentialLeaseProvider, CredentialLeaseSecretEnvelope,
    ResolvedVendedS3Access, StorageAccessRequest, StorageCredentialScopePrefix,
};
use novarocks_task_codec::domain::{VendedCredentialLease, WireCredential};

use super::host::HostRejection;

/// One installed lease: an immutable scope and the exact secret it authorizes.
struct InstalledLease {
    descriptor: CredentialLeaseDescriptor,
    envelope: CredentialLeaseSecretEnvelope,
}

impl fmt::Debug for InstalledLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The descriptor is secret-free by construction and is the only half
        // worth showing; the envelope redacts itself, and naming it at all
        // would put a secret one careless field access away.
        formatter
            .debug_struct("InstalledLease")
            .field("lease_id", &self.descriptor.lease_id())
            .field("epoch", &self.descriptor.epoch())
            .field("material", &"[REDACTED]")
            .finish()
    }
}

/// The installed vended credentials of one query context.
#[derive(Default)]
pub struct QueryContextCredentialSlot {
    leases: RwLock<BTreeMap<CredentialLeaseId, InstalledLease>>,
}

impl fmt::Debug for QueryContextCredentialSlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let leases = self
            .leases
            .read()
            .unwrap_or_else(|error| error.into_inner());
        formatter
            .debug_struct("QueryContextCredentialSlot")
            .field("lease_count", &leases.len())
            .field("material", &"[REDACTED]")
            .finish()
    }
}

impl QueryContextCredentialSlot {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replaces every installed lease with this rotation, atomically.
    ///
    /// A rejection leaves the previous table exactly as it was: a storage
    /// request served while a bad rotation is refused keeps working on the
    /// epoch that was already installed, rather than finding an empty slot.
    pub fn install(&self, material: &WireCredential) -> Result<(), HostRejection> {
        let now = unix_ms();
        let mut installed = BTreeMap::new();
        for lease in material.leases() {
            let lease = validate(lease, now)?;
            if installed
                .insert(lease.descriptor.lease_id(), lease)
                .is_some()
            {
                // The wire decoder already rejects unsorted and duplicated
                // lease ids, so reaching this is a codec regression rather
                // than something a peer can send.
                return Err(rejected("credential rotation repeats a lease id"));
            }
        }
        *self
            .leases
            .write()
            .unwrap_or_else(|error| error.into_inner()) = installed;
        Ok(())
    }

    /// Drops every installed lease.
    ///
    /// Called for an establish rollback, an abort, and a normal release, so it
    /// is idempotent. It is the only way material leaves this slot.
    pub fn clear(&self) {
        self.leases
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
    }

    /// Resolves one process-local vended storage target.
    ///
    /// The rule is the installed one, unchanged: the same owning catalog, the
    /// S3 provider, an unexpired token, and the longest matching location
    /// prefix. Every absence or mismatch is a typed refusal — there is no
    /// fallback to a process-level credential, because a query that was
    /// planned against vended access has no other authority to fall back to.
    pub fn resolve_vended_s3(
        &self,
        request: &StorageAccessRequest,
    ) -> Result<ResolvedVendedS3Access, ConnectorError> {
        let now = unix_ms();
        let leases = self
            .leases
            .read()
            .unwrap_or_else(|error| error.into_inner());
        let mut selected: Option<(&InstalledLease, &StorageCredentialScopePrefix)> = None;
        for lease in leases.values() {
            if lease.descriptor.provider() != CredentialLeaseProvider::S3
                || lease.descriptor.owner() != request.owner()
                || lease.envelope.session_token_expires_at_unix_ms() <= now
            {
                continue;
            }
            for prefix in lease.descriptor.prefixes() {
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
            lease.descriptor.storage_access_domain_id(),
            lease.descriptor.lease_id(),
            lease.envelope.epoch(),
            matched_prefix.clone(),
            lease.envelope.session_token_expires_at_unix_ms(),
            lease.envelope.access_key_id().clone(),
            lease.envelope.secret_access_key().clone(),
            lease.envelope.session_token().clone(),
        ))
    }

    /// The installed epoch of one lease, for receipts and tests.
    ///
    /// Epochs are not secret; this is the only thing about the slot's contents
    /// that anything outside it may read.
    pub fn installed_epoch(&self, lease_id: CredentialLeaseId) -> Option<u64> {
        self.leases
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(&lease_id)
            .map(|lease| lease.envelope.epoch())
    }

    pub fn installed_lease_count(&self) -> usize {
        self.leases
            .read()
            .unwrap_or_else(|error| error.into_inner())
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

/// Refuses a lease that cannot be installed.
///
/// Expiry is checked here rather than only at use: installing material that is
/// already dead would make the slot report a lease it can never serve, and the
/// frontend's refresh scheduling would have no way to notice.
fn validate(lease: &VendedCredentialLease, now: u64) -> Result<InstalledLease, HostRejection> {
    let descriptor = lease.descriptor();
    let envelope = lease.envelope();
    if !envelope.matches_descriptor(descriptor) {
        // The decoder proves this too. Repeating it here keeps the slot
        // correct on its own terms rather than by trusting its caller.
        return Err(rejected(
            "credential envelope does not match the scope it claims",
        ));
    }
    if descriptor.not_after_unix_ms() <= now || envelope.session_token_expires_at_unix_ms() <= now {
        return Err(rejected("credential lease is already expired"));
    }
    Ok(InstalledLease {
        descriptor: descriptor.clone(),
        envelope: envelope.clone(),
    })
}

/// Every rejection carries the same secret-free shape.
///
/// The detail says what was wrong with the *scope*, never what the material
/// was, so a rejection cannot become an oracle over a secret.
fn rejected(detail: &str) -> HostRejection {
    HostRejection::new(TaskFailureCategory::Protocol, detail)
}

fn vended_storage_access_denied() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        "vended storage access is unavailable for this query attempt",
    )
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{QueryContextCredentialSlot, unix_ms};

    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_codec::lifecycle::{
        CredentialLeaseSecretEnvelope, encode_credential_lease_descriptor,
        encode_credential_lease_secret_envelope,
    };
    use novarocks_proto_models::novarocks as proto;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, CredentialLeaseDescriptor,
        CredentialLeaseId, CredentialLeaseProvider, StorageAccessDomainId, StorageAccessRequest,
        StorageCredentialScopePrefix,
    };
    use novarocks_task_codec::domain::WireCredential;

    const SECRET_SENTINEL: &str = "NOVAROCKS_SECRET_SENTINEL";

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

    fn descriptor(
        seed: u8,
        epoch: u64,
        prefixes: &[&str],
        not_after: u64,
    ) -> proto::CredentialLeaseDescriptor {
        encode_credential_lease_descriptor(
            &CredentialLeaseDescriptor::try_new(
                lease_id(seed),
                epoch,
                owner(),
                CredentialLeaseProvider::S3,
                prefixes.iter().copied().map(prefix).collect(),
                not_after,
                true,
                StorageAccessDomainId::from_bytes([8; 32]),
            )
            .expect("legal descriptor"),
        )
    }

    fn envelope(
        seed: u8,
        epoch: u64,
        secret: &str,
        not_after: u64,
    ) -> proto::CredentialLeaseSecretEnvelope {
        encode_credential_lease_secret_envelope(
            &CredentialLeaseSecretEnvelope::try_new_from_wire_scalars(
                lease_id(seed),
                epoch,
                "access-key".to_owned(),
                secret.to_owned(),
                "session-token".to_owned(),
                not_after,
            )
            .expect("legal envelope"),
        )
    }

    fn live() -> u64 {
        unix_ms() + 600_000
    }

    fn material(
        descriptors: Vec<proto::CredentialLeaseDescriptor>,
        envelopes: Vec<proto::CredentialLeaseSecretEnvelope>,
    ) -> WireCredential {
        WireCredential::decode(&descriptors, &envelopes, FieldPath::root("credential"))
            .expect("legal rotation")
    }

    fn request(location: &str) -> StorageAccessRequest {
        StorageAccessRequest::try_new(owner(), location).expect("legal storage request")
    }

    #[test]
    fn a_rotation_replaces_the_whole_table_in_one_move() {
        let slot = QueryContextCredentialSlot::new();
        let expiry = live();
        slot.install(&material(
            vec![
                descriptor(1, 1, &["s3://bucket/a"], expiry),
                descriptor(2, 1, &["s3://bucket/b"], expiry),
            ],
            vec![
                envelope(1, 1, SECRET_SENTINEL, expiry),
                envelope(2, 1, "second", expiry),
            ],
        ))
        .expect("initial install");
        assert_eq!(slot.installed_lease_count(), 2);

        // The next epoch drops lease two entirely. A staging slot would have
        // left it resolvable; a whole-table replace must not.
        slot.install(&material(
            vec![descriptor(1, 2, &["s3://bucket/a"], expiry)],
            vec![envelope(1, 2, "rotated", expiry)],
        ))
        .expect("rotation install");
        assert_eq!(slot.installed_lease_count(), 1);
        assert_eq!(slot.installed_epoch(lease_id(1)), Some(2));
        assert_eq!(slot.installed_epoch(lease_id(2)), None);
        assert!(slot.resolve_vended_s3(&request("s3://bucket/b/x")).is_err());
    }

    #[test]
    fn the_longest_matching_prefix_wins_and_a_foreign_location_is_refused() {
        let slot = QueryContextCredentialSlot::new();
        let expiry = live();
        slot.install(&material(
            vec![
                descriptor(1, 1, &["s3://bucket"], expiry),
                descriptor(2, 1, &["s3://bucket/deep/nested"], expiry),
            ],
            vec![
                envelope(1, 1, "broad", expiry),
                envelope(2, 1, "narrow", expiry),
            ],
        ))
        .expect("install");

        let resolved = slot
            .resolve_vended_s3(&request("s3://bucket/deep/nested/file.parquet"))
            .expect("nested location resolves");
        assert_eq!(resolved.lease_id(), lease_id(2));
        assert_eq!(
            resolved.matched_prefix(),
            &prefix("s3://bucket/deep/nested")
        );

        let resolved = slot
            .resolve_vended_s3(&request("s3://bucket/shallow/file.parquet"))
            .expect("shallow location resolves");
        assert_eq!(resolved.lease_id(), lease_id(1));

        assert!(
            slot.resolve_vended_s3(&request("s3://other/file.parquet"))
                .is_err(),
            "an unscoped location must not borrow a scoped lease"
        );
    }

    #[test]
    fn a_refused_rotation_leaves_the_installed_epoch_serving() {
        let slot = QueryContextCredentialSlot::new();
        let expiry = live();
        slot.install(&material(
            vec![descriptor(1, 1, &["s3://bucket/a"], expiry)],
            vec![envelope(1, 1, SECRET_SENTINEL, expiry)],
        ))
        .expect("install");

        let rejection = slot
            .install(&material(
                vec![descriptor(1, 2, &["s3://bucket/a"], 1)],
                vec![envelope(1, 2, "rotated", 1)],
            ))
            .expect_err("an expired rotation is refused");
        assert!(
            rejection.detail().as_str().contains("expired"),
            "{rejection}"
        );

        assert_eq!(
            slot.installed_epoch(lease_id(1)),
            Some(1),
            "a refused rotation must not empty the slot"
        );
        assert!(slot.resolve_vended_s3(&request("s3://bucket/a/x")).is_ok());
    }

    #[test]
    fn clearing_drops_every_lease_and_stays_idempotent() {
        let slot = QueryContextCredentialSlot::new();
        let expiry = live();
        slot.install(&material(
            vec![descriptor(1, 1, &["s3://bucket/a"], expiry)],
            vec![envelope(1, 1, SECRET_SENTINEL, expiry)],
        ))
        .expect("install");

        slot.clear();
        assert_eq!(slot.installed_lease_count(), 0);
        assert!(slot.resolve_vended_s3(&request("s3://bucket/a/x")).is_err());
        slot.clear();
        assert_eq!(slot.installed_lease_count(), 0);
    }

    #[test]
    fn credential_material_never_appears_in_any_rendering() {
        let slot = QueryContextCredentialSlot::new();
        let expiry = live();
        slot.install(&material(
            vec![descriptor(1, 1, &["s3://bucket/a"], expiry)],
            vec![envelope(1, 1, SECRET_SENTINEL, expiry)],
        ))
        .expect("install");

        let rendered = format!("{slot:?}");
        assert!(
            !rendered.contains(SECRET_SENTINEL),
            "credential material leaked into Debug: {rendered}"
        );
        assert!(rendered.contains("[REDACTED]"), "{rendered}");
        assert!(rendered.contains("lease_count"), "{rendered}");

        // A rejection is the other rendering path a secret could escape
        // through, because it is the one that describes what arrived.
        let refused = slot
            .install(&material(
                vec![descriptor(1, 2, &["s3://bucket/a"], 1)],
                vec![envelope(1, 2, SECRET_SENTINEL, 1)],
            ))
            .map(|()| String::new())
            .unwrap_or_else(|rejection| rejection.to_string());
        assert!(!refused.is_empty(), "the rotation must have been refused");
        assert!(!refused.contains(SECRET_SENTINEL), "{refused}");

        let denied = slot
            .resolve_vended_s3(&request("s3://other/file.parquet"))
            .map(|_| String::new())
            .unwrap_or_else(|error| error.to_string());
        assert!(
            !denied.contains(SECRET_SENTINEL),
            "credential material leaked into a storage refusal: {denied}"
        );
    }
}
