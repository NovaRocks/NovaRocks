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

//! The closed three-way classification every state family must declare.
//!
//! The classification is not a label attached to a family after the fact; it
//! decides whether the family may own a StateStore record at all.  That is why
//! the persistent key prefix lives inside the two persistent variants' data
//! instead of alongside the classification tag.

use bytes::Bytes;
use novarocks_state_store_api::Key;

/// A StateStore key prefix owned by exactly one persistent state family.
///
/// The literal is private and [`PersistentKeyPrefix::new`] is visible only
/// inside the `state_family` module tree, so a prefix can be *read* anywhere in
/// the crate but *minted* only by the manifest.  That is what makes "an owner
/// module declares its own `const PREFIX`" unrepresentable rather than merely
/// discouraged: an owner has nothing to build a `PersistentKeyPrefix` from, so
/// a second definition point for a prefix cannot exist.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PersistentKeyPrefix(&'static str);

impl PersistentKeyPrefix {
    pub(super) const fn new(prefix: &'static str) -> Self {
        Self(prefix)
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }

    pub const fn as_bytes(self) -> &'static [u8] {
        self.0.as_bytes()
    }

    /// The prefix itself, as a StateStore key.
    ///
    /// Range scans over a whole family start here, and families whose entire
    /// state is one record (backend desired state) use this as their only key.
    pub fn key(self) -> Result<Key, String> {
        self.build(Bytes::from_static(self.0.as_bytes()))
    }

    /// The prefix followed by `suffix`, concatenated verbatim.
    ///
    /// The manifest deliberately inserts no separator.  Registered prefixes are
    /// not uniform — some end in `/` and some do not, because they were frozen
    /// by the stores that already hold them — so a separator injected here
    /// would silently rewrite live keys.  Each owner therefore keeps its own
    /// suffix scheme and hands over the exact bytes that follow the prefix.
    pub fn key_with_suffix(self, suffix: &str) -> Result<Key, String> {
        let mut bytes = Vec::with_capacity(self.0.len() + suffix.len());
        bytes.extend_from_slice(self.0.as_bytes());
        bytes.extend_from_slice(suffix.as_bytes());
        self.build(Bytes::from(bytes))
    }

    fn build(self, bytes: Bytes) -> Result<Key, String> {
        Key::try_from(bytes)
            .map_err(|error| format!("build state family key under prefix {}: {error}", self.0))
    }
}

/// Exactly one classification per family, chosen from a closed set of three.
///
/// Consumers match this exhaustively, so a fourth classification cannot be
/// introduced without every consumer being forced to decide what it means.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateFamilyClassification {
    /// Desired state that originates outside this frontend process — from
    /// pre-deployment configuration, an external controller, or SQL acting as
    /// the authority.  A restart must find it again; it is never rebuilt from
    /// derived facts, which is why an `ExternalProjection` declares no rebuild
    /// policy.
    ExternalProjection(ExternalProjectionContract),
    /// State that belongs only to the current statement, the current attempt,
    /// or the current frontend incarnation.  It dies with its authority and
    /// must never outlive the process.
    ProcessRuntime(ProcessRuntimeContract),
    /// A derived, discardable copy of facts some external authority owns.
    ///
    /// Uniform contract for every `Accelerator`, which is why it is documented
    /// here instead of being repeated as a field on each entry: an unknown,
    /// corrupt, or unsupported record is discarded and rebuilt from the
    /// declared authority, and a rebuild source that is unavailable fails
    /// closed for the requests that depend on it.  Cache incompatibility is
    /// never reported as a business-truth error.
    Accelerator(AcceleratorContract),
}

impl StateFamilyClassification {
    /// The family's persistent key prefix, or `None` when the family owns no
    /// StateStore records.
    ///
    /// The `ProcessRuntime` arm cannot even name a prefix: the contract holds
    /// none and exposes no accessor for one, so this function is the compiler's
    /// own proof that runtime state is not durable rather than a check run
    /// after the fact.
    pub const fn persistent_prefix(self) -> Option<PersistentKeyPrefix> {
        match self {
            Self::ExternalProjection(contract) => Some(contract.persistent_prefix()),
            Self::ProcessRuntime(_) => None,
            Self::Accelerator(contract) => contract.persistent_prefix(),
        }
    }

    /// Whether the classification may own a StateStore record at all.
    ///
    /// This is derived, never declared: an in-process `Accelerator` is
    /// forbidden durability for the same structural reason a `ProcessRuntime`
    /// family is — it has no prefix to write under.
    pub const fn durability_admission(self) -> DurabilityAdmission {
        match self.persistent_prefix() {
            Some(_) => DurabilityAdmission::Permitted,
            None => DurabilityAdmission::Forbidden,
        }
    }

    /// Whether records of this family survive a frontend restart.
    pub const fn retain_on_restart(self) -> bool {
        match self {
            // An `ExternalProjection` *is* the desired state a restart has to
            // find again.  Retention is structural, not a policy choice.
            Self::ExternalProjection(_) => true,
            // A `ProcessRuntime` family's authority ends with the process, so
            // nothing it wrote can still be valid afterwards.
            Self::ProcessRuntime(_) => false,
            Self::Accelerator(contract) => contract.retain_on_restart(),
        }
    }

    /// What happens to this family when a deployment is cloned.
    pub const fn clone_policy(self) -> ClonePolicy {
        match self {
            Self::ExternalProjection(contract) => contract.clone_policy(),
            // Nothing of a `ProcessRuntime` family survives the process, so a
            // clone has nothing to copy in the first place.
            Self::ProcessRuntime(_) => ClonePolicy::NotCloned,
            Self::Accelerator(contract) => contract.clone_policy(),
        }
    }

    /// The single record version the current binary reads and writes, or `None`
    /// when the family encodes no record.
    ///
    /// There is deliberately no list: the frontend has no legacy readers, so a
    /// record that does not carry this version is not an older format to
    /// migrate but a record this binary refuses to interpret.
    pub const fn record_version(self) -> Option<u8> {
        match self {
            Self::ExternalProjection(contract) => Some(contract.record_version()),
            Self::ProcessRuntime(_) => None,
            Self::Accelerator(contract) => contract.record_version(),
        }
    }
}

/// Whether a family may own a StateStore record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurabilityAdmission {
    Permitted,
    Forbidden,
}

/// What an `ExternalProjection` family declares.
///
/// The prefix is mandatory rather than optional because both registered
/// external projections are durable today.  The StateStore is not the general
/// authority for desired state — for catalog attachments it is one selected
/// source mode among several — but where a projection *is* carried in the
/// store, the manifest owns the prefix it is carried under.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExternalProjectionContract {
    source: ExternalProjectionSource,
    snapshot_identity: SnapshotIdentity,
    bootstrap_failure_scope: BootstrapFailureScope,
    prefix: PersistentKeyPrefix,
    record_version: u8,
    clone_policy: ClonePolicy,
}

impl ExternalProjectionContract {
    pub(super) const fn new(
        source: ExternalProjectionSource,
        snapshot_identity: SnapshotIdentity,
        bootstrap_failure_scope: BootstrapFailureScope,
        prefix: PersistentKeyPrefix,
        record_version: u8,
        clone_policy: ClonePolicy,
    ) -> Self {
        Self {
            source,
            snapshot_identity,
            bootstrap_failure_scope,
            prefix,
            record_version,
            clone_policy,
        }
    }

    pub const fn source(self) -> ExternalProjectionSource {
        self.source
    }

    pub const fn snapshot_identity(self) -> SnapshotIdentity {
        self.snapshot_identity
    }

    pub const fn bootstrap_failure_scope(self) -> BootstrapFailureScope {
        self.bootstrap_failure_scope
    }

    pub const fn persistent_prefix(self) -> PersistentKeyPrefix {
        self.prefix
    }

    pub const fn record_version(self) -> u8 {
        self.record_version
    }

    pub const fn clone_policy(self) -> ClonePolicy {
        self.clone_policy
    }
}

/// Where an `ExternalProjection` family's desired state comes from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalProjectionSource {
    /// The deployment's selected catalog desired-state source mode.  Only the
    /// dynamic StateStore mode is implemented, which is the sole reason this
    /// family has a StateStore prefix; a future file or controller mode moves
    /// the authority without changing the classification.
    SelectedCatalogSourceMode,
}

/// How a reader decides it is looking at one coherent snapshot of the family
/// rather than a partially observed one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotIdentity {
    /// The snapshot is a complete enumeration of the family's prefix.  Identity
    /// is "every record the enumeration returned", so a partial enumeration is
    /// not a smaller snapshot — it is a failure.
    CompleteEnumeration,
}

/// The blast radius of a bootstrap failure for an `ExternalProjection`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapFailureScope {
    /// Two distinct scopes on purpose.  Failing to enumerate the snapshot, or
    /// to trust its integrity, fails the whole frontend bootstrap: an
    /// incomplete enumeration is indistinguishable from a smaller desired
    /// state, and silently accepting it would delete entries nobody asked to
    /// remove.  Failing to materialize one *located* entry only marks that
    /// entry unavailable, so one broken catalog cannot take the frontend down.
    GlobalEnumerationPerEntryMaterialization,
}

/// What a `ProcessRuntime` family declares — and, more importantly, what it
/// structurally cannot.
///
/// This struct has exactly one field.  There is deliberately no prefix field,
/// no `Option<PersistentKeyPrefix>`, and no prefix accessor, so "give a
/// `ProcessRuntime` family a persistent prefix" is a type error rather than a
/// rule someone has to remember to check.  Modelling the classification as a
/// flat tag plus an optional prefix table would have made the illegal state
/// representable and pushed the invariant back into review discipline; keeping
/// the prefix inside the two persistent variants' data makes the compiler the
/// enforcement point.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessRuntimeContract {
    authority: ProcessRuntimeAuthority,
}

impl ProcessRuntimeContract {
    pub(super) const fn new(authority: ProcessRuntimeAuthority) -> Self {
        Self { authority }
    }

    pub const fn authority(self) -> ProcessRuntimeAuthority {
        self.authority
    }
}

/// The authority a `ProcessRuntime` family's records belong to.
///
/// A family declares the *widest* scope any of its records belongs to, i.e. the
/// lifetime bound of the family as a whole.  A family that mixes per-attempt
/// records with incarnation-scoped scheduler state therefore declares
/// [`ProcessRuntimeAuthority::FrontendIncarnation`], because that is the scope
/// whose end invalidates all of it — declaring the narrower scope would
/// understate what has to disappear.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessRuntimeAuthority {
    /// The statement currently executing.
    Statement,
    /// The attempt currently executing (refresh, maintenance, or analyze).
    Attempt,
    /// The current frontend incarnation.
    FrontendIncarnation,
}

/// What an `Accelerator` family declares.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AcceleratorContract {
    residence: AcceleratorResidence,
    rebuild_authority: AcceleratorRebuildAuthority,
    rebuild_determinism: RebuildDeterminism,
    retain_on_restart: bool,
    clone_policy: ClonePolicy,
}

impl AcceleratorContract {
    pub(super) const fn new(
        residence: AcceleratorResidence,
        rebuild_authority: AcceleratorRebuildAuthority,
        rebuild_determinism: RebuildDeterminism,
        retain_on_restart: bool,
        clone_policy: ClonePolicy,
    ) -> Self {
        Self {
            residence,
            rebuild_authority,
            rebuild_determinism,
            retain_on_restart,
            clone_policy,
        }
    }

    pub const fn residence(self) -> AcceleratorResidence {
        self.residence
    }

    pub const fn persistent_prefix(self) -> Option<PersistentKeyPrefix> {
        match self.residence {
            AcceleratorResidence::Durable { prefix, .. } => Some(prefix),
            AcceleratorResidence::InProcess => None,
        }
    }

    pub const fn record_version(self) -> Option<u8> {
        match self.residence {
            AcceleratorResidence::Durable { record_version, .. } => Some(record_version),
            AcceleratorResidence::InProcess => None,
        }
    }

    /// The external authority this family is rebuilt from after a wipe.
    pub const fn rebuild_authority(self) -> AcceleratorRebuildAuthority {
        self.rebuild_authority
    }

    /// What a rebuild from that authority actually reproduces.
    pub const fn rebuild_determinism(self) -> RebuildDeterminism {
        self.rebuild_determinism
    }

    pub const fn retain_on_restart(self) -> bool {
        self.retain_on_restart
    }

    pub const fn clone_policy(self) -> ClonePolicy {
        self.clone_policy
    }

    /// The destructive entry point that removes the whole family.
    ///
    /// Derived from the residence rather than declared: a durable family is
    /// wiped by deleting its own prefix and an in-process family by dropping
    /// its cache, so a separate field could only ever agree with the residence
    /// or contradict it.
    pub const fn wipe_entry(self) -> WipeEntry {
        match self.residence {
            AcceleratorResidence::Durable { .. } => WipeEntry::DeleteWholePrefix,
            AcceleratorResidence::InProcess => WipeEntry::DropInProcessCache,
        }
    }
}

/// Where an `Accelerator` family's records live.
///
/// The two variants pair residence with record version on purpose: a durable
/// accelerator always has exactly one record version this binary reads and
/// writes, and an in-process accelerator has no encoded record at all.  Neither
/// "a durable family with no record version" nor "an in-process family with
/// one" can be written down.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcceleratorResidence {
    Durable {
        prefix: PersistentKeyPrefix,
        record_version: u8,
    },
    InProcess,
}

/// The external authority an `Accelerator` family rebuilds from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcceleratorRebuildAuthority {
    /// The MV lake descriptor, its publication facts, and the immutable target
    /// identity.
    MvLakeDescriptorAndPublicationFacts,
    /// The current provider-proven exact ref, head and provenance tuple, read
    /// together with the safety-age window that consumes it.
    ProvenOwnedRefWithSafetyAgeWindow,
    /// The connector's current schema and schema version.
    ConnectorSchemaVersion,
    /// The connector's immutable statistics evidence revision.
    ConnectorStatisticsEvidenceRevision,
}

/// What a rebuild from the declared authority actually reproduces.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RebuildDeterminism {
    /// The rebuild reproduces the same user-visible answers.  Internal
    /// identifiers may be reassigned by the rebuild; nothing user-visible
    /// depends on the discarded values.
    UserVisibleIdentical,
    /// The rebuild is safe but not identical: it restarts a conservative window
    /// instead of reproducing the discarded value.  A wipe therefore costs
    /// latency, never correctness — the family exists to let a safety window
    /// mature, and a fresh window is always the conservative side.
    ConservativeRestart,
}

/// What happens to a family when a deployment is cloned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClonePolicy {
    /// Only the logical configuration crosses the clone boundary.  Every
    /// derived identity — attachment id, CAS version, runtime generation,
    /// resolved secret, readiness — is re-established by the clone rather than
    /// copied, because those values name facts about the source deployment.
    SemanticRebind,
    /// The family never crosses a clone boundary; the clone re-establishes it
    /// from its own authority.
    NotCloned,
    /// The clone must re-validate copied records against the source revision; a
    /// mismatch wipes and rebuilds instead of trusting the copy.
    RevalidateSourceRevisionOrWipe,
    /// The clone always wipes the family and lets it rebuild.  Copying the
    /// records would import maturity the clone has not earned.
    WipeAndRebuild,
}

/// The destructive entry point that removes a whole family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WipeEntry {
    /// Range-delete the family's StateStore prefix.  No per-record fence or
    /// ordering is required: every record is rebuildable from the declared
    /// authority, so a partially completed wipe is only a partially completed
    /// rebuild.
    DeleteWholePrefix,
    /// Drop the in-process cache.  There is no durable residue to wipe.
    DropInProcessCache,
}
