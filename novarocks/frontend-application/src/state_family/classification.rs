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

//! The closed two-way classification every state family must declare.
//!
//! The classification is not a label attached to a family after the fact; it
//! decides whether the family may own a StateStore record at all. Frontend
//! accelerators are in-process, and durable product descriptors remain owned
//! by their product crates.

/// A StateStore key prefix owned by exactly one persistent state family.
///
/// The literal is private and has no public constructor. Durable product
/// descriptors live with their product crates, so Frontend cannot mint a
/// local durable owner.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PersistentKeyPrefix(&'static str);

impl PersistentKeyPrefix {
    pub const fn as_str(self) -> &'static str {
        self.0
    }

    pub const fn as_bytes(self) -> &'static [u8] {
        self.0.as_bytes()
    }
}

/// Exactly one classification per family, chosen from a closed set of two.
///
/// Consumers match this exhaustively, so a fourth classification cannot be
/// introduced without every consumer being forced to decide what it means.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateFamilyClassification {
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
    /// A `ProcessRuntime` contract has no prefix field or accessor, while this
    /// manifest's accelerator contracts are all in-process. Durable product
    /// descriptors remain outside this local classification.
    pub const fn persistent_prefix(self) -> Option<PersistentKeyPrefix> {
        match self {
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
            // A `ProcessRuntime` family's authority ends with the process, so
            // nothing it wrote can still be valid afterwards.
            Self::ProcessRuntime(_) => false,
            Self::Accelerator(contract) => contract.retain_on_restart(),
        }
    }

    /// What happens to this family when a deployment is cloned.
    pub const fn clone_policy(self) -> ClonePolicy {
        match self {
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

/// What a `ProcessRuntime` family declares — and, more importantly, what it
/// structurally cannot.
///
/// This struct has exactly one field. There is deliberately no prefix field,
/// no `Option<PersistentKeyPrefix>`, and no prefix accessor, so "give a
/// `ProcessRuntime` family a persistent prefix" is a type error rather than a
/// rule someone has to remember to check.
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
            AcceleratorResidence::InProcess => None,
        }
    }

    pub const fn record_version(self) -> Option<u8> {
        match self.residence {
            AcceleratorResidence::InProcess => None,
        }
    }

    /// The external authority this family is rebuilt from.
    pub const fn rebuild_authority(self) -> AcceleratorRebuildAuthority {
        self.rebuild_authority
    }

    /// What rebuilding the accelerator actually reproduces from that authority.
    pub const fn rebuild_determinism(self) -> RebuildDeterminism {
        self.rebuild_determinism
    }

    pub const fn retain_on_restart(self) -> bool {
        self.retain_on_restart
    }

    pub const fn clone_policy(self) -> ClonePolicy {
        self.clone_policy
    }

    /// The destructive entry point that removes this in-process family.
    pub const fn wipe_entry(self) -> WipeEntry {
        match self.residence {
            AcceleratorResidence::InProcess => WipeEntry::DropInProcessCache,
        }
    }
}

/// Where an `Accelerator` family's records live.
///
/// Frontend accelerators are in-process; durable product descriptors belong to
/// their product crates.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcceleratorResidence {
    InProcess,
}

/// The external authority an `Accelerator` family rebuilds from.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcceleratorRebuildAuthority {
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
}

/// What happens to a family when a deployment is cloned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClonePolicy {
    /// The family never crosses a clone boundary; the clone re-establishes it
    /// from its own authority.
    NotCloned,
}

/// The destructive entry point that removes a whole family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WipeEntry {
    /// Drop the in-process cache.  There is no durable residue to wipe.
    DropInProcessCache,
}
