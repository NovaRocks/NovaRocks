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

//! The registry: every frontend state family and the contract it declares.

use super::classification::{
    AcceleratorContract, AcceleratorRebuildAuthority, AcceleratorResidence, ClonePolicy,
    DurabilityAdmission, PersistentKeyPrefix, ProcessRuntimeAuthority, ProcessRuntimeContract,
    RebuildDeterminism, StateFamilyClassification,
};

/// Every frontend state family, registered exactly once.
///
/// Retired families are absent by deletion, not by a tombstone entry: this
/// binary has no reader for them, so registering them would be the compatibility
/// surface the hard cut exists to remove.
///
/// Maintenance, statistics, and MV process state are likewise absent: their
/// product crates own those lifetimes and their durable descriptors. Frontend
/// must not retain a nominal ownership entry after a product becomes the only
/// authority.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum StateFamily {
    /// Resolved connector table metadata, validated against the connector's
    /// current schema version.
    SchemaCache,
    /// Immutable connector statistics artifacts, keyed by evidence revision.
    StatisticsArtifactCache,
    /// Views defined in the local (non-external) catalog.
    LocalViewRegistry,
    /// DML operations, side records and their coordination state.
    DmlRuntime,
    /// Backend liveness, generation and fragment activity as this frontend
    /// observed it.
    BackendObservedRuntime,
}

impl StateFamily {
    /// The number of registered families.
    ///
    /// Hand-written, and checked against the chain below at compile time.
    pub const COUNT: usize = 5;

    /// Every registered family, in manifest order.
    ///
    /// Derived from [`StateFamily::next_in_manifest`] rather than hand-listed,
    /// so it cannot fall behind the enum: a new variant must be linked into
    /// that exhaustive chain to compile at all, and once linked it appears here
    /// automatically.
    pub const ALL: [Self; Self::COUNT] = Self::enumerate();

    const FIRST: Self = Self::SchemaCache;

    /// The contract this family declares.
    ///
    /// One exhaustive `match`, which is what forces a new family to pick a
    /// classification, an authority, a record version and a retain/clone policy
    /// before it can exist.
    pub const fn classification(self) -> StateFamilyClassification {
        match self {
            Self::SchemaCache => StateFamilyClassification::Accelerator(AcceleratorContract::new(
                AcceleratorResidence::InProcess,
                AcceleratorRebuildAuthority::ConnectorSchemaVersion,
                RebuildDeterminism::UserVisibleIdentical,
                false,
                ClonePolicy::NotCloned,
            )),
            Self::StatisticsArtifactCache => {
                StateFamilyClassification::Accelerator(AcceleratorContract::new(
                    AcceleratorResidence::InProcess,
                    AcceleratorRebuildAuthority::ConnectorStatisticsEvidenceRevision,
                    RebuildDeterminism::UserVisibleIdentical,
                    false,
                    ClonePolicy::NotCloned,
                ))
            }
            // A local view exists only while the frontend that defined it does.
            // Deployments that need durable views define them in an external
            // catalog, which owns them as provider truth instead.
            Self::LocalViewRegistry => StateFamilyClassification::ProcessRuntime(
                ProcessRuntimeContract::new(ProcessRuntimeAuthority::FrontendIncarnation),
            ),
            Self::DmlRuntime => StateFamilyClassification::ProcessRuntime(
                ProcessRuntimeContract::new(ProcessRuntimeAuthority::Statement),
            ),
            Self::BackendObservedRuntime => StateFamilyClassification::ProcessRuntime(
                ProcessRuntimeContract::new(ProcessRuntimeAuthority::FrontendIncarnation),
            ),
        }
    }

    /// Stable identifier for logs, metrics and operator-facing errors.
    ///
    /// These strings outlive refactors of the Rust identifiers, so they are
    /// spelled out rather than derived from the variant name.
    pub const fn family_id(self) -> &'static str {
        match self {
            Self::SchemaCache => "frontend/catalog/schema-cache",
            Self::StatisticsArtifactCache => "frontend/statistics/immutable-artifact-cache",
            Self::LocalViewRegistry => "frontend/view/local-registry",
            Self::DmlRuntime => "frontend/dml/runtime",
            Self::BackendObservedRuntime => "frontend/cluster-backends/observed-runtime",
        }
    }

    /// This frontend family's persistent key prefix, or `None` when it owns no
    /// StateStore records.
    ///
    /// Frontend currently owns no durable family. Product-owned descriptors
    /// intentionally live in their product crates instead of being projected
    /// into this local manifest.
    pub const fn persistent_prefix(self) -> Option<PersistentKeyPrefix> {
        self.classification().persistent_prefix()
    }

    /// Whether this family may own a StateStore record at all.
    pub const fn durability_admission(self) -> DurabilityAdmission {
        self.classification().durability_admission()
    }

    /// Whether records of this family survive a frontend restart.
    pub const fn retain_on_restart(self) -> bool {
        self.classification().retain_on_restart()
    }

    /// What happens to this family when a deployment is cloned.
    pub const fn clone_policy(self) -> ClonePolicy {
        self.classification().clone_policy()
    }

    /// The single record version this binary reads and writes, or `None` when
    /// the family encodes no record.
    pub const fn record_version(self) -> Option<u8> {
        self.classification().record_version()
    }

    /// The registered family that owns `key`, or `None` when no family does.
    ///
    /// Attribution is by persistent prefix, and only the durable accelerator
    /// classification can carry one, so a `Some` answer already implies the
    /// owner is allowed to be durable.  The store-content gate still asks
    /// [`StateFamily::durability_admission`] separately, so "the key is
    /// attributable" and "its owner may persist" stay two independent
    /// assertions instead of one inferring the other.
    ///
    /// Attribution is unambiguous because no registered prefix is a prefix of
    /// another — see `persistent_prefixes_are_unique_and_non_nested`.
    pub fn for_key(key: &[u8]) -> Option<Self> {
        Self::ALL.into_iter().find(|family| {
            family
                .persistent_prefix()
                .is_some_and(|prefix| key.starts_with(prefix.as_bytes()))
        })
    }

    /// The family that follows `self` in manifest order, or `None` for the last.
    ///
    /// This chain, not a hand-written array, is what makes [`StateFamily::ALL`]
    /// complete.  The `match` is exhaustive, so a new variant cannot be added
    /// without being linked in, and `enumerate` walks the chain at compile time
    /// and rejects a length that disagrees with [`StateFamily::COUNT`].
    const fn next_in_manifest(self) -> Option<Self> {
        match self {
            Self::SchemaCache => Some(Self::StatisticsArtifactCache),
            Self::StatisticsArtifactCache => Some(Self::LocalViewRegistry),
            Self::LocalViewRegistry => Some(Self::DmlRuntime),
            Self::DmlRuntime => Some(Self::BackendObservedRuntime),
            Self::BackendObservedRuntime => None,
        }
    }

    /// Walks the manifest chain into an array.
    ///
    /// Evaluated as a `const`, so both failures below are compile errors rather
    /// than runtime ones: a family linked into the chain without bumping
    /// `COUNT` ends the walk early, and a `COUNT` raised without linking a
    /// family leaves the chain running past the end.
    const fn enumerate() -> [Self; Self::COUNT] {
        let mut families = [Self::FIRST; Self::COUNT];
        let mut index = 1;
        while index < Self::COUNT {
            families[index] = match families[index - 1].next_in_manifest() {
                Some(next) => next,
                None => panic!("state family chain ends before COUNT families are registered"),
            };
            index += 1;
        }
        assert!(
            families[Self::COUNT - 1].next_in_manifest().is_none(),
            "state family chain continues past COUNT registered families"
        );
        families
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::super::classification::WipeEntry;
    use super::*;

    /// Five Frontend families: two in-process `Accelerator` and
    /// three `ProcessRuntime`.
    ///
    /// Backend desired state is deliberately absent. It was registered while
    /// the frontend still carried a durable membership record; backend
    /// self-registration removed that record, and an orchestrator-owned
    /// desired state the frontend never projects is not a frontend-local
    /// state family at all.
    #[test]
    fn manifest_registers_exactly_the_spec_family_table() {
        assert_eq!(
            StateFamily::ALL.len(),
            5,
            "the manifest registers five frontend state families"
        );

        let mut process_runtime = 0;
        let mut accelerator = 0;
        for family in StateFamily::ALL {
            // Exhaustive over the closed classification: a fourth variant makes
            // this match, and every other consumer, fail to compile.
            match family.classification() {
                StateFamilyClassification::ProcessRuntime(_) => process_runtime += 1,
                StateFamilyClassification::Accelerator(_) => accelerator += 1,
            }
        }

        assert_eq!(accelerator, 2, "schema cache, statistics artifact cache");
        assert_eq!(process_runtime, 3, "local views, DML, backend observations");
    }

    #[test]
    fn manifest_ids_are_unique_and_the_chain_visits_each_family_once() {
        let ids: BTreeSet<&str> = StateFamily::ALL
            .into_iter()
            .map(StateFamily::family_id)
            .collect();
        assert_eq!(
            ids.len(),
            StateFamily::ALL.len(),
            "every registered family needs a distinct stable id"
        );

        let families: BTreeSet<StateFamily> = StateFamily::ALL.into_iter().collect();
        assert_eq!(
            families.len(),
            StateFamily::ALL.len(),
            "the manifest chain must not visit a family twice"
        );
    }

    /// Key-to-family attribution is only well defined when no prefix contains
    /// another; otherwise a key under the longer prefix would also match the
    /// shorter one and the store-content gate could not name its owner.
    #[test]
    fn persistent_prefixes_are_unique_and_non_nested() {
        let prefixes: Vec<(StateFamily, &str)> = StateFamily::ALL
            .into_iter()
            .filter_map(|family| {
                family
                    .persistent_prefix()
                    .map(|prefix| (family, prefix.as_str()))
            })
            .collect();
        assert!(prefixes.is_empty(), "Frontend owns no durable family today");

        let distinct: BTreeSet<&str> = prefixes.iter().map(|(_, prefix)| *prefix).collect();
        assert_eq!(
            distinct.len(),
            prefixes.len(),
            "Frontend durable families must never share a prefix"
        );

        for (left_family, left) in &prefixes {
            for (right_family, right) in &prefixes {
                if left_family == right_family {
                    continue;
                }
                assert!(
                    !left.starts_with(*right),
                    "{} prefix {left:?} is nested under {} prefix {right:?}",
                    left_family.family_id(),
                    right_family.family_id()
                );
            }
        }
    }

    /// A `ProcessRuntime` family cannot yield a persistent prefix, and this test
    /// shows it by *shape* rather than by assertion.
    ///
    /// `prefix_of` is a total function over the closed classification.  Its
    /// `ProcessRuntime` arm has nothing to return a prefix from:
    /// `ProcessRuntimeContract` holds no prefix field and exposes no prefix
    /// accessor, so the only expression that arm can produce is `None`.  Adding
    /// a prefix to a `ProcessRuntime` entry would mean adding it to that
    /// contract, which is a change to the type, not to this test.
    #[test]
    fn process_runtime_cannot_express_a_persistent_prefix() {
        fn prefix_of(classification: StateFamilyClassification) -> Option<&'static str> {
            match classification {
                StateFamilyClassification::Accelerator(contract) => contract
                    .persistent_prefix()
                    .map(PersistentKeyPrefix::as_str),
                StateFamilyClassification::ProcessRuntime(contract) => {
                    // The only fact available here is the authority the family
                    // belongs to.  There is no prefix to name.
                    let _authority = contract.authority();
                    None
                }
            }
        }

        for family in StateFamily::ALL {
            let classification = family.classification();
            assert_eq!(
                prefix_of(classification),
                classification
                    .persistent_prefix()
                    .map(PersistentKeyPrefix::as_str),
                "{} must derive its prefix only through the persistent contracts",
                family.family_id()
            );

            if matches!(classification, StateFamilyClassification::ProcessRuntime(_)) {
                assert_eq!(
                    family.durability_admission(),
                    DurabilityAdmission::Forbidden,
                    "{} is runtime state and must never own a StateStore record",
                    family.family_id()
                );
                assert!(!family.retain_on_restart());
                assert_eq!(family.clone_policy(), ClonePolicy::NotCloned);
                assert_eq!(family.record_version(), None);
            }
        }
    }

    #[test]
    fn every_accelerator_answers_retain_clone_and_wipe() {
        let mut accelerators = 0;
        for family in StateFamily::ALL {
            let StateFamilyClassification::Accelerator(contract) = family.classification() else {
                continue;
            };
            accelerators += 1;

            let retain = contract.retain_on_restart();
            let clone_policy = contract.clone_policy();
            let wipe_entry = contract.wipe_entry();
            let determinism = contract.rebuild_determinism();

            match contract.residence() {
                AcceleratorResidence::Durable {
                    prefix,
                    record_version,
                } => {
                    assert!(
                        !prefix.as_str().is_empty(),
                        "{} declares a durable prefix",
                        family.family_id()
                    );
                    assert!(record_version > 0, "{}", family.family_id());
                    assert!(
                        retain,
                        "{} is durable so a restart must find it",
                        family.family_id()
                    );
                    assert_eq!(wipe_entry, WipeEntry::DeleteWholePrefix);
                    assert!(
                        matches!(
                            clone_policy,
                            ClonePolicy::RevalidateSourceRevisionOrWipe
                                | ClonePolicy::WipeAndRebuild
                        ),
                        "{} must not carry derived records into a clone unchecked",
                        family.family_id()
                    );
                    assert_eq!(
                        family.durability_admission(),
                        DurabilityAdmission::Permitted
                    );
                }
                AcceleratorResidence::InProcess => {
                    assert!(
                        !retain,
                        "{} lives in process memory only",
                        family.family_id()
                    );
                    assert_eq!(wipe_entry, WipeEntry::DropInProcessCache);
                    assert_eq!(clone_policy, ClonePolicy::NotCloned);
                    assert_eq!(contract.record_version(), None);
                    assert_eq!(
                        family.durability_admission(),
                        DurabilityAdmission::Forbidden
                    );
                }
            }

            assert_eq!(
                determinism,
                RebuildDeterminism::UserVisibleIdentical,
                "{}",
                family.family_id()
            );
        }
        assert_eq!(accelerators, 2);
    }

    #[test]
    fn every_process_runtime_family_declares_its_authority() {
        let mut authorities = Vec::new();
        for family in StateFamily::ALL {
            if let StateFamilyClassification::ProcessRuntime(contract) = family.classification() {
                authorities.push((family, contract.authority()));
            }
        }
        assert_eq!(authorities.len(), 3);

        assert!(
            authorities.contains(&(StateFamily::DmlRuntime, ProcessRuntimeAuthority::Statement))
        );
        assert!(authorities.contains(&(
            StateFamily::LocalViewRegistry,
            ProcessRuntimeAuthority::FrontendIncarnation
        )));
        assert!(authorities.contains(&(
            StateFamily::BackendObservedRuntime,
            ProcessRuntimeAuthority::FrontendIncarnation
        )));
    }

    #[test]
    fn key_attribution_names_the_owning_family_or_nothing() {
        // Retired families are unattributable by construction: they are absent
        // from the manifest, so the store-content gate reports them instead of
        // finding an owner willing to claim them.
        assert_eq!(StateFamily::for_key(b"\0novarocks/cp/v1/control"), None);
        assert_eq!(StateFamily::for_key(b"novarocks/frontend/views/v2/x"), None);
        assert_eq!(
            StateFamily::for_key(b"novarocks/frontend/mv/accelerator/v1/projection/by-id/1"),
            None
        );
        assert_eq!(StateFamily::for_key(b""), None);

        for family in StateFamily::ALL {
            let Some(prefix) = family.persistent_prefix() else {
                continue;
            };
            assert_eq!(
                StateFamily::for_key(prefix.as_bytes()),
                Some(family),
                "{} must claim its own prefix",
                family.family_id()
            );
            assert_eq!(
                family.durability_admission(),
                DurabilityAdmission::Permitted,
                "{} owns keys so its classification must permit durability",
                family.family_id()
            );
        }
    }
}
