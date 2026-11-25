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

use super::{
    CreateIntent, ManagedMvTarget, ManagementOwnershipError, ManagementTimestamp,
    ProcessIncarnation,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EffectIdentity([u8; 16]);

impl EffectIdentity {
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

/// External paths have separate guarantees. A catalog guarantee never covers
/// object deletion, and a delete guarantee never protects a catalog commit.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EffectPath {
    CatalogCommit,
    ObjectDeletion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EffectScope {
    catalog_commit: bool,
    object_deletion: bool,
}

impl EffectScope {
    pub const CATALOG_COMMIT: Self = Self {
        catalog_commit: true,
        object_deletion: false,
    };
    pub const OBJECT_DELETION: Self = Self {
        catalog_commit: false,
        object_deletion: true,
    };
    pub const CATALOG_AND_OBJECT_DELETION: Self = Self {
        catalog_commit: true,
        object_deletion: true,
    };

    pub const fn contains(self, path: EffectPath) -> bool {
        match path {
            EffectPath::CatalogCommit => self.catalog_commit,
            EffectPath::ObjectDeletion => self.object_deletion,
        }
    }

    pub const fn covers(self, required: Self) -> bool {
        (!required.catalog_commit || self.catalog_commit)
            && (!required.object_deletion || self.object_deletion)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectDisposition {
    KnownCommitted,
    KnownUncommitted,
    CommitUnknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffectResponsibility {
    identity: EffectIdentity,
    target: ManagedMvTarget,
    dispatching_incarnation: ProcessIncarnation,
    scope: EffectScope,
    last_possible_dispatch_at: ManagementTimestamp,
}

impl EffectResponsibility {
    pub const fn new(
        identity: EffectIdentity,
        target: ManagedMvTarget,
        dispatching_incarnation: ProcessIncarnation,
        scope: EffectScope,
        last_possible_dispatch_at: ManagementTimestamp,
    ) -> Self {
        Self {
            identity,
            target,
            dispatching_incarnation,
            scope,
            last_possible_dispatch_at,
        }
    }

    pub const fn identity(&self) -> EffectIdentity {
        self.identity
    }

    pub const fn target(&self) -> &ManagedMvTarget {
        &self.target
    }

    pub const fn dispatching_incarnation(&self) -> &ProcessIncarnation {
        &self.dispatching_incarnation
    }

    pub const fn scope(&self) -> EffectScope {
        self.scope
    }

    pub const fn last_possible_dispatch_at(&self) -> ManagementTimestamp {
        self.last_possible_dispatch_at
    }

    pub fn record_terminal(self, disposition: EffectDisposition) -> EffectTerminalFact {
        match disposition {
            EffectDisposition::KnownCommitted => EffectTerminalFact::KnownCommitted(self),
            EffectDisposition::KnownUncommitted => EffectTerminalFact::KnownUncommitted(self),
            EffectDisposition::CommitUnknown => {
                EffectTerminalFact::CommitUnknown(UnsettledEffect {
                    responsibility: self,
                    publication_recovery: None,
                })
            }
        }
    }
}

/// A dispatched staged CREATE before the provider has returned its real
/// object identity. It is deliberately not an `EffectResponsibility`: the
/// latter requires an exact physical target and callers must not fabricate one
/// merely to satisfy management bookkeeping.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateIntentResponsibility {
    intent: CreateIntent,
    dispatching_incarnation: ProcessIncarnation,
    scope: EffectScope,
    last_possible_dispatch_at: ManagementTimestamp,
}

impl CreateIntentResponsibility {
    pub const fn new(
        intent: CreateIntent,
        dispatching_incarnation: ProcessIncarnation,
        scope: EffectScope,
        last_possible_dispatch_at: ManagementTimestamp,
    ) -> Self {
        Self {
            intent,
            dispatching_incarnation,
            scope,
            last_possible_dispatch_at,
        }
    }

    pub const fn intent(&self) -> &CreateIntent {
        &self.intent
    }

    pub const fn identity(&self) -> EffectIdentity {
        self.intent.operation_id()
    }

    pub const fn dispatching_incarnation(&self) -> &ProcessIncarnation {
        &self.dispatching_incarnation
    }

    pub const fn scope(&self) -> EffectScope {
        self.scope
    }

    pub const fn last_possible_dispatch_at(&self) -> ManagementTimestamp {
        self.last_possible_dispatch_at
    }

    /// Narrow the pre-object responsibility only after the provider has
    /// returned its exact target. The operation identity, scope and dispatch
    /// timestamp remain unchanged.
    pub fn late_bind(
        self,
        target: ManagedMvTarget,
    ) -> Result<EffectResponsibility, ManagementOwnershipError> {
        let target = self.intent.bind_target(target)?;
        Ok(EffectResponsibility::new(
            self.identity(),
            target,
            self.dispatching_incarnation,
            self.scope,
            self.last_possible_dispatch_at,
        ))
    }
}

/// The original terminal result is immutable. Readmission closes only the
/// responsibility for possible future effects; it does not rewrite this fact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EffectTerminalFact {
    KnownCommitted(EffectResponsibility),
    KnownUncommitted(EffectResponsibility),
    CommitUnknown(UnsettledEffect),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnsettledEffect {
    responsibility: EffectResponsibility,
    pub(super) publication_recovery: Option<PublicationRecoveryEvidence>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicationRecoveryEvidence {
    pub original: novarocks_spi::connector::ExternalMutationEvidence,
    pub observation: Option<std::sync::Arc<novarocks_spi::connector::KeyedWriteObservation>>,
}

impl UnsettledEffect {
    pub const fn responsibility(&self) -> &EffectResponsibility {
        &self.responsibility
    }

    pub const fn publication_recovery(&self) -> Option<&PublicationRecoveryEvidence> {
        self.publication_recovery.as_ref()
    }

    pub const fn original_disposition(&self) -> EffectDisposition {
        EffectDisposition::CommitUnknown
    }
}
