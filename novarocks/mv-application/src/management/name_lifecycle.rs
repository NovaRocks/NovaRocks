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

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use novarocks_spi::connector::{
    CatalogHandle, ConnectorDocumentManagementOperation, ConnectorTableIdentity,
    ConnectorTableObjectId,
};

use super::{
    DeploymentOwner, EffectDisposition, EffectIdentity, EffectScope, ManagementClock,
    ManagementTimestamp, ProcessIncarnation, RemoteEffectPolicy, StartupIsolationEvidence,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NameStartupGrace {
    Discovering,
    Active { until: Option<ManagementTimestamp> },
    Ended(NameStartupGraceEnd),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NameStartupGraceEnd {
    NoRestartEvidence,
    Lifetime,
    Declaration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NameStartupGraceObservation {
    pub state: NameStartupGrace,
    pub incarnation: ProcessIncarnation,
    pub prior_incarnations: BTreeSet<ProcessIncarnation>,
    pub discovery_complete: bool,
}

pub(super) struct NameLifecycle {
    state: NameStartupGrace,
    last_reported: Option<NameStartupGrace>,
    startup_deadline: Option<ManagementTimestamp>,
    clock: Arc<dyn ManagementClock>,
    policy: RemoteEffectPolicy,
    prior_incarnations: BTreeSet<ProcessIncarnation>,
    discovery_complete: bool,
    effects: HashMap<ConnectorTableIdentity, NameEffect>,
}

struct NameEffect {
    catalog: CatalogHandle,
    object_id: Option<ConnectorTableObjectId>,
    identity: EffectIdentity,
    scope: EffectScope,
    operation: ConnectorDocumentManagementOperation,
    unknown: bool,
    until: Option<ManagementTimestamp>,
}

impl NameLifecycle {
    pub(super) fn new(
        started_at: Option<ManagementTimestamp>,
        policy: RemoteEffectPolicy,
        clock: Arc<dyn ManagementClock>,
    ) -> Self {
        let startup_deadline = started_at
            .and_then(|at| Self::deadline(&policy, EffectScope::CATALOG_AND_OBJECT_DELETION, at));
        Self {
            state: if started_at.is_some() {
                NameStartupGrace::Discovering
            } else {
                NameStartupGrace::Ended(NameStartupGraceEnd::NoRestartEvidence)
            },
            last_reported: None,
            startup_deadline,
            clock,
            policy,
            prior_incarnations: BTreeSet::new(),
            discovery_complete: true,
            effects: HashMap::new(),
        }
    }

    fn deadline(
        policy: &RemoteEffectPolicy,
        scope: EffectScope,
        since: ManagementTimestamp,
    ) -> Option<ManagementTimestamp> {
        let bound = policy.guarantee_for(scope)?;
        since
            .checked_add(bound.lifetime().checked_add(bound.margin())?)
            .ok()
    }

    pub(super) fn state(&mut self) -> NameStartupGrace {
        if let NameStartupGrace::Active { until: Some(until) } = self.state
            && self.clock.now().is_ok_and(|now| now >= until)
        {
            self.state = NameStartupGrace::Ended(NameStartupGraceEnd::Lifetime);
        }
        self.state
    }

    pub(super) fn observe(
        &mut self,
        incarnation: &ProcessIncarnation,
    ) -> NameStartupGraceObservation {
        NameStartupGraceObservation {
            state: self.state(),
            incarnation: incarnation.clone(),
            prior_incarnations: self.prior_incarnations.clone(),
            discovery_complete: self.state != NameStartupGrace::Discovering
                && self.discovery_complete,
        }
    }

    pub(super) fn take_transition(
        &mut self,
        incarnation: &ProcessIncarnation,
    ) -> Option<NameStartupGraceObservation> {
        let observation = self.observe(incarnation);
        if self.last_reported == Some(observation.state) {
            return None;
        }
        self.last_reported = Some(observation.state);
        Some(observation)
    }

    pub(super) fn note_discovery(&mut self, previous: Option<ProcessIncarnation>) {
        if self.state != NameStartupGrace::Discovering {
            return;
        }
        if let Some(previous) = previous {
            self.prior_incarnations.insert(previous);
        } else {
            self.discovery_complete = false;
        }
    }

    pub(super) fn finish_discovery(&mut self, catalogs_available: bool) {
        if self.state != NameStartupGrace::Discovering {
            return;
        }
        self.discovery_complete &= catalogs_available;
        self.state = if self.discovery_complete && self.prior_incarnations.is_empty() {
            NameStartupGrace::Ended(NameStartupGraceEnd::NoRestartEvidence)
        } else {
            NameStartupGrace::Active {
                until: self.startup_deadline,
            }
        };
    }

    pub(super) fn accept_declaration(
        &mut self,
        evidence: &StartupIsolationEvidence,
        owner: &DeploymentOwner,
        incarnation: &ProcessIncarnation,
    ) {
        if matches!(self.state(), NameStartupGrace::Active { .. })
            && evidence.settled_for_launch(owner, incarnation)
            && evidence.covers_incarnations(&self.prior_incarnations)
        {
            self.state = NameStartupGrace::Ended(NameStartupGraceEnd::Declaration);
        }
    }

    pub(super) fn isolated(&self, table: &ConnectorTableIdentity) -> bool {
        self.effects.get(table).is_some_and(|effect| {
            !effect
                .until
                .is_some_and(|until| self.clock.now().is_ok_and(|now| now >= until))
        })
    }

    pub(super) fn register(
        &mut self,
        catalog: &CatalogHandle,
        table: &ConnectorTableIdentity,
        object_id: Option<&ConnectorTableObjectId>,
        identity: EffectIdentity,
        scope: EffectScope,
        operation: ConnectorDocumentManagementOperation,
    ) -> bool {
        if self.isolated(table) {
            return false;
        }
        self.effects.insert(
            table.clone(),
            NameEffect {
                catalog: catalog.clone(),
                object_id: object_id.cloned(),
                identity,
                scope,
                operation,
                unknown: false,
                until: None,
            },
        );
        true
    }

    pub(super) fn settle(
        &mut self,
        catalog: &CatalogHandle,
        table: &ConnectorTableIdentity,
        identity: EffectIdentity,
        disposition: EffectDisposition,
    ) {
        let Some(effect) = self
            .effects
            .get_mut(table)
            .filter(|effect| effect.identity == identity && &effect.catalog == catalog)
        else {
            return;
        };
        if disposition == EffectDisposition::CommitUnknown {
            // The original holder can still dispatch later stages until it reports
            // the terminal outcome or drops. Measure from that boundary.
            effect.unknown = true;
            effect.until = self
                .clock
                .now()
                .ok()
                .and_then(|at| Self::deadline(&self.policy, effect.scope, at));
        } else {
            self.effects.remove(table);
        }
    }

    pub(super) fn bind_create(
        &mut self,
        catalog: &CatalogHandle,
        table: &ConnectorTableIdentity,
        identity: EffectIdentity,
        object_id: &ConnectorTableObjectId,
    ) -> bool {
        let Some(effect) = self.effects.get_mut(table).filter(|effect| {
            effect.identity == identity
                && &effect.catalog == catalog
                && effect.operation == ConnectorDocumentManagementOperation::Create
                && effect.unknown
        }) else {
            return false;
        };
        effect.object_id = Some(object_id.clone());
        true
    }

    pub(super) fn expire_create(&mut self, table: &ConnectorTableIdentity) {
        if self
            .effects
            .get(table)
            .is_some_and(|effect| effect.operation == ConnectorDocumentManagementOperation::Create)
            && !self.isolated(table)
        {
            self.effects.remove(table);
        }
    }

    pub(super) fn unresolved_drop(
        &self,
        catalog: &CatalogHandle,
        table: &ConnectorTableIdentity,
    ) -> Option<(EffectIdentity, &ConnectorTableObjectId)> {
        let effect = self.effects.get(table)?;
        if !effect.unknown
            || &effect.catalog != catalog
            || effect.operation != ConnectorDocumentManagementOperation::Drop
        {
            return None;
        }
        Some((effect.identity, effect.object_id.as_ref()?))
    }
}
