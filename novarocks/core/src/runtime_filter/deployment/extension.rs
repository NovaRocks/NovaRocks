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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Mutex;

use crate::common::types::UniqueId;
use crate::runtime_filter::deployment::RuntimeFilterDeploymentPlan;
use crate::runtime_filter::port::identity::{DeploymentEpoch, RuntimeFilterParticipantId};
use crate::runtime_filter::port::install::{
    RuntimeFilterInstallView, RuntimeFilterParticipantInstall,
};
use crate::runtime_filter::port::routing::RuntimeFilterRoutingShard;

/// Install-port failures surfaced once a real coordinator starts issuing
/// installs (RFD-6). Distinct from RFD-2's compile-time `DeploymentError`:
/// these are runtime phase-contract violations, not static plan defects.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DeploymentInstallError {
    /// The participant already has a deployment installed under a different epoch.
    EpochConflict { installed: u64, incoming: u64 },
    /// The coordinator produced a core view without the matching routing authority.
    MissingRoutingShard { participant: u32 },
    /// An install projection disagrees with its plan or install-phase epoch.
    EpochIdentityMismatch {
        authority: &'static str,
        expected: u64,
        actual: u64,
    },
    /// An install projection disagrees with its map key or install target.
    ParticipantIdentityMismatch {
        authority: &'static str,
        expected: u32,
        actual: u32,
    },
    /// Same epoch, but either side of the incoming composite differs.
    ConflictingDeployment { participant: u32 },
}

impl fmt::Display for DeploymentInstallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EpochConflict {
                installed,
                incoming,
            } => write!(
                f,
                "runtime filter install epoch conflict: installed {installed}, incoming {incoming}"
            ),
            Self::MissingRoutingShard { participant } => write!(
                f,
                "runtime filter install is missing routing shard for participant {participant}"
            ),
            Self::EpochIdentityMismatch {
                authority,
                expected,
                actual,
            } => write!(
                f,
                "runtime filter install epoch identity mismatch for {authority}: expected \
                 {expected}, actual {actual}"
            ),
            Self::ParticipantIdentityMismatch {
                authority,
                expected,
                actual,
            } => write!(
                f,
                "runtime filter install participant identity mismatch for {authority}: expected \
                 {expected}, actual {actual}"
            ),
            Self::ConflictingDeployment { participant } => write!(
                f,
                "runtime filter install conflict: participant {participant} received a \
                 different deployment for the same epoch"
            ),
        }
    }
}

impl std::error::Error for DeploymentInstallError {}

/// Coordinator-side install port. `RuntimeFilterInstallPort` is RFD-2's own
/// abstraction; RFD-6 later provides the real adapter that wraps
/// `RuntimeFilterService::install` on each participant BE. Defining the
/// contract here lets the pre-submit phase contract (participant-only
/// install, idempotent retries, epoch-conflict rejection) be proven against a
/// fake ahead of any live wiring.
pub(crate) trait RuntimeFilterInstallPort: Send + Sync {
    fn install(
        &self,
        query_id: UniqueId,
        epoch: DeploymentEpoch,
        participant: RuntimeFilterParticipantId,
        install: RuntimeFilterParticipantInstall,
    ) -> Result<(), DeploymentInstallError>;
}

/// RF pre-submit extension. Turns a compiled [`RuntimeFilterDeploymentPlan`]
/// into per-participant install requests. It does not own query lifecycle —
/// The frontend query execution owner wires this into its pre-submit phase.
#[derive(Debug, Default)]
pub(crate) struct RuntimeFilterDeploymentExtension;

impl RuntimeFilterDeploymentExtension {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Install requests for the participants the compiler assigned an install
    /// view to (participant-only fan-out — a strict subset of the plan's live
    /// `participants`; role-less backends get no install request).
    pub(crate) fn participant_installs(
        &self,
        plan: &RuntimeFilterDeploymentPlan,
    ) -> Result<
        Vec<(RuntimeFilterParticipantId, RuntimeFilterParticipantInstall)>,
        DeploymentInstallError,
    > {
        for (participant, view) in &plan.install_views {
            validate_core_view_identity(plan.epoch, *participant, view)?;
        }
        for (participant, routing_shard) in &plan.routing_shards {
            validate_routing_shard_identity(plan.epoch, *participant, routing_shard)?;
        }

        plan.install_views
            .keys()
            .copied()
            .chain(
                plan.routing_shards
                    .iter()
                    .filter_map(|(participant, shard)| {
                        (!shard.channels().is_empty()).then_some(*participant)
                    }),
            )
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|participant| {
                let view = plan
                    .install_views
                    .get(&participant)
                    .cloned()
                    .unwrap_or_else(|| {
                        RuntimeFilterInstallView::new(plan.epoch, participant, BTreeMap::new())
                    });
                let routing_shard = plan.routing_shards.get(&participant).ok_or(
                    DeploymentInstallError::MissingRoutingShard {
                        participant: participant.get(),
                    },
                )?;
                let install = RuntimeFilterParticipantInstall::new(view, routing_shard.clone());
                validate_install_identity(plan.epoch, participant, &install)?;
                Ok((participant, install))
            })
            .collect()
    }
}

/// Recording fake [`RuntimeFilterInstallPort`]: idempotent on an identical
/// `(epoch, composite)` retry for a participant, rejects a differing epoch for a
/// participant that already has a deployment installed. Used to prove the
/// pre-submit phase contract ahead of RFD-6's real adapter.
#[derive(Default)]
pub(crate) struct RecordingInstallPort {
    installed: Mutex<
        BTreeMap<RuntimeFilterParticipantId, (DeploymentEpoch, RuntimeFilterParticipantInstall)>,
    >,
}

impl RecordingInstallPort {
    /// True iff every participant in `participants` has a recorded install.
    pub(crate) fn all_installed(
        &self,
        participants: &BTreeSet<RuntimeFilterParticipantId>,
    ) -> bool {
        let guard = self
            .installed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        participants.iter().all(|p| guard.contains_key(p))
    }
}

impl RuntimeFilterInstallPort for RecordingInstallPort {
    fn install(
        &self,
        _query_id: UniqueId,
        epoch: DeploymentEpoch,
        participant: RuntimeFilterParticipantId,
        install: RuntimeFilterParticipantInstall,
    ) -> Result<(), DeploymentInstallError> {
        validate_install_identity(epoch, participant, &install)?;
        let mut guard = self
            .installed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some((existing_epoch, existing_install)) = guard.get(&participant) {
            if existing_epoch.get() != epoch.get() {
                return Err(DeploymentInstallError::EpochConflict {
                    installed: existing_epoch.get(),
                    incoming: epoch.get(),
                });
            }
            if existing_install != &install {
                return Err(DeploymentInstallError::ConflictingDeployment {
                    participant: participant.get(),
                });
            }
            return Ok(()); // idempotent retry
        }
        guard.insert(participant, (epoch, install));
        Ok(())
    }
}

fn validate_install_identity(
    outer_epoch: DeploymentEpoch,
    outer_participant: RuntimeFilterParticipantId,
    install: &RuntimeFilterParticipantInstall,
) -> Result<(), DeploymentInstallError> {
    validate_core_view_identity(outer_epoch, outer_participant, install.core_view())?;
    validate_routing_shard_identity(outer_epoch, outer_participant, install.routing_shard())
}

fn validate_core_view_identity(
    expected_epoch: DeploymentEpoch,
    expected_participant: RuntimeFilterParticipantId,
    view: &RuntimeFilterInstallView,
) -> Result<(), DeploymentInstallError> {
    if view.epoch() != expected_epoch {
        return Err(DeploymentInstallError::EpochIdentityMismatch {
            authority: "core view",
            expected: expected_epoch.get(),
            actual: view.epoch().get(),
        });
    }
    if view.local_participant_id() != expected_participant {
        return Err(DeploymentInstallError::ParticipantIdentityMismatch {
            authority: "core view",
            expected: expected_participant.get(),
            actual: view.local_participant_id().get(),
        });
    }
    Ok(())
}

fn validate_routing_shard_identity(
    expected_epoch: DeploymentEpoch,
    expected_participant: RuntimeFilterParticipantId,
    routing_shard: &RuntimeFilterRoutingShard,
) -> Result<(), DeploymentInstallError> {
    if routing_shard.deployment_epoch() != expected_epoch {
        return Err(DeploymentInstallError::EpochIdentityMismatch {
            authority: "routing shard",
            expected: expected_epoch.get(),
            actual: routing_shard.deployment_epoch().get(),
        });
    }
    if routing_shard.local_participant_id() != expected_participant {
        return Err(DeploymentInstallError::ParticipantIdentityMismatch {
            authority: "routing shard",
            expected: expected_participant.get(),
            actual: routing_shard.local_participant_id().get(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    use crate::common::types::UniqueId;
    use crate::runtime_filter::deployment::role_graph::RoleGraph;
    use crate::runtime_filter::model::contract::{BindingId, ChannelId};
    use crate::runtime_filter::port::identity::{DeploymentEpoch, RuntimeFilterParticipantId};
    use crate::runtime_filter::port::install::{
        RuntimeFilterInstallView, RuntimeFilterParticipantInstall,
    };
    use crate::runtime_filter::port::routing::{
        RuntimeFilterChannelRoutingView, RuntimeFilterRouteRole, RuntimeFilterRoutingShard,
    };

    const QUERY: UniqueId = UniqueId::new(1, 1);

    fn pid(x: u32) -> RuntimeFilterParticipantId {
        RuntimeFilterParticipantId::new(x)
    }

    fn shard(
        epoch: DeploymentEpoch,
        participant: RuntimeFilterParticipantId,
    ) -> RuntimeFilterRoutingShard {
        RuntimeFilterRoutingShard::new(epoch, participant, BTreeMap::new()).unwrap()
    }

    fn changed_shard(
        epoch: DeploymentEpoch,
        participant: RuntimeFilterParticipantId,
    ) -> RuntimeFilterRoutingShard {
        let channel_id = ChannelId::new(1);
        let channel = RuntimeFilterChannelRoutingView::new(
            channel_id,
            BTreeSet::from([RuntimeFilterRouteRole::Consumer(BindingId::new(2))]),
            BTreeMap::new(),
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
        RuntimeFilterRoutingShard::new(epoch, participant, BTreeMap::from([(channel_id, channel)]))
            .unwrap()
    }

    fn sample_plan(epoch: u64) -> RuntimeFilterDeploymentPlan {
        let e = DeploymentEpoch::new(epoch);
        let mut install_views = BTreeMap::new();
        let mut routing_shards = BTreeMap::new();
        for p in [pid(10), pid(1)] {
            install_views.insert(p, RuntimeFilterInstallView::new(e, p, BTreeMap::new()));
            routing_shards.insert(p, shard(e, p));
        }
        RuntimeFilterDeploymentPlan {
            epoch: e,
            participants: BTreeSet::from([pid(10), pid(1)]),
            install_views,
            routing_shards,
            role_graph: RoleGraph::default(),
        }
    }

    fn sample_plan_with_roleless_participant(epoch: u64) -> RuntimeFilterDeploymentPlan {
        let e = DeploymentEpoch::new(epoch);
        let mut install_views = BTreeMap::new();
        let mut routing_shards = BTreeMap::new();
        for p in [pid(10), pid(1)] {
            install_views.insert(p, RuntimeFilterInstallView::new(e, p, BTreeMap::new()));
            routing_shards.insert(p, shard(e, p));
        }
        RuntimeFilterDeploymentPlan {
            epoch: e,
            // pid(2) is a live backend with no RF role; it must NOT be installed.
            participants: BTreeSet::from([pid(10), pid(1), pid(2)]),
            install_views,
            routing_shards,
            role_graph: RoleGraph::default(),
        }
    }

    #[test]
    fn participant_installs_pair_every_core_view_with_its_matching_routing_shard() {
        let plan = sample_plan_with_roleless_participant(7);
        let ext = RuntimeFilterDeploymentExtension::new();
        let installs = ext.participant_installs(&plan).unwrap();
        // Only the backends the compiler assigned a view to, never the role-less pid(2).
        assert_eq!(installs.len(), plan.install_views.len());
        for (participant, install) in &installs {
            assert_eq!(
                install.core_view(),
                plan.install_views.get(participant).unwrap()
            );
            assert_eq!(
                install.routing_shard(),
                plan.routing_shards.get(participant).unwrap()
            );
        }
        let port = RecordingInstallPort::default();
        for (participant, install) in installs {
            port.install(QUERY, plan.epoch, participant, install)
                .unwrap();
        }
        assert!(port.all_installed(&BTreeSet::from([pid(10), pid(1)])));
        assert!(!port.all_installed(&plan.participants)); // pid(2) never installed
    }

    #[test]
    fn participant_installs_reject_missing_routing_shard() {
        let mut plan = sample_plan(7);
        plan.routing_shards.remove(&pid(10));

        let err = RuntimeFilterDeploymentExtension::new()
            .participant_installs(&plan)
            .unwrap_err();

        assert!(matches!(
            err,
            DeploymentInstallError::MissingRoutingShard { participant: 10 }
        ));
    }

    #[test]
    fn participant_installs_reject_epoch_or_participant_mismatch() {
        let mut epoch_mismatch = sample_plan(7);
        epoch_mismatch
            .routing_shards
            .insert(pid(10), shard(DeploymentEpoch::new(8), pid(10)));
        let err = RuntimeFilterDeploymentExtension::new()
            .participant_installs(&epoch_mismatch)
            .unwrap_err();
        assert!(matches!(
            err,
            DeploymentInstallError::EpochIdentityMismatch { .. }
        ));

        let mut participant_mismatch = sample_plan(7);
        participant_mismatch
            .routing_shards
            .insert(pid(10), shard(DeploymentEpoch::new(7), pid(1)));
        let err = RuntimeFilterDeploymentExtension::new()
            .participant_installs(&participant_mismatch)
            .unwrap_err();
        assert!(matches!(
            err,
            DeploymentInstallError::ParticipantIdentityMismatch { .. }
        ));
    }

    #[test]
    fn participant_installs_ignore_extra_routing_only_participant() {
        let mut plan = sample_plan_with_roleless_participant(7);
        plan.routing_shards
            .insert(pid(2), shard(plan.epoch, pid(2)));

        let installs = RuntimeFilterDeploymentExtension::new()
            .participant_installs(&plan)
            .unwrap();

        assert_eq!(installs.len(), 2);
        assert!(
            installs
                .iter()
                .all(|(participant, _)| *participant != pid(2))
        );
        assert!(plan.routing_shards.contains_key(&pid(2)));
    }

    #[test]
    fn participant_installs_reject_malformed_extra_routing_shard_epoch() {
        let mut plan = sample_plan_with_roleless_participant(7);
        plan.routing_shards.insert(
            pid(2),
            shard(DeploymentEpoch::new(plan.epoch.get() + 1), pid(2)),
        );

        let err = RuntimeFilterDeploymentExtension::new()
            .participant_installs(&plan)
            .unwrap_err();

        assert!(matches!(
            err,
            DeploymentInstallError::EpochIdentityMismatch { .. }
        ));
    }

    #[test]
    fn participant_installs_reject_malformed_extra_routing_shard_participant() {
        let mut plan = sample_plan_with_roleless_participant(7);
        plan.routing_shards
            .insert(pid(2), shard(plan.epoch, pid(3)));

        let err = RuntimeFilterDeploymentExtension::new()
            .participant_installs(&plan)
            .unwrap_err();

        assert!(matches!(
            err,
            DeploymentInstallError::ParticipantIdentityMismatch { .. }
        ));
    }

    #[test]
    fn participant_installs_reject_malformed_view_before_missing_shard() {
        let mut plan = sample_plan(7);
        plan.install_views.insert(
            pid(10),
            RuntimeFilterInstallView::new(plan.epoch, pid(9), BTreeMap::new()),
        );
        plan.routing_shards.remove(&pid(10));

        let err = RuntimeFilterDeploymentExtension::new()
            .participant_installs(&plan)
            .unwrap_err();

        assert!(matches!(
            err,
            DeploymentInstallError::ParticipantIdentityMismatch { .. }
        ));
    }

    #[test]
    fn recording_port_conflicts_when_only_routing_shard_changes() {
        let epoch = DeploymentEpoch::new(7);
        let participant = pid(10);
        let view = RuntimeFilterInstallView::new(epoch, participant, BTreeMap::new());
        let port = RecordingInstallPort::default();
        port.install(
            QUERY,
            epoch,
            participant,
            RuntimeFilterParticipantInstall::new(view.clone(), shard(epoch, participant)),
        )
        .unwrap();

        let err = port
            .install(
                QUERY,
                epoch,
                participant,
                RuntimeFilterParticipantInstall::new(view, changed_shard(epoch, participant)),
            )
            .unwrap_err();

        assert!(matches!(
            err,
            DeploymentInstallError::ConflictingDeployment { participant: 10 }
        ));
    }

    #[test]
    fn recording_port_is_idempotent_and_validates_outer_identity() {
        let plan = sample_plan(7);
        let port = RecordingInstallPort::default();
        let (participant, install) = RuntimeFilterDeploymentExtension::new()
            .participant_installs(&plan)
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        port.install(QUERY, plan.epoch, participant, install.clone())
            .unwrap();
        port.install(QUERY, plan.epoch, participant, install.clone())
            .unwrap();

        let err = port
            .install(
                QUERY,
                DeploymentEpoch::new(plan.epoch.get() + 1),
                participant,
                install.clone(),
            )
            .unwrap_err();
        assert!(matches!(
            err,
            DeploymentInstallError::EpochIdentityMismatch { .. }
        ));

        let next_epoch = DeploymentEpoch::new(plan.epoch.get() + 1);
        let next_install = RuntimeFilterParticipantInstall::new(
            RuntimeFilterInstallView::new(next_epoch, participant, BTreeMap::new()),
            shard(next_epoch, participant),
        );
        let err = port
            .install(QUERY, next_epoch, participant, next_install)
            .unwrap_err();
        assert!(matches!(err, DeploymentInstallError::EpochConflict { .. }));

        let err = port
            .install(QUERY, plan.epoch, pid(99), install)
            .unwrap_err();
        assert!(matches!(
            err,
            DeploymentInstallError::ParticipantIdentityMismatch { .. }
        ));
    }
}
