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

//! What a freshly opened process may assume about the writer before it.
//!
//! A process that rediscovers a materialized view from the lake knows which
//! incarnation wrote its documents, and knows that it is not that incarnation.
//! What it cannot know is whether that writer had an effect in flight when it
//! stopped: an unanswered catalog commit or object deletion leaves no trace in
//! the documents, and assuming there was none is the one assumption that can
//! corrupt the target.
//!
//! So management closes on exactly that possibility. The barrier stands for
//! everything the old incarnation could have dispatched, and only evidence
//! that the old writer can no longer act retires it. Reading the view is
//! unaffected: a published materialization is what the lake says it is.

use novarocks_mv_application::management::{
    EffectDisposition, EffectIdentity, EffectResponsibility, EffectScope, EffectTerminalFact,
    ManagedMvTarget, ManagementContinuation, ManagementEntrance, ManagementTimestamp,
    UnsettledEffect,
};
use novarocks_mv_application::persistence::keyed_documents::StoredKeyedMvProjection;
use novarocks_spi::connector::CatalogHandle;

/// Close management on one target this process rediscovered from the lake.
///
/// Returns whether a barrier was installed. A target this process itself
/// wrote, or one owned by another deployment, gets none: the first has nothing
/// to recover, and the second is not this deployment's to close.
pub(crate) fn close_recovered_target_management(
    entrance: &ManagementEntrance,
    catalog: CatalogHandle,
    projection: &StoredKeyedMvProjection,
) -> Result<bool, String> {
    let source = projection.facts.source_revision();
    if source.deployment_owner != *entrance.owner() {
        return Ok(false);
    }
    if source.process_incarnation == *entrance.incarnation() {
        return Ok(false);
    }
    entrance.note_startup_incarnation(&source.process_incarnation);
    let target = ManagedMvTarget::try_new(
        catalog,
        source.target.clone(),
        source.target_object_id.clone(),
    )
    .map_err(|error| format!("name the recovered MV target: {error:?}"))?;
    if !projection.facts.interpretation().is_aggregate() {
        entrance
            .recover_non_aggregate_target(target, source.process_incarnation.clone())
            .map_err(|error| {
                format!("continue recovered non-aggregate MV management: {error:?}")
            })?;
        return Ok(true);
    }
    let barrier = recovery_barrier(&target, &source.process_incarnation);
    let observation = entrance
        .begin_recovered_target(
            target,
            ManagementContinuation::SameOwner {
                previous_incarnation: source.process_incarnation.clone(),
            },
            barrier,
        )
        .map_err(|error| format!("close recovered MV management: {error:?}"))?;
    // The barrier is the point; the observation token that comes with it is
    // not. Holding one would block the readmission that eventually resolves
    // the barrier, so it is handed straight back while the barrier stays.
    entrance
        .abandon_observation(observation)
        .map_err(|error| format!("release the recovered MV observation token: {error:?}"))?;
    Ok(true)
}

/// One unresolved effect standing for everything the old incarnation could
/// have dispatched.
///
/// Its identity is fresh because it names a possibility, not a specific
/// attempt: nobody recorded what the old writer was doing, which is exactly
/// why this exists. Its scope is both paths, because an operator's statement
/// has to cover both before management can reopen.
fn recovery_barrier(
    target: &ManagedMvTarget,
    previous_incarnation: &novarocks_mv_application::management::ProcessIncarnation,
) -> UnsettledEffect {
    let responsibility = EffectResponsibility::new(
        EffectIdentity::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
        target.clone(),
        previous_incarnation.clone(),
        EffectScope::CATALOG_AND_OBJECT_DELETION,
        // The old writer's last possible dispatch is unknown, so the barrier
        // uses the earliest time that cannot understate it: any real dispatch
        // happened at or after the epoch, and a guaranteed-window resume
        // therefore measures from the isolation evidence instead.
        ManagementTimestamp::from_unix_millis(0),
    );
    match responsibility.record_terminal(EffectDisposition::CommitUnknown) {
        EffectTerminalFact::CommitUnknown(effect) => effect,
        _ => unreachable!("CommitUnknown produces an unsettled effect"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::test_admission::sample_keyed_projection as sample_projection;
    use novarocks_mv_application::management::{
        DeploymentOwner, MvManagementPhase, ProcessIncarnation,
    };
    use novarocks_mv_application::product::MvTarget;
    use novarocks_spi::connector::{CatalogVersion, ConnectorInstanceId};

    /// The fixture records `test-deployment` / `test-process` as the writer.
    fn projection() -> StoredKeyedMvProjection {
        StoredKeyedMvProjection {
            mv_id: 1,
            facts: sample_projection(MvTarget::from_parts(Some("ice"), "sales", "mv"), Some(1)),
        }
    }

    fn catalog() -> CatalogHandle {
        CatalogHandle::new(
            ConnectorInstanceId::parse("ice").expect("catalog"),
            CatalogVersion::from_bytes([1; 32]),
        )
    }

    fn entrance(owner: &str, incarnation: &str) -> ManagementEntrance {
        ManagementEntrance::new(
            DeploymentOwner::parse(owner).expect("owner"),
            ProcessIncarnation::parse(incarnation).expect("incarnation"),
        )
    }

    fn table(
        projection: &StoredKeyedMvProjection,
    ) -> novarocks_spi::connector::ConnectorTableIdentity {
        projection.facts.source_revision().target.clone()
    }

    #[test]
    fn a_view_written_by_an_earlier_incarnation_closes_management() {
        let projection = projection();
        let entrance = entrance("test-deployment", "this-process");

        assert!(
            close_recovered_target_management(&entrance, catalog(), &projection)
                .expect("the barrier installs")
        );

        assert_eq!(
            entrance.management_phase(&table(&projection)),
            MvManagementPhase::AwaitingEffectSettlement { unsettled: 1 },
        );
        let unsettled = entrance.unsettled_effects(&table(&projection));
        assert_eq!(
            unsettled[0]
                .responsibility()
                .dispatching_incarnation()
                .as_str(),
            "test-process",
            "the barrier stands for what the previous writer might have done"
        );
        assert!(
            unsettled[0]
                .responsibility()
                .scope()
                .covers(EffectScope::CATALOG_AND_OBJECT_DELETION),
            "an operator has to cover both paths before management reopens"
        );
    }

    #[test]
    fn a_rediscovered_non_aggregate_uses_barrier_free_same_owner_readmission() {
        let facts = novarocks_mv_application::persistence::test_support::union_all_keyed_projection_fixture(
            MvTarget::from_parts(Some("ice"), "sales", "mv"),
            Some(1), 2, 2, 1,
        ).build().unwrap();
        let projection = StoredKeyedMvProjection { mv_id: 1, facts };
        let entrance = entrance("test-deployment", "this-process");
        assert!(close_recovered_target_management(&entrance, catalog(), &projection).unwrap());
        let table = table(&projection);
        assert_eq!(
            entrance.management_phase(&table),
            MvManagementPhase::AwaitingObservation
        );
        assert!(entrance.unsettled_effects(&table).is_empty());
        let previous_incarnation = entrance.non_aggregate_reobservation(&table).unwrap();
        assert_eq!(previous_incarnation.as_str(), "test-process");
        let state = entrance
            .begin_readmission(
                &table,
                ManagementContinuation::SameOwner {
                    previous_incarnation,
                },
            )
            .unwrap();
        assert_eq!(state.phase(), novarocks_mv_application::management::ManagementObservationPhase::AwaitingFreshObservation);
    }

    #[test]
    fn a_view_this_process_wrote_has_nothing_to_recover() {
        let projection = projection();
        let entrance = entrance("test-deployment", "test-process");

        assert!(
            !close_recovered_target_management(&entrance, catalog(), &projection)
                .expect("no barrier is needed")
        );
        assert_eq!(
            entrance.management_phase(&table(&projection)),
            MvManagementPhase::NotObserved,
        );
    }

    #[test]
    fn another_deployments_view_is_not_this_ones_to_close() {
        let projection = projection();
        let entrance = entrance("other-deployment", "this-process");

        assert!(
            !close_recovered_target_management(&entrance, catalog(), &projection)
                .expect("a foreign owner is left alone")
        );
        assert_eq!(
            entrance.management_phase(&table(&projection)),
            MvManagementPhase::NotObserved,
        );
    }

    #[test]
    fn the_barrier_leaves_no_observation_token_blocking_its_own_readmission() {
        let projection = projection();
        let entrance = entrance("test-deployment", "this-process");
        close_recovered_target_management(&entrance, catalog(), &projection)
            .expect("the barrier installs");

        entrance
            .begin_readmission(
                &table(&projection),
                ManagementContinuation::SameOwner {
                    previous_incarnation: ProcessIncarnation::parse("test-process")
                        .expect("incarnation"),
                },
            )
            .expect("the readmission that resolves the barrier can begin");
    }
}
