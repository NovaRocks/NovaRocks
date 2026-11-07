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

//! The native backend participation side of execution.
//!
//! One establish installs four shared facts on this backend — query options,
//! catalog runtimes, a runtime-filter participant, and the query's vended
//! credentials — and establishes their native execution resources under the
//! same query options. One release takes the shared facts back and retires idle
//! resources; live fragments and independent type holders retain their own
//! actual responsibilities. This is the only place that owns that
//! pairing, and it owns nothing else: the owner in [`super::registry`] decides
//! *whether* a participation establishes, advances, or retires, and this host only
//! carries out the decision it was handed.
//!
//! # Materialization, stop and release
//!
//! An instance's materialization borrows its original Participation Scope stop view.
//! Abort closes that view before the original M call has exited. Cold provider
//! binding checks it independently of physical release, which still waits for
//! the same M and its captures to retire. The legacy host entrypoint retains its
//! existing release-race rollback. Every installed fact is published into the
//! per-participation record, so either rollback or release takes its actual custody
//! exactly once. Observing stop does not attest release or resource exit.
//!
//! # Lock discipline
//!
//! The per-participation lock is never held across a call into the catalog manager,
//! the runtime-filter participant, or the credential slot. Each of those has
//! its own lock, and the catalog manager calls back into this host through the
//! `active` predicate of an in-flight install, so holding both would invert the
//! order and deadlock.

use novarocks_control_runtime::exit_custody::ExitObligation;
use novarocks_execution::runtime::query_context::QueryContext;
use novarocks_worker::{InstanceWorkIssuer, ParticipationInstanceGrant, RuntimeFilterSenderWork};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::mem::ManuallyDrop;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_execution_contract::task_execution::domain::ContentFingerprint;
use novarocks_execution_contract::task_execution::identity::{ParticipationRef, TaskIdentity};
use novarocks_execution_contract::task_execution::operation::ParticipationDomainUpdate;
use novarocks_execution_contract::task_execution::status::TaskFailureCategory;
use novarocks_proto_codec::lifecycle::QueryTerminationReason;
use novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry;
use novarocks_proto_models::novarocks as proto;
use novarocks_spi::connector::{
    CatalogProperties, ConnectorExecutionRoleBinding, ConnectorStorageResolver,
};
use novarocks_task_codec::domain::{WireCredential, stored_message};
use novarocks_types::QueryExecutionId;
use tracing::error;

use super::execution_host::EstablishedQueryOptions;
use crate::runtime_filter_ingress::BackendRuntimeFilterParticipantAuthority;
use crate::{
    BackendDataRuntime,
    runtime_filter_feedback::TaskRuntimeFilterFeedbackEgress,
    runtime_filter_install::{
        DecodedRuntimeFilterContribution, decode_runtime_filter_contribution_with_type_creator,
    },
    runtime_filter_participant::RuntimeFilterParticipant,
    runtime_filter_terminal::{
        RUNTIME_FILTER_TERMINAL_CAPTURE_STAGE, capture_runtime_filter_terminal_profile_contribution,
    },
    task_shared_facts::{
        catalog_bindings, credential_material, query_options, runtime_filter_install,
        sealed_runtime_filter_evidence,
    },
};
use novarocks_worker::RuntimeFilterContractError;
use novarocks_worker::runtime_filter::domain::BackendFrontendFeedbackSink;
use novarocks_worker::runtime_filter::domain::BackendParticipantIdentity;
use novarocks_worker::{
    CatalogManager, CatalogManagerError, CatalogPruneResult,
    ConnectorExecutionRoleBindingFactorySet, HostRejection, ParticipationCredentialSlot,
    ParticipationHost, ParticipationHostFactory, ParticipationHostInstance,
    ParticipationMaterializationControl, ReleasedParticipationEvidence, SharedFactsRequest,
};

type RuntimeFilterParticipantInstaller = dyn Fn(
        QueryExecutionId,
        DecodedRuntimeFilterContribution,
        RuntimeFilterSenderWork,
    ) -> Result<Arc<RuntimeFilterParticipant>, RuntimeFilterContractError>
    + Send
    + Sync;

/// The mutable half of one participation's installed facts.
///
/// The credential slot is deliberately *not* in here: it owns its own lock and
/// is replaced atomically by a rotation, so putting it behind this mutex would
/// only serialise rotations against catalog installs for no reason.
#[derive(Default)]
struct ParticipationFacts {
    /// Set by whichever side retires the participation first. An in-flight
    /// `materialize` reads it as its only cancellation signal — it has no
    /// deadline of its own, because the owner's `SharedFactsRequest` carries
    /// none.
    released: bool,
    /// The immutable query contract every task must match before preparation.
    query_options: Option<EstablishedQueryOptions>,
    /// `None` either because the query installs no runtime filter on this
    /// backend, or because the side that tore the participation down already took it.
    participant: Option<Arc<RuntimeFilterParticipant>>,
    /// The one task carrying this participation's terminal filter feedback to the
    /// frontend, and the only strong reference to that sink.
    ///
    /// The participant holds it weakly, so this is what keeps it alive. Taking
    /// it at tear-down is what makes a released participation stop publishing rather
    /// than keep a status reporter reachable from a background publisher.
    feedback: Option<Arc<TaskRuntimeFilterFeedbackEgress>>,
}

/// Everything one participation installed on this backend.
struct InstalledParticipation {
    instance_owned: bool,
    credentials: Arc<ParticipationCredentialSlot>,
    facts: Mutex<ParticipationFacts>,
    query_context: Mutex<Option<Arc<QueryContext>>>,
}

/// Observe the same release decision as in-flight shared-fact installation.
/// Only the decode frame borrows this participation; returned type storage does not.
struct ParticipationTypeConstructionControl<'a>(&'a InstalledParticipation);
impl novarocks_type_contract::PureCompileControl for ParticipationTypeConstructionControl<'_> {
    fn checkpoint(
        &self,
        _: novarocks_type_contract::CompilePhase,
        _: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        if self.0.is_released() {
            Err(novarocks_type_contract::CompileControlError::Cancelled)
        } else {
            Ok(())
        }
    }
}

impl InstalledParticipation {
    fn establishing() -> Self {
        Self {
            instance_owned: false,
            credentials: Arc::new(ParticipationCredentialSlot::new()),
            facts: Mutex::new(ParticipationFacts::default()),
            query_context: Mutex::new(None),
        }
    }

    fn instance_owned() -> Self {
        Self {
            instance_owned: true,
            ..Self::establishing()
        }
    }

    /// A record for a participation the owner released before it was materialized.
    ///
    /// See [`NativeParticipationHost::release`] for why this exists.
    fn released() -> Self {
        Self {
            instance_owned: false,
            credentials: Arc::new(ParticipationCredentialSlot::new()),
            query_context: Mutex::new(None),
            facts: Mutex::new(ParticipationFacts {
                released: true,
                query_options: None,
                participant: None,
                feedback: None,
            }),
        }
    }

    fn is_released(&self) -> bool {
        self.facts
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .released
    }
}

/// Every participation this host has installed facts for, under both addresses.
///
/// A participation is named by its full reference, but a running task can only
/// name the query execution it belongs to. The owner fences one live participation
/// reference per execution id, so the second index is exact rather than a
/// heuristic — and keeping it here means a task-side lookup is one map probe
/// instead of a scan over every live query.
#[derive(Default)]
struct HostParticipations {
    by_reference: BTreeMap<ParticipationRef, Arc<InstalledParticipation>>,
    by_execution: BTreeMap<QueryExecutionId, ParticipationRef>,
}

impl HostParticipations {
    fn insert(&mut self, participation: ParticipationRef, installed: Arc<InstalledParticipation>) {
        self.by_reference.insert(participation, installed);
        self.by_execution
            .insert(participation.query_execution_id(), participation);
    }

    /// Records that a participation was released before it was ever materialized.
    ///
    /// It is deliberately not put in the execution index: a task must never
    /// resolve a participation that carries no facts and never will.
    fn insert_released_marker(&mut self, participation: ParticipationRef) {
        self.by_reference
            .insert(participation, Arc::new(InstalledParticipation::released()));
    }

    fn remove(&mut self, participation: ParticipationRef) -> Option<Arc<InstalledParticipation>> {
        let installed = self.by_reference.remove(&participation);
        // Only if this exact reference still owns the execution id: a later
        // participation must not lose its index because an older one was reclaimed.
        if self.by_execution.get(&participation.query_execution_id()) == Some(&participation) {
            self.by_execution
                .remove(&participation.query_execution_id());
        }
        installed
    }

    /// Removes a record only while it is still the one the caller installed.
    ///
    /// An unwinding `materialize` must not evict whatever took its place.
    fn remove_if_same(
        &mut self,
        participation: ParticipationRef,
        installed: &Arc<InstalledParticipation>,
    ) {
        if self
            .by_reference
            .get(&participation)
            .is_some_and(|current| Arc::ptr_eq(current, installed))
        {
            self.remove(participation);
        }
    }

    fn get(&self, participation: ParticipationRef) -> Option<&Arc<InstalledParticipation>> {
        self.by_reference.get(&participation)
    }
}

/// The backend's real [`ParticipationHost`].
pub struct NativeParticipationHost {
    queries: crate::native_query_context::NativeQueryContextRuntime,
    catalog_manager: Arc<CatalogManager<ConnectorExecutionRoleBinding>>,
    execution_role_binding_factories: Arc<ConnectorExecutionRoleBindingFactorySet>,
    runtime_filter_installer: Arc<RuntimeFilterParticipantInstaller>,
    catalog_install_runtime: BackendDataRuntime,
    participations: Mutex<HostParticipations>,
    catalog_metrics_publish: Mutex<()>,
    #[cfg(novarocks_dev_controls)]
    test_controls: Option<Arc<crate::test_controls::NativeTestControls>>,
    #[cfg(novarocks_dev_controls)]
    observations: Option<Arc<crate::test_observation_controls::NativeObservationControls>>,
}

impl BackendRuntimeFilterParticipantAuthority for NativeParticipationHost {
    fn authority_name(&self) -> &'static str {
        "the task participation host"
    }

    fn claim_participant(
        &self,
        participant: BackendParticipantIdentity,
    ) -> Option<Arc<RuntimeFilterParticipant>> {
        self.claim_runtime_filter_participant(participant)
    }
}

impl fmt::Debug for NativeParticipationHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Only the count. Naming a participation's credential slot here would put a
        // secret one derived `Debug` away, and the slot redacts itself anyway.
        let participations = self
            .participations
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        formatter
            .debug_struct("NativeParticipationHost")
            .field(
                "installed_participations",
                &participations.by_reference.len(),
            )
            .field("material", &"[REDACTED]")
            .finish()
    }
}

/// Snapshot sampled from the original Native instance records.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NativeQueryExecutionResourceSnapshot {
    pub active_contexts: usize,
    pub active_fragments: usize,
}

impl NativeParticipationHost {
    pub fn query_resource_snapshot(&self) -> NativeQueryExecutionResourceSnapshot {
        let records = self
            .participations
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut snapshot = NativeQueryExecutionResourceSnapshot::default();
        for installed in records.by_reference.values() {
            let context = installed
                .query_context
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(context) = context.as_ref() {
                snapshot.active_contexts += 1;
                snapshot.active_fragments += context.active_tasks();
            }
        }
        snapshot
    }

    /// Crate-private because only this process's composition can install a
    /// Native runtime-filter participant around a Worker participant.
    pub(crate) fn new(
        queries: crate::native_query_context::NativeQueryContextRuntime,
        catalog_manager: Arc<CatalogManager<ConnectorExecutionRoleBinding>>,
        execution_role_binding_factories: Arc<ConnectorExecutionRoleBindingFactorySet>,
        runtime_filter_installer: Arc<RuntimeFilterParticipantInstaller>,
        catalog_install_runtime: BackendDataRuntime,
    ) -> Self {
        Self {
            queries,
            catalog_manager,
            execution_role_binding_factories,
            runtime_filter_installer,
            catalog_install_runtime,
            participations: Mutex::new(HostParticipations::default()),
            catalog_metrics_publish: Mutex::new(()),
            #[cfg(novarocks_dev_controls)]
            test_controls: None,
            #[cfg(novarocks_dev_controls)]
            observations: None,
        }
    }

    #[cfg(novarocks_dev_controls)]
    pub(crate) fn with_test_controls(
        mut self,
        controls: Option<Arc<crate::test_controls::NativeTestControls>>,
    ) -> Self {
        self.test_controls = controls;
        self
    }
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn with_observation_controls(
        mut self,
        controls: Option<Arc<crate::test_observation_controls::NativeObservationControls>>,
    ) -> Self {
        self.observations = controls;
        self
    }

    /// The vended-credential authority of one live participation.
    ///
    /// This is the read side of the credential slot: a connector reaching for
    /// scoped storage access resolves it through here, and a participation that was
    /// never established or has already been released has none — there is no
    /// process-level credential to fall back to.
    /// The resolver one established participation installed.
    ///
    /// Named apart from the `TaskParticipationFacts` method of the same shape:
    /// that one is keyed by execution id and refuses when there is no participation,
    /// and two same-named methods keyed differently is how a caller reaches
    /// the wrong one.
    pub fn storage_resolver_for_participation(
        &self,
        participation: ParticipationRef,
    ) -> Option<Arc<dyn ConnectorStorageResolver>> {
        let installed = self
            .participations
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(participation)
            .map(Arc::clone)?;
        if installed.is_released() {
            return None;
        }
        Some(Arc::clone(&installed.credentials) as Arc<dyn ConnectorStorageResolver>)
    }

    /// The runtime-filter participant installed for the exact query execution.
    ///
    /// `None` covers both "no filter on this backend" and "the participation is
    /// gone"; the caller decides which of those is an error, because only it
    /// knows whether its plan binds a filter.
    fn participant_for_execution(
        &self,
        execution: QueryExecutionId,
    ) -> Option<Arc<RuntimeFilterParticipant>> {
        let participation = self.participation_for_execution(execution)?;
        let participations = self
            .participations
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let installed = participations.get(participation)?;
        let facts = installed
            .facts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        facts.participant.clone()
    }

    #[cfg(novarocks_dev_controls)]
    fn wait_for_accepted_contribution_retry_rendezvous(&self, participation: ParticipationRef) {
        let participant = {
            let participations = self
                .participations
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let Some(installed) = participations.get(participation) else {
                return;
            };
            let facts = installed
                .facts
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            facts.participant.clone()
        };
        if let Some(participant) = participant {
            participant.wait_for_accepted_contribution_retry_rendezvous();
        }
    }

    /// The participant this host installed for the exact attempt.
    ///
    /// This is the read side a peer backend reaches: a runtime-filter envelope
    /// carries a wire participant identity, not an execution id, so the
    /// attempt has to be recovered from the query id and deployment epoch it
    /// does carry. Ownership only — accept, duplicate and reject stay the
    /// participant's own verdict.
    ///
    /// Without this, every cross-backend runtime-filter envelope of a
    /// task-protocol query is refused at the peer: contributions never reach
    /// their aggregator, artifacts never reach their consumers, and each
    /// consumer waits out its whole wait cap before scanning unfiltered.
    pub(crate) fn claim_runtime_filter_participant(
        &self,
        participant: novarocks_worker::runtime_filter::domain::BackendParticipantIdentity,
    ) -> Option<Arc<RuntimeFilterParticipant>> {
        let execution = {
            let participations = self
                .participations
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            participations
                .by_execution
                .keys()
                .copied()
                .find(|execution| {
                    execution.query_id().high() == participant.query_id().high()
                        && execution.query_id().low() == participant.query_id().low()
                        && execution.attempt_id().get() == participant.deployment_epoch()
                })?
        };
        self.participant_for_execution(execution)
    }

    /// Which task carries this participation's dynamic filter feedback.
    ///
    /// This is the supply observable of the backend half of that loop: a sink
    /// that is built and never installed answers `None` here, and no test of
    /// the sink itself can see that.
    pub fn feedback_carrier(&self, execution: QueryExecutionId) -> Option<TaskIdentity> {
        let participation = self.participation_for_execution(execution)?;
        let participations = self
            .participations
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let installed = participations.get(participation)?;
        let facts = installed
            .facts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        facts.feedback.as_ref().map(|sink| sink.as_ref().carrier())
    }

    pub fn participation_for_execution(
        &self,
        execution: QueryExecutionId,
    ) -> Option<ParticipationRef> {
        let participations = self
            .participations
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let participation = *participations.by_execution.get(&execution)?;
        // A capability-owned released row still fences successor allocation,
        // but it is no longer a task-visible installed Participation.
        (!participations.get(participation)?.is_released()).then_some(participation)
    }

    /// Observe the exact participation's facts without holding the registry lock.
    /// Absence alone cannot distinguish a completed release from an admission
    /// disagreement. A retained Arc can also observe release after lookup;
    /// its released flag is direct stop evidence, not an invariant failure.
    fn active_participation(
        &self,
        participation: ParticipationRef,
    ) -> Result<Arc<InstalledParticipation>, HostRejection> {
        let installed = self
            .participations
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(participation)
            .map(Arc::clone)
            .ok_or_else(|| {
                participation_facts_rejection(ParticipationFactsFailure::Unavailable(participation))
            })?;
        if installed.is_released() {
            return Err(participation_facts_rejection(
                ParticipationFactsFailure::Released,
            ));
        }
        Ok(installed)
    }

    /// Undoes every installed fact of one participation, exactly once per fact.
    ///
    /// Ordering is deliberate: the credential slot is emptied first so the
    /// window in which secret material is resident is as short as this host can
    /// make it, then the participant is closed, then the catalog leases are
    /// dropped. None of them depend on each other, and by the time a release
    /// runs the owner has already stood every task down.
    fn tear_down(
        &self,
        participation: ParticipationRef,
        installed: &InstalledParticipation,
    ) -> ReleasedParticipationEvidence {
        installed.credentials.close();
        let participant = {
            let mut facts = installed
                .facts
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            facts.released = true;
            facts.query_options = None;
            // Dropped with the participant: it is the only strong reference to
            // the feedback sink, so releasing the participation is what makes a late
            // publication find nothing to publish through.
            facts.feedback = None;
            facts.participant.take()
        };
        let evidence = match participant {
            Some(participant) => {
                let evidence = seal_runtime_filter_evidence(participation, &participant);
                close_participant(participation, &participant);
                evidence
            }
            None => ReleasedParticipationEvidence::none(),
        };
        // Unconditional: a lease may have been taken by an install that is
        // still unwinding, and releasing a query that holds none is a no-op.
        self.catalog_manager
            .release_query(participation.query_execution_id());
        let context = installed
            .query_context
            .lock()
            .expect("original Participation query resources")
            .take();
        if let Some(context) = &context {
            assert_eq!(
                context.active_tasks(),
                0,
                "Release follows actual Task resource retirement"
            );
        }
        drop(context);
        self.publish_catalog_lease_metrics();
        evidence
    }

    /// Publishes this process's catalog lease counts.
    ///
    /// This host is the only owner that takes and releases catalog leases, so
    /// it is the only place that can say what the gauge should read. It is
    /// called after each event that changes the count rather than on a timer:
    /// the manager holds the numbers and nothing else observes them.
    fn publish_catalog_lease_metrics(&self) {
        // Install and release can finish on different threads. Keep the
        // manager snapshot and both gauge writes in one publication order,
        // otherwise an older snapshot may overwrite a later release's zero.
        let _publication = self
            .catalog_metrics_publish
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let snapshot = self.catalog_manager.pin_snapshot();
        novarocks_native_adapter::backend_metrics::publish_backend_query_execution_resource(
            "catalog_query_pins",
            snapshot.query_pins,
        );
        novarocks_native_adapter::backend_metrics::publish_backend_query_execution_resource(
            "catalog_handle_pins",
            snapshot.handle_pins,
        );
    }

    /// Refuses to keep installing into a participation that was retired under us.
    fn still_establishing(&self, installed: &InstalledParticipation) -> Result<(), HostRejection> {
        if installed.is_released() {
            return Err(cancelled_establish());
        }
        Ok(())
    }

    fn install_shared_facts(
        &self,
        participation: ParticipationRef,
        installed: &InstalledParticipation,
        issuer: &InstanceWorkIssuer,
        catalogs: Vec<CatalogProperties>,
        contribution: &proto::RuntimeFilterContribution,
        options: QueryOptions,
        options_fingerprint: ContentFingerprint,
        material: &WireCredential,
        stopped: &dyn Fn() -> bool,
    ) -> Result<(), HostRejection> {
        if stopped() {
            return Err(cancelled_establish());
        }
        let execution_id = participation.query_execution_id();

        // Credentials first: a catalog runtime is the thing most likely to need
        // scoped storage access, so the authority has to exist before the
        // binding that may reach for it.
        installed.credentials.install(material.leases())?;
        self.still_establishing(installed)?;

        let context = self
            .queries
            .establish_query_resources(execution_id, options.exec_mem_limit())
            .map_err(|error| {
                HostRejection::from_domain(super::execution_host::failure_category(&error), error)
            })?;
        {
            let facts = installed
                .facts
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if facts.released {
                return Err(cancelled_establish());
            }
            let mut resources = installed
                .query_context
                .lock()
                .expect("original Participation query resources");
            assert!(
                resources.is_none(),
                "one original Participation QueryContext"
            );
            *resources = Some(context);
        }
        self.still_establishing(installed)?;

        {
            let mut facts = installed
                .facts
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if facts.released {
                return Err(cancelled_establish());
            }
            facts.query_options = Some(EstablishedQueryOptions::new(
                Arc::new(options),
                options_fingerprint,
            ));
        }

        if let Some(decoded) = self.decode_contribution(participation, installed, contribution)? {
            let sender = issuer
                .runtime_filter_sender()
                .map_err(participation_work_rejection)?;
            let participant = (self.runtime_filter_installer)(execution_id, decoded, sender)
                .map_err(runtime_filter_rejection)?;
            self.adopt_participant(participation, installed, participant)?;
        }

        self.install_catalogs(execution_id, catalogs, installed, stopped)?;

        // The last word. Every install above is published in the record by the
        // time this runs, so a release that arrives from here on finds all of
        // them, and one that already ran is seen here and unwound by the caller.
        if stopped() {
            return Err(cancelled_establish());
        }
        self.still_establishing(installed)
    }

    fn decode_contribution(
        &self,
        participation: ParticipationRef,
        installed: &InstalledParticipation,
        contribution: &proto::RuntimeFilterContribution,
    ) -> Result<Option<DecodedRuntimeFilterContribution>, HostRejection> {
        if contribution.participant_id == 0 {
            if contribution.lifecycle.is_some() || contribution.install.is_some() {
                return Err(participation_facts_rejection(
                    ParticipationFactsFailure::InstallWithoutParticipant,
                ));
            }
            return Ok(None);
        }
        let control = ParticipationTypeConstructionControl(installed);
        novarocks_type_contract::PureCompileControl::checkpoint(
            &control,
            novarocks_type_contract::CompilePhase::LowerProgram,
            0,
        )
        .map_err(|_| cancelled_establish())?;
        // The original query options already installed this policy while the
        // participation established. This is only an exact-owner lookup.
        let context = installed
            .query_context
            .lock()
            .expect("original query resources")
            .as_ref()
            .cloned()
            .ok_or_else(|| {
                participation_facts_rejection(ParticipationFactsFailure::Unavailable(participation))
            })?;
        let tracker = self
            .queries
            .type_construction_tracker(context.as_ref(), None)
            .map_err(|error| {
                if installed.is_released() {
                    cancelled_establish()
                } else {
                    HostRejection::from_domain(
                        super::execution_host::failure_category(&error),
                        error,
                    )
                }
            })?;
        let creator = crate::native_type_construction::NativeTypeConstruction::try_with_control(
            tracker, &control,
        )
        .map_err(|cause| participation_type_construction_rejection(&cause))?;
        decoded_contribution(participation.query_execution_id(), contribution, &creator)
    }

    /// Publishes a freshly installed participant, or closes it if it lost.
    fn adopt_participant(
        &self,
        participation: ParticipationRef,
        installed: &InstalledParticipation,
        participant: Arc<RuntimeFilterParticipant>,
    ) -> Result<(), HostRejection> {
        let mut facts = installed
            .facts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if facts.released {
            drop(facts);
            // A concurrent release already took the record's participant slot
            // (it was empty), so closing this one is ours to do and cannot
            // double-close.
            close_participant(participation, &participant);
            return Err(cancelled_establish());
        }
        if facts.participant.is_some() {
            drop(facts);
            close_participant(participation, &participant);
            return Err(sealed_participant());
        }
        facts.participant = Some(participant);
        Ok(())
    }

    fn install_catalogs(
        &self,
        execution_id: QueryExecutionId,
        catalogs: Vec<CatalogProperties>,
        installed: &InstalledParticipation,
        stopped: &dyn Fn() -> bool,
    ) -> Result<(), HostRejection> {
        if stopped() {
            return Err(cancelled_establish());
        }
        if catalogs.is_empty() {
            return Ok(());
        }
        // Both branches below take leases, and the loop can take some and
        // then refuse, so the gauge is republished on the way out of every
        // one of them rather than only on success.
        let _publish_on_exit = RepublishCatalogGaugesOnExit(self);
        // The old stack ran every provider bind on this runtime's blocking
        // pool, which also gave the bind an ambient Tokio context. This host is
        // called on a thread the owner chose, so it enters the injected runtime
        // explicitly instead: the guarantee a provider depends on then comes
        // from the runtime it was composed with rather than from whoever
        // happened to call `materialize`.
        let _runtime_context = self.catalog_install_runtime.handle().enter();

        // The whole-set fast path: when every exact runtime is already Ready
        // this leases them in one move and touches no provider. A conflict is
        // propagated rather than demoted to "not ready" — two queries claiming
        // the same catalog handle with different definitions is a real refusal,
        // and retrying it one catalog at a time only reaches the same error
        // more slowly.
        if self
            .catalog_manager
            .try_acquire_ready_catalogs(execution_id, &catalogs)
            .map_err(|error| catalog_rejection(installed, error))?
        {
            return Ok(());
        }

        // Past this point at least one catalog runtime has to be built, so
        // this establish is a cold install. That is the only state the
        // runner-owned rendezvous below is about, and it is the state the
        // marker names.
        #[cfg(novarocks_dev_controls)]
        if let Some(controls) = self.observations.as_deref() {
            if let Some(observer) = controls
                .observation_for(
                    novarocks_failpoint::observation_marker::ObservationAction::CatalogLifecycle
                        .as_str(),
                    execution_id,
                )
                .map_err(|error| {
                    catalog_rejection(
                        installed,
                        CatalogManagerError::materialization_failed(error),
                    )
                })?
            {
                emit_catalog_install_started(observer.as_ref(), execution_id, catalogs.len());
            }
            controls
                .catalog_hold(execution_id, || stopped() || installed.is_released())
                .map_err(|error| {
                    catalog_rejection(
                        installed,
                        CatalogManagerError::materialization_failed(error),
                    )
                })?;
            if controls.catalog_failure(execution_id).map_err(|error| {
                catalog_rejection(
                    installed,
                    CatalogManagerError::materialization_failed(error),
                )
            })? {
                if let Some(observer) = controls
                    .observation_for(novarocks_failpoint::observation_marker::ObservationAction::CatalogLifecycle.as_str(), execution_id)
                    .map_err(|error| {
                        catalog_rejection(
                            installed,
                            CatalogManagerError::materialization_failed(error),
                        )
                    })?
                {
                    observer.observe(&format!(
                        "{} execution_id={}:{}:{}", novarocks_failpoint::observation_marker::CATALOG_INSTALL_FAILED,
                        execution_id.query_id().high(),
                        execution_id.query_id().low(),
                        execution_id.attempt_id().get()
                    ));
                }
                return Err(catalog_rejection(
                    installed,
                    CatalogManagerError::materialization_failed(
                        "runner-injected catalog install failure",
                    ),
                ));
            }
        }

        for properties in catalogs {
            let factories = Arc::clone(&self.execution_role_binding_factories);
            #[cfg(novarocks_dev_controls)]
            let observation = self
                .observations
                .as_deref()
                .map(|controls| controls.observation_for(novarocks_failpoint::observation_marker::ObservationAction::CatalogMaterialization.as_str(), execution_id))
                .transpose()
                .map_err(|error| {
                    catalog_rejection(
                        installed,
                        CatalogManagerError::materialization_failed(error),
                    )
                })?
                .flatten();
            self.catalog_manager
                .ensure_while(
                    execution_id,
                    properties,
                    || !stopped() && !installed.is_released(),
                    move |properties| {
                        factories
                            .bind(
                                properties,
                                #[cfg(novarocks_dev_controls)]
                                observation.clone(),
                            )
                            .map_err(CatalogManagerError::from_materialization)
                    },
                )
                .map_err(|error| catalog_rejection(installed, error))?;
        }
        Ok(())
    }
}

impl NativeParticipationHost {
    /// Takes a released marker without installing anything.
    ///
    /// A marker exists only between a release that arrived before its
    /// establish and the materialize that answers it. Every exit from
    /// `materialize` has to take it, or the marker outlives the attempt.
    fn discard_released_marker(&self, participation: ParticipationRef) {
        let mut participations = self
            .participations
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if participations
            .get(participation)
            .is_some_and(|installed| !installed.instance_owned && installed.is_released())
        {
            participations.remove(participation);
        }
    }
}

/// The one admitted Participation incarnation. Directory retention follows this
/// capability, including every actual in-flight call clone held by its owner.
struct NativeParticipationInstance {
    host: ManuallyDrop<Arc<NativeParticipationHost>>,
    participation: ParticipationRef,
    installed: ManuallyDrop<Arc<InstalledParticipation>>,
    issuer: ManuallyDrop<InstanceWorkIssuer>,
    work: Option<ExitObligation>,
    materialization_started: AtomicBool,
    release_started: AtomicBool,
    release_completed: AtomicBool,
}

impl ParticipationHostFactory for NativeParticipationHost {
    fn create_instance(
        self: Arc<Self>,
        participation: ParticipationRef,
        grant: ParticipationInstanceGrant,
    ) -> Result<Arc<dyn ParticipationHostInstance>, HostRejection> {
        if grant.participation() != participation {
            return Err(participation_facts_rejection(
                ParticipationFactsFailure::InstanceCapabilityRequired,
            ));
        }
        let (installed, work, issuer) = {
            let mut participations = self
                .participations
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if participations.get(participation).is_some() {
                return Err(participation_facts_rejection(
                    ParticipationFactsFailure::AlreadyMaterialized,
                ));
            }
            if participations
                .by_execution
                .contains_key(&participation.query_execution_id())
            {
                return Err(participation_facts_rejection(
                    ParticipationFactsFailure::ExecutionAlreadyInstalled(
                        participation.query_execution_id(),
                    ),
                ));
            }
            let (mut work, issuer) = grant.into_parts();
            if let Err(error) = work.start() {
                work.revoke_unused_and_exit()
                    .expect("refused original instance was never started");
                return Err(participation_work_rejection(
                    novarocks_worker::ParticipationWorkError::Exit(error),
                ));
            }
            let installed = Arc::new(InstalledParticipation::instance_owned());
            participations.insert(participation, Arc::clone(&installed));
            (installed, work, issuer)
        };
        Ok(Arc::new(NativeParticipationInstance {
            host: ManuallyDrop::new(self),
            participation,
            installed: ManuallyDrop::new(installed),
            issuer: ManuallyDrop::new(issuer),
            work: Some(work),
            materialization_started: AtomicBool::new(false),
            release_started: AtomicBool::new(false),
            release_completed: AtomicBool::new(false),
        }))
    }
}

impl ParticipationHostInstance for NativeParticipationInstance {
    fn participation(&self) -> ParticipationRef {
        self.participation
    }

    fn materialize(
        &self,
        request: SharedFactsRequest<'_>,
        control: &dyn ParticipationMaterializationControl,
    ) -> Result<(), HostRejection> {
        if request.participation() != self.participation {
            return Err(participation_facts_rejection(
                ParticipationFactsFailure::WrongInstanceParticipation {
                    expected: self.participation,
                    actual: request.participation(),
                },
            ));
        }
        self.host.still_establishing(&self.installed)?;
        if self.materialization_started.swap(true, Ordering::AcqRel) {
            return Err(participation_facts_rejection(
                ParticipationFactsFailure::AlreadyMaterialized,
            ));
        }
        let result = (|| {
            let options_fingerprint = request.query_options().fingerprint();
            let catalogs = catalog_bindings(request.catalog_binding().as_ref())?;
            let contribution = runtime_filter_install(request.initial_runtime_filter().as_ref())?;
            let options = query_options(request.query_options().as_ref())?;
            let material = credential_material(request.initial_credential())?;
            self.host.still_establishing(&self.installed)?;
            self.host.install_shared_facts(
                self.participation,
                &self.installed,
                &self.issuer,
                catalogs,
                contribution,
                options,
                options_fingerprint,
                material,
                &|| control.is_stopped(),
            )
        })();
        if result.is_err() {
            // R may already have run while M was installing. Repeat only this
            // same instance's rollback; no successor can occupy its directory
            // until the last actual capability owner has exited.
            drop(self.host.tear_down(self.participation, &self.installed));
        }
        result
    }

    fn release(&self) -> ReleasedParticipationEvidence {
        if self.release_started.swap(true, Ordering::AcqRel) {
            if !self.release_completed.load(Ordering::Acquire) {
                // Concurrent re-entry or a panicking first release has no
                // positive exit evidence. This API cannot report Unknown.
                std::process::abort();
            }
            return ReleasedParticipationEvidence::none();
        }
        let evidence = self.host.tear_down(self.participation, &self.installed);
        self.release_completed.store(true, Ordering::Release);
        evidence
    }

    fn advance_shared_domain(
        &self,
        domain: &ParticipationDomainUpdate,
    ) -> Result<(), HostRejection> {
        self.host
            .apply_shared_domain(self.participation, &self.installed, &self.issuer, domain)
    }
}

impl Drop for NativeParticipationInstance {
    fn drop(&mut self) {
        let released = self.release_completed.load(Ordering::Acquire);
        if released {
            self.host
                .participations
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .remove_if_same(self.participation, &self.installed);
        }
        let work = self
            .work
            .take()
            .expect("one original instance exit obligation");
        // SAFETY: these fields are initialized once and destroyed only here.
        // ManuallyDrop prevents a second automatic Drop after the exit fact.
        unsafe {
            ManuallyDrop::drop(&mut self.installed);
            ManuallyDrop::drop(&mut self.issuer);
            ManuallyDrop::drop(&mut self.host);
        }
        if released {
            work.exited()
                .expect("original instance exits after its real resources");
        } else {
            // A lost unreleased instance is a real defect. Its retained row
            // and original custody cannot be turned into successful teardown.
            drop(work);
        }
    }
}

/// Republishes the catalog lease gauge when a lease-taking scope exits.
///
/// A cold install can take some leases and then refuse, so the gauge has to
/// be republished on the refusal path too; a guard states that once instead
/// of at every `?`.
struct RepublishCatalogGaugesOnExit<'host>(&'host NativeParticipationHost);

impl Drop for RepublishCatalogGaugesOnExit<'_> {
    fn drop(&mut self) {
        self.0.publish_catalog_lease_metrics();
    }
}

/// This host reconciles catalog reachability because it is this process's only
/// catalog lease owner: it takes the leases on establish and drops them on
/// release, so it is the only owner that can decide what is still needed.
impl novarocks_native_adapter::catalog_prune_rpc::CatalogReachabilityAuthority
    for NativeParticipationHost
{
    fn prune_unreachable_catalogs(
        &self,
        reachable: std::collections::BTreeSet<novarocks_spi::connector::CatalogHandle>,
    ) -> CatalogPruneResult {
        let result = self.catalog_manager.prune_unreachable(&reachable);
        self.publish_catalog_lease_metrics();
        result
    }
}

#[cfg(any(test, novarocks_dev_controls))]
impl ParticipationHost for NativeParticipationHost {
    fn materialize(&self, request: SharedFactsRequest<'_>) -> Result<(), HostRejection> {
        let released = self
            .participations
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(request.participation())
            .is_some_and(|installed| !installed.instance_owned && installed.is_released());
        let projected = catalog_bindings(request.catalog_binding().as_ref()).and_then(|_| {
            runtime_filter_install(request.initial_runtime_filter().as_ref())?;
            query_options(request.query_options().as_ref())?;
            credential_material(request.initial_credential())?;
            Ok(())
        });
        self.discard_released_marker(request.participation());
        projected?;
        if released {
            return Err(cancelled_establish());
        }
        Err(participation_facts_rejection(
            ParticipationFactsFailure::InstanceCapabilityRequired,
        ))
    }

    fn release(&self, participation: ParticipationRef) -> ReleasedParticipationEvidence {
        #[cfg(novarocks_dev_controls)]
        self.wait_for_accepted_contribution_retry_rendezvous(participation);
        let installed = {
            let mut participations = self
                .participations
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if participations
                .get(participation)
                .is_some_and(|owner| owner.instance_owned)
                || participations
                    .by_execution
                    .get(&participation.query_execution_id())
                    .and_then(|owner| participations.get(*owner))
                    .is_some_and(|owner| owner.instance_owned)
            {
                // This temporary legacy API cannot return a typed refusal.
                // Never turn a key-only call into false release evidence.
                std::process::abort();
            }
            match participations.remove(participation) {
                Some(installed) => installed,
                None => {
                    // Nothing is installed under this reference — either this
                    // is a second release, or the owner is releasing a participation
                    // whose `materialize` has not started yet.
                    //
                    // The second case is real: the owner admits
                    // `Establishing -> Aborting`, a participation with no tasks
                    // completes that abort immediately, and it calls `release`
                    // exactly once per participation — so a materialize that then ran
                    // would install facts nothing would ever take back. Leaving
                    // a released marker makes that establish refuse instead.
                    // The marker is consumed by the materialize that follows,
                    // so markers are bounded by in-flight establishes rather
                    // than accumulating.
                    participations.insert_released_marker(participation);
                    self.catalog_manager
                        .release_query(participation.query_execution_id());
                    // Published after this owner's own lock is released: the
                    // metrics layer holds no execution resource references and
                    // must not be reached with one held.
                    drop(participations);
                    self.publish_catalog_lease_metrics();
                    return ReleasedParticipationEvidence::none();
                }
            }
        };
        self.tear_down(participation, &installed)
    }

    fn advance_shared_domain(
        &self,
        participation: ParticipationRef,
        domain: &ParticipationDomainUpdate,
    ) -> Result<(), HostRejection> {
        let installed = self.active_participation(participation)?;
        if installed.instance_owned {
            return Err(participation_facts_rejection(
                ParticipationFactsFailure::InstanceCapabilityRequired,
            ));
        }
        let _ = domain;
        Err(participation_facts_rejection(
            ParticipationFactsFailure::InstanceCapabilityRequired,
        ))
    }
}

impl NativeParticipationHost {
    fn apply_shared_domain(
        &self,
        participation: ParticipationRef,
        installed: &InstalledParticipation,
        issuer: &InstanceWorkIssuer,
        domain: &ParticipationDomainUpdate,
    ) -> Result<(), HostRejection> {
        self.still_establishing(installed)?;
        match domain {
            // The catalog domain is a scalar: a new version replaces the whole
            // binding. This backend can add a query's catalog leases but can
            // only drop them as a whole query, so a replacement that removes
            // one catalog has no representation here — and applying it as an
            // addition would silently keep the removed catalog resolvable.
            ParticipationDomainUpdate::CatalogBinding { .. } => Err(participation_facts_rejection(
                ParticipationFactsFailure::CatalogReplacementUnsupported,
            )),
            ParticipationDomainUpdate::SharedDynamicFilter { payload, .. } => {
                let contribution = runtime_filter_install(payload.as_ref())?;
                let Some(decoded) =
                    self.decode_contribution(participation, installed, contribution)?
                else {
                    // An advance exists to carry a change. "Install nothing"
                    // either says the participant should be removed, which a
                    // sealed participant cannot represent, or says nothing at
                    // all.
                    return Err(participation_facts_rejection(
                        ParticipationFactsFailure::EmptyFilterAdvance,
                    ));
                };
                {
                    // Checked before the install so the common refusal costs no
                    // participant construction; checked again after, because
                    // this lock is not held across the install.
                    let facts = installed
                        .facts
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    if facts.participant.is_some() {
                        return Err(sealed_participant());
                    }
                }
                let sender = issuer
                    .runtime_filter_sender()
                    .map_err(participation_work_rejection)?;
                let participant = (self.runtime_filter_installer)(
                    participation.query_execution_id(),
                    decoded,
                    sender,
                )
                .map_err(runtime_filter_rejection)?;
                self.adopt_participant(participation, installed, participant)
            }
            ParticipationDomainUpdate::Credential(update) => {
                // One atomic whole-table replace. The owner already classified
                // this rotation as the exact next epoch, and this backend mints
                // nothing: there is no prepare, no commit, and no catalog call.
                let material = credential_material(update)?;
                installed.credentials.install(material.leases())
            }
        }
    }
}

/// The participant install one contribution asks for, if it asks for one.
///
/// A query that runs no runtime filter on this backend still has to establish,
/// and the establish message's contribution field is required, so "no
/// participant" has exactly one representation: a contribution that names no
/// participant and carries nothing else. A contribution that names no
/// participant but *does* carry lifecycle options or an install is a
/// contradiction — reading it as "no participant" would drop a real install on
/// the floor, so it is refused instead.
fn decoded_contribution(
    execution_id: QueryExecutionId,
    contribution: &proto::RuntimeFilterContribution,
    creator: &crate::native_type_construction::NativeTypeConstruction<'_>,
) -> Result<Option<DecodedRuntimeFilterContribution>, HostRejection> {
    if contribution.participant_id == 0 {
        if contribution.lifecycle.is_some() || contribution.install.is_some() {
            return Err(participation_facts_rejection(
                ParticipationFactsFailure::InstallWithoutParticipant,
            ));
        }
        return Ok(None);
    }
    decode_runtime_filter_contribution_with_type_creator(execution_id, contribution, creator)
        .map(Some)
        .map_err(|cause| {
            HostRejection::from_domain(super::execution_host::failure_category(&cause), cause)
        })
}

fn participation_type_construction_rejection(
    cause: &crate::native_type_construction::NativeTypeConstructionError,
) -> HostRejection {
    HostRejection::from_domain(
        super::execution_host::failure_category(cause),
        novarocks_execution::runtime::task::ExecutionFailure::from_domain(cause.clone()),
    )
}

/// Closes one participant, recording a close that did not complete.
///
/// `release` has no way to report a failure — it is the undo half of a pairing
/// the owner has already decided — so a failed close is logged and the
/// participant is dropped anyway. That is deliberate rather than a discard:
/// this host is the last owner of the handle, and the old stack's alternative
/// (put it back and wait for a terminal sweep) has no sweep to wait for here,
/// so retaining it would be an invisible leak instead of a visible failure.
///
/// It is also unreachable today: the production close hook is infallible, and
/// only a test hook can fail. If a hook that can genuinely fail is ever
/// installed, the fix is to give the host contract a place to report it, not to
/// retry blindly here.
/// Seals this participant's runtime-filter observation for the release that is
/// taking it down.
///
/// `QueryTerminationCoordinatorFinalize` is the honest reason here and the
/// only one this boundary can state: the frontend has declared that no legal
/// create can follow, and the registry does not hand a participation to this
/// tear-down until every task it knows is a terminal record. A channel still
/// open at that point closed without publishing, which is a fact to report --
/// cancelling it first would overwrite the observation with a cause the
/// release does not have.
fn seal_runtime_filter_evidence(
    participation: ParticipationRef,
    participant: &Arc<RuntimeFilterParticipant>,
) -> ReleasedParticipationEvidence {
    let snapshot = participant
        .prepare_terminal_capture(QueryTerminationReason::QueryTerminationCoordinatorFinalize);
    match capture_runtime_filter_terminal_profile_contribution(Some(snapshot), true) {
        Ok(telemetry) => match QueryTerminalProfileContributionTelemetry::parse(telemetry) {
            Ok(telemetry) => sealed_runtime_filter_evidence(telemetry),
            Err(error) => {
                // The projection produced a value this process cannot vouch
                // for. Reporting it anyway would make the frontend the first
                // owner to discover it is malformed.
                error!(
                    target: "novarocks::task_execution",
                    participation = %participation,
                    error = %error,
                    "sealed runtime filter contribution does not satisfy the terminal contract; \
                     the release reports it as unavailable"
                );
                sealed_runtime_filter_evidence(runtime_filter_unavailable("CONTRIBUTION_INVALID"))
            }
        },
        Err(error) => {
            // A correctness failure in the observation itself. The query has
            // already produced its answer, so this is reported as unavailable
            // telemetry rather than turned into a release failure.
            error!(
                target: "novarocks::task_execution",
                participation = %participation,
                error = %error,
                "runtime filter observation failed its correctness check at release"
            );
            sealed_runtime_filter_evidence(runtime_filter_unavailable(
                "OBSERVATION_CORRECTNESS_FAILURE",
            ))
        }
    }
}

/// The unavailable telemetry a release reports when it held a participant but
/// could not publish its contribution. Never an empty contribution: that would
/// say the participant observed nothing.
fn runtime_filter_unavailable(code: &str) -> QueryTerminalProfileContributionTelemetry {
    QueryTerminalProfileContributionTelemetry::parse(
        proto::QueryTerminalProfileContributionTelemetry {
            telemetry: Some(
                proto::query_terminal_profile_contribution_telemetry::Telemetry::Unavailable(
                    proto::TerminalTelemetryUnavailable {
                        stage: RUNTIME_FILTER_TERMINAL_CAPTURE_STAGE.to_owned(),
                        code: code.to_owned(),
                    },
                ),
            ),
        },
    )
    .expect("a stage-and-code unavailable reason satisfies the terminal contract")
}

fn close_participant(participation: ParticipationRef, participant: &Arc<RuntimeFilterParticipant>) {
    // The task protocol's release is deliberately cause-free: the owner
    // publishes the termination cause on the participation and on every task, and
    // this boundary is called identically for a rollback and for a normal
    // release. Naming `COORDINATOR_FINALIZE` or `COORDINATOR_ABORT` here would
    // state a cause this host does not know.
    if let Err(error) = participant.close(QueryTerminationReason::Unspecified) {
        error!(
            target: "novarocks::task_execution",
            participation = %participation,
            error = %error,
            "runtime filter participant close did not complete; the participant is dropped"
        );
    }
}

/// The participation owner already retired this install attempt.
fn cancelled_establish() -> HostRejection {
    retired_install(None)
}

fn sealed_participant() -> HostRejection {
    participation_facts_rejection(ParticipationFactsFailure::SealedParticipant)
}

/// One backend is about to build at least one catalog runtime for this
/// attempt.
///
/// The successor of the retired `NOVAROCKS_CATALOG_LOADING`, and deliberately
/// narrower than it: this is emitted only after the whole-set ready fast path
/// has already declined, so it means "a cold install starts here" rather than
/// "an install pass ran". A warm establish emits nothing, which is what makes
/// a case asserting that a warm query rebuilds no runtime able to fail.
///
/// The identity is printed in the task protocol's marker shape -- the same
/// `execution_id=<high>:<low>:<attempt>` word the operation markers use -- so
/// one execution's evidence can be selected across all of them.
#[cfg(novarocks_dev_controls)]
fn emit_catalog_install_started(
    observer: &dyn novarocks_execution::runtime::exchange::TestEventObserver,
    execution_id: QueryExecutionId,
    catalog_count: usize,
) {
    observer.observe(&format!(
        "{} execution_id={}:{}:{} catalog_count={catalog_count}",
        novarocks_failpoint::observation_marker::CATALOG_INSTALL_STARTED,
        execution_id.query_id().high(),
        execution_id.query_id().low(),
        execution_id.attempt_id().get(),
    ));
}

/// Materialization owns classification and the original Connector source.
/// A concurrent participation retirement is an observation, not a new Task cause.
fn catalog_rejection(
    installed: &InstalledParticipation,
    error: CatalogManagerError,
) -> HostRejection {
    if installed.is_released() {
        return retired_install(Some(error));
    }
    HostRejection::from_domain(super::execution_host::failure_category(&error), error)
}

fn participation_work_rejection(error: novarocks_worker::ParticipationWorkError) -> HostRejection {
    if error.is_stopped() {
        return cancelled_establish();
    }
    HostRejection::from_domain(TaskFailureCategory::Internal, error)
}

fn runtime_filter_rejection(error: RuntimeFilterContractError) -> HostRejection {
    HostRejection::from_domain(super::execution_host::failure_category(&error), error)
}

#[derive(Debug)]
struct RetiredParticipationInstall(Option<CatalogManagerError>);
impl fmt::Display for RetiredParticipationInstall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the participation was released while its shared facts were being installed")
    }
}
impl std::error::Error for RetiredParticipationInstall {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.as_ref().map(|error| error as _)
    }
}
impl novarocks_execution_contract::failure::FailureProjection for RetiredParticipationInstall {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        FailureFact::stop_observation(FailureReason::new(
            domain::NATIVE,
            std::num::NonZeroU16::new(810).unwrap(),
        ))
    }
}
fn retired_install(source: Option<CatalogManagerError>) -> HostRejection {
    let cause = RetiredParticipationInstall(source);
    let detail = novarocks_execution_contract::SafeDetail::truncating_display(&cause);
    HostRejection::from_domain_with_detail(TaskFailureCategory::Execution, cause, detail)
}

/// Facts owned by this participation materialization boundary. Foreign codec,
/// Connector and memory causes use their own projection and source chain.
#[derive(Debug)]
enum ParticipationFactsFailure {
    Unavailable(ParticipationRef),
    Released,
    AlreadyMaterialized,
    InstanceCapabilityRequired,
    WrongInstanceParticipation {
        expected: ParticipationRef,
        actual: ParticipationRef,
    },
    ExecutionAlreadyInstalled(QueryExecutionId),
    InstallWithoutParticipant,
    CatalogReplacementUnsupported,
    EmptyFilterAdvance,
    SealedParticipant,
    TaskParticipationUnavailable(QueryExecutionId),
    QueryOptionsMissing,
    FilterCarrier(DomainVersion),
    FilterParticipantUnavailable(QueryExecutionId),
    StorageParticipationUnavailable(QueryExecutionId),
}
impl fmt::Display for ParticipationFactsFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(participation) => write!(f, "participation {participation} holds no shared facts"),
            Self::Released => f.write_str("the participation released its shared facts"),
            Self::AlreadyMaterialized => f.write_str("participation shared facts are already materialized"),
            Self::InstanceCapabilityRequired => f.write_str("participation operation requires its original host instance capability"),
            Self::WrongInstanceParticipation { expected, actual } => write!(f, "host instance for {expected} cannot materialize {actual}"),
            Self::ExecutionAlreadyInstalled(execution) => write!(f, "query execution {execution:?} already has a host instance"),
            Self::InstallWithoutParticipant => f.write_str("runtime filter contribution names no participant but carries an install"),
            Self::CatalogReplacementUnsupported => f.write_str("a catalog binding replacement cannot be applied because catalog leases are released only for a whole query"),
            Self::EmptyFilterAdvance => f.write_str("a shared dynamic filter advance carries no participant install"),
            Self::SealedParticipant => f.write_str("a runtime filter participant is already installed and sealed for this attempt"),
            Self::TaskParticipationUnavailable(execution) => write!(f, "task of {execution:?} has no established participation"),
            Self::QueryOptionsMissing => f.write_str("active participation has no installed query options"),
            Self::FilterCarrier(version) => write!(f, "task dynamic filter version {} is not a codec-produced runtime filter envelope", version.get()),
            Self::FilterParticipantUnavailable(execution) => write!(f, "task dynamic filter for {execution:?} has no installed participant"),
            Self::StorageParticipationUnavailable(execution) => write!(f, "no established participation holds storage credentials for {execution:?}"),
        }
    }
}
impl std::error::Error for ParticipationFactsFailure {}
impl novarocks_execution_contract::failure::FailureProjection for ParticipationFactsFailure {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        let (class, code) = match self {
            Self::Unavailable(_) => (FailureClass::Dependency, 9001),
            Self::Released => {
                return FailureFact::stop_observation(FailureReason::new(
                    domain::NATIVE,
                    std::num::NonZeroU16::new(9002).unwrap(),
                ));
            }
            Self::AlreadyMaterialized => (FailureClass::Invariant, 9003),
            Self::InstanceCapabilityRequired => (FailureClass::Invariant, 9013),
            Self::WrongInstanceParticipation { .. } => (FailureClass::Invariant, 9014),
            Self::ExecutionAlreadyInstalled(_) => (FailureClass::Invariant, 9015),
            Self::InstallWithoutParticipant => (FailureClass::Invariant, 9004),
            Self::CatalogReplacementUnsupported => (FailureClass::Invariant, 9005),
            Self::EmptyFilterAdvance => (FailureClass::Invariant, 9006),
            Self::SealedParticipant => (FailureClass::Invariant, 9007),
            Self::TaskParticipationUnavailable(_) => (FailureClass::Dependency, 9008),
            Self::QueryOptionsMissing => (FailureClass::Invariant, 9009),
            Self::FilterCarrier(_) => (FailureClass::Invariant, 9010),
            Self::FilterParticipantUnavailable(_) => (FailureClass::Dependency, 9011),
            Self::StorageParticipationUnavailable(_) => (FailureClass::Dependency, 9012),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
fn participation_facts_rejection(source: ParticipationFactsFailure) -> HostRejection {
    let detail = novarocks_execution_contract::SafeDetail::truncating_display(&source);
    // The operation refusal verdict is separate from the Task cause's
    // neutral classification. This adapter owns this local policy vocabulary.
    let category = match &source {
        ParticipationFactsFailure::InstallWithoutParticipant
        | ParticipationFactsFailure::CatalogReplacementUnsupported
        | ParticipationFactsFailure::EmptyFilterAdvance
        | ParticipationFactsFailure::SealedParticipant => TaskFailureCategory::Protocol,
        ParticipationFactsFailure::AlreadyMaterialized
        | ParticipationFactsFailure::InstanceCapabilityRequired
        | ParticipationFactsFailure::WrongInstanceParticipation { .. }
        | ParticipationFactsFailure::ExecutionAlreadyInstalled(_)
        | ParticipationFactsFailure::QueryOptionsMissing
        | ParticipationFactsFailure::FilterCarrier(_) => TaskFailureCategory::Internal,
        ParticipationFactsFailure::Unavailable(_)
        | ParticipationFactsFailure::Released
        | ParticipationFactsFailure::TaskParticipationUnavailable(_)
        | ParticipationFactsFailure::FilterParticipantUnavailable(_)
        | ParticipationFactsFailure::StorageParticipationUnavailable(_) => {
            TaskFailureCategory::Execution
        }
    };
    HostRejection::from_domain_with_detail(category, source, detail)
}

#[derive(Debug)]
struct DynamicFilterNotAccepted {
    version: DomainVersion,
    status: i32,
    detail: novarocks_execution_contract::failure::BoundedDetail,
}
impl std::fmt::Display for DynamicFilterNotAccepted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "task dynamic filter version {} received status {}: {}",
            self.version.get(),
            self.status,
            self.detail
        )
    }
}
impl std::error::Error for DynamicFilterNotAccepted {}
impl novarocks_execution_contract::failure::FailureProjection for DynamicFilterNotAccepted {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::*;
        // The response says only that delivery was not accepted. It cannot
        // prove Task cancellation, a capacity quantity, or recovery authority.
        FailureFact::new(
            FailureClass::Dependency,
            FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(9541).unwrap()),
        )
    }
}

/// The query-scoped facts one task's preparation needs.
///
/// The execution side only ever holds a `&TaskDescriptor`, and a `TaskIdentity`
/// carries no frontend process id, so it structurally cannot name a
/// `ParticipationRef`. This maps the execution id it does have onto the participation
/// that installed the facts, and refuses when there is none: an unestablished
/// or released participation has no runtime filter, no catalog lease, and no
/// storage credential, and there is no process-level substitute for any of
/// them that a task could legitimately fall back to.
use novarocks_execution::runtime::task::io::{TaskEvent, TaskEventSink};
use novarocks_execution::runtime_filter::RuntimeFilterSessionRef;
use novarocks_execution_contract::task_execution::domain::{CodecOwnedContent, DomainVersion};
use novarocks_proto_models::filter;
use novarocks_types::UniqueId;

use crate::backend_task_execution::execution_host::TaskParticipationFacts;
use novarocks_spi::connector::{
    CatalogHandle, ConnectorExecutionReadBinding, ConnectorExecutionWriteBinding,
};

impl TaskParticipationFacts for NativeParticipationHost {
    fn query_context(
        &self,
        execution: QueryExecutionId,
    ) -> Result<Arc<QueryContext>, HostRejection> {
        let participation = self.participation_for_execution(execution).ok_or_else(|| {
            participation_facts_rejection(ParticipationFactsFailure::TaskParticipationUnavailable(
                execution,
            ))
        })?;
        let installed = self.active_participation(participation)?;
        let context = installed
            .query_context
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        context.ok_or_else(|| {
            participation_facts_rejection(ParticipationFactsFailure::QueryOptionsMissing)
        })
    }

    fn query_options(
        &self,
        execution: QueryExecutionId,
    ) -> Result<EstablishedQueryOptions, HostRejection> {
        let participation = self.participation_for_execution(execution).ok_or_else(|| {
            participation_facts_rejection(ParticipationFactsFailure::TaskParticipationUnavailable(
                execution,
            ))
        })?;
        let installed = self.active_participation(participation)?;
        let facts = installed
            .facts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if facts.released {
            return Err(participation_facts_rejection(
                ParticipationFactsFailure::Released,
            ));
        }
        facts.query_options.clone().ok_or_else(|| {
            participation_facts_rejection(ParticipationFactsFailure::QueryOptionsMissing)
        })
    }

    fn runtime_filter_session(
        &self,
        execution: QueryExecutionId,
        task_key: UniqueId,
        expects_bindings: bool,
    ) -> Result<Option<RuntimeFilterSessionRef>, novarocks_worker::RuntimeFilterContractError> {
        let Some(participant) = self.participant_for_execution(execution) else {
            // A query that installs no filter on this backend is the ordinary
            // case; a task whose plan binds one and finds none is not, because
            // it would otherwise read unfiltered and call that success.
            return if expects_bindings {
                Err(
                    novarocks_worker::RuntimeFilterContractError::invalid_contract(format!(
                        "task of {execution:?} binds a runtime filter but its participation installed \
                     none on this backend"
                    )),
                )
            } else {
                Ok(None)
            };
        };
        participant.session_for_task(execution, task_key, expects_bindings)
    }

    fn runtime_filter_session_for_bindings(
        &self,
        execution: QueryExecutionId,
        task_key: UniqueId,
        bindings: &BTreeSet<u32>,
    ) -> Result<Option<RuntimeFilterSessionRef>, novarocks_worker::RuntimeFilterContractError> {
        let Some(participant) = self.participant_for_execution(execution) else {
            // As for a plan-tree task: no participant hosts nothing, which is
            // the ordinary answer only for a program that binds nothing.
            return if bindings.is_empty() {
                Ok(None)
            } else {
                Err(
                    novarocks_worker::RuntimeFilterContractError::invalid_contract(format!(
                        "task of {execution:?} binds runtime filters {bindings:?} but its query \
                     participation installed none on this backend"
                    )),
                )
            };
        };
        participant.session_for_task_bindings(execution, task_key, bindings)
    }

    fn runtime_filter_event_sink(
        &self,
        execution: QueryExecutionId,
        task_key: UniqueId,
    ) -> Arc<dyn TaskEventSink> {
        Arc::new(ParticipationRuntimeFilterEventSink {
            participant: self.participant_for_execution(execution),
            task_key,
        })
    }

    fn bind_runtime_filter_feedback(
        &self,
        execution: QueryExecutionId,
        carrier: TaskIdentity,
        producer: novarocks_worker::TaskDynamicFilterWriter,
    ) -> bool {
        let Some(participation) = self.participation_for_execution(execution) else {
            return false;
        };
        let installed = {
            let participations = self
                .participations
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            match participations.get(participation) {
                Some(installed) => Arc::clone(installed),
                None => return false,
            }
        };
        let mut facts = installed
            .facts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if facts.released {
            return false;
        }
        // First-wins. A second carrier would replace the sink on every channel
        // session of the participant, so which task publishes a domain would
        // depend on submission order and the frontend would poll a task that
        // no longer publishes.
        if facts.feedback.is_some() {
            return false;
        }
        // A query with no participant on this backend has no channel that could
        // ever reduce, so there is nothing to carry.
        let Some(participant) = facts.participant.clone() else {
            return false;
        };
        let sink = TaskRuntimeFilterFeedbackEgress::new(carrier, producer);
        #[cfg(novarocks_dev_controls)]
        let sink = {
            let sink = if let Some(controls) = self.test_controls.clone() {
                sink.with_test_controls(controls)
            } else {
                sink
            };
            sink.with_observation_controls(self.observations.clone())
        };
        let sink = Arc::new(sink);
        // The participant holds this weakly, exactly as the lifecycle egress
        // was held: a background publisher must not be able to keep the query
        // participation alive through its own sink.
        participant.set_frontend_feedback_sink(Arc::downgrade(
            &(Arc::clone(&sink) as Arc<dyn BackendFrontendFeedbackSink>),
        ));
        facts.feedback = Some(sink);
        true
    }

    fn deliver_task_dynamic_filter(
        &self,
        execution: QueryExecutionId,
        _task_key: UniqueId,
        version: DomainVersion,
        payload: &Arc<dyn CodecOwnedContent>,
    ) -> Result<(), HostRejection> {
        let envelope = stored_message::<filter::RuntimeFilterEnvelope>(payload.as_ref())
            .ok_or_else(|| {
                participation_facts_rejection(ParticipationFactsFailure::FilterCarrier(version))
            })?
            .clone();
        let participant = self.participant_for_execution(execution).ok_or_else(|| {
            participation_facts_rejection(ParticipationFactsFailure::FilterParticipantUnavailable(
                execution,
            ))
        })?;
        // The one decoder that turns a wire envelope into a backend one lives
        // in the runtime-filter transport. Reusing it is what keeps this from
        // becoming a second authority over the same wire shape.
        let response = novarocks_native_adapter::runtime_filter_rpc::handle_runtime_filter_envelope_typed(
            participant as Arc<dyn novarocks_native_adapter::runtime_filter_rpc::BackendRuntimeFilterEnvelopeIngress>,
            envelope,
        )
        .map_err(|source| HostRejection::from_domain(super::execution_host::failure_category(&source), source))?;
        // A duplicate is a legal answer to a replayed push; only a rejection
        // or an unknown status means the filter did not land.
        let accepted = matches!(
            filter::RuntimeFilterAcceptStatus::try_from(response.accept_status),
            Ok(filter::RuntimeFilterAcceptStatus::Accepted
                | filter::RuntimeFilterAcceptStatus::Duplicate)
        );
        if !accepted {
            let source = DynamicFilterNotAccepted {
                version,
                status: response.accept_status,
                detail: novarocks_execution_contract::failure::BoundedDetail::truncating(
                    &response.rejection_reason,
                ),
            };
            return Err(HostRejection::from_domain(
                super::execution_host::failure_category(&source),
                source,
            ));
        }
        Ok(())
    }

    fn catalog_read_execution(
        &self,
        execution: QueryExecutionId,
        handle: &CatalogHandle,
    ) -> Result<ConnectorExecutionReadBinding, novarocks_worker::CatalogExecutionAccessError> {
        let runtime = self
            .catalog_manager
            .resolve_for_query(execution, handle)
            .ok_or_else(
                || novarocks_worker::CatalogExecutionAccessError::LeaseUnavailable {
                    catalog: handle.clone(),
                },
            )?;
        runtime.read().cloned().ok_or_else(|| {
            novarocks_worker::CatalogExecutionAccessError::ReadUnavailable {
                catalog: handle.clone(),
            }
        })
    }

    fn catalog_write_execution(
        &self,
        execution: QueryExecutionId,
        handle: &CatalogHandle,
    ) -> Result<ConnectorExecutionWriteBinding, novarocks_worker::CatalogExecutionAccessError> {
        let runtime = self
            .catalog_manager
            .resolve_for_query(execution, handle)
            .ok_or_else(
                || novarocks_worker::CatalogExecutionAccessError::LeaseUnavailable {
                    catalog: handle.clone(),
                },
            )?;
        runtime.write().cloned().ok_or_else(|| {
            novarocks_worker::CatalogExecutionAccessError::WriteUnavailable {
                catalog: handle.clone(),
            }
        })
    }

    fn storage_resolver(
        &self,
        execution: QueryExecutionId,
    ) -> Result<Arc<dyn ConnectorStorageResolver>, HostRejection> {
        // Deliberately a refusal, not a permissive default: a scan that
        // reached read time with no vended credential would fail there anyway,
        // and failing here says which participation is missing.
        self.participation_for_execution(execution)
            .and_then(|participation| self.storage_resolver_for_participation(participation))
            .ok_or_else(|| {
                participation_facts_rejection(
                    ParticipationFactsFailure::StorageParticipationUnavailable(execution),
                )
            })
    }
}

/// Folds a running fragment's runtime-filter evidence into the participant
/// that owns the installed consumer identity.
///
/// Progress and profile events are dropped here: they belong to the task's own
/// metrics owner, which is a different sink. Silently folding them into the
/// filter participant would put two unrelated facts behind one identity.
struct ParticipationRuntimeFilterEventSink {
    /// Resolved once, when the fragment is installed: the participation has
    /// materialized by then, and a query with no filter on this backend has
    /// nothing to fold into.
    participant: Option<Arc<RuntimeFilterParticipant>>,
    task_key: UniqueId,
}

impl TaskEventSink for ParticipationRuntimeFilterEventSink {
    fn record(&self, event: TaskEvent) {
        let Some(participant) = self.participant.as_ref() else {
            return;
        };
        match event {
            TaskEvent::RuntimeFilterRowEffect(effect) => {
                participant.record_row_effect(self.task_key, effect);
            }
            TaskEvent::Progress(_) | TaskEvent::ProfileSnapshot(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{NativeParticipationHost, NativeQueryExecutionResourceSnapshot};

    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Mutex};

    use novarocks_execution_contract::ConfidentialContent;
    use novarocks_execution_contract::task_execution::domain::{
        CodecOwnedContent, CredentialDomainId, CredentialEpoch, DomainVersion,
    };
    use novarocks_execution_contract::task_execution::identity::ParticipationRef;
    use novarocks_execution_contract::task_execution::operation::{
        CredentialUpdate, ParticipationDomainUpdate,
    };
    use novarocks_execution_contract::task_execution::status::TaskFailureCategory;
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_codec::catalog::CatalogSet;
    use novarocks_proto_codec::lifecycle::encode_credential_lease_descriptor;
    use novarocks_proto_models::{filter, novarocks as proto};
    use novarocks_spi::connector::{
        CatalogHandle, CatalogProperties, CatalogVersion, ConnectorInstanceId, ConnectorProviderId,
        CredentialLeaseDescriptor, CredentialLeaseProvider, StorageAccessDomainId,
        StorageAccessRequest, StorageCredentialScopePrefix,
    };
    use novarocks_spi::connector::{
        ConnectorExecutionRoleBinding, ConnectorExecutionRoleBindingFactory,
        ConnectorMaterializationError, ConnectorMaterializationErrorClass,
        ConnectorMaterializationRetryDisposition, NormalizedCatalogProperties,
    };
    use novarocks_task_codec::domain::{WireContent, WireCredential};
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
    };

    use super::super::execution_host::TaskParticipationFacts;
    use crate::backend_task_execution::tests::node_fixture::{
        NativeNodePreparation, ParticipationContents,
    };
    use novarocks_execution_contract::task_execution::identity::TaskIdentity;
    use novarocks_native_adapter::backend_test_support::test_backend_data_runtime;
    use novarocks_native_adapter::runtime_filter_install::DecodedRuntimeFilterContribution;
    use novarocks_native_adapter::runtime_filter_participant::{
        NativeRuntimeFilterParticipantFactory, RuntimeFilterParticipant,
    };
    use novarocks_native_adapter::task_shared_facts::release_runtime_filter_telemetry;
    use novarocks_worker::SharedFactsRequest;
    use novarocks_worker::{CatalogManager, ConnectorExecutionRoleBindingFactorySet};
    use novarocks_worker::{
        HostRejection, ParticipationHost, ParticipationHostFactory, ParticipationHostInstance,
        ParticipationMaterializationControl,
    };
    use novarocks_worker::{RuntimeFilterContractError, RuntimeFilterContractErrorCode};

    const ANNOUNCED_SCOPE: &str = "s3://bucket/a";
    const ROTATED_SCOPE: &str = "s3://bucket/b";

    // ---------------------------------------------------------------- identity

    fn execution_id(attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(0x5245_4553, 11),
            AttemptId::new(attempt).expect("nonzero attempt"),
        )
        .expect("legal execution id")
    }

    fn participation(attempt: u64) -> ParticipationRef {
        ParticipationRef::new(
            execution_id(attempt),
            FrontendProcessId::new_v7(),
            BackendProcessId::new_v7(),
        )
    }

    // ---------------------------------------------------------------- catalogs

    fn catalog_owner() -> CatalogHandle {
        CatalogHandle::new(
            ConnectorInstanceId::try_from_canonical("catalog.analytics").expect("catalog id"),
            CatalogVersion::from_bytes([7; 32]),
        )
    }

    fn catalog_properties() -> CatalogProperties {
        CatalogProperties::new(
            catalog_owner(),
            ConnectorProviderId::parse("iceberg").expect("static provider ID"),
            1,
            vec![],
            vec![],
        )
        .expect("legal catalog properties")
    }

    fn catalog_payload(catalogs: Vec<CatalogProperties>) -> Arc<dyn CodecOwnedContent> {
        let set = CatalogSet::new(catalogs).expect("legal catalog set");
        Arc::new(WireContent::new(b"catalog", set.as_proto().clone()))
    }

    fn query_options_payload() -> Arc<dyn CodecOwnedContent> {
        Arc::new(WireContent::new(
            b"query-options",
            proto::QueryOptions::default(),
        ))
    }

    /// A binding factory whose behaviour the test drives.
    struct ScriptedFactory {
        fail: AtomicBool,
        /// Released once the factory is inside a bind, so a test can act while
        /// an install is genuinely in flight.
        entered: Mutex<Option<Arc<Barrier>>>,
        proceed: Mutex<Option<Arc<Barrier>>>,
        binds: AtomicUsize,
    }

    impl ScriptedFactory {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                fail: AtomicBool::new(false),
                entered: Mutex::new(None),
                proceed: Mutex::new(None),
                binds: AtomicUsize::new(0),
            })
        }
    }

    impl ConnectorExecutionRoleBindingFactory for ScriptedFactory {
        fn provider_id(&self) -> ConnectorProviderId {
            ConnectorProviderId::parse("iceberg").expect("static provider ID")
        }

        fn bind(
            &self,
            properties: &NormalizedCatalogProperties,
        ) -> Result<ConnectorExecutionRoleBinding, ConnectorMaterializationError> {
            self.binds.fetch_add(1, Ordering::SeqCst);
            let entered = self.entered.lock().expect("entered gate").clone();
            if let Some(entered) = entered {
                entered.wait();
            }
            let proceed = self.proceed.lock().expect("proceed gate").clone();
            if let Some(proceed) = proceed {
                proceed.wait();
            }
            if self.fail.load(Ordering::SeqCst) {
                return Err(ConnectorMaterializationError::new(
                    ConnectorMaterializationErrorClass::Unavailable,
                    ConnectorMaterializationRetryDisposition::Transient,
                    "scripted catalog materialization failure",
                ));
            }
            ConnectorExecutionRoleBinding::try_new(properties.clone(), None, None)
                .map_err(Into::into)
        }
    }

    // --------------------------------------------------------- runtime filters

    fn lifecycle_options() -> filter::RuntimeFilterQueryLifecycleOptions {
        filter::RuntimeFilterQueryLifecycleOptions {
            delivery_expire_ms: 1,
            query_expire_ms: 1,
            transport_retry_interval_ms: 1,
            transport_max_attempts: 1,
            transport_deadline_ms: 1,
            transport_max_pending_entries: 1,
            transport_max_pending_bytes: 1,
        }
    }

    /// A contribution that installs a real, channel-less participant.
    fn participant_contribution(attempt: u64) -> proto::RuntimeFilterContribution {
        proto::RuntimeFilterContribution {
            participant_id: u32::try_from(attempt).expect("small attempt"),
            lifecycle: Some(lifecycle_options()),
            install: Some(filter::RuntimeFilterParticipantInstall::default()),
        }
    }

    /// The one representation of "this backend installs no participant".
    fn no_contribution() -> proto::RuntimeFilterContribution {
        proto::RuntimeFilterContribution::default()
    }

    fn filter_payload(
        contribution: proto::RuntimeFilterContribution,
    ) -> Arc<dyn CodecOwnedContent> {
        Arc::new(WireContent::new(b"contribution", contribution))
    }

    /// What a test observes about participant installs and closes.
    ///
    /// It is shared rather than owned by the factory because the close hook
    /// outlives the call that built it: the participant carries the hook, and
    /// the host may close it long after `install` returned.
    #[derive(Default)]
    struct FilterLedger {
        installs: AtomicUsize,
        closes: AtomicUsize,
        fail_close: AtomicBool,
    }

    /// Wraps the real factory so a test can count installs and fail a close.
    struct RecordingFilterFactory {
        ledger: Arc<FilterLedger>,
    }

    impl RecordingFilterFactory {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                ledger: Arc::new(FilterLedger::default()),
            })
        }
    }

    impl RecordingFilterFactory {
        fn install(
            &self,
            execution_id: QueryExecutionId,
            contribution: DecodedRuntimeFilterContribution,
            work: novarocks_worker::RuntimeFilterSenderWork,
        ) -> Result<Arc<RuntimeFilterParticipant>, RuntimeFilterContractError> {
            self.ledger.installs.fetch_add(1, Ordering::SeqCst);
            let participant = NativeRuntimeFilterParticipantFactory::new(
                test_backend_data_runtime(),
            )
            .install(execution_id, contribution, work)?;
            let ledger = Arc::clone(&self.ledger);
            Ok(
                participant.with_close_hook_for_test(Arc::new(move |_participant, _reason| {
                    ledger.closes.fetch_add(1, Ordering::SeqCst);
                    if ledger.fail_close.load(Ordering::SeqCst) {
                        return Err(RuntimeFilterContractError::new(
                            RuntimeFilterContractErrorCode::ParticipantClosed,
                            "scripted runtime filter close failure",
                        ));
                    }
                    Ok(())
                })),
            )
        }
    }

    // ------------------------------------------------------------- credentials

    fn lease_id(seed: u8) -> novarocks_spi::connector::CredentialLeaseId {
        novarocks_spi::connector::CredentialLeaseId::try_from_bytes([seed; 16])
            .expect("legal lease id")
    }

    fn prefix(value: &str) -> StorageCredentialScopePrefix {
        StorageCredentialScopePrefix::try_from_normalized(value).expect("legal prefix")
    }

    fn live_until() -> u64 {
        u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock after epoch")
                .as_millis(),
        )
        .expect("representable")
            + 600_000
    }

    fn credential(epoch: u64, scope: &str, not_after: u64) -> CredentialUpdate {
        let descriptor = encode_credential_lease_descriptor(
            &CredentialLeaseDescriptor::try_new(
                lease_id(1),
                epoch,
                catalog_owner(),
                CredentialLeaseProvider::S3,
                vec![prefix(scope)],
                not_after,
                true,
                None,
                StorageAccessDomainId::from_bytes([8; 32]),
            )
            .expect("legal descriptor"),
        );
        let material = Arc::new(
            WireCredential::decode(&[descriptor], FieldPath::root("credential"))
                .expect("legal rotation"),
        );
        CredentialUpdate::new(
            CredentialDomainId::new(1),
            CredentialEpoch::new(epoch).expect("nonzero epoch"),
            material as Arc<dyn ConfidentialContent>,
        )
    }

    fn empty_credential() -> CredentialUpdate {
        let material = Arc::new(
            WireCredential::decode(&[], FieldPath::root("credential"))
                .expect("an empty rotation is legal"),
        );
        CredentialUpdate::new(
            CredentialDomainId::new(1),
            CredentialEpoch::FIRST,
            material as Arc<dyn ConfidentialContent>,
        )
    }

    fn storage_request(location: &str) -> StorageAccessRequest {
        StorageAccessRequest::try_new(catalog_owner(), location).expect("legal storage request")
    }

    // ------------------------------------------------------------------- fixture

    struct Fixture {
        host: Arc<NativeParticipationHost>,
        catalog_manager: Arc<CatalogManager<ConnectorExecutionRoleBinding>>,
        catalog_factory: Arc<ScriptedFactory>,
        filter_factory: Arc<RecordingFilterFactory>,
        owners: Mutex<BTreeMap<ParticipationRef, novarocks_worker::ParticipationWorkTestOwner>>,
        instances: Mutex<BTreeMap<ParticipationRef, Arc<dyn ParticipationHostInstance>>>,
    }

    impl Fixture {
        fn new() -> Self {
            Self::with_catalog_config(novarocks_worker::CatalogManagerConfig::default())
        }
        fn with_catalog_config(config: novarocks_worker::CatalogManagerConfig) -> Self {
            let catalog_factory = ScriptedFactory::new();
            let filter_factory = RecordingFilterFactory::new();
            let catalog_manager = Arc::new(
                CatalogManager::<ConnectorExecutionRoleBinding>::try_new(config)
                    .expect("the fixture declares a valid catalog policy"),
            );
            let factories = Arc::new(
                ConnectorExecutionRoleBindingFactorySet::try_new([
                    Arc::clone(&catalog_factory) as Arc<dyn ConnectorExecutionRoleBindingFactory>
                ])
                .expect("legal factory set"),
            );
            let filter_installer = Arc::clone(&filter_factory);
            let host = Arc::new(NativeParticipationHost::new(
                crate::native_query_context::NativeQueryContextRuntime::new(
                    crate::backend_test_support::test_memory_authority(),
                ),
                Arc::clone(&catalog_manager),
                factories,
                Arc::new(move |execution_id, contribution, work| {
                    filter_installer.install(execution_id, contribution, work)
                }),
                test_backend_data_runtime(),
            ));
            Self {
                host,
                catalog_manager,
                catalog_factory,
                filter_factory,
                owners: Mutex::new(BTreeMap::new()),
                instances: Mutex::new(BTreeMap::new()),
            }
        }

        fn create_instance(
            &self,
            participation: ParticipationRef,
        ) -> Result<Arc<dyn ParticipationHostInstance>, HostRejection> {
            let grant = self
                .owners
                .lock()
                .unwrap()
                .entry(participation)
                .or_insert_with(|| {
                    novarocks_worker::ParticipationWorkTestOwner::new_for(participation)
                })
                .instance();
            self.host.clone().create_instance(participation, grant)
        }

        fn release(
            &self,
            participation: ParticipationRef,
        ) -> novarocks_worker::ReleasedParticipationEvidence {
            let instance = self.instances.lock().unwrap().get(&participation).cloned();
            match instance {
                Some(instance) => instance.release(),
                None => self.host.release(participation),
            }
        }

        fn advance_shared_domain(
            &self,
            participation: ParticipationRef,
            domain: &ParticipationDomainUpdate,
        ) -> Result<(), HostRejection> {
            let instance = self.instances.lock().unwrap().get(&participation).cloned();
            match instance {
                Some(instance) => instance.advance_shared_domain(domain),
                None => self.host.advance_shared_domain(participation, domain),
            }
        }

        fn establish(
            &self,
            participation: ParticipationRef,
            catalogs: Vec<CatalogProperties>,
            contribution: proto::RuntimeFilterContribution,
            credential: &CredentialUpdate,
        ) -> Result<(), super::HostRejection> {
            self.establish_with_options(
                participation,
                catalogs,
                contribution,
                proto::QueryOptions::default(),
                credential,
            )
        }

        fn establish_with_options(
            &self,
            participation: ParticipationRef,
            catalogs: Vec<CatalogProperties>,
            contribution: proto::RuntimeFilterContribution,
            query_options: proto::QueryOptions,
            credential: &CredentialUpdate,
        ) -> Result<(), super::HostRejection> {
            let catalog = catalog_payload(catalogs);
            let filter = filter_payload(contribution);
            let options: Arc<dyn CodecOwnedContent> =
                Arc::new(WireContent::new(b"query-options", query_options));
            let instance = {
                let mut instances = self.instances.lock().unwrap();
                if let Some(instance) = instances.get(&participation) {
                    instance.clone()
                } else {
                    let instance = self.create_instance(participation)?;
                    instances.insert(participation, instance.clone());
                    instance
                }
            };
            struct Active;
            impl ParticipationMaterializationControl for Active {
                fn is_stopped(&self) -> bool {
                    false
                }
            }
            instance.materialize(
                SharedFactsRequest::new(participation, &catalog, &filter, &options, credential),
                &Active,
            )
        }

        fn legacy_establish(
            &self,
            participation: ParticipationRef,
            catalogs: Vec<CatalogProperties>,
            contribution: proto::RuntimeFilterContribution,
            credential: &CredentialUpdate,
        ) -> Result<(), HostRejection> {
            let catalog = catalog_payload(catalogs);
            let filter = filter_payload(contribution);
            let options = query_options_payload();
            self.host.materialize(SharedFactsRequest::new(
                participation,
                &catalog,
                &filter,
                &options,
                credential,
            ))
        }

        fn query_leases(&self) -> usize {
            self.catalog_manager.pin_snapshot().query_pins
        }

        fn handle_leases(&self) -> usize {
            self.catalog_manager.pin_snapshot().handle_pins
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let instances = std::mem::take(self.instances.get_mut().unwrap());
            for instance in instances.values() {
                drop(instance.release());
            }
            drop(instances);
            let owners = std::mem::take(self.owners.get_mut().unwrap());
            let runtime = test_backend_data_runtime();
            for owner in owners.into_values() {
                runtime.block_on(owner.finish());
            }
        }
    }

    // ----------------------------------------------------------------- tests

    fn materialize_instance(
        instance: &Arc<dyn ParticipationHostInstance>,
        participation: ParticipationRef,
        catalogs: Vec<CatalogProperties>,
        credential: &CredentialUpdate,
    ) -> Result<(), HostRejection> {
        let catalog = catalog_payload(catalogs);
        let filter = filter_payload(no_contribution());
        let options = query_options_payload();
        struct Active;
        impl ParticipationMaterializationControl for Active {
            fn is_stopped(&self) -> bool {
                false
            }
        }
        instance.materialize(
            SharedFactsRequest::new(participation, &catalog, &filter, &options, credential),
            &Active,
        )
    }

    #[test]
    fn original_scope_stop_prevents_a_cold_bind_before_physical_release() {
        struct Stopped;
        impl ParticipationMaterializationControl for Stopped {
            fn is_stopped(&self) -> bool {
                true
            }
        }
        let fixture = Fixture::new();
        let identity = participation(91_003);
        let instance = fixture.create_instance(identity).unwrap();
        assert!(
            !fixture
                .host
                .participations
                .lock()
                .unwrap()
                .get(identity)
                .unwrap()
                .is_released()
        );
        let catalogs = catalog_payload(vec![catalog_properties()]);
        let filter = filter_payload(no_contribution());
        let options = query_options_payload();
        let credential = credential(1, ANNOUNCED_SCOPE, live_until());
        let result = instance.materialize(
            SharedFactsRequest::new(identity, &catalogs, &filter, &options, &credential),
            &Stopped,
        );
        assert!(result.is_err());
        assert_eq!(fixture.catalog_factory.binds.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.query_leases(), 0);
        // The original capability still owns the actual release and disposal.
        let _evidence = instance.release();
        drop(instance);
        assert!(
            fixture
                .host
                .participations
                .lock()
                .unwrap()
                .get(identity)
                .is_none()
        );
    }

    #[test]
    fn an_instance_is_registered_once_and_release_before_materialization_cannot_reopen_it() {
        let fixture = Fixture::new();
        let identity = participation(91_001);
        let instance = fixture.create_instance(identity).unwrap();
        assert_eq!(instance.participation(), identity);
        assert!(fixture.create_instance(identity).is_err());
        let other_reference = ParticipationRef::new(
            identity.query_execution_id(),
            FrontendProcessId::new_v7(),
            identity.backend_process_id(),
        );
        assert!(fixture.create_instance(other_reference).is_err());
        let wrong = materialize_instance(
            &instance,
            participation(91_002),
            vec![],
            &empty_credential(),
        );
        assert!(wrong.is_err());
        assert_eq!(fixture.query_leases(), 0);
        drop(instance.release());
        let late = materialize_instance(&instance, identity, vec![], &empty_credential());
        assert!(late.is_err());
        assert!(
            instance
                .advance_shared_domain(&ParticipationDomainUpdate::Credential(credential(
                    2,
                    ANNOUNCED_SCOPE,
                    live_until()
                ),))
                .is_err()
        );
        assert!(fixture.create_instance(identity).is_err());
        assert!(
            fixture
                .host
                .participations
                .lock()
                .unwrap()
                .get(identity)
                .is_some()
        );
        drop(instance.release());
        drop(instance);
        assert!(
            fixture
                .host
                .participations
                .lock()
                .unwrap()
                .get(identity)
                .is_none()
        );
        assert_eq!(
            fixture
                .host
                .participation_for_execution(identity.query_execution_id()),
            None
        );
        let successor = fixture.create_instance(identity).unwrap();
        drop(successor.release());
        drop(successor);
    }

    #[test]
    fn the_actual_instance_rotates_and_closes_its_original_slot_without_key_fallback() {
        let fixture = Fixture::new();
        let identity = participation(91_003);
        let instance = fixture.create_instance(identity).unwrap();
        materialize_instance(
            &instance,
            identity,
            vec![catalog_properties()],
            &credential(1, ANNOUNCED_SCOPE, live_until()),
        )
        .unwrap();
        let original = fixture.host.active_participation(identity).unwrap();
        let resolver = fixture
            .host
            .storage_resolver_for_participation(identity)
            .unwrap();
        assert!(
            resolver
                .resolve_vended_s3(&storage_request("s3://bucket/a/file.parquet"))
                .is_ok()
        );
        instance
            .advance_shared_domain(&ParticipationDomainUpdate::Credential(credential(
                2,
                "s3://bucket/b",
                live_until(),
            )))
            .unwrap();
        assert_eq!(original.credentials.installed_epoch(lease_id(1)), Some(2));
        assert!(
            resolver
                .resolve_vended_s3(&storage_request("s3://bucket/a/file.parquet"))
                .is_err()
        );
        assert!(
            resolver
                .resolve_vended_s3(&storage_request("s3://bucket/b/file.parquet"))
                .is_ok()
        );
        assert!(
            fixture
                .host
                .advance_shared_domain(
                    identity,
                    &ParticipationDomainUpdate::Credential(credential(
                        3,
                        ANNOUNCED_SCOPE,
                        live_until()
                    ),)
                )
                .is_err()
        );
        assert!(
            fixture
                .legacy_establish(identity, vec![], no_contribution(), &empty_credential())
                .is_err()
        );
        assert_eq!(original.credentials.installed_epoch(lease_id(1)), Some(2));
        drop(instance.release());
        assert_eq!(fixture.query_leases(), 0);
        assert_eq!(original.credentials.installed_lease_count(), 0);
        assert!(
            resolver
                .resolve_vended_s3(&storage_request("s3://bucket/b/file.parquet"))
                .is_err()
        );
        assert!(
            instance
                .advance_shared_domain(&ParticipationDomainUpdate::Credential(credential(
                    3,
                    ANNOUNCED_SCOPE,
                    live_until()
                ),))
                .is_err()
        );
        assert!(fixture.create_instance(identity).is_err());
        drop(instance.release());
        drop(instance);
        let successor = fixture.create_instance(identity).unwrap();
        materialize_instance(
            &successor,
            identity,
            vec![catalog_properties()],
            &credential(1, ANNOUNCED_SCOPE, live_until()),
        )
        .unwrap();
        let installed = fixture.host.active_participation(identity).unwrap();
        assert!(!Arc::ptr_eq(&original, &installed));
        assert_eq!(installed.credentials.installed_epoch(lease_id(1)), Some(1));
        assert!(
            resolver
                .resolve_vended_s3(&storage_request("s3://bucket/b/file.parquet"))
                .is_err()
        );
        drop(successor.release());
        drop(successor);
        assert_eq!(fixture.query_leases(), 0);
    }

    #[test]
    fn an_inflight_materialization_keeps_its_released_instance_until_the_actual_call_clone_exits() {
        // This case observes original M/instance/lease custody, independently
        // of the catalog owner's separately tested transient backoff policy.
        let fixture = Fixture::with_catalog_config(novarocks_worker::CatalogManagerConfig {
            transient_retry_cooldown: std::time::Duration::ZERO,
            ..novarocks_worker::CatalogManagerConfig::default()
        });
        let identity = participation(91_004);
        let instance = fixture.create_instance(identity).unwrap();
        let entered = Arc::new(Barrier::new(2));
        let proceed = Arc::new(Barrier::new(2));
        *fixture.catalog_factory.entered.lock().unwrap() = Some(entered.clone());
        *fixture.catalog_factory.proceed.lock().unwrap() = Some(proceed.clone());
        let running_instance = instance.clone();
        let materialization = std::thread::spawn(move || {
            materialize_instance(
                &running_instance,
                identity,
                vec![catalog_properties()],
                &credential(1, ANNOUNCED_SCOPE, live_until()),
            )
        });
        entered.wait();
        let original = fixture.host.active_participation(identity).unwrap();
        drop(instance.release());
        drop(instance);
        // The actual M call owns the remaining capability. A released row is
        // not a released identity while its external rollback may still run.
        let retained = fixture
            .host
            .participations
            .lock()
            .unwrap()
            .get(identity)
            .is_some_and(|current| Arc::ptr_eq(current, &original));
        let replacement_refused = fixture.create_instance(identity).is_err();
        let closed = original.credentials.installed_lease_count() == 0;
        proceed.wait();
        let materialized = materialization.join().unwrap();
        let old_leases_released = fixture.query_leases() == 0;
        *fixture.catalog_factory.entered.lock().unwrap() = None;
        *fixture.catalog_factory.proceed.lock().unwrap() = None;
        let successor = fixture.create_instance(identity).unwrap();
        materialize_instance(
            &successor,
            identity,
            vec![catalog_properties()],
            &empty_credential(),
        )
        .unwrap();
        let successor_leases = fixture.query_leases();
        drop(successor.release());
        drop(successor);
        assert!(retained);
        assert!(replacement_refused);
        assert!(closed);
        assert!(materialized.is_err());
        assert!(old_leases_released);
        assert_eq!(successor_leases, 1);
        assert_eq!(fixture.query_leases(), 0);
    }

    #[test]
    fn an_admitted_participation_has_its_original_resources_before_its_first_task() {
        let fixture = Fixture::new();
        let participation = participation(81_001);
        assert!(
            fixture
                .host
                .query_context(participation.query_execution_id())
                .is_err()
        );
        fixture
            .establish_with_options(
                participation,
                Vec::new(),
                no_contribution(),
                proto::QueryOptions {
                    query_mem_limit: 4096,
                    ..Default::default()
                },
                &credential(1, ANNOUNCED_SCOPE, live_until()),
            )
            .expect("admitted participation initializes its real execution resources");
        let tracker = fixture
            .host
            .queries
            .type_construction_tracker(
                &fixture
                    .host
                    .query_context(participation.query_execution_id())
                    .unwrap(),
                Some(4096),
            )
            .expect("the first task can borrow the already-established tracker");
        let installed = fixture.host.active_participation(participation).unwrap();
        let control = super::ParticipationTypeConstructionControl(&installed);
        let creator = crate::native_type_construction::NativeTypeConstruction::try_with_control(
            tracker.clone(),
            &control,
        )
        .unwrap();
        let held = tracker.current();
        assert!(held > 0);
        fixture.release(participation);
        assert!(matches!(
            creator.request_layout(std::alloc::Layout::new::<u64>()),
            Err(
                crate::native_type_construction::NativeTypeConstructionError::Control(
                    novarocks_type_contract::CompileControlError::Cancelled,
                )
            )
        ));
        assert_eq!(
            tracker.current(),
            held,
            "release does not manufacture construction exit"
        );
        drop(creator);
        assert_eq!(tracker.current(), 0);
        assert!(
            fixture
                .host
                .query_context(participation.query_execution_id())
                .is_err()
        );
    }

    #[test]
    fn later_tasks_borrow_the_same_instance_resources_and_metrics_follow_actual_guards() {
        let fixture = Fixture::new();
        let participation = participation(81_011);
        let execution = participation.query_execution_id();
        assert_eq!(
            fixture.host.query_resource_snapshot(),
            NativeQueryExecutionResourceSnapshot::default()
        );
        fixture
            .establish_with_options(
                participation,
                Vec::new(),
                no_contribution(),
                proto::QueryOptions {
                    query_mem_limit: 4096,
                    ..Default::default()
                },
                &empty_credential(),
            )
            .unwrap();
        let context = fixture.host.query_context(execution).unwrap();
        let weak = Arc::downgrade(&context);
        let account = context.memory_binding().account().id();
        let first = fixture
            .host
            .queries
            .prepare_query_memory_typed(&context, Some(4096))
            .unwrap();
        assert_eq!(
            fixture.host.query_resource_snapshot(),
            NativeQueryExecutionResourceSnapshot {
                active_contexts: 1,
                active_fragments: 1,
            }
        );
        drop(first);
        assert_eq!(fixture.host.query_resource_snapshot().active_fragments, 0);
        let later = fixture.host.query_context(execution).unwrap();
        assert!(Arc::ptr_eq(&context, &later));
        let later_task = fixture
            .host
            .queries
            .prepare_query_memory_typed(&later, None)
            .unwrap();
        assert_eq!(later_task.binding().account().id(), account);
        assert_eq!(
            later_task.binding().account().snapshot().policy_limit_bytes,
            Some(4096)
        );
        assert!(
            fixture
                .host
                .queries
                .prepare_query_memory_typed(&later, Some(8192))
                .is_err()
        );
        assert_eq!(
            fixture.host.query_resource_snapshot().active_fragments,
            1,
            "a rejected limit cannot create another actual Task reference"
        );
        assert_eq!(
            context
                .memory_binding()
                .account()
                .snapshot()
                .policy_limit_bytes,
            Some(4096)
        );
        drop(later_task);
        drop(later);
        fixture.release(participation);
        assert_eq!(
            fixture.host.query_resource_snapshot(),
            NativeQueryExecutionResourceSnapshot::default()
        );
        assert!(fixture.host.query_context(execution).is_err());
        assert!(
            fixture
                .instances
                .lock()
                .unwrap()
                .contains_key(&participation),
            "cold instance metadata is not an active QueryContext"
        );
        drop(context);
        assert!(
            weak.upgrade().is_none(),
            "Release relinquished the instance's original resource reference"
        );
    }

    /// One establish installs all three shared facts, or it is not an establish.
    #[test]
    fn an_establish_installs_catalogs_the_runtime_filter_and_the_credential_slot() {
        let fixture = Fixture::new();
        let participation = participation(1);
        fixture
            .establish(
                participation,
                vec![catalog_properties()],
                participant_contribution(1),
                &credential(1, ANNOUNCED_SCOPE, live_until()),
            )
            .expect("a complete establish");

        assert_eq!(fixture.handle_leases(), 1, "the catalog must be leased");
        assert_eq!(
            fixture
                .filter_factory
                .ledger
                .installs
                .load(Ordering::SeqCst),
            1
        );
        assert!(
            fixture
                .host
                .storage_resolver_for_participation(participation)
                .expect("an active participation resolves storage")
                .resolve_vended_s3(&storage_request("s3://bucket/a/file.parquet"))
                .is_ok(),
            "the installed credential must serve its own scope"
        );
    }

    #[test]
    fn an_establish_installs_the_query_options_for_every_task() {
        let fixture = Fixture::new();
        let participation = participation(1);
        let wire = proto::QueryOptions {
            query_mem_limit: 8192,
            pipeline_dop: 3,
            query_timeout: -1,
            runtime_filter_wait_timeout_ms: Some(0),
            ..proto::QueryOptions::default()
        };
        fixture
            .establish_with_options(
                participation,
                vec![catalog_properties()],
                no_contribution(),
                wire,
                &credential(1, ANNOUNCED_SCOPE, live_until()),
            )
            .expect("a complete establish");

        let options = fixture
            .host
            .query_options(participation.query_execution_id())
            .expect("the active participation owns its query options");
        assert_eq!(options.runtime().exec_mem_limit(), Some(8192));
        assert_eq!(options.runtime().pipeline_dop(), Some(3));
        assert_eq!(
            options.fingerprint(),
            WireContent::new(b"query-options", wire).fingerprint(),
            "the established participation retains the exact original wire witness"
        );

        fixture.release(participation);
        assert!(
            fixture
                .host
                .query_options(participation.query_execution_id())
                .is_err(),
            "released participations cannot supply query options"
        );
    }

    /// Release takes all three back, and a second release changes nothing.
    #[test]
    fn a_release_undoes_every_shared_fact_and_stays_idempotent() {
        let fixture = Fixture::new();
        let participation = participation(1);
        fixture
            .establish(
                participation,
                vec![catalog_properties()],
                participant_contribution(1),
                &credential(1, ANNOUNCED_SCOPE, live_until()),
            )
            .expect("a complete establish");

        fixture.release(participation);
        assert_eq!(fixture.query_leases(), 0, "catalog leases must be dropped");
        assert_eq!(
            fixture.filter_factory.ledger.closes.load(Ordering::SeqCst),
            1
        );
        assert!(
            fixture
                .host
                .storage_resolver_for_participation(participation)
                .is_none(),
            "a released participation has no credential authority"
        );

        fixture.release(participation);
        assert_eq!(
            fixture.filter_factory.ledger.closes.load(Ordering::SeqCst),
            1,
            "a second release must not close the participant again"
        );
        assert_eq!(fixture.query_leases(), 0);
    }

    /// The release is what seals the participant's terminal observation, and
    /// it is the only message that carries it.
    ///
    /// The defect this catches: a task-protocol release that closes the
    /// participant without ever sealing it. Every part of the projection is
    /// individually correct and unit-tested through the retired lifecycle, so
    /// nothing else fails -- the frontend simply never receives a
    /// contribution, and every runtime-filter convergence fact reads as
    /// absent. This drives the production `release`, not a helper.
    #[test]
    fn a_release_seals_the_runtime_filter_observation_and_reports_it() {
        let fixture = Fixture::new();
        let participation = participation(1);
        fixture
            .establish(
                participation,
                vec![catalog_properties()],
                participant_contribution(1),
                &credential(1, ANNOUNCED_SCOPE, live_until()),
            )
            .expect("a complete establish");

        let evidence = fixture.release(participation);
        let telemetry = release_runtime_filter_telemetry(&evidence)
            .expect("the sealed content has a Backend projection")
            .expect("a release that held a participant reports its observation");
        assert!(
            telemetry.available().is_some(),
            "a sealed participant reports an available contribution, not an \
             unavailable reason: {telemetry:?}"
        );
    }

    /// A backend that installed no participant reports no contribution, which
    /// is a different fact from an empty one.
    #[test]
    fn a_release_without_a_participant_reports_no_contribution() {
        let fixture = Fixture::new();
        let participation = participation(1);
        fixture
            .establish(
                participation,
                vec![catalog_properties()],
                no_contribution(),
                &credential(1, ANNOUNCED_SCOPE, live_until()),
            )
            .expect("a complete establish");

        assert!(
            fixture.release(participation).runtime_filter().is_none(),
            "a query with no runtime filter on this backend must not report an \
             empty contribution"
        );
    }

    /// A failure in the last install step must not strand the earlier ones.
    #[test]
    fn a_failed_catalog_install_leaves_no_credential_and_no_participant_behind() {
        let fixture = Fixture::new();
        fixture.catalog_factory.fail.store(true, Ordering::SeqCst);
        let participation = participation(1);

        let rejection = fixture
            .establish(
                participation,
                vec![catalog_properties()],
                participant_contribution(1),
                &credential(1, ANNOUNCED_SCOPE, live_until()),
            )
            .expect_err("an unbindable catalog fails the establish");
        assert_eq!(rejection.category(), TaskFailureCategory::Execution);

        assert_eq!(fixture.query_leases(), 0);
        assert_eq!(
            fixture.filter_factory.ledger.closes.load(Ordering::SeqCst),
            1,
            "the participant installed before the failing step must be closed"
        );
        assert!(
            fixture
                .host
                .storage_resolver_for_participation(participation)
                .is_none(),
            "a refused establish must not leave credential material resident"
        );
    }

    /// A release that lands while `materialize` is inside a provider bind is
    /// the race the trait exists to describe: the establish must unwind and
    /// leave no lease, no participant, and no resident secret.
    #[test]
    fn a_release_racing_materialize_cancels_the_install_and_retains_nothing() {
        let fixture = Arc::new(Fixture::new());
        let entered = Arc::new(Barrier::new(2));
        let proceed = Arc::new(Barrier::new(2));
        *fixture
            .catalog_factory
            .entered
            .lock()
            .expect("entered gate") = Some(Arc::clone(&entered));
        *fixture
            .catalog_factory
            .proceed
            .lock()
            .expect("proceed gate") = Some(Arc::clone(&proceed));

        let participation = participation(1);
        let establishing = {
            let fixture = Arc::clone(&fixture);
            std::thread::spawn(move || {
                fixture.establish(
                    participation,
                    vec![catalog_properties()],
                    participant_contribution(1),
                    &credential(1, ANNOUNCED_SCOPE, live_until()),
                )
            })
        };

        // The install is now inside the provider bind.
        entered.wait();
        fixture.release(participation);
        proceed.wait();

        let rejection = establishing
            .join()
            .expect("the establishing thread must not panic")
            .expect_err("a released participation cannot finish establishing");
        use novarocks_execution_contract::failure::{
            FailureClass, FailureEvidence, FailureProjection,
        };
        assert_eq!(rejection.failure_fact().class(), FailureClass::Control);
        assert_eq!(
            rejection.failure_fact().evidence(),
            FailureEvidence::StopObservation
        );
        assert_eq!(rejection.category(), TaskFailureCategory::Execution);
        assert!(
            rejection.detail().as_str().contains("released"),
            "{rejection}"
        );

        assert_eq!(
            fixture.query_leases(),
            0,
            "a cancelled install must not leave a catalog leased"
        );
        assert_eq!(
            fixture.filter_factory.ledger.closes.load(Ordering::SeqCst),
            1,
            "the participant installed before the cancellation must be closed exactly once"
        );
        assert!(
            fixture
                .host
                .storage_resolver_for_participation(participation)
                .is_none()
        );
    }

    /// A release that arrives before its establish is called exactly once by
    /// the owner, so the establish that follows must refuse rather than install
    /// facts nothing will take back.
    #[test]
    fn a_task_that_binds_a_filter_is_refused_when_its_participation_installed_none() {
        use super::super::execution_host::TaskParticipationFacts;

        // Answering `None` here would let the scan run unfiltered and call the
        // result correct. A query that installs no filter on this backend is
        // the ordinary case and must still be answered `None`, so the two are
        // distinguished by what the task's own plan binds.
        let fixture = Fixture::new();
        let participation = participation(1);
        fixture
            .establish(
                participation,
                vec![catalog_properties()],
                no_contribution(),
                &credential(1, ANNOUNCED_SCOPE, live_until()),
            )
            .expect("an establish with no filter participant is legal");

        let execution = participation.query_execution_id();
        let task_key = novarocks_types::UniqueId::new(4, 5);
        assert!(
            fixture
                .host
                .runtime_filter_session(execution, task_key, false)
                .expect("a task that binds none is answered")
                .is_none()
        );
        match fixture
            .host
            .runtime_filter_session(execution, task_key, true)
        {
            Err(refusal) => assert_eq!(
                refusal.code(),
                novarocks_worker::RuntimeFilterContractErrorCode::InvalidContract
            ),
            Ok(_) => panic!("a task that binds a filter has nothing to bind to"),
        }
    }

    /// A compiled task names the exact bindings its program binds. With no
    /// participant, or with one that installed nothing for its instance, only
    /// a program that binds nothing is answered, and it is answered `None`.
    #[test]
    fn a_compiled_task_binds_exactly_the_filters_its_participation_installed_for_it() {
        use super::super::execution_host::TaskParticipationFacts;
        use std::collections::BTreeSet;

        let task_key = novarocks_types::UniqueId::new(4, 5);
        for contribution in [no_contribution(), participant_contribution(1)] {
            let fixture = Fixture::new();
            let participation = participation(1);
            fixture
                .establish(
                    participation,
                    vec![catalog_properties()],
                    contribution,
                    &credential(1, ANNOUNCED_SCOPE, live_until()),
                )
                .expect("the establish is legal");
            let execution = participation.query_execution_id();
            assert!(
                fixture
                    .host
                    .runtime_filter_session_for_bindings(execution, task_key, &BTreeSet::new())
                    .expect("a program that binds none is answered")
                    .is_none()
            );
            match fixture.host.runtime_filter_session_for_bindings(
                execution,
                task_key,
                &BTreeSet::from([3]),
            ) {
                Err(refusal) => {
                    assert_eq!(
                        refusal.code(),
                        novarocks_worker::RuntimeFilterContractErrorCode::InvalidContract
                    );
                    assert!(
                        refusal.detail().contains("runtime filters {3}"),
                        "the refusal names the program's bindings: {refusal}"
                    );
                }
                Ok(_) => panic!("binding 3 was never installed for this instance"),
            }
        }
    }

    #[test]
    fn storage_credentials_are_refused_rather_than_defaulted() {
        use super::super::execution_host::TaskParticipationFacts;

        // There is no process-level credential a scan could legitimately fall
        // back to, so an unestablished or released participation must fail the
        // task's preparation instead of deferring the failure to read time,
        // where it would surface as an object-store error with no context.
        let fixture = Fixture::new();
        let participation = participation(1);
        let execution = participation.query_execution_id();

        let assert_refusal = |refusal: novarocks_worker::HostRejection| {
            use novarocks_execution_contract::failure::{
                FailureClass, FailureFact, FailureProjection, FailureReason, domain,
            };
            let expected = FailureFact::new(
                FailureClass::Dependency,
                FailureReason::new(domain::NATIVE, std::num::NonZeroU16::new(9012).unwrap()),
            );
            assert_eq!(refusal.category(), TaskFailureCategory::Execution);
            assert_eq!(refusal.failure_fact(), expected);
            let author =
                crate::backend_test_support::find_source::<super::ParticipationFactsFailure>(
                    &refusal,
                )
                .expect("the storage lookup's original domain cause remains local");
            assert!(
                matches!(author, super::ParticipationFactsFailure::StorageParticipationUnavailable(actual)
                if *actual == execution)
            );
            assert_eq!(author.failure_fact(), expected);
            let detail =
                format!("no established participation holds storage credentials for {execution:?}");
            assert_eq!(author.to_string(), detail);
            assert_eq!(refusal.detail().as_str(), detail);
            assert!(!refusal.detail().as_str().contains("access-key"));
            let carrier = crate::backend_test_support::find_source::<
                novarocks_execution::runtime::task::ExecutionFailure,
            >(&refusal)
            .expect("the host retains its exact typed execution carrier");
            assert_eq!(carrier.failure_fact(), expected);
            assert_eq!(carrier.detail(), detail);
            assert_eq!(
                carrier.safe_detail().as_str(),
                "Dependency failure (reason 13:9012)"
            );
        };
        match TaskParticipationFacts::storage_resolver(fixture.host.as_ref(), execution) {
            Err(refusal) => assert_refusal(refusal),
            Ok(_) => panic!("an unestablished participation holds no credentials"),
        }

        fixture
            .establish(
                participation,
                vec![catalog_properties()],
                no_contribution(),
                &credential(1, ANNOUNCED_SCOPE, live_until()),
            )
            .expect("establish");
        assert!(TaskParticipationFacts::storage_resolver(fixture.host.as_ref(), execution).is_ok());

        fixture.release(participation);
        match TaskParticipationFacts::storage_resolver(fixture.host.as_ref(), execution) {
            Err(refusal) => assert_refusal(refusal),
            Ok(_) => panic!("a released participation holds no credentials either"),
        }
    }

    #[test]
    fn an_unreadable_establish_after_a_release_leaves_no_marker_behind() {
        // A released marker is consumed only by the materialize that answers
        // it. `participations` lives as long as the process and a participation reference
        // is distinct per attempt, so an early return that skipped the marker
        // would leak one entry per occurrence — reachable only through a
        // protocol bug, which is exactly when unbounded growth is worst.
        let fixture = Fixture::new();
        let participation = participation(1);
        fixture.release(participation);

        // A catalog payload where the runtime filter belongs: well-formed, and
        // not what this domain asked for, so the projection refuses.
        let wrong_domain = catalog_payload(vec![catalog_properties()]);
        let options = query_options_payload();
        let refusal = fixture
            .host
            .materialize(SharedFactsRequest::new(
                participation,
                &wrong_domain,
                &wrong_domain,
                &options,
                &credential(1, ANNOUNCED_SCOPE, live_until()),
            ))
            .expect_err("a payload of the wrong domain is refused");
        assert!(!refusal.detail().as_str().contains("access-key"));

        assert!(
            fixture
                .host
                .participations
                .lock()
                .expect("participation map")
                .get(participation)
                .is_none(),
            "a refused establish must take the marker with it"
        );
    }

    #[test]
    fn an_establish_after_its_participation_was_released_installs_nothing() {
        let fixture = Fixture::new();
        let participation = participation(1);

        fixture.release(participation);
        let rejection = fixture
            .legacy_establish(
                participation,
                vec![catalog_properties()],
                participant_contribution(1),
                &credential(1, ANNOUNCED_SCOPE, live_until()),
            )
            .expect_err("a released participation cannot be established");
        assert_eq!(rejection.category(), TaskFailureCategory::Execution);

        assert_eq!(fixture.query_leases(), 0);
        assert_eq!(
            fixture
                .filter_factory
                .ledger
                .installs
                .load(Ordering::SeqCst),
            0,
            "no participant may be built for a participation that was already released"
        );
        assert_eq!(
            fixture.catalog_factory.binds.load(Ordering::SeqCst),
            0,
            "no provider may be bound for a participation that was already released"
        );
        assert!(
            fixture
                .host
                .storage_resolver_for_participation(participation)
                .is_none()
        );
    }

    /// The empty contribution is how a query with no runtime filter establishes.
    #[test]
    fn a_contribution_that_names_no_participant_installs_none() {
        let fixture = Fixture::new();
        let participation = participation(1);
        fixture
            .establish(
                participation,
                vec![],
                no_contribution(),
                &empty_credential(),
            )
            .expect("a query with no runtime filter still establishes");
        assert_eq!(
            fixture
                .filter_factory
                .ledger
                .installs
                .load(Ordering::SeqCst),
            0
        );

        fixture.release(participation);
        assert_eq!(
            fixture.filter_factory.ledger.closes.load(Ordering::SeqCst),
            0,
            "nothing was installed, so nothing may be closed"
        );
    }

    /// The contradiction between the two must not be resolved by dropping the
    /// install.
    #[test]
    fn a_contribution_with_an_install_but_no_participant_id_is_refused() {
        let fixture = Fixture::new();
        let contribution = proto::RuntimeFilterContribution {
            participant_id: 0,
            lifecycle: Some(lifecycle_options()),
            install: Some(filter::RuntimeFilterParticipantInstall::default()),
        };
        let rejection = fixture
            .establish(participation(1), vec![], contribution, &empty_credential())
            .expect_err("a participant-less install is a contradiction");
        assert_eq!(rejection.category(), TaskFailureCategory::Protocol);
        assert!(
            rejection.detail().as_str().contains("names no participant"),
            "{rejection}"
        );
    }

    /// A payload this process cannot read back must fail before any install.
    #[test]
    fn a_payload_of_the_wrong_domain_fails_before_anything_is_installed() {
        let fixture = Fixture::new();
        let participation = participation(1);
        // A catalog set where the runtime filter belongs.
        let catalog = catalog_payload(vec![catalog_properties()]);
        let filter = catalog_payload(vec![]);
        let options = query_options_payload();
        let credential = credential(1, ANNOUNCED_SCOPE, live_until());
        let rejection = fixture
            .host
            .materialize(SharedFactsRequest::new(
                participation,
                &catalog,
                &filter,
                &options,
                &credential,
            ))
            .expect_err("a catalog set is not a participant contribution");
        assert_eq!(rejection.category(), TaskFailureCategory::Internal);

        assert_eq!(
            fixture.catalog_factory.binds.load(Ordering::SeqCst),
            0,
            "no provider may be bound for a request that cannot be projected"
        );
        assert_eq!(fixture.query_leases(), 0);
        assert!(
            fixture
                .host
                .storage_resolver_for_participation(participation)
                .is_none()
        );
    }

    /// A second establish must not silently replace the first participation's facts.
    #[test]
    fn a_second_materialize_for_a_live_participation_is_an_invariant_violation() {
        let fixture = Fixture::new();
        let participation = participation(1);
        fixture
            .establish(
                participation,
                vec![catalog_properties()],
                participant_contribution(1),
                &credential(1, ANNOUNCED_SCOPE, live_until()),
            )
            .expect("the first establish");

        let rejection = fixture
            .establish(
                participation,
                vec![catalog_properties()],
                participant_contribution(1),
                &credential(1, ANNOUNCED_SCOPE, live_until()),
            )
            .expect_err("a live participation cannot be materialized twice");
        assert_eq!(rejection.category(), TaskFailureCategory::Internal);
        assert_eq!(
            fixture
                .filter_factory
                .ledger
                .installs
                .load(Ordering::SeqCst),
            1,
            "the refused establish must not have built a second participant"
        );
        // The first participation is untouched.
        assert!(
            fixture
                .host
                .storage_resolver_for_participation(participation)
                .is_some()
        );
    }

    /// A rotation replaces only the credential slot.
    #[test]
    fn a_credential_rotation_replaces_the_slot_and_touches_nothing_else() {
        let fixture = Fixture::new();
        let participation = participation(1);
        let expiry = live_until();
        fixture
            .establish(
                participation,
                vec![catalog_properties()],
                participant_contribution(1),
                &credential(1, ANNOUNCED_SCOPE, expiry),
            )
            .expect("establish");
        let binds_before = fixture.catalog_factory.binds.load(Ordering::SeqCst);

        fixture
            .advance_shared_domain(
                participation,
                &ParticipationDomainUpdate::Credential(credential(2, ANNOUNCED_SCOPE, expiry)),
            )
            .expect("an exact-next-epoch rotation installs");

        assert_eq!(
            fixture.catalog_factory.binds.load(Ordering::SeqCst),
            binds_before,
            "a rotation must not call the catalog manager"
        );
        assert_eq!(
            fixture
                .filter_factory
                .ledger
                .installs
                .load(Ordering::SeqCst),
            1,
            "a rotation must not reinstall the runtime filter"
        );
        assert!(
            fixture
                .host
                .storage_resolver_for_participation(participation)
                .expect("still active")
                .resolve_vended_s3(&storage_request("s3://bucket/a/file.parquet"))
                .is_ok()
        );
    }

    /// A refused rotation must leave the announcement that was already serving.
    #[test]
    fn a_refused_rotation_leaves_the_installed_announcement_serving() {
        struct ForeignHandle;

        impl ConfidentialContent for ForeignHandle {
            fn encoded_len(&self) -> usize {
                0
            }

            fn matches(&self, _other: &dyn ConfidentialContent) -> bool {
                false
            }
        }

        let fixture = Fixture::new();
        let participation = participation(1);
        fixture
            .establish(
                participation,
                vec![],
                no_contribution(),
                &credential(1, ANNOUNCED_SCOPE, live_until()),
            )
            .expect("establish");

        // A handle this codec did not produce cannot be projected, so the
        // rotation is refused before it can touch the slot.
        let rejection = fixture
            .advance_shared_domain(
                participation,
                &ParticipationDomainUpdate::Credential(CredentialUpdate::new(
                    CredentialDomainId::new(1),
                    CredentialEpoch::new(2).expect("nonzero epoch"),
                    Arc::new(ForeignHandle) as Arc<dyn ConfidentialContent>,
                )),
            )
            .expect_err("a foreign credential handle is refused");
        assert_eq!(rejection.category(), TaskFailureCategory::Internal);

        assert!(
            fixture
                .host
                .storage_resolver_for_participation(participation)
                .expect("still active")
                .resolve_vended_s3(&storage_request("s3://bucket/a/file.parquet"))
                .is_ok(),
            "a refused rotation must not empty the slot"
        );
    }

    /// A catalog binding advance replaces a set this backend can only release
    /// as a whole, so applying it as an addition would keep a removed catalog
    /// resolvable.
    #[test]
    fn a_catalog_binding_advance_is_refused_rather_than_applied_as_an_addition() {
        let fixture = Fixture::new();
        let participation = participation(1);
        fixture
            .establish(
                participation,
                vec![],
                no_contribution(),
                &empty_credential(),
            )
            .expect("establish");

        let rejection = fixture
            .advance_shared_domain(
                participation,
                &ParticipationDomainUpdate::CatalogBinding {
                    version: DomainVersion::new(2).expect("nonzero"),
                    payload: catalog_payload(vec![catalog_properties()]),
                },
            )
            .expect_err("a catalog replacement has no representation here");
        assert_eq!(rejection.category(), TaskFailureCategory::Protocol);
        assert_eq!(
            fixture.query_leases(),
            0,
            "the refused advance must not have leased the new catalog"
        );
    }

    /// The shared filter domain installs the participant an establish did not,
    /// and never replaces one that is already sealed for the attempt.
    #[test]
    fn a_shared_filter_advance_installs_once_and_refuses_to_replace_a_sealed_participant() {
        let fixture = Fixture::new();
        let participation = participation(1);
        fixture
            .establish(
                participation,
                vec![],
                no_contribution(),
                &empty_credential(),
            )
            .expect("establish with no participant");

        fixture
            .advance_shared_domain(
                participation,
                &ParticipationDomainUpdate::SharedDynamicFilter {
                    version: DomainVersion::new(2).expect("nonzero"),
                    payload: filter_payload(participant_contribution(1)),
                },
            )
            .expect("the first shared filter advance installs the participant");
        assert_eq!(
            fixture
                .filter_factory
                .ledger
                .installs
                .load(Ordering::SeqCst),
            1
        );

        let rejection = fixture
            .advance_shared_domain(
                participation,
                &ParticipationDomainUpdate::SharedDynamicFilter {
                    version: DomainVersion::new(3).expect("nonzero"),
                    payload: filter_payload(participant_contribution(1)),
                },
            )
            .expect_err("a sealed participant cannot be replaced");
        assert_eq!(rejection.category(), TaskFailureCategory::Protocol);
        assert_eq!(
            fixture
                .filter_factory
                .ledger
                .installs
                .load(Ordering::SeqCst),
            1,
            "the refusal must be decided before a second participant is built"
        );

        fixture.release(participation);
        assert_eq!(
            fixture.filter_factory.ledger.closes.load(Ordering::SeqCst),
            1,
            "the participant an advance installed must be released like any other"
        );
    }

    /// A running task can name only its query execution, so the execution index
    /// has to resolve exactly the live participation — and nothing that has no facts.
    #[test]
    fn the_execution_index_resolves_only_a_live_participation() {
        let fixture = Fixture::new();
        let live = participation(1);
        assert_eq!(
            fixture.host.participation_for_execution(execution_id(1)),
            None
        );

        fixture
            .establish(live, vec![], no_contribution(), &empty_credential())
            .expect("establish");
        assert_eq!(
            fixture.host.participation_for_execution(execution_id(1)),
            Some(live)
        );

        fixture.release(live);
        assert_eq!(
            fixture.host.participation_for_execution(execution_id(1)),
            None,
            "a released participation must not stay resolvable by a task"
        );

        // A release that arrives before its establish leaves a marker behind.
        // A marker carries no facts, so it must never be resolvable either.
        fixture.release(participation(2));
        assert_eq!(
            fixture.host.participation_for_execution(execution_id(2)),
            None
        );
    }

    /// An advance for a participation this host holds no facts for must fail closed,
    /// and must not claim to know whether it lost a race or found a bug.
    #[test]
    fn an_advance_for_a_participation_without_shared_facts_is_refused() {
        let fixture = Fixture::new();
        let rejection = fixture
            .advance_shared_domain(
                participation(1),
                &ParticipationDomainUpdate::Credential(empty_credential()),
            )
            .expect_err("a participation with no shared facts cannot take an advance");
        assert_eq!(rejection.category(), TaskFailureCategory::Execution);

        // A released participation is the same answer: an advance must never install
        // credential material into a participation that has been torn down.
        let released = participation(2);
        fixture
            .establish(released, vec![], no_contribution(), &empty_credential())
            .expect("establish");
        fixture.release(released);
        assert!(
            fixture
                .advance_shared_domain(
                    released,
                    &ParticipationDomainUpdate::Credential(credential(
                        1,
                        ANNOUNCED_SCOPE,
                        live_until()
                    )),
                )
                .is_err(),
            "a released participation must not accept a rotation"
        );
        assert!(
            fixture
                .host
                .storage_resolver_for_participation(released)
                .is_none()
        );
    }

    /// A close that does not complete must still leave the participation releasable.
    #[test]
    fn a_participant_whose_close_fails_is_still_dropped_and_release_stays_idempotent() {
        let fixture = Fixture::new();
        let participation = participation(1);
        fixture
            .filter_factory
            .ledger
            .fail_close
            .store(true, Ordering::SeqCst);
        fixture
            .establish(
                participation,
                vec![catalog_properties()],
                participant_contribution(1),
                &credential(1, ANNOUNCED_SCOPE, live_until()),
            )
            .expect("establish");

        fixture.release(participation);
        assert_eq!(
            fixture.filter_factory.ledger.closes.load(Ordering::SeqCst),
            1
        );
        assert_eq!(
            fixture.query_leases(),
            0,
            "a failed close must not hold the catalog leases open"
        );
        assert!(
            fixture
                .host
                .storage_resolver_for_participation(participation)
                .is_none()
        );

        fixture.release(participation);
        assert_eq!(
            fixture.filter_factory.ledger.closes.load(Ordering::SeqCst),
            1,
            "a failed close must not be retried blindly"
        );
    }

    /// The first submitted task of a participation becomes its feedback carrier.
    #[test]
    fn the_first_submitted_task_carries_the_participation_dynamic_filter_feedback() {
        // The defect this catches: nothing installed a feedback sink on the
        // participant, so a reduced filter domain was published into a sink
        // that did not exist and no frontend ever learned about it. The
        // symptom is unpruned connector enumeration with identical rows, which
        // no result assertion can see.
        //
        // It also pins first-wins. A second carrier would replace the sink on
        // every channel session, so which task publishes a domain would depend
        // on submission order and the frontend would be polling a task that no
        // longer publishes.
        let fixture = Fixture::new();
        let participation = participation(1);
        let first = task_identity(participation, 1);
        let second = task_identity(participation, 2);
        let execution = participation.query_execution_id();
        let original = NativeNodePreparation::with_participation_each(
            participation,
            fixture.host.clone(),
            ParticipationContents {
                catalog: catalog_payload(vec![]),
                filters: filter_payload(participant_contribution(1)),
                options: query_options_payload(),
                credential: credential(1, ANNOUNCED_SCOPE, live_until()),
            },
            &[first, second],
            |ordinal, original| {
                if ordinal == 0 {
                    assert_eq!(
                        fixture.host.feedback_carrier(execution),
                        None,
                        "an established participation with no submitted task carries nothing yet"
                    );
                    assert!(
                        fixture.host.bind_runtime_filter_feedback(
                            execution,
                            first,
                            original.dynamic_filter_writer()
                        ),
                        "the first submitted task takes the carrier role"
                    );
                } else {
                    assert_eq!(ordinal, 1);
                    assert!(
                        !fixture.host.bind_runtime_filter_feedback(
                            execution,
                            second,
                            original.dynamic_filter_writer()
                        ),
                        "a second task must not replace the carrier"
                    );
                }
                assert_eq!(
                    fixture.host.feedback_carrier(execution),
                    Some(first),
                    "the carrier is first-wins, not last-writer"
                );
            },
        );
        // Releasing the participation drops the only strong reference to the sink,
        // so a late publication finds nothing to publish through.
        NativeNodePreparation::release_participation(&original);
        assert_eq!(fixture.host.feedback_carrier(execution), None);
    }

    /// A query with no participant on this backend has nothing to carry.
    #[test]
    fn a_participation_without_a_participant_declines_the_carrier_role() {
        let fixture = Fixture::new();
        let participation = participation(1);
        let task = task_identity(participation, 1);
        let original = NativeNodePreparation::with_participation(
            participation,
            fixture.host.clone(),
            ParticipationContents {
                catalog: catalog_payload(vec![]),
                filters: filter_payload(no_contribution()),
                options: query_options_payload(),
                credential: credential(1, ANNOUNCED_SCOPE, live_until()),
            },
            &[task],
        );
        let execution = participation.query_execution_id();

        assert!(
            !fixture.host.bind_runtime_filter_feedback(
                execution,
                task,
                original[0].dynamic_filter_writer()
            ),
            "there is no channel that could ever reduce, so there is nothing to carry"
        );
        assert_eq!(fixture.host.feedback_carrier(execution), None);
        NativeNodePreparation::release_participation(&original);
    }

    /// One task of a participation, on the exact backend process that participation names.
    fn task_identity(participation: ParticipationRef, task: u32) -> TaskIdentity {
        TaskIdentity::new(
            participation.query_execution_id(),
            novarocks_types::identity::StageId::new(1).expect("nonzero stage"),
            novarocks_types::identity::TaskId::new(task).expect("nonzero task"),
            participation.backend_process_id(),
        )
    }

    /// No credential material exists to render, and none may be invented.
    ///
    /// This used to prove that secret material never reached a rendering or a
    /// rejection. Material no longer crosses this boundary at all (CAD-1 D1),
    /// so what it proves now is the stronger fact: an established participation holds
    /// an announcement and nothing that looks like a credential.
    #[test]
    fn a_credential_domain_carries_no_material_to_render() {
        let fixture = Fixture::new();
        let participation = participation(1);
        fixture
            .establish(
                participation,
                vec![],
                no_contribution(),
                &credential(1, ANNOUNCED_SCOPE, live_until()),
            )
            .expect("establish");

        for rendered in [
            format!("{:?}", fixture.host),
            format!("{:?}", credential(3, ROTATED_SCOPE, live_until())),
        ] {
            for token in ["access-key", "session-token", "secret-access"] {
                assert!(
                    !rendered.contains(token),
                    "a credential-shaped token appeared in a rendering: {rendered}"
                );
            }
        }
    }
}
