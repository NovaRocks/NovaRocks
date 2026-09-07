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

//! The production query-context side of execution.
//!
//! One establish installs four shared facts on this backend — query options,
//! catalog runtimes, a runtime-filter participant, and the query's vended
//! credentials — and one release takes all four back. This is the only place that owns that
//! pairing, and it owns nothing else: the owner in [`super::registry`] decides
//! *whether* a context establishes, advances, or retires, and this host only
//! carries out the decision it was handed.
//!
//! # Why every install is mirrored by per-context state
//!
//! [`QueryContextHost::materialize`] runs outside the owner's lock, while the
//! context is `Establishing` and already racing its sequence-zero lease, and
//! [`QueryContextHost::release`] can run *concurrently with it*: the owner's
//! state machine admits `Establishing -> Aborting`, and a context with no tasks
//! completes that abort immediately. So a release is not a "later" event that
//! can read whatever materialize finished — it is a peer that has to undo an
//! install still in flight.
//!
//! That is what the per-context record is for. Every installed fact is
//! published into it as soon as it exists, `released` is the one flag both
//! sides read, and each side only ever tears down what it took out of the
//! record. Whoever loses the race unwinds; nothing is closed twice, and nothing
//! is left behind.
//!
//! # Lock discipline
//!
//! The per-context lock is never held across a call into the catalog manager,
//! the runtime-filter participant, or the credential slot. Each of those has
//! its own lock, and the catalog manager calls back into this host through the
//! `active` predicate of an in-flight install, so holding both would invert the
//! order and deadlock.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use novarocks_connector_binding::ConnectorMaterializationErrorClass;
use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_execution::task_execution::domain::ContentFingerprint;
use novarocks_execution::task_execution::identity::{QueryContextRef, TaskIdentity};
use novarocks_execution::task_execution::operation::QueryContextDomainUpdate;
use novarocks_execution::task_execution::status::TaskFailureCategory;
use novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry;
use novarocks_proto_codec::lifecycle::{QueryTerminationReason, RuntimeFilterContribution};
use novarocks_proto_codec::task_execution::domain::WireCredential;
use novarocks_proto_models::novarocks as proto;
use novarocks_spi::connector::{CatalogProperties, ConnectorStorageResolver};
use novarocks_types::QueryExecutionId;
use tracing::error;

use super::credential_slot::QueryContextCredentialSlot;
use super::execution_host::QueryContextOptions;
use super::feedback::TaskRuntimeFilterFeedbackEgress;
use super::host::{HostRejection, QueryContextHost, ReleasedContextEvidence, SharedFactsRequest};
use super::shared_facts::{
    catalog_bindings, credential_material, query_options, runtime_filter_install,
};
use super::status::TaskStatusReporter;
use crate::BackendDataRuntime;
use crate::connector::ConnectorExecutionRoleBinding;
use crate::connector::catalog_manager::{
    CatalogManager, CatalogManagerError, ConnectorExecutionRoleBindingFactorySet,
};
use crate::runtime_filter::domain::BackendFrontendFeedbackSink;
use crate::runtime_filter::error::{RuntimeFilterContractError, RuntimeFilterContractErrorCode};
use crate::runtime_filter::install_decode::{
    DecodedRuntimeFilterContribution, decode_runtime_filter_contribution,
};
use crate::runtime_filter::participant::{
    RuntimeFilterParticipant, RuntimeFilterParticipantFactory,
};
use crate::runtime_filter::terminal_contribution::{
    RUNTIME_FILTER_TERMINAL_CAPTURE_STAGE, capture_terminal_profile_contribution,
};

/// The mutable half of one context's installed facts.
///
/// The credential slot is deliberately *not* in here: it owns its own lock and
/// is replaced atomically by a rotation, so putting it behind this mutex would
/// only serialise rotations against catalog installs for no reason.
#[derive(Default)]
struct ContextFacts {
    /// Set by whichever side retires the context first. An in-flight
    /// `materialize` reads it as its only cancellation signal — it has no
    /// deadline of its own, because the owner's `SharedFactsRequest` carries
    /// none.
    released: bool,
    /// The immutable query contract every task must match before preparation.
    query_options: Option<QueryContextOptions>,
    /// `None` either because the query installs no runtime filter on this
    /// backend, or because the side that tore the context down already took it.
    participant: Option<Arc<RuntimeFilterParticipant>>,
    /// The one task carrying this context's terminal filter feedback to the
    /// frontend, and the only strong reference to that sink.
    ///
    /// The participant holds it weakly, so this is what keeps it alive. Taking
    /// it at tear-down is what makes a released context stop publishing rather
    /// than keep a status reporter reachable from a background publisher.
    feedback: Option<Arc<TaskRuntimeFilterFeedbackEgress>>,
}

/// Everything one query context installed on this backend.
struct InstalledContext {
    credentials: Arc<QueryContextCredentialSlot>,
    facts: Mutex<ContextFacts>,
}

impl InstalledContext {
    fn establishing() -> Self {
        Self {
            credentials: Arc::new(QueryContextCredentialSlot::new()),
            facts: Mutex::new(ContextFacts::default()),
        }
    }

    /// A record for a context the owner released before it was materialized.
    ///
    /// See [`NativeQueryContextHost::release`] for why this exists.
    fn released() -> Self {
        Self {
            credentials: Arc::new(QueryContextCredentialSlot::new()),
            facts: Mutex::new(ContextFacts {
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

/// Every context this host has installed facts for, under both addresses.
///
/// A query context is named by its full reference, but a running task can only
/// name the query execution it belongs to. The owner fences one live context
/// reference per execution id, so the second index is exact rather than a
/// heuristic — and keeping it here means a task-side lookup is one map probe
/// instead of a scan over every live query.
#[derive(Default)]
struct HostContexts {
    by_reference: BTreeMap<QueryContextRef, Arc<InstalledContext>>,
    by_execution: BTreeMap<QueryExecutionId, QueryContextRef>,
}

impl HostContexts {
    fn insert(&mut self, context: QueryContextRef, installed: Arc<InstalledContext>) {
        self.by_reference.insert(context, installed);
        self.by_execution
            .insert(context.query_execution_id(), context);
    }

    /// Records that a context was released before it was ever materialized.
    ///
    /// It is deliberately not put in the execution index: a task must never
    /// resolve a context that carries no facts and never will.
    fn insert_released_marker(&mut self, context: QueryContextRef) {
        self.by_reference
            .insert(context, Arc::new(InstalledContext::released()));
    }

    fn remove(&mut self, context: QueryContextRef) -> Option<Arc<InstalledContext>> {
        let installed = self.by_reference.remove(&context);
        // Only if this exact reference still owns the execution id: a later
        // context must not lose its index because an older one was reclaimed.
        if self.by_execution.get(&context.query_execution_id()) == Some(&context) {
            self.by_execution.remove(&context.query_execution_id());
        }
        installed
    }

    /// Removes a record only while it is still the one the caller installed.
    ///
    /// An unwinding `materialize` must not evict whatever took its place.
    fn remove_if_same(&mut self, context: QueryContextRef, installed: &Arc<InstalledContext>) {
        if self
            .by_reference
            .get(&context)
            .is_some_and(|current| Arc::ptr_eq(current, installed))
        {
            self.remove(context);
        }
    }

    fn get(&self, context: QueryContextRef) -> Option<&Arc<InstalledContext>> {
        self.by_reference.get(&context)
    }
}

/// The backend's real [`QueryContextHost`].
pub struct NativeQueryContextHost {
    catalog_manager: Arc<CatalogManager<ConnectorExecutionRoleBinding>>,
    execution_role_binding_factories: Arc<ConnectorExecutionRoleBindingFactorySet>,
    runtime_filter_factory: Arc<dyn RuntimeFilterParticipantFactory>,
    catalog_install_runtime: BackendDataRuntime,
    contexts: Mutex<HostContexts>,
}

impl fmt::Debug for NativeQueryContextHost {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Only the count. Naming a context's credential slot here would put a
        // secret one derived `Debug` away, and the slot redacts itself anyway.
        let contexts = self
            .contexts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        formatter
            .debug_struct("NativeQueryContextHost")
            .field("installed_contexts", &contexts.by_reference.len())
            .field("material", &"[REDACTED]")
            .finish()
    }
}

impl NativeQueryContextHost {
    /// Crate-private because it names the backend-private runtime-filter
    /// participant factory: only this process's own composition can build one.
    pub(crate) fn new(
        catalog_manager: Arc<CatalogManager<ConnectorExecutionRoleBinding>>,
        execution_role_binding_factories: Arc<ConnectorExecutionRoleBindingFactorySet>,
        runtime_filter_factory: Arc<dyn RuntimeFilterParticipantFactory>,
        catalog_install_runtime: BackendDataRuntime,
    ) -> Self {
        Self {
            catalog_manager,
            execution_role_binding_factories,
            runtime_filter_factory,
            catalog_install_runtime,
            contexts: Mutex::new(HostContexts::default()),
        }
    }

    /// The vended-credential authority of one live query context.
    ///
    /// This is the read side of the credential slot: a connector reaching for
    /// scoped storage access resolves it through here, and a context that was
    /// never established or has already been released has none — there is no
    /// process-level credential to fall back to.
    /// The resolver one established context installed.
    ///
    /// Named apart from the `TaskQueryContextFacts` method of the same shape:
    /// that one is keyed by execution id and refuses when there is no context,
    /// and two same-named methods keyed differently is how a caller reaches
    /// the wrong one.
    pub fn storage_resolver_for_context(
        &self,
        context: QueryContextRef,
    ) -> Option<Arc<dyn ConnectorStorageResolver>> {
        let installed = self
            .contexts
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(context)
            .map(Arc::clone)?;
        if installed.is_released() {
            return None;
        }
        Some(Arc::clone(&installed.credentials) as Arc<dyn ConnectorStorageResolver>)
    }

    /// The live context reference of one query execution, if it has one.
    ///
    /// A running task names its query execution and nothing else, so this is
    /// how the task side of execution reaches the shared facts of the context
    /// it belongs to.
    /// The runtime-filter participant this query installed on this backend.
    ///
    /// `None` covers both "no filter on this backend" and "the context is
    /// gone"; the caller decides which of those is an error, because only it
    /// knows whether its plan binds a filter.
    fn participant_for_execution(
        &self,
        execution: QueryExecutionId,
    ) -> Option<Arc<RuntimeFilterParticipant>> {
        let context = self.context_for_execution(execution)?;
        let contexts = self
            .contexts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let installed = contexts.get(context)?;
        let facts = installed
            .facts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        facts.participant.clone()
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
        participant: crate::runtime_filter::domain::BackendParticipantIdentity,
    ) -> Option<Arc<RuntimeFilterParticipant>> {
        let execution = {
            let contexts = self
                .contexts
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            contexts.by_execution.keys().copied().find(|execution| {
                execution.query_id().high() == participant.query_id().high()
                    && execution.query_id().low() == participant.query_id().low()
                    && execution.attempt_id().get() == participant.deployment_epoch()
            })?
        };
        self.participant_for_execution(execution)
    }

    /// Which task carries this query context's dynamic filter feedback.
    ///
    /// This is the supply observable of the backend half of that loop: a sink
    /// that is built and never installed answers `None` here, and no test of
    /// the sink itself can see that.
    pub fn feedback_carrier(&self, execution: QueryExecutionId) -> Option<TaskIdentity> {
        let context = self.context_for_execution(execution)?;
        let contexts = self
            .contexts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let installed = contexts.get(context)?;
        let facts = installed
            .facts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        facts.feedback.as_ref().map(|sink| sink.as_ref().carrier())
    }

    pub fn context_for_execution(&self, execution: QueryExecutionId) -> Option<QueryContextRef> {
        self.contexts
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .by_execution
            .get(&execution)
            .copied()
    }

    /// Looks up a context the owner says is active.
    ///
    /// The two absences are different facts and are reported as such. A missing
    /// record is *either* a release that completed between the owner's
    /// classification and this call — the owner drops its lock in between — or a
    /// real disagreement about which contexts exist; this host cannot tell them
    /// apart, and calling an ordinary race an invariant violation would make
    /// `Internal` mean nothing. A record that is present and released can only
    /// be a pre-materialize marker, and the owner cannot have called a context
    /// with one of those `Active` — so that one really is an invariant
    /// violation.
    fn active_context(
        &self,
        context: QueryContextRef,
    ) -> Result<Arc<InstalledContext>, HostRejection> {
        let installed = self
            .contexts
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(context)
            .map(Arc::clone)
            .ok_or_else(|| {
                HostRejection::new(
                    TaskFailureCategory::Execution,
                    "the query context holds no shared facts: it was released, or it was never \
                     materialized",
                )
            })?;
        if installed.is_released() {
            return Err(internal(
                "query context is active but its shared facts were released before they were \
                 installed",
            ));
        }
        Ok(installed)
    }

    /// Undoes every installed fact of one context, exactly once per fact.
    ///
    /// Ordering is deliberate: the credential slot is emptied first so the
    /// window in which secret material is resident is as short as this host can
    /// make it, then the participant is closed, then the catalog leases are
    /// dropped. None of them depend on each other, and by the time a release
    /// runs the owner has already stood every task down.
    fn tear_down(
        &self,
        context: QueryContextRef,
        installed: &InstalledContext,
    ) -> ReleasedContextEvidence {
        let participant = {
            let mut facts = installed
                .facts
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            facts.released = true;
            facts.query_options = None;
            // Dropped with the participant: it is the only strong reference to
            // the feedback sink, so releasing the context is what makes a late
            // publication find nothing to publish through.
            facts.feedback = None;
            facts.participant.take()
        };
        installed.credentials.clear();
        let evidence = match participant {
            Some(participant) => {
                let evidence = seal_runtime_filter_evidence(context, &participant);
                close_participant(context, &participant);
                evidence
            }
            None => ReleasedContextEvidence::none(),
        };
        // Unconditional: a lease may have been taken by an install that is
        // still unwinding, and releasing a query that holds none is a no-op.
        self.catalog_manager
            .release_query(context.query_execution_id());
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
        let snapshot = self.catalog_manager.lease_snapshot();
        crate::metrics::publish_backend_query_execution_resource(
            "catalog_query_leases",
            snapshot.query_leases,
        );
        crate::metrics::publish_backend_query_execution_resource(
            "catalog_handle_leases",
            snapshot.handle_leases,
        );
    }

    /// Refuses to keep installing into a context that was retired under us.
    fn still_establishing(&self, installed: &InstalledContext) -> Result<(), HostRejection> {
        if installed.is_released() {
            return Err(cancelled_establish());
        }
        Ok(())
    }

    fn install_shared_facts(
        &self,
        context: QueryContextRef,
        installed: &InstalledContext,
        catalogs: Vec<CatalogProperties>,
        contribution: &proto::RuntimeFilterContribution,
        options: QueryOptions,
        options_fingerprint: ContentFingerprint,
        material: &WireCredential,
    ) -> Result<(), HostRejection> {
        let execution_id = context.query_execution_id();

        // Credentials first: a catalog runtime is the thing most likely to need
        // scoped storage access, so the authority has to exist before the
        // binding that may reach for it.
        installed.credentials.install(material)?;
        self.still_establishing(installed)?;

        {
            let mut facts = installed
                .facts
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if facts.released {
                return Err(cancelled_establish());
            }
            facts.query_options = Some(QueryContextOptions::new(
                Arc::new(options),
                options_fingerprint,
            ));
        }

        if let Some(decoded) = decoded_contribution(execution_id, contribution)? {
            let participant = self
                .runtime_filter_factory
                .install(execution_id, decoded)
                .map_err(runtime_filter_rejection)?;
            self.adopt_participant(context, installed, participant)?;
        }

        self.install_catalogs(execution_id, catalogs, installed)?;

        // The last word. Every install above is published in the record by the
        // time this runs, so a release that arrives from here on finds all of
        // them, and one that already ran is seen here and unwound by the caller.
        self.still_establishing(installed)
    }

    /// Publishes a freshly installed participant, or closes it if it lost.
    fn adopt_participant(
        &self,
        context: QueryContextRef,
        installed: &InstalledContext,
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
            close_participant(context, &participant);
            return Err(cancelled_establish());
        }
        if facts.participant.is_some() {
            drop(facts);
            close_participant(context, &participant);
            return Err(sealed_participant());
        }
        facts.participant = Some(participant);
        Ok(())
    }

    fn install_catalogs(
        &self,
        execution_id: QueryExecutionId,
        catalogs: Vec<CatalogProperties>,
        installed: &InstalledContext,
    ) -> Result<(), HostRejection> {
        if catalogs.is_empty() {
            return Ok(());
        }
        // Both branches below take leases, and the loop can take some and
        // then refuse, so the gauge is republished on the way out of every
        // one of them rather than only on success.
        let _publish_on_exit = PublishCatalogLeasesOnExit(self);
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
        emit_catalog_install_started(execution_id, catalogs.len());
        hold_cold_catalog_install(installed)?;
        inject_catalog_install_failure(execution_id, installed)?;

        for properties in catalogs {
            let factories = Arc::clone(&self.execution_role_binding_factories);
            self.catalog_manager
                .ensure_while(
                    execution_id,
                    properties,
                    || !installed.is_released(),
                    move |properties| {
                        factories
                            .bind(properties)
                            .map_err(CatalogManagerError::from_materialization)
                    },
                )
                .map_err(|error| catalog_rejection(installed, error))?;
        }
        Ok(())
    }
}

impl NativeQueryContextHost {
    /// Takes a released marker without installing anything.
    ///
    /// A marker exists only between a release that arrived before its
    /// establish and the materialize that answers it. Every exit from
    /// `materialize` has to take it, or the marker outlives the attempt.
    fn discard_released_marker(&self, context: QueryContextRef) {
        let mut contexts = self
            .contexts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if contexts
            .get(context)
            .is_some_and(|installed| installed.is_released())
        {
            contexts.remove(context);
        }
    }
}

/// Republishes the catalog lease gauge when a lease-taking scope exits.
///
/// A cold install can take some leases and then refuse, so the gauge has to
/// be republished on the refusal path too; a guard states that once instead
/// of at every `?`.
struct PublishCatalogLeasesOnExit<'host>(&'host NativeQueryContextHost);

impl Drop for PublishCatalogLeasesOnExit<'_> {
    fn drop(&mut self) {
        self.0.publish_catalog_lease_metrics();
    }
}

/// This host reconciles catalog reachability because it is this process's only
/// catalog lease owner: it takes the leases on establish and drops them on
/// release, so it is the only owner that can decide what is still needed.
impl crate::rpc::server::CatalogReachabilityAuthority for NativeQueryContextHost {
    fn prune_unreachable_catalogs(
        &self,
        reachable: std::collections::BTreeSet<novarocks_spi::connector::CatalogHandle>,
    ) -> crate::connector::catalog_manager::CatalogPruneResult {
        let result = self.catalog_manager.prune_unreachable(&reachable);
        self.publish_catalog_lease_metrics();
        result
    }
}

impl QueryContextHost for NativeQueryContextHost {
    fn materialize(&self, request: SharedFactsRequest<'_>) -> Result<(), HostRejection> {
        let context = request.context();
        let options_fingerprint = request.query_options().fingerprint();

        // Every payload is projected before anything is installed. A request
        // this process cannot read back is not half an establish: refusing it
        // here means the failure costs no catalog bind and no resident secret.
        //
        // A projection failure still has to consume a released marker. The
        // marker is only ever consumed by the materialize that follows it, so
        // returning early without taking it would leave one entry per such
        // establish in a map that lives as long as the process.
        let projected = catalog_bindings(request.catalog_binding().as_ref()).and_then(|catalogs| {
            let contribution = runtime_filter_install(request.initial_runtime_filter().as_ref())?;
            let options = query_options(request.query_options().as_ref())?;
            let material = credential_material(request.initial_credential())?;
            Ok((catalogs, contribution, options, material))
        });
        let (catalogs, contribution, options, material) = match projected {
            Ok(projected) => projected,
            Err(error) => {
                self.discard_released_marker(context);
                return Err(error);
            }
        };

        let installed = {
            let mut contexts = self
                .contexts
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            match contexts.get(context) {
                Some(existing) if existing.is_released() => {
                    // The owner released this context before its facts were
                    // installed. Consume the marker and refuse: installing now
                    // would strand a participant and a credential slot that
                    // nothing will ever release, because the owner calls
                    // `release` exactly once per context.
                    contexts.remove(context);
                    return Err(cancelled_establish());
                }
                Some(_) => {
                    return Err(internal(
                        "query context shared facts are already materialized",
                    ));
                }
                None => {
                    let installed = Arc::new(InstalledContext::establishing());
                    contexts.insert(context, Arc::clone(&installed));
                    installed
                }
            }
        };

        match self.install_shared_facts(
            context,
            &installed,
            catalogs,
            contribution,
            options,
            options_fingerprint,
            material,
        ) {
            Ok(()) => Ok(()),
            Err(rejection) => {
                // Undo here as well as in `release`. The owner does call
                // `release` after a failed materialize, but a host that leaves
                // a half-installed context behind when it says "no" would be
                // relying on that, and the trait does not promise it.
                //
                // The evidence this seals is dropped on purpose: a materialize
                // that failed has no release acknowledgement to report it on,
                // and an establish that never completed has no observation
                // worth publishing.
                drop(self.tear_down(context, &installed));
                self.contexts
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .remove_if_same(context, &installed);
                Err(rejection)
            }
        }
    }

    fn release(&self, context: QueryContextRef) -> ReleasedContextEvidence {
        let installed = {
            let mut contexts = self
                .contexts
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            match contexts.remove(context) {
                Some(installed) => installed,
                None => {
                    // Nothing is installed under this reference — either this
                    // is a second release, or the owner is releasing a context
                    // whose `materialize` has not started yet.
                    //
                    // The second case is real: the owner admits
                    // `Establishing -> Aborting`, a context with no tasks
                    // completes that abort immediately, and it calls `release`
                    // exactly once per context — so a materialize that then ran
                    // would install facts nothing would ever take back. Leaving
                    // a released marker makes that establish refuse instead.
                    // The marker is consumed by the materialize that follows,
                    // so markers are bounded by in-flight establishes rather
                    // than accumulating.
                    contexts.insert_released_marker(context);
                    self.catalog_manager
                        .release_query(context.query_execution_id());
                    // Published after this owner's own lock is released: the
                    // metrics layer holds no execution resource references and
                    // must not be reached with one held.
                    drop(contexts);
                    self.publish_catalog_lease_metrics();
                    return ReleasedContextEvidence::none();
                }
            }
        };
        self.tear_down(context, &installed)
    }

    fn advance_shared_domain(
        &self,
        context: QueryContextRef,
        domain: &QueryContextDomainUpdate,
    ) -> Result<(), HostRejection> {
        let installed = self.active_context(context)?;
        match domain {
            // The catalog domain is a scalar: a new version replaces the whole
            // binding. This backend can add a query's catalog leases but can
            // only drop them as a whole query, so a replacement that removes
            // one catalog has no representation here — and applying it as an
            // addition would silently keep the removed catalog resolvable.
            QueryContextDomainUpdate::CatalogBinding { .. } => Err(protocol(
                "a catalog binding advance cannot be applied: this backend releases catalog \
                 leases only for a whole query, so a replacement that drops a catalog has no \
                 representation",
            )),
            QueryContextDomainUpdate::SharedDynamicFilter { payload, .. } => {
                let contribution = runtime_filter_install(payload.as_ref())?;
                let Some(decoded) =
                    decoded_contribution(context.query_execution_id(), contribution)?
                else {
                    // An advance exists to carry a change. "Install nothing"
                    // either says the participant should be removed, which a
                    // sealed participant cannot represent, or says nothing at
                    // all.
                    return Err(protocol(
                        "a shared dynamic filter advance carries no participant install",
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
                let participant = self
                    .runtime_filter_factory
                    .install(context.query_execution_id(), decoded)
                    .map_err(runtime_filter_rejection)?;
                self.adopt_participant(context, &installed, participant)
            }
            QueryContextDomainUpdate::Credential(update) => {
                // One atomic whole-table replace. The owner already classified
                // this rotation as the exact next epoch, and this backend mints
                // nothing: there is no prepare, no commit, and no catalog call.
                let material = credential_material(update)?;
                installed.credentials.install(material)
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
) -> Result<Option<DecodedRuntimeFilterContribution>, HostRejection> {
    if contribution.participant_id == 0 {
        if contribution.lifecycle.is_some() || contribution.install.is_some() {
            return Err(protocol(
                "runtime filter contribution names no participant but carries an install",
            ));
        }
        return Ok(None);
    }
    let parsed = RuntimeFilterContribution::parse(contribution.clone())
        .map_err(|error| protocol(&format!("runtime filter contribution is invalid: {error}")))?;
    decode_runtime_filter_contribution(execution_id, &parsed)
        .map(Some)
        .map_err(runtime_filter_rejection)
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
/// create can follow, and the registry does not hand a context to this
/// tear-down until every task it knows is a terminal record. A channel still
/// open at that point closed without publishing, which is a fact to report --
/// cancelling it first would overwrite the observation with a cause the
/// release does not have.
fn seal_runtime_filter_evidence(
    context: QueryContextRef,
    participant: &Arc<RuntimeFilterParticipant>,
) -> ReleasedContextEvidence {
    let snapshot = participant
        .prepare_terminal_capture(QueryTerminationReason::QueryTerminationCoordinatorFinalize);
    match capture_terminal_profile_contribution(Some(snapshot), true) {
        Ok(telemetry) => match QueryTerminalProfileContributionTelemetry::parse(telemetry) {
            Ok(telemetry) => ReleasedContextEvidence::with_runtime_filter(telemetry),
            Err(error) => {
                // The projection produced a value this process cannot vouch
                // for. Reporting it anyway would make the frontend the first
                // owner to discover it is malformed.
                error!(
                    target: "novarocks::task_execution",
                    query_context = %context,
                    error = %error,
                    "sealed runtime filter contribution does not satisfy the terminal contract; \
                     the release reports it as unavailable"
                );
                ReleasedContextEvidence::with_runtime_filter(runtime_filter_unavailable(
                    "CONTRIBUTION_INVALID",
                ))
            }
        },
        Err(error) => {
            // A correctness failure in the observation itself. The query has
            // already produced its answer, so this is reported as unavailable
            // telemetry rather than turned into a release failure.
            error!(
                target: "novarocks::task_execution",
                query_context = %context,
                error = %error,
                "runtime filter observation failed its correctness check at release"
            );
            ReleasedContextEvidence::with_runtime_filter(runtime_filter_unavailable(
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

fn close_participant(context: QueryContextRef, participant: &Arc<RuntimeFilterParticipant>) {
    // The task protocol's release is deliberately cause-free: the owner
    // publishes the termination cause on the context and on every task, and
    // this boundary is called identically for a rollback and for a normal
    // release. Naming `COORDINATOR_FINALIZE` or `COORDINATOR_ABORT` here would
    // state a cause this host does not know.
    if let Err(error) = participant.close(QueryTerminationReason::Unspecified) {
        error!(
            target: "novarocks::task_execution",
            query_context = %context,
            error = %error,
            "runtime filter participant close did not complete; the participant is dropped"
        );
    }
}

/// A shared-facts install that was retired while it ran.
///
/// None of the five failure categories names "the owner abandoned this attempt
/// underneath me". `Execution` is the closest true statement — the install did
/// not complete at runtime — and the detail carries the rest.
fn cancelled_establish() -> HostRejection {
    HostRejection::new(
        TaskFailureCategory::Execution,
        "the query context was released while its shared facts were being installed",
    )
}

fn sealed_participant() -> HostRejection {
    protocol(
        "a runtime filter participant is already installed for this query context and is sealed \
         for the attempt",
    )
}

/// How long a held cold install waits between checks for its own retirement.
///
/// The same interval the retired stack's hold used. It bounds how long a
/// cancelled establish stays parked after its context is released, and nothing
/// else: the rendezvous itself ends on a file the runner removes.
const CATALOG_INSTALL_HOLD_POLL: std::time::Duration = std::time::Duration::from_millis(10);

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
fn emit_catalog_install_started(execution_id: QueryExecutionId, catalog_count: usize) {
    if !crate::config::debug_emit_catalog_lifecycle_marker() {
        return;
    }
    println!(
        "NOVAROCKS_CATALOG_INSTALL_STARTED execution_id={}:{}:{} catalog_count={catalog_count}",
        execution_id.query_id().high(),
        execution_id.query_id().low(),
        execution_id.attempt_id().get(),
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());
}

/// Parks a cold catalog install while the runner's hold file exists.
///
/// The task protocol installs shared facts inside the establish rather than in
/// a background pass, so this is the one place a cold install is observably
/// in flight. A case about cancelling an install mid-flight has nowhere else
/// to stand: without the hold the install is a few milliseconds long and the
/// cancellation always arrives before or after it, never during.
///
/// The wait ends on the context's own retirement as well as on the file, and
/// that ordering matters: an abort that arrives while this is parked has to
/// end the install rather than wait for the runner. Returning the cancelled
/// rejection here is what keeps the provider bind below from running at all,
/// so a cancelled attempt materializes no catalog runtime.
fn hold_cold_catalog_install(installed: &InstalledContext) -> Result<(), HostRejection> {
    let Some(hold_file) = crate::config::debug_catalog_install_hold_file() else {
        return Ok(());
    };
    while hold_file.exists() {
        if installed.is_released() {
            return Err(cancelled_establish());
        }
        std::thread::sleep(CATALOG_INSTALL_HOLD_POLL);
    }
    // Checked once more after the file is gone: the release may have landed in
    // the same instant the runner lifted the hold.
    if installed.is_released() {
        return Err(cancelled_establish());
    }
    Ok(())
}

/// Fails one selected backend's cold catalog install on the runner's trigger.
///
/// Checked before the catalog manager is touched, exactly as the retired stack
/// checked it: a failure recorded in a manager cell would be suppressed for
/// its retry cooldown, and the case that clears the trigger and retries
/// immediately would then be measuring the cooldown instead of the retry.
///
/// The detail is routed through `catalog_rejection` so an injected failure is
/// classified by the same rule as a real one, rather than by a category chosen
/// here.
fn inject_catalog_install_failure(
    execution_id: QueryExecutionId,
    installed: &InstalledContext,
) -> Result<(), HostRejection> {
    let armed = crate::config::debug_catalog_install_failure_file()
        .is_some_and(|failure_file| failure_file.exists());
    if !armed {
        return Ok(());
    }
    if crate::config::debug_emit_catalog_lifecycle_marker() {
        println!(
            "NOVAROCKS_CATALOG_INSTALL_FAILED execution_id={}:{}:{}",
            execution_id.query_id().high(),
            execution_id.query_id().low(),
            execution_id.attempt_id().get(),
        );
        let _ = std::io::Write::flush(&mut std::io::stdout());
    }
    Err(catalog_rejection(
        installed,
        CatalogManagerError::materialization_failed("runner-injected catalog install failure"),
    ))
}

/// Maps a catalog install failure onto this protocol's categories.
///
/// The match is exhaustive on purpose: a new connector error class must be
/// classified here rather than folded into whatever the last arm happened to
/// be.
fn catalog_rejection(installed: &InstalledContext, error: CatalogManagerError) -> HostRejection {
    if installed.is_released() {
        // The manager reports its own cancellation, which is true and says
        // nothing about who cancelled it. This host is the one that did, so it
        // reports that rather than passing along a message a reader would have
        // to work backwards from.
        return cancelled_establish();
    }
    let category = match &error {
        // This process was composed without the factory the definition needs,
        // or with a contradictory one. Neither is something a peer sent.
        CatalogManagerError::InvalidConfiguration(_) => TaskFailureCategory::Internal,
        // Two queries claim one catalog handle with different definitions. The
        // handle is a content version, so this is illegal content.
        CatalogManagerError::ConflictingProperties { .. } => TaskFailureCategory::Protocol,
        CatalogManagerError::MaterializationFailed { class, .. } => match class {
            ConnectorMaterializationErrorClass::InvalidDefinition => TaskFailureCategory::Protocol,
            ConnectorMaterializationErrorClass::ResourceExhausted => {
                TaskFailureCategory::ResourceExhausted
            }
            ConnectorMaterializationErrorClass::Internal => TaskFailureCategory::Internal,
            ConnectorMaterializationErrorClass::Authentication
            | ConnectorMaterializationErrorClass::Unavailable
            | ConnectorMaterializationErrorClass::Timeout
            | ConnectorMaterializationErrorClass::Cancelled => TaskFailureCategory::Execution,
        },
    };
    HostRejection::new(category, safe_detail(&error.to_string()))
}

/// Maps a runtime-filter contract refusal onto this protocol's categories.
///
/// The match is exhaustive on purpose: a new contract failure class must be
/// classified here rather than folded into whichever arm happened to be last.
fn runtime_filter_rejection(error: RuntimeFilterContractError) -> HostRejection {
    let category = match error.code() {
        // Illegal content in a contribution, install, session binding, or
        // terminal projection. A peer sent it, so it is a protocol failure.
        RuntimeFilterContractErrorCode::InvalidContract => TaskFailureCategory::Protocol,
        // The participant that would answer is gone. Nothing about the
        // request was wrong, so this describes this attempt's own progress.
        RuntimeFilterContractErrorCode::ParticipantClosed => TaskFailureCategory::Execution,
    };
    HostRejection::new(category, safe_detail(error.detail()))
}

/// Strips control characters out of a detail this host did not write.
///
/// Connector and codec messages end up in a task's status and in logs.
/// `SafeDetail` bounds their length; this bounds what they can inject. It is
/// not a secret filter: catalog definitions and vended material never travel
/// through these strings, and a rejection that describes a credential says only
/// what was wrong with its scope.
fn safe_detail(value: &str) -> String {
    value.chars().filter(|c| !c.is_control()).collect()
}

fn internal(detail: &str) -> HostRejection {
    HostRejection::new(TaskFailureCategory::Internal, detail)
}

fn protocol(detail: &str) -> HostRejection {
    HostRejection::new(TaskFailureCategory::Protocol, detail)
}

#[cfg(test)]
mod tests {
    use super::NativeQueryContextHost;

    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Mutex};

    use novarocks_connector_binding::{
        ConnectorExecutionRoleBinding, ConnectorExecutionRoleBindingFactory,
        ConnectorMaterializationError, ConnectorMaterializationErrorClass,
        ConnectorMaterializationRetryDisposition, NormalizedCatalogProperties,
    };
    use novarocks_execution::task_execution::ConfidentialContent;
    use novarocks_execution::task_execution::domain::{
        CodecOwnedContent, CredentialEpoch, CredentialLeaseId, DomainVersion,
    };
    use novarocks_execution::task_execution::identity::QueryContextRef;
    use novarocks_execution::task_execution::operation::{
        CredentialUpdate, QueryContextDomainUpdate,
    };
    use novarocks_execution::task_execution::status::TaskFailureCategory;
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_codec::catalog::CatalogSet;
    use novarocks_proto_codec::lifecycle::{
        CredentialLeaseSecretEnvelope, encode_credential_lease_descriptor,
        encode_credential_lease_secret_envelope,
    };
    use novarocks_proto_codec::task_execution::domain::{WireContent, WireCredential};
    use novarocks_proto_models::{filter, novarocks as proto};
    use novarocks_spi::connector::{
        CatalogHandle, CatalogProperties, CatalogProviderKind, CatalogVersion, ConnectorInstanceId,
        CredentialLeaseDescriptor, CredentialLeaseProvider, StorageAccessDomainId,
        StorageAccessRequest, StorageCredentialScopePrefix,
    };
    use novarocks_types::identity::{
        AttemptId, BackendProcessId, FrontendProcessId, QueryExecutionId, QueryId,
    };

    use crate::connector::catalog_manager::{
        CatalogManager, ConnectorExecutionRoleBindingFactorySet,
    };
    use crate::rpc::runtime::test_backend_data_runtime;
    use crate::runtime_filter::error::{
        RuntimeFilterContractError, RuntimeFilterContractErrorCode,
    };
    use crate::runtime_filter::install_decode::DecodedRuntimeFilterContribution;
    use crate::runtime_filter::participant::{
        BackendRuntimeFilterParticipantFactory, RuntimeFilterParticipant,
        RuntimeFilterParticipantFactory,
    };
    use crate::task_execution::clock::ProcessMonotonicClock;
    use crate::task_execution::execution_host::TaskQueryContextFacts;
    use crate::task_execution::host::{QueryContextHost, SharedFactsRequest};
    use crate::task_execution::observation::TaskStatusSource;
    use crate::task_execution::status::{
        METRIC_PUBLISH_MIN_INTERVAL, TaskStatusOwner, TaskStatusReporter,
    };
    use novarocks_execution::task_execution::identity::TaskIdentity;

    const SECRET_SENTINEL: &str = "NOVAROCKS_SECRET_SENTINEL";

    // ---------------------------------------------------------------- identity

    fn execution_id(attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(0x5245_4553, 11),
            AttemptId::new(attempt).expect("nonzero attempt"),
        )
        .expect("legal execution id")
    }

    fn context(attempt: u64) -> QueryContextRef {
        QueryContextRef::new(
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
            CatalogProviderKind::Iceberg,
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
        fn provider_kind(&self) -> CatalogProviderKind {
            CatalogProviderKind::Iceberg
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

    impl RuntimeFilterParticipantFactory for RecordingFilterFactory {
        fn install(
            &self,
            execution_id: QueryExecutionId,
            contribution: DecodedRuntimeFilterContribution,
        ) -> Result<Arc<RuntimeFilterParticipant>, RuntimeFilterContractError> {
            self.ledger.installs.fetch_add(1, Ordering::SeqCst);
            let participant =
                BackendRuntimeFilterParticipantFactory::new(test_backend_data_runtime())
                    .install(execution_id, contribution)?;
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

    fn credential(epoch: u64, secret: &str, not_after: u64) -> CredentialUpdate {
        let descriptor = encode_credential_lease_descriptor(
            &CredentialLeaseDescriptor::try_new(
                lease_id(1),
                epoch,
                catalog_owner(),
                CredentialLeaseProvider::S3,
                vec![prefix("s3://bucket/a")],
                not_after,
                true,
                StorageAccessDomainId::from_bytes([8; 32]),
            )
            .expect("legal descriptor"),
        );
        let envelope = encode_credential_lease_secret_envelope(
            &CredentialLeaseSecretEnvelope::try_new_from_wire_scalars(
                lease_id(1),
                epoch,
                "access-key".to_owned(),
                secret.to_owned(),
                "session-token".to_owned(),
                not_after,
            )
            .expect("legal envelope"),
        );
        let material = Arc::new(
            WireCredential::decode(&[descriptor], &[envelope], FieldPath::root("credential"))
                .expect("legal rotation"),
        );
        CredentialUpdate::new(
            CredentialLeaseId::new(1),
            CredentialEpoch::new(epoch).expect("nonzero epoch"),
            material as Arc<dyn ConfidentialContent>,
        )
    }

    fn empty_credential() -> CredentialUpdate {
        let material = Arc::new(
            WireCredential::decode(&[], &[], FieldPath::root("credential"))
                .expect("an empty rotation is legal"),
        );
        CredentialUpdate::new(
            CredentialLeaseId::new(1),
            CredentialEpoch::FIRST,
            material as Arc<dyn ConfidentialContent>,
        )
    }

    fn storage_request(location: &str) -> StorageAccessRequest {
        StorageAccessRequest::try_new(catalog_owner(), location).expect("legal storage request")
    }

    // ------------------------------------------------------------------- fixture

    struct Fixture {
        host: Arc<NativeQueryContextHost>,
        catalog_manager: Arc<CatalogManager<ConnectorExecutionRoleBinding>>,
        catalog_factory: Arc<ScriptedFactory>,
        filter_factory: Arc<RecordingFilterFactory>,
    }

    impl Fixture {
        fn new() -> Self {
            let catalog_factory = ScriptedFactory::new();
            let filter_factory = RecordingFilterFactory::new();
            let catalog_manager =
                Arc::new(CatalogManager::<ConnectorExecutionRoleBinding>::default());
            let factories = Arc::new(
                ConnectorExecutionRoleBindingFactorySet::try_new([
                    Arc::clone(&catalog_factory) as Arc<dyn ConnectorExecutionRoleBindingFactory>
                ])
                .expect("legal factory set"),
            );
            let host = Arc::new(NativeQueryContextHost::new(
                Arc::clone(&catalog_manager),
                factories,
                Arc::clone(&filter_factory) as Arc<dyn RuntimeFilterParticipantFactory>,
                test_backend_data_runtime(),
            ));
            Self {
                host,
                catalog_manager,
                catalog_factory,
                filter_factory,
            }
        }

        fn establish(
            &self,
            context: QueryContextRef,
            catalogs: Vec<CatalogProperties>,
            contribution: proto::RuntimeFilterContribution,
            credential: &CredentialUpdate,
        ) -> Result<(), super::HostRejection> {
            self.establish_with_options(
                context,
                catalogs,
                contribution,
                proto::QueryOptions::default(),
                credential,
            )
        }

        fn establish_with_options(
            &self,
            context: QueryContextRef,
            catalogs: Vec<CatalogProperties>,
            contribution: proto::RuntimeFilterContribution,
            query_options: proto::QueryOptions,
            credential: &CredentialUpdate,
        ) -> Result<(), super::HostRejection> {
            let catalog = catalog_payload(catalogs);
            let filter = filter_payload(contribution);
            let options: Arc<dyn CodecOwnedContent> =
                Arc::new(WireContent::new(b"query-options", query_options));
            self.host.materialize(SharedFactsRequest::new(
                context, &catalog, &filter, &options, credential,
            ))
        }

        fn query_leases(&self) -> usize {
            self.catalog_manager.lease_snapshot().query_leases
        }

        fn handle_leases(&self) -> usize {
            self.catalog_manager.lease_snapshot().handle_leases
        }
    }

    // ----------------------------------------------------------------- tests

    /// One establish installs all three shared facts, or it is not an establish.
    #[test]
    fn an_establish_installs_catalogs_the_runtime_filter_and_the_credential_slot() {
        let fixture = Fixture::new();
        let context = context(1);
        fixture
            .establish(
                context,
                vec![catalog_properties()],
                participant_contribution(1),
                &credential(1, SECRET_SENTINEL, live_until()),
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
                .storage_resolver_for_context(context)
                .expect("an active context resolves storage")
                .resolve_vended_s3(&storage_request("s3://bucket/a/file.parquet"))
                .is_ok(),
            "the installed credential must serve its own scope"
        );
    }

    #[test]
    fn an_establish_installs_the_query_options_for_every_task() {
        let fixture = Fixture::new();
        let context = context(1);
        fixture
            .establish_with_options(
                context,
                vec![catalog_properties()],
                no_contribution(),
                proto::QueryOptions {
                    query_mem_limit: 8192,
                    pipeline_dop: 3,
                    ..proto::QueryOptions::default()
                },
                &credential(1, SECRET_SENTINEL, live_until()),
            )
            .expect("a complete establish");

        let options = fixture
            .host
            .query_options(context.query_execution_id())
            .expect("the active context owns its query options");
        assert_eq!(options.runtime().exec_mem_limit(), Some(8192));
        assert_eq!(options.runtime().pipeline_dop(), Some(3));

        fixture.host.release(context);
        assert!(
            fixture
                .host
                .query_options(context.query_execution_id())
                .is_err(),
            "released contexts cannot supply query options"
        );
    }

    /// Release takes all three back, and a second release changes nothing.
    #[test]
    fn a_release_undoes_every_shared_fact_and_stays_idempotent() {
        let fixture = Fixture::new();
        let context = context(1);
        fixture
            .establish(
                context,
                vec![catalog_properties()],
                participant_contribution(1),
                &credential(1, SECRET_SENTINEL, live_until()),
            )
            .expect("a complete establish");

        fixture.host.release(context);
        assert_eq!(fixture.query_leases(), 0, "catalog leases must be dropped");
        assert_eq!(
            fixture.filter_factory.ledger.closes.load(Ordering::SeqCst),
            1
        );
        assert!(
            fixture.host.storage_resolver_for_context(context).is_none(),
            "a released context has no credential authority"
        );

        fixture.host.release(context);
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
        let context = context(1);
        fixture
            .establish(
                context,
                vec![catalog_properties()],
                participant_contribution(1),
                &credential(1, SECRET_SENTINEL, live_until()),
            )
            .expect("a complete establish");

        let evidence = fixture.host.release(context);
        let telemetry = evidence
            .runtime_filter()
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
        let context = context(1);
        fixture
            .establish(
                context,
                vec![catalog_properties()],
                no_contribution(),
                &credential(1, SECRET_SENTINEL, live_until()),
            )
            .expect("a complete establish");

        assert!(
            fixture.host.release(context).runtime_filter().is_none(),
            "a query with no runtime filter on this backend must not report an \
             empty contribution"
        );
    }

    /// A failure in the last install step must not strand the earlier ones.
    #[test]
    fn a_failed_catalog_install_leaves_no_credential_and_no_participant_behind() {
        let fixture = Fixture::new();
        fixture.catalog_factory.fail.store(true, Ordering::SeqCst);
        let context = context(1);

        let rejection = fixture
            .establish(
                context,
                vec![catalog_properties()],
                participant_contribution(1),
                &credential(1, SECRET_SENTINEL, live_until()),
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
            fixture.host.storage_resolver_for_context(context).is_none(),
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

        let context = context(1);
        let establishing = {
            let fixture = Arc::clone(&fixture);
            std::thread::spawn(move || {
                fixture.establish(
                    context,
                    vec![catalog_properties()],
                    participant_contribution(1),
                    &credential(1, SECRET_SENTINEL, live_until()),
                )
            })
        };

        // The install is now inside the provider bind.
        entered.wait();
        fixture.host.release(context);
        proceed.wait();

        let rejection = establishing
            .join()
            .expect("the establishing thread must not panic")
            .expect_err("a released context cannot finish establishing");
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
        assert!(fixture.host.storage_resolver_for_context(context).is_none());
    }

    /// A release that arrives before its establish is called exactly once by
    /// the owner, so the establish that follows must refuse rather than install
    /// facts nothing will take back.
    #[test]
    fn a_task_that_binds_a_filter_is_refused_when_its_context_installed_none() {
        use crate::task_execution::execution_host::TaskQueryContextFacts;

        // Answering `None` here would let the scan run unfiltered and call the
        // result correct. A query that installs no filter on this backend is
        // the ordinary case and must still be answered `None`, so the two are
        // distinguished by what the task's own plan binds.
        let fixture = Fixture::new();
        let context = context(1);
        fixture
            .establish(
                context,
                vec![catalog_properties()],
                no_contribution(),
                &credential(1, SECRET_SENTINEL, live_until()),
            )
            .expect("an establish with no filter participant is legal");

        let execution = context.query_execution_id();
        let finst = novarocks_types::UniqueId::new(4, 5);
        assert!(
            fixture
                .host
                .runtime_filter_session(execution, finst, false)
                .expect("a task that binds none is answered")
                .is_none()
        );
        match fixture.host.runtime_filter_session(execution, finst, true) {
            Err(refusal) => assert_eq!(refusal.category(), TaskFailureCategory::Protocol),
            Ok(_) => panic!("a task that binds a filter has nothing to bind to"),
        }
    }

    #[test]
    fn storage_credentials_are_refused_rather_than_defaulted() {
        use crate::task_execution::execution_host::TaskQueryContextFacts;

        // There is no process-level credential a scan could legitimately fall
        // back to, so an unestablished or released context must fail the
        // task's preparation instead of deferring the failure to read time,
        // where it would surface as an object-store error with no context.
        let fixture = Fixture::new();
        let context = context(1);
        let execution = context.query_execution_id();

        match TaskQueryContextFacts::storage_resolver(fixture.host.as_ref(), execution) {
            Err(refusal) => assert_eq!(refusal.category(), TaskFailureCategory::Protocol),
            Ok(_) => panic!("an unestablished context holds no credentials"),
        }

        fixture
            .establish(
                context,
                vec![catalog_properties()],
                no_contribution(),
                &credential(1, SECRET_SENTINEL, live_until()),
            )
            .expect("establish");
        assert!(TaskQueryContextFacts::storage_resolver(fixture.host.as_ref(), execution).is_ok());

        fixture.host.release(context);
        match TaskQueryContextFacts::storage_resolver(fixture.host.as_ref(), execution) {
            Err(refusal) => {
                assert_eq!(refusal.category(), TaskFailureCategory::Protocol);
                assert!(!refusal.detail().as_str().contains(SECRET_SENTINEL));
            }
            Ok(_) => panic!("a released context holds no credentials either"),
        }
    }

    #[test]
    fn an_unreadable_establish_after_a_release_leaves_no_marker_behind() {
        // A released marker is consumed only by the materialize that answers
        // it. `contexts` lives as long as the process and a context reference
        // is distinct per attempt, so an early return that skipped the marker
        // would leak one entry per occurrence — reachable only through a
        // protocol bug, which is exactly when unbounded growth is worst.
        let fixture = Fixture::new();
        let context = context(1);
        fixture.host.release(context);

        // A catalog payload where the runtime filter belongs: well-formed, and
        // not what this domain asked for, so the projection refuses.
        let wrong_domain = catalog_payload(vec![catalog_properties()]);
        let options = query_options_payload();
        let refusal = fixture
            .host
            .materialize(SharedFactsRequest::new(
                context,
                &wrong_domain,
                &wrong_domain,
                &options,
                &credential(1, SECRET_SENTINEL, live_until()),
            ))
            .expect_err("a payload of the wrong domain is refused");
        assert!(!refusal.detail().as_str().contains(SECRET_SENTINEL));

        assert!(
            fixture
                .host
                .contexts
                .lock()
                .expect("context map")
                .get(context)
                .is_none(),
            "a refused establish must take the marker with it"
        );
    }

    #[test]
    fn an_establish_after_its_context_was_released_installs_nothing() {
        let fixture = Fixture::new();
        let context = context(1);

        fixture.host.release(context);
        let rejection = fixture
            .establish(
                context,
                vec![catalog_properties()],
                participant_contribution(1),
                &credential(1, SECRET_SENTINEL, live_until()),
            )
            .expect_err("a released context cannot be established");
        assert_eq!(rejection.category(), TaskFailureCategory::Execution);

        assert_eq!(fixture.query_leases(), 0);
        assert_eq!(
            fixture
                .filter_factory
                .ledger
                .installs
                .load(Ordering::SeqCst),
            0,
            "no participant may be built for a context that was already released"
        );
        assert_eq!(
            fixture.catalog_factory.binds.load(Ordering::SeqCst),
            0,
            "no provider may be bound for a context that was already released"
        );
        assert!(fixture.host.storage_resolver_for_context(context).is_none());
    }

    /// The empty contribution is how a query with no runtime filter establishes.
    #[test]
    fn a_contribution_that_names_no_participant_installs_none() {
        let fixture = Fixture::new();
        let context = context(1);
        fixture
            .establish(context, vec![], no_contribution(), &empty_credential())
            .expect("a query with no runtime filter still establishes");
        assert_eq!(
            fixture
                .filter_factory
                .ledger
                .installs
                .load(Ordering::SeqCst),
            0
        );

        fixture.host.release(context);
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
            .establish(context(1), vec![], contribution, &empty_credential())
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
        let context = context(1);
        // A catalog set where the runtime filter belongs.
        let catalog = catalog_payload(vec![catalog_properties()]);
        let filter = catalog_payload(vec![]);
        let options = query_options_payload();
        let credential = credential(1, SECRET_SENTINEL, live_until());
        let rejection = fixture
            .host
            .materialize(SharedFactsRequest::new(
                context,
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
        assert!(fixture.host.storage_resolver_for_context(context).is_none());
    }

    /// A second establish must not silently replace the first context's facts.
    #[test]
    fn a_second_materialize_for_a_live_context_is_an_invariant_violation() {
        let fixture = Fixture::new();
        let context = context(1);
        fixture
            .establish(
                context,
                vec![catalog_properties()],
                participant_contribution(1),
                &credential(1, SECRET_SENTINEL, live_until()),
            )
            .expect("the first establish");

        let rejection = fixture
            .establish(
                context,
                vec![catalog_properties()],
                participant_contribution(1),
                &credential(1, SECRET_SENTINEL, live_until()),
            )
            .expect_err("a live context cannot be materialized twice");
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
        // The first context is untouched.
        assert!(fixture.host.storage_resolver_for_context(context).is_some());
    }

    /// A rotation replaces only the credential slot.
    #[test]
    fn a_credential_rotation_replaces_the_slot_and_touches_nothing_else() {
        let fixture = Fixture::new();
        let context = context(1);
        let expiry = live_until();
        fixture
            .establish(
                context,
                vec![catalog_properties()],
                participant_contribution(1),
                &credential(1, SECRET_SENTINEL, expiry),
            )
            .expect("establish");
        let binds_before = fixture.catalog_factory.binds.load(Ordering::SeqCst);

        fixture
            .host
            .advance_shared_domain(
                context,
                &QueryContextDomainUpdate::Credential(credential(2, "rotated", expiry)),
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
                .storage_resolver_for_context(context)
                .expect("still active")
                .resolve_vended_s3(&storage_request("s3://bucket/a/file.parquet"))
                .is_ok()
        );
    }

    /// A refused rotation must leave the epoch that was already serving.
    #[test]
    fn a_refused_rotation_leaves_the_installed_credential_serving() {
        let fixture = Fixture::new();
        let context = context(1);
        fixture
            .establish(
                context,
                vec![],
                no_contribution(),
                &credential(1, SECRET_SENTINEL, live_until()),
            )
            .expect("establish");

        let rejection = fixture
            .host
            .advance_shared_domain(
                context,
                // Already expired: legal on the wire, unusable once installed.
                &QueryContextDomainUpdate::Credential(credential(2, "rotated", 1)),
            )
            .expect_err("an expired rotation is refused");
        assert_eq!(rejection.category(), TaskFailureCategory::Protocol);

        assert!(
            fixture
                .host
                .storage_resolver_for_context(context)
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
        let context = context(1);
        fixture
            .establish(context, vec![], no_contribution(), &empty_credential())
            .expect("establish");

        let rejection = fixture
            .host
            .advance_shared_domain(
                context,
                &QueryContextDomainUpdate::CatalogBinding {
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
        let context = context(1);
        fixture
            .establish(context, vec![], no_contribution(), &empty_credential())
            .expect("establish with no participant");

        fixture
            .host
            .advance_shared_domain(
                context,
                &QueryContextDomainUpdate::SharedDynamicFilter {
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
            .host
            .advance_shared_domain(
                context,
                &QueryContextDomainUpdate::SharedDynamicFilter {
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

        fixture.host.release(context);
        assert_eq!(
            fixture.filter_factory.ledger.closes.load(Ordering::SeqCst),
            1,
            "the participant an advance installed must be released like any other"
        );
    }

    /// A running task can name only its query execution, so the execution index
    /// has to resolve exactly the live context — and nothing that has no facts.
    #[test]
    fn the_execution_index_resolves_only_a_live_context() {
        let fixture = Fixture::new();
        let live = context(1);
        assert_eq!(fixture.host.context_for_execution(execution_id(1)), None);

        fixture
            .establish(live, vec![], no_contribution(), &empty_credential())
            .expect("establish");
        assert_eq!(
            fixture.host.context_for_execution(execution_id(1)),
            Some(live)
        );

        fixture.host.release(live);
        assert_eq!(
            fixture.host.context_for_execution(execution_id(1)),
            None,
            "a released context must not stay resolvable by a task"
        );

        // A release that arrives before its establish leaves a marker behind.
        // A marker carries no facts, so it must never be resolvable either.
        fixture.host.release(context(2));
        assert_eq!(fixture.host.context_for_execution(execution_id(2)), None);
    }

    /// An advance for a context this host holds no facts for must fail closed,
    /// and must not claim to know whether it lost a race or found a bug.
    #[test]
    fn an_advance_for_a_context_without_shared_facts_is_refused() {
        let fixture = Fixture::new();
        let rejection = fixture
            .host
            .advance_shared_domain(
                context(1),
                &QueryContextDomainUpdate::Credential(empty_credential()),
            )
            .expect_err("a context with no shared facts cannot take an advance");
        assert_eq!(rejection.category(), TaskFailureCategory::Execution);

        // A released context is the same answer: an advance must never install
        // credential material into a context that has been torn down.
        let released = context(2);
        fixture
            .establish(released, vec![], no_contribution(), &empty_credential())
            .expect("establish");
        fixture.host.release(released);
        assert!(
            fixture
                .host
                .advance_shared_domain(
                    released,
                    &QueryContextDomainUpdate::Credential(credential(
                        1,
                        SECRET_SENTINEL,
                        live_until()
                    )),
                )
                .is_err(),
            "a released context must not accept a rotation"
        );
        assert!(
            fixture
                .host
                .storage_resolver_for_context(released)
                .is_none()
        );
    }

    /// A close that does not complete must still leave the context releasable.
    #[test]
    fn a_participant_whose_close_fails_is_still_dropped_and_release_stays_idempotent() {
        let fixture = Fixture::new();
        let context = context(1);
        fixture
            .filter_factory
            .ledger
            .fail_close
            .store(true, Ordering::SeqCst);
        fixture
            .establish(
                context,
                vec![catalog_properties()],
                participant_contribution(1),
                &credential(1, SECRET_SENTINEL, live_until()),
            )
            .expect("establish");

        fixture.host.release(context);
        assert_eq!(
            fixture.filter_factory.ledger.closes.load(Ordering::SeqCst),
            1
        );
        assert_eq!(
            fixture.query_leases(),
            0,
            "a failed close must not hold the catalog leases open"
        );
        assert!(fixture.host.storage_resolver_for_context(context).is_none());

        fixture.host.release(context);
        assert_eq!(
            fixture.filter_factory.ledger.closes.load(Ordering::SeqCst),
            1,
            "a failed close must not be retried blindly"
        );
    }

    /// The first submitted task of a context becomes its feedback carrier.
    #[test]
    fn the_first_submitted_task_carries_the_context_dynamic_filter_feedback() {
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
        let context = context(1);
        fixture
            .establish(
                context,
                vec![],
                participant_contribution(1),
                &credential(1, SECRET_SENTINEL, live_until()),
            )
            .expect("establish with a participant");
        let execution = context.query_execution_id();
        assert_eq!(
            fixture.host.feedback_carrier(execution),
            None,
            "an established context with no submitted task carries nothing yet"
        );

        let first = task_identity(context, 1);
        let second = task_identity(context, 2);
        let (_first_owner, first_reporter) = reporter_for(first);
        let (_second_owner, second_reporter) = reporter_for(second);

        assert!(
            fixture
                .host
                .bind_runtime_filter_feedback(execution, first, &first_reporter),
            "the first submitted task takes the carrier role"
        );
        assert_eq!(fixture.host.feedback_carrier(execution), Some(first));
        assert!(
            !fixture
                .host
                .bind_runtime_filter_feedback(execution, second, &second_reporter),
            "a second task must not replace the carrier"
        );
        assert_eq!(
            fixture.host.feedback_carrier(execution),
            Some(first),
            "the carrier is first-wins, not last-writer"
        );

        // Releasing the context drops the only strong reference to the sink,
        // so a late publication finds nothing to publish through.
        fixture.host.release(context);
        assert_eq!(fixture.host.feedback_carrier(execution), None);
    }

    /// A query with no participant on this backend has nothing to carry.
    #[test]
    fn a_context_without_a_participant_declines_the_carrier_role() {
        let fixture = Fixture::new();
        let context = context(1);
        fixture
            .establish(
                context,
                vec![],
                no_contribution(),
                &credential(1, SECRET_SENTINEL, live_until()),
            )
            .expect("establish without a participant");
        let execution = context.query_execution_id();
        let task = task_identity(context, 1);
        let (_owner, reporter) = reporter_for(task);

        assert!(
            !fixture
                .host
                .bind_runtime_filter_feedback(execution, task, &reporter),
            "there is no channel that could ever reduce, so there is nothing to carry"
        );
        assert_eq!(fixture.host.feedback_carrier(execution), None);
    }

    /// One task of a context, on the exact backend process that context names.
    fn task_identity(context: QueryContextRef, task: u32) -> TaskIdentity {
        TaskIdentity::new(
            context.query_execution_id(),
            novarocks_types::identity::StageId::new(1).expect("nonzero stage"),
            novarocks_types::identity::TaskId::new(task).expect("nonzero task"),
            context.backend_process_id(),
        )
    }

    fn reporter_for(identity: TaskIdentity) -> (Arc<TaskStatusOwner>, TaskStatusReporter) {
        let owner = Arc::new(TaskStatusOwner::new(
            identity,
            Arc::new(TaskStatusSource::new()),
            Arc::new(ProcessMonotonicClock::new()),
            METRIC_PUBLISH_MIN_INTERVAL,
        ));
        owner.release_to_observers();
        (Arc::clone(&owner), TaskStatusReporter::new(owner))
    }

    /// Nothing this host renders or refuses may carry credential material.
    #[test]
    fn credential_material_never_appears_in_a_rendering_or_a_rejection() {
        let fixture = Fixture::new();
        let context = context(1);
        fixture
            .establish(
                context,
                vec![],
                no_contribution(),
                &credential(1, SECRET_SENTINEL, live_until()),
            )
            .expect("establish");

        let rendered = format!("{:?}", fixture.host);
        assert!(
            !rendered.contains(SECRET_SENTINEL),
            "credential material leaked into Debug: {rendered}"
        );
        assert!(rendered.contains("[REDACTED]"), "{rendered}");

        // A refused rotation is the rendering path that describes what arrived.
        let refused = fixture
            .host
            .advance_shared_domain(
                context,
                &QueryContextDomainUpdate::Credential(credential(2, SECRET_SENTINEL, 1)),
            )
            .map(|()| String::new())
            .unwrap_or_else(|rejection| rejection.to_string());
        assert!(!refused.is_empty(), "the rotation must have been refused");
        assert!(!refused.contains(SECRET_SENTINEL), "{refused}");

        // And the update itself still renders redacted on the way in.
        let update = credential(3, SECRET_SENTINEL, live_until());
        let rendered = format!("{update:?}");
        assert!(!rendered.contains(SECRET_SENTINEL), "{rendered}");
    }
}

/// The query-scoped facts one task's preparation needs.
///
/// The execution side only ever holds a `&TaskDescriptor`, and a `TaskIdentity`
/// carries no frontend process id, so it structurally cannot name a
/// `QueryContextRef`. This maps the execution id it does have onto the context
/// that installed the facts, and refuses when there is none: an unestablished
/// or released context has no runtime filter, no catalog lease, and no
/// storage credential, and there is no process-level substitute for any of
/// them that a task could legitimately fall back to.
use novarocks_execution::runtime::fragment::io::{FragmentEvent, FragmentEventSink};
use novarocks_execution::runtime_filter::RuntimeFilterSessionRef;
use novarocks_execution::task_execution::domain::{CodecOwnedContent, DomainVersion};
use novarocks_proto_codec::task_execution::domain::stored_message;
use novarocks_proto_models::filter;
use novarocks_types::UniqueId;

use crate::connector::{ConnectorExecutionReadBinding, ConnectorExecutionWriteBinding};
use crate::task_execution::execution_host::TaskQueryContextFacts;
use novarocks_spi::connector::CatalogHandle;

impl TaskQueryContextFacts for NativeQueryContextHost {
    fn query_options(
        &self,
        execution: QueryExecutionId,
    ) -> Result<QueryContextOptions, HostRejection> {
        let context = self.context_for_execution(execution).ok_or_else(|| {
            HostRejection::new(
                TaskFailureCategory::Execution,
                format!("task of {execution:?} has no established query context"),
            )
        })?;
        let installed = self.active_context(context)?;
        let facts = installed
            .facts
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if facts.released {
            return Err(HostRejection::new(
                TaskFailureCategory::Execution,
                "the query context released its query options before task preparation",
            ));
        }
        facts.query_options.clone().ok_or_else(|| {
            HostRejection::new(
                TaskFailureCategory::Internal,
                "active query context has no installed query options",
            )
        })
    }

    fn runtime_filter_session(
        &self,
        execution: QueryExecutionId,
        fragment_instance_id: UniqueId,
        expects_bindings: bool,
    ) -> Result<Option<RuntimeFilterSessionRef>, HostRejection> {
        let Some(participant) = self.participant_for_execution(execution) else {
            // A query that installs no filter on this backend is the ordinary
            // case; a task whose plan binds one and finds none is not, because
            // it would otherwise read unfiltered and call that success.
            return if expects_bindings {
                Err(protocol(&format!(
                    "task of {execution:?} binds a runtime filter but its query context installed \
                     none on this backend"
                )))
            } else {
                Ok(None)
            };
        };
        participant
            .session_for_fragment(execution, fragment_instance_id, expects_bindings)
            .map_err(|error| protocol(&format!("runtime filter session refused: {error}")))
    }

    fn runtime_filter_event_sink(
        &self,
        execution: QueryExecutionId,
        fragment_instance_id: UniqueId,
    ) -> Arc<dyn FragmentEventSink> {
        Arc::new(ContextRuntimeFilterEventSink {
            participant: self.participant_for_execution(execution),
            fragment_instance_id,
        })
    }

    fn bind_runtime_filter_feedback(
        &self,
        execution: QueryExecutionId,
        carrier: TaskIdentity,
        reporter: &TaskStatusReporter,
    ) -> bool {
        let Some(context) = self.context_for_execution(execution) else {
            return false;
        };
        let installed = {
            let contexts = self
                .contexts
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            match contexts.get(context) {
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
        let sink = Arc::new(TaskRuntimeFilterFeedbackEgress::new(
            carrier,
            reporter.clone(),
        ));
        // The participant holds this weakly, exactly as the lifecycle egress
        // was held: a background publisher must not be able to keep the query
        // context alive through its own sink.
        participant.set_frontend_feedback_sink(Arc::downgrade(
            &(Arc::clone(&sink) as Arc<dyn BackendFrontendFeedbackSink>),
        ));
        facts.feedback = Some(sink);
        true
    }

    fn deliver_task_dynamic_filter(
        &self,
        execution: QueryExecutionId,
        _fragment_instance_id: UniqueId,
        version: DomainVersion,
        payload: &Arc<dyn CodecOwnedContent>,
    ) -> Result<(), HostRejection> {
        let envelope = stored_message::<filter::RuntimeFilterEnvelope>(payload.as_ref())
            .ok_or_else(|| {
                internal(&format!(
                    "task dynamic filter version {} is not a codec-produced runtime filter \
                     envelope",
                    version.get()
                ))
            })?
            .clone();
        let participant = self.participant_for_execution(execution).ok_or_else(|| {
            protocol(&format!(
                "task dynamic filter for {execution:?} has no installed participant to accept it"
            ))
        })?;
        // The one decoder that turns a wire envelope into a backend one lives
        // in the runtime-filter transport. Reusing it is what keeps this from
        // becoming a second authority over the same wire shape.
        let response = crate::runtime_filter::rpc::handle_runtime_filter_envelope(
            participant as Arc<dyn crate::runtime_filter::rpc::BackendRuntimeFilterEnvelopeIngress>,
            envelope,
        )
        .map_err(|status| protocol(&format!("task dynamic filter was refused: {status}")))?;
        // A duplicate is a legal answer to a replayed push; only a rejection
        // or an unknown status means the filter did not land.
        let accepted = matches!(
            filter::RuntimeFilterAcceptStatus::try_from(response.accept_status),
            Ok(filter::RuntimeFilterAcceptStatus::Accepted
                | filter::RuntimeFilterAcceptStatus::Duplicate)
        );
        if !accepted {
            return Err(protocol(&format!(
                "task dynamic filter version {} was not accepted: {}",
                version.get(),
                response.rejection_reason
            )));
        }
        Ok(())
    }

    fn catalog_read_execution(
        &self,
        execution: QueryExecutionId,
        handle: &CatalogHandle,
    ) -> Result<ConnectorExecutionReadBinding, String> {
        let runtime = self
            .catalog_manager
            .resolve_for_query(execution, handle)
            .ok_or_else(|| missing_catalog_lease(handle))?;
        runtime
            .read()
            .cloned()
            .ok_or_else(|| missing_catalog_capability(handle, "read"))
    }

    fn catalog_write_execution(
        &self,
        execution: QueryExecutionId,
        handle: &CatalogHandle,
    ) -> Result<ConnectorExecutionWriteBinding, String> {
        let runtime = self
            .catalog_manager
            .resolve_for_query(execution, handle)
            .ok_or_else(|| missing_catalog_lease(handle))?;
        runtime
            .write()
            .cloned()
            .ok_or_else(|| missing_catalog_capability(handle, "write"))
    }

    fn storage_resolver(
        &self,
        execution: QueryExecutionId,
    ) -> Result<Arc<dyn ConnectorStorageResolver>, HostRejection> {
        // Deliberately a refusal, not a permissive default: a scan that
        // reached read time with no vended credential would fail there anyway,
        // and failing here says which context is missing.
        self.context_for_execution(execution)
            .and_then(|context| self.storage_resolver_for_context(context))
            .ok_or_else(|| {
                protocol(&format!(
                    "no established query context on this backend holds storage credentials for \
                     {execution:?}"
                ))
            })
    }
}

fn missing_catalog_lease(handle: &CatalogHandle) -> String {
    format!(
        "no query-leased catalog runtime exists for {}@{}",
        handle.catalog_name().as_str(),
        handle.version().short_hex()
    )
}

fn missing_catalog_capability(handle: &CatalogHandle, capability: &str) -> String {
    format!(
        "catalog runtime for {}@{} has no typed {capability} capability",
        handle.catalog_name().as_str(),
        handle.version().short_hex()
    )
}

/// Folds a running fragment's runtime-filter evidence into the participant
/// that owns the installed consumer identity.
///
/// Progress and profile events are dropped here: they belong to the task's own
/// metrics owner, which is a different sink. Silently folding them into the
/// filter participant would put two unrelated facts behind one identity.
struct ContextRuntimeFilterEventSink {
    /// Resolved once, when the fragment is installed: the context has
    /// materialized by then, and a query with no filter on this backend has
    /// nothing to fold into.
    participant: Option<Arc<RuntimeFilterParticipant>>,
    fragment_instance_id: UniqueId,
}

impl FragmentEventSink for ContextRuntimeFilterEventSink {
    fn record(&self, event: FragmentEvent) {
        let Some(participant) = self.participant.as_ref() else {
            return;
        };
        match event {
            FragmentEvent::RuntimeFilterRowEffect(effect) => {
                participant.record_row_effect(self.fragment_instance_id, effect);
            }
            FragmentEvent::RuntimeFilterScanUnitOutcome(outcome) => {
                participant.record_scan_unit_outcome(self.fragment_instance_id, outcome);
            }
            FragmentEvent::Progress(_) | FragmentEvent::ProfileSnapshot(_) => {}
        }
    }
}
