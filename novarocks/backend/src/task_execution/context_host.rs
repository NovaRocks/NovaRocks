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
//! One establish installs three shared facts on this backend — catalog
//! runtimes, a runtime-filter participant, and the query's vended credentials —
//! and one release takes all three back. This is the only place that owns that
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
use novarocks_execution::task_execution::identity::QueryContextRef;
use novarocks_execution::task_execution::operation::QueryContextDomainUpdate;
use novarocks_execution::task_execution::status::TaskFailureCategory;
use novarocks_proto_codec::lifecycle::{QueryTerminationReason, RuntimeFilterContribution};
use novarocks_proto_codec::task_execution::domain::WireCredential;
use novarocks_proto_models::novarocks as proto;
use novarocks_spi::connector::{CatalogProperties, ConnectorStorageResolver};
use novarocks_types::QueryExecutionId;
use tracing::error;

use super::credential_slot::QueryContextCredentialSlot;
use super::host::{HostRejection, QueryContextHost, SharedFactsRequest};
use super::shared_facts::{catalog_bindings, credential_material, runtime_filter_install};
use crate::BackendDataRuntime;
use crate::connector::ConnectorExecutionRoleBinding;
use crate::connector::catalog_manager::{
    CatalogManager, CatalogManagerError, ConnectorExecutionRoleBindingFactorySet,
};
use crate::query_lifecycle::{QueryLifecycleError, QueryLifecycleErrorCode};
use crate::runtime_filter::install_decode::{
    DecodedRuntimeFilterContribution, decode_runtime_filter_contribution,
};
use crate::runtime_filter::participant::{
    RuntimeFilterParticipant, RuntimeFilterParticipantFactory,
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
    /// `None` either because the query installs no runtime filter on this
    /// backend, or because the side that tore the context down already took it.
    participant: Option<Arc<RuntimeFilterParticipant>>,
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
                participant: None,
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
    pub fn storage_resolver(
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
    fn tear_down(&self, context: QueryContextRef, installed: &InstalledContext) {
        let participant = {
            let mut facts = installed
                .facts
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            facts.released = true;
            facts.participant.take()
        };
        installed.credentials.clear();
        if let Some(participant) = participant {
            close_participant(context, &participant);
        }
        // Unconditional: a lease may have been taken by an install that is
        // still unwinding, and releasing a query that holds none is a no-op.
        self.catalog_manager
            .release_query(context.query_execution_id());
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
        material: &WireCredential,
    ) -> Result<(), HostRejection> {
        let execution_id = context.query_execution_id();

        // Credentials first: a catalog runtime is the thing most likely to need
        // scoped storage access, so the authority has to exist before the
        // binding that may reach for it.
        installed.credentials.install(material)?;
        self.still_establishing(installed)?;

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

impl QueryContextHost for NativeQueryContextHost {
    fn materialize(&self, request: SharedFactsRequest<'_>) -> Result<(), HostRejection> {
        let context = request.context();

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
            let material = credential_material(request.initial_credential())?;
            Ok((catalogs, contribution, material))
        });
        let (catalogs, contribution, material) = match projected {
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

        match self.install_shared_facts(context, &installed, catalogs, contribution, material) {
            Ok(()) => Ok(()),
            Err(rejection) => {
                // Undo here as well as in `release`. The owner does call
                // `release` after a failed materialize, but a host that leaves
                // a half-installed context behind when it says "no" would be
                // relying on that, and the trait does not promise it.
                self.tear_down(context, &installed);
                self.contexts
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .remove_if_same(context, &installed);
                Err(rejection)
            }
        }
    }

    fn release(&self, context: QueryContextRef) {
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
                    return;
                }
            }
        };
        self.tear_down(context, &installed);
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

fn runtime_filter_rejection(error: QueryLifecycleError) -> HostRejection {
    let category = match error.code() {
        QueryLifecycleErrorCode::InvalidManifest
        | QueryLifecycleErrorCode::Conflict
        | QueryLifecycleErrorCode::StaleBackend => TaskFailureCategory::Protocol,
        QueryLifecycleErrorCode::Capacity => TaskFailureCategory::ResourceExhausted,
        QueryLifecycleErrorCode::Terminated => TaskFailureCategory::Execution,
        // The only category that names a peer route failing.
        QueryLifecycleErrorCode::Transport => TaskFailureCategory::Exchange,
        QueryLifecycleErrorCode::Internal => TaskFailureCategory::Internal,
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
    use crate::query_lifecycle::{QueryLifecycleError, QueryLifecycleErrorCode};
    use crate::rpc::runtime::test_backend_data_runtime;
    use crate::runtime_filter::install_decode::DecodedRuntimeFilterContribution;
    use crate::runtime_filter::participant::{
        BackendRuntimeFilterParticipantFactory, RuntimeFilterParticipant,
        RuntimeFilterParticipantFactory,
    };
    use crate::task_execution::host::{QueryContextHost, SharedFactsRequest};

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
            ConnectorExecutionRoleBinding::try_new(properties.clone(), None, None, None)
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
        ) -> Result<Arc<RuntimeFilterParticipant>, QueryLifecycleError> {
            self.ledger.installs.fetch_add(1, Ordering::SeqCst);
            let participant =
                BackendRuntimeFilterParticipantFactory::new(test_backend_data_runtime())
                    .install(execution_id, contribution)?;
            let ledger = Arc::clone(&self.ledger);
            Ok(
                participant.with_close_hook_for_test(Arc::new(move |_participant, _reason| {
                    ledger.closes.fetch_add(1, Ordering::SeqCst);
                    if ledger.fail_close.load(Ordering::SeqCst) {
                        return Err(QueryLifecycleError::new(
                            QueryLifecycleErrorCode::Internal,
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
            let catalog = catalog_payload(catalogs);
            let filter = filter_payload(contribution);
            self.host.materialize(SharedFactsRequest::new(
                context, &catalog, &filter, credential,
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
                .storage_resolver(context)
                .expect("an active context resolves storage")
                .resolve_vended_s3(&storage_request("s3://bucket/a/file.parquet"))
                .is_ok(),
            "the installed credential must serve its own scope"
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
            fixture.host.storage_resolver(context).is_none(),
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
            fixture.host.storage_resolver(context).is_none(),
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
        assert!(fixture.host.storage_resolver(context).is_none());
    }

    /// A release that arrives before its establish is called exactly once by
    /// the owner, so the establish that follows must refuse rather than install
    /// facts nothing will take back.
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
        let refusal = fixture
            .host
            .materialize(SharedFactsRequest::new(
                context,
                &wrong_domain,
                &wrong_domain,
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
        assert!(fixture.host.storage_resolver(context).is_none());
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
        let credential = credential(1, SECRET_SENTINEL, live_until());
        let rejection = fixture
            .host
            .materialize(SharedFactsRequest::new(
                context,
                &catalog,
                &filter,
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
        assert!(fixture.host.storage_resolver(context).is_none());
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
        assert!(fixture.host.storage_resolver(context).is_some());
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
                .storage_resolver(context)
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
                .storage_resolver(context)
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
        assert!(fixture.host.storage_resolver(released).is_none());
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
        assert!(fixture.host.storage_resolver(context).is_none());

        fixture.host.release(context);
        assert_eq!(
            fixture.filter_factory.ledger.closes.load(Ordering::SeqCst),
            1,
            "a failed close must not be retried blindly"
        );
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
