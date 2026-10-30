// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Fixed CountOnly consumption and stable, activation-exact Profile receiving custody.

use std::sync::{Arc, Mutex};

use novarocks_coordination_contract::{AttemptActivationIdentity, ExecutionEffect};
use novarocks_execution_contract::{
    SafeDetail, TaskIdentity, failure::FailureProjection, root_result::RootResultEnd,
};
use novarocks_query_application::{
    api::{
        ComputationReadySource, ComputationReadyToken, InternalComputationConsumer,
        InternalConsumerActivation, InternalConsumerFuture, InternalResultDelivery, PlanSeal,
        QueryExecutionError, ResultRowCarrier,
    },
    execution_error::{ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason},
    preparation::{ExecutionFinishBoundary, FrozenExecutionDescription},
};
use novarocks_result_contract::RootOutputKind;

use super::attempt_handoff::{
    AttemptHandoffClaimPort, AttemptHandoffOfferPort, attempt_handoff_mailbox,
};
use crate::{
    query_execution::{
        artifact::PreparedDistributedAttemptTemplate,
        contract::ResolvedQueryOptions,
        lifecycle_plan::AttemptCredentialStorage,
        logical_execution::{ComputationDiagnostics, PreparedLogicalComputation},
    },
    task_execution::{
        profile_diagnostics::{ProfileDiagnosticObserver, ProfileDiagnosticSnapshot},
        protocol_facts::AttemptProtocolProvenance,
    },
};

/// Absence of diagnostics never changes an already accepted computation cut.
/// Per-Task missing reasons remain authored by the original TE collector.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProfileSnapshotMissing {
    ObserverNotInstalled,
    PublisherExitedBeforeSnapshot,
    ForeignComputationCut,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProfileBinding {
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
    plan: PlanSeal,
}

impl ProfileBinding {
    fn from_source(source: &ComputationReadySource) -> Self {
        Self {
            activation: source.activation(),
            root: source.root(),
            plan: source.plan(),
        }
    }

    fn verify_ready(
        self,
        ready: ComputationReadyToken,
        end: RootResultEnd,
    ) -> Result<(), QueryExecutionError> {
        if ready.activation() != self.activation || ready.root() != self.root || ready.end() != end
        {
            return Err(profile_error(
                "Profile receiving custody does not match the accepted computation cut",
            ));
        }
        Ok(())
    }
}

/// The observer, original storage and frozen Task provenance reach this
/// receiving owner before the consumer proposes Ready. No payload is held only
/// by the destructible consumer future at that boundary.
struct StagedProfile {
    binding: ProfileBinding,
    end: RootResultEnd,
    provenance: AttemptProtocolProvenance,
    observer: Option<ProfileDiagnosticObserver>,
    _credentials: Option<Arc<AttemptCredentialStorage>>,
}

enum ReceivingState {
    Vacant,
    Reserved(ProfileBinding),
    Staged(StagedProfile),
    Taken(ComputationReadyToken),
}

struct ProfileReceivingHolder {
    state: Mutex<ReceivingState>,
}

impl ProfileReceivingHolder {
    fn new() -> Self {
        Self {
            state: Mutex::new(ReceivingState::Vacant),
        }
    }

    /// One pending receiving slot, with monotonic fences rather than an
    /// activation registry. Superseded diagnostics have no publication or
    /// deletion authority; their original Native owner still supervises I/O.
    fn reserve(&self, binding: ProfileBinding) -> Result<(), QueryExecutionError> {
        let retired = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let prior = match &*state {
                ReceivingState::Vacant => None,
                ReceivingState::Reserved(prior) => Some(*prior),
                ReceivingState::Staged(prior) => Some(prior.binding),
                ReceivingState::Taken(_) => {
                    return Err(profile_error(
                        "Profile receiving custody was already transferred",
                    ));
                }
            };
            if let Some(prior) = prior
                && (prior.activation.actor() != binding.activation.actor()
                    || prior.activation.execution().query_id()
                        != binding.activation.execution().query_id()
                    || prior.plan != binding.plan
                    || prior.activation.generation() >= binding.activation.generation())
            {
                return Err(profile_error(
                    "Profile receiving custody cannot admit a stale or foreign activation",
                ));
            }
            std::mem::replace(&mut *state, ReceivingState::Reserved(binding))
        };
        // Dropping a final credential Arc may revoke its original route.
        // Resource destructors never run under the receiving lock.
        drop(retired);
        Ok(())
    }

    fn stage(&self, payload: StagedProfile) -> Result<(), QueryExecutionError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if !matches!(&*state, ReceivingState::Reserved(binding) if *binding == payload.binding) {
            // The parameter is dropped after the lock guard, outside the lock.
            return Err(profile_error(
                "Profile proof cannot replace another activation's receiving custody",
            ));
        }
        *state = ReceivingState::Staged(payload);
        Ok(())
    }

    fn take_ready(
        &self,
        ready: ComputationReadyToken,
    ) -> Result<ReadyProfileComputation, QueryExecutionError> {
        let payload = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            match &*state {
                ReceivingState::Staged(payload) => {
                    payload.binding.verify_ready(ready, payload.end)?;
                }
                ReceivingState::Taken(original) if *original == ready => {
                    return Err(profile_error(
                        "Profile computation result was already transferred",
                    ));
                }
                _ => {
                    return Err(profile_error(
                        "Profile computation has no matching staged receiving custody",
                    ));
                }
            }
            let ReceivingState::Staged(payload) =
                std::mem::replace(&mut *state, ReceivingState::Taken(ready))
            else {
                unreachable!("staged payload checked under the same lock")
            };
            payload
        };
        Ok(ReadyProfileComputation { ready, payload })
    }
}

/// The original statement owner retains this handle while waiting for QA's
/// start result. An old attempt never sends a separate terminal outcome here.
pub(crate) struct ProfileComputationOutput {
    receiving: Arc<ProfileReceivingHolder>,
}

impl ProfileComputationOutput {
    /// Supply only the real token returned by ExecutionHandle::ready_token.
    /// The token has no public constructor and the actor mints one cut only.
    /// Reading it here does not depend on a second consumer publication await.
    pub(crate) fn take_ready(
        &mut self,
        ready: ComputationReadyToken,
    ) -> Result<ReadyProfileComputation, QueryExecutionError> {
        self.receiving.take_ready(ready)
    }
}

/// Current count and the original observation capability, never a rendered
/// profile or a claim that remote resources have exited.
pub(crate) struct ReadyProfileComputation {
    ready: ComputationReadyToken,
    payload: StagedProfile,
}

impl ReadyProfileComputation {
    pub(crate) const fn ready_token(&self) -> ComputationReadyToken {
        self.ready
    }

    pub(crate) const fn output_rows(&self) -> u64 {
        self.payload.end.output_rows
    }

    pub(crate) fn expected_tasks(&self) -> &[TaskIdentity] {
        self.payload.provenance.tasks()
    }

    /// Keep the same attempt credential table through diagnostic observation.
    /// Native retains its own original custody independently until actual exit.
    #[cfg(all(test, novarocks_dev_controls))]
    pub(crate) fn original_credentials(&self) -> Option<&Arc<AttemptCredentialStorage>> {
        self.payload._credentials.as_ref()
    }

    pub(crate) fn current_diagnostics(
        &self,
    ) -> Result<Option<Arc<ProfileDiagnosticSnapshot>>, ProfileSnapshotMissing> {
        self.payload
            .observer
            .as_ref()
            .ok_or(ProfileSnapshotMissing::ObserverNotInstalled)?
            .current(self.ready)
            .map_err(|_| ProfileSnapshotMissing::ForeignComputationCut)
    }

    /// TE owns the original absolute diagnostic deadline, missing reasons and
    /// pending RPC. Dropping this wait never stops or restarts that owner.
    pub(crate) async fn wait_diagnostics(
        &mut self,
    ) -> Result<Arc<ProfileDiagnosticSnapshot>, ProfileSnapshotMissing> {
        loop {
            if let Some(snapshot) = self.current_diagnostics()? {
                return Ok(snapshot);
            }
            let observer = self
                .payload
                .observer
                .as_mut()
                .ok_or(ProfileSnapshotMissing::ObserverNotInstalled)?;
            if observer.changed().await.is_err() {
                // A final immutable snapshot may have been published before
                // the sender exited. Always observe it before reporting loss.
                return self
                    .current_diagnostics()?
                    .ok_or(ProfileSnapshotMissing::PublisherExitedBeforeSnapshot);
            }
        }
    }
}

struct ProfileComputationConsumer {
    plan: PlanSeal,
    handoffs: AttemptHandoffClaimPort,
    receiving: Arc<ProfileReceivingHolder>,
}

impl std::fmt::Debug for ProfileComputationConsumer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProfileComputationConsumer")
            .finish_non_exhaustive()
    }
}

/// Closed Profile construction. The caller passes the frozen template's seal,
/// not a key reconstructed from display annotations or a Task profile tree.
pub(crate) fn profile_computation_consumer(
    description: &FrozenExecutionDescription,
    plan: PlanSeal,
) -> Result<
    (
        Arc<dyn InternalComputationConsumer>,
        AttemptHandoffOfferPort,
        ProfileComputationOutput,
    ),
    QueryExecutionError,
> {
    if description.finish_boundary() != ExecutionFinishBoundary::ComputationReady
        || !description.matches_plan_seal(plan)
        || description.effect() != ExecutionEffect::None
        || !matches!(
            description
                .row_carrier()
                .map_err(|_| profile_error("Profile computation has no frozen root carrier"))?,
            ResultRowCarrier::Relayed {
                kind: RootOutputKind::CountOnly,
                client_rows: None,
            }
        )
    {
        return Err(profile_error(
            "Profile computation requires its exact effect-free CountOnly finish contract",
        ));
    }
    let (offers, handoffs) = attempt_handoff_mailbox();
    let receiving = Arc::new(ProfileReceivingHolder::new());
    let consumer: Arc<dyn InternalComputationConsumer> = Arc::new(ProfileComputationConsumer {
        plan,
        handoffs,
        receiving: Arc::clone(&receiving),
    });
    Ok((consumer, offers, ProfileComputationOutput { receiving }))
}

/// Concrete preparation connects the fixed consumer, exact handoff offer and
/// diagnostic mode without a caller assembling independent registrations.
/// All fallible Profile preparation precedes binding the original access.
pub(crate) struct PreparedProfileComputationParts {
    computation: Option<crate::query_execution::logical_execution::PreparedLogicalComputationParts>,
    output: Option<ProfileComputationOutput>,
}
pub(crate) fn prepare_profile_computation_parts(
    description: FrozenExecutionDescription,
    plan: PlanSeal,
    options: Arc<ResolvedQueryOptions>,
) -> Result<PreparedProfileComputationParts, QueryExecutionError> {
    let description = description
        .into_internal_computation()
        .map_err(|_| profile_error("Profile preparation has no frozen internal result contract"))?;
    let (consumer, offers, output) = profile_computation_consumer(&description, plan)?;
    let computation =
        crate::query_execution::logical_execution::PreparedLogicalComputationParts::try_new(
            description,
            plan,
            options,
            consumer,
            offers,
            ComputationDiagnostics::Profile,
        )
        .map_err(|_| profile_error("Profile preparation differs from its Native template"))?;
    Ok(PreparedProfileComputationParts {
        computation: Some(computation),
        output: Some(output),
    })
}
impl PreparedProfileComputationParts {
    pub(crate) fn move_in(
        parts: &mut Option<Self>,
        template: &mut Option<PreparedDistributedAttemptTemplate>,
        destination: &mut Option<(PreparedLogicalComputation, ProfileComputationOutput)>,
    ) -> Result<(), QueryExecutionError> {
        if destination.is_some() {
            return Err(profile_error("Profile receiver is occupied"));
        }
        let original = parts
            .as_mut()
            .ok_or_else(|| profile_error("original Profile parts are absent"))?;
        if original.output.is_none() {
            return Err(profile_error(
                "original Profile receiving cells are occupied",
            ));
        }
        let mut received = None;
        crate::query_execution::logical_execution::PreparedLogicalComputationParts::move_in(
            &mut original.computation,
            template,
            &mut received,
        )
        .map_err(|_| profile_error("Profile preparation differs from its Native template"))?;
        *destination = Some((received.unwrap(), original.output.take().unwrap()));
        let retired = parts.take();
        drop(retired);
        Ok(())
    }
}

impl InternalComputationConsumer for ProfileComputationConsumer {
    // This CountOnly owner never constructs or submits a local CPU job.
    fn begin_local_close(&self) {}

    fn poll_local_closed(
        &self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), novarocks_query_application::api::StatementNativeHealthFailure>>
    {
        std::task::Poll::Ready(Ok(()))
    }

    fn consume(&self, activation: InternalConsumerActivation) -> InternalConsumerFuture {
        let (mut stream, source) = activation.into_parts();
        let binding = ProfileBinding::from_source(&source);
        let receiving = Arc::clone(&self.receiving);
        // Validate the fixed carrier before waiting, but authorize receiving
        // custody only after the original handoff has accepted this source.
        let reservation = if binding.plan != self.plan
            || stream.activation() != binding.activation
            || !matches!(
                stream.carrier(),
                ResultRowCarrier::Relayed {
                    kind: RootOutputKind::CountOnly,
                    client_rows: None,
                }
            ) {
            Err(profile_error(
                "Profile activation differs from its frozen CountOnly consumer",
            ))
        } else {
            Ok(())
        };
        let handoff = self.handoffs.claim(&source);
        Box::pin(async move {
            reservation?;
            let original = handoff.await?;
            if original.activation() != binding.activation
                || original.root() != binding.root
                || original.plan() != binding.plan
            {
                return Err(profile_error("Profile handoff has a foreign source"));
            }
            // A rejected foreign claim must not poison the logical holder's
            // first actor fence. A delayed old authorized claim is still
            // refused by the monotonic receiving generation below.
            receiving.reserve(binding)?;
            let (mut protocol, credentials, observer) = original.into_parts();
            let initial = protocol
                .changed(binding.activation, binding.root, binding.plan)
                .await
                .map_err(|_| profile_error("Profile has no original frozen Task provenance"))?;
            let provenance = AttemptProtocolProvenance::from_initial(&initial)
                .map_err(|_| profile_error("Profile protocol provenance is not an initial view"))?;
            // Profile needs the original frozen Task membership only. QA
            // continues to own root/failure observation; retain no status copy.
            drop(initial);
            drop(protocol);
            // Root Finished and shared failure/Establish gates belong to QA.
            // CountOnly has no Arrow/body codec or independent success seal.
            let end = match stream.next().await? {
                Some(InternalResultDelivery::End(delivery)) => delivery,
                Some(InternalResultDelivery::Segment(delivery)) => {
                    let error = profile_error("CountOnly Profile received a body segment");
                    let (reply, consumed) = delivery.into_parts();
                    drop(reply);
                    consumed.fail(error.clone());
                    return Err(error);
                }
                None => return Err(profile_error("CountOnly Profile has no original End")),
            };
            if end.activation() != binding.activation {
                let error = profile_error("Profile End belongs to another activation");
                end.fail(error.clone());
                return Err(error);
            }
            let original_end = end.end();
            let decoded = end.complete().await?;
            receiving.stage(StagedProfile {
                binding,
                end: original_end,
                provenance,
                observer,
                _credentials: credentials,
            })?;
            // Receiving custody is now stable outside this future. The actor
            // remains the sole selected-outcome author, across replacement.
            let ready = source.accept(decoded).await?;
            debug_assert!(binding.verify_ready(ready, original_end).is_ok());
            Ok(())
        })
    }
}

fn profile_error(message: &'static str) -> QueryExecutionError {
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        QueryFailureReason::PreparationProtocol.failure_fact(),
        SafeDetail::truncating(message),
        ExecutionFailureSite::Coordinator,
    ))
}
