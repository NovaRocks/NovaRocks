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

//! Statistics consumers; domain algorithms remain with their original authors.

use novarocks_coordination_contract::{AttemptActivationIdentity, ExecutionEffect, RecoveryMode};
use novarocks_execution_contract::{TaskIdentity, root_result::RootResultEnd};
use novarocks_query_application::{
    admitted_query_context::QueryResultCapacityBinding,
    api::{
        ComputationReadySource, ComputationReadyToken, InternalComputationConsumer,
        InternalConsumerActivation, InternalConsumerFuture, InternalResultDelivery,
        InternalResultStream, PlanSeal, QueryExecutionError, ResultRowCarrier,
    },
    cancellation::{QueryCancellationReason, QueryCancellationView},
    preparation::{ExecutionFinishBoundary, FrozenExecutionDescription},
};
use novarocks_result_contract::{InternalResultDomain, RootOutputKind};
use novarocks_spi::connector::StatisticsArtifactDraft;
use std::sync::{Arc, Mutex};

use super::{
    attempt_handoff::{AttemptHandoffClaimPort, AttemptHandoffOfferPort, attempt_handoff_mailbox},
    domain_cpu::{DomainCpuCustody, domain_error},
    domain_state::InternalDomainState,
};
use crate::{
    native::data_runtime::FrontendDataRuntime,
    query_execution::{
        internal_result_cpu::{InternalResultCpu, InternalResultValue},
        lifecycle_plan::AttemptCredentialStorage,
        statistics::{StatisticsCollectionProgram, completion::StatisticsProtocolTracker},
    },
    task_execution::protocol_facts::AttemptProtocolFactsReceiver,
};

/// The stable statement owner retains this exact CPU custody alongside the
/// concrete output. Neither a result waiter nor a consumer unwind owns its join.
pub(crate) struct DecodedConsumerRuntime {
    pub(crate) binding: QueryResultCapacityBinding,
    pub(crate) computation:
        Option<novarocks_workload_control::joint_admission::InternalComputationGrant>,
    pub(crate) cpu: InternalResultCpu,
    pub(crate) runtime: FrontendDataRuntime,
    pub(crate) cancellation: QueryCancellationView,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Binding {
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
    plan: PlanSeal,
}
impl Binding {
    fn from_source(source: &ComputationReadySource) -> Self {
        Self {
            activation: source.activation(),
            root: source.root(),
            plan: source.plan(),
        }
    }
    fn matches(self, token: ComputationReadyToken, end: RootResultEnd) -> bool {
        self.activation == token.activation() && self.root == token.root() && end == token.end()
    }
}

#[derive(Clone, Copy)]
enum Domain {
    Statistics,
}
impl Domain {
    fn kind(self) -> RootOutputKind {
        RootOutputKind::InternalFacts(match self {
            Self::Statistics => InternalResultDomain::StatisticsArtifactV1,
        })
    }
}

enum Payload {
    Statistics(Vec<StatisticsArtifactDraft>),
}
struct Staged {
    binding: Binding,
    end: RootResultEnd,
    payload: InternalResultValue<Payload>,
    // Preserve the actual attempt's original table through business handoff.
    _credentials: Option<Arc<AttemptCredentialStorage>>,
}
enum Receiving {
    Vacant,
    Reserved(Binding),
    Staged(Box<Staged>),
    Taken(ComputationReadyToken),
}
struct Holder {
    #[cfg(test)]
    decoded_end: std::sync::OnceLock<RootResultEnd>,
    #[cfg(test)]
    stream_failure: std::sync::OnceLock<QueryExecutionError>,
    receiving: Mutex<Receiving>,
    cpu: Arc<DomainCpuCustody>,
}
impl Holder {
    fn reserve(&self, binding: Binding) -> Result<(), QueryExecutionError> {
        let mut receiving = self.receiving.lock().unwrap_or_else(|e| e.into_inner());
        if !matches!(*receiving, Receiving::Vacant) {
            return Err(domain_error(
                "NoRecovery domain consumer already owns its original activation",
            ));
        }
        *receiving = Receiving::Reserved(binding);
        Ok(())
    }
    fn stage(&self, staged: Staged) -> Result<(), QueryExecutionError> {
        let mut receiving = self.receiving.lock().unwrap_or_else(|e| e.into_inner());
        if !matches!(*receiving, Receiving::Reserved(binding) if binding == staged.binding) {
            return Err(domain_error(
                "decoded payload differs from its original receiving custody",
            ));
        }
        *receiving = Receiving::Staged(Box::new(staged));
        Ok(())
    }
    fn take(
        &self,
        token: ComputationReadyToken,
        domain: Domain,
    ) -> Result<Staged, QueryExecutionError> {
        let mut receiving = self.receiving.lock().unwrap_or_else(|e| e.into_inner());
        match &*receiving {
            Receiving::Staged(staged) if staged.binding.matches(token, staged.end) => {
                if !matches!(
                    (domain, staged.payload.value()),
                    (Domain::Statistics, Payload::Statistics(_))
                ) {
                    return Err(domain_error("decoded output receiver has a foreign domain"));
                }
            }
            Receiving::Taken(original) if *original == token => {
                return Err(domain_error("decoded result was already transferred"));
            }
            _ => {
                return Err(domain_error(
                    "decoded result has no matching staged computation cut",
                ));
            }
        }
        let Receiving::Staged(staged) = std::mem::replace(&mut *receiving, Receiving::Taken(token))
        else {
            unreachable!()
        };
        Ok(*staged)
    }
}

pub(crate) struct StatisticsComputationOutput {
    holder: Arc<Holder>,
    binding: QueryResultCapacityBinding,
}

pub(crate) struct ReadyStatisticsComputation {
    pub(crate) _binding: QueryResultCapacityBinding,
    pub(crate) artifacts: Vec<StatisticsArtifactDraft>,
    pub(crate) _credentials: Option<Arc<AttemptCredentialStorage>>,
}

impl StatisticsComputationOutput {
    /// Passive observation only: the actual CPU EOF and checked End receipt
    /// have both completed. Reading this never polls or advances any Work.
    #[cfg(test)]
    pub(crate) fn decoded_end_for_fixture(&self) -> Option<RootResultEnd> {
        self.holder.decoded_end.get().copied()
    }
    #[cfg(test)]
    pub(crate) fn stream_failure_for_fixture(&self) -> Option<QueryExecutionError> {
        self.holder.stream_failure.get().cloned()
    }

    pub(crate) fn take_ready(
        &mut self,
        token: ComputationReadyToken,
    ) -> Result<ReadyStatisticsComputation, QueryExecutionError> {
        let staged = self.holder.take(token, Domain::Statistics)?;
        // Project the original positive payload before releasing the decoded
        // value's carrier. No manifest/compute authority crosses publication.
        let binding = self
            .binding
            .clone()
            .into_statement_retention()
            .map_err(|error| {
                use novarocks_execution_contract::failure::FailureProjection;
                QueryExecutionError::from_execution(
                novarocks_query_application::execution_error::ExecutionFailureInput::new(
                    error.failure_fact(),
                    novarocks_execution_contract::SafeDetail::truncating(
                        "statistics result could not retain its original statement payload",
                    ),
                    novarocks_query_application::execution_error::ExecutionFailureSite::Coordinator,
                ),
            )
            })?;
        let artifacts = staged.payload.hand_off(|payload, _| match payload {
            Payload::Statistics(artifacts) => artifacts,
        });
        Ok(ReadyStatisticsComputation {
            _binding: binding,
            artifacts,
            _credentials: staged._credentials,
        })
    }
    pub(crate) fn cpu_custody(&self) -> Arc<DomainCpuCustody> {
        Arc::clone(&self.holder.cpu)
    }
}

struct DecodedComputationConsumer {
    plan: PlanSeal,
    domain: Domain,
    claims: AttemptHandoffClaimPort,
    holder: Arc<Holder>,
    cancellation: QueryCancellationView,
}
impl std::fmt::Debug for DecodedComputationConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecodedComputationConsumer")
            .finish_non_exhaustive()
    }
}

type DecodedConsumerParts = (
    Arc<dyn InternalComputationConsumer>,
    AttemptHandoffOfferPort,
    Arc<Holder>,
);

fn validate_contract(
    description: &FrozenExecutionDescription,
    plan: PlanSeal,
    domain: Domain,
) -> Result<(), QueryExecutionError> {
    if description.finish_boundary() != ExecutionFinishBoundary::ComputationReady
        || description.effect() != ExecutionEffect::None
        || description.recovery() != RecoveryMode::NoRecovery
        || !description.matches_plan_seal(plan)
        || !matches!(description.row_carrier(), Ok(ResultRowCarrier::Relayed { kind, client_rows: None }) if kind == domain.kind())
    {
        return Err(domain_error(
            "decoded computation requires its exact NoRecovery internal contract",
        ));
    }
    Ok(())
}

fn construct(
    description: &FrozenExecutionDescription,
    plan: PlanSeal,
    domain: Domain,
    state: InternalDomainState,
    runtime: DecodedConsumerRuntime,
) -> Result<DecodedConsumerParts, QueryExecutionError> {
    validate_contract(description, plan, domain)?;
    let cpu = match runtime.computation {
        Some(computation) => DomainCpuCustody::try_new_with_computation(
            state,
            runtime.binding,
            runtime.cpu,
            runtime.runtime,
            runtime.cancellation.clone(),
            computation,
        )?,
        None => DomainCpuCustody::try_new(
            state,
            runtime.binding,
            runtime.cpu,
            runtime.runtime,
            runtime.cancellation.clone(),
        )?,
    };
    let holder = Arc::new(Holder {
        #[cfg(test)]
        decoded_end: std::sync::OnceLock::new(),
        #[cfg(test)]
        stream_failure: std::sync::OnceLock::new(),
        receiving: Mutex::new(Receiving::Vacant),
        cpu,
    });
    let (offers, claims) = attempt_handoff_mailbox();
    let consumer: Arc<dyn InternalComputationConsumer> = Arc::new(DecodedComputationConsumer {
        plan,
        domain,
        claims,
        holder: Arc::clone(&holder),
        cancellation: runtime.cancellation,
    });
    Ok((consumer, offers, holder))
}

pub(crate) fn statistics_computation_consumer(
    description: &FrozenExecutionDescription,
    plan: PlanSeal,
    program: &StatisticsCollectionProgram,
    runtime: DecodedConsumerRuntime,
) -> Result<
    (
        Arc<dyn InternalComputationConsumer>,
        AttemptHandoffOfferPort,
        StatisticsComputationOutput,
    ),
    QueryExecutionError,
> {
    let binding = runtime.binding.clone();
    let decoder = program
        .result_decoder_with_capacity(&runtime.binding)
        .map_err(|_| domain_error("statistics decoder has no complete admitted capacity"))?;
    let (consumer, offers, holder) = construct(
        description,
        plan,
        Domain::Statistics,
        InternalDomainState::Statistics(decoder),
        runtime,
    )?;
    Ok((
        consumer,
        offers,
        StatisticsComputationOutput { holder, binding },
    ))
}

pub(crate) fn prepare_statistics_computation(
    description: FrozenExecutionDescription,
    template: crate::query_execution::artifact::PreparedDistributedAttemptTemplate,
    options: Arc<crate::query_execution::contract::ResolvedQueryOptions>,
    program: &StatisticsCollectionProgram,
    runtime: DecodedConsumerRuntime,
) -> Result<
    (
        crate::query_execution::logical_execution::PreparedLogicalComputation,
        StatisticsComputationOutput,
    ),
    QueryExecutionError,
> {
    let description = description.into_internal_computation().map_err(|_| {
        domain_error("statistics preparation has no frozen internal computation contract")
    })?;
    let (consumer, offers, output) = statistics_computation_consumer(
        &description,
        template.native_manifest_template().plan(),
        program,
        runtime,
    )?;
    let prepared = crate::query_execution::logical_execution::PreparedLogicalComputation::new(
        description,
        template,
        options,
        consumer,
        offers,
        crate::query_execution::logical_execution::ComputationDiagnostics::None,
    )
    .map_err(|_| domain_error("statistics preparation differs from its Native template"))?;
    Ok((prepared, output))
}

impl InternalComputationConsumer for DecodedComputationConsumer {
    fn begin_local_close(&self) {
        self.holder.cpu.begin_close();
    }

    fn poll_local_closed(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), novarocks_query_application::api::StatementNativeHealthFailure>>
    {
        self.holder.cpu.poll_join_closed(cx).map(|result| {
            result.map_err(|original| {
                novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                    "Original Internal CPU failure remains in custody",
                    original,
                )
            })
        })
    }

    fn local_health_failure(
        &self,
    ) -> Option<novarocks_query_application::api::StatementNativeHealthFailure> {
        self.holder.cpu.health_failure().map(|original| {
            novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                "Original Internal CPU failure remains in custody",
                original,
            )
        })
    }

    fn consume(&self, activation: InternalConsumerActivation) -> InternalConsumerFuture {
        let (mut stream, source) = activation.into_parts();
        let binding = Binding::from_source(&source);
        let claims = self.claims.claim(&source);
        let holder = Arc::clone(&self.holder);
        let cancellation = self.cancellation.clone();
        let domain = self.domain;
        let valid = binding.plan == self.plan
            && stream.activation() == binding.activation
            && matches!(stream.carrier(), ResultRowCarrier::Relayed { kind, client_rows: None } if kind == domain.kind());
        Box::pin(async move {
            let mut source = Some(source);
            let mut owns_activation = false;
            let result = async {
                if !valid {
                    return Err(domain_error(
                        "internal activation has a foreign frozen domain",
                    ));
                }
                let handoff = claims.await?;
                if handoff.activation() != binding.activation
                    || handoff.root() != binding.root
                    || handoff.plan() != binding.plan
                {
                    return Err(domain_error(
                        "decoded computation handoff has a foreign source",
                    ));
                }
                // Only the actual claimed source can reserve this receiving slot.
                holder.reserve(binding)?;
                owns_activation = true;
                let (mut protocol, credentials, _profile) = handoff.into_parts();
                let initial = protocol
                    .changed(binding.activation, binding.root, binding.plan)
                    .await
                    .map_err(|_| {
                        domain_error("decoded computation lost its original Task facts")
                    })?;
                let mut statistics = match domain {
                    Domain::Statistics => Some(
                        StatisticsProtocolTracker::from_initial(&initial).map_err(|_| {
                            domain_error("statistics initial Task provenance is invalid")
                        })?,
                    ),
                };
                drop(initial);
                let end = loop {
                    match stream.next().await? {
                        Some(InternalResultDelivery::Segment(delivery)) => {
                            let (reply, consumed) = delivery.into_parts();
                            match holder.cpu.apply_body(reply).await {
                                Ok(()) => consumed.complete(),
                                Err(error) => {
                                    consumed.fail(error.clone());
                                    return Err(error);
                                }
                            }
                        }
                        Some(InternalResultDelivery::End(delivery)) => {
                            if delivery.activation() != binding.activation {
                                let error = domain_error("internal End has a foreign activation");
                                delivery.fail(error.clone());
                                return Err(error);
                            }
                            if let Err(error) = holder.cpu.finish_eof(delivery.end()).await {
                                delivery.fail(error.clone());
                                return Err(error);
                            }
                            break delivery;
                        }
                        None => {
                            return Err(domain_error("internal computation did not receive End"));
                        }
                    }
                };
                let original_end = end.end();
                // ACK EOF before waiting for all Tasks. This unblocks the
                // original relay so the same Native driver can observe status.
                let decoded_end = end.complete().await?;
                #[cfg(test)]
                holder
                    .decoded_end
                    .set(original_end)
                    .expect("one original checked End receipt");
                if let Some(statistics) = statistics.as_mut() {
                    wait_statistics(
                        statistics,
                        &mut protocol,
                        binding,
                        &cancellation,
                        &mut stream,
                        #[cfg(test)]
                        holder.as_ref(),
                    )
                    .await?;
                    // This second actual CPU receipt validates complete artifact
                    // membership after the original full-Task success verdict.
                    holder.cpu.finish_statistics().await?;
                    // The second CPU job can outlive a newly accepted Task
                    // veto. Refresh the same original revision before staging.
                    wait_statistics(
                        statistics,
                        &mut protocol,
                        binding,
                        &cancellation,
                        &mut stream,
                        #[cfg(test)]
                        holder.as_ref(),
                    )
                    .await?;
                }
                if let Some(reason) = cancellation.reason() {
                    return Err(cancelled(reason));
                }
                let payload = holder
                    .cpu
                    .take()?
                    .try_transform(|slot, _| match domain {
                        Domain::Statistics => slot.into_statistics().map(Payload::Statistics),
                    })
                    .map_err(|_| {
                        domain_error("internal computation lost its checked domain output")
                    })?;
                holder.stage(Staged {
                    binding,
                    end: original_end,
                    payload,
                    _credentials: credentials,
                })?;
                holder.cpu.finish_computation_after_handoff()?;
                // The exact receiving owner already holds the complete graph.
                // The actor remains the sole author of the computation cut.
                let token = source
                    .take()
                    .expect("original Ready source")
                    .accept(decoded_end)
                    .await?;
                debug_assert!(binding.matches(token, original_end));
                Ok(())
            }
            .await;
            if let Err(error) = &result
                && let Some(source) = source.take()
            {
                let _ = source.fail(error.clone()).await;
            }
            // An early CPU error is reported first. The original job remains
            // in stable custody until its actual forwarder has exited.
            let closed = if owns_activation {
                holder.cpu.join_closed().await
            } else {
                Ok(())
            };
            match (result, closed) {
                (Err(error), _) => Err(error),
                (Ok(()), Ok(())) => Ok(()),
                (Ok(()), Err(_)) => Err(domain_error("internal CPU close did not exit normally")),
            }
        })
    }
}

async fn wait_statistics(
    tracker: &mut StatisticsProtocolTracker,
    protocol: &mut AttemptProtocolFactsReceiver,
    binding: Binding,
    cancellation: &QueryCancellationView,
    stream: &mut InternalResultStream,
    #[cfg(test)] observed: &Holder,
) -> Result<(), QueryExecutionError> {
    loop {
        if let Some(changes) = protocol
            .take_changes(binding.activation, binding.root, binding.plan)
            .map_err(|_| domain_error("statistics lost its original current Task facts"))?
        {
            tracker
                .observe(&changes)
                .map_err(|_| domain_error("statistics Task facts have a foreign provenance"))?;
        }
        if let Some(error) = stream.current_failure() {
            #[cfg(test)]
            let _ = observed.stream_failure.set(error.clone());
            return Err(error);
        }
        let awaiting_origin = if let Some(selected) = tracker.selected_failure() {
            if let Some(input) = novarocks_query_application::execution_error::ExecutionFailureInput::from_selected_task(selected) {
                return Err(QueryExecutionError::from_execution(input));
            }
            // The original author has only a peer placeholder. Preserve its
            // refinement right instead of selecting a generic domain error.
            true
        } else {
            false
        };
        if !awaiting_origin && tracker.all_terminal() {
            return if tracker.all_success_compatible() {
                Ok(())
            } else {
                Err(domain_error(
                    "statistics Tasks did not reach a success-compatible terminal set",
                ))
            };
        }
        let changes = tokio::select! {
            biased;
            error = stream.failure() => {
                #[cfg(test)]
                let _ = observed.stream_failure.set(error.clone());
                return Err(error);
            },
            reason = cancellation.cancelled() => return Err(cancelled(reason)),
            changes = protocol.changed(binding.activation, binding.root, binding.plan) =>
                changes.map_err(|_| domain_error("statistics Task fact publisher exited before completion"))?,
        };
        tracker
            .observe(&changes)
            .map_err(|_| domain_error("statistics Task facts have a foreign provenance"))?;
    }
}

pub(super) fn cancelled(reason: QueryCancellationReason) -> QueryExecutionError {
    use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
    use novarocks_query_application::execution_error::{
        ExecutionFailureInput, ExecutionFailureSite, QueryFailureReason,
    };
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        match reason {
            QueryCancellationReason::DeadlineExceeded { .. }
            | QueryCancellationReason::FrontendDrainDeadlineExceeded { .. } => {
                QueryFailureReason::DeadlineExceeded
            }
            _ => QueryFailureReason::Cancelled,
        }
        .failure_fact(),
        SafeDetail::truncating("internal computation was cancelled"),
        ExecutionFailureSite::Coordinator,
    ))
}
