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

//! Fixed write computation. Publication and its Provider checks remain later.

use super::{
    attempt_handoff::{AttemptHandoffClaimPort, AttemptHandoffOfferPort, attempt_handoff_mailbox},
    decoded::DecodedConsumerRuntime,
    domain_cpu::{DomainCpuCustody, domain_error},
    domain_state::InternalDomainState,
};
use crate::query_execution::{
    artifact::PreparedDistributedAttemptTemplate,
    lifecycle_plan::AttemptCredentialStorage,
    logical_execution::{
        InitialComputationCredentials, InitialComputationReservation, PreparedLogicalComputation,
    },
    write_result::{RootWriteDecodeContract, RootWriteResultDecoder},
    write_session::{AcceptedWriteSegment, ConnectorWriteSession, WriteComputationDecode},
};
use crate::task_execution::{
    completion::WriteVerdict,
    protocol_facts::{AttemptProtocolFactsReceiver, WriteProtocolTracker},
};
use novarocks_coordination_contract::{AttemptActivationIdentity, ExecutionEffect, RecoveryMode};
use novarocks_execution_contract::{TaskIdentity, root_result::RootResultEnd};
use novarocks_query_application::{
    api::{
        ComputationReadyToken, InternalComputationConsumer, InternalConsumerActivation,
        InternalConsumerFuture, InternalResultDelivery, InternalResultStream, PlanSeal,
        QueryExecutionError, ResultRowCarrier,
    },
    cancellation::QueryCancellationView,
    execution_error::ExecutionFailureInput,
    preparation::{ExecutionFinishBoundary, FrozenExecutionDescription},
};
use novarocks_result_contract::{InternalResultDomain, RootOutputKind};
use std::sync::{Arc, Mutex};

/// Only frozen-plan facts and the original session create this seed. The
/// initial credential collector remains the original one throughout planning.
pub(crate) struct WriteComputationSeed {
    session: Arc<ConnectorWriteSession>,
    initial: Arc<InitialComputationCredentials>,
    contract: RootWriteDecodeContract,
    decode: WriteComputationDecode,
    production_execution: Option<(
        novarocks_types::QueryExecutionId,
        novarocks_query_application::api::PlanSeal,
    )>,
}
impl WriteComputationSeed {
    pub(crate) fn from_frozen(
        template: &PreparedDistributedAttemptTemplate,
        session: Arc<ConnectorWriteSession>,
        initial: Arc<InitialComputationCredentials>,
    ) -> Result<Self, QueryExecutionError> {
        let targets = template
            .native_manifest_template()
            .plan_facts()
            .write_root_targets()
            .ok_or_else(|| {
                domain_error("write computation has no original frozen TableFinish targets")
            })?;
        let contract =
            RootWriteDecodeContract::try_new(targets, session.targets()).map_err(|_| {
                domain_error("write root targets differ from the original sealed session")
            })?;
        let decode = session
            .computation_decode(targets)
            .map_err(domain_error_with_source)?;
        // Install before Native effects or consumer scheduling. A failed or
        // never-claimed attempt still has its original terminal capability.
        initial
            .bind_write_production_session(&session)
            .map_err(domain_error)?;
        session
            .retain_computation_credentials(&initial)
            .map_err(domain_error_with_source)?;
        let production_execution = Some((
            initial.execution(),
            template.native_manifest_template().plan(),
        ));
        Ok(Self {
            session,
            initial,
            contract,
            decode,
            production_execution,
        })
    }
}

fn domain_error_with_source<E>(source: E) -> QueryExecutionError
where
    E: std::error::Error + novarocks_failure_contract::FailureProjection + Send + Sync + 'static,
{
    use novarocks_execution_contract::SafeDetail;
    use novarocks_query_application::execution_error::{
        ExecutionFailureInput, ExecutionFailureSite,
    };
    QueryExecutionError::from_execution(ExecutionFailureInput::new(
        source.failure_fact(),
        SafeDetail::truncating("write computation rejected its original input"),
        ExecutionFailureSite::Coordinator,
    ))
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Binding {
    activation: AttemptActivationIdentity,
    root: TaskIdentity,
    plan: PlanSeal,
}
struct Staged {
    binding: Binding,
    end: RootResultEnd,
    accepted: AcceptedWriteSegment,
    credentials: Arc<AttemptCredentialStorage>,
}
enum Receiving {
    Vacant,
    Reserved(Binding),
    Staged(Staged),
    Taken(ComputationReadyToken),
}
struct Holder {
    receiving: Mutex<Receiving>,
    cpu: Arc<DomainCpuCustody>,
    session: Arc<ConnectorWriteSession>,
    initial: Arc<InitialComputationCredentials>,
}
impl Holder {
    fn reserve(&self, binding: Binding) -> Result<(), QueryExecutionError> {
        let mut slot = self.receiving.lock().unwrap_or_else(|e| e.into_inner());
        if !matches!(*slot, Receiving::Vacant) {
            return Err(domain_error(
                "NoRecovery write computation already owns its original source",
            ));
        }
        *slot = Receiving::Reserved(binding);
        Ok(())
    }
    fn stage(&self, staged: Staged) -> Result<(), QueryExecutionError> {
        let mut slot = self.receiving.lock().unwrap_or_else(|e| e.into_inner());
        if !matches!(*slot, Receiving::Reserved(binding) if binding == staged.binding)
            || !staged.accepted.belongs_to(&self.session)
        {
            return Err(domain_error(
                "write receiving receipt has a foreign original source",
            ));
        }
        *slot = Receiving::Staged(staged);
        Ok(())
    }
    fn take(&self, token: ComputationReadyToken) -> Result<Staged, QueryExecutionError> {
        let mut slot = self.receiving.lock().unwrap_or_else(|e| e.into_inner());
        match &*slot {
            Receiving::Staged(staged)
                if staged.binding.activation == token.activation()
                    && staged.binding.root == token.root()
                    && staged.end == token.end() => {}
            Receiving::Taken(original) if *original == token => {
                return Err(domain_error("write computation was already transferred"));
            }
            _ => {
                return Err(domain_error(
                    "write computation has no matching original staged cut",
                ));
            }
        }
        let Receiving::Staged(staged) = std::mem::replace(&mut *slot, Receiving::Taken(token))
        else {
            unreachable!()
        };
        Ok(staged)
    }
}

pub(crate) struct WriteComputationOutput {
    holder: Arc<Holder>,
}
pub(crate) struct ReadyWriteComputation {
    pub(crate) accepted: AcceptedWriteSegment,
    pub(crate) session: Arc<ConnectorWriteSession>,
    pub(crate) _credentials: Arc<AttemptCredentialStorage>,
    // The actual statement/result holder retains the initial weak entry point.
    _initial: Arc<InitialComputationCredentials>,
}
impl WriteComputationOutput {
    pub(crate) fn cpu_custody(&self) -> Arc<DomainCpuCustody> {
        Arc::clone(&self.holder.cpu)
    }
    pub(crate) fn take_ready(
        &mut self,
        token: ComputationReadyToken,
    ) -> Result<ReadyWriteComputation, QueryExecutionError> {
        let staged = self.holder.take(token)?;
        Ok(ReadyWriteComputation {
            accepted: staged.accepted,
            session: Arc::clone(&self.holder.session),
            _credentials: staged.credentials,
            _initial: Arc::clone(&self.holder.initial),
        })
    }
}

struct WriteComputationConsumer {
    plan: PlanSeal,
    claims: AttemptHandoffClaimPort,
    holder: Arc<Holder>,
    cancellation: QueryCancellationView,
}
impl std::fmt::Debug for WriteComputationConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteComputationConsumer")
            .finish_non_exhaustive()
    }
}
pub(crate) fn write_computation_consumer(
    description: &FrozenExecutionDescription,
    plan: PlanSeal,
    seed: WriteComputationSeed,
    runtime: DecodedConsumerRuntime,
) -> Result<
    (
        Arc<dyn InternalComputationConsumer>,
        AttemptHandoffOfferPort,
        WriteComputationOutput,
    ),
    QueryExecutionError,
> {
    if description.finish_boundary() != ExecutionFinishBoundary::ComputationReady
        || description.effect() != ExecutionEffect::External
        || description.recovery() != RecoveryMode::NoRecovery
        || !description.matches_plan_seal(plan)
        || !matches!(
            description.row_carrier(),
            Ok(ResultRowCarrier::Relayed {
                kind: RootOutputKind::InternalFacts(InternalResultDomain::PreparedWriteCommitV1),
                client_rows: None
            })
        )
    {
        return Err(domain_error(
            "write computation requires its exact frozen External NoRecovery carrier",
        ));
    }
    let mut decoder = RootWriteResultDecoder::new_with_capacity(seed.contract, &runtime.binding)
        .map_err(|_| domain_error("write decoder has no original admitted capacity"))?;
    if let Some((execution, plan)) = seed.production_execution {
        decoder.bind_production_execution(execution, plan);
    }
    let state = InternalDomainState::WriteComputation {
        decoder,
        decode: seed.decode,
    };
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
        receiving: Mutex::new(Receiving::Vacant),
        cpu,
        session: seed.session,
        initial: seed.initial,
    });
    let (offers, claims) = attempt_handoff_mailbox();
    let consumer: Arc<dyn InternalComputationConsumer> = Arc::new(WriteComputationConsumer {
        plan,
        claims,
        holder: Arc::clone(&holder),
        cancellation: runtime.cancellation,
    });
    Ok((consumer, offers, WriteComputationOutput { holder }))
}

pub(crate) fn prepare_write_computation(
    description: FrozenExecutionDescription,
    template: PreparedDistributedAttemptTemplate,
    options: Arc<crate::query_execution::contract::ResolvedQueryOptions>,
    session: Arc<ConnectorWriteSession>,
    initial: InitialComputationReservation,
    runtime: DecodedConsumerRuntime,
) -> Result<
    (
        PreparedLogicalComputation,
        novarocks_query_application::api::ReservedExecutionIdentity,
        WriteComputationOutput,
    ),
    QueryExecutionError,
> {
    let description = description
        .into_internal_computation()
        .map_err(|_| domain_error("write preparation has no internal finish contract"))?;
    let seed = WriteComputationSeed::from_frozen(&template, session, initial.credentials())?;
    let (consumer, offers, output) = write_computation_consumer(
        &description,
        template.native_manifest_template().plan(),
        seed,
        runtime,
    )?;
    let prepared = PreparedLogicalComputation::new(
        description,
        template,
        options,
        consumer,
        offers,
        crate::query_execution::logical_execution::ComputationDiagnostics::None,
    )
    .map_err(|_| domain_error("write preparation differs from its original Native template"))?;
    let (prepared, reservation) = prepared
        .with_initial_reservation(initial)
        .map_err(|error| {
            QueryExecutionError::from_execution(error.into_execution_failure_input())
        })?;
    Ok((prepared, reservation, output))
}

impl InternalComputationConsumer for WriteComputationConsumer {
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
        let binding = Binding {
            activation: source.activation(),
            root: source.root(),
            plan: source.plan(),
        };
        let valid = binding.plan == self.plan
            && binding.activation.execution() == self.holder.initial.execution()
            && stream.activation() == binding.activation
            && matches!(
                stream.carrier(),
                ResultRowCarrier::Relayed {
                    kind: RootOutputKind::InternalFacts(
                        InternalResultDomain::PreparedWriteCommitV1
                    ),
                    client_rows: None
                }
            );
        let claim = self.claims.claim(&source);
        let holder = Arc::clone(&self.holder);
        let cancellation = self.cancellation.clone();
        Box::pin(async move {
            let mut source = Some(source);
            let mut owns_activation = false;
            let result = async {
                if !valid {
                    return Err(domain_error(
                        "write source differs from its exact initial seed",
                    ));
                }
                let handoff = claim.await?;
                if handoff.activation() != binding.activation
                    || handoff.root() != binding.root
                    || handoff.plan() != binding.plan
                {
                    return Err(domain_error("write handoff has a foreign source"));
                }
                let (mut protocol, credentials, _) = handoff.into_parts();
                let credentials = credentials.ok_or_else(|| {
                    domain_error("write handoff lost its original frozen credential table")
                })?;
                holder
                    .initial
                    .verify_native_storage(&credentials)
                    .map_err(|error| {
                        QueryExecutionError::from_execution(error.into_execution_failure_input())
                    })?;
                holder.reserve(binding)?;
                owns_activation = true;
                let initial = protocol
                    .changed(binding.activation, binding.root, binding.plan)
                    .await
                    .map_err(task_error)?;
                let mut tracker =
                    WriteProtocolTracker::from_initial(&initial).map_err(task_error)?;
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
                                let error = domain_error("write End has a foreign activation");
                                delivery.fail(error.clone());
                                return Err(error);
                            }
                            // EOF includes the original generation's one CPU
                            // interpretation. A bad fragment still fails End.
                            if let Err(error) = holder.cpu.finish_eof(delivery.end()).await {
                                delivery.fail(error.clone());
                                return Err(error);
                            }
                            break delivery;
                        }
                        None => {
                            return Err(domain_error(
                                "write computation did not receive its original End",
                            ));
                        }
                    }
                };
                let original_end = end.end();
                let decoded = end.complete().await?;
                wait_write(
                    &mut tracker,
                    &mut protocol,
                    binding,
                    &cancellation,
                    &mut stream,
                )
                .await?;
                // Pure receiving/union work executes in the existing finite
                // CPU lane and moves the SAME payload into the stable session.
                holder
                    .cpu
                    .accept_write(holder.session.receiving_port())
                    .await?;
                wait_write(
                    &mut tracker,
                    &mut protocol,
                    binding,
                    &cancellation,
                    &mut stream,
                )
                .await?;
                let accepted = holder
                    .cpu
                    .take()?
                    .hand_off(|slot, _| slot.into_accepted_write())
                    .map_err(|_| {
                        domain_error("write receiving lost its original accepted segment")
                    })?;
                holder.stage(Staged {
                    binding,
                    end: original_end,
                    accepted,
                    credentials,
                })?;
                holder.cpu.finish_computation_after_handoff()?;
                let token = source
                    .take()
                    .expect("original write Ready source")
                    .accept(decoded)
                    .await?;
                debug_assert!(
                    token.activation() == binding.activation
                        && token.root() == binding.root
                        && token.end() == original_end
                );
                Ok(())
            }
            .await;
            if let Err(error) = &result
                && let Some(source) = source.take()
            {
                let _ = source.fail(error.clone()).await;
            }
            let closed = if owns_activation {
                holder.cpu.join_closed().await
            } else {
                Ok(())
            };
            match (result, closed) {
                (Err(error), _) => Err(error),
                (Ok(()), Ok(())) => Ok(()),
                (Ok(()), Err(_)) => Err(domain_error("write CPU custody did not close normally")),
            }
        })
    }
}
fn task_error(error: crate::task_execution::error::TaskExecutionError) -> QueryExecutionError {
    QueryExecutionError::from_execution(error.preparation_input())
}
async fn wait_write(
    tracker: &mut WriteProtocolTracker,
    protocol: &mut AttemptProtocolFactsReceiver,
    binding: Binding,
    cancellation: &QueryCancellationView,
    stream: &mut InternalResultStream,
) -> Result<(), QueryExecutionError> {
    loop {
        if let Some(changes) = protocol
            .take_changes(binding.activation, binding.root, binding.plan)
            .map_err(task_error)?
        {
            tracker.observe(&changes).map_err(task_error)?;
        }
        if let Some(error) = stream.current_failure() {
            return Err(error);
        }
        if let Some(selected) = tracker.selected_failure() {
            if let Some(input) = ExecutionFailureInput::from_selected_task(selected) {
                return Err(QueryExecutionError::from_execution(input));
            }
            // Only the original author may refine the peer placeholder.
        } else {
            let verdict = tracker.execution_verdict();
            if verdict == WriteVerdict::Complete {
                if let Some(reason) = cancellation.reason() {
                    return Err(super::decoded::cancelled(reason));
                }
                return Ok(());
            }
            if !verdict.is_pending() {
                return Err(task_error(
                    crate::task_execution::error::TaskExecutionError::Schedule(format!(
                        "write reached a non-committable terminal: {verdict}"
                    )),
                ));
            }
        }
        let changes = tokio::select! { biased;
            error = stream.failure() => return Err(error),
            reason = cancellation.cancelled() => return Err(super::decoded::cancelled(reason)),
            changes = protocol.changed(binding.activation, binding.root, binding.plan) => changes.map_err(task_error)?,
        };
        tracker.observe(&changes).map_err(task_error)?;
    }
}

#[cfg(all(test, novarocks_dev_controls))]
#[path = "write_component_tests.rs"]
mod component_tests;
