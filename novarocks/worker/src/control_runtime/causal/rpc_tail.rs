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

//! Retirement-only custody of one uniquely moved original RPC causal role.

use super::*;
use crate::control_runtime::source_positions::{
    DetachedSourcePosition, RetirementProbe, SourcePositionObservation, SourcePositionPair,
    UnusedPosition,
};
use novarocks_worker_protocol::creation::RequestContext;

pub(crate) struct RpcTailBinding<'a> {
    pub request: RequestContext,
    pub financial_scope: &'a ScopeHandle,
    pub original_report: &'a ReportSlot,
    pub target: &'a ActorRegistration,
}

pub(super) struct CausalRoleEnv<'a> {
    pub(super) runtime: &'a ActorRuntime<WorkerActor>,
    pub(super) registration: &'a ActorRegistration,
    pub(super) actor: ActorId,
    pub(super) ingress: &'a TaskProposalIngress,
}
impl CausalRoleEnv<'_> {
    fn validate_financial_scope(
        &self,
        role: &Role,
        scope: &ScopeHandle,
    ) -> Result<(), CausalError> {
        self.runtime.validate_registration(self.registration)?;
        if !role.financial_scope.same_scope(scope) {
            return Err(CausalError::WrongAuthor);
        }
        Ok(())
    }
    fn source_core_consumption(
        &self,
        role: &Role,
        source: SourceId,
    ) -> Result<Consumption, CausalError> {
        let delivered = role.delivered.as_ref().ok_or(CausalError::NotDelivered)?;
        if !delivered.names_source(source) {
            return Err(CausalError::UnknownSource);
        }
        if delivered.source_consumption == Consumption::Pending
            && let Some(prefix) = delivered.prefix
        {
            self.runtime
                .input_custody(self.actor, prefix)?
                .ok_or(CausalError::InputObservationMissing)?;
        }
        Ok(delivered.source_consumption)
    }
    pub(super) fn observe_effects_exited(
        &self,
        cut: CutId,
        role: &Role,
    ) -> Result<bool, CausalError> {
        if role.inputs.in_flight()?.iter().any(Option::is_some) {
            return Ok(false);
        }
        match &role.delivered {
            Some(delivered) if delivered.rejected.is_some() => return Ok(false),
            None if !self.ingress.event_unused(cut)? => return Ok(false),
            _ => {}
        }
        self.ingress.observe_cut_exit(cut, false, true)?;
        Ok(true)
    }
    pub(super) fn advance_source_pair_retirement(
        &self,
        cut: CutId,
        role: &Role,
        scope: &ScopeHandle,
    ) -> Result<CausalSourceProgress, CausalError> {
        use novarocks_control_runtime_core::identity::ConsumerId;
        self.validate_financial_scope(role, scope)?;
        let runtime_error = |error| {
            CausalError::Role(novarocks_control_runtime::exit_role::ExitRoleError::Runtime(error))
        };
        let mut progressed = false;
        let route = crate::task_proposal_ingress::PairRoute::Cut(cut);
        // The ingress verifies sealed/unused before moving any Available
        // holder. Its real unused Drop revokes, rather than consuming reports.
        for holder in self.ingress.take_unused_pair(route)?.into_iter().flatten() {
            holder.exit();
            progressed = true;
        }
        for original in self.ingress.pair_inventory(route)? {
            let before = original.custody.phase();
            if before == ExitRolePhase::Retired {
                continue;
            }
            if let Some(source) = original.source {
                // ExitAuthority fills this original report only after its real
                // body/captures return. A saved Core receipt alone cannot fill it.
                if original
                    .custody
                    .actual_exit_ready()
                    .map_err(CausalError::Role)?
                {
                    let core = ConsumerId::new(1).expect("original source Core consumer");
                    let receipt = self.source_core_consumption(role, source)?;
                    if receipt != Consumption::Pending
                        && original
                            .custody
                            .report()
                            .consumption(core)
                            .map_err(runtime_error)?
                            == Consumption::Pending
                    {
                        let consumer = original.custody.report().bind_actor_consumer(
                            self.runtime,
                            self.registration,
                            core,
                        )?;
                        match receipt {
                            Consumption::Consumed => consumer.consume().map_err(runtime_error)?,
                            Consumption::AbandonedByOwnerFailure => {
                                // This saved exact receipt was produced by the
                                // original stage-authenticated failure path.
                                let proof = self.runtime.failure_proof_for(self.registration)?;
                                consumer.abandon(&proof)?;
                            }
                            Consumption::Pending => unreachable!("resolved original receipt"),
                        }
                        progressed = true;
                    }
                    let host = ConsumerId::new(2).expect("original source host consumer");
                    if original
                        .custody
                        .report()
                        .consumption(host)
                        .map_err(runtime_error)?
                        == Consumption::Pending
                    {
                        original
                            .custody
                            .report()
                            .consume(host)
                            .map_err(runtime_error)?;
                        progressed = true;
                    }
                }
            }
            original.custody.try_retire().map_err(CausalError::Role)?;
            progressed |= original.custody.phase() != before;
        }
        let originals = self.ingress.pair_inventory(route)?;
        for original in &originals {
            if !original.custody.reusable() {
                progressed |= original
                    .custody
                    .financial_scope()
                    .reap_ready(1)
                    .map_err(runtime_error)?
                    != 0;
            }
        }
        let [first, second] = self.ingress.prepare_pair_reap(route)?;
        let reaped = match (first.observe_reaped(), second.observe_reaped()) {
            (Ok(first), Ok(second)) => Some([first, second]),
            _ => None,
        };
        Ok(CausalSourceProgress { progressed, reaped })
    }
    pub(super) fn retire_after_exit(
        &self,
        cut: CutId,
        role: &mut Role,
        retirement: CausalSourceRetirement,
    ) -> Result<RetiredCut, CausalRetirementRefusal> {
        let checked = (|| {
            let input_tail_ready = match &role.input_author {
                Some(author) => author.cut_tail_ready().map_err(CausalError::Role)?,
                None => true,
            };
            let rpc_tail_ready = match &role.rpc_author {
                Some(author) => author.cut_tail_ready()?,
                None => true,
            };
            if !input_tail_ready
                || !rpc_tail_ready
                || role.loader_exit.as_ref().is_some_and(|loader| {
                    loader.phase() != novarocks_control_runtime::exit_role::ExitRolePhase::Retired
                })
                || role.inputs.in_flight()?.iter().any(Option::is_some)
                || role
                    .delivered
                    .as_ref()
                    .is_some_and(|d| d.rejected.is_some())
            {
                return Err(DriverError::RoleBusy.into());
            }
            self.ingress.event_unused(cut).map_err(Into::into)
        })();
        let unused = match checked {
            Ok(unused) => unused,
            Err(error) => return Err(CausalRetirementRefusal { error, retirement }),
        };
        let retired = match retirement {
            CausalSourceRetirement::Unclaimed => {
                let retired = if unused {
                    self.ingress.revoke_unused_cut_unclaimed(cut)
                } else {
                    self.ingress.retire_cut_unclaimed(cut)
                };
                retired.map_err(|error| CausalRetirementRefusal {
                    error: error.into(),
                    retirement: CausalSourceRetirement::Unclaimed,
                })?
            }
            CausalSourceRetirement::Reaped(reaped) => {
                let retired = if unused {
                    self.ingress.revoke_unused_cut(cut, reaped)
                } else {
                    self.ingress.retire_cut(cut, reaped)
                };
                retired.map_err(|(error, reaped)| CausalRetirementRefusal {
                    error: error.into(),
                    retirement: CausalSourceRetirement::Reaped(reaped),
                })?
            }
        };
        role.inputs
            .close_after_exit()
            .expect("exclusive idle causal positions retain their actor");
        Ok(retired)
    }
}

#[must_use = "The original RPC causal role must retain its actual retirement owner"]
pub(crate) struct RpcCausalTail {
    runtime: ActorRuntime<WorkerActor>,
    registration: ActorRegistration,
    cut: CutId,
    ingress: Arc<TaskProposalIngress>,
    original: Option<Role>,
}
impl RpcCausalTail {
    fn role_env(&self) -> CausalRoleEnv<'_> {
        CausalRoleEnv {
            runtime: &self.runtime,
            registration: &self.registration,
            actor: self.registration.actor(),
            ingress: &self.ingress,
        }
    }
    pub(crate) fn observe_effects_exited(&self) -> Result<bool, CausalError> {
        let role = self.original.as_ref().ok_or(CausalError::UnknownRole)?;
        self.role_env().observe_effects_exited(self.cut, role)
    }
    pub(crate) fn advance_source_pair_retirement(
        &self,
        scope: &ScopeHandle,
    ) -> Result<CausalSourceProgress, CausalError> {
        let role = self.original.as_ref().ok_or(CausalError::UnknownRole)?;
        self.role_env()
            .advance_source_pair_retirement(self.cut, role, scope)
    }
    pub(crate) fn retire_after_exit(
        &mut self,
        retirement: CausalSourceRetirement,
    ) -> Result<RetiredCut, CausalRetirementRefusal> {
        let Some(role) = self.original.as_mut() else {
            return Err(CausalRetirementRefusal {
                error: CausalError::UnknownRole,
                retirement,
            });
        };
        let env = CausalRoleEnv {
            runtime: &self.runtime,
            registration: &self.registration,
            actor: self.registration.actor(),
            ingress: &self.ingress,
        };
        let retired = env.retire_after_exit(self.cut, role, retirement)?;
        self.original.take();
        Ok(retired)
    }
    pub(crate) fn retirement_bank(&self) -> CausalRetirementBank {
        CausalRetirementBank(self.ingress.clone())
    }
}
impl TaskCausalInbox {
    pub(super) fn role_env(&self) -> CausalRoleEnv<'_> {
        CausalRoleEnv {
            runtime: &self.runtime,
            registration: &self.registration,
            actor: self.actor,
            ingress: &self.ingress,
        }
    }
    pub(crate) fn retirement_bank(&self) -> CausalRetirementBank {
        CausalRetirementBank(self.ingress.clone())
    }
    /// Transfer the one original role only after all Core/input facts have
    /// been saved. External R/report exit remains a final retirement gate.
    pub(crate) fn try_take_rpc_tail(
        &mut self,
        cut: CutId,
        binding: RpcTailBinding<'_>,
    ) -> Result<Option<RpcCausalTail>, CausalError> {
        self.runtime.validate_registration(&self.registration)?;
        if !self.registration.same_registration(binding.target) {
            return Err(CausalError::WrongAuthor);
        }
        let failed = self.runtime.snapshot(self.actor)?.failed;
        // Snapshot is addressed numerically; recheck the same original origin.
        self.runtime.validate_registration(&self.registration)?;
        if failed {
            return Ok(None);
        }
        let role = self.roles.get(&cut).ok_or(CausalError::UnknownRole)?;
        let rpc = role.rpc_author.as_ref().ok_or(CausalError::WrongAuthor)?;
        self.validate_author_binding(&rpc.binding)?;
        if !role.financial_scope.same_scope(binding.financial_scope)
            || !rpc.report.same_slot(binding.original_report)
            || rpc.binding.work != binding.request.author
            || role.inputs.event_author()?.work() != binding.request.author
            || rpc.request.operation != binding.request.operation
            || rpc.request.deadline != binding.request.deadline
        {
            return Err(CausalError::WrongAuthor);
        }
        let Some(returned) = &role.rpc_sender_returned else {
            return Ok(None);
        };
        if !Arc::ptr_eq(&returned.original, rpc) {
            return Err(CausalError::WrongAuthor);
        }
        if role.input_author.is_some() || role.loader_exit.is_some() {
            return Err(CausalError::WrongAuthor);
        }
        let Some(delivered) = role.delivered.as_ref() else {
            return Ok(None);
        };
        if delivered.event != Some(binding.request.token) {
            return Err(CausalError::WrongAuthor);
        }
        if delivered.rejected.is_some()
            || delivered.event_consumption != Consumption::Consumed
            || self.ingress.cut_consumption(cut) != Some(Consumption::Consumed)
            || (delivered.sources.iter().any(Option::is_some)
                && delivered.source_consumption != Consumption::Consumed)
            || role.inputs.in_flight()?.iter().any(Option::is_some)
        {
            return Ok(None);
        }
        let runtime = self.runtime.clone();
        let registration = self.registration.clone();
        let ingress = self.ingress.clone();
        let original = self
            .roles
            .remove(&cut)
            .expect("same preflighted original RPC role");
        if self.observing_prefix == Some(cut) {
            self.observing_prefix = None;
        }
        Ok(Some(RpcCausalTail {
            runtime,
            registration,
            cut,
            ingress,
            original: Some(original),
        }))
    }
}

/// Same original bank, with no admission/fill/registration surface or state.
/// Keeping this handle does not create Work, Source or a consumer leg.
pub(crate) struct CausalRetirementBank(Arc<TaskProposalIngress>);
impl CausalRetirementBank {
    pub(crate) fn arm_rpc_retirement_notice(
        &self,
        cut: CutId,
        slot: &Arc<crate::control_runtime::request_ready::RequestReadySlot>,
    ) -> Result<(), IngressError> {
        self.0.arm_rpc_retirement_notice(cut, slot)
    }
    pub(crate) fn arm_retired_rpc_retirement_notice(
        &self,
        retired: &RetiredCut,
        slot: &Arc<crate::control_runtime::request_ready::RequestReadySlot>,
    ) -> Result<(), IngressError> {
        self.0.arm_retired_rpc_retirement_notice(retired, slot)
    }
    /// Only the original host cut leg; no new consumer or earlier exit fact.
    pub(crate) fn consume_host_cut(&self, cut: CutId) -> Result<(), IngressError> {
        self.0.consume_cut(cut, 0)
    }
    pub(crate) fn restore_retired_funding_pair(
        &self,
        original: RetiredCut,
    ) -> Result<RetiredCut, (IngressError, RetiredCut)> {
        self.0.restore_retired_funding_pair(original)
    }
    pub(crate) fn take_unused_owned_pair(
        &self,
        pair: &SourcePositionPair,
    ) -> Result<[Option<UnusedPosition>; 2], IngressError> {
        self.0.take_unused_owned_pair(pair)
    }
    pub(crate) fn owned_pair_inventory(
        &self,
        pair: &SourcePositionPair,
    ) -> Result<[SourcePositionObservation; 2], IngressError> {
        self.0.owned_pair_inventory(pair)
    }
    pub(crate) fn prepare_owned_pair_reap(
        &self,
        pair: &SourcePositionPair,
    ) -> Result<[RetirementProbe; 2], IngressError> {
        self.0.prepare_owned_pair_reap(pair)
    }
    pub(crate) fn release_owned_pair(
        &self,
        pair: &SourcePositionPair,
        original: [ReapedPosition; 2],
    ) -> Result<[DetachedSourcePosition; 2], (IngressError, [ReapedPosition; 2])> {
        self.0.release_owned_pair(pair, original)
    }
}
