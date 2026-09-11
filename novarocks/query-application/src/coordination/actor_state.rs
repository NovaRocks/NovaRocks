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

#![allow(
    dead_code,
    reason = "the actor reducer keeps transition helpers private until each state is reachable"
)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use novarocks_execution_contract::{
    CredentialLeaseId, PlanNodeId, QueryContextRef, TaskOperationId,
};
use novarocks_types::identity::{QueryExecutionId, QueryId};

use super::{
    AttemptFailureClass, BeginSchemaDelivery, CompletedDelivery, DeliveryCompletion,
    DeliveryCompletionError, DeliveryGate, DeliveryGateError, DeliveryPermit, DeliveryPhase,
    DeliveryRejection, ExecutionEffect, RecoveryDecision, RecoveryInput, RecoveryMode,
    ResultPacket, SchemaDeliveryPermit, evaluate_recovery,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AttemptConvergenceFacts {
    pub actual_stopped: bool,
    pub output_released: bool,
    pub resources_converged: bool,
}

impl AttemptConvergenceFacts {
    pub const fn retired(self) -> bool {
        self.actual_stopped && self.output_released && self.resources_converged
    }

    fn merge(&mut self, newer: Self) {
        self.actual_stopped |= newer.actual_stopped;
        self.output_released |= newer.output_released;
        self.resources_converged |= newer.resources_converged;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptDisposition {
    Current,
    Residual,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AttemptRecord {
    pub disposition: AttemptDisposition,
    pub current_unknown: bool,
    pub last_known_usage_bytes: u64,
    pub convergence: AttemptConvergenceFacts,
    capability_generation: u64,
}

impl AttemptRecord {
    const fn current(capability_generation: u64) -> Self {
        Self {
            disposition: AttemptDisposition::Current,
            current_unknown: false,
            last_known_usage_bytes: 0,
            convergence: AttemptConvergenceFacts {
                actual_stopped: false,
                output_released: false,
                resources_converged: false,
            },
            capability_generation,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalConclusion {
    Succeeded,
    Failed,
    Cancelled,
    BusinessDecisionRequired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalOutputMode {
    ResultStream,
    CompletionOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionPhase {
    Running {
        execution: QueryExecutionId,
        eligibility_generation: u64,
    },
    Replacing {
        failed: QueryExecutionId,
        replacement: QueryExecutionId,
        eligibility_generation: u64,
    },
    Instantiating {
        execution: QueryExecutionId,
        eligibility_generation: u64,
    },
    FinishingSuccess {
        execution: QueryExecutionId,
        eligibility_generation: u64,
    },
    Converging,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum ReplacementFact {
    ReachableContextsClosed,
    AttemptIsolationProven,
    StaleUsageAccounted,
    NewCapacityAdmitted,
}

/// Opaque identity of one replacement qualification round. Every asynchronous
/// completion is checked against its exact failed and proposed attempt before
/// it may advance the actor.
#[derive(Clone, Debug, Eq, PartialEq)]
#[must_use = "the logical execution actor must retain a replacement token until activation or conclusion"]
pub(crate) struct ReplacementToken {
    failed: QueryExecutionId,
    replacement: QueryExecutionId,
    eligibility_generation: u64,
}

impl ReplacementToken {
    pub(super) const fn failed(&self) -> QueryExecutionId {
        self.failed
    }

    pub(super) const fn replacement(&self) -> QueryExecutionId {
        self.replacement
    }

    pub(super) const fn eligibility_generation(&self) -> u64 {
        self.eligibility_generation
    }
}

/// Move-only authority for one supervised replacement effect.  The actor
/// runtime must return this exact value after the effect completes; the
/// reducer never accepts a caller-authored replacement fact.
#[derive(Debug)]
#[must_use = "the logical execution actor must supervise or cancel every pending effect"]
pub(crate) struct PendingReplacementEffect {
    actor_instance_id: u64,
    effect_id: u64,
}

#[derive(Debug)]
#[must_use = "the logical execution actor must complete or fail the authorized success EOF"]
pub(crate) struct SuccessEndOfStreamPermit {
    inner: DeliveryPermit<()>,
}

/// Opaque, actor-instance-scoped authority for one exact attempt.  The
/// generation is retained with the attempt record, so a capability cannot be
/// transplanted to another actor or a later registration of the same ids.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct AttemptCapability {
    actor_instance_id: u64,
    execution: QueryExecutionId,
    generation: u64,
}

impl AttemptCapability {
    pub(super) const fn actor_instance_id(&self) -> u64 {
        self.actor_instance_id
    }

    pub(super) const fn execution(&self) -> QueryExecutionId {
        self.execution
    }

    pub(super) const fn generation(&self) -> u64 {
        self.generation
    }
}

/// Move-only authority for the supervised completion of one exact attempt.
#[derive(Debug)]
#[must_use = "the logical execution actor must supervise or cancel every pending effect"]
pub(crate) struct PendingAttemptSuccessEffect {
    actor_instance_id: u64,
    effect_id: u64,
}

/// A replacement fact can enter the reducer only through an exact pending
/// effect receipt.  The future production actor will construct these from its
/// supervised effect table; callers outside the coordination owner cannot
/// manufacture one.
#[derive(Debug)]
pub(crate) struct ReplacementEffectReceipt {
    failed: QueryExecutionId,
    replacement: QueryExecutionId,
    eligibility_generation: u64,
    fact: ReplacementFact,
}

impl ReplacementEffectReceipt {
    const fn from_effect(token: &ReplacementToken, fact: ReplacementFact) -> Self {
        Self {
            failed: token.failed,
            replacement: token.replacement,
            eligibility_generation: token.eligibility_generation,
            fact,
        }
    }
}

/// Opaque observation that the exact current attempt reached stable success.
/// Root EOS alone never creates this authority.
#[derive(Debug)]
pub(crate) struct AttemptSuccessReceipt {
    actor_instance_id: u64,
    execution: QueryExecutionId,
    capability_generation: u64,
}

impl AttemptSuccessReceipt {
    const fn from_effect(capability: &AttemptCapability) -> Self {
        Self {
            actor_instance_id: capability.actor_instance_id,
            execution: capability.execution,
            capability_generation: capability.generation,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingEffect {
    Replacement {
        failed: QueryExecutionId,
        replacement: QueryExecutionId,
        eligibility_generation: u64,
        fact: ReplacementFact,
    },
    AttemptSuccess {
        execution: QueryExecutionId,
        capability_generation: u64,
    },
}

/// Private mint and first-wins consumption table for effect receipts.  Effect
/// ids exist only inside one actor instance and are never serialized.
#[derive(Debug)]
struct EffectReceiptOwner {
    next_effect_id: u64,
    pending: BTreeMap<u64, PendingEffect>,
}

impl EffectReceiptOwner {
    const fn new() -> Self {
        Self {
            next_effect_id: 1,
            pending: BTreeMap::new(),
        }
    }

    fn issue(&mut self, effect: PendingEffect) -> Result<u64, ActorStateError> {
        if self.pending.values().any(|pending| *pending == effect) {
            return Err(ActorStateError::EffectAlreadyPending);
        }
        let effect_id = self.next_effect_id;
        self.next_effect_id = self
            .next_effect_id
            .checked_add(1)
            .ok_or(ActorStateError::EffectIdentityExhausted)?;
        let previous = self.pending.insert(effect_id, effect);
        debug_assert!(previous.is_none());
        Ok(effect_id)
    }

    fn consume(
        &mut self,
        actor_instance_id: u64,
        expected_actor_instance_id: u64,
        effect_id: u64,
    ) -> Result<PendingEffect, ActorStateError> {
        if actor_instance_id != expected_actor_instance_id {
            return Err(ActorStateError::StaleEffectReceipt);
        }
        self.pending
            .remove(&effect_id)
            .ok_or(ActorStateError::StaleEffectReceipt)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ReplacementProgress {
    reachable_contexts_closed: bool,
    attempt_isolation_proven: bool,
    stale_usage_accounted: bool,
    new_capacity_admitted: bool,
}

impl ReplacementProgress {
    fn record(&mut self, fact: ReplacementFact) {
        match fact {
            ReplacementFact::ReachableContextsClosed => self.reachable_contexts_closed = true,
            ReplacementFact::AttemptIsolationProven => self.attempt_isolation_proven = true,
            ReplacementFact::StaleUsageAccounted => self.stale_usage_accounted = true,
            ReplacementFact::NewCapacityAdmitted => self.new_capacity_admitted = true,
        }
    }

    const fn missing(self) -> Option<ReplacementFact> {
        if !self.reachable_contexts_closed {
            return Some(ReplacementFact::ReachableContextsClosed);
        }
        if !self.attempt_isolation_proven {
            return Some(ReplacementFact::AttemptIsolationProven);
        }
        if !self.stale_usage_accounted {
            return Some(ReplacementFact::StaleUsageAccounted);
        }
        if !self.new_capacity_admitted {
            return Some(ReplacementFact::NewCapacityAdmitted);
        }
        None
    }

    const fn contains(self, fact: ReplacementFact) -> bool {
        match fact {
            ReplacementFact::ReachableContextsClosed => self.reachable_contexts_closed,
            ReplacementFact::AttemptIsolationProven => self.attempt_isolation_proven,
            ReplacementFact::StaleUsageAccounted => self.stale_usage_accounted,
            ReplacementFact::NewCapacityAdmitted => self.new_capacity_admitted,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ActorStateError {
    ForeignQuery,
    NotCurrent,
    ReusedAttempt,
    NonMonotonicAttempt,
    WrongPhase,
    AlreadyConcluded,
    SuccessRequiresEndOfStream,
    SuccessAlreadyAuthorized,
    RecoveryRefused(super::RecoveryRefusal),
    ReplacementNotReady(ReplacementFact),
    StaleReplacement,
    StaleAttemptCapability,
    EffectAlreadyPending,
    EffectAlreadySatisfied,
    EffectIdentityExhausted,
    StaleEffectReceipt,
    Delivery(DeliveryGateError),
}

impl From<DeliveryGateError> for ActorStateError {
    fn from(value: DeliveryGateError) -> Self {
        Self::Delivery(value)
    }
}

/// Exact timer identity. One actor owns one aggregate sleep, while this key
/// prevents deadlines for distinct contexts, operations, scans, or attempts
/// from overwriting one another.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum WakeKey {
    Statement,
    AttemptBackoff(QueryExecutionId),
    Lease(QueryContextRef),
    Operation {
        execution: QueryExecutionId,
        operation: TaskOperationId,
    },
    Credential {
        execution: QueryExecutionId,
        lease: CredentialLeaseId,
    },
    Split {
        execution: QueryExecutionId,
        plan_node: PlanNodeId,
    },
    Convergence(QueryExecutionId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScheduledWakeup {
    pub key: WakeKey,
    pub generation: u64,
    pub deadline: super::MonotonicInstant,
}

#[derive(Clone, Debug, Default)]
pub struct NextWakeup {
    deadlines: BTreeMap<(WakeKey, u64), super::MonotonicInstant>,
}

impl NextWakeup {
    pub fn schedule(&mut self, key: WakeKey, generation: u64, deadline: super::MonotonicInstant) {
        self.deadlines.insert((key, generation), deadline);
    }

    pub fn clear(&mut self, key: WakeKey, generation: u64) {
        self.deadlines.remove(&(key, generation));
    }

    pub fn next(&self) -> Option<ScheduledWakeup> {
        self.deadlines
            .iter()
            .min_by_key(|((key, generation), deadline)| (**deadline, *generation, *key))
            .map(|((key, generation), deadline)| ScheduledWakeup {
                key: *key,
                generation: *generation,
                deadline: *deadline,
            })
    }

    pub fn take_due(&mut self, now: super::MonotonicInstant) -> Vec<ScheduledWakeup> {
        let mut due = self
            .deadlines
            .iter()
            .filter(|(_, deadline)| now.has_reached(**deadline))
            .map(|((key, generation), deadline)| ScheduledWakeup {
                key: *key,
                generation: *generation,
                deadline: *deadline,
            })
            .collect::<Vec<_>>();
        due.sort_by_key(|wakeup| (wakeup.deadline, wakeup.generation, wakeup.key));
        for wakeup in &due {
            self.deadlines.remove(&(wakeup.key, wakeup.generation));
        }
        due
    }
}

/// Pure serial state of one logical execution. The async FE actor owns this
/// value exclusively and is the only caller allowed to turn effect completions
/// into replacement facts.
#[derive(Debug)]
pub(crate) struct LogicalExecutionState {
    actor_instance_id: u64,
    query_id: QueryId,
    recovery_mode: RecoveryMode,
    effect: ExecutionEffect,
    output_mode: LogicalOutputMode,
    max_attempts: u32,
    attempts_started: u32,
    phase: ExecutionPhase,
    attempts: BTreeMap<QueryExecutionId, AttemptRecord>,
    replacement: Option<ReplacementProgress>,
    effect_receipts: EffectReceiptOwner,
    stable_success_observed: Option<(QueryExecutionId, u64)>,
    delivery: DeliveryGate,
    conclusion: Option<LogicalConclusion>,
    wakeup: NextWakeup,
}

static NEXT_ACTOR_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

impl LogicalExecutionState {
    pub(crate) fn new(
        initial: QueryExecutionId,
        recovery_mode: RecoveryMode,
        effect: ExecutionEffect,
        output_mode: LogicalOutputMode,
        max_attempts: u32,
    ) -> Result<Self, ActorStateError> {
        if max_attempts == 0 {
            return Err(ActorStateError::RecoveryRefused(
                super::RecoveryRefusal::AttemptBudget,
            ));
        }
        if matches!(effect, ExecutionEffect::External)
            && !matches!(recovery_mode, RecoveryMode::NoRecovery)
        {
            return Err(ActorStateError::RecoveryRefused(
                super::RecoveryRefusal::ExternalEffect,
            ));
        }
        let actor_instance_id = NEXT_ACTOR_INSTANCE_ID
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| ActorStateError::StaleAttemptCapability)?;
        let delivery = DeliveryGate::new(initial);
        let capability_generation = delivery.eligibility_generation();
        Ok(Self {
            actor_instance_id,
            query_id: initial.query_id(),
            recovery_mode,
            effect,
            output_mode,
            max_attempts,
            attempts_started: 1,
            phase: ExecutionPhase::Instantiating {
                execution: initial,
                eligibility_generation: delivery.eligibility_generation(),
            },
            attempts: BTreeMap::from([(initial, AttemptRecord::current(capability_generation))]),
            replacement: None,
            effect_receipts: EffectReceiptOwner::new(),
            stable_success_observed: None,
            delivery,
            conclusion: None,
            wakeup: NextWakeup::default(),
        })
    }

    pub(super) fn attempt_capability(
        &self,
        execution: QueryExecutionId,
    ) -> Result<AttemptCapability, ActorStateError> {
        let record = self
            .attempts
            .get(&execution)
            .ok_or(ActorStateError::StaleAttemptCapability)?;
        Ok(AttemptCapability {
            actor_instance_id: self.actor_instance_id,
            execution,
            generation: record.capability_generation,
        })
    }

    pub const fn phase(&self) -> ExecutionPhase {
        self.phase
    }

    pub const fn delivery_phase(&self) -> DeliveryPhase {
        self.delivery.phase()
    }

    pub const fn output_visible(&self) -> bool {
        self.delivery.output_visible()
    }

    pub const fn schema_emitted(&self) -> bool {
        self.delivery.schema_emitted()
    }

    pub fn accepted_result_packets(&self) -> u64 {
        self.delivery.accepted_packets()
    }

    pub const fn delivered_result_through(&self) -> Option<u64> {
        self.delivery.delivered_through()
    }

    pub const fn conclusion(&self) -> Option<LogicalConclusion> {
        self.conclusion
    }

    pub fn wakeup_mut(&mut self) -> &mut NextWakeup {
        &mut self.wakeup
    }

    pub fn attempt(&self, execution: QueryExecutionId) -> Option<AttemptRecord> {
        self.attempts.get(&execution).copied()
    }

    pub(crate) fn begin_replacement(
        &mut self,
        failed: &AttemptCapability,
        replacement: QueryExecutionId,
        failure: AttemptFailureClass,
        deadline_reached: bool,
    ) -> Result<ReplacementToken, ActorStateError> {
        if self.conclusion.is_some() {
            return Err(ActorStateError::AlreadyConcluded);
        }
        self.require_current_capability(failed)?;
        let failed_execution = failed.execution;
        match self.phase {
            ExecutionPhase::Running { execution, .. } if execution == failed_execution => {}
            ExecutionPhase::Running { .. } => return Err(ActorStateError::NotCurrent),
            _ => return Err(ActorStateError::WrongPhase),
        }
        self.validate_replacement_identity(failed_execution, replacement)?;
        match evaluate_recovery(RecoveryInput {
            mode: self.recovery_mode,
            effect: self.effect,
            failure,
            output_visible: self.delivery.output_visible(),
            attempts_started: self.attempts_started,
            max_attempts: self.max_attempts,
            deadline_reached,
        }) {
            RecoveryDecision::BeginReplacement => {}
            RecoveryDecision::Refuse(reason) => {
                return Err(ActorStateError::RecoveryRefused(reason));
            }
        }
        let generation = self.delivery.revoke_for_replacement(failed_execution)?;
        self.attempts
            .get_mut(&failed_execution)
            .ok_or(ActorStateError::NotCurrent)?
            .disposition = AttemptDisposition::Residual;
        self.phase = ExecutionPhase::Replacing {
            failed: failed_execution,
            replacement,
            eligibility_generation: generation,
        };
        self.replacement = Some(ReplacementProgress::default());
        Ok(ReplacementToken {
            failed: failed_execution,
            replacement,
            eligibility_generation: generation,
        })
    }

    /// Registers one exact replacement prerequisite before its asynchronous
    /// effect is dispatched.  Re-registering an outstanding prerequisite is a
    /// contract error, so one callback cannot race a duplicate operation.
    pub(crate) fn issue_replacement_effect(
        &mut self,
        token: &ReplacementToken,
        fact: ReplacementFact,
    ) -> Result<PendingReplacementEffect, ActorStateError> {
        self.require_replacement_token(token)?;
        if self
            .replacement
            .ok_or(ActorStateError::WrongPhase)?
            .contains(fact)
        {
            return Err(ActorStateError::EffectAlreadySatisfied);
        }
        let effect_id = self.effect_receipts.issue(PendingEffect::Replacement {
            failed: token.failed,
            replacement: token.replacement,
            eligibility_generation: token.eligibility_generation,
            fact,
        })?;
        Ok(PendingReplacementEffect {
            actor_instance_id: self.actor_instance_id,
            effect_id,
        })
    }

    /// Consumes a supervised effect exactly once and records the fact sealed in
    /// the actor-owned pending table.  The caller never supplies the fact at
    /// completion time.
    pub(crate) fn complete_replacement_effect(
        &mut self,
        token: &ReplacementToken,
        pending: PendingReplacementEffect,
    ) -> Result<(), ActorStateError> {
        self.require_replacement_token(token)?;
        let effect = self.effect_receipts.consume(
            pending.actor_instance_id,
            self.actor_instance_id,
            pending.effect_id,
        )?;
        let PendingEffect::Replacement {
            failed,
            replacement,
            eligibility_generation,
            fact,
        } = effect
        else {
            return Err(ActorStateError::StaleEffectReceipt);
        };
        let receipt = ReplacementEffectReceipt {
            failed,
            replacement,
            eligibility_generation,
            fact,
        };
        self.record_replacement_receipt(token, receipt)
    }

    fn record_replacement_receipt(
        &mut self,
        token: &ReplacementToken,
        receipt: ReplacementEffectReceipt,
    ) -> Result<(), ActorStateError> {
        let exact = match self.phase {
            ExecutionPhase::Replacing {
                failed,
                replacement,
                eligibility_generation,
            } => {
                failed == token.failed
                    && replacement == token.replacement
                    && eligibility_generation == token.eligibility_generation
                    && failed == receipt.failed
                    && replacement == receipt.replacement
                    && eligibility_generation == receipt.eligibility_generation
            }
            _ => return Err(ActorStateError::WrongPhase),
        };
        if !exact {
            return Err(ActorStateError::StaleReplacement);
        }
        self.replacement
            .as_mut()
            .ok_or(ActorStateError::WrongPhase)?
            .record(receipt.fact);
        Ok(())
    }

    fn require_replacement_token(&self, token: &ReplacementToken) -> Result<(), ActorStateError> {
        let exact = matches!(
            self.phase,
            ExecutionPhase::Replacing {
                failed,
                replacement,
                eligibility_generation,
            } if failed == token.failed
                && replacement == token.replacement
                && eligibility_generation == token.eligibility_generation
        );
        if exact {
            Ok(())
        } else {
            Err(ActorStateError::StaleReplacement)
        }
    }

    pub(crate) fn activate_replacement(
        &mut self,
        token: &ReplacementToken,
    ) -> Result<AttemptCapability, ActorStateError> {
        let exact = match self.phase {
            ExecutionPhase::Replacing {
                failed,
                replacement,
                eligibility_generation,
            } => {
                failed == token.failed
                    && replacement == token.replacement
                    && eligibility_generation == token.eligibility_generation
            }
            _ => return Err(ActorStateError::WrongPhase),
        };
        if !exact {
            return Err(ActorStateError::StaleReplacement);
        }
        if let Some(missing) = self
            .replacement
            .ok_or(ActorStateError::WrongPhase)?
            .missing()
        {
            return Err(ActorStateError::ReplacementNotReady(missing));
        }
        let generation = self.delivery.activate(token.replacement)?;
        self.attempts
            .insert(token.replacement, AttemptRecord::current(generation));
        self.attempts_started =
            self.attempts_started
                .checked_add(1)
                .ok_or(ActorStateError::RecoveryRefused(
                    super::RecoveryRefusal::AttemptBudget,
                ))?;
        self.phase = ExecutionPhase::Instantiating {
            execution: token.replacement,
            eligibility_generation: generation,
        };
        self.replacement = None;
        self.stable_success_observed = None;
        self.attempt_capability(token.replacement)
    }

    fn validate_replacement_identity(
        &self,
        failed: QueryExecutionId,
        replacement: QueryExecutionId,
    ) -> Result<(), ActorStateError> {
        if replacement.query_id() != self.query_id {
            return Err(ActorStateError::ForeignQuery);
        }
        if self.attempts.contains_key(&replacement) {
            return Err(ActorStateError::ReusedAttempt);
        }
        if replacement.attempt_id() <= failed.attempt_id() {
            return Err(ActorStateError::NonMonotonicAttempt);
        }
        Ok(())
    }

    pub(crate) fn mark_running(
        &mut self,
        capability: &AttemptCapability,
    ) -> Result<(), ActorStateError> {
        self.validate_attempt_capability(capability)?;
        let execution = capability.execution;
        match self.phase {
            ExecutionPhase::Instantiating {
                execution: expected,
                eligibility_generation: generation,
            } if expected == execution && generation == capability.generation => {
                self.phase = ExecutionPhase::Running {
                    execution,
                    eligibility_generation: capability.generation,
                };
                Ok(())
            }
            ExecutionPhase::Instantiating {
                execution: expected,
                ..
            } if expected != execution => Err(ActorStateError::NotCurrent),
            _ => Err(ActorStateError::WrongPhase),
        }
    }

    pub(crate) fn begin_schema_delivery(
        &mut self,
        capability: &AttemptCapability,
    ) -> Result<BeginSchemaDelivery, ActorStateError> {
        if !matches!(self.output_mode, LogicalOutputMode::ResultStream) {
            return Err(ActorStateError::Delivery(
                DeliveryGateError::WrongOutputMode,
            ));
        }
        self.require_running_capability(capability)?;
        self.delivery
            .begin_schema_delivery(capability.execution)
            .map_err(ActorStateError::from)
    }

    pub(crate) fn complete_schema_delivery(
        &mut self,
        permit: SchemaDeliveryPermit,
    ) -> Result<(), ActorStateError> {
        self.require_running(permit.execution())?;
        self.delivery
            .complete_schema_delivery(permit)
            .map_err(ActorStateError::from)
    }

    pub(crate) fn fail_schema_delivery(
        &mut self,
        permit: SchemaDeliveryPermit,
    ) -> Result<(), ActorStateError> {
        let execution = permit.execution();
        self.require_running(execution)?;
        self.delivery.fail_schema_delivery(permit)?;
        self.conclude_current_execution(execution, LogicalConclusion::Failed)
    }

    pub(crate) fn begin_result_delivery<P>(
        &mut self,
        capability: &AttemptCapability,
        sequence: u64,
        packet: ResultPacket<P>,
    ) -> Result<DeliveryPermit<P>, DeliveryRejection<P>> {
        if !matches!(self.output_mode, LogicalOutputMode::ResultStream) {
            return Err(DeliveryRejection::new(
                DeliveryGateError::WrongOutputMode,
                packet,
            ));
        }
        if matches!(packet, ResultPacket::EndOfStream) {
            return Err(DeliveryRejection::new(
                DeliveryGateError::WrongExecutionPhase,
                packet,
            ));
        }
        if self.require_running_capability(capability).is_err() {
            return Err(DeliveryRejection::new(
                DeliveryGateError::WrongExecutionPhase,
                packet,
            ));
        }
        self.delivery
            .begin_delivery(capability.execution, sequence, packet)
    }

    pub(crate) fn complete_result_delivery<P>(
        &mut self,
        permit: DeliveryPermit<P>,
    ) -> Result<CompletedDelivery<P>, DeliveryCompletionError<P>> {
        self.delivery.complete_delivery(permit)
    }

    pub(crate) fn fail_result_delivery<P>(
        &mut self,
        permit: DeliveryPermit<P>,
    ) -> Result<ResultPacket<P>, DeliveryCompletionError<P>> {
        let execution = permit.execution();
        let packet = self.delivery.fail_delivery(permit)?;
        self.conclude_current_execution(execution, LogicalConclusion::Failed)
            .expect("a current delivery failure has no prior stable conclusion");
        Ok(packet)
    }

    /// Grants the only success-EOF write. Success remains provisional until
    /// this exact permit completes; cancellation or a later exact failure may
    /// consume it before protocol acceptance and publish a non-success result.
    pub(crate) fn begin_success_end_of_stream(
        &mut self,
        capability: &AttemptCapability,
        success: &AttemptSuccessReceipt,
        sequence: u64,
    ) -> Result<SuccessEndOfStreamPermit, DeliveryRejection<()>> {
        if !matches!(self.output_mode, LogicalOutputMode::ResultStream) {
            return Err(DeliveryRejection::new(
                DeliveryGateError::WrongOutputMode,
                ResultPacket::EndOfStream,
            ));
        }
        if self.require_running_capability(capability).is_err()
            || success.actor_instance_id != capability.actor_instance_id
            || success.execution != capability.execution
            || success.capability_generation != capability.generation
        {
            return Err(DeliveryRejection::new(
                DeliveryGateError::WrongExecutionPhase,
                ResultPacket::EndOfStream,
            ));
        }
        let execution = capability.execution;
        let generation = match self.phase {
            ExecutionPhase::Running {
                execution: current,
                eligibility_generation,
            } if current == execution => eligibility_generation,
            _ => {
                return Err(DeliveryRejection::new(
                    DeliveryGateError::WrongExecutionPhase,
                    ResultPacket::EndOfStream,
                ));
            }
        };
        let permit =
            self.delivery
                .begin_delivery(execution, sequence, ResultPacket::EndOfStream)?;
        self.phase = ExecutionPhase::FinishingSuccess {
            execution,
            eligibility_generation: generation,
        };
        Ok(SuccessEndOfStreamPermit { inner: permit })
    }

    /// Creates the only pending authority that may report stable success for
    /// this exact current attempt.  The production actor issues it before
    /// awaiting its supervised attempt-completion effect.
    pub(crate) fn issue_attempt_success_effect(
        &mut self,
        capability: &AttemptCapability,
    ) -> Result<PendingAttemptSuccessEffect, ActorStateError> {
        self.require_running_capability(capability)?;
        if self.stable_success_observed.is_some() {
            return Err(ActorStateError::EffectAlreadySatisfied);
        }
        let effect_id = self.effect_receipts.issue(PendingEffect::AttemptSuccess {
            execution: capability.execution,
            capability_generation: capability.generation,
        })?;
        Ok(PendingAttemptSuccessEffect {
            actor_instance_id: self.actor_instance_id,
            effect_id,
        })
    }

    /// Consumes a supervised stable-success observation exactly once.  An EOS
    /// packet cannot mint this receipt and therefore cannot self-authorize the
    /// logical success transition.
    pub(crate) fn complete_attempt_success_effect(
        &mut self,
        capability: &AttemptCapability,
        pending: PendingAttemptSuccessEffect,
    ) -> Result<AttemptSuccessReceipt, ActorStateError> {
        self.require_running_capability(capability)?;
        let effect = self.effect_receipts.consume(
            pending.actor_instance_id,
            self.actor_instance_id,
            pending.effect_id,
        )?;
        let PendingEffect::AttemptSuccess {
            execution,
            capability_generation,
        } = effect
        else {
            return Err(ActorStateError::StaleEffectReceipt);
        };
        if execution != capability.execution || capability_generation != capability.generation {
            return Err(ActorStateError::StaleEffectReceipt);
        }
        self.stable_success_observed = Some((execution, capability_generation));
        Ok(AttemptSuccessReceipt::from_effect(capability))
    }

    pub(crate) fn complete_success_end_of_stream(
        &mut self,
        permit: SuccessEndOfStreamPermit,
    ) -> Result<DeliveryCompletion, DeliveryCompletionError<()>> {
        let execution = permit.inner.execution();
        if !matches!(
            self.phase,
            ExecutionPhase::FinishingSuccess {
                execution: current,
                ..
            } if current == execution
        ) {
            return Err(DeliveryCompletionError::new(
                DeliveryGateError::WrongExecutionPhase,
                permit.inner,
            ));
        }
        let completed = self.delivery.complete_delivery(permit.inner)?;
        let completion = completed.completion();
        debug_assert!(completion.end_of_stream());
        self.finish_stream_success(execution)
            .expect("an authorized success EOF has one current attempt");
        Ok(completion)
    }

    pub(crate) fn fail_success_end_of_stream(
        &mut self,
        permit: SuccessEndOfStreamPermit,
    ) -> Result<(), DeliveryCompletionError<()>> {
        self.conclude_success_end_of_stream(permit, LogicalConclusion::Failed)
    }

    pub(crate) fn conclude_success_end_of_stream(
        &mut self,
        permit: SuccessEndOfStreamPermit,
        conclusion: LogicalConclusion,
    ) -> Result<(), DeliveryCompletionError<()>> {
        let execution = permit.inner.execution();
        if matches!(conclusion, LogicalConclusion::Succeeded) {
            return Err(DeliveryCompletionError::new(
                DeliveryGateError::WrongExecutionPhase,
                permit.inner,
            ));
        }
        if !matches!(
            self.phase,
            ExecutionPhase::FinishingSuccess {
                execution: current,
                ..
            } if current == execution
        ) {
            return Err(DeliveryCompletionError::new(
                DeliveryGateError::WrongExecutionPhase,
                permit.inner,
            ));
        }
        let _ = self.delivery.fail_delivery(permit.inner)?;
        self.delivery
            .close_current(execution)
            .expect("the failed success EOF still owns current eligibility");
        self.attempts
            .get_mut(&execution)
            .expect("a finishing execution remains registered")
            .disposition = AttemptDisposition::Residual;
        self.conclusion = Some(conclusion);
        self.phase = ExecutionPhase::Converging;
        debug_assert!(!self.delivery.has_eligible_attempt());
        Ok(())
    }

    fn finish_stream_success(
        &mut self,
        execution: QueryExecutionId,
    ) -> Result<(), ActorStateError> {
        if self.conclusion.is_some() {
            return Err(ActorStateError::AlreadyConcluded);
        }
        self.delivery.close_current(execution)?;
        self.attempts
            .get_mut(&execution)
            .ok_or(ActorStateError::NotCurrent)?
            .disposition = AttemptDisposition::Residual;
        self.conclusion = Some(LogicalConclusion::Succeeded);
        self.phase = ExecutionPhase::Converging;
        self.replacement = None;
        Ok(())
    }

    fn require_running(&self, execution: QueryExecutionId) -> Result<(), ActorStateError> {
        match self.phase {
            ExecutionPhase::Running {
                execution: current, ..
            } if current == execution => Ok(()),
            ExecutionPhase::Running { .. } => Err(ActorStateError::NotCurrent),
            _ => Err(ActorStateError::WrongPhase),
        }
    }

    pub(crate) fn record_unknown_or_usage(
        &mut self,
        capability: &AttemptCapability,
        current_unknown: bool,
        last_known_usage_bytes: u64,
    ) -> Result<(), ActorStateError> {
        self.validate_attempt_capability(capability)?;
        let execution = capability.execution;
        let record = self
            .attempts
            .get_mut(&execution)
            .ok_or(ActorStateError::NotCurrent)?;
        record.current_unknown |= current_unknown;
        record.last_known_usage_bytes = record.last_known_usage_bytes.max(last_known_usage_bytes);
        Ok(())
    }

    pub(crate) fn observe_convergence(
        &mut self,
        capability: &AttemptCapability,
        facts: AttemptConvergenceFacts,
    ) -> Result<bool, ActorStateError> {
        self.validate_attempt_capability(capability)?;
        let execution = capability.execution;
        let record = self
            .attempts
            .get_mut(&execution)
            .ok_or(ActorStateError::NotCurrent)?;
        if !matches!(record.disposition, AttemptDisposition::Residual) {
            return Err(ActorStateError::NotCurrent);
        }
        record.convergence.merge(facts);
        Ok(record.convergence.retired())
    }

    pub fn all_attempts_converged(&self) -> bool {
        self.conclusion.is_some()
            && self
                .attempts
                .values()
                .all(|attempt| attempt.convergence.retired())
    }

    pub(crate) fn conclude(
        &mut self,
        current: &AttemptCapability,
        conclusion: LogicalConclusion,
    ) -> Result<(), ActorStateError> {
        if matches!(conclusion, LogicalConclusion::Succeeded) {
            self.require_running_capability(current)?;
        } else {
            self.require_current_capability(current)?;
        }
        self.conclude_current_execution(current.execution, conclusion)
    }

    /// Concludes while replacement qualification owns the logical execution
    /// and there is deliberately no current attempt.  The exact replacement
    /// token is the current phase capability; a residual attempt capability
    /// cannot enter this path.
    pub(crate) fn conclude_replacement(
        &mut self,
        token: &ReplacementToken,
        conclusion: LogicalConclusion,
    ) -> Result<(), ActorStateError> {
        if self.conclusion.is_some() {
            return Err(ActorStateError::AlreadyConcluded);
        }
        let exact = matches!(
            self.phase,
            ExecutionPhase::Replacing {
                failed,
                replacement,
                eligibility_generation,
            } if failed == token.failed
                && replacement == token.replacement
                && eligibility_generation == token.eligibility_generation
        );
        if !exact {
            return Err(ActorStateError::StaleReplacement);
        }
        if matches!(conclusion, LogicalConclusion::Succeeded) {
            return Err(ActorStateError::WrongPhase);
        }
        self.conclusion = Some(conclusion);
        self.phase = ExecutionPhase::Converging;
        self.replacement = None;
        Ok(())
    }

    fn conclude_current_execution(
        &mut self,
        execution: QueryExecutionId,
        conclusion: LogicalConclusion,
    ) -> Result<(), ActorStateError> {
        if self.conclusion.is_some() {
            return Err(ActorStateError::AlreadyConcluded);
        }
        if matches!(self.phase, ExecutionPhase::FinishingSuccess { .. }) {
            return Err(ActorStateError::SuccessAlreadyAuthorized);
        }
        if matches!(conclusion, LogicalConclusion::Succeeded)
            && matches!(self.output_mode, LogicalOutputMode::ResultStream)
        {
            return Err(ActorStateError::SuccessRequiresEndOfStream);
        }
        if let ExecutionPhase::Running {
            execution: current, ..
        }
        | ExecutionPhase::Instantiating {
            execution: current, ..
        } = self.phase
        {
            if current != execution {
                return Err(ActorStateError::NotCurrent);
            }
            self.delivery.close_current(current)?;
            self.attempts
                .get_mut(&current)
                .ok_or(ActorStateError::NotCurrent)?
                .disposition = AttemptDisposition::Residual;
        }
        self.conclusion = Some(conclusion);
        self.phase = ExecutionPhase::Converging;
        self.replacement = None;
        Ok(())
    }

    fn validate_attempt_capability(
        &self,
        capability: &AttemptCapability,
    ) -> Result<(), ActorStateError> {
        if capability.actor_instance_id != self.actor_instance_id {
            return Err(ActorStateError::StaleAttemptCapability);
        }
        let record = self
            .attempts
            .get(&capability.execution)
            .ok_or(ActorStateError::StaleAttemptCapability)?;
        if record.capability_generation != capability.generation {
            return Err(ActorStateError::StaleAttemptCapability);
        }
        Ok(())
    }

    fn require_current_capability(
        &self,
        capability: &AttemptCapability,
    ) -> Result<(), ActorStateError> {
        self.validate_attempt_capability(capability)?;
        let record = self
            .attempts
            .get(&capability.execution)
            .ok_or(ActorStateError::StaleAttemptCapability)?;
        if !matches!(record.disposition, AttemptDisposition::Current) {
            return Err(ActorStateError::NotCurrent);
        }
        Ok(())
    }

    fn require_running_capability(
        &self,
        capability: &AttemptCapability,
    ) -> Result<(), ActorStateError> {
        self.require_current_capability(capability)?;
        self.require_running(capability.execution)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::RecoveryRefusal;
    use novarocks_types::identity::{AttemptId, QueryId};
    use std::time::Duration;

    fn execution(attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(7, 9),
            AttemptId::new(attempt).expect("nonzero attempt"),
        )
        .expect("nonzero query")
    }

    #[derive(Debug)]
    struct Payload;

    fn ready_replacement(state: &mut LogicalExecutionState, token: &ReplacementToken) {
        for fact in [
            ReplacementFact::ReachableContextsClosed,
            ReplacementFact::AttemptIsolationProven,
            ReplacementFact::StaleUsageAccounted,
            ReplacementFact::NewCapacityAdmitted,
        ] {
            let pending = state.issue_replacement_effect(token, fact).unwrap();
            state.complete_replacement_effect(token, pending).unwrap();
        }
    }

    fn capability(state: &LogicalExecutionState, execution: QueryExecutionId) -> AttemptCapability {
        state.attempt_capability(execution).unwrap()
    }

    fn running_capability(
        state: &mut LogicalExecutionState,
        execution: QueryExecutionId,
    ) -> AttemptCapability {
        let capability = capability(state, execution);
        state.mark_running(&capability).unwrap();
        capability
    }

    fn success_receipt(
        state: &mut LogicalExecutionState,
        capability: &AttemptCapability,
    ) -> AttemptSuccessReceipt {
        let pending = state.issue_attempt_success_effect(capability).unwrap();
        state
            .complete_attempt_success_effect(capability, pending)
            .unwrap()
    }

    fn emit_schema(state: &mut LogicalExecutionState, capability: &AttemptCapability) {
        let BeginSchemaDelivery::Permit(permit) = state.begin_schema_delivery(capability).unwrap()
        else {
            panic!("schema must not already be emitted");
        };
        state.complete_schema_delivery(permit).unwrap();
    }

    #[test]
    fn initial_attempt_requires_exact_running_capability_once() {
        let mut state = LogicalExecutionState::new(
            execution(1),
            RecoveryMode::RestartAttemptBeforeVisibility,
            ExecutionEffect::None,
            LogicalOutputMode::ResultStream,
            2,
        )
        .unwrap();
        let initial = capability(&state, execution(1));

        assert!(matches!(
            state.phase(),
            ExecutionPhase::Instantiating { execution: current, .. }
                if current == execution(1)
        ));
        assert!(matches!(
            state.begin_schema_delivery(&initial),
            Err(ActorStateError::WrongPhase)
        ));
        assert_eq!(
            state
                .begin_result_delivery(&initial, 0, ResultPacket::Data(Payload))
                .unwrap_err()
                .error(),
            DeliveryGateError::WrongExecutionPhase
        );
        assert_eq!(
            state.begin_replacement(
                &initial,
                execution(2),
                AttemptFailureClass::RecoverableInfrastructure,
                false,
            ),
            Err(ActorStateError::WrongPhase)
        );
        assert!(matches!(
            state.issue_attempt_success_effect(&initial),
            Err(ActorStateError::WrongPhase)
        ));
        assert_eq!(
            state.conclude(&initial, LogicalConclusion::Succeeded),
            Err(ActorStateError::WrongPhase)
        );

        let wrong = AttemptCapability {
            generation: initial.generation + 1,
            ..initial
        };
        assert_eq!(
            state.mark_running(&wrong),
            Err(ActorStateError::StaleAttemptCapability)
        );

        let initial = capability(&state, execution(1));
        state.mark_running(&initial).unwrap();
        assert!(matches!(
            state.phase(),
            ExecutionPhase::Running { execution: current, .. }
                if current == execution(1)
        ));
        assert_eq!(
            state.mark_running(&initial),
            Err(ActorStateError::WrongPhase)
        );
    }

    #[test]
    fn begin_delivery_then_replacement_is_refused() {
        let mut state = LogicalExecutionState::new(
            execution(1),
            RecoveryMode::RestartAttemptBeforeVisibility,
            ExecutionEffect::None,
            LogicalOutputMode::ResultStream,
            2,
        )
        .unwrap();
        let initial = running_capability(&mut state, execution(1));
        emit_schema(&mut state, &initial);
        let _permit = state
            .begin_result_delivery(&initial, 0, ResultPacket::Data(Payload))
            .unwrap();
        assert_eq!(
            state.begin_replacement(
                &initial,
                execution(2),
                AttemptFailureClass::RecoverableInfrastructure,
                false,
            ),
            Err(ActorStateError::RecoveryRefused(
                RecoveryRefusal::OutputVisible
            ))
        );
    }

    #[test]
    fn replacement_revokes_first_and_activates_only_after_owned_facts() {
        let mut state = LogicalExecutionState::new(
            execution(1),
            RecoveryMode::RestartAttemptBeforeVisibility,
            ExecutionEffect::None,
            LogicalOutputMode::ResultStream,
            2,
        )
        .unwrap();
        let initial = running_capability(&mut state, execution(1));
        emit_schema(&mut state, &initial);
        let token = state
            .begin_replacement(
                &initial,
                execution(2),
                AttemptFailureClass::RecoverableInfrastructure,
                false,
            )
            .unwrap();
        assert_eq!(
            state
                .begin_result_delivery(&initial, 0, ResultPacket::Data(Payload))
                .unwrap_err()
                .error(),
            DeliveryGateError::WrongExecutionPhase
        );
        assert_eq!(
            state.activate_replacement(&token),
            Err(ActorStateError::ReplacementNotReady(
                ReplacementFact::ReachableContextsClosed
            ))
        );
        ready_replacement(&mut state, &token);
        let replacement = state.activate_replacement(&token).unwrap();
        state.mark_running(&replacement).unwrap();
        assert!(matches!(
            state.phase(),
            ExecutionPhase::Running { execution: current, .. } if current == execution(2)
        ));
    }

    #[test]
    fn final_attempt_remains_until_all_convergence_facts_arrive() {
        let mut state = LogicalExecutionState::new(
            execution(1),
            RecoveryMode::NoRecovery,
            ExecutionEffect::None,
            LogicalOutputMode::CompletionOnly,
            1,
        )
        .unwrap();
        let initial = running_capability(&mut state, execution(1));
        state
            .conclude(&initial, LogicalConclusion::Succeeded)
            .unwrap();
        assert!(!state.all_attempts_converged());
        assert!(
            !state
                .observe_convergence(
                    &initial,
                    AttemptConvergenceFacts {
                        actual_stopped: true,
                        output_released: true,
                        resources_converged: false,
                    },
                )
                .unwrap()
        );
        assert!(
            state
                .observe_convergence(
                    &initial,
                    AttemptConvergenceFacts {
                        resources_converged: true,
                        ..AttemptConvergenceFacts::default()
                    },
                )
                .unwrap()
        );
        assert!(state.all_attempts_converged());
    }

    #[test]
    fn completion_only_execution_cannot_enter_result_delivery() {
        let mut state = LogicalExecutionState::new(
            execution(1),
            RecoveryMode::NoRecovery,
            ExecutionEffect::None,
            LogicalOutputMode::CompletionOnly,
            1,
        )
        .unwrap();
        let initial = running_capability(&mut state, execution(1));
        assert_eq!(
            state
                .begin_result_delivery(&initial, 0, ResultPacket::Data(Payload))
                .unwrap_err()
                .error(),
            DeliveryGateError::WrongOutputMode
        );
        assert!(matches!(
            state.begin_schema_delivery(&initial),
            Err(ActorStateError::Delivery(
                DeliveryGateError::WrongOutputMode
            ))
        ));
        state
            .conclude(&initial, LogicalConclusion::Succeeded)
            .unwrap();
    }

    #[test]
    fn streamed_success_requires_eos_and_delivery_completion() {
        let mut state = LogicalExecutionState::new(
            execution(1),
            RecoveryMode::NoRecovery,
            ExecutionEffect::None,
            LogicalOutputMode::ResultStream,
            1,
        )
        .unwrap();
        let initial = running_capability(&mut state, execution(1));
        emit_schema(&mut state, &initial);
        assert_eq!(
            state.conclude(&initial, LogicalConclusion::Succeeded),
            Err(ActorStateError::SuccessRequiresEndOfStream)
        );
        let success = success_receipt(&mut state, &initial);
        let permit = state
            .begin_success_end_of_stream(&initial, &success, 0)
            .unwrap();
        state.complete_success_end_of_stream(permit).unwrap();
        assert_eq!(state.conclusion(), Some(LogicalConclusion::Succeeded));
        assert_eq!(
            state.conclude(&initial, LogicalConclusion::Failed),
            Err(ActorStateError::NotCurrent)
        );
    }

    #[test]
    fn data_and_success_eos_require_committed_schema() {
        let mut state = LogicalExecutionState::new(
            execution(1),
            RecoveryMode::NoRecovery,
            ExecutionEffect::None,
            LogicalOutputMode::ResultStream,
            1,
        )
        .unwrap();
        let initial = running_capability(&mut state, execution(1));
        assert_eq!(
            state
                .begin_result_delivery(&initial, 0, ResultPacket::Data(Payload))
                .unwrap_err()
                .error(),
            DeliveryGateError::SchemaNotEmitted
        );
        let success = success_receipt(&mut state, &initial);
        assert_eq!(
            state
                .begin_success_end_of_stream(&initial, &success, 0)
                .unwrap_err()
                .error(),
            DeliveryGateError::SchemaNotEmitted
        );
        let BeginSchemaDelivery::Permit(schema) = state.begin_schema_delivery(&initial).unwrap()
        else {
            panic!("first schema write must receive a permit");
        };
        assert_eq!(
            state
                .begin_success_end_of_stream(&initial, &success, 0)
                .unwrap_err()
                .error(),
            DeliveryGateError::SchemaDeliveryInFlight
        );
        state.fail_schema_delivery(schema).unwrap();
        assert_eq!(state.conclusion(), Some(LogicalConclusion::Failed));
    }

    #[test]
    fn failure_wins_before_success_eos_authorization() {
        let mut state = LogicalExecutionState::new(
            execution(1),
            RecoveryMode::NoRecovery,
            ExecutionEffect::None,
            LogicalOutputMode::ResultStream,
            1,
        )
        .unwrap();
        let initial = running_capability(&mut state, execution(1));
        let success = success_receipt(&mut state, &initial);
        state.conclude(&initial, LogicalConclusion::Failed).unwrap();
        assert_eq!(
            state
                .begin_success_end_of_stream(&initial, &success, 0)
                .unwrap_err()
                .error(),
            DeliveryGateError::WrongExecutionPhase
        );
        assert_eq!(state.conclusion(), Some(LogicalConclusion::Failed));
    }

    #[test]
    fn only_the_eof_permit_can_replace_provisional_success() {
        let mut state = LogicalExecutionState::new(
            execution(1),
            RecoveryMode::NoRecovery,
            ExecutionEffect::None,
            LogicalOutputMode::ResultStream,
            1,
        )
        .unwrap();
        let initial = running_capability(&mut state, execution(1));
        emit_schema(&mut state, &initial);
        let success = success_receipt(&mut state, &initial);
        let permit = state
            .begin_success_end_of_stream(&initial, &success, 0)
            .unwrap();
        assert_eq!(
            state.conclude(&initial, LogicalConclusion::Failed),
            Err(ActorStateError::SuccessAlreadyAuthorized)
        );
        state
            .conclude_success_end_of_stream(permit, LogicalConclusion::Cancelled)
            .unwrap();
        assert_eq!(state.conclusion(), Some(LogicalConclusion::Cancelled));
    }

    #[test]
    fn success_eos_protocol_failure_publishes_failure() {
        let mut state = LogicalExecutionState::new(
            execution(1),
            RecoveryMode::NoRecovery,
            ExecutionEffect::None,
            LogicalOutputMode::ResultStream,
            1,
        )
        .unwrap();
        let initial = running_capability(&mut state, execution(1));
        emit_schema(&mut state, &initial);
        let success = success_receipt(&mut state, &initial);
        let permit = state
            .begin_success_end_of_stream(&initial, &success, 0)
            .unwrap();
        state.fail_success_end_of_stream(permit).unwrap();
        assert_eq!(state.conclusion(), Some(LogicalConclusion::Failed));
    }

    #[test]
    fn replacement_token_binds_the_proposed_attempt() {
        let mut state = LogicalExecutionState::new(
            execution(1),
            RecoveryMode::RestartAttemptBeforeVisibility,
            ExecutionEffect::None,
            LogicalOutputMode::ResultStream,
            2,
        )
        .unwrap();
        let initial = running_capability(&mut state, execution(1));
        let token = state
            .begin_replacement(
                &initial,
                execution(2),
                AttemptFailureClass::RecoverableInfrastructure,
                false,
            )
            .unwrap();
        let stale = ReplacementToken {
            replacement: execution(3),
            ..token.clone()
        };
        assert_eq!(
            state.record_replacement_receipt(
                &token,
                ReplacementEffectReceipt::from_effect(
                    &stale,
                    ReplacementFact::ReachableContextsClosed,
                ),
            ),
            Err(ActorStateError::StaleReplacement)
        );
        ready_replacement(&mut state, &token);
        assert_eq!(
            state.activate_replacement(&stale),
            Err(ActorStateError::StaleReplacement)
        );
        state.activate_replacement(&token).unwrap();
    }

    #[test]
    fn residual_attempt_capability_cannot_conclude_its_replacement() {
        let mut state = LogicalExecutionState::new(
            execution(1),
            RecoveryMode::RestartAttemptBeforeVisibility,
            ExecutionEffect::None,
            LogicalOutputMode::CompletionOnly,
            2,
        )
        .unwrap();
        let initial = running_capability(&mut state, execution(1));
        let token = state
            .begin_replacement(
                &initial,
                execution(2),
                AttemptFailureClass::RecoverableInfrastructure,
                false,
            )
            .unwrap();
        ready_replacement(&mut state, &token);
        let replacement = state.activate_replacement(&token).unwrap();
        state.mark_running(&replacement).unwrap();

        assert_eq!(
            state.conclude(&initial, LogicalConclusion::Failed),
            Err(ActorStateError::NotCurrent)
        );
        assert_eq!(state.conclusion(), None);
        assert!(matches!(
            state.phase(),
            ExecutionPhase::Running { execution: current, .. } if current == execution(2)
        ));
    }

    #[test]
    fn exact_replacement_capability_can_cancel_while_no_attempt_is_current() {
        let mut state = LogicalExecutionState::new(
            execution(1),
            RecoveryMode::RestartAttemptBeforeVisibility,
            ExecutionEffect::None,
            LogicalOutputMode::CompletionOnly,
            2,
        )
        .unwrap();
        let initial = running_capability(&mut state, execution(1));
        let token = state
            .begin_replacement(
                &initial,
                execution(2),
                AttemptFailureClass::RecoverableInfrastructure,
                false,
            )
            .unwrap();

        assert_eq!(
            state.conclude(&initial, LogicalConclusion::Cancelled),
            Err(ActorStateError::NotCurrent)
        );
        state
            .conclude_replacement(&token, LogicalConclusion::Cancelled)
            .unwrap();
        assert_eq!(state.conclusion(), Some(LogicalConclusion::Cancelled));
        assert!(matches!(state.phase(), ExecutionPhase::Converging));
    }

    #[test]
    fn replacement_effect_receipt_is_pending_bound_and_first_wins() {
        let mut state = LogicalExecutionState::new(
            execution(1),
            RecoveryMode::RestartAttemptBeforeVisibility,
            ExecutionEffect::None,
            LogicalOutputMode::CompletionOnly,
            2,
        )
        .unwrap();
        let initial = running_capability(&mut state, execution(1));
        let token = state
            .begin_replacement(
                &initial,
                execution(2),
                AttemptFailureClass::RecoverableInfrastructure,
                false,
            )
            .unwrap();
        let pending = state
            .issue_replacement_effect(&token, ReplacementFact::ReachableContextsClosed)
            .unwrap();
        assert!(matches!(
            state.issue_replacement_effect(&token, ReplacementFact::ReachableContextsClosed),
            Err(ActorStateError::EffectAlreadyPending)
        ));
        let replay = PendingReplacementEffect {
            actor_instance_id: pending.actor_instance_id,
            effect_id: pending.effect_id,
        };
        state.complete_replacement_effect(&token, pending).unwrap();
        assert_eq!(
            state.complete_replacement_effect(&token, replay),
            Err(ActorStateError::StaleEffectReceipt)
        );
        assert!(matches!(
            state.issue_replacement_effect(&token, ReplacementFact::ReachableContextsClosed),
            Err(ActorStateError::EffectAlreadySatisfied)
        ));
    }

    #[test]
    fn success_effect_handle_cannot_cross_actor_instances() {
        let mut first = LogicalExecutionState::new(
            execution(1),
            RecoveryMode::NoRecovery,
            ExecutionEffect::None,
            LogicalOutputMode::ResultStream,
            1,
        )
        .unwrap();
        let mut second = LogicalExecutionState::new(
            execution(1),
            RecoveryMode::NoRecovery,
            ExecutionEffect::None,
            LogicalOutputMode::ResultStream,
            1,
        )
        .unwrap();
        let first_capability = running_capability(&mut first, execution(1));
        let second_capability = running_capability(&mut second, execution(1));
        let pending = first
            .issue_attempt_success_effect(&first_capability)
            .unwrap();
        let original = PendingAttemptSuccessEffect {
            actor_instance_id: pending.actor_instance_id,
            effect_id: pending.effect_id,
        };
        assert!(matches!(
            second.complete_attempt_success_effect(&second_capability, pending),
            Err(ActorStateError::StaleEffectReceipt)
        ));
        first
            .complete_attempt_success_effect(&first_capability, original)
            .expect("foreign consumption must not remove the owner's pending effect");
        assert!(matches!(
            first.issue_attempt_success_effect(&first_capability),
            Err(ActorStateError::EffectAlreadySatisfied)
        ));
    }

    #[test]
    fn attempt_capability_and_success_receipt_cannot_cross_actor_instances() {
        let mut first = LogicalExecutionState::new(
            execution(1),
            RecoveryMode::NoRecovery,
            ExecutionEffect::None,
            LogicalOutputMode::ResultStream,
            1,
        )
        .unwrap();
        let mut second = LogicalExecutionState::new(
            execution(1),
            RecoveryMode::NoRecovery,
            ExecutionEffect::None,
            LogicalOutputMode::ResultStream,
            1,
        )
        .unwrap();
        let first_capability = running_capability(&mut first, execution(1));
        let second_capability = running_capability(&mut second, execution(1));
        let first_success = success_receipt(&mut first, &first_capability);
        emit_schema(&mut second, &second_capability);

        assert_eq!(
            second.conclude(&first_capability, LogicalConclusion::Failed),
            Err(ActorStateError::StaleAttemptCapability)
        );
        assert_eq!(
            second
                .begin_success_end_of_stream(&second_capability, &first_success, 0,)
                .unwrap_err()
                .error(),
            DeliveryGateError::WrongExecutionPhase
        );
        assert_eq!(second.conclusion(), None);

        first
            .conclude(&first_capability, LogicalConclusion::Failed)
            .unwrap();
    }

    #[test]
    fn schema_failure_is_a_stable_statement_failure() {
        let mut state = LogicalExecutionState::new(
            execution(1),
            RecoveryMode::RestartAttemptBeforeVisibility,
            ExecutionEffect::None,
            LogicalOutputMode::ResultStream,
            2,
        )
        .unwrap();
        let initial = running_capability(&mut state, execution(1));
        let BeginSchemaDelivery::Permit(permit) = state.begin_schema_delivery(&initial).unwrap()
        else {
            panic!("first schema write must receive a permit");
        };
        state.fail_schema_delivery(permit).unwrap();
        assert_eq!(state.conclusion(), Some(LogicalConclusion::Failed));
        assert!(matches!(state.phase(), ExecutionPhase::Converging));
    }

    #[test]
    fn exact_timer_keys_do_not_overwrite_attempts() {
        let mut wakeup = NextWakeup::default();
        let origin = super::super::MonotonicInstant::ORIGIN;
        wakeup.schedule(
            WakeKey::AttemptBackoff(execution(1)),
            1,
            origin.saturating_add(Duration::from_secs(5)),
        );
        wakeup.schedule(
            WakeKey::AttemptBackoff(execution(2)),
            2,
            origin.saturating_add(Duration::from_secs(2)),
        );
        assert_eq!(
            wakeup.next(),
            Some(ScheduledWakeup {
                key: WakeKey::AttemptBackoff(execution(2)),
                generation: 2,
                deadline: origin.saturating_add(Duration::from_secs(2)),
            })
        );
        assert_eq!(
            wakeup.take_due(origin.saturating_add(Duration::from_secs(3))),
            vec![ScheduledWakeup {
                key: WakeKey::AttemptBackoff(execution(2)),
                generation: 2,
                deadline: origin.saturating_add(Duration::from_secs(2)),
            }]
        );
        assert!(wakeup.next().is_some());
    }

    #[test]
    fn stale_timer_generation_cannot_replace_or_clear_the_current_generation() {
        let mut wakeup = NextWakeup::default();
        let origin = super::super::MonotonicInstant::ORIGIN;
        let key = WakeKey::Statement;
        wakeup.schedule(key, 2, origin.saturating_add(Duration::from_secs(2)));
        wakeup.schedule(key, 1, origin.saturating_add(Duration::from_secs(1)));
        wakeup.clear(key, 1);
        assert_eq!(
            wakeup.next(),
            Some(ScheduledWakeup {
                key,
                generation: 2,
                deadline: origin.saturating_add(Duration::from_secs(2)),
            })
        );
    }
}
