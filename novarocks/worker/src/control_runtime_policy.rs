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

//! Storage is prepaid by admitted roles, never by the number of notifications.
//! Native composition supplies its real ingress and batch limits. Worker does
//! not import transport defaults or assume a single-node deployment.

use novarocks_control_runtime::actor::ActorConfig;
use novarocks_worker_protocol::context::ContextCore;

/// Physical listener positions and the exact codec batch bound. Waiting calls
/// are included so their ownership can move without acquiring another slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IngressRoleLimits {
    pub ordinary_running: usize,
    pub ordinary_waiting: usize,
    pub control_running: usize,
    pub control_waiting: usize,
    pub max_batch_items: usize,
    pub root_read_holders: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PolicyError {
    ZeroRunningPositions,
    ZeroBatchItems,
    ZeroRootReadHolders,
    Overflow,
    InvalidStorageGeometry,
    InvalidActorBudget,
}
impl std::fmt::Display for PolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ZeroRunningPositions => "control runtime running positions must be positive",
            Self::ZeroBatchItems => "control runtime batch items must be positive",
            Self::ZeroRootReadHolders => "control runtime root read holders must be positive",
            Self::Overflow => "control runtime storage geometry overflowed",
            Self::InvalidStorageGeometry => {
                "loading storage exceeds the complete Task storage inventory"
            }
            Self::InvalidActorBudget => {
                "control runtime actor budget cannot cover every input class"
            }
        })
    }
}
impl std::error::Error for PolicyError {}

impl IngressRoleLimits {
    pub fn request_roles(self) -> Result<usize, PolicyError> {
        if self.ordinary_running == 0 || self.control_running == 0 {
            return Err(PolicyError::ZeroRunningPositions);
        }
        if self.max_batch_items == 0 {
            return Err(PolicyError::ZeroBatchItems);
        }
        if self.root_read_holders == 0 {
            return Err(PolicyError::ZeroRootReadHolders);
        }
        self.ordinary_running
            .checked_add(self.ordinary_waiting)
            .and_then(|n| n.checked_add(self.control_running))
            .and_then(|n| n.checked_add(self.control_waiting))
            .and_then(|n| n.checked_mul(self.max_batch_items))
            .ok_or(PolicyError::Overflow)
    }
}

/// Every field is a separate storage/custody dimension. In particular a work
/// exit does not release its completion or a still-consumed input batch.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StorageNeed {
    pub inputs: usize,
    pub batches: usize,
    pub effects: usize,
    pub cuts: usize,
    pub replies: usize,
    pub completions: usize,
    pub captures: usize,
    pub work: usize,
}

impl StorageNeed {
    pub fn checked_sub(self, other: Self) -> Result<Self, PolicyError> {
        Ok(Self {
            inputs: self
                .inputs
                .checked_sub(other.inputs)
                .ok_or(PolicyError::InvalidStorageGeometry)?,
            batches: self
                .batches
                .checked_sub(other.batches)
                .ok_or(PolicyError::InvalidStorageGeometry)?,
            effects: self
                .effects
                .checked_sub(other.effects)
                .ok_or(PolicyError::InvalidStorageGeometry)?,
            cuts: self
                .cuts
                .checked_sub(other.cuts)
                .ok_or(PolicyError::InvalidStorageGeometry)?,
            replies: self
                .replies
                .checked_sub(other.replies)
                .ok_or(PolicyError::InvalidStorageGeometry)?,
            completions: self
                .completions
                .checked_sub(other.completions)
                .ok_or(PolicyError::InvalidStorageGeometry)?,
            captures: self
                .captures
                .checked_sub(other.captures)
                .ok_or(PolicyError::InvalidStorageGeometry)?,
            work: self
                .work
                .checked_sub(other.work)
                .ok_or(PolicyError::InvalidStorageGeometry)?,
        })
    }
    pub fn checked_add(self, other: Self) -> Result<Self, PolicyError> {
        Ok(Self {
            inputs: self
                .inputs
                .checked_add(other.inputs)
                .ok_or(PolicyError::Overflow)?,
            batches: self
                .batches
                .checked_add(other.batches)
                .ok_or(PolicyError::Overflow)?,
            effects: self
                .effects
                .checked_add(other.effects)
                .ok_or(PolicyError::Overflow)?,
            cuts: self
                .cuts
                .checked_add(other.cuts)
                .ok_or(PolicyError::Overflow)?,
            replies: self
                .replies
                .checked_add(other.replies)
                .ok_or(PolicyError::Overflow)?,
            completions: self
                .completions
                .checked_add(other.completions)
                .ok_or(PolicyError::Overflow)?,
            captures: self
                .captures
                .checked_add(other.captures)
                .ok_or(PolicyError::Overflow)?,
            work: self
                .work
                .checked_add(other.work)
                .ok_or(PolicyError::Overflow)?,
        })
    }

    pub fn checked_mul(self, count: usize) -> Result<Self, PolicyError> {
        Ok(Self {
            inputs: self
                .inputs
                .checked_mul(count)
                .ok_or(PolicyError::Overflow)?,
            batches: self
                .batches
                .checked_mul(count)
                .ok_or(PolicyError::Overflow)?,
            effects: self
                .effects
                .checked_mul(count)
                .ok_or(PolicyError::Overflow)?,
            cuts: self.cuts.checked_mul(count).ok_or(PolicyError::Overflow)?,
            replies: self
                .replies
                .checked_mul(count)
                .ok_or(PolicyError::Overflow)?,
            completions: self
                .completions
                .checked_mul(count)
                .ok_or(PolicyError::Overflow)?,
            captures: self
                .captures
                .checked_mul(count)
                .ok_or(PolicyError::Overflow)?,
            work: self.work.checked_mul(count).ok_or(PolicyError::Overflow)?,
        })
    }

    /// A causal role has distinct prefix/event inputs, each with an atomic
    /// bounded batch. Its cut retains two rich proposal candidates; the role
    /// itself can retain one callback/input capture and one reliable exit.
    /// Each candidate additionally owns its original cleanup Work and report;
    /// a cleanup effect alone cannot account for its opaque source destructor.
    pub fn role(reply: bool) -> Self {
        Self {
            inputs: crate::task_causal_input::CAUSAL_INPUT_POSITIONS,
            batches: crate::task_causal_input::CAUSAL_INPUT_POSITIONS,
            effects: ContextCore::MAX_STEP_WORK * crate::task_causal_input::CAUSAL_INPUT_POSITIONS,
            cuts: 1,
            replies: usize::from(reply),
            completions: 3,
            captures: 3,
            work: 3,
        }
    }
}

/// Fixed Task inputs are actual one-shot lifecycle roles, not RPC op counts.
/// A role with several observations uses its existing report position and a
/// coalesced pending flag; it cannot enqueue an unbounded stream of copies.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(usize)]
pub enum TaskLifecycleRole {
    InputLoaded,
    LoaderExited,
    PreparationBodyExited,
    PreparationExited,
    Installed,
    Running,
    TerminalObservation,
    ActualStopped,
    OutputReleased,
    ResourcesConverged,
    RootIdle,
    Metrics,
    OperatorStatistics,
    DynamicFilters,
    Finisher,
    Settlement,
    WallTime,
    TerminalPublisher,
    TerminalAction,
    ContextClose,
    FailureProjection,
}
impl TaskLifecycleRole {
    pub(crate) const COUNT: usize = Self::FailureProjection as usize + 1;
    const LOADING: [Self; 2] = [Self::InputLoaded, Self::LoaderExited];
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ControlRuntimePolicy {
    pub ingress: IngressRoleLimits,
    pub request_roles: usize,
    pub task_base: StorageNeed,
    /// Creating owns these original positions before any input getter runs.
    /// Accepted reserves only the remainder, never the same storage twice.
    pub loading: StorageNeed,
    pub accepted: StorageNeed,
    /// Maximum per-Task cut geometry, including a possible concentration of
    /// the shared request pool. These positions are not all reserved per Task.
    pub max_task_cuts: usize,
    pub request_pool: StorageNeed,
    pub root_read_pool_per_root: StorageNeed,
}

impl ControlRuntimePolicy {
    pub fn try_new(ingress: IngressRoleLimits) -> Result<Self, PolicyError> {
        let request_roles = ingress.request_roles()?;
        // Preparation owns four original unwind/publication roles plus its
        // fixed cleanup steps. Their counts share the actual role definitions.
        let fixed_roles = TaskLifecycleRole::COUNT
            .checked_add(crate::task_preparation::BODY_ROLES)
            .and_then(|n| n.checked_add(crate::task_preparation::STEPS))
            .ok_or(PolicyError::Overflow)?;
        let mut task_base = StorageNeed::role(false)
            .checked_mul(fixed_roles)?
            .checked_add(crate::control_runtime::open_summary::OPEN_SUMMARY_STORAGE)?;
        // Protocol lifecycle roles are bounded input authors. The original
        // Task lifetime is a separate parent, not another notification author.
        task_base.work = task_base.work.checked_add(1).ok_or(PolicyError::Overflow)?;
        // A returned host refusal can retain its consumed creation input. Its
        // original child remains counted until actual transfer or disposal.
        task_base.work = task_base
            .work
            .checked_add(crate::control_runtime::preparation_refusal::HOST_REFUSAL_WORK_POSITIONS)
            .ok_or(PolicyError::Overflow)?;
        task_base.completions = task_base
            .completions
            .checked_add(crate::control_runtime::preparation_refusal::HOST_REFUSAL_REPORT_POSITIONS)
            .ok_or(PolicyError::Overflow)?;
        task_base.captures = task_base
            .captures
            .checked_add(
                crate::control_runtime::preparation_refusal::HOST_REFUSAL_CAPTURE_POSITIONS,
            )
            .ok_or(PolicyError::Overflow)?;
        let mut loading = StorageNeed::role(false)
            .checked_mul(TaskLifecycleRole::LOADING.len())?
            .checked_add(crate::control_runtime::open_summary::OPEN_SUMMARY_STORAGE)?;
        // Loading's two real producer/input holders have their own original
        // Work/report geometry. Keep the candidate positions of their actual
        // cuts, and fund the Task's open/consuming candidates before exposing
        // even its first port. Accepted acquires only the remaining inventory.
        let loading_sources = TaskLifecycleRole::LOADING
            .len()
            .checked_mul(2)
            .and_then(|cuts| cuts.checked_add(4))
            .ok_or(PolicyError::Overflow)?;
        loading.work = crate::control_runtime::loading::LOADING_WORK_POSITIONS
            .checked_add(loading_sources)
            .ok_or(PolicyError::Overflow)?;
        loading.completions = crate::control_runtime::loading::LOADING_REPORT_POSITIONS
            .checked_add(loading_sources)
            .ok_or(PolicyError::Overflow)?;
        loading.captures = loading
            .captures
            .checked_add(4)
            .ok_or(PolicyError::Overflow)?;
        // The open and consuming summary each have two candidate positions.
        // This is per Task. Cut candidates are already included in role(false)
        // and therefore follow the actual fixed/shared/root role inventories;
        // max_task_cuts is only an addressing bound, never another Task charge.
        task_base.captures = task_base
            .captures
            .checked_add(4)
            .ok_or(PolicyError::Overflow)?;
        task_base.work = task_base.work.checked_add(4).ok_or(PolicyError::Overflow)?;
        task_base.completions = task_base
            .completions
            .checked_add(4)
            .ok_or(PolicyError::Overflow)?;
        let accepted = task_base.checked_sub(loading)?;
        let max_task_cuts = fixed_roles
            .checked_add(request_roles)
            .and_then(|n| n.checked_add(ingress.root_read_holders))
            .ok_or(PolicyError::Overflow)?;
        Ok(Self {
            ingress,
            request_roles,
            task_base,
            loading,
            accepted,
            max_task_cuts,
            request_pool: StorageNeed::role(true).checked_mul(request_roles)?,
            root_read_pool_per_root: StorageNeed::role(false)
                .checked_mul(ingress.root_read_holders)?,
        })
    }

    /// A process pool reserves ingress roles once, independently of how many
    /// Tasks they happen to address. Per-context cumulative identity limits
    /// remain protocol facts and are deliberately absent from this formula.
    pub fn process_task_storage(
        self,
        active_tasks: usize,
        roots: usize,
    ) -> Result<StorageNeed, PolicyError> {
        self.task_base
            .checked_mul(active_tasks)?
            .checked_add(self.request_pool)?
            .checked_add(self.root_read_pool_per_root.checked_mul(roots)?)
    }

    pub fn context_actor(self, max_live_tasks: usize) -> Result<ActorConfig, PolicyError> {
        if max_live_tasks == 0 {
            return Err(PolicyError::ZeroRunningPositions);
        }
        // Periodic data uses one original input per live Task, independently
        // of how many RPC positions happen to be busy. Move its already paid
        // position between classes rather than adding another storage charge.
        let task_ordinary = crate::control_runtime::operator_statistics::STATISTICS_INPUT_POSITIONS;
        let lifecycle = self
            .task_base
            .inputs
            .checked_sub(task_ordinary)
            .ok_or(PolicyError::InvalidStorageGeometry)?
            .checked_add(self.root_read_pool_per_root.inputs)
            .and_then(|n| n.checked_mul(max_live_tasks))
            .and_then(|n| n.checked_add(self.request_roles))
            .ok_or(PolicyError::Overflow)?;
        let ordinary = task_ordinary
            .checked_mul(max_live_tasks)
            .and_then(|n| n.checked_add(self.request_roles))
            .ok_or(PolicyError::Overflow)?;
        let control = self.request_roles;
        let inputs = lifecycle
            .checked_add(ordinary)
            .and_then(|n| n.checked_add(control))
            .and_then(|n| n.checked_add(1))
            .ok_or(PolicyError::Overflow)?;
        ActorConfig {
            mailbox: [lifecycle, control, ordinary, 1],
            max_batches: inputs,
            max_effects_per_step: ContextCore::MAX_STEP_WORK,
            max_dependencies_per_effect: ContextCore::MAX_STEP_WORK,
            turn_budget: 64,
            max_step_work: ContextCore::MAX_STEP_WORK,
        }
        .validate()
        .map_err(|_| PolicyError::InvalidActorBudget)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ingress() -> IngressRoleLimits {
        IngressRoleLimits {
            ordinary_running: 3,
            ordinary_waiting: 2,
            control_running: 2,
            control_waiting: 1,
            max_batch_items: 7,
            root_read_holders: 2,
        }
    }

    #[test]
    fn request_storage_counts_every_batch_item_in_each_actual_ingress_position() {
        let policy = ControlRuntimePolicy::try_new(ingress()).unwrap();
        assert_eq!(policy.request_roles, (3 + 2 + 2 + 1) * 7);
        assert_eq!(policy.request_pool.replies, policy.request_roles);
        assert_eq!(policy.request_pool.cuts, policy.request_roles);
        let actor = policy.context_actor(9).unwrap();
        assert_eq!(
            actor.mailbox[0],
            (policy.task_base.inputs + 4 - 1) * 9 + policy.request_roles
        );
        assert_eq!(actor.mailbox[1], policy.request_roles);
        assert_eq!(actor.mailbox[2], policy.request_roles + 9);
    }

    #[test]
    fn live_statistics_inputs_do_not_borrow_busy_request_positions() {
        use novarocks_control_runtime::{
            actor::{Actor, StepContext},
            driver::{ActorRuntime, DriverError},
            transaction::RuntimeLedger,
        };
        use novarocks_control_runtime_core::{
            capacity::ResourceVector,
            identity::{ActorId, Generation, ScopeId, Tick},
            mailbox::{InputClass, InputEnvelope},
        };
        struct Idle;
        impl Actor for Idle {
            type Input = ();
            type Effect = ();
            fn timer_input(_: Tick) {}
            fn handle(&mut self, _: Tick, _: InputEnvelope, _: (), _: &mut StepContext<'_, ()>) {}
            fn next_deadline(&self) -> Option<Tick> {
                None
            }
            fn owner_failed(
                _: ScopeId,
                _: Generation,
                _: InputEnvelope,
                _: &mut StepContext<'_, ()>,
            ) {
            }
        }
        let policy = ControlRuntimePolicy::try_new(IngressRoleLimits {
            ordinary_running: 1,
            ordinary_waiting: 0,
            control_running: 1,
            control_waiting: 0,
            max_batch_items: 1,
            root_read_holders: 1,
        })
        .unwrap();
        let live_tasks = policy.request_roles + 1;
        let config = policy.context_actor(live_tasks).unwrap();
        let ledger = RuntimeLedger::new(ResourceVector::new([]).unwrap(), 1, 1);
        let scope = ledger
            .open_scope_next(None, Generation::new(1).unwrap(), 1, 1)
            .unwrap();
        let runtime = ActorRuntime::new(1);
        let actor = ActorId::new(1).unwrap();
        runtime
            .register_next(actor, scope.clone(), Idle, config)
            .unwrap_or_else(|(error, _)| panic!("actual mailbox registration: {error:?}"));
        let mut permits = Vec::new();
        // All request positions may already be busy when each live Task
        // attaches its independently paid periodic data producer.
        for _ in 0..policy.request_roles + live_tasks {
            permits.push(
                runtime
                    .reserve_input(actor, actor, InputClass::Ordinary)
                    .unwrap(),
            );
        }
        assert!(matches!(
            runtime.reserve_input(actor, actor, InputClass::Ordinary),
            Err(DriverError::InputCapacity)
        ));
        // Moving between classes must not add another storage payment.
        assert_eq!(
            config.mailbox.iter().sum::<usize>(),
            (policy.task_base.inputs + policy.root_read_pool_per_root.inputs) * live_tasks
                + 3 * policy.request_roles
                + 1
        );
        for permit in permits {
            permit.revoke_unused().unwrap();
        }
        scope.close_external().unwrap();
        runtime
            .remove_actor(actor)
            .unwrap_or_else(|error| panic!("retired mailbox: {error:?}"));
        ledger.remove_scope(scope.id()).unwrap();
    }

    #[test]
    fn process_request_pool_is_shared_rather_than_multiplied_by_each_task() {
        let policy = ControlRuntimePolicy::try_new(ingress()).unwrap();
        assert_eq!(
            policy.loading.checked_add(policy.accepted).unwrap(),
            policy.task_base
        );
        assert!(policy.loading.work > 0 && policy.loading.completions > 0);
        let one = policy.process_task_storage(1, 0).unwrap();
        let two = policy.process_task_storage(2, 0).unwrap();
        assert_eq!(two.replies, one.replies);
        assert_eq!(two.inputs - one.inputs, policy.task_base.inputs);
        assert_eq!(two.cuts - one.cuts, policy.task_base.cuts);
        assert_eq!(two.work - one.work, policy.task_base.work);
        assert_eq!(
            two.completions - one.completions,
            policy.task_base.completions
        );
        let root = policy.process_task_storage(2, 1).unwrap();
        assert_eq!(root.cuts - two.cuts, 2);
        assert_eq!(root.work - two.work, policy.root_read_pool_per_root.work);
        assert_eq!(
            root.completions - two.completions,
            policy.root_read_pool_per_root.completions
        );
        assert_eq!(
            policy.max_task_cuts,
            policy.task_base.cuts + policy.request_roles + 2
        );
    }

    #[test]
    fn increasing_shared_ingress_does_not_charge_its_source_candidates_to_each_task() {
        let small = ControlRuntimePolicy::try_new(ingress()).unwrap();
        let mut expanded = ingress();
        expanded.ordinary_running += 2;
        let large = ControlRuntimePolicy::try_new(expanded).unwrap();
        assert_eq!(small.task_base, large.task_base);
        assert_eq!(small.loading, large.loading);
        assert_eq!(small.accepted, large.accepted);
        let extra_pool = large.request_pool.checked_sub(small.request_pool).unwrap();
        for tasks in [0, 1, 9] {
            let delta = large
                .process_task_storage(tasks, 0)
                .unwrap()
                .checked_sub(small.process_task_storage(tasks, 0).unwrap())
                .unwrap();
            assert_eq!(
                delta, extra_pool,
                "the same actual request pool, including source Work/report, is funded once"
            );
        }
    }

    #[test]
    fn loading_pays_one_open_notification_without_a_second_work_or_cut() {
        let policy = ControlRuntimePolicy::try_new(ingress()).unwrap();
        // InputLoaded and LoaderExited each own prefix/event positions. The
        // Task also owns one independent coalesced open-summary position.
        assert_eq!(policy.loading.inputs, 5);
        assert_eq!(policy.loading.batches, 5);
        assert_eq!(policy.loading.effects, 5 * ContextCore::MAX_STEP_WORK);
        assert_eq!(policy.loading.cuts, 2);
        assert_eq!(policy.loading.work, 11);
        assert_eq!(policy.loading.completions, 10);
        assert_eq!(policy.loading.captures, 10);
        assert_eq!(
            policy.loading.checked_add(policy.accepted).unwrap(),
            policy.task_base
        );
        assert_eq!(policy.accepted.inputs, 64);
    }

    #[test]
    fn invalid_or_overflowing_geometry_is_rejected_before_admission() {
        let mut limits = ingress();
        limits.max_batch_items = 0;
        assert_eq!(
            ControlRuntimePolicy::try_new(limits),
            Err(PolicyError::ZeroBatchItems)
        );
        limits.max_batch_items = usize::MAX;
        assert_eq!(
            ControlRuntimePolicy::try_new(limits),
            Err(PolicyError::Overflow)
        );
        let policy = ControlRuntimePolicy::try_new(ingress()).unwrap();
        assert_eq!(
            policy.process_task_storage(usize::MAX, 0),
            Err(PolicyError::Overflow)
        );
        assert!(policy.context_actor(usize::MAX).is_err());
        assert!(policy.context_actor(0).is_err());
    }
}
