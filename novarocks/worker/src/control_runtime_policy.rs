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
    pub fn role(reply: bool) -> Self {
        Self {
            inputs: crate::task_causal_input::CAUSAL_INPUT_POSITIONS,
            batches: crate::task_causal_input::CAUSAL_INPUT_POSITIONS,
            effects: ContextCore::MAX_STEP_WORK * crate::task_causal_input::CAUSAL_INPUT_POSITIONS,
            cuts: 1,
            replies: usize::from(reply),
            completions: 1,
            captures: 3,
            work: 1,
        }
    }
}

/// Fixed Task inputs are actual one-shot lifecycle roles, not RPC op counts.
/// A role with several observations uses its existing report position and a
/// coalesced pending flag; it cannot enqueue an unbounded stream of copies.
#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub enum TaskLifecycleRole {
    InputLoaded,
    LoaderExited,
    PreparationBodyExited,
    PreparationExited,
    Installed,
    Running,
    Flushing,
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
        let mut task_base = StorageNeed::role(false).checked_mul(fixed_roles)?;
        // Protocol lifecycle roles are bounded input authors. The original
        // Task lifetime is a separate parent, not another notification author.
        task_base.work = task_base.work.checked_add(1).ok_or(PolicyError::Overflow)?;
        let mut loading = StorageNeed::role(false).checked_mul(TaskLifecycleRole::LOADING.len())?;
        loading.work = crate::control_runtime::loading::LOADING_WORK_POSITIONS;
        loading.completions = crate::control_runtime::loading::LOADING_REPORT_POSITIONS;
        // The open and consuming summary each have two candidate positions.
        task_base.captures = task_base
            .captures
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
        let lifecycle = self
            .task_base
            .inputs
            .checked_add(self.root_read_pool_per_root.inputs)
            .and_then(|n| n.checked_mul(max_live_tasks))
            .and_then(|n| n.checked_add(self.request_roles))
            .ok_or(PolicyError::Overflow)?;
        let ordinary = self.request_roles;
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
            (policy.task_base.inputs + 4) * 9 + policy.request_roles
        );
        assert_eq!(actor.mailbox[1], policy.request_roles);
        assert_eq!(actor.mailbox[2], policy.request_roles);
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
        let root = policy.process_task_storage(2, 1).unwrap();
        assert_eq!(root.cuts - two.cuts, 2);
        assert_eq!(
            policy.max_task_cuts,
            policy.task_base.cuts + policy.request_roles + 2
        );
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
