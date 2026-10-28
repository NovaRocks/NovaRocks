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

//! The original Context pays for M, R and their opaque resource holder before
//! any host capture. M and R have independent callback positions; withdrawing
//! a never-issued inventory is different from observing an actual host exit.

use super::{
    actors::WorkerActor,
    admitted_role::{dispose_unused_completion, reserve_reported_role},
};
use crate::{control_runtime_policy::StorageNeed, task_completion_roles::AdmittedCompletionRole};
use novarocks_control_runtime::{
    driver::{ActorRuntime, DriverError, InputPermit, InputRole},
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::{
    identity::{ActorId, Generation, ReservationId},
    mailbox::InputClass,
    scope::WorkKind,
};
use novarocks_worker_protocol::context::ContextCore;

pub(crate) const CONTEXT_IO_ROLES: usize = 3;
pub(crate) const CONTEXT_IO_CALLBACKS: usize = 2;
/// Fixed capture containers, rather than one credit per Arc alias. The holder
/// keeps the actual instance, credential and retained host sources separately
/// from each call's body/report. U has not been included in this inventory.
pub(crate) const CONTEXT_IO_STORAGE: StorageNeed = StorageNeed {
    requests: 0,
    inputs: CONTEXT_IO_CALLBACKS,
    batches: CONTEXT_IO_CALLBACKS,
    effects: CONTEXT_IO_CALLBACKS * ContextCore::MAX_STEP_WORK,
    work: CONTEXT_IO_ROLES,
    completions: CONTEXT_IO_ROLES,
    captures: CONTEXT_IO_ROLES,
    cuts: 0,
    replies: 0,
};

#[derive(Debug)]
pub(crate) enum ContextIoError {
    Runtime(RuntimeError),
    Driver(DriverError),
    AlreadyAdmitted,
}

/// These are actual original Work/report/input owners. There are no replica
/// counters, inferred physical exits or opaque fields in this pre-capture
/// inventory. Its zero-resource children use the original Context's prepaid
/// bundle, which remains charged through all child/report retirement.
pub(crate) struct ContextIoInventory {
    roles: Vec<AdmittedCompletionRole>,
    inputs: Vec<InputRole<WorkerActor>>,
    permits: Vec<InputPermit<WorkerActor>>,
}
/// Single ownership transfer into the actual host-call custodian. The holder
/// has no protocol callback and therefore declares only its host observer;
/// neither M nor R can consume its physical-exit report on behalf of Core.
pub(crate) struct ContextIoParts {
    pub materialization: AdmittedCompletionRole,
    pub release: AdmittedCompletionRole,
    pub holder: AdmittedCompletionRole,
    pub materialization_input: InputRole<WorkerActor>,
    pub release_input: InputRole<WorkerActor>,
}
impl ContextIoInventory {
    pub(super) fn admit(
        runtime: &ActorRuntime<WorkerActor>,
        actor: ActorId,
        scope: &ScopeHandle,
        parent: ReservationId,
        generation: Generation,
    ) -> Result<Self, ContextIoError> {
        let mut inventory = Self {
            roles: Vec::with_capacity(CONTEXT_IO_ROLES),
            inputs: Vec::with_capacity(CONTEXT_IO_CALLBACKS),
            permits: Vec::with_capacity(CONTEXT_IO_CALLBACKS),
        };
        let result = (|| {
            for (index, kind) in [WorkKind::Business, WorkKind::Cleanup, WorkKind::Cleanup]
                .into_iter()
                .enumerate()
            {
                inventory.roles.push(
                    reserve_reported_role(
                        scope,
                        parent,
                        kind,
                        (index < CONTEXT_IO_CALLBACKS).then_some(generation),
                    )
                    .map_err(ContextIoError::Runtime)?,
                );
            }
            for role in inventory.roles.iter().take(CONTEXT_IO_CALLBACKS) {
                let permit = runtime
                    .reserve_admitted_input(actor, actor, InputClass::Control, &role.work)
                    .map_err(ContextIoError::Driver)?;
                match permit.into_role() {
                    Ok(input) => inventory.inputs.push(input),
                    Err((error, permit)) => {
                        inventory.permits.push(permit);
                        return Err(ContextIoError::Driver(error));
                    }
                }
            }
            Ok(())
        })();
        if let Err(error) = result {
            // No source, host instance or job was captured. These exact unused
            // originals can be withdrawn; their real ready Work headers still
            // belong to the common scope reaper. No Context fee is refunded.
            inventory.withdraw_unused();
            return Err(error);
        }
        Ok(inventory)
    }

    pub(crate) fn into_parts(self) -> ContextIoParts {
        let Self {
            roles,
            inputs,
            permits,
        } = self;
        assert!(
            permits.is_empty(),
            "only complete Context IO admission transfers"
        );
        let [materialization, release, holder] = roles
            .try_into()
            .unwrap_or_else(|_| unreachable!("three original Context IO roles"));
        let [materialization_input, release_input] = inputs
            .try_into()
            .unwrap_or_else(|_| unreachable!("two original Context IO callbacks"));
        ContextIoParts {
            materialization,
            release,
            holder,
            materialization_input,
            release_input,
        }
    }

    /// This type has not handed out a call authority or captured any payload.
    /// Issued host jobs will consume a different custody type, which cannot
    /// invoke this unused-only path.
    pub(super) fn withdraw_unused(self) {
        let Self {
            roles,
            mut inputs,
            permits,
        } = self;
        for role in roles {
            dispose_unused_completion(role);
        }
        for input in &mut inputs {
            input
                .close_after_exit()
                .expect("original unused Context callback");
        }
        for permit in permits {
            permit
                .revoke_unused()
                .expect("original unused Context input reservation");
        }
    }
}
