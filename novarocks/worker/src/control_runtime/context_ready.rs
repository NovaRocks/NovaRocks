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

//! An original Context's host wake is only registration-qualified metadata.
//! Common owns its one host ready queue; there is no Context scheduler here.

use super::actors::WorkerActor;
use novarocks_control_runtime::{
    driver::{ActorRegistration, ActorRuntime, DriverError},
    transaction::{RuntimeError, ScopeHandle},
};
use novarocks_control_runtime_core::identity::ScopeId;
use novarocks_execution_contract::QueryContextRef;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ContextReadyError {
    Duplicate,
    Driver(DriverError),
    Runtime(RuntimeError),
}
struct Endpoint {
    runtime: ActorRuntime<WorkerActor>,
    context: QueryContextRef,
    scope: ScopeHandle,
    registration: ActorRegistration,
}
/// Clones retain only the original address. They grant no Work, capacity,
/// publication, physical exit, or a second host dispatch claim.
#[derive(Clone)]
pub(crate) struct ContextReadyNotice(Arc<Endpoint>);
impl ContextReadyNotice {
    pub(super) fn for_registered(
        runtime: &ActorRuntime<WorkerActor>,
        context: QueryContextRef,
        scope: &ScopeHandle,
        registration: &ActorRegistration,
    ) -> Result<Self, ContextReadyError> {
        if !registration.matches_scope(scope) {
            return Err(ContextReadyError::Driver(
                DriverError::EffectTicketOriginMismatch,
            ));
        }
        runtime
            .validate_registration(registration)
            .map_err(ContextReadyError::Driver)?;
        Ok(Self(Arc::new(Endpoint {
            runtime: runtime.clone(),
            context,
            scope: scope.clone(),
            registration: registration.clone(),
        })))
    }
    pub(crate) fn same_origin(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub(crate) fn context(&self) -> QueryContextRef {
        self.0.context
    }
    pub(crate) fn scope(&self) -> ScopeId {
        self.0.scope.id()
    }
    pub(super) fn notify(&self) {
        match self.0.runtime.notify_host_actor(&self.0.registration) {
            Ok(()) | Err(DriverError::UnknownActor | DriverError::EffectTicketOriginMismatch) => {}
            Err(_) => std::process::abort(),
        }
    }
    /// Actual Scope removal closes this source's useful lifetime. This check
    /// never retires Work, revokes another producer, or refunds any fee.
    pub(super) fn unregister_removed(&self) -> Result<(), ContextReadyError> {
        match self.0.scope.gates() {
            Err(RuntimeError::UnknownScope) => Ok(()),
            Ok(_) => Err(ContextReadyError::Runtime(RuntimeError::ScopeNotRetired)),
            Err(error) => Err(ContextReadyError::Runtime(error)),
        }
    }
}
