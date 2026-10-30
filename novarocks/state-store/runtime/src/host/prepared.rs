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

//! Stable provider-neutral opening custody. It owns no executor or Root.

use super::*;
use novarocks_state_store_api::{
    StateStoreBlockingSourceVocabulary, StateStoreExecutionContext, StateStoreExecutionFailure,
    StateStoreOriginalOpen, StateStoreOriginalUnsupported, StateStoreProviderDescriptor,
    StateStoreProviderFactory,
};
use std::task::{Context, Poll};

pub struct PreparedStateStoreHost {
    descriptor: StateStoreProviderDescriptor,
    run_policy: StateStoreRunPolicy,
    vocabulary: StateStoreBlockingSourceVocabulary,
    dependencies: Vec<novarocks_state_store_api::SharedDependency>,
    request: StateStoreOpenRequest,
    factory: Option<Box<dyn StateStoreProviderFactory>>,
    original: Option<Box<dyn StateStoreOriginalOpen>>,
    instance: Option<Box<dyn StateStoreProviderInstance>>,
    first_failure: Option<StateStoreExecutionFailure>,
    validation_failure: Option<StateStoreHostError>,
    started: bool,
    closing: bool,
    ready: bool,
    transferred: bool,
}
impl PreparedStateStoreHost {
    pub(super) fn new(
        registry: &StateStoreProviderRegistry,
        input: StateStoreHostInput,
        deadline: Instant,
    ) -> Result<Self, StateStoreHostError> {
        let bound = registry.bind(&input)?;
        let vocabulary = bound.factory.blocking_source_vocabulary().ok_or_else(|| {
            StateStoreHostError::from_original_failure(
                StateStoreHostErrorKind::Open,
                input.provider_id,
                "selected provider does not supply original opening",
                StateStoreOriginalUnsupported.into_original_failure(),
            )
        })?;
        let dependencies = canonical_blocking_facts(bound.factory.blocking_dependency_facts());
        validate_selected_original_facts(
            bound.descriptor,
            Some(vocabulary),
            dependencies.as_deref(),
        )
        .map_err(StateStoreHostError::retain_original_primary)?;
        let dependencies = dependencies.expect("validated original dependency facts");
        if vocabulary.provider() != bound.descriptor.id {
            return Err(StateStoreHostError::provider_failure(
                StateStoreHostErrorKind::DescriptorMismatch,
                input.provider_id,
                "selected provider original vocabulary has another provider identity",
                StateStoreError::new(
                    StateStoreErrorKind::Internal,
                    "state store factory original vocabulary provider mismatch",
                ),
            )
            .retain_original_primary());
        }
        Ok(Self {
            descriptor: bound.descriptor,
            run_policy: input.run_policy,
            vocabulary,
            dependencies,
            request: StateStoreOpenRequest {
                cluster_id: input.cluster_id,
                limits: input.limits,
                deadline,
            },
            factory: Some(bound.factory),
            original: None,
            instance: None,
            first_failure: None,
            validation_failure: None,
            started: false,
            closing: false,
            ready: false,
            transferred: false,
        })
    }
    pub fn provider_id(&self) -> StateStoreProviderId {
        self.descriptor.id
    }
    pub fn vocabulary(&self) -> StateStoreBlockingSourceVocabulary {
        self.vocabulary
    }
    pub fn original_blocking_dependency_facts(
        &self,
    ) -> &[novarocks_state_store_api::SharedDependency] {
        &self.dependencies
    }
    pub fn run_policy(&self) -> StateStoreRunPolicy {
        self.run_policy
    }
    pub fn deadline(&self) -> Instant {
        self.request.deadline
    }
    pub fn original_failure(&self) -> Option<&StateStoreExecutionFailure> {
        self.first_failure.as_ref()
    }
    pub fn has_retained_instance(&self) -> bool {
        self.instance.is_some()
    }
    /// Borrow the actual instance for a selected purpose-closed lifecycle
    /// author. This does not authorize legacy shutdown or any physical I/O.
    pub fn retained_instance_mut(
        &mut self,
    ) -> Option<&mut (dyn StateStoreProviderInstance + 'static)> {
        self.instance.as_deref_mut()
    }
    pub fn start_in(
        &mut self,
        execution: &StateStoreExecutionContext,
    ) -> Result<(), StateStoreExecutionFailure> {
        if let Some(failure) = &self.first_failure {
            return Err(failure.clone());
        }
        if self.started
            || self.closing
            || self.transferred
            || execution.deadline() != self.request.deadline
        {
            return Err(StateStoreError::new(
                StateStoreErrorKind::InvalidRequest,
                "prepared state store opening is not accepting this start",
            )
            .into_original_failure());
        }
        self.started = true;
        let mut result = self
            .factory
            .take()
            .expect("one selected original factory")
            .prepare_open_in(self.request.clone(), execution, &mut self.original);
        if result.is_ok() && self.original.is_none() {
            result = Err(StateStoreError::new(
                StateStoreErrorKind::Internal,
                "provider returned no original opening receiver",
            )
            .into_original_failure());
        }
        if let Err(failure) = &result {
            self.first_failure = Some(failure.clone());
        }
        result
    }
    /// Only borrows the provider receiver. The enclosing role must continue
    /// polling its same physical owner; this object cannot create progress.
    pub fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), StateStoreExecutionFailure>> {
        if let Some(failure) = &self.first_failure {
            return Poll::Ready(Err(failure.clone()));
        }
        if self.closing || !self.started || self.transferred {
            return Poll::Ready(Err(StateStoreError::new(
                StateStoreErrorKind::InvalidRequest,
                "prepared state store opening is not waiting for readiness",
            )
            .into_original_failure()));
        }
        let result = self
            .original
            .as_mut()
            .expect("successful original source installation")
            .poll_ready(cx);
        match &result {
            Poll::Ready(Ok(())) => self.ready = true,
            Poll::Ready(Err(failure)) => self.first_failure = Some(failure.clone()),
            Poll::Pending => {}
        }
        result
    }
    /// The actual instance receiving is a stable field of this original owner.
    /// Validation failure preserves it; no unique instance is moved into an F.
    pub fn install_ready_in(
        &mut self,
        target: &mut Option<StateStoreHost>,
    ) -> Result<(), StateStoreHostError> {
        if target.is_some() {
            return Err(StateStoreHostError::new(
                StateStoreHostErrorKind::Open,
                Some(self.provider_id()),
                "state store Host receiving slot is occupied",
            ));
        }
        if let Some(failure) = &self.validation_failure {
            return Err(failure.clone());
        }
        if let Some(failure) = &self.first_failure {
            return Err(StateStoreHostError::from_original_failure(
                StateStoreHostErrorKind::Open,
                self.provider_id(),
                "original state store opening failed",
                failure.clone(),
            ));
        }
        if self.closing || !self.ready || self.transferred {
            return Err(StateStoreHostError::new(
                StateStoreHostErrorKind::Open,
                Some(self.provider_id()),
                "prepared state store opening is not ready for handoff",
            ));
        }
        if self.instance.is_none()
            && let Err(failure) = self
                .original
                .as_mut()
                .expect("original source remains installed")
                .take_instance_in(&mut self.instance)
        {
            self.first_failure = Some(failure.clone());
            return Err(StateStoreHostError::from_original_failure(
                StateStoreHostErrorKind::Open,
                self.provider_id(),
                "original instance handoff failed",
                failure,
            ));
        }
        if let Err(failure) = validate_original_instance(
            self.descriptor,
            Some(self.vocabulary),
            Some(&self.dependencies),
            self.instance.as_ref().unwrap().as_ref(),
        ) {
            let failure = failure.retain_original_primary();
            self.first_failure = failure.original_failure().cloned();
            self.validation_failure = Some(failure.clone());
            return Err(failure);
        }
        let original_dependencies = Some(self.dependencies.clone());
        let instance = self.instance.as_ref().unwrap();
        let original_sources = Some((
            self.vocabulary,
            instance
                .blocking_instance_scope()
                .expect("validated exact instance scope"),
        ));
        let state_store = instance.state_store().expect("validated provider exposure");
        *target = Some(StateStoreHost {
            provider_id: self.provider_id(),
            lifecycle: StateStoreHostLifecycle::Ready,
            state_store: Some(state_store),
            run_policy: self.run_policy,
            instance: self.instance.take().unwrap(),
            original_sources,
            original_dependencies,
        });
        self.transferred = true;
        drop(self.original.take());
        Ok(())
    }
    /// The actual exit observer belongs to the enclosing FA/role owner. It
    /// must poll that owner's same receipts before releasing this custody.
    /// A received instance stays here for its own original close author.
    pub fn request_close(&mut self) {
        self.closing = true;
        drop(self.original.take());
        drop(self.factory.take());
    }
}

#[cfg(test)]
#[path = "prepared_tests.rs"]
mod tests;
