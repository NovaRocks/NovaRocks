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

use std::sync::Arc;
use std::time::Instant;

use novarocks_spi::state_store::{
    StateStore, StateStoreError, StateStoreErrorKind, StateStoreOpenRequest, StateStoreProviderId,
    StateStoreProviderInstance, StateStoreProviderLifecycle,
};

use super::host_error::{StateStoreHostError, StateStoreHostErrorKind};
use super::provider::{StateStoreHostInput, StateStoreProviderRegistry};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateStoreHostLifecycle {
    Ready,
    Draining,
    Stopped,
}

pub struct StateStoreHost {
    provider_id: StateStoreProviderId,
    lifecycle: StateStoreHostLifecycle,
    state_store: Option<Arc<dyn StateStore>>,
    instance: Box<dyn StateStoreProviderInstance>,
}

impl StateStoreHost {
    pub async fn open(
        registry: &StateStoreProviderRegistry,
        input: StateStoreHostInput,
        deadline: Instant,
    ) -> Result<Self, StateStoreHostError> {
        let provider_id = input.provider_id;
        let bound = registry.bind(&input)?;
        let request = StateStoreOpenRequest {
            cluster_id: input.cluster_id,
            limits: input.limits,
            deadline,
        };
        let mut instance = bound.factory.open(request).await.map_err(|error| {
            StateStoreHostError::provider_failure(
                StateStoreHostErrorKind::Open,
                provider_id,
                "state store provider failed to open",
                error,
            )
        })?;

        let validation = validate_open_instance(bound.descriptor, instance.as_ref());
        if let Err(error) = validation {
            return match instance.shutdown(deadline).await {
                Ok(()) => Err(error),
                Err(cleanup) => Err(error.with_cleanup(cleanup)),
            };
        }
        let state_store = instance.state_store().expect("validated provider exposure");
        Ok(Self {
            provider_id,
            lifecycle: StateStoreHostLifecycle::Ready,
            state_store: Some(state_store),
            instance,
        })
    }

    pub const fn provider_id(&self) -> StateStoreProviderId {
        self.provider_id
    }

    pub const fn lifecycle(&self) -> StateStoreHostLifecycle {
        self.lifecycle
    }

    pub fn state_store(&self) -> Option<Arc<dyn StateStore>> {
        self.state_store.clone()
    }

    pub async fn shutdown(&mut self, deadline: Instant) -> Result<(), StateStoreHostError> {
        if self.lifecycle == StateStoreHostLifecycle::Stopped {
            return Ok(());
        }
        self.lifecycle = StateStoreHostLifecycle::Draining;
        self.state_store.take();
        match self.instance.shutdown(deadline).await {
            Ok(()) => {
                self.lifecycle = StateStoreHostLifecycle::Stopped;
                Ok(())
            }
            Err(error) => {
                let kind = if error.kind() == StateStoreErrorKind::DeadlineExceeded {
                    StateStoreHostErrorKind::ShutdownDeadlineExceeded
                } else {
                    StateStoreHostErrorKind::Shutdown
                };
                Err(StateStoreHostError::provider_failure(
                    kind,
                    self.provider_id,
                    "state store provider failed to shut down",
                    error,
                ))
            }
        }
    }
}

fn validate_open_instance(
    descriptor: novarocks_spi::state_store::StateStoreProviderDescriptor,
    instance: &dyn StateStoreProviderInstance,
) -> Result<(), StateStoreHostError> {
    let provider_id = descriptor.id;
    if instance.descriptor() != &descriptor {
        return Err(StateStoreHostError::provider_failure(
            StateStoreHostErrorKind::DescriptorMismatch,
            provider_id,
            "state store provider instance descriptor does not match the selected provider",
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "state store provider instance descriptor mismatch",
            ),
        ));
    }
    if instance.lifecycle() != StateStoreProviderLifecycle::Ready {
        return Err(StateStoreHostError::provider_failure(
            StateStoreHostErrorKind::Open,
            provider_id,
            "state store provider instance did not become ready",
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "state store provider instance is not ready",
            ),
        ));
    }
    if instance.state_store().is_none() {
        return Err(StateStoreHostError::provider_failure(
            StateStoreHostErrorKind::Open,
            provider_id,
            "state store provider instance did not expose a store",
            StateStoreError::new(
                StateStoreErrorKind::Internal,
                "state store provider instance has no store",
            ),
        ));
    }
    Ok(())
}
