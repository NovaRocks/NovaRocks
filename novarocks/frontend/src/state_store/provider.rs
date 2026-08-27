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

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_spi::state_store::{
    StateStoreLimits, StateStoreProviderDescriptor, StateStoreProviderFactory, StateStoreProviderId,
};

use super::host_error::{StateStoreHostError, StateStoreHostErrorKind};

/// Provider-neutral StateStore opening facts resolved by Server composition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateStoreHostInput {
    pub cluster_id: String,
    pub provider_id: StateStoreProviderId,
    pub limits: StateStoreLimits,
}

/// A provider factory builder supplied by the concrete composition root.
///
/// Design: ADR-0093 keeps this registry in Frontend while Server owns concrete
/// provider selection and leaf crates own native construction.
pub type StateStoreProviderBinder = Arc<
    dyn Fn(&StateStoreHostInput) -> Result<Box<dyn StateStoreProviderFactory>, StateStoreHostError>
        + Send
        + Sync,
>;

#[derive(Clone)]
pub struct StateStoreProviderRegistration {
    descriptor: StateStoreProviderDescriptor,
    binder: StateStoreProviderBinder,
}

impl StateStoreProviderRegistration {
    pub fn new<F>(descriptor: StateStoreProviderDescriptor, binder: F) -> Self
    where
        F: Fn(
                &StateStoreHostInput,
            ) -> Result<Box<dyn StateStoreProviderFactory>, StateStoreHostError>
            + Send
            + Sync
            + 'static,
    {
        Self {
            descriptor,
            binder: Arc::new(binder),
        }
    }

    pub fn unavailable(
        descriptor: StateStoreProviderDescriptor,
        reason: impl Into<String>,
    ) -> Self {
        let reason = reason.into();
        Self::new(descriptor, move |_| {
            Err(StateStoreHostError::new(
                StateStoreHostErrorKind::ProviderNotCompiled,
                Some(descriptor.id),
                reason.clone(),
            ))
        })
    }
}

#[derive(Clone, Default)]
pub struct StateStoreProviderRegistry {
    registrations: BTreeMap<StateStoreProviderId, StateStoreProviderRegistration>,
}

pub(crate) struct BoundStateStoreProvider {
    pub factory: Box<dyn StateStoreProviderFactory>,
    pub descriptor: StateStoreProviderDescriptor,
}

impl StateStoreProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        registration: StateStoreProviderRegistration,
    ) -> Result<(), StateStoreHostError> {
        let provider_id = registration.descriptor.id;
        if self.registrations.contains_key(&provider_id) {
            return Err(StateStoreHostError::new(
                StateStoreHostErrorKind::DuplicateProvider,
                Some(provider_id),
                "state store provider is already registered",
            ));
        }
        self.registrations.insert(provider_id, registration);
        Ok(())
    }

    pub(crate) fn bind(
        &self,
        input: &StateStoreHostInput,
    ) -> Result<BoundStateStoreProvider, StateStoreHostError> {
        let provider_id = input.provider_id;
        let Some(registration) = self.registrations.get(&provider_id) else {
            return Err(StateStoreHostError::new(
                StateStoreHostErrorKind::ProviderNotRegistered,
                Some(provider_id),
                "state store provider is not registered",
            ));
        };
        if input.cluster_id.trim().is_empty()
            || input.limits.max_key_bytes == 0
            || input.limits.max_key_bytes > registration.descriptor.max_key_bytes
        {
            return Err(StateStoreHostError::new(
                StateStoreHostErrorKind::InvalidConfiguration,
                Some(provider_id),
                "Server supplied invalid StateStore opening facts",
            ));
        }
        let factory = (registration.binder)(input)?;
        if factory.descriptor() != &registration.descriptor {
            return Err(StateStoreHostError::new(
                StateStoreHostErrorKind::DescriptorMismatch,
                Some(provider_id),
                "state store provider factory descriptor does not match registration",
            ));
        }
        Ok(BoundStateStoreProvider {
            factory,
            descriptor: registration.descriptor,
        })
    }
}
