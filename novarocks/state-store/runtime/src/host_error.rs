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

use std::fmt;

use novarocks_state_store_api::{StateStoreError, StateStoreProviderId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateStoreHostErrorKind {
    InvalidConfiguration,
    DuplicateProvider,
    ProviderNotRegistered,
    ProviderNotCompiled,
    DescriptorMismatch,
    Bind,
    Open,
    Shutdown,
    ShutdownDeadlineExceeded,
}

#[derive(Clone, Debug)]
pub struct StateStoreHostError {
    kind: StateStoreHostErrorKind,
    provider_id: Option<StateStoreProviderId>,
    message: String,
    primary: Option<StateStoreError>,
    cleanup: Option<StateStoreError>,
    original: Option<novarocks_state_store_api::StateStoreExecutionFailure>,
    original_cleanup: Option<novarocks_state_store_api::StateStoreExecutionFailure>,
}

impl StateStoreHostError {
    pub fn new(
        kind: StateStoreHostErrorKind,
        provider_id: Option<StateStoreProviderId>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            provider_id,
            message: message.into(),
            primary: None,
            cleanup: None,
            original: None,
            original_cleanup: None,
        }
    }

    pub(crate) fn provider_failure(
        kind: StateStoreHostErrorKind,
        provider_id: StateStoreProviderId,
        message: impl Into<String>,
        primary: StateStoreError,
    ) -> Self {
        Self {
            kind,
            provider_id: Some(provider_id),
            message: message.into(),
            primary: Some(primary),
            cleanup: None,
            original: None,
            original_cleanup: None,
        }
    }

    pub fn from_original_failure(
        kind: StateStoreHostErrorKind,
        provider_id: StateStoreProviderId,
        message: &'static str,
        original: novarocks_state_store_api::StateStoreExecutionFailure,
    ) -> Self {
        let mut error = Self::new(kind, Some(provider_id), message);
        error.original = Some(original);
        error
    }

    pub fn original_failure(
        &self,
    ) -> Option<&novarocks_state_store_api::StateStoreExecutionFailure> {
        self.original.as_ref()
    }

    pub fn original_cleanup_failure(
        &self,
    ) -> Option<&novarocks_state_store_api::StateStoreExecutionFailure> {
        self.original_cleanup.as_ref()
    }

    pub fn with_original_cleanup(
        mut self,
        cleanup: novarocks_state_store_api::StateStoreExecutionFailure,
    ) -> Self {
        self.original_cleanup = Some(cleanup);
        self
    }

    pub(crate) fn retain_original_primary(mut self) -> Self {
        if self.original.is_none() {
            self.original = self
                .primary
                .take()
                .map(StateStoreError::into_original_failure);
        }
        self
    }

    pub(crate) fn with_cleanup(mut self, cleanup: StateStoreError) -> Self {
        self.cleanup = Some(cleanup);
        self
    }

    pub const fn kind(&self) -> StateStoreHostErrorKind {
        self.kind
    }

    pub const fn provider_id(&self) -> Option<StateStoreProviderId> {
        self.provider_id
    }

    pub fn primary(&self) -> Option<&StateStoreError> {
        self.primary.as_ref()
    }

    pub fn cleanup(&self) -> Option<&StateStoreError> {
        self.cleanup.as_ref()
    }
}

impl fmt::Display for StateStoreHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}", self.kind)?;
        if let Some(provider_id) = self.provider_id {
            write!(formatter, " ({provider_id})")?;
        }
        write!(formatter, ": {}", self.message)?;
        if let Some(primary) = &self.original {
            write!(formatter, "; primary: {primary}")?;
        }
        if let Some(cleanup) = &self.original_cleanup {
            write!(formatter, "; cleanup failed: {cleanup}")?;
        }
        if let Some(primary) = &self.primary {
            write!(formatter, "; primary: {primary}")?;
        }
        if let Some(cleanup) = &self.cleanup {
            write!(formatter, "; cleanup failed: {cleanup}")?;
        }
        Ok(())
    }
}

impl std::error::Error for StateStoreHostError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.original
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
            .or_else(|| {
                self.primary
                    .as_ref()
                    .map(|primary| primary as &(dyn std::error::Error + 'static))
                    .or_else(|| {
                        self.original_cleanup
                            .as_ref()
                            .map(|source| source as &(dyn std::error::Error + 'static))
                    })
                    .or_else(|| {
                        self.cleanup
                            .as_ref()
                            .map(|cleanup| cleanup as &(dyn std::error::Error + 'static))
                    })
            })
    }
}
