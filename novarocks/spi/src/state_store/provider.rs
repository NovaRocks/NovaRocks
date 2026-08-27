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
use std::sync::Arc;
use std::time::Instant;

use super::{MAX_KEY_BYTES, StateStore, StateStoreError, StateStoreLimits};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StateStoreProviderId(&'static str);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StateStoreProviderIdError;

impl StateStoreProviderId {
    pub const fn try_new(value: &'static str) -> Result<Self, StateStoreProviderIdError> {
        if is_valid_provider_id(value) {
            Ok(Self(value))
        } else {
            Err(StateStoreProviderIdError)
        }
    }

    pub const fn new(value: &'static str) -> Self {
        match Self::try_new(value) {
            Ok(id) => id,
            Err(_) => panic!("invalid state store provider id"),
        }
    }

    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

const fn is_valid_provider_id(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.is_empty() {
        return false;
    }

    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        let is_ascii_lowercase = byte >= b'a' && byte <= b'z';
        let is_ascii_digit = byte >= b'0' && byte <= b'9';
        let is_separator = byte == b'-';
        if !is_ascii_lowercase && !is_ascii_digit && !is_separator {
            return false;
        }
        if is_separator && (index == 0 || index + 1 == bytes.len() || bytes[index - 1] == b'-') {
            return false;
        }
        index += 1;
    }
    true
}

impl fmt::Display for StateStoreProviderId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl fmt::Display for StateStoreProviderIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid state store provider id")
    }
}

impl std::error::Error for StateStoreProviderIdError {}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StateStoreProviderDescriptor {
    pub id: StateStoreProviderId,
    pub max_key_bytes: usize,
}

impl StateStoreProviderDescriptor {
    pub const fn new(id: StateStoreProviderId, max_key_bytes: usize) -> Self {
        assert!(max_key_bytes > 0 && max_key_bytes <= MAX_KEY_BYTES);
        Self { id, max_key_bytes }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateStoreOpenRequest {
    pub cluster_id: String,
    pub limits: StateStoreLimits,
    pub deadline: Instant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StateStoreProviderLifecycle {
    Ready,
    Draining,
    Stopped,
}

#[async_trait::async_trait]
pub trait StateStoreProviderFactory: Send + Sync {
    fn descriptor(&self) -> &StateStoreProviderDescriptor;

    async fn open(
        self: Box<Self>,
        request: StateStoreOpenRequest,
    ) -> Result<Box<dyn StateStoreProviderInstance>, StateStoreError>;
}

#[async_trait::async_trait]
pub trait StateStoreProviderInstance: Send {
    fn descriptor(&self) -> &StateStoreProviderDescriptor;
    fn lifecycle(&self) -> StateStoreProviderLifecycle;
    fn state_store(&self) -> Option<Arc<dyn StateStore>>;

    async fn shutdown(&mut self, deadline: Instant) -> Result<(), StateStoreError>;
}
