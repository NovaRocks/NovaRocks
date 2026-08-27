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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum StateStoreErrorKind {
    InvalidRequest,
    InvalidConfiguration,
    UnsupportedFormat,
    LimitExceeded,
    DeadlineExceeded,
    PreconditionFailed,
    Conflict,
    Transient,
    Corruption,
    ProviderUnavailable,
    Cancelled,
    Internal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StateStoreError {
    kind: StateStoreErrorKind,
    message: &'static str,
    cleanup: Option<Box<StateStoreError>>,
}

impl StateStoreError {
    pub const fn new(kind: StateStoreErrorKind, message: &'static str) -> Self {
        Self {
            kind,
            message,
            cleanup: None,
        }
    }

    pub const fn kind(&self) -> StateStoreErrorKind {
        self.kind
    }

    pub fn with_cleanup_context(mut self, cleanup: StateStoreError) -> Self {
        self.cleanup = Some(Box::new(cleanup));
        self
    }

    pub fn cleanup_context(&self) -> Option<&StateStoreError> {
        self.cleanup.as_deref()
    }
}

impl fmt::Display for StateStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)?;
        if let Some(cleanup) = &self.cleanup {
            write!(formatter, "; cleanup failed: {cleanup}")?;
        }
        Ok(())
    }
}

impl std::error::Error for StateStoreError {}
