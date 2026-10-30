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

//! An existing owner's original retained-record pin, held by value.
//! No new holder allocation, capacity authority, or arbitrary closure is created.
use std::{fmt, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetentionPinError {
    Released,
    Conflict,
    Overflow,
}
impl fmt::Display for RetentionPinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Released => "retention pin is released",
            Self::Conflict => "retention pin conflicts with its original record",
            Self::Overflow => "retention pin count overflow",
        })
    }
}
impl std::error::Error for RetentionPinError {}

/// Implemented by an already-existing original owner, never a newly allocated
/// per-lease adapter. Keys reach only that owner, not lease consumers.
/// Retain must be a checked callback-free transaction. Release commits its pin
/// transition before any original lock-free progress notification.
pub trait RetentionOwner: Send + Sync + std::panic::RefUnwindSafe + 'static {
    fn try_retain(&self, key: [u64; 2]) -> Result<(), RetentionPinError>;
    fn release_pin(&self, key: [u64; 2]);
    fn capacity_bytes(&self, key: [u64; 2]) -> Result<u64, RetentionPinError>;
}

/// A pin to one original paid record. Owner and key are deliberately private;
/// no accessor, numeric lookup, arbitrary-release or source API is provided.
pub struct ExistingOwnerRetentionLease {
    owner: Arc<dyn RetentionOwner>,
    key: Option<[u64; 2]>,
}
impl ExistingOwnerRetentionLease {
    /// Original-author construction only: the caller has already installed
    /// this exact one pin in its stable owner transaction. This function does
    /// not acquire another pin. Possessing numbers without that original owner
    /// Arc cannot construct that owner's capability. Never expose the original
    /// owner Arc/key through another public API.
    pub fn from_existing_pin(owner: Arc<dyn RetentionOwner>, key: [u64; 2]) -> Self {
        Self {
            owner,
            key: Some(key),
        }
    }
    pub fn try_clone(&self) -> Result<Self, RetentionPinError> {
        let key = self.key.ok_or(RetentionPinError::Released)?;
        let owner = Arc::clone(&self.owner);
        owner.try_retain(key)?;
        Ok(Self {
            owner,
            key: Some(key),
        })
    }
    pub fn capacity_bytes(&self) -> Result<u64, RetentionPinError> {
        self.owner
            .capacity_bytes(self.key.ok_or(RetentionPinError::Released)?)
    }
    pub fn shares_record_with(&self, other: &Self) -> bool {
        self.key.is_some() && self.key == other.key && Arc::ptr_eq(&self.owner, &other.owner)
    }
}
impl Drop for ExistingOwnerRetentionLease {
    fn drop(&mut self) {
        if let Some(key) = self.key.take() {
            self.owner.release_pin(key);
        }
    }
}
impl fmt::Debug for ExistingOwnerRetentionLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExistingOwnerRetentionLease")
            .finish_non_exhaustive()
    }
}
