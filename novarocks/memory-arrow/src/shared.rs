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

//! Explicit service ownership and cross-scope borrowing of one paid lineage.

use novarocks_memory::ids::ExternalRef;

use crate::domain::RetentionDomain;
use crate::retained::Retained;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedError {
    DifferentAuthority,
    ForeignSponsor,
    ClosedService,
    ClosedBorrower,
}

/// A service-owned resource whose callers can borrow the exact paid lineage.
/// Dropping this registration does not revoke already borrowed values.
#[derive(Debug)]
pub struct SharedRetention<T> {
    resource: ExternalRef,
    owned: Retained<T>,
}

impl<T: Clone> SharedRetention<T> {
    pub fn register(
        service: &RetentionDomain,
        resource: ExternalRef,
        owned: Retained<T>,
    ) -> Result<Self, (SharedError, Retained<T>)> {
        if service.authority_key() != owned.authority_key() {
            return Err((SharedError::DifferentAuthority, owned));
        }
        if !owned.paid_entirely_by(service.leaf_id()) {
            return Err((SharedError::ForeignSponsor, owned));
        }
        if service.snapshot().closed {
            return Err((SharedError::ClosedService, owned));
        }
        Ok(Self { resource, owned })
    }

    pub fn resource(&self) -> ExternalRef {
        self.resource
    }

    pub fn borrow_for(&self, borrower: &RetentionDomain) -> Result<Retained<T>, SharedError> {
        if borrower.authority_key() != self.owned.authority_key() {
            return Err(SharedError::DifferentAuthority);
        }
        if borrower.snapshot().closed {
            return Err(SharedError::ClosedBorrower);
        }
        Ok(self.owned.fork())
    }

    /// A sponsor change needs the W2F transactional publication protocol.
    /// The source is returned intact so the caller keeps its paid responsibility.
    pub fn try_publish_to(self, _destination: &RetentionDomain) -> Result<Self, Self> {
        Err(self)
    }
}
