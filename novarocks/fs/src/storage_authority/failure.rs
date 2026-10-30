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

//! Stable acquisition reasons owned by the credential consumer.

use std::{error::Error, fmt, num::NonZeroU16};

use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, domain,
};

/// The existing acquisition classification, independent of file operation kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub(crate) enum AuthorityFailureReason {
    TransientAcquisition = 1,
    AcquisitionDenied = 2,
    CatalogUnreachable = 3,
    NoRenewalCapability = 4,
    MetadataUnreachable = 5,
}

/// The local source stays private; the neutral fact carries no provider text.
#[derive(Debug)]
pub(crate) struct AuthorityFailure {
    reason: AuthorityFailureReason,
    detail: String,
}

impl AuthorityFailure {
    pub(crate) fn new(reason: AuthorityFailureReason, detail: String) -> Self {
        Self { reason, detail }
    }
}

impl fmt::Display for AuthorityFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.detail)
    }
}

impl Error for AuthorityFailure {}

impl FailureProjection for AuthorityFailure {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Dependency,
            FailureReason::new(
                domain::STORAGE_AUTHORITY,
                NonZeroU16::new(self.reason as u16).expect("authority reasons are nonzero"),
            ),
        )
    }
}
