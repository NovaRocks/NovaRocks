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

//! A Source admission owns its one actual bound Work/report/capture profile.
//! No second bundle or transferable payment survives beside that role.

use crate::control_runtime_policy::StorageNeed;
use novarocks_control_runtime::reported_work::{ReportedWork, ReportedWorkBindingError};

pub(crate) const SOURCE_SLOT_STORAGE: StorageNeed = StorageNeed {
    requests: 0,
    inputs: 0,
    batches: 0,
    effects: 0,
    cuts: 0,
    replies: 0,
    completions: 1,
    captures: 1,
    work: 1,
};

#[derive(Debug)]
pub(crate) enum SourceSlotStorageError {
    InvalidStorage,
    Binding(ReportedWorkBindingError),
}
impl std::fmt::Display for SourceSlotStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidStorage => f.write_str("Source slot storage has an invalid bound profile"),
            Self::Binding(error) => write!(f, "Source slot reported Work binding failed: {error}"),
        }
    }
}
impl std::error::Error for SourceSlotStorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidStorage => None,
            Self::Binding(error) => Some(error),
        }
    }
}
#[must_use = "A refused admission retains its original reported Work"]
#[derive(Debug)]
pub(crate) struct SourceSlotAdmissionRefusal {
    pub error: SourceSlotStorageError,
    pub reported: ReportedWork,
}

#[must_use = "The Source bank accepts the unique original bound role"]
#[derive(Debug)]
pub(crate) struct SourceSlotAdmission {
    reported: ReportedWork,
}
impl SourceSlotAdmission {
    #[cfg(test)]
    pub(crate) fn reported(&self) -> &ReportedWork {
        &self.reported
    }
    /// Validate outside the ingress fence. The profile comes from the original
    /// actual Header, report and bound captures, never remaining unused slots.
    pub(crate) fn from_admitted(
        reported: ReportedWork,
    ) -> Result<Self, SourceSlotAdmissionRefusal> {
        let profile = match reported.bound_storage_profile() {
            Ok(profile) => profile,
            Err(error) => {
                return Err(SourceSlotAdmissionRefusal {
                    error: SourceSlotStorageError::Binding(error),
                    reported,
                });
            }
        };
        let expected = SOURCE_SLOT_STORAGE
            .resources()
            .expect("fixed Source role description fits")
            .owned_slots;
        if profile != expected {
            return Err(SourceSlotAdmissionRefusal {
                error: SourceSlotStorageError::InvalidStorage,
                reported,
            });
        }
        Ok(Self { reported })
    }
    pub(crate) fn into_reported(self) -> ReportedWork {
        self.reported
    }
}

#[cfg(test)]
mod tests;
