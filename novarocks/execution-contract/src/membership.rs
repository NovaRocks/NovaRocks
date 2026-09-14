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

//! Transport-neutral backend membership facts.
//!
//! Native adapters validate and project their generated membership messages
//! into these values. Role-local topology owners and query coordination retain
//! only this immutable vocabulary, never a protobuf wrapper.

use std::fmt;

use novarocks_types::{BackendProcessId, NativeCompatibilityId};

use crate::RuntimeEndpoint;

const MAX_DEPLOYMENT_ID_BYTES: usize = 256;
const MAX_BUILD_IDENTITY_BYTES: usize = 256;

/// The only backend liveness states a membership authority may admit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendReportedState {
    Running,
    Draining,
}

/// Immutable process facts announced by a backend and confirmed by heartbeat.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendProcessDescriptor {
    process_id: BackendProcessId,
    endpoint: RuntimeEndpoint,
    deployment_id: String,
    build_identity: String,
    native_compatibility_id: NativeCompatibilityId,
}

impl BackendProcessDescriptor {
    pub fn try_new(
        process_id: BackendProcessId,
        endpoint: RuntimeEndpoint,
        deployment_id: impl Into<String>,
        build_identity: impl Into<String>,
        native_compatibility_id: NativeCompatibilityId,
    ) -> Result<Self, BackendProcessDescriptorError> {
        let deployment_id = deployment_id.into();
        validate_text(
            &deployment_id,
            MAX_DEPLOYMENT_ID_BYTES,
            BackendProcessDescriptorError::DeploymentId,
        )?;
        let build_identity = build_identity.into();
        validate_text(
            &build_identity,
            MAX_BUILD_IDENTITY_BYTES,
            BackendProcessDescriptorError::BuildIdentity,
        )?;
        Ok(Self {
            process_id,
            endpoint,
            deployment_id,
            build_identity,
            native_compatibility_id,
        })
    }

    pub const fn process_id(&self) -> BackendProcessId {
        self.process_id
    }

    pub fn endpoint(&self) -> &RuntimeEndpoint {
        &self.endpoint
    }

    pub fn deployment_id(&self) -> &str {
        &self.deployment_id
    }

    pub fn build_identity(&self) -> &str {
        &self.build_identity
    }

    pub const fn native_compatibility_id(&self) -> NativeCompatibilityId {
        self.native_compatibility_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendProcessDescriptorError {
    DeploymentId,
    BuildIdentity,
}

impl fmt::Display for BackendProcessDescriptorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeploymentId => {
                formatter.write_str("deployment id must be non-empty and at most 256 bytes")
            }
            Self::BuildIdentity => {
                formatter.write_str("build identity must be non-empty and at most 256 bytes")
            }
        }
    }
}

impl std::error::Error for BackendProcessDescriptorError {}

fn validate_text(
    value: &str,
    maximum_bytes: usize,
    error: BackendProcessDescriptorError,
) -> Result<(), BackendProcessDescriptorError> {
    if value.trim().is_empty() || value.len() > maximum_bytes {
        return Err(error);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{BackendProcessDescriptor, BackendProcessDescriptorError};
    use crate::RuntimeEndpoint;
    use novarocks_types::{BackendProcessId, NativeCompatibilityId};

    #[test]
    fn descriptor_rejects_empty_or_oversized_identity_text() {
        let endpoint = RuntimeEndpoint::new("be-0.internal", 9060).expect("endpoint");
        assert_eq!(
            BackendProcessDescriptor::try_new(
                BackendProcessId::new_v7(),
                endpoint.clone(),
                "",
                "build",
                NativeCompatibilityId::new([7; 32]),
            ),
            Err(BackendProcessDescriptorError::DeploymentId)
        );
        assert_eq!(
            BackendProcessDescriptor::try_new(
                BackendProcessId::new_v7(),
                endpoint,
                "deployment",
                "b".repeat(257),
                NativeCompatibilityId::new([7; 32]),
            ),
            Err(BackendProcessDescriptorError::BuildIdentity)
        );
    }
}
