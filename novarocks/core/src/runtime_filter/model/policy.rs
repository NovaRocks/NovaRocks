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

use super::contract::RuntimeFilterPolicyRequirement;

pub const MAX_ARTIFACT_BYTES: u64 = 1 << 30;
pub const MAX_DEADLINE_MS: u64 = 86_400_000;
pub const MAX_RETRIES: u32 = 100;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeFilterPolicyValidationError {
    ZeroMaxContributionBytes,
    ZeroMaxArtifactBytes,
    ZeroDeadlineMs,
    ZeroMaxRetries,
    ContributionBytesExceedArtifactBytes,
    ArtifactBytesExceedLimit,
    DeadlineExceedsLimit,
    RetriesExceedLimit,
}

pub fn validate_runtime_filter_policy(
    policy: RuntimeFilterPolicyRequirement,
) -> Result<(), RuntimeFilterPolicyValidationError> {
    if policy.max_contribution_bytes == 0 {
        return Err(RuntimeFilterPolicyValidationError::ZeroMaxContributionBytes);
    }
    if policy.max_artifact_bytes == 0 {
        return Err(RuntimeFilterPolicyValidationError::ZeroMaxArtifactBytes);
    }
    if policy.deadline_ms == 0 {
        return Err(RuntimeFilterPolicyValidationError::ZeroDeadlineMs);
    }
    if policy.max_retries == 0 {
        return Err(RuntimeFilterPolicyValidationError::ZeroMaxRetries);
    }
    if policy.max_contribution_bytes > policy.max_artifact_bytes {
        return Err(RuntimeFilterPolicyValidationError::ContributionBytesExceedArtifactBytes);
    }
    if policy.max_artifact_bytes > MAX_ARTIFACT_BYTES {
        return Err(RuntimeFilterPolicyValidationError::ArtifactBytesExceedLimit);
    }
    if policy.deadline_ms > MAX_DEADLINE_MS {
        return Err(RuntimeFilterPolicyValidationError::DeadlineExceedsLimit);
    }
    if policy.max_retries > MAX_RETRIES {
        return Err(RuntimeFilterPolicyValidationError::RetriesExceedLimit);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_policy_boundaries_are_accepted() {
        assert_eq!(
            validate_runtime_filter_policy(RuntimeFilterPolicyRequirement {
                max_contribution_bytes: MAX_ARTIFACT_BYTES,
                max_artifact_bytes: MAX_ARTIFACT_BYTES,
                deadline_ms: MAX_DEADLINE_MS,
                max_retries: MAX_RETRIES,
            }),
            Ok(())
        );
    }
}
