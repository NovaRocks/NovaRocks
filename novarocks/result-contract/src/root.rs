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
use std::num::NonZeroU32;

/// Closed internal domains. Each variant has a private codec and collector;
/// no variant authorizes a generic Arrow result fallback.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum InternalResultDomain {
    ScalarValueV1,
    CowSelectionArrowV1,
    StatisticsArtifactV1,
    PreparedWriteCommitV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum RootOutputKind {
    ClientRows,
    InternalFacts(InternalResultDomain),
    CountOnly,
}

/// A wire support identity, distinct from runtime capacity or query funding.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct RootProfileId(NonZeroU32);
impl RootProfileId {
    pub const V1: Self = Self(NonZeroU32::new(1).unwrap());
    pub fn try_from_wire(value: u32) -> Result<Self, RootContractError> {
        match value {
            1 => Ok(Self::V1),
            _ => Err(RootContractError::UnsupportedProfile),
        }
    }
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootContractError {
    UnsupportedProfile,
    InvalidSchema,
    UnsupportedRenderType,
    SchemaLimit,
    InvalidSequence,
    InvalidPayload,
}
impl fmt::Display for RootContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnsupportedProfile => "unsupported root-result profile",
            Self::InvalidSchema => "invalid frozen root-result schema",
            Self::UnsupportedRenderType => "unsupported frozen result render type",
            Self::SchemaLimit => "frozen root-result schema exceeds its profile",
            Self::InvalidSequence => "invalid root-result sequence",
            Self::InvalidPayload => "invalid root-result payload for its frozen output kind",
        })
    }
}
impl std::error::Error for RootContractError {}

/// Pure profile constants. Runtime owners must reserve their complete
/// capacity/backing overlap separately before constructing these payloads.
pub struct RootProfileV1;
impl RootProfileV1 {
    pub const SEGMENT_BYTES: usize = 1024 * 1024;
    pub const ENVELOPE_BYTES: usize = 4096;
    pub const DATA_POSITIONS: usize = 2;
    pub const TERMINAL_POSITIONS: usize = 1;
    pub const ROW_PAYLOAD_BYTES: u32 = 64 * 1024 * 1024;
    pub const SMALL_ROW_BYTES: usize = 64 * 1024;
    pub const EMIT_BYTES_PER_TURN: usize = 64 * 1024;
    pub const CELLS_PER_TURN: usize = 1024;
    pub const MAX_DEPTH: usize = 64;
    pub const MAX_ELEMENTS_PER_ROW: usize = 1_048_576;
    pub const MAX_COLUMNS: usize = 4096;
    pub const MAX_NAME_BYTES: usize = 64 * 1024;
    pub const SCHEMA_WIRE_BYTES: usize = 256 * 1024;
    // Each semantic field consumes at least 32 bytes in the schema's checked
    // wire allowance. Raw preflight uses the same derived expansion bound.
    pub const SCHEMA_TYPE_NODES: usize = Self::SCHEMA_WIRE_BYTES / 32;
    pub const SCHEMA_BACKING_BYTES: usize = 1024 * 1024;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unknown_profiles_have_no_default() {
        assert_eq!(RootProfileId::try_from_wire(1), Ok(RootProfileId::V1));
        for value in [0, 2, u32::MAX] {
            assert_eq!(
                RootProfileId::try_from_wire(value),
                Err(RootContractError::UnsupportedProfile)
            );
        }
    }
}

/// The frozen plan owns this immutable purpose. Runtime bindings must match
/// it exactly before installing a root producer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FrozenRootOutput {
    ClientRows(crate::ClientRenderSchema),
    ScalarValue(crate::ScalarSchema),
    InternalFacts(InternalResultDomain),
    CountOnly,
}
impl FrozenRootOutput {
    pub const fn kind(&self) -> RootOutputKind {
        match self {
            Self::ClientRows(_) => RootOutputKind::ClientRows,
            Self::ScalarValue(_) => {
                RootOutputKind::InternalFacts(InternalResultDomain::ScalarValueV1)
            }
            Self::InternalFacts(domain) => RootOutputKind::InternalFacts(*domain),
            Self::CountOnly => RootOutputKind::CountOnly,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootOutputContract {
    profile: RootProfileId,
    output: FrozenRootOutput,
}
impl RootOutputContract {
    pub const fn new(profile: RootProfileId, output: FrozenRootOutput) -> Self {
        Self { profile, output }
    }
    pub fn bind_native_slots(self, slots: &[u32]) -> Result<Self, RootContractError> {
        self.validate_purpose()?;
        let output = match self.output {
            FrozenRootOutput::ClientRows(schema) => {
                FrozenRootOutput::ClientRows(schema.bind_native_slots(slots)?)
            }
            FrozenRootOutput::ScalarValue(schema) => {
                FrozenRootOutput::ScalarValue(schema.bind_native_slots(slots)?)
            }
            other => other,
        };
        Ok(Self {
            profile: self.profile,
            output,
        })
    }
    /// A scalar domain identity alone cannot authorize an untyped producer.
    pub fn validate_purpose(&self) -> Result<(), RootContractError> {
        if matches!(
            self.output,
            FrozenRootOutput::InternalFacts(InternalResultDomain::ScalarValueV1)
        ) {
            return Err(RootContractError::InvalidSchema);
        }
        Ok(())
    }
    pub const fn profile(&self) -> RootProfileId {
        self.profile
    }
    pub const fn output(&self) -> &FrozenRootOutput {
        &self.output
    }
    pub const fn kind(&self) -> RootOutputKind {
        self.output.kind()
    }
}

impl novarocks_failure_contract::FailureProjection for RootContractError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        let (class, code) = match self {
            Self::UnsupportedProfile => (FailureClass::Data, 1100),
            Self::InvalidSchema => (FailureClass::Invariant, 1101),
            Self::UnsupportedRenderType => (FailureClass::Data, 1102),
            Self::SchemaLimit => (FailureClass::Resource, 1103),
            Self::InvalidSequence => (FailureClass::Data, 1104),
            Self::InvalidPayload => (FailureClass::Data, 1105),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::RESULT, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
