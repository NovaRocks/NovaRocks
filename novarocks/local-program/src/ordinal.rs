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

//! Checked representation ordinals shared by fragment runtime consumers.

/// The frozen program site's index width is a representation limit, not an
/// allocator budget or a component invariant. The original standard refusal
/// remains available to every caller, including kernel and driver shells.
#[derive(Debug)]
pub struct ProgramOrdinalFailure {
    value: usize,
    source: std::num::TryFromIntError,
}
impl std::fmt::Display for ProgramOrdinalFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "program site ordinal {} exceeds UInt32: {}",
            self.value, self.source
        )
    }
}
impl std::error::Error for ProgramOrdinalFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl novarocks_failure_contract::FailureProjection for ProgramOrdinalFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        let code = std::num::NonZeroU16::new(30).unwrap();
        FailureFact::new(
            FailureClass::Representation,
            FailureReason::new(domain::LOCAL_PROGRAM, code),
        )
        .with_representation(RepresentationFacts {
            boundary: RepresentationBoundary::MinimumUnit,
            layout: RepresentationLayout {
                domain: domain::LOCAL_PROGRAM,
                code,
            },
            required: u64::try_from(self.value).ok(),
            extent_limit: Some(u32::MAX as u64),
            representable_prefix: None,
        })
    }
}
impl novarocks_failure_contract::OuterFailureProjection for ProgramOrdinalFailure {}
pub fn checked_program_ordinal(value: usize) -> Result<u32, ProgramOrdinalFailure> {
    u32::try_from(value).map_err(|source| ProgramOrdinalFailure { value, source })
}
#[cfg(test)]
mod ordinal_properties {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn ordinal_format_boundary_retains_the_real_standard_source_without_memory_facts() {
        assert_eq!(checked_program_ordinal(0).unwrap(), 0);
        assert_eq!(
            checked_program_ordinal(u32::MAX as usize).unwrap(),
            u32::MAX
        );
        if usize::BITS > 32 {
            let value = u32::MAX as usize + 1;
            let error = checked_program_ordinal(value).unwrap_err();
            let fact = error.failure_fact();
            assert_eq!(fact.class(), FailureClass::Representation);
            let representation = fact.representation().unwrap();
            assert_eq!(representation.required, Some(value as u64));
            assert_eq!(representation.extent_limit, Some(u32::MAX as u64));
            assert_eq!(representation.representable_prefix, None);
            assert!(fact.resource().is_none());
            assert!(
                std::error::Error::source(&error)
                    .unwrap()
                    .is::<std::num::TryFromIntError>()
            );
        }
    }
}
