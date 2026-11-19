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

//! Write-result counts are protocol scalars, not SQL numeric row values.
use crate::connector::{ConnectorError, ConnectorErrorKind};
use novarocks_failure_contract::{
    FailureClass, FailureFact, FailureProjection, FailureReason, RepresentationBoundary,
    RepresentationFacts, RepresentationLayout, domain,
};
use std::{error::Error, fmt, num::NonZeroU16};
#[derive(Debug)]
pub(super) enum WriteCountFailure {
    Negative { value: i64 },
    SignedCarrier { required: u64 },
    Total { left: u64, right: u64 },
}
impl fmt::Display for WriteCountFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Negative { value } => write!(f, "connector write row count is negative: {value}"),
            Self::SignedCarrier { required } => write!(
                f,
                "connector write row count exceeds the relation's signed carrier: {required}"
            ),
            Self::Total { left, right } => {
                write!(f, "connector write row count overflowed: {left} + {right}")
            }
        }
    }
}
impl Error for WriteCountFailure {}
impl FailureProjection for WriteCountFailure {
    fn failure_fact(&self) -> FailureFact {
        let (code, layout, required, limit) = match self {
            Self::Negative { .. } => {
                return FailureFact::new(
                    FailureClass::Invariant,
                    FailureReason::new(domain::CONNECTOR, NonZeroU16::new(40).unwrap()),
                );
            }
            Self::SignedCarrier { required } => (41, 1, Some(*required), i64::MAX as u64),
            Self::Total { left, right } => (
                42,
                2,
                u64::try_from(u128::from(*left) + u128::from(*right)).ok(),
                u64::MAX,
            ),
        };
        FailureFact::new(
            FailureClass::Representation,
            FailureReason::new(domain::CONNECTOR, NonZeroU16::new(code).unwrap()),
        )
        .with_representation(RepresentationFacts {
            // Affected-row count is one indivisible scalar. Splitting a
            // batch cannot make this scalar representable.
            boundary: RepresentationBoundary::MinimumUnit,
            layout: RepresentationLayout {
                domain: domain::CONNECTOR,
                code: NonZeroU16::new(layout).unwrap(),
            },
            required,
            extent_limit: Some(limit),
            representable_prefix: None,
        })
    }
}
impl From<WriteCountFailure> for ConnectorError {
    fn from(source: WriteCountFailure) -> Self {
        let kind = if matches!(source, WriteCountFailure::Negative { .. }) {
            ConnectorErrorKind::CorruptData
        } else {
            ConnectorErrorKind::ResourceExhausted
        };
        Self::from_domain(kind, source)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::write_stack::{
        WriteRowCountAccumulator, row_count_from_wire, row_count_to_wire,
    };
    #[test]
    fn count_carrier_bounds_are_atomic_representation_and_negative_is_contract() {
        assert_eq!(row_count_to_wire(i64::MAX as u64).unwrap(), i64::MAX);
        let failure = row_count_to_wire(i64::MAX as u64 + 1).unwrap_err();
        let fact = failure.failure_fact();
        assert_eq!(fact.class(), FailureClass::Representation);
        let extent = fact.representation().unwrap();
        assert_eq!(extent.boundary, RepresentationBoundary::MinimumUnit);
        assert_eq!(extent.required, Some(i64::MAX as u64 + 1));
        assert_eq!(extent.extent_limit, Some(i64::MAX as u64));
        assert_eq!(
            row_count_from_wire(-1).unwrap_err().failure_fact().class(),
            FailureClass::Invariant
        );
        let mut total = WriteRowCountAccumulator::new();
        total.add(u64::MAX).unwrap();
        let overflow = total.add(1).unwrap_err();
        assert_eq!(total.get(), u64::MAX);
        assert_eq!(
            overflow.failure_fact().representation().unwrap().required,
            None
        );
        assert_eq!(
            overflow
                .failure_fact()
                .representation()
                .unwrap()
                .extent_limit,
            Some(u64::MAX)
        );
    }
}
