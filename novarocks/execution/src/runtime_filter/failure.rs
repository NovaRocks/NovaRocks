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

//! Nominal runtime-filter invariant/contract causes. Kernel control/host
//! failures remain the original Kernel category, never a formatted RF cause.

use super::contribution::{
    ContributionCodecError, ControlledContributionCodecError, ControlledOrderedTupleError,
    OrderedTupleError,
};
use super::{AggregateTopNBoundaryError, RuntimeFilterContractViolation};
use crate::exec::operators::OwnedTopNBoundaryError;
use crate::runtime::task::ExecutionFailure;
use std::{error::Error, fmt};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeFilterFailure {
    Contract(RuntimeFilterContractViolation),
    Schema(novarocks_local_program::FilterMembershipSchemaError),
    Boundary(AggregateTopNBoundaryError),
    Tuple(OrderedTupleError),
    Codec(ContributionCodecError),
}
impl RuntimeFilterFailure {
    pub fn detail(&self) -> &str {
        match self {
            Self::Contract(cause) => cause.detail(),
            Self::Schema(cause) => cause.detail(),
            Self::Boundary(AggregateTopNBoundaryError::InvalidCandidateValue(detail)) => detail,
            Self::Boundary(_) => "aggregate TopN runtime filter boundary failed",
            Self::Tuple(_) => "runtime filter order tuple failed",
            Self::Codec(_) => "runtime filter contribution codec failed",
        }
    }
}
impl fmt::Display for RuntimeFilterFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Contract(cause) => cause.fmt(f),
            Self::Schema(cause) => cause.fmt(f),
            Self::Boundary(cause) => cause.fmt(f),
            Self::Tuple(cause) => cause.fmt(f),
            Self::Codec(cause) => cause.fmt(f),
        }
    }
}
impl Error for RuntimeFilterFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(match self {
            Self::Contract(cause) => cause,
            Self::Schema(cause) => cause,
            Self::Boundary(cause) => cause,
            Self::Tuple(cause) => cause,
            Self::Codec(cause) => cause,
        })
    }
}
macro_rules! raw_cause {
    ($ty:ty, $variant:ident) => {
        impl From<$ty> for RuntimeFilterFailure {
            fn from(cause: $ty) -> Self {
                Self::$variant(cause)
            }
        }
        impl From<$ty> for ExecutionFailure {
            fn from(cause: $ty) -> Self {
                RuntimeFilterFailure::from(cause).into()
            }
        }
    };
}
raw_cause!(RuntimeFilterContractViolation, Contract);
raw_cause!(novarocks_local_program::FilterMembershipSchemaError, Schema);
raw_cause!(AggregateTopNBoundaryError, Boundary);
raw_cause!(OrderedTupleError, Tuple);
raw_cause!(ContributionCodecError, Codec);

impl From<OwnedTopNBoundaryError> for ExecutionFailure {
    fn from(cause: OwnedTopNBoundaryError) -> Self {
        match cause {
            OwnedTopNBoundaryError::Boundary(cause) => cause.into(),
            OwnedTopNBoundaryError::Kernel(cause) => cause.into(),
        }
    }
}
impl From<ControlledOrderedTupleError> for ExecutionFailure {
    fn from(cause: ControlledOrderedTupleError) -> Self {
        match cause {
            ControlledOrderedTupleError::Tuple(cause) => cause.into(),
            ControlledOrderedTupleError::Kernel(cause) => cause.into(),
        }
    }
}
impl From<ControlledContributionCodecError> for ExecutionFailure {
    fn from(cause: ControlledContributionCodecError) -> Self {
        match cause {
            ControlledContributionCodecError::Codec(cause) => cause.into(),
            ControlledContributionCodecError::Kernel(cause) => cause.into(),
        }
    }
}

impl novarocks_failure_contract::FailureProjection for RuntimeFilterFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        match self {
            Self::Contract(cause) => cause.failure_fact(),
            Self::Schema(cause) => cause.failure_fact(),
            Self::Boundary(cause) => cause.failure_fact(),
            Self::Tuple(cause) => cause.failure_fact(),
            Self::Codec(cause) => cause.failure_fact(),
        }
    }
}
