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

macro_rules! numeric_id {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(u32);

        impl $name {
            pub const fn new(value: u32) -> Self {
                Self(value)
            }

            pub const fn get(self) -> u32 {
                self.0
            }
        }
    };
}

numeric_id!(FragmentId);
numeric_id!(NodeId);
pub use novarocks_constant_contract::ConstantPoolId;
pub use novarocks_expression_contract::{ExprId, ValueId};
numeric_id!(AggregateCallId);
numeric_id!(AggregateSequenceId);
numeric_id!(EdgeId);
numeric_id!(RuntimeFilterId);
numeric_id!(RuntimeFilterWitnessId);
numeric_id!(RuntimeFilterEqualityWitnessId);
numeric_id!(TopNSequenceId);
numeric_id!(ProviderReadOccurrenceId);

/// Identity of one immutable physical-plan version.
///
/// The creator owns uniqueness. The all-zero value is reserved so a missing
/// wire field cannot accidentally become a valid version.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PlanVersionId([u8; 16]);

impl PlanVersionId {
    pub fn try_new(bytes: [u8; 16]) -> Result<Self, IdentityError> {
        if bytes == [0; 16] {
            return Err(IdentityError::ZeroPlanVersion);
        }
        Ok(Self(bytes))
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IdentityError {
    ZeroPlanVersion,
    EmptyStableIdentity { kind: &'static str },
    StableIdentityTooLong { kind: &'static str, actual: usize },
}

impl fmt::Display for IdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroPlanVersion => formatter.write_str("plan version identity must be non-zero"),
            Self::EmptyStableIdentity { kind } => write!(formatter, "{kind} identity is empty"),
            Self::StableIdentityTooLong { kind, actual } => write!(
                formatter,
                "{kind} identity is {actual} bytes, exceeding the 1024-byte limit"
            ),
        }
    }
}

impl std::error::Error for IdentityError {}

pub(crate) fn stable_identity(
    kind: &'static str,
    value: impl AsRef<str>,
) -> Result<Box<str>, IdentityError> {
    let value = value.as_ref();
    if value.is_empty() {
        return Err(IdentityError::EmptyStableIdentity { kind });
    }
    if value.len() > 1024 {
        return Err(IdentityError::StableIdentityTooLong {
            kind,
            actual: value.len(),
        });
    }
    Ok(value.into())
}
