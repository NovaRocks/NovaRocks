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

//! Durable MV identities and revisions.
//!
//! A format version, content revision, computation identity, publication
//! identity, stable object/field identity, provider-native version, deployment
//! owner, and process incarnation answer different questions. This module owns
//! the durable domain types and deliberately cannot construct an owner or
//! runtime incarnation.

use sha2::{Digest, Sha256};

pub const SHA256_IDENTITY_BYTES: usize = 32;
pub const MAX_OPAQUE_IDENTITY_BYTES: usize = 64 * 1024;

macro_rules! digest_identity {
    ($name:ident, $label:literal) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name([u8; SHA256_IDENTITY_BYTES]);

        impl $name {
            pub fn from_canonical_bytes(bytes: &[u8]) -> Self {
                Self(Sha256::digest(bytes).into())
            }

            pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, IdentityError> {
                let value: [u8; SHA256_IDENTITY_BYTES] =
                    bytes.try_into().map_err(|_| IdentityError::InvalidLength {
                        identity: $label,
                        expected: SHA256_IDENTITY_BYTES,
                        actual: bytes.len(),
                    })?;
                Ok(Self(value))
            }

            pub fn as_bytes(&self) -> &[u8; SHA256_IDENTITY_BYTES] {
                &self.0
            }
        }
    };
}

digest_identity!(DocumentRevision, "document revision");

macro_rules! opaque_identity {
    ($name:ident, $label:literal) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(Vec<u8>);

        impl $name {
            pub fn try_new(bytes: Vec<u8>) -> Result<Self, IdentityError> {
                if bytes.is_empty() {
                    return Err(IdentityError::Empty($label));
                }
                if bytes.len() > MAX_OPAQUE_IDENTITY_BYTES {
                    return Err(IdentityError::TooLong {
                        identity: $label,
                        maximum: MAX_OPAQUE_IDENTITY_BYTES,
                        actual: bytes.len(),
                    });
                }
                Ok(Self(bytes))
            }

            pub fn as_bytes(&self) -> &[u8] {
                &self.0
            }

            pub fn into_bytes(self) -> Vec<u8> {
                self.0
            }
        }
    };
}

opaque_identity!(ObjectIdentity, "stable object identity");
opaque_identity!(FieldIdentity, "stable field identity");
opaque_identity!(SchemaVersion, "provider schema version");
opaque_identity!(PartitionSpecVersion, "provider partition-spec version");
opaque_identity!(NativeDataVersion, "provider native data version");
opaque_identity!(OutputIdentity, "stable output identity");
opaque_identity!(StateSlotIdentity, "stable state-slot identity");
opaque_identity!(AggregateIdentity, "stable aggregate identity");
opaque_identity!(BranchIdentity, "stable branch identity");
opaque_identity!(ApplyKeyIdentity, "stable apply-key component identity");

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PublicationIdentity(Vec<u8>);

impl PublicationIdentity {
    pub const MAX_BYTES: usize = 256;

    pub fn try_new(bytes: Vec<u8>) -> Result<Self, IdentityError> {
        if bytes.is_empty() {
            return Err(IdentityError::Empty("publication identity"));
        }
        if bytes.len() > Self::MAX_BYTES {
            return Err(IdentityError::TooLong {
                identity: "publication identity",
                maximum: Self::MAX_BYTES,
                actual: bytes.len(),
            });
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdentityError {
    Empty(&'static str),
    InvalidLength {
        identity: &'static str,
        expected: usize,
        actual: usize,
    },
    TooLong {
        identity: &'static str,
        maximum: usize,
        actual: usize,
    },
}

impl std::fmt::Display for IdentityError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty(identity) => write!(formatter, "{identity} must not be empty"),
            Self::InvalidLength {
                identity,
                expected,
                actual,
            } => write!(
                formatter,
                "{identity} must be exactly {expected} bytes, got {actual}"
            ),
            Self::TooLong {
                identity,
                maximum,
                actual,
            } => write!(
                formatter,
                "{identity} is {actual} bytes, exceeding the {maximum}-byte limit"
            ),
        }
    }
}

impl std::error::Error for IdentityError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publication_identity_is_bounded_and_nonempty() {
        assert!(PublicationIdentity::try_new(Vec::new()).is_err());
        assert!(PublicationIdentity::try_new(vec![0; PublicationIdentity::MAX_BYTES + 1]).is_err());
        assert_eq!(
            PublicationIdentity::try_new(vec![1, 2, 3])
                .expect("identity")
                .as_bytes(),
            &[1, 2, 3]
        );
    }

    #[test]
    fn apply_key_and_target_field_identities_remain_distinct_domains() {
        let apply_key = ApplyKeyIdentity::try_new(vec![7]).expect("apply-key identity");
        let target_field = FieldIdentity::try_new(vec![7]).expect("target field identity");
        assert_eq!(apply_key.as_bytes(), target_field.as_bytes());
    }
}
