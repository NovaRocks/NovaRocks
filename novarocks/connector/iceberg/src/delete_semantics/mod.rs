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

//! Pure Iceberg delete facts, candidate selection, and closure validation.
//!
//! The metadata owner observes manifests and supplies a pinned read domain.
//! This module neither discovers metadata nor acquires runtime resources. Raw
//! observation validation is deliberately separate from replaying a normalized
//! closure: a second live DV entry is invalid, while recalling the same already
//! validated member is harmless. Statistics affect a `LoadView`, never the
//! logical `DeleteSet` used to compare snapshot endpoints.

mod candidate_index;
mod canonical_json;
mod facts;
mod metrics;
mod partition_codec;
mod set_view;

pub use candidate_index::*;
pub(crate) use canonical_json::{canonical_metadata_json, canonical_schema_json};
pub use facts::*;
pub use metrics::*;
pub use partition_codec::*;
pub use set_view::*;

/// Stable categories that the provider boundary maps to its own error contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeleteSemanticsErrorKind {
    MissingSequence,
    InvalidSequence,
    InvalidPartition,
    InvalidFieldBinding,
    UnsupportedPromotion,
    InvalidAddress,
    InvalidReadFacts,
    MultipleDeletionVectors,
    DeletionVectorOlderThanData,
    InvalidClosure,
    DomainMismatch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteSemanticsError {
    pub kind: DeleteSemanticsErrorKind,
    pub message: String,
}

impl DeleteSemanticsError {
    pub(crate) fn new(kind: DeleteSemanticsErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for DeleteSemanticsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for DeleteSemanticsError {}

pub type Result<T> = std::result::Result<T, DeleteSemanticsError>;

#[cfg(test)]
mod tests;

/// An explicitly pinned fixture domain shared by all test owners of a relation.
#[cfg(test)]
pub(crate) fn test_read_domain(
    schema: &crate::iceberg::spec::Schema,
    specs: &[crate::iceberg::spec::PartitionSpec],
    snapshot: i64,
) -> std::sync::Arc<ReadDomain> {
    std::sync::Arc::new(ReadDomain::new(
        ReadObservationId::try_new([7; 16]).expect("fixture observation"),
        PinnedEndpointFacts::try_new(
            uuid::Uuid::from_u128(7),
            "fixture-pinned-metadata",
            snapshot,
            schema,
            specs,
        )
        .expect("fixture endpoint"),
    ))
}
