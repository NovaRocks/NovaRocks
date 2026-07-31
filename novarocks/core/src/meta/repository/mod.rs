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

use crate::meta::{MetaError, MetaErrorKind};

pub mod iceberg_operation;
pub mod id_scopes;
pub mod job;

pub use crate::meta::payload::{decode_payload_for_kind, encode_record_payload};

#[cfg(test)]
pub(crate) mod test_avro_seed {
    use serde::{Deserialize, Serialize};

    use crate::meta::MetaPayload;
    use crate::meta::repository::iceberg_operation::StoredIcebergOperation;
    use crate::meta::repository::job::StoredEraseJob;
    use crate::meta::repository::{RepositoryError, RepositoryResult, encode_record_payload};

    pub(crate) fn encode_seed_payload(
        kind: &str,
        payload: &serde_json::Value,
    ) -> RepositoryResult<MetaPayload> {
        match kind {
            "job.erase" => encode_from_json::<StoredEraseJob>(kind, payload),
            "iceberg.operation" => encode_from_json::<StoredIcebergOperation>(kind, payload),
            _ => Err(RepositoryError::invalid(format!(
                "unsupported test seed metadata kind `{kind}`"
            ))),
        }
    }

    fn encode_from_json<T>(kind: &str, payload: &serde_json::Value) -> RepositoryResult<MetaPayload>
    where
        T: for<'de> Deserialize<'de> + Serialize,
    {
        let value = serde_json::from_value::<T>(payload.clone()).map_err(|err| {
            RepositoryError::invalid(format!(
                "failed to materialize test seed payload for `{kind}`: {err}"
            ))
        })?;
        encode_record_payload(kind, &value)
    }
}

pub type RepositoryResult<T> = Result<T, RepositoryError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepositoryErrorKind {
    Conflict,
    NotFound,
    InvalidRequest,
    Provider,
}

#[derive(Debug)]
pub struct RepositoryError {
    kind: RepositoryErrorKind,
    message: String,
}

impl RepositoryError {
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(RepositoryErrorKind::Conflict, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(RepositoryErrorKind::NotFound, message)
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(RepositoryErrorKind::InvalidRequest, message)
    }

    pub fn provider(message: impl Into<String>) -> Self {
        Self::new(RepositoryErrorKind::Provider, message)
    }

    pub fn kind(&self) -> RepositoryErrorKind {
        self.kind
    }

    fn new(kind: RepositoryErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for RepositoryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self.kind {
            RepositoryErrorKind::Conflict => "conflict",
            RepositoryErrorKind::NotFound => "not found",
            RepositoryErrorKind::InvalidRequest => "invalid request",
            RepositoryErrorKind::Provider => "provider error",
        };
        write!(f, "metadata repository {label}: {}", self.message)
    }
}

impl std::error::Error for RepositoryError {}

impl From<MetaError> for RepositoryError {
    fn from(err: MetaError) -> Self {
        match err.kind() {
            MetaErrorKind::Conflict | MetaErrorKind::AlreadyExists => {
                Self::conflict(err.to_string())
            }
            MetaErrorKind::NotFound => Self::not_found(err.to_string()),
            MetaErrorKind::InvalidRequest | MetaErrorKind::Unsupported => {
                Self::invalid(err.to_string())
            }
            MetaErrorKind::Transient
            | MetaErrorKind::DefiniteCommitFailure
            | MetaErrorKind::CommitUnknown
            | MetaErrorKind::ProviderCorruption => Self::provider(err.to_string()),
        }
    }
}
