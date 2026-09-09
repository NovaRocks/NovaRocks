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

//! Provider-neutral port for the lake-sourced MV Accelerator.
//!
//! The repository owns one closed rebuildable family. Runtime attempts,
//! scheduler state, partition freshness, recovery and provider transactions do
//! not cross this boundary.

use std::collections::BTreeMap;
use std::fmt;

use novarocks_spi::connector::ConnectorTableObjectId;
use novarocks_state_store_api::VersionToken;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use crate::mv::domain::model::MvTarget;
use crate::mv::domain::persistence::definition::{
    CreateMvDefinitionRequest, MvAcceleratorSourceRevision, MvDesiredRefreshPolicy,
    StoredMvDefinition,
};
pub use crate::mv::domain::persistence::dependency::CreateMvDependencyRequest;
use crate::mv::domain::persistence::dependency::StoredMvDependency;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvRepositoryErrorKind {
    InvalidRequest,
    NotFound,
    Conflict,
    Corruption,
    Unavailable,
    CommitUnknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRepositoryError {
    kind: MvRepositoryErrorKind,
    message: String,
}

impl MvRepositoryError {
    pub fn new(kind: MvRepositoryErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub const fn kind(&self) -> MvRepositoryErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for MvRepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MvRepositoryError {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvTargetLookup {
    pub mv_id: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitialMvRefreshConfiguration {
    pub policy: MvDesiredRefreshPolicy,
    pub paused: bool,
    pub interval_ms: Option<i64>,
    pub max_staleness_ms: Option<i64>,
}

impl Default for InitialMvRefreshConfiguration {
    fn default() -> Self {
        Self {
            policy: MvDesiredRefreshPolicy::Manual,
            paused: false,
            interval_ms: None,
            max_staleness_ms: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MvPublishedProjection {
    NeverPublished,
    Published(MvPublishedWaterline),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvPublishedWaterline {
    pub last_refresh_ms: i64,
    pub last_refresh_rows: i64,
    pub last_refreshed_iceberg_snapshot_id: i64,
    pub base_snapshots: BTreeMap<String, i64>,
    pub base_table_object_ids: BTreeMap<String, ConnectorTableObjectId>,
}

/// Complete payload replaced as one root/index CAS.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvProjectionRequest {
    pub definition: CreateMvDefinitionRequest,
    pub refresh: InitialMvRefreshConfiguration,
    pub publication: MvPublishedProjection,
    pub source_revision: MvAcceleratorSourceRevision,
    pub dependencies: Vec<CreateMvDependencyRequest>,
}

/// Opaque StateStore version returned only by a successful repository read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvProjectionVersion(VersionToken);

impl MvProjectionVersion {
    pub(crate) fn from_store(version: VersionToken) -> Self {
        Self(version)
    }

    pub(crate) fn store_version(&self) -> &VersionToken {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedMvProjection {
    pub definition: StoredMvDefinition,
    pub version: MvProjectionVersion,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplaceMvProjectionRequest {
    pub mv_id: i64,
    pub expected_version: MvProjectionVersion,
    pub projection: MvProjectionRequest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteMvProjectionRequest {
    pub mv_id: i64,
    pub expected_version: MvProjectionVersion,
    pub expected_source_revision: MvAcceleratorSourceRevision,
}

/// Synchronous application port. Concrete repositories may bridge to an async
/// StateStore internally, but no raw key or transaction crosses this boundary.
pub trait MvRepository: Send + Sync {
    fn create_projection(
        &self,
        operation_id: Uuid,
        projection: MvProjectionRequest,
    ) -> Result<LoadedMvProjection, MvRepositoryError>;

    fn replace_projection(
        &self,
        operation_id: Uuid,
        request: ReplaceMvProjectionRequest,
    ) -> Result<LoadedMvProjection, MvRepositoryError>;

    fn load_by_id(&self, mv_id: i64) -> Result<Option<LoadedMvProjection>, MvRepositoryError>;

    fn find_by_target(
        &self,
        target: &MvTarget,
    ) -> Result<Option<LoadedMvProjection>, MvRepositoryError>;

    fn list_projections(&self) -> Result<Vec<LoadedMvProjection>, MvRepositoryError>;

    fn delete_projection(
        &self,
        operation_id: Uuid,
        request: DeleteMvProjectionRequest,
    ) -> Result<bool, MvRepositoryError>;

    /// Test/harness-only destructive wipe of one rebuildable projection.
    /// It deliberately has no source-equivalence semantics.
    fn wipe_projection_by_target(
        &self,
        operation_id: Uuid,
        target: &MvTarget,
    ) -> Result<bool, MvRepositoryError>;

    /// Test/harness-only wipe of the complete current Accelerator family,
    /// including the internal sequence. Old physical families remain untouched.
    fn wipe_accelerator(&self, operation_id: Uuid) -> Result<(), MvRepositoryError>;

    fn list_dependencies_by_downstream(
        &self,
        mv_id: i64,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError>;

    fn list_downstream_dependencies(
        &self,
        upstream: &crate::mv::domain::dependency::model::MvDependencyObjectRef,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError>;

    fn ensure_no_downstream_dependencies(
        &self,
        upstream: &crate::mv::domain::dependency::model::MvDependencyObjectRef,
    ) -> Result<(), MvRepositoryError>;
}
