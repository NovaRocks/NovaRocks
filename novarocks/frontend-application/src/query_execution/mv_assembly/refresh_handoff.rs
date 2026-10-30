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

//! Frontend refresh-lifecycle handoff assembled from Core SQL facts.

use novarocks_mv_application::product::MvRefreshAttemptIdentity;
use novarocks_spi::connector::{ConnectorProviderBindingKey, ConnectorWriteOperationId};

use super::refresh_artifact::{
    PreparedMvFirstRefreshWrite, PreparedMvIncrementalWrite, PreparedMvMetadataOnlyWrite,
};
use novarocks_mv_application::publication::MvRefreshPublicationIntent;
use novarocks_sql::planning::mv::{MvRefreshFinalizeFacts, MvRefreshStatement, SqlMvTarget};

/// Application request supplied to the side-effect-free SQL preparation port.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshPreparationRequest {
    pub statement: MvRefreshStatement,
    pub target: SqlMvTarget,
    pub attempt: MvRefreshAttemptIdentity,
}

impl MvRefreshPreparationRequest {
    pub fn validate(&self) -> Result<(), String> {
        self.attempt.validate()
    }
}

/// Application-owned work handoff after SQL planning. SQL plans determine the
/// semantic shape; this lifecycle envelope owns operation/cohort-bearing
/// artifacts and is the only value admitted by frontend staging.
#[expect(
    clippy::large_enum_variant,
    reason = "The refresh handoff keeps the exact prepared-work payload at the frontend boundary."
)]
pub enum PreparedMvRefreshWork {
    NoOp,
    MetadataOnly {
        write: PreparedMvMetadataOnlyWrite,
        /// Held until the metadata-only publication reaches a terminal, so a
        /// lost outcome leaves the target unsettled rather than silently
        /// available for the next effect. It is a data publication's lease
        /// because a metadata-only refresh freezes the same input watermark:
        /// advancing that watermark is the whole reason to publish one.
        admitted: crate::mv::domain::staged_create::AdmittedMvDataPublication,
    },
    DataProducing {
        write: PreparedMvRefreshWrite,
        /// Held until the data publication reaches a terminal, and carrying
        /// the input watermark its P records.
        admitted: crate::mv::domain::staged_create::AdmittedMvDataPublication,
    },
}

/// Exactly one SQL-prepared staged write for a data-producing refresh.
///
/// The public application handoff intentionally hides the assembly-specific
/// first-refresh and incremental artifact shapes. Frontend lifecycle code can
/// validate its operation, cohort, and publication facts, but only the
/// query-assembly activation adapter may recover the exact artifact needed to
/// construct a native write.
pub struct PreparedMvRefreshWrite {
    artifact: PreparedMvRefreshWriteArtifact,
    sources: std::sync::Arc<super::refresh_preparation::captured_sources::CapturedRefreshSources>,
}

/// The assembly-facing shape remains crate-private so it cannot become part
/// of the public refresh-lifecycle contract.
pub(crate) enum PreparedMvRefreshWriteArtifact {
    FirstRefresh(PreparedMvFirstRefreshWrite),
    Incremental(PreparedMvIncrementalWrite),
}

impl PreparedMvRefreshWrite {
    pub(crate) fn first_refresh(
        write: PreparedMvFirstRefreshWrite,
        sources: std::sync::Arc<
            super::refresh_preparation::captured_sources::CapturedRefreshSources,
        >,
    ) -> Self {
        Self {
            artifact: PreparedMvRefreshWriteArtifact::FirstRefresh(write),
            sources,
        }
    }

    pub(crate) fn incremental(
        write: PreparedMvIncrementalWrite,
        sources: std::sync::Arc<
            super::refresh_preparation::captured_sources::CapturedRefreshSources,
        >,
    ) -> Self {
        Self {
            artifact: PreparedMvRefreshWriteArtifact::Incremental(write),
            sources,
        }
    }

    pub fn operation_id(&self) -> ConnectorWriteOperationId {
        match &self.artifact {
            PreparedMvRefreshWriteArtifact::FirstRefresh(write) => write.operation_id(),
            PreparedMvRefreshWriteArtifact::Incremental(write) => write.operation_id(),
        }
    }

    pub fn publication_intent(&self) -> &MvRefreshPublicationIntent {
        match &self.artifact {
            PreparedMvRefreshWriteArtifact::FirstRefresh(write) => write.publication_intent(),
            PreparedMvRefreshWriteArtifact::Incremental(write) => write.publication_intent(),
        }
    }

    /// Borrow the caller-owned artifact and receiving Source for Provider begin.
    /// The exact target binding is retained by that same caller; incremental
    /// input fields are projected from it without another Provider observation.
    pub(crate) async fn begin_session_in(
        &self,
        context: novarocks_spi::connector::ConnectorRequestContext,
        exact_lease: &novarocks_spi::connector::ConnectorWriteLease,
        planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
        typed_connector_control: &std::sync::Arc<
            novarocks_catalog_application::ConnectorControlHost,
        >,
        target: &crate::mv::domain::refresh::target_binding::MvTargetBinding,
        receiving: &crate::query_execution::write_session::WriteBeginReceiving,
    ) -> Result<
        std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
        novarocks_spi::connector::ConnectorError,
    > {
        match &self.artifact {
            PreparedMvRefreshWriteArtifact::FirstRefresh(write) => {
                super::iceberg_activation::begin_first_refresh_connector_write_session_async_in(
                    write,
                    context,
                    exact_lease,
                    planning_lease,
                    typed_connector_control,
                    receiving,
                )
                .await
            }
            PreparedMvRefreshWriteArtifact::Incremental(write) => {
                let request = write.request();
                if !exact_lease.matches_provider_binding_key(&request.observed_binding)
                    || exact_lease.control_runtime_id() != planning_lease.control_runtime_id()
                    || !std::sync::Arc::ptr_eq(target.lease().binding(), planning_lease.binding())
                    || target.handle() != request.target_table()
                {
                    return Err(novarocks_spi::connector::ConnectorError::new(
                        novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                        "MV incremental begin differs from its prepared target generation",
                    ));
                }
                let fields = target.write_input_fields().map_err(|error| {
                    novarocks_spi::connector::ConnectorError::new(
                        novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                        error,
                    )
                })?;
                super::iceberg_activation::begin_incremental_connector_write_session_async_in(
                    request,
                    target.handle(),
                    write.publication_intent(),
                    write.mode(),
                    &fields,
                    context,
                    exact_lease,
                    planning_lease,
                    typed_connector_control,
                    receiving,
                )
                .await
            }
        }
    }

    pub(crate) fn into_assembly_artifact(
        self,
    ) -> (
        PreparedMvRefreshWriteArtifact,
        std::sync::Arc<super::refresh_preparation::captured_sources::CapturedRefreshSources>,
    ) {
        (self.artifact, self.sources)
    }
}

/// Frontend lifecycle artifact assembled from SQL facts and a reserved attempt.
pub struct PreparedMvRefresh {
    pub attempt: MvRefreshAttemptIdentity,
    pub observed_binding: ConnectorProviderBindingKey,
    pub finalize: MvRefreshFinalizeFacts,
    pub work: PreparedMvRefreshWork,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attempt() -> MvRefreshAttemptIdentity {
        novarocks_mv_application::service::MvKeyedProductService::default()
            .reserve_refresh_attempt()
    }

    #[test]
    fn refresh_attempt_is_lifecycle_owned() {
        attempt().validate().expect("complete attempt identity");
        let attempt = attempt();
        assert_eq!(
            attempt.write_operation_id().to_bytes(),
            attempt.publication_id.to_bytes()
        );
        assert_eq!(
            attempt.staging_branch(),
            format!("__novarocks_mv_publication_{}", attempt.publication_id)
        );
    }
}
