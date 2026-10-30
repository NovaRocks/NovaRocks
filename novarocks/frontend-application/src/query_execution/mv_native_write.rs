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

//! Native write assembly for the Frontend-owned MV refresh lifecycle.
//!
//! The MV application module owns refresh domain facts; this module owns the
//! assembly vocabulary those facts are dispatched through.  Keeping the two
//! apart lets the MV application port stay with the MV domain while the
//! sealed encoding carrier and its provider activation port travel with the
//! rest of query assembly.

use novarocks_proto_codec::lifecycle::QueryOptions;
use novarocks_spi::connector::ConnectorWriteReceipt;

use novarocks_mv_application::publication::{MvRefreshCommittedFacts, MvRefreshPublicationIntent};

/// Exact completed-plan inputs for one Frontend-owned MV native assembly.
///
/// The plan and its frozen read access are paired before encoding. Finishing
/// consumes their attempt template, so another binding cannot reach dispatch.
///
/// Every MV data write -- first refresh and incremental alike -- commits through
/// the write session that admitted it. The session sealed the recipes this
/// plan's writer nodes carry, so the two travel together and no operation,
/// cohort, or attempt identity reaches the writer data plane.
pub struct PreparedMvNativeWriteAssembly {
    description: novarocks_query_application::preparation::FrozenExecutionDescription,
    template: crate::query_execution::artifact::PreparedDistributedAttemptTemplate,
    query_options: Option<QueryOptions>,
    session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
}

impl PreparedMvNativeWriteAssembly {
    /// The commit authority of this write, so a caller that fails between
    /// assembly and dispatch can release it rather than leaving the provider
    /// holding a session for a plan that will never run.
    pub(crate) fn write_session(
        &self,
    ) -> &std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession> {
        &self.session
    }
}

/// MV uses the same complete-write consumer as the other write intents. The
/// original session and reservation are consumed once, without constructing a
/// synchronous execution request.
impl crate::query::statement_computation::StatementComputationAssembly
    for PreparedMvNativeWriteAssembly
{
    type Output = crate::query_execution::internal_consumer::write::WriteComputationOutput;

    fn prepare(
        self,
        initial: crate::query_execution::logical_execution::InitialComputationReservation,
        runtime: crate::query_execution::internal_consumer::decoded::DecodedConsumerRuntime,
    ) -> Result<
        (
            crate::query_execution::logical_execution::PreparedLogicalComputation,
            novarocks_query_application::api::ReservedExecutionIdentity,
            Self::Output,
        ),
        novarocks_query_application::api::QueryExecutionError,
    > {
        crate::query_execution::internal_consumer::write::prepare_write_computation(
            self.description,
            self.template,
            std::sync::Arc::new(
                crate::query_execution::contract::ResolvedQueryOptions::from_upstream(
                    self.query_options,
                ),
            ),
            self.session,
            initial,
            runtime,
        )
    }
}

/// Validate every static-plan check while the actual read capability set is
/// still held by the original activation Source. The final bind only moves it.
pub(crate) struct PreparedMvNativeWriteParts {
    encoded: crate::query_execution::physical_encoding::PreparedStaticPlan,
    description: novarocks_query_application::preparation::FrozenExecutionDescription,
    version: novarocks_physical_plan::PlanVersionId,
    query_options: Option<QueryOptions>,
}
impl PreparedMvNativeWriteParts {
    pub(crate) fn try_new(
        encoded: crate::query_execution::physical_encoding::PreparedStaticPlan,
        version: novarocks_physical_plan::PlanVersionId,
        query_options: Option<QueryOptions>,
    ) -> Result<Self, String> {
        let scheduling = encoded.scheduling_facts(version)?;
        let description = mv_write_description(
            encoded.candidate().clone(),
            scheduling
                .fragments
                .iter()
                .flat_map(|fragment| fragment.scans.iter().map(|scan| scan.scan))
                .collect(),
        )?;
        Ok(Self {
            encoded,
            description,
            version,
            query_options,
        })
    }
    pub(crate) fn bind_access(
        self,
        access: novarocks_query_application::preparation::FinalPlanRuntimeAccess<
            crate::query_execution::provider_read_facts::FrozenProviderRead,
        >,
        session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    ) -> PreparedMvNativeWriteAssembly {
        let template = self
            .encoded
            .bind_access(access)
            .into_attempt_template(self.version);
        PreparedMvNativeWriteAssembly {
            description: self.description,
            template,
            query_options: self.query_options,
            session,
        }
    }
}
fn mv_write_description(
    candidate: novarocks_query_application::preparation::CompletedPhysicalPlanCandidate,
    scan_identities: Vec<novarocks_query_application::api::PlanScanIdentity>,
) -> Result<novarocks_query_application::preparation::FrozenExecutionDescription, String> {
    novarocks_query_application::preparation::FrozenExecutionDescription::for_completed_plan(
        novarocks_query_application::api::QueryExecutionKind::Write,
        candidate,
        scan_identities,
        novarocks_query_application::preparation::OutputContract::CompletionOnly,
        novarocks_coordination_contract::ExecutionEffect::External,
        novarocks_coordination_contract::RecoveryMode::NoRecovery,
        Vec::new(),
        novarocks_query_application::preparation::FrozenCostEstimate::unknown(
            novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected,
        ),
        novarocks_query_application::preparation::ExecutionResourceRequirements::unknown(
            novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected,
        ),
    )
}

pub(crate) fn encode_compile_error(
    error: novarocks_plan_codec::PhysicalEncodeError,
) -> novarocks_sql::compiler::SqlCompileError {
    match error {
        novarocks_plan_codec::PhysicalEncodeError::Control(error) => error.into(),
        other => novarocks_sql::compiler::SqlCompileError::CompilationSource(
            novarocks_failure_contract::SharedFailureCause::new(other),
        ),
    }
}

/// Provider activation and native fragment preparation for a SQL-shaped
/// refresh artifact. The frontend owns intent persistence, write-session
/// admission, native assembly, execution, commit, publication, and cleanup;
/// the port returns only an exact sealed encoding carrier after the lease is
/// retained.
pub trait MvRefreshProviderActivation: Send + Sync {
    /// Install one original write-preparation source from already received
    /// facts. Construction performs no Provider I/O, polling or execution.
    fn prepare_write_source(
        &self,
        request: crate::query_execution::mv_assembly::OriginalMvWritePreparationRequest,
    ) -> crate::query_execution::mv_assembly::MvWritePreparationCustody;

    fn interpret_write_commit(
        &self,
        intent: MvRefreshPublicationIntent,
        receipt: &ConnectorWriteReceipt,
    ) -> Result<MvRefreshCommittedFacts, String>;
}

#[cfg(test)]
mod capability_identity_tests {
    #[test]
    fn mv_encode_capability_refusal_keeps_the_client_identity() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        let original = novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(
            "change-stream router".into(),
        );
        let expected = original.failure_fact();
        let novarocks_sql::compiler::SqlCompileError::CompilationSource(source) =
            super::encode_compile_error(original)
        else {
            panic!("retain the actual encode author")
        };
        assert_eq!(source.failure_fact(), expected);
        assert_eq!(expected.class(), FailureClass::Unsupported);
        assert!(
            matches!(source.downcast_ref::<novarocks_plan_codec::PhysicalEncodeError>(), Some(novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(detail)) if detail == "change-stream router")
        );
    }
}
