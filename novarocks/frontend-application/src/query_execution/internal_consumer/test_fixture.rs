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

//! Closed validated fixtures for actual fixed-consumer component tests.
#[cfg(test)]
use crate::query_execution::completion_facts::test_support::FinalPlanCompletionDriverTestExt;
use crate::query_execution::lifecycle_plan::{
    AttemptCredentialLeaseCollector, AttemptCredentialStorage,
};
use novarocks_coordination_contract::{ExecutionEffect, RecoveryMode};
use novarocks_query_application::api::QueryExecutionKind;
use novarocks_query_application::preparation::{
    CompletedPhysicalPlanCandidate, ExecutionResourceRequirements, FrozenCostEstimate,
    FrozenEstimateUnknownReason, FrozenExecutionDescription, OutputContract,
};
use novarocks_secret::SecretValue;
use novarocks_spi::connector::{
    CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose, CatalogHandle,
    CatalogProperties, CatalogVersion, ConnectorInstanceId, ConnectorProviderId,
    ConnectorVendedCredentialLeaseSink, CredentialConsumerRole, StorageCredentialScopePrefix,
    VendedS3CredentialLeaseContribution, VendedS3CredentialLeaseEntry,
};
use novarocks_types::QueryExecutionId;
use std::sync::Arc;
/// Profile is an effect-free read that can replace its attempt only before
/// the original actor accepts computation readiness. Statistics/Write fixtures
/// keep their independently frozen NoRecovery contracts below.
pub(crate) async fn internal_profile_recovery_description(
    scope: &novarocks_workload_control::WorkScope,
) -> (
    FrozenExecutionDescription,
    novarocks_query_application::api::PlanSeal,
) {
    let completed = original_profile_completed_plan(scope).await;
    let (candidate, access) = completed.into_parts();
    drop(access);
    original_profile_description(candidate)
}

/// The encoder and logical description share the SAME completed candidate and
/// original access sidecar. No task graph, Root or Native acceptance is rebuilt.
pub(crate) async fn internal_profile_recovery_encoded_description(
    scope: &novarocks_workload_control::WorkScope,
    carrier: &crate::query_execution::package_freeze::CompiledPackageCarrier,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<
    (
        FrozenExecutionDescription,
        novarocks_query_application::api::PlanSeal,
        crate::query_execution::physical_encoding::EncodedCompletedPlan,
    ),
    novarocks_plan_codec::PhysicalEncodeError,
> {
    let completed = original_profile_completed_plan(scope).await;
    let (description, plan) = original_profile_description(completed.candidate().clone());
    let encoded = crate::query_execution::physical_encoding::encode_completed_plan_for_test(
        completed, carrier, None, control,
    )?;
    Ok((description, plan, encoded))
}

async fn original_profile_completed_plan(
    scope: &novarocks_workload_control::WorkScope,
) -> novarocks_query_application::preparation::CompletedPlanWithAccess<
    crate::query_execution::provider_read_facts::FrozenProviderRead,
> {
    original_literal_completed_plan(
        scope,
        novarocks_result_contract::FrozenRootOutput::CountOnly,
    )
    .await
}

/// A real SQL completion/journal for the finite transport/consumer component.
/// The peer supplies domain frames; SELECT 1 is not a Statistics/TableWriter
/// physical implementation, a Worker execution or an M5 fixture.
async fn original_literal_completed_plan(
    scope: &novarocks_workload_control::WorkScope,
    root_output: novarocks_result_contract::FrozenRootOutput,
) -> novarocks_query_application::preparation::CompletedPlanWithAccess<
    crate::query_execution::provider_read_facts::FrozenProviderRead,
> {
    use novarocks_physical_plan::{PipelineDopDomain, PlanVersionId};
    use novarocks_query_application::preparation::{
        FinalPlanCompletionDriver, ReadAccessSink, SqlCompletionFactSource,
    };
    use novarocks_sql::compiler::{
        DEFAULT_COMPLETION_LIMITS, SessionOptimizerSettings, SqlCompileControl, SqlCompileIntent,
        SqlFactBatch, SqlFinalPlanCompileRequest, SqlNeedBatch, SqlPlanningEnvironment,
        SqlSessionContext, SqlStatementInput, builtin_sql_function_catalog,
    };
    struct LiteralSqlComponentFacts;
    #[async_trait::async_trait]
    impl SqlCompletionFactSource for LiteralSqlComponentFacts {
        type Access = crate::query_execution::provider_read_facts::FrozenProviderRead;
        async fn resolve(
            &self,
            _: &SqlNeedBatch,
            _: &ReadAccessSink<Self::Access>,
        ) -> Result<SqlFactBatch, novarocks_sql::compiler::SqlCompileError> {
            panic!("the literal SQL component requires no external facts")
        }
    }
    let version = PlanVersionId::try_new([151; 16]).unwrap();
    let query = novarocks_parser::parser::parse_query("SELECT 1 AS a").unwrap();
    let request = SqlFinalPlanCompileRequest::new(
        version,
        SqlStatementInput::parsed_query(Box::new(query)),
        SqlCompileIntent::Query,
        SqlSessionContext {
            sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            current_catalog: None,
            current_database: "db".to_owned(),
            optimizer_settings: SessionOptimizerSettings::default(),
        },
        SqlPlanningEnvironment::Distributed,
        builtin_sql_function_catalog().snapshot(),
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        crate::application::test_constant_policy(),
        SqlCompileControl::unbounded(),
        PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: true,
        },
        DEFAULT_COMPLETION_LIMITS,
    );
    FinalPlanCompletionDriver::new(Arc::new(LiteralSqlComponentFacts))
        .complete_for_test(request, scope)
        .await
        .expect("the original literal SQL component completes without external facts")
        .freeze_root_output(root_output)
        .unwrap_or_else(|(error, original_access)| {
            // No Debug requirement on opaque Provider capabilities. This
            // original literal fixture has no external read sidecar entries.
            drop(original_access);
            panic!("freeze the original SQL component carrier: {error:?}");
        })
}

fn original_profile_description(
    candidate: CompletedPhysicalPlanCandidate,
) -> (
    FrozenExecutionDescription,
    novarocks_query_application::api::PlanSeal,
) {
    // CountOnly retains the compiler's original public SQL result declaration.
    let version = candidate.plan().version();
    let output =
        OutputContract::from_completed_candidate(QueryExecutionKind::Read, &candidate).unwrap();
    let description = FrozenExecutionDescription::for_completed_plan(
        QueryExecutionKind::Read,
        candidate,
        Vec::new(),
        output,
        ExecutionEffect::None,
        RecoveryMode::RestartAttemptBeforeVisibility,
        Vec::new(),
        FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
        ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
    )
    .unwrap()
    .into_internal_computation()
    .unwrap();
    let plan = novarocks_query_application::api::PlanSeal::Version(version);
    assert!(description.matches_plan_seal(plan));
    (description, plan)
}

/// Original structural candidate for pure frozen-contract consumer tests.
/// The encoded component uses the same source through the pure package validators.
pub(crate) fn internal_statistics_description_with_candidate() -> (
    FrozenExecutionDescription,
    novarocks_query_application::api::PlanSeal,
    CompletedPhysicalPlanCandidate,
) {
    internal_description_with_recovery_and_candidate(
        novarocks_result_contract::FrozenRootOutput::InternalFacts(
            novarocks_result_contract::InternalResultDomain::StatisticsArtifactV1,
        ),
        QueryExecutionKind::Statistics,
        ExecutionEffect::None,
        RecoveryMode::NoRecovery,
    )
}

/// Seal the SAME original structural component through the existing pure
/// package validators. No SQL source journal or provider access is fabricated.
pub(crate) fn internal_statistics_encoded_description(
    carrier: &crate::query_execution::package_freeze::CompiledPackageCarrier,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<
    (
        FrozenExecutionDescription,
        novarocks_query_application::api::PlanSeal,
        crate::fixtures::native_recipe::RecipeRoot,
    ),
    crate::query_execution::package_freeze::PackageFreezeError,
> {
    freeze_internal_fixture_description(
        internal_statistics_description_with_candidate(),
        carrier,
        control,
    )
}

/// The original PreparedWriteCommitV1 component remains External/NoRecovery.
/// Its declared protocol writer roles are not a real TableWriter/M5 claim.
pub(crate) fn internal_write_encoded_description(
    carrier: &crate::query_execution::package_freeze::CompiledPackageCarrier,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<
    (
        FrozenExecutionDescription,
        novarocks_query_application::api::PlanSeal,
        crate::fixtures::native_recipe::RecipeRoot,
    ),
    crate::query_execution::package_freeze::PackageFreezeError,
> {
    let original = internal_description_with_recovery_and_candidate(
        novarocks_result_contract::FrozenRootOutput::InternalFacts(
            novarocks_result_contract::InternalResultDomain::PreparedWriteCommitV1,
        ),
        QueryExecutionKind::Write,
        ExecutionEffect::External,
        RecoveryMode::NoRecovery,
    );
    freeze_internal_fixture_description(original, carrier, control)
}

fn freeze_internal_fixture_description(
    original: (
        FrozenExecutionDescription,
        novarocks_query_application::api::PlanSeal,
        CompletedPhysicalPlanCandidate,
    ),
    carrier: &crate::query_execution::package_freeze::CompiledPackageCarrier,
    control: &dyn novarocks_type_contract::PureCompileControl,
) -> Result<
    (
        FrozenExecutionDescription,
        novarocks_query_application::api::PlanSeal,
        crate::fixtures::native_recipe::RecipeRoot,
    ),
    crate::query_execution::package_freeze::PackageFreezeError,
> {
    let (description, plan, candidate) = original;
    let root = crate::fixtures::native_recipe::RecipeRoot::from_structural_component(
        &candidate, plan, carrier, control,
    )?;
    Ok((description, plan, root))
}

pub(crate) fn internal_statistics_description() -> (
    FrozenExecutionDescription,
    novarocks_query_application::api::PlanSeal,
) {
    let (description, plan, _candidate) = internal_statistics_description_with_candidate();
    (description, plan)
}

pub(crate) fn internal_write_description() -> (
    FrozenExecutionDescription,
    novarocks_query_application::api::PlanSeal,
) {
    internal_description_with_policy(
        novarocks_result_contract::FrozenRootOutput::InternalFacts(
            novarocks_result_contract::InternalResultDomain::PreparedWriteCommitV1,
        ),
        QueryExecutionKind::Write,
        ExecutionEffect::External,
    )
}

fn internal_description_with_policy(
    root_output: novarocks_result_contract::FrozenRootOutput,
    kind: QueryExecutionKind,
    effect: ExecutionEffect,
) -> (
    FrozenExecutionDescription,
    novarocks_query_application::api::PlanSeal,
) {
    internal_description_with_recovery(root_output, kind, effect, RecoveryMode::NoRecovery)
}

fn internal_description_with_recovery(
    root_output: novarocks_result_contract::FrozenRootOutput,
    kind: QueryExecutionKind,
    effect: ExecutionEffect,
    recovery: RecoveryMode,
) -> (
    FrozenExecutionDescription,
    novarocks_query_application::api::PlanSeal,
) {
    let (description, plan, _candidate) =
        internal_description_with_recovery_and_candidate(root_output, kind, effect, recovery);
    (description, plan)
}

fn internal_description_with_recovery_and_candidate(
    root_output: novarocks_result_contract::FrozenRootOutput,
    kind: QueryExecutionKind,
    effect: ExecutionEffect,
    recovery: RecoveryMode,
) -> (
    FrozenExecutionDescription,
    novarocks_query_application::api::PlanSeal,
    CompletedPhysicalPlanCandidate,
) {
    use novarocks_physical_plan::{
        FragmentBuilder, FragmentId, FragmentSink, OutputPort, PipelineDopDomain, PlanBuilder,
        PlanVersionId, ResultPort,
    };
    use novarocks_result_contract::{RootOutputContract, RootProfileId};
    // Only the frozen purpose varies; every fixture uses the same validated
    // physical author and actual actor End/Ready boundary.
    let fragment_id = FragmentId::new(0);
    let mut fragment = FragmentBuilder::new(fragment_id);
    let node = fragment.reserve_node_id().unwrap();
    fragment
        .add_values(node, Box::default(), Box::default())
        .unwrap();
    let fragment = fragment
        .finish_definition(
            node,
            FragmentSink::RootResult(Box::new(RootOutputContract::new(
                RootProfileId::V1,
                root_output,
            ))),
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: true,
            },
            novarocks_physical_plan::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let mut plan = PlanBuilder::new(PlanVersionId::try_new([151; 16]).unwrap());
    plan.add_fragment(fragment).unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: OutputPort {
            node,
            columns: Box::default(),
        },
        fields: Box::default(),
    })
    .unwrap();
    let candidate = CompletedPhysicalPlanCandidate::for_program(
        plan.finish().unwrap(),
        &novarocks_sql::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap();
    let output = OutputContract::from_completed_candidate(kind, &candidate).unwrap();
    let description = FrozenExecutionDescription::for_completed_plan(
        kind,
        candidate.clone(),
        Vec::new(),
        output,
        effect,
        recovery,
        Vec::new(),
        FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
        ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
    )
    .unwrap()
    .into_internal_computation()
    .unwrap();
    let plan = novarocks_query_application::api::PlanSeal::Version(
        PlanVersionId::try_new([151; 16]).unwrap(),
    );
    assert!(description.matches_plan_seal(plan));
    (description, plan, candidate)
}

/// One actual table via the original lifecycle author, never a re-created
/// terminal resolver. Tests compare Arc identity and print no secret material.
pub(crate) fn original_credentials(execution: QueryExecutionId) -> Arc<AttemptCredentialStorage> {
    let collector = AttemptCredentialLeaseCollector::new(execution);
    offer_original_credentials(collector.as_ref());
    let leases = collector.drain_credential_leases().unwrap();
    assert!(!leases.leases().is_empty(), "original vended table");
    leases.into_storage()
}

pub(crate) fn offer_original_credentials(sink: &dyn ConnectorVendedCredentialLeaseSink) {
    let properties = CatalogProperties::new(
        CatalogHandle::new(
            ConnectorInstanceId::try_from_canonical("catalog.handoff").unwrap(),
            CatalogVersion::from_bytes([0x24; 32]),
        ),
        ConnectorProviderId::parse("iceberg").unwrap(),
        1,
        vec![],
        vec![
            CatalogCredentialBinding::try_new(
                CatalogCredentialPurpose::ObjectStoreData,
                CredentialConsumerRole::Backend,
                CatalogCredentialMode::Vended,
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let contribution = VendedS3CredentialLeaseContribution::try_new(
        vec![
            VendedS3CredentialLeaseEntry::try_new(
                StorageCredentialScopePrefix::try_from_normalized("s3://warehouse/data").unwrap(),
                u64::MAX,
                SecretValue::new("handoff-test-access".to_owned()),
                SecretValue::new("handoff-test-secret".to_owned()),
                SecretValue::new("handoff-test-token".to_owned()),
            )
            .unwrap(),
        ],
        None,
    )
    .unwrap();
    sink.offer_vended_s3_credential_lease(&properties, contribution)
        .unwrap();
}
