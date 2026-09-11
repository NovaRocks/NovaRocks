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

//! Core-owned assembly after the SQL compiler has produced sealed facts.
//!
//! This module deliberately accepts a sealed distributed plan together with
//! the application materializer that admitted it.  It keeps the exact binding
//! store, fragment preparation, and native-request finalization outside the
//! SQL compiler and does not expose a route to substitute a newer binding.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use novarocks_plan_codec::SealedWriteTargets;
use novarocks_proto_codec::lifecycle::QueryOptions;

use crate::common::admitted_query_context::QueryExecutionContext;
use crate::query_execution::preparation::{PreparedFragmentHandoff, PreparedFragmentSet};
use crate::query_execution::{PreparedQueryCompletion, PreparedQueryOperation};
use novarocks_sql::compiler::{SqlPlanCostFacts, SqlPlanCostUnknownReason, SqlPlanCostValue};
use novarocks_sql::plan_read::DistributedPlan;
use novarocks_sql::planning::query_execution::SealedPreparationPlan;

/// Select the completion formatter paired with one post-compile assembly.
/// SQL owns the plan facts; Core owns the profile formatter and its
/// connector-planning observations.
pub enum PostCompileIntent {
    Result,
    Profile {
        planning_elapsed: std::time::Duration,
        execution_started_at: std::time::Instant,
    },
}

/// Exact plan/preparation pair frozen by Core for one Frontend-owned native
/// assembly step. It has no constructor and exposes only immutable encoder
/// inputs, so callers cannot replace bindings or acquire a newer generation.
pub struct NativeFragmentEncodingInput {
    prepared: PreparedFragmentHandoff,
    sql_cost: Option<SqlPlanCostFacts>,
    provenance: u64,
    /// Present exactly when this plan contains dataflow writer nodes. The
    /// recipes travel with the plan they were sealed for so an encode can
    /// never pair one round's plan with another round's session.
    write_targets: Option<SealedWriteTargets>,
}

impl NativeFragmentEncodingInput {
    pub(crate) fn new(prepared: PreparedFragmentHandoff) -> Self {
        Self {
            prepared,
            sql_cost: None,
            provenance: next_native_encoding_provenance(),
            write_targets: None,
        }
    }

    pub(crate) fn from_sealed_sql_terminal(
        sealed_plan: SealedPreparationPlan,
        sql_cost: SqlPlanCostFacts,
        prepared: PreparedFragmentHandoff,
    ) -> Result<Self, String> {
        if sealed_plan.id() != prepared.sealed_plan().id() {
            return Err(
                "SQL terminal plan was not consumed by its exact prepared handoff".to_string(),
            );
        }
        drop(sealed_plan);
        Ok(Self {
            prepared,
            sql_cost: Some(sql_cost),
            provenance: next_native_encoding_provenance(),
            write_targets: None,
        })
    }

    pub(crate) fn new_for_test(
        distributed_plan: DistributedPlan,
        prepared: PreparedFragmentSet,
    ) -> Self {
        let sealed_plan = SealedPreparationPlan::seal(distributed_plan);
        Self::new(PreparedFragmentHandoff::for_test(sealed_plan, prepared))
    }

    #[cfg(test)]
    pub(crate) fn native_attachment_for_test(
        &self,
        fragments: impl IntoIterator<Item = novarocks_proto_models::plan::PlanFragment>,
        expected_ids: &std::collections::BTreeSet<novarocks_sql::plan_read::FragmentId>,
    ) -> Result<crate::query_execution::native_fragment::NativeFragmentAttachment, String> {
        crate::query_execution::native_fragment::native_fragment_attachment_for_test(
            fragments,
            expected_ids,
            Some(self.provenance),
        )
    }

    /// Attach the write targets one begin session sealed for exactly this plan.
    pub(crate) fn with_sealed_write_targets(mut self, write_targets: SealedWriteTargets) -> Self {
        self.write_targets = Some(write_targets);
        self
    }

    /// The sealed recipes, present only for a dataflow write plan.
    pub(crate) const fn sealed_write_targets(&self) -> Option<&SealedWriteTargets> {
        self.write_targets.as_ref()
    }

    pub fn distributed_plan(&self) -> &DistributedPlan {
        self.prepared.sealed_plan().plan()
    }

    pub fn prepared(&self) -> &PreparedFragmentSet {
        self.prepared.prepared()
    }

    pub fn encoding_view(
        &self,
    ) -> crate::query_execution::native_fragment::NativeFragmentEncodingView<'_> {
        crate::query_execution::native_fragment::NativeFragmentEncodingView::sealed(
            self.prepared.sealed_plan().plan(),
            self.prepared.prepared(),
            self.provenance,
        )
    }

    fn into_finalizer_inputs(
        self,
        native_attachment: &crate::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> Result<NativeFinalizerInputs, String> {
        if !native_attachment.matches_provenance(self.provenance) {
            return Err(
                "native fragment bundle does not match the sealed query encoding input".into(),
            );
        }
        let (sealed_plan, prepared, attempt_access, description_inputs, selected_mv_query_inputs) =
            self.prepared.into_parts();
        Ok(NativeFinalizerInputs {
            sealed_plan,
            prepared,
            attempt_access,
            description_inputs,
            selected_mv_query_inputs,
            sql_cost: self.sql_cost,
        })
    }
}

struct NativeFinalizerInputs {
    sealed_plan: SealedPreparationPlan,
    prepared: PreparedFragmentSet,
    attempt_access: crate::query_execution::preparation::ConnectorAttemptAccessPlan,
    description_inputs: crate::query_execution::preparation::FrozenDescriptionInputs,
    selected_mv_query_inputs:
        Option<novarocks_query_application::preparation::SelectedMvQueryInputs>,
    sql_cost: Option<SqlPlanCostFacts>,
}

/// Move-only result of the sole semantic/native finalizer. Its constructor is
/// private to this module, so sibling modules cannot pair a frozen description
/// with native projection or attempt access from another assembly.
pub(crate) struct FinalizedDistributedExecution {
    description: Arc<novarocks_query_application::preparation::FrozenExecutionDescription>,
    attempt_template: crate::query_execution::artifact::PreparedDistributedAttemptTemplate,
}

impl FinalizedDistributedExecution {
    fn new(
        description: novarocks_query_application::preparation::FrozenExecutionDescription,
        prepared: PreparedFragmentSet,
        native_attachment: crate::query_execution::native_fragment::NativeFragmentAttachment,
        attempt_access: crate::query_execution::preparation::ConnectorAttemptAccessPlan,
    ) -> Self {
        Self {
            description: Arc::new(description),
            attempt_template:
                crate::query_execution::artifact::PreparedDistributedAttemptTemplate::new(
                    prepared,
                    native_attachment,
                    attempt_access,
                ),
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Arc<novarocks_query_application::preparation::FrozenExecutionDescription>,
        crate::query_execution::artifact::PreparedDistributedAttemptTemplate,
    ) {
        (self.description, self.attempt_template)
    }
}

fn next_native_encoding_provenance() -> u64 {
    static NEXT_PROVENANCE: AtomicU64 = AtomicU64::new(1);
    loop {
        let provenance = NEXT_PROVENANCE.fetch_add(1, Ordering::Relaxed);
        if provenance != 0 {
            return provenance;
        }
    }
}

/// Core-owned request finalizer for one Frontend-encoded distributed query.
/// Frontend supplies the only native bundle after reading the exact sealed
/// pair; Core retains lifecycle request construction and completion pairing.
pub struct PreparedDistributedQueryAssembly {
    encoding: NativeFragmentEncodingInput,
    query_options: Option<QueryOptions>,
    intent: crate::query_execution::contract::DistributedQueryIntent,
    execution: QueryExecutionContext,
}

impl PreparedDistributedQueryAssembly {
    pub(crate) fn new(
        encoding: NativeFragmentEncodingInput,
        query_options: Option<QueryOptions>,
        intent: crate::query_execution::contract::DistributedQueryIntent,
        execution: QueryExecutionContext,
    ) -> Self {
        Self {
            encoding,
            query_options,
            intent,
            execution,
        }
    }

    pub fn encoding(&self) -> &NativeFragmentEncodingInput {
        &self.encoding
    }

    pub fn finish(
        self,
        native_attachment: crate::query_execution::native_fragment::NativeFragmentAttachment,
    ) -> Result<crate::query_execution::contract::DistributedQueryRequest, String> {
        self.finish_internal(native_attachment, None)
    }

    pub(crate) fn finish_statistics(
        self,
        native_attachment: crate::query_execution::native_fragment::NativeFragmentAttachment,
        program: crate::query_execution::statistics::StatisticsCollectionProgram,
    ) -> Result<crate::query_execution::contract::DistributedQueryRequest, String> {
        self.finish_internal(native_attachment, Some(program))
    }

    fn finish_internal(
        self,
        native_attachment: crate::query_execution::native_fragment::NativeFragmentAttachment,
        statistics_program: Option<crate::query_execution::statistics::StatisticsCollectionProgram>,
    ) -> Result<crate::query_execution::contract::DistributedQueryRequest, String> {
        let finalized = self.encoding.into_finalizer_inputs(&native_attachment)?;
        let NativeFinalizerInputs {
            sealed_plan,
            prepared,
            attempt_access,
            description_inputs,
            selected_mv_query_inputs,
            sql_cost,
        } = finalized;
        let receipts = description_inputs.into_receipts();
        let restartable = attempt_access.exactly_covers(&receipts);
        let plan_has_write = prepared.write_root_targets().is_some();
        if plan_has_write
            != matches!(
                self.intent,
                crate::query_execution::contract::DistributedQueryIntent::Write
            )
        {
            return Err(
                "distributed query intent does not match the sealed plan effect".to_string(),
            );
        }
        let (kind, effect, recovery) = match self.intent {
            crate::query_execution::contract::DistributedQueryIntent::Result
            | crate::query_execution::contract::DistributedQueryIntent::Profile => (
                novarocks_query_application::api::QueryExecutionKind::Read,
                novarocks_query_application::coordination::ExecutionEffect::None,
                if restartable {
                    novarocks_query_application::coordination::RecoveryMode::RestartAttemptBeforeVisibility
                } else {
                    novarocks_query_application::coordination::RecoveryMode::NoRecovery
                },
            ),
            crate::query_execution::contract::DistributedQueryIntent::Write => (
                novarocks_query_application::api::QueryExecutionKind::Write,
                novarocks_query_application::coordination::ExecutionEffect::External,
                novarocks_query_application::coordination::RecoveryMode::NoRecovery,
            ),
            crate::query_execution::contract::DistributedQueryIntent::Statistics => (
                novarocks_query_application::api::QueryExecutionKind::Statistics,
                novarocks_query_application::coordination::ExecutionEffect::None,
                novarocks_query_application::coordination::RecoveryMode::NoRecovery,
            ),
        };
        let mv_candidate_match =
            finalize_selected_mv_candidate(&sealed_plan, selected_mv_query_inputs, &receipts)?;
        let cost = sql_cost.map(map_sql_cost).unwrap_or_else(|| {
            novarocks_query_application::preparation::FrozenCostEstimate::unknown(
                novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected,
            )
        });
        let resources =
            novarocks_query_application::preparation::ExecutionResourceRequirements::unknown(
                novarocks_query_application::preparation::FrozenEstimateUnknownReason::NotProjected,
            );
        let description =
            novarocks_query_application::preparation::FrozenExecutionDescription::try_freeze(
                novarocks_query_application::preparation::FrozenExecutionDescriptionDraft::new(
                    kind,
                    sealed_plan,
                    mv_candidate_match,
                    effect,
                    recovery,
                    receipts,
                    cost,
                    resources,
                ),
            )?;
        let finalized = FinalizedDistributedExecution::new(
            description,
            prepared,
            native_attachment,
            attempt_access,
        );
        crate::query_execution::contract::build_request_from_finalized_execution(
            finalized,
            self.query_options,
            self.intent,
            &self.execution,
            statistics_program,
        )
        .map_err(|error| error.to_string())
    }

    pub fn into_operation(
        self,
        native_attachment: crate::query_execution::native_fragment::NativeFragmentAttachment,
        completion: PreparedQueryCompletion,
        logical_reservation: crate::query_execution::completion::LogicalQueryReservation,
    ) -> Result<PreparedQueryOperation, String> {
        let request = self.finish(native_attachment)?;
        Ok(PreparedQueryOperation::Distributed(
            crate::query_execution::PreparedQueryDistributedOperation::new(
                request,
                completion,
                logical_reservation,
            ),
        ))
    }
}

fn finalize_selected_mv_candidate(
    sealed_plan: &SealedPreparationPlan,
    selected: Option<novarocks_query_application::preparation::SelectedMvQueryInputs>,
    receipts: &[novarocks_query_application::preparation::NegotiatedScanReceipt],
) -> Result<Option<novarocks_query_application::preparation::StrictMvCandidateMatch>, String> {
    let mut actions = sealed_plan
        .scan_contracts()?
        .into_iter()
        .filter_map(|scan| scan.mv_rewrite_action());
    let action = actions.next();
    if actions.next().is_some() {
        return Err("sealed distributed plan selects more than one MV rewrite action".to_string());
    }
    let (action, selected) = match (action, selected) {
        (None, None) => return Ok(None),
        (Some(_), None) => {
            return Err(
                "selected MV rewrite is missing its query-input proof at the production finalizer"
                    .to_string(),
            );
        }
        (None, Some(_)) => {
            return Err(
                "MV query-input proof is attached to a plan with no selected rewrite".to_string(),
            );
        }
        (Some(action), Some(selected)) => (action, selected),
    };
    if selected.target() != action.target() {
        return Err("MV query-input proof belongs to another sealed plan action".to_string());
    }
    let mut target_receipts = receipts
        .iter()
        .filter(|receipt| receipt.lineage().occurrence().occurrence() == action.target());
    let target = target_receipts
        .next()
        .ok_or_else(|| "selected MV rewrite has no exact final target receipt".to_string())?;
    if target_receipts.next().is_some() {
        return Err("selected MV rewrite has duplicate final target receipts".to_string());
    }
    novarocks_query_application::preparation::prove_selected_mv_target(
        selected,
        target.lineage().occurrence(),
    )
    .map(Some)
}

fn map_sql_cost(
    cost: SqlPlanCostFacts,
) -> novarocks_query_application::preparation::FrozenCostEstimate {
    novarocks_query_application::preparation::FrozenCostEstimate::new(
        map_sql_cost_value(cost.root_rows()),
        map_sql_cost_value(cost.cpu()),
        map_sql_cost_value(cost.memory()),
        map_sql_cost_value(cost.network()),
    )
}

fn map_sql_cost_value(
    value: SqlPlanCostValue,
) -> novarocks_query_application::preparation::FrozenCostValue {
    use novarocks_query_application::preparation::{FrozenCostValue, FrozenEstimateUnknownReason};
    match value {
        SqlPlanCostValue::Known(value) => FrozenCostValue::Known(value),
        SqlPlanCostValue::Unknown(reason) => FrozenCostValue::Unknown(match reason {
            SqlPlanCostUnknownReason::MissingRootFragment => {
                FrozenEstimateUnknownReason::MissingRootFragment
            }
            SqlPlanCostUnknownReason::FallbackRowEstimate => {
                FrozenEstimateUnknownReason::FallbackRowEstimate
            }
            SqlPlanCostUnknownReason::MissingCostEstimate => {
                FrozenEstimateUnknownReason::MissingCostEstimate
            }
            SqlPlanCostUnknownReason::NonFinite => FrozenEstimateUnknownReason::NonFinite,
            SqlPlanCostUnknownReason::Negative => FrozenEstimateUnknownReason::Negative,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_query_application::preparation::{FrozenCostValue, FrozenEstimateUnknownReason};

    #[test]
    fn sql_cost_mapping_preserves_zero_and_named_unknown() {
        assert_eq!(
            map_sql_cost_value(SqlPlanCostValue::Known(0.0)),
            FrozenCostValue::Known(0.0)
        );
        assert_eq!(
            map_sql_cost_value(SqlPlanCostValue::Unknown(
                SqlPlanCostUnknownReason::FallbackRowEstimate,
            )),
            FrozenCostValue::Unknown(FrozenEstimateUnknownReason::FallbackRowEstimate)
        );
        assert_eq!(
            map_sql_cost_value(SqlPlanCostValue::Unknown(
                SqlPlanCostUnknownReason::MissingCostEstimate,
            )),
            FrozenCostValue::Unknown(FrozenEstimateUnknownReason::MissingCostEstimate)
        );
    }

    #[test]
    fn mv_rewritten_plan_without_selected_input_proof_fails_at_finalizer() {
        let plan = SealedPreparationPlan::seal(
            novarocks_sql::test_support::native_mv_rewritten_scan_plan()
                .expect("MV rewrite fixture"),
        );
        let error = finalize_selected_mv_candidate(&plan, None, &[])
            .expect_err("a selected rewrite requires its query-input proof");
        assert!(error.contains("missing its query-input proof"), "{error}");
    }

    #[test]
    fn ordinary_plan_requires_no_mv_candidate_proof() {
        let plan = SealedPreparationPlan::seal(
            novarocks_sql::test_support::native_scan_plan(
                novarocks_sql::test_support::NativeScanFixture::ConnectorRead,
            )
            .expect("ordinary scan fixture"),
        );
        assert!(
            finalize_selected_mv_candidate(&plan, None, &[])
                .expect("ordinary plan has no MV proof")
                .is_none()
        );
    }
}

/// Prepare one SQL compiler result against the exact materializer that
/// admitted its bindings.  The Frontend calls the compiler itself, then hands
/// the sealed result to this Core-only preparation step; no caller can supply
/// a separate binding store or reacquire a current connector generation.
#[allow(clippy::too_many_arguments)]
pub fn prepare_compiled_distributed_query(
    terminal: novarocks_sql::compiler::SqlDistributedQueryTerminal,
    query_kernel: &crate::query_execution::kernels::QueryPreparationKernel,
    analyzer_catalog: &crate::catalog_application::query_materializer::CatalogServiceMaterializer<
        '_,
    >,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    query_options: Option<QueryOptions>,
    execution: &QueryExecutionContext,
    completion_intent: PostCompileIntent,
) -> Result<(PreparedDistributedQueryAssembly, PreparedQueryCompletion), String> {
    let (distributed_plan, sql_cost) = terminal.into_parts();
    let sealed_plan = SealedPreparationPlan::seal(distributed_plan);
    prepare_sealed_logical_execution(
        &sealed_plan,
        sql_cost,
        query_kernel,
        Some(analyzer_catalog.query_table_bindings().as_ref()),
        connector_context,
        query_options,
        execution,
        completion_intent,
    )
}

/// Freeze one logical execution and its immutable attempt template.
///
/// Scan negotiation, native template encoding, MV proof and description
/// sealing happen exactly once here. Replacement attempts consume the
/// resulting template and cannot call back into this function.
#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_sealed_logical_execution(
    sealed_plan: &SealedPreparationPlan,
    sql_cost: SqlPlanCostFacts,
    query_kernel: &crate::query_execution::kernels::QueryPreparationKernel,
    query_table_bindings: Option<
        &crate::catalog_application::query_bindings::QueryTableBindingStore,
    >,
    connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    query_options: Option<QueryOptions>,
    execution: &QueryExecutionContext,
    completion_intent: PostCompileIntent,
) -> Result<(PreparedDistributedQueryAssembly, PreparedQueryCompletion), String> {
    crate::query_execution::compiler::ensure_mainline_distributed_execution(
        false,
        query_kernel.exchange_port(),
    )?;
    let prepared = crate::query_execution::preparation::prepare_fragments_for_sealed_plan(
        sealed_plan,
        query_kernel.connector_control().as_ref(),
        connector_context,
        query_table_bindings,
        None,
        crate::query_execution::compiler::scan_preparation_options(
            query_kernel.typed_connector_control(),
            execution.optimizer_settings(),
        )?,
    )?;
    let distributed_intent = match &completion_intent {
        PostCompileIntent::Result => {
            crate::query_execution::contract::DistributedQueryIntent::Result
        }
        PostCompileIntent::Profile { .. } => {
            crate::query_execution::contract::DistributedQueryIntent::Profile
        }
    };
    let completion = match completion_intent {
        PostCompileIntent::Result => PreparedQueryCompletion::result(),
        PostCompileIntent::Profile {
            planning_elapsed,
            execution_started_at,
        } => PreparedQueryCompletion::profile(
            sealed_plan.shared_plan(),
            planning_elapsed,
            execution_started_at,
        ),
    };
    let assembly = PreparedDistributedQueryAssembly::new(
        NativeFragmentEncodingInput::from_sealed_sql_terminal(
            sealed_plan.clone(),
            sql_cost,
            prepared,
        )?,
        query_options,
        distributed_intent,
        execution.clone(),
    );
    Ok((assembly, completion))
}
