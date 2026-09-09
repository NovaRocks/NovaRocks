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

use std::{num::NonZeroU64, sync::Arc};

use novarocks_spi::connector::read_stack::runtime::ConnectorReadAssignment;
use novarocks_spi::connector::read_stack::{
    ConnectorReadConstraint, ConnectorReadFilterApplication, ConnectorReadLimitApplication,
    ConnectorReadMetadata, ConnectorReadTableHandle, ConnectorSession,
};
use novarocks_sql::{
    plan_read::{DistributedPlan, OutputColumn},
    planning::query_execution::{SealedPreparationPlan, SealedScanContract, SealedScanIdentity},
};

use crate::api::{
    ExactBindingReceiptStore, ExactObjectBinding, QueryExecutionKind, RelationOccurrence,
    SealedExactBindingReceipts,
};
use crate::coordination::{ExecutionEffect, RecoveryMode};
use crate::observation::PreparationBudget;

use super::StrictMvCandidateMatch;

#[derive(Clone, Debug)]
pub enum OutputContract {
    Rows(Arc<[OutputColumn]>),
    CompletionOnly,
}

impl OutputContract {
    fn from_plan(kind: QueryExecutionKind, plan: &DistributedPlan) -> Result<Self, String> {
        let columns = plan
            .fragment_edge_outputs()
            .fragment_output_columns(plan.root_fragment_id());
        match columns {
            Some(columns) if !columns.is_empty() => Ok(Self::Rows(columns.into())),
            _ if kind == QueryExecutionKind::Read => {
                Err("frozen read plan has no row output contract".to_string())
            }
            _ => Ok(Self::CompletionOnly),
        }
    }
    pub fn columns(&self) -> &[OutputColumn] {
        match self {
            Self::Rows(columns) => columns,
            Self::CompletionOnly => &[],
        }
    }
}

/// Exact association between one SQL scan node and its admitted object
/// occurrence. The node id is the compiler's occurrence identity in the
/// sealed plan; the application binding supplies the exact provider version.
#[derive(Clone, Debug)]
pub struct PlanScanBinding {
    scan: SealedScanIdentity,
    binding: novarocks_sql::binding::SqlTableBindingId,
    occurrence: RelationOccurrence,
}

impl PlanScanBinding {
    fn resolved(scan: &SealedScanContract, binding: ExactObjectBinding) -> Self {
        Self {
            scan: scan.identity(),
            binding: scan.binding(),
            occurrence: RelationOccurrence::resolved(
                scan.identity(),
                scan.sql_occurrence(),
                scan.binding(),
                binding,
            ),
        }
    }

    pub const fn node_id(&self) -> i32 {
        self.scan.node_id()
    }

    pub const fn scan_identity(&self) -> SealedScanIdentity {
        self.scan
    }

    pub const fn occurrence(&self) -> &RelationOccurrence {
        &self.occurrence
    }
}

/// Store-issued, indivisible pairing of one sealed SQL scan, its exact object
/// receipt, and the opaque Connector handle admitted for that same runtime
/// generation. Only the receipt owner can construct this value.
pub struct AdmittedScanSubject {
    contract: SealedScanContract,
    lineage: PlanScanBinding,
    expected_read_binding: novarocks_spi::connector::read_stack::ConnectorReadBinding,
    handle: ConnectorReadTableHandle,
}

impl ExactBindingReceiptStore {
    pub fn admit_scan_handle(
        &self,
        contract: SealedScanContract,
        handle: ConnectorReadTableHandle,
    ) -> Result<AdmittedScanSubject, String> {
        let receipts = self.sealed_view().ok_or_else(|| {
            "exact binding receipt store must be semantically sealed before scan negotiation"
                .to_string()
        })?;
        let exact = receipts.resolve(contract.binding())?;
        if exact
            .object()
            .parts()
            .iter()
            .map(AsRef::as_ref)
            .collect::<Vec<_>>()
            != [contract.catalog(), contract.namespace(), contract.table()]
        {
            return Err(format!(
                "sealed scan node {} object differs from its store-issued exact binding receipt",
                contract.node_id()
            ));
        }
        let expected_read_binding = exact.read_binding()?.clone();
        if handle.binding() != &expected_read_binding {
            return Err(format!(
                "sealed scan node {} received a Connector handle from another admitted runtime generation",
                contract.node_id()
            ));
        }
        Ok(AdmittedScanSubject {
            lineage: PlanScanBinding::resolved(&contract, exact),
            contract,
            expected_read_binding,
            handle,
        })
    }
}

/// Jointly bind every sealed scan to the exact result retained by the
/// query-owned receipt store. The store is concrete and request scoped; there
/// is no open resolver adapter that can manufacture exact facts.
pub fn resolve_plan_scan_bindings(
    plan: &SealedPreparationPlan,
    receipts: &SealedExactBindingReceipts,
) -> Result<Vec<PlanScanBinding>, String> {
    plan.scan_contracts()?
        .iter()
        .map(|scan| {
            let binding = receipts.resolve(scan.binding())?;
            if binding.object().parts().iter().map(AsRef::as_ref).collect::<Vec<_>>()
                != [scan.catalog(), scan.namespace(), scan.table()]
            {
                return Err(format!(
                    "sealed scan node {} object differs from its store-issued exact binding receipt",
                    scan.node_id()
                ));
            }
            Ok(PlanScanBinding::resolved(scan, binding))
        })
        .collect()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ResidualResponsibility {
    EngineFilter,
    EngineLimit,
    EffectCommit,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct FrozenCostEstimate {
    estimated_rows: Option<NonZeroU64>,
    estimated_input_bytes: Option<NonZeroU64>,
    estimated_cpu_units: Option<NonZeroU64>,
}

impl FrozenCostEstimate {
    pub const fn new(
        estimated_rows: Option<NonZeroU64>,
        estimated_input_bytes: Option<NonZeroU64>,
        estimated_cpu_units: Option<NonZeroU64>,
    ) -> Self {
        Self {
            estimated_rows,
            estimated_input_bytes,
            estimated_cpu_units,
        }
    }

    pub const fn estimated_rows(self) -> Option<NonZeroU64> {
        self.estimated_rows
    }

    pub const fn estimated_input_bytes(self) -> Option<NonZeroU64> {
        self.estimated_input_bytes
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExecutionResourceRequirements {
    minimum_memory_bytes: Option<NonZeroU64>,
    result_credit_bytes: Option<NonZeroU64>,
    spill_bytes: Option<NonZeroU64>,
}

impl ExecutionResourceRequirements {
    pub const fn new(
        minimum_memory_bytes: Option<NonZeroU64>,
        result_credit_bytes: Option<NonZeroU64>,
        spill_bytes: Option<NonZeroU64>,
    ) -> Self {
        Self {
            minimum_memory_bytes,
            result_credit_bytes,
            spill_bytes,
        }
    }

    pub const fn minimum_memory_bytes(self) -> Option<NonZeroU64> {
        self.minimum_memory_bytes
    }

    pub const fn result_credit_bytes(self) -> Option<NonZeroU64> {
        self.result_credit_bytes
    }
}

#[derive(Clone, Debug)]
pub struct ScanNegotiationOutcome {
    scan: SealedScanIdentity,
    binding: novarocks_sql::binding::SqlTableBindingId,
    residual_predicate_ordinals: Arc<[usize]>,
    projected_columns: Arc<[OutputColumn]>,
    projection_applied: bool,
    offered_limit: bool,
    limit_guaranteed: bool,
}

/// The only successful output of a closed Connector negotiation. The final
/// opaque handle cannot be separated from the exact scan lineage and the
/// residual-responsibility facts that were observed while evolving it.
pub struct NegotiatedScanReceipt {
    final_handle: Option<ConnectorReadTableHandle>,
    lineage: PlanScanBinding,
    outcome: ScanNegotiationOutcome,
}

impl NegotiatedScanReceipt {
    pub fn final_handle(&self) -> Result<&ConnectorReadTableHandle, String> {
        self.final_handle.as_ref().ok_or_else(|| {
            "test-only negotiation receipt has no Connector table handle".to_string()
        })
    }

    pub const fn lineage(&self) -> &PlanScanBinding {
        &self.lineage
    }

    pub const fn outcome(&self) -> &ScanNegotiationOutcome {
        &self.outcome
    }

    #[cfg(test)]
    fn accepted_for_test(contract: &SealedScanContract, lineage: PlanScanBinding) -> Self {
        Self {
            final_handle: None,
            lineage,
            outcome: ScanNegotiationOutcome::accepted_for_test(contract),
        }
    }
}

impl std::fmt::Debug for NegotiatedScanReceipt {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NegotiatedScanReceipt")
            .field("final_handle", &self.final_handle)
            .field("lineage", &self.lineage)
            .field("outcome", &self.outcome)
            .finish()
    }
}

impl ScanNegotiationOutcome {
    pub const fn node_id(&self) -> i32 {
        self.scan.node_id()
    }

    pub const fn scan_identity(&self) -> SealedScanIdentity {
        self.scan
    }

    pub fn projected_columns(&self) -> &[OutputColumn] {
        &self.projected_columns
    }

    pub const fn projection_applied(&self) -> bool {
        self.projection_applied
    }

    #[cfg(test)]
    fn accepted_for_test(contract: &SealedScanContract) -> Self {
        Self {
            scan: contract.identity(),
            binding: contract.binding(),
            residual_predicate_ordinals: Arc::from([]),
            projected_columns: contract.projected_columns().into(),
            projection_applied: true,
            offered_limit: contract.offered_limit(),
            limit_guaranteed: false,
        }
    }
}

/// Stateful adapter that derives scan responsibility from the sealed SQL scan
/// and the actual typed Connector responses. Callers cannot directly set the
/// projection or limit claims in the resulting outcome.
struct ScanNegotiationAdapter {
    contract: SealedScanContract,
    unlowerable_predicate_ordinals: Vec<usize>,
    residual_predicate_ordinals: Option<Vec<usize>>,
    projection_applied: Option<bool>,
    limit_guaranteed: Option<bool>,
}

impl ScanNegotiationAdapter {
    fn begin(
        contract: SealedScanContract,
        mut unlowerable_predicate_ordinals: Vec<usize>,
    ) -> Result<Self, String> {
        unlowerable_predicate_ordinals.sort_unstable();
        if unlowerable_predicate_ordinals
            .windows(2)
            .any(|pair| pair[0] == pair[1])
            || unlowerable_predicate_ordinals
                .iter()
                .any(|ordinal| *ordinal >= contract.predicate_count())
        {
            return Err(format!(
                "scan node {} has invalid unlowerable predicate ordinals",
                contract.node_id()
            ));
        }
        Ok(Self {
            contract,
            unlowerable_predicate_ordinals,
            residual_predicate_ordinals: None,
            projection_applied: None,
            limit_guaranteed: None,
        })
    }

    fn record_filter_response(
        &mut self,
        response: Option<&ConnectorReadFilterApplication>,
    ) -> Result<(), String> {
        if self.residual_predicate_ordinals.is_some() {
            return Err(format!(
                "scan node {} recorded its filter response more than once",
                self.contract.node_id()
            ));
        }
        self.residual_predicate_ordinals = Some(if response.is_some() {
            self.unlowerable_predicate_ordinals.clone()
        } else {
            (0..self.contract.predicate_count()).collect()
        });
        Ok(())
    }

    fn record_projection_response(
        &mut self,
        response: Option<&ConnectorReadTableHandle>,
    ) -> Result<(), String> {
        if self
            .projection_applied
            .replace(response.is_some())
            .is_some()
        {
            return Err(format!(
                "scan node {} recorded its projection response more than once",
                self.contract.node_id()
            ));
        }
        Ok(())
    }

    fn record_limit_response(
        &mut self,
        offered_limit: Option<u64>,
        response: Option<&ConnectorReadLimitApplication>,
    ) -> Result<(), String> {
        if self.contract.offered_limit() != offered_limit.is_some() {
            return Err(format!(
                "scan node {} limit offer differs from its sealed SQL contract",
                self.contract.node_id()
            ));
        }
        if offered_limit.is_none() && response.is_some() {
            return Err(format!(
                "scan node {} returned a limit response without an offer",
                self.contract.node_id()
            ));
        }
        let guaranteed = response.is_some_and(ConnectorReadLimitApplication::limit_guaranteed);
        if self.limit_guaranteed.replace(guaranteed).is_some() {
            return Err(format!(
                "scan node {} recorded its limit response more than once",
                self.contract.node_id()
            ));
        }
        Ok(())
    }

    fn finish(self) -> Result<ScanNegotiationOutcome, String> {
        Ok(ScanNegotiationOutcome {
            scan: self.contract.identity(),
            binding: self.contract.binding(),
            residual_predicate_ordinals: self
                .residual_predicate_ordinals
                .ok_or_else(|| "scan negotiation has no filter response".to_string())?
                .into(),
            projected_columns: self.contract.projected_columns().into(),
            projection_applied: self
                .projection_applied
                .ok_or_else(|| "scan negotiation has no projection response".to_string())?,
            offered_limit: self.contract.offered_limit(),
            limit_guaranteed: self
                .limit_guaranteed
                .ok_or_else(|| "scan negotiation has no limit response".to_string())?,
        })
    }
}

/// One closed Connector negotiation exchange for one exact SQL scan.
///
/// The session owns the evolving table handle and invokes the Connector
/// metadata service itself. Callers can offer requests and inspect returned
/// residual facts, but cannot attach a response produced by another query,
/// scan, binding, handle generation, or Connector call.
pub struct ScanNegotiationSession<'a> {
    metadata: &'a dyn ConnectorReadMetadata,
    connector_session: &'a ConnectorSession,
    preparation_budget: &'a PreparationBudget,
    relation_name: &'a str,
    handle: ConnectorReadTableHandle,
    lineage: PlanScanBinding,
    expected_read_binding: novarocks_spi::connector::read_stack::ConnectorReadBinding,
    outcome: ScanNegotiationAdapter,
}

impl<'a> ScanNegotiationSession<'a> {
    pub fn begin(
        subject: AdmittedScanSubject,
        unlowerable_predicate_ordinals: Vec<usize>,
        metadata: &'a dyn ConnectorReadMetadata,
        connector_session: &'a ConnectorSession,
        preparation_budget: &'a PreparationBudget,
        relation_name: &'a str,
    ) -> Result<Self, String> {
        let AdmittedScanSubject {
            contract,
            lineage,
            expected_read_binding,
            handle,
        } = subject;
        Ok(Self {
            metadata,
            connector_session,
            preparation_budget,
            relation_name,
            handle,
            lineage,
            expected_read_binding,
            outcome: ScanNegotiationAdapter::begin(contract, unlowerable_predicate_ordinals)?,
        })
    }

    fn accept_handle(&mut self, handle: ConnectorReadTableHandle) -> Result<(), String> {
        if handle.binding() != &self.expected_read_binding {
            return Err(format!(
                "typed scan negotiation on relation {} returned a handle from another admitted runtime generation",
                self.relation_name
            ));
        }
        self.handle = handle;
        Ok(())
    }

    fn observe<T>(
        &self,
        operation: &str,
        call: impl FnOnce() -> Result<T, novarocks_spi::connector::ConnectorError>,
    ) -> Result<T, String> {
        self.preparation_budget
            .begin_negotiation(
                self.relation_name
                    .len()
                    .checked_add(operation.len())
                    .ok_or_else(|| "Connector negotiation request size overflowed".to_string())?,
            )
            .map_err(|error| error.to_string())?;
        let response = call().map_err(|error| {
            format!(
                "typed scan {operation} on relation {} failed: {error}",
                self.relation_name
            )
        })?;
        self.preparation_budget
            .charge_unmeasured_response_weight()
            .map_err(|error| error.to_string())?;
        Ok(response)
    }

    pub fn apply_filter(
        &mut self,
        constraint: &ConnectorReadConstraint,
    ) -> Result<Option<ConnectorReadFilterApplication>, String> {
        let response = self.observe("apply_filter", || {
            self.metadata
                .apply_filter(self.connector_session, &self.handle, constraint)
        })?;
        self.outcome.record_filter_response(response.as_ref())?;
        if let Some(application) = &response {
            self.accept_handle(application.handle().clone())?;
        }
        Ok(response)
    }

    pub fn apply_projection(
        &mut self,
        assignments: &[ConnectorReadAssignment],
    ) -> Result<bool, String> {
        let response = self.observe("apply_projection", || {
            self.metadata
                .apply_projection(self.connector_session, &self.handle, assignments)
        })?;
        self.outcome.record_projection_response(response.as_ref())?;
        if let Some(handle) = response {
            self.accept_handle(handle)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn apply_limit(&mut self, limit: Option<u64>) -> Result<bool, String> {
        let response = match limit {
            Some(limit) => self.observe("apply_limit", || {
                self.metadata
                    .apply_limit(self.connector_session, &self.handle, limit)
            })?,
            None => None,
        };
        self.outcome
            .record_limit_response(limit, response.as_ref())?;
        let guaranteed = response
            .as_ref()
            .is_some_and(ConnectorReadLimitApplication::limit_guaranteed);
        if let Some(application) = response {
            self.accept_handle(application.into_handle())?;
        }
        Ok(guaranteed)
    }

    pub fn finish(self) -> Result<NegotiatedScanReceipt, String> {
        Ok(NegotiatedScanReceipt {
            final_handle: Some(self.handle),
            lineage: self.lineage,
            outcome: self.outcome.finish()?,
        })
    }
}

pub struct FrozenExecutionDescriptionDraft {
    kind: QueryExecutionKind,
    plan: SealedPreparationPlan,
    bindings: Vec<PlanScanBinding>,
    mv_candidate_match: Option<StrictMvCandidateMatch>,
    effect: ExecutionEffect,
    recovery: RecoveryMode,
    scan_receipts: Vec<NegotiatedScanReceipt>,
    cost: FrozenCostEstimate,
    resources: ExecutionResourceRequirements,
}

impl FrozenExecutionDescriptionDraft {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        kind: QueryExecutionKind,
        plan: SealedPreparationPlan,
        bindings: Vec<PlanScanBinding>,
        mv_candidate_match: Option<StrictMvCandidateMatch>,
        effect: ExecutionEffect,
        recovery: RecoveryMode,
        scan_receipts: Vec<NegotiatedScanReceipt>,
        cost: FrozenCostEstimate,
        resources: ExecutionResourceRequirements,
    ) -> Self {
        Self {
            kind,
            plan,
            bindings,
            mv_candidate_match,
            effect,
            recovery,
            scan_receipts,
            cost,
            resources,
        }
    }
}

/// Immutable semantic input for all attempts of one logical execution.
#[derive(Clone, Debug)]
pub struct FrozenExecutionDescription {
    kind: QueryExecutionKind,
    plan: DistributedPlan,
    bindings: Arc<[PlanScanBinding]>,
    mv_candidate_match: Option<StrictMvCandidateMatch>,
    output: OutputContract,
    effect: ExecutionEffect,
    recovery: RecoveryMode,
    residuals: Arc<[ResidualResponsibility]>,
    cost: FrozenCostEstimate,
    resources: ExecutionResourceRequirements,
}

impl FrozenExecutionDescription {
    pub fn try_freeze(draft: FrozenExecutionDescriptionDraft) -> Result<Self, String> {
        if draft.effect == ExecutionEffect::External && draft.recovery != RecoveryMode::NoRecovery {
            return Err("execution with external effects must use NoRecovery".to_string());
        }
        if draft.mv_candidate_match.is_some() && draft.effect != ExecutionEffect::None {
            return Err("MV candidate match is valid only without external effects".to_string());
        }
        let output = OutputContract::from_plan(draft.kind, draft.plan.plan())?;
        let scan_contracts = draft.plan.scan_contracts()?;
        let expected_scan_ids = scan_contracts
            .iter()
            .map(SealedScanContract::identity)
            .collect::<std::collections::BTreeSet<_>>();
        let mut binding_scan_ids = std::collections::BTreeSet::new();
        let mut occurrence_ids = std::collections::BTreeSet::new();
        for binding in &draft.bindings {
            if !binding_scan_ids.insert(binding.scan) {
                return Err(format!(
                    "query execution description repeats binding for scan node {}",
                    binding.node_id()
                ));
            }
            if !occurrence_ids.insert(binding.occurrence.occurrence()) {
                return Err("query execution description repeats a relation occurrence".to_string());
            }
            let contract = scan_contracts
                .iter()
                .find(|contract| contract.identity() == binding.scan)
                .ok_or_else(|| {
                    format!(
                        "query execution description has a binding for unknown scan node {}",
                        binding.node_id()
                    )
                })?;
            if binding.binding != contract.binding() {
                return Err(format!(
                    "query execution binding for scan node {} does not match its sealed SQL binding identity",
                    binding.node_id()
                ));
            }
            let object_parts = binding.occurrence.binding().object().parts();
            if object_parts.len() != 3
                || object_parts[0].as_ref() != contract.catalog()
                || object_parts[1].as_ref() != contract.namespace()
                || object_parts[2].as_ref() != contract.table()
            {
                return Err(format!(
                    "query execution binding for scan node {} names an object different from its sealed SQL source",
                    binding.node_id()
                ));
            }
        }
        if binding_scan_ids != expected_scan_ids {
            return Err(
                "query execution bindings do not exactly cover every sealed scan occurrence"
                    .to_string(),
            );
        }
        let rewritten_scan_count = scan_contracts
            .iter()
            .filter(|contract| contract.was_mv_rewritten())
            .count();
        if rewritten_scan_count > 1 {
            return Err("query execution description has multiple MV rewrite targets".to_string());
        }
        if rewritten_scan_count == 1 && draft.mv_candidate_match.is_none() {
            return Err("optimizer MV rewrite has no exact candidate selection proof".to_string());
        }
        if rewritten_scan_count == 0 && draft.mv_candidate_match.is_some() {
            return Err("MV candidate proof has no optimizer rewrite target".to_string());
        }
        if let Some(candidate_match) = &draft.mv_candidate_match {
            let target = draft
                .bindings
                .iter()
                .find(|binding| binding.scan_identity() == candidate_match.target_scan())
                .ok_or_else(|| {
                    "MV candidate match does not name a scan in the final sealed plan".to_string()
                })?;
            let target_contract = scan_contracts
                .iter()
                .find(|contract| contract.identity() == candidate_match.target_scan())
                .expect("target binding and contract covers are equal");
            let actual_action = target_contract.mv_rewrite_action().ok_or_else(|| {
                "MV candidate match target has no optimizer rewrite action".to_string()
            })?;
            if &actual_action != candidate_match.rewrite_action() {
                return Err("MV candidate match rewrite action belongs to another plan".to_string());
            }
            if target.occurrence.binding() != candidate_match.output_binding() {
                return Err(
                    "MV candidate output does not match the final sealed plan target binding"
                        .to_string(),
                );
            }
        }
        let mut seen = std::collections::BTreeSet::new();
        let mut residuals = std::collections::BTreeSet::new();
        for receipt in &draft.scan_receipts {
            let outcome = receipt.outcome();
            let lineage = receipt.lineage();
            let admitted = draft
                .bindings
                .iter()
                .find(|binding| binding.scan_identity() == outcome.scan_identity())
                .ok_or_else(|| {
                    format!(
                        "scan node {} negotiation receipt has no admitted plan binding",
                        outcome.node_id()
                    )
                })?;
            if lineage.scan != outcome.scan
                || lineage.binding != outcome.binding
                || lineage.scan != admitted.scan
                || lineage.binding != admitted.binding
                || lineage.occurrence.sql_occurrence() != admitted.occurrence.sql_occurrence()
                || lineage.occurrence.binding() != admitted.occurrence.binding()
            {
                return Err(format!(
                    "scan node {} negotiation receipt belongs to another admitted scan lineage",
                    outcome.node_id()
                ));
            }
            if !seen.insert(outcome.scan) {
                return Err(format!(
                    "query execution description repeats scan node {}",
                    outcome.node_id()
                ));
            }
            let contract = scan_contracts
                .iter()
                .find(|contract| contract.identity() == outcome.scan)
                .ok_or_else(|| {
                    format!(
                        "query execution description has an outcome for unknown scan node {}",
                        outcome.node_id()
                    )
                })?;
            let predicate_count = contract.predicate_count();
            if outcome
                .residual_predicate_ordinals
                .iter()
                .any(|ordinal| *ordinal >= predicate_count)
            {
                return Err(format!(
                    "scan node {} residual contract does not match the frozen plan",
                    outcome.node_id()
                ));
            }
            if outcome.binding != contract.binding()
                || outcome.offered_limit != contract.offered_limit()
                || !same_output_columns(&outcome.projected_columns, contract.projected_columns())
            {
                return Err(format!(
                    "scan node {} negotiation contract does not match the frozen plan",
                    outcome.node_id()
                ));
            }
            if !outcome.residual_predicate_ordinals.is_empty() {
                residuals.insert(ResidualResponsibility::EngineFilter);
            }
            if outcome.offered_limit && !outcome.limit_guaranteed {
                residuals.insert(ResidualResponsibility::EngineLimit);
            }
        }
        if seen != expected_scan_ids {
            return Err(
                "query execution negotiation outcomes do not exactly cover every sealed scan"
                    .to_string(),
            );
        }
        if draft.effect == ExecutionEffect::External {
            residuals.insert(ResidualResponsibility::EffectCommit);
        }
        Ok(Self {
            kind: draft.kind,
            plan: draft.plan.plan().clone(),
            bindings: draft.bindings.into(),
            mv_candidate_match: draft.mv_candidate_match,
            output,
            effect: draft.effect,
            recovery: draft.recovery,
            residuals: residuals.into_iter().collect::<Vec<_>>().into(),
            cost: draft.cost,
            resources: draft.resources,
        })
    }

    pub const fn kind(&self) -> QueryExecutionKind {
        self.kind
    }
    pub const fn plan(&self) -> &DistributedPlan {
        &self.plan
    }
    pub fn bindings(&self) -> &[PlanScanBinding] {
        &self.bindings
    }
    pub const fn mv_candidate_match(&self) -> Option<&StrictMvCandidateMatch> {
        self.mv_candidate_match.as_ref()
    }
    pub const fn output(&self) -> &OutputContract {
        &self.output
    }
    pub const fn effect(&self) -> ExecutionEffect {
        self.effect
    }
    pub const fn recovery(&self) -> RecoveryMode {
        self.recovery
    }
    pub fn residuals(&self) -> &[ResidualResponsibility] {
        &self.residuals
    }
    pub const fn cost(&self) -> FrozenCostEstimate {
        self.cost
    }
    pub const fn resources(&self) -> ExecutionResourceRequirements {
        self.resources
    }
}

fn same_output_columns(left: &[OutputColumn], right: &[OutputColumn]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.column_id == right.column_id
                && left.name == right.name
                && left.data_type == right.data_type
                && left.nullable == right.nullable
                && left.is_internal == right.is_internal
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks_sql::binding::SqlTableBindingAllocator;
    use novarocks_sql::test_support::{
        NativePreparationFixture, NativeScanFixture, native_preparation_plan, native_scan_plan,
    };

    use super::*;
    use crate::api::{
        CatalogGeneration, DataVersion, ExactBindingReceiptStore, ExactObjectBinding,
        ObjectIdentity, ObjectPath, ProviderFactFormat,
    };

    fn format(kind: &str) -> ProviderFactFormat {
        ProviderFactFormat::try_new("iceberg-rest", kind).unwrap()
    }

    fn binding(catalog: &str, namespace: &str, table: &str, version: u8) -> ExactObjectBinding {
        ExactObjectBinding::new_for_test(
            ObjectPath::try_new([catalog, namespace, table]).unwrap(),
            CatalogGeneration::try_new(format("catalog-generation/v1"), Arc::<[u8]>::from([1]))
                .unwrap(),
            ObjectIdentity::try_new(format("table-uuid/v1"), Arc::<[u8]>::from([2])).unwrap(),
            DataVersion::try_new(format("snapshot-id/v1"), Arc::<[u8]>::from([version])).unwrap(),
        )
    }

    fn test_bindings(
        plan: &SealedPreparationPlan,
        replacement_table: Option<&str>,
    ) -> Vec<PlanScanBinding> {
        let contracts = plan.scan_contracts().unwrap();
        let allocator =
            SqlTableBindingAllocator::try_new_for_test(contracts[0].binding().scope().get())
                .unwrap();
        let receipts = ExactBindingReceiptStore::new(&allocator);
        for scan in &contracts {
            receipts.register_for_test(
                scan.binding(),
                binding(
                    scan.catalog(),
                    scan.namespace(),
                    replacement_table.unwrap_or_else(|| scan.table()),
                    101,
                ),
            );
        }
        resolve_plan_scan_bindings(plan, &receipts.seal()).unwrap()
    }

    fn scan_draft(
        plan: DistributedPlan,
        effect: ExecutionEffect,
        recovery: RecoveryMode,
    ) -> FrozenExecutionDescriptionDraft {
        let plan = SealedPreparationPlan::seal(plan);
        let contracts = plan.scan_contracts().unwrap();
        let bindings = test_bindings(&plan, None);
        let scan_receipts = contracts
            .iter()
            .map(|contract| {
                let lineage = bindings
                    .iter()
                    .find(|binding| binding.scan_identity() == contract.identity())
                    .unwrap()
                    .clone();
                NegotiatedScanReceipt::accepted_for_test(contract, lineage)
            })
            .collect();
        FrozenExecutionDescriptionDraft::new(
            QueryExecutionKind::Read,
            plan,
            bindings,
            None,
            effect,
            recovery,
            scan_receipts,
            FrozenCostEstimate::default(),
            ExecutionResourceRequirements::default(),
        )
    }

    #[test]
    fn freeze_requires_exact_binding_and_negotiation_scan_cover() {
        let plan = native_scan_plan(NativeScanFixture::ConnectorRead).unwrap();
        let mut draft = scan_draft(plan, ExecutionEffect::None, RecoveryMode::NoRecovery);
        draft.bindings.clear();
        assert!(FrozenExecutionDescription::try_freeze(draft).is_err());

        let plan = native_scan_plan(NativeScanFixture::ConnectorRead).unwrap();
        let mut draft = scan_draft(plan, ExecutionEffect::None, RecoveryMode::NoRecovery);
        draft.scan_receipts.clear();
        assert!(FrozenExecutionDescription::try_freeze(draft).is_err());
    }

    #[test]
    fn same_source_self_join_freezes_two_distinct_scan_occurrences() {
        let draft = scan_draft(
            novarocks_sql::test_support::native_self_join_scan_plan().unwrap(),
            ExecutionEffect::None,
            RecoveryMode::NoRecovery,
        );
        assert_eq!(draft.bindings.len(), 2);
        assert_eq!(
            draft.bindings[0].occurrence().binding(),
            draft.bindings[1].occurrence().binding()
        );
        assert_ne!(
            draft.bindings[0].occurrence().occurrence(),
            draft.bindings[1].occurrence().occurrence()
        );
        assert!(FrozenExecutionDescription::try_freeze(draft).is_ok());
    }

    #[test]
    fn freeze_rejects_negotiation_outcome_from_structurally_identical_query() {
        let source = scan_draft(
            native_scan_plan(NativeScanFixture::ConnectorRead).unwrap(),
            ExecutionEffect::None,
            RecoveryMode::NoRecovery,
        );
        let mut target = scan_draft(
            native_scan_plan(NativeScanFixture::ConnectorRead).unwrap(),
            ExecutionEffect::None,
            RecoveryMode::NoRecovery,
        );
        assert_eq!(
            source.scan_receipts[0].outcome().node_id(),
            target.scan_receipts[0].outcome().node_id()
        );
        target.scan_receipts = source.scan_receipts;
        assert!(FrozenExecutionDescription::try_freeze(target).is_err());
    }

    #[test]
    fn negotiation_adapter_derives_declined_responsibility_from_actual_responses() {
        let plan = native_scan_plan(NativeScanFixture::ConnectorRead).unwrap();
        let contract = SealedPreparationPlan::seal(plan)
            .scan_contracts()
            .unwrap()
            .remove(0);
        let predicate_count = contract.predicate_count();
        let offered_limit = contract.offered_limit().then_some(1);
        let mut adapter = ScanNegotiationAdapter::begin(contract, Vec::new()).unwrap();
        adapter.record_filter_response(None).unwrap();
        adapter.record_projection_response(None).unwrap();
        adapter.record_limit_response(offered_limit, None).unwrap();
        let outcome = adapter.finish().unwrap();
        assert_eq!(outcome.residual_predicate_ordinals.len(), predicate_count);
        assert!(!outcome.projection_applied);
        assert!(!outcome.limit_guaranteed);
    }

    #[test]
    fn freeze_rejects_a_binding_token_paired_with_another_object() {
        let plan = native_scan_plan(NativeScanFixture::ConnectorRead).unwrap();
        let draft = scan_draft(plan, ExecutionEffect::None, RecoveryMode::NoRecovery);
        let plan = draft.plan.clone();
        let contracts = plan.scan_contracts().unwrap();
        let allocator =
            SqlTableBindingAllocator::try_new_for_test(contracts[0].binding().scope().get())
                .unwrap();
        let receipts = ExactBindingReceiptStore::new(&allocator);
        receipts.register_for_test(
            contracts[0].binding(),
            binding("ice", "ns", "other_table", 101),
        );
        assert!(resolve_plan_scan_bindings(&plan, &receipts.seal()).is_err());
        assert!(FrozenExecutionDescription::try_freeze(draft).is_ok());
    }

    #[test]
    fn freeze_derives_projection_and_external_effect_recovery_rules() {
        let plan = native_scan_plan(NativeScanFixture::ConnectorRead).unwrap();
        let description = FrozenExecutionDescription::try_freeze(scan_draft(
            plan,
            ExecutionEffect::None,
            RecoveryMode::RestartAttemptBeforeVisibility,
        ))
        .unwrap();
        assert!(!description.output().columns().is_empty());

        let plan = native_scan_plan(NativeScanFixture::ConnectorRead).unwrap();
        assert!(
            FrozenExecutionDescription::try_freeze(scan_draft(
                plan,
                ExecutionEffect::External,
                RecoveryMode::RestartAttemptBeforeVisibility,
            ))
            .is_err()
        );
    }

    #[test]
    fn mv_candidate_match_must_name_the_final_plan_target_binding() {
        use crate::{
            api::{MvCandidateFact, MvPublicationId, QueryConsistency},
            preparation::{MvInputMatch, prove_strict_mv_candidate_match},
        };

        let pre_plan = SealedPreparationPlan::seal(
            native_scan_plan(NativeScanFixture::ConnectorRead).unwrap(),
        );
        let pre_bindings = test_bindings(&pre_plan, None);
        let plan = novarocks_sql::test_support::native_mv_rewritten_scan_plan().unwrap();
        let mut draft = scan_draft(plan, ExecutionEffect::None, RecoveryMode::NoRecovery);
        let target = draft.bindings[0].clone();
        let query_occurrences = vec![pre_bindings[0].occurrence().clone()];
        let mapping = [MvInputMatch::new(query_occurrences[0].occurrence(), 0)];
        let rewrite_action = draft
            .plan
            .scan_contracts()
            .unwrap()
            .remove(0)
            .mv_rewrite_action()
            .unwrap();
        let candidate = MvCandidateFact::try_new_for_test(
            MvPublicationId::try_new([7; 16]).unwrap(),
            [9; 32],
            "test-definition",
            &[query_occurrences[0].binding().clone()],
            target.occurrence().binding(),
        )
        .unwrap();
        draft.mv_candidate_match = Some(
            prove_strict_mv_candidate_match(
                QueryConsistency::Strict,
                &query_occurrences,
                &mapping,
                &candidate,
                rewrite_action,
            )
            .unwrap(),
        );
        assert!(FrozenExecutionDescription::try_freeze(draft).is_ok());

        let pre_plan = SealedPreparationPlan::seal(
            native_scan_plan(NativeScanFixture::ConnectorRead).unwrap(),
        );
        let pre_bindings = test_bindings(&pre_plan, None);
        let plan = novarocks_sql::test_support::native_mv_rewritten_scan_plan().unwrap();
        let mut draft = scan_draft(plan, ExecutionEffect::None, RecoveryMode::NoRecovery);
        let target = draft.bindings[0].clone();
        let query_occurrences = vec![pre_bindings[0].occurrence().clone()];
        let mapping = [MvInputMatch::new(query_occurrences[0].occurrence(), 0)];
        let rewrite_action = draft
            .plan
            .scan_contracts()
            .unwrap()
            .remove(0)
            .mv_rewrite_action()
            .unwrap();
        let candidate = MvCandidateFact::try_new_for_test(
            MvPublicationId::try_new([7; 16]).unwrap(),
            [9; 32],
            "test-definition",
            &[target.occurrence().binding().clone()],
            &binding("ice", "ns", "different_target", 101),
        )
        .unwrap();
        draft.mv_candidate_match = Some(
            prove_strict_mv_candidate_match(
                QueryConsistency::Strict,
                &query_occurrences,
                &mapping,
                &candidate,
                rewrite_action,
            )
            .unwrap(),
        );
        assert!(FrozenExecutionDescription::try_freeze(draft).is_err());
    }

    #[test]
    fn mv_candidate_match_cannot_move_between_identical_final_plans() {
        use crate::{
            api::{MvCandidateFact, MvPublicationId, QueryConsistency},
            preparation::{MvInputMatch, prove_strict_mv_candidate_match},
        };

        let pre_plan = SealedPreparationPlan::seal(
            native_scan_plan(NativeScanFixture::ConnectorRead).unwrap(),
        );
        let pre_bindings = test_bindings(&pre_plan, None);
        let source = pre_bindings[0].occurrence().clone();
        let occurrences = [source.clone()];
        let mapping = [MvInputMatch::new(source.occurrence(), 0)];
        let source_final = scan_draft(
            novarocks_sql::test_support::native_mv_rewritten_scan_plan().unwrap(),
            ExecutionEffect::None,
            RecoveryMode::NoRecovery,
        );
        let rewrite_action = source_final.plan.scan_contracts().unwrap()[0]
            .mv_rewrite_action()
            .unwrap();
        let mut target_final = scan_draft(
            novarocks_sql::test_support::native_mv_rewritten_scan_plan().unwrap(),
            ExecutionEffect::None,
            RecoveryMode::NoRecovery,
        );
        let candidate = MvCandidateFact::try_new_for_test(
            MvPublicationId::try_new([7; 16]).unwrap(),
            [9; 32],
            "test-definition",
            &[source.binding().clone()],
            target_final.bindings[0].occurrence().binding(),
        )
        .unwrap();
        target_final.mv_candidate_match = Some(
            prove_strict_mv_candidate_match(
                QueryConsistency::Strict,
                &occurrences,
                &mapping,
                &candidate,
                rewrite_action,
            )
            .unwrap(),
        );
        assert!(FrozenExecutionDescription::try_freeze(target_final).is_err());
    }

    #[test]
    fn non_read_execution_may_have_completion_only_output() {
        let plan = native_preparation_plan(NativePreparationFixture::MissingResultOutput).unwrap();
        let description =
            FrozenExecutionDescription::try_freeze(FrozenExecutionDescriptionDraft::new(
                QueryExecutionKind::Maintenance,
                SealedPreparationPlan::seal(plan),
                Vec::new(),
                None,
                ExecutionEffect::External,
                RecoveryMode::NoRecovery,
                Vec::new(),
                FrozenCostEstimate::default(),
                ExecutionResourceRequirements::default(),
            ))
            .unwrap();
        assert!(matches!(
            description.output(),
            OutputContract::CompletionOnly
        ));
    }
}
