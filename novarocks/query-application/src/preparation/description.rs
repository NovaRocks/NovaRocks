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

use std::sync::Arc;

use novarocks_spi::connector::read_stack::runtime::ConnectorReadAssignment;
use novarocks_spi::connector::read_stack::{
    ConnectorReadConstraint, ConnectorReadFilterApplication, ConnectorReadLimitApplication,
    ConnectorReadMetadata, ConnectorReadTableHandle, ConnectorSession,
};
use novarocks_sql::{
    plan_read::{DistributedPlan, OutputColumn},
    planning::query_execution::{
        SealedPreparationPlan, SealedPreparationPlanId, SealedScanContract, SealedScanIdentity,
        SqlExecutionSchedulingFacts, project_execution_scheduling_facts,
    },
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrozenEstimateUnknownReason {
    MissingRootFragment,
    FallbackRowEstimate,
    MissingCostEstimate,
    NonFinite,
    Negative,
    NotProjected,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FrozenCostValue {
    Known(f64),
    Unknown(FrozenEstimateUnknownReason),
}

impl FrozenCostValue {
    pub const fn known(self) -> Option<f64> {
        match self {
            Self::Known(value) => Some(value),
            Self::Unknown(_) => None,
        }
    }

    pub const fn unknown_reason(self) -> Option<FrozenEstimateUnknownReason> {
        match self {
            Self::Known(_) => None,
            Self::Unknown(reason) => Some(reason),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FrozenCostEstimate {
    root_rows: FrozenCostValue,
    cpu: FrozenCostValue,
    memory: FrozenCostValue,
    network: FrozenCostValue,
}

impl FrozenCostEstimate {
    pub const fn new(
        root_rows: FrozenCostValue,
        cpu: FrozenCostValue,
        memory: FrozenCostValue,
        network: FrozenCostValue,
    ) -> Self {
        Self {
            root_rows,
            cpu,
            memory,
            network,
        }
    }

    pub const fn unknown(reason: FrozenEstimateUnknownReason) -> Self {
        Self::new(
            FrozenCostValue::Unknown(reason),
            FrozenCostValue::Unknown(reason),
            FrozenCostValue::Unknown(reason),
            FrozenCostValue::Unknown(reason),
        )
    }

    pub const fn root_rows(self) -> FrozenCostValue {
        self.root_rows
    }

    pub const fn cpu(self) -> FrozenCostValue {
        self.cpu
    }

    pub const fn memory(self) -> FrozenCostValue {
        self.memory
    }

    pub const fn network(self) -> FrozenCostValue {
        self.network
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrozenResourceValue {
    Known(u64),
    Unknown(FrozenEstimateUnknownReason),
}

impl FrozenResourceValue {
    pub const fn known(self) -> Option<u64> {
        match self {
            Self::Known(value) => Some(value),
            Self::Unknown(_) => None,
        }
    }

    pub const fn unknown_reason(self) -> Option<FrozenEstimateUnknownReason> {
        match self {
            Self::Known(_) => None,
            Self::Unknown(reason) => Some(reason),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionResourceRequirements {
    minimum_memory_bytes: FrozenResourceValue,
    result_credit_bytes: FrozenResourceValue,
    spill_bytes: FrozenResourceValue,
}

impl ExecutionResourceRequirements {
    pub const fn new(
        minimum_memory_bytes: FrozenResourceValue,
        result_credit_bytes: FrozenResourceValue,
        spill_bytes: FrozenResourceValue,
    ) -> Self {
        Self {
            minimum_memory_bytes,
            result_credit_bytes,
            spill_bytes,
        }
    }

    pub const fn unknown(reason: FrozenEstimateUnknownReason) -> Self {
        Self::new(
            FrozenResourceValue::Unknown(reason),
            FrozenResourceValue::Unknown(reason),
            FrozenResourceValue::Unknown(reason),
        )
    }

    pub const fn minimum_memory_bytes(self) -> FrozenResourceValue {
        self.minimum_memory_bytes
    }

    pub const fn result_credit_bytes(self) -> FrozenResourceValue {
        self.result_credit_bytes
    }

    pub const fn spill_bytes(self) -> FrozenResourceValue {
        self.spill_bytes
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
    final_handle: ConnectorReadTableHandle,
    lineage: PlanScanBinding,
    outcome: ScanNegotiationOutcome,
    offered_constraint: ConnectorReadConstraint,
}

impl NegotiatedScanReceipt {
    pub const fn final_handle(&self) -> &ConnectorReadTableHandle {
        &self.final_handle
    }

    pub const fn lineage(&self) -> &PlanScanBinding {
        &self.lineage
    }

    pub const fn outcome(&self) -> &ScanNegotiationOutcome {
        &self.outcome
    }

    pub const fn offered_constraint(&self) -> &ConnectorReadConstraint {
        &self.offered_constraint
    }

    #[cfg(test)]
    fn accepted_for_test(
        contract: &SealedScanContract,
        lineage: PlanScanBinding,
        final_handle: ConnectorReadTableHandle,
    ) -> Self {
        Self {
            final_handle,
            lineage,
            outcome: ScanNegotiationOutcome::accepted_for_test(contract),
            offered_constraint: ConnectorReadConstraint::of_summary(
                novarocks_spi::connector::read_stack::TupleDomain::all(),
            ),
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
            .field("offered_constraint", &self.offered_constraint)
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

    pub fn residual_predicate_ordinals(&self) -> &[usize] {
        &self.residual_predicate_ordinals
    }

    pub const fn offered_limit(&self) -> bool {
        self.offered_limit
    }

    pub const fn limit_guaranteed(&self) -> bool {
        self.limit_guaranteed
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
    offered_constraint: Option<ConnectorReadConstraint>,
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
            offered_constraint: None,
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
        if self.offered_constraint.is_some() {
            return Err(format!(
                "scan node {} offered its filter contract more than once",
                self.outcome.contract.node_id()
            ));
        }
        let response = self.observe("apply_filter", || {
            self.metadata
                .apply_filter(self.connector_session, &self.handle, constraint)
        })?;
        self.outcome.record_filter_response(response.as_ref())?;
        self.offered_constraint = Some(constraint.clone());
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
            final_handle: self.handle,
            lineage: self.lineage,
            outcome: self.outcome.finish()?,
            offered_constraint: self
                .offered_constraint
                .ok_or_else(|| "scan negotiation has no offered constraint".to_string())?,
        })
    }
}

/// Immutable per-scan input retained by one logical execution.
///
/// It keeps the exact SQL occurrence, the final opaque Connector handle and
/// the constraint that produced that handle together. Attempts may use this
/// value only to request a fresh execution access capability; they must not
/// rerun planning negotiation or select another table version.
#[derive(Clone, Debug)]
pub struct FrozenScanDescription {
    lineage: PlanScanBinding,
    final_handle: ConnectorReadTableHandle,
    offered_constraint: ConnectorReadConstraint,
    outcome: ScanNegotiationOutcome,
}

impl FrozenScanDescription {
    fn from_receipt(receipt: NegotiatedScanReceipt) -> Self {
        Self {
            lineage: receipt.lineage,
            final_handle: receipt.final_handle,
            offered_constraint: receipt.offered_constraint,
            outcome: receipt.outcome,
        }
    }

    pub const fn scan_identity(&self) -> SealedScanIdentity {
        self.lineage.scan_identity()
    }

    pub const fn node_id(&self) -> i32 {
        self.lineage.node_id()
    }

    pub const fn lineage(&self) -> &PlanScanBinding {
        &self.lineage
    }

    pub const fn final_handle(&self) -> &ConnectorReadTableHandle {
        &self.final_handle
    }

    pub const fn offered_constraint(&self) -> &ConnectorReadConstraint {
        &self.offered_constraint
    }

    pub const fn outcome(&self) -> &ScanNegotiationOutcome {
        &self.outcome
    }
}

pub struct FrozenExecutionDescriptionDraft {
    kind: QueryExecutionKind,
    plan: SealedPreparationPlan,
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
// Design: ADR-0145 (docs/adr/ADR-0145-freeze-query-semantics-before-attempt-access.md)
#[derive(Clone, Debug)]
pub struct FrozenExecutionDescription {
    plan_seal: SealedPreparationPlanId,
    kind: QueryExecutionKind,
    plan: Arc<DistributedPlan>,
    scheduling: Arc<SqlExecutionSchedulingFacts>,
    scans: Arc<[FrozenScanDescription]>,
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
        for (name, value) in [
            ("root rows", draft.cost.root_rows()),
            ("cpu", draft.cost.cpu()),
            ("memory", draft.cost.memory()),
            ("network", draft.cost.network()),
        ] {
            if let FrozenCostValue::Known(value) = value
                && (!value.is_finite() || value < 0.0)
            {
                return Err(format!(
                    "frozen {name} cost must be finite and non-negative"
                ));
            }
        }
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
        for receipt in &draft.scan_receipts {
            let binding = receipt.lineage();
            if !binding_scan_ids.insert(binding.scan) {
                return Err(format!(
                    "query execution description repeats negotiation receipt for scan node {}",
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
                "query execution negotiation receipts do not exactly cover every sealed scan occurrence"
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
                .scan_receipts
                .iter()
                .map(NegotiatedScanReceipt::lineage)
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
            if lineage.scan != outcome.scan || lineage.binding != outcome.binding {
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
        let plan_seal = draft.plan.id();
        let scheduling = Arc::new(project_execution_scheduling_facts(&draft.plan)?);
        let scans = draft
            .scan_receipts
            .into_iter()
            .map(FrozenScanDescription::from_receipt)
            .collect::<Vec<_>>();
        let plan = draft.plan.into_shared_plan();
        Ok(Self {
            plan_seal,
            kind: draft.kind,
            plan,
            scheduling,
            scans: scans.into(),
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
    pub(crate) const fn plan_seal(&self) -> SealedPreparationPlanId {
        self.plan_seal
    }

    /// Borrowed affinity check for a role adapter that must atomically bind
    /// the description to its opaque Native template without exposing the
    /// seal as a reconstructible application value.
    pub fn matches_plan_seal(&self, seal: SealedPreparationPlanId) -> bool {
        self.plan_seal == seal
    }
    pub fn plan(&self) -> &DistributedPlan {
        self.plan.as_ref()
    }
    pub fn scheduling(&self) -> &SqlExecutionSchedulingFacts {
        self.scheduling.as_ref()
    }
    /// Share the one immutable plan owned by this logical execution without
    /// rebuilding or deep-cloning it for a replacement attempt.
    pub fn shared_plan(&self) -> Arc<DistributedPlan> {
        Arc::clone(&self.plan)
    }
    pub fn scans(&self) -> &[FrozenScanDescription] {
        &self.scans
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
pub(crate) mod tests {
    use std::sync::Arc;

    use novarocks_spi::connector::read_stack::adapter::{ProviderReadRuntime, ReadRuntimeAdapter};
    use novarocks_spi::connector::read_stack::{ColumnHandle, ConnectorSplit};
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceDescriptor, ConnectorInstanceId,
        ConnectorProviderId,
    };
    use novarocks_sql::binding::SqlTableBindingAllocator;
    use novarocks_sql::test_support::{
        NativePreparationFixture, NativeScanFixture, native_preparation_plan, native_scan_plan,
    };

    use super::*;
    use crate::api::{
        CatalogGeneration, DataVersion, ExactBindingReceiptStore, ExactObjectBinding,
        ObjectIdentity, ObjectPath, ProviderFactFormat,
    };

    #[derive(Clone, Debug)]
    struct FixtureTable;

    #[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct FixtureColumn;

    impl ColumnHandle for FixtureColumn {}

    #[derive(Debug)]
    struct FixtureSplit;

    impl ConnectorSplit for FixtureSplit {
        fn retained_size_in_bytes(&self) -> u64 {
            0
        }
    }

    struct FixtureReadRuntime {
        descriptor: ConnectorInstanceDescriptor,
        catalog: CatalogHandle,
    }

    impl ProviderReadRuntime for FixtureReadRuntime {
        type Table = FixtureTable;
        type Column = FixtureColumn;
        type Transaction = ();
        type Split = FixtureSplit;

        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }

        fn catalog_handle(&self) -> &CatalogHandle {
            &self.catalog
        }

        fn transaction(&self) -> Self::Transaction {}
    }

    fn fixture_table_handle() -> ConnectorReadTableHandle {
        let instance_id = ConnectorInstanceId::parse("fixture-catalog").unwrap();
        let runtime = FixtureReadRuntime {
            descriptor: ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("fixture-provider").unwrap(),
                instance_id: instance_id.clone(),
            },
            catalog: CatalogHandle::new(instance_id, CatalogVersion::from_bytes([7; 32])),
        };
        ReadRuntimeAdapter::new(Arc::new(runtime)).wrap_table(FixtureTable)
    }

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

    fn publication_binding(
        relation: &novarocks_sql::compiler::SqlMvRewritePublicationRelation,
    ) -> ExactObjectBinding {
        ExactObjectBinding::new_for_publication_test(
            ObjectPath::try_new(relation.table_fqn().split('.')).unwrap(),
            CatalogGeneration::try_new(format("catalog-generation/v1"), Arc::<[u8]>::from([1]))
                .unwrap(),
            relation,
        )
    }

    fn attach_strict_mv_proof(draft: &mut FrozenExecutionDescriptionDraft) {
        let contract = draft.plan.scan_contracts().unwrap().remove(0);
        let action = contract.mv_rewrite_action().unwrap();
        let allocator = SqlTableBindingAllocator::try_new_for_test(
            action.input_mapping()[0].binding().scope().get(),
        )
        .unwrap();
        let receipts = ExactBindingReceiptStore::new(&allocator);
        for selected in action.input_mapping() {
            receipts.register_for_test(
                selected.binding(),
                publication_binding(
                    &action.publication_inputs()[selected.publication_input_ordinal()],
                ),
            );
        }
        let selected = crate::preparation::prove_selected_mv_query_inputs(
            crate::api::QueryConsistency::Strict,
            &receipts.seal(),
            action,
        )
        .unwrap();
        let lineage = PlanScanBinding::resolved(
            &contract,
            publication_binding(contract.mv_rewrite_action().unwrap().publication_target()),
        );
        let target =
            NegotiatedScanReceipt::accepted_for_test(&contract, lineage, fixture_table_handle());
        draft.mv_candidate_match = Some(
            crate::preparation::prove_selected_mv_target(selected, target.lineage().occurrence())
                .unwrap(),
        );
        draft.scan_receipts[0] = target;
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

    pub(crate) fn scan_draft(
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
                NegotiatedScanReceipt::accepted_for_test(contract, lineage, fixture_table_handle())
            })
            .collect();
        FrozenExecutionDescriptionDraft::new(
            QueryExecutionKind::Read,
            plan,
            None,
            effect,
            recovery,
            scan_receipts,
            FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
            ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
        )
    }

    #[test]
    fn freeze_rejects_a_missing_scan_receipt() {
        let plan = native_scan_plan(NativeScanFixture::ConnectorRead).unwrap();
        let mut draft = scan_draft(plan, ExecutionEffect::None, RecoveryMode::NoRecovery);
        draft.scan_receipts.clear();
        let error = FrozenExecutionDescription::try_freeze(draft).unwrap_err();
        assert!(error.contains("do not exactly cover every sealed scan occurrence"));
    }

    #[test]
    fn freeze_rejects_a_duplicate_scan_receipt() {
        let plan = native_scan_plan(NativeScanFixture::ConnectorRead).unwrap();
        let mut draft = scan_draft(plan, ExecutionEffect::None, RecoveryMode::NoRecovery);
        let contract = draft.plan.scan_contracts().unwrap().remove(0);
        let duplicate = NegotiatedScanReceipt::accepted_for_test(
            &contract,
            draft.scan_receipts[0].lineage().clone(),
            fixture_table_handle(),
        );
        draft.scan_receipts.push(duplicate);
        let error = FrozenExecutionDescription::try_freeze(draft).unwrap_err();
        assert!(error.contains("repeats negotiation receipt"));
    }

    #[test]
    fn freeze_rejects_invalid_known_cost_and_preserves_known_zero() {
        let plan = native_scan_plan(NativeScanFixture::ConnectorRead).unwrap();
        let mut invalid = scan_draft(
            plan.clone(),
            ExecutionEffect::None,
            RecoveryMode::NoRecovery,
        );
        invalid.cost = FrozenCostEstimate::new(
            FrozenCostValue::Known(f64::NAN),
            FrozenCostValue::Known(1.0),
            FrozenCostValue::Known(2.0),
            FrozenCostValue::Known(3.0),
        );
        let error = FrozenExecutionDescription::try_freeze(invalid).unwrap_err();
        assert!(error.contains("root rows cost must be finite and non-negative"));

        let mut zero = scan_draft(plan, ExecutionEffect::None, RecoveryMode::NoRecovery);
        zero.cost = FrozenCostEstimate::new(
            FrozenCostValue::Known(0.0),
            FrozenCostValue::Known(0.0),
            FrozenCostValue::Known(0.0),
            FrozenCostValue::Known(0.0),
        );
        let frozen = FrozenExecutionDescription::try_freeze(zero).unwrap();
        assert_eq!(frozen.cost().root_rows(), FrozenCostValue::Known(0.0));
    }

    #[test]
    fn same_source_self_join_freezes_two_distinct_scan_occurrences() {
        let draft = scan_draft(
            novarocks_sql::test_support::native_self_join_scan_plan().unwrap(),
            ExecutionEffect::None,
            RecoveryMode::NoRecovery,
        );
        assert_eq!(draft.scan_receipts.len(), 2);
        assert_eq!(
            draft.scan_receipts[0].lineage().occurrence().binding(),
            draft.scan_receipts[1].lineage().occurrence().binding()
        );
        assert_ne!(
            draft.scan_receipts[0].lineage().occurrence().occurrence(),
            draft.scan_receipts[1].lineage().occurrence().occurrence()
        );
        let description = FrozenExecutionDescription::try_freeze(draft).unwrap();
        assert_eq!(description.scans().len(), 2);
        assert_ne!(
            description.scans()[0].scan_identity(),
            description.scans()[1].scan_identity()
        );
        assert!(
            description
                .scans()
                .iter()
                .all(|scan| scan.offered_constraint().summary().is_all())
        );
    }

    #[test]
    fn freeze_retains_the_exact_negotiated_scan_contract() {
        let draft = scan_draft(
            native_scan_plan(NativeScanFixture::ConnectorRead).unwrap(),
            ExecutionEffect::None,
            RecoveryMode::NoRecovery,
        );
        let sealed_plan = draft.plan.plan() as *const DistributedPlan;
        let description = FrozenExecutionDescription::try_freeze(draft).unwrap();
        assert_eq!(sealed_plan, description.plan() as *const DistributedPlan);
        let scan = &description.scans()[0];
        assert_eq!(scan.node_id(), scan.outcome().node_id());
        assert_eq!(scan.lineage().scan_identity(), scan.scan_identity());
        assert!(scan.offered_constraint().summary().is_all());
        assert_eq!(
            scan.final_handle()
                .binding()
                .catalog_handle()
                .catalog_name()
                .as_str(),
            "fixture-catalog"
        );
    }

    #[test]
    fn freeze_rejects_a_receipt_from_a_foreign_plan() {
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
        let error = FrozenExecutionDescription::try_freeze(target).unwrap_err();
        assert!(error.contains("binding for unknown scan node"));
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
        let plan = novarocks_sql::test_support::native_mv_rewritten_scan_plan().unwrap();
        let mut draft = scan_draft(plan, ExecutionEffect::None, RecoveryMode::NoRecovery);
        attach_strict_mv_proof(&mut draft);
        FrozenExecutionDescription::try_freeze(draft).unwrap();

        let plan = novarocks_sql::test_support::native_mv_rewritten_scan_plan().unwrap();
        let mut draft = scan_draft(plan, ExecutionEffect::None, RecoveryMode::NoRecovery);
        attach_strict_mv_proof(&mut draft);
        let contract = draft.plan.scan_contracts().unwrap().remove(0);
        draft.scan_receipts[0] = NegotiatedScanReceipt::accepted_for_test(
            &contract,
            PlanScanBinding::resolved(&contract, binding("ice", "ns", "different_target", 101)),
            fixture_table_handle(),
        );
        assert!(FrozenExecutionDescription::try_freeze(draft).is_err());
    }

    #[test]
    fn mv_candidate_match_cannot_move_between_identical_final_plans() {
        let mut source_final = scan_draft(
            novarocks_sql::test_support::native_mv_rewritten_scan_plan().unwrap(),
            ExecutionEffect::None,
            RecoveryMode::NoRecovery,
        );
        attach_strict_mv_proof(&mut source_final);
        let mut target_final = scan_draft(
            novarocks_sql::test_support::native_mv_rewritten_scan_plan().unwrap(),
            ExecutionEffect::None,
            RecoveryMode::NoRecovery,
        );
        target_final.mv_candidate_match = source_final.mv_candidate_match.take();
        assert!(FrozenExecutionDescription::try_freeze(target_final).is_err());
    }

    #[test]
    fn non_read_execution_may_have_completion_only_output() {
        let plan = native_preparation_plan(NativePreparationFixture::MissingResultOutput).unwrap();
        let description =
            FrozenExecutionDescription::try_freeze(FrozenExecutionDescriptionDraft::new(
                QueryExecutionKind::Maintenance,
                SealedPreparationPlan::seal(plan),
                None,
                ExecutionEffect::External,
                RecoveryMode::NoRecovery,
                Vec::new(),
                FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
                ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
            ))
            .unwrap();
        assert!(matches!(
            description.output(),
            OutputContract::CompletionOnly
        ));
    }
}
