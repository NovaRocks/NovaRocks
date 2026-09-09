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

use std::{
    collections::HashMap,
    error::Error,
    fmt,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Instant,
};

use novarocks_spi::connector::read_stack::ConnectorReadBinding;
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorReadSelector, ConnectorTableHandle,
};
use novarocks_sql::binding::{SqlTableBindingAllocator, SqlTableBindingId, SqlTableBindingScopeId};
use novarocks_sql::planning::query_execution::SealedScanIdentity;
use novarocks_workload_control::WorkOwner;
use sha2::{Digest, Sha256};

const MAX_FACT_BYTES: usize = 64 * 1024;
const MAX_IDENTITY_BYTES: usize = 256;
const MAX_DIAGNOSTIC_MESSAGE_BYTES: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ObjectPath(Arc<[Arc<str>]>);

impl ObjectPath {
    pub fn try_new(parts: impl IntoIterator<Item = impl Into<Arc<str>>>) -> Option<Self> {
        let parts: Vec<Arc<str>> = parts.into_iter().map(Into::into).collect();
        if parts.is_empty() || parts.len() > 3 || parts.iter().any(|part| part.is_empty()) {
            return None;
        }
        Some(Self(parts.into()))
    }

    pub fn parts(&self) -> &[Arc<str>] {
        &self.0
    }

    fn encoded_len(&self) -> Option<usize> {
        self.0
            .iter()
            .try_fold(0_usize, |total, part| total.checked_add(part.len()))
    }
}

/// Query-owned identity of the provider and encoding used for one opaque fact.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ProviderFactFormat {
    provider: Arc<str>,
    format: Arc<str>,
}

impl ProviderFactFormat {
    pub fn try_new(provider: impl Into<Arc<str>>, format: impl Into<Arc<str>>) -> Option<Self> {
        let provider = provider.into();
        let format = format.into();
        if provider.is_empty()
            || provider.len() > MAX_IDENTITY_BYTES
            || format.is_empty()
            || format.len() > MAX_IDENTITY_BYTES
        {
            return None;
        }
        Some(Self { provider, format })
    }

    pub fn provider(&self) -> &str {
        &self.provider
    }

    pub fn format(&self) -> &str {
        &self.format
    }

    fn encoded_len(&self) -> Option<usize> {
        self.provider.len().checked_add(self.format.len())
    }
}

macro_rules! encoded_binding_fact {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, PartialEq)]
        pub struct $name {
            format: ProviderFactFormat,
            value: Arc<[u8]>,
        }

        impl $name {
            pub fn try_new(
                format: ProviderFactFormat,
                value: impl Into<Arc<[u8]>>,
            ) -> Option<Self> {
                let value = value.into();
                if value.is_empty() || value.len() > MAX_FACT_BYTES {
                    return None;
                }
                Some(Self { format, value })
            }

            pub const fn format_identity(&self) -> &ProviderFactFormat {
                &self.format
            }

            pub fn encoded_value(&self) -> &[u8] {
                &self.value
            }

            fn encoded_len(&self) -> Option<usize> {
                self.format
                    .encoded_len()
                    .and_then(|size| size.checked_add(self.value.len()))
            }
        }
    };
}

encoded_binding_fact!(CatalogGeneration);
encoded_binding_fact!(ObjectIdentity);
encoded_binding_fact!(DataVersion);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactObjectBinding {
    object: ObjectPath,
    catalog_generation: CatalogGeneration,
    object_identity: ObjectIdentity,
    data_version: DataVersion,
    read_binding: Option<ConnectorReadBinding>,
}

impl ExactObjectBinding {
    #[cfg(test)]
    pub(crate) fn new_for_test(
        object: ObjectPath,
        catalog_generation: CatalogGeneration,
        object_identity: ObjectIdentity,
        data_version: DataVersion,
    ) -> Self {
        Self {
            object,
            catalog_generation,
            object_identity,
            data_version,
            read_binding: None,
        }
    }

    pub const fn object(&self) -> &ObjectPath {
        &self.object
    }

    pub const fn catalog_generation(&self) -> &CatalogGeneration {
        &self.catalog_generation
    }

    pub const fn object_identity(&self) -> &ObjectIdentity {
        &self.object_identity
    }

    pub const fn data_version(&self) -> &DataVersion {
        &self.data_version
    }

    pub(crate) fn read_binding(&self) -> Result<&ConnectorReadBinding, String> {
        self.read_binding.as_ref().ok_or_else(|| {
            "exact binding receipt has no production Connector read binding".to_string()
        })
    }

    fn retained_encoded_len(&self) -> Option<usize> {
        self.object
            .encoded_len()?
            .checked_add(self.catalog_generation.encoded_len()?)?
            .checked_add(self.object_identity.encoded_len()?)?
            .checked_add(self.data_version.encoded_len()?)
    }
}

/// Query-owned receipt authority paired with one request-local binding store.
///
/// Production registration accepts only the opaque Connector lease, table
/// handle, selector, and the SQL token minted by that store. It derives all
/// identity facts internally; no external adapter can fill generation,
/// object-identity, or data-version fields.
pub struct ExactBindingReceiptStore {
    scope: SqlTableBindingScopeId,
    state: Mutex<ExactBindingReceiptState>,
}

#[derive(Default)]
struct ExactBindingReceiptState {
    receipts: HashMap<SqlTableBindingId, ExactObjectBinding>,
    sealed: Option<Arc<HashMap<SqlTableBindingId, ExactObjectBinding>>>,
}

/// Immutable read authority produced when the query binding owner closes its
/// semantic admission phase.
#[derive(Clone)]
pub struct SealedExactBindingReceipts {
    scope: SqlTableBindingScopeId,
    receipts: Arc<HashMap<SqlTableBindingId, ExactObjectBinding>>,
}

impl ExactBindingReceiptStore {
    pub fn new(allocator: &SqlTableBindingAllocator) -> Self {
        Self {
            scope: allocator.scope(),
            state: Mutex::new(ExactBindingReceiptState::default()),
        }
    }

    pub fn register_connector_binding(
        &self,
        binding: SqlTableBindingId,
        object: [&str; 3],
        planning_lease: &ConnectorControlPlanningLease,
        table: &ConnectorTableHandle,
        selector: ConnectorReadSelector,
    ) -> Result<(), String> {
        if !binding.belongs_to(self.scope) {
            return Err("exact binding receipt token belongs to another query".to_string());
        }
        let descriptor = planning_lease.binding().descriptor();
        if table.owner() != &descriptor.instance_id {
            return Err(
                "exact binding receipt table handle belongs to another Connector instance"
                    .to_string(),
            );
        }
        let provider = descriptor.provider_id.as_str();
        let format = |kind: &str| {
            ProviderFactFormat::try_new(provider, kind)
                .ok_or_else(|| "Connector binding fact format is invalid".to_string())
        };
        let object = ObjectPath::try_new(object)
            .ok_or_else(|| "exact binding receipt object path is invalid".to_string())?;
        let catalog_generation = CatalogGeneration::try_new(
            format("connector-control-runtime/v1")?,
            planning_lease.control_runtime_id().to_bytes(),
        )
        .ok_or_else(|| "Connector control generation fact is invalid".to_string())?;

        let mut object_hash = Sha256::new();
        object_hash.update(b"novarocks/exact-object-binding/v1\0");
        object_hash.update(table.owner().as_str().as_bytes());
        object_hash.update(b"\0");
        object_hash.update(table.payload());
        let object_identity = ObjectIdentity::try_new(
            format("connector-table-handle-digest/v1")?,
            <[u8; 32]>::from(object_hash.finalize()),
        )
        .ok_or_else(|| "Connector object identity fact is invalid".to_string())?;

        let mut version_hash = Sha256::new();
        version_hash.update(b"novarocks/exact-data-version/v1\0");
        version_hash.update(table.payload());
        match selector {
            ConnectorReadSelector::Current => version_hash.update([0]),
            ConnectorReadSelector::SnapshotId(snapshot) => {
                version_hash.update([1]);
                version_hash.update(snapshot.to_be_bytes());
            }
            ConnectorReadSelector::TimestampMicros(timestamp) => {
                version_hash.update([2]);
                version_hash.update(timestamp.to_be_bytes());
            }
        }
        let data_version = DataVersion::try_new(
            format("connector-read-version-digest/v1")?,
            <[u8; 32]>::from(version_hash.finalize()),
        )
        .ok_or_else(|| "Connector data version fact is invalid".to_string())?;
        let receipt = ExactObjectBinding {
            object,
            catalog_generation,
            object_identity,
            data_version,
            read_binding: Some(ConnectorReadBinding::new(
                descriptor.clone(),
                planning_lease
                    .binding()
                    .catalog_properties()
                    .map_err(|error| error.to_string())?
                    .handle()
                    .clone(),
            )),
        };
        self.register_receipt(binding, receipt)
    }

    fn register_receipt(
        &self,
        binding: SqlTableBindingId,
        receipt: ExactObjectBinding,
    ) -> Result<(), String> {
        let mut state = self.state.lock().expect("exact binding receipt lock");
        if state.sealed.is_some() {
            return Err("exact binding receipt store is semantically sealed".to_string());
        }
        match state.receipts.get(&binding) {
            Some(existing) if existing == &receipt => Ok(()),
            Some(_) => {
                Err("exact binding receipt token was registered with conflicting facts".to_string())
            }
            None => {
                state.receipts.insert(binding, receipt);
                Ok(())
            }
        }
    }

    /// Close registration and publish one immutable snapshot. Repeated calls
    /// return the same snapshot and never reopen admission.
    pub fn seal(&self) -> SealedExactBindingReceipts {
        let mut state = self.state.lock().expect("exact binding receipt lock");
        let receipts = match &state.sealed {
            Some(receipts) => Arc::clone(receipts),
            None => {
                let receipts = Arc::new(state.receipts.clone());
                state.sealed = Some(Arc::clone(&receipts));
                receipts
            }
        };
        SealedExactBindingReceipts {
            scope: self.scope,
            receipts,
        }
    }

    pub fn sealed_view(&self) -> Option<SealedExactBindingReceipts> {
        let state = self.state.lock().expect("exact binding receipt lock");
        state
            .sealed
            .as_ref()
            .map(|receipts| SealedExactBindingReceipts {
                scope: self.scope,
                receipts: Arc::clone(receipts),
            })
    }

    #[cfg(test)]
    pub(crate) fn register_for_test(
        &self,
        binding: SqlTableBindingId,
        receipt: ExactObjectBinding,
    ) {
        assert!(binding.belongs_to(self.scope));
        self.register_receipt(binding, receipt)
            .expect("test receipt registration must precede semantic seal");
    }
}

impl SealedExactBindingReceipts {
    pub(crate) fn resolve(&self, binding: SqlTableBindingId) -> Result<ExactObjectBinding, String> {
        if !binding.belongs_to(self.scope) {
            return Err("exact binding receipt token belongs to another query".to_string());
        }
        self.receipts
            .get(&binding)
            .cloned()
            .ok_or_else(|| "exact binding receipt is missing from this query".to_string())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MvPublicationId([u8; 16]);

impl MvPublicationId {
    pub fn try_new(value: [u8; 16]) -> Option<Self> {
        (value != [0; 16]).then_some(Self(value))
    }

    pub const fn bytes(self) -> [u8; 16] {
        self.0
    }
}

/// Product evidence about one published MV version, never a validity decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvCandidateFact {
    publication_id: MvPublicationId,
    definition_fingerprint: [u8; 32],
    definition_provenance: Arc<str>,
    inputs: Arc<[ExactObjectBinding]>,
    output: ExactObjectBinding,
}

impl MvCandidateFact {
    fn retain(input: MvCandidateFactInput<'_>) -> Self {
        Self {
            publication_id: input.publication_id,
            definition_fingerprint: input.definition_fingerprint,
            definition_provenance: Arc::from(input.definition_provenance),
            inputs: input.inputs.to_vec().into(),
            output: input.output.clone(),
        }
    }
    #[cfg(test)]
    pub(crate) fn try_new_for_test(
        publication_id: MvPublicationId,
        definition_fingerprint: [u8; 32],
        definition_provenance: &str,
        inputs: &[ExactObjectBinding],
        output: &ExactObjectBinding,
    ) -> Option<Self> {
        if definition_fingerprint == [0; 32]
            || definition_provenance.is_empty()
            || definition_provenance.len() > MAX_FACT_BYTES
            || inputs.is_empty()
        {
            return None;
        }
        Some(Self {
            publication_id,
            definition_fingerprint,
            definition_provenance: Arc::from(definition_provenance),
            inputs: Arc::from(inputs),
            output: output.clone(),
        })
    }
    pub const fn publication_id(&self) -> MvPublicationId {
        self.publication_id
    }
    pub const fn definition_fingerprint(&self) -> [u8; 32] {
        self.definition_fingerprint
    }
    pub fn definition_provenance(&self) -> &str {
        &self.definition_provenance
    }
    pub fn inputs(&self) -> &[ExactObjectBinding] {
        &self.inputs
    }
    pub const fn output(&self) -> &ExactObjectBinding {
        &self.output
    }
}

/// Borrowed product evidence offered to the query-owned collector.
///
/// The producer cannot construct a retained candidate. Count, input, and byte
/// admission therefore happens before this borrowed view is copied into the
/// query's retained candidate set.
#[derive(Clone, Copy)]
pub struct MvCandidateFactInput<'a> {
    publication_id: MvPublicationId,
    definition_fingerprint: [u8; 32],
    definition_provenance: &'a str,
    inputs: &'a [ExactObjectBinding],
    output: &'a ExactObjectBinding,
}

impl<'a> MvCandidateFactInput<'a> {
    pub fn try_new(
        publication_id: MvPublicationId,
        definition_fingerprint: [u8; 32],
        definition_provenance: &'a str,
        inputs: &'a [ExactObjectBinding],
        output: &'a ExactObjectBinding,
    ) -> Option<Self> {
        if definition_fingerprint == [0; 32]
            || definition_provenance.is_empty()
            || definition_provenance.len() > MAX_FACT_BYTES
            || inputs.is_empty()
        {
            return None;
        }
        Some(Self {
            publication_id,
            definition_fingerprint,
            definition_provenance,
            inputs,
            output,
        })
    }

    fn encoded_len(self) -> Option<usize> {
        let fixed = 48_usize.checked_add(self.definition_provenance.len())?;
        let with_inputs = self.inputs.iter().try_fold(fixed, |total, binding| {
            total.checked_add(binding.retained_encoded_len()?)
        })?;
        with_inputs.checked_add(self.output.retained_encoded_len()?)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationOccurrence {
    occurrence: SealedScanIdentity,
    sql_occurrence: novarocks_sql::planning::query_execution::SqlScanOccurrence,
    sql_binding: SqlTableBindingId,
    binding: ExactObjectBinding,
}

impl RelationOccurrence {
    pub(crate) const fn resolved(
        occurrence: SealedScanIdentity,
        sql_occurrence: novarocks_sql::planning::query_execution::SqlScanOccurrence,
        sql_binding: SqlTableBindingId,
        binding: ExactObjectBinding,
    ) -> Self {
        Self {
            occurrence,
            sql_occurrence,
            sql_binding,
            binding,
        }
    }
    pub const fn occurrence(&self) -> SealedScanIdentity {
        self.occurrence
    }
    pub(crate) const fn sql_binding(&self) -> SqlTableBindingId {
        self.sql_binding
    }
    pub(crate) const fn sql_occurrence(
        &self,
    ) -> novarocks_sql::planning::query_execution::SqlScanOccurrence {
        self.sql_occurrence
    }
    pub const fn binding(&self) -> &ExactObjectBinding {
        &self.binding
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum QueryConsistency {
    #[default]
    Strict,
}

#[derive(Clone, Copy, Debug)]
pub struct MvCandidateFactBudget {
    max_candidates: NonZeroUsize,
    max_inputs_per_candidate: NonZeroUsize,
    max_encoded_bytes: NonZeroUsize,
}

impl MvCandidateFactBudget {
    pub const fn new(
        max_candidates: NonZeroUsize,
        max_inputs_per_candidate: NonZeroUsize,
        max_encoded_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            max_candidates,
            max_inputs_per_candidate,
            max_encoded_bytes,
        }
    }
    pub const fn max_candidates(self) -> NonZeroUsize {
        self.max_candidates
    }
    pub const fn max_inputs_per_candidate(self) -> NonZeroUsize {
        self.max_inputs_per_candidate
    }
    pub const fn max_encoded_bytes(self) -> NonZeroUsize {
        self.max_encoded_bytes
    }
}

#[derive(Clone, Copy, Debug)]
pub struct MvCandidateDiagnosticBudget {
    max_diagnostics: NonZeroUsize,
    max_item_bytes: NonZeroUsize,
    max_total_bytes: NonZeroUsize,
}

impl MvCandidateDiagnosticBudget {
    pub const fn new(
        max_diagnostics: NonZeroUsize,
        max_item_bytes: NonZeroUsize,
        max_total_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            max_diagnostics,
            max_item_bytes,
            max_total_bytes,
        }
    }
    pub const fn max_diagnostics(self) -> NonZeroUsize {
        self.max_diagnostics
    }
    pub const fn max_item_bytes(self) -> NonZeroUsize {
        self.max_item_bytes
    }
    pub const fn max_total_bytes(self) -> NonZeroUsize {
        self.max_total_bytes
    }
}

#[derive(Clone, Copy, Debug)]
pub struct MvCandidateBudget {
    facts: MvCandidateFactBudget,
    diagnostics: MvCandidateDiagnosticBudget,
    max_concurrency: NonZeroUsize,
    deadline: Instant,
}

impl MvCandidateBudget {
    pub const fn new(
        facts: MvCandidateFactBudget,
        diagnostics: MvCandidateDiagnosticBudget,
        max_concurrency: NonZeroUsize,
        deadline: Instant,
    ) -> Self {
        Self {
            facts,
            diagnostics,
            max_concurrency,
            deadline,
        }
    }
    pub const fn facts(self) -> MvCandidateFactBudget {
        self.facts
    }
    pub const fn diagnostics(self) -> MvCandidateDiagnosticBudget {
        self.diagnostics
    }
    pub const fn max_concurrency(self) -> NonZeroUsize {
        self.max_concurrency
    }
    pub const fn deadline(self) -> Instant {
        self.deadline
    }
}

pub struct MvCandidateRequest {
    consistency: QueryConsistency,
    occurrences: Arc<[RelationOccurrence]>,
    budget: MvCandidateBudget,
}

impl MvCandidateRequest {
    pub fn try_from_plan_bindings(
        consistency: QueryConsistency,
        bindings: &[crate::preparation::PlanScanBinding],
        budget: MvCandidateBudget,
    ) -> Option<Self> {
        let occurrences = bindings
            .iter()
            .map(|binding| binding.occurrence().clone())
            .collect::<Vec<_>>();
        (!occurrences.is_empty()).then(|| Self {
            consistency,
            occurrences: occurrences.into(),
            budget,
        })
    }
    pub const fn consistency(&self) -> QueryConsistency {
        self.consistency
    }
    pub fn occurrences(&self) -> &[RelationOccurrence] {
        &self.occurrences
    }
    pub const fn budget(&self) -> MvCandidateBudget {
        self.budget
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MvCandidateDiagnosticKind {
    CandidateUnavailable,
    DefinitionInvalid,
    UnsupportedShape,
    MetadataUnavailable,
    BudgetExhausted,
    SourceUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvCandidateDiagnostic {
    candidate: Option<ObjectPath>,
    kind: MvCandidateDiagnosticKind,
    message: Arc<str>,
}

impl MvCandidateDiagnostic {
    fn retain(input: MvCandidateDiagnosticInput<'_>) -> Self {
        Self {
            candidate: input.candidate.cloned(),
            kind: input.kind,
            message: Arc::from(input.message),
        }
    }
    pub const fn candidate(&self) -> Option<&ObjectPath> {
        self.candidate.as_ref()
    }
    pub const fn kind(&self) -> MvCandidateDiagnosticKind {
        self.kind
    }
    pub fn message(&self) -> &str {
        &self.message
    }
}

#[derive(Clone, Copy)]
pub struct MvCandidateDiagnosticInput<'a> {
    candidate: Option<&'a ObjectPath>,
    kind: MvCandidateDiagnosticKind,
    message: &'a str,
}

impl<'a> MvCandidateDiagnosticInput<'a> {
    pub fn try_new(
        candidate: Option<&'a ObjectPath>,
        kind: MvCandidateDiagnosticKind,
        message: &'a str,
    ) -> Option<Self> {
        if message.is_empty() || message.len() > MAX_DIAGNOSTIC_MESSAGE_BYTES {
            return None;
        }
        Some(Self {
            candidate,
            kind,
            message,
        })
    }

    fn encoded_len(self) -> Option<usize> {
        self.candidate
            .map_or(Some(0), ObjectPath::encoded_len)?
            .checked_add(1)?
            .checked_add(self.message.len())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MvCandidateSet {
    facts: Vec<MvCandidateFact>,
    diagnostics: Vec<MvCandidateDiagnostic>,
    budget_exhaustion: Vec<MvCandidateLimit>,
}

impl MvCandidateSet {
    pub fn facts(&self) -> &[MvCandidateFact] {
        &self.facts
    }
    pub fn diagnostics(&self) -> &[MvCandidateDiagnostic] {
        &self.diagnostics
    }

    /// Limits reached while isolating optional candidates. Exhaustion removes
    /// only the affected candidate or diagnostic; it never fails the required
    /// base-table query.
    pub fn budget_exhaustion(&self) -> &[MvCandidateLimit] {
        &self.budget_exhaustion
    }

    fn record_exhaustion(&mut self, limit: MvCandidateLimit) {
        if !self.budget_exhaustion.contains(&limit) {
            self.budget_exhaustion.push(limit);
        }
    }
}

#[derive(Default)]
struct MvCandidateCollectorState {
    closed: bool,
    candidate_index: usize,
    diagnostic_index: usize,
    fact_bytes: usize,
    diagnostic_bytes: usize,
    set: MvCandidateSet,
}

struct MvCandidateCollectorInner {
    budget: MvCandidateBudget,
    permits: Arc<tokio::sync::Semaphore>,
    state: Mutex<MvCandidateCollectorState>,
}

/// Query-owned bounded sink for optional MV discovery.
///
/// A product driver never returns an unbounded collection. It must acquire a
/// permit before producing each item, and the sink checks count, lineage-input,
/// and byte limits before retaining that item.
#[derive(Clone)]
pub struct MvCandidateCollector {
    inner: Arc<MvCandidateCollectorInner>,
}

impl MvCandidateCollector {
    fn new(budget: MvCandidateBudget) -> Self {
        Self {
            inner: Arc::new(MvCandidateCollectorInner {
                budget,
                permits: Arc::new(tokio::sync::Semaphore::new(budget.max_concurrency().get())),
                state: Mutex::new(MvCandidateCollectorState::default()),
            }),
        }
    }

    pub async fn acquire(&self) -> Result<MvCandidatePermit, MvCandidateSourceError> {
        let permit = self
            .inner
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| {
                MvCandidateSourceError::new(
                    MvCandidateSourceErrorKind::ContractViolation,
                    "MV candidate collector is closed",
                )
            })?;
        Ok(MvCandidatePermit {
            collector: self.clone(),
            _permit: permit,
        })
    }

    fn record_fact(
        &self,
        fact: MvCandidateFactInput<'_>,
    ) -> Result<Option<MvCandidateLimit>, String> {
        let mut state = self.inner.state.lock().expect("candidate collector lock");
        if state.closed {
            return Err("MV candidate collector accepted an item after completion".to_string());
        }
        let candidate_index = state.candidate_index;
        state.candidate_index = state.candidate_index.saturating_add(1);
        let limit = if state.set.facts.len() >= self.inner.budget.facts.max_candidates.get() {
            Some(MvCandidateLimit::CandidateCount)
        } else if fact.inputs.len() > self.inner.budget.facts.max_inputs_per_candidate.get() {
            Some(MvCandidateLimit::InputsPerCandidate { candidate_index })
        } else {
            let bytes = fact.encoded_len();
            match bytes.and_then(|bytes| state.fact_bytes.checked_add(bytes)) {
                Some(next_bytes)
                    if next_bytes <= self.inner.budget.facts.max_encoded_bytes.get() =>
                {
                    state.fact_bytes = next_bytes;
                    state.set.facts.push(MvCandidateFact::retain(fact));
                    None
                }
                _ => Some(MvCandidateLimit::FactEncodedBytes),
            }
        };
        if let Some(limit) = limit {
            state.set.record_exhaustion(limit);
        }
        Ok(limit)
    }

    fn record_diagnostic(
        &self,
        diagnostic: MvCandidateDiagnosticInput<'_>,
    ) -> Option<MvCandidateLimit> {
        let mut state = self.inner.state.lock().expect("candidate collector lock");
        if state.closed {
            return None;
        }
        let diagnostic_index = state.diagnostic_index;
        state.diagnostic_index = state.diagnostic_index.saturating_add(1);
        let limit = if state.set.diagnostics.len()
            >= self.inner.budget.diagnostics.max_diagnostics.get()
        {
            Some(MvCandidateLimit::DiagnosticCount)
        } else {
            let bytes = diagnostic.encoded_len();
            match bytes {
                Some(bytes) if bytes > self.inner.budget.diagnostics.max_item_bytes.get() => {
                    Some(MvCandidateLimit::DiagnosticItemBytes { diagnostic_index })
                }
                Some(bytes) => match state.diagnostic_bytes.checked_add(bytes) {
                    Some(next_bytes)
                        if next_bytes <= self.inner.budget.diagnostics.max_total_bytes.get() =>
                    {
                        state.diagnostic_bytes = next_bytes;
                        state
                            .set
                            .diagnostics
                            .push(MvCandidateDiagnostic::retain(diagnostic));
                        None
                    }
                    _ => Some(MvCandidateLimit::DiagnosticTotalBytes),
                },
                None => Some(MvCandidateLimit::DiagnosticItemBytes { diagnostic_index }),
            }
        };
        if let Some(limit) = limit {
            state.set.record_exhaustion(limit);
        }
        limit
    }

    fn finish(&self) -> MvCandidateSet {
        self.inner.permits.close();
        let mut state = self.inner.state.lock().expect("candidate collector lock");
        state.closed = true;
        std::mem::take(&mut state.set)
    }

    fn record_exhaustion(&self, limit: MvCandidateLimit) {
        let mut state = self.inner.state.lock().expect("candidate collector lock");
        if !state.closed {
            state.set.record_exhaustion(limit);
        }
    }

    fn record_source_failure(
        &self,
        kind: MvCandidateDiagnosticKind,
        message: &str,
        exhaustion: Option<MvCandidateLimit>,
    ) {
        let mut end = message.len().min(MAX_DIAGNOSTIC_MESSAGE_BYTES);
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        let message = &message[..end];
        let diagnostic = MvCandidateDiagnosticInput::try_new(
            None,
            kind,
            if message.is_empty() {
                "MV candidate source is unavailable"
            } else {
                message
            },
        )
        .expect("fallback MV diagnostic is bounded and non-empty");
        self.record_diagnostic(diagnostic);
        if let Some(limit) = exhaustion {
            self.record_exhaustion(limit);
        }
    }
}

pub struct MvCandidatePermit {
    collector: MvCandidateCollector,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl MvCandidatePermit {
    pub fn accept_fact(
        &self,
        fact: MvCandidateFactInput<'_>,
    ) -> Result<Option<MvCandidateLimit>, MvCandidateSourceError> {
        self.collector.record_fact(fact).map_err(|message| {
            MvCandidateSourceError::new(MvCandidateSourceErrorKind::ContractViolation, message)
        })
    }

    pub fn accept_diagnostic(
        &self,
        diagnostic: MvCandidateDiagnosticInput<'_>,
    ) -> Option<MvCandidateLimit> {
        self.collector.record_diagnostic(diagnostic)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvCandidateLimit {
    DiscoveryDeadline,
    CandidateCount,
    InputsPerCandidate { candidate_index: usize },
    FactEncodedBytes,
    DiagnosticCount,
    DiagnosticItemBytes { diagnostic_index: usize },
    DiagnosticTotalBytes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MvCandidateSourceErrorKind {
    Cancelled,
    DeadlineExceeded,
    RequiredBindingInvalid,
    ContractViolation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvCandidateSourceError {
    kind: MvCandidateSourceErrorKind,
    message: Arc<str>,
}

impl MvCandidateSourceError {
    pub fn new(kind: MvCandidateSourceErrorKind, message: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
    pub const fn kind(&self) -> MvCandidateSourceErrorKind {
        self.kind
    }
}

impl fmt::Display for MvCandidateSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}
impl Error for MvCandidateSourceError {}

pub type MvCandidateFuture =
    Pin<Box<dyn Future<Output = Result<MvCandidateSet, MvCandidateSourceError>> + Send + 'static>>;
pub type MvCandidateDriverFuture =
    Pin<Box<dyn Future<Output = Result<(), MvCandidateSourceError>> + Send + 'static>>;

/// MV-product implementation injected by Server composition.
pub trait MvCandidateSourceDriver: Send + Sync + 'static {
    fn discover(
        &self,
        request: MvCandidateRequest,
        collector: MvCandidateCollector,
        owner: WorkOwner,
    ) -> MvCandidateDriverFuture;
}

/// Query-owned source that validates all product output before returning it.
#[derive(Clone)]
pub struct MvCandidateSource {
    driver: Arc<dyn MvCandidateSourceDriver>,
}

impl MvCandidateSource {
    pub fn new(driver: impl MvCandidateSourceDriver) -> Self {
        Self {
            driver: Arc::new(driver),
        }
    }
    pub fn discover(&self, request: MvCandidateRequest, owner: WorkOwner) -> MvCandidateFuture {
        let budget = request.budget();
        let scope = owner.scope();
        let cancellation = match scope.cancellation() {
            Ok(cancellation) => cancellation,
            Err(error) => {
                return Box::pin(async move {
                    Err(MvCandidateSourceError::new(
                        MvCandidateSourceErrorKind::Cancelled,
                        error.to_string(),
                    ))
                });
            }
        };
        let collector = MvCandidateCollector::new(budget);
        let future = self.driver.discover(request, collector.clone(), owner);
        Box::pin(async move {
            let result = tokio::select! {
                reason = cancellation.cancelled() => {
                    let _ = collector.finish();
                    return Err(MvCandidateSourceError::new(
                        MvCandidateSourceErrorKind::Cancelled,
                        format!("MV candidate discovery cancelled: {reason:?}"),
                    ));
                }
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(budget.deadline())) => {
                    collector.record_source_failure(
                        MvCandidateDiagnosticKind::BudgetExhausted,
                        "MV candidate discovery reached its optional budget deadline",
                        Some(MvCandidateLimit::DiscoveryDeadline),
                    );
                    return Ok(collector.finish());
                }
                result = future => result,
            };
            match result {
                Ok(()) => Ok(collector.finish()),
                Err(error) if error.kind() == MvCandidateSourceErrorKind::ContractViolation => {
                    let message = error.to_string();
                    collector.record_source_failure(
                        MvCandidateDiagnosticKind::SourceUnavailable,
                        &message,
                        None,
                    );
                    Ok(collector.finish())
                }
                Err(error) if error.kind() == MvCandidateSourceErrorKind::DeadlineExceeded => {
                    let message = error.to_string();
                    collector.record_source_failure(
                        MvCandidateDiagnosticKind::BudgetExhausted,
                        &message,
                        Some(MvCandidateLimit::DiscoveryDeadline),
                    );
                    Ok(collector.finish())
                }
                Err(error) => {
                    let _ = collector.finish();
                    Err(error)
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan_contract() -> novarocks_sql::planning::query_execution::SealedScanContract {
        novarocks_sql::planning::query_execution::SealedPreparationPlan::seal(
            novarocks_sql::test_support::native_scan_plan(
                novarocks_sql::test_support::NativeScanFixture::ConnectorRead,
            )
            .unwrap(),
        )
        .scan_contracts()
        .unwrap()
        .remove(0)
    }

    fn relation(version: u8) -> RelationOccurrence {
        let contract = scan_contract();
        RelationOccurrence::resolved(
            contract.identity(),
            contract.sql_occurrence(),
            contract.binding(),
            binding(version),
        )
    }

    fn nz(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }
    fn format(kind: &str) -> ProviderFactFormat {
        ProviderFactFormat::try_new("iceberg-rest", kind).unwrap()
    }
    fn binding(version: u8) -> ExactObjectBinding {
        ExactObjectBinding::new_for_test(
            ObjectPath::try_new(["ice", "ns", "orders"]).unwrap(),
            CatalogGeneration::try_new(format("catalog-generation/v1"), Arc::<[u8]>::from([1]))
                .unwrap(),
            ObjectIdentity::try_new(format("table-uuid/v1"), Arc::<[u8]>::from([2])).unwrap(),
            DataVersion::try_new(format("snapshot-id/v1"), Arc::<[u8]>::from([version])).unwrap(),
        )
    }

    struct FactFixture {
        inputs: Vec<ExactObjectBinding>,
        output: ExactObjectBinding,
    }

    impl FactFixture {
        fn input(&self) -> MvCandidateFactInput<'_> {
            MvCandidateFactInput::try_new(
                MvPublicationId::try_new([1; 16]).unwrap(),
                [2; 32],
                "definition",
                &self.inputs,
                &self.output,
            )
            .unwrap()
        }
    }

    fn fact(inputs: usize) -> FactFixture {
        FactFixture {
            inputs: (0..inputs).map(|_| binding(7)).collect(),
            output: binding(8),
        }
    }
    fn diagnostic(message: &str) -> MvCandidateDiagnosticInput<'_> {
        MvCandidateDiagnosticInput::try_new(
            None,
            MvCandidateDiagnosticKind::CandidateUnavailable,
            message,
        )
        .unwrap()
    }
    fn budget(
        candidates: usize,
        inputs: usize,
        fact_bytes: usize,
        diagnostics: usize,
        item_bytes: usize,
        total_bytes: usize,
    ) -> MvCandidateBudget {
        MvCandidateBudget::new(
            MvCandidateFactBudget::new(nz(candidates), nz(inputs), nz(fact_bytes)),
            MvCandidateDiagnosticBudget::new(nz(diagnostics), nz(item_bytes), nz(total_bytes)),
            nz(1),
            Instant::now() + std::time::Duration::from_secs(1),
        )
    }

    #[test]
    fn semantic_seal_closes_receipt_registration_and_keeps_one_snapshot() {
        let mut allocator = novarocks_sql::binding::SqlTableBindingAllocator::new_unique().unwrap();
        let token = allocator.allocate().unwrap();
        let store = ExactBindingReceiptStore::new(&allocator);
        store.register_for_test(token, binding(7));
        let sealed = store.seal();

        assert!(store.register_receipt(token, binding(8)).is_err());
        assert_eq!(sealed.resolve(token).unwrap(), binding(7));
        assert_eq!(store.seal().resolve(token).unwrap(), binding(7));
    }

    struct ContractViolationSource;

    impl MvCandidateSourceDriver for ContractViolationSource {
        fn discover(
            &self,
            _request: MvCandidateRequest,
            _collector: MvCandidateCollector,
            owner: WorkOwner,
        ) -> MvCandidateDriverFuture {
            Box::pin(async move {
                owner.complete();
                Err(MvCandidateSourceError::new(
                    MvCandidateSourceErrorKind::ContractViolation,
                    "one MV publication is malformed",
                ))
            })
        }
    }

    struct PendingSource;

    impl MvCandidateSourceDriver for PendingSource {
        fn discover(
            &self,
            _request: MvCandidateRequest,
            _collector: MvCandidateCollector,
            owner: WorkOwner,
        ) -> MvCandidateDriverFuture {
            Box::pin(async move {
                let _owner = owner;
                std::future::pending().await
            })
        }
    }

    struct MixedSource;

    impl MvCandidateSourceDriver for MixedSource {
        fn discover(
            &self,
            _request: MvCandidateRequest,
            collector: MvCandidateCollector,
            owner: WorkOwner,
        ) -> MvCandidateDriverFuture {
            Box::pin(async move {
                let unavailable = collector.acquire().await?;
                unavailable.accept_diagnostic(diagnostic("one malformed publication"));
                drop(unavailable);
                let accepted = collector.acquire().await?;
                let fact = fact(1);
                assert_eq!(accepted.accept_fact(fact.input())?, None);
                owner.complete();
                Ok(())
            })
        }
    }

    #[test]
    fn binding_fact_types_keep_provider_and_format_identity() {
        let value = binding(7);
        assert_eq!(
            value.catalog_generation().format_identity().format(),
            "catalog-generation/v1"
        );
        assert_eq!(
            value.object_identity().format_identity().format(),
            "table-uuid/v1"
        );
        assert_eq!(
            value.data_version().format_identity().provider(),
            "iceberg-rest"
        );
    }

    #[test]
    fn repeated_relations_keep_occurrences_and_strict_is_default() {
        let plan = novarocks_sql::planning::query_execution::SealedPreparationPlan::seal(
            novarocks_sql::test_support::native_self_join_scan_plan().unwrap(),
        );
        let contracts = plan.scan_contracts().unwrap();
        let left = RelationOccurrence::resolved(
            contracts[0].identity(),
            contracts[0].sql_occurrence(),
            contracts[0].binding(),
            binding(7),
        );
        let right = RelationOccurrence::resolved(
            contracts[1].identity(),
            contracts[1].sql_occurrence(),
            contracts[1].binding(),
            binding(7),
        );
        assert_ne!(left.occurrence(), right.occurrence());
        assert_eq!(left.binding(), right.binding());
        assert_eq!(QueryConsistency::default(), QueryConsistency::Strict);
    }

    #[test]
    fn isolates_candidate_and_input_count_exhaustion() {
        let collector = MvCandidateCollector::new(budget(1, 2, 100_000, 1, 100, 100));
        let first = fact(1);
        let second = fact(1);
        collector.record_fact(first.input()).unwrap();
        collector.record_fact(second.input()).unwrap();
        let bounded = collector.finish();
        assert_eq!(bounded.facts().len(), 1);
        assert_eq!(
            bounded.budget_exhaustion(),
            &[MvCandidateLimit::CandidateCount]
        );
        let collector = MvCandidateCollector::new(budget(1, 1, 100_000, 1, 100, 100));
        let fact = fact(2);
        collector.record_fact(fact.input()).unwrap();
        let bounded = collector.finish();
        assert!(bounded.facts().is_empty());
        assert_eq!(
            bounded.budget_exhaustion(),
            &[MvCandidateLimit::InputsPerCandidate { candidate_index: 0 }]
        );
    }

    #[test]
    fn isolates_fact_byte_exhaustion() {
        let collector = MvCandidateCollector::new(budget(1, 1, 1, 1, 100, 100));
        let fact = fact(1);
        collector.record_fact(fact.input()).unwrap();
        let bounded = collector.finish();
        assert!(bounded.facts().is_empty());
        assert_eq!(
            bounded.budget_exhaustion(),
            &[MvCandidateLimit::FactEncodedBytes]
        );
    }

    #[test]
    fn isolates_diagnostic_count_item_and_total_exhaustion() {
        let collector = MvCandidateCollector::new(budget(1, 1, 1, 1, 100, 100));
        collector.record_diagnostic(diagnostic("a"));
        collector.record_diagnostic(diagnostic("b"));
        let bounded = collector.finish();
        assert_eq!(bounded.diagnostics().len(), 1);
        assert_eq!(
            bounded.budget_exhaustion(),
            &[MvCandidateLimit::DiagnosticCount]
        );
        let collector = MvCandidateCollector::new(budget(1, 1, 1, 1, 2, 100));
        collector.record_diagnostic(diagnostic("too long"));
        let bounded = collector.finish();
        assert_eq!(
            bounded.budget_exhaustion(),
            &[MvCandidateLimit::DiagnosticItemBytes {
                diagnostic_index: 0
            }]
        );
        let collector = MvCandidateCollector::new(budget(1, 1, 1, 2, 100, 4));
        collector.record_diagnostic(diagnostic("abc"));
        collector.record_diagnostic(diagnostic("def"));
        let bounded = collector.finish();
        assert_eq!(bounded.diagnostics().len(), 1);
        assert_eq!(
            bounded.budget_exhaustion(),
            &[MvCandidateLimit::DiagnosticTotalBytes]
        );
    }

    #[tokio::test]
    async fn collector_enforces_real_producer_concurrency() {
        let collector = MvCandidateCollector::new(budget(2, 2, 100_000, 2, 100, 100));
        let first = collector.acquire().await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), collector.acquire())
                .await
                .is_err()
        );
        drop(first);
        assert!(collector.acquire().await.is_ok());
    }

    #[tokio::test]
    async fn one_candidate_error_preserves_other_accepted_candidates() {
        use novarocks_workload_control::{
            ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
        };

        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let request = MvCandidateRequest {
            consistency: QueryConsistency::Strict,
            occurrences: Arc::from([relation(7)]),
            budget: budget(2, 2, 100_000, 2, 100, 100),
        };
        let result = MvCandidateSource::new(MixedSource)
            .discover(request, root.owner)
            .await
            .unwrap();
        assert_eq!(result.facts().len(), 1);
        assert_eq!(result.diagnostics().len(), 1);
    }

    #[tokio::test]
    async fn candidate_source_contract_failure_becomes_optional_diagnostic() {
        use novarocks_workload_control::{
            ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
        };

        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let request = MvCandidateRequest {
            consistency: QueryConsistency::Strict,
            occurrences: Arc::from([relation(7)]),
            budget: budget(2, 2, 100_000, 2, 100, 100),
        };
        let result = MvCandidateSource::new(ContractViolationSource)
            .discover(request, root.owner)
            .await
            .expect("optional MV source failure must not fail the base query");
        assert!(result.facts().is_empty());
        assert_eq!(result.diagnostics().len(), 1);
        assert_eq!(
            result.diagnostics()[0].kind(),
            MvCandidateDiagnosticKind::SourceUnavailable
        );
    }

    #[tokio::test]
    async fn candidate_discovery_deadline_exhaustion_does_not_fail_the_base_query() {
        use novarocks_workload_control::{
            ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
        };

        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let request = MvCandidateRequest {
            consistency: QueryConsistency::Strict,
            occurrences: Arc::from([relation(7)]),
            budget: MvCandidateBudget::new(
                MvCandidateFactBudget::new(nz(2), nz(2), nz(100_000)),
                MvCandidateDiagnosticBudget::new(nz(2), nz(100), nz(100)),
                nz(1),
                Instant::now() + std::time::Duration::from_millis(10),
            ),
        };
        let result = MvCandidateSource::new(PendingSource)
            .discover(request, root.owner)
            .await
            .expect("optional candidate deadline must not fail the base query");
        assert!(result.facts().is_empty());
        assert_eq!(
            result.budget_exhaustion(),
            &[MvCandidateLimit::DiscoveryDeadline]
        );
        assert_eq!(
            result.diagnostics()[0].kind(),
            MvCandidateDiagnosticKind::BudgetExhausted
        );
    }
}
