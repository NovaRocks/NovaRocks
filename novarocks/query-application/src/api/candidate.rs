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
    error::Error,
    fmt,
    future::Future,
    num::{NonZeroU32, NonZeroUsize},
    pin::Pin,
    sync::Arc,
    time::Instant,
};

use novarocks_workload_control::WorkOwner;

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
}

impl ExactObjectBinding {
    pub const fn new(
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

    fn encoded_len(&self) -> Option<usize> {
        self.object
            .encoded_len()?
            .checked_add(self.catalog_generation.encoded_len()?)?
            .checked_add(self.object_identity.encoded_len()?)?
            .checked_add(self.data_version.encoded_len()?)
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
    pub fn try_new(
        publication_id: MvPublicationId,
        definition_fingerprint: [u8; 32],
        definition_provenance: impl Into<Arc<str>>,
        inputs: Vec<ExactObjectBinding>,
        output: ExactObjectBinding,
    ) -> Option<Self> {
        let definition_provenance = definition_provenance.into();
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
            inputs: inputs.into(),
            output,
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

    fn encoded_len(&self) -> Option<usize> {
        let fixed = 48_usize.checked_add(self.definition_provenance.len())?;
        let with_inputs = self.inputs.iter().try_fold(fixed, |total, binding| {
            total.checked_add(binding.encoded_len()?)
        })?;
        with_inputs.checked_add(self.output.encoded_len()?)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelationOccurrence {
    occurrence: NonZeroU32,
    binding: ExactObjectBinding,
}

impl RelationOccurrence {
    pub const fn new(occurrence: NonZeroU32, binding: ExactObjectBinding) -> Self {
        Self {
            occurrence,
            binding,
        }
    }
    pub const fn occurrence(&self) -> NonZeroU32 {
        self.occurrence
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
    pub fn try_new(
        consistency: QueryConsistency,
        occurrences: Vec<RelationOccurrence>,
        budget: MvCandidateBudget,
    ) -> Option<Self> {
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
    pub fn try_new(
        candidate: Option<ObjectPath>,
        kind: MvCandidateDiagnosticKind,
        message: impl Into<Arc<str>>,
    ) -> Option<Self> {
        let message = message.into();
        if message.is_empty() || message.len() > MAX_DIAGNOSTIC_MESSAGE_BYTES {
            return None;
        }
        Some(Self {
            candidate,
            kind,
            message,
        })
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

    fn encoded_len(&self) -> Option<usize> {
        self.candidate
            .as_ref()
            .map_or(Some(0), ObjectPath::encoded_len)?
            .checked_add(1)?
            .checked_add(self.message.len())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MvCandidateSet {
    facts: Vec<MvCandidateFact>,
    diagnostics: Vec<MvCandidateDiagnostic>,
}

impl MvCandidateSet {
    pub fn new(facts: Vec<MvCandidateFact>, diagnostics: Vec<MvCandidateDiagnostic>) -> Self {
        Self { facts, diagnostics }
    }
    pub fn facts(&self) -> &[MvCandidateFact] {
        &self.facts
    }
    pub fn diagnostics(&self) -> &[MvCandidateDiagnostic] {
        &self.diagnostics
    }

    fn validate_against(
        &self,
        budget: MvCandidateBudget,
    ) -> Result<(), MvCandidateValidationError> {
        check_limit(
            MvCandidateLimit::CandidateCount,
            self.facts.len(),
            budget.facts.max_candidates.get(),
        )?;
        for (candidate_index, fact) in self.facts.iter().enumerate() {
            check_limit(
                MvCandidateLimit::InputsPerCandidate { candidate_index },
                fact.inputs.len(),
                budget.facts.max_inputs_per_candidate.get(),
            )?;
        }
        let fact_bytes = self.facts.iter().try_fold(0_usize, |total, fact| {
            total.checked_add(fact.encoded_len()?)
        });
        check_limit(
            MvCandidateLimit::FactEncodedBytes,
            fact_bytes.unwrap_or(usize::MAX),
            budget.facts.max_encoded_bytes.get(),
        )?;
        check_limit(
            MvCandidateLimit::DiagnosticCount,
            self.diagnostics.len(),
            budget.diagnostics.max_diagnostics.get(),
        )?;
        let mut total = 0_usize;
        for (diagnostic_index, diagnostic) in self.diagnostics.iter().enumerate() {
            let bytes = diagnostic.encoded_len().unwrap_or(usize::MAX);
            check_limit(
                MvCandidateLimit::DiagnosticItemBytes { diagnostic_index },
                bytes,
                budget.diagnostics.max_item_bytes.get(),
            )?;
            total = total.saturating_add(bytes);
        }
        check_limit(
            MvCandidateLimit::DiagnosticTotalBytes,
            total,
            budget.diagnostics.max_total_bytes.get(),
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvCandidateLimit {
    CandidateCount,
    InputsPerCandidate { candidate_index: usize },
    FactEncodedBytes,
    DiagnosticCount,
    DiagnosticItemBytes { diagnostic_index: usize },
    DiagnosticTotalBytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvCandidateValidationError {
    limit_kind: MvCandidateLimit,
    limit: usize,
    actual: usize,
}

impl MvCandidateValidationError {
    pub const fn limit_kind(&self) -> MvCandidateLimit {
        self.limit_kind
    }
    pub const fn limit(&self) -> usize {
        self.limit
    }
    pub const fn actual(&self) -> usize {
        self.actual
    }
}

impl fmt::Display for MvCandidateValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "MV candidate source exceeded {:?}: actual {}, limit {}",
            self.limit_kind, self.actual, self.limit
        )
    }
}

impl Error for MvCandidateValidationError {}

fn check_limit(
    kind: MvCandidateLimit,
    actual: usize,
    limit: usize,
) -> Result<(), MvCandidateValidationError> {
    if actual <= limit {
        Ok(())
    } else {
        Err(MvCandidateValidationError {
            limit_kind: kind,
            limit,
            actual,
        })
    }
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
    validation: Option<MvCandidateValidationError>,
}

impl MvCandidateSourceError {
    pub fn new(kind: MvCandidateSourceErrorKind, message: impl Into<Arc<str>>) -> Self {
        Self {
            kind,
            message: message.into(),
            validation: None,
        }
    }
    fn validation(error: MvCandidateValidationError) -> Self {
        Self {
            kind: MvCandidateSourceErrorKind::ContractViolation,
            message: Arc::from(error.to_string()),
            validation: Some(error),
        }
    }
    pub const fn kind(&self) -> MvCandidateSourceErrorKind {
        self.kind
    }
    pub const fn validation_error(&self) -> Option<&MvCandidateValidationError> {
        self.validation.as_ref()
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

/// MV-product implementation injected by Server composition.
pub trait MvCandidateSourceDriver: Send + Sync + 'static {
    fn discover(&self, request: MvCandidateRequest, owner: WorkOwner) -> MvCandidateFuture;
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
        let future = self.driver.discover(request, owner);
        Box::pin(async move {
            let candidates = future.await?;
            candidates
                .validate_against(budget)
                .map_err(MvCandidateSourceError::validation)?;
            Ok(candidates)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nz(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }
    fn format(kind: &str) -> ProviderFactFormat {
        ProviderFactFormat::try_new("iceberg-rest", kind).unwrap()
    }
    fn binding(version: u8) -> ExactObjectBinding {
        ExactObjectBinding::new(
            ObjectPath::try_new(["ice", "ns", "orders"]).unwrap(),
            CatalogGeneration::try_new(format("catalog-generation/v1"), Arc::<[u8]>::from([1]))
                .unwrap(),
            ObjectIdentity::try_new(format("table-uuid/v1"), Arc::<[u8]>::from([2])).unwrap(),
            DataVersion::try_new(format("snapshot-id/v1"), Arc::<[u8]>::from([version])).unwrap(),
        )
    }
    fn fact(inputs: usize) -> MvCandidateFact {
        MvCandidateFact::try_new(
            MvPublicationId::try_new([1; 16]).unwrap(),
            [2; 32],
            "definition",
            (0..inputs).map(|_| binding(7)).collect(),
            binding(8),
        )
        .unwrap()
    }
    fn diagnostic(message: &str) -> MvCandidateDiagnostic {
        MvCandidateDiagnostic::try_new(
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
            Instant::now(),
        )
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
        let left = RelationOccurrence::new(NonZeroU32::new(1).unwrap(), binding(7));
        let right = RelationOccurrence::new(NonZeroU32::new(2).unwrap(), binding(7));
        assert_ne!(left.occurrence(), right.occurrence());
        assert_eq!(left.binding(), right.binding());
        assert_eq!(QueryConsistency::default(), QueryConsistency::Strict);
    }

    #[test]
    fn rejects_candidate_and_input_count_overflow() {
        let error = MvCandidateSet::new(vec![fact(1), fact(1)], vec![])
            .validate_against(budget(1, 2, 100_000, 1, 100, 100))
            .unwrap_err();
        assert_eq!(error.limit_kind(), MvCandidateLimit::CandidateCount);
        let error = MvCandidateSet::new(vec![fact(2)], vec![])
            .validate_against(budget(1, 1, 100_000, 1, 100, 100))
            .unwrap_err();
        assert_eq!(
            error.limit_kind(),
            MvCandidateLimit::InputsPerCandidate { candidate_index: 0 }
        );
    }

    #[test]
    fn rejects_fact_byte_overflow() {
        let error = MvCandidateSet::new(vec![fact(1)], vec![])
            .validate_against(budget(1, 1, 1, 1, 100, 100))
            .unwrap_err();
        assert_eq!(error.limit_kind(), MvCandidateLimit::FactEncodedBytes);
    }

    #[test]
    fn rejects_diagnostic_count_item_and_total_overflow() {
        let error = MvCandidateSet::new(vec![], vec![diagnostic("a"), diagnostic("b")])
            .validate_against(budget(1, 1, 1, 1, 100, 100))
            .unwrap_err();
        assert_eq!(error.limit_kind(), MvCandidateLimit::DiagnosticCount);
        let error = MvCandidateSet::new(vec![], vec![diagnostic("too long")])
            .validate_against(budget(1, 1, 1, 1, 2, 100))
            .unwrap_err();
        assert_eq!(
            error.limit_kind(),
            MvCandidateLimit::DiagnosticItemBytes {
                diagnostic_index: 0
            }
        );
        let error = MvCandidateSet::new(vec![], vec![diagnostic("abc"), diagnostic("def")])
            .validate_against(budget(1, 1, 1, 2, 100, 4))
            .unwrap_err();
        assert_eq!(error.limit_kind(), MvCandidateLimit::DiagnosticTotalBytes);
    }
}
