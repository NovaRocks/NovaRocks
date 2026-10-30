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

//! Prepared publication mechanisms have no independently writable verdict.
//! The journal consumes one mechanism, records its evidence synchronously, and
//! only then runs independently bounded finalization.

use crate::catalog::publication::EvidenceContext;
use crate::commit::attempt::observation::DispatchEvidence;
use crate::commit::publication_descriptor::RecoveryBinding;
use async_trait::async_trait;
use novarocks_spi::connector::ExternalMutationFinalization;

/// Preparation has never entered a mutation interval. Preserve its typed
/// refusal independently of an SDK error kind or a later transport fact.
#[derive(Debug)]
pub(crate) struct PreparationFailure(pub novarocks_spi::connector::ConnectorMutationFailure);
pub(crate) type PreparationResult<T> = std::result::Result<T, PreparationFailure>;
impl From<crate::iceberg::Error> for PreparationFailure {
    fn from(error: crate::iceberg::Error) -> Self {
        Self(crate::commit::attempt::before_dispatch_failure(&error))
    }
}
impl From<novarocks_spi::connector::ConnectorError> for PreparationFailure {
    fn from(error: novarocks_spi::connector::ConnectorError) -> Self {
        crate::iceberg::Error::new(
            crate::iceberg::ErrorKind::Unexpected,
            "Publication preparation stopped",
        )
        .with_source(error)
        .into()
    }
}

pub(crate) enum PreparedPublication {
    Mutation {
        binding: Option<RecoveryBinding>,
        dispatch: Box<dyn PreparedCatalogDispatch>,
    },
    NoOp,
}
impl std::fmt::Debug for PreparedPublication {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Mutation { binding, .. } => formatter
                .debug_struct("PreparedPublication::Mutation")
                .field("bound", &binding.is_some())
                .finish_non_exhaustive(),
            Self::NoOp => formatter.write_str("PreparedPublication::NoOp"),
        }
    }
}

/// The evidence is complete when the adapter returns. Finalization may not be
/// awaited while deriving it, including cache acquisition or table construction.
pub(crate) struct DispatchCompletion {
    pub evidence: DispatchEvidence,
    pub finalization: Option<Box<dyn FinalizationWork>>,
}
impl DispatchCompletion {
    pub(crate) fn evidence(evidence: DispatchEvidence) -> Self {
        Self {
            evidence,
            finalization: None,
        }
    }
}

#[async_trait]
pub(crate) trait PreparedCatalogDispatch: Send {
    /// Consumes the exact prepared request once. REST session/authentication
    /// and request encoding are complete; the retained filesystem protocol
    /// still loads, validates and encodes its metadata in this interval.
    async fn dispatch_once(self: Box<Self>, evidence: EvidenceContext) -> DispatchCompletion;
}

#[async_trait]
pub(crate) trait FinalizationWork: Send {
    /// The work owns any in-flight I/O until its actual exit. The context bounds
    /// starting/polling work; callers must not time out a join and detach it.
    async fn run(self: Box<Self>, context: EvidenceContext) -> ExternalMutationFinalization;
}

impl EvidenceContext {
    /// Project an explicit phase grant to already admitted storage capability.
    /// This grants no new credential collection, source operation, or admission;
    /// those belong to the statement/attempt that prepared the publication.
    pub(crate) fn for_io(
        self,
        original: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> std::result::Result<
        novarocks_spi::connector::ConnectorRequestContext,
        novarocks_spi::connector::ConnectorError,
    > {
        use crate::catalog::publication::EvidencePhase;
        use novarocks_spi::connector::{ConnectorRequestContext, ConnectorStopOwner};
        let inherited = original.necessary_finalization_deadline_cap();
        let deadline = match self.phase() {
            EvidencePhase::Reclamation => self.deadline(),
            EvidencePhase::Evidence | EvidencePhase::Finalization => {
                inherited.map_or(self.deadline(), |cap| cap.min(self.deadline()))
            }
        };
        let stop = if inherited.is_some() && self.phase() != EvidencePhase::Reclamation {
            // This is the necessary phase's own control, not the statement's
            // business stop. A nested publication must not replace it.
            original.stop().clone()
        } else {
            ConnectorStopOwner::new().view()
        };
        let mut request = if original.io_admission().is_some() {
            original
                .for_existing_obligation(deadline, stop)
                .after_external_effect()
        } else {
            // Keep the original no-MIO capability boundary: do not copy an
            // attempt source, credential collector or request-scope extension.
            let mut request = ConnectorRequestContext::try_new(
                deadline,
                stop,
                original.max_handle_payload_bytes(),
                original.max_total_payload_bytes(),
            )?
            .with_initiation(original.initiation())
            .after_external_effect();
            if let Some(resolver) = original.storage_resolver() {
                request = request.with_storage_resolver(std::sync::Arc::clone(resolver));
            }
            request
        };
        // Cleanup keeps its independent deadline, but cannot erase a parent
        // restriction and thereby turn a later publication into fresh authority.
        if let Some(cap) = inherited {
            request = request.with_necessary_finalization_deadline_cap(cap);
        }
        if self.phase() == EvidencePhase::Finalization {
            request = request.with_necessary_finalization_deadline_cap(deadline);
        }
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::publication::CatalogEvidencePolicy;
    use novarocks_spi::connector::read_stack::ConnectorSourceOperations;
    use novarocks_spi::connector::{
        ConnectorErrorKind, ConnectorOperationControl, ConnectorRangeScope,
        ConnectorRequestContext, ConnectorStopOwner,
    };
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    struct OriginalCredentialSink;
    impl novarocks_spi::connector::ConnectorVendedCredentialLeaseSink for OriginalCredentialSink {
        fn offer_vended_s3_credential_lease(
            &self,
            _: &novarocks_spi::connector::CatalogProperties,
            _: novarocks_spi::connector::VendedS3CredentialLeaseContribution,
        ) -> Result<(), novarocks_spi::connector::ConnectorError> {
            panic!("phase projection must not offer credentials");
        }
    }

    #[test]
    fn necessary_control_is_inherited_without_reauthorizing_no_mio_capabilities() {
        let business_stop = ConnectorStopOwner::new();
        let original = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            business_stop.view(),
            64,
            128,
        )
        .unwrap()
        .with_vended_credential_lease_sink(Arc::new(OriginalCredentialSink))
        .with_execution_source(
            ConnectorRangeScope::try_new(1, 2, 3, 4, 5, 6).unwrap(),
            ConnectorSourceOperations::new(),
        );
        let marker = original.request_scope_extension_or_insert_with(|| 42_u64);
        let policy = CatalogEvidencePolicy::default();
        let grant = policy.begin_finalization().unwrap();
        let necessary = grant.for_io(&original).unwrap();
        business_stop.request_stop();
        assert!(necessary.check_active().is_ok());
        assert!(necessary.deadline() > original.deadline());
        assert_eq!(
            necessary.necessary_finalization_deadline_cap(),
            Some(grant.deadline())
        );
        assert!(necessary.range_scope().is_none());
        assert!(necessary.source_operations().is_none());
        assert!(necessary.vended_credential_lease_sink().is_none());
        assert!(!Arc::ptr_eq(
            &marker,
            &necessary.request_scope_extension_or_insert_with(|| 42_u64),
        ));
        assert_eq!(necessary.max_handle_payload_bytes(), 64);
        assert_eq!(necessary.max_total_payload_bytes(), 128);

        // The original phase may carry a host-owned stop. Only this stop,
        // never the already-cancelled business stop, survives nesting.
        let phase_stop = ConnectorStopOwner::new();
        let necessary = necessary.with_additional_stop(phase_stop.view());
        let nested = policy.begin_evidence(&necessary).unwrap();
        assert_eq!(nested.deadline(), grant.deadline());
        let nested_io = nested.for_io(&necessary).unwrap();
        let final_io = policy
            .begin_finalization()
            .unwrap()
            .for_io(&necessary)
            .unwrap();
        let cleanup = policy
            .begin_reclamation()
            .unwrap()
            .for_io(&necessary)
            .unwrap();
        phase_stop.request_stop();
        for context in [&nested_io, &final_io] {
            assert_eq!(context.deadline(), grant.deadline());
            assert_eq!(
                context.check_active().unwrap_err().kind(),
                ConnectorErrorKind::Cancelled
            );
        }
        assert!(cleanup.deadline() > grant.deadline());
        assert!(cleanup.check_active().is_ok());
        assert_eq!(
            cleanup.necessary_finalization_deadline_cap(),
            Some(grant.deadline())
        );
    }
}
