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

//! Application-side execution of one external catalog mutation.

use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorCatalogMutationReceipt,
    ConnectorCatalogMutationReconcileRequest, ConnectorCatalogMutationResolver, ConnectorError,
    ConnectorInstanceId, ConnectorMutationFailure, ConnectorMutationOperationId,
    ConnectorRequestContext, ExternalMutationEvidence, ExternalMutationFinalization,
    ExternalMutationOutcome,
};

use novarocks_query_application::engine_error::EngineError;

#[derive(Clone, Debug)]
pub struct CompletedCatalogMutation {
    pub finalization: ExternalMutationFinalization,
    pub cleanup: ExternalMutationFinalization,
}

/// Provider outcome after the application has performed its one permitted
/// authoritative reconciliation. Consumers that own durable state machines
/// must use this instead of inferring commit state from an EngineError string.
#[derive(Clone, Debug)]
pub enum ResolvedCatalogMutation {
    KnownCommitted(CompletedCatalogMutation),
    KnownUncommitted {
        failure: ConnectorMutationFailure,
        cleanup: ExternalMutationFinalization,
    },
    CommitUnknown {
        failure: ConnectorMutationFailure,
        evidence: ExternalMutationEvidence,
    },
    /// A local SPI contract failure. This is deliberately distinct from a
    /// provider outcome: a caller with durable recovery must know whether the
    /// operation was definitely never sent to the provider.
    ContractFailure {
        error: ConnectorError,
        dispatch: MutationDispatchState,
    },
}

/// What the application can prove about dispatch when the SPI boundary itself
/// fails. It must not be inferred from a provider error string.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationDispatchState {
    ConfirmedNotDispatched,
    PossiblyDispatched,
}

/// Cleanup failure accompanies the definite publication failure; it never
/// changes the verdict or makes the mutation eligible for a replay.
pub(crate) fn known_uncommitted_message(
    failure: impl std::fmt::Display,
    cleanup: &ExternalMutationFinalization,
) -> String {
    match cleanup {
        ExternalMutationFinalization::Complete => failure.to_string(),
        ExternalMutationFinalization::Failed(error) => {
            format!("{failure}; artifact cleanup failed: {error}")
        }
    }
}

/// Executes an external mutation once. A provider may return `CommitUnknown`
/// only with evidence; this adapter reconciles that evidence on the same
/// generation-fenced lease and deliberately never replays the mutation.
// Design: ADR-0017 (docs/adr/ADR-0017-connector-catalog-mutation-outcomes.md)
pub fn execute_catalog_mutation(
    resolver: &dyn ConnectorCatalogMutationResolver,
    instance_id: &ConnectorInstanceId,
    operation: ConnectorCatalogMutationOperation,
    context: ConnectorRequestContext,
) -> Result<CompletedCatalogMutation, String> {
    present_catalog_mutation_result(resolve_catalog_mutation(
        resolver,
        instance_id,
        operation,
        context,
    ))
}

/// Executes and presents a mutation through the exact lease retained during
/// admission and metadata guards, including its one permitted reconciliation.
pub(crate) fn execute_catalog_mutation_with_lease(
    lease: &novarocks_spi::connector::ConnectorCatalogMutationLease,
    operation_id: ConnectorMutationOperationId,
    operation: ConnectorCatalogMutationOperation,
    context: ConnectorRequestContext,
) -> Result<CompletedCatalogMutation, String> {
    present_catalog_mutation_result(resolve_catalog_mutation_with_lease(
        lease,
        operation_id,
        operation,
        context,
    ))
}

fn present_catalog_mutation_result(
    result: ResolvedCatalogMutation,
) -> Result<CompletedCatalogMutation, String> {
    match result {
        ResolvedCatalogMutation::KnownCommitted(completed) => {
            if let ExternalMutationFinalization::Failed(error) = &completed.cleanup {
                tracing::warn!(%error, "committed catalog mutation retains incomplete cleanup");
            }
            if let ExternalMutationFinalization::Failed(failure) = &completed.finalization {
                Err(
                    EngineError::commit_known_committed_finalize_failed(failure.to_string())
                        .to_string(),
                )
            } else {
                Ok(completed)
            }
        }
        ResolvedCatalogMutation::KnownUncommitted { failure, cleanup } => Err(
            EngineError::commit_known_uncommitted(known_uncommitted_message(failure, &cleanup))
                .to_string(),
        ),
        ResolvedCatalogMutation::CommitUnknown { failure, .. } => {
            Err(EngineError::commit_unknown(failure.to_string()).to_bracketed_user_message())
        }
        ResolvedCatalogMutation::ContractFailure { error, .. } => Err(error.to_string()),
    }
}

/// Executes once and, only for `CommitUnknown`, reconciles once through the
/// same generation-fenced lease. Outer errors are SPI contract failures; every
/// provider/network/store result stays in the typed state space.
pub fn resolve_catalog_mutation(
    resolver: &dyn ConnectorCatalogMutationResolver,
    instance_id: &ConnectorInstanceId,
    operation: ConnectorCatalogMutationOperation,
    context: ConnectorRequestContext,
) -> ResolvedCatalogMutation {
    let lease = match resolver.acquire_current_mutation(instance_id) {
        Ok(lease) => lease,
        Err(error) => {
            return ResolvedCatalogMutation::ContractFailure {
                error,
                dispatch: MutationDispatchState::ConfirmedNotDispatched,
            };
        }
    };
    resolve_catalog_mutation_with_lease(
        &lease,
        ConnectorMutationOperationId::new(),
        operation,
        context,
    )
}

/// Executes one mutation through an already-retained exact lease.
///
/// This is deliberately separate from [`resolve_catalog_mutation`]: a caller
/// that acquired a planning generation may derive a mutation lease from it and
/// must not silently acquire a newer current generation for the external
/// mutation.
pub fn resolve_catalog_mutation_with_lease(
    lease: &novarocks_spi::connector::ConnectorCatalogMutationLease,
    operation_id: ConnectorMutationOperationId,
    operation: ConnectorCatalogMutationOperation,
    context: ConnectorRequestContext,
) -> ResolvedCatalogMutation {
    let outcome = match lease.execute_operation(operation_id, operation, context.clone()) {
        Ok(outcome) => outcome,
        Err(error) => {
            return ResolvedCatalogMutation::ContractFailure {
                error,
                dispatch: MutationDispatchState::PossiblyDispatched,
            };
        }
    };
    resolve_outcome(lease, outcome, context)
}

/// Dispatches one catalog mutation without reconciliation.
///
/// A caller that owns a publication identity with a terminal `CommitUnknown`
/// contract must use this entry point.  It receives the provider's first
/// outcome unchanged and must not inspect, replay, clean up, or project after
/// an unknown result.
pub fn dispatch_catalog_mutation_once_with_lease(
    lease: &novarocks_spi::connector::ConnectorCatalogMutationLease,
    operation_id: ConnectorMutationOperationId,
    operation: ConnectorCatalogMutationOperation,
    context: ConnectorRequestContext,
) -> ResolvedCatalogMutation {
    match lease.execute_operation(operation_id, operation, context) {
        Ok(ExternalMutationOutcome::KnownCommitted {
            finalization,
            cleanup,
            ..
        }) => ResolvedCatalogMutation::KnownCommitted(CompletedCatalogMutation {
            finalization,
            cleanup,
        }),
        Ok(ExternalMutationOutcome::KnownUncommitted { failure, cleanup }) => {
            ResolvedCatalogMutation::KnownUncommitted { failure, cleanup }
        }
        Ok(ExternalMutationOutcome::CommitUnknown { failure, evidence }) => {
            ResolvedCatalogMutation::CommitUnknown { failure, evidence }
        }
        Err(error) => ResolvedCatalogMutation::ContractFailure {
            error,
            dispatch: MutationDispatchState::PossiblyDispatched,
        },
    }
}

/// Dispatch the original asynchronous mutation once. The caller retains both
/// this future and receiving across cancelled borrows, then reads the original
/// validated outcome without taking or replaying it. No reconciliation occurs.
pub async fn dispatch_catalog_mutation_once_with_lease_async_in(
    lease: &novarocks_spi::connector::ConnectorCatalogMutationLease,
    operation_id: ConnectorMutationOperationId,
    operation: ConnectorCatalogMutationOperation,
    context: ConnectorRequestContext,
    receiving: &novarocks_spi::connector::ConnectorCatalogMutationReceiving,
) -> Result<(), ConnectorError> {
    lease
        .execute_operation_async_in(operation_id, operation, context, receiving)
        .await
}

fn resolve_outcome(
    lease: &novarocks_spi::connector::ConnectorCatalogMutationLease,
    outcome: ExternalMutationOutcome<ConnectorCatalogMutationReceipt>,
    context: ConnectorRequestContext,
) -> ResolvedCatalogMutation {
    let outcome = match outcome {
        ExternalMutationOutcome::CommitUnknown { failure, evidence } => {
            match lease.reconcile(ConnectorCatalogMutationReconcileRequest {
                evidence: evidence.clone(),
                context,
            }) {
                Ok(ExternalMutationOutcome::KnownUncommitted { .. }) | Err(_) => {
                    return ResolvedCatalogMutation::CommitUnknown { failure, evidence };
                }
                Ok(outcome) => outcome,
            }
        }
        outcome => outcome,
    };
    match outcome {
        ExternalMutationOutcome::KnownCommitted {
            finalization,
            cleanup,
            ..
        } => ResolvedCatalogMutation::KnownCommitted(CompletedCatalogMutation {
            finalization,
            cleanup,
        }),
        ExternalMutationOutcome::KnownUncommitted { failure, cleanup } => {
            ResolvedCatalogMutation::KnownUncommitted { failure, cleanup }
        }
        ExternalMutationOutcome::CommitUnknown { failure, evidence } => {
            ResolvedCatalogMutation::CommitUnknown { failure, evidence }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorCatalogMutation, ConnectorCatalogMutationReceipt,
        ConnectorCatalogMutationReconcileRequest, ConnectorCatalogMutationRequest,
        ConnectorCatalogMutationResolver, ConnectorControlRuntimeId, ConnectorError,
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorMutationFailure,
        ConnectorMutationFailureKind, ConnectorProviderId, ConnectorRequestContext, CreatePolicy,
        ExternalMutationEffect, ExternalMutationEvidence, ExternalMutationFinalization,
        ExternalMutationOutcome, ProviderBindingEpoch,
    };

    use super::*;

    struct UnknownMutation {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        negative_reconciliation: bool,
    }

    impl ConnectorCatalogMutation for UnknownMutation {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn incarnation(&self) -> ProviderBindingEpoch {
            self.incarnation
        }
        fn execute(
            &self,
            request: ConnectorCatalogMutationRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError>
        {
            Ok(ExternalMutationOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "response lost",
                ),
                evidence: ExternalMutationEvidence::try_new(
                    1,
                    self.descriptor.clone(),
                    self.incarnation,
                    request.operation_id,
                    request.operation.kind(),
                    Bytes::from_static(b"test"),
                )?,
            })
        }
        fn reconcile(
            &self,
            request: ConnectorCatalogMutationReconcileRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError>
        {
            if self.negative_reconciliation {
                return Ok(ExternalMutationOutcome::KnownUncommitted {
                    failure: ConnectorMutationFailure::new(
                        ConnectorMutationFailureKind::Conflict,
                        "ref does not currently match",
                    ),
                    cleanup: ExternalMutationFinalization::Complete,
                });
            }
            Ok(ExternalMutationOutcome::KnownCommitted {
                cleanup: novarocks_spi::connector::ExternalMutationFinalization::Complete,
                effect: ExternalMutationEffect::Applied,
                receipt: ConnectorCatalogMutationReceipt::try_new(
                    self.descriptor.clone(),
                    self.incarnation,
                    request.evidence.operation_id(),
                    request.evidence.operation_kind(),
                    None,
                )?,
                finalization: ExternalMutationFinalization::Complete,
            })
        }
    }

    struct Resolver(Arc<UnknownMutation>);
    impl ConnectorCatalogMutationResolver for Resolver {
        fn acquire_current_mutation(
            &self,
            _instance_id: &ConnectorInstanceId,
        ) -> Result<novarocks_spi::connector::ConnectorCatalogMutationLease, ConnectorError>
        {
            novarocks_spi::connector::ConnectorCatalogMutationLease::new(
                self.0.descriptor.clone(),
                ConnectorControlRuntimeId::from_bytes([1; 16]),
                self.0.incarnation,
                self.0.clone(),
                || {},
            )
        }

        fn acquire_exact_mutation(
            &self,
            _control_runtime_id: ConnectorControlRuntimeId,
        ) -> Result<novarocks_spi::connector::ConnectorCatalogMutationLease, ConnectorError>
        {
            unreachable!()
        }
    }

    struct MissingResolver;
    impl ConnectorCatalogMutationResolver for MissingResolver {
        fn acquire_current_mutation(
            &self,
            _instance_id: &ConnectorInstanceId,
        ) -> Result<novarocks_spi::connector::ConnectorCatalogMutationLease, ConnectorError>
        {
            Err(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Unsupported,
                "mutation capability is unavailable",
            ))
        }

        fn acquire_exact_mutation(
            &self,
            _control_runtime_id: ConnectorControlRuntimeId,
        ) -> Result<novarocks_spi::connector::ConnectorCatalogMutationLease, ConnectorError>
        {
            unreachable!()
        }
    }

    struct OuterErrorMutation {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
    }

    impl ConnectorCatalogMutation for OuterErrorMutation {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn incarnation(&self) -> ProviderBindingEpoch {
            self.incarnation
        }
        fn execute(
            &self,
            _request: ConnectorCatalogMutationRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError>
        {
            Err(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::Internal,
                "provider violated its outcome contract",
            ))
        }
        fn reconcile(
            &self,
            _request: ConnectorCatalogMutationReconcileRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError>
        {
            unreachable!("execute must fail before reconcile")
        }
    }

    struct OuterErrorResolver(Arc<OuterErrorMutation>);
    impl ConnectorCatalogMutationResolver for OuterErrorResolver {
        fn acquire_current_mutation(
            &self,
            _instance_id: &ConnectorInstanceId,
        ) -> Result<novarocks_spi::connector::ConnectorCatalogMutationLease, ConnectorError>
        {
            novarocks_spi::connector::ConnectorCatalogMutationLease::new(
                self.0.descriptor.clone(),
                ConnectorControlRuntimeId::from_bytes([2; 16]),
                self.0.incarnation,
                self.0.clone(),
                || {},
            )
        }

        fn acquire_exact_mutation(
            &self,
            _control_runtime_id: ConnectorControlRuntimeId,
        ) -> Result<novarocks_spi::connector::ConnectorCatalogMutationLease, ConnectorError>
        {
            unreachable!()
        }
    }

    fn create_namespace_operation(
        instance_id: ConnectorInstanceId,
    ) -> ConnectorCatalogMutationOperation {
        ConnectorCatalogMutationOperation::CreateNamespace {
            namespace: novarocks_spi::connector::ConnectorNamespaceIdentity {
                instance_id,
                namespace: Arc::from("db"),
            },
            policy: CreatePolicy::FailIfExists,
        }
    }

    fn test_context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(1),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            1024,
            1024,
        )
        .expect("context")
    }

    #[test]
    fn result_presentation_preserves_finalization_cleanup_unknown_and_contract_errors() {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("catalog").unwrap(),
        };
        let incarnation = ProviderBindingEpoch::from_bytes([1; 16]);
        let operation_id = ConnectorMutationOperationId::new();
        let operation = create_namespace_operation(descriptor.instance_id.clone());
        let failure = ConnectorMutationFailure::new(
            ConnectorMutationFailureKind::Unavailable,
            "publication failed",
        );
        let cleanup_failure =
            ConnectorMutationFailure::new(ConnectorMutationFailureKind::Internal, "cleanup failed");
        let finalization = ExternalMutationFinalization::Failed(cleanup_failure.clone());
        let committed = present_catalog_mutation_result(ResolvedCatalogMutation::KnownCommitted(
            CompletedCatalogMutation {
                cleanup: ExternalMutationFinalization::Complete,
                finalization: finalization.clone(),
            },
        ))
        .unwrap_err();
        assert_eq!(
            committed,
            EngineError::commit_known_committed_finalize_failed(cleanup_failure.to_string())
                .to_string()
        );
        let uncommitted =
            present_catalog_mutation_result(ResolvedCatalogMutation::KnownUncommitted {
                failure: failure.clone(),
                cleanup: finalization,
            })
            .unwrap_err();
        assert_eq!(
            uncommitted,
            EngineError::commit_known_uncommitted(format!(
                "{failure}; artifact cleanup failed: {cleanup_failure}"
            ))
            .to_string()
        );
        let evidence = ExternalMutationEvidence::try_new(
            1,
            descriptor,
            incarnation,
            operation_id,
            operation.kind(),
            Bytes::from_static(b"unknown"),
        )
        .unwrap();
        let unknown = present_catalog_mutation_result(ResolvedCatalogMutation::CommitUnknown {
            failure: failure.clone(),
            evidence,
        })
        .unwrap_err();
        assert_eq!(unknown, "[CommitUnknown] Unavailable: publication failed");
        assert!(!uncommitted.contains("[CommitUnknown]"));
        assert!(!committed.contains("[CommitUnknown]"));
        for dispatch in [
            MutationDispatchState::ConfirmedNotDispatched,
            MutationDispatchState::PossiblyDispatched,
        ] {
            let error = ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "original contract failure",
            );
            let expected = error.to_string();
            assert_eq!(
                present_catalog_mutation_result(ResolvedCatalogMutation::ContractFailure {
                    error,
                    dispatch
                })
                .unwrap_err(),
                expected
            );
        }
    }

    #[test]
    fn unknown_is_reconciled_once_without_replaying_execute() {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            instance_id: ConnectorInstanceId::parse("catalog.analytics").expect("instance"),
        };
        let mutation = Arc::new(UnknownMutation {
            descriptor: descriptor.clone(),
            incarnation: ProviderBindingEpoch::from_bytes([3; 16]),
            negative_reconciliation: false,
        });
        execute_catalog_mutation(
            &Resolver(mutation),
            &descriptor.instance_id,
            create_namespace_operation(descriptor.instance_id.clone()),
            test_context(),
        )
        .expect("reconciled committed result");
    }

    #[test]
    fn negative_reconciliation_cannot_erase_the_original_unknown() {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
            instance_id: ConnectorInstanceId::parse("catalog.analytics").unwrap(),
        };
        let mutation = Arc::new(UnknownMutation {
            descriptor: descriptor.clone(),
            incarnation: ProviderBindingEpoch::from_bytes([3; 16]),
            negative_reconciliation: true,
        });
        let result = resolve_catalog_mutation(
            &Resolver(mutation),
            &descriptor.instance_id,
            create_namespace_operation(descriptor.instance_id.clone()),
            test_context(),
        );
        let ResolvedCatalogMutation::CommitUnknown { failure, evidence } = result else {
            panic!("a negative read cannot authorize uncommitted cleanup");
        };
        assert_eq!(failure.message(), "response lost");
        assert_eq!(evidence.provider_payload().as_ref(), b"test");
    }

    #[test]
    fn missing_mutation_lease_proves_no_dispatch() {
        let instance_id = ConnectorInstanceId::parse("catalog.analytics").expect("instance");
        let resolution = resolve_catalog_mutation(
            &MissingResolver,
            &instance_id,
            create_namespace_operation(instance_id.clone()),
            test_context(),
        );
        assert!(matches!(
            resolution,
            ResolvedCatalogMutation::ContractFailure {
                dispatch: MutationDispatchState::ConfirmedNotDispatched,
                ..
            }
        ));
    }

    #[test]
    fn outer_execute_contract_failure_is_conservative_about_dispatch() {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            instance_id: ConnectorInstanceId::parse("catalog.analytics").expect("instance"),
        };
        let mutation = Arc::new(OuterErrorMutation {
            descriptor: descriptor.clone(),
            incarnation: ProviderBindingEpoch::from_bytes([7; 16]),
        });
        let resolution = resolve_catalog_mutation(
            &OuterErrorResolver(mutation),
            &descriptor.instance_id,
            create_namespace_operation(descriptor.instance_id.clone()),
            test_context(),
        );
        assert!(matches!(
            resolution,
            ResolvedCatalogMutation::ContractFailure {
                dispatch: MutationDispatchState::PossiblyDispatched,
                ..
            }
        ));
    }
}
