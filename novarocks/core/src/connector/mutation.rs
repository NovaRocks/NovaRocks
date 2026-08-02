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
    ConnectorCatalogMutationReconcileRequest, ConnectorCatalogMutationRequest,
    ConnectorCatalogMutationResolver, ConnectorError, ConnectorExecutionBindingKey,
    ConnectorInstanceId, ConnectorMutationFailure, ConnectorMutationOperationId,
    ConnectorRequestContext, ExternalMutationEffect, ExternalMutationEvidence,
    ExternalMutationFinalization, ExternalMutationOutcome,
};

use crate::common::engine_error::EngineError;

#[derive(Clone, Debug)]
pub struct CompletedCatalogMutation {
    pub effect: ExternalMutationEffect,
    pub receipt: ConnectorCatalogMutationReceipt,
    pub finalization: ExternalMutationFinalization,
}

/// Provider outcome after the application has performed its one permitted
/// authoritative reconciliation. Consumers that own durable state machines
/// must use this instead of inferring commit state from an EngineError string.
#[derive(Clone, Debug)]
pub enum ResolvedCatalogMutation {
    KnownCommitted(CompletedCatalogMutation),
    KnownUncommitted {
        failure: ConnectorMutationFailure,
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

/// Executes an external mutation once. A provider may return `CommitUnknown`
/// only with evidence; this adapter reconciles that evidence on the same
/// generation-fenced lease and deliberately never replays the mutation.
// Design: ADR-0017 (docs/adr/ADR-0017-connector-catalog-mutation-outcomes.md)
pub(crate) fn execute_catalog_mutation(
    resolver: &dyn ConnectorCatalogMutationResolver,
    instance_id: &ConnectorInstanceId,
    operation: ConnectorCatalogMutationOperation,
    context: ConnectorRequestContext,
) -> Result<CompletedCatalogMutation, String> {
    match resolve_catalog_mutation(resolver, instance_id, operation, context) {
        ResolvedCatalogMutation::KnownCommitted(completed) => {
            if let ExternalMutationFinalization::Failed(failure) = &completed.finalization {
                Err(
                    EngineError::commit_known_committed_finalize_failed(failure.to_string())
                        .to_string(),
                )
            } else {
                Ok(completed)
            }
        }
        ResolvedCatalogMutation::KnownUncommitted { failure } => {
            Err(EngineError::commit_known_uncommitted(failure.to_string()).to_string())
        }
        ResolvedCatalogMutation::CommitUnknown { failure, .. } => {
            Err(EngineError::commit_unknown(failure.to_string()).to_string())
        }
        ResolvedCatalogMutation::ContractFailure { error, .. } => Err(error.to_string()),
    }
}

/// Executes once and, only for `CommitUnknown`, reconciles once through the
/// same generation-fenced lease. Outer errors are SPI contract failures; every
/// provider/network/store result stays in the typed state space.
pub(crate) fn resolve_catalog_mutation(
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
    let request = ConnectorCatalogMutationRequest {
        operation_id,
        target: ConnectorExecutionBindingKey {
            instance_id: lease.descriptor().instance_id.clone(),
            incarnation: lease.incarnation(),
        },
        operation,
        context: context.clone(),
    };
    let outcome = match lease.execute(request) {
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
                Ok(outcome) => outcome,
                Err(_) => {
                    return ResolvedCatalogMutation::CommitUnknown { failure, evidence };
                }
            }
        }
        outcome => outcome,
    };
    match outcome {
        ExternalMutationOutcome::KnownCommitted {
            effect,
            receipt,
            finalization,
        } => ResolvedCatalogMutation::KnownCommitted(CompletedCatalogMutation {
            effect,
            receipt,
            finalization,
        }),
        ExternalMutationOutcome::KnownUncommitted { failure } => {
            ResolvedCatalogMutation::KnownUncommitted { failure }
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
        ConnectorCatalogMutationResolver, ConnectorError, ConnectorInstanceDescriptor,
        ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorMutationFailure,
        ConnectorMutationFailureKind, ConnectorProviderId, ConnectorRequestContext, CreatePolicy,
        ExternalMutationEffect, ExternalMutationEvidence, ExternalMutationFinalization,
        ExternalMutationOutcome,
    };

    use super::*;

    struct NeverCancelled;
    impl novarocks_spi::connector::ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct UnknownMutation {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
    }

    impl ConnectorCatalogMutation for UnknownMutation {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn incarnation(&self) -> ConnectorInstanceIncarnation {
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
            Ok(ExternalMutationOutcome::KnownCommitted {
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
                self.0.incarnation,
                self.0.clone(),
                || {},
            )
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
    }

    struct OuterErrorMutation {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
    }

    impl ConnectorCatalogMutation for OuterErrorMutation {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn incarnation(&self) -> ConnectorInstanceIncarnation {
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
                self.0.incarnation,
                self.0.clone(),
                || {},
            )
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
            Arc::new(NeverCancelled),
            1024,
            1024,
        )
        .expect("context")
    }

    #[test]
    fn unknown_is_reconciled_once_without_replaying_execute() {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            instance_id: ConnectorInstanceId::parse("catalog.analytics").expect("instance"),
        };
        let mutation = Arc::new(UnknownMutation {
            descriptor: descriptor.clone(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([3; 16]),
        });
        let result = execute_catalog_mutation(
            &Resolver(mutation),
            &descriptor.instance_id,
            create_namespace_operation(descriptor.instance_id.clone()),
            test_context(),
        )
        .expect("reconciled committed result");
        assert_eq!(result.effect, ExternalMutationEffect::Applied);
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
            incarnation: ConnectorInstanceIncarnation::from_bytes([7; 16]),
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
