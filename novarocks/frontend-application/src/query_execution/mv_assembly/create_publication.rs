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

//! Closed CREATE-only pure turns and the original empty staged write borrower.

use crate::mv::domain::{
    application::{MvApplicationError, MvApplicationErrorKind, MvCreateProjectionSeed},
    iceberg_refresh::{IcebergMvCreatePlan, build_create_documents_for_prepared_target},
};
use crate::query_execution::write_session::{
    ConnectorWriteSession, WriteBeginReceiving, WritePublicationCall,
};
use novarocks_mv_application::{
    management::{DeploymentOwner, ProcessIncarnation},
    persistence::create_documents::MvCreateDocuments,
};
use novarocks_spi::connector::document_storage::{
    ConnectorDocumentManagementAdmission, KeyedCreateIntent,
};
use novarocks_spi::connector::write_stack::{
    ConnectorWriteBeginRequest, ConnectorWriteNeededReady, ConnectorWriteSessionFlavor,
};
use novarocks_spi::connector::{
    ConnectorColumnDefinition, ConnectorControlPlanningLease, ConnectorError, ConnectorErrorKind,
    ConnectorPartitionTransform, ConnectorRequestContext, ConnectorStagedCreateLease,
    ConnectorStagedTableHandle, ConnectorStagedWritePlanningRequest, ConnectorStagedWriteProof,
    ConnectorWriteAdmissionPurpose, ConnectorWriteInputRequest, ConnectorWriteIntent,
    ConnectorWriteTargetRef, ExternalMutationFinalization,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

/// Neither variant accepts an arbitrary callback or Future. The existing
/// MvWriteActivationOriginal runs these pure authors on its original CPU worker.
#[expect(
    clippy::large_enum_variant,
    reason = "The complete CREATE input is inline in the original activation request and Arc-backed Body; inline variants avoid an additional variant allocation."
)]
pub(crate) enum CreatePublicationInput {
    StageDeclarations {
        plan: Arc<IcebergMvCreatePlan>,
    },
    Publication {
        plan: Arc<IcebergMvCreatePlan>,
        seed: Arc<MvCreateProjectionSeed>,
        staged: ConnectorStagedCreateLease,
        handle: ConnectorStagedTableHandle,
        admission: ConnectorDocumentManagementAdmission,
        owner: DeploymentOwner,
        incarnation: ProcessIncarnation,
        context: ConnectorRequestContext,
    },
}
#[expect(
    clippy::large_enum_variant,
    reason = "The complete CREATE output moves from the original Arc-backed activation Body into the existing stable CREATE Body; inline variants avoid an additional variant allocation."
)]
pub(crate) enum CreatePublicationOutput {
    StageDeclarations {
        columns: Vec<ConnectorColumnDefinition>,
        partitioning: Vec<ConnectorPartitionTransform>,
        properties: BTreeMap<Arc<str>, Arc<str>>,
    },
    Publication {
        begin: ConnectorWriteBeginRequest,
        intent: KeyedCreateIntent,
        // Keep the exact canonical producer facts with the original Source.
        documents: MvCreateDocuments,
    },
}
impl CreatePublicationInput {
    pub(crate) fn run(&self) -> Result<CreatePublicationOutput, MvApplicationError> {
        match self {
            Self::StageDeclarations { plan } => Ok(CreatePublicationOutput::StageDeclarations {
                columns: plan
                    .columns
                    .iter()
                    .map(crate::catalog_application::statement::connector_column)
                    .collect::<Result<_, _>>()
                    .map_err(invalid)?,
                partitioning: plan
                    .partition_fields
                    .iter()
                    .map(crate::catalog_application::statement::connector_partition_transform)
                    .collect(),
                properties: plan
                    .target_properties
                    .iter()
                    .map(|(key, value)| (Arc::from(key.as_str()), Arc::from(value.as_str())))
                    .collect(),
            }),
            Self::Publication {
                plan,
                seed,
                staged,
                handle,
                admission,
                owner,
                incarnation,
                context,
            } => {
                let target = handle.document_target().ok_or_else(|| {
                    invalid("staged MV CREATE target carries no prepared document binding")
                })?;
                let documents = build_create_documents_for_prepared_target(plan, seed, target)
                    .map_err(invalid)?;
                let keyed = novarocks_mv_application::persistence::keyed_documents::keyed_create_document_set(
                    &documents.definition, &documents.interpretation, &documents.configuration,
                    plan.binding_document.clone(), target, owner, incarnation,
                ).map_err(|error| invalid(error.to_string()))?;
                // This provider method is pure: it binds the already staged
                // metadata/schema and serializes its original writer handle.
                let binding = staged
                    .plan_write(ConnectorStagedWritePlanningRequest {
                        handle: handle.clone(),
                        context: context.clone(),
                    })
                    .map_err(provider)?;
                let fields = plan.staged_write_fields(handle)?;
                Ok(CreatePublicationOutput::Publication {
                    begin: ConnectorWriteBeginRequest {
                        table: Arc::from(format!(
                            "{}.{}",
                            plan.target.namespace, plan.target.table
                        )),
                        target_ref: ConnectorWriteTargetRef::main(),
                        intent: ConnectorWriteIntent::Append,
                        purpose: ConnectorWriteAdmissionPurpose::OrdinaryDml,
                        input: ConnectorWriteInputRequest::Data { fields },
                        base: None,
                        flavor: ConnectorWriteSessionFlavor::StagedCreate(binding.table().clone()),
                        context: binding.context().clone(),
                    },
                    intent: KeyedCreateIntent {
                        target: target.clone(),
                        admission: admission.clone(),
                        documents: keyed,
                    },
                    documents,
                })
            }
        }
    }
}
fn invalid(message: impl Into<String>) -> MvApplicationError {
    MvApplicationError::new(MvApplicationErrorKind::InvalidRequest, message)
}
fn provider(error: ConnectorError) -> MvApplicationError {
    MvApplicationError::from_connector(MvApplicationErrorKind::Engine, error)
}

/// Embedded in the stable CREATE Body. The phase Future only borrows this
/// original begin/session/call, including after cancellation or ordinary Err.
pub(crate) struct EmptyCreateWrite {
    begin: Arc<WriteBeginReceiving>,
    session: Option<Arc<ConnectorWriteSession>>,
    call: Option<WritePublicationCall>,
    needed: Option<Arc<ConnectorWriteNeededReady>>,
    proof: Option<ConnectorStagedWriteProof>,
}
impl EmptyCreateWrite {
    pub(crate) fn new() -> Self {
        Self {
            begin: WriteBeginReceiving::new(Arc::new(Mutex::new(None))),
            session: None,
            call: None,
            needed: None,
            proof: None,
        }
    }
    pub(crate) async fn seal(
        &mut self,
        host: &novarocks_catalog_application::ConnectorControlHost,
        planning: &ConnectorControlPlanningLease,
        staged: &ConnectorStagedCreateLease,
        handle: &ConnectorStagedTableHandle,
        request: ConnectorWriteBeginRequest,
    ) -> Result<(), ConnectorError> {
        if self.session.is_some() || self.call.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "empty CREATE write was already started",
            ));
        }
        let context = request.context.clone();
        let write = planning.derive_write_lease()?;
        let properties = write
            .catalog_properties()
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "CREATE write has no exact catalog identity",
                )
            })?
            .clone();
        let stack = host.acquire_exact_write_stack(planning.control_runtime_id())?;
        let session =
            ConnectorWriteSession::begin_async_in(stack, properties, request, &self.begin).await;
        // WriteBeginReceiving already owns the original session before the
        // Provider child Drop; this receiving alias survives any later refusal.
        self.session = self.begin.session();
        let session = session?;
        self.call = Some(session.prepare_finish_call(context)?);
        std::future::poll_fn(|cx| self.call.as_mut().unwrap().poll_needed_ready(cx)).await;
        if self.call.as_ref().unwrap().needed_ready().is_some() {
            self.call
                .as_mut()
                .unwrap()
                .take_needed_ready_in(&mut self.needed)?;
        }
        // No canonical CREATE publication may consume the seal until its
        // original worker/Provider Future/destructor and receipts really exit.
        std::future::poll_fn(|cx| self.call.as_mut().unwrap().poll_retirement(cx)).await;
        let call = self.call.as_ref().unwrap();
        match call.outcome() {
            Some(Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownCommitted { needed, .. })) => {
                match needed.finalization() {
                    ExternalMutationFinalization::Complete => {},
                    ExternalMutationFinalization::Failed(failure) => return Err(seal_failure(failure)),
                }
                let proof = ConnectorStagedWriteProof::try_new(needed.receipt().clone(), 0)?;
                staged.bind_write(handle.clone(), proof.clone())?;
                self.proof = Some(proof);
                Ok(())
            }
            Some(Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::KnownUncommitted { failure, .. }))
            | Some(Ok(novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion::CommitUnknown { failure, .. })) => {
                Err(seal_failure(failure))
            }
            Some(Err(error)) => Err(error.clone()),
            None => Err(ConnectorError::new(ConnectorErrorKind::Internal,
                "empty CREATE write exited without its original completion")),
        }
    }
    pub(crate) fn allows_staged_abort(&self) -> bool {
        use novarocks_spi::connector::write_stack::ConnectorWritePublicationCompletion as C;
        match self.call.as_ref() {
            None => true,
            Some(call) => matches!(
                call.outcome(),
                Some(Ok(C::KnownCommitted { .. } | C::KnownUncommitted { .. }))
            ),
        }
    }
    pub(crate) fn proof(&self) -> Option<&ConnectorStagedWriteProof> {
        self.proof.as_ref()
    }
    pub(crate) fn local_health_failure(
        &self,
    ) -> Option<novarocks_query_application::api::StatementNativeHealthFailure> {
        self.begin.local_health_failure().or_else(|| {
            self.call
                .as_ref()
                .and_then(|call| call.local_health_failure())
        })
    }
    pub(crate) fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.begin.local_health_failure().is_some() {
            return Poll::Pending;
        }
        if let Some(call) = self.call.as_mut() {
            call.poll_retirement(cx)
        } else {
            Poll::Ready(())
        }
    }
}

fn seal_failure(failure: &novarocks_spi::connector::ConnectorMutationFailure) -> ConnectorError {
    failure.connector_source().cloned().unwrap_or_else(|| {
        ConnectorError::from_domain_static(
            ConnectorErrorKind::Internal,
            failure.clone(),
            "empty CREATE staged write did not complete",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
    #[test]
    fn empty_seal_failure_keeps_actual_connector_fact_and_shared_cause_identity() {
        let cause =
            SharedFailureCause::new(novarocks_spi::connector::ConnectorMutationFailure::new(
                novarocks_spi::connector::ConnectorMutationFailureKind::PermissionDenied,
                "actual refusal",
            ));
        let original = ConnectorError::from_domain_static(
            ConnectorErrorKind::PermissionDenied,
            cause.clone(),
            "actual seal refusal",
        );
        let failure =
            novarocks_spi::connector::ConnectorMutationFailure::from_connector(original.clone());
        let retained = seal_failure(&failure);
        assert_eq!(retained, original);
        assert_eq!(retained.failure_fact(), cause.failure_fact());
        let source = std::error::Error::source(&retained)
            .unwrap()
            .downcast_ref::<SharedFailureCause>()
            .unwrap();
        // The ConnectorError itself is retained, rather than wrapping it in a
        // second producer and assigning a new failure class from a message.
        assert_eq!(
            Some(source),
            std::error::Error::source(&original)
                .unwrap()
                .downcast_ref::<SharedFailureCause>()
        );
    }
}
