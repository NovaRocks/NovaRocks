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

//! Domain preparation borrows the original control Body, never a replacement Root.
use super::*;
use novarocks_mv_application::{
    management::{EffectIdentity, EffectResponsibility, EffectScope, ManagementRequest},
    persistence::{
        documents::MvDocumentError,
        keyed_documents::{
            keyed_configuration_slot_change, observe_current_keyed_documents_with_management_async,
        },
    },
};
use novarocks_spi::connector::{
    ConnectorControlResolver, ConnectorDocumentManagementAdmissionRequest,
    ConnectorDocumentManagementOperation, ConnectorInstanceId, ConnectorTableIdentity,
    ConnectorTableObjectCaptureRequest, ConnectorTableObjectSelector, ConnectorTableResolution,
    KeyedMutation, KeyedMutationIntent,
};

impl ControlSource {
    pub(super) async fn prepare(&self, body: &mut Body) -> Result<(), MvProviderFailure> {
        self.check_business_refusal()?;
        let product = body.input.product.clone();
        let cancelled = body.cancel.clone();
        let mut waiting = Box::pin(product.acquire_foreground_async(
            self.target.clone(),
            self.request.owner(),
            self.cancelled_or_refused(cancelled),
        ));
        body.activity = Some(waiting.as_mut().await);
        drop(waiting);
        self.check_business_refusal()?;
        if let Some(Err(error)) = &body.activity {
            return Err(MvProviderFailure::new(
                MvProviderFailureKind::Unavailable,
                format!("MV control activity admission: {error:?}"),
            ));
        }
        let readiness = body.input.ports.readiness().clone();
        let ready = readiness.in_requests(self.requests.clone());
        let loaded = ready
            .load_ready(self.request.target())
            .await
            .map_err(|error| shared(SharedFailureCause::new(error)))?;
        if loaded.is_none() && !matches!(self.request, MvControlRequest::Drop { .. }) {
            if readiness.is_retired_mv(self.request.target()) {
                return Err(connector(ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::Unsupported,
                    "Retired MV persistence format supports DROP only",
                )));
            }
            return Err(invalid(format!(
                "materialized view does not exist: {:?}",
                self.target
            )));
        }
        let instance = ConnectorInstanceId::parse(
            self.target
                .catalog()
                .ok_or_else(|| invalid("MV control requires current Iceberg catalog context"))?,
        )
        .map_err(|error| shared(SharedFailureCause::new(error)))?;
        let planning = ConnectorControlResolver::acquire_current(
            body.input.ports.connector_control(),
            &instance,
        )
        .map_err(connector)?;
        if let MvControlRequest::Alter { statement, .. } = &self.request
            && let MvAlterAction::Storage { owner, .. }
            | MvAlterAction::RefreshAndStorage { owner, .. } = &statement.action
            && (owner.instance_id != planning.binding().descriptor().instance_id
                || owner.incarnation != planning.binding().incarnation())
        {
            return Err(MvProviderFailure::new(
                MvProviderFailureKind::TargetReplaced,
                "MV storage property scope belongs to a different connector generation",
            ));
        }
        body.planning = Some(planning.clone());
        let table = ConnectorTableIdentity {
            instance_id: instance,
            namespace: Arc::from(self.target.namespace()),
            table: Arc::from(self.target.name()),
        };
        use crate::mv::domain::staged_create::CaptureFailure;
        let captured = self
            .capture
            .capture(
                &planning,
                &body.factory,
                ConnectorTableObjectCaptureRequest {
                    table: table.clone(),
                    resolution: ConnectorTableResolution::StrictBaseTable,
                    selector: ConnectorTableObjectSelector::Current,
                    context: body.context.clone(),
                },
            )
            .await;
        body.binding = Some(match captured {
            Ok(binding) => binding,
            Err(CaptureFailure::Provider(error))
                if error.kind() == novarocks_spi::connector::ConnectorErrorKind::NotFound
                    && matches!(&self.request, MvControlRequest::Drop { statement, .. } if statement.if_exists) =>
            {
                self.check_business_refusal()?;
                body.drop_readiness = Some(MvKeyedDropReadiness::AlreadyAbsent);
                return Ok(());
            }
            Err(error) => {
                return Err({
                    let error = error.into_application();
                    let mut result = invalid(error.to_string());
                    if let Some(cause) = error.original_failure() {
                        result = result.with_original_failure(cause.clone())
                    }
                    result
                });
            }
        });
        let binding = body.binding.as_ref().unwrap().clone();
        if binding.metadata.identity != table {
            return Err(invalid("MV provider bound a different control target"));
        }
        if matches!(self.request, MvControlRequest::Drop { .. })
            && loaded.as_ref().is_some_and(|loaded| {
                loaded.projection.facts.source_revision().target_object_id != binding.object_id
            })
        {
            return Err(MvProviderFailure::new(
                MvProviderFailureKind::TargetReplaced,
                "MV DROP target changed since its last Ready projection",
            ));
        }
        let documents = planning
            .derive_document_storage_lease()
            .map_err(connector)?;
        body.documents = Some(documents.clone());
        let request = crate::mv::domain::staged_create::keyed_request(
            &documents,
            table.clone(),
            binding.object_id.clone(),
            body.context.clone(),
        );
        body.keyed_request = Some(request.clone());
        let entrance = body
            .input
            .ports
            .management_entrance()
            .map_err(invalid)?
            .clone();
        match &self.request {
            MvControlRequest::Drop { statement, .. } => {
                let observe = |request| async {
                    if loaded.is_none() {
                        entrance
                            .observe_retired_keyed_drop_async(&documents, request)
                            .await
                    } else {
                        entrance
                            .observe_keyed_drop_async(&documents, request)
                            .await
                            .map(|(observed, _)| observed)
                    }
                };
                let first = observe(request.clone()).await.map_err(document)?;
                let cancellation = body.cancel.clone();
                let mut waiting = Box::pin(
                    entrance
                        .acquire_async(first.request(), self.cancelled_or_refused(cancellation)),
                );
                match waiting.as_mut().await {
                    Ok(lease) => body.management = Some(lease),
                    Err(error) => body.management_failure = Some(invalid(error)),
                }
                drop(waiting);
                self.check_business_refusal()?;
                if let Some(error) = &body.management_failure {
                    return Err(error.clone());
                }
                let second = observe(request).await.map_err(document)?;
                if !first.matches(&second) {
                    return Err(MvProviderFailure::new(
                        MvProviderFailureKind::TargetReplaced,
                        "MV DROP target changed before dispatch admission",
                    ));
                }
                body.exact_management_target = Some(second.target().clone());
                let mut prepared = Box::pin(async {
                    if second.is_retired() {
                        ready
                            .prepare_retired_drop(self.request.target(), &second)
                            .await
                    } else {
                        ready
                            .prepare_drop(self.request.target(), statement.if_exists)
                            .await
                    }
                });
                let raw = prepared.as_mut().await;
                match raw {
                    Ok(value) => body.drop_readiness = Some(value),
                    Err(error) => body.preparation_failure = Some(projection_failure(error)),
                }
                drop(prepared);
                self.check_business_refusal()?;
                if let Some(error) = &body.preparation_failure {
                    return Err(error.clone());
                }
                body.drop_readiness =
                    Some(crate::mv::domain::iceberg_refresh::validate_drop_readiness(
                        body.drop_readiness.take().unwrap(),
                        Some(&binding.object_id),
                    )?);
                let admission = documents
                    .admit_management(
                        ConnectorDocumentManagementAdmissionRequest::try_new(
                            documents.owner().clone(),
                            documents.catalog_handle().clone(),
                            ConnectorMutationOperationId::from_bytes(*self.operation.as_bytes()),
                            binding.metadata.identity.clone(),
                            Some(binding.object_id.clone()),
                            ConnectorDocumentManagementOperation::Drop,
                            body.context.clone(),
                        )
                        .map_err(connector)?,
                    )
                    .map_err(connector)?;
                body.operation = Some(ConnectorCatalogMutationOperation::DropTable {
                    table,
                    policy: novarocks_spi::connector::DropPolicy::FailIfMissing,
                    data_disposition:
                        novarocks_spi::connector::ConnectorDropTableDataDisposition::Purge,
                    managed: Some(novarocks_spi::connector::ConnectorManagedDropFence {
                        admission,
                        expected_metadata: second.metadata_version().clone(),
                    }),
                });
            }
            MvControlRequest::Alter { statement, .. } => match &statement.action {
                MvAlterAction::Storage { properties, .. } => {
                    if properties
                        .iter()
                        .any(|(name, _)| name.as_ref() == "partitioning")
                    {
                        return Err(invalid(
                            "MV partitioning requires its original data-producing step",
                        ));
                    }
                    body.operation = Some(ConnectorCatalogMutationOperation::SetTableProperties {
                        table,
                        properties: Arc::clone(properties),
                    });
                }
                MvAlterAction::Refresh(changes)
                | MvAlterAction::RefreshAndStorage { changes, .. } => {
                    ready
                        .candidate_reader()
                        .observe_current_application_async(&documents, request.clone())
                        .await
                        .map_err(service_failure)?;
                    let (initial, admission, _) = entrance
                        .observe_keyed_admission_async(&documents, request.clone())
                        .await
                        .map_err(document)?;
                    body.current_admission = Some(admission);
                    let dependencies =
                        initial.management_dependencies(planning.control_runtime_id());
                    let cancellation = body.cancel.clone();
                    let mut waiting = Box::pin(
                        entrance.acquire_async(
                            ManagementRequest::try_new(
                                documents.catalog_handle().clone(),
                                table.clone(),
                                Some(binding.object_id.clone()),
                                ConnectorDocumentManagementOperation::SingleTargetUpdate,
                                Some(dependencies.clone()),
                                EffectScope::CATALOG_COMMIT,
                            )
                            .map_err(invalid)?,
                            self.cancelled_or_refused(cancellation),
                        ),
                    );
                    match waiting.as_mut().await {
                        Ok(lease) => body.management = Some(lease),
                        Err(error) => body.management_failure = Some(invalid(error)),
                    }
                    drop(waiting);
                    self.check_business_refusal()?;
                    if let Some(error) = &body.management_failure {
                        return Err(error.clone());
                    }
                    // Admission itself is not a publication and grants no cancellation bypass.
                    let (observed, _) = observe_current_keyed_documents_with_management_async(
                        &documents, request, &entrance,
                    )
                    .await
                    .map_err(document)?;
                    if observed.management_dependencies(planning.control_runtime_id())
                        != dependencies
                    {
                        return Err(invalid(
                            "MV definition, interpretation or publication changed during configuration admission",
                        ));
                    }
                    if observed.deployment_owner != *entrance.owner()
                        || observed.process_incarnation != *entrance.incarnation()
                    {
                        return Err(invalid(
                            "MV configuration target is no longer owned by this process",
                        ));
                    }
                    let desired = crate::mv::domain::flow::configuration_after_alter(
                        &observed.configuration,
                        changes,
                    )
                    .map_err(invalid)?;
                    let configuration =
                        crate::mv::domain::iceberg_refresh::configuration_document(desired)
                            .map_err(invalid)?;
                    body.exact_management_target = Some(observed.management_target.clone());
                    let admission = documents
                        .admit_management(
                            ConnectorDocumentManagementAdmissionRequest::try_new(
                                documents.owner().clone(),
                                documents.catalog_handle().clone(),
                                ConnectorMutationOperationId::from_bytes(
                                    *self.operation.as_bytes(),
                                ),
                                table,
                                Some(binding.object_id.clone()),
                                ConnectorDocumentManagementOperation::SingleTargetUpdate,
                                body.context.clone(),
                            )
                            .map_err(connector)?,
                        )
                        .map_err(connector)?;
                    let change =
                        keyed_configuration_slot_change(&configuration).map_err(document)?;
                    let mutation = if let MvAlterAction::RefreshAndStorage { properties, .. } =
                        &statement.action
                    {
                        KeyedMutation::SlotChangeWithTableProperties {
                            change,
                            properties: Arc::clone(properties),
                        }
                    } else {
                        KeyedMutation::SlotChange(change)
                    };
                    body.operation = Some(ConnectorCatalogMutationOperation::KeyedDocuments {
                        intent: KeyedMutationIntent {
                            admission,
                            expected_metadata: observed.view.metadata_version.clone(),
                            expected_snapshot: observed.view.current_snapshot,
                            mutation,
                        },
                    });
                }
            },
        }
        self.check_business_refusal()
    }
    pub(super) fn mark_dispatched(&self, body: &mut Body) -> Result<(), MvProviderFailure> {
        if body.management_marked {
            return Ok(());
        }
        let Some(management) = body.management.as_mut() else {
            return Ok(());
        };
        let entrance = body.input.ports.management_entrance().map_err(invalid)?;
        let operation_id = ConnectorMutationOperationId::from_bytes(*self.operation.as_bytes());
        management
            .mark_dispatched(EffectResponsibility::new(
                EffectIdentity::from_bytes(operation_id.to_bytes()),
                body.exact_management_target.as_ref().unwrap().clone(),
                entrance.incarnation().clone(),
                if matches!(self.request, MvControlRequest::Drop { .. }) {
                    EffectScope::CATALOG_AND_OBJECT_DELETION
                } else {
                    EffectScope::CATALOG_COMMIT
                },
                crate::mv::domain::staged_create::now_management_timestamp().map_err(invalid)?,
            ))
            .map_err(invalid)?;
        body.management_marked = true;
        Ok(())
    }
    pub(in crate::query::mv_action::control) fn finish_management(
        &self,
    ) -> Result<(), MvProviderFailure> {
        let mut foreign = Foreign {
            source: self,
            normal: false,
        };
        let result = (|| {
            let mut body = self.loan();
            if let Some(result) = &body.management_terminal {
                return result.clone();
            }
            if !body.management_marked {
                return Ok(());
            }
            let disposition = match self.external_effect_disposition() {
                Some(StatementExternalEffectDisposition::KnownCommitted) => {
                    EffectDisposition::KnownCommitted
                }
                Some(StatementExternalEffectDisposition::CommitUnknown) => {
                    EffectDisposition::CommitUnknown
                }
                _ => EffectDisposition::KnownUncommitted,
            };
            let evidence = {
                let o = self.call.receiving.observation();
                match o.validated_outcome() {
                    Some(Ok(ExternalMutationOutcome::CommitUnknown { evidence, .. })) => {
                        Some(evidence.clone())
                    }
                    _ => None,
                }
            };
            // Decode is fallible before consuming the unique management lease.
            let evidence = if let Some(evidence) = evidence.filter(|_| {
                matches!(
                    body.operation,
                    Some(ConnectorCatalogMutationOperation::KeyedDocuments { .. })
                ) || self.call.input.get().is_some_and(|p| {
                    matches!(
                        p.operation,
                        ConnectorCatalogMutationOperation::KeyedDocuments { .. }
                    )
                })
            }) {
                Some(
                    body.documents
                        .as_ref()
                        .unwrap()
                        .decode_keyed_slot_change_evidence(
                            body.keyed_request.as_ref().unwrap().clone(),
                            evidence,
                        )
                        .map_err(connector)?,
                )
            } else {
                None
            };
            let result = if let Some(evidence) = evidence {
                body.management
                    .take()
                    .unwrap()
                    .record_slot_change_unknown(evidence)
                    .map_err(invalid)
            } else if matches!(self.request, MvControlRequest::Drop { .. }) {
                body.management
                    .take()
                    .unwrap()
                    .record_drop_terminal(disposition)
                    .map_err(invalid)
            } else {
                body.management
                    .as_mut()
                    .unwrap()
                    .record_terminal_in_place(disposition)
                    .map_err(invalid)
            };
            body.management_terminal = Some(result.clone());
            result
        })();
        foreign.normal = true;
        result
    }
}
fn document(error: MvDocumentError) -> MvProviderFailure {
    projection_failure(error.into())
}
fn service_failure(error: QueryServiceError) -> MvProviderFailure {
    let mut result = MvProviderFailure::new(MvProviderFailureKind::Unavailable, error.to_string())
        .with_compile_control(error.compile_control_error());
    if let Some(cause) = error.failure_cause() {
        result = result.with_original_failure(cause.clone())
    }
    result
}
