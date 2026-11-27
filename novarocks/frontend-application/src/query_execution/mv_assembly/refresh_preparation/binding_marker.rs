// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use crate::mv::domain::{
    application::{MvApplicationError, MvApplicationErrorKind},
    iceberg_refresh::IcebergMvCorePorts,
    staged_create::CommittedMvMarker,
};
use novarocks_failure_contract::SharedFailureCause;
use novarocks_mv_application::{
    management::{
        EffectDisposition, EffectIdentity, EffectResponsibility, EffectScope, ManagementRequest,
    },
    persistence::{codec::Marker, keyed_documents::MvKeyedObservedCurrentDocuments},
};
use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorCatalogMutationReceiving,
    ConnectorCatalogMutationReconcileRequest, ConnectorCatalogMutationRequest,
    ConnectorControlPlanningPin, ConnectorDocumentManagementAdmissionRequest,
    ConnectorDocumentManagementOperation, ConnectorError, ConnectorMutationOperationId,
    ConnectorOperationControl, ConnectorRequestContext, ExternalMutationFinalization,
    ExternalMutationOutcome, KeyedMarkerWrite, KeyedMutation, KeyedMutationIntent,
};
use std::{collections::BTreeMap, future::Future};

pub(crate) async fn write_binding_marker(
    source: &IcebergMvCorePorts,
    planning: &ConnectorControlPlanningPin,
    observed: MvKeyedObservedCurrentDocuments,
    marker: Marker,
    context: ConnectorRequestContext,
    cancelled: impl Future<Output = ()>,
) -> Result<Option<CommittedMvMarker>, MvApplicationError> {
    context
        .original_operation_control()
        .check_active()
        .map_err(connector)?;
    let fenced = matches!(
        marker,
        Marker::Pending { .. } | Marker::PendingReleased { .. }
    );
    if fenced && observed.interpretation.is_aggregate() {
        return Err(engine(
            "MV content verification requires a non-aggregate admission",
        ));
    }
    let original_view = fenced.then(|| observed.view.clone());
    let (key, value) = marker.encode().map_err(|error| {
        MvApplicationError::new(MvApplicationErrorKind::Engine, error.to_string())
            .with_original_failure(SharedFailureCause::new(error))
    })?;
    if observed.view.markers.get(&key) == Some(&value) {
        return Ok(None);
    }
    let entrance = source.management_entrance().map_err(engine)?;
    if &observed.deployment_owner != entrance.owner()
        || &observed.process_incarnation != entrance.incarnation()
    {
        return Err(engine(
            "The MV classifier does not own this observed target",
        ));
    }
    let documents = planning
        .derive_document_storage_lease()
        .map_err(connector)?;
    let operation = ConnectorMutationOperationId::new();
    let action = ConnectorDocumentManagementOperation::SingleTargetUpdate;
    let mut management = entrance
        .acquire_async(
            ManagementRequest::try_new(
                documents.catalog_handle().clone(),
                observed.view.table.clone(),
                Some(observed.view.object_id.clone()),
                action,
                Some(observed.management_dependencies(planning.control_runtime_id())),
                EffectScope::CATALOG_COMMIT,
            )
            .map_err(|error| engine(format!("Build MV marker admission: {error:?}")))?,
            cancelled,
        )
        .await
        .map_err(|error| engine(format!("Acquire MV marker admission: {error:?}")))?;
    let admission = documents
        .admit_management(
            ConnectorDocumentManagementAdmissionRequest::try_new(
                documents.owner().clone(),
                documents.catalog_handle().clone(),
                operation,
                observed.view.table,
                Some(observed.view.object_id),
                action,
                context.clone(),
            )
            .map_err(connector)?,
        )
        .map_err(connector)?;
    let intent = KeyedMutationIntent {
        admission,
        expected_metadata: observed.view.metadata_version,
        expected_snapshot: observed.view.current_snapshot,
        mutation: {
            let write = KeyedMarkerWrite {
                markers: BTreeMap::from([(key.clone(), value.clone())]),
            };
            if fenced {
                KeyedMutation::FencedMarkers(write)
            } else {
                KeyedMutation::Markers(write)
            }
        },
    };
    let mutation = planning.derive_mutation_lease().map_err(connector)?;
    let receiving = ConnectorCatalogMutationReceiving::default();
    context
        .original_operation_control()
        .check_active()
        .map_err(connector)?;
    management
        .mark_dispatched(EffectResponsibility::new(
            EffectIdentity::from_bytes(operation.to_bytes()),
            observed.management_target,
            entrance.incarnation().clone(),
            EffectScope::CATALOG_COMMIT,
            crate::mv::domain::staged_create::now_management_timestamp().map_err(engine)?,
        ))
        .map_err(|error| engine(format!("Dispatch MV marker responsibility: {error:?}")))?;
    let request = ConnectorCatalogMutationRequest {
        operation_id: operation,
        target: documents.owner().clone(),
        operation: ConnectorCatalogMutationOperation::KeyedDocuments { intent },
        context,
    };
    let dispatched = mutation.execute_async_in(request.clone(), &receiving).await;
    let unknown = match receiving.observation().validated_outcome() {
        Some(Ok(ExternalMutationOutcome::CommitUnknown { evidence, failure })) if fenced => {
            Some((evidence.clone(), failure.clone()))
        }
        _ => None,
    };
    let reconciled = ConnectorCatalogMutationReceiving::default();
    let reconciliation = if let Some((evidence, _)) = &unknown {
        Some(
            mutation
                .reconcile_async_in(
                    &request,
                    ConnectorCatalogMutationReconcileRequest {
                        evidence: evidence.clone(),
                        context: request.context.clone().after_external_effect(),
                    },
                    &reconciled,
                )
                .await,
        )
    } else {
        None
    };
    let selected = reconciled.observation();
    let (source, dispatched) = if matches!(
        selected.validated_outcome(),
        Some(Ok(ExternalMutationOutcome::KnownCommitted { .. }
            | ExternalMutationOutcome::KnownUncommitted { .. }))
    ) {
        (&reconciled, reconciliation.unwrap())
    } else {
        (&receiving, dispatched)
    };
    drop(selected);
    let result = source.observation();
    let (disposition, outcome) = match result.raw_outcome() {
        Some(Ok(ExternalMutationOutcome::KnownCommitted { finalization, .. })) => (
            EffectDisposition::KnownCommitted,
            match finalization {
                ExternalMutationFinalization::Failed(error) => Err(MvApplicationError::new(
                    MvApplicationErrorKind::KnownCommittedFinalizeFailed,
                    error.to_string(),
                )
                .with_original_failure(SharedFailureCause::new(error.clone()))),
                _ => dispatched.map_err(connector),
            },
        ),
        Some(Ok(ExternalMutationOutcome::KnownUncommitted { failure, .. })) => (
            EffectDisposition::KnownUncommitted,
            Err(engine(failure.to_string())
                .with_original_failure(SharedFailureCause::new(failure.clone()))),
        ),
        Some(Ok(ExternalMutationOutcome::CommitUnknown { failure, .. })) => (
            EffectDisposition::CommitUnknown,
            Err(MvApplicationError::new(
                MvApplicationErrorKind::CommitUnknown,
                failure.to_string(),
            )
            .with_original_failure(SharedFailureCause::new(failure.clone()))),
        ),
        _ if !result.dispatched() => (
            EffectDisposition::KnownUncommitted,
            Err(dispatched
                .err()
                .map(connector)
                .unwrap_or_else(|| engine("MV marker has no catalog outcome"))),
        ),
        _ => (
            EffectDisposition::CommitUnknown,
            Err(MvApplicationError::new(
                MvApplicationErrorKind::CommitUnknown,
                "MV marker catalog outcome is unknown",
            )),
        ),
    };
    drop(result);
    if let Some((evidence, _)) = &unknown
        && disposition == EffectDisposition::CommitUnknown
    {
        management
            .defer_content_marker_observation(evidence)
            .map_err(|error| engine(format!("Defer original MV marker observation: {error:?}")))?;
    } else {
        management
            .record_terminal(disposition)
            .map_err(|error| engine(format!("Record MV marker outcome: {error:?}")))?;
    }
    let mut completion_error = unknown.map(|(_, error)| {
        MvApplicationError::new(MvApplicationErrorKind::CommitUnknown, error.to_string())
            .with_original_failure(SharedFailureCause::new(error))
    });
    if disposition == EffectDisposition::KnownCommitted && fenced {
        completion_error = outcome.err().or(completion_error);
        return Ok(Some(CommittedMvMarker {
            operation: uuid::Uuid::from_bytes(operation.to_bytes()),
            key,
            value,
            original_view,
            completion_error,
        }));
    }
    outcome.map(|()| {
        Some(CommittedMvMarker {
            operation: uuid::Uuid::from_bytes(operation.to_bytes()),
            key,
            value,
            original_view,
            completion_error,
        })
    })
}

fn connector(error: ConnectorError) -> MvApplicationError {
    MvApplicationError::from_connector(MvApplicationErrorKind::Engine, error)
}

fn engine(message: impl Into<String>) -> MvApplicationError {
    MvApplicationError::new(MvApplicationErrorKind::Engine, message)
}
