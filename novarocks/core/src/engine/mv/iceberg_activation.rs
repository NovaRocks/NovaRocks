// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

//! Provider-neutral activation for the current frontend-owned MV refresh route.
//!
//! The adapter translates application publication facts into the generic
//! managed-publication intent. The exact connector generation owns physical
//! writer registration, provenance encoding and commit/reconcile machinery.

use std::sync::Weak;

use novarocks_spi::connector::{
    ConnectorManagedPublicationEmptyInputDisposition, ConnectorManagedPublicationIntent,
    ConnectorManagedPublicationTechnique, ConnectorRequestContext,
    ConnectorStagedPublicationBaseFact, ConnectorWriteActivationIntent, ConnectorWriteInputRequest,
    ConnectorWriteLease, ConnectorWriteOperationId,
};

use crate::engine::StandaloneState;
use crate::mv::application::{
    MvRefreshCommittedFacts, MvRefreshProviderActivation, MvRefreshPublicationIntent,
    MvRefreshPublicationTechnique, PreparedMvFirstRefreshWrite, PreparedMvRefreshWrite,
};
use crate::query_execution::prepared_write::PreparedDistributedWriteRequest;
use crate::query_execution::request_context::QueryExecutionContext;

/// Core-side provider adapter installed into the frontend composition. It
/// retains only a weak engine reference, preventing a direct all-in-one
/// lifecycle path or a runtime-liveness cycle.
pub(crate) struct StandaloneMvRefreshProviderActivation {
    state: Weak<StandaloneState>,
}

impl StandaloneMvRefreshProviderActivation {
    pub(crate) fn new(state: Weak<StandaloneState>) -> Self {
        Self { state }
    }
}

impl MvRefreshProviderActivation for StandaloneMvRefreshProviderActivation {
    fn activate_write(
        &self,
        prepared: PreparedMvRefreshWrite,
        planning_lease: &novarocks_spi::connector::ConnectorControlPlanningLease,
        exact_lease: &ConnectorWriteLease,
        execution: &QueryExecutionContext,
    ) -> Result<PreparedDistributedWriteRequest, String> {
        let state = self.state.upgrade().ok_or_else(|| {
            "MV refresh provider activation is unavailable during engine shutdown".to_string()
        })?;
        match prepared {
            PreparedMvRefreshWrite::FirstRefresh(prepared) => {
                crate::engine::mv_first_refresh_staging::bind_prepared_mv_first_refresh_staging(
                    &state,
                    prepared,
                    planning_lease,
                    exact_lease,
                    execution,
                )
            }
            PreparedMvRefreshWrite::Incremental(prepared) => {
                crate::engine::mv::iceberg_refresh::bind_prepared_mv_incremental_staging(
                    &state,
                    prepared,
                    planning_lease,
                    exact_lease,
                    execution,
                )
            }
        }
    }

    fn interpret_write_commit(
        &self,
        intent: MvRefreshPublicationIntent,
        receipt: &novarocks_spi::connector::ConnectorWriteReceipt,
    ) -> Result<MvRefreshCommittedFacts, String> {
        MvRefreshCommittedFacts::from_write_receipt(intent, receipt)
    }
}

/// Activate a managed MV write from the exact provider-signed preparation.
/// No application caller reloads a catalog, constructs a physical collector,
/// encodes provenance, or registers a provider write service.
pub(crate) fn activate_first_refresh_connector_write(
    prepared: &PreparedMvFirstRefreshWrite,
    connector_context: ConnectorRequestContext,
    exact_lease: &ConnectorWriteLease,
) -> Result<crate::query_execution::contract::ConnectorWritePlanningTemplate, String> {
    if prepared.observed_binding() != exact_lease.binding_key() {
        return Err("MV first-refresh write lease drifted from prepared binding".to_string());
    }
    if prepared.target_table().owner() != &exact_lease.binding_key().instance_id {
        return Err(
            "MV first-refresh staging table belongs to a different connector instance".to_string(),
        );
    }
    let operation_id: ConnectorWriteOperationId = prepared.operation_id();
    let target = crate::engine::backend_resolver::TargetBackend {
        backend_name: "iceberg",
        catalog: prepared.target_catalog().to_string(),
        namespace: prepared.target_namespace().to_string(),
        table: prepared.target_name().to_string(),
    };
    let intent = match prepared.write_mode() {
        crate::mv::application::MvStagedRefreshWriteMode::Append => {
            novarocks_spi::connector::ConnectorWriteIntent::Append
        }
        crate::mv::application::MvStagedRefreshWriteMode::FullOverwrite => {
            novarocks_spi::connector::ConnectorWriteIntent::Overwrite
        }
    };
    let empty_input = match prepared.write_mode() {
        crate::mv::application::MvStagedRefreshWriteMode::Append => {
            ConnectorManagedPublicationEmptyInputDisposition::AbortWithoutExternalCommit
        }
        crate::mv::application::MvStagedRefreshWriteMode::FullOverwrite => {
            ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite
        }
    };
    let preparation = crate::engine::iceberg_writer::prepare_iceberg_connector_write(
        exact_lease,
        &target,
        prepared.staging_branch(),
        intent,
        ConnectorWriteInputRequest::Data {
            fields: prepared
                .target_contract()
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    novarocks_spi::connector::ConnectorWriteFieldRequest::new(
                        field.as_ref().clone(),
                    )
                })
                .collect(),
        },
        novarocks_spi::connector::ConnectorWriteAdmissionPurpose::MaterializedViewRefresh,
        connector_context.clone(),
    )?;
    let managed_publication = ConnectorManagedPublicationIntent::try_new(
        prepared.publication_intent().refresh_id(),
        prepared.publication_intent().mv_id(),
        prepared.publication_intent().marker_token(),
        match prepared.publication_intent().technique() {
            MvRefreshPublicationTechnique::Full => ConnectorManagedPublicationTechnique::Full,
            MvRefreshPublicationTechnique::Incremental => {
                ConnectorManagedPublicationTechnique::Incremental
            }
        },
        prepared
            .publication_intent()
            .bases()
            .iter()
            .map(|base| ConnectorStagedPublicationBaseFact {
                table: base.table_fqn().into(),
                uuid: base.table_uuid().into(),
                from_version: base.from_snapshot(),
                to_version: base.to_snapshot(),
            })
            .collect(),
        prepared.publication_intent().definition_fingerprint(),
        empty_input,
    )
    .map_err(|error| format!("build managed MV publication activation intent: {error}"))?;
    crate::query_execution::contract::ConnectorWritePlanningTemplate::activate_prepared_with_intent(
        operation_id,
        preparation,
        ConnectorWriteActivationIntent::ManagedPublication(managed_publication),
        connector_context,
        exact_lease.clone(),
    )
    .map_err(|error| format!("activate exact Iceberg MV write generation: {error}"))
}
