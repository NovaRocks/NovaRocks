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

    fn sync_repartition_descriptor(
        &self,
        mv_id: i64,
        partition_spec: crate::mv::persistence::schema::MvPartitionContract,
        connector_context: &ConnectorRequestContext,
    ) -> Result<(), String> {
        let state = self.state.upgrade().ok_or_else(|| {
            "MV repartition descriptor projection is unavailable during engine shutdown".to_string()
        })?;
        let mut definition = state
            .mv_repository
            .load_by_id(mv_id)
            .map_err(|error| format!("load MV definition for descriptor projection: {error}"))?
            .ok_or_else(|| {
                format!("materialized view {mv_id} is absent during descriptor projection")
            })?;
        let schema = definition.schema_contract.as_mut().ok_or_else(|| {
            format!("materialized view {mv_id} has no schema contract during descriptor projection")
        })?;
        schema.target.partition = Some(partition_spec.clone());
        definition.partition_spec = Some(partition_spec);
        crate::engine::mv::iceberg_refresh::sync_iceberg_mv_descriptor(
            &state,
            &definition,
            &definition.refresh_policy,
            definition.refresh_paused,
            definition.refresh_interval_ms,
            connector_context,
        )
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
        if prepared
            .publication_intent()
            .partition_spec_replacement()
            .is_some()
        {
            "main"
        } else {
            prepared.staging_branch()
        },
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
    let managed_publication =
        managed_publication_activation_intent(prepared.publication_intent(), empty_input)?;
    crate::query_execution::contract::ConnectorWritePlanningTemplate::activate_prepared_with_intent(
        operation_id,
        preparation,
        ConnectorWriteActivationIntent::ManagedPublication(managed_publication),
        connector_context,
        exact_lease.clone(),
    )
    .map_err(|error| format!("activate exact Iceberg MV write generation: {error}"))
}

pub(crate) fn managed_publication_activation_intent(
    publication: &MvRefreshPublicationIntent,
    empty_input: ConnectorManagedPublicationEmptyInputDisposition,
) -> Result<ConnectorManagedPublicationIntent, String> {
    let arguments = (
        publication.refresh_id(),
        publication.mv_id(),
        publication.marker_token(),
        match publication.technique() {
            MvRefreshPublicationTechnique::Full => ConnectorManagedPublicationTechnique::Full,
            MvRefreshPublicationTechnique::Incremental => {
                ConnectorManagedPublicationTechnique::Incremental
            }
        },
        publication
            .bases()
            .iter()
            .map(|base| ConnectorStagedPublicationBaseFact {
                table: base.table_fqn().into(),
                uuid: base.table_uuid().into(),
                from_version: base.from_snapshot(),
                to_version: base.to_snapshot(),
            })
            .collect(),
        publication.definition_fingerprint(),
        empty_input,
    );
    match publication.partition_spec_replacement() {
        Some(replacement) => {
            ConnectorManagedPublicationIntent::try_new_with_partition_spec_replacement(
                arguments.0,
                arguments.1,
                arguments.2,
                arguments.3,
                arguments.4,
                arguments.5,
                arguments.6,
                replacement.clone(),
            )
        }
        None => ConnectorManagedPublicationIntent::try_new(
            arguments.0,
            arguments.1,
            arguments.2,
            arguments.3,
            arguments.4,
            arguments.5,
            arguments.6,
        ),
    }
    .map_err(|error| format!("build managed MV publication activation intent: {error}"))
}
