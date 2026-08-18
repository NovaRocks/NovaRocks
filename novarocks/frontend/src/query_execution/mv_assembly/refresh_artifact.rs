// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Application-owned MV refresh activation artifacts.
//!
//! SQL produces an immutable first-refresh plan. This module adds the
//! operation, cohort, connector binding and persisted MV facts needed by the
//! frontend staging lifecycle; none of those authorities can cross back into
//! `sql/**`.

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorCommittedPartitioning, ConnectorCommittedVersion, ConnectorExecutionBindingKey,
    ConnectorManagedPartitionSpecReplacement, ConnectorTableHandle, ConnectorWriteCohortId,
    ConnectorWriteOperationId, ConnectorWriteReceipt,
};

use novarocks_sql::planning::mv::MV_JOIN_APPLY_KEY_COLUMN_NAME;
use novarocks_sql::planning::mv::first_refresh::{SqlMvFirstRefreshArtifact, SqlMvSnapshotPin};

use novarocks::mv::application::{
    MvIncrementalJoinMode, MvIncrementalRewriteEvidence, MvIncrementalWriteMode,
};

/// The application commit semantics selected after first-refresh SQL planning.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MvStagedRefreshWriteMode {
    Append,
    FullOverwrite,
}

/// Application-owned refresh technique. Provider-specific snapshot encodings
/// are derived only inside the activation adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvRefreshPublicationTechnique {
    Full,
    Incremental,
}

/// One complete base-table watermark that is known before a refresh write is
/// admitted. A provider encoder may render this fact, but may not alter it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshPublicationBase {
    table_fqn: String,
    table_uuid: String,
    from_snapshot: Option<i64>,
    to_snapshot: i64,
}

impl MvRefreshPublicationBase {
    pub(crate) fn try_new(
        table_fqn: String,
        table_uuid: String,
        from_snapshot: Option<i64>,
        to_snapshot: i64,
    ) -> Result<Self, String> {
        if table_fqn.is_empty()
            || table_uuid.is_empty()
            || from_snapshot.is_some_and(|snapshot| snapshot < 0)
            || to_snapshot < 0
        {
            return Err("invalid MV refresh publication base fact".to_string());
        }
        Ok(Self {
            table_fqn,
            table_uuid,
            from_snapshot,
            to_snapshot,
        })
    }

    pub(crate) fn table_fqn(&self) -> &str {
        &self.table_fqn
    }

    pub(crate) fn table_uuid(&self) -> &str {
        &self.table_uuid
    }

    pub(crate) const fn from_snapshot(&self) -> Option<i64> {
        self.from_snapshot
    }

    pub(crate) const fn to_snapshot(&self) -> i64 {
        self.to_snapshot
    }
}

/// Complete immutable intent known before the writer commits. It deliberately
/// has no row-count or committed-version field: those facts cannot be
/// fabricated by SQL preparation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshPublicationIntent {
    refresh_id: i64,
    mv_id: i64,
    marker_token: String,
    technique: MvRefreshPublicationTechnique,
    bases: Vec<MvRefreshPublicationBase>,
    definition_fingerprint: String,
    target_catalog: String,
    target_namespace: String,
    target_name: String,
    staging_branch: String,
    partition_spec_replacement: Option<ConnectorManagedPartitionSpecReplacement>,
}

impl MvRefreshPublicationIntent {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        refresh_id: i64,
        mv_id: i64,
        marker_token: String,
        technique: MvRefreshPublicationTechnique,
        bases: Vec<MvRefreshPublicationBase>,
        definition_fingerprint: String,
        target_catalog: String,
        target_namespace: String,
        target_name: String,
        staging_branch: String,
    ) -> Result<Self, String> {
        if refresh_id <= 0
            || mv_id <= 0
            || marker_token.is_empty()
            || bases.is_empty()
            || definition_fingerprint.is_empty()
            || target_catalog.is_empty()
            || target_namespace.is_empty()
            || target_name.is_empty()
            || staging_branch.is_empty()
        {
            return Err("invalid MV refresh publication intent".to_string());
        }
        let mut table_fqns = std::collections::BTreeSet::new();
        let mut table_uuids = std::collections::BTreeSet::new();
        if bases.iter().any(|base| {
            !table_fqns.insert(base.table_fqn.as_str())
                || !table_uuids.insert(base.table_uuid.as_str())
        }) {
            return Err("MV refresh publication intent has duplicate base identity".to_string());
        }
        Ok(Self {
            refresh_id,
            mv_id,
            marker_token,
            technique,
            bases,
            definition_fingerprint,
            target_catalog,
            target_namespace,
            target_name,
            staging_branch,
            partition_spec_replacement: None,
        })
    }

    pub(crate) const fn refresh_id(&self) -> i64 {
        self.refresh_id
    }
    pub(crate) const fn mv_id(&self) -> i64 {
        self.mv_id
    }
    pub(crate) fn marker_token(&self) -> &str {
        &self.marker_token
    }
    pub(crate) const fn technique(&self) -> MvRefreshPublicationTechnique {
        self.technique
    }
    pub(crate) fn bases(&self) -> &[MvRefreshPublicationBase] {
        &self.bases
    }
    pub(crate) fn definition_fingerprint(&self) -> &str {
        &self.definition_fingerprint
    }
    pub(crate) fn target_catalog(&self) -> &str {
        &self.target_catalog
    }
    pub(crate) fn target_namespace(&self) -> &str {
        &self.target_namespace
    }
    pub(crate) fn target_name(&self) -> &str {
        &self.target_name
    }
    pub(crate) fn staging_branch(&self) -> &str {
        &self.staging_branch
    }

    pub(crate) fn with_partition_spec_replacement(
        mut self,
        replacement: ConnectorManagedPartitionSpecReplacement,
    ) -> Self {
        self.partition_spec_replacement = Some(replacement);
        self
    }

    pub fn partition_spec_replacement(&self) -> Option<&ConnectorManagedPartitionSpecReplacement> {
        self.partition_spec_replacement.as_ref()
    }
}

/// Facts admitted only after the provider has committed the exact write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshCommittedFacts {
    intent: MvRefreshPublicationIntent,
    committed_version: ConnectorCommittedVersion,
    resulting_row_count: i64,
    committed_partitioning: Option<ConnectorCommittedPartitioning>,
}

impl MvRefreshCommittedFacts {
    pub(crate) fn from_write_receipt(
        intent: MvRefreshPublicationIntent,
        receipt: &ConnectorWriteReceipt,
    ) -> Result<Self, String> {
        let committed_version = receipt
            .committed_version()
            .cloned()
            .ok_or_else(|| "MV refresh write committed without a provider version".to_string())?;
        let resulting_row_count =
            i64::try_from(receipt.resulting_row_count().ok_or_else(|| {
                "MV refresh write committed without resulting row-count fact".to_string()
            })?)
            .map_err(|_| "MV refresh committed row count exceeds i64 range".to_string())?;
        let committed_partitioning = receipt.committed_partitioning().cloned();
        if intent.partition_spec_replacement().is_some() != committed_partitioning.is_some() {
            return Err(
                "MV refresh committed partitioning does not match the requested transition"
                    .to_string(),
            );
        }
        Ok(Self {
            intent,
            committed_version,
            resulting_row_count,
            committed_partitioning,
        })
    }

    pub fn intent(&self) -> &MvRefreshPublicationIntent {
        &self.intent
    }
    pub fn committed_version(&self) -> &ConnectorCommittedVersion {
        &self.committed_version
    }
    pub const fn resulting_row_count(&self) -> i64 {
        self.resulting_row_count
    }
    pub fn committed_partitioning(&self) -> Option<&ConnectorCommittedPartitioning> {
        self.committed_partitioning.as_ref()
    }
}

/// Facts available only after the publication action is known committed and
/// provider finalization completed. The frontend constructs this value after
/// recording the catalog action; it never changes the provider outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshPublishedFacts {
    committed: MvRefreshCommittedFacts,
    publication_version: ConnectorCommittedVersion,
}

impl MvRefreshPublishedFacts {
    pub fn try_new(
        committed: MvRefreshCommittedFacts,
        publication_version: ConnectorCommittedVersion,
    ) -> Result<Self, String> {
        Ok(Self {
            committed,
            publication_version,
        })
    }

    pub fn committed(&self) -> &MvRefreshCommittedFacts {
        &self.committed
    }
    pub fn publication_version(&self) -> &ConnectorCommittedVersion {
        &self.publication_version
    }
}

/// Application facts retained for the typed join activation path. The SQL
/// artifact contains its logical plan only; persistence and refresh-context
/// reconstruction stay at this application boundary.
pub(crate) struct MvFirstRefreshLogicalContext {
    pub(crate) mv_definition: novarocks::mv::persistence::definition::StoredMvDefinition,
    pub(crate) canonical_select_query: sqlparser::ast::Query,
    pub(crate) base_refs: Vec<novarocks_catalog::identifier::TableIdentity>,
    pub(crate) pin: SqlMvSnapshotPin,
    pub(crate) previous_snapshot_ids: BTreeMap<String, i64>,
    pub(crate) previous_table_uuids: BTreeMap<String, String>,
    pub(crate) target_table_uuid: String,
    pub(crate) affected_partitions: novarocks::mv::model::AffectedTargetPartitions,
    /// Base-table materializations admitted while the first-refresh artifact
    /// was prepared.  The overlays retain their exact control leases, files,
    /// and snapshot facts until activation creates the request-local binding
    /// store. `None` identifies artifact modes that have not admitted a
    /// logical join input and therefore cannot use this handoff.
    pub(crate) frozen_base_overlays:
        Option<Vec<novarocks::catalog_application::query_materializer::QueryLocalTableOverlay>>,
}

/// Application envelope for a join first-refresh artifact.
///
/// The canonical SELECT is deliberately not compiled until activation, after
/// the frontend has retained the exact planning lease and admitted the query
/// execution.  Carrying only immutable refresh facts here prevents an
/// unscoped logical plan from outliving the request-local table bindings that
/// must prepare its scans.
pub(crate) struct MvFirstRefreshLogicalArtifact {
    context: MvFirstRefreshLogicalContext,
}

impl MvFirstRefreshLogicalArtifact {
    pub(crate) fn from_join_context(context: MvFirstRefreshLogicalContext) -> Self {
        Self { context }
    }

    pub(crate) fn into_context(self) -> MvFirstRefreshLogicalContext {
        self.context
    }

    pub(crate) const fn root_hash_column(&self) -> &str {
        MV_JOIN_APPLY_KEY_COLUMN_NAME
    }
}

pub(crate) enum MvFirstRefreshExecutionArtifact {
    Sql(SqlMvFirstRefreshArtifact),
    Logical(MvFirstRefreshLogicalArtifact),
}

impl MvFirstRefreshExecutionArtifact {
    pub(crate) fn root_hash_column(&self) -> &str {
        match self {
            Self::Sql(sql) => sql.root_hash_column(),
            Self::Logical(logical) => logical.root_hash_column(),
        }
    }
}

/// Application handoff before a first-refresh writer is admitted.
#[derive(Clone)]
pub(crate) struct MvFirstRefreshWriteRequest {
    target_catalog: String,
    target_namespace: String,
    target_name: String,
    staging_branch: String,
    current_catalog: Option<String>,
    current_database: String,
    expected_target_snapshot_id: Option<i64>,
    target_table: ConnectorTableHandle,
    write_input_fields: Arc<[arrow::datatypes::Field]>,
    observed_binding: ConnectorExecutionBindingKey,
    operation_id: ConnectorWriteOperationId,
}

impl MvFirstRefreshWriteRequest {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        target_catalog: String,
        target_namespace: String,
        target_name: String,
        staging_branch: String,
        current_catalog: Option<String>,
        current_database: String,
        expected_target_snapshot_id: Option<i64>,
        target_table: ConnectorTableHandle,
        write_input_fields: Arc<[arrow::datatypes::Field]>,
        observed_binding: ConnectorExecutionBindingKey,
        operation_id: ConnectorWriteOperationId,
    ) -> Result<Self, String> {
        if target_catalog.is_empty()
            || target_namespace.is_empty()
            || target_name.is_empty()
            || staging_branch.is_empty()
            || current_database.is_empty()
            || write_input_fields.is_empty()
            || target_table.owner() != &observed_binding.instance_id
        {
            return Err("invalid MV first-refresh write request identity".to_string());
        }
        Ok(Self {
            target_catalog,
            target_namespace,
            target_name,
            staging_branch,
            current_catalog,
            current_database,
            expected_target_snapshot_id,
            target_table,
            write_input_fields,
            observed_binding,
            operation_id,
        })
    }

    pub(crate) fn target_catalog(&self) -> &str {
        &self.target_catalog
    }

    pub(crate) fn target_namespace(&self) -> &str {
        &self.target_namespace
    }

    pub(crate) fn target_name(&self) -> &str {
        &self.target_name
    }

    pub(crate) fn staging_branch(&self) -> &str {
        &self.staging_branch
    }

    pub(crate) fn current_catalog(&self) -> Option<&str> {
        self.current_catalog.as_deref()
    }

    pub(crate) fn current_database(&self) -> &str {
        &self.current_database
    }

    pub(crate) const fn expected_target_snapshot_id(&self) -> Option<i64> {
        self.expected_target_snapshot_id
    }

    pub(crate) fn target_table(&self) -> &ConnectorTableHandle {
        &self.target_table
    }

    /// Immutable Arrow field facts copied from the validated target before
    /// write admission. This contains no SQL planner or provider handle.
    pub(crate) fn write_input_fields(&self) -> &[arrow::datatypes::Field] {
        &self.write_input_fields
    }

    pub(crate) fn observed_binding(&self) -> &ConnectorExecutionBindingKey {
        &self.observed_binding
    }

    pub(crate) const fn operation_id(&self) -> ConnectorWriteOperationId {
        self.operation_id
    }
}

/// Opaque application artifact consumed by the staging lifecycle.
pub struct PreparedMvFirstRefreshWrite {
    request: MvFirstRefreshWriteRequest,
    artifact: MvFirstRefreshExecutionArtifact,
    primary_cohort: ConnectorWriteCohortId,
    write_mode: MvStagedRefreshWriteMode,
    publication_intent: MvRefreshPublicationIntent,
}

impl PreparedMvFirstRefreshWrite {
    pub fn operation_id(&self) -> ConnectorWriteOperationId {
        self.request.operation_id()
    }

    pub fn primary_cohort(&self) -> ConnectorWriteCohortId {
        self.primary_cohort
    }

    pub(crate) fn observed_binding(&self) -> &ConnectorExecutionBindingKey {
        self.request.observed_binding()
    }

    pub(crate) fn target_table(&self) -> &ConnectorTableHandle {
        self.request.target_table()
    }

    pub(crate) fn write_input_fields(&self) -> &[arrow::datatypes::Field] {
        self.request.write_input_fields()
    }

    pub(crate) fn root_hash_column(&self) -> &str {
        self.artifact.root_hash_column()
    }

    pub(crate) fn target_catalog(&self) -> &str {
        self.request.target_catalog()
    }

    pub(crate) fn target_namespace(&self) -> &str {
        self.request.target_namespace()
    }

    pub(crate) fn target_name(&self) -> &str {
        self.request.target_name()
    }

    pub(crate) fn staging_branch(&self) -> &str {
        self.request.staging_branch()
    }

    pub(crate) fn current_catalog(&self) -> Option<&str> {
        self.request.current_catalog()
    }

    pub(crate) fn current_database(&self) -> &str {
        self.request.current_database()
    }

    pub(crate) const fn expected_target_snapshot_id(&self) -> Option<i64> {
        self.request.expected_target_snapshot_id()
    }

    pub(crate) const fn write_mode(&self) -> MvStagedRefreshWriteMode {
        self.write_mode
    }

    pub(crate) fn into_full_overwrite(mut self) -> Self {
        self.write_mode = MvStagedRefreshWriteMode::FullOverwrite;
        self
    }

    pub(crate) fn with_publication_intent(
        mut self,
        publication_intent: MvRefreshPublicationIntent,
    ) -> Self {
        self.publication_intent = publication_intent;
        self
    }

    pub(crate) fn publication_intent(&self) -> &MvRefreshPublicationIntent {
        &self.publication_intent
    }

    pub(crate) fn into_execution_artifact(self) -> MvFirstRefreshExecutionArtifact {
        self.artifact
    }
}

pub(crate) struct MvFirstRefreshWritePreparer;

impl MvFirstRefreshWritePreparer {
    pub(crate) fn prepare(
        request: MvFirstRefreshWriteRequest,
        artifact: SqlMvFirstRefreshArtifact,
        publication_intent: MvRefreshPublicationIntent,
    ) -> Result<PreparedMvFirstRefreshWrite, String> {
        Self::prepare_artifact(
            request,
            MvFirstRefreshExecutionArtifact::Sql(artifact),
            MvStagedRefreshWriteMode::Append,
            publication_intent,
        )
    }

    pub(crate) fn prepare_full_overwrite(
        request: MvFirstRefreshWriteRequest,
        artifact: SqlMvFirstRefreshArtifact,
        publication_intent: MvRefreshPublicationIntent,
    ) -> Result<PreparedMvFirstRefreshWrite, String> {
        Self::prepare_artifact(
            request,
            MvFirstRefreshExecutionArtifact::Sql(artifact),
            MvStagedRefreshWriteMode::FullOverwrite,
            publication_intent,
        )
    }

    pub(crate) fn prepare_join_logical(
        request: MvFirstRefreshWriteRequest,
        context: MvFirstRefreshLogicalContext,
        publication_intent: MvRefreshPublicationIntent,
    ) -> Result<PreparedMvFirstRefreshWrite, String> {
        Self::prepare_artifact(
            request,
            MvFirstRefreshExecutionArtifact::Logical(
                MvFirstRefreshLogicalArtifact::from_join_context(context),
            ),
            MvStagedRefreshWriteMode::Append,
            publication_intent,
        )
    }

    fn prepare_artifact(
        request: MvFirstRefreshWriteRequest,
        artifact: MvFirstRefreshExecutionArtifact,
        write_mode: MvStagedRefreshWriteMode,
        publication_intent: MvRefreshPublicationIntent,
    ) -> Result<PreparedMvFirstRefreshWrite, String> {
        if publication_intent.target_catalog() != request.target_catalog()
            || publication_intent.target_namespace() != request.target_namespace()
            || publication_intent.target_name() != request.target_name()
            || publication_intent.staging_branch() != request.staging_branch()
        {
            return Err(
                "MV first-refresh publication intent does not match write target".to_string(),
            );
        }
        let operation_id = request.operation_id();
        Ok(PreparedMvFirstRefreshWrite {
            request,
            artifact,
            primary_cohort: ConnectorWriteCohortId::primary(operation_id),
            write_mode,
            publication_intent,
        })
    }
}

/// Application activation shape for an incremental IMV change-stream write.
pub(crate) enum MvIncrementalExecutionArtifact {
    CanonicalQuery,
    /// The join shape is frozen before connector admission, but construction
    /// of its SQL logical plan waits for the exact query-local target binding.
    /// A pre-admission artifact must never fabricate an unbound SQL token.
    JoinLogical {
        mode: MvIncrementalJoinMode,
    },
}

/// Application handoff before an incremental write is admitted. It carries
/// request lifecycle identity but no provider table or prepared writer.
pub(crate) struct MvIncrementalWriteRequest {
    pub(crate) target_catalog: String,
    pub(crate) target_namespace: String,
    pub(crate) target_name: String,
    pub(crate) staging_branch: String,
    pub(crate) current_catalog: Option<String>,
    pub(crate) current_database: String,
    pub(crate) expected_target_snapshot_id: Option<i64>,
    pub(crate) observed_binding: ConnectorExecutionBindingKey,
    pub(crate) operation_id: ConnectorWriteOperationId,
}

impl MvIncrementalWriteRequest {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        target_catalog: String,
        target_namespace: String,
        target_name: String,
        staging_branch: String,
        current_catalog: Option<String>,
        current_database: String,
        expected_target_snapshot_id: Option<i64>,
        observed_binding: ConnectorExecutionBindingKey,
        operation_id: ConnectorWriteOperationId,
    ) -> Result<Self, String> {
        if target_catalog.is_empty()
            || target_namespace.is_empty()
            || target_name.is_empty()
            || staging_branch.is_empty()
            || current_database.is_empty()
        {
            return Err("invalid MV incremental write request identity".to_string());
        }
        Ok(Self {
            target_catalog,
            target_namespace,
            target_name,
            staging_branch,
            current_catalog,
            current_database,
            expected_target_snapshot_id,
            observed_binding,
            operation_id,
        })
    }
}

#[cfg(test)]
mod incremental_tests {
    use super::*;

    #[test]
    fn publication_intent_rejects_duplicate_base_identity() {
        let base = MvRefreshPublicationBase::try_new(
            "ice.db.base".to_string(),
            "uuid-1".to_string(),
            None,
            7,
        )
        .expect("base");
        let error = MvRefreshPublicationIntent::try_new(
            1,
            2,
            "token".to_string(),
            MvRefreshPublicationTechnique::Full,
            vec![base.clone(), base],
            "fingerprint".to_string(),
            "ice".to_string(),
            "db".to_string(),
            "mv".to_string(),
            "__nova_mv_1".to_string(),
        )
        .expect_err("duplicate base must fail");
        assert_eq!(
            error,
            "MV refresh publication intent has duplicate base identity"
        );
    }

    #[test]
    fn sqlx2_mv_incremental_request_rejects_missing_staging_identity() {
        let result = MvIncrementalWriteRequest::try_new(
            "ice".to_string(),
            "db".to_string(),
            "mv".to_string(),
            String::new(),
            Some("ice".to_string()),
            "db".to_string(),
            Some(7),
            ConnectorExecutionBindingKey {
                instance_id: novarocks_spi::connector::ConnectorInstanceId::parse("ice")
                    .expect("instance"),
                incarnation: novarocks_spi::connector::ConnectorInstanceIncarnation::from_bytes(
                    [1; 16],
                ),
            },
            ConnectorWriteOperationId::from_bytes([2; 16]),
        );
        match result {
            Err(error) => assert_eq!(error, "invalid MV incremental write request identity"),
            Ok(_) => panic!("missing staging branch must fail"),
        }
    }
}

/// Opaque incremental artifact owned by the application staging lifecycle.
pub struct PreparedMvIncrementalWrite {
    request: MvIncrementalWriteRequest,
    logical_context: MvFirstRefreshLogicalContext,
    mode: MvIncrementalWriteMode,
    evidence: MvIncrementalRewriteEvidence,
    execution_artifact: MvIncrementalExecutionArtifact,
    publication_intent: MvRefreshPublicationIntent,
}

impl PreparedMvIncrementalWrite {
    pub fn operation_id(&self) -> ConnectorWriteOperationId {
        self.request.operation_id
    }

    pub fn primary_cohort(&self) -> ConnectorWriteCohortId {
        ConnectorWriteCohortId::primary(self.request.operation_id)
    }

    pub(crate) fn publication_intent(&self) -> &MvRefreshPublicationIntent {
        &self.publication_intent
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        MvIncrementalWriteRequest,
        MvFirstRefreshLogicalContext,
        MvIncrementalWriteMode,
        MvIncrementalRewriteEvidence,
        MvIncrementalExecutionArtifact,
        MvRefreshPublicationIntent,
    ) {
        (
            self.request,
            self.logical_context,
            self.mode,
            self.evidence,
            self.execution_artifact,
            self.publication_intent,
        )
    }
}

pub(crate) struct MvIncrementalWritePreparer;

impl MvIncrementalWritePreparer {
    pub(crate) fn prepare(
        request: MvIncrementalWriteRequest,
        logical_context: MvFirstRefreshLogicalContext,
        mode: MvIncrementalWriteMode,
        evidence: MvIncrementalRewriteEvidence,
        execution_artifact: MvIncrementalExecutionArtifact,
        publication_intent: MvRefreshPublicationIntent,
    ) -> Result<PreparedMvIncrementalWrite, String> {
        if logical_context.base_refs.is_empty() || logical_context.pin.is_empty() {
            return Err("MV incremental write requires pinned base facts".to_string());
        }
        if logical_context.pin.len() != logical_context.base_refs.len() {
            return Err("MV incremental write has incomplete base snapshot pins".to_string());
        }
        if publication_intent.target_catalog() != request.target_catalog
            || publication_intent.target_namespace() != request.target_namespace
            || publication_intent.target_name() != request.target_name
            || publication_intent.staging_branch() != request.staging_branch
        {
            return Err(
                "MV incremental publication intent does not match write target".to_string(),
            );
        }
        Ok(PreparedMvIncrementalWrite {
            request,
            logical_context,
            mode,
            evidence,
            execution_artifact,
            publication_intent,
        })
    }
}
