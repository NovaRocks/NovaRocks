// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Value-only SQL handoff for an incremental MV change-stream write.
//!
//! Provider activation, native fragment preparation and every external
//! mutation remain deferred until the frontend owns an exact retained lease.

use std::collections::BTreeMap;

use novarocks_spi::connector::{
    ConnectorExecutionBindingKey, ConnectorRequestContext, ConnectorWriteCohortId,
    ConnectorWriteOperationId,
};

use super::first_refresh::MvFirstRefreshLogicalContext;

/// The logical execution shape frozen by SQL preparation. It contains no
/// provider handle or local executor; native fragment construction remains an
/// exact-lease activation responsibility.
pub(crate) enum MvIncrementalExecutionArtifact {
    CanonicalQuery,
    JoinLogical {
        plan: crate::sql::planner::logical::LogicalPlanNode,
        factory: crate::sql::column_id::ColumnRefFactory,
        change_stream_override:
            Option<crate::sql::planner::imv_rewrite::change_stream::ImvChangeStreamDescriptor>,
    },
}

/// The Iceberg commit semantics selected from immutable change facts. The
/// frontend can carry this value but cannot turn it into a provider operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MvIncrementalWriteMode {
    FastAppend,
    RowDelta,
}

/// Semantic evidence required by the typed IMV rewrite. It is SQL-owned and
/// intentionally independent of the provider's commit vocabulary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MvIncrementalRewriteEvidence {
    None,
    Aggregate,
    JoinAggregate,
    BranchUnionAggregate,
}

/// SQL/application handoff before an incremental change-stream plan is bound.
/// It contains no catalog handle, physical fragment, provider codec or local
/// executable program.
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
    pub(crate) connector_context: ConnectorRequestContext,
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
        connector_context: ConnectorRequestContext,
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
            connector_context,
        })
    }
}

/// Opaque, single-use incremental artifact consumed only by the application
/// route that owns staging, the exact write lease and the write session.
pub struct PreparedMvIncrementalWrite {
    request: MvIncrementalWriteRequest,
    logical_context: MvFirstRefreshLogicalContext,
    mode: MvIncrementalWriteMode,
    evidence: MvIncrementalRewriteEvidence,
    execution_artifact: MvIncrementalExecutionArtifact,
    provenance_properties: BTreeMap<String, String>,
}

impl PreparedMvIncrementalWrite {
    pub fn operation_id(&self) -> ConnectorWriteOperationId {
        self.request.operation_id
    }

    pub fn primary_cohort(&self) -> ConnectorWriteCohortId {
        ConnectorWriteCohortId::primary(self.request.operation_id)
    }

    pub fn observed_binding(&self) -> &ConnectorExecutionBindingKey {
        &self.request.observed_binding
    }

    pub(crate) fn target_catalog(&self) -> &str {
        &self.request.target_catalog
    }

    pub(crate) fn target_namespace(&self) -> &str {
        &self.request.target_namespace
    }

    pub(crate) fn target_name(&self) -> &str {
        &self.request.target_name
    }

    pub(crate) fn staging_branch(&self) -> &str {
        &self.request.staging_branch
    }

    pub(crate) fn current_catalog(&self) -> Option<&str> {
        self.request.current_catalog.as_deref()
    }

    pub(crate) fn current_database(&self) -> &str {
        &self.request.current_database
    }

    pub(crate) const fn expected_target_snapshot_id(&self) -> Option<i64> {
        self.request.expected_target_snapshot_id
    }

    pub(crate) fn connector_context(&self) -> &ConnectorRequestContext {
        &self.request.connector_context
    }

    pub(crate) const fn mode(&self) -> MvIncrementalWriteMode {
        self.mode
    }

    pub(crate) const fn evidence(&self) -> MvIncrementalRewriteEvidence {
        self.evidence
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        MvIncrementalWriteRequest,
        MvFirstRefreshLogicalContext,
        MvIncrementalWriteMode,
        MvIncrementalRewriteEvidence,
        MvIncrementalExecutionArtifact,
        BTreeMap<String, String>,
    ) {
        (
            self.request,
            self.logical_context,
            self.mode,
            self.evidence,
            self.execution_artifact,
            self.provenance_properties,
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
        provenance_properties: BTreeMap<String, String>,
    ) -> Result<PreparedMvIncrementalWrite, String> {
        if logical_context.base_refs.is_empty() || logical_context.pin.is_empty() {
            return Err("MV incremental write requires pinned base facts".to_string());
        }
        if logical_context.pin.len() != logical_context.base_refs.len() {
            return Err("MV incremental write has incomplete base snapshot pins".to_string());
        }
        Ok(PreparedMvIncrementalWrite {
            request,
            logical_context,
            mode,
            evidence,
            execution_artifact,
            provenance_properties,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_rejects_missing_staging_identity() {
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
            crate::connector::connector_request_context(
                None,
                std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )
            .expect("request context"),
        );
        match result {
            Err(error) => assert_eq!(error, "invalid MV incremental write request identity"),
            Ok(_) => panic!("missing staging branch must fail"),
        }
    }
}
