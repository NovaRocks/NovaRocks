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

//! One original source preparation, projected by its exact read generation.
//!
//! This process-local receipt retains Provider facts, not an access grant or
//! executable callback. Native attempts still obtain their own access and
//! perform the observations required by the selected immutable endpoints.

use super::{ConnectorReadBinding, ConnectorReadChangeWindow, OpaquePayload, SchemaTableName};
use crate::connector::{
    ConnectorChangeWindowAdmission, ConnectorExactSemanticRevision, ConnectorProcessLocalPayload,
    ConnectorTableHandle, ConnectorTableMetadata, ConnectorTableObjectId,
};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorReadSourceSelection {
    Snapshot(i64),
    ChangeWindow(ConnectorReadChangeWindow),
}

/// The original object's pin and exact revisions. A snapshot selection has no
/// From revision; an empty source retains its original SkipEmpty disposition
/// instead of manufacturing snapshot zero or a change window.
#[derive(Clone, Debug)]
pub struct ConnectorReadSourcePreparationRequest {
    name: SchemaTableName,
    observed_table: ConnectorTableHandle,
    observed_source_facts: ConnectorProcessLocalPayload,
    object_id: ConnectorTableObjectId,
    selection: ConnectorReadSourceSelection,
    from_revision: Option<ConnectorExactSemanticRevision>,
    to_revision: ConnectorExactSemanticRevision,
}

impl ConnectorReadSourcePreparationRequest {
    pub fn snapshot(
        name: SchemaTableName,
        observed_table: ConnectorTableHandle,
        observed_source_facts: ConnectorProcessLocalPayload,
        object_id: ConnectorTableObjectId,
        snapshot_id: i64,
        revision: ConnectorExactSemanticRevision,
    ) -> Self {
        Self {
            name,
            observed_table,
            observed_source_facts,
            object_id,
            selection: ConnectorReadSourceSelection::Snapshot(snapshot_id),
            from_revision: None,
            to_revision: revision,
        }
    }

    pub fn change_window(
        name: SchemaTableName,
        observed_table: ConnectorTableHandle,
        observed_source_facts: ConnectorProcessLocalPayload,
        object_id: ConnectorTableObjectId,
        window: ConnectorReadChangeWindow,
        from_revision: ConnectorExactSemanticRevision,
        to_revision: ConnectorExactSemanticRevision,
    ) -> Self {
        Self {
            name,
            observed_table,
            observed_source_facts,
            object_id,
            selection: ConnectorReadSourceSelection::ChangeWindow(window),
            from_revision: Some(from_revision),
            to_revision,
        }
    }

    pub const fn name(&self) -> &SchemaTableName {
        &self.name
    }

    /// The exact observation which authored this pin. A later Current lookup
    /// cannot replace its schema fallback, defaults or interpretation facts.
    /// The original caller retains the full metadata and its storage lifetime.
    pub const fn observed_table(&self) -> &ConnectorTableHandle {
        &self.observed_table
    }

    /// Non-secret original observation facts. The Provider validates their
    /// exact generation, private type and agreement with observed_table before
    /// using them; these facts never authorize a metadata or storage request.
    pub const fn observed_source_facts(&self) -> &ConnectorProcessLocalPayload {
        &self.observed_source_facts
    }

    pub const fn object_id(&self) -> &ConnectorTableObjectId {
        &self.object_id
    }

    pub const fn selection(&self) -> ConnectorReadSourceSelection {
        self.selection
    }

    pub const fn from_revision(&self) -> Option<&ConnectorExactSemanticRevision> {
        self.from_revision.as_ref()
    }

    pub const fn to_revision(&self) -> &ConnectorExactSemanticRevision {
        &self.to_revision
    }
}

/// A projection cannot choose a new endpoint or change the prepared decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorReadSourceProjection {
    To,
    From,
    Delta,
}

// Design: ADR-0207 (docs/adr/ADR-0207-single-prepared-change-window-and-frozen-source-endpoints.md)
#[derive(Clone)]
pub struct ConnectorPreparedSourceRead {
    binding: ConnectorReadBinding,
    request: ConnectorReadSourcePreparationRequest,
    admission: Option<ConnectorChangeWindowAdmission>,
    payload: OpaquePayload,
    to_metadata: Arc<ConnectorTableMetadata>,
}

impl ConnectorPreparedSourceRead {
    pub const fn binding(&self) -> &ConnectorReadBinding {
        &self.binding
    }

    pub const fn request(&self) -> &ConnectorReadSourcePreparationRequest {
        &self.request
    }

    /// Only a change-window selection carries a lineage decision. Snapshot
    /// reads do not masquerade as MetadataOnly or as a full-rebuild window.
    pub const fn admission(&self) -> Option<&ConnectorChangeWindowAdmission> {
        self.admission.as_ref()
    }

    /// Complete catalog projection of the selected To schema. Its column
    /// ordinals, defaults, nominal declarations and execution bindings share
    /// the same original author; consumers never splice in Current facts.
    pub fn to_metadata(&self) -> &ConnectorTableMetadata {
        &self.to_metadata
    }

    pub(crate) fn new<T: Send + Sync + 'static>(
        binding: ConnectorReadBinding,
        request: ConnectorReadSourcePreparationRequest,
        admission: Option<ConnectorChangeWindowAdmission>,
        to_metadata: Arc<ConnectorTableMetadata>,
        facts: T,
    ) -> Self {
        Self {
            binding,
            request,
            admission,
            payload: OpaquePayload::new(facts),
            to_metadata,
        }
    }

    pub(crate) fn facts<T: 'static>(&self) -> Option<&T> {
        self.payload.downcast_ref()
    }

    pub(crate) fn permits(&self, projection: ConnectorReadSourceProjection) -> bool {
        match projection {
            ConnectorReadSourceProjection::To => true,
            ConnectorReadSourceProjection::From => matches!(
                self.admission,
                Some(ConnectorChangeWindowAdmission::Incremental { .. })
            ),
            ConnectorReadSourceProjection::Delta => matches!(
                (self.request.selection(), &self.admission),
                (
                    ConnectorReadSourceSelection::ChangeWindow(_),
                    Some(
                        ConnectorChangeWindowAdmission::Incremental { .. }
                            | ConnectorChangeWindowAdmission::MetadataOnly
                    )
                )
            ),
        }
    }
}

impl std::fmt::Debug for ConnectorPreparedSourceRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectorPreparedSourceRead")
            .field("binding", &self.binding)
            .field("selection", &self.request.selection)
            .field("admission", &self.admission)
            .finish_non_exhaustive()
    }
}

pub type ConnectorPreparedSourceReadFuture<'a> = futures::future::BoxFuture<
    'a,
    Result<ConnectorPreparedSourceRead, crate::connector::ConnectorError>,
>;

/// Existing execution-time authors borrowed by one real split opening. This
/// packet grants no new capacity or I/O authority and never crosses the wire.
/// A Provider requires it for reads carrying prepared-source provenance.
#[derive(Clone)]
pub struct ConnectorReadSplitPreparation {
    cpu: Arc<dyn crate::connector::ProviderMetadataCpuPort>,
    original: crate::connector::ConnectorOriginalResultScope,
}
impl ConnectorReadSplitPreparation {
    pub fn new(
        cpu: Arc<dyn crate::connector::ProviderMetadataCpuPort>,
        original: crate::connector::ConnectorOriginalResultScope,
    ) -> Self {
        Self { cpu, original }
    }
    pub fn cpu(&self) -> &Arc<dyn crate::connector::ProviderMetadataCpuPort> {
        &self.cpu
    }
    pub fn original(&self) -> &crate::connector::ConnectorOriginalResultScope {
        &self.original
    }
}
