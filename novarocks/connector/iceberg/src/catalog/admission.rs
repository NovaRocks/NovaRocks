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

//! Operation-shaped, side-effect-free catalog admission.
//!
//! Design: ADR-0169 (docs/adr/ADR-0169-read-only-hms-and-single-writer-admission.md)

use super::error::CatalogUnsupported;
use super::{CatalogCreateIntent, CatalogNamespaceName, CatalogTableName};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind, ConnectorRequestInitiation};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CatalogInitiation {
    Statement,
    StatementJob,
    JobAttempt,
    Background,
}

impl From<ConnectorRequestInitiation> for CatalogInitiation {
    fn from(value: ConnectorRequestInitiation) -> Self {
        match value {
            ConnectorRequestInitiation::Statement => Self::Statement,
            ConnectorRequestInitiation::JobAttempt => Self::JobAttempt,
            ConnectorRequestInitiation::Background => Self::Background,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CatalogOperation {
    CreateNamespace,
    DropNamespace,
    CreateTable(CatalogCreateIntent),
    DropTable,
    AnchorWrittenMetadata,
    AlterSchema,
    AlterProperties,
    AlterPartitionSpec,
    CreateBranch,
    DropBranch,
    CreateTag,
    DropTag,
    FastForwardBranch,
    CreateView,
    ReplaceView,
    DropView,
    Append,
    Overwrite,
    RowDelta,
    RowMutation,
    CopyOnWrite,
    Truncate,
    RegisterFiles,
    ExpireSnapshots,
    RewriteManifests,
    RemoveOrphanFiles,
    RewriteDataFiles,
    RewritePositionDeletes,
    Statistics,
    CreateDocuments,
    UpdateDocuments,
    PublishDocuments,
    DropDocuments,
}

impl CatalogOperation {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::CreateNamespace => "create namespace",
            Self::DropNamespace => "drop namespace",
            Self::CreateTable(CatalogCreateIntent::EmptyTable) => "create table",
            Self::CreateTable(CatalogCreateIntent::CreateTableAsSelect) => "CREATE TABLE AS SELECT",
            Self::DropTable => "drop table",
            Self::AnchorWrittenMetadata => "anchor written metadata",
            Self::AlterSchema => "alter schema",
            Self::AlterProperties => "alter properties",
            Self::AlterPartitionSpec => "alter partition spec",
            Self::CreateBranch => "create branch",
            Self::DropBranch => "drop branch",
            Self::CreateTag => "create tag",
            Self::DropTag => "drop tag",
            Self::FastForwardBranch => "fast-forward branch",
            Self::CreateView => "create view",
            Self::ReplaceView => "replace view",
            Self::DropView => "drop view",
            Self::Append => "append",
            Self::Overwrite => "overwrite",
            Self::RowDelta => "row delta",
            Self::RowMutation => "row mutation",
            Self::CopyOnWrite => "copy-on-write",
            Self::Truncate => "truncate",
            Self::RegisterFiles => "register files",
            Self::ExpireSnapshots => "expire snapshots",
            Self::RewriteManifests => "rewrite manifests",
            Self::RemoveOrphanFiles => "remove orphan files",
            Self::RewriteDataFiles => "rewrite data files",
            Self::RewritePositionDeletes => "rewrite position deletes",
            Self::Statistics => "statistics",
            Self::CreateDocuments => "create application documents",
            Self::UpdateDocuments => "update application documents",
            Self::PublishDocuments => "publish application documents",
            Self::DropDocuments => "drop application documents",
        }
    }

    pub(crate) fn validate_target(
        self,
        target: &CatalogAdmissionTarget,
    ) -> Result<(), CatalogUnsupported> {
        let namespace_operation = matches!(self, Self::CreateNamespace | Self::DropNamespace);
        if namespace_operation != matches!(target, CatalogAdmissionTarget::Namespace(_)) {
            return Err(CatalogUnsupported::new(
                "catalog admission target does not match its operation",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CatalogAdmissionTarget {
    Namespace(CatalogNamespaceName),
    Table(CatalogTableName),
}
impl From<CatalogTableName> for CatalogAdmissionTarget {
    fn from(value: CatalogTableName) -> Self {
        Self::Table(value)
    }
}
impl From<CatalogNamespaceName> for CatalogAdmissionTarget {
    fn from(value: CatalogNamespaceName) -> Self {
        Self::Namespace(value)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CatalogAdmissionRequest {
    pub(crate) operation: CatalogOperation,
    pub(crate) target: CatalogAdmissionTarget,
    pub(crate) initiation: CatalogInitiation,
}
impl CatalogAdmissionRequest {
    pub(crate) fn new(
        operation: CatalogOperation,
        target: impl Into<CatalogAdmissionTarget>,
        initiation: impl Into<CatalogInitiation>,
    ) -> Self {
        Self {
            operation,
            target: target.into(),
            initiation: initiation.into(),
        }
    }
    pub(crate) fn statement(operation: CatalogOperation, table: CatalogTableName) -> Self {
        Self::new(operation, table, CatalogInitiation::Statement)
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CatalogAdmission {
    Admitted,
    AdmittedAwaitingCompletion,
}

pub(crate) fn connector_unsupported(reason: CatalogUnsupported) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        reason.message().to_string(),
    )
}
