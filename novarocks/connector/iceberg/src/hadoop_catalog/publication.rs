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

//! Design: ADR-0196 (docs/adr/ADR-0196-hadoop-test-catalog-complete-version-installation.md)
//! Frozen Hadoop version installation. The journal owns the verdict; hints
//! are independently bounded finalization after complete, non-replacing install.
use super::*;
use crate::access_binding::IcebergReadBinding;
use crate::catalog::publication::EvidenceContext;
use crate::catalog::publication_dispatch::{
    DispatchCompletion, FinalizationWork, PreparationResult, PreparedCatalogDispatch,
    PreparedPublication,
};
use crate::catalog::transaction::{CommitProof, MetadataPredecessorObservation};
use crate::commit::attempt::observation::{DispatchEvidence, ProtocolObservation};
use crate::commit::model::FrozenRequest;
use novarocks_spi::connector::{
    ConnectorError, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorOperationControl, ConnectorRequestContext, ExternalMutationFinalization,
};

pub(super) fn connector_error(error: ConnectorError) -> Error {
    Error::new(
        ErrorKind::Unexpected,
        "Hadoop catalog storage operation failed",
    )
    .with_source(error)
}
pub(super) fn file_error(error: novarocks_fs::FileError) -> Error {
    connector_error(ConnectorError::from(error))
}
pub(super) fn not_dispatched(error: Error) -> DispatchCompletion {
    DispatchCompletion::evidence(DispatchEvidence::NotDispatched {
        failure: crate::commit::attempt::before_dispatch_failure(&error),
    })
}
pub(super) fn rejected(
    kind: ConnectorMutationFailureKind,
    stage: &'static str,
    message: &'static str,
) -> DispatchCompletion {
    DispatchCompletion::evidence(DispatchEvidence::Rejected {
        failure: ConnectorMutationFailure::new(kind, message),
        observation: ProtocolObservation {
            adapter: "hadoop",
            stage,
            ..Default::default()
        },
    })
}
pub(super) fn unknown_error(stage: &'static str, error: Error) -> DispatchCompletion {
    DispatchCompletion::evidence(DispatchEvidence::Unknown {
        failure: crate::commit::attempt::before_dispatch_failure(&error),
        observation: ProtocolObservation {
            adapter: "hadoop",
            stage,
            ..Default::default()
        },
    })
}
pub(super) fn original_binding(
    client: &HadoopFileSystemCatalog,
    preparation: &ConnectorRequestContext,
) -> Result<IcebergReadBinding> {
    client
        .binding
        .as_ref()
        .map(|binding| binding.for_request(preparation.clone()))
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Hadoop catalog has no admitted filesystem capability",
            )
        })
}
pub(super) fn phase_binding(
    original: &IcebergReadBinding,
    context: EvidenceContext,
) -> Result<IcebergReadBinding> {
    let request = original.request_context().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            "Hadoop publication requires an admitted request",
        )
    })?;
    Ok(original.for_request(context.for_io(request).map_err(connector_error)?))
}

fn local_error(error: std::io::Error) -> Error {
    let kind = match error.kind() {
        std::io::ErrorKind::PermissionDenied => novarocks_fs::FileErrorKind::Permission,
        std::io::ErrorKind::NotFound => novarocks_fs::FileErrorKind::NotFound,
        _ => novarocks_fs::FileErrorKind::Transient,
    };
    file_error(novarocks_fs::FileError::with_source(
        kind,
        "Hadoop local filesystem operation failed",
        error,
    ))
}

/// Local catalog syscalls stay on the retained joining bridge. Object requests
/// keep the exact admitted operator and are awaited through their real result.
pub(super) async fn delete_authorized(binding: &IcebergReadBinding, path: &str) -> Result<()> {
    let access = binding.resolve_access(path).map_err(connector_error)?;
    if let Some(control) = binding.operation_control() {
        control.check_active().map_err(connector_error)?;
    }
    let relative = access.paths()[0].operator_relative_path();
    if access.scheme() == novarocks_fs::FsScheme::Local {
        let root = access
            .root()
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "Local Hadoop access has no root"))?;
        match std::fs::remove_file(std::path::Path::new(root).join(relative)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(local_error(error)),
        }
    } else {
        access
            .operator()
            .delete(relative)
            .await
            .map_err(|error| file_error(novarocks_fs::map_object_store_listing_error(error)))
    }
}

/// Both CREATE and UPDATE use this exact installation and readback classifier.
/// It holds only immutable request facts, never a second publication verdict.
pub(super) struct FrozenInstallation {
    pub location: String,
    pub uuid: uuid::Uuid,
    pub bytes: Bytes,
    pub digest: String,
}
impl FrozenInstallation {
    pub fn new(location: String, metadata: &TableMetadata) -> Result<Self> {
        let bytes = Bytes::from(serde_json::to_vec(metadata)?);
        Ok(Self {
            location,
            uuid: metadata.uuid(),
            digest: hex_digest(&bytes),
            bytes,
        })
    }
    pub async fn install(
        &self,
        _client: &HadoopFileSystemCatalog,
        binding: &IcebergReadBinding,
        context: EvidenceContext,
        foreign_kind: ConnectorMutationFailureKind,
        no_op: bool,
    ) -> DispatchEvidence {
        let failure = |error: Error| DispatchEvidence::NotDispatched {
            failure: crate::commit::attempt::before_dispatch_failure(&error),
        };
        if let Err(error) = HadoopFileSystemCatalog::check_publication_budget(Some(context)) {
            return failure(error);
        }
        let access = match binding.resolve_access(&self.location) {
            Ok(access) => access,
            Err(error) => return failure(connector_error(error)),
        };
        if access.scheme() != novarocks_fs::FsScheme::Local
            && let Some(parent) =
                std::path::Path::new(access.paths()[0].operator_relative_path()).parent()
        {
            let parent = parent.to_string_lossy();
            if !parent.is_empty()
                && let Err(error) = access.operator().create_dir(&format!("{parent}/")).await
            {
                return failure(file_error(novarocks_fs::map_object_store_listing_error(
                    error,
                )));
            }
        }
        if let Err(error) = HadoopFileSystemCatalog::check_publication_budget(Some(context)) {
            return failure(error);
        }
        #[cfg(test)]
        if _client.take_test_fault(HadoopCatalogTestFault::BeforeConditionalUnsupported) {
            return failure(file_error(FileError::new(
                FileErrorKind::Unsupported,
                "injected unsupported conditional storage operation",
            )));
        }
        #[cfg(test)]
        if _client.take_test_fault(HadoopCatalogTestFault::BeforeConditionalRequest) {
            return failure(Error::new(
                ErrorKind::Unexpected,
                "injected failure before Hadoop conditional install",
            ));
        }
        let cancellation = FileCancellation::from_connector_request(
            binding
                .request_context()
                .expect("phase binding has request"),
        );
        let conditional = access
            .create_if_absent(0, self.bytes.clone(), &cancellation)
            .await;
        #[cfg(test)]
        let conditional = match conditional {
            Ok(ConditionalCreateOutcome::Created)
                if _client
                    .take_test_fault(HadoopCatalogTestFault::AfterConditionalResponseLoss) =>
            {
                Err(FileError::new(
                    FileErrorKind::Transient,
                    "injected conditional install response loss",
                ))
            }
            Ok(ConditionalCreateOutcome::AlreadyExists)
                if _client.take_test_fault(
                    HadoopCatalogTestFault::AfterExistingConditionalResponseLoss,
                ) =>
            {
                Err(FileError::new(
                    FileErrorKind::Transient,
                    "injected conditional existing response loss",
                ))
            }
            Ok(_)
                if _client.take_test_fault(HadoopCatalogTestFault::AfterConditionalUnsupported) =>
            {
                Err(FileError::new(
                    FileErrorKind::Unsupported,
                    "injected unsupported response after conditional install",
                ))
            }
            value => value,
        };
        let (exists, install_error) = match conditional {
            Ok(ConditionalCreateOutcome::Created) => {
                return DispatchEvidence::Applied {
                    proof: self.proof(),
                };
            }
            Ok(ConditionalCreateOutcome::AlreadyExists) => (true, None),
            Err(error) => (false, Some(file_error(error))),
        };
        #[cfg(test)]
        if _client.take_test_fault(HadoopCatalogTestFault::BeforeConditionalReadDelete) {
            delete_authorized(binding, &self.location)
                .await
                .expect("injected exact target removal");
        }
        #[cfg(test)]
        if _client.take_test_fault(HadoopCatalogTestFault::BeforeConditionalReadEvidenceExpiry) {
            tokio::time::sleep_until(context.deadline().into()).await;
        }
        let readback = self.read_exact(_client, binding, context).await;
        match readback {
            Ok((metadata, digest)) if metadata.uuid() == self.uuid && digest == self.digest => {
                if let Some(error) = install_error {
                    tracing::warn!(stage="conditional-install", error=%error, "Hadoop install acknowledged by exact positive readback");
                }
                DispatchEvidence::Applied {
                    proof: self.proof(),
                }
            }
            Ok(_) if exists && no_op => DispatchEvidence::Applied {
                proof: CommitProof::no_op(),
            },
            Ok(_) if exists => DispatchEvidence::Rejected {
                failure: ConnectorMutationFailure::new(
                    foreign_kind,
                    "Hadoop metadata version slot already contains another complete request",
                ),
                observation: ProtocolObservation {
                    adapter: "hadoop",
                    stage: "conditional-install-exists",
                    ..Default::default()
                },
            },
            readback => {
                let error = match (install_error, readback) {
                    (Some(error), Err(read_error)) => {
                        tracing::warn!(stage="conditional-install-readback", error=%read_error, "Hadoop readback failed after installation error");
                        error
                    }
                    (Some(error), _) => error,
                    (None, Err(error)) => error,
                    (None, Ok(_)) => unreachable!("complete conditional rejection handled above"),
                };
                DispatchEvidence::Unknown {
                    failure: crate::commit::attempt::before_dispatch_failure(&error),
                    observation: ProtocolObservation {
                        adapter: "hadoop",
                        stage: "conditional-install-readback",
                        ..Default::default()
                    },
                }
            }
        }
    }
    async fn read_exact(
        &self,
        _client: &HadoopFileSystemCatalog,
        binding: &IcebergReadBinding,
        context: EvidenceContext,
    ) -> Result<(TableMetadata, String)> {
        HadoopFileSystemCatalog::check_publication_budget(Some(context))?;
        #[cfg(test)]
        if _client.take_test_fault(HadoopCatalogTestFault::AuthoritativeV1Read) {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "injected exact Hadoop metadata readback failure",
            ));
        }
        let io = crate::fs_io::build_file_io_for_location(&self.location, binding.clone());
        let bytes = io.new_input(&self.location)?.read().await?;
        let metadata = serde_json::from_slice::<TableMetadata>(&bytes).map_err(|error| {
            Error::new(
                ErrorKind::DataInvalid,
                "Hadoop exact metadata readback is invalid",
            )
            .with_source(ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::CorruptData,
                error.to_string(),
            ))
        })?;
        Ok((metadata, hex_digest(&bytes)))
    }
    fn proof(&self) -> CommitProof {
        CommitProof::applied(None)
            .with_table_uuid(self.uuid.to_string())
            .with_metadata(self.location.clone(), self.digest.clone())
    }
}

pub(super) struct HadoopHintFinalization {
    pub _client: Arc<HadoopFileSystemCatalog>,
    pub table_location: String,
    pub version: u32,
    pub original: IcebergReadBinding,
}
#[async_trait]
impl FinalizationWork for HadoopHintFinalization {
    async fn run(self: Box<Self>, context: EvidenceContext) -> ExternalMutationFinalization {
        let result = async {
            HadoopFileSystemCatalog::check_publication_budget(Some(context))?;
            #[cfg(test)]
            if self
                ._client
                .take_test_fault(HadoopCatalogTestFault::BeforeHintWrite)
            {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "injected failure before Hadoop version-hint write",
                ));
            }
            let binding = phase_binding(&self.original, context)?;
            let path = HadoopFileSystemCatalog::version_hint_path(&self.table_location);
            let access = binding.resolve_access(&path).map_err(connector_error)?;
            let relative = access.paths()[0].operator_relative_path();
            if let Some(control) = binding.operation_control() {
                control.check_active().map_err(connector_error)?;
            }
            if access.scheme() == novarocks_fs::FsScheme::Local {
                let root = access.root().ok_or_else(|| {
                    Error::new(ErrorKind::DataInvalid, "Local Hadoop access has no root")
                })?;
                std::fs::write(
                    std::path::Path::new(root).join(relative),
                    format!("{}\n", self.version),
                )
                .map_err(local_error)?;
            } else {
                access
                    .operator()
                    .write(relative, format!("{}\n", self.version))
                    .await
                    .map_err(|error| {
                        file_error(novarocks_fs::map_object_store_listing_error(error))
                    })?;
            }
            #[cfg(test)]
            if self
                ._client
                .take_test_fault(HadoopCatalogTestFault::AfterHintWriteResponseLoss)
            {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "injected version-hint response loss",
                ));
            }
            Ok(())
        }
        .await;
        match result {
            Ok(()) => ExternalMutationFinalization::Complete,
            Err(error) => ExternalMutationFinalization::Failed(
                crate::commit::attempt::before_dispatch_failure(&error),
            ),
        }
    }
}

impl HadoopFileSystemCatalog {
    pub(crate) fn prepare_namespace_publication(
        self: Arc<Self>,
        namespace: &NamespaceIdent,
        create: bool,
    ) -> PreparedPublication {
        PreparedPublication::Mutation {
            binding: None,
            dispatch: Box::new(HadoopPreparedNamespace {
                marker: self.namespace_marker_location(namespace),
                file_io: self.file_io.clone(),
                create,
            }),
        }
    }
    pub(crate) async fn prepare_frozen_update(
        self: Arc<Self>,
        request: FrozenRequest,
        preparation: ConnectorRequestContext,
    ) -> PreparationResult<PreparedPublication> {
        preparation.check_active()?;
        let crate::commit::model::BaseIdentity::Existing {
            uuid,
            metadata_location: base_location,
            ..
        } = request.base()
        else {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "Hadoop update requires an existing frozen base",
            )
            .into());
        };
        let original = original_binding(&self, &preparation)?;
        let current = self
            .discover_current_version(request.identifier(), original.clone(), None)
            .await?
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::TableNotFound,
                    "Hadoop update target does not exist",
                )
            })?;
        if current.metadata.uuid() != *uuid || current.file.path != *base_location {
            return Err(crate::catalog::publication_dispatch::PreparationFailure(
                ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Conflict,
                    "Frozen Hadoop metadata location or UUID no longer matches",
                ),
            ));
        }
        for requirement in request.requirements() {
            requirement.check(Some(&current.metadata))?;
        }
        let predecessor = MetadataPredecessorObservation::Candidate {
            location: base_location.clone(),
            timestamp_ms: current.metadata.last_updated_ms(),
        };
        let mut builder = current
            .metadata
            .clone()
            .into_builder(Some(base_location.clone()));
        for update in request.updates() {
            if matches!(update, crate::iceberg::TableUpdate::SetLocation { .. }) {
                return Err(Error::new(
                    ErrorKind::FeatureUnsupported,
                    "Hadoop metadata root cannot be relocated",
                )
                .into());
            }
            builder = update.clone().apply(builder)?;
        }
        let metadata = builder.build()?.metadata;
        self.validate_table_root(request.identifier(), metadata.location())?;
        if metadata.properties().contains_key("write.metadata.path") {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Hadoop catalog does not support write.metadata.path",
            )
            .into());
        }
        let version = current
            .file
            .version
            .checked_add(1)
            .filter(|v| *v <= i32::MAX as u32)
            .ok_or_else(|| {
                Error::new(ErrorKind::DataInvalid, "Hadoop metadata version exhausted")
            })?;
        let install =
            FrozenInstallation::new(Self::metadata_path(metadata.location(), version), &metadata)?;
        // Resolve capability before any dispatch, preserving the original authority.
        let access = original
            .resolve_access(&install.location)
            .map_err(connector_error)?;
        if !access
            .operator()
            .info()
            .capability()
            .write_with_if_not_exists
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Hadoop storage lacks native conditional install",
            )
            .into());
        }
        let snapshot_id = crate::catalog::dispatch::committed_snapshot_id(
            &metadata,
            request.target_ref(),
            request.ref_snapshot_after(request.target_ref()),
        )?;
        preparation.check_active()?;
        Ok(PreparedPublication::Mutation {
            binding: None,
            dispatch: Box::new(HadoopPreparedUpdate {
                client: self,
                request,
                original,
                install,
                version,
                snapshot_id,
                predecessor,
            }),
        })
    }
}
struct HadoopPreparedNamespace {
    marker: String,
    file_io: FileIO,
    create: bool,
}
#[async_trait]
impl PreparedCatalogDispatch for HadoopPreparedNamespace {
    async fn dispatch_once(self: Box<Self>, evidence: EvidenceContext) -> DispatchCompletion {
        if let Err(error) = HadoopFileSystemCatalog::check_publication_budget(Some(evidence)) {
            return not_dispatched(error);
        }
        let result = if self.create {
            match self.file_io.new_output(&self.marker) {
                Ok(output) => output.write(Bytes::new()).await,
                Err(error) => return not_dispatched(error),
            }
        } else {
            self.file_io.delete(&self.marker).await
        };
        match result {
            Ok(()) => DispatchCompletion::evidence(DispatchEvidence::Applied {
                proof: CommitProof::applied(None),
            }),
            Err(error) => unknown_error("namespace-marker", error),
        }
    }
}
struct HadoopPreparedUpdate {
    client: Arc<HadoopFileSystemCatalog>,
    request: FrozenRequest,
    original: IcebergReadBinding,
    install: FrozenInstallation,
    version: u32,
    snapshot_id: Option<i64>,
    predecessor: MetadataPredecessorObservation,
}
#[async_trait]
impl PreparedCatalogDispatch for HadoopPreparedUpdate {
    async fn dispatch_once(self: Box<Self>, context: EvidenceContext) -> DispatchCompletion {
        let binding = match phase_binding(&self.original, context) {
            Ok(binding) => binding,
            Err(error) => return not_dispatched(error),
        };
        let current = match self
            .client
            .discover_current_version(self.request.identifier(), binding.clone(), None)
            .await
        {
            Ok(Some(current)) => current,
            Ok(None) => {
                return rejected(
                    ConnectorMutationFailureKind::Conflict,
                    "base-identity",
                    "Hadoop update baseline disappeared",
                );
            }
            Err(error) => return not_dispatched(error),
        };
        let crate::commit::model::BaseIdentity::Existing {
            uuid,
            metadata_location,
            ..
        } = self.request.base()
        else {
            unreachable!()
        };
        if current.metadata.uuid() != *uuid || current.file.path != *metadata_location {
            return rejected(
                ConnectorMutationFailureKind::Conflict,
                "base-identity",
                "Hadoop update baseline changed before dispatch",
            );
        }
        let mut evidence = self
            .install
            .install(
                &self.client,
                &binding,
                context,
                ConnectorMutationFailureKind::Conflict,
                false,
            )
            .await;
        let finalization = if let DispatchEvidence::Applied { proof } = &mut evidence {
            proof.snapshot_id = self.snapshot_id;
            proof.metadata_predecessor = self.predecessor;
            Some(Box::new(HadoopHintFinalization {
                table_location: self.client.table_location(self.request.identifier()),
                _client: self.client,
                version: self.version,
                original: self.original,
            }) as Box<dyn FinalizationWork>)
        } else {
            None
        };
        DispatchCompletion {
            evidence,
            finalization,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::publication::CatalogEvidencePolicy;
    use crate::commit::attempt::observation::PublicationJournal;
    use crate::commit::model::{
        AttemptArtifacts, AttemptToken, BaseIdentity, FrozenRequestParts, OperationToken,
        RequestShape,
    };
    use novarocks_spi::connector::{
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorMutationOperationId,
        ConnectorProviderId, ConnectorStopOwner, ExternalMutationEvidence, ExternalMutationOutcome,
        ProviderBindingEpoch,
    };

    async fn fixture(
        location: &str,
    ) -> (
        Arc<HadoopFileSystemCatalog>,
        FrozenRequest,
        ConnectorRequestContext,
        PublicationJournal<()>,
    ) {
        let catalog = Arc::new(super::super::tests::test_catalog(location));
        let namespace = NamespaceIdent::new("db".into());
        let table = catalog
            .create_fixture_table(&namespace, super::super::tests::test_creation("t"))
            .await
            .unwrap();
        let operation =
            OperationToken::from_mutation(ConnectorMutationOperationId::from_bytes([6; 16]));
        let request = FrozenRequest::new(FrozenRequestParts {
            shape: RequestShape::MetadataOnly,
            target: table.identifier().clone(),
            target_ref: "main".into(),
            base: BaseIdentity::Existing {
                uuid: table.metadata().uuid(),
                parent: None,
                metadata_location: table.metadata_location().unwrap().into(),
            },
            requirements: vec![crate::iceberg::TableRequirement::UuidMatch {
                uuid: table.metadata().uuid(),
            }],
            updates: vec![crate::iceberg::TableUpdate::SetProperties {
                updates: HashMap::from([("test.publication".into(), "value".into())]),
            }],
            artifacts: AttemptArtifacts::empty(AttemptToken::new(operation, 0)),
        })
        .unwrap()
        .with_base_facts(table.metadata())
        .unwrap();
        let preparation = ConnectorRequestContext::try_new(
            std::time::Instant::now() + Duration::from_secs(5),
            ConnectorStopOwner::new().view(),
            64,
            128,
        )
        .unwrap();
        let journal = PublicationJournal::new(operation);
        journal
            .record(
                ExternalMutationEvidence::try_new(
                    2,
                    ConnectorInstanceDescriptor {
                        provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                        instance_id: ConnectorInstanceId::parse("hadoop-test").unwrap(),
                    },
                    ProviderBindingEpoch::from_bytes([2; 16]),
                    ConnectorMutationOperationId::from_bytes(operation.to_bytes()),
                    "alter_properties",
                    Bytes::from_static(b"{}"),
                )
                .unwrap(),
                (),
                &request,
            )
            .unwrap();
        (catalog, request, preparation, journal)
    }
    #[test]
    fn update_hint_failure_preserves_applied_complete_install() {
        crate::hadoop_catalog::tests::on_bridge(async move {
            let directory = tempfile::tempdir().unwrap();
            let (catalog, request, preparation, journal) =
                fixture(directory.path().to_str().unwrap()).await;
            catalog.inject_test_fault(HadoopCatalogTestFault::BeforeHintWrite);
            let entry = journal
                .publish_once(
                    preparation,
                    CatalogEvidencePolicy::default(),
                    move |_, _, context| catalog.prepare_frozen_update(request, context),
                )
                .await
                .unwrap();
            assert!(matches!(
                journal.catalog_entry_outcome(entry),
                ExternalMutationOutcome::KnownCommitted {
                    finalization: ExternalMutationFinalization::Failed(_),
                    ..
                }
            ));
            let location = directory.path().join("db/t/metadata");
            assert!(location.join("v2.metadata.json").exists());
            assert_eq!(
                std::fs::read_to_string(location.join("version-hint.text"))
                    .unwrap()
                    .trim(),
                "1"
            );
            let metadata: serde_json::Value =
                serde_json::from_slice(&std::fs::read(location.join("v2.metadata.json")).unwrap())
                    .unwrap();
            assert_eq!(metadata["properties"]["test.publication"], "value");
        });
    }
    #[test]
    fn missing_or_unparseable_hint_keeps_v1_and_advances_to_v2() {
        crate::hadoop_catalog::tests::on_bridge(async move {
            for missing in [true, false] {
                let directory = tempfile::tempdir().unwrap();
                let (catalog, request, preparation, journal) =
                    fixture(directory.path().to_str().unwrap()).await;
                let metadata = directory.path().join("db/t/metadata");
                let original = std::fs::read(metadata.join("v1.metadata.json")).unwrap();
                if missing {
                    std::fs::remove_file(metadata.join("version-hint.text")).unwrap();
                } else {
                    std::fs::write(metadata.join("version-hint.text"), "unparseable").unwrap();
                }
                let entry = journal
                    .publish_once(
                        preparation,
                        CatalogEvidencePolicy::default(),
                        move |_, _, context| catalog.prepare_frozen_update(request, context),
                    )
                    .await
                    .unwrap();
                assert!(matches!(
                    journal.catalog_entry_outcome(entry),
                    ExternalMutationOutcome::KnownCommitted { .. }
                ));
                assert_eq!(
                    std::fs::read(metadata.join("v1.metadata.json")).unwrap(),
                    original
                );
                assert!(metadata.join("v2.metadata.json").exists());
                assert_eq!(
                    std::fs::read_to_string(metadata.join("version-hint.text"))
                        .unwrap()
                        .trim(),
                    "2"
                );
            }
        });
    }
    #[test]
    fn frozen_location_or_uuid_change_is_a_definite_conflict_with_no_update_write() {
        crate::hadoop_catalog::tests::on_bridge(async move {
            for uuid_changed in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let (catalog, request, preparation, journal) =
                    fixture(directory.path().to_str().unwrap()).await;
                let metadata = directory.path().join("db/t/metadata");
                let mut external: serde_json::Value = serde_json::from_slice(
                    &std::fs::read(metadata.join("v1.metadata.json")).unwrap(),
                )
                .unwrap();
                let version = if uuid_changed {
                    external["table-uuid"] = serde_json::json!(uuid::Uuid::now_v7());
                    1
                } else {
                    2
                };
                let changed = serde_json::to_vec(&external).unwrap();
                std::fs::write(metadata.join(format!("v{version}.metadata.json")), &changed)
                    .unwrap();
                std::fs::write(metadata.join("version-hint.text"), format!("{version}\n")).unwrap();
                let entry = journal
                    .publish_once(
                        preparation,
                        CatalogEvidencePolicy::default(),
                        move |_, _, context| catalog.prepare_frozen_update(request, context),
                    )
                    .await
                    .unwrap();
                let ExternalMutationOutcome::KnownUncommitted { failure, .. } =
                    journal.catalog_entry_outcome(entry)
                else {
                    panic!("stale frozen base must be rejected")
                };
                assert_eq!(failure.kind(), ConnectorMutationFailureKind::Conflict);
                assert_eq!(
                    std::fs::read(metadata.join(format!("v{version}.metadata.json"))).unwrap(),
                    changed
                );
                assert!(
                    !metadata
                        .join(format!("v{}.metadata.json", version + 1))
                        .exists()
                );
                assert_eq!(
                    std::fs::read_to_string(metadata.join("version-hint.text"))
                        .unwrap()
                        .trim(),
                    version.to_string()
                );
            }
        });
    }
}

#[cfg(test)]
#[path = "install_tests.rs"]
mod install_tests;

#[cfg(test)]
#[path = "retry_tests.rs"]
mod retry_tests;
