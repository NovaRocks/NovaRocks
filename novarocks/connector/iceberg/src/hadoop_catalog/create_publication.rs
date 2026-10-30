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

//! Empty-table publication uses the same complete installation as UPDATE.
use super::publication::{
    FrozenInstallation, HadoopHintFinalization, not_dispatched, original_binding, phase_binding,
    rejected,
};
use super::*;
use crate::access_binding::IcebergReadBinding;
use crate::catalog::publication::EvidenceContext;
use crate::catalog::publication_dispatch::{
    DispatchCompletion, FinalizationWork, PreparationFailure, PreparationResult,
    PreparedCatalogDispatch, PreparedPublication,
};
use crate::commit::attempt::observation::DispatchEvidence;
#[cfg(test)]
use novarocks_spi::connector::ExternalMutationFinalization;
use novarocks_spi::connector::{
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorOperationControl,
    ConnectorRequestContext, ExternalMutationEffect,
};

impl HadoopFileSystemCatalog {
    pub(crate) async fn prepare_conditional_create(
        self: Arc<Self>,
        attempt: HadoopCreateAttempt,
        no_op_if_exists: bool,
        context: &ConnectorRequestContext,
    ) -> PreparationResult<PreparedPublication> {
        context.check_active()?;
        self.validate_table_root(&attempt.ident, &attempt.table_location)?;
        if attempt
            .metadata
            .properties()
            .contains_key("write.metadata.path")
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Hadoop catalog does not support write.metadata.path",
            )
            .into());
        }
        let original = original_binding(&self, context)?;
        if !self
            .version_inventory(&attempt.ident, original.clone())
            .await?
            .files
            .is_empty()
        {
            return if no_op_if_exists {
                Ok(PreparedPublication::NoOp)
            } else {
                Err(PreparationFailure(ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::AlreadyExists,
                    "Iceberg table already exists",
                )))
            };
        }
        let access = original
            .resolve_access(&attempt.facts.metadata_location)
            .map_err(publication::connector_error)?;
        if !access
            .operator()
            .info()
            .capability()
            .write_with_if_not_exists
        {
            return Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "Hadoop catalog storage lacks native conditional install",
            )
            .into());
        }
        context.check_active()?;
        Ok(PreparedPublication::Mutation {
            binding: None,
            dispatch: Box::new(HadoopPreparedCreate {
                client: self,
                attempt,
                no_op_if_exists,
                original,
            }),
        })
    }
}
struct HadoopPreparedCreate {
    client: Arc<HadoopFileSystemCatalog>,
    attempt: HadoopCreateAttempt,
    no_op_if_exists: bool,
    original: IcebergReadBinding,
}
#[async_trait]
impl PreparedCatalogDispatch for HadoopPreparedCreate {
    async fn dispatch_once(self: Box<Self>, context: EvidenceContext) -> DispatchCompletion {
        let binding = match phase_binding(&self.original, context) {
            Ok(binding) => binding,
            Err(error) => return not_dispatched(error),
        };
        // Observe a residual hint before the complete inventory recheck. A
        // late creator that sees the winner's hint must also see its version
        // before deciding to delete; an initially empty directory needs no
        // DELETE that could later erase the winner's finalization.
        let hint = HadoopFileSystemCatalog::version_hint_path(&self.attempt.table_location);
        let residual_hint = match versions::probe_path(&binding, &hint).await {
            Ok(metadata) => metadata.is_some(),
            Err(error) => return not_dispatched(error),
        };
        match self
            .client
            .version_inventory(&self.attempt.ident, binding.clone())
            .await
        {
            Err(error) => return not_dispatched(error),
            Ok(inventory) if !inventory.files.is_empty() => {
                return if self.no_op_if_exists {
                    DispatchCompletion::evidence(DispatchEvidence::Applied {
                        proof: crate::catalog::transaction::CommitProof::no_op(),
                    })
                } else {
                    rejected(
                        ConnectorMutationFailureKind::AlreadyExists,
                        "create-inventory",
                        "Iceberg table already exists",
                    )
                };
            }
            Ok(_) => (),
        }
        // Residual hints are not table identity. Remove one before installing v1;
        // a failed removal never grants permission to start the installation.
        if let Err(error) = HadoopFileSystemCatalog::check_publication_budget(Some(context)) {
            return not_dispatched(error);
        }
        if residual_hint && let Err(error) = publication::delete_authorized(&binding, &hint).await {
            return not_dispatched(error);
        }
        let install = FrozenInstallation {
            location: self.attempt.facts.metadata_location.clone(),
            uuid: self.attempt.metadata.uuid(),
            bytes: self.attempt.metadata_bytes.clone(),
            digest: self.attempt.facts.metadata_digest.clone(),
        };
        let evidence = install
            .install(
                &self.client,
                &binding,
                context,
                ConnectorMutationFailureKind::AlreadyExists,
                self.no_op_if_exists,
            )
            .await;
        let applied = matches!(&evidence, DispatchEvidence::Applied { proof } if proof.effect == ExternalMutationEffect::Applied);
        DispatchCompletion {
            evidence,
            finalization: if applied {
                Some(Box::new(HadoopHintFinalization {
                    _client: self.client,
                    table_location: self.attempt.table_location,
                    version: 1,
                    original: self.original,
                }) as Box<dyn FinalizationWork>)
            } else {
                None
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::publication::CatalogEvidencePolicy;
    use crate::commit::attempt::observation::{PublicationJournal, PublicationReceipts};
    use crate::commit::model::OperationToken;
    use crate::commit::publication_descriptor::AttributionPlan;
    use novarocks_spi::connector::{
        ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorMutationOperationId,
        ConnectorProviderId, ConnectorStopOwner, ExternalMutationEffect, ExternalMutationEvidence,
        ExternalMutationOutcome, ProviderBindingEpoch,
    };

    async fn fixture(
        location: &str,
        conditional: bool,
        existing: bool,
    ) -> (
        Arc<HadoopFileSystemCatalog>,
        HadoopCreateAttempt,
        ConnectorRequestContext,
        PublicationJournal<String>,
    ) {
        let catalog = Arc::new(super::super::tests::test_catalog(location));
        let namespace = NamespaceIdent::new("db".into());
        if existing {
            catalog
                .create_fixture_table(&namespace, super::super::tests::test_creation("t"))
                .await
                .unwrap();
        }
        let operation = ConnectorMutationOperationId::from_bytes([31; 16]);
        let attempt = catalog
            .prepare_create_attempt(
                &namespace,
                super::super::tests::test_creation("t"),
                uuid::Uuid::from_bytes(operation.to_bytes())
                    .simple()
                    .to_string(),
            )
            .unwrap();
        let context = ConnectorRequestContext::try_new(
            std::time::Instant::now() + Duration::from_secs(5),
            ConnectorStopOwner::new().view(),
            65536,
            131072,
        )
        .unwrap();
        let journal = PublicationJournal::new(OperationToken::from_mutation(operation));
        journal
            .prepare_catalog_with_receipts(
                ExternalMutationEvidence::try_new(
                    2,
                    ConnectorInstanceDescriptor {
                        provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                        instance_id: ConnectorInstanceId::parse("hadoop-create-test").unwrap(),
                    },
                    ProviderBindingEpoch::from_bytes([3; 16]),
                    operation,
                    "create_table",
                    Bytes::from_static(b"{}"),
                )
                .unwrap(),
                PublicationReceipts::for_effects(
                    attempt.facts.table_uuid.clone(),
                    "no-op-without-attempt-identity".into(),
                ),
                vec![format!("conditional={conditional}")],
                attempt.metadata_bytes.to_vec(),
                AttributionPlan::ExactMetadata {
                    table_uuid: attempt.metadata.uuid(),
                    location: attempt.facts.metadata_location.clone(),
                    digest: attempt.facts.metadata_digest.clone(),
                },
                context.clone(),
            )
            .unwrap();
        (catalog, attempt, context, journal)
    }
    async fn publish(
        catalog: Arc<HadoopFileSystemCatalog>,
        attempt: HadoopCreateAttempt,
        conditional: bool,
        context: ConnectorRequestContext,
        journal: &PublicationJournal<String>,
    ) -> ExternalMutationOutcome<String> {
        let entry = journal
            .publish_once(
                context,
                CatalogEvidencePolicy::default(),
                move |_, _, context| async move {
                    catalog
                        .prepare_conditional_create(attempt, conditional, &context)
                        .await
                },
            )
            .await
            .unwrap();
        journal.catalog_entry_outcome(entry)
    }
    #[test]
    fn created_ack_and_exact_response_loss_keep_the_frozen_identity() {
        crate::hadoop_catalog::tests::on_bridge(async move {
            for loss in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let (catalog, attempt, context, journal) =
                    fixture(directory.path().to_str().unwrap(), false, false).await;
                let facts = attempt.facts.clone();
                let bytes = attempt.metadata_bytes.clone();
                if loss {
                    catalog.inject_test_fault(HadoopCatalogTestFault::AfterConditionalResponseLoss);
                }
                assert!(
                    matches!(publish(catalog,attempt,false,context,&journal).await,
                ExternalMutationOutcome::KnownCommitted { cleanup: _, effect:ExternalMutationEffect::Applied,receipt,finalization:ExternalMutationFinalization::Complete} if receipt==facts.table_uuid)
                );
                assert_eq!(std::fs::read(facts.metadata_location).unwrap(), bytes);
                assert_eq!(
                    std::fs::read_to_string(
                        directory.path().join("db/t/metadata/version-hint.text")
                    )
                    .unwrap(),
                    "1\n"
                );
            }
        });
    }
    #[test]
    fn hint_failure_is_finalization_after_a_decisive_create() {
        crate::hadoop_catalog::tests::on_bridge(async move {
            for fault in [
                HadoopCatalogTestFault::BeforeHintWrite,
                HadoopCatalogTestFault::AfterHintWriteResponseLoss,
            ] {
                let directory = tempfile::tempdir().unwrap();
                let (catalog, attempt, context, journal) =
                    fixture(directory.path().to_str().unwrap(), false, false).await;
                let uuid = attempt.facts.table_uuid.clone();
                catalog.inject_test_fault(fault);
                assert!(
                    matches!(publish(catalog,attempt,false,context,&journal).await,ExternalMutationOutcome::KnownCommitted{ cleanup: _, effect:ExternalMutationEffect::Applied, receipt, finalization:ExternalMutationFinalization::Failed(_)} if receipt==uuid)
                );
                assert!(
                    directory
                        .path()
                        .join("db/t/metadata/v1.metadata.json")
                        .exists()
                );
            }
        });
    }

    #[test]
    fn residual_hint_is_removed_before_v1_but_failed_removal_never_installs() {
        crate::hadoop_catalog::tests::on_bridge(async move {
            for blocked in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let (catalog, attempt, context, journal) =
                    fixture(directory.path().to_str().unwrap(), false, false).await;
                let hint = std::path::Path::new(&attempt.table_location)
                    .join("metadata/version-hint.text");
                std::fs::create_dir_all(hint.parent().unwrap()).unwrap();
                if blocked {
                    std::fs::create_dir(&hint).unwrap();
                } else {
                    std::fs::write(&hint, b"77\n").unwrap();
                }
                let version = attempt.facts.metadata_location.clone();
                let result = publish(catalog, attempt, false, context, &journal).await;
                if blocked {
                    assert!(matches!(
                        result,
                        ExternalMutationOutcome::KnownUncommitted { .. }
                    ));
                    assert!(!std::path::Path::new(&version).exists());
                    assert!(hint.is_dir());
                } else {
                    assert!(matches!(
                        result,
                        ExternalMutationOutcome::KnownCommitted {
                            effect: ExternalMutationEffect::Applied,
                            finalization: ExternalMutationFinalization::Complete,
                            ..
                        }
                    ));
                    assert!(std::path::Path::new(&version).is_file());
                    assert_eq!(std::fs::read_to_string(hint).unwrap(), "1\n");
                }
            }
        });
    }

    #[test]
    fn existing_inventory_rejects_or_noops_without_reading_invalid_metadata() {
        crate::hadoop_catalog::tests::on_bridge(async move {
            for no_op in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let (catalog, attempt, context, journal) =
                    fixture(directory.path().to_str().unwrap(), no_op, true).await;
                std::fs::write(&attempt.facts.metadata_location, b"{").unwrap();
                let outcome = publish(catalog, attempt, no_op, context, &journal).await;
                match outcome {
                    ExternalMutationOutcome::KnownCommitted {
                        effect: ExternalMutationEffect::NoOp,
                        receipt,
                        ..
                    } if no_op => assert_eq!(receipt, "no-op-without-attempt-identity"),
                    ExternalMutationOutcome::KnownUncommitted { failure, .. } if !no_op => {
                        assert_eq!(failure.kind(), ConnectorMutationFailureKind::AlreadyExists)
                    }
                    other => panic!("unexpected existing inventory outcome: {other:?}"),
                }
            }
        });
    }
}
