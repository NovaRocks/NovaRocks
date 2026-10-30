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

//! The only place a concrete Iceberg catalog kind is chosen.
//!
//! Design: ADR-0118 (docs/adr/ADR-0118-iceberg-provider-private-catalog-owner.md)
//!
//! `IcebergCatalogKind` is a validated configuration value, and this is the one
//! function allowed to match on it. Past this point the rest of the provider
//! holds an `Arc<dyn NovaRocksCatalog>` and asks operations, never kinds.

use std::sync::Arc;

use crate::catalog_config::{IcebergCatalogConfiguration, IcebergCatalogKind};

use super::NovaRocksCatalog;

/// Builds the single catalog retained by one control generation.
pub(crate) struct NovaRocksCatalogFactory;

impl NovaRocksCatalogFactory {
    /// Wrap a client this generation already built.
    ///
    /// The generation must end up with exactly one client. Building a second
    /// one here would give it two with separate in-memory state, and they would
    /// disagree about the same lake — a table dropped through one still
    /// resolving through the other.
    pub(crate) fn adopt(
        configuration: &IcebergCatalogConfiguration,
        client: &crate::catalog_runtime::IcebergCatalogClient,
    ) -> Result<Arc<dyn NovaRocksCatalog>, String> {
        let evidence_policy =
            super::publication::CatalogEvidencePolicy::from_properties(&configuration.properties)?;
        let warehouse: Option<Arc<str>> = if configuration.warehouse_uri.is_empty() {
            None
        } else {
            Some(Arc::from(configuration.warehouse_uri.as_str()))
        };
        match configuration.kind {
            IcebergCatalogKind::Hadoop => {
                let hadoop = client.hadoop().cloned().ok_or_else(|| {
                    "Hadoop Iceberg configuration produced no Hadoop client".to_string()
                })?;
                Ok(Arc::new(
                    super::hadoop::NovaRocksHadoopCatalog::new(hadoop)
                        .with_evidence_policy(evidence_policy),
                ))
            }
            IcebergCatalogKind::Rest => {
                let endpoints = client.rest_endpoints().cloned().ok_or_else(|| {
                    "REST Iceberg configuration produced no REST endpoints".to_string()
                })?;
                Ok(Arc::new(
                    super::rest::NovaRocksRestCatalog::new(
                        endpoints,
                        warehouse,
                        client.rest_access_delegation(),
                    )
                    .with_evidence_policy(evidence_policy),
                ))
            }
            IcebergCatalogKind::Hive => {
                let selected = configuration
                    .hms_uris
                    .as_deref()
                    .ok_or_else(|| "HMS generation has no selected address".to_owned())?;
                let selected =
                    novarocks_spi::connector::SelectedThriftServiceOrigin::from_selected_address(
                        selected,
                    )
                    .map_err(|e| e.to_string())?
                    .into_owned();
                Ok(Arc::new(
                    super::hive::NovaRocksHiveCatalog::adopt(
                        client.hms().cloned().ok_or_else(|| {
                            "Hive configuration produced no exact HMS client".to_owned()
                        })?,
                        selected,
                    )
                    .with_evidence_policy(evidence_policy),
                ))
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn adopt_vended_rest_for_test(
        endpoints: Arc<crate::rest_protocol::endpoints::RestEndpoints>,
        warehouse: &str,
    ) -> Arc<dyn NovaRocksCatalog> {
        Arc::new(super::rest::NovaRocksRestCatalog::new(
            endpoints,
            Some(Arc::from(warehouse)),
            crate::catalog_runtime::RestAccessDelegationMode::Vended,
        ))
    }

    #[cfg(test)]
    pub(crate) fn adopt_hadoop_for_test(
        client: Arc<crate::hadoop_catalog::HadoopFileSystemCatalog>,
    ) -> Arc<dyn NovaRocksCatalog> {
        Arc::new(super::hadoop::NovaRocksHadoopCatalog::new(client))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::catalog::error::CatalogOutcome;
    use crate::catalog::{CatalogCreateIntent, CatalogNamespaceName, CatalogTableName};
    use crate::iceberg::TableCreation;
    use crate::iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use novarocks_fs::{FsAccessResolver, TokioFileIoRuntime, TokioFileTaskSpawner};
    use novarocks_spi::connector::{ConnectorErrorKind, ConnectorListingBound};

    /// Build a catalog for a test the way production does.
    ///
    /// Production never constructs a client here: the generation already has
    /// one and this adopts it. A second construction path used to exist, and it
    /// had already drifted -- it wrapped a different Hive client than adoption
    /// did, and it built a second client of its own, which is exactly the
    /// two-clients-one-lake failure `adopt` documents.
    async fn adopted(
        configuration: &IcebergCatalogConfiguration,
    ) -> Result<Arc<dyn NovaRocksCatalog>, String> {
        let runtime = tokio::runtime::Handle::current();
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime)),
        );
        let client = crate::catalog_runtime::build_catalog_client(
            configuration,
            binding,
            crate::iceberg::Runtime::try_current().expect("test runtime"),
        )
        .await?;
        NovaRocksCatalogFactory::adopt(configuration, &client)
    }

    fn hadoop_configuration(warehouse: &std::path::Path) -> IcebergCatalogConfiguration {
        crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.display().to_string(),
            )],
        )
        .expect("hadoop catalog configuration")
    }

    fn creation(name: &str) -> TableCreation {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema");
        TableCreation::builder()
            .name(name.to_string())
            .schema(schema)
            .build()
    }

    async fn create_fixture(
        owner: Arc<dyn NovaRocksCatalog>,
        namespace: &str,
        name: &str,
    ) -> CatalogOutcome<super::super::transaction::CommitProof> {
        use novarocks_spi::connector::*;
        let operation_id = ConnectorMutationOperationId::new();
        let request = owner
            .freeze_create_publication(
                super::super::catalog_request::CatalogCreatePublicationRequest {
                    operation_id: operation_id.to_bytes(),
                    namespace: CatalogNamespaceName::new(namespace),
                    creation: creation(name),
                    no_op_if_exists: false,
                },
            )
            .expect("freeze local create before any I/O");
        publish_fixture(owner, request, operation_id).await
    }

    async fn namespace_fixture(
        owner: Arc<dyn NovaRocksCatalog>,
    ) -> CatalogOutcome<super::super::transaction::CommitProof> {
        let operation_id = novarocks_spi::connector::ConnectorMutationOperationId::new();
        let request = super::super::catalog_request::CatalogPublicationRequest::CreateNamespace {
            namespace: CatalogNamespaceName::new("db"),
            properties: Default::default(),
            no_op_if_exists: false,
        }
        .freeze(operation_id.to_bytes())
        .unwrap();
        publish_fixture(owner, request, operation_id).await
    }

    async fn publish_fixture(
        owner: Arc<dyn NovaRocksCatalog>,
        request: super::super::catalog_request::FrozenCatalogPublication,
        operation_id: novarocks_spi::connector::ConnectorMutationOperationId,
    ) -> CatalogOutcome<super::super::transaction::CommitProof> {
        use novarocks_spi::connector::*;
        let runtime = tokio::runtime::Handle::current();
        let context = ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            ConnectorStopOwner::new().view(),
            64 * 1024,
            1024 * 1024,
        )
        .unwrap();
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime)),
        )
        .for_request(context.clone());
        super::super::admission_test_support::publish_catalog(
            owner,
            request,
            operation_id,
            ProviderBindingEpoch::from_bytes([77; 16]),
            context,
            binding,
        )
        .await
    }

    #[tokio::test]
    async fn factory_builds_one_catalog_and_hides_the_concrete_kind() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = hadoop_configuration(warehouse.path());
        let catalog = adopted(&configuration).await.expect("catalog");
        assert_eq!(catalog.implementation_name(), "hadoop");
    }

    /// The behavior change this owner exists for.
    ///
    /// A Hadoop catalog cannot store views, and it used to answer view
    /// enumeration with `false` / an empty list — turning "cannot answer" into
    /// "authoritatively none". Callers could not tell the difference, and
    /// `DROP DATABASE ... FORCE` silently relied on the fiction.
    #[tokio::test]
    async fn view_enumeration_reports_unsupported_instead_of_faking_absence() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let catalog = adopted(&hadoop_configuration(warehouse.path()))
            .await
            .expect("catalog");

        let exists = catalog
            .view_exists(CatalogTableName::new("db", "v"))
            .await
            .expect_err("view_exists must not answer false");
        assert_eq!(exists.kind(), ConnectorErrorKind::Unsupported);

        let listed = catalog
            .list_views(
                CatalogNamespaceName::new("db"),
                novarocks_spi::connector::ConnectorListingBound::V1,
            )
            .await
            .expect_err("list_views must not answer with an empty list");
        assert_eq!(listed.kind(), ConnectorErrorKind::Unsupported);

        let loaded = catalog
            .load_view(CatalogTableName::new("db", "v"))
            .await
            .expect_err("load_view must not answer not-found");
        assert_eq!(loaded.kind(), ConnectorErrorKind::Unsupported);
    }

    /// Hadoop enumeration has no paging, so its directory listing is bounded
    /// as a whole: an over-bound namespace is refused with its typed bound on
    /// both the generation path and the admitted-read path, never truncated.
    #[test]
    fn hadoop_table_listing_over_its_bound_is_refused_not_truncated() {
        let runtime = tokio::runtime::Runtime::new().expect("test runtime");
        let bridge = crate::resources::IcebergCatalogRuntime::new(runtime.handle().clone());
        bridge
            .block_on(async move {
                let warehouse = tempfile::tempdir().expect("warehouse");
                let catalog = adopted(&hadoop_configuration(warehouse.path()))
                    .await
                    .expect("catalog");
                assert!(matches!(
                    namespace_fixture(Arc::clone(&catalog)).await,
                    CatalogOutcome::KnownCommitted { .. }
                ));
                // A hint alone is no table. Publish real complete versions through
                // the production owner so this fixture tests the enumeration bound.
                for table in ["a", "b", "c"] {
                    assert!(matches!(
                        create_fixture(Arc::clone(&catalog), "db", table).await,
                        CatalogOutcome::KnownCommitted { .. }
                    ));
                }
                let runtime = tokio::runtime::Handle::current();
                let binding = || {
                    crate::access_binding::IcebergReadBinding::new(
                        None,
                        FsAccessResolver::new(),
                        Arc::new(TokioFileIoRuntime::new(runtime.clone())),
                        Arc::new(TokioFileTaskSpawner::new(runtime.clone())),
                    )
                    .for_request(
                        novarocks_spi::connector::ConnectorRequestContext::try_new(
                            std::time::Instant::now() + std::time::Duration::from_secs(5),
                            novarocks_spi::connector::ConnectorStopOwner::new().view(),
                            1024,
                            4096,
                        )
                        .unwrap(),
                    )
                };
                let namespace = || CatalogNamespaceName::new("db");
                let exact = ConnectorListingBound {
                    entries: 3,
                    ..ConnectorListingBound::V1
                };
                let over = ConnectorListingBound {
                    entries: 2,
                    ..exact
                };

                assert_eq!(
                    catalog.list_tables(namespace(), exact).await.unwrap(),
                    ["a", "b", "c"]
                );
                assert_eq!(
                    catalog
                        .list_tables_for_read(namespace(), binding(), exact)
                        .await
                        .unwrap(),
                    ["a", "b", "c"]
                );
                for error in [
                    catalog.list_tables(namespace(), over).await.unwrap_err(),
                    catalog
                        .list_tables_for_read(namespace(), binding(), over)
                        .await
                        .unwrap_err(),
                ] {
                    assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
                    assert!(error.message().contains("entries bound"), "{error}");
                }
            })
            .expect("publication bridge must exit before test cleanup");
    }

    /// Absence must be readable from the error's kind alone.
    ///
    /// The owner classifies reads purely by `iceberg::ErrorKind`; the
    /// message-sniffing fallback that used to recognize a missing table from
    /// wording like "does not exist" is gone. That only works if every backend
    /// tags absence honestly, and two of them did not: the vendored REST client
    /// reported a 404 as the generic `Unexpected`, and the vendored HMS client
    /// funnelled `NoSuchObjectException` into it. Both are patched. This pins
    /// the contract for the backend that can be exercised in-process, so a
    /// regression shows up as a failing test rather than as `IF EXISTS`
    /// reporting an unavailable control plane.
    #[tokio::test]
    async fn absence_is_reported_as_not_found_rather_than_unavailable() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let catalog = adopted(&hadoop_configuration(warehouse.path()))
            .await
            .expect("catalog");
        assert!(matches!(
            namespace_fixture(Arc::clone(&catalog)).await,
            CatalogOutcome::KnownCommitted { .. }
        ));

        let missing = catalog
            .load_table(CatalogTableName::new("db", "absent"))
            .await
            .expect_err("loading an absent table must fail");
        assert_eq!(
            missing.kind(),
            ConnectorErrorKind::NotFound,
            "an absent table must be NotFound, not an unavailable control plane"
        );

        assert!(
            !catalog
                .table_exists(CatalogTableName::new("db", "absent"))
                .await
                .expect("table_exists answers for a catalog that can tell"),
        );
    }

    /// Admission is a per-request question, not a per-catalog flag: the same
    /// catalog accepts one create intent and refuses the other.
    #[tokio::test]
    async fn hadoop_admits_empty_create_and_refuses_ctas_before_any_side_effect() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let catalog = adopted(&hadoop_configuration(warehouse.path()))
            .await
            .expect("catalog");
        assert!(matches!(
            namespace_fixture(Arc::clone(&catalog)).await,
            CatalogOutcome::KnownCommitted { .. }
        ));

        let empty = create_fixture(Arc::clone(&catalog), "db", "t_empty").await;
        assert!(
            matches!(empty, CatalogOutcome::KnownCommitted { .. }),
            "atomic empty create: {empty:?}"
        );

        let before = std::fs::read_dir(warehouse.path())
            .expect("warehouse readable")
            .count();
        let reason = catalog
            .admit_operation(
                &super::super::admission::CatalogOperation::CreateTable(
                    CatalogCreateIntent::CreateTableAsSelect,
                ),
                &CatalogTableName::new("db", "t_ctas").into(),
            )
            .expect_err("CTAS must be refused before source execution");
        assert!(reason.message().contains("staged-create"));
        assert_eq!(
            std::fs::read_dir(warehouse.path())
                .expect("warehouse readable")
                .count(),
            before,
            "a refused CTAS must not create anything in the warehouse"
        );
    }

    #[tokio::test]
    async fn frozen_create_cannot_replace_an_existing_hadoop_incarnation() {
        let warehouse = tempfile::tempdir().unwrap();
        let catalog = adopted(&hadoop_configuration(warehouse.path()))
            .await
            .unwrap();
        assert!(matches!(
            namespace_fixture(Arc::clone(&catalog)).await,
            CatalogOutcome::KnownCommitted { .. }
        ));
        assert!(matches!(
            create_fixture(Arc::clone(&catalog), "db", "t").await,
            CatalogOutcome::KnownCommitted { .. }
        ));
        let before = catalog
            .load_table(CatalogTableName::new("db", "t"))
            .await
            .unwrap()
            .into_parts()
            .0
            .into_static_table()
            .unwrap();
        let location = before.metadata_location().unwrap();
        let bytes = before
            .file_io()
            .new_input(location)
            .unwrap()
            .read()
            .await
            .unwrap();
        let collision = create_fixture(Arc::clone(&catalog), "db", "t").await;
        assert!(
            matches!(collision,CatalogOutcome::KnownUncommitted{ref failure}
            if failure.kind()==novarocks_spi::connector::ConnectorMutationFailureKind::AlreadyExists),
            "{collision:?}"
        );
        let after = catalog
            .load_table(CatalogTableName::new("db", "t"))
            .await
            .unwrap()
            .into_parts()
            .0
            .into_static_table()
            .unwrap();
        assert_eq!(after.metadata(), before.metadata());
        assert_eq!(
            after
                .file_io()
                .new_input(location)
                .unwrap()
                .read()
                .await
                .unwrap(),
            bytes
        );
    }

    #[tokio::test]
    async fn existing_table_admission_does_not_read_a_target_before_preparation() {
        let warehouse = tempfile::tempdir().unwrap();
        let catalog = adopted(&hadoop_configuration(warehouse.path()))
            .await
            .unwrap();
        let before = std::fs::read_dir(warehouse.path()).unwrap().count();
        catalog
            .admit_operation(
                &super::super::admission::CatalogOperation::Append,
                &CatalogTableName::new("db", "absent").into(),
            )
            .expect("pure operation admission");
        assert_eq!(std::fs::read_dir(warehouse.path()).unwrap().count(), before);
    }

    #[tokio::test]
    async fn operation_admission_matrix_is_owned_by_each_catalog_before_io() {
        use crate::catalog::admission::*;
        use CatalogOperation::*;
        let operations = [
            CreateNamespace,
            DropNamespace,
            CreateTable(CatalogCreateIntent::EmptyTable),
            CreateTable(CatalogCreateIntent::CreateTableAsSelect),
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
        ];
        let endpoint = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        endpoint.set_nonblocking(true).unwrap();
        for kind in ["hive", "hadoop", "rest"] {
            let warehouse = tempfile::tempdir().unwrap();
            let properties = vec![
                ("iceberg.catalog.type".into(), kind.into()),
                (
                    "iceberg.catalog.warehouse".into(),
                    warehouse.path().display().to_string(),
                ),
                (
                    "hive.metastore.uris".into(),
                    format!("thrift://{}", endpoint.local_addr().unwrap()),
                ),
                (
                    "iceberg.catalog.uri".into(),
                    format!("http://{}", endpoint.local_addr().unwrap()),
                ),
            ];
            let configuration =
                crate::catalog_config::parse_catalog_configuration("ice", &properties).unwrap();
            let catalog = adopted(&configuration).await.unwrap();
            for operation in operations {
                let target = if matches!(operation, CreateNamespace | DropNamespace) {
                    CatalogAdmissionTarget::Namespace(CatalogNamespaceName::new("db"))
                } else {
                    CatalogTableName::new("db", "t").into()
                };
                for initiation in [
                    CatalogInitiation::Statement,
                    CatalogInitiation::StatementJob,
                    CatalogInitiation::JobAttempt,
                    CatalogInitiation::Background,
                ] {
                    let result = catalog.admit(&CatalogAdmissionRequest::new(
                        operation,
                        target.clone(),
                        initiation,
                    ));
                    let unsupported_hadoop = matches!(
                        operation,
                        CreateTable(CatalogCreateIntent::CreateTableAsSelect)
                            | CreateView
                            | ReplaceView
                            | DropView
                            | CreateDocuments
                            | UpdateDocuments
                            | PublishDocuments
                            | DropDocuments
                    );
                    if kind == "hive"
                        || kind == "hadoop"
                            && (unsupported_hadoop || initiation == CatalogInitiation::Background)
                    {
                        let refused = result.unwrap_err();
                        if kind == "hive" {
                            assert!(refused.message().contains("read-only compatibility entry"));
                            assert!(refused.message().contains(operation.name()));
                        }
                    } else {
                        assert_eq!(
                            result.unwrap(),
                            if kind == "hadoop" && initiation == CatalogInitiation::StatementJob {
                                CatalogAdmission::AdmittedAwaitingCompletion
                            } else {
                                CatalogAdmission::Admitted
                            }
                        );
                    }
                }
            }
        }
        assert_eq!(
            endpoint.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    #[tokio::test]
    async fn hadoop_create_freezer_retains_one_exact_document_without_io() {
        use crate::catalog::catalog_request::CatalogCreatePublicationRequest;
        use crate::commit::publication_descriptor::{AttributionPlan, PublicationDescriptor};
        use sha2::Digest;
        let warehouse = tempfile::tempdir().unwrap();
        let owner = adopted(&hadoop_configuration(warehouse.path()))
            .await
            .unwrap();
        let operation_id = [23; 16];
        let frozen = owner
            .freeze_create_publication(CatalogCreatePublicationRequest {
                operation_id,
                namespace: CatalogNamespaceName::new("db"),
                creation: creation("t"),
                no_op_if_exists: true,
            })
            .unwrap();
        let facts = frozen
            .conditional_create_facts()
            .expect("Hadoop exact identity");
        let metadata: serde_json::Value = serde_json::from_slice(frozen.request_bytes()).unwrap();
        assert_eq!(metadata["table-uuid"], facts.table_uuid.as_ref());
        assert_eq!(
            facts.operation_id.as_ref(),
            uuid::Uuid::from_bytes(operation_id).simple().to_string()
        );
        let digest: String = sha2::Sha256::digest(frozen.request_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_eq!(facts.metadata_digest.as_ref(), digest);
        assert!(
            matches!(frozen.attribution(), AttributionPlan::ExactMetadata { table_uuid, location, digest }
            if table_uuid.to_string() == facts.table_uuid.as_ref() && location == facts.metadata_location.as_ref() && digest == facts.metadata_digest.as_ref())
        );
        let scope: serde_json::Value = serde_json::from_str(&frozen.targets()[0]).unwrap();
        assert_eq!(scope["table-create-no-op"], true);
        let descriptor = PublicationDescriptor::catalog(
            operation_id,
            frozen.targets(),
            frozen.request_bytes().to_vec(),
            frozen.attribution().clone(),
        )
        .unwrap();
        frozen.validate_descriptor(&descriptor).unwrap();
        assert_eq!(std::fs::read_dir(warehouse.path()).unwrap().count(), 0);
        let (attempt, no_op) = frozen.into_hadoop_create().unwrap();
        assert!(no_op);
        attempt.validate_frozen_identity(operation_id).unwrap();
        let mut foreign_operation = operation_id;
        foreign_operation[0] ^= 1;
        assert!(attempt.validate_frozen_identity(foreign_operation).is_err());
        assert_eq!(
            attempt.facts().metadata_location,
            facts.metadata_location.as_ref()
        );
        assert_eq!(attempt.metadata_bytes(), descriptor.request.as_slice());
        assert_eq!(std::fs::read_dir(warehouse.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn hadoop_create_freezer_preserves_trailing_slash_protocol_location() {
        use crate::catalog::catalog_request::CatalogCreatePublicationRequest;
        let warehouse = tempfile::tempdir().unwrap();
        let configuration = hadoop_configuration(warehouse.path());
        let owner = adopted(&configuration).await.unwrap();
        let mut request = creation("t");
        // An explicit location may retain a trailing slash, but it must name
        // this identifier's configured root, including local canonicalization.
        let location = format!(
            "{}/db/t/",
            configuration.warehouse_uri.trim_end_matches('/')
        );
        request.location = Some(location.clone());
        let frozen = owner
            .freeze_create_publication(CatalogCreatePublicationRequest {
                operation_id: [25; 16],
                namespace: CatalogNamespaceName::new("db"),
                creation: request,
                no_op_if_exists: false,
            })
            .unwrap();
        let metadata: serde_json::Value = serde_json::from_slice(frozen.request_bytes()).unwrap();
        assert_eq!(metadata["location"], location.trim_end_matches('/'));
        let (attempt, _) = frozen.into_hadoop_create().unwrap();
        attempt.validate_frozen_identity([25; 16]).unwrap();
        assert_eq!(
            attempt.facts().metadata_location,
            format!(
                "{}/metadata/v1.metadata.json",
                location.trim_end_matches('/')
            )
        );
        assert_eq!(std::fs::read_dir(warehouse.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn rest_create_freezer_binds_policy_without_session_io_or_hadoop_facts() {
        use crate::catalog::catalog_request::CatalogCreatePublicationRequest;
        use crate::commit::publication_descriptor::PublicationDescriptor;
        let endpoint = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        endpoint.set_nonblocking(true).unwrap();
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[
                ("iceberg.catalog.type".into(), "rest".into()),
                (
                    "uri".into(),
                    format!("http://{}", endpoint.local_addr().unwrap()),
                ),
                ("warehouse".into(), "s3://warehouse".into()),
            ],
        )
        .unwrap();
        let owner = adopted(&configuration).await.unwrap();
        let freeze = |conditional| {
            owner
                .freeze_create_publication(CatalogCreatePublicationRequest {
                    operation_id: [24; 16],
                    namespace: CatalogNamespaceName::new("db"),
                    creation: creation("t"),
                    no_op_if_exists: conditional,
                })
                .unwrap()
        };
        let strict = freeze(false);
        let conditional = freeze(true);
        assert!(strict.conditional_create_facts().is_none());
        assert_eq!(strict.request_bytes(), conditional.request_bytes());
        let descriptor = PublicationDescriptor::catalog(
            [24; 16],
            strict.targets(),
            strict.request_bytes().to_vec(),
            strict.attribution().clone(),
        )
        .unwrap();
        strict.validate_descriptor(&descriptor).unwrap();
        assert!(conditional.validate_descriptor(&descriptor).is_err());
        let request: serde_json::Value = serde_json::from_slice(strict.request_bytes()).unwrap();
        assert_eq!(request["name"], "t");
        assert!(request.get("no-op-if-exists").is_none());
        assert_eq!(
            endpoint.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    #[tokio::test]
    async fn anchor_is_pure_admitted_no_op_without_namespace_or_pointer_registration() {
        let endpoint = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        endpoint.set_nonblocking(true).unwrap();
        for kind in ["hadoop", "rest", "hive"] {
            let warehouse = tempfile::tempdir().unwrap();
            let configuration = crate::catalog_config::parse_catalog_configuration(
                "ice",
                &[
                    ("iceberg.catalog.type".into(), kind.into()),
                    (
                        "iceberg.catalog.warehouse".into(),
                        warehouse.path().display().to_string(),
                    ),
                    (
                        "iceberg.catalog.uri".into(),
                        format!("http://{}", endpoint.local_addr().unwrap()),
                    ),
                    (
                        "hive.metastore.uris".into(),
                        format!("thrift://{}", endpoint.local_addr().unwrap()),
                    ),
                ],
            )
            .unwrap();
            let owner = adopted(&configuration).await.unwrap();
            let name = CatalogTableName::new("absent_namespace", "absent_table");
            let outcome = owner
                .anchor_written_metadata(name.clone(), Arc::from("s3://never-read/metadata.json"))
                .await;
            match (kind, outcome) {
                ("hive", CatalogOutcome::Unsupported(_)) => (),
                (
                    "hadoop" | "rest",
                    CatalogOutcome::KnownCommitted {
                        effect: novarocks_spi::connector::ExternalMutationEffect::NoOp,
                        receipt,
                        ..
                    },
                ) => assert_eq!(receipt, name),
                (_, other) => panic!("Unexpected {kind} anchor: {other:?}"),
            }
            assert_eq!(
                std::fs::read_dir(warehouse.path()).unwrap().count(),
                0,
                "Anchor must not create a namespace or metadata pointer"
            );
            assert_eq!(
                endpoint.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock,
                "Anchor must not reach catalog services"
            );
        }
    }

    #[tokio::test]
    async fn hms_direct_mutations_and_constructors_refuse_before_metastore_io() {
        let endpoint = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        endpoint.set_nonblocking(true).unwrap();
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[
                ("iceberg.catalog.type".into(), "hive".into()),
                (
                    "iceberg.catalog.warehouse".into(),
                    "s3://unused/warehouse".into(),
                ),
                (
                    "hive.metastore.uris".into(),
                    format!("thrift://{}", endpoint.local_addr().unwrap()),
                ),
            ],
        )
        .unwrap();
        let owner = adopted(&configuration).await.unwrap();
        for (operation, target) in [
            (
                super::super::admission::CatalogOperation::CreateNamespace,
                CatalogNamespaceName::new("db").into(),
            ),
            (
                super::super::admission::CatalogOperation::DropNamespace,
                CatalogNamespaceName::new("db").into(),
            ),
            (
                super::super::admission::CatalogOperation::DropTable,
                CatalogTableName::new("db", "t").into(),
            ),
        ] {
            owner
                .admit_operation(&operation, &target)
                .expect_err("HMS mutation must be refused before metastore I/O");
        }
        assert!(matches!(
            owner
                .anchor_written_metadata(
                    CatalogTableName::new("db", "t"),
                    Arc::from("s3://unused/metadata.json")
                )
                .await,
            CatalogOutcome::Unsupported(_)
        ));
        for intent in [
            CatalogCreateIntent::EmptyTable,
            CatalogCreateIntent::CreateTableAsSelect,
        ] {
            owner
                .admit_operation(
                    &super::super::admission::CatalogOperation::CreateTable(intent),
                    &CatalogTableName::new("db", "t").into(),
                )
                .expect_err("HMS refuses each create intent before I/O");
        }
        let refused = owner
            .freeze_create_publication(
                super::super::catalog_request::CatalogCreatePublicationRequest {
                    operation_id: [1; 16],
                    namespace: CatalogNamespaceName::new("db"),
                    creation: creation("t"),
                    no_op_if_exists: false,
                },
            )
            .err()
            .expect("HMS has no publication freezer");
        assert_eq!(
            refused.0.kind(),
            novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported
        );
        assert_eq!(
            endpoint.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
}
