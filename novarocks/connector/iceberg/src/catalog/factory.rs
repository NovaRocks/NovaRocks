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
                Ok(Arc::new(super::hadoop::NovaRocksHadoopCatalog::new(hadoop)))
            }
            IcebergCatalogKind::Rest => {
                let rest = client.rest().cloned().ok_or_else(|| {
                    "REST Iceberg configuration produced no REST client".to_string()
                })?;
                Ok(Arc::new(super::rest::NovaRocksRestCatalog::new(
                    rest,
                    warehouse,
                    client.rest_access_delegation(),
                )))
            }
            IcebergCatalogKind::Hive => Ok(Arc::new(super::hive::NovaRocksHiveCatalog::adopt(
                Arc::clone(client.generic()),
            ))),
        }
    }

    #[cfg(test)]
    pub(crate) fn adopt_recording_hadoop_for_test(
        client: Arc<crate::hadoop_catalog::HadoopFileSystemCatalog>,
        vendored_client: Arc<dyn crate::iceberg::Catalog>,
    ) -> Arc<dyn NovaRocksCatalog> {
        Arc::new(
            super::hadoop::NovaRocksHadoopCatalog::new_with_vendored_client_for_test(
                client,
                vendored_client,
            ),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::catalog::error::CatalogOutcome;
    use crate::catalog::transaction::{
        CreateTableTransactionRequest, TransactionIdentity, TransactionRequest,
    };
    use crate::catalog::{
        CatalogCreateIntent, CatalogNamespaceName, CatalogTableName, CatalogTransactionStart,
    };
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
        let client = crate::catalog_runtime::build_catalog_client(configuration, binding).await?;
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

    fn create_request(
        namespace: &str,
        table: &str,
        intent: CatalogCreateIntent,
    ) -> CreateTableTransactionRequest {
        CreateTableTransactionRequest {
            identity: TransactionIdentity::new("test", [1u8; 16]),
            target: CatalogTableName::new(namespace, table),
            intent,
            creation: creation(table),
            warehouse: None,
        }
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
    #[tokio::test]
    async fn hadoop_table_listing_over_its_bound_is_refused_not_truncated() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        for table in ["a", "b", "c"] {
            let metadata = warehouse.path().join("db").join(table).join("metadata");
            std::fs::create_dir_all(&metadata).expect("metadata directory");
            std::fs::write(metadata.join("version-hint.text"), b"1\n").expect("version hint");
        }
        let catalog = adopted(&hadoop_configuration(warehouse.path()))
            .await
            .expect("catalog");
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
            catalog
                .create_namespace(CatalogNamespaceName::new("db"))
                .await,
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
            catalog
                .create_namespace(CatalogNamespaceName::new("db"))
                .await,
            CatalogOutcome::KnownCommitted { .. }
        ));

        let empty = catalog
            .new_create_table_transaction(create_request(
                "db",
                "t_empty",
                CatalogCreateIntent::EmptyTable,
            ))
            .await;
        assert!(
            matches!(empty, CatalogTransactionStart::Ready(_)),
            "empty-table creation is atomic on this catalog"
        );

        let before = std::fs::read_dir(warehouse.path())
            .expect("warehouse readable")
            .count();
        let ctas = catalog
            .new_create_table_transaction(create_request(
                "db",
                "t_ctas",
                CatalogCreateIntent::CreateTableAsSelect,
            ))
            .await;
        let CatalogTransactionStart::Unsupported(reason) = &ctas else {
            panic!("CTAS must be refused on a Hadoop catalog, got {ctas:?}");
        };
        assert!(reason.message().contains("staged-create"));
        assert!(ctas.permits_cleanup());
        assert_eq!(
            std::fs::read_dir(warehouse.path())
                .expect("warehouse readable")
                .count(),
            before,
            "a refused CTAS must not create anything in the warehouse"
        );
    }

    #[tokio::test]
    async fn create_or_replace_is_refused_where_it_cannot_be_atomic() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let catalog = adopted(&hadoop_configuration(warehouse.path()))
            .await
            .expect("catalog");
        let outcome = catalog
            .new_create_or_replace_table_transaction(create_request(
                "db",
                "t",
                CatalogCreateIntent::EmptyTable,
            ))
            .await;
        assert!(matches!(outcome, CatalogTransactionStart::Unsupported(_)));
    }

    #[tokio::test]
    async fn existing_table_transactions_are_admitted_on_every_catalog() {
        let warehouse = tempfile::tempdir().expect("warehouse");
        let catalog = adopted(&hadoop_configuration(warehouse.path()))
            .await
            .expect("catalog");
        let start = catalog
            .new_transaction(TransactionRequest {
                identity: TransactionIdentity::new("test", [2u8; 16]),
                target: CatalogTableName::new("db", "t"),
                target_ref: Arc::from("main"),
                base_snapshot_id: None,
                expected_table_uuid: None,
                marker: None,
            })
            .await;
        assert!(matches!(start, CatalogTransactionStart::Ready(_)));
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
        assert!(matches!(
            owner
                .create_namespace(CatalogNamespaceName::new("db"))
                .await,
            CatalogOutcome::Unsupported(_)
        ));
        assert!(matches!(
            owner.drop_namespace(CatalogNamespaceName::new("db")).await,
            CatalogOutcome::Unsupported(_)
        ));
        assert!(matches!(
            owner.drop_table(CatalogTableName::new("db", "t")).await,
            CatalogOutcome::Unsupported(_)
        ));
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
            assert!(matches!(
                owner
                    .new_create_table_transaction(create_request("db", "t", intent))
                    .await,
                CatalogTransactionStart::Unsupported(_)
            ));
            assert!(matches!(
                owner
                    .new_create_or_replace_table_transaction(create_request("db", "t", intent))
                    .await,
                CatalogTransactionStart::Unsupported(_)
            ));
        }
        assert_eq!(
            endpoint.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
}
