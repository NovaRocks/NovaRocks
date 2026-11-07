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

use std::collections::HashMap;
use std::sync::Arc;

use novarocks_spi::connector::ConnectorViewDefinition;
use novarocks_types::naming::normalize_identifier;

#[cfg(test)]
use crate::catalog::CatalogNamespaceName;
#[cfg(test)]
use crate::catalog::CatalogTableName;
#[cfg(test)]
use crate::catalog::error::CatalogOutcome;
pub(crate) use crate::catalog::view_request::VIEW_DIALECT_STARROCKS;
use crate::catalog::view_request::ViewDefinition;
#[cfg(test)]
use crate::catalog::view_request::{ValidatedViewReplace, ViewCreate};
use crate::iceberg::spec::{ViewMetadata, ViewRepresentation};
use crate::iceberg::{NamespaceIdent, TableIdent};
use crate::metadata_context::IcebergMetadataContext;

#[path = "views/async_listing.rs"]
pub(crate) mod async_listing;
#[path = "views/declaration_projection.rs"]
mod declaration_projection;
#[path = "views/version_identity.rs"]
mod version_identity;

const NOVAROCKS_ENGINE_NAME: &str = "novarocks";

/// Poll one catalog call on the generation's runtime.
///
/// The bridge wraps the request, so a bridge failure says nothing about
/// whether the request reached the catalog. Reads have no publication frontier,
/// so `Unavailable` is the honest answer here.
fn bridge<F, T>(runtime: &IcebergMetadataContext, future: F) -> Result<T, ConnectorError>
where
    F: std::future::Future<Output = Result<T, ConnectorError>> + Send + 'static,
    T: Send + 'static,
{
    runtime
        .resources()
        .catalog_runtime()
        .block_on(future)
        .map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::Unavailable,
                format!("Iceberg catalog runtime bridge: {error}"),
            )
        })?
}

fn view_ident(namespace: &str, view: &str) -> Result<(NamespaceIdent, TableIdent), String> {
    let namespace = normalize_identifier(namespace)?;
    let view = normalize_identifier(view)?;
    let ident = TableIdent::from_strs([namespace.as_str(), view.as_str()])
        .map_err(|error| format!("build view identity: {error}"))?;
    Ok((NamespaceIdent::new(namespace), ident))
}

fn novarocks_source_context(
    definition: &ConnectorViewDefinition,
) -> Result<(String, NamespaceIdent), String> {
    if definition.dialect != ConnectorViewDialect::StarRocks {
        return Err("unsupported SQL dialect for NovaRocks Iceberg view creation".to_string());
    }
    if definition.raw_sql.is_empty() {
        return Err("NovaRocks Iceberg view source SQL is empty".to_string());
    }
    let source_catalog = definition
        .default_catalog
        .as_ref()
        .ok_or_else(|| "NovaRocks Iceberg view source is missing default catalog".to_string())?;
    let source_catalog = normalize_identifier(source_catalog)
        .map_err(|error| format!("normalize NovaRocks view source catalog: {error}"))?;
    let source_namespace = normalize_identifier(&definition.default_namespace)
        .map_err(|error| format!("normalize NovaRocks view source namespace: {error}"))?;
    Ok((source_catalog, NamespaceIdent::new(source_namespace)))
}

pub(super) fn prepare_definition(
    definition: &ConnectorViewDefinition,
    comment: Option<&str>,
    extra_properties: &[(String, String)],
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(ViewDefinition, HashMap<String, String>), ConnectorError> {
    crate::metadata::with_pure_schema_creation(context, |creation| {
        let invalid = |message| ConnectorError::new(ConnectorErrorKind::InvalidRequest, message);
        let (source_catalog, source_namespace) =
            novarocks_source_context(definition).map_err(invalid)?;
        definition
            .validate_output_declaration()
            .map_err(|message| invalid(message.to_owned()))?;
        let schema = declaration_projection::schema(definition, creation)
            .map_err(crate::metadata::metadata_creation_error)?;
        let summary = HashMap::from([("engine-name".to_owned(), NOVAROCKS_ENGINE_NAME.to_owned())]);
        // The summary and this exact schema publish in one version operation.
        let mut properties = extra_properties.iter().cloned().collect::<HashMap<_, _>>();
        if let Some(comment) = comment
            && properties
                .insert("comment".to_string(), comment.to_string())
                .is_some()
        {
            return Err(invalid(
                "duplicate Iceberg view comment property".to_string(),
            ));
        }
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| invalid(format!("system clock before epoch: {error}")))?
            .as_millis() as i64;
        let definition = ViewDefinition {
            schema,
            sql: definition.raw_sql.to_string(),
            summary,
            default_catalog: Some(source_catalog),
            default_namespace: source_namespace,
            timestamp_ms,
        };
        Ok((definition, properties))
    })
}

#[cfg(test)]
pub(crate) fn create_view(
    runtime: &IcebergMetadataContext,
    namespace: &str,
    view: &str,
    definition: &ConnectorViewDefinition,
    comment: Option<&str>,
    replace: bool,
    extra_properties: &[(String, String)],
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<CatalogOutcome<crate::catalog::transaction::CommitProof>, String> {
    let (namespace_ident, ident) = view_ident(namespace, view)?;
    let owner = Arc::clone(runtime.novarocks_catalog());

    let target = CatalogTableName::from_identifier(&ident);
    let operation = if replace {
        crate::catalog::admission::CatalogOperation::ReplaceView
    } else {
        crate::catalog::admission::CatalogOperation::CreateView
    };
    if let Err(reason) = owner.admit_operation(&operation, &target.into()) {
        return Ok(CatalogOutcome::Unsupported(reason));
    }
    let (definition, properties) =
        prepare_definition(definition, comment, extra_properties, context)
            .map_err(|error| error.to_string())?;
    if replace {
        let target = CatalogTableName::new(ident.namespace().to_url_string(), ident.name());
        let current = runtime
            .resources()
            .catalog_runtime()
            .block_on({
                let owner = Arc::clone(&owner);
                async move { owner.load_view(target).await }
            })?
            .map_err(|error| match error.kind() {
                ConnectorErrorKind::NotFound => format!("unknown view: {ident}"),
                _ => format!("load Iceberg view {ident}: {error}"),
            })?;
        // Semantic validation precedes dispatch: a refusal here is certain,
        // while the same refusal from the server would be an unknown outcome.
        let replace = match ValidatedViewReplace::validate(ident, &current, definition, properties)
        {
            Ok(replace) => replace,
            Err(message) => return Ok(locally_refused(message)),
        };
        return publish_view_fixture(
            runtime,
            crate::catalog::catalog_request::CatalogPublicationRequest::ReplaceView(replace),
        );
    }

    let creation = match ViewCreate::validate(ident.name.clone(), definition, properties) {
        Ok(creation) => creation,
        Err(message) => return Ok(locally_refused(message)),
    };
    let namespace = CatalogNamespaceName::new(namespace_ident.to_url_string());
    publish_view_fixture(
        runtime,
        crate::catalog::catalog_request::CatalogPublicationRequest::CreateView {
            namespace,
            creation,
        },
    )
}

/// A view mutation refused before dispatch is known not to have committed.
#[cfg(test)]
fn locally_refused(message: String) -> CatalogOutcome<crate::catalog::transaction::CommitProof> {
    CatalogOutcome::uncommitted(
        novarocks_spi::connector::ConnectorMutationFailureKind::InvalidRequest,
        message,
    )
}

/// Load a view. The owner's error reaches the caller with its kind intact:
/// a catalog that cannot hold views answers `Unsupported`, which callers read
/// as "no such view". Only failures this module produces itself are
/// classified from their text.
pub(crate) fn load_view(
    runtime: &IcebergMetadataContext,
    namespace: &str,
    view: &str,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<ViewMetadata, ConnectorError> {
    load_view_metadata_for_request(runtime, namespace, view, context)
}

pub(super) fn load_view_metadata_for_request(
    runtime: &IcebergMetadataContext,
    namespace: &str,
    view: &str,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<ViewMetadata, ConnectorError> {
    let (_, ident) = view_ident(namespace, view).map_err(invalid_view_request)?;
    let owner = std::sync::Arc::clone(runtime.novarocks_catalog());
    let target =
        crate::catalog::CatalogTableName::new(ident.namespace().to_url_string(), ident.name());
    let context = context.clone();
    runtime
        .resources()
        .catalog_runtime()
        .block_on(async move { owner.load_view_for_request(target, context).await })
        .map_err(unavailable)?
        .map_err(|error| match error.kind() {
            // Absence is read from the error's kind. The owner classifies it,
            // so there is nothing here to recognize by substring.
            ConnectorErrorKind::NotFound => ConnectorError::new(
                ConnectorErrorKind::NotFound,
                format!("unknown view: {ident}"),
            ),
            _ => error,
        })
}

pub(super) async fn load_view_metadata_for_request_async(
    runtime: &IcebergMetadataContext,
    namespace: &str,
    view: &str,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<ViewMetadata, ConnectorError> {
    let (_, ident) = view_ident(namespace, view).map_err(invalid_view_request)?;
    let owner = std::sync::Arc::clone(runtime.novarocks_catalog());
    let target =
        crate::catalog::CatalogTableName::new(ident.namespace().to_url_string(), ident.name());
    let context = context.clone();
    owner
        .load_view_for_request(target, context)
        .await
        .map_err(|error| match error.kind() {
            // Absence is read from the error's kind. The owner classifies it,
            // so there is nothing here to recognize by substring.
            ConnectorErrorKind::NotFound => ConnectorError::new(
                ConnectorErrorKind::NotFound,
                format!("unknown view: {ident}"),
            ),
            _ => error,
        })
}

#[cfg(test)]
pub(crate) fn drop_view(
    runtime: &IcebergMetadataContext,
    namespace: &str,
    view: &str,
) -> Result<CatalogOutcome<crate::catalog::transaction::CommitProof>, String> {
    let (_, ident) = view_ident(namespace, view)?;
    let target = CatalogTableName::new(ident.namespace().to_url_string(), ident.name());
    if let Err(reason) = runtime.novarocks_catalog().admit_operation(
        &crate::catalog::admission::CatalogOperation::DropView,
        &target.clone().into(),
    ) {
        return Ok(CatalogOutcome::Unsupported(reason));
    }
    publish_view_fixture(
        runtime,
        crate::catalog::catalog_request::CatalogPublicationRequest::DropView(target),
    )
}

#[cfg(test)]
fn publish_view_fixture(
    runtime: &IcebergMetadataContext,
    request: crate::catalog::catalog_request::CatalogPublicationRequest,
) -> Result<CatalogOutcome<crate::catalog::transaction::CommitProof>, String> {
    let publication = runtime.clone();
    runtime.resources().catalog_runtime().block_on(async move {
        crate::catalog::admission_test_support::publish_catalog_fixture(
            &publication,
            novarocks_spi::connector::ProviderBindingEpoch::from_bytes([68; 16]),
            request,
        )
        .await
    })
}

/// Whether the view exists.
///
/// A catalog that cannot answer says so. This used to short-circuit to
/// `Ok(false)` whenever the generation had no REST client, which turned
/// "this catalog cannot hold views" into "this catalog says there is no such
/// view" — the same sentence a caller gets for a name that genuinely is not a
/// view. Callers that only want a hint may still treat `Unsupported` as "not a
/// view"; callers that need an authoritative answer now get to know they did
/// not receive one.
pub(crate) fn view_exists(
    runtime: &IcebergMetadataContext,
    namespace: &str,
    view: &str,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<bool, ConnectorError> {
    let catalog = Arc::clone(runtime.novarocks_catalog());
    let name = crate::catalog::CatalogTableName::new(namespace, view);
    let context = context.clone();
    bridge(runtime, async move {
        catalog.view_exists_for_request(name, context).await
    })
}

/// Enumerate views in a namespace.
///
/// This used to return an empty vector whenever the generation had no REST
/// client, with the reasoning that a catalog which cannot create views holds
/// none. That is true of the catalog and false of the answer: an empty list is
/// indistinguishable from an authoritative "no views here", and
/// `DROP DATABASE ... FORCE` consumed it as exactly that. It now reports what
/// the catalog actually said.
///
/// The listing is refused as a whole when it would exceed `bound`.
pub(crate) fn list_views(
    runtime: &IcebergMetadataContext,
    namespace: &str,
    bound: novarocks_spi::connector::ConnectorListingBound,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<Vec<String>, ConnectorError> {
    let catalog = Arc::clone(runtime.novarocks_catalog());
    let name = crate::catalog::CatalogNamespaceName::new(namespace);
    let context = context.clone();
    bridge(runtime, async move {
        catalog.list_views_for_request(name, context, bound).await
    })
}

pub(crate) fn materialize_view_metadata(
    request: ConnectorViewRequest,
    metadata: &ViewMetadata,
    creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
) -> Result<
    ConnectorViewMetadataValue,
    novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
> {
    creation.work(0)?;
    let (_, ident) =
        view_ident(&request.view.namespace, &request.view.view).map_err(invalid_view_request)?;
    let version = metadata
        .version_by_id(metadata.current_version_id())
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg view current version is absent from its same metadata observation",
            )
        })?;
    let selected = version
        .representations()
        .iter()
        .map(|representation| {
            let ViewRepresentation::Sql(sql) = representation;
            sql
        })
        .find(|sql| sql.dialect.eq_ignore_ascii_case(VIEW_DIALECT_STARROCKS))
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                format!("Iceberg view {ident} has no supported SQL representation"),
            )
        })?;
    let schema = metadata.schema_by_id(version.schema_id()).ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg view version schema is absent from its same metadata observation",
        )
    })?;
    if version.default_namespace().iter().count() != 1 {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "Iceberg view requires one exact default namespace component",
        )
        .into());
    }
    let columns = declaration_projection::columns(schema, creation)?;
    let version_identity = version_identity::observed(metadata, version, schema, creation)?;
    ConnectorViewMetadataValue::try_new(
        request.view,
        ConnectorViewDefinition {
            dialect: ConnectorViewDialect::StarRocks,
            raw_sql: selected.sql.as_str().into(),
            default_catalog: version.default_catalog().map(|value| value.as_str().into()),
            default_namespace: version
                .default_namespace()
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(".")
                .into(),
            columns: columns.into(),
        },
        version_identity,
        metadata
            .properties()
            .get("comment")
            .map(|value| value.as_str().into()),
        metadata
            .properties()
            .iter()
            .map(|(key, value)| (key.as_str().into(), value.as_str().into()))
            .collect(),
        &request.context,
    )
    .map_err(Into::into)
}

use crate::metadata::IcebergMetadata;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorListViewsRequest,
    ConnectorViewDialect, ConnectorViewIdentity, ConnectorViewMetadata, ConnectorViewMetadataValue,
    ConnectorViewRequest, ProviderBindingEpoch,
};

impl ConnectorViewMetadata for IcebergMetadata {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        self.descriptor()
    }

    fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation()
    }

    fn view_exists(&self, request: ConnectorViewRequest) -> Result<bool, ConnectorError> {
        ensure_request(self, &request.view.instance_id, &request.context)?;
        // The typed result reaches the caller intact. Flattening it to
        // Unavailable here would tell a caller "ask again later" about a
        // catalog that will never be able to answer.
        view_exists(
            self.runtime(),
            &request.view.namespace,
            &request.view.view,
            &request.context,
        )
    }

    fn load_view(
        &self,
        request: ConnectorViewRequest,
        creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorViewMetadataValue,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        ensure_request(self, &request.view.instance_id, &request.context)?;
        let metadata = load_view(
            self.runtime(),
            &request.view.namespace,
            &request.view.view,
            &request.context,
        )?;
        self.create_prepared_view_metadata(self.prepared_view_facts(request, metadata), creation)
    }

    fn prepare_view(
        &self,
        request: ConnectorViewRequest,
    ) -> novarocks_spi::connector::ConnectorViewPreparationFuture<'_> {
        Box::pin(async move {
            ensure_request(self, &request.view.instance_id, &request.context)?;
            let metadata = load_view_metadata_for_request_async(
                self.runtime(),
                &request.view.namespace,
                &request.view.view,
                &request.context,
            )
            .await?;
            Ok(self.prepared_view_facts(request, metadata))
        })
    }

    fn create_prepared_view(
        &self,
        preparation: novarocks_spi::connector::ConnectorViewPreparation,
        creation: &mut novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation<'_>,
    ) -> Result<
        ConnectorViewMetadataValue,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        self.create_prepared_view_metadata(preparation, creation)
    }

    fn list_views(
        &self,
        request: ConnectorListViewsRequest,
    ) -> Result<Vec<ConnectorViewIdentity>, ConnectorError> {
        ensure_request(self, &request.namespace.instance_id, &request.context)?;
        let listed = list_views(
            self.runtime(),
            &request.namespace.namespace,
            request.bound,
            &request.context,
        );
        listed_view_identities(self, &request, listed)
    }

    fn list_views_async(
        &self,
        request: ConnectorListViewsRequest,
        cpu: Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort>,
    ) -> futures::future::BoxFuture<'_, Result<Vec<ConnectorViewIdentity>, ConnectorError>> {
        Box::pin(async move {
            ensure_request(self, &request.namespace.instance_id, &request.context)?;
            let catalog = Arc::clone(self.runtime().novarocks_catalog());
            let source = crate::catalog_control::statistics::scoped_cpu::IcebergMetadataCpuOriginal::view_listing(self.clone(), request);
            catalog.list_views_with_metadata_cpu(source, cpu).await
        })
    }
}

fn listed_view_identities(
    provider: &IcebergMetadata,
    request: &ConnectorListViewsRequest,
    listed: Result<Vec<String>, ConnectorError>,
) -> Result<Vec<ConnectorViewIdentity>, ConnectorError> {
    // The catalog selected this original cause before any later local stop.
    let listed = listed?;
    ensure_request(provider, &request.namespace.instance_id, &request.context)?;
    listed
        .into_iter()
        .map(|view| {
            ensure_request(provider, &request.namespace.instance_id, &request.context)?;
            Ok(ConnectorViewIdentity {
                instance_id: provider.descriptor().instance_id.clone(),
                namespace: request.namespace.namespace.clone(),
                view: view.into(),
            })
        })
        .collect()
}

fn ensure_request(
    provider: &IcebergMetadata,
    owner: &novarocks_spi::connector::ConnectorInstanceId,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    if owner != &provider.descriptor().instance_id {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "Iceberg view request belongs to another connector instance",
        ));
    }
    if context.is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "connector request was cancelled",
        ));
    }
    if std::time::Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "connector request deadline elapsed",
        ));
    }
    Ok(())
}

fn unavailable(error: String) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unavailable, error)
}
fn invalid_view_request(error: String) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, error)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::resources::IcebergMetadataResources;

    pub(super) fn novarocks_definition(raw_sql: &str) -> ConnectorViewDefinition {
        ConnectorViewDefinition {
            dialect: ConnectorViewDialect::StarRocks,
            raw_sql: raw_sql.into(),
            default_catalog: Some("source_catalog".into()),
            default_namespace: "source_namespace".into(),
            columns: vec![novarocks_spi::connector::ConnectorViewColumn {
                name: "a".into(),
                value_type: novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int32,
                    true,
                ),
            }]
            .into(),
        }
    }

    pub(super) fn test_context() -> novarocks_spi::connector::ConnectorRequestContext {
        novarocks_spi::connector::ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            65536,
            262144,
        )
        .unwrap()
    }

    pub(super) fn hadoop_runtime() -> (
        tokio::runtime::Runtime,
        tempfile::TempDir,
        IcebergMetadataContext,
    ) {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(
                executor.handle().clone(),
            )),
            Arc::new(novarocks_fs::TokioFileIoSpawner::new(
                executor.handle().clone(),
            )),
        );
        let runtime = IcebergMetadataContext::try_new(
            IcebergCatalogControlState::new(configuration),
            IcebergMetadataResources::new(binding, executor.handle().clone()),
        )
        .expect("control runtime");
        (executor, warehouse, runtime)
    }

    /// A catalog that cannot hold views says so, instead of answering.
    ///
    /// This test previously asserted the opposite for the probes: that
    /// `view_exists` returned `false` and `list_views` returned an empty list
    /// on a Hadoop catalog. Both are indistinguishable from an authoritative
    /// answer, and `DROP DATABASE ... FORCE` consumed the empty list as exactly
    /// that. The catalog now reports that it cannot answer, and the caller
    /// decides what to do with that.
    #[test]
    fn hadoop_reports_unsupported_rather_than_answering_view_questions() {
        let (_executor, _warehouse, runtime) = hadoop_runtime();
        let assert_unsupported_outcome =
            |result: Result<CatalogOutcome<crate::catalog::transaction::CommitProof>, String>| {
                let outcome = result.expect("a view mutation reaches the owner");
                assert!(
                    matches!(outcome, CatalogOutcome::Unsupported(_)),
                    "expected an unsupported refusal, got: {outcome:?}"
                );
            };
        assert_unsupported_outcome(create_view(
            &runtime,
            "db",
            "v",
            &novarocks_definition("select 1 as a"),
            None,
            false,
            &[],
            &test_context(),
        ));
        assert_unsupported_outcome(drop_view(&runtime, "db", "v"));
        let loaded = load_view(&runtime, "db", "v", &test_context())
            .expect_err("a catalog that cannot hold views must not load one");
        assert_eq!(loaded.kind(), ConnectorErrorKind::Unsupported);
        assert!(
            loaded
                .message()
                .contains("load_view is not supported by this catalog"),
            "{loaded}"
        );

        let exists = view_exists(&runtime, "db", "v", &test_context())
            .expect_err("a probe must not be answered by a catalog that cannot hold views");
        assert_eq!(exists.kind(), ConnectorErrorKind::Unsupported);

        let listed = list_views(
            &runtime,
            "db",
            novarocks_spi::connector::ConnectorListingBound::V1,
            &novarocks_spi::connector::ConnectorRequestContext::try_new(
                std::time::Instant::now() + std::time::Duration::from_secs(5),
                novarocks_spi::connector::ConnectorStopOwner::new().view(),
                1024,
                4096,
            )
            .unwrap(),
        )
        .expect_err("an enumeration must not be answered with an empty list");
        assert_eq!(listed.kind(), ConnectorErrorKind::Unsupported);
    }

    #[test]
    fn bridge_diagnostics_are_not_reclassified_as_sql_or_provider_facts() {
        for message in [
            "view operations require a REST iceberg catalog",
            "unsupported SQL dialect: spark",
            "unknown view: db.v",
            "corrupt NovaRocks Iceberg view db.v: missing format",
            "unsupported NovaRocks Iceberg view db.v: unknown format",
            "REST response lost",
        ] {
            assert_eq!(
                unavailable(message.to_owned()).kind(),
                ConnectorErrorKind::Unavailable
            );
        }
    }

    #[test]
    fn novarocks_view_source_context_is_frozen_independently_of_target() {
        let definition = novarocks_definition("SELECT  /* preserve */ 1 AS a");
        let (catalog, namespace) =
            novarocks_source_context(&definition).expect("valid NovaRocks source context");

        assert_eq!(catalog, "source_catalog");
        assert_eq!(namespace.to_string(), "source_namespace");
        assert_eq!(definition.raw_sql.as_ref(), "SELECT  /* preserve */ 1 AS a");
    }

    /// The SPI keeps the owner's `Unsupported`: callers read it as "no such
    /// view" instead of failing on an `Unavailable` catalog.
    #[test]
    fn spi_load_view_on_hadoop_is_unsupported() {
        let (_executor, _warehouse, runtime) = hadoop_runtime();
        let provider = IcebergMetadata::new(
            novarocks_spi::connector::ConnectorInstanceDescriptor {
                provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                    .unwrap(),
                instance_id: novarocks_spi::connector::ConnectorInstanceId::parse("ice").unwrap(),
            },
            ProviderBindingEpoch::from_bytes([3; 16]),
            Arc::new(runtime),
        );
        let error = crate::metadata::with_pure_schema_creation(&test_context(), |creation| {
            ConnectorViewMetadata::load_view(
                &provider,
                ConnectorViewRequest {
                    view: ConnectorViewIdentity {
                        instance_id: novarocks_spi::connector::ConnectorInstanceId::parse("ice")
                            .unwrap(),
                        namespace: Arc::from("db"),
                        view: Arc::from("v"),
                    },
                    context: novarocks_spi::connector::ConnectorRequestContext::try_new(
                        std::time::Instant::now() + std::time::Duration::from_secs(5),
                        novarocks_spi::connector::ConnectorStopOwner::new().view(),
                        1024,
                        4096,
                    )
                    .unwrap(),
                },
                creation,
            )
            .map_err(crate::metadata::metadata_creation_error)
        })
        .expect_err("Hadoop holds no views");
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        assert_eq!(
            error.message(),
            "load_view is not supported by this catalog"
        );
    }

    mod local_refusal {
        use super::*;
        use crate::catalog::error::CatalogOutcome;
        use crate::rest_protocol::test_server::{Reply, Script, TestServer};
        use novarocks_spi::connector::ConnectorMutationFailureKind as Kind;

        /// `db.v`, whose current version also carries a Spark definition.
        fn load_view_reply() -> Reply {
            Reply::json(
                200,
                &serde_json::json!({
                    "metadata-location": "s3://warehouse/db/v/metadata/00001.json",
                    "config": {},
                    "metadata": {
                        "view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385",
                        "format-version": 1,
                        "location": "s3://warehouse/db/v",
                        "current-version-id": 1,
                        "versions": [{
                            "version-id": 1,
                            "schema-id": 0,
                            "timestamp-ms": 1_600_000_000_000i64,
                            "summary": {"engine-name": "novarocks"},
                            "representations": [
                                {"type": "sql", "sql": "select 1 as a", "dialect": "starrocks"},
                                {"type": "sql", "sql": "select 1 as a", "dialect": "spark"}
                            ],
                            "default-namespace": ["db"]
                        }],
                        "version-log": [{"version-id": 1, "timestamp-ms": 1_600_000_000_000i64}],
                        "schemas": [{
                            "type": "struct",
                            "schema-id": 0,
                            "fields": [{"id": 1, "name": "a", "required": false, "type": "int"}]
                        }],
                        "properties": {}
                    }
                }),
            )
        }

        fn rest_runtime(server: &TestServer) -> (tokio::runtime::Runtime, IcebergMetadataContext) {
            let executor = tokio::runtime::Runtime::new().expect("runtime");
            let handle = executor.handle().clone();
            let endpoints = executor
                .block_on(async {
                    crate::catalog_runtime::build_vended_rest_generation_for_test(
                        HashMap::from([("uri".to_string(), server.url())]),
                        None,
                        crate::iceberg::Runtime::try_current().expect("test runtime"),
                    )
                    .await
                })
                .expect("REST generation");
            let configuration = crate::catalog_config::parse_catalog_configuration(
                "ice",
                &[
                    ("iceberg.catalog.type".to_string(), "rest".to_string()),
                    ("uri".to_string(), server.url()),
                    ("warehouse".to_string(), "s3://warehouse".to_string()),
                ],
            )
            .expect("configuration");
            let binding = IcebergReadBinding::new(
                None,
                novarocks_fs::FsAccessResolver::new(),
                Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
                Arc::new(novarocks_fs::TokioFileIoSpawner::new(handle.clone())),
            );
            let runtime = IcebergMetadataContext::with_catalog_for_test(
                IcebergCatalogControlState::new(configuration),
                IcebergMetadataResources::new(binding, handle),
                crate::catalog::factory::NovaRocksCatalogFactory::adopt_vended_rest_for_test(
                    endpoints,
                    "s3://warehouse",
                ),
            );
            (executor, runtime)
        }

        #[test]
        fn asynchronous_view_listing_keeps_original_paging_bound_and_exact_identities() {
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on_sequence(
                        "GET",
                        "/v1/namespaces/db/views",
                        vec![
                            Reply::json(
                                200,
                                &serde_json::json!({
                                    "identifiers": [{"namespace": ["db"], "name": "first"}],
                                    "next-page-token": "p1"
                                }),
                            ),
                            Reply::json(
                                200,
                                &serde_json::json!({
                                    "identifiers": [{"namespace": ["db"], "name": "second"}]
                                }),
                            ),
                        ],
                    ),
            );
            let (executor, runtime) = rest_runtime(&server);
            let provider = view_provider(runtime);
            let mut request = listing_request(&provider, test_context());
            request.bound = novarocks_spi::connector::ConnectorListingBound {
                entries: 2,
                page_entries: 1,
                ..novarocks_spi::connector::ConnectorListingBound::V1
            };
            let identities = executor
                .block_on(provider.list_views_async(request.clone(), listing_cpu(&provider)))
                .expect("the complete original bounded listing is returned");
            assert_eq!(
                identities,
                ["first", "second"]
                    .into_iter()
                    .map(|name| ConnectorViewIdentity {
                        instance_id: request.namespace.instance_id.clone(),
                        namespace: request.namespace.namespace.clone(),
                        view: Arc::from(name),
                    })
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                server
                    .requests_to("GET", "/v1/namespaces/db/views")
                    .iter()
                    .map(|request| request.target.as_str())
                    .collect::<Vec<_>>(),
                [
                    "/v1/namespaces/db/views?pageSize=1",
                    "/v1/namespaces/db/views?pageSize=1&pageToken=p1"
                ]
            );
        }

        #[test]
        fn governed_view_pages_keep_decode_namespace_and_whole_listing_refusals() {
            for (reply, entries, expected) in [
                (
                    Reply::bytes(200, b"not-json".to_vec()),
                    2,
                    ConnectorErrorKind::Unavailable,
                ),
                (
                    Reply::json(
                        200,
                        &serde_json::json!({"identifiers":[{"namespace":["other"],"name":"v"}]}),
                    ),
                    2,
                    ConnectorErrorKind::CorruptData,
                ),
                (
                    Reply::json(
                        200,
                        &serde_json::json!({"identifiers":[{"namespace":["db"],"name":"a"},{"namespace":["db"],"name":"b"}]}),
                    ),
                    1,
                    ConnectorErrorKind::ResourceExhausted,
                ),
            ] {
                let server = TestServer::start(
                    Script::new()
                        .on("GET", "/v1/config", Reply::config(&[], &[], None))
                        .on("GET", "/v1/namespaces/db/views", reply),
                );
                let (executor, runtime) = rest_runtime(&server);
                let provider = view_provider(runtime);
                let mut request = listing_request(&provider, test_context());
                request.bound.entries = entries;
                let error = executor
                    .block_on(provider.list_views_async(request, listing_cpu(&provider)))
                    .unwrap_err();
                assert_eq!(error.kind(), expected);
                assert_eq!(server.count("GET", "/v1/namespaces/db/views"), 1);
            }
        }

        fn refused_as_invalid(
            outcome: Result<CatalogOutcome<crate::catalog::transaction::CommitProof>, String>,
        ) -> String {
            match outcome.expect("the refusal is an outcome") {
                CatalogOutcome::KnownUncommitted { failure } => {
                    assert_eq!(failure.kind(), Kind::InvalidRequest);
                    failure.to_string()
                }
                other => panic!("expected a local InvalidRequest refusal, got {other:?}"),
            }
        }

        #[test]
        fn java_create_rebuilds_summary_before_the_first_spi_load() {
            let published = Arc::new(std::sync::Mutex::new(None::<serde_json::Value>));
            let for_create = published.clone();
            let for_load = published.clone();
            let server = TestServer::start(Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on_fn("POST", "/v1/namespaces/db/views", move |request, _| {
                    let sent: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
                    let mut version = sent["view-version"].clone();
                    assert_eq!(version["representations"][0]["sql"], "SELECT CAST(NULL AS integer) AS a");
                    assert_eq!(sent["schema"]["fields"][0]["required"], true);
                    version["summary"] = serde_json::json!({"engine-name":"Apache Iceberg"});
                    let response = serde_json::json!({
                        "metadata-location":"s3://warehouse/db/v/metadata/1.json",
                        "metadata": {
                            "view-uuid":"fa6506c3-7681-40c8-86dc-e36561f83385", "format-version":1,
                            "location":"s3://warehouse/db/v", "current-version-id":1,
                            "version-log":[{"version-id":1,"timestamp-ms":version["timestamp-ms"]}],
                            "versions":[version], "schemas":[sent["schema"]], "properties":{}
                        }
                    });
                    *for_create.lock().unwrap() = Some(response.clone());
                    Reply::json(200, &response)
                })
                .on_fn("GET", "/v1/namespaces/db/views/v", move |_, _| {
                    Reply::json(200, for_load.lock().unwrap().as_ref().unwrap())
                }));
            let (executor, runtime) = rest_runtime(&server);
            let provider = view_provider(runtime.clone());
            let definition = novarocks_definition("SELECT CAST(NULL AS integer) AS a");
            let (prepared, properties) =
                prepare_definition(&definition, None, &[], &test_context()).unwrap();
            let creation = ViewCreate::validate("v".into(), prepared, properties).unwrap();
            // This original catalog client is already bound to the provider.
            // Publish with that same generation, not the unrelated fixed epoch
            // used by the old create_view convenience fixture.
            let outcome = executor.block_on(
                crate::catalog::admission_test_support::publish_catalog_fixture(
                    &runtime,
                    provider.incarnation(),
                    crate::catalog::catalog_request::CatalogPublicationRequest::CreateView {
                        namespace: CatalogNamespaceName::new("db"),
                        creation,
                    },
                ),
            );
            assert!(matches!(outcome, CatalogOutcome::KnownCommitted { .. }));
            let request = ConnectorViewRequest {
                view: ConnectorViewIdentity {
                    instance_id: provider.descriptor().instance_id.clone(),
                    namespace: "db".into(),
                    view: "v".into(),
                },
                context: test_context(),
            };
            let value =
                crate::metadata::with_pure_schema_creation(&request.context.clone(), |creation| {
                    provider
                        .load_view(request, creation)
                        .map_err(crate::metadata::metadata_creation_error)
                })
                .unwrap();
            assert_eq!(value.definition.raw_sql, definition.raw_sql);
            assert_eq!(value.definition.default_catalog, definition.default_catalog);
            assert_eq!(
                value.definition.default_namespace,
                definition.default_namespace
            );
            assert_eq!(
                value.definition.columns[0].value_type,
                novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int32,
                    true
                )
            );
            assert_eq!(server.count("POST", "/v1/namespaces/db/views"), 1);
            assert_eq!(server.count("POST", "/v1/namespaces/db/views/v"), 0);
        }

        #[test]
        fn dropping_a_dialect_is_refused_before_dispatch() {
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on("GET", "/v1/namespaces/db/views/v", load_view_reply())
                    .on("POST", "/v1/namespaces/db/views/v", Reply::status(200)),
            );
            let (_executor, runtime) = rest_runtime(&server);
            let message = refused_as_invalid(create_view(
                &runtime,
                "db",
                "v",
                &novarocks_definition("select 2 as a"),
                None,
                true,
                &[],
                &test_context(),
            ));
            assert!(message.contains("spark"), "{message}");
            assert!(
                message.contains("replace.drop-dialect.allowed"),
                "{message}"
            );
            assert_eq!(server.count("GET", "/v1/namespaces/db/views/v"), 1);
            assert_eq!(
                server.count("POST", "/v1/namespaces/db/views/v"),
                0,
                "no commit request is dispatched"
            );
        }

        #[test]
        fn an_invalid_history_property_is_refused_before_dispatch() {
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on("GET", "/v1/namespaces/db/views/v", load_view_reply())
                    .on("POST", "/v1/namespaces/db/views", Reply::status(200))
                    .on("POST", "/v1/namespaces/db/views/v", Reply::status(200)),
            );
            let (_executor, runtime) = rest_runtime(&server);
            let invalid = [("version.history.num-entries".to_string(), "0".to_string())];
            for replace in [false, true] {
                let message = refused_as_invalid(create_view(
                    &runtime,
                    "db",
                    "v",
                    &novarocks_definition("select 2 as a"),
                    None,
                    replace,
                    &invalid,
                    &test_context(),
                ));
                assert!(message.contains("version.history.num-entries"), "{message}");
            }
            assert_eq!(server.count("POST", "/v1/namespaces/db/views"), 0);
            assert_eq!(server.count("POST", "/v1/namespaces/db/views/v"), 0);
        }
    }

    pub(super) fn view_provider(runtime: IcebergMetadataContext) -> IcebergMetadata {
        IcebergMetadata::new(
            ConnectorInstanceDescriptor {
                provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                    .unwrap(),
                instance_id: novarocks_spi::connector::ConnectorInstanceId::parse("ice").unwrap(),
            },
            ProviderBindingEpoch::new(),
            Arc::new(runtime),
        )
    }

    struct ListingCpu {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
    }
    impl novarocks_spi::connector::ProviderMetadataCpuPort for ListingCpu {
        fn submit_metadata_in(
            &self,
            input: &mut Option<novarocks_spi::connector::ProviderMetadataCpuInput>,
        ) -> Result<(), ConnectorError> {
            let phase = input.as_ref().expect("one original listing CPU input");
            assert_eq!(phase.descriptor(), &self.descriptor);
            assert_eq!(phase.incarnation(), self.incarnation);
            assert_eq!(
                phase.original_source_type_id(),
                std::any::TypeId::of::<
                    crate::catalog_control::statistics::scoped_cpu::IcebergMetadataCpuOriginal,
                >()
            );
            input.take().unwrap().run_original_phase();
            Ok(())
        }
        fn poll_finished(
            &self,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), ConnectorError>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_closed(
            &self,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), ConnectorError>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
            None
        }
    }
    // Provider algorithm fixture only. The FE owner tests the actual worker,
    // queue grant, footer and Root lifetime; this fixture grants none of them.
    fn listing_cpu(
        provider: &IcebergMetadata,
    ) -> Arc<dyn novarocks_spi::connector::ProviderMetadataCpuPort> {
        Arc::new(ListingCpu {
            descriptor: provider.descriptor().clone(),
            incarnation: provider.incarnation(),
        })
    }

    fn listing_request(
        provider: &IcebergMetadata,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> ConnectorListViewsRequest {
        ConnectorListViewsRequest {
            namespace: novarocks_spi::connector::ConnectorNamespaceIdentity {
                instance_id: provider.descriptor().instance_id.clone(),
                namespace: Arc::from("db"),
            },
            bound: novarocks_spi::connector::ConnectorListingBound::V1,
            context,
        }
    }

    #[test]
    fn asynchronous_view_listing_keeps_real_unsupported_instead_of_empty_success() {
        let (executor, _warehouse, runtime) = hadoop_runtime();
        let provider = view_provider(runtime);
        let synchronous = provider
            .list_views(listing_request(&provider, test_context()))
            .expect_err("Hadoop cannot author an authoritative view listing");
        let asynchronous = executor
            .block_on(provider.list_views_async(
                listing_request(&provider, test_context()),
                listing_cpu(&provider),
            ))
            .expect_err("the async path cannot fabricate authoritative absence");
        assert_eq!(asynchronous.kind(), ConnectorErrorKind::Unsupported);
        assert_eq!(asynchronous.kind(), synchronous.kind());
        assert_eq!(asynchronous.message(), synchronous.message());
    }

    #[test]
    fn asynchronous_view_listing_rejects_foreign_owner_stop_and_deadline_before_catalog() {
        let (executor, _warehouse, runtime) = hadoop_runtime();
        let provider = view_provider(runtime);
        let mut foreign = listing_request(&provider, test_context());
        foreign.namespace.instance_id =
            novarocks_spi::connector::ConnectorInstanceId::parse("other").unwrap();
        let error = executor
            .block_on(provider.list_views_async(foreign, listing_cpu(&provider)))
            .expect_err("the exact provider owner is checked before catalog work");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);

        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let stopped = novarocks_spi::connector::ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            stop.view(),
            65536,
            262144,
        )
        .unwrap();
        stop.request_stop();
        let error = executor
            .block_on(
                provider
                    .list_views_async(listing_request(&provider, stopped), listing_cpu(&provider)),
            )
            .expect_err("the original stop cannot reach an unsupported catalog call");
        assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);

        let expired = novarocks_spi::connector::ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            65536,
            262144,
        )
        .unwrap();
        // Use the existing deadline projection rather than changing a live clock.
        let expired = expired.with_earlier_deadline(std::time::Instant::now());
        let error = executor
            .block_on(
                provider
                    .list_views_async(listing_request(&provider, expired), listing_cpu(&provider)),
            )
            .expect_err("the original expired deadline cannot reach the catalog");
        assert_eq!(error.kind(), ConnectorErrorKind::DeadlineExceeded);
    }

    #[test]
    fn view_listing_mapping_keeps_exact_identity_and_rechecks_original_control() {
        let (_executor, _warehouse, runtime) = hadoop_runtime();
        let provider = view_provider(runtime);
        let request = listing_request(&provider, test_context());
        let identities = listed_view_identities(
            &provider,
            &request,
            Ok(vec!["v2".to_owned(), "v1".to_owned()]),
        )
        .unwrap();
        assert_eq!(
            identities
                .iter()
                .map(|view| view.view.as_ref())
                .collect::<Vec<_>>(),
            ["v2", "v1"],
            "Provider identity projection does not invent ordering or drop names"
        );
        assert!(identities.iter().all(|view| {
            view.instance_id == provider.descriptor().instance_id
                && view.namespace == request.namespace.namespace
        }));
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let stopped = listing_request(
            &provider,
            novarocks_spi::connector::ConnectorRequestContext::try_new(
                std::time::Instant::now() + std::time::Duration::from_secs(30),
                stop.view(),
                65536,
                262144,
            )
            .unwrap(),
        );
        stop.request_stop();
        assert_eq!(
            listed_view_identities(&provider, &stopped, Ok(Vec::new()))
                .expect_err("even an empty result must recheck the original stop")
                .kind(),
            ConnectorErrorKind::Cancelled
        );
    }

    #[test]
    fn selected_view_listing_error_keeps_original_source_before_later_stop() {
        use novarocks_failure_contract::FailureProjection;

        let (_executor, _warehouse, runtime) = hadoop_runtime();
        let provider = view_provider(runtime);
        let stop = novarocks_spi::connector::ConnectorStopOwner::new();
        let request = listing_request(
            &provider,
            novarocks_spi::connector::ConnectorRequestContext::try_new(
                std::time::Instant::now() + std::time::Duration::from_secs(30),
                stop.view(),
                65536,
                262144,
            )
            .unwrap(),
        );
        let original_source = novarocks_type_contract::CompileControlError::from_domain(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel,
        );
        let original = ConnectorError::compile_interruption(original_source.clone());
        let expected_fact = original.failure_fact();
        let selected = Err(original.clone());
        stop.request_stop();
        let actual = listed_view_identities(&provider, &request, selected)
            .expect_err("later stop cannot overwrite a catalog-selected Source");
        assert_eq!(actual, original, "including the same opaque cause identity");
        assert_eq!(actual.failure_fact(), expected_fact);
        let preserved = std::error::Error::source(&actual)
            .unwrap()
            .downcast_ref::<novarocks_failure_contract::SharedFailureCause>()
            .unwrap()
            .downcast_ref::<novarocks_type_contract::CompileControlError>();
        assert_eq!(preserved, Some(&original_source));
        let mut work = async_listing::Work::new(provider.clone(), request.clone());
        work.receive(Err(actual.clone()));
        work.run();
        assert_eq!(
            work.error(),
            Some(actual),
            "the closed phase also preserves raw Err before later stop/CPU work"
        );
        assert_eq!(
            listed_view_identities(&provider, &request, Ok(Vec::new()))
                .expect_err("a successful empty listing still observes the original stop")
                .kind(),
            ConnectorErrorKind::Cancelled
        );
    }
}

#[cfg(test)]
mod declaration_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use novarocks_spi::connector::ConnectorViewColumn;
    use novarocks_type_contract::{FunctionValueType, ValueLogicalType, ValueTypeStorageLifetime};

    fn definition(value: FunctionValueType) -> ConnectorViewDefinition {
        let mut result = super::tests::novarocks_definition(
            "SELECT  /* exact source */ CAST(NULL AS integer) AS a",
        );
        result.columns = vec![ConnectorViewColumn {
            name: "a".into(),
            value_type: value,
        }]
        .into();
        result
    }
    fn prepared(value: FunctionValueType) -> ViewDefinition {
        let (value, properties) =
            prepare_definition(&definition(value), None, &[], &request().context).unwrap();
        assert!(properties.is_empty());
        assert_eq!(value.summary.len(), 1);
        value
    }
    fn metadata(mut definition: ViewDefinition, version: i32) -> ViewMetadata {
        // Java REST rebuilds the first version; it does not echo client summary.
        definition.summary = HashMap::new();
        serde_json::from_value(serde_json::json!({
            "view-uuid": "fa6506c3-7681-40c8-86dc-e36561f83385", "format-version":1,
            "location":"s3://warehouse/db/v", "current-version-id":version,
            "versions":[{"version-id":version, "schema-id":definition.schema.schema_id(),
                "timestamp-ms":1_600_000_000_000_i64, "summary":definition.summary,
                "representations":[{"type":"sql", "sql":definition.sql, "dialect":"starrocks"}],
                "default-catalog":definition.default_catalog,
                "default-namespace":definition.default_namespace}],
            "version-log":[{"version-id":version,"timestamp-ms":1_600_000_000_000_i64}],
            "schemas":[definition.schema], "properties":{}
        }))
        .unwrap()
    }
    fn request() -> ConnectorViewRequest {
        ConnectorViewRequest {
            view: ConnectorViewIdentity {
                instance_id: novarocks_spi::connector::ConnectorInstanceId::parse("ice").unwrap(),
                namespace: "db".into(),
                view: "v".into(),
            },
            context: super::tests::test_context(),
        }
    }
    fn load(metadata: &ViewMetadata) -> Result<ConnectorViewMetadataValue, ConnectorError> {
        let request = request();
        crate::metadata::with_pure_schema_creation(&request.context.clone(), |creation| {
            materialize_view_metadata(request, metadata, creation)
                .map_err(crate::metadata::metadata_creation_error)
        })
    }
    #[test]
    fn java_created_view_without_client_summary_keeps_named_sql_context_and_nullable_type() {
        let stored = prepared(FunctionValueType::new(DataType::Int32, true));
        let (_, schema, version, _) =
            ViewCreate::validate("v".into(), stored.clone(), HashMap::new())
                .unwrap()
                .into_parts();
        assert_eq!(schema.schema_id(), version.schema_id);
        assert!(schema.as_struct().fields()[0].required);
        let observed = load(&metadata(stored, 1)).unwrap();
        assert_eq!(
            observed.definition.raw_sql.as_ref(),
            "SELECT  /* exact source */ CAST(NULL AS integer) AS a"
        );
        assert_eq!(
            observed.definition.default_catalog.as_deref(),
            Some("source_catalog")
        );
        assert_eq!(
            observed.definition.default_namespace.as_ref(),
            "source_namespace"
        );
        assert_eq!(observed.definition.columns[0].name.as_ref(), "a");
        assert_eq!(
            observed.definition.columns[0].value_type,
            FunctionValueType::new(DataType::Int32, true)
        );
    }
    #[test]
    fn standard_view_types_do_not_require_an_engine_or_private_type_summary() {
        for value in [
            FunctionValueType::new(DataType::Boolean, false),
            FunctionValueType::new(DataType::Int32, false),
            FunctionValueType::new(DataType::Int64, false),
            FunctionValueType::new(DataType::Float32, false),
            FunctionValueType::new(DataType::Float64, false),
            FunctionValueType::new(DataType::Decimal128(20, 3), false),
            FunctionValueType::new(DataType::Utf8, false),
            FunctionValueType::new(DataType::Binary, false),
            FunctionValueType::new(DataType::LargeUtf8, true),
            FunctionValueType::new(DataType::Utf8View, true),
            FunctionValueType::new(DataType::LargeBinary, true),
            FunctionValueType::new(DataType::BinaryView, true),
            FunctionValueType::new(
                DataType::List(Arc::new(Field::new("item", DataType::LargeUtf8, true))),
                true,
            ),
            FunctionValueType::new(DataType::Date32, false),
            FunctionValueType::new(DataType::Time64(TimeUnit::Microsecond), false),
            FunctionValueType::new(DataType::Timestamp(TimeUnit::Microsecond, None), false),
            FunctionValueType::new(DataType::Timestamp(TimeUnit::Nanosecond, None), false),
            FunctionValueType::new(
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            FunctionValueType::new(
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            FunctionValueType::try_with_logical_type(
                DataType::FixedSizeBinary(16),
                false,
                ValueLogicalType::Uuid,
            )
            .unwrap(),
            FunctionValueType::try_with_logical_type(
                DataType::LargeBinary,
                false,
                ValueLogicalType::Variant,
            )
            .unwrap(),
        ] {
            let raw = metadata(prepared(value.clone()), 1);
            let observed = load(&raw).unwrap();
            assert!(observed.definition.columns[0].value_type.nullable());
            assert_eq!(
                observed.definition.columns[0]
                    .value_type
                    .value_type()
                    .kind(),
                value.value_type().kind()
            );
        }
    }
    #[test]
    fn unsupported_types_are_refused_before_any_view_publication() {
        let mut values = vec![
            FunctionValueType::new(DataType::Int8, true),
            FunctionValueType::new(DataType::Int16, true),
            FunctionValueType::new(DataType::UInt32, true),
            FunctionValueType::new(DataType::Null, true),
            FunctionValueType::new(DataType::Time64(TimeUnit::Nanosecond), true),
            FunctionValueType::new(DataType::Timestamp(TimeUnit::Millisecond, None), true),
            FunctionValueType::new(DataType::FixedSizeBinary(16), true),
            FunctionValueType::new(
                DataType::Timestamp(TimeUnit::Microsecond, Some("Europe/Paris".into())),
                true,
            ),
            FunctionValueType::new(
                DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                true,
            ),
        ];
        for (carrier, domain) in [
            (DataType::Utf8, ValueLogicalType::Json),
            (DataType::Binary, ValueLogicalType::Hll),
            (DataType::Binary, ValueLogicalType::Bitmap),
            (DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt),
        ] {
            values.push(FunctionValueType::try_with_logical_type(carrier, true, domain).unwrap());
        }
        for value in values {
            let error =
                prepare_definition(&definition(value), None, &[], &request().context).unwrap_err();
            assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        }
    }
    #[test]
    fn nested_view_nullability_is_canonical_while_identity_keeps_original_required_bits() {
        let ty = DataType::Struct(
            vec![
                Arc::new(Field::new(
                    "items",
                    DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                    false,
                )),
                Arc::new(Field::new(
                    "pairs",
                    DataType::Map(
                        Arc::new(Field::new(
                            "entries",
                            DataType::Struct(
                                vec![
                                    Field::new("key", DataType::Int64, false),
                                    Field::new("value", DataType::Utf8, false),
                                ]
                                .into(),
                            ),
                            false,
                        )),
                        false,
                    ),
                    false,
                )),
            ]
            .into(),
        );
        let raw = metadata(prepared(FunctionValueType::new(ty, false)), 1);
        let mut changed = serde_json::to_value(&raw).unwrap();
        changed["schemas"][0]["fields"][0]["required"] = false.into();
        let members = &mut changed["schemas"][0]["fields"][0]["type"]["fields"];
        members[0]["required"] = true.into();
        members[0]["type"]["element-required"] = true.into();
        members[1]["required"] = true.into();
        members[1]["type"]["value-required"] = true.into();
        let changed: ViewMetadata = serde_json::from_value(changed).unwrap();
        let first = load(&raw).unwrap();
        let second = load(&changed).unwrap();
        assert_ne!(first.version, second.version);
        assert_eq!(first.definition.columns, second.definition.columns);
        let top = first.definition.columns[0].value_type.value_type();
        assert!(top.nullable());
        let novarocks_type_contract::ValueKind::Struct(fields) = top.kind() else {
            panic!("struct")
        };
        assert!(fields.iter().all(|f| f.value_type().nullable()));
        let novarocks_type_contract::ValueKind::List(item) = fields[0].value_type().kind() else {
            panic!("list")
        };
        assert!(item.nullable());
        let novarocks_type_contract::ValueKind::Map { key, value } = fields[1].value_type().kind()
        else {
            panic!("map")
        };
        assert!(!key.nullable());
        assert!(value.nullable());
    }
    #[test]
    fn standard_fixed_binary_never_recovers_a_nova_nominal_domain() {
        let raw = metadata(prepared(FunctionValueType::new(DataType::Binary, true)), 1);
        let mut value = serde_json::to_value(raw).unwrap();
        value["schemas"][0]["fields"][0]["type"] = "fixed[16]".into();
        value["versions"][0]["summary"] =
            serde_json::json!({"engine-name":"novarocks", "novarocks.source-format":"future"});
        let observed = load(&serde_json::from_value(value).unwrap()).unwrap();
        assert_eq!(
            observed.definition.columns[0].value_type,
            FunctionValueType::new(DataType::Binary, true)
        );
    }
    #[test]
    fn reused_numeric_ids_bind_the_observed_schema_sql_and_context() {
        let original = metadata(prepared(FunctionValueType::new(DataType::Int32, true)), 1);
        let other_type = metadata(prepared(FunctionValueType::new(DataType::Int64, true)), 1);
        let baseline = load(&original).unwrap().version;
        assert_ne!(baseline, load(&other_type).unwrap().version);
        for field in ["sql", "default-catalog", "default-namespace"] {
            let mut value = serde_json::to_value(&original).unwrap();
            match field {
                "sql" => value["versions"][0]["representations"][0]["sql"] = "SELECT 2 AS a".into(),
                "default-catalog" => value["versions"][0][field] = "another".into(),
                _ => value["versions"][0][field] = serde_json::json!(["another"]),
            }
            assert_ne!(
                baseline,
                load(&serde_json::from_value(value).unwrap())
                    .unwrap()
                    .version
            );
        }
    }
    #[test]
    fn declaration_identity_canonicalizes_identifier_order_without_discarding_ids() {
        let raw = metadata(prepared(FunctionValueType::new(DataType::Int32, true)), 1);
        let mut value = serde_json::to_value(raw).unwrap();
        let mut second = value["schemas"][0]["fields"][0].clone();
        second["id"] = 2.into();
        second["name"] = "b".into();
        value["schemas"][0]["fields"]
            .as_array_mut()
            .unwrap()
            .push(second);
        value["schemas"][0]["identifier-field-ids"] = serde_json::json!([1, 2]);
        let original: ViewMetadata = serde_json::from_value(value.clone()).unwrap();
        let identity = load(&original).unwrap().version;
        value["schemas"][0]["identifier-field-ids"] = serde_json::json!([2, 1]);
        assert_eq!(
            identity,
            load(&serde_json::from_value(value.clone()).unwrap())
                .unwrap()
                .version
        );
        value["schemas"][0]["identifier-field-ids"] = serde_json::json!([1]);
        assert_ne!(
            identity,
            load(&serde_json::from_value(value).unwrap())
                .unwrap()
                .version
        );
    }
    #[test]
    fn same_descriptor_and_epoch_do_not_substitute_for_the_original_generation_owner() {
        let (_executor, _warehouse, runtime) = super::tests::hadoop_runtime();
        let provider = super::tests::view_provider(runtime.clone());
        let other = IcebergMetadata::new(
            provider.descriptor().clone(),
            provider.incarnation(),
            Arc::new(runtime),
        );
        let raw = || metadata(prepared(FunctionValueType::new(DataType::Int32, true)), 1);
        let preparation = provider.prepared_view_facts(request(), raw());
        let error = crate::metadata::with_pure_schema_creation(&request().context, |creation| {
            other
                .create_prepared_view(preparation, creation)
                .map_err(crate::metadata::metadata_creation_error)
        })
        .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        let preparation = provider.prepared_view_facts(request(), raw());
        crate::metadata::with_pure_schema_creation(&request().context, |creation| {
            provider
                .clone()
                .create_prepared_view(preparation, creation)
                .map_err(crate::metadata::metadata_creation_error)
        })
        .unwrap();
    }

    #[test]
    fn original_schema_control_and_source_refusals_survive_without_reclassification() {
        use novarocks_connector_contract::PureProviderCompileError;
        use novarocks_type_contract::{
            CompileControlError, owned_resources::layout::LayoutResourceError,
        };
        let raw = metadata(prepared(FunctionValueType::new(DataType::Int32, true)), 1);
        for expected in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
            CompileControlError::from_domain(LayoutResourceError::SourceModel),
        ] {
            let mut allocations = |_: std::alloc::Layout| Ok(());
            let mut work = |units: usize| {
                if units > 0 {
                    Err(expected.clone())
                } else {
                    Ok(())
                }
            };
            let mut creation =
                novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
                    ValueTypeStorageLifetime::pure(),
                    &mut allocations,
                    &mut work,
                );
            let error = materialize_view_metadata(request(), &raw, &mut creation).unwrap_err();
            assert!(
                matches!(error, PureProviderCompileError::Control(actual) if actual == expected)
            );
        }
    }

    #[test]
    fn public_column_name_arc_refusal_keeps_the_original_source_cause() {
        use novarocks_connector_contract::PureProviderCompileError;
        use novarocks_type_contract::{
            CompileControlError, owned_resources::layout::LayoutResourceError,
        };
        let raw = metadata(prepared(FunctionValueType::new(DataType::Int32, true)), 1);
        let name = "a".repeat(277);
        let mut value = serde_json::to_value(raw).unwrap();
        value["schemas"][0]["fields"][0]["name"] = name.clone().into();
        let raw: ViewMetadata = serde_json::from_value(value).unwrap();
        let requested = novarocks_type_contract::owned_resources::layout::arc_layout(
            std::alloc::Layout::array::<u8>(name.len()).unwrap(),
        )
        .unwrap();
        let expected = CompileControlError::from_domain(LayoutResourceError::SourceModel);
        let refused = std::cell::Cell::new(false);
        let mut allocations = |layout: std::alloc::Layout| {
            if layout == requested {
                refused.set(true);
                Err(expected.clone())
            } else {
                Ok(())
            }
        };
        let mut work = |_: usize| Ok(());
        let mut creation = novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut allocations,
            &mut work,
        );
        let error = materialize_view_metadata(request(), &raw, &mut creation).unwrap_err();
        assert!(
            refused.get(),
            "the public name Arc has its own original request"
        );
        assert!(matches!(error, PureProviderCompileError::Control(actual) if actual == expected));
    }
    #[test]
    fn declaration_identity_observer_preserves_the_original_source_and_stops_before_later_bytes() {
        use novarocks_connector_contract::PureProviderCompileError;
        use novarocks_type_contract::{
            CompileControlError, owned_resources::layout::LayoutResourceError,
        };
        let raw = metadata(prepared(FunctionValueType::new(DataType::Int32, true)), 1);
        let expected = CompileControlError::from_domain(LayoutResourceError::SourceModel);
        let successful_calls = std::cell::Cell::new(0usize);
        let mut allocations = |_: std::alloc::Layout| Ok(());
        let mut observe = |_: usize| {
            successful_calls.set(successful_calls.get() + 1);
            Ok(())
        };
        let mut creation = novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut allocations,
            &mut observe,
        );
        version_identity::observed(
            &raw,
            raw.current_version(),
            raw.current_schema(),
            &mut creation,
        )
        .unwrap();
        // The final observed write is inside the raw schema declaration observer.
        for refusal in [4, successful_calls.get()] {
            let calls = std::cell::Cell::new(0usize);
            let mut work = |_: usize| {
                calls.set(calls.get() + 1);
                assert!(
                    calls.get() <= refusal,
                    "identity callback after primary refusal"
                );
                if calls.get() == refusal {
                    Err(expected.clone())
                } else {
                    Ok(())
                }
            };
            let mut creation =
                novarocks_spi::connector::read_stack::ConnectorReadSchemaCreation::new(
                    ValueTypeStorageLifetime::pure(),
                    &mut allocations,
                    &mut work,
                );
            let error = version_identity::observed(
                &raw,
                raw.current_version(),
                raw.current_schema(),
                &mut creation,
            )
            .unwrap_err();
            assert!(
                matches!(error, PureProviderCompileError::Control(actual) if actual == expected)
            );
            assert_eq!(calls.get(), refusal);
        }
    }
}
