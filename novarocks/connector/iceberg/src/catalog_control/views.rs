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

use novarocks_spi::connector::{
    ConnectorColumnDefinition, ConnectorViewDefinition, ConnectorViewSourceFormat,
};
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
use crate::iceberg::spec::{Schema, ViewMetadata, ViewRepresentation};
use crate::iceberg::{NamespaceIdent, TableIdent};
use crate::metadata_context::IcebergMetadataContext;

#[path = "views/async_listing.rs"]
pub(crate) mod async_listing;

const NOVAROCKS_ENGINE_NAME: &str = "novarocks";
const NOVAROCKS_SOURCE_FORMAT_KEY: &str = "novarocks.source-format";
const EFFECTIVE_USER_SOURCE_V1: &str = "effective-user-source-v1";

#[derive(Clone, Debug)]
pub(crate) struct LoadedIcebergView {
    pub raw_sql: String,
    pub dialect: String,
    pub default_catalog: Option<String>,
    pub default_namespace: String,
    pub source_format: Option<ConnectorViewSourceFormat>,
    pub column_names: Vec<String>,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
}

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

fn build_view_schema(columns: &[ConnectorColumnDefinition]) -> Result<Schema, String> {
    Schema::builder()
        .with_fields(super::type_mapping::schema_fields(columns)?)
        .build()
        .map_err(|error| format!("build Iceberg view schema: {error}"))
}

fn novarocks_source_context(
    definition: &ConnectorViewDefinition,
) -> Result<(String, NamespaceIdent, &'static str), String> {
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
    let source_format = match definition.source_format {
        Some(ConnectorViewSourceFormat::EffectiveUserSourceV1) => EFFECTIVE_USER_SOURCE_V1,
        None => {
            return Err(
                "NovaRocks Iceberg view creation requires effective-user-source-v1 provenance"
                    .to_string(),
            );
        }
    };
    Ok((
        source_catalog,
        NamespaceIdent::new(source_namespace),
        source_format,
    ))
}

fn source_format_from_summary(
    ident: &TableIdent,
    summary: &HashMap<String, String>,
) -> Result<Option<ConnectorViewSourceFormat>, String> {
    if summary.get("engine-name").map(String::as_str) != Some(NOVAROCKS_ENGINE_NAME) {
        return Ok(None);
    }
    match summary.get(NOVAROCKS_SOURCE_FORMAT_KEY).map(String::as_str) {
        Some(EFFECTIVE_USER_SOURCE_V1) => {
            Ok(Some(ConnectorViewSourceFormat::EffectiveUserSourceV1))
        }
        None => Err(format!(
            "corrupt NovaRocks Iceberg view {ident}: missing {NOVAROCKS_SOURCE_FORMAT_KEY}"
        )),
        Some(value) => Err(format!(
            "unsupported NovaRocks Iceberg view {ident}: unknown {NOVAROCKS_SOURCE_FORMAT_KEY} `{value}`"
        )),
    }
}

pub(super) fn prepare_definition(
    columns: &[ConnectorColumnDefinition],
    definition: &ConnectorViewDefinition,
    comment: Option<&str>,
    extra_properties: &[(String, String)],
) -> Result<(ViewDefinition, HashMap<String, String>), String> {
    let (source_catalog, source_namespace, source_format) = novarocks_source_context(definition)?;
    let schema = build_view_schema(columns)?;
    let mut properties = extra_properties.iter().cloned().collect::<HashMap<_, _>>();
    if let Some(comment) = comment
        && properties
            .insert("comment".to_string(), comment.to_string())
            .is_some()
    {
        return Err("duplicate Iceberg view comment property".to_string());
    }
    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| format!("system clock before epoch: {error}"))?
        .as_millis() as i64;
    let definition = ViewDefinition {
        schema,
        sql: definition.raw_sql.to_string(),
        summary: HashMap::from([
            ("engine-name".to_string(), NOVAROCKS_ENGINE_NAME.to_string()),
            (
                NOVAROCKS_SOURCE_FORMAT_KEY.to_string(),
                source_format.to_string(),
            ),
        ]),
        default_catalog: Some(source_catalog),
        default_namespace: source_namespace,
        timestamp_ms,
    };
    Ok((definition, properties))
}

#[cfg(test)]
pub(crate) fn create_view(
    runtime: &IcebergMetadataContext,
    namespace: &str,
    view: &str,
    columns: &[ConnectorColumnDefinition],
    definition: &ConnectorViewDefinition,
    comment: Option<&str>,
    replace: bool,
    extra_properties: &[(String, String)],
) -> Result<CatalogOutcome<crate::catalog::transaction::CommitProof>, String> {
    let (namespace_ident, ident) = view_ident(namespace, view)?;
    let (definition, properties) =
        prepare_definition(columns, definition, comment, extra_properties)?;
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
) -> Result<LoadedIcebergView, ConnectorError> {
    let (_, ident) = view_ident(namespace, view).map_err(unavailable)?;
    let metadata = load_view_metadata_for_request(runtime, namespace, view, context)?;
    loaded_view_from_metadata(&ident, &metadata).map_err(unavailable)
}

pub(super) fn load_view_metadata_for_request(
    runtime: &IcebergMetadataContext,
    namespace: &str,
    view: &str,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<ViewMetadata, ConnectorError> {
    let (_, ident) = view_ident(namespace, view).map_err(unavailable)?;
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
    let (_, ident) = view_ident(namespace, view).map_err(unavailable)?;
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

fn loaded_view_from_metadata(
    ident: &TableIdent,
    metadata: &ViewMetadata,
) -> Result<LoadedIcebergView, String> {
    let version = metadata.current_version();
    let mut selected = None;
    for representation in version.representations().iter() {
        let ViewRepresentation::Sql(sql) = representation;
        if sql.dialect.eq_ignore_ascii_case(VIEW_DIALECT_STARROCKS) {
            selected = Some(sql);
            break;
        }
        selected.get_or_insert(sql);
    }
    let selected =
        selected.ok_or_else(|| format!("Iceberg view {ident} has no SQL representation"))?;
    let default_namespace = version
        .default_namespace()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(".");
    let properties = metadata.properties().clone();
    let source_format = source_format_from_summary(ident, version.summary())?;
    Ok(LoadedIcebergView {
        raw_sql: selected.sql.clone(),
        dialect: selected.dialect.clone(),
        default_catalog: version.default_catalog().cloned(),
        default_namespace,
        source_format,
        column_names: metadata
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect(),
        comment: properties.get("comment").cloned(),
        properties,
    })
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
    ) -> Result<ConnectorViewMetadataValue, ConnectorError> {
        ensure_request(self, &request.view.instance_id, &request.context)?;
        let loaded = load_view(
            self.runtime(),
            &request.view.namespace,
            &request.view.view,
            &request.context,
        )?;
        if !loaded.dialect.eq_ignore_ascii_case(VIEW_DIALECT_STARROCKS) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                format!(
                    "Iceberg view uses unsupported SQL dialect {}",
                    loaded.dialect
                ),
            ));
        }
        ConnectorViewMetadataValue::try_new(
            request.view,
            ConnectorViewDefinition {
                dialect: ConnectorViewDialect::StarRocks,
                raw_sql: loaded.raw_sql.into(),
                default_catalog: loaded.default_catalog.map(Into::into),
                default_namespace: loaded.default_namespace.into(),
                source_format: loaded.source_format,
            },
            loaded.column_names.into_iter().map(Into::into).collect(),
            loaded.comment.map(Into::into),
            loaded
                .properties
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
            &request.context,
        )
    }

    fn load_view_async(
        &self,
        request: ConnectorViewRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<ConnectorViewMetadataValue, ConnectorError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            ensure_request(self, &request.view.instance_id, &request.context)?;
            let (_, ident) =
                view_ident(&request.view.namespace, &request.view.view).map_err(unavailable)?;
            let metadata = load_view_metadata_for_request_async(
                self.runtime(),
                &request.view.namespace,
                &request.view.view,
                &request.context,
            )
            .await?;
            let loaded = loaded_view_from_metadata(&ident, &metadata).map_err(unavailable)?;
            if !loaded.dialect.eq_ignore_ascii_case(VIEW_DIALECT_STARROCKS) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    format!(
                        "Iceberg view uses unsupported SQL dialect {}",
                        loaded.dialect
                    ),
                ));
            }
            ConnectorViewMetadataValue::try_new(
                request.view,
                ConnectorViewDefinition {
                    dialect: ConnectorViewDialect::StarRocks,
                    raw_sql: loaded.raw_sql.into(),
                    default_catalog: loaded.default_catalog.map(Into::into),
                    default_namespace: loaded.default_namespace.into(),
                    source_format: loaded.source_format,
                },
                loaded.column_names.into_iter().map(Into::into).collect(),
                loaded.comment.map(Into::into),
                loaded
                    .properties
                    .into_iter()
                    .map(|(key, value)| (key.into(), value.into()))
                    .collect(),
                &request.context,
            )
        })
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
    let kind = if error.starts_with("unknown view:") {
        ConnectorErrorKind::NotFound
    } else if error.starts_with("corrupt NovaRocks Iceberg view") {
        ConnectorErrorKind::CorruptData
    } else if error.contains("require a REST")
        || error.contains("unsupported SQL dialect")
        || error.starts_with("unsupported NovaRocks Iceberg view")
    {
        ConnectorErrorKind::Unsupported
    } else {
        ConnectorErrorKind::Unavailable
    };
    ConnectorError::new(kind, error)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::resources::IcebergMetadataResources;

    fn novarocks_definition(raw_sql: &str) -> ConnectorViewDefinition {
        ConnectorViewDefinition {
            dialect: ConnectorViewDialect::StarRocks,
            raw_sql: raw_sql.into(),
            default_catalog: Some("source_catalog".into()),
            default_namespace: "source_namespace".into(),
            source_format: Some(ConnectorViewSourceFormat::EffectiveUserSourceV1),
        }
    }

    fn test_context() -> novarocks_spi::connector::ConnectorRequestContext {
        novarocks_spi::connector::ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            novarocks_spi::connector::ConnectorStopOwner::new().view(),
            65536,
            262144,
        )
        .unwrap()
    }

    fn hadoop_runtime() -> (
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
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(
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
            &[],
            &novarocks_definition("select 1"),
            None,
            false,
            &[],
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
    fn view_error_mapping_keeps_capability_and_lookup_failures_typed() {
        assert_eq!(
            unavailable("view operations require a REST iceberg catalog".to_string()).kind(),
            ConnectorErrorKind::Unsupported
        );
        assert_eq!(
            unavailable("unsupported SQL dialect: spark".to_string()).kind(),
            ConnectorErrorKind::Unsupported
        );
        assert_eq!(
            unavailable("unknown view: db.v".to_string()).kind(),
            ConnectorErrorKind::NotFound
        );
        assert_eq!(
            unavailable("corrupt NovaRocks Iceberg view db.v: missing format".to_string()).kind(),
            ConnectorErrorKind::CorruptData
        );
        assert_eq!(
            unavailable("unsupported NovaRocks Iceberg view db.v: unknown format".to_string())
                .kind(),
            ConnectorErrorKind::Unsupported
        );
        assert_eq!(
            unavailable("REST response lost".to_string()).kind(),
            ConnectorErrorKind::Unavailable
        );
    }

    #[test]
    fn novarocks_view_source_context_is_frozen_independently_of_target() {
        let definition = novarocks_definition("SELECT  /* preserve */ 1");
        let (catalog, namespace, format) =
            novarocks_source_context(&definition).expect("valid NovaRocks source context");

        assert_eq!(catalog, "source_catalog");
        assert_eq!(namespace.to_string(), "source_namespace");
        assert_eq!(format, EFFECTIVE_USER_SOURCE_V1);
        assert_eq!(definition.raw_sql.as_ref(), "SELECT  /* preserve */ 1");
    }

    #[test]
    fn provenance_validation_rejects_old_novarocks_and_accepts_third_party_views() {
        let ident = TableIdent::from_strs(["db", "v"]).expect("view ident");
        let third_party = HashMap::new();
        assert_eq!(
            source_format_from_summary(&ident, &third_party).expect("third-party view"),
            None
        );

        let old_novarocks =
            HashMap::from([("engine-name".to_string(), NOVAROCKS_ENGINE_NAME.to_string())]);
        let error = source_format_from_summary(&ident, &old_novarocks)
            .expect_err("NovaRocks provenance without format is corrupt");
        assert!(
            error.starts_with("corrupt NovaRocks Iceberg view"),
            "{error}"
        );

        let unknown_novarocks = HashMap::from([
            ("engine-name".to_string(), NOVAROCKS_ENGINE_NAME.to_string()),
            (
                NOVAROCKS_SOURCE_FORMAT_KEY.to_string(),
                "future-v2".to_string(),
            ),
        ]);
        let error = source_format_from_summary(&ident, &unknown_novarocks)
            .expect_err("unknown NovaRocks format is unsupported");
        assert!(
            error.starts_with("unsupported NovaRocks Iceberg view"),
            "{error}"
        );
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
        let error = ConnectorViewMetadata::load_view(
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
        )
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
        use novarocks_spi::connector::{ConnectorDataType, ConnectorMutationFailureKind as Kind};

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
                Arc::new(novarocks_fs::TokioFileTaskSpawner::new(handle.clone())),
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

        fn column() -> ConnectorColumnDefinition {
            ConnectorColumnDefinition {
                name: "a".into(),
                data_type: ConnectorDataType::Int,
                nullable: true,
                aggregation: None,
                default: None,
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
                &[column()],
                &novarocks_definition("select 2 as a"),
                None,
                true,
                &[],
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
                    &[column()],
                    &novarocks_definition("select 2 as a"),
                    None,
                    replace,
                    &invalid,
                ));
                assert!(message.contains("version.history.num-entries"), "{message}");
            }
            assert_eq!(server.count("POST", "/v1/namespaces/db/views"), 0);
            assert_eq!(server.count("POST", "/v1/namespaces/db/views/v"), 0);
        }
    }

    fn view_provider(runtime: IcebergMetadataContext) -> IcebergMetadata {
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
