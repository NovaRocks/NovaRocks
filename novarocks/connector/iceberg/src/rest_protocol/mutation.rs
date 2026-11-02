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

//! NovaRocks-issued REST mutations and their D13 evidence.
//!
//! Every mutation is written at most once on the generation's session
//! binding, never follows a redirect, and is classified from what the send
//! observed plus the minimal view of a success body (spec 5.7). A caller that
//! must hand a full object back (the transitional adapter, stage, view
//! writes) keeps the complete success document; owner commits decode only the
//! minimal view, chunk by chunk.

pub(crate) mod prepared;

use std::collections::HashMap;

use reqwest::Method;
use reqwest::header::HeaderValue;
use serde::Serialize;

use uuid::Uuid;

use crate::catalog::view_request::{ValidatedViewReplace, ViewCreate, ViewSchema};
use crate::iceberg::spec::Schema;
use crate::iceberg::{NamespaceIdent, TableIdent};
use crate::rest_protocol::binding::SessionBinding;
use crate::rest_protocol::endpoints::RestEndpoints;
#[cfg(test)]
use crate::rest_protocol::endpoints::VENDED_CREDENTIALS;
use crate::rest_protocol::evidence::{
    AttributionExpectation, BodyRead, BodyReadFailure, DispatchStage, NotWrittenCause,
    ResponseShape, RestEvidence, classify,
};
use crate::rest_protocol::http::{BodyReadError, RequestContext, ResponseBody, RestRequest};
use crate::rest_protocol::json::{JsonScanner, ScanError, ViewSink};
use crate::rest_protocol::minimal_view::{
    MutationView, NamespaceSink, TableResponseSink, ViewMutationSink,
};
#[cfg(test)]
use crate::rest_protocol::wire::{CommitTableRequest, CreateNamespaceRequest, CreateTableRequest};

// Design: ADR-0176 (docs/adr/ADR-0176-iceberg-rest-request-vocabulary-and-supported-server-floor.md)
// NovaRocks owns the view wire vocabulary: the upstream REST crate has no
// view API, and the core view types cannot express a request the reference
// server accepts for an unchanged schema.

/// One view version as the REST specification serializes it.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct ViewVersionWire {
    pub(crate) version_id: i32,
    /// `-1` selects the schema added by the same commit.
    pub(crate) schema_id: i32,
    pub(crate) timestamp_ms: i64,
    pub(crate) summary: HashMap<String, String>,
    pub(crate) representations: Vec<SqlRepresentationWire>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) default_catalog: Option<String>,
    pub(crate) default_namespace: NamespaceIdent,
}

/// A SQL representation of a view version.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "type", rename = "sql")]
pub(crate) struct SqlRepresentationWire {
    pub(crate) sql: String,
    pub(crate) dialect: String,
}

/// The view metadata updates NovaRocks issues.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub(crate) enum ViewUpdateWire {
    /// Carries no `last-column-id`: the server derives it.
    AddSchema {
        schema: Box<Schema>,
    },
    #[serde(rename_all = "kebab-case")]
    AddViewVersion {
        view_version: ViewVersionWire,
    },
    /// `-1` selects the version added by the same commit. A replacement
    /// never names an existing version by number: version IDs are reused
    /// after history expiry.
    #[serde(rename_all = "kebab-case")]
    SetCurrentViewVersion {
        view_version_id: i32,
    },
    SetProperties {
        updates: HashMap<String, String>,
    },
}

/// The view requirements NovaRocks issues.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "type")]
pub(crate) enum ViewRequirementWire {
    #[serde(rename = "assert-view-uuid")]
    AssertViewUuid { uuid: Uuid },
}

/// `POST .../namespaces/{ns}/views`. The server assigns the location.
#[derive(Debug, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct CreateViewRequest {
    name: String,
    schema: Schema,
    view_version: ViewVersionWire,
    properties: HashMap<String, String>,
}

/// `POST .../namespaces/{ns}/views/{view}`.
#[derive(Debug, PartialEq, Serialize)]
pub(crate) struct CommitViewRequest {
    identifier: TableIdent,
    requirements: Vec<ViewRequirementWire>,
    updates: Vec<ViewUpdateWire>,
}

impl CreateViewRequest {
    pub(crate) fn encode(creation: ViewCreate) -> Self {
        let (name, schema, view_version, properties) = creation.into_parts();
        Self {
            name,
            schema,
            view_version,
            properties,
        }
    }
}

impl CommitViewRequest {
    /// Encode a validated replacement. Semantic validation happened before:
    /// this only states the replacement in the REST vocabulary. The new
    /// version is always added and selected as the one this commit adds.
    pub(crate) fn encode(replace: &ValidatedViewReplace) -> Self {
        let mut updates = Vec::with_capacity(4);
        if !replace.properties().is_empty() {
            updates.push(ViewUpdateWire::SetProperties {
                updates: replace.properties().clone(),
            });
        }
        if let ViewSchema::New(schema) = replace.schema() {
            updates.push(ViewUpdateWire::AddSchema {
                schema: schema.clone(),
            });
        }
        updates.push(ViewUpdateWire::AddViewVersion {
            view_version: replace.version().clone(),
        });
        updates.push(ViewUpdateWire::SetCurrentViewVersion {
            view_version_id: -1,
        });
        Self {
            identifier: replace.ident().clone(),
            requirements: vec![ViewRequirementWire::AssertViewUuid {
                uuid: replace.uuid(),
            }],
            updates,
        }
    }
}

/// The classified result of one mutation.
pub(crate) struct MutationOutcome {
    pub(crate) evidence: RestEvidence,
    /// The complete success document, kept only when the caller asked for it
    /// and the response was attributed (`Applied`).
    pub(crate) document: Option<Vec<u8>>,
}

impl RestEndpoints {
    /// Commit to an existing table, or publish a CTAS staged create
    /// (`assert-create`), as decided by `expect`.
    #[cfg(test)]
    pub(crate) async fn commit_table(
        &self,
        body: CommitTableRequest,
        expect: &AttributionExpectation,
        keep_document: bool,
    ) -> MutationOutcome {
        let ident = body
            .identifier
            .as_ref()
            .expect("Protocol fixture must name an exact table")
            .clone();
        self.mutate(
            Method::POST,
            |binding| binding.table_url(&ident),
            Some(&body),
            None,
            expect,
            keep_document,
        )
        .await
    }

    /// Create a table immediately (static access only).
    #[cfg(test)]
    pub(crate) async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        request: CreateTableRequest,
    ) -> MutationOutcome {
        self.mutate(
            Method::POST,
            |binding| binding.tables_url(namespace),
            Some(&request),
            None,
            &AttributionExpectation::create_table(),
            true,
        )
        .await
    }

    /// Stage a create (`stage-create: true`), asking for vended credentials in
    /// both access modes, as today (L23).
    #[cfg(test)]
    pub(crate) async fn stage_create(
        &self,
        namespace: &NamespaceIdent,
        mut request: CreateTableRequest,
    ) -> MutationOutcome {
        request.stage_create = Some(true);
        self.mutate(
            Method::POST,
            |binding| binding.tables_url(namespace),
            Some(&request),
            Some(VENDED_CREDENTIALS),
            &AttributionExpectation::stage_create(),
            true,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> MutationOutcome {
        let request = CreateNamespaceRequest {
            namespace: namespace.clone(),
            properties,
        };
        self.mutate(
            Method::POST,
            SessionBinding::namespaces_url,
            Some(&request),
            None,
            &AttributionExpectation::create_namespace(namespace.as_ref().clone()),
            false,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn drop_namespace(&self, namespace: &NamespaceIdent) -> MutationOutcome {
        self.mutate::<()>(
            Method::DELETE,
            |binding| binding.namespace_url(namespace),
            None,
            None,
            &AttributionExpectation::drop_namespace(),
            false,
        )
        .await
    }

    /// Drop a table. `purgeRequested` is never sent.
    #[cfg(test)]
    pub(crate) async fn drop_table(&self, table: &TableIdent) -> MutationOutcome {
        self.mutate::<()>(
            Method::DELETE,
            |binding| binding.table_url(table),
            None,
            None,
            &AttributionExpectation::drop_table(),
            false,
        )
        .await
    }

    /// Create a view from its validated first version.
    #[cfg(test)]
    pub(crate) async fn create_view(
        &self,
        namespace: &NamespaceIdent,
        creation: ViewCreate,
    ) -> MutationOutcome {
        let request = CreateViewRequest::encode(creation);
        self.mutate(
            Method::POST,
            |binding| binding.views_url(namespace),
            Some(&request),
            None,
            &AttributionExpectation::create_view(),
            true,
        )
        .await
    }

    /// Replace a view's definition; `expect` carries the frozen view UUID.
    #[cfg(test)]
    pub(crate) async fn replace_view(
        &self,
        replace: &ValidatedViewReplace,
        expect: &AttributionExpectation,
    ) -> MutationOutcome {
        let request = CommitViewRequest::encode(replace);
        self.mutate(
            Method::POST,
            |binding| binding.view_url(replace.ident()),
            Some(&request),
            None,
            expect,
            true,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn drop_view(&self, view: &TableIdent) -> MutationOutcome {
        self.mutate::<()>(
            Method::DELETE,
            |binding| binding.view_url(view),
            None,
            None,
            &AttributionExpectation::drop_view(),
            false,
        )
        .await
    }

    /// Issue one mutation and classify it.
    async fn mutate<B: Serialize>(
        &self,
        method: Method,
        url: impl FnOnce(&SessionBinding) -> String,
        body: Option<&B>,
        delegation: Option<HeaderValue>,
        expect: &AttributionExpectation,
        keep_document: bool,
    ) -> MutationOutcome {
        let binding = match self.session().await {
            Ok(binding) => binding,
            Err(failure) => {
                return not_written(NotWrittenCause::Binding(failure.step), expect);
            }
        };
        let mut headers = binding.request_headers(delegation);
        if let Err(failure) = self.owner().authorize(&mut headers).await {
            return not_written(NotWrittenCause::Binding(failure.step), expect);
        }
        let request = RestRequest::new(method, url(&binding), headers);
        let request = match body {
            Some(body) => request.with_json_body(body),
            None => Ok(request),
        };
        let (stage, body) = self
            .owner()
            .http()
            .send_once(
                request,
                expect.endpoint().success_statuses(),
                // The legacy migration path retains a finite request phase.
                // Prepared publication consumes the journal's evidence grant.
                &RequestContext::for_read(),
            )
            .await
            .expect("the legacy read context has no original MIO admission source");
        let (read, document) = match (expect.endpoint().response_shape(), body) {
            (Some(shape), Some(body)) => read_view(shape, expect, body, keep_document).await,
            // A drop's success body must be empty (L24); its bytes are only
            // counted, never kept.
            (None, Some(body)) => (read_empty(body).await, None),
            _ => (BodyRead::NotRead, None),
        };
        let evidence = classify(&stage, read, expect);
        let document = document.filter(|_| matches!(evidence, RestEvidence::Applied { .. }));
        MutationOutcome { evidence, document }
    }
}

fn not_written(cause: NotWrittenCause, expect: &AttributionExpectation) -> MutationOutcome {
    MutationOutcome {
        evidence: classify(&DispatchStage::NotWritten(cause), BodyRead::NotRead, expect),
        document: None,
    }
}

/// Scan and optionally retain a success body in the same bounded read phase.
/// A retained document never starts a second synchronous attribution parse.
async fn read_view(
    shape: ResponseShape,
    expect: &AttributionExpectation,
    body: ResponseBody,
    keep_document: bool,
) -> (BodyRead<MutationView>, Option<Vec<u8>>) {
    let view = match shape {
        ResponseShape::Table => stream(body, table_sink(expect), keep_document)
            .await
            .map(|(view, document)| (MutationView::Table(view), document)),
        ResponseShape::Namespace => stream(body, NamespaceSink::new(), keep_document)
            .await
            .map(|(view, document)| (MutationView::Namespace(view), document)),
        ResponseShape::View => stream(body, ViewMutationSink::new(), keep_document)
            .await
            .map(|(view, document)| (MutationView::View(view), document)),
    };
    match view {
        Ok((view, document)) => (BodyRead::Complete(view), document),
        Err(failure) => (BodyRead::Failed(failure), None),
    }
}

/// Read a drop's success body: `Empty` only if it ends without a byte.
async fn read_empty(mut body: ResponseBody) -> BodyRead<MutationView> {
    loop {
        match body.next_chunk().await {
            Ok(None) => return BodyRead::Empty,
            Ok(Some(chunk)) if chunk.is_empty() => {}
            // Bytes in a drop's 200 are a different answer than "dropped".
            Ok(Some(_)) => return BodyRead::NotRead,
            Err(error) => return BodyRead::Failed(read_failure(error)),
        }
    }
}

fn table_sink(expect: &AttributionExpectation) -> TableResponseSink {
    TableResponseSink::new(expect.table_view_request().cloned().unwrap_or_default())
}

/// Scan a body chunk by chunk; no raw byte outlives its chunk (L8).
async fn stream<S: ViewSink>(
    mut body: ResponseBody,
    sink: S,
    keep_document: bool,
) -> Result<(S::View, Option<Vec<u8>>), BodyReadFailure> {
    let mut scanner = JsonScanner::new(sink);
    let mut document = keep_document.then(Vec::new);
    while let Some(chunk) = body.next_chunk().await.map_err(read_failure)? {
        scanner.feed(&chunk).map_err(scan_failure)?;
        if let Some(document) = &mut document {
            document.extend_from_slice(&chunk);
        }
    }
    Ok((scanner.finish().map_err(scan_failure)?, document))
}

fn scan_failure(error: ScanError) -> BodyReadFailure {
    BodyReadFailure::Scan(error)
}

fn read_failure(error: BodyReadError) -> BodyReadFailure {
    match error {
        BodyReadError::DeadlineExceeded => BodyReadFailure::DeadlineExceeded,
        BodyReadError::Transport => BodyReadFailure::Transport,
    }
}

#[cfg(test)]
mod tests {
    //! D13 evidence end to end: every NovaRocks mutation endpoint against every
    //! observation of spec 5.7, through the real session and HTTP layer. The
    //! oracle below is written from the spec's tables, independently of the
    //! classifier, and the IRU-5 consequence is checked as `permits_cleanup`.
    //! Each case owns a loopback scripted server; Drop joins it.
    use std::collections::HashMap;
    use std::sync::Arc;

    use novarocks_spi::connector::ConnectorMutationFailureKind as Kind;
    use uuid::Uuid;

    use super::MutationOutcome;
    use crate::catalog::error::CatalogCommitEvidence;
    use crate::catalog::view_request::{ValidatedViewReplace, ViewCreate, ViewDefinition};
    use crate::iceberg::spec::{NestedField, PrimitiveType, Schema, Type, ViewMetadata};
    use crate::iceberg::{NamespaceIdent, TableIdent, TableRequirement};
    use crate::rest_protocol::endpoints::RestEndpoints;
    use crate::rest_protocol::evidence::{AttributionExpectation, ExpectedSnapshot, RestEvidence};
    use crate::rest_protocol::test_server::{BodyPlan, Reply, Script, TestServer};
    use crate::rest_protocol::wire::CommitTableRequest;

    const TABLE_UUID: &str = "1a2b3c4d-0000-4000-8000-000000000001";
    const VIEW_UUID: &str = "1a2b3c4d-0000-4000-8000-000000000002";
    const SNAPSHOT: i64 = 6;
    const MARKER: (&str, &str) = ("novarocks.ctas.operation-id", "op-1");

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum Endpoint {
        TableCommit,
        CtasFinalCommit,
        CreateTable,
        StageCreate,
        CreateNamespace,
        DropNamespace,
        DropTable,
        CreateView,
        ReplaceView,
        DropView,
    }

    const ENDPOINTS: [Endpoint; 10] = [
        Endpoint::TableCommit,
        Endpoint::CtasFinalCommit,
        Endpoint::CreateTable,
        Endpoint::StageCreate,
        Endpoint::CreateNamespace,
        Endpoint::DropNamespace,
        Endpoint::DropTable,
        Endpoint::CreateView,
        Endpoint::ReplaceView,
        Endpoint::DropView,
    ];

    impl Endpoint {
        fn route(self) -> (&'static str, &'static str) {
            match self {
                Self::TableCommit | Self::CtasFinalCommit => ("POST", "/v1/namespaces/db/tables/t"),
                Self::CreateTable | Self::StageCreate => ("POST", "/v1/namespaces/db/tables"),
                Self::CreateNamespace => ("POST", "/v1/namespaces"),
                Self::DropNamespace => ("DELETE", "/v1/namespaces/db"),
                Self::DropTable => ("DELETE", "/v1/namespaces/db/tables/t"),
                Self::CreateView => ("POST", "/v1/namespaces/db/views"),
                Self::ReplaceView => ("POST", "/v1/namespaces/db/views/v"),
                Self::DropView => ("DELETE", "/v1/namespaces/db/views/v"),
            }
        }

        fn is_drop(self) -> bool {
            matches!(self, Self::DropNamespace | Self::DropTable | Self::DropView)
        }

        fn creates(self) -> bool {
            matches!(
                self,
                Self::CtasFinalCommit
                    | Self::CreateTable
                    | Self::StageCreate
                    | Self::CreateNamespace
                    | Self::CreateView
            )
        }

        /// A body that satisfies every attribution item for this endpoint.
        fn attributable_body(self) -> Option<serde_json::Value> {
            let table = |location: serde_json::Value| {
                serde_json::json!({
                    "metadata-location": location,
                    "metadata": {
                        "table-uuid": TABLE_UUID,
                        "snapshots": [{"snapshot-id": 5}, {"snapshot-id": SNAPSHOT}],
                        "refs": {"main": {"snapshot-id": SNAPSHOT, "type": "branch"}},
                        "properties": {MARKER.0: MARKER.1},
                    },
                })
            };
            match self {
                Self::TableCommit | Self::CtasFinalCommit | Self::CreateTable => Some(table(
                    serde_json::json!("s3://warehouse/db/t/metadata/2.json"),
                )),
                Self::StageCreate => Some(table(serde_json::Value::Null)),
                Self::CreateNamespace => {
                    Some(serde_json::json!({"namespace": ["db"], "properties": {}}))
                }
                Self::CreateView | Self::ReplaceView => Some(serde_json::json!({
                    "metadata-location": "s3://warehouse/db/v/metadata/1.json",
                    "metadata": {"view-uuid": VIEW_UUID},
                })),
                Self::DropNamespace | Self::DropTable | Self::DropView => None,
            }
        }
    }

    /// What spec 5.7 prescribes for a status line, independent of the code.
    #[derive(Debug, Eq, PartialEq)]
    enum Expected {
        NotDispatched(Kind),
        Rejected(Kind),
        Applied,
        Unknown,
    }

    fn oracle(endpoint: Endpoint, status: u16) -> Expected {
        let success = if endpoint.is_drop() { 204 } else { 200 };
        match status {
            status if status == success => Expected::Applied,
            // `status_reply` answers a drop's 200 with an error body: unknown.
            200 if endpoint.is_drop() => Expected::Unknown,
            409 if endpoint.creates() => Expected::Rejected(Kind::AlreadyExists),
            409 => Expected::Rejected(Kind::Conflict),
            401 | 419 => Expected::Rejected(Kind::Unauthenticated),
            403 => Expected::Rejected(Kind::PermissionDenied),
            _ => Expected::Unknown,
        }
    }

    fn observed(evidence: &RestEvidence) -> Expected {
        match evidence {
            RestEvidence::NotDispatched { kind, .. } => Expected::NotDispatched(*kind),
            RestEvidence::Rejected { kind, .. } => Expected::Rejected(*kind),
            RestEvidence::Applied { .. } => Expected::Applied,
            RestEvidence::Unknown { .. } => Expected::Unknown,
        }
    }

    async fn endpoints(server: &TestServer, extra: &[(&str, &str)]) -> Arc<RestEndpoints> {
        let mut properties: HashMap<String, String> = extra
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        properties.insert("uri".to_string(), server.url());
        crate::catalog_runtime::build_vended_rest_generation_for_test(
            properties,
            None,
            crate::iceberg::Runtime::try_current().expect("test runtime"),
        )
        .await
        .expect("REST generation")
    }

    fn uuid(text: &str) -> Uuid {
        Uuid::parse_str(text).unwrap()
    }

    fn table() -> TableIdent {
        TableIdent::new(NamespaceIdent::new("db".to_string()), "t".to_string())
    }

    fn schema() -> Schema {
        Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap()
    }

    fn create_request() -> crate::rest_protocol::wire::CreateTableRequest {
        crate::rest_protocol::wire::CreateTableRequest {
            name: "t".to_string(),
            location: None,
            schema: schema(),
            partition_spec: None,
            write_order: None,
            stage_create: None,
            properties: HashMap::new(),
        }
    }

    fn view_definition() -> ViewDefinition {
        ViewDefinition {
            schema: schema(),
            sql: "select 1".to_string(),
            summary: HashMap::new(),
            default_catalog: None,
            default_namespace: NamespaceIdent::new("db".to_string()),
            timestamp_ms: 1_700_000_000_000,
        }
    }

    fn view_creation() -> ViewCreate {
        ViewCreate::validate("v".to_string(), view_definition(), HashMap::new()).unwrap()
    }

    /// View `db.v` with one version over schema 0 (`schema()`).
    fn view_metadata() -> ViewMetadata {
        serde_json::from_value(serde_json::json!({
            "view-uuid": VIEW_UUID,
            "format-version": 1,
            "location": "s3://warehouse/db/v",
            "current-version-id": 1,
            "versions": [{
                "version-id": 1,
                "schema-id": 0,
                "timestamp-ms": 1_600_000_000_000i64,
                "summary": {},
                "representations": [{"type": "sql", "sql": "select 0", "dialect": "starrocks"}],
                "default-namespace": ["db"],
            }],
            "version-log": [{"version-id": 1, "timestamp-ms": 1_600_000_000_000i64}],
            "schemas": [serde_json::to_value(schema()).unwrap()],
            "properties": {},
        }))
        .unwrap()
    }

    fn view_replace() -> ValidatedViewReplace {
        ValidatedViewReplace::validate(
            TableIdent::new(NamespaceIdent::new("db".to_string()), "v".to_string()),
            &view_metadata(),
            view_definition(),
            HashMap::new(),
        )
        .unwrap()
    }

    async fn dispatch(endpoints: &RestEndpoints, endpoint: Endpoint) -> MutationOutcome {
        let db = NamespaceIdent::new("db".to_string());
        let snapshot = || Some(ExpectedSnapshot::new(SNAPSHOT, Some("main".to_string())));
        match endpoint {
            Endpoint::TableCommit => {
                let commit = CommitTableRequest {
                    identifier: Some(table()),
                    requirements: vec![TableRequirement::UuidMatch {
                        uuid: uuid(TABLE_UUID),
                    }],
                    updates: vec![],
                };
                let expect =
                    AttributionExpectation::table_commit(uuid(TABLE_UUID), snapshot()).unwrap();
                endpoints.commit_table(commit, &expect, false).await
            }
            Endpoint::CtasFinalCommit => {
                let commit = CommitTableRequest {
                    identifier: Some(table()),
                    requirements: vec![TableRequirement::NotExist],
                    updates: vec![],
                };
                let expect = AttributionExpectation::ctas_final_commit(
                    uuid(TABLE_UUID),
                    snapshot(),
                    vec![(MARKER.0.to_string(), MARKER.1.to_string())],
                )
                .unwrap();
                endpoints.commit_table(commit, &expect, false).await
            }
            Endpoint::CreateTable => endpoints.create_table(&db, create_request()).await,
            Endpoint::StageCreate => endpoints.stage_create(&db, create_request()).await,
            Endpoint::CreateNamespace => endpoints.create_namespace(&db, HashMap::new()).await,
            Endpoint::DropNamespace => endpoints.drop_namespace(&db).await,
            Endpoint::DropTable => endpoints.drop_table(&table()).await,
            Endpoint::CreateView => endpoints.create_view(&db, view_creation()).await,
            Endpoint::ReplaceView => {
                endpoints
                    .replace_view(
                        &view_replace(),
                        &AttributionExpectation::replace_view(uuid(VIEW_UUID)),
                    )
                    .await
            }
            Endpoint::DropView => {
                endpoints
                    .drop_view(&TableIdent::new(db, "v".to_string()))
                    .await
            }
        }
    }

    /// Run one endpoint against one scripted reply on a fresh session.
    async fn observe(endpoint: Endpoint, reply: Reply) -> (MutationOutcome, TestServer) {
        let (method, path) = endpoint.route();
        let server = TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on(method, path, reply)
                .on("GET", "/redirected", Reply::status(200))
                .on("POST", "/redirected", Reply::status(200)),
        );
        let endpoints = endpoints(&server, &[]).await;
        let outcome = dispatch(&endpoints, endpoint).await;
        (outcome, server)
    }

    fn assert_cleanup_follows_evidence(outcome: &MutationOutcome) {
        let permits = outcome
            .evidence
            .clone()
            .into_catalog_outcome(CatalogCommitEvidence::for_target("db.t"))
            .permits_cleanup();
        let proven_not_applied = matches!(
            outcome.evidence,
            RestEvidence::NotDispatched { .. } | RestEvidence::Rejected { .. }
        );
        assert_eq!(permits, proven_not_applied, "{:?}", outcome.evidence);
    }

    fn status_reply(endpoint: Endpoint, status: u16) -> Reply {
        let success = if endpoint.is_drop() { 204 } else { 200 };
        match endpoint.attributable_body() {
            Some(body) if status == success => Reply::json(status, &body),
            _ => Reply::json(
                status,
                &serde_json::json!({"error": {"message": "secret-canary", "type": "X", "code": status}}),
            ),
        }
    }

    #[tokio::test]
    async fn every_endpoint_maps_every_status_line_as_spec_5_7_prescribes() {
        let statuses = [
            200, 201, 202, 204, 400, 401, 403, 404, 408, 409, 419, 429, 500, 501, 502, 503, 504,
            505,
        ];
        for endpoint in ENDPOINTS {
            for status in statuses {
                let (outcome, server) = observe(endpoint, status_reply(endpoint, status)).await;
                assert_eq!(
                    observed(&outcome.evidence),
                    oracle(endpoint, status),
                    "{endpoint:?} HTTP {status}"
                );
                assert_cleanup_follows_evidence(&outcome);
                assert!(!outcome.evidence.message().contains("secret-canary"));
                let (method, path) = endpoint.route();
                assert_eq!(server.count(method, path), 1, "written once: {endpoint:?}");
            }
        }
    }

    #[tokio::test]
    async fn redirects_are_unknown_and_never_followed() {
        for endpoint in ENDPOINTS {
            for status in [301, 302, 303, 307, 308] {
                let (outcome, server) =
                    observe(endpoint, Reply::redirect(status, "/redirected")).await;
                assert_eq!(
                    observed(&outcome.evidence),
                    Expected::Unknown,
                    "{endpoint:?}"
                );
                assert_eq!(server.count("GET", "/redirected"), 0);
                assert_eq!(server.count("POST", "/redirected"), 0);
            }
        }
    }

    #[tokio::test]
    async fn success_bodies_must_be_complete_and_attributable() {
        for endpoint in ENDPOINTS {
            if endpoint.is_drop() {
                // A drop succeeds with 204 or an empty 200; a 200 with any
                // body, HTML included, is unknown (L24 as revised).
                let (outcome, _server) = observe(endpoint, Reply::bytes(200, Vec::new())).await;
                assert_eq!(
                    observed(&outcome.evidence),
                    Expected::Applied,
                    "{endpoint:?}"
                );
                for body in [b"<html>ok</html>".to_vec(), b"{}".to_vec()] {
                    let (outcome, _server) = observe(endpoint, Reply::bytes(200, body)).await;
                    assert_eq!(
                        observed(&outcome.evidence),
                        Expected::Unknown,
                        "{endpoint:?}"
                    );
                }
                continue;
            }
            let good = endpoint.attributable_body().unwrap().to_string();
            let mut bad: Vec<(&str, Vec<u8>)> = vec![
                ("html", b"<html>gateway</html>".to_vec()),
                ("truncated", good.as_bytes()[..good.len() / 2].to_vec()),
                ("trailing", format!("{good} {{}}").into_bytes()),
            ];
            let mut value: serde_json::Value = serde_json::from_str(&good).unwrap();
            match endpoint {
                Endpoint::CreateNamespace => {
                    value["namespace"] = serde_json::json!(["other"]);
                    bad.push(("namespace mismatch", value.to_string().into_bytes()));
                }
                Endpoint::CreateView | Endpoint::ReplaceView => {
                    let mut missing = value.clone();
                    missing.as_object_mut().unwrap().remove("metadata-location");
                    bad.push(("missing location", missing.to_string().into_bytes()));
                    if endpoint == Endpoint::ReplaceView {
                        value["metadata"]["view-uuid"] =
                            serde_json::json!("ffffffff-0000-4000-8000-000000000000");
                        bad.push(("uuid mismatch", value.to_string().into_bytes()));
                    }
                }
                _ => {
                    if endpoint != Endpoint::StageCreate {
                        let mut missing = value.clone();
                        missing.as_object_mut().unwrap().remove("metadata-location");
                        bad.push(("missing location", missing.to_string().into_bytes()));
                    }
                    if matches!(endpoint, Endpoint::TableCommit | Endpoint::CtasFinalCommit) {
                        let mut other = value.clone();
                        other["metadata"]["table-uuid"] =
                            serde_json::json!("ffffffff-0000-4000-8000-000000000000");
                        bad.push(("uuid mismatch", other.to_string().into_bytes()));
                        let mut absent = value.clone();
                        absent["metadata"]["snapshots"] = serde_json::json!([{"snapshot-id": 5}]);
                        bad.push(("snapshot absent", absent.to_string().into_bytes()));
                    }
                    if endpoint == Endpoint::CtasFinalCommit {
                        let mut unmarked = value.clone();
                        unmarked["metadata"]["properties"] = serde_json::json!({});
                        bad.push(("marker absent", unmarked.to_string().into_bytes()));
                    }
                }
            }
            for (label, body) in bad {
                let (outcome, _server) = observe(endpoint, Reply::bytes(200, body)).await;
                assert_eq!(
                    observed(&outcome.evidence),
                    Expected::Unknown,
                    "{endpoint:?} {label}"
                );
                assert_cleanup_follows_evidence(&outcome);
            }
        }
    }

    #[tokio::test]
    async fn a_refreshed_commit_response_is_applied() {
        // S is present but `main` already points at a later commit.
        let mut body = Endpoint::TableCommit.attributable_body().unwrap();
        body["metadata"]["snapshots"] =
            serde_json::json!([{"snapshot-id": SNAPSHOT}, {"snapshot-id": 7}]);
        body["metadata"]["refs"]["main"]["snapshot-id"] = serde_json::json!(7);
        let (outcome, _server) = observe(Endpoint::TableCommit, Reply::json(200, &body)).await;
        let RestEvidence::Applied { receipt } = &outcome.evidence else {
            panic!("refreshed response must be applied: {:?}", outcome.evidence);
        };
        assert_eq!(receipt.snapshot_id, Some(SNAPSHOT));
    }

    #[tokio::test]
    async fn a_success_body_arriving_in_chunks_is_still_attributed() {
        let body = Endpoint::TableCommit
            .attributable_body()
            .unwrap()
            .to_string()
            .into_bytes();
        let chunks = body
            .chunks(7)
            .map(|chunk| (std::time::Duration::from_millis(1), chunk.to_vec()))
            .collect();
        let (outcome, _server) = observe(
            Endpoint::TableCommit,
            Reply::status(200).with_body(BodyPlan::Chunked(chunks)),
        )
        .await;
        assert_eq!(observed(&outcome.evidence), Expected::Applied);
    }

    #[tokio::test]
    async fn written_requests_without_a_status_line_are_unknown() {
        for endpoint in ENDPOINTS {
            let (outcome, _server) = observe(endpoint, Reply::CloseAfterRequest).await;
            assert_eq!(
                observed(&outcome.evidence),
                Expected::Unknown,
                "{endpoint:?}"
            );
            assert_cleanup_follows_evidence(&outcome);
        }
    }

    #[tokio::test]
    async fn establishment_failures_are_not_dispatched_with_their_step_kind() {
        let cases: Vec<(&str, Script, Expected)> = vec![
            (
                "token rejected",
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on("POST", "/v1/oauth/tokens", Reply::status(401)),
                Expected::NotDispatched(Kind::Unauthenticated),
            ),
            (
                "token unavailable",
                Script::new()
                    .on("GET", "/v1/config", Reply::config(&[], &[], None))
                    .on("POST", "/v1/oauth/tokens", Reply::status(503)),
                Expected::NotDispatched(Kind::Unavailable),
            ),
            (
                "config failure",
                Script::new()
                    .on("POST", "/v1/oauth/tokens", Reply::token("tok"))
                    .on("GET", "/v1/config", Reply::status(500)),
                Expected::NotDispatched(Kind::Unavailable),
            ),
            (
                "server-sent token",
                Script::new()
                    .on("POST", "/v1/oauth/tokens", Reply::token("tok"))
                    .on(
                        "GET",
                        "/v1/config",
                        Reply::config(&[("token", "x")], &[], None),
                    ),
                Expected::NotDispatched(Kind::Unsupported),
            ),
        ];
        for (label, script, expected) in cases {
            let server = TestServer::start(script.on(
                "POST",
                "/v1/namespaces/db/tables/t",
                Reply::status(500),
            ));
            let mut properties = HashMap::new();
            properties.insert("uri".to_string(), server.url());
            let endpoints = crate::catalog_runtime::build_vended_rest_generation_for_test(
                properties,
                Some(crate::rest_protocol::test_server::oauth2_identity(
                    "fe", "secret",
                )),
                crate::iceberg::Runtime::try_current().unwrap(),
            )
            .await
            .unwrap();
            let outcome = dispatch(&endpoints, Endpoint::TableCommit).await;
            assert_eq!(observed(&outcome.evidence), expected, "{label}");
            assert_eq!(
                server.count("POST", "/v1/namespaces/db/tables/t"),
                0,
                "{label}"
            );
            assert_cleanup_follows_evidence(&outcome);
        }
    }

    mod view_wire {
        //! The view request bodies, fixed as JSON. The first version is
        //! compared with the upstream `ViewVersion` serialization the vendored
        //! client used, field by field.
        use super::super::{CommitViewRequest, CreateViewRequest};
        use super::*;
        use crate::catalog::view_request::OPERATION_ID_SUMMARY_KEY;
        use crate::iceberg::spec::{SqlViewRepresentation, ViewRepresentation, ViewVersion};

        fn summary() -> HashMap<String, String> {
            HashMap::from([("engine-name".to_string(), "novarocks".to_string())])
        }

        fn definition(schema: Schema, sql: &str) -> ViewDefinition {
            ViewDefinition {
                schema,
                sql: sql.to_string(),
                summary: summary(),
                default_catalog: Some("source".to_string()),
                default_namespace: NamespaceIdent::new("db".to_string()),
                timestamp_ms: 1_700_000_000_000,
            }
        }

        fn two_columns() -> Schema {
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                    Arc::new(NestedField::optional(
                        2,
                        "name",
                        Type::Primitive(PrimitiveType::String),
                    )),
                ])
                .build()
                .unwrap()
        }

        /// The encoded replacement with its operation identity replaced by a
        /// known value.
        fn encoded(replace: &ValidatedViewReplace) -> serde_json::Value {
            let mut json = serde_json::to_value(CommitViewRequest::encode(replace)).unwrap();
            for update in json["updates"].as_array_mut().unwrap() {
                if update["action"] == "add-view-version" {
                    let summary = &mut update["view-version"]["summary"];
                    assert!(summary[OPERATION_ID_SUMMARY_KEY].is_string());
                    summary[OPERATION_ID_SUMMARY_KEY] = "op".into();
                }
            }
            json
        }

        fn replace(schema: Schema, properties: &[(&str, &str)]) -> ValidatedViewReplace {
            ValidatedViewReplace::validate(
                TableIdent::new(NamespaceIdent::new("db".to_string()), "v".to_string()),
                &view_metadata(),
                definition(schema, "select 2"),
                properties
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect(),
            )
            .unwrap()
        }

        fn version_json(schema_id: i32) -> serde_json::Value {
            serde_json::json!({
                "version-id": 2,
                "schema-id": schema_id,
                "timestamp-ms": 1_700_000_000_000i64,
                "summary": {"engine-name": "novarocks", "novarocks.operation-id": "op"},
                "representations": [{"type": "sql", "sql": "select 2", "dialect": "starrocks"}],
                "default-catalog": "source",
                "default-namespace": ["db"],
            })
        }

        #[test]
        fn a_creation_states_the_first_version_without_a_location() {
            let creation = ViewCreate::validate(
                "v".to_string(),
                definition(schema(), "select 1"),
                HashMap::from([("comment".to_string(), "c".to_string())]),
            )
            .unwrap();
            let json = serde_json::to_value(CreateViewRequest::encode(creation)).unwrap();
            let upstream = ViewVersion::builder()
                .with_version_id(1)
                .with_schema_id(schema().schema_id())
                .with_timestamp_ms(1_700_000_000_000)
                .with_summary(summary())
                .with_representations(
                    serde_json::from_value(
                        serde_json::to_value(vec![ViewRepresentation::Sql(
                            SqlViewRepresentation {
                                sql: "select 1".to_string(),
                                dialect: "starrocks".to_string(),
                            },
                        )])
                        .unwrap(),
                    )
                    .unwrap(),
                )
                .with_default_catalog(Some("source".to_string()))
                .with_default_namespace(NamespaceIdent::new("db".to_string()))
                .build();
            assert_eq!(
                json,
                serde_json::json!({
                    "name": "v",
                    "schema": serde_json::to_value(schema()).unwrap(),
                    "view-version": serde_json::to_value(&upstream).unwrap(),
                    "properties": {"comment": "c"},
                })
            );
        }

        #[test]
        fn an_unchanged_schema_is_referenced_and_the_added_version_selected() {
            assert_eq!(
                encoded(&replace(schema(), &[])),
                serde_json::json!({
                    "identifier": {"namespace": ["db"], "name": "v"},
                    "requirements": [{"type": "assert-view-uuid", "uuid": VIEW_UUID}],
                    "updates": [
                        {"action": "add-view-version", "view-version": version_json(0)},
                        {"action": "set-current-view-version", "view-version-id": -1},
                    ],
                })
            );
        }

        #[test]
        fn a_changed_schema_is_added_and_properties_come_first() {
            let mut added = serde_json::to_value(
                two_columns()
                    .into_builder()
                    .with_schema_id(1)
                    .build()
                    .unwrap(),
            )
            .unwrap();
            assert!(added.get("last-column-id").is_none());
            added["schema-id"] = 1.into();
            assert_eq!(
                encoded(&replace(two_columns(), &[("comment", "c")])),
                serde_json::json!({
                    "identifier": {"namespace": ["db"], "name": "v"},
                    "requirements": [{"type": "assert-view-uuid", "uuid": VIEW_UUID}],
                    "updates": [
                        {"action": "set-properties", "updates": {"comment": "c"}},
                        {"action": "add-schema", "schema": added},
                        {"action": "add-view-version", "view-version": version_json(-1)},
                        {"action": "set-current-view-version", "view-version-id": -1},
                    ],
                })
            );
        }
    }
}

#[cfg(test)]
mod scratch_probe {
    #[test]
    fn scratch_probe_create_response() {
        let Ok(path) = std::env::var("SCRATCH_RESPONSE") else {
            return;
        };
        let bytes = std::fs::read(path).unwrap();
        let expect = crate::rest_protocol::evidence::AttributionExpectation::create_table();
        let view = crate::rest_protocol::json::scan_complete(
            crate::rest_protocol::minimal_view::TableResponseSink::new(
                expect.table_view_request().cloned().unwrap_or_default(),
            ),
            &bytes,
        );
        println!(
            "view: {:?}",
            view.as_ref().map(|_| "ok").map_err(|e| format!("{e:?}"))
        );
        let read = match view {
            Ok(v) => crate::rest_protocol::evidence::BodyRead::Complete(
                crate::rest_protocol::minimal_view::MutationView::Table(v),
            ),
            Err(e) => crate::rest_protocol::evidence::BodyRead::Failed(
                crate::rest_protocol::evidence::BodyReadFailure::Scan(e),
            ),
        };
        let evidence = crate::rest_protocol::evidence::classify(
            &crate::rest_protocol::evidence::DispatchStage::Status {
                code: 200,
                header_names: vec![],
            },
            read,
            &expect,
        );
        println!("evidence: {evidence:?}");
        let decoded = serde_json::from_slice::<crate::rest_protocol::wire::LoadTableResult>(&bytes);
        println!("decode ok: {}", decoded.is_ok());
    }
}
