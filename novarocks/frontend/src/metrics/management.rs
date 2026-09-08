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

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;
use axum::{
    Json, Router,
    routing::{get, post},
};

use crate::coordinator::QueryLifecycleConvergenceReader;
use crate::topology::{BackendIslandSnapshot, BackendIslandSnapshotReader};
use crate::workload_lifecycle::{
    FrontendCatalogServingSnapshot, FrontendDrainServingSnapshot, FrontendServingLifecycle,
    FrontendServingSnapshot, FrontendServingSnapshotReader, FrontendServingState,
    FrontendWorkloadServingSnapshot,
};

use super::{FrontendMetricsRegistry, render_metrics, render_metrics_json};

#[derive(Clone)]
struct FrontendManagementState {
    registry: Arc<FrontendMetricsRegistry>,
    serving_reader: Arc<dyn FrontendServingSnapshotReader>,
    island_reader: Arc<dyn BackendIslandSnapshotReader>,
}

/// Versioned management document composed from independent read-only owners.
/// The serving lifecycle remains FE-local; topology remains the membership
/// authority. This type only joins their already-sanitized observations.
#[derive(serde::Serialize)]
struct FrontendManagementSnapshot {
    schema_version: u8,
    serving_state: FrontendServingState,
    catalog: FrontendCatalogServingSnapshot,
    workload: FrontendWorkloadServingSnapshot,
    drain: FrontendDrainServingSnapshot,
    island: FrontendIslandManagementSnapshot,
}

#[derive(serde::Serialize)]
struct FrontendIslandManagementSnapshot {
    native_compatibility_id: String,
    topology_revision: u64,
    compatible_eligible_backend_count: usize,
    other_island_backend_count: usize,
    unknown_or_invalid_backend_count: usize,
    ready: bool,
}

impl FrontendManagementSnapshot {
    fn compose(serving: FrontendServingSnapshot, island: BackendIslandSnapshot) -> Self {
        let ready = serving.base_ready() && island.compatible_eligible_backend_count() >= 1;
        Self {
            schema_version: 2,
            serving_state: serving.serving_state,
            catalog: serving.catalog,
            workload: serving.workload,
            drain: serving.drain,
            island: FrontendIslandManagementSnapshot {
                native_compatibility_id: island.native_compatibility_id().to_string(),
                topology_revision: island.topology_revision(),
                compatible_eligible_backend_count: island.compatible_eligible_backend_count(),
                other_island_backend_count: island.other_island_backend_count(),
                unknown_or_invalid_backend_count: island.unknown_or_invalid_backend_count(),
                ready,
            },
        }
    }
}

/// Builds the complete Frontend management HTTP surface. Native report gRPC
/// must not compose any management routes.
pub(crate) fn frontend_management_router(
    registry: Arc<FrontendMetricsRegistry>,
    convergence_reader: Arc<dyn QueryLifecycleConvergenceReader>,
    island_reader: Arc<dyn BackendIslandSnapshotReader>,
) -> Router {
    frontend_management_router_with_readers(
        registry,
        Arc::new(FrontendServingLifecycle::new()),
        island_reader,
        Some(convergence_reader),
        crate::native::report_server::lifecycle_convergence_debug_enabled(),
    )
}

/// Builds the management surface from late-bindable, read-only capabilities.
/// No route in this router can mutate the serving lifecycle.
pub(crate) fn frontend_management_router_with_readers(
    registry: Arc<FrontendMetricsRegistry>,
    serving_reader: Arc<dyn FrontendServingSnapshotReader>,
    island_reader: Arc<dyn BackendIslandSnapshotReader>,
    convergence_reader: Option<Arc<dyn QueryLifecycleConvergenceReader>>,
    debug_enabled: bool,
) -> Router {
    let state = FrontendManagementState {
        registry,
        serving_reader,
        island_reader,
    };
    let router = Router::new()
        .route("/metrics", get(handle_management_metrics))
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/island-readyz", get(island_readyz))
        .route("/v1/frontend/state", get(frontend_state));
    let router = if debug_enabled && convergence_reader.is_some() {
        let convergence_reader = convergence_reader.expect("checked above");
        router.route(
            crate::native::report_server::LIFECYCLE_CONVERGENCE_DEBUG_PATH,
            get(move || latest_lifecycle_convergence_snapshot(Arc::clone(&convergence_reader))),
        )
    } else {
        router
    };
    let router = if crate::preparation_diagnostics::enabled() {
        router
            .route(
                "/v1/diagnostics/preparation/arm",
                post(arm_preparation_diagnostics),
            )
            .route(
                "/v1/diagnostics/preparation/drain",
                post(drain_preparation_diagnostics),
            )
    } else {
        router
    };
    router.with_state(state)
}

async fn arm_preparation_diagnostics(
    headers: HeaderMap,
    Json(request): Json<crate::preparation_diagnostics::ControlRequest>,
) -> axum::response::Response {
    if !preparation_diagnostic_authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match crate::preparation_diagnostics::arm(request.run_token) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => (StatusCode::CONFLICT, error).into_response(),
    }
}

async fn drain_preparation_diagnostics(
    headers: HeaderMap,
    Json(request): Json<crate::preparation_diagnostics::ControlRequest>,
) -> axum::response::Response {
    if !preparation_diagnostic_authorized(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match crate::preparation_diagnostics::drain(&request.run_token) {
        Ok(response) => Json(response).into_response(),
        Err(error) => (StatusCode::CONFLICT, error).into_response(),
    }
}

fn preparation_diagnostic_authorized(headers: &HeaderMap) -> bool {
    crate::preparation_diagnostics::authorize(
        headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok()),
    )
}

async fn handle_management_metrics(
    State(state): State<FrontendManagementState>,
    Query(params): Query<HashMap<String, String>>,
) -> axum::response::Response {
    let snapshot = management_snapshot(&state);
    super::publish_frontend_island_metrics(
        snapshot.island.ready,
        snapshot.island.compatible_eligible_backend_count,
    );
    if params
        .get("type")
        .is_some_and(|value| value.eq_ignore_ascii_case("json"))
    {
        return match render_metrics_json(state.registry.as_ref()) {
            Ok(body) => ([(header::CONTENT_TYPE, "application/json")], body).into_response(),
            Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
        };
    }
    match render_metrics(state.registry.as_ref()) {
        Ok(body) => ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
    }
}

async fn livez(State(state): State<FrontendManagementState>) -> axum::response::Response {
    if state
        .serving_reader
        .frontend_serving_snapshot()
        .serving_state
        == FrontendServingState::Stopping
    {
        StatusCode::SERVICE_UNAVAILABLE.into_response()
    } else {
        StatusCode::OK.into_response()
    }
}

async fn readyz(State(state): State<FrontendManagementState>) -> axum::response::Response {
    if state
        .serving_reader
        .frontend_serving_snapshot()
        .base_ready()
    {
        StatusCode::OK.into_response()
    } else {
        StatusCode::SERVICE_UNAVAILABLE.into_response()
    }
}

async fn frontend_state(
    State(state): State<FrontendManagementState>,
) -> Json<FrontendManagementSnapshot> {
    Json(management_snapshot(&state))
}

async fn island_readyz(State(state): State<FrontendManagementState>) -> axum::response::Response {
    let snapshot = management_snapshot(&state);
    super::publish_frontend_island_metrics(
        snapshot.island.ready,
        snapshot.island.compatible_eligible_backend_count,
    );
    if snapshot.island.ready {
        StatusCode::OK.into_response()
    } else {
        StatusCode::SERVICE_UNAVAILABLE.into_response()
    }
}

fn management_snapshot(state: &FrontendManagementState) -> FrontendManagementSnapshot {
    FrontendManagementSnapshot::compose(
        state.serving_reader.frontend_serving_snapshot(),
        state.island_reader.backend_island_snapshot(),
    )
}

async fn latest_lifecycle_convergence_snapshot(
    reader: Arc<dyn QueryLifecycleConvergenceReader>,
) -> axum::response::Response {
    let Some(snapshot) = reader.latest_convergence_snapshot() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    Json(crate::native::report_server::lifecycle_convergence_debug_json(snapshot)).into_response()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use tower::ServiceExt;

    use super::{FrontendMetricsRegistry, frontend_management_router_with_readers};
    use crate::coordinator::{QueryLifecycleConvergenceReader, QueryLifecycleConvergenceSnapshot};
    use crate::topology::{BackendIslandSnapshot, BackendIslandSnapshotReader};
    use crate::workload_lifecycle::{
        FrontendCatalogCounts, FrontendCatalogSnapshotIdentity, FrontendCatalogSourceMode,
        FrontendServingLifecycle,
    };

    struct EmptyConvergenceReader;

    impl QueryLifecycleConvergenceReader for EmptyConvergenceReader {
        fn latest_convergence_snapshot(&self) -> Option<QueryLifecycleConvergenceSnapshot> {
            None
        }
    }

    struct TestIslandReader {
        snapshot: RwLock<BackendIslandSnapshot>,
    }

    impl TestIslandReader {
        fn new(snapshot: BackendIslandSnapshot) -> Self {
            Self {
                snapshot: RwLock::new(snapshot),
            }
        }

        fn set(&self, snapshot: BackendIslandSnapshot) {
            *self.snapshot.write().expect("test island reader lock") = snapshot;
        }
    }

    impl BackendIslandSnapshotReader for TestIslandReader {
        fn backend_island_snapshot(&self) -> BackendIslandSnapshot {
            *self.snapshot.read().expect("test island reader lock")
        }
    }

    fn island_snapshot(
        revision: u64,
        compatible_eligible: usize,
        other_island: usize,
        unknown_or_invalid: usize,
    ) -> BackendIslandSnapshot {
        BackendIslandSnapshot::new(
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            revision,
            compatible_eligible,
            other_island,
            unknown_or_invalid,
        )
    }

    fn router(
        debug_enabled: bool,
        lifecycle: Arc<FrontendServingLifecycle>,
        island: Arc<TestIslandReader>,
    ) -> axum::Router {
        frontend_management_router_with_readers(
            FrontendMetricsRegistry::new().expect("create frontend metrics registry"),
            lifecycle,
            island,
            Some(Arc::new(EmptyConvergenceReader)),
            debug_enabled,
        )
    }

    #[tokio::test]
    async fn management_router_serves_metrics_and_gates_lifecycle_debug_route() {
        let lifecycle = Arc::new(FrontendServingLifecycle::new());
        let island = Arc::new(TestIslandReader::new(island_snapshot(0, 0, 0, 0)));
        let metrics = router(false, Arc::clone(&lifecycle), Arc::clone(&island))
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .expect("metrics request"),
            )
            .await
            .expect("metrics response");
        assert_eq!(metrics.status(), StatusCode::OK);

        let debug_off = router(false, Arc::clone(&lifecycle), Arc::clone(&island))
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(crate::native::report_server::LIFECYCLE_CONVERGENCE_DEBUG_PATH)
                    .body(Body::empty())
                    .expect("debug-off request"),
            )
            .await
            .expect("debug-off response");
        assert_eq!(debug_off.status(), StatusCode::NOT_FOUND);

        let debug_on = router(true, lifecycle, island)
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(crate::native::report_server::LIFECYCLE_CONVERGENCE_DEBUG_PATH)
                    .body(Body::empty())
                    .expect("debug-on request"),
            )
            .await
            .expect("debug-on response");
        assert_eq!(debug_on.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn management_routes_report_liveness_readiness_and_sanitized_state() {
        let lifecycle = Arc::new(FrontendServingLifecycle::new());
        let island = Arc::new(TestIslandReader::new(island_snapshot(3, 0, 1, 2)));
        lifecycle.publish_catalog_bootstrap(
            FrontendCatalogSourceMode::StaticFile,
            true,
            Some(
                FrontendCatalogSnapshotIdentity::try_new(2, "0123456789abcdef")
                    .expect("snapshot identity"),
            ),
            FrontendCatalogCounts {
                desired: 2,
                ready: 1,
                unavailable: 1,
            },
        );
        let live = router(false, Arc::clone(&lifecycle), Arc::clone(&island))
            .oneshot(
                Request::builder()
                    .uri("/livez")
                    .body(Body::empty())
                    .expect("live request"),
            )
            .await
            .expect("live response");
        assert_eq!(live.status(), StatusCode::OK);
        let not_ready = router(false, Arc::clone(&lifecycle), Arc::clone(&island))
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .expect("ready request"),
            )
            .await
            .expect("ready response");
        assert_eq!(not_ready.status(), StatusCode::SERVICE_UNAVAILABLE);

        lifecycle.mark_ready().expect("mark ready");
        let state = router(false, Arc::clone(&lifecycle), Arc::clone(&island))
            .oneshot(
                Request::builder()
                    .uri("/v1/frontend/state")
                    .body(Body::empty())
                    .expect("state request"),
            )
            .await
            .expect("state response");
        assert_eq!(state.status(), StatusCode::OK);
        let body = axum::body::to_bytes(state.into_body(), usize::MAX)
            .await
            .expect("state body");
        let json: serde_json::Value = serde_json::from_slice(&body).expect("state json");
        assert_eq!(json["schema_version"], 2);
        assert_eq!(json["catalog"]["counts"]["desired"], 2);
        assert!(json.get("properties").is_none());
        assert!(json.to_string().contains("0123456789abcdef"));
        assert_eq!(json["island"]["native_compatibility_id"], "71".repeat(32));
        assert_eq!(json["island"]["topology_revision"], 3);
        assert_eq!(json["island"]["compatible_eligible_backend_count"], 0);
        assert_eq!(json["island"]["other_island_backend_count"], 1);
        assert_eq!(json["island"]["unknown_or_invalid_backend_count"], 2);
        assert_eq!(json["island"]["ready"], false);

        let ready = router(false, lifecycle, island)
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .expect("ready request"),
            )
            .await
            .expect("ready response");
        assert_eq!(ready.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn island_readyz_requires_base_readiness_and_a_compatible_eligible_backend() {
        let lifecycle = Arc::new(FrontendServingLifecycle::new());
        let island = Arc::new(TestIslandReader::new(island_snapshot(0, 0, 0, 0)));
        let base_false = router(false, Arc::clone(&lifecycle), Arc::clone(&island))
            .oneshot(
                Request::builder()
                    .uri("/island-readyz")
                    .body(Body::empty())
                    .expect("island ready request"),
            )
            .await
            .expect("island ready response");
        assert_eq!(base_false.status(), StatusCode::SERVICE_UNAVAILABLE);

        lifecycle.publish_catalog_bootstrap(
            FrontendCatalogSourceMode::StaticFile,
            true,
            None,
            FrontendCatalogCounts::default(),
        );
        lifecycle.mark_ready().expect("mark ready");
        let no_backend = router(false, Arc::clone(&lifecycle), Arc::clone(&island))
            .oneshot(
                Request::builder()
                    .uri("/island-readyz")
                    .body(Body::empty())
                    .expect("island ready request"),
            )
            .await
            .expect("island ready response");
        assert_eq!(no_backend.status(), StatusCode::SERVICE_UNAVAILABLE);

        island.set(island_snapshot(1, 1, 0, 0));
        let compatible_backend = router(false, Arc::clone(&lifecycle), Arc::clone(&island))
            .oneshot(
                Request::builder()
                    .uri("/island-readyz")
                    .body(Body::empty())
                    .expect("island ready request"),
            )
            .await
            .expect("island ready response");
        assert_eq!(compatible_backend.status(), StatusCode::OK);

        island.set(island_snapshot(2, 0, 1, 0));
        let other_island_only = router(false, lifecycle, island)
            .oneshot(
                Request::builder()
                    .uri("/island-readyz")
                    .body(Body::empty())
                    .expect("island ready request"),
            )
            .await
            .expect("island ready response");
        assert_eq!(other_island_only.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
