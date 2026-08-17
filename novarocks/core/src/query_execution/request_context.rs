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

//! Immutable request state captured exactly once at statement admission.
// Design: ADR-0011 (docs/adr/ADR-0011-immutable-request-execution-context.md)

use std::time::Instant;

use crate::query_execution::backend::BackendTopologySnapshot;
use crate::query_execution::cancellation::QueryCancellationView;
pub use novarocks_sql::compiler::SessionOptimizerSettings;
use novarocks_types::ClusterRole;

/// All inputs accepted at the frontend statement-admission boundary.
///
/// The contained values are moved into an immutable [`RequestContext`]; the
/// individual projection constructors intentionally remain private.
pub struct RequestAdmission {
    current_catalog: Option<String>,
    current_database: String,
    role: ClusterRole,
    topology: BackendTopologySnapshot,
    deadline: Option<Instant>,
    cancellation: QueryCancellationView,
    optimizer_settings: SessionOptimizerSettings,
}

impl RequestAdmission {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        current_catalog: Option<String>,
        current_database: String,
        role: ClusterRole,
        topology: BackendTopologySnapshot,
        deadline: Option<Instant>,
        cancellation: QueryCancellationView,
        optimizer_settings: SessionOptimizerSettings,
    ) -> Self {
        Self {
            current_catalog,
            current_database,
            role,
            topology,
            deadline,
            cancellation,
            optimizer_settings,
        }
    }
}

/// Session-derived inputs frozen at the request boundary.
#[derive(Clone, Debug)]
pub struct RequestSessionContext {
    current_catalog: Option<String>,
    current_database: String,
    optimizer_settings: SessionOptimizerSettings,
}

impl RequestSessionContext {
    pub fn new(
        current_catalog: Option<String>,
        current_database: String,
        optimizer_settings: SessionOptimizerSettings,
    ) -> Self {
        Self {
            current_catalog,
            current_database,
            optimizer_settings,
        }
    }

    pub fn current_catalog(&self) -> Option<&str> {
        self.current_catalog.as_deref()
    }

    pub fn current_database(&self) -> &str {
        &self.current_database
    }

    pub fn optimizer_settings(&self) -> &SessionOptimizerSettings {
        &self.optimizer_settings
    }
}

/// Execution inputs which must remain identical from planning through native
/// coordinator submission.
///
/// CLS-R2 boundary: request admission — deciding these inputs once per
/// statement — is frontend authority and moves there. The immutable value
/// itself stays with the aggregate package because `mv` and `connector` still
/// receive it as a parameter. Those consumers leave with CLS-R3 and CLS-R5.
#[derive(Clone)]
pub struct QueryExecutionContext {
    role: ClusterRole,
    topology: BackendTopologySnapshot,
    deadline: Option<Instant>,
    cancellation: QueryCancellationView,
    optimizer_settings: SessionOptimizerSettings,
}

impl QueryExecutionContext {
    pub fn new(
        role: ClusterRole,
        topology: BackendTopologySnapshot,
        deadline: Option<Instant>,
        cancellation: QueryCancellationView,
        optimizer_settings: SessionOptimizerSettings,
    ) -> Self {
        Self {
            role,
            topology,
            deadline,
            cancellation,
            optimizer_settings,
        }
    }

    pub const fn role(&self) -> ClusterRole {
        self.role
    }

    pub fn topology(&self) -> &BackendTopologySnapshot {
        &self.topology
    }

    pub const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub fn cancellation(&self) -> &QueryCancellationView {
        &self.cancellation
    }

    /// Settings frozen with the request so DML and coordinator-adjacent plan
    /// construction never consult process- or thread-local session state.
    pub fn optimizer_settings(&self) -> &SessionOptimizerSettings {
        &self.optimizer_settings
    }
}

/// Complete immutable statement context.  The frontend application admits a
/// statement once, then consumers receive only narrow projections.
#[derive(Clone)]
pub struct RequestContext {
    session: RequestSessionContext,
    execution: QueryExecutionContext,
}

impl RequestContext {
    pub fn new(session: RequestSessionContext, execution: QueryExecutionContext) -> Self {
        Self { session, execution }
    }

    /// Freeze one statement's context.
    ///
    /// The cost budget arrives inside `optimizer_settings`: the frontend query
    /// service and the MV worker each carry the value resolved from `[runtime]`
    /// at composition, so admission never reads a process-global configuration.
    /// A settings value of `None` means "no budget", which leaves optimizer
    /// costing on its profile default.
    pub fn admit(admission: RequestAdmission) -> Self {
        let settings = admission.optimizer_settings;
        Self::new(
            RequestSessionContext::new(
                admission.current_catalog,
                admission.current_database,
                settings.clone(),
            ),
            QueryExecutionContext::new(
                admission.role,
                admission.topology,
                admission.deadline,
                admission.cancellation,
                settings,
            ),
        )
    }

    pub fn session(&self) -> &RequestSessionContext {
        &self.session
    }

    pub fn execution(&self) -> &QueryExecutionContext {
        &self.execution
    }

    pub(crate) fn preparation(&self) -> QueryPreparationContext<'_> {
        QueryPreparationContext {
            session: &self.session,
            execution: &self.execution,
        }
    }
}

/// Borrowed projection used by SQL preparation and optimizer layers.
pub(crate) struct QueryPreparationContext<'a> {
    session: &'a RequestSessionContext,
    execution: &'a QueryExecutionContext,
}

impl<'a> QueryPreparationContext<'a> {
    pub(crate) fn session(&self) -> &'a RequestSessionContext {
        self.session
    }

    pub(crate) fn execution(&self) -> &'a QueryExecutionContext {
        self.execution
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::*;
    use crate::query_execution::backend::LiveBackendTarget;
    use crate::query_execution::cancellation::QueryCancellationSource;

    #[test]
    fn projections_share_one_cancellation_and_topology_identity() {
        let cancellation = QueryCancellationSource::new();
        let snapshot = BackendTopologySnapshot::try_new(
            9,
            vec![LiveBackendTarget::new(
                7,
                SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9030),
                2,
            )],
        )
        .expect("valid snapshot");
        let context = RequestContext::new(
            RequestSessionContext::new(
                Some("iceberg".to_string()),
                "db1".to_string(),
                SessionOptimizerSettings::default(),
            ),
            QueryExecutionContext::new(
                ClusterRole::Fe,
                snapshot,
                None,
                cancellation.view(),
                SessionOptimizerSettings::default(),
            ),
        );

        assert_eq!(context.execution().topology().revision(), 9);
        assert_eq!(context.session().current_catalog(), Some("iceberg"));
        assert!(!context.execution().cancellation().is_cancelled());
        cancellation.request(
            crate::query_execution::cancellation::QueryCancellationReason::ClientDisconnected,
        );
        assert!(context.execution().cancellation().is_cancelled());
    }

    #[test]
    fn admission_copies_session_settings_and_preserves_deadline() {
        let deadline = Instant::now();
        let mut admitted_settings = SessionOptimizerSettings {
            enable_eliminate_agg: true,
            cbo_broadcast_backend_count: Some(3.0),
            ..SessionOptimizerSettings::default()
        };
        let context = RequestContext::new(
            RequestSessionContext::new(None, "db1".to_string(), admitted_settings.clone()),
            QueryExecutionContext::new(
                ClusterRole::AllInOne,
                BackendTopologySnapshot::empty(4),
                Some(deadline),
                QueryCancellationSource::new().view(),
                admitted_settings.clone(),
            ),
        );

        admitted_settings.enable_eliminate_agg = false;
        admitted_settings.cbo_broadcast_backend_count = Some(99.0);

        assert!(context.session().optimizer_settings().enable_eliminate_agg);
        assert_eq!(
            context
                .execution()
                .optimizer_settings()
                .cbo_broadcast_backend_count,
            Some(3.0)
        );
        assert_eq!(context.execution().deadline(), Some(deadline));
        assert!(context.execution().topology().targets().is_empty());
    }

    #[test]
    fn admission_carries_the_optimizer_query_memory_budget_verbatim() {
        let context = RequestContext::admit(RequestAdmission::new(
            None,
            "db1".to_string(),
            ClusterRole::Fe,
            BackendTopologySnapshot::empty(7),
            None,
            QueryCancellationSource::new().view(),
            SessionOptimizerSettings {
                optimizer_query_mem_limit_bytes: Some(512.0 * 1024.0 * 1024.0),
                ..SessionOptimizerSettings::default()
            },
        ));

        assert_eq!(
            context
                .execution()
                .optimizer_settings()
                .optimizer_query_mem_limit_bytes,
            Some(512.0 * 1024.0 * 1024.0)
        );
    }

    #[test]
    fn admission_without_a_budget_leaves_costing_on_its_profile_default() {
        let context = RequestContext::admit(RequestAdmission::new(
            None,
            "db1".to_string(),
            ClusterRole::Fe,
            BackendTopologySnapshot::empty(7),
            None,
            QueryCancellationSource::new().view(),
            SessionOptimizerSettings::default(),
        ));

        assert_eq!(
            context
                .execution()
                .optimizer_settings()
                .optimizer_query_mem_limit_bytes,
            None
        );
    }
}
