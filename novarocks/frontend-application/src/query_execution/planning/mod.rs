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

//! Sealed request-local inputs consumed by query execution after SQL
//! compilation. SQL receives opaque binding tokens only; these modules retain
//! the paired exact connector admission and never reacquire a newer binding.

pub mod statistics;
pub mod time_travel;
pub(crate) mod write_sink;

use std::sync::Arc;

use novarocks_query_application::cancellation::QueryCancellationView;
use novarocks_sql::compiler::{SqlAnalyzeRequest, SqlCancellationObservation};

#[derive(Clone)]
pub(crate) struct QueryCancellationObservation {
    view: QueryCancellationView,
}

impl QueryCancellationObservation {
    pub(crate) fn new(view: QueryCancellationView) -> Self {
        Self { view }
    }
}

impl SqlCancellationObservation for QueryCancellationObservation {
    fn is_cancelled(&self) -> bool {
        self.view.is_cancelled()
    }
}

pub fn sql_cancellation_observation(
    view: QueryCancellationView,
) -> Arc<dyn SqlCancellationObservation> {
    Arc::new(QueryCancellationObservation::new(view))
}

/// Borrow the existing admitted statement's control facts for a pure compile phase.
pub(crate) fn sql_compile_control_from_execution(
    execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
) -> novarocks_sql::compiler::SqlCompileControl {
    let control = novarocks_sql::compiler::SqlCompileControl::new(
        execution.deadline(),
        sql_cancellation_observation(execution.cancellation().clone()),
    );
    match execution.fold_dependency_observer() {
        Some(observer) => control.with_fold_dependency_observer(Arc::clone(observer)),
        None => control,
    }
}

struct ConnectorCancellationObservation {
    stop: novarocks_spi::connector::ConnectorStopView,
}

impl SqlCancellationObservation for ConnectorCancellationObservation {
    fn is_cancelled(&self) -> bool {
        self.stop.is_stopped()
    }
}

/// Project only the admitted request's existing deadline and stop authority.
/// SQL compilation receives no Connector admission or storage capability.
pub(crate) fn sql_compile_control_from_connector_request(
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> novarocks_sql::compiler::SqlCompileControl {
    novarocks_sql::compiler::SqlCompileControl::new(
        Some(context.deadline()),
        Arc::new(ConnectorCancellationObservation {
            stop: context.stop().clone(),
        }),
    )
}

#[allow(
    dead_code,
    reason = "Post-compile planning inputs remain explicit for target-gated native assembly callers."
)]
pub(crate) struct PostCompilePlanningContext<'a> {
    pub(crate) table_bindings:
        Arc<crate::catalog_application::query_bindings::QueryTableBindingStore>,
    pub(crate) connector_controls: &'a dyn novarocks_spi::connector::ConnectorControlResolver,
    pub(crate) connector_context: &'a novarocks_spi::connector::ConnectorRequestContext,
}

#[allow(
    dead_code,
    reason = "The aggregate planning input is retained for target-gated native assembly callers."
)]
pub(crate) struct QueryPlanningInputs<'a> {
    pub(crate) analyze_request: SqlAnalyzeRequest<'a>,
    pub(crate) post_compile: PostCompilePlanningContext<'a>,
}

#[cfg(test)]
mod tests {
    use super::sql_compile_control_from_connector_request;
    use novarocks_spi::connector::{ConnectorRequestContext, ConnectorStopOwner};
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
    use std::time::{Duration, Instant};

    #[test]
    fn connector_compile_projection_observes_the_same_stop_and_deadline() {
        let stop = ConnectorStopOwner::new();
        let deadline = Instant::now() + Duration::from_secs(60);
        let context = ConnectorRequestContext::try_new(deadline, stop.view(), 1, 1).unwrap();
        let control = sql_compile_control_from_connector_request(&context);
        assert_eq!(control.deadline(), Some(deadline));
        assert_eq!(control.checkpoint(CompilePhase::Validate, 0), Ok(()));
        drop(context);
        stop.request_stop();
        assert_eq!(
            control.checkpoint(CompilePhase::Validate, 256),
            Err(CompileControlError::Cancelled)
        );

        let expired = Instant::now() - Duration::from_secs(1);
        let context =
            ConnectorRequestContext::try_new(expired, ConnectorStopOwner::new().view(), 1, 1)
                .unwrap();
        let control = sql_compile_control_from_connector_request(&context);
        assert_eq!(control.deadline(), Some(expired));
        assert_eq!(
            control.checkpoint(CompilePhase::Validate, 0),
            Err(CompileControlError::DeadlineExceeded)
        );
    }
}

#[cfg(test)]
mod dependency_projection_tests {
    use super::sql_compile_control_from_execution;
    use novarocks_query_application::{
        admitted_query_context::QueryExecutionContext,
        api::BackendTopologySnapshot,
        cancellation::{QueryCancellationReason, QueryCancellationSource},
    };
    use novarocks_sql::compiler::{SessionOptimizerSettings, SqlFoldDependencyObserver};
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    struct Observer;
    impl SqlFoldDependencyObserver for Observer {}

    #[test]
    fn sql_dependency_execution_projector_keeps_original_deadline_and_cancellation() {
        let cancellation = QueryCancellationSource::new();
        let deadline = Instant::now() + Duration::from_secs(60);
        let context = QueryExecutionContext::new(
            novarocks_types::ClusterRole::Fe,
            BackendTopologySnapshot::empty(3),
            Some(deadline),
            cancellation.view(),
            SessionOptimizerSettings::default(),
            novarocks_sql::sql_mode::SqlSemanticSettings::default(),
        )
        .with_fold_dependency_observer(Arc::new(Observer));
        let control = sql_compile_control_from_execution(&context);
        assert_eq!(control.deadline(), Some(deadline));
        assert_eq!(
            control.checkpoint(CompilePhase::CarrierPreflight, 0),
            Ok(())
        );
        cancellation.request(QueryCancellationReason::ClientDisconnected);
        assert_eq!(
            control.checkpoint(CompilePhase::CarrierPreflight, 0),
            Err(CompileControlError::Cancelled)
        );
    }
}
