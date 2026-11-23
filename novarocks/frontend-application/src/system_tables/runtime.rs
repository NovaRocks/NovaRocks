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

//! Statement projections of the FE topology and query-control owners.
use crate::topology::ClusterBackendService;
use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
use novarocks_query_application::query_control::{
    ClientQueryColumns, ClientQueryObservation, ClientQueryTime,
};
use novarocks_query_application::serving_admission::FrontendServingAdmission;
use novarocks_query_application::session_control::QueryControlService;
use novarocks_query_application::system_tables::{
    QUERY_OBSERVATION_TIME, SystemTableError, SystemTableRequest,
};
use novarocks_spi::connector::ConnectorOperationControl;
use novarocks_sql::system_table::SystemTableKeyValues;
use novarocks_type_contract::{
    CompileControlError, TemporalCarrier, TemporalMicros, TemporalValue, TimeZoneKey,
};
use novarocks_types::{BackendProcessId, FrontendProcessId};
use novarocks_workload_control::{ResultPayloadBytes, WorkError};
use std::{
    fmt,
    net::SocketAddr,
    sync::{Arc, OnceLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use super::construction::{array_layout, control_error};
use super::provider::Batches;
use super::source::SystemValueRef as V;

pub(crate) struct FrontendRuntimeMetadata {
    frontend: FrontendProcessId,
    version: String,
    http: OnceLock<SocketAddr>,
    serving: FrontendServingAdmission,
    backends: Arc<ClusterBackendService>,
    queries: QueryControlService,
}

impl FrontendRuntimeMetadata {
    pub(crate) fn new(
        frontend: FrontendProcessId,
        version: String,
        serving: FrontendServingAdmission,
        backends: Arc<ClusterBackendService>,
        queries: QueryControlService,
    ) -> Self {
        Self {
            frontend,
            version,
            http: OnceLock::new(),
            serving,
            backends,
            queries,
        }
    }

    /// The FE publishes the actual bound address before announcing readiness.
    pub(crate) fn publish_http(&self, address: SocketAddr) -> Result<(), &'static str> {
        self.http
            .set(address)
            .map_err(|_| "Trino HTTP address already published")
    }

    pub(super) async fn nodes(
        &self,
        batches: &mut Batches<'_>,
        request: &SystemTableRequest<'_>,
        capacity: &QueryResultCapacityBinding,
    ) -> Result<(), SystemTableError> {
        let mut backends = BackendSnapshot::new(capacity)?;
        let filter = request.key_constraint.values_for(0);
        let serving = self.serving.snapshot();
        self.backends.visit_backend_nodes(|node| {
            request.context.original_operation_control().check_active()?;
            if matches!(filter, Some(SystemTableKeyValues::Values(ids)) if !ids.iter().any(|id| id.parse() == Ok(node.process_id))) {
                return Ok(());
            }
            backends.push(
                node.process_id,
                request.requires(2).then_some(node.build_identity),
                request.requires(1).then_some(node.http_uri).flatten(),
                node.state.sql_name(),
            )
        }).map_err(SystemTableError::Internal)??;
        if !matches!(filter, Some(SystemTableKeyValues::Values(ids)) if !ids.iter().any(|id| id.parse() == Ok(self.frontend)))
        {
            let http = self.http.get().copied().map(HttpUri);
            batches
                .push_values(&[
                    V::FormattedText(&self.frontend),
                    http.as_ref().map_or(V::Null, |uri| V::FormattedText(uri)),
                    V::Utf8(&self.version),
                    V::Boolean(true),
                    V::Utf8(serving.state.node_state_name()),
                ])
                .await?;
        }
        for node in &backends.rows {
            batches
                .push_values(&[
                    V::FormattedText(&node.id),
                    node.http_uri.as_deref().into(),
                    node.version.as_deref().into(),
                    V::Boolean(false),
                    V::Utf8(node.state),
                ])
                .await?;
        }
        Ok(())
    }

    pub(super) async fn queries(
        &self,
        batches: &mut Batches<'_>,
        request: &SystemTableRequest<'_>,
        principal: &str,
    ) -> Result<(), SystemTableError> {
        let now = ClientQueryTime::now();
        let columns = ClientQueryColumns {
            user: request.requires(2),
            source: request.requires(3),
            query: request.requires(4),
        };
        if let Some(SystemTableKeyValues::Values(ids)) = request.key_constraint.values_for(0) {
            for id in ids {
                request
                    .context
                    .original_operation_control()
                    .check_active()?;
                if let Some(row) = self
                    .queries
                    .observe_client_query(principal, id, columns, now)
                {
                    emit_query(batches, request, &row).await?;
                }
            }
        } else {
            let mut previous = None;
            loop {
                request
                    .context
                    .original_operation_control()
                    .check_active()?;
                let Some(row) = self.queries.next_client_query(
                    principal,
                    previous.as_ref().map(
                        |value: &novarocks_query_application::query_control::ClientQueryText| {
                            value.as_str()
                        },
                    ),
                    columns,
                    now,
                ) else {
                    break;
                };
                emit_query(batches, request, &row).await?;
                previous = Some(row.query_id);
            }
        }
        Ok(())
    }
}

struct HttpUri(SocketAddr);
impl fmt::Display for HttpUri {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(output, "http://{}", self.0)
    }
}

struct BackendNode {
    id: BackendProcessId,
    version: Option<String>,
    http_uri: Option<String>,
    state: &'static str,
}

struct BackendSnapshot {
    rows: Vec<BackendNode>,
    held: ResultPayloadBytes,
}

impl BackendSnapshot {
    fn new(capacity: &QueryResultCapacityBinding) -> Result<Self, SystemTableError> {
        let held = capacity
            .try_window_alias()
            .map_err(work_error)?
            .reserve_payload_bytes(0)
            .map_err(work_error)?;
        Ok(Self {
            rows: Vec::new(),
            held,
        })
    }

    fn push(
        &mut self,
        id: BackendProcessId,
        version: Option<&str>,
        http_uri: Option<&str>,
        state: &'static str,
    ) -> Result<(), SystemTableError> {
        let capacity = self.rows.capacity();
        if self.rows.len() == capacity {
            let next = capacity
                .max(1)
                .checked_mul(2)
                .ok_or_else(|| work_error(WorkError::ArithmeticOverflow))?;
            let planned = array_layout::<BackendNode>(next)?.size();
            self.held.try_grow(planned as u64).map_err(work_error)?;
            self.rows
                .try_reserve_exact(next - self.rows.len())
                .map_err(|_| control_error(CompileControlError::ResourceExhausted))?;
            let previous = array_layout::<BackendNode>(capacity)?.size();
            self.held
                .release_destroyed(previous as u64)
                .map_err(work_error)?;
        }
        let version = self.copy_text(version)?;
        let http_uri = self.copy_text(http_uri)?;
        self.rows.push(BackendNode {
            id,
            version,
            http_uri,
            state,
        });
        Ok(())
    }

    fn copy_text(&mut self, text: Option<&str>) -> Result<Option<String>, SystemTableError> {
        let Some(text) = text else {
            return Ok(None);
        };
        self.held.try_grow(text.len() as u64).map_err(work_error)?;
        let mut copy = String::new();
        copy.try_reserve_exact(text.len())
            .map_err(|_| control_error(CompileControlError::ResourceExhausted))?;
        copy.push_str(text);
        Ok(Some(copy))
    }
}

fn work_error(error: WorkError) -> SystemTableError {
    control_error(CompileControlError::Source(
        novarocks_failure_contract::SharedFailureCause::new(error),
    ))
}

fn millis(duration: Duration) -> Result<V<'static>, SystemTableError> {
    i64::try_from(duration.as_millis())
        .map(V::Int64)
        .map_err(|_| SystemTableError::Internal("query observation duration exceeds bigint".into()))
}

fn time(value: SystemTime) -> Result<V<'static>, SystemTableError> {
    let micros = match value.duration_since(UNIX_EPOCH) {
        Ok(after) => i128::try_from(after.as_millis()).ok(),
        Err(before) => {
            let elapsed = before.duration();
            let millis = i128::try_from(elapsed.as_millis()).ok();
            millis.map(|n| -n - i128::from(elapsed.subsec_nanos() % 1_000_000 != 0))
        }
    }
    .and_then(|n| n.checked_mul(1_000))
    .and_then(|n| i64::try_from(n).ok())
    .ok_or_else(|| {
        SystemTableError::Internal("query observation timestamp exceeds carrier range".into())
    })?;
    let instant = TemporalValue::TimestampWithTimeZone {
        instant: TemporalMicros::whole_micros(micros),
        zone: TimeZoneKey::UTC,
    };
    let TemporalCarrier::ZonedTimestamp(bytes) = instant
        .to_carrier(QUERY_OBSERVATION_TIME)
        .map_err(|error| SystemTableError::Internal(error.to_string()))?
    else {
        unreachable!("typed query observation timestamp")
    };
    Ok(V::ZonedTimestamp(bytes))
}

fn selected<T>(
    request: &SystemTableRequest<'_>,
    ordinal: usize,
    value: Option<T>,
    project: impl FnOnce(T) -> Result<V<'static>, SystemTableError>,
) -> Result<V<'static>, SystemTableError> {
    if request.requires(ordinal) {
        value
            .map(project)
            .transpose()
            .map(|value| value.unwrap_or(V::Null))
    } else {
        Ok(V::Null)
    }
}

async fn emit_query(
    batches: &mut Batches<'_>,
    request: &SystemTableRequest<'_>,
    row: &ClientQueryObservation,
) -> Result<(), SystemTableError> {
    batches
        .push_values(&[
            V::Utf8(row.query_id.as_str()),
            V::Utf8(row.state.as_str()),
            row.user.as_ref().map(|value| value.as_str()).into(),
            row.source.as_ref().map(|value| value.as_str()).into(),
            row.query.as_ref().map(|value| value.as_str()).into(),
            V::Null,
            selected(request, 6, Some(row.queued), millis)?,
            selected(request, 7, row.analysis, millis)?,
            selected(request, 8, row.planning, millis)?,
            selected(request, 9, Some(row.created), time)?,
            selected(request, 10, row.started, time)?,
            selected(request, 11, row.last_heartbeat, time)?,
            selected(request, 12, row.end, time)?,
            row.error.map(|error| error.error_type).into(),
            row.error.map(|error| error.name).into(),
        ])
        .await
}
