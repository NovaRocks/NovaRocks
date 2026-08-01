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
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use moka::sync::Cache;

use crate::common::config;
use crate::connector::starrocks::ports::{
    ConnectorWireError, ConnectorWireErrorKind, TableSchemaProvider, TableSchemaRequest,
    TableSchemaRequestSource,
};
use crate::connector::starrocks::schema::LakeScanTableSchema;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct TableSchemaCacheKey {
    db_id: i64,
    table_id: i64,
    schema_id: i64,
}

pub(crate) struct TableSchemaService {
    cache: Cache<TableSchemaCacheKey, LakeScanTableSchema>,
    flights: SingleFlightGroup,
    max_retries: usize,
}

impl TableSchemaService {
    fn new() -> Self {
        Self {
            cache: Cache::builder()
                .max_capacity(config::table_schema_service_cache_capacity())
                .build(),
            flights: SingleFlightGroup::default(),
            max_retries: config::table_schema_service_max_retries(),
        }
    }

    pub(crate) fn shared() -> &'static Self {
        static INSTANCE: OnceLock<TableSchemaService> = OnceLock::new();
        INSTANCE.get_or_init(TableSchemaService::new)
    }

    pub(crate) fn fetch(
        &self,
        request: TableSchemaRequest,
        provider: &dyn TableSchemaProvider,
    ) -> Result<LakeScanTableSchema, String> {
        self.fetch_with_loader(request, |request| provider.fetch_table_schema(request))
    }

    fn fetch_with_loader<F>(
        &self,
        request: TableSchemaRequest,
        mut load_remote: F,
    ) -> Result<LakeScanTableSchema, String>
    where
        F: FnMut(&TableSchemaRequest) -> Result<LakeScanTableSchema, ConnectorWireError>,
    {
        validate_request(&request)?;
        let cache_key = TableSchemaCacheKey {
            db_id: request.db_id,
            table_id: request.table_id,
            schema_id: request.schema_id,
        };

        if let Some(schema) = self.cache.get(&cache_key) {
            tracing::debug!(
                target: "novarocks::schema",
                event = "cache_hit",
                fe_addr = %format_addr(&request.endpoint),
                db_id = request.db_id,
                table_id = request.table_id,
                schema_id = request.schema_id,
                source = %source_label(request.source),
                "Resolved table schema from local cache"
            );
            return Ok(schema);
        }

        let mut last_error = None;
        for attempt in 1..=self.max_retries {
            let isolated_retry = attempt == self.max_retries && self.max_retries > 1;
            let flight_key = build_flight_key(&request, isolated_retry);
            if isolated_retry {
                tracing::debug!(
                    target: "novarocks::schema",
                    event = "isolated_retry",
                    fe_addr = %format_addr(&request.endpoint),
                    db_id = request.db_id,
                    table_id = request.table_id,
                    schema_id = request.schema_id,
                    source = %source_label(request.source),
                    attempt,
                    "Running isolated table schema retry"
                );
            }
            let (result, shared) = self.flights.execute(flight_key, || load_remote(&request));
            if shared {
                tracing::debug!(
                    target: "novarocks::schema",
                    event = "singleflight_shared",
                    fe_addr = %format_addr(&request.endpoint),
                    db_id = request.db_id,
                    table_id = request.table_id,
                    schema_id = request.schema_id,
                    source = %source_label(request.source),
                    attempt,
                    "Shared in-flight FE getTableSchema request"
                );
            }

            match result {
                Ok(schema) => {
                    self.cache.insert(cache_key, schema.clone());
                    return Ok(schema);
                }
                Err(err)
                    if matches!(
                        err.kind(),
                        ConnectorWireErrorKind::Transport | ConnectorWireErrorKind::NotFound
                    ) =>
                {
                    return Err(err.to_string());
                }
                Err(err) => {
                    last_error = Some(err.to_string());
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            "FE getTableSchema failed without a terminal error message".to_string()
        }))
    }
}

pub(crate) fn fetch_table_schema_for_lake_scan(
    provider: Option<&dyn TableSchemaProvider>,
    endpoint: Option<&crate::runtime::endpoint::RuntimeEndpoint>,
    db_id: i64,
    table_id: i64,
    schema_id: i64,
    tablet_id: Option<i64>,
    query_id: Option<crate::common::types::UniqueId>,
) -> Result<LakeScanTableSchema, String> {
    let query_id = query_id.ok_or_else(|| {
        format!(
            "missing query_id for FE getTableSchema scan request: db_id={} table_id={} schema_id={}",
            db_id, table_id, schema_id
        )
    })?;
    let provider = provider.ok_or_else(|| {
        "StarRocks table schema capability is unavailable for this fragment".to_string()
    })?;
    let endpoint = endpoint
        .ok_or_else(|| "missing FE address for getTableSchema (coord is absent)".to_string())?;
    TableSchemaService::shared().fetch(
        TableSchemaRequest {
            endpoint: crate::connector::starrocks::ports::FrontendEndpoint {
                host: endpoint.host().to_string(),
                port: endpoint.port(),
            },
            db_id,
            table_id,
            schema_id,
            source: TableSchemaRequestSource::Scan,
            tablet_id,
            query_id: Some(query_id),
            txn_id: None,
        },
        provider,
    )
}

fn validate_request(request: &TableSchemaRequest) -> Result<(), String> {
    if request.schema_id <= 0 {
        return Err(format!(
            "invalid schema_id for FE getTableSchema: db_id={} table_id={} schema_id={}",
            request.db_id, request.table_id, request.schema_id
        ));
    }
    match request.source {
        TableSchemaRequestSource::Scan => {
            if request.query_id.is_none() {
                return Err(format!(
                    "missing query_id for FE getTableSchema scan request: db_id={} table_id={} schema_id={}",
                    request.db_id, request.table_id, request.schema_id
                ));
            }
        }
        TableSchemaRequestSource::Load => {
            if request.txn_id.is_none() {
                return Err(format!(
                    "missing txn_id for FE getTableSchema load request: db_id={} table_id={} schema_id={}",
                    request.db_id, request.table_id, request.schema_id
                ));
            }
        }
    }
    Ok(())
}

fn build_flight_key(request: &TableSchemaRequest, isolated_retry: bool) -> String {
    if !isolated_retry {
        return format!(
            "shared:{}:{}:{}:{}:{}",
            format_addr(&request.endpoint),
            request.db_id,
            request.table_id,
            request.schema_id,
            source_label(request.source)
        );
    }
    match request.source {
        TableSchemaRequestSource::Scan => {
            let query_id = request
                .query_id
                .as_ref()
                .map(|id| format!("{}:{}", id.high(), id.low()))
                .unwrap_or_else(|| "missing".to_string());
            format!(
                "isolated-scan:{}:{}:{}:{}:{}",
                format_addr(&request.endpoint),
                request.db_id,
                request.table_id,
                request.schema_id,
                query_id
            )
        }
        TableSchemaRequestSource::Load => format!(
            "isolated-load:{}:{}:{}:{}:{}",
            format_addr(&request.endpoint),
            request.db_id,
            request.table_id,
            request.schema_id,
            request.txn_id.unwrap_or_default()
        ),
    }
}

fn format_addr(endpoint: &crate::connector::starrocks::ports::FrontendEndpoint) -> String {
    format!("{}:{}", endpoint.host, endpoint.port)
}

fn source_label(source: TableSchemaRequestSource) -> &'static str {
    match source {
        TableSchemaRequestSource::Scan => "SCAN",
        TableSchemaRequestSource::Load => "LOAD",
    }
}

#[derive(Default)]
struct SingleFlightGroup {
    entries: Mutex<HashMap<String, Arc<SingleFlightEntry>>>,
}

struct SingleFlightEntry {
    state: Mutex<SingleFlightState>,
    cv: Condvar,
}

enum SingleFlightState {
    Running,
    Ready(Result<LakeScanTableSchema, ConnectorWireError>),
}

impl Default for SingleFlightEntry {
    fn default() -> Self {
        Self {
            state: Mutex::new(SingleFlightState::Running),
            cv: Condvar::new(),
        }
    }
}

impl SingleFlightGroup {
    fn execute<F>(
        &self,
        key: String,
        op: F,
    ) -> (Result<LakeScanTableSchema, ConnectorWireError>, bool)
    where
        F: FnOnce() -> Result<LakeScanTableSchema, ConnectorWireError>,
    {
        let (entry, shared) = {
            let mut guard = self.entries.lock().expect("table schema flights lock");
            if let Some(entry) = guard.get(&key) {
                (Arc::clone(entry), true)
            } else {
                let entry = Arc::new(SingleFlightEntry::default());
                guard.insert(key.clone(), Arc::clone(&entry));
                (entry, false)
            }
        };

        if shared {
            let mut state = entry.state.lock().expect("table schema flight state");
            loop {
                match &*state {
                    SingleFlightState::Ready(result) => return (result.clone(), true),
                    SingleFlightState::Running => {
                        state = entry.cv.wait(state).expect("table schema flight wait");
                    }
                }
            }
        }

        let result = op();
        {
            let mut state = entry.state.lock().expect("table schema flight state");
            *state = SingleFlightState::Ready(result.clone());
            entry.cv.notify_all();
        }
        let mut guard = self.entries.lock().expect("table schema flights lock");
        if guard
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, &entry))
        {
            guard.remove(&key);
        }
        (result, false)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::connector::starrocks::ports::FrontendEndpoint;

    struct RecordingProvider {
        calls: AtomicUsize,
    }

    impl TableSchemaProvider for RecordingProvider {
        fn fetch_table_schema(
            &self,
            _request: &TableSchemaRequest,
        ) -> Result<LakeScanTableSchema, ConnectorWireError> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            Ok(LakeScanTableSchema::default())
        }
    }

    fn scan_request() -> TableSchemaRequest {
        TableSchemaRequest {
            endpoint: FrontendEndpoint {
                host: "fe-1".to_string(),
                port: 9020,
            },
            db_id: 1,
            table_id: 2,
            schema_id: 3,
            source: TableSchemaRequestSource::Scan,
            tablet_id: Some(4),
            query_id: Some(novarocks_types::UniqueId::new(5, 6)),
            txn_id: None,
        }
    }

    #[test]
    fn cache_hit_does_not_call_table_schema_provider_again() {
        let service = TableSchemaService::new();
        let provider = RecordingProvider {
            calls: AtomicUsize::new(0),
        };

        service
            .fetch(scan_request(), &provider)
            .expect("first provider fetch");
        service
            .fetch(scan_request(), &provider)
            .expect("cached schema fetch");

        assert_eq!(provider.calls.load(Ordering::Acquire), 1);
    }

    #[test]
    fn unavailable_provider_error_is_terminal() {
        let service = TableSchemaService::new();
        let provider = |_request: &TableSchemaRequest| {
            Err(ConnectorWireError::new(
                ConnectorWireErrorKind::Unavailable,
                "StarRocks table schema capability is unavailable",
            ))
        };

        let error = service
            .fetch_with_loader(scan_request(), provider)
            .expect_err("unavailable provider must fail");
        assert_eq!(error, "StarRocks table schema capability is unavailable");
    }
}
