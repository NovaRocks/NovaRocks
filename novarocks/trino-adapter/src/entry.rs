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

use crate::{
    error_mapping::TrinoQueryError,
    paging::PagePipeline,
    request::ParsedStatementRequest,
    settlement::{Disposition, Settlement},
};
use axum::http::{HeaderMap, StatusCode};
use bytes::Bytes;
use novarocks_query_application::{session_effects::SessionEffects, statement_kind::StatementKind};
use novarocks_workload_control::ResultWindowAlias;
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::sync::{Notify, watch};

pub(crate) struct Page {
    pub(crate) chunks: Vec<Bytes>,
    pub(crate) headers: HeaderMap,
    pub(crate) status: StatusCode,
    // A response body may outlive acknowledgement and must retain the original window.
    pub(crate) _window: Option<ResultWindowAlias>,
}
pub(crate) struct Production {
    pub(crate) pipeline: Option<PagePipeline>,
    pub(crate) columns: Option<Bytes>,
    pub(crate) outcome: Option<Outcome>,
    pub(crate) retained: bool,
}
pub(crate) struct Outcome {
    pub(crate) settlement: Option<Settlement>,
    pub(crate) error: Option<TrinoQueryError>,
    pub(crate) disposition: Disposition,
    pub(crate) effects: SessionEffects,
    pub(crate) kind: StatementKind,
}
pub(crate) enum Phase {
    Registered,
    Executing,
    Settled {
        token: u64,
        page: Arc<Page>,
        abandoned: bool,
    },
}
pub(crate) struct EntryState {
    pub(crate) phase: Phase,
    pub(crate) next_token: u64,
    pub(crate) retained: Option<(u64, Arc<Page>)>,
}
pub(crate) struct QueryEntry {
    pub(crate) id: novarocks_query_application::session_control::ClientQueryId,
    pub(crate) slug: String,
    pub(crate) base: String,
    pub(crate) request: Mutex<Option<ParsedStatementRequest>>,
    pub(crate) state: tokio::sync::Mutex<EntryState>,
    pub(crate) production: Mutex<Production>,
    pub(crate) progress: Arc<Notify>,
    pub(crate) touched: Mutex<Instant>,
    pub(crate) created: Instant,
    pub(crate) started: Mutex<Option<Instant>>,
    pub(crate) command: watch::Sender<Option<Disposition>>,
    pub(crate) driver: Mutex<Option<tokio::task::JoinHandle<()>>>,
    pub(crate) observation: crate::observation::TrinoProtocolObservation,
    pub(crate) requests: Notify,
    pub(crate) timer: Mutex<Option<tokio::task::JoinHandle<()>>>,
}
impl QueryEntry {
    pub(crate) fn touch(&self) {
        *self.touched.lock().expect("query request clock") = Instant::now();
        self.requests.notify_one();
    }
}
