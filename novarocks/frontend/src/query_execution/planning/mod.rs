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

pub(crate) mod delta_scan;
pub mod statistics;
pub mod time_travel;
pub(crate) mod write_sink;

use std::sync::Arc;

use crate::common::query_cancellation::QueryCancellationView;
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

pub(crate) struct PostCompilePlanningContext<'a> {
    pub(crate) table_bindings:
        Arc<novarocks::catalog_application::query_bindings::QueryTableBindingStore>,
    pub(crate) connector_controls: &'a dyn novarocks_spi::connector::ConnectorControlResolver,
    pub(crate) connector_context: &'a novarocks_spi::connector::ConnectorRequestContext,
}

pub(crate) struct QueryPlanningInputs<'a> {
    pub(crate) analyze_request: SqlAnalyzeRequest<'a>,
    pub(crate) post_compile: PostCompilePlanningContext<'a>,
}
