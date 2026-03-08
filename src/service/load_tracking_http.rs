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

use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::IntoResponse;

use crate::runtime::load_tracking;
use crate::runtime::query_context::QueryId;

pub(crate) async fn handle_load_tracking_log(
    Path((hi, lo)): Path<(i64, i64)>,
) -> impl IntoResponse {
    let query_id = QueryId { hi, lo };
    match load_tracking::get_tracking_log(query_id) {
        Some(log) => (StatusCode::OK, format!("{log}\n")).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            format!("tracking log is not available for query_id={query_id}"),
        )
            .into_response(),
    }
}
