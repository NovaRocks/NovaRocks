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

use crate::StatementFailure;
use serde::Serialize;

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StatementTraceState {
    Submitted,
    Finished,
    Failed,
}

/// Borrowed protocol facts for the caller's diagnostic sink. They grant no
/// retry, cancellation, session effect, or result-comparison authority.
#[derive(Debug, Serialize)]
pub struct StatementTrace<'a> {
    pub query_id: Option<&'a str>,
    pub initial_next_uri: Option<&'a str>,
    pub pages: usize,
    pub update_type: Option<&'a str>,
    pub state: StatementTraceState,
    pub failure: Option<&'a StatementFailure>,
}

#[derive(Clone, Default)]
pub(crate) struct TraceProgress {
    pub query_id: Option<String>,
    pub initial_next_uri: Option<String>,
    pub pages: usize,
    pub update_type: Option<String>,
}
impl TraceProgress {
    pub fn observation<'a>(
        &'a self,
        state: StatementTraceState,
        failure: Option<&'a StatementFailure>,
    ) -> StatementTrace<'a> {
        StatementTrace {
            query_id: self.query_id.as_deref(),
            initial_next_uri: self.initial_next_uri.as_deref(),
            pages: self.pages,
            update_type: self.update_type.as_deref(),
            state,
            failure,
        }
    }
}
