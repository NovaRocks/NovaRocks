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

//! Immutable Native query-options facts shared by the role-local task hosts.

use std::sync::Arc;

use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_execution_contract::task_execution::domain::{CodecOwnedContent, ContentFingerprint};
use novarocks_task_codec::domain::WireContent;
use novarocks_task_codec::operation::ESTABLISH_QUERY_OPTIONS_DOMAIN_TAG;

#[derive(Clone, Debug)]
pub struct QueryContextOptions {
    runtime: Arc<QueryOptions>,
    fingerprint: ContentFingerprint,
}

pub fn query_options_fingerprint(
    wire: novarocks_proto_models::novarocks::QueryOptions,
) -> ContentFingerprint {
    WireContent::new(ESTABLISH_QUERY_OPTIONS_DOMAIN_TAG, wire).fingerprint()
}

impl QueryContextOptions {
    pub const fn new(runtime: Arc<QueryOptions>, fingerprint: ContentFingerprint) -> Self {
        Self {
            runtime,
            fingerprint,
        }
    }

    pub fn runtime(&self) -> &Arc<QueryOptions> {
        &self.runtime
    }

    pub const fn fingerprint(&self) -> ContentFingerprint {
        self.fingerprint
    }
}
