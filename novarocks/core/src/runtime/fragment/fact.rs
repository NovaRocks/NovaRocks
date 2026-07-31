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

use crate::common::types::UniqueId;
use crate::runtime::fragment::error::FragmentExecutionError;
use crate::runtime::profile::RuntimeProfileTree;
use crate::runtime::query_context::QueryId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FragmentCancelReason {
    detail: String,
}

impl FragmentCancelReason {
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FragmentOutcome {
    Succeeded,
    Failed(FragmentExecutionError),
    Cancelled { reason: FragmentCancelReason },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FragmentTerminalFact {
    query_id: QueryId,
    fragment_instance_id: UniqueId,
    outcome: FragmentOutcome,
    profile: Option<RuntimeProfileTree>,
    statistics_payload: Vec<u8>,
}

impl FragmentTerminalFact {
    pub(crate) fn new(
        query_id: QueryId,
        fragment_instance_id: UniqueId,
        outcome: FragmentOutcome,
        profile: Option<RuntimeProfileTree>,
        statistics_payload: Vec<u8>,
    ) -> Self {
        Self {
            query_id,
            fragment_instance_id,
            outcome,
            profile,
            statistics_payload,
        }
    }

    pub const fn query_id(&self) -> QueryId {
        self.query_id
    }

    pub const fn fragment_instance_id(&self) -> UniqueId {
        self.fragment_instance_id
    }

    pub const fn outcome(&self) -> &FragmentOutcome {
        &self.outcome
    }

    pub const fn profile(&self) -> Option<&RuntimeProfileTree> {
        self.profile.as_ref()
    }

    /// The bounded opaque partial produced by a statistics terminal sink.
    /// It is empty for all non-statistics fragments.
    pub fn statistics_payload(&self) -> &[u8] {
        &self.statistics_payload
    }
}
