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

//! Frontend lifecycle completion aggregation.
//!
//! Native lifecycle wire values remain in `lifecycle::terminal`. This module
//! owns the coordinator-only aggregate reconstructed after protocol ingress
//! validates every participant terminal snapshot.

use std::collections::BTreeSet;

use crate::{QueryLifecycleError, QueryLifecycleErrorCode};
use novarocks_proto::lifecycle::QueryTerminalSnapshot;
use novarocks_proto::novarocks;

use super::runtime_filter_terminal_rollup::RuntimeFilterTerminalRollup;

#[derive(Clone, Debug, PartialEq)]
pub struct QueryTerminalSet {
    snapshots: Vec<QueryTerminalSnapshot>,
}

impl QueryTerminalSet {
    pub fn new(mut snapshots: Vec<QueryTerminalSnapshot>) -> Result<Self, QueryLifecycleError> {
        snapshots.sort_by_key(|snapshot| {
            (
                snapshot.execution_id(),
                snapshot.backend().backend_id(),
                snapshot.backend().start_epoch(),
            )
        });
        let mut identities = BTreeSet::new();
        for snapshot in &snapshots {
            let identity = (
                snapshot.execution_id(),
                snapshot.backend().backend_id(),
                snapshot.backend().start_epoch(),
            );
            if !identities.insert(identity) {
                return Err(QueryLifecycleError::new(
                    QueryLifecycleErrorCode::Conflict,
                    "query terminal set contains duplicate participant identity",
                ));
            }
        }
        Ok(Self { snapshots })
    }

    /// Protocol has already validated every canonical snapshot at ingress.
    pub fn from_protocol_snapshots(
        snapshots: Vec<QueryTerminalSnapshot>,
    ) -> Result<Self, QueryLifecycleError> {
        Self::new(snapshots)
    }

    pub fn snapshots(&self) -> &[QueryTerminalSnapshot] {
        &self.snapshots
    }

    pub fn fragments(&self) -> impl Iterator<Item = &novarocks::QueryTerminalFragmentSnapshot> {
        self.snapshots
            .iter()
            .flat_map(|snapshot| snapshot.as_proto().fragments.iter())
    }

    pub fn is_success(&self) -> bool {
        self.snapshots.iter().all(|snapshot| {
            snapshot.fragments().into_iter().all(|fragment| {
                fragment.outcome() == novarocks::QueryTerminalFragmentOutcome::Succeeded
            })
        })
    }

    /// Builds the Frontend-private Runtime Filter diagnostic projection from
    /// this already de-duplicated, immutable terminal set.
    pub(crate) fn runtime_filter_terminal_rollup(&self) -> RuntimeFilterTerminalRollup {
        super::runtime_filter_terminal_rollup::rollup(self)
    }
}
