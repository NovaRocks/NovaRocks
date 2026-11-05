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

//! Provider-frozen cohort inputs retained by one rewrite statement.

use novarocks_spi::connector::{
    ConnectorControlPlanningPin, ConnectorFrozenRewriteGroup, ConnectorPinnedFileSet,
};

/// The exact file set a mutation or rewrite cohort reads and later replaces.
#[derive(Clone)]
pub(crate) struct QueryPinnedFileSetRead {
    pub(crate) pinned: ConnectorPinnedFileSet,
    /// Provider-owned original source; FE only loans it to the admitted reader.
    /// Ordinary maintenance cohorts have no such receipt.
    pub(crate) frozen_source:
        Option<novarocks_spi::connector::read_stack::runtime::ConnectorFrozenReadSource>,
    pub(crate) planning_pin: ConnectorControlPlanningPin,
    // COW-only neutral holder: declared last so schema/source peers retire first.
    pub(crate) _original: Option<novarocks_spi::connector::ConnectorOriginalResultScope>,
}

impl std::fmt::Debug for QueryPinnedFileSetRead {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PinnedFileSetRead")
            .field("namespace", &self.pinned.namespace())
            .field("table", &self.pinned.table())
            .field("version_ordinal", &self.pinned.version_ordinal())
            .field("files", &self.pinned.files().len())
            .finish_non_exhaustive()
    }
}

/// The exact frozen artifact group a distributed procedure cohort reads.
#[derive(Clone)]
pub(crate) struct QueryRewriteGroupRead {
    pub(crate) group: ConnectorFrozenRewriteGroup,
    pub(crate) group_digest: [u8; 32],
    pub(crate) planning_pin: ConnectorControlPlanningPin,
}

impl std::fmt::Debug for QueryRewriteGroupRead {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RewriteGroupRead")
            .field("namespace", &self.group.schema_name())
            .field("table", &self.group.table_name())
            .field("operation_id", &self.group.operation_id())
            .finish_non_exhaustive()
    }
}
