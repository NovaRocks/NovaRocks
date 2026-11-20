// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Dependency validation reads only the input class that the dependency requires.

mod builtin;
pub(crate) mod history;
mod inputs;
mod row_delta;

pub(crate) use builtin::diagnose_ref_unchanged;
pub use builtin::validate;
#[allow(
    unused_imports,
    reason = "P07 supplies the borrowed source observation"
)]
pub(crate) use builtin::validate_with_removed_data;
pub use history::HistoryGap;
#[cfg(test)]
pub use history::{HistoryWindow, history_window};
#[cfg(test)]
pub use inputs::LiveSet;
pub use inputs::ValidationInputs;
pub(crate) use row_delta::{RemovedDataFiles, RowDeltaWindowCheck};

use novarocks_spi::connector::ConnectorMutationFailure;

use crate::iceberg::spec::TableMetadata;

use super::model::{Dependency, EntryIdentity};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Culprit {
    Ref {
        expected: Option<i64>,
        actual: Option<i64>,
    },
    Snapshot(i64),
    Entry {
        snapshot: i64,
        identity: EntryIdentity,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Verdict<D = Dependency> {
    Holds,
    Broken { dependency: D, culprit: Culprit },
    ValidationConflict(ConnectorMutationFailure),
    Unprovable { dependency: D, reason: HistoryGap },
}

impl Verdict {
    #[allow(
        dead_code,
        reason = "P07 projects the typed row-delta failure before dispatch"
    )]
    pub(crate) fn row_delta_conflict(
        &self,
        metadata: &TableMetadata,
        start: Option<i64>,
        parent: Option<i64>,
    ) -> Option<ConnectorMutationFailure> {
        match self {
            Self::ValidationConflict(failure) => Some(failure.clone()),
            Self::Unprovable {
                dependency: Dependency::RowDeltaWindow(_),
                reason,
            } => Some(row_delta::history_conflict(reason, metadata, start, parent)),
            _ => None,
        }
    }
}
