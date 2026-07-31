#![allow(dead_code)]
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

use crate::sql::optimizer::stats_input::{BaseTableStatistics, StatsMissingReason};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ScanSourceIdentity {
    IcebergTable {
        catalog: String,
        namespace: String,
        table: String,
    },
    Unsupported {
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum TableSnapshotRef {
    Current,
    SnapshotId(i64),
    Branch(String),
    Tag(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct TableStatsRequest {
    pub catalog: Option<String>,
    pub database: String,
    pub table: String,
    pub source: ScanSourceIdentity,
    pub snapshot: Option<TableSnapshotRef>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum StatsProviderError {
    Unsupported(String),
    Catalog(String),
    Metadata(String),
}

impl StatsProviderError {
    pub(crate) fn into_missing_reason(self) -> StatsMissingReason {
        match self {
            Self::Unsupported(reason) => StatsMissingReason::ConnectorUnsupported(reason),
            Self::Catalog(err) | Self::Metadata(err) => StatsMissingReason::CatalogLoadError(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::optimizer::stats_input::StatsMissingReason;

    #[test]
    fn provider_error_converts_to_missing_reason() {
        assert_eq!(
            StatsProviderError::Unsupported("jdbc".to_string()).into_missing_reason(),
            StatsMissingReason::ConnectorUnsupported("jdbc".to_string())
        );
        assert_eq!(
            StatsProviderError::Catalog("missing catalog".to_string()).into_missing_reason(),
            StatsMissingReason::CatalogLoadError("missing catalog".to_string())
        );
        assert_eq!(
            StatsProviderError::Metadata("bad metadata".to_string()).into_missing_reason(),
            StatsMissingReason::CatalogLoadError("bad metadata".to_string())
        );
    }
}
