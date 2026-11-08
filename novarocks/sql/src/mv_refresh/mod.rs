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

//! SQL-owned artifacts for materialized-view refresh.
//!
//! These artifacts describe immutable SQL and refresh facts. They never carry
//! result batches, catalog handles, or a connector implementation.

use std::collections::BTreeMap;

pub(crate) mod aggregate_shape;
pub mod first_refresh;

/// SQL classification of an aggregate state expression in an IMV plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateFunctionKind {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    BoolOr,
    BoolAnd,
    CountDistinct,
    ApproxCountDistinct,
}

/// Stable visible-output ordering for an aggregate IMV plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VisibleAggregateOutput {
    GroupKey(usize),
    Aggregate(usize),
}

/// SQL identity of a materialized-view target.
///
/// This value is shared with the application lifecycle, but it is canonical
/// planning vocabulary: it has no repository, connector, or execution
/// authority. Application code may re-export it for compatibility while its
/// persistence and lifecycle adapters are migrated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlMvTarget {
    pub catalog: Option<String>,
    pub database: String,
    pub name: String,
}

impl SqlMvTarget {
    pub fn display_name(&self) -> String {
        match self.catalog.as_deref() {
            Some(catalog) => format!("{catalog}.{}.{}", self.database, self.name),
            None => format!("{}.{}", self.database, self.name),
        }
    }
}

/// Typed SQL projection of `REFRESH MATERIALIZED VIEW`.
///
/// It preserves `FULL` so the application prepares a full overwrite rather
/// than silently selecting an incremental refresh.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshStatement {
    pub name_parts: Vec<String>,
    pub full: bool,
}

#[cfg(test)]
mod aggregate_vocabulary_tests {
    use super::*;

    #[test]
    fn sqlx2_mv_aggregate_vocabulary_is_sql_owned() {
        assert_eq!(AggregateFunctionKind::Count, AggregateFunctionKind::Count);
        assert_eq!(
            VisibleAggregateOutput::GroupKey(0),
            VisibleAggregateOutput::GroupKey(0)
        );
    }
}

/// SQL facts that the frontend needs to atomically finalize the MV definition
/// after provider publication. The values are observed during preparation;
/// the frontend never resolves catalog metadata itself.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvRefreshFinalizeFacts {
    pub mv_id: i64,
    pub target: SqlMvTarget,
    /// What each base relation occurrence was read at, keyed by the occurrence
    /// the CREATE documents minted. A definition may read one table twice, and
    /// each mention is pinned on its own.
    pub base_snapshots: BTreeMap<crate::compiler::SqlMvRelationOccurrenceId, Option<i64>>,
    pub base_table_object_ids: BTreeMap<
        crate::compiler::SqlMvRelationOccurrenceId,
        novarocks_spi::connector::ConnectorTableObjectId,
    >,
    pub expected_target_snapshot_id: Option<i64>,
    /// Immutable target identity observed during refresh preparation. It is
    /// carried into the staged-ref and publication CASes to reject a
    /// DROP/recreate under the same logical table name.
    pub target_table_uuid: String,
}

#[cfg(test)]
mod tests {
    use super::SqlMvTarget;

    #[test]
    fn sqlx2_mv_target_identity_is_sql_owned() {
        let target = SqlMvTarget {
            catalog: Some("iceberg".to_string()),
            database: "analytics".to_string(),
            name: "daily_orders".to_string(),
        };

        assert_eq!(target.display_name(), "iceberg.analytics.daily_orders");
    }
}
