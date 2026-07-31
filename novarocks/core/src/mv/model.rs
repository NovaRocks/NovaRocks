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

use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MvStorageEngine {
    /// Legacy persisted state only. Native DDL rejects this value; future
    /// StarRocks support must be represented by an external connector.
    StarRocks,
    Iceberg,
}

impl MvStorageEngine {
    pub(crate) fn as_sql_str(self) -> &'static str {
        match self {
            Self::StarRocks => "starrocks",
            Self::Iceberg => "iceberg",
        }
    }

    pub(crate) fn backend_name(self) -> &'static str {
        match self {
            Self::StarRocks => "starrocks",
            Self::Iceberg => "iceberg",
        }
    }

    pub(crate) fn from_sql_str(value: &str) -> Result<Self, String> {
        match value.to_ascii_lowercase().as_str() {
            "starrocks" => Err(
                "materialized view storage_engine='starrocks' is no longer supported; use storage_engine='iceberg'"
                    .to_string(),
            ),
            "iceberg" => Ok(Self::Iceberg),
            _ => Err(format!(
                "unknown materialized view storage_engine `{value}`"
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvTarget {
    pub catalog: Option<String>,
    pub database: String,
    pub name: String,
}

impl MvTarget {
    pub fn display_name(&self) -> String {
        match self.catalog.as_deref() {
            Some(catalog) => format!("{catalog}.{}.{}", self.database, self.name),
            None => format!("{}.{}", self.database, self.name),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RefreshMode {
    Noop,
    Full,
    Incremental,
    Rebuild,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct MvPartitionKey {
    pub spec_id: i32,
    pub fields: Vec<MvPartitionKeyField>,
}

impl MvPartitionKey {
    pub(crate) fn new(spec_id: i32, fields: Vec<MvPartitionKeyField>) -> Self {
        Self { spec_id, fields }
    }

    pub(crate) fn canonical_string(&self) -> String {
        let mut out = format!("spec={}", self.spec_id);
        for field in &self.fields {
            out.push(';');
            out.push_str(&encode_component(&field.field_name));
            out.push('=');
            match &field.value {
                MvPartitionValue::Null => out.push_str("null"),
                MvPartitionValue::String(value) => {
                    out.push_str("s:");
                    out.push_str(&encode_component(value));
                }
            }
        }
        out
    }
}

fn encode_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            _ => {
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                out.push('%');
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 0x0F) as usize] as char);
            }
        }
    }
    out
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct MvPartitionKeyField {
    pub field_name: String,
    pub value: MvPartitionValue,
}

impl MvPartitionKeyField {
    pub(crate) fn new(field_name: String, value: MvPartitionValue) -> Self {
        Self { field_name, value }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum MvPartitionValue {
    Null,
    String(String),
}

/// Optional partition predicate that the aggregate target state loader and
/// the iceberg MV target locator share. `None` means "do not prune"; an
/// `AllowList` means "drop FileScanTasks whose target partition key is not in
/// this set". The empty allow-list is a legitimate state (no partition is
/// affected); callers MUST NOT silently treat it as "no filter".
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TargetPartitionFilter {
    None,
    AllowList(BTreeSet<MvPartitionKey>),
}

impl TargetPartitionFilter {
    pub(crate) fn matches(&self, key: &MvPartitionKey) -> bool {
        match self {
            Self::None => true,
            Self::AllowList(set) => set.contains(key),
        }
    }

    pub(crate) fn allow_list_len(&self) -> Option<usize> {
        match self {
            Self::None => None,
            Self::AllowList(set) => Some(set.len()),
        }
    }

    pub(crate) fn is_allow_list(&self) -> bool {
        matches!(self, Self::AllowList(_))
    }
}

impl std::fmt::Display for TargetPartitionFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::AllowList(set) => write!(f, "AllowList({} keys)", set.len()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AffectedTargetPartitions {
    Unpartitioned,
    Known {
        partitions: BTreeSet<MvPartitionKey>,
    },
    NotDerived {
        reason: String,
    },
}

impl AffectedTargetPartitions {
    pub(crate) fn known<I: IntoIterator<Item = MvPartitionKey>>(partitions: I) -> Self {
        Self::Known {
            partitions: partitions.into_iter().collect(),
        }
    }

    pub(crate) fn not_derived(reason: impl Into<String>) -> Self {
        Self::NotDerived {
            reason: reason.into(),
        }
    }

    pub(crate) fn not_derived_reason(&self) -> Option<&str> {
        match self {
            Self::NotDerived { reason } => Some(reason.as_str()),
            Self::Unpartitioned | Self::Known { .. } => None,
        }
    }

    pub(crate) fn is_not_derived(&self) -> bool {
        matches!(self, Self::NotDerived { .. })
    }

    pub(crate) fn partition_count(&self) -> usize {
        match self {
            Self::Unpartitioned | Self::NotDerived { .. } => 0,
            Self::Known { partitions } => partitions.len(),
        }
    }

    /// Convert to a `TargetPartitionFilter` for file-scan pruning. `Known`
    /// becomes an `AllowList`; `Unpartitioned` and `NotDerived` become `None`
    /// (no pruning). Pruning is an optimization: a `NotDerived` outcome must
    /// never restrict the scan. The empty `Known` set legitimately produces an
    /// empty `AllowList` (nothing affected), which the locator honors by
    /// scanning zero files.
    pub(crate) fn to_target_partition_filter(&self) -> TargetPartitionFilter {
        match self {
            Self::Known { partitions } => TargetPartitionFilter::AllowList(partitions.clone()),
            Self::Unpartitioned | Self::NotDerived { .. } => TargetPartitionFilter::None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggregateFunctionKind {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    /// `BOOL_OR(col)` / `boolor_agg(col)`. Uses `Map<Boolean, Int64>` detail
    /// state, same framework as `MIN/MAX`.
    BoolOr,
    /// `BOOL_AND(col)` / `booland_agg(col)`. Uses `Map<Boolean, Int64>` detail
    /// state, same framework as `MIN/MAX`.
    BoolAnd,
    /// `count(DISTINCT col)` / `count_distinct(col)` / `multi_distinct_count(col)`.
    /// Uses shared multiset state encoding; visible counts positive entries.
    CountDistinct,
    /// `approx_count_distinct(col)` / `ndv(col)` / `hll_ndv(col)`.
    /// Shares multiset state with CountDistinct; visible computes an HLL estimate.
    ApproxCountDistinct,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum VisibleAggregateOutput {
    GroupKey(usize),
    Aggregate(usize),
}

/// Identifies a state column's role within its logical aggregate.
///
/// Cardinality contract: one opaque `Single` state per aggregate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggregateStateRole {
    /// Single opaque VARBINARY state column.
    Single,
    /// Hidden row-count state used only to decide whether a group has been fully retracted.
    RetractionCount,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(value: &str) -> MvPartitionKey {
        MvPartitionKey::new(
            7,
            vec![MvPartitionKeyField::new(
                "region/name".to_string(),
                MvPartitionValue::String(value.to_string()),
            )],
        )
    }

    #[test]
    fn leaf_model_preserves_partition_and_filter_contracts() {
        assert_eq!(
            key("east west").canonical_string(),
            "spec=7;region%2Fname=s:east%20west"
        );
        assert!(TargetPartitionFilter::None.matches(&key("any")));
        let empty = TargetPartitionFilter::AllowList(BTreeSet::new());
        assert!(empty.is_allow_list());
        assert_eq!(empty.allow_list_len(), Some(0));
        assert!(!empty.matches(&key("any")));
    }

    #[test]
    fn affected_partition_filter_contract_is_exact() {
        assert!(matches!(
            AffectedTargetPartitions::Unpartitioned.to_target_partition_filter(),
            TargetPartitionFilter::None
        ));
        assert!(matches!(
            AffectedTargetPartitions::not_derived("missing").to_target_partition_filter(),
            TargetPartitionFilter::None
        ));
        let known = AffectedTargetPartitions::known(std::iter::empty());
        assert_eq!(known.to_target_partition_filter().allow_list_len(), Some(0));
    }

    #[test]
    fn mv_storage_engine_errors_remain_exact() {
        assert_eq!(
            MvStorageEngine::from_sql_str("ICEBERG"),
            Ok(MvStorageEngine::Iceberg)
        );
        assert_eq!(
            MvStorageEngine::from_sql_str("starrocks").unwrap_err(),
            "materialized view storage_engine='starrocks' is no longer supported; use storage_engine='iceberg'"
        );
        assert_eq!(
            MvStorageEngine::from_sql_str("other").unwrap_err(),
            "unknown materialized view storage_engine `other`"
        );
    }
}
