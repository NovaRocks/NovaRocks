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

//! Pure retention planning. No manifest, catalog, or object-store I/O occurs here.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::iceberg::spec::{Snapshot, SnapshotRetention, TableMetadata, TableProperties};
use crate::iceberg::{Error, ErrorKind, Result, TableRequirement, TableUpdate};

/// One attempt's metadata-only decision at the operation's fixed evaluation time.
#[derive(Clone, Debug)]
pub(crate) struct RetentionPlan {
    pub format_version: crate::iceberg::spec::FormatVersion,
    pub removed_refs: BTreeSet<String>,
    pub removed_snapshots: Vec<Snapshot>,
    #[cfg(test)]
    pub protected_snapshots: BTreeSet<i64>,
    pub requirements: Vec<TableRequirement>,
    pub statistics_removals: Vec<TableUpdate>,
    pub removed_statistics_lengths: BTreeMap<String, u64>,
    pub removed_statistics_puffin: BTreeSet<String>,
    pub removed_statistics_paths: BTreeSet<String>,
}
impl RetentionPlan {
    /// Upper bound for the first-party retention decision's actual flat owned
    /// copies and collection replacements, before compute()/journal clone.
    /// This does not claim ownership or measurement of the borrowed SDK table.
    pub(crate) fn owned_decision_bytes(metadata: &TableMetadata) -> Result<u64> {
        fn add(total: &mut usize, bytes: usize) -> Result<()> {
            *total = total
                .checked_add(bytes)
                .ok_or_else(|| invalid("retention backing overflow"))?;
            Ok(())
        }
        fn tree<K, V>(n: usize) -> Result<usize> {
            // Original ordered containers: worst sparse nodes plus a full
            // replacement. Eleven K/V slots and twelve child pointers per node.
            let node = 11usize
                .checked_mul(std::mem::size_of::<K>() + std::mem::size_of::<V>())
                .and_then(|v| v.checked_add(16 + 12 * std::mem::size_of::<usize>()))
                .ok_or_else(|| invalid("retention tree overflow"))?;
            n.checked_mul(2)
                .and_then(|n| n.checked_add(2))
                .and_then(|n| n.checked_mul(node))
                .ok_or_else(|| invalid("retention tree overflow"))
        }
        let n = metadata.snapshots().len();
        let refs = metadata.refs().len();
        let mut total = std::mem::size_of::<Self>();
        add(
            &mut total,
            tree::<i64, ()>(n)?
                .checked_mul(4)
                .ok_or_else(|| invalid("retention scratch overflow"))?,
        )?;
        add(&mut total, tree::<String, ()>(refs)?)?;
        add(
            &mut total,
            refs.checked_mul(
                2 * std::mem::size_of::<TableRequirement>()
                    + 2 * std::mem::size_of::<TableUpdate>()
                    + 2 * std::mem::size_of::<(i64, usize, i64)>(),
            )
            .ok_or_else(|| invalid("retention ref overflow"))?,
        )?;
        for name in metadata.refs().keys() {
            add(
                &mut total,
                name.len()
                    .checked_mul(4)
                    .ok_or_else(|| invalid("retention ref overflow"))?,
            )?;
        }
        for snapshot in metadata.snapshots() {
            add(&mut total, 2 * std::mem::size_of::<Snapshot>())?;
            add(&mut total, snapshot.manifest_list().len())?;
            if let Some(key) = snapshot.encryption_key_id() {
                add(&mut total, key.len())?;
            }
            let properties = &snapshot.summary().additional_properties;
            add(&mut total, tree::<String, String>(properties.capacity())?)?;
            for (key, value) in properties {
                add(&mut total, key.len())?;
                add(&mut total, value.len())?;
            }
            add(
                &mut total,
                2 * std::mem::size_of::<TableUpdate>() + 2 * std::mem::size_of::<i64>(),
            )?;
            for path in [
                metadata
                    .statistics_for_snapshot(snapshot.snapshot_id())
                    .map(|s| s.statistics_path.as_str()),
                metadata
                    .partition_statistics_for_snapshot(snapshot.snapshot_id())
                    .map(|s| s.statistics_path.as_str()),
            ]
            .into_iter()
            .flatten()
            {
                add(&mut total, tree::<String, u64>(1)?)?;
                add(
                    &mut total,
                    path.len()
                        .checked_mul(4)
                        .ok_or_else(|| invalid("retention statistics overflow"))?,
                )?;
            }
        }
        // The current decision, its original journal clone and updates tail
        // coexist. Retried journal entries are accumulated by their caller.
        u64::try_from(
            total
                .checked_mul(3)
                .ok_or_else(|| invalid("retention copies overflow"))?,
        )
        .map_err(|_| invalid("retention backing overflow"))
    }
    pub(crate) fn compute(
        metadata: &TableMetadata,
        evaluation_time_ms: i64,
        older_than_ms: Option<i64>,
        retain_last: Option<u32>,
    ) -> Result<Self> {
        if evaluation_time_ms <= 0 || older_than_ms.is_some_and(|time| time < 0) {
            return Err(invalid(
                "Snapshot expiration requires a valid fixed evaluation time",
            ));
        }
        require_gc(metadata)?;
        let properties = TableProperties::new(metadata.properties());
        let min_keep = match retain_last {
            Some(0) => return Err(invalid("Snapshot retention count must be positive")),
            Some(value) => value as usize,
            None => properties.min_snapshots_to_keep()?,
        };
        if min_keep == 0 {
            return Err(invalid("Snapshot retention count must be positive"));
        }
        let snapshot_age = positive(properties.max_snapshot_age_ms()?, "snapshot age")?;
        let ref_age = positive(properties.max_ref_age_ms()?, "ref age")?;
        let cutoff =
            older_than_ms.unwrap_or_else(|| evaluation_time_ms.saturating_sub(snapshot_age));
        let mut removed_refs = BTreeSet::new();
        let mut protected = BTreeSet::new();
        let mut referenced = HashSet::new();
        let mut branches = Vec::new();
        let mut requirements = Vec::new();
        let mut refs = metadata.refs().iter().collect::<Vec<_>>();
        refs.sort_by(|left, right| left.0.cmp(right.0));
        for (name, reference) in refs {
            // Every baseline ref participates, including refs this plan removes.
            requirements.push(TableRequirement::RefSnapshotIdMatch {
                r#ref: name.clone(),
                snapshot_id: Some(reference.snapshot_id),
            });
            let head = metadata
                .snapshot_by_id(reference.snapshot_id)
                .ok_or_else(|| invalid("Snapshot reference points to an absent snapshot"))?;
            let max_ref_age = match reference.retention {
                SnapshotRetention::Branch { max_ref_age_ms, .. }
                | SnapshotRetention::Tag { max_ref_age_ms } => max_ref_age_ms,
            }
            .unwrap_or(ref_age);
            positive(max_ref_age, "ref age")?;
            // Validate all explicit policy fields even for a ref that ages out.
            let branch_policy = match reference.retention {
                SnapshotRetention::Branch {
                    min_snapshots_to_keep,
                    max_snapshot_age_ms,
                    ..
                } => {
                    let keep = match min_snapshots_to_keep {
                        Some(value) if value <= 0 => {
                            return Err(invalid("Branch retention count must be positive"));
                        }
                        Some(value) => value as usize,
                        None => min_keep,
                    };
                    let branch_cutoff = match max_snapshot_age_ms {
                        Some(age) => {
                            evaluation_time_ms.saturating_sub(positive(age, "branch snapshot age")?)
                        }
                        None => cutoff,
                    };
                    Some((keep, branch_cutoff))
                }
                SnapshotRetention::Tag { .. } => None,
            };
            if name != "main"
                && evaluation_time_ms.saturating_sub(head.timestamp_ms()) > max_ref_age
            {
                removed_refs.insert(name.clone());
                continue;
            }
            protected.insert(reference.snapshot_id);
            referenced.insert(reference.snapshot_id);
            if let Some((keep, branch_cutoff)) = branch_policy {
                branches.push((reference.snapshot_id, keep, branch_cutoff));
            }
        }
        if let Some(current) = metadata.current_snapshot() {
            protected.insert(current.snapshot_id());
            if !branches
                .iter()
                .any(|(head, _, _)| *head == current.snapshot_id())
            {
                branches.push((current.snapshot_id(), min_keep, cutoff));
            }
        }
        for (head, keep, branch_cutoff) in branches {
            let mut next = Some(head);
            let mut ordinal = 0usize;
            let mut seen = HashSet::new();
            while let Some(id) = next {
                if !seen.insert(id) {
                    return Err(invalid("Snapshot ancestry contains a cycle"));
                }
                let Some(snapshot) = metadata.snapshot_by_id(id) else {
                    // A previously expired ancestor terminates the available chain.
                    break;
                };
                referenced.insert(id);
                if ordinal < keep || snapshot.timestamp_ms() >= branch_cutoff {
                    protected.insert(id);
                }
                ordinal += 1;
                next = snapshot.parent_snapshot_id();
            }
        }
        for snapshot in metadata.snapshots() {
            if !referenced.contains(&snapshot.snapshot_id()) && snapshot.timestamp_ms() >= cutoff {
                protected.insert(snapshot.snapshot_id());
            }
        }
        let mut removed_snapshots = metadata
            .snapshots()
            .filter(|snapshot| !protected.contains(&snapshot.snapshot_id()))
            .map(|snapshot| snapshot.as_ref().clone())
            .collect::<Vec<_>>();
        removed_snapshots.sort_by_key(Snapshot::snapshot_id);
        let mut statistics_removals = Vec::new();
        let mut removed_statistics_paths = BTreeSet::new();
        let mut removed_statistics_puffin = BTreeSet::new();
        let mut removed_statistics_lengths = BTreeMap::new();
        for snapshot in &removed_snapshots {
            let id = snapshot.snapshot_id();
            if let Some(statistics) = metadata.statistics_for_snapshot(id) {
                statistics_removals.push(TableUpdate::RemoveStatistics { snapshot_id: id });
                removed_statistics_puffin.insert(statistics.statistics_path.clone());
                removed_statistics_lengths.insert(
                    statistics.statistics_path.clone(),
                    u64::try_from(statistics.file_size_in_bytes)
                        .map_err(|_| invalid("Statistics inline length is negative"))?,
                );
                removed_statistics_paths.insert(statistics.statistics_path.clone());
            }
            if let Some(statistics) = metadata.partition_statistics_for_snapshot(id) {
                statistics_removals
                    .push(TableUpdate::RemovePartitionStatistics { snapshot_id: id });
                removed_statistics_paths.insert(statistics.statistics_path.clone());
            }
        }
        Ok(Self {
            format_version: metadata.format_version(),
            statistics_removals,
            removed_statistics_paths,
            removed_statistics_puffin,
            removed_statistics_lengths,
            removed_refs,
            removed_snapshots,
            #[cfg(test)]
            protected_snapshots: protected,
            requirements,
        })
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.removed_refs.is_empty() && self.removed_snapshots.is_empty()
    }
    pub(crate) fn updates(&self) -> Vec<TableUpdate> {
        let mut updates = self
            .removed_refs
            .iter()
            .map(|name| TableUpdate::RemoveSnapshotRef {
                ref_name: name.clone(),
            })
            .collect::<Vec<_>>();
        if !self.removed_snapshots.is_empty() {
            updates.push(TableUpdate::RemoveSnapshots {
                snapshot_ids: self
                    .removed_snapshots
                    .iter()
                    .map(Snapshot::snapshot_id)
                    .collect(),
            });
        }
        updates.extend(self.statistics_removals.clone());
        updates
    }
}

pub(crate) fn parse_gc_enabled_property_value(value: &str) -> Result<bool> {
    value.to_ascii_lowercase().parse::<bool>().map_err(|error| {
        Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Invalid value for {}: {error}",
                TableProperties::PROPERTY_GC_ENABLED
            ),
        )
    })
}

pub(crate) fn require_gc(metadata: &TableMetadata) -> Result<()> {
    let enabled = metadata
        .properties()
        .get(TableProperties::PROPERTY_GC_ENABLED)
        .map(|value| parse_gc_enabled_property_value(value))
        .transpose()?
        .unwrap_or(TableProperties::PROPERTY_GC_ENABLED_DEFAULT);
    if !enabled {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Snapshot expiration is disabled because gc.enabled is false",
        ));
    }
    Ok(())
}
fn positive(value: i64, label: &str) -> Result<i64> {
    if value <= 0 {
        Err(invalid(format!(
            "Snapshot retention {label} must be positive"
        )))
    } else {
        Ok(value)
    }
}
fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::DataInvalid, message.into())
}

#[cfg(test)]
#[path = "retention/tests.rs"]
mod tests;
