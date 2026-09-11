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

//! Exact and normalized identities for one prepared mixed-workload window.
//!
//! Raw identities retain private object names and physical Iceberg identifiers
//! so a run can prove what it actually measured. Semantic identities replace
//! those run-specific values with stable window/job symbols for A/A and A/B
//! comparison. Constructing an identity validates the complete input rather
//! than repairing incomplete provider observations.

use super::manifest::BusinessKind;
use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const FIXTURE_IDENTITY_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PartitionShape {
    Unpartitioned,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RawFileFact {
    pub content: i32,
    pub file_path: String,
    pub file_format: String,
    pub spec_id: i32,
    pub record_count: u64,
    pub file_size_in_bytes: u64,
    pub partition_shape: PartitionShape,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RawSnapshotFact {
    pub snapshot_id: i64,
    pub parent_id: Option<i64>,
    pub operation: String,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct RawTableIdentity {
    pub qualified_name: String,
    pub rows: u64,
    pub sum: u64,
    pub files: Vec<RawFileFact>,
    pub snapshots: Vec<RawSnapshotFact>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub(crate) struct SemanticFileFact {
    pub content: i32,
    pub file_format: String,
    pub spec_ordinal: usize,
    pub record_count: u64,
    pub file_size_in_bytes: u64,
    pub partition_shape: PartitionShape,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct SemanticSnapshotFact {
    pub ordinal: usize,
    pub parent_ordinal: Option<usize>,
    pub operation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct SemanticTableIdentity {
    pub symbol: String,
    pub window_index: usize,
    pub business_kind: Option<BusinessKind>,
    pub job_ordinal: Option<usize>,
    pub rows: u64,
    pub sum: u64,
    /// A sorted multiset. Equal entries remain repeated because two physical
    /// files with the same semantic properties are still two input files.
    pub files: Vec<SemanticFileFact>,
    pub snapshots: Vec<SemanticSnapshotFact>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct FixtureTableIdentity {
    pub raw: RawTableIdentity,
    pub raw_bundle_sha256: String,
    pub semantic: SemanticTableIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub(crate) enum BusinessInitialState {
    MvRefresh {
        refresh_time: Option<String>,
        refresh_rows: Option<u64>,
    },
    Analyze {
        matching_jobs: usize,
        theta_statistics_available: bool,
    },
    Optimize {
        matching_jobs: usize,
        live_delete_files: usize,
    },
}

impl BusinessInitialState {
    pub(crate) fn validate_for(&self, kind: BusinessKind) -> Result<()> {
        match (kind, self) {
            (
                BusinessKind::MvRefresh,
                Self::MvRefresh {
                    refresh_time,
                    refresh_rows,
                },
            ) => ensure!(
                refresh_time.is_none() && refresh_rows.is_none_or(|rows| rows == 0),
                "mixed MV input already has a published refresh"
            ),
            (
                BusinessKind::Analyze,
                Self::Analyze {
                    matching_jobs,
                    theta_statistics_available,
                },
            ) => ensure!(
                *matching_jobs == 0 && !theta_statistics_available,
                "mixed ANALYZE input already has job history or provider statistics"
            ),
            (
                BusinessKind::Optimize,
                Self::Optimize {
                    matching_jobs,
                    live_delete_files,
                },
            ) => ensure!(
                *matching_jobs == 0 && *live_delete_files > 0,
                "mixed OPTIMIZE input is missing its clean job state or deletion vectors"
            ),
            _ => bail!("mixed business initial state does not match its producer kind"),
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PreparedJobIdentity {
    pub kind: BusinessKind,
    pub ordinal: usize,
    pub input: FixtureTableIdentity,
    pub initial_state: BusinessInitialState,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct PreparedWindowIdentity {
    pub schema_version: u32,
    pub window_index: usize,
    pub foreground: FixtureTableIdentity,
    pub jobs: Vec<PreparedJobIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct PreparedJobSemanticIdentity {
    pub kind: BusinessKind,
    pub ordinal: usize,
    pub input: SemanticTableIdentity,
    pub initial_state: BusinessInitialState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct PreparedWindowSemanticIdentity {
    pub schema_version: u32,
    pub window_index: usize,
    pub foreground: SemanticTableIdentity,
    pub jobs: Vec<PreparedJobSemanticIdentity>,
}

impl PreparedWindowIdentity {
    pub(crate) fn try_new(
        window_index: usize,
        foreground: FixtureTableIdentity,
        mut jobs: Vec<PreparedJobIdentity>,
    ) -> Result<Self> {
        ensure!(
            foreground.semantic.window_index == window_index
                && foreground.semantic.business_kind.is_none()
                && foreground.semantic.job_ordinal.is_none()
                && foreground.semantic.symbol == format!("window/{window_index}/foreground"),
            "mixed foreground identity has an invalid semantic symbol"
        );
        let mut keys = BTreeSet::new();
        for job in &jobs {
            ensure!(
                job.input.semantic.window_index == window_index
                    && job.input.semantic.business_kind == Some(job.kind)
                    && job.input.semantic.job_ordinal == Some(job.ordinal)
                    && job.input.semantic.symbol
                        == format!("window/{window_index}/{}/{}", job.kind.name(), job.ordinal),
                "mixed job identity has an invalid semantic symbol"
            );
            ensure!(
                keys.insert((job.kind, job.ordinal)),
                "mixed fixture contains a duplicate producer job identity"
            );
            job.initial_state.validate_for(job.kind)?;
        }
        jobs.sort_by_key(|job| (job.kind, job.ordinal));
        Ok(Self {
            schema_version: FIXTURE_IDENTITY_SCHEMA_VERSION,
            window_index,
            foreground,
            jobs,
        })
    }

    pub(crate) fn semantic_identity(&self) -> PreparedWindowSemanticIdentity {
        PreparedWindowSemanticIdentity {
            schema_version: self.schema_version,
            window_index: self.window_index,
            foreground: self.foreground.semantic.clone(),
            jobs: self
                .jobs
                .iter()
                .map(|job| PreparedJobSemanticIdentity {
                    kind: job.kind,
                    ordinal: job.ordinal,
                    input: job.input.semantic.clone(),
                    initial_state: job.initial_state.clone(),
                })
                .collect(),
        }
    }
}

pub(crate) fn freeze_table_identity(
    qualified_name: String,
    symbol: String,
    window_index: usize,
    business_kind: Option<BusinessKind>,
    job_ordinal: Option<usize>,
    rows: u64,
    sum: u64,
    mut files: Vec<RawFileFact>,
    mut snapshots: Vec<RawSnapshotFact>,
) -> Result<FixtureTableIdentity> {
    ensure!(
        !qualified_name.trim().is_empty() && !symbol.trim().is_empty(),
        "mixed table identity has an empty name or symbol"
    );
    ensure!(rows > 0, "mixed table identity has no effective rows");
    ensure!(!files.is_empty(), "mixed table identity has no live files");
    ensure!(
        !snapshots.is_empty(),
        "mixed table identity has no snapshots"
    );
    ensure!(
        business_kind.is_some() == job_ordinal.is_some(),
        "mixed table identity has a partial business symbol"
    );
    files.sort_by(|left, right| left.file_path.cmp(&right.file_path));
    snapshots.sort_by_key(|snapshot| snapshot.snapshot_id);

    let mut paths = BTreeSet::new();
    let mut spec_shapes = BTreeMap::new();
    for file in &files {
        ensure!(
            matches!(file.content, 0..=2),
            "mixed table identity has an unknown Iceberg file content"
        );
        ensure!(
            !file.file_path.trim().is_empty()
                && !file.file_format.trim().is_empty()
                && file.spec_id >= 0
                && file.record_count > 0
                && file.file_size_in_bytes > 0,
            "mixed table identity has an empty or invalid file fact"
        );
        ensure!(
            paths.insert(file.file_path.as_str()),
            "mixed table identity repeats one physical file path"
        );
        match spec_shapes.insert(file.spec_id, file.partition_shape.clone()) {
            Some(prior) if prior != file.partition_shape => {
                bail!("mixed table identity assigns two partition shapes to one spec")
            }
            _ => {}
        }
    }

    let lineage = normalize_lineage(&snapshots)?;
    let spec_ordinals = spec_shapes
        .keys()
        .enumerate()
        .map(|(ordinal, spec_id)| (*spec_id, ordinal))
        .collect::<BTreeMap<_, _>>();
    let mut semantic_files = files
        .iter()
        .map(|file| {
            Ok(SemanticFileFact {
                content: file.content,
                file_format: file.file_format.trim().to_ascii_lowercase(),
                spec_ordinal: *spec_ordinals
                    .get(&file.spec_id)
                    .context("normalized partition spec is absent")?,
                record_count: file.record_count,
                file_size_in_bytes: file.file_size_in_bytes,
                partition_shape: file.partition_shape.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    semantic_files.sort();

    let raw = RawTableIdentity {
        qualified_name,
        rows,
        sum,
        files,
        snapshots,
    };
    let raw_bytes = serde_json::to_vec(&raw).context("serialize raw mixed table identity")?;
    let raw_bundle_sha256 = hex_sha256(&raw_bytes);
    Ok(FixtureTableIdentity {
        raw,
        raw_bundle_sha256,
        semantic: SemanticTableIdentity {
            symbol,
            window_index,
            business_kind,
            job_ordinal,
            rows,
            sum,
            files: semantic_files,
            snapshots: lineage,
        },
    })
}

fn normalize_lineage(snapshots: &[RawSnapshotFact]) -> Result<Vec<SemanticSnapshotFact>> {
    let mut by_id = BTreeMap::new();
    let mut children = BTreeMap::<i64, Vec<i64>>::new();
    let mut roots = Vec::new();
    for snapshot in snapshots {
        ensure!(
            snapshot.snapshot_id >= 0 && !snapshot.operation.trim().is_empty(),
            "mixed snapshot lineage has an invalid id or operation"
        );
        ensure!(
            by_id.insert(snapshot.snapshot_id, snapshot).is_none(),
            "mixed snapshot lineage repeats a snapshot id"
        );
        if let Some(parent) = snapshot.parent_id {
            ensure!(
                parent >= 0 && parent != snapshot.snapshot_id,
                "mixed snapshot lineage has an invalid self parent"
            );
            children
                .entry(parent)
                .or_default()
                .push(snapshot.snapshot_id);
        } else {
            roots.push(snapshot.snapshot_id);
        }
    }
    ensure!(
        roots.len() == 1,
        "mixed snapshot lineage must contain exactly one root"
    );
    for (parent, child_ids) in &children {
        ensure!(
            by_id.contains_key(parent),
            "mixed snapshot lineage contains an orphan parent"
        );
        ensure!(
            child_ids.len() == 1,
            "mixed snapshot lineage contains a fork"
        );
    }

    let mut ordered = Vec::with_capacity(snapshots.len());
    let mut current = roots[0];
    let mut seen = BTreeSet::new();
    loop {
        ensure!(
            seen.insert(current),
            "mixed snapshot lineage contains a cycle"
        );
        let snapshot = by_id
            .get(&current)
            .context("mixed snapshot lineage traversal lost a snapshot")?;
        ordered.push(SemanticSnapshotFact {
            ordinal: ordered.len(),
            parent_ordinal: snapshot.parent_id.map(|_| ordered.len() - 1),
            operation: snapshot.operation.trim().to_ascii_lowercase(),
        });
        let Some(child) = children.get(&current).and_then(|children| children.first()) else {
            break;
        };
        current = *child;
    }
    ensure!(
        ordered.len() == snapshots.len(),
        "mixed snapshot lineage contains a detached component"
    );
    Ok(ordered)
}

fn hex_sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(run: u64) -> (Vec<RawFileFact>, Vec<RawSnapshotFact>) {
        (
            vec![
                RawFileFact {
                    content: 0,
                    file_path: format!("s3://private-{run}/a.parquet"),
                    file_format: "PARQUET".into(),
                    spec_id: run as i32,
                    record_count: 5,
                    file_size_in_bytes: 128,
                    partition_shape: PartitionShape::Unpartitioned,
                },
                RawFileFact {
                    content: 0,
                    file_path: format!("s3://private-{run}/b.parquet"),
                    file_format: "PARQUET".into(),
                    spec_id: run as i32,
                    record_count: 5,
                    file_size_in_bytes: 128,
                    partition_shape: PartitionShape::Unpartitioned,
                },
            ],
            vec![
                RawSnapshotFact {
                    snapshot_id: (run * 10) as i64,
                    parent_id: None,
                    operation: "append".into(),
                },
                RawSnapshotFact {
                    snapshot_id: (run * 10 + 1) as i64,
                    parent_id: Some((run * 10) as i64),
                    operation: "append".into(),
                },
            ],
        )
    }

    #[test]
    fn run_specific_physical_values_normalize_to_one_semantic_identity() {
        let (files_a, snapshots_a) = raw(1);
        let (files_b, snapshots_b) = raw(2);
        let a = freeze_table_identity(
            "cat.run_a.table".into(),
            "window/0/analyze/0".into(),
            0,
            Some(BusinessKind::Analyze),
            Some(0),
            10,
            55,
            files_a,
            snapshots_a,
        )
        .unwrap();
        let b = freeze_table_identity(
            "cat.run_b.table".into(),
            "window/0/analyze/0".into(),
            0,
            Some(BusinessKind::Analyze),
            Some(0),
            10,
            55,
            files_b,
            snapshots_b,
        )
        .unwrap();
        assert_eq!(a.semantic, b.semantic);
        assert_ne!(a.raw_bundle_sha256, b.raw_bundle_sha256);
        let semantic = serde_json::to_string(&a.semantic).unwrap();
        for raw_key in [
            "qualified_name",
            "file_path",
            "spec_id",
            "snapshot_id",
            "parent_id",
        ] {
            assert!(!semantic.contains(raw_key));
        }
        assert!(!semantic.contains("private-1"));
        assert!(!semantic.contains("cat.run_a.table"));
    }

    #[test]
    fn semantic_file_facts_preserve_multiset_cardinality() {
        let (files, snapshots) = raw(1);
        let identity = freeze_table_identity(
            "cat.run.table".into(),
            "window/0/foreground".into(),
            0,
            None,
            None,
            10,
            55,
            files,
            snapshots,
        )
        .unwrap();
        assert_eq!(identity.semantic.files.len(), 2);
        assert_eq!(identity.semantic.files[0], identity.semantic.files[1]);
    }

    #[test]
    fn provider_row_order_does_not_change_raw_or_semantic_identity() {
        let (files, snapshots) = raw(1);
        let mut reversed_files = files.clone();
        reversed_files.reverse();
        let mut reversed_snapshots = snapshots.clone();
        reversed_snapshots.reverse();
        let freeze = |files, snapshots| {
            freeze_table_identity(
                "cat.run.table".into(),
                "window/0/foreground".into(),
                0,
                None,
                None,
                10,
                55,
                files,
                snapshots,
            )
            .unwrap()
        };
        let ordered = freeze(files, snapshots);
        let reversed = freeze(reversed_files, reversed_snapshots);
        assert_eq!(ordered.raw_bundle_sha256, reversed.raw_bundle_sha256);
        assert_eq!(ordered.semantic, reversed.semantic);
    }

    #[test]
    fn malformed_snapshot_graphs_fail_closed() {
        let (files, snapshots) = raw(1);
        for invalid in [
            vec![snapshots[0].clone(), snapshots[0].clone()],
            vec![
                snapshots[0].clone(),
                RawSnapshotFact {
                    snapshot_id: 12,
                    parent_id: Some(10),
                    operation: "append".into(),
                },
                snapshots[1].clone(),
            ],
            vec![RawSnapshotFact {
                snapshot_id: 11,
                parent_id: Some(99),
                operation: "append".into(),
            }],
            vec![
                snapshots[0].clone(),
                RawSnapshotFact {
                    snapshot_id: 20,
                    parent_id: None,
                    operation: "append".into(),
                },
            ],
        ] {
            assert!(
                freeze_table_identity(
                    "cat.run.table".into(),
                    "window/0/foreground".into(),
                    0,
                    None,
                    None,
                    10,
                    55,
                    files.clone(),
                    invalid,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn empty_and_duplicate_file_facts_fail_closed() {
        let (files, snapshots) = raw(1);
        assert!(
            freeze_table_identity(
                "cat.run.table".into(),
                "window/0/foreground".into(),
                0,
                None,
                None,
                10,
                55,
                Vec::new(),
                snapshots.clone(),
            )
            .is_err()
        );
        let mut duplicate = files.clone();
        duplicate[1].file_path = duplicate[0].file_path.clone();
        assert!(
            freeze_table_identity(
                "cat.run.table".into(),
                "window/0/foreground".into(),
                0,
                None,
                None,
                10,
                55,
                duplicate,
                snapshots,
            )
            .is_err()
        );
    }

    #[test]
    fn window_rejects_duplicate_jobs_and_wrong_initial_state() {
        let (foreground_files, foreground_snapshots) = raw(1);
        let foreground = freeze_table_identity(
            "cat.run.foreground".into(),
            "window/0/foreground".into(),
            0,
            None,
            None,
            10,
            55,
            foreground_files,
            foreground_snapshots,
        )
        .unwrap();
        let (job_files, job_snapshots) = raw(2);
        let job_input = freeze_table_identity(
            "cat.run.job".into(),
            "window/0/analyze/0".into(),
            0,
            Some(BusinessKind::Analyze),
            Some(0),
            10,
            55,
            job_files,
            job_snapshots,
        )
        .unwrap();
        let job = PreparedJobIdentity {
            kind: BusinessKind::Analyze,
            ordinal: 0,
            input: job_input,
            initial_state: BusinessInitialState::Analyze {
                matching_jobs: 0,
                theta_statistics_available: false,
            },
        };
        assert!(
            PreparedWindowIdentity::try_new(0, foreground.clone(), vec![job.clone(), job]).is_err()
        );
        assert!(
            PreparedWindowIdentity::try_new(
                0,
                foreground,
                vec![PreparedJobIdentity {
                    kind: BusinessKind::Analyze,
                    ordinal: 0,
                    input: freeze_table_identity(
                        "cat.run.job2".into(),
                        "window/0/analyze/0".into(),
                        0,
                        Some(BusinessKind::Analyze),
                        Some(0),
                        10,
                        55,
                        raw(3).0,
                        raw(3).1,
                    )
                    .unwrap(),
                    initial_state: BusinessInitialState::Analyze {
                        matching_jobs: 1,
                        theta_statistics_available: false,
                    },
                }]
            )
            .is_err()
        );
    }
}
