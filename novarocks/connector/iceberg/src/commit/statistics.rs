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

//! Shared metadata-only commit for an iceberg Puffin StatisticsFile.
//! Design: ADR-0082 (docs/adr/ADR-0082-same-snapshot-statistics-publication-arbitration.md)

use std::fmt;

use crate::iceberg::spec::StatisticsFile;
use crate::iceberg::table::Table;
use crate::iceberg::transaction::{ApplyTransactionAction, Transaction};
use crate::stats_assembler::StatisticsCoverageMark;

/// How many times a losing commit race is retried before giving up.
///
/// Statistics commits are metadata-only and cheap to redo, but they are also
/// never worth blocking on: the data they describe is already published.
const MAX_STATISTICS_COMMIT_ATTEMPTS: usize = 5;

/// What a publication attempt did to the table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsCommitOutcome {
    /// The entry is now registered against its snapshot.
    Registered,
    /// A more complete entry already covers this snapshot, so this one stood
    /// down. Not a failure — an incremental union has nothing to add to a full
    /// scan of the same rows.
    YieldedToFullerCoverage,
}

/// Why a registration attempt did not produce authoritative proof.
#[derive(Debug)]
pub enum StatisticsRegistrationFailure {
    Commit(String),
    Unknown(String),
}

impl fmt::Display for StatisticsRegistrationFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Commit(error) | Self::Unknown(error) => formatter.write_str(error),
        }
    }
}

/// Registers `stats_file`, deciding against whatever currently occupies its
/// snapshot and re-deciding after every lost race.
///
/// Iceberg keeps one statistics file per snapshot and `set_statistics`
/// replaces it, so two writers aiming at the same snapshot need an order. A
/// full visible-row scan outranks an incremental union; between equals the
/// later commit wins, since they describe the same rows and the newer one is
/// simply fresher evidence.
///
/// The re-decision after a conflict is the point: a blind retry would let an
/// incremental result overwrite a full scan that landed in between, which is
/// exactly the race the ranking exists to prevent.
pub async fn commit_statistics_file(
    table: &Table,
    catalog: &dyn crate::iceberg::Catalog,
    stats_file: StatisticsFile,
    coverage: StatisticsCoverageMark,
) -> Result<StatisticsCommitOutcome, StatisticsRegistrationFailure> {
    let mut table = table.clone();
    for attempt in 0..MAX_STATISTICS_COMMIT_ATTEMPTS {
        if !supersedes_registered_entry(&table, &stats_file, coverage) {
            return Ok(StatisticsCommitOutcome::YieldedToFullerCoverage);
        }
        let tx = Transaction::new(&table);
        let action = tx.update_statistics().set_statistics(stats_file.clone());
        let tx = action.apply(tx).await.map_err(|e| {
            StatisticsRegistrationFailure::Commit(format!(
                "iceberg update_statistics apply failed: {e}"
            ))
        })?;
        let commit_result = tx.commit(catalog).await;

        // A commit response is not the publication receipt. Catalogs can lose
        // the response after applying the update, and another writer can
        // replace this snapshot's singleton statistics entry immediately after
        // a successful response. Always reload the catalog's authoritative
        // metadata and make the exact path the registration proof.
        let reloaded = crate::iceberg::Catalog::load_table(catalog, table.identifier())
            .await
            .map_err(|reload| match &commit_result {
                Ok(_) => StatisticsRegistrationFailure::Unknown(format!(
                    "iceberg update_statistics commit succeeded, but reloading the table to \
                     confirm statistics registration failed: {reload}"
                )),
                Err(error) => StatisticsRegistrationFailure::Unknown(format!(
                    "iceberg update_statistics commit failed ({error}); reloading the table to \
                     confirm statistics registration also failed: {reload}"
                )),
            })?;

        match decide_registration_attempt(
            commit_result.map(|_| ()),
            is_exact_statistics_path_registered(&reloaded, &stats_file),
            attempt + 1 == MAX_STATISTICS_COMMIT_ATTEMPTS,
            &stats_file.statistics_path,
        ) {
            Ok(RegistrationAttemptDecision::Registered) => {
                return Ok(StatisticsCommitOutcome::Registered);
            }
            Ok(RegistrationAttemptDecision::Retry) => {}
            Err(error) => return Err(error),
        }

        // Re-arbitrate against the authoritative winner before a retry. This
        // may correctly yield to a fuller entry instead of blindly replacing
        // it.
        table = reloaded;
    }
    Err(StatisticsRegistrationFailure::Unknown(
        "iceberg update_statistics exhausted its commit attempts".to_string(),
    ))
}

/// Decide a commit attempt only after authoritative metadata has been loaded.
/// A failed response is still successful when that metadata names the exact
/// candidate path, because the catalog may have persisted before losing its
/// response.
enum RegistrationAttemptDecision {
    Registered,
    Retry,
}

fn decide_registration_attempt(
    commit_result: Result<(), crate::iceberg::Error>,
    candidate_registered: bool,
    final_attempt: bool,
    candidate_path: &str,
) -> Result<RegistrationAttemptDecision, StatisticsRegistrationFailure> {
    if candidate_registered {
        return Ok(RegistrationAttemptDecision::Registered);
    }
    if !final_attempt {
        return Ok(RegistrationAttemptDecision::Retry);
    }
    match commit_result {
        Err(error) => Err(StatisticsRegistrationFailure::Commit(format!(
            "iceberg update_statistics commit failed: {error}"
        ))),
        Ok(()) => Err(StatisticsRegistrationFailure::Unknown(format!(
            "iceberg update_statistics commit completed, but authoritative table metadata did not \
             register statistics path {candidate_path}"
        ))),
    }
}

/// Whether `candidate` should replace whatever is registered for its snapshot.
fn supersedes_registered_entry(
    table: &Table,
    candidate: &StatisticsFile,
    coverage: StatisticsCoverageMark,
) -> bool {
    let Some(registered) = table
        .metadata()
        .statistics_for_snapshot(candidate.snapshot_id)
    else {
        return true;
    };
    if registered.statistics_path == candidate.statistics_path {
        // Re-registering the identical artifact, e.g. a reconcile after an
        // uncertain commit.
        return true;
    }
    match (StatisticsCoverageMark::of(registered), coverage) {
        // Only this pairing stands down; everything else is same-rank, where
        // the later writer is the fresher evidence.
        (StatisticsCoverageMark::AllVisibleRows, StatisticsCoverageMark::IncrementalUnion) => false,
        _ => true,
    }
}

/// Whether the catalog's currently loaded metadata contains this exact
/// singleton statistics entry for the candidate snapshot.
fn is_exact_statistics_path_registered(table: &Table, candidate: &StatisticsFile) -> bool {
    statistics_path_matches(
        table
            .metadata()
            .statistics_for_snapshot(candidate.snapshot_id),
        candidate,
    )
}

fn statistics_path_matches(
    registered: Option<&StatisticsFile>,
    candidate: &StatisticsFile,
) -> bool {
    registered.is_some_and(|registered| registered.statistics_path == candidate.statistics_path)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::iceberg::spec::{BlobMetadata, StatisticsFile};
    use crate::stats_assembler::{
        STATISTICS_COVERAGE_ALL_VISIBLE_ROWS, STATISTICS_COVERAGE_PROPERTY,
    };

    use super::*;

    fn entry(path: &str, coverage: Option<&str>) -> StatisticsFile {
        StatisticsFile {
            snapshot_id: 7,
            statistics_path: path.to_string(),
            file_size_in_bytes: 1,
            file_footer_size_in_bytes: 1,
            key_metadata: None,
            blob_metadata: vec![BlobMetadata {
                r#type: "apache-datasketches-theta-v1".to_string(),
                snapshot_id: 7,
                sequence_number: 1,
                fields: vec![1],
                properties: coverage
                    .map(|value| {
                        HashMap::from([(
                            STATISTICS_COVERAGE_PROPERTY.to_string(),
                            value.to_string(),
                        )])
                    })
                    .unwrap_or_default(),
            }],
        }
    }

    #[test]
    fn an_entry_without_the_marker_reads_as_incremental() {
        // Conservative by design: the unknown side is the one that yields.
        assert_eq!(
            StatisticsCoverageMark::of(&entry("s.puffin", None)),
            StatisticsCoverageMark::IncrementalUnion
        );
        assert_eq!(
            StatisticsCoverageMark::of(&entry(
                "s.puffin",
                Some(STATISTICS_COVERAGE_ALL_VISIBLE_ROWS)
            )),
            StatisticsCoverageMark::AllVisibleRows
        );
    }

    #[test]
    fn exact_registration_requires_the_candidate_path() {
        let candidate = entry("candidate.puffin", None);
        let other = entry("other.puffin", None);

        assert!(statistics_path_matches(Some(&candidate), &candidate));
        assert!(!statistics_path_matches(Some(&other), &candidate));
        assert!(!statistics_path_matches(None, &candidate));
    }

    #[test]
    fn response_loss_is_registered_when_reloaded_metadata_proves_the_path() {
        let response_loss = crate::iceberg::Error::new(
            crate::iceberg::ErrorKind::Unexpected,
            "response lost after commit",
        );

        assert!(matches!(
            decide_registration_attempt(Err(response_loss), true, false, "candidate.puffin"),
            Ok(RegistrationAttemptDecision::Registered)
        ));
    }
}
