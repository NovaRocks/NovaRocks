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

use rusqlite::{Connection, OptionalExtension, params};

use novarocks_spi::state_store::{StateStoreError, StateStoreErrorKind};

use super::{SqliteHistoryRetentionConfig, schema, sqlite_error};

pub(super) fn maintain_after_commit(
    connection: &Connection,
    policy: &SqliteHistoryRetentionConfig,
    revision: u64,
    now_ms: i64,
) -> Result<bool, StateStoreError> {
    let change_rows = count_rows(connection, "state_store_changes")?;
    let commit_rows = count_rows(connection, "state_store_commits")?;
    let capacity_exceeded = change_rows > policy.max_change_rows as u64
        || commit_rows > policy.max_commit_receipts as u64;
    let age_sweep = revision % policy.maintenance_interval_commits as u64 == 0;
    if !capacity_exceeded && !age_sweep {
        update_u64(connection, schema::CHANGE_ROW_COUNT_KEY, change_rows)?;
        update_u64(connection, schema::COMMIT_RECEIPT_COUNT_KEY, commit_rows)?;
        return Ok(false);
    }

    let age_cutoff_ms = now_ms.saturating_sub(
        i64::try_from(policy.max_age_secs)
            .unwrap_or(i64::MAX)
            .saturating_mul(1000),
    );
    let prune_age = age_sweep;
    let deleted_change_position =
        select_last_deleted_change(connection, prune_age, age_cutoff_ms, policy.max_change_rows)?;
    let deleted_changes =
        delete_changes(connection, prune_age, age_cutoff_ms, policy.max_change_rows)?;
    if let Some(position) = deleted_change_position {
        advance_change_floor(connection, position)?;
    }

    let deleted_receipts = select_deleted_receipts(
        connection,
        prune_age,
        age_cutoff_ms,
        policy.max_commit_receipts,
    )?;
    if !deleted_receipts.is_empty() {
        delete_receipts(
            connection,
            prune_age,
            age_cutoff_ms,
            policy.max_commit_receipts,
        )?;
        merge_retired_transaction_bounds(connection, &deleted_receipts)?;
    }

    let change_rows = count_rows(connection, "state_store_changes")?;
    let commit_rows = count_rows(connection, "state_store_commits")?;
    update_u64(connection, schema::CHANGE_ROW_COUNT_KEY, change_rows)?;
    update_u64(connection, schema::COMMIT_RECEIPT_COUNT_KEY, commit_rows)?;
    update_u64(
        connection,
        schema::LAST_HISTORY_MAINTENANCE_MS_KEY,
        now_ms.max(0) as u64,
    )?;
    let reclaim_pending = deleted_changes > 0 || !deleted_receipts.is_empty();
    if reclaim_pending {
        update_bytes(connection, schema::PHYSICAL_RECLAIM_PENDING_KEY, &[1])?;
    }
    Ok(reclaim_pending)
}

pub(super) fn reclaim_pending_on_open(
    connection: &Connection,
    policy: &SqliteHistoryRetentionConfig,
) -> Result<(), StateStoreError> {
    if load_required(connection, schema::PHYSICAL_RECLAIM_PENDING_KEY)?.as_slice() == [0] {
        return Ok(());
    }
    physical_reclaim(connection, policy)?;
    update_bytes(connection, schema::PHYSICAL_RECLAIM_PENDING_KEY, &[0])
}

pub(super) fn reclaim_after_commit(
    connection: &Connection,
    policy: &SqliteHistoryRetentionConfig,
    pending: bool,
) -> Result<(), StateStoreError> {
    if !pending {
        return Ok(());
    }
    physical_reclaim(connection, policy)?;
    update_bytes(connection, schema::PHYSICAL_RECLAIM_PENDING_KEY, &[0])
}

pub(super) fn transaction_id_is_retired(
    connection: &Connection,
    transaction_id: &[u8; 16],
) -> Result<bool, StateStoreError> {
    let min = load_required(connection, schema::RETIRED_TRANSACTION_ID_MIN_KEY)?;
    let max = load_required(connection, schema::RETIRED_TRANSACTION_ID_MAX_KEY)?;
    match (min.is_empty(), max.is_empty()) {
        (true, true) => Ok(false),
        (false, false) if min.len() == 16 && max.len() == 16 => Ok(min.as_slice()
            <= transaction_id.as_slice()
            && transaction_id.as_slice() <= max.as_slice()),
        _ => Err(corruption(
            "SQLite retired transaction bounds are malformed",
        )),
    }
}

fn physical_reclaim(
    connection: &Connection,
    policy: &SqliteHistoryRetentionConfig,
) -> Result<(), StateStoreError> {
    connection
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::ProviderUnavailable,
                "failed to checkpoint SQLite state store history",
            )
        })?;
    connection
        .execute_batch(&format!(
            "PRAGMA incremental_vacuum({})",
            policy.incremental_vacuum_pages
        ))
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::ProviderUnavailable,
                "failed to reclaim SQLite state store history",
            )
        })
}

fn select_last_deleted_change(
    connection: &Connection,
    prune_age: bool,
    age_cutoff_ms: i64,
    keep_rows: usize,
) -> Result<Option<(u64, u32)>, StateStoreError> {
    let sql = history_predicate_sql(
        "state_store_changes",
        prune_age,
        "revision DESC, sequence DESC",
    );
    connection
        .query_row(
            &format!(
                "SELECT revision, sequence FROM state_store_changes WHERE {sql} \
                 ORDER BY revision DESC, sequence DESC LIMIT 1"
            ),
            params![
                age_cutoff_ms,
                i64::try_from(keep_rows)
                    .map_err(|_| corruption("SQLite retention row limit is out of range"))?
            ],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to inspect SQLite change retention",
            )
        })?
        .map(|(revision, sequence)| {
            Ok((
                u64::try_from(revision)
                    .map_err(|_| corruption("SQLite change revision is malformed"))?,
                u32::try_from(sequence)
                    .map_err(|_| corruption("SQLite change sequence is malformed"))?,
            ))
        })
        .transpose()
}

fn delete_changes(
    connection: &Connection,
    prune_age: bool,
    age_cutoff_ms: i64,
    keep_rows: usize,
) -> Result<usize, StateStoreError> {
    let predicate = history_predicate_sql(
        "state_store_changes",
        prune_age,
        "revision DESC, sequence DESC",
    );
    connection
        .execute(
            &format!("DELETE FROM state_store_changes WHERE {predicate}"),
            params![
                age_cutoff_ms,
                i64::try_from(keep_rows)
                    .map_err(|_| corruption("SQLite retention row limit is out of range"))?
            ],
        )
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to prune SQLite change history",
            )
        })
}

fn select_deleted_receipts(
    connection: &Connection,
    prune_age: bool,
    age_cutoff_ms: i64,
    keep_rows: usize,
) -> Result<Vec<Vec<u8>>, StateStoreError> {
    let predicate = history_predicate_sql(
        "state_store_commits",
        prune_age,
        "committed_at_ms DESC, revision DESC",
    );
    let mut statement = connection
        .prepare(&format!(
            "SELECT transaction_id FROM state_store_commits WHERE {predicate}"
        ))
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to inspect SQLite commit retention",
            )
        })?;
    statement
        .query_map(
            params![
                age_cutoff_ms,
                i64::try_from(keep_rows)
                    .map_err(|_| corruption("SQLite retention row limit is out of range"))?
            ],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to inspect SQLite commit retention",
            )
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to inspect SQLite commit retention",
            )
        })
}

fn delete_receipts(
    connection: &Connection,
    prune_age: bool,
    age_cutoff_ms: i64,
    keep_rows: usize,
) -> Result<(), StateStoreError> {
    let predicate = history_predicate_sql(
        "state_store_commits",
        prune_age,
        "committed_at_ms DESC, revision DESC",
    );
    connection
        .execute(
            &format!("DELETE FROM state_store_commits WHERE {predicate}"),
            params![
                age_cutoff_ms,
                i64::try_from(keep_rows)
                    .map_err(|_| corruption("SQLite retention row limit is out of range"))?
            ],
        )
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to prune SQLite commit receipts",
            )
        })?;
    Ok(())
}

fn history_predicate_sql(table: &str, prune_age: bool, order_by: &str) -> String {
    let capacity = format!("rowid NOT IN (SELECT rowid FROM {table} ORDER BY {order_by} LIMIT ?2)");
    if prune_age {
        format!("committed_at_ms < ?1 OR {capacity}")
    } else {
        capacity
    }
}

fn advance_change_floor(
    connection: &Connection,
    candidate: (u64, u32),
) -> Result<(), StateStoreError> {
    let existing = load_required(connection, schema::CHANGE_RETENTION_FLOOR_KEY)?;
    let existing = decode_change_position(&existing)?;
    if candidate > existing {
        let mut encoded = Vec::with_capacity(12);
        encoded.extend_from_slice(&candidate.0.to_be_bytes());
        encoded.extend_from_slice(&candidate.1.to_be_bytes());
        update_bytes(connection, schema::CHANGE_RETENTION_FLOOR_KEY, &encoded)?;
    }
    Ok(())
}

fn merge_retired_transaction_bounds(
    connection: &Connection,
    deleted: &[Vec<u8>],
) -> Result<(), StateStoreError> {
    let mut min = load_required(connection, schema::RETIRED_TRANSACTION_ID_MIN_KEY)?;
    let mut max = load_required(connection, schema::RETIRED_TRANSACTION_ID_MAX_KEY)?;
    for transaction_id in deleted {
        if transaction_id.len() != 16 {
            return Err(corruption("SQLite commit transaction id is malformed"));
        }
        if min.is_empty() || transaction_id < &min {
            min = transaction_id.clone();
        }
        if max.is_empty() || transaction_id > &max {
            max = transaction_id.clone();
        }
    }
    update_bytes(connection, schema::RETIRED_TRANSACTION_ID_MIN_KEY, &min)?;
    update_bytes(connection, schema::RETIRED_TRANSACTION_ID_MAX_KEY, &max)
}

fn count_rows(connection: &Connection, table: &'static str) -> Result<u64, StateStoreError> {
    let count = connection
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get::<_, i64>(0)
        })
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to count SQLite history rows",
            )
        })?;
    u64::try_from(count).map_err(|_| corruption("SQLite history row count is malformed"))
}

fn update_u64(connection: &Connection, key: &[u8], value: u64) -> Result<(), StateStoreError> {
    update_bytes(connection, key, &value.to_be_bytes())
}

fn update_bytes(connection: &Connection, key: &[u8], value: &[u8]) -> Result<(), StateStoreError> {
    match connection
        .execute(
            "UPDATE state_store_meta SET value = ?1 WHERE key = ?2",
            params![value, key],
        )
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to update SQLite history metadata",
            )
        })? {
        1 => Ok(()),
        _ => Err(corruption("SQLite history metadata is missing")),
    }
}

fn load_required(connection: &Connection, key: &[u8]) -> Result<Vec<u8>, StateStoreError> {
    connection
        .query_row(
            "SELECT value FROM state_store_meta WHERE key = ?1",
            params![key],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .optional()
        .map_err(|error| {
            sqlite_error(
                &error,
                StateStoreErrorKind::Corruption,
                "failed to read SQLite history metadata",
            )
        })?
        .ok_or_else(|| corruption("SQLite history metadata is missing"))
}

fn decode_change_position(value: &[u8]) -> Result<(u64, u32), StateStoreError> {
    let bytes: [u8; 12] = value
        .try_into()
        .map_err(|_| corruption("SQLite change retention floor is malformed"))?;
    Ok((
        u64::from_be_bytes(bytes[..8].try_into().expect("fixed revision bytes")),
        u32::from_be_bytes(bytes[8..].try_into().expect("fixed sequence bytes")),
    ))
}

const fn corruption(message: &'static str) -> StateStoreError {
    StateStoreError::new(StateStoreErrorKind::Corruption, message)
}
