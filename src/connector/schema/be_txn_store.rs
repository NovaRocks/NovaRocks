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
use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};

use crate::common::config;
use crate::common::types::format_uuid;

use super::SchemaScanContext;

#[derive(Clone, Debug)]
pub(crate) struct BeTxnEntry {
    pub(crate) backend_id: i64,
    pub(crate) load_id: String,
    pub(crate) txn_id: i64,
    pub(crate) partition_id: i64,
    pub(crate) tablet_id: i64,
    pub(crate) create_time: i64,
    pub(crate) commit_time: i64,
    pub(crate) publish_time: i64,
    pub(crate) rowset_id: String,
    pub(crate) num_segment: i64,
    pub(crate) num_delfile: i64,
    pub(crate) num_row: i64,
    pub(crate) data_size: i64,
    pub(crate) version: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct BeTxnActiveRecord {
    pub(crate) backend_id: i64,
    pub(crate) load_id_hi: Option<i64>,
    pub(crate) load_id_lo: Option<i64>,
    pub(crate) txn_id: i64,
    pub(crate) partition_id: i64,
    pub(crate) tablet_id: i64,
    pub(crate) rowset_id: Option<String>,
    pub(crate) num_segment: i64,
    pub(crate) num_delfile: i64,
    pub(crate) num_row: i64,
    pub(crate) data_size: i64,
}

#[derive(Default)]
struct BeTxnStore {
    active: HashMap<(i64, i64), BeTxnEntry>,
    history: VecDeque<BeTxnEntry>,
}

static STORE: OnceLock<Mutex<BeTxnStore>> = OnceLock::new();

fn store() -> &'static Mutex<BeTxnStore> {
    STORE.get_or_init(|| Mutex::new(BeTxnStore::default()))
}

fn trim_history(history: &mut VecDeque<BeTxnEntry>) {
    let max_size = be_txn_info_history_size();
    while history.len() > max_size {
        let _ = history.pop_front();
    }
}

fn be_txn_info_history_size() -> usize {
    #[cfg(test)]
    if let Some(size) = history_size_override_for_test() {
        return size.max(1);
    }
    config::be_txn_info_history_size().max(1)
}

fn load_id_to_string(hi: Option<i64>, lo: Option<i64>) -> String {
    match (hi, lo) {
        (Some(hi), Some(lo)) => format_uuid(hi, lo),
        _ => format_uuid(0, 0),
    }
}

pub(crate) fn record_active(record: BeTxnActiveRecord) {
    if record.txn_id <= 0 || record.tablet_id <= 0 {
        return;
    }
    let now = unix_seconds_now();
    let key = (record.txn_id, record.tablet_id);
    let mut guard = store().lock().expect("be txn store lock");
    let entry = guard.active.entry(key).or_insert_with(|| BeTxnEntry {
        backend_id: record.backend_id,
        load_id: load_id_to_string(record.load_id_hi, record.load_id_lo),
        txn_id: record.txn_id,
        partition_id: record.partition_id.max(0),
        tablet_id: record.tablet_id,
        create_time: now,
        commit_time: now,
        publish_time: 0,
        rowset_id: String::new(),
        num_segment: 0,
        num_delfile: 0,
        num_row: 0,
        data_size: 0,
        version: 0,
    });
    entry.backend_id = record.backend_id;
    if record.partition_id > 0 {
        entry.partition_id = record.partition_id;
    }
    if record.load_id_hi.is_some() && record.load_id_lo.is_some() {
        entry.load_id = load_id_to_string(record.load_id_hi, record.load_id_lo);
    }
    entry.commit_time = now;
    entry.rowset_id = record.rowset_id.unwrap_or_default();
    entry.num_segment = record.num_segment.max(0);
    entry.num_delfile = record.num_delfile.max(0);
    entry.num_row = record.num_row.max(0);
    entry.data_size = record.data_size.max(0);
}

pub(crate) fn mark_published(txn_id: i64, tablet_id: i64, publish_time: i64, version: i64) {
    if txn_id <= 0 || tablet_id <= 0 {
        return;
    }
    let mut guard = store().lock().expect("be txn store lock");
    let Some(mut entry) = guard.active.remove(&(txn_id, tablet_id)) else {
        return;
    };
    entry.publish_time = publish_time.max(0);
    entry.version = version.max(0);
    guard.history.push_back(entry);
    trim_history(&mut guard.history);
}

pub(crate) fn abort_active(txn_id: i64, tablet_id: i64) {
    if txn_id <= 0 || tablet_id <= 0 {
        return;
    }
    let mut guard = store().lock().expect("be txn store lock");
    guard.active.remove(&(txn_id, tablet_id));
}

pub(crate) fn snapshot(ctx: &SchemaScanContext) -> Vec<BeTxnEntry> {
    let guard = store().lock().expect("be txn store lock");
    let mut out = Vec::with_capacity(guard.active.len() + guard.history.len());
    for entry in guard.active.values() {
        if matches_filter(entry, ctx) {
            out.push(entry.clone());
        }
    }
    for entry in &guard.history {
        if matches_filter(entry, ctx) {
            out.push(entry.clone());
        }
    }
    out.sort_unstable_by_key(|entry| {
        (
            entry.txn_id,
            entry.tablet_id,
            entry.partition_id,
            entry.create_time,
            entry.publish_time,
            entry.version,
            entry.rowset_id.clone(),
        )
    });
    out
}

fn matches_filter(entry: &BeTxnEntry, ctx: &SchemaScanContext) -> bool {
    if let Some(txn_id) = ctx.txn_id
        && entry.txn_id != txn_id
    {
        return false;
    }
    if let Some(tablet_id) = ctx.tablet_id
        && entry.tablet_id != tablet_id
    {
        return false;
    }
    true
}

fn unix_seconds_now() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let Ok(duration) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return 0;
    };
    i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
}

#[cfg(test)]
pub(crate) fn clear_for_test() {
    let mut guard = store().lock().expect("be txn store lock");
    guard.active.clear();
    guard.history.clear();
    set_history_size_override_for_test(None);
}

#[cfg(test)]
static TEST_HISTORY_SIZE: OnceLock<Mutex<Option<usize>>> = OnceLock::new();

#[cfg(test)]
fn history_size_override_store() -> &'static Mutex<Option<usize>> {
    TEST_HISTORY_SIZE.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn history_size_override_for_test() -> Option<usize> {
    *history_size_override_store()
        .lock()
        .expect("be txn history override lock")
}

#[cfg(test)]
fn set_history_size_override_for_test(value: Option<usize>) {
    *history_size_override_store()
        .lock()
        .expect("be txn history override lock") = value;
}

#[cfg(test)]
pub(crate) fn set_history_size_for_test(size: usize) {
    set_history_size_override_for_test(Some(size.max(1)));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::schema;

    fn ctx() -> SchemaScanContext {
        SchemaScanContext {
            table_name: "be_txns".to_string(),
            db: None,
            table: None,
            wild: None,
            user: None,
            ip: None,
            port: None,
            thread_id: None,
            user_ip: None,
            current_user_ident: None,
            catalog_name: None,
            table_id: None,
            partition_id: None,
            tablet_id: None,
            txn_id: None,
            job_id: None,
            label: None,
            type_: None,
            state: None,
            limit: None,
            log_start_ts: None,
            log_end_ts: None,
            log_level: None,
            log_pattern: None,
            log_limit: None,
            frontends: Vec::new(),
        }
    }

    fn active(txn_id: i64, tablet_id: i64, rowset_id: &str) {
        record_active(BeTxnActiveRecord {
            backend_id: 11,
            load_id_hi: Some(1),
            load_id_lo: Some(2),
            txn_id,
            partition_id: 3,
            tablet_id,
            rowset_id: Some(rowset_id.to_string()),
            num_segment: 1,
            num_delfile: 0,
            num_row: 10,
            data_size: 100,
        });
    }

    #[test]
    fn active_entry_moves_to_history_on_publish() {
        let _guard = schema::test_lock().lock().expect("schema scan test lock");
        clear_for_test();
        active(101, 1001, "r1");
        mark_published(101, 1001, 2000, 77);
        let rows = snapshot(&ctx());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].txn_id, 101);
        assert_eq!(rows[0].tablet_id, 1001);
        assert_eq!(rows[0].publish_time, 2000);
        assert_eq!(rows[0].version, 77);
        clear_for_test();
    }

    #[test]
    fn history_truncates_by_configured_size() {
        let _guard = schema::test_lock().lock().expect("schema scan test lock");
        clear_for_test();
        set_history_size_for_test(1);
        active(201, 2001, "r201");
        mark_published(201, 2001, 3001, 1);
        active(202, 2002, "r202");
        mark_published(202, 2002, 3002, 2);
        let rows = snapshot(&ctx());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].txn_id, 202);
        clear_for_test();
    }

    #[test]
    fn snapshot_supports_txn_and_tablet_filters() {
        let _guard = schema::test_lock().lock().expect("schema scan test lock");
        clear_for_test();
        active(301, 3001, "r301");
        active(302, 3002, "r302");
        let mut filter_ctx = ctx();
        filter_ctx.txn_id = Some(302);
        filter_ctx.tablet_id = Some(3002);
        let rows = snapshot(&filter_ctx);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].txn_id, 302);
        assert_eq!(rows[0].tablet_id, 3002);
        clear_for_test();
    }
}
