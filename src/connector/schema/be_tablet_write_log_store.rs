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
use std::collections::VecDeque;
use std::sync::{Mutex, OnceLock};

use crate::common::config;

use super::SchemaScanContext;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BeTabletWriteLogType {
    Load,
    Compaction,
}

impl BeTabletWriteLogType {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Load => "LOAD",
            Self::Compaction => "COMPACTION",
        }
    }

    fn parse_filter(value: Option<&str>) -> Option<Self> {
        match value {
            Some(raw) if raw.eq_ignore_ascii_case("LOAD") => Some(Self::Load),
            Some(raw) if raw.eq_ignore_ascii_case("COMPACTION") => Some(Self::Compaction),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct BeTabletWriteLogEntry {
    pub(crate) backend_id: i64,
    pub(crate) begin_time_ms: i64,
    pub(crate) finish_time_ms: i64,
    pub(crate) txn_id: i64,
    pub(crate) tablet_id: i64,
    pub(crate) table_id: i64,
    pub(crate) partition_id: i64,
    pub(crate) log_type: BeTabletWriteLogType,
    pub(crate) input_rows: i64,
    pub(crate) input_bytes: i64,
    pub(crate) output_rows: i64,
    pub(crate) output_bytes: i64,
    pub(crate) input_segments: Option<i32>,
    pub(crate) output_segments: i32,
    pub(crate) label: Option<String>,
    pub(crate) compaction_score: Option<i64>,
    pub(crate) compaction_type: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct BeTabletWriteLoadLogRecord {
    pub(crate) backend_id: i64,
    pub(crate) begin_time_ms: i64,
    pub(crate) finish_time_ms: i64,
    pub(crate) txn_id: i64,
    pub(crate) tablet_id: i64,
    pub(crate) table_id: i64,
    pub(crate) partition_id: i64,
    pub(crate) input_rows: i64,
    pub(crate) input_bytes: i64,
    pub(crate) output_rows: i64,
    pub(crate) output_bytes: i64,
    pub(crate) output_segments: i32,
    pub(crate) label: Option<String>,
}

#[derive(Default)]
struct BeTabletWriteLogStore {
    entries: VecDeque<BeTabletWriteLogEntry>,
}

static STORE: OnceLock<Mutex<BeTabletWriteLogStore>> = OnceLock::new();

fn store() -> &'static Mutex<BeTabletWriteLogStore> {
    STORE.get_or_init(|| Mutex::new(BeTabletWriteLogStore::default()))
}

fn enable_tablet_write_log() -> bool {
    #[cfg(test)]
    if let Some(options) = test_options() {
        return options.enable;
    }
    config::enable_tablet_write_log()
}

fn tablet_write_log_buffer_size() -> usize {
    #[cfg(test)]
    if let Some(options) = test_options() {
        return options.buffer_size.max(1);
    }
    config::tablet_write_log_buffer_size().max(1)
}

fn trim_to_capacity(entries: &mut VecDeque<BeTabletWriteLogEntry>) {
    let max_size = tablet_write_log_buffer_size();
    while entries.len() > max_size {
        let _ = entries.pop_front();
    }
}

pub(crate) fn record_load(record: BeTabletWriteLoadLogRecord) {
    if !enable_tablet_write_log() {
        return;
    }
    let entry = BeTabletWriteLogEntry {
        backend_id: record.backend_id,
        begin_time_ms: record.begin_time_ms,
        finish_time_ms: record.finish_time_ms,
        txn_id: record.txn_id,
        tablet_id: record.tablet_id,
        table_id: record.table_id,
        partition_id: record.partition_id,
        log_type: BeTabletWriteLogType::Load,
        input_rows: record.input_rows.max(0),
        input_bytes: record.input_bytes.max(0),
        output_rows: record.output_rows.max(0),
        output_bytes: record.output_bytes.max(0),
        input_segments: None,
        output_segments: record.output_segments.max(0),
        label: record.label.filter(|value| !value.is_empty()),
        compaction_score: None,
        compaction_type: None,
    };
    let mut guard = store().lock().expect("tablet write log store lock");
    guard.entries.push_back(entry);
    trim_to_capacity(&mut guard.entries);
}

pub(crate) fn snapshot(ctx: &SchemaScanContext) -> Vec<BeTabletWriteLogEntry> {
    if !enable_tablet_write_log() {
        return Vec::new();
    }
    let expected_log_type = BeTabletWriteLogType::parse_filter(ctx.type_.as_deref());
    let guard = store().lock().expect("tablet write log store lock");
    guard
        .entries
        .iter()
        .filter(|entry| {
            if let Some(table_id) = ctx.table_id
                && entry.table_id != table_id
            {
                return false;
            }
            if let Some(partition_id) = ctx.partition_id
                && entry.partition_id != partition_id
            {
                return false;
            }
            if let Some(tablet_id) = ctx.tablet_id
                && entry.tablet_id != tablet_id
            {
                return false;
            }
            if let Some(txn_id) = ctx.txn_id
                && entry.txn_id != txn_id
            {
                return false;
            }
            if let Some(log_type) = expected_log_type
                && entry.log_type != log_type
            {
                return false;
            }
            if let Some(start_finish_time) = ctx.log_start_ts
                && entry.finish_time_ms < start_finish_time
            {
                return false;
            }
            if let Some(end_finish_time) = ctx.log_end_ts
                && entry.finish_time_ms > end_finish_time
            {
                return false;
            }
            true
        })
        .cloned()
        .collect()
}

#[cfg(test)]
pub(crate) fn clear_for_test() {
    let mut guard = store().lock().expect("tablet write log store lock");
    guard.entries.clear();
    set_test_options(None);
}

#[cfg(test)]
#[derive(Clone, Debug)]
struct TestOptions {
    enable: bool,
    buffer_size: usize,
}

#[cfg(test)]
static TEST_OPTIONS: OnceLock<Mutex<Option<TestOptions>>> = OnceLock::new();

#[cfg(test)]
fn test_options_store() -> &'static Mutex<Option<TestOptions>> {
    TEST_OPTIONS.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn test_options() -> Option<TestOptions> {
    test_options_store()
        .lock()
        .expect("tablet write log test options lock")
        .clone()
}

#[cfg(test)]
fn set_test_options(options: Option<TestOptions>) {
    *test_options_store()
        .lock()
        .expect("tablet write log test options lock") = options;
}

#[cfg(test)]
pub(crate) fn set_options_for_test(enable: bool, buffer_size: usize) {
    set_test_options(Some(TestOptions {
        enable,
        buffer_size: buffer_size.max(1),
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::schema;

    fn test_context() -> SchemaScanContext {
        SchemaScanContext {
            table_name: "be_tablet_write_log".to_string(),
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

    fn record(txn_id: i64, table_id: i64, partition_id: i64, tablet_id: i64, finish_time_ms: i64) {
        record_load(BeTabletWriteLoadLogRecord {
            backend_id: 9001,
            begin_time_ms: finish_time_ms.saturating_sub(1),
            finish_time_ms,
            txn_id,
            tablet_id,
            table_id,
            partition_id,
            input_rows: 10,
            input_bytes: 100,
            output_rows: 10,
            output_bytes: 100,
            output_segments: 1,
            label: None,
        });
    }

    #[test]
    fn ring_buffer_respects_capacity() {
        let _guard = schema::test_lock().lock().expect("schema scan test lock");
        clear_for_test();
        set_options_for_test(true, 2);
        record(1, 10, 20, 30, 1001);
        record(2, 10, 20, 30, 1002);
        record(3, 10, 20, 30, 1003);
        let logs = snapshot(&test_context());
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0].txn_id, 2);
        assert_eq!(logs[1].txn_id, 3);
        clear_for_test();
    }

    #[test]
    fn collection_switch_disables_snapshot_and_record() {
        let _guard = schema::test_lock().lock().expect("schema scan test lock");
        clear_for_test();
        set_options_for_test(false, 16);
        record(1, 10, 20, 30, 1001);
        assert!(snapshot(&test_context()).is_empty());
        clear_for_test();
    }

    #[test]
    fn snapshot_applies_parameter_filters() {
        let _guard = schema::test_lock().lock().expect("schema scan test lock");
        clear_for_test();
        set_options_for_test(true, 16);
        record(101, 5001, 6001, 7001, 2001);
        record(102, 5002, 6002, 7002, 2002);
        let mut ctx = test_context();
        ctx.table_id = Some(5001);
        ctx.partition_id = Some(6001);
        ctx.tablet_id = Some(7001);
        ctx.txn_id = Some(101);
        ctx.log_start_ts = Some(2000);
        ctx.log_end_ts = Some(3000);
        let logs = snapshot(&ctx);
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].txn_id, 101);
        clear_for_test();
    }
}
