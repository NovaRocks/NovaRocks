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
mod be_compaction_stats_store;
mod be_tablet_write_log_store;
mod be_txn_store;
mod chunk_builder;
mod context;
mod loads;
mod op;

pub(crate) use be_tablet_write_log_store::BeTabletWriteLoadLogRecord;
pub(crate) use be_txn_store::BeTxnActiveRecord;
pub(crate) use context::SchemaScanContext;
pub(crate) use op::SchemaScanOp;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum BeSchemaTable {
    TabletWriteLog,
    Txns,
    Compactions,
    Unsupported(String),
}

impl BeSchemaTable {
    pub(crate) fn from_table_name(table_name: &str) -> Option<Self> {
        let normalized = table_name.trim().to_ascii_lowercase();
        if !normalized.starts_with("be_") {
            return None;
        }
        Some(match normalized.as_str() {
            "be_tablet_write_log" => Self::TabletWriteLog,
            "be_txns" => Self::Txns,
            "be_compactions" => Self::Compactions,
            _ => Self::Unsupported(normalized),
        })
    }

    pub(crate) fn table_name(&self) -> &str {
        match self {
            Self::TabletWriteLog => "be_tablet_write_log",
            Self::Txns => "be_txns",
            Self::Compactions => "be_compactions",
            Self::Unsupported(name) => name.as_str(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SchemaTable {
    Loads,
    Be(BeSchemaTable),
}

impl SchemaTable {
    pub(crate) fn from_table_name(table_name: &str) -> Option<Self> {
        let normalized = table_name.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "loads" => Some(Self::Loads),
            _ => BeSchemaTable::from_table_name(&normalized).map(Self::Be),
        }
    }

    pub(crate) fn table_name(&self) -> &str {
        match self {
            Self::Loads => "loads",
            Self::Be(table) => table.table_name(),
        }
    }
}

pub(crate) fn record_tablet_write_load_log(record: BeTabletWriteLoadLogRecord) {
    be_tablet_write_log_store::record_load(record);
}

pub(crate) fn record_be_txn_active(record: BeTxnActiveRecord) {
    be_txn_store::record_active(record);
}

pub(crate) fn mark_be_txn_published(txn_id: i64, tablet_id: i64, publish_time: i64, version: i64) {
    be_txn_store::mark_published(txn_id, tablet_id, publish_time, version);
}

pub(crate) fn abort_be_txn_active(txn_id: i64, tablet_id: i64) {
    be_txn_store::abort_active(txn_id, tablet_id);
}

#[cfg(test)]
pub(crate) fn test_lock() -> &'static std::sync::Mutex<()> {
    use std::sync::{Mutex, OnceLock};
    static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    TEST_LOCK.get_or_init(|| Mutex::new(()))
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn clear_for_test() {
    be_tablet_write_log_store::clear_for_test();
    be_txn_store::clear_for_test();
    be_compaction_stats_store::clear_for_test();
}
