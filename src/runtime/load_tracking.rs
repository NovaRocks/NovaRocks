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

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use crate::runtime::query_context::QueryId;

const MAX_TRACKING_QUERIES: usize = 1024;

#[derive(Default)]
struct TrackingLogStore {
    mu: Mutex<HashMap<QueryId, TrackingLogEntry>>,
    next_seq: Mutex<u64>,
}

#[derive(Clone, Debug, Default)]
struct TrackingLogEntry {
    seq: u64,
    lines: Vec<String>,
}

fn store() -> &'static TrackingLogStore {
    static STORE: OnceLock<TrackingLogStore> = OnceLock::new();
    STORE.get_or_init(TrackingLogStore::default)
}

pub(crate) fn append_logs(
    query_id: QueryId,
    logs: impl IntoIterator<Item = String>,
) -> Option<String> {
    let logs = logs
        .into_iter()
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    if logs.is_empty() {
        return None;
    }

    let store = store();
    let mut seq_guard = store.next_seq.lock().expect("tracking seq lock");
    let seq = *seq_guard;
    *seq_guard = seq.saturating_add(1);
    drop(seq_guard);

    let mut guard = store.mu.lock().expect("tracking log store lock");
    let rendered = {
        let entry = guard.entry(query_id).or_default();
        entry.seq = seq;
        entry.lines.extend(logs);
        entry.lines.join("\n")
    };
    if guard.len() > MAX_TRACKING_QUERIES
        && let Some(oldest) = guard
            .iter()
            .min_by_key(|(_, entry)| entry.seq)
            .map(|(query_id, _)| *query_id)
    {
        guard.remove(&oldest);
    }
    Some(rendered)
}

pub(crate) fn has_tracking_log(query_id: QueryId) -> bool {
    let store = store();
    let guard = store.mu.lock().expect("tracking log store lock");
    guard
        .get(&query_id)
        .is_some_and(|entry| !entry.lines.is_empty())
}

pub(crate) fn get_tracking_log(query_id: QueryId) -> Option<String> {
    let store = store();
    let guard = store.mu.lock().expect("tracking log store lock");
    guard.get(&query_id).and_then(|entry| {
        if entry.lines.is_empty() {
            None
        } else {
            Some(entry.lines.join("\n"))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_read_tracking_logs() {
        let query_id = QueryId { hi: 1, lo: 2 };
        append_logs(query_id, ["row-1".to_string(), "row-2".to_string()]);

        assert!(has_tracking_log(query_id));
        assert_eq!(get_tracking_log(query_id).as_deref(), Some("row-1\nrow-2"));
    }
}
