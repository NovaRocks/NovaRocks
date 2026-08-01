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
use std::sync::Mutex;

use novarocks_types::QueryId;

const MAX_TRACKING_QUERIES: usize = 1024;

#[derive(Debug, Default)]
pub(crate) struct LoadTrackingStore {
    mu: Mutex<HashMap<QueryId, TrackingLogEntry>>,
    next_seq: Mutex<u64>,
}

#[derive(Clone, Debug, Default)]
struct TrackingLogEntry {
    seq: u64,
    lines: Vec<String>,
}

impl LoadTrackingStore {
    pub(crate) fn append_logs(
        &self,
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

        let mut seq_guard = self.next_seq.lock().expect("tracking seq lock");
        let seq = *seq_guard;
        *seq_guard = seq.saturating_add(1);
        drop(seq_guard);

        let mut guard = self.mu.lock().expect("tracking log store lock");
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

    pub(crate) fn has_tracking_log(&self, query_id: QueryId) -> bool {
        self.mu
            .lock()
            .expect("tracking log store lock")
            .get(&query_id)
            .is_some_and(|entry| !entry.lines.is_empty())
    }

    pub(crate) fn get_tracking_log(&self, query_id: QueryId) -> Option<String> {
        self.mu
            .lock()
            .expect("tracking log store lock")
            .get(&query_id)
            .and_then(|entry| (!entry.lines.is_empty()).then(|| entry.lines.join("\n")))
    }

    pub(crate) fn clear(&self) {
        self.mu.lock().expect("tracking log store lock").clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_read_tracking_logs() {
        let query_id = QueryId::new(1, 2);
        let store = LoadTrackingStore::default();
        store.append_logs(query_id, ["row-1".to_string(), "row-2".to_string()]);

        assert!(store.has_tracking_log(query_id));
        assert_eq!(
            store.get_tracking_log(query_id).as_deref(),
            Some("row-1\nrow-2")
        );
    }
}
