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
use std::sync::{Mutex, OnceLock};

#[derive(Clone, Debug, Default)]
pub(crate) struct BeCompactionStatsSnapshot {
    pub(crate) candidates_num: i64,
    pub(crate) base_compaction_concurrency: i64,
    pub(crate) cumulative_compaction_concurrency: i64,
    pub(crate) latest_compaction_score: f64,
    pub(crate) candidate_max_score: f64,
    pub(crate) manual_compaction_concurrency: i64,
    pub(crate) manual_compaction_candidates_num: i64,
}

static STORE: OnceLock<Mutex<BeCompactionStatsSnapshot>> = OnceLock::new();

fn store() -> &'static Mutex<BeCompactionStatsSnapshot> {
    STORE.get_or_init(|| Mutex::new(BeCompactionStatsSnapshot::default()))
}

#[allow(dead_code)]
pub(crate) fn update(snapshot: BeCompactionStatsSnapshot) {
    let mut guard = store().lock().expect("be compaction stats store lock");
    *guard = snapshot;
}

pub(crate) fn snapshot() -> BeCompactionStatsSnapshot {
    store()
        .lock()
        .expect("be compaction stats store lock")
        .clone()
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn clear_for_test() {
    update(BeCompactionStatsSnapshot::default());
}
