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

use crate::common::types::UniqueId;
use crate::types::{TSinkCommitInfo, TTabletCommitInfo, TTabletFailInfo};

struct SinkCommitStore {
    mu: Mutex<HashMap<UniqueId, SinkCommitEntry>>,
}

#[derive(Default)]
struct SinkCommitEntry {
    sink_commit_infos: Vec<TSinkCommitInfo>,
    tablet_commit_infos: Vec<TTabletCommitInfo>,
    tablet_fail_infos: Vec<TTabletFailInfo>,
    loaded_rows: i64,
    loaded_bytes: i64,
}

static STORE: OnceLock<SinkCommitStore> = OnceLock::new();

fn store() -> &'static SinkCommitStore {
    STORE.get_or_init(|| SinkCommitStore {
        mu: Mutex::new(HashMap::new()),
    })
}

pub(crate) fn register(finst_id: UniqueId) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    guard.entry(finst_id).or_default();
}

pub(crate) fn unregister(finst_id: UniqueId) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    guard.remove(&finst_id);
}

pub(crate) fn add(finst_id: UniqueId, info: TSinkCommitInfo) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    guard
        .entry(finst_id)
        .or_default()
        .sink_commit_infos
        .push(info);
}

pub(crate) fn list(finst_id: UniqueId) -> Vec<TSinkCommitInfo> {
    let store = store();
    let guard = store.mu.lock().expect("sink commit store lock");
    guard
        .get(&finst_id)
        .map(|entry| entry.sink_commit_infos.clone())
        .unwrap_or_default()
}

pub(crate) fn add_tablet_commit_info(finst_id: UniqueId, info: TTabletCommitInfo) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    let entry = guard.entry(finst_id).or_default();
    let already_exists = entry.tablet_commit_infos.iter().any(|current| {
        current.tablet_id == info.tablet_id && current.backend_id == info.backend_id
    });
    if !already_exists {
        entry.tablet_commit_infos.push(info);
    }
}

pub(crate) fn list_tablet_commit_infos(finst_id: UniqueId) -> Vec<TTabletCommitInfo> {
    let store = store();
    let guard = store.mu.lock().expect("sink commit store lock");
    guard
        .get(&finst_id)
        .map(|entry| entry.tablet_commit_infos.clone())
        .unwrap_or_default()
}

pub(crate) fn add_tablet_fail_info(finst_id: UniqueId, info: TTabletFailInfo) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    let entry = guard.entry(finst_id).or_default();
    let already_exists = entry.tablet_fail_infos.iter().any(|current| {
        current.tablet_id == info.tablet_id && current.backend_id == info.backend_id
    });
    if !already_exists {
        entry.tablet_fail_infos.push(info);
    }
}

pub(crate) fn list_tablet_fail_infos(finst_id: UniqueId) -> Vec<TTabletFailInfo> {
    let store = store();
    let guard = store.mu.lock().expect("sink commit store lock");
    guard
        .get(&finst_id)
        .map(|entry| entry.tablet_fail_infos.clone())
        .unwrap_or_default()
}

pub(crate) fn add_load_counters(finst_id: UniqueId, loaded_rows: i64, loaded_bytes: i64) {
    let store = store();
    let mut guard = store.mu.lock().expect("sink commit store lock");
    let entry = guard.entry(finst_id).or_default();
    entry.loaded_rows = entry.loaded_rows.saturating_add(loaded_rows.max(0));
    entry.loaded_bytes = entry.loaded_bytes.saturating_add(loaded_bytes.max(0));
}

pub(crate) fn get_load_counters(finst_id: UniqueId) -> (i64, i64) {
    let store = store();
    let guard = store.mu.lock().expect("sink commit store lock");
    guard
        .get(&finst_id)
        .map(|entry| (entry.loaded_rows, entry.loaded_bytes))
        .unwrap_or((0, 0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedup_tablet_commit_infos_by_tablet_backend_pair() {
        let finst_id = UniqueId { hi: 901, lo: 1 };
        unregister(finst_id);
        register(finst_id);

        add_tablet_commit_info(
            finst_id,
            TTabletCommitInfo::new(
                10,
                20,
                Option::<Vec<String>>::None,
                Option::<Vec<String>>::None,
                Option::<Vec<i64>>::None,
            ),
        );
        add_tablet_commit_info(
            finst_id,
            TTabletCommitInfo::new(
                10,
                20,
                Option::<Vec<String>>::None,
                Option::<Vec<String>>::None,
                Option::<Vec<i64>>::None,
            ),
        );
        let listed = list_tablet_commit_infos(finst_id);
        assert_eq!(listed.len(), 1);

        unregister(finst_id);
    }

    #[test]
    fn dedup_tablet_fail_infos_by_tablet_backend_pair() {
        let finst_id = UniqueId { hi: 902, lo: 1 };
        unregister(finst_id);
        register(finst_id);

        add_tablet_fail_info(finst_id, TTabletFailInfo::new(Some(100), Some(200)));
        add_tablet_fail_info(finst_id, TTabletFailInfo::new(Some(100), Some(200)));
        let listed = list_tablet_fail_infos(finst_id);
        assert_eq!(listed.len(), 1);

        unregister(finst_id);
    }

    #[test]
    fn accumulate_load_counters() {
        let finst_id = UniqueId { hi: 903, lo: 1 };
        unregister(finst_id);
        register(finst_id);

        add_load_counters(finst_id, 10, 100);
        add_load_counters(finst_id, 7, 50);
        assert_eq!(get_load_counters(finst_id), (17, 150));

        unregister(finst_id);
        assert_eq!(get_load_counters(finst_id), (0, 0));
    }
}
