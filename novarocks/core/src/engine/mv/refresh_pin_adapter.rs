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

use std::sync::Arc;

use crate::engine::StandaloneState;
use crate::mv::refresh::pin::RefreshSnapshotPin;
use novarocks_catalog::identifier::TableIdentity;

pub(crate) fn capture_refresh_snapshot_pin(
    state: &Arc<StandaloneState>,
    base_refs: &[TableIdentity],
) -> Result<RefreshSnapshotPin, String> {
    let mut entries = Vec::with_capacity(base_refs.len());
    for base_ref in base_refs {
        let loaded =
            crate::engine::mv::refresh_io::load_current_iceberg_base_table(state, base_ref)?;
        let snapshot_id = loaded
            .table
            .metadata()
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id())
            .ok_or_else(|| {
                format!(
                    "iceberg base table {} has no current snapshot; cannot freeze refresh pin",
                    base_ref.fqn()
                )
            })?;
        entries.push((
            base_ref.clone(),
            snapshot_id,
            loaded.table.metadata().uuid().to_string(),
        ));
    }
    let pin = RefreshSnapshotPin::from_captured_entries(entries);
    #[cfg(test)]
    invoke_after_capture_hook();
    Ok(pin)
}

#[cfg(test)]
type AfterCaptureHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
struct AfterCaptureHookRegistration {
    owner: std::thread::ThreadId,
    hook: AfterCaptureHook,
}

#[cfg(test)]
fn lock_after_capture_hook_for_test() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .expect("after_capture_hook test lock")
}

#[cfg(test)]
fn after_capture_hook_slot() -> &'static std::sync::Mutex<Option<AfterCaptureHookRegistration>> {
    static HOOK: std::sync::OnceLock<std::sync::Mutex<Option<AfterCaptureHookRegistration>>> =
        std::sync::OnceLock::new();
    HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn invoke_after_capture_hook() {
    let current_thread = std::thread::current().id();
    let hook = after_capture_hook_slot()
        .lock()
        .expect("after_capture_hook lock")
        .as_ref()
        .and_then(|registration| {
            (registration.owner == current_thread).then(|| Arc::clone(&registration.hook))
        });
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn set_after_capture_hook(f: AfterCaptureHook) {
    *after_capture_hook_slot()
        .lock()
        .expect("after_capture_hook lock") = Some(AfterCaptureHookRegistration {
        owner: std::thread::current().id(),
        hook: f,
    });
}

#[cfg(test)]
fn clear_after_capture_hook() {
    *after_capture_hook_slot()
        .lock()
        .expect("after_capture_hook lock") = None;
}

#[cfg(test)]
pub(crate) struct AfterCaptureHookGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl AfterCaptureHookGuard {
    pub(crate) fn install(hook: Arc<dyn Fn() + Send + Sync>) -> Self {
        let lock = lock_after_capture_hook_for_test();
        set_after_capture_hook(hook);
        Self { _lock: lock }
    }
}

#[cfg(test)]
impl Drop for AfterCaptureHookGuard {
    fn drop(&mut self) {
        clear_after_capture_hook();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn after_capture_hook_round_trip() {
        let _hook_lock = lock_after_capture_hook_for_test();
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag_for_hook = Arc::clone(&flag);
        set_after_capture_hook(Arc::new(move || {
            flag_for_hook.store(true, std::sync::atomic::Ordering::SeqCst);
        }));
        std::thread::spawn(invoke_after_capture_hook)
            .join()
            .expect("invoke hook from non-owner thread");
        assert!(!flag.load(std::sync::atomic::Ordering::SeqCst));
        invoke_after_capture_hook();
        assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
        clear_after_capture_hook();
        flag.store(false, std::sync::atomic::Ordering::SeqCst);
        invoke_after_capture_hook();
        assert!(!flag.load(std::sync::atomic::Ordering::SeqCst));
    }
}
