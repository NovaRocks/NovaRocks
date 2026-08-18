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

//! The MV startup restore steps, as a port the application owner drives.
//!
//! Restoring MV state at startup is an ordered sequence whose ordering is a
//! correctness property, not an implementation detail. Expressing the steps as a
//! port lets the frontend own that ordering while the lake-reading work stays
//! where the engine state lives.
//!
//! This is the dependency-inversion half of moving startup orchestration out of
//! aggregate Core: the ordering contract moves first, the implementation later.
//! Inverting first means the eventual physical move cannot silently change the
//! order, because the order is stated here rather than implied by three
//! consecutive statements in an engine function.

/// The ordered MV restore steps.
///
/// Each step is separate because each has a distinct precondition, and the
/// sequence exists to satisfy them in turn.
pub trait MvStartupRestore: Send + Sync {
    /// Rediscover lake-native MV packages missing from the MV repository and
    /// persist their rebuilt definitions.
    ///
    /// First because everything after it reads MV definitions. The implementation
    /// always enters the bounded discovery sweep; the frontend's admitted catalog
    /// projection and provider observations naturally determine whether there is
    /// work to rebuild.
    fn rebuild_cache_from_lake(&self) -> Result<(), String>;

    /// Restore each definition's provider-side target state.
    ///
    /// After the rebuild so it sees rebuilt definitions too, and before recovery
    /// so recovery has target descriptors to inspect against.
    fn restore_targets(&self) -> Result<(), String>;

    /// Reconcile unfinished refresh attempts.
    ///
    /// Last because it needs both catalog bindings and target descriptors already
    /// restored in order to acquire a current-generation inspection lease per
    /// fenced attempt.
    fn recover_unfinished_refreshes(&self) -> Result<(), String>;
}

/// Runs the restore steps in the one order that satisfies their preconditions.
///
/// Callers use this rather than invoking the steps themselves, so the ordering
/// lives in one place and a future move of the implementation cannot reorder it
/// by accident.
pub fn run_mv_startup_restore(restore: &dyn MvStartupRestore) -> Result<(), String> {
    restore.rebuild_cache_from_lake()?;
    restore.restore_targets()?;
    restore.recover_unfinished_refreshes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingRestore {
        calls: Mutex<Vec<&'static str>>,
        fail_at: Option<&'static str>,
    }

    impl RecordingRestore {
        fn failing_at(step: &'static str) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                fail_at: Some(step),
            }
        }

        fn record(&self, step: &'static str) -> Result<(), String> {
            self.calls.lock().unwrap().push(step);
            if self.fail_at == Some(step) {
                return Err(format!("{step} failed"));
            }
            Ok(())
        }

        fn calls(&self) -> Vec<&'static str> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl MvStartupRestore for RecordingRestore {
        fn rebuild_cache_from_lake(&self) -> Result<(), String> {
            self.record("rebuild")
        }
        fn restore_targets(&self) -> Result<(), String> {
            self.record("targets")
        }
        fn recover_unfinished_refreshes(&self) -> Result<(), String> {
            self.record("recover")
        }
    }

    #[test]
    fn restore_runs_rebuild_then_targets_then_recovery() {
        let restore = RecordingRestore::default();
        run_mv_startup_restore(&restore).expect("restore succeeds");

        // The order is the contract: recovery needs target descriptors, and
        // target restore needs definitions the rebuild may have just recreated.
        assert_eq!(restore.calls(), vec!["rebuild", "targets", "recover"]);
    }

    #[test]
    fn a_failed_step_stops_the_sequence() {
        // Continuing past a failed rebuild would run recovery against a cache
        // that is still missing definitions, and recovery would then see fewer
        // attempts than exist.
        let restore = RecordingRestore::failing_at("rebuild");
        let error = run_mv_startup_restore(&restore).expect_err("rebuild failure propagates");
        assert!(error.contains("rebuild failed"), "{error}");
        assert_eq!(restore.calls(), vec!["rebuild"]);

        let restore = RecordingRestore::failing_at("targets");
        assert!(run_mv_startup_restore(&restore).is_err());
        assert_eq!(
            restore.calls(),
            vec!["rebuild", "targets"],
            "recovery must not run after target restore failed"
        );
    }
}
