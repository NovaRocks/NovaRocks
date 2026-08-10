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

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[cfg(test)]
use novarocks_execution::runtime::fragment::{DormantFragmentHandle, RunningFragmentHandle};

pub(super) const FRAGMENT_EXECUTOR_FAILURE_MESSAGE: &str =
    "fragment executor failure injected after start";
const FRAGMENT_FAILURE_RELEASE_TIMEOUT: Duration = Duration::from_secs(30);
const FRAGMENT_FAILURE_RELEASE_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[cfg(test)]
pub(super) fn start_with_configured_fragment_failure_trigger(
    dormant: DormantFragmentHandle,
    failure_injection_eligible: bool,
) -> (RunningFragmentHandle, Option<FragmentFailureRelease>) {
    let trigger = configured_fragment_failure_trigger();
    start_with_fragment_failure_trigger(dormant, trigger.as_deref(), failure_injection_eligible)
}

/// Claims the runner-owned failure rendezvous without making the fragment
/// terminal. Stage workers use this form so the SQL runner can first observe
/// the successful Stage batch and explicitly release the post-Start failure.
pub(super) fn claim_configured_fragment_failure_trigger(
    failure_injection_eligible: bool,
) -> Result<Option<FragmentFailureRelease>, String> {
    if !failure_injection_eligible {
        return Ok(None);
    }
    let trigger = configured_fragment_failure_trigger();
    consume_fragment_failure_trigger(trigger.as_deref())
}

#[cfg(test)]
pub(super) fn start_with_fragment_failure_trigger(
    dormant: DormantFragmentHandle,
    failure_trigger: Option<&Path>,
    failure_injection_eligible: bool,
) -> (RunningFragmentHandle, Option<FragmentFailureRelease>) {
    if !failure_injection_eligible {
        return (dormant.start(), None);
    }
    match consume_fragment_failure_trigger(failure_trigger) {
        Ok(Some(release)) => (
            dormant.start_failed(FRAGMENT_EXECUTOR_FAILURE_MESSAGE),
            Some(release),
        ),
        Ok(None) => (dormant.start(), None),
        Err(error) => (dormant.start_failed(error), None),
    }
}

pub(super) struct FragmentFailureRelease {
    token: String,
    release_path: PathBuf,
}

impl FragmentFailureRelease {
    pub(super) fn wait(self) -> Result<String, String> {
        let deadline = Instant::now() + FRAGMENT_FAILURE_RELEASE_TIMEOUT;
        loop {
            match std::fs::read_to_string(&self.release_path) {
                Ok(release_token) => {
                    std::fs::remove_file(&self.release_path).map_err(|error| {
                        format!(
                            "remove fragment executor failure release {} failed: {error}",
                            self.release_path.display()
                        )
                    })?;
                    let release_token = release_token.trim();
                    if release_token != self.token {
                        return Err(format!(
                            "fragment executor failure release {} token mismatch",
                            self.release_path.display()
                        ));
                    }
                    return Ok(self.token);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    if Instant::now() >= deadline {
                        return Err(format!(
                            "timed out waiting for fragment executor failure release {}",
                            self.release_path.display()
                        ));
                    }
                    std::thread::sleep(FRAGMENT_FAILURE_RELEASE_POLL_INTERVAL);
                }
                Err(error) => {
                    return Err(format!(
                        "read fragment executor failure release {} failed: {error}",
                        self.release_path.display()
                    ));
                }
            }
        }
    }
}

fn configured_fragment_failure_trigger() -> Option<PathBuf> {
    std::env::var_os("NOVAROCKS_SQL_TEST_FRAGMENT_FAILURE_TRIGGER_FILE").map(PathBuf::from)
}

fn consume_fragment_failure_trigger(
    failure_trigger: Option<&Path>,
) -> Result<Option<FragmentFailureRelease>, String> {
    let Some(path) = failure_trigger else {
        return Ok(None);
    };
    static NEXT_CLAIM: AtomicU64 = AtomicU64::new(1);
    let claim_sequence = NEXT_CLAIM.fetch_add(1, Ordering::Relaxed);
    let claim_path =
        path.with_extension(format!("claimed-{}-{claim_sequence}", std::process::id()));
    match std::fs::rename(path, &claim_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(format!(
                "claim fragment executor failure trigger {} failed: {error}",
                path.display()
            ));
        }
    }
    let token_result = std::fs::read_to_string(&claim_path);
    let cleanup_result = std::fs::remove_file(&claim_path);
    let token = token_result.map_err(|error| {
        format!(
            "read claimed fragment executor failure trigger {} failed: {error}",
            claim_path.display()
        )
    })?;
    cleanup_result.map_err(|error| {
        format!(
            "remove claimed fragment executor failure trigger {} failed: {error}",
            claim_path.display()
        )
    })?;
    let token = token.trim();
    if token.is_empty() || token.split_ascii_whitespace().count() != 1 {
        return Err(format!(
            "fragment executor failure trigger {} contains an invalid evidence token",
            path.display()
        ));
    }
    Ok(Some(FragmentFailureRelease {
        token: token.to_string(),
        release_path: path.with_extension("release"),
    }))
}
