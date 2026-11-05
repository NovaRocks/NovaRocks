// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Child of exact_native_admission: actual additional input/feature admission.
//! No runtime role, Root authority or scene clock is created by this module.
use super::*;
use serde::Deserialize;
const INPUT_PATH: &str = "tests/system-test-runner/fixtures/mem-1-m07/held-late-ack-freeze-v3.json";
const NEUTRAL_ARGUMENT: &str = "--mem-1-m07-root-observation-build-identity";
const CASE: &str = "result-delivery/held-response-late-ack";
const SQL: &str = "SELECT REPEAT('x', 1048576) AS payload FROM generate_series(1, 1)";
const INPUT_SHA: &str = "5c8975f987c9ece83cbae75c582e91535439d763557d0d5285060dacbb827812";
const INSTALLED_SHA: &str = "60425f90a0f7f6da0f81f85702e967d093c1360dcbbfc162e5c4d301c41054d8";
const RETENTION_SHA: &str = "3f8813c8c8205ba724ad8c3dc6eeecae4562681244e46d12910de360a1c45752";
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HeldBinding {
    schema_version: u32,
    kind: String,
    runnable: bool,
    frozen_before_execution: bool,
    original_execution_binding_path: PathBuf,
    original_execution_binding_sha256: Option<String>,
    held_input_sha256: Option<String>,
    cargo_lock_sha256: Option<String>,
    neutral_feature_policy: String,
    prelaunch_effective_config_policy: String,
    preparation: Preparation,
    scene: HeldScene,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HeldScene {
    name: String,
    whole_prelaunch_ms: u64,
    metadata_wait_ms: u64,
    whole_protocol_phase_ms: u64,
    phase_sample_interval_ms: u64,
    phase_max_samples: usize,
    mysql_write_deadline_ms: u64,
    closing_deadline_ms: u64,
    request_frame_bytes: u64,
    response_data_bytes: u64,
    held_capture_bytes: u64,
    held_frame_bytes: u64,
    held_capture_frames: usize,
    max_identity_markers_per_backend: usize,
    original_log_bytes_per_backend: u64,
    original_marker_line_bytes: usize,
    segment_bytes: u64,
    window_positions: usize,
    max_wait_millis: u64,
    case_count: usize,
}
impl HeldBinding {
    fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == 1
                && self.kind == "mem-1-m07-held-native-admission-v1"
                && self.runnable
                && self.frozen_before_execution,
            "held binding is not a frozen runnable execution admission"
        );
        ensure!(
            !self.original_execution_binding_path.as_os_str().is_empty(),
            "original execution binding locator is absent"
        );
        for value in [
            &self.original_execution_binding_sha256,
            &self.held_input_sha256,
            &self.cargo_lock_sha256,
        ] {
            hash(
                value
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("actual frozen digest is absent"))?,
            )?;
        }
        ensure!(
            self.neutral_feature_policy == "actual_exact_argument_neutral_feature_full_clean_build"
                && self.prelaunch_effective_config_policy == EFFECTIVE_POLICY,
            "held feature/config policy changed"
        );
        ensure!(
            self.held_input_sha256.as_deref() == Some(INPUT_SHA),
            "binding changed immutable held input bytes"
        );
        let p = &self.preparation;
        ensure!(
            p.deadline_ms == PREP_MS
                && p.command_ms == COMMAND_MS
                && p.reap_ms == REAP_MS
                && p.binding_bytes == BINDING_BYTES
                && p.input_bytes == INPUT_BYTES
                && p.config_bytes == CONFIG_BYTES
                && p.binary_bytes == BINARY_BYTES
                && p.git_stdout_bytes == GIT_STDOUT_BYTES
                && p.stderr_bytes == STDERR_BYTES
                && p.identity_stdout_bytes == IDENTITY_STDOUT_BYTES
                && p.scratch_bytes == SCRATCH,
            "held preparation bounds changed"
        );
        let s = &self.scene;
        ensure!(
            s.name == CASE
                && s.whole_prelaunch_ms == 20000
                && s.metadata_wait_ms == 5000
                && s.whole_protocol_phase_ms == 5000
                && s.phase_sample_interval_ms == 100
                && s.phase_max_samples == 51
                && s.mysql_write_deadline_ms == 30000
                && s.closing_deadline_ms == 5000
                && s.request_frame_bytes == 4096
                && s.response_data_bytes == 1052672
                && s.held_capture_bytes == 4096
                && s.held_frame_bytes == 16384
                && s.held_capture_frames == 16
                && s.max_identity_markers_per_backend == 8
                && s.original_log_bytes_per_backend == 2097152
                && s.original_marker_line_bytes == 384
                && s.segment_bytes == 1048576
                && s.window_positions == 2
                && s.max_wait_millis == 100
                && s.case_count == 1,
            "held scene changed original workload, clocks or finite positions"
        );
        Ok(())
    }
}
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct AdmittedHeldNativeRun {
    original: AdmittedExactNativeRun,
    pub(crate) held_execution_binding_sha256: String,
    pub(crate) held_input_sha256: String,
    pub(crate) cargo_lock_sha256: String,
    pub(crate) neutral_build_commit: String,
    pub(crate) neutral_build_identity: String,
}
impl std::fmt::Debug for AdmittedHeldNativeRun {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AdmittedHeldNativeRun(actual admission identities retained)")
    }
}
impl AdmittedHeldNativeRun {
    /// Current scene receipt must name its current execution binding, not the
    /// immutable original ten-case binding. Both original hashes remain retained.
    pub(crate) fn for_scene(&self) -> AdmittedExactNativeRun {
        let mut value = self.original.clone();
        value.frozen_execution_binding_sha256 = self.held_execution_binding_sha256.clone();
        value
    }
}
fn neutral_identity(bytes: &[u8], expected: &str) -> Result<()> {
    revision(expected)?;
    let required = format!(
        "{} commit={expected} build_identity={expected} root_observation=true\n",
        novarocks_failpoint::observation_marker::MEM_1_M07_ROOT_OBSERVATION_BUILD
    );
    ensure!(
        bytes == required.as_bytes(),
        "actual neutral feature/full-clean-build diagnostic differs"
    );
    Ok(())
}
fn value<'a>(object: &'a serde_json::Value, key: &str) -> Result<&'a serde_json::Value> {
    object
        .get(key)
        .ok_or_else(|| anyhow::anyhow!("frozen held input field absent"))
}
// The caller binds the full raw digest before this closed literal oracle is checked.
// This is metadata validation, not evidence that any Native request has run.
fn input(bytes: &[u8]) -> Result<()> {
    // Closed raw bytes are required, so unknown/duplicate/missing fields, alternate
    // serialization and changed nested literals are rejected before JSON projection.
    // The proposed immutable input is frozen before execution, not Native proof.
    // Only the independently actual-admitted binding may enable execution.
    ensure!(
        bytes.len() <= INPUT_BYTES as usize && digest(bytes) == INPUT_SHA,
        "held input is not the exact immutable frozen source"
    );
    let v: serde_json::Value = serde_json::from_slice(bytes)?;
    ensure!(
        v.is_object() && v["status"].as_str() == Some("FROZEN_INPUT_NOT_EXECUTED"),
        "held input status is not frozen before execution"
    );
    for (key, expected) in [
        ("case", CASE),
        ("sql", SQL),
        ("topology", "1FE+3BE"),
        (
            "expected_fe_source",
            &*format!(
                "{} actual original NativeTrust FrontendProcessId; default-off distinct root-observation feature",
                novarocks_failpoint::observation_marker::MEM_1_M07_ROOT_OBSERVATION_FE
            ),
        ),
        ("config_policy", EFFECTIVE_POLICY),
        ("root_output_kind", "ClientRows"),
    ] {
        ensure!(
            value(&v, key)?.as_str() == Some(expected),
            "frozen held input literal differs"
        );
    }
    for (key, expected) in [
        ("schema_version", 1),
        ("root_profile_id", 1),
        ("segment_bytes", 1048576),
        ("window_positions", 2),
        ("original_segments_before_seal", 2),
        ("original_segments_after_seal", 0),
        ("native_row_bytes", 1048584),
        ("data1_bytes", 1048576),
        ("data2_bytes", 8),
        ("end_sequence", 3),
        ("end_rows", 1),
        ("max_wait_millis", 100),
        ("request_frame_bytes", 4096),
        ("response_data_bytes", 1052672),
        ("held_capture_bytes", 4096),
        ("held_frame_bytes", 16384),
        ("held_capture_frames", 16),
        ("max_identity_markers_per_backend", 8),
        ("original_log_bytes_per_backend", 2097152),
        ("original_marker_line_bytes", 384),
        ("whole_prelaunch_clock_ms", 20000),
        ("metadata_wait_ms", 5000),
        ("whole_protocol_phase_ms", 5000),
        ("phase_sample_interval_ms", 100),
        ("phase_max_samples", 51),
        ("client_receive_buffer_bytes", 4096),
        ("max_applied_receive_buffer_bytes", 65536),
        ("mysql_client_max_packet_bytes", 67108864),
        ("mysql_expected_rows", 1),
        ("mysql_expected_packets", 5),
        ("mysql_expected_row_payload_bytes", 1048580),
        ("mysql_expected_terminal_code", 1317),
        ("health_row_payload_bytes", 5),
        ("health_packets", 5),
        ("mysql_write_deadline_ms", 30000),
        ("closing_deadline_ms", 5000),
    ] {
        ensure!(
            value(&v, key)?.as_u64() == Some(expected),
            "frozen held input bound differs"
        );
    }
    for (key, expected) in [
        ("sql_sha256", digest(SQL.as_bytes())),
        (
            "data1_sha256",
            "e1778a1a63f0deff423d34c267bbc2be60c018adde82cf23dfcc9926582d0b42".into(),
        ),
        (
            "mysql_expected_row_sha256",
            "e5a05e54f4636fe6e87eb8094fceda8e002bff799f3de76413a2a77c52fd50b8".into(),
        ),
        (
            "health_row_sha256",
            "6dab454b19ecc06d337eb421a7ecc69aaa272a74fdf3a6bf7b81900ca94758b1".into(),
        ),
    ] {
        ensure!(
            value(&v, key)?.as_str() == Some(expected.as_str()),
            "frozen independent held oracle differs"
        );
    }
    let original = value(&v, "original_inputs")?
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("original input references absent"))?;
    ensure!(
        original.len() == 2
            && original[0]["path"].as_str()
                == Some("docs/testing/mem-1-m07/inputs/root-context-retention-freeze-v1.json")
            && original[0]["sha256"].as_str() == Some(RETENTION_SHA)
            && original[1]["path"].as_str()
                == Some("docs/testing/mem-1-m07/inputs/installed-root-protocol-freeze-v3.json")
            && original[1]["sha256"].as_str() == Some(INSTALLED_SHA),
        "original retention reference changed"
    );
    Ok(())
}

pub(crate) fn admit(
    binding_path: &Path,
    server: &Path,
    base_config: &Path,
    build: &crate::build_information::BuildInformation,
) -> Result<AdmittedHeldNativeRun> {
    let deadline = Instant::now() + Duration::from_millis(PREP_MS); // one prep clock, no scene20 renewal
    let repository = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .ok_or_else(|| anyhow::anyhow!("runner workspace unavailable"))?
        .canonicalize()?;
    let dot_git = fs::symlink_metadata(repository.join(".git"))?;
    ensure!(
        !dot_git.file_type().is_symlink(),
        "source Git owner locator is a symlink"
    );
    let dot_git_stamp = Stamp::from(&dot_git);
    let mut binding_owner = InputOwner::open(binding_path, BINDING_BYTES, deadline)?;
    let binding_bytes = binding_owner.read_small(BINDING_BYTES, deadline)?;
    let binding: HeldBinding = serde_json::from_slice(&binding_bytes)?;
    binding.validate()?;
    let locator = if binding.original_execution_binding_path.is_absolute() {
        binding.original_execution_binding_path.clone()
    } else {
        binding_owner
            .canonical
            .parent()
            .ok_or_else(|| anyhow::anyhow!("binding parent absent"))?
            .join(&binding.original_execution_binding_path)
    };
    let mut original_owner = InputOwner::open(&locator, BINDING_BYTES, deadline)?;
    let original_sha = original_owner.stream_hash(deadline)?;
    ensure!(
        Some(&original_sha) == binding.original_execution_binding_sha256.as_ref(),
        "immutable original execution binding differs"
    );
    // Retain exact outer file identities through both old and neutral checks.
    let server_owner = InputOwner::open(server, BINARY_BYTES, deadline)?;
    let runner_owner = InputOwner::open(&std::env::current_exe()?, BINARY_BYTES, deadline)?;
    let config_owner = InputOwner::open(base_config, CONFIG_BYTES, deadline)?;
    let large_owner = InputOwner::open(&repository.join(LARGE_PATH), INPUT_BYTES, deadline)?;
    let tiny_owner = InputOwner::open(&repository.join(TINY_PATH), INPUT_BYTES, deadline)?;
    let mut input_owner = InputOwner::open(&repository.join(INPUT_PATH), INPUT_BYTES, deadline)?;
    let bytes = input_owner.read_small(INPUT_BYTES, deadline)?;
    let held_sha = digest(&bytes);
    ensure!(
        Some(&held_sha) == binding.held_input_sha256.as_ref(),
        "actual held immutable input differs"
    );
    input(&bytes)?;
    let mut lock_owner = InputOwner::open(&repository.join("Cargo.lock"), CONFIG_BYTES, deadline)?;
    let lock_sha = lock_owner.stream_hash(deadline)?;
    ensure!(
        Some(&lock_sha) == binding.cargo_lock_sha256.as_ref(),
        "actual locked dependency input differs"
    );
    let original = admit_until(
        &original_owner.canonical,
        &server_owner.canonical,
        &config_owner.canonical,
        build,
        deadline,
    )?;
    ensure!(
        original.frozen_execution_binding_sha256 == original_sha,
        "original nested admission binding changed"
    );
    let before = source(&repository, deadline)?;
    ensure!(
        before.revision == original.clean_revision && before.tree == original.source_tree_sha256,
        "clean source drifted between old and neutral admission"
    );
    server_owner.recheck(deadline)?;
    let mut command = Command::new(&server_owner.canonical);
    command.env_clear().arg(NEUTRAL_ARGUMENT);
    let diagnostic = bounded_command(command, IDENTITY_STDOUT_BYTES, deadline)?;
    neutral_identity(&diagnostic, &original.clean_revision)?;
    let after = source(&repository, deadline)?;
    ensure!(
        before == after
            && Stamp::from(&fs::symlink_metadata(repository.join(".git"))?) == dot_git_stamp,
        "clean source/Git locator changed during neutral diagnostic"
    );
    for owner in [
        &binding_owner,
        &original_owner,
        &server_owner,
        &runner_owner,
        &config_owner,
        &large_owner,
        &tiny_owner,
        &input_owner,
        &lock_owner,
    ] {
        owner.recheck(deadline)?;
    }
    check(deadline)?;
    Ok(AdmittedHeldNativeRun {
        neutral_build_commit: original.clean_revision.clone(),
        neutral_build_identity: original.clean_revision.clone(),
        original,
        held_execution_binding_sha256: digest(&binding_bytes),
        held_input_sha256: held_sha,
        cargo_lock_sha256: lock_sha,
    })
}
#[cfg(test)]
#[path = "held_native_admission_tests.rs"]
mod tests;
