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

use super::*;
use serde_json::{Value, json};

fn binding() -> Value {
    let mut value: Value =
        serde_json::from_str(include_str!("exact_native_admission_test_binding.json")).unwrap();
    value["runnable"] = json!(true);
    value["frozen_before_execution"] = json!(true);
    value["clean_revision"] = json!("1".repeat(40));
    value["server_build_identity"] = json!("1".repeat(40));
    for key in [
        "source_tree_sha256",
        "server_binary_sha256",
        "runner_binary_sha256",
        "base_config_sha256",
    ] {
        value[key] = json!("2".repeat(64));
    }
    value
}
fn parse(value: Value) -> Result<Binding> {
    let binding: Binding = serde_json::from_value(value)?;
    binding.validate()?;
    Ok(binding)
}
#[test]
fn strict_binding_requires_explicit_freeze_nonempty_and_immutable_inputs() {
    assert!(parse(binding()).is_ok());
    let mut value = binding();
    value["runnable"] = json!(false);
    assert!(parse(value).is_err());
    let mut value = binding();
    value["frozen_before_execution"] = json!(false);
    assert!(parse(value).is_err());
    let mut value = binding();
    value["source_tree_sha256"] = json!("");
    assert!(parse(value).is_err());
    let mut value = binding();
    value["large_input_sha256"] = json!("3".repeat(64));
    assert!(parse(value).is_err());
}
#[test]
fn root_nested_unknown_fields_and_clock_capacity_changes_are_rejected() {
    let mut value = binding();
    value["unexpected"] = json!(true);
    assert!(parse(value).is_err());
    let mut value = binding();
    value["preparation"]["unexpected"] = json!(true);
    assert!(parse(value).is_err());
    let mut value = binding();
    value["scene"]["original_prelaunch_ms"] = json!(20001);
    assert!(parse(value).is_err());
    let mut value = binding();
    value["preparation"]["deadline_ms"] = json!(30001);
    assert!(parse(value).is_err());
    let mut value = binding();
    value["scene"]["stop"] = json!(0);
    assert!(parse(value).is_err());
    let mut value = binding();
    value["prelaunch_effective_config_policy"] = json!("hash_after_run");
    assert!(parse(value).is_err());
}
#[test]
fn actual_build_record_is_one_exact_feature_line_with_independent_full_commit() {
    let revision = "1".repeat(40);
    let valid = format!(
        "{IDENTITY_PREFIX} commit={revision} build_identity={revision} exact_mysql_write=true\n"
    );
    assert_eq!(
        server_identity(valid.as_bytes(), &revision).unwrap(),
        revision
    );
    assert!(server_identity(valid.trim_end().as_bytes(), &revision).is_err());
    assert!(server_identity(format!("{valid}{valid}").as_bytes(), &revision).is_err());
    assert!(server_identity(valid.replace("true", "false").as_bytes(), &revision).is_err());
    assert!(
        server_identity(
            valid
                .replace(&format!("commit={revision}"), "commit=11111111")
                .as_bytes(),
            &revision
        )
        .is_err()
    );
    assert!(
        server_identity(
            valid
                .replace(
                    &format!("build_identity={revision}"),
                    "build_identity=override"
                )
                .as_bytes(),
            &revision
        )
        .is_err()
    );
    assert!(
        server_identity(
            valid
                .replace(&format!("commit={revision}"), "commit=")
                .as_bytes(),
            &revision
        )
        .is_err()
    );
}
fn temp() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("nr-admission-test-")
        .tempdir()
        .unwrap()
}
#[test]
fn stream_hash_is_literal_and_original_regular_owner_rejects_replacement() {
    let root = temp();
    let path = root.path().join("input");
    fs::write(&path, b"abc").unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut owner = InputOwner::open(&path, 10, deadline).unwrap();
    let hash = owner.stream_hash(deadline);
    fs::rename(&path, root.path().join("old")).unwrap();
    fs::write(&path, b"abc").unwrap();
    let rejected = owner.recheck(deadline).is_err();
    drop(owner);
    root.close().unwrap();
    assert_eq!(
        hash.unwrap(),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert!(rejected);
}
#[test]
fn grown_truncated_symlink_and_expired_inputs_are_not_admitted() {
    let root = temp();
    let path = root.path().join("input");
    fs::write(&path, b"abcd").unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    let owner = InputOwner::open(&path, 4, deadline).unwrap();
    fs::write(&path, b"a").unwrap();
    let truncated = owner.recheck(deadline).is_err();
    drop(owner);
    std::os::unix::fs::symlink(&path, root.path().join("alias")).unwrap();
    let symlink = InputOwner::open(&root.path().join("alias"), 4, deadline).is_err();
    let expired = InputOwner::open(&path, 4, Instant::now()).is_err();
    fs::write(&path, b"abcde").unwrap();
    let grown = InputOwner::open(&path, 4, deadline).is_err();
    root.close().unwrap();
    assert!(truncated && symlink && expired && grown);
}
#[test]
fn source_tree_keeps_the_original_five_raw_parts_and_nul_separators() {
    let parts = [b"111".as_slice(), b"", b"tracked\n", b"", b""];
    let mut original = Sha256::new();
    for part in parts {
        original.update(part);
        original.update([0]);
    }
    let mut joined = Vec::new();
    for part in parts {
        joined.extend_from_slice(part);
        joined.push(0);
    }
    assert_eq!(
        <[u8; 32]>::from(original.finalize()),
        <[u8; 32]>::from(Sha256::digest(joined))
    );
    assert_ne!(
        Sha256::digest(b"111tracked\n"),
        Sha256::digest(b"111\0\0tracked\n\0\0\0")
    );
}
fn shell(script: &str) -> Command {
    let mut command = Command::new("/bin/sh");
    command.env_clear().arg("-c").arg(script);
    command
}
fn settle_retained(error: &mut anyhow::Error) {
    let failure = error.downcast_mut::<CommandFailure>().unwrap();
    if let Some(owner) = failure.retained_child.take() {
        let mut child = owner.into_inner().unwrap();
        // Host component cleanup only. Admission remains failed and has no Native20 clock.
        child.kill().ok();
        let status = child.wait().unwrap();
        failure.status = Some(status);
        assert!(!status.success());
    }
}
#[test]
fn actual_original_child_normal_and_nonzero_exit_are_distinct_no_raw_stderr() {
    let deadline = Instant::now() + Duration::from_secs(2);
    assert_eq!(
        bounded_command(shell("printf literal"), 64, deadline).unwrap(),
        b"literal"
    );
    let error = bounded_command(shell("printf diagnostic >&2; exit 1"), 64, deadline).unwrap_err();
    let failure = error.downcast_ref::<CommandFailure>().unwrap();
    assert!(matches!(failure.class, CommandClass::Nonzero));
    assert_eq!(failure.status.unwrap().code(), Some(1));
    assert!(failure.retained_child.is_none());
    assert_eq!(failure.stderr_bytes, 10);
    assert!(!failure.to_string().contains("diagnostic"));
}
#[test]
fn actual_output_overflow_aborts_and_reaps_the_same_original_child() {
    let mut error = bounded_command(
        shell("while :; do printf 0123456789; done"),
        64,
        Instant::now() + Duration::from_secs(2),
    )
    .unwrap_err();
    settle_retained(&mut error);
    let failure = error.downcast_ref::<CommandFailure>().unwrap();
    assert!(matches!(failure.class, CommandClass::OutputBound));
    assert!(failure.stdout_bytes > 64);
}
#[test]
fn actual_deadline_never_mints_success_and_retains_original_handle_until_reap() {
    let mut error = bounded_command(
        shell("while :; do :; done"),
        64,
        Instant::now() + Duration::from_millis(20),
    )
    .unwrap_err();
    settle_retained(&mut error);
    let failure = error.downcast_ref::<CommandFailure>().unwrap();
    assert!(matches!(failure.class, CommandClass::Deadline));
    assert!(failure.status.is_some());
}
#[test]
fn actual_spawn_io_cause_is_owned_and_no_child_exit_is_invented() {
    let error = bounded_command(
        Command::new("/definitely-missing-exact-native-admission-binary"),
        64,
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap_err();
    let failure = error.downcast_ref::<CommandFailure>().unwrap();
    assert!(matches!(failure.class, CommandClass::Spawn));
    assert!(failure.status.is_none());
    assert!(failure.cause.is_some());
    assert!(failure.retained_child.is_none());
    assert!(!failure.to_string().contains("definitely-missing"));
}

#[test]
fn runner_hash_cannot_substitute_for_actual_compiled_commit_and_build() {
    let current = "1".repeat(40);
    let older = "2".repeat(40);
    require_compiled_runner(&current, &current, &current).unwrap();
    assert!(require_compiled_runner(&older, &current, &current).is_err());
    assert!(require_compiled_runner(&current, "override", &current).is_err());
    assert!(require_compiled_runner("", &current, &current).is_err());
}
