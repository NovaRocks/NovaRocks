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

use super::*;
use serde_json::{Value, json};
fn shape() -> Value {
    let mut value: Value =
        serde_json::from_str(include_str!("held_native_admission_test_binding.json")).unwrap();
    value["runnable"] = json!(true);
    value["frozen_before_execution"] = json!(true);
    value["original_execution_binding_path"] = json!("original.json");
    value["original_execution_binding_sha256"] = json!("2".repeat(64));
    value["held_input_sha256"] = json!(INPUT_SHA);
    value["cargo_lock_sha256"] = json!("3".repeat(64));
    value
}
fn parse(value: Value) -> Result<HeldBinding> {
    let binding: HeldBinding = serde_json::from_value(value)?;
    binding.validate()?;
    Ok(binding)
}
#[test]
fn template_is_not_admission_and_each_actual_hash_is_mandatory() {
    let template: HeldBinding =
        serde_json::from_str(include_str!("held_native_admission_test_binding.json")).unwrap();
    assert!(template.validate().is_err());
    assert!(parse(shape()).is_ok()); // DTO consistency only, never actual admit success
    for key in [
        "original_execution_binding_sha256",
        "held_input_sha256",
        "cargo_lock_sha256",
    ] {
        let mut value = shape();
        value[key] = Value::Null;
        assert!(parse(value).is_err());
        let mut value = shape();
        value[key] = json!("A".repeat(64));
        assert!(parse(value).is_err());
    }
    let mut value = shape();
    value["original_execution_binding_path"] = json!("");
    assert!(parse(value).is_err());
}
#[test]
fn unknown_duplicate_fields_and_unsupported_modes_are_rejected() {
    let mut value = shape();
    value["extra"] = json!(true);
    assert!(parse(value).is_err());
    let mut value = shape();
    value["scene"]["extra"] = json!(true);
    assert!(parse(value).is_err());
    let mut value = shape();
    value["preparation"]["extra"] = json!(true);
    assert!(parse(value).is_err());
    let bytes = serde_json::to_string(&shape()).unwrap();
    let duplicate = bytes.replacen("{", "{\"schema_version\":1,", 1);
    assert!(serde_json::from_str::<HeldBinding>(&duplicate).is_err());
    for key in ["runnable", "frozen_before_execution"] {
        let mut value = shape();
        value[key] = json!(false);
        assert!(parse(value).is_err());
    }
}
#[test]
fn original_clocks_positions_input_and_config_policy_are_closed() {
    for key in [
        "whole_prelaunch_ms",
        "whole_protocol_phase_ms",
        "phase_sample_interval_ms",
        "phase_max_samples",
        "mysql_write_deadline_ms",
        "closing_deadline_ms",
        "window_positions",
        "held_capture_bytes",
        "held_capture_frames",
        "max_identity_markers_per_backend",
        "case_count",
    ] {
        let mut value = shape();
        let old = value["scene"][key].as_u64().unwrap();
        value["scene"][key] = json!(old + 1);
        assert!(parse(value).is_err());
    }
    let mut value = shape();
    value["held_input_sha256"] = json!("4".repeat(64));
    assert!(parse(value).is_err());
    let mut value = shape();
    value["preparation"]["deadline_ms"] = json!(30001);
    assert!(parse(value).is_err());
    let mut value = shape();
    value["neutral_feature_policy"] = json!("shape_only");
    assert!(parse(value).is_err());
    let mut value = shape();
    value["prelaunch_effective_config_policy"] = json!("postrun_receipt");
    assert!(parse(value).is_err());
}
#[test]
fn neutral_diagnostic_is_exact_one_line_and_full_actual_clean_revision() {
    let rev = "1".repeat(40);
    let line = format!(
        "{} commit={rev} build_identity={rev} root_observation=true\n",
        novarocks_failpoint::observation_marker::MEM_1_M07_ROOT_OBSERVATION_BUILD
    );
    assert!(neutral_identity(line.as_bytes(), &rev).is_ok());
    for bytes in [
        line.trim_end().as_bytes().to_vec(),
        format!("{line}{line}").into_bytes(),
        line.replace("true", "false").into_bytes(),
        line.replace("root_observation", "exact_mysql_write")
            .into_bytes(),
        line.replace("build_identity=111", "build_identity=222")
            .into_bytes(),
    ] {
        assert!(neutral_identity(&bytes, &rev).is_err());
    }
    assert!(neutral_identity(line.as_bytes(), "11111111").is_err());
}
#[test]
fn immutable_input_rejects_unknown_duplicate_or_oracle_mutation_before_projection() {
    let bytes = include_bytes!("../fixtures/mem-1-m07/held-late-ack-freeze-v3.json");
    assert!(input(bytes).is_ok());
    let text = std::str::from_utf8(bytes).unwrap();
    for changed in [
        text.replacen("{", "{\"extra\":true,", 1),
        text.replacen("{", "{\"schema_version\":1,", 1),
        text.replace("data2_bytes\": 8", "data2_bytes\": 9"),
        text.replace("ClientRows", "Scalar"),
        text.replace("FROZEN_INPUT_NOT_EXECUTED", "DRAFT_UNEXECUTED_NOT_RUNNABLE"),
    ] {
        assert!(input(changed.as_bytes()).is_err());
    }
    assert!(input(&bytes[..bytes.len() - 1]).is_err());
}
#[test]
fn failed_binding_admission_precedes_any_command_or_native_role_launch() {
    let directory = tempfile::tempdir().unwrap();
    let binding = directory.path().join("binding.json");
    fs::write(
        &binding,
        include_bytes!("held_native_admission_test_binding.json"),
    )
    .unwrap();
    // Both paths deliberately absent. A runnable=false template fails before
    // opening or launching either, preserving the original admission error.
    let result = admit(
        &binding,
        &directory.path().join("not-a-server"),
        &directory.path().join("no-config"),
        &crate::build_information::BuildInformation::fixture(),
    );
    assert!(result.is_err());
}
