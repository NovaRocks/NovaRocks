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
use crate::fixture_run::{FixturePolicy, ScratchIdentity};
use std::os::unix::fs::PermissionsExt;

fn check(
    script: &str,
    inputs: BTreeSet<ReferenceInput>,
) -> (tempfile::TempDir, Result<ResolvedReference>) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    fs::create_dir_all(root.join("docker/fixture-inputs")).unwrap();
    let jar = root.join("driver.jar");
    fs::write(&jar, b"driver").unwrap();
    fs::write(root.join("docker/fixture-inputs/versions.json"), serde_json::json!({"images":{"trino":{"jdbc_artifact":"driver.jar"}},"artifacts":{"driver.jar":{"sha256":"declared-driver-hash"}}}).to_string()).unwrap();
    let builder = root.join("builder");
    fs::write(
        &builder,
        format!(
            "#!/bin/sh\nset -eu\nprintf '%s\n' \"$2\" >> '{}'\n{script}\n",
            root.join("calls").display()
        ),
    )
    .unwrap();
    fs::set_permissions(&builder, fs::Permissions::from_mode(0o700)).unwrap();
    let policy = FixturePolicy::default();
    let budget = RunBudget::admit(
        policy,
        ScratchIdentity::create("s3://novarocks/reference-input-tests").unwrap(),
        policy.max_run_seconds,
        None,
    )
    .unwrap();
    let result = resolve_with_builder(root, &inputs, &budget, &builder);
    (directory, result)
}

#[test]
fn builder_result_file_is_authoritative_and_each_component_is_resolved_once() {
    let script = r#"printf 'cold pull diagnostic, not JSON\n'
printf 'builder stderr\n' >&2
case "$2" in
  image:trino) printf '%s' '{"images":{"trino":{"alias":"reference-alias","image_id":"sha256:exact-image","version":"declared-version"}}}' > "$4" ;;
  artifact:driver.jar) printf '{"artifact_dir":"%s"}' "$PWD" > "$4" ;;
  *) exit 9 ;;
esac"#;
    let (directory, result) = check(
        script,
        BTreeSet::from([ReferenceInput::TrinoImage, ReferenceInput::TrinoJdbcDriver]),
    );
    let reference = result.unwrap();
    assert_eq!(
        reference.trino_image.unwrap(),
        TrinoImageRef {
            alias: "reference-alias".into(),
            image_id: "sha256:exact-image".into(),
            version: "declared-version".into()
        }
    );
    let driver = reference.trino_jdbc_driver.unwrap();
    assert!(driver.path.is_absolute() && driver.path.is_file());
    assert_eq!(
        driver.path,
        directory.path().join("driver.jar").canonicalize().unwrap()
    );
    assert_eq!(fs::read(&driver.path).unwrap(), b"driver");
    assert_eq!(
        fs::read_to_string(directory.path().join("calls")).unwrap(),
        "image:trino\nartifact:driver.jar\n"
    );
}

#[test]
fn image_only_does_not_request_driver_and_empty_inputs_never_spawn() {
    let (directory, reference) = check(
        r#"printf '%s' '{"images":{"trino":{"alias":"alias","image_id":"image","version":"version"}}}' > "$4""#,
        BTreeSet::from([ReferenceInput::TrinoImage]),
    );
    assert!(reference.unwrap().trino_jdbc_driver.is_none());
    assert_eq!(
        fs::read_to_string(directory.path().join("calls")).unwrap(),
        "image:trino\n"
    );
    let (directory, result) = check("exit 7", BTreeSet::new());
    assert_eq!(result.unwrap(), ResolvedReference::default());
    assert!(!directory.path().join("calls").exists());
}

#[test]
fn nonzero_and_missing_result_preserve_original_command_and_both_channels() {
    for script in [
        "printf diagnostic; printf failure >&2; exit 7",
        "printf '{\"images\":{}}'",
        r#"printf '%s' '{"images":{"trino":{"alias":"incomplete"}}}' > "$4""#,
    ] {
        let (_, result) = check(script, BTreeSet::from([ReferenceInput::TrinoImage]));
        let message = format!("{:#}", result.unwrap_err());
        assert!(
            message.contains("command=")
                && message.contains("stdout=")
                && message.contains("stderr="),
            "{message}"
        );
        assert!(
            message.contains("status=")
                || message.contains("result file")
                || message.contains("omitted image_id"),
            "{message}"
        );
    }
}
