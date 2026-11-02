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

use clap::Parser;
use novarocks_cluster_harness::fixture_run::{FixturePolicy, RunBudget, ScratchIdentity};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

#[test]
fn resource_free_hms_cli_never_invokes_executable_sentinels() {
    const CHILD: &str = "NOVAROCKS_HMS_RESOURCE_FREE_TEST_CHILD";
    const ROOT: &str = "NOVAROCKS_HMS_RESOURCE_FREE_TEST_ROOT";
    const NAME: &str =
        "hms_selection_tests::resource_free_hms_cli_never_invokes_executable_sentinels";
    if std::env::var(CHILD).as_deref() == Ok(NAME) {
        let root = PathBuf::from(std::env::var_os(ROOT).unwrap());
        let marker = root.join("command-called");
        // Prove the guard itself is executable and records a real launch.
        let status = std::process::Command::new("python3").status().unwrap();
        assert_eq!(status.code(), Some(97));
        assert!(fs::read_to_string(&marker).unwrap().contains("python3"));
        fs::remove_file(&marker).unwrap();
        for option in [
            "--list-declarations",
            "--list-suites",
            "--list-extensions",
            "--list-legacy-goldens",
        ] {
            let cli = crate::Cli::try_parse_from([
                "sql-test",
                "--suite",
                "iceberg-hms",
                option,
                "--config",
                root.join("must-not-read-runtime-config.toml")
                    .to_str()
                    .unwrap(),
            ])
            .unwrap();
            assert_eq!(crate::run_cli(cli).unwrap(), 0, "{option}");
            assert!(!marker.exists(), "{option} invoked an external command");
        }
        let help = crate::Cli::try_parse_from(["sql-test", "--help"]).unwrap_err();
        assert_eq!(help.kind(), clap::error::ErrorKind::DisplayHelp);
        assert!(!marker.exists());
        fs::write(root.join("child-completed"), NAME).unwrap();
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let bin = root.join("bin");
    fs::create_dir(&bin).unwrap();
    for command in [
        "python3",
        "docker",
        "spark-submit",
        "bash",
        "sh",
        "curl",
        "novarocks",
    ] {
        let path = bin.join(command);
        fs::write(
            &path,
            "#!/bin/sh\nprintf '%s\\n' \"$0\" >>\"$TRC2_D21_SENTINEL_MARKER\"\nexit 97\n",
        )
        .unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", NAME, "--nocapture"])
        .env(CHILD, NAME)
        .env(ROOT, root)
        .env("PATH", &bin)
        .env("TRC2_D21_SENTINEL_MARKER", root.join("command-called"))
        .env("NOVAROCKS_BIN", bin.join("novarocks"))
        .env("NOVAROCKS_FE_CONFIG", root.join("must-not-read-fe.toml"))
        .env("NOVAROCKS_BE_CONFIG", root.join("must-not-read-be.toml"))
        .env_remove("NOVA_FIXTURE_RUN_CONTEXT")
        .env_remove("NOVA_FIXTURE_RUN_STORAGE")
        .env_remove("NOVA_FIXTURE_CLEANUP_MODE")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(root.join("child-completed")).unwrap(),
        NAME
    );
    assert!(!root.join("command-called").exists());
}

fn case_variables(
    names: &crate::fixture_context::RunNames,
    suite: &str,
    case: &str,
) -> HashMap<String, String> {
    let mut variables = HashMap::new();
    names.extend_suite_variables(suite, &mut variables).unwrap();
    crate::config::case_placeholder_variables(&variables, case).unwrap()
}

#[test]
fn mixed_static_load_failure_and_hms_definition_keep_frozen_ordinals() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let ordinary = root.join("a-static");
    let hms = root.join("iceberg-hms");
    fs::create_dir(&ordinary).unwrap();
    fs::create_dir(&hms).unwrap();
    let valid = ordinary.join("a-valid.sql");
    let invalid = ordinary.join("b-invalid.sql");
    fs::write(&valid, "-- query 1\nSELECT '${uuid0}' AS id;\n").unwrap();
    fs::write(
        &invalid,
        "-- query 1\n-- @order_sensitive=invalid\nSELECT 1;\n",
    )
    .unwrap();
    let hms_path = hms.join(format!("{}.sql", crate::hms_case_input::CASE));
    fs::write(
        &hms_path,
        include_str!(
            "../../../correctness/iceberg-hms/sql/iceberg_hms_read_only_compatibility.sql"
        ),
    )
    .unwrap();
    let budget = RunBudget::admit(
        FixturePolicy {
            scratch_ttl_seconds: 120,
            max_run_seconds: 30,
            cleanup_grace_seconds: 2,
        },
        ScratchIdentity::create("s3://novarocks/frozen-selection-test").unwrap(),
        20,
        None,
    )
    .unwrap();
    let inputs = BTreeSet::from([("iceberg-hms".into(), crate::hms_case_input::CASE.into())]);
    let sources = BTreeMap::from([
        ("a-static".into(), vec![valid.clone(), invalid.clone()]),
        ("iceberg-hms".into(), vec![hms_path.clone()]),
    ]);
    let names =
        crate::fixture_context::RunNames::prepare_with_case_inputs(&budget, &sources, &inputs)
            .unwrap();
    let mut reverse = sources.clone();
    reverse.get_mut("a-static").unwrap().reverse();
    let reversed =
        crate::fixture_context::RunNames::prepare_with_case_inputs(&budget, &reverse, &inputs)
            .unwrap();
    assert_eq!(names.suites["a-static"].ordinal, 0);
    assert_eq!(names.suites["iceberg-hms"].ordinal, 1);
    assert_eq!(names.suites["a-static"].cases["a-valid"], 0);
    assert_eq!(names.suites["a-static"].cases["b-invalid"], 1);
    assert_eq!(
        names.suites["iceberg-hms"].cases[crate::hms_case_input::CASE],
        2
    );
    let meta = regex::Regex::new(r"^--\s*@([a-zA-Z0-9_]+)\s*=\s*(.+?)\s*$").unwrap();
    let marker = regex::Regex::new(r"(?i)^--\s*query\s+(\d+)(?:\s+.*)?$").unwrap();
    let mut variables = HashMap::new();
    names
        .extend_suite_variables("a-static", &mut variables)
        .unwrap();
    assert!(crate::parser::load_sql_case_from_file(&invalid, &meta, &marker, &variables).is_err());
    let case = crate::parser::load_sql_case_from_file(&valid, &meta, &marker, &variables)
        .unwrap()
        .unwrap();
    assert_eq!(case.case_id, "a-valid");
    let definition = crate::hms_case_input::Definition::load(&hms_path, &meta, &marker).unwrap();
    assert_eq!(definition.case_id(), crate::hms_case_input::CASE);
    let available = HashSet::from([
        case.case_id,
        "b-invalid".into(),
        definition.case_id().into(),
    ]);
    for selector in ["a-valid", "b-invalid", crate::hms_case_input::CASE] {
        let selected =
            crate::runner::parse_selector_list(Some(selector), &available, "--only").unwrap();
        assert_eq!(selected, HashSet::from([selector.to_owned()]));
    }
    assert!(
        crate::runner::parse_selector_list(Some("intentionally-unknown"), &available, "--only")
            .unwrap()
            .contains("intentionally-unknown")
    );
    let skip =
        crate::runner::parse_selector_list(Some(crate::hms_case_input::CASE), &available, "--skip")
            .unwrap();
    assert!(skip.contains(crate::hms_case_input::CASE));
    // Selection never re-prepares the complete naming map. Both retained and
    // skipped units keep the same original identity, including a load failure.
    for (suite, case) in [
        ("a-static", "a-valid"),
        ("a-static", "b-invalid"),
        ("iceberg-hms", crate::hms_case_input::CASE),
    ] {
        assert_eq!(
            case_variables(&names, suite, case),
            case_variables(&reversed, suite, case)
        );
    }
    let hms_vars = case_variables(&names, "iceberg-hms", crate::hms_case_input::CASE);
    assert_eq!(
        hms_vars["case_db"],
        crate::fixture_context::resource(&budget.identity().run_id, 'd', 2, 0).unwrap()
    );
    assert_eq!(
        hms_vars["uuid0"],
        crate::fixture_context::resource(&budget.identity().run_id, 'u', 2, 0).unwrap()
    );
    assert!(!root.join("tests/sql/.runtime/hms-evidence").exists());
}
