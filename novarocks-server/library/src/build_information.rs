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

#[cfg(novarocks_dev_controls)]
const BUILD_KIND: &str = "test-controls";
#[cfg(not(novarocks_dev_controls))]
const BUILD_KIND: &str = "normal-build";

#[path = "../../../build-support/information.rs"]
mod information;
pub use information::BuildInformation;

fn owners() -> Vec<(&'static str, &'static [&'static str])> {
    vec![
        crate::compiled_test_capabilities(),
        novarocks_failpoint::compiled_test_capabilities(),
        novarocks_frontend_application::compiled_test_capabilities(),
        novarocks_query_application::compiled_test_capabilities(),
        novarocks_native_adapter::compiled_test_capabilities(),
        novarocks_worker::compiled_test_capabilities(),
        novarocks_execution::compiled_test_capabilities(),
        novarocks_fs::compiled_test_capabilities(),
        novarocks_connector_iceberg::compiled_test_capabilities(),
        novarocks_mv_application::compiled_test_capabilities(),
        novarocks_version::compiled_test_capabilities(),
    ]
}

fn known(value: &str) -> serde_json::Value {
    if value.is_empty() {
        serde_json::Value::Null
    } else {
        value.into()
    }
}

// Design: ADR-0174 (docs/adr/ADR-0174-test-capabilities-and-finite-role-authorization.md)
pub fn value(build: &BuildInformation) -> serde_json::Value {
    let mut capabilities = owners().into_iter().map(|(owner, compiled)|
        serde_json::json!({"owner": owner, "compiled_capabilities": compiled}))
        .collect::<Vec<_>>();
    capabilities.sort_by(|a, b| a["owner"].as_str().cmp(&b["owner"].as_str()));
    serde_json::json!({
        "schema_version": 1,
        "package": build.package,
        "version": build.version,
        "source_commit": known(build.source_commit),
        "explicit_build_identity": known(build.explicit_identity),
        "native_build_identity": known(build.native_build_identity),
        "build_kind": BUILD_KIND,
        "capabilities": capabilities,
        "declared_profile": serde_json::Value::Null,
        "package_profile": {
            "opt_level": known(build.opt_level),
            "debug_info": known(build.debug_info)
        },
        "actual_cfg": {
            "target": known(build.target),
            "debug_assertions": build.debug_assertions,
            "panic": if build.panic_unwind { "unwind" } else { "abort" },
            "pointer_width": usize::BITS,
            "target_os": std::env::consts::OS,
            "target_arch": std::env::consts::ARCH
        },
        "rustc_identity": known(build.rustc_identity),
        "explicitly_unknown_settings": ["final_lto", "final_codegen_units", "dependency_profile_overrides", "additional_compiler_flag_effects"],
        "release_certification": "not-certified-by-self-report"
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn local_cfg_and_unknown_facts_remain_separate() {
        let value = super::value(&super::BuildInformation::fixture());
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["package"], "fixture-cli");
        assert_eq!(value["version"], "0.1.0");
        assert_eq!(value["source_commit"], serde_json::Value::Null);
        assert_eq!(value["explicit_build_identity"], "fixture-identity");
        assert_eq!(value["native_build_identity"], "fixture-identity");
        assert_eq!(value["declared_profile"], serde_json::Value::Null);
        assert_eq!(value["actual_cfg"]["target"], "fixture-target");
        assert_eq!(value["package_profile"]["opt_level"], "0");
        assert_eq!(value["rustc_identity"], "fixture-rustc");
        assert_eq!(value["actual_cfg"]["debug_assertions"], false);
        assert_eq!(value["actual_cfg"]["panic"], "unwind");
        assert_eq!(super::known(""), serde_json::Value::Null);
        assert!(
            !value["explicitly_unknown_settings"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }
}
