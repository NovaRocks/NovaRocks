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

use std::fs;

use novarocks_frontend::catalog_application::{
    CatalogDesiredStateSourceInput, CatalogDesiredStateSourceMode,
};
use novarocks_server::app_config::NovaRocksConfig;

fn native_trust() -> &'static str {
    r#"
[native_trust]
deployment_id = "catalog-source-test"
shared_secret = "test-secret"
"#
}

fn fe_config(catalog_source: &str) -> String {
    format!(
        r#"
[cluster]
role = "fe"
{catalog_source}
{}"#,
        native_trust()
    )
}

#[test]
fn frontend_static_file_preflight_resolves_relative_path_and_retains_snapshot() {
    let temp = tempfile::tempdir().expect("temporary directory");
    let catalogs = temp.path().join("catalogs.toml");
    fs::write(
        &catalogs,
        r#"
format_version = 3
[[catalogs]]
instance_id = "catalog.analytics"
provider_id = "iceberg"
display_name = "Analytics"
config_format_version = 3
credential_bindings = []
[catalogs.properties]
type = "iceberg"
"#,
    )
    .expect("catalog fixture");
    let config = temp.path().join("novarocks-fe.toml");
    fs::write(
        &config,
        fe_config(
            r#"
[catalog_source]
static_file_path = "catalogs.toml"
"#,
        ),
    )
    .expect("server config");

    let loaded = NovaRocksConfig::load_deployable_from_file(&config).expect("valid FE config");
    let source = loaded.catalog_source.expect("FE source");
    assert_eq!(source.mode(), CatalogDesiredStateSourceMode::StaticFile);
    let canonical_catalogs = fs::canonicalize(&catalogs).expect("canonical catalog fixture");
    assert_eq!(
        source.static_file_path(),
        Some(canonical_catalogs.as_path())
    );
    let CatalogDesiredStateSourceInput::StaticFile(snapshot) =
        source.input().expect("source input")
    else {
        panic!("expected StaticFile input");
    };
    assert_eq!(snapshot.len(), 1);
}

#[test]
fn deployable_role_cross_validation_is_closed_and_fail_closed() {
    let temp = tempfile::tempdir().expect("temporary directory");
    let missing_static = temp.path().join("missing-static.toml");
    fs::write(&missing_static, "format_version = 2\ncatalogs = []\n").expect("fixture");
    let config = temp.path().join("novarocks-fe.toml");

    fs::write(
        &config,
        fe_config(
            r#"
[catalog_source]
mode = "dynamic-state-store"
"#,
        ),
    )
    .expect("dynamic config");
    let error = match NovaRocksConfig::load_deployable_from_file(&config) {
        Ok(_) => panic!("dynamic mode without StateStore must fail"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("requires [state_store]"));

    fs::write(
        &config,
        fe_config(
            r#"
[catalog_source]
mode = "managed-controller"
"#,
        ),
    )
    .expect("managed config");
    let error = match NovaRocksConfig::load_deployable_from_file(&config) {
        Ok(_) => panic!("managed mode is not implemented"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("UnsupportedSourceMode"));

    fs::write(
        &config,
        format!(
            r#"
[cluster]
role = "be"
frontend_endpoint = "127.0.0.1:9000"

[catalog_source]
static_file_path = "{}"
{}"#,
            missing_static.display(),
            native_trust()
        ),
    )
    .expect("BE config");
    let error = match NovaRocksConfig::load_deployable_from_file(&config) {
        Ok(_) => panic!("BE must reject FE source"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("only valid for [cluster].role=fe")
    );
}
