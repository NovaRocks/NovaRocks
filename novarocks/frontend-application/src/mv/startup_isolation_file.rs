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

//! Reading the deployment's startup isolation statement from a file.
//!
//! The statement is written by whatever stopped the previous process, so it
//! arrives from outside and may arrive after this process is already serving.
//! The file is therefore re-read rather than loaded once: an orchestration
//! that first reads this process's incarnation out of
//! `novarocks_mv_management_status` and then writes the file is the normal
//! case, not a late one.
//!
//! The file carries an operator's authority and must be protected the way the
//! deployment protects its configuration file. Nothing here can enforce that;
//! what it can do is make a statement worthless outside the launch it was
//! written for, which the nonce and the incarnation binding do.
//!
//! An absent file is not an error. It is the default: no statement, no
//! automatic continuation, management stays closed until an operator makes
//! the same statement through `novarocks_mv_resume_management`.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use novarocks_mv_application::management::{
    DeploymentOwner, ManagementTimestamp, ProcessIncarnation, StartupIsolationDeclaration,
    StartupIsolationScope, StartupNonce,
};
use novarocks_spi::connector::{ConnectorInstanceId, ConnectorTableIdentity};
use serde::Deserialize;

/// The largest statement this process will read, so a file cannot be used to
/// make startup allocate without bound.
const MAX_DECLARATION_BYTES: u64 = 64 * 1024;

/// Where a startup isolation statement is read from, and the launch it must
/// name to be accepted.
#[derive(Clone, Debug)]
pub struct StartupIsolationSource {
    path: PathBuf,
    launch_nonce: StartupNonce,
}

impl StartupIsolationSource {
    pub const fn new(path: PathBuf, launch_nonce: StartupNonce) -> Self {
        Self { path, launch_nonce }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub const fn launch_nonce(&self) -> &StartupNonce {
        &self.launch_nonce
    }

    /// Read the current statement, if there is one.
    ///
    /// `Ok(None)` means there is no file; every other problem is an error,
    /// because a file that exists and cannot be understood is a deployment
    /// mistake an operator has to see, not an absent statement.
    pub fn read(&self) -> Result<Option<StartupIsolationDeclaration>, String> {
        let metadata = match std::fs::metadata(&self.path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(format!(
                    "read startup isolation evidence {}: {error}",
                    self.path.display()
                ));
            }
        };
        if metadata.len() > MAX_DECLARATION_BYTES {
            return Err(format!(
                "startup isolation evidence {} exceeds the {MAX_DECLARATION_BYTES}-byte limit",
                self.path.display()
            ));
        }
        let source = std::fs::read_to_string(&self.path).map_err(|error| {
            format!(
                "read startup isolation evidence {}: {error}",
                self.path.display()
            )
        })?;
        parse_declaration(&source).map(Some).map_err(|error| {
            format!(
                "startup isolation evidence {}: {error}",
                self.path.display()
            )
        })
    }
}

/// The file's shape. Unknown keys are refused: a statement whose author
/// believed they wrote something this process did not read is worse than no
/// statement at all.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeclarationFile {
    deployment: String,
    for_incarnation: String,
    nonce: String,
    isolated_incarnations: Vec<String>,
    #[serde(default)]
    all_other_incarnations_settled: bool,
    isolated_at_unix_ms: u64,
    source: String,
    /// Absent means every target this deployment owns. Present means only
    /// these, and every other target stays read-only.
    #[serde(default)]
    target: Vec<TargetFile>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TargetFile {
    catalog: String,
    database: String,
    name: String,
}

fn parse_declaration(source: &str) -> Result<StartupIsolationDeclaration, String> {
    let file: DeclarationFile =
        toml::from_str(source).map_err(|error| format!("parse TOML: {error}"))?;
    let deployment = DeploymentOwner::parse(&file.deployment)
        .map_err(|error| format!("field `deployment`: {error}"))?;
    let for_incarnation = ProcessIncarnation::parse(&file.for_incarnation)
        .map_err(|error| format!("field `for_incarnation`: {error}"))?;
    let nonce =
        StartupNonce::parse(&file.nonce).map_err(|error| format!("field `nonce`: {error}"))?;
    let isolated_incarnations = file
        .isolated_incarnations
        .iter()
        .map(|value| {
            ProcessIncarnation::parse(value)
                .map_err(|error| format!("field `isolated_incarnations`: {error}"))
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    let isolated_at = ManagementTimestamp::from_unix_millis(file.isolated_at_unix_ms);
    let scope = if file.target.is_empty() {
        StartupIsolationScope::Deployment
    } else {
        StartupIsolationScope::Targets(
            file.target
                .iter()
                .map(|target| {
                    Ok(ConnectorTableIdentity {
                        instance_id: ConnectorInstanceId::parse(&target.catalog)
                            .map_err(|error| format!("field `target.catalog`: {error}"))?,
                        namespace: Arc::from(target.database.as_str()),
                        table: Arc::from(target.name.as_str()),
                    })
                })
                .collect::<Result<_, String>>()?,
        )
    };
    Ok(StartupIsolationDeclaration {
        deployment,
        for_incarnation,
        nonce,
        isolated_incarnations,
        scope,
        all_other_incarnations_settled: file.all_other_incarnations_settled,
        isolated_at,
        source: file.source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const COMPLETE: &str = r#"
deployment = "prod-a"
for_incarnation = "0199-new"
nonce = "launch-7f3c"
isolated_incarnations = ["0198-old"]
isolated_at_unix_ms = 1726650000000
source = "systemd: novarocks-fe.service MainPID 1234 exited"
"#;

    #[test]
    fn a_whole_deployment_statement_needs_no_target_list() {
        let declaration = parse_declaration(COMPLETE).expect("a complete statement parses");

        assert_eq!(declaration.deployment.as_str(), "prod-a");
        assert_eq!(declaration.for_incarnation.as_str(), "0199-new");
        assert_eq!(declaration.nonce.as_str(), "launch-7f3c");
        assert_eq!(declaration.scope, StartupIsolationScope::Deployment);
        assert_eq!(
            declaration.isolated_at,
            ManagementTimestamp::from_unix_millis(1_726_650_000_000)
        );
    }

    #[test]
    fn a_deployment_can_explicitly_declare_all_old_effects_settled() {
        let declaration = parse_declaration(&format!(
            "{COMPLETE}\nall_other_incarnations_settled = true\n",
        ))
        .unwrap();
        assert!(declaration.all_other_incarnations_settled);
    }

    #[test]
    fn listed_targets_become_a_scope() {
        let declaration = parse_declaration(&format!(
            "{COMPLETE}\n[[target]]\ncatalog = \"ice\"\ndatabase = \"db\"\nname = \"mv\"\n"
        ))
        .expect("a scoped statement parses");

        let StartupIsolationScope::Targets(targets) = declaration.scope else {
            panic!("a listed target must narrow the scope");
        };
        assert_eq!(targets.len(), 1);
        assert!(targets.iter().any(|table| &*table.table == "mv"));
    }

    #[test]
    fn a_key_this_process_does_not_read_is_refused() {
        let error = parse_declaration(&format!("{COMPLETE}expires_at_unix_ms = 1\n"))
            .expect_err("an unread key means the author stated something that has no effect");

        assert!(error.contains("expires_at_unix_ms"), "{error}");
    }

    #[test]
    fn a_missing_file_is_not_an_error() {
        let source = StartupIsolationSource::new(
            PathBuf::from("/nonexistent/novarocks/startup-isolation.toml"),
            StartupNonce::parse("launch-1").expect("nonce"),
        );

        assert_eq!(
            source.read().expect("an absent statement is the default"),
            None
        );
    }

    #[test]
    fn a_file_that_cannot_be_understood_is_an_error() {
        let directory = tempfile::tempdir().expect("temp dir");
        let path = directory.path().join("startup-isolation.toml");
        std::fs::write(&path, "deployment = 5\n").expect("write");
        let source =
            StartupIsolationSource::new(path, StartupNonce::parse("launch-1").expect("nonce"));

        let error = source
            .read()
            .expect_err("a file that exists and cannot be read is a deployment mistake");
        assert!(error.contains("parse TOML"), "{error}");
    }

    #[test]
    fn a_present_file_is_read_back() {
        let directory = tempfile::tempdir().expect("temp dir");
        let path = directory.path().join("startup-isolation.toml");
        std::fs::write(&path, COMPLETE).expect("write");
        let source =
            StartupIsolationSource::new(path, StartupNonce::parse("launch-7f3c").expect("nonce"));

        let declaration = source
            .read()
            .expect("a readable statement")
            .expect("the file is present");
        assert_eq!(declaration.nonce, *source.launch_nonce());
    }
}
