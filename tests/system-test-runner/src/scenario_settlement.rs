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

use crate::scenario::Scenario;
use anyhow::{Context, Result};
use novarocks_cluster_harness::resource_registration::{self, RegistrationSnapshot};
use serde_json::{Value, json};
use std::cell::{Cell, RefCell};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static NEXT_RECORD: AtomicU64 = AtomicU64::new(1);

/// Records observations independently from scenario success and the fixture's
/// teardown slot. An empty slot makes no statement about owner registrations.
pub(crate) struct ScenarioSettlement {
    root: PathBuf,
    before: RegistrationSnapshot,
    record: RefCell<Value>,
    written: Cell<bool>,
}

impl ScenarioSettlement {
    pub(crate) fn new(scenario: &str, root: &Path) -> Result<Self> {
        Ok(Self {
            root: root.to_owned(),
            before: resource_registration::list(root)?,
            written: Cell::new(false),
            record: RefCell::new(json!({
                "schema": 1, "scenario": scenario,
                "fixture": {"launch_config": "failed", "teardown": null, "resources": []},
                "cluster": {"launch": "not_attempted", "shutdown": "not_applicable", "error": null, "partial_cleanup": null}
            })),
        })
    }

    pub(crate) fn record_reference(
        &self,
        reference: &novarocks_cluster_harness::reference_inputs::ResolvedReference,
    ) -> Result<()> {
        self.record.borrow_mut()["reference"] = serde_json::to_value(reference)?;
        Ok(())
    }

    pub(crate) fn launch_config_succeeded(&self) {
        self.record.borrow_mut()["fixture"]["launch_config"] = json!("ok");
    }

    pub(crate) fn teardown(&self, scenario: &dyn Scenario) -> Result<()> {
        let result = scenario.teardown();
        self.record.borrow_mut()["fixture"]["teardown"] = json!({
            "status": if result.is_ok() { "ok" } else { "failed" },
            "error": result.as_ref().err().map(|error| format!("{error:#}")),
        });
        result
    }

    pub(crate) fn cluster_launch_attempted(&self) {
        let mut record = self.record.borrow_mut();
        record["cluster"]["launch"] = json!("failed");
        record["cluster"]["partial_cleanup"] = json!("monitor");
    }

    pub(crate) fn cluster_launch_failed(&self, error: &anyhow::Error) {
        self.record.borrow_mut()["cluster"]["error"] = json!(format!("{error:#}"));
    }

    pub(crate) fn cluster_launched(&self) {
        let mut record = self.record.borrow_mut();
        record["cluster"]["launch"] = json!("ok");
        record["cluster"]["partial_cleanup"] = Value::Null;
    }

    pub(crate) fn cluster_shutdown(&self, result: &Result<()>) {
        let mut record = self.record.borrow_mut();
        record["cluster"]["shutdown"] = json!(if result.is_ok() { "ok" } else { "failed" });
        record["cluster"]["error"] = json!(result.as_ref().err().map(|error| format!("{error:#}")));
    }

    pub(crate) fn write(&self) -> Result<()> {
        if self.written.get() {
            return Ok(());
        }
        let resources =
            resource_registration::collect(&self.root, &self.before, std::process::id())?;
        let mut record = self.record.borrow_mut();
        record["fixture"]["resources"] = serde_json::to_value(resources)?;
        record["recorded_at"] = json!(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis());
        anyhow::ensure!(
            !record["fixture"]["teardown"].is_null(),
            "scenario fixture teardown was not observed"
        );
        let sequence = NEXT_RECORD.fetch_add(1, Ordering::Relaxed);
        let temporary = self.root.join(format!(
            ".scenario-teardown-{}-{sequence}.tmp",
            std::process::id()
        ));
        let write = (|| -> Result<()> {
            let mut options = OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&temporary)?;
            serde_json::to_writer(&mut file, &*record)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            fs::rename(&temporary, self.root.join("scenario-teardown.json"))?;
            File::open(&self.root)?.sync_all()?;
            Ok(())
        })();
        if write.is_err() {
            let _ = fs::remove_file(temporary);
        } else {
            self.written.set(true);
        }
        write.context("write scenario teardown record")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scenario::ScenarioContext;

    struct Fixture {
        fails: bool,
    }
    impl Scenario for Fixture {
        fn name(&self) -> &'static str {
            "component/settlement"
        }
        fn run(&self, _: &mut ScenarioContext) -> Result<()> {
            unreachable!()
        }
        fn teardown(&self) -> Result<()> {
            anyhow::ensure!(!self.fails, "actual fixture cleanup failure");
            Ok(())
        }
    }

    fn read(root: &Path) -> Value {
        serde_json::from_slice(&fs::read(root.join("scenario-teardown.json")).unwrap()).unwrap()
    }

    #[test]
    fn passing_and_assertion_failure_cleanup_facts_are_independent_of_outcome() {
        // Both outcomes use exactly the same owner observations. Assertion
        // verdicts are intentionally absent from this cleanup-only record.
        for _assertions_passed in [true, false] {
            for fixture_fails in [true, false] {
                for cluster_fails in [true, false] {
                    let root = tempfile::tempdir().unwrap();
                    let observation =
                        ScenarioSettlement::new("component/settlement", root.path()).unwrap();
                    observation.launch_config_succeeded();
                    observation.cluster_launch_attempted();
                    observation.cluster_launched();
                    observation.cluster_shutdown(&if cluster_fails {
                        Err(anyhow::anyhow!("actual cluster cleanup failure"))
                    } else {
                        Ok(())
                    });
                    let _ = observation.teardown(&Fixture {
                        fails: fixture_fails,
                    });
                    observation.write().unwrap();
                    let record = read(root.path());
                    assert_eq!(record["fixture"]["launch_config"], "ok");
                    assert_eq!(
                        record["fixture"]["teardown"]["status"],
                        if fixture_fails { "failed" } else { "ok" }
                    );
                    assert_eq!(record["cluster"]["launch"], "ok");
                    assert_eq!(
                        record["cluster"]["shutdown"],
                        if cluster_fails { "failed" } else { "ok" }
                    );
                    assert!(record["cluster"]["partial_cleanup"].is_null());
                }
            }
        }
    }

    #[test]
    fn normal_and_failed_settlements_keep_only_the_scenarios_reference_components() {
        use novarocks_cluster_harness::reference_inputs::{
            ArtifactRef, ReferenceInput, ResolvedReference, TrinoImageRef,
        };
        let reference = ResolvedReference {
            trino_image: Some(TrinoImageRef {
                alias: "declared-alias".into(),
                image_id: "exact-image".into(),
                version: "declared-version".into(),
            }),
            trino_jdbc_driver: Some(ArtifactRef {
                name: "driver.jar".into(),
                path: "/declared/driver.jar".into(),
                sha256: "driver-hash".into(),
            }),
        };
        for fixture_fails in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let observation = ScenarioSettlement::new("component/settlement", root.path()).unwrap();
            observation
                .record_reference(
                    &reference.subset(&[ReferenceInput::TrinoImage].into_iter().collect()),
                )
                .unwrap();
            let _ = observation.teardown(&Fixture {
                fails: fixture_fails,
            });
            observation.write().unwrap();
            let record = read(root.path());
            assert_eq!(
                record["reference"]["trino_image"]["image_id"],
                "exact-image"
            );
            assert_eq!(
                record["reference"]["trino_image"]["version"],
                "declared-version"
            );
            assert!(record["reference"].get("trino_jdbc_driver").is_none());
        }
    }

    #[test]
    fn partially_started_cluster_failure_defers_process_cleanup_to_monitor() {
        let root = tempfile::tempdir().unwrap();
        let observation = ScenarioSettlement::new("component/settlement", root.path()).unwrap();
        observation.launch_config_succeeded();
        let mut registration =
            resource_registration::register(root.path(), "fixture", "released", "released-project")
                .unwrap();
        registration.settle(&Ok(())).unwrap();
        observation.cluster_launch_attempted();
        observation.cluster_launch_failed(&anyhow::anyhow!("partial startup failure"));
        observation.teardown(&Fixture { fails: false }).unwrap();
        observation.write().unwrap();
        let record = read(root.path());
        assert_eq!(record["cluster"]["launch"], "failed");
        assert_eq!(record["cluster"]["partial_cleanup"], "monitor");
        assert_eq!(record["cluster"]["shutdown"], "not_applicable");
        assert_eq!(record["cluster"]["error"], "partial startup failure");
        assert_eq!(record["fixture"]["resources"][0]["state"], "released");
    }
}
