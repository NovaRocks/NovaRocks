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

use crate::performance::{PerformanceScenario, Uea1WorkloadManifest};
use crate::scenario::{Scenario, ScenarioContext};
use anyhow::{Context, Result};

struct Uea1PerformanceScenario(PerformanceScenario);

impl Scenario for Uea1PerformanceScenario {
    fn name(&self) -> &'static str {
        self.0.name()
    }

    fn is_explicit_stage(&self) -> bool {
        true
    }

    fn run(&self, context: &mut ScenarioContext) -> Result<()> {
        let path = context
            .uea1_workload_manifest()
            .context("UEA-1 performance scenarios require --uea1-workload-manifest")?
            .to_path_buf();
        let manifest = Uea1WorkloadManifest::load(&path)?;
        crate::performance::run(self.0, context, &manifest)
    }
}

pub fn scenarios() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(Uea1PerformanceScenario(
            PerformanceScenario::ShortConcurrent,
        )),
        Box::new(Uea1PerformanceScenario(PerformanceScenario::Mixed)),
        Box::new(Uea1PerformanceScenario(PerformanceScenario::SlowOutput)),
    ]
}
