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
//! Pipeline structure metadata.
//!
//! Responsibilities:
//! - Represents one pipeline with ordered operator factories and dependency edges.
//! - Carries execution properties used by scheduler and driver construction.
//!
//! Key exported interfaces:
//! - Types: `Pipeline`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::Arc;

use super::driver::PipelineDriver;
use super::fragment_context::FragmentContext;
use super::operator_factory::OperatorFactory;
use crate::novarocks_logging::debug;
use crate::runtime::profile::OperatorProfiles;

/// One pipeline definition containing ordered operator factories and dependency links.
pub struct Pipeline {
    id: i32,
    op_factories: Vec<Box<dyn OperatorFactory>>,
    dop: i32,
}

impl Pipeline {
    pub fn new(id: i32, op_factories: Vec<Box<dyn OperatorFactory>>, dop: i32) -> Self {
        Self {
            id,
            op_factories,
            dop: dop.max(1),
        }
    }

    pub fn id(&self) -> i32 {
        self.id
    }

    pub fn dop(&self) -> i32 {
        self.dop
    }

    pub(crate) fn instantiate_drivers(
        &self,
        ctx: &Arc<FragmentContext>,
    ) -> Result<Vec<PipelineDriver>, String> {
        let mut drivers = Vec::new();
        let pipeline_profiler = ctx
            .profiler()
            .map(|p| p.child(format!("Pipeline (id={})", self.id)));
        for i in 0..self.dop {
            let mut operators = Vec::with_capacity(self.op_factories.len());
            let mut operator_profiles = Vec::new();
            let mut source_idx = None;
            let mut sink_idx = None;
            let driver_id = ctx.next_driver_id();
            if self
                .op_factories
                .iter()
                .any(|factory| factory.name().contains("LOCAL_EXCHANGE_SINK"))
            {
                let op_names = self
                    .op_factories
                    .iter()
                    .map(|factory| factory.name())
                    .collect::<Vec<_>>()
                    .join(" -> ");
                debug!(
                    "Pipeline driver created: pipeline_id={} driver_id={} local_index={} dop={} operators={}",
                    self.id, driver_id, i, self.dop, op_names
                );
            }
            let driver_profiler = pipeline_profiler
                .as_ref()
                .map(|p| p.child(format!("PipelineDriver (id={})", driver_id)));
            for (idx, factory) in self.op_factories.iter().enumerate() {
                if factory.is_source() {
                    if source_idx.is_some() {
                        return Err("pipeline has multiple source operators".to_string());
                    }
                    source_idx = Some(idx);
                }
                if factory.is_sink() {
                    if sink_idx.is_some() {
                        return Err("pipeline has multiple sink operators".to_string());
                    }
                    sink_idx = Some(idx);
                }
                let mut op = factory.create(self.dop, i);
                if let Some(driver_profiler) = driver_profiler.as_ref() {
                    let op_profile = driver_profiler.child(op.name().to_string());
                    let profiles = OperatorProfiles::new(op_profile);
                    op.set_profiles(profiles.clone());
                    operator_profiles.push(profiles);
                }
                op.prepare()?;
                operators.push(op);
            }
            let source_idx =
                source_idx.ok_or_else(|| "pipeline missing source operator".to_string())?;
            let sink_idx = sink_idx.ok_or_else(|| "pipeline missing sink operator".to_string())?;
            if source_idx != 0 {
                return Err("pipeline source must be the first operator".to_string());
            }
            if sink_idx + 1 != operators.len() {
                return Err("pipeline sink must be the last operator".to_string());
            }
            drivers.push(PipelineDriver::new(
                driver_id,
                operators,
                driver_profiler,
                operator_profiles,
                std::sync::Arc::clone(ctx.runtime_state()),
                ctx.fragment_instance_id(),
            ));
        }
        Ok(drivers)
    }
}
