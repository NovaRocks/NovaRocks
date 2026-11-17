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

//! Same-parent Control projection admission. Source B is a caller-owned union;
//! it is never a request multiplier or a measured private BTree capacity.
use super::ControlCodecError;
use novarocks_type_contract::{
    CompileControlError, ControlOwnedResourceFacts, ControlResourceCounter, control_resource_add,
};
#[derive(Clone, Copy, Debug)]
pub struct ControlProjectionLimits {
    pub max_regions: usize,
    pub max_use_references: usize,
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ControlProjectionFacts {
    pub region_count: usize,
    pub use_reference_count: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
pub(super) struct Model {
    pub counts: [usize; 2],
    pub resources: ControlResourceCounter,
    pub future: ControlOwnedResourceFacts,
    pub planned_child: ControlOwnedResourceFacts,
    source: usize,
    limits: Option<ControlProjectionLimits>,
    input_source: bool,
}
impl Model {
    pub fn new(d: usize, u: usize, b: usize, l: ControlProjectionLimits) -> Self {
        Self {
            counts: [d, u],
            resources: ControlResourceCounter::default(),
            future: ControlOwnedResourceFacts::default(),
            planned_child: ControlOwnedResourceFacts::default(),
            source: b,
            limits: Some(l),
            input_source: false,
        }
    }
    pub fn input_derived(d: usize, u: usize, b: usize) -> Self {
        Self {
            counts: [d, u],
            resources: ControlResourceCounter::default(),
            future: ControlOwnedResourceFacts::default(),
            planned_child: ControlOwnedResourceFacts::default(),
            source: b,
            limits: None,
            input_source: false,
        }
    }
    /// The pure convenience API owns no caller invoice. Its existing input
    /// walk derives only real DTO header/Vec backing, never a fabricated MAX.
    pub fn infer_input_source(&mut self) {
        self.input_source = true;
    }
    pub fn receiving_floor(&mut self, known: usize) -> Result<(), ControlCodecError> {
        if self.input_source {
            self.source = self.source.max(known);
        }
        self.floor(known)
    }
    pub fn facts_with(
        &self,
        child: ControlOwnedResourceFacts,
    ) -> Result<ControlProjectionFacts, ControlCodecError> {
        let mut all = ControlResourceCounter::default();
        all.merge(self.resources.facts())?;
        all.merge(self.future)?;
        // These are the same contribution, not two simultaneous children.
        // Preserve every known planned axis while its real Count prefix grows.
        all.merge(ControlOwnedResourceFacts {
            allocation_requests_upper_bound: child
                .allocation_requests_upper_bound
                .max(self.planned_child.allocation_requests_upper_bound),
            allocation_request_bytes_upper_bound: child
                .allocation_request_bytes_upper_bound
                .max(self.planned_child.allocation_request_bytes_upper_bound),
            cumulative_work_upper_bound: child
                .cumulative_work_upper_bound
                .max(self.planned_child.cumulative_work_upper_bound),
        })?;
        let f = all.facts();
        Ok(ControlProjectionFacts {
            region_count: self.counts[0],
            use_reference_count: self.counts[1],
            allocation_requests_upper_bound: f.allocation_requests_upper_bound,
            allocation_request_bytes_upper_bound: f.allocation_request_bytes_upper_bound,
            coexisting_source_and_request_bytes_upper_bound: control_resource_add(
                self.source,
                f.allocation_request_bytes_upper_bound,
            )?,
            cumulative_work_upper_bound: f.cumulative_work_upper_bound,
        })
    }
    pub fn check_with(
        &self,
        child: ControlOwnedResourceFacts,
    ) -> Result<ControlProjectionFacts, ControlCodecError> {
        let f = self.facts_with(child)?;
        let Some(l) = self.limits else {
            return Ok(f);
        };
        if f.region_count > l.max_regions
            || f.use_reference_count > l.max_use_references
            || f.allocation_requests_upper_bound > l.max_allocation_requests
            || f.allocation_request_bytes_upper_bound > l.max_allocation_request_bytes
            || f.coexisting_source_and_request_bytes_upper_bound
                > l.max_coexisting_source_and_request_bytes
            || f.cumulative_work_upper_bound > l.max_work
        {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        Ok(f)
    }
    pub fn gate_with(
        &self,
        child: ControlOwnedResourceFacts,
        admit: &mut impl FnMut(&ControlProjectionFacts) -> Result<(), CompileControlError>,
    ) -> Result<ControlProjectionFacts, ControlCodecError> {
        let facts = self.check_with(child)?;
        admit(&facts)?;
        Ok(facts)
    }
    pub fn gate(
        &self,
        admit: &mut impl FnMut(&ControlProjectionFacts) -> Result<(), CompileControlError>,
    ) -> Result<ControlProjectionFacts, ControlCodecError> {
        self.gate_with(ControlOwnedResourceFacts::default(), admit)
    }
    pub fn floor(&self, n: usize) -> Result<(), ControlCodecError> {
        if self.source < n {
            return Err(ControlCodecError::InvalidShape(
                "Control source invoice is below necessary backing",
            ));
        }
        Ok(())
    }
}
pub(super) fn unbounded() -> ControlProjectionLimits {
    ControlProjectionLimits {
        max_regions: usize::MAX,
        max_use_references: usize::MAX,
        max_allocation_requests: usize::MAX,
        max_allocation_request_bytes: usize::MAX,
        max_coexisting_source_and_request_bytes: usize::MAX,
        max_work: usize::MAX,
    }
}
