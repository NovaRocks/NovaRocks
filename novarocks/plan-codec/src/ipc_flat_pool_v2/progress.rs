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

//! Snapshots of the existing flat writer contribution, replaced by the parent.
use super::{
    FlatPoolWriteFacts, FlatPoolWriteLimits,
    geometry::{Geometry, add, mul},
};
use crate::host_projection_v2::ProjectionFailure;
use crate::physical_type_v2::TypeCodecError;
use novarocks_type_contract::CompileControlError;
type HostError<H> = ProjectionFailure<TypeCodecError, H>;
pub(crate) struct Admission<'a, 'b, H> {
    pub parent: &'a mut (dyn FnMut(&FlatPoolWriteFacts) -> Result<(), HostError<H>> + 'b),
    pub limits: FlatPoolWriteLimits,
    pub facts: FlatPoolWriteFacts,
}
impl<H> Admission<'_, '_, H> {
    pub fn initial(
        source: usize,
        rows: usize,
        limits: FlatPoolWriteLimits,
    ) -> Result<FlatPoolWriteFacts, TypeCodecError> {
        let policy = crate::ipc_flat_pool_v2::WriterAdmissionPolicy(true);
        policy.cap(
            rows,
            limits.max_rows.min(i64::MAX as usize),
            "flat pool writer row envelope exceeded",
        )?;
        Ok(FlatPoolWriteFacts {
            source_retained_bytes: source,
            rows,
            buffer_descriptors: 0,
            variadic_buffers: 0,
            body_bytes: 0,
            batch_backing_bytes_upper_bound: 0,
            encoded_stream_bytes_upper_bound: 0,
            new_allocation_request_bytes_upper_bound: 0,
            allocation_request_count_upper_bound: 0,
            coexisting_source_and_request_bytes_upper_bound: source,
            cumulative_library_work_upper_bound: 0,
        })
    }
    pub fn gate(&mut self) -> Result<(), HostError<H>> {
        let f = &self.facts;
        let l = &self.limits;
        if f.rows > l.max_rows
            || f.buffer_descriptors > l.max_buffer_descriptors
            || f.body_bytes > l.max_body_bytes
            || f.encoded_stream_bytes_upper_bound > l.max_encoded_stream_bytes
            || f.new_allocation_request_bytes_upper_bound > l.max_new_allocation_request_bytes
            || f.coexisting_source_and_request_bytes_upper_bound
                > l.max_coexisting_source_and_request_bytes
            || f.cumulative_library_work_upper_bound > l.max_cumulative_library_work
        {
            return Err((CompileControlError::ResourceExhausted).into());
        }
        (self.parent)(&self.facts)
    }
    pub fn construction(
        &mut self,
        requests: super::allocations::Requests,
        work_upper_bound: usize,
    ) -> Result<(), HostError<H>> {
        self.facts.new_allocation_request_bytes_upper_bound = self
            .facts
            .new_allocation_request_bytes_upper_bound
            .max(requests.bytes);
        self.facts.allocation_request_count_upper_bound = self
            .facts
            .allocation_request_count_upper_bound
            .max(requests.count);
        self.facts.cumulative_library_work_upper_bound = self
            .facts
            .cumulative_library_work_upper_bound
            .max(work_upper_bound);
        self.facts.coexisting_source_and_request_bytes_upper_bound = add(
            self.facts.source_retained_bytes,
            self.facts.new_allocation_request_bytes_upper_bound,
        )
        .map_err(|_| CompileControlError::ResourceExhausted)?;
        self.gate()
    }
    pub fn geometry(&mut self, g: &Geometry) -> Result<(), HostError<H>> {
        self.facts.rows = g.rows;
        self.facts.buffer_descriptors = g.buffers;
        self.facts.variadic_buffers = g.variadic;
        self.facts.body_bytes = g.body_bytes;
        self.facts.encoded_stream_bytes_upper_bound = self
            .facts
            .encoded_stream_bytes_upper_bound
            .max(g.body_bytes);
        let own = (|| add(mul(3, g.rows)?, mul(4, g.buffers)?))()
            .map_err(|_| CompileControlError::ResourceExhausted)?;
        self.facts.cumulative_library_work_upper_bound =
            self.facts.cumulative_library_work_upper_bound.max(own);
        self.gate()?;
        Ok(())
    }
    pub fn complete(&mut self, f: FlatPoolWriteFacts) -> Result<FlatPoolWriteFacts, HostError<H>> {
        let old = self.facts;
        self.facts = f;
        self.facts.new_allocation_request_bytes_upper_bound = f
            .new_allocation_request_bytes_upper_bound
            .max(old.new_allocation_request_bytes_upper_bound);
        self.facts.allocation_request_count_upper_bound = f
            .allocation_request_count_upper_bound
            .max(old.allocation_request_count_upper_bound);
        self.facts.cumulative_library_work_upper_bound = f
            .cumulative_library_work_upper_bound
            .max(old.cumulative_library_work_upper_bound);
        self.facts.coexisting_source_and_request_bytes_upper_bound = add(
            f.source_retained_bytes,
            self.facts.new_allocation_request_bytes_upper_bound,
        )
        .map_err(|_| CompileControlError::ResourceExhausted)?;
        self.gate()?;
        Ok(self.facts)
    }
}
