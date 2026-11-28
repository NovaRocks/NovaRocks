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

//! One receiver contribution borrowed by its containing namespace. Snapshots
//! replace that contribution; geometry is included once in complete requests.

use novarocks_type_contract::CompileControlError;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct IpcReaderProgressFacts {
    pub source_retained_bytes: usize,
    pub geometry_scratch_request_bytes: usize,
    pub geometry_scratch_request_count: usize,
    pub allocation_request_count_upper_bound: usize,
    pub new_allocation_request_bytes_upper_bound: usize,
    pub cumulative_library_work_upper_bound: usize,
}

pub(crate) type ReaderAdmit<'a, E = CompileControlError> =
    dyn FnMut(&IpcReaderProgressFacts) -> Result<(), E> + 'a;

pub(crate) struct Admission<'a, 'b, E = CompileControlError> {
    admit: &'a mut ReaderAdmit<'b, E>,
    facts: IpcReaderProgressFacts,
    requests: [(usize, usize); 6],
    constant: (usize, usize, usize),
    reader_work: usize,
    geometry_work: usize,
    count_work: [usize; 5],
    clone_work: usize,
    sealed: Option<IpcReaderProgressFacts>,
    limits: Option<(usize, usize, usize)>,
    construction: Option<&'a mut dyn FnMut(std::alloc::Layout) -> Result<(), E>>,
}
impl<'a, 'b, E: From<CompileControlError>> Admission<'a, 'b, E> {
    pub(crate) fn new(source: usize, admit: &'a mut ReaderAdmit<'b, E>) -> Self {
        Self {
            admit,
            facts: IpcReaderProgressFacts {
                source_retained_bytes: source,
                ..Default::default()
            },
            requests: [(0, 0); 6],
            constant: (0, 0, 0),
            reader_work: 0,
            geometry_work: 0,
            count_work: [0; 5],
            clone_work: 0,
            sealed: None,
            limits: None,
            construction: None,
        }
    }
    pub(crate) fn with_construction(
        mut self,
        request: Option<&'a mut dyn FnMut(std::alloc::Layout) -> Result<(), E>>,
    ) -> Self {
        self.construction = request;
        self
    }
    pub(crate) fn construction_request(&mut self, layout: std::alloc::Layout) -> Result<(), E> {
        if let Some(request) = &mut self.construction {
            request(layout)?;
        }
        Ok(())
    }
    pub(crate) fn limits(&mut self, bytes: usize, coexist: usize, work: usize) -> Result<(), E> {
        self.limits = Some((bytes, coexist, work));
        self.check()
    }
    pub(crate) fn requests(&mut self, part: usize, bytes: usize, count: usize) -> Result<(), E> {
        self.requests[part].0 = self.requests[part].0.max(bytes);
        self.requests[part].1 = self.requests[part].1.max(count);
        if part == 0 {
            self.facts.geometry_scratch_request_bytes = self.requests[part].0;
            self.facts.geometry_scratch_request_count = self.requests[part].1;
        }
        self.check()
    }
    pub(crate) fn seed_prefix(&mut self, facts: IpcReaderProgressFacts) -> Result<(), E> {
        self.requests[0] = (
            facts.geometry_scratch_request_bytes,
            facts.geometry_scratch_request_count,
        );
        self.requests[4] = (
            facts
                .new_allocation_request_bytes_upper_bound
                .checked_sub(facts.geometry_scratch_request_bytes)
                .ok_or(CompileControlError::ResourceExhausted)?,
            facts
                .allocation_request_count_upper_bound
                .checked_sub(facts.geometry_scratch_request_count)
                .ok_or(CompileControlError::ResourceExhausted)?,
        );
        self.facts.geometry_scratch_request_bytes = facts.geometry_scratch_request_bytes;
        self.facts.geometry_scratch_request_count = facts.geometry_scratch_request_count;
        self.geometry_work = facts.cumulative_library_work_upper_bound;
        self.check()
    }
    pub(crate) fn count_work(&mut self, part: usize, upper: usize) -> Result<(), E> {
        self.count_work[part] = self.count_work[part].max(upper);
        self.check()
    }
    pub(crate) fn clone_work(&mut self, upper: usize) -> Result<(), E> {
        self.clone_work = self.clone_work.max(upper);
        self.check()
    }
    pub(crate) fn batch_work_add(&mut self, upper: usize) -> Result<(), E> {
        self.geometry_work = self
            .geometry_work
            .checked_add(upper)
            .ok_or(CompileControlError::ResourceExhausted)?;
        self.check()
    }
    pub(crate) fn batch_step(
        &mut self,
        work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<(), E> {
        self.batch_work_add(1)?;
        work.step().map_err(Into::into)
    }
    pub(crate) fn seed(&mut self, facts: IpcReaderProgressFacts) -> Result<(), E> {
        self.requests[0] = (
            facts.geometry_scratch_request_bytes,
            facts.geometry_scratch_request_count,
        );
        self.requests[2] = (
            facts
                .new_allocation_request_bytes_upper_bound
                .checked_sub(facts.geometry_scratch_request_bytes)
                .ok_or(CompileControlError::ResourceExhausted)?,
            facts
                .allocation_request_count_upper_bound
                .checked_sub(facts.geometry_scratch_request_count)
                .ok_or(CompileControlError::ResourceExhausted)?,
        );
        self.facts.geometry_scratch_request_bytes = facts.geometry_scratch_request_bytes;
        self.facts.geometry_scratch_request_count = facts.geometry_scratch_request_count;
        self.reader_work = facts.cumulative_library_work_upper_bound;
        self.sealed = Some(facts);
        self.check()
    }
    pub(crate) fn source(&self) -> usize {
        self.facts.source_retained_bytes
    }
    pub(crate) fn numeric<T>(
        &self,
        result: Result<T, super::FlatPoolResourceError>,
    ) -> Result<T, super::FlatPoolResourceError> {
        result.map_err(|_| CompileControlError::ResourceExhausted.into())
    }
    pub(crate) fn reader_work(&mut self, upper: usize) -> Result<(), E> {
        self.reader_work = self.reader_work.max(upper);
        self.check()
    }
    pub(crate) fn constant(&mut self, bytes: usize, count: usize, work: usize) -> Result<(), E> {
        self.constant.0 = self.constant.0.max(bytes);
        self.constant.1 = self.constant.1.max(count);
        self.constant.2 = self.constant.2.max(work);
        self.check()
    }
    pub(crate) fn check(&mut self) -> Result<(), E> {
        let add = |a: usize, b: usize| {
            a.checked_add(b)
                .ok_or(CompileControlError::ResourceExhausted)
        };
        let mut bytes = 0;
        let mut count = 0;
        for &(b, c) in &self.requests {
            bytes = add(bytes, b)?;
            count = add(count, c)?;
        }
        // Constant observations describe allocations already included by the
        // reader's structural/payload authors, not a second pool contribution.
        self.facts.new_allocation_request_bytes_upper_bound = bytes.max(self.constant.0);
        self.facts.allocation_request_count_upper_bound = count.max(self.constant.1);
        let count_work = self
            .count_work
            .iter()
            .try_fold(0usize, |sum, &n| add(sum, n))?;
        self.facts.cumulative_library_work_upper_bound = add(
            add(self.clone_work, count_work)?,
            add(self.geometry_work, self.reader_work.max(self.constant.2))?,
        )?;
        let coexist = add(
            self.facts.source_retained_bytes,
            self.facts.new_allocation_request_bytes_upper_bound,
        )?;
        if let Some((b, c, w)) = self.limits
            && (self.facts.new_allocation_request_bytes_upper_bound > b
                || coexist > c
                || self.facts.cumulative_library_work_upper_bound > w)
        {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        if let Some(full) = self.sealed
            && (self.facts.source_retained_bytes != full.source_retained_bytes
                || self.facts.geometry_scratch_request_bytes > full.geometry_scratch_request_bytes
                || self.facts.geometry_scratch_request_count > full.geometry_scratch_request_count
                || self.facts.new_allocation_request_bytes_upper_bound
                    > full.new_allocation_request_bytes_upper_bound
                || self.facts.allocation_request_count_upper_bound
                    > full.allocation_request_count_upper_bound
                || self.facts.cumulative_library_work_upper_bound
                    > full.cumulative_library_work_upper_bound)
        {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        (self.admit)(&self.facts)
    }
    pub(crate) fn facts(&self) -> IpcReaderProgressFacts {
        self.facts
    }
}

/// Exact caller failure used by the shared borrowed framing and geometry body.
pub(crate) trait ReaderGeometryFailure:
    From<CompileControlError>
    + From<crate::physical_type_v2::TypeCodecError>
    + From<novarocks_type_contract::ValueTypeError>
{
}
impl<E> ReaderGeometryFailure for E where
    E: From<CompileControlError>
        + From<crate::physical_type_v2::TypeCodecError>
        + From<novarocks_type_contract::ValueTypeError>
{
}
