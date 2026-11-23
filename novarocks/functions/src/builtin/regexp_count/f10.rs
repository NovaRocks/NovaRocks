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

//! Controlled compilation and ordered matching for the Trino pattern subset.

use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_input::EvaluationCheckpoints;
use crate::pattern_memo::{ControlledPatternMemo, DeterministicPatternFailure};
use crate::{AggregateStateAllocator, KernelFailure};
use allocator_api2::vec::Vec as HostVec;
use std::sync::Arc;

mod batch;
mod parse;
mod program;
mod rows;
mod search;
#[cfg(test)]
mod tests;

use parse::PatternRejection;
use program::Program;
use search::RegexpSearch;

pub(crate) use batch::RegexpOperation;
pub use batch::{PreparedRegexp, RegexpArgument, RegexpBatch, RegexpRow};

struct PatternPrograms<'p> {
    host: Arc<dyn AggregateStateAllocator>,
    memo: ControlledPatternMemo<'p, Program<'p>, &'static str, &'static str>,
}

impl<'p> PatternPrograms<'p> {
    fn try_new(host: Arc<dyn AggregateStateAllocator>) -> Result<Self, KernelFailure> {
        Ok(Self {
            memo: ControlledPatternMemo::try_new(Arc::clone(&host))?,
            host,
        })
    }

    fn get(
        &mut self,
        pattern: &'p str,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Result<&Program<'p>, PatternRejection>, KernelFailure> {
        let host = &self.host;
        let outcome = self.memo.get_or_compile(pattern, work, |pattern, work| {
            Program::compile(pattern, Arc::clone(host), work).map(|outcome| {
                outcome.map_err(|rejection| match rejection {
                    PatternRejection::Data(message) => DeterministicPatternFailure::Data(message),
                    PatternRejection::Capability(message) => {
                        DeterministicPatternFailure::Capability(message)
                    }
                })
            })
        })?;
        Ok(outcome.map_err(|rejection| match rejection {
            DeterministicPatternFailure::Data(message) => PatternRejection::Data(message),
            DeterministicPatternFailure::Capability(message) => {
                PatternRejection::Capability(message)
            }
        }))
    }
}

type Slots = [usize];
const NO_CAPTURE: usize = usize::MAX;

fn reserve<T>(
    capacity: usize,
    allocator: &HostAggregateAllocator,
) -> Result<HostVec<T, HostAggregateAllocator>, KernelFailure> {
    let mut values = HostVec::new_in(allocator.clone());
    values
        .try_reserve_exact(capacity)
        .map_err(|_| allocator.take_failure())?;
    Ok(values)
}

fn init<T: Copy>(
    len: usize,
    value: T,
    allocator: &HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<HostVec<T, HostAggregateAllocator>, KernelFailure> {
    let mut values = reserve(len, allocator)?;
    for _ in 0..len {
        work.step()?;
        values.push(value);
    }
    Ok(values)
}

fn copy_slots(
    from: &Slots,
    to: &mut Slots,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    for (slot, offset) in to.iter_mut().zip(from) {
        work.step()?;
        *slot = *offset;
    }
    Ok(())
}

/// Admit both buffers before growth and observe the copy while they coexist.
fn push_observed<T: Copy>(
    values: &mut HostVec<T, HostAggregateAllocator>,
    value: T,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    work.step()?;
    if values.len() == values.capacity() {
        let capacity = values
            .capacity()
            .checked_mul(2)
            .ok_or(KernelFailure::ResourceExhausted)?
            .max(4);
        let mut next = reserve(capacity, values.allocator())?;
        for stored in values.iter().copied() {
            work.step()?;
            next.push(stored);
        }
        *values = next;
    }
    values.push(value);
    Ok(())
}
