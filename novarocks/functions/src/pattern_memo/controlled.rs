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

//! Selection-local memo for a controlled pattern compiler.

use super::MAX_RETAINED_PATTERNS;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_input::EvaluationCheckpoints;
use crate::{AggregateStateAllocator, KernelFailure};
use allocator_api2::vec::Vec as HostVec;
use std::{
    hash::{DefaultHasher, Hasher},
    sync::Arc,
};

/// Only repeatable value and capability facts belong in the memo. Live
/// control, capacity and execution failures use the separate outer channel.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum DeterministicPatternFailure<D, C> {
    Data(D),
    Capability(C),
}

type PatternOutcome<P, D, C> = Result<P, DeterministicPatternFailure<D, C>>;
type BorrowedOutcome<'a, P, D, C> = Result<&'a P, &'a DeterministicPatternFailure<D, C>>;

struct Entry<'p, P, D, C> {
    source: &'p str,
    hash: u64,
    outcome: PatternOutcome<P, D, C>,
}

#[derive(Clone, Copy)]
enum Location {
    Resident(usize),
    Transient,
}

/// Borrows keys from the live argument values. Each compiled outcome owns
/// its original host backing while it is cached; compiling a replacement
/// cannot refund the incumbent's backing before the candidate is published.
pub(crate) struct ControlledPatternMemo<'p, P, D, C> {
    residents: HostVec<Entry<'p, P, D, C>, HostAggregateAllocator>,
    transient: Option<Entry<'p, P, D, C>>,
    last: Option<Location>,
}

impl<'p, P, D, C> ControlledPatternMemo<'p, P, D, C> {
    pub(crate) fn try_new(host: Arc<dyn AggregateStateAllocator>) -> Result<Self, KernelFailure> {
        Ok(Self {
            residents: HostVec::new_in(HostAggregateAllocator::try_new_with_journal(host)?),
            transient: None,
            last: None,
        })
    }

    pub(crate) fn get_or_compile(
        &mut self,
        pattern: &'p str,
        work: &mut EvaluationCheckpoints<'_>,
        compile: impl FnOnce(
            &'p str,
            &mut EvaluationCheckpoints<'_>,
        ) -> Result<PatternOutcome<P, D, C>, KernelFailure>,
    ) -> Result<BorrowedOutcome<'_, P, D, C>, KernelFailure> {
        work.step()?;
        if let Some(last) = self.last
            && same_text(self.entry(last).source, pattern, work)?
        {
            return Ok(self.entry(last).outcome.as_ref());
        }

        let hash = hash_pattern(pattern, work)?;
        let mut hit = None;
        for (index, entry) in self.residents.iter().enumerate() {
            work.step()?;
            if entry.hash == hash && same_text(entry.source, pattern, work)? {
                hit = Some(index);
                break;
            }
        }
        let location = match hit {
            Some(index) => Location::Resident(index),
            None => {
                work.flush()?;
                let outcome = compile(pattern, work)?;
                // The old transient is still retained here. An interruption
                // drops the candidate and leaves the published memo intact.
                work.flush()?;
                self.insert(
                    Entry {
                        source: pattern,
                        hash,
                        outcome,
                    },
                    work,
                )?
            }
        };
        self.last = Some(location);
        Ok(self.entry(location).outcome.as_ref())
    }

    fn insert(
        &mut self,
        entry: Entry<'p, P, D, C>,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Location, KernelFailure> {
        if self.residents.len() == MAX_RETAINED_PATTERNS {
            self.transient = Some(entry);
            return Ok(Location::Transient);
        }
        if self.residents.capacity() == 0 {
            // Allocate the fixed resident table on its first admission. No
            // resident payload moves, and transient replacement never grows it.
            self.residents
                .try_reserve_exact(MAX_RETAINED_PATTERNS)
                .map_err(|_| self.residents.allocator().take_failure())?;
            work.flush()?;
        }
        let index = self.residents.len();
        self.residents.push(entry);
        Ok(Location::Resident(index))
    }

    fn entry(&self, at: Location) -> &Entry<'p, P, D, C> {
        match at {
            Location::Resident(index) => &self.residents[index],
            Location::Transient => self
                .transient
                .as_ref()
                .expect("published transient pattern"),
        }
    }
}

fn hash_pattern(pattern: &str, work: &mut EvaluationCheckpoints<'_>) -> Result<u64, KernelFailure> {
    let mut hasher = DefaultHasher::new();
    for byte in pattern.bytes() {
        work.step()?;
        hasher.write_u8(byte);
    }
    Ok(hasher.finish())
}

fn same_text(
    left: &str,
    right: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<bool, KernelFailure> {
    // Reuse a borrowed constant's actual address and extent, and observe
    // every byte for a same-value column delivered through different buffers.
    if std::ptr::eq(left, right) {
        return Ok(true);
    }
    if left.len() != right.len() {
        return Ok(false);
    }
    for (left, right) in left.bytes().zip(right.bytes()) {
        work.step()?;
        if left != right {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests;
