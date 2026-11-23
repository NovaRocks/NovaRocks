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

//! Ordered search over the admitted F10 program.

use super::program::{Program, State};
use super::{NO_CAPTURE, Slots, copy_slots, init, reserve};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_control::invalid;
use crate::kernel_input::EvaluationCheckpoints;
use crate::{AggregateStateAllocator, KernelFailure};
use allocator_api2::vec::Vec as HostVec;
use std::{ops::Range, sync::Arc};

struct ActiveStates {
    order: HostVec<usize, HostAggregateAllocator>,
    seen: HostVec<bool, HostAggregateAllocator>,
    captures: HostVec<usize, HostAggregateAllocator>,
    slot_len: usize,
}

impl ActiveStates {
    fn try_new(
        states: usize,
        slot_len: usize,
        allocator: &HostAggregateAllocator,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        let captures = states
            .checked_mul(slot_len)
            .ok_or(KernelFailure::ResourceExhausted)?;
        Ok(Self {
            order: reserve(states, allocator)?,
            seen: init(states, false, allocator, work)?,
            captures: init(captures, NO_CAPTURE, allocator, work)?,
            slot_len,
        })
    }

    fn insert(&mut self, id: usize) -> bool {
        if std::mem::replace(&mut self.seen[id], true) {
            return false;
        }
        self.order.push(id);
        true
    }

    fn clear(&mut self, work: &mut EvaluationCheckpoints<'_>) -> Result<(), KernelFailure> {
        while let Some(&id) = self.order.last() {
            work.step()?;
            self.order.pop();
            self.seen[id] = false;
        }
        Ok(())
    }
}

enum Frame {
    Follow(usize),
    Restore { slot: usize, previous: usize },
}

struct SourcePosition {
    offset: usize,
    len: usize,
}

pub(crate) struct RegexpSearch<'g, 'p> {
    pub(super) program: &'g Program<'p>,
    active: ActiveStates,
    next: ActiveStates,
    stack: HostVec<Frame, HostAggregateAllocator>,
    seed: HostVec<usize, HostAggregateAllocator>,
    matched: HostVec<usize, HostAggregateAllocator>,
}

impl<'g, 'p> RegexpSearch<'g, 'p> {
    pub(super) fn try_new(
        program: &'g Program<'p>,
        host: Arc<dyn AggregateStateAllocator>,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        let states = program.states.len();
        let slot_len = program
            .names
            .len()
            .checked_mul(2)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let allocator = HostAggregateAllocator::try_new_with_journal(host)?;
        Ok(Self {
            program,
            active: ActiveStates::try_new(states, slot_len, &allocator, work)?,
            next: ActiveStates::try_new(states, slot_len, &allocator, work)?,
            // Each visited split or save can schedule one frame. The active
            // set visits a state at most once at a given source boundary.
            stack: reserve(
                states
                    .checked_add(1)
                    .ok_or(KernelFailure::ResourceExhausted)?,
                &allocator,
            )?,
            seed: init(slot_len, NO_CAPTURE, &allocator, work)?,
            matched: init(slot_len, NO_CAPTURE, &allocator, work)?,
        })
    }

    pub(super) fn find(
        &mut self,
        source: &str,
        start: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<RegexpMatch<'_, 'p>>, KernelFailure> {
        let mut boundary = start;
        let found = match self.find_ordered(source, start, &mut boundary, work) {
            Ok(found) => found,
            Err(cause) => {
                work.regexp_search_stopped(boundary);
                return Err(cause);
            }
        };
        Ok(found.then_some(RegexpMatch {
            names: &self.program.names,
            slots: &self.matched,
        }))
    }

    fn find_ordered(
        &mut self,
        source: &str,
        start: usize,
        boundary: &mut usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<bool, KernelFailure> {
        let tail = source
            .get(start..)
            .ok_or_else(|| invalid("regexp search start must be a source character boundary"))?;
        self.active.clear(work)?;
        self.next.clear(work)?;
        self.stack.clear();
        for slot in &mut *self.seed {
            work.step()?;
            *slot = NO_CAPTURE;
        }

        let mut matched = false;
        let positions = tail
            .char_indices()
            .map(|(offset, ch)| (start + offset, Some(ch)))
            .chain(std::iter::once((source.len(), None)));

        // Earlier starts and preferred branches enter the active list first.
        // After a match, only higher-priority threads may extend or replace
        // it. No new start or lower-priority branch can win.
        let mut progress_reported = false;
        for (offset, ch) in positions {
            *boundary = offset;
            work.step()?;
            if !progress_reported && offset - start >= crate::MAX_UNOBSERVED_KERNEL_WORK as usize {
                work.regexp_search_progress(offset)?;
                progress_reported = true;
            }
            if !matched {
                follow(
                    self.program,
                    &mut self.active,
                    &mut self.stack,
                    &mut self.seed,
                    SourcePosition {
                        offset,
                        len: source.len(),
                    },
                    self.program.start,
                    work,
                )?;
            }
            let ActiveStates {
                order,
                captures,
                slot_len,
                ..
            } = &mut self.active;
            for &id in &**order {
                work.step()?;
                let slot = id * *slot_len;
                let slots = &mut captures[slot..slot + *slot_len];
                match self.program.states[id] {
                    State::Match => {
                        copy_slots(slots, &mut self.matched, work)?;
                        matched = true;
                        break;
                    }
                    State::Consume { matcher, next } => {
                        if let Some(ch) = ch
                            && matcher.matches(self.program, ch, work)?
                        {
                            follow(
                                self.program,
                                &mut self.next,
                                &mut self.stack,
                                slots,
                                SourcePosition {
                                    offset: offset + ch.len_utf8(),
                                    len: source.len(),
                                },
                                next,
                                work,
                            )?;
                        }
                    }
                    _ => {}
                }
            }
            self.active.clear(work)?;
            std::mem::swap(&mut self.active, &mut self.next);
            if matched && self.active.order.is_empty() {
                break;
            }
        }
        Ok(matched)
    }
}

fn follow(
    program: &Program<'_>,
    states: &mut ActiveStates,
    stack: &mut HostVec<Frame, HostAggregateAllocator>,
    slots: &mut Slots,
    position: SourcePosition,
    start: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    stack.push(Frame::Follow(start));
    while let Some(frame) = stack.pop() {
        work.step()?;
        let mut id = match frame {
            Frame::Follow(id) => id,
            Frame::Restore { slot, previous } => {
                slots[slot] = previous;
                continue;
            }
        };
        loop {
            work.step()?;
            // Future transitions at this offset depend only on the state.
            // The admitted grammar has no capture-dependent branch or
            // repeat that can return to its entry without consuming input.
            if !states.insert(id) {
                break;
            }
            id = match program.states[id] {
                State::Jump(next) => next,
                State::Split { first, second } => {
                    stack.push(Frame::Follow(second));
                    first
                }
                State::Save { slot, next } => {
                    let previous = std::mem::replace(&mut slots[slot], position.offset);
                    stack.push(Frame::Restore { slot, previous });
                    next
                }
                State::Start(next) if position.offset == 0 => next,
                State::End(next) if position.offset == position.len => next,
                State::Start(_) | State::End(_) => break,
                State::Consume { .. } | State::Match => {
                    let start = id * states.slot_len;
                    copy_slots(
                        slots,
                        &mut states.captures[start..start + states.slot_len],
                        work,
                    )?;
                    break;
                }
            };
        }
    }
    Ok(())
}

pub(super) struct RegexpMatch<'s, 'p> {
    pub(super) names: &'s [Option<&'p str>],
    slots: &'s Slots,
}

impl RegexpMatch<'_, '_> {
    pub(super) fn capture(&self, group: usize) -> Option<Range<usize>> {
        let slot = group.checked_mul(2)?;
        let &[start, end] = self.slots.get(slot..)?.first_chunk::<2>()?;
        (start != NO_CAPTURE && end != NO_CAPTURE).then_some(start..end)
    }

    pub(super) fn span(&self) -> Range<usize> {
        self.capture(0).expect("matched full capture")
    }

    pub(super) fn next_start(&self, source: &str) -> Option<usize> {
        let span = self.span();
        if !span.is_empty() {
            return Some(span.end);
        }
        source[span.end..]
            .chars()
            .next()
            .map(|ch| span.end + ch.len_utf8())
    }
}
