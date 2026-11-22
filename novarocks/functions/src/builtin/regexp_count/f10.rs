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

//! Ordered search over the upstream Thompson transition graph.

use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_control::invalid;
use crate::kernel_input::EvaluationCheckpoints;
use crate::{AggregateStateAllocator, KernelFailure};
use allocator_api2::vec::Vec as HostVec;
use regex_automata::nfa::thompson::{NFA, State};
use regex_automata::util::primitives::{PatternID, StateID};
use std::{ops::Range, sync::Arc};

mod rows;
#[cfg(test)]
mod tests;

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

struct ActiveStates {
    order: HostVec<StateID, HostAggregateAllocator>,
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

    fn insert(&mut self, id: StateID) -> bool {
        if std::mem::replace(&mut self.seen[id.as_usize()], true) {
            return false;
        }
        self.order.push(id);
        true
    }

    fn clear(&mut self, work: &mut EvaluationCheckpoints<'_>) -> Result<(), KernelFailure> {
        while let Some(id) = self.order.pop() {
            work.step()?;
            self.seen[id.as_usize()] = false;
        }
        Ok(())
    }
}

enum Frame {
    Follow(StateID),
    Alternate { union: StateID, index: usize },
    Restore { slot: usize, previous: usize },
}

pub(crate) struct RegexpSearch<'p> {
    nfa: &'p NFA,
    active: ActiveStates,
    next: ActiveStates,
    stack: HostVec<Frame, HostAggregateAllocator>,
    seed: HostVec<usize, HostAggregateAllocator>,
    matched: HostVec<usize, HostAggregateAllocator>,
}

impl<'p> RegexpSearch<'p> {
    pub(crate) fn try_new(
        nfa: &'p NFA,
        host: Arc<dyn AggregateStateAllocator>,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        let states = nfa.states().len();
        let slot_len = nfa.group_info().slot_len();
        if nfa.pattern_len() != 1 || slot_len < 2 || !nfa.is_utf8() || nfa.is_reverse() {
            return Err(invalid(
                "regexp search requires one forward UTF-8 pattern with captures",
            ));
        }
        let allocator = HostAggregateAllocator::try_new_with_journal(host)?;
        Ok(Self {
            nfa,
            active: ActiveStates::try_new(states, slot_len, &allocator, work)?,
            next: ActiveStates::try_new(states, slot_len, &allocator, work)?,
            // A capture or union schedules at most one deferred action per
            // visited state. A resumed union reuses its previous stack slot.
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

    pub(crate) fn find(
        &mut self,
        source: &str,
        start: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<RegexpMatch<'_>>, KernelFailure> {
        if !source.is_char_boundary(start) {
            return Err(invalid(
                "regexp search start must be a source character boundary",
            ));
        }
        self.active.clear(work)?;
        self.next.clear(work)?;
        let mut matched = false;

        // Earlier starts and preferred branches enter the active list first.
        // After a match, only higher-priority threads may extend or replace
        // it; no new start or lower-priority branch can win.
        for offset in start..=source.len() {
            work.step()?;
            if !matched && source.is_char_boundary(offset) {
                follow(
                    self.nfa,
                    &mut self.active,
                    &mut self.stack,
                    &mut self.seed,
                    source.as_bytes(),
                    offset,
                    self.nfa.start_anchored(),
                    work,
                )?;
            }
            let byte = source.as_bytes().get(offset).copied();
            let ActiveStates {
                order,
                captures,
                slot_len,
                ..
            } = &mut self.active;
            for &id in &**order {
                work.step()?;
                let slot = id.as_usize() * *slot_len;
                let slots = &mut captures[slot..slot + *slot_len];
                let state = self.nfa.state(id);
                if matches!(state, State::Match { .. }) {
                    copy_slots(slots, &mut self.matched, work)?;
                    matched = true;
                    break;
                }
                if let Some(next) = byte
                    .map(|byte| transition(state, byte, work))
                    .transpose()?
                    .flatten()
                {
                    follow(
                        self.nfa,
                        &mut self.next,
                        &mut self.stack,
                        slots,
                        source.as_bytes(),
                        offset + 1,
                        next,
                        work,
                    )?;
                }
            }
            self.active.clear(work)?;
            std::mem::swap(&mut self.active, &mut self.next);
            if matched && self.active.order.is_empty() {
                break;
            }
        }
        if !matched {
            return Ok(None);
        }
        let found = RegexpMatch {
            nfa: self.nfa,
            slots: &self.matched,
        };
        Ok(Some(found))
    }
}

fn transition(
    state: &State,
    byte: u8,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<StateID>, KernelFailure> {
    Ok(match state {
        State::ByteRange { trans } => trans.matches_byte(byte).then_some(trans.next),
        State::Dense(dense) => dense.matches_byte(byte),
        State::Sparse(sparse) => {
            for range in &sparse.transitions {
                work.step()?;
                if byte < range.start {
                    break;
                }
                if range.matches_byte(byte) {
                    return Ok(Some(range.next));
                }
            }
            None
        }
        _ => None,
    })
}

fn follow(
    nfa: &NFA,
    states: &mut ActiveStates,
    stack: &mut HostVec<Frame, HostAggregateAllocator>,
    slots: &mut Slots,
    source: &[u8],
    offset: usize,
    start: StateID,
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
            Frame::Alternate { union, index } => {
                let State::Union { alternates } = nfa.state(union) else {
                    unreachable!("only unions can schedule another alternative");
                };
                if index + 1 < alternates.len() {
                    stack.push(Frame::Alternate {
                        union,
                        index: index + 1,
                    });
                }
                alternates[index]
            }
        };
        loop {
            work.step()?;
            if !states.insert(id) {
                break;
            }
            id = match nfa.state(id) {
                State::Look { look, next } if nfa.look_matcher().matches(*look, source, offset) => {
                    *next
                }
                State::Union { alternates } => {
                    let Some(&first) = alternates.first() else {
                        break;
                    };
                    if alternates.len() > 1 {
                        stack.push(Frame::Alternate {
                            union: id,
                            index: 1,
                        });
                    }
                    first
                }
                State::BinaryUnion { alt1, alt2 } => {
                    stack.push(Frame::Follow(*alt2));
                    *alt1
                }
                State::Capture { next, slot, .. } => {
                    let slot = slot.as_usize();
                    let previous = std::mem::replace(&mut slots[slot], offset);
                    stack.push(Frame::Restore { slot, previous });
                    *next
                }
                State::Fail | State::Look { .. } => break,
                State::ByteRange { .. }
                | State::Dense(_)
                | State::Sparse(_)
                | State::Match { .. } => {
                    let start = id.as_usize() * states.slot_len;
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

pub(crate) struct RegexpMatch<'s> {
    nfa: &'s NFA,
    slots: &'s Slots,
}

impl RegexpMatch<'_> {
    pub(crate) fn capture(&self, group: usize) -> Option<Range<usize>> {
        let slot = self.nfa.group_info().slot(PatternID::ZERO, group)?;
        let start = self.slots[slot];
        let end = self.slots[slot + 1];
        (start != NO_CAPTURE && end != NO_CAPTURE).then_some(start..end)
    }

    pub(crate) fn span(&self) -> Range<usize> {
        self.capture(0).expect("matched full capture")
    }

    pub(crate) fn next_start(&self, source: &str) -> Option<usize> {
        let span = self.span();
        if !span.is_empty() {
            return Some(span.end);
        }
        source[span.end..]
            .chars()
            .next()
            .map(|c| span.end + c.len_utf8())
    }
}
