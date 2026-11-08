// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Bounded classified inputs preserve FIFO for every source/receiver pair.

use crate::identity::{ActorId, InputId};
use crate::scheduler::ReadyQueue;
use std::collections::{BTreeMap, VecDeque};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(usize)]
pub enum InputClass {
    Lifecycle = 0,
    Control = 1,
    Ordinary = 2,
    Timer = 3,
}
impl InputClass {
    pub const ALL: [Self; 4] = [Self::Lifecycle, Self::Control, Self::Ordinary, Self::Timer];
    pub const fn index(self) -> usize {
        self as usize
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InputEnvelope {
    pub source: ActorId,
    pub input: InputId,
    pub class: InputClass,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MailboxError {
    Full(InputClass),
    DuplicateInput,
}
#[derive(Debug)]
pub struct MailboxState {
    limits: [usize; 4],
    counts: [usize; 4],
    sources: BTreeMap<ActorId, VecDeque<InputEnvelope>>,
    // Only each nonempty source's current head has a registered row. The
    // intrusive queue permits exact selection without scanning siblings or
    // retaining an index after that head leaves the mailbox.
    heads: [ReadyQueue; 4],
    inputs: BTreeMap<InputId, ActorId>,
}
impl MailboxState {
    pub fn new(limits: [usize; 4]) -> Self {
        Self {
            limits,
            counts: [0; 4],
            sources: BTreeMap::new(),
            heads: std::array::from_fn(|_| ReadyQueue::new()),
            inputs: BTreeMap::new(),
        }
    }
    pub fn len(&self) -> usize {
        self.inputs.len()
    }
    pub fn is_empty(&self) -> bool {
        self.inputs.is_empty()
    }
    pub fn count(&self, class: InputClass) -> usize {
        self.counts[class.index()]
    }
    pub fn available(&self, class: InputClass) -> usize {
        self.limits[class.index()] - self.count(class)
    }
    /// The current FIFO tail of this one source, across all input classes.
    /// This is a read-only identity sample, not an ordering sequence, a grant,
    /// or proof that an empty source/producer has exited.
    pub fn tail_from(&self, source: ActorId) -> Option<InputId> {
        self.sources.get(&source)?.back().map(|input| input.input)
    }
    pub fn has_ready(&self, class: InputClass) -> bool {
        self.heads[class.index()].ready_count() != 0
    }
    /// Only this source's current head may be selected. A later input of the
    /// requested class remains blocked behind an earlier different class.
    pub fn has_ready_from(&self, class: InputClass, source: ActorId) -> bool {
        self.heads[class.index()].is_ready(source)
    }
    pub fn push(
        &mut self,
        class: InputClass,
        source: ActorId,
        input: InputId,
    ) -> Result<(), MailboxError> {
        if self.inputs.contains_key(&input) {
            return Err(MailboxError::DuplicateInput);
        }
        if self.available(class) == 0 {
            return Err(MailboxError::Full(class));
        }
        if !self.sources.contains_key(&source) {
            self.enqueue_head(class, source);
        }
        let queue = self.sources.entry(source).or_default();
        queue.push_back(InputEnvelope {
            source,
            input,
            class,
        });
        self.inputs.insert(input, source);
        self.counts[class.index()] += 1;
        Ok(())
    }
    /// A source's later control message cannot overtake its earlier ordinary message.
    pub fn pop(&mut self, class: InputClass) -> Option<InputEnvelope> {
        let source = self.heads[class.index()].claim()?;
        Some(self.pop_claimed(class, source))
    }
    /// Remove only this source's current head, preserving the relative order
    /// of all other sources. Wrong-class and unknown-source requests do not
    /// consume or reorder any head, input or capacity.
    pub fn pop_from(&mut self, class: InputClass, source: ActorId) -> Option<InputEnvelope> {
        let source = self.heads[class.index()].claim_exact(source)?;
        Some(self.pop_claimed(class, source))
    }
    fn enqueue_head(&mut self, class: InputClass, source: ActorId) {
        let heads = &mut self.heads[class.index()];
        heads.register(source).expect("one current head per source");
        heads
            .notify(source)
            .expect("registered current source head");
    }
    fn pop_claimed(&mut self, class: InputClass, source: ActorId) -> InputEnvelope {
        self.heads[class.index()]
            .unregister_claimed(source)
            .expect("the selected head owns its sole scheduling row");
        let queue = self.sources.get_mut(&source).expect("head source exists");
        let item = queue.pop_front().expect("head source has an input");
        assert_eq!(item.class, class, "one ready head per source");
        if let Some(next) = queue.front() {
            let next_class = next.class;
            self.enqueue_head(next_class, source);
        } else {
            self.sources.remove(&source);
        }
        self.inputs.remove(&item.input);
        self.counts[class.index()] -= 1;
        item
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_live_indexes(mailbox: &MailboxState) {
        assert_eq!(
            mailbox
                .heads
                .iter()
                .map(ReadyQueue::actor_count)
                .sum::<usize>(),
            mailbox.sources.len(),
            "only actual current source heads retain scheduling rows"
        );
        assert!(mailbox.sources.len() <= mailbox.len());
        for class in InputClass::ALL {
            let expected = mailbox
                .sources
                .values()
                .filter(|queue| queue.front().unwrap().class == class)
                .count();
            let heads = &mailbox.heads[class.index()];
            assert_eq!(heads.ready_count(), expected);
            assert_eq!(heads.actor_count(), expected);
        }
        for (&source, queue) in &mailbox.sources {
            assert!(mailbox.has_ready_from(queue.front().unwrap().class, source));
        }
    }

    #[test]
    fn selective_source_rotation_reclaims_every_departed_head_index() {
        let mut mailbox = MailboxState::new([3; 4]);
        for cycle in 0..4096_u64 {
            let first = cycle * 3 + 1;
            let sources = [first, first + 1, first + 2].map(|id| ActorId::new(id).unwrap());
            for (offset, source) in sources.iter().copied().enumerate() {
                let class = InputClass::ALL[(cycle as usize + offset) % 4];
                mailbox
                    .push(class, source, InputId::new(first + offset as u64).unwrap())
                    .unwrap();
                assert_live_indexes(&mailbox);
            }
            for offset in [1, 0, 2] {
                let class = InputClass::ALL[(cycle as usize + offset) % 4];
                assert!(mailbox.pop_from(class, sources[offset]).is_some());
                assert_live_indexes(&mailbox);
            }
            assert!(mailbox.is_empty());
            assert!(mailbox.sources.is_empty());
            assert!(mailbox.heads.iter().all(|heads| heads.actor_count() == 0));
        }
    }
}
