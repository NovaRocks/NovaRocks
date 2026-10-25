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
    heads: [VecDeque<ActorId>; 4],
    inputs: BTreeMap<InputId, ActorId>,
}
impl MailboxState {
    pub fn new(limits: [usize; 4]) -> Self {
        Self {
            limits,
            counts: [0; 4],
            sources: BTreeMap::new(),
            heads: std::array::from_fn(|_| VecDeque::new()),
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
    pub fn has_ready(&self, class: InputClass) -> bool {
        !self.heads[class.index()].is_empty()
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
        let queue = self.sources.entry(source).or_default();
        if queue.is_empty() {
            self.heads[class.index()].push_back(source);
        }
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
        let source = self.heads[class.index()].pop_front()?;
        let queue = self.sources.get_mut(&source).expect("head source exists");
        let item = queue.pop_front().expect("head source has an input");
        assert_eq!(item.class, class, "one ready head per source");
        if let Some(next) = queue.front() {
            self.heads[next.class.index()].push_back(source);
        } else {
            self.sources.remove(&source);
        }
        self.inputs.remove(&item.input);
        self.counts[class.index()] -= 1;
        Some(item)
    }
}
