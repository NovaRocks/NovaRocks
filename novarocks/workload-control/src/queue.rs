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

use crate::WorkId;
use std::collections::{BTreeMap, VecDeque};

#[derive(Default)]
pub(crate) struct FairQueue {
    roots: VecDeque<WorkId>,
    requests: BTreeMap<WorkId, VecDeque<u64>>,
}

impl FairQueue {
    pub(crate) fn is_empty(&self) -> bool {
        self.roots.is_empty()
    }

    pub(crate) fn push(&mut self, root: WorkId, request: u64) {
        let requests = self.requests.entry(root).or_default();
        if requests.is_empty() {
            self.roots.push_back(root);
        }
        requests.push_back(request);
    }

    pub(crate) fn remove(&mut self, root: WorkId, request: u64) {
        if let Some(requests) = self.requests.get_mut(&root) {
            requests.retain(|id| *id != request);
            if requests.is_empty() {
                self.requests.remove(&root);
                self.roots.retain(|id| *id != root);
            }
        }
    }

    pub(crate) fn pop_runnable(&mut self, mut ready: impl FnMut(WorkId) -> bool) -> Option<u64> {
        for _ in 0..self.roots.len() {
            let root = self.roots.pop_front()?;
            if !ready(root) {
                self.roots.push_back(root);
                continue;
            }
            let requests = self.requests.get_mut(&root).unwrap();
            let request = requests.pop_front().unwrap();
            if requests.is_empty() {
                self.requests.remove(&root);
            } else {
                self.roots.push_back(root);
            }
            return Some(request);
        }
        None
    }
}
