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

use super::{OriginalQueuedInput, SendKey};
use novarocks_attempt_protocol::input::QueuedInputGrantRef;
use std::{collections::BTreeMap, num::NonZeroU64};

/// Original Source custody of admitted WC inputs. Each row is backed by
/// that live admission and retains its exact operation and send generation.
#[derive(Default)]
pub(crate) struct GrantedInputs {
    originals: BTreeMap<NonZeroU64, OriginalQueuedInput>,
}
impl GrantedInputs {
    pub(crate) fn contains(&self, grant: QueuedInputGrantRef) -> bool {
        self.originals.contains_key(&grant.record())
    }

    pub(crate) fn install(&mut self, grant: QueuedInputGrantRef, input: OriginalQueuedInput) {
        assert_eq!(input.key().generation, grant.generation());
        let std::collections::btree_map::Entry::Vacant(position) =
            self.originals.entry(grant.record())
        else {
            unreachable!("original Source prechecked the grant position");
        };
        position.insert(input);
    }

    pub(crate) fn take(
        &mut self,
        grant: QueuedInputGrantRef,
        expected: SendKey,
    ) -> Option<OriginalQueuedInput> {
        if expected.generation != grant.generation()
            || self.originals.get(&grant.record())?.key() != expected
        {
            return None;
        }
        self.originals.remove(&grant.record())
    }
}
