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

//! A readiness hint only. Usage, limits, domains and real releases remain in
//! RuntimeLedger. WC reobserve/native wakes cannot manufacture this flag.
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Wake, Waker},
};
pub(crate) struct LedgerReleaseWake {
    released: AtomicBool,
    parent: Mutex<Option<Waker>>,
}
impl LedgerReleaseWake {
    /// The constructor below allocates ArcInner, not merely an Arc handle.
    /// Reuse the locked Rust allocation-layout author for the same payload.
    pub(crate) fn allocation_layout() -> Result<
        std::alloc::Layout,
        novarocks_type_contract::owned_resources::layout::LayoutResourceError,
    > {
        novarocks_type_contract::owned_resources::layout::arc_layout(
            std::alloc::Layout::new::<Self>(),
        )
    }

    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            released: AtomicBool::new(false),
            parent: Mutex::new(None),
        })
    }
    pub(crate) fn refresh(&self, parent: &Waker) {
        let previous = {
            let mut slot = self
                .parent
                .lock()
                .expect("original capacity wait task waker");
            slot.replace(parent.clone())
        };
        drop(previous);
    }
    pub(crate) fn arm(&self, parent: &Waker) {
        self.refresh(parent);
        self.released.store(false, Ordering::Release);
    }
    pub(crate) fn take_release(&self) -> bool {
        self.released.swap(false, Ordering::AcqRel)
    }
}
impl Wake for LedgerReleaseWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.released.store(true, Ordering::Release);
        let parent = self
            .parent
            .lock()
            .expect("original capacity wait task waker")
            .clone();
        if let Some(parent) = parent {
            parent.wake();
        }
    }
}
