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

use crate::opaque_memory::OpaqueAllocationHost;
use crate::{AggregateStateAllocator, KernelDiagnostic, KernelEvaluationControl, KernelFailure};
use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
pub(super) struct Ledger {
    attempts: usize,
    blocks: Vec<(usize, Layout)>,
    opaque: usize,
    opaque_attempts: usize,
}
pub(super) struct Host {
    ledger: Mutex<Ledger>,
    actual_limit: usize,
    pub(super) refusal: Mutex<Option<(usize, KernelFailure)>>,
    pub(super) opaque_refusal: Mutex<Option<(usize, KernelFailure)>>,
}
impl Host {
    pub(super) fn with_limit(actual_limit: usize) -> Arc<Self> {
        Arc::new(Self {
            ledger: Mutex::new(Ledger::default()),
            actual_limit,
            refusal: Mutex::new(None),
            opaque_refusal: Mutex::new(None),
        })
    }
    pub(super) fn attempts(&self) -> usize {
        self.ledger.lock().unwrap().attempts
    }
    pub(super) fn released(&self) {
        let l = self.ledger.lock().unwrap();
        assert!(l.blocks.is_empty());
        assert_eq!(l.opaque, 0);
    }
}
impl AggregateStateAllocator for Host {
    fn opaque_allocation_host(&self) -> Option<&dyn OpaqueAllocationHost> {
        Some(self)
    }
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        assert_ne!(layout.size(), 0);
        let mut l = self.ledger.lock().unwrap();
        let at = l.attempts;
        l.attempts += 1;
        if let Some((stop, cause)) = &*self.refusal.lock().unwrap() {
            assert!(at <= *stop, "host request after first refusal");
            if at == *stop {
                return Err(cause.clone());
            }
        }
        if l.opaque + l.blocks.iter().map(|(_, v)| v.size()).sum::<usize>() + layout.size()
            > self.actual_limit
        {
            return Err(KernelFailure::ResourceExhausted);
        }
        let ptr = NonNull::new(unsafe { std::alloc::alloc(layout) })
            .ok_or(KernelFailure::ResourceExhausted)?;
        l.blocks.push((ptr.as_ptr().addr(), layout));
        Ok(ptr)
    }
    unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
        let mut l = self.ledger.lock().unwrap();
        let at = l
            .blocks
            .iter()
            .position(|(p, t)| *p == pointer.as_ptr().addr() && *t == layout)
            .expect("exact live host block");
        l.blocks.swap_remove(at);
        unsafe { std::alloc::dealloc(pointer.as_ptr(), layout) };
    }
}
impl OpaqueAllocationHost for Host {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        let mut l = self.ledger.lock().unwrap();
        let at = l.opaque_attempts;
        l.opaque_attempts += 1;
        if let Some((stop, cause)) = &*self.opaque_refusal.lock().unwrap() {
            assert!(at <= *stop, "host request after first refusal");
            if at == *stop {
                return Err(cause.clone());
            }
        }
        if l.opaque + l.blocks.iter().map(|(_, v)| v.size()).sum::<usize>() + bytes
            > self.actual_limit
        {
            return Err(KernelFailure::ResourceExhausted);
        }
        l.opaque += bytes;
        Ok(())
    }
    fn release_opaque(&self, bytes: usize) {
        let mut l = self.ledger.lock().unwrap();
        assert!(bytes <= l.opaque);
        l.opaque -= bytes;
    }
}
#[derive(Default)]
pub(super) struct Control {
    pub(super) trace: Mutex<Vec<u32>>,
    pub(super) refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut t = self.trace.lock().unwrap();
        let at = t.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after first cause");
        }
        t.push(units);
        match &self.refusal {
            Some((stop, cause)) if at == *stop => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("array invocation never waits")
    }
}
pub(super) fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("original host cause")),
        KernelFailure::Internal(KernelDiagnostic::new("original host cause")),
        KernelFailure::Operational(KernelDiagnostic::new("original host cause")),
        KernelFailure::InstanceFailed,
    ]
}

impl Host {
    pub(super) fn arm_allocation(&self, offset: usize, cause: KernelFailure) {
        let at = self.ledger.lock().unwrap().attempts;
        *self.refusal.lock().unwrap() = Some((at + offset, cause));
    }
    pub(super) fn arm_opaque(&self, offset: usize, cause: KernelFailure) {
        let at = self.ledger.lock().unwrap().opaque_attempts;
        *self.opaque_refusal.lock().unwrap() = Some((at + offset, cause));
    }
    pub(super) fn opaque_attempts(&self) -> usize {
        self.ledger.lock().unwrap().opaque_attempts
    }
}

impl Host {
    pub(super) fn live_blocks(&self) -> usize {
        self.ledger.lock().unwrap().blocks.len()
    }
}
