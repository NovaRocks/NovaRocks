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

#[cfg(not(loom))]
#[test]
fn attribution_reconcile() {
    use novarocks_memory::attribution::{AttributingAllocator, binding};
    use novarocks_memory::lane::{RecordStorePin, ResponsibilityClass, global_store};
    use std::alloc::{GlobalAlloc, Layout, System};
    let a = std::sync::Arc::new(AttributingAllocator::new(System));
    let o = RecordStorePin::global()
        .acquire(
            7,
            ResponsibilityClass::Query,
            novarocks_memory::lane::RecordRegion::Observation,
        )
        .unwrap();
    let r = o.reference();
    let l = Layout::from_size_align(512, 8).unwrap();
    let previous = unsafe { binding::install_ambient(r) };
    let p = unsafe { a.alloc(l) };
    assert!(!p.is_null());
    unsafe { binding::restore_ambient(previous) };
    let q = unsafe { a.alloc(l) };
    assert!(!q.is_null());
    let small = Layout::from_size_align(64, 8).unwrap();
    let previous = unsafe { binding::install_ambient(r) };
    let before = global_store().snapshot_ref(r).unwrap();
    let tiny = unsafe { a.alloc(small) };
    assert!(!tiny.is_null());
    unsafe { binding::restore_ambient(previous) };
    assert_eq!(global_store().snapshot_ref(r).unwrap(), before);
    struct SmallBlock(*mut u8);
    unsafe impl Send for SmallBlock {}
    impl SmallBlock {
        unsafe fn release(self, a: &AttributingAllocator<System>, l: Layout) {
            unsafe { a.dealloc(self.0, l) };
        }
    }
    let b = a.clone();
    let tiny = SmallBlock(tiny);
    std::thread::spawn(move || unsafe { tiny.release(&b, small) })
        .join()
        .unwrap();
    assert_eq!(global_store().snapshot_ref(r).unwrap(), before);
    let recorded: i64 = global_store()
        .records()
        .map(|(reference, _)| global_store().snapshot_ref(reference).unwrap().tagged_bytes)
        .sum();
    assert_eq!(recorded, a.band_snapshot().tagged.live_bytes as i64);
    assert_eq!(a.band_snapshot().small.live_bytes, 0);
    unsafe {
        a.dealloc(p, l);
        a.dealloc(q, l)
    };
    assert_eq!(a.snapshot().live_bytes, 0);
    assert_eq!(a.band_snapshot().small_deallocations, 1);
    assert_eq!(a.band_snapshot().tagged_deallocations, 2);
    assert_eq!(a.band_snapshot().total_deallocations, 3);
    assert_eq!(global_store().snapshot_ref(r).unwrap().tagged_bytes, 0);
    println!("attribution_reconcile: PASS (flushed tagged and cross-thread small facts)");
}
