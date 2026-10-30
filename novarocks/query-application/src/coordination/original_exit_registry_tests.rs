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

use super::*;
use std::sync::atomic::AtomicUsize;

struct OpaqueOriginalCause(Arc<AtomicUsize>);
impl Drop for OpaqueOriginalCause {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn cancelled_original_join_wait_keeps_one_real_handle_and_original_opaque_payload() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let (release, released) = tokio::sync::oneshot::channel();
    let original = OpaqueOriginalCause(Arc::clone(&dropped));
    let handle = tokio::spawn(async move {
        released.await.unwrap();
        std::panic::panic_any(original);
    });
    let same = OriginalActorJoin::new(handle, std::sync::Weak::new());
    let mut cancelled = Box::pin(std::future::poll_fn(|cx| same.poll(cx)));
    assert!(futures::poll!(&mut cancelled).is_pending());
    drop(cancelled);
    release.send(()).unwrap();
    let (result, completed_handle) = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        std::future::poll_fn(|cx| same.poll(cx)),
    )
    .await
    .unwrap();
    let error = Arc::new(result.expect_err("actual original task panicked"));
    // The original opaque payload remains in its genuine JoinError. The
    // completed handle is destroyed only after that exact result is stable.
    drop(completed_handle);
    assert!(error.is_panic());
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    let retained = Arc::clone(&error);
    drop(error);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(retained);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}
