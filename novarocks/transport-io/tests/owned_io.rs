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

//! A connection's admission guard leaves only after its transport IO has been
//! destroyed, including when the transport destructor unwinds.

use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use novarocks_transport_io::OwnedIo;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

struct TrackedIo {
    destroyed: Arc<AtomicBool>,
    panic_on_drop: bool,
}

impl Drop for TrackedIo {
    fn drop(&mut self) {
        self.destroyed.store(true, Ordering::SeqCst);
        if self.panic_on_drop {
            panic!("transport destructor unwinds");
        }
    }
}

impl AsyncRead for TrackedIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for TrackedIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

struct Guard {
    io_destroyed: Arc<AtomicBool>,
    released: Arc<AtomicBool>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        assert!(
            self.io_destroyed.load(Ordering::SeqCst),
            "the admission guard must not leave while its IO is live"
        );
        self.released.store(true, Ordering::SeqCst);
    }
}

fn owned(panic_on_drop: bool) -> (OwnedIo, Arc<AtomicBool>, Arc<AtomicBool>) {
    let destroyed = Arc::new(AtomicBool::new(false));
    let released = Arc::new(AtomicBool::new(false));
    let io = OwnedIo::with_guard(
        Box::new(TrackedIo {
            destroyed: Arc::clone(&destroyed),
            panic_on_drop,
        }),
        Guard {
            io_destroyed: Arc::clone(&destroyed),
            released: Arc::clone(&released),
        },
    );
    (io, destroyed, released)
}

#[test]
fn guard_leaves_after_the_transport_is_destroyed() {
    let (io, destroyed, released) = owned(false);
    assert!(!destroyed.load(Ordering::SeqCst));
    drop(io);
    assert!(destroyed.load(Ordering::SeqCst));
    assert!(released.load(Ordering::SeqCst));
}

#[test]
fn guard_still_leaves_when_the_transport_destructor_unwinds() {
    let (io, destroyed, released) = owned(true);
    assert!(catch_unwind(AssertUnwindSafe(move || drop(io))).is_err());
    assert!(destroyed.load(Ordering::SeqCst));
    assert!(released.load(Ordering::SeqCst));
}

#[test]
fn unguarded_io_is_an_ordinary_transport() {
    let destroyed = Arc::new(AtomicBool::new(false));
    let io = OwnedIo::new(Box::new(TrackedIo {
        destroyed: Arc::clone(&destroyed),
        panic_on_drop: false,
    }));
    drop(io);
    assert!(destroyed.load(Ordering::SeqCst));
}
