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

//! Shared transport lifetime custody. A guard does not grant capacity; its
//! original owner decides admission and retains any independent join proof.

use std::{
    any::Any,
    fmt, io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Object-safe stream IO, independent of protocol, identity and admission.
pub trait Io: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Io for T {}
pub type BoxedIo = Box<dyn Io>;

/// Transport IO that keeps a caller-supplied guard alive until the transport
/// itself has been destroyed.
///
/// The guard drops after the boxed transport, even when the transport
/// destructor unwinds, so an admission position carried by the guard is never
/// returned while its connection's socket is still open. There is deliberately
/// no IO or guard extraction API.
pub struct OwnedIo {
    io: Option<BoxedIo>,
    guard: Option<Box<dyn Any + Send + Sync>>,
}

impl OwnedIo {
    pub fn new(io: BoxedIo) -> Self {
        Self {
            io: Some(io),
            guard: None,
        }
    }

    pub fn with_guard<G: Send + Sync + 'static>(io: BoxedIo, guard: G) -> Self {
        Self {
            io: Some(io),
            guard: Some(Box::new(guard)),
        }
    }

    fn io_mut(&mut self) -> &mut (dyn Io + 'static) {
        self.io.as_deref_mut().expect("owned transport IO is live")
    }
}

impl fmt::Debug for OwnedIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedIo")
            .field("guarded", &self.guard.is_some())
            .finish_non_exhaustive()
    }
}

impl Drop for OwnedIo {
    fn drop(&mut self) {
        // The local guard unwinds after drop(io), even if the boxed
        // destructor panics.
        let guard = self.guard.take();
        drop(self.io.take());
        drop(guard);
    }
}

impl AsyncRead for OwnedIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().io_mut()).poll_read(cx, buf)
    }
}

impl AsyncWrite for OwnedIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.get_mut().io_mut()).poll_write(cx, bytes)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.get_mut().io_mut()).poll_write_vectored(cx, buffers)
    }

    fn is_write_vectored(&self) -> bool {
        self.io
            .as_deref()
            .expect("owned transport IO is live")
            .is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().io_mut()).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().io_mut()).poll_shutdown(cx)
    }
}
