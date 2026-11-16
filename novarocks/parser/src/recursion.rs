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

//! Stack growth around polling and destruction of syntax traversal futures.
//! No runtime, execution authority, ownership substitution or global tuning.

#![forbid(unsafe_code)]

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// The pinned allocation retains F's original value and borrowing lifetime.
/// Moving this container moves only Pin<Box<F>>, never the pinned F allocation.
pub struct GuardedFuture<F: Future + ?Sized> {
    inner: Option<Pin<Box<F>>>,
}

impl<F: Future> GuardedFuture<F> {
    // No 'static or Send bound: caller-owned borrowed/local futures stay valid.
    pub fn new(future: F) -> Self {
        Self::from_pin(Box::pin(future))
    }
}

impl<F: Future + ?Sized> GuardedFuture<F> {
    /// Also accepts an already pinned allocation without moving its payload.
    pub fn from_pin(future: Pin<Box<F>>) -> Self {
        Self {
            inner: Some(future),
        }
    }
}

impl<F: Future + ?Sized> Future for GuardedFuture<F> {
    type Output = F::Output;

    #[recursive::recursive]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // GuardedFuture is Unpin even for !Unpin F: its structural field is a
        // movable pinned pointer, and no mutable unpinned reference to F escapes.
        let this = self.get_mut();
        this.inner
            .as_mut()
            .expect("live guarded future payload")
            .as_mut()
            .poll(cx)
    }
}

impl<F: Future + ?Sized> Drop for GuardedFuture<F> {
    #[recursive::recursive]
    fn drop(&mut self) {
        // Transfer and actual payload destruction both happen inside maybe_grow.
        // On return or unwinding, automatic field destruction sees only None.
        drop(self.inner.take());
    }
}

#[cfg(test)]
#[path = "recursion_tests.rs"]
mod tests;
