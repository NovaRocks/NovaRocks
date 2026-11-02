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

//! Exact prepared-kernel drain hold. Polling never parks a Worker or driver.

use crate::test_controls::{NativeHoldClaim, NativeTestControls};
use novarocks_execution::runtime::fragment::ExecutionFailure;
use novarocks_execution_contract::TaskIdentity;
use std::{
    future::Future,
    io::{Read, Write},
    net::Shutdown,
    os::unix::net::UnixStream,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Instant,
};

pub(super) struct PreparedExitGate {
    claim: NativeHoldClaim,
    identity: TaskIdentity,
    cause: ExecutionFailure,
    stream: tokio::io::unix::AsyncFd<UnixStream>,
    runtime: tokio::runtime::Handle,
    readiness: Waker,
    cancellation: Pin<Box<dyn Future<Output = ()> + Send>>,
    marker: Vec<u8>,
    written: usize,
    announced: bool,
    open: bool,
}
impl PreparedExitGate {
    pub(super) fn connect(
        controls: &NativeTestControls,
        identity: TaskIdentity,
        runtime: &tokio::runtime::Handle,
        readiness: Waker,
    ) -> Result<Option<Self>, String> {
        let Some(claim) = controls.claim_prepared_failure_exit_hold(identity)? else {
            return Ok(None);
        };
        let cancellation = claim.cancellation();
        let path = novarocks_failpoint::native_registry_hold_socket_path(&claim.token)?;
        let stream =
            novarocks_failpoint::liveness::connect_until(&path, cancellation.deadline(), || {
                !cancellation.is_active()
            })
            .map_err(|error| format!("connect original prepared exit owner: {error}"))?;
        Self::from_stream(claim, identity, stream, runtime, readiness).map(Some)
    }
    fn from_stream(
        claim: NativeHoldClaim,
        identity: TaskIdentity,
        stream: UnixStream,
        runtime: &tokio::runtime::Handle,
        readiness: Waker,
    ) -> Result<Self, String> {
        stream
            .set_nonblocking(true)
            .map_err(|error| error.to_string())?;
        let execution = identity.query_execution_id();
        let marker = format!(
            "NPE1 {} {} {} {} {} {} {}\n",
            identity.backend_process_id(),
            execution.query_id().high(),
            execution.query_id().low(),
            execution.attempt_id().get(),
            identity.stage_id().get(),
            identity.task_id().get(),
            claim.token
        )
        .into_bytes();
        if marker.len() > 512 {
            return Err("prepared exit marker exceeds its finite identity bound".into());
        }
        // Register the same original descriptor before the first readiness
        // check. The existing Native I/O reactor wakes the original Task;
        // no extra async task, polling timer or Work is created.
        let _entered = runtime.enter();
        // SAFETY: this owned UnixStream supplies its one live descriptor. The
        // AsyncFd keeps ownership until deregistration and drop; our readiness
        // helpers only borrow it for I/O or shutdown, never replace/close the
        // descriptor or change its underlying file description.
        let stream = unsafe { tokio::io::unix::AsyncFd::register(stream) }
            .map_err(|error| error.to_string())?;
        let cancellation = claim.cancellation();
        let cancellation = Box::pin(async move { cancellation.cancelled().await });
        Ok(Self {
            claim,
            identity,
            cause: crate::task_protocol_fault::injected_prepared_task_failure(),
            stream,
            runtime: runtime.clone(),
            readiness,
            cancellation,
            marker,
            written: 0,
            announced: false,
            open: false,
        })
    }
    pub(super) fn cause(&self) -> &ExecutionFailure {
        &self.cause
    }
    fn release(&mut self, reason: &str) -> bool {
        if !self.open {
            self.open = true;
            let execution = self.identity.query_execution_id();
            eprintln!(
                "NOVAROCKS_PREPARED_FAILURE_EXIT_RELEASED backend={} execution_id={}:{}:{} stage={} task={} token={} reason={reason}",
                self.identity.backend_process_id(),
                execution.query_id().high(),
                execution.query_id().low(),
                execution.attempt_id().get(),
                self.identity.stage_id().get(),
                self.identity.task_id().get(),
                self.claim.token
            );
        }
        true
    }
    /// At most one bounded write and one single-byte read per original turn.
    /// Pending I/O parks the same original Task on its real reactor readiness;
    /// the fixed cancellation future also retains the original claim deadline.
    pub(super) fn poll_open(&mut self) -> bool {
        if self.open {
            return true;
        }
        let runtime = self.runtime.clone();
        let _entered = runtime.enter();
        let readiness = self.readiness.clone();
        let mut context = Context::from_waker(&readiness);
        if self.cancellation.as_mut().poll(&mut context).is_ready() {
            return self.release("owner-ended");
        }
        let cancellation = self.claim.cancellation();
        if !cancellation.is_active() {
            return self.release("owner-ended");
        }
        if Instant::now() >= cancellation.deadline() {
            return self.release("deadline");
        }
        if self.written < self.marker.len() {
            match poll_write_once(&self.stream, &mut context, &self.marker[self.written..]) {
                Poll::Pending => return false,
                Poll::Ready(Ok(0)) => return self.release("eof"),
                Poll::Ready(Ok(count)) => self.written += count,
                Poll::Ready(Err(error))
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                    ) =>
                {
                    self.readiness.wake_by_ref();
                    return false;
                }
                Poll::Ready(Err(_)) => return self.release("invalid"),
            }
            if self.written < self.marker.len() {
                self.readiness.wake_by_ref();
                return false;
            }
        }
        if !self.announced {
            if let Err(error) = self.claim.record_hit() {
                tracing::error!(%error,"prepared exit hold could not record its actual accepted hit");
                return self.release("invalid");
            }
            if self.stream.get_ref().shutdown(Shutdown::Write).is_err() {
                return self.release("invalid");
            }
            self.announced = true;
        }
        let mut byte = [0u8; 1];
        match poll_read_once(&self.stream, &mut context, &mut byte) {
            Poll::Pending => false,
            Poll::Ready(Ok(1)) if byte[0] == b'R' => self.release("release"),
            Poll::Ready(Ok(0)) => self.release("eof"),
            Poll::Ready(Ok(_)) => self.release("invalid"),
            Poll::Ready(Err(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                ) =>
            {
                self.readiness.wake_by_ref();
                false
            }
            Poll::Ready(Err(_)) => self.release("invalid"),
        }
    }
}

fn poll_read_once(
    stream: &tokio::io::unix::AsyncFd<UnixStream>,
    context: &mut Context<'_>,
    bytes: &mut [u8],
) -> Poll<std::io::Result<usize>> {
    let mut ready = match stream.poll_read_ready(context) {
        Poll::Pending => return Poll::Pending,
        Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
        Poll::Ready(Ok(ready)) => ready,
    };
    match ready.try_io(|inner| inner.get_ref().read(bytes)) {
        Ok(result) => Poll::Ready(result),
        Err(_) => {
            // try_io clears stale readiness on WouldBlock. Register again
            // before parking; an already-arrived new event gets one real turn.
            drop(ready);
            if stream.poll_read_ready(context).is_ready() {
                context.waker().wake_by_ref();
            }
            Poll::Pending
        }
    }
}
fn poll_write_once(
    stream: &tokio::io::unix::AsyncFd<UnixStream>,
    context: &mut Context<'_>,
    bytes: &[u8],
) -> Poll<std::io::Result<usize>> {
    let mut ready = match stream.poll_write_ready(context) {
        Poll::Pending => return Poll::Pending,
        Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
        Poll::Ready(Ok(ready)) => ready,
    };
    match ready.try_io(|inner| inner.get_ref().write(bytes)) {
        Ok(result) => Poll::Ready(result),
        Err(_) => {
            drop(ready);
            if stream.poll_write_ready(context).is_ready() {
                context.waker().wake_by_ref();
            }
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_controls::tests::{controlled, task};
    #[test]
    fn original_nonblocking_gate_waits_for_release_and_eof_without_restarting_its_claim() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        for eof in [false, true] {
            let fixture = controlled("prepared-failure-exit-hold");
            let identity = task(&fixture, 2);
            let claim = fixture
                .controls
                .claim_prepared_failure_exit_hold(identity)
                .unwrap()
                .unwrap();
            let (stream, mut peer) = UnixStream::pair().unwrap();
            let mut gate = PreparedExitGate::from_stream(
                claim,
                identity,
                stream,
                runtime.handle(),
                Waker::noop().clone(),
            )
            .unwrap();
            runtime.block_on(async {
                drop(gate.stream.writable().await.unwrap());
            });
            assert!(!gate.poll_open());
            let mut marker = String::new();
            peer.read_to_string(&mut marker).unwrap();
            assert_eq!(
                marker,
                format!("NPE1 {} 71 73 1 1 2 {}\n", fixture.process, fixture.token)
            );
            assert!(!gate.poll_open());
            if eof {
                drop(peer);
            } else {
                peer.write_all(b"R").unwrap();
            }
            runtime.block_on(async {
                drop(gate.stream.readable().await.unwrap());
            });
            assert!(gate.poll_open());
            assert!(gate.poll_open());
            let hits = fixture
                .role
                .scope()
                .observations()
                .iter()
                .filter(|event| event.event == "hit")
                .count();
            assert_eq!(hits, 1);
        }
    }
}
