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

//! Capacity-pressure retirement of original bounded actor connection owners.

use super::*;
use std::os::fd::AsRawFd;
use std::sync::{Weak, atomic::AtomicBool};
use tokio::io::unix::AsyncFd;
use tokio::sync::OwnedSemaphorePermit;

pub(super) fn register_listener(listener: TcpListener) -> Result<AsyncFd<TcpListener>> {
    // SAFETY: AsyncFd exclusively owns this original TcpListener and its stable
    // open descriptor. We only borrow it to accept; no path replaces or closes
    // the descriptor before AsyncFd deregisters and drops its owned listener.
    unsafe { AsyncFd::register_with_interest(listener, tokio::io::Interest::READABLE) }
        .context("register original actor listener readiness")
}

/// A cached reactor event is not evidence of a pending connection after accept.
/// Observe the same owned descriptor without accepting or registering it again.
fn listener_has_pending_connection(listener: &TcpListener) -> Result<bool> {
    loop {
        let mut descriptor = libc::pollfd {
            fd: listener.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: The borrowed listener keeps this original descriptor open and
        // stable. The single initialized pollfd remains valid for this call;
        // zero timeout only observes readiness and neither consumes nor owns it.
        let observed = unsafe { libc::poll(&mut descriptor, 1, 0) };
        if observed < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error).context("observe original actor listener readiness");
        }
        return match (observed, descriptor.revents) {
            (0, 0) => Ok(false),
            (1, libc::POLLIN) => Ok(true),
            (count, events) => bail!(
                "original actor listener reported unexpected poll result: count={count}, events={events:#x}"
            ),
        };
    }
}

/// Explicit provenance for every actor-owned payload allocation. Production
/// has only the connection-bound constructor; standalone buffers are test-only.
#[derive(Clone)]
pub(super) struct BufferContext {
    pub(super) core: Arc<Core>,
    binding: BufferBinding,
}
#[derive(Clone)]
enum BufferBinding {
    Connection(Arc<ConnectionState>),
    #[cfg(test)]
    Standalone,
}
impl BufferContext {
    pub(super) fn connection(core: Arc<Core>, connection: Arc<ConnectionState>) -> Self {
        Self {
            core,
            binding: BufferBinding::Connection(connection),
        }
    }
    #[cfg(test)]
    pub(super) fn standalone(core: Arc<Core>) -> Self {
        Self {
            core,
            binding: BufferBinding::Standalone,
        }
    }
    pub(super) fn charge(&self, bytes: usize) {
        match &self.binding {
            BufferBinding::Connection(connection) => {
                let previous = connection.bytes.fetch_add(bytes, Ordering::AcqRel);
                if bytes > 0 && previous == 0 {
                    connection.accepted_during_grace();
                }
                self.core
                    .connection_bound_buffer_charges
                    .fetch_add(1, Ordering::AcqRel);
            }
            #[cfg(test)]
            BufferBinding::Standalone => {
                self.core
                    .standalone_buffer_charges
                    .fetch_add(1, Ordering::AcqRel);
            }
        }
    }
    pub(super) fn refund(&self, bytes: usize) {
        match &self.binding {
            BufferBinding::Connection(connection) => {
                let old = connection.bytes.fetch_sub(bytes, Ordering::AcqRel);
                assert!(old >= bytes, "connection payload accounting underflow");
                self.core
                    .connection_bound_buffer_drops
                    .fetch_add(1, Ordering::AcqRel);
                if old == bytes {
                    connection.changed.notify_waiters();
                    connection.retire_if_complete();
                }
            }
            #[cfg(test)]
            BufferBinding::Standalone => {}
        }
    }
}

/// The original position follows transport exit AND the last payload alias.
/// The weak Core edge prevents an ownership cycle. No new capacity is minted.
pub(super) struct ConnectionState {
    core: Weak<Core>,
    pub(super) id: u64,
    pub(super) bytes: AtomicUsize,
    pub(super) changed: Notify,
    position: Mutex<Option<OwnedSemaphorePermit>>,
    transport_exited: AtomicBool,
    children_joined: AtomicBool,
    draining: AtomicBool,
}
impl ConnectionState {
    pub(super) fn new(core: &Arc<Core>, position: OwnedSemaphorePermit) -> Result<Arc<Self>> {
        let id = core
            .next_connection_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |id| id.checked_add(1))
            .map_err(|_| anyhow::anyhow!("actor connection identity exhausted"))?;
        core.connections.fetch_add(1, Ordering::AcqRel);
        Ok(Arc::new(Self {
            core: Arc::downgrade(core),
            id,
            bytes: AtomicUsize::new(0),
            changed: Notify::new(),
            position: Mutex::new(Some(position)),
            transport_exited: AtomicBool::new(false),
            children_joined: AtomicBool::new(true),
            draining: AtomicBool::new(false),
        }))
    }
    pub(super) fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }
    pub(super) fn spawned_children(&self) {
        self.children_joined.store(false, Ordering::Release);
    }
    pub(super) fn joined_children(&self) {
        self.children_joined.store(true, Ordering::Release);
        self.retire_if_complete();
    }
    pub(super) fn idle(&self, application_children: usize, h2_streams: bool) -> bool {
        application_children == 0 && !h2_streams && self.bytes.load(Ordering::Acquire) == 0
    }
    pub(super) fn begin_idle_drain(&self, application_children: usize, h2_streams: bool) -> bool {
        let Some(core) = self.core.upgrade() else {
            return false;
        };
        if self.draining.load(Ordering::Acquire)
            || !self.idle(application_children, h2_streams)
            || core.pressure_listeners.load(Ordering::Acquire) == 0
            || core.connection_positions.available_permits() != 0
        {
            return false;
        }
        if core
            .idle_victim
            .compare_exchange(0, self.id, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        // The selection marker is only bookkeeping. Recheck current pressure
        // and actual local ownership after acquiring it, never a stale receipt.
        if !self.idle(application_children, h2_streams)
            || core.pressure_listeners.load(Ordering::Acquire) == 0
            || core.connection_positions.available_permits() != 0
        {
            let _ =
                core.idle_victim
                    .compare_exchange(self.id, 0, Ordering::AcqRel, Ordering::Acquire);
            core.changed.notify_waiters();
            return false;
        }
        self.draining.store(true, Ordering::Release);
        core.idle_drains_started.fetch_add(1, Ordering::AcqRel);
        true
    }
    /// A legal request can arrive during GOAWAY's grace RTT. Its original
    /// position remains held; only the non-authoritative idle selection moves.
    pub(super) fn accepted_during_grace(&self) {
        let Some(core) = self.core.upgrade() else {
            return;
        };
        if self.draining.load(Ordering::Acquire)
            && core
                .idle_victim
                .compare_exchange(self.id, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            core.idle_drain_yields.fetch_add(1, Ordering::AcqRel);
            core.changed.notify_waiters();
        }
    }
    fn mark_transport_exited(&self) {
        self.transport_exited.store(true, Ordering::Release);
        self.retire_if_complete();
    }
    fn retire_if_complete(&self) {
        if !self.transport_exited.load(Ordering::Acquire)
            || !self.children_joined.load(Ordering::Acquire)
            || self.bytes.load(Ordering::Acquire) != 0
        {
            return;
        }
        let mut original = self
            .position
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(position) = original.take() else {
            return;
        };
        if let Some(core) = self.core.upgrade() {
            core.connections.fetch_sub(1, Ordering::AcqRel);
            // Release the original resource before publishing a capacity wake.
            drop(position);
            if self.draining.load(Ordering::Acquire) {
                core.idle_drains_completed.fetch_add(1, Ordering::AcqRel);
            }
            let _ =
                core.idle_victim
                    .compare_exchange(self.id, 0, Ordering::AcqRel, Ordering::Acquire);
            core.changed.notify_waiters();
        } else {
            drop(position);
        }
    }
}
impl Drop for ConnectionState {
    fn drop(&mut self) {
        if let Some(position) = self
            .position
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            if let Some(core) = self.core.upgrade() {
                core.failure(
                    "connection position could not retire after its original owners exited",
                );
                core.stop.send_replace(true);
            }
            // An unknown/unjoined exit must never advertise reusable capacity.
            position.forget();
        }
    }
}

pub(super) struct ConnectionExit(pub(super) Arc<ConnectionState>);
impl Drop for ConnectionExit {
    fn drop(&mut self) {
        self.0.mark_transport_exited();
    }
}

struct PressureRegistration {
    core: Arc<Core>,
    active: bool,
}
impl PressureRegistration {
    fn new(core: Arc<Core>) -> Self {
        Self {
            core,
            active: false,
        }
    }
    fn register(&mut self) {
        if !self.active {
            self.active = true;
            self.core.pressure_listeners.fetch_add(1, Ordering::AcqRel);
            self.core.changed.notify_waiters();
        }
    }
    fn clear(&mut self) {
        if self.active {
            self.active = false;
            self.core.pressure_listeners.fetch_sub(1, Ordering::AcqRel);
            self.core.changed.notify_waiters();
        }
    }
}
impl Drop for PressureRegistration {
    fn drop(&mut self) {
        self.clear();
    }
}

/// Readiness observes the original listening FD without accepting a socket.
/// Full listeners retain no extra connection or application-side wait queue.
pub(super) async fn accept_owned(
    listener: &AsyncFd<TcpListener>,
    core: &Arc<Core>,
) -> Result<(tokio::net::TcpStream, OwnedSemaphorePermit)> {
    let mut pressure = PressureRegistration::new(core.clone());
    loop {
        let mut ready = listener
            .readable()
            .await
            .context("actor listener readiness")?;
        loop {
            let changed = core.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match core.connection_positions.clone().try_acquire_owned() {
                Ok(position) => {
                    pressure.clear();
                    match ready.try_io(|listener| listener.get_ref().accept()) {
                        Ok(Ok((stream, _))) => {
                            // Accepted sockets do not portably inherit O_NONBLOCK.
                            stream
                                .set_nonblocking(true)
                                .context("set accepted actor socket nonblocking")?;
                            return Ok((tokio::net::TcpStream::from_std(stream)?, position));
                        }
                        Ok(Err(error)) => {
                            return Err(error).context("accept original actor socket");
                        }
                        Err(_) => {
                            drop(position);
                            core.changed.notify_waiters();
                            break;
                        }
                    }
                }
                Err(_) => {
                    if !listener_has_pending_connection(listener.get_ref())? {
                        pressure.clear();
                        // Kernel poll observed no pending connection. Like an
                        // accept returning WouldBlock, this clears only the
                        // cached event represented by this guard, not a newer
                        // reactor event racing the observation.
                        ready.clear_ready();
                        break;
                    }
                    pressure.register();
                    // Do not poll the persistently-readable listener again until
                    // capacity changes. The registration precedes this recheck.
                    if core.connection_positions.available_permits() != 0 {
                        continue;
                    }
                    changed.await;
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn finish_children(
    children: &mut JoinSet<Result<ChildExit>>,
    mut application_children: usize,
    sender: client::SendRequest<Bytes>,
    mut natural: bool,
    stop: &mut watch::Receiver<bool>,
    generation: &mut watch::Receiver<u64>,
    core: &Arc<Core>,
) -> bool {
    let mut failed = false;
    let mut sender = Some(sender);
    natural = natural
        && !*stop.borrow()
        && !stop.has_changed().unwrap_or(true)
        && !generation.has_changed().unwrap_or(true);
    if natural {
        // Every application child already owns its original request/stream
        // references. The parent's idle template must not hold driver EOF.
        drop(sender.take());
        // Wire EOF is not application-future exit. Keep every original child
        // alive until its post-wire responsibilities settle and it is joined.
        while application_children > 0 && natural {
            tokio::select! {
                _=stop.changed()=>natural=false,
                _=generation.changed()=>natural=false,
                joined=children.join_next()=>{
                    core.joined_children.fetch_add(1,Ordering::AcqRel);
                    match joined {
                        Some(Ok(Ok(ChildExit::Stream)))=>application_children-=1,
                        Some(Ok(Ok(ChildExit::Upstream)))=>{},
                        Some(Ok(Err(error)))=>{core.failure(&format!("root natural child exit: {error:#}"));failed=true;natural=false;},
                        Some(Err(error))=>{core.failure(&format!("root natural child join: {error}"));failed=true;natural=false;},
                        None=>{core.failure("root natural drain lost its application child owner");failed=true;natural=false;},
                    }
                }
            }
        }
        if natural {
            // Every application child is joined. Drive the original upstream
            // connection's actual completion, without aborting its driver.
            while !children.is_empty() && natural {
                tokio::select! {
                    _=stop.changed()=>natural=false,
                    _=generation.changed()=>natural=false,
                    joined=children.join_next()=>{
                        core.joined_children.fetch_add(1,Ordering::AcqRel);
                        match joined {
                            Some(Ok(Ok(ChildExit::Upstream)))=>{},
                            Some(Ok(Ok(ChildExit::Stream)))=>{core.failure("root natural drain found an uncounted application child");failed=true;natural=false;},
                            Some(Ok(Err(error)))=>{core.failure(&format!("root natural upstream exit: {error:#}"));failed=true;natural=false;},
                            Some(Err(error))=>{core.failure(&format!("root natural upstream join: {error}"));failed=true;natural=false;},
                            None=>{},
                        }
                    }
                }
            }
        }
    }
    // Only original shutdown/generation cuts or actual failures abort work.
    // Every aborted original handle is still joined before its owner returns.
    if !natural {
        children.abort_all();
    }
    while let Some(joined) = children.join_next().await {
        core.joined_children.fetch_add(1, Ordering::AcqRel);
        match joined {
            Ok(Err(error)) => {
                failed = true;
                core.failure(&format!("root child cleanup failure: {error:#}"));
            }
            Err(error) if !error.is_cancelled() => {
                failed = true;
                core.failure(&format!("root child cleanup join: {error}"));
            }
            Ok(Ok(_)) | Err(_) => {}
        }
    }
    drop(sender);
    failed
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn original_position_waits_for_all_joined_children_and_the_last_byte_alias() -> Result<()>
    {
        let core = super::super::tests::core();
        let maximum = core.bounds.maximum_connections;
        let connection = ConnectionState::new(
            &core,
            core.connection_positions.clone().try_acquire_owned()?,
        )?;
        connection.spawned_children();
        let mut owner = OwnedBuffer::new(
            BufferContext::connection(core.clone(), connection.clone()),
            8,
        )?;
        owner.append(b"original")?;
        let body = owner.into_bytes();
        let alias = body.slice(1..);
        drop(body);
        drop(ConnectionExit(connection.clone()));
        assert_eq!(core.connection_positions.available_permits(), maximum - 1);
        drop(alias);
        assert_eq!(connection.bytes.load(Ordering::Acquire), 0);
        assert_eq!(
            core.connection_positions.available_permits(),
            maximum - 1,
            "unjoined children still own the original position"
        );
        connection.joined_children();
        assert_eq!(core.connection_positions.available_permits(), maximum);
        assert_eq!(core.connections.load(Ordering::Acquire), 0);
        assert_eq!(
            core.connection_bound_buffer_charges.load(Ordering::Acquire),
            1
        );
        assert_eq!(
            core.connection_bound_buffer_drops.load(Ordering::Acquire),
            1
        );
        assert_eq!(core.standalone_buffer_charges.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[tokio::test]
    async fn last_alias_after_transport_exit_wakes_the_original_owner_before_refund() -> Result<()>
    {
        let core = super::super::tests::core();
        let maximum = core.bounds.maximum_connections;
        let connection = ConnectionState::new(
            &core,
            core.connection_positions.clone().try_acquire_owned()?,
        )?;
        let body = OwnedBuffer::new(
            BufferContext::connection(core.clone(), connection.clone()),
            8,
        )?
        .into_bytes();
        let alias = body.clone();
        drop(body);
        let changed = connection.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        drop(ConnectionExit(connection.clone()));
        assert_eq!(core.connection_positions.available_permits(), maximum - 1);
        assert_eq!(connection.bytes.load(Ordering::Acquire), 8);
        drop(alias);
        tokio::time::timeout(Duration::from_secs(1), changed).await?;
        assert_eq!(core.connection_positions.available_permits(), maximum);
        assert_eq!(core.bytes.load(Ordering::Acquire), 0);
        Ok(())
    }

    #[tokio::test]
    async fn pressure_cannot_select_a_live_unjoined_or_payload_owning_connection() -> Result<()> {
        let core = super::super::tests::core();
        let connection = ConnectionState::new(
            &core,
            core.connection_positions.clone().try_acquire_owned()?,
        )?;
        let filler = core
            .connection_positions
            .clone()
            .try_acquire_many_owned(u32::try_from(core.bounds.maximum_connections - 1)?)?;
        let mut pressure = PressureRegistration::new(core.clone());
        pressure.register();
        assert!(!connection.begin_idle_drain(1, false));
        assert!(!connection.begin_idle_drain(0, true));
        let mut children = JoinSet::new();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        children.spawn(async move {
            let _ = done_tx.send(());
        });
        done_rx.await?;
        assert!(
            !connection.begin_idle_drain(children.len(), false),
            "finished-but-unjoined application ownership is not idle"
        );
        children
            .join_next()
            .await
            .context("application child absent")??;
        let body = OwnedBuffer::new(
            BufferContext::connection(core.clone(), connection.clone()),
            8,
        )?
        .into_bytes();
        assert!(!connection.begin_idle_drain(0, false));
        drop(body);
        assert!(connection.begin_idle_drain(0, false));
        assert_eq!(core.idle_victim.load(Ordering::Acquire), connection.id);
        drop(ConnectionExit(connection));
        drop(filler);
        pressure.clear();
        Ok(())
    }

    #[tokio::test]
    async fn grace_admission_yields_only_selection_while_both_original_positions_stay_owned()
    -> Result<()> {
        let core = super::super::tests::core();
        let maximum = core.bounds.maximum_connections;
        let a = ConnectionState::new(
            &core,
            core.connection_positions.clone().try_acquire_owned()?,
        )?;
        let b = ConnectionState::new(
            &core,
            core.connection_positions.clone().try_acquire_owned()?,
        )?;
        let filler = core
            .connection_positions
            .clone()
            .try_acquire_many_owned(u32::try_from(maximum - 2)?)?;
        let mut pressure = PressureRegistration::new(core.clone());
        pressure.register();
        assert!(a.begin_idle_drain(0, false));
        assert!(!b.begin_idle_drain(0, false));
        a.accepted_during_grace();
        assert_eq!(core.connection_positions.available_permits(), 0);
        assert!(b.begin_idle_drain(0, false));
        assert!(!a.begin_idle_drain(0, false));
        drop(ConnectionExit(a));
        assert_eq!(
            core.idle_victim.load(Ordering::Acquire),
            b.id,
            "an older owner must not clear its successor's selection"
        );
        assert_eq!(core.connection_positions.available_permits(), 1);
        drop(ConnectionExit(b));
        assert_eq!(core.connection_positions.available_permits(), 2);
        drop(filler);
        pressure.clear();
        assert_eq!(core.idle_drain_yields.load(Ordering::Acquire), 1);
        assert_eq!(core.idle_drains_completed.load(Ordering::Acquire), 2);
        Ok(())
    }

    #[tokio::test]
    async fn cached_listener_readiness_without_pending_connection_cannot_create_pressure()
    -> Result<()> {
        tokio::time::timeout(Duration::from_secs(3), async {
            let core = super::super::tests::core();
            let maximum = core.bounds.maximum_connections;
            let filler = core
                .connection_positions
                .clone()
                .try_acquire_many_owned(u32::try_from(maximum - 1)?)?;
            let listener = TcpListener::bind("127.0.0.1:0")?;
            listener.set_nonblocking(true)?;
            let address = listener.local_addr()?;
            let listener = register_listener(listener)?;
            let first_client = tokio::net::TcpStream::connect(address).await?;
            let (first_socket, position) = accept_owned(&listener, &core).await?;
            let original = ConnectionState::new(&core, position)?;
            assert_eq!(core.connection_positions.available_permits(), 0);

            // Successful accept retains the reactor event until WouldBlock (or
            // a non-consuming kernel observation) proves it is stale.
            let cached = listener.readable();
            tokio::pin!(cached);
            std::future::poll_fn(|context| {
                assert!(std::future::Future::poll(cached.as_mut(), context).is_ready());
                std::task::Poll::Ready(())
            })
            .await;

            let accept = accept_owned(&listener, &core);
            tokio::pin!(accept);
            std::future::poll_fn(|context| {
                assert!(std::future::Future::poll(accept.as_mut(), context).is_pending());
                assert_eq!(core.pressure_listeners.load(Ordering::Acquire), 0);
                assert!(!original.begin_idle_drain(0, false));
                assert_eq!(core.idle_drains_started.load(Ordering::Acquire), 0);
                std::task::Poll::Ready(())
            })
            .await;

            let cold_client = tokio::net::TcpStream::connect(address).await?;
            loop {
                let changed = core.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                tokio::select! {
                    result = &mut accept => bail!(
                        "full listener accepted without an original position: {}", result.is_ok()
                    ),
                    _ = &mut changed => {}
                }
                if core.pressure_listeners.load(Ordering::Acquire) == 1 {
                    break;
                }
            }
            assert!(original.begin_idle_drain(0, false));
            drop(first_socket);
            drop(ConnectionExit(original));
            let (cold_socket, position) = accept.await?;
            drop(cold_socket);
            drop(position);
            drop((first_client, cold_client, filler));
            assert_eq!(core.pressure_listeners.load(Ordering::Acquire), 0);
            assert_eq!(core.connection_positions.available_permits(), maximum);
            assert_eq!(core.connections.load(Ordering::Acquire), 0);
            assert_eq!(core.bytes.load(Ordering::Acquire), 0);
            assert!(
                core.state
                    .lock()
                    .expect("root fault state lock")
                    .failures
                    .is_empty()
            );
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("cached-readiness capacity fixture deadline")?
    }

    #[tokio::test]
    async fn two_full_listeners_keep_separate_pressure_until_original_capacity_retires()
    -> Result<()> {
        tokio::time::timeout(Duration::from_secs(3),async {
            let core=super::super::tests::core();let maximum=core.bounds.maximum_connections;
            let filler=core.connection_positions.clone().try_acquire_many_owned(u32::try_from(maximum-1)?)?;
            let original=ConnectionState::new(&core,core.connection_positions.clone().try_acquire_owned()?)?;
            let a=TcpListener::bind("127.0.0.1:0")?;a.set_nonblocking(true)?;let address_a=a.local_addr()?;let a=Arc::new(register_listener(a)?);
            let b=TcpListener::bind("127.0.0.1:0")?;b.set_nonblocking(true)?;let address_b=b.local_addr()?;let b=Arc::new(register_listener(b)?);
            let mut clients=[tokio::net::TcpStream::connect(address_a).await?,tokio::net::TcpStream::connect(address_b).await?];
            // Both original listener futures are polled inside this scope. No
            // detached accept task or accepted-socket wait queue is introduced.
            let accept_a=accept_owned(&a,&core);let accept_b=accept_owned(&b,&core);tokio::pin!(accept_a,accept_b);
            loop {
                let changed=core.changed.notified();tokio::pin!(changed);changed.as_mut().enable();
                tokio::select! {result=&mut accept_a=>bail!("full listener A accepted without a position: {}",result.is_ok()),result=&mut accept_b=>bail!("full listener B accepted without a position: {}",result.is_ok()),_=&mut changed=>{}}
                if core.pressure_listeners.load(Ordering::Acquire)==2 {break}
            }
            assert_eq!(core.connections.load(Ordering::Acquire),1);drop(ConnectionExit(original));
            let ((mut stream,position),first)=tokio::select! {result=&mut accept_a=>(result?,0usize),result=&mut accept_b=>(result?,1usize)};
            assert_eq!(core.pressure_listeners.load(Ordering::Acquire),1);
            clients[first].write_all(b"x").await?;let mut byte=[0];stream.read_exact(&mut byte).await?;assert_eq!(byte,[b'x']);
            drop(stream);let accepted=ConnectionState::new(&core,position)?;drop(ConnectionExit(accepted));
            let (stream,position)=if first==0 {accept_b.await?}else{accept_a.await?};
            assert_eq!(core.pressure_listeners.load(Ordering::Acquire),0);drop(stream);drop(position);drop(filler);Ok::<_,anyhow::Error>(())
        }).await.context("two-listener capacity fixture deadline")?
    }
}

#[cfg(test)]
mod h2_tests {
    use super::*;
    use tokio::sync::oneshot;

    async fn echo(mut sender: client::SendRequest<Bytes>, path: &str) -> Result<Bytes> {
        sender = sender.ready().await?;
        let (reply, mut send) = sender.send_request(
            Request::builder()
                .uri(format!("http://localhost{path}"))
                .body(())?,
            false,
        )?;
        send.send_data(Bytes::from_static(b"original"), true)?;
        let reply = reply.await?;
        let mut body = reply.into_body();
        let mut result = Vec::new();
        while let Some(chunk) = body.data().await {
            let chunk = chunk?;
            result.extend_from_slice(&chunk);
            body.flow_control().release_capacity(chunk.len())?;
        }
        let trailers = body.trailers().await?.context("echo trailers absent")?;
        assert_eq!(trailers.get("grpc-status").unwrap(), "0");
        Ok(Bytes::from(result))
    }
    // This peer qualifies H2 framing and original actor ownership only. It
    // does not replace the required Native task/query/real-role qualification.
    #[derive(Default)]
    struct EchoState {
        held: AtomicUsize,
        arrived: Notify,
        release: Notify,
    }
    async fn peer_connection(
        io: tokio::net::TcpStream,
        state: Arc<EchoState>,
        mut stop: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut connection = server::handshake::<_>(io).await?;
        let mut handlers = JoinSet::new();
        let mut failure = None;
        loop {
            tokio::select! {
                _=stop.changed()=>break,
                incoming=connection.accept()=>{
                    let Some(incoming)=incoming else{break};let (request,mut reply)=incoming?;let state=state.clone();
                    handlers.spawn(async move {
                        let held=request.uri().path()=="/capacity-held";let mut body=request.into_body();let mut bytes=Vec::new();
                        while let Some(chunk)=body.data().await {let chunk=chunk?;bytes.extend_from_slice(&chunk);body.flow_control().release_capacity(chunk.len())?;}
                        if held {state.held.fetch_add(1,Ordering::AcqRel);state.arrived.notify_waiters();state.release.notified().await;}
                        let mut output=reply.send_response(Response::new(()),false)?;output.send_data(Bytes::from(bytes),false)?;
                        let mut trailers=HeaderMap::new();trailers.insert("grpc-status",http::HeaderValue::from_static("0"));output.send_trailers(trailers)?;Ok::<_,anyhow::Error>(())
                    });
                },
                joined=handlers.join_next(),if !handlers.is_empty()=>{if let Some(joined)=joined && let Err(error)=joined? {failure=Some(error);break}},
            }
        }
        handlers.abort_all();
        while let Some(joined) = handlers.join_next().await {
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    if failure.is_none() {
                        failure = Some(error)
                    }
                }
                Err(error) if error.is_cancelled() => {}
                Err(error) => {
                    if failure.is_none() {
                        failure = Some(error.into())
                    }
                }
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        Ok(())
    }
    async fn echo_service(
        listener: tokio::net::TcpListener,
        state: Arc<EchoState>,
        mut stop: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut connections = JoinSet::new();
        let mut failure = None;
        loop {
            tokio::select! {
                _=stop.changed()=>break,
                accepted=listener.accept()=>{let (io,_)=accepted?;connections.spawn(peer_connection(io,state.clone(),stop.clone()));},
                joined=connections.join_next(),if !connections.is_empty()=>{if let Some(joined)=joined && let Err(error)=joined? {failure=Some(error);break}},
            }
        }
        // Each connection observes this same original stop, then joins its
        // own handler set. The listener joins the original connection handles.
        while let Some(joined) = connections.join_next().await {
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    if failure.is_none() {
                        failure = Some(error)
                    }
                }
                Err(error) => {
                    if failure.is_none() {
                        failure = Some(error.into())
                    }
                }
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        Ok(())
    }

    #[tokio::test]
    async fn original_32_positions_gracefully_recycle_idle_h2_without_aborting_an_active_sibling()
    -> Result<()> {
        let fixture = super::super::tests::core();
        let state = Arc::new(EchoState::default());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let upstream = listener.local_addr()?;
        let (peer_stop, peer_stopped) = watch::channel(false);
        let service = tokio::spawn(echo_service(listener, state.clone(), peer_stopped));
        let mut proxy = RootReplyFaultProxy::start(
            upstream,
            0,
            None,
            RootReplyFaultBounds::default(),
            fixture.verifier.clone(),
            fixture.normal_subject.clone(),
        )?;
        let control = proxy.control();
        let mut drivers = JoinSet::new();
        let mut senders = Vec::new();
        let mut held = None;
        let result = tokio::time::timeout(Duration::from_secs(8), async {
            let maximum = control.core.bounds.maximum_connections;
            assert_eq!(maximum, 32);
            for index in 0..maximum {
                let io = tokio::net::TcpStream::connect(proxy.address()).await?;
                let (sender, driver) = client::handshake(io).await?;
                drivers
                    .spawn(async move { driver.await.context("capacity original client driver") });
                assert_eq!(
                    echo(sender.clone(), "/capacity-echo").await?,
                    Bytes::from_static(b"original")
                );
                if index == 0 {
                    assert_eq!(
                        echo(sender.clone(), "/capacity-echo").await?,
                        Bytes::from_static(b"original")
                    );
                    assert_eq!(
                        control.observation().idle_drains_started,
                        0,
                        "low-pressure idle reuse must not emit GOAWAY"
                    );
                }
                senders.push(sender);
            }
            assert_eq!(control.observation().active_connections, maximum);
            held = Some(tokio::spawn(echo(senders[0].clone(), "/capacity-held")));
            loop {
                let arrived = state.arrived.notified();
                tokio::pin!(arrived);
                arrived.as_mut().enable();
                if state.held.load(Ordering::Acquire) == 1 {
                    break;
                }
                arrived.await;
            }
            assert_eq!(
                control.observation().idle_drains_started,
                0,
                "full capacity without a pending connection must not emit GOAWAY"
            );
            let io = tokio::net::TcpStream::connect(proxy.address()).await?;
            let (cold, driver) = client::handshake(io).await?;
            drivers.spawn(async move { driver.await.context("capacity cold client driver") });
            assert_eq!(
                echo(cold.clone(), "/capacity-echo").await?,
                Bytes::from_static(b"original")
            );
            let observed = control.observation();
            assert!(observed.idle_drains_started > 0);
            assert!(observed.idle_drains_completed > 0);
            assert!(observed.active_connections <= 32);
            assert!(observed.failures.is_empty(), "{:?}", observed.failures);
            assert!(
                !held.as_ref().unwrap().is_finished(),
                "the original active sibling must remain owned"
            );
            state.release.notify_one();
            let joined = held.as_mut().unwrap().await;
            held.take();
            assert_eq!(joined??, Bytes::from_static(b"original"));
            senders.push(cold);
            Ok::<_, anyhow::Error>(())
        })
        .await;
        state.release.notify_one();
        if let Some(task) = held {
            task.abort();
            let _ = task.await;
        }
        drop(senders);
        // Successful peers close through their original H2 drivers. A fixture
        // failure still aborts and joins those same handles as bounded cleanup.
        if !matches!(&result, Ok(Ok(()))) {
            drivers.abort_all();
        }
        let mut cleanup_error = None;
        while let Some(joined) = drivers.join_next().await {
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    if cleanup_error.is_none() {
                        cleanup_error = Some(error)
                    }
                }
                Err(error) if error.is_cancelled() => {}
                Err(error) => {
                    if cleanup_error.is_none() {
                        cleanup_error = Some(error.into())
                    }
                }
            }
        }
        proxy.stop();
        peer_stop.send_replace(true);
        let service_result = service.await?;
        result.context("original 32-position real H2 pressure deadline")??;
        if let Some(error) = cleanup_error {
            return Err(error);
        }
        service_result?;
        let observed = control.observation();
        assert!(observed.shutdown_joined);
        assert_eq!(observed.active_connections, 0);
        assert_eq!(observed.connection_positions, 0);
        assert_eq!(observed.owned_buffer_bytes, 0);
        assert!(observed.connection_bound_buffer_charges > 0);
        assert_eq!(
            observed.connection_bound_buffer_charges,
            observed.connection_bound_buffer_drops
        );
        assert_eq!(
            observed.standalone_buffer_charges, 0,
            "all real Root/opaque forwarding allocations retain connection provenance"
        );
        assert!(observed.failures.is_empty(), "{:?}", observed.failures);
        Ok(())
    }

    #[derive(Clone, Copy)]
    enum OriginalCut {
        None,
        Stop,
        Generation,
    }
    async fn grace_tail_case(cut: OriginalCut) -> Result<()> {
        let core = super::super::tests::core();
        let filler = core
            .connection_positions
            .clone()
            .try_acquire_many_owned(31)?;
        let connection = ConnectionState::new(
            &core,
            core.connection_positions.clone().try_acquire_owned()?,
        )?;
        let mut pressure = PressureRegistration::new(core.clone());
        pressure.register();
        let mut drivers = JoinSet::new();
        let mut owner = None;
        let (release_tx, release_rx) = oneshot::channel();
        let (queued_tx, queued_rx) = oneshot::channel();
        let (grace_tx, grace_rx) = oneshot::channel();
        let (alias_tx, alias_rx) = oneshot::channel();
        let (eof_tx, eof_rx) = oneshot::channel();
        let (cleanup_tx, cleanup_rx) = oneshot::channel();
        let completed = Arc::new(AtomicBool::new(false));
        let (generation, generation_rx) = watch::channel(0);
        let result = tokio::time::timeout(Duration::from_secs(4), async {
            let (down_client, down_server) = tokio::io::duplex(65536);
            let (up_client, up_server) = tokio::io::duplex(65536);
            let (mut downstream, (sender, client_driver)) = tokio::try_join!(
                server::handshake(down_server),
                client::handshake(down_client)
            )?;
            let (mut upstream, (up_sender, up_driver)) =
                tokio::try_join!(server::handshake(up_server), client::handshake(up_client))?;
            drivers.spawn(async move {
                client_driver
                    .await
                    .context("grace original downstream driver")
            });
            drivers.spawn(async move {
                while upstream.accept().await.is_some() {}
                Ok::<_, anyhow::Error>(())
            });
            let owner_core = core.clone();
            let owner_connection = connection.clone();
            let owner_completed = completed.clone();
            owner = Some(tokio::spawn(async move {
                let _exit = ConnectionExit(owner_connection.clone());
                owner_connection.spawned_children();
                let mut children = JoinSet::new();
                children.spawn(async move {
                    up_driver.await?;
                    Ok(ChildExit::Upstream)
                });
                assert!(owner_connection.begin_idle_drain(0, downstream.has_streams()));
                downstream.graceful_shutdown();
                let _ = grace_tx.send(());
                queued_rx.await?;
                let (request, mut response) = downstream
                    .accept()
                    .await
                    .context("legal grace request was discarded")??;
                assert_eq!(request.uri().path(), "/in-flight");
                owner_connection.accepted_during_grace();
                let context =
                    BufferContext::connection(owner_core.clone(), owner_connection.clone());
                children.spawn(async move {
                    let _stream = CountGuard::new(context.core.clone(), false);
                    let mut input = request.into_body();
                    while let Some(chunk) = input.data().await {
                        let chunk = chunk?;
                        input.flow_control().release_capacity(chunk.len())?;
                    }
                    let mut buffer = OwnedBuffer::new(context, 8)?;
                    buffer.append(b"original")?;
                    let body = buffer.into_bytes();
                    let mut output = response.send_response(Response::new(()), false)?;
                    output.send_data(body.clone(), true)?;
                    let _ = alias_tx.send(body.clone());
                    release_rx.await?;
                    drop(body);
                    owner_completed.store(true, Ordering::Release);
                    Ok::<_, anyhow::Error>(ChildExit::Stream)
                });
                assert!(
                    downstream.accept().await.is_none(),
                    "grace fixture must reach real wire EOF"
                );
                let _ = eof_tx.send(());
                cleanup_rx.await?;
                let mut stop = owner_core.stop.subscribe();
                let mut generation = generation_rx;
                let failed = finish_children(
                    &mut children,
                    1,
                    up_sender,
                    true,
                    &mut stop,
                    &mut generation,
                    &owner_core,
                )
                .await;
                assert!(!failed);
                drop(downstream);
                owner_connection.joined_children();
                Ok::<_, anyhow::Error>(())
            }));
            grace_rx.await?;
            let mut sender = sender.ready().await?;
            let (reply, mut send) = sender.send_request(
                Request::builder()
                    .uri("http://localhost/in-flight")
                    .body(())?,
                false,
            )?;
            send.send_data(Bytes::new(), true)?;
            let _ = queued_tx.send(());
            let reply = reply.await?;
            let mut body = reply.into_body();
            let bytes = body
                .data()
                .await
                .context("legal grace response missing")??;
            assert_eq!(&bytes[..], b"original");
            body.flow_control().release_capacity(bytes.len())?;
            while let Some(chunk) = body.data().await {
                let chunk = chunk?;
                body.flow_control().release_capacity(chunk.len())?;
            }
            drop(body);
            let alias = alias_rx.await?;
            eof_rx.await?;
            assert!(!owner.as_ref().unwrap().is_finished());
            assert!(!completed.load(Ordering::Acquire));
            assert_eq!(core.connection_positions.available_permits(), 0);
            assert_eq!(core.idle_victim.load(Ordering::Acquire), 0);
            match cut {
                OriginalCut::None => {
                    let _ = release_tx.send(());
                }
                OriginalCut::Stop => {
                    core.stop.send_replace(true);
                }
                OriginalCut::Generation => {
                    generation.send_replace(1);
                }
            }
            let _ = cleanup_tx.send(());
            let joined = owner.as_mut().unwrap().await;
            owner.take();
            joined??;
            assert_eq!(
                completed.load(Ordering::Acquire),
                matches!(cut, OriginalCut::None)
            );
            assert_eq!(
                core.connection_positions.available_permits(),
                0,
                "post-wire payload alias still owns the original position"
            );
            drop(alias);
            assert_eq!(core.connection_positions.available_permits(), 1);
            drop(send);
            drop(sender);
            Ok::<_, anyhow::Error>(())
        })
        .await;
        core.stop.send_replace(true);
        generation.send_replace(2);
        if let Some(task) = owner {
            task.abort();
            let _ = task.await;
        }
        drivers.abort_all();
        let mut cleanup_error = None;
        while let Some(joined) = drivers.join_next().await {
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    if cleanup_error.is_none() {
                        cleanup_error = Some(error)
                    }
                }
                Err(error) if error.is_cancelled() => {}
                Err(error) => {
                    if cleanup_error.is_none() {
                        cleanup_error = Some(error.into())
                    }
                }
            }
        }
        drop(filler);
        pressure.clear();
        result.context("real H2 grace tail deadline")??;
        if let Some(error) = cleanup_error {
            return Err(error);
        }
        assert_eq!(core.connections.load(Ordering::Acquire), 0);
        assert_eq!(core.bytes.load(Ordering::Acquire), 0);
        assert!(core.state.lock().unwrap().failures.is_empty());
        Ok(())
    }
    #[tokio::test]
    async fn legal_inflight_h2_child_naturally_joins_after_wire_eof_and_last_alias() -> Result<()> {
        grace_tail_case(OriginalCut::None).await
    }
    #[tokio::test]
    async fn original_stop_interrupts_natural_grace_tail_then_joins_original_children() -> Result<()>
    {
        grace_tail_case(OriginalCut::Stop).await
    }
    #[tokio::test]
    async fn original_generation_cut_interrupts_natural_grace_tail_then_joins_original_children()
    -> Result<()> {
        grace_tail_case(OriginalCut::Generation).await
    }
}
