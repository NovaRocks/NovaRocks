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

//! One original, fully route-known Native handoff. No runtime is created.
use super::super::root_domains::{FactCapture, GroupOrigin};
use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::joint_admission::ledger) enum NativeDispatchKind {
    Action,
    Reply,
    Input,
}
#[derive(Clone, Copy)]
pub(in crate::joint_admission::ledger) struct NativeDispatchOrigin {
    pub(in crate::joint_admission::ledger) root: WorkId,
    pub(in crate::joint_admission::ledger) execution: WorkId,
    pub(in crate::joint_admission::ledger) ticket: u64,
    pub(in crate::joint_admission::ledger) kind: NativeDispatchKind,
    pub(in crate::joint_admission::ledger) frozen: bool,
    pub(in crate::joint_admission::ledger) lane: JointLane,
}
struct Member {
    pin: OriginalPin,
    facts: Option<Arc<ResponsibilityFacts>>,
    flat: Option<Arc<ResponsibilityFacts>>,
    structure: u64,
    release: u64,
}
/// A notification only: no graph, owner, pin, I/O right or historical Root view.
pub struct NativeDispatchFrozen {
    epoch: u64,
}
impl NativeDispatchFrozen {
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}
type RootGraphCapture = (u64, Option<Arc<Vec<DependencyRef>>>);
type RootGraphWait = Pin<Box<dyn Future<Output = Result<(), WorkError>> + Send>>;

/// Original pins keep the real paid rows alive while this finite preparer runs.
/// A row admits one staging token, and a frozen row admits no second capture.
pub struct NativeDispatchSource {
    inner: Arc<Inner>,
    token: u64,
    action: u64,
    root: WorkId,
    lane: JointLane,
    members: Vec<Member>,
    root_graph: Option<Arc<Vec<DependencyRef>>>,
    epoch: Option<u64>,
    cursor: usize,
    capture: Option<FactCapture>,
    wait: Option<RootGraphWait>,
    complete: bool,
    per_turn: usize,
    _exit: crate::scope::ReclaimWakeOnExit,
}
fn pin_error(error: RetentionPinError) -> WorkError {
    match error {
        RetentionPinError::Released => WorkError::Released,
        RetentionPinError::Conflict => WorkError::Conflict,
        RetentionPinError::Overflow => WorkError::ArithmeticOverflow,
    }
}
fn valid_pin<'a>(state: &'a State, pin: &OriginalPin) -> Result<&'a ClaimRecord, WorkError> {
    let [id, epoch] = pin.key.ok_or(WorkError::Released)?;
    let row = state
        .joint
        .as_ref()
        .and_then(|l| l.claims.get(&id))
        .ok_or(WorkError::Released)?;
    let valid = match (pin.kind, row.custody) {
        (
            Kind::NativeAction,
            ClaimCustody::NativeAction {
                epoch: actual,
                pins,
            },
        )
        | (
            Kind::NativeReply,
            ClaimCustody::NativeReply {
                epoch: actual,
                pins,
            },
        )
        | (
            Kind::NativeInput,
            ClaimCustody::NativeInput {
                epoch: actual,
                pins,
                queued: true,
            },
        ) => epoch == actual && pins != 0,
        _ => false,
    };
    if !valid
        || row.pending.is_some()
        || row.amounts != row.active_amounts
        || row.facts.key != row.active_facts.key
    {
        return Err(WorkError::Conflict);
    }
    Ok(row)
}
struct Construction {
    inner: Arc<Inner>,
    action: u64,
    token: Option<u64>,
}
impl Drop for Construction {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            let mut state = self.inner.state.lock().unwrap();
            if let Some(row) = state
                .joint
                .as_mut()
                .and_then(|l| l.claims.get_mut(&self.action))
                && row.dispatch_staging == Some(token)
            {
                row.dispatch_staging = None;
            }
        }
    }
}
impl NativeDispatchSource {
    /// Heap only. The actual original F/backing layout separately pays this
    /// Source's inline header. Graph backing uses the original per-claim slots.
    pub fn metadata_backing_bytes(input_count: usize) -> Result<usize, WorkError> {
        let count = input_count
            .checked_add(2)
            .ok_or(WorkError::ArithmeticOverflow)?;
        crate::control_storage::array_bytes::<Member>(count)?
            .checked_add(wait_backing_bytes())
            .ok_or(WorkError::ArithmeticOverflow)
    }
    pub fn capture<'a>(
        action: &NativeAsyncActionGrant,
        reply: &NativeReplyGrant,
        mut inputs: impl ExactSizeIterator<Item = Option<&'a NativeQueuedInputGrant>>,
    ) -> Result<Self, WorkError> {
        if !Arc::ptr_eq(&action.pin.inner, &reply.pin.inner) {
            return Err(WorkError::ForeignAuthority);
        }
        let inner = Arc::clone(&action.pin.inner);
        let count = inputs.len();
        let bytes = Self::metadata_backing_bytes(count)?;
        let action_id = action.pin.key.ok_or(WorkError::Released)?[0];
        // One exact original Action reserves construction before any member
        // allocation. Concurrent capture cannot allocate against the same fee.
        let (origin, token, per_turn) = {
            let mut state = inner.state.lock().unwrap();
            let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
            if ledger.driver_closed {
                return Err(WorkError::Closed);
            }
            let a = valid_pin(&state, &action.pin)?;
            let r = valid_pin(&state, &reply.pin)?;
            let origin = a.native_dispatch.ok_or(WorkError::Conflict)?;
            let other = r.native_dispatch.ok_or(WorkError::Conflict)?;
            if origin.kind != NativeDispatchKind::Action
                || other.kind != NativeDispatchKind::Reply
                || origin.ticket != other.ticket
                || origin.root != other.root
                || origin.execution != other.execution
                || origin.frozen
                || other.frozen
                || a.dispatch_staging.is_some()
                || r.dispatch_staging.is_some()
                || u64::try_from(count).map_err(|_| WorkError::ArithmeticOverflow)?
                    != r.amounts.get(JointResource::ReplyRecords)
                || u64::try_from(bytes).map_err(|_| WorkError::ArithmeticOverflow)?
                    > a.amounts.get(JointResource::MetadataBytes)
            {
                return Err(WorkError::Conflict);
            }
            let per_turn = ledger.config.work_per_turn.get();
            let token = state.next_id()?;
            state
                .joint
                .as_mut()
                .unwrap()
                .claims
                .get_mut(&action_id)
                .unwrap()
                .dispatch_staging = Some(token);
            (origin, token, per_turn)
        };
        let mut construction = Construction {
            inner: Arc::clone(&inner),
            action: action_id,
            token: Some(token),
        };
        let mut members = Vec::new();
        members
            .try_reserve_exact(count.checked_add(2).ok_or(WorkError::ArithmeticOverflow)?)
            .map_err(|_| WorkError::Capacity("Native dispatch member backing"))?;
        let member = |pin: &OriginalPin| -> Result<Member, WorkError> {
            Ok(Member {
                pin: pin.try_clone().map_err(pin_error)?,
                facts: None,
                flat: None,
                structure: 0,
                release: 0,
            })
        };
        members.push(member(&action.pin)?);
        members.push(member(&reply.pin)?);
        for _ in 0..count {
            let input = inputs.next().flatten().ok_or(WorkError::Conflict)?;
            if !Arc::ptr_eq(&input.pin.inner, &inner) {
                return Err(WorkError::ForeignAuthority);
            }
            members.push(member(&input.pin)?);
        }
        if inputs.next().is_some() {
            return Err(WorkError::Conflict);
        }
        // No vtable, iterator callback, allocation or foreign Drop under State.
        members.sort_unstable_by_key(|m| m.pin.key.unwrap()[0]);
        if members
            .windows(2)
            .any(|p| p[0].pin.key.unwrap()[0] == p[1].pin.key.unwrap()[0])
        {
            return Err(WorkError::Conflict);
        }
        {
            let mut state = inner.state.lock().unwrap();
            for m in &members {
                let row = valid_pin(&state, &m.pin)?;
                let actual = row.native_dispatch.ok_or(WorkError::Conflict)?;
                if actual.root != origin.root
                    || actual.execution != origin.execution
                    || actual.frozen
                    || (row.dispatch_staging.is_some()
                        && !(m.pin.key.unwrap()[0] == action_id
                            && row.dispatch_staging == Some(token)))
                {
                    return Err(WorkError::Conflict);
                }
                match row.facts.key.origin {
                    GroupOrigin::Inherited(root) if root.work == origin.root => {}
                    GroupOrigin::Frozen
                        if state
                            .nodes
                            .get(&origin.root)
                            .and_then(|n| n.root_domains.as_ref())
                            .is_none() => {}
                    _ => return Err(WorkError::Conflict),
                }
            }
            for m in &mut members {
                let row = state
                    .joint
                    .as_mut()
                    .unwrap()
                    .claims
                    .get_mut(&m.pin.key.unwrap()[0])
                    .unwrap();
                row.dispatch_staging = Some(token);
                m.facts = Some(Arc::clone(&row.active_facts));
                m.structure = row.structure_revision;
                m.release = row.release_revision;
            }
        }
        construction.token = None;
        Ok(Self {
            inner: Arc::clone(&inner),
            token,
            action: action_id,
            root: origin.root,
            lane: origin.lane,
            members,
            root_graph: None,
            epoch: None,
            cursor: 0,
            capture: None,
            wait: None,
            complete: false,
            per_turn,
            _exit: crate::scope::ReclaimWakeOnExit::observe(&Arc::downgrade(&inner)),
        })
    }
    fn clear_graphs(&mut self) {
        self.capture = None;
        self.root_graph = None;
        for member in &mut self.members {
            member.flat = None;
        }
    }
    fn capture_active_root(&self) -> Result<RootGraphCapture, WorkError> {
        let state = self.inner.state.lock().unwrap();
        if state
            .joint
            .as_ref()
            .ok_or(WorkError::NotReady)?
            .driver_closed
        {
            return Err(WorkError::Closed);
        }
        let root = state.nodes.get(&self.root).ok_or(WorkError::Released)?;
        if root.owner != crate::OwnerState::Active {
            return Err(WorkError::Released);
        }
        if self.lane == JointLane::Foreground {
            root.check(&state.cancellations)?;
        }
        let (epoch, graph) = match &root.root_domains {
            Some(domains) if domains.epoch != 0 => {
                (domains.epoch, Some(Arc::clone(&domains.active)))
            }
            Some(_) => return Err(WorkError::NotReady),
            None => (0, None),
        };
        let row = state
            .joint
            .as_ref()
            .unwrap()
            .claims
            .get(&self.action)
            .ok_or(WorkError::Released)?;
        if row.dispatch_staging != Some(self.token) {
            return Err(WorkError::Conflict);
        }
        row.native_dispatch.as_ref().ok_or(WorkError::Conflict)?;
        Ok((epoch, graph))
    }
    fn commit(&mut self) -> Result<bool, WorkError> {
        let mut state = self.inner.state.lock().unwrap();
        if state
            .nodes
            .get(&self.root)
            .and_then(|n| n.root_domains.as_ref())
            .is_some_and(|d| d.pending.is_some())
        {
            return Ok(false);
        }
        let node = state.nodes.get(&self.root).ok_or(WorkError::Released)?;
        if self.lane == JointLane::Foreground {
            node.check(&state.cancellations)?;
        }
        let actual_epoch = state
            .nodes
            .get(&self.root)
            .ok_or(WorkError::Released)?
            .root_domains
            .as_ref()
            .map_or(0, |d| d.epoch);
        if Some(actual_epoch) != self.epoch {
            return Ok(false);
        }
        let ledger = state.joint.as_ref().ok_or(WorkError::NotReady)?;
        if ledger.driver_closed {
            return Err(WorkError::Closed);
        }
        ledger
            .facts_revision
            .checked_add(1)
            .ok_or(WorkError::ArithmeticOverflow)?;
        for member in &self.members {
            let row = valid_pin(&state, &member.pin)?;
            if row.dispatch_staging != Some(self.token)
                || row.structure_revision != member.structure
                || row.release_revision != member.release
                || row.active_facts.key != member.facts.as_ref().unwrap().key
                || member
                    .flat
                    .as_ref()
                    .is_none_or(|f| f.key.origin != GroupOrigin::Frozen)
            {
                return Err(WorkError::Conflict);
            }
        }
        let ledger = state.joint.as_mut().unwrap();
        for member in &self.members {
            let target = Arc::clone(member.flat.as_ref().unwrap());
            let row = ledger.claims.get_mut(&member.pin.key.unwrap()[0]).unwrap();
            // Source retains old S and any unused dedup candidate in its two
            // paid slots until the lock exits. Neither graph can final-Drop here.
            row.facts = Arc::clone(&target);
            row.active_facts = target;
            row.dispatch_staging = None;
            row.native_dispatch.as_mut().unwrap().frozen = true;
        }
        ledger.layout_changed();
        state.advance_progress_revision();
        Ok(true)
    }
}
impl Future for NativeDispatchSource {
    type Output = Result<NativeDispatchFrozen, WorkError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.complete {
            return Poll::Ready(Err(WorkError::Released));
        }
        if let Some(wait) = &mut self.wait {
            match wait.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    self.complete = true;
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(Ok(())) => self.wait = None,
            }
        }
        let pending = {
            let state = self.inner.state.lock().unwrap();
            state
                .nodes
                .get(&self.root)
                .and_then(|n| n.root_domains.as_ref())
                .is_some_and(|d| d.pending.is_some())
        };
        if pending {
            self.wait = Some(Box::pin(wait_root(
                Arc::clone(&self.inner),
                self.root,
                self.lane,
            )));
            let wait = self.wait.as_mut().unwrap();
            return match wait.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(error)) => {
                    self.complete = true;
                    Poll::Ready(Err(error))
                }
                Poll::Ready(Ok(())) => {
                    self.wait = None;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            };
        }
        for _ in 0..self.per_turn {
            let (epoch, graph) = match self.capture_active_root() {
                Ok(value) => value,
                Err(error) => {
                    self.complete = true;
                    return Poll::Ready(Err(error));
                }
            };
            if self.epoch != Some(epoch) {
                self.clear_graphs();
                self.cursor = 0;
                self.epoch = Some(epoch);
                self.root_graph = graph;
            } else {
                drop(graph);
            }
            if self.cursor < self.members.len() {
                if self.capture.is_none() {
                    self.capture = Some(FactCapture::active(
                        Arc::clone(self.members[self.cursor].facts.as_ref().unwrap()),
                        self.root_graph.as_ref().map(Arc::clone),
                    )?);
                }
                match self.capture.as_mut().unwrap().step() {
                    Err(error) => {
                        self.complete = true;
                        return Poll::Ready(Err(error));
                    }
                    Ok(Poll::Pending) => continue,
                    Ok(Poll::Ready(flat)) => {
                        self.capture = None;
                        let cursor = self.cursor;
                        self.members[cursor].flat = Some(flat);
                        self.cursor += 1;
                    }
                }
                continue;
            }
            // Free the captured-history slot BEFORE dedup/old-S retirement.
            self.root_graph = None;
            if let Err(error) = self.capture_active_root() {
                self.complete = true;
                return Poll::Ready(Err(error));
            }
            match self.commit() {
                Err(error) => {
                    self.complete = true;
                    return Poll::Ready(Err(error));
                }
                Ok(false) => {
                    self.clear_graphs();
                    self.cursor = 0;
                    self.epoch = None;
                }
                Ok(true) => {
                    let epoch = self.epoch.unwrap();
                    self.clear_graphs();
                    for member in &mut self.members {
                        member.facts = None;
                    }
                    self.complete = true;
                    self.inner.notify_reclaim_progress();
                    return Poll::Ready(Ok(NativeDispatchFrozen { epoch }));
                }
            }
        }
        // Original Future yield only; no private executor/queue/authority.
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
impl Drop for NativeDispatchSource {
    fn drop(&mut self) {
        self.wait = None;
        self.clear_graphs();
        for member in &mut self.members {
            member.facts = None;
        }
        {
            let mut state = self.inner.state.lock().unwrap();
            for member in &self.members {
                let id = member.pin.key.unwrap()[0];
                // Keep this original Action's construction lock until its paid
                // member allocation has actually exited. Other members no
                // longer retain any graph after clear_graphs above.
                if id == self.action {
                    continue;
                }
                if let Some(row) = state.joint.as_mut().and_then(|l| l.claims.get_mut(&id))
                    && row.dispatch_staging == Some(self.token)
                {
                    row.dispatch_staging = None;
                }
            }
        }
        drop(std::mem::take(&mut self.members));
        {
            let mut state = self.inner.state.lock().unwrap();
            if let Some(row) = state
                .joint
                .as_mut()
                .and_then(|l| l.claims.get_mut(&self.action))
                && row.dispatch_staging == Some(self.token)
            {
                row.dispatch_staging = None;
            }
        }
    }
}

async fn wait_root(inner: Arc<Inner>, root: WorkId, lane: JointLane) -> Result<(), WorkError> {
    loop {
        let changed = inner.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        {
            let state = inner.state.lock().unwrap();
            if state
                .joint
                .as_ref()
                .ok_or(WorkError::NotReady)?
                .driver_closed
            {
                return Err(WorkError::Closed);
            }
            let node = state.nodes.get(&root).ok_or(WorkError::Released)?;
            if lane == JointLane::Foreground {
                node.check(&state.cancellations)?;
            }
            if node
                .root_domains
                .as_ref()
                .is_none_or(|d| d.pending.is_none())
            {
                return Ok(());
            }
        }
        changed.await;
    }
}
fn wait_backing_bytes() -> usize {
    fn layout<F: Future<Output = Result<(), WorkError>>>(
        _: fn(Arc<Inner>, WorkId, JointLane) -> F,
    ) -> usize {
        std::mem::size_of::<F>()
    }
    layout(wait_root)
}
