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

//! One BE-owned range dispatcher. Queue entries represent requests, while
//! window slots represent physical segments and survive until task exit.

// Design: ADR-0158 (docs/adr/ADR-0158-bounded-parquet-range-preparation.md)

use std::collections::{HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::{ConnectorOperationTicket, ConnectorSourceOperations};
use tokio::runtime::Handle;
use tokio::sync::{Notify, oneshot};

use crate::{
    BoundFile, FileCancellation, FileError, FileErrorKind, FileReadRange, FileResult,
    FileTaskSpawner, PreparedFileInput,
};

const SEGMENT_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FileRangeScope {
    query: (i64, i64, u64),
    source: (i64, i64, i32),
}

impl FileRangeScope {
    pub fn try_new(
        query_high: i64,
        query_low: i64,
        attempt: u64,
        fragment_high: i64,
        fragment_low: i64,
        node_id: i32,
    ) -> FileResult<Self> {
        if query_high == 0 && query_low == 0 {
            return Err(FileError::invalid("range query identity must be nonzero"));
        }
        if attempt == 0 {
            return Err(FileError::invalid("range query attempt must be nonzero"));
        }
        if fragment_high == 0 && fragment_low == 0 {
            return Err(FileError::invalid(
                "range fragment identity must be nonzero",
            ));
        }
        if node_id < 0 {
            return Err(FileError::invalid(
                "range source node id must be nonnegative",
            ));
        }
        Ok(Self {
            query: (query_high, query_low, attempt),
            source: (fragment_high, fragment_low, node_id),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileRangeClass {
    Demand,
    Prefetch,
}

pub enum FileRangeStart {
    Started(FileRangeRequest),
    Deferred,
}

pub enum FileStatStart {
    Started(FileStatRequest),
    Deferred,
}

/// One execution source's handle on the shared range service.
///
/// Every request made through it is scheduled under the source's exact scope
/// and is admitted to the source's operations the moment it is submitted,
/// before it waits for queue room or starts any work. Sealing those
/// operations refuses later submissions and stops every admitted request,
/// and the source's exit observes each request's physical exit, not only its
/// result.
#[derive(Clone)]
pub struct FileRangeBinding {
    service: Arc<FileRangeService>,
    scope: FileRangeScope,
    operations: ConnectorSourceOperations,
}

/// A submitted request's registration: its id, its own cancellation and the
/// ticket that ends once the request physically exited. Dropped before the
/// request is queued, the ticket ends as an operation that never started.
struct Admission {
    id: u64,
    cancellation: FileCancellation,
    ticket: ConnectorOperationTicket,
}

enum Queued<T> {
    Started(T),
    Deferred(Admission),
}

impl std::fmt::Debug for FileRangeBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FileRangeBinding")
            .field("scope", &self.scope)
            .field("operations", &self.operations)
            .finish_non_exhaustive()
    }
}

impl FileRangeBinding {
    pub fn scope(&self) -> FileRangeScope {
        self.scope
    }

    pub fn operations(&self) -> &ConnectorSourceOperations {
        &self.operations
    }

    pub fn service(&self) -> &Arc<FileRangeService> {
        &self.service
    }

    /// The same service and scope, admitting to `operations` instead: a
    /// stream reading one unit of the source binds the unit's own child
    /// operations, so closing the unit observes only its requests.
    pub fn with_operations(&self, operations: ConnectorSourceOperations) -> FileRangeBinding {
        FileRangeBinding {
            service: Arc::clone(&self.service),
            scope: self.scope,
            operations,
        }
    }

    /// Admits a read that must start now: a full queue is an error.
    pub fn start(
        &self,
        class: FileRangeClass,
        file: BoundFile,
        range: FileReadRange,
        cancellation: FileCancellation,
    ) -> FileResult<FileRangeRequest> {
        match self.try_start(class, file, range, cancellation)? {
            FileRangeStart::Started(request) => Ok(request),
            FileRangeStart::Deferred => Err(FileError::new(
                FileErrorKind::ResourceExhausted,
                "range request queue is full",
            )),
        }
    }

    pub fn try_start(
        &self,
        class: FileRangeClass,
        file: BoundFile,
        range: FileReadRange,
        cancellation: FileCancellation,
    ) -> FileResult<FileRangeStart> {
        self.try_start_with_present(class, file, range, cancellation, None)
    }

    /// Admits a read now, or reports that it would have to wait for room.
    pub fn try_start_with_present(
        &self,
        class: FileRangeClass,
        file: BoundFile,
        range: FileReadRange,
        cancellation: FileCancellation,
        present: Option<PreparedFileInput>,
    ) -> FileResult<FileRangeStart> {
        let admission = self.admit(&cancellation)?;
        Ok(
            match self.service.queue_read(
                self.scope,
                admission,
                class,
                file,
                range,
                present.as_ref(),
            )? {
                Queued::Started(request) => FileRangeStart::Started(request),
                Queued::Deferred(_) => FileRangeStart::Deferred,
            },
        )
    }

    pub async fn start_wait(
        &self,
        file: BoundFile,
        range: FileReadRange,
        cancellation: FileCancellation,
    ) -> FileResult<FileRangeRequest> {
        self.start_wait_with_present(file, range, cancellation, None)
            .await
    }

    /// Admits a demand read, waiting for queue room. The request is already
    /// the source's operation while it waits, so sealing the source ends the
    /// wait.
    pub async fn start_wait_with_present(
        &self,
        file: BoundFile,
        range: FileReadRange,
        cancellation: FileCancellation,
        present: Option<PreparedFileInput>,
    ) -> FileResult<FileRangeRequest> {
        let _waiter = DemandWaitGuard::new(Arc::clone(&self.service));
        let mut admission = self.admit(&cancellation)?;
        loop {
            let changed = self.service.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self.service.queue_read(
                self.scope,
                admission,
                FileRangeClass::Demand,
                file.clone(),
                range,
                present.as_ref(),
            )? {
                Queued::Started(request) => return Ok(request),
                Queued::Deferred(deferred) => {
                    admission = deferred;
                    tokio::select! {
                        _ = &mut changed => {}
                        error = admission.cancellation.ended() => return Err(error),
                    }
                }
            }
        }
    }

    /// Admits one HEAD of `file` as demand work now, or reports that it would
    /// have to wait for room.
    pub fn try_stat(
        &self,
        file: BoundFile,
        cancellation: FileCancellation,
    ) -> FileResult<FileStatStart> {
        let admission = self.admit(&cancellation)?;
        Ok(
            match self.service.queue_stat(self.scope, admission, file)? {
                Queued::Started(request) => FileStatStart::Started(request),
                Queued::Deferred(_) => FileStatStart::Deferred,
            },
        )
    }

    /// Admits one HEAD of `file`, waiting for queue room like a demand read.
    pub async fn stat_wait(
        &self,
        file: BoundFile,
        cancellation: FileCancellation,
    ) -> FileResult<FileStatRequest> {
        let _waiter = DemandWaitGuard::new(Arc::clone(&self.service));
        let mut admission = self.admit(&cancellation)?;
        loop {
            let changed = self.service.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self
                .service
                .queue_stat(self.scope, admission, file.clone())?
            {
                Queued::Started(request) => return Ok(request),
                Queued::Deferred(deferred) => {
                    admission = deferred;
                    tokio::select! {
                        _ = &mut changed => {}
                        error = admission.cancellation.ended() => return Err(error),
                    }
                }
            }
        }
    }

    /// Registers one request with the source before anything else happens to
    /// it. Its stop cancels the request's own cancellation, which ends a wait
    /// for room, and stops the request once it is queued.
    fn admit(&self, cancellation: &FileCancellation) -> FileResult<Admission> {
        cancellation.check()?;
        let id = self.service.next_request_id()?;
        let cancellation = cancellation.child();
        let stop = {
            let service = Arc::downgrade(&self.service);
            let cancellation = cancellation.clone();
            Arc::new(move || {
                cancellation.cancel();
                if let Some(service) = service.upgrade() {
                    service.stop_request(id);
                }
            })
        };
        let ticket = self
            .operations
            .admit(stop)
            .map_err(|_| FileError::cancelled("range request source is closed"))?;
        Ok(Admission {
            id,
            cancellation,
            ticket,
        })
    }
}

/// One HEAD of a bound file, admitted and supervised like a range read: it
/// takes a process and a source window slot, is dispatched fairly with demand
/// reads, and its slot returns only when its task exited. Dropping it stops it.
pub struct FileStatRequest {
    service: Arc<FileRangeService>,
    id: u64,
    cancellation: FileCancellation,
    size: Option<oneshot::Receiver<FileResult<u64>>>,
    exit: Option<oneshot::Receiver<FileResult<()>>>,
}

impl Drop for FileStatRequest {
    fn drop(&mut self) {
        self.request_stop();
    }
}

impl FileStatRequest {
    /// The object's current size. Awaited in place: a dropped wait loses
    /// nothing and a later one receives the same answer.
    pub async fn size_ready(&mut self) -> FileResult<u64> {
        let receiver = self
            .size
            .as_mut()
            .ok_or_else(|| FileError::invalid("stat result already consumed"))?;
        let outcome = receiver.await;
        self.size = None;
        outcome
            .map_err(|_| FileError::new(FileErrorKind::Internal, "stat request lost its result"))?
    }

    /// Waits for the stat task to exit; resumable like the result.
    pub async fn drained(&mut self) -> FileResult<()> {
        let receiver = self
            .exit
            .as_mut()
            .ok_or_else(|| FileError::invalid("stat exit already consumed"))?;
        let outcome = receiver.await;
        self.exit = None;
        outcome.map_err(|_| {
            FileError::new(
                FileErrorKind::Internal,
                "stat request lost its exit receipt",
            )
        })?
    }

    pub fn request_stop(&self) {
        self.cancellation.cancel();
        self.service.stop_request(self.id);
    }
}

pub struct FileRangeRequest {
    service: Arc<FileRangeService>,
    id: u64,
    cancellation: FileCancellation,
    file: BoundFile,
    offset: u64,
    partial_copy_bytes: usize,
    missing_bytes: usize,
    result: Option<oneshot::Receiver<FileResult<RangeOutput>>>,
    exit: Option<oneshot::Receiver<FileResult<()>>>,
}

/// A non-owning dispatch gate for one prefetch request. Dropping this handle
/// does not stop the request; only its owning `FileRangeRequest` can do that.
#[derive(Clone)]
pub struct FileRangeControl {
    service: std::sync::Weak<FileRangeService>,
    id: u64,
}

impl FileRangeControl {
    pub fn request_pause(&self) {
        if let Some(service) = self.service.upgrade() {
            service.set_prefetch_paused(self.id, true);
        }
    }

    pub fn request_resume(&self) {
        if let Some(service) = self.service.upgrade() {
            service.set_prefetch_paused(self.id, false);
        }
    }
}

impl Drop for FileRangeRequest {
    fn drop(&mut self) {
        self.request_stop();
    }
}

impl FileRangeRequest {
    pub fn control(&self) -> FileRangeControl {
        FileRangeControl {
            service: Arc::downgrade(&self.service),
            id: self.id,
        }
    }

    pub async fn result_ready(&mut self) -> FileResult<Bytes> {
        self.take_result().await.map(|output| output.bytes)
    }

    pub async fn prepared_input_ready(&mut self) -> FileResult<PreparedFileInput> {
        let output = self.take_result().await?;
        PreparedFileInput::from_completed(
            &self.file,
            self.offset,
            output.bytes,
            output.retained_backing_capacity,
        )
    }

    pub fn partial_copy_bytes(&self) -> usize {
        self.partial_copy_bytes
    }

    pub fn missing_bytes(&self) -> usize {
        self.missing_bytes
    }

    /// The result receiver is awaited in place: a dropped wait loses
    /// nothing, and a later wait receives the same result.
    async fn take_result(&mut self) -> FileResult<RangeOutput> {
        let receiver = self
            .result
            .as_mut()
            .ok_or_else(|| FileError::invalid("range result already consumed"))?;
        let outcome = receiver.await;
        self.result = None;
        outcome
            .map_err(|_| FileError::new(FileErrorKind::Internal, "range request lost its result"))?
    }

    /// Waits for every physical segment of the request to exit. Awaited in
    /// place like the result, so a dropped wait can be resumed.
    pub async fn drained(&mut self) -> FileResult<()> {
        let receiver = self
            .exit
            .as_mut()
            .ok_or_else(|| FileError::invalid("range exit already consumed"))?;
        let outcome = receiver.await;
        self.exit = None;
        outcome.map_err(|_| {
            FileError::new(
                FileErrorKind::Internal,
                "range request lost its exit receipt",
            )
        })?
    }

    pub fn request_stop(&self) {
        self.cancellation.cancel();
        self.service.stop_request(self.id);
    }

    /// Keeps `hold` alive until the request's physical exit, however long
    /// that outlives this handle: a charge a caller took for the request's
    /// bytes stays taken while a task can still hold them. A request that
    /// already exited drops it at once.
    pub fn retain_until_exit(&self, hold: Box<dyn std::any::Any + Send>) {
        self.service.retain_until_exit(self.id, hold);
    }
}

struct RangeOutput {
    bytes: Bytes,
    retained_backing_capacity: usize,
}

fn prepare_segments(
    offset: u64,
    length: usize,
    present: Option<&PreparedFileInput>,
) -> FileResult<(VecDeque<Segment>, Vec<Option<BytesMut>>, usize)> {
    let end = offset + length as u64;
    let overlap = present.and_then(|input| {
        let range = input.range();
        let start = offset.max(range.start);
        let stop = end.min(range.end);
        (start < stop).then_some(start..stop)
    });
    let mut backing = BytesMut::zeroed(length);
    if backing.capacity() != length {
        return Err(FileError::new(
            FileErrorKind::ResourceExhausted,
            "range target capacity exceeds its reserved length",
        ));
    }
    let mut copy_bytes = 0;
    if let (Some(input), Some(overlap)) = (present, overlap.as_ref()) {
        let target_start = (overlap.start - offset) as usize;
        let source_start = (overlap.start - input.range().start) as usize;
        copy_bytes = (overlap.end - overlap.start) as usize;
        backing[target_start..target_start + copy_bytes]
            .copy_from_slice(&input.bytes()[source_start..source_start + copy_bytes]);
    }
    let mut pending = VecDeque::new();
    let mut completed = Vec::new();
    let mut cursor = offset;
    let mut parts: Vec<(Range<u64>, bool)> = Vec::new();
    if let Some(overlap) = overlap {
        if cursor < overlap.start {
            parts.push((cursor..overlap.start, false));
        }
        parts.push((overlap.clone(), true));
        cursor = overlap.end;
    }
    if cursor < end {
        parts.push((cursor..end, false));
    }
    for (part, ready) in parts {
        let mut part_cursor = part.start;
        while part_cursor < part.end {
            let take = ((part.end - part_cursor) as usize).min(SEGMENT_BYTES);
            let bytes = backing.split_to(take);
            let index = completed.len();
            if ready {
                completed.push(Some(bytes));
            } else {
                completed.push(None);
                pending.push_back(Segment {
                    offset: part_cursor,
                    bytes,
                    index,
                });
            }
            part_cursor += take as u64;
        }
    }
    Ok((pending, completed, copy_bytes))
}

struct Segment {
    offset: u64,
    bytes: BytesMut,
    index: usize,
}

/// What one request asks of the object store.
enum RequestWork {
    /// Byte ranges read into one final backing, one segment per unit.
    Read {
        pending: VecDeque<Segment>,
        completed: Vec<Option<BytesMut>>,
        expected_length: usize,
        result: Option<oneshot::Sender<FileResult<RangeOutput>>>,
    },
    /// One HEAD of the bound file, a single unit.
    Stat {
        pending: bool,
        size: Option<u64>,
        result: Option<oneshot::Sender<FileResult<u64>>>,
    },
}

impl RequestWork {
    fn has_pending(&self) -> bool {
        match self {
            Self::Read { pending, .. } => !pending.is_empty(),
            Self::Stat { pending, .. } => *pending,
        }
    }

    fn next_unit(&mut self) -> DispatchUnit {
        match self {
            Self::Read { pending, .. } => {
                DispatchUnit::Segment(pending.pop_front().expect("queued segment"))
            }
            Self::Stat { pending, .. } => {
                *pending = false;
                DispatchUnit::Stat
            }
        }
    }

    /// Drops the work not dispatched yet and reports `error` as the result.
    fn fail(&mut self, error: FileError) {
        match self {
            Self::Read {
                pending, result, ..
            } => {
                pending.clear();
                if let Some(sender) = result.take() {
                    let _ = sender.send(Err(error));
                }
            }
            Self::Stat {
                pending, result, ..
            } => {
                *pending = false;
                if let Some(sender) = result.take() {
                    let _ = sender.send(Err(error));
                }
            }
        }
    }

    fn record(&mut self, output: UnitOutput) {
        match (self, output) {
            (Self::Read { completed, .. }, UnitOutput::Segment { index, bytes }) => {
                completed[index] = Some(bytes);
            }
            (Self::Stat { size, .. }, UnitOutput::Stat(stat)) => *size = Some(stat),
            _ => unreachable!("a unit reports the work it was dispatched for"),
        }
    }

    /// Publishes the result of work every unit of which completed.
    fn publish(self) {
        match self {
            Self::Read {
                completed,
                expected_length,
                result: Some(sender),
                ..
            } => {
                let mut segments = completed.into_iter();
                let mut backing = segments
                    .next()
                    .expect("nonempty range")
                    .expect("completed segment");
                for segment in segments {
                    backing.unsplit(segment.expect("completed segment"));
                }
                let result =
                    if backing.len() == expected_length && backing.capacity() == expected_length {
                        Ok(RangeOutput {
                            bytes: backing.freeze(),
                            retained_backing_capacity: expected_length,
                        })
                    } else {
                        Err(FileError::new(
                            FileErrorKind::Internal,
                            "range target changed its reserved length or capacity",
                        ))
                    };
                let _ = sender.send(result);
            }
            Self::Stat {
                size,
                result: Some(sender),
                ..
            } => {
                let _ = sender.send(size.ok_or_else(|| {
                    FileError::new(
                        FileErrorKind::Internal,
                        "stat request exited without a size",
                    )
                }));
            }
            Self::Read { result: None, .. } | Self::Stat { result: None, .. } => {}
        }
    }
}

/// One dispatched unit of a request.
enum DispatchUnit {
    Segment(Segment),
    Stat,
}

/// What a dispatched unit produced.
enum UnitOutput {
    Segment { index: usize, bytes: BytesMut },
    Stat(u64),
}

struct RequestState {
    scope: FileRangeScope,
    class: FileRangeClass,
    paused: bool,
    file: BoundFile,
    cancellation: FileCancellation,
    work: RequestWork,
    active: usize,
    failed: bool,
    exit_error: Option<FileError>,
    exit: Option<oneshot::Sender<FileResult<()>>>,
    /// The request's operation in its source, ended with its physical exit.
    ticket: Option<ConnectorOperationTicket>,
    /// What callers keep alive until the physical exit; see
    /// [`FileRangeRequest::retain_until_exit`].
    holds: Vec<Box<dyn std::any::Any + Send>>,
}

#[derive(Default)]
struct State {
    closed: bool,
    active: usize,
    source_active: HashMap<FileRangeScope, usize>,
    requests: HashMap<u64, RequestState>,
    demand: VecDeque<u64>,
    prefetch: VecDeque<u64>,
    last_query: Option<(i64, i64, u64)>,
    last_source: HashMap<(i64, i64, u64), (i64, i64, i32)>,
}

struct Dispatch {
    id: u64,
    scope: FileRangeScope,
    file: BoundFile,
    cancellation: FileCancellation,
    unit: DispatchUnit,
}

pub struct FileRangeService {
    process_window: usize,
    source_window: usize,
    queue_capacity: usize,
    demand_waiters: AtomicUsize,
    next_id: AtomicU64,
    task_spawner: Arc<dyn FileTaskSpawner>,
    scan_handle: Handle,
    state: Mutex<State>,
    /// Segment supervisors that have not exited yet.
    live_supervisors: AtomicUsize,
    changed: Notify,
}

/// Settles one dispatched segment exactly once: with the supervisor's real
/// outcome, or, when the supervisor ends early (its runtime dropped it or it
/// panicked), with an error. The window slot is released either way.
struct SegmentSettlement {
    service: Arc<FileRangeService>,
    id: u64,
    scope: FileRangeScope,
    result_delivered: bool,
    settled: bool,
}

impl SegmentSettlement {
    fn new(service: Arc<FileRangeService>, id: u64, scope: FileRangeScope) -> Self {
        service.live_supervisors.fetch_add(1, Ordering::AcqRel);
        Self {
            service,
            id,
            scope,
            result_delivered: false,
            settled: false,
        }
    }

    fn deliver(&mut self, result: FileResult<UnitOutput>) {
        self.result_delivered = true;
        self.service.unit_result(self.id, result);
    }

    fn settle(mut self, exit: FileResult<()>) {
        self.settled = true;
        self.service.segment_exit(self.id, self.scope, exit);
    }
}

impl Drop for SegmentSettlement {
    fn drop(&mut self) {
        if !self.settled {
            if !self.result_delivered {
                self.service.unit_result(
                    self.id,
                    Err(FileError::new(
                        FileErrorKind::Internal,
                        "range segment supervisor ended before its result",
                    )),
                );
            }
            self.service.segment_exit(
                self.id,
                self.scope,
                Err(FileError::new(
                    FileErrorKind::Internal,
                    "range segment supervisor ended before its task exited",
                )),
            );
        }
        if self.service.live_supervisors.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.service.changed.notify_waiters();
        }
    }
}

struct DemandWaitGuard(Arc<FileRangeService>);

impl DemandWaitGuard {
    fn new(service: Arc<FileRangeService>) -> Self {
        service.demand_waiters.fetch_add(1, Ordering::SeqCst);
        Self(service)
    }
}

impl Drop for DemandWaitGuard {
    fn drop(&mut self) {
        self.0.demand_waiters.fetch_sub(1, Ordering::SeqCst);
        self.0.dispatch();
    }
}

impl FileRangeService {
    pub fn new(
        process_window: NonZeroUsize,
        source_window: NonZeroUsize,
        queue_capacity: NonZeroUsize,
        task_spawner: Arc<dyn FileTaskSpawner>,
        scan_handle: Handle,
    ) -> Arc<Self> {
        Arc::new(Self {
            process_window: process_window.get(),
            source_window: source_window.get(),
            queue_capacity: queue_capacity.get(),
            demand_waiters: AtomicUsize::new(0),
            next_id: AtomicU64::new(0),
            task_spawner,
            scan_handle,
            state: Mutex::new(State::default()),
            live_supervisors: AtomicUsize::new(0),
            changed: Notify::new(),
        })
    }

    /// How many units one source may run at once; a caller planning several
    /// requests of one source gains nothing from submitting more at a time.
    pub fn source_window(&self) -> usize {
        self.source_window
    }

    /// Binds one execution source to this service; see [`FileRangeBinding`].
    pub fn bind(
        self: &Arc<Self>,
        scope: FileRangeScope,
        operations: ConnectorSourceOperations,
    ) -> FileRangeBinding {
        FileRangeBinding {
            service: Arc::clone(self),
            scope,
            operations,
        }
    }

    fn next_request_id(&self) -> FileResult<u64> {
        self.next_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |id| id.checked_add(1))
            .map_err(|_| FileError::new(FileErrorKind::Internal, "range request id exhausted"))
    }

    fn queue_read(
        self: &Arc<Self>,
        scope: FileRangeScope,
        admission: Admission,
        class: FileRangeClass,
        file: BoundFile,
        range: FileReadRange,
        present: Option<&PreparedFileInput>,
    ) -> FileResult<Queued<FileRangeRequest>> {
        if let Some(input) = present {
            input.validate_for(&file)?;
        }
        let (offset, length) = match range {
            FileReadRange::WholeFile => (0, file.identity().file_size()),
            FileReadRange::Bounded { offset, length } => (offset, length),
        };
        if length == 0 {
            return Err(FileError::invalid("range request must be nonempty"));
        }
        if offset
            .checked_add(length)
            .is_none_or(|end| end > file.identity().file_size())
        {
            return Err(FileError::new(
                FileErrorKind::Corrupt,
                "range exceeds bound file length",
            ));
        }
        let length = usize::try_from(length).map_err(|_| {
            FileError::new(
                FileErrorKind::ResourceExhausted,
                "range target length exceeds address space",
            )
        })?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| FileError::new(FileErrorKind::Internal, "range service state poisoned"))?;
        if state.closed {
            return Err(FileError::cancelled("range service admission is closed"));
        }
        // Checked under the lock a stop takes: a source sealed before this
        // point refuses the request, and one sealed after it finds it queued.
        admission.cancellation.check()?;
        if class == FileRangeClass::Prefetch && self.demand_waiters.load(Ordering::SeqCst) != 0 {
            return Ok(Queued::Deferred(admission));
        }
        if state.demand.len() + state.prefetch.len() >= self.queue_capacity {
            if class == FileRangeClass::Prefetch {
                return Ok(Queued::Deferred(admission));
            }
            if !self.make_demand_room_locked(&mut state) {
                return Ok(Queued::Deferred(admission));
            }
        }
        let (pending, completed, partial_copy_bytes) = prepare_segments(offset, length, present)?;
        let (result_sender, result) = oneshot::channel();
        let (exit_sender, exit) = oneshot::channel();
        let Admission {
            id,
            cancellation,
            ticket,
        } = admission;
        state.requests.insert(
            id,
            RequestState {
                scope,
                class,
                paused: false,
                file: file.clone(),
                cancellation: cancellation.clone(),
                work: RequestWork::Read {
                    pending,
                    completed,
                    expected_length: length,
                    result: Some(result_sender),
                },
                active: 0,
                failed: false,
                exit_error: None,
                exit: Some(exit_sender),
                ticket: Some(ticket),
                holds: Vec::new(),
            },
        );
        if !state.requests[&id].work.has_pending() {
            self.finish_locked(&mut state, id);
        } else {
            match class {
                FileRangeClass::Demand => state.demand.push_back(id),
                FileRangeClass::Prefetch => state.prefetch.push_back(id),
            };
        }
        drop(state);
        self.dispatch();
        Ok(Queued::Started(FileRangeRequest {
            service: Arc::clone(self),
            id,
            cancellation,
            file,
            offset,
            partial_copy_bytes,
            missing_bytes: length - partial_copy_bytes,
            result: Some(result),
            exit: Some(exit),
        }))
    }

    fn queue_stat(
        self: &Arc<Self>,
        scope: FileRangeScope,
        admission: Admission,
        file: BoundFile,
    ) -> FileResult<Queued<FileStatRequest>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| FileError::new(FileErrorKind::Internal, "range service state poisoned"))?;
        if state.closed {
            return Err(FileError::cancelled("range service admission is closed"));
        }
        admission.cancellation.check()?;
        if !self.make_demand_room_locked(&mut state) {
            return Ok(Queued::Deferred(admission));
        }
        let (size_sender, size) = oneshot::channel();
        let (exit_sender, exit) = oneshot::channel();
        let Admission {
            id,
            cancellation,
            ticket,
        } = admission;
        state.requests.insert(
            id,
            RequestState {
                scope,
                class: FileRangeClass::Demand,
                paused: false,
                file,
                cancellation: cancellation.clone(),
                work: RequestWork::Stat {
                    pending: true,
                    size: None,
                    result: Some(size_sender),
                },
                active: 0,
                failed: false,
                exit_error: None,
                exit: Some(exit_sender),
                ticket: Some(ticket),
                holds: Vec::new(),
            },
        );
        state.demand.push_back(id);
        drop(state);
        self.dispatch();
        Ok(Queued::Started(FileStatRequest {
            service: Arc::clone(self),
            id,
            cancellation,
            size: Some(size),
            exit: Some(exit),
        }))
    }

    /// Makes room for demand work in the shared queue: queued speculation must
    /// not occupy the last demand slot, so queued prefetches yield from the
    /// back. False when the queue holds nothing but demand.
    fn make_demand_room_locked(&self, state: &mut State) -> bool {
        while state.demand.len() + state.prefetch.len() >= self.queue_capacity {
            let Some(evicted) = state.prefetch.pop_back() else {
                return false;
            };
            self.fail_request_locked(
                state,
                evicted,
                FileError::cancelled("queued prefetch yielded to demand"),
            );
        }
        true
    }

    pub fn close_admission(self: &Arc<Self>) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.closed = true;
        let ids: Vec<_> = state.requests.keys().copied().collect();
        for id in ids {
            self.fail_request_locked(
                &mut state,
                id,
                FileError::cancelled("range service is closing"),
            );
        }
        state.demand.clear();
        state.prefetch.clear();
        drop(state);
        self.changed.notify_waiters();
    }

    fn stop_request(self: &Arc<Self>, id: u64) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        self.fail_request_locked(
            &mut state,
            id,
            FileError::cancelled("range request stopped"),
        );
        drop(state);
        self.dispatch();
    }

    fn retain_until_exit(&self, id: u64, hold: Box<dyn std::any::Any + Send>) {
        let released = match self.state.lock() {
            Ok(mut state) => match state.requests.get_mut(&id) {
                Some(request) => {
                    request.holds.push(hold);
                    None
                }
                None => Some(hold),
            },
            Err(_) => Some(hold),
        };
        // Outside the lock: a request that already exited holds no bytes.
        drop(released);
    }

    fn set_prefetch_paused(self: &Arc<Self>, id: u64, paused: bool) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        let Some(request) = state.requests.get_mut(&id) else {
            return;
        };
        if request.class != FileRangeClass::Prefetch || request.failed || request.paused == paused {
            return;
        }
        // Segment admission is decided under this same lock. A segment that
        // already holds a window slot keeps running; queued segments wait.
        request.paused = paused;
        drop(state);
        if !paused {
            self.dispatch();
        }
    }

    /// Waits until every request ended and every segment supervisor exited.
    /// It owns nothing, so a dropped wait loses no exit.
    pub async fn drain(&self) -> FileResult<()> {
        self.wait_requests_empty(|| {}).await
    }

    async fn wait_requests_empty(&self, mut before_wait: impl FnMut()) -> FileResult<()> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .state
                .lock()
                .map_err(|_| {
                    FileError::new(FileErrorKind::Internal, "range service state poisoned")
                })?
                .requests
                .is_empty()
                && self.live_supervisors.load(Ordering::Acquire) == 0
            {
                break;
            }
            before_wait();
            notified.await;
        }
        Ok(())
    }

    fn fail_request_locked(&self, state: &mut State, id: u64, error: FileError) {
        state.demand.retain(|queued| *queued != id);
        state.prefetch.retain(|queued| *queued != id);
        self.changed.notify_waiters();
        let Some(request) = state.requests.get_mut(&id) else {
            return;
        };
        if !request.failed {
            request.cancellation.cancel();
            request.failed = true;
            request.work.fail(error);
        }
        if request.active == 0 {
            self.finish_locked(state, id);
        }
    }

    fn finish_locked(&self, state: &mut State, id: u64) {
        let Some(mut request) = state.requests.remove(&id) else {
            return;
        };
        request.work.publish();
        // Released before the exit is announced, so whoever observes the
        // exit also observes its charges returned.
        drop(std::mem::take(&mut request.holds));
        if let Some(ticket) = request.ticket.take() {
            ticket.end(match &request.exit_error {
                None => Ok(()),
                Some(error) => Err(ConnectorError::from(error)),
            });
        }
        if let Some(sender) = request.exit.take() {
            let _ = sender.send(request.exit_error.map_or(Ok(()), Err));
        }
        self.changed.notify_waiters();
    }

    fn dispatch(self: &Arc<Self>) {
        loop {
            let work = {
                let Ok(mut state) = self.state.lock() else {
                    return;
                };
                if state.closed || state.active >= self.process_window {
                    return;
                }
                let id = self.choose_locked(&mut state);
                let Some(id) = id else {
                    return;
                };
                let request = state.requests.get_mut(&id).expect("queued range request");
                let unit = request.work.next_unit();
                let work = Dispatch {
                    id,
                    scope: request.scope,
                    file: request.file.clone(),
                    cancellation: request.cancellation.child(),
                    unit,
                };
                let has_more = request.work.has_pending();
                let class = request.class;
                request.active += 1;
                state.active += 1;
                *state.source_active.entry(work.scope).or_default() += 1;
                if has_more {
                    match class {
                        FileRangeClass::Demand => state.demand.push_back(id),
                        FileRangeClass::Prefetch => state.prefetch.push_back(id),
                    }
                }
                work
            };
            self.changed.notify_waiters();
            self.spawn_segment(work);
        }
    }

    fn choose_locked(&self, state: &mut State) -> Option<u64> {
        for class in [FileRangeClass::Demand, FileRangeClass::Prefetch] {
            if class == FileRangeClass::Prefetch && self.demand_waiters.load(Ordering::SeqCst) != 0
            {
                return None;
            }
            let queue = match class {
                FileRangeClass::Demand => &state.demand,
                FileRangeClass::Prefetch => &state.prefetch,
            };
            let mut queries = Vec::new();
            for id in queue {
                let request = &state.requests[id];
                if !request.paused
                    && state
                        .source_active
                        .get(&request.scope)
                        .copied()
                        .unwrap_or(0)
                        < self.source_window
                    && !queries.contains(&request.scope.query)
                {
                    queries.push(request.scope.query);
                }
            }
            if queries.is_empty() {
                continue;
            }
            let query_index = state
                .last_query
                .and_then(|last| {
                    queries
                        .iter()
                        .position(|query| *query == last)
                        .map(|index| (index + 1) % queries.len())
                })
                .unwrap_or(0);
            let query = queries[query_index];
            let mut sources = Vec::new();
            for id in queue {
                let request = &state.requests[id];
                if !request.paused
                    && request.scope.query == query
                    && state
                        .source_active
                        .get(&request.scope)
                        .copied()
                        .unwrap_or(0)
                        < self.source_window
                    && !sources.contains(&request.scope.source)
                {
                    sources.push(request.scope.source);
                }
            }
            let source_index = state
                .last_source
                .get(&query)
                .and_then(|last| {
                    sources
                        .iter()
                        .position(|source| source == last)
                        .map(|index| (index + 1) % sources.len())
                })
                .unwrap_or(0);
            let source = sources[source_index];
            let queue = match class {
                FileRangeClass::Demand => &mut state.demand,
                FileRangeClass::Prefetch => &mut state.prefetch,
            };
            let index = queue
                .iter()
                .position(|id| {
                    let request = &state.requests[id];
                    !request.paused
                        && request.scope.query == query
                        && request.scope.source == source
                })
                .expect("eligible request");
            state.last_query = Some(query);
            state.last_source.insert(query, source);
            return queue.remove(index);
        }
        None
    }

    fn spawn_segment(self: &Arc<Self>, work: Dispatch) {
        let Dispatch {
            id,
            scope,
            file,
            cancellation,
            unit,
        } = work;
        let (sender, receiver) = oneshot::channel();
        let task = match unit {
            DispatchUnit::Segment(Segment {
                offset,
                mut bytes,
                index,
            }) => {
                let len = bytes.len() as u64;
                self.task_spawner.spawn(Box::pin(async move {
                    let result = file
                        .read_into(
                            FileReadRange::Bounded {
                                offset,
                                length: len,
                            },
                            &mut bytes,
                            &cancellation,
                        )
                        .await
                        .map(|()| UnitOutput::Segment { index, bytes });
                    let _ = sender.send(result);
                }))
            }
            DispatchUnit::Stat => self.task_spawner.spawn(Box::pin(async move {
                let result = file.stat(&cancellation).await.map(UnitOutput::Stat);
                let _ = sender.send(result);
            })),
        };
        match task {
            Ok(mut task) => {
                // Created before the spawn: a supervisor the runtime never
                // runs still releases its slot when its future is dropped.
                let mut settlement = SegmentSettlement::new(Arc::clone(self), id, scope);
                // The supervisor is the one owner of the physical task; its
                // handle is not kept, the settlement accounts for its exit.
                drop(self.scan_handle.spawn(async move {
                    let result = receiver.await.unwrap_or_else(|_| {
                        Err(FileError::new(
                            FileErrorKind::Internal,
                            "range segment exited without result",
                        ))
                    });
                    settlement.deliver(result);
                    let exit = task.drain().await;
                    settlement.settle(exit);
                }));
            }
            Err(error) => {
                self.unit_result(id, Err(error));
                self.segment_exit(id, scope, Ok(()));
            }
        }
    }

    fn unit_result(&self, id: u64, result: FileResult<UnitOutput>) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        match result {
            Ok(output) => {
                if let Some(request) = state.requests.get_mut(&id)
                    && !request.failed
                {
                    request.work.record(output);
                }
            }
            Err(error) => self.fail_request_locked(&mut state, id, error),
        }
    }

    fn segment_exit(self: &Arc<Self>, id: u64, scope: FileRangeScope, exit: FileResult<()>) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.active -= 1;
        let source_active = state
            .source_active
            .get_mut(&scope)
            .expect("active range source");
        *source_active -= 1;
        if *source_active == 0 {
            state.source_active.remove(&scope);
        }
        if let Some(request) = state.requests.get_mut(&id) {
            request.active -= 1;
            if let Err(error) = exit {
                if let Some(request) = state.requests.get_mut(&id) {
                    request
                        .exit_error
                        .get_or_insert_with(|| FileError::new(error.kind(), error.to_string()));
                }
                self.fail_request_locked(&mut state, id, error);
            }
            if state
                .requests
                .get(&id)
                .is_some_and(|request| request.active == 0 && !request.work.has_pending())
            {
                self.finish_locked(&mut state, id);
            }
        }
        drop(state);
        self.changed.notify_waiters();
        self.dispatch();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FileIdentity, FileTask, FileTaskFuture, FsAccessResolver};
    use novarocks_spi::connector::StorageAccessDomainId;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Semaphore;

    struct GateSpawner {
        permits: Arc<Semaphore>,
        started: AtomicUsize,
    }

    impl GateSpawner {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                permits: Arc::new(Semaphore::new(0)),
                started: AtomicUsize::new(0),
            })
        }
        fn release(&self, count: usize) {
            self.permits.add_permits(count);
        }
        fn started(&self) -> usize {
            self.started.load(Ordering::SeqCst)
        }
    }

    impl FileTaskSpawner for GateSpawner {
        fn spawn(&self, task: FileTaskFuture) -> FileResult<FileTask> {
            self.started.fetch_add(1, Ordering::SeqCst);
            let permits = Arc::clone(&self.permits);
            Ok(FileTask::new(tokio::spawn(async move {
                let permit = permits.acquire_owned().await.expect("gate open");
                permit.forget();
                task.await;
            })))
        }
        fn spawn_detached_blocking(&self, _job: Box<dyn FnOnce() + Send + 'static>) {
            unreachable!()
        }
    }

    fn fixture() -> (tempfile::TempDir, BoundFile) {
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("range.parquet");
        std::fs::write(&path, b"abcdefghijklmnop").expect("fixture");
        let access = FsAccessResolver::new()
            .resolve_location(
                StorageAccessDomainId::from_bytes([7; 32]),
                path.to_string_lossy(),
                None,
            )
            .expect("access");
        let file = access
            .bind(0, FileIdentity::new(path.to_string_lossy(), 16, None))
            .expect("bound file");
        (dir, file)
    }

    fn scope(query: i64, source: i64) -> FileRangeScope {
        FileRangeScope::try_new(query, 0, 1, source, 0, 1).expect("scope")
    }

    fn range(offset: u64, length: u64) -> FileReadRange {
        FileReadRange::bounded(offset, length).expect("range")
    }

    #[tokio::test]
    async fn drain_registers_wakeup_before_last_request_exits() {
        let (_dir, file) = fixture();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            GateSpawner::new(),
            Handle::current(),
        );
        service.state.lock().expect("state").requests.insert(
            7,
            RequestState {
                scope: scope(1, 1),
                class: FileRangeClass::Demand,
                paused: false,
                file,
                cancellation: FileCancellation::new(),
                work: RequestWork::Read {
                    pending: VecDeque::new(),
                    completed: Vec::new(),
                    expected_length: 0,
                    result: None,
                },
                active: 0,
                failed: false,
                exit_error: None,
                exit: None,
                ticket: None,
                holds: Vec::new(),
            },
        );
        let mut removed = false;
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            service.wait_requests_empty(|| {
                if !removed {
                    removed = true;
                    service.state.lock().expect("state").requests.remove(&7);
                    service.changed.notify_waiters();
                }
            }),
        )
        .await
        .expect("last-exit notification cannot be lost")
        .expect("drain wait");
        assert!(removed);
    }

    #[tokio::test]
    async fn dropped_waits_for_the_result_and_the_exit_can_be_resumed() {
        let (_dir, file) = fixture();
        let spawner = GateSpawner::new();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        let mut request = service
            .bind(scope(1, 1), ConnectorSourceOperations::new())
            .start(
                FileRangeClass::Demand,
                file,
                range(0, 4),
                FileCancellation::new(),
            )
            .expect("admitted");

        // A waiter gives up before the segment ran: nothing is lost.
        let gave_up =
            tokio::time::timeout(std::time::Duration::from_millis(20), request.result_ready())
                .await;
        assert!(gave_up.is_err(), "the gated segment has not run yet");
        let gave_up =
            tokio::time::timeout(std::time::Duration::from_millis(20), request.drained()).await;
        assert!(gave_up.is_err());

        spawner.release(1);
        assert_eq!(
            &request.result_ready().await.expect("the same result")[..],
            b"abcd"
        );
        request.drained().await.expect("the same exit");
        assert_eq!(spawner.started(), 1, "waiting again never re-reads");
        service.drain().await.expect("service drain");
    }

    #[tokio::test]
    async fn a_stat_waits_for_the_window_a_read_holds_and_then_reports_the_size() {
        let (_dir, file) = fixture();
        let spawner = GateSpawner::new();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(4).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        let mut read = service
            .bind(scope(1, 1), ConnectorSourceOperations::new())
            .start(
                FileRangeClass::Demand,
                file.clone(),
                range(0, 4),
                FileCancellation::new(),
            )
            .expect("read admitted");
        let mut stat = service
            .bind(scope(1, 1), ConnectorSourceOperations::new())
            .stat_wait(file, FileCancellation::new())
            .await
            .expect("stat admitted");
        assert_eq!(
            spawner.started(),
            1,
            "the stat queues behind the read holding the only slot"
        );

        spawner.release(1);
        read.result_ready().await.expect("read bytes");
        read.drained().await.expect("read exit");
        spawner.release(1);
        assert_eq!(stat.size_ready().await.expect("stat size"), 16);
        stat.drained().await.expect("stat exit");
        assert_eq!(spawner.started(), 2);
        service.drain().await.expect("service drain");
    }

    #[tokio::test]
    async fn a_stopped_queued_stat_reports_cancellation_and_exits() {
        let (_dir, file) = fixture();
        let spawner = GateSpawner::new();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(4).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        let mut read = service
            .bind(scope(1, 1), ConnectorSourceOperations::new())
            .start(
                FileRangeClass::Demand,
                file.clone(),
                range(0, 4),
                FileCancellation::new(),
            )
            .expect("read admitted");
        let FileStatStart::Started(mut stat) = service
            .bind(scope(1, 1), ConnectorSourceOperations::new())
            .try_stat(file, FileCancellation::new())
            .expect("stat admission")
        else {
            panic!("room for the stat");
        };
        stat.request_stop();
        let error = stat.size_ready().await.expect_err("stopped");
        assert_eq!(error.kind(), FileErrorKind::Cancelled);
        stat.drained()
            .await
            .expect("a stat that never ran has exited");

        spawner.release(1);
        read.result_ready().await.expect("read bytes");
        read.drained().await.expect("read exit");
        assert_eq!(spawner.started(), 1, "the stopped stat never ran");
        service.drain().await.expect("service drain");
    }

    #[tokio::test]
    async fn a_hold_outlives_its_dropped_request_until_the_physical_exit() {
        let (_dir, file) = fixture();
        let spawner = GateSpawner::new();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(4).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        let operations = ConnectorSourceOperations::new();
        let source = service.bind(scope(1, 1), operations.clone());
        let charge = Arc::new(());
        let request = source
            .start(
                FileRangeClass::Demand,
                file.clone(),
                range(0, 4),
                FileCancellation::new(),
            )
            .expect("admitted");
        request.retain_until_exit(Box::new(Arc::clone(&charge)));
        assert_eq!(spawner.started(), 1);

        drop(request);
        assert_eq!(
            Arc::strong_count(&charge),
            2,
            "the stopped read's task can still hold its bytes"
        );
        operations.seal();
        spawner.release(1);
        operations.exited().await.expect("the source exits");
        assert_eq!(
            Arc::strong_count(&charge),
            1,
            "the charge is returned by the time the exit is observed"
        );

        // A request that already exited keeps nothing it is handed.
        let operations = ConnectorSourceOperations::new();
        let source = service.bind(scope(1, 2), operations.clone());
        let mut request = source
            .start(
                FileRangeClass::Demand,
                file,
                range(4, 4),
                FileCancellation::new(),
            )
            .expect("admitted");
        spawner.release(1);
        assert_eq!(&request.result_ready().await.expect("read")[..], b"efgh");
        request.drained().await.expect("exit");
        request.retain_until_exit(Box::new(Arc::clone(&charge)));
        assert_eq!(Arc::strong_count(&charge), 1);
        drop(request);
        service.drain().await.expect("service drain");
    }

    #[tokio::test]
    async fn a_sealed_source_stops_its_request_and_exits_only_after_the_physical_task() {
        let (_dir, file) = fixture();
        let spawner = GateSpawner::new();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(4).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        let operations = ConnectorSourceOperations::new();
        let source = service.bind(scope(1, 1), operations.clone());
        let mut request = source
            .start(
                FileRangeClass::Demand,
                file.clone(),
                range(0, 4),
                FileCancellation::new(),
            )
            .expect("admitted");
        assert_eq!(spawner.started(), 1);
        assert_eq!(operations.live_operations(), 1);

        operations.seal();
        assert_eq!(
            request.result_ready().await.expect_err("stopped").kind(),
            FileErrorKind::Cancelled
        );
        match source.try_start(
            FileRangeClass::Demand,
            file.clone(),
            range(4, 4),
            FileCancellation::new(),
        ) {
            Err(error) => assert_eq!(error.kind(), FileErrorKind::Cancelled),
            Ok(_) => panic!("a sealed source admits no request"),
        }
        let refused = source
            .stat_wait(file, FileCancellation::new())
            .await
            .err()
            .expect("a sealed source admits no stat");
        assert_eq!(refused.kind(), FileErrorKind::Cancelled);

        let mut exit = operations.exited();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut exit)
                .await
                .is_err(),
            "the stopped read still holds its physical task"
        );
        assert_eq!(service.state.lock().expect("state").active, 1);

        spawner.release(1);
        exit.await
            .expect("the source exits cleanly once the task exited");
        request.drained().await.expect("request exit");
        {
            let state = service.state.lock().expect("state");
            assert_eq!(state.active, 0, "the window slot is released exactly once");
            assert!(state.source_active.is_empty());
            assert!(state.requests.is_empty());
        }
        assert_eq!(spawner.started(), 1, "a sealed source dispatches nothing");
        service.drain().await.expect("service drain");
    }

    #[tokio::test]
    async fn a_request_waiting_for_room_ends_when_its_source_is_sealed() {
        let (_dir, file) = fixture();
        let spawner = GateSpawner::new();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        // Another source holds the only slot and the only queue entry.
        let other = service.bind(scope(1, 2), ConnectorSourceOperations::new());
        let mut running = other
            .start(
                FileRangeClass::Demand,
                file.clone(),
                range(0, 4),
                FileCancellation::new(),
            )
            .expect("running");
        let mut queued = other
            .start(
                FileRangeClass::Demand,
                file.clone(),
                range(4, 4),
                FileCancellation::new(),
            )
            .expect("queued");

        let operations = ConnectorSourceOperations::new();
        let source = service.bind(scope(1, 1), operations.clone());
        let waiting = tokio::spawn({
            let file = file.clone();
            async move {
                source
                    .start_wait(file, range(8, 4), FileCancellation::new())
                    .await
                    .map(drop)
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while operations.live_operations() != 1 {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the waiting request is already the source's operation");

        operations.seal();
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .expect("the seal ends the wait for room")
            .expect("waiting task");
        assert_eq!(
            outcome.expect_err("sealed").kind(),
            FileErrorKind::Cancelled
        );
        tokio::time::timeout(std::time::Duration::from_secs(1), operations.exited())
            .await
            .expect("a wait that never queued leaves nothing behind")
            .expect("clean exit");

        spawner.release(2);
        running
            .result_ready()
            .await
            .expect("other source unaffected");
        queued
            .result_ready()
            .await
            .expect("other source unaffected");
        running.drained().await.expect("running exit");
        queued.drained().await.expect("queued exit");
        assert_eq!(spawner.started(), 2, "the refused request never ran");
        service.drain().await.expect("service drain");
    }

    #[tokio::test]
    async fn each_source_of_a_task_closes_on_its_own() {
        let (_dir, file) = fixture();
        let spawner = GateSpawner::new();
        let service = FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(4).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        // Two scan nodes of one Task: one query, two sources.
        let first_operations = ConnectorSourceOperations::new();
        let second_operations = ConnectorSourceOperations::new();
        let first = service.bind(scope(1, 1), first_operations.clone());
        let second = service.bind(scope(1, 2), second_operations.clone());
        let mut first_read = first
            .start(
                FileRangeClass::Demand,
                file.clone(),
                range(0, 4),
                FileCancellation::new(),
            )
            .expect("first source read");
        let mut second_read = second
            .start(
                FileRangeClass::Demand,
                file,
                range(4, 4),
                FileCancellation::new(),
            )
            .expect("second source read");
        assert_eq!(spawner.started(), 2);

        first_operations.seal();
        spawner.release(2);
        assert_eq!(
            first_read
                .result_ready()
                .await
                .expect_err("first source closed")
                .kind(),
            FileErrorKind::Cancelled
        );
        assert_eq!(
            &second_read
                .result_ready()
                .await
                .expect("the other source still reads")[..],
            b"efgh"
        );
        first_operations
            .exited()
            .await
            .expect("first source exited");
        second_read.drained().await.expect("second read exit");
        assert_eq!(second_operations.live_operations(), 0);
        assert!(
            !second_operations.is_exited(),
            "an idle open source has not exited"
        );
        second_operations.seal();
        second_operations
            .exited()
            .await
            .expect("second source exited");
        first_read.drained().await.expect("first read exit");
        service.drain().await.expect("service drain");
    }

    #[test]
    fn a_supervisor_its_runtime_drops_still_releases_its_slot() {
        let (_dir, file) = fixture();
        let io_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("io runtime");
        let scan_runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("scan runtime");
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            Arc::new(crate::TokioFileTaskSpawner::new(
                io_runtime.handle().clone(),
            )),
            scan_runtime.handle().clone(),
        );
        // A read that never completes holds the only process slot.
        let spawner = GateSpawner::new();
        let blocked_service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            spawner.clone(),
            scan_runtime.handle().clone(),
        );
        let _guard = io_runtime.enter();
        let operations = ConnectorSourceOperations::new();
        let mut stuck = blocked_service
            .bind(scope(1, 1), operations.clone())
            .start(
                FileRangeClass::Demand,
                file.clone(),
                range(0, 4),
                FileCancellation::new(),
            )
            .expect("admitted");
        drop(service);
        assert_eq!(blocked_service.state.lock().expect("state").active, 1);

        // The scan runtime goes away with the supervisor still waiting.
        drop(scan_runtime);
        let state = blocked_service.state.lock().expect("state");
        assert_eq!(state.active, 0, "the window slot is released exactly once");
        assert!(state.requests.is_empty());
        drop(state);
        assert_eq!(blocked_service.live_supervisors.load(Ordering::Acquire), 0);
        let outcome = io_runtime.block_on(stuck.result_ready());
        assert!(
            outcome.is_err(),
            "no result is invented for an abandoned read"
        );
        assert!(io_runtime.block_on(stuck.drained()).is_err());
        assert_eq!(operations.live_operations(), 0);
        operations.seal();
        let exit = io_runtime
            .block_on(operations.exited())
            .expect_err("the source keeps the supervisor's exit error");
        assert_eq!(
            exit.kind(),
            novarocks_spi::connector::ConnectorErrorKind::Internal
        );
        io_runtime
            .block_on(blocked_service.drain())
            .expect("nothing is left to wait for");
    }

    #[tokio::test]
    async fn prepared_middle_span_dispatches_only_two_missing_segments() {
        let (_dir, file) = fixture();
        let spawner = GateSpawner::new();
        let service = FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        let present = PreparedFileInput::new(&file, 4, BytesMut::from(&b"efgh"[..]))
            .expect("prepared middle span");
        let mut request = match service
            .bind(scope(1, 1), ConnectorSourceOperations::new())
            .try_start_with_present(
                FileRangeClass::Demand,
                file,
                range(0, 12),
                FileCancellation::new(),
                Some(present),
            )
            .expect("range request")
        {
            FileRangeStart::Started(request) => request,
            FileRangeStart::Deferred => panic!("unexpected defer"),
        };
        assert_eq!(request.partial_copy_bytes(), 4);
        assert_eq!(request.missing_bytes(), 8);
        assert_eq!(spawner.started(), 2);
        spawner.release(2);
        assert_eq!(
            request.result_ready().await.expect("filled input"),
            b"abcdefghijkl"[..]
        );
        request.drained().await.expect("actual exit");
        service.close_admission();
        service.drain().await.expect("service drain");
    }

    #[tokio::test]
    async fn prefetch_result_carries_owned_backing_capacity_and_exact_identity() {
        let (_dir, file) = fixture();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            Arc::new(crate::TokioFileTaskSpawner::new(Handle::current())),
            Handle::current(),
        );
        let mut request = service
            .bind(scope(1, 1), ConnectorSourceOperations::new())
            .start(
                FileRangeClass::Prefetch,
                file.clone(),
                range(2, 7),
                FileCancellation::new(),
            )
            .expect("prefetch");
        let input = request.prepared_input_ready().await.expect("owned input");
        assert_eq!(input.range(), 2..9);
        assert_eq!(input.retained_backing_capacity(), 7);
        assert_eq!(input.access_domain(), file.access_domain());
        assert_eq!(input.identity(), file.identity());
        request.drained().await.expect("physical exit");
        service.close_admission();
        service.drain().await.expect("service drain");
    }

    #[tokio::test]
    async fn prepared_input_rejects_other_domain_or_file_identity() {
        let (_dir, file) = fixture();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            GateSpawner::new(),
            Handle::current(),
        );
        let same_path = file.identity().path().to_owned();
        let other_domain = FsAccessResolver::new()
            .resolve_location(StorageAccessDomainId::from_bytes([8; 32]), &same_path, None)
            .expect("other access")
            .bind(0, file.identity().clone())
            .expect("other bound file");
        let other_identity = file
            .access()
            .bind(0, FileIdentity::new(&same_path, 15, None))
            .expect("other identity");
        for wrong in [other_domain, other_identity] {
            let present = PreparedFileInput::new(&wrong, 0, BytesMut::from(&b"abcd"[..]))
                .expect("prepared wrong file");
            assert!(
                service
                    .bind(scope(1, 1), ConnectorSourceOperations::new())
                    .try_start_with_present(
                        FileRangeClass::Demand,
                        file.clone(),
                        range(0, 8),
                        FileCancellation::new(),
                        Some(present),
                    )
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn failed_missing_span_never_publishes_partial_input() {
        let (dir, file) = fixture();
        let service = FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            Arc::new(crate::TokioFileTaskSpawner::new(Handle::current())),
            Handle::current(),
        );
        let present = PreparedFileInput::new(&file, 4, BytesMut::from(&b"efgh"[..]))
            .expect("prepared middle span");
        std::fs::remove_file(dir.path().join("range.parquet")).expect("remove source");
        let mut request = service
            .bind(scope(1, 1), ConnectorSourceOperations::new())
            .start_wait_with_present(file, range(0, 12), FileCancellation::new(), Some(present))
            .await
            .expect("accepted request");
        assert!(request.result_ready().await.is_err());
        request.drained().await.expect("actual exit");
        service.close_admission();
        service.drain().await.expect("service drain");
    }

    #[tokio::test]
    async fn cancelled_partial_fill_keeps_its_slot_until_physical_exit() {
        let (_dir, file) = fixture();
        let spawner = GateSpawner::new();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        let present = PreparedFileInput::new(&file, 4, BytesMut::from(&b"efgh"[..]))
            .expect("prepared middle span");
        let mut partial = match service
            .bind(scope(1, 1), ConnectorSourceOperations::new())
            .try_start_with_present(
                FileRangeClass::Demand,
                file,
                range(0, 12),
                FileCancellation::new(),
                Some(present),
            )
            .expect("partial request")
        {
            FileRangeStart::Started(request) => request,
            FileRangeStart::Deferred => panic!("unexpected defer"),
        };
        assert_eq!(spawner.started(), 1);
        partial.request_stop();
        assert!(partial.result_ready().await.is_err());
        let drain = tokio::spawn(async move { partial.drained().await });
        tokio::task::yield_now().await;
        assert!(!drain.is_finished());
        assert_eq!(spawner.started(), 1);
        spawner.release(1);
        drain.await.expect("join drain").expect("physical exit");
        service.close_admission();
        service.drain().await.expect("service drain");
    }

    #[tokio::test]
    async fn demand_overtakes_queued_prefetch_and_source_slot_waits_for_exit() {
        let (_dir, file) = fixture();
        let spawner = GateSpawner::new();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        let mut first = service
            .bind(scope(1, 1), ConnectorSourceOperations::new())
            .start(
                FileRangeClass::Demand,
                file.clone(),
                range(0, 4),
                FileCancellation::new(),
            )
            .expect("first");
        assert_eq!(spawner.started(), 1);
        let mut speculative = match service
            .bind(scope(1, 2), ConnectorSourceOperations::new())
            .try_start(
                FileRangeClass::Prefetch,
                file.clone(),
                range(4, 4),
                FileCancellation::new(),
            )
            .expect("prefetch")
        {
            FileRangeStart::Started(request) => request,
            FileRangeStart::Deferred => panic!("unexpected defer"),
        };
        let mut later_demand = service
            .bind(scope(2, 1), ConnectorSourceOperations::new())
            .start(
                FileRangeClass::Demand,
                file,
                range(8, 4),
                FileCancellation::new(),
            )
            .expect("demand");
        spawner.release(1);
        assert_eq!(
            first.result_ready().await.expect("first bytes").as_ref(),
            b"abcd"
        );
        first.drained().await.expect("first exit");
        for _ in 0..100 {
            if spawner.started() >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(spawner.started(), 2);
        spawner.release(1);
        assert_eq!(
            later_demand
                .result_ready()
                .await
                .expect("demand bytes")
                .as_ref(),
            b"ijkl"
        );
        later_demand.drained().await.expect("demand exit");
        spawner.release(1);
        assert_eq!(
            speculative
                .result_ready()
                .await
                .expect("prefetch bytes")
                .as_ref(),
            b"efgh"
        );
        speculative.drained().await.expect("prefetch exit");
        service.close_admission();
        service.drain().await.expect("service drain");
    }

    #[tokio::test]
    async fn queued_prefetch_defers_and_cancelled_active_request_keeps_slot_until_exit() {
        let (_dir, file) = fixture();
        let spawner = GateSpawner::new();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        let mut active = service
            .bind(scope(1, 1), ConnectorSourceOperations::new())
            .start(
                FileRangeClass::Demand,
                file.clone(),
                range(0, 4),
                FileCancellation::new(),
            )
            .expect("active");
        let mut waiting = service
            .bind(scope(2, 1), ConnectorSourceOperations::new())
            .start(
                FileRangeClass::Demand,
                file.clone(),
                range(4, 4),
                FileCancellation::new(),
            )
            .expect("waiting");
        assert!(matches!(
            service
                .bind(scope(3, 1), ConnectorSourceOperations::new())
                .try_start(
                    FileRangeClass::Prefetch,
                    file,
                    range(8, 4),
                    FileCancellation::new()
                )
                .expect("prefetch attempt"),
            FileRangeStart::Deferred
        ));
        active.request_stop();
        assert_eq!(
            spawner.started(),
            1,
            "cancellation has not released the physical slot"
        );
        spawner.release(1);
        assert_eq!(
            active
                .result_ready()
                .await
                .expect_err("cancelled read")
                .kind(),
            FileErrorKind::Cancelled
        );
        active.drained().await.expect("actual exit");
        for _ in 0..100 {
            if spawner.started() >= 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(spawner.started(), 2);
        spawner.release(1);
        assert_eq!(
            waiting
                .result_ready()
                .await
                .expect("waiting bytes")
                .as_ref(),
            b"efgh"
        );
        waiting.drained().await.expect("waiting exit");
        service.close_admission();
        service.drain().await.expect("service drain");
    }

    #[tokio::test]
    async fn demand_rotates_queries_then_sources() {
        let (_dir, file) = fixture();
        let spawner = GateSpawner::new();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(3).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        let mut q1s1 = service
            .bind(scope(1, 1), ConnectorSourceOperations::new())
            .start(
                FileRangeClass::Demand,
                file.clone(),
                range(0, 4),
                FileCancellation::new(),
            )
            .expect("first");
        let mut q1s1_next = service
            .bind(scope(1, 1), ConnectorSourceOperations::new())
            .start(
                FileRangeClass::Demand,
                file.clone(),
                range(4, 4),
                FileCancellation::new(),
            )
            .expect("same source");
        let mut q1s2 = service
            .bind(scope(1, 2), ConnectorSourceOperations::new())
            .start(
                FileRangeClass::Demand,
                file.clone(),
                range(8, 4),
                FileCancellation::new(),
            )
            .expect("other source");
        let mut q2s1 = service
            .bind(scope(2, 1), ConnectorSourceOperations::new())
            .start(
                FileRangeClass::Demand,
                file,
                range(12, 4),
                FileCancellation::new(),
            )
            .expect("other query");
        spawner.release(1);
        assert_eq!(
            q1s1.result_ready().await.expect("first bytes").as_ref(),
            b"abcd"
        );
        q1s1.drained().await.expect("first exit");
        spawner.release(1);
        assert_eq!(
            q2s1.result_ready().await.expect("query rotation").as_ref(),
            b"mnop"
        );
        q2s1.drained().await.expect("query exit");
        spawner.release(1);
        assert_eq!(
            q1s2.result_ready().await.expect("source rotation").as_ref(),
            b"ijkl"
        );
        q1s2.drained().await.expect("source exit");
        spawner.release(1);
        assert_eq!(
            q1s1_next
                .result_ready()
                .await
                .expect("remaining bytes")
                .as_ref(),
            b"efgh"
        );
        q1s1_next.drained().await.expect("remaining exit");
        service.close_admission();
        service.drain().await.expect("service drain");
    }

    #[tokio::test]
    async fn waiting_demand_blocks_new_prefetch_until_it_enters_queue() {
        let (_dir, file) = fixture();
        let spawner = GateSpawner::new();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        let mut first = service
            .bind(scope(1, 1), ConnectorSourceOperations::new())
            .start(
                FileRangeClass::Demand,
                file.clone(),
                range(0, 4),
                FileCancellation::new(),
            )
            .expect("active demand");
        let mut queued = service
            .bind(scope(2, 1), ConnectorSourceOperations::new())
            .start(
                FileRangeClass::Demand,
                file.clone(),
                range(4, 4),
                FileCancellation::new(),
            )
            .expect("queued demand");
        let waiting_service = Arc::clone(&service);
        let waiting_file = file.clone();
        let waiting = tokio::spawn(async move {
            waiting_service
                .bind(scope(3, 1), ConnectorSourceOperations::new())
                .start_wait(waiting_file, range(8, 4), FileCancellation::new())
                .await
        });
        for _ in 0..100 {
            if service.demand_waiters.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(service.demand_waiters.load(Ordering::SeqCst), 1);
        assert!(matches!(
            service
                .bind(scope(4, 1), ConnectorSourceOperations::new())
                .try_start(
                    FileRangeClass::Prefetch,
                    file,
                    range(12, 4),
                    FileCancellation::new()
                )
                .expect("prefetch attempt"),
            FileRangeStart::Deferred
        ));
        spawner.release(1);
        first.result_ready().await.expect("active result");
        first.drained().await.expect("active exit");
        spawner.release(1);
        queued.result_ready().await.expect("queued result");
        queued.drained().await.expect("queued exit");
        let mut admitted = waiting
            .await
            .expect("waiter task")
            .expect("waiting demand admitted");
        spawner.release(1);
        assert_eq!(
            admitted
                .result_ready()
                .await
                .expect("waiting result")
                .as_ref(),
            b"ijkl"
        );
        admitted.drained().await.expect("waiting exit");
        service.close_admission();
        service.drain().await.expect("service drain");
    }

    #[test]
    fn prefetch_submission_from_scan_cpu_does_not_need_a_caller_runtime() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("scan runtime");
        let (_dir, file) = fixture();
        let spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(crate::TokioFileTaskSpawner::new(runtime.handle().clone()));
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            spawner,
            runtime.handle().clone(),
        );
        let mut request = match service
            .bind(scope(1, 1), ConnectorSourceOperations::new())
            .try_start(
                FileRangeClass::Prefetch,
                file,
                range(0, 4),
                FileCancellation::new(),
            )
            .expect("CPU submit")
        {
            FileRangeStart::Started(request) => request,
            FileRangeStart::Deferred => panic!("free slot should accept prefetch"),
        };
        runtime.block_on(async {
            assert_eq!(
                request.result_ready().await.expect("bytes").as_ref(),
                b"abcd"
            );
            request.drained().await.expect("actual exit");
            service.close_admission();
            service.drain().await.expect("service drain");
        });
    }

    #[tokio::test]
    async fn adjacent_segments_rejoin_into_the_exact_backing() {
        use std::io::{Seek, SeekFrom, Write};
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("large.parquet");
        let mut output = std::fs::File::create(&path).expect("large fixture");
        output
            .set_len((SEGMENT_BYTES + 4) as u64)
            .expect("sparse fixture");
        output.write_all(b"head").expect("first bytes");
        output
            .seek(SeekFrom::Start(SEGMENT_BYTES as u64))
            .expect("tail position");
        output.write_all(b"tail").expect("last bytes");
        drop(output);
        let access = FsAccessResolver::new()
            .resolve_location(
                StorageAccessDomainId::from_bytes([8; 32]),
                path.to_string_lossy(),
                None,
            )
            .expect("access");
        let file = access
            .bind(
                0,
                FileIdentity::new(path.to_string_lossy(), (SEGMENT_BYTES + 4) as u64, None),
            )
            .expect("bound file");
        let spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(crate::TokioFileTaskSpawner::new(Handle::current()));
        let service = FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            spawner,
            Handle::current(),
        );
        let mut request = service
            .bind(scope(1, 1), ConnectorSourceOperations::new())
            .start(
                FileRangeClass::Demand,
                file,
                FileReadRange::WholeFile,
                FileCancellation::new(),
            )
            .expect("large range");
        let bytes = request.result_ready().await.expect("exact result");
        assert_eq!(bytes.len(), SEGMENT_BYTES + 4);
        assert_eq!(&bytes[..4], b"head");
        assert_eq!(&bytes[SEGMENT_BYTES..], b"tail");
        request.drained().await.expect("segment exits");
        service.close_admission();
        service.drain().await.expect("service drain");
    }

    #[tokio::test]
    async fn paused_prefetch_keeps_ready_segment_and_blocks_queued_segment() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("paused-large.parquet");
        std::fs::File::create(&path)
            .expect("fixture")
            .set_len((SEGMENT_BYTES + 4) as u64)
            .expect("sparse fixture");
        let access = FsAccessResolver::new()
            .resolve_location(
                StorageAccessDomainId::from_bytes([18; 32]),
                path.to_string_lossy(),
                None,
            )
            .expect("access");
        let file = access
            .bind(
                0,
                FileIdentity::new(path.to_string_lossy(), (SEGMENT_BYTES + 4) as u64, None),
            )
            .expect("bound file");
        let spawner = GateSpawner::new();
        let service = FileRangeService::new(
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        let mut prefetch = service
            .bind(scope(1, 1), ConnectorSourceOperations::new())
            .start(
                FileRangeClass::Prefetch,
                file.clone(),
                FileReadRange::WholeFile,
                FileCancellation::new(),
            )
            .expect("prefetch");
        assert_eq!(spawner.started(), 1);
        let control = prefetch.control();
        control.request_pause();
        spawner.release(1);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let changed = service.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if service.state.lock().expect("state").active == 0 {
                    break;
                }
                changed.await;
            }
        })
        .await
        .expect("first segment exits");
        assert_eq!(spawner.started(), 1, "queued segment remains paused");
        let mut demand = service
            .bind(scope(2, 1), ConnectorSourceOperations::new())
            .start(
                FileRangeClass::Demand,
                file,
                range(0, 4),
                FileCancellation::new(),
            )
            .expect("demand passes paused prefetch");
        assert_eq!(spawner.started(), 2);
        spawner.release(1);
        assert_eq!(demand.result_ready().await.expect("demand bytes").len(), 4);
        demand.drained().await.expect("demand exit");
        assert_eq!(spawner.started(), 2);
        control.request_resume();
        assert_eq!(spawner.started(), 3);
        spawner.release(1);
        assert_eq!(
            prefetch
                .result_ready()
                .await
                .expect("completed prefetch")
                .len(),
            SEGMENT_BYTES + 4
        );
        prefetch.drained().await.expect("prefetch exit");
        service.close_admission();
        service.drain().await.expect("service drain");
    }

    struct FailFirstSpawner {
        gate: Arc<Semaphore>,
        count: AtomicUsize,
    }

    impl FileTaskSpawner for FailFirstSpawner {
        fn spawn(&self, task: FileTaskFuture) -> FileResult<FileTask> {
            let first = self.count.fetch_add(1, Ordering::SeqCst) == 0;
            let gate = Arc::clone(&self.gate);
            Ok(FileTask::new(tokio::spawn(async move {
                if first {
                    drop(task);
                    return;
                }
                gate.acquire_owned().await.expect("gate").forget();
                task.await;
            })))
        }
        fn spawn_detached_blocking(&self, _job: Box<dyn FnOnce() + Send + 'static>) {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn first_segment_error_is_reported_before_held_sibling_exits() {
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("large.parquet");
        std::fs::File::create(&path)
            .expect("fixture")
            .set_len((SEGMENT_BYTES + 4) as u64)
            .expect("sparse fixture");
        let access = FsAccessResolver::new()
            .resolve_location(
                StorageAccessDomainId::from_bytes([8; 32]),
                path.to_string_lossy(),
                None,
            )
            .expect("access");
        let file = access
            .bind(
                0,
                FileIdentity::new(path.to_string_lossy(), (SEGMENT_BYTES + 4) as u64, None),
            )
            .expect("bound file");
        let spawner = Arc::new(FailFirstSpawner {
            gate: Arc::new(Semaphore::new(0)),
            count: AtomicUsize::new(0),
        });
        let service = FileRangeService::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            NonZeroUsize::new(2).unwrap(),
            spawner.clone(),
            Handle::current(),
        );
        let operations = ConnectorSourceOperations::new();
        let mut request = service
            .bind(scope(1, 1), operations.clone())
            .start(
                FileRangeClass::Demand,
                file,
                FileReadRange::WholeFile,
                FileCancellation::new(),
            )
            .expect("large range");
        assert_eq!(
            request
                .result_ready()
                .await
                .expect_err("first error")
                .kind(),
            FileErrorKind::Internal
        );
        assert_eq!(spawner.count.load(Ordering::SeqCst), 2);
        assert_eq!(
            service.state.lock().unwrap().active,
            1,
            "held sibling still owns its physical slot"
        );
        // Closing the source after the error does not end it early: the
        // held sibling is still the source's physical work.
        operations.seal();
        assert!(!operations.is_exited());
        spawner.gate.add_permits(1);
        request.drained().await.expect("sibling exit");
        operations
            .exited()
            .await
            .expect("a result error is not an exit error");
        service.close_admission();
        service.drain().await.expect("service drain");
    }
}
