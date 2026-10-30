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

//! One original scalar observation. The caller drives existing catalog loading
//! and raw FileIO. Manifest decode, live-file projection and grouping use the existing
//! closed metadata CPU source and the rewrite author's same pure algorithm.
use super::*;
use crate::catalog_control::statistics::scoped_cpu::IcebergMetadataCpuOriginal;
use crate::catalog_control::statistics::scoped_cpu::scoped_snapshot::raw_read_failure;
use novarocks_spi::connector::{
    ConnectorMaxCompactableDataFilesSource, ProviderMetadataCpuInput, ProviderMetadataCpuPort,
};

const IMMUTABLE_SIZE_CHANGED: &str = "immutable compaction manifest size changed during read";

type Scalar = Result<ConnectorMaxCompactableDataFiles, ConnectorError>;
type IoFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

pub(crate) enum RawRead {
    Table {
        namespace: String,
        table: String,
    },
    Bytes {
        io: crate::iceberg::io::FileIO,
        path: String,
    },
}
pub(crate) enum RawResult {
    Table(Result<crate::loaded_table::IcebergPhysicalTable, ConnectorError>),
    Bytes(Result<Bytes, ConnectorError>),
}
pub(crate) enum Next {
    Read(RawRead),
    Complete(Scalar),
}
enum Phase {
    Prepare,
    Table,
    List,
    Manifest,
}

pub(crate) struct CompactionWork {
    engine: Arc<MaintenanceEngine>,
    request: ConnectorMaxCompactableDataFilesRequest,
    phase: Phase,
    loaded: Option<Result<crate::loaded_table::IcebergPhysicalTable, ConnectorError>>,
    raw: Option<Result<Bytes, ConnectorError>>,
    manifests: Option<crate::iceberg::spec::ManifestList>,
    index: usize,
    live: crate::commit::LiveFiles,
    preserve_row_lineage: bool,
    next: Option<RawRead>,
    result: Option<Scalar>,
}
impl CompactionWork {
    fn new(
        engine: Arc<MaintenanceEngine>,
        request: ConnectorMaxCompactableDataFilesRequest,
    ) -> Self {
        Self {
            engine,
            request,
            phase: Phase::Prepare,
            loaded: None,
            raw: None,
            manifests: None,
            index: 0,
            live: Default::default(),
            preserve_row_lineage: false,
            next: None,
            result: None,
        }
    }
    pub(crate) fn receive(&mut self, raw: RawResult) {
        // Preserve the first actual raw error before a later stop or CPU
        // admission refusal. Raw I/O still has to exit before it is handed off.
        let failure = match &raw {
            RawResult::Table(value) => value.as_ref().err(),
            RawResult::Bytes(value) => value.as_ref().err(),
        };
        if let Some(failure) = failure {
            self.result = Some(Err(failure.clone()));
        }
        match raw {
            RawResult::Table(value) => {
                assert!(self.loaded.is_none(), "one original table receiver");
                self.loaded = Some(value);
            }
            RawResult::Bytes(value) => {
                assert!(self.raw.is_none(), "one original raw metadata receiver");
                self.raw = Some(value);
            }
        }
    }
    pub(crate) fn received_result(&self) -> Option<Scalar> {
        self.result.clone()
    }
    pub(crate) fn next(&mut self) -> Next {
        if let Some(result) = &self.result {
            return Next::Complete(result.clone());
        }
        Next::Read(
            self.next
                .take()
                .expect("original CPU phase requested raw I/O"),
        )
    }
    pub(crate) fn run(&mut self) {
        if let Err(error) = self.run_phase() {
            self.result = Some(Err(error));
        }
    }
    fn run_phase(&mut self) -> Result<(), ConnectorError> {
        self.request.context.check_active()?;
        match self.phase {
            Phase::Prepare => {
                let (namespace, table) = decode_base_table_target(&self.request.table)?;
                // Preserve the observation's existing fresh-current semantics.
                self.engine
                    .runtime
                    .control_state()
                    .invalidate_table_cache(&namespace, &table);
                self.phase = Phase::Table;
                self.next = Some(RawRead::Table { namespace, table });
                return Ok(());
            }
            Phase::Table => {
                let physical = self
                    .loaded
                    .as_ref()
                    .expect("original table result")
                    .as_ref()
                    .map_err(Clone::clone)?;
                self.preserve_row_lineage =
                    crate::schema_facts::row_lineage_enabled(physical.table.metadata());
                let Some(snapshot) = physical.table.metadata().current_snapshot() else {
                    self.result = Some(Ok(ConnectorMaxCompactableDataFiles::new(Some(0))));
                    return Ok(());
                };
                self.next = Some(RawRead::Bytes {
                    io: physical.table.file_io().clone(),
                    path: snapshot.manifest_list().to_owned(),
                });
                self.phase = Phase::List;
                return Ok(());
            }
            Phase::List => {
                let bytes = self
                    .raw
                    .as_ref()
                    .expect("original manifest-list bytes")
                    .as_ref()
                    .map_err(Clone::clone)?;
                let physical = self
                    .loaded
                    .as_ref()
                    .unwrap()
                    .as_ref()
                    .map_err(Clone::clone)?;
                self.manifests = Some(
                    crate::manifest::decode_manifest_list(bytes, physical.table.metadata())
                        .map_err(raw_read_failure)?,
                );
                self.phase = Phase::Manifest;
            }
            Phase::Manifest => {
                let bytes = self
                    .raw
                    .as_ref()
                    .expect("original manifest bytes")
                    .as_ref()
                    .map_err(Clone::clone)?;
                let descriptor = &self.manifests.as_ref().unwrap().entries()[self.index];
                let manifest = crate::manifest::decode_manifest(bytes, descriptor)
                    .map_err(raw_read_failure)?;
                crate::commit::append_live_manifest(&mut self.live, descriptor, &manifest);
                self.index += 1;
            }
        }
        // Parsing has installed its facts before the old raw block is retired.
        drop(self.raw.take());
        let list = self.manifests.as_ref().expect("same manifest list");
        if let Some(descriptor) = list.entries().get(self.index) {
            let physical = self
                .loaded
                .as_ref()
                .unwrap()
                .as_ref()
                .map_err(Clone::clone)?;
            self.next = Some(RawRead::Bytes {
                io: physical.table.file_io().clone(),
                path: descriptor.manifest_path.clone(),
            });
        } else {
            let stats =
                crate::commit::compactable_file_groups(&self.live, self.preserve_row_lineage)
                    .map_err(raw_read_failure)?;
            let count = u64::try_from(stats.max_compactable_data_files)
                .map_err(|_| internal("compactable file count overflow"))?;
            self.result = Some(Ok(ConnectorMaxCompactableDataFiles::new(Some(count))));
        }
        Ok(())
    }
}

struct Body {
    input: Option<ProviderMetadataCpuInput>,
    submitted: bool,
    io: Option<IoFuture>,
    in_foreign: bool,
    result: Option<Scalar>,
    closed: Option<Result<(), ConnectorError>>,
}
pub(crate) struct ObservationSource {
    engine: Arc<MaintenanceEngine>,
    context: ConnectorRequestContext,
    cpu: Arc<dyn ProviderMetadataCpuPort>,
    original: Arc<IcebergMetadataCpuOriginal>,
    weak: Weak<Self>,
    body: Mutex<Option<Body>>,
}
struct Loan<'a> {
    source: &'a ObservationSource,
    body: Option<Body>,
}
impl<'a> Loan<'a> {
    fn take(source: &'a ObservationSource) -> Self {
        let body = source.body.lock().unwrap_or_else(|p| p.into_inner()).take();
        assert!(body.is_some(), "one original compaction observation loan");
        Self { source, body }
    }
    fn body(&mut self) -> &mut Body {
        self.body.as_mut().unwrap()
    }
}
impl Drop for Loan<'_> {
    fn drop(&mut self) {
        let mut slot = self.source.body.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            slot.is_none(),
            "same original compaction observation receiver"
        );
        *slot = self.body.take();
    }
}
impl ObservationSource {
    pub(crate) fn new(
        engine: Arc<MaintenanceEngine>,
        request: ConnectorMaxCompactableDataFilesRequest,
        cpu: Arc<dyn ProviderMetadataCpuPort>,
    ) -> Arc<Self> {
        let original = IcebergMetadataCpuOriginal::compaction_source(
            engine.descriptor.clone(),
            engine.key.incarnation,
            CompactionWork::new(engine.clone(), request.clone()),
        );
        Arc::new_cyclic(|weak| Self {
            engine,
            context: request.context,
            cpu,
            original,
            weak: weak.clone(),
            body: Mutex::new(Some(Body {
                input: None,
                submitted: false,
                io: None,
                in_foreign: false,
                result: None,
                closed: None,
            })),
        })
    }
    fn receive(&self, raw: RawResult) {
        self.original.compaction_received(raw);
    }
}
impl ConnectorMaxCompactableDataFilesSource for ObservationSource {
    fn poll_result(&self, cx: &mut Context<'_>) -> Poll<Scalar> {
        let mut loan = Loan::take(self);
        let body = loan.body();
        if let Some(result) = &body.result {
            return Poll::Ready(result.clone());
        }
        // A foreign poll/Drop panic leaves the exact F and input in this header.
        // The outer original owner catches the panic and retains this Source.
        if body.in_foreign {
            return Poll::Pending;
        }
        loop {
            if let Some(io) = body.io.as_mut() {
                body.in_foreign = true;
                if io.as_mut().poll(cx).is_pending() {
                    body.in_foreign = false;
                    return Poll::Pending;
                }
                // run_io already received the actual raw result before child Drop.
                drop(body.io.take());
                body.in_foreign = false;
            }
            if !body.submitted {
                if let Some(result) = self.original.compaction_received_result() {
                    body.result = Some(result);
                    return Poll::Ready(body.result.as_ref().unwrap().clone());
                }
                if body.input.is_none() {
                    body.input = Some(ProviderMetadataCpuInput(self.original.clone()));
                }
                body.in_foreign = true;
                let submitted = self.cpu.submit_metadata_in(&mut body.input);
                body.in_foreign = false;
                if let Err(error) = submitted {
                    body.result = Some(Err(error));
                    return Poll::Ready(body.result.as_ref().unwrap().clone());
                }
                body.submitted = true;
            }
            body.in_foreign = true;
            let finished = self.cpu.poll_finished(cx);
            body.in_foreign = false;
            match finished {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => {
                    body.result = Some(Err(error));
                    return Poll::Ready(body.result.as_ref().unwrap().clone());
                }
                Poll::Ready(Ok(())) => body.submitted = false,
            }
            // The original CPU job/footer has exited before any real I/O poll.
            match self.original.compaction_next() {
                Next::Complete(result) => {
                    body.result = Some(result);
                    return Poll::Ready(body.result.as_ref().unwrap().clone());
                }
                Next::Read(request) => {
                    body.io = Some(Box::pin(run_io(
                        self.weak.clone(),
                        self.engine.clone(),
                        self.context.clone(),
                        request,
                    )));
                }
            }
        }
    }
    fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        if self.poll_result(cx).is_pending() {
            return Poll::Pending;
        }
        let mut loan = Loan::take(self);
        let body = loan.body();
        if let Some(closed) = &body.closed {
            return Poll::Ready(closed.clone());
        }
        if body.in_foreign {
            return Poll::Pending;
        }
        body.in_foreign = true;
        let closed = self.cpu.poll_closed(cx);
        body.in_foreign = false;
        match closed {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                body.closed = Some(result.clone());
                Poll::Ready(result)
            }
        }
    }
    fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
        self.cpu.local_failure()
    }
}

async fn run_io(
    receiving: Weak<ObservationSource>,
    engine: Arc<MaintenanceEngine>,
    context: ConnectorRequestContext,
    request: RawRead,
) {
    match request {
        RawRead::Table { namespace, table } => {
            let mut original = std::pin::pin!(
                engine
                    .runtime
                    .load_table_original_for_request_async(&namespace, &table, &context,)
            );
            std::future::poll_fn(|cx| match original.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(raw) => {
                    receiving
                        .upgrade()
                        .expect("stable observation owner")
                        .receive(RawResult::Table(raw));
                    Poll::Ready(())
                }
            })
            .await;
        }
        RawRead::Bytes { io, path } => {
            let input = match io.new_input(&path) {
                Ok(input) => input,
                Err(error) => {
                    receiving
                        .upgrade()
                        .expect("stable observation owner")
                        .receive(RawResult::Bytes(Err(raw_read_failure(error))));
                    return;
                }
            };
            {
                let mut original = std::pin::pin!(async {
                    context.check_active()?;
                    let metadata = input.metadata().await.map_err(raw_read_failure)?;
                    if let Some(size) = std::num::NonZeroU64::new(metadata.size) {
                        context.reserve_owned_metadata(size).await?;
                    }
                    let bytes = input.read().await.map_err(raw_read_failure)?;
                    if bytes.len() as u64 != metadata.size {
                        return Err(ConnectorError::new(
                            ConnectorErrorKind::CorruptData,
                            IMMUTABLE_SIZE_CHANGED,
                        ));
                    }
                    Ok(bytes)
                });
                std::future::poll_fn(|cx| match original.as_mut().poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(raw) => {
                        receiving
                            .upgrade()
                            .expect("stable observation owner")
                            .receive(RawResult::Bytes(raw));
                        Poll::Ready(())
                    }
                })
                .await;
            }
            drop(input);
        }
    }
}

/// Fixed backing of this exact Source/CPU header and its single owned I/O F.
/// Inputs and manifest graphs retain their original observed runtime backing.
pub(super) fn fixed_backing_bytes() -> Option<usize> {
    use novarocks_type_contract::owned_resources::layout::arc_layout;
    fn future_size<A, B, C, D, F>(_: fn(A, B, C, D) -> F) -> usize {
        std::mem::size_of::<F>()
    }
    arc_layout(std::alloc::Layout::new::<ObservationSource>())
        .ok()?
        .size()
        .checked_add(
            arc_layout(std::alloc::Layout::new::<IcebergMetadataCpuOriginal>())
                .ok()?
                .size(),
        )?
        .checked_add(future_size(run_io))?
        // Raw receiver, work result, stable scalar and one outward result share
        // one cause record, but each ConnectorError owns its safe message.
        .checked_add(
            crate::catalog_control::statistics::scoped_cpu::scoped_snapshot::raw_read_failure_backing_bytes(3)
                .ok()?
                .max(IMMUTABLE_SIZE_CHANGED.len().checked_mul(4)?),
        )?
        .checked_add(if cfg!(target_os = "macos") { 2 * 64 } else { 0 })
}
