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

//! One Iceberg data split as a page stream the host driver polls.
//!
//! The stream runs the split's reader logic -- footer checkpoint, run
//! planning against the live filter, delete and predicate judgment, page
//! assembly -- with every read awaited through the split's range service and
//! the decoder run on the polling driver. Creating it reads nothing: the
//! delete state, footer and readers are opened in the first poll.
//!
//! Each step moves the split state into one future and gets it back when the
//! step ends, so between steps the host can advance successor preparation and
//! read metrics. The split's reads are admitted to its own child of the task
//! source; closing the stream seals that child, stops the successor and any
//! promoted preparation, and returns a future that only observes their exit.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;
use futures::future::BoxFuture;
use novarocks_fs::FileReadContext;
use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::{
    BudgetConsume, ConnectorPageStream, ConnectorPollBudget, ConnectorPreparationControl,
    ConnectorPreparationProgress, ConnectorSourceOperations, OwnedConnectorPageStream,
    PageSourceMetrics, SourcePage,
};

use super::super::delete_manager::DeleteEvaluationMode;
use super::super::preparation::SuccessorPreparationGroup;
use super::{
    AdmittedSplit, IcebergPageSourceRequest, IcebergParquetPageSource,
    IcebergPartitionOnlyPageSource, ParquetSplitRequest, admit_split, partition_only_source,
};

/// Build the page stream for one Iceberg data split, polled with `budget`.
///
/// Admission, the partition-only fast path and the split's child operations
/// are decided here without I/O; everything the split reads is opened in the
/// stream's first poll.
pub fn create_iceberg_page_stream(
    request: IcebergPageSourceRequest<'_>,
    budget: &ConnectorPollBudget,
) -> Result<OwnedConnectorPageStream, ConnectorError> {
    let admitted = admit_split(&request)?;
    if let Some(fast_path) = partition_only_source(&request, &admitted)? {
        return Ok(Box::pin(IcebergPartitionOnlyPageStream {
            source: fast_path,
            budget: budget.clone(),
            spending: None,
            pending_preparation_control: request.pending_preparation_control,
        }));
    }
    let (mut request, delete_mode) = ParquetSplitRequest::of(request);
    let operations = SplitOperations::bind(&mut request.context)?;
    let successor_control = Arc::new(SuccessorPreparationGroup::new());
    // A promoted split holds its prepared input from here on.
    let retained_base_bytes = request.retained_base_bytes();
    Ok(Box::pin(IcebergParquetPageStream {
        pending_preparation_control: request.pending_preparation_control.clone(),
        step: StreamStep::Unopened(Box::new(ParquetStreamInit {
            request,
            delete_mode,
            admitted,
            successor_control: Arc::clone(&successor_control),
        })),
        budget: budget.clone(),
        operations,
        successor_control,
        metrics: PageSourceMetrics::default(),
        retained_base_bytes,
    }))
}

/// What the first poll opens the split from.
struct ParquetStreamInit {
    request: ParquetSplitRequest,
    delete_mode: DeleteEvaluationMode,
    admitted: AdmittedSplit,
    successor_control: Arc<SuccessorPreparationGroup>,
}

impl ParquetStreamInit {
    async fn open(self) -> Result<Box<IcebergParquetPageSource>, ConnectorError> {
        let delete_filter = self
            .request
            .delete_manager
            .open_split(
                &self.request.split,
                &self.admitted.table_schema,
                self.delete_mode,
            )
            .await?;
        Ok(Box::new(self.request.into_source(
            self.admitted,
            delete_filter,
            self.successor_control,
        )))
    }
}

type StepOutcome = (
    Option<Box<IcebergParquetPageSource>>,
    Result<Option<SourcePage>, ConnectorError>,
);

enum StreamStep {
    /// Created; nothing is open yet.
    Unopened(Box<ParquetStreamInit>),
    /// Between steps: the host may advance successors and read metrics.
    Idle(Box<IcebergParquetPageSource>),
    /// One step in flight, holding the split until it ends.
    Running(BoxFuture<'static, StepOutcome>),
    /// Opening failed or the stream ended without a split to keep.
    Ended,
}

/// One Iceberg data split read as a page stream.
pub struct IcebergParquetPageStream {
    step: StreamStep,
    budget: ConnectorPollBudget,
    operations: SplitOperations,
    successor_control: Arc<SuccessorPreparationGroup>,
    pending_preparation_control: Option<Arc<dyn ConnectorPreparationControl>>,
    /// As of the last completed step.
    metrics: PageSourceMetrics,
    /// The split's own retention as of the last completed step; successor
    /// and promoted preparation are read live.
    retained_base_bytes: u64,
}

impl IcebergParquetPageStream {
    fn observe(&mut self, source: &IcebergParquetPageSource) {
        self.metrics = source.metrics();
        self.retained_base_bytes = source.retained_base_bytes();
    }
}

impl Stream for IcebergParquetPageStream {
    type Item = Result<SourcePage, ConnectorError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match std::mem::replace(&mut this.step, StreamStep::Ended) {
                StreamStep::Unopened(init) => {
                    let budget = this.budget.clone();
                    this.step = StreamStep::Running(Box::pin(async move {
                        match init.open().await {
                            Ok(mut source) => {
                                let page = source.produce_page_async(&budget).await;
                                (Some(source), page)
                            }
                            Err(error) => (None, Err(error)),
                        }
                    }));
                }
                StreamStep::Idle(mut source) => {
                    if source.finished {
                        this.step = StreamStep::Idle(source);
                        return Poll::Ready(None);
                    }
                    let budget = this.budget.clone();
                    this.step = StreamStep::Running(Box::pin(async move {
                        let page = source.produce_page_async(&budget).await;
                        (Some(source), page)
                    }));
                }
                StreamStep::Running(mut step) => match step.as_mut().poll(cx) {
                    Poll::Pending => {
                        this.step = StreamStep::Running(step);
                        return Poll::Pending;
                    }
                    Poll::Ready((source, outcome)) => {
                        if let Some(source) = source {
                            this.observe(&source);
                            this.step = StreamStep::Idle(source);
                        }
                        return Poll::Ready(outcome.transpose());
                    }
                },
                StreamStep::Ended => return Poll::Ready(None),
            }
        }
    }
}

impl ConnectorPageStream for IcebergParquetPageStream {
    fn metrics(&self) -> PageSourceMetrics {
        self.metrics
    }

    fn memory_usage_bytes(&self) -> u64 {
        self.retained_base_bytes
            .saturating_add(self.successor_control.retained_input_bytes())
            .saturating_add(
                self.pending_preparation_control
                    .as_ref()
                    .map_or(0, |control| control.retained_input_bytes()),
            )
    }

    fn advance_successor_preparation(
        self: Pin<&mut Self>,
        remaining_input_bytes: u64,
        remaining_candidates: usize,
    ) -> Result<ConnectorPreparationProgress, ConnectorError> {
        // Successors are planned from the split state, which only the
        // stream holds between steps; a step in flight defers them.
        match &mut self.get_mut().step {
            StreamStep::Idle(source) => {
                source.advance_successor_preparation(remaining_input_bytes, remaining_candidates)
            }
            _ => Ok(ConnectorPreparationProgress::Deferred),
        }
    }

    fn successor_preparation_input_bytes(&self) -> u64 {
        self.successor_control.retained_input_bytes()
    }

    fn successor_preparation_candidate_count(&self) -> usize {
        match &self.step {
            StreamStep::Idle(source) => source.successor_preparation_candidate_count(),
            _ => self.successor_control.undrained_candidate_count(),
        }
    }

    fn successor_preparation_control(&self) -> Option<Arc<dyn ConnectorPreparationControl>> {
        Some(Arc::clone(&self.successor_control) as Arc<dyn ConnectorPreparationControl>)
    }

    fn close(self: Pin<Box<Self>>) -> BoxFuture<'static, Result<(), ConnectorError>> {
        let this = *Pin::into_inner(self);
        let reader = match this.step {
            // Its reader is dropped here; no read is left in flight.
            StreamStep::Idle(mut source) => source.close_reader(),
            // Dropping the step stops its reads; their exit is observed below.
            StreamStep::Running(step) => {
                drop(step);
                Ok(())
            }
            StreamStep::Unopened(_) | StreamStep::Ended => Ok(()),
        };
        let exited = this.operations.seal();
        this.successor_control.request_stop();
        if let Some(control) = &this.pending_preparation_control {
            control.request_stop();
        }
        let successor_control = this.successor_control;
        let pending = this.pending_preparation_control;
        Box::pin(async move {
            successor_control.wait_drained().await;
            if let Some(control) = &pending {
                control.wait_drained().await;
            }
            let exited = exited.await;
            reader?;
            exited
        })
    }
}

/// A split that needs no byte of its data file, as a page stream: every page
/// is constants, and each spends one unit of the host's turn budget.
pub struct IcebergPartitionOnlyPageStream {
    source: IcebergPartitionOnlyPageSource,
    budget: ConnectorPollBudget,
    spending: Option<BudgetConsume>,
    pending_preparation_control: Option<Arc<dyn ConnectorPreparationControl>>,
}

impl Stream for IcebergPartitionOnlyPageStream {
    type Item = Result<SourcePage, ConnectorError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.source.is_exhausted() {
            return Poll::Ready(None);
        }
        let spending = this.spending.get_or_insert_with(|| this.budget.consume(1));
        if Pin::new(spending).poll(cx).is_pending() {
            return Poll::Pending;
        }
        this.spending = None;
        Poll::Ready(this.source.next_page().transpose())
    }
}

impl ConnectorPageStream for IcebergPartitionOnlyPageStream {
    fn metrics(&self) -> PageSourceMetrics {
        self.source.metrics()
    }

    fn memory_usage_bytes(&self) -> u64 {
        self.source.retained_bytes().saturating_add(
            self.pending_preparation_control
                .as_ref()
                .map_or(0, |control| control.retained_input_bytes()),
        )
    }

    fn close(self: Pin<Box<Self>>) -> BoxFuture<'static, Result<(), ConnectorError>> {
        // Nothing of the split is read; only a promoted preparation can still
        // hold input, and its exit is what the close observes.
        let pending = Pin::into_inner(self).pending_preparation_control;
        if let Some(control) = &pending {
            control.request_stop();
        }
        Box::pin(async move {
            if let Some(control) = &pending {
                control.wait_drained().await;
            }
            Ok(())
        })
    }
}

/// A split's own child of its task source's operations: closing the split
/// stops and observes exactly the reads it admitted, and leaves the task
/// source open for its other splits.
pub(crate) struct SplitOperations(Option<ConnectorSourceOperations>);

impl SplitOperations {
    /// Binds `context`'s reads to a new child of their source. A read without
    /// a range service has no operations to bind.
    pub(crate) fn bind(context: &mut FileReadContext) -> Result<Self, ConnectorError> {
        let Some(range) = &context.range else {
            return Ok(Self(None));
        };
        let operations = range.operations().child()?;
        context.range = Some(range.with_operations(operations.clone()));
        Ok(Self(Some(operations)))
    }

    /// Seals the split's operations, stopping each one, and returns a future
    /// that only observes their exit.
    pub(crate) fn seal(&self) -> impl Future<Output = Result<(), ConnectorError>> + Send + 'static {
        let exited = self.0.as_ref().map(|operations| {
            operations.seal();
            operations.exited()
        });
        async move {
            match exited {
                Some(exited) => exited.await,
                None => Ok(()),
            }
        }
    }
}

/// Rows a split holds in memory once it is loaded, handed out a page at a
/// time. Nothing here reads: every byte was loaded before the first page.
pub(crate) trait MaterializedPages: Send + Unpin + 'static {
    fn next_page(&mut self) -> Result<Option<SourcePage>, ConnectorError>;

    fn is_exhausted(&self) -> bool;

    fn metrics(&self) -> PageSourceMetrics;

    fn memory_usage_bytes(&self) -> u64;
}

/// A split whose rows are all materialized by one awaited load, then handed
/// out a page at a time; each page spends one unit of the host's turn budget.
///
/// Creating it reads nothing: the load runs in the first poll. Closing it
/// drops an unfinished load, seals the split's operations and returns a
/// future that observes their exit.
pub(crate) struct MaterializedPageStream<M> {
    step: MaterializedStep<M>,
    budget: ConnectorPollBudget,
    spending: Option<BudgetConsume>,
    operations: SplitOperations,
}

enum MaterializedStep<M> {
    Loading(BoxFuture<'static, Result<M, ConnectorError>>),
    Ready(M),
    /// The load failed; its error was the stream's last item.
    Ended,
}

impl<M: MaterializedPages> MaterializedPageStream<M> {
    /// `load` performs every read of the split through a context bound to
    /// `operations`.
    pub(crate) fn new(
        load: BoxFuture<'static, Result<M, ConnectorError>>,
        operations: SplitOperations,
        budget: &ConnectorPollBudget,
    ) -> Self {
        Self {
            step: MaterializedStep::Loading(load),
            budget: budget.clone(),
            spending: None,
            operations,
        }
    }
}

impl<M: MaterializedPages> Stream for MaterializedPageStream<M> {
    type Item = Result<SourcePage, ConnectorError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match &mut this.step {
                MaterializedStep::Loading(load) => match load.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(pages)) => this.step = MaterializedStep::Ready(pages),
                    Poll::Ready(Err(error)) => {
                        this.step = MaterializedStep::Ended;
                        return Poll::Ready(Some(Err(error)));
                    }
                },
                MaterializedStep::Ready(pages) => {
                    if pages.is_exhausted() {
                        return Poll::Ready(None);
                    }
                    let spending = this.spending.get_or_insert_with(|| this.budget.consume(1));
                    if Pin::new(spending).poll(cx).is_pending() {
                        return Poll::Pending;
                    }
                    this.spending = None;
                    return Poll::Ready(pages.next_page().transpose());
                }
                MaterializedStep::Ended => return Poll::Ready(None),
            }
        }
    }
}

impl<M: MaterializedPages> ConnectorPageStream for MaterializedPageStream<M> {
    fn metrics(&self) -> PageSourceMetrics {
        match &self.step {
            MaterializedStep::Ready(pages) => pages.metrics(),
            MaterializedStep::Loading(_) | MaterializedStep::Ended => PageSourceMetrics::default(),
        }
    }

    fn memory_usage_bytes(&self) -> u64 {
        match &self.step {
            MaterializedStep::Ready(pages) => pages.memory_usage_bytes(),
            MaterializedStep::Loading(_) | MaterializedStep::Ended => 0,
        }
    }

    fn close(self: Pin<Box<Self>>) -> BoxFuture<'static, Result<(), ConnectorError>> {
        let Self {
            step, operations, ..
        } = *Pin::into_inner(self);
        // Dropping an unfinished load stops its reads; their exit is
        // observed through the sealed operations.
        drop(step);
        Box::pin(operations.seal())
    }
}
