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
use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use futures::future::BoxFuture;

use crate::exec::chunk::Chunk;
use crate::runtime::profile::RuntimeProfile;
use novarocks_spi::connector::read_stack::ConnectorPollBudget;

/// Which work the independently held exit observation actually covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScanExitBodyProof {
    /// The observer covers every work owner; the stream and its close future
    /// retain only control metadata. The implementation must justify this.
    Included,
    /// The stream or close future holds additional reader/data owners. Their
    /// normal close completion and successful destruction are also required.
    CloseBodyRequired,
}

/// Pure proof held independently from the stream and its close body.
///
/// This future observes the original work scope. It must register the supplied
/// waker against that scope with no lost wake, and must not cancel its work on
/// drop. Authored Required events are reported separately, even before close.
/// An unresolved proof never permits dropping a receiver with live event sources.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanExitProof {
    Exited,
    Unresolved,
}
pub struct ScanExitObservation {
    pub actual_exit: BoxFuture<'static, ScanExitProof>,
    pub body_proof: ScanExitBodyProof,
}

/// A scan's single output stream, owned by the one driver that polls it.
///
/// `Poll::Pending` registers the context's waker and is never end of
/// stream; neither is an empty chunk. `Ready(None)` means no more chunks.
/// After an error the driver polls the stream no more.
pub trait ScanChunkStream:
    Stream<Item = crate::runtime::task::ExecutionResult<Chunk>> + Send
{
    /// Acquire the original observation immediately after claim, before the first
    /// data poll. Register the original receiver before work can be observed.
    /// There is deliberately no default: every author states the real scope
    /// and whether close still owns an additional body that must exit.
    fn take_exit_observation(
        self: Pin<&mut Self>,
        failures: Arc<dyn crate::runtime::task::ExecutionFailureReporter>,
    ) -> crate::runtime::task::ExecutionResult<ScanExitObservation>;

    /// Ends delivery and asks every operation of the scan to stop, before
    /// returning. Its result belongs to reader/close cleanup; originating work
    /// failures belong to the independent observation and its original reporter.
    /// Completion proves only this body exited, never the independent scope.
    /// Report cleanup before waiting on another owner. Unknown body exit must
    /// stay pending and retain its original responsibility.
    fn close(
        self: Pin<Box<Self>>,
        failures: Arc<dyn crate::runtime::task::ExecutionFailureReporter>,
    ) -> BoxFuture<'static, crate::runtime::task::ExecutionResult<()>>;
}

/// A scan output stream owned by its driver.
pub type ScanOutputStream = Pin<Box<dyn ScanChunkStream>>;

/// Hands a scan's single output stream to the driver that runs it.
pub trait ScanStreamSource: Send + Sync {
    /// Hands the stream over, polled with `budget` in every driver turn and
    /// reporting into the scan operator's own `profile`. A scan has one
    /// stream: a second claim is refused, never a second reader.
    fn claim(
        &self,
        budget: ConnectorPollBudget,
        profile: Option<RuntimeProfile>,
        construction: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) -> crate::runtime::task::ExecutionResult<ScanOutputStream>;
}

/// One bound scan: the stream its driver polls, and the hooks through which
/// the pipeline tells the scan about its consumer.
pub trait ScanOp: Send + Sync {
    /// The scan's one output stream. The pipeline runs a single driver that
    /// polls it and hands its chunks to the target degree of parallelism.
    fn stream_source(&self) -> Arc<dyn ScanStreamSource>;

    /// Starts terminal cleanup of the scan's reads without waiting for them:
    /// the scan's stream observes their exit when it is closed.
    fn terminate(&self) -> crate::runtime::task::ExecutionResult<()> {
        Ok(())
    }

    /// Reports whether the scan's consumer is full and holds back more reader
    /// work. The callback must not wait: output can stay queued while the
    /// downstream pipeline is blocked.
    fn on_output_backpressure(&self, _paused: bool) {}

    /// Reports one nonempty chunk actually taken by the scan's consumer.
    /// Producing a chunk is not progress at this boundary.
    fn on_nonempty_chunk_consumed(&self) {}

    fn profile_name(&self) -> Option<String> {
        None
    }
}

/// Instance-decoded, proto-free connector ranges handed to [`ScanSource::bind`].
///
/// The wire -> connector-range conversion lives in the decoders; this enum is
/// that conversion's already-enriched output. Keeping it proto-free lets the
/// wire-free connector layer materialize a per-instance [`ScanOp`] from static
/// config plus these ranges.
///
/// Every variant is consumed at execution time: the decoders route the
/// enriched ranges into the instance's `ScanAssignment`, and
/// `materialize_scan_bindings` replays them through `ScanSource::bind` to
/// produce the per-instance `ScanOp`.
#[derive(Clone, Debug)]
pub enum BoundScanRanges {
    /// No frozen range: a typed scan's work arrives as splits at runtime,
    /// and a system relation is read without any.
    None,
    /// Schema scans carry only the per-instance assignment gate.
    SchemaSelection { should_scan: bool },
}

/// Static, proto-free description of a scan source that materializes a
/// per-instance [`ScanOp`] from [`BoundScanRanges`].
///
/// `bind` performs only "connector-ranges + static config -> op"; all wire
/// decoding happens earlier in the decoders. This keeps the connector layer
/// uniform and free of proto/thrift types. Later KRN-1 phases store an
/// Task-owned `Arc<dyn ScanSource>` values bind their exact assignment at execution time.
pub trait ScanSource: Send + Sync {
    fn bind(
        &self,
        ranges: BoundScanRanges,
    ) -> crate::runtime::task::ExecutionResult<Arc<dyn ScanOp>>;

    fn profile_name(&self) -> Option<String> {
        None
    }
}

// Compile-time object-safety assertion for `ScanSource`.
#[cfg(test)]
const _: fn(&dyn ScanSource) = |_scan_source: &dyn ScanSource| {};

/// A stream source for tests whose scan is built but never run: claiming it fails.
#[cfg(test)]
pub(crate) struct UnusedScanStream;

#[cfg(test)]
impl ScanStreamSource for UnusedScanStream {
    fn claim(
        &self,
        _budget: ConnectorPollBudget,
        _profile: Option<RuntimeProfile>,
        _construction: Arc<dyn crate::exec::pipeline::operator::InputConstructionFactory>,
    ) -> crate::runtime::task::ExecutionResult<ScanOutputStream> {
        Err(novarocks_functions::KernelFailure::Internal(
            novarocks_functions::KernelDiagnostic::new("this test scan is never run"),
        )
        .into())
    }
}
