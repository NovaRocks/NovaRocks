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

//! Execution-side adapters for the typed connector read stack.
//!
//! Two provider-neutral pieces live here, matching the two runtime boundaries a
//! typed connector scan crosses:
//!
//! - [`scan_queue`] owns the split stream a scan receives after its plan was
//!   frozen, one queue per (task attempt, plan node).
//! - [`page_adapter`] owns the `SourcePage` to [`Chunk`](crate::exec::chunk::Chunk)
//!   conversion on the way out of a connector.
//!
//! Neither interprets a provider variant, holds an opaque payload, or names a
//! provider, so this module compiles with no provider crate in the dependency
//! graph. Neither owns a lifecycle: both are created by, and die with, the
//! query-scoped state that already owns the attempt.

pub mod page_adapter;
pub mod scan_queue;

pub use page_adapter::{
    PageAdapterError, PageAdapterErrorKind, SourcePageConverter, source_page_to_chunk,
};
pub use scan_queue::{
    ScheduledSplitFacts, SplitOfferOutcome, SplitPoll, SplitQueue, SplitQueueConfig,
    SplitQueueError, SplitQueueErrorKind, SplitQueueRegistry, SplitQueueStats,
    SplitSequenceEvidence, SplitSequencePreflight, TaskAttemptKey, TaskAttemptSplitQueues,
};
