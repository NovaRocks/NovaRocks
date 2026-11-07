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

//! Orthogonal write-result, distribution and placement facts.

use std::num::NonZeroU32;

use crate::{
    ChangeStreamWriterCut, EdgeId, FragmentId, ValueId, WriteTargetOrdinal, WriterResultCut,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeliveryRequirement {
    DemandDriven,
    MustDrain,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutputRole {
    Relational,
    ProviderRoute { target: WriteTargetOrdinal },
    Summary(SummaryKind),
    WriterResults,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StagePlacementPolicy {
    Singleton,
    ScanWork,
    Spread { max_instances: Option<NonZeroU32> },
    FollowSources { sources: Box<[FragmentId]> },
}

pub use novarocks_result_contract::{
    FinishCountMode, FinishInputs, PerKind, SummaryField, SummaryKind, SummaryWireType,
};

/// Which fragment-owned output a source edge projects.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FragmentOutputPort {
    Root,
    Summary(SummaryKind),
}

/// Kind fixes the exact ordered field identities without dynamic backing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SummaryColumns {
    WriteCounts([ValueId; 3]),
    DeleteVerification([ValueId; 4]),
}
impl SummaryColumns {
    pub const fn write_counts(inserted: ValueId, replaced: ValueId, deleted: ValueId) -> Self {
        Self::WriteCounts([inserted, replaced, deleted])
    }
    pub const fn delete_verification(
        requested: ValueId,
        matched: ValueId,
        insufficient_keys: ValueId,
        fingerprint: ValueId,
    ) -> Self {
        Self::DeleteVerification([requested, matched, insufficient_keys, fingerprint])
    }
    pub const fn kind(self) -> SummaryKind {
        match self {
            Self::WriteCounts(_) => SummaryKind::WriteCounts,
            Self::DeleteVerification(_) => SummaryKind::DeleteVerification,
        }
    }
    pub const fn values(&self) -> &[ValueId] {
        match self {
            Self::WriteCounts(values) => values,
            Self::DeleteVerification(values) => values,
        }
    }
    pub fn fields(&self) -> impl ExactSizeIterator<Item = (SummaryField, ValueId)> + '_ {
        self.kind()
            .fields()
            .iter()
            .copied()
            .zip(self.values().iter().copied())
    }
}

/// Both endpoints must declare the same kind and its exact ordered fields.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SummaryCut {
    pub source: SummaryColumns,
    pub destination: SummaryColumns,
}
impl SummaryCut {
    pub const fn kind(self) -> SummaryKind {
        self.source.kind()
    }
}

/// Mutually exclusive payload proofs. The payload is the sole cut-role author;
/// summary counters cannot be accepted as a provider writer relation.
#[derive(Clone, Debug, PartialEq)]
pub enum ExchangePayloadContract {
    Relational,
    ProviderRoute(ChangeStreamWriterCut),
    WriterResults(WriterResultCut),
    Summary(SummaryCut),
}

impl ExchangePayloadContract {
    pub fn provider_route(&self) -> Option<&ChangeStreamWriterCut> {
        match self {
            Self::ProviderRoute(proof) => Some(proof),
            _ => None,
        }
    }
    pub fn writer_results(&self) -> Option<&WriterResultCut> {
        match self {
            Self::WriterResults(proof) => Some(proof),
            _ => None,
        }
    }
    pub fn role(&self) -> OutputRole {
        match self {
            Self::Relational => OutputRole::Relational,
            Self::ProviderRoute(proof) => OutputRole::ProviderRoute {
                target: proof.write_target_ordinal,
            },
            Self::WriterResults(_) => OutputRole::WriterResults,
            Self::Summary(proof) => OutputRole::Summary(proof.kind()),
        }
    }
}

impl crate::InboundFragmentCut {
    pub fn change_stream_writer(&self) -> Option<&ChangeStreamWriterCut> {
        self.payload.provider_route()
    }
    pub fn writer_result(&self) -> Option<&WriterResultCut> {
        self.payload.writer_results()
    }
}
impl crate::OutboundFragmentCut {
    pub fn change_stream_writer(&self) -> Option<&ChangeStreamWriterCut> {
        self.payload.provider_route()
    }
    pub fn writer_result(&self) -> Option<&WriterResultCut> {
        self.payload.writer_results()
    }
}

// Design: ADR-0219 (docs/adr/ADR-0219-typed-task-summaries-and-per-kind-completion.md)
/// Static authorship, separate from fragment sink and live Task capabilities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SummaryAuthor {
    RouterSink,
    Node(crate::NodeId),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SummaryOutput {
    pub edge: EdgeId,
    pub columns: SummaryColumns,
    pub author: SummaryAuthor,
}
impl SummaryOutput {
    pub const fn kind(self) -> SummaryKind {
        self.columns.kind()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssertionApplicability {
    AllRows,
    WhenTrue(ValueId),
}

/// Correctness colocation is independent from layout and stage placement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteColocation {
    None,
    Singleton,
    ByKey(Box<[ValueId]>),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteLayoutPreference {
    None,
    ClusterBy(Box<[ValueId]>),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteRouteRequirements {
    pub colocation: WriteColocation,
    pub layout: WriteLayoutPreference,
}
impl WriteRouteRequirements {
    pub fn colocation_keys(&self) -> &[ValueId] {
        match &self.colocation {
            WriteColocation::ByKey(keys) => keys,
            _ => &[],
        }
    }
    pub fn layout_keys(&self) -> &[ValueId] {
        match &self.layout {
            WriteLayoutPreference::ClusterBy(keys) => keys,
            _ => &[],
        }
    }
}
