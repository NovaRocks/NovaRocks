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

//! Pure bounded client-row encoding boundary shared by BE roots and FE-local
//! producers. It owns neither packet framing nor a result-retention wallet.
//! Hosts pre-admit input/hydration/scratch and hold their capability through
//! actual cursor exit. This interface alone advertises no runtime support.

mod encoder;
pub use encoder::{ArrowMysqlTextEncoder, ArrowTrinoJsonEncoder, ValidatedClientInput};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RenderErrorKind {
    SchemaMismatch,
    UnsupportedCarrier,
    UnsupportedPresentation,
    RowTooLarge,
    ElementLimit,
    DepthLimit,
    ArithmeticOverflow,
    Cancelled,
    InvalidValue,
    InvalidUtf8,
    InvalidJson,
    InvalidVariant,
    CursorInvariant,
    WorkStackLimit,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RenderError {
    pub kind: RenderErrorKind,
    pub output_ordinal: Option<u32>,
    /// Actual extent at the producer, in bytes/items/depth as named by kind.
    pub required: Option<u64>,
}
impl std::fmt::Display for RenderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bounded MySQL text encoding failed: {:?}", self.kind)?;
        if let Some(ordinal) = self.output_ordinal {
            write!(f, " at output occurrence {ordinal}")?;
        }
        Ok(())
    }
}
impl std::error::Error for RenderError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RenderTurnStatus {
    Yielded,
    NeedsOutput,
    InputComplete,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RenderTurn {
    pub emitted_bytes: usize,
    /// Count/hydrate/escape scans must obey this CPU-byte quantum even when no
    /// logical payload has been emitted yet.
    pub examined_bytes: usize,
    pub visited_cells: usize,
    pub completed_rows: u64,
    pub status: RenderTurnStatus,
}

/// Emits the flat ClientRows body, including each exact u32LE rowTotal and at
/// least one payload byte in the same segment. Output remains unpublished until
/// the host commits its immutable segment. A 1..4-byte tail returns NeedsOutput
/// at a new row, while short continuation slices are legal. No padding is added.
///
/// Each call advances at most RootProfileV1's 64KiB byte/1024-cell quantum,
/// including row-length counting and nested traversal. Rows <=64KiB use bounded
/// single-pass staging; larger rows use an exact count then an immutable cursor,
/// never whole-row materialization. Cancellation prevents further publication.
pub trait BoundedClientRowEncoder: Send {
    fn step(&mut self, output: &mut [u8]) -> Result<RenderTurn, RenderError>;
    fn cancel(&mut self);
    fn scratch_capacity_bytes(&self) -> usize;
}

impl novarocks_failure_contract::FailureProjection for RenderError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::*;
        let (class, code) = match self.kind {
            RenderErrorKind::SchemaMismatch => (FailureClass::Invariant, 100),
            RenderErrorKind::UnsupportedCarrier => (FailureClass::Invariant, 101),
            RenderErrorKind::UnsupportedPresentation => (FailureClass::Data, 102),
            RenderErrorKind::RowTooLarge => (FailureClass::Representation, 103),
            RenderErrorKind::ElementLimit => (FailureClass::Resource, 104),
            RenderErrorKind::DepthLimit => (FailureClass::Resource, 105),
            RenderErrorKind::ArithmeticOverflow => (FailureClass::Resource, 106),
            RenderErrorKind::Cancelled => (FailureClass::Control, 107),
            RenderErrorKind::InvalidValue => (FailureClass::Data, 108),
            RenderErrorKind::InvalidUtf8 => (FailureClass::Data, 109),
            RenderErrorKind::InvalidJson => (FailureClass::Data, 110),
            RenderErrorKind::InvalidVariant => (FailureClass::Data, 111),
            RenderErrorKind::CursorInvariant => (FailureClass::Invariant, 112),
            RenderErrorKind::WorkStackLimit => (FailureClass::Resource, 113),
        };
        let fact = FailureFact::new(
            class,
            FailureReason::new(domain::RESULT, std::num::NonZeroU16::new(code).unwrap()),
        );
        match self.kind {
            RenderErrorKind::RowTooLarge => fact.with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::MinimumUnit,
                layout: RepresentationLayout {
                    domain: domain::RESULT,
                    code: std::num::NonZeroU16::new(1001).unwrap(),
                },
                required: self.required,
                extent_limit: Some(
                    novarocks_result_contract::RootProfileV1::ROW_PAYLOAD_BYTES.into(),
                ),
                representable_prefix: None,
            }),
            RenderErrorKind::ElementLimit
            | RenderErrorKind::DepthLimit
            | RenderErrorKind::WorkStackLimit => fact.with_resource(ResourceFacts {
                dimension: ResourceDimension::Items,
                request_scope: Some(ResourceScope::Request),
                constraint_scope: Some(ResourceScope::Request),
                requested: self.required,
                required_growth: None,
                limit: Some(if self.kind == RenderErrorKind::ElementLimit {
                    novarocks_result_contract::RootProfileV1::MAX_ELEMENTS_PER_ROW
                } else if self.kind == RenderErrorKind::DepthLimit {
                    novarocks_result_contract::RootProfileV1::MAX_DEPTH
                } else {
                    196
                } as u64),
                live: None,
                committed: None,
            }),
            _ => fact,
        }
    }
}

/// Dispatches only on the frontend-frozen presentation family.
pub enum ClientRowEncoder {
    MysqlText(ArrowMysqlTextEncoder),
    TrinoJson(ArrowTrinoJsonEncoder),
}
impl ClientRowEncoder {
    /// Construction-independent bound for this renderer's fixed state and
    /// scratch. Input arrays and the immutable render schema remain separately
    /// owned. Observing this bound grants no result capacity.
    pub const fn scratch_capacity_bound_bytes() -> usize {
        encoder::scratch_capacity_bound_bytes() + std::mem::size_of::<Self>()
            - std::mem::size_of::<ArrowMysqlTextEncoder>()
    }

    pub fn try_new(
        schema: std::sync::Arc<novarocks_result_contract::ClientRenderSchema>,
        batch: arrow::record_batch::RecordBatch,
    ) -> Result<Self, RenderError> {
        use novarocks_result_contract::ClientPresentationFamily as F;
        match schema.family() {
            F::MysqlText => ArrowMysqlTextEncoder::try_new(schema, batch).map(Self::MysqlText),
            F::TrinoJson(_) => ArrowTrinoJsonEncoder::try_new(schema, batch).map(Self::TrinoJson),
        }
    }
    pub fn try_new_root(
        contract: std::sync::Arc<novarocks_result_contract::RootOutputContract>,
        batch: arrow::record_batch::RecordBatch,
    ) -> Result<Self, RenderError> {
        use novarocks_result_contract::{ClientPresentationFamily as F, FrozenRootOutput};
        let FrozenRootOutput::ClientRows(schema) = contract.output() else {
            return Err(RenderError {
                kind: RenderErrorKind::SchemaMismatch,
                output_ordinal: None,
                required: None,
            });
        };
        match schema.family() {
            F::MysqlText => {
                ArrowMysqlTextEncoder::try_new_root(contract, batch).map(Self::MysqlText)
            }
            F::TrinoJson(_) => {
                ArrowTrinoJsonEncoder::try_new_root(contract, batch).map(Self::TrinoJson)
            }
        }
    }
    pub fn try_new_typed_root(
        input: ValidatedClientInput,
        batch: arrow::record_batch::RecordBatch,
    ) -> Result<Self, RenderError> {
        use novarocks_result_contract::{ClientPresentationFamily as F, FrozenRootOutput};
        let FrozenRootOutput::ClientRows(schema) = input.contract().output() else {
            return Err(RenderError {
                kind: RenderErrorKind::SchemaMismatch,
                output_ordinal: None,
                required: None,
            });
        };
        match schema.family() {
            F::MysqlText => {
                ArrowMysqlTextEncoder::try_new_typed_root(input, batch).map(Self::MysqlText)
            }
            F::TrinoJson(_) => {
                ArrowTrinoJsonEncoder::try_new_typed_root(input, batch).map(Self::TrinoJson)
            }
        }
    }
}
impl BoundedClientRowEncoder for ClientRowEncoder {
    fn step(&mut self, out: &mut [u8]) -> Result<RenderTurn, RenderError> {
        match self {
            Self::MysqlText(e) => e.step(out),
            Self::TrinoJson(e) => e.step(out),
        }
    }
    fn cancel(&mut self) {
        match self {
            Self::MysqlText(e) => e.cancel(),
            Self::TrinoJson(e) => e.cancel(),
        }
    }
    fn scratch_capacity_bytes(&self) -> usize {
        let (inner, control) = match self {
            Self::MysqlText(e) => (
                e.scratch_capacity_bytes(),
                std::mem::size_of::<ArrowMysqlTextEncoder>(),
            ),
            Self::TrinoJson(e) => (
                e.scratch_capacity_bytes(),
                std::mem::size_of::<ArrowTrinoJsonEncoder>(),
            ),
        };
        inner + std::mem::size_of::<Self>() - control
    }
}
