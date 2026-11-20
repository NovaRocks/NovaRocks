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

//! The connector `SourcePage` to execution `Chunk` boundary.
//!
//! Responsibilities:
//! - Turns each page a scan's page stream produces into a `Chunk` bound to
//!   the scan's ordered output slot ids.
//! - Keeps the distinction the page contract makes and a naive adapter would
//!   lose: a page with zero channels and a positive position count is a real
//!   result, never end of stream.
//!
//! Key exported interfaces:
//! - Types: `SourcePageConverter`, `PageAdapterError`, `PageAdapterErrorKind`.
//!
//! Every page binds the scan's immutable complete typed output declaration.
//! Runtime construction uses the installed scan factory; page arrays never
//! author a competing schema.
//!
//! Provider neutrality: nothing here names a provider or inspects a provider
//! variant, so this file compiles with no provider crate in the dependency
//! graph.

use std::sync::Arc;

use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::SourcePage;

use crate::exec::chunk::{ArrayNormalizationStep, Chunk, ChunkSchemaRef};
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, InputConstructionOwner,
};
use crate::runtime::task::{ExecutionFailure, ExecutionResult};

/// The source diagnostic and creating owner's original cause stay distinct.
enum PageConversionFailure {
    Page(PageAdapterError),
    Construction(ConstructionFailure),
}
impl From<PageAdapterError> for PageConversionFailure {
    fn from(error: PageAdapterError) -> Self {
        Self::Page(error)
    }
}
impl From<ConstructionFailure> for PageConversionFailure {
    fn from(error: ConstructionFailure) -> Self {
        Self::Construction(error)
    }
}
impl PageConversionFailure {
    fn into_execution(self) -> ExecutionFailure {
        match self {
            Self::Page(error) => ExecutionFailure::from_domain(error),
            Self::Construction(error) => error.into(),
        }
    }
    #[cfg(test)]
    fn into_page(self) -> PageAdapterError {
        match self {
            Self::Page(error) => error,
            // Only the explicit pure adapter uses this conversion. Its body
            // has no construction owner and cannot originate this variant.
            Self::Construction(error) => {
                PageAdapterError::new(PageAdapterCause::Chunk, error.to_string())
            }
        }
    }
}

/// Why one page could not become a chunk.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PageAdapterErrorKind {
    /// The page carries fewer channels than the output binds.
    ChannelMismatch,
    /// A materialized channel does not agree with the page's position count.
    PositionMismatch,
    /// The connector failed while producing, materializing, or closing.
    Connector,
    /// The materialized columns could not form a chunk.
    Chunk,
}

impl std::fmt::Display for PageAdapterErrorKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::ChannelMismatch => "channel mismatch",
            Self::PositionMismatch => "position mismatch",
            Self::Connector => "connector failure",
            Self::Chunk => "chunk construction failure",
        })
    }
}

/// A page-conversion cause. Connector failures always retain their original
/// typed payload; local contract failures cannot impersonate a Connector.
#[derive(Clone, Debug, Eq, PartialEq)]
enum PageAdapterCause {
    ChannelMismatch,
    PositionMismatch,
    Chunk,
    Connector(ConnectorError),
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageAdapterError {
    cause: PageAdapterCause,
    detail: novarocks_failure_contract::BoundedDetail,
}
impl PageAdapterError {
    fn new(cause: PageAdapterCause, detail: impl std::fmt::Display) -> Self {
        Self {
            cause,
            detail: novarocks_failure_contract::BoundedDetail::truncating_display(detail),
        }
    }
    pub fn from_connector(error: ConnectorError, context: &str) -> Self {
        let detail = novarocks_failure_contract::BoundedDetail::truncating_display(format_args!(
            "{context}: {error}"
        ));
        Self {
            cause: PageAdapterCause::Connector(error),
            detail,
        }
    }
    pub const fn kind(&self) -> PageAdapterErrorKind {
        match &self.cause {
            PageAdapterCause::ChannelMismatch => PageAdapterErrorKind::ChannelMismatch,
            PageAdapterCause::PositionMismatch => PageAdapterErrorKind::PositionMismatch,
            PageAdapterCause::Chunk => PageAdapterErrorKind::Chunk,
            PageAdapterCause::Connector(_) => PageAdapterErrorKind::Connector,
        }
    }
    pub fn detail(&self) -> &str {
        self.detail.as_str()
    }
    pub const fn connector_error(&self) -> Option<&ConnectorError> {
        match &self.cause {
            PageAdapterCause::Connector(error) => Some(error),
            _ => None,
        }
    }
}
impl std::fmt::Display for PageAdapterError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.kind(), self.detail)
    }
}
impl std::error::Error for PageAdapterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.connector_error()
            .map(|error| error as &dyn std::error::Error)
    }
}
impl novarocks_failure_contract::FailureProjection for PageAdapterError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let reason = match &self.cause {
            PageAdapterCause::Connector(error) => return error.failure_fact(),
            PageAdapterCause::ChannelMismatch => ExecutionReason::SourcePageChannels,
            PageAdapterCause::PositionMismatch => ExecutionReason::SourcePagePositions,
            PageAdapterCause::Chunk => ExecutionReason::SourcePageBinding,
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}

/// Converts pages against the scan's frozen complete typed declaration.
/// Channels beyond its bound prefix remain provider working columns and are
/// dropped. A page's accounted output memory moves onto its chunk.
pub struct SourcePageConverter {
    output_schema: ChunkSchemaRef,
}

impl SourcePageConverter {
    /// Borrow the scan's frozen ordered slots and complete typed declarations.
    /// This converter creates no competing slot-ID vector or type author.
    pub fn with_output_schema(output_schema: ChunkSchemaRef) -> Self {
        Self { output_schema }
    }

    #[cfg(test)]
    fn convert(&mut self, page: SourcePage) -> Result<Chunk, PageAdapterError> {
        convert_page(page, &self.output_schema, None).map_err(PageConversionFailure::into_page)
    }

    /// The installed scan factory creates only the new batch/type scaffolds.
    /// The provider's column vector and output-memory token transfer unchanged.
    pub fn convert_with_factory(
        &mut self,
        page: SourcePage,
        factory: &dyn InputConstructionFactory,
    ) -> ExecutionResult<Chunk> {
        let owner = factory.begin_typed_construction(&self.output_schema)?;
        convert_page(page, &self.output_schema, Some(owner.as_ref()))
            .map_err(PageConversionFailure::into_execution)
    }
}

fn convert_page(
    mut page: SourcePage,
    output_schema: &ChunkSchemaRef,
    owner: Option<&dyn InputConstructionOwner>,
) -> Result<Chunk, PageConversionFailure> {
    if let Some(owner) = owner {
        owner.observe(
            0,
            &[],
            ArrayNormalizationStep::InspectConstructionWork { units: 1 },
        )?;
    }
    if !output_schema.is_typed() {
        return Err(PageAdapterError::new(
            PageAdapterCause::Chunk,
            "connector page output schema lacks complete typed bindings",
        )
        .into());
    }
    let slot_ids = output_schema.slot_ids();
    let positions = page.position_count();
    let channel_count = page.channel_count();
    if slot_ids.len() > channel_count {
        return Err(PageAdapterError::new(
            PageAdapterCause::ChannelMismatch,
            format!(
                "scan output binds {} channels but the page carries {channel_count}",
                slot_ids.len()
            ),
        )
        .into());
    }

    // Discard provider working channels before extracting the page. The
    // accounted extraction moves the provider reservation together with the
    // visible Arrow buffers.
    page.truncate_channels(slot_ids.len()).map_err(|error| {
        PageAdapterError::from_connector(error, "connector page channel projection")
    })?;
    let (extracted_positions, columns, output_memory) =
        page.into_accounted_columns().map_err(|error| {
            PageAdapterError::from_connector(error, "connector page channel materialization")
        })?;
    debug_assert_eq!(positions, extracted_positions);
    for (index, column) in columns.iter().enumerate() {
        if let Some(owner) = owner {
            owner.observe(
                index,
                &[],
                ArrayNormalizationStep::InspectConstructionWork { units: 1 },
            )?;
        }
        if column.len() != positions {
            return Err(PageAdapterError::new(
                PageAdapterCause::PositionMismatch,
                format!(
                    "channel {index} produced {} values for a page of {positions} positions",
                    column.len()
                ),
            )
            .into());
        }
    }

    let mut visible_bytes = 0_u64;
    for (index, column) in columns.iter().enumerate() {
        if let Some(owner) = owner {
            owner.observe(index, &[], ArrayNormalizationStep::OpaqueOperationFinished)?;
        }
        visible_bytes = visible_bytes.saturating_add(column.get_array_memory_size() as u64);
        if let Some(owner) = owner {
            owner.observe(index, &[], ArrayNormalizationStep::OpaqueOperationFinished)?;
        }
    }
    let mut chunk =
        if let Some(owner) = owner {
            // The incoming Vec already belongs to the SourcePage. Only actual
            // normalized wrappers and the new RecordBatch Arc are constructed.
            Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
                Arc::clone(output_schema),
                columns,
                positions,
                owner.storage_lifetime(),
                |step| owner.observe(0, &[], step),
            )?
        } else {
            #[cfg(not(test))]
            return Err(ConstructionFailure::MissingOwner.into());
            #[cfg(test)]
            {
                // The explicit pure fixture path uses the same complete declaration,
                // including row count for a zero-channel page; it never derives types.
                Chunk::try_new_typed_with_columns_observed::<
                    crate::runtime::task::ExecutionFailure,
                    _,
                >(Arc::clone(output_schema), columns, positions, |_| Ok(()))
                .map_err(|error| PageAdapterError::new(PageAdapterCause::Chunk, error))?
            }
        };

    let output_memory = if let Some(mut output_memory) = output_memory {
        output_memory.shrink_to(visible_bytes).map_err(|error| {
            PageAdapterError::from_connector(error, "connector page visible output accounting")
        })?;
        if visible_bytes > 0 {
            Some(output_memory)
        } else {
            None
        }
    } else {
        None
    };
    if let Some(output_memory) = output_memory {
        chunk
            .attach_connector_output_memory(output_memory)
            .map_err(|error| PageAdapterError::new(PageAdapterCause::Chunk, error))?;
    }
    Ok(chunk)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod failure_projection_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    #[test]
    fn page_conversion_preserves_connector_cause_and_does_not_classify_diagnostics() {
        use novarocks_spi::connector::ConnectorErrorKind;
        for kind in [
            ConnectorErrorKind::Unavailable,
            ConnectorErrorKind::Cancelled,
        ] {
            let original = ConnectorError::new(kind, "ResourceExhausted: secret".repeat(200));
            let expected = original.failure_fact();
            let failure = PageConversionFailure::Page(PageAdapterError::from_connector(
                original.clone(),
                "page input",
            ))
            .into_execution();
            assert_eq!(failure.failure_fact(), expected);
            let crate::runtime::task::ExecutionFailureCause::Domain(domain) = failure.cause()
            else {
                panic!("page cause")
            };
            let actual = domain.downcast_ref::<PageAdapterError>().unwrap();
            assert_eq!(actual.connector_error(), Some(&original));
            assert!(actual.detail().len() <= novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES);
            assert_eq!(
                std::error::Error::source(actual)
                    .unwrap()
                    .downcast_ref::<ConnectorError>(),
                Some(&original)
            );
        }
        let local = PageAdapterError::new(PageAdapterCause::ChannelMismatch, "resource exhausted");
        assert_eq!(local.failure_fact().class(), FailureClass::Invariant);
    }
}
