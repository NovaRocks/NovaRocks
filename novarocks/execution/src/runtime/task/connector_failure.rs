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

//! Lossless execution context around a Connector-owned failure.

use novarocks_failure_contract::{BoundedDetail, FailureFact, FailureProjection};
use novarocks_spi::connector::ConnectorError;
use std::{error::Error, fmt, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorOperation {
    OpenReader,
    PrepareReader,
    PromoteReader,
    ReadPage,
    CloseReader,
    SourceExit,
    OpenWriter,
    AppendWriter,
    FinishWriter,
    AbortWriter,
    EncodeCommitFragment,
    DecodeWriteTarget,
    ShrinkOutputReservation,
    ValidateWriteRelation,
    CheckPartialAggregateUpdate,
    CheckPartialAggregateFinalize,
}
impl fmt::Display for ConnectorOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::OpenReader => "open connector reader",
            Self::PrepareReader => "prepare connector reader",
            Self::PromoteReader => "promote connector reader",
            Self::ReadPage => "read connector page",
            Self::CloseReader => "close connector reader",
            Self::SourceExit => "exit connector source",
            Self::OpenWriter => "open connector writer",
            Self::AppendWriter => "append connector writer batch",
            Self::FinishWriter => "finish connector writer",
            Self::AbortWriter => "abort connector writer",
            Self::EncodeCommitFragment => "encode table writer commit fragment",
            Self::DecodeWriteTarget => "decode writer target ordinal",
            Self::ValidateWriteRelation => "validate connector write relation",
            Self::CheckPartialAggregateUpdate => "check writer partial aggregate update",
            Self::CheckPartialAggregateFinalize => "check writer partial aggregate finalization",
            Self::ShrinkOutputReservation => "shrink connector output accounting after projection",
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorCallFailure {
    operation: ConnectorOperation,
    cause: Arc<ConnectorError>,
    sequence_id: Option<u64>,
    detail: BoundedDetail,
}
impl ConnectorCallFailure {
    pub fn new(operation: ConnectorOperation, cause: ConnectorError) -> Self {
        let detail = BoundedDetail::truncating_display(format_args!("{operation}: {cause}"));
        Self {
            operation,
            cause: Arc::new(cause),
            sequence_id: None,
            detail,
        }
    }
    pub fn at_sequence(mut self, sequence_id: u64) -> Self {
        self.sequence_id = Some(sequence_id);
        self.detail = BoundedDetail::truncating_display(format_args!(
            "{} for sequence {sequence_id}: {}",
            self.operation, self.cause
        ));
        self
    }
    pub const fn sequence_id(&self) -> Option<u64> {
        self.sequence_id
    }
    pub const fn operation(&self) -> ConnectorOperation {
        self.operation
    }
    pub fn cause(&self) -> &ConnectorError {
        self.cause.as_ref()
    }
    pub fn detail(&self) -> &str {
        self.detail.as_str()
    }
}
impl fmt::Display for ConnectorCallFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.detail.fmt(f)
    }
}
impl Error for ConnectorCallFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.cause.as_ref())
    }
}
impl FailureProjection for ConnectorCallFailure {
    fn failure_fact(&self) -> FailureFact {
        self.cause.failure_fact()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::{ConnectorErrorKind, ConnectorTableObjectBindingFailure};
    #[test]
    fn execution_context_preserves_connector_facts_and_bounds_only_the_rendering() {
        for source in [
            ConnectorError::new(ConnectorErrorKind::Unavailable, "界".repeat(1000))
                .with_retryable_before_progress()
                .with_cleanup_context("provider cleanup"),
            ConnectorError::table_object_binding(
                ConnectorTableObjectBindingFailure::Replaced,
                "rebound",
            ),
        ] {
            let call = ConnectorCallFailure::new(ConnectorOperation::ReadPage, source.clone());
            assert_eq!(call.cause(), &source);
            assert_eq!(call.failure_fact(), source.failure_fact());
            assert!(
                call.detail().len() <= novarocks_execution_contract::status::SAFE_DETAIL_MAX_BYTES
            );
            assert_eq!(
                call.source().unwrap().downcast_ref::<ConnectorError>(),
                Some(&source)
            );
        }
    }
}
