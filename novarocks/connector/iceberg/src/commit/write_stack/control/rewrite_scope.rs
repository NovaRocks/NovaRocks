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

//! Scoped rewrite preparation retaining the caller's original operation.

use super::rewrite_source as own;
use super::*;
use crate::metadata::existing_serde_projection::{self as projection, OriginalScope};
pub(crate) use novarocks_spi::connector::ConnectorScopedBeginCause as Cause;
use novarocks_spi::connector::{
    ConnectorOriginalResultScope, ConnectorPayloadRetentionGuard, OriginalResultCheckError,
};

pub(crate) fn overflow() -> Cause {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "rewrite construction arithmetic overflowed",
    )
    .into()
}
pub(crate) fn projection_error(e: projection::Error<OriginalResultCheckError>) -> Cause {
    match e {
        projection::Error::Control(e) => e.into(),
        projection::Error::Json => ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "Iceberg rewrite canonical projection failed",
        )
        .into(),
        projection::Error::Overflow => overflow(),
        projection::Error::ShapeChanged => ConnectorError::new(
            ConnectorErrorKind::Internal,
            "Iceberg rewrite canonical projection changed during serialization",
        )
        .into(),
    }
}
pub(super) fn own_error(e: own::Failure<OriginalResultCheckError>) -> Cause {
    match e {
        own::Failure::Original(e) => e.into(),
        own::Failure::Provider(e) => e.into(),
        own::Failure::Projection(e) => projection_error(e),
        own::Failure::Overflow => overflow(),
        own::Failure::ShapeChanged => ConnectorError::new(
            ConnectorErrorKind::Internal,
            "Iceberg rewrite constructor changed its exact output shape",
        )
        .into(),
    }
}
/// Borrows the original operation's cancellation, deadline and retention.
pub(crate) struct RewriteBeginScope<'a> {
    original: &'a ConnectorOriginalResultScope,
}
impl<'a> RewriteBeginScope<'a> {
    pub(crate) fn new(original: &'a ConnectorOriginalResultScope) -> Result<Self, Cause> {
        original.check_active()?;
        Ok(Self { original })
    }
}
impl OriginalScope<OriginalResultCheckError> for RewriteBeginScope<'_> {
    fn check_active(&self) -> Result<(), OriginalResultCheckError> {
        self.original.check_active()
    }
    fn original_guard(&self) -> ConnectorPayloadRetentionGuard {
        self.original.retention_guard()
    }
}
