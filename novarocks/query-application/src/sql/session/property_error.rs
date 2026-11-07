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

//! Nominal failure projection for the property owner's business boundary.
use super::registry::SessionPropertyError;
use crate::{session_error::QueryServiceError, sql::session_admit::SessionAdmitError};
use novarocks_failure_contract::{FailureFact, FailureProjection, SharedFailureCause};

pub fn session_property_error(error: SessionPropertyError) -> QueryServiceError {
    match error {
        error @ (SessionPropertyError::UnknownProperty { .. }
        | SessionPropertyError::InvalidValue { .. }
        | SessionPropertyError::NullValue { .. }) => QueryServiceError::from_user_error(
            SessionAdmitError::InvalidSessionProperty.without_location(error.to_string()),
        ),
        error @ SessionPropertyError::CatalogNotFound { .. } => QueryServiceError::from_user_error(
            SessionAdmitError::CatalogNotFound.without_location(error.to_string()),
        ),
        SessionPropertyError::Source(error) => error,
        SessionPropertyError::Constant(novarocks_constant_contract::ConstantError::Control(
            error,
        )) => QueryServiceError::from_compile_control(error),
        SessionPropertyError::Constant(error) => {
            QueryServiceError::from_failure_cause(SharedFailureCause::new(error))
        }
        SessionPropertyError::Render(error) => {
            QueryServiceError::from_failure_cause(SharedFailureCause::new(error))
        }
        error => QueryServiceError::from_failure_cause(SharedFailureCause::new(
            PropertyProjectionFailure(error),
        )),
    }
}
#[derive(Debug)]
struct PropertyProjectionFailure(SessionPropertyError);
impl std::fmt::Display for PropertyProjectionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for PropertyProjectionFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl FailureProjection for PropertyProjectionFailure {
    fn failure_fact(&self) -> FailureFact {
        crate::execution_error::QueryFailureReason::PreparationProtocol.failure_fact()
    }
}
