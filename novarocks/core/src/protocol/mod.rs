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

pub mod common;
pub(crate) mod native;

pub use common::error::{FieldPath, ProtocolError, ProtocolErrorKind, ProtocolFamily};

/// Decode the lifecycle-owned query-options DTO into execution options.
///
/// This is deliberately separate from native fragment assembly: backend roles
/// validate their `InstanceParams` before passing the resulting execution value
/// to the core assembly kernel.
pub fn decode_native_query_options(
    src: &crate::proto::novarocks::QueryOptions,
) -> Result<crate::runtime::query_options::QueryOptions, ProtocolError> {
    native::query_options_contract::decode_query_options(src)
}

/// Decode the lifecycle identity carried by native control-plane DTOs.
///
/// This is transport validation shared by connector-binding and query-lifecycle
/// handlers; it deliberately does not expose fragment-program assembly.
pub fn decode_native_query_execution_id(
    execution_id: &crate::proto::novarocks::QueryExecutionId,
) -> Result<crate::query_execution::lifecycle::QueryExecutionId, ProtocolError> {
    use crate::query_execution::contract::QueryId;
    use crate::query_execution::lifecycle::AttemptId;

    let root = FieldPath::root("execution_id");
    let query_id = execution_id.query_id.as_ref().ok_or_else(|| {
        ProtocolError::new(
            ProtocolFamily::Native,
            root.clone().field("query_id"),
            ProtocolErrorKind::MissingField,
            "native fragment execution_id requires query_id",
        )
    })?;
    let attempt_id = AttemptId::new(execution_id.attempt_id).map_err(|error| {
        ProtocolError::new(
            ProtocolFamily::Native,
            root.clone().field("attempt_id"),
            ProtocolErrorKind::InvalidValue,
            error.to_string(),
        )
    })?;
    crate::query_execution::lifecycle::QueryExecutionId::new(
        QueryId::new(query_id.hi, query_id.lo),
        attempt_id,
    )
    .map_err(|error| {
        ProtocolError::new(
            ProtocolFamily::Native,
            root,
            ProtocolErrorKind::InvalidValue,
            error.to_string(),
        )
    })
}
