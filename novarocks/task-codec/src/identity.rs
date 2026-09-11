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

//! Task protocol identity codec.

use novarocks_execution_contract::task_execution::identity::{
    AdmissionEpochCapability, AdmissionTicketId, QueryContextRef, TaskIdentity, TaskOperationId,
};
use novarocks_proto_models::novarocks;
use novarocks_types::identity::{BackendProcessId, FrontendProcessId, StageId, TaskId};

use novarocks_proto_codec::lifecycle::identity::{
    decode_query_execution_id, encode_query_execution_id,
};
use novarocks_proto_codec::{FieldPath, ProtocolError};

use crate::{invalid, missing};

/// Every process identity, operation identity, and opaque admission nonce on
/// this wire is exactly 16 bytes. Each typed decoder applies its own semantic
/// validation after this width check.
fn decode_identity_bytes(value: &[u8], path: FieldPath) -> Result<[u8; 16], ProtocolError> {
    value
        .try_into()
        .map_err(|_| invalid(path, "identity must be exactly 16 bytes"))
}

/// Decodes a frontend process incarnation fence.
pub fn decode_frontend_process_id(
    src: &novarocks::FrontendProcessId,
    path: FieldPath,
) -> Result<FrontendProcessId, ProtocolError> {
    let bytes = decode_identity_bytes(&src.value, path.clone().field("value"))?;
    FrontendProcessId::try_from_bytes(bytes)
        .map_err(|error| invalid(path.field("value"), error.to_string()))
}

pub fn encode_frontend_process_id(value: FrontendProcessId) -> novarocks::FrontendProcessId {
    novarocks::FrontendProcessId {
        value: value.to_bytes().to_vec(),
    }
}

/// Decodes an operation identity.
pub fn decode_task_operation_id(
    src: &novarocks::TaskOperationId,
    path: FieldPath,
) -> Result<TaskOperationId, ProtocolError> {
    let bytes = decode_identity_bytes(&src.value, path.clone().field("value"))?;
    TaskOperationId::try_from_bytes(bytes)
        .map_err(|error| invalid(path.field("value"), error.to_string()))
}

pub fn encode_task_operation_id(value: TaskOperationId) -> novarocks::TaskOperationId {
    novarocks::TaskOperationId {
        value: value.to_bytes().to_vec(),
    }
}

/// Decodes an opaque worker admission ticket identity.
pub fn decode_admission_ticket_id(
    src: &novarocks::AdmissionTicketId,
    path: FieldPath,
) -> Result<AdmissionTicketId, ProtocolError> {
    let bytes = decode_identity_bytes(&src.value, path.clone().field("value"))?;
    AdmissionTicketId::try_from_bytes(bytes)
        .map_err(|error| invalid(path.field("value"), error.to_string()))
}

pub fn encode_admission_ticket_id(value: AdmissionTicketId) -> novarocks::AdmissionTicketId {
    novarocks::AdmissionTicketId {
        value: value.to_bytes().to_vec(),
    }
}

/// Decodes the opaque worker admission issuance epoch.
pub fn decode_admission_epoch_capability(
    src: &novarocks::AdmissionEpochCapability,
    path: FieldPath,
) -> Result<AdmissionEpochCapability, ProtocolError> {
    let bytes = decode_identity_bytes(&src.value, path.clone().field("value"))?;
    AdmissionEpochCapability::try_from_bytes(bytes)
        .map_err(|error| invalid(path.field("value"), error.to_string()))
}

pub fn encode_admission_epoch_capability(
    value: AdmissionEpochCapability,
) -> novarocks::AdmissionEpochCapability {
    novarocks::AdmissionEpochCapability {
        value: value.to_bytes().to_vec(),
    }
}

fn decode_backend_process_id(
    src: &novarocks::BackendProcessId,
    path: FieldPath,
) -> Result<BackendProcessId, ProtocolError> {
    let bytes = decode_identity_bytes(&src.value, path.clone().field("value"))?;
    BackendProcessId::try_from_bytes(bytes)
        .map_err(|error| invalid(path.field("value"), error.to_string()))
}

fn encode_backend_process_id(value: BackendProcessId) -> novarocks::BackendProcessId {
    novarocks::BackendProcessId {
        value: value.to_bytes().to_vec(),
    }
}

/// Decodes the indivisible wire identity of one task.
pub fn decode_task_identity(
    src: &novarocks::TaskIdentity,
    path: FieldPath,
) -> Result<TaskIdentity, ProtocolError> {
    let execution = src.query_execution_id.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("query_execution_id"),
            "task identity requires a query execution id",
        )
    })?;
    let execution = decode_query_execution_id(execution)?;
    let stage_id = StageId::new(src.stage_id)
        .map_err(|error| invalid(path.clone().field("stage_id"), error.to_string()))?;
    let task_id = TaskId::new(src.task_id)
        .map_err(|error| invalid(path.clone().field("task_id"), error.to_string()))?;
    let backend = src.backend_process_id.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("backend_process_id"),
            "task identity requires a backend process id",
        )
    })?;
    let backend = decode_backend_process_id(backend, path.field("backend_process_id"))?;
    Ok(TaskIdentity::new(execution, stage_id, task_id, backend))
}

pub fn encode_task_identity(value: TaskIdentity) -> novarocks::TaskIdentity {
    novarocks::TaskIdentity {
        query_execution_id: Some(encode_query_execution_id(value.query_execution_id())),
        stage_id: value.stage_id().get(),
        task_id: value.task_id().get(),
        backend_process_id: Some(encode_backend_process_id(value.backend_process_id())),
    }
}

/// Decodes an exact query context reference.
pub fn decode_query_context_ref(
    src: &novarocks::QueryContextRef,
    path: FieldPath,
) -> Result<QueryContextRef, ProtocolError> {
    let execution = src.query_execution_id.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("query_execution_id"),
            "query context requires a query execution id",
        )
    })?;
    let execution = decode_query_execution_id(execution)?;
    let frontend = src.frontend_process_id.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("frontend_process_id"),
            "query context requires a frontend process id",
        )
    })?;
    let frontend = decode_frontend_process_id(frontend, path.clone().field("frontend_process_id"))?;
    let backend = src.backend_process_id.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("backend_process_id"),
            "query context requires a backend process id",
        )
    })?;
    let backend = decode_backend_process_id(backend, path.field("backend_process_id"))?;
    Ok(QueryContextRef::new(execution, frontend, backend))
}

pub fn encode_query_context_ref(value: QueryContextRef) -> novarocks::QueryContextRef {
    novarocks::QueryContextRef {
        query_execution_id: Some(encode_query_execution_id(value.query_execution_id())),
        frontend_process_id: Some(encode_frontend_process_id(value.frontend_process_id())),
        backend_process_id: Some(encode_backend_process_id(value.backend_process_id())),
    }
}
