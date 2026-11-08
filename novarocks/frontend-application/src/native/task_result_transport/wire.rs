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

//! Passive dynamic-filter request, reply and error projection.

use super::*;

/// The caller supplies the original operation identity; projection mints none.
pub fn encode_dynamic_filter_request(
    operation: TaskOperationId,
    identity: TaskIdentity,
    acknowledged: Option<DomainVersion>,
) -> novarocks_proto_models::novarocks::FetchTaskDynamicFiltersRequest {
    encode_fetch_dynamic_filters(FetchTaskDynamicFilters::new(
        operation,
        identity,
        acknowledged,
    ))
}

pub fn decode_dynamic_filter_response(
    address: &str,
    identity: TaskIdentity,
    response: novarocks_proto_models::novarocks::FetchTaskDynamicFiltersResponse,
) -> Result<DynamicFilterRead, DynamicFilterReadError> {
    let answered = response
        .identity
        .as_ref()
        .map(|answered| {
            novarocks_task_codec::identity::decode_task_identity(
                answered,
                FieldPath::root("fetch_task_dynamic_filters").field("identity"),
            )
        })
        .transpose()
        .map_err(|error| DynamicFilterReadError::Refused(format!("{address}: {error}")))?
        .ok_or_else(|| {
            DynamicFilterReadError::Refused(format!(
                "{address}: dynamic filter read answered without a task identity"
            ))
        })?;
    // A read that agrees with itself proves nothing. This is the identity
    // this frontend asked about, so an answer naming another task is
    // refused rather than admitted as that task's feedback.
    if answered != identity {
        return Err(DynamicFilterReadError::Refused(format!(
            "{address}: dynamic filter read for {identity} answered for {answered}"
        )));
    }
    let version = DomainVersion::new(response.version).ok();
    if version.is_none() && !response.domains.is_empty() {
        return Err(DynamicFilterReadError::Refused(format!(
            "{address}: dynamic filter read carries domains under version zero"
        )));
    }
    let mut feedback = Vec::with_capacity(response.domains.len());
    for domain in &response.domains {
        let envelope = domain.envelope.as_ref().ok_or_else(|| {
            DynamicFilterReadError::Refused(format!(
                "{address}: dynamic filter domain carries no envelope"
            ))
        })?;
        feedback.push(
            TaskRuntimeFilterFeedback::parse(envelope)
                .map_err(|error| DynamicFilterReadError::Refused(format!("{address}: {error}")))?,
        );
    }
    Ok(DynamicFilterRead { version, feedback })
}

/// Classifies one dynamic filter read failure by type.
///
/// A status that leaves the read unfinished is `Unavailable`, and the next turn
/// asks again. Everything else is the backend having answered that the request
/// is illegal against the task it names, which asking again cannot repair.
pub fn classify_dynamic_filter_status(
    address: &str,
    status: &tonic::Status,
) -> DynamicFilterReadError {
    let detail = format!(
        "{address}: fetch_task_dynamic_filters rpc failed: {}",
        status.message()
    );
    match status.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::Unknown
        | tonic::Code::ResourceExhausted => DynamicFilterReadError::Unavailable(detail),
        _ => DynamicFilterReadError::Refused(detail),
    }
}
