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

//! Public passive projections used by Native execution and deterministic worlds.
//! These functions create no work, schedule no transport, and observe no metrics.
//! Request identities, original permits and payload custody remain with callers.

pub use crate::native::task_result_transport::wire::{
    classify_dynamic_filter_status, decode_dynamic_filter_response, encode_dynamic_filter_request,
};
pub use crate::native::task_result_transport::{DynamicFilterRead, DynamicFilterReadError};
pub use crate::native::task_transport::wire::{
    AckAddress, AckPayloadProjection, AckTerminationDiagnostic, AcknowledgementProjection,
    AttemptWireFacts, ChannelFailureClass, CoveredEventValidationError, CoveredRequestedVersions,
    EncodedMethodBatch, EncodedMethodRequest, EstablishWireContent, SentOperation,
    classify_apply_status, classify_channel_failure, decode_ack, decode_ack_owned,
    decode_covered_status_event, decode_operation_receipts,
    encode_establish_participation_operation, encode_method_batches, encode_operation,
    encode_participation_operation, encode_task_domains, project_ack_payload,
    project_ack_payload_owned, project_acknowledgement, project_acknowledgement_owned,
    subscription_rejection_is_fatal, validate_covered_event_participation,
};
pub use crate::runtime_filter::feedback::TaskRuntimeFilterFeedback;
pub use crate::task_execution::intent::{AckPayload, OperationAcknowledgement, OperationIntent};
pub use crate::task_execution::operation::{
    IngressRejection, OperationDispatchResult, WorkerReceiptOutcome,
};

pub use crate::task_execution::creation::CreateTaskIntent;
