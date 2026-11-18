// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::{CompletionEvent, ControlCut, run_arrival_order};

#[tokio::test(flavor = "current_thread")]
async fn original_task_failure_vetoes_confirmation_after_paid_arrival_and_end() {
    run_arrival_order(
        &[
            CompletionEvent::RootRows,
            CompletionEvent::RootEnd,
            CompletionEvent::TaskFailure,
        ],
        true,
        ControlCut::Arrival,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn original_cancellation_vetoes_confirmation_after_paid_arrival() {
    run_arrival_order(
        &[CompletionEvent::RootRows, CompletionEvent::Cancelled],
        true,
        ControlCut::Arrival,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn original_deadline_vetoes_confirmation_after_paid_arrival() {
    run_arrival_order(
        &[CompletionEvent::RootRows, CompletionEvent::DeadlineExpired],
        true,
        ControlCut::Arrival,
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn provider_arrival_refusal_keeps_its_typed_source_and_vetoes_confirmation() {
    run_arrival_order(
        &[CompletionEvent::ArrivalRefusal],
        true,
        ControlCut::Arrival,
    )
    .await;
}
