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

// Included as a child of attempt_handoff; all product payloads stay concrete.
use crate::native::task_result_transport::{FinalTaskInfoRead, TaskResultTransport};
use novarocks_execution_contract::{OperationOutcome, TaskIdentity};

struct UnavailableFinalInfo;
impl TaskResultTransport for UnavailableFinalInfo {
    fn final_task_info(
        &self,
        _: TaskIdentity,
    ) -> futures::future::BoxFuture<'_, Result<FinalTaskInfoRead, String>> {
        Box::pin(async { Ok(FinalTaskInfoRead::Unavailable(OperationOutcome::Gone)) })
    }
}

#[path = "attempt_handoff_actual_fixture.rs"]
mod actual_fixture;

#[tokio::test(flavor = "current_thread")]
async fn actual_staged_bundle_waits_for_drive_then_claims_once_and_reaches_real_ready() {
    actual_fixture::staged_original_bundle().await;
}

#[tokio::test(flavor = "current_thread")]
async fn claimed_old_bundle_survives_replacement_and_a_stale_source_cannot_take_the_new_offer() {
    actual_fixture::claimed_original_bundle_across_new_source().await;
}

// Child tests reuse this original private fixture instead of cloning a driver.
#[path = "profile_component_tests.rs"]
mod profile_consumer_tests;
