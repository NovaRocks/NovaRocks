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

use super::*;
use crate::execution_error::ExecutionFailurePresentation;
use novarocks_coordination_contract::{ExecutionFailureSite, QueryFailureReason};
use novarocks_execution_contract::failure::FailureProjection;

#[tokio::test]
async fn count_only_consumer_refuses_client_rows_frontier_before_any_read_or_ack() {
    // Both bindings retain the SAME legitimately admitted Internal window.
    // The positive control rules out scope/class failure as the negative's cause.
    let OriginalWindow {
        _control: control,
        _work: work,
        window,
    } = original_window(RootOutputKind::CountOnly);
    let scope = work.owner.scope();
    let capacity = scope.result_capacity().unwrap();
    let (consumer, stream) = original_consumer(RootOutputKind::CountOnly).await;
    let port = Port::new(Vec::new());
    let original = OriginalWindow {
        _control: control,
        _work: work,
        window,
    };
    let healthy = binding(&original, port.clone(), RootOutputKind::CountOnly);
    // binding() authors a fresh backend identity on every call. Reuse the
    // positive control's exact Root so ONLY the frozen carrier differs.
    let mismatched = RootRelayBinding {
        port: port.clone(),
        frontier: crate::coordination::RootRelayFrontier::new(
            healthy.frontier.root(),
            RootProfileId::V1,
            RootOutputKind::ClientRows,
            Some(profile()),
        )
        .unwrap(),
        window: original.window.retain_alias(),
        max_wait: healthy.max_wait,
    };
    let healthy_accepted = validate_binding(&scope, &consumer, activation(1), &healthy).is_ok();
    let only_carrier_differs = healthy.frontier.root() == mismatched.frontier.root()
        && healthy.frontier.root().query_execution_id() == activation(1).execution()
        && healthy.window.is_for_scope(&scope)
        && mismatched.window.is_for_scope(&scope)
        && healthy.window.class() == ResultWindowClass::Internal
        && mismatched.window.class() == ResultWindowClass::Internal;
    let refusal = validate_binding(&scope, &consumer, activation(1), &mismatched).err();
    let requests = port.requests();
    drop((healthy, mismatched, consumer, stream));
    let OriginalWindow {
        _control: control,
        _work: work,
        window,
    } = original;
    drop(window);
    work.business.release();
    work.owner.complete();
    scope.wait_released().await;
    let held_positions = capacity.snapshot().held_positions;
    control.close_admission();
    assert!(control.shutdown().is_ok());

    assert!(healthy_accepted && only_carrier_differs);
    let refusal = refusal.expect("the frozen CountOnly consumer cannot accept ClientRows");
    let input = refusal.execution_failure().unwrap();
    assert_eq!(
        input.fact(),
        QueryFailureReason::ResultProtocol.failure_fact()
    );
    assert_eq!(input.site(), ExecutionFailureSite::Coordinator);
    assert_eq!(
        (input.descriptor().errno, input.descriptor().sqlstate),
        (1105, "HY000")
    );
    assert_eq!(
        refusal,
        protocol("relay identity differs from its original root, carrier or window")
    );
    assert!(
        requests.is_empty(),
        "entry refusal cannot dispatch a read or ACK"
    );
    assert_eq!(held_positions, [0; 4]);
}
