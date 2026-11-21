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
use crate::api::{LogicalNativeOpenRequest, NativeLogicalExecutionSeed};
use crate::preparation::{
    ExecutionResourceRequirements, FrozenCostEstimate, FrozenEstimateUnknownReason,
    FrozenExecutionDescription, OutputContract,
};
use novarocks_coordination_contract::{ExecutionEffect, RecoveryMode};

#[tokio::test]
async fn streamed_cut_retains_original_producer_after_preparation_exits() {
    let (control, mut driver, work, stage, payload, binding) = owned_consumer_computation();
    let scope = work.owner.scope();
    let cancellation = scope.cancellation().unwrap();
    let plan = crate::completed_plan_fixture::completed_noop_plan([65; 16]);
    let description = Arc::new(
        FrozenExecutionDescription::for_completed_plan(
            crate::api::QueryExecutionKind::Maintenance,
            plan.candidate().clone(),
            Vec::new(),
            OutputContract::CompletionOnly,
            ExecutionEffect::None,
            RecoveryMode::NoRecovery,
            Vec::new(),
            FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
            ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
        )
        .unwrap(),
    );
    let drops = Arc::new(AtomicUsize::new(0));
    let seed = NativeLogicalExecutionSeed::issue(
        description.plan_identity(),
        None,
        RetiringOriginalPreparation {
            drops: drops.clone(),
            _window: binding.try_window_alias().unwrap(),
        },
    );
    let (open, accepted) = LogicalNativeOpenRequest::issue(
        execution(1),
        description,
        scope.id(),
        cancellation.clone(),
        seed,
    );
    let mut session = accepted.accept(open.bind().unwrap()).unwrap();
    let mut capacity = Some(binding.try_clone().unwrap());
    let streamed_computation = capacity.as_ref().unwrap().streamed_computation_producer();
    session.set_result_capacity_in(&mut capacity);
    let body = Arc::new(AsyncMutex::new(OriginalNativePreparation {
        session: Some(session),
        streamed_computation,
        ..OriginalNativePreparation::default()
    }));
    let mut registry = crate::coordination::LogicalExecutionRuntimeRegistry::new();
    let handle = registry.handle();
    let mut reservation = handle.reserve(execution(1), vec![]).unwrap();
    let registration = reservation.runtime_registration();
    let initial = AttemptActivationIdentity::from_parts(
        reservation.actor_instance().get(),
        execution(1),
        activation().generation(),
    );
    let native = NativeExecutor::new(
        None,
        Arc::new(UnboundInstalledAttemptPort),
        body.clone(),
        Weak::new(),
        handle,
        registration,
        initial,
        Arc::new(schedule()),
        FrontendProcessId::try_from_bytes(
            OperationIds::new(IdSeed::new(93, 100)).issue().to_bytes(),
        )
        .unwrap(),
        Arc::new(FixedClock(MonotonicInstant::ORIGIN)),
        cancellation,
    );
    let before = native.streamed_computation_cut_handle().await.unwrap();
    assert!(before.same_source(&binding.streamed_computation_cut_handle().unwrap()));
    assert_eq!(control.snapshot().result_windows.held_positions[2], 1);
    assert!(matches!(
        native.execute(NativeEffect::RetirePreparation).await,
        NativeFact::Applied
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let after = native
        .streamed_computation_cut_handle()
        .await
        .expect("the same producer must remain reachable after actual preparation retirement");
    assert!(before.same_source(&after));
    assert_eq!(control.snapshot().result_windows.held_positions[2], 1);

    drop((native, body, binding, reservation));
    registry
        .shutdown_until(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    drop((stage, payload));
    drive_paid_native_fixture(&mut driver);
    work.business.release();
    work.owner.complete();
    assert_eq!(control.snapshot().result_windows.held_positions[2], 0);
}
