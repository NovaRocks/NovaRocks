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
use novarocks_coordination_contract::AttemptActionId;
use novarocks_workload_control::{
    ControlStorageConfig, ResultCapacityConfig, ResultWindowClass, WorkClass, WorkRequest,
    WorkloadConfig, WorkloadControl,
    joint_admission::{JointProgress, JointProgressDriver, ResponsibilityConfig},
};
use std::{
    alloc::Layout,
    num::{NonZeroU64, NonZeroUsize},
    task::Waker,
};

fn drive(driver: &mut JointProgressDriver) {
    for _ in 0..100_000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("original finite WC proposal did not settle");
}
pub(crate) struct Governed {
    control: WorkloadControl,
    driver: JointProgressDriver,
    root: novarocks_workload_control::RootWork,
    window: novarocks_workload_control::ResultWindowGrant,
    listener: std::net::TcpListener,
}
impl Governed {
    pub(crate) fn new() -> Self {
        Self::with_origin(None)
    }

    pub(crate) fn with_origin(
        endpoint: Option<&novarocks_execution_contract::RuntimeEndpoint>,
    ) -> Self {
        // Use the existing Native author's actual proposal-observer geometry;
        // this fixture creates no alternate WC grant or process quota wallet.
        let source =
            crate::native::task_transport::NativeTaskOperationSink::proposal_wait_source_geometry(
                novarocks_task_codec::TransportBudget::DEFAULT,
            )
            .unwrap();
        let backing = Layout::array::<u8>(source.resident_bytes().unwrap()).unwrap();
        let storage = ControlStorageConfig::new(8, &[]).with_process_source_backing(
            novarocks_workload_control::PollRoleSourceBacking::from_layouts(&[], &[backing])
                .unwrap(),
        );
        let control = WorkloadControl::try_new_counted(WorkloadConfig {
            root_limit: 8,
            business_limit: 8,
            query_concurrency_limit: 8,
            scope_records_limit: 8,
            ..WorkloadConfig::default()
        })
        .unwrap()
        .owner;
        let (_, mut driver) = control
            .configure_responsibility(ResponsibilityConfig {
                work_per_turn: NonZeroUsize::new(31).unwrap(),
            })
            .unwrap();
        control.configure_control_storage(storage).unwrap();
        let capacity = control
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        control.mark_ready().unwrap();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let scope = root.owner.scope();
        // A first control binding must contain the actual source dependency.
        // Use the same Native origin author as governed_wire; the listener
        // retains this exact fixture endpoint through its real cleanup.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let local_endpoint = novarocks_execution_contract::RuntimeEndpoint::from_socket_addr(
            listener.local_addr().unwrap(),
        );
        let origin = novarocks_native_adapter::FrontendNativeTransport::plaintext()
            .selected_network_origin(endpoint.unwrap_or(&local_endpoint).native_endpoint())
            .unwrap()
            .into_owned();
        let mut binding = scope.begin_control_binding().unwrap();
        binding
            .push_dependency(&novarocks_resource_contract::SharedDependency::Network(
                origin,
            ))
            .unwrap();
        binding.end_change().unwrap();
        let mut pending = binding.submit().unwrap();
        drive(&mut driver);
        let mut ready = match Pin::new(&mut pending).poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(Ok(ready)) => ready,
            _ => panic!("original selected Native dependency binding grants"),
        };
        ready
            .adjustment
            .as_mut()
            .unwrap()
            .install_control_scope_in(&scope)
            .unwrap()
            .notify();
        drop((ready, pending));
        drive(&mut driver);
        let window = capacity
            .try_acquire(&scope, ResultWindowClass::Client)
            .unwrap();
        Self {
            control,
            driver,
            root,
            window,
            listener,
        }
    }
    pub(crate) fn authority(&self) -> NativeAsyncActionAuthority {
        self.window
            .retain_alias()
            .native_action_authority()
            .unwrap()
    }
    pub(crate) fn progress(&mut self) {
        drive(&mut self.driver);
    }
    pub(crate) fn admit(&mut self, key: SendKey, new_backing: usize) -> OriginalQueuedInput {
        let quote = crate::task_execution::host::transport::queued_input_quote(
            new_backing,
            crate::task_execution::host::transport::SourceLayouts { original: &[] },
        )
        .unwrap();
        let mut pre =
            Box::pin(AdmitQueuedInput::new(key, &self.authority(), false, quote).unwrap());
        for _ in 0..100_000 {
            match pre.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
                Poll::Ready(result) => return result.unwrap(),
                Poll::Pending => drive(&mut self.driver),
            }
        }
        panic!("original queued input did not grant");
    }
    pub(crate) fn finish(self) -> bool {
        let Self {
            control,
            mut driver,
            root,
            window,
            listener,
        } = self;
        drop((window, listener));
        root.owner.complete_after_terminal_cancel_settled();
        root.business.release();
        control.close_admission();
        drive(&mut driver);
        control.shutdown().is_ok()
    }
}
fn key(operation: TaskOperationId, generation: u64) -> SendKey {
    SendKey {
        operation,
        generation: SendGeneration::new(AttemptActionId::new(NonZeroU64::new(generation).unwrap())),
    }
}

#[test]
fn new_generation_admits_a_fresh_queue_while_original_payload_retention_survives() {
    let mut governed = Governed::new();
    let operation = TaskOperationId::new_v7();
    let first_key = key(operation, 1);
    let second_key = key(operation, 2);
    let first = governed.admit(first_key, 8192);
    let original_payload = Box::new([37u8; 8192]);
    let original_pin = first.retain_payload().unwrap();
    let original_address = original_payload.as_ptr() as usize;
    let second = governed.admit(second_key, 0);
    let observed = (first.key(), second.key());
    // These two inputs have never acquired physical reservations. Their actual
    // unused queue exits leave the independent first payload pin intact.
    drop((first, second));
    drive(&mut governed.driver);
    let retained_original = original_payload.as_ptr() as usize == original_address
        && original_payload.iter().all(|byte| *byte == 37);
    drop(original_payload);
    drop(original_pin);
    let clean = governed.finish();
    assert_eq!(observed, (first_key, second_key));
    assert_ne!(observed.0.generation, observed.1.generation);
    assert!(retained_original);
    assert!(
        clean,
        "both original WC records and payload pin actually exited"
    );
}

#[test]
fn cancelling_pending_original_proposal_releases_its_actual_observer_and_queue_claim() {
    let mut governed = Governed::new();
    let authority = governed.authority();
    let quote = crate::task_execution::host::transport::queued_input_quote(
        0,
        crate::task_execution::host::transport::SourceLayouts { original: &[] },
    )
    .unwrap();
    let mut pending = Box::pin(
        AdmitQueuedInput::new(key(TaskOperationId::new_v7(), 1), &authority, false, quote).unwrap(),
    );
    let observed = pending
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending();
    drop(pending);
    drive(&mut governed.driver);
    drop(authority);
    let clean = governed.finish();
    assert!(
        observed,
        "the actual original proposal was pending before cancellation"
    );
    assert!(
        clean,
        "same WC proposal and slot observer have genuinely exited"
    );
}
