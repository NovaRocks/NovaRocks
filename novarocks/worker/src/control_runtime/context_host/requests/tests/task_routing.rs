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

//! Task routing retains the same Context-owned Task and original request
//! through admission closure and actual unused request retirement.

use super::*;
use crate::control_runtime::{
    actors::tests::task, causal::CausalError, context_host::ContextHostError,
};

#[test]
fn unknown_task_request_is_refused_without_a_direct_context_request_fallback() {
    let f = original_context(1, StorageNeed::default());
    let ready = ready(&f, 1);
    let missing = task(f.identity, 1);
    let refusal = f
        .service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .admit_task_request(
            &ready,
            missing,
            InputClass::Ordinary,
            operation(101),
            MonotonicInstant::from_origin(Duration::from_secs(5)),
        )
        .err()
        .expect("the actual Context owns no such Task");
    assert!(matches!(refusal.error, ContextRequestError::UnknownTask));
    assert!(refusal.partial.is_none());
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 0);
    assert_eq!(ready.registered(), 0);
    finish(f);
}

#[test]
fn closed_task_admission_keeps_the_original_request_routable_until_unused_retirement() {
    let (mut f, task) = live_loading_task_fixture();
    let ready = ready(&f, 1);
    let (work, handle) = f
        .service
        .context(f.identity)
        .unwrap()
        .lock()
        .unwrap()
        .admit_task_request(
            &ready,
            task,
            InputClass::Ordinary,
            operation(102),
            MonotonicInstant::from_origin(Duration::from_secs(5)),
        )
        .unwrap();
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
    assert_eq!(ready.registered(), 1);
    let host_address = f.service.context(f.identity).unwrap();
    let mut host = host_address.lock().unwrap();
    host.close_task_request_admission(task).unwrap();
    host.close_task_request_admission(task).unwrap();
    let refusal = host
        .admit_task_request(
            &ready,
            task,
            InputClass::Control,
            operation(103),
            MonotonicInstant::from_origin(Duration::from_secs(5)),
        )
        .err()
        .expect("closing new admission must preserve the original request");
    assert!(matches!(
        refusal.error,
        ContextRequestError::Causal(CausalError::RequestAdmissionClosed)
    ));
    assert!(refusal.partial.is_none());
    let removal = host
        .take_task(task)
        .err()
        .expect("the same Task still owns the admitted request cut");
    assert!(matches!(removal, ContextHostError::TaskStillOwned));
    assert!(host.task(task).is_some());
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 1);
    assert_eq!(ready.registered(), 1);

    // This exact request has never submitted a command or started Native
    // execution. Ending its real external wait lets its original sender and
    // unused author/source holders retire through the actual host route.
    drop(host);
    drop(handle);
    assert!(advance(&mut f, work));
    assert_eq!(f.ledger.used(Dimension::Requests.id()), 0);
    assert_eq!(ready.registered(), 0);
    assert!(
        f.service
            .context(f.identity)
            .unwrap()
            .lock()
            .unwrap()
            .task(task)
            .is_some()
    );
    assert!(
        matches!(
            f.service
                .context(f.identity)
                .unwrap()
                .lock()
                .unwrap()
                .take_task(task),
            Err(ContextHostError::TaskStillOwned)
        ),
        "the original Source/lifetime publication inventory remains owned after R exits"
    );
    finish_task_fixture(f, task);
}
