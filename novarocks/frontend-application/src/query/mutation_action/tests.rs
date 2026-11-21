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

use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug)]
struct SourceExitProbe(Arc<AtomicBool>);
impl novarocks_failure_contract::SourceRetentionCapability for SourceExitProbe {}
impl Drop for SourceExitProbe {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
fn retained_compile_failure(
    message: &str,
    probe: Arc<AtomicBool>,
) -> novarocks_failure_contract::SharedFailureCause {
    let source = novarocks_failure_contract::SharedFailureCause::new(
        novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability(message.into()),
    );
    source.retain_source_lifetime(Box::new(SourceExitProbe(probe)));
    source
}

struct ReadyErrorThenPanicDrop(Option<DmlExecutionError>);
impl Future for ReadyErrorThenPanicDrop {
    type Output = Result<PhaseValue, DmlExecutionError>;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Err(self.0.take().unwrap()))
    }
}
impl Drop for ReadyErrorThenPanicDrop {
    fn drop(&mut self) {
        panic!("scripted original preparation Future Drop");
    }
}
#[test]
fn actual_preparation_stores_typed_return_before_hostile_future_drop() {
    let exited = Arc::new(AtomicBool::new(false));
    let probe = Arc::clone(&exited);
    let failure = retained_compile_failure("original preparation refused", probe);
    let mut preparation = PhasePreparation {
        future: Some(Box::pin(ReadyErrorThenPanicDrop(Some(
            DmlExecutionError::Compilation(failure),
        )))),
        result: None,
        busy: false,
    };
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || preparation.poll_returned(&mut cx)
        ))
        .is_err()
    );
    assert!(
        preparation.busy,
        "the actual preparation owner is poisoned by child Drop"
    );
    let Some(Err(DmlExecutionError::Compilation(original))) = preparation.result.as_ref() else {
        panic!("original typed return must be stable");
    };
    assert!(original.detail().contains("original preparation refused"));
    assert!(
        !exited.load(Ordering::SeqCst),
        "client projection must not retire the original cause"
    );
    assert!(
        preparation.poll_returned(&mut cx).is_pending(),
        "a poisoned original F is never repolled"
    );
    drop(preparation);
    assert!(exited.load(Ordering::SeqCst));
}
#[test]
fn actual_preparation_refusal_keeps_original_source_through_client_projection() {
    let exited = Arc::new(AtomicBool::new(false));
    let probe = Arc::clone(&exited);
    let failure = retained_compile_failure("same preparation failure", probe);
    let mut preparation = PhasePreparation {
        future: Some(Box::pin(async move {
            Err(DmlExecutionError::Compilation(failure))
        })),
        result: None,
        busy: false,
    };
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(preparation.poll_returned(&mut cx).is_ready());
    let original = preparation.result.as_ref().unwrap().as_ref().err().unwrap();
    let client = project_failure(original, "UPDATE exact_admitted_source SET x = 1");
    assert_eq!(
        client.message(),
        "[UnsupportedCompiledShape] pure compilation failed in its source domain"
    );
    let Some(novarocks_type_contract::CompileControlError::Source(projected)) =
        client.compile_control_error()
    else {
        panic!("client diagnostic must retain the same original source");
    };
    let DmlExecutionError::Compilation(retained) = original else {
        panic!("preparation must retain its original compilation cause");
    };
    assert_eq!(&projected, retained);
    assert!(projected.detail().contains("same preparation failure"));
    drop(projected);
    assert!(!exited.load(Ordering::SeqCst));
    drop(preparation);
    assert!(
        !exited.load(Ordering::SeqCst),
        "the client still retains the original compile cause"
    );
    drop(client);
    assert!(exited.load(Ordering::SeqCst));
}

mod empty_write;
