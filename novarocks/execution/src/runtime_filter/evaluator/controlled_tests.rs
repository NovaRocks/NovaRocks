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

//! Local declared-artifact tests for the controlled evaluator policy. The
//! literal mask/effect oracle is independent of the row decoder; this is not
//! a Worker ordering oracle, a Native lane or GLOBAL/physical-peak evidence.

use super::*;
use crate::runtime::tracked_state_allocator::expression_allocation_host;
use crate::runtime::{mem_tracker::MemTracker, task::ExecutionFailureCause};
use crate::runtime_filter::RuntimeFilterFailure;
use arrow::array::StringArray;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

struct Control {
    calls: AtomicUsize,
    refuse_at: Option<usize>,
    cause: KernelFailure,
}
impl Control {
    fn running() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            refuse_at: None,
            cause: KernelFailure::Cancelled,
        }
    }
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        let ordinal = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.refuse_at == Some(ordinal) {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("row evaluation never waits");
    }
}

#[derive(Clone)]
enum Verdict {
    Literals,
    Artifact(RuntimeFilterArtifactQueryError),
    Kernel(KernelFailure),
}
struct Query {
    installed: bool,
    verdict: Verdict,
    calls: AtomicUsize,
}
impl Query {
    fn literals() -> Self {
        Self {
            installed: true,
            verdict: Verdict::Literals,
            calls: AtomicUsize::new(0),
        }
    }
    fn verdict(&self) -> Result<(), RuntimeFilterControlledQueryError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match &self.verdict {
            Verdict::Literals => Ok(()),
            Verdict::Artifact(cause) => Err((*cause).into()),
            Verdict::Kernel(cause) => Err(cause.clone().into()),
        }
    }
}
impl RuntimeFilterArtifactQuery for Query {
    fn data_type(&self) -> &DataType {
        &DataType::Utf8
    }
    fn controlled_scalar_query(&self) -> Option<&dyn RuntimeFilterControlledScalarQuery> {
        self.installed.then_some(self)
    }
    fn matches_null(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
        panic!("the controlled evaluator must not fall back to the old null port");
    }
    fn has_non_null_matches(&self) -> Result<bool, RuntimeFilterArtifactQueryError> {
        panic!("the controlled row policy needs no uncontrolled existence query");
    }
    fn non_null_value_may_match(
        &self,
        _: RuntimeFilterScalarRef<'_>,
    ) -> Result<bool, RuntimeFilterArtifactQueryError> {
        panic!("the controlled evaluator must not fall back to the old scalar port");
    }
    fn non_null_range_may_match(
        &self,
        _: &ConnectorScalarValue,
        _: &ConnectorScalarValue,
    ) -> Result<bool, RuntimeFilterArtifactQueryError> {
        panic!("row evaluation must not query a connector range");
    }
}
impl RuntimeFilterControlledScalarQuery for Query {
    fn matches_null_controlled(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<bool, RuntimeFilterControlledQueryError> {
        control.checkpoint(1)?;
        self.verdict()?;
        Ok(true)
    }
    fn non_null_value_may_match_controlled(
        &self,
        value: RuntimeFilterScalarRef<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<bool, RuntimeFilterControlledQueryError> {
        control.checkpoint(1)?;
        self.verdict()?;
        let RuntimeFilterScalarRef::Utf8(value) = value else {
            panic!("the original decoder must preserve the declared UTF8 domain")
        };
        // This artifact declares these three literal members, not an ordered
        // comparator. The independent test mask below states the row oracle.
        Ok(matches!(value, "" | "海" | "a\0b"))
    }
}
fn input() -> ArrayRef {
    let original: ArrayRef = Arc::new(StringArray::from(vec![
        Some("unused-prefix"),
        None,
        Some(""),
        Some("海"),
        Some("a\0b"),
        Some("z"),
        Some("港"),
    ]));
    original.slice(1, 6)
}
fn tracker() -> Arc<MemTracker> {
    let tracker = MemTracker::new_root("controlled-row-evaluator");
    tracker.install_limit_once(64 << 20).unwrap();
    tracker
}
fn evaluate(
    query: &Query,
    input: &ArrayRef,
    tracker: &Arc<MemTracker>,
    control: &Control,
) -> ExecutionResult<ControlledRowOutcome> {
    evaluate_rows_controlled(
        RuntimeFilterBindingId::new(7),
        LogicalVersion::new(3),
        query,
        input,
        expression_allocation_host(tracker.clone()),
        control,
    )
}

#[test]
fn controlled_row_evaluator_uses_only_installed_queries_and_literal_utf8_mask_effect() {
    let tracker = tracker();
    let query = Query::literals();
    let result = evaluate(&query, &input(), &tracker, &Control::running()).unwrap();
    assert_eq!(query.calls.load(Ordering::SeqCst), 6);
    assert_eq!(
        result.outcome().binding_id(),
        RuntimeFilterBindingId::new(7)
    );
    let RuntimeFilterRowEvaluation::Evaluated {
        mask,
        logical_version,
    } = result.outcome().evaluation()
    else {
        panic!("the declared artifact has an evaluated row mask")
    };
    assert_eq!(*logical_version, LogicalVersion::new(3));
    assert_eq!(
        mask.values().iter().collect::<Vec<_>>(),
        vec![true, true, true, true, false, false]
    );
    assert_eq!(mask.null_count(), 0);
    assert_eq!(
        tracker.current(),
        i64::try_from(mask.values().inner().capacity()).unwrap()
    );
    assert!(
        tracker.current() > 0,
        "the actual synchronous mask carrier retains its stock"
    );
    let effect = result
        .effect_controlled(&Control::running())
        .unwrap()
        .unwrap();
    assert_eq!(effect.binding_id(), RuntimeFilterBindingId::new(7));
    assert_eq!(effect.logical_version(), LogicalVersion::new(3));
    assert_eq!(effect.input_rows(), 6);
    assert_eq!(effect.output_rows(), 4);
    drop(result);
    assert_eq!(tracker.current(), 0);
    // The copied typed effect is a fact; it retains no mask or paying host.
    assert_eq!(effect.output_rows(), 4);
}

#[test]
fn controlled_row_evaluator_refuses_a_missing_port_before_all_null_rows_or_callbacks() {
    let tracker = tracker();
    let query = Query {
        installed: false,
        ..Query::literals()
    };
    let control = Control::running();
    let input: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>; 3]));
    let error = evaluate(&query, &input, &tracker, &control).unwrap_err();
    assert!(
        matches!(error.cause(), ExecutionFailureCause::Kernel(KernelFailure::InvalidProgram(detail))
        if detail.message() == "compiled ordered runtime-filter artifact has no controlled scalar query")
    );
    assert_eq!(query.calls.load(Ordering::SeqCst), 0);
    assert_eq!(control.calls.load(Ordering::SeqCst), 0);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn controlled_row_evaluator_keeps_artifact_advisory_and_contract_kernel_failures_distinct() {
    for (cause, reason) in [
        (
            RuntimeFilterArtifactQueryError::Unsupported,
            RuntimeFilterRowNotEvaluatedReason::ArtifactQueryUnsupported,
        ),
        (
            RuntimeFilterArtifactQueryError::ResourceUnavailable,
            RuntimeFilterRowNotEvaluatedReason::ResourceUnavailable,
        ),
    ] {
        let tracker = tracker();
        let query = Query {
            verdict: Verdict::Artifact(cause),
            ..Query::literals()
        };
        let result = evaluate(&query, &input(), &tracker, &Control::running()).unwrap();
        assert_eq!(
            result.outcome().evaluation(),
            &RuntimeFilterRowEvaluation::NotEvaluated {
                reason,
                observed_version: LogicalVersion::new(3),
            }
        );
        assert_eq!(result.effect_controlled(&Control::running()).unwrap(), None);
        assert_eq!(query.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            tracker.current(),
            0,
            "advisory outcomes retain no partial mask"
        );
    }
    let tracker = tracker();
    let query = Query {
        verdict: Verdict::Artifact(RuntimeFilterArtifactQueryError::ContractViolation),
        ..Query::literals()
    };
    let error = evaluate(&query, &input(), &tracker, &Control::running()).unwrap_err();
    let ExecutionFailureCause::RuntimeFilter(RuntimeFilterFailure::Contract(cause)) = error.cause()
    else {
        panic!("a rejected immutable artifact must preserve its typed contract cause")
    };
    assert_eq!(
        cause.kind(),
        RuntimeFilterContractViolationKind::ContractMismatch
    );
    assert_eq!(
        cause.detail(),
        "runtime-filter artifact query rejected its immutable artifact"
    );
    assert_eq!(tracker.current(), 0);
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        let query = Query {
            verdict: Verdict::Kernel(cause.clone()),
            ..Query::literals()
        };
        let error = evaluate(&query, &input(), &tracker, &Control::running()).unwrap_err();
        assert_eq!(error.cause(), &ExecutionFailureCause::Kernel(cause));
        assert_eq!(query.calls.load(Ordering::SeqCst), 1);
        assert_eq!(tracker.current(), 0);
    }
}

#[test]
fn controlled_row_evaluator_rolls_back_early_and_late_control_refusal() {
    let tracker = tracker();
    let successful = Control::running();
    drop(evaluate(&Query::literals(), &input(), &tracker, &successful).unwrap());
    let calls = successful.calls.load(Ordering::SeqCst);
    assert!(calls > 1);
    assert_eq!(tracker.current(), 0);
    for cause in [KernelFailure::Cancelled, KernelFailure::DeadlineExceeded] {
        for ordinal in [0, calls - 1] {
            let query = Query::literals();
            let control = Control {
                refuse_at: Some(ordinal),
                cause: cause.clone(),
                ..Control::running()
            };
            let error = evaluate(&query, &input(), &tracker, &control).unwrap_err();
            assert_eq!(error.cause(), &ExecutionFailureCause::Kernel(cause.clone()));
            assert_eq!(control.calls.load(Ordering::SeqCst), ordinal + 1);
            assert_eq!(tracker.current(), 0);
        }
    }
}

#[test]
fn controlled_row_evaluator_effect_refusal_returns_no_fact_and_preserves_mask_stock() {
    let tracker = tracker();
    let result = evaluate(&Query::literals(), &input(), &tracker, &Control::running()).unwrap();
    let paid = tracker.current();
    let observation = Control::running();
    assert!(result.effect_controlled(&observation).unwrap().is_some());
    let calls = observation.calls.load(Ordering::SeqCst);
    assert!(calls > 1);
    for ordinal in [0, calls - 1] {
        let control = Control {
            refuse_at: Some(ordinal),
            cause: KernelFailure::DeadlineExceeded,
            ..Control::running()
        };
        let effect = result.effect_controlled(&control);
        assert_eq!(effect, Err(KernelFailure::DeadlineExceeded));
        assert_eq!(control.calls.load(Ordering::SeqCst), ordinal + 1);
        assert_eq!(tracker.current(), paid);
    }
    drop(result);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn controlled_row_evaluator_refuses_a_real_positive_host_limit_as_fatal_resource() {
    let tracker = MemTracker::new_root("controlled-row-evaluator-hard-limit");
    tracker.install_limit_once(1).unwrap();
    let query = Query::literals();
    let control = Control::running();
    let error = evaluate(&query, &input(), &tracker, &control).unwrap_err();
    crate::runtime_filter::assert_real_tracker_capacity(&error, 1);
    assert_eq!(query.calls.load(Ordering::SeqCst), 0);
    assert_eq!(tracker.current(), 0);
}
