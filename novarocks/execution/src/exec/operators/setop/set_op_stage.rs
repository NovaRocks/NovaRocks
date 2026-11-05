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
//! Set-operation stage controller.
//!
//! Responsibilities:
//! - Tracks per-stage completion for multi-input set operations such as EXCEPT and INTERSECT.
//! - Provides deterministic stage gating semantics for shared/source operators.
//!
//! Key exported interfaces:
//! - Types: `SetOpStageController`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::exec::pipeline::schedule::observer::Observable;

#[derive(Clone)]
/// Controller tracking stage completion boundaries for multi-input set operations.
pub(crate) struct SetOpStageController {
    inner: Arc<SetOpStageControllerInner>,
}

struct SetOpStageControllerInner {
    current_stage: AtomicUsize,
    remaining_producers: Vec<AtomicUsize>,
    stage_total: usize,
    observable: Arc<Observable>,
}

impl SetOpStageController {
    #[cfg(test)]
    pub(crate) fn new(
        op_name: &'static str,
        stage_producer_counts: Vec<usize>,
    ) -> crate::runtime::task::ExecutionResult<Self> {
        let node_failure = |detail: std::fmt::Arguments<'_>| {
            let detail = novarocks_failure_contract::BoundedDetail::truncating_display(detail);
            crate::exec::fragment::error::ExecPlanBuildError::new(
                crate::exec::fragment::error::ExecPlanInvariant::Node,
                detail.as_str(),
            )
            .into()
        };
        if stage_producer_counts.len() < 2 {
            return Err(node_failure(format_args!(
                "{} expects at least 2 inputs, got {}",
                op_name,
                stage_producer_counts.len()
            )));
        }
        if let Some(stage) = stage_producer_counts.iter().position(|count| *count == 0) {
            return Err(node_failure(format_args!(
                "{op_name} stage {stage} has no declared producer"
            )));
        }
        let stage_total = stage_producer_counts.len();
        let remaining_producers = stage_producer_counts
            .into_iter()
            .map(AtomicUsize::new)
            .collect::<Vec<_>>();
        Ok(Self {
            inner: Arc::new(SetOpStageControllerInner {
                current_stage: AtomicUsize::new(0),
                remaining_producers,
                stage_total,
                observable: Arc::new(Observable::new()),
            }),
        })
    }

    pub(crate) fn stage_total(&self) -> usize {
        self.inner.stage_total
    }

    pub(crate) fn current_stage(&self) -> usize {
        self.inner.current_stage.load(Ordering::Acquire)
    }

    pub(crate) fn is_stage_ready(&self, stage: usize) -> bool {
        self.current_stage() >= stage
    }

    pub(crate) fn observable(&self) -> Arc<Observable> {
        Arc::clone(&self.inner.observable)
    }

    pub(crate) fn producer_finished(&self, stage: usize) -> Result<(), StageCompletionFailure> {
        let remaining = self.inner.remaining_producers.get(stage).ok_or(
            StageCompletionFailure::InvalidStage {
                stage,
                total: self.inner.stage_total,
            },
        )?;
        let prev = remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_sub(1)
            })
            .map_err(|_| StageCompletionFailure::DuplicateCompletion { stage })?;
        if prev == 1 {
            // Empty later branches can finish before an earlier build. Their
            // completion is remembered, but only a contiguous finished prefix
            // authorizes the next stage or the source's final EOS.
            let mut current = self.inner.current_stage.load(Ordering::Acquire);
            let mut advanced = false;
            while current < self.inner.stage_total
                && self.inner.remaining_producers[current].load(Ordering::Acquire) == 0
            {
                match self.inner.current_stage.compare_exchange(
                    current,
                    current + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        current += 1;
                        advanced = true;
                    }
                    Err(observed) => current = observed,
                }
            }
            if advanced {
                self.inner.observable.notify_observers();
            }
        }
        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum StageCompletionFailure {
    InvalidStage { stage: usize, total: usize },
    DuplicateCompletion { stage: usize },
}
impl std::fmt::Display for StageCompletionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidStage { stage, total } => {
                write!(f, "set-op stage {stage} is outside {total} declared inputs")
            }
            Self::DuplicateCompletion { stage } => write!(
                f,
                "set-op stage {stage} has no outstanding producer completion"
            ),
        }
    }
}
impl std::error::Error for StageCompletionFailure {}
impl novarocks_failure_contract::FailureProjection for StageCompletionFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::task::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        let reason = match self {
            Self::InvalidStage { .. } => ExecutionReason::SetInvalidStage,
            Self::DuplicateCompletion { .. } => ExecutionReason::SetDuplicateCompletion,
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn zero_producer_counts_are_rejected_before_creating_a_stage() {
        assert!(SetOpStageController::new("EXCEPT", vec![0, 1]).is_err());
        assert!(SetOpStageController::new("EXCEPT", vec![1, 0]).is_err());
    }
    #[test]
    fn every_completion_order_advances_only_the_contiguous_finished_prefix() {
        for order in [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ] {
            let controller = SetOpStageController::new("EXCEPT", vec![1, 1, 1]).unwrap();
            let mut finished = [false; 3];
            for stage in order {
                controller.producer_finished(stage).unwrap();
                finished[stage] = true;
                let expected = finished.iter().take_while(|done| **done).count();
                assert_eq!(
                    controller.current_stage(),
                    expected,
                    "completion order {order:?}"
                );
            }
            assert_eq!(controller.current_stage(), 3);
            assert_eq!(
                controller.producer_finished(2),
                Err(StageCompletionFailure::DuplicateCompletion { stage: 2 })
            );
            assert_eq!(controller.current_stage(), 3);
        }
    }
    #[test]
    fn the_last_required_producer_releases_all_already_finished_later_stages() {
        let controller = SetOpStageController::new("EXCEPT", vec![2, 1, 1]).unwrap();
        controller.producer_finished(2).unwrap();
        controller.producer_finished(1).unwrap();
        controller.producer_finished(0).unwrap();
        assert_eq!(controller.current_stage(), 0);
        controller.producer_finished(0).unwrap();
        assert_eq!(controller.current_stage(), 3);
        assert!(matches!(
            controller.producer_finished(3),
            Err(StageCompletionFailure::InvalidStage { stage: 3, total: 3 })
        ));
    }
}
