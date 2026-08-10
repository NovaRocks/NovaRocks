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
    pub(crate) fn new(
        op_name: &'static str,
        stage_producer_counts: Vec<usize>,
    ) -> Result<Self, String> {
        if stage_producer_counts.len() < 2 {
            return Err(format!(
                "{} expects at least 2 inputs, got {}",
                op_name,
                stage_producer_counts.len()
            ));
        }
        let stage_total = stage_producer_counts.len();
        let remaining_producers = stage_producer_counts
            .into_iter()
            .map(|n| AtomicUsize::new(n.max(1)))
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

    pub(crate) fn producer_finished(&self, stage: usize) -> Result<(), String> {
        let remaining = self
            .inner
            .remaining_producers
            .get(stage)
            .ok_or_else(|| format!("set-op stage out of bounds: stage={}", stage))?;
        let prev = remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                if v == 0 { None } else { Some(v - 1) }
            })
            .unwrap_or(0);
        if prev == 1 {
            let next = stage + 1;
            self.inner.current_stage.store(next, Ordering::Release);
            let notify = self.inner.observable.defer_notify();
            notify.arm();
        }
        Ok(())
    }
}
