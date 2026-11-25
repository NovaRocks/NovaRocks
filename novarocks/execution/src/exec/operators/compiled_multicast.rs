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

//! A relational multicast shares its root input across independent stream sinks.
//! Partitioning, projection and sender completion remain owned by each child.

// Design: ADR-0269 (docs/adr/ADR-0269-cte-multicast-freezes-exact-branches.md)

use crate::{
    exec::{
        chunk::Chunk,
        pipeline::{
            driver_step::DriverStep,
            input::PreparedInput,
            operator::{
                EncodedValueOccurrence, InputConstructionFactory, InputEncodingPolicy, Operator,
            },
            operator_factory::OperatorFactory,
            shape::fanout::{Fanout, FanoutRouter, RouteStep},
        },
    },
    runtime::task::ExecutionResult,
};

const NAME: &str = "CompiledMulticast";

pub(crate) struct CompiledMulticastFactory {
    branches: Vec<Box<dyn OperatorFactory>>,
}

impl CompiledMulticastFactory {
    pub(crate) fn new(branches: Vec<Box<dyn OperatorFactory>>) -> Self {
        Self { branches }
    }
}

impl OperatorFactory for CompiledMulticastFactory {
    fn name(&self) -> &str {
        NAME
    }

    fn is_sink(&self) -> bool {
        true
    }

    fn bind_pipeline_dop(&self, dop: i32) -> ExecutionResult<()> {
        for branch in &self.branches {
            branch.bind_pipeline_dop(dop)?;
        }
        Ok(())
    }

    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        Box::new(Fanout::new(
            NAME.into(),
            self.branches
                .iter()
                .map(|branch| branch.create(dop, driver_id))
                .collect(),
            MulticastRouter {
                branch_count: self.branches.len(),
            },
            None,
        ))
    }
}

struct MulticastRouter {
    branch_count: usize,
}

struct MulticastCursor {
    chunk: Chunk,
    next_branch: usize,
}

impl InputEncodingPolicy for MulticastRouter {
    fn retains_encoded_path(&self, _: EncodedValueOccurrence<'_>) -> bool {
        // Each child applies its own installed encoding policy.
        true
    }
}

impl FanoutRouter for MulticastRouter {
    type Cursor = MulticastCursor;

    fn begin(
        &mut self,
        input: PreparedInput,
        _: &dyn InputConstructionFactory,
        _: &mut DriverStep<'_>,
    ) -> ExecutionResult<Self::Cursor> {
        let chunk = input.into_uncovered_chunk()?;
        chunk.retain_shared_accounting();
        Ok(MulticastCursor {
            chunk,
            next_branch: 0,
        })
    }

    fn advance(
        &mut self,
        cursor: &mut Self::Cursor,
        _: &dyn InputConstructionFactory,
        _: &mut DriverStep<'_>,
    ) -> ExecutionResult<RouteStep> {
        if cursor.next_branch == self.branch_count {
            return Ok(RouteStep::Finished);
        }
        let branch = cursor.next_branch;
        cursor.next_branch += 1;
        Ok(RouteStep::Input {
            branch,
            chunk: cursor.chunk.clone(),
        })
    }

    fn accepts_branch(&self, _: usize) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        exec::pipeline::{
            input::RawInput,
            operator::{ExitObservation, ExitProgress, FinishWatch, OperatorLifecycle},
            shape::{
                ControlStep, SinkKernel, SinkStep, StepOperator, Transform,
                fanout::test_support::{context, prepare, typed_i32, until_wait_or_input},
            },
            step::{InputResult, NextStep},
        },
        runtime::observable::Observable,
    };
    use arrow::array::Int32Array;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    #[derive(Default)]
    struct BranchFacts {
        observable: Arc<Observable>,
        rows: Mutex<Vec<i32>>,
        withdrawn: AtomicBool,
        may_exit: AtomicBool,
        exit_started: AtomicBool,
        closes: AtomicUsize,
        ends: AtomicUsize,
    }

    impl BranchFacts {
        fn withdraw(&self) {
            self.withdrawn.store(true, Ordering::Release);
            self.observable.notify_observers();
        }

        fn finish_exit(&self) {
            self.may_exit.store(true, Ordering::Release);
            self.observable.notify_observers();
        }
    }

    struct Branch(Arc<BranchFacts>);

    impl OperatorLifecycle for Branch {
        fn name(&self) -> &str {
            "Multicast branch fixture"
        }

        fn begin_exit(&mut self) -> ExecutionResult<()> {
            self.0.exit_started.store(true, Ordering::Release);
            Ok(())
        }

        fn observe_exit(&mut self) -> ExitObservation {
            if self.0.exit_started.load(Ordering::Acquire)
                && !self.0.may_exit.load(Ordering::Acquire)
            {
                ExitObservation::pending(FinishWatch::Notify(self.0.observable.clone()))
            } else {
                ExitObservation::exited()
            }
        }

        fn close(&mut self) -> ExecutionResult<()> {
            assert!(self.0.may_exit.load(Ordering::Acquire));
            assert_eq!(self.0.closes.fetch_add(1, Ordering::AcqRel), 0);
            Ok(())
        }
    }

    impl InputEncodingPolicy for Branch {}

    impl SinkKernel for Branch {
        fn control_observable(&self) -> Option<Arc<Observable>> {
            Some(self.0.observable.clone())
        }

        fn poll_control(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<ControlStep> {
            Ok(if self.0.withdrawn.load(Ordering::Acquire) {
                ControlStep::Withdrawn
            } else {
                ControlStep::Active
            })
        }

        fn consume(
            &mut self,
            _: &mut DriverStep<'_>,
            input: PreparedInput,
        ) -> ExecutionResult<SinkStep> {
            let chunk = input.into_uncovered_chunk()?;
            let values = chunk.columns()[0]
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            self.0
                .rows
                .lock()
                .unwrap()
                .extend(values.values().iter().copied());
            Ok(SinkStep::new(NextStep::NeedInput, chunk.len() as u64))
        }

        fn finish(&mut self, _: &mut DriverStep<'_>) -> ExecutionResult<SinkStep> {
            self.0.ends.fetch_add(1, Ordering::AcqRel);
            self.0.finish_exit();
            Ok(SinkStep::new(NextStep::Finished, 0))
        }
    }

    fn fanout<const N: usize>(facts: &[Arc<BranchFacts>; N]) -> Fanout<MulticastRouter> {
        let branches = facts
            .iter()
            .map(|branch| {
                Box::new(Transform::hydrating(Branch(branch.clone()))) as Box<dyn Operator>
            })
            .collect();
        let mut fanout = Fanout::new(
            NAME.into(),
            branches,
            MulticastRouter { branch_count: N },
            None,
        );
        prepare(&mut fanout);
        assert!(matches!(
            until_wait_or_input(&mut fanout),
            NextStep::NeedInput
        ));
        fanout
    }

    fn send(fanout: &mut Fanout<MulticastRouter>, rows: Vec<i32>) {
        assert!(matches!(
            context(|ctx| fanout.try_push(ctx, RawInput::for_test(typed_i32(rows)))),
            InputResult::Accepted(Ok(_)),
        ));
        assert!(matches!(until_wait_or_input(fanout), NextStep::NeedInput));
    }

    #[test]
    fn withdrawn_branch_exits_while_sibling_keeps_its_input_and_end() {
        let abandoned = Arc::new(BranchFacts::default());
        let surviving = Arc::new(BranchFacts::default());
        let mut fanout = fanout(&[abandoned.clone(), surviving.clone()]);
        send(&mut fanout, vec![3, 5, 3]);
        assert_eq!(abandoned.rows.lock().unwrap().as_slice(), &[3, 5, 3]);
        abandoned.withdraw();
        assert!(matches!(
            context(|ctx| fanout.poll_control(ctx)).unwrap(),
            ControlStep::Runnable,
        ));
        let mut pending = false;
        fanout
            .visit_exits(&mut |exit| pending |= matches!(exit.progress, ExitProgress::Pending(_)));
        assert!(pending);
        assert_eq!(abandoned.closes.load(Ordering::Acquire), 0);

        assert!(matches!(
            until_wait_or_input(&mut fanout),
            NextStep::NeedInput
        ));
        send(&mut fanout, vec![-7, 9]);
        abandoned.finish_exit();
        fanout.visit_exits(&mut |_| {});
        assert_eq!(abandoned.closes.load(Ordering::Acquire), 1);
        assert_eq!(abandoned.rows.lock().unwrap().as_slice(), &[3, 5, 3]);
        assert_eq!(surviving.rows.lock().unwrap().as_slice(), &[3, 5, 3, -7, 9]);

        context(|ctx| fanout.finish(ctx)).unwrap();
        assert!(matches!(
            until_wait_or_input(&mut fanout),
            NextStep::Finished
        ));
        assert!(matches!(
            context(|ctx| fanout.poll_control(ctx)).unwrap(),
            ControlStep::Active,
        ));
        assert_eq!(abandoned.ends.load(Ordering::Acquire), 0);
        assert_eq!(surviving.ends.load(Ordering::Acquire), 1);
        fanout.begin_exit().unwrap();
        fanout.visit_exits(&mut |_| {});
        assert_eq!(surviving.closes.load(Ordering::Acquire), 1);
    }

    #[test]
    fn a_wide_fanout_resumes_its_budgeted_control_and_delivery() {
        let branches: [Arc<BranchFacts>; 65] =
            std::array::from_fn(|_| Arc::new(BranchFacts::default()));
        let mut fanout = fanout(&branches);
        assert!(matches!(
            context(|ctx| fanout.poll_control(ctx)).unwrap(),
            ControlStep::Runnable,
        ));
        assert!(matches!(
            context(|ctx| fanout.drive(ctx)).unwrap().next,
            NextStep::Yield,
        ));
        assert!(matches!(
            context(|ctx| fanout.poll_control(ctx)).unwrap(),
            ControlStep::Active,
        ));
        assert!(matches!(
            until_wait_or_input(&mut fanout),
            NextStep::NeedInput
        ));

        assert!(matches!(
            context(|ctx| fanout.try_push(ctx, RawInput::for_test(typed_i32(vec![2, 7])))),
            InputResult::Accepted(Ok(_)),
        ));
        let mut admitted = false;
        for _ in 0..(branches.len() * 8) {
            if matches!(
                context(|ctx| fanout.drive(ctx)).unwrap().next,
                NextStep::NeedInput
            ) {
                admitted = true;
                break;
            }
        }
        assert!(admitted);
        for branch in &branches {
            assert_eq!(branch.rows.lock().unwrap().as_slice(), &[2, 7]);
        }
    }

    #[test]
    fn all_destinations_withdrawing_retires_upstream_without_end() {
        let first = Arc::new(BranchFacts::default());
        let second = Arc::new(BranchFacts::default());
        let mut fanout = fanout(&[first.clone(), second.clone()]);
        send(&mut fanout, vec![1]);
        first.withdraw();
        second.withdraw();
        assert!(matches!(
            context(|ctx| fanout.poll_control(ctx)).unwrap(),
            ControlStep::Withdrawn,
        ));
        for branch in [&first, &second] {
            assert_eq!(branch.ends.load(Ordering::Acquire), 0);
            branch.finish_exit();
        }
        fanout.visit_exits(&mut |_| {});
        assert_eq!(first.closes.load(Ordering::Acquire), 1);
        assert_eq!(second.closes.load(Ordering::Acquire), 1);
    }
}
