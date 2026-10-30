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

//! DROP policy retains the original exact guard across borrowed phases.
use super::*;
use crate::ports::MvDropProgressPort;

pub struct MvDropTransition<G> {
    operation: MvOperationContext,
    target: MvTarget,
    if_exists: bool,
    guard: Option<G>,
    phase: Phase,
    result: Option<Result<MvProductResult, MvProductError>>,
}
#[derive(Clone, Copy)]
enum Phase {
    Prepare,
    Drop,
    DeleteProjection,
    Unregister,
    Finished,
    Poisoned,
}
impl<G> MvDropTransition<G> {
    pub fn new(operation: MvOperationContext, target: MvTarget, if_exists: bool) -> Self {
        Self {
            operation,
            target,
            if_exists,
            guard: None,
            phase: Phase::Prepare,
            result: None,
        }
    }
    pub fn result(&self) -> Option<&Result<MvProductResult, MvProductError>> {
        self.result.as_ref()
    }
    fn finish(&mut self, result: Result<MvProductResult, MvProductError>) {
        self.result = Some(result);
        self.phase = Phase::Finished;
    }
}
impl MvProductOperations {
    /// Product policy alone orders the exact guard, Provider drop, projection
    /// deletion and catalog removal. The outer original owns physical exit.
    pub fn poll_drop<G>(
        &self,
        cx: &mut Context<'_>,
        transition: &mut MvDropTransition<G>,
        execution: &mut dyn MvDropProgressPort<G>,
    ) -> Poll<()> {
        loop {
            let phase = transition.phase;
            transition.phase = Phase::Poisoned;
            match phase {
                Phase::Prepare => match execution.poll_prepare_drop(
                    cx,
                    transition.operation,
                    &transition.target,
                    transition.if_exists,
                ) {
                    Poll::Pending => {
                        transition.phase = phase;
                        return Poll::Pending;
                    }
                    Poll::Ready(Err(error)) => transition.finish(Err(error.into_product_error())),
                    Poll::Ready(Ok(MvDropReadiness::AlreadyAbsent)) => {
                        transition.finish(Ok(MvProductResult::Acknowledged))
                    }
                    Poll::Ready(Ok(MvDropReadiness::ReadyToDrop(guard))) => {
                        transition.guard = Some(guard);
                        transition.phase = Phase::Drop;
                    }
                },
                Phase::Drop => {
                    match execution.poll_drop_target(cx, transition.operation, &transition.target) {
                        Poll::Pending => {
                            transition.phase = phase;
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(error)) => {
                            transition.finish(Err(error.into_product_error()))
                        }
                        Poll::Ready(Ok(())) => transition.phase = Phase::DeleteProjection,
                    }
                }
                Phase::DeleteProjection => match execution.poll_delete_after_provider_drop(
                    cx,
                    transition.operation,
                    &mut transition.guard,
                ) {
                    Poll::Pending => {
                        transition.phase = phase;
                        return Poll::Pending;
                    }
                    Poll::Ready(Err(error)) => {
                        transition.finish(Err(known_committed_finalize_failure(error)))
                    }
                    Poll::Ready(Ok(())) => transition.phase = Phase::Unregister,
                },
                Phase::Unregister => match execution.poll_unregister_target(
                    cx,
                    transition.operation,
                    &transition.target,
                ) {
                    Poll::Pending => {
                        transition.phase = phase;
                        return Poll::Pending;
                    }
                    Poll::Ready(Err(error)) => {
                        transition.finish(Err(known_committed_finalize_failure(error)))
                    }
                    Poll::Ready(Ok(())) => transition.finish(Ok(MvProductResult::Dropped)),
                },
                Phase::Finished => {
                    transition.phase = phase;
                    return Poll::Ready(());
                }
                Phase::Poisoned => return Poll::Pending,
            }
        }
    }
}
