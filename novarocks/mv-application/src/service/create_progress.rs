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

//! The CREATE product transition borrows the original outer phase Source.

use super::*;
use crate::ports::MvCreateProgressPort;
use crate::product::{MvCreatePublicationOutcome, MvCreatedTarget};

/// Stable product state installed before the first Provider phase is polled.
/// The original outer Source retains its prepared handles and exact publication
/// journal; this state contains only product identities, order and outcomes.
pub struct MvCreateTransition {
    operation: MvOperationContext,
    command: MvCommand,
    staged: Option<MvStagedTarget>,
    created: Option<MvCreatedTarget>,
    primary: Option<MvProductError>,
    abort: Option<Result<(), MvProviderFailure>>,
    phase: Phase,
    result: Option<Result<MvProductResult, MvProductError>>,
}
#[derive(Clone, Copy)]
enum Phase {
    Stage,
    Publish,
    Abort,
    Project,
    Register,
    Finished,
    Poisoned,
}
impl MvCreateTransition {
    pub fn new(operation: MvOperationContext, command: MvCreateCommand) -> Self {
        Self {
            operation,
            command: MvCommand::Create(command),
            staged: None,
            created: None,
            primary: None,
            abort: None,
            phase: Phase::Stage,
            result: None,
        }
    }
    pub fn result(&self) -> Option<&Result<MvProductResult, MvProductError>> {
        self.result.as_ref()
    }
    fn target(&self) -> &MvTarget {
        let MvCommand::Create(command) = &self.command else {
            unreachable!("CREATE transition has one frozen command kind")
        };
        &command.target
    }
    fn finish(&mut self, result: Result<MvProductResult, MvProductError>) {
        self.result = Some(result);
        self.phase = Phase::Finished;
    }
}
impl MvProductOperations {
    /// Advance CREATE without moving its original custody into a Future.
    /// Poison before each foreign poll so unwind cannot replay a phase. A
    /// Pending result restores only that phase; no later effect starts early.
    pub fn poll_create(
        &self,
        cx: &mut Context<'_>,
        transition: &mut MvCreateTransition,
        execution: &mut dyn MvCreateProgressPort,
    ) -> Poll<()> {
        loop {
            let phase = transition.phase;
            transition.phase = Phase::Poisoned;
            match phase {
                Phase::Stage => {
                    match execution.poll_stage_target(cx, transition.operation, &transition.command)
                    {
                        Poll::Pending => {
                            transition.phase = phase;
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(error)) => {
                            transition.finish(Err(error.into_product_error()));
                        }
                        Poll::Ready(Ok(staged)) => {
                            transition.staged = Some(staged);
                            if &transition.staged.as_ref().unwrap().target != transition.target() {
                                transition.finish(Err(MvProductError::new(
                                MvProductErrorKind::InvalidRequest,
                                "MV CREATE provider staged a different target than the command named",
                            )));
                            } else {
                                transition.phase = Phase::Publish;
                            }
                        }
                    }
                }
                Phase::Publish => match execution.poll_publish_staged_target(
                    cx,
                    transition.operation,
                    transition.staged.as_ref().unwrap(),
                ) {
                    Poll::Pending => {
                        transition.phase = phase;
                        return Poll::Pending;
                    }
                    Poll::Ready(Err(error)) => {
                        transition.primary = Some(error.into_product_error());
                        if matches!(
                            transition.primary.as_ref().unwrap().kind(),
                            MvProductErrorKind::CommitUnknown
                                | MvProductErrorKind::KnownCommittedFinalizeFailed
                        ) {
                            transition.finish(Err(transition.primary.as_ref().unwrap().clone()));
                        } else {
                            transition.phase = Phase::Abort;
                        }
                    }
                    Poll::Ready(Ok(MvCreatePublicationOutcome::NoOp)) => {
                        let MvCommand::Create(command) = &transition.command else {
                            unreachable!("CREATE transition has one frozen command kind")
                        };
                        let result = if command.if_not_exists {
                            Ok(MvProductResult::Acknowledged)
                        } else {
                            Err(MvProductError::new(
                                MvProductErrorKind::Corruption,
                                "MV CREATE publication returned NoOp without IF NOT EXISTS",
                            ))
                        };
                        transition.finish(result);
                    }
                    Poll::Ready(Ok(MvCreatePublicationOutcome::Applied(created))) => {
                        transition.created = Some(created);
                        if &transition.created.as_ref().unwrap().target != transition.target() {
                            transition.finish(Err(MvProductError::new(
                                MvProductErrorKind::Corruption,
                                "MV CREATE publish reported a different target than it staged",
                            )));
                        } else {
                            transition.phase = Phase::Project;
                        }
                    }
                },
                Phase::Abort => match execution.poll_abort_staged_target(
                    cx,
                    transition.operation,
                    transition.staged.as_ref().unwrap(),
                ) {
                    Poll::Pending => {
                        transition.phase = phase;
                        return Poll::Pending;
                    }
                    Poll::Ready(result) => {
                        transition.abort = Some(result);
                        let result = settle_stage_abort(
                            transition.primary.as_ref().unwrap().clone(),
                            transition.abort.as_ref().unwrap().clone(),
                        );
                        transition.finish(Err(result));
                    }
                },
                Phase::Project => match execution.poll_install_created_projection(
                    cx,
                    transition.operation,
                    transition.created.as_ref().unwrap(),
                ) {
                    Poll::Pending => {
                        transition.phase = phase;
                        return Poll::Pending;
                    }
                    Poll::Ready(Err(error)) => {
                        transition.finish(Err(known_committed_finalize_failure(error)));
                    }
                    Poll::Ready(Ok(())) => transition.phase = Phase::Register,
                },
                Phase::Register => match execution.poll_register_target(
                    cx,
                    transition.operation,
                    transition.created.as_ref().unwrap(),
                ) {
                    Poll::Pending => {
                        transition.phase = phase;
                        return Poll::Pending;
                    }
                    Poll::Ready(Err(error)) => {
                        transition.finish(Err(known_committed_finalize_failure(error)));
                    }
                    Poll::Ready(Ok(())) => {
                        transition.finish(Ok(MvProductResult::Created(
                            transition.created.as_ref().unwrap().clone(),
                        )));
                    }
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

/// Compatibility with existing synchronous callers during their migration.
/// There is no second CREATE algorithm: all ordering above is shared.
struct ImmediateCreate<'a> {
    provider: &'a dyn MvCreateProviderPort,
    registration: &'a dyn MvCreateCatalogRegistrationPort,
}
impl MvCreateProgressPort for ImmediateCreate<'_> {
    fn poll_stage_target(
        &mut self,
        _: &mut Context<'_>,
        operation: MvOperationContext,
        command: &MvCommand,
    ) -> Poll<Result<MvStagedTarget, MvProviderFailure>> {
        Poll::Ready(self.provider.stage_target(operation, command))
    }
    fn poll_publish_staged_target(
        &mut self,
        _: &mut Context<'_>,
        operation: MvOperationContext,
        staged: &MvStagedTarget,
    ) -> Poll<Result<MvCreatePublicationOutcome, MvProviderFailure>> {
        Poll::Ready(
            self.provider
                .publish_staged_target(operation, staged)
                .map(MvCreatePublicationOutcome::Applied),
        )
    }
    fn poll_abort_staged_target(
        &mut self,
        _: &mut Context<'_>,
        operation: MvOperationContext,
        staged: &MvStagedTarget,
    ) -> Poll<Result<(), MvProviderFailure>> {
        Poll::Ready(self.provider.abort_staged_target(operation, staged))
    }
    fn poll_install_created_projection(
        &mut self,
        _: &mut Context<'_>,
        operation: MvOperationContext,
        target: &MvCreatedTarget,
    ) -> Poll<Result<(), MvProviderFailure>> {
        Poll::Ready(self.provider.install_created_projection(operation, target))
    }
    fn poll_register_target(
        &mut self,
        _: &mut Context<'_>,
        operation: MvOperationContext,
        target: &MvCreatedTarget,
    ) -> Poll<Result<(), MvProviderFailure>> {
        Poll::Ready(self.registration.register_target(operation, target))
    }
}
pub(super) fn create_immediate(
    service: &MvProductOperations,
    operation: MvOperationContext,
    create: MvCreateCommand,
    provider: &dyn MvCreateProviderPort,
    registration: &dyn MvCreateCatalogRegistrationPort,
) -> Result<MvProductResult, MvProductError> {
    let mut transition = MvCreateTransition::new(operation, create);
    let mut execution = ImmediateCreate {
        provider,
        registration,
    };
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(
        service
            .poll_create(&mut cx, &mut transition, &mut execution)
            .is_ready(),
        "immediate Provider effects cannot suspend"
    );
    transition
        .result
        .take()
        .expect("completed CREATE product result")
}
