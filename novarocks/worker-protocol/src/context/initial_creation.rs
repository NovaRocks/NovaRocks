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

//! The original Task row owns the initial domain journal. Accepted publication
//! precedes classification; actual installation supplies queued depths before
//! the same bounded journal commits. No separate host-side domain authority exists.

use super::{ContextEffect, ContextStep, CoreError, LiveContextCore, TaskShape};
use crate::{
    creation::{AdmissionPort, AdmissionRefusal, CreationCost},
    domain::{TaskDomainInput, TaskDomainTransaction, TaskDomainTransactionProgress, TaskDomains},
    task::CreationPhase,
};
use novarocks_execution_contract::{
    DomainConflict, TaskConclusion, TaskDomainReceipt, TaskFailurePhase, TaskIdentity,
    TerminationDetail,
};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) enum InitialCreation {
    Projecting {
        transaction: TaskDomainTransaction,
    },
    AwaitingPreparationFailure,
    AwaitingInstallation {
        transaction: TaskDomainTransaction,
    },
    Completing {
        transaction: TaskDomainTransaction,
    },
    RollingBack {
        transaction: TaskDomainTransaction,
        finish: InitialFinish,
    },
    DisposingReceipts {
        domains: TaskDomains,
        receipts: Vec<TaskDomainReceipt>,
        finish: InitialFinish,
    },
}
#[derive(Debug)]
pub(crate) enum InitialFinish {
    Install,
    RejectPreparation(DomainConflict),
    AlreadyDecided,
}

impl LiveContextCore {
    pub(super) fn loaded_task(
        &mut self,
        task: TaskIdentity,
        result: Result<CreationCost, AdmissionRefusal>,
        shape: TaskShape,
        initial_domains: Vec<TaskDomainInput>,
        capacity: &mut impl AdmissionPort,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        self.flush(task, step)?;
        let record = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
        if record.phase != CreationPhase::Loading
            || record.initial_creation.is_some()
            || self.task_domains.contains_key(&task)
        {
            return Err(CoreError::IllegalFact);
        }
        let domains =
            TaskDomains::from_frozen_shape(shape.split_nodes, shape.edges, shape.destinations);
        let transaction = TaskDomainTransaction::new(domains, initial_domains);
        // Initial-domain validity is a preparation result, not a new
        // pre-Accepted refusal. Reserve and publish the original Accepted
        // entity before classifying its journal, just as the production
        // acceptance boundary did before this owner migration.
        self.tasks
            .get_mut(&task)
            .expect("original loading Task")
            .initial_creation = Some(InitialCreation::Projecting { transaction });
        self.loaded(task, result, Arc::new(Vec::new()), capacity, step)?;
        let initial = self
            .tasks
            .get_mut(&task)
            .expect("original Task")
            .initial_creation
            .take()
            .expect("original initial journal");
        let InitialCreation::Projecting { mut transaction } = initial else {
            return Err(CoreError::IllegalFact);
        };
        let initial = if self.tasks[&task].phase == CreationPhase::Accepted {
            InitialCreation::Projecting { transaction }
        } else {
            transaction.rollback(DomainConflict::NotMonotonic);
            InitialCreation::RollingBack {
                transaction,
                finish: InitialFinish::AlreadyDecided,
            }
        };
        self.continue_initial_creation(task, initial, step);
        Ok(())
    }

    fn continue_initial_creation(
        &mut self,
        task: TaskIdentity,
        initial: InitialCreation,
        step: &mut ContextStep,
    ) {
        self.tasks
            .get_mut(&task)
            .expect("original initial Task row")
            .initial_creation = Some(initial);
        self.runnable_task_domains.insert(task);
        step.effects
            .push(ContextEffect::ContinueTaskDomain { task });
    }

    pub(super) fn installed_task(
        &mut self,
        task: TaskIdentity,
        initial_queued: Vec<Option<u64>>,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        self.validate_installation(task, step)?;
        let record = self.tasks.get_mut(&task).expect("validated original Task");
        let initial = record
            .initial_creation
            .take()
            .ok_or(CoreError::IllegalFact)?;
        let InitialCreation::AwaitingInstallation { mut transaction } = initial else {
            record.initial_creation = Some(initial);
            return Err(CoreError::IllegalFact);
        };
        if !transaction.begin_completion(initial_queued) {
            record.initial_creation = Some(InitialCreation::AwaitingInstallation { transaction });
            return Err(CoreError::IllegalFact);
        }
        self.continue_initial_creation(task, InitialCreation::Completing { transaction }, step);
        Ok(())
    }

    /// Failure without Installed cannot strand an already Accepted journal.
    /// Only the original preparation's physical exit enables this rollback;
    /// a terminal proposal alone is not the host application's completion.
    pub(super) fn resume_initial_after_preparation_exit(
        &mut self,
        task: TaskIdentity,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        let record = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
        if !record.preparation_exited {
            return Ok(());
        }
        let Some(initial) = record.initial_creation.take() else {
            return Ok(());
        };
        match initial {
            InitialCreation::AwaitingInstallation { mut transaction } => {
                transaction.rollback(DomainConflict::NotMonotonic);
                self.continue_initial_creation(
                    task,
                    InitialCreation::RollingBack {
                        transaction,
                        finish: InitialFinish::AlreadyDecided,
                    },
                    step,
                );
            }
            InitialCreation::AwaitingPreparationFailure => {
                step.effects.push(ContextEffect::WakeTaskDomains { task });
                self.retire_if_ready(task, step)?;
            }
            initial => record.initial_creation = Some(initial),
        }
        Ok(())
    }

    pub(super) fn advance_initial_creation(
        &mut self,
        task: TaskIdentity,
        _capacity: &mut impl AdmissionPort,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        use TaskDomainTransactionProgress as P;
        self.flush(task, step)?;
        let initial = self
            .tasks
            .get_mut(&task)
            .ok_or(CoreError::UnknownTask)?
            .initial_creation
            .take()
            .ok_or(CoreError::IllegalFact)?;
        self.runnable_task_domains.remove(&task);
        match initial {
            InitialCreation::Projecting { mut transaction } => match transaction.advance() {
                P::Pending => self.continue_initial_creation(
                    task,
                    InitialCreation::Projecting { transaction },
                    step,
                ),
                P::Ready => {
                    let progressions = transaction.progressions();
                    let record = self.tasks.get_mut(&task).expect("original Accepted Task");
                    record.initial_creation =
                        Some(InitialCreation::AwaitingInstallation { transaction });
                    step.effects.push(ContextEffect::InitialPreparationReady {
                        task,
                        progressions,
                        refusal: None,
                    });
                }
                P::RolledBack(conflict) => {
                    let (domains, receipts, _) = transaction.into_result();
                    self.continue_initial_creation(
                        task,
                        InitialCreation::DisposingReceipts {
                            domains,
                            receipts,
                            finish: InitialFinish::RejectPreparation(conflict),
                        },
                        step,
                    );
                }
                P::Complete => return Err(CoreError::IllegalFact),
            },
            InitialCreation::Completing { mut transaction } => match transaction.advance() {
                P::Pending => self.continue_initial_creation(
                    task,
                    InitialCreation::Completing { transaction },
                    step,
                ),
                P::Complete => {
                    let (domains, receipts, _) = transaction.into_result();
                    self.continue_initial_creation(
                        task,
                        InitialCreation::DisposingReceipts {
                            domains,
                            receipts,
                            finish: InitialFinish::Install,
                        },
                        step,
                    );
                }
                P::Ready | P::RolledBack(_) => return Err(CoreError::IllegalFact),
            },
            InitialCreation::RollingBack {
                mut transaction,
                finish,
            } => match transaction.advance() {
                P::Pending => self.continue_initial_creation(
                    task,
                    InitialCreation::RollingBack {
                        transaction,
                        finish,
                    },
                    step,
                ),
                P::RolledBack(_) => {
                    let (domains, receipts, _) = transaction.into_result();
                    self.continue_initial_creation(
                        task,
                        InitialCreation::DisposingReceipts {
                            domains,
                            receipts,
                            finish,
                        },
                        step,
                    );
                }
                P::Ready | P::Complete => return Err(CoreError::IllegalFact),
            },
            InitialCreation::DisposingReceipts {
                domains,
                mut receipts,
                finish,
            } => {
                if matches!(finish, InitialFinish::Install) {
                    // Move the actual original receipt vector; do not clone
                    // an arbitrary member set in this bounded Core step.
                    self.tasks
                        .get_mut(&task)
                        .expect("original installed Task")
                        .initial_domain_receipts = Arc::new(receipts);
                    self.task_domains.insert(task, domains);
                    self.publish_installation(task, step)?;
                    step.effects.push(ContextEffect::WakeTaskDomains { task });
                    self.retire_if_ready(task, step)?;
                    return Ok(());
                }
                // Each receipt contains only neutral IDs/scalars and primitive
                // member Vecs. Dispose at most one original entry this turn.
                if receipts.pop().is_some() {
                    self.continue_initial_creation(
                        task,
                        InitialCreation::DisposingReceipts {
                            domains,
                            receipts,
                            finish,
                        },
                        step,
                    );
                } else {
                    self.task_domains.insert(task, domains);
                    match finish {
                        InitialFinish::Install => self.publish_installation(task, step)?,
                        InitialFinish::RejectPreparation(conflict) => {
                            let record = self.tasks.get_mut(&task).expect("original Accepted Task");
                            record.initial_creation =
                                Some(InitialCreation::AwaitingPreparationFailure);
                            step.effects.push(ContextEffect::InitialPreparationReady {
                                task,
                                progressions: Arc::new(Vec::new()),
                                refusal: Some(conflict),
                            });
                        }
                        InitialFinish::AlreadyDecided => {}
                    }
                    step.effects.push(ContextEffect::WakeTaskDomains { task });
                    self.retire_if_ready(task, step)?;
                }
            }
            initial @ (InitialCreation::AwaitingInstallation { .. }
            | InitialCreation::AwaitingPreparationFailure) => {
                self.tasks
                    .get_mut(&task)
                    .expect("original Task")
                    .initial_creation = Some(initial);
                return Err(CoreError::IllegalFact);
            }
        }
        Ok(())
    }

    pub(super) fn validate_installation(
        &mut self,
        task: TaskIdentity,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        self.flush(task, step)?;
        let record = self.tasks.get(&task).ok_or(CoreError::UnknownTask)?;
        if record.phase != CreationPhase::Accepted {
            return Err(CoreError::IllegalFact);
        }
        if !record.installed
            && (record.state.is_terminal()
                || matches!(record.conclusion.accepted.as_ref(),
                Some(TaskConclusion::Terminated(TerminationDetail::Failed(failure)))
                    if failure.phase() == TaskFailurePhase::Preparation))
        {
            return Err(CoreError::IllegalFact);
        }
        Ok(())
    }

    /// InstalledTask already established this positive fact before its bounded
    /// journal completion. A later close cannot erase that real observation.
    pub(super) fn publish_installation(
        &mut self,
        task: TaskIdentity,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        let record = self.tasks.get_mut(&task).ok_or(CoreError::UnknownTask)?;
        if record.phase != CreationPhase::Accepted {
            return Err(CoreError::IllegalFact);
        }
        if !record.installed {
            record.installed = true;
            record.bump_version();
            Self::publish(record, step)?;
        }
        self.try_finish_completion_intent(task, step)?;
        Ok(())
    }
}
