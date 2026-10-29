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

//! One original terminal callback's co-observed neutral facts. This input
//! neither selects an originating failure nor supplies the callback's exit.

use super::{ContextCore, ContextStep, CoreError};
use novarocks_execution_contract::{
    FINAL_TASK_INFO_MAX_OPERATORS, OperatorStatistics, TaskConclusion, TaskIdentity,
    TaskOutputFacts,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalIntent {
    SuccessfulProduction {
        completed_output: Option<TaskOutputFacts>,
    },
    FinishExistingTermination {
        output: Option<TaskOutputFacts>,
    },
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TerminalStatistics {
    pub statistics: Vec<OperatorStatistics>,
    pub truncated: bool,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TerminalObservation {
    pub intent: TerminalIntent,
    /// The actual receiver author validated every frozen stream's close rule.
    /// Task terminal/Installed or a withdrawn MustDrain stream is not proof.
    pub normal_close_validated: bool,
    /// Positive output-responsibility exit, independent of physical runtime exit.
    pub output_released: bool,
    pub final_statistics: Option<TerminalStatistics>,
}
impl ContextCore {
    pub(super) fn observe_terminal(
        &mut self,
        task: TaskIdentity,
        observation: TerminalObservation,
        step: &mut ContextStep,
    ) -> Result<(), CoreError> {
        let record = self.tasks.get(&task).ok_or(CoreError::UnknownTask)?;
        let output = match observation.intent {
            TerminalIntent::SuccessfulProduction { completed_output } => {
                if !record.installed {
                    return Err(CoreError::IllegalFact);
                }
                completed_output
            }
            TerminalIntent::FinishExistingTermination { output } => output,
        };
        if !record.accepted()
            || record.terminal_observation_received
            || output.is_some_and(|facts| !facts.responsibility_complete())
            || observation
                .final_statistics
                .as_ref()
                .is_some_and(|facts| facts.statistics.len() > FINAL_TASK_INFO_MAX_OPERATORS)
        {
            return Err(CoreError::IllegalFact);
        }
        // The original sender sealed every preceding proposal at handoff.
        // This step flushes that same prefix once, never recurses into handle.
        self.flush(task, step)?;
        let record = self.tasks.get_mut(&task).expect("validated original Task");
        if matches!(
            observation.intent,
            TerminalIntent::FinishExistingTermination { .. }
        ) && !matches!(
            record.conclusion.accepted,
            Some(TaskConclusion::Terminated(_))
        ) {
            return Err(CoreError::IllegalFact);
        }
        if let Some(final_statistics) = observation.final_statistics
            && !record.state.is_terminal()
        {
            record.operator_statistics = final_statistics.statistics;
            record.operator_statistics_truncated |= final_statistics.truncated;
        }
        record.statistics_sealed = true;
        record.terminal_observation_received = true;
        record.normal_close_validated = observation.normal_close_validated;
        if let Some(facts) = output {
            record.output = facts;
        }
        match observation.intent {
            TerminalIntent::SuccessfulProduction { .. } => {
                record.completion_success_pending = !record.state.is_terminal();
                self.try_finish_completion_intent(task, step)?;
            }
            TerminalIntent::FinishExistingTermination { .. } => self.finish_task(task, step)?,
        }
        if observation.output_released {
            let record = self.tasks.get_mut(&task).expect("original terminal Task");
            let before = record.convergence.snapshot().version();
            record
                .convergence
                .note_output_released()
                .map_err(|_| CoreError::IllegalFact)?;
            record.output_released = true;
            Self::apply_runtime_exit_receipts(record, before, step)?;
        }
        self.retire_if_ready(task, step)
    }
}
