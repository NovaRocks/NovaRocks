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

//! Intent-bound completion capability.

use crate::query_execution::contract::{
    DistributedQueryError, DistributedQueryErrorKind, DistributedQueryIntent,
};
use crate::query_execution::statistics::StatisticsCollectionProgram;
use crate::query_execution::write::{ConnectorWriteCommitInput, WriteAbortInput, WriteCommitInput};
use crate::query_execution::write_operation::ConnectorWriteOperationSession;
use crate::query_execution::write_plan::ConnectorWritePlanAttachment;
use crate::runtime::profile::RuntimeProfileTree;
use crate::runtime::query_result::QueryResult;

/// Role-neutral execution data assembled by core engine flows before intent
/// validation seals the public distributed-query outcome.
pub(crate) struct QueryExecutionResult {
    pub(crate) query_result: QueryResult,
    pub(crate) write_commit: Option<WriteCommitInput>,
    pub(crate) write_abort: Option<WriteAbortInput>,
    /// Present only when a native distributed writer completed through the
    /// provider-neutral carrier.  It owns the exact control lease until the
    /// engine transaction layer makes the terminal decision.
    pub(crate) connector_completion: Option<ConnectorWriteCompletion>,
    pub(crate) fragment_profiles: Vec<RuntimeProfileTree>,
}

impl std::fmt::Debug for QueryExecutionResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryExecutionResult")
            .field("query_result", &self.query_result)
            .field("write_commit", &self.write_commit)
            .field("write_abort", &self.write_abort)
            .field(
                "has_connector_completion",
                &self.connector_completion.is_some(),
            )
            .field("fragment_profiles", &self.fragment_profiles)
            .finish()
    }
}

pub enum DistributedQueryOutcome {
    Result(ResultExecutionOutcome),
    Write(WriteExecutionOutcome),
    Profile(ProfileExecutionOutcome),
    Statistics(StatisticsExecutionOutcome),
}

pub struct ResultExecutionOutcome {
    result: QueryResult,
}

impl ResultExecutionOutcome {
    pub(crate) fn into_query_result(self) -> QueryResult {
        self.result
    }
}

pub struct WriteExecutionOutcome {
    result: QueryResult,
    commit: Option<WriteCommitInput>,
    abort: Option<WriteAbortInput>,
    connector_completion: Option<ConnectorWriteCompletion>,
}

impl WriteExecutionOutcome {
    pub(crate) fn into_parts(
        self,
    ) -> (
        QueryResult,
        Option<WriteCommitInput>,
        Option<WriteAbortInput>,
    ) {
        (self.result, self.commit, self.abort)
    }

    pub(crate) fn into_parts_with_connector(
        self,
    ) -> (
        QueryResult,
        Option<WriteCommitInput>,
        Option<WriteAbortInput>,
        Option<ConnectorWriteCompletion>,
    ) {
        (
            self.result,
            self.commit,
            self.abort,
            self.connector_completion,
        )
    }
}

/// Successful provider-neutral terminal write facts.  The attachment owns the
/// exact FE generation lease that issued the handles; carrying it through the
/// outcome prevents a newer control generation from committing an older BE
/// report set.
pub struct ConnectorWriteCompletion {
    session: ConnectorWriteOperationSession,
    attachment: ConnectorWritePlanAttachment,
    input: ConnectorWriteCommitInput,
}

impl ConnectorWriteCompletion {
    pub fn from_write_commit(
        session: ConnectorWriteOperationSession,
        attachment: ConnectorWritePlanAttachment,
        commit: &WriteCommitInput,
    ) -> Result<Self, DistributedQueryError> {
        let input = ConnectorWriteCommitInput::try_extract(commit)?.ok_or_else(|| {
            DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "connector write attachment completed without generic staged reports",
            )
        })?;
        let manifest = attachment.manifest();
        if input.owner() != manifest.owner()
            || input.operation_id() != manifest.operation_id()
            || input.cohort_id() != manifest.cohort_id()
            || input.execution_id() != manifest.execution_id()
        {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "generic staged reports do not match the frozen connector write attachment",
            ));
        }
        let expected = manifest
            .writers()
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        let actual = input
            .reports()
            .iter()
            .map(|report| report.writer().clone())
            .collect::<std::collections::BTreeSet<_>>();
        if expected != actual || input.reports().len() != actual.len() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "generic staged reports do not exactly cover the frozen connector writer manifest",
            ));
        }
        session
            .accept_attempt(&attachment, &input)
            .map_err(|error| {
                DistributedQueryError::new(
                    DistributedQueryErrorKind::ContractViolation,
                    format!("register accepted connector write attempt: {error}"),
                )
            })?;
        Ok(Self {
            session,
            attachment,
            input,
        })
    }

    pub(crate) fn attachment(&self) -> &ConnectorWritePlanAttachment {
        &self.attachment
    }

    pub(crate) fn input(&self) -> &ConnectorWriteCommitInput {
        &self.input
    }

    pub fn session(&self) -> &ConnectorWriteOperationSession {
        &self.session
    }
}

pub struct FragmentProfileSet {
    profiles: Vec<RuntimeProfileTree>,
}

impl FragmentProfileSet {
    pub(crate) fn new(profiles: Vec<RuntimeProfileTree>) -> Self {
        Self { profiles }
    }

    pub(crate) fn into_profiles(self) -> Vec<RuntimeProfileTree> {
        self.profiles
    }
}

pub struct ProfileExecutionOutcome {
    result: QueryResult,
    profiles: FragmentProfileSet,
}

/// Typed internal completion for a statistics collection. This intentionally
/// has no query-result field, preventing statistics sinks from becoming a
/// second client-row transport.
pub struct StatisticsExecutionOutcome {
    result: novarocks_spi::connector::StatisticsCollectionResult,
}

impl StatisticsExecutionOutcome {
    pub fn into_collection_result(self) -> novarocks_spi::connector::StatisticsCollectionResult {
        self.result
    }
}

impl ProfileExecutionOutcome {
    pub(crate) fn into_parts(self) -> (QueryResult, FragmentProfileSet) {
        (self.result, self.profiles)
    }
}

impl DistributedQueryOutcome {
    pub fn intent(&self) -> DistributedQueryIntent {
        match self {
            Self::Result(_) => DistributedQueryIntent::Result,
            Self::Write(_) => DistributedQueryIntent::Write,
            Self::Profile(_) => DistributedQueryIntent::Profile,
            Self::Statistics(_) => DistributedQueryIntent::Statistics,
        }
    }

    pub(crate) fn into_write(self) -> Result<WriteExecutionOutcome, DistributedQueryError> {
        match self {
            Self::Write(outcome) => Ok(outcome),
            other => Err(outcome_variant_mismatch(
                DistributedQueryIntent::Write,
                other.intent(),
            )),
        }
    }

    pub(crate) fn into_result(self) -> Result<ResultExecutionOutcome, DistributedQueryError> {
        match self {
            Self::Result(outcome) => Ok(outcome),
            other => Err(outcome_variant_mismatch(
                DistributedQueryIntent::Result,
                other.intent(),
            )),
        }
    }

    pub(crate) fn into_profile(self) -> Result<ProfileExecutionOutcome, DistributedQueryError> {
        match self {
            Self::Profile(outcome) => Ok(outcome),
            other => Err(outcome_variant_mismatch(
                DistributedQueryIntent::Profile,
                other.intent(),
            )),
        }
    }

    pub fn into_statistics(self) -> Result<StatisticsExecutionOutcome, DistributedQueryError> {
        match self {
            Self::Statistics(outcome) => Ok(outcome),
            other => Err(outcome_variant_mismatch(
                DistributedQueryIntent::Statistics,
                other.intent(),
            )),
        }
    }
}

pub struct QueryOutcomeFactory {
    intent: DistributedQueryIntent,
}

impl QueryOutcomeFactory {
    pub(super) fn new(intent: DistributedQueryIntent) -> Self {
        Self { intent }
    }

    pub fn intent(&self) -> DistributedQueryIntent {
        self.intent
    }

    pub fn write(
        self,
        result: QueryResult,
        commit: Option<WriteCommitInput>,
        abort: Option<WriteAbortInput>,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.write_with_connector(result, commit, abort, None)
    }

    pub fn write_with_connector(
        self,
        result: QueryResult,
        commit: Option<WriteCommitInput>,
        abort: Option<WriteAbortInput>,
        connector_completion: Option<ConnectorWriteCompletion>,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.require_intent(DistributedQueryIntent::Write)?;
        if commit.is_some() && abort.is_some() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "Write outcome cannot contain both commit and abort payloads",
            ));
        }
        if connector_completion.is_some() && commit.is_none() {
            return Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "connector write completion requires a write commit payload",
            ));
        }
        Ok(DistributedQueryOutcome::Write(WriteExecutionOutcome {
            result,
            commit,
            abort,
            connector_completion,
        }))
    }

    pub(crate) fn from_execution_result(
        self,
        result: QueryExecutionResult,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        let QueryExecutionResult {
            query_result,
            write_commit,
            write_abort,
            connector_completion,
            fragment_profiles,
        } = result;
        match self.intent {
            DistributedQueryIntent::Write => {
                if !fragment_profiles.is_empty() {
                    return Err(DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        "Write outcome cannot contain fragment profiles",
                    ));
                }
                self.write_with_connector(
                    query_result,
                    write_commit,
                    write_abort,
                    connector_completion,
                )
            }
            DistributedQueryIntent::Profile => {
                if write_commit.is_some() || write_abort.is_some() || connector_completion.is_some()
                {
                    return Err(DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        "Profile outcome cannot contain write commit or abort payloads",
                    ));
                }
                self.profile(query_result, FragmentProfileSet::new(fragment_profiles))
            }
            DistributedQueryIntent::Result => {
                if write_commit.is_some()
                    || write_abort.is_some()
                    || connector_completion.is_some()
                    || !fragment_profiles.is_empty()
                {
                    return Err(DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        "Result outcome cannot contain write or profile payloads",
                    ));
                }
                self.result(query_result)
            }
            DistributedQueryIntent::Statistics => Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "Statistics outcome must be completed through the typed statistics result sink",
            )),
        }
    }

    pub fn result(
        self,
        result: QueryResult,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.require_intent(DistributedQueryIntent::Result)?;
        Ok(DistributedQueryOutcome::Result(ResultExecutionOutcome {
            result,
        }))
    }

    pub fn profile(
        self,
        result: QueryResult,
        profiles: FragmentProfileSet,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.require_intent(DistributedQueryIntent::Profile)?;
        Ok(DistributedQueryOutcome::Profile(ProfileExecutionOutcome {
            result,
            profiles,
        }))
    }

    pub fn statistics(
        self,
        program: &StatisticsCollectionProgram,
        result: novarocks_spi::connector::StatisticsCollectionResult,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.require_intent(DistributedQueryIntent::Statistics)?;
        let mut sink = program.result_sink();
        sink.accept(result)?;
        Ok(DistributedQueryOutcome::Statistics(
            StatisticsExecutionOutcome {
                result: sink.finish()?,
            },
        ))
    }

    fn require_intent(
        &self,
        received: DistributedQueryIntent,
    ) -> Result<(), DistributedQueryError> {
        if self.intent == received {
            return Ok(());
        }
        Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            format!(
                "distributed query outcome intent mismatch: expected {:?}, received {received:?}",
                self.intent
            ),
        ))
    }
}

fn outcome_variant_mismatch(
    expected: DistributedQueryIntent,
    received: DistributedQueryIntent,
) -> DistributedQueryError {
    DistributedQueryError::new(
        DistributedQueryErrorKind::ContractViolation,
        format!(
            "distributed query outcome variant mismatch: expected {expected:?}, received {received:?}"
        ),
    )
}
