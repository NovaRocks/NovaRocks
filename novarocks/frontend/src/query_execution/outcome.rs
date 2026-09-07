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
use crate::query_execution::profile::FragmentProfileTree;
use crate::runtime::query_result::QueryResult;

/// Role-neutral execution data assembled by core engine flows before intent
/// validation seals the public distributed-query outcome.
pub(crate) struct QueryExecutionResult {
    pub(crate) query_result: QueryResult,
    /// Present only when this write travelled the NCP-6 write-session data
    /// plane. It carries the commit authority and the rows every writer
    /// accepted; neither may be surfaced before the external commit succeeds.
    pub(crate) write_session: Option<ConnectorWriteSessionCompletion>,
    pub(crate) fragment_profiles: Vec<FragmentProfileTree>,
}

impl std::fmt::Debug for QueryExecutionResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryExecutionResult")
            .field("query_result", &self.query_result)
            .field("has_write_session", &self.write_session.is_some())
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
    write_session: Option<ConnectorWriteSessionCompletion>,
}

/// A write whose data plane closed and whose execution succeeded, carried to
/// the statement owner so it can perform the one external commit.
///
/// The coordinator has already checked both halves of the gate; what it has not
/// done, and must not do, is commit. Affected rows stay inside here until the
/// commit is known to have succeeded, because reporting them earlier would tell
/// a client about rows that may never become visible.
pub struct ConnectorWriteSessionCompletion {
    session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
    prepared: crate::query_execution::write_result::DecodedPreparedWriteSet,
}

impl ConnectorWriteSessionCompletion {
    pub(crate) const fn session(
        &self,
    ) -> &std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession> {
        &self.session
    }

    /// The rows every writer accepted. Report them to a client only after the
    /// external commit is known to have succeeded.
    pub(crate) fn row_count(&self) -> u64 {
        self.prepared.row_count()
    }

    /// Whether the closed data plane produced no commit fragment at all.
    ///
    /// This is not "zero rows": a writer that accepted no row still emits its
    /// fragment. An empty set means no writer produced anything, which is the
    /// one case where committing would publish a snapshot describing nothing.
    pub(crate) fn is_empty(&self) -> bool {
        self.prepared.fragments().is_empty()
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
        crate::query_execution::write_result::DecodedPreparedWriteSet,
    ) {
        (self.session, self.prepared)
    }

    /// Build a completion directly, for statement-flow tests that need one
    /// without driving a whole distributed round. Production code has no such
    /// constructor: only the coordinator's dual barrier produces one.
    #[cfg(test)]
    pub(crate) const fn for_test(
        session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
        prepared: crate::query_execution::write_result::DecodedPreparedWriteSet,
    ) -> Self {
        Self { session, prepared }
    }
}

impl WriteExecutionOutcome {
    /// The NCP-6 session completion, present exactly when this query used the
    /// write-session data plane.
    pub(crate) fn into_write_session(self) -> Option<ConnectorWriteSessionCompletion> {
        self.write_session
    }

    pub(crate) const fn write_session(&self) -> Option<&ConnectorWriteSessionCompletion> {
        self.write_session.as_ref()
    }

    /// Carry the write terminal this outcome holds into the role-neutral
    /// execution result.
    pub(crate) fn into_execution_result(self) -> QueryExecutionResult {
        QueryExecutionResult {
            query_result: QueryResult {
                columns: Vec::new(),
                chunks: Vec::new(),
            },
            write_session: self.write_session,
            fragment_profiles: Vec::new(),
        }
    }
}

pub struct FragmentProfileSet {
    profiles: Vec<FragmentProfileTree>,
}

impl FragmentProfileSet {
    pub(crate) fn new(profiles: Vec<FragmentProfileTree>) -> Self {
        Self { profiles }
    }

    pub(crate) fn into_profiles(self) -> Vec<FragmentProfileTree> {
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
    artifacts: Vec<novarocks_spi::connector::StatisticsArtifactDraft>,
}

impl StatisticsExecutionOutcome {
    pub fn into_artifacts(self) -> Vec<novarocks_spi::connector::StatisticsArtifactDraft> {
        self.artifacts
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

    /// Consume the outcome as a distributed write terminal result.
    ///
    /// Frontend-owned application lifecycles use the returned write-session
    /// completion to decide commit, abort, or authoritative reconciliation on
    /// their retained commit authority.
    pub fn into_write(self) -> Result<WriteExecutionOutcome, DistributedQueryError> {
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

    /// Hand a completed write session to its statement owner.
    ///
    /// The client result is deliberately empty: the root write relation is
    /// engine machinery, and exposing its shape -- even with no rows -- would
    /// make an internal contract part of the user-visible one. The statement
    /// owner builds the affected-row result after its commit succeeds.
    pub(crate) fn write_session_outcome(
        self,
        session: std::sync::Arc<crate::query_execution::write_session::ConnectorWriteSession>,
        prepared: crate::query_execution::write_result::DecodedPreparedWriteSet,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.require_intent(DistributedQueryIntent::Write)?;
        Ok(DistributedQueryOutcome::Write(WriteExecutionOutcome {
            write_session: Some(ConnectorWriteSessionCompletion { session, prepared }),
        }))
    }

    #[expect(
        clippy::wrong_self_convention,
        reason = "The established outcome translator name describes its execution-result input."
    )]
    pub(crate) fn from_execution_result(
        self,
        result: QueryExecutionResult,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        let QueryExecutionResult {
            query_result,
            write_session,
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
                let completion = write_session.ok_or_else(|| {
                    DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        "Write outcome requires a connector write session completion",
                    )
                })?;
                let (session, prepared) = completion.into_parts();
                self.write_session_outcome(session, prepared)
            }
            DistributedQueryIntent::Profile => {
                if write_session.is_some() {
                    return Err(DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        "Profile outcome cannot contain a write session completion",
                    ));
                }
                self.profile(query_result, FragmentProfileSet::new(fragment_profiles))
            }
            DistributedQueryIntent::Result => {
                if write_session.is_some() || !fragment_profiles.is_empty() {
                    return Err(DistributedQueryError::new(
                        DistributedQueryErrorKind::ContractViolation,
                        "Result outcome cannot contain write or profile payloads",
                    ));
                }
                self.result(query_result)
            }
            DistributedQueryIntent::Statistics => Err(DistributedQueryError::new(
                DistributedQueryErrorKind::ContractViolation,
                "Statistics outcome must be completed from the validated Root artifact stream",
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
        artifacts: Vec<novarocks_spi::connector::StatisticsArtifactDraft>,
    ) -> Result<DistributedQueryOutcome, DistributedQueryError> {
        self.require_intent(DistributedQueryIntent::Statistics)?;
        Ok(DistributedQueryOutcome::Statistics(
            StatisticsExecutionOutcome { artifacts },
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
