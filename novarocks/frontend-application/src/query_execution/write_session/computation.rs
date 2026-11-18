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

//! Pure generation decoding and the original session's bounded receiving side.

use super::*;
use crate::query_execution::write_result::{
    DecodedPreparedWriteSet, RootWriteDecodeFailure, RootWriteResultDecoder,
};
use novarocks_proto_codec::connector_write::{ConnectorWriteCodecError, ValidatedCommitFragment};
use novarocks_result_contract::DeleteVerificationCounts;
use novarocks_spi::connector::write_stack::{ConnectorCommitFragment, WriteStatisticsArtifact};
use novarocks_spi::connector::{ConnectorPayloadRetentionGuard, ConnectorWriteFragmentWireDecoder};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::Weak;

/// Decode and receive complete root records against their original session.
pub(crate) struct WriteComputationDecode {
    origin: Arc<()>,
    decoder: Arc<dyn ConnectorWriteFragmentWireDecoder>,
    receiving: WriteReceivingPort,
    #[cfg(test)]
    query_targets: Arc<[WriteTargetOrdinal]>,
    #[cfg(test)]
    local_query: Option<ConnectorWriteQueryIdentity>,
}

/// Complete root facts ready for the original task and cancellation barrier.
pub(crate) struct SessionDecodedWriteSegment {
    origin: Arc<()>,
    production_execution: Option<(
        novarocks_types::QueryExecutionId,
        novarocks_query_application::api::PlanSeal,
    )>,
    query: Option<ConnectorWriteQueryIdentity>,
    row_count: u64,
    fragment_count: usize,
    #[cfg(test)]
    pending_fragments: Vec<(WriteTargetOrdinal, ConnectorCommitFragment)>,
    #[cfg(test)]
    pending_author: Option<ConnectorPayloadRetentionGuard>,
    #[cfg(test)]
    local_targets: Arc<[WriteTargetOrdinal]>,
    delete_verification: Option<DeleteVerificationCounts>,
    statistics: Vec<novarocks_spi::connector::write_stack::WriteStatisticsArtifact>,
    retention: Option<crate::query_execution::internal_result_cpu::InternalResultRetention>,
}

#[derive(Debug)]
struct OriginalFragmentCodecFailure(ConnectorWriteCodecError);
impl std::fmt::Display for OriginalFragmentCodecFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for OriginalFragmentCodecFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl novarocks_failure_contract::FailureProjection for OriginalFragmentCodecFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        self.0.protocol().failure_fact()
    }
}

impl WriteComputationDecode {
    pub(crate) fn apply_body(
        &mut self,
        root: &mut RootWriteResultDecoder,
        body: &[u8],
    ) -> Result<(), RootWriteDecodeFailure> {
        root.apply_relay_body_with(body, |root, target, bytes| {
            let query = self.query_for_arrival(root.relay_production_execution())?;
            let author = root.fragment_decode_author()?;
            let fragment = self.decode_fragment(bytes, &author)?;
            self.receiving
                .arrive(&query, target, fragment, &author)
                .map_err(RootWriteDecodeFailure::source)
        })
    }

    pub(crate) fn complete_root(
        &self,
        root: DecodedPreparedWriteSet,
    ) -> Result<SessionDecodedWriteSegment, RootWriteDecodeFailure> {
        let query = root
            .production_execution
            .map(|(execution, plan)| write_query_identity(execution, plan))
            .transpose()
            .map_err(RootWriteDecodeFailure::source)?;
        #[cfg(test)]
        let query = query.or(self.local_query);
        Ok(SessionDecodedWriteSegment {
            origin: Arc::clone(&self.origin),
            production_execution: root.production_execution,
            query,
            row_count: root.row_count,
            fragment_count: root.fragment_count,
            delete_verification: root.delete_verification,
            statistics: root.statistics,
            retention: root.retention,
            #[cfg(test)]
            pending_fragments: Vec::new(),
            #[cfg(test)]
            pending_author: None,
            #[cfg(test)]
            local_targets: Arc::clone(&self.query_targets),
        })
    }

    fn query_for_arrival(
        &mut self,
        production: Option<(
            novarocks_types::QueryExecutionId,
            novarocks_query_application::api::PlanSeal,
        )>,
    ) -> Result<ConnectorWriteQueryIdentity, RootWriteDecodeFailure> {
        if let Some((execution, plan)) = production {
            return write_query_identity(execution, plan).map_err(RootWriteDecodeFailure::source);
        }
        #[cfg(test)]
        {
            if let Some(query) = self.local_query {
                return Ok(query);
            }
            let session = self
                .receiving
                .session()
                .map_err(RootWriteDecodeFailure::source)?;
            let query = session
                .begin_local_query(&self.query_targets)
                .map_err(RootWriteDecodeFailure::source)?;
            self.local_query = Some(query);
            Ok(query)
        }
        #[cfg(not(test))]
        Err(RootWriteDecodeFailure::source(
            production::production_error("write root record has no original production identity"),
        ))
    }

    fn decode_fragment(
        &self,
        bytes: &[u8],
        author: &ConnectorPayloadRetentionGuard,
    ) -> Result<ConnectorCommitFragment, RootWriteDecodeFailure> {
        use novarocks_proto_codec::{FieldPath, ProtocolError, ProtocolErrorKind};
        use prost::Message;
        // The complete relay record stays borrowed during the copy. Reserve
        // only Prost's actual outer carrier before decoding it.
        let receipt = author
            .reserve_owned_bytes(bytes.len())
            .map_err(RootWriteDecodeFailure::source)?;
        let path = FieldPath::root("prepared_write_fragment");
        let raw = write_dto::ConnectorCommitFragment::decode(bytes).map_err(|error| {
            RootWriteDecodeFailure::source(ProtocolError::new(
                path.clone(),
                ProtocolErrorKind::InvalidValue,
                format!("invalid commit fragment protobuf: {error}"),
            ))
        })?;
        let validated = ValidatedCommitFragment::parse(raw, path)
            .map_err(RootWriteDecodeFailure::source)?
            .retain_provider_payload(receipt);
        self.decoder
            .decode_commit_fragment(&validated, author)
            .map_err(|error| RootWriteDecodeFailure::source(OriginalFragmentCodecFailure(error)))
    }

    /// Enter through the same decoder from explicit relation/packet fixtures.
    #[cfg(test)]
    pub(crate) fn interpret(
        &self,
        mut root: DecodedPreparedWriteSet,
    ) -> Result<SessionDecodedWriteSegment, RootWriteDecodeFailure> {
        let (pending_fragments, author) = self.interpret_borrowed_with_author(&root)?;
        let canonical_bytes = root.fragments.iter().try_fold(0usize, |sum, (_, bytes)| {
            sum.checked_add(bytes.len()).ok_or_else(|| {
                RootWriteDecodeFailure::source(
                    novarocks_workload_control::WorkError::ArithmeticOverflow,
                )
            })
        })?;
        drop(std::mem::take(&mut root.fragments));
        if let Some(retention) = &root.retention {
            retention
                .release_bytes_destroyed(canonical_bytes)
                .map_err(RootWriteDecodeFailure::source)?;
        }
        let mut segment = self.complete_root(root)?;
        segment.pending_fragments = pending_fragments;
        segment.pending_author = Some(author);
        Ok(segment)
    }

    #[cfg(test)]
    pub(super) fn interpret_borrowed(
        &self,
        root: &DecodedPreparedWriteSet,
    ) -> Result<Vec<(WriteTargetOrdinal, ConnectorCommitFragment)>, RootWriteDecodeFailure> {
        self.interpret_borrowed_with_author(root)
            .map(|(fragments, _)| fragments)
    }

    #[cfg(test)]
    fn interpret_borrowed_with_author(
        &self,
        root: &DecodedPreparedWriteSet,
    ) -> Result<
        (
            Vec<(WriteTargetOrdinal, ConnectorCommitFragment)>,
            ConnectorPayloadRetentionGuard,
        ),
        RootWriteDecodeFailure,
    > {
        let author = root.fragment_decode_author()?;
        let mut decoded = Vec::with_capacity(root.fragments.len());
        for (target, bytes) in &root.fragments {
            if !self.query_targets.contains(target) {
                return Err(RootWriteDecodeFailure::source(
                    production::production_error(
                        "connector commit fragment names a write target outside the sealed set",
                    ),
                ));
            }
            decoded.push((*target, self.decode_fragment(bytes, &author)?));
        }
        Ok((decoded, author))
    }
}

/// An exact one-time receiving fact, with no publication permission. Payloads
/// and their original allowance already belong to the session receiving state.
pub(crate) struct AcceptedWriteSegment {
    origin: Arc<()>,
    row_count: u64,
    delete_verification: Option<DeleteVerificationCounts>,
    empty: bool,
}
impl AcceptedWriteSegment {
    /// Evidence from this exact computation cut, never the Session's query union.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "RWM-10 reads this attempt's accepted verification evidence."
        )
    )]
    pub(crate) const fn delete_verification(&self) -> Option<DeleteVerificationCounts> {
        self.delete_verification
    }
    pub(crate) fn row_count(&self) -> u64 {
        self.row_count
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.empty
    }
    pub(crate) fn belongs_to(&self, session: &ConnectorWriteSession) -> bool {
        Arc::ptr_eq(&self.origin, &session.decode_origin)
    }
}

/// A concrete receiving-only endpoint. It cannot finish, abort or reconcile.
/// The session itself remains in stable custody outside every CPU future.
#[derive(Clone)]
pub(crate) struct WriteReceivingPort {
    session: Weak<ConnectorWriteSession>,
}
impl WriteReceivingPort {
    fn session(&self) -> Result<Arc<ConnectorWriteSession>, ConnectorError> {
        self.session.upgrade().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "original write receiving session exited",
            )
        })
    }

    fn arrive(
        &self,
        query: &ConnectorWriteQueryIdentity,
        target: WriteTargetOrdinal,
        fragment: ConnectorCommitFragment,
        author: &ConnectorPayloadRetentionGuard,
    ) -> Result<(), ConnectorError> {
        let session = self.session()?;
        // Exclude the FE terminal claim through the synchronous handoff.
        // Leave the terminal lock usable if the original Provider unwinds.
        let terminal = session.receiving_terminal_guard()?;
        let arrival = catch_unwind(AssertUnwindSafe(|| {
            session.write_stack_pin.session().arrive_write_fragments(
                session.plan.commit_session(),
                query,
                vec![(target, fragment)],
                author,
            )
        }));
        drop(terminal);
        match arrival {
            Ok(outcome) => outcome,
            Err(panic) => resume_unwind(panic),
        }
    }

    pub(crate) fn accept(
        &self,
        segment: SessionDecodedWriteSegment,
    ) -> Result<AcceptedWriteSegment, ConnectorError> {
        self.session()?.accept_interpreted(segment)
    }
}

impl ConnectorWriteSession {
    /// Register this actual stage without replacing any previous stage's
    /// terminal capability. Publication still belongs to this one Session.
    pub(crate) fn retain_computation_credentials(
        self: &Arc<Self>,
        credentials: &crate::query_execution::logical_execution::InitialComputationCredentials,
    ) -> Result<(), ConnectorError> {
        if credentials
            .write_production_session()
            .is_none_or(|original| !original.ptr_eq(&Arc::downgrade(self)))
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "terminal credentials do not belong to this original write session",
            ));
        }
        let terminal = self.lock_terminal()?;
        if terminal.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "terminal write session refuses another credential registration",
            ));
        }
        let mut storage = self.lock_terminal_storage()?;
        if let Some(router) = storage.as_ref() {
            return router.register(credentials);
        }
        let router = Arc::new(
            crate::query_execution::lifecycle_plan::SessionTerminalCredentialRouter::default(),
        );
        router.register(credentials)?;
        *storage = Some(router);
        Ok(())
    }
    pub(crate) fn computation_decode(
        self: &Arc<Self>,
        query_targets: &[WriteTargetOrdinal],
    ) -> Result<WriteComputationDecode, ConnectorError> {
        crate::query_execution::write_result::RootWriteDecodeContract::try_new(
            query_targets,
            self.targets(),
            novarocks_result_contract::PerKind::empty(),
        )
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error))?;
        Ok(WriteComputationDecode {
            origin: Arc::clone(&self.decode_origin),
            decoder: self.write_stack_pin.fragment_decoder(),
            receiving: self.receiving_port(),
            #[cfg(test)]
            query_targets: Arc::from(query_targets),
            #[cfg(test)]
            local_query: None,
        })
    }
    #[cfg(test)]
    fn begin_local_query(
        &self,
        targets: &[WriteTargetOrdinal],
    ) -> Result<ConnectorWriteQueryIdentity, ConnectorError> {
        let key = *uuid::Uuid::now_v7().as_bytes();
        let query = ConnectorWriteQueryIdentity::try_new(key, 1, key)?;
        self.write_stack_pin.session().begin_write_query(
            self.plan.commit_session(),
            &query,
            targets,
            None,
        )?;
        Ok(query)
    }

    pub(crate) fn receiving_port(self: &Arc<Self>) -> WriteReceivingPort {
        WriteReceivingPort {
            session: Arc::downgrade(self),
        }
    }
    pub(super) fn receiving_terminal_guard(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, Option<TerminalDecision>>, ConnectorError> {
        let terminal = self.lock_terminal()?;
        if terminal.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write session already reached a terminal decision",
            ));
        }
        if self.lock_accumulated()?.received.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector write receiving union is already sealed",
            ));
        }
        Ok(terminal)
    }
    pub(super) fn accept_interpreted(
        &self,
        segment: SessionDecodedWriteSegment,
    ) -> Result<AcceptedWriteSegment, ConnectorError> {
        let (accepted, notification) = self.accept_interpreted_in(segment)?;
        if let Some(notification) = notification {
            notification.notify();
        }
        Ok(accepted)
    }
    fn accept_interpreted_in(
        &self,
        segment: SessionDecodedWriteSegment,
    ) -> Result<
        (
            AcceptedWriteSegment,
            Option<novarocks_workload_control::WorkloadProgressNotification>,
        ),
        ConnectorError,
    > {
        // Keep the original decision lock through the pure receiving cut.
        // A concurrent abort cannot pass between the check and payload move.
        let _terminal = self.receiving_terminal_guard()?;
        if !Arc::ptr_eq(&segment.origin, &self.decode_origin) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "write segment belongs to a different original session",
            ));
        }
        let mut production = self.productions.lock().map_err(|_| {
            production::production_error("write production receiving state is poisoned")
        })?;
        let registration = production.current.as_ref();
        match (segment.production_execution, registration) {
            (Some((execution, plan)), Some(original))
                if execution == original.execution && plan == original.plan =>
            {
                original
                    .grant
                    .as_ref()
                    .ok_or_else(|| {
                        production::production_error(
                            "original production registration is incomplete",
                        )
                    })?
                    .check_receive()
                    .map_err(production::production_work_error)?;
            }
            (None, None) => {} // Non-producing pure/legacy relation fixture; Native writer ingress never uses this path.
            _ => {
                return Err(production::production_error(
                    "complete write End has no matching original production record",
                ));
            }
        }
        let SessionDecodedWriteSegment {
            query,
            row_count,
            fragment_count,
            delete_verification,
            statistics,
            retention,
            #[cfg(test)]
            pending_fragments,
            #[cfg(test)]
            pending_author,
            #[cfg(test)]
            local_targets,
            ..
        } = segment;
        let empty = fragment_count == 0;
        let mut accumulated = self.lock_accumulated()?;
        if let (Some(current), Some(incoming)) = (&accumulated.retention, &retention) {
            if !current.is_same_admission(incoming) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "write union received a different result allowance or attribution",
                ));
            }
        } else if accumulated.retention.is_some() && retention.is_none()
            || accumulated.retention.is_none()
                && retention.is_some()
                && (accumulated.has_fragments || !accumulated.statistics.is_empty())
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "write union cannot mix governed and legacy payload ownership",
            ));
        }
        let mut next_rows = accumulated.rows;
        next_rows.add(row_count)?;
        let expected = &self.statistics_expected;
        let mut observed = std::collections::BTreeSet::new();
        let mut next_property_bytes = accumulated.statistics_property_bytes;
        for artifact in &statistics {
            let key = (artifact.target(), artifact.draft().identity().clone());
            if !expected.contains(&key)
                || accumulated.statistics_identities.contains(&key)
                || !observed.insert(key)
            {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "write session accumulated an unknown or duplicate statistics artifact",
                ));
            }
            let property_bytes =
                artifact
                    .draft()
                    .properties()
                    .iter()
                    .try_fold(0usize, |total, (key, value)| {
                        total
                            .checked_add(key.len())
                            .and_then(|sum| sum.checked_add(value.len()))
                    });
            next_property_bytes = property_bytes
                .and_then(|bytes| next_property_bytes.checked_add(bytes))
                .filter(|bytes| {
                    *bytes <= novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES
                })
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "write session statistics property bytes exceeded their limit",
                    )
                })?;
        }
        // All FE-side checks have passed. Provisional fragment arrivals
        // remain insufficient for publication until this original barrier.
        #[cfg(test)]
        let query = match query {
            Some(query) => Some(query),
            None => Some(self.begin_local_query(&local_targets)?),
        };
        let query = query.ok_or_else(|| {
            production::production_error(
                "complete write End has no original receiving query identity",
            )
        })?;
        let control = self.write_stack_pin.session();
        #[cfg(test)]
        if !pending_fragments.is_empty() {
            control.arrive_write_fragments(
                self.plan.commit_session(),
                &query,
                pending_fragments,
                pending_author
                    .as_ref()
                    .expect("fixture's original byte author"),
            )?;
        }
        let notification = if let Some(original) = production.current.as_ref() {
            Some(
                original
                    .grant
                    .as_ref()
                    .unwrap()
                    .receive_in()
                    .map_err(production::production_work_error)?,
            )
        } else {
            None
        };
        let confirmation = catch_unwind(AssertUnwindSafe(|| {
            control.confirm_write_received(self.plan.commit_session(), &query)
        }));
        if !matches!(confirmation, Ok(Ok(()))) {
            // Even when the Workload receipt has arrived, retain current
            // production until the Provider confirms. Abort cannot infer
            // complete inventory from the Workload's receipt alone.
            drop(accumulated);
            drop(production);
            drop(_terminal);
            if let Some(notification) = notification {
                notification.notify();
            }
            return match confirmation {
                Ok(Err(error)) => Err(error),
                Err(panic) => resume_unwind(panic),
                Ok(Ok(())) => unreachable!("successful confirmation continues receiving"),
            };
        }
        match (&mut accumulated.retention, retention) {
            (Some(current), Some(incoming)) => current.merge_retention(&incoming),
            (target @ None, incoming) => *target = incoming,
            _ => {}
        }
        accumulated.rows = next_rows;
        accumulated.has_fragments |= !empty;
        accumulated.statistics_property_bytes = next_property_bytes;
        accumulated.statistics_identities.extend(observed);
        accumulated.statistics.extend(statistics);
        let received = production.current.take();
        drop(accumulated);
        drop(production);
        drop(_terminal);
        // A typed production handle has no release-on-Drop behavior. Its
        // known canonical record/metadata remains charged until disposition.
        drop(received);
        Ok((
            AcceptedWriteSegment {
                origin: Arc::clone(&self.decode_origin),
                row_count,
                delete_verification,
                empty,
            },
            notification,
        ))
    }
}

#[cfg(test)]
mod outer_byte_budget_tests;
