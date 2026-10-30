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
use crate::query_execution::write_result::RootWriteDecodeFailure;
use novarocks_proto_codec::connector_write::{ConnectorWriteCodecError, ValidatedCommitFragment};
use novarocks_spi::connector::ConnectorWriteFragmentWireDecoder;
use std::sync::Weak;

/// This capability exposes the exact generation's decode direction only. The
/// original stable session retains its non-cloneable catalog generation lease.
#[derive(Clone)]
pub(crate) struct WriteComputationDecode {
    origin: Arc<()>,
    decoder: Arc<dyn ConnectorWriteFragmentWireDecoder>,
    query_targets: Arc<[WriteTargetOrdinal]>,
}

/// One actual graph after a single canonical-to-Provider conversion. No
/// Provider publication predicate or terminal action has been invoked.
pub(crate) struct SessionDecodedWriteSegment {
    origin: Arc<()>,
    production_execution: Option<(
        novarocks_types::QueryExecutionId,
        novarocks_query_application::api::PlanSeal,
    )>,
    prepared: ConnectorPreparedWriteSet,
    fragment_sizes: Vec<usize>,
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
    pub(crate) fn interpret(
        &self,
        prepared: DecodedPreparedWriteSet,
    ) -> Result<SessionDecodedWriteSegment, RootWriteDecodeFailure> {
        let canonical_bytes = prepared
            .fragments()
            .iter()
            .try_fold(0usize, |sum, (_, bytes)| {
                sum.checked_add(bytes.len()).ok_or_else(|| {
                    RootWriteDecodeFailure::source(
                        novarocks_workload_control::WorkError::ArithmeticOverflow,
                    )
                })
            })?;
        // Decoding borrows the same original byte author while every canonical
        // input remains live; each new outer buffer pays its coexistence first.
        let decoded = self.interpret_borrowed(&prepared)?;
        let (_row_count, fragments, statistics, retention, production_execution) =
            prepared.into_parts();
        let fragment_sizes = fragments.iter().map(|(_, bytes)| bytes.len()).collect();
        drop(fragments);
        if let Some(retention) = &retention {
            retention
                .release_bytes_destroyed(canonical_bytes)
                .map_err(RootWriteDecodeFailure::source)?;
        }
        Ok(SessionDecodedWriteSegment {
            origin: Arc::clone(&self.origin),
            production_execution,
            prepared: decoded,
            fragment_sizes,
            statistics,
            retention,
        })
    }

    pub(super) fn interpret_borrowed(
        &self,
        prepared: &DecodedPreparedWriteSet,
    ) -> Result<ConnectorPreparedWriteSet, RootWriteDecodeFailure> {
        use novarocks_proto_codec::{FieldPath, ProtocolError, ProtocolErrorKind};
        use prost::Message;
        let author = prepared.fragment_decode_author()?;
        let mut decoded = Vec::with_capacity(prepared.fragments().len());
        for (index, (target, bytes)) in prepared.fragments().iter().enumerate() {
            // prost copies byte/string fields within the original encoded
            // carrier. Its encoded length bounds those actual byte copies;
            // this is not an invoice for the Provider's decoded SDK graph.
            let receipt = author
                .as_ref()
                .map(|author| author.reserve_owned_bytes(bytes.len()))
                .transpose()
                .map_err(RootWriteDecodeFailure::source)?;
            let raw =
                write_dto::ConnectorCommitFragment::decode(bytes.as_slice()).map_err(|error| {
                    RootWriteDecodeFailure::source(ProtocolError::new(
                        FieldPath::root("prepared_write_set").index(index),
                        ProtocolErrorKind::InvalidValue,
                        format!("invalid commit fragment protobuf: {error}"),
                    ))
                })?;
            let validated = ValidatedCommitFragment::parse(
                raw,
                FieldPath::root("prepared_write_set").index(index),
            )
            .map_err(RootWriteDecodeFailure::source)?;
            let validated = match receipt {
                Some(receipt) => validated.retain_provider_payload(receipt),
                None => validated, // explicit local test constructor only
            };
            let fragment = self
                .decoder
                .decode_commit_fragment(&validated)
                .map_err(|error| {
                    RootWriteDecodeFailure::source(OriginalFragmentCodecFailure(error))
                })?;
            decoded.push((*target, fragment));
        }
        ConnectorPreparedWriteSet::try_new(prepared.row_count(), decoded, &self.query_targets)
            .map_err(RootWriteDecodeFailure::source)
    }
}

/// An exact one-time receiving fact, with no publication permission. Payloads
/// and their original allowance already belong to the session receiving state.
pub(crate) struct AcceptedWriteSegment {
    origin: Arc<()>,
    row_count: u64,
    empty: bool,
}
impl AcceptedWriteSegment {
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
    pub(crate) fn accept(
        &self,
        segment: SessionDecodedWriteSegment,
    ) -> Result<AcceptedWriteSegment, ConnectorError> {
        let session = self.session.upgrade().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "original write receiving session exited",
            )
        })?;
        session.accept_interpreted(segment)
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
        &self,
        query_targets: &[WriteTargetOrdinal],
    ) -> Result<WriteComputationDecode, ConnectorError> {
        crate::query_execution::write_result::RootWriteDecodeContract::try_new(
            query_targets,
            self.targets(),
        )
        .map_err(|error| ConnectorError::new(ConnectorErrorKind::InvalidRequest, error))?;
        Ok(WriteComputationDecode {
            origin: Arc::clone(&self.decode_origin),
            decoder: self.lease.fragment_decoder(),
            query_targets: Arc::from(query_targets),
        })
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
            prepared,
            fragment_sizes,
            statistics,
            retention,
            ..
        } = segment;
        let row_count = prepared.row_count();
        let fragments = prepared.into_fragments();
        if fragments.len() != fragment_sizes.len() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "interpreted write lost its canonical byte ledger",
            ));
        }
        let empty = fragments.is_empty();
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
                && (!accumulated.fragments.is_empty() || !accumulated.statistics.is_empty())
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "write union cannot mix governed and legacy payload ownership",
            ));
        }
        let mut next_rows = accumulated.rows;
        next_rows.add(row_count)?;
        let mut next_ledger = accumulated.ledger;
        for bytes in fragment_sizes {
            next_ledger.reserve_fragment(bytes)?;
        }
        let expected = &self.statistics_expected;
        let mut observed = std::collections::BTreeSet::new();
        if accumulated
            .statistics
            .len()
            .checked_add(statistics.len())
            .is_none_or(|count| {
                count > novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_ARTIFACTS
            })
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "write session statistics artifact count exceeded its limit",
            ));
        }
        let mut next_body_bytes = accumulated.statistics_body_bytes;
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
            next_body_bytes = next_body_bytes
                .checked_add(artifact.draft().body().len())
                .filter(|bytes| {
                    *bytes <= novarocks_spi::connector::MAX_CONNECTOR_STATISTICS_RESULT_BODY_BYTES
                })
                .ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::ResourceExhausted,
                        "write session statistics body bytes exceeded their limit",
                    )
                })?;
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
        match (&mut accumulated.retention, retention) {
            (Some(current), Some(incoming)) => current.merge_retention(&incoming),
            (target @ None, incoming) => *target = incoming,
            _ => {}
        }
        accumulated.rows = next_rows;
        accumulated.ledger = next_ledger;
        accumulated.fragments.extend(fragments);
        accumulated.statistics_body_bytes = next_body_bytes;
        accumulated.statistics_property_bytes = next_property_bytes;
        accumulated.statistics_identities.extend(observed);
        accumulated.statistics.extend(statistics);
        // The original payload/retention union is already stable. No callback
        // runs while the same production/terminal/union locks are held.
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
                empty,
            },
            notification,
        ))
    }
}

#[cfg(test)]
mod outer_byte_budget_tests {
    use super::*;
    use novarocks_spi::connector::{
        ConnectorCodecError, ConnectorCodecErrorKind, ConnectorEncodedPayload, ConnectorFieldPath,
    };

    struct ObservedDecoder {
        calls: Arc<AtomicUsize>,
        alias: Arc<Mutex<Option<bytes::Bytes>>>,
    }
    impl ConnectorWriteFragmentWireDecoder for ObservedDecoder {
        fn owner(&self) -> &str {
            "outer_byte_budget_test"
        }
        fn decode_commit_fragment_payload(
            &self,
            payload: &ConnectorEncodedPayload,
        ) -> Result<
            novarocks_spi::connector::write_stack::ConnectorCommitFragment,
            ConnectorCodecError,
        > {
            self.calls.fetch_add(1, Ordering::AcqRel);
            *self.alias.lock().unwrap() = Some(payload.payload().clone());
            Err(ConnectorCodecError::new(
                ConnectorFieldPath::root("provider_payload"),
                ConnectorCodecErrorKind::InvalidValue,
                "original Provider rejected its fragment",
            ))
        }
    }
    fn observed_decoder(
        target: WriteTargetOrdinal,
    ) -> (
        WriteComputationDecode,
        Arc<AtomicUsize>,
        Arc<Mutex<Option<bytes::Bytes>>>,
    ) {
        let calls = Arc::new(AtomicUsize::new(0));
        let alias = Arc::new(Mutex::new(None));
        (
            WriteComputationDecode {
                origin: Arc::new(()),
                decoder: Arc::new(ObservedDecoder {
                    calls: calls.clone(),
                    alias: alias.clone(),
                }),
                query_targets: vec![target].into(),
            },
            calls,
            alias,
        )
    }

    #[test]
    fn full_original_budget_refuses_outer_copy_before_provider_and_keeps_canonical_input() {
        let (_control, root, binding, _) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let window = binding.window_alias();
        let target = WriteTargetOrdinal::try_new(0).unwrap();
        let bytes = super::super::tests::commit_fragment_bytes();
        let count = bytes.len() as u64;
        let prepared = DecodedPreparedWriteSet::for_test(1, vec![(target, bytes)])
            .with_test_capacity(&binding);
        assert_eq!(window.retained_payload_bytes(), count);
        let limit = novarocks_workload_control::ResultCapacityConfig::V1.all_objects_bytes[2];
        let other_reserved_backing = window.reserve_payload_bytes(limit - 2 * count + 1).unwrap();
        let before = window.retained_payload_bytes();
        let (decoder, calls, alias) = observed_decoder(target);
        let error = decoder.interpret_borrowed(&prepared).unwrap_err();
        assert!(error.to_string().contains("retained result bytes"));
        assert_eq!(calls.load(Ordering::Acquire), 0);
        assert!(alias.lock().unwrap().is_none());
        assert_eq!(window.retained_payload_bytes(), before);
        assert_eq!(prepared.fragments()[0].1.len() as u64, count);
        drop(other_reserved_backing);
        assert_eq!(window.retained_payload_bytes(), count);
        drop(prepared);
        assert_eq!(window.retained_payload_bytes(), 0);
        drop((binding, window));
        root.owner.complete();
        root.business.release();
    }

    #[test]
    fn provider_bytes_alias_keeps_only_its_outer_receipt_after_borrowed_failure() {
        let (_control, root, binding, _) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let window = binding.window_alias();
        let target = WriteTargetOrdinal::try_new(0).unwrap();
        let bytes = super::super::tests::commit_fragment_bytes();
        let count = bytes.len() as u64;
        let prepared = DecodedPreparedWriteSet::for_test(1, vec![(target, bytes)])
            .with_test_capacity(&binding);
        let (decoder, calls, alias) = observed_decoder(target);
        let error = decoder.interpret_borrowed(&prepared).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("original Provider rejected its fragment")
        );
        assert_eq!(calls.load(Ordering::Acquire), 1);
        assert_eq!(window.retained_payload_bytes(), 2 * count);
        // Borrowed logical-row inspection cannot retire the canonical input.
        drop(prepared);
        assert_eq!(window.retained_payload_bytes(), count);
        let last = alias.lock().unwrap().take().unwrap();
        assert!(!last.is_empty());
        drop(last);
        assert_eq!(window.retained_payload_bytes(), 0);
        drop((binding, window));
        root.owner.complete();
        root.business.release();
    }
}
