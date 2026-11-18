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

//! Rebuilding the prepared write set from the root result relation.
//!
//! This is the frontend's half of the write data plane. The root backend
//! aggregates every writer's rows and emits them through the ordinary result
//! sink; the frontend fetches them like any other result and turns them back
//! into a complete prepared write set here.
//!
//! Two things make this different from decoding a user query result:
//!
//! * the relation is engine machinery and is never shown to a SQL client, so it
//!   is decoded by column position against the frozen write relation rather
//!   than by the statement's output columns;
//! * a set is complete or it does not exist. A prefix that arrives without an
//!   observed EOF is not "most of a write" -- it is no write at all, and the
//!   only method that can produce a complete set takes the EOF as its
//!   precondition.
//!
//! This is also a trust boundary: the same budgets the writer and the root
//! backend already charged are charged again here, because a frontend that
//! trusted a backend's arithmetic would have no way to notice a backend that
//! got it wrong.
//!
//! Design: ADR-0136 (docs/adr/ADR-0136-ordinary-aggregate-statistics-dataflow.md)

use crate::query_execution::internal_result_cpu::InternalResultRetention;
use novarocks_native_adapter::root_record_assembly::{
    RootRecordAssembly, RootRecordAssemblyError, RootRecordDomain,
};
use novarocks_result_contract::{DeleteVerificationCounts, PerKind, SummaryKind};
use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
use arrow::array::{
    Array, BinaryArray, FixedSizeBinaryArray, Int8Array, Int32Array, Int64Array, ListArray,
    MapArray, StringArray, StructArray,
};
#[cfg(test)]
use novarocks_execution::exec::chunk::Chunk;
use novarocks_native_adapter::root_write_commit_codec::WriteCommitRecordView;
#[cfg(test)]
use novarocks_spi::connector::write_stack::{
    ROOT_WRITE_RESULT_BLOB_TYPE_INDEX, ROOT_WRITE_RESULT_BODY_INDEX,
    ROOT_WRITE_RESULT_FRAGMENT_INDEX, ROOT_WRITE_RESULT_INPUT_FIELDS_INDEX,
    ROOT_WRITE_RESULT_INSUFFICIENT_FINGERPRINT_INDEX, ROOT_WRITE_RESULT_INSUFFICIENT_KEYS_INDEX,
    ROOT_WRITE_RESULT_KIND_INDEX, ROOT_WRITE_RESULT_MATCHED_INDEX,
    ROOT_WRITE_RESULT_PROPERTIES_INDEX, ROOT_WRITE_RESULT_REQUESTED_INDEX,
    ROOT_WRITE_RESULT_ROW_COUNT_INDEX, ROOT_WRITE_RESULT_TARGET_INDEX, RootWriteResultSchema,
};
use novarocks_spi::connector::write_stack::{
    RootRowKind, RootWriteResultMembershipValidator, RootWriteResultRowShape,
    WriteRowCountAccumulator, WriteStatisticsArtifact, WriteTargetOrdinal, row_count_from_wire,
    target_ordinal_from_wire, validate_artifact_draft_nested_values,
};
use novarocks_spi::connector::{
    MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES, StatisticsArtifactDraft, StatisticsArtifactIdentity,
};

pub(crate) type RootWriteResultParts = (
    u64,
    Option<DeleteVerificationCounts>,
    Vec<(WriteTargetOrdinal, Vec<u8>)>,
    Vec<WriteStatisticsArtifact>,
    Option<InternalResultRetention>,
    Option<(
        novarocks_types::QueryExecutionId,
        novarocks_query_application::api::PlanSeal,
    )>,
);

/// Source-preserving rejection from the original write decoder.
#[derive(Debug)]
pub(crate) struct RootWriteDecodeFailure {
    message: String,
    source: Option<novarocks_failure_contract::SharedFailureCause>,
}
impl RootWriteDecodeFailure {
    pub(crate) fn source<E>(error: E) -> Self
    where
        E: std::error::Error
            + novarocks_failure_contract::FailureProjection
            + Send
            + Sync
            + 'static,
    {
        let source = novarocks_failure_contract::SharedFailureCause::new(error);
        Self {
            message: source.to_string(),
            source: Some(source),
        }
    }
    pub(crate) fn context(mut self, context: &'static str) -> Self {
        self.message = format!("{context}: {}", self.message);
        self
    }
    #[cfg(test)]
    fn contains(&self, text: &str) -> bool {
        self.message.contains(text)
    }
}
impl From<String> for RootWriteDecodeFailure {
    fn from(message: String) -> Self {
        Self {
            message,
            source: None,
        }
    }
}
impl From<&str> for RootWriteDecodeFailure {
    fn from(message: &str) -> Self {
        message.to_owned().into()
    }
}
impl From<novarocks_native_adapter::root_record_assembly::RootRecordFramingError>
    for RootWriteDecodeFailure
{
    fn from(error: novarocks_native_adapter::root_record_assembly::RootRecordFramingError) -> Self {
        match error {
            novarocks_native_adapter::root_record_assembly::RootRecordFramingError::Write(
                error,
            ) => Self::source(error),
            other => Self::source(other),
        }
    }
}
impl
    From<
        novarocks_native_adapter::root_record_assembly::RootRecordAssemblyError<
            RootWriteDecodeFailure,
        >,
    > for RootWriteDecodeFailure
{
    fn from(
        error: novarocks_native_adapter::root_record_assembly::RootRecordAssemblyError<Self>,
    ) -> Self {
        match error {
            novarocks_native_adapter::root_record_assembly::RootRecordAssemblyError::Framing(
                error,
            ) => Self::from(error),
            novarocks_native_adapter::root_record_assembly::RootRecordAssemblyError::Sink(
                error,
            ) => error,
        }
    }
}
impl std::fmt::Display for RootWriteDecodeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.message.fmt(f)
    }
}
impl std::error::Error for RootWriteDecodeFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}
impl RootWriteDecodeFailure {
    pub(crate) fn into_parts(
        self,
    ) -> (
        String,
        Option<novarocks_failure_contract::SharedFailureCause>,
    ) {
        (self.message, self.source)
    }
    #[cfg(test)]
    pub(crate) fn into_connector_error(self) -> novarocks_spi::connector::ConnectorError {
        let (message, source) = self.into_parts();
        match source {
            Some(source) => novarocks_spi::connector::ConnectorError::from_domain(
                novarocks_spi::connector::ConnectorErrorKind::CorruptData,
                source,
            ),
            None => novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::CorruptData,
                message,
            ),
        }
    }
}

/// A complete prepared write set, still in canonical carrier form.
///
/// The fragments stay encoded here on purpose: turning them into provider
/// values requires the exact control binding that produced them, and this type
/// exists to be handed to it.
#[derive(Debug)]
pub(crate) struct DecodedPreparedWriteSet {
    row_count: u64,
    delete_verification: Option<DeleteVerificationCounts>,
    fragments: Vec<(WriteTargetOrdinal, Vec<u8>)>,
    statistics: Vec<WriteStatisticsArtifact>,
    #[cfg(test)]
    local_decode: bool,
    retention: Option<InternalResultRetention>,
    production_execution: Option<(
        novarocks_types::QueryExecutionId,
        novarocks_query_application::api::PlanSeal,
    )>,
}

impl DecodedPreparedWriteSet {
    /// Build a set directly, for tests that need a complete set without
    /// driving a whole root relation through the decoder. Production code has
    /// no such constructor: the only way to obtain one is to observe EOF.
    #[cfg(test)]
    pub(crate) const fn for_test(
        row_count: u64,
        fragments: Vec<(WriteTargetOrdinal, Vec<u8>)>,
    ) -> Self {
        Self {
            row_count,
            delete_verification: None,
            fragments,
            statistics: Vec::new(),
            retention: None,
            local_decode: true,
            production_execution: None,
        }
    }

    #[cfg(test)]
    pub(crate) const fn for_test_with_statistics(
        row_count: u64,
        fragments: Vec<(WriteTargetOrdinal, Vec<u8>)>,
        statistics: Vec<WriteStatisticsArtifact>,
    ) -> Self {
        Self {
            row_count,
            delete_verification: None,
            fragments,
            statistics,
            retention: None,
            local_decode: true,
            production_execution: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_test_delete_verification(mut self, value: DeleteVerificationCounts) -> Self {
        self.delete_verification = Some(value);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_test_capacity(
        mut self,
        binding: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    ) -> Self {
        let retention = InternalResultRetention::try_new(binding).unwrap();
        let bytes = self.fragments.iter().map(|(_, bytes)| bytes.len()).sum();
        retention.reserve_bytes(bytes).unwrap();
        self.retention = Some(retention);
        self
    }

    pub(crate) const fn row_count(&self) -> u64 {
        self.row_count
    }

    pub(crate) fn fragments(&self) -> &[(WriteTargetOrdinal, Vec<u8>)] {
        &self.fragments
    }

    /// Borrow only this prepared result's actual byte author. A missing
    /// production author is never converted into a local decode permission.
    pub(crate) fn fragment_decode_author(
        &self,
    ) -> Result<novarocks_spi::connector::ConnectorPayloadRetentionGuard, RootWriteDecodeFailure>
    {
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
        if let Some(retention) = &self.retention {
            return retention
                .try_spi_guard()
                .map_err(RootWriteDecodeFailure::source);
        }
        #[cfg(test)]
        if self.local_decode {
            use novarocks_spi::connector::{
                ConnectorPayloadByteBudget, ConnectorPayloadRetentionGuard,
            };

            struct LocalAuthor;
            impl ConnectorPayloadByteBudget for LocalAuthor {
                fn reserve_bytes(
                    &self,
                    _bytes: usize,
                ) -> Result<ConnectorPayloadRetentionGuard, ConnectorError> {
                    Ok(ConnectorPayloadRetentionGuard::new(()))
                }
            }
            return Ok(ConnectorPayloadRetentionGuard::from_byte_budget(
                std::sync::Arc::new(LocalAuthor),
            ));
        }
        Err(RootWriteDecodeFailure::source(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "prepared write decoding requires its original byte-budget author",
        )))
    }

    pub(crate) fn into_parts(self) -> RootWriteResultParts {
        (
            self.row_count,
            self.delete_verification,
            self.fragments,
            self.statistics,
            self.retention,
            self.production_execution,
        )
    }
}

/// FE-only expectations frozen from this query's TableFinish and the exact
/// write session generation. It is never reconstructed from native wire data.
#[derive(Clone, Debug)]
pub(crate) struct RootWriteDecodeContract {
    expected_targets: BTreeSet<WriteTargetOrdinal>,
    expected_artifacts: BTreeSet<(WriteTargetOrdinal, StatisticsArtifactIdentity)>,
    declared_summary_kinds: PerKind<()>,
    #[cfg(test)]
    schema: RootWriteResultSchema,
}

impl RootWriteDecodeContract {
    pub(crate) fn try_new(
        query_targets: &[WriteTargetOrdinal],
        session_targets: &[novarocks_spi::connector::write_stack::ConnectorWriteTargetPlan],
        declared_summary_kinds: PerKind<()>,
    ) -> Result<Self, String> {
        novarocks_spi::connector::write_stack::validate_query_target_ordinals(query_targets)
            .map_err(|error| format!("write Root query target set: {error}"))?;
        let expected_targets = query_targets.iter().copied().collect::<BTreeSet<_>>();
        if expected_targets.len() != query_targets.len() {
            return Err("write Root query target set contains a duplicate ordinal".into());
        }
        let by_ordinal = session_targets
            .iter()
            .map(|target| (target.ordinal(), target))
            .collect::<BTreeMap<_, _>>();
        let mut expected_artifacts = BTreeSet::new();
        for target in &expected_targets {
            let plan = by_ordinal.get(target).ok_or_else(|| {
                format!(
                    "write Root query target {} is outside the sealed write session",
                    target.get()
                )
            })?;
            for requirement in plan.statistics().requirements() {
                if !expected_artifacts.insert((*target, requirement.artifact().clone())) {
                    return Err(format!(
                        "write Root contract repeats an artifact for target {}",
                        target.get()
                    ));
                }
            }
        }
        Ok(Self {
            expected_targets,
            expected_artifacts,
            declared_summary_kinds,
            #[cfg(test)]
            schema: RootWriteResultSchema::new(),
        })
    }

    #[cfg(test)]
    fn for_test(
        targets: impl IntoIterator<Item = WriteTargetOrdinal>,
        artifacts: impl IntoIterator<Item = (WriteTargetOrdinal, StatisticsArtifactIdentity)>,
    ) -> Self {
        Self {
            expected_targets: targets.into_iter().collect(),
            expected_artifacts: artifacts.into_iter().collect(),
            declared_summary_kinds: PerKind::empty(),
            schema: RootWriteResultSchema::new(),
        }
    }
}

/// Accumulates the root result relation across fetched batches.
/// One Root write result row, from either the Arrow relation or a relayed
/// record. Absent values are the row's nulls.
struct RootWriteRow<'a> {
    kind: RootRowKind,
    target: Option<i32>,
    count: Option<i64>,
    fragment: Option<&'a [u8]>,
    field_values: Option<Vec<Option<i32>>>,
    blob_type: Option<&'a str>,
    body: Option<&'a [u8]>,
    pairs: Option<Vec<(Option<&'a str>, Option<&'a str>)>>,
    verification: [Option<i64>; 3],
    fingerprint: Option<[u8; 16]>,
}

pub(crate) struct RootWriteResultDecoder {
    relay_assembly: RootRecordAssembly,
    relayed_record_count: u64,
    contract: RootWriteDecodeContract,
    rows: WriteRowCountAccumulator,
    delete_verification: Option<DeleteVerificationCounts>,
    fragments: Vec<(WriteTargetOrdinal, Vec<u8>)>,
    statistics: BTreeMap<(WriteTargetOrdinal, StatisticsArtifactIdentity), WriteStatisticsArtifact>,
    membership: RootWriteResultMembershipValidator,
    property_bytes: usize,
    root_eof: bool,
    retention: Option<InternalResultRetention>,
    #[cfg(test)]
    local_assembly: bool,
    production_execution: Option<(
        novarocks_types::QueryExecutionId,
        novarocks_query_application::api::PlanSeal,
    )>,
}

impl RootWriteResultDecoder {
    pub(crate) fn new(contract: RootWriteDecodeContract) -> Self {
        Self {
            relayed_record_count: 0,
            relay_assembly: RootRecordAssembly::new(
                RootRecordDomain::WriteCommit,
                32 * 1024 * 1024,
            ),
            contract,
            rows: WriteRowCountAccumulator::new(),
            delete_verification: None,
            fragments: Vec::new(),
            statistics: BTreeMap::new(),
            membership: RootWriteResultMembershipValidator::default(),
            property_bytes: 0,
            root_eof: false,
            retention: None,
            #[cfg(test)]
            local_assembly: false,
            production_execution: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test_local(contract: RootWriteDecodeContract) -> Self {
        let mut decoder = Self::new(contract);
        decoder.local_assembly = true;
        decoder
    }

    pub(crate) fn new_with_capacity(
        contract: RootWriteDecodeContract,
        binding: &novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    ) -> Result<Self, String> {
        let retention = InternalResultRetention::try_new(binding)?;
        let mut decoder = Self::new(contract);
        decoder.retention = Some(retention);
        Ok(decoder)
    }

    /// Original attempt identity only; it grants no capacity or completeness.
    pub(crate) fn bind_production_execution(
        &mut self,
        execution: novarocks_types::QueryExecutionId,
        plan: novarocks_query_application::api::PlanSeal,
    ) {
        self.production_execution = Some((execution, plan));
    }

    #[cfg(test)]
    pub(crate) fn apply_chunk(&mut self, chunk: &Chunk) -> Result<(), RootWriteDecodeFailure> {
        if self.root_eof {
            return Err("write Root emitted a trailing batch after EOF".into());
        }
        let schema = chunk.schema();
        if schema.as_ref() != self.contract.schema.arrow_schema().as_ref() {
            return Err(format!(
                "write Root schema mismatch: expected {:?}, received {:?}",
                self.contract.schema.arrow_schema(),
                schema
            )
            .into());
        }
        let columns = chunk.columns();
        let kinds = downcast::<Int8Array>(&columns[ROOT_WRITE_RESULT_KIND_INDEX], "kind")?;
        let targets = downcast::<Int32Array>(&columns[ROOT_WRITE_RESULT_TARGET_INDEX], "target")?;
        let counts =
            downcast::<Int64Array>(&columns[ROOT_WRITE_RESULT_ROW_COUNT_INDEX], "row count")?;
        let fragments = downcast::<BinaryArray>(
            &columns[ROOT_WRITE_RESULT_FRAGMENT_INDEX],
            "commit fragment",
        )?;
        let input_fields = downcast::<ListArray>(
            &columns[ROOT_WRITE_RESULT_INPUT_FIELDS_INDEX],
            "input fields",
        )?;
        let blob_types =
            downcast::<StringArray>(&columns[ROOT_WRITE_RESULT_BLOB_TYPE_INDEX], "blob type")?;
        let bodies = downcast::<BinaryArray>(&columns[ROOT_WRITE_RESULT_BODY_INDEX], "body")?;
        let properties =
            downcast::<MapArray>(&columns[ROOT_WRITE_RESULT_PROPERTIES_INDEX], "properties")?;
        let requested =
            downcast::<Int64Array>(&columns[ROOT_WRITE_RESULT_REQUESTED_INDEX], "requested")?;
        let matched = downcast::<Int64Array>(&columns[ROOT_WRITE_RESULT_MATCHED_INDEX], "matched")?;
        let insufficient = downcast::<Int64Array>(
            &columns[ROOT_WRITE_RESULT_INSUFFICIENT_KEYS_INDEX],
            "insufficient keys",
        )?;
        let fingerprint = downcast::<FixedSizeBinaryArray>(
            &columns[ROOT_WRITE_RESULT_INSUFFICIENT_FINGERPRINT_INDEX],
            "insufficient fingerprint",
        )?;

        for row in 0..chunk.len() {
            if kinds.is_null(row) {
                return Err("root write result row has no kind".to_string().into());
            }
            let kind = RootRowKind::from_wire(kinds.value(row)).map_err(|error| {
                RootWriteDecodeFailure::source(error).context("root write result row kind")
            })?;
            let field_values = if input_fields.is_null(row) {
                None
            } else {
                let fields = input_fields.value(row);
                let fields = fields
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or_else(|| "write Root input_fields item is not Int32".to_string())?;
                Some(
                    (0..fields.len())
                        .map(|index| (!fields.is_null(index)).then(|| fields.value(index)))
                        .collect::<Vec<_>>(),
                )
            };
            let entries = if properties.is_null(row) {
                None
            } else {
                Some(properties.value(row))
            };
            let pairs = match &entries {
                None => None,
                Some(entries) => {
                    let entries =
                        entries
                            .as_any()
                            .downcast_ref::<StructArray>()
                            .ok_or_else(|| {
                                "write Root properties entries are not Struct".to_string()
                            })?;
                    let keys = entries
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| "write Root property keys are not Utf8".to_string())?;
                    let values = entries
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| "write Root property values are not Utf8".to_string())?;
                    Some(
                        (0..entries.len())
                            .map(|index| {
                                (
                                    (!keys.is_null(index)).then(|| keys.value(index)),
                                    (!values.is_null(index)).then(|| values.value(index)),
                                )
                            })
                            .collect::<Vec<_>>(),
                    )
                }
            };
            self.apply_row(RootWriteRow {
                kind,
                target: (!targets.is_null(row)).then(|| targets.value(row)),
                count: (!counts.is_null(row)).then(|| counts.value(row)),
                fragment: (!fragments.is_null(row)).then(|| fragments.value(row)),
                field_values,
                blob_type: (!blob_types.is_null(row)).then(|| blob_types.value(row)),
                body: (!bodies.is_null(row)).then(|| bodies.value(row)),
                pairs,
                verification: [requested, matched, insufficient]
                    .map(|values| (!values.is_null(row)).then(|| values.value(row))),
                fingerprint: (!fingerprint.is_null(row)).then(|| {
                    fingerprint
                        .value(row)
                        .try_into()
                        .expect("validated fingerprint width")
                }),
            })?;
        }
        Ok(())
    }

    /// Validate complete local domain consumption before the coordinator
    /// records End. This count is relation rows, not affected write rows.
    pub(crate) fn check_relay_end(&self, output_rows: u64) -> Result<(), RootWriteDecodeFailure> {
        self.relay_assembly.finish()?;
        if output_rows != self.relayed_record_count {
            return Err("internal root End differs from decoded record count".into());
        }
        Ok(())
    }

    /// Reserve partial record backing against the original result byte budget.
    /// A complete borrowed record needs no assembly copy.
    pub(crate) fn apply_relay_body(&mut self, body: &[u8]) -> Result<(), RootWriteDecodeFailure> {
        if self.root_eof {
            return Err("write Root emitted a trailing body after EOF".into());
        }
        #[cfg(test)]
        let explicit_local = self.local_assembly;
        #[cfg(not(test))]
        let explicit_local = false;
        let author = self
            .retention
            .as_ref()
            .map(InternalResultRetention::try_spi_guard)
            .transpose()
            .map_err(RootWriteDecodeFailure::source)?;
        let mut assembly = std::mem::replace(
            &mut self.relay_assembly,
            RootRecordAssembly::new(RootRecordDomain::WriteCommit, 32 * 1024 * 1024),
        );
        let consume = |record: &[u8]| {
            self.apply_record(record)?;
            self.relayed_record_count = self
                .relayed_record_count
                .checked_add(1)
                .ok_or("internal root record count overflow")?;
            Ok::<(), RootWriteDecodeFailure>(())
        };
        let result = match author {
            Some(author) => assembly.push_budgeted(
                body,
                |bytes| {
                    author
                        .reserve_owned_bytes(bytes)
                        .map_err(RootWriteDecodeFailure::source)
                },
                consume,
            ),
            None if explicit_local => assembly.push(body, consume),
            None => {
                let error = novarocks_spi::connector::ConnectorError::new(
                    novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                    "write Root assembly requires its original byte-budget author",
                );
                Err(RootRecordAssemblyError::Sink(
                    RootWriteDecodeFailure::source(error),
                ))
            }
        };
        self.relay_assembly = assembly;
        result.map_err(Into::into)
    }

    /// Apply one complete PreparedWriteCommitV1 record relayed from the
    /// Backend. The same row rules apply as for the Arrow relation.
    pub(crate) fn apply_record(&mut self, record: &[u8]) -> Result<(), RootWriteDecodeFailure> {
        if self.root_eof {
            return Err("write Root emitted a trailing record after EOF".into());
        }
        let view = WriteCommitRecordView::parse(record)
            .map_err(|error| RootWriteDecodeFailure::source(error).context("write Root record"))?;
        let header = view.header();
        let kind = header.kind();
        let artifact = kind == RootRowKind::ArtifactDraft;
        let verification = view.delete_verification().map(|value| value.into_wire());
        self.apply_row(RootWriteRow {
            kind,
            target: matches!(
                kind,
                RootRowKind::PreparedFragment | RootRowKind::ArtifactDraft
            )
            .then(|| header.target()),
            count: (kind == RootRowKind::RowCount)
                .then(|| i64::try_from(header.row_count()))
                .transpose()
                .map_err(|_| "write Root record row count exceeds i64".to_string())?,
            fragment: (kind == RootRowKind::PreparedFragment).then(|| view.fragment()),
            field_values: artifact.then(|| view.field_ids().map(Some).collect()),
            blob_type: artifact.then(|| view.blob_type()),
            body: artifact.then(|| view.body()),
            pairs: artifact.then(|| {
                view.properties()
                    .map(|(key, value)| (Some(key), Some(value)))
                    .collect()
            }),
            verification: verification.map_or([None; 3], |(counts, _)| counts.map(Some)),
            fingerprint: verification.map(|(_, fingerprint)| fingerprint),
        })
    }

    fn apply_row(&mut self, row: RootWriteRow<'_>) -> Result<(), RootWriteDecodeFailure> {
        let RootWriteRow {
            kind,
            target,
            count,
            fragment,
            field_values,
            blob_type,
            body,
            pairs,
            verification,
            fingerprint,
        } = row;
        self.membership
            .observe(
                kind,
                RootWriteResultRowShape {
                    target,
                    row_count: count,
                    fragment_len: fragment.map(<[u8]>::len),
                    input_fields_len: field_values.as_ref().map(Vec::len),
                    blob_type_len: blob_type.map(str::len),
                    body_len: body.map(<[u8]>::len),
                    properties_len: pairs.as_ref().map(Vec::len),
                    requested: verification[0],
                    matched: verification[1],
                    insufficient_keys: verification[2],
                    insufficient_fingerprint_len: fingerprint.as_ref().map(|bytes| bytes.len()),
                },
            )
            .map_err(|error| {
                RootWriteDecodeFailure::source(error).context("write Root row shape")
            })?;

        match kind {
            RootRowKind::DeleteVerification => {
                if self
                    .contract
                    .declared_summary_kinds
                    .get(SummaryKind::DeleteVerification)
                    .is_none()
                {
                    return Err(
                        "write Root contains undeclared summary kind DeleteVerification".into(),
                    );
                }
                self.delete_verification = Some(
                    DeleteVerificationCounts::try_from_wire(
                        verification[0].expect("validated verification count"),
                        verification[1].expect("validated verification count"),
                        verification[2].expect("validated verification count"),
                        fingerprint.expect("validated fingerprint"),
                    )
                    .map_err(|error| {
                        RootWriteDecodeFailure::source(
                            novarocks_spi::connector::ConnectorError::new(
                                novarocks_spi::connector::ConnectorErrorKind::CorruptData,
                                error.to_string(),
                            ),
                        )
                    })?,
                );
            }
            RootRowKind::RowCount => {
                let count = count.expect("validated above");
                let count = row_count_from_wire(count).map_err(|error| {
                    RootWriteDecodeFailure::source(error).context("write Root row count")
                })?;
                self.rows.add(count).map_err(|error| {
                    RootWriteDecodeFailure::source(error).context("write Root row count")
                })?;
            }
            RootRowKind::PreparedFragment => {
                let target = target_ordinal_from_wire(target.expect("validated above")).map_err(
                    |error| {
                        RootWriteDecodeFailure::source(error).context("write Root target ordinal")
                    },
                )?;
                if !self.contract.expected_targets.contains(&target) {
                    return Err(format!(
                        "write Root names target {} outside this query's frozen set",
                        target.get()
                    )
                    .into());
                }
                let fragment = fragment.expect("validated above");
                if let Some(retention) = &self.retention {
                    retention.reserve_bytes(fragment.len()).map_err(|error| {
                        RootWriteDecodeFailure::source(error)
                            .context("prepared write retained bytes")
                    })?;
                }
                self.fragments.push((target, fragment.to_vec()));
            }
            RootRowKind::ArtifactDraft => {
                let target = target_ordinal_from_wire(target.expect("validated above")).map_err(
                    |error| {
                        RootWriteDecodeFailure::source(error).context("write Root artifact target")
                    },
                )?;
                if !self.contract.expected_targets.contains(&target) {
                    return Err(format!(
                        "write Root artifact names target {} outside this query's frozen set",
                        target.get()
                    )
                    .into());
                }
                let field_values = field_values.expect("validated above");
                let pairs = pairs.expect("validated above");
                validate_artifact_draft_nested_values(&field_values, &pairs).map_err(|error| {
                    RootWriteDecodeFailure::source(error).context("write Root artifact values")
                })?;
                let field_ids = field_values
                    .into_iter()
                    .map(|field| field.expect("validated non-null field"))
                    .collect::<Vec<_>>();
                let identity = StatisticsArtifactIdentity::try_new(
                    field_ids.clone(),
                    blob_type.expect("validated above"),
                )
                .map_err(|error| {
                    RootWriteDecodeFailure::source(error).context("write Root artifact identity")
                })?;
                let key = (target, identity.clone());
                if !self.contract.expected_artifacts.contains(&key) {
                    return Err(format!(
                        "write Root emitted an unexpected artifact for target {}: {identity:?}",
                        target.get()
                    )
                    .into());
                }
                if self.statistics.contains_key(&key) {
                    return Err(format!(
                        "write Root emitted a duplicate artifact for target {}: {identity:?}",
                        target.get()
                    )
                    .into());
                }
                let body = body.expect("validated above");
                let mut property_map = BTreeMap::new();
                let mut row_property_bytes = 0usize;
                for (key, value) in pairs {
                    let key = key.expect("validated property key");
                    let value = value.expect("validated property value");
                    row_property_bytes = row_property_bytes
                        .checked_add(key.len())
                        .and_then(|total| total.checked_add(value.len()))
                        .ok_or_else(|| {
                            RootWriteDecodeFailure::source(
                                novarocks_spi::connector::ConnectorError::new(
                                    novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted,
                                    "write Root property budget overflow",
                                ),
                            )
                        })?;
                    if let Some(retention) = &self.retention {
                        let bytes = key.len().checked_add(value.len()).ok_or_else(|| {
                            RootWriteDecodeFailure::source(
                                novarocks_workload_control::WorkError::ArithmeticOverflow,
                            )
                        })?;
                        retention.reserve_bytes(bytes).map_err(|error| {
                            RootWriteDecodeFailure::source(error)
                                .context("write artifact property retained bytes")
                        })?;
                    }
                    property_map.insert(key.to_string(), value.to_string());
                }
                self.property_bytes = charge_total(
                    self.property_bytes,
                    row_property_bytes,
                    MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES,
                    "write Root artifact properties",
                )?;
                if let Some(retention) = &self.retention {
                    retention.reserve_bytes(body.len()).map_err(|error| {
                        RootWriteDecodeFailure::source(error)
                            .context("write artifact retained bytes")
                    })?;
                }
                let draft = StatisticsArtifactDraft::try_new(
                    field_ids,
                    identity.blob_type(),
                    bytes::Bytes::copy_from_slice(body),
                    property_map,
                )
                .map_err(|error| {
                    RootWriteDecodeFailure::source(error).context("write Root artifact draft")
                })?;
                let draft = match &self.retention {
                    Some(retention) => draft.attach_guard(retention.spi_guard()),
                    None => draft,
                };
                self.statistics
                    .insert(key, WriteStatisticsArtifact::new(target, draft));
            }
        }
        Ok(())
    }

    pub(crate) fn observe_root_eof(&mut self) -> Result<(), RootWriteDecodeFailure> {
        self.relay_assembly.finish()?;
        if std::mem::replace(&mut self.root_eof, true) {
            return Err("write Root emitted duplicate EOF".into());
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<DecodedPreparedWriteSet, RootWriteDecodeFailure> {
        if !self.root_eof {
            return Err("write Root EOF was not observed".into());
        }
        self.membership.finish().map_err(|error| {
            RootWriteDecodeFailure::source(error).context("write Root membership")
        })?;
        if self
            .contract
            .declared_summary_kinds
            .get(SummaryKind::DeleteVerification)
            .is_some()
            && self.delete_verification.is_none()
        {
            return Err("write Root is missing declared summary kind DeleteVerification".into());
        }
        let observed = self.statistics.keys().cloned().collect::<BTreeSet<_>>();
        if observed != self.contract.expected_artifacts {
            let missing = self
                .contract
                .expected_artifacts
                .difference(&observed)
                .collect::<Vec<_>>();
            return Err(format!(
                "write Root artifact membership is incomplete; missing {missing:?}"
            )
            .into());
        }
        Ok(DecodedPreparedWriteSet {
            row_count: self.rows.get(),
            delete_verification: self.delete_verification,
            fragments: self.fragments,
            statistics: self.statistics.into_values().collect(),
            retention: self.retention,
            #[cfg(test)]
            local_decode: self.local_assembly,
            production_execution: self.production_execution,
        })
    }
}

fn charge_total(
    current: usize,
    additional: usize,
    limit: usize,
    label: &str,
) -> Result<usize, RootWriteDecodeFailure> {
    let total = current.checked_add(additional).ok_or_else(|| {
        RootWriteDecodeFailure::source(novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted,
            format!("{label} budget overflow"),
        ))
    })?;
    if total > limit {
        return Err(RootWriteDecodeFailure::source(
            novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted,
                format!("{label} budget exceeded"),
            ),
        ));
    }
    Ok(total)
}

#[cfg(test)]
fn downcast<'a, T: 'static>(
    column: &'a arrow::array::ArrayRef,
    label: &str,
) -> Result<&'a T, String> {
    column
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("root write result {label} column has the wrong Arrow type"))
}

#[cfg(test)]
pub(crate) mod tests;
