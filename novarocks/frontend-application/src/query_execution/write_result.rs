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
use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
use arrow::array::{
    Array, BinaryArray, Int8Array, Int32Array, Int64Array, ListArray, MapArray, StringArray,
    StructArray,
};
#[cfg(test)]
use novarocks_execution::exec::chunk::Chunk;
use novarocks_native_adapter::root_write_commit_codec::WriteCommitRecordView;
use novarocks_spi::connector::write_stack::{
    PreparedWriteSetLedger, RootRowKind, RootWriteResultMembershipValidator,
    RootWriteResultRowShape, WriteRowCountAccumulator, WriteStatisticsArtifact, WriteTargetOrdinal,
    row_count_from_wire, target_ordinal_from_wire, validate_artifact_draft_nested_values,
};
#[cfg(test)]
use novarocks_spi::connector::write_stack::{
    ROOT_WRITE_RESULT_BLOB_TYPE_INDEX, ROOT_WRITE_RESULT_BODY_INDEX,
    ROOT_WRITE_RESULT_FRAGMENT_INDEX, ROOT_WRITE_RESULT_INPUT_FIELDS_INDEX,
    ROOT_WRITE_RESULT_KIND_INDEX, ROOT_WRITE_RESULT_PROPERTIES_INDEX,
    ROOT_WRITE_RESULT_ROW_COUNT_INDEX, ROOT_WRITE_RESULT_TARGET_INDEX, RootWriteResultSchema,
};
use novarocks_spi::connector::{
    MAX_CONNECTOR_STATISTICS_ARTIFACTS, MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES,
    MAX_CONNECTOR_STATISTICS_RESULT_BODY_BYTES, StatisticsArtifactDraft,
    StatisticsArtifactIdentity,
};

pub(crate) type RootWriteResultParts = (
    u64,
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
            fragments,
            statistics,
            retention: None,
            local_decode: true,
            production_execution: None,
        }
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
    ) -> Result<
        Option<novarocks_spi::connector::ConnectorPayloadRetentionGuard>,
        RootWriteDecodeFailure,
    > {
        if let Some(retention) = &self.retention {
            return retention
                .try_spi_guard()
                .map(Some)
                .map_err(RootWriteDecodeFailure::source);
        }
        #[cfg(test)]
        if self.local_decode {
            return Ok(None);
        }
        Err(RootWriteDecodeFailure::source(
            novarocks_spi::connector::ConnectorError::new(
                novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
                "prepared write decoding requires its original byte-budget author",
            ),
        ))
    }

    pub(crate) fn into_parts(self) -> RootWriteResultParts {
        (
            self.row_count,
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
    #[cfg(test)]
    schema: RootWriteResultSchema,
}

impl RootWriteDecodeContract {
    pub(crate) fn try_new(
        query_targets: &[WriteTargetOrdinal],
        session_targets: &[novarocks_spi::connector::write_stack::ConnectorWriteTargetPlan],
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
        if expected_artifacts.len() > MAX_CONNECTOR_STATISTICS_ARTIFACTS {
            return Err("write Root contract exceeds the statistics artifact limit".into());
        }
        Ok(Self {
            expected_targets,
            expected_artifacts,
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
}

pub(crate) struct RootWriteResultDecoder {
    relay_assembly: RootRecordAssembly,
    relayed_record_count: u64,
    contract: RootWriteDecodeContract,
    rows: WriteRowCountAccumulator,
    ledger: PreparedWriteSetLedger,
    fragments: Vec<(WriteTargetOrdinal, Vec<u8>)>,
    statistics: BTreeMap<(WriteTargetOrdinal, StatisticsArtifactIdentity), WriteStatisticsArtifact>,
    membership: RootWriteResultMembershipValidator,
    body_bytes: usize,
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
            ledger: PreparedWriteSetLedger::new(),
            fragments: Vec::new(),
            statistics: BTreeMap::new(),
            membership: RootWriteResultMembershipValidator::default(),
            body_bytes: 0,
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
        self.apply_row(RootWriteRow {
            kind,
            target: (kind != RootRowKind::Summary).then(|| header.target()),
            count: (kind == RootRowKind::Summary)
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
                },
            )
            .map_err(|error| {
                RootWriteDecodeFailure::source(error).context("write Root row shape")
            })?;

        match kind {
            RootRowKind::Summary => {
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
                self.ledger
                    .reserve_fragment(fragment.len())
                    .map_err(|error| {
                        RootWriteDecodeFailure::source(error).context("prepared write set budget")
                    })?;
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
                if self.statistics.len() >= MAX_CONNECTOR_STATISTICS_ARTIFACTS {
                    return Err(RootWriteDecodeFailure::source(
                        novarocks_spi::connector::ConnectorError::new(
                            novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted,
                            "write Root statistics artifact budget exceeded",
                        ),
                    ));
                }
                let body = body.expect("validated above");
                self.body_bytes = charge_total(
                    self.body_bytes,
                    body.len(),
                    MAX_CONNECTOR_STATISTICS_RESULT_BODY_BYTES,
                    "write Root artifact body",
                )?;
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
pub(crate) mod tests {
    use std::sync::Arc;

    use arrow::array::builder::{
        Int32Builder, ListBuilder, MapBuilder, MapFieldNames, StringBuilder,
    };
    use arrow::array::{ArrayRef, BinaryArray, Int8Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use novarocks_execution::exec::node::table_write_relation::RootWriteResultRelationSchema;

    use super::*;

    struct Row {
        kind: i8,
        target: Option<i32>,
        row_count: Option<i64>,
        fragment: Option<Vec<u8>>,
        input_fields: Option<Vec<Option<i32>>>,
        blob_type: Option<String>,
        body: Option<Vec<u8>>,
        properties: Option<Vec<(Option<String>, Option<String>)>>,
    }

    fn summary(row_count: i64) -> Row {
        Row {
            kind: RootRowKind::SUMMARY,
            target: None,
            row_count: Some(row_count),
            fragment: None,
            input_fields: None,
            blob_type: None,
            body: None,
            properties: None,
        }
    }

    fn fragment(target: i32, bytes: usize) -> Row {
        Row {
            kind: RootRowKind::PREPARED_FRAGMENT,
            target: Some(target),
            row_count: None,
            fragment: Some(vec![7_u8; bytes]),
            input_fields: None,
            blob_type: None,
            body: None,
            properties: None,
        }
    }

    fn artifact(target: i32, field_id: i32, blob_type: &str, body: &[u8]) -> Row {
        Row {
            kind: RootRowKind::ARTIFACT_DRAFT,
            target: Some(target),
            row_count: None,
            fragment: None,
            input_fields: Some(vec![Some(field_id)]),
            blob_type: Some(blob_type.to_string()),
            body: Some(body.to_vec()),
            properties: Some(vec![(Some("ndv".into()), Some("1".into()))]),
        }
    }

    fn chunk(rows: Vec<Row>) -> Chunk {
        let kinds: ArrayRef = Arc::new(Int8Array::from(
            rows.iter().map(|row| row.kind).collect::<Vec<_>>(),
        ));
        let targets: ArrayRef = Arc::new(Int32Array::from(
            rows.iter().map(|row| row.target).collect::<Vec<_>>(),
        ));
        let counts: ArrayRef = Arc::new(Int64Array::from(
            rows.iter().map(|row| row.row_count).collect::<Vec<_>>(),
        ));
        let fragments: ArrayRef = Arc::new(BinaryArray::from(
            rows.iter()
                .map(|row| row.fragment.as_deref())
                .collect::<Vec<_>>(),
        ));
        let mut fields = ListBuilder::new(Int32Builder::new()).with_field(Arc::new(Field::new(
            "item",
            DataType::Int32,
            false,
        )));
        let mut properties = MapBuilder::new(
            Some(MapFieldNames {
                entry: "entries".into(),
                key: "key".into(),
                value: "value".into(),
            }),
            StringBuilder::new(),
            StringBuilder::new(),
        )
        .with_keys_field(Arc::new(Field::new("key", DataType::Utf8, false)))
        .with_values_field(Arc::new(Field::new("value", DataType::Utf8, false)));
        for row in &rows {
            match row.input_fields.as_ref() {
                Some(values) => {
                    for value in values {
                        match value {
                            Some(value) => fields.values().append_value(*value),
                            None => fields.values().append_null(),
                        }
                    }
                    fields.append(true);
                }
                None => fields.append(false),
            }
            match row.properties.as_ref() {
                Some(values) => {
                    for (key, value) in values {
                        match key {
                            Some(key) => properties.keys().append_value(key),
                            None => properties.keys().append_null(),
                        }
                        match value {
                            Some(value) => properties.values().append_value(value),
                            None => properties.values().append_null(),
                        }
                    }
                    properties.append(true).expect("map row");
                }
                None => properties.append(false).expect("null map row"),
            }
        }
        let blob_types: ArrayRef = Arc::new(StringArray::from(
            rows.iter()
                .map(|row| row.blob_type.as_deref())
                .collect::<Vec<_>>(),
        ));
        let bodies: ArrayRef = Arc::new(BinaryArray::from(
            rows.iter()
                .map(|row| row.body.as_deref())
                .collect::<Vec<_>>(),
        ));
        let relation = RootWriteResultRelationSchema::fixed();
        Chunk::try_new_with_columns(
            Arc::clone(relation.chunk_schema()),
            vec![
                kinds,
                targets,
                counts,
                fragments,
                Arc::new(fields.finish()),
                blob_types,
                bodies,
                Arc::new(properties.finish()),
            ],
        )
        .expect("root relation chunk")
    }

    /// Backend record encoder with the original session's real outer envelope.
    #[cfg(novarocks_dev_controls)]
    pub(crate) fn component_write_body(fragment: Vec<u8>) -> Vec<u8> {
        records(vec![
            Row {
                kind: RootRowKind::PREPARED_FRAGMENT,
                target: Some(0),
                row_count: None,
                fragment: Some(fragment),
                input_fields: None,
                blob_type: None,
                body: None,
                properties: None,
            },
            summary(3),
        ])
        .concat()
    }

    fn targets(count: u32) -> Vec<WriteTargetOrdinal> {
        (0..count)
            .map(|ordinal| WriteTargetOrdinal::try_new(ordinal).expect("bounded ordinal"))
            .collect()
    }

    fn new_decoder(count: u32) -> RootWriteResultDecoder {
        RootWriteResultDecoder::for_test_local(RootWriteDecodeContract::for_test(
            targets(count),
            std::iter::empty(),
        ))
    }

    fn finish(mut decoder: RootWriteResultDecoder) -> Result<DecodedPreparedWriteSet, String> {
        decoder
            .observe_root_eof()
            .map_err(|error| error.to_string())?;
        decoder.finish().map_err(|error| error.to_string())
    }

    fn exact_decoder() -> RootWriteResultDecoder {
        let target = WriteTargetOrdinal::try_new(0).expect("target");
        let identity = StatisticsArtifactIdentity::try_new(vec![11], "theta-v1").expect("identity");
        RootWriteResultDecoder::for_test_local(RootWriteDecodeContract::for_test(
            [target],
            [(target, identity)],
        ))
    }

    /// Encode `rows` exactly as the Backend root producer does.
    fn records(rows: Vec<Row>) -> Vec<Vec<u8>> {
        use novarocks_native_adapter::root_record_assembly::{
            RootRecordAssembly, RootRecordDomain,
        };
        use novarocks_native_adapter::root_write_commit_codec::{
            WriteCommitEncoder, WriteCommitTotals,
        };
        let mut encoder = WriteCommitEncoder::try_new(
            chunk(rows).record_batch().clone(),
            WriteCommitTotals::default(),
        )
        .expect("fixed relation");
        let mut stream = Vec::new();
        let mut output = vec![0_u8; 7];
        loop {
            let turn = encoder.step(&mut output).expect("encodable rows");
            stream.extend_from_slice(&output[..turn.emitted_bytes]);
            if turn.status == novarocks_result_render::RenderTurnStatus::InputComplete {
                break;
            }
        }
        let mut assembly = RootRecordAssembly::new(RootRecordDomain::WriteCommit, 1 << 20);
        let mut out = Vec::new();
        for body in stream.chunks(5) {
            assembly
                .push(body, |record| {
                    out.push(record.to_vec());
                    Ok::<(), std::convert::Infallible>(())
                })
                .unwrap();
        }
        assembly.finish().unwrap();
        out
    }

    #[test]
    fn relayed_record_sink_keeps_original_resource_fact_through_domain_projection() {
        use novarocks_failure_contract::FailureProjection;
        let original = PreparedWriteSetLedger::new()
            .reserve_fragment(
                novarocks_spi::connector::write_stack::MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES + 1,
            )
            .unwrap_err();
        let expected = original.failure_fact();
        let body = records(vec![summary(1)]).remove(0);
        let mut assembly = RootRecordAssembly::new(RootRecordDomain::WriteCommit, 1 << 20);
        let rejected = assembly
            .push(&body, |_| {
                Err::<(), RootWriteDecodeFailure>(RootWriteDecodeFailure::source(original.clone()))
            })
            .unwrap_err();
        let failure = RootWriteDecodeFailure::from(rejected);
        assert_eq!(failure.source.as_ref().unwrap().failure_fact(), expected);
        let domain =
            crate::query_execution::internal_consumer::domain_state::InternalDomainFailure::from(
                failure,
            );
        assert_eq!(domain.execution_input().unwrap().fact(), expected);
        let crate::query_execution::internal_consumer::domain_state::InternalDomainFailure::Source(
            source,
        ) = domain
        else {
            unreachable!()
        };
        assert_eq!(
            source.downcast_ref::<novarocks_spi::connector::ConnectorError>(),
            Some(&original)
        );
    }

    #[test]
    fn production_write_decoder_without_author_is_refused_even_in_tests() {
        let mut decoder = RootWriteResultDecoder::new(RootWriteDecodeContract::for_test(
            targets(1),
            std::iter::empty(),
        ));
        let record = records(vec![fragment(0, 16)]).remove(0);
        let error = decoder.apply_relay_body(&record).unwrap_err();
        assert_eq!(
            error
                .source
                .unwrap()
                .downcast_ref::<novarocks_spi::connector::ConnectorError>()
                .unwrap()
                .kind(),
            novarocks_spi::connector::ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(decoder.relay_assembly.retained_bytes(), 0);
        assert!(decoder.fragments.is_empty());
    }

    fn original_assembly_budget(
        bytes: usize,
    ) -> (
        novarocks_workload_control::WorkloadControl,
        novarocks_workload_control::RootWork,
        novarocks_query_application::admitted_query_context::QueryResultCapacityBinding,
    ) {
        use novarocks_workload_control::{
            ResourceConfig, ResultCapacityConfig, ResultWindowClass, WorkClass, WorkRequest,
            WorkloadConfig, WorkloadControl,
        };
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        let mut result = ResultCapacityConfig::V1;
        result.all_objects_bytes[2] = bytes as u64;
        control.configure_result_capacity(result).unwrap();
        control.mark_ready().unwrap();
        let (root, window) = control
            .root_admission()
            .try_begin_root_with_result(
                WorkRequest::new(WorkClass::Management),
                ResultWindowClass::Internal,
            )
            .unwrap();
        let binding = novarocks_query_application::admitted_query_context::QueryResultCapacityBinding::try_new(
            &root.owner.scope(), window.retain_alias(),
        ).unwrap();
        (control, root, binding)
    }

    #[test]
    fn original_result_budget_refuses_partial_header_before_backing_and_preserves_source() {
        let record = records(vec![fragment(0, 16)]).remove(0);
        let header = novarocks_native_adapter::root_write_commit_codec::WRITE_COMMIT_HEADER_BYTES;
        assert!(record.len() > header);
        let (_control, root, binding) = original_assembly_budget(header - 1);
        let window = binding.window_alias();
        let mut decoder = RootWriteResultDecoder::new_with_capacity(
            RootWriteDecodeContract::for_test(targets(1), std::iter::empty()),
            &binding,
        )
        .unwrap();
        let error = decoder.apply_relay_body(&record[..header]).unwrap_err();
        let original = error.source.unwrap();
        assert_eq!(
            original
                .downcast_ref::<novarocks_spi::connector::ConnectorError>()
                .unwrap()
                .kind(),
            novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(decoder.relay_assembly.retained_bytes(), 0);
        assert_eq!(window.retained_payload_bytes(), 0);
        assert!(decoder.fragments.is_empty());
        drop((decoder, binding, window));
        root.owner.complete();
        root.business.release();
    }

    #[test]
    fn partial_replacement_needs_old_and_new_bytes_and_last_partial_exit_refunds() {
        let record = records(vec![fragment(0, 16)]).remove(0);
        let header = novarocks_native_adapter::root_write_commit_codec::WRITE_COMMIT_HEADER_BYTES;
        let (_control, root, binding) = original_assembly_budget(record.len());
        let window = binding.window_alias();
        let mut decoder = RootWriteResultDecoder::new_with_capacity(
            RootWriteDecodeContract::for_test(targets(1), std::iter::empty()),
            &binding,
        )
        .unwrap();
        decoder.apply_relay_body(&record[..1]).unwrap();
        assert_eq!(decoder.relay_assembly.retained_bytes(), header);
        assert_eq!(window.retained_payload_bytes(), header as u64);
        // New full record alone fits; old header plus new record does not.
        let error = decoder.apply_relay_body(&record[1..header]).unwrap_err();
        assert_eq!(
            error
                .source
                .unwrap()
                .downcast_ref::<novarocks_spi::connector::ConnectorError>()
                .unwrap()
                .kind(),
            novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted
        );
        assert_eq!(decoder.relay_assembly.retained_bytes(), header);
        assert_eq!(window.retained_payload_bytes(), header as u64);
        assert!(decoder.fragments.is_empty());
        drop(decoder);
        assert_eq!(window.retained_payload_bytes(), 0);
        drop((binding, window));
        root.owner.complete();
        root.business.release();
    }

    #[test]
    fn replaced_partial_keeps_exact_receipt_through_original_sink_failure_and_drop() {
        let record = records(vec![fragment(0, 16)]).remove(0);
        let header = novarocks_native_adapter::root_write_commit_codec::WRITE_COMMIT_HEADER_BYTES;
        let (_control, root, binding) = original_assembly_budget(header + record.len());
        let window = binding.window_alias();
        let mut assembly = RootRecordAssembly::new(RootRecordDomain::WriteCommit, record.len());
        let mut observations = Vec::new();
        let reserve = |bytes| {
            observations.push((bytes, window.retained_payload_bytes()));
            window
                .reserve_payload_bytes(bytes as u64)
                .map(novarocks_spi::connector::ConnectorPayloadRetentionGuard::new)
                .map_err(RootWriteDecodeFailure::source)
        };
        assembly
            .push_budgeted(&record[..header], reserve, |_| Ok(()))
            .unwrap();
        assert_eq!(observations, [(header, 0), (record.len(), header as u64)]);
        assert_eq!(assembly.retained_bytes(), record.len());
        assert_eq!(window.retained_payload_bytes(), record.len() as u64);
        let original = PreparedWriteSetLedger::new()
            .reserve_fragment(
                novarocks_spi::connector::write_stack::MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES + 1,
            )
            .unwrap_err();
        let error = assembly
            .push_budgeted(
                &record[header..],
                |bytes| {
                    window
                        .reserve_payload_bytes(bytes as u64)
                        .map(novarocks_spi::connector::ConnectorPayloadRetentionGuard::new)
                        .map_err(RootWriteDecodeFailure::source)
                },
                |received| {
                    assert_eq!(received, record.as_slice());
                    Err(RootWriteDecodeFailure::source(original.clone()))
                },
            )
            .unwrap_err();
        let error = RootWriteDecodeFailure::from(error);
        assert_eq!(
            error
                .source
                .unwrap()
                .downcast_ref::<novarocks_spi::connector::ConnectorError>(),
            Some(&original)
        );
        assert_eq!(window.retained_payload_bytes(), record.len() as u64);
        drop(assembly);
        assert_eq!(window.retained_payload_bytes(), 0);
        drop((binding, window));
        root.owner.complete();
        root.business.release();
    }

    #[test]
    fn complete_borrowed_record_charges_only_its_retained_fragment_and_hands_receipt_off() {
        let record = records(vec![fragment(0, 16)]).remove(0);
        let (_control, root, binding) = original_assembly_budget(16);
        let window = binding.window_alias();
        let mut decoder = RootWriteResultDecoder::new_with_capacity(
            RootWriteDecodeContract::for_test(targets(1), std::iter::empty()),
            &binding,
        )
        .unwrap();
        decoder.apply_relay_body(&record).unwrap();
        decoder
            .apply_relay_body(&records(vec![summary(1)]).remove(0))
            .unwrap();
        assert_eq!(decoder.relay_assembly.retained_bytes(), 0);
        assert_eq!(window.retained_payload_bytes(), 16);
        let prepared = finish(decoder).unwrap();
        assert_eq!(window.retained_payload_bytes(), 16);
        drop(prepared);
        assert_eq!(window.retained_payload_bytes(), 0);
        drop((binding, window));
        root.owner.complete();
        root.business.release();
    }

    #[test]
    fn prepared_payload_and_last_statistics_body_clone_retain_actual_capacity() {
        let (_control, root, binding, capacity) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let target = WriteTargetOrdinal::try_new(0).unwrap();
        let contract = RootWriteDecodeContract::for_test(
            [target],
            [(
                target,
                StatisticsArtifactIdentity::try_new(vec![11], "theta-v1").unwrap(),
            )],
        );
        let mut decoder = RootWriteResultDecoder::new_with_capacity(contract, &binding).unwrap();
        for record in records(vec![
            fragment(0, 5),
            artifact(0, 11, "theta-v1", b"sketch"),
            summary(9),
        ]) {
            decoder.apply_relay_body(&record).unwrap();
        }
        let prepared = finish(decoder).unwrap();
        let body = prepared.statistics[0].draft().body().clone();
        drop(binding);
        root.owner.complete();
        root.business.release();
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        drop(prepared);
        assert_eq!(body.as_ref(), b"sketch");
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        drop(body);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[test]
    fn fragment_only_prepared_set_retains_capacity_until_its_payload_exits() {
        let (_control, root, binding, capacity) =
            crate::query_execution::internal_result_cpu::admitted_internal_fixture();
        let mut decoder = RootWriteResultDecoder::new_with_capacity(
            RootWriteDecodeContract::for_test(targets(1), std::iter::empty()),
            &binding,
        )
        .unwrap();
        for record in records(vec![fragment(0, 5), summary(9)]) {
            decoder.apply_relay_body(&record).unwrap();
        }
        let prepared = finish(decoder).unwrap();
        drop(binding);
        root.owner.complete();
        root.business.release();
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        drop(prepared);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[test]
    fn relayed_records_decode_to_the_same_prepared_set_as_the_arrow_relation() {
        let rows = || {
            vec![
                fragment(0, 5),
                artifact(0, 11, "theta-v1", b"sketch"),
                fragment(0, 3),
                summary(9),
            ]
        };
        let mut from_chunk = exact_decoder();
        from_chunk.apply_chunk(&chunk(rows())).unwrap();
        let from_chunk = finish(from_chunk).unwrap();
        let mut from_records = exact_decoder();
        for record in records(rows()) {
            for body in record.chunks(3) {
                from_records.apply_relay_body(body).unwrap();
            }
        }
        let from_records = finish(from_records).unwrap();
        assert_eq!(from_records.row_count, from_chunk.row_count);
        assert_eq!(from_records.fragments, from_chunk.fragments);
        assert_eq!(from_records.statistics, from_chunk.statistics);
    }

    #[test]
    fn relayed_body_end_refuses_partial_record_without_publishing_eof() {
        let record = records(vec![fragment(0, 5)]).remove(0);
        let mut decoder = new_decoder(1);
        decoder.apply_relay_body(&record[..7]).unwrap();
        assert!(
            decoder
                .observe_root_eof()
                .unwrap_err()
                .contains("unfinished")
        );
        assert!(!decoder.root_eof);
        decoder.apply_relay_body(&record[7..]).unwrap();
        let tail = records(vec![summary(5)]).remove(0);
        decoder.apply_relay_body(&tail).unwrap();
        assert!(decoder.check_relay_end(1).is_err());
        assert!(!decoder.root_eof);
        decoder.check_relay_end(2).unwrap();
        let prepared = finish(decoder).unwrap();
        assert_eq!(prepared.row_count(), 5);
    }

    #[test]
    fn relayed_records_obey_membership_targets_and_artifact_sets() {
        // A target outside the frozen set is refused.
        let mut decoder = new_decoder(1);
        let foreign = records(vec![fragment(3, 1), summary(0)]);
        assert!(decoder.apply_record(&foreign[0]).is_err());
        // An unexpected artifact identity is refused.
        let mut decoder = exact_decoder();
        let unexpected = records(vec![artifact(0, 12, "theta-v1", b"x")]);
        assert!(decoder.apply_record(&unexpected[0]).is_err());
        // A missing SUMMARY fails at finish; records after EOF are refused.
        let mut decoder = new_decoder(1);
        let only_fragment = records(vec![fragment(0, 1)]);
        decoder.apply_record(&only_fragment[0]).unwrap();
        decoder.observe_root_eof().unwrap();
        assert!(decoder.apply_record(&only_fragment[0]).is_err());
        assert!(decoder.finish().is_err());
        // A truncated record is refused before any fact is applied.
        let mut decoder = new_decoder(1);
        let record = &records(vec![fragment(0, 4), summary(1)])[0];
        assert!(decoder.apply_record(&record[..record.len() - 1]).is_err());
    }

    #[test]
    fn a_complete_set_spans_several_batches_and_keeps_its_fragments() {
        let mut decoder = new_decoder(2);
        decoder
            .apply_chunk(&chunk(vec![summary(42), fragment(0, 8)]))
            .expect("first batch");
        decoder
            .apply_chunk(&chunk(vec![fragment(1, 16), fragment(0, 4)]))
            .expect("second batch");
        let set = finish(decoder).expect("complete set");
        assert_eq!(set.row_count(), 42);
        assert_eq!(set.fragments().len(), 3);
        assert_eq!(set.fragments()[0].0.get(), 0);
        assert_eq!(set.fragments()[1].0.get(), 1);
        assert_eq!(set.fragments()[0].1.len(), 8);
    }

    #[test]
    fn a_write_that_staged_nothing_is_still_a_complete_set() {
        let mut decoder = new_decoder(1);
        decoder
            .apply_chunk(&chunk(vec![summary(0)]))
            .expect("batch");
        let set = finish(decoder).expect("complete set");
        assert_eq!(set.row_count(), 0);
        assert!(set.fragments().is_empty());
    }

    #[test]
    fn a_prefix_without_a_summary_is_not_a_partial_write_but_no_write() {
        let mut decoder = new_decoder(1);
        decoder
            .apply_chunk(&chunk(vec![fragment(0, 8)]))
            .expect("batch");
        let error = finish(decoder).expect_err("no summary");
        assert!(
            error.contains("missing its SUMMARY row"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn two_summary_rows_would_mean_two_roots_and_are_refused() {
        let mut decoder = new_decoder(1);
        let error = decoder
            .apply_chunk(&chunk(vec![summary(1), summary(2)]))
            .expect_err("two summaries");
        assert!(
            error.contains("more than one SUMMARY"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn a_row_whose_payload_contradicts_its_kind_is_refused() {
        // A summary that also claims a target ordinal.
        let mut decoder = new_decoder(1);
        assert!(
            decoder
                .apply_chunk(&chunk(vec![Row {
                    kind: RootRowKind::SUMMARY,
                    target: Some(0),
                    row_count: Some(1),
                    fragment: None,
                    input_fields: None,
                    blob_type: None,
                    body: None,
                    properties: None,
                }]))
                .is_err()
        );

        // A fragment row with no fragment.
        let mut decoder = new_decoder(1);
        assert!(
            decoder
                .apply_chunk(&chunk(vec![Row {
                    kind: RootRowKind::PREPARED_FRAGMENT,
                    target: Some(0),
                    row_count: None,
                    fragment: None,
                    input_fields: None,
                    blob_type: None,
                    body: None,
                    properties: None,
                }]))
                .is_err()
        );

        // An unknown kind.
        let mut decoder = new_decoder(1);
        assert!(
            decoder
                .apply_chunk(&chunk(vec![Row {
                    kind: 9,
                    target: None,
                    row_count: Some(1),
                    fragment: None,
                    input_fields: None,
                    blob_type: None,
                    body: None,
                    properties: None,
                }]))
                .is_err()
        );
    }

    #[test]
    fn a_fragment_naming_a_target_outside_the_sealed_set_is_refused() {
        let mut decoder = new_decoder(1);
        let error = decoder
            .apply_chunk(&chunk(vec![fragment(1, 4)]))
            .expect_err("foreign target");
        assert!(
            error.contains("outside this query's frozen set"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn a_negative_row_count_is_corrupt_rather_than_a_huge_unsigned_one() {
        let mut decoder = new_decoder(1);
        let error = decoder
            .apply_chunk(&chunk(vec![summary(-1)]))
            .expect_err("negative row count");
        assert!(error.contains("row shape"), "unexpected error: {error}");
    }

    #[test]
    fn the_frontend_recharges_the_budgets_the_backend_already_charged() {
        use novarocks_spi::connector::write_stack::MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES;

        let mut decoder = new_decoder(1);
        decoder
            .apply_chunk(&chunk(vec![fragment(
                0,
                MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES,
            )]))
            .expect("exactly at the single-fragment bound");

        let mut decoder = new_decoder(1);
        let error = decoder
            .apply_chunk(&chunk(vec![fragment(
                0,
                MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES + 1,
            )]))
            .expect_err("over the single-fragment bound");
        assert!(error.contains("budget"), "unexpected error: {error}");
    }

    #[test]
    fn query_local_membership_is_exact_not_max_ordinal_based() {
        let target_five = WriteTargetOrdinal::try_new(5).expect("target");
        let mut decoder = RootWriteResultDecoder::for_test_local(
            RootWriteDecodeContract::for_test([target_five], std::iter::empty()),
        );
        let error = decoder
            .apply_chunk(&chunk(vec![fragment(4, 4)]))
            .expect_err("target below the maximum is still foreign");
        assert!(error.contains("outside this query's frozen set"));
    }

    #[test]
    fn artifact_identity_includes_the_query_local_target() {
        let target_zero = WriteTargetOrdinal::try_new(0).expect("target");
        let target_one = WriteTargetOrdinal::try_new(1).expect("target");
        let identity =
            StatisticsArtifactIdentity::try_new(vec![11], "theta-v1").expect("artifact identity");
        let mut decoder =
            RootWriteResultDecoder::for_test_local(RootWriteDecodeContract::for_test(
                [target_zero, target_one],
                [
                    (target_zero, identity.clone()),
                    (target_one, identity.clone()),
                ],
            ));
        decoder
            .apply_chunk(&chunk(vec![
                summary(2),
                artifact(0, 11, "theta-v1", b"a"),
                artifact(1, 11, "theta-v1", b"b"),
            ]))
            .expect("two target-qualified artifacts");
        let complete = finish(decoder).expect("complete Root result");
        assert_eq!(complete.statistics.len(), 2);
        assert_eq!(complete.statistics[0].target(), target_zero);
        assert_eq!(complete.statistics[1].target(), target_one);
    }

    #[test]
    fn the_decoder_requires_eof_and_does_not_own_task_success() {
        let mut no_eof = new_decoder(1);
        no_eof.apply_chunk(&chunk(vec![summary(0)])).expect("row");
        assert!(
            no_eof
                .finish()
                .unwrap_err()
                .contains("EOF was not observed")
        );

        let mut complete_root = new_decoder(1);
        complete_root
            .apply_chunk(&chunk(vec![summary(0)]))
            .expect("row");
        complete_root.observe_root_eof().expect("EOF");
        assert_eq!(
            complete_root
                .finish()
                .expect("complete Root relation")
                .row_count(),
            0
        );
    }

    #[test]
    fn exact_root_rows_and_eof_preserve_the_original_complete_payload() {
        let mut decoder = exact_decoder();
        decoder
            .apply_chunk(&chunk(vec![
                summary(7),
                fragment(0, 3),
                artifact(0, 11, "theta-v1", b"sketch"),
            ]))
            .unwrap();
        decoder.observe_root_eof().unwrap();
        let prepared = decoder.finish().expect("complete Root relation");
        assert_eq!(prepared.row_count(), 7);
        assert_eq!(prepared.fragments().len(), 1);
        assert_eq!(prepared.fragments()[0].1, vec![7_u8; 3]);
        assert_eq!(prepared.statistics.len(), 1);
        assert_eq!(prepared.statistics[0].target().get(), 0);
        assert_eq!(
            prepared.statistics[0].draft().identity().input_fields(),
            &[11]
        );
        assert_eq!(prepared.statistics[0].draft().body().as_ref(), b"sketch");
    }

    #[test]
    fn root_eof_before_rows_rejects_the_original_trailing_batch() {
        let mut decoder = exact_decoder();
        decoder.observe_root_eof().unwrap();
        let error = decoder
            .apply_chunk(&chunk(vec![
                summary(7),
                fragment(0, 3),
                artifact(0, 11, "theta-v1", b"sketch"),
            ]))
            .expect_err("EOF closes the Root stream");
        assert!(error.to_string().contains("trailing batch after EOF"));
    }
}
