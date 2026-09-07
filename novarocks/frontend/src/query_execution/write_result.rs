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

use std::collections::{BTreeMap, BTreeSet};

use arrow::array::{
    Array, BinaryArray, Int8Array, Int32Array, Int64Array, ListArray, MapArray, StringArray,
    StructArray,
};
use novarocks_execution::exec::chunk::Chunk;
use novarocks_spi::connector::write_stack::{
    PreparedWriteSetLedger, ROOT_WRITE_RESULT_BLOB_TYPE_INDEX, ROOT_WRITE_RESULT_BODY_INDEX,
    ROOT_WRITE_RESULT_FRAGMENT_INDEX, ROOT_WRITE_RESULT_INPUT_FIELDS_INDEX,
    ROOT_WRITE_RESULT_KIND_INDEX, ROOT_WRITE_RESULT_PROPERTIES_INDEX,
    ROOT_WRITE_RESULT_ROW_COUNT_INDEX, ROOT_WRITE_RESULT_TARGET_INDEX, RootRowKind,
    RootWriteResultMembershipValidator, RootWriteResultRowShape, RootWriteResultSchema,
    WriteRowCountAccumulator, WriteStatisticsArtifact, WriteTargetOrdinal, row_count_from_wire,
    target_ordinal_from_wire, validate_artifact_draft_nested_values,
};
use novarocks_spi::connector::{
    MAX_CONNECTOR_STATISTICS_ARTIFACTS, MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES,
    MAX_CONNECTOR_STATISTICS_RESULT_BODY_BYTES, StatisticsArtifactDraft,
    StatisticsArtifactIdentity,
};

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
        }
    }

    pub(crate) const fn row_count(&self) -> u64 {
        self.row_count
    }

    pub(crate) fn fragments(&self) -> &[(WriteTargetOrdinal, Vec<u8>)] {
        &self.fragments
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        u64,
        Vec<(WriteTargetOrdinal, Vec<u8>)>,
        Vec<WriteStatisticsArtifact>,
    ) {
        (self.row_count, self.fragments, self.statistics)
    }
}

/// FE-only expectations frozen from this query's TableFinish and the exact
/// write session generation. It is never reconstructed from native wire data.
#[derive(Clone, Debug)]
pub(crate) struct RootWriteDecodeContract {
    expected_targets: BTreeSet<WriteTargetOrdinal>,
    expected_artifacts: BTreeSet<(WriteTargetOrdinal, StatisticsArtifactIdentity)>,
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
pub(crate) struct RootWriteResultDecoder {
    contract: RootWriteDecodeContract,
    rows: WriteRowCountAccumulator,
    ledger: PreparedWriteSetLedger,
    fragments: Vec<(WriteTargetOrdinal, Vec<u8>)>,
    statistics: BTreeMap<(WriteTargetOrdinal, StatisticsArtifactIdentity), WriteStatisticsArtifact>,
    membership: RootWriteResultMembershipValidator,
    body_bytes: usize,
    property_bytes: usize,
    root_eof: bool,
    execution_succeeded: bool,
}

impl RootWriteResultDecoder {
    pub(crate) fn new(contract: RootWriteDecodeContract) -> Self {
        Self {
            contract,
            rows: WriteRowCountAccumulator::new(),
            ledger: PreparedWriteSetLedger::new(),
            fragments: Vec::new(),
            statistics: BTreeMap::new(),
            membership: RootWriteResultMembershipValidator::default(),
            body_bytes: 0,
            property_bytes: 0,
            root_eof: false,
            execution_succeeded: false,
        }
    }

    pub(crate) fn apply_chunk(&mut self, chunk: &Chunk) -> Result<(), String> {
        if self.root_eof {
            return Err("write Root emitted a trailing batch after EOF".into());
        }
        let schema = chunk.schema();
        if schema.as_ref() != self.contract.schema.arrow_schema().as_ref() {
            return Err(format!(
                "write Root schema mismatch: expected {:?}, received {:?}",
                self.contract.schema.arrow_schema(),
                schema
            ));
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
                return Err("root write result row has no kind".to_string());
            }
            let kind = RootRowKind::from_wire(kinds.value(row))
                .map_err(|error| format!("root write result row kind: {error}"))?;
            let target = (!targets.is_null(row)).then(|| targets.value(row));
            let count = (!counts.is_null(row)).then(|| counts.value(row));
            let fragment = (!fragments.is_null(row)).then(|| fragments.value(row));
            let field_count = (!input_fields.is_null(row)).then(|| input_fields.value_length(row));
            let blob_type = (!blob_types.is_null(row)).then(|| blob_types.value(row));
            let body = (!bodies.is_null(row)).then(|| bodies.value(row));
            let property_count = (!properties.is_null(row)).then(|| properties.value_length(row));
            self.membership
                .observe(
                    kind,
                    RootWriteResultRowShape {
                        target,
                        row_count: count,
                        fragment_len: fragment.map(<[u8]>::len),
                        input_fields_len: field_count.map(|value| value as usize),
                        blob_type_len: blob_type.map(str::len),
                        body_len: body.map(<[u8]>::len),
                        properties_len: property_count.map(|value| value as usize),
                    },
                )
                .map_err(|error| format!("write Root row shape: {error}"))?;

            match kind {
                RootRowKind::Summary => {
                    let count = count.expect("validated above");
                    let count = row_count_from_wire(count)
                        .map_err(|error| format!("write Root row count: {error}"))?;
                    self.rows
                        .add(count)
                        .map_err(|error| format!("write Root row count: {error}"))?;
                }
                RootRowKind::PreparedFragment => {
                    let target = target_ordinal_from_wire(target.expect("validated above"))
                        .map_err(|error| format!("write Root target ordinal: {error}"))?;
                    if !self.contract.expected_targets.contains(&target) {
                        return Err(format!(
                            "write Root names target {} outside this query's frozen set",
                            target.get()
                        ));
                    }
                    let fragment = fragment.expect("validated above");
                    self.ledger
                        .reserve_fragment(fragment.len())
                        .map_err(|error| format!("prepared write set budget: {error}"))?;
                    self.fragments.push((target, fragment.to_vec()));
                }
                RootRowKind::ArtifactDraft => {
                    let target = target_ordinal_from_wire(target.expect("validated above"))
                        .map_err(|error| format!("write Root artifact target: {error}"))?;
                    if !self.contract.expected_targets.contains(&target) {
                        return Err(format!(
                            "write Root artifact names target {} outside this query's frozen set",
                            target.get()
                        ));
                    }
                    let fields = input_fields.value(row);
                    let fields = fields
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .ok_or_else(|| "write Root input_fields item is not Int32".to_string())?;
                    let field_values = (0..fields.len())
                        .map(|index| (!fields.is_null(index)).then(|| fields.value(index)))
                        .collect::<Vec<_>>();
                    let entries = properties.value(row);
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
                    let pairs = (0..entries.len())
                        .map(|index| {
                            (
                                (!keys.is_null(index)).then(|| keys.value(index)),
                                (!values.is_null(index)).then(|| values.value(index)),
                            )
                        })
                        .collect::<Vec<_>>();
                    validate_artifact_draft_nested_values(&field_values, &pairs)
                        .map_err(|error| format!("write Root artifact values: {error}"))?;
                    let field_ids = field_values
                        .into_iter()
                        .map(|field| field.expect("validated non-null field"))
                        .collect::<Vec<_>>();
                    let identity = StatisticsArtifactIdentity::try_new(
                        field_ids.clone(),
                        blob_type.expect("validated above"),
                    )
                    .map_err(|error| format!("write Root artifact identity: {error}"))?;
                    let key = (target, identity.clone());
                    if !self.contract.expected_artifacts.contains(&key) {
                        return Err(format!(
                            "write Root emitted an unexpected artifact for target {}: {identity:?}",
                            target.get()
                        ));
                    }
                    if self.statistics.contains_key(&key) {
                        return Err(format!(
                            "write Root emitted a duplicate artifact for target {}: {identity:?}",
                            target.get()
                        ));
                    }
                    if self.statistics.len() >= MAX_CONNECTOR_STATISTICS_ARTIFACTS {
                        return Err("write Root statistics artifact budget exceeded".into());
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
                            .ok_or_else(|| "write Root property budget overflow".to_string())?;
                        property_map.insert(key.to_string(), value.to_string());
                    }
                    self.property_bytes = charge_total(
                        self.property_bytes,
                        row_property_bytes,
                        MAX_CONNECTOR_STATISTICS_PAYLOAD_BYTES,
                        "write Root artifact properties",
                    )?;
                    let draft = StatisticsArtifactDraft::try_new(
                        field_ids,
                        identity.blob_type(),
                        bytes::Bytes::copy_from_slice(body),
                        property_map,
                    )
                    .map_err(|error| format!("write Root artifact draft: {error}"))?;
                    self.statistics
                        .insert(key, WriteStatisticsArtifact::new(target, draft));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn observe_root_eof(&mut self) -> Result<(), String> {
        if std::mem::replace(&mut self.root_eof, true) {
            return Err("write Root emitted duplicate EOF".into());
        }
        Ok(())
    }

    pub(crate) fn observe_execution_success(&mut self) -> Result<(), String> {
        if std::mem::replace(&mut self.execution_succeeded, true) {
            return Err("write execution success was observed twice".into());
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<DecodedPreparedWriteSet, String> {
        if !self.root_eof {
            return Err("write Root EOF was not observed".into());
        }
        if !self.execution_succeeded {
            return Err("write execution did not reach all-success".into());
        }
        self.membership
            .finish()
            .map_err(|error| format!("write Root membership: {error}"))?;
        let observed = self.statistics.keys().cloned().collect::<BTreeSet<_>>();
        if observed != self.contract.expected_artifacts {
            let missing = self
                .contract
                .expected_artifacts
                .difference(&observed)
                .collect::<Vec<_>>();
            return Err(format!(
                "write Root artifact membership is incomplete; missing {missing:?}"
            ));
        }
        Ok(DecodedPreparedWriteSet {
            row_count: self.rows.get(),
            fragments: self.fragments,
            statistics: self.statistics.into_values().collect(),
        })
    }
}

fn charge_total(
    current: usize,
    additional: usize,
    limit: usize,
    label: &str,
) -> Result<usize, String> {
    let total = current
        .checked_add(additional)
        .ok_or_else(|| format!("{label} budget overflow"))?;
    if total > limit {
        return Err(format!("{label} budget exceeded"));
    }
    Ok(total)
}

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
mod tests {
    use std::sync::Arc;

    use arrow::array::builder::{
        Int32Builder, ListBuilder, MapBuilder, MapFieldNames, StringBuilder,
    };
    use arrow::array::{ArrayRef, BinaryArray, Int8Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use novarocks_execution::exec::node::table_write_relation::RootWriteResultRelationSchema;

    use crate::query_execution::write_barrier::WriteCommitBarrier;

    use super::*;

    #[derive(Clone, Copy, Debug)]
    enum CompletionFact {
        RootRows,
        RootEof,
        ExecutionAllSuccess,
        TaskFailure,
        Cancelled,
        DeadlineExpired,
    }

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

    fn targets(count: u32) -> Vec<WriteTargetOrdinal> {
        (0..count)
            .map(|ordinal| WriteTargetOrdinal::try_new(ordinal).expect("bounded ordinal"))
            .collect()
    }

    fn new_decoder(count: u32) -> RootWriteResultDecoder {
        RootWriteResultDecoder::new(RootWriteDecodeContract::for_test(
            targets(count),
            std::iter::empty(),
        ))
    }

    fn finish(mut decoder: RootWriteResultDecoder) -> Result<DecodedPreparedWriteSet, String> {
        decoder.observe_root_eof()?;
        decoder.observe_execution_success()?;
        decoder.finish()
    }

    fn exact_decoder() -> RootWriteResultDecoder {
        let target = WriteTargetOrdinal::try_new(0).expect("target");
        let identity = StatisticsArtifactIdentity::try_new(vec![11], "theta-v1").expect("identity");
        RootWriteResultDecoder::new(RootWriteDecodeContract::for_test(
            [target],
            [(target, identity)],
        ))
    }

    fn permutations(facts: &[CompletionFact]) -> Vec<Vec<CompletionFact>> {
        if facts.is_empty() {
            return vec![Vec::new()];
        }
        let mut result = Vec::new();
        for index in 0..facts.len() {
            let mut remaining = facts.to_vec();
            let fact = remaining.remove(index);
            for mut suffix in permutations(&remaining) {
                let mut permutation = Vec::with_capacity(facts.len());
                permutation.push(fact);
                permutation.append(&mut suffix);
                result.push(permutation);
            }
        }
        result
    }

    fn causal_permutations(facts: &[CompletionFact]) -> Vec<Vec<CompletionFact>> {
        permutations(facts)
            .into_iter()
            .filter(|order| {
                let rows = order
                    .iter()
                    .position(|fact| matches!(fact, CompletionFact::RootRows))
                    .expect("test case has Root rows");
                let eof = order
                    .iter()
                    .position(|fact| matches!(fact, CompletionFact::RootEof))
                    .expect("test case has Root EOF");
                rows < eof
            })
            .collect()
    }

    fn observe_completion_fact(
        decoder: &mut RootWriteResultDecoder,
        barrier: &mut WriteCommitBarrier,
        fact: CompletionFact,
    ) -> Result<(), String> {
        match fact {
            CompletionFact::RootRows => decoder.apply_chunk(&chunk(vec![
                summary(7),
                fragment(0, 3),
                artifact(0, 11, "theta-v1", b"sketch"),
            ])),
            CompletionFact::RootEof => decoder.observe_root_eof(),
            CompletionFact::ExecutionAllSuccess => {
                decoder.observe_execution_success()?;
                barrier.observe_execution_terminals(true);
                Ok(())
            }
            CompletionFact::TaskFailure => {
                barrier.observe_execution_terminals(false);
                Ok(())
            }
            CompletionFact::Cancelled => {
                barrier.observe_cancelled();
                Ok(())
            }
            CompletionFact::DeadlineExpired => {
                barrier.observe_deadline_expired();
                Ok(())
            }
        }
    }

    fn run_completion_order(order: &[CompletionFact]) -> Result<DecodedPreparedWriteSet, String> {
        let mut decoder = exact_decoder();
        let mut barrier = WriteCommitBarrier::new();
        for fact in order {
            observe_completion_fact(&mut decoder, &mut barrier, *fact)?;
        }
        let prepared = decoder.finish()?;
        barrier.observe_prepared_write_set(prepared);
        barrier
            .into_committable()
            .map_err(|blocked| blocked.as_str().to_string())
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
        let mut decoder = RootWriteResultDecoder::new(RootWriteDecodeContract::for_test(
            [target_five],
            std::iter::empty(),
        ));
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
        let mut decoder = RootWriteResultDecoder::new(RootWriteDecodeContract::for_test(
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
    fn eof_and_all_success_are_independent_completion_facts() {
        let mut no_eof = new_decoder(1);
        no_eof.apply_chunk(&chunk(vec![summary(0)])).expect("row");
        no_eof.observe_execution_success().expect("success");
        assert!(
            no_eof
                .finish()
                .unwrap_err()
                .contains("EOF was not observed")
        );

        let mut no_success = new_decoder(1);
        no_success
            .apply_chunk(&chunk(vec![summary(0)]))
            .expect("row");
        no_success.observe_root_eof().expect("EOF");
        assert!(
            no_success
                .finish()
                .unwrap_err()
                .contains("did not reach all-success")
        );
    }

    #[test]
    fn exact_root_rows_eof_and_all_success_accept_every_causal_arrival_order() {
        // Root rows necessarily precede Root EOF, but execution convergence is
        // independent and may be observed before, between, or after them.
        let facts = [
            CompletionFact::RootRows,
            CompletionFact::RootEof,
            CompletionFact::ExecutionAllSuccess,
        ];
        let orders = causal_permutations(&facts);
        assert_eq!(orders.len(), 3);

        for order in orders {
            let prepared = run_completion_order(&order).expect("completion facts open the gate");
            assert_eq!(prepared.row_count(), 7, "{order:?}");
            assert_eq!(prepared.fragments().len(), 1, "{order:?}");
            assert_eq!(prepared.fragments()[0].1, vec![7_u8; 3], "{order:?}");
            assert_eq!(prepared.statistics.len(), 1, "{order:?}");
            assert_eq!(prepared.statistics[0].target().get(), 0, "{order:?}");
            assert_eq!(
                prepared.statistics[0].draft().identity().input_fields(),
                &[11],
                "{order:?}"
            );
            assert_eq!(
                prepared.statistics[0].draft().body().as_ref(),
                b"sketch",
                "{order:?}"
            );
        }
    }

    #[test]
    fn root_eof_before_rows_rejects_every_noncausal_arrival_order() {
        let facts = [
            CompletionFact::RootRows,
            CompletionFact::RootEof,
            CompletionFact::ExecutionAllSuccess,
        ];
        let orders = permutations(&facts)
            .into_iter()
            .filter(|order| {
                let rows = order
                    .iter()
                    .position(|fact| matches!(fact, CompletionFact::RootRows))
                    .expect("Root rows");
                let eof = order
                    .iter()
                    .position(|fact| matches!(fact, CompletionFact::RootEof))
                    .expect("Root EOF");
                eof < rows
            })
            .collect::<Vec<_>>();
        assert_eq!(orders.len(), 3);

        for order in orders {
            let error = run_completion_order(&order).expect_err("EOF closes the Root stream");
            assert!(
                error.contains("trailing batch after EOF"),
                "{order:?}: {error}"
            );
        }
    }

    #[test]
    fn task_failure_cancel_and_deadline_veto_every_causal_arrival_order() {
        let cases: &[(&str, &[CompletionFact], usize, &str)] = &[
            (
                "task failure",
                &[
                    CompletionFact::RootRows,
                    CompletionFact::RootEof,
                    CompletionFact::TaskFailure,
                ],
                3,
                "did not reach all-success",
            ),
            (
                "cancellation",
                &[
                    CompletionFact::RootRows,
                    CompletionFact::RootEof,
                    CompletionFact::ExecutionAllSuccess,
                    CompletionFact::Cancelled,
                ],
                12,
                "cancelled",
            ),
            (
                "deadline",
                &[
                    CompletionFact::RootRows,
                    CompletionFact::RootEof,
                    CompletionFact::ExecutionAllSuccess,
                    CompletionFact::DeadlineExpired,
                ],
                12,
                "deadline expired",
            ),
        ];

        for (name, facts, expected_orders, expected_error) in cases {
            let orders = causal_permutations(facts);
            assert_eq!(orders.len(), *expected_orders, "{name}");
            for order in orders {
                let error = run_completion_order(&order).expect_err("must not commit");
                assert!(error.contains(expected_error), "{name} {order:?}: {error}");
            }
        }
    }
}
