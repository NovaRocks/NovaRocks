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

//! Provider-neutral row-mutation match collection and validation.
//!
//! This module deliberately knows only the signed SPI contract.  It neither
//! interprets provider identity values nor derives a physical write strategy.

use std::collections::HashSet;
use std::time::Instant;

use arrow::array::{Array, ArrayRef, Int8Array};
use arrow::record_batch::RecordBatch;
use arrow::row::{OwnedRow, RowConverter, SortField};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorMutationMatchContract, ConnectorRequestContext,
    ConnectorRowMutationEffect, ConnectorRowMutationIntent, ConnectorRowMutationSelection,
};

use crate::query_execution::fragment_transport::FetchedQueryBatch;
use novarocks_execution::runtime::query_options::QueryOptions;

const DELETE_EFFECT_TAG: i8 = 1;
const REPLACE_EFFECT_TAG: i8 = 2;
const INSERT_EFFECT_TAG: i8 = 3;

/// A non-concatenating collector for a Copy-on-Write match result.
///
/// The row budget is intentionally capped by the byte budget: every retained
/// row consumes at least one byte of the result budget, while Arrow's actual
/// allocation cost is accounted independently through `get_array_memory_size`.
pub struct BoundedRowMutationMatchCollector {
    context: ConnectorRequestContext,
    max_rows: u64,
    max_bytes: u64,
    row_count: u64,
    byte_count: u64,
    batches: Vec<RecordBatch>,
}

impl BoundedRowMutationMatchCollector {
    /// Convenience constructor for the coordinator's admitted query options.
    pub fn try_from_query_options(
        context: ConnectorRequestContext,
        options: &QueryOptions,
    ) -> Result<Self, ConnectorError> {
        Self::try_new(context, options.exec_mem_limit())
    }

    /// Creates a collector with the smaller of connector payload and effective
    /// execution-memory limits.  Non-positive memory limits are not effective
    /// limits and therefore do not lower the admitted connector budget.
    pub fn try_new(
        context: ConnectorRequestContext,
        exec_mem_limit: Option<i64>,
    ) -> Result<Self, ConnectorError> {
        let connector_budget = u64::try_from(context.max_total_payload_bytes()).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "row-mutation connector payload budget does not fit u64",
            )
        })?;
        let effective_memory_budget = exec_mem_limit
            .and_then(|limit| u64::try_from(limit).ok())
            .filter(|limit| *limit > 0)
            .unwrap_or(connector_budget);
        let max_bytes = connector_budget.min(effective_memory_budget);
        if max_bytes == 0 {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "row-mutation match collection has no usable byte budget",
            ));
        }
        Ok(Self {
            context,
            max_rows: max_bytes,
            max_bytes,
            row_count: 0,
            byte_count: 0,
            batches: Vec::new(),
        })
    }

    pub const fn max_rows(&self) -> u64 {
        self.max_rows
    }

    pub const fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    pub const fn row_count(&self) -> u64 {
        self.row_count
    }

    pub const fn byte_count(&self) -> u64 {
        self.byte_count
    }

    /// Retains one result batch without concatenating it with prior batches.
    pub fn push(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        self.check_control()?;
        let rows = u64::try_from(batch.num_rows()).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "row-mutation match batch row count does not fit u64",
            )
        })?;
        let bytes = u64::try_from(batch.get_array_memory_size()).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "row-mutation match batch byte count does not fit u64",
            )
        })?;
        let next_rows = self.row_count.checked_add(rows).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "row-mutation match row accounting overflowed",
            )
        })?;
        let next_bytes = self.byte_count.checked_add(bytes).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "row-mutation match byte accounting overflowed",
            )
        })?;
        if next_rows > self.max_rows || next_bytes > self.max_bytes {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "row-mutation match result exceeds its row or byte budget",
            ));
        }
        self.row_count = next_rows;
        self.byte_count = next_bytes;
        self.batches.push(batch);
        Ok(())
    }

    /// Core owns the opaque native fetched batch.  The coordinator can use
    /// this method without exposing or manufacturing execution-layer chunks.
    pub fn push_fetched(&mut self, batch: FetchedQueryBatch) -> Result<(), ConnectorError> {
        self.push(batch.into_chunk().batch)
    }

    pub fn finish(self) -> Result<ConnectorRowMutationSelection, ConnectorError> {
        self.check_control()?;
        ConnectorRowMutationSelection::try_new(self.batches, self.max_rows, self.max_bytes)
    }

    fn check_control(&self) -> Result<(), ConnectorError> {
        if self.context.cancellation().is_cancelled() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Cancelled,
                "row-mutation match collection cancelled",
            ));
        }
        if Instant::now() >= self.context.deadline() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::DeadlineExceeded,
                "row-mutation match collection deadline elapsed",
            ));
        }
        Ok(())
    }
}

/// Validates that a match result remains within the signed, token-bound
/// contract and that no target row is matched twice.  Insert rows intentionally
/// do not participate in target uniqueness.
pub struct RowMutationMatchValidator {
    contract: ConnectorMutationMatchContract,
    intent: ConnectorRowMutationIntent,
    uniqueness_ordinals: Vec<usize>,
    converter: RowConverter,
    seen: HashSet<OwnedRow>,
}

impl RowMutationMatchValidator {
    pub fn try_new(
        contract: ConnectorMutationMatchContract,
        intent: ConnectorRowMutationIntent,
    ) -> Result<Self, ConnectorError> {
        contract.validate()?;
        intent.validate()?;
        let uniqueness_ordinals = contract
            .uniqueness_tokens()
            .iter()
            .map(|token| {
                match_ordinal(&contract, *token).ok_or_else(|| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        "row-mutation uniqueness token is foreign to the match contract",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let fields = contract
            .uniqueness_tokens()
            .iter()
            .map(|token| {
                match_field(&contract, *token)
                    .map(|field| SortField::new(field.data_type().clone()))
                    .ok_or_else(|| {
                        ConnectorError::new(
                            ConnectorErrorKind::InvalidRequest,
                            "row-mutation uniqueness token is foreign to the match contract",
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let converter = RowConverter::new(fields).map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!("row-mutation uniqueness tuple cannot be canonicalized: {error}"),
            )
        })?;
        Ok(Self {
            contract,
            intent,
            uniqueness_ordinals,
            converter,
            seen: HashSet::new(),
        })
    }

    pub fn validate_batch(&mut self, batch: &RecordBatch) -> Result<(), ConnectorError> {
        self.validate_schema(batch)?;
        let effect_ordinal = usize::try_from(self.contract.effect_field().target_ordinal())
            .map_err(|_| invalid_match("row-mutation effect ordinal does not fit usize"))?;
        let effects = batch
            .column(effect_ordinal)
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| invalid_match("row-mutation effect column is not Int8"))?;
        if effects.null_count() != 0 {
            return Err(invalid_match("row-mutation effect column contains nulls"));
        }
        let uniqueness_columns = self
            .uniqueness_ordinals
            .iter()
            .map(|ordinal| {
                batch
                    .columns()
                    .get(*ordinal)
                    .cloned()
                    .ok_or_else(|| invalid_match("row-mutation uniqueness ordinal is missing"))
            })
            .collect::<Result<Vec<ArrayRef>, _>>()?;
        let rows = self
            .converter
            .convert_columns(&uniqueness_columns)
            .map_err(|error| {
                invalid_match(format!(
                    "row-mutation uniqueness tuple conversion failed: {error}"
                ))
            })?;
        for row_idx in 0..batch.num_rows() {
            let effect = decode_effect(effects.value(row_idx))?;
            if !self.intent.accepts(effect) {
                return Err(invalid_match(
                    "row-mutation effect is not accepted by the signed intent",
                ));
            }
            if effect == ConnectorRowMutationEffect::Insert {
                continue;
            }
            if uniqueness_columns
                .iter()
                .any(|column| column.is_null(row_idx))
            {
                return Err(invalid_match(
                    "row-mutation delete or replace uniqueness tuple contains null",
                ));
            }
            let key = rows.row(row_idx).owned();
            if !self.seen.insert(key) {
                return Err(invalid_match(
                    "row-mutation delete or replace matched the same target more than once",
                ));
            }
        }
        Ok(())
    }

    pub fn validate_selection(
        &mut self,
        selection: &ConnectorRowMutationSelection,
    ) -> Result<(), ConnectorError> {
        selection.validate()?;
        for batch in selection.batches() {
            self.validate_batch(batch)?;
        }
        Ok(())
    }

    fn validate_schema(&self, batch: &RecordBatch) -> Result<(), ConnectorError> {
        for field in self.contract.identity_fields() {
            validate_schema_field(batch, field.source_ordinal(), field.field())?;
        }
        for field in self
            .contract
            .before_fields()
            .iter()
            .chain(self.contract.after_fields())
        {
            validate_schema_field(batch, field.target_ordinal(), field.field())?;
        }
        validate_schema_field(
            batch,
            self.contract.effect_field().target_ordinal(),
            self.contract.effect_field().field(),
        )
    }
}

fn match_ordinal(
    contract: &ConnectorMutationMatchContract,
    token: novarocks_spi::connector::ConnectorWriteFieldToken,
) -> Option<usize> {
    contract
        .identity_fields()
        .iter()
        .find(|field| field.token() == token)
        .map(|field| field.source_ordinal())
        .or_else(|| {
            contract
                .before_fields()
                .iter()
                .chain(contract.after_fields())
                .find(|field| field.token() == token)
                .map(|field| field.target_ordinal())
        })
        .or_else(|| {
            (contract.effect_field().token() == token)
                .then_some(contract.effect_field().target_ordinal())
        })
        .and_then(|ordinal| usize::try_from(ordinal).ok())
}

fn match_field(
    contract: &ConnectorMutationMatchContract,
    token: novarocks_spi::connector::ConnectorWriteFieldToken,
) -> Option<&arrow::datatypes::Field> {
    contract
        .identity_fields()
        .iter()
        .find(|field| field.token() == token)
        .map(|field| field.field())
        .or_else(|| {
            contract
                .before_fields()
                .iter()
                .chain(contract.after_fields())
                .find(|field| field.token() == token)
                .map(|field| field.field())
        })
        .or_else(|| {
            (contract.effect_field().token() == token).then_some(contract.effect_field().field())
        })
}

fn validate_schema_field(
    batch: &RecordBatch,
    ordinal: u32,
    expected: &arrow::datatypes::Field,
) -> Result<(), ConnectorError> {
    let ordinal = usize::try_from(ordinal)
        .map_err(|_| invalid_match("row-mutation field ordinal does not fit usize"))?;
    let schema = batch.schema();
    let actual = schema
        .fields()
        .get(ordinal)
        .ok_or_else(|| invalid_match("row-mutation contract field ordinal is missing"))?;
    if actual.as_ref() != expected {
        return Err(invalid_match(
            "row-mutation match batch schema does not match the signed contract",
        ));
    }
    Ok(())
}

fn decode_effect(value: i8) -> Result<ConnectorRowMutationEffect, ConnectorError> {
    match value {
        DELETE_EFFECT_TAG => Ok(ConnectorRowMutationEffect::Delete),
        REPLACE_EFFECT_TAG => Ok(ConnectorRowMutationEffect::Replace),
        INSERT_EFFECT_TAG => Ok(ConnectorRowMutationEffect::Insert),
        _ => Err(invalid_match("row-mutation effect tag is unknown")),
    }
}

fn invalid_match(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use arrow::array::{Int8Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorExecutionBindingKey, ConnectorInstanceId,
        ConnectorInstanceIncarnation, ConnectorMutationEffectField, ConnectorMutationSourceField,
        ConnectorMutationTargetField, ConnectorRequestContext, ConnectorTableHandle,
        ConnectorWriteBaseVersion, ConnectorWriteFieldToken,
    };

    use super::*;

    #[derive(Default)]
    struct Cancellation(AtomicBool);

    impl ConnectorCancellation for Cancellation {
        fn is_cancelled(&self) -> bool {
            self.0.load(Ordering::Relaxed)
        }
    }

    fn context(cancellation: Arc<Cancellation>, bytes: usize) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            cancellation,
            1,
            bytes,
        )
        .unwrap()
    }

    fn contract() -> ConnectorMutationMatchContract {
        let owner = ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("iceberg").unwrap(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([4; 16]),
        };
        let table = ConnectorTableHandle::try_new(
            owner.instance_id.clone(),
            bytes::Bytes::from_static(b"t"),
        )
        .unwrap();
        let identity = ConnectorMutationSourceField::new(
            ConnectorWriteFieldToken::from_bytes([1; 32]),
            Field::new("identity", DataType::Int32, false),
            0,
        );
        let before = ConnectorMutationTargetField::new(
            ConnectorWriteFieldToken::from_bytes([2; 32]),
            Field::new("before", DataType::Int32, true),
            1,
        );
        let after = ConnectorMutationTargetField::new(
            ConnectorWriteFieldToken::from_bytes([3; 32]),
            Field::new("after", DataType::Int32, true),
            2,
        );
        let effect = ConnectorMutationEffectField::try_new(
            ConnectorWriteFieldToken::from_bytes([4; 32]),
            Field::new("effect", DataType::Int8, false),
            3,
        )
        .unwrap();
        ConnectorMutationMatchContract::try_new(
            owner,
            table,
            ConnectorWriteBaseVersion::try_new(bytes::Bytes::from_static(b"v")).unwrap(),
            vec![identity],
            vec![before],
            vec![after],
            vec![
                ConnectorWriteFieldToken::from_bytes([1; 32]),
                ConnectorWriteFieldToken::from_bytes([2; 32]),
            ],
            effect,
        )
        .unwrap()
    }

    fn batch(rows: Vec<(i32, i32, Option<i32>, i8)>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("identity", DataType::Int32, false),
            Field::new("before", DataType::Int32, true),
            Field::new("after", DataType::Int32, true),
            Field::new("effect", DataType::Int8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int32Array::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int32Array::from(
                    rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                )),
                Arc::new(Int8Array::from(
                    rows.iter().map(|row| row.3).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn collector_keeps_batches_separate_and_uses_smaller_memory_budget() {
        let cancellation = Arc::new(Cancellation::default());
        let first = batch(vec![(1, 10, Some(11), REPLACE_EFFECT_TAG)]);
        let second = batch(vec![(2, 20, Some(21), REPLACE_EFFECT_TAG)]);
        let max_bytes =
            u64::try_from(first.get_array_memory_size() + second.get_array_memory_size()).unwrap();
        let mut collector = BoundedRowMutationMatchCollector::try_new(
            context(cancellation, usize::try_from(max_bytes + 10).unwrap()),
            Some(i64::try_from(max_bytes).unwrap()),
        )
        .unwrap();
        assert_eq!(collector.max_bytes(), max_bytes);
        assert_eq!(collector.max_rows(), max_bytes);
        collector.push(first).unwrap();
        collector.push(second).unwrap();
        let selection = collector.finish().unwrap();
        assert_eq!(selection.batches().len(), 2);
        assert_eq!(selection.row_count(), 2);
        assert_eq!(selection.byte_count(), max_bytes);
    }

    #[test]
    fn collector_rejects_budget_cancel_and_deadline_before_retaining_batch() {
        let cancellation = Arc::new(Cancellation::default());
        let one = batch(vec![(1, 10, Some(11), REPLACE_EFFECT_TAG)]);
        let limit = one.get_array_memory_size();
        let mut collector = BoundedRowMutationMatchCollector::try_new(
            context(Arc::clone(&cancellation), limit),
            None,
        )
        .unwrap();
        collector.push(one.clone()).unwrap();
        let error = collector.push(one).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::ResourceExhausted);
        cancellation.0.store(true, Ordering::Relaxed);
        let error = collector.finish().unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);

        let expired = ConnectorRequestContext::try_new(
            Instant::now() - Duration::from_millis(1),
            Arc::new(Cancellation::default()),
            1,
            32,
        )
        .unwrap();
        let error = BoundedRowMutationMatchCollector::try_new(expired, None)
            .unwrap()
            .push(batch(vec![]))
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::DeadlineExceeded);
    }

    #[test]
    fn validator_uses_composite_canonical_keys_and_excludes_inserts() {
        let mut validator = RowMutationMatchValidator::try_new(
            contract(),
            ConnectorRowMutationIntent::Merge {
                effects: vec![
                    ConnectorRowMutationEffect::Delete,
                    ConnectorRowMutationEffect::Replace,
                    ConnectorRowMutationEffect::Insert,
                ],
            },
        )
        .unwrap();
        validator
            .validate_batch(&batch(vec![
                (1, 10, Some(11), REPLACE_EFFECT_TAG),
                (1, 11, Some(12), INSERT_EFFECT_TAG),
                (1, 11, Some(13), INSERT_EFFECT_TAG),
            ]))
            .unwrap();
        let error = validator
            .validate_batch(&batch(vec![(1, 10, Some(12), DELETE_EFFECT_TAG)]))
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }

    #[test]
    fn validator_fails_closed_for_null_unknown_effect_and_schema_mismatch() {
        let intent = ConnectorRowMutationIntent::Merge {
            effects: vec![
                ConnectorRowMutationEffect::Delete,
                ConnectorRowMutationEffect::Replace,
                ConnectorRowMutationEffect::Insert,
            ],
        };
        let mut null_validator =
            RowMutationMatchValidator::try_new(contract(), intent.clone()).unwrap();
        let null_batch = batch(vec![(1, 10, Some(11), REPLACE_EFFECT_TAG)]);
        let null_columns = vec![
            null_batch.column(0).clone(),
            Arc::new(Int32Array::from(vec![None])) as ArrayRef,
            null_batch.column(2).clone(),
            null_batch.column(3).clone(),
        ];
        let null_batch = RecordBatch::try_new(null_batch.schema(), null_columns).unwrap();
        assert_eq!(
            null_validator
                .validate_batch(&null_batch)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );

        let mut unknown_validator =
            RowMutationMatchValidator::try_new(contract(), intent.clone()).unwrap();
        assert_eq!(
            unknown_validator
                .validate_batch(&batch(vec![(1, 10, Some(11), 0)]))
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );

        let bad_schema = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("identity", DataType::Int64, false),
                Field::new("before", DataType::Int32, true),
                Field::new("after", DataType::Int32, true),
                Field::new("effect", DataType::Int8, false),
            ])),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(Int32Array::from(vec![10])) as ArrayRef,
                Arc::new(Int32Array::from(vec![Some(11)])) as ArrayRef,
                Arc::new(Int8Array::from(vec![REPLACE_EFFECT_TAG])) as ArrayRef,
            ],
        )
        .unwrap();
        let mut schema_validator = RowMutationMatchValidator::try_new(contract(), intent).unwrap();
        assert_eq!(
            schema_validator
                .validate_batch(&bad_schema)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
    }
}
