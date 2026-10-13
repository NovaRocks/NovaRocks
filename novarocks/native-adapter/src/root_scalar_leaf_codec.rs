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

//! Leaf-only ScalarValueV1 Native source bridge. The immutable source stays
//! owned across turns; each turn borrows its selected cell anew. No lifetime
//! extension, hydration, dictionary expansion, or variable-value copy occurs.
//!
//! The caller prepays the cursor box and one-column RecordBatch clone BEFORE
//! construction, proves the complete original Chunk backing separately, and
//! retains its original RootInputPermit until this cursor and its input exit.
//! Arbitrary source metadata-container work also remains the caller's responsibility.
//! These checks neither fund source growth nor admit a root/session assignment.
//! Empty batches have a separate schema-only validation cursor that emits no
//! record. Cumulative row admission and NoRows at sealed End belong to the
//! root session; List/Map/Struct values use the container cursor in
//! `root_scalar_container_codec`.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    DictionaryArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, NullArray, RecordBatch,
    StringArray, Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, FieldRef, Int32Type, TimeUnit};
use novarocks_execution::exec::chunk::Chunk;
use novarocks_result_contract::{
    BorrowedScalarLeaf, FrozenRootOutput, RootOutputContract, ScalarField, ScalarLeafCursor,
    ScalarLeafError, ScalarOpaqueType, ScalarSchema, ScalarTimestampUnit, ScalarValueType,
};
use novarocks_result_render::{RenderTurn, RenderTurnStatus};
use novarocks_type_contract::result_scalar_type::scalar_field_matches_storage;
use novarocks_types::logical::{LogicalType, NR_LOGICAL_TYPE_KEY};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeScalarLeafError {
    Shape,
    EmptyInput,
    Slot,
    Type,
    LogicalMetadata,
    UnsupportedContainer,
    ScratchLimit,
    Leaf(ScalarLeafError),
    ChangedInput,
    Failed,
}
impl std::fmt::Display for NativeScalarLeafError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Shape => "scalar leaf input must contain exactly one column and at most one row",
            Self::EmptyInput => "empty scalar input does not encode a value",
            Self::Slot => "scalar leaf input differs from its exact Native source slot",
            Self::Type => "scalar leaf input differs from its exact physical carrier",
            Self::LogicalMetadata => "scalar leaf logical metadata differs from its frozen facts",
            Self::UnsupportedContainer => "scalar container value requires the container cursor",
            Self::ScratchLimit => "scalar leaf cursor exceeds its prepaid scratch capacity",
            Self::Leaf(error) => return std::fmt::Display::fmt(error, f),
            Self::ChangedInput => "scalar leaf input changed during emission",
            Self::Failed => "Native scalar leaf cursor is failed",
        })
    }
}
impl std::error::Error for NativeScalarLeafError {}
impl From<ScalarLeafError> for NativeScalarLeafError {
    fn from(value: ScalarLeafError) -> Self {
        Self::Leaf(value)
    }
}

/// An existing frozen owner of the cursor's scalar schema. Retaining it is an
/// Arc clone; the schema itself is never copied.
#[derive(Clone)]
pub enum ScalarSchemaOwner {
    Schema(Arc<ScalarSchema>),
    Contract(Arc<RootOutputContract>),
}
impl ScalarSchemaOwner {
    /// Only a ScalarValue root output owns a scalar schema.
    pub fn try_from_contract(
        contract: Arc<RootOutputContract>,
    ) -> Result<Self, NativeScalarLeafError> {
        match contract.output() {
            FrozenRootOutput::ScalarValue(_) => Ok(Self::Contract(contract)),
            _ => Err(NativeScalarLeafError::Type),
        }
    }
    pub fn schema(&self) -> &ScalarSchema {
        match self {
            Self::Schema(schema) => schema,
            Self::Contract(contract) => match contract.output() {
                FrozenRootOutput::ScalarValue(schema) => schema,
                _ => unreachable!("scalar schema owner is checked at construction"),
            },
        }
    }
}
impl From<Arc<ScalarSchema>> for ScalarSchemaOwner {
    fn from(schema: Arc<ScalarSchema>) -> Self {
        Self::Schema(schema)
    }
}

/// No extra cursor heap storage exists besides the separately prepaid Box and
/// RecordBatch columns Vec. The schema owner retains an existing frozen owner.
pub struct NativeScalarLeafEncoder {
    batch: RecordBatch,
    slot_field: FieldRef,
    schema: ScalarSchemaOwner,
    candidate_encoded_len: usize,
    offset: usize,
    phase: ScalarLeafPhase,
}

#[derive(Clone, Copy)]
struct ValidatedLeaf {
    encoded_len: usize,
}
#[derive(Clone, Copy)]
enum ScalarLeafPhase {
    Validating { source: u8, offset: usize },
    Encoding(ValidatedLeaf),
    Complete(ValidatedLeaf),
    Failed,
}
impl NativeScalarLeafEncoder {
    pub const fn inline_capacity_bytes() -> usize {
        size_of::<Self>()
    }
    /// Actual one-column Vec clone Layout plus the caller's cursor Box Layout.
    /// Arc clones retain original owners without copying variable metadata.
    pub const fn scratch_capacity_bytes() -> usize {
        size_of::<Self>() + size_of::<ArrayRef>()
    }
    /// Begin a prepaid immutable cursor. No full timezone comparison occurs.
    /// A fixed set of shape/slot/carrier/metadata checks and bounded fixed-coefficient
    /// validation precede all cloning. The caller separately proves complete
    /// original backing and retains its original input permit through exit.
    pub fn try_begin(
        chunk: &Chunk,
        schema: impl Into<ScalarSchemaOwner>,
        prepaid_scratch_capacity: usize,
    ) -> Result<Self, NativeScalarLeafError> {
        Self::try_begin_mode(chunk, schema.into(), prepaid_scratch_capacity, false)
    }
    /// Validate an exactly empty one-column batch under the same original input
    /// and scratch contract. No cell is selected and no ScalarValueV1 record is
    /// emitted. A successful witness exposes Some(0), not a NoRows record: only
    /// the root owner may decide absence after its entire input stream seals.
    pub fn try_validate_empty(
        chunk: &Chunk,
        schema: impl Into<ScalarSchemaOwner>,
        prepaid_scratch_capacity: usize,
    ) -> Result<Self, NativeScalarLeafError> {
        Self::try_begin_mode(chunk, schema.into(), prepaid_scratch_capacity, true)
    }
    fn try_begin_mode(
        chunk: &Chunk,
        owner: ScalarSchemaOwner,
        prepaid_scratch_capacity: usize,
        empty_validation: bool,
    ) -> Result<Self, NativeScalarLeafError> {
        let schema = owner.schema();
        let batch = &chunk.batch;
        if batch.num_columns() != 1
            || batch.schema_ref().fields().len() != 1
            || batch.num_rows() > 1
            || chunk.chunk_schema().slots().len() != 1
        {
            return Err(NativeScalarLeafError::Shape);
        }
        if empty_validation {
            if batch.num_rows() != 0 {
                return Err(NativeScalarLeafError::Shape);
            }
        } else if batch.num_rows() == 0 {
            return Err(NativeScalarLeafError::EmptyInput);
        }
        let slot = &chunk.chunk_schema().slots()[0];
        if schema.source_slot() != Some(slot.slot_id().as_u32()) {
            return Err(NativeScalarLeafError::Slot);
        }
        reject_container(schema.field())?;
        if !slot.field_schema().children().is_empty()
            || slot.field_schema().logical_type() != expected_logical(&schema.field().value_type)
        {
            return Err(NativeScalarLeafError::LogicalMetadata);
        }
        if Self::scratch_capacity_bytes() > prepaid_scratch_capacity {
            return Err(NativeScalarLeafError::ScratchLimit);
        }
        validate_field_preflight(schema.field(), slot.field())?;
        validate_field_preflight(schema.field(), &batch.schema_ref().fields()[0])?;
        let array = batch.column(0).as_ref();
        if array.len() != usize::from(!empty_validation)
            || !carrier_preflight_matches(
                schema.field(),
                array.data_type(),
                schema.field().nullable,
            )
        {
            return Err(NativeScalarLeafError::Type);
        }
        // At most 304 fixed coefficient limbs plus constant selected-cell work.
        // Variable lengths are checked without scanning/copying their payload.
        let candidate_encoded_len = if empty_validation {
            validate_empty_carrier(schema.field(), array)?;
            0
        } else {
            ScalarLeafCursor::try_new(schema, borrow_validated_leaf(schema.field(), array)?)?
                .encoded_len()
        };
        Ok(Self {
            batch: batch.clone(),
            slot_field: Arc::clone(slot.field_ref()),
            schema: owner,
            candidate_encoded_len,
            offset: 0,
            phase: ScalarLeafPhase::Validating {
                source: 0,
                offset: 0,
            },
        })
    }
    /// A length is available only after the private immutable validation witness.
    pub const fn encoded_len(&self) -> Option<usize> {
        match self.phase {
            ScalarLeafPhase::Encoding(witness) | ScalarLeafPhase::Complete(witness) => {
                Some(witness.encoded_len)
            }
            ScalarLeafPhase::Validating { .. } | ScalarLeafPhase::Failed => None,
        }
    }
    /// Validation advances even with empty output, but never writes output or
    /// completes a row. Its final value-validation turn yields before encoding;
    /// empty validation completes immediately with a zero-length witness.
    pub fn step(&mut self, output: &mut [u8]) -> Result<RenderTurn, NativeScalarLeafError> {
        if matches!(self.phase, ScalarLeafPhase::Failed) {
            return Err(NativeScalarLeafError::Failed);
        }
        let result = match self.phase {
            ScalarLeafPhase::Validating { source, offset } => self.validate_turn(source, offset),
            ScalarLeafPhase::Encoding(witness) => self.encode_turn(witness, output),
            ScalarLeafPhase::Complete(_) => Ok(RenderTurn {
                emitted_bytes: 0,
                examined_bytes: 0,
                visited_cells: 0,
                completed_rows: 0,
                status: RenderTurnStatus::InputComplete,
            }),
            ScalarLeafPhase::Failed => unreachable!("failed phase checked before dispatch"),
        };
        if result.is_err() {
            self.phase = ScalarLeafPhase::Failed;
        }
        result
    }
    fn source_zone(&self, source: u8) -> Option<&str> {
        let data_type = match source {
            0 => self.slot_field.data_type(),
            1 => self.batch.schema_ref().fields()[0].data_type(),
            2 => self.batch.column(0).data_type(),
            _ => unreachable!("closed timezone source index"),
        };
        match data_type {
            DataType::Timestamp(_, zone) => zone.as_deref(),
            _ => None,
        }
    }
    fn validate_turn(
        &mut self,
        mut source: u8,
        mut offset: usize,
    ) -> Result<RenderTurn, NativeScalarLeafError> {
        let mut examined = 0;
        let mut work = 0;
        while source < 3 && work + 8 <= 1024 {
            let expected = match &self.schema.schema().field().value_type {
                ScalarValueType::Timestamp { timezone, .. } => timezone.as_deref(),
                _ => None,
            };
            let actual = self.source_zone(source);
            work += 8;
            match (expected, actual) {
                (None, None) => {
                    source += 1;
                    offset = 0;
                }
                (Some(expected), Some(actual)) if expected.len() == actual.len() => {
                    if offset > expected.len() {
                        return Err(NativeScalarLeafError::ChangedInput);
                    }
                    let count = (expected.len() - offset).min((64 * 1024 - examined) / 2);
                    if count == 0 && offset < expected.len() {
                        break;
                    }
                    let end = offset + count;
                    // Count both borrowed inputs, including the frozen schema.
                    if expected.as_bytes()[offset..end] != actual.as_bytes()[offset..end] {
                        return Err(NativeScalarLeafError::Type);
                    }
                    examined += 2 * count;
                    offset = end;
                    if offset == expected.len() {
                        source += 1;
                        offset = 0;
                    }
                }
                _ => return Err(NativeScalarLeafError::Type),
            }
        }
        let empty_complete = source == 3 && self.candidate_encoded_len == 0;
        self.phase = if source == 3 {
            let witness = ValidatedLeaf {
                encoded_len: self.candidate_encoded_len,
            };
            if empty_complete {
                ScalarLeafPhase::Complete(witness)
            } else {
                ScalarLeafPhase::Encoding(witness)
            }
        } else {
            ScalarLeafPhase::Validating { source, offset }
        };
        Ok(RenderTurn {
            emitted_bytes: 0,
            examined_bytes: examined,
            visited_cells: work,
            completed_rows: 0,
            status: if empty_complete {
                RenderTurnStatus::InputComplete
            } else {
                RenderTurnStatus::Yielded
            },
        })
    }
    fn encode_turn(
        &mut self,
        witness: ValidatedLeaf,
        output: &mut [u8],
    ) -> Result<RenderTurn, NativeScalarLeafError> {
        let cursor = ScalarLeafCursor::try_new(
            self.schema.schema(),
            borrow_validated_leaf(self.schema.schema().field(), self.batch.column(0).as_ref())?,
        )?;
        if cursor.encoded_len() != witness.encoded_len {
            return Err(NativeScalarLeafError::ChangedInput);
        }
        let turn = cursor.copy_range(self.offset, output)?;
        self.offset = self
            .offset
            .checked_add(turn.emitted_bytes)
            .ok_or(NativeScalarLeafError::ChangedInput)?;
        if turn.complete {
            self.phase = ScalarLeafPhase::Complete(witness);
        }
        Ok(RenderTurn {
            emitted_bytes: turn.emitted_bytes,
            examined_bytes: 0,
            visited_cells: 512,
            completed_rows: u64::from(turn.complete),
            status: if turn.complete {
                RenderTurnStatus::InputComplete
            } else {
                RenderTurnStatus::NeedsOutput
            },
        })
    }
}

fn reject_container(field: &ScalarField) -> Result<(), NativeScalarLeafError> {
    if matches!(
        field.value_type,
        ScalarValueType::List(_) | ScalarValueType::Map { .. } | ScalarValueType::Struct(_)
    ) {
        return Err(NativeScalarLeafError::UnsupportedContainer);
    }
    Ok(())
}
pub(crate) fn expected_logical(value: &ScalarValueType) -> Option<LogicalType> {
    match value {
        ScalarValueType::Json => Some(LogicalType::Json),
        ScalarValueType::Opaque(ScalarOpaqueType::Hll) => Some(LogicalType::Hll),
        ScalarValueType::Opaque(ScalarOpaqueType::Bitmap) => Some(LogicalType::Bitmap),
        ScalarValueType::Opaque(ScalarOpaqueType::Object) => Some(LogicalType::Object),
        ScalarValueType::Opaque(ScalarOpaqueType::Percentile) => Some(LogicalType::Percentile),
        _ => None,
    }
}
fn validate_field_preflight(
    expected: &ScalarField,
    actual: &Field,
) -> Result<(), NativeScalarLeafError> {
    if !carrier_preflight_matches(expected, actual.data_type(), actual.is_nullable()) {
        return Err(NativeScalarLeafError::Type);
    }
    // Borrow the canonical fact; normalizing with logical_type_of_field would
    // allocate and could conceal a stale/forged cached logical classification.
    let logical = actual
        .metadata()
        .get(NR_LOGICAL_TYPE_KEY)
        .map(String::as_str);
    let complete = logical
        .map(novarocks_type_contract::ValueLogicalType::from_metadata_value)
        .transpose()
        .map_err(|_| NativeScalarLeafError::LogicalMetadata)?;
    let expected_complete = match expected.value_type {
        ScalarValueType::LargeInt => Some(novarocks_type_contract::ValueLogicalType::LargeInt),
        ScalarValueType::Variant => Some(novarocks_type_contract::ValueLogicalType::Variant),
        _ => expected_logical(&expected.value_type).map(LogicalType::semantic_identity),
    };
    // Original unmarked LARGEINT/Variant sources remain valid. A complete
    // marker, when present, must match this exact frozen scalar schema.
    let historical_unmarked = complete.is_none()
        && matches!(
            expected.value_type,
            ScalarValueType::LargeInt | ScalarValueType::Variant
        );
    if !historical_unmarked && complete != expected_complete {
        return Err(NativeScalarLeafError::LogicalMetadata);
    }
    Ok(())
}
fn dictionary_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Dictionary(key, value)
        if key.as_ref() == &DataType::Int32 && matches!(value.as_ref(), DataType::Utf8 | DataType::LargeUtf8))
}
fn carrier_preflight_matches(expected: &ScalarField, actual: &DataType, nullable: bool) -> bool {
    if dictionary_type(actual) {
        expected.nullable == nullable
            && matches!(
                expected.value_type,
                ScalarValueType::String | ScalarValueType::Json
            )
    } else if let ScalarValueType::Timestamp { unit, timezone } = &expected.value_type {
        let DataType::Timestamp(actual_unit, actual_zone) = actual else {
            return false;
        };
        expected.nullable == nullable
            && matches!(
                (unit, actual_unit),
                (ScalarTimestampUnit::Microsecond, TimeUnit::Microsecond)
                    | (ScalarTimestampUnit::Nanosecond, TimeUnit::Nanosecond)
            )
            && match (timezone.as_deref(), actual_zone.as_deref()) {
                (None, None) => true,
                (Some(expected), Some(actual)) => expected.len() == actual.len(),
                _ => false,
            }
    } else {
        scalar_field_matches_storage(expected, actual, nullable)
    }
}
fn exact<T: Array + 'static>(array: &dyn Array) -> Result<&T, NativeScalarLeafError> {
    let value = array
        .as_any()
        .downcast_ref::<T>()
        .ok_or(NativeScalarLeafError::Type)?;
    if std::ptr::from_ref(array).cast::<()>() != std::ptr::from_ref(value).cast::<()>()
        || size_of_val(array) != size_of::<T>()
        || align_of_val(array) != align_of::<T>()
    {
        return Err(NativeScalarLeafError::Type);
    }
    Ok(value)
}
// Verify closed concrete carriers without touching a selected cell. In particular,
// an empty dictionary may retain a nonempty values array, but no key/value index
// or null bitmap bit is read here. Original backing inspection stays with caller.
fn validate_empty_carrier(
    expected: &ScalarField,
    array: &dyn Array,
) -> Result<(), NativeScalarLeafError> {
    use ScalarValueType as S;
    if !array.is_empty() {
        return Err(NativeScalarLeafError::Type);
    }
    if dictionary_type(array.data_type()) {
        let dictionary = exact::<DictionaryArray<Int32Type>>(array)?;
        return match dictionary.values().data_type() {
            DataType::Utf8 => exact::<StringArray>(dictionary.values().as_ref()).map(|_| ()),
            DataType::LargeUtf8 => {
                exact::<LargeStringArray>(dictionary.values().as_ref()).map(|_| ())
            }
            _ => Err(NativeScalarLeafError::Type),
        };
    }
    macro_rules! carrier {
        ($ty:ty) => {
            exact::<$ty>(array).map(|_| ())
        };
    }
    match &expected.value_type {
        S::Null => carrier!(NullArray),
        S::Boolean => carrier!(BooleanArray),
        S::SignedInteger(8) => carrier!(Int8Array),
        S::SignedInteger(16) => carrier!(Int16Array),
        S::SignedInteger(32) => carrier!(Int32Array),
        S::SignedInteger(64) => carrier!(Int64Array),
        S::LargeInt => carrier!(FixedSizeBinaryArray),
        S::Float32 => carrier!(Float32Array),
        S::Float64 => carrier!(Float64Array),
        S::Decimal { bits: 128, .. } => carrier!(Decimal128Array),
        S::Decimal { bits: 256, .. } => carrier!(Decimal256Array),
        S::String | S::Json => carrier!(StringArray),
        S::Binary | S::Opaque(_) => carrier!(BinaryArray),
        S::Variant => carrier!(LargeBinaryArray),
        S::Date => carrier!(Date32Array),
        S::TimeMicros => carrier!(Time64MicrosecondArray),
        S::Timestamp {
            unit: ScalarTimestampUnit::Microsecond,
            ..
        } => {
            carrier!(TimestampMicrosecondArray)
        }
        S::Timestamp {
            unit: ScalarTimestampUnit::Nanosecond,
            ..
        } => {
            carrier!(TimestampNanosecondArray)
        }
        S::List(_) | S::Map { .. } | S::Struct(_) => {
            Err(NativeScalarLeafError::UnsupportedContainer)
        }
        _ => Err(NativeScalarLeafError::Type),
    }
}

// Concrete shape/value preflight and the private witness guard emission.
// Temporary borrows never rescan timezone bytes or escape the current turn.
fn borrow_validated_leaf<'a>(
    expected: &ScalarField,
    array: &'a dyn Array,
) -> Result<BorrowedScalarLeaf<'a>, NativeScalarLeafError> {
    if array.len() != 1 {
        return Err(NativeScalarLeafError::Type);
    }
    borrow_leaf_at(expected, array, 0)
}

/// Select one leaf value of an exact standard carrier at `index`. Container
/// cursors use this for nested children; the top-level leaf path selects 0.
pub(crate) fn borrow_leaf_at<'a>(
    expected: &ScalarField,
    array: &'a dyn Array,
    index: usize,
) -> Result<BorrowedScalarLeaf<'a>, NativeScalarLeafError> {
    use BorrowedScalarLeaf as V;
    use ScalarValueType as S;
    if index >= array.len() {
        return Err(NativeScalarLeafError::Type);
    }
    if dictionary_type(array.data_type()) {
        let dictionary = exact::<DictionaryArray<Int32Type>>(array)?;
        if dictionary.keys().is_null(index) {
            return Ok(V::Null);
        }
        let key = usize::try_from(dictionary.keys().value(index))
            .map_err(|_| NativeScalarLeafError::Type)?;
        let values = dictionary.values();
        if key >= values.len() {
            return Err(NativeScalarLeafError::Type);
        }
        let text = match values.data_type() {
            DataType::Utf8 => {
                let values = exact::<StringArray>(values.as_ref())?;
                if values.is_null(key) {
                    return Ok(V::Null);
                }
                values.value(key)
            }
            DataType::LargeUtf8 => {
                let values = exact::<LargeStringArray>(values.as_ref())?;
                if values.is_null(key) {
                    return Ok(V::Null);
                }
                values.value(key)
            }
            _ => return Err(NativeScalarLeafError::Type),
        };
        return Ok(if matches!(expected.value_type, S::Json) {
            V::Json(text)
        } else {
            V::String(text)
        });
    }
    macro_rules! selected {
        ($ty:ty, $value:ident, $expression:expr) => {{
            let $value = exact::<$ty>(array)?;
            if $value.is_null(index) {
                V::Null
            } else {
                $expression
            }
        }};
    }
    Ok(match &expected.value_type {
        S::Null => {
            exact::<NullArray>(array)?;
            V::Null
        }
        S::Boolean => selected!(BooleanArray, a, V::Boolean(a.value(index))),
        S::SignedInteger(8) => selected!(
            Int8Array,
            a,
            V::SignedInteger {
                bits: 8,
                value: i64::from(a.value(index))
            }
        ),
        S::SignedInteger(16) => selected!(
            Int16Array,
            a,
            V::SignedInteger {
                bits: 16,
                value: i64::from(a.value(index))
            }
        ),
        S::SignedInteger(32) => selected!(
            Int32Array,
            a,
            V::SignedInteger {
                bits: 32,
                value: i64::from(a.value(index))
            }
        ),
        S::SignedInteger(64) => selected!(
            Int64Array,
            a,
            V::SignedInteger {
                bits: 64,
                value: a.value(index)
            }
        ),
        S::LargeInt => selected!(
            FixedSizeBinaryArray,
            a,
            V::LargeInt(i128::from_le_bytes(
                a.value(index)
                    .try_into()
                    .map_err(|_| NativeScalarLeafError::Type)?
            ))
        ),
        S::Float32 => selected!(Float32Array, a, V::Float32(a.value(index).to_bits())),
        S::Float64 => selected!(Float64Array, a, V::Float64(a.value(index).to_bits())),
        S::Decimal {
            bits: 128,
            precision,
            scale,
        } => selected!(
            Decimal128Array,
            a,
            V::Decimal128 {
                coefficient: a.value(index),
                precision: *precision,
                scale: *scale
            }
        ),
        S::Decimal {
            bits: 256,
            precision,
            scale,
        } => selected!(
            Decimal256Array,
            a,
            V::Decimal256 {
                coefficient_le: a.value(index).to_le_bytes(),
                precision: *precision,
                scale: *scale
            }
        ),
        S::String => selected!(StringArray, a, V::String(a.value(index))),
        S::Binary => selected!(BinaryArray, a, V::Binary(a.value(index))),
        S::Json => selected!(StringArray, a, V::Json(a.value(index))),
        S::Variant => selected!(LargeBinaryArray, a, V::Variant(a.value(index))),
        S::Opaque(kind) => selected!(
            BinaryArray,
            a,
            V::Opaque {
                kind: *kind,
                bytes: a.value(index)
            }
        ),
        S::Date => selected!(Date32Array, a, V::Date(a.value(index))),
        S::TimeMicros => selected!(Time64MicrosecondArray, a, V::TimeMicros(a.value(index))),
        S::Timestamp {
            unit: ScalarTimestampUnit::Microsecond,
            ..
        } => selected!(
            TimestampMicrosecondArray,
            a,
            V::Timestamp {
                ticks: a.value(index),
                unit: ScalarTimestampUnit::Microsecond
            }
        ),
        S::Timestamp {
            unit: ScalarTimestampUnit::Nanosecond,
            ..
        } => selected!(
            TimestampNanosecondArray,
            a,
            V::Timestamp {
                ticks: a.value(index),
                unit: ScalarTimestampUnit::Nanosecond
            }
        ),
        S::List(_) | S::Map { .. } | S::Struct(_) => {
            return Err(NativeScalarLeafError::UnsupportedContainer);
        }
        _ => return Err(NativeScalarLeafError::Type),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::i256;
    use novarocks_execution::exec::chunk::{ChunkFieldSchema, ChunkSchema, ChunkSlotSchema};
    use novarocks_result_contract::SCALAR_LEAF_HEADER_BYTES;
    use novarocks_types::SlotId;

    fn schema(value_type: ScalarValueType, nullable: bool) -> Arc<ScalarSchema> {
        Arc::new(
            ScalarSchema::try_new(ScalarField {
                nullable,
                value_type,
            })
            .unwrap()
            .bind_native_slots(&[7])
            .unwrap(),
        )
    }
    #[test]
    fn empty_dictionary_does_not_select_retained_values_and_requires_logical_facts() {
        let values: ArrayRef =
            Arc::new(LargeStringArray::from(vec!["x".repeat(
                novarocks_result_contract::ScalarProfileV1::SINGLE_VALUE_BYTES + 1,
            )]));
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(Int32Array::from(Vec::<i32>::new()), values)
                .unwrap(),
        );
        let source = chunk(
            Arc::clone(&dictionary),
            Some(LogicalType::Json.metadata_value()),
            None,
        );
        let mut cursor = NativeScalarLeafEncoder::try_validate_empty(
            &source,
            schema(ScalarValueType::Json, true),
            NativeScalarLeafEncoder::scratch_capacity_bytes(),
        )
        .unwrap();
        assert_eq!(cursor.encoded_len(), None);
        let mut output = [0xa5; 32];
        let turn = cursor.step(&mut output).unwrap();
        assert_eq!(turn.status, RenderTurnStatus::InputComplete);
        assert_eq!((turn.emitted_bytes, turn.completed_rows), (0, 0));
        assert_eq!(cursor.encoded_len(), Some(0));
        assert_eq!(output, [0xa5; 32]);
        let untrusted = chunk(dictionary, None, None);
        assert!(matches!(
            NativeScalarLeafEncoder::try_validate_empty(
                &untrusted,
                schema(ScalarValueType::Json, true),
                NativeScalarLeafEncoder::scratch_capacity_bytes(),
            ),
            Err(NativeScalarLeafError::LogicalMetadata)
        ));
    }
    // These source fixtures are not original funding/provenance proofs. The
    // leaf bridge is tested separately from the caller's complete Chunk proof.
    fn chunk(array: ArrayRef, logical: Option<&str>, cached: Option<ChunkFieldSchema>) -> Chunk {
        let mut field = Field::new("value", array.data_type().clone(), true);
        if let Some(logical) = logical {
            field =
                field.with_metadata([(NR_LOGICAL_TYPE_KEY.to_owned(), logical.to_owned())].into());
        }
        let slot = ChunkSlotSchema::try_new_with_field(SlotId(7), field, cached, None).unwrap();
        let schema = Arc::new(ChunkSchema::try_new(vec![slot]).unwrap());
        let batch = RecordBatch::try_new(schema.arrow_schema_ref(), vec![array]).unwrap();
        Chunk::try_new_with_chunk_schema(batch, schema).unwrap()
    }
    fn finish_validation(
        cursor: &mut NativeScalarLeafEncoder,
    ) -> Result<(), NativeScalarLeafError> {
        for _ in 0..16 {
            if cursor.encoded_len().is_some() {
                return Ok(());
            }
            let mut untouched = [0xa5; 3];
            let turn = cursor.step(&mut untouched)?;
            assert_eq!(untouched, [0xa5; 3]);
            assert_eq!((turn.emitted_bytes, turn.completed_rows), (0, 0));
            assert_eq!(turn.status, RenderTurnStatus::Yielded);
            assert!(turn.examined_bytes <= 64 * 1024 && turn.visited_cells <= 1024);
        }
        panic!("finite leaf validation failed to complete");
    }
    fn validate_to_completion(
        input: &Chunk,
        expected: &ScalarSchema,
    ) -> Result<(), NativeScalarLeafError> {
        // Test-only schema copies are outside the cursor allocation contract.
        let mut cursor = NativeScalarLeafEncoder::try_begin(
            input,
            Arc::new(expected.clone()),
            NativeScalarLeafEncoder::scratch_capacity_bytes(),
        )?;
        finish_validation(&mut cursor)
    }
    fn encode(input: &Chunk, expected: Arc<ScalarSchema>, quantum: usize) -> Vec<u8> {
        let mut cursor = NativeScalarLeafEncoder::try_begin(
            input,
            expected,
            NativeScalarLeafEncoder::scratch_capacity_bytes(),
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut rows = 0;
        for _ in 0..100_000 {
            let mut output = vec![0; quantum];
            let turn = cursor.step(&mut output).unwrap();
            assert!(turn.visited_cells <= 1024);
            assert!(turn.emitted_bytes <= 64 * 1024);
            rows += turn.completed_rows;
            bytes.extend_from_slice(&output[..turn.emitted_bytes]);
            if turn.status == RenderTurnStatus::InputComplete {
                assert_eq!(rows, 1);
                assert_eq!(cursor.step(&mut output).unwrap().completed_rows, 0);
                return bytes;
            }
        }
        panic!("bounded scalar cursor failed to complete");
    }
    #[test]
    fn shape_and_empty_refuse_before_semantic_metadata() {
        let expected = schema(ScalarValueType::Json, true);
        let empty = chunk(
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Some("forged"),
            None,
        );
        assert_eq!(
            validate_to_completion(&empty, &expected),
            Err(NativeScalarLeafError::EmptyInput)
        );
        let rows = chunk(
            Arc::new(StringArray::from(vec!["a", "b"])),
            Some("forged"),
            None,
        );
        assert_eq!(
            validate_to_completion(&rows, &expected),
            Err(NativeScalarLeafError::Shape)
        );
    }
    #[test]
    fn canonical_metadata_and_cached_facts_must_both_match() {
        let expected = schema(ScalarValueType::Json, true);
        let plain = chunk(Arc::new(StringArray::from(vec!["{}"])), None, None);
        assert_eq!(
            validate_to_completion(&plain, &expected),
            Err(NativeScalarLeafError::LogicalMetadata)
        );
        let fake = ChunkFieldSchema::from_field(
            &Field::new("fake", DataType::Utf8, true)
                .with_metadata([(NR_LOGICAL_TYPE_KEY.to_owned(), "json".to_owned())].into()),
        )
        .unwrap();
        let forged_cache = chunk(Arc::new(StringArray::from(vec!["{}"])), None, Some(fake));
        assert_eq!(
            validate_to_completion(&forged_cache, &expected),
            Err(NativeScalarLeafError::LogicalMetadata)
        );
        let normalized = chunk(
            Arc::new(StringArray::from(vec!["{}"])),
            Some(" JSON "),
            None,
        );
        assert_eq!(
            validate_to_completion(&normalized, &expected),
            Err(NativeScalarLeafError::LogicalMetadata)
        );
        let exact = chunk(Arc::new(StringArray::from(vec!["{}"])), Some("json"), None);
        assert!(validate_to_completion(&exact, &expected).is_ok());
        assert_eq!(
            validate_to_completion(&exact, &schema(ScalarValueType::String, true)),
            Err(NativeScalarLeafError::LogicalMetadata)
        );
    }
    #[test]
    fn cached_logical_fact_cannot_override_or_erase_exact_field_metadata() {
        let canonical_json_with_empty_cache = chunk(
            Arc::new(StringArray::from(vec!["{}"])),
            Some("json"),
            Some(ChunkFieldSchema::empty()),
        );
        assert_eq!(
            validate_to_completion(
                &canonical_json_with_empty_cache,
                &schema(ScalarValueType::Json, true),
            ),
            Err(NativeScalarLeafError::LogicalMetadata),
        );
        let cached_json = ChunkFieldSchema::from_field(
            &Field::new("cache", DataType::Utf8, true)
                .with_metadata([(NR_LOGICAL_TYPE_KEY.to_owned(), "json".to_owned())].into()),
        )
        .unwrap();
        let plain_string_with_json_cache = chunk(
            Arc::new(StringArray::from(vec!["plain"])),
            None,
            Some(cached_json),
        );
        assert_eq!(
            validate_to_completion(
                &plain_string_with_json_cache,
                &schema(ScalarValueType::String, true),
            ),
            Err(NativeScalarLeafError::LogicalMetadata),
        );
    }
    #[test]
    fn exact_64k_record_includes_its_header_and_preserves_output_suffix() {
        let payload = "x".repeat(64 * 1024 - SCALAR_LEAF_HEADER_BYTES);
        let input = chunk(
            Arc::new(StringArray::from(vec![payload.as_str()])),
            None,
            None,
        );
        let expected = schema(ScalarValueType::String, true);
        let mut cursor = NativeScalarLeafEncoder::try_begin(
            &input,
            Arc::clone(&expected),
            NativeScalarLeafEncoder::scratch_capacity_bytes(),
        )
        .unwrap();
        assert_eq!(cursor.encoded_len(), None);
        finish_validation(&mut cursor).unwrap();
        assert_eq!(cursor.encoded_len(), Some(65_536));
        let mut output = vec![0xa5; 64 * 1024 + 17];
        let first = cursor.step(&mut output).unwrap();
        assert_eq!(first.emitted_bytes, 65_536);
        assert_eq!(first.completed_rows, 1);
        assert_eq!(first.status, RenderTurnStatus::InputComplete);
        assert!(
            output[first.emitted_bytes..]
                .iter()
                .all(|byte| *byte == 0xa5)
        );
        let record = output[..first.emitted_bytes].to_vec();
        output.fill(0xa5);
        let second = cursor.step(&mut output).unwrap();
        assert_eq!(second.emitted_bytes, 0);
        assert_eq!(second.completed_rows, 0);
        assert_eq!(second.status, RenderTurnStatus::InputComplete);
        assert!(
            output[second.emitted_bytes..]
                .iter()
                .all(|byte| *byte == 0xa5)
        );
        assert_eq!(record.len(), 65_536);
        assert_eq!(u32::from_le_bytes(record[4..8].try_into().unwrap()), 65_536);
        assert_eq!(
            u32::from_le_bytes(record[8..12].try_into().unwrap()),
            65_512
        );
        assert_eq!(&record[SCALAR_LEAF_HEADER_BYTES..], payload.as_bytes());
        assert_eq!(
            BorrowedScalarLeaf::decode(&expected, &record).unwrap(),
            BorrowedScalarLeaf::String(&payload),
        );
        let complete = cursor.step(&mut output).unwrap();
        assert_eq!(complete.emitted_bytes, 0);
        assert_eq!(complete.completed_rows, 0);
    }
    #[test]
    fn one_byte_over_complete_record_limit_refuses_before_cursor_emission() {
        let input = chunk(
            Arc::new(StringArray::from(vec!["x".repeat(65_513)])),
            None,
            None,
        );
        assert!(matches!(
            NativeScalarLeafEncoder::try_begin(
                &input,
                schema(ScalarValueType::String, true),
                NativeScalarLeafEncoder::scratch_capacity_bytes()
            ),
            Err(NativeScalarLeafError::Leaf(ScalarLeafError::ValueLimit))
        ));
    }
    #[test]
    fn dictionary_key_and_selected_value_null_are_distinct_physical_cases() {
        for keys in [
            Int32Array::from(vec![None]),
            Int32Array::from(vec![Some(0)]),
        ] {
            let dictionary = DictionaryArray::<Int32Type>::try_new(
                keys,
                Arc::new(StringArray::from(vec![None::<&str>, Some("unused")])),
            )
            .unwrap();
            let input = chunk(Arc::new(dictionary), None, None);
            let expected = schema(ScalarValueType::String, true);
            let wire = encode(&input, Arc::clone(&expected), 1);
            assert_eq!(
                BorrowedScalarLeaf::decode(&expected, &wire).unwrap(),
                BorrowedScalarLeaf::Null
            );
        }
    }
    #[test]
    fn only_selected_dictionary_value_is_encoded_without_expansion() {
        let large = "u".repeat(128 * 1024);
        let values = Arc::new(LargeStringArray::from(vec!["selected", large.as_str()]));
        let dictionary =
            DictionaryArray::<Int32Type>::try_new(Int32Array::from(vec![0]), values).unwrap();
        let input = chunk(Arc::new(dictionary), None, None);
        let expected = schema(ScalarValueType::String, true);
        let wire = encode(&input, Arc::clone(&expected), 1);
        assert_eq!(&wire[SCALAR_LEAF_HEADER_BYTES..], b"selected");
        assert_eq!(
            BorrowedScalarLeaf::decode(&expected, &wire).unwrap(),
            BorrowedScalarLeaf::String("selected")
        );
        // This result does not approve ignoring the large unselected backing:
        // the caller's complete original proof still counts all of it.
    }
    #[test]
    fn raw_binary_and_opaque_bytes_survive_one_byte_output_turns() {
        for (value_type, logical) in [
            (ScalarValueType::Binary, None),
            (ScalarValueType::Opaque(ScalarOpaqueType::Hll), Some("hll")),
        ] {
            let input = chunk(
                Arc::new(BinaryArray::from(vec![&[0xff, 0x00, 0x80][..]])),
                logical,
                None,
            );
            let expected = schema(value_type, true);
            let wire = encode(&input, Arc::clone(&expected), 1);
            assert_eq!(&wire[SCALAR_LEAF_HEADER_BYTES..], &[0xff, 0x00, 0x80]);
            assert!(BorrowedScalarLeaf::decode(&expected, &wire).is_ok());
        }
    }
    #[test]
    fn raw_nanosecond_ticks_and_timezone_remain_exact() {
        let array = TimestampNanosecondArray::from(vec![-1_234_567_891]).with_timezone("UTC");
        let input = chunk(Arc::new(array), None, None);
        let expected = schema(
            ScalarValueType::Timestamp {
                unit: ScalarTimestampUnit::Nanosecond,
                timezone: Some("UTC".to_owned()),
            },
            true,
        );
        let wire = encode(&input, Arc::clone(&expected), 1);
        assert_eq!(
            &wire[SCALAR_LEAF_HEADER_BYTES..],
            &(-1_234_567_891_i64).to_le_bytes()
        );
        assert_eq!(
            BorrowedScalarLeaf::decode(&expected, &wire).unwrap(),
            BorrowedScalarLeaf::Timestamp {
                ticks: -1_234_567_891,
                unit: ScalarTimestampUnit::Nanosecond
            }
        );
        let wrong_zone = schema(
            ScalarValueType::Timestamp {
                unit: ScalarTimestampUnit::Nanosecond,
                timezone: Some("+00:00".to_owned()),
            },
            true,
        );
        assert_eq!(
            validate_to_completion(&input, &wrong_zone),
            Err(NativeScalarLeafError::Type)
        );
    }
    #[test]
    fn decimal256_preserves_unscaled_coefficient_and_declared_scale() {
        let coefficient = i256::from_i128(-123_456_789_123_456_789_123_456_789);
        let input = chunk(
            Arc::new(
                Decimal256Array::from(vec![coefficient])
                    .with_precision_and_scale(40, 9)
                    .unwrap(),
            ),
            None,
            None,
        );
        let expected = schema(
            ScalarValueType::Decimal {
                bits: 256,
                precision: 40,
                scale: 9,
            },
            true,
        );
        let wire = encode(&input, Arc::clone(&expected), 1);
        assert_eq!(
            &wire[SCALAR_LEAF_HEADER_BYTES..],
            &coefficient.to_le_bytes()
        );
        assert_eq!(
            BorrowedScalarLeaf::decode(&expected, &wire).unwrap(),
            BorrowedScalarLeaf::Decimal256 {
                coefficient_le: coefficient.to_le_bytes(),
                precision: 40,
                scale: 9
            }
        );
    }
    #[test]
    fn fixed_leaf_payloads_preserve_integer_float_and_temporal_bits() {
        let large = i128::MIN;
        let large_array =
            FixedSizeBinaryArray::try_from_iter([large.to_le_bytes()].into_iter()).unwrap();
        let cases: Vec<(ArrayRef, ScalarValueType, Vec<u8>)> = vec![
            (
                Arc::new(BooleanArray::from(vec![true])),
                ScalarValueType::Boolean,
                vec![1],
            ),
            (
                Arc::new(Int8Array::from(vec![i8::MIN])),
                ScalarValueType::SignedInteger(8),
                i8::MIN.to_le_bytes().to_vec(),
            ),
            (
                Arc::new(Int16Array::from(vec![i16::MIN])),
                ScalarValueType::SignedInteger(16),
                i16::MIN.to_le_bytes().to_vec(),
            ),
            (
                Arc::new(Int32Array::from(vec![i32::MIN])),
                ScalarValueType::SignedInteger(32),
                i32::MIN.to_le_bytes().to_vec(),
            ),
            (
                Arc::new(Int64Array::from(vec![i64::MIN])),
                ScalarValueType::SignedInteger(64),
                i64::MIN.to_le_bytes().to_vec(),
            ),
            (
                Arc::new(large_array),
                ScalarValueType::LargeInt,
                large.to_le_bytes().to_vec(),
            ),
            (
                Arc::new(Float32Array::from(vec![-0.0])),
                ScalarValueType::Float32,
                (-0.0_f32).to_bits().to_le_bytes().to_vec(),
            ),
            (
                Arc::new(Float64Array::from(vec![-0.0])),
                ScalarValueType::Float64,
                (-0.0_f64).to_bits().to_le_bytes().to_vec(),
            ),
            (
                Arc::new(Date32Array::from(vec![-719_560])),
                ScalarValueType::Date,
                (-719_560_i32).to_le_bytes().to_vec(),
            ),
            (
                Arc::new(Time64MicrosecondArray::from(vec![-123_456_789])),
                ScalarValueType::TimeMicros,
                (-123_456_789_i64).to_le_bytes().to_vec(),
            ),
            (
                Arc::new(TimestampMicrosecondArray::from(vec![-1_234_567])),
                ScalarValueType::Timestamp {
                    unit: ScalarTimestampUnit::Microsecond,
                    timezone: None,
                },
                (-1_234_567_i64).to_le_bytes().to_vec(),
            ),
            (
                Arc::new(
                    Decimal128Array::from(vec![-123_456_789_123_456_789_123_i128])
                        .with_precision_and_scale(30, 3)
                        .unwrap(),
                ),
                ScalarValueType::Decimal {
                    bits: 128,
                    precision: 30,
                    scale: 3,
                },
                (-123_456_789_123_456_789_123_i128).to_le_bytes().to_vec(),
            ),
        ];
        for (array, semantic, payload) in cases {
            let input = chunk(array, None, None);
            let expected = schema(semantic, true);
            let wire = encode(&input, Arc::clone(&expected), 1);
            assert_eq!(&wire[SCALAR_LEAF_HEADER_BYTES..], payload.as_slice());
            assert!(BorrowedScalarLeaf::decode(&expected, &wire).is_ok());
        }
    }
    #[test]
    fn complete_scalar_markers_preserve_largeint_and_variant_without_new_tags() {
        let large: ArrayRef =
            Arc::new(FixedSizeBinaryArray::try_from_iter([[0_u8; 16]].into_iter()).unwrap());
        let variant: ArrayRef = Arc::new(LargeBinaryArray::from(vec![&[0xff, 0x00][..]]));
        for (array, marker, value_type) in [
            (large, "largeint", ScalarValueType::LargeInt),
            (variant, "variant", ScalarValueType::Variant),
        ] {
            let plain = chunk(Arc::clone(&array), None, None);
            let marked = chunk(array, Some(marker), None);
            let expected = schema(value_type, true);
            assert_eq!(
                encode(&plain, Arc::clone(&expected), 1),
                encode(&marked, expected, 1)
            );
        }
        let uuid = chunk(
            Arc::new(FixedSizeBinaryArray::try_from_iter([[0_u8; 16]].into_iter()).unwrap()),
            Some("uuid"),
            None,
        );
        assert_eq!(
            validate_to_completion(&uuid, &schema(ScalarValueType::LargeInt, true)),
            Err(NativeScalarLeafError::LogicalMetadata)
        );
    }

    #[test]
    fn variant_and_json_preserve_the_declared_domain_without_formatting() {
        let variant = chunk(
            Arc::new(LargeBinaryArray::from(vec![&[0xff, 0x00][..]])),
            None,
            None,
        );
        let expected = schema(ScalarValueType::Variant, true);
        let wire = encode(&variant, Arc::clone(&expected), 1);
        assert_eq!(
            BorrowedScalarLeaf::decode(&expected, &wire).unwrap(),
            BorrowedScalarLeaf::Variant(&[0xff, 0x00])
        );
        let json = chunk(
            Arc::new(StringArray::from(vec![" {\"key\": 1} "])),
            Some("json"),
            None,
        );
        let expected = schema(ScalarValueType::Json, true);
        let wire = encode(&json, Arc::clone(&expected), 1);
        assert_eq!(&wire[SCALAR_LEAF_HEADER_BYTES..], b" {\"key\": 1} ");
        assert_eq!(
            BorrowedScalarLeaf::decode(&expected, &wire).unwrap(),
            BorrowedScalarLeaf::Json(" {\"key\": 1} ")
        );
    }
    #[test]
    fn immutable_batch_alias_remains_until_cursor_exits() {
        let array: ArrayRef = Arc::new(BinaryArray::from(vec![&[0xff, 0x00][..]]));
        let weak = Arc::downgrade(&array);
        let input = chunk(array, None, None);
        let expected = schema(ScalarValueType::Binary, true);
        let mut cursor = NativeScalarLeafEncoder::try_begin(
            &input,
            Arc::clone(&expected),
            NativeScalarLeafEncoder::scratch_capacity_bytes(),
        )
        .unwrap();
        drop(input);
        assert!(weak.upgrade().is_some());
        let mut one = [0; 1];
        finish_validation(&mut cursor).unwrap();
        assert_eq!(cursor.step(&mut one).unwrap().emitted_bytes, 1);
        drop(cursor);
        assert!(weak.upgrade().is_none());
        // Physical source alias evidence only; the caller's permit/funding
        // ownership is intentionally not replaced by this Weak observation.
    }
    #[test]
    fn unsupported_dictionary_key_and_slice_selection_are_explicit() {
        use arrow::datatypes::Int8Type;
        let wrong = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0]),
            Arc::new(StringArray::from(vec!["value"])),
        )
        .unwrap();
        let input = chunk(Arc::new(wrong), None, None);
        let expected = schema(ScalarValueType::String, true);
        assert_eq!(
            validate_to_completion(&input, &expected),
            Err(NativeScalarLeafError::Type)
        );
        let input = chunk(Arc::new(Int64Array::from(vec![11, -22, 33])), None, None).slice(1, 1);
        let expected = schema(ScalarValueType::SignedInteger(64), true);
        let wire = encode(&input, Arc::clone(&expected), 1);
        assert_eq!(&wire[SCALAR_LEAF_HEADER_BYTES..], &(-22_i64).to_le_bytes());
    }
    #[test]
    fn scratch_and_value_limits_are_checked_before_record_batch_clone() {
        let input = chunk(Arc::new(StringArray::from(vec!["value"])), None, None);
        let expected = schema(ScalarValueType::String, true);
        assert!(matches!(
            NativeScalarLeafEncoder::try_begin(
                &input,
                Arc::clone(&expected),
                NativeScalarLeafEncoder::scratch_capacity_bytes() - 1
            ),
            Err(NativeScalarLeafError::ScratchLimit)
        ));
        let large = "v".repeat(64 * 1024 + 1);
        let oversized = chunk(
            Arc::new(StringArray::from(vec![large.as_str()])),
            None,
            None,
        );
        assert_eq!(
            validate_to_completion(&oversized, &expected),
            Err(NativeScalarLeafError::Leaf(ScalarLeafError::ValueLimit))
        );
    }
    #[test]
    fn exact_slot_and_leaf_only_domain_cannot_fallback() {
        let input = chunk(Arc::new(Int32Array::from(vec![7])), None, None);
        let wrong = Arc::new(
            ScalarSchema::try_new(ScalarField {
                nullable: true,
                value_type: ScalarValueType::SignedInteger(32),
            })
            .unwrap()
            .bind_native_slots(&[8])
            .unwrap(),
        );
        assert_eq!(
            validate_to_completion(&input, &wrong),
            Err(NativeScalarLeafError::Slot)
        );
        let container = schema(
            ScalarValueType::List(Box::new(ScalarField {
                nullable: true,
                value_type: ScalarValueType::SignedInteger(32),
            })),
            true,
        );
        assert_eq!(
            validate_to_completion(&input, &container),
            Err(NativeScalarLeafError::UnsupportedContainer)
        );
    }
    #[test]
    fn initial_timezone_validation_counts_all_three_sources_without_emission() {
        let zone = "z".repeat(64 * 1024);
        let input = chunk(
            Arc::new(TimestampNanosecondArray::from(vec![7]).with_timezone(zone.clone())),
            None,
            None,
        );
        let expected = schema(
            ScalarValueType::Timestamp {
                unit: ScalarTimestampUnit::Nanosecond,
                timezone: Some(zone),
            },
            true,
        );
        let mut cursor = NativeScalarLeafEncoder::try_begin(
            &input,
            expected,
            NativeScalarLeafEncoder::scratch_capacity_bytes(),
        )
        .unwrap();
        let mut examined = 0;
        for turn_index in 0..6 {
            assert_eq!(cursor.encoded_len(), None);
            let mut output = [0xa5; 128];
            let turn = cursor.step(&mut output).unwrap();
            assert_eq!(output, [0xa5; 128]);
            assert_eq!((turn.emitted_bytes, turn.completed_rows), (0, 0));
            assert_eq!(turn.status, RenderTurnStatus::Yielded);
            assert_eq!(turn.examined_bytes, 64 * 1024);
            assert!(turn.visited_cells <= 1024);
            examined += turn.examined_bytes;
            assert_eq!(cursor.encoded_len().is_some(), turn_index == 5);
        }
        assert_eq!(examined, 6 * 64 * 1024);
        assert_eq!(cursor.encoded_len(), Some(32));
        let before = cursor.offset;
        assert_eq!(cursor.step(&mut []).unwrap().emitted_bytes, 0);
        assert_eq!(cursor.offset, before);
        let mut output = [0; 32];
        assert_eq!(cursor.step(&mut output).unwrap().completed_rows, 1);
        assert_eq!(&output[24..], &7_i64.to_le_bytes());
    }
    #[test]
    fn equal_length_timezone_mismatch_is_incremental_and_latched() {
        let original_zone = "z".repeat(64 * 1024);
        let input = chunk(
            Arc::new(TimestampNanosecondArray::from(vec![7]).with_timezone(original_zone.clone())),
            None,
            None,
        );
        let mut expected_zone = original_zone.into_bytes();
        *expected_zone.last_mut().unwrap() = b'q';
        let expected = schema(
            ScalarValueType::Timestamp {
                unit: ScalarTimestampUnit::Nanosecond,
                timezone: Some(String::from_utf8(expected_zone).unwrap()),
            },
            true,
        );
        let mut cursor = NativeScalarLeafEncoder::try_begin(
            &input,
            expected,
            NativeScalarLeafEncoder::scratch_capacity_bytes(),
        )
        .unwrap();
        let first = cursor.step(&mut []).unwrap();
        assert_eq!(first.examined_bytes, 64 * 1024);
        assert_eq!(cursor.encoded_len(), None);
        let mut output = [0xa5; 32];
        assert!(matches!(
            cursor.step(&mut output),
            Err(NativeScalarLeafError::Type)
        ));
        assert_eq!(output, [0xa5; 32]);
        assert_eq!(cursor.encoded_len(), None);
        assert!(matches!(
            cursor.step(&mut output),
            Err(NativeScalarLeafError::Failed)
        ));
    }
    #[test]
    fn batch_timezone_is_checked_after_a_distinct_matching_slot_field() {
        let zone = "z".repeat(64 * 1024);
        let good = TimestampNanosecondArray::from(vec![7]).with_timezone(zone.clone());
        let mut input = chunk(Arc::new(good), None, None);
        let mut wrong = zone.clone().into_bytes();
        *wrong.last_mut().unwrap() = b'q';
        let bad = Arc::new(
            TimestampNanosecondArray::from(vec![7])
                .with_timezone(String::from_utf8(wrong).unwrap()),
        ) as ArrayRef;
        // The public Chunk batch can be replaced independently of its slot
        // schema. Keep a valid RecordBatch; do not violate Arrow unsafe APIs.
        input.batch = RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
                "value",
                bad.data_type().clone(),
                true,
            )])),
            vec![bad],
        )
        .unwrap();
        let expected = schema(
            ScalarValueType::Timestamp {
                unit: ScalarTimestampUnit::Nanosecond,
                timezone: Some(zone),
            },
            true,
        );
        let mut cursor = NativeScalarLeafEncoder::try_begin(
            &input,
            expected,
            NativeScalarLeafEncoder::scratch_capacity_bytes(),
        )
        .unwrap();
        for _ in 0..3 {
            let turn = cursor.step(&mut []).unwrap();
            assert_eq!(turn.examined_bytes, 64 * 1024);
            assert_eq!(turn.emitted_bytes, 0);
        }
        assert!(matches!(
            cursor.step(&mut []),
            Err(NativeScalarLeafError::Type)
        ));
    }
    #[test]
    fn cancellation_in_both_phases_retires_retained_array_and_slot_aliases() {
        for validate in [false, true] {
            let array = Arc::new(BinaryArray::from(vec![&b"value"[..]])) as ArrayRef;
            let weak_array = Arc::downgrade(&array);
            let input = chunk(array, None, None);
            let weak_field = Arc::downgrade(input.chunk_schema().slots()[0].field_ref());
            let mut cursor = NativeScalarLeafEncoder::try_begin(
                &input,
                schema(ScalarValueType::Binary, true),
                NativeScalarLeafEncoder::scratch_capacity_bytes(),
            )
            .unwrap();
            drop(input);
            if validate {
                finish_validation(&mut cursor).unwrap();
            }
            assert!(weak_array.upgrade().is_some());
            assert!(weak_field.upgrade().is_some());
            drop(cursor);
            assert!(weak_array.upgrade().is_none());
            assert!(weak_field.upgrade().is_none());
        }
    }
}
