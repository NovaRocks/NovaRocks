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

//! Standard SQL conversion using the existing prepared CAST and selected ABI.

use super::*;
use arrow_schema::DataType;
use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection, kernel_reason};
use novarocks_type_contract::{
    ArrayCastFailureMode, CharacterKind, ValueKind, trino_can_coerce_scalar,
    trino_common_scalar_kind, trino_decimal_widening_is_total,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TextKind {
    Varchar(Option<u32>),
    Char(u32),
}

impl TextKind {
    fn from_kind(value: &ValueKind) -> Option<Self> {
        match value {
            ValueKind::String => Some(Self::Varchar(None)),
            ValueKind::BoundedCharacter(ty) => match ty.kind() {
                CharacterKind::Char => Some(Self::Char(ty.length())),
                CharacterKind::Varchar => Some(Self::Varchar(Some(ty.length()))),
            },
            _ => None,
        }
    }

    fn length(self) -> Option<u32> {
        match self {
            Self::Varchar(len) => len,
            Self::Char(len) => Some(len),
        }
    }

    fn can_retag_to(self, target: Self) -> bool {
        match (self, target) {
            (_, Self::Varchar(None)) => true,
            (source, Self::Varchar(Some(target)))
            | (source @ Self::Char(_), Self::Char(target)) => {
                source.length().is_some_and(|len| len <= target)
            }
            _ => false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Numeric {
    Integer(SignedWidth),
    Decimal(u8, u8),
    Real,
    Double,
}

impl Numeric {
    fn from_kind(value: &ValueKind) -> Option<Self> {
        let ValueKind::Scalar(value) = value else {
            return None;
        };
        match value {
            DataType::Decimal128(p, s) if (1..=38).contains(p) && *s >= 0 && *s as u8 <= *p => {
                Some(Self::Decimal(*p, *s as u8))
            }
            DataType::Float32 => Some(Self::Real),
            DataType::Float64 => Some(Self::Double),
            value => SignedWidth::from_type(value).map(Self::Integer),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Scalar {
    Character {
        target: TextKind,
        check_truncation: bool,
    },
    ParseNumber(Numeric),
    ParseBoolean,
    NumberToBoolean(Numeric),
    BooleanToNumber(Numeric),
    Numeric {
        source: Numeric,
        target: Numeric,
    },
    Render {
        max_length: Option<u32>,
    },
    Temporal(trino_temporal::Operation),
    Uuid(UuidConversion),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UuidConversion {
    ParseText,
    FromBinary,
    ToBinary,
    ToText(Option<u32>),
}

impl UuidConversion {
    fn may_raise(self) -> bool {
        match self {
            Self::ParseText | Self::FromBinary => true,
            Self::ToBinary => false,
            Self::ToText(limit) => limit.is_some_and(|length| length < 36),
        }
    }

    fn prepare(
        source: &FunctionValueType,
        result: &FunctionValueType,
        from_text: Option<TextKind>,
        to_text: Option<TextKind>,
    ) -> Option<Self> {
        use DataType::{Binary, FixedSizeBinary};
        use ValueKind::Nominal;
        match (
            source.value_type().kind(),
            result.value_type().kind(),
            source.data_type(),
            result.data_type(),
        ) {
            (_, Nominal(ValueLogicalType::Uuid), DataType::Utf8, FixedSizeBinary(16))
                if matches!(from_text, Some(TextKind::Varchar(_))) =>
            {
                Some(Self::ParseText)
            }
            (ValueKind::Binary, Nominal(ValueLogicalType::Uuid), Binary, FixedSizeBinary(16)) => {
                Some(Self::FromBinary)
            }
            (Nominal(ValueLogicalType::Uuid), ValueKind::Binary, FixedSizeBinary(16), Binary) => {
                Some(Self::ToBinary)
            }
            (Nominal(ValueLogicalType::Uuid), _, FixedSizeBinary(16), DataType::Utf8) => {
                match to_text {
                    Some(TextKind::Varchar(limit)) => Some(Self::ToText(limit)),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn evaluate(self, array: &dyn Array, row: usize) -> Result<CastRowResult, ConversionFailure> {
        use novarocks_value_arrays::uuid::{format_uuid, parse_uuid};
        match self {
            Self::ParseText => {
                let text = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| internal("UUID text source differs from its frozen carrier"))?;
                parse_uuid(text.value(row))
                    .map(CastRowResult::UuidBytes)
                    .ok_or(ConversionFailure::Invalid)
            }
            Self::FromBinary => {
                let binary = array
                    .as_any()
                    .downcast_ref::<arrow_array::BinaryArray>()
                    .ok_or_else(|| internal("UUID byte source differs from its frozen carrier"))?;
                let bytes = binary
                    .value(row)
                    .try_into()
                    .map_err(|_| ConversionFailure::Invalid)?;
                Ok(CastRowResult::UuidBytes(bytes))
            }
            Self::ToBinary | Self::ToText(_) => {
                let uuid = array
                    .as_any()
                    .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
                    .ok_or_else(|| internal("UUID input differs from its frozen carrier"))?;
                let bytes = uuid
                    .value(row)
                    .try_into()
                    .map_err(|_| internal("UUID input lost its exact byte width"))?;
                match self {
                    Self::ToBinary => Ok(CastRowResult::UuidBytes(bytes)),
                    Self::ToText(limit) => {
                        if limit.is_some_and(|length| length < 36) {
                            return Err(ConversionFailure::Invalid);
                        }
                        let text = String::from_utf8(format_uuid(bytes).to_vec())
                            .map_err(|_| internal("UUID formatter produced invalid UTF-8"))?;
                        Ok(CastRowResult::Text(text))
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
}

impl Scalar {
    pub(super) fn may_raise(self) -> bool {
        match self {
            Self::Character {
                check_truncation, ..
            } => check_truncation,
            Self::ParseNumber(_) | Self::ParseBoolean => true,
            Self::Render { max_length } => max_length.is_some(),
            Self::Temporal(operation) => operation.may_raise(),
            Self::Uuid(operation) => operation.may_raise(),
            Self::Numeric { source, target } => match (source, target) {
                (Numeric::Integer(from), Numeric::Integer(to)) => from > to,
                (Numeric::Decimal(sp, ss), Numeric::Decimal(tp, ts)) => {
                    !trino_decimal_widening_is_total(
                        &DataType::Decimal128(sp, ss as i8),
                        &DataType::Decimal128(tp, ts as i8),
                    )
                }
                (Numeric::Real | Numeric::Double, Numeric::Integer(_) | Numeric::Decimal(..))
                | (Numeric::Decimal(..), Numeric::Integer(_))
                | (Numeric::Integer(_), Numeric::Decimal(..)) => true,
                _ => false,
            },
            Self::BooleanToNumber(Numeric::Decimal(p, s)) => p == s,
            Self::NumberToBoolean(_) | Self::BooleanToNumber(_) => false,
        }
    }
}

pub(super) fn prepare(
    source: &FunctionValueType,
    result: &FunctionValueType,
    mode: TrinoCastMode,
    control: &dyn PureCompileControl,
    check_nullable: bool,
) -> Result<PreparedCastRecipe, CastPrepareError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let outcome = (|| {
        for ty in [source, result] {
            validate_type_observed(ty, &mut work).map_err(CastPrepareError::Kernel)?;
        }
        let recipe = prepare_value(source, result, mode, &mut work)?;
        if check_nullable && recipe.trino_can_return_null() && !result.nullable() {
            return Err(CastPrepareError::TypeMismatch);
        }
        Ok(recipe)
    })();
    if let Some(error) = outcome
        .as_ref()
        .err()
        .and_then(CastPrepareError::control_error)
    {
        return Err(CastPrepareError::Control(error));
    }
    work.finish()?;
    outcome
}

fn prepare_value(
    source: &FunctionValueType,
    result: &FunctionValueType,
    mode: TrinoCastMode,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedCastRecipe, CastPrepareError> {
    use DataType::{Boolean, Date32, Null, Time64, Timestamp, Utf8};
    use TimeUnit::{Microsecond, Nanosecond};
    use ValueKind::{Binary, List, Map, Nominal, Scalar as Kind, Struct};
    use trino_temporal::Operation as Temporal;

    let from = source.value_type().kind();
    let to = result.value_type().kind();
    let mut recipe = PreparedCastRecipe {
        operation: CastOperation::Carrier,
        array_failure_mode: None,
        source: source.clone(),
        result: result.clone(),
        body: CastBody::Identity,
        children: CastChildren::None,
        trino_mode: Some(mode),
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        allow_throw_exception: false,
    };
    work.step()?;
    if source.same_physical_binding_ignoring_root_nullability_observed(result, || work.step())? {
        return Ok(recipe);
    }
    if mode == TrinoCastMode::Implicit
        && novarocks_type_contract::byte_view_to_offsets_projection(source, result)
    {
        recipe.body = CastBody::SelectedArray(f3_array::Body::ByteViewProjection);
        return Ok(recipe);
    }
    if matches!(from, Kind(Null)) {
        recipe.body = CastBody::ArrayNull;
        return Ok(recipe);
    }
    // A TRY container catches its own failed row, never a successful NULL
    // substituted by a child's TRY.
    let (child_mode, failure_mode) = match mode {
        TrinoCastMode::Try => (TrinoCastMode::Explicit, ArrayCastFailureMode::OutputNull),
        _ => (mode, ArrayCastFailureMode::ReportError),
    };
    if matches!((from, to), (List(_), List(_))) {
        let from = source.child_at(0).ok_or(CastPrepareError::TypeMismatch)?;
        let to = result.child_at(0).ok_or(CastPrepareError::TypeMismatch)?;
        let child = prepare_value(&from, &to, child_mode, work)?;
        if child.trino_can_return_null() && !to.nullable() {
            return Err(CastPrepareError::TypeMismatch);
        }
        recipe.body = CastBody::ExplicitArray;
        recipe.array_failure_mode = Some(failure_mode);
        recipe.children = CastChildren::Array(Box::new(child));
        return Ok(recipe);
    }
    if matches!((from, to), (Struct(_), Struct(_)))
        && f3_array::can_project_positional_structure(source, result, work)?
    {
        recipe.body = CastBody::SelectedArray(f3_array::Body::Structure);
        return Ok(recipe);
    }
    if matches!((from, to), (Map { .. }, Map { .. })) {
        let source_key = source.child_at(0).ok_or(CastPrepareError::TypeMismatch)?;
        let target_key = result.child_at(0).ok_or(CastPrepareError::TypeMismatch)?;
        let source_value = source.child_at(1).ok_or(CastPrepareError::TypeMismatch)?;
        let target_value = result.child_at(1).ok_or(CastPrepareError::TypeMismatch)?;
        let key = prepare_value(&source_key, &target_key, child_mode, work)?;
        let value = prepare_value(&source_value, &target_value, child_mode, work)?;
        if value.trino_can_return_null() && !target_value.nullable() {
            return Err(CastPrepareError::TypeMismatch);
        }
        // Empty MAP(unknown, unknown) has no keys to compare. For all
        // nonempty target domains the prepared key relation must be total.
        if !matches!(target_key.value_type().kind(), Kind(Null)) {
            novarocks_type_contract::KeyRelation::Identical
                .validate_type_observed(target_key.value_type(), &mut |_| work.step())?
                .map_err(|_| CastPrepareError::Unsupported)?;
        }
        recipe.body = CastBody::ExplicitMap;
        recipe.array_failure_mode = Some(failure_mode);
        recipe.children = CastChildren::Map(Box::new([key, value]));
        return Ok(recipe);
    }
    if let (Struct(source_fields), Struct(target_fields)) = (from, to) {
        if source_fields.len() != target_fields.len() {
            return Err(CastPrepareError::Illegal);
        }
        work.flush()?;
        let mut children = Vec::new();
        children
            .try_reserve_exact(source_fields.len())
            .map_err(|_| CastPrepareError::Kernel(KernelFailure::ResourceExhausted))?;
        let mut unsupported_child = false;
        let mut may_raise = false;
        for ordinal in 0..source_fields.len() {
            work.step()?;
            let from = source
                .child_at(ordinal)
                .ok_or(CastPrepareError::TypeMismatch)?;
            let to = result
                .child_at(ordinal)
                .ok_or(CastPrepareError::TypeMismatch)?;
            let child = match prepare_value(&from, &to, child_mode, work) {
                Err(CastPrepareError::Unsupported) => {
                    // Another field can still make the pair illegal. Use the
                    // actual scalar author for both facts.
                    unsupported_child = true;
                    continue;
                }
                child => child?,
            };
            if child.trino_can_return_null() && !to.nullable() {
                return Err(CastPrepareError::TypeMismatch);
            }
            may_raise |= child.conversion_may_raise();
            children.push(child);
        }
        if unsupported_child {
            return Err(CastPrepareError::Unsupported);
        }
        work.flush()?;
        recipe.body = CastBody::ExplicitRow { may_raise };
        recipe.array_failure_mode = Some(failure_mode);
        recipe.children = CastChildren::Row(children.into_boxed_slice());
        return Ok(recipe);
    }

    if is_timestamp_text_cast(from, to, mode) {
        let cast = crate::builtin::TemporalTextCast::bind(source, result)
            .ok_or(CastPrepareError::Unsupported)?;
        recipe.body = CastBody::TemporalText(cast);
        return Ok(recipe);
    }

    if !context_allows(from, to, mode) {
        return Err(CastPrepareError::Illegal);
    }
    let from_text = TextKind::from_kind(from);
    let to_text = TextKind::from_kind(to);
    if let Some(operation) = UuidConversion::prepare(source, result, from_text, to_text) {
        recipe.body = CastBody::Trino(Scalar::Uuid(operation));
        return Ok(recipe);
    }
    let from_number = Numeric::from_kind(from);
    let to_number = Numeric::from_kind(to);
    if (from_text.is_some() && source.data_type() != &Utf8)
        || (to_text.is_some() && result.data_type() != &Utf8)
    {
        return Err(CastPrepareError::Unsupported);
    }
    recipe.body = if let (Some(from), Some(to)) = (from_text, to_text) {
        if source.data_type() != &Utf8 || result.data_type() != &Utf8 {
            return Err(CastPrepareError::Unsupported);
        }
        if from.can_retag_to(to) {
            CastBody::Identity
        } else {
            CastBody::Trino(Scalar::Character {
                target: to,
                check_truncation: mode == TrinoCastMode::StoreAssignment
                    && to
                        .length()
                        .is_some_and(|len| from.length().is_none_or(|n| n > len)),
            })
        }
    } else if matches!(to, Binary)
        && let Some(text) = from_text
    {
        if result.data_type() != &DataType::Binary {
            return Err(CastPrepareError::Unsupported);
        }
        // VARCHAR shares its UTF-8 bytes. CHAR restores the declared fill
        // before projecting the same binary bytes.
        CastBody::SelectedArray(match text {
            TextKind::Varchar(_) => f3_array::Body::ArrowScalar,
            TextKind::Char(len) => f3_array::Body::TrinoCharBinary(len),
        })
    } else if let Some(numeric) = to_number.filter(|_| from_text.is_some()) {
        if matches!(from_text, Some(TextKind::Char(_))) && matches!(numeric, Numeric::Decimal(..)) {
            return Err(CastPrepareError::Illegal);
        }
        CastBody::Trino(Scalar::ParseNumber(numeric))
    } else if matches!(to, Kind(Boolean)) && from_text.is_some() {
        CastBody::Trino(Scalar::ParseBoolean)
    } else if let (Some(source_numeric), Some(target_numeric)) = (from_number, to_number) {
        if let Some(leaf) = trino_array_leaf::profile(source.data_type(), result.data_type()) {
            CastBody::ArrayLeaf(leaf)
        } else {
            CastBody::Trino(Scalar::Numeric {
                source: source_numeric,
                target: target_numeric,
            })
        }
    } else if let Some(numeric) = from_number.filter(|_| matches!(to, Kind(Boolean))) {
        CastBody::Trino(Scalar::NumberToBoolean(numeric))
    } else if let Some(numeric) = to_number.filter(|_| matches!(from, Kind(Boolean))) {
        CastBody::Trino(Scalar::BooleanToNumber(numeric))
    } else if let Some(TextKind::Varchar(max_length)) = to_text
        && (matches!(from, Kind(Boolean | Date32)) || from_number.is_some())
    {
        CastBody::Trino(Scalar::Render { max_length })
    } else if matches!(to, Kind(Timestamp(Microsecond, None))) && from_text.is_some() {
        CastBody::Trino(Scalar::Temporal(Temporal::TextToTimestamp))
    } else if matches!(to, Kind(Time64(Microsecond)))
        && matches!(from_text, Some(TextKind::Varchar(_)))
    {
        CastBody::Trino(Scalar::Temporal(Temporal::TextToTime))
    } else if matches!(
        (from, to),
        (
            Kind(Timestamp(Microsecond, None)),
            Kind(Time64(Microsecond))
        )
    ) {
        CastBody::Trino(Scalar::Temporal(Temporal::TimestampToTime))
    } else if matches!(
        (from, to),
        (
            Kind(Timestamp(Nanosecond, None)),
            Kind(Timestamp(Microsecond, None))
        )
    ) {
        CastBody::Trino(Scalar::Temporal(Temporal::TimestampNanosToMicros))
    } else if matches!(from, Kind(Timestamp(Microsecond, None)))
        && matches!(to_text, Some(TextKind::Varchar(None)))
    {
        CastBody::Trino(Scalar::Temporal(Temporal::TimestampToText))
    } else if matches!(from, Kind(Time64(Microsecond)))
        && let Some(TextKind::Varchar(max_length)) = to_text
    {
        CastBody::Trino(Scalar::Temporal(Temporal::TimeToText(max_length)))
    } else if from_text.is_some() && matches!(to, Kind(Date32)) {
        CastBody::ArrayDateText
    } else if matches!(
        (from, to),
        (Kind(Date32), Kind(Timestamp(_, None))) | (Kind(Timestamp(_, None)), Kind(Date32))
    ) {
        CastBody::TemporalCarrier {
            source: Source::from_type(source.data_type()).expect("admitted temporal carrier"),
        }
    } else {
        match (from, to) {
            (Nominal(ValueLogicalType::Json), _)
                if matches!(to_text, Some(TextKind::Varchar(_))) =>
            {
                CastBody::ArrayJson(array_json::Operation::JsonToVarchar)
            }
            (_, Nominal(ValueLogicalType::Json))
                if matches!(from_text, Some(TextKind::Varchar(_))) =>
            {
                CastBody::ArrayJson(array_json::Operation::VarcharToJson)
            }
            // Legal temporal, JSON and composite shapes with no corresponding
            // installed prepared body keep their exact capability refusal.
            (_, Kind(Timestamp(..))) if from_text.is_some() => {
                return Err(CastPrepareError::Unsupported);
            }
            _ if legal_non_identity(from, to) => return Err(CastPrepareError::Unsupported),
            _ => return Err(CastPrepareError::Illegal),
        }
    };
    Ok(recipe)
}

fn context_allows(from: &ValueKind, to: &ValueKind, mode: TrinoCastMode) -> bool {
    match mode {
        TrinoCastMode::Explicit | TrinoCastMode::Try => true,
        TrinoCastMode::Implicit => {
            trino_can_coerce_scalar(from, to)
                || matches!(
                    (from, to),
                    (
                        ValueKind::Scalar(DataType::Date32),
                        ValueKind::Scalar(DataType::Timestamp(_, None))
                    )
                )
        }
        TrinoCastMode::StoreAssignment => {
            trino_common_scalar_kind(from, to).is_some()
                || matches!(
                    (from, to),
                    (
                        ValueKind::Scalar(DataType::Date32),
                        ValueKind::Scalar(DataType::Timestamp(_, None))
                    ) | (
                        ValueKind::Scalar(DataType::Timestamp(_, None)),
                        ValueKind::Scalar(DataType::Date32)
                    )
                )
        }
    }
}

fn is_timestamp_text_cast(from: &ValueKind, to: &ValueKind, mode: TrinoCastMode) -> bool {
    use novarocks_type_contract::{TemporalPrecision, TemporalType};

    matches!(mode, TrinoCastMode::Explicit | TrinoCastMode::Try)
        && matches!(
            from,
            ValueKind::Temporal(TemporalType::Timestamp(TemporalPrecision::MICROSECONDS))
        )
        && matches!(to, ValueKind::String)
}

fn legal_non_identity(from: &ValueKind, to: &ValueKind) -> bool {
    use DataType::{Boolean, Date32, Time64, Timestamp};
    use ValueKind::{Binary, List, Map, Nominal, Scalar as Kind, Struct};
    let from_text = TextKind::from_kind(from);
    let to_text = TextKind::from_kind(to);
    let from_number = Numeric::from_kind(from);
    let to_number = Numeric::from_kind(to);
    // Numeric, string and temporal domains are independent even when they
    // currently share one Arrow representation.
    match (from, to) {
        (Nominal(ValueLogicalType::Json), List(_) | Map { .. } | Struct(_) | Kind(Boolean)) => true,
        (List(_) | Map { .. } | Struct(_) | Kind(Boolean), Nominal(ValueLogicalType::Json)) => true,
        (Kind(Date32), Kind(Timestamp(..))) => true,
        (Kind(Timestamp(..)), Kind(Date32 | Timestamp(..) | Time64(..))) => true,
        (Kind(Time64(..)), Kind(Time64(..))) => true,
        (_, Kind(Time64(..))) => matches!(from_text, Some(TextKind::Varchar(_))),
        (_, Kind(Date32 | Timestamp(..))) => from_text.is_some(),
        (Kind(Date32 | Timestamp(..) | Time64(..)), _) => {
            matches!(to_text, Some(TextKind::Varchar(_)))
        }
        (Nominal(ValueLogicalType::Json), _) => {
            matches!(to_text, Some(TextKind::Varchar(_))) || to_number.is_some()
        }
        (_, Nominal(ValueLogicalType::Json)) => {
            matches!(from_text, Some(TextKind::Varchar(_))) || from_number.is_some()
        }
        (_, Binary) => from_text.is_some(),
        (Kind(Boolean), _) => to_number.is_some() || matches!(to_text, Some(TextKind::Varchar(_))),
        (_, Kind(Boolean)) => from_number.is_some() || from_text.is_some(),
        _ => {
            (from_number.is_some() && to_number.is_some())
                || (from_text.is_some() && to_text.is_some())
                || (from_text.is_some() && to_number.is_some())
                || (from_number.is_some() && matches!(to_text, Some(TextKind::Varchar(_))))
        }
    }
}

#[derive(Clone, Copy)]
enum Number {
    Integer(i64),
    Decimal(i128, u8),
    Real(f32),
    Double(f64),
}

impl Number {
    fn is_nonzero(self) -> bool {
        match self {
            Self::Integer(value) => value != 0,
            Self::Decimal(value, _) => value != 0,
            Self::Real(value) => value != 0.0,
            Self::Double(value) => value != 0.0,
        }
    }

    fn convert(
        self,
        target: Numeric,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<CastRowResult, ConversionFailure> {
        use CastRowResult as R;
        match (self, target) {
            (Self::Integer(value), Numeric::Integer(width)) => {
                signed_result(i128::from(value), width)
            }
            (Self::Integer(value), Numeric::Decimal(p, s)) => {
                coefficient(i128::from(value), 0, p, s)
            }
            (Self::Integer(value), Numeric::Real) => Ok(R::Float32(value as f32)),
            (Self::Integer(value), Numeric::Double) => Ok(R::Float64(value as f64)),
            (Self::Decimal(value, scale), Numeric::Decimal(p, s)) => {
                coefficient(value, scale, p, s)
            }
            (Self::Decimal(value, scale), Numeric::Integer(width)) => {
                let rounded = trino_array_leaf::rescale_coefficient(value, scale, 38, 0)
                    .map_err(ConversionFailure::from_leaf)?;
                signed_result(rounded, width)
            }
            (Self::Double(value), Numeric::Integer(width)) => rounded_integer(value, width),
            (Self::Real(value), Numeric::Integer(width)) => {
                rounded_integer(f64::from(value), width)
            }
            (Self::Double(value), Numeric::Decimal(p, s)) => float_decimal(
                value.is_nan(),
                value.is_infinite(),
                || format!("{value:e}"),
                p,
                s,
                work,
            ),
            (Self::Real(value), Numeric::Decimal(p, s)) => float_decimal(
                value.is_nan(),
                value.is_infinite(),
                || trino_float_text::real_decimal(value),
                p,
                s,
                work,
            ),
            (Self::Double(value), Numeric::Real) => Ok(R::Float32(value as f32)),
            (Self::Real(value), Numeric::Double) => Ok(R::Float64(f64::from(value))),
            (Self::Double(value), Numeric::Double) => Ok(R::Float64(value)),
            (Self::Real(value), Numeric::Real) => Ok(R::Float32(value)),
            (Self::Decimal(..), Numeric::Real | Numeric::Double) => Err(ConversionFailure::Outer(
                internal("decimal floating cast lost its exact leaf recipe"),
            )),
        }
    }
}

fn signed_result(value: i128, width: SignedWidth) -> Result<CastRowResult, ConversionFailure> {
    let accepted = match width {
        SignedWidth::I8 => i8::try_from(value).is_ok(),
        SignedWidth::I16 => i16::try_from(value).is_ok(),
        SignedWidth::I32 => i32::try_from(value).is_ok(),
        SignedWidth::I64 => i64::try_from(value).is_ok(),
    };
    if !accepted {
        return Err(ConversionFailure::Range);
    }
    Ok(CastRowResult::Signed(value as i64))
}

fn rounded_integer(value: f64, width: SignedWidth) -> Result<CastRowResult, ConversionFailure> {
    if value.is_nan() {
        return Err(ConversionFailure::Invalid);
    }
    let rounded = value.round();
    if !(-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&rounded) {
        return Err(ConversionFailure::Range);
    }
    signed_result(rounded as i128, width)
}

fn coefficient(value: i128, scale: u8, p: u8, s: u8) -> Result<CastRowResult, ConversionFailure> {
    trino_array_leaf::rescale_coefficient(value, scale, p, s)
        .map(CastRowResult::Decimal128)
        .map_err(ConversionFailure::from_leaf)
}

fn float_decimal(
    nan: bool,
    infinite: bool,
    value: impl FnOnce() -> String,
    p: u8,
    s: u8,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<CastRowResult, ConversionFailure> {
    if nan {
        return Err(ConversionFailure::Invalid);
    }
    if infinite {
        return Err(ConversionFailure::Range);
    }
    work.flush()?;
    let value = value();
    work.flush()?;
    parse_decimal(&value, p, s, work)?
        .map(CastRowResult::Decimal128)
        .ok_or(ConversionFailure::Range)
}

#[derive(Debug)]
pub(super) enum ConversionFailure {
    Invalid,
    Range,
    Truncation,
    Outer(KernelFailure),
}

impl ConversionFailure {
    fn from_leaf(cause: trino_array_leaf::LeafError) -> Self {
        use trino_array_leaf::LeafError as E;
        match cause {
            E::InvalidCastArgument => Self::Invalid,
            E::NumericOutOfRange => Self::Range,
            E::TypeMismatch => {
                Self::Outer(invalid("cast input violates its frozen decimal domain"))
            }
        }
    }
}

impl From<KernelFailure> for ConversionFailure {
    fn from(error: KernelFailure) -> Self {
        Self::Outer(error)
    }
}

impl fmt::Display for ConversionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid => f.write_str("Cannot represent the value as the requested cast type"),
            Self::Range => f.write_str("Cast value is outside the target numeric range"),
            Self::Truncation => f.write_str("Cannot truncate non-space characters on assignment"),
            Self::Outer(cause) => cause.fmt(f),
        }
    }
}

impl Error for ConversionFailure {}

impl FailureProjection for ConversionFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Invalid | Self::Truncation => {
                FailureFact::new(FailureClass::Data, kernel_reason::INVALID_CAST_ARGUMENT)
            }
            Self::Range => {
                FailureFact::new(FailureClass::Data, kernel_reason::NUMERIC_OUT_OF_RANGE)
            }
            Self::Outer(cause) => cause.failure_fact(),
        }
    }
}

pub(super) fn evaluate_scalar(
    recipe: &PreparedCastRecipe,
    body: Scalar,
    argument: EvaluatedArgument<'_>,
    ordinal: usize,
    logical_row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<CastRowResult, KernelFailure> {
    let row =
        recipe.checked_row_with_shape(
            argument,
            ordinal,
            logical_row,
            work,
            |array| match recipe.source.data_type() {
                DataType::Decimal128(..) => array.as_any().is::<Decimal128Array>(),
                DataType::Time64(TimeUnit::Microsecond) => {
                    array.as_any().is::<arrow_array::Time64MicrosecondArray>()
                }
                DataType::FixedSizeBinary(16) if matches!(body, Scalar::Uuid(_)) => {
                    array.as_any().is::<arrow_array::FixedSizeBinaryArray>()
                }
                DataType::Binary if matches!(body, Scalar::Uuid(_)) => {
                    array.as_any().is::<arrow_array::BinaryArray>()
                }
                ty => Source::from_type(ty).is_some_and(|source| source.validate(array)),
            },
        )?;
    let array = argument.array().as_ref();
    if logical_is_null(array, row, 1, work)? {
        return if recipe.source.nullable() {
            Ok(CastRowResult::Null)
        } else {
            Err(invalid("non-null CAST source contains a selected NULL"))
        };
    }
    let output = (|| {
        work.step()?;
        let text = || {
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .map(|text| text.value(row))
                .ok_or_else(|| internal("cast text source lost its checked carrier"))
        };
        Ok(match body {
            Scalar::Character {
                target,
                check_truncation,
            } => CastRowResult::Text(convert_character(text()?, target, check_truncation, work)?),
            Scalar::ParseNumber(target) => parse_number(text()?, target, work)?,
            Scalar::ParseBoolean => CastRowResult::Boolean(parse_boolean(text()?)?),
            Scalar::NumberToBoolean(source) => {
                CastRowResult::Boolean(read_number(array, row, source)?.is_nonzero())
            }
            Scalar::BooleanToNumber(target) => {
                let value = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| internal("cast boolean source lost its checked carrier"))?
                    .value(row);
                Number::Integer(i64::from(value)).convert(target, work)?
            }
            Scalar::Numeric { source, target } => {
                read_number(array, row, source)?.convert(target, work)?
            }
            Scalar::Render { max_length } => {
                let text = render(array, row, work)?;
                if max_length.is_some_and(|len| text.len() > len as usize) {
                    return Err(ConversionFailure::Invalid);
                }
                CastRowResult::Text(text)
            }
            Scalar::Temporal(operation) => operation.evaluate(array, row, work)?,
            Scalar::Uuid(operation) => operation.evaluate(array, row)?,
        })
    })();
    match output {
        Ok(value) => Ok(value),
        Err(ConversionFailure::Outer(cause)) => Err(cause),
        Err(cause) => {
            work.flush()?;
            Ok(CastRowResult::RowError(RowDataError::from_domain(
                ordinal, cause,
            )?))
        }
    }
}

fn read_number(array: &dyn Array, row: usize, ty: Numeric) -> Result<Number, KernelFailure> {
    macro_rules! value {
        ($array:ty) => {
            array
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| invalid("numeric cast lost its checked carrier"))?
                .value(row)
        };
    }
    Ok(match ty {
        Numeric::Integer(SignedWidth::I8) => Number::Integer(i64::from(value!(Int8Array))),
        Numeric::Integer(SignedWidth::I16) => Number::Integer(i64::from(value!(Int16Array))),
        Numeric::Integer(SignedWidth::I32) => Number::Integer(i64::from(value!(Int32Array))),
        Numeric::Integer(SignedWidth::I64) => Number::Integer(value!(Int64Array)),
        Numeric::Decimal(p, s) => {
            let value = value!(Decimal128Array);
            if value.unsigned_abs() >= 10_u128.pow(u32::from(p)) {
                return Err(invalid(
                    "decimal coefficient is outside its frozen precision",
                ));
            }
            Number::Decimal(value, s)
        }
        Numeric::Real => Number::Real(value!(Float32Array)),
        Numeric::Double => Number::Double(value!(Float64Array)),
    })
}

fn convert_character(
    value: &str,
    target: TextKind,
    check_truncation: bool,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<String, ConversionFailure> {
    let limit = target.length().unwrap_or(u32::MAX) as usize;
    let mut end = value.len();
    let mut content_end = 0;
    for (index, (byte, ch)) in value.char_indices().enumerate() {
        work.step()?;
        if index >= limit {
            end = end.min(byte);
            if !check_truncation {
                break;
            }
            if ch != ' ' {
                return Err(ConversionFailure::Truncation);
            }
        } else if ch != ' ' {
            content_end = byte + ch.len_utf8();
        }
    }
    if matches!(target, TextKind::Char(_)) {
        end = content_end;
    }
    let mut output = String::new();
    work.flush()?;
    output
        .try_reserve_exact(end)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    output.push_str(&value[..end]);
    work.flush()?;
    Ok(output)
}

fn parse_boolean(value: &str) -> Result<bool, ConversionFailure> {
    if value == "1" || value.eq_ignore_ascii_case("t") || value.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if value == "0" || value.eq_ignore_ascii_case("f") || value.eq_ignore_ascii_case("false")
    {
        Ok(false)
    } else {
        Err(ConversionFailure::Invalid)
    }
}

fn parse_number(
    value: &str,
    target: Numeric,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<CastRowResult, ConversionFailure> {
    match target {
        Numeric::Integer(width) => {
            let parsed =
                super::array_text::parse_integer(value, work)?.ok_or(ConversionFailure::Invalid)?;
            signed_result(i128::from(parsed), width).map_err(|_| ConversionFailure::Invalid)
        }
        Numeric::Decimal(p, s) => parse_decimal(value, p, s, work)?
            .map(CastRowResult::Decimal128)
            .ok_or(ConversionFailure::Invalid),
        Numeric::Real | Numeric::Double => parse_float(value, target, work),
    }
}

/// Retain at most the target's 38 coefficients and the first discarded digit.
/// Even a very long exponent cannot cause unbounded allocation or scaling.
fn parse_decimal(
    value: &str,
    precision: u8,
    scale: u8,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<i128>, KernelFailure> {
    let (negative, value) = strip_sign(value);
    let mut leading = [0_u8; 39];
    let mut significant = 0_i64;
    let mut fractional = 0_i64;
    let mut dot = false;
    let mut saw_digit = false;
    let mut chars = value.chars();
    let mut exponent = 0_i64;
    while let Some(ch) = chars.next() {
        work.step()?;
        match ch {
            '.' if !dot => dot = true,
            'e' | 'E' if saw_digit => {
                let Some(parsed) = parse_exponent(chars.as_str(), work)? else {
                    return Ok(None);
                };
                exponent = parsed;
                break;
            }
            _ => {
                let Some(digit) = array_text::digit(ch, work)? else {
                    return Ok(None);
                };
                saw_digit = true;
                if dot {
                    fractional += 1;
                }
                if digit != 0 || significant != 0 {
                    if let Some(slot) = leading.get_mut(significant as usize) {
                        *slot = digit as u8;
                    }
                    significant += 1;
                }
            }
        }
    }
    if !saw_digit || i32::try_from(fractional - exponent).is_err() {
        return Ok(None);
    }
    if significant == 0 {
        return Ok(Some(0));
    }
    let keep = significant + i64::from(scale) + exponent - fractional;
    if keep < 0 {
        return Ok(Some(0));
    }
    if keep > i64::from(precision) {
        return Ok(None);
    }
    let keep = keep as usize;
    let mut coefficient = leading[..keep].iter().fold(0_i128, |coefficient, digit| {
        coefficient * 10 + i128::from(*digit)
    });
    if leading[keep] >= 5 {
        coefficient += 1;
    }
    if coefficient >= 10_i128.pow(u32::from(precision)) {
        return Ok(None);
    }
    Ok(Some(if negative { -coefficient } else { coefficient }))
}

fn strip_sign(value: &str) -> (bool, &str) {
    if let Some(rest) = value.strip_prefix('-') {
        (true, rest)
    } else {
        (false, value.strip_prefix('+').unwrap_or(value))
    }
}

fn parse_exponent(
    value: &str,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<i64>, KernelFailure> {
    let (negative, value) = strip_sign(value);
    if value.is_empty() {
        return Ok(None);
    }
    let mut exponent = 0_i64;
    for ch in value.chars() {
        work.step()?;
        let Some(digit) = array_text::digit(ch, work)? else {
            return Ok(None);
        };
        exponent = exponent * 10 + digit;
        if exponent > i64::from(i32::MAX) {
            return Ok(None);
        }
    }
    Ok(Some(if negative { -exponent } else { exponent }))
}

fn parse_float(
    value: &str,
    target: Numeric,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<CastRowResult, ConversionFailure> {
    // Java trim is deliberately limited to U+0000 through U+0020.
    for _ in value.as_bytes().chunks(64) {
        work.step()?;
    }
    let value = value.trim_matches(|ch| ch <= '\u{20}');
    let (negative, unsigned) = strip_sign(value);
    let special = match unsigned {
        "NaN" => Some(f64::NAN),
        "Infinity" => Some(if negative {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        }),
        _ => None,
    };
    if let Some(value) = special {
        return Ok(if target == Numeric::Real {
            CastRowResult::Float32(value as f32)
        } else {
            CastRowResult::Float64(value)
        });
    }
    let value = value.strip_suffix(['d', 'D', 'f', 'F']).unwrap_or(value);
    let (_, unsigned) = strip_sign(value);
    if let Some(hex) = unsigned
        .strip_prefix("0x")
        .or_else(|| unsigned.strip_prefix("0X"))
    {
        return parse_hex_float(hex, negative, target, work);
    }
    // Rust's primitive parsing is exact at the requested IEEE width, but
    // accepts spellings of infinity and NaN that the original SQL rejects.
    if !unsigned.as_bytes().iter().any(u8::is_ascii_digit)
        || !unsigned
            .bytes()
            .all(|b| b.is_ascii_digit() || b".eE+-".contains(&b))
    {
        return Err(ConversionFailure::Invalid);
    }
    work.flush()?;
    if target == Numeric::Real {
        value
            .parse::<f32>()
            .map(CastRowResult::Float32)
            .map_err(|_| ConversionFailure::Invalid)
    } else {
        value
            .parse::<f64>()
            .map(CastRowResult::Float64)
            .map_err(|_| ConversionFailure::Invalid)
    }
}

fn parse_hex_float(
    value: &str,
    negative: bool,
    target: Numeric,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<CastRowResult, ConversionFailure> {
    let (coefficient, exponent) = value
        .split_once(['p', 'P'])
        .ok_or(ConversionFailure::Invalid)?;
    let (exponent_negative, exponent) = strip_sign(exponent);
    if exponent.is_empty() {
        return Err(ConversionFailure::Invalid);
    }
    let mut exponent_value = 0_i64;
    for byte in exponent.bytes() {
        work.step()?;
        if !byte.is_ascii_digit() {
            return Err(ConversionFailure::Invalid);
        }
        exponent_value = (exponent_value * 10 + i64::from(byte - b'0')).min(i64::from(i32::MAX));
    }
    if exponent_negative {
        exponent_value = -exponent_value;
    }
    let mut mantissa = 0_u64;
    let mut significant = 0_i64;
    let mut fractional = 0_i64;
    let mut sticky = false;
    let mut dot = false;
    let mut saw_digit = false;
    for byte in coefficient.bytes() {
        work.step()?;
        if byte == b'.' && !dot {
            dot = true;
            continue;
        }
        let digit = char::from(byte)
            .to_digit(16)
            .ok_or(ConversionFailure::Invalid)?;
        saw_digit = true;
        if dot {
            fractional += 1;
        }
        if digit != 0 || significant != 0 {
            if significant < 16 {
                mantissa = (mantissa << 4) | u64::from(digit);
            } else {
                sticky |= digit != 0;
            }
            significant += 1;
        }
    }
    if !saw_digit {
        return Err(ConversionFailure::Invalid);
    }
    let shift = exponent_value + 4 * ((significant - 16).max(0) - fractional);
    Ok(if target == Numeric::Real {
        let bits = hex_float_bits(mantissa, sticky, shift, 23, 127);
        CastRowResult::Float32(f32::from_bits(bits as u32 | (u32::from(negative) << 31)))
    } else {
        let bits = hex_float_bits(mantissa, sticky, shift, 52, 1023);
        CastRowResult::Float64(f64::from_bits(bits | (u64::from(negative) << 63)))
    })
}

fn hex_float_bits(mantissa: u64, sticky: bool, shift: i64, fraction_bits: u32, bias: i64) -> u64 {
    if mantissa == 0 {
        return 0;
    }
    let highest_bit = i64::from(63 - mantissa.leading_zeros());
    let exponent = highest_bit + shift;
    let minimum = 1 - bias;
    let infinity = ((2 * bias + 1) as u64) << fraction_bits;
    if exponent > bias {
        return infinity;
    }
    let bits_to_remove = exponent.max(minimum) - i64::from(fraction_bits) - shift;
    let rounded = match bits_to_remove {
        n if n <= 0 => mantissa << -n,
        1..=64 => {
            let half = 1_u64 << (bits_to_remove - 1);
            let remaining = mantissa & (half | (half - 1));
            let keep = mantissa.checked_shr(bits_to_remove as u32).unwrap_or(0);
            keep + u64::from(remaining > half || (remaining == half && (sticky || keep & 1 == 1)))
        }
        _ => 0,
    };
    if exponent < minimum {
        return rounded;
    }
    let implicit_bit = 1_u64 << fraction_bits;
    let encoded_exponent = ((exponent + bias) as u64) << fraction_bits;
    encoded_exponent + (rounded - implicit_bit)
}

fn render(
    array: &dyn Array,
    row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<String, ConversionFailure> {
    work.flush()?;
    let value = match array.data_type() {
        DataType::Decimal128(_, scale) => {
            let coefficient = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| invalid("decimal text cast lost its checked carrier"))?
                .value(row);
            let digits = coefficient.unsigned_abs().to_string();
            if *scale == 0 {
                coefficient.to_string()
            } else {
                let scale = *scale as usize;
                let sign = if coefficient < 0 { "-" } else { "" };
                if digits.len() <= scale {
                    format!("{sign}0.{digits:0>scale$}")
                } else {
                    let (whole, fraction) = digits.split_at(digits.len() - scale);
                    format!("{sign}{whole}.{fraction}")
                }
            }
        }
        DataType::Float32 => {
            let value = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| invalid("REAL text cast lost its checked carrier"))?
                .value(row);
            trino_float_text::format_real(value, work)?
        }
        DataType::Float64 => {
            let value = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| invalid("DOUBLE text cast lost its checked carrier"))?
                .value(row);
            trino_float_text::format_double(value, work)?
        }
        DataType::Date32 => arrow_array::temporal_conversions::date32_to_datetime(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| invalid("DATE text cast lost its checked carrier"))?
                .value(row),
        )
        .map(|date| date.date().to_string())
        .ok_or(ConversionFailure::Invalid)?,
        _ => crate::carrier_text::render(array, row).map_err(KernelFailure::from_domain)?,
    };
    work.flush()?;
    Ok(value)
}
