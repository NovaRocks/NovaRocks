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

//! Writer assignment for same-family precision changes inside Iceberg values.

use arrow_array::{Array, ArrayRef, ListArray, MapArray, StructArray, make_array};
use arrow_buffer::NullBufferBuilder;
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    TemporalMicros, TemporalType, TemporalValue, TimeZoneKey, ValueKind, ValueTypeRef,
};
use novarocks_value_arrays::{
    ValueFailure, decode::PreparedTemporalArray, visit_reachable_child_rows_observed,
};

use super::{extent, temporal::CastFailure};
use crate::builtin::temporal_output::TemporalOutputBuilder;
use crate::kernel_control::{internal, invalid};
use crate::kernel_input::EvaluationCheckpoints;
use crate::temporal_cast::Type;
use crate::temporal_value::{LiteralError, ParsedTemporal, cast_precision};
use crate::{KernelFailure, RowDataError};

pub(in crate::builtin) fn matches<E>(
    source: &novarocks_type_contract::FunctionValueType,
    target: &novarocks_type_contract::FunctionValueType,
    work: &mut impl FnMut() -> Result<(), E>,
) -> Result<bool, E> {
    if !matches!(
        source.value_type().kind(),
        ValueKind::Struct(_) | ValueKind::List(_) | ValueKind::Map { .. }
    ) {
        return Ok(false);
    }
    Ok(check_node(
        source.value_type(),
        source.data_type(),
        target.value_type(),
        target.data_type(),
        work,
    )? == Some(true))
}

fn check_node<E>(
    source: &ValueTypeRef,
    source_layout: &DataType,
    target: &ValueTypeRef,
    target_layout: &DataType,
    work: &mut impl FnMut() -> Result<(), E>,
) -> Result<Option<bool>, E> {
    work()?;
    if source.nullable() && !target.nullable() {
        return Ok(None);
    }
    let mut changes = false;
    let mut check = |source, source_layout, target, target_layout| {
        let result = check_node(source, source_layout, target, target_layout, work)?;
        changes |= result == Some(true);
        Ok(result.is_some())
    };
    match (source.kind(), source_layout, target.kind(), target_layout) {
        (ValueKind::Temporal(source), _, ValueKind::Temporal(target), _) => {
            let supported = matches!(
                (source, target),
                (TemporalType::Time(_), TemporalType::Time(_))
                    | (TemporalType::Timestamp(_), TemporalType::Timestamp(_))
                    | (
                        TemporalType::TimestampWithTimeZone(_),
                        TemporalType::TimestampWithTimeZone(_)
                    )
            );
            return Ok((source == target || supported).then_some(source != target));
        }
        (
            ValueKind::List(source),
            DataType::List(a),
            ValueKind::List(target),
            DataType::List(b),
        ) => {
            if !check(source, a.data_type(), target, b.data_type())? {
                return Ok(None);
            }
        }
        (
            ValueKind::Struct(source),
            DataType::Struct(a),
            ValueKind::Struct(target),
            DataType::Struct(b),
        ) if source.len() == target.len() && a.len() == b.len() => {
            for (((source, a), target), b) in source.iter().zip(a).zip(target).zip(b) {
                if source.name() != target.name()
                    || !check(
                        source.value_type(),
                        a.data_type(),
                        target.value_type(),
                        b.data_type(),
                    )?
                {
                    return Ok(None);
                }
            }
        }
        (
            ValueKind::Map { key: sk, value: sv },
            DataType::Map(a, a_sorted),
            ValueKind::Map { key: tk, value: tv },
            DataType::Map(b, b_sorted),
        ) if a_sorted == b_sorted => {
            let (Some(a), Some(b)) = (entries(a), entries(b)) else {
                return Ok(None);
            };
            if !check(sk, a[0].data_type(), tk, b[0].data_type())?
                || !check(sv, a[1].data_type(), tv, b[1].data_type())?
            {
                return Ok(None);
            }
        }
        _ if source.child_count() == 0
            && source.kind() == target.kind()
            && source_layout == target_layout => {}
        _ => return Ok(None),
    }
    Ok(Some(changes))
}

fn entries(field: &Field) -> Option<&[arrow_schema::FieldRef]> {
    match field.data_type() {
        DataType::Struct(fields) if fields.len() == 2 && !field.is_nullable() => Some(fields),
        _ => None,
    }
}

pub(super) fn convert(
    array: &ArrayRef,
    source: &novarocks_type_contract::FunctionValueType,
    target: &novarocks_type_contract::FunctionValueType,
    errors: &mut Vec<RowDataError>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    let size = array.len();
    extent(size, std::mem::size_of::<Option<LiteralError>>())?;
    let mut failures = vec![None; size];
    let root = (0..size).map(|row| (row, row));
    let result = convert_at(
        array,
        source.value_type(),
        target.value_type(),
        target.data_type(),
        root,
        &mut failures,
        work,
    )?;
    if failures.iter().all(Option::is_none) {
        return Ok(result);
    }
    work.flush()?;
    let mut nulls = NullBufferBuilder::new(size);
    for (row, failure) in failures.into_iter().enumerate() {
        work.step()?;
        nulls.append(failure.is_none() && result.is_valid(row));
        if let Some(failure) = failure {
            errors
                .try_reserve(1)
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            errors.push(RowDataError::from_domain(row, CastFailure(failure))?);
        }
    }
    let data = result
        .to_data()
        .into_builder()
        .nulls(nulls.finish())
        .build()
        .map_err(|_| internal("temporal assignment could not mask an invalid row"))?;
    work.flush()?;
    Ok(make_array(data))
}

fn convert_at(
    array: &ArrayRef,
    source: &ValueTypeRef,
    target: &ValueTypeRef,
    target_layout: &DataType,
    owners: impl Iterator<Item = (usize, usize)>,
    failures: &mut [Option<LiteralError>],
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<ArrayRef, KernelFailure> {
    work.step()?;
    if let (Some(source), Some(target)) = (source.temporal(), target.temporal())
        && source != target
    {
        let prepared = PreparedTemporalArray::bind(source, array.as_ref(), true)
            .map_err(|_| invalid("temporal assignment requires its exact source carrier"))?;
        let mut output = TemporalOutputBuilder::new(Type::Temporal(target), array.len())?;
        let mut owned = owners.peekable();
        for row in 0..array.len() {
            work.step()?;
            let owner = owned
                .next_if(|(index, _)| *index == row)
                .map(|(_, owner)| owner);
            if array.is_null(row) {
                output.null();
                continue;
            }
            let Some(owner) = owner else {
                output.temporal(target, zero(target))?;
                continue;
            };
            let value = prepared
                .value(row)
                .map_err(|_| invalid("temporal assignment found an invalid source value"))?
                .ok_or_else(|| internal("non-NULL temporal source returned NULL"))?;
            match cast_precision(ParsedTemporal { ty: source, value }, target) {
                Ok(converted) => output.temporal(target, converted.value)?,
                Err(error) => {
                    failures[owner].get_or_insert(error);
                    output.temporal(target, zero(target))?;
                }
            }
        }
        work.flush()?;
        return Ok(output.finish());
    }
    if array.data_type() == target_layout && source == target {
        return Ok(array.clone());
    }
    let mut children = Vec::new();
    match (source.kind(), target.kind(), target_layout) {
        (ValueKind::List(source), ValueKind::List(target), DataType::List(field)) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| invalid("temporal assignment lost its Array source"))?;
            let child = child_owners(array, owners, 0, list.values().len(), work)?;
            children.push(
                convert_at(
                    list.values(),
                    source,
                    target,
                    field.data_type(),
                    child.into_iter().enumerate().filter_map(present),
                    failures,
                    work,
                )?
                .to_data(),
            );
        }
        (ValueKind::Struct(source), ValueKind::Struct(target), DataType::Struct(fields)) => {
            let rows = owners.collect::<Vec<_>>();
            let record = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| invalid("temporal assignment lost its Row source"))?;
            for (ordinal, (((value, source), target), field)) in record
                .columns()
                .iter()
                .zip(source.iter())
                .zip(target.iter())
                .zip(fields)
                .enumerate()
            {
                let child = child_owners(array, rows.iter().copied(), ordinal, value.len(), work)?;
                children.push(
                    convert_at(
                        value,
                        source.value_type(),
                        target.value_type(),
                        field.data_type(),
                        child.into_iter().enumerate().filter_map(present),
                        failures,
                        work,
                    )?
                    .to_data(),
                );
            }
        }
        (
            ValueKind::Map { key: sk, value: sv },
            ValueKind::Map { key: tk, value: tv },
            DataType::Map(field, _),
        ) => {
            let map = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| invalid("temporal assignment lost its Map source"))?;
            let fields = entries(field)
                .ok_or_else(|| invalid("temporal assignment has invalid Map target fields"))?;
            let child = child_owners(array, owners, 0, map.entries().len(), work)?;
            let convert_child =
                |values, source, target, field: &Field, failures: &mut [_], work: &mut _| {
                    convert_at(
                        values,
                        source,
                        target,
                        field.data_type(),
                        child.iter().copied().enumerate().filter_map(present),
                        failures,
                        work,
                    )
                };
            let keys = convert_child(map.keys(), sk, tk, &fields[0], failures, work)?;
            let values = convert_child(map.values(), sv, tv, &fields[1], failures, work)?;
            let entries = StructArray::try_new_with_length(
                fields.to_vec().into(),
                vec![keys, values],
                None,
                map.entries().len(),
            )
            .map_err(|_| internal("temporal assignment could not rebuild Map entries"))?;
            children.push(entries.to_data());
        }
        _ if source.kind() == target.kind() && array.data_type() == target_layout => {
            return Ok(array.clone());
        }
        _ => {
            return Err(invalid(
                "temporal assignment shape differs from its admitted pair",
            ));
        }
    }
    work.flush()?;
    let rebuilt = array
        .to_data()
        .into_builder()
        .data_type(target_layout.clone())
        .child_data(children)
        .build()
        .map_err(|_| internal("temporal assignment could not rebuild its target container"))?;
    let output = make_array(rebuilt);
    work.flush()?;
    Ok(output)
}

fn present((index, owner): (usize, Option<usize>)) -> Option<(usize, usize)> {
    owner.map(|owner| (index, owner))
}

fn child_owners(
    array: &ArrayRef,
    parents: impl Iterator<Item = (usize, usize)>,
    ordinal: usize,
    length: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Vec<Option<usize>>, KernelFailure> {
    use std::cell::RefCell;
    extent(length, std::mem::size_of::<Option<usize>>())?;
    let mut owners = vec![None; length];
    let work = RefCell::new(work);
    for (parent, owner) in parents {
        visit_reachable_child_rows_observed(
            array,
            [parent],
            &mut |_| work.borrow_mut().step(),
            &mut |index, range| {
                if index == ordinal {
                    for row in range {
                        work.borrow_mut().step()?;
                        owners[row] = Some(owner);
                    }
                }
                Ok(())
            },
        )
        .map_err(|error| match error {
            ValueFailure::Caller(cause) => cause,
            ValueFailure::Value(_) => {
                invalid("temporal assignment has invalid source child geometry")
            }
        })?;
    }
    Ok(owners)
}

fn zero(target: TemporalType) -> TemporalValue {
    let epoch = TemporalMicros::from_picos(0).expect("the epoch fits every temporal carrier");
    match target {
        TemporalType::Time(_) => TemporalValue::Time(0),
        TemporalType::Timestamp(_) => TemporalValue::Timestamp(epoch),
        TemporalType::TimestampWithTimeZone(_) => TemporalValue::TimestampWithTimeZone {
            instant: epoch,
            zone: TimeZoneKey::UTC,
        },
        _ => unreachable!("only same-family Iceberg temporal assignment enters the recipe"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_buffer::OffsetBuffer;
    use novarocks_type_contract::{
        FunctionValueType, TemporalPrecision, ValueLayout, ValueTypeStorageLifetime,
    };
    use std::sync::Arc;

    struct Control;

    impl crate::KernelEvaluationControl for Control {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            Ok(())
        }

        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            panic!("fixed temporal assignment must not wait")
        }
    }

    fn map_type(key: TemporalType) -> FunctionValueType {
        let lifetime = ValueTypeStorageLifetime::pure();
        let fields = vec![
            Arc::new(Field::new("key", key.carrier(), false)),
            Arc::new(Field::new("value", DataType::Int32, true)),
        ];
        let layout = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(fields.into()),
                false,
            )),
            false,
        );
        let value = ValueTypeRef::try_new(
            ValueKind::Map {
                key: ValueTypeRef::try_new(ValueKind::Temporal(key), false, lifetime.clone())
                    .unwrap(),
                value: ValueTypeRef::try_new(
                    ValueKind::Scalar(DataType::Int32),
                    true,
                    lifetime.clone(),
                )
                .unwrap(),
            },
            true,
            lifetime.clone(),
        )
        .unwrap();
        FunctionValueType::try_from_value(value, ValueLayout::try_new(&layout, lifetime).unwrap())
            .unwrap()
    }

    #[test]
    fn nested_temporal_assignment_rounds_once_and_reports_the_original_row() {
        let pico = TemporalType::Timestamp(TemporalPrecision::PICOSECONDS);
        let micros = TemporalType::Timestamp(TemporalPrecision::MICROSECONDS);
        let source = map_type(pico);
        let target = map_type(micros);
        assert!(matches(&source, &target, &mut || Ok::<_, ()>(())).unwrap());

        let mut keys = TemporalOutputBuilder::new(Type::Temporal(pico), 4).unwrap();
        let high = i128::from(i64::MAX) * 1_000_000;
        for picos in [0, 400_000, high + 900_000, high + 900_000] {
            keys.temporal(
                pico,
                TemporalValue::Timestamp(TemporalMicros::from_picos(picos).unwrap()),
            )
            .unwrap();
        }
        let DataType::Map(entries, false) = source.data_type() else {
            panic!("source map")
        };
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("source fields")
        };
        let array: ArrayRef = Arc::new(MapArray::new(
            entries.clone(),
            OffsetBuffer::new(vec![0, 2, 3, 4].into()),
            StructArray::new(
                fields.clone(),
                vec![keys.finish(), Arc::new(Int32Array::from(vec![1, 2, 3, 4]))],
                None,
            ),
            Some(vec![true, false, true].into()),
            false,
        ));
        let mut errors = Vec::new();
        let result = convert(
            &array,
            &source,
            &target,
            &mut errors,
            &mut EvaluationCheckpoints::new(&Control),
        )
        .unwrap();
        let map = result.as_any().downcast_ref::<MapArray>().unwrap();
        let keys = PreparedTemporalArray::bind(micros, map.keys().as_ref(), false).unwrap();
        assert_eq!(keys.value(0).unwrap(), keys.value(1).unwrap());
        assert!(map.is_null(1));
        assert!(map.is_null(2));
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].selected_ordinal(), 2);
    }
}
