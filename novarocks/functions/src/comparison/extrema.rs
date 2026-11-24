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

use super::{FlatLeaf, FlatOrderPolicy, cast, compare_flat, invalid};
use crate::{
    KernelFailure, aggregate_scalar::ExtremaDirection, kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ListArray, StructArray};
use arrow_schema::DataType;
use novarocks_type_contract::{CharacterKind, ValueKind, ValueLogicalType, ValueTypeRef};
use std::cmp::Ordering;

/// Borrowed comparison over an exact selected value layout. Only the winning
/// row address is retained by the scalar caller.
#[derive(Debug)]
pub(crate) struct ExtremaOrder {
    nullable: bool,
    kind: Kind,
}

#[derive(Debug)]
enum Kind {
    Flat(FlatLeaf),
    List(Box<ExtremaOrder>),
    Row(Box<[ExtremaOrder]>),
}

impl ExtremaOrder {
    pub(crate) fn prepare<E>(
        declaration: &ValueTypeRef,
        carrier: &DataType,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<Option<Self>, E> {
        Self::prepare_node(declaration, carrier, &mut observe)
    }

    pub(crate) fn supports<E>(
        declaration: &ValueTypeRef,
        carrier: &DataType,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        Self::supports_node(declaration, carrier, &mut observe)
    }

    fn supports_node<E>(
        declaration: &ValueTypeRef,
        carrier: &DataType,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        observe()?;
        match rule(declaration, carrier) {
            Some(Rule::Flat(_)) => Ok(true),
            Some(Rule::List(item, layout)) => Self::supports_node(item, layout, observe),
            Some(Rule::Row(fields, layouts)) => {
                for (field, layout) in fields.iter().zip(layouts) {
                    if !Self::supports_node(field.value_type(), layout.data_type(), observe)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn prepare_node<E>(
        declaration: &ValueTypeRef,
        carrier: &DataType,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<Option<Self>, E> {
        observe()?;
        let kind = match rule(declaration, carrier) {
            Some(Rule::Flat(leaf)) => Kind::Flat(leaf),
            Some(Rule::List(item, layout)) => {
                let Some(child) = Self::prepare_node(item, layout, observe)? else {
                    return Ok(None);
                };
                Kind::List(Box::new(child))
            }
            Some(Rule::Row(fields, layouts)) => {
                let mut children = Vec::with_capacity(fields.len());
                for (field, layout) in fields.iter().zip(layouts) {
                    let Some(child) =
                        Self::prepare_node(field.value_type(), layout.data_type(), observe)?
                    else {
                        return Ok(None);
                    };
                    children.push(child);
                }
                Kind::Row(children.into_boxed_slice())
            }
            None => return Ok(None),
        };
        Ok(Some(Self {
            nullable: declaration.nullable(),
            kind,
        }))
    }

    pub(crate) fn compare(
        &self,
        left: (&dyn Array, usize),
        right: (&dyn Array, usize),
        direction: ExtremaDirection,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Ordering, KernelFailure> {
        work.step()?;
        let missing = |(value, row): (&dyn Array, usize)| {
            value.data_type() == &DataType::Null || value.is_null(row)
        };
        let (left_null, right_null) = (missing(left), missing(right));
        if left_null || right_null {
            if !self.nullable {
                return Err(invalid(
                    "extrema comparison reached a NULL in a required value",
                ));
            }
            return Ok(direction.compare_unordered(left_null, right_null));
        }
        match &self.kind {
            Kind::Flat(leaf) => compare_flat(
                *leaf,
                left,
                right,
                (self.nullable, self.nullable),
                FlatOrderPolicy::Extrema(direction),
                work,
            )?
            .ok_or_else(|| invalid("extrema comparison returned NULL for present values")),
            Kind::List(item) => {
                let lhs = cast::<ListArray>(left.0)?;
                let rhs = cast::<ListArray>(right.0)?;
                let l = lhs.value_offsets();
                let r = rhs.value_offsets();
                let l = l[left.1] as usize..l[left.1 + 1] as usize;
                let r = r[right.1] as usize..r[right.1 + 1] as usize;
                let lengths = l.len().cmp(&r.len());
                for (left, right) in l.zip(r) {
                    let order = item.compare(
                        (lhs.values().as_ref(), left),
                        (rhs.values().as_ref(), right),
                        direction,
                        work,
                    )?;
                    if order != Ordering::Equal {
                        return Ok(order);
                    }
                }
                Ok(lengths)
            }
            Kind::Row(fields) => {
                let lhs = cast::<StructArray>(left.0)?;
                let rhs = cast::<StructArray>(right.0)?;
                for (index, field) in fields.iter().enumerate() {
                    let order = field.compare(
                        (lhs.column(index).as_ref(), left.1),
                        (rhs.column(index).as_ref(), right.1),
                        direction,
                        work,
                    )?;
                    if order != Ordering::Equal {
                        return Ok(order);
                    }
                }
                Ok(Ordering::Equal)
            }
        }
    }
}

enum Rule<'a> {
    Flat(FlatLeaf),
    List(&'a ValueTypeRef, &'a DataType),
    Row(
        &'a [novarocks_type_contract::ValueField],
        &'a arrow_schema::Fields,
    ),
}

fn rule<'a>(declaration: &'a ValueTypeRef, carrier: &'a DataType) -> Option<Rule<'a>> {
    Some(match (declaration.kind(), carrier) {
        (_, DataType::Timestamp(_, Some(zone)))
            if zone.parse::<arrow_array::timezone::Tz>().is_err() =>
        {
            return None;
        }
        (ValueKind::List(item), DataType::List(field)) => Rule::List(item, field.data_type()),
        (ValueKind::Struct(fields), DataType::Struct(layout)) if fields.len() == layout.len() => {
            Rule::Row(fields, layout)
        }
        (ValueKind::Temporal(temporal), _) => Rule::Flat(FlatLeaf::Temporal(*temporal)),
        (ValueKind::BoundedCharacter(character), DataType::Utf8)
            if character.kind() == CharacterKind::Char =>
        {
            Rule::Flat(FlatLeaf::Char)
        }
        (ValueKind::Nominal(ValueLogicalType::LargeInt), DataType::FixedSizeBinary(16)) => {
            Rule::Flat(FlatLeaf::LargeInt)
        }
        (ValueKind::Nominal(ValueLogicalType::Uuid), DataType::FixedSizeBinary(16)) => {
            Rule::Flat(FlatLeaf::FixedSizeBinary)
        }
        (
            ValueKind::Scalar(_)
            | ValueKind::String
            | ValueKind::BoundedCharacter(_)
            | ValueKind::Binary,
            _,
        ) => Rule::Flat(FlatLeaf::from_type(carrier)?),
        _ => return None,
    })
}
