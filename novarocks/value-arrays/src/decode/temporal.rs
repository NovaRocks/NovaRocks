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

use arrow_array::{Array, FixedSizeBinaryArray, Int32Array, Int64Array, TimestampMicrosecondArray};
use arrow_schema::DataType;
use novarocks_type_contract::{TemporalRelationKey, TemporalType, TemporalValue};

use crate::{ValueError, ValueViolation};

/// Borrowed temporal row access selected from a complete declaration once
/// per batch. A byte width never establishes temporal semantics.
pub struct PreparedTemporalArray<'a> {
    array: &'a dyn Array,
    carrier: TemporalArrayCarrier<'a>,
    declaration: TemporalType,
    nullable: bool,
}

enum TemporalArrayCarrier<'a> {
    I32(&'a Int32Array),
    I64(&'a Int64Array),
    TimestampMicros(&'a TimestampMicrosecondArray),
    Fixed(&'a FixedSizeBinaryArray),
}

impl<'a> PreparedTemporalArray<'a> {
    pub fn bind(
        declaration: TemporalType,
        array: &'a dyn Array,
        nullable: bool,
    ) -> Result<Self, ValueError> {
        if declaration.carrier() != *array.data_type() {
            return Err(ValueError::invariant(ValueViolation::Layout));
        }
        let any = array.as_any();
        let carrier = match array.data_type() {
            DataType::Int32 => any.downcast_ref().map(TemporalArrayCarrier::I32),
            DataType::Int64 => any.downcast_ref().map(TemporalArrayCarrier::I64),
            DataType::Timestamp(..) => any
                .downcast_ref()
                .map(TemporalArrayCarrier::TimestampMicros),
            DataType::FixedSizeBinary(_) => any.downcast_ref().map(TemporalArrayCarrier::Fixed),
            _ => None,
        }
        .ok_or_else(|| ValueError::invariant(ValueViolation::Carrier))?;
        Ok(Self {
            declaration,
            array,
            carrier,
            nullable,
        })
    }

    /// A caller supplies only reachable Arrow rows. In particular, children
    /// behind a NULL parent do not participate in range or NULL validation.
    pub fn value(&self, row: usize) -> Result<Option<TemporalValue>, ValueError> {
        if row >= self.array.len() {
            return Err(ValueError::invariant(ValueViolation::Bounds));
        }
        if self.array.is_null(row) {
            return if self.nullable {
                Ok(None)
            } else {
                Err(ValueError::invariant(ValueViolation::RequiredNull))
            };
        }
        match self.carrier {
            TemporalArrayCarrier::I32(array) => {
                TemporalValue::from_i32_carrier(self.declaration, array.value(row))
            }
            TemporalArrayCarrier::I64(array) => {
                TemporalValue::from_i64_carrier(self.declaration, array.value(row))
            }
            TemporalArrayCarrier::TimestampMicros(array) => {
                TemporalValue::from_i64_carrier(self.declaration, array.value(row))
            }
            TemporalArrayCarrier::Fixed(array) => {
                TemporalValue::from_fixed_carrier(self.declaration, array.value(row))
            }
        }
        .map(Some)
        .map_err(|_| ValueError::invariant(ValueViolation::Carrier))
    }

    pub fn relation_key(&self, row: usize) -> Result<Option<TemporalRelationKey>, ValueError> {
        self.value(row)?
            .map(|value| {
                value
                    .relation_key(self.declaration)
                    .map_err(|_| ValueError::invariant(ValueViolation::Carrier))
            })
            .transpose()
    }
}
