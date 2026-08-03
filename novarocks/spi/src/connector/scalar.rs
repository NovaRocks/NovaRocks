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

//! Provider-neutral scalar vocabulary shared by connector contracts.

use std::cmp::Ordering;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectorScalarType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Date32,
    TimestampMicros,
    TimestampNanos,
    Utf8,
    Binary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConnectorScalarValue {
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Date32(i32),
    TimestampMicros(i64),
    TimestampNanos(i64),
    Utf8(String),
    Binary(Vec<u8>),
}

impl ConnectorScalarValue {
    pub const fn data_type(&self) -> ConnectorScalarType {
        match self {
            Self::Boolean(_) => ConnectorScalarType::Boolean,
            Self::Int8(_) => ConnectorScalarType::Int8,
            Self::Int16(_) => ConnectorScalarType::Int16,
            Self::Int32(_) => ConnectorScalarType::Int32,
            Self::Int64(_) => ConnectorScalarType::Int64,
            Self::Date32(_) => ConnectorScalarType::Date32,
            Self::TimestampMicros(_) => ConnectorScalarType::TimestampMicros,
            Self::TimestampNanos(_) => ConnectorScalarType::TimestampNanos,
            Self::Utf8(_) => ConnectorScalarType::Utf8,
            Self::Binary(_) => ConnectorScalarType::Binary,
        }
    }

    pub(crate) fn payload_bytes(&self) -> usize {
        match self {
            Self::Boolean(_) | Self::Int8(_) => 1,
            Self::Int16(_) => 2,
            Self::Int32(_) | Self::Date32(_) => 4,
            Self::Int64(_) | Self::TimestampMicros(_) | Self::TimestampNanos(_) => 8,
            Self::Utf8(value) => value.len(),
            Self::Binary(value) => value.len(),
        }
    }

    pub(crate) fn variable_payload_bytes(&self) -> Option<usize> {
        match self {
            Self::Utf8(value) => Some(value.len()),
            Self::Binary(value) => Some(value.len()),
            _ => None,
        }
    }

    /// Compares only values with the same frozen type. UTF-8 and binary use
    /// bytewise order; no collation, timezone, width, or unit conversion is implicit.
    pub fn compare_same_type(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Boolean(left), Self::Boolean(right)) => Some(left.cmp(right)),
            (Self::Int8(left), Self::Int8(right)) => Some(left.cmp(right)),
            (Self::Int16(left), Self::Int16(right)) => Some(left.cmp(right)),
            (Self::Int32(left), Self::Int32(right)) => Some(left.cmp(right)),
            (Self::Int64(left), Self::Int64(right)) => Some(left.cmp(right)),
            (Self::Date32(left), Self::Date32(right)) => Some(left.cmp(right)),
            (Self::TimestampMicros(left), Self::TimestampMicros(right)) => Some(left.cmp(right)),
            (Self::TimestampNanos(left), Self::TimestampNanos(right)) => Some(left.cmp(right)),
            (Self::Utf8(left), Self::Utf8(right)) => Some(left.as_bytes().cmp(right.as_bytes())),
            (Self::Binary(left), Self::Binary(right)) => Some(left.cmp(right)),
            _ => None,
        }
    }
}
