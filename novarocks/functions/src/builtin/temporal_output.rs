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

//! Canonical selected temporal Arrow output shared by scalar kernels.

use std::sync::Arc;

use arrow_array::{
    ArrayRef,
    builder::{FixedSizeBinaryBuilder, PrimitiveBuilder, StringBuilder},
    types::{Date32Type, Int32Type, Int64Type, TimestampMicrosecondType},
};
use arrow_schema::DataType;
use novarocks_type_contract::{TemporalCarrier, TemporalType, TemporalValue};

use super::value_conversion_kernel::extent;
use crate::KernelFailure;
use crate::kernel_control::{internal, invalid};
use crate::temporal_cast::Type;

pub(crate) enum TemporalOutputBuilder {
    Date(PrimitiveBuilder<Date32Type>),
    I32(PrimitiveBuilder<Int32Type>),
    I64(PrimitiveBuilder<Int64Type>),
    Timestamp(PrimitiveBuilder<TimestampMicrosecondType>),
    Fixed(FixedSizeBinaryBuilder),
    Text(StringBuilder),
}

impl TemporalOutputBuilder {
    pub(crate) fn new(ty: Type, count: usize) -> Result<Self, KernelFailure> {
        Ok(match ty {
            Type::Date => {
                extent(count, 4)?;
                Self::Date(PrimitiveBuilder::with_capacity(count))
            }
            Type::Text => {
                extent(count, 128)?;
                Self::Text(StringBuilder::with_capacity(count, count * 128))
            }
            Type::Bigint => {
                extent(count, 8)?;
                Self::I64(PrimitiveBuilder::with_capacity(count))
            }
            Type::Integer => return Err(invalid("unexpected INTEGER conversion result")),
            Type::Temporal(_) | Type::LegacyTimestamp => {
                let carrier = ty
                    .temporal()
                    .ok_or_else(|| invalid("temporal output missing result type"))?
                    .carrier();
                match carrier {
                    DataType::Int32 => {
                        extent(count, 4)?;
                        Self::I32(PrimitiveBuilder::with_capacity(count))
                    }
                    DataType::Int64 => {
                        extent(count, 8)?;
                        Self::I64(PrimitiveBuilder::with_capacity(count))
                    }
                    DataType::Timestamp(..) => {
                        extent(count, 8)?;
                        Self::Timestamp(PrimitiveBuilder::with_capacity(count))
                    }
                    DataType::FixedSizeBinary(width) => {
                        extent(count, width as usize)?;
                        Self::Fixed(FixedSizeBinaryBuilder::with_capacity(count, width))
                    }
                    _ => return Err(invalid("temporal output has a foreign result carrier")),
                }
            }
        })
    }

    pub(crate) fn temporal(
        &mut self,
        ty: TemporalType,
        value: TemporalValue,
    ) -> Result<(), KernelFailure> {
        let wrong = || internal("temporal output differs from its frozen carrier");
        let fixed = |out: &mut FixedSizeBinaryBuilder, value: &[u8]| {
            out.append_value(value).map_err(|_| wrong())
        };
        match (self, value.to_carrier(ty).map_err(|_| wrong())?) {
            (Self::I32(out), TemporalCarrier::I32(value)) => out.append_value(value),
            (Self::I64(out), TemporalCarrier::I64(value)) => out.append_value(value),
            (Self::Timestamp(out), TemporalCarrier::TimestampMicros(value)) => {
                out.append_value(value)
            }
            (Self::Fixed(out), TemporalCarrier::Wide(value)) => fixed(out, &value)?,
            (Self::Fixed(out), TemporalCarrier::ZonedTime(value)) => fixed(out, &value)?,
            (Self::Fixed(out), TemporalCarrier::ZonedTimestamp(value)) => fixed(out, &value)?,
            _ => return Err(wrong()),
        }
        Ok(())
    }

    pub(crate) fn date(&mut self, value: i32) -> Result<(), KernelFailure> {
        let Self::Date(out) = self else {
            return Err(internal("unexpected date result"));
        };
        out.append_value(value);
        Ok(())
    }

    pub(crate) fn bigint(&mut self, value: i64) -> Result<(), KernelFailure> {
        let Self::I64(out) = self else {
            return Err(internal("unexpected BIGINT result"));
        };
        out.append_value(value);
        Ok(())
    }

    pub(crate) fn text(&mut self, value: &str) -> Result<(), KernelFailure> {
        let Self::Text(out) = self else {
            return Err(internal("unexpected VARCHAR result"));
        };
        out.append_value(value);
        Ok(())
    }

    pub(crate) fn null(&mut self) {
        match self {
            Self::Date(out) => out.append_null(),
            Self::I32(out) => out.append_null(),
            Self::I64(out) => out.append_null(),
            Self::Timestamp(out) => out.append_null(),
            Self::Fixed(out) => out.append_null(),
            Self::Text(out) => out.append_null(),
        }
    }

    pub(crate) fn finish(self) -> ArrayRef {
        match self {
            Self::Date(mut out) => Arc::new(out.finish()),
            Self::I32(mut out) => Arc::new(out.finish()),
            Self::I64(mut out) => Arc::new(out.finish()),
            Self::Timestamp(mut out) => Arc::new(out.finish()),
            Self::Fixed(mut out) => Arc::new(out.finish()),
            Self::Text(mut out) => Arc::new(out.finish()),
        }
    }
}
