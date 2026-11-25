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

//! Exact SQL entry points for the shared versioned JSON value.

use super::signature::{Signature, TypeSpec};
use crate::{FunctionBindingError, FunctionId, FunctionValueType};
use arrow_schema::DataType;
use novarocks_type_contract::{CompileCheckpoints, ValueKind, ValueLogicalType};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JsonEncoding {
    Utf8,
    Utf16,
    Utf32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JsonFunction {
    Parse,
    Scalar,
    Format,
    ReadText {
        null_on_error: bool,
    },
    ReadBinary {
        encoding: JsonEncoding,
        null_on_error: bool,
    },
    SerializeBinary(JsonEncoding),
    Array {
        null_on_null: bool,
    },
    Object {
        null_on_null: bool,
        unique: bool,
    },
}
use JsonEncoding::{Utf8, Utf16, Utf32};
use JsonFunction::*;

pub(super) const FUNCTIONS: &[(JsonFunction, &str)] = &[
    (Parse, "json_parse"),
    (Scalar, "json_scalar"),
    (Format, "json_format"),
    (
        ReadText {
            null_on_error: false,
        },
        "__json_read_text",
    ),
    (
        ReadText {
            null_on_error: true,
        },
        "__json_try_read_text",
    ),
    (
        ReadBinary {
            encoding: Utf8,
            null_on_error: false,
        },
        "__json_read_utf8",
    ),
    (
        ReadBinary {
            encoding: Utf16,
            null_on_error: false,
        },
        "__json_read_utf16",
    ),
    (
        ReadBinary {
            encoding: Utf32,
            null_on_error: false,
        },
        "__json_read_utf32",
    ),
    (
        ReadBinary {
            encoding: Utf8,
            null_on_error: true,
        },
        "__json_try_read_utf8",
    ),
    (
        ReadBinary {
            encoding: Utf16,
            null_on_error: true,
        },
        "__json_try_read_utf16",
    ),
    (
        ReadBinary {
            encoding: Utf32,
            null_on_error: true,
        },
        "__json_try_read_utf32",
    ),
    (SerializeBinary(Utf8), "__json_write_utf8"),
    (SerializeBinary(Utf16), "__json_write_utf16"),
    (SerializeBinary(Utf32), "__json_write_utf32"),
    (
        Array {
            null_on_null: false,
        },
        "__json_array_absent",
    ),
    (Array { null_on_null: true }, "__json_array_null"),
    (
        Object {
            null_on_null: false,
            unique: false,
        },
        "__json_object_absent",
    ),
    (
        Object {
            null_on_null: false,
            unique: true,
        },
        "__json_object_absent_unique",
    ),
    (
        Object {
            null_on_null: true,
            unique: false,
        },
        "__json_object_null",
    ),
    (
        Object {
            null_on_null: true,
            unique: true,
        },
        "__json_object_null_unique",
    ),
];

impl JsonFunction {
    pub fn name(self) -> &'static str {
        FUNCTIONS
            .iter()
            .find(|(function, _)| *function == self)
            .expect("JSON function has a registered name")
            .1
    }

    pub fn function_id(self) -> FunctionId {
        FunctionId::try_new(format!("builtin.scalar/{}/v1", self.name()))
            .expect("valid JSON function identity")
    }

    pub(super) fn from_name(name: &str) -> Option<Self> {
        FUNCTIONS
            .iter()
            .find(|(_, candidate)| *candidate == name)
            .map(|(function, _)| *function)
    }

    pub(super) fn public(self) -> bool {
        matches!(self, Parse | Scalar | Format)
    }

    pub(super) fn signature(self) -> Signature {
        match self {
            Parse | ReadText { .. } | Scalar => {
                Signature::new(vec![TypeSpec::AnyType], TypeSpec::Json)
            }
            ReadBinary { .. } => Signature::new(vec![TypeSpec::Binary], TypeSpec::Json),
            Format => Signature::new(vec![TypeSpec::Json], TypeSpec::Utf8),
            SerializeBinary(_) => Signature::new(vec![TypeSpec::Json], TypeSpec::Binary),
            Array { .. } => Signature::variadic(vec![TypeSpec::Json], TypeSpec::Json),
            Object { .. } => Signature::variadic(vec![TypeSpec::AnyType], TypeSpec::Json),
        }
    }

    pub(super) const fn returns_json(self) -> bool {
        !matches!(self, Format | SerializeBinary(_))
    }

    pub(super) const fn constructs(self) -> bool {
        matches!(self, Array { .. } | Object { .. })
    }

    pub(super) const fn null_on_error(self) -> bool {
        matches!(
            self,
            ReadText {
                null_on_error: true
            } | ReadBinary {
                null_on_error: true,
                ..
            }
        )
    }

    pub(super) const fn may_raise(self) -> bool {
        matches!(
            self,
            Parse
                | ReadText {
                    null_on_error: false
                }
                | ReadBinary {
                    null_on_error: false,
                    ..
                }
                | Array { .. }
                | Object { .. }
        )
    }

    pub(super) fn check_arguments<'a>(
        self,
        arguments: impl ExactSizeIterator<Item = Option<&'a FunctionValueType>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), FunctionBindingError> {
        if matches!(self, Object { .. }) && !arguments.len().is_multiple_of(2) {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        if !self.constructs() && arguments.len() != 1 {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        let key_count = arguments.len() / 2;
        for (index, argument) in arguments.enumerate() {
            work.step()?;
            let argument = argument.ok_or(FunctionBindingError::NoMatchingOverload)?;
            let valid = match self {
                Parse | ReadText { .. } => {
                    is_text(argument) || argument.data_type() == &DataType::Null
                }
                Scalar => valid_scalar(argument),
                ReadBinary { .. } => {
                    (argument.value_type().kind() == &ValueKind::Binary
                        && argument.data_type() == &DataType::Binary)
                        || argument.data_type() == &DataType::Null
                }
                Format | SerializeBinary(_) => {
                    is_json(argument) || argument.data_type() == &DataType::Null
                }
                Array { .. } => is_json(argument),
                Object { .. } if index < key_count => is_text(argument),
                Object { .. } => is_json(argument),
            };
            if !valid {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
        }
        Ok(())
    }
}

pub fn is_text(value: &FunctionValueType) -> bool {
    matches!(
        value.value_type().kind(),
        ValueKind::String | ValueKind::BoundedCharacter(_)
    ) && value.data_type() == &DataType::Utf8
}

pub fn is_json(value: &FunctionValueType) -> bool {
    value.logical_type() == ValueLogicalType::Json && value.data_type() == &DataType::LargeBinary
}

pub(crate) fn valid_scalar(value: &FunctionValueType) -> bool {
    is_text(value)
        || matches!(
            value.value_type().kind(),
            ValueKind::Scalar(
                DataType::Null
                    | DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Date32
            ) | ValueKind::Temporal(
                novarocks_type_contract::TemporalType::Time(_)
                    | novarocks_type_contract::TemporalType::TimeWithTimeZone(_)
                    | novarocks_type_contract::TemporalType::Timestamp(_)
                    | novarocks_type_contract::TemporalType::TimestampWithTimeZone(_)
            )
        )
        || matches!(value.data_type(), DataType::Decimal128(precision, scale)
        if *precision <= 38 && *scale >= 0)
}

/// Borrow a selected scalar while retaining its declared temporal/decimal facts.
pub(crate) fn read_scalar<'a>(
    array: &'a dyn arrow_array::Array,
    value_type: &FunctionValueType,
    row: usize,
) -> Result<Option<crate::json_value::Scalar<'a>>, crate::KernelFailure> {
    if array.data_type() != value_type.data_type() {
        return Err(crate::kernel_control::internal(
            "SQL JSON scalar input differs from its selected declaration",
        ));
    }
    read_scalar_value_type(array, value_type.value_type(), row)
}

/// The checked ARRAY/MAP/ROW reader borrows its frozen child declaration and
/// the actual Arrow carrier from the original source.
pub(crate) fn read_scalar_value_type<'a>(
    array: &'a dyn arrow_array::Array,
    value_type: &novarocks_type_contract::ValueTypeRef,
    row: usize,
) -> Result<Option<crate::json_value::Scalar<'a>>, crate::KernelFailure> {
    use crate::json_value::Scalar;
    use crate::kernel_control::internal;
    use arrow_array::*;
    use novarocks_type_contract::TemporalValue;

    if row >= array.len() {
        return Err(internal(
            "SQL JSON scalar input differs from its selected declaration",
        ));
    }
    if array.is_null(row) || array.data_type() == &DataType::Null {
        return Ok(None);
    }
    macro_rules! read {
        ($array:ty) => {
            array
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| internal("SQL JSON scalar input has an invalid Arrow carrier"))?
                .value(row)
        };
    }
    if let Some(ty) = value_type.temporal() {
        let value = match array.data_type() {
            DataType::Int64 => TemporalValue::from_i64_carrier(ty, read!(Int64Array)),
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None) => {
                TemporalValue::from_i64_carrier(ty, read!(TimestampMicrosecondArray))
            }
            DataType::FixedSizeBinary(_) => {
                TemporalValue::from_fixed_carrier(ty, read!(FixedSizeBinaryArray))
            }
            _ => {
                return Err(internal(
                    "SQL JSON scalar input has an invalid temporal carrier",
                ));
            }
        }
        .map_err(|_| internal("SQL JSON scalar input has an invalid temporal value"))?;
        return Ok(Some(Scalar::Temporal { ty, value }));
    }
    let scalar = match array.data_type() {
        DataType::Boolean => Scalar::Boolean(read!(BooleanArray)),
        DataType::Utf8
            if matches!(
                value_type.kind(),
                ValueKind::String | ValueKind::BoundedCharacter(_)
            ) =>
        {
            Scalar::String(read!(StringArray))
        }
        DataType::Int8 => Scalar::TinyInt(read!(Int8Array)),
        DataType::Int16 => Scalar::SmallInt(read!(Int16Array)),
        DataType::Int32 => Scalar::Integer(read!(Int32Array)),
        DataType::Int64 => Scalar::BigInt(read!(Int64Array)),
        DataType::Float32 => Scalar::Real(read!(Float32Array)),
        DataType::Float64 => Scalar::Double(read!(Float64Array)),
        DataType::Date32 => Scalar::Date(read!(Date32Array)),
        DataType::Decimal128(precision, scale) if *precision <= 38 && *scale >= 0 => {
            Scalar::Decimal {
                precision: *precision,
                scale: *scale as u8,
                coefficient: read!(Decimal128Array),
            }
        }
        _ => return Err(internal("SQL JSON scalar value has an unsupported type")),
    };
    Ok(Some(scalar))
}
