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

//! Typed argument and result rules for ROUND and TRUNCATE.

use crate::{
    FunctionArgument, FunctionArgumentType, FunctionBindingCreationFailure, FunctionBindingError,
    FunctionBindingRequest, FunctionValueType,
};
use arrow_schema::DataType;
use novarocks_type_contract::{CompileCheckpoints, ValueLogicalType, ValueSchemaCreationScope};

/// Public call shapes of the exact rounding result rules.
pub(super) fn public_signatures(name: &str) -> &'static [(&'static str, &'static str)] {
    match name {
        "round" => &[
            ("tinyint", "tinyint"),
            ("tinyint, integer", "tinyint"),
            ("smallint", "smallint"),
            ("smallint, integer", "smallint"),
            ("integer", "integer"),
            ("integer, integer", "integer"),
            ("bigint", "bigint"),
            ("bigint, integer", "bigint"),
            ("real", "real"),
            ("real, integer", "real"),
            ("double", "double"),
            ("double, integer", "double"),
            ("decimal(p,s)", "decimal(rp,0)"),
            ("decimal(p,s), integer", "decimal(rp,s)"),
        ],
        "truncate" => &[
            ("real", "real"),
            ("double", "double"),
            ("decimal(p,s)", "decimal(rp,0)"),
            ("decimal(p,s), integer", "decimal(p,s)"),
        ],
        _ => &[],
    }
}

pub(super) fn bind<H>(
    round: bool,
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
    creation: Option<&mut dyn ValueSchemaCreationScope<H>>,
) -> Result<(Vec<FunctionArgumentType>, FunctionValueType), FunctionBindingCreationFailure<H>> {
    let (source, digits) = match request.arguments {
        [FunctionArgument::Value { value_type, .. }] => (value_type, None),
        [
            FunctionArgument::Value { value_type, .. },
            FunctionArgument::Value {
                value_type: digits, ..
            },
        ] => (value_type, Some(digits)),
        _ => return Err(FunctionBindingError::NoMatchingOverload.into()),
    };
    if digits.is_some_and(|digits| {
        digits.logical_type() != ValueLogicalType::Physical
            || !matches!(
                digits.data_type(),
                DataType::Null | DataType::Int8 | DataType::Int16 | DataType::Int32
            )
    }) || source.logical_type() != ValueLogicalType::Physical
    {
        return Err(FunctionBindingError::NoMatchingOverload.into());
    }
    let formal = match source.data_type() {
        DataType::Null if round => DataType::Int8,
        DataType::Null => DataType::Decimal128(1, 0),
        t @ (DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64) if round => {
            t.clone()
        }
        DataType::Int8 => DataType::Decimal128(3, 0),
        DataType::Int16 => DataType::Decimal128(5, 0),
        DataType::Int32 => DataType::Decimal128(10, 0),
        DataType::Int64 => DataType::Decimal128(19, 0),
        t @ (DataType::Float32 | DataType::Float64) if round || digits.is_none() => t.clone(),
        t @ DataType::Decimal128(_, s) if *s >= 0 => t.clone(),
        _ => return Err(FunctionBindingError::NoMatchingOverload.into()),
    };
    let result = match (round, digits.is_some(), &formal) {
        (true, true, DataType::Decimal128(p, s)) => DataType::Decimal128((p + 1).min(38), *s),
        (true, false, DataType::Decimal128(p, s)) => {
            DataType::Decimal128(p - *s as u8 + u8::from(*s > 0), 0)
        }
        (false, false, DataType::Decimal128(p, s)) => {
            DataType::Decimal128((p - *s as u8).max(1), 0)
        }
        _ => formal.clone(),
    };
    let source_nullable = source.nullable() || source.data_type() == &DataType::Null;
    let digits_nullable =
        digits.is_some_and(|digits| digits.nullable() || digits.data_type() == &DataType::Null);
    work.step()?;
    super::scalar_construction::with_binding_creation(creation, work, |cx| {
        cx.array::<FunctionArgumentType>(request.arguments.len())?;
        let mut args = Vec::with_capacity(request.arguments.len());
        args.push(FunctionArgumentType::Value(cx.value(
            formal,
            source_nullable,
            ValueLogicalType::Physical,
        )?));
        if digits.is_some() {
            args.push(FunctionArgumentType::Value(cx.value(
                DataType::Int32,
                digits_nullable,
                ValueLogicalType::Physical,
            )?));
        }
        let result = cx.value(
            result,
            source_nullable || digits_nullable,
            ValueLogicalType::Physical,
        )?;
        Ok((args, result))
    })
}
