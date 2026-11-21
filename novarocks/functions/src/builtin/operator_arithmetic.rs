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

use arrow_schema::DataType;
use novarocks_type_contract::{ArithmeticOperator, FunctionValueType, ValueLogicalType};

use crate::arithmetic::trino_decimal;
use crate::{FunctionBindingError, FunctionIntrinsicRowError};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(super) enum ArithmeticSignature {
    TinyInt,
    SmallInt,
    Integer,
    BigInt,
    Real,
    Double,
    Decimal,
}

#[derive(Clone, Copy)]
enum NumericArgument {
    Null,
    Number(ArithmeticSignature),
    Decimal(u8, i8),
}

impl NumericArgument {
    fn from_type(value: &FunctionValueType) -> Result<Self, FunctionBindingError> {
        use ArithmeticSignature::*;

        if value.logical_type() != ValueLogicalType::Physical {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        Ok(match value.data_type() {
            DataType::Null => Self::Null,
            DataType::Int8 => Self::Number(TinyInt),
            DataType::Int16 => Self::Number(SmallInt),
            DataType::Int32 => Self::Number(Integer),
            DataType::Int64 => Self::Number(BigInt),
            DataType::Float32 => Self::Number(Real),
            DataType::Float64 => Self::Number(Double),
            DataType::Decimal128(precision, scale)
                if (1..=38).contains(precision) && (0..=*precision as i8).contains(scale) =>
            {
                Self::Decimal(*precision, *scale)
            }
            _ => return Err(FunctionBindingError::NoMatchingOverload),
        })
    }

    fn decimal(self) -> (u8, i8) {
        use ArithmeticSignature::*;

        match self {
            Self::Decimal(precision, scale) => (precision, scale),
            Self::Null => (1, 0),
            Self::Number(TinyInt) => (3, 0),
            Self::Number(SmallInt) => (5, 0),
            Self::Number(Integer) => (10, 0),
            Self::Number(BigInt) => (19, 0),
            Self::Number(_) => unreachable!("floating arithmetic has a floating signature"),
        }
    }
}

impl ArithmeticSignature {
    pub(super) const ALL: [Self; 7] = [
        Self::TinyInt,
        Self::SmallInt,
        Self::Integer,
        Self::BigInt,
        Self::Real,
        Self::Double,
        Self::Decimal,
    ];

    pub(super) const fn pattern(self) -> &'static str {
        match self {
            Self::TinyInt => "(tinyint,tinyint)",
            Self::SmallInt => "(smallint,smallint)",
            Self::Integer => "(integer,integer)",
            Self::BigInt => "(bigint,bigint)",
            Self::Real => "(real,real)",
            Self::Double => "(double,double)",
            Self::Decimal => "(decimal(p1,s1),decimal(p2,s2))",
        }
    }

    pub(super) const fn name(self) -> &'static str {
        match self {
            Self::TinyInt => "tinyint",
            Self::SmallInt => "smallint",
            Self::Integer => "integer",
            Self::BigInt => "bigint",
            Self::Real => "real",
            Self::Double => "double",
            Self::Decimal => "decimal",
        }
    }

    pub(super) const fn result_pattern(self) -> &'static str {
        match self {
            Self::Decimal => "decimal(result_precision,result_scale)",
            _ => self.name(),
        }
    }

    pub(super) const fn row_error(self) -> FunctionIntrinsicRowError {
        match self {
            Self::Real | Self::Double => FunctionIntrinsicRowError::NoRowError,
            _ => FunctionIntrinsicRowError::MayRaise,
        }
    }
}

pub(super) struct ArithmeticTypes {
    pub signature: ArithmeticSignature,
    pub arguments: [DataType; 2],
    pub result: DataType,
}

pub(super) fn arithmetic_types(
    operator: ArithmeticOperator,
    [left, right]: [&FunctionValueType; 2],
) -> Result<ArithmeticTypes, FunctionBindingError> {
    use ArithmeticOperator::{Divide, Modulo, Multiply};
    use ArithmeticSignature::{BigInt, Double, Integer, Real, SmallInt, TinyInt};
    use NumericArgument::{Decimal, Null, Number};

    let left = NumericArgument::from_type(left)?;
    let right = NumericArgument::from_type(right)?;

    // When only unknown arguments need coercion, the pinned Trino binder
    // retains competing interval signatures for these arities and widths.
    let signature = match (left, right) {
        (Null, Null) if operator == Modulo => TinyInt,
        (Null, Null) => return Err(FunctionBindingError::AmbiguousOverload),
        (Null, Number(Double)) if matches!(operator, Divide | Multiply) => {
            return Err(FunctionBindingError::AmbiguousOverload);
        }
        (Number(Double | BigInt), Null) | (Null, Number(BigInt)) if operator == Multiply => {
            return Err(FunctionBindingError::AmbiguousOverload);
        }
        (Number(Double), _) | (_, Number(Double)) => Double,
        (Number(Real), _) | (_, Number(Real)) => Real,
        (Decimal(..), _) | (_, Decimal(..)) => {
            let (p1, s1) = left.decimal();
            let (p2, s2) = right.decimal();
            return Ok(ArithmeticTypes {
                signature: ArithmeticSignature::Decimal,
                arguments: [DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)],
                result: trino_decimal::result_type(operator, (p1, s1), (p2, s2)),
            });
        }
        (Number(left), Number(right)) => left.max(right),
        (Number(known), Null) | (Null, Number(known)) => known,
    };
    let carrier = match signature {
        TinyInt => DataType::Int8,
        SmallInt => DataType::Int16,
        Integer => DataType::Int32,
        BigInt => DataType::Int64,
        Real => DataType::Float32,
        Double => DataType::Float64,
        ArithmeticSignature::Decimal => unreachable!("decimal retains distinct input declarations"),
    };
    Ok(ArithmeticTypes {
        signature,
        arguments: [carrier.clone(), carrier.clone()],
        result: carrier,
    })
}
