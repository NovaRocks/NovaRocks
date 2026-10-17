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

//! Exact integer coefficient preparation for nominal LARGEINT conversions.
//! Decimal negative scales use the original integral truncation toward zero.
use arrow_buffer::i256;
use arrow_schema::DataType;

#[derive(Clone, Copy, Debug)]
pub(crate) struct DecimalRecipe {
    decimal128: bool,
    precision: u8,
    scale: i8,
    factor: i256,
    limit: i256,
}
impl DecimalRecipe {
    pub(crate) fn try_new(target: &DataType) -> Option<Self> {
        let (decimal128, precision, scale, maximum) = match *target {
            DataType::Decimal128(p, s) => (true, p, s, 38),
            DataType::Decimal256(p, s) => (false, p, s, 76),
            _ => return None,
        };
        if !(1..=maximum).contains(&precision)
            || scale < -(maximum as i8)
            || scale > precision as i8
        {
            return None;
        }
        // Both bounded powers are representable in i256. This is numerical
        // preparation only; no formatted text or floating point participates.
        Some(Self {
            decimal128,
            precision,
            scale,
            factor: crate::legacy_decimal::pow10_i256(usize::from(scale.unsigned_abs())).ok()?,
            limit: crate::legacy_decimal::pow10_i256(usize::from(precision)).ok()?,
        })
    }
    pub(crate) fn is_decimal128(self) -> bool {
        self.decimal128
    }
    pub(crate) fn precision(self) -> u8 {
        self.precision
    }
    pub(crate) fn scale(self) -> i8 {
        self.scale
    }
    pub(crate) fn convert(self, value: i128) -> Option<i256> {
        let value = i256::from_i128(value);
        let coefficient = if self.scale >= 0 {
            value.checked_mul(self.factor)?
        } else {
            value.checked_div(self.factor)?
        };
        (coefficient > -self.limit && coefficient < self.limit).then_some(coefficient)
    }
    pub(crate) fn can_overflow(self) -> bool {
        // Scaling is monotone, so these exact endpoints prove the full i128
        // domain. i128::MIN is widened before any arithmetic or negation.
        self.convert(i128::MIN).is_none() || self.convert(i128::MAX).is_none()
    }
}
