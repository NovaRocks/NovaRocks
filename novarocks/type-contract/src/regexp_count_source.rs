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
//! Exact non-value provenance for REGEXP_COUNT's original error policy.
/// Native v1's checked Utf8 constant-reference form retains the original
/// literal error policy. Its NULL payload is masked before the policy matters;
/// every non-NULL payload uses Utf8Literal.
/// Dynamic includes casts, calls, slots and the separate legacy pooled form.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RegexpCountPatternSource {
    NativeV1Utf8LiteralWhenPresent,
    Dynamic,
}
impl RegexpCountPatternSource {
    /// Shared author for an already checked native-v1 emitted pattern.
    /// The caller supplies actual emitted shape, never lexical syntax or value
    /// equality. A standalone legacy pooled Constant is not this author.
    pub fn from_native_v1_emitted_shape(
        pattern_type: &crate::FunctionValueType,
        checked_constant_reference: bool,
    ) -> Result<Self, &'static str> {
        if pattern_type.logical_type() != crate::ValueLogicalType::Physical
            || *pattern_type.data_type() != arrow_schema::DataType::Utf8
        {
            return Err("regexp_count emitted pattern is not exact Physical Utf8");
        }
        Ok(if checked_constant_reference {
            Self::NativeV1Utf8LiteralWhenPresent
        } else {
            Self::Dynamic
        })
    }

    pub const fn invalid_pattern_is_error(self) -> bool {
        matches!(self, Self::NativeV1Utf8LiteralWhenPresent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn emitted_regexp_source_requires_actual_shape_and_exact_physical_utf8() {
        let ty = crate::FunctionValueType::new(arrow_schema::DataType::Utf8, true);
        assert_eq!(
            RegexpCountPatternSource::from_native_v1_emitted_shape(&ty, true),
            Ok(RegexpCountPatternSource::NativeV1Utf8LiteralWhenPresent)
        );
        assert_eq!(
            RegexpCountPatternSource::from_native_v1_emitted_shape(&ty, false),
            Ok(RegexpCountPatternSource::Dynamic)
        );
        for ty in [
            arrow_schema::DataType::Int64,
            arrow_schema::DataType::LargeUtf8,
            arrow_schema::DataType::Null,
        ] {
            assert!(
                RegexpCountPatternSource::from_native_v1_emitted_shape(
                    &crate::FunctionValueType::new(ty, true),
                    true
                )
                .is_err()
            );
        }
    }
}
