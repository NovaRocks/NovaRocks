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
//! Schema-type to on-disk logical-type mapping.
//!
//! This module keeps FE schema text mapping centralized for native reads.
//!
//! Current limitations:
//! - Maps only scalar schema types used by the native reader.
//! - Decimal mapping supports DECIMALV3 only (`DECIMAL32/64/128/256`).
//! - Unsupported or unknown schema types are rejected during plan build.

use arrow::datatypes::DataType;

use super::constants::*;
use super::types::decimal::DecimalOutputMeta;

/// Resolve StarRocks schema type text to on-disk logical type id.
///
/// The mapping follows StarRocks FE schema type naming.
pub(super) fn expected_logical_type_from_schema_type(schema_type: &str) -> Option<i32> {
    match schema_type.trim().to_ascii_uppercase().as_str() {
        "TINYINT" => Some(LOGICAL_TYPE_TINYINT),
        "SMALLINT" => Some(LOGICAL_TYPE_SMALLINT),
        "INT" => Some(LOGICAL_TYPE_INT),
        "BIGINT" => Some(LOGICAL_TYPE_BIGINT),
        "LARGEINT" => Some(LOGICAL_TYPE_LARGEINT),
        "FLOAT" => Some(LOGICAL_TYPE_FLOAT),
        "DOUBLE" => Some(LOGICAL_TYPE_DOUBLE),
        "BOOLEAN" => Some(LOGICAL_TYPE_BOOLEAN),
        "DATE" | "DATE_V2" => Some(LOGICAL_TYPE_DATE),
        "DATETIME" | "DATETIME_V2" | "TIMESTAMP" => Some(LOGICAL_TYPE_DATETIME),
        "CHAR" => Some(LOGICAL_TYPE_CHAR),
        "VARCHAR" | "STRING" => Some(LOGICAL_TYPE_VARCHAR),
        "HLL" => Some(LOGICAL_TYPE_HLL),
        "BITMAP" | "OBJECT" => Some(LOGICAL_TYPE_OBJECT),
        "JSON" => Some(LOGICAL_TYPE_JSON),
        "BINARY" => Some(LOGICAL_TYPE_BINARY),
        "VARBINARY" => Some(LOGICAL_TYPE_VARBINARY),
        "DECIMAL32" => Some(LOGICAL_TYPE_DECIMAL32),
        "DECIMAL64" => Some(LOGICAL_TYPE_DECIMAL64),
        "DECIMAL128" => Some(LOGICAL_TYPE_DECIMAL128),
        "DECIMAL256" => Some(LOGICAL_TYPE_DECIMAL256),
        _ => None,
    }
}

/// Whether the projected schema type is CHAR and requires trailing-zero trim.
pub(super) fn is_char_schema_type(schema_type: &str) -> bool {
    schema_type.trim().eq_ignore_ascii_case("CHAR")
}

/// Build decimal output metadata from output Arrow type.
///
/// Returns metadata only for `Arrow::Decimal128/Decimal256`, which corresponds to DECIMALV3.
pub(super) fn decimal_output_meta_from_arrow_type(
    data_type: &DataType,
) -> Option<DecimalOutputMeta> {
    match data_type {
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            Some(DecimalOutputMeta {
                precision: *precision,
                scale: *scale,
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expected_logical_type_supports_decimal_v3() {
        assert_eq!(
            expected_logical_type_from_schema_type("DECIMAL32"),
            Some(LOGICAL_TYPE_DECIMAL32)
        );
        assert_eq!(
            expected_logical_type_from_schema_type("DECIMAL64"),
            Some(LOGICAL_TYPE_DECIMAL64)
        );
        assert_eq!(
            expected_logical_type_from_schema_type("DECIMAL128"),
            Some(LOGICAL_TYPE_DECIMAL128)
        );
        assert_eq!(
            expected_logical_type_from_schema_type("DECIMAL256"),
            Some(LOGICAL_TYPE_DECIMAL256)
        );
        assert_eq!(expected_logical_type_from_schema_type("DECIMALV2"), None);
    }

    #[test]
    fn expected_logical_type_supports_hll_and_object() {
        assert_eq!(
            expected_logical_type_from_schema_type("HLL"),
            Some(LOGICAL_TYPE_HLL)
        );
        assert_eq!(
            expected_logical_type_from_schema_type("OBJECT"),
            Some(LOGICAL_TYPE_OBJECT)
        );
        assert_eq!(
            expected_logical_type_from_schema_type("BITMAP"),
            Some(LOGICAL_TYPE_OBJECT)
        );
        assert_eq!(
            expected_logical_type_from_schema_type("JSON"),
            Some(LOGICAL_TYPE_JSON)
        );
    }

    #[test]
    fn expected_logical_type_supports_largeint() {
        assert_eq!(
            expected_logical_type_from_schema_type("LARGEINT"),
            Some(LOGICAL_TYPE_LARGEINT)
        );
    }
}
