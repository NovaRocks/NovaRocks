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
use arrow::datatypes::DataType;

pub(super) fn avg_decimal_sum_info(
    intermediate_type: &DataType,
    output_type: Option<&DataType>,
) -> Result<(u8, i8), String> {
    // avg(decimal) intermediate can be an opaque VARBINARY/VARCHAR blob.
    // Its scale is defined by FE arg_types (input type), so it cannot be inferred from the intermediate type alone.
    // Callers must not use this helper for Binary/Utf8 intermediates.
    if matches!(intermediate_type, DataType::Binary | DataType::Utf8) {
        return Err(format!(
            "avg decimal intermediate type mismatch: {:?}",
            intermediate_type
        ));
    }

    let _ = output_type;

    let DataType::Struct(fields) = intermediate_type else {
        return Err(format!(
            "avg decimal intermediate type mismatch: {:?}",
            intermediate_type
        ));
    };
    if fields.len() != 2 {
        return Err("avg decimal intermediate expects 2 fields".to_string());
    }
    let sum_field = fields
        .get(0)
        .ok_or_else(|| "avg decimal intermediate missing sum field".to_string())?;
    let count_field = fields
        .get(1)
        .ok_or_else(|| "avg decimal intermediate missing count field".to_string())?;
    match count_field.data_type() {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {}
        other => {
            return Err(format!(
                "avg decimal intermediate count type mismatch: {:?}",
                other
            ));
        }
    }
    match sum_field.data_type() {
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            Ok((*precision, *scale))
        }
        other => Err(format!(
            "avg decimal intermediate sum type mismatch: {:?}",
            other
        )),
    }
}

pub(super) fn avg_decimal_sum_scale(
    intermediate_type: &DataType,
    output_type: &DataType,
) -> Result<i8, String> {
    let (_, scale) = avg_decimal_sum_info(intermediate_type, Some(output_type))?;
    Ok(scale)
}
