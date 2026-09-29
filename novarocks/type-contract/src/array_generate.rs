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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use arrow_schema::DataType;

/// Bounds, not the unit string or row values, determine the frozen range domain.
/// SQL must normalize both temporal bounds to the selected common type first.
pub fn array_generate_item_type(arguments: &[DataType]) -> Option<DataType> {
    if arguments.is_empty() || arguments.len() > 4 {
        return None;
    }
    let bounds = &arguments[..arguments.len().min(2)];
    let temporal = bounds
        .iter()
        .find(|ty| matches!(ty, DataType::Date32 | DataType::Timestamp(_, _)));
    if let Some(temporal) = temporal {
        if bounds.len() != 2
            || !bounds.iter().all(|ty| ty == temporal)
            || arguments.get(2).is_some_and(|ty| {
                !matches!(
                    ty,
                    DataType::Null
                        | DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                )
            })
            || arguments.get(3).is_some_and(|ty| *ty != DataType::Utf8)
        {
            return None;
        }
        return Some((*temporal).clone());
    }
    if arguments.len() > 3 || bounds.iter().all(|ty| *ty == DataType::Null) {
        return None;
    }
    arguments
        .iter()
        .all(|ty| {
            matches!(
                ty,
                DataType::Null
                    | DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Decimal128(_, _)
                    | DataType::Decimal256(_, _)
            ) || crate::is_largeint_data_type(ty)
        })
        .then_some(DataType::Int64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::TimeUnit;

    #[test]
    fn range_domain_is_closed_and_unit_does_not_guess_bounds() {
        let timestamp = DataType::Timestamp(TimeUnit::Microsecond, None);
        for bound in [DataType::Date32, timestamp.clone()] {
            assert_eq!(
                array_generate_item_type(&[
                    bound.clone(),
                    bound.clone(),
                    DataType::Int64,
                    DataType::Utf8
                ]),
                Some(bound)
            );
        }
        for arguments in [
            vec![DataType::Date32, timestamp.clone()],
            vec![timestamp, DataType::Date32],
            vec![DataType::Utf8, DataType::Utf8],
            vec![DataType::Null, DataType::Null],
            vec![
                DataType::Int64,
                DataType::Int64,
                DataType::Int64,
                DataType::Utf8,
            ],
        ] {
            assert_eq!(array_generate_item_type(&arguments), None);
        }
        assert_eq!(
            array_generate_item_type(&[DataType::Int64, DataType::Null, DataType::Int64]),
            Some(DataType::Int64)
        );
    }
}
