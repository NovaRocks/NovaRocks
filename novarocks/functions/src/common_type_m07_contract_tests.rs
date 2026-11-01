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

use arrow_schema::{DataType, Field};
use novarocks_type_contract::{NR_LOGICAL_TYPE_KEY, wider_type};
use std::sync::Arc;

#[test]
fn m07_common_type_keeps_original_logical_marker_but_not_provider_decoration() {
    let tagged = Arc::new(Field::new("item", DataType::Utf8, false).with_metadata(
        std::collections::HashMap::<String, String>::from([
            (NR_LOGICAL_TYPE_KEY.to_owned(), "json".to_owned()),
            ("provider_field_id".to_owned(), "17".to_owned()),
        ]),
    ));
    let empty = Arc::new(Field::new("item", DataType::Null, true));
    let result = wider_type(&DataType::List(tagged), &DataType::List(empty));
    assert_eq!(
        result,
        DataType::List(Arc::new(
            Field::new("item", DataType::Utf8, true).with_metadata(std::collections::HashMap::<
                String,
                String,
            >::from([(
                NR_LOGICAL_TYPE_KEY.to_owned(),
                "json".to_owned()
            )]))
        ))
    );
}

#[test]
fn m07_common_type_covers_actual_signed_integer_decimal_ranges() {
    for (integer, expected) in [
        (DataType::Int8, DataType::Decimal128(5, 2)),
        (DataType::Int16, DataType::Decimal128(7, 2)),
        (DataType::Int32, DataType::Decimal128(12, 2)),
        (DataType::Int64, DataType::Decimal128(21, 2)),
    ] {
        let decimal = DataType::Decimal128(3, 2);
        assert_eq!(wider_type(&decimal, &integer), expected);
        assert_eq!(wider_type(&integer, &decimal), expected);
    }
}

#[test]
fn m07_common_type_does_not_erase_conflicting_opaque_domain_as_variant() {
    let field = |marker: &str| {
        Arc::new(
            Field::new("item", DataType::LargeBinary, true).with_metadata(
                std::collections::HashMap::<String, String>::from([(
                    NR_LOGICAL_TYPE_KEY.to_owned(),
                    marker.to_owned(),
                )]),
            ),
        )
    };
    let result = wider_type(
        &DataType::List(field("hll")),
        &DataType::List(field("bitmap")),
    );
    let DataType::List(item) = result else {
        panic!("expected List")
    };
    assert_eq!(
        item.metadata().get(NR_LOGICAL_TYPE_KEY).map(String::as_str),
        Some("invalid")
    );
}
