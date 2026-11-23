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
use novarocks_result_contract::{NativeRenderType, RenderField, RenderPresentation};
use novarocks_type_contract::{
    FunctionValueType, ValueLogicalType,
    result_render_type::{render_field_matches_declaration, render_field_matches_storage},
};

#[test]
fn uuid_render_declaration_authenticates_the_nominal_type_and_width() {
    let render = RenderField {
        native_type: NativeRenderType::Uuid,
        nullable: true,
        presentation: RenderPresentation::TrinoValue,
    };
    for (logical, valid) in [
        (ValueLogicalType::Uuid, true),
        (ValueLogicalType::LargeInt, false),
        (ValueLogicalType::Physical, false),
    ] {
        let declaration =
            FunctionValueType::try_with_logical_type(DataType::FixedSizeBinary(16), true, logical)
                .unwrap();
        assert_eq!(
            render_field_matches_declaration(&render, &declaration),
            valid
        );
    }
    assert!(!render_field_matches_storage(
        &render,
        &DataType::FixedSizeBinary(15),
        true
    ));
    assert!(!render_field_matches_storage(
        &render,
        &DataType::Binary,
        true
    ));
    assert!(!render_field_matches_storage(
        &render,
        &DataType::FixedSizeBinary(16),
        false
    ));
}
