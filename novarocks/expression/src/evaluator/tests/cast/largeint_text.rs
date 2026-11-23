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

use crate::testkit::numeric_fixtures::{Source, Wrap, cast_fixture_in_mode};
use crate::{CallAuthorError, CapabilityRefusal};
use arrow::datatypes::DataType;
use novarocks_type_contract::{FunctionValueType, TrinoCastMode, ValueLogicalType};

#[test]
fn retired_largeint_cannot_cast_to_varchar() {
    let source = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    for mode in [TrinoCastMode::Explicit, TrinoCastMode::Try] {
        let result = cast_fixture_in_mode(
            source.clone(),
            FunctionValueType::new(DataType::Utf8, true),
            Source::Column,
            Wrap::Bare,
            mode,
            false,
        );
        assert!(matches!(
            result,
            Err(CallAuthorError::Capability(CapabilityRefusal::Primitive {
                cast: None,
                ..
            }))
        ));
    }
}
