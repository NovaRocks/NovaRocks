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

use super::*;
use novarocks_expression::{
    CallAuthorError, CapabilityRefusal, testkit::numeric_fixtures::cast_fixture_in_mode,
};
use novarocks_functions::CastPrepareError;
use novarocks_type_contract::TrinoCastMode;

fn assert_illegal_cast(source: DataType, target: DataType) {
    for (value_source, nullable) in [(Source::Column, true), (Source::Constant, false)] {
        for mode in [TrinoCastMode::Explicit, TrinoCastMode::Try] {
            let error = match cast_fixture_in_mode(
                FunctionValueType::new(source.clone(), nullable),
                FunctionValueType::new(target.clone(), true),
                value_source,
                Wrap::Bare,
                mode,
                false,
            ) {
                Ok(_) => panic!("{mode:?} admitted {source:?} -> {target:?}"),
                Err(error) => error,
            };
            let CallAuthorError::Capability(CapabilityRefusal::Primitive { source: cause, .. }) =
                error
            else {
                panic!("expected a typed CAST refusal for {source:?} -> {target:?}, got {error:?}");
            };
            assert_eq!(
                cause.downcast_ref::<CastPrepareError>(),
                Some(&CastPrepareError::Illegal)
            );
        }
    }
}

#[test]
fn date_and_floating_point_casts_are_rejected_in_both_directions() {
    for float in [DataType::Float32, DataType::Float64] {
        assert_illegal_cast(DataType::Date32, float.clone());
        assert_illegal_cast(float, DataType::Date32);
    }
}

#[test]
fn varbinary_to_varchar_cast_is_rejected_at_authorship() {
    assert_illegal_cast(DataType::Binary, DataType::Utf8);
}
