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

#[test]
fn conversions_involving_unsigned_carriers_are_rejected_at_authorship() {
    let unsigned = [
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ];
    let carriers = [
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal128(20, 0),
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ];
    for source in &carriers {
        for target in &carriers {
            if source == target || !(unsigned.contains(source) || unsigned.contains(target)) {
                continue;
            }
            for mode in [TrinoCastMode::Explicit, TrinoCastMode::Try] {
                let error = match cast_fixture_in_mode(
                    FunctionValueType::new(source.clone(), true),
                    FunctionValueType::new(target.clone(), true),
                    Source::Column,
                    Wrap::Bare,
                    mode,
                    false,
                ) {
                    Ok(_) => panic!("{mode:?} admitted {source:?} -> {target:?}"),
                    Err(error) => error,
                };
                let CallAuthorError::Capability(CapabilityRefusal::Primitive {
                    source: cause, ..
                }) = error
                else {
                    panic!(
                        "expected a typed CAST refusal for {source:?} -> {target:?}, got {error:?}"
                    );
                };
                assert_eq!(
                    cause.downcast_ref::<CastPrepareError>(),
                    Some(&CastPrepareError::Illegal),
                    "{mode:?} {source:?} -> {target:?}"
                );
            }
        }
    }
}
