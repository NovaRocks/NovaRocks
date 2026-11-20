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

use super::{super::contract, fixtures::Fixture};
use arrow::datatypes::DataType;
use novarocks_local_program::SetOpKind;
use novarocks_type_contract::FunctionValueType;

#[test]
fn first_failure_prevents_any_publication_or_tail_read() {
    let fixture = Fixture::new(
        SetOpKind::Except,
        &FunctionValueType::new(DataType::Int32, true),
        1,
    );
    let first = contract("original E refusal");
    let got = fixture.frame.inputs.fail_input(0, first.clone());
    fixture.frame.fail(got);
    fixture.frame.fail(contract("secondary Q refusal"));
    assert_eq!(
        fixture
            .frame
            .completion
            .published()
            .err()
            .unwrap()
            .to_string(),
        first.to_string()
    );
    assert_eq!(
        fixture
            .frame
            .claim_tail(&fixture.work())
            .err()
            .unwrap()
            .to_string(),
        first.to_string()
    );
}
