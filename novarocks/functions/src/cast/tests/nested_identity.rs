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

use super::super::*;
use crate::builtin::signature::declared_list;
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
#[test]
fn container_identity_requires_complete_nominal_semantics() {
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let source = declared_list(&json, false).unwrap();
    let plain = declared_list(&FunctionValueType::new(DataType::Utf8, true), true).unwrap();
    assert_eq!(source.data_type(), plain.data_type());
    assert_eq!(
        PreparedCastRecipe::try_new(
            CastOperation::Carrier,
            &source,
            &plain,
            DecimalOverflowPolicy::ReportError,
            true,
            &Control
        ),
        Err(CastPrepareError::Unsupported)
    );
    let same_semantics = source.with_nullable(true).unwrap();
    assert!(
        PreparedCastRecipe::try_new(
            CastOperation::Carrier,
            &source,
            &same_semantics,
            DecimalOverflowPolicy::ReportError,
            true,
            &Control
        )
        .unwrap()
        .is_identity()
    );
}
