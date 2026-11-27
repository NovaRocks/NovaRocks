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

use crate::ValueType;
use novarocks_constant_contract::ConstantReference;
use novarocks_function_contract::{FunctionArgument, FunctionBindingRequest};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalCallRequest {
    pub arguments: Box<[FunctionArgument<ConstantReference>]>,
    pub logical_argument_count: usize,
    /// The original request's optional constraint, never its selected result.
    pub expected_result_type: Option<ValueType>,
}
impl PhysicalCallRequest {
    pub fn request(&self) -> FunctionBindingRequest<'_, ConstantReference> {
        FunctionBindingRequest {
            arguments: &self.arguments,
            logical_argument_count: self.logical_argument_count,
            expected_result_type: self.expected_result_type.as_ref(),
        }
    }
}
