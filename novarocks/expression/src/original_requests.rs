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

//! The compiler delegates its original request materialization to the shared
//! expression-contract author; only its nominal error projection belongs here.
use crate::compiler::ExprCompileError;
pub(crate) use novarocks_expression_contract::MaterializedCallRequest;
use novarocks_functions::MAX_CALL_EFFECT_ARGUMENTS;

use novarocks_constant_contract::ConstantPools;
use novarocks_expression_contract::PhysicalCallRequest;
use novarocks_type_contract::CompileCheckpoints;

pub(crate) fn materialize_call_request_observed<'a>(
    source: &'a PhysicalCallRequest,
    pools: &ConstantPools,
    work: &mut CompileCheckpoints<'_>,
) -> Result<MaterializedCallRequest<'a>, ExprCompileError> {
    novarocks_expression_contract::materialize_call_request_observed(
        source,
        pools,
        work,
        MAX_CALL_EFFECT_ARGUMENTS,
    )
    .map_err(|error| match error {
        novarocks_expression_contract::CallRequestMaterializationError::Control(cause) => {
            ExprCompileError::Control(cause)
        }
        novarocks_expression_contract::CallRequestMaterializationError::Reference(cause) => {
            cause.into()
        }
        novarocks_expression_contract::CallRequestMaterializationError::Invalid(cause) => {
            ExprCompileError::Invalid(cause)
        }
    })
}

pub(crate) fn materialize_call_request_created<
    'a,
    H,
    F: novarocks_type_contract::ValueSchemaCreationFactory<H>,
>(
    source: &'a PhysicalCallRequest,
    pools: &ConstantPools,
    work: &mut CompileCheckpoints<'_>,
    creation: crate::compiler::CallCreation<'_, F>,
) -> Result<MaterializedCallRequest<'a>, crate::compiler::ExprCompileFailure<H>> {
    use crate::compiler::ExprCompileFailure as Failure;
    use novarocks_expression_contract::{
        CallRequestMaterializationError as Owner,
        CallRequestMaterializationFailure as Materialization,
    };
    match creation {
        Some((factory, _)) => {
            novarocks_expression_contract::materialize_call_request_with_factory_in(
                source,
                pools,
                factory,
                work,
                MAX_CALL_EFFECT_ARGUMENTS,
            )
            .map_err(|error| match error {
                Materialization::Control(cause) => Failure::Control(cause),
                Materialization::Creator(cause) => Failure::Creator(cause),
                Materialization::Owner(Owner::Control(cause)) => Failure::Control(cause),
                Materialization::Owner(Owner::Reference(cause)) => cause.into(),
                Materialization::Owner(Owner::Invalid(cause)) => {
                    ExprCompileError::Invalid(cause).into()
                }
            })
        }
        None => materialize_call_request_observed(source, pools, work).map_err(Into::into),
    }
}
