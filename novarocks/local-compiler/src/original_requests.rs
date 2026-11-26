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
//! physical-plan author; only its nominal error projection belongs here.
use crate::expressions::ExpressionLoweringError;
use novarocks_expression_contract::PhysicalCallRequest;
#[cfg(test)]
use novarocks_functions::ConstantValidation;
#[cfg(test)]
use novarocks_functions::FunctionArgument;
use novarocks_functions::MAX_CALL_EFFECT_ARGUMENTS;
use novarocks_physical_plan::ConstantPools;
pub(crate) use novarocks_physical_plan::MaterializedCallRequest;
#[cfg(test)]
use novarocks_physical_plan::StaticFunctionArgument;
use novarocks_type_contract::CompileCheckpoints;
#[cfg(test)]
use novarocks_type_contract::{CompileControlError, FunctionValueType};

pub(crate) fn materialize_call_request_observed<'a>(
    source: &'a PhysicalCallRequest,
    pools: &ConstantPools,
    work: &mut CompileCheckpoints<'_>,
) -> Result<MaterializedCallRequest<'a>, ExpressionLoweringError> {
    novarocks_physical_plan::materialize_call_request_observed(
        source,
        pools,
        work,
        MAX_CALL_EFFECT_ARGUMENTS,
    )
    .map_err(|error| match error {
        novarocks_physical_plan::CallRequestMaterializationError::Control(cause) => {
            ExpressionLoweringError::Control(cause)
        }
        novarocks_physical_plan::CallRequestMaterializationError::Reference(cause) => cause.into(),
        novarocks_physical_plan::CallRequestMaterializationError::Invalid(cause) => {
            ExpressionLoweringError::Invalid(cause)
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
    creation: crate::expressions::CallCreation<'_, F>,
) -> Result<MaterializedCallRequest<'a>, crate::expressions::ExpressionCreationFailure<H>> {
    use crate::expressions::ExpressionCreationFailure as Failure;
    use novarocks_physical_plan::{
        CallRequestMaterializationError as Owner,
        CallRequestMaterializationFailure as Materialization,
    };
    match creation {
        Some((factory, _)) => novarocks_physical_plan::materialize_call_request_with_factory_in(
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
                ExpressionLoweringError::Invalid(cause).into()
            }
        }),
        None => materialize_call_request_observed(source, pools, work).map_err(Into::into),
    }
}

#[cfg(test)]
#[path = "original_requests/tests.rs"]
mod tests;
