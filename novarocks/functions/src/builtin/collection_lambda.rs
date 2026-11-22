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
//! Exact selected adapters for ONE original constructor and map lookup cores.

//! Public collection lambda signatures awaiting body typing and execution.

use std::sync::Arc;

use arrow_schema::DataType;
use novarocks_type_contract::{
    CompilePhase, FunctionId, FunctionIntrinsicRowError, FunctionOverloadId, FunctionValueType,
    PureCompileControl, ValueLogicalType,
};

use crate::{
    EngineFunctionCatalogBuilder, FunctionArgument, FunctionArgumentEvaluation,
    FunctionBindingDeclaration, FunctionBindingError, FunctionBindingRequest,
    FunctionBindingResolver, FunctionBindingSelection, FunctionCatalogError, FunctionDefinition,
    FunctionFailureBehavior, FunctionKind, FunctionOverloadDeclaration, FunctionSemantics,
    FunctionVisibility, FunctionVolatility,
};

#[derive(Clone, Copy)]
pub struct CollectionLambdaSignature {
    name: &'static str,
    map: bool,
    arity: usize,
    arguments: &'static str,
    result: &'static str,
}

impl CollectionLambdaSignature {
    pub const fn arity(self) -> usize {
        self.arity
    }

    pub fn accepts_collection(self, source: &FunctionValueType) -> bool {
        source.logical_type() == ValueLogicalType::Physical
            && if self.map {
                matches!(source.data_type(), DataType::Map(_, _) | DataType::Null)
            } else {
                matches!(source.data_type(), DataType::List(_) | DataType::Null)
            }
    }

    pub fn overload(self) -> FunctionOverloadId {
        FunctionOverloadId::try_new(format!("builtin.scalar/{}/lambda-v1", self.name))
            .expect("valid collection lambda overload identity")
    }

    pub(super) fn declaration(self) -> FunctionOverloadDeclaration {
        FunctionOverloadDeclaration {
            identity: self.overload(),
            argument_pattern: self.arguments.into(),
            result_pattern: self.result.into(),
            semantics: FunctionSemantics {
                volatility: FunctionVolatility::Immutable,
                argument_evaluation: FunctionArgumentEvaluation::Eager,
                failure_behavior: FunctionFailureBehavior::Propagate,
                intrinsic_row_error: FunctionIntrinsicRowError::MayRaise,
            },
            effects: None,
            aggregate: None,
        }
    }
}

pub fn signature(name: &str) -> Option<CollectionLambdaSignature> {
    let (name, map, arity, arguments, result) = match name {
        "transform" => (
            "transform",
            false,
            1,
            "(array(T),function(T,U))",
            "array(U)",
        ),
        "filter" => (
            "filter",
            false,
            1,
            "(array(T),function(T,boolean))",
            "array(T)",
        ),
        "array_sort" => (
            "array_sort",
            false,
            2,
            "(array(T),function(T,T,integer))",
            "array(T)",
        ),
        "any_match" => (
            "any_match",
            false,
            1,
            "(array(T),function(T,boolean))",
            "boolean",
        ),
        "all_match" => (
            "all_match",
            false,
            1,
            "(array(T),function(T,boolean))",
            "boolean",
        ),
        "map_filter" => (
            "map_filter",
            true,
            2,
            "(map(K,V),function(K,V,boolean))",
            "map(K,V)",
        ),
        "transform_keys" => (
            "transform_keys",
            true,
            2,
            "(map(K1,V),function(K1,V,K2))",
            "map(K2,V)",
        ),
        "transform_values" => (
            "transform_values",
            true,
            2,
            "(map(K,V1),function(K,V1,V2))",
            "map(K,V2)",
        ),
        _ => return None,
    };
    Some(CollectionLambdaSignature {
        name,
        map,
        arity,
        arguments,
        result,
    })
}

pub(super) fn register(
    builder: &mut EngineFunctionCatalogBuilder,
) -> Result<(), FunctionCatalogError> {
    for name in [
        "transform",
        "filter",
        "map_filter",
        "transform_keys",
        "transform_values",
        "any_match",
        "all_match",
    ] {
        let signature = signature(name).expect("registered collection lambda signature");
        let function = FunctionId::try_new(format!("builtin.scalar/{name}/v1"))
            .expect("valid collection lambda function identity");
        let declaration = FunctionBindingDeclaration::try_new(
            function,
            FunctionKind::Scalar,
            [signature.declaration()],
        )
        .map_err(|error| FunctionCatalogError::InvalidStableIdentity {
            subject: "collection lambda binding declaration",
            value: error.to_string().into(),
        })?;
        builder.register(FunctionDefinition::try_new_bound(
            name,
            FunctionVisibility::Public,
            declaration,
            Arc::new(signature),
        )?)?;
    }
    Ok(())
}

impl FunctionBindingResolver for CollectionLambdaSignature {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        control.checkpoint(CompilePhase::FunctionSpecialization, 1)?;
        if let [
            FunctionArgument::Value { value_type, .. },
            FunctionArgument::Lambda {
                parameter_types, ..
            },
        ] = request.arguments
            && request.logical_argument_count == 2
            && self.accepts_collection(value_type)
            && parameter_types.len() == self.arity
        {
            Err(FunctionBindingError::UnavailableImplementation(
                self.overload(),
            ))
        } else {
            Err(FunctionBindingError::NoMatchingOverload)
        }
    }

    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        if selected.overload != self.overload() {
            return Err(FunctionBindingError::UnknownOverload(
                selected.overload.clone(),
            ));
        }
        self.resolve(request, control).map(|_| ())
    }
}
