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

use std::{collections::BTreeSet, fmt};

use novarocks_type_contract::{OperatorFunction, PureCompileControl};

use crate::{
    FunctionId, FunctionKind, FunctionOverloadId, FunctionSpecializationFailure,
    FunctionVisibility, PureEngineFunctionCatalog, PureKernelAbi,
};

#[derive(Debug)]
pub enum OperatorFunctionCatalogError {
    DuplicateIdentity(FunctionId),
    MissingDefinition(FunctionId),
    InvalidDefinition(FunctionId),
    InvalidAbi(FunctionOverloadId),
    OverloadLookup(FunctionSpecializationFailure),
}

impl fmt::Display for OperatorFunctionCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateIdentity(id) => {
                write!(
                    formatter,
                    "operator function identity `{}` is not unique",
                    id.as_str()
                )
            }
            Self::MissingDefinition(id) => {
                write!(
                    formatter,
                    "operator function `{}` is missing from the pure catalog",
                    id.as_str()
                )
            }
            Self::InvalidDefinition(id) => {
                write!(
                    formatter,
                    "operator function `{}` must have its exact hidden scalar definition",
                    id.as_str()
                )
            }
            Self::InvalidAbi(overload) => {
                write!(
                    formatter,
                    "operator overload `{}` must have a ScalarV1 implementation",
                    overload.as_str()
                )
            }
            Self::OverloadLookup(cause) => {
                write!(formatter, "operator overload lookup failed: {cause}")
            }
        }
    }
}

impl std::error::Error for OperatorFunctionCatalogError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::OverloadLookup(cause) => Some(cause),
            _ => None,
        }
    }
}

/// Require the complete built-in operator set at the composition roots.
pub fn validate_installed_operator_functions(
    catalog: &PureEngineFunctionCatalog,
    control: &dyn PureCompileControl,
) -> Result<(), OperatorFunctionCatalogError> {
    let metadata = catalog.metadata();
    let mut seen = BTreeSet::new();
    for operator in OperatorFunction::ALL {
        let id = operator.function_id();
        if !seen.insert(id.clone()) {
            return Err(OperatorFunctionCatalogError::DuplicateIdentity(id));
        }
        let definition = metadata
            .definition_by_id(&id)
            .ok_or_else(|| OperatorFunctionCatalogError::MissingDefinition(id.clone()))?;
        if definition.kind() != FunctionKind::Scalar
            || definition.visibility() != FunctionVisibility::Hidden
            || definition.canonical_name() != operator.catalog_name()
        {
            return Err(OperatorFunctionCatalogError::InvalidDefinition(id));
        }
        let declaration = definition
            .binding_declaration()
            .ok_or_else(|| OperatorFunctionCatalogError::InvalidDefinition(id.clone()))?;
        for overload in declaration.overloads() {
            let actual = metadata
                .pure_overload_declaration_observed(
                    &id,
                    FunctionKind::Scalar,
                    &overload.identity,
                    control,
                )
                .map_err(OperatorFunctionCatalogError::OverloadLookup)?;
            if actual.implementation().abi != PureKernelAbi::ScalarV1 {
                return Err(OperatorFunctionCatalogError::InvalidAbi(
                    overload.identity.clone(),
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EngineFunctionCatalogBuilder, InstalledPureKernel};

    #[test]
    fn visible_operator_is_not_accepted_as_an_installed_operator() {
        let builtin = crate::builtin::catalogue::builtin_engine_function_catalog();
        let id = OperatorFunction::Negation.function_id();
        let control = crate::binding_test_control();
        let mut definition = builtin.definition_by_id(&id).unwrap().clone();
        let installed: Vec<_> = definition
            .binding_declaration()
            .unwrap()
            .overloads()
            .iter()
            .map(|overload| {
                let actual = builtin
                    .pure_overload_declaration_observed(
                        &id,
                        FunctionKind::Scalar,
                        &overload.identity,
                        control,
                    )
                    .unwrap();
                InstalledPureKernel {
                    function: id.clone(),
                    kind: FunctionKind::Scalar,
                    implementation: actual.implementation().clone(),
                    aggregate_state_format: None,
                }
            })
            .collect();
        definition.visibility = FunctionVisibility::Public;
        let mut builder = EngineFunctionCatalogBuilder::new();
        builder.register(definition).unwrap();
        let catalog = builder.seal_pure(installed).unwrap();
        assert!(matches!(
            validate_installed_operator_functions(&catalog, control),
            Err(OperatorFunctionCatalogError::InvalidDefinition(actual)) if actual == id
        ));
    }
}
