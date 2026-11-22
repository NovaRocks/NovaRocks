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

//! SQL catalogue integration tests for the Functions-owned dispositions.

#[cfg(test)]
mod tests {
    use novarocks_functions::{
        FunctionArgument, FunctionBindingError, FunctionBindingRequest, FunctionKind,
        FunctionValueType,
        builtin::{
            collection_lambda,
            intrinsic::{BuiltinDisposition, builtin_disposition, builtin_dispositions},
        },
    };
    use std::collections::BTreeSet;

    #[test]
    fn every_declared_builtin_has_one_explicit_disposition_and_unknown_has_none() {
        let catalog = super::super::build_builtin_engine_function_catalog().unwrap();
        let collection_lambdas = catalog
            .definitions()
            .iter()
            .filter(|definition| definition.kind() == FunctionKind::Scalar)
            .filter_map(|definition| {
                let signature = collection_lambda::signature(definition.canonical_name())?;
                let declaration = definition.binding_declaration()?;
                declaration
                    .overloads()
                    .iter()
                    .any(|overload| overload.identity == signature.overload())
                    .then_some(definition.canonical_name().to_owned())
            });
        let mut declared = novarocks_functions::builtin::registry::builtin_scalar_declarations()
            .into_iter()
            .map(|(name, _)| name)
            .chain(
                novarocks_functions::builtin::catalogue::dynamic_scalar_names()
                    .iter()
                    .map(|name| (*name).to_owned()),
            )
            .chain(collection_lambdas)
            .collect::<BTreeSet<_>>();
        // The retired registry spelling still has a SQL diagnostic disposition.
        assert!(declared.insert("negative".to_owned()));
        assert_eq!(
            builtin_disposition("negative"),
            Some(BuiltinDisposition::LoweredOnly)
        );
        let admitted = builtin_dispositions()
            .iter()
            .map(|(name, _)| (*name).to_owned())
            .collect::<BTreeSet<_>>();
        assert_eq!(builtin_dispositions().len(), declared.len());
        assert_eq!(admitted.len(), builtin_dispositions().len());
        assert_eq!(declared, admitted);
        assert!(
            builtin_dispositions()
                .windows(2)
                .all(|pair| pair[0].0 < pair[1].0)
        );
        assert_eq!(builtin_disposition("not_an_advertised_builtin"), None);
    }

    #[test]
    fn unavailable_and_lowered_builtins_have_no_selected_scalar_binding() {
        use arrow::datatypes::DataType;
        let catalog = super::super::build_builtin_engine_function_catalog().unwrap();
        for (name, disposition) in builtin_dispositions() {
            if matches!(
                disposition,
                BuiltinDisposition::Unavailable
                    | BuiltinDisposition::LoweredOnly
                    | BuiltinDisposition::AggregateBoundary
                    | BuiltinDisposition::WindowBoundary
            ) {
                let control = crate::compiler::SqlCompileControl::unbounded();
                let empty = catalog.resolve_bound_user(
                    name,
                    FunctionKind::Scalar,
                    FunctionBindingRequest {
                        expected_result_type: None,
                        arguments: &[],
                        logical_argument_count: 0,
                    },
                    &control,
                );
                if let Some(signature) = collection_lambda::signature(name) {
                    assert_eq!(
                        empty,
                        Err(FunctionBindingError::NoMatchingOverload),
                        "{name}"
                    );
                    let value = FunctionValueType::new(DataType::Null, true);
                    let arguments = [
                        FunctionArgument::Value {
                            value_type: value.clone(),
                            constant: None,
                        },
                        FunctionArgument::Lambda {
                            parameter_types: vec![value; signature.arity()].into(),
                            result_type: FunctionValueType::new(DataType::Boolean, true),
                        },
                    ];
                    let binding = catalog.resolve_bound_user(
                        name,
                        FunctionKind::Scalar,
                        FunctionBindingRequest {
                            expected_result_type: None,
                            arguments: &arguments,
                            logical_argument_count: arguments.len(),
                        },
                        &control,
                    );
                    assert_eq!(
                        binding,
                        Err(FunctionBindingError::UnavailableImplementation(
                            signature.overload()
                        )),
                        "{name}"
                    );
                } else {
                    assert_eq!(empty, Err(FunctionBindingError::UnknownFunction), "{name}");
                }
            }
        }
        assert!(
            catalog
                .resolve_bound_user(
                    "not_an_advertised_builtin",
                    FunctionKind::Scalar,
                    FunctionBindingRequest {
                        expected_result_type: None,
                        arguments: &[],
                        logical_argument_count: 0
                    },
                    &crate::compiler::SqlCompileControl::unbounded()
                )
                .is_err()
        );
    }
}
