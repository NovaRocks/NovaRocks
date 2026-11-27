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

//! Optional construction in the original parametric aggregate binding author.
use super::{
    binding_control,
    scalar_construction::{borrow_creator, with_binding_creation},
};
use crate::binding::{ParametricAggregateBindingResolver, invalid_binding as invalid};
use crate::{
    AggregateOverloadIdentity, AggregateUpdateSignatureRecipe, FunctionArgument,
    FunctionArgumentType, FunctionBindingCreationFailure, FunctionBindingError,
    FunctionBindingRequest, FunctionBindingSelection, FunctionOverloadId, FunctionResultType,
    ResolvedAggregateSignature,
};
use novarocks_type_contract::{CompileCheckpoints, PureCompileControl, ValueSchemaCreationScope};

impl ParametricAggregateBindingResolver {
    pub(crate) fn argument_types(
        &self,
        request: FunctionBindingRequest<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Vec<arrow_schema::DataType>, FunctionBindingError> {
        let values = request
            .arguments
            .iter()
            .map(|argument| {
                work.step()?;
                match argument {
                    FunctionArgument::Value { value_type, .. } => Ok(value_type.clone()),
                    FunctionArgument::Lambda { .. } | FunctionArgument::CandidateGroup { .. } => {
                        Err(FunctionBindingError::NoMatchingOverload)
                    }
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        work.flush()?;
        let validation = self
            .aggregate_resolver
            .validate_value_arguments(&values)
            .map_err(FunctionBindingError::from);
        work.step()?;
        validation?;
        Ok(values
            .into_iter()
            .map(|value| value.data_type().clone())
            .collect())
    }

    fn scoped_signature<H>(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        creation: &mut dyn ValueSchemaCreationScope<H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<ResolvedAggregateSignature, FunctionBindingCreationFailure<H>> {
        let mut values = Vec::new();
        for argument in request.arguments {
            work.step()?;
            let value = match argument {
                FunctionArgument::Value { value_type, .. } => value_type.clone(),
                FunctionArgument::Lambda { .. } | FunctionArgument::CandidateGroup { .. } => {
                    return Err(FunctionBindingError::NoMatchingOverload.into());
                }
            };
            // Result collection obtains the next valid item before growing the
            // original Vec. Quote that same push, rather than charging an
            // allocation before an immediately rejected first Lambda.
            with_binding_creation(Some(&mut *creation), work, |c| c.push(&values))?;
            values.push(value);
        }
        work.flush()?;
        let validation = self
            .aggregate_resolver
            .validate_value_arguments(&values)
            .map_err(FunctionBindingError::from);
        work.step()?;
        validation?;
        // Preserve the original selected-identity boundary before the family
        // call. The borrowed recipe avoids allocating a copied carrier tree
        // before its original family author admits that tree.
        work.step()?;
        work.flush()?;
        let recipe = self
            .aggregate_resolver
            .borrow_update_signature(overload.as_str(), &values)
            .map_err(FunctionBindingError::from);
        work.step()?;
        let recipe = recipe?.ok_or(FunctionBindingCreationFailure::UnconvertedResolverOwner)?;
        materialize_scoped_signature(recipe, creation, work)
    }

    fn select_exact_with_creation<H>(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        mut creation: Option<&mut dyn ValueSchemaCreationScope<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingCreationFailure<H>> {
        let (argument_types, resolved) = match borrow_creator(&mut creation) {
            None => {
                let argument_types = self.argument_types(request, work)?;
                let selected_overload = AggregateOverloadIdentity::try_new(overload.as_str())
                    .map_err(|error| invalid(&error.to_string()));
                work.step()?;
                let selected_overload = selected_overload?;
                work.flush()?;
                let resolved = self
                    .aggregate_resolver
                    .resolve_update_signature(&selected_overload, &argument_types)
                    .map_err(FunctionBindingError::from);
                work.step()?;
                (Some(argument_types), resolved?)
            }
            Some(creation) => (
                None,
                self.scoped_signature(overload, request, creation, work)?,
            ),
        };
        let actual_len = argument_types
            .as_ref()
            .map_or(request.arguments.len(), Vec::len);
        let same_arity = resolved.argument_types.len() == actual_len;
        work.step()?;
        if !same_arity {
            return Err(
                invalid("aggregate update author returned a different channel count").into(),
            );
        }
        for (index, returned) in resolved.argument_types.iter().enumerate() {
            let actual = match &argument_types {
                Some(arguments) => &arguments[index],
                None => match &request.arguments[index] {
                    FunctionArgument::Value { value_type, .. } => value_type.data_type(),
                    FunctionArgument::Lambda { .. } | FunctionArgument::CandidateGroup { .. } => {
                        unreachable!("checked aggregate value channels")
                    }
                },
            };
            let exact =
                novarocks_type_contract::arrow_data_types_exact_observed(actual, returned, || {
                    work.step().map_err(FunctionBindingError::from)
                })?;
            if !exact {
                return Err(
                    invalid("aggregate update author returned a different channel type").into(),
                );
            }
        }
        for _ in request.arguments {
            work.step()?;
        }
        work.flush()?;
        let selection =
            self.selection_with_creation(request, &resolved, borrow_creator(&mut creation), work);
        if matches!(
            &selection,
            Err(FunctionBindingCreationFailure::Creator(_)
                | FunctionBindingCreationFailure::Resource(_)
                | FunctionBindingCreationFailure::Binding(FunctionBindingError::Control(_)))
        ) {
            return selection;
        }
        work.step()?;
        selection
    }

    pub(crate) fn select_exact_observed(
        &self,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.select_exact_with_creation::<std::convert::Infallible>(overload, request, None, work)
            .map_err(FunctionBindingCreationFailure::into_pure)
    }

    pub(crate) fn validate_selected_with_creator_in<H>(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        creation: &mut dyn ValueSchemaCreationScope<H>,
        control: &dyn PureCompileControl,
    ) -> Result<(), FunctionBindingCreationFailure<H>> {
        binding_control::created_scope(control, |work| {
            let expected =
                self.select_exact_with_creation(&selected.overload, request, Some(creation), work)?;
            if binding_control::same_selection(&expected, selected, work)? {
                Ok(())
            } else {
                Err(invalid("selected aggregate overload differs from its typed contract").into())
            }
        })
    }

    fn selection_with_creation<H>(
        &self,
        request: FunctionBindingRequest<'_>,
        resolved: &ResolvedAggregateSignature,
        mut creation: Option<&mut dyn ValueSchemaCreationScope<H>>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingCreationFailure<H>> {
        work.flush()?;
        let state_argument_contract = self
            .aggregate_resolver
            .state_argument_contract(&resolved.overload)
            .map_err(FunctionBindingError::from);
        if let Err(FunctionBindingError::Control(cause)) = state_argument_contract {
            return Err(FunctionBindingError::Control(cause).into());
        }
        work.step()?;
        let state_argument_contract = state_argument_contract?;
        work.flush()?;
        let nullable = self.aggregate_resolver.produces_null();
        let overload = match borrow_creator(&mut creation) {
            None => FunctionOverloadId::try_new(resolved.overload.as_str())
                .map_err(|error| invalid(&error.to_string()))?,
            Some(_) => {
                let original = self
                    .declaration
                    .overloads()
                    .iter()
                    .find(|original| original.identity.as_str() == resolved.overload.as_str())
                    .ok_or_else(|| {
                        invalid("aggregate update author returned an undeclared overload")
                    })?;
                // This is the immutable registration author, not the selected
                // candidate. Its cheap clone performs no new label allocation.
                original.identity.clone()
            }
        };
        with_binding_creation(borrow_creator(&mut creation), work, |c| {
            c.collected_vec::<FunctionArgumentType>(request.arguments.len())
        })?;
        let arguments = request
            .arguments
            .iter()
            .map(FunctionArgument::argument_type)
            .collect::<Vec<_>>();
        let argument_types = with_binding_creation(borrow_creator(&mut creation), work, |c| {
            c.publish_arguments(arguments)
        })?;
        let output = with_binding_creation(borrow_creator(&mut creation), work, |c| {
            let carrier = c.cloned_data_type(&resolved.output_type)?;
            c.value(
                carrier,
                nullable,
                novarocks_type_contract::ValueLogicalType::Physical,
            )
        })?;
        let intermediate_type = with_binding_creation(borrow_creator(&mut creation), work, |c| {
            let carrier = c.cloned_data_type(&resolved.intermediate_type)?;
            c.value(
                carrier,
                nullable,
                novarocks_type_contract::ValueLogicalType::Physical,
            )
        })?;
        Ok(FunctionBindingSelection {
            overload,
            argument_types,
            result_type: FunctionResultType::Scalar(output),
            aggregate: Some(crate::AggregateBindingSelection {
                state_argument_contract,
                intermediate_type,
                state_format: resolved.state_format.clone(),
            }),
        })
    }

    pub(crate) fn selection(
        &self,
        request: FunctionBindingRequest<'_>,
        resolved: &ResolvedAggregateSignature,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.selection_with_creation::<std::convert::Infallible>(request, resolved, None, work)
            .map_err(FunctionBindingCreationFailure::into_pure)
    }
}

impl AggregateUpdateSignatureRecipe<'_> {
    /// Pure construction delegates the same original signature materializer.
    /// This does not implement a resource or cancellation authority.
    pub fn materialize_pure(
        self,
    ) -> Result<ResolvedAggregateSignature, crate::FunctionResolutionError> {
        let mut construction = super::scalar_construction::ScalarConstruction::new(
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
            None,
        );
        materialize_signature::<std::convert::Infallible>(self, &mut construction).map_err(
            |error| match error.into_pure() {
                FunctionBindingError::Control(cause) => {
                    crate::FunctionResolutionError::Control(cause)
                }
                FunctionBindingError::InvalidBinding(message) => {
                    crate::FunctionResolutionError::BadSignature(message.into())
                }
                other => crate::FunctionResolutionError::BadSignature(other.to_string()),
            },
        )
    }
}

fn materialize_signature<E>(
    recipe: AggregateUpdateSignatureRecipe<'_>,
    construction: &mut super::scalar_construction::ScalarConstruction<'_, E>,
) -> Result<ResolvedAggregateSignature, super::scalar_construction::OwnerFailure<E>> {
    // The original clone author handles private Dictionary Box roots. Field,
    // Fields, UnionFields and timezone handles keep their original shared Arc;
    // no family-specific clone grammar or guessed backing fee is introduced.
    let observed = construction.observer().is_some();
    let overload = if observed {
        construction.array::<u8>(recipe.original.identity.as_str().len())?;
        AggregateOverloadIdentity::try_new(recipe.original.identity.as_str())
    } else {
        // Keep pure construction's temporary owned label and original stable
        // identity constructor; no scope/observer is fabricated for that path.
        AggregateOverloadIdentity::try_new(recipe.original.identity.as_str())
    }
    .map_err(|error| invalid(&error.to_string()))?;
    construction.array::<arrow_schema::DataType>(recipe.argument_types.len())?;
    let argument_types = if observed {
        let mut arguments = Vec::with_capacity(recipe.argument_types.len());
        for ty in recipe.argument_types {
            arguments.push(construction.cloned_data_type(ty)?);
        }
        arguments
    } else {
        recipe.argument_types.to_vec()
    };
    let intermediate_type = construction.cloned_data_type(recipe.intermediate_type)?;
    let output_type = construction.cloned_data_type(recipe.output_type)?;
    let state_format = if observed {
        recipe.original.state_format.clone()
    } else {
        crate::AggregateStateFormatIdentity::try_new(recipe.original.state_format.as_str())
            .map_err(|error| invalid(&error.to_string()))?
    };
    Ok(ResolvedAggregateSignature {
        overload,
        argument_types,
        intermediate_type,
        output_type,
        state_format,
    })
}

fn materialize_scoped_signature<H>(
    recipe: AggregateUpdateSignatureRecipe<'_>,
    creation: &mut dyn ValueSchemaCreationScope<H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ResolvedAggregateSignature, FunctionBindingCreationFailure<H>> {
    with_binding_creation(Some(creation), work, |construction| {
        materialize_signature(recipe, construction).map_err(|error| match error {
            super::scalar_construction::OwnerFailure::Binding(error) => {
                novarocks_type_contract::ValueConstructionFailure::Observer(error.into())
            }
            super::scalar_construction::OwnerFailure::Construction(error) => error,
        })
    })
}

#[cfg(test)]
mod clone_tests {
    use super::*;
    use arrow_schema::DataType;
    use novarocks_type_contract::{CompileControlError, CompilePhase, ValueTypeStorageLifetime};
    use std::{
        alloc::Layout,
        sync::{Arc, Mutex},
    };

    #[derive(Default)]
    struct Control(Mutex<Vec<(CompilePhase, u32)>>);
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert!(units <= 256);
            self.0.lock().unwrap().push((phase, units));
            Ok(())
        }
    }
    struct Storage;
    impl novarocks_type_contract::ValueTypeStorageHold for Storage {}
    struct Scope {
        requests: Vec<Layout>,
        refuse: Option<usize>,
        cause: Arc<()>,
        storage: Arc<Storage>,
    }
    impl ValueSchemaCreationScope<Arc<()>> for Scope {
        fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
            ValueTypeStorageLifetime::held(self.storage.clone())
        }
        fn request_layout(&mut self, layout: Layout) -> Result<(), Arc<()>> {
            self.requests.push(layout);
            let finite = self
                .requests
                .iter()
                .try_fold(0usize, |sum, layout| sum.checked_add(layout.size()))
                .is_some_and(|sum| sum <= 8192);
            if self.refuse == Some(self.requests.len()) || !finite {
                Err(self.cause.clone())
            } else {
                Ok(())
            }
        }
        fn observe_opaque_upper_bound(&mut self, _bytes: usize) -> Result<(), Arc<()>> {
            panic!("the original clone author only requests exact Dictionary Boxes")
        }
    }

    #[test]
    fn generic_signature_clone_quotes_dictionary_boxes_before_copy_and_preserves_first_h() {
        // This validates the shared clone constructor, not Theta kernel support
        // for Dictionary. The original closed Theta refusal tests remain intact.
        let ty = DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Utf8),
            )),
        );
        let args = [ty.clone()];
        let declaration = crate::AggregateOverloadDeclaration::try_new(
            "test/signature-clone/v1",
            "dictionary",
            "dictionary",
            "dictionary",
            "test/signature-clone/state-v1",
        )
        .unwrap();
        let recipe = AggregateUpdateSignatureRecipe {
            original: &declaration,
            argument_types: &args,
            intermediate_type: &ty,
            output_type: &ty,
        };
        let mut scope = Scope {
            requests: Vec::new(),
            refuse: None,
            cause: Arc::new(()),
            storage: Arc::new(Storage),
        };
        let control = Control::default();
        let mut work =
            CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
        let signature = materialize_scoped_signature(recipe, &mut scope, &mut work).unwrap();
        work.finish().unwrap();
        let mut expected = vec![
            Layout::array::<u8>(declaration.identity.as_str().len()).unwrap(),
            Layout::array::<DataType>(args.len()).unwrap(),
        ];
        // Each original clone visits four Dictionary Box roots. Arguments,
        // intermediate and output are three distinct actual clone operations.
        expected.extend(std::iter::repeat_n(Layout::new::<DataType>(), 12));
        assert_eq!(scope.requests, expected);
        assert_eq!(signature.argument_types, args);
        assert_eq!(signature.intermediate_type, ty);
        assert_eq!(signature.output_type, ty);
        let DataType::Dictionary(original_key, _) = &args[0] else {
            unreachable!()
        };
        let DataType::Dictionary(copied_key, _) = &signature.argument_types[0] else {
            unreachable!()
        };
        assert!(!std::ptr::eq(&**original_key, &**copied_key));
        let pure = recipe.materialize_pure().unwrap();
        assert_eq!(signature, pure);
        let calls = control.0.lock().unwrap().clone();
        for at in 1..=expected.len() {
            let first = Arc::new(());
            let mut scope = Scope {
                requests: Vec::new(),
                refuse: Some(at),
                cause: first.clone(),
                storage: Arc::new(Storage),
            };
            let control = Control::default();
            let mut work =
                CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization)
                    .unwrap();
            match materialize_scoped_signature(recipe, &mut scope, &mut work) {
                Err(FunctionBindingCreationFailure::Creator(actual)) => {
                    assert!(Arc::ptr_eq(&actual, &first))
                }
                other => panic!("clone lost original H: {other:?}"),
            }
            // Refusal returns before the original clone and without a completed
            // footer. Earlier actual temporary allocations may already exist.
            assert_eq!(scope.requests, expected[..at]);
            let observed = control.0.lock().unwrap().clone();
            assert_eq!(observed, calls[..observed.len()]);
            assert!(observed.len() < calls.len());
        }
    }
}
