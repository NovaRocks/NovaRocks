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

//! Original-request control for the exact builtin binding path.
use crate::{
    FunctionArgument, FunctionArgumentType, FunctionBindingError, FunctionBindingRequest,
    FunctionBindingSelection, FunctionResultType, FunctionValueType, KernelFailure,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};

pub(super) fn type_error(error: KernelFailure) -> FunctionBindingError {
    match error {
        KernelFailure::CompileControl(cause) => cause.into(),
        KernelFailure::Cancelled => CompileControlError::Cancelled.into(),
        KernelFailure::DeadlineExceeded => CompileControlError::DeadlineExceeded.into(),
        KernelFailure::ResourceExhausted => CompileControlError::ResourceExhausted.into(),
        other => FunctionBindingError::InvalidBinding(other.to_string().into()),
    }
}

pub(super) fn scope<T>(
    control: &dyn PureCompileControl,
    body: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, FunctionBindingError>,
) -> Result<T, FunctionBindingError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = body(&mut work);
    // Preserve an originating typed control failure. Ordinary refusals and
    // successful outputs still observe their completed tail before return.
    if result
        .as_ref()
        .err()
        .is_some_and(|error| error.control_error().is_some())
    {
        return result;
    }
    work.finish()?;
    result
}

pub(super) fn created_scope<T, H>(
    control: &dyn PureCompileControl,
    body: impl FnOnce(
        &mut CompileCheckpoints<'_>,
    ) -> Result<T, crate::FunctionBindingCreationFailure<H>>,
) -> Result<T, crate::FunctionBindingCreationFailure<H>> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = body(&mut work);
    if matches!(
        &result,
        Err(crate::FunctionBindingCreationFailure::Creator(_)
            | crate::FunctionBindingCreationFailure::Resource(_)
            | crate::FunctionBindingCreationFailure::Binding(FunctionBindingError::Control(_)))
    ) {
        return result;
    }
    work.finish()?;
    result
}
pub(super) fn value_type(
    value: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FunctionBindingError> {
    crate::kernel_input::validate_type_observed(value, work).map_err(type_error)
}

pub(super) fn request_types(
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FunctionBindingError> {
    if request.logical_argument_count > request.arguments.len() {
        return Err(FunctionBindingError::NoMatchingOverload);
    }
    for argument in request.arguments {
        work.step()?;
        match argument {
            FunctionArgument::Value { value_type: ty, .. } => value_type(ty, work)?,
            FunctionArgument::CandidateGroup {
                element_type,
                members,
            } => {
                value_type(element_type, work)?;
                for member in members {
                    work.step()?;
                    value_type(&member.value_type, work)?;
                }
            }
            FunctionArgument::Lambda {
                parameter_types,
                result_type,
            } => {
                for parameter in parameter_types {
                    work.step()?;
                    value_type(parameter, work)?;
                }
                value_type(result_type, work)?;
            }
        }
    }
    if let Some(expected) = request.expected_result_type {
        value_type(expected, work)?;
    }
    Ok(())
}

/// A closed, constant-size view of complete scalar argument declarations.
/// The request arm is published only after the original ordered Lambda gate.
#[derive(Clone, Copy)]
pub(super) struct ScalarTypes<'a>(ScalarTypeSource<'a>);
#[derive(Clone, Copy)]
enum ScalarTypeSource<'a> {
    Values(&'a [FunctionValueType]),
    Arguments(&'a [FunctionArgument]),
}
impl<'a> ScalarTypes<'a> {
    pub(super) fn from_values(values: &'a [FunctionValueType]) -> Self {
        Self(ScalarTypeSource::Values(values))
    }
    pub(super) fn len(self) -> usize {
        match self.0 {
            ScalarTypeSource::Values(values) => values.len(),
            ScalarTypeSource::Arguments(arguments) => arguments.len(),
        }
    }
    pub(super) fn get(self, index: usize) -> Option<&'a FunctionValueType> {
        match self.0 {
            ScalarTypeSource::Values(values) => values.get(index),
            ScalarTypeSource::Arguments(arguments) => arguments.get(index).map(|argument| {
                let FunctionArgument::Value { value_type, .. } = argument else {
                    unreachable!("scalar request view passed the ordered Lambda gate")
                };
                value_type
            }),
        }
    }
    pub(super) fn iter(self) -> impl Clone + ExactSizeIterator<Item = &'a FunctionValueType> {
        (0..self.len()).map(move |index| self.get(index).expect("scalar argument ordinal"))
    }
}

/// Carrier projections borrow the same source; they do not clone DataType
/// trees, allocate an index, or change the consumer's domain rules.
#[derive(Clone, Copy)]
pub(super) struct ScalarCarriers<'a>(ScalarCarrierSource<'a>);
#[derive(Clone, Copy)]
enum ScalarCarrierSource<'a> {
    Values(ScalarTypes<'a>),
    DataTypes(&'a [arrow_schema::DataType]),
}
impl<'a> ScalarCarriers<'a> {
    pub(super) fn from_values(values: ScalarTypes<'a>) -> Self {
        Self(ScalarCarrierSource::Values(values))
    }
    pub(super) fn from_data_types(types: &'a [arrow_schema::DataType]) -> Self {
        Self(ScalarCarrierSource::DataTypes(types))
    }
    pub(super) fn len(self) -> usize {
        match self.0 {
            ScalarCarrierSource::Values(values) => values.len(),
            ScalarCarrierSource::DataTypes(types) => types.len(),
        }
    }
    pub(super) fn get(self, index: usize) -> Option<&'a arrow_schema::DataType> {
        match self.0 {
            ScalarCarrierSource::Values(values) => {
                values.get(index).map(FunctionValueType::data_type)
            }
            ScalarCarrierSource::DataTypes(types) => types.get(index),
        }
    }
    pub(super) fn iter(self) -> impl Clone + ExactSizeIterator<Item = &'a arrow_schema::DataType> {
        (0..self.len()).map(move |index| self.get(index).expect("scalar carrier ordinal"))
    }
}
impl std::ops::Index<usize> for ScalarCarriers<'_> {
    type Output = arrow_schema::DataType;
    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).expect("scalar carrier ordinal")
    }
}

pub(super) fn scalar_types<'a>(
    request: FunctionBindingRequest<'a>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ScalarTypes<'a>, FunctionBindingError> {
    for argument in request.arguments {
        work.step()?;
        match argument {
            FunctionArgument::Value { .. } => {}
            FunctionArgument::Lambda { .. } | FunctionArgument::CandidateGroup { .. } => {
                return Err(FunctionBindingError::NoMatchingOverload);
            }
        }
    }
    Ok(ScalarTypes(ScalarTypeSource::Arguments(request.arguments)))
}

pub(super) fn argument_types(
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<novarocks_type_contract::FunctionArgumentTypes, FunctionBindingError> {
    argument_types_with_creation::<std::convert::Infallible>(request, work, None)
        .map_err(crate::FunctionBindingCreationFailure::into_pure)
}
pub(super) fn argument_types_with_creation<H>(
    request: FunctionBindingRequest<'_>,
    work: &mut CompileCheckpoints<'_>,
    mut creation: Option<&mut dyn novarocks_type_contract::ValueSchemaCreationScope<H>>,
) -> Result<novarocks_type_contract::FunctionArgumentTypes, crate::FunctionBindingCreationFailure<H>>
{
    super::scalar_construction::with_binding_creation(
        super::scalar_construction::borrow_creator(&mut creation),
        work,
        |cx| cx.array::<FunctionArgumentType>(request.arguments.len()),
    )?;
    let mut result = Vec::with_capacity(request.arguments.len());
    for argument in request.arguments {
        work.step()?;
        result.push(argument.argument_type());
    }
    super::scalar_construction::with_binding_creation(
        super::scalar_construction::borrow_creator(&mut creation),
        work,
        |cx| cx.publish_arguments(result),
    )
}

pub(super) fn exact_type(
    left: &FunctionValueType,
    right: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, FunctionBindingError> {
    left.exactly_equals_observed::<KernelFailure>(right, || {
        work.step().map_err(crate::kernel_control::compile_failure)
    })
    .map_err(type_error)
}

fn exact_argument(
    left: &FunctionArgumentType,
    right: &FunctionArgumentType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, FunctionBindingError> {
    work.step()?;
    match (left, right) {
        (FunctionArgumentType::Value(a), FunctionArgumentType::Value(b)) => exact_type(a, b, work),
        (
            FunctionArgumentType::CandidateGroup { element_type: a },
            FunctionArgumentType::CandidateGroup { element_type: b },
        ) => exact_type(a, b, work),
        (
            FunctionArgumentType::Lambda {
                parameter_types: a,
                result_type: ar,
            },
            FunctionArgumentType::Lambda {
                parameter_types: b,
                result_type: br,
            },
        ) => {
            if a.len() != b.len() {
                return Ok(false);
            }
            for (a, b) in a.iter().zip(b) {
                if !exact_type(a, b, work)? {
                    return Ok(false);
                }
            }
            exact_type(ar, br, work)
        }
        _ => Ok(false),
    }
}

pub(crate) fn same_selection(
    left: &FunctionBindingSelection,
    right: &FunctionBindingSelection,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, FunctionBindingError> {
    if left.overload != right.overload || left.argument_types.len() != right.argument_types.len() {
        return Ok(false);
    }
    for (a, b) in left.argument_types.iter().zip(&right.argument_types) {
        if !exact_argument(a, b, work)? {
            return Ok(false);
        }
    }
    match (&left.result_type, &right.result_type) {
        (FunctionResultType::Scalar(a), FunctionResultType::Scalar(b)) => {
            if !exact_type(a, b, work)? {
                return Ok(false);
            }
        }
        (FunctionResultType::Relation(a), FunctionResultType::Relation(b)) => {
            if a.len() != b.len() {
                return Ok(false);
            }
            for (a, b) in a.iter().zip(b) {
                if !exact_type(a, b, work)? {
                    return Ok(false);
                }
            }
        }
        _ => return Ok(false),
    }
    match (&left.aggregate, &right.aggregate) {
        (None, None) => Ok(true),
        (Some(a), Some(b)) => Ok(a.state_format == b.state_format
            && a.state_argument_contract == b.state_argument_contract
            && exact_type(&a.intermediate_type, &b.intermediate_type, work)?),
        _ => Ok(false),
    }
}
