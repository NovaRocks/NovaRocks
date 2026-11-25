// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Evaluate the authored batch and each applicable constant substitution.

use super::{TextError, TypedText, compile, freeze, unit_limits};
use crate::{
    CompiledExprSet, CompiledExpressionInstance, EvaluationEnvironment, EvaluationInput,
    testkit::{PassThroughCallHost, register_constant},
};
use arrow::{
    array::{ArrayData, ArrayRef, UInt64Array},
    buffer::Buffer,
    compute::take,
    datatypes::{ArrowNativeType, DataType},
    error::ArrowError,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use novarocks_constant_contract::{ConstantError, ConstantPools, ConstantValue};
use novarocks_expression_contract::{
    BoundExprSet, BoundExprSetBuilder, ExprKind, RootOrdinal, RootScope, ValueId,
};
use novarocks_function_contract::FunctionArgument;
use novarocks_functions::{
    KernelEvaluationControl, PreparedPureKernel, RowDataError, ScalarInvocationActivation,
    ScalarInvocationScope, Selection,
};
use novarocks_type_contract::{
    ArgumentControl, CompileCheckpoints, CompileControlError, CompilePhase, FunctionId,
    FunctionInstanceState, FunctionValueType, PureCompileControl,
};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EvaluationShape {
    pub port_count: usize,
    pub input_columns: usize,
    pub input_rows: usize,
}

#[derive(Debug)]
pub struct RowEvaluation {
    pub result_type: FunctionValueType,
    pub values: ArrayRef,
    pub errors: Box<[RowDataError]>,
    pub shape: EvaluationShape,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallInapplicability {
    Volatile,
    InstanceState,
    ObservableEffects,
    WholeInvocation,
    ArgumentControl,
}

#[derive(Debug)]
pub enum Inapplicability {
    NoInputColumns,
    NoInputRows,
    Call {
        function: FunctionId,
        reason: CallInapplicability,
    },
    InputConstant {
        row: usize,
        column: String,
        cause: ConstantError,
    },
}

#[derive(Debug)]
pub enum Consistency {
    Compared {
        constant_shapes: Vec<EvaluationShape>,
    },
    NotApplicable(Inapplicability),
}

#[derive(Debug)]
pub struct CheckedRows {
    pub batch: RowEvaluation,
    pub consistency: Consistency,
}

impl TypedText<'_> {
    /// Compare the real input batch with constant substitution where its
    /// compiled calls and every actual replacement are admissible.
    pub fn check_rows(
        &self,
        rows: &RecordBatch,
        control: &dyn PureCompileControl,
        evaluation_control: &dyn KernelEvaluationControl,
    ) -> Result<CheckedRows, TextError> {
        let compiled = compile(
            self,
            &self.unit,
            &self.constants,
            self.layout.as_ref(),
            control,
        )?;
        let Some(_) = self.layout else {
            let batch = evaluate_constant(compiled, evaluation_control)?;
            return Ok(CheckedRows {
                batch,
                consistency: Consistency::NotApplicable(Inapplicability::NoInputColumns),
            });
        };
        let input = self.project_rows(rows)?;
        let applicability = if input.num_rows() == 0 {
            Some(Inapplicability::NoInputRows)
        } else {
            call_inapplicability(&compiled, control)?
        };
        let batch = evaluate(compiled, &input, evaluation_control)?;
        if let Some(reason) = applicability {
            return Ok(CheckedRows {
                batch,
                consistency: Consistency::NotApplicable(reason),
            });
        }
        let replacements = match self.admit_inputs(&input, control)? {
            Ok(replacements) => replacements,
            Err(reason) => {
                return Ok(CheckedRows {
                    batch,
                    consistency: Consistency::NotApplicable(reason),
                });
            }
        };
        let constant_shapes =
            self.compare_rows(&batch, replacements, control, evaluation_control)?;
        Ok(CheckedRows {
            batch,
            consistency: Consistency::Compared { constant_shapes },
        })
    }

    fn project_rows(&self, rows: &RecordBatch) -> Result<RecordBatch, TextError> {
        if rows.num_columns() != self.columns.len() {
            return Err(TextError::Layout(
                "batch width differs from the declared input layout",
            ));
        }
        let layout = self.layout.as_ref().expect("this form has an input port");
        let source_schema = rows.schema();
        let projected = self
            .used_ordinals
            .iter()
            .zip(layout.schema().fields())
            .map(|(&ordinal, expected)| {
                if source_schema.field(ordinal) != expected.as_ref() {
                    return Err(TextError::Layout(
                        "batch column field differs from the declared input layout",
                    ));
                }
                Ok(Arc::clone(rows.column(ordinal)))
            })
            .collect::<Result<Vec<_>, _>>()?;
        RecordBatch::try_new(layout.schema().clone(), projected).map_err(TextError::Input)
    }

    fn admit_inputs(
        &self,
        input: &RecordBatch,
        control: &dyn PureCompileControl,
    ) -> Result<Result<Vec<Vec<ConstantValue>>, Inapplicability>, TextError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let schema = input.schema();
        let mut replacements = Vec::with_capacity(input.num_rows());
        for row in 0..input.num_rows() {
            let index = u64::try_from(row).map_err(|_| CompileControlError::ResourceExhausted)?;
            let indices = UInt64Array::from(vec![index]);
            let mut values = Vec::with_capacity(input.num_columns());
            for (ordinal, (column, &source)) in
                input.columns().iter().zip(&self.used_ordinals).enumerate()
            {
                work.step()?;
                let ty = self.columns[source].value_type.clone();
                work.flush()?;
                // Compact the row so its admission does not charge unrelated
                // rows retained by a slice into the original batch buffer.
                let selected = take(column.as_ref(), &indices, None).map_err(TextError::Input)?;
                match ConstantValue::from_scalar_array(
                    Arc::clone(&schema.fields()[ordinal]),
                    ty,
                    selected.to_data(),
                    self.constant_policy,
                    CompilePhase::FunctionSpecialization,
                    control,
                ) {
                    Ok(constant) => values.push(constant),
                    Err(ConstantError::Control(cause)) => return Err(TextError::Control(cause)),
                    Err(cause) => {
                        work.finish()?;
                        return Ok(Err(Inapplicability::InputConstant {
                            row,
                            column: self.columns[source].name.clone(),
                            cause,
                        }));
                    }
                }
            }
            replacements.push(values);
        }
        work.finish()?;
        Ok(Ok(replacements))
    }

    fn compare_rows(
        &self,
        batch: &RowEvaluation,
        replacements: Vec<Vec<ConstantValue>>,
        control: &dyn PureCompileControl,
        evaluation_control: &dyn KernelEvaluationControl,
    ) -> Result<Vec<EvaluationShape>, TextError> {
        let mut shapes = Vec::with_capacity(replacements.len());
        let batch_values = normalize_nans(batch.values.to_data()).map_err(TextError::Input)?;
        for (row, values) in replacements.into_iter().enumerate() {
            let (unit, constants) = self.constant_unit(values, control)?;
            let compiled = compile(self, &unit, &constants, None, control)?;
            let single = evaluate_constant(compiled, evaluation_control)?;
            compare_row(batch, &batch_values, &single, row, control)?;
            shapes.push(single.shape);
        }
        Ok(shapes)
    }

    fn constant_unit(
        &self,
        replacements: Vec<ConstantValue>,
        control: &dyn PureCompileControl,
    ) -> Result<(BoundExprSet, ConstantPools), TextError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        let mut constants = self.constants.clone();
        let supplied: BTreeMap<ValueId, _> = self.unit.ports()[0]
            .values
            .iter()
            .zip(replacements)
            .map(|((value, _), constant)| (*value, register_constant(&mut constants, &constant)))
            .collect();
        let mut draft = BoundExprSetBuilder::new(unit_limits());
        for definition in self.unit.definitions() {
            work.step()?;
            let kind = match &definition.kind {
                ExprKind::Value(value) => ExprKind::Constant(*supplied.get(value).ok_or(
                    TextError::Layout("constant substitution has no value for an input occurrence"),
                )?),
                kind => kind.clone(),
            };
            let id = draft.add(None, definition.ty.clone(), kind)?;
            if id != definition.id {
                return Err(TextError::Layout(
                    "substituted unit changed a definition identity",
                ));
            }
        }
        for (&id, original) in self.unit.requests() {
            work.step()?;
            let mut request = original.clone();
            let ExprKind::FunctionCall { args, .. } = &self
                .unit
                .definition(id)
                .expect("request has an original definition")
                .kind
            else {
                unreachable!("text requests belong to scalar function calls");
            };
            for (&argument, source) in args.iter().zip(&mut request.arguments) {
                work.step()?;
                if let FunctionArgument::Value { constant, .. } = source
                    && let Some(definition) = draft.definition(argument)
                    && let ExprKind::Constant(reference) = definition.kind
                {
                    *constant = Some(reference);
                }
            }
            draft.request(id, request)?;
        }
        let root = &self.unit.roots()[0];
        draft.root(root.definition, root.demand, RootScope::AllPorts)?;
        let unit = freeze(
            draft,
            Vec::new(),
            EvaluationEnvironment {
                constants: &constants,
                parameters: &self.parameters,
            },
            self.functions,
            self.constant_policy,
            self.decimal_overflow_policy,
            &mut work,
        )?;
        work.finish()?;
        Ok((unit, constants))
    }
}

fn call_inapplicability(
    compiled: &CompiledExprSet,
    control: &dyn PureCompileControl,
) -> Result<Option<Inapplicability>, TextError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    for call in compiled.calls().values() {
        work.step()?;
        let contract = call.call_contract();
        let effects = contract.effects();
        let reason = if effects.value_stability.is_volatile() {
            Some(CallInapplicability::Volatile)
        } else if effects.instance_state != FunctionInstanceState::None {
            Some(CallInapplicability::InstanceState)
        } else if !effects.observable_effects.is_empty() {
            Some(CallInapplicability::ObservableEffects)
        } else {
            match call.prepared() {
                PreparedPureKernel::Scalar(kernel)
                    if kernel.instance_inline_allocation_bytes() == 0 =>
                {
                    None
                }
                PreparedPureKernel::Scalar(_) => Some(CallInapplicability::InstanceState),
                PreparedPureKernel::ControlIntrinsic(_) => None,
                _ => Some(CallInapplicability::WholeInvocation),
            }
        };
        let reason = reason.or(match effects.argument_control {
            ArgumentControl::Eager
            | ArgumentControl::If
            | ArgumentControl::Coalesce
            | ArgumentControl::Try
            | ArgumentControl::OrderedRegexp
            | ArgumentControl::OrderedNull
            | ArgumentControl::NoArguments => None,
            _ => Some(CallInapplicability::ArgumentControl),
        });
        if let Some(reason) = reason {
            work.finish()?;
            return Ok(Some(Inapplicability::Call {
                function: contract.function_id().clone(),
                reason,
            }));
        }
    }
    work.finish()?;
    Ok(None)
}

fn evaluate_constant(
    compiled: Arc<CompiledExprSet>,
    control: &dyn KernelEvaluationControl,
) -> Result<RowEvaluation, TextError> {
    let schema = compiled
        .scope(RootScope::AllPorts)
        .ok_or(TextError::Layout(
            "zero-port program omitted its compiled empty layout",
        ))?
        .schema();
    let input = RecordBatch::try_new_with_options(
        schema.clone(),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .map_err(TextError::Input)?;
    evaluate(compiled, &input, control)
}

fn evaluate(
    compiled: Arc<CompiledExprSet>,
    batch: &RecordBatch,
    control: &dyn KernelEvaluationControl,
) -> Result<RowEvaluation, TextError> {
    let root = RootOrdinal::new(0);
    let definition = compiled
        .root(root)
        .expect("compiled text has a single root")
        .definition;
    let result_type = compiled
        .definitions()
        .node(definition)
        .and_then(|node| node.value_type())
        .ok_or(TextError::Layout(
            "compiled text has no declared result type",
        ))?
        .clone();
    let shape = EvaluationShape {
        port_count: compiled.scopes().len(),
        input_columns: batch.num_columns(),
        input_rows: batch.num_rows(),
    };
    let allocator: Arc<dyn novarocks_functions::AggregateStateAllocator> =
        Arc::new(crate::testkit::CountingAllocator::new(usize::MAX));
    let mut instance = CompiledExpressionInstance::try_new_with_allocator(
        compiled,
        root,
        control,
        Some(allocator),
    )
    .map_err(|cause| TextError::Instance(Box::new(cause)))?;
    let selection = Selection::all(batch.num_rows());
    let result = instance
        .evaluate(
            EvaluationInput::Batch {
                batch,
                source: None,
            },
            ScalarInvocationScope::new(selection, selection),
            ScalarInvocationActivation::Activated,
            control,
            &mut PassThroughCallHost,
        )
        .map_err(|cause| TextError::Evaluation(Box::new(cause)))?;
    let (selected, values, errors) = result.into_parts();
    if !selected.is_all() || selected.len() != shape.input_rows || values.len() != shape.input_rows
    {
        return Err(TextError::Layout(
            "evaluated rows differ from the actual input shape",
        ));
    }
    Ok(RowEvaluation {
        result_type,
        values,
        errors,
        shape,
    })
}

fn compare_row(
    batch: &RowEvaluation,
    batch_values: &ArrayData,
    single: &RowEvaluation,
    row: usize,
    control: &dyn PureCompileControl,
) -> Result<(), TextError> {
    let difference = |detail| TextError::Mismatch { row, detail };
    if single.shape
        != (EvaluationShape {
            port_count: 0,
            input_columns: 0,
            input_rows: 1,
        })
    {
        return Err(difference(format!(
            "unexpected constant input shape {:?}",
            single.shape
        )));
    }
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let same_type = batch
        .result_type
        .exactly_equals_checked_observed(&single.result_type, || work.step())?;
    work.finish()?;
    if !same_type {
        return Err(difference(format!(
            "declared types differ: {:?} vs {:?}",
            batch.result_type, single.result_type
        )));
    }
    let batch_error = batch
        .errors
        .binary_search_by_key(&row, RowDataError::selected_ordinal)
        .ok()
        .map(|index| &batch.errors[index]);
    let single_error = single.errors.first();
    match (batch_error, single_error) {
        (Some(batch), Some(single))
            if batch.reason() == single.reason() && batch.message() == single.message() =>
        {
            Ok(())
        }
        (None, None) => {
            let actual = batch_values.slice(row, 1);
            let expected = normalize_nans(single.values.to_data()).map_err(TextError::Input)?;
            if actual == expected {
                Ok(())
            } else {
                Err(difference(format!(
                    "values differ: {:?} vs {:?}",
                    batch.values.slice(row, 1),
                    single.values
                )))
            }
        }
        (batch, single) => Err(difference(format!(
            "row errors differ: {batch:?} vs {single:?}"
        ))),
    }
}

// Arrow equality already handles logical nested selections, NULL, and signed
// zero. Normalize only floating NaN encodings, including inside ROW results.
fn normalize_nans(data: ArrayData) -> Result<ArrayData, ArrowError> {
    let buffer = match data.data_type() {
        DataType::Float16 => {
            canonical_nan_buffer(
                &data,
                |bits: u16| {
                    if bits & 0x7fff > 0x7c00 { 0x7e00 } else { bits }
                },
            )
        }
        DataType::Float32 => canonical_nan_buffer(&data, |bits: u32| {
            if f32::from_bits(bits).is_nan() {
                f32::NAN.to_bits()
            } else {
                bits
            }
        }),
        DataType::Float64 => canonical_nan_buffer(&data, |bits: u64| {
            if f64::from_bits(bits).is_nan() {
                f64::NAN.to_bits()
            } else {
                bits
            }
        }),
        _ => None,
    };
    if let Some(buffer) = buffer {
        return data.into_builder().buffers(vec![buffer]).build();
    }
    if data.child_data().is_empty() {
        return Ok(data);
    }
    let children = data
        .child_data()
        .iter()
        .cloned()
        .map(normalize_nans)
        .collect::<Result<_, _>>()?;
    data.into_builder().child_data(children).build()
}

fn canonical_nan_buffer<T: ArrowNativeType>(
    data: &ArrayData,
    normalize: impl Fn(T) -> T,
) -> Option<Buffer> {
    let source = data.buffers()[0].typed_data::<T>();
    let range = data.offset()..data.offset() + data.len();
    if source[range.clone()]
        .iter()
        .any(|&value| normalize(value) != value)
    {
        let mut values = source.to_vec();
        for value in &mut values[range] {
            *value = normalize(*value);
        }
        Some(Buffer::from_vec(values))
    } else {
        None
    }
}
