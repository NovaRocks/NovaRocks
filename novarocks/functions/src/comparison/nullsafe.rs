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

//! The original flat null-safe equality, independently frozen from ordinary Eq.
//! In particular, floating NaN compares equal to every non-NULL floating value.
use super::*;
use crate::ScopedExpressionEffects;
use novarocks_type_contract::{ExpressionEffectContext, ExpressionEffects};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedNullSafeComparisonRecipe {
    left: FunctionValueType,
    right: FunctionValueType,
    leaf: FlatLeaf,
}
impl PreparedNullSafeComparisonRecipe {
    pub fn try_new(
        left: &FunctionValueType,
        right: &FunctionValueType,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ComparisonPrepareError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = (|| {
            validate_type_observed(left, &mut work).map_err(ComparisonPrepareError::Kernel)?;
            validate_type_observed(right, &mut work).map_err(ComparisonPrepareError::Kernel)?;
            let same_root = left.logical_type() == right.logical_type();
            work.step()?;
            if !same_root {
                return Err(ComparisonPrepareError::TypeMismatch);
            }
            if left.logical_type() != ValueLogicalType::Physical {
                return Err(ComparisonPrepareError::Unsupported);
            }
            // This is the legacy null-safe scalar capability, not the ordinary
            // Arrow comparison matrix. No data-dependent NULL-only permission.
            let supported = matches!(
                left.data_type(),
                DataType::Null
                    | DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Utf8
                    | DataType::Date32
                    | DataType::Timestamp(_, None)
                    | DataType::Decimal128(..)
            );
            work.step()?;
            if !supported {
                return Err(ComparisonPrepareError::Unsupported);
            }
            if !arrow_data_types_exact_observed::<ComparisonPrepareError>(
                left.data_type(),
                right.data_type(),
                || work.step().map_err(Into::into),
            )? {
                return Err(ComparisonPrepareError::TypeMismatch);
            }
            let leaf =
                FlatLeaf::from_type(left.data_type()).ok_or(ComparisonPrepareError::Unsupported)?;
            work.step()?;
            // These admitted carriers have no retained field graph or timezone.
            let recipe = Self {
                left: left.clone(),
                right: right.clone(),
                leaf,
            };
            work.step()?;
            Ok(recipe)
        })();
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
    pub fn left_type(&self) -> &FunctionValueType {
        &self.left
    }
    pub fn right_type(&self) -> &FunctionValueType {
        &self.right
    }
    pub const fn nullable_result(&self) -> bool {
        false
    }
    pub fn own_effects(&self, context: ExpressionEffectContext) -> ScopedExpressionEffects {
        ScopedExpressionEffects::primitive(context, ExpressionEffects::PURE_VALUE)
    }
    /// Required child errors are not successful input NULLs. Both addresses,
    /// classes and NULL promises are verified before this total predicate runs.
    pub fn compare_rows(
        &self,
        left: EvaluatedArgument<'_>,
        left_ordinal: usize,
        left_batch_row: usize,
        right: EvaluatedArgument<'_>,
        right_ordinal: usize,
        right_batch_row: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<bool, KernelFailure> {
        control.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(control);
        let result = (|| {
            let l = checked_row(left, left_ordinal, left_batch_row, &self.left, &mut work)?;
            let r = checked_row(
                right,
                right_ordinal,
                right_batch_row,
                &self.right,
                &mut work,
            )?;
            for (argument, ordinal) in [(left, left_ordinal), (right, right_ordinal)] {
                reject_unresolved_error(argument, ordinal, &mut work)?;
            }
            let la = left.array();
            let ra = right.array();
            self.validate_array(la.as_ref())?;
            work.step()?;
            self.validate_array(ra.as_ref())?;
            work.step()?;
            let ln = self.leaf == FlatLeaf::Null || la.is_null(l);
            let rn = self.leaf == FlatLeaf::Null || ra.is_null(r);
            work.step()?;
            if self.leaf != FlatLeaf::Null
                && ((ln && !self.left.nullable()) || (rn && !self.right.nullable()))
            {
                return Err(invalid(
                    "non-null null-safe comparison argument contains a selected NULL",
                ));
            }
            if ln || rn {
                return Ok(ln && rn);
            }
            self.compare_non_null(la.as_ref(), l, ra.as_ref(), r, &mut work)
        })();
        // The shared observer latches every callback cause, including non-control
        // KernelFailure variants; finishing never retries the refusing callback.
        work.finish()?;
        result
    }
    fn validate_array(&self, array: &dyn Array) -> Result<(), KernelFailure> {
        macro_rules! p {
            ($ty:ty) => {
                cast::<PrimitiveArray<$ty>>(array).map(|_| ())
            };
        }
        match self.leaf {
            FlatLeaf::Null => cast::<NullArray>(array).map(|_| ()),
            FlatLeaf::Boolean => cast::<BooleanArray>(array).map(|_| ()),
            FlatLeaf::Int8 => p!(Int8Type),
            FlatLeaf::Int16 => p!(Int16Type),
            FlatLeaf::Int32 => p!(Int32Type),
            FlatLeaf::Int64 => p!(Int64Type),
            FlatLeaf::Float32 => p!(Float32Type),
            FlatLeaf::Float64 => p!(Float64Type),
            FlatLeaf::Utf8 => cast::<StringArray>(array).map(|_| ()),
            FlatLeaf::Date32 => p!(Date32Type),
            FlatLeaf::Decimal128 => p!(Decimal128Type),
            FlatLeaf::TimestampSecond => p!(TimestampSecondType),
            FlatLeaf::TimestampMillisecond => p!(TimestampMillisecondType),
            FlatLeaf::TimestampMicrosecond => p!(TimestampMicrosecondType),
            FlatLeaf::TimestampNanosecond => p!(TimestampNanosecondType),
            _ => Err(internal(
                "null-safe recipe contains an unsupported frozen leaf",
            )),
        }
    }
    fn compare_non_null(
        &self,
        left: &dyn Array,
        l: usize,
        right: &dyn Array,
        r: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<bool, KernelFailure> {
        macro_rules! p {
            ($ty:ty) => {{
                let left = cast::<PrimitiveArray<$ty>>(left)?;
                let right = cast::<PrimitiveArray<$ty>>(right)?;
                let same = left.value(l) == right.value(r);
                work.step()?;
                Ok(same)
            }};
        }
        macro_rules! float {
            ($ty:ty) => {{
                let left = cast::<PrimitiveArray<$ty>>(left)?;
                let right = cast::<PrimitiveArray<$ty>>(right)?;
                let same = left
                    .value(l)
                    .partial_cmp(&right.value(r))
                    .unwrap_or(Ordering::Equal)
                    == Ordering::Equal;
                work.step()?;
                Ok(same)
            }};
        }
        match self.leaf {
            FlatLeaf::Boolean => {
                let left = cast::<BooleanArray>(left)?;
                let right = cast::<BooleanArray>(right)?;
                let same = left.value(l) == right.value(r);
                work.step()?;
                Ok(same)
            }
            FlatLeaf::Int8 => p!(Int8Type),
            FlatLeaf::Int16 => p!(Int16Type),
            FlatLeaf::Int32 => p!(Int32Type),
            FlatLeaf::Int64 => p!(Int64Type),
            FlatLeaf::Float32 => float!(Float32Type),
            FlatLeaf::Float64 => float!(Float64Type),
            FlatLeaf::Utf8 => {
                let left = cast::<StringArray>(left)?;
                let right = cast::<StringArray>(right)?;
                equal_bytes(left.value(l).as_bytes(), right.value(r).as_bytes(), work)
            }
            FlatLeaf::Date32 => p!(Date32Type),
            FlatLeaf::Decimal128 => p!(Decimal128Type),
            FlatLeaf::TimestampSecond => p!(TimestampSecondType),
            FlatLeaf::TimestampMillisecond => p!(TimestampMillisecondType),
            FlatLeaf::TimestampMicrosecond => p!(TimestampMicrosecondType),
            FlatLeaf::TimestampNanosecond => p!(TimestampNanosecondType),
            _ => Err(internal(
                "null-safe non-NULL comparison has a foreign frozen leaf",
            )),
        }
    }
}
fn reject_unresolved_error(
    argument: EvaluatedArgument<'_>,
    ordinal: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    if let EvaluatedArgument::SelectedColumn(values) = argument {
        // Sorted unique checked journals permit bounded address-local lookup.
        let (mut start, mut end) = (0, values.errors().len());
        while start < end {
            let middle = start + (end - start) / 2;
            let actual = values.errors()[middle].selected_ordinal();
            work.step()?;
            match actual.cmp(&ordinal) {
                Ordering::Less => start = middle + 1,
                Ordering::Greater => end = middle,
                Ordering::Equal => {
                    return Err(invalid(
                        "null-safe comparison cannot consume an unresolved selected row error",
                    ));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
