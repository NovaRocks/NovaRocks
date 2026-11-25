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
//! One original CV, with a canonical metadata-only execution representation.
//! The original pool/ordinal remain the only selected value and source author.
use super::*;
use novarocks_functions::array_projection::{
    ArrayNormalizationStep, ArrayProjectionFailure, normalize_execution_array_observed,
    observe_array_normalization_construction, rebuild_array_observed,
};
use novarocks_functions::opaque_memory::OpaqueRetainedCharge;
use std::cell::RefCell;

#[derive(Clone)]
pub(super) struct CanonicalConstant {
    pub(super) original: novarocks_functions::ConstantValue,
    pub(super) values: ArrayRef,
    #[cfg(test)]
    pub(super) created: std::sync::Weak<dyn arrow::array::Array>,
}
impl CanonicalConstant {
    pub(super) fn argument(&self) -> EvaluatedArgument<'_> {
        EvaluatedArgument::ConstantView(self.view())
    }
    pub(super) fn view(&self) -> novarocks_functions::CanonicalConstantView<'_> {
        novarocks_functions::CanonicalConstantView::from_authored_projection(
            &self.original,
            &self.values,
        )
    }
}

// The actual normalized array and original source drop before their new stock.
struct ProjectionBacking<S> {
    _values: ArrayRef,
    _original: S,
    _stock: OpaqueRetainedCharge,
}
struct ConstructionStock {
    actual: Option<OpaqueRetainedCharge>,
    bytes: usize,
    host: Option<Arc<dyn novarocks_functions::AggregateStateAllocator>>,
}
impl ConstructionStock {
    fn new(
        allocator: Option<&Arc<dyn novarocks_functions::AggregateStateAllocator>>,
    ) -> Result<Self, KernelFailure> {
        Ok(Self {
            actual: None,
            bytes: 0,
            host: allocator.cloned(),
        })
    }
    fn request(&mut self, bytes: usize) -> Result<(), KernelFailure> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .filter(|total| *total <= isize::MAX as usize)
            .ok_or_else(|| KernelFailure::from(ArrayProjectionFailure::RequestArithmetic))?;
        if self.actual.is_none()
            && let Some(host) = &self.host
        {
            self.actual = Some(OpaqueRetainedCharge::try_new(host.clone())?);
        }
        if let Some(actual) = &mut self.actual {
            let mut reservation = actual.reserve_operation(bytes)?;
            actual.reconcile_under_reservation(self.bytes, &mut reservation)?;
        }
        // No host means the existing pure entry's representation observation,
        // never a successful substitute runtime grant or a relabelled source.
        Ok(())
    }
}

fn observe_projection(
    step: ArrayNormalizationStep<'_>,
    stock: &RefCell<ConstructionStock>,
    meter: &RefCell<&mut Work<'_, '_, impl Sized>>,
) -> Result<(), KernelFailure> {
    observe_array_normalization_construction::<KernelFailure>(
        step,
        &mut |layout| {
            meter.borrow_mut().flush()?;
            stock.borrow_mut().request(layout.size())
        },
        &mut |bound| {
            meter.borrow_mut().flush()?;
            stock.borrow_mut().request(bound.bytes_upper)?;
            // The Arrow constructor may inspect structural NULL masks. Its
            // traversal does not activate or convert unselected values.
            for _ in 0..bound.work_upper {
                meter.borrow_mut().step()?;
            }
            meter.borrow_mut().flush()
        },
        &mut |units| {
            if units == 0 {
                return meter.borrow_mut().flush();
            }
            for _ in 0..units {
                meter.borrow_mut().step()?;
            }
            Ok(())
        },
    )
}

fn retain_projection<S: Send + Sync + 'static>(
    values: ArrayRef,
    original: S,
    stock: OpaqueRetainedCharge,
    host: &Arc<dyn novarocks_functions::AggregateStateAllocator>,
    control: &dyn KernelEvaluationControl,
) -> Result<ArrayRef, KernelFailure> {
    let mut observed = novarocks_functions::EvaluationCheckpoints::new(control);
    let backing = novarocks_functions::SourceBackingOwner::try_from_owned_direct(
        ProjectionBacking {
            _values: values.clone(),
            _original: original,
            _stock: stock,
        },
        host.clone(),
        &mut observed,
    )?;
    let prepared = novarocks_functions::PreparedSourceCustody::<ProjectionBacking<S>>::prepare(
        &values,
        &mut observed,
    )?;
    let authority = OpaqueRetainedCharge::try_new(host.clone())?;
    let mut reservation =
        authority.reserve_operation(prepared.request_facts().global_peak_upper())?;
    let retained = prepared.retain(&backing, &mut reservation, &mut observed)?;
    observed.finish()?;
    Ok(retained)
}

/// Publish the original compact body result with its frozen Arrow layout.
/// Only successful rows make a value claim. Failed slots already hold NULL
/// placeholders and retain their exact journal positions through rebuilding.
pub(super) fn rebuild_lambda_body_output<'a>(
    output: SelectedValues<'a>,
    result: &novarocks_functions::FunctionValueType,
    allocator: &Arc<dyn novarocks_functions::AggregateStateAllocator>,
    work: &mut Work<'_, '_, impl Sized>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    if novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
        output.values().data_type(),
        result.data_type(),
        || work.step(),
    )? {
        return Ok(output);
    }
    let (selection, source, errors) = output.into_parts();
    if errors.is_empty() {
        work.flush()?;
        validate_evaluated_argument_observed(
            EvaluatedArgument::Column(&source),
            Selection::all(source.len()),
            result,
            work.control,
        )?;
    } else {
        let successful = source.len() - errors.len();
        let _rows_hold =
            super::mapped::reserve(Some(allocator), super::mapped::bytes::<usize>(successful)?)?;
        let mut rows = super::mapped::buffer(successful)?;
        let mut failed = errors.iter().peekable();
        for row in 0..source.len() {
            if failed
                .next_if(|error| error.selected_ordinal() == row)
                .is_none()
            {
                rows.push(row);
            }
            work.step()?;
        }
        let selected = Selection::try_sparse_observed(source.len(), &rows, || work.step())?;
        work.flush()?;
        validate_evaluated_argument_observed(
            EvaluatedArgument::Column(&source),
            selected,
            result,
            work.control,
        )?;
    }

    work.flush()?;
    let stock = RefCell::new(ConstructionStock::new(Some(allocator))?);
    let meter = RefCell::new(&mut *work);
    let values = rebuild_array_observed(&source, result.data_type(), &mut |step| {
        observe_projection(step, &stock, &meter)
    })?;
    let stock = stock
        .into_inner()
        .actual
        .ok_or_else(|| internal("lambda body projection rebuilt without its construction stock"))?;
    let values = retain_projection(
        values,
        (source, result.clone()),
        stock,
        allocator,
        work.control,
    )?;
    SelectedValues::try_new_observed(selection, result.data_type(), values, errors, || {
        work.step()
    })
}

pub(super) fn project(
    original: &novarocks_functions::ConstantValue,
    result: &novarocks_functions::FunctionValueType,
    allocator: Option<&Arc<dyn novarocks_functions::AggregateStateAllocator>>,
    work: &mut Work<'_, '_, impl Sized>,
) -> Result<Option<(CanonicalConstant, Option<input_source::ArrayContinuation>)>, KernelFailure> {
    work.flush()?;
    let stock = RefCell::new(ConstructionStock::new(allocator)?);
    let meter = RefCell::new(&mut *work);
    let values = normalize_execution_array_observed(original.pool().array(), |step| {
        // This author only removes source annotations/anonymous role labels.
        // A checked CV never grants installed runtime encoding access. Reject
        // before the normalizer constructs any canonical type/array wrapper.
        if matches!(
            step,
            ArrayNormalizationStep::InspectType(
                DataType::Dictionary(_, _) | DataType::RunEndEncoded(_, _)
            )
        ) {
            return Err(invalid(
                "canonical constant view does not install encoded access",
            ));
        }
        observe_projection(step, &stock, &meter)
    })?;
    if Arc::ptr_eq(&values, original.pool().array()) {
        // Root source Field metadata is not part of the array's type. Do not
        // create a new runtime custody scope when the real author rebuilt none.
        return Ok(None);
    }
    #[cfg(test)]
    let created = Arc::downgrade(&values);
    work.flush()?;
    if values.len() != original.pool().array().len() {
        return Err(invalid(
            "canonical constant view changed original pool shape",
        ));
    }
    if !novarocks_type_contract::arrow_data_types_exact_observed::<KernelFailure>(
        values.data_type(),
        result.data_type(),
        || work.step(),
    )? {
        return Err(invalid(
            "canonical constant view differs from its complete selected layout",
        ));
    }
    let values = if let Some(host) = allocator {
        // Keep BOTH original CV/source and every actually new wrapper request
        // in the real output's Buffer/child/bufferless custody until last alias.
        let stock = stock
            .into_inner()
            .actual
            .expect("actual supplied host owns projection stock");
        retain_projection(values, original.clone(), stock, host, work.control)?
    } else {
        // This is the existing hostless compilation entry. It borrows, rather
        // than replaces, the original finite/pure CV source while evaluating.
        // No runtime allocation authority or pure source label is manufactured.
        values
    };
    // New wrapper stock is a genuine creator even when the original CV is
    // wholly pure. Carry this explicit continuation through Arrow copies which
    // may share newly canonical Field Arcs while replacing all payload buffers.
    let creator = match allocator {
        Some(host) => Some(input_source::ArrayContinuation::capture(
            &values,
            host,
            work.control,
        )?),
        None => None,
    };
    let projected = CanonicalConstant {
        original: original.clone(),
        values,
        #[cfg(test)]
        created,
    };
    validate_evaluated_argument_observed(
        projected.argument(),
        Selection::all(1),
        result,
        work.control,
    )?;
    Ok(Some((projected, creator)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{CountingAllocator, HarnessControl};
    use arrow::{
        array::{Array, Int32Array, StructArray},
        buffer::NullBuffer,
        datatypes::{Field, Fields},
    };
    use novarocks_functions::DataFailureReason;

    #[test]
    fn array_invocation_frames_rebuild_body_carrier_without_activating_error_slots() {
        let fields = |nullable| {
            Fields::from(vec![Arc::new(Field::new(
                "value",
                DataType::Int32,
                nullable,
            ))])
        };
        let source: ArrayRef = Arc::new(StructArray::new(
            fields(true),
            vec![Arc::new(Int32Array::from(vec![Some(17), None]))],
            Some(NullBuffer::from(vec![true, false])),
        ));
        let result =
            novarocks_functions::FunctionValueType::new(DataType::Struct(fields(false)), false);
        let error =
            RowDataError::with_reason(1, DataFailureReason::DivisionByZero, "original body error");
        let positions = [1, 3];
        let selection = Selection::try_sparse(4, &positions).unwrap();
        let input = SelectedValues::try_new(
            selection,
            source.data_type(),
            Arc::clone(&source),
            Box::new([error.clone()]),
        )
        .unwrap();
        let counting = Arc::new(CountingAllocator::new(usize::MAX));
        let allocator: Arc<dyn novarocks_functions::AggregateStateAllocator> = counting.clone();
        let control = ObservedControl {
            original: &HarnessControl,
            refused: AtomicBool::new(false),
            operation_aborted: AtomicBool::new(false),
        };
        let mut work: Work<'_, '_> = Work {
            control: &control,
            pending: 0,
            scalar_host: None,
        };
        let output = rebuild_lambda_body_output(input, &result, &allocator, &mut work).unwrap();
        assert_eq!(output.selection().iter().collect::<Vec<_>>(), positions);
        assert_eq!(output.errors(), &[error]);
        assert_eq!(output.values().data_type(), result.data_type());
        let values = output
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(values.is_null(1));
        let alias = Arc::clone(values.column(0));
        assert_eq!(
            alias
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            17,
        );
        drop(output);
        assert!(counting.outstanding() > 0);
        drop(alias);
        assert_eq!(counting.outstanding(), 0);
    }
}
