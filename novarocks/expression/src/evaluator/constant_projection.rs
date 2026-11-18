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
    observe_array_normalization_construction,
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
struct ProjectionBacking {
    _values: ArrayRef,
    _original: novarocks_functions::ConstantValue,
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
        observe_array_normalization_construction::<KernelFailure>(
            step,
            &mut |layout| {
                meter.borrow_mut().flush()?;
                stock.borrow_mut().request(layout.size())
            },
            &mut |bound| {
                meter.borrow_mut().flush()?;
                stock.borrow_mut().request(bound.bytes_upper)?;
                // Original Arrow constructors may validate the complete visible
                // structural NULL masks. Their actual opaque work is observed;
                // it is not activation/conversion of unselected pool values.
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
        let mut observed = novarocks_functions::EvaluationCheckpoints::new(work.control);
        let backing = novarocks_functions::SourceBackingOwner::try_from_owned_direct(
            ProjectionBacking {
                _values: values.clone(),
                _original: original.clone(),
                _stock: stock,
            },
            host.clone(),
            &mut observed,
        )?;
        let prepared = novarocks_functions::PreparedSourceCustody::<ProjectionBacking>::prepare(
            &values,
            &mut observed,
        )?;
        let authority = OpaqueRetainedCharge::try_new(host.clone())?;
        let mut reservation =
            authority.reserve_operation(prepared.request_facts().global_peak_upper())?;
        let retained = prepared.retain(&backing, &mut reservation, &mut observed)?;
        observed.finish()?;
        retained
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
