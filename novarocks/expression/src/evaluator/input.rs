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

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use novarocks_functions::KernelFailure;
use novarocks_type_contract::{
    ValueTypeStorageLifetime, owned_resources::layout::LayoutResourceError,
};
use std::{alloc::Layout, sync::Arc};

/// A checked storage layout can fail independently of the request callback.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InputStorageFailure {
    Kernel(KernelFailure),
    Layout(LayoutResourceError),
}

impl From<KernelFailure> for InputStorageFailure {
    fn from(cause: KernelFailure) -> Self {
        Self::Kernel(cause)
    }
}

impl From<LayoutResourceError> for InputStorageFailure {
    fn from(cause: LayoutResourceError) -> Self {
        Self::Layout(cause)
    }
}

/// Retain the actual finite storage backing a batch. Request each new holder's
/// layout before constructing it.
pub trait InputSourceStorage {
    fn storage_lifetime_observed(
        &self,
        base: ValueTypeStorageLifetime,
        request: &mut dyn FnMut(Layout) -> Result<(), InputStorageFailure>,
    ) -> Result<ValueTypeStorageLifetime, InputStorageFailure>;
}

/// A synchronous borrow of the actual input to a compiled expression.
#[derive(Clone, Copy)]
pub enum EvaluationInput<'a> {
    Batch {
        batch: &'a RecordBatch,
        source: Option<&'a dyn InputSourceStorage>,
    },
    Projected(ProjectedEvaluationInput<'a>),
}

/// The compact columns have already been matched to their original declaration.
#[derive(Clone, Copy)]
pub struct ProjectedEvaluationInput<'a> {
    batch: &'a RecordBatch,
    declared: &'a SchemaRef,
    projection: &'a Arc<InputColumnProjection>,
    source: Option<&'a dyn InputSourceStorage>,
}

impl<'a> EvaluationInput<'a> {
    /// Validate the physical projection in its original column order. The
    /// caller may additionally require its own binding for each coordinate.
    pub fn try_projected_observed(
        batch: &'a RecordBatch,
        declared: &'a SchemaRef,
        projection: &'a Arc<InputColumnProjection>,
        source: Option<&'a dyn InputSourceStorage>,
        control: &dyn novarocks_functions::KernelEvaluationControl,
        mut matches_binding: impl FnMut(usize, usize) -> Result<bool, KernelFailure>,
    ) -> Result<Self, KernelFailure> {
        use novarocks_functions::KernelDiagnostic;
        let ordinals = projection.ordinals();
        if batch.num_columns() != ordinals.len() {
            return Err(KernelFailure::InvalidProgram(KernelDiagnostic::new(
                "projected input lacks its exact typed column map",
            )));
        }
        let actual = batch.schema();
        let mut previous = None;
        for (physical, &original) in ordinals.iter().enumerate() {
            control.checkpoint(1)?;
            if previous.is_some_and(|p| p >= original)
                || original >= declared.fields().len()
                || !matches_binding(physical, original)?
                || !novarocks_type_contract::arrow_fields_exact_with_nullability_observed(
                    declared.field(original),
                    actual.field(physical),
                    novarocks_type_contract::CarrierNullabilityPolicy::Permissive,
                    || control.checkpoint(1),
                )?
            {
                return Err(KernelFailure::InvalidProgram(KernelDiagnostic::new(
                    "projected input column differs from its original frozen ordinal",
                )));
            }
            previous = Some(original);
        }
        Ok(Self::Projected(ProjectedEvaluationInput {
            batch,
            declared,
            projection,
            source,
        }))
    }

    pub fn batch(self) -> &'a RecordBatch {
        match self {
            Self::Batch { batch, .. } => batch,
            Self::Projected(input) => input.batch,
        }
    }

    pub fn source_storage(self) -> Option<&'a dyn InputSourceStorage> {
        match self {
            Self::Batch { source, .. } => source,
            Self::Projected(input) => input.source,
        }
    }

    pub fn declared_schema(self) -> SchemaRef {
        match self {
            Self::Batch { batch, .. } => batch.schema(),
            Self::Projected(input) => Arc::clone(input.declared),
        }
    }

    pub fn projection(self) -> Option<&'a Arc<InputColumnProjection>> {
        match self {
            Self::Batch { .. } => None,
            Self::Projected(input) => Some(input.projection),
        }
    }
}

/// Data-only column coordinates and their actual metadata creator. The caller
/// funds the vector and shared header before publishing this original map.
pub struct InputColumnProjection {
    ordinals: Vec<usize>,
    _storage: ValueTypeStorageLifetime,
}
impl InputColumnProjection {
    pub fn from_owned(ordinals: Vec<usize>, storage: ValueTypeStorageLifetime) -> Self {
        Self {
            ordinals,
            _storage: storage,
        }
    }
    pub fn ordinals(&self) -> &[usize] {
        &self.ordinals
    }
}
