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
//! The default host's exact guarded Values batch, prepared by its actual caller.
use crate::exec::chunk::{Chunk, ChunkSchemaRef};
use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionFactory, Operator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::shape::{Source, SourceKernel, SourceStart};
use crate::exec::pipeline::step::{NextStep, StepResult};
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime::task::ExecutionResult;
use arrow::array::ArrayRef;
use novarocks_local_program::StaticValues;
use std::sync::Arc;

pub(crate) struct TypedValuesSourceFactory {
    name: String,
    values: StaticValues,
}
impl TypedValuesSourceFactory {
    pub(crate) fn try_new(
        values: StaticValues,
        node_id: i32,
    ) -> crate::runtime::task::ExecutionResult<Self> {
        if !values.layout().is_typed() || values.batch_projection().is_none() {
            return Err(crate::exec::fragment::error::node_construction_failure(
                "native Values require the original complete guarded batch",
            ));
        }
        Ok(Self {
            name: if node_id >= 0 {
                format!("ValuesSource (id={node_id})")
            } else {
                "ValuesSource".into()
            },
            values,
        })
    }
}
impl OperatorFactory for TypedValuesSourceFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn is_source(&self) -> bool {
        true
    }
    fn create(&self, _dop: i32, driver_id: i32) -> Box<dyn Operator> {
        Box::new(Source::new(TypedValuesSource {
            name: self.name.clone(),
            values: self.values.clone(),
            driver_id: driver_id.max(0),
            output: None,
            factory: None,
        }))
    }
}
struct TypedValuesSource {
    name: String,
    values: StaticValues,
    driver_id: i32,
    output: Option<ChunkSchemaRef>,
    factory: Option<Arc<dyn InputConstructionFactory>>,
}
impl OperatorLifecycle for TypedValuesSource {
    fn name(&self) -> &str {
        &self.name
    }
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.factory = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        let factory = self
            .factory
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        self.output = Some(factory.create_compiled_chunk_schema(self.values.layout())?);
        Ok(())
    }
}

impl SourceKernel for TypedValuesSource {
    fn start(&self) -> SourceStart {
        if self.driver_id != 0 {
            SourceStart::Finished
        } else {
            SourceStart::Ready
        }
    }
    fn poll_output(
        &mut self,
        _ctx: &mut crate::exec::pipeline::driver_step::DriverStep<'_>,
    ) -> ExecutionResult<StepResult> {
        let source = self.values.batch_projection().ok_or_else(|| {
            ConstructionFailure::InvalidSource("native Values batch disappeared".into())
        })?;
        let batch = source.record_batch();
        if batch.num_rows() == 0 || self.driver_id != 0 {
            return Ok(StepResult::new(None, NextStep::Finished, 0));
        }
        let output = self
            .output
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let factory = self
            .factory
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = factory.begin_typed_construction(output)?;
        let storage = owner
            .storage_lifetime()
            .retaining_observed::<ConstructionFailure>(&source.storage_lifetime(), |layout| {
                owner.request(layout)
            })?;
        let layout = std::alloc::Layout::array::<ArrayRef>(batch.num_columns()).map_err(|_| {
            ConstructionFailure::Resource(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?;
        owner.request(layout)?;
        let mut columns = Vec::new();
        columns
            .try_reserve_exact(batch.num_columns())
            .map_err(|_| {
                ConstructionFailure::Control(
                    novarocks_type_contract::CompileControlError::ResourceExhausted,
                )
            })?;
        columns.extend(batch.columns().iter().cloned());
        // Payload values and original source custody are unchanged. This scope
        // observes only actual new column/reference and typed Chunk scaffolds.
        let chunk = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
            output.clone(),
            columns,
            batch.num_rows(),
            storage,
            |step| owner.observe(0, &[], step),
        )?;
        Ok(StepResult::new(Some(chunk), NextStep::Finished, 0))
    }
}

#[cfg(test)]
#[path = "typed_values_source_tests.rs"]
mod typed_values_source_tests;
