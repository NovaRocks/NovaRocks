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

// Design: ADR-0235 (docs/adr/ADR-0235-iceberg-write-layout-and-data-plane-capacity.md)

//! Materialize a provider-authored routing key and consume it at the writer's local exchange.

use std::num::NonZeroU32;
use std::sync::Arc;

use arrow::array::{Array, Int8Array, Int64Array};
use arrow::record_batch::RecordBatch;
use novarocks_local_program::StaticLayout;
use novarocks_spi::connector::binding::ConnectorPartitionKeyEvaluator;

use crate::exec::chunk::{Chunk, ChunkSchemaRef, record_batch_bytes};
use crate::exec::pipeline::driver_step::DriverStep;
use crate::exec::pipeline::input::PreparedInput;
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, InputEncodingPolicy, Operator, OperatorLifecycle,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::shape::{Transform, TransformKernel};
use crate::exec::pipeline::step::{NextStep, StepResult};
use crate::runtime::task::ExecutionResult;
use crate::runtime::task::connector_failure::{ConnectorCallFailure, ConnectorOperation};

use super::project_processor::project_reserve;
use super::write_spread::{WriteSpreadDriver, WriteSpreadState};

pub(crate) struct WriteLayoutProjectionFactory {
    output_layout: StaticLayout,
    key: Option<KeyProjection>,
}

#[derive(Clone)]
struct KeyProjection {
    evaluator: Arc<dyn ConnectorPartitionKeyEvaluator>,
    input_ordinals: Arc<[usize]>,
    effects: Option<(
        usize,
        Arc<[novarocks_connector_contract::ConnectorRowMutationEffect]>,
    )>,
    spread: Arc<WriteSpreadState>,
}

impl WriteLayoutProjectionFactory {
    pub(crate) fn attach(
        output_layout: StaticLayout,
        evaluator: Arc<dyn ConnectorPartitionKeyEvaluator>,
        input_ordinals: Vec<usize>,
        effects: Option<(
            usize,
            Arc<[novarocks_connector_contract::ConnectorRowMutationEffect]>,
        )>,
        writers: NonZeroU32,
    ) -> Self {
        Self {
            output_layout,
            key: Some(KeyProjection {
                evaluator,
                input_ordinals: Arc::from(input_ordinals),
                effects,
                spread: Arc::new(WriteSpreadState::new(writers)),
            }),
        }
    }

    pub(crate) fn drop_key(output_layout: StaticLayout) -> Self {
        Self {
            output_layout,
            key: None,
        }
    }
}

impl WriteLayoutProjectionFactory {
    fn operator_name(&self) -> &'static str {
        if self.key.is_some() {
            "WriteLayoutKey"
        } else {
            "DropWriteLayoutKey"
        }
    }
}

impl OperatorFactory for WriteLayoutProjectionFactory {
    fn name(&self) -> &str {
        self.operator_name()
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(WriteLayoutProjection {
            name: self.operator_name(),
            output_layout: self.output_layout.clone(),
            output: None,
            construction: None,
            key: self.key.clone().map(|key| (key, WriteSpreadDriver::new())),
        }))
    }
}

struct WriteLayoutProjection {
    name: &'static str,
    output_layout: StaticLayout,
    output: Option<ChunkSchemaRef>,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    key: Option<(KeyProjection, WriteSpreadDriver)>,
}

impl OperatorLifecycle for WriteLayoutProjection {
    fn name(&self) -> &str {
        self.name
    }
    fn creates_typed_storage(&self) -> bool {
        true
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.construction = Some(factory);
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        self.output = Some(factory.create_compiled_chunk_schema(&self.output_layout)?);
        Ok(())
    }
}

impl InputEncodingPolicy for WriteLayoutProjection {}

impl TransformKernel for WriteLayoutProjection {
    fn apply(
        &mut self,
        _: &mut DriverStep<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        let chunk = input.into_uncovered_chunk()?;
        let rows = chunk.len();
        if rows == 0 {
            return Ok(StepResult::new(None, NextStep::NeedInput, 0));
        }

        let output = self
            .output
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let width = output.slots().len();
        let batch = chunk.record_batch();
        let attaching = self.key.is_some();
        if attaching && batch.num_columns() + 1 != width
            || !attaching && batch.num_columns() != width + 1
        {
            return Err(ConstructionFailure::InvalidSource(
                "write layout projection column count differs".into(),
            )
            .into());
        }
        let owner = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?
            .begin_typed_construction(output)?;
        let mut columns = project_reserve(width, owner.as_ref())?;
        columns.extend(batch.columns().iter().take(width).cloned());
        if let Some((key, driver)) = self.key.as_mut() {
            columns.push(Arc::new(key.evaluate(batch, driver)?));
        }
        let storage = chunk.source_storage_lifetime_observed::<ConstructionFailure>(
            owner.storage_lifetime(),
            |layout| owner.request(layout),
        )?;
        let chunk = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
            Arc::clone(output),
            columns,
            rows,
            storage,
            |step| owner.observe(0, &[], step),
        )?;
        Ok(StepResult::new(
            Some(chunk),
            NextStep::NeedInput,
            rows as u64,
        ))
    }
}

impl KeyProjection {
    fn evaluate(
        &self,
        batch: &RecordBatch,
        driver: &mut WriteSpreadDriver,
    ) -> ExecutionResult<Int64Array> {
        let rows = batch.num_rows();
        if let Some((ordinal, accepted)) = &self.effects {
            let effects = batch
                .column(*ordinal)
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or(ConstructionFailure::InvalidSource(
                    "write layout route effect must be Int8".into(),
                ))?;
            let first = effects.value(0);
            if effects.null_count() != 0 || effects.values().iter().any(|effect| *effect != first) {
                return Err(ConstructionFailure::InvalidSource(
                    "write layout route requires a single change effect per page".into(),
                )
                .into());
            }
            if !accepted.iter().any(|effect| *effect as i8 == first) {
                // The router sends this page only to its other branches. This
                // key is removed by their projections and never used.
                return Ok(Int64Array::from_value(0, rows));
            }
        }
        let inputs = batch
            .project(&self.input_ordinals)
            .map_err(|error| ConstructionFailure::InvalidSource(error.to_string()))?;
        let partition_keys = self.evaluator.partition_keys(&inputs).map_err(|cause| {
            ConnectorCallFailure::new(ConnectorOperation::EvaluateWriteLayout, cause)
        })?;
        if partition_keys.len() != rows || partition_keys.null_count() != 0 {
            return Err(ConstructionFailure::InvalidSource(
                "provider write layout returned a null or incorrectly sized partition key column"
                    .into(),
            )
            .into());
        }
        Ok(Int64Array::from_iter_values(driver.layout_keys(
            &self.spread,
            partition_keys.values(),
            record_batch_bytes(batch) as u64,
        )))
    }
}

#[cfg(test)]
mod tests;
