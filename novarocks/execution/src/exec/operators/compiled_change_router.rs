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

//! An effect-validated, strict fanout over the original compiled input layout.
//! Each branch's DataStreamSink performs its own frozen wire projection. Keeping
//! the complete root input here preserves compiled partition-root bindings.
// Design: ADR-0208 (docs/adr/ADR-0208-logical-write-counts-and-strict-stream-delivery.md)
use super::{
    compiled_writer_failure::contract,
    data_stream_sink::take_partition_rows_created,
    logical_change_summary::{SummaryUnitObligation, TaskSummaryState},
};
use crate::{
    exec::{
        chunk::Chunk,
        pipeline::{
            driver_step::DriverStep,
            input::PreparedInput,
            operator::{InputConstructionFactory, InputEncodingPolicy, Operator},
            operator_factory::OperatorFactory,
            shape::fanout::{Fanout, FanoutRouter, RouteStep},
        },
    },
    runtime::{observable::Observable, task::ExecutionResult},
};
use arrow::{
    array::{Array, Int8Array},
    datatypes::DataType,
};
use novarocks_local_program::{LogicalChangeCounts, StaticRouterRoute};
use novarocks_spi::connector::ConnectorRowMutationEffect as Effect;
use novarocks_types::SlotId;
use std::sync::Arc;

pub(crate) struct CompiledChangeRouterFactory {
    name: String,
    effect: SlotId,
    routes: Arc<[StaticRouterRoute]>,
    branches: Vec<Arc<dyn OperatorFactory>>,
    summary: Option<TaskSummaryState>,
}
impl CompiledChangeRouterFactory {
    /// Branches are provider routes only. The Task's one summary source is an
    /// independent pipeline, so it never waits for writer finalization.
    pub(crate) fn new(
        effect: SlotId,
        routes: Arc<[StaticRouterRoute]>,
        branches: Vec<Arc<dyn OperatorFactory>>,
        summary: Option<TaskSummaryState>,
    ) -> ExecutionResult<Self> {
        if routes.is_empty()
            || routes.len() != branches.len()
            || routes
                .iter()
                .enumerate()
                .any(|(i, r)| r.branch as usize != i || r.accepted_effects.is_empty())
        {
            return Err(contract(
                "change router provider routes are not an exact dense branch set",
            ));
        }
        Ok(Self {
            name: "CompiledChangeRouter".into(),
            effect,
            routes,
            branches,
            summary,
        })
    }
}
impl OperatorFactory for CompiledChangeRouterFactory {
    fn name(&self) -> &str {
        &self.name
    }
    fn is_sink(&self) -> bool {
        true
    }
    fn bind_pipeline_dop(&self, dop: i32) -> ExecutionResult<()> {
        if let Some(summary) = &self.summary {
            summary.validate_units(
                usize::try_from(dop).map_err(|_| contract("change router has invalid width"))?,
            )?;
        }
        for branch in &self.branches {
            branch.bind_pipeline_dop(dop)?;
        }
        Ok(())
    }
    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let contribution = self
            .summary
            .as_ref()
            .map(|summary| {
                summary.validate_units(
                    usize::try_from(dop)
                        .map_err(|_| contract("change router has invalid width"))?,
                )?;
                let id = usize::try_from(driver_id)
                    .map_err(|_| contract("change router has a negative driver ordinal"))?;
                summary.contributor(id)
            })
            .transpose();
        let (contributor, failure) = match contribution {
            Ok(c) => (c, None),
            Err(e) => (None, Some(e)),
        };
        Box::new(Fanout::new(
            self.name.clone(),
            self.branches
                .iter()
                .map(|b| b.create(dop, driver_id))
                .collect(),
            ChangeRouter {
                effect: self.effect,
                routes: self.routes.clone(),
                contributor,
                exhausted: false,
            },
            failure,
        ))
    }
}
struct ChangeRouter {
    effect: SlotId,
    routes: Arc<[StaticRouterRoute]>,
    contributor: Option<SummaryUnitObligation>,
    exhausted: bool,
}
pub(crate) struct ChangeCursor {
    input: Chunk,
    effect_ordinal: usize,
    next: usize,
}
impl InputEncodingPolicy for ChangeRouter {
    fn retains_encoded_column(&self, slot: SlotId, _: &DataType) -> bool {
        slot != self.effect
    }
    fn retains_encoded_path(
        &self,
        value: crate::exec::pipeline::operator::EncodedValueOccurrence<'_>,
    ) -> bool {
        value.slot != self.effect
    }
}
impl FanoutRouter for ChangeRouter {
    type Cursor = ChangeCursor;
    fn begin(
        &mut self,
        input: PreparedInput,
        _: &dyn InputConstructionFactory,
        ctx: &mut DriverStep<'_>,
    ) -> ExecutionResult<ChangeCursor> {
        let input = input.into_uncovered_chunk()?;
        let ordinal = *input
            .slot_id_to_index()
            .get(&self.effect)
            .ok_or_else(|| contract("change router input has no frozen effect slot"))?;
        let effects = input.columns()[ordinal]
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| contract("change router effect is not Int8"))?;
        let mut counts = LogicalChangeCounts::ZERO;
        // Validate the WHOLE block before any branch projection or contribution.
        for row in 0..effects.len() {
            if row % 256 == 0 {
                ctx.checkpoint()?;
            }
            let effect = if effects.is_null(row) {
                return Err(contract("change router effect is NULL"));
            } else {
                match effects.value(row) {
                    1 => Effect::Delete,
                    2 => Effect::Replace,
                    3 => Effect::Insert,
                    _ => {
                        return Err(contract(
                            "change router effect is outside the frozen vocabulary",
                        ));
                    }
                }
            };
            if !self
                .routes
                .iter()
                .any(|route| route.accepted_effects.contains(&effect))
            {
                return Err(contract(
                    "change router effect has no accepting provider route",
                ));
            }
            let delta = match effect {
                Effect::Insert => LogicalChangeCounts::INSERT,
                Effect::Replace => LogicalChangeCounts::REPLACE,
                Effect::Delete => LogicalChangeCounts::DELETE,
            };
            counts = counts.checked_add(delta).map_err(|e| {
                super::logical_change_summary::summary_failure(
                    super::logical_change_summary::SummaryFailure::Count {
                        kind: novarocks_result_contract::SummaryKind::WriteCounts,
                        cause: novarocks_result_contract::SummaryMergeError::WriteCounts(e),
                    },
                )
            })?;
        }
        ctx.checkpoint()?;
        if let Some(contributor) = &mut self.contributor {
            contributor.accept(novarocks_result_contract::SummaryValue::WriteCounts(counts))?;
        }
        Ok(ChangeCursor {
            input,
            effect_ordinal: ordinal,
            next: 0,
        })
    }
    fn advance(
        &mut self,
        cursor: &mut ChangeCursor,
        factory: &dyn InputConstructionFactory,
        ctx: &mut DriverStep<'_>,
    ) -> ExecutionResult<RouteStep> {
        let Some(route) = self.routes.get(cursor.next) else {
            return Ok(RouteStep::Finished);
        };
        cursor.next += 1;
        let effects = cursor.input.columns()[cursor.effect_ordinal]
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("whole block validated before branch work");
        let owner = factory.begin_input_construction(&cursor.input)?;
        owner.request(std::alloc::Layout::array::<u32>(effects.len()).map_err(|_|crate::exec::pipeline::operator::ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
        let mut rows = Vec::with_capacity(effects.len());
        for row in 0..effects.len() {
            if row % 256 == 0 {
                ctx.checkpoint()?;
            }
            if route
                .accepted_effects
                .iter()
                .any(|e| *e as i8 == effects.value(row))
            {
                rows.push(
                    u32::try_from(row)
                        .map_err(|_| contract("change router row index exceeds UInt32"))?,
                );
            }
        }
        // Empty routes do no projection; continue the bounded cursor next turn.
        if rows.is_empty() {
            return Ok(RouteStep::Skip);
        }
        let chunk =
            take_partition_rows_created(&cursor.input, &rows, factory, &|| ctx.checkpoint())?;
        Ok(RouteStep::Input {
            branch: route.branch as usize,
            chunk,
        })
    }
    fn accepts_branch(&self, _: usize) -> bool {
        true
    }
    fn requires_branch_drain(&self, _: usize) -> bool {
        true
    }
    fn input_exhausted(&mut self, ctx: &mut DriverStep<'_>) -> ExecutionResult<()> {
        ctx.checkpoint()?;
        if !self.exhausted {
            if let Some(contributor) = &mut self.contributor {
                contributor.finish()?;
            }
            self.exhausted = true;
        }
        Ok(())
    }
    fn visit_demand_events(&self, _: &mut dyn FnMut(Arc<Observable>)) {}
}

#[cfg(test)]
mod tests;
