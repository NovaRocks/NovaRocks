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

//! Compiled writer-family pipelines.
//!
//! A TableWriter runs on its input's drivers, one independent writer per
//! driver, exactly as the plan-tree writer does. An immediate partitioned
//! receiver is re-shuffled on its exact edge keys at driver granularity.
//! A TableFinish owns the complete prepared write set, so its one
//! writer-result input is gathered to one driver first: DOP 1 is the finish
//! operator's own law.

use super::*;
use crate::exec::operators::compiled_writer::{
    compiled_table_finish_factory, compiled_table_writer_factory,
};

pub(super) fn build_table_writer<H: CompiledSchemaMetadataScope>(
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    node_id: i32,
    input: ProgramNodeId,
    ctx: &mut PipelineBuildContext,
    error: &Arc<RuntimeErrorState>,
    metadata: &mut CompiledMetadataMode<'_, H>,
) -> ExecutionResult<PipelineBuildResult> {
    // The factory copies the Task capability it needs, so a refused binding
    // builds no driver.
    let binding = ctx.compiled_writers.writer(id).ok_or_else(|| {
        format!(
            "missing Task write binding for compiled table writer at local node {}",
            id.index()
        )
    })?;
    let factory = compiled_table_writer_factory(program, id, node_id, binding, error)?;
    let mut build = build_node(program, input, ctx, error, metadata)?;
    if matches!(
        program.graph().nodes()[input.index()].kind(),
        ProgramNodeKind::ExchangeSource { .. }
    ) {
        let receiver = program.exchange_inputs().get(&input).ok_or_else(|| {
            format!(
                "missing compiled exchange input for table writer at local node {}",
                id.index()
            )
        })?;
        if !receiver.hash_partition_slots.is_empty() {
            let partitions = build.pipeline.dop.max(1) as usize;
            build = shuffle_compiled_group_input_slots(
                build,
                ctx,
                node_id,
                receiver.hash_partition_slots.to_vec(),
                partitions,
            );
        }
    }
    build.pipeline.factories.push(Box::new(factory));
    build.stream = StreamDesc::any(build.pipeline.dop);
    Ok(build)
}

pub(super) fn build_table_finish<H: CompiledSchemaMetadataScope>(
    program: &Arc<LocalProgram>,
    id: ProgramNodeId,
    node_id: i32,
    inputs: &[ProgramNodeId],
    ctx: &mut PipelineBuildContext,
    error: &Arc<RuntimeErrorState>,
    metadata: &mut CompiledMetadataMode<'_, H>,
) -> ExecutionResult<PipelineBuildResult> {
    let binding = ctx.compiled_writers.finisher(id).ok_or_else(|| {
        format!(
            "missing Task finish binding for compiled table finish at local node {}",
            id.index()
        )
    })?;
    let factory = compiled_table_finish_factory(program, id, node_id, binding, error)?;
    let [input] = inputs else {
        return Err(format!(
            "compiled table finish at local node {} reads more than one writer input",
            id.index()
        )
        .into());
    };
    let build = build_node(program, *input, ctx, error, metadata)?;
    let mut build = gather_to_one(build, ctx, node_id);
    build.pipeline.factories.push(Box::new(factory));
    build.stream = StreamDesc::single();
    Ok(build)
}
