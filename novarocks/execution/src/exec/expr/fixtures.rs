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

//! Execution fixtures whose original inputs go through the native fragment compiler.
use crate::exec::chunk::{Chunk, ChunkSchema};
use arrow::{
    array::{BooleanArray, Int64Array},
    record_batch::RecordBatch,
};
use novarocks_local_compiler::LocalCompileOptions;
use novarocks_local_program::{
    KernelAbiVersion, LocalProgram, ProgramNodeId, ProgramNodeKind, ProgramRootRef,
};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::{num::NonZeroUsize, sync::Arc};

mod frame;
mod seed;
pub(crate) use frame::{author, call, compile_checked_fragment_with_parameters};
pub(crate) use seed::{RandArgument, program};

struct FixtureControl;
impl PureCompileControl for FixtureControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

fn options(dop: usize) -> LocalCompileOptions {
    LocalCompileOptions {
        pipeline_dop: NonZeroUsize::new(dop).unwrap(),
        root_sink_dop: Some(NonZeroUsize::new(1).unwrap()),
        kernel_abi: KernelAbiVersion::CURRENT,
        // Explicit fixture admission; these values are not production defaults.
        constants: novarocks_functions::ConstantValidation::bounded(
            novarocks_functions::ConstantPolicy {
                max_rows: 16,
                max_array_nodes: 128,
                max_logical_elements: 1024,
                max_retained_buffer_bytes: 1 << 20,
                max_type_depth: 64,
                max_type_nodes: 4096,
                max_dictionary_depth: 64,
                max_metadata_bytes: 1 << 20,
                max_library_validation_work: 1 << 20,
                max_library_validation_bytes: 1 << 20,
            },
        ),
    }
}

// These explicit ceilings belong to the checked fixture source only.
fn package_admission() -> novarocks_physical_plan::FragmentPackageAdmission {
    novarocks_physical_plan::FragmentPackageAdmission {
        plan_limits: novarocks_physical_plan::PlanLimits::FROZEN,
        source_retained_bytes: 64 * 1024 * 1024,
        property_projection_limits: novarocks_physical_plan::PropertyProofProjectionLimits {
            max_request_bytes: 16 * 1024 * 1024,
            max_coexisting_bytes: 256 * 1024 * 1024,
            max_projection_work: 16 * 1024 * 1024,
        },
    }
}

pub(crate) fn input_chunk(program: &LocalProgram, values: Vec<Option<i64>>) -> Chunk {
    let rows = values.len();
    let (input, _) = input_project(program);
    let layout = program.graph().nodes()[input.index()].output_layout();
    let batch = RecordBatch::try_new(
        Arc::clone(layout.schema()),
        vec![
            Arc::new(Int64Array::from(vec![999; rows])),
            Arc::new(Int64Array::from(values)),
            Arc::new(BooleanArray::from(vec![true; rows])),
        ],
    )
    .unwrap();
    Chunk::try_new_with_chunk_schema(batch, ChunkSchema::from_compiled_layout(layout).unwrap())
        .unwrap()
}

pub(crate) fn input_project(program: &LocalProgram) -> (ProgramNodeId, &[ProgramRootRef]) {
    let nodes = program.graph().nodes();
    let ProgramNodeKind::Limit { input: project, .. } =
        nodes[program.graph().root().index()].kind()
    else {
        panic!("the input fixture publishes its Project through a Limit")
    };
    let ProgramNodeKind::Project { input, exprs, .. } = nodes[project.index()].kind() else {
        panic!("the input fixture's Limit consumes its original Project")
    };
    (*input, exprs)
}
