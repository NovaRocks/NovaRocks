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

//! RootResult boundary probes using the original checked package fixture.
use super::*;

pub(crate) fn boundary_program(project: Option<bool>) -> Arc<LocalProgram> {
    let mut builder = FragmentBuilder::new(FragmentId::new(57));
    let values = super::super::family_fixture::values(
        &mut builder,
        VALUES,
        &[FunctionValueType::new(DataType::Int64, false)],
        &[vec![LiteralValue::Int64(7)]],
    );
    let root = if let Some(computed) = project {
        let ty = FunctionValueType::new(DataType::Int64, false);
        let expr = builder
            .add_expression(
                PROJECT,
                ty.clone(),
                if computed {
                    ExprKind::Literal(LiteralValue::Int64(19))
                } else {
                    ExprKind::Value(values[0])
                },
            )
            .unwrap();
        let output = builder
            .add_value(
                ty,
                ValueOrigin::Expr {
                    node: PROJECT,
                    expr,
                },
            )
            .unwrap();
        builder
            .add_project(
                PROJECT,
                VALUES,
                Box::from([(expr, output)]),
                Box::from([output]),
            )
            .unwrap();
        PROJECT
    } else {
        VALUES
    };
    let fragment = builder
        .finish_definition(
            root,
            FragmentSink::RootResult(Box::new(
                novarocks_result_contract::RootOutputContract::new(
                    novarocks_result_contract::RootProfileId::V1,
                    novarocks_result_contract::FrozenRootOutput::CountOnly,
                ),
            )),
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
        )
        .unwrap();
    compile(
        package_with_parameters(
            fragment,
            ConstantPools::empty(),
            SemanticParameters::try_new([]).unwrap(),
        ),
        1,
    )
}

#[test]
fn compiled_root_result_boundary_computes_nonidentity_project_before_validation() {
    let program = boundary_program(Some(true));
    let node = &program.graph().nodes()[program.graph().root().index()];
    assert!(matches!(
        node.kind(),
        ProgramNodeKind::Project {
            validate_final_result_input: false,
            ..
        }
    ));
    assert_eq!(int64_rows(&run(&program)), vec![vec![Some(19)]]);
}

#[test]
fn compiled_root_result_boundary_preserves_identity_project_output() {
    let program = boundary_program(Some(false));
    assert_eq!(int64_rows(&run(&program)), vec![vec![Some(7)]]);
}

#[test]
fn compiled_root_result_boundary_covers_nonproject_values_root() {
    let program = boundary_program(None);
    assert!(matches!(
        program.graph().nodes()[program.graph().root().index()].kind(),
        ProgramNodeKind::Values { .. }
    ));
    assert_eq!(int64_rows(&run(&program)), vec![vec![Some(7)]]);
}

use crate::exec::chunk::{ChunkSchema, ChunkSchemaRef};
use crate::exec::operators::compiled_expression::CompiledProjectProcessorFactory;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::{ExecutionFailureCause, ExecutionResult};
use crate::runtime::preparation_metadata::{CompiledSchemaMetadataScope, ProjectSchemaSite};
use crate::runtime::runtime_state::RuntimeErrorState;
use novarocks_local_program::{ProgramNodeId, StaticLayout};

struct RecordingSchemaScope {
    program: Arc<LocalProgram>,
    expected: ProjectSchemaSite,
    refuse: bool,
    calls: usize,
    bodies: usize,
    output: Option<ChunkSchemaRef>,
}
impl CompiledSchemaMetadataScope for RecordingSchemaScope {
    fn materialize<B>(
        &mut self,
        program: &Arc<LocalProgram>,
        site: ProjectSchemaSite,
        layout: &StaticLayout,
        body: B,
    ) -> ExecutionResult<ChunkSchemaRef>
    where
        B: FnOnce() -> Result<ChunkSchemaRef, String>,
    {
        self.calls += 1;
        assert!(Arc::ptr_eq(program, &self.program));
        assert_eq!(site, self.expected);
        let node = match site {
            ProjectSchemaSite::Project(node) => node,
            ProjectSchemaSite::FinalResult => program.graph().root(),
        };
        assert!(std::ptr::eq(
            layout,
            program.graph().nodes()[node.index()].output_layout(),
        ));
        if self.refuse {
            return Err(novarocks_functions::KernelFailure::ResourceExhausted.into());
        }
        self.bodies += 1;
        let output = body()?;
        let direct = ChunkSchema::from_compiled_layout(layout).unwrap();
        assert_eq!(output.slot_ids(), direct.slot_ids());
        assert_eq!(output.arrow_schema_ref(), direct.arrow_schema_ref());
        self.output = Some(Arc::clone(&output));
        Ok(output)
    }
}

fn schema_scope(program: &Arc<LocalProgram>, site: ProjectSchemaSite) -> RecordingSchemaScope {
    RecordingSchemaScope {
        program: Arc::clone(program),
        expected: site,
        refuse: false,
        calls: 0,
        bodies: 0,
        output: None,
    }
}

#[test]
fn compiled_project_schema_scope_preserves_both_actual_factory_sites() {
    for computed in [false, true] {
        let program = boundary_program(Some(computed));
        let node = program.graph().root();
        let error = Arc::new(RuntimeErrorState::default());
        let mut host = schema_scope(&program, ProjectSchemaSite::Project(node));
        let direct = CompiledProjectProcessorFactory::try_new(
            Arc::clone(&program),
            node,
            Arc::clone(&error),
        )
        .unwrap();
        let hosted = CompiledProjectProcessorFactory::try_new_with_metadata_host(
            Arc::clone(&program),
            node,
            Arc::clone(&error),
            &mut host,
        )
        .unwrap();
        assert_eq!(hosted.name(), direct.name());
        assert_eq!((host.calls, host.bodies), (1, 1));
        assert!(host.output.is_some());

        let mut host = schema_scope(&program, ProjectSchemaSite::FinalResult);
        let direct = CompiledProjectProcessorFactory::try_new_final_result_boundary(
            Arc::clone(&program),
            Arc::clone(&error),
        )
        .unwrap();
        let hosted =
            CompiledProjectProcessorFactory::try_new_final_result_boundary_with_metadata_host(
                program, error, &mut host,
            )
            .unwrap();
        assert_eq!(hosted.name(), direct.name());
        assert_eq!((host.calls, host.bodies), (1, 1));
    }
    // The terminal boundary may borrow a Values root. It does not claim a
    // Project constructor merely because its schema uses the same port.
    let program = boundary_program(None);
    let mut host = schema_scope(&program, ProjectSchemaSite::FinalResult);
    CompiledProjectProcessorFactory::try_new_final_result_boundary_with_metadata_host(
        program,
        Arc::new(RuntimeErrorState::default()),
        &mut host,
    )
    .unwrap();
    assert_eq!((host.calls, host.bodies), (1, 1));
}

#[test]
fn compiled_project_schema_scope_refusal_runs_neither_original_body() {
    let program = boundary_program(Some(true));
    for site in [
        ProjectSchemaSite::Project(program.graph().root()),
        ProjectSchemaSite::FinalResult,
    ] {
        let mut host = schema_scope(&program, site);
        host.refuse = true;
        let error = Arc::new(RuntimeErrorState::default());
        let result = match site {
            ProjectSchemaSite::Project(node) => {
                CompiledProjectProcessorFactory::try_new_with_metadata_host(
                    Arc::clone(&program),
                    node,
                    error,
                    &mut host,
                )
            }
            ProjectSchemaSite::FinalResult => {
                CompiledProjectProcessorFactory::try_new_final_result_boundary_with_metadata_host(
                    Arc::clone(&program),
                    error,
                    &mut host,
                )
            }
        };
        let failure = result
            .err()
            .expect("the host refused before schema construction");
        assert_eq!(
            failure.cause(),
            &ExecutionFailureCause::Kernel(novarocks_functions::KernelFailure::ResourceExhausted)
        );
        assert_eq!((host.calls, host.bodies), (1, 0));
        assert!(host.output.is_none());
    }
}

#[test]
fn compiled_project_schema_scope_keeps_original_early_factory_errors() {
    let program = boundary_program(None);
    for (node, expected) in [
        (program.graph().root(), "compiled node is not a Project"),
        (
            ProgramNodeId::new(usize::MAX),
            "compiled Project node is absent",
        ),
    ] {
        let mut host = schema_scope(&program, ProjectSchemaSite::Project(node));
        let error = Arc::new(RuntimeErrorState::default());
        let direct = CompiledProjectProcessorFactory::try_new(
            Arc::clone(&program),
            node,
            Arc::clone(&error),
        )
        .err()
        .unwrap();
        let failure = CompiledProjectProcessorFactory::try_new_with_metadata_host(
            Arc::clone(&program),
            node,
            error,
            &mut host,
        )
        .err()
        .unwrap();
        assert_eq!(direct, expected);
        assert_eq!(failure.cause(), &ExecutionFailureCause::Pipeline(direct));
        assert_eq!((host.calls, host.bodies), (0, 0));
    }
    let program = crate::exec::expr::compiled_program::tests::program(
        crate::exec::expr::compiled_program::tests::SeedMode::Input,
        false,
    );
    let mut host = schema_scope(&program, ProjectSchemaSite::FinalResult);
    let error = Arc::new(RuntimeErrorState::default());
    let direct = CompiledProjectProcessorFactory::try_new_final_result_boundary(
        Arc::clone(&program),
        Arc::clone(&error),
    )
    .err()
    .unwrap();
    let failure =
        CompiledProjectProcessorFactory::try_new_final_result_boundary_with_metadata_host(
            program, error, &mut host,
        )
        .err()
        .unwrap();
    assert_eq!(
        direct,
        "final result boundary requires its actual RootResult sink"
    );
    assert_eq!(failure.cause(), &ExecutionFailureCause::Pipeline(direct));
    assert_eq!((host.calls, host.bodies), (0, 0));
}
