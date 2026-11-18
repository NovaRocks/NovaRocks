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

//! Real SQL and LocalCompiler shape required by the expression CASE observations.
use arrow::datatypes::{DataType, Field};
use novarocks_expression::StaticExprKind;
use novarocks_local_program::ProgramNodeKind;
use novarocks_sql_testkit::{installed_builtin_owner_catalogue, sql_source_with_declared_fields};
use novarocks_test_compile_to_execute::compiler_results::compiler_results_with_declared_read_fields;
use std::sync::Arc;

#[test]
fn fixed_binary_case_roots_preserve_values_and_input_lifetime() {
    let nested = DataType::Struct(
        vec![Arc::new(Field::new(
            "f",
            DataType::FixedSizeBinary(4),
            false,
        ))]
        .into(),
    );
    let fields = [
        Field::new("raw", DataType::FixedSizeBinary(16), false),
        Field::new("st", nested, true),
        Field::new("n", DataType::Int32, false),
    ];
    let source = sql_source_with_declared_fields(
        "SELECT CASE WHEN n=10 THEN raw ELSE NULL END AS raw,CASE WHEN n=10 THEN st ELSE NULL END AS st,n+1 AS next_n FROM fixture",
        &fields,
    );
    let programs = compiler_results_with_declared_read_fields(
        &source,
        &fields,
        &installed_builtin_owner_catalogue(),
    );
    let mut sites = Vec::new();
    for program in programs.into_values() {
        let program = program.expect("real SQL fixed roots compile unchanged");
        for node in program.graph().nodes() {
            if let ProgramNodeKind::Project { input, exprs, .. } = node.kind() {
                let child = &program.graph().nodes()[input.index()];
                if exprs.len() == 3
                    && matches!(child.kind(), ProgramNodeKind::Scan { .. })
                    && child.output_layout().schema().fields().len() == 3
                {
                    assert_eq!(
                        child.output_layout().schema().field(0).data_type(),
                        &DataType::FixedSizeBinary(16)
                    );
                    assert_eq!(
                        child.output_layout().schema().field(1).data_type(),
                        fields[1].data_type()
                    );
                    for &expression in &exprs[..2] {
                        assert!(
                            matches!(
                                program
                                    .unit(expression.unit)
                                    .unwrap()
                                    .definitions()
                                    .node(
                                        program
                                            .unit(expression.unit)
                                            .unwrap()
                                            .root(expression.root)
                                            .unwrap()
                                            .definition
                                    )
                                    .unwrap()
                                    .kind(),
                                StaticExprKind::Case { .. }
                            ),
                            "the real SQL author must preserve both whole CASE roots"
                        );
                    }
                    sites.push(node.local_id().unwrap());
                }
            }
        }
    }
    assert_eq!(
        sites.len(),
        1,
        "use the one genuine scan-fed Project, not another fragment's input"
    );
}
