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

//! Shared test controls and constant admission policy, independent of legacy evaluation.
use novarocks_functions::{ConstantPolicy, KernelEvaluationControl, KernelFailure};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::time::Duration;

/// Parse one owned query before handing test syntax to the compiler.
pub(crate) fn parse_query(sql: &str) -> Box<novarocks_parser::ast::Query> {
    let [novarocks_parser::ast::Statement::Query(query)]: [novarocks_parser::ast::Statement; 1] =
        novarocks_parser::parse(sql)
            .expect("parse test query")
            .try_into()
            .expect("exactly one test statement")
    else {
        panic!("expected a test query");
    };
    Box::new(query)
}

/// Unbounded compile and evaluation control. Waiting returns immediately so a
/// sleeping kernel cannot stall a differential run.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct HarnessControl;

impl PureCompileControl for HarnessControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

impl KernelEvaluationControl for HarnessControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }

    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        Ok(())
    }
}

/// Generous admission limits for harness constants.
pub(crate) fn constant_policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1024,
        max_array_nodes: 4096,
        max_logical_elements: 1_000_000,
        max_retained_buffer_bytes: 4_194_304,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1_048_576,
        max_library_validation_work: 16_777_216,
        max_library_validation_bytes: 16_777_216,
    }
}
