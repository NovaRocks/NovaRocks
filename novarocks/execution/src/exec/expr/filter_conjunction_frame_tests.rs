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
//! LocalCompiler control refusal for actual multiple-predicate Filter SQL.

use super::filter_conjunction_actual_sql_compiler_tests::{
    compile_options, source_packages, validate_package,
};
use super::ndv_filter_actual_sql_source_tests::ndv_sql_source;
use super::numeric_unary_ordered_sql_source_tests::installed_builtin_owner_catalogue;
use novarocks_local_compiler::{FragmentCompileError, compile_fragment};
use novarocks_physical_plan::NodeKind;
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::sync::{Arc, Mutex};

const NATIVE: &str = "SELECT k FROM fixture.ndv_null_contract GROUP BY k\nHAVING approx_distinct(v) = 0 AND approx_distinct(CAST(v AS BIGINT)) = 0 ORDER BY k;";
struct CompileStop {
    at: usize,
    cause: CompileControlError,
    trace: Mutex<Vec<(CompilePhase, u32)>>,
}
impl PureCompileControl for CompileStop {
    fn checkpoint(&self, phase: CompilePhase, n: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        if trace.len() == self.at {
            return Err(self.cause.clone());
        }
        trace.push((phase, n));
        Ok(())
    }
}
#[test]
fn filter_conjunction_actual_local_compile_three_causes_sampled_prefix() {
    let source = ndv_sql_source(NATIVE);
    let functions = installed_builtin_owner_catalogue();
    let (fragment, _) = source
        .plan()
        .fragments()
        .iter()
        .find(|(_, fragment)| {
            fragment.nodes().values().any(
                |node| matches!(&node.kind,NodeKind::Filter{predicates} if predicates.len()==2),
            )
        })
        .unwrap();
    let package = source_packages(&source).remove(fragment).unwrap();
    let success = CompileStop {
        at: usize::MAX,
        cause: CompileControlError::Cancelled,
        trace: Mutex::new(Vec::new()),
    };
    compile_fragment(
        validate_package(Arc::clone(&package)),
        &functions,
        compile_options(),
        &success,
    )
    .unwrap();
    let trace = success.trace.into_inner().unwrap();
    assert!(!trace.is_empty());
    // Sample the first, middle, and final callbacks of the actual compiler.
    for index in [0, trace.len() / 2, trace.len() - 1] {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let stop = CompileStop {
                at: index,
                cause: cause.clone(),
                trace: Mutex::new(Vec::new()),
            };
            let result = compile_fragment(
                validate_package(Arc::clone(&package)),
                &functions,
                compile_options(),
                &stop,
            );
            assert!(matches!(result,Err(FragmentCompileError::Control(actual)) if actual==cause));
            assert_eq!(*stop.trace.lock().unwrap(), trace[..index]);
        }
    }
}
