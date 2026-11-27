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

//! Original relational call requests for fixtures that publish through the
//! observed plan builder. Each request is authored from the site's own frozen
//! binding; nothing is recovered from selected results, names or occurrences.

use crate::{
    Fragment, FrozenCallError, FunctionArgumentType, PhysicalCallBinding, PhysicalCallRequest,
    StaticFunctionArgument,
};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};

/// Attach one original request per relational call site of `fragment`, keeping
/// any expression requests it already carries.
///
/// An aggregate call (including a TopN grouped-state or writer call) requests
/// its original logical arguments followed by its ORDER channels, keyed by its
/// position in the node's call list. That holds in every phase: a merging
/// phase still requests the logical argument types, never its state type. A
/// table function requests its bound arguments. No argument is a constant.
pub(crate) fn with_original_relational_requests(
    fragment: Fragment,
    control: &dyn PureCompileControl,
) -> Fragment {
    let mut entries = fragment
        .call_requests()
        .entries()
        .iter()
        .map(|(definition, request)| (*definition, request.clone()))
        .collect::<Vec<_>>();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate).unwrap();
    crate::visit_relational_calls_observed::<FrozenCallError>(
        &fragment,
        &mut work,
        |site, binding, _| {
            let (argument_types, logical_argument_count) = match binding {
                PhysicalCallBinding::Aggregate(binding) => (
                    &*binding.function.argument_types,
                    binding.logical_argument_count as usize,
                ),
                PhysicalCallBinding::Table(function) => {
                    (&*function.argument_types, function.argument_types.len())
                }
                PhysicalCallBinding::Window {
                    function,
                    aggregate,
                } => (
                    &*function.argument_types,
                    aggregate.map_or(function.argument_types.len(), |binding| {
                        binding.logical_argument_count as usize
                    }),
                ),
            };
            let arguments = argument_types
                .iter()
                .map(|argument| match argument {
                    FunctionArgumentType::Value(value_type) => StaticFunctionArgument::Value {
                        value_type: value_type.clone(),
                        constant: None,
                    },
                    FunctionArgumentType::Lambda { .. }
                    | FunctionArgumentType::CandidateGroup { .. } => {
                        panic!("relational fixture calls take Value arguments")
                    }
                })
                .collect();
            entries.push((
                site,
                PhysicalCallRequest {
                    arguments,
                    logical_argument_count,
                    expected_result_type: None,
                },
            ));
            Ok(())
        },
    )
    .unwrap();
    work.finish().unwrap();
    fragment
        .with_call_requests_observed(entries, control)
        .unwrap()
}
