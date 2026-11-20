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

//! Pure compilation and evaluation of self-contained scalar expression units.

mod definitions;
pub use definitions::*;
mod creation;
pub use creation::ProgramCreationFailure as ExprConstructionFailure;
mod compiler;
pub use compiler::{ExprCompileError, ExprCompileFailure};
mod call_authorship;
pub use call_authorship::{
    CallAuthorError, CallAuthorInputs, CapabilityRefusal, InvariantRefusal, UnitCallAuthorship,
    author_call_controls, author_unit_calls,
};
mod original_requests;
mod primitives;
pub use primitives::{
    ComparisonSite, ProgramPrimitiveError as PrimitiveError, prepare_arithmetic_node,
    prepare_cast_node, prepare_comparison_node, prepare_native_bitnot_node,
    prepare_native_negate_node, prepare_null_safe_comparison_node,
};
pub mod typing;
mod scopes;
pub use scopes::*;
mod compiled;
pub use compiled::*;
mod failure;
use compiled::CompiledParts;

pub(crate) use creation::ProgramCreationFailure;

mod evaluator;
pub use evaluator::{
    CompiledExpressionInstance, CompiledFilterConjunctionInstance, EvaluationInput,
    ExprEvaluationFailure, InputColumnProjection, InputSourceStorage, InputStorageFailure,
    RequiredExpressionRowError, ScalarCallFailure, ScalarCallHost, ScalarCallPhase,
    ScalarCallRequestFacts,
};

#[cfg(any(test, novarocks_dev_controls))]
pub mod testkit;
