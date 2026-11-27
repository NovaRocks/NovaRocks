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
use crate::common::LiteralValue;
use crate::optimizer::scalar::HashableLiteral;
use novarocks_type_contract::{DecimalOverflowPolicy::OutputNull, FunctionValueType};

use super::{fixtures::*, *};

#[test]
fn evaluator_error_leaves_expression_unchanged_without_planning_error() {
    let mut fixture = Fixture::new();
    let one = fixture.int_literal(1);
    let zero = fixture.int_literal(0);
    assert!(matches!(
        fixture.apply(project(fixture.binary(
            crate::common::BinOp::Div,
            one,
            zero
        ))),
        RewriteResult::Unchanged
    ));
    assert_eq!(fixture.calls(), 1);
}

#[test]
fn called_on_null_comparison_folds_unknown_identity() {
    let mut fixture = Fixture::new();
    let null = fixture.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Null)),
        FunctionValueType::new(DataType::Null, true),
    );
    let equal = fixture.binary_typed(
        crate::common::BinOp::EqForNull,
        null,
        null,
        DataType::Boolean,
    );
    let result = changed(fixture.apply(project(equal)));
    let ScalarNode::Constant(value) = fixture.node(project_expr(&result)) else {
        panic!("expected the selected NULL-safe comparison to fold");
    };
    assert_eq!(value.try_boolean().unwrap(), Some(true));
    assert_eq!(fixture.calls(), 1);
}

#[test]
fn preparation_failures_never_become_optional_fold_misses() {
    let mut fixture = Fixture::new();
    let invalid = fixture.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Int(128))),
        FunctionValueType::new(DataType::Int8, false),
    );
    let root = fixture.intern(
        ScalarNode::Cast {
            cast_mode: None,
            child: invalid,
            target: DataType::Int8,
            decimal_overflow_policy: OutputNull,
        },
        FunctionValueType::new(DataType::Int8, false),
    );
    let failure = FoldConstant
        .apply(project(root), &mut fixture.ctx)
        .unwrap_err();
    assert!(
        matches!(
            failure,
            SqlCompileError::CompilationSource(_) | SqlCompileError::Compilation(_)
        ),
        "{failure:?}"
    );
}

#[test]
fn invalid_frozen_type_is_fatal_in_the_real_fold_rule() {
    let mut fixture = Fixture::new();
    let malformed = fixture.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::String(
            "not a boolean".into(),
        ))),
        FunctionValueType::new(DataType::Boolean, false),
    );
    let root = fixture.intern(
        ScalarNode::Cast {
            cast_mode: None,
            child: malformed,
            target: DataType::Boolean,
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        },
        FunctionValueType::new(DataType::Boolean, false),
    );
    let failure = FoldConstant
        .apply(project(root), &mut fixture.ctx)
        .unwrap_err();
    assert!(
        matches!(
            failure,
            SqlCompileError::CompilationSource(_) | SqlCompileError::Compilation(_)
        ),
        "{failure:?}"
    );
}
