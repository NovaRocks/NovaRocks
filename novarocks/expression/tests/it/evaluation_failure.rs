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

use novarocks_expression::{ExprEvaluationFailure, RequiredExpressionRowError, ScalarCallFailure};
use novarocks_expression_contract::RootOrdinal;
use novarocks_failure_contract::{FailureProjection, FailureRowPosition};
use novarocks_functions::{
    InvocationPreparationFailure, KernelFailure, RowDataError, ScalarInvocationFailure,
    ScalarResourceError, Selection,
};
use std::{convert::Infallible, error::Error};

fn assert_delegates(
    wrapper: &(impl Error + FailureProjection),
    original: &(impl Error + FailureProjection),
) {
    assert_eq!(wrapper.to_string(), original.to_string());
    assert_eq!(wrapper.failure_fact(), original.failure_fact());
    assert_eq!(wrapper.source().unwrap().to_string(), original.to_string());
}

#[test]
fn call_failure_transport_preserves_the_function_cause() {
    let kernel = KernelFailure::Cancelled;
    let source = ScalarResourceError::Arithmetic;
    let call = ScalarCallFailure::<Infallible>::from(kernel.clone());
    assert_eq!(call, ScalarCallFailure::Kernel(kernel.clone()));
    assert_delegates(&call, &kernel);
    assert_eq!(
        ScalarCallFailure::<Infallible>::from(ScalarInvocationFailure::Kernel(kernel.clone())),
        call,
    );
    assert_eq!(
        ScalarCallFailure::<Infallible>::from(InvocationPreparationFailure::Kernel(kernel.clone())),
        call,
    );
    let failure = ExprEvaluationFailure::from(call);
    assert_eq!(failure, ExprEvaluationFailure::Kernel(kernel.clone()));
    assert_delegates(&failure, &kernel);

    let call = ScalarCallFailure::<Infallible>::from(InvocationPreparationFailure::Source(source));
    assert_eq!(call, ScalarCallFailure::Source(source));
    assert_delegates(&call, &source);
    let failure = ExprEvaluationFailure::from(call);
    assert_eq!(failure, ExprEvaluationFailure::Source(source));
    assert_delegates(&failure, &source);
}

#[test]
fn host_refusal_keeps_its_type_and_projection() {
    let cause = ScalarResourceError::SourceModel("test host refusal");
    let call = ScalarCallFailure::Host(cause);
    assert_delegates(&call, &cause);
    assert_eq!(call.source().unwrap().downcast_ref(), Some(&cause));

    let failure = ExprEvaluationFailure::from(call);
    assert_eq!(failure, ExprEvaluationFailure::Host(cause));
    assert_delegates(&failure, &cause);
    assert_eq!(failure.source().unwrap().downcast_ref(), Some(&cause));
}

#[test]
fn required_row_uses_the_original_batch_coordinate() {
    let row = RowDataError::new(1, "required row refused");
    let root = RootOrdinal::new(2);
    let selection = Selection::try_sparse(5, &[0, 4]).unwrap();
    let required = RequiredExpressionRowError::try_new(root, selection, row.clone()).unwrap();
    assert_eq!(required.root(), root);
    assert_eq!(required.error(), &row);
    assert_eq!(required.batch_row(), 4);
    assert_eq!(
        required.failure_fact(),
        row.failure_fact().with_row_position(FailureRowPosition {
            selected_ordinal: 1,
            batch_row: 4,
        }),
    );
    assert_eq!(required.source().unwrap().downcast_ref(), Some(&row));
    let failure = ExprEvaluationFailure::<Infallible>::from(required.clone());
    assert_delegates(&failure, &required);

    assert!(matches!(
        RequiredExpressionRowError::try_new(root, selection, RowDataError::new(2, "outside")),
        Err(KernelFailure::Internal(_)),
    ));
}
