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

//! Real catalogue facts at the legacy codec's identity and wire boundaries.
use super::*;
use arrow::datatypes::DataType;
use novarocks_functions::{FunctionBindingRequest, FunctionKind, FunctionValueType};
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::sync::Mutex;

#[derive(Default)]
struct Control {
    seen: Mutex<Vec<(CompilePhase, u32)>>,
    refuse: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut seen = self.seen.lock().unwrap();
        let at = seen.len();
        seen.push((phase, units));
        if let Some((position, cause)) = self.refuse
            && at == position
        {
            return Err(cause);
        }
        Ok(())
    }
}
fn arguments() -> [FunctionArgument; 2] {
    [
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Utf8, false),
            constant: None,
        },
        FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Int64, true),
            constant: None,
        },
    ]
}
fn request(args: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
    FunctionBindingRequest {
        arguments: args,
        expected_result_type: None,
        logical_argument_count: args.len(),
    }
}
fn bound() -> (EngineFunctionCatalog, ResolvedFunctionBinding) {
    let catalogue = novarocks_functions::builtin::catalogue::builtin_engine_function_catalog();
    let bound = catalogue
        .resolve_bound_user(
            "json_object",
            FunctionKind::Scalar,
            request(&arguments()),
            &Control::default(),
        )
        .unwrap();
    assert!(bound.declared_scalar_invocation_data);
    assert_eq!(bound.admitted_execution_abi, None);
    (catalogue.clone(), bound)
}

#[test]
fn c16_p1_json_object_legacy_scratch_reconstructs_fact_without_execution_permission() {
    let (catalogue, original) = bound();
    let mut scratch = original.clone();
    scratch.declared_scalar_invocation_data = false;
    catalogue
        .validate_legacy_bound_observed(&mut scratch, request(&arguments()), &Control::default())
        .unwrap();
    assert_eq!(scratch, original);
    assert_eq!(scratch.admitted_execution_abi, None);
    scratch.declared_scalar_invocation_data = false;
    scratch.logical_argument_count = 0;
    assert!(
        matches!(catalogue.validate_legacy_bound_observed(&mut scratch, request(&arguments()), &Control::default()), Err(novarocks_functions::FunctionBindingError::InvalidBinding(message)) if message.as_ref() == "frozen logical argument count differs from the expression")
    );
    assert!(scratch.declared_scalar_invocation_data);
    assert_eq!(scratch.admitted_execution_abi, None);
}

#[test]
fn c16_p1_json_object_legacy_reconstruction_preserves_each_control_prefix() {
    let (catalogue, original) = bound();
    let baseline = Control::default();
    let mut scratch = original.clone();
    scratch.declared_scalar_invocation_data = false;
    catalogue
        .validate_legacy_bound_observed(&mut scratch, request(&arguments()), &baseline)
        .unwrap();
    let trace = baseline.seen.into_inner().unwrap();
    assert!(!trace.is_empty());
    for position in 0..trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                refuse: Some((position, cause)),
                ..Default::default()
            };
            let mut scratch = original.clone();
            scratch.declared_scalar_invocation_data = false;
            assert_eq!(
                catalogue.validate_legacy_bound_observed(
                    &mut scratch,
                    request(&arguments()),
                    &control
                ),
                Err(novarocks_functions::FunctionBindingError::Control(cause))
            );
            assert_eq!(control.seen.into_inner().unwrap(), trace[..=position]);
            assert_eq!(scratch.admitted_execution_abi, None);
        }
    }
}

#[test]
fn c16_p1_json_object_v1_wire_retains_accurate_logical_identity_refusal() {
    let (catalogue, original) = bound();
    let FunctionResultType::Scalar(result) = &original.selected.result_type else {
        panic!("actual scalar result")
    };
    let metadata = novarocks_physical_plan::LegacyBindingMetadata {
        semantic_parameters: Box::default(),
        volatility: original.semantics.volatility,
        argument_evaluation: original.semantics.argument_evaluation,
        failure_behavior: original.semantics.failure_behavior,
        intrinsic_row_error: original.semantics.intrinsic_row_error,
    };
    let function = novarocks_physical_plan::BoundFunction {
        function_id: original.function_id,
        overload: original.selected.overload,
        kind: original.kind,
        argument_types: original.selected.argument_types,
        result_type: result.clone(),
        legacy_metadata: Some(metadata),
    };
    let control = Control::default();
    assert_eq!(
        validate_bound_function(
            &catalogue,
            &function,
            &arguments(),
            2,
            FunctionResultType::Scalar(result.clone()),
            None,
            &control
        ),
        Err(PhysicalEncodeError::Invalid(
            "bound function result: native wire v1 cannot carry explicit root logical identities"
                .into()
        ))
    );
    assert!(control.seen.lock().unwrap().is_empty());
    // Original wire rejection occurs before catalogue/control reconstruction.
    let mut missing = function;
    missing.legacy_metadata = None;
    assert_eq!(
        validate_bound_function(
            &catalogue,
            &missing,
            &arguments(),
            2,
            FunctionResultType::Scalar(result.clone()),
            None,
            &control
        ),
        Err(PhysicalEncodeError::UnsupportedCapability(
            "native wire v1 requires original legacy binding metadata"
        ))
    );
    assert!(control.seen.into_inner().unwrap().is_empty());
}
