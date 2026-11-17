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

use crate::{BoundFunction, ExprId, ExprKind, UnitError, UnitTypeEditor, ValueType};
use novarocks_type_contract::{CompileCheckpoints, arrow_data_types_exact_borrowed_observed};

impl UnitTypeEditor<'_> {
    pub fn expression_type(
        &mut self,
        id: ExprId,
        expected: &ValueType,
        revised: ValueType,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), UnitError> {
        let result = (|| {
            let actual = self.definition(id).ok_or_else(|| {
                UnitError::association("expression edit names an absent definition")
            })?;
            exact_type(&actual.ty, expected, work)?;
            root_admission(expected, &revised, work)?;
            if matches!(actual.kind, ExprKind::FunctionCall { .. }) {
                return Err(UnitError::association(
                    "call type requires its accompanying canonical signature",
                ));
            }
            self.definitions[id.get() as usize]
                .as_mut()
                .expect("checked definition")
                .ty = revised;
            Ok(())
        })();
        result.map_err(|error: UnitError| error.at_definition(id))
    }
    /// The exact fresh operational request remains the SQL producer's duty.
    /// Signature/output change together; arguments and lexical ownership do not.
    pub fn scalar_signature(
        &mut self,
        id: ExprId,
        expected: &BoundFunction,
        revised: BoundFunction,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), UnitError> {
        let result = (|| {
            let actual = self
                .definition(id)
                .ok_or_else(|| UnitError::association("scalar edit names an absent definition"))?;
            let ExprKind::FunctionCall { function, .. } = &actual.kind else {
                return Err(UnitError::association(
                    "scalar edit names a different lifecycle",
                ));
            };
            if !binding_equal(function, expected, work)? {
                return Err(UnitError::association(
                    "earlier scalar binding differs from its actual definition",
                ));
            }
            signature_identity(expected, &revised, work)?;
            exact_type(&actual.ty, &expected.result_type, work)?;
            root_admission(&expected.result_type, &revised.result_type, work)?;
            let actual = self.definitions[id.get() as usize]
                .as_mut()
                .expect("checked definition");
            actual.ty = revised.result_type.clone();
            let ExprKind::FunctionCall { function, .. } = &mut actual.kind else {
                unreachable!("checked scalar definition")
            };
            *function = revised;
            Ok(())
        })();
        result.map_err(|error: UnitError| error.at_definition(id))
    }
}

fn signature_identity(
    original: &BoundFunction,
    revised: &BoundFunction,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), UnitError> {
    if !identity_equal(original, revised, work)? {
        return Err(UnitError::association(
            "rebind changes call identity or original provenance",
        ));
    }
    for (original, revised) in original
        .argument_types
        .iter()
        .zip(revised.argument_types.iter())
    {
        argument_type(original, revised, false, work)?;
    }

    // This writer does not authorize argument shapes. The sole SQL canonical
    // author and final request/source validators must certify the actual new
    // argument contracts; signature equality alone never supplies that proof.
    Ok(())
}
fn identity_equal(
    a: &BoundFunction,
    b: &BoundFunction,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, UnitError> {
    work.step()?;
    if a.kind != b.kind
        || a.argument_types.len() != b.argument_types.len()
        || !text_equal(a.function_id.as_str(), b.function_id.as_str(), work)?
        || !text_equal(a.overload.as_str(), b.overload.as_str(), work)?
    {
        return Ok(false);
    }
    match (&a.legacy_metadata, &b.legacy_metadata) {
        (None, None) => {}
        (Some(a), Some(b)) => {
            work.step()?;
            if a.volatility != b.volatility
                || a.argument_evaluation != b.argument_evaluation
                || a.failure_behavior != b.failure_behavior
                || a.intrinsic_row_error != b.intrinsic_row_error
                || a.semantic_parameters.len() != b.semantic_parameters.len()
            {
                return Ok(false);
            }
            for (a, b) in a
                .semantic_parameters
                .iter()
                .zip(b.semantic_parameters.iter())
            {
                work.step()?;
                if a != b {
                    return Ok(false);
                }
            }
        }
        _ => return Ok(false),
    }
    Ok(true)
}
fn binding_equal(
    a: &BoundFunction,
    b: &BoundFunction,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, UnitError> {
    if !identity_equal(a, b, work)? {
        return Ok(false);
    }
    exact_type(&a.result_type, &b.result_type, work)?;
    for (a, b) in a.argument_types.iter().zip(b.argument_types.iter()) {
        argument_type(a, b, true, work)?;
    }
    Ok(true)
}
fn argument_type(
    a: &novarocks_type_contract::FunctionArgumentType,
    b: &novarocks_type_contract::FunctionArgumentType,
    exact: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), UnitError> {
    use novarocks_type_contract::FunctionArgumentType;
    work.step()?;
    let compare = if exact { exact_type } else { root_admission };
    match (a, b) {
        (FunctionArgumentType::Value(a), FunctionArgumentType::Value(b)) => compare(a, b, work),
        (
            FunctionArgumentType::Lambda {
                parameter_types: a,
                result_type: ar,
            },
            FunctionArgumentType::Lambda {
                parameter_types: b,
                result_type: br,
            },
        ) => {
            work.step()?;
            if a.len() != b.len() {
                return Err(UnitError::association("rebind changes lambda arity"));
            }
            for (a, b) in a.iter().zip(b.iter()) {
                exact_type(a, b, work)?;
            }
            compare(ar, br, work)
        }
        _ => Err(UnitError::association(
            "rebind changes value/lambda channel role",
        )),
    }
}
fn text_equal(a: &str, b: &str, work: &mut CompileCheckpoints<'_>) -> Result<bool, UnitError> {
    work.step()?;
    if a.len() != b.len() {
        return Ok(false);
    }
    for (a, b) in a.bytes().zip(b.bytes()) {
        work.step()?;
        if a != b {
            return Ok(false);
        }
    }
    Ok(true)
}
fn exact_type(
    a: &ValueType,
    b: &ValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), UnitError> {
    work.step()?;
    if !a.exactly_equals_observed(b, || work.step().map_err(UnitError::from))? {
        return Err(UnitError::association(
            "earlier full type differs from its actual construction definition",
        ));
    }
    Ok(())
}
fn root_admission(
    a: &ValueType,
    b: &ValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), UnitError> {
    work.step()?;
    if (a.nullable() && !b.nullable())
        || !a.same_value_domain_observed(b, || work.step().map_err(UnitError::from))?
        || !arrow_data_types_exact_borrowed_observed::<UnitError>(
            a.data_type(),
            b.data_type(),
            || work.step().map_err(UnitError::from),
        )?
    {
        return Err(UnitError::association(
            "rebind changes facts beyond root nullable admission",
        ));
    }
    Ok(())
}
