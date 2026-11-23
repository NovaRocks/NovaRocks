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

use std::fmt;

use crate::{
    CompileCheckpoints, CompileControlError, ExecutionValueTypeError, FunctionValueType,
    ValueConstructionFailure, ValueConstructionRequest, ValueTypeStorageLifetime,
};

#[derive(Debug)]
pub enum AssignmentCommonTypeError {
    Control(CompileControlError),
    Construction(ExecutionValueTypeError),
    IncompatibleShape(&'static str),
}

impl From<CompileControlError> for AssignmentCommonTypeError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}

impl fmt::Display for AssignmentCommonTypeError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(output),
            Self::Construction(error) => error.fmt(output),
            Self::IncompatibleShape(message) => output.write_str(message),
        }
    }
}

impl std::error::Error for AssignmentCommonTypeError {}

/// Select the common expression declaration for CASE, VALUES and sets.
/// The same Trino type rules also determine applicable scalar overloads.
pub fn assignment_common_value_type(
    left: &FunctionValueType,
    right: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<FunctionValueType, AssignmentCommonTypeError> {
    let result = crate::trino_common_type_with_construction(
        left,
        right,
        ValueTypeStorageLifetime::pure(),
        Some(&mut |request| match request {
            ValueConstructionRequest::Work(units) => {
                for _ in 0..units {
                    work.step()?;
                }
                Ok(())
            }
            _ => work.flush(),
        }),
    );
    match result {
        Ok(Some(value)) => Ok(value),
        Ok(None) => Err(AssignmentCommonTypeError::IncompatibleShape(
            "expressions do not have a common Trino type",
        )),
        Err(ValueConstructionFailure::Observer(error)) => {
            Err(AssignmentCommonTypeError::Control(error))
        }
        Err(ValueConstructionFailure::ValueType(error)) => {
            Err(AssignmentCommonTypeError::Construction(error))
        }
        Err(_) => Err(AssignmentCommonTypeError::Control(
            CompileControlError::ResourceExhausted,
        )),
    }
}
