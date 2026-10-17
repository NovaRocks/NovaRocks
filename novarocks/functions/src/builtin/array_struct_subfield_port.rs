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

//! Operation/control adapter; all values and Cast semantics belong to the core.
use super::array_scalar_diagnostic_source::HostProjectionDiagnostics;
use super::array_struct_subfield_core::{ProjectionOperation, ProjectionPort};
use crate::ScalarInvocationFailure;
use std::fmt;

pub(super) struct ArrayStructSubfieldPort<'work, 'control, 'rows> {
    pub(super) diagnostics: HostProjectionDiagnostics<'work, 'control, 'rows>,
}
impl ProjectionPort for ArrayStructSubfieldPort<'_, '_, '_> {
    type Failure = ScalarInvocationFailure;
    fn diagnostic(&mut self, original: fmt::Arguments<'_>) -> Self::Failure {
        self.diagnostics.diagnostic(original)
    }
    fn arrow_error(&mut self, original: arrow_schema::ArrowError) -> Self::Failure {
        self.diagnostics.arrow_error(original)
    }
    fn step(&mut self) -> Result<(), Self::Failure> {
        self.diagnostics.step()
    }
    fn before_operation(
        &mut self,
        operation: ProjectionOperation<'_>,
    ) -> Result<(), Self::Failure> {
        match operation {
            // Reuse the original exact diagnostic/name String author.
            ProjectionOperation::FieldNameCopy { .. } => {
                self.diagnostics.before_operation(operation)
            }
            // The current runtime retains control observations, not the retired
            // opaque payload measurement gates. No invented Cast budget.
            _ => self.diagnostics.after_operation(),
        }
    }
    fn after_operation(&mut self) -> Result<(), Self::Failure> {
        self.diagnostics.after_operation()
    }
}
