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

//! Frontend-owned DELETE statement recognition and application routing.

use crate::dml::error::DmlError;
use crate::dml::service::DmlService;
use crate::query_execution::dml::delete::DeleteStatement;
use novarocks_query_application::sql::dml_admission::DmlAdmissionError;

impl DmlService {
    /// Validate the parser-owned shape before forming a governed write action.
    pub(crate) fn admit_delete_shape(
        &self,
        statement: DeleteStatement<'_>,
        source: &str,
    ) -> Result<(), DmlError> {
        if let DeleteStatement::Predicate(delete) = statement
            && delete.selection.is_none()
        {
            return Err(DmlError::admit(DmlAdmissionError::DeleteRequiresWhere.to_user_error(
                source,
                delete.span,
                "DELETE requires a WHERE clause; for full table replacement use INSERT OVERWRITE t SELECT * FROM t WHERE FALSE",
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod execution_error_tests {
    use crate::dml::error::DmlExecutionError;
    use crate::query_execution::contract::DistributedQueryError;
    use novarocks_failure_contract::FailureProjection;
    use novarocks_plan_codec::PhysicalEncodeError;
    #[test]
    fn delete_governed_error_projection_preserves_typed_runtime_identity() {
        for source in [
            PhysicalEncodeError::Invalid("unsupported diagnostic".into()),
            PhysicalEncodeError::UnsupportedCapability("unsupported diagnostic".into()),
        ] {
            let expected = source.failure_fact();
            let error =
                DmlExecutionError::from_distributed(DistributedQueryError::from_encode(source))
                    .into_dml_error(Some("DELETE FROM orders WHERE id = 1"));
            assert_eq!(error.execution_failure().unwrap().fact(), expected);
            assert_eq!(error.engine_error_code(), None);
            assert_eq!(
                error.to_string(),
                format!(
                    "Executor: native attempt initialization failed in its owning domain (reason {})",
                    expected.reason(),
                ),
            );
        }
    }
}
