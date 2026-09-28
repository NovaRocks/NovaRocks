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

use super::{
    ConnectorError, ConnectorErrorKind, ConnectorMetadata, ConnectorTableObjectBinding,
    ConnectorTableObjectBindingFailure, ConnectorTableObjectCaptureRequest,
    ConnectorTableObjectRebindRequest,
};

/// Capture and immediately rebind one current table object through the same
/// metadata owner.
///
/// The successful rebind must preserve the opaque physical object ID captured
/// from the first observation. This is intentionally limited to the success
/// path: test harnesses that change or remove a table between observations use
/// [`assert_typed_table_object_binding_failure`] for their terminal result.
pub fn assert_current_table_object_binding_contract(
    metadata: &dyn ConnectorMetadata,
    request: ConnectorTableObjectCaptureRequest,
) -> Result<ConnectorTableObjectBinding, ConnectorError> {
    let captured = metadata.capture_table_object_binding(request.clone())?;
    let rebound = metadata.rebind_table_object_binding(ConnectorTableObjectRebindRequest {
        table: request.table,
        expected_object_id: captured.object_id.clone(),
        resolution: request.resolution,
        selector: request.selector,
        context: request.context,
    })?;
    if rebound.object_id != captured.object_id {
        return Err(contract(
            "connector table object rebinding returned a different physical object ID",
        ));
    }
    Ok(captured)
}

/// Assert the terminal classification for a failed object-ID-gated rebind.
///
/// The result is checked structurally rather than by matching provider error
/// messages. A caller may therefore distinguish a renamed/replaced object
/// from a missing object without treating either condition as retryable.
pub fn assert_typed_table_object_binding_failure(
    error: &ConnectorError,
    expected: ConnectorTableObjectBindingFailure,
) -> Result<(), ConnectorError> {
    if error.table_object_binding_failure() != Some(expected) {
        return Err(contract(
            "table object rebinding rejection must carry the expected typed binding failure",
        ));
    }
    if error.retryable_before_progress() {
        return Err(contract(
            "a table object rebinding failure must never be retryable before progress",
        ));
    }
    let expected_kind = match expected {
        ConnectorTableObjectBindingFailure::Replaced => ConnectorErrorKind::InvalidRequest,
        ConnectorTableObjectBindingFailure::Missing => ConnectorErrorKind::NotFound,
    };
    if error.kind() != expected_kind {
        return Err(contract(
            "table object rebinding failure has an invalid carrier error kind",
        ));
    }
    Ok(())
}

/// Assert that a provider that cannot prove a physical table identity rejects
/// the optional object-binding contract explicitly.
pub fn assert_table_object_binding_unsupported(
    error: &ConnectorError,
) -> Result<(), ConnectorError> {
    if error.kind() != ConnectorErrorKind::Unsupported {
        return Err(contract(
            "unsupported table object binding must use the Unsupported error kind",
        ));
    }
    if error.is_table_object_binding_failure() {
        return Err(contract(
            "unsupported table object binding must not be reported as a terminal binding failure",
        ));
    }
    if error.retryable_before_progress() {
        return Err(contract(
            "unsupported table object binding must not be retryable before progress",
        ));
    }
    Ok(())
}

fn contract(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::Schema;
    use bytes::Bytes;

    use super::*;
    use crate::connector::{
        ConnectorInstanceId, ConnectorListTablesRequest, ConnectorMetadata,
        ConnectorNamespaceRequest, ConnectorProviderBindingKey, ConnectorRequestContext,
        ConnectorTableDefinitionFacts, ConnectorTableHandle, ConnectorTableIdentity,
        ConnectorTableMetadata, ConnectorTableObjectId, ConnectorTableObjectSelector,
        ConnectorTablePlanningFacts, ConnectorTableRequest, ConnectorTableResolution,
        ProviderBindingEpoch,
    };

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            crate::connector::ConnectorStopOwner::new().view(),
            1024,
            4096,
        )
        .expect("request context")
    }

    fn owner() -> ConnectorProviderBindingKey {
        ConnectorProviderBindingKey {
            instance_id: ConnectorInstanceId::parse("catalog.ice").expect("instance id"),
            incarnation: ProviderBindingEpoch::from_bytes([2; 16]),
        }
    }

    #[derive(Clone, Copy)]
    enum ObjectBindingOutcome {
        Current,
        Replaced,
        Missing,
        Unsupported,
    }

    struct ObjectBindingMetadata {
        instance_id: ConnectorInstanceId,
        outcome: ObjectBindingOutcome,
    }

    impl ObjectBindingMetadata {
        fn object_id() -> ConnectorTableObjectId {
            ConnectorTableObjectId::try_new(Bytes::from_static(b"stable-table-object"))
                .expect("bounded object ID")
        }

        fn binding(
            table: ConnectorTableIdentity,
            object_id: ConnectorTableObjectId,
        ) -> ConnectorTableObjectBinding {
            ConnectorTableObjectBinding {
                metadata: ConnectorTableMetadata {
                    table: ConnectorTableHandle::try_new(
                        table.instance_id.clone(),
                        Bytes::from_static(b"table-handle"),
                    )
                    .expect("test handle"),
                    identity: table,
                    schema: Arc::new(Schema::empty()),
                    planning_facts: ConnectorTablePlanningFacts::empty(),
                    definition_facts: ConnectorTableDefinitionFacts::empty(),
                    version: None,
                    statistics_data_version: None,
                },
                object_id,
            }
        }
    }

    impl ConnectorMetadata for ObjectBindingMetadata {
        fn instance_id(&self) -> &ConnectorInstanceId {
            &self.instance_id
        }

        fn namespace_exists(
            &self,
            _request: ConnectorNamespaceRequest,
        ) -> Result<bool, ConnectorError> {
            Ok(true)
        }

        fn table_exists(&self, _request: ConnectorTableRequest) -> Result<bool, ConnectorError> {
            Ok(true)
        }

        fn list_tables(
            &self,
            _request: ConnectorListTablesRequest,
        ) -> Result<Vec<ConnectorTableIdentity>, ConnectorError> {
            Ok(Vec::new())
        }

        fn capture_table_object_binding(
            &self,
            request: ConnectorTableObjectCaptureRequest,
        ) -> Result<ConnectorTableObjectBinding, ConnectorError> {
            match self.outcome {
                ObjectBindingOutcome::Unsupported => Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "test provider does not support table object binding",
                )),
                _ => Ok(Self::binding(request.table, Self::object_id())),
            }
        }

        fn rebind_table_object_binding(
            &self,
            request: ConnectorTableObjectRebindRequest,
        ) -> Result<ConnectorTableObjectBinding, ConnectorError> {
            match self.outcome {
                ObjectBindingOutcome::Current => {
                    Ok(Self::binding(request.table, request.expected_object_id))
                }
                ObjectBindingOutcome::Replaced => Err(ConnectorError::table_object_binding(
                    ConnectorTableObjectBindingFailure::Replaced,
                    "test table was replaced",
                )),
                ObjectBindingOutcome::Missing => Err(ConnectorError::table_object_binding(
                    ConnectorTableObjectBindingFailure::Missing,
                    "test table is missing",
                )),
                ObjectBindingOutcome::Unsupported => Err(ConnectorError::new(
                    ConnectorErrorKind::Unsupported,
                    "test provider does not support table object rebinding",
                )),
            }
        }

        fn load_table(
            &self,
            request: ConnectorTableRequest,
        ) -> Result<ConnectorTableMetadata, ConnectorError> {
            Ok(Self::binding(request.table, Self::object_id()).metadata)
        }
    }

    fn object_binding_capture_request() -> ConnectorTableObjectCaptureRequest {
        ConnectorTableObjectCaptureRequest {
            table: ConnectorTableIdentity {
                instance_id: owner().instance_id,
                namespace: Arc::from("db"),
                table: Arc::from("orders"),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            selector: ConnectorTableObjectSelector::Current,
            context: context(),
        }
    }

    fn object_binding_rebind_request() -> ConnectorTableObjectRebindRequest {
        let capture = object_binding_capture_request();
        ConnectorTableObjectRebindRequest {
            table: capture.table,
            expected_object_id: ObjectBindingMetadata::object_id(),
            resolution: capture.resolution,
            selector: capture.selector,
            context: capture.context,
        }
    }

    #[test]
    fn table_object_binding_conformance_accepts_a_stable_rebind() {
        let metadata = ObjectBindingMetadata {
            instance_id: owner().instance_id,
            outcome: ObjectBindingOutcome::Current,
        };
        let captured = assert_current_table_object_binding_contract(
            &metadata,
            object_binding_capture_request(),
        )
        .expect("a current table object must rebind to its captured ID");
        assert_eq!(captured.object_id, ObjectBindingMetadata::object_id());
    }

    #[test]
    fn table_object_binding_conformance_requires_typed_replacement_and_missing_failures() {
        for (outcome, expected) in [
            (
                ObjectBindingOutcome::Replaced,
                ConnectorTableObjectBindingFailure::Replaced,
            ),
            (
                ObjectBindingOutcome::Missing,
                ConnectorTableObjectBindingFailure::Missing,
            ),
        ] {
            let metadata = ObjectBindingMetadata {
                instance_id: owner().instance_id,
                outcome,
            };
            let error = match metadata.rebind_table_object_binding(object_binding_rebind_request())
            {
                Ok(_) => panic!("terminal object binding outcome must not return a table handle"),
                Err(error) => error,
            };
            assert_typed_table_object_binding_failure(&error, expected)
                .expect("replacement and missing outcomes stay typed and terminal");
        }
    }

    #[test]
    fn table_object_binding_conformance_requires_explicit_unsupported() {
        let metadata = ObjectBindingMetadata {
            instance_id: owner().instance_id,
            outcome: ObjectBindingOutcome::Unsupported,
        };
        let capture = match metadata.capture_table_object_binding(object_binding_capture_request())
        {
            Ok(_) => panic!("unsupported binding must not return a table handle"),
            Err(error) => error,
        };
        assert_table_object_binding_unsupported(&capture)
            .expect("unsupported capture remains explicit");

        let rebind = match metadata.rebind_table_object_binding(object_binding_rebind_request()) {
            Ok(_) => panic!("unsupported rebinding must not return a table handle"),
            Err(error) => error,
        };
        assert_table_object_binding_unsupported(&rebind)
            .expect("unsupported rebinding remains explicit");
    }
}
