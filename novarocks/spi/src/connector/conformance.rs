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

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use super::{
    ConnectorBatchBudget, ConnectorBatchReader, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionBindingKey, ConnectorExternalFenceRequest, ConnectorExternalOperationFence,
    ConnectorHistoricalWriteDescriptor, ConnectorHistoricalWriteObservation,
    ConnectorHistoricalWriteRecovery, ConnectorMetadata, ConnectorRequestContext,
    ConnectorTableObjectBinding, ConnectorTableObjectBindingFailure,
    ConnectorTableObjectCaptureRequest, ConnectorTableObjectRebindRequest, ConnectorWriteControl,
};

pub fn assert_batch_reader_contract(
    reader: &mut dyn ConnectorBatchReader,
    expected_schema: &SchemaRef,
    budget: ConnectorBatchBudget,
) -> Result<Vec<RecordBatch>, ConnectorError> {
    let result = read_batches(reader, expected_schema, budget);
    let close_error = close_idempotently(reader).err();
    match (result, close_error) {
        (Ok(batches), None) => Ok(batches),
        (Ok(_), Some(error)) => Err(error),
        (Err(primary), None) => Err(primary),
        (Err(primary), Some(cleanup)) => Err(primary.with_cleanup_context(cleanup.to_string())),
    }
}

fn read_batches(
    reader: &mut dyn ConnectorBatchReader,
    expected_schema: &SchemaRef,
    budget: ConnectorBatchBudget,
) -> Result<Vec<RecordBatch>, ConnectorError> {
    let mut batches = Vec::new();
    while let Some(batch) = reader.next_batch()? {
        validate_batch(&batch, expected_schema, budget)?;
        batches.push(batch);
    }
    if reader.next_batch()?.is_some() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "connector reader returned a batch after end of stream",
        ));
    }
    Ok(batches)
}

fn validate_batch(
    batch: &RecordBatch,
    expected_schema: &SchemaRef,
    budget: ConnectorBatchBudget,
) -> Result<(), ConnectorError> {
    if batch.schema().as_ref() != expected_schema.as_ref() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "connector reader batch schema differs from its declared output schema",
        ));
    }
    if batch.num_rows() > budget.max_rows.get() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector reader batch exceeds the row budget",
        ));
    }
    if batch.get_array_memory_size() > budget.max_bytes.get() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "connector reader batch exceeds the byte budget",
        ));
    }
    Ok(())
}

fn close_idempotently(reader: &mut dyn ConnectorBatchReader) -> Result<(), ConnectorError> {
    reader.close()?;
    reader.close()
}

/// The two fence generations one external-fence conformance run needs. The
/// `raised` fence must strictly supersede `established` within one authority.
#[derive(Clone)]
pub struct ConnectorExternalFenceConformanceInput {
    pub established: ConnectorExternalOperationFence,
    pub raised: ConnectorExternalOperationFence,
    pub context: ConnectorRequestContext,
}

/// Assert the frozen external write fence contract against a provider.
///
/// This exercises all four fence invariants end to end:
/// 1. establishing one fence returns a receipt bound to exactly that fence;
/// 2. replaying the identical fence is idempotent;
/// 3. a strictly higher generation supersedes the established fence;
/// 4. the superseded generation can no longer be established, and is refused
///    with a typed, non-retryable fence failure that is neither `Unsupported`
///    nor a commit-unknown kind.
pub fn assert_external_write_fence_contract(
    control: &dyn ConnectorWriteControl,
    owner: &ConnectorExecutionBindingKey,
    input: ConnectorExternalFenceConformanceInput,
) -> Result<(), ConnectorError> {
    let ConnectorExternalFenceConformanceInput {
        established,
        raised,
        context,
    } = input;
    if !raised.supersedes(&established)? {
        return Err(contract(
            "external fence conformance input must supply a strictly higher raised fence",
        ));
    }
    let request = |fence: &ConnectorExternalOperationFence| ConnectorExternalFenceRequest {
        owner: owner.clone(),
        fence: fence.clone(),
        context: context.clone(),
    };

    let first = control.establish_external_fence(request(&established))?;
    first.validate()?;
    if !first.matches(&established) {
        return Err(contract(
            "connector external fence receipt does not acknowledge the established fence",
        ));
    }
    let replay = control.establish_external_fence(request(&established))?;
    replay.validate()?;
    if replay != first {
        return Err(contract(
            "connector external fence establishment is not idempotent for one generation",
        ));
    }

    let higher = control.establish_external_fence(request(&raised))?;
    higher.validate()?;
    if !higher.matches(&raised) {
        return Err(contract(
            "connector external fence receipt does not acknowledge the raised fence",
        ));
    }

    match control.establish_external_fence(request(&established)) {
        Ok(_) => Err(contract(
            "connector external fence accepted a superseded generation after a higher fence",
        )),
        Err(error) => assert_typed_fence_conflict(&error),
    }
}

/// Assert that a rejected fenced request carries the frozen typed stale/conflict
/// classification instead of an unknown, unsupported, or retryable result.
pub fn assert_typed_fence_conflict(error: &ConnectorError) -> Result<(), ConnectorError> {
    if error.external_fence_failure().is_none() {
        return Err(contract(
            "a fenced request rejection must carry a typed external fence failure",
        ));
    }
    if error.retryable_before_progress() {
        return Err(contract(
            "an external fence conflict must never be retryable before progress",
        ));
    }
    if matches!(
        error.kind(),
        ConnectorErrorKind::Unsupported
            | ConnectorErrorKind::Unavailable
            | ConnectorErrorKind::Internal
    ) {
        return Err(contract(
            "an external fence conflict must never be downgraded to an unsupported or commit-unknown kind",
        ));
    }
    Ok(())
}

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

/// Assert the frozen historical write recovery contract for one immutable
/// descriptor.
///
/// The observation must answer exactly this descriptor, stay digest sealed,
/// classify the same immutable input identically on replay, and respect the
/// disposition rules: only a proven not-dispatched operation may carry a
/// continuation, only an applied operation may carry finalization facts, and an
/// unresolved operation may not request cleanup.
pub fn assert_historical_write_recovery_contract(
    recovery: &dyn ConnectorHistoricalWriteRecovery,
    descriptor: ConnectorHistoricalWriteDescriptor,
    context: ConnectorRequestContext,
) -> Result<ConnectorHistoricalWriteObservation, ConnectorError> {
    descriptor.validate()?;
    let observation = recovery.inspect(descriptor.clone(), context.clone())?;
    observation.validate_for(&descriptor)?;
    if observation.continuation.is_some() && !observation.disposition.may_continue() {
        return Err(contract(
            "historical write observation carries a continuation for a dispatched disposition",
        ));
    }
    if observation.cleanup_required && !observation.disposition.is_resolved() {
        return Err(contract(
            "historical write observation requests cleanup for an unresolved disposition",
        ));
    }
    let replay = recovery.inspect(descriptor.clone(), context)?;
    replay.validate_for(&descriptor)?;
    if replay.digest() != observation.digest() {
        return Err(contract(
            "historical write inspection is not idempotent for one immutable descriptor",
        ));
    }
    Ok(observation)
}

fn contract(message: &'static str) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use arrow::datatypes::Schema;
    use bytes::Bytes;

    use super::*;
    use crate::connector::{
        ConnectorCancellation, ConnectorClusterIdentity, ConnectorExternalFenceGeneration,
        ConnectorExternalFenceReceipt, ConnectorExternalOperationFence, ConnectorInstanceId,
        ConnectorInstanceIncarnation, ConnectorListTablesRequest, ConnectorMetadata,
        ConnectorNamespaceRequest, ConnectorTableDefinitionFacts, ConnectorTableHandle,
        ConnectorTableIdentity, ConnectorTableMetadata, ConnectorTableObjectId,
        ConnectorTableObjectSelector, ConnectorTablePlanningFacts, ConnectorTableRequest,
        ConnectorTableResolution, ConnectorWriteOperationId, ConnectorWritePlan,
        ConnectorWritePlanningRequest, ConnectorWriteTargetRef,
    };

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            Arc::new(NeverCancelled),
            1024,
            4096,
        )
        .expect("request context")
    }

    fn owner() -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: ConnectorInstanceId::parse("catalog.ice").expect("instance id"),
            incarnation: ConnectorInstanceIncarnation::from_bytes([2; 16]),
        }
    }

    fn fence(epoch: u64) -> ConnectorExternalOperationFence {
        ConnectorExternalOperationFence::try_new(
            ConnectorClusterIdentity::derive("conformance-cluster").expect("cluster identity"),
            ConnectorExternalFenceGeneration::try_new(1, epoch, 1).expect("generation"),
            ConnectorWriteOperationId::from_bytes([3; 16]),
            [4; 16],
            ConnectorTableIdentity {
                instance_id: owner().instance_id,
                namespace: Arc::from("db"),
                table: Arc::from("orders"),
            },
            ConnectorWriteTargetRef::main(),
        )
        .expect("fence")
    }

    /// A minimal fenced control. `downgrade_conflict` models the failure this
    /// conformance assertion exists to catch: reporting a superseded generation
    /// as an unsupported capability instead of a typed fence conflict.
    struct FencedControl {
        key: ConnectorExecutionBindingKey,
        established: Mutex<Option<ConnectorExternalFenceGeneration>>,
        downgrade_conflict: bool,
    }

    impl ConnectorWriteControl for FencedControl {
        fn binding_key(&self) -> &ConnectorExecutionBindingKey {
            &self.key
        }

        fn establish_external_fence(
            &self,
            request: ConnectorExternalFenceRequest,
        ) -> Result<ConnectorExternalFenceReceipt, ConnectorError> {
            request.validate(&self.key)?;
            let mut established = self.established.lock().expect("fence table");
            if let Some(current) = *established
                && request.fence.generation() < current
            {
                return Err(if self.downgrade_conflict {
                    ConnectorError::new(
                        ConnectorErrorKind::Unsupported,
                        "provider laundered a fence conflict",
                    )
                } else {
                    ConnectorError::external_fence(
                        crate::connector::ConnectorExternalFenceFailure::Stale,
                        "fence generation is behind the established fence",
                    )
                });
            }
            *established = Some(request.fence.generation());
            ConnectorExternalFenceReceipt::try_new(
                &request.fence,
                Bytes::from_static(b"fence-marker"),
            )
        }

        fn plan_write(
            &self,
            _request: ConnectorWritePlanningRequest,
        ) -> Result<ConnectorWritePlan, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "conformance control does not plan writes",
            ))
        }

        fn commit(
            &self,
            _request: crate::connector::ConnectorWriteCommitRequest,
        ) -> Result<
            crate::connector::ExternalMutationOutcome<crate::connector::ConnectorWriteReceipt>,
            ConnectorError,
        > {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "conformance control does not commit",
            ))
        }

        fn abort(
            &self,
            _request: crate::connector::ConnectorWriteAbortRequest,
        ) -> Result<crate::connector::ConnectorWriteAbortOutcome, ConnectorError> {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "conformance control does not abort",
            ))
        }

        fn reconcile(
            &self,
            _request: crate::connector::ConnectorWriteReconcileRequest,
        ) -> Result<
            crate::connector::ExternalMutationOutcome<crate::connector::ConnectorWriteReceipt>,
            ConnectorError,
        > {
            Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "conformance control does not reconcile",
            ))
        }
    }

    fn input() -> ConnectorExternalFenceConformanceInput {
        ConnectorExternalFenceConformanceInput {
            established: fence(1),
            raised: fence(2),
            context: context(),
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
    fn fence_conformance_accepts_a_monotonic_fencing_provider() {
        let control = FencedControl {
            key: owner(),
            established: Mutex::new(None),
            downgrade_conflict: false,
        };
        assert_external_write_fence_contract(&control, &owner(), input())
            .expect("a monotonic fencing provider satisfies the contract");
    }

    #[test]
    fn fence_conformance_rejects_a_conflict_downgraded_to_unsupported() {
        let control = FencedControl {
            key: owner(),
            established: Mutex::new(None),
            downgrade_conflict: true,
        };
        let error = assert_external_write_fence_contract(&control, &owner(), input())
            .expect_err("a laundered fence conflict must fail the contract");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn fence_conformance_requires_a_strictly_higher_raised_fence() {
        let control = FencedControl {
            key: owner(),
            established: Mutex::new(None),
            downgrade_conflict: false,
        };
        let error = assert_external_write_fence_contract(
            &control,
            &owner(),
            ConnectorExternalFenceConformanceInput {
                established: fence(2),
                raised: fence(2),
                context: context(),
            },
        )
        .expect_err("an equal raised fence is not a valid conformance input");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn typed_fence_conflict_assertion_rejects_a_retryable_or_untyped_error() {
        assert_typed_fence_conflict(&ConnectorError::external_fence(
            crate::connector::ConnectorExternalFenceFailure::Superseded,
            "superseded",
        ))
        .expect("a typed fence conflict is accepted");
        assert!(
            assert_typed_fence_conflict(&ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "untyped",
            ))
            .is_err()
        );
        assert!(
            assert_typed_fence_conflict(
                &ConnectorError::new(ConnectorErrorKind::Unavailable, "transient")
                    .with_retryable_before_progress(),
            )
            .is_err()
        );
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
