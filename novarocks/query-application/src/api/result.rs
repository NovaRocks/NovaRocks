// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Query-application-owned result delivery contracts.
//!
//! A row result begins with one schema delivery, continues with zero or more
//! exactly sequenced batch deliveries, and ends only with an explicitly
//! acknowledged success EOF. Every delivery is move-only and reports whether
//! the protocol consumer completed, failed, or dropped it. The batch token
//! owns the existing workload-control credit through protocol consumption, so
//! an item queue can never become a second or weaker byte authority.

use std::sync::Arc;

use arrow::{
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use novarocks_execution_contract::ResultPacketSequence;
use novarocks_types::{QueryExecutionId, QueryId, schema::SqlType};
use novarocks_workload_control::{
    LocalResourceAuthority, ResultCredit, ResultCreditReservationError, ResultCreditStage,
    WorkError,
};
use tokio::sync::{mpsc, oneshot, watch};

use super::{QueryExecutionError, QueryExecutionErrorKind};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResultField {
    name: Arc<str>,
    data_type: DataType,
    nullable: bool,
    logical_type: Option<SqlType>,
}

impl ResultField {
    pub fn new(
        name: impl Into<Arc<str>>,
        data_type: DataType,
        nullable: bool,
        logical_type: Option<SqlType>,
    ) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
            logical_type,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub const fn nullable(&self) -> bool {
        self.nullable
    }

    pub const fn logical_type(&self) -> Option<&SqlType> {
        self.logical_type.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResultSchema {
    fields: Arc<[ResultField]>,
}

impl ResultSchema {
    pub fn new(fields: impl Into<Arc<[ResultField]>>) -> Self {
        Self {
            fields: fields.into(),
        }
    }

    pub fn fields(&self) -> &[ResultField] {
        &self.fields
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn arrow_schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(
            self.fields
                .iter()
                .map(|field| Field::new(field.name(), field.data_type().clone(), field.nullable()))
                .collect::<Vec<_>>(),
        ))
    }

    pub(crate) fn accepts(&self, batch: &RecordBatch) -> bool {
        let actual = batch.schema();
        actual.fields().len() == self.fields.len()
            && actual
                .fields()
                .iter()
                .zip(self.fields.iter())
                .all(|(actual, expected)| {
                    actual.name() == expected.name()
                        && actual.data_type() == expected.data_type()
                        && actual.is_nullable() == expected.nullable()
                })
    }
}

pub enum ExecutionOutput {
    Rows(QueryResultStream),
    /// The product owner finalized its effect without a row stream. Commit and
    /// publication evidence remains inside that product-specific lifecycle.
    Completion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResultDeliveryDisposition {
    Completed,
    Failed(QueryExecutionError),
    Dropped,
}

pub(crate) type ResultDeliveryReceipt = oneshot::Receiver<ResultDeliveryDisposition>;

struct DeliverySignal {
    sender: Option<oneshot::Sender<ResultDeliveryDisposition>>,
}

impl DeliverySignal {
    fn channel() -> (Self, ResultDeliveryReceipt) {
        let (sender, receiver) = oneshot::channel();
        (
            Self {
                sender: Some(sender),
            },
            receiver,
        )
    }

    fn finish(&mut self, disposition: ResultDeliveryDisposition) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(disposition);
        }
    }
}

impl Drop for DeliverySignal {
    fn drop(&mut self) {
        self.finish(ResultDeliveryDisposition::Dropped);
    }
}

/// Move-only schema delivery that starts a row result.
pub struct SchemaDelivery {
    query_id: QueryId,
    schema: ResultSchema,
    signal: DeliverySignal,
}

impl SchemaDelivery {
    pub(crate) fn new(query_id: QueryId, schema: ResultSchema) -> (Self, ResultDeliveryReceipt) {
        let (signal, receipt) = DeliverySignal::channel();
        (
            Self {
                query_id,
                schema,
                signal,
            },
            receipt,
        )
    }

    pub const fn query_id(&self) -> QueryId {
        self.query_id
    }

    pub const fn schema(&self) -> &ResultSchema {
        &self.schema
    }

    pub fn complete(mut self) {
        self.signal.finish(ResultDeliveryDisposition::Completed);
    }

    pub fn fail(mut self, error: QueryExecutionError) {
        self.signal.finish(ResultDeliveryDisposition::Failed(error));
    }
}

pub struct BatchDeliveryReservationError {
    error: WorkError,
    delivery: BatchDelivery,
}

impl BatchDeliveryReservationError {
    pub const fn error(&self) -> &WorkError {
        &self.error
    }

    pub fn into_parts(self) -> (WorkError, BatchDelivery) {
        (self.error, self.delivery)
    }
}

impl std::fmt::Debug for BatchDeliveryReservationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BatchDeliveryReservationError")
            .field("error", &self.error)
            .field("execution_id", &self.delivery.execution_id)
            .field("sequence", &self.delivery.sequence)
            .finish()
    }
}

/// Move-only ownership of one decoded batch and its result byte credit.
pub struct BatchDelivery {
    execution_id: QueryExecutionId,
    sequence: ResultPacketSequence,
    batch: Option<RecordBatch>,
    decoded_bytes: u64,
    credit: Option<ResultCredit>,
    signal: DeliverySignal,
}

impl BatchDelivery {
    pub(crate) fn try_new(
        execution_id: QueryExecutionId,
        sequence: ResultPacketSequence,
        batch: RecordBatch,
        credit: ResultCredit,
    ) -> Result<(Self, ResultDeliveryReceipt), QueryExecutionError> {
        let mut batch = Some(batch);
        let mut credit = Some(credit);
        if credit.as_ref().unwrap().stage() != ResultCreditStage::DecodedQueued {
            let actual = credit.as_ref().unwrap().stage();
            return Err(reject_batch(
                batch.take().unwrap(),
                credit.take().unwrap(),
                invalid_result_delivery(format!(
                    "result batch credit must be DecodedQueued, got {actual:?}"
                )),
            ));
        }
        let decoded_bytes = match u64::try_from(batch.as_ref().unwrap().get_array_memory_size()) {
            Ok(bytes) => bytes,
            Err(_) => {
                return Err(reject_batch(
                    batch.take().unwrap(),
                    credit.take().unwrap(),
                    invalid_result_delivery("decoded result batch size does not fit u64"),
                ));
            }
        };
        if decoded_bytes == 0 || credit.as_ref().unwrap().held_bytes() != decoded_bytes {
            let credited = credit.as_ref().unwrap().held_bytes();
            return Err(reject_batch(
                batch.take().unwrap(),
                credit.take().unwrap(),
                invalid_result_delivery(format!(
                    "decoded result batch holds {decoded_bytes} bytes but its credit holds {credited}"
                )),
            ));
        }
        let (signal, receipt) = DeliverySignal::channel();
        Ok((
            Self {
                execution_id,
                sequence,
                batch,
                decoded_bytes,
                credit,
                signal,
            },
            receipt,
        ))
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn sequence(&self) -> ResultPacketSequence {
        self.sequence
    }

    pub fn batch(&self) -> &RecordBatch {
        self.batch
            .as_ref()
            .expect("batch delivery owns its batch before completion")
    }

    pub const fn decoded_bytes(&self) -> u64 {
        self.decoded_bytes
    }

    pub fn credit_stage(&self) -> ResultCreditStage {
        self.credit
            .as_ref()
            .expect("batch delivery owns credit before completion")
            .stage()
    }

    pub fn reserve_protocol(
        mut self,
        authority: &LocalResourceAuthority,
        bytes: u64,
    ) -> Result<Self, BatchDeliveryReservationError> {
        let credit = self
            .credit
            .take()
            .expect("batch delivery owns credit before completion");
        match credit.reserve_protocol(authority, bytes) {
            Ok(credit) => {
                self.credit = Some(credit);
                Ok(self)
            }
            Err(error) => {
                let (error, credit) = reservation_parts(error);
                self.credit = Some(credit);
                Err(BatchDeliveryReservationError {
                    error,
                    delivery: self,
                })
            }
        }
    }

    /// Reserve protocol-output capacity, asynchronously waiting through
    /// temporary local pressure while retaining this delivery and its decoded
    /// credit.
    ///
    /// The wait is attributed to the credit's original work scope and inherits
    /// that scope's cancellation and deadline. Permanent capacity, authority,
    /// lifecycle, and transition failures return the intact delivery so the
    /// adapter can report an explicit failure. Dropping this future drops the
    /// delivery and reports `Dropped` to its logical owner.
    pub async fn reserve_protocol_when_available(
        mut self,
        authority: &LocalResourceAuthority,
        bytes: u64,
    ) -> Result<Self, BatchDeliveryReservationError> {
        let credit = self
            .credit
            .take()
            .expect("batch delivery owns credit while awaiting protocol capacity");
        match credit
            .reserve_protocol_when_available(authority, bytes)
            .await
        {
            Ok(credit) => {
                self.credit = Some(credit);
                Ok(self)
            }
            Err(rejection) => {
                let (error, credit) = rejection.into_parts();
                self.credit = Some(credit);
                Err(BatchDeliveryReservationError {
                    error,
                    delivery: self,
                })
            }
        }
    }

    pub fn begin_protocol_write(mut self, actual_bytes: u64) -> Result<Self, QueryExecutionError> {
        let credit = self
            .credit
            .take()
            .expect("batch delivery owns credit before completion");
        match credit.begin_protocol_write(actual_bytes) {
            Ok(credit) => {
                self.credit = Some(credit);
                Ok(self)
            }
            Err(error) => {
                let (error, credit) = reservation_parts(error);
                drop(self.batch.take());
                drop(credit);
                let error = failed_result_delivery("begin protocol result write", error);
                self.signal
                    .finish(ResultDeliveryDisposition::Failed(error.clone()));
                Err(error)
            }
        }
    }

    /// Release all result credit after actual protocol acceptance.
    pub fn complete(mut self) -> Result<(), QueryExecutionError> {
        // The credit covers the Arrow buffers through their final owner.
        drop(self.batch.take());
        let credit = self
            .credit
            .take()
            .expect("batch delivery owns credit before completion");
        match credit.consume() {
            Ok(()) => {
                self.signal.finish(ResultDeliveryDisposition::Completed);
                Ok(())
            }
            Err(error) => {
                let error = failed_result_delivery("consume protocol result", error);
                self.signal
                    .finish(ResultDeliveryDisposition::Failed(error.clone()));
                Err(error)
            }
        }
    }

    pub fn fail(mut self, error: QueryExecutionError) {
        drop(self.batch.take());
        drop(self.credit.take());
        self.signal.finish(ResultDeliveryDisposition::Failed(error));
    }
}

impl Drop for BatchDelivery {
    fn drop(&mut self) {
        // The payload dies before its capacity becomes available, and both
        // happen before the owner observes Dropped.
        drop(self.batch.take());
        drop(self.credit.take());
        self.signal.finish(ResultDeliveryDisposition::Dropped);
    }
}

/// Successful EOF for one exact execution. Only the logical execution owner
/// may construct this after the attempt has reached stable success.
pub struct EndDelivery {
    execution_id: QueryExecutionId,
    sequence: ResultPacketSequence,
    signal: DeliverySignal,
}

impl EndDelivery {
    pub(crate) fn success_eof(
        execution_id: QueryExecutionId,
        sequence: ResultPacketSequence,
    ) -> (Self, ResultDeliveryReceipt) {
        let (signal, receipt) = DeliverySignal::channel();
        (
            Self {
                execution_id,
                sequence,
                signal,
            },
            receipt,
        )
    }

    pub const fn execution_id(&self) -> QueryExecutionId {
        self.execution_id
    }

    pub const fn sequence(&self) -> ResultPacketSequence {
        self.sequence
    }

    pub fn complete(mut self) {
        self.signal.finish(ResultDeliveryDisposition::Completed);
    }

    pub fn fail(mut self, error: QueryExecutionError) {
        self.signal.finish(ResultDeliveryDisposition::Failed(error));
    }
}

pub enum ResultDelivery {
    Batch(BatchDelivery),
    End(EndDelivery),
}

impl ResultDelivery {
    pub const fn execution_id(&self) -> QueryExecutionId {
        match self {
            Self::Batch(delivery) => delivery.execution_id(),
            Self::End(delivery) => delivery.execution_id(),
        }
    }
}

type StreamMessage = ResultDelivery;

enum StreamEvent {
    Failure(QueryExecutionError),
    Message(Option<StreamMessage>),
}

/// Cloneable observation of a logical failure that must interrupt any current
/// schema or batch protocol write. The protocol adapter retains one view while
/// it owns a delivery, instead of waiting for the next stream item.
#[derive(Clone, Debug)]
pub struct ResultFailureView {
    receiver: watch::Receiver<Option<QueryExecutionError>>,
}

impl ResultFailureView {
    pub fn current(&self) -> Option<QueryExecutionError> {
        self.receiver.borrow().clone()
    }

    pub async fn wait(&mut self) -> QueryExecutionError {
        loop {
            if let Some(error) = self.current() {
                return error;
            }
            if self.receiver.changed().await.is_err() {
                return failed_result_delivery_message(
                    "logical result owner disappeared before success EOF",
                );
            }
        }
    }
}

/// Pure bounded transport for one actor-owned result stream.
///
/// Query identity, attempt eligibility, packet sequencing, schema state, and
/// visibility remain exclusively in the logical execution actor. This value
/// only reserves queue capacity and synchronously transfers an already
/// authorized delivery into that slot.
#[derive(Clone)]
pub(crate) struct QueryResultTransport {
    sender: mpsc::Sender<StreamMessage>,
}

impl std::fmt::Debug for QueryResultTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryResultTransport")
            .field("capacity", &self.sender.capacity())
            .field("closed", &self.sender.is_closed())
            .finish()
    }
}

pub(crate) struct ResultQueuePermit {
    permit: mpsc::OwnedPermit<StreamMessage>,
}

impl QueryResultTransport {
    pub(crate) async fn reserve_owned(&self) -> Result<ResultQueuePermit, QueryExecutionError> {
        self.sender
            .clone()
            .reserve_owned()
            .await
            .map(|permit| ResultQueuePermit { permit })
            .map_err(|_| failed_result_delivery_message("result stream consumer disappeared"))
    }

    pub(crate) fn enqueue(&self, permit: ResultQueuePermit, delivery: ResultDelivery) {
        permit.permit.send(delivery);
    }

    pub(crate) async fn closed(&self) {
        self.sender.closed().await;
    }
}

pub struct QueryResultStream {
    query_id: QueryId,
    schema: Option<SchemaDelivery>,
    receiver: mpsc::Receiver<StreamMessage>,
    failure: Option<ResultFailureView>,
    terminal_seen: bool,
}

impl QueryResultStream {
    pub(crate) fn try_channel(
        query_id: QueryId,
        schema: ResultSchema,
        delivery_capacity: usize,
    ) -> Result<
        (
            QueryResultTransport,
            ResultDeliveryReceipt,
            watch::Sender<Option<QueryExecutionError>>,
            Self,
        ),
        QueryExecutionError,
    > {
        if delivery_capacity == 0 {
            return Err(invalid_result_delivery(
                "result delivery capacity must be nonzero",
            ));
        }
        let (schema_delivery, schema_receipt) = SchemaDelivery::new(query_id, schema.clone());
        let (sender, receiver) = mpsc::channel(delivery_capacity);
        let (failure_sender, failure) = watch::channel(None);
        Ok((
            QueryResultTransport { sender },
            schema_receipt,
            failure_sender,
            Self {
                query_id,
                schema: Some(schema_delivery),
                receiver,
                failure: Some(ResultFailureView { receiver: failure }),
                terminal_seen: false,
            },
        ))
    }

    pub const fn query_id(&self) -> QueryId {
        self.query_id
    }

    pub fn begin_schema(&mut self) -> Option<SchemaDelivery> {
        self.schema.take()
    }

    pub fn failure_view(&self) -> Option<ResultFailureView> {
        self.failure.clone()
    }

    pub async fn next(&mut self) -> Result<Option<ResultDelivery>, QueryExecutionError> {
        if self.schema.is_some() {
            return Err(invalid_result_delivery(
                "result schema must begin before reading result batches",
            ));
        }
        if self.terminal_seen {
            return Ok(None);
        }
        let message = loop {
            let event = if let Some(failure) = self.failure.as_mut() {
                tokio::select! {
                    biased;
                    failure = failure.wait() => StreamEvent::Failure(failure),
                    message = self.receiver.recv() => StreamEvent::Message(message),
                }
            } else {
                StreamEvent::Message(self.receiver.recv().await)
            };
            match event {
                StreamEvent::Failure(error) => {
                    self.failure.take();
                    self.receiver.close();
                    while let Ok(delivery) = self.receiver.try_recv() {
                        drop(delivery);
                    }
                    self.terminal_seen = true;
                    return Err(error);
                }
                StreamEvent::Message(message) => break message,
            }
        };
        match message {
            Some(delivery @ ResultDelivery::Batch(_)) => Ok(Some(delivery)),
            Some(delivery @ ResultDelivery::End(_)) => {
                self.terminal_seen = true;
                Ok(Some(delivery))
            }
            None => {
                self.terminal_seen = true;
                Err(failed_result_delivery_message(
                    "result stream closed before success EOF",
                ))
            }
        }
    }
}

fn reservation_parts(error: ResultCreditReservationError) -> (WorkError, ResultCredit) {
    error.into_parts()
}

fn reject_batch(
    batch: RecordBatch,
    credit: ResultCredit,
    error: QueryExecutionError,
) -> QueryExecutionError {
    // Never make the governed bytes available while their Arrow owner lives.
    drop(batch);
    drop(credit);
    error
}

fn invalid_result_delivery(message: impl Into<Arc<str>>) -> QueryExecutionError {
    QueryExecutionError::new(QueryExecutionErrorKind::InvalidRequest, message)
}

fn failed_result_delivery(context: &str, error: WorkError) -> QueryExecutionError {
    failed_result_delivery_message(format!("{context}: {error}"))
}

fn failed_result_delivery_message(message: impl Into<Arc<str>>) -> QueryExecutionError {
    QueryExecutionError::new(QueryExecutionErrorKind::Failed, message)
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::Int64Array,
        datatypes::{DataType, Field, Schema},
    };
    use novarocks_types::{AttemptId, QueryId};
    use novarocks_workload_control::{
        CancellationReason, ResourceClass, ResourceConfig, WorkClass, WorkRequest, WorkScope,
        WorkloadConfig, WorkloadControl,
    };

    use super::*;

    fn execution_id(attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(17, 23), AttemptId::new(attempt).unwrap()).unwrap()
    }

    fn result_schema() -> ResultSchema {
        ResultSchema::new(vec![ResultField::new(
            "value",
            DataType::Int64,
            false,
            Some(SqlType::BigInt),
        )])
    }

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![11_i64, 13]))],
        )
        .unwrap()
    }

    fn workload() -> WorkloadControl {
        workload_with_limits(1024 * 1024 - 1024, 1024 * 1024 - 1024)
    }

    fn workload_with_limits(data_bytes: u64, per_scope_bytes: u64) -> WorkloadControl {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: data_bytes + 1024,
                control_bytes: 1024,
                per_scope_bytes,
            },
        )
        .unwrap();
        control.mark_ready().unwrap();
        control
    }

    fn decoded_credit(control: &WorkloadControl, scope: &WorkScope, bytes: u64) -> ResultCredit {
        let authority = control.resources();
        authority
            .reserve_result_credit(scope, bytes)
            .unwrap()
            .begin_fetch()
            .unwrap()
            .retain_raw(bytes)
            .unwrap()
            .reserve_decode(&authority, bytes)
            .unwrap()
            .queue_decoded(bytes)
            .unwrap()
    }

    #[tokio::test]
    async fn schema_disposition_distinguishes_complete_failure_and_drop() {
        let id = execution_id(1);

        let (complete, complete_receipt) = SchemaDelivery::new(id.query_id(), result_schema());
        complete.complete();
        assert_eq!(
            complete_receipt.await.unwrap(),
            ResultDeliveryDisposition::Completed
        );

        let expected = QueryExecutionError::new(QueryExecutionErrorKind::Failed, "encode schema");
        let (failed, failed_receipt) = SchemaDelivery::new(id.query_id(), result_schema());
        failed.fail(expected.clone());
        assert_eq!(
            failed_receipt.await.unwrap(),
            ResultDeliveryDisposition::Failed(expected)
        );

        let (dropped, dropped_receipt) = SchemaDelivery::new(id.query_id(), result_schema());
        drop(dropped);
        assert_eq!(
            dropped_receipt.await.unwrap(),
            ResultDeliveryDisposition::Dropped
        );
    }

    #[tokio::test]
    async fn batch_credit_lives_until_protocol_completion() {
        let control = workload();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let batch = batch();
        let bytes = u64::try_from(batch.get_array_memory_size()).unwrap();
        let credit = decoded_credit(&control, &root.owner.scope(), bytes);
        let authority = control.resources();
        let (delivery, receipt) =
            BatchDelivery::try_new(execution_id(1), ResultPacketSequence::new(0), batch, credit)
                .unwrap();
        assert_eq!(
            authority.snapshot().result_credit.decoded_queued_bytes,
            bytes
        );

        let delivery = delivery
            .reserve_protocol_when_available(&authority, bytes)
            .await
            .unwrap();
        let delivery = delivery.begin_protocol_write(bytes).unwrap();
        assert_eq!(
            authority.snapshot().result_credit.protocol_writing_bytes,
            bytes * 2
        );
        delivery.complete().unwrap();

        assert_eq!(receipt.await.unwrap(), ResultDeliveryDisposition::Completed);
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);
        drop(root);
    }

    #[tokio::test]
    async fn protocol_capacity_wait_succeeds_after_capacity_release() {
        let batch = batch();
        let bytes = u64::try_from(batch.get_array_memory_size()).unwrap();
        let control = workload_with_limits(bytes * 3, bytes * 2);
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let blocker_work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let authority = control.resources();
        let credit = decoded_credit(&control, &work.owner.scope(), bytes);
        let blocker = authority
            .reserve(&blocker_work.owner.scope(), bytes * 2, ResourceClass::Data)
            .unwrap();
        let (delivery, receipt) =
            BatchDelivery::try_new(execution_id(2), ResultPacketSequence::new(0), batch, credit)
                .unwrap();

        let mut waiting = Box::pin(delivery.reserve_protocol_when_available(&authority, bytes));
        tokio::select! {
            biased;
            _ = &mut waiting => panic!("protocol reservation must wait for capacity"),
            _ = tokio::task::yield_now() => {}
        }
        assert_eq!(control.snapshot().resource_waiters, 1);

        drop(blocker);
        let delivery = waiting.await.unwrap();
        assert_eq!(delivery.credit_stage(), ResultCreditStage::ProtocolReserved);
        delivery.fail(QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "test completed after capacity release",
        ));
        assert!(matches!(
            receipt.await.unwrap(),
            ResultDeliveryDisposition::Failed(_)
        ));
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);
        drop((work, blocker_work));
    }

    #[tokio::test]
    async fn protocol_capacity_waits_from_one_scope_are_fifo_and_coexist() {
        let first_batch = batch();
        let second_batch = batch();
        let bytes = u64::try_from(first_batch.get_array_memory_size()).unwrap();
        let control = workload_with_limits(bytes * 5, bytes * 4);
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let blocker_work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let authority = control.resources();
        let first_credit = decoded_credit(&control, &work.owner.scope(), bytes);
        let second_credit = decoded_credit(&control, &work.owner.scope(), bytes);
        let mut blocker = authority
            .reserve(&blocker_work.owner.scope(), bytes * 3, ResourceClass::Data)
            .unwrap();
        let (first, first_receipt) = BatchDelivery::try_new(
            execution_id(8),
            ResultPacketSequence::new(0),
            first_batch,
            first_credit,
        )
        .unwrap();
        let (second, second_receipt) = BatchDelivery::try_new(
            execution_id(8),
            ResultPacketSequence::new(1),
            second_batch,
            second_credit,
        )
        .unwrap();

        let mut first = Box::pin(first.reserve_protocol_when_available(&authority, bytes));
        let mut second = Box::pin(second.reserve_protocol_when_available(&authority, bytes));
        tokio::select! {
            biased;
            _ = &mut first => panic!("first protocol reservation must wait for capacity"),
            _ = tokio::task::yield_now() => {}
        }
        tokio::select! {
            biased;
            _ = &mut second => panic!("second protocol reservation must wait for capacity"),
            _ = tokio::task::yield_now() => {}
        }
        assert_eq!(control.snapshot().resource_waiters, 2);

        blocker.release_unused(bytes).unwrap();
        tokio::select! {
            biased;
            _ = &mut second => panic!("later protocol waiter must not bypass the queue head"),
            _ = tokio::task::yield_now() => {}
        }
        let first = first.await.unwrap();
        assert_eq!(control.snapshot().resource_waiters, 1);
        first.fail(QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "release the first FIFO protocol grant",
        ));
        assert!(matches!(
            first_receipt.await.unwrap(),
            ResultDeliveryDisposition::Failed(_)
        ));

        let second = second.await.unwrap();
        assert_eq!(control.snapshot().resource_waiters, 0);
        second.fail(QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "release the second FIFO protocol grant",
        ));
        assert!(matches!(
            second_receipt.await.unwrap(),
            ResultDeliveryDisposition::Failed(_)
        ));
        drop(blocker);
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);
        drop((work, blocker_work));
    }

    #[tokio::test(start_paused = true)]
    async fn protocol_capacity_wait_timeout_is_one_absolute_deadline() {
        let batch = batch();
        let bytes = u64::try_from(batch.get_array_memory_size()).unwrap();
        let control = workload_with_limits(bytes * 3, bytes * 2);
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let blocker_work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let authority = control.resources();
        let credit = decoded_credit(&control, &work.owner.scope(), bytes);
        let blocker = authority
            .reserve(&blocker_work.owner.scope(), bytes * 2, ResourceClass::Data)
            .unwrap();
        let (delivery, receipt) =
            BatchDelivery::try_new(execution_id(9), ResultPacketSequence::new(0), batch, credit)
                .unwrap();
        let mut waiting = Box::pin(delivery.reserve_protocol_when_available(&authority, bytes));
        tokio::select! {
            biased;
            _ = &mut waiting => panic!("protocol reservation must wait for capacity"),
            _ = tokio::task::yield_now() => {}
        }

        for _ in 0..2 {
            tokio::time::advance(std::time::Duration::from_secs(10)).await;
            let signal = authority
                .reserve(&blocker_work.owner.scope(), 1, ResourceClass::Control)
                .unwrap();
            drop(signal);
            tokio::select! {
                biased;
                _ = &mut waiting => panic!("capacity notification must not renew the wait deadline"),
                _ = tokio::task::yield_now() => {}
            }
        }
        tokio::time::advance(std::time::Duration::from_secs(10)).await;
        let rejection = match waiting.await {
            Ok(_) => panic!("protocol capacity wait must expire at its original deadline"),
            Err(rejection) => rejection,
        };
        assert_eq!(rejection.error(), &WorkError::CapacityWaitTimeout);
        let (_, delivery) = rejection.into_parts();
        drop(delivery);
        assert_eq!(receipt.await.unwrap(), ResultDeliveryDisposition::Dropped);
        assert_eq!(control.snapshot().resource_waiters, 0);
        drop(blocker);
        drop((work, blocker_work));
    }

    #[tokio::test]
    async fn protocol_capacity_wait_returns_cancelled_delivery_to_adapter() {
        let batch = batch();
        let bytes = u64::try_from(batch.get_array_memory_size()).unwrap();
        let control = workload_with_limits(bytes * 3, bytes * 2);
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let blocker_work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let authority = control.resources();
        let credit = decoded_credit(&control, &work.owner.scope(), bytes);
        let blocker = authority
            .reserve(&blocker_work.owner.scope(), bytes * 2, ResourceClass::Data)
            .unwrap();
        let (delivery, receipt) =
            BatchDelivery::try_new(execution_id(3), ResultPacketSequence::new(0), batch, credit)
                .unwrap();

        let mut waiting = Box::pin(delivery.reserve_protocol_when_available(&authority, bytes));
        tokio::select! {
            biased;
            _ = &mut waiting => panic!("protocol reservation must wait for capacity"),
            _ = tokio::task::yield_now() => {}
        }
        work.owner.cancel(CancellationReason::Requested);
        let rejection = match waiting.await {
            Ok(_) => panic!("cancelled work must not reserve protocol capacity"),
            Err(rejection) => rejection,
        };
        assert!(matches!(rejection.error(), WorkError::Cancelled(_)));
        let (_, delivery) = rejection.into_parts();
        assert_eq!(delivery.credit_stage(), ResultCreditStage::DecodedQueued);
        assert_eq!(
            authority.snapshot().result_credit.decoded_queued_bytes,
            bytes
        );
        delivery.fail(QueryExecutionError::new(
            QueryExecutionErrorKind::Cancelled,
            "protocol capacity wait cancelled",
        ));
        assert!(matches!(
            receipt.await.unwrap(),
            ResultDeliveryDisposition::Failed(_)
        ));
        drop(blocker);
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);
        drop((work, blocker_work));
    }

    #[tokio::test(start_paused = true)]
    async fn protocol_capacity_wait_uses_the_work_scope_deadline() {
        let batch = batch();
        let bytes = u64::try_from(batch.get_array_memory_size()).unwrap();
        let control = workload_with_limits(bytes * 3, bytes * 2);
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let work = control
            .try_begin_root(WorkRequest {
                class: WorkClass::Query,
                deadline: Some(deadline),
            })
            .unwrap();
        let blocker_work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let authority = control.resources();
        let credit = decoded_credit(&control, &work.owner.scope(), bytes);
        let blocker = authority
            .reserve(&blocker_work.owner.scope(), bytes * 2, ResourceClass::Data)
            .unwrap();
        let (delivery, receipt) =
            BatchDelivery::try_new(execution_id(4), ResultPacketSequence::new(0), batch, credit)
                .unwrap();

        let mut waiting = Box::pin(delivery.reserve_protocol_when_available(&authority, bytes));
        tokio::select! {
            biased;
            _ = &mut waiting => panic!("protocol reservation must wait for capacity"),
            _ = tokio::task::yield_now() => {}
        }
        tokio::time::advance(std::time::Duration::from_secs(5)).await;
        let rejection = match waiting.await {
            Ok(_) => panic!("scope deadline must bound the protocol capacity wait"),
            Err(rejection) => rejection,
        };
        assert_eq!(
            rejection.error(),
            &WorkError::Cancelled(CancellationReason::DeadlineExceeded)
        );
        let (_, delivery) = rejection.into_parts();
        assert_eq!(delivery.credit_stage(), ResultCreditStage::DecodedQueued);
        drop(delivery);
        assert_eq!(receipt.await.unwrap(), ResultDeliveryDisposition::Dropped);
        assert_eq!(control.snapshot().resource_waiters, 0);
        drop(blocker);
        drop((work, blocker_work));
    }

    #[tokio::test]
    async fn unrepresentable_protocol_capacity_fails_without_waiting() {
        let batch = batch();
        let bytes = u64::try_from(batch.get_array_memory_size()).unwrap();
        let control = workload_with_limits(bytes * 3, bytes * 2);
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let authority = control.resources();
        let credit = decoded_credit(&control, &work.owner.scope(), bytes);
        let (delivery, receipt) =
            BatchDelivery::try_new(execution_id(5), ResultPacketSequence::new(0), batch, credit)
                .unwrap();

        let rejection = match delivery
            .reserve_protocol_when_available(&authority, bytes * 2)
            .await
        {
            Ok(_) => panic!("unrepresentable protocol capacity must fail"),
            Err(rejection) => rejection,
        };
        assert_eq!(
            rejection.error(),
            &WorkError::Capacity("unrepresentable protocol allocation")
        );
        assert_eq!(control.snapshot().resource_waiters, 0);
        let (_, delivery) = rejection.into_parts();
        assert_eq!(delivery.credit_stage(), ResultCreditStage::DecodedQueued);
        drop(delivery);
        assert_eq!(receipt.await.unwrap(), ResultDeliveryDisposition::Dropped);
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);
        drop(work);
    }

    #[tokio::test]
    async fn dropping_protocol_capacity_wait_releases_delivery_credit() {
        let batch = batch();
        let bytes = u64::try_from(batch.get_array_memory_size()).unwrap();
        let control = workload_with_limits(bytes * 3, bytes * 2);
        let work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let blocker_work = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let authority = control.resources();
        let credit = decoded_credit(&control, &work.owner.scope(), bytes);
        let blocker = authority
            .reserve(&blocker_work.owner.scope(), bytes * 2, ResourceClass::Data)
            .unwrap();
        let (delivery, receipt) =
            BatchDelivery::try_new(execution_id(6), ResultPacketSequence::new(0), batch, credit)
                .unwrap();

        let mut waiting = Box::pin(delivery.reserve_protocol_when_available(&authority, bytes));
        tokio::select! {
            biased;
            _ = &mut waiting => panic!("protocol reservation must wait for capacity"),
            _ = tokio::task::yield_now() => {}
        }
        assert_eq!(control.snapshot().resource_waiters, 1);
        drop(waiting);

        assert_eq!(receipt.await.unwrap(), ResultDeliveryDisposition::Dropped);
        assert_eq!(control.snapshot().resource_waiters, 0);
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);
        assert_eq!(authority.snapshot().held_bytes(), bytes * 2);
        drop(blocker);
        drop((work, blocker_work));
    }

    #[tokio::test]
    async fn foreign_protocol_capacity_authority_fails_closed() {
        let local = workload();
        let foreign = workload();
        let work = local
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let batch = batch();
        let bytes = u64::try_from(batch.get_array_memory_size()).unwrap();
        let credit = decoded_credit(&local, &work.owner.scope(), bytes);
        let (delivery, receipt) =
            BatchDelivery::try_new(execution_id(7), ResultPacketSequence::new(0), batch, credit)
                .unwrap();

        let rejection = match delivery
            .reserve_protocol_when_available(&foreign.resources(), bytes)
            .await
        {
            Ok(_) => panic!("foreign authority must fail"),
            Err(rejection) => rejection,
        };
        assert!(matches!(rejection.error(), WorkError::ForeignAuthority));
        let (_, delivery) = rejection.into_parts();
        assert_eq!(delivery.credit_stage(), ResultCreditStage::DecodedQueued);
        drop(delivery);
        assert_eq!(receipt.await.unwrap(), ResultDeliveryDisposition::Dropped);
        assert_eq!(local.resources().snapshot().result_credit.held_bytes(), 0);
        drop(work);
    }

    #[tokio::test]
    async fn dropped_batch_releases_credit_before_owner_observes_drop() {
        let control = workload();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let batch = batch();
        let bytes = u64::try_from(batch.get_array_memory_size()).unwrap();
        let credit = decoded_credit(&control, &root.owner.scope(), bytes);
        let authority = control.resources();
        let (delivery, receipt) =
            BatchDelivery::try_new(execution_id(1), ResultPacketSequence::new(0), batch, credit)
                .unwrap();

        drop(delivery);
        assert_eq!(receipt.await.unwrap(), ResultDeliveryDisposition::Dropped);
        assert_eq!(authority.snapshot().result_credit.held_bytes(), 0);
        drop(root);
    }

    #[tokio::test]
    async fn stream_transport_only_moves_actor_authorized_deliveries() {
        let control = workload();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let id = execution_id(2);
        let (transport, schema_receipt, _failure_sender, mut stream) =
            QueryResultStream::try_channel(id.query_id(), result_schema(), 1).unwrap();

        let error = match stream.next().await {
            Err(error) => error,
            Ok(_) => panic!("result batches must not precede the schema"),
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::InvalidRequest);
        let delivered_schema = stream.begin_schema().unwrap();
        assert_eq!(delivered_schema.query_id(), id.query_id());
        delivered_schema.complete();
        assert_eq!(
            schema_receipt.await.unwrap(),
            ResultDeliveryDisposition::Completed
        );

        let batch = batch();
        let bytes = u64::try_from(batch.get_array_memory_size()).unwrap();
        let credit = decoded_credit(&control, &root.owner.scope(), bytes);
        let (delivery, receipt) =
            BatchDelivery::try_new(id, ResultPacketSequence::new(0), batch, credit).unwrap();
        let slot = transport.reserve_owned().await.unwrap();
        transport.enqueue(slot, ResultDelivery::Batch(delivery));
        let ResultDelivery::Batch(delivery) = stream.next().await.unwrap().unwrap() else {
            panic!("expected batch delivery");
        };
        assert_eq!(delivery.execution_id(), id);
        assert_eq!(delivery.sequence(), ResultPacketSequence::new(0));
        delivery.fail(QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "client disconnected",
        ));
        assert!(matches!(
            receipt.await.unwrap(),
            ResultDeliveryDisposition::Failed(_)
        ));

        let (eof, eof_receipt) = EndDelivery::success_eof(id, ResultPacketSequence::new(1));
        let slot = transport.reserve_owned().await.unwrap();
        transport.enqueue(slot, ResultDelivery::End(eof));
        let ResultDelivery::End(eof) = stream.next().await.unwrap().unwrap() else {
            panic!("expected EOF delivery");
        };
        assert_eq!(eof.execution_id(), id);
        assert_eq!(eof.sequence(), ResultPacketSequence::new(1));
        eof.complete();
        assert_eq!(
            eof_receipt.await.unwrap(),
            ResultDeliveryDisposition::Completed
        );
        drop(transport);
        assert!(stream.next().await.unwrap().is_none());
        drop(root);
    }

    #[tokio::test]
    async fn stream_failure_is_terminal_without_success_eof() {
        let id = execution_id(3);
        let (_transport, schema_receipt, failure_sender, mut stream) =
            QueryResultStream::try_channel(id.query_id(), result_schema(), 1).unwrap();
        stream.begin_schema().unwrap().complete();
        assert_eq!(
            schema_receipt.await.unwrap(),
            ResultDeliveryDisposition::Completed
        );

        let expected = QueryExecutionError::new(QueryExecutionErrorKind::Failed, "attempt failed");
        failure_sender.send_replace(Some(expected.clone()));
        let actual = match stream.next().await {
            Err(error) => error,
            Ok(_) => panic!("failed stream must return its terminal error"),
        };
        assert_eq!(actual, expected);
        assert!(stream.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn owner_loss_drops_queued_success_eof() {
        let id = execution_id(4);
        let (transport, schema_receipt, failure_sender, mut stream) =
            QueryResultStream::try_channel(id.query_id(), result_schema(), 1).unwrap();
        stream.begin_schema().unwrap().complete();
        assert_eq!(
            schema_receipt.await.unwrap(),
            ResultDeliveryDisposition::Completed
        );

        let (eof, eof_receipt) = EndDelivery::success_eof(id, ResultPacketSequence::new(0));
        let slot = transport.reserve_owned().await.unwrap();
        transport.enqueue(slot, ResultDelivery::End(eof));
        drop(failure_sender);
        drop(transport);

        let error = match stream.next().await {
            Err(error) => error,
            Ok(_) => panic!("owner loss must preempt a queued success EOF"),
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::Failed);
        assert_eq!(
            eof_receipt.await.unwrap(),
            ResultDeliveryDisposition::Dropped
        );
        assert!(stream.next().await.unwrap().is_none());
    }
}
