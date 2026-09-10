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
use tokio::sync::{mpsc, oneshot};

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

    fn accepts(&self, batch: &RecordBatch) -> bool {
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

type StreamMessage = Result<ResultDelivery, QueryExecutionError>;

/// Move-only owner endpoint for one result stream. The channel bounds queued
/// records; the `ResultCredit` inside every batch bounds bytes.
pub(crate) struct QueryResultSink {
    query_id: QueryId,
    schema: ResultSchema,
    schema_receipt: Option<ResultDeliveryReceipt>,
    schema_completed: bool,
    current_execution: Option<QueryExecutionId>,
    next_sequence: ResultPacketSequence,
    sender: mpsc::Sender<StreamMessage>,
}

impl QueryResultSink {
    pub(crate) async fn wait_schema_completed(&mut self) -> Result<(), QueryExecutionError> {
        if self.schema_completed {
            return Ok(());
        }
        let receipt = self.schema_receipt.take().ok_or_else(|| {
            invalid_result_delivery("result schema completion was already observed")
        })?;
        match receipt.await {
            Ok(ResultDeliveryDisposition::Completed) => {
                self.schema_completed = true;
                Ok(())
            }
            Ok(ResultDeliveryDisposition::Failed(error)) => Err(error),
            Ok(ResultDeliveryDisposition::Dropped) => Err(failed_result_delivery_message(
                "result schema was dropped before completion",
            )),
            Err(_) => Err(failed_result_delivery_message(
                "result schema disposition sender disappeared",
            )),
        }
    }

    pub(crate) async fn send_batch(
        &mut self,
        execution_id: QueryExecutionId,
        sequence: ResultPacketSequence,
        batch: RecordBatch,
        credit: ResultCredit,
    ) -> Result<ResultDeliveryReceipt, QueryExecutionError> {
        if !self.schema_completed {
            return Err(reject_batch(
                batch,
                credit,
                invalid_result_delivery("result batch cannot be sent before schema completion"),
            ));
        }
        if let Err(error) = self.validate_execution(execution_id, sequence) {
            return Err(reject_batch(batch, credit, error));
        }
        if sequence != self.next_sequence {
            return Err(reject_batch(
                batch,
                credit,
                invalid_result_delivery(format!(
                    "result batch sequence {} does not match expected {}",
                    sequence.get(),
                    self.next_sequence.get()
                )),
            ));
        }
        if !self.schema.accepts(&batch) {
            return Err(reject_batch(
                batch,
                credit,
                invalid_result_delivery("result batch does not match the fixed result schema"),
            ));
        }
        let next_sequence = match self.next_sequence.next() {
            Some(next) => next,
            None => {
                return Err(reject_batch(
                    batch,
                    credit,
                    failed_result_delivery_message("result packet sequence is exhausted"),
                ));
            }
        };
        let (delivery, receipt) = BatchDelivery::try_new(execution_id, sequence, batch, credit)?;
        if self.current_execution.is_none() {
            self.current_execution = Some(execution_id);
        }
        self.sender
            .send(Ok(ResultDelivery::Batch(delivery)))
            .await
            .map_err(|_| failed_result_delivery_message("result stream consumer disappeared"))?;
        self.next_sequence = next_sequence;
        Ok(receipt)
    }

    pub(crate) async fn send_success_eof(
        self,
        execution_id: QueryExecutionId,
        sequence: ResultPacketSequence,
    ) -> Result<ResultDeliveryReceipt, QueryExecutionError> {
        if !self.schema_completed {
            return Err(invalid_result_delivery(
                "success EOF cannot be sent before schema completion",
            ));
        }
        if execution_id.query_id() != self.query_id {
            return Err(invalid_result_delivery(
                "success EOF belongs to a different logical query",
            ));
        }
        if self
            .current_execution
            .is_some_and(|current| current != execution_id)
        {
            return Err(invalid_result_delivery(
                "success EOF belongs to a different visible execution attempt",
            ));
        }
        if sequence != self.next_sequence {
            return Err(invalid_result_delivery(format!(
                "success EOF sequence {} does not match expected {}",
                sequence.get(),
                self.next_sequence.get()
            )));
        }
        let (delivery, receipt) = EndDelivery::success_eof(execution_id, sequence);
        self.sender
            .send(Ok(ResultDelivery::End(delivery)))
            .await
            .map_err(|_| failed_result_delivery_message("result stream consumer disappeared"))?;
        Ok(receipt)
    }

    pub(crate) async fn fail(self, error: QueryExecutionError) -> Result<(), QueryExecutionError> {
        if !self.schema_completed {
            return Err(invalid_result_delivery(
                "schema delivery must report a pre-schema result failure",
            ));
        }
        self.sender
            .send(Err(error))
            .await
            .map_err(|_| failed_result_delivery_message("result stream consumer disappeared"))
    }

    fn validate_execution(
        &self,
        execution_id: QueryExecutionId,
        sequence: ResultPacketSequence,
    ) -> Result<(), QueryExecutionError> {
        if execution_id.query_id() != self.query_id {
            return Err(invalid_result_delivery(
                "result batch belongs to a different logical query",
            ));
        }
        match self.current_execution {
            Some(current) if current != execution_id => Err(invalid_result_delivery(
                "result batch belongs to a different visible execution attempt",
            )),
            Some(_) => Ok(()),
            None if sequence == ResultPacketSequence::new(0) => Ok(()),
            None => Err(invalid_result_delivery(
                "the first result batch of an execution must have sequence zero",
            )),
        }
    }
}

pub struct QueryResultStream {
    query_id: QueryId,
    schema: Option<SchemaDelivery>,
    receiver: mpsc::Receiver<StreamMessage>,
    terminal_seen: bool,
}

impl QueryResultStream {
    pub(crate) fn try_channel(
        query_id: QueryId,
        schema: ResultSchema,
        delivery_capacity: usize,
    ) -> Result<(QueryResultSink, Self), QueryExecutionError> {
        if delivery_capacity == 0 {
            return Err(invalid_result_delivery(
                "result delivery capacity must be nonzero",
            ));
        }
        let (schema_delivery, schema_receipt) = SchemaDelivery::new(query_id, schema.clone());
        let (sender, receiver) = mpsc::channel(delivery_capacity);
        Ok((
            QueryResultSink {
                query_id,
                schema,
                schema_receipt: Some(schema_receipt),
                schema_completed: false,
                current_execution: None,
                next_sequence: ResultPacketSequence::new(0),
                sender,
            },
            Self {
                query_id,
                schema: Some(schema_delivery),
                receiver,
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

    pub async fn next(&mut self) -> Result<Option<ResultDelivery>, QueryExecutionError> {
        if self.schema.is_some() {
            return Err(invalid_result_delivery(
                "result schema must begin before reading result batches",
            ));
        }
        if self.terminal_seen {
            return Ok(None);
        }
        match self.receiver.recv().await {
            Some(Ok(delivery @ ResultDelivery::Batch(_))) => Ok(Some(delivery)),
            Some(Ok(delivery @ ResultDelivery::End(_))) => {
                self.terminal_seen = true;
                Ok(Some(delivery))
            }
            Some(Err(error)) => {
                self.terminal_seen = true;
                Err(error)
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
        ResourceConfig, WorkClass, WorkRequest, WorkScope, WorkloadConfig, WorkloadControl,
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
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
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

        let delivery = delivery.reserve_protocol(&authority, bytes).unwrap();
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
    async fn stream_requires_schema_exact_sequence_and_success_eof() {
        let control = workload();
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let id = execution_id(2);
        let (mut sink, mut stream) =
            QueryResultStream::try_channel(id.query_id(), result_schema(), 1).unwrap();

        let error = match stream.next().await {
            Err(error) => error,
            Ok(_) => panic!("result batches must not precede the schema"),
        };
        assert_eq!(error.kind(), QueryExecutionErrorKind::InvalidRequest);
        let delivered_schema = stream.begin_schema().unwrap();
        assert_eq!(delivered_schema.query_id(), id.query_id());
        delivered_schema.complete();
        sink.wait_schema_completed().await.unwrap();

        let batch = batch();
        let bytes = u64::try_from(batch.get_array_memory_size()).unwrap();
        let credit = decoded_credit(&control, &root.owner.scope(), bytes);
        let receipt = sink
            .send_batch(id, ResultPacketSequence::new(0), batch, credit)
            .await
            .unwrap();
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

        let eof_receipt = sink
            .send_success_eof(id, ResultPacketSequence::new(1))
            .await
            .unwrap();
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
        assert!(stream.next().await.unwrap().is_none());
        drop(root);
    }

    #[tokio::test]
    async fn stream_failure_is_terminal_without_success_eof() {
        let id = execution_id(3);
        let (mut sink, mut stream) =
            QueryResultStream::try_channel(id.query_id(), result_schema(), 1).unwrap();
        stream.begin_schema().unwrap().complete();
        sink.wait_schema_completed().await.unwrap();

        let expected = QueryExecutionError::new(QueryExecutionErrorKind::Failed, "attempt failed");
        sink.fail(expected.clone()).await.unwrap();
        let actual = match stream.next().await {
            Err(error) => error,
            Ok(_) => panic!("failed stream must return its terminal error"),
        };
        assert_eq!(actual, expected);
        assert!(stream.next().await.unwrap().is_none());
    }
}
