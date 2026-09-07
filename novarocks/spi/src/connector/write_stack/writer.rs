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

//! Backend-local writer execution contract.
//!
//! One pipeline driver's asynchronous owner task exclusively owns exactly one
//! [`ConnectorBatchWriter`] for its whole lifetime: it opens it, appends to it,
//! and either finishes or aborts it. No writer is shared between drivers, so
//! the append path holds no cross-driver lock and there is no "last driver
//! finishes" protocol.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::connector::write_stack::runtime::{ConnectorCommitFragment, ConnectorWriterHandle};
use crate::connector::write_stack::target::WriteTargetOrdinal;
use crate::connector::{CatalogHandle, ConnectorError, ConnectorRequestContext};

/// Where, in this exact execution attempt, a writer is running.
///
/// A provider may use these facts for attempt-local output naming, logging,
/// metrics, and failure localization. They are never an external commit
/// authority, never part of a commit fragment's identity, and never a recovery
/// token: losing or replaying them cannot change what was or was not committed.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ConnectorWriterPhysicalContext {
    execution_query_id: [u8; 16],
    execution_attempt_id: u64,
    fragment_instance_id: [u8; 16],
    driver_id: u32,
    writer_ordinal: u32,
}

impl ConnectorWriterPhysicalContext {
    pub const fn new(
        execution_query_id: [u8; 16],
        execution_attempt_id: u64,
        fragment_instance_id: [u8; 16],
        driver_id: u32,
        writer_ordinal: u32,
    ) -> Self {
        Self {
            execution_query_id,
            execution_attempt_id,
            fragment_instance_id,
            driver_id,
            writer_ordinal,
        }
    }

    pub const fn execution_query_id(&self) -> [u8; 16] {
        self.execution_query_id
    }

    pub const fn execution_attempt_id(&self) -> u64 {
        self.execution_attempt_id
    }

    pub const fn fragment_instance_id(&self) -> [u8; 16] {
        self.fragment_instance_id
    }

    pub const fn driver_id(&self) -> u32 {
        self.driver_id
    }

    pub const fn writer_ordinal(&self) -> u32 {
        self.writer_ordinal
    }
}

/// Everything one driver needs to open its own writer.
pub struct ConnectorOpenWriterRequest {
    pub handle: ConnectorWriterHandle,
    pub target: WriteTargetOrdinal,
    pub expected_schema: SchemaRef,
    pub physical: ConnectorWriterPhysicalContext,
    pub context: ConnectorRequestContext,
}

/// A driver-local asynchronous writer.
///
/// Every lifecycle future is polled by the writer's single-owner I/O task, never
/// by the pipeline driver. `finish` returns zero or more independent commit fragments — one per written
/// artifact — instead of a single opaque report document. A writer that fails
/// any step must leave no committed external effect; the frontend is the only
/// owner of external commit.
#[async_trait::async_trait]
pub trait ConnectorBatchWriter: Send {
    async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError>;

    /// Close this writer and describe every artifact it staged. A writer that
    /// staged nothing returns an empty vector; that is a legal outcome and is
    /// not the same as a failure.
    async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError>;

    /// Best-effort local cleanup after a failure or cancellation. It never
    /// reaches external catalog metadata.
    async fn abort(&mut self) -> Result<(), ConnectorError>;
}

/// The backend-local write capability of one exact catalog generation.
///
/// It can open writers and nothing else. There is deliberately no begin,
/// finish, abort, or reconcile here: a backend never holds a commit handle and
/// never mutates catalog metadata.
#[async_trait::async_trait]
pub trait ConnectorWriteExecution: Send + Sync {
    fn catalog_handle(&self) -> &CatalogHandle;

    /// Construct one driver-local writer without publishing or staging any
    /// external effect before returning it. The future must be cancellation
    /// safe: dropping it may abandon construction and there is no writer value
    /// available to receive `abort`. All staged effects begin in lifecycle
    /// calls on the returned writer and are cleaned by that writer's `abort`.
    async fn open_writer(
        &self,
        request: ConnectorOpenWriterRequest,
    ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError>;
}

/// Build the backend write capability for one frozen catalog generation.
pub trait ConnectorWriteExecutionFactory: Send + Sync {
    fn build(
        &self,
        properties: &crate::connector::CatalogProperties,
    ) -> Result<Arc<dyn ConnectorWriteExecution>, ConnectorError>;
}
