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

//! Test-only: a page stream driven to its end on a test runtime, the way a
//! host polls it, with one refilled turn budget per poll.

use futures::StreamExt;
use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::{
    ConnectorPollBudget, ConnectorPreparationProgress, OwnedConnectorPageStream, PageSourceMetrics,
    SourcePage,
};

/// Budget units of one host turn in these tests.
const TURN_BUDGET: u64 = 1024;

/// A page stream read to its end one page at a time.
pub(crate) struct DrivenStream<'r> {
    runtime: &'r tokio::runtime::Runtime,
    stream: Option<OwnedConnectorPageStream>,
    budget: ConnectorPollBudget,
    ended: bool,
}

impl<'r> DrivenStream<'r> {
    pub(crate) fn new(
        runtime: &'r tokio::runtime::Runtime,
        stream: OwnedConnectorPageStream,
        budget: ConnectorPollBudget,
    ) -> Self {
        Self {
            runtime,
            stream: Some(stream),
            budget,
            ended: false,
        }
    }

    /// The next page; `None` once the stream ended, failed or was closed.
    pub(crate) fn next_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
        let Some(stream) = self.stream.as_mut() else {
            return Ok(None);
        };
        if self.ended {
            return Ok(None);
        }
        self.budget.refill(TURN_BUDGET);
        match self.runtime.block_on(stream.next()) {
            Some(Ok(page)) => Ok(Some(page)),
            Some(Err(error)) => {
                // A host polls a failed stream no more.
                self.ended = true;
                Err(error)
            }
            None => {
                self.ended = true;
                Ok(None)
            }
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.ended || self.stream.is_none()
    }

    pub(crate) fn metrics(&self) -> PageSourceMetrics {
        self.stream
            .as_ref()
            .map_or_else(PageSourceMetrics::default, |stream| stream.metrics())
    }

    pub(crate) fn advance_successor_preparation(
        &mut self,
        remaining_input_bytes: u64,
        remaining_candidates: usize,
    ) -> Result<ConnectorPreparationProgress, ConnectorError> {
        match self.stream.as_mut() {
            Some(stream) => stream
                .as_mut()
                .advance_successor_preparation(remaining_input_bytes, remaining_candidates),
            None => Ok(ConnectorPreparationProgress::Deferred),
        }
    }

    pub(crate) fn successor_preparation_input_bytes(&self) -> u64 {
        self.stream
            .as_ref()
            .map_or(0, |stream| stream.successor_preparation_input_bytes())
    }

    /// Closes the stream and waits for its exit; idempotent.
    pub(crate) fn close(&mut self) -> Result<(), ConnectorError> {
        match self.stream.take() {
            Some(stream) => self.runtime.block_on(stream.close()),
            None => Ok(()),
        }
    }
}
