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

//! Borrowed access to the production statistics publication author.
//! This adapter owns the actual decoder retention in the same SessionSlot.

#[path = "publication_fixture/session_close.rs"]
pub(crate) mod session_close;

use super::*;

pub(crate) type PublicationFuture =
    Pin<Box<dyn Future<Output = Result<(), CoreStatisticsAttemptError>> + Send>>;

pub(crate) struct OriginalStatisticsPublication {
    slot: Arc<tokio::sync::Mutex<SessionSlot>>,
}

impl OriginalStatisticsPublication {
    pub(crate) fn new(
        session: Box<dyn AsyncStatisticsCollectionSession>,
        mut ready: ReadyStatisticsComputation,
    ) -> Self {
        let artifacts = std::mem::take(&mut ready.artifacts);
        Self {
            slot: Arc::new(tokio::sync::Mutex::new(SessionSlot {
                session: Some(session),
                artifacts: Some(artifacts),
                ready: Some(ready),
                ..SessionSlot::default()
            })),
        }
    }

    pub(crate) fn start(&self, context: ConnectorRequestContext) -> PublicationFuture {
        // Keep the actual borrowed future in the caller's stable owner slot.
        // There is no parallel publication implementation in this adapter.
        Box::pin(publish_session(self.slot.clone(), context, None))
    }

    pub(crate) async fn publication(&self) -> Option<StatisticsPublicationReady> {
        self.slot.lock().await.publication.clone()
    }

    pub(crate) async fn outcome(&self) -> Option<StatisticsPublicationOutcome> {
        self.slot
            .lock()
            .await
            .publication
            .as_ref()
            .map(FrontendStatisticsAttemptExecutor::publication_ready_outcome)
    }

    pub(crate) async fn take_ready(&self) -> ReadyStatisticsComputation {
        self.slot
            .lock()
            .await
            .ready
            .take()
            .expect("same decoder retention")
    }
}
