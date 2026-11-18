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

//! Only the original statistics session's borrowed publication-close call.
//! This is not FrontendCompletionCustody close or the full close_session path.

use super::*;
use novarocks_spi::connector::ConnectorError;

pub(crate) type SessionPublicationCloseFuture =
    Pin<Box<dyn Future<Output = Result<StatisticsPublicationClose, ConnectorError>> + Send>>;

impl OriginalStatisticsPublication {
    pub(crate) fn start_session_publication_close(
        &self,
        context: ConnectorRequestContext,
    ) -> SessionPublicationCloseFuture {
        let slot = self.slot.clone();
        Box::pin(async move {
            let mut original = slot.lock().await;
            let session = original.session.as_mut().expect("same statistics session");
            match session.close_publication_async(&context).await {
                Ok(observation) => {
                    original.close = Some(observation.clone());
                    Ok(observation)
                }
                Err(error) => {
                    original.close_failure = Some(error.clone());
                    Err(error)
                }
            }
        })
    }
}
