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

use crate::api::{
    EndDelivery, InternalEndDelivery, InternalResultDelivery, InternalSegmentDelivery,
    QueryExecutionError, QueryResultTransport, ResultDelivery, ResultQueuePermit,
    RootSegmentDelivery,
};

/// Queue slots remain tied to the actual closed client/internal channel.
#[derive(Clone, Debug)]
pub(super) enum ResultTransport {
    Client(QueryResultTransport),
    Internal(QueryResultTransport<InternalResultDelivery>),
    UnboundInternal,
}

pub(super) enum ResultSlot {
    Client(ResultQueuePermit),
    Internal(ResultQueuePermit<InternalResultDelivery>),
}

impl ResultTransport {
    pub(super) async fn reserve_owned(&self) -> Result<ResultSlot, QueryExecutionError> {
        match self {
            Self::Client(transport) => transport.reserve_owned().await.map(ResultSlot::Client),
            Self::Internal(transport) => transport.reserve_owned().await.map(ResultSlot::Internal),
            Self::UnboundInternal => Err(super::result_protocol_failure(
                "internal consumer is not bound",
            )),
        }
    }

    pub(super) fn enqueue_segment(
        &self,
        slot: ResultSlot,
        delivery: RootSegmentDelivery,
    ) -> Result<(), QueryExecutionError> {
        match (self, slot) {
            (Self::Client(transport), ResultSlot::Client(slot)) => {
                transport.enqueue(slot, ResultDelivery::Segment(delivery));
            }
            (Self::Internal(transport), ResultSlot::Internal(slot)) => {
                transport.enqueue(
                    slot,
                    InternalResultDelivery::Segment(InternalSegmentDelivery::from_root(delivery)),
                );
            }
            _ => {
                return Err(super::result_protocol_failure(
                    "result slot belongs to another consumer channel",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn enqueue_client_end(
        &self,
        slot: ResultSlot,
        delivery: EndDelivery,
    ) -> Result<(), QueryExecutionError> {
        let (Self::Client(transport), ResultSlot::Client(slot)) = (self, slot) else {
            return Err(super::result_protocol_failure(
                "client EOF has no client delivery slot",
            ));
        };
        transport.enqueue(slot, ResultDelivery::End(delivery));
        Ok(())
    }

    pub(super) fn enqueue_internal_end(
        &self,
        slot: ResultSlot,
        delivery: InternalEndDelivery,
    ) -> Result<(), QueryExecutionError> {
        let (Self::Internal(transport), ResultSlot::Internal(slot)) = (self, slot) else {
            return Err(super::result_protocol_failure(
                "internal End has no internal delivery slot",
            ));
        };
        transport.enqueue(slot, InternalResultDelivery::End(delivery));
        Ok(())
    }

    pub(super) async fn closed(&self) {
        match self {
            Self::Client(transport) => transport.closed().await,
            Self::Internal(transport) => transport.closed().await,
            Self::UnboundInternal => std::future::pending().await,
        }
    }
}
