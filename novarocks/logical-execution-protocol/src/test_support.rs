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

use crate::{
    LogicalRequestToken, LogicalSourceId,
    input::InternalEndReceiptId,
    start::{LogicalPlanIdentity, ObservationBudgets, RootCarrier},
};
use novarocks_coordination_contract::{
    AttemptActivationIdentity, IdSeed, MonotonicInstant, OperationIds,
};
use novarocks_execution_contract::{TaskIdentity, TaskOperationId, root_result::RootResultEnd};
use novarocks_result_contract::RootOutputKind;
use novarocks_types::{AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId};
use std::{num::NonZeroU64, time::Duration};
pub fn operation(seed: u128) -> TaskOperationId {
    OperationIds::new(IdSeed::new(seed, 100)).issue()
}
pub fn activation(attempt: u64) -> AttemptActivationIdentity {
    AttemptActivationIdentity::from_parts(
        71,
        QueryExecutionId::new(QueryId::new(17, 19), AttemptId::new(attempt).unwrap()).unwrap(),
        11 + attempt,
    )
}
pub fn root(attempt: u64) -> TaskIdentity {
    TaskIdentity::new(
        activation(attempt).execution(),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::try_from_bytes(operation(23).to_bytes()).unwrap(),
    )
}
pub fn end(packets: u64, rows: u64) -> RootResultEnd {
    RootResultEnd {
        sequence: NonZeroU64::new(packets + 1).unwrap(),
        output_rows: rows,
    }
}
pub fn source(n: u64) -> LogicalSourceId {
    LogicalSourceId::new(NonZeroU64::new(n).unwrap())
}
pub fn token(n: u64) -> LogicalRequestToken {
    LogicalRequestToken::new(NonZeroU64::new(n).unwrap())
}
pub fn receipt(n: u64) -> InternalEndReceiptId {
    InternalEndReceiptId(NonZeroU64::new(n).unwrap())
}
pub fn now(millis: u64) -> MonotonicInstant {
    MonotonicInstant::from_origin(Duration::from_millis(millis))
}
pub const PLAN: LogicalPlanIdentity = LogicalPlanIdentity([97; 16]);
pub fn budgets() -> ObservationBudgets {
    ObservationBudgets {
        diagnostics: Duration::from_millis(20),
        convergence: Duration::from_millis(90),
    }
}
pub fn carrier() -> RootCarrier {
    RootCarrier {
        kind: RootOutputKind::ClientRows,
        client_rows: Some(novarocks_result_contract::ClientRowProfile::try_new(16, 64).unwrap()),
    }
}
