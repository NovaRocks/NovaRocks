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

//! One original Provider enumeration in the installed AP split pump.
//! The gate holds its real CPU call; it is neither a protocol conclusion nor
//! a receipt that a BE accepted a SplitAssignment update.
use super::*;
use crate::query_execution::{
    split_assignment::{AssignmentTarget, RoundSplitSource, ScanNodeKey, SupervisedSplitSource},
    split_assignment_round::RoundSplitAssignmentPlan,
};
use crate::task_execution::blocking_io::ConnectorBlockingIoResponsibility;
use novarocks_spi::connector::{self as spi, read_stack as read};

#[derive(Default)]
pub(crate) struct OriginalSplitObservation {
    pub(crate) call: Arc<ReadCloseGate>,
    pub(crate) closes: AtomicUsize,
    pub(crate) drops: AtomicUsize,
}
struct OriginalSource {
    observed: Arc<OriginalSplitObservation>,
    finished: bool,
}
impl read::ConnectorReadSplitSource for OriginalSource {
    fn next_batch(
        &mut self,
        max_size: usize,
        _: &read::ConnectorReadDynamicFilterSnapshot,
    ) -> Result<read::ConnectorSplitBatch<read::ConnectorReadSplit>, spi::ConnectorError> {
        assert!(max_size > 0);
        self.observed.call.wait();
        self.finished = true;
        Ok(read::ConnectorSplitBatch::new(Vec::new(), true))
    }
    fn is_finished(&self) -> bool {
        self.finished
    }
    fn close(&mut self) -> Result<(), spi::ConnectorError> {
        assert_eq!(self.observed.closes.fetch_add(1, Ordering::AcqRel), 0);
        Ok(())
    }
}
impl Drop for OriginalSource {
    fn drop(&mut self) {
        self.observed.drops.fetch_add(1, Ordering::AcqRel);
    }
}
struct EmptySourceEncoder;
impl spi::ConnectorReadWireEncoder for EmptySourceEncoder {
    fn owner(&self) -> &str {
        "original-empty-split-source"
    }
    fn encode_relation_payload(
        &self,
        _: &read::ConnectorReadRelation,
    ) -> Result<spi::ConnectorReadRelationPayload, spi::ConnectorCodecError> {
        unreachable!("the original empty source has no relation payload")
    }
    fn encode_column_payload(
        &self,
        _: &read::ConnectorReadColumnHandle,
    ) -> Result<spi::ConnectorEncodedPayload, spi::ConnectorCodecError> {
        unreachable!("the original empty source has no column payload")
    }
    fn encode_transaction_payload(
        &self,
        _: &read::ConnectorReadTransactionHandle,
    ) -> Result<spi::ConnectorEncodedPayload, spi::ConnectorCodecError> {
        unreachable!("the original empty source has no transaction payload")
    }
    fn encode_split_payload(
        &self,
        _: &read::ConnectorReadSplit,
    ) -> Result<spi::ConnectorReadSplitPayload, spi::ConnectorCodecError> {
        unreachable!("the original empty source emits no split payload")
    }
}
pub(super) async fn original_plan(
    root: TaskIdentity,
    fragment: u32,
    task_key: novarocks_types::UniqueId,
    scope: &WorkScope,
    window: &ResultWindowAlias,
    data: &FrontendDataRuntime,
    observed: Arc<OriginalSplitObservation>,
) -> RoundSplitAssignmentPlan {
    // Bound parents obtain their same-class child through the original joint
    // request/receipt author. No LP input-bank loan or metadata lock is held
    // while this actual admission future waits.
    let mut admission = ConnectorBlockingIoResponsibility::request(scope, window).unwrap();
    let responsibility = admission.ready().await.unwrap();
    let scan = ScanNodeKey::new(fragment, 7);
    let backing = responsibility.retain_backing();
    let source = RoundSplitSource {
        scan,
        source: SupervisedSplitSource::new(
            Box::new(OriginalSource {
                observed,
                finished: false,
            }),
            7,
            responsibility,
        ),
        encoder: Arc::new(EmptySourceEncoder),
        feedback: Arc::new(
            crate::runtime_filter::feedback::RuntimeFilterFeedbackState::new(
                root.query_execution_id(),
                Default::default(),
            )
            .unwrap(),
        ),
        feedback_bindings: Vec::new(),
        initial_wait_initialized: false,
        initial_wait_deadline: None,
        _backing: Some(backing),
    };
    RoundSplitAssignmentPlan::new(
        BTreeMap::from([(
            scan,
            vec![AssignmentTarget {
                backend_idx: 0,
                task_key,
            }],
        )]),
        vec![source],
        Default::default(),
        Duration::ZERO,
        data.connector_blocking_io().clone(),
    )
}
