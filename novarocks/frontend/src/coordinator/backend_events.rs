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

use std::sync::Arc;

use novarocks::query_execution::backend::{
    BackendQueryEvent, BackendQueryEventSink, LiveBackendTarget,
};
use novarocks::query_execution::contract::QueryId;

use super::query_registry::FrontendQueryRegistry;

/// Frontend-owned view used to translate backend lifecycle events into
/// query-wide failure and dispatcher cancellation.
#[derive(Clone)]
pub struct BackendQueryActivity {
    registry: Arc<FrontendQueryRegistry>,
}

impl BackendQueryActivity {
    pub(crate) fn new(registry: Arc<FrontendQueryRegistry>) -> Self {
        Self { registry }
    }

    pub fn backend_lost(&self, backend_idx: usize) -> Vec<QueryId> {
        self.registry
            .backend_failed(backend_idx, format!("backend {backend_idx} lost"))
    }

    pub fn backend_restarted(
        &self,
        backend_idx: usize,
        old_epoch: u64,
        new_epoch: u64,
    ) -> Vec<QueryId> {
        self.registry.backend_restarted(
            backend_idx,
            old_epoch,
            format!("backend {backend_idx} restarted (epoch {old_epoch} -> {new_epoch})"),
        )
    }
}

impl BackendQueryEventSink for BackendQueryActivity {
    fn on_backend_event(&self, event: BackendQueryEvent) {
        match event {
            BackendQueryEvent::Unavailable {
                backend_idx,
                reason,
            } => {
                self.registry.backend_failed(backend_idx, reason);
            }
            BackendQueryEvent::Restarted {
                backend_idx,
                old_epoch,
                new_epoch,
            } => {
                self.backend_restarted(backend_idx, old_epoch, new_epoch);
            }
        }
    }

    fn backend_has_active_queries(&self, backend_idx: usize) -> bool {
        self.registry.backend_has_active_queries(backend_idx)
    }

    fn replace_live_backends(&self, revision: u64, backends: Vec<LiveBackendTarget>) {
        self.registry.replace_live_backends(revision, &backends);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use novarocks::UniqueId;
    use novarocks::query_execution::backend::{
        BackendQueryEvent, BackendQueryEventSink, LiveBackendTarget,
    };
    use novarocks::query_execution::contract::{DistributedQueryIntent, QueryId};
    use novarocks::query_execution::fragment_transport::{
        FetchOutcome, FragmentDispatcher, NativeFragmentEnvelope,
    };

    use super::BackendQueryActivity;
    use crate::coordinator::query_registry::FrontendQueryRegistry;

    #[derive(Default)]
    struct RecordingDispatcher {
        cancellations: Mutex<Vec<(usize, Vec<UniqueId>)>>,
        cancellation_query_ids: Mutex<Vec<QueryId>>,
    }

    impl FragmentDispatcher for RecordingDispatcher {
        fn submit_fragment(
            &self,
            _backend_idx: usize,
            _submission: NativeFragmentEnvelope,
        ) -> Result<(), String> {
            panic!("backend-event test must not submit fragments")
        }

        fn fetch_result(
            &self,
            _backend_idx: usize,
            _finst_id: UniqueId,
            _max_wait_ms: i64,
            _expected_output_schema: Option<
                novarocks::query_execution::fragment_transport::ExpectedOutputSchemaView<'_>,
            >,
        ) -> Result<FetchOutcome, String> {
            panic!("backend-event test must not fetch results")
        }

        fn cancel_fragments(&self, backend_idx: usize, query_id: QueryId, finst_ids: &[UniqueId]) {
            self.cancellation_query_ids.lock().unwrap().push(query_id);
            self.cancellations
                .lock()
                .unwrap()
                .push((backend_idx, finst_ids.to_vec()));
        }

        fn backend_count(&self) -> usize {
            3
        }
    }

    fn query_id(n: i64) -> QueryId {
        QueryId::new(n, n + 100)
    }

    fn finst_id(n: i64) -> UniqueId {
        UniqueId {
            hi: n,
            lo: n + 1_000,
        }
    }

    fn record_completed_attempt(
        registry: &FrontendQueryRegistry,
        query_id: QueryId,
        backend_idx: usize,
        finst_id: UniqueId,
    ) {
        registry
            .record_attempt(query_id, backend_idx, finst_id)
            .unwrap();
        registry.finish_attempt(query_id).unwrap();
    }

    fn backend_activity(registry: Arc<FrontendQueryRegistry>) -> BackendQueryActivity {
        BackendQueryActivity::new(registry)
    }

    #[test]
    fn backend_loss_cancels_all_attempted_instances_for_only_affected_queries_once() {
        let registry = Arc::new(FrontendQueryRegistry::default());
        let affected_dispatcher = Arc::new(RecordingDispatcher::default());
        let unaffected_dispatcher = Arc::new(RecordingDispatcher::default());
        let second_affected_dispatcher = Arc::new(RecordingDispatcher::default());
        let affected_query = query_id(1);
        let unaffected_query = query_id(2);
        let second_affected_query = query_id(3);
        let _affected_guard = registry
            .register(
                affected_query,
                DistributedQueryIntent::Result,
                affected_dispatcher.clone(),
            )
            .unwrap();
        let _unaffected_guard = registry
            .register(
                unaffected_query,
                DistributedQueryIntent::Result,
                unaffected_dispatcher.clone(),
            )
            .unwrap();
        let _second_affected_guard = registry
            .register(
                second_affected_query,
                DistributedQueryIntent::Result,
                second_affected_dispatcher.clone(),
            )
            .unwrap();
        registry
            .set_scheduled_backends(affected_query, &[7, 8])
            .unwrap();
        registry
            .set_scheduled_backends(unaffected_query, &[9])
            .unwrap();
        registry
            .set_scheduled_backends(second_affected_query, &[7, 10])
            .unwrap();
        record_completed_attempt(&registry, affected_query, 7, finst_id(11));
        record_completed_attempt(&registry, affected_query, 8, finst_id(12));
        record_completed_attempt(&registry, unaffected_query, 9, finst_id(21));
        record_completed_attempt(&registry, second_affected_query, 7, finst_id(31));
        record_completed_attempt(&registry, second_affected_query, 10, finst_id(32));
        let activity = backend_activity(Arc::clone(&registry));
        assert!(activity.backend_has_active_queries(7));

        let affected = activity.backend_lost(7);

        assert_eq!(affected, vec![affected_query, second_affected_query]);
        assert_eq!(
            registry.first_failure(affected_query).as_deref(),
            Some("backend 7 lost")
        );
        assert_eq!(
            registry.first_failure(second_affected_query).as_deref(),
            Some("backend 7 lost")
        );
        assert_eq!(registry.first_failure(unaffected_query), None);
        assert_eq!(
            *affected_dispatcher.cancellations.lock().unwrap(),
            vec![(7, vec![finst_id(11)]), (8, vec![finst_id(12)])]
        );
        assert_eq!(
            *affected_dispatcher.cancellation_query_ids.lock().unwrap(),
            vec![affected_query, affected_query]
        );
        assert!(
            unaffected_dispatcher
                .cancellations
                .lock()
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            *second_affected_dispatcher.cancellations.lock().unwrap(),
            vec![(7, vec![finst_id(31)]), (10, vec![finst_id(32)])]
        );
        assert_eq!(
            *second_affected_dispatcher
                .cancellation_query_ids
                .lock()
                .unwrap(),
            vec![second_affected_query, second_affected_query]
        );

        assert!(activity.backend_lost(7).is_empty());
        assert_eq!(
            registry.first_failure(affected_query).as_deref(),
            Some("backend 7 lost")
        );
        assert_eq!(affected_dispatcher.cancellations.lock().unwrap().len(), 2);
        assert_eq!(
            second_affected_dispatcher
                .cancellations
                .lock()
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn duplicate_registration_does_not_replace_the_active_query_owner() {
        let registry = Arc::new(FrontendQueryRegistry::default());
        let original_dispatcher = Arc::new(RecordingDispatcher::default());
        let duplicate_dispatcher = Arc::new(RecordingDispatcher::default());
        let query_id = query_id(4);
        let _guard = registry
            .register(
                query_id,
                DistributedQueryIntent::Result,
                original_dispatcher.clone(),
            )
            .unwrap();
        registry.set_scheduled_backends(query_id, &[7]).unwrap();
        record_completed_attempt(&registry, query_id, 7, finst_id(41));

        let error = match registry.register(
            query_id,
            DistributedQueryIntent::Result,
            duplicate_dispatcher.clone(),
        ) {
            Ok(_) => panic!("duplicate query ids must be rejected"),
            Err(error) => error,
        };
        assert!(error.message().contains("already active"));

        let activity = backend_activity(Arc::clone(&registry));
        assert_eq!(activity.backend_lost(7), vec![query_id]);
        assert_eq!(
            *original_dispatcher.cancellations.lock().unwrap(),
            vec![(7, vec![finst_id(41)])]
        );
        assert!(
            duplicate_dispatcher
                .cancellations
                .lock()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn production_event_sink_maps_backend_restart_to_exact_frontend_cancellation() {
        let registry = Arc::new(FrontendQueryRegistry::default());
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let query_id = query_id(5);
        let _guard = registry
            .register(query_id, DistributedQueryIntent::Result, dispatcher.clone())
            .unwrap();
        registry
            .set_scheduled_backend_ownership(query_id, &[(12, 7)])
            .unwrap();
        record_completed_attempt(&registry, query_id, 12, finst_id(51));
        let activity = backend_activity(registry);

        activity.on_backend_event(BackendQueryEvent::Restarted {
            backend_idx: 12,
            old_epoch: 7,
            new_epoch: 8,
        });

        assert_eq!(
            dispatcher.cancellations.lock().unwrap().as_slice(),
            [(12, vec![finst_id(51)])]
        );
    }

    #[test]
    fn backend_restart_only_cancels_queries_scheduled_on_the_old_generation() {
        let registry = Arc::new(FrontendQueryRegistry::default());
        let old_dispatcher = Arc::new(RecordingDispatcher::default());
        let new_dispatcher = Arc::new(RecordingDispatcher::default());
        let old_query = query_id(6);
        let new_query = query_id(7);
        let endpoint = "127.0.0.1:19070".parse().unwrap();
        let activity = backend_activity(Arc::clone(&registry));
        activity.replace_live_backends(1, vec![LiveBackendTarget::new(12, endpoint, 7)]);
        let _old_guard = registry
            .register(
                old_query,
                DistributedQueryIntent::Result,
                old_dispatcher.clone(),
            )
            .unwrap();
        registry
            .set_scheduled_backend_ownership(old_query, &[(12, 7)])
            .unwrap();
        record_completed_attempt(&registry, old_query, 12, finst_id(61));

        activity.on_backend_event(BackendQueryEvent::Restarted {
            backend_idx: 12,
            old_epoch: 7,
            new_epoch: 8,
        });
        activity.replace_live_backends(2, vec![LiveBackendTarget::new(12, endpoint, 8)]);
        let _new_guard = registry
            .register(
                new_query,
                DistributedQueryIntent::Result,
                new_dispatcher.clone(),
            )
            .unwrap();
        registry
            .set_scheduled_backend_ownership(new_query, &[(12, 8)])
            .unwrap();
        record_completed_attempt(&registry, new_query, 12, finst_id(71));

        assert_eq!(
            registry.first_failure(old_query).as_deref(),
            Some("backend 12 restarted (epoch 7 -> 8)")
        );
        assert_eq!(registry.first_failure(new_query), None);
        assert_eq!(
            old_dispatcher.cancellations.lock().unwrap().as_slice(),
            [(12, vec![finst_id(61)])]
        );
        assert!(
            new_dispatcher.cancellations.lock().unwrap().is_empty(),
            "a query scheduled from the already-published new topology must survive the old-generation restart event"
        );
    }

    #[test]
    fn stale_generation_is_rejected_when_restart_publication_wins_the_ownership_race() {
        let registry = Arc::new(FrontendQueryRegistry::default());
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let query_id = query_id(8);
        let endpoint = "127.0.0.1:19070".parse().unwrap();
        let activity = backend_activity(Arc::clone(&registry));
        activity.replace_live_backends(1, vec![LiveBackendTarget::new(12, endpoint, 7)]);
        let _guard = registry
            .register(query_id, DistributedQueryIntent::Result, dispatcher.clone())
            .unwrap();

        activity.on_backend_event(BackendQueryEvent::Restarted {
            backend_idx: 12,
            old_epoch: 7,
            new_epoch: 8,
        });
        activity.replace_live_backends(2, vec![LiveBackendTarget::new(12, endpoint, 8)]);
        let error = registry
            .set_scheduled_backend_ownership(query_id, &[(12, 7)])
            .expect_err("an old resolved snapshot must not register after restart publication");

        assert!(error.message().contains("generation 7 is stale"), "{error}");
        assert_eq!(
            registry.first_failure(query_id).as_deref(),
            Some("backend topology revision changed from 1 to 2")
        );
        assert!(
            dispatcher.cancellations.lock().unwrap().is_empty(),
            "no fragment was attempted before stale ownership was rejected"
        );
    }

    #[test]
    fn scheduled_backend_loss_is_latched_before_its_submission_attempt() {
        let registry = Arc::new(FrontendQueryRegistry::default());
        let dispatcher = Arc::new(RecordingDispatcher::default());
        let query_id = query_id(9);
        let _guard = registry
            .register(query_id, DistributedQueryIntent::Result, dispatcher.clone())
            .unwrap();
        registry.set_scheduled_backends(query_id, &[7, 8]).unwrap();
        registry.record_attempt(query_id, 7, finst_id(61)).unwrap();
        let activity = backend_activity(Arc::clone(&registry));

        assert_eq!(activity.backend_lost(8), vec![query_id]);
        assert!(
            dispatcher.cancellations.lock().unwrap().is_empty(),
            "cancellation waits for the unary submit outcome"
        );
        registry.finish_attempt(query_id).unwrap();

        assert_eq!(
            dispatcher.cancellations.lock().unwrap().as_slice(),
            [(7, vec![finst_id(61)])],
            "only attempted or unknown-outcome instances are cancelled"
        );
        assert!(
            registry.record_attempt(query_id, 8, finst_id(62)).is_err(),
            "the lost scheduled backend cannot be submitted after the event"
        );
    }
}
