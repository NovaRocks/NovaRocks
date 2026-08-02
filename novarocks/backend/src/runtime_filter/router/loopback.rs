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

use std::collections::BTreeMap;
use std::sync::Arc;

use novarocks::runtime_filter_transition::port::identity::RouteEdgeId;
use novarocks::runtime_filter_transition::port::subscription::{
    ArtifactDelivery, ArtifactDeliveryOutcome, LiveTerminal,
};

pub(crate) struct LoopbackRouter {
    routes: BTreeMap<RouteEdgeId, Arc<dyn ArtifactDelivery>>,
}

impl LoopbackRouter {
    pub(crate) fn new(routes: BTreeMap<RouteEdgeId, Arc<dyn ArtifactDelivery>>) -> Self {
        Self { routes }
    }

    pub(crate) fn contains_route(&self, route_edge_id: RouteEdgeId) -> bool {
        self.routes.contains_key(&route_edge_id)
    }

    pub(crate) fn route(
        &self,
        route_edge_ids: &[RouteEdgeId],
        outcome: &ArtifactDeliveryOutcome,
    ) -> Vec<RouteEdgeId> {
        let deliveries = route_edge_ids
            .iter()
            .filter_map(|route_edge_id| {
                self.routes
                    .get(route_edge_id)
                    .cloned()
                    .map(|delivery| (*route_edge_id, delivery))
            })
            .collect::<Vec<_>>();
        for (route_edge_id, delivery) in &deliveries {
            delivery.deliver(*route_edge_id, outcome.clone());
        }
        deliveries
            .into_iter()
            .map(|(route_edge_id, _)| route_edge_id)
            .collect()
    }

    pub(crate) fn route_live(
        &self,
        route_edge_ids: &[RouteEdgeId],
        outcome: Option<&ArtifactDeliveryOutcome>,
        terminal: Option<LiveTerminal>,
    ) -> Vec<RouteEdgeId> {
        let deliveries = route_edge_ids
            .iter()
            .filter_map(|route_edge_id| {
                self.routes
                    .get(route_edge_id)
                    .cloned()
                    .map(|delivery| (*route_edge_id, delivery))
            })
            .collect::<Vec<_>>();
        for (route_edge_id, delivery) in &deliveries {
            delivery.deliver_live(*route_edge_id, outcome.cloned(), terminal);
        }
        deliveries
            .into_iter()
            .map(|(route_edge_id, _)| route_edge_id)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, Weak};

    use novarocks::runtime_filter_transition::port::identity::RouteEdgeId;
    use novarocks::runtime_filter_transition::port::subscription::{
        ArtifactDelivery, ArtifactDeliveryOutcome, UnavailableReason,
    };

    use super::LoopbackRouter;

    struct ReentrantDelivery {
        router: Mutex<Weak<LoopbackRouter>>,
        terminal_calls: AtomicUsize,
        reentered: AtomicBool,
    }

    impl ArtifactDelivery for ReentrantDelivery {
        fn deliver(&self, _route_edge_id: RouteEdgeId, _outcome: ArtifactDeliveryOutcome) {
            self.terminal_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(router) = self.router.lock().unwrap().upgrade() {
                assert!(router.contains_route(RouteEdgeId::new(1)));
                self.reentered.store(true, Ordering::SeqCst);
            }
        }
    }

    #[test]
    fn reentrant_cancel_delivery_runs_without_router_lock() {
        let delivery = Arc::new(ReentrantDelivery {
            router: Mutex::new(Weak::new()),
            terminal_calls: AtomicUsize::new(0),
            reentered: AtomicBool::new(false),
        });
        let router = Arc::new(LoopbackRouter::new(BTreeMap::from([(
            RouteEdgeId::new(1),
            delivery.clone() as Arc<dyn ArtifactDelivery>,
        )])));
        *delivery.router.lock().unwrap() = Arc::downgrade(&router);
        assert_eq!(
            router.route(&[RouteEdgeId::new(1)], &ArtifactDeliveryOutcome::Cancelled,),
            vec![RouteEdgeId::new(1)]
        );
        assert!(router.contains_route(RouteEdgeId::new(1)));
        assert_eq!(delivery.terminal_calls.load(Ordering::SeqCst), 1);
        assert!(delivery.reentered.load(Ordering::SeqCst));
        assert!(
            router
                .route(
                    &[],
                    &ArtifactDeliveryOutcome::Unavailable(UnavailableReason::RouteUnavailable,),
                )
                .is_empty()
        );
        assert_eq!(delivery.terminal_calls.load(Ordering::SeqCst), 1);
    }
}
