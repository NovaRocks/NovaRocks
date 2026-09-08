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

//! Where one attempt's frozen facts come from.
//!
//! The graph builder and the context owners ask for facts by identity; this is
//! the one place that answers from what the planner and encoder already
//! produced. Nothing here plans, schedules, or encodes: it indexes sealed
//! output so a builder never has to know which artifact a fact came from.

use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

use novarocks_execution::task_execution::domain::{
    CodecOwnedContent, CredentialEpoch, CredentialLeaseId,
};
use novarocks_execution::task_execution::identity::QueryContextRef;
use novarocks_execution::task_execution::operation::CredentialUpdate;
use novarocks_proto_codec::FieldPath;

use novarocks_proto_codec::lifecycle::{
    encode_credential_lease_descriptor, encode_credential_lease_secret_envelope,
};
use novarocks_proto_models::catalog::CatalogSet;
use novarocks_proto_models::novarocks::{QueryOptions, RuntimeFilterContribution};
use novarocks_task_codec::descriptor::WireFragmentPlan;
use novarocks_task_codec::domain::{WireContent, WireCredential};
use novarocks_task_codec::operation::{
    ESTABLISH_CATALOG_DOMAIN_TAG, ESTABLISH_FILTER_DOMAIN_TAG, ESTABLISH_QUERY_OPTIONS_DOMAIN_TAG,
};
use novarocks_types::identity::BackendProcessId;

use novarocks_sql::plan_read::FragmentId;

use crate::query_execution::artifact::ValidatedNativeSubmission;
use crate::query_execution::lifecycle_plan::QueryCredentialLeases;
use crate::query_execution::schedule::SchedulingPlan;
use crate::task_execution::context_owner::{ContextEstablishFacts, ContextEstablishSource};
use crate::task_execution::error::TaskExecutionError;
use crate::task_execution::graph::{FragmentPlanFacts, FragmentPlanSource};

/// The sealed submissions of one attempt, indexed the way the graph asks.
///
/// The encoder emits one submission per fragment instance, each carrying that
/// instance's own parameters. The graph builder asks by
/// `(fragment_id, instance_index)`, and the schedule is what maps an index to
/// the instance identity a submission names, so the two are correlated here
/// once rather than at every lookup.
#[derive(Debug)]
pub struct SubmissionFragmentPlans {
    plans: BTreeMap<(FragmentId, usize), FragmentPlanFacts>,
}

impl SubmissionFragmentPlans {
    /// Indexes every submission against the schedule that placed it.
    ///
    /// A submission whose instance the schedule does not contain, or a
    /// placement with no submission, fails here rather than at the first task
    /// that needs one: a partial index would produce a graph missing exactly
    /// the tasks nobody asked about yet.
    pub fn index(
        submissions: Vec<ValidatedNativeSubmission>,
        schedule: &SchedulingPlan,
    ) -> Result<Self, TaskExecutionError> {
        let mut index_of_instance = BTreeMap::new();
        for (&fragment_id, placements) in &schedule.by_fragment {
            for placement in placements {
                index_of_instance
                    .insert(placement.finst_id, (fragment_id, placement.instance_index));
            }
        }

        let mut plans = BTreeMap::new();
        for submission in submissions {
            let finst = submission.fragment_instance_id();
            let fragment_id = submission.fragment_id();
            let &(scheduled_fragment, instance_index) =
                index_of_instance.get(&finst).ok_or_else(|| {
                    TaskExecutionError::Schedule(format!(
                        "fragment {fragment_id} submission names instance {finst} which the \
                         schedule did not place"
                    ))
                })?;
            if scheduled_fragment != fragment_id {
                return Err(TaskExecutionError::Schedule(format!(
                    "instance {finst} is placed under fragment {scheduled_fragment} but its \
                     submission names fragment {fragment_id}"
                )));
            }
            let wire = submission.into_task_fragment_plan();
            let pipeline_dop = wire
                .instance_params
                .as_ref()
                .and_then(|params| params.query_options.as_ref())
                .map(|options| options.pipeline_dop)
                .and_then(|dop| usize::try_from(dop).ok())
                .and_then(NonZeroUsize::new)
                .ok_or_else(|| {
                    TaskExecutionError::Schedule(format!(
                        "fragment {fragment_id} instance {finst} has no positive pipeline dop"
                    ))
                })?;
            let plan = WireFragmentPlan::parse(wire, FieldPath::root("fragment_plan")).map_err(
                |error| {
                    TaskExecutionError::Schedule(format!(
                        "fragment {fragment_id} instance {finst} plan is not encodable: {error}"
                    ))
                },
            )?;
            if plans
                .insert(
                    (fragment_id, instance_index),
                    FragmentPlanFacts {
                        plan: Arc::new(plan),
                        pipeline_dop,
                    },
                )
                .is_some()
            {
                return Err(TaskExecutionError::Schedule(format!(
                    "fragment {fragment_id} instance {instance_index} was submitted twice"
                )));
            }
        }

        let placed: usize = schedule
            .by_fragment
            .values()
            .map(|placements| placements.len())
            .sum();
        if plans.len() != placed {
            return Err(TaskExecutionError::Schedule(format!(
                "the schedule placed {placed} instances but {} were submitted",
                plans.len()
            )));
        }
        Ok(Self { plans })
    }
}

impl FragmentPlanSource for SubmissionFragmentPlans {
    fn plan_for(
        &self,
        fragment_id: FragmentId,
        instance_index: usize,
    ) -> Result<FragmentPlanFacts, TaskExecutionError> {
        self.plans
            .get(&(fragment_id, instance_index))
            .cloned()
            .ok_or_else(|| {
                TaskExecutionError::Schedule(format!(
                    "fragment {fragment_id} instance {instance_index} has no submitted plan"
                ))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
    use novarocks_proto_models::{novarocks as proto, plan};
    use novarocks_types::UniqueId;
    use novarocks_types::identity::{AttemptId, QueryExecutionId, QueryId};

    use crate::query_execution::schedule::FragmentInstancePlacement;

    const ROOT: FragmentId = 1;
    const LEAF: FragmentId = 2;

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(5, 6),
            AttemptId::new(1).expect("attempt one is nonzero"),
        )
        .expect("a nonzero query id")
    }

    fn finst(fragment_id: FragmentId, instance_index: usize) -> UniqueId {
        UniqueId::new(i64::from(fragment_id), instance_index as i64 + 1)
    }

    fn placement(fragment_id: FragmentId, instance_index: usize) -> FragmentInstancePlacement {
        FragmentInstancePlacement {
            fragment_id,
            instance_index,
            finst_id: finst(fragment_id, instance_index),
            backend_idx: instance_index,
            endpoint: RuntimeEndpoint::new("127.0.0.1", 9000 + instance_index as i32)
                .expect("a valid endpoint"),
            scan_ranges: BTreeMap::new(),
            destinations: Vec::new(),
            per_exch_num_senders: BTreeMap::new(),
        }
    }

    /// One root instance and `leaves` leaf instances.
    fn schedule(leaves: usize) -> SchedulingPlan {
        let mut by_fragment = BTreeMap::new();
        by_fragment.insert(ROOT, vec![placement(ROOT, 0)]);
        by_fragment.insert(
            LEAF,
            (0..leaves).map(|index| placement(LEAF, index)).collect(),
        );
        SchedulingPlan {
            root_fragment_id: ROOT,
            by_fragment,
            root_finst_id: finst(ROOT, 0),
            root_backend_idx: 0,
        }
    }

    fn submission(
        fragment_id: FragmentId,
        instance_index: usize,
        dop: i32,
    ) -> ValidatedNativeSubmission {
        let id = finst(fragment_id, instance_index);
        ValidatedNativeSubmission::new(
            instance_index,
            id,
            execution_id(),
            plan::PlanFragment {
                fragment_id,
                sink: Some(plan::DataSink {
                    kind: Some(plan::data_sink::Kind::Result(true)),
                }),
                ..Default::default()
            },
            proto::InstanceParams {
                fragment_instance_id: Some(novarocks_proto_models::common::UniqueId {
                    hi: id.high(),
                    lo: id.low(),
                }),
                query_options: Some(proto::QueryOptions {
                    pipeline_dop: dop,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
    }

    fn every_submission(leaves: usize) -> Vec<ValidatedNativeSubmission> {
        let mut all = vec![submission(ROOT, 0, 2)];
        all.extend((0..leaves).map(|index| submission(LEAF, index, 2)));
        all
    }

    #[test]
    fn each_instance_is_indexed_under_its_own_placement() {
        // Every instance of one fragment carries its own parameters, and the
        // backend refuses a descriptor whose plan names a different instance.
        // Two instances resolving to the same plan is the failure this index
        // exists to prevent.
        let schedule = schedule(3);
        let plans =
            SubmissionFragmentPlans::index(every_submission(3), &schedule).expect("a full index");

        let first = plans.plan_for(LEAF, 0).expect("instance zero");
        let second = plans.plan_for(LEAF, 1).expect("instance one");
        assert_ne!(
            first.plan.fingerprint(),
            second.plan.fingerprint(),
            "two instances of one fragment must not resolve to one plan"
        );
        assert_eq!(first.pipeline_dop.get(), 2);
        assert!(plans.plan_for(LEAF, 3).is_err(), "there is no fourth leaf");
    }

    #[test]
    fn a_submission_the_schedule_never_placed_is_refused() {
        // Catching this at indexing time is the point: a submission for an
        // unplaced instance means the encoder and the scheduler disagree, and
        // a graph built from it would address a task no backend was told to
        // expect.
        let mut submissions = every_submission(2);
        submissions.push(submission(LEAF, 7, 2));
        let error = SubmissionFragmentPlans::index(submissions, &schedule(2))
            .expect_err("an unplaced instance is refused")
            .to_string();
        assert!(error.contains("the schedule did not place"), "{error}");
    }

    #[test]
    fn a_placement_with_no_submission_is_refused() {
        // The opposite direction, and the more dangerous one: a missing
        // submission would silently produce a graph short one task, and the
        // query would wait forever for output nobody was asked to produce.
        let error = SubmissionFragmentPlans::index(every_submission(1), &schedule(3))
            .expect_err("a missing submission is refused")
            .to_string();
        assert!(
            error.contains("placed 4 instances but 2 were submitted"),
            "{error}"
        );
    }

    #[test]
    fn a_plan_without_a_positive_pipeline_dop_is_refused() {
        // Zero is not a degree of parallelism. Defaulting it would start a
        // fragment with a silently different shape than the planner chose.
        let mut submissions = every_submission(1);
        submissions.push(submission(LEAF, 1, 0));
        let mut schedule = schedule(2);
        schedule
            .by_fragment
            .get_mut(&LEAF)
            .expect("leaf placements")
            .truncate(2);
        let error = SubmissionFragmentPlans::index(submissions, &schedule)
            .expect_err("a zero dop is refused")
            .to_string();
        assert!(error.contains("no positive pipeline dop"), "{error}");
    }

    #[test]
    fn each_backend_establishes_its_own_filter_contribution() {
        use novarocks_execution::task_execution::identity::QueryContextRef;
        use novarocks_task_codec::domain::stored_message;
        use novarocks_types::identity::{BackendProcessId, FrontendProcessId};

        use crate::query_execution::lifecycle_plan::QueryCredentialLeases;
        use crate::task_execution::context_owner::ContextEstablishSource;

        // A filter deployment gives each backend the role bindings its own
        // tasks play. Handing one backend another's contribution would install
        // the wrong producer and consumer identities, and nothing downstream
        // would notice until a filter reached the wrong place.
        let first = BackendProcessId::new_v7();
        let second = BackendProcessId::new_v7();
        let query_options = QueryOptions {
            query_mem_limit: 4096,
            pipeline_dop: 3,
            ..QueryOptions::default()
        };
        let facts = AttemptEstablishFacts::freeze(
            CatalogSet::default(),
            vec![
                (
                    first,
                    RuntimeFilterContribution {
                        participant_id: 11,
                        ..Default::default()
                    },
                ),
                (
                    second,
                    RuntimeFilterContribution {
                        participant_id: 22,
                        ..Default::default()
                    },
                ),
            ],
            query_options,
            &QueryCredentialLeases::empty(),
        )
        .expect("a legal attempt");

        let frontend = FrontendProcessId::new_v7();
        for (backend, expected) in [(first, 11), (second, 22)] {
            let context = QueryContextRef::new(execution_id(), frontend, backend);
            let established = facts.facts_for(context).expect("a scheduled backend");
            let contribution = stored_message::<RuntimeFilterContribution>(
                established.initial_runtime_filter.as_ref(),
            )
            .expect("this codec produced it");
            assert_eq!(contribution.participant_id, expected);
            assert_eq!(
                stored_message::<QueryOptions>(established.query_options.as_ref())
                    .expect("the establish owns its exact query options"),
                &query_options
            );
        }

        // A backend that hosts tasks but compiled no contribution is a
        // disagreement between the filter compiler and the scheduler, not an
        // empty install.
        let stranger = QueryContextRef::new(execution_id(), frontend, BackendProcessId::new_v7());
        assert!(facts.facts_for(stranger).is_err());
    }

    #[test]
    fn the_credential_domain_survives_into_the_refresh_owner() {
        use novarocks_execution::task_execution::identity::QueryContextRef;
        use novarocks_types::identity::{BackendProcessId, FrontendProcessId};

        use crate::query_execution::lifecycle_plan::QueryCredentialLeases;
        use crate::task_execution::context_owner::ContextEstablishSource;
        use crate::task_execution::credential::CredentialRefreshOwner;

        // The owner adopts the establish's domain. If they disagreed, the
        // first rotation would advance a domain nobody installed and every
        // context would refuse it as a gap.
        let backend = BackendProcessId::new_v7();
        let facts = AttemptEstablishFacts::freeze(
            CatalogSet::default(),
            vec![(backend, RuntimeFilterContribution::default())],
            QueryOptions::default(),
            &QueryCredentialLeases::empty(),
        )
        .expect("a legal attempt");
        let context = QueryContextRef::new(execution_id(), FrontendProcessId::new_v7(), backend);
        let established = facts.facts_for(context).expect("its own backend");

        let owner = CredentialRefreshOwner::from_establish(
            &established.initial_credential,
            std::iter::once(context),
        );
        assert_eq!(owner.lease_id(), ATTEMPT_CREDENTIAL_DOMAIN);
        assert_eq!(owner.minted_epoch(), established.initial_credential.epoch());

        // And nothing about the facts renders the material.
        assert!(!format!("{facts:?}").contains("secret"));
    }
}

/// The credential domain every attempt installs.
///
/// One query context has exactly one, so the value only has to be stable and
/// nonzero. It is the protocol's own numbering, unrelated to a vended lease's
/// sixteen-byte storage identity.
const ATTEMPT_CREDENTIAL_DOMAIN: CredentialLeaseId = CredentialLeaseId::new(1);

/// The shared facts one attempt establishes on every backend it scheduled.
///
/// The catalog set and the credential table are query-wide; the runtime-filter
/// contribution is not. A filter deployment gives each backend the role
/// bindings its own tasks play, so handing one backend another's contribution
/// would install the wrong producer and consumer identities. That is why this
/// is keyed by backend and why a scheduled backend with no contribution is an
/// error rather than an empty install.
pub struct AttemptEstablishFacts {
    catalog_binding: Arc<dyn CodecOwnedContent>,
    filters: BTreeMap<BackendProcessId, Arc<dyn CodecOwnedContent>>,
    query_options: Arc<dyn CodecOwnedContent>,
    credential: CredentialUpdate,
}

impl fmt::Debug for AttemptEstablishFacts {
    /// Renders only the shape: this transitively holds credential material.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AttemptEstablishFacts")
            .field("backends", &self.filters.len())
            .field("query_options", &self.query_options.fingerprint())
            .finish()
    }
}

impl AttemptEstablishFacts {
    /// Freezes one attempt's shared facts.
    ///
    /// The credential progression token names the lease whose rotation this
    /// update represents; an initial install carries the whole table at its
    /// starting epochs, so the token is the lowest lease id present. An
    /// attempt with no vended credential carries an empty table rather than a
    /// missing one, because the backend installs what it is given and an
    /// absent domain would be a different statement.
    pub fn freeze(
        catalog_set: CatalogSet,
        filters: impl IntoIterator<Item = (BackendProcessId, RuntimeFilterContribution)>,
        query_options: QueryOptions,
        leases: &QueryCredentialLeases,
    ) -> Result<Self, TaskExecutionError> {
        let mut descriptors = Vec::new();
        let mut envelopes = Vec::new();
        for lease in leases.leases() {
            descriptors.push(encode_credential_lease_descriptor(lease.descriptor()));
            envelopes.push(encode_credential_lease_secret_envelope(lease.envelope()));
        }
        let material = WireCredential::decode(
            &descriptors,
            &envelopes,
            FieldPath::root("initial_credential"),
        )
        .map_err(|error| {
            // The message is the codec's own and carries no secret; the
            // material never reaches a rendering.
            TaskExecutionError::Schedule(format!(
                "credential contribution is not installable: {error}"
            ))
        })?;

        Ok(Self {
            catalog_binding: Arc::new(WireContent::new(ESTABLISH_CATALOG_DOMAIN_TAG, catalog_set)),
            filters: filters
                .into_iter()
                .map(|(backend, contribution)| {
                    (
                        backend,
                        Arc::new(WireContent::new(ESTABLISH_FILTER_DOMAIN_TAG, contribution))
                            as Arc<dyn CodecOwnedContent>,
                    )
                })
                .collect(),
            query_options: Arc::new(WireContent::new(
                ESTABLISH_QUERY_OPTIONS_DOMAIN_TAG,
                query_options,
            )),
            // The protocol's lease id names the attempt's credential domain,
            // not a storage lease: a query context has exactly one such
            // domain, and each vended lease keeps its own sixteen-byte
            // identity inside the table. The refresh owner adopts this value
            // from the establish, so an install and its first rotation are
            // provably the same domain.
            credential: CredentialUpdate::new(
                ATTEMPT_CREDENTIAL_DOMAIN,
                CredentialEpoch::FIRST,
                Arc::new(material),
            ),
        })
    }
}

impl AttemptEstablishFacts {
    /// The credential domain every context of this attempt installs.
    ///
    /// The rotation owner adopts exactly this, so an install and its first
    /// rotation are provably the same domain at the same starting epoch.
    pub fn credential(&self) -> &CredentialUpdate {
        &self.credential
    }
}

impl ContextEstablishSource for AttemptEstablishFacts {
    fn facts_for(
        &self,
        context: QueryContextRef,
    ) -> Result<ContextEstablishFacts, TaskExecutionError> {
        let backend = context.backend_process_id();
        let initial_runtime_filter = self.filters.get(&backend).cloned().ok_or_else(|| {
            TaskExecutionError::Schedule(format!(
                "backend {backend} hosts tasks but this attempt compiled no runtime filter \
                 contribution for it"
            ))
        })?;
        Ok(ContextEstablishFacts {
            catalog_binding: Arc::clone(&self.catalog_binding),
            initial_runtime_filter,
            query_options: Arc::clone(&self.query_options),
            initial_credential: self.credential.clone(),
        })
    }
}
