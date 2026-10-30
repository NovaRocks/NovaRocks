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
use std::sync::Arc;

use novarocks_execution::task_execution::domain::{
    CodecOwnedContent, CredentialEpoch, CredentialLeaseId,
};
use novarocks_execution::task_execution::identity::QueryContextRef;
use novarocks_execution::task_execution::operation::CredentialUpdate;
use novarocks_proto_codec::FieldPath;

use novarocks_proto_codec::lifecycle::encode_credential_lease_descriptor;
use novarocks_proto_models::catalog::CatalogSet;
use novarocks_proto_models::novarocks::{QueryOptions, RuntimeFilterContribution};
use novarocks_task_codec::domain::{WireContent, WireCredential};
use novarocks_task_codec::operation::{
    ESTABLISH_CATALOG_DOMAIN_TAG, ESTABLISH_FILTER_DOMAIN_TAG, ESTABLISH_QUERY_OPTIONS_DOMAIN_TAG,
};
use novarocks_types::identity::BackendProcessId;

use crate::query_execution::artifact::FragmentId;

use crate::query_execution::artifact::{TaskManifestBinding, ValidatedNativeSubmission};
use crate::query_execution::lifecycle_plan::QueryCredentialLeases;
use crate::task_execution::context_owner::{ContextEstablishFacts, ContextEstablishSource};
use crate::task_execution::error::TaskExecutionError;
use crate::task_execution::graph::{FragmentPlanFacts, FragmentPlanSource};

/// The sealed submissions of one attempt, indexed the way the graph asks.
///
/// The encoder emits one submission per fragment instance, each holding its
/// fragment's shared frozen plan and that instance's own assignment. Nothing
/// here parses a plan: the frozen fragment already carries the typed facts
/// the graph binds with. The graph builder asks by
/// `(fragment_id, instance_index)`, and the schedule is what maps an index to
/// the instance identity a submission names, so the two are correlated here
/// once rather than at every lookup.
#[derive(Debug)]
pub struct SubmissionFragmentPlans {
    plans: BTreeMap<(FragmentId, usize), FragmentPlanFacts>,
}

impl SubmissionFragmentPlans {
    /// Indexes sealed plans against the exact Task manifest without consulting
    /// the scheduling plan that preceded it.
    pub(crate) fn index_manifest(
        submissions: Vec<ValidatedNativeSubmission>,
        manifest: &TaskManifestBinding,
    ) -> Result<Self, TaskExecutionError> {
        let expected = manifest
            .tasks()
            .iter()
            .map(|task| {
                (
                    task.fragment_instance_id(),
                    (task.fragment_id(), task.instance_index()),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut plans = BTreeMap::new();
        for submission in submissions {
            let finst = submission.fragment_instance_id();
            let fragment_id = submission.fragment_id();
            let &(scheduled_fragment, instance_index) = expected.get(&finst).ok_or_else(|| {
                TaskExecutionError::Schedule(format!(
                    "fragment {fragment_id} submission names instance {finst} absent from the Task manifest"
                ))
            })?;
            if scheduled_fragment != fragment_id {
                return Err(TaskExecutionError::Schedule(format!(
                    "manifest instance {finst} belongs to fragment {scheduled_fragment} but its submission names {fragment_id}"
                )));
            }
            let execution = submission.execution_id();
            let (fragment, assignment) = submission.into_creation_parts();
            manifest
                .validate_submission_fragment(execution, &fragment)
                .map_err(|error| TaskExecutionError::Schedule(error.to_string()))?;
            if plans
                .insert(
                    (fragment_id, instance_index),
                    FragmentPlanFacts {
                        fragment,
                        assignment,
                    },
                )
                .is_some()
            {
                return Err(TaskExecutionError::Schedule(format!(
                    "manifest fragment {fragment_id} instance {instance_index} was submitted twice"
                )));
            }
        }
        if plans.len() != expected.len() {
            return Err(TaskExecutionError::Schedule(format!(
                "the Task manifest contains {} tasks but {} plans were submitted",
                expected.len(),
                plans.len()
            )));
        }
        Ok(Self { plans })
    }
}

impl FragmentPlanSource for SubmissionFragmentPlans {
    fn take_plan(
        &mut self,
        fragment_id: FragmentId,
        instance_index: usize,
    ) -> Result<FragmentPlanFacts, TaskExecutionError> {
        self.plans
            .remove(&(fragment_id, instance_index))
            .ok_or_else(|| {
                TaskExecutionError::Schedule(format!(
                    "fragment {fragment_id} instance {instance_index} has no submitted plan"
                ))
            })
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
        for lease in leases.leases() {
            descriptors.push(encode_credential_lease_descriptor(lease.descriptor()));
        }
        let material = WireCredential::decode(&descriptors, FieldPath::root("initial_credential"))
            .map_err(|error| {
                TaskExecutionError::Schedule(format!(
                    "credential announcement is not installable: {error}"
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::num::NonZeroUsize;

    use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
    use novarocks_physical_plan::PipelineDopDomain;
    use novarocks_types::UniqueId;
    use novarocks_types::identity::{AttemptId, QueryExecutionId, QueryId};

    use crate::native::fragment_encoder::frozen::FragmentArtifact;
    use crate::query_execution::artifact::NativePlacementAssignment;
    use crate::query_execution::schedule::{FragmentInstancePlacement, SchedulingPlan};

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
        crate::query_execution::artifact::fragment_instance_id_for_contract_test(
            execution_id().query_id(),
            fragment_id,
            instance_index,
        )
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

    fn artifact(fragment_id: FragmentId) -> Arc<FragmentArtifact> {
        crate::native::fragment_encoder::frozen::tests::frozen_package(
            fragment_id,
            0,
            [1; 16],
            PipelineDopDomain {
                min: 1,
                max: 16,
                requires_power_of_two: false,
            },
            None,
            0,
        )
    }

    fn submission_of(
        fragment: &Arc<FragmentArtifact>,
        instance_index: usize,
    ) -> ValidatedNativeSubmission {
        let fragment_id = fragment.facts().fragment_id();
        ValidatedNativeSubmission::new(
            instance_index,
            finst(fragment_id, instance_index),
            execution_id(),
            Arc::clone(fragment),
            NativePlacementAssignment::new(
                instance_index as u32,
                NonZeroUsize::new(2).expect("nonzero"),
                BTreeMap::new(),
            ),
        )
    }

    fn submission_fixture(
        leaves: usize,
    ) -> (
        Vec<ValidatedNativeSubmission>,
        TaskManifestBinding,
        Arc<FragmentArtifact>,
    ) {
        let root = artifact(ROOT);
        let leaf = artifact(LEAF);
        let schedule = schedule(leaves);
        let mut processes = BTreeMap::new();
        for placement in schedule.by_fragment.values().flatten() {
            processes
                .entry(placement.backend_idx)
                .or_insert_with(BackendProcessId::new_v7);
        }
        let manifest = TaskManifestBinding::protocol_fixture_for_test(
            execution_id(),
            novarocks_types::FrontendProcessId::new_v7(),
            &schedule,
            &[],
            &processes,
            [(ROOT, Arc::clone(&root)), (LEAF, Arc::clone(&leaf))].into(),
        )
        .expect("bind the original finite manifest");
        let mut all = vec![submission_of(&root, 0)];
        all.extend((0..leaves).map(|index| submission_of(&leaf, index)));
        (all, manifest, leaf)
    }

    #[test]
    fn each_instance_is_indexed_under_its_own_placement() {
        // Every instance of one fragment shares that fragment's frozen plan
        // and carries only its own assignment. Taking an instance moves its
        // facts out, so the index keeps no second copy of what a task owns.
        let (submissions, manifest, _) = submission_fixture(3);
        let mut plans =
            SubmissionFragmentPlans::index_manifest(submissions, &manifest).expect("a full index");

        let first = plans.take_plan(LEAF, 0).expect("instance zero");
        let second = plans.take_plan(LEAF, 1).expect("instance one");
        assert!(
            Arc::ptr_eq(&first.fragment, &second.fragment),
            "two instances of one fragment share one frozen plan"
        );
        assert_eq!(first.assignment.instance_ordinal(), 0);
        assert_eq!(second.assignment.instance_ordinal(), 1);
        assert_eq!(first.assignment.pipeline_dop().get(), 2);
        assert!(
            plans.take_plan(LEAF, 0).is_err(),
            "a taken instance is gone"
        );
        assert!(plans.take_plan(LEAF, 3).is_err(), "there is no fourth leaf");
    }

    #[test]
    fn a_submission_the_schedule_never_placed_is_refused() {
        // Catching this at indexing time is the point: a submission for an
        // unplaced instance means the encoder and the scheduler disagree, and
        // a graph built from it would address a task no backend was told to
        // expect.
        let (mut submissions, manifest, leaf) = submission_fixture(2);
        submissions.push(submission_of(&leaf, 7));
        let error = SubmissionFragmentPlans::index_manifest(submissions, &manifest)
            .expect_err("an unplaced instance is refused");
        let TaskExecutionError::Schedule(error) = error else {
            panic!("unplaced submission keeps the original schedule error category");
        };
        assert_eq!(
            error,
            format!(
                "fragment {LEAF} submission names instance {} absent from the Task manifest",
                finst(LEAF, 7),
            ),
        );
    }

    #[test]
    fn a_placement_with_no_submission_is_refused() {
        // The opposite direction, and the more dangerous one: a missing
        // submission would silently produce a graph short one task, and the
        // query would wait forever for output nobody was asked to produce.
        let (mut submissions, manifest, _) = submission_fixture(3);
        submissions.truncate(2);
        let error = SubmissionFragmentPlans::index_manifest(submissions, &manifest)
            .expect_err("a missing submission is refused");
        let TaskExecutionError::Schedule(error) = error else {
            panic!("missing submission keeps the original schedule error category");
        };
        assert_eq!(
            error,
            "the Task manifest contains 4 tasks but 2 plans were submitted",
        );
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
    fn the_credential_domain_is_frozen_into_every_establish() {
        use novarocks_execution::task_execution::identity::QueryContextRef;
        use novarocks_types::identity::{BackendProcessId, FrontendProcessId};

        use crate::query_execution::lifecycle_plan::QueryCredentialLeases;

        // Every context is established with the attempt's one credential
        // domain. The refresh owner that used to read it here is gone with the
        // rotation it drove (CAD-1 C14); the domain itself still has to be
        // exactly what each establish installs.
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

        assert_eq!(
            established.initial_credential.lease_id(),
            ATTEMPT_CREDENTIAL_DOMAIN
        );

        // And nothing about the facts renders the material.
        assert!(!format!("{facts:?}").contains("secret"));
    }
}
