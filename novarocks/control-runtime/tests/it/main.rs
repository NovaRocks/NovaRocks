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

//! Public-port contract tests of the control runtime. Loom models live in `tests/loom`.
#![cfg(not(loom))]

mod actor_failure_policy;
mod actor_retirement;
mod admitted_actor_attachment;
mod admitted_input;
mod capacity_keyed_family;
mod capacity_release_notification;
mod capacity_split_next;
mod capacity_wait_observation;
mod child_scope_bootstrap;
mod coalescing_input;
mod concurrent_actor_turn;
mod continuation_custody;
mod cross_scope_capacity_handoff;
mod deterministic_actor_turn;
mod effect_input_forwarding;
mod effect_ticket_origin;
mod exit_authority_child_scope;
mod exit_custody_binding;
mod exit_custody_lifecycle;
mod failure_publication;
mod host_turn;
mod input_source_selection;
mod owned_object_storage;
mod owned_return;
mod poller_host_turn;
mod read_resource;
mod reservation_parentage;
mod runtime_identity;
mod scope_origin;
mod source_cut;
mod source_preference;
mod step_reservation_custody;
mod step_scope_stop;
mod timer_source_close;
mod unpublished_exit;
