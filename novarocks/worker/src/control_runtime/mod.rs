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

//! Worker binding of the common control runtime and the pure protocol.

pub(crate) mod actors;
pub(crate) mod admitted_role;
pub(crate) mod context_executors;
pub(crate) mod context_host;
pub(crate) mod context_host_jobs;
pub(crate) mod context_io;
pub(crate) mod context_lifecycle;
pub(crate) mod context_ready;
pub(crate) mod control_internal;
pub(crate) mod control_request;
pub(crate) mod control_ticket;
pub(crate) mod establish_executors;
pub(crate) mod establish_ingress;
pub(crate) mod establish_route;
pub(crate) mod process_control;
pub(crate) mod request_endpoint;
pub(crate) mod request_ready;
pub(crate) mod service;

pub(crate) mod effect_dispatch;
pub(crate) mod host_executor;
pub(crate) mod open_summary;
pub(crate) mod operator_statistics;

pub(crate) mod preparation;
pub(crate) mod preparation_refusal;

pub(crate) mod task;
pub(crate) mod task_publication;

pub(crate) mod causal;
pub(crate) mod causal_input;
pub(crate) mod completion_action;
pub(crate) mod failure;
pub(crate) mod lifecycle_read;
pub(crate) mod loading;
pub(crate) mod loading_events;
pub(crate) mod loading_host_job;
pub(crate) mod source_custody;
pub(crate) mod source_positions;
pub(crate) mod source_slot_issuer;

pub(crate) mod task_host_call;
