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

//! Shared control-plane control runtime contracts.

#![forbid(unsafe_code)]

pub mod actor;
pub mod actor_turn;
pub mod completion;
pub mod continuation;
pub mod exit_custody;
pub mod header_readiness;
pub mod notify;
pub mod obligation;
pub mod owned_return;
pub mod read_resource;
pub mod reply_notification;
pub mod reported_work;
pub mod resource;
pub mod shutdown;
pub mod sync;
pub mod transaction;

pub mod history;
pub mod publication;

pub mod observation;
