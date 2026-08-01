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

mod admission;
mod dependency;
mod event_io;
mod ffi;
mod io;
mod lookup_io;
mod result_io;
mod service;
mod statistic_result;
mod sync;

pub(crate) use dependency::lake_meta_storage_resolver;
pub(crate) use event_io::compat_fragment_event_sink;
pub(crate) use io::brpc_exchange_transmitter;
pub(crate) use lookup_io::brpc_fragment_lookup_client;
pub(crate) use result_io::compat_result_writer;
pub use service::CompatFragmentService;
pub(crate) use sync::SyncFragmentExecutor;
