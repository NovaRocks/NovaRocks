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

pub(crate) mod activity;
pub(crate) mod background;
pub(crate) mod background_engine;
pub mod command;
pub mod domain;
pub(crate) mod maintenance_worker;
pub mod management_audit;
// Installed by the composition root so the frontend owns startup ordering.
mod refresh;
pub(crate) use refresh::bind_publication_documents;
pub mod scheduler;
mod service;
#[allow(dead_code)]
pub mod startup_isolation_file;
pub(crate) mod startup_restore;

pub use service::FrontendMvProductAdapter;

#[cfg(novarocks_dev_controls)]
pub mod test_controls;
