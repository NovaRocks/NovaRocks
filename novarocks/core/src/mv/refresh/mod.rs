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

pub(crate) mod aggregate_first_refresh;
pub(crate) mod apply_key;
pub(crate) mod capabilities;
pub(crate) mod contract;
pub mod definition;
pub(crate) mod execution;
pub(crate) mod execution_context;
pub(crate) mod execution_policy;
pub(crate) mod non_join_incremental;
pub mod observation;
pub(crate) mod pin;
pub(crate) mod planning;
pub(crate) mod projection_first_refresh;
pub(crate) mod repartition;
pub(crate) mod rewrite_context;
pub(crate) mod scan_binding;
pub(crate) mod schema_contract;
pub(crate) mod snapshot;
pub(crate) mod target;
pub(crate) mod target_apply;
pub(crate) mod target_binding;
