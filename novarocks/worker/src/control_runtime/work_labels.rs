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

//! Fixed diagnostic vocabulary; labels grant no work or resources.

pub(crate) const PARTICIPATION_INSTANCE: &str = "participation-instance";
pub(crate) const RUNTIME_FILTER_SENDER: &str = "runtime-filter-sender";

pub(crate) const PREPARATION_CANCEL: &str = "preparation.cancel";
pub(crate) const PREPARATION_INBOUND: &str = "preparation.inbound";
pub(crate) const PREPARATION_RECEIVER: &str = "preparation.receiver";
pub(crate) const PREPARATION_CHILD: &str = "preparation.child";
pub(crate) const PREPARATION_ADOPT: &str = "preparation.adopt";
pub(crate) const PREPARATION_PUBLICATION: &str = "preparation.publication";

pub const WORK_DIAGNOSTIC_LABELS: &[&str] = &[
    PARTICIPATION_INSTANCE,
    RUNTIME_FILTER_SENDER,
    PREPARATION_CANCEL,
    PREPARATION_INBOUND,
    PREPARATION_RECEIVER,
    PREPARATION_CHILD,
    PREPARATION_ADOPT,
    PREPARATION_PUBLICATION,
];
