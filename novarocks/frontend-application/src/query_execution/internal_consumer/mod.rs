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

//! Fixed application-owned internal computation consumers.

pub(crate) mod attempt_handoff;
pub(crate) mod decoded;
pub(crate) mod domain_cpu;
pub(crate) mod domain_state;
pub(crate) mod profile;
pub(crate) mod write;

#[cfg(all(test, novarocks_dev_controls))]
pub(crate) mod test_fixture;
