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

pub(crate) mod codec;
// RFD-6 live cutover will consume this deployment compiler; remove the allowance then.
#[allow(dead_code)]
pub(crate) mod deployment;
pub(crate) mod exec;
pub(crate) mod materializer;
// RFD-3/RFD-5A will consume this staged planner/runtime seam; remove the allowance then.
#[allow(dead_code)]
pub(crate) mod model;
pub(crate) mod port;
#[cfg(any(test, feature = "runtime-filter-test-support"))]
pub(crate) mod test_support;
