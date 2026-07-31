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

use crate::meta::{IdScope, MetaError};

pub fn mv_id() -> IdScope {
    stable("mv.id")
}

pub fn refresh_id() -> IdScope {
    stable("refresh.id")
}

pub fn erase_job() -> IdScope {
    stable("job.erase")
}

pub fn iceberg_operation() -> IdScope {
    stable("iceberg.operation")
}

pub fn custom(value: impl Into<String>) -> Result<IdScope, MetaError> {
    IdScope::new(value)
}

fn stable(value: &'static str) -> IdScope {
    IdScope::new(value).expect("stable metadata id scope must be valid")
}
