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
//! Public SQL completion inputs for owner and compile-to-execute tests.
mod catalogue;
mod source;
mod support;

pub use catalogue::installed_builtin_owner_catalogue;
pub use source::{
    sql_source, sql_source_with_declared_fields, sql_source_with_semantics,
    sql_source_with_single_field, sql_source_with_single_field_and_core_residuals,
    try_sql_source_with_declared_fields,
};
pub use support::{parse_query, sql_fixture_constant_policy};
