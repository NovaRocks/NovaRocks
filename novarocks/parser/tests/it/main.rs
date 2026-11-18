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

mod coverage;
mod lexer;

mod build;
mod order_by;
mod validation;

mod window;

mod call_argument;

mod query_count;

mod json_arrow;

mod ordinary_special;

mod group_by;

mod relation;

mod projection_values;

mod expression;
mod printer_first_slice;
mod query;
mod trino_special;
mod types;

mod trino_json;

mod nesting;
mod printer_advanced_relation;
mod printer_json;
mod printer_pattern;
mod printer_routine;
mod printer_special;
mod roundtrip;
mod trino_access;
mod trino_advanced_relation;
mod trino_pattern;
mod trino_routine;

mod statements_branches;
mod statements_catalog_schema;
mod statements_ctas;
mod statements_merge_call;
mod statements_metadata_session;
mod statements_permissions;
mod statements_prepared;
mod statements_session;
mod statements_show_create_stats;
mod statements_table_alter;
mod statements_table_create;
mod statements_views;
mod statements_write;
