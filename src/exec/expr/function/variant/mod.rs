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
#![allow(dead_code)]
#![allow(unused_variables)]

mod common;
mod dispatch;
mod get_variant;
mod json_keys;
mod parse_json;
mod variant_typeof;

pub use dispatch::{eval_variant_function, metadata, register};
pub use get_variant::{
    eval_get_variant_bool, eval_get_variant_date, eval_get_variant_datetime,
    eval_get_variant_double, eval_get_variant_int, eval_get_variant_string, eval_get_variant_time,
    eval_json_exists, eval_json_length, eval_json_query, eval_variant_query,
};
pub use json_keys::eval_json_keys;
pub use parse_json::eval_parse_json;
pub use variant_typeof::eval_variant_typeof;
