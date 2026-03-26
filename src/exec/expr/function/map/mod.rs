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

mod arrays_zip;
mod cardinality;
mod common;
mod dispatch;
mod distinct_map_keys;
mod element_at;
mod map_apply;
mod map_concat;
mod map_entries;
mod map_filter;
mod map_from_arrays;
mod map_keys;
mod map_size;
mod map_values;
pub use arrays_zip::eval_arrays_zip;
pub use cardinality::eval_cardinality;
pub(crate) use common::sorted_map_offsets_and_indices;
pub use dispatch::{eval_map_function, metadata, register};
pub use distinct_map_keys::eval_distinct_map_keys;
pub use element_at::eval_element_at;
pub use map_apply::eval_map_apply;
pub use map_concat::eval_map_concat;
pub use map_entries::eval_map_entries;
pub use map_filter::eval_map_filter;
pub use map_from_arrays::eval_map_from_arrays;
pub use map_keys::eval_map_keys;
pub use map_size::eval_map_size;
pub use map_values::eval_map_values;
