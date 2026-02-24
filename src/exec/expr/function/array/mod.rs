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
mod all_match;
mod any_match;
mod array_append;
mod array_avg;
mod array_concat;
mod array_contains;
mod array_contains_all;
mod array_contains_seq;
mod array_cum_sum;
mod array_difference;
mod array_distinct;
mod array_filter;
mod array_flatten;
mod array_generate;
mod array_intersect;
mod array_join;
mod array_length;
mod array_map;
mod array_max;
mod array_min;
mod array_position;
mod array_remove;
mod array_repeat;
mod array_slice;
mod array_sort;
mod array_sort_lambda;
mod array_sortby;
mod array_sum;
mod array_top_n;
mod arrays_overlap;
mod common;
mod dispatch;
mod element_at;
#[cfg(test)]
mod test_utils;

pub use array_map::eval_array_map;
pub use dispatch::{eval_array_function, metadata, register};
