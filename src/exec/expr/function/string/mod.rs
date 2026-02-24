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

mod append_trailing_char_if_absent;
mod ascii;
mod bar;
mod case_ops;
mod concat;
mod concat_ws;
mod crc32;
mod field;
mod find_in_set;
mod format_bytes;
mod group_concat;
mod hex;
mod initcap;
mod left_right_ops;
mod length_ops;
mod locate_ops;
mod money_format;
mod murmur_hash3_32;
mod ngram_search;
mod null_or_empty;
mod pad_ops;
mod parse_url;
mod prefix_suffix_ops;
mod regexp_count;
mod regexp_extract;
mod regexp_extract_all;
mod regexp_position;
mod regexp_replace;
mod repeat;
mod replace;
mod reverse;
mod space;
mod split;
mod split_part;
mod str_to_map;
mod substring;
mod substring_index;
#[cfg(test)]
mod test_utils;
mod translate;
mod trim_ops;
mod unhex;
mod upper;
mod url_ops;
mod uuid;

pub use dispatch::{eval_string_function, metadata, register};
pub use split::eval_split;
pub use substring::eval_substring;
pub use upper::eval_upper;
