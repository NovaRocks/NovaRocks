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

mod array_to_bitmap;
pub(crate) mod bitmap_common;
mod bitmap_functions;
mod bitmap_to_array;
mod bitmap_to_string;
mod dispatch;
mod hll_codec;
mod hll_hash;
mod json_object;
mod to_bitmap;

pub use dispatch::{eval_object_function, metadata, register};
