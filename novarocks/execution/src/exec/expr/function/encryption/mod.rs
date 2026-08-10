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

mod aes_decrypt;
mod aes_encrypt;
mod common;
mod dispatch;
mod encode_fingerprint_sha256;
mod encode_sort_key;
mod from_base64;
mod from_binary;
mod md5;
mod md5sum;
mod md5sum_numeric;
mod sha2;
mod sm3;
mod to_base64;
mod to_binary;

pub use aes_decrypt::eval_aes_decrypt;
pub use aes_encrypt::eval_aes_encrypt;
pub use dispatch::{eval_encryption_function, metadata, register};
pub use encode_fingerprint_sha256::eval_encode_fingerprint_sha256;
pub use encode_sort_key::eval_encode_sort_key;
pub use from_base64::eval_from_base64;
pub use from_binary::eval_from_binary;
pub use md5::eval_md5;
pub use md5sum::eval_md5sum;
pub use md5sum_numeric::eval_md5sum_numeric;
pub use sha2::eval_sha2;
pub use sm3::eval_sm3;
pub use to_base64::eval_to_base64;
pub use to_binary::eval_to_binary;
