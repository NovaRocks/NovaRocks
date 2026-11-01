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
/// The bits a float is keyed by.
///
/// Two values SQL compares equal have to key the same, and bits alone do not
/// say that: `-0.0` and `+0.0` are equal and carry different bits, while two
/// NaNs carry different payloads and this engine groups them together. Both
/// are folded here so that hashing, group-key encoding and partitioning all
/// answer the same question.
pub fn canonical_f64_bits(value: f64) -> u64 {
    if value.is_nan() {
        f64::NAN.to_bits()
    } else if value == 0.0 {
        0.0f64.to_bits()
    } else {
        value.to_bits()
    }
}

/// See [`canonical_f64_bits`].
pub fn canonical_f32_bits(value: f32) -> u32 {
    if value.is_nan() {
        f32::NAN.to_bits()
    } else if value == 0.0 {
        0.0f32.to_bits()
    } else {
        value.to_bits()
    }
}
