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
use std::hash::{BuildHasher, Hash, Hasher};

pub(crate) fn make_hash<Q: ?Sized, S: BuildHasher>(build_hasher: &S, value: &Q) -> u64
where
    Q: Hash,
{
    let mut hasher = build_hasher.build_hasher();
    value.hash(&mut hasher);
    hasher.finish()
}

pub(crate) fn seed_from_hasher<S: BuildHasher>(build_hasher: &S) -> u64 {
    make_hash(build_hasher, &0u8)
}

pub(crate) fn combine_hash(acc: u64, value_hash: u64) -> u64 {
    acc ^ value_hash
        .wrapping_add(0x9e3779b97f4a7c15)
        .wrapping_add(acc << 6)
        .wrapping_add(acc >> 2)
}

pub(crate) fn hash_u64_with_seed(seed: u64, value: u64) -> u64 {
    mix_u64(seed ^ value)
}

pub(crate) fn hash_i128_with_seed(seed: u64, value: i128) -> u64 {
    let value = value as u128;
    let low = value as u64;
    let high = (value >> 64) as u64;
    let low_hash = hash_u64_with_seed(seed, low);
    let high_hash = hash_u64_with_seed(seed, high);
    combine_hash(low_hash, high_hash)
}

pub(crate) fn hash_bytes_with_seed(seed: u64, bytes: &[u8]) -> u64 {
    let mut hash = seed ^ 0xcbf29ce484222325;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

pub(crate) fn hash_null_with_seed(seed: u64) -> u64 {
    hash_u64_with_seed(seed, 0x9e3779b97f4a7c15)
}

pub(crate) fn canonical_f64_bits(value: f64) -> u64 {
    if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

pub(crate) fn canonical_f32_bits(value: f32) -> u32 {
    if value.is_nan() {
        f32::NAN.to_bits()
    } else {
        value.to_bits()
    }
}

fn mix_u64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e3779b97f4a7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}
