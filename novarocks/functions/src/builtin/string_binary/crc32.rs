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

//! Zlib CRC32 over bounded input chunks.
use crate::{KernelFailure, kernel_input::EvaluationCheckpoints};

pub(super) fn crc32_zlib_observed(
    data: &[u8],
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<u32, KernelFailure> {
    let mut crc = 0xffff_ffff_u32;
    for &byte in data {
        work.step()?;
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xedb8_8320;
            } else {
                crc >>= 1;
            }
        }
    }
    Ok(crc ^ 0xffff_ffff)
}
