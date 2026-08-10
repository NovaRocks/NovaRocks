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

//! Bounds-checked primitives for execution-owned filter payloads.

fn require(data: &[u8], offset: usize, width: usize) -> Result<(), String> {
    if data.len() < offset.saturating_add(width) {
        return Err("runtime filter data truncated".to_string());
    }
    Ok(())
}

pub(super) fn read_u8(data: &[u8], offset: &mut usize) -> Result<u8, String> {
    require(data, *offset, 1)?;
    let value = data[*offset];
    *offset += 1;
    Ok(value)
}

pub(super) fn read_i8(data: &[u8], offset: &mut usize) -> Result<i8, String> {
    Ok(read_u8(data, offset)? as i8)
}

pub(super) fn read_i16_le(data: &[u8], offset: &mut usize) -> Result<i16, String> {
    require(data, *offset, 2)?;
    let value = i16::from_le_bytes([data[*offset], data[*offset + 1]]);
    *offset += 2;
    Ok(value)
}

pub(super) fn read_i32_le(data: &[u8], offset: &mut usize) -> Result<i32, String> {
    require(data, *offset, 4)?;
    let value = i32::from_le_bytes(data[*offset..*offset + 4].try_into().unwrap());
    *offset += 4;
    Ok(value)
}

pub(super) fn read_u32_le(data: &[u8], offset: &mut usize) -> Result<u32, String> {
    Ok(read_i32_le(data, offset)? as u32)
}

pub(super) fn read_i64_le(data: &[u8], offset: &mut usize) -> Result<i64, String> {
    require(data, *offset, 8)?;
    let value = i64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
    *offset += 8;
    Ok(value)
}

pub(super) fn read_u64_le(data: &[u8], offset: &mut usize) -> Result<u64, String> {
    Ok(read_i64_le(data, offset)? as u64)
}
