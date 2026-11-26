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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Wire tags and byte ranges for native JSON v1. Item reads are shallow and
//! allocate nothing. Functions owns the interpretation and validation of
//! scalar values and container members.

pub const VERSION: u8 = 1;
pub const NULL: u8 = 1 << 5;
pub const SCALAR: u8 = 2 << 5;
pub const ARRAY: u8 = 3 << 5;
pub const OBJECT: u8 = 4 << 5;
pub const INDEX_ARRAY: u8 = 5 << 5;
pub const INDEX_OBJECT: u8 = 6 << 5;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrameError {
    Version,
    Extent,
    Tag,
    Scalar,
}

/// The single item covered by a complete document, after its version byte.
pub fn root_item(bytes: &[u8]) -> Result<&[u8], FrameError> {
    let (version, bytes) = bytes.split_first().ok_or(FrameError::Version)?;
    if *version != VERSION {
        return Err(FrameError::Version);
    }
    exact_item(bytes)
}

pub fn exact_item(bytes: &[u8]) -> Result<&[u8], FrameError> {
    let (item, rest) = split_item(bytes)?;
    if !rest.is_empty() {
        return Err(FrameError::Extent);
    }
    Ok(item)
}

/// The declared extent of the first item and any bytes after it.
pub fn split_item(bytes: &[u8]) -> Result<(&[u8], &[u8]), FrameError> {
    let (&tag, mut payload) = bytes.split_first().ok_or(FrameError::Extent)?;
    match tag {
        NULL => {}
        tag if tag & 0xe1 == SCALAR => match (tag >> 1) & 15 {
            1 | 3 => take(&mut payload, 1)?,
            2 => take_blob(&mut payload)?,
            4 => take(&mut payload, 2)?,
            5 | 7 | 11 => take(&mut payload, 4)?,
            6 | 8 => take(&mut payload, 8)?,
            9 => {
                take(&mut payload, 2)?;
                take_blob(&mut payload)?;
            }
            10 => {
                let (&number, tail) = payload.split_first().ok_or(FrameError::Extent)?;
                payload = tail;
                match number {
                    0 => {
                        take(&mut payload, 4)?;
                        take_blob(&mut payload)?;
                    }
                    1..=3 => {}
                    _ => return Err(FrameError::Scalar),
                }
            }
            12 => take(&mut payload, 9)?,
            13 => take(&mut payload, 13)?,
            14 => take(&mut payload, 17)?,
            15 => {
                take(&mut payload, 17)?;
                take_blob(&mut payload)?;
            }
            _ => return Err(FrameError::Tag),
        },
        tag if container_is_object(tag).is_some() => take_blob(&mut payload)?,
        _ => return Err(FrameError::Tag),
    }
    Ok(bytes.split_at(bytes.len() - payload.len()))
}

/// `Some(true)` for object tags and `Some(false)` for array tags.
pub fn container_is_object(tag: u8) -> Option<bool> {
    match tag {
        ARRAY => Some(false),
        OBJECT => Some(true),
        tag if tag & 0xe7 == INDEX_ARRAY => Some(false),
        tag if tag & 0xe1 == INDEX_OBJECT => Some(true),
        _ => None,
    }
}

/// Consume a canonical unsigned LEB128 length that fits a byte-slice index.
pub fn read_length(input: &mut &[u8]) -> Result<usize, FrameError> {
    let mut value = 0_u64;
    for shift in (0..64).step_by(7) {
        let (&byte, tail) = input.split_first().ok_or(FrameError::Extent)?;
        *input = tail;
        if shift == 63 && byte > 1 {
            return Err(FrameError::Extent);
        }
        value |= u64::from(byte & 127) << shift;
        if byte < 128 {
            if shift != 0 && byte == 0 {
                return Err(FrameError::Extent);
            }
            return usize::try_from(value).map_err(|_| FrameError::Extent);
        }
    }
    Err(FrameError::Extent)
}

fn take(input: &mut &[u8], size: usize) -> Result<(), FrameError> {
    *input = input.get(size..).ok_or(FrameError::Extent)?;
    Ok(())
}

fn take_blob(input: &mut &[u8]) -> Result<(), FrameError> {
    let size = read_length(input)?;
    take(input, size)
}
