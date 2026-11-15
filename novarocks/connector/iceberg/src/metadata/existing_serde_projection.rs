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

// Checked Nova output projection, preserving the existing Iceberg serializer.
// Original borrowed Iceberg Serialize is deliberately retained (D17).
use novarocks_spi::connector::{ConnectorError, ConnectorPayloadRetentionGuard};
use serde::Serialize;
use std::io;

pub(crate) enum Error<E = ConnectorError> {
    Control(E),
    Json,
    Overflow,
    ShapeChanged,
}
impl<E> std::fmt::Debug for Error<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Control(_) => "Control",
            Self::Json => "Json",
            Self::Overflow => "Overflow",
            Self::ShapeChanged => "ShapeChanged",
        })
    }
}
impl<E> std::fmt::Display for Error<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}
pub(crate) trait OriginalScope<E = ConnectorError> {
    fn check_active(&self) -> Result<(), E>;
    fn original_guard(&self) -> ConnectorPayloadRetentionGuard;
}

// Library errors are retained as objects; no arbitrary Display into a String.
// io::Error::from(kind) is allocation-free. Original control error stays here.
struct Writer<'a, E> {
    scope: &'a dyn OriginalScope<E>,
    original: Option<E>,
    length: u64,
    output: Option<&'a mut Vec<u8>>,
    ceiling: u64,
    changed: bool,
}
impl<E> io::Write for Writer<'_, E> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.original.is_some() {
            return Err(io::ErrorKind::Other.into());
        }
        if let Err(error) = self.scope.check_active() {
            self.original = Some(error);
            // write_all retries Interrupted. Keep the first cause and exit.
            return Err(io::ErrorKind::Other.into());
        }
        let Some(next) = self.length.checked_add(bytes.len() as u64) else {
            self.changed = true;
            return Err(io::ErrorKind::InvalidData.into());
        };
        if next > self.ceiling {
            self.changed = true;
            return Err(io::ErrorKind::InvalidData.into());
        }
        if let Some(output) = &mut self.output {
            output.extend_from_slice(bytes);
        }
        self.length = next;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn serialize<T: Serialize + ?Sized, E>(
    value: &T,
    scope: &dyn OriginalScope<E>,
    output: Option<&mut Vec<u8>>,
    ceiling: u64,
) -> Result<u64, Error<E>> {
    scope.check_active().map_err(Error::Control)?;
    let mut writer = Writer {
        scope,
        original: None,
        length: 0,
        output,
        ceiling,
        changed: false,
    };
    let result = serde_json::to_writer(&mut writer, value);
    if let Some(error) = writer.original {
        return Err(Error::Control(error));
    }
    if writer.changed {
        return Err(Error::ShapeChanged);
    }
    result.map_err(|_| Error::Json)?;
    scope.check_active().map_err(Error::Control)?;
    Ok(writer.length)
}
pub(crate) fn count<T: Serialize + ?Sized, E>(
    value: &T,
    scope: &dyn OriginalScope<E>,
) -> Result<u64, Error<E>> {
    serialize(value, scope, None, u64::MAX)
}
pub(crate) fn exact_json<T: Serialize + ?Sized, E>(
    value: &T,
    length: u64,
    scope: &dyn OriginalScope<E>,
) -> Result<String, Error<E>> {
    let length = usize::try_from(length).map_err(|_| Error::Overflow)?;
    let mut bytes = Vec::with_capacity(length);
    if bytes.capacity() != length {
        return Err(Error::ShapeChanged);
    }
    let actual = serialize(value, scope, Some(&mut bytes), length as u64)?;
    if actual != length as u64 {
        return Err(Error::ShapeChanged);
    }
    // Serializer produces UTF-8; conversion transfers this SAME allocation.
    String::from_utf8(bytes).map_err(|_| Error::ShapeChanged)
}
#[cfg(test)]
#[path = "projection_tests.rs"]
mod tests;
