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

use std::error::Error;
use std::fmt;

use novarocks::protocol::common::error::FieldPathSegment;
use novarocks::protocol::{FieldPath, ProtocolError, ProtocolErrorKind, ProtocolFamily};

/// Typed error used by nested native-protobuf leaf decoders before the owning
/// DTO boundary attaches its exact [`FieldPath`].
#[derive(Debug)]
pub(crate) struct NativeFragmentLeafDecodeError {
    kind: ProtocolErrorKind,
    relative_path: Vec<FieldPathSegment>,
    detail: String,
}

impl NativeFragmentLeafDecodeError {
    pub(crate) fn at_collection(kind: ProtocolErrorKind, detail: impl fmt::Display) -> Self {
        Self {
            kind,
            relative_path: Vec::new(),
            detail: detail.to_string(),
        }
    }

    pub(crate) fn at_field(
        kind: ProtocolErrorKind,
        field: &'static str,
        detail: impl fmt::Display,
    ) -> Self {
        Self {
            kind,
            relative_path: vec![FieldPathSegment::Field(field)],
            detail: detail.to_string(),
        }
    }

    pub(crate) fn prepend_field(mut self, field: &'static str) -> Self {
        self.relative_path.insert(0, FieldPathSegment::Field(field));
        self
    }

    pub(crate) fn prepend_index(mut self, index: usize) -> Self {
        self.relative_path.insert(0, FieldPathSegment::Index(index));
        self
    }

    pub(crate) fn append_field(mut self, field: &'static str) -> Self {
        self.relative_path.push(FieldPathSegment::Field(field));
        self
    }

    pub(crate) fn append_index(mut self, index: usize) -> Self {
        self.relative_path.push(FieldPathSegment::Index(index));
        self
    }

    pub(crate) fn into_native(self, path: FieldPath) -> NativeFragmentDecodeError {
        NativeFragmentDecodeError::protocol_error(
            path.append_segments(self.relative_path),
            self.kind,
            self.detail,
        )
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, pattern: &str) -> bool {
        self.detail.contains(pattern)
    }
}

impl fmt::Display for NativeFragmentLeafDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.detail.fmt(f)
    }
}

impl Error for NativeFragmentLeafDecodeError {}

pub(crate) trait NativeFragmentErrorAtPath {
    fn into_native_at(self, path: FieldPath) -> NativeFragmentDecodeError;
}

impl NativeFragmentErrorAtPath for NativeFragmentLeafDecodeError {
    fn into_native_at(self, path: FieldPath) -> NativeFragmentDecodeError {
        self.into_native(path)
    }
}

impl NativeFragmentErrorAtPath for String {
    fn into_native_at(self, path: FieldPath) -> NativeFragmentDecodeError {
        NativeFragmentDecodeError::invalid_value(path, self)
    }
}

#[cfg(test)]
impl PartialEq<&str> for NativeFragmentLeafDecodeError {
    fn eq(&self, other: &&str) -> bool {
        self.detail == *other
    }
}

#[derive(Debug)]
pub(crate) enum NativeFragmentDecodeError {
    Protocol(ProtocolError),
    Plan(novarocks::exec::fragment::error::ExecPlanBuildError),
    Binding(novarocks::exec::fragment::error::FragmentBindingError),
}

impl NativeFragmentDecodeError {
    #[cfg(test)]
    pub(crate) fn contains(&self, pattern: &str) -> bool {
        self.to_string().contains(pattern)
    }

    pub(crate) fn protocol(&self) -> Option<&ProtocolError> {
        match self {
            Self::Protocol(error) => Some(error),
            Self::Plan(_) | Self::Binding(_) => None,
        }
    }

    pub(crate) fn missing(path: FieldPath, detail: impl fmt::Display) -> Self {
        Self::protocol_error(path, ProtocolErrorKind::MissingField, detail)
    }

    pub(crate) fn invalid_value(path: FieldPath, detail: impl fmt::Display) -> Self {
        Self::protocol_error(path, ProtocolErrorKind::InvalidValue, detail)
    }

    pub(crate) fn invalid_enum(path: FieldPath, detail: impl fmt::Display) -> Self {
        Self::protocol_error(path, ProtocolErrorKind::InvalidEnum, detail)
    }

    pub(crate) fn out_of_range(path: FieldPath, detail: impl fmt::Display) -> Self {
        Self::protocol_error(path, ProtocolErrorKind::OutOfRange, detail)
    }

    pub(crate) fn inconsistent(path: FieldPath, detail: impl fmt::Display) -> Self {
        Self::protocol_error(path, ProtocolErrorKind::InconsistentFields, detail)
    }

    pub(crate) fn unsupported(path: FieldPath, detail: impl fmt::Display) -> Self {
        Self::protocol_error(path, ProtocolErrorKind::Unsupported, detail)
    }

    pub(crate) fn map_invalid<T, E: NativeFragmentErrorAtPath>(
        path: FieldPath,
        result: Result<T, E>,
    ) -> Result<T, Self> {
        result.map_err(|error| error.into_native_at(path))
    }

    fn protocol_error(path: FieldPath, kind: ProtocolErrorKind, detail: impl fmt::Display) -> Self {
        Self::Protocol(ProtocolError::new(
            ProtocolFamily::Native,
            path,
            kind,
            detail.to_string(),
        ))
    }
}

#[cfg(test)]
impl PartialEq<&str> for NativeFragmentDecodeError {
    fn eq(&self, other: &&str) -> bool {
        self.to_string().ends_with(other)
    }
}

impl fmt::Display for NativeFragmentDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(error) => error.fmt(f),
            Self::Plan(error) => error.fmt(f),
            Self::Binding(error) => error.fmt(f),
        }
    }
}

impl Error for NativeFragmentDecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Protocol(error) => Some(error),
            Self::Plan(error) => Some(error),
            Self::Binding(error) => Some(error),
        }
    }
}

impl From<ProtocolError> for NativeFragmentDecodeError {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}

impl From<novarocks::exec::fragment::error::ExecPlanBuildError> for NativeFragmentDecodeError {
    fn from(error: novarocks::exec::fragment::error::ExecPlanBuildError) -> Self {
        Self::Plan(error)
    }
}

impl From<novarocks::exec::fragment::error::FragmentBindingError> for NativeFragmentDecodeError {
    fn from(error: novarocks::exec::fragment::error::FragmentBindingError) -> Self {
        Self::Binding(error)
    }
}
