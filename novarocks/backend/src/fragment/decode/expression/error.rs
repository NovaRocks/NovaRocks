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

//! Fragment expression decoder errors.

use std::fmt;

use novarocks_proto::{FieldPath, FieldPathSegment, ProtocolError, ProtocolErrorKind};

#[derive(Debug)]
pub(crate) struct NativeExpressionLeafDecodeError {
    kind: ProtocolErrorKind,
    relative_path: Vec<FieldPathSegment>,
    detail: String,
}

#[allow(
    dead_code,
    reason = "Retained for target-specific native integration and regression coverage."
)]
impl NativeExpressionLeafDecodeError {
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

    pub(crate) fn into_protocol(self, path: FieldPath) -> ProtocolError {
        ProtocolError::new(
            path.append_segments(self.relative_path),
            self.kind,
            self.detail,
        )
    }
}

impl fmt::Display for NativeExpressionLeafDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.detail.fmt(f)
    }
}

#[derive(Debug)]
pub(crate) struct NativeExpressionDecodeError(ProtocolError);

impl NativeExpressionDecodeError {
    pub(crate) fn missing(path: FieldPath, detail: impl fmt::Display) -> Self {
        Self::new(path, ProtocolErrorKind::MissingField, detail)
    }
    pub(crate) fn invalid_value(path: FieldPath, detail: impl fmt::Display) -> Self {
        Self::new(path, ProtocolErrorKind::InvalidValue, detail)
    }
    pub(crate) fn invalid_enum(path: FieldPath, detail: impl fmt::Display) -> Self {
        Self::new(path, ProtocolErrorKind::InvalidEnum, detail)
    }
    pub(crate) fn out_of_range(path: FieldPath, detail: impl fmt::Display) -> Self {
        Self::new(path, ProtocolErrorKind::OutOfRange, detail)
    }
    pub(crate) fn inconsistent(path: FieldPath, detail: impl fmt::Display) -> Self {
        Self::new(path, ProtocolErrorKind::InconsistentFields, detail)
    }
    pub(crate) fn unsupported(path: FieldPath, detail: impl fmt::Display) -> Self {
        Self::new(path, ProtocolErrorKind::Unsupported, detail)
    }
    fn new(path: FieldPath, kind: ProtocolErrorKind, detail: impl fmt::Display) -> Self {
        Self(ProtocolError::new(path, kind, detail.to_string()))
    }
    pub(crate) fn into_protocol(self) -> ProtocolError {
        self.0
    }

    #[cfg(test)]
    pub(crate) fn protocol(&self) -> &ProtocolError {
        &self.0
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, pattern: &str) -> bool {
        self.to_string().contains(pattern)
    }
}

impl From<ProtocolError> for NativeExpressionDecodeError {
    fn from(error: ProtocolError) -> Self {
        Self(error)
    }
}

impl fmt::Display for NativeExpressionDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
