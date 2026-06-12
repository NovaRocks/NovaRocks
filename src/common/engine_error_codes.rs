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

use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum EngineErrorCode {
    TypeMismatch,
    TypeDeterminismViolation,
    ExchangeDescriptorMismatch,
    AggregateStateLayoutMismatch,
    IcebergWriteDescriptorMismatch,
    UnsupportedDistributedDmlShape,
    DistributedWriteOutputMismatch,
    WriteCoordinatorGone,
    CommitKnownUncommitted,
    CommitUnknown,
    ProtocolDecodeError,
    InternalInvariantViolation,
}

impl EngineErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TypeMismatch => "TypeMismatch",
            Self::TypeDeterminismViolation => "TypeDeterminismViolation",
            Self::ExchangeDescriptorMismatch => "ExchangeDescriptorMismatch",
            Self::AggregateStateLayoutMismatch => "AggregateStateLayoutMismatch",
            Self::IcebergWriteDescriptorMismatch => "IcebergWriteDescriptorMismatch",
            Self::UnsupportedDistributedDmlShape => "UnsupportedDistributedDmlShape",
            Self::DistributedWriteOutputMismatch => "DistributedWriteOutputMismatch",
            Self::WriteCoordinatorGone => "WriteCoordinatorGone",
            Self::CommitKnownUncommitted => "CommitKnownUncommitted",
            Self::CommitUnknown => "CommitUnknown",
            Self::ProtocolDecodeError => "ProtocolDecodeError",
            Self::InternalInvariantViolation => "InternalInvariantViolation",
        }
    }

    pub fn parse(input: &str) -> Option<Self> {
        match input {
            "TypeMismatch" => Some(Self::TypeMismatch),
            "TypeDeterminismViolation" => Some(Self::TypeDeterminismViolation),
            "ExchangeDescriptorMismatch" => Some(Self::ExchangeDescriptorMismatch),
            "AggregateStateLayoutMismatch" => Some(Self::AggregateStateLayoutMismatch),
            "IcebergWriteDescriptorMismatch" => Some(Self::IcebergWriteDescriptorMismatch),
            "UnsupportedDistributedDmlShape" => Some(Self::UnsupportedDistributedDmlShape),
            "DistributedWriteOutputMismatch" => Some(Self::DistributedWriteOutputMismatch),
            "WriteCoordinatorGone" => Some(Self::WriteCoordinatorGone),
            "CommitKnownUncommitted" => Some(Self::CommitKnownUncommitted),
            "CommitUnknown" => Some(Self::CommitUnknown),
            "ProtocolDecodeError" => Some(Self::ProtocolDecodeError),
            "InternalInvariantViolation" => Some(Self::InternalInvariantViolation),
            _ => None,
        }
    }
}

impl fmt::Display for EngineErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
