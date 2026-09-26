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

use std::collections::BTreeSet;
use std::num::{NonZeroU32, NonZeroUsize};

use novarocks_execution_contract::FragmentContractVersion;

/// Every layout-dependent local specialization receives the effective DOP and
/// root sink width explicitly. The profile carries no task or process state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompileProfile {
    pipeline_dop: NonZeroUsize,
    root_sink_dop: Option<NonZeroUsize>,
    layout: LayoutIdentity,
    kernel_abi: KernelAbiVersion,
}

impl CompileProfile {
    pub const fn new(
        pipeline_dop: NonZeroUsize,
        root_sink_dop: Option<NonZeroUsize>,
        layout: LayoutIdentity,
        kernel_abi: KernelAbiVersion,
    ) -> Self {
        Self {
            pipeline_dop,
            root_sink_dop,
            layout,
            kernel_abi,
        }
    }

    pub const fn pipeline_dop(self) -> NonZeroUsize {
        self.pipeline_dop
    }

    pub const fn root_sink_dop(self) -> Option<NonZeroUsize> {
        self.root_sink_dop
    }

    pub const fn layout(self) -> LayoutIdentity {
        self.layout
    }

    pub const fn kernel_abi(self) -> KernelAbiVersion {
        self.kernel_abi
    }
}

/// Digest of the resolved slot order and types consumed by a specialization.
/// The builder computes the digest from canonical layout data; callers cannot
/// substitute a process-local pointer or a Task identity for this value.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct LayoutIdentity([u8; 32]);

impl LayoutIdentity {
    pub const fn from_sha256(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    pub const fn sha256(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct KernelAbiVersion(NonZeroU32);

impl KernelAbiVersion {
    pub const CURRENT: Self = Self(NonZeroU32::MIN);

    pub const fn new(value: NonZeroU32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FragmentProgramOptions {
    contract_version: FragmentContractVersion,
}

impl FragmentProgramOptions {
    pub const fn new(contract_version: FragmentContractVersion) -> Self {
        Self { contract_version }
    }

    pub const fn contract_version(&self) -> FragmentContractVersion {
        self.contract_version
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanAssignmentKind {
    File,
    BrokerFile,
    SchemaSelection,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScanSourceContract {
    assignment_kind: ScanAssignmentKind,
}

impl ScanSourceContract {
    pub const fn new(assignment_kind: ScanAssignmentKind) -> Self {
        Self { assignment_kind }
    }

    pub const fn assignment_kind(&self) -> ScanAssignmentKind {
        self.assignment_kind
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RuntimeFilterId(i32);

impl RuntimeFilterId {
    pub const fn new(value: i32) -> Self {
        Self(value)
    }

    pub const fn get(self) -> i32 {
        self.0
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RuntimeFilterContract {
    build_filters: BTreeSet<RuntimeFilterId>,
    probe_filters: BTreeSet<RuntimeFilterId>,
}

impl RuntimeFilterContract {
    pub fn new(
        build_filters: BTreeSet<RuntimeFilterId>,
        probe_filters: BTreeSet<RuntimeFilterId>,
    ) -> Self {
        Self {
            build_filters,
            probe_filters,
        }
    }

    pub fn build_filters(&self) -> &BTreeSet<RuntimeFilterId> {
        &self.build_filters
    }

    pub fn probe_filters(&self) -> &BTreeSet<RuntimeFilterId> {
        &self.probe_filters
    }

    pub fn has_bindings(&self) -> bool {
        !self.build_filters.is_empty() || !self.probe_filters.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FragmentSinkAssignmentKind {
    StreamDestinations,
    DestinationGroups(NonZeroUsize),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FragmentSinkAssignmentRequirement {
    None,
    Required(FragmentSinkAssignmentKind),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compile_profile_keeps_effective_widths_explicit() {
        let profile = CompileProfile::new(
            NonZeroUsize::new(4).unwrap(),
            Some(NonZeroUsize::new(1).unwrap()),
            LayoutIdentity::from_sha256([7; 32]),
            KernelAbiVersion::new(NonZeroU32::new(1).unwrap()),
        );
        assert_eq!(profile.pipeline_dop().get(), 4);
        assert_eq!(profile.root_sink_dop().unwrap().get(), 1);
        assert_eq!(profile.layout().sha256(), [7; 32]);
        assert_eq!(profile.kernel_abi().get(), 1);
    }
}
