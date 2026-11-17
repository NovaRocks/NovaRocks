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

//! Borrow the original node-local definitions across SQL construction stages.
//! The node association belongs to the lowering source; this view grants no
//! journal authentication, allocation capacity or cross-node lookup.
use novarocks_expression_contract::{
    BoundExprSet, BoundExprSetBuilder, ExprId, ExprNode, ExprRoot, RootOrdinal, SealedUnit,
};
use novarocks_physical_plan::{FragmentBuilder, FragmentId, NodeId, PhysicalNode};

/// Association with an actual scalar unit, not authentication or capacity.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum ScalarUnitOwner {
    Node(NodeId),
}

/// Original emission site. Only the existing node producer is wired here.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum ScalarSourceSite {
    Node { fragment: FragmentId, node: NodeId },
}
impl ScalarSourceSite {
    pub(super) const fn owner(self) -> ScalarUnitOwner {
        match self {
            Self::Node { node, .. } => ScalarUnitOwner::Node(node),
        }
    }
}

/// The site precedes the local definition, preserving original journal order.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct ScalarDefinitionAddress {
    pub(super) site: ScalarSourceSite,
    pub(super) definition: ExprId,
}
impl ScalarDefinitionAddress {
    pub(super) const fn new(site: ScalarSourceSite, definition: ExprId) -> Self {
        Self { site, definition }
    }
}

#[derive(Clone, Copy)]
enum Storage<'source> {
    Empty,
    Draft(&'source BoundExprSetBuilder),
    Sealed(&'source SealedUnit),
    Frozen(&'source BoundExprSet),
}
#[derive(Clone, Copy)]
pub(crate) struct UnitDefinitions<'source> {
    owner: ScalarUnitOwner,
    storage: Storage<'source>,
}
impl<'source> UnitDefinitions<'source> {
    pub(super) fn from_draft(fragment: &'source FragmentBuilder, node: NodeId) -> Self {
        Self {
            owner: ScalarUnitOwner::Node(node),
            storage: match fragment.construction_unit(node) {
                Some(draft) => Storage::Draft(draft),
                // A real zero-argument relation need not own a scalar unit. Any
                // attempted definition/root lookup still returns absence below.
                None => Storage::Empty,
            },
        }
    }
    pub(super) fn from_sealed(owner: ScalarUnitOwner, unit: &'source SealedUnit) -> Self {
        Self {
            owner,
            storage: Storage::Sealed(unit),
        }
    }
    pub(super) fn from_node(node: &'source PhysicalNode) -> Self {
        Self {
            owner: ScalarUnitOwner::Node(node.id),
            storage: match node.expressions.as_deref() {
                Some(unit) => Storage::Frozen(unit),
                None => Storage::Empty,
            },
        }
    }
    pub(super) const fn owner(self) -> ScalarUnitOwner {
        self.owner
    }
    pub(super) fn is_sealed_source(self, expected: &SealedUnit) -> bool {
        matches!(self.storage, Storage::Sealed(original) if std::ptr::eq(original, expected))
    }
    pub(super) fn len(self) -> usize {
        match self.storage {
            Storage::Empty => 0,
            Storage::Draft(draft) => draft.len(),
            Storage::Sealed(unit) => unit.definitions().len(),
            Storage::Frozen(unit) => unit.definitions().len(),
        }
    }
    pub(super) fn definition(self, id: ExprId) -> Option<&'source ExprNode> {
        match self.storage {
            Storage::Empty => None,
            Storage::Draft(draft) => draft.definition(id),
            Storage::Sealed(unit) => unit.definitions().get(id.get() as usize),
            Storage::Frozen(unit) => unit.definition(id),
        }
    }
    pub(super) fn roots(self) -> &'source [ExprRoot] {
        match self.storage {
            Storage::Empty => &[],
            Storage::Draft(draft) => draft.roots(),
            Storage::Sealed(unit) => unit.roots(),
            Storage::Frozen(unit) => unit.roots(),
        }
    }
    pub(super) fn root(self, root: RootOrdinal) -> Option<&'source ExprRoot> {
        self.roots().get(root.get() as usize)
    }
    pub(super) fn root_definition(self, root: RootOrdinal) -> Option<&'source ExprNode> {
        self.definition(self.root(root)?.definition)
    }
}
