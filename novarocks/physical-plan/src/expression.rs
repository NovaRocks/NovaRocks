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

//! Relational bindings, ordering and window-frame specifications.
// Design: ADR-0238-self-contained-expression-units-and-explicit-environments.md
use crate::ValueType;
use novarocks_expression_contract::{
    BoundFunction, LegacyBindingMetadata, MissingLegacyBindingMetadata, RootOrdinal,
};
use novarocks_type_contract::{
    AggregateStateFormatId, FunctionArgumentTypes, FunctionId, FunctionOverloadId, TypedFactSlice,
};

/// Exact binding for a function whose result is a relation rather than a
/// scalar value. Keeping this separate prevents outer-input pass-through
/// columns from being mistaken for function result columns.
#[derive(Clone, Debug, PartialEq)]
pub struct BoundTableFunction {
    pub function_id: FunctionId,
    pub overload: FunctionOverloadId,
    pub argument_types: FunctionArgumentTypes,
    pub result_types: TypedFactSlice<ValueType>,
    /// Original legacy producer provenance, absent on a V2 exact signature.
    pub legacy_metadata: Option<LegacyBindingMetadata>,
}
impl BoundTableFunction {
    /// Relation signature only; pass-through columns and actual occurrence
    /// effects are separate facts, never inferred from these result types.
    pub fn from_exact_signature(
        function_id: FunctionId,
        overload: FunctionOverloadId,
        argument_types: FunctionArgumentTypes,
        result_types: TypedFactSlice<ValueType>,
    ) -> Self {
        Self {
            function_id,
            overload,
            argument_types,
            result_types,
            legacy_metadata: None,
        }
    }

    pub fn require_legacy_metadata(
        &self,
    ) -> Result<&LegacyBindingMetadata, MissingLegacyBindingMetadata> {
        self.legacy_metadata
            .as_ref()
            .ok_or(MissingLegacyBindingMetadata)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregatePhase {
    Single,
    Partial {
        sequence: crate::AggregateSequenceId,
    },
    Intermediate {
        sequence: crate::AggregateSequenceId,
    },
    Final {
        sequence: crate::AggregateSequenceId,
    },
}

impl AggregatePhase {
    pub const fn sequence(self) -> Option<crate::AggregateSequenceId> {
        match self {
            Self::Single => None,
            Self::Partial { sequence }
            | Self::Intermediate { sequence }
            | Self::Final { sequence } => Some(sequence),
        }
    }

    pub const fn consumes_logical_arguments(self) -> bool {
        matches!(self, Self::Single | Self::Partial { .. })
    }

    pub const fn produces_final_result(self) -> bool {
        matches!(self, Self::Single | Self::Final { .. })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggregateBinding {
    /// Original state-row policy, independent of this phase's input channels.
    pub state_interpretation: Option<novarocks_type_contract::AggregateStateInterpretation>,
    pub state_argument_contract: novarocks_type_contract::AggregateStateArgumentContract,
    pub function: BoundFunction,
    pub phase: AggregatePhase,
    /// Number of logical SQL arguments at the front of `argument_types`.
    /// Remaining channels are aggregate-owned ORDER BY inputs.
    pub logical_argument_count: u32,
    pub intermediate_type: ValueType,
    pub state_format: AggregateStateFormatId,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum NullOrdering {
    First,
    Last,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SortExpr {
    pub expr: RootOrdinal,
    pub direction: SortDirection,
    pub null_ordering: NullOrdering,
}

pub use novarocks_type_contract::{WindowFrameExclusion, WindowFrameUnits};
pub type WindowBound = novarocks_type_contract::WindowBound<crate::ConstantReference>;
pub type WindowFrame = novarocks_type_contract::WindowFrame<crate::ConstantReference>;

/// Read one direct input value through this node's exact root.
pub fn expression_value(
    fragment: &crate::Fragment,
    node: crate::NodeId,
    root: RootOrdinal,
) -> Option<crate::ValueId> {
    match fragment.root_expression(node, root)?.kind {
        crate::ExprKind::Value(value) => Some(value),
        _ => None,
    }
}
/// Preserve key identity only through an injective widening signed conversion.
pub fn join_key_source_value(
    fragment: &crate::Fragment,
    node: crate::NodeId,
    root: RootOrdinal,
) -> Option<crate::ValueId> {
    fn read(unit: &crate::BoundExprSet, id: crate::ExprId) -> Option<crate::ValueId> {
        let definition = unit.definition(id)?;
        match &definition.kind {
            crate::ExprKind::Value(value) => Some(*value),
            crate::ExprKind::Cast { expr, target, .. } => {
                let from = unit.definition(*expr)?.ty.data_type();
                fn width(ty: &arrow_schema::DataType) -> Option<u8> {
                    use arrow_schema::DataType::*;
                    match ty {
                        Int8 => Some(1),
                        Int16 => Some(2),
                        Int32 => Some(4),
                        Int64 => Some(8),
                        _ => None,
                    }
                }
                if from == target || matches!((width(from),width(target)),(Some(a),Some(b)) if b>=a)
                {
                    read(unit, *expr)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
    let unit = fragment.unit(node)?;
    read(unit, unit.root(root)?.definition)
}
pub fn ordering_keys(
    fragment: &crate::Fragment,
    node: crate::NodeId,
    partition: &[SortExpr],
    order: &[SortExpr],
) -> Option<Vec<crate::OrderingKey>> {
    partition
        .iter()
        .chain(order)
        .map(|item| {
            expression_value(fragment, node, item.expr).map(|value| crate::OrderingKey {
                value,
                direction: item.direction,
                null_ordering: item.null_ordering,
            })
        })
        .collect()
}
/// Definition closure is independent of occurrence effect authentication.
pub(crate) fn expressions_have_closed_value_scope(
    unit: &crate::BoundExprSet,
    roots: impl IntoIterator<Item = RootOrdinal>,
) -> bool {
    let mut pending = Vec::new();
    for ordinal in roots {
        let Some(root) = unit.root(ordinal) else {
            return false;
        };
        pending.push(root.definition);
    }
    let mut visited = std::collections::BTreeSet::new();
    while let Some(id) = pending.pop() {
        if !visited.insert(id) {
            continue;
        }
        let Some(definition) = unit.definition(id) else {
            return false;
        };
        if matches!(definition.kind, crate::ExprKind::Value(_)) {
            return false;
        }
        definition
            .kind
            .expression_references_observed::<std::convert::Infallible>(|id| {
                pending.push(id);
                Ok(())
            })
            .unwrap_or_else(|never| match never {});
    }
    true
}
