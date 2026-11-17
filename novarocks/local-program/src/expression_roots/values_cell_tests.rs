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

//! Dynamic Values cells through the checked chain: each one is a ValuesCell
//! root collected from the Values node, bound to exactly one root use, typed
//! as its column and evaluated over an explicit empty port that has no input
//! source in scope.

use super::*;
use crate::{
    BindingRequirements, CompileProfile, ControlShape, KernelAbiVersion, ProgramChannelLayoutRole,
    ProgramChannelSite, ProgramChannelTypeError, ProgramEvaluationRegion, ProgramExpressionUse,
    ProgramLexicalBindingError, ProgramLexicalBindings, ProgramLexicalSource, ProgramNode,
    ProgramResolvedCalls, ProgramRootInput, ProgramSlotBinding, ProgramTypedChannels,
    ProgramTypedExpressions, ProgramUseRef, StaticExprKind, StaticExprNode, StaticLayout,
    StaticLiteral, StaticValues, StaticValuesCell, root_input_layout,
};
use arrow_array::{ArrayRef, BooleanArray};
use arrow_schema::{DataType, Field, Schema};
use novarocks_functions::{FunctionArgumentType, FunctionValueType};
use novarocks_type_contract::{EvaluationRegionId, ExpressionEffectContext};
use novarocks_types::SlotId;
use std::{collections::HashMap, num::NonZeroUsize};

struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}

const DOMAIN: EvaluationRegionId = EvaluationRegionId::new(u32::MAX);
const TRUE: usize = 0;
const NOT: usize = 1;
const SLOT: usize = 2;

/// 0: TRUE, 1: NOT(0), 2: the Values node's own output slot.
fn arena() -> Arc<ImmutableExpressions> {
    Arc::new(
        ImmutableExpressions::try_new(
            vec![
                StaticExprNode::new(
                    StaticExprKind::Literal(StaticLiteral::Bool(true)),
                    DataType::Boolean,
                    None,
                ),
                StaticExprNode::new(
                    StaticExprKind::Not(ProgramExprId::new(TRUE)),
                    DataType::Boolean,
                    None,
                ),
                StaticExprNode::new(
                    StaticExprKind::SlotId(SlotId::new(1)),
                    DataType::Boolean,
                    None,
                ),
            ],
            false,
            HashMap::new(),
            None,
        )
        .unwrap(),
    )
}
fn layout() -> StaticLayout {
    StaticLayout::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "flag",
            DataType::Boolean,
            false,
        )])),
        Arc::from([SlotId::new(1)]),
    )
    .unwrap()
}
/// Two rows: row 0 is the constant TRUE, row 1 is the dynamic `definition`.
fn graph(definition: usize) -> LocalProgramGraph {
    let layout = layout();
    let constants: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
    let values = StaticValues::try_new_with_cells_for_compile(
        2,
        vec![constants],
        vec![StaticValuesCell {
            row: 1,
            column: 0,
            definition: ProgramExprId::new(definition),
        }],
        layout.clone(),
        &Control,
    )
    .unwrap();
    LocalProgramGraph::try_new(
        vec![ProgramNode::new(
            1,
            ProgramNodeKind::Values { values },
            layout.clone(),
        )],
        ProgramNodeId::new(0),
        arena(),
        CompileProfile::new(
            NonZeroUsize::new(1).unwrap(),
            None,
            layout.identity().unwrap(),
            KernelAbiVersion::CURRENT,
        ),
        BindingRequirements::try_new(vec![]).unwrap(),
    )
    .unwrap()
}
fn cell_site(row: u32) -> ProgramExpressionRootSite {
    ProgramExpressionRootSite::Node {
        node: ProgramNodeId::new(0),
        role: ProgramNodeExpressionRole::ValuesCell { row, column: 0 },
    }
}
fn invocation(id: u32, definition: usize, arguments: &[u32]) -> ProgramExpressionUse {
    ProgramExpressionUse {
        context: ExpressionEffectContext {
            use_id: ExpressionUseId::new(id),
            region: DOMAIN,
            demand: EvaluationDemand::Value,
        },
        definition: ProgramExprId::new(definition),
        control: ControlShape::Eager,
        arguments: arguments
            .iter()
            .copied()
            .map(ExpressionUseId::new)
            .collect(),
    }
}
/// The dynamic cell's root use is 0; NOT reads its operand through use 1.
fn flow(definition: usize) -> BTreeMap<ProgramExpressionArena, ProgramControlFlow> {
    let uses = if definition == NOT {
        vec![invocation(0, NOT, &[1]), invocation(1, TRUE, &[])]
    } else {
        vec![invocation(0, definition, &[])]
    };
    let flow = ProgramControlFlow::try_new(
        vec![ProgramEvaluationRegion {
            id: DOMAIN,
            parent: None,
            guard: None,
        }],
        uses,
        3,
        &Control,
    )
    .unwrap();
    BTreeMap::from([(ProgramExpressionArena::Main, flow)])
}
fn bind(
    definition: usize,
    row: u32,
) -> Result<ProgramRootControlBindings, ProgramRootBindingError> {
    ProgramRootControlBindings::try_new(
        graph(definition),
        flow(definition),
        vec![ProgramRootUseBinding {
            site: cell_site(row),
            use_id: ExpressionUseId::new(0),
        }],
        &Control,
    )
}
fn boolean(nullable: bool) -> FunctionArgumentType {
    FunctionArgumentType::Value(FunctionValueType::new(DataType::Boolean, nullable))
}
fn channels(
    definition: usize,
    cell_nullable: bool,
) -> Result<ProgramTypedChannels, ProgramChannelTypeError> {
    let calls =
        ProgramResolvedCalls::try_new(bind(definition, 1).unwrap(), vec![], &Control).unwrap();
    let mut types = vec![boolean(false), boolean(false), boolean(false)];
    if cell_nullable {
        types[TRUE] = boolean(true);
        types[NOT] = boolean(true);
    }
    let typed = ProgramTypedExpressions::try_new(
        calls,
        BTreeMap::from([(ProgramExpressionArena::Main, types)]),
        &Control,
    )
    .unwrap();
    ProgramTypedChannels::try_new(
        typed,
        vec![(
            ProgramChannelSite::Layout {
                node: ProgramNodeId::new(0),
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal: 0,
            },
            FunctionValueType::new(DataType::Boolean, false),
        )],
        &Control,
    )
}

#[test]
fn dynamic_values_cells_are_empty_port_roots_through_the_checked_chain() {
    let graph = graph(NOT);
    let roots = ProgramExpressionRoots::collect(&graph, &Control).unwrap();
    assert_eq!(
        roots.sites().iter().collect::<Vec<_>>(),
        vec![(
            &cell_site(1),
            &ProgramExpressionRoot {
                definition: ProgramExprId::new(NOT),
                demand: EvaluationDemand::Value,
            }
        )]
    );
    assert_eq!(
        root_input_layout(&graph, cell_site(1)).unwrap(),
        ProgramRootInput::Empty
    );
    // Only the ValuesCell role has a port on a Values node.
    assert_eq!(
        root_input_layout(
            &graph,
            ProgramExpressionRootSite::Node {
                node: ProgramNodeId::new(0),
                role: ProgramNodeExpressionRole::ProjectOutput { expression: 0 },
            },
        ),
        Err(ProgramLexicalBindingError::InvalidSource)
    );
    let lexical =
        ProgramLexicalBindings::try_new(channels(NOT, false).unwrap(), vec![], vec![], &Control)
            .unwrap();
    assert!(lexical.slots().is_empty());
    assert_eq!(
        lexical
            .channels()
            .expressions()
            .resolved_calls()
            .snapshot()
            .bindings()
            .get(&cell_site(1)),
        Some(&ExpressionUseId::new(0))
    );
}

#[test]
fn dynamic_values_cell_roots_require_exact_bindings_and_column_types() {
    // The constant row is backing, not a root; only the dynamic position binds.
    assert!(matches!(
        bind(NOT, 0),
        Err(ProgramRootBindingError::InvalidRoot)
    ));
    assert!(matches!(
        ProgramRootControlBindings::try_new(graph(NOT), flow(NOT), vec![], &Control),
        Err(ProgramRootBindingError::IncompleteCoverage)
    ));
    // A nullable cell cannot fill a non-null column.
    assert!(matches!(
        channels(NOT, true),
        Err(ProgramChannelTypeError::TypeMismatch)
    ));
}

#[test]
fn dynamic_values_cell_has_no_input_source_in_scope() {
    let occurrence = ProgramUseRef {
        arena: ProgramExpressionArena::Main,
        use_id: ExpressionUseId::new(0),
    };
    let own_output = ProgramSlotBinding {
        occurrence,
        source: ProgramLexicalSource::Input(ProgramChannelSite::Layout {
            node: ProgramNodeId::new(0),
            role: ProgramChannelLayoutRole::NodeOutput,
            ordinal: 0,
        }),
    };
    assert!(matches!(
        ProgramLexicalBindings::try_new(
            channels(SLOT, false).unwrap(),
            vec![],
            vec![own_output],
            &Control
        ),
        Err(ProgramLexicalBindingError::WrongScope)
    ));
    assert!(matches!(
        ProgramLexicalBindings::try_new(channels(SLOT, false).unwrap(), vec![], vec![], &Control),
        Err(ProgramLexicalBindingError::IncompleteCoverage)
    ));
}
