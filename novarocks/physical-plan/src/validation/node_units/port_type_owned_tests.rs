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

//! Exact node-port type equality borrows the caller's actual work throughout.
use super::*;
use crate::*;
use arrow_schema::{DataType, Field};
use novarocks_expression_contract::{BoundExprSetBuilder, ExprKind, ExprPort, UnitLimits};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDemand, PureCompileControl,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
struct Control {
    stop: Option<(usize, CompileControlError)>,
    trace: Mutex<Vec<(CompilePhase, u32)>>,
}
impl Control {
    fn good() -> Self {
        Self {
            stop: None,
            trace: Mutex::default(),
        }
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(
            phase,
            CompilePhase::Decode,
            "node port comparison created another scope"
        );
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after original refusal");
        }
        trace.push((phase, units));
        if let Some((stop, cause)) = &self.stop
            && at == *stop
        {
            Err(cause.clone())
        } else {
            Ok(())
        }
    }
}
struct Setup;
impl PureCompileControl for Setup {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn properties() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Singleton,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}
fn ty(fields: usize) -> ValueType {
    if fields == 0 {
        ValueType::new(DataType::Int64, false)
    } else {
        ValueType::new(
            DataType::Struct(
                (0..fields)
                    .map(|ordinal| {
                        Arc::new(Field::new(
                            format!("field_{ordinal}"),
                            DataType::Int64,
                            false,
                        ))
                    })
                    .collect(),
            ),
            false,
        )
    }
}
fn source(fields: usize) -> Fragment {
    let fragment = FragmentId::new(29);
    let input = NodeId::new(0);
    let filter = NodeId::new(1);
    let wide = ValueId::new(0);
    let predicate = ValueId::new(1);
    // Two constructor calls create independent immutable semantic roots and
    // layout trees. Equality cannot be answered by a shared source Arc alone.
    let producer_type = ty(fields);
    let declared_type = ty(fields);
    assert_eq!(producer_type, declared_type);
    assert!(!std::ptr::eq(
        producer_type.value_type().kind(),
        declared_type.value_type().kind()
    ));
    let boolean = ValueType::new(DataType::Boolean, false);
    let mut builder = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: PlanLimits::FROZEN.expression_semantic_depth,
    });
    let definition = builder
        .add(None, boolean.clone(), ExprKind::Value(predicate))
        .unwrap();
    let root = builder
        .root(definition, EvaluationDemand::TruthOnly, RootScope::Port(0))
        .unwrap();
    let mut work = CompileCheckpoints::try_new(&Setup, CompilePhase::Validate).unwrap();
    let unit = builder
        .seal(
            vec![ExprPort {
                values: Box::new([(wide, declared_type), (predicate, boolean.clone())]),
            }],
            &mut work,
        )
        .unwrap()
        .finish(BTreeMap::new(), &mut work)
        .unwrap();
    work.finish().unwrap();
    let fragment = Fragment::from(crate::plan::FragmentParts {
        id: fragment,
        root: filter,
        values: BTreeMap::from([
            (
                wide,
                ValueDef {
                    id: wide,
                    ty: producer_type,
                    origin: ValueOrigin::NodeOutput {
                        node: input,
                        output_ordinal: 0,
                    },
                },
            ),
            (
                predicate,
                ValueDef {
                    id: predicate,
                    ty: boolean,
                    origin: ValueOrigin::NodeOutput {
                        node: input,
                        output_ordinal: 1,
                    },
                },
            ),
        ]),
        nodes: BTreeMap::from([
            (
                input,
                PhysicalNode {
                    id: input,
                    inputs: Box::default(),
                    required_inputs: Box::default(),
                    output_properties: properties(),
                    output: OutputPort {
                        node: input,
                        columns: Box::new([wide, predicate]),
                    },
                    kind: NodeKind::Values {
                        rows: Box::default(),
                    },
                    expressions: None,
                },
            ),
            (
                filter,
                PhysicalNode {
                    id: filter,
                    inputs: Box::new([input]),
                    required_inputs: Box::new([properties()]),
                    output_properties: properties(),
                    output: OutputPort {
                        node: filter,
                        columns: Box::new([wide, predicate]),
                    },
                    kind: NodeKind::Filter {
                        predicates: Box::new([root]),
                    },
                    expressions: Some(Arc::new(unit)),
                },
            ),
        ]),
        sink: FragmentSink::Noop,
        summaries: Box::default(),
        dop_domain: PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        },
        stage_placement: StagePlacementPolicy::Singleton,
        runtime_filters: Box::default(),
        call_requests: FragmentCallRequests::unpublished_empty(fragment),
        calls: FrozenFragmentCalls::unpublished_empty(fragment),
    });
    // The source and root unit are complete, exact and legal before the
    // isolated node-unit owner is measured. No malformed input masks the walk.
    crate::validation::validate_fragment_definition(&fragment).unwrap();
    fragment
}
fn run(fragment: &Fragment, control: &Control) -> Result<(), CompileControlError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let mut errors = ValidationContext::with_limits(PlanLimits::FROZEN);
    validate_node_unit_in(
        fragment,
        &fragment.nodes()[&fragment.root()],
        &mut errors,
        &mut work,
    )?;
    assert!(
        errors.is_empty(),
        "the independent exact port fixture must remain valid"
    );
    work.finish()
}
fn units(control: &Control) -> u64 {
    control
        .trace
        .lock()
        .unwrap()
        .iter()
        .map(|(_, n)| u64::from(*n))
        .sum()
}
#[test]
fn equal_full_port_types_observe_the_actual_wide_structure_without_source_alias_shortcut() {
    let narrow = source(0);
    let wide = source(512);
    let narrow_control = Control::good();
    let wide_control = Control::good();
    run(&narrow, &narrow_control).unwrap();
    run(&wide, &wide_control).unwrap();
    // The unused first input has identical port cardinality and root/unit
    // topology in both fixtures. Only this exact full-type comparison grows.
    assert!(
        units(&wide_control) >= units(&narrow_control) + 512,
        "512 actual Struct fields were compared without original work: narrow={}, wide={}",
        units(&narrow_control),
        units(&wide_control)
    );
}
#[test]
fn equal_wide_port_types_keep_all_original_callback_causes_and_exact_prefixes() {
    let fragment = source(512);
    let baseline = Control::good();
    run(&fragment, &baseline).unwrap();
    let expected = baseline.trace.into_inner().unwrap();
    assert!(
        expected.iter().any(|(_, units)| *units == 256),
        "the actual full type comparison must reach a work quantum"
    );
    for at in 0..expected.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                stop: Some((at, cause.clone())),
                trace: Mutex::default(),
            };
            assert_eq!(run(&fragment, &control), Err(cause));
            assert_eq!(*control.trace.lock().unwrap(), expected[..=at]);
        }
    }
}
