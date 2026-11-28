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

//! The node-unit caller must observe real outer rows even when no scalar root exists.
use super::*;
use crate::*;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::{collections::BTreeMap, sync::Mutex};
struct Control {
    refuse: Option<CompileControlError>,
    observed: Mutex<Vec<(CompilePhase, u32)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode);
        assert!(units <= 256);
        self.observed.lock().unwrap().push((phase, units));
        if units > 0
            && let Some(cause) = self.refuse.clone()
        {
            Err(cause)
        } else {
            Ok(())
        }
    }
}
fn source(rows: usize) -> Fragment {
    let id = FragmentId::new(39);
    let node = NodeId::new(91);
    Fragment::from(crate::plan::FragmentParts {
        id,
        root: node,
        values: BTreeMap::new(),
        nodes: BTreeMap::from([(
            node,
            PhysicalNode {
                id: node,
                expressions: None,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: PhysicalProperties {
                    distribution: Distribution::Singleton,
                    row_multiplicity: RowMultiplicity::SingleCopy,
                    ordering: Box::default(),
                },
                output: OutputPort {
                    node,
                    columns: Box::default(),
                },
                kind: NodeKind::Values {
                    rows: vec![Box::default(); rows].into_boxed_slice(),
                },
            },
        )]),
        sink: FragmentSink::Noop,
        summaries: Box::default(),
        dop_domain: PipelineDopDomain {
            min: 1,
            max: 1,
            requires_power_of_two: false,
        },
        stage_placement: StagePlacementPolicy::Singleton,
        input_completion: crate::FragmentInputCompletion::MayShortCircuit,
        runtime_filters: Box::default(),
        call_requests: FragmentCallRequests::unpublished_empty(id),
        calls: FrozenFragmentCalls::unpublished_empty(id),
    })
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
    work.finish()
}
#[test]
fn empty_values_outer_rows_keep_original_borrowed_control_before_unbounded_walk() {
    let fragment = source(1000);
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Control {
            refuse: Some(cause.clone()),
            observed: Mutex::default(),
        };
        assert_eq!(run(&fragment, &control), Err(cause));
        assert_eq!(
            *control.observed.lock().unwrap(),
            [(CompilePhase::Decode, 0), (CompilePhase::Decode, 256)]
        );
    }
}
#[test]
fn empty_values_outer_rows_are_real_work_in_the_original_scope() {
    let fragment = source(1000);
    let control = Control {
        refuse: None,
        observed: Mutex::default(),
    };
    run(&fragment, &control).unwrap();
    let observed = control.observed.lock().unwrap();
    assert!(
        observed
            .iter()
            .map(|(_, units)| *units as usize)
            .sum::<usize>()
            >= 1000
    );
}
