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

use super::*;
use novarocks_constant_contract::ConstantError;
use novarocks_type_contract::{CompilePhase, PureCompileControl};
use std::sync::Mutex;

struct Control(Mutex<Vec<u32>>);
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode);
        self.0.lock().unwrap().push(units);
        Ok(())
    }
}

#[test]
fn dependency_refusals_preserve_the_first_cause_without_an_enclosing_tail() {
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for error in [
            NodeCodecError::from(ExpressionCodecError::Control(cause.clone())),
            NodeCodecError::from(BindingCodecError::Control(cause.clone())),
            NodeCodecError::from(TypeCodecError::Control(cause.clone())),
            NodeCodecError::from(
                crate::physical_writer_schema_v2::WriterSchemaCodecError::Type(
                    TypeCodecError::Control(cause.clone()),
                ),
            ),
            NodeCodecError::from(
                crate::physical_writer_schema_v2::WriterSchemaCodecError::Resources(
                    NodeCodecError::Control(cause.clone()),
                ),
            ),
            NodeCodecError::from(RelationCodecError::Control(cause.clone())),
            NodeCodecError::from(ConnectorPayloadCodecError::Control(cause.clone())),
            NodeCodecError::from(RelationCodecError::from(
                crate::physical_provider_read_v2::ProviderReadCodecError::from(
                    ConnectorPayloadCodecError::Control(cause.clone()),
                ),
            )),
            NodeCodecError::from(p::ConstantReferenceError::Control(cause.clone())),
            NodeCodecError::from(p::ConstantReferenceError::Constant(ConstantError::Control(
                cause.clone(),
            ))),
        ] {
            let control = Control(Mutex::new(Vec::new()));
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            work.step().unwrap();
            assert!(
                matches!(finish::<()>(work, Err(error)), Err(NodeCodecError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.0.lock().unwrap(), [0]);
        }
    }
    let control = Control(Mutex::new(Vec::new()));
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    work.step().unwrap();
    let error =
        p::ConstantReferenceError::Constant(ConstantError::Limit("selected source envelope"));
    assert!(matches!(
        finish::<()>(work, Err(error.into())),
        Err(NodeCodecError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    assert_eq!(*control.0.lock().unwrap(), [0]);
}

#[test]
fn ordinary_dependency_errors_retain_typed_details_and_completed_work() {
    for error in [
        NodeCodecError::from(BindingCodecError::InvalidShape("missing table signature")),
        NodeCodecError::from(ExpressionCodecError::InvalidShape(
            "missing scalar expression",
        )),
        NodeCodecError::from(p::ConstantReferenceError::Table(
            novarocks_constant_contract::ConstantTableError::MissingPool(p::ConstantPoolId::new(
                u32::MAX,
            )),
        )),
        NodeCodecError::from(RelationCodecError::InvalidShape("missing relation")),
        NodeCodecError::from(
            crate::physical_writer_schema_v2::WriterSchemaCodecError::Type(
                TypeCodecError::InvalidShape("missing writer type"),
            ),
        ),
        NodeCodecError::from(
            crate::physical_writer_schema_v2::WriterSchemaCodecError::Resources(
                NodeCodecError::Relation(RelationCodecError::InvalidShape("missing relation")),
            ),
        ),
        NodeCodecError::from(ConnectorPayloadCodecError::InvalidShape("missing payload")),
        NodeCodecError::Identity(ConnectorIdentityError::InvalidWriteTargetOrdinal),
    ] {
        let control = Control(Mutex::new(Vec::new()));
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        work.step().unwrap();
        let actual = finish::<()>(work, Err(error)).unwrap_err();
        assert!(std::error::Error::source(&actual).is_some());
        match actual {
            NodeCodecError::Binding(BindingCodecError::InvalidShape(message)) => {
                assert_eq!(message, "missing table signature")
            }
            NodeCodecError::Expression(ExpressionCodecError::InvalidShape(message)) => {
                assert_eq!(message, "missing scalar expression")
            }
            NodeCodecError::Constant(p::ConstantReferenceError::Table(
                novarocks_constant_contract::ConstantTableError::MissingPool(id),
            )) => {
                assert_eq!(id.get(), u32::MAX)
            }
            NodeCodecError::Relation(RelationCodecError::InvalidShape(message)) => {
                assert_eq!(message, "missing relation")
            }
            NodeCodecError::Type(TypeCodecError::InvalidShape(message)) => {
                assert_eq!(message, "missing writer type")
            }
            NodeCodecError::Payload(ConnectorPayloadCodecError::InvalidShape(message)) => {
                assert_eq!(message, "missing payload")
            }
            NodeCodecError::Identity(ConnectorIdentityError::InvalidWriteTargetOrdinal) => {}
            other => panic!("unexpected dependency error: {other:?}"),
        }
        assert_eq!(*control.0.lock().unwrap(), [0, 1]);
    }
}

#[test]
fn complete_numeric_model_rejects_all_known_axes_before_pending_quantum() {
    struct LateControl {
        trace: Mutex<Vec<u32>>,
        cause: CompileControlError,
    }
    impl PureCompileControl for LateControl {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut trace = self.trace.lock().unwrap();
            assert!(trace.len() < 2, "callback after late refusal");
            trace.push(units);
            if trace.len() == 1 {
                Ok(())
            } else {
                Err(self.cause.clone())
            }
        }
    }
    let model = Model {
        inputs: 1,
        refs: 1,
        items: 2,
        requests: 1,
        requested: 8,
        delegated_work: 0,
    };
    // Hand arithmetic: 256 + (1+2)*32 + (1+32) + 8*4 = 417.
    let exact = NodeProjectionLimits {
        max_input_nodes: 1,
        max_value_references: 1,
        max_list_items: 2,
        max_allocation_requests: 1,
        max_allocation_request_bytes: 8,
        max_coexisting_source_and_request_bytes: 72,
        max_work: 417,
        properties: PhysicalPropertyProjectionLimits {
            max_value_references: 0,
            max_allocation_requests: 0,
            max_allocation_request_bytes: 0,
            max_coexisting_source_and_request_bytes: 0,
            max_work: 0,
        },
    };
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for axis in 0..12 {
            let control = LateControl {
                trace: Mutex::new(Vec::new()),
                cause: cause.clone(),
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            for _ in 0..255 {
                work.step().unwrap();
            }
            let mut limits = exact;
            match axis {
                0 => limits.max_input_nodes -= 1,
                1 => limits.max_value_references -= 1,
                2 => limits.max_list_items -= 1,
                3 => limits.max_allocation_requests -= 1,
                4 => limits.max_allocation_request_bytes -= 1,
                5 => limits.max_coexisting_source_and_request_bytes -= 1,
                6 => limits.max_work -= 1,
                _ => {}
            }
            let result = match axis {
                0..=6 => model.facts(64, 0, limits, &mut work).map(|_| ()),
                7 => add(usize::MAX, 1).map(|_| ()),
                8 => mul(usize::MAX, 2).map(|_| ()),
                9 => bytes::<u64>(usize::MAX).map(|_| ()),
                10 => count_prefix(1, 3, 64, 0, exact, &mut work),
                11 => count_prefix(
                    1,
                    2,
                    64,
                    0,
                    NodeProjectionLimits {
                        max_work: 351,
                        ..exact
                    },
                    &mut work,
                ),
                _ => unreachable!(),
            };
            assert!(
                matches!(
                    finish(work, result),
                    Err(Error::Control(CompileControlError::ResourceExhausted))
                ),
                "axis {axis}"
            );
            assert_eq!(*control.trace.lock().unwrap(), [0], "axis {axis}");
        }
    }
    let control = Control(Mutex::new(Vec::new()));
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let facts = model.facts(64, 0, exact, &mut work).unwrap();
    assert_eq!(facts.cumulative_work_upper_bound, 417);
    assert_eq!(facts.coexisting_source_and_request_bytes_upper_bound, 72);
    finish(work, Ok(())).unwrap();
    assert_eq!(*control.0.lock().unwrap(), [0, 7]);
}

#[test]
fn authored_layout_requests_keep_empty_arc_header_and_checked_numeric_first_cause() {
    use crate::ipc_flat_stream_v2::reader_allocations::arc_layout;
    use arrow::datatypes::Field;
    use std::{alloc::Layout, sync::Arc};
    let mut model = Model::default();
    let payload = Layout::array::<Arc<Field>>(0).unwrap();
    let arc = arc_layout(payload).unwrap();
    // Independent pinned Arc header: two usize counters, empty aligned slice.
    assert_eq!(arc.size(), 2 * std::mem::size_of::<usize>());
    model.layout_request(arc, 1).unwrap();
    assert_eq!((model.requests, model.requested), (1, arc.size()));
    model
        .layout_request(Layout::array::<u8>(0).unwrap(), 1)
        .unwrap();
    assert_eq!((model.requests, model.requested), (1, arc.size()));
    assert!(matches!(
        model.layout_request(arc, usize::MAX),
        Err(NodeCodecError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
}
