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

use novarocks_expression_contract::{ExprKind, RootScope};
use novarocks_physical_plan as p;
use novarocks_plan_codec::{
    physical_package_v2::*, resource_preflight_v2::FragmentDecodeResourceModel,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, EvaluationDemand, PureCompileControl,
};
use prost::Message;
use std::{collections::BTreeMap, sync::Arc};
struct Control;
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}
fn install(builder: &mut p::FragmentBuilder, node: p::NodeId) {
    let ports = builder.node_ports(node).unwrap();
    let draft = builder.take_unit_builder(node).unwrap();
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Validate).unwrap();
    let unit = draft
        .seal(ports, &mut work)
        .unwrap()
        .finish(BTreeMap::new(), &mut work)
        .unwrap();
    work.finish().unwrap();
    builder.install_unit(node, Arc::new(unit)).unwrap();
}
fn pair() -> p::FragmentPackage {
    pair_with_unary(false)
}
fn pair_with_unary(unary: bool) -> p::FragmentPackage {
    let policy = FROZEN_CONSTANT_POLICY;
    let ty = p::ValueType::new(arrow::datatypes::DataType::Int64, false);
    let cv = p::ConstantValue::from_i64(
        Arc::new(ty.try_to_field("exact").unwrap()),
        ty.clone(),
        41,
        policy,
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    pair_with_constant(cv, unary)
}
fn pair_with_constant(cv: p::ConstantValue, unary: bool) -> p::FragmentPackage {
    let policy = FROZEN_CONSTANT_POLICY;
    let ty = cv.value_type().clone();
    let address = p::ConstantReference {
        pool: p::ConstantPoolId::new(u32::MAX),
        ordinal: cv.ordinal(),
    };
    let mut pools = p::ConstantPools::empty();
    pools.insert(address.pool, cv.pool().clone()).unwrap();
    let mut builder = p::FragmentBuilder::new(p::FragmentId::new(0));
    let values = builder.reserve_node_id().unwrap();
    let value = builder
        .add_value(
            ty.clone(),
            p::ValueOrigin::NodeOutput {
                node: values,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let definition = builder
        .unit(values)
        .unwrap()
        .add(None, ty.clone(), ExprKind::Constant(address))
        .unwrap();
    let root = builder
        .unit(values)
        .unwrap()
        .root(definition, EvaluationDemand::Value, RootScope::AllPorts)
        .unwrap();
    builder
        .add_values(
            values,
            vec![vec![root].into_boxed_slice()].into_boxed_slice(),
            vec![value].into_boxed_slice(),
        )
        .unwrap();
    install(&mut builder, values);
    let project = builder.reserve_node_id().unwrap();
    let definition = builder
        .unit(project)
        .unwrap()
        .add(None, ty.clone(), ExprKind::Value(value))
        .unwrap();
    let projected_type = if unary {
        p::ValueType::new(arrow::datatypes::DataType::Boolean, false)
    } else {
        ty
    };
    let definition = if unary {
        builder
            .unit(project)
            .unwrap()
            .add(
                None,
                projected_type.clone(),
                ExprKind::Unary {
                    op: novarocks_expression_contract::UnaryOperator::Not,
                    expr: definition,
                },
            )
            .unwrap()
    } else {
        definition
    };
    let root = builder
        .unit(project)
        .unwrap()
        .root(definition, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let repeated_root = builder
        .unit(project)
        .unwrap()
        .root(definition, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    let projected = builder
        .add_value(
            projected_type,
            p::ValueOrigin::Expr {
                node: project,
                root,
            },
        )
        .unwrap();
    builder
        .add_project(
            project,
            values,
            vec![(root, projected), (repeated_root, projected)].into_boxed_slice(),
            vec![projected, projected].into_boxed_slice(),
        )
        .unwrap();
    install(&mut builder, project);
    let fragment = builder
        .finish_definition(
            project,
            p::FragmentSink::Noop,
            p::PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            p::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    let mut plan = p::PlanBuilder::new(p::PlanVersionId::try_new([1; 16]).unwrap(), policy)
        .with_constant_pools(pools);
    plan.add_fragment(fragment).unwrap();
    let plan = plan.finish_observed(&Control).unwrap();
    let fragments = p::extract_fragment_packages(
        &plan,
        &BTreeMap::new(),
        &BTreeMap::new(),
        &BTreeMap::from([(
            p::FragmentId::new(0),
            p::FrozenFragmentPruning::try_new(p::FragmentId::new(0), vec![], &Control).unwrap(),
        )]),
        &BTreeMap::from([(p::FragmentId::new(0), FROZEN_PACKAGE_ADMISSION)]),
        &Control,
    )
    .unwrap();
    fragments.into_values().next().unwrap()
}

#[test]
fn expression_unit_round_trips_temporal_precision_and_observable_value_zone() {
    use arrow::array::{Array, FixedSizeBinaryArray};
    use novarocks_type_contract::{
        TemporalCarrier, TemporalMicros, TemporalPrecision, TemporalType, TemporalValue,
        TimeZoneKey, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
    };
    let declaration = TemporalType::TimestampWithTimeZone(TemporalPrecision::PICOSECONDS);
    let observed = TemporalValue::TimestampWithTimeZone {
        instant: TemporalMicros::from_picos(-1).unwrap(),
        zone: TimeZoneKey::parse("Asia/Shanghai").unwrap(),
    };
    let TemporalCarrier::ZonedTimestamp(bytes) = observed.to_carrier(declaration).unwrap() else {
        panic!("unexpected carrier");
    };
    let array = FixedSizeBinaryArray::try_from_iter([bytes.as_slice()].into_iter()).unwrap();
    let storage = ValueTypeStorageLifetime::pure();
    let ty = p::ValueType::try_from_value(
        ValueTypeRef::try_new(ValueKind::Temporal(declaration), false, storage.clone()).unwrap(),
        ValueLayout::try_new(&declaration.carrier(), storage).unwrap(),
    )
    .unwrap();
    let constant = p::ConstantPool::try_new(
        Arc::new(ty.try_to_field("exact").unwrap()),
        ty.clone(),
        array.into_data(),
        FROZEN_CONSTANT_POLICY,
        CompilePhase::Validate,
        &Control,
    )
    .unwrap()
    .value(0)
    .unwrap();
    let package = pair_with_constant(constant, false);
    let encoded =
        encode_fragment_package(&package, &PackageEncodeLimits::FROZEN, &Control).unwrap();
    let raw = encoded.encode_to_vec();
    let model = FragmentDecodeResourceModel::try_new(&Control).unwrap();
    let decoded =
        decode_fragment_package(&raw, &model, &PackageDecodeLimits::FROZEN, &Control).unwrap();
    let mut work = CompileCheckpoints::try_new(&Control, CompilePhase::Validate).unwrap();
    let value = decoded
        .constants()
        .resolve_observed(
            p::ConstantReference {
                pool: p::ConstantPoolId::new(u32::MAX),
                ordinal: 0,
            },
            &ty,
            &mut work,
        )
        .unwrap();
    work.finish().unwrap();
    assert_eq!(value.value_type(), &ty);
    assert_eq!(
        value
            .temporal_observed(CompilePhase::Validate, &Control)
            .unwrap(),
        Some(observed)
    );
    assert_eq!(
        encode_fragment_package(&decoded, &PackageEncodeLimits::FROZEN, &Control)
            .unwrap()
            .encode_to_vec(),
        raw
    );
}
#[test]
fn node_units_keep_local_zero_distinct_ports_source_pool_and_repeated_outputs() {
    let package = pair();
    let encoded =
        encode_fragment_package(&package, &PackageEncodeLimits::FROZEN, &Control).unwrap();
    let nodes = &encoded.fragment.as_ref().unwrap().nodes;
    assert_eq!(nodes.len(), 2);
    for node in nodes {
        let unit = node.expressions.as_ref().unwrap();
        assert_eq!(unit.definitions.len(), 1);
        assert_eq!(unit.definitions[0].id, 0);
        assert_eq!(unit.roots[0].definition_id, Some(0));
    }
    assert!(nodes[0].expressions.as_ref().unwrap().ports.is_empty());
    assert_eq!(nodes[1].expressions.as_ref().unwrap().ports.len(), 1);
    assert_eq!(encoded.constants.len(), 1);
    assert_eq!(encoded.constants[0].id, u32::MAX);
    let raw = encoded.encode_to_vec();
    let model = FragmentDecodeResourceModel::try_new(&Control).unwrap();
    let decoded =
        decode_fragment_package(&raw, &model, &PackageDecodeLimits::FROZEN, &Control).unwrap();
    assert_eq!(decoded.fragment().nodes(), package.fragment().nodes());
    assert_eq!(
        decoded.statement_constant_policy(),
        package.statement_constant_policy()
    );
    assert_eq!(
        decoded
            .fragment()
            .nodes()
            .get(&p::NodeId::new(1))
            .unwrap()
            .output
            .columns
            .len(),
        2
    );
    assert_eq!(
        encode_fragment_package(&decoded, &PackageEncodeLimits::FROZEN, &Control).unwrap(),
        encoded
    );
}
#[test]
fn two_valid_unit_namespaces_share_the_original_fragment_projection_limits() {
    let package = pair();
    let encoded =
        encode_fragment_package(&package, &PackageEncodeLimits::FROZEN, &Control).unwrap();
    let raw = encoded.encode_to_vec();
    let model = FragmentDecodeResourceModel::try_new(&Control).unwrap();
    for axis in 0..2 {
        let mut sender = PackageEncodeLimits::FROZEN;
        let mut receiver = PackageDecodeLimits::FROZEN;
        if axis == 0 {
            sender.expressions.max_definitions = 1;
            receiver.expressions.max_definitions = 1;
        } else {
            // Each actual unit fits two references (Values=1, Project=2),
            // while the complete fragment contains three independent uses.
            for node in package.fragment().nodes().values() {
                assert!(
                    node.expressions
                        .as_ref()
                        .unwrap()
                        .control_flow()
                        .use_reference_count()
                        <= 2
                );
            }
            sender.control.max_use_references = 2;
            receiver.control.max_use_references = 2;
        }
        assert!(matches!(
            encode_fragment_package(&package, &sender, &Control),
            Err(PackageEncodeError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert!(matches!(
            decode_fragment_package(&raw, &model, &receiver, &Control),
            Err(PackageDecodeError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
    }
}

#[test]
fn unit_root_wire_presence_and_fragment_root_header_boundary_remain_explicit() {
    let package = pair();
    let encoded =
        encode_fragment_package(&package, &PackageEncodeLimits::FROZEN, &Control).unwrap();
    let model = FragmentDecodeResourceModel::try_new(&Control).unwrap();
    for field in 0..4 {
        let mut source = encoded.clone();
        let root = &mut source.fragment.as_mut().unwrap().nodes[1]
            .expressions
            .as_mut()
            .unwrap()
            .roots[0];
        let expected = match field {
            0 => {
                root.definition_id = None;
                "unit root definition is absent"
            }
            1 => {
                root.use_id = None;
                "unit root use is absent"
            }
            2 => {
                root.scope = None;
                "unit root scope is absent"
            }
            _ => {
                root.scope.as_mut().unwrap().kind = None;
                "unit root scope is absent"
            }
        };
        let result = decode_fragment_package(
            &source.encode_to_vec(),
            &model,
            &PackageDecodeLimits::FROZEN,
            &Control,
        );
        if field == 0 {
            assert!(
                matches!(result,Err(PackageDecodeError::Expression(novarocks_plan_codec::physical_expression_v2::ExpressionCodecError::InvalidShape(message))) if message == expected)
            );
        } else {
            assert!(
                matches!(result,Err(PackageDecodeError::Invalid(message)) if message==expected)
            );
        }
    }
    for count in [
        novarocks_type_contract::MAX_CONTROL_USE_REFERENCES - 2,
        novarocks_type_contract::MAX_CONTROL_USE_REFERENCES - 1,
        novarocks_type_contract::MAX_CONTROL_USE_REFERENCES,
    ] {
        let mut source = encoded.clone();
        let unit = source.fragment.as_mut().unwrap().nodes[1]
            .expressions
            .as_mut()
            .unwrap();
        let mut malformed = unit.roots[0];
        malformed.definition_id = None;
        unit.roots = vec![malformed; count];
        let result = decode_fragment_package(
            &source.encode_to_vec(),
            &model,
            &PackageDecodeLimits::FROZEN,
            &Control,
        );
        if count < novarocks_type_contract::MAX_CONTROL_USE_REFERENCES {
            assert!(matches!(
                result,
                Err(PackageDecodeError::Expression(novarocks_plan_codec::physical_expression_v2::ExpressionCodecError::InvalidShape("unit root definition is absent")))
            ));
        } else {
            assert!(matches!(
                result,
                Err(PackageDecodeError::Expression(
                    novarocks_plan_codec::physical_expression_v2::ExpressionCodecError::Unit(
                        novarocks_expression_contract::UnitError::Flow(
                            novarocks_type_contract::ExpressionControlFlowError::TooManyItems
                        )
                    )
                ))
            ));
        }
    }
}

#[test]
fn receiver_original_creator_refusals_and_escaping_unit_diagnostic_keep_their_owner() {
    use novarocks_plan_codec::{
        host_projection_v2::ProjectionFailure, physical_expression_v2::ExpressionCodecError,
    };
    use novarocks_type_contract::{
        ValueSchemaCreation, ValueTypeStorageHold, ValueTypeStorageLifetime,
    };
    use std::{alloc::Layout, cell::RefCell};
    #[derive(Clone, Debug, Eq, PartialEq)]
    enum Request {
        Exact(usize, usize),
        Opaque(usize),
    }
    #[derive(Debug, Eq, PartialEq)]
    struct Refusal(usize);
    struct Hold;
    impl ValueTypeStorageHold for Hold {}
    let source = encode_fragment_package(&pair(), &PackageEncodeLimits::FROZEN, &Control).unwrap();
    let model = FragmentDecodeResourceModel::try_new(&Control).unwrap();
    let raw = source.encode_to_vec();
    let run = |raw: &[u8], stop: Option<usize>| {
        let trace = RefCell::new(Vec::new());
        let observe = |request| {
            let mut events = trace.borrow_mut();
            let index = events.len();
            if let Some(stop) = stop {
                assert!(index <= stop, "creator callback after refusal");
            }
            events.push(request);
            if stop == Some(index) {
                Err(Refusal(index))
            } else {
                Ok(())
            }
        };
        let hold = Arc::new(Hold);
        let weak = Arc::downgrade(&hold);
        let mut exact = |layout: Layout| observe(Request::Exact(layout.size(), layout.align()));
        let mut opaque = |bytes| observe(Request::Opaque(bytes));
        let mut creation = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::held(hold),
            &mut exact,
            &mut opaque,
        );
        let result = decode_fragment_package_with_creator(
            raw,
            &model,
            &PackageDecodeLimits::FROZEN,
            &mut creation,
            &Control,
        );
        drop(creation);
        (result, trace.into_inner(), weak)
    };
    let (result, baseline, weak) = run(&raw, None);
    assert!(result.is_ok(), "{result:?}");
    assert!(!baseline.is_empty());
    assert!(
        baseline
            .iter()
            .any(|request| matches!(request, Request::Exact(_, _)))
    );
    assert!(
        baseline
            .iter()
            .any(|request| matches!(request, Request::Opaque(_)))
    );
    assert!(weak.upgrade().is_some());
    let flow = result
        .as_ref()
        .unwrap()
        .fragment()
        .unit(p::NodeId::new(1))
        .unwrap()
        .control_flow()
        .clone();
    drop(result);
    assert!(
        weak.upgrade().is_some(),
        "independent flow alias must keep its own source and backing funded"
    );
    drop(flow);
    assert!(weak.upgrade().is_none());
    for index in 0..baseline.len() {
        let (result, actual, weak) = run(&raw, Some(index));
        assert!(matches!(result, Err(ProjectionFailure::Host(Refusal(actual))) if actual == index));
        assert_eq!(actual, baseline[..=index]);
        assert!(weak.upgrade().is_none());
    }
    let mut bad = source;
    bad.fragment.as_mut().unwrap().nodes[1]
        .expressions
        .as_mut()
        .unwrap()
        .roots[0]
        .scope
        .as_mut()
        .unwrap()
        .kind = Some(novarocks_proto_models::physical_package_v2::root_scope::Kind::Port(u32::MAX));
    let (result, _, weak) = run(&bad.encode_to_vec(), None);
    let Err(ProjectionFailure::Codec(PackageDecodeError::Expression(
        ExpressionCodecError::ConstructedUnit(error),
    ))) = result
    else {
        panic!("malformed unit must reach the actual receiving unit owner");
    };
    assert!(matches!(
        error.cause(),
        novarocks_expression_contract::UnitError::Invalid { root: Some(_), .. }
    ));
    assert!(
        weak.upgrade().is_some(),
        "escaping diagnostic must retain its original storage"
    );
    drop(error);
    assert!(weak.upgrade().is_none());
}

#[test]
fn receiver_factory_flow_keeps_raw_argument_payload_in_the_same_retained_scope() {
    use novarocks_type_contract::{
        ValueSchemaCreation, ValueSchemaCreationFactory, ValueSchemaCreationScope,
        ValueSchemaScopeBeginFailure, ValueTypeStorageHold, ValueTypeStorageLifetime,
        owned_resources::layout::{LayoutResourceError, arc_layout},
    };
    use std::{
        alloc::Layout,
        convert::Infallible,
        sync::{Mutex, Weak},
    };
    struct Hold;
    impl ValueTypeStorageHold for Hold {}
    #[derive(Default)]
    struct Journal {
        scopes: Vec<Weak<Hold>>,
        layouts: Vec<(usize, usize, usize)>,
    }
    struct Factory(Arc<Mutex<Journal>>);
    struct Scope {
        id: usize,
        storage: ValueTypeStorageLifetime,
        journal: Arc<Mutex<Journal>>,
    }
    impl Factory {
        fn open(&self) -> Scope {
            let hold = Arc::new(Hold);
            let mut journal = self.0.lock().unwrap();
            let id = journal.scopes.len();
            journal.scopes.push(Arc::downgrade(&hold));
            Scope {
                id,
                storage: ValueTypeStorageLifetime::held(hold),
                journal: self.0.clone(),
            }
        }
    }
    impl ValueSchemaCreationScope<Infallible> for Scope {
        fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
            self.storage.clone()
        }
        fn request_layout(&mut self, layout: Layout) -> Result<(), Infallible> {
            self.journal
                .lock()
                .unwrap()
                .layouts
                .push((self.id, layout.size(), layout.align()));
            Ok(())
        }
        fn observe_opaque_upper_bound(&mut self, _: usize) -> Result<(), Infallible> {
            Ok(())
        }
    }
    impl ValueSchemaCreationFactory<Infallible> for Factory {
        type Scope<'a> = Scope;
        fn begin_scope(&self) -> Result<Scope, Infallible> {
            Ok(self.open())
        }
        fn visit_scope_allocation_layouts<E>(
            &self,
            visit: &mut dyn FnMut(Layout) -> Result<(), E>,
        ) -> Result<(), E>
        where
            E: From<LayoutResourceError>,
        {
            visit(arc_layout(Layout::new::<Hold>()).map_err(E::from)?)
        }
        fn begin_scope_observed<O>(
            &self,
            observe: &mut dyn FnMut(Layout) -> Result<(), O>,
        ) -> Result<Scope, ValueSchemaScopeBeginFailure<O, Infallible>> {
            observe(arc_layout(Layout::new::<Hold>()).unwrap())
                .map_err(ValueSchemaScopeBeginFailure::Observer)?;
            Ok(self.open())
        }
    }
    let source = encode_fragment_package(
        &pair_with_unary(true),
        &PackageEncodeLimits::FROZEN,
        &Control,
    )
    .unwrap()
    .encode_to_vec();
    let model = FragmentDecodeResourceModel::try_new(&Control).unwrap();
    let journal = Arc::new(Mutex::new(Journal::default()));
    let factory = Factory(journal.clone());
    let mut exact = |_: Layout| Ok::<_, Infallible>(());
    let mut opaque = |_: usize| Ok::<_, Infallible>(());
    let mut creation =
        ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
    let decoded = decode_fragment_package_with_factory(
        &source,
        &model,
        &PackageDecodeLimits::FROZEN,
        &mut creation,
        &factory,
        &Control,
    )
    .unwrap();
    let flow = decoded
        .fragment()
        .unit(p::NodeId::new(1))
        .unwrap()
        .control_flow()
        .clone();
    assert!(flow.uses().values().any(|use_| !use_.arguments.is_empty()));
    drop(decoded);
    drop(creation);
    let ledger = journal.lock().unwrap();
    let live = ledger
        .scopes
        .iter()
        .enumerate()
        .filter_map(|(id, weak)| weak.upgrade().map(|_| id))
        .collect::<Vec<_>>();
    assert_eq!(
        live.len(),
        1,
        "only the cloned flow's original retained scope may remain"
    );
    let argument = Layout::new::<novarocks_type_contract::ExpressionUseId>();
    assert!(
        ledger
            .layouts
            .iter()
            .any(|&(scope, size, align)| scope == live[0]
                && (size, align) == (argument.size(), argument.align())),
        "the independently surviving flow scope must fund its raw argument Box before graph construction"
    );
    drop(ledger);
    drop(flow);
    assert!(
        journal
            .lock()
            .unwrap()
            .scopes
            .iter()
            .all(|weak| weak.upgrade().is_none())
    );
}
