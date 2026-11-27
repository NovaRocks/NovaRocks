// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Complete TableFunction representation through the original expression,
//! value and materialized relation-signature owners. Fragment/Package retains
//! semantic, argument-source and graph closure.

use crate::physical_aggregate_node_v2::{node_facts, node_gate};
pub use crate::physical_node_v2::{
    NodeCodecError as TableFunctionNodeCodecError,
    NodeProjectionFacts as TableFunctionNodeProjectionFacts,
};
use crate::{
    physical_binding_v2::{
        BindingProjectionLimits, MaterializationModel, MaterializedFunctionBinding,
        MaterializedFunctionBindings, copy_table_signature_observed,
        preflight_table_signature_copy_counts, preflight_table_signature_copy_counts_in,
        preflight_table_signature_copy_types, preflight_table_signature_copy_types_in,
    },
    physical_expression_v2::{DecodedExpressions, EncodedExpressions},
    physical_node_v2::*,
    physical_properties_v2 as properties,
    physical_value_v2::EncodedValues,
};
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase};

type Error = TableFunctionNodeCodecError;
#[derive(Clone, Copy, Debug)]
pub struct TableFunctionNodeProjectionLimits<N = NodeProjectionLimits> {
    pub node: N,
    pub binding: Option<BindingProjectionLimits>,
}
impl<N: Into<Option<NodeProjectionLimits>>> TableFunctionNodeProjectionLimits<N> {
    fn capture_node_profile(
        self,
    ) -> TableFunctionNodeProjectionLimits<Option<NodeProjectionLimits>> {
        TableFunctionNodeProjectionLimits {
            node: self.node.into(),
            binding: self.binding,
        }
    }
}
struct Physical<'a> {
    function: &'a p::BoundTableFunction,
    arguments: &'a [novarocks_expression_contract::RootOrdinal],
    outputs: &'a [p::TableFunctionOutput],
    left_outer: bool,
}
fn physical(source: &p::PhysicalNode) -> Result<Physical<'_>, Error> {
    match &source.kind {
        p::NodeKind::TableFunction {
            function,
            arguments,
            outputs,
            left_outer,
        } => Ok(Physical {
            function,
            arguments,
            outputs,
            left_outer: *left_outer,
        }),
        _ => Err(invalid("physical node is not TableFunction")),
    }
}
fn raw(source: &wire::PhysicalNode) -> Result<&wire::TableFunctionNode, Error> {
    match source.kind.as_ref() {
        Some(wire::physical_node::Kind::TableFunction(body)) => Ok(body),
        _ => Err(invalid("wire node is not TableFunction")),
    }
}
fn required(value: Option<u32>, work: &mut CompileCheckpoints<'_>) -> Result<u32, Error> {
    let result = value.ok_or_else(|| invalid("TableFunction required reference is absent"));
    work.step()?;
    result
}
fn output_value(
    output: &wire::TableFunctionOutput,
    work: &mut CompileCheckpoints<'_>,
) -> Result<u32, Error> {
    let result = match output.kind.as_ref() {
        Some(wire::table_function_output::Kind::PassthroughValueId(value)) => Ok(*value),
        Some(wire::table_function_output::Kind::FunctionResult(value)) => {
            required(value.value_id, work)
        }
        None => Err(invalid("TableFunction output kind is absent")),
    };
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    work.step()?;
    result
}
fn properties_encode(
    source: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    source_bytes: usize,
    limits: Option<NodeProjectionLimits>,
    model: &mut Model,
    admit: &mut Option<&mut NodeAdmit<'_>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    for property in source
        .required_inputs
        .iter()
        .chain(std::iter::once(&source.output_properties))
    {
        if admit.is_some() {
            let facts = properties::properties_encode_numerical_facts_in(property, source_bytes)?;
            properties::check_properties_numerical_facts(
                facts,
                limits.map(|limits| limits.properties),
            )?;
            model.property(facts)?;
            node_gate(model, source_bytes, values.count(), limits, admit)?;
            properties::preflight_encode_observed(
                property,
                source_bytes,
                limits.map(|limits| limits.properties),
                work,
            )?;
        } else {
            model.property(properties::preflight_encode_observed(
                property,
                source_bytes,
                limits.map(|limits| limits.properties),
                work,
            )?)?;
        }
        work.step()?;
    }
    node_facts(model, source_bytes, values.count(), limits, admit, work)?;
    for property in source
        .required_inputs
        .iter()
        .chain(std::iter::once(&source.output_properties))
    {
        physical_property_refs(property, values, work)?;
    }
    Ok(())
}
fn table_encode_root_model(source: &p::PhysicalNode, roots: bool) -> Result<(Model, usize), Error> {
    let body = physical(source)?;
    let mut model = Model {
        inputs: source.inputs.len(),
        items: add(
            add(source.required_inputs.len(), source.output.columns.len())?,
            add(body.arguments.len(), body.outputs.len())?,
        )?,
        refs: add(source.output.columns.len(), body.outputs.len())?,
        ..Model::default()
    };
    let backing = add(
        bytes::<novarocks_expression_contract::RootOrdinal>(body.arguments.len())?,
        bytes::<p::TableFunctionOutput>(body.outputs.len())?,
    )?;
    if roots {
        encode_header_requests(source, &mut model)?;
        model.request::<u32>(body.arguments.len(), 1)?;
        model.request::<wire::TableFunctionOutput>(body.outputs.len(), 1)?;
    }
    Ok((model, backing))
}
fn prepare_encode(
    source: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    source_bytes: usize,
    limits: TableFunctionNodeProjectionLimits<Option<NodeProjectionLimits>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(u32, NodeProjectionFacts), Error> {
    prepare_encode_core(
        source,
        values,
        expressions,
        source_bytes,
        limits,
        None,
        work,
    )
}
fn prepare_encode_core<'parent>(
    source: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    source_bytes: usize,
    limits: TableFunctionNodeProjectionLimits<Option<NodeProjectionLimits>>,
    mut admit: Option<&'parent mut NodeAdmit<'parent>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(u32, NodeProjectionFacts), Error> {
    if admit.is_some() && !std::ptr::addr_eq(work.control(), expressions.original_control()) {
        return Err(invalid(
            "TableFunction caller has a different original control",
        ));
    }
    let same = std::ptr::eq(values.types(), expressions.types())
        && if admit.is_some() {
            std::ptr::addr_eq(values.original_control(), expressions.original_control())
        } else {
            std::ptr::eq(values.original_control(), expressions.original_control())
        };
    if admit.is_some() && same {
        values.retained_floor_header_admitted()?;
        expressions.retained_floor_header_in()?;
    }
    let mut early = if admit.is_some() && same && physical(source).is_ok() {
        let (mut model, backing) = table_encode_root_model(source, true)?;
        let body = physical(source)?;
        model.delegated_work = add(
            mul(body.arguments.len(), expressions.lookup_work_upper_bound()?)?,
            add(expressions.functions().source_counts(), 32)?,
        )?;
        node_gate(
            &model,
            source_bytes,
            values.count(),
            limits.node,
            &mut admit,
        )?;
        Some((model, backing))
    } else {
        None
    };
    work.step()?;
    if !same {
        return Err(invalid(
            "TableFunction emission has different original type/control loans",
        ));
    }
    let body = physical(source);
    work.step()?;
    let body = body?;
    let roots_prepared = early.is_some();
    let (mut model, backing) = match early.take() {
        Some(root) => root,
        None => table_encode_root_model(source, false)?,
    };
    node_gate(
        &model,
        source_bytes,
        values.count(),
        limits.node,
        &mut admit,
    )?;
    count_prefix(
        model.inputs,
        model.items,
        source_bytes,
        add(physical_header_floor(source)?, backing)?,
        limits.node,
        work,
    )?;
    model.delegated_work = add(
        mul(body.arguments.len(), expressions.lookup_work_upper_bound()?)?,
        add(expressions.functions().source_counts(), 32)?,
    )?;
    node_facts(
        &model,
        source_bytes,
        values.count(),
        limits.node,
        &mut admit,
        work,
    )?;
    floor(source_bytes, values.retained_floor(work)?, work)?;
    floor(
        source_bytes,
        expressions.retained_floor_observed(work)?,
        work,
    )?;
    if !roots_prepared {
        encode_header_requests(source, &mut model)?;
        model.request::<u32>(body.arguments.len(), 1)?;
        model.request::<wire::TableFunctionOutput>(body.outputs.len(), 1)?;
    }
    node_facts(
        &model,
        source_bytes,
        values.count(),
        limits.node,
        &mut admit,
        work,
    )?;
    properties_encode(
        source,
        values,
        source_bytes,
        limits.node,
        &mut model,
        &mut admit,
        work,
    )?;
    let binding = expressions
        .functions()
        .table_source_id_observed(body.function, work)?;
    work.step()?;
    for argument in body.arguments {
        let found = expressions
            .root_expression_observed(argument.get(), work)?
            .is_some();
        work.step()?;
        if !found {
            return Err(invalid(
                "TableFunction argument is absent from original emission",
            ));
        }
    }
    for output in body.outputs {
        reference(output.value().get(), values, work)?;
    }
    for value in &source.output.columns {
        reference(value.get(), values, work)?;
    }
    let facts = node_facts(
        &model,
        source_bytes,
        values.count(),
        limits.node,
        &mut admit,
        work,
    )?;
    Ok((binding, facts))
}
struct ReadPreparation<'a> {
    function: &'a p::BoundTableFunction,
    facts: NodeProjectionFacts,
}
fn table_decode_root_model(
    source: &wire::PhysicalNode,
    roots: bool,
) -> Result<(Model, usize), Error> {
    let body = raw(source)?;
    let port = source
        .output
        .as_ref()
        .ok_or_else(|| invalid("TableFunction output port is absent"))?;
    let mut model = Model {
        inputs: source.input_node_ids.len(),
        items: add(
            add(source.required_inputs.len(), port.value_ids.len())?,
            add(body.argument_roots.len(), body.outputs.len())?,
        )?,
        refs: add(port.value_ids.len(), body.outputs.len())?,
        ..Model::default()
    };
    let backing = add(
        bytes::<u32>(body.argument_roots.capacity())?,
        bytes::<wire::TableFunctionOutput>(body.outputs.capacity())?,
    )?;
    if roots {
        decode_header_requests(source, port, &mut model)?;
        model
            .request::<novarocks_expression_contract::RootOrdinal>(body.argument_roots.len(), 2)?;
        model.request::<p::TableFunctionOutput>(body.outputs.len(), 2)?;
    }
    Ok((model, backing))
}
fn prepare_decode<'a>(
    source: &wire::PhysicalNode,
    expressions: &DecodedExpressions<'_, '_, '_>,
    functions: &'a MaterializedFunctionBindings<'_, '_>,
    source_bytes: usize,
    limits: TableFunctionNodeProjectionLimits<Option<NodeProjectionLimits>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ReadPreparation<'a>, Error> {
    prepare_decode_core(
        source,
        expressions,
        functions,
        source_bytes,
        limits,
        None,
        work,
    )
}
fn prepare_decode_core<'a, 'parent>(
    source: &wire::PhysicalNode,
    expressions: &DecodedExpressions<'_, '_, '_>,
    functions: &'a MaterializedFunctionBindings<'_, '_>,
    source_bytes: usize,
    limits: TableFunctionNodeProjectionLimits<Option<NodeProjectionLimits>>,
    mut admit: Option<&'parent mut NodeAdmit<'parent>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ReadPreparation<'a>, Error> {
    if admit.is_some() && !std::ptr::addr_eq(work.control(), expressions.original_control()) {
        return Err(invalid(
            "TableFunction caller has a different original control",
        ));
    }
    let same = std::ptr::eq(functions.headers(), expressions.functions())
        && std::ptr::eq(functions.headers().type_table(), expressions.types())
        && if admit.is_some() {
            std::ptr::addr_eq(
                functions.headers().original_control(),
                expressions.original_control(),
            )
        } else {
            std::ptr::eq(
                functions.headers().original_control(),
                expressions.original_control(),
            )
        };
    if admit.is_some() && same {
        expressions.values().retained_floor_header_admitted()?;
        expressions.owned_binding_floor(functions, None)?;
    }
    let mut early = if admit.is_some() && same && raw(source).is_ok() && source.output.is_some() {
        let (mut model, backing) = table_decode_root_model(source, true)?;
        let body = raw(source)?;
        model.delegated_work = add(
            mul(
                body.argument_roots.len(),
                expressions.lookup_work_upper_bound()?,
            )?,
            add(functions.definitions().len(), 32)?,
        )?;
        node_gate(
            &model,
            source_bytes,
            expressions.values().count(),
            limits.node,
            &mut admit,
        )?;
        Some((model, backing))
    } else {
        None
    };
    work.step()?;
    if !same {
        return Err(invalid(
            "TableFunction receiving has different original binding/type/control loans",
        ));
    }
    let body = raw(source);
    work.step()?;
    let body = body?;
    let port = source
        .output
        .as_ref()
        .ok_or_else(|| invalid("TableFunction output port is absent"));
    work.step()?;
    let port = port?;
    required(port.node_id, work)?;
    let output_properties = source
        .output_properties
        .as_ref()
        .ok_or_else(|| invalid("TableFunction output properties are absent"));
    work.step()?;
    let output_properties = output_properties?;
    let roots_prepared = early.is_some();
    let (mut model, backing) = match early.take() {
        Some(root) => root,
        None => table_decode_root_model(source, false)?,
    };
    node_gate(
        &model,
        source_bytes,
        expressions.values().count(),
        limits.node,
        &mut admit,
    )?;
    count_prefix(
        model.inputs,
        model.items,
        source_bytes,
        add(wire_header_floor(source, port)?, backing)?,
        limits.node,
        work,
    )?;
    model.delegated_work = add(
        mul(
            body.argument_roots.len(),
            expressions.lookup_work_upper_bound()?,
        )?,
        add(functions.definitions().len(), 32)?,
    )?;
    node_facts(
        &model,
        source_bytes,
        expressions.values().count(),
        limits.node,
        &mut admit,
        work,
    )?;
    // The original namespace invoice is counted once. Only the newly owned
    // materialized signatures add a separate retained floor.
    expressions.retained_floor_observed(work)?;
    let known = expressions.owned_binding_floor(functions, None)?;
    floor(source_bytes, known, work)?;
    floor(
        source_bytes,
        expressions.values().retained_floor(work)?,
        work,
    )?;
    if !roots_prepared {
        decode_header_requests(source, port, &mut model)?;
        model
            .request::<novarocks_expression_contract::RootOrdinal>(body.argument_roots.len(), 2)?;
        model.request::<p::TableFunctionOutput>(body.outputs.len(), 2)?;
    }
    node_facts(
        &model,
        source_bytes,
        expressions.values().count(),
        limits.node,
        &mut admit,
        work,
    )?;
    for property in source
        .required_inputs
        .iter()
        .chain(std::iter::once(output_properties))
    {
        if admit.is_some() {
            let facts = properties::properties_decode_numerical_facts_in(property, source_bytes)?;
            properties::check_properties_numerical_facts(
                facts,
                limits.node.map(|limits| limits.properties),
            )?;
            model.property(facts)?;
            node_gate(
                &model,
                source_bytes,
                expressions.values().count(),
                limits.node,
                &mut admit,
            )?;
            properties::preflight_decode_observed(
                property,
                source_bytes,
                limits.node.map(|limits| limits.properties),
                work,
            )?;
        } else {
            model.property(properties::preflight_decode_observed(
                property,
                source_bytes,
                limits.node.map(|limits| limits.properties),
                work,
            )?)?;
        }
        work.step()?;
    }
    node_facts(
        &model,
        source_bytes,
        expressions.values().count(),
        limits.node,
        &mut admit,
        work,
    )?;
    let id = required(body.function_binding_id, work)?;
    let mut child = if let Some(parent) = admit.take() {
        let mut child = MaterializationModel::for_composition(
            1,
            add(expressions.types().value_types().len(), 1)?,
            source_bytes,
            known,
        );
        child.compose_in_node_in(
            model,
            expressions.values().count(),
            limits.node,
            limits.binding,
            parent,
        )?;
        Some(child)
    } else {
        None
    };
    let found = if let Some(child) = child.as_mut() {
        functions.definition_captured(
            id,
            &mut |definition, work| {
                if let MaterializedFunctionBinding::Table(function) = definition {
                    preflight_table_signature_copy_counts_in(
                        function,
                        child,
                        limits.binding,
                        &mut |_| Ok(()),
                        work,
                    )?;
                    child.node_facts(0, work)?;
                    preflight_table_signature_copy_types_in(
                        function,
                        child,
                        limits.binding,
                        &mut |_| Ok(()),
                        work,
                    )?;
                }
                Ok(())
            },
            work,
        )?
    } else {
        functions.definition_observed(id, work)?
    };
    work.step()?;
    let function = match found {
        Some(MaterializedFunctionBinding::Table(function)) => function,
        _ => {
            return Err(invalid(
                "TableFunction relation signature reference is unknown or scalar",
            ));
        }
    };
    for argument in &body.argument_roots {
        let found = expressions
            .root_definition_observed(*argument, work)?
            .is_some();
        work.step()?;
        if !found {
            return Err(invalid(
                "TableFunction argument is absent from original receiving namespace",
            ));
        }
    }
    for output in &body.outputs {
        reference(output_value(output, work)?, expressions.values(), work)?;
    }
    for value in &port.value_ids {
        reference(*value, expressions.values(), work)?;
    }
    for property in source
        .required_inputs
        .iter()
        .chain(std::iter::once(output_properties))
    {
        wire_property_refs(property, expressions.values(), work)?;
    }
    let facts = if let Some(child) = child.as_mut() {
        child.node_facts(mul(child.facts.cumulative_work_upper_bound, 2)?, work)?
    } else {
        let base_items = model.items;
        // O(1) outer signature count admits the count-only pass itself. Lambda
        // parameter lengths are then charged by the sole shared signature author.
        model.items = add(
            base_items,
            add(function.argument_types.len(), function.result_types.len())?,
        )?;
        node_facts(
            &model,
            source_bytes,
            expressions.values().count(),
            limits.node,
            &mut admit,
            work,
        )?;
        let mut child = MaterializationModel::for_composition(
            1,
            add(expressions.types().value_types().len(), 1)?,
            source_bytes,
            known,
        );
        // The shared author counts all identity/list/type-reference requests first;
        // the whole node must admit that complete bound before the sole type walk.
        preflight_table_signature_copy_counts(function, &mut child, limits.binding, work)?;
        model.items = add(base_items, child.items)?;
        let base_requests = model.requests;
        let base_requested = model.requested;
        let base_work = model.delegated_work;
        model.requests = add(base_requests, child.facts.allocation_requests_upper_bound)?;
        model.requested = add(base_requested, child.facts.request_bytes_upper_bound)?;
        let early_work = mul(child.facts.cumulative_work_upper_bound, 2)?;
        model.delegated_work = add(base_work, early_work)?;
        node_facts(
            &model,
            source_bytes,
            expressions.values().count(),
            limits.node,
            &mut admit,
            work,
        )?;
        preflight_table_signature_copy_types(function, &mut child, limits.binding, work)?;
        model.requests = add(base_requests, child.facts.allocation_requests_upper_bound)?;
        model.requested = add(base_requested, child.facts.request_bytes_upper_bound)?;
        model.delegated_work = add(
            base_work,
            early_work.max(mul(child.facts.cumulative_work_upper_bound, 2)?),
        )?;
        node_facts(
            &model,
            source_bytes,
            expressions.values().count(),
            limits.node,
            &mut admit,
            work,
        )?
    };
    Ok(ReadPreparation { function, facts })
}
fn emit_encode(
    source: &p::PhysicalNode,
    binding: u32,
    source_bytes: usize,
    limits: Option<NodeProjectionLimits>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::PhysicalNode, Error> {
    let body = physical(source)?;
    let (inputs, required_inputs, output_properties, output) =
        encode_header(source, source_bytes, limits, work)?;
    let mut arguments = reserve(body.arguments.len(), work)?;
    for argument in body.arguments {
        arguments.push(argument.get());
        work.step()?;
    }
    let mut outputs = reserve(body.outputs.len(), work)?;
    for value in body.outputs {
        let kind = match value {
            p::TableFunctionOutput::PassThrough(value) => {
                wire::table_function_output::Kind::PassthroughValueId(value.get())
            }
            p::TableFunctionOutput::FunctionResult {
                result_ordinal,
                value,
            } => wire::table_function_output::Kind::FunctionResult(wire::TableFunctionResult {
                result_ordinal: *result_ordinal,
                value_id: Some(value.get()),
            }),
        };
        outputs.push(wire::TableFunctionOutput { kind: Some(kind) });
        work.step()?;
    }
    let node = wire::PhysicalNode {
        expressions: None,
        id: source.id.get(),
        input_node_ids: inputs,
        required_inputs,
        output_properties: Some(output_properties),
        output: Some(output),
        kind: Some(wire::physical_node::Kind::TableFunction(
            wire::TableFunctionNode {
                function_binding_id: Some(binding),
                argument_roots: arguments,
                outputs,
                left_outer: body.left_outer,
            },
        )),
    };
    work.step()?;
    Ok(node)
}
fn emit_decode(
    source: &wire::PhysicalNode,
    function: &p::BoundTableFunction,
    source_bytes: usize,
    limits: Option<NodeProjectionLimits>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<p::PhysicalNode, Error> {
    let body = raw(source)?;
    let function = copy_table_signature_observed(function, work)?;
    let (inputs, required_inputs, output_properties, output) =
        decode_header(source, source_bytes, limits, work)?;
    let mut arguments = reserve(body.argument_roots.len(), work)?;
    for argument in &body.argument_roots {
        arguments.push(novarocks_expression_contract::RootOrdinal::new(*argument));
        work.step()?;
    }
    let mut outputs = reserve(body.outputs.len(), work)?;
    for value in &body.outputs {
        let output = match value.kind.as_ref() {
            Some(wire::table_function_output::Kind::PassthroughValueId(value)) => {
                p::TableFunctionOutput::PassThrough(p::ValueId::new(*value))
            }
            Some(wire::table_function_output::Kind::FunctionResult(value)) => {
                p::TableFunctionOutput::FunctionResult {
                    result_ordinal: value.result_ordinal,
                    value: p::ValueId::new(required(value.value_id, work)?),
                }
            }
            None => return Err(invalid("prepared TableFunction output kind is absent")),
        };
        outputs.push(output);
        work.step()?;
    }
    let node = p::PhysicalNode {
        expressions: None,
        id: p::NodeId::new(source.id),
        inputs,
        required_inputs,
        output_properties,
        output,
        kind: p::NodeKind::TableFunction {
            function,
            arguments: boxed(arguments, work)?,
            outputs: boxed(outputs, work)?,
            left_outer: body.left_outer,
        },
    };
    work.step()?;
    Ok(node)
}
pub struct PreparedTableFunctionNodeEncode<'node, 'namespace, 'loan, 'source, 'control> {
    source: &'node p::PhysicalNode,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    binding: u32,
    source_bytes: usize,
    limits: TableFunctionNodeProjectionLimits<Option<NodeProjectionLimits>>,
    facts: NodeProjectionFacts,
}
impl PreparedTableFunctionNodeEncode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &NodeProjectionFacts {
        &self.facts
    }
    pub(crate) fn emit_in(
        self,
        admit: &mut NodeAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(wire::PhysicalNode, NodeProjectionFacts), Error> {
        if !std::ptr::addr_eq(work.control(), self.expressions.original_control()) {
            return Err(invalid(
                "TableFunction emission has a different original control",
            ));
        }
        admit(&self.facts)?;
        let node = emit_encode(
            self.source,
            self.binding,
            self.source_bytes,
            self.limits.node,
            work,
        )?;
        Ok((node, self.facts))
    }
    pub fn emit(self) -> Result<(wire::PhysicalNode, NodeProjectionFacts), Error> {
        let mut work =
            CompileCheckpoints::try_new(self.expressions.original_control(), CompilePhase::Encode)?;
        debug_assert!(std::ptr::eq(self.values.types(), self.expressions.types()));
        let result = emit_encode(
            self.source,
            self.binding,
            self.source_bytes,
            self.limits.node,
            &mut work,
        )
        .map(|node| (node, self.facts));
        finish(work, result)
    }
}
pub fn prepare_table_function_node_encode<'node, 'namespace, 'loan, 'source, 'control>(
    source: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source_retained_bytes: usize,
    limits: TableFunctionNodeProjectionLimits,
) -> Result<PreparedTableFunctionNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, Error> {
    let limits = limits.capture_node_profile();
    let mut work =
        CompileCheckpoints::try_new(expressions.original_control(), CompilePhase::Encode)?;
    let result = prepare_encode(
        source,
        values,
        expressions,
        source_retained_bytes,
        limits,
        &mut work,
    );
    let (binding, facts) = finish(work, result)?;
    Ok(PreparedTableFunctionNodeEncode {
        source,
        expressions,
        values,
        binding,
        source_bytes: source_retained_bytes,
        limits,
        facts,
    })
}
/// Borrow the original caller scope and cumulative parent admission.
/// This port owns no entry/footer, allocation grant, or Fragment/Package proof.
pub(crate) fn prepare_table_function_node_encode_in<
    'node,
    'namespace,
    'loan,
    'source,
    'control,
    'parent,
>(
    source: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source_retained_bytes: usize,
    limits: TableFunctionNodeProjectionLimits<impl Into<Option<NodeProjectionLimits>>>,
    admit: &'parent mut NodeAdmit<'parent>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedTableFunctionNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, Error> {
    let limits = limits.capture_node_profile();
    let (binding, facts) = prepare_encode_core(
        source,
        values,
        expressions,
        source_retained_bytes,
        limits,
        Some(admit),
        work,
    )?;
    Ok(PreparedTableFunctionNodeEncode {
        source,
        expressions,
        values,
        binding,
        source_bytes: source_retained_bytes,
        limits,
        facts,
    })
}
pub struct PreparedTableFunctionNodeDecode<'node, 'namespace, 'loan, 'wire, 'control> {
    source: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    functions: &'namespace MaterializedFunctionBindings<'loan, 'wire>,
    function: &'namespace p::BoundTableFunction,
    source_bytes: usize,
    limits: TableFunctionNodeProjectionLimits<Option<NodeProjectionLimits>>,
    facts: NodeProjectionFacts,
}
impl PreparedTableFunctionNodeDecode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &NodeProjectionFacts {
        &self.facts
    }
    pub(crate) fn emit_in(
        self,
        admit: &mut NodeAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(p::PhysicalNode, NodeProjectionFacts), Error> {
        if !std::ptr::addr_eq(work.control(), self.expressions.original_control()) {
            return Err(invalid(
                "TableFunction emission has a different original control",
            ));
        }
        admit(&self.facts)?;
        let node = emit_decode(
            self.source,
            self.function,
            self.source_bytes,
            self.limits.node,
            work,
        )?;
        Ok((node, self.facts))
    }
    pub fn emit(self) -> Result<(p::PhysicalNode, NodeProjectionFacts), Error> {
        let mut work =
            CompileCheckpoints::try_new(self.expressions.original_control(), CompilePhase::Decode)?;
        // Retain the materialized owner as well as its selected binding loan.
        debug_assert!(std::ptr::eq(
            self.functions.headers(),
            self.expressions.functions()
        ));
        let result = emit_decode(
            self.source,
            self.function,
            self.source_bytes,
            self.limits.node,
            &mut work,
        )
        .map(|node| (node, self.facts));
        finish(work, result)
    }
}
pub fn prepare_table_function_node_decode<'node, 'namespace, 'loan, 'wire, 'control>(
    source: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    functions: &'namespace MaterializedFunctionBindings<'loan, 'wire>,
    source_retained_bytes: usize,
    limits: TableFunctionNodeProjectionLimits,
) -> Result<PreparedTableFunctionNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
    let limits = limits.capture_node_profile();
    let mut work =
        CompileCheckpoints::try_new(expressions.original_control(), CompilePhase::Decode)?;
    let result = prepare_decode(
        source,
        expressions,
        functions,
        source_retained_bytes,
        limits,
        &mut work,
    );
    let prepared = finish(work, result)?;
    Ok(PreparedTableFunctionNodeDecode {
        source,
        expressions,
        functions,
        function: prepared.function,
        source_bytes: source_retained_bytes,
        limits,
        facts: prepared.facts,
    })
}
/// Borrow the original caller scope and cumulative parent admission.
/// This port owns no entry/footer, allocation grant, or Fragment/Package proof.
pub(crate) fn prepare_table_function_node_decode_in<
    'node,
    'namespace,
    'loan,
    'wire,
    'control,
    'parent,
>(
    source: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    functions: &'namespace MaterializedFunctionBindings<'loan, 'wire>,
    source_retained_bytes: usize,
    limits: TableFunctionNodeProjectionLimits<impl Into<Option<NodeProjectionLimits>>>,
    admit: &'parent mut NodeAdmit<'parent>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedTableFunctionNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
    let limits = limits.capture_node_profile();
    let prepared = prepare_decode_core(
        source,
        expressions,
        functions,
        source_retained_bytes,
        limits,
        Some(admit),
        work,
    )?;
    Ok(PreparedTableFunctionNodeDecode {
        source,
        expressions,
        functions,
        function: prepared.function,
        source_bytes: source_retained_bytes,
        limits,
        facts: prepared.facts,
    })
}
#[cfg(test)]
#[path = "physical_table_function_node_v2/tests.rs"]
mod tests;
