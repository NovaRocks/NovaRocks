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

//! Complete ordinary Unpivot representation with original namespace loans.
//! Fragment and Constant owners retain type, output-role and collection policy
//! proofs. This projection neither authenticates a package nor grants memory.

pub use crate::physical_node_v2::{
    NodeCodecError as UnpivotNodeCodecError, NodeProjectionFacts as UnpivotNodeProjectionFacts,
    NodeProjectionLimits as UnpivotNodeProjectionLimits,
};
use crate::{
    physical_expression_v2::{DecodedExpressions, EncodedExpressions, expression_tree_lookup_work},
    physical_node_v2::*,
    physical_properties_v2 as properties,
    physical_value_v2::EncodedValues,
};
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};
type Error = UnpivotNodeCodecError;

pub(crate) trait Expressions {
    fn control(&self) -> &dyn PureCompileControl;
    fn floor(&self, w: &mut CompileCheckpoints<'_>) -> Result<usize, Error>;
    fn lookup_work(&self) -> Result<usize, Error>;
    fn pools(&self) -> &p::ConstantPools;
    fn scalar(&self, id: u32, w: &mut CompileCheckpoints<'_>) -> Result<(), Error>;
}
impl Expressions for EncodedExpressions<'_, '_, '_> {
    fn control(&self) -> &dyn PureCompileControl {
        self.original_control()
    }
    fn floor(&self, w: &mut CompileCheckpoints<'_>) -> Result<usize, Error> {
        Ok(self.retained_floor_observed(w)?)
    }
    fn lookup_work(&self) -> Result<usize, Error> {
        Ok(self.lookup_work_upper_bound()?)
    }
    fn pools(&self) -> &p::ConstantPools {
        self.pools()
    }
    fn scalar(&self, id: u32, w: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
        let constant = self.root_expression_observed(id, w)?.is_some_and(|expr| {
            matches!(
                expr.kind,
                novarocks_expression_contract::ExprKind::Constant(_)
            )
        });
        w.step()?;
        if constant {
            Ok(())
        } else {
            Err(invalid(
                "Unpivot scalar is not an original Constant expression",
            ))
        }
    }
}
impl Expressions for DecodedExpressions<'_, '_, '_> {
    fn control(&self) -> &dyn PureCompileControl {
        self.original_control()
    }
    fn floor(&self, w: &mut CompileCheckpoints<'_>) -> Result<usize, Error> {
        Ok(self.retained_floor_observed(w)?)
    }
    fn lookup_work(&self) -> Result<usize, Error> {
        Ok(self.lookup_work_upper_bound()?)
    }
    fn pools(&self) -> &p::ConstantPools {
        self.pools()
    }
    fn scalar(&self, id: u32, w: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
        let constant = self.root_definition_observed(id, w)?.is_some_and(|expr| {
            matches!(
                expr.kind,
                Some(wire::expression_definition::Kind::Constant(_))
            )
        });
        w.step()?;
        if constant {
            Ok(())
        } else {
            Err(invalid(
                "Unpivot scalar is not an original received Literal expression",
            ))
        }
    }
}
pub(crate) fn address(
    reference: p::ConstantReference,
    expressions: &impl Expressions,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    // Address only, with original pool/type/Field/selected ordinal preserved.
    // No bucketed full-type comparator or collection payload scan is needed.
    w.flush()?;
    let result = expressions.pools().resolve_source_observed(reference, w);
    let selected = result?;
    w.flush()?;
    drop(selected);
    w.step()?;
    Ok(())
}
fn decode_address(input: &wire::ConstantReference) -> Result<p::ConstantReference, Error> {
    Ok(p::ConstantReference {
        pool: p::ConstantPoolId::new(
            input
                .pool_id
                .ok_or_else(|| invalid("Unpivot constant pool ID is absent"))?,
        ),
        ordinal: input.row_ordinal,
    })
}
fn encode_address(input: p::ConstantReference) -> wire::ConstantReference {
    wire::ConstantReference {
        pool_id: Some(input.pool.get()),
        row_ordinal: input.ordinal,
    }
}
pub(crate) fn delegate_counts(
    model: &mut Model,
    scalars: usize,
    pools: usize,
    expressions: &impl Expressions,
) -> Result<(), Error> {
    model.delegated_work = add(
        model.delegated_work,
        mul(scalars, expressions.lookup_work()?)?,
    )?;
    model.delegated_work = add(
        model.delegated_work,
        mul(
            pools,
            expression_tree_lookup_work(expressions.pools().entries().len())?,
        )?,
    )?;
    Ok(())
}
/// Both Unpivot forms borrow this same constant-address grammar. These
/// projections allocate no backing and leave each occurrence's work step to
/// the original enclosing loop.
pub(crate) fn encode_constant(input: &p::UnpivotConstant) -> wire::UnpivotConstant {
    let kind = match input {
        p::UnpivotConstant::Scalar(id) => wire::unpivot_constant::Kind::ScalarRoot(id.get()),
        p::UnpivotConstant::Int32List(reference) => {
            wire::unpivot_constant::Kind::Int32List(encode_address(*reference))
        }
        p::UnpivotConstant::Utf8Map(reference) => {
            wire::unpivot_constant::Kind::Utf8Map(encode_address(*reference))
        }
    };
    wire::UnpivotConstant { kind: Some(kind) }
}
pub(crate) fn decode_constant(input: &wire::UnpivotConstant) -> Result<p::UnpivotConstant, Error> {
    Ok(
        match input
            .kind
            .as_ref()
            .ok_or_else(|| invalid("prepared Unpivot constant kind is absent"))?
        {
            wire::unpivot_constant::Kind::ScalarRoot(id) => {
                p::UnpivotConstant::Scalar(novarocks_expression_contract::RootOrdinal::new(*id))
            }
            wire::unpivot_constant::Kind::Int32List(reference) => {
                p::UnpivotConstant::Int32List(decode_address(reference)?)
            }
            wire::unpivot_constant::Kind::Utf8Map(reference) => {
                p::UnpivotConstant::Utf8Map(decode_address(reference)?)
            }
        },
    )
}
fn encode_spec(input: &p::PhysicalNode) -> Result<&p::UnpivotSpec, Error> {
    match &input.kind {
        p::NodeKind::Unpivot { spec } => Ok(spec),
        _ => Err(invalid("physical node is not ordinary Unpivot")),
    }
}
fn decode_spec(input: &wire::PhysicalNode) -> Result<&wire::UnpivotNode, Error> {
    match input.kind.as_ref() {
        Some(wire::physical_node::Kind::Unpivot(spec)) => Ok(spec),
        _ => Err(invalid("wire node is not ordinary Unpivot")),
    }
}
fn prepare_encode(
    input: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    source: usize,
    l: UnpivotNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
    mut parent: Option<&mut NodeAdmit<'_>>,
) -> Result<UnpivotNodeProjectionFacts, Error> {
    let namespace_floor = if parent.is_some() {
        values
            .retained_floor_header()?
            .max(expressions.retained_floor_header_in()?)
    } else {
        0
    };
    // Synchronous snapshots borrow the sole containing-node numerical author.
    // They replace this contribution; they do not create a meter or another B.
    let gate = |model: &Model,
                known: usize,
                parent: &mut Option<&mut NodeAdmit<'_>>|
     -> Result<(), Error> {
        if let Some(admit) = parent.as_deref_mut() {
            model.admit_in(source, values.count(), l, admit)?;
            if source < known.max(namespace_floor) {
                return Err(invalid("Repeat source invoice omits original backing"));
            }
        }
        Ok(())
    };

    // The retained Values token is a distinct mandatory loan. An equivalent
    // type table or a different control is not this original correspondence.
    let same = std::ptr::eq(values.types(), expressions.types())
        && if parent.is_some() {
            std::ptr::addr_eq(values.original_control(), expressions.control())
        } else {
            std::ptr::eq(values.original_control(), expressions.control())
        };
    if parent.is_none() {
        w.step()?;
    }
    if !same {
        return Err(invalid(
            "Unpivot namespaces do not retain the same type table and original control",
        ));
    }
    let spec = encode_spec(input)?;
    let outer = add(
        add(input.required_inputs.len(), input.output.columns.len())?,
        add(
            spec.passthrough.len(),
            add(spec.literal_outputs.len(), spec.mappings.len())?,
        )?,
    )?;
    let mut known = add(
        physical_header_floor(input)?,
        add(
            bytes::<(p::ValueId, p::ValueId)>(spec.passthrough.len())?,
            add(
                bytes::<p::ValueId>(spec.literal_outputs.len())?,
                bytes::<p::UnpivotValueMapping>(spec.mappings.len())?,
            )?,
        )?,
    )?;
    if parent.is_none() {
        count_prefix(input.inputs.len(), outer, source, known, l, w)?;
        floor(source, values.retained_floor(w)?, w)?;
        floor(source, expressions.floor(w)?, w)?;
    }
    let mut model = Model {
        inputs: input.inputs.len(),
        items: outer,
        refs: add(
            input.output.columns.len(),
            add(
                mul(spec.passthrough.len(), 2)?,
                add(1, add(spec.literal_outputs.len(), spec.mappings.len())?)?,
            )?,
        )?,
        ..Model::default()
    };
    encode_header_requests(input, &mut model)?;
    model.request::<wire::ValueMapping>(spec.passthrough.len(), 1)?;
    model.request::<u32>(spec.literal_outputs.len(), 1)?;
    model.request::<wire::UnpivotMapping>(spec.mappings.len(), 1)?;
    let mut scalars = 0;
    let mut pools = 0;
    for mapping in &spec.mappings {
        model.items = add(model.items, mapping.constants.len())?;
        known = add(known, bytes::<p::UnpivotConstant>(mapping.constants.len())?)?;
        model.request::<wire::UnpivotConstant>(mapping.constants.len(), 1)?;
        gate(&model, known, &mut parent)?;
        count_prefix(model.inputs, model.items, source, known, l, w)?;
        for constant in &mapping.constants {
            match constant {
                p::UnpivotConstant::Scalar(_) => scalars = add(scalars, 1)?,
                _ => pools = add(pools, 1)?,
            }
            if parent.is_some() {
                model.delegated_work = 0;
                delegate_counts(&mut model, scalars, pools, expressions)?;
            }
            gate(&model, known, &mut parent)?;
            w.step()?;
        }
        gate(&model, known, &mut parent)?;
        w.step()?;
    }
    if parent.is_some() {
        gate(&model, known, &mut parent)?;
    }
    floor(source, known, w)?;
    model.delegated_work = 0;
    delegate_counts(&mut model, scalars, pools, expressions)?;
    if parent.is_some() {
        gate(&model, known, &mut parent)?;
        count_prefix(input.inputs.len(), outer, source, known, l, w)?;
        floor(source, values.retained_floor(w)?, w)?;
        floor(source, expressions.floor(w)?, w)?;
    }
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(&input.output_properties))
    {
        if parent.is_some() {
            let pf = properties::properties_encode_numerical_facts_in(property, source)?;
            properties::check_properties_numerical_facts(pf, l.properties)?;
            model.property(pf)?;
            gate(&model, known, &mut parent)?;
            properties::preflight_encode_observed(property, source, l.properties, w)?;
        } else {
            model.property(properties::preflight_encode_observed(
                property,
                source,
                l.properties,
                w,
            )?)?;
        }
        gate(&model, known, &mut parent)?;
        w.step()?;
    }
    let facts = if let Some(admit) = parent {
        model.facts_in(source, values.count(), l, admit, w)?
    } else {
        model.facts(source, values.count(), l, w)?
    };
    for id in &input.output.columns {
        reference(id.get(), values, w)?;
    }
    for (from, to) in &spec.passthrough {
        reference(from.get(), values, w)?;
        reference(to.get(), values, w)?;
    }
    reference(spec.value_output.get(), values, w)?;
    for id in &spec.literal_outputs {
        reference(id.get(), values, w)?;
    }
    for mapping in &spec.mappings {
        reference(mapping.input.get(), values, w)?;
        for constant in &mapping.constants {
            match constant {
                p::UnpivotConstant::Scalar(id) => expressions.scalar(id.get(), w)?,
                p::UnpivotConstant::Int32List(reference)
                | p::UnpivotConstant::Utf8Map(reference) => address(*reference, expressions, w)?,
            }
        }
    }
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(&input.output_properties))
    {
        physical_property_refs(property, values, w)?;
    }
    Ok(facts)
}
fn prepare_decode(
    input: &wire::PhysicalNode,
    expressions: &DecodedExpressions<'_, '_, '_>,
    source: usize,
    l: UnpivotNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
    mut parent: Option<&mut NodeAdmit<'_>>,
) -> Result<UnpivotNodeProjectionFacts, Error> {
    let values = expressions.values();
    let namespace_floor = if parent.is_some() {
        values
            .retained_floor_header()?
            .max(expressions.retained_floor_header_in()?)
    } else {
        0
    };
    // Synchronous snapshots borrow the sole containing-node numerical author.
    // They replace this contribution; they do not create a meter or another B.
    let gate = |model: &Model,
                known: usize,
                parent: &mut Option<&mut NodeAdmit<'_>>|
     -> Result<(), Error> {
        if let Some(admit) = parent.as_deref_mut() {
            model.admit_in(source, values.count(), l, admit)?;
            if source < known.max(namespace_floor) {
                return Err(invalid("Repeat source invoice omits original backing"));
            }
        }
        Ok(())
    };

    let spec = decode_spec(input)?;
    let port = input
        .output
        .as_ref()
        .ok_or_else(|| invalid("Unpivot output port is absent"))?;
    port.node_id
        .ok_or_else(|| invalid("Unpivot output node ID is absent"))?;
    let output_properties = input
        .output_properties
        .as_ref()
        .ok_or_else(|| invalid("Unpivot output properties are absent"))?;
    let outer = add(
        add(input.required_inputs.len(), port.value_ids.len())?,
        add(
            spec.passthrough.len(),
            add(spec.literal_output_ids.len(), spec.mappings.len())?,
        )?,
    )?;
    let mut known = add(
        wire_header_floor(input, port)?,
        add(
            bytes::<wire::ValueMapping>(spec.passthrough.capacity())?,
            add(
                bytes::<u32>(spec.literal_output_ids.capacity())?,
                bytes::<wire::UnpivotMapping>(spec.mappings.capacity())?,
            )?,
        )?,
    )?;
    if parent.is_none() {
        count_prefix(input.input_node_ids.len(), outer, source, known, l, w)?;
        floor(source, values.retained_floor(w)?, w)?;
        floor(source, expressions.floor(w)?, w)?;
    }
    let mut model = Model {
        inputs: input.input_node_ids.len(),
        items: outer,
        refs: add(
            port.value_ids.len(),
            add(
                mul(spec.passthrough.len(), 2)?,
                add(1, add(spec.literal_output_ids.len(), spec.mappings.len())?)?,
            )?,
        )?,
        ..Model::default()
    };
    decode_header_requests(input, port, &mut model)?;
    model.request::<(p::ValueId, p::ValueId)>(spec.passthrough.len(), 2)?;
    model.request::<p::ValueId>(spec.literal_output_ids.len(), 2)?;
    model.request::<p::UnpivotValueMapping>(spec.mappings.len(), 2)?;
    let mut scalars = 0;
    let mut pools = 0;
    for mapping in &spec.mappings {
        model.items = add(model.items, mapping.constants.len())?;
        known = add(
            known,
            bytes::<wire::UnpivotConstant>(mapping.constants.capacity())?,
        )?;
        model.request::<p::UnpivotConstant>(mapping.constants.len(), 2)?;
        gate(&model, known, &mut parent)?;
        count_prefix(model.inputs, model.items, source, known, l, w)?;
        for constant in &mapping.constants {
            let kind = constant
                .kind
                .as_ref()
                .ok_or_else(|| invalid("Unpivot constant kind is absent"));
            if parent.is_some() {
                match &kind {
                    Ok(wire::unpivot_constant::Kind::ScalarRoot(_)) => {
                        delegate_counts(&mut model, 1, 0, expressions)?;
                    }
                    Ok(_) => {
                        delegate_counts(&mut model, 0, 1, expressions)?;
                    }
                    Err(_) => {}
                }
            }
            gate(&model, known, &mut parent)?;
            w.step()?;
            match kind? {
                wire::unpivot_constant::Kind::ScalarRoot(_) => scalars = add(scalars, 1)?,
                _ => pools = add(pools, 1)?,
            }
        }
        gate(&model, known, &mut parent)?;
        w.step()?;
    }
    if parent.is_some() {
        gate(&model, known, &mut parent)?;
    }
    floor(source, known, w)?;
    model.delegated_work = 0;
    delegate_counts(&mut model, scalars, pools, expressions)?;
    if parent.is_some() {
        gate(&model, known, &mut parent)?;
        count_prefix(input.input_node_ids.len(), outer, source, known, l, w)?;
        floor(source, values.retained_floor(w)?, w)?;
        floor(source, expressions.floor(w)?, w)?;
    }
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(output_properties))
    {
        if parent.is_some() {
            let pf = properties::properties_decode_numerical_facts_in(property, source)?;
            properties::check_properties_numerical_facts(pf, l.properties)?;
            model.property(pf)?;
            gate(&model, known, &mut parent)?;
            properties::preflight_decode_observed(property, source, l.properties, w)?;
        } else {
            model.property(properties::preflight_decode_observed(
                property,
                source,
                l.properties,
                w,
            )?)?;
        }
        gate(&model, known, &mut parent)?;
        w.step()?;
    }
    let facts = if let Some(admit) = parent {
        model.facts_in(source, values.count(), l, admit, w)?
    } else {
        model.facts(source, values.count(), l, w)?
    };
    for id in &port.value_ids {
        reference(*id, values, w)?;
    }
    for pair in &spec.passthrough {
        reference(
            pair.source_value_id
                .ok_or_else(|| invalid("Unpivot passthrough source is absent"))?,
            values,
            w,
        )?;
        reference(
            pair.destination_value_id
                .ok_or_else(|| invalid("Unpivot passthrough destination is absent"))?,
            values,
            w,
        )?;
    }
    reference(
        spec.value_output_id
            .ok_or_else(|| invalid("Unpivot value output is absent"))?,
        values,
        w,
    )?;
    for id in &spec.literal_output_ids {
        reference(*id, values, w)?;
    }
    for mapping in &spec.mappings {
        reference(
            mapping
                .input_value_id
                .ok_or_else(|| invalid("Unpivot mapping input is absent"))?,
            values,
            w,
        )?;
        for constant in &mapping.constants {
            match constant
                .kind
                .as_ref()
                .ok_or_else(|| invalid("Unpivot constant kind is absent"))?
            {
                wire::unpivot_constant::Kind::ScalarRoot(id) => expressions.scalar(*id, w)?,
                wire::unpivot_constant::Kind::Int32List(reference)
                | wire::unpivot_constant::Kind::Utf8Map(reference) => {
                    address(decode_address(reference)?, expressions, w)?
                }
            }
        }
    }
    for property in input
        .required_inputs
        .iter()
        .chain(std::iter::once(output_properties))
    {
        wire_property_refs(property, values, w)?;
    }
    Ok(facts)
}
fn emit_encode(
    input: &p::PhysicalNode,
    source: usize,
    l: UnpivotNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<wire::PhysicalNode, Error> {
    let spec = encode_spec(input)?;
    let (inputs, required_inputs, output_properties, output) = encode_header(input, source, l, w)?;
    let mut passthrough = reserve(spec.passthrough.len(), w)?;
    for (from, to) in &spec.passthrough {
        passthrough.push(wire::ValueMapping {
            source_value_id: Some(from.get()),
            destination_value_id: Some(to.get()),
        });
        w.step()?;
    }
    let literal_output_ids = encode_ids(&spec.literal_outputs, w)?;
    let mut mappings = reserve(spec.mappings.len(), w)?;
    for mapping in &spec.mappings {
        let mut constants = reserve(mapping.constants.len(), w)?;
        for constant in &mapping.constants {
            constants.push(encode_constant(constant));
            w.step()?;
        }
        mappings.push(wire::UnpivotMapping {
            input_value_id: Some(mapping.input.get()),
            constants,
        });
        w.step()?;
    }
    let node = wire::PhysicalNode {
        expressions: None,
        id: input.id.get(),
        input_node_ids: inputs,
        required_inputs,
        output_properties: Some(output_properties),
        output: Some(output),
        kind: Some(wire::physical_node::Kind::Unpivot(wire::UnpivotNode {
            passthrough,
            value_output_id: Some(spec.value_output.get()),
            literal_output_ids,
            mappings,
            max_output_rows: spec.max_output_rows,
            max_output_bytes: spec.max_output_bytes,
        })),
    };
    w.step()?;
    Ok(node)
}
fn emit_decode(
    input: &wire::PhysicalNode,
    source: usize,
    l: UnpivotNodeProjectionLimits,
    w: &mut CompileCheckpoints<'_>,
) -> Result<p::PhysicalNode, Error> {
    let spec = decode_spec(input)?;
    let (inputs, required_inputs, output_properties, output) = decode_header(input, source, l, w)?;
    let mut passthrough = reserve(spec.passthrough.len(), w)?;
    for pair in &spec.passthrough {
        passthrough.push((
            p::ValueId::new(
                pair.source_value_id
                    .ok_or_else(|| invalid("prepared Unpivot passthrough source is absent"))?,
            ),
            p::ValueId::new(
                pair.destination_value_id
                    .ok_or_else(|| invalid("prepared Unpivot passthrough destination is absent"))?,
            ),
        ));
        w.step()?;
    }
    let passthrough = boxed(passthrough, w)?;
    let literal_outputs = decode_ids(&spec.literal_output_ids, w)?;
    let mut mappings = reserve(spec.mappings.len(), w)?;
    for mapping in &spec.mappings {
        let mut constants = reserve(mapping.constants.len(), w)?;
        for constant in &mapping.constants {
            constants.push(decode_constant(constant)?);
            w.step()?;
        }
        mappings.push(p::UnpivotValueMapping {
            input: p::ValueId::new(
                mapping
                    .input_value_id
                    .ok_or_else(|| invalid("prepared Unpivot mapping input is absent"))?,
            ),
            constants: boxed(constants, w)?,
        });
        w.step()?;
    }
    let mappings = boxed(mappings, w)?;
    let node = p::PhysicalNode {
        expressions: None,
        id: p::NodeId::new(input.id),
        inputs,
        required_inputs,
        output_properties,
        output,
        kind: p::NodeKind::Unpivot {
            spec: p::UnpivotSpec {
                passthrough,
                value_output: p::ValueId::new(
                    spec.value_output_id
                        .ok_or_else(|| invalid("prepared Unpivot value output is absent"))?,
                ),
                literal_outputs,
                mappings,
                max_output_rows: spec.max_output_rows,
                max_output_bytes: spec.max_output_bytes,
            },
        },
    };
    w.step()?;
    Ok(node)
}

/// Immutable preparation retains both actual emission loans. It does not rerun
/// source counting or reference validation when consumed.
pub struct PreparedUnpivotNodeEncode<'node, 'namespace, 'loan, 'source, 'control> {
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source: usize,
    limits: UnpivotNodeProjectionLimits,
    facts: UnpivotNodeProjectionFacts,
}
impl PreparedUnpivotNodeEncode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &UnpivotNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(wire::PhysicalNode, UnpivotNodeProjectionFacts), Error> {
        // Retaining this Values loan prevents exchanging the admitted namespace.
        let mut work =
            CompileCheckpoints::try_new(self.values.original_control(), CompilePhase::Encode)?;
        debug_assert!(std::ptr::eq(
            self.values.original_control(),
            self.expressions.control()
        ));
        let result = emit_encode(self.input, self.source, self.limits, &mut work)
            .map(|node| (node, self.facts));
        finish(work, result)
    }

    /// Emit the already admitted original body in the containing caller scope.
    pub fn emit_in(
        self,
        admit: &mut NodeAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(wire::PhysicalNode, UnpivotNodeProjectionFacts), Error> {
        if !std::ptr::addr_eq(self.values.original_control(), work.control()) {
            return Err(invalid(
                "node caller does not borrow the original controller",
            ));
        }
        admit(&self.facts)?;
        let node = emit_encode(self.input, self.source, self.limits, work)?;
        Ok((node, self.facts))
    }
}
pub fn prepare_unpivot_node_encode<'node, 'namespace, 'loan, 'source, 'control>(
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source_retained_bytes: usize,
    limits: UnpivotNodeProjectionLimits,
) -> Result<PreparedUnpivotNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let result = prepare_encode(
        input,
        values,
        expressions,
        source_retained_bytes,
        limits,
        &mut work,
        None,
    );
    let facts = finish(work, result)?;
    Ok(PreparedUnpivotNodeEncode {
        input,
        values,
        expressions,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
/// Caller-owned preparation; no entry, footer, or second namespace author.
pub fn prepare_unpivot_node_encode_in<'node, 'namespace, 'loan, 'source, 'control>(
    input: &'node p::PhysicalNode,
    values: &'namespace EncodedValues<'loan, 'source, 'control>,
    expressions: &'namespace EncodedExpressions<'loan, 'source, 'control>,
    source_retained_bytes: usize,
    limits: UnpivotNodeProjectionLimits,
    admit: &mut NodeAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedUnpivotNodeEncode<'node, 'namespace, 'loan, 'source, 'control>, Error> {
    if !std::ptr::addr_eq(values.original_control(), work.control()) {
        return Err(invalid(
            "node caller does not borrow the original controller",
        ));
    }
    let facts = prepare_encode(
        input,
        values,
        expressions,
        source_retained_bytes,
        limits,
        work,
        Some(admit),
    )?;
    Ok(PreparedUnpivotNodeEncode {
        input,
        values,
        expressions,
        source: source_retained_bytes,
        limits,
        facts,
    })
}

pub struct PreparedUnpivotNodeDecode<'node, 'namespace, 'loan, 'wire, 'control> {
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    source: usize,
    limits: UnpivotNodeProjectionLimits,
    facts: UnpivotNodeProjectionFacts,
}
impl PreparedUnpivotNodeDecode<'_, '_, '_, '_, '_> {
    pub fn facts(&self) -> &UnpivotNodeProjectionFacts {
        &self.facts
    }
    pub fn emit(self) -> Result<(p::PhysicalNode, UnpivotNodeProjectionFacts), Error> {
        let mut work =
            CompileCheckpoints::try_new(self.expressions.control(), CompilePhase::Decode)?;
        let result = emit_decode(self.input, self.source, self.limits, &mut work)
            .map(|node| (node, self.facts));
        finish(work, result)
    }

    /// Emit the already admitted original body in the containing caller scope.
    pub fn emit_in(
        self,
        admit: &mut NodeAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(p::PhysicalNode, UnpivotNodeProjectionFacts), Error> {
        if !std::ptr::addr_eq(self.expressions.control(), work.control()) {
            return Err(invalid(
                "node caller does not borrow the original controller",
            ));
        }
        admit(&self.facts)?;
        let node = emit_decode(self.input, self.source, self.limits, work)?;
        Ok((node, self.facts))
    }
}
pub fn prepare_unpivot_node_decode<'node, 'namespace, 'loan, 'wire, 'control>(
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    source_retained_bytes: usize,
    limits: UnpivotNodeProjectionLimits,
) -> Result<PreparedUnpivotNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
    let mut work = CompileCheckpoints::try_new(expressions.control(), CompilePhase::Decode)?;
    let result = prepare_decode(
        input,
        expressions,
        source_retained_bytes,
        limits,
        &mut work,
        None,
    );
    let facts = finish(work, result)?;
    Ok(PreparedUnpivotNodeDecode {
        input,
        expressions,
        source: source_retained_bytes,
        limits,
        facts,
    })
}
/// Caller-owned preparation; no entry, footer, or second namespace author.
pub fn prepare_unpivot_node_decode_in<'node, 'namespace, 'loan, 'wire, 'control>(
    input: &'node wire::PhysicalNode,
    expressions: &'namespace DecodedExpressions<'loan, 'wire, 'control>,
    source_retained_bytes: usize,
    limits: UnpivotNodeProjectionLimits,
    admit: &mut NodeAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedUnpivotNodeDecode<'node, 'namespace, 'loan, 'wire, 'control>, Error> {
    if !std::ptr::addr_eq(expressions.control(), work.control()) {
        return Err(invalid(
            "node caller does not borrow the original controller",
        ));
    }
    let facts = prepare_decode(
        input,
        expressions,
        source_retained_bytes,
        limits,
        work,
        Some(admit),
    )?;
    Ok(PreparedUnpivotNodeDecode {
        input,
        expressions,
        source: source_retained_bytes,
        limits,
        facts,
    })
}

pub fn encode_unpivot_node(
    input: &p::PhysicalNode,
    values: &EncodedValues<'_, '_, '_>,
    expressions: &EncodedExpressions<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: UnpivotNodeProjectionLimits,
) -> Result<(wire::PhysicalNode, UnpivotNodeProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(values.original_control(), CompilePhase::Encode)?;
    let result = (|| {
        let facts = prepare_encode(
            input,
            values,
            expressions,
            source_retained_bytes,
            limits,
            &mut work,
            None,
        )?;
        let node = emit_encode(input, source_retained_bytes, limits, &mut work)?;
        Ok((node, facts))
    })();
    finish(work, result)
}
pub fn decode_unpivot_node(
    input: &wire::PhysicalNode,
    expressions: &DecodedExpressions<'_, '_, '_>,
    source_retained_bytes: usize,
    limits: UnpivotNodeProjectionLimits,
) -> Result<(p::PhysicalNode, UnpivotNodeProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(expressions.control(), CompilePhase::Decode)?;
    let result = (|| {
        let facts = prepare_decode(
            input,
            expressions,
            source_retained_bytes,
            limits,
            &mut work,
            None,
        )?;
        let node = emit_decode(input, source_retained_bytes, limits, &mut work)?;
        Ok((node, facts))
    })();
    finish(work, result)
}
#[cfg(test)]
pub(crate) mod tests;
