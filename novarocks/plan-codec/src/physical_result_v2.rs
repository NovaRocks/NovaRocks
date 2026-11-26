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

//! Complete result vocabulary projection, without Fragment/Package certification.
//! Exact types and namespace loans remain with their original owners. Numerical
//! source/request facts are not a host allocation grant or an opaque time bound.

use crate::{
    borrowed_type_resources::{
        type_binding_prefix_work_upper_bound_in, verify_type_binding,
        verify_type_binding_optional_work_in,
    },
    physical_node_v2::*,
    physical_type_v2::{self, DecodedTypeTable, TypeCodecError},
    physical_value_v2::{DecodedValues, EncodedValues, ValueCodecError},
};
use novarocks_physical_plan as p;
use novarocks_proto_models::physical_package_v2 as wire;
#[cfg(test)]
use novarocks_type_contract::CompilePhase;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, FunctionValueType, PureCompileControl,
};
use std::mem::size_of;
type Error = NodeCodecError;
fn type_error(error: TypeCodecError) -> Error {
    ValueCodecError::from(error).into()
}
fn required<T>(
    value: Option<T>,
    text: &'static str,
    w: &mut CompileCheckpoints<'_>,
) -> Result<T, Error> {
    let result = value.ok_or_else(|| invalid(text));
    w.step()?;
    result
}
trait Namespace: Values {
    fn ty<'a>(
        &'a self,
        id: u32,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<&'a FunctionValueType, Error> {
        self.ty_captured(id, &mut |_, _| Ok(()), w)
    }
    fn ty_captured<'a>(
        &'a self,
        id: u32,
        capture: &mut impl FnMut(
            &'a FunctionValueType,
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), Error>,
        w: &mut CompileCheckpoints<'_>,
    ) -> Result<&'a FunctionValueType, Error> {
        let found = self.value_captured(id, &mut |value, work| capture(&value.ty, work), w)?;
        w.step()?;
        found
            .map(|value| &value.ty)
            .ok_or_else(|| invalid("result value reference is unknown"))
    }
}
impl Namespace for EncodedValues<'_, '_, '_> {}
impl Namespace for DecodedValues<'_, '_, '_> {}
fn decoded_type_captured<'a>(
    types: &'a DecodedTypeTable,
    id: u32,
    capture: &mut impl FnMut(&'a FunctionValueType, &mut CompileCheckpoints<'_>) -> Result<(), Error>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a FunctionValueType, Error> {
    w.flush()?;
    let ty = types.value_type(id);
    if let Some(ty) = ty {
        capture(ty, w)?;
    }
    w.step()?;
    w.flush()?;
    ty.ok_or_else(|| invalid("result type reference is unknown"))
}
fn decoded_type<'a>(
    types: &'a DecodedTypeTable,
    id: u32,
    w: &mut CompileCheckpoints<'_>,
) -> Result<&'a FunctionValueType, Error> {
    decoded_type_captured(types, id, &mut |_, _| Ok(()), w)
}
struct Admission<'loan, 'parent> {
    source: usize,
    values: usize,
    limits: Option<NodeProjectionLimits>,
    parent: Option<&'loan mut NodeAdmit<'parent>>,
    work_peak: usize,
}
impl Admission<'_, '_> {
    fn observed(&self) -> bool {
        self.parent.is_some()
    }
    fn admit(&mut self, model: &Model) -> Result<NodeProjectionFacts, Error> {
        let mut facts = model.numerical_facts(self.source, self.values, self.limits)?;
        if let Some(parent) = self.parent.as_deref_mut() {
            self.work_peak = self.work_peak.max(facts.cumulative_work_upper_bound);
            if let Some(limits) = self.limits {
                check_cap(self.work_peak, limits.max_work)?;
            }
            facts.cumulative_work_upper_bound = self.work_peak;
            parent(&facts)?;
        }
        Ok(facts)
    }
    fn facts(
        &mut self,
        model: &Model,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<NodeProjectionFacts, Error> {
        match self.parent.as_deref_mut() {
            Some(parent) => {
                let peak = &mut self.work_peak;
                let mut facts = model.facts_in(
                    self.source,
                    self.values,
                    self.limits,
                    &mut |facts| {
                        *peak = (*peak).max(facts.cumulative_work_upper_bound);
                        if self.limits.is_some_and(|limits| *peak > limits.max_work) {
                            return Err(CompileControlError::ResourceExhausted);
                        }
                        let mut current = *facts;
                        current.cumulative_work_upper_bound = *peak;
                        parent(&current)
                    },
                    work,
                )?;
                facts.cumulative_work_upper_bound = self.work_peak;
                Ok(facts)
            }
            None => model.facts(self.source, self.values, self.limits, work),
        }
    }
    fn comparison_prefix(
        &mut self,
        left: &FunctionValueType,
        right: &FunctionValueType,
        model: &Model,
    ) -> Result<(), Error> {
        let prefix = type_binding_prefix_work_upper_bound_in(left, right, self.source)
            .map_err(type_error)?;
        let mut next = *model;
        next.delegated_work = add(next.delegated_work, prefix.work_upper_bound())?;
        self.admit(&next)?;
        Ok(())
    }
}
fn remaining(
    model: &Model,
    admission: &mut Admission<'_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<Option<usize>, Error> {
    let facts = if admission.observed() {
        // The known comparison prefix is a replacement work ceiling, not a
        // second charge deducted from this same delegated comparison's room.
        let actual = model.numerical_facts(admission.source, admission.values, admission.limits)?;
        admission.facts(model, w)?;
        actual
    } else {
        admission.facts(model, w)?
    };
    admission
        .limits
        .map(|limits| {
            limits
                .max_work
                .checked_sub(facts.cumulative_work_upper_bound)
                .ok_or_else(|| CompileControlError::ResourceExhausted.into())
        })
        .transpose()
}
fn compare(
    left: &FunctionValueType,
    right: &FunctionValueType,
    model: &mut Model,
    admission: &mut Admission<'_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    if admission.observed() {
        admission.comparison_prefix(left, right, model)?;
    }
    let room = remaining(model, admission, w)?;
    let compared = if admission.observed() {
        let base = model.delegated_work;
        verify_type_binding_optional_work_in::<Error>(
            left,
            right,
            admission.source,
            room,
            &mut |prefix| {
                model.delegated_work = add(base, prefix.work_upper_bound())?;
                admission.admit(model)?;
                Ok(())
            },
            w,
        )
        .map_err(|error| match error {
            Error::Type(error) => type_error(error),
            error => error,
        })?
    } else {
        let compared =
            verify_type_binding(left, right, admission.source, room, w).map_err(type_error)?;
        model.delegated_work = add(model.delegated_work, compared.work_upper_bound())?;
        compared
    };
    admission.facts(model, w)?;
    let matches = compared.matches();
    w.step()?;
    if !matches {
        return Err(invalid("result full type differs from original namespace"));
    }
    Ok(())
}
fn physical_floor(input: &p::ResultPort, ids: &[u32]) -> Result<usize, Error> {
    add(
        size_of::<p::ResultPort>(),
        add(
            bytes::<p::ValueId>(input.output.columns.len())?,
            add(
                bytes::<p::ResultField>(input.fields.len())?,
                bytes::<u32>(ids.len())?,
            )?,
        )?,
    )
}
fn wire_floor(input: &wire::ResultPort, output: &wire::OutputPort) -> Result<usize, Error> {
    add(
        size_of::<wire::ResultPort>(),
        add(
            bytes::<u32>(output.value_ids.capacity())?,
            bytes::<wire::ResultField>(input.fields.capacity())?,
        )?,
    )
}
fn text_request(
    model: &mut Model,
    text: &str,
    copies: usize,
    admission: &mut Admission<'_, '_>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    model.request::<u8>(text.len(), copies)?;
    admission.facts(model, w)?;
    Ok(())
}
fn preflight_encode(
    input: Option<&p::ResultPort>,
    ids: &[u32],
    values: &EncodedValues<'_, '_, '_>,
    source: usize,
    limits: Option<NodeProjectionLimits>,
    parent: Option<&mut NodeAdmit<'_>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<NodeProjectionFacts, Error> {
    let mut admission = Admission {
        source,
        values: values.count(),
        limits,
        parent,
        work_peak: 0,
    };
    let mut model = Model::default();
    let known_namespace = if admission.observed() {
        values.retained_floor_header()?
    } else {
        values.retained_floor(w)?
    };
    if admission.observed() {
        if let Some(input) = input {
            physical_floor(input, ids)?;
            model.items = add(input.output.columns.len(), input.fields.len())?;
            model.refs = model.items;
            model.request::<u32>(input.output.columns.len(), 1)?;
            model.request::<wire::ResultField>(input.fields.len(), 1)?;
            if input.scalar_schema.is_some() {
                model.request::<u8>(
                    crate::physical_fragment_envelope_v2::root_projection_request_bytes()?,
                    1,
                )?;
            }
            model.delegated_work = mul(
                input.fields.len(),
                add(values.types().source_counts().0, 8)?,
            )?;
        }
        admission.admit(&model)?;
    }
    let Some(input) = input else {
        let empty = ids.is_empty();
        w.step()?;
        if !empty {
            return Err(invalid("absent result has field type IDs"));
        }
        count_prefix(0, 0, source, known_namespace, limits, w)?;
        return admission.facts(&model, w);
    };
    // The value namespace retains the original whole-source invoice, which
    // already covers this ResultPort. These necessary floors may overlap.
    let mut known = physical_floor(input, ids)?;
    model.items = add(input.output.columns.len(), input.fields.len())?;
    model.refs = model.items;
    count_prefix(
        0,
        model.items,
        source,
        known.max(known_namespace),
        limits,
        w,
    )?;
    admission.facts(&model, w)?;
    let same_width = ids.len() == input.fields.len();
    w.step()?;
    if !same_width {
        return Err(invalid("result field type ID count differs"));
    }
    if !admission.observed() {
        model.request::<u32>(input.output.columns.len(), 1)?;
        model.request::<wire::ResultField>(input.fields.len(), 1)?;
        if input.scalar_schema.is_some() {
            model.request::<u8>(
                crate::physical_fragment_envelope_v2::root_projection_request_bytes()?,
                1,
            )?;
        }
        let roots = values.types().source_counts().0;
        model.delegated_work = add(
            model.delegated_work,
            mul(input.fields.len(), add(roots, 8)?)?,
        )?;
    }
    admission.facts(&model, w)?;
    for field in &input.fields {
        known = add(
            known,
            add(
                field.name.len(),
                field.alias.as_ref().map_or(0, |v| v.len()),
            )?,
        )?;
        if admission.observed() {
            model.request::<u8>(field.name.len(), 1)?;
            if let Some(alias) = &field.alias {
                model.request::<u8>(alias.len(), 1)?;
            }
            admission.admit(&model)?;
        }
        floor(source, known.max(known_namespace), w)?;
        if !admission.observed() {
            text_request(&mut model, &field.name, 1, &mut admission, w)?;
            if let Some(alias) = &field.alias {
                text_request(&mut model, alias, 1, &mut admission, w)?;
            }
        }
        w.step()?;
    }
    for value in &input.output.columns {
        values.ty(value.get(), w)?;
    }
    for (field, id) in input.fields.iter().zip(ids) {
        if admission.observed() {
            model.delegated_work = add(
                model.delegated_work,
                physical_type_v2::value_type_clone_preflight_work_upper_bound(),
            )?;
            admission.admit(&model)?;
        }
        let referenced = if admission.observed() {
            values
                .types()
                .value_type_captured::<Error>(
                    *id,
                    &mut |ty, _| admission.comparison_prefix(&field.ty, ty, &model),
                    w,
                )
                .map_err(|error| match error {
                    Error::Type(error) => type_error(error),
                    error => error,
                })?
        } else {
            values
                .types()
                .value_type_observed(*id, w)
                .map_err(type_error)?
        };
        w.step()?;
        let referenced = referenced.ok_or_else(|| invalid("result type reference is unknown"))?;
        compare(&field.ty, referenced, &mut model, &mut admission, w)?;
        let value_ty = if admission.observed() {
            values.ty_captured(
                field.value.get(),
                &mut |ty, _| admission.comparison_prefix(&field.ty, ty, &model),
                w,
            )?
        } else {
            values.ty(field.value.get(), w)?
        };
        compare(&field.ty, value_ty, &mut model, &mut admission, w)?;
        // Direct Dictionary Boxes are owned by this actual ResultField FVT;
        // shared FieldRef children are neither copied nor billed as deep clones.
        // Admit the sole owner's clone-preflight bound before that actual
        // walk. Keep this early ceiling in the final facts for tight replay.
        if !admission.observed() {
            model.delegated_work = add(
                model.delegated_work,
                physical_type_v2::value_type_clone_preflight_work_upper_bound(),
            )?;
        }
        admission.facts(&model, w)?;
        if admission.observed() {
            let base = known;
            physical_type_v2::preflight_value_type_clone_admitted::<Error>(
                &field.ty,
                &mut |clone, _| {
                    known = add(base, clone.allocation_request_bytes_upper_bound())?;
                    admission.admit(&model)?;
                    Ok(())
                },
                w,
            )
            .map_err(|error| match error {
                Error::Type(error) => type_error(error),
                error => error,
            })?;
        } else {
            let clone =
                physical_type_v2::preflight_value_type_clone(&field.ty, w).map_err(type_error)?;
            known = add(known, clone.allocation_request_bytes_upper_bound())?;
        }
        floor(source, known.max(known_namespace), w)?;
        admission.facts(&model, w)?;
    }
    admission.facts(&model, w)
}
fn preflight_decode(
    input: Option<&wire::ResultPort>,
    values: &DecodedValues<'_, '_, '_>,
    source: usize,
    limits: Option<NodeProjectionLimits>,
    parent: Option<&mut NodeAdmit<'_>>,
    w: &mut CompileCheckpoints<'_>,
) -> Result<NodeProjectionFacts, Error> {
    let mut admission = Admission {
        source,
        values: values.count(),
        limits,
        parent,
        work_peak: 0,
    };
    let mut model = Model::default();
    let known_namespace = if admission.observed() {
        values.retained_floor_header()?
    } else {
        values.retained_floor(w)?
    };
    if admission.observed() {
        if let Some(input) = input {
            let columns = input
                .output
                .as_ref()
                .map_or(0, |output| output.value_ids.len());
            if let Some(output) = &input.output {
                wire_floor(input, output)?;
            }
            model.items = add(columns, input.fields.len())?;
            model.refs = model.items;
            model.request::<p::ValueId>(columns, 2)?;
            model.request::<p::ResultField>(input.fields.len(), 2)?;
            if input.scalar_schema.is_some() {
                model.request::<u8>(
                    crate::physical_fragment_envelope_v2::root_projection_request_bytes()?,
                    1,
                )?;
            }
            let lookup = crate::btree_resources_v2::lookup_work(values.types().value_types().len())
                .map_err(invalid)?;
            model.delegated_work = mul(input.fields.len(), mul(lookup, 2)?)?;
        }
        admission.admit(&model)?;
    }
    let Some(input) = input else {
        count_prefix(0, 0, source, known_namespace, limits, w)?;
        return admission.facts(&model, w);
    };
    required(input.fragment_id, "result fragment ID is absent", w)?;
    let output = required(input.output.as_ref(), "result output port is absent", w)?;
    required(output.node_id, "result output node ID is absent", w)?;
    // The admitted wire source already contains the result DTO. Preserve
    // both necessary floors without charging their shared backing twice.
    let mut known = wire_floor(input, output)?;
    model.items = add(output.value_ids.len(), input.fields.len())?;
    model.refs = model.items;
    count_prefix(
        0,
        model.items,
        source,
        known.max(known_namespace),
        limits,
        w,
    )?;
    if !admission.observed() {
        model.request::<p::ValueId>(output.value_ids.len(), 2)?;
        model.request::<p::ResultField>(input.fields.len(), 2)?;
        if input.scalar_schema.is_some() {
            model.request::<u8>(
                crate::physical_fragment_envelope_v2::root_projection_request_bytes()?,
                1,
            )?;
        }
        let lookup = crate::btree_resources_v2::lookup_work(values.types().value_types().len())
            .map_err(invalid)?;
        model.delegated_work = mul(input.fields.len(), mul(lookup, 2)?)?;
    }
    admission.facts(&model, w)?;
    for field in &input.fields {
        known = add(
            known,
            add(
                field.name.capacity(),
                field.alias.as_ref().map_or(0, String::capacity),
            )?,
        )?;
        if admission.observed() {
            model.request::<u8>(field.name.len(), 2)?;
            if let Some(alias) = &field.alias {
                model.request::<u8>(alias.len(), 2)?;
            }
            admission.admit(&model)?;
        }
        floor(source, known.max(known_namespace), w)?;
        if !admission.observed() {
            text_request(&mut model, &field.name, 2, &mut admission, w)?;
            if let Some(alias) = &field.alias {
                text_request(&mut model, alias, 2, &mut admission, w)?;
            }
        }
        required(field.value_id, "result field value ID is absent", w)?;
        required(field.value_type_id, "result field type ID is absent", w)?;
        w.step()?;
    }
    for value in &output.value_ids {
        values.ty(*value, w)?;
    }
    for field in &input.fields {
        let id = required(field.value_type_id, "result field type ID is absent", w)?;
        let mut root_clone = None;
        let ty = if admission.observed() {
            decoded_type_captured(
                values.types(),
                id,
                &mut |ty, _| {
                    let root =
                        physical_type_v2::value_type_clone_root_facts(ty).map_err(type_error)?;
                    model.requests = add(model.requests, root.allocation_requests_upper_bound())?;
                    model.requested =
                        add(model.requested, root.allocation_request_bytes_upper_bound())?;
                    model.delegated_work = add(
                        model.delegated_work,
                        mul(
                            physical_type_v2::value_type_clone_preflight_work_upper_bound(),
                            2,
                        )?,
                    )?;
                    root_clone = Some(root);
                    admission.admit(&model)?;
                    Ok(())
                },
                w,
            )?
        } else {
            decoded_type(values.types(), id, w)?
        };
        let value = required(field.value_id, "result field value ID is absent", w)?;
        let value_ty = if admission.observed() {
            values.ty_captured(
                value,
                &mut |value_ty, _| admission.comparison_prefix(ty, value_ty, &model),
                w,
            )?
        } else {
            values.ty(value, w)?
        };
        compare(ty, value_ty, &mut model, &mut admission, w)?;
        // This covers both the original preflight and the later emit clone;
        // the actual loops still observe only their completed work.
        if admission.observed() {
            admission.facts(&model, w)?;
            let mut previous =
                root_clone.ok_or_else(|| invalid("captured result clone root is absent"))?;
            physical_type_v2::preflight_value_type_clone_admitted::<Error>(
                ty,
                &mut |clone, _| {
                    model.requests = add(
                        model.requests,
                        clone
                            .allocation_requests_upper_bound()
                            .checked_sub(previous.allocation_requests_upper_bound())
                            .ok_or(CompileControlError::ResourceExhausted)?,
                    )?;
                    model.requested = add(
                        model.requested,
                        clone
                            .allocation_request_bytes_upper_bound()
                            .checked_sub(previous.allocation_request_bytes_upper_bound())
                            .ok_or(CompileControlError::ResourceExhausted)?,
                    )?;
                    previous = clone;
                    admission.admit(&model)?;
                    Ok(())
                },
                w,
            )
            .map_err(|error| match error {
                Error::Type(error) => type_error(error),
                error => error,
            })?;
        } else {
            model.delegated_work = add(
                model.delegated_work,
                mul(
                    physical_type_v2::value_type_clone_preflight_work_upper_bound(),
                    2,
                )?,
            )?;
            admission.facts(&model, w)?;
            let clone = physical_type_v2::preflight_value_type_clone(ty, w).map_err(type_error)?;
            model.requests = add(model.requests, clone.allocation_requests_upper_bound())?;
            model.requested = add(
                model.requested,
                clone.allocation_request_bytes_upper_bound(),
            )?;
        }
        admission.facts(&model, w)?;
    }
    admission.facts(&model, w)
}
pub(crate) fn copy_string(input: &str, w: &mut CompileCheckpoints<'_>) -> Result<String, Error> {
    let mut bytes = reserve::<u8>(input.len(), w)?;
    for byte in input.bytes() {
        bytes.push(byte);
        w.step()?;
    }
    // The library UTF8 check consumes the actual immutable copied extent;
    // its work is bracketed opaque, not synthetic completed-byte callbacks.
    w.flush()?;
    let result = String::from_utf8(bytes).map_err(|_| invalid("copied result text is not UTF8"));
    w.step()?;
    w.flush()?;
    result
}
pub(crate) fn copy_box(input: &str, w: &mut CompileCheckpoints<'_>) -> Result<Box<str>, Error> {
    let copied = copy_string(input, w)?;
    w.flush()?;
    let output = copied.into_boxed_str();
    w.flush()?;
    Ok(output)
}
fn emit_encode(
    input: Option<&p::ResultPort>,
    ids: &[u32],
    w: &mut CompileCheckpoints<'_>,
) -> Result<Option<wire::ResultPort>, Error> {
    let Some(input) = input else {
        return Ok(None);
    };
    let mut columns = reserve(input.output.columns.len(), w)?;
    for value in &input.output.columns {
        columns.push(value.get());
        w.step()?;
    }
    let mut fields = reserve(input.fields.len(), w)?;
    for (field, id) in input.fields.iter().zip(ids) {
        let name = copy_string(&field.name, w)?;
        let alias = field
            .alias
            .as_ref()
            .map(|v| copy_string(v, w))
            .transpose()?;
        fields.push(wire::ResultField {
            name,
            alias,
            value_id: Some(field.value.get()),
            value_type_id: Some(*id),
            domain: Some(encode_domain(field.domain)),
        });
        w.step()?;
    }
    Ok(Some(wire::ResultPort {
        fragment_id: Some(input.fragment.get()),
        output: Some(wire::OutputPort {
            node_id: Some(input.output.node.get()),
            value_ids: columns,
        }),
        fields,
        scalar_schema: match &input.scalar_schema {
            Some(schema) => {
                w.flush()?;
                let output = novarocks_proto_codec::scalar_result::encode_scalar_schema(schema);
                w.flush()?;
                Some(output)
            }
            None => None,
        },
    }))
}
fn emit_decode(
    input: Option<&wire::ResultPort>,
    types: &DecodedTypeTable,
    w: &mut CompileCheckpoints<'_>,
) -> Result<Option<p::ResultPort>, Error> {
    let Some(input) = input else {
        return Ok(None);
    };
    let output = required(input.output.as_ref(), "result output port is absent", w)?;
    let mut columns = reserve(output.value_ids.len(), w)?;
    for value in &output.value_ids {
        columns.push(p::ValueId::new(*value));
        w.step()?;
    }
    let mut fields = reserve(input.fields.len(), w)?;
    for field in &input.fields {
        let ty = decoded_type(
            types,
            required(field.value_type_id, "result field type ID is absent", w)?,
            w,
        )?;
        let ty = physical_type_v2::clone_value_type_observed(ty, w).map_err(type_error)?;
        let name = copy_box(&field.name, w)?;
        let alias = field.alias.as_ref().map(|v| copy_box(v, w)).transpose()?;
        fields.push(p::ResultField {
            name,
            alias,
            value: p::ValueId::new(required(
                field.value_id,
                "result field value ID is absent",
                w,
            )?),
            ty,
            domain: decode_domain(
                required(field.domain, "result field domain is absent", w)?,
                w,
            )?,
        });
        w.step()?;
    }
    Ok(Some(p::ResultPort {
        fragment: p::FragmentId::new(required(
            input.fragment_id,
            "result fragment ID is absent",
            w,
        )?),
        output: p::OutputPort {
            node: p::NodeId::new(required(
                output.node_id,
                "result output node ID is absent",
                w,
            )?),
            columns: boxed(columns, w)?,
        },
        scalar_schema: match &input.scalar_schema {
            Some(schema) => {
                w.flush()?;
                // ResultPort carries a compiler semantic tree before native
                // slots exist. Already-bound inputs retain the native decoder;
                // neither branch retries or invents a slot from a ValueId.
                let path = novarocks_proto_codec::FieldPath::root("result.scalar_schema");
                let output = match schema.source_slot {
                    Some(_) => novarocks_proto_codec::scalar_result::decode_scalar_schema(
                        schema,
                        output.value_ids.len(),
                        path,
                    ),
                    None => {
                        novarocks_proto_codec::scalar_result::decode_semantic_result_scalar_schema(
                            schema,
                            output.value_ids.len(),
                            path,
                        )
                    }
                }
                .map_err(Error::Root)?;
                w.flush()?;
                Some(output)
            }
            None => None,
        },
        fields: boxed(fields, w)?,
    }))
}
pub(crate) struct PreparedResultEncode<'input, 'values, 'loan, 'source, 'control> {
    input: Option<&'input p::ResultPort>,
    ids: &'input [u32],
    values: &'values EncodedValues<'loan, 'source, 'control>,
    control: &'control dyn PureCompileControl,
    facts: NodeProjectionFacts,
}
impl PreparedResultEncode<'_, '_, '_, '_, '_> {
    #[cfg(test)]
    pub(crate) fn facts(&self) -> &NodeProjectionFacts {
        &self.facts
    }
    #[cfg(test)]
    pub(crate) fn emit(self) -> Result<(Option<wire::ResultPort>, NodeProjectionFacts), Error> {
        let mut w = CompileCheckpoints::try_new(self.control, CompilePhase::Encode)?;
        // Retain the exact original namespace until the component is published.
        let _values = self.values;
        let result = emit_encode(self.input, self.ids, &mut w).map(|v| (v, self.facts));
        finish(w, result)
    }
    pub(crate) fn emit_in(
        self,
        admit: &mut NodeAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(Option<wire::ResultPort>, NodeProjectionFacts), Error> {
        if !std::ptr::addr_eq(self.control, work.control()) {
            return Err(invalid(
                "result control differs from original value namespace",
            ));
        }
        admit(&self.facts)?;
        let _values = self.values;
        emit_encode(self.input, self.ids, work).map(|output| (output, self.facts))
    }
}
pub(crate) fn prepare_result_encode_in<'input, 'values, 'loan, 'source, 'control>(
    input: Option<&'input p::ResultPort>,
    ids: &'input [u32],
    values: &'values EncodedValues<'loan, 'source, 'control>,
    source: usize,
    limits: impl Into<Option<NodeProjectionLimits>>,
    admit: &mut NodeAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedResultEncode<'input, 'values, 'loan, 'source, 'control>, Error> {
    let control = values.original_control();
    if !std::ptr::addr_eq(control, work.control()) {
        return Err(invalid(
            "result control differs from original value namespace",
        ));
    }
    let facts = preflight_encode(input, ids, values, source, limits.into(), Some(admit), work)?;
    Ok(PreparedResultEncode {
        input,
        ids,
        values,
        control,
        facts,
    })
}
#[cfg(test)]
pub(crate) fn prepare_result_encode<'input, 'values, 'loan, 'source, 'control>(
    input: Option<&'input p::ResultPort>,
    ids: &'input [u32],
    values: &'values EncodedValues<'loan, 'source, 'control>,
    source: usize,
    limits: impl Into<Option<NodeProjectionLimits>>,
    control: &'control dyn PureCompileControl,
) -> Result<PreparedResultEncode<'input, 'values, 'loan, 'source, 'control>, Error> {
    let mut w = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = (|| {
        let same = std::ptr::eq(control, values.original_control());
        w.step()?;
        if !same {
            return Err(invalid(
                "result control differs from original value namespace",
            ));
        }
        preflight_encode(input, ids, values, source, limits.into(), None, &mut w)
    })();
    let facts = finish(w, result)?;
    Ok(PreparedResultEncode {
        input,
        ids,
        values,
        control,
        facts,
    })
}
pub(crate) struct PreparedResultDecode<'input, 'values, 'loan, 'wire, 'control> {
    input: Option<&'input wire::ResultPort>,
    values: &'values DecodedValues<'loan, 'wire, 'control>,
    control: &'control dyn PureCompileControl,
    facts: NodeProjectionFacts,
}
impl PreparedResultDecode<'_, '_, '_, '_, '_> {
    #[cfg(test)]
    pub(crate) fn facts(&self) -> &NodeProjectionFacts {
        &self.facts
    }
    #[cfg(test)]
    pub(crate) fn emit(self) -> Result<(Option<p::ResultPort>, NodeProjectionFacts), Error> {
        let mut w = CompileCheckpoints::try_new(self.control, CompilePhase::Decode)?;
        let result = emit_decode(self.input, self.values.types(), &mut w).map(|v| (v, self.facts));
        finish(w, result)
    }
    pub(crate) fn emit_in(
        self,
        admit: &mut NodeAdmit<'_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(Option<p::ResultPort>, NodeProjectionFacts), Error> {
        if !std::ptr::addr_eq(self.control, work.control()) {
            return Err(invalid(
                "result control differs from original value namespace",
            ));
        }
        admit(&self.facts)?;
        emit_decode(self.input, self.values.types(), work).map(|output| (output, self.facts))
    }
}
pub(crate) fn prepare_result_decode_in<'input, 'values, 'loan, 'wire, 'control>(
    input: Option<&'input wire::ResultPort>,
    values: &'values DecodedValues<'loan, 'wire, 'control>,
    source: usize,
    limits: impl Into<Option<NodeProjectionLimits>>,
    admit: &mut NodeAdmit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PreparedResultDecode<'input, 'values, 'loan, 'wire, 'control>, Error> {
    let control = values.original_control();
    if !std::ptr::addr_eq(control, work.control()) {
        return Err(invalid(
            "result control differs from original value namespace",
        ));
    }
    let facts = preflight_decode(input, values, source, limits.into(), Some(admit), work)?;
    Ok(PreparedResultDecode {
        input,
        values,
        control,
        facts,
    })
}
#[cfg(test)]
pub(crate) fn prepare_result_decode<'input, 'values, 'loan, 'wire, 'control>(
    input: Option<&'input wire::ResultPort>,
    values: &'values DecodedValues<'loan, 'wire, 'control>,
    source: usize,
    limits: impl Into<Option<NodeProjectionLimits>>,
    control: &'control dyn PureCompileControl,
) -> Result<PreparedResultDecode<'input, 'values, 'loan, 'wire, 'control>, Error> {
    let mut w = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = (|| {
        let same = std::ptr::eq(control, values.original_control());
        w.step()?;
        if !same {
            return Err(invalid(
                "result control differs from original value namespace",
            ));
        }
        preflight_decode(input, values, source, limits.into(), None, &mut w)
    })();
    let facts = finish(w, result)?;
    Ok(PreparedResultDecode {
        input,
        values,
        control,
        facts,
    })
}
#[cfg(test)]
#[path = "physical_result_v2/tests.rs"]
mod tests;

fn encode_domain(domain: p::ResultValueDomain) -> i32 {
    use p::ResultValueDomain as P;
    use wire::ResultValueDomain as W;
    (match domain {
        P::Plain => W::Plain,
        P::Json => W::Json,
        P::Variant => W::Variant,
        P::Hll => W::Hll,
        P::Bitmap => W::Bitmap,
        P::Object => W::Object,
        P::Percentile => W::Percentile,
    }) as i32
}
fn decode_domain(raw: i32, w: &mut CompileCheckpoints<'_>) -> Result<p::ResultValueDomain, Error> {
    use p::ResultValueDomain as P;
    use wire::ResultValueDomain as W;
    let decoded = W::try_from(raw).map_err(|_| invalid("unknown result value domain"));
    w.step()?;
    Ok(match decoded? {
        W::Plain => P::Plain,
        W::Json => P::Json,
        W::Variant => P::Variant,
        W::Hll => P::Hll,
        W::Bitmap => P::Bitmap,
        W::Object => P::Object,
        W::Percentile => P::Percentile,
    })
}
