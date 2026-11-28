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

//! ContentApply payload within the original node projection and admission.

use super::{Error, Model, add, boxed, bytes, p, required, reserve, wire};
use novarocks_type_contract::CompileCheckpoints;

pub(super) fn physical_shape(spec: &p::ContentApplySpec) -> Result<(usize, usize), Error> {
    let content = add(spec.weighted_content.len(), spec.target_content.len())?;
    let keys = spec.route_keys.len();
    Ok((
        add(add(content, keys)?, 5)?,
        add(bytes::<p::ValueId>(content)?, bytes::<u32>(keys)?)?,
    ))
}

pub(super) fn wire_shape(v: &wire::ContentApplyNode) -> Result<(usize, usize), Error> {
    let content = add(
        v.weighted_content_value_ids.len(),
        v.target_content_value_ids.len(),
    )?;
    let capacity = add(
        add(
            v.weighted_content_value_ids.capacity(),
            v.target_content_value_ids.capacity(),
        )?,
        v.route_key_ordinals.capacity(),
    )?;
    Ok((
        add(add(content, v.route_key_ordinals.len())?, 5)?,
        bytes::<u32>(capacity)?,
    ))
}

pub(super) fn model_encode(spec: &p::ContentApplySpec, model: &mut Model) -> Result<(), Error> {
    model.refs = add(
        model.refs,
        add(
            add(spec.weighted_content.len(), spec.target_content.len())?,
            5,
        )?,
    )?;
    for count in [
        spec.weighted_content.len(),
        spec.target_content.len(),
        spec.route_keys.len(),
    ] {
        model.request::<u32>(count, 1)?;
    }
    Ok(())
}

pub(super) fn model_decode(v: &wire::ContentApplyNode, model: &mut Model) -> Result<(), Error> {
    let weighted = v.weighted_content_value_ids.len();
    let target = v.target_content_value_ids.len();
    model.refs = add(model.refs, add(add(weighted, target)?, 5)?)?;
    model.request::<p::ValueId>(weighted, 2)?;
    model.request::<p::ValueId>(target, 2)?;
    model.request::<u32>(v.route_key_ordinals.len(), 2)
}

pub(super) fn physical_references(
    spec: &p::ContentApplySpec,
) -> impl Iterator<Item = p::ValueId> + '_ {
    spec.weighted_content
        .iter()
        .chain(&spec.target_content)
        .copied()
        .chain([spec.weight])
        .chain(spec.target.columns())
}

pub(super) fn wire_references<'v>(
    v: &'v wire::ContentApplyNode,
    work: &mut CompileCheckpoints<'_>,
) -> Result<impl Iterator<Item = u32> + use<'v>, Error> {
    Ok(v.weighted_content_value_ids
        .iter()
        .chain(&v.target_content_value_ids)
        .copied()
        .chain(wire_fields(v, work)?))
}

fn wire_fields(
    v: &wire::ContentApplyNode,
    work: &mut CompileCheckpoints<'_>,
) -> Result<[u32; 5], Error> {
    Ok([
        required(v.weight_value_id, work)?,
        required(v.file_value_id, work)?,
        required(v.position_value_id, work)?,
        required(v.spec_id_value_id, work)?,
        required(v.partition_value_id, work)?,
    ])
}

fn copy<T>(
    len: usize,
    values: impl IntoIterator<Item = T>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<T>, Error> {
    let mut output = reserve(len, work)?;
    for value in values {
        output.push(value);
        work.step()?;
    }
    Ok(output)
}

pub(super) fn emit_wire(
    spec: &p::ContentApplySpec,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::ContentApplyNode, Error> {
    let ids = |values: &[p::ValueId], work: &mut CompileCheckpoints<'_>| {
        copy(values.len(), values.iter().map(|value| value.get()), work)
    };
    Ok(wire::ContentApplyNode {
        weighted_content_value_ids: ids(&spec.weighted_content, work)?,
        target_content_value_ids: ids(&spec.target_content, work)?,
        route_key_ordinals: copy(spec.route_keys.len(), spec.route_keys.iter().copied(), work)?,
        weight_value_id: Some(spec.weight.get()),
        file_value_id: Some(spec.target.file.get()),
        position_value_id: Some(spec.target.position.get()),
        spec_id_value_id: Some(spec.target.spec_id.get()),
        partition_value_id: Some(spec.target.partition.get()),
    })
}

pub(super) fn emit_physical(
    v: &wire::ContentApplyNode,
    work: &mut CompileCheckpoints<'_>,
) -> Result<p::ContentApplySpec, Error> {
    let [weight, file, position, spec_id, partition] = wire_fields(v, work)?.map(p::ValueId::new);
    let ids = |values: &[u32], work: &mut CompileCheckpoints<'_>| {
        let output = copy(
            values.len(),
            values.iter().copied().map(p::ValueId::new),
            work,
        )?;
        boxed(output, work)
    };
    Ok(p::ContentApplySpec {
        weighted_content: ids(&v.weighted_content_value_ids, work)?,
        target_content: ids(&v.target_content_value_ids, work)?,
        route_keys: boxed(
            copy(
                v.route_key_ordinals.len(),
                v.route_key_ordinals.iter().copied(),
                work,
            )?,
            work,
        )?,
        weight,
        target: p::ContentApplyTarget {
            file,
            position,
            spec_id,
            partition,
        },
    })
}
