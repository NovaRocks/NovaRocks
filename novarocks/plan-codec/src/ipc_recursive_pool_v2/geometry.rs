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

//! Borrowed recursive spans; no sliced ArrayData, descriptor Vec, or body allocation.
use super::RecursivePoolWriteLimits;
use crate::{
    ipc_flat_pool_v2::geometry::{self, add, aligned, bit_bytes, mul, native_offset},
    physical_type_v2::TypeCodecError,
};
use arrow::{array::ArrayData, datatypes::DataType};
use novarocks_type_contract::{CompileCheckpoints, MAX_CANONICAL_ARROW_TYPE_PATH_DEPTH};

pub(super) fn invalid() -> TypeCodecError {
    TypeCodecError::InvalidShape("recursive pool writer geometry is not representable")
}

#[derive(Clone, Copy)]
pub(super) struct Span<'a> {
    pub data: &'a ArrayData,
    pub start: usize,
    pub len: usize,
}

pub(super) struct Node<'a> {
    pub span: Span<'a>,
    pub leaf: Option<geometry::Geometry>,
    pub offset: Option<(usize, usize, usize)>,
    pub buffers: usize,
    pub body_bytes: usize,
    pub payload_bytes: usize,
    pub null_count: usize,
}

#[derive(Clone, Copy, Default)]
pub(super) struct Geometry {
    pub rows: usize,
    pub nodes: usize,
    pub total_rows: usize,
    pub buffers: usize,
    pub variadic: usize,
    pub view_fields: usize,
    pub body_bytes: usize,
    pub payload_bytes: usize,
}

// This type-only pass cannot omit an unsupported child because its selected
// parent is empty or NULL. The checked ConstantPool already owns grammar bounds.
fn profile(
    ty: &DataType,
    depth: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), TypeCodecError> {
    if depth > MAX_CANONICAL_ARROW_TYPE_PATH_DEPTH {
        return Err(invalid());
    }
    work.step()?;
    match ty {
        DataType::Struct(fields) => {
            for field in fields {
                profile(field.data_type(), depth + 1, work)?;
                work.step()?;
            }
        }
        DataType::List(field) | DataType::LargeList(field) | DataType::Map(field, _) => {
            profile(field.data_type(), depth + 1, work)?;
            work.step()?;
        }
        // The sole flat profile rejects every encoded or unsupported container.
        other => {
            crate::ipc_flat_batch_v2::layout(other)?;
            work.step()?;
        }
    }
    Ok(())
}

use crate::host_projection_v2::ProjectionFailure;
type HostError<H> = ProjectionFailure<TypeCodecError, H>;
type NodeCapture<'node, 'callback, H> =
    dyn FnMut(&Node<'node>, bool) -> Result<(), HostError<H>> + 'callback;
type GeometryCapture<'callback, H> = dyn FnMut(&Geometry) -> Result<(), HostError<H>> + 'callback;

pub(super) fn node_core<'a, H>(
    span: Span<'a>,
    count_nulls: bool,
    capture: &mut Option<&mut NodeCapture<'a, '_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Node<'a>, HostError<H>> {
    let policy = crate::ipc_flat_pool_v2::WriterAdmissionPolicy(capture.is_some());
    let add = |a, b| policy.numeric(add(a, b));
    let mul = |a, b| policy.numeric(mul(a, b));
    let aligned = |a| policy.numeric(aligned(a));
    let bit_bytes = |a| policy.numeric(bit_bytes(a));
    if add(span.start, span.len)? > span.data.len() || i64::try_from(span.len).is_err() {
        return Err((invalid()).into());
    }
    work.step()?;
    let null_count = if !count_nulls {
        0
    } else if matches!(span.data.data_type(), DataType::Null) {
        span.len
    } else if span.start == 0 && span.len == span.data.len() {
        span.data.null_count()
    } else if let Some(nulls) = span.data.nulls() {
        let mut count = 0;
        for row in span.start..add(span.start, span.len)? {
            count = add(count, usize::from(nulls.is_null(row)))?;
            work.step()?;
        }
        count
    } else {
        0
    };
    work.step()?;
    let mut result = Node {
        span,
        leaf: None,
        offset: None,
        buffers: 0,
        body_bytes: 0,
        payload_bytes: 0,
        null_count,
    };
    match span.data.data_type() {
        DataType::Struct(_) => {
            let bytes = bit_bytes(span.len)?;
            result.buffers = 1;
            result.body_bytes = aligned(bytes)?;
            result.payload_bytes = bytes;
        }
        DataType::List(_) | DataType::Map(_, _) | DataType::LargeList(_) => {
            let width = if matches!(span.data.data_type(), DataType::LargeList(_)) {
                8
            } else {
                4
            };
            let (base, end) = if span.len == 0 {
                (0, 0)
            } else {
                let source = span.data.buffers().first().ok_or_else(invalid)?.as_slice();
                let start = add(span.data.offset(), span.start)?;
                (
                    native_offset(source, start, width)?,
                    native_offset(source, add(start, span.len)?, width)?,
                )
            };
            let extent = end.checked_sub(base).ok_or_else(invalid)?;
            let child = span.data.child_data().first().ok_or_else(invalid)?;
            if end > child.len() {
                return Err((invalid()).into());
            }
            let validity = bit_bytes(span.len)?;
            let offsets = mul(add(span.len, 1)?, width)?;
            result.buffers = 2;
            result.body_bytes = add(aligned(validity)?, aligned(offsets)?)?;
            result.payload_bytes = add(validity, offsets)?;
            result.offset = Some((width, base, extent));
        }
        _ => {
            let leaf = if let Some(capture) = capture.as_mut() {
                geometry::inspect_span_pure(
                    span.data,
                    span.start,
                    span.len,
                    Some(&mut |leaf| {
                        let partial = Node {
                            span,
                            leaf: Some(*leaf),
                            offset: None,
                            buffers: leaf.buffers,
                            body_bytes: leaf.body_bytes,
                            payload_bytes: leaf.payload_bytes,
                            null_count,
                        };
                        capture(&partial, false)
                    }),
                    work,
                )?
            } else {
                geometry::inspect_span_pure::<std::convert::Infallible>(
                    span.data, span.start, span.len, None, work,
                )
                .map_err(ProjectionFailure::without_host)?
            };
            result.buffers = leaf.buffers;
            result.body_bytes = leaf.body_bytes;
            result.payload_bytes = leaf.payload_bytes;
            result.leaf = Some(leaf);
        }
    }
    if let Some(capture) = capture.as_mut() {
        capture(&result, true)?;
    }
    work.step()?;
    Ok(result)
}

fn child<'a>(node: &Node<'a>, index: usize) -> Result<Span<'a>, TypeCodecError> {
    let data = node.span.data.child_data().get(index).ok_or_else(invalid)?;
    if let Some((_, base, extent)) = node.offset {
        if index != 0 {
            return Err(invalid());
        }
        Ok(Span {
            data,
            start: base,
            len: extent,
        })
    } else {
        // Canonical Struct ArrayData already carries independently sliced
        // children. A further span starts at the same child-relative row.
        Ok(Span {
            data,
            start: node.span.start,
            len: node.span.len,
        })
    }
}

pub(super) fn walk<'a>(
    span: Span<'a>,
    reverse: bool,
    count_nulls: bool,
    depth: usize,
    work: &mut CompileCheckpoints<'_>,
    visit: &mut impl FnMut(&Node<'a>, &mut CompileCheckpoints<'_>) -> Result<(), TypeCodecError>,
) -> Result<(), TypeCodecError> {
    walk_core::<std::convert::Infallible>(
        span,
        reverse,
        count_nulls,
        depth,
        &mut None,
        work,
        &mut |node, work| visit(node, work).map_err(HostError::<std::convert::Infallible>::from),
    )
    .map_err(ProjectionFailure::without_host)
}

pub(super) fn walk_core<'a, H>(
    span: Span<'a>,
    reverse: bool,
    count_nulls: bool,
    depth: usize,
    capture: &mut Option<&mut NodeCapture<'a, '_, H>>,
    work: &mut CompileCheckpoints<'_>,
    visit: &mut impl FnMut(&Node<'a>, &mut CompileCheckpoints<'_>) -> Result<(), HostError<H>>,
) -> Result<(), HostError<H>> {
    if depth > MAX_CANONICAL_ARROW_TYPE_PATH_DEPTH {
        return Err((invalid()).into());
    }
    let current = node_core(span, count_nulls, capture, work)?;
    let children = match span.data.data_type() {
        DataType::Struct(fields) => fields.len(),
        DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) => 1,
        _ => 0,
    };
    if children != span.data.child_data().len() {
        return Err((invalid()).into());
    }
    work.step()?;
    if reverse {
        for index in (0..children).rev() {
            walk_core(
                child(&current, index)?,
                true,
                count_nulls,
                depth + 1,
                capture,
                work,
                visit,
            )?;
            work.step()?;
        }
        visit(&current, work)?;
    } else {
        visit(&current, work)?;
        for index in 0..children {
            walk_core(
                child(&current, index)?,
                false,
                count_nulls,
                depth + 1,
                capture,
                work,
                visit,
            )?;
            work.step()?;
        }
    }
    Ok(())
}

pub(super) fn inspect(
    data: &ArrayData,
    limits: RecursivePoolWriteLimits,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Geometry, TypeCodecError> {
    inspect_core::<std::convert::Infallible>(data, limits, None, work)
        .map_err(ProjectionFailure::without_host)
}

pub(super) fn inspect_with_host_in<H>(
    data: &ArrayData,
    limits: RecursivePoolWriteLimits,
    capture: &mut dyn FnMut(&Geometry) -> Result<(), HostError<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Geometry, HostError<H>> {
    inspect_core(data, limits, Some(capture), work)
}

pub(super) fn inspect_core<H>(
    data: &ArrayData,
    limits: RecursivePoolWriteLimits,
    capture: Option<&mut GeometryCapture<'_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Geometry, HostError<H>> {
    inspect_policy_core(data, Some(limits), capture, work)
}
pub(super) fn inspect_pure(
    data: &ArrayData,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Geometry, TypeCodecError> {
    inspect_policy_core::<std::convert::Infallible>(data, None, None, work)
        .map_err(ProjectionFailure::without_host)
}
fn inspect_policy_core<H>(
    data: &ArrayData,
    receiving: Option<RecursivePoolWriteLimits>,
    mut capture: Option<&mut GeometryCapture<'_, H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Geometry, HostError<H>> {
    let policy = crate::ipc_flat_pool_v2::WriterAdmissionPolicy(capture.is_some());
    let add = |a, b| policy.numeric(add(a, b));
    if receiving.is_some_and(|limits| data.len() > limits.flat.max_rows) {
        return Err((if policy.0 {
            novarocks_type_contract::CompileControlError::ResourceExhausted.into()
        } else {
            invalid()
        })
        .into());
    }
    work.step()?;
    profile(data.data_type(), 1, work)?;
    let mut result = Geometry {
        rows: data.len(),
        ..Default::default()
    };
    let observed = capture.is_some();
    let mut committed = result;
    let mut summarize = |node: &Node<'_>,
                         complete: bool,
                         mut step: Option<&mut CompileCheckpoints<'_>>|
     -> Result<(), HostError<H>> {
        result = committed;
        result.nodes = add(result.nodes, 1)?;
        result.total_rows = add(result.total_rows, node.span.len)?;
        result.buffers = add(result.buffers, node.buffers)?;
        result.body_bytes = add(result.body_bytes, node.body_bytes)?;
        result.payload_bytes = add(result.payload_bytes, node.payload_bytes)?;
        if let Some(leaf) = &node.leaf {
            result.variadic = add(result.variadic, leaf.variadic)?;
            result.view_fields = add(result.view_fields, usize::from(leaf.views))?;
        }

        if let Some(work) = step.as_deref_mut() {
            work.step()?;
        }
        if receiving.is_some_and(|limits| {
            result.nodes > limits.max_field_nodes
                || result.total_rows > limits.max_total_rows
                || result.buffers > limits.flat.max_buffer_descriptors
                || result.body_bytes > limits.flat.max_body_bytes
        }) || i64::try_from(result.body_bytes).is_err()
            || u32::try_from(result.nodes).is_err()
            || u32::try_from(result.buffers).is_err()
        {
            return Err((if policy.0 {
                novarocks_type_contract::CompileControlError::ResourceExhausted.into()
            } else {
                invalid()
            })
            .into());
        }
        if let Some(capture) = capture.as_deref_mut() {
            capture(&result)?;
        }
        if complete {
            committed = result;
        }
        Ok(())
    };
    if observed {
        walk_core(
            Span {
                data,
                start: 0,
                len: data.len(),
            },
            false,
            false,
            1,
            &mut Some(&mut |node, complete| summarize(node, complete, None)),
            work,
            &mut |_, work| {
                work.step()?;
                Ok(())
            },
        )?;
    } else {
        walk_core(
            Span {
                data,
                start: 0,
                len: data.len(),
            },
            false,
            false,
            1,
            &mut None,
            work,
            &mut |node, work| summarize(node, true, Some(work)),
        )?;
    }
    Ok(result)
}
