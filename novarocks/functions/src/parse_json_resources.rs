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

//! Checked pre-operation facts for the original serde Value/canonical writer.
//! Facts grant no authority: the pure adapter must reserve on its actual host.
//! Pinned authors: Rust1.98.1, serde_json1.0.150 default/std/raw_value,
//! serde_core1.0.228 and Arrow58.4.0. Dependency/feature changes require review.

use crate::{KernelFailure, arrow_result_custody::custody_type_metadata_upper_bound};
use serde_json::Value;
use std::alloc::Layout;

fn add(a: usize, b: usize) -> Result<usize, KernelFailure> {
    a.checked_add(b).ok_or(KernelFailure::ResourceExhausted)
}
fn mul(a: usize, b: usize) -> Result<usize, KernelFailure> {
    a.checked_mul(b).ok_or(KernelFailure::ResourceExhausted)
}
// RawVec::grow_amortized: max(2*old, required, minimum). Twice the bound
// covers original and replacement buffers coexisting during reallocation.
fn capacity(n: usize, initial: usize, minimum: usize) -> Result<usize, KernelFailure> {
    add(add(initial, mul(n, 2)?)?, minimum)
}
fn byte_peak(n: usize, initial: usize) -> Result<usize, KernelFailure> {
    mul(capacity(n, initial, 8)?, 2)
}
// Pinned rustc_abi univariant: align before each field, add field size,
// round the final size. Charge every possible alignment gap regardless of
// field order. This does not assume repr(Rust) uses declaration order.
fn field_envelope(fields: &[Layout], alignment: usize) -> Result<Layout, KernelFailure> {
    let mut bytes = 0;
    let mut alignment = alignment;
    for field in fields {
        bytes = add(bytes, add(field.size(), field.align() - 1)?)?;
        alignment = alignment.max(field.align());
    }
    bytes = add(bytes, alignment - 1)?;
    Layout::from_size_align(bytes, alignment)
        .map(|layout| layout.pad_to_align())
        .map_err(|_| KernelFailure::ResourceExhausted)
}
fn btree_node_bytes() -> Result<usize, KernelFailure> {
    // B=6: parent pointer, two u16 fields, eleven keys/values, twelve edges.
    // Bound the leaf first, then the repr(C) internal node's separate fields.
    let leaf = field_envelope(
        &[
            Layout::new::<Option<std::ptr::NonNull<()>>>(),
            Layout::new::<u16>(),
            Layout::new::<u16>(),
            Layout::array::<String>(11).map_err(|_| KernelFailure::ResourceExhausted)?,
            Layout::array::<Value>(11).map_err(|_| KernelFailure::ResourceExhausted)?,
        ],
        align_of::<(usize, u16, u16, [String; 11], [Value; 11])>(),
    )?;
    let edges =
        Layout::array::<std::ptr::NonNull<()>>(12).map_err(|_| KernelFailure::ResourceExhausted)?;
    Ok(field_envelope(&[leaf, edges], leaf.align())?.size())
}
fn error_impl_bytes() -> Result<usize, KernelFailure> {
    // ErrorCode has fewer than256 variants, no explicit repr/discriminants.
    // Pinned rustc uses I8 or for_align(first_field_align); for_align returns
    // only an integer whose SIZE EQUALS that alignment. Niche packing only
    // reduces this bound. Sum both variant payloads and every padding gap.
    let alignment = align_of::<(Box<str>, std::io::Error)>();
    let tag = Layout::from_size_align(alignment, alignment)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    let code = field_envelope(
        &[
            tag,
            Layout::new::<Box<str>>(),
            Layout::new::<std::io::Error>(),
        ],
        alignment,
    )?;
    Ok(field_envelope(
        &[code, Layout::new::<usize>(), Layout::new::<usize>()],
        align_of::<(Box<str>, std::io::Error, usize)>(),
    )?
    .size())
}
fn diagnostic_peak() -> Result<usize, KernelFailure> {
    let syntax = "control character (\\u0000-\\u001F) found while parsing a string".len();
    // Value accepts strings. Only non-string input at the raw marker can
    // reach invalid_type; Unexpected::Str/Bytes/Char/Io are unreachable here.
    let unexpected = [
        "boolean ``".len() + "false".len(),
        "integer ``".len() + 20,
        "floating point ``".len() + 24,
        "null".len(),
        "sequence".len(),
        "map".len(),
    ]
    .into_iter()
    .max()
    .expect("known original diagnostic alternatives");
    let invalid =
        "invalid value: ".len() + unexpected + ", expected ".len() + "any valid JSON value".len();
    let position = " at line ".len() + " column ".len() + 2 * (usize::MAX.ilog10() as usize + 1);
    let message = add(syntax.max(invalid), position)?;
    let prefix = "parse_json stringify key failed: ".len();
    let formatted = add(message, prefix)?;
    // format! may initially allocate twice its literal pieces. These exact
    // templates cover invalid-type/value and the original three wrappers.
    let initial = 2 * prefix.max("invalid value: , expected ".len());
    // Raw custom rewrapping strips/reinstalls one location suffix. Two error
    // boxes and three message buffers cover growth/shrink/old-error overlap.
    add(
        mul(error_impl_bytes()?, 2)?,
        add(message, byte_peak(formatted, initial)?)?,
    )
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ParseJsonRowResources {
    pub(crate) canonical_bytes: usize,
    pub(crate) transient_bytes: usize,
}
impl ParseJsonRowResources {
    pub(crate) fn from_raw_bytes(bytes: usize) -> Result<Self, KernelFailure> {
        let nodes = add(bytes, 1)?;
        // Each raw reentry consumes a distinct marker's source characters;
        // escape decoding cannot duplicate those characters. Parent scratch,
        // partial trees and owned raw strings remain live in recursive calls.
        let parses = add(bytes / "$serde_json::private::RawValue".len(), 1)?;
        let parsed_nodes = mul(nodes, parses)?;
        // Array capacity <=2*elements+4*arrays; old/new overlap gives12 slots.
        let arrays = mul(mul(parsed_nodes, 12)?, size_of::<Value>())?;
        // Key tickets bound valid nodes; roots and the split frontier each
        // need at most one extra node per ticket, plus a new root/split pair.
        let maps = mul(add(mul(parsed_nodes, 3)?, 2)?, btree_node_bytes()?)?;
        let strings = parsed_nodes;
        let raw_boxed = mul(mul(bytes, parses - 1)?, 2)?;
        // One scratch Vec per parser, four-byte maximum Unicode code point,
        // byte minimum eight and original/replacement overlap.
        let scratch = mul(add(mul(parsed_nodes, 8)?, mul(parses, 8)?)?, 2)?;
        // Returned raw subtrees replace encoded marker strings; final tokens
        // and decoded text do not multiply. Escapes6, numbers24, syntax8.
        let canonical_bytes = add(mul(bytes, 6)?, mul(nodes, 32)?)?;
        let keys = mul(mul(nodes, 12)?, size_of::<&String>())?;
        // Ancestor escaped keys survive child recursion. Each serde stringify
        // writer starts at128; cover all live strings, quotes and old/new.
        let live = add(nodes, 1)?;
        let escaped_len = add(mul(bytes, 6)?, mul(live, 2)?)?;
        let escaped = mul(add(mul(live, 128 + 8)?, mul(escaped_len, 2)?)?, 2)?;
        let mut transient_bytes = 0;
        for part in [
            arrays,
            maps,
            strings,
            raw_boxed,
            scratch,
            keys,
            escaped,
            byte_peak(24, 0)?,
            byte_peak(canonical_bytes, 0)?,
            diagnostic_peak()?,
        ] {
            transient_bytes = add(transient_bytes, part)?;
        }
        Ok(Self {
            canonical_bytes,
            transient_bytes,
        })
    }
}

/// Fold only demanded non-NULL raw lengths, without parsing/storing values.
#[derive(Default, Debug)]
pub(crate) struct ParseJsonBatchResources {
    output_bytes: usize,
    row_peak: usize,
}
impl ParseJsonBatchResources {
    pub(crate) fn add_row(&mut self, raw_bytes: usize) -> Result<(), KernelFailure> {
        let row = ParseJsonRowResources::from_raw_bytes(raw_bytes)?;
        self.output_bytes = add(self.output_bytes, row.canonical_bytes)?;
        self.row_peak = self.row_peak.max(row.transient_bytes);
        Ok(())
    }
    pub(crate) fn peak(&self, selected_rows: usize) -> Result<usize, KernelFailure> {
        // Original StringBuilder::new: data1024/offsets1025. finish moves
        // backing and starts a new four-element i32 offset Vec.
        let data = byte_peak(self.output_bytes, 1024)?;
        let offsets = mul(
            capacity(add(selected_rows, 1)?, 1025, 4)?,
            2 * size_of::<i32>(),
        )?;
        // Lazy validity uses MutableBuffer's64-byte rounding and doubling.
        // Admit it for empty/all-NULL batches as well, with old/new overlap.
        let validity_len = add(selected_rows, 7)? / 8;
        let validity = mul(add(128, mul(add(validity_len, 63)?, 2)?)?, 2)?;
        // One EvaluationCheckpoints refuses once then exits: the original
        // bounded KernelDiagnostic and its one latched Box<str> clone coexist.
        // No KernelControlObservation (pthread Mutex/extra clones) is created.
        let mut peak = add(self.row_peak, mul(crate::MAX_ROW_ERROR_MESSAGE_BYTES, 2)?)?;
        for part in [
            data,
            offsets,
            validity,
            4 * size_of::<i32>(),
            custody_type_metadata_upper_bound(1)?,
        ] {
            peak = add(peak, part)?;
        }
        Ok(peak)
    }
}
