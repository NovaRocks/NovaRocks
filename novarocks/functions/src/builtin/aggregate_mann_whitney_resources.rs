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
//! Checked facts for the original Mann-Whitney emission containers.
//! These stack facts neither run the statistic nor authorize an allocation.

use super::aggregate_mann_whitney_core::{MannResourceEvent, MannStateExtents};
use crate::arrow_result_custody::{
    FlatByteKind, FlatDirectByteFacts, RequestUpper, direct_flat_byte_facts,
};
use crate::scalar_copy_geometry::{
    ScalarCopyDiagnosticGeometry, arrow_default_debug_bytes_upper, scalar_copy_diagnostic_geometry,
};
use crate::scalar_resources::ScalarResourceError;
use crate::{KernelEvaluationControl, KernelFailure};
use arrow_array::ArrayRef;
use arrow_schema::DataType;
use novarocks_type_contract::{
    ControlResourceError, owned_resources::formatting::original_format_string_request_bound,
};
use std::alloc::Layout;

type Result<T> = std::result::Result<T, ScalarResourceError>;

fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or(ScalarResourceError::Arithmetic)
}
fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b).ok_or(ScalarResourceError::Arithmetic)
}
fn bytes<T>(count: usize) -> Result<usize> {
    Layout::array::<T>(count)
        .map(|layout| layout.size())
        .map_err(|_| ScalarResourceError::Arithmetic)
}
fn requests(a: RequestUpper, b: RequestUpper) -> Result<RequestUpper> {
    Ok(RequestUpper {
        bytes: add(a.bytes, b.bytes)?,
        requests: add(a.requests, b.requests)?,
    })
}
const ZERO_REQUESTS: RequestUpper = RequestUpper {
    bytes: 0,
    requests: 0,
};

/// Cumulative requests, simultaneous peak, and surviving stock are distinct.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ComponentFacts {
    pub(crate) requests: RequestUpper,
    pub(crate) peak: usize,
    pub(crate) retained: usize,
}
impl ComponentFacts {
    const ZERO: Self = Self {
        requests: ZERO_REQUESTS,
        peak: 0,
        retained: 0,
    };
    fn exact(bytes: usize) -> Self {
        Self {
            requests: RequestUpper {
                bytes,
                requests: usize::from(bytes != 0),
            },
            peak: bytes,
            retained: bytes,
        }
    }
    /// Mutually exclusive original branches; each coordinate is an upper bound.
    fn envelope(self, other: Self) -> Self {
        Self {
            requests: RequestUpper {
                bytes: self.requests.bytes.max(other.requests.bytes),
                requests: self.requests.requests.max(other.requests.requests),
            },
            peak: self.peak.max(other.peak),
            retained: self.retained.max(other.retained),
        }
    }
    /// Independent buffers remain live together, including their growth overlap.
    fn overlap(self, other: Self) -> Result<Self> {
        Ok(Self {
            requests: requests(self.requests, other.requests)?,
            peak: add(self.peak, other.peak)?,
            retained: add(self.retained, other.retained)?,
        })
    }
}

/// Original RawVec growth: a nonzero request at least doubles, except the
/// first minimum. Every request fits initial + 2*demand + minimum elements.
/// Exact no-growth cases preserve the always-created empty builder facts.
fn vector<T>(initial: usize, demand: usize, minimum: usize) -> Result<ComponentFacts> {
    if demand <= initial {
        return Ok(ComponentFacts::exact(bytes::<T>(initial)?));
    }
    let capacity_upper = add(add(initial, mul(2, demand)?)?, minimum)?;
    let request = bytes::<T>(capacity_upper)?;
    let count_upper = add(usize::BITS as usize, 2)?;
    Ok(ComponentFacts {
        requests: RequestUpper {
            bytes: mul(count_upper, request)?,
            requests: count_upper,
        },
        peak: mul(2, request)?,
        retained: request,
    })
}

fn format_string(initial: usize, output: usize) -> Result<ComponentFacts> {
    let facts =
        original_format_string_request_bound(initial, output).map_err(|error| match error {
            ControlResourceError::SourceModel(detail) => ScalarResourceError::SourceModel(detail),
            ControlResourceError::Control(_) => ScalarResourceError::Arithmetic,
        })?;
    let retained = if output <= initial {
        initial
    } else {
        initial.max(mul(2, output)?).max(8)
    };
    bytes::<u8>(retained)?;
    Ok(ComponentFacts {
        requests: RequestUpper {
            bytes: facts.request_bytes_upper_bound,
            requests: facts.allocation_requests_upper_bound,
        },
        peak: if output <= initial {
            initial
        } else {
            mul(2, retained)?
        },
        retained,
    })
}

/// The original header and fixed eight-byte appends force capacities
/// 8,16,...,q. Resource extents use actual usize counts, not truncated u32.
pub(crate) fn codec_facts(samples: usize) -> Result<(ComponentFacts, usize)> {
    let output = add(17, bytes::<f64>(samples)?)?;
    let capacity = output
        .checked_next_power_of_two()
        .ok_or(ScalarResourceError::Arithmetic)?;
    bytes::<u8>(capacity)?;
    Ok((
        ComponentFacts {
            requests: RequestUpper {
                bytes: mul(2, capacity)?
                    .checked_sub(8)
                    .ok_or(ScalarResourceError::Arithmetic)?,
                requests: add(1, (capacity / 8).ilog2() as usize)?,
            },
            peak: mul(3, capacity / 2)?,
            retained: capacity,
        },
        output,
    ))
}

fn sample_sorts(state: MannStateExtents) -> Result<ComponentFacts> {
    if state.sorted {
        return Ok(ComponentFacts::ZERO);
    }
    let left = bytes::<f64>(state.sample_lengths[0])?;
    let right = bytes::<f64>(state.sample_lengths[1])?;
    // The two original stable-sort scratch buffers are destroyed sequentially.
    Ok(ComponentFacts {
        requests: RequestUpper {
            bytes: add(left, right)?,
            requests: add(usize::from(left != 0), usize::from(right != 0))?,
        },
        peak: left.max(right),
        retained: 0,
    })
}

fn require_number_sources(lock: &str) -> Result<()> {
    for name in ["\nname = \"serde_json\"\n", "\nname = \"zmij\"\n"] {
        if lock.matches(name).count() != 1 {
            return Err(ScalarResourceError::SourceModel(
                "Mann-Whitney number formatter source model drift",
            ));
        }
    }
    for package in [
        "name = \"serde_json\"\nversion = \"1.0.150\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\nchecksum = \"e8014e44b4736ed0538adeecded0fce2a272f22dc9578a7eb6b2d9993c74cfb9\"",
        "name = \"zmij\"\nversion = \"1.0.23\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\nchecksum = \"29666d0abbfad1e3dc4dcf6144730dd3a3ab225bbbdac83319345b1b44ccfc1b\"",
    ] {
        if lock.matches(package).count() != 1 {
            return Err(ScalarResourceError::SourceModel(
                "Mann-Whitney number formatter source model drift",
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct MannSourceParams {
    renderer: ComponentFacts,
    buffer_alignment: usize,
}
impl MannSourceParams {
    pub(crate) fn try_new() -> Result<Self> {
        crate::scalar_resources::require_arrow_model()?;
        // The shared formatter also checks the exact Rust source profile.
        // Number covers both original serde cfg branches, not a cfg guessed
        // from this dependent crate. Reject a changed number-format author.
        require_number_sources(include_str!("../../../../Cargo.lock"))?;
        let sd = format_string(94, 374)?;
        let tuple = format_string(8, 52)?;
        // Two Number creations and Displays each request <=24. The first
        // returned String coexists with the second Number's two Strings.
        let tuple = ComponentFacts {
            requests: requests(
                RequestUpper {
                    bytes: 96,
                    requests: 4,
                },
                tuple.requests,
            )?,
            peak: 72.max(add(48, tuple.peak)?),
            retained: tuple.retained,
        };
        let renderer = sd.envelope(tuple).envelope(ComponentFacts::exact(55));
        Ok(Self {
            renderer,
            buffer_alignment: if cfg!(target_arch = "aarch64") {
                64
            } else {
                128
            },
        })
    }
}

pub(crate) fn default_byte_payload(
    rows: usize,
    data_bytes: usize,
    validity: bool,
    source: &MannSourceParams,
) -> Result<ComponentFacts> {
    let data = vector::<u8>(1024, data_bytes, 8)?;
    let offsets = vector::<i32>(1025, add(rows, 1)?, 4)?;
    let bitmap = if validity && rows != 0 {
        let ceil_bytes = add(rows / 8, usize::from(rows % 8 != 0))?;
        let rounded = mul(add(ceil_bytes / 64, usize::from(ceil_bytes % 64 != 0))?, 64)?;
        let capacity_upper = add(128, rounded)?;
        let retained = mul(2, capacity_upper)?;
        Layout::from_size_align(retained, source.buffer_alignment)
            .map_err(|_| ScalarResourceError::Arithmetic)?;
        ComponentFacts {
            requests: RequestUpper {
                bytes: mul(4, capacity_upper)?,
                requests: add(usize::BITS as usize, 2)?,
            },
            peak: mul(3, capacity_upper)?,
            retained,
        }
    } else {
        ComponentFacts::ZERO
    };
    data.overlap(offsets)?.overlap(bitmap)
}

fn source_failure(error: ScalarResourceError) -> KernelFailure {
    match error {
        ScalarResourceError::Arithmetic => KernelFailure::ResourceExhausted,
        ScalarResourceError::SourceModel(detail) => crate::kernel_control::invalid(detail),
        ScalarResourceError::Invariant(detail) => crate::kernel_control::internal(detail),
    }
}

fn static_diagnostic_facts(message: &'static str, replacement: bool) -> Result<ComponentFacts> {
    let original = ComponentFacts::exact(bytes::<u8>(message.len())?);
    replace_diagnostic(original, replacement)
}
fn replace_diagnostic(original: ComponentFacts, replacement: bool) -> Result<ComponentFacts> {
    if !replacement {
        return Ok(original);
    }
    // The original treatment replacement is one exact str::to_string birth.
    let replacement = ComponentFacts::exact(bytes::<u8>(56)?);
    Ok(ComponentFacts {
        requests: requests(original.requests, replacement.requests)?,
        peak: add(original.peak, replacement.peak)?,
        retained: replacement.retained,
    })
}

/// Generic default ToString starts empty and may write several Debug pieces.
/// This is not the nonzero estimated-capacity format! destination author.
fn fresh_display_string(output: usize) -> Result<ComponentFacts> {
    if output == 0 {
        return Ok(ComponentFacts::ZERO);
    }
    let capacity = mul(2, output)?.max(8);
    bytes::<u8>(capacity)?;
    let count = add(usize::BITS as usize, 2)?;
    Ok(ComponentFacts {
        requests: RequestUpper {
            bytes: mul(count, capacity)?,
            requests: count,
        },
        peak: mul(2, capacity)?,
        retained: capacity,
    })
}
/// ScalarStateError Display writes its whole message once into a fresh String.
fn one_write_display_string(output: usize) -> Result<ComponentFacts> {
    if output == 0 {
        return Ok(ComponentFacts::ZERO);
    }
    Ok(ComponentFacts::exact(bytes::<u8>(output.max(8))?))
}
fn first_alternative_facts(source_bytes: usize) -> Result<ComponentFacts> {
    let output = mul(12, source_bytes)?;
    let lowercase = if source_bytes == 0 {
        ComponentFacts::ZERO
    } else {
        vector::<u8>(source_bytes, output, 8)?
    };
    let invalid = format_string(76, add(38, output)?)?;
    Ok(ComponentFacts {
        requests: requests(lowercase.requests, invalid.requests)?,
        peak: lowercase.peak.max(add(lowercase.retained, invalid.peak)?),
        retained: invalid.retained,
    })
}
fn merge_scratch_facts(left: MannStateExtents, right: MannStateExtents) -> Result<ComponentFacts> {
    let left = sample_sorts(left)?;
    let right = sample_sorts(right)?;
    Ok(ComponentFacts {
        requests: requests(left.requests, right.requests)?,
        peak: left.peak.max(right.peak),
        retained: 0,
    })
}
fn int_setup_diagnostic_facts(
    data_type: &DataType,
    replacement: bool,
    control: &dyn KernelEvaluationControl,
) -> std::result::Result<ComponentFacts, KernelFailure> {
    let debug = arrow_default_debug_bytes_upper(data_type, control)?;
    let output = add(28, debug).map_err(source_failure)?;
    replace_diagnostic(
        format_string(56, output).map_err(source_failure)?,
        replacement,
    )
    .map_err(source_failure)
}
fn scalar_read_event_facts(
    array: &ArrayRef,
    row: usize,
    control: &dyn KernelEvaluationControl,
) -> std::result::Result<ComponentFacts, KernelFailure> {
    let geometry = scalar_copy_diagnostic_geometry(array, row, control)?;
    let (copy, diagnostic) = match geometry {
        ScalarCopyDiagnosticGeometry::Known { copy } => (copy, None),
        ScalarCopyDiagnosticGeometry::FailureOnly {
            copy,
            diagnostic_bytes_upper,
        } => (copy, Some(diagnostic_bytes_upper)),
    };
    let copy_requests = RequestUpper {
        bytes: copy.requested_bytes_upper,
        requests: copy.requests_upper,
    };
    let result = (|| {
        if let Some(output) = diagnostic {
            let first = fresh_display_string(output)?;
            let second = one_write_display_string(output)?;
            Ok(ComponentFacts {
                requests: requests(requests(copy_requests, first.requests)?, second.requests)?,
                peak: add(copy.peak_bytes_upper, add(first.peak, second.retained)?)?,
                retained: second.retained,
            })
        } else {
            let numeric = ComponentFacts::exact(bytes::<u8>(41)?);
            Ok(ComponentFacts {
                requests: requests(copy_requests, numeric.requests)?,
                peak: add(copy.peak_bytes_upper, numeric.peak)?,
                retained: numeric.retained,
            })
        }
    })();
    result.map_err(source_failure)
}
impl MannSourceParams {
    /// Only source-qualified parameters can project reached-event facts.
    /// The caller flushes pending core work before neutral geometry and borrows
    /// the SAME controller; Noop legacy calls never reach this dispatcher.
    pub(crate) fn event_facts(
        &self,
        event: MannResourceEvent<'_>,
        control: &dyn KernelEvaluationControl,
    ) -> std::result::Result<ComponentFacts, KernelFailure> {
        match event {
            MannResourceEvent::StaticDiagnostic {
                message,
                treatment_replacement,
            } => static_diagnostic_facts(message, treatment_replacement).map_err(source_failure),
            MannResourceEvent::IntSetupDiagnostic {
                data_type,
                treatment_replacement,
            } => int_setup_diagnostic_facts(data_type, treatment_replacement, control),
            MannResourceEvent::FirstAlternative { source } => {
                first_alternative_facts(source.len()).map_err(source_failure)
            }
            MannResourceEvent::ScalarRead { array, row } => {
                scalar_read_event_facts(array, row, control)
            }
            MannResourceEvent::MergeScratch { left, right } => {
                merge_scratch_facts(left, right).map_err(source_failure)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RowFacts {
    requests: RequestUpper,
    peak: usize,
    returned: usize,
    output: usize,
}
fn row_facts(
    kind: FlatByteKind,
    state: MannStateExtents,
    source: &MannSourceParams,
) -> Result<RowFacts> {
    let count = add(state.sample_lengths[0], state.sample_lengths[1])?;
    if kind == FlatByteKind::Utf8 && (!state.initialized || count == 0) {
        return Ok(RowFacts {
            requests: ZERO_REQUESTS,
            peak: 0,
            returned: 0,
            output: 0,
        });
    }
    let sorts = sample_sorts(state)?;
    if kind == FlatByteKind::Binary {
        let (codec, output) = codec_facts(count)?;
        return Ok(RowFacts {
            requests: requests(sorts.requests, codec.requests)?,
            peak: sorts.peak.max(codec.peak),
            returned: codec.retained,
            output,
        });
    }
    // Preserve the original float addition/cast, separately from real pushes.
    let initial = ((state.sample_lengths[0] as f64) + (state.sample_lengths[1] as f64)) as usize;
    let combined = vector::<(f64, usize)>(initial, count, 4)?;
    let scratch = bytes::<(f64, usize)>(count)?;
    let scratch_requests = RequestUpper {
        bytes: scratch,
        requests: usize::from(scratch != 0),
    };
    Ok(RowFacts {
        requests: requests(
            requests(
                requests(sorts.requests, combined.requests)?,
                scratch_requests,
            )?,
            source.renderer.requests,
        )?,
        peak: sorts
            .peak
            .max(combined.peak)
            .max(add(combined.retained, scratch)?)
            .max(add(combined.retained, source.renderer.peak)?),
        returned: source.renderer.retained,
        output: 374,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MannEmissionFacts {
    pub(crate) payload: ComponentFacts,
    pub(crate) row_requests: RequestUpper,
    pub(crate) row_peak: usize,
    pub(crate) row_returned_stock: usize,
    pub(crate) finish_and_custody: FlatDirectByteFacts,
    pub(crate) body_checkpoint_clone: RequestUpper,
    pub(crate) external_failure_stock_upper: usize,
    pub(crate) operation_global_requests: RequestUpper,
    pub(crate) operation_global_peak: usize,
    pub(crate) global_output_retained: usize,
    // Existing custody charges a conservative metadata invoice in addition to
    // the original carrier stock; this is distinct from physical backing.
    pub(crate) global_settlement_charge_upper: usize,
    pub(crate) typed_custody_peak: usize,
    pub(crate) typed_output_retained: usize,
}

/// A fixed stack fold over the actual emission order. No cache of states or
/// sample values is created and no absent group is guessed.
pub(crate) struct MannEmissionResources {
    kind: FlatByteKind,
    rows: usize,
    seen: usize,
    output: usize,
    row_requests: RequestUpper,
    row_peak: usize,
    row_returned: usize,
}
impl MannEmissionResources {
    pub(crate) fn new(kind: FlatByteKind, rows: usize) -> Self {
        Self {
            kind,
            rows,
            seen: 0,
            output: 0,
            row_requests: ZERO_REQUESTS,
            row_peak: 0,
            row_returned: 0,
        }
    }
    pub(crate) fn add_state(
        &mut self,
        state: MannStateExtents,
        source: &MannSourceParams,
    ) -> Result<()> {
        if self.seen >= self.rows {
            return Err(ScalarResourceError::Invariant(
                "Mann-Whitney emission has more states than output rows",
            ));
        }
        let row = row_facts(self.kind, state, source)?;
        self.output = add(self.output, row.output)?;
        self.row_requests = requests(self.row_requests, row.requests)?;
        self.row_peak = self.row_peak.max(row.peak);
        self.row_returned = self.row_returned.max(row.returned);
        self.seen = add(self.seen, 1)?;
        Ok(())
    }
    pub(crate) fn finish(self, source: &MannSourceParams) -> Result<MannEmissionFacts> {
        if self.seen != self.rows {
            return Err(ScalarResourceError::Invariant(
                "Mann-Whitney emission is missing output states",
            ));
        }
        let validity = self.kind == FlatByteKind::Utf8 && self.rows != 0;
        let payload = default_byte_payload(self.rows, self.output, validity, source)?;
        let flat = direct_flat_byte_facts(self.kind, validity)?;
        let peak = add(payload.peak, self.row_peak)?
            .max(add(payload.retained, flat.finish_global_peak_extra)?)
            .max(add(
                payload.retained,
                add(flat.custody_global_peak_extra, bytes::<i32>(4)?)?,
            )?);
        let diagnostic = crate::MAX_ROW_ERROR_MESSAGE_BYTES;
        let settlement = add(
            add(payload.retained, flat.original_metadata_stock_upper)?,
            flat.settlement_metadata_upper,
        )?;
        let clone = RequestUpper {
            bytes: diagnostic,
            requests: 1,
        };
        Ok(MannEmissionFacts {
            payload,
            row_requests: self.row_requests,
            row_peak: self.row_peak,
            row_returned_stock: self.row_returned,
            finish_and_custody: flat,
            body_checkpoint_clone: clone,
            external_failure_stock_upper: diagnostic,
            operation_global_requests: requests(
                requests(
                    requests(
                        requests(payload.requests, self.row_requests)?,
                        flat.finish_global,
                    )?,
                    flat.custody_global,
                )?,
                clone,
            )?,
            operation_global_peak: add(peak.max(settlement), mul(2, diagnostic)?)?,
            global_output_retained: add(payload.retained, flat.global_retained_metadata_upper)?,
            global_settlement_charge_upper: settlement,
            typed_custody_peak: flat.custody_typed_peak_extra,
            typed_output_retained: flat.typed_retained_metadata_upper,
        })
    }
}

#[cfg(test)]
mod tests;
