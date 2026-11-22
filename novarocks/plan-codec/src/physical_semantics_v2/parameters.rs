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

//! Exact parameter DTO projection after carrier admission. The caller owns
//! entry/tail checkpoints; this leaf observes each bounded item. It does not
//! establish raw-byte first-allocation bounds or host allocation authority.

use super::{SemanticsCodecError, required_id};
use novarocks_proto_models::physical_semantics_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, MAX_SEMANTIC_PARAMETERS, SemanticParameterError, SemanticParameterId,
    SemanticParameterKey, SemanticParameterRef, SemanticParameterValue, SemanticParameters,
    StatementStartUtc, TimeZoneKey,
};

pub(super) fn encode_key(key: SemanticParameterKey) -> i32 {
    (match key {
        SemanticParameterKey::StatementStartUtc => wire::SemanticParameterKey::StatementStartUtc,
        SemanticParameterKey::TimeZone => wire::SemanticParameterKey::TimeZone,
        SemanticParameterKey::AllowThrowException => {
            wire::SemanticParameterKey::AllowThrowException
        }
        SemanticParameterKey::DecimalOverflowToDouble => {
            wire::SemanticParameterKey::DecimalOverflowToDouble
        }
        SemanticParameterKey::GroupConcatLegacy => wire::SemanticParameterKey::GroupConcatLegacy,
        SemanticParameterKey::GroupConcatMaxLen => wire::SemanticParameterKey::GroupConcatMaxLen,
    }) as i32
}

pub(super) fn decode_key(key: i32) -> Result<SemanticParameterKey, SemanticsCodecError> {
    match wire::SemanticParameterKey::try_from(key) {
        Ok(wire::SemanticParameterKey::StatementStartUtc) => {
            Ok(SemanticParameterKey::StatementStartUtc)
        }
        Ok(wire::SemanticParameterKey::TimeZone) => Ok(SemanticParameterKey::TimeZone),
        Ok(wire::SemanticParameterKey::AllowThrowException) => {
            Ok(SemanticParameterKey::AllowThrowException)
        }
        Ok(wire::SemanticParameterKey::DecimalOverflowToDouble) => {
            Ok(SemanticParameterKey::DecimalOverflowToDouble)
        }
        Ok(wire::SemanticParameterKey::GroupConcatLegacy) => {
            Ok(SemanticParameterKey::GroupConcatLegacy)
        }
        Ok(wire::SemanticParameterKey::GroupConcatMaxLen) => {
            Ok(SemanticParameterKey::GroupConcatMaxLen)
        }
        _ => Err(SemanticsCodecError::InvalidShape(
            "unknown or unspecified semantic parameter key",
        )),
    }
}

pub fn encode_reference(
    reference: &SemanticParameterRef,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::SemanticParameterRef, SemanticsCodecError> {
    work.step()?;
    Ok(wire::SemanticParameterRef {
        id: Some(reference.id.get()),
        expected_key: encode_key(reference.expected_key),
    })
}

pub fn decode_reference(
    reference: &wire::SemanticParameterRef,
    work: &mut CompileCheckpoints<'_>,
) -> Result<SemanticParameterRef, SemanticsCodecError> {
    work.step()?;
    Ok(SemanticParameterRef {
        id: SemanticParameterId::new(required_id(
            reference.id,
            "semantic parameter reference is missing its ID",
        )?),
        expected_key: decode_key(reference.expected_key)?,
    })
}

pub(super) fn encode_parameters(
    parameters: &SemanticParameters,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::SemanticParameters, SemanticsCodecError> {
    if parameters.entries().len() > MAX_SEMANTIC_PARAMETERS {
        return Err(SemanticsCodecError::from(
            SemanticParameterError::TooManyParameters,
        ));
    }
    emit_parameter_entries(
        parameters,
        Vec::with_capacity(parameters.entries().len()),
        work,
        false,
    )
}

/// Same wire grammar as the legacy component, with a preadmitted fallible Vec
/// and original checkpoints around the actual iterator/string copy exits.
pub(super) fn encode_parameters_observed(
    parameters: &SemanticParameters,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::SemanticParameters, SemanticsCodecError> {
    work.flush()?;
    let mut entries = Vec::new();
    let request = entries.try_reserve_exact(parameters.entries().len());
    crate::allocation_exit_v2::reserve_exit::<SemanticsCodecError>(request, work)?;
    emit_parameter_entries(parameters, entries, work, true)
}

fn emit_parameter_entries(
    parameters: &SemanticParameters,
    mut entries: Vec<wire::SemanticParameter>,
    work: &mut CompileCheckpoints<'_>,
    observed: bool,
) -> Result<wire::SemanticParameters, SemanticsCodecError> {
    let mut input = parameters.entries().iter();
    loop {
        if observed {
            work.flush()?;
        }
        let next = input.next();
        if observed {
            work.step()?;
            work.flush()?;
        }
        let Some((id, value)) = next else {
            break;
        };
        // A single item includes at most 255 bytes of owned text and bounded
        // table work. Do not let a future unchecked text source expand it.
        work.step()?;
        let value = match value {
            SemanticParameterValue::StatementStartUtc(value) => {
                wire::semantic_parameter::Value::StatementStartUtc(wire::StatementStartUtc {
                    seconds: Some(value.seconds()),
                    nanos: Some(value.nanos()),
                })
            }
            SemanticParameterValue::TimeZone(value) => {
                if observed {
                    work.flush()?;
                }
                let name = value.name().to_owned();
                if observed {
                    work.step()?;
                    work.flush()?;
                }
                wire::semantic_parameter::Value::TimeZone(name)
            }
            SemanticParameterValue::AllowThrowException(value) => {
                wire::semantic_parameter::Value::AllowThrowException(*value)
            }
            SemanticParameterValue::DecimalOverflowToDouble(value) => {
                wire::semantic_parameter::Value::DecimalOverflowToDouble(*value)
            }
            SemanticParameterValue::GroupConcatLegacy(value) => {
                wire::semantic_parameter::Value::GroupConcatLegacy(*value)
            }
            SemanticParameterValue::GroupConcatMaxLen(value) => {
                wire::semantic_parameter::Value::GroupConcatMaxLen(*value)
            }
        };
        entries.push(wire::SemanticParameter {
            id: id.get(),
            value: Some(value),
        });
    }
    Ok(wire::SemanticParameters { entries })
}

fn decode_value(
    value: Option<&wire::semantic_parameter::Value>,
) -> Result<SemanticParameterValue, SemanticsCodecError> {
    use wire::semantic_parameter::Value;
    match value {
        Some(Value::StatementStartUtc(value)) => {
            let start = value
                .seconds
                .zip(value.nanos)
                .and_then(|(seconds, nanos)| StatementStartUtc::try_new(seconds, nanos))
                .ok_or(SemanticsCodecError::InvalidShape(
                    "statement start must contain UTC seconds and normalized nanoseconds",
                ))?;
            Ok(SemanticParameterValue::StatementStartUtc(start))
        }
        Some(Value::TimeZone(value)) => {
            if value.len() > 255 {
                return Err(SemanticParameterError::InvalidTimeZone.into());
            }
            let key = TimeZoneKey::parse(value)
                .filter(|key| key.name() == value)
                .ok_or(SemanticParameterError::InvalidTimeZone)?;
            Ok(SemanticParameterValue::TimeZone(key))
        }
        Some(Value::AllowThrowException(value)) => {
            Ok(SemanticParameterValue::AllowThrowException(*value))
        }
        Some(Value::DecimalOverflowToDouble(value)) => {
            Ok(SemanticParameterValue::DecimalOverflowToDouble(*value))
        }
        Some(Value::GroupConcatLegacy(value)) => {
            Ok(SemanticParameterValue::GroupConcatLegacy(*value))
        }
        Some(Value::GroupConcatMaxLen(value)) => {
            Ok(SemanticParameterValue::GroupConcatMaxLen(*value))
        }
        None => Err(SemanticsCodecError::InvalidShape(
            "semantic parameter is missing its value variant",
        )),
    }
}

pub(super) fn decode_parameters(
    parameters: &wire::SemanticParameters,
    work: &mut CompileCheckpoints<'_>,
) -> Result<SemanticParameters, SemanticsCodecError> {
    if parameters.entries.len() > MAX_SEMANTIC_PARAMETERS {
        return Err(SemanticsCodecError::from(
            SemanticParameterError::TooManyParameters,
        ));
    }
    let mut input = parameters.entries.iter();
    let mut projection_error = None;
    // Feed the actual checked owner lazily. Its bounded per-item validation
    // and insertion cannot become an unobserved second walk over a Vec.
    let checked = SemanticParameters::try_new(std::iter::from_fn(|| {
        if projection_error.is_some() {
            return None;
        }
        let entry = input.next()?;
        let decoded = (|| {
            work.step()?;
            Ok((
                SemanticParameterId::new(entry.id),
                decode_value(entry.value.as_ref())?,
            ))
        })();
        match decoded {
            Ok(entry) => Some(entry),
            Err(error) => {
                projection_error = Some(error);
                None
            }
        }
    }));
    // Iterator termination after a refused projection can produce a valid
    // prefix in the owner. Never publish that prefix or replace its failure.
    if let Some(error) = projection_error {
        return Err(error);
    }
    checked.map_err(Into::into)
}

/// Same wire value grammar, fed lazily into the original observed constructor.
/// Pulling the iterator brackets the bounded value projection with the caller's
/// existing work scope, so no temporary entries Vec or second meter is needed.
pub(super) fn decode_parameters_observed(
    parameters: &wire::SemanticParameters,
    work: &mut CompileCheckpoints<'_>,
) -> Result<SemanticParameters, SemanticsCodecError> {
    use novarocks_type_contract::SemanticParameterProjectionError;
    if parameters.entries.len() > MAX_SEMANTIC_PARAMETERS {
        return Err(SemanticsCodecError::from(
            SemanticParameterError::TooManyParameters,
        ));
    }
    let mut input = parameters.entries.iter();
    let mut projection_error = None;
    let entries = std::iter::from_fn(|| {
        if projection_error.is_some() {
            return None;
        }
        let entry = input.next()?;
        match decode_value(entry.value.as_ref()) {
            Ok(value) => Some((SemanticParameterId::new(entry.id), value)),
            Err(error) => {
                projection_error = Some(error);
                None
            }
        }
    });
    let checked = SemanticParameters::try_new_observed(entries, work);
    // The constructor's originating control refusal must outrank a lazy
    // decoder latch. An ordinary terminated prefix is never publication.
    match checked {
        Err(SemanticParameterProjectionError::Control(cause)) => Err(cause.into()),
        checked => {
            if let Some(error) = projection_error {
                return Err(error);
            }
            checked.map_err(|error| match error {
                SemanticParameterProjectionError::Parameter(error) => error.into(),
                SemanticParameterProjectionError::Control(cause) => cause.into(),
            })
        }
    }
}

/// Source values and their actual construction responsibility. Borrowing does
/// not publish a naked independently owning parameter table.
pub struct CreatedWriterSemanticParameters {
    parameters: SemanticParameters,
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
impl CreatedWriterSemanticParameters {
    pub fn parameters(&self) -> &SemanticParameters {
        &self.parameters
    }
}

/// Keep the original parameter grammar/constructor. Known timezone boxes and
/// the locked insertion-only BTree operation are observed separately before
/// the stock constructor; the bound is not an allocator receipt or grant.
pub fn decode_writer_semantic_parameters_with_creator_in<H>(
    source: &wire::SemanticParameters,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    std::sync::Arc<CreatedWriterSemanticParameters>,
    crate::host_projection_v2::ProjectionFailure<SemanticsCodecError, H>,
> {
    use crate::host_projection_v2::ProjectionFailure;
    use novarocks_type_contract::{SemanticParameterId, owned_resources::layout::arc_layout};
    use std::{alloc::Layout, sync::Arc};
    if source.entries.len() > MAX_SEMANTIC_PARAMETERS {
        return Err(SemanticsCodecError::from(SemanticParameterError::TooManyParameters).into());
    }
    let tree = crate::btree_resources_v2::insertion_only::<
        SemanticParameterId,
        SemanticParameterValue,
    >(source.entries.len())
    .map_err(|_| {
        SemanticsCodecError::SourceModel("writer parameter tree bound is unrepresentable")
    })?;
    work.flush().map_err(SemanticsCodecError::from)?;
    if tree.request_bytes_upper_bound != 0 {
        creation
            .observe_opaque_upper_bound(tree.request_bytes_upper_bound)
            .map_err(ProjectionFailure::Host)?;
    }
    for entry in &source.entries {
        work.step().map_err(SemanticsCodecError::from)?;
        if let Some(wire::semantic_parameter::Value::TimeZone(value)) = &entry.value
            && value.len() > 255
        {
            return Err(SemanticsCodecError::from(SemanticParameterError::InvalidTimeZone).into());
        }
    }
    let parameters = decode_parameters_observed(source, work)?;
    work.flush().map_err(SemanticsCodecError::from)?;
    let layout = arc_layout(Layout::new::<CreatedWriterSemanticParameters>()).map_err(|_| {
        SemanticsCodecError::SourceModel("writer parameter holder layout is unrepresentable")
    })?;
    creation
        .request_layout(layout)
        .map_err(ProjectionFailure::Host)?;
    let holder = Arc::new(CreatedWriterSemanticParameters {
        parameters,
        _storage: creation.storage_lifetime(),
    });
    work.step().map_err(SemanticsCodecError::from)?;
    work.flush().map_err(SemanticsCodecError::from)?;
    Ok(holder)
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
    use std::sync::Mutex;

    #[derive(Default)]
    struct Control {
        events: Mutex<Vec<(CompilePhase, u32)>>,
        refusal: Option<(usize, CompileControlError)>,
    }

    impl PureCompileControl for Control {
        fn checkpoint(
            &self,
            phase: CompilePhase,
            work_units: u32,
        ) -> Result<(), CompileControlError> {
            let mut events = self.events.lock().unwrap();
            let ordinal = events.len();
            events.push((phase, work_units));
            match self.refusal.clone() {
                Some((at, error)) if ordinal == at => Err(error),
                _ => Ok(()),
            }
        }
    }

    // Exercise the leaf under the same entry/tail ownership as its public
    // component wrapper, preserving an already observed primary control error.
    fn projected<T>(
        control: &dyn PureCompileControl,
        phase: CompilePhase,
        project: impl FnOnce(&mut CompileCheckpoints<'_>) -> Result<T, SemanticsCodecError>,
    ) -> Result<T, SemanticsCodecError> {
        let mut work = CompileCheckpoints::try_new(control, phase)?;
        let result = project(&mut work);
        if matches!(&result, Err(SemanticsCodecError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }

    fn decode(
        dto: &wire::SemanticParameters,
        control: &dyn PureCompileControl,
    ) -> Result<SemanticParameters, SemanticsCodecError> {
        projected(control, CompilePhase::Decode, |work| {
            decode_parameters(dto, work)
        })
    }

    fn encode(
        table: &SemanticParameters,
        control: &dyn PureCompileControl,
    ) -> Result<wire::SemanticParameters, SemanticsCodecError> {
        projected(control, CompilePhase::Encode, |work| {
            encode_parameters(table, work)
        })
    }

    fn entry(id: u32, value: wire::semantic_parameter::Value) -> wire::SemanticParameter {
        wire::SemanticParameter {
            id,
            value: Some(value),
        }
    }

    fn repeated_booleans(count: usize) -> wire::SemanticParameters {
        wire::SemanticParameters {
            entries: (0..count)
                .map(|ordinal| {
                    entry(
                        u32::try_from(ordinal).unwrap(),
                        wire::semantic_parameter::Value::AllowThrowException(ordinal % 2 == 0),
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn parameter_projection_preserves_all_six_exact_values_without_clamps() {
        use wire::semantic_parameter::Value;
        let dto = wire::SemanticParameters {
            entries: vec![
                entry(
                    0,
                    wire::semantic_parameter::Value::StatementStartUtc(wire::StatementStartUtc {
                        seconds: Some(i64::MIN),
                        nanos: Some(0),
                    }),
                ),
                entry(9, Value::TimeZone("-07:30".into())),
                entry(11, Value::AllowThrowException(false)),
                entry(13, Value::DecimalOverflowToDouble(true)),
                entry(15, Value::GroupConcatLegacy(false)),
                entry(u32::MAX, Value::GroupConcatMaxLen(-77)),
            ],
        };
        let before = dto.clone();
        let expected = SemanticParameters::try_new([
            (
                SemanticParameterId::new(0),
                SemanticParameterValue::StatementStartUtc(
                    novarocks_type_contract::StatementStartUtc::try_new(i64::MIN, 0).unwrap(),
                ),
            ),
            (
                SemanticParameterId::new(9),
                SemanticParameterValue::TimeZone(
                    novarocks_type_contract::TimeZoneKey::parse("-07:30").unwrap(),
                ),
            ),
            (
                SemanticParameterId::new(11),
                SemanticParameterValue::AllowThrowException(false),
            ),
            (
                SemanticParameterId::new(13),
                SemanticParameterValue::DecimalOverflowToDouble(true),
            ),
            (
                SemanticParameterId::new(15),
                SemanticParameterValue::GroupConcatLegacy(false),
            ),
            (
                SemanticParameterId::new(u32::MAX),
                SemanticParameterValue::GroupConcatMaxLen(-77),
            ),
        ])
        .unwrap();
        let table = decode(&dto, &Control::default()).unwrap();
        assert_eq!(table, expected);
        assert_eq!(dto, before);
        assert_eq!(encode(&table, &Control::default()).unwrap(), dto);
        for raw in [i64::MIN, 0, i64::MAX] {
            let dto = wire::SemanticParameters {
                entries: vec![entry(42, Value::GroupConcatMaxLen(raw))],
            };
            let table = decode(&dto, &Control::default()).unwrap();
            assert_eq!(
                table.get(SemanticParameterId::new(42)),
                Some(&SemanticParameterValue::GroupConcatMaxLen(raw))
            );
        }
    }

    #[test]
    fn intrinsic_allow_throw_parameter_author_keeps_scoped_boolean_refs_and_exact_keys() {
        use wire::semantic_parameter::Value;
        let dto = wire::SemanticParameters {
            entries: vec![
                entry(0, Value::AllowThrowException(false)),
                entry(u32::MAX, Value::AllowThrowException(true)),
            ],
        };
        let table = decode(&dto, &Control::default()).unwrap();
        assert_eq!(encode(&table, &Control::default()).unwrap(), dto);
        for (id, value) in [(0, false), (u32::MAX, true)] {
            let reference = SemanticParameterRef {
                id: SemanticParameterId::new(id),
                expected_key: SemanticParameterKey::AllowThrowException,
            };
            let encoded = projected(&Control::default(), CompilePhase::Encode, |work| {
                encode_reference(&reference, work)
            })
            .unwrap();
            assert_eq!(encoded.id, Some(id));
            assert_eq!(
                encoded.expected_key,
                wire::SemanticParameterKey::AllowThrowException as i32
            );
            let decoded = projected(&Control::default(), CompilePhase::Decode, |work| {
                decode_reference(&encoded, work)
            })
            .unwrap();
            assert_eq!(decoded, reference);
            assert_eq!(
                table.require(decoded).unwrap(),
                &SemanticParameterValue::AllowThrowException(value)
            );
            let wrong = SemanticParameterRef {
                expected_key: SemanticParameterKey::DecimalOverflowToDouble,
                ..decoded
            };
            assert!(table.require(wrong).is_err());
        }
        assert!(
            table
                .require(SemanticParameterRef {
                    id: SemanticParameterId::new(71),
                    expected_key: SemanticParameterKey::AllowThrowException
                })
                .is_err()
        );
        for expected_key in [0, i32::MAX] {
            assert!(matches!(
                projected(&Control::default(), CompilePhase::Decode, |work| {
                    decode_reference(
                        &wire::SemanticParameterRef {
                            id: Some(0),
                            expected_key,
                        },
                        work,
                    )
                }),
                Err(SemanticsCodecError::InvalidShape(_))
            ));
        }
        assert!(matches!(
            projected(&Control::default(), CompilePhase::Decode, |work| {
                decode_reference(
                    &wire::SemanticParameterRef {
                        id: None,
                        expected_key: wire::SemanticParameterKey::AllowThrowException as i32,
                    },
                    work,
                )
            }),
            Err(SemanticsCodecError::InvalidShape(_))
        ));
    }

    #[test]
    fn parameter_projection_keeps_scoped_keys_and_sparse_reference_identity() {
        use wire::semantic_parameter::Value;
        let dto = wire::SemanticParameters {
            entries: vec![
                entry(u32::MAX, Value::TimeZone("UTC".into())),
                entry(0, Value::TimeZone("+08:00".into())),
            ],
        };
        let table = decode(&dto, &Control::default()).unwrap();
        for (id, zone) in [(0, "+08:00"), (u32::MAX, "UTC")] {
            let reference = SemanticParameterRef {
                id: SemanticParameterId::new(id),
                expected_key: SemanticParameterKey::TimeZone,
            };
            let encoded = projected(&Control::default(), CompilePhase::Encode, |work| {
                encode_reference(&reference, work)
            })
            .unwrap();
            assert_eq!(encoded.id, Some(id));
            let decoded = projected(&Control::default(), CompilePhase::Decode, |work| {
                decode_reference(&encoded, work)
            })
            .unwrap();
            assert_eq!(decoded, reference);
            assert_eq!(
                table.require(decoded).unwrap(),
                &SemanticParameterValue::TimeZone(TimeZoneKey::parse(zone).unwrap())
            );
        }
        assert!(matches!(
            table.require(SemanticParameterRef {
                id: SemanticParameterId::new(0),
                expected_key: SemanticParameterKey::GroupConcatLegacy,
            }),
            Err(SemanticParameterError::KeyMismatch(_))
        ));
        assert_eq!(table.entries().len(), 2);
    }

    #[test]
    fn parameter_reference_projection_rejects_absence_and_nonclosed_keys() {
        let key_pairs = [
            (
                SemanticParameterKey::StatementStartUtc,
                wire::SemanticParameterKey::StatementStartUtc,
            ),
            (
                SemanticParameterKey::TimeZone,
                wire::SemanticParameterKey::TimeZone,
            ),
            (
                SemanticParameterKey::AllowThrowException,
                wire::SemanticParameterKey::AllowThrowException,
            ),
            (
                SemanticParameterKey::DecimalOverflowToDouble,
                wire::SemanticParameterKey::DecimalOverflowToDouble,
            ),
            (
                SemanticParameterKey::GroupConcatLegacy,
                wire::SemanticParameterKey::GroupConcatLegacy,
            ),
            (
                SemanticParameterKey::GroupConcatMaxLen,
                wire::SemanticParameterKey::GroupConcatMaxLen,
            ),
        ];
        for (typed, wire_key) in key_pairs {
            assert_eq!(encode_key(typed), wire_key as i32);
            assert_eq!(decode_key(wire_key as i32).unwrap(), typed);
        }
        for bad_key in [0, -1, 7, i32::MAX] {
            let reference = wire::SemanticParameterRef {
                id: Some(0),
                expected_key: bad_key,
            };
            assert!(matches!(
                projected(&Control::default(), CompilePhase::Decode, |work| {
                    decode_reference(&reference, work)
                }),
                Err(SemanticsCodecError::InvalidShape(_))
            ));
        }
        let absent = wire::SemanticParameterRef {
            id: None,
            expected_key: wire::SemanticParameterKey::TimeZone as i32,
        };
        assert!(matches!(
            projected(&Control::default(), CompilePhase::Decode, |work| {
                decode_reference(&absent, work)
            }),
            Err(SemanticsCodecError::InvalidShape(_))
        ));
    }

    #[test]
    fn parameter_checked_owner_rejects_duplicate_authorities_and_missing_values() {
        use wire::semantic_parameter::Value;
        let duplicate = wire::SemanticParameters {
            entries: vec![
                entry(0, Value::AllowThrowException(false)),
                entry(0, Value::GroupConcatLegacy(true)),
            ],
        };
        assert!(
            matches!(decode(&duplicate, &Control::default()), Err(SemanticsCodecError::Parameter(SemanticParameterError::DuplicateId(id))) if id.get() == 0)
        );
        let clocks = wire::SemanticParameters {
            entries: vec![
                entry(
                    0,
                    wire::semantic_parameter::Value::StatementStartUtc(wire::StatementStartUtc {
                        seconds: Some(-1),
                        nanos: Some(0),
                    }),
                ),
                entry(
                    u32::MAX,
                    wire::semantic_parameter::Value::StatementStartUtc(wire::StatementStartUtc {
                        seconds: Some(-1),
                        nanos: Some(0),
                    }),
                ),
            ],
        };
        assert!(matches!(
            decode(&clocks, &Control::default()),
            Err(SemanticsCodecError::Parameter(
                SemanticParameterError::DuplicateStatementStart
            ))
        ));
        let missing = wire::SemanticParameters {
            entries: vec![
                entry(0, Value::GroupConcatLegacy(false)),
                wire::SemanticParameter { id: 1, value: None },
            ],
        };
        assert!(matches!(
            decode(&missing, &Control::default()),
            Err(SemanticsCodecError::InvalidShape(_))
        ));
    }

    #[test]
    fn parameter_time_zone_bound_and_public_spelling_validation_are_preserved() {
        for spelling in [
            "",
            "UTC\n",
            "Z",
            "+00:00",
            "\0",
            &"a".repeat(256),
            &"é".repeat(128),
        ] {
            let dto = wire::SemanticParameters {
                entries: vec![entry(
                    0,
                    wire::semantic_parameter::Value::TimeZone(spelling.into()),
                )],
            };
            assert!(matches!(
                decode(&dto, &Control::default()),
                Err(SemanticsCodecError::Parameter(
                    SemanticParameterError::InvalidTimeZone
                ))
            ));
        }
        let spelling = "America/Argentina/Buenos_Aires".to_owned();
        let dto = wire::SemanticParameters {
            entries: vec![entry(
                u32::MAX,
                wire::semantic_parameter::Value::TimeZone(spelling.clone()),
            )],
        };
        let table = decode(&dto, &Control::default()).unwrap();
        assert_eq!(
            table.get(SemanticParameterId::new(u32::MAX)),
            Some(&SemanticParameterValue::TimeZone(
                TimeZoneKey::parse(&spelling).unwrap()
            ))
        );
        assert_eq!(encode(&table, &Control::default()).unwrap(), dto);
    }

    #[test]
    fn statement_start_requires_presence_and_normalized_nanoseconds() {
        use wire::semantic_parameter::Value;
        for (seconds, nanos) in [
            (None, Some(0)),
            (Some(0), None),
            (Some(0), Some(1_000_000_000)),
        ] {
            let dto = wire::SemanticParameters {
                entries: vec![entry(
                    0,
                    Value::StatementStartUtc(wire::StatementStartUtc { seconds, nanos }),
                )],
            };
            assert!(matches!(
                decode(&dto, &Control::default()),
                Err(SemanticsCodecError::InvalidShape(_))
            ));
        }
        let dto = wire::SemanticParameters {
            entries: vec![entry(
                0,
                Value::StatementStartUtc(wire::StatementStartUtc {
                    seconds: Some(-1),
                    nanos: Some(987_654_321),
                }),
            )],
        };
        let table = decode(&dto, &Control::default()).unwrap();
        assert_eq!(encode(&table, &Control::default()).unwrap(), dto);
    }

    #[test]
    fn parameter_count_precedes_item_projection_and_allocation() {
        let mut too_many = repeated_booleans(MAX_SEMANTIC_PARAMETERS + 1);
        // This earlier item is invalid too. The count gate must run first.
        too_many.entries[0].value = None;
        let control = Control::default();
        assert!(matches!(
            decode(&too_many, &control),
            Err(SemanticsCodecError::Parameter(
                SemanticParameterError::TooManyParameters
            ))
        ));
        assert_eq!(
            *control.events.lock().unwrap(),
            [(CompilePhase::Decode, 0), (CompilePhase::Decode, 0)]
        );
        let dto = repeated_booleans(MAX_SEMANTIC_PARAMETERS);
        let control = Control::default();
        let table = decode(&dto, &control).unwrap();
        assert_eq!(table.entries().len(), MAX_SEMANTIC_PARAMETERS);
        assert_eq!(
            control
                .events
                .lock()
                .unwrap()
                .iter()
                .map(|(_, units)| *units)
                .sum::<u32>(),
            MAX_SEMANTIC_PARAMETERS as u32
        );
        assert_eq!(encode(&table, &Control::default()).unwrap(), dto);
    }

    #[test]
    fn parameter_projection_observes_empty_entry_and_every_bounded_item_tail() {
        let empty = wire::SemanticParameters::default();
        let control = Control::default();
        assert_eq!(
            decode(&empty, &control).unwrap(),
            SemanticParameters::default()
        );
        assert_eq!(
            *control.events.lock().unwrap(),
            [(CompilePhase::Decode, 0), (CompilePhase::Decode, 0)]
        );
        let dto = repeated_booleans(259);
        let control = Control::default();
        let table = decode(&dto, &control).unwrap();
        assert_eq!(
            *control.events.lock().unwrap(),
            [
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 256),
                (CompilePhase::Decode, 3)
            ]
        );
        let control = Control::default();
        assert_eq!(encode(&table, &control).unwrap(), dto);
        assert_eq!(
            *control.events.lock().unwrap(),
            [
                (CompilePhase::Encode, 0),
                (CompilePhase::Encode, 256),
                (CompilePhase::Encode, 3)
            ]
        );
    }

    #[test]
    fn parameter_projection_preserves_all_control_categories_at_entry_quantum_and_tail() {
        let dto = repeated_booleans(259);
        let table = decode(&dto, &Control::default()).unwrap();
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for ordinal in 0..=2 {
                for phase in [CompilePhase::Encode, CompilePhase::Decode] {
                    let control = Control {
                        events: Mutex::default(),
                        refusal: Some((ordinal, error.clone())),
                    };
                    let result = if phase == CompilePhase::Encode {
                        encode(&table, &control).map(|_| ())
                    } else {
                        decode(&dto, &control).map(|_| ())
                    };
                    assert!(
                        matches!(result, Err(SemanticsCodecError::Control(actual)) if actual == error)
                    );
                    let events = control.events.lock().unwrap();
                    assert_eq!(events.len(), ordinal + 1);
                    assert!(
                        events
                            .iter()
                            .all(|(actual_phase, units)| *actual_phase == phase && *units <= 256)
                    );
                }
            }
        }
    }

    #[test]
    fn parameter_lazy_owner_never_publishes_a_prefix_after_projection_refusal() {
        let mut dto = repeated_booleans(259);
        dto.entries[256].value = None;
        let control = Control {
            events: Mutex::default(),
            refusal: Some((1, CompileControlError::Cancelled)),
        };
        // The refusal while visiting item 256 is primary, before the later
        // missing variant; the owner's otherwise valid prefix is discarded.
        assert!(matches!(
            decode(&dto, &control),
            Err(SemanticsCodecError::Control(CompileControlError::Cancelled))
        ));
        assert_eq!(
            *control.events.lock().unwrap(),
            [(CompilePhase::Decode, 0), (CompilePhase::Decode, 256)]
        );
        let control = Control::default();
        assert!(matches!(
            decode(&dto, &control),
            Err(SemanticsCodecError::InvalidShape(_))
        ));
        assert_eq!(
            *control.events.lock().unwrap(),
            [
                (CompilePhase::Decode, 0),
                (CompilePhase::Decode, 256),
                (CompilePhase::Decode, 1)
            ]
        );
    }
}
