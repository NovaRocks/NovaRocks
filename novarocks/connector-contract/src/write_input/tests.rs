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
use crate::PureProviderCompileError;
use crate::owned_copy::{ObservedCopy, WriterOwnedResourceFacts};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::{alloc::Layout, collections::TryReserveError, sync::Mutex};

type Failure = PureProviderCompileError<ConnectorError>;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn refusing(at: usize, cause: CompileControlError) -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            refusal: Some((at, cause)),
        }
    }
    fn trace(&self) -> Vec<u32> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::ProviderValidation);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        trace.push(units);
        if let Some((refuse_at, cause)) = self.refusal.clone()
            && at == refuse_at
        {
            return Err(cause);
        }
        Ok(())
    }
}
fn binding(token: u8, name: &str) -> ConnectorWriteFieldBinding {
    ConnectorWriteFieldBinding::new(
        ConnectorWriteFieldToken::from_bytes([token; 32]),
        Field::new(name, DataType::Int64, token.is_multiple_of(2)),
    )
}
fn invoke(
    shape: &ConnectorWriteInputShape,
    control: &Control,
) -> Result<ConnectorWriteInputShape, Failure> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
    let mut admit = |_: &WriterOwnedResourceFacts| Ok(());
    let result = (|| {
        let mut context = ObservedCopy::new(16 << 20, &mut admit, &mut work)?;
        shape.preflight_owned_roles(&mut context)?;
        shape.validate_with(&mut context)?;
        assert!(shape.owned_bounded_core(&mut context)?.is_none());
        context.begin_copy()?;
        shape.owned_bounded_core(&mut context)?.ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                "copy fixture did not materialize",
            )
            .into()
        })
    })();
    match result {
        Err(Failure::Control(cause)) => Err(Failure::Control(cause)),
        result => {
            work.finish()?;
            result
        }
    }
}
fn assert_control(error: Failure, cause: CompileControlError) {
    assert!(matches!(error, Failure::Control(actual) if actual == cause));
}

#[test]
fn owned_input_preserves_each_role_order_tokens_and_complete_fields() {
    #[allow(deprecated)]
    let field = Field::new_dict(
        "nested",
        DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
        true,
        711,
        true,
    )
    .with_metadata(std::collections::HashMap::<String, String>::from([(
        "tag".to_owned(),
        "中\0".to_owned(),
    )]));
    let nested =
        ConnectorWriteFieldBinding::new(ConnectorWriteFieldToken::from_bytes([7; 32]), field);
    let shapes = [
        ConnectorWriteInputShape::Data {
            fields: vec![nested.clone(), binding(1, "a")],
        },
        ConnectorWriteInputShape::RowLineage {
            data_fields: vec![nested.clone()],
            row_identity_fields: vec![binding(2, "row")],
        },
        ConnectorWriteInputShape::PositionDelete {
            identity_fields: vec![binding(3, "id")],
            partition_source_fields: vec![nested.clone()],
        },
        ConnectorWriteInputShape::DeletionVector {
            identity_fields: vec![binding(4, "id")],
            partition_source_fields: vec![nested.clone()],
        },
        ConnectorWriteInputShape::EqualityDelete {
            equality_fields: vec![nested, binding(5, "key")],
        },
    ];
    for shape in shapes {
        let copied = invoke(&shape, &Control::default()).unwrap();
        assert_eq!(copied, shape);
        let mut plain = PlainCopy;
        copied.preflight_owned_roles(&mut plain).unwrap();
        assert_eq!(
            copied.owned_bounded_core(&mut plain).unwrap().unwrap(),
            shape
        );
        let copied_roles = copied.role_vectors();
        let source_roles = shape.role_vectors();
        for (copied, source) in copied_roles.into_iter().zip(source_roles) {
            match (copied, source) {
                (Some(copied), Some(source)) => {
                    assert_eq!(copied.len(), source.len());
                    assert_ne!(copied.as_ptr(), source.as_ptr());
                    for (actual, expected) in copied.iter().zip(source) {
                        assert_eq!(actual.token(), expected.token());
                        assert!(crate::arrow_fields_exact(actual.field(), expected.field()));
                    }
                }
                (None, None) => {}
                _ => panic!("a role was merged or dropped"),
            }
        }
    }
}

#[derive(Default)]
struct Recording {
    requests: Vec<(Layout, usize)>,
    units: Vec<usize>,
    floor: usize,
    spellings: Vec<String>,
}
impl OwnedCopy for Recording {
    type Error = ConnectorError;
    fn materializes(&self) -> bool {
        false
    }
    fn source_invoice(&self) -> Option<usize> {
        Some(16 << 20)
    }
    fn request(&mut self, layout: Layout, copies: usize) -> Result<(), Self::Error> {
        self.requests.push((layout, copies));
        Ok(())
    }
    fn work(&mut self, units: usize) -> Result<(), Self::Error> {
        self.units.push(units);
        Ok(())
    }
    fn source_floor(&mut self, floor: usize) -> Result<(), Self::Error> {
        self.floor = self.floor.max(floor);
        Ok(())
    }
    fn arithmetic(&self) -> Self::Error {
        ConnectorError::new(
            ConnectorErrorKind::ResourceExhausted,
            "recording arithmetic",
        )
    }
    fn reserve_exit(&mut self, result: Result<(), TryReserveError>) -> Result<(), Self::Error> {
        PlainCopy.reserve_exit(result)
    }
    fn spelling(&mut self, input: &str) -> Result<String, Self::Error> {
        self.spellings.push(input.to_owned());
        Ok(input.to_owned())
    }
}
#[test]
fn duplicate_token_short_circuits_huge_name_copy_and_duplicate_name_keeps_original_error() {
    let duplicate_token = ConnectorWriteInputShape::Data {
        fields: vec![binding(1, "first"), binding(1, &"中".repeat(2048))],
    };
    let mut observed = Recording::default();
    let error = duplicate_token.validate_with(&mut observed).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    assert_eq!(
        error.message(),
        "connector write input shape contains a duplicate field token or name"
    );
    assert_eq!(observed.spellings, ["first"]);
    // No second name request was admitted or allocated on the short circuit.
    assert!(
        !observed
            .requests
            .iter()
            .any(|(layout, _)| layout.size() == 6144)
    );

    let duplicate_name = ConnectorWriteInputShape::RowLineage {
        data_fields: vec![binding(1, "same")],
        row_identity_fields: vec![binding(2, "same")],
    };
    let mut observed = Recording::default();
    let error = duplicate_name.validate_with(&mut observed).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    assert_eq!(
        error.message(),
        duplicate_token.validate().unwrap_err().message()
    );
    assert_eq!(observed.spellings, ["same", "same"]);
    let empty = ConnectorWriteInputShape::Data { fields: Vec::new() };
    assert_eq!(
        empty.validate().unwrap_err().message(),
        "connector write input shape must contain at least one field"
    );
}

#[test]
fn role_preflight_counts_real_separate_vec_capacities_and_two_trim_requests() {
    let mut data = Vec::with_capacity(8);
    data.push(binding(1, "data"));
    let mut identity = Vec::with_capacity(16);
    identity.push(binding(2, "id"));
    let expected_floor = size_of::<ConnectorWriteInputShape>()
        + (data.capacity() + identity.capacity()) * size_of::<ConnectorWriteFieldBinding>();
    let shape = ConnectorWriteInputShape::RowLineage {
        data_fields: data,
        row_identity_fields: identity,
    };
    let mut counted = Recording::default();
    shape.preflight_owned_roles(&mut counted).unwrap();
    let layout = Layout::array::<ConnectorWriteFieldBinding>(1).unwrap();
    assert_eq!(counted.requests, [(layout, 2), (layout, 2)]);
    assert_eq!(counted.floor, expected_floor);
    assert_eq!(
        counted.units,
        [size_of::<ConnectorWriteInputShape>() + 6 * size_of::<ConnectorWriteFieldBinding>()]
    );
    let role_requests = counted.requests.clone();
    assert!(shape.owned_bounded_core(&mut counted).unwrap().is_none());
    // Scalar field name/metadata copying has its own requests; role headers
    // remain exactly the originally admitted two entries, with no core replay.
    assert_eq!(
        counted
            .requests
            .iter()
            .filter(|(actual, _)| *actual == layout)
            .copied()
            .collect::<Vec<_>>(),
        role_requests
    );
}

#[test]
fn known_role_and_table_resource_refusals_precede_pending_255_late_controls() {
    let shape = ConnectorWriteInputShape::Data {
        fields: vec![binding(1, "v")],
    };
    for cause in CAUSES {
        for role_preflight in [false, true] {
            let control = Control::refusing(1, cause.clone());
            let mut work =
                CompileCheckpoints::try_new(&control, CompilePhase::ProviderValidation).unwrap();
            let source = [11_u8; 255];
            let mut copied = [0_u8; 255];
            for (source, target) in source.iter().zip(&mut copied) {
                *target = *source;
                work.step().unwrap();
            }
            assert_eq!(copied, [11_u8; 255]);
            let mut admission = |facts: &WriterOwnedResourceFacts| {
                if facts.requested_bytes > 16 * 1024 {
                    Err(CompileControlError::ResourceExhausted)
                } else {
                    Ok(())
                }
            };
            let mut context = ObservedCopy::new(16 << 20, &mut admission, &mut work).unwrap();
            let result = if role_preflight {
                shape.preflight_owned_roles(&mut context)
            } else {
                shape.validate_with(&mut context)
            };
            assert_control(result.unwrap_err(), CompileControlError::ResourceExhausted);
            assert_eq!(control.trace(), [0]);
        }
    }
}

#[test]
fn actual_validation_and_copy_callbacks_keep_all_three_causes_and_ordinary_tail() {
    let success = ConnectorWriteInputShape::RowLineage {
        data_fields: vec![binding(1, "a\0中")],
        row_identity_fields: vec![binding(2, "id")],
    };
    let ordinary = ConnectorWriteInputShape::Data {
        fields: vec![binding(1, "same"), binding(2, "same")],
    };
    let wide = ConnectorWriteInputShape::Data {
        fields: vec![binding(1, &"中".repeat(320))],
    };
    for (shape, is_success) in [(&success, true), (&ordinary, false), (&wide, true)] {
        let baseline = Control::default();
        let outcome = invoke(shape, &baseline);
        if is_success {
            assert_eq!(outcome.unwrap(), *shape);
        } else {
            assert!(matches!(outcome, Err(Failure::Provider(ref error))
                if error.kind() == ConnectorErrorKind::InvalidRequest
                && error.message() == "connector write input shape contains a duplicate field token or name"));
        }
        let trace = baseline.trace();
        assert!(!trace.is_empty());
        if std::ptr::eq(shape, &wide) {
            assert!(trace.contains(&256));
        }
        for at in 0..trace.len() {
            for cause in CAUSES {
                let control = Control::refusing(at, cause.clone());
                assert_control(invoke(shape, &control).unwrap_err(), cause);
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    }
}
