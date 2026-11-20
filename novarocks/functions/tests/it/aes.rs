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
use arrow_array::{Array, BinaryArray, StringArray};
use arrow_schema::DataType;
use novarocks_functions::builtin::{
    aes_rows::{
        Operation, Row, RowFailure, evaluate_required_row, evaluate_row, evaluate_row_observed,
    },
    bytes_output,
    md5_shared::{Observation, OwnedBytesArray},
};
use novarocks_functions::{
    EvaluationCheckpoints, KernelDiagnostic, KernelEvaluationControl, KernelFailure,
};
use std::{sync::Mutex, time::Duration};
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal.as_ref() {
            assert!(at <= *stop, "callback after originating refusal");
        }
        trace.push(units);
        match self.refusal.as_ref() {
            Some((stop, cause)) if at == *stop => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("AES must not wait")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("AES original invalid cause")),
        KernelFailure::Internal(KernelDiagnostic::new("AES original internal cause")),
        KernelFailure::Operational(KernelDiagnostic::new("AES original operational cause")),
        KernelFailure::InstanceFailed,
    ]
}
fn text(value: Option<&str>) -> OwnedBytesArray {
    OwnedBytesArray::Utf8(StringArray::from(vec![value]))
}
fn binary(value: &[u8]) -> OwnedBytesArray {
    OwnedBytesArray::Binary(BinaryArray::from(vec![Some(value)]))
}
fn run(
    operation: Operation,
    inputs: &[OwnedBytesArray],
    control: &Control,
) -> Result<Row, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let rows = [0; 5];
    let out =
        evaluate_row_observed(
            operation,
            inputs,
            &rows[..inputs.len()],
            &mut |event| match event {
                Observation::Step => work.step(),
                Observation::OpaqueBoundary => work.flush(),
            },
        );
    work.finish_result(out)
}
#[test]
fn aes_original_source_sensitive_ctr_and_output_projection_are_same_core() {
    let zeros = [0; 16];
    let args = [
        text(Some("ÿ\u{80}")),
        binary(&zeros),
        binary(&zeros),
        text(Some("AES_128_CTR")),
    ];
    // B8c gives every Utf8 ciphertext the same Latin-1 reading.
    let Row::Value(Some(value)) = run(Operation::Decrypt, &args, &Control::default()).unwrap()
    else {
        panic!("CTR must produce bytes");
    };
    assert_eq!(value, [0x99, 0x69]);
    let output =
        bytes_output::build_bytes_output_lossy(vec![Some(value.clone())], Some(&DataType::Utf8))
            .unwrap();
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        String::from_utf8_lossy(&value),
    );
}
#[test]
fn aes_original_null_empty_and_aad_errors_have_original_priority() {
    let mut args = [
        text(Some("a")),
        text(Some("k")),
        text(None),
        text(Some("AES_128_ECB")),
        text(Some("")),
    ];
    assert!(matches!(
        evaluate_row(Operation::Encrypt, &args, &[0; 5]),
        Row::Failure(RowFailure::AadRequiresGcm)
    ));
    args[0] = text(None);
    assert!(matches!(
        evaluate_row(Operation::Encrypt, &args, &[0; 5]),
        Row::Value(None)
    ));
    args[0] = text(Some(""));
    assert!(matches!(
        evaluate_row(Operation::Decrypt, &args, &[0; 5]),
        Row::Value(None)
    ));
    args[0] = text(Some("a"));
    args[3] = text(Some("AES_128_CBC"));
    assert!(matches!(
        evaluate_row(Operation::Decrypt, &args, &[0; 5]),
        Row::Value(None)
    ));
}
#[test]
fn aes_every_original_owned_or_library_checkpoint_preserves_all_seven_causes() {
    let payload = "a\0b".repeat(260);
    let key = "key".repeat(120);
    let iv = "123";
    let aad = "aad".repeat(90);
    let encrypt_args = vec![
        text(Some(&payload)),
        text(Some(&key)),
        text(Some(iv)),
        text(Some("AES_128_GCM")),
        text(Some(&aad)),
    ];
    let Row::Value(Some(cipher)) = evaluate_row(Operation::Encrypt, &encrypt_args, &[0; 5]) else {
        panic!("original GCM must produce ciphertext")
    };
    let latin1 =
        bytes_output::build_bytes_output_latin1(vec![Some(cipher)], Some(&DataType::Utf8)).unwrap();
    let utf8 = latin1
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .clone();
    let decrypt_args = vec![
        OwnedBytesArray::Utf8(utf8),
        text(Some(&key)),
        text(Some("ignored")),
        text(Some("AES_128_GCM")),
        text(Some(&aad)),
    ];
    for (operation, inputs) in [
        (Operation::Encrypt, &encrypt_args),
        (Operation::Decrypt, &decrypt_args),
    ] {
        let recorder = Control::default();
        assert!(matches!(
            run(operation, inputs, &recorder),
            Ok(Row::Value(Some(_)))
        ));
        let trace = recorder.trace.lock().unwrap().clone();
        assert!(trace.contains(&256));
        for stop in 0..trace.len() {
            for cause in causes() {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((stop, cause.clone())),
                };
                assert!(matches!(run(operation,inputs,&control),Err(actual)if actual==cause));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}
#[test]
fn required_aes_preserves_domain_aad_refusal_and_separates_invalid_addresses() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let args = [
        text(Some("a")),
        text(Some("k")),
        text(None),
        text(Some("AES_128_ECB")),
        text(Some("")),
    ];
    let failure = evaluate_required_row(Operation::Encrypt, &args, &[0; 5], 7).unwrap_err();
    assert_eq!(failure.failure_fact().class(), FailureClass::Data);
    assert_eq!(failure.failure_fact().reason().code(), 373);
    assert_eq!(
        failure.to_string(),
        "aes_encrypt: requires GCM mode to use AAD parameter"
    );
    for rows in [&[0][..], &[0, 0, 0, 0, 1][..]] {
        let failure = evaluate_required_row(Operation::Encrypt, &args, rows, 0).unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(failure.failure_fact().reason().code(), 372);
    }
    let zeros = [0; 16];
    let input = OwnedBytesArray::LargeUtf8(arrow_array::LargeStringArray::from(vec!["ÿ\u{80}"]));
    let args = [
        input,
        binary(&zeros),
        binary(&zeros),
        text(Some("AES_128_CTR")),
    ];
    assert_eq!(
        evaluate_required_row(Operation::Decrypt, &args, &[0; 4], 0).unwrap(),
        Some(vec![0x99, 0x69])
    );
}

#[test]
fn catalogue_round_trip_reads_utf8_ciphertext_by_value_and_binary_as_raw_bytes() {
    use std::sync::Arc;

    use super::scalar_rows::{Control as CatalogueControl, prepare};
    use arrow_array::ArrayRef;
    use novarocks_functions::{EvaluatedArgument, FunctionArgument, RowDataError, Selection};
    use novarocks_type_contract::FunctionValueType;

    fn call(name: &str, data: &[ArrayRef]) -> (ArrayRef, Vec<RowDataError>) {
        let arguments = data
            .iter()
            .map(|array| FunctionArgument::Value {
                value_type: FunctionValueType::new(array.data_type().clone(), true),
                constant: None,
            })
            .collect::<Vec<_>>();
        let mut instance = prepare(name, &arguments);
        let values = data
            .iter()
            .map(EvaluatedArgument::Scalar)
            .collect::<Vec<_>>();
        let output = instance
            .evaluate(Selection::all(1), &values, &CatalogueControl)
            .unwrap();
        (Arc::clone(output.values()), output.errors().to_vec())
    }

    fn text(value: &str) -> ArrayRef {
        Arc::new(StringArray::from(vec![value]))
    }

    let plaintext = "native AES policy";
    for mode in [
        "AES_128_ECB",
        "AES_128_CFB128",
        "AES_128_CBC",
        "AES_128_GCM",
    ] {
        let mut data = vec![
            text(plaintext),
            text("secret"),
            text("1234567890123456"),
            text(mode),
        ];
        if mode == "AES_128_GCM" {
            data.push(text("associated"));
        }
        let (encrypted, errors) = call("aes_encrypt", &data);
        assert!(errors.is_empty());
        let output = encrypted.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(!output.is_null(0), "{mode}");
        data[0] = Arc::clone(&encrypted);
        let (utf8, errors) = call("aes_decrypt", &data);
        assert!(errors.is_empty());
        assert_eq!(
            utf8.as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            plaintext,
            "{mode}"
        );

        let raw = output
            .value(0)
            .chars()
            .map(|character| u8::try_from(character).unwrap())
            .collect::<Vec<_>>();
        data[0] = Arc::new(BinaryArray::from(vec![raw.as_slice()]));
        let (binary, errors) = call("aes_decrypt", &data);
        assert!(errors.is_empty());
        assert_eq!(
            binary
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            plaintext,
            "{mode}"
        );

        data[0] = text("invalid ciphertext Ā");
        let (invalid, errors) = call("aes_decrypt", &data);
        assert!(errors.is_empty());
        assert!(invalid.is_null(0));
    }
    let unsupported = [
        text("a"),
        text("key"),
        text(""),
        text("AES_128_ECB"),
        text("aad"),
    ];
    let (refused, errors) = call("aes_encrypt", &unsupported);
    assert!(refused.is_null(0));
    assert_eq!(errors[0].selected_ordinal(), 0);
    assert!(
        errors[0]
            .message()
            .contains("requires GCM mode to use AAD parameter")
    );
}
