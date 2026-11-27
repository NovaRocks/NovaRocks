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
fn run(
    pool: &ConstantPool,
    control: &Control,
) -> Result<(Vec<u8>, FlatPoolWriteFacts), TypeCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let mut last = None;
    let mut admit = |facts: &FlatPoolWriteFacts| {
        last = Some(*facts);
        Ok(())
    };
    let token = prepare_flat_pool_write_in(pool, invoice(pool), limits(), &mut admit, &mut work)?;
    let facts = *token.facts();
    let bytes = token.emit_in(&mut admit, &mut work)?;
    assert_eq!(last, Some(facts));
    work.finish()?;
    Ok((bytes, facts))
}
fn controller(at: Option<(usize, CompileControlError)>) -> Control {
    Control {
        phase: CompilePhase::Validate,
        refusal: at,
        trace: Mutex::new(Vec::new()),
    }
}
#[test]
fn parent_flat_same_source_stream_and_every_real_prefix() {
    let source = pool(Arc::new(Int32Array::from(vec![Some(7), None, Some(-9)])));
    let control = controller(None);
    let (bytes, facts) = run(&source, &control).unwrap();
    let plain = encode_flat_pool(
        &source,
        invoice(&source),
        limits(),
        &Control::good(CompilePhase::Encode),
    )
    .unwrap();
    assert_eq!(bytes, plain);
    compare_standard_batch(&bytes, &standard(&source), source.field().data_type());
    let batch = standard_read(&bytes, source.field_ref());
    assert_eq!(batch.schema().field(0), source.field());
    assert_eq!(batch.num_rows(), 3);
    let column = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(column.value(0), 7);
    assert!(column.is_null(1));
    assert_eq!(column.value(2), -9);
    assert_eq!(facts.rows, 3);
    assert_eq!(facts.buffer_descriptors, 2);
    let baseline = control.trace();
    assert!(baseline.iter().all(|(p, _)| *p == CompilePhase::Validate));
    for at in 0..baseline.len() {
        for cause in CAUSES {
            let control = controller(Some((at, cause.clone())));
            assert!(
                matches!(run(&source,&control),Err(TypeCodecError::Control(actual))if actual==cause)
            );
            assert_eq!(control.trace(), baseline[..=at]);
        }
    }
}
#[test]
fn parent_flat_known_header_and_emit_refusals_precede_late_control() {
    let source = pool(Arc::new(Int32Array::from(vec![7])));
    for cause in CAUSES {
        let control = controller(Some((1, cause)));
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        for _ in 0..255 {
            work.step().unwrap();
        }
        let mut l = limits();
        l.max_rows = 0;
        assert!(matches!(
            prepare_flat_pool_write_in(&source, invoice(&source), l, &mut |_| Ok(()), &mut work),
            Err(TypeCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(control.trace(), [(CompilePhase::Validate, 0)]);
    }
    let control = controller(None);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let token = prepare_flat_pool_write_in(
        &source,
        invoice(&source),
        limits(),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    let before = control.trace();
    assert!(matches!(
        token.emit_in(
            &mut |_| Err(CompileControlError::ResourceExhausted),
            &mut work
        ),
        Err(TypeCodecError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    assert_eq!(control.trace(), before);
}
#[test]
fn parent_flat_batch_request_gate_and_foreign_controller() {
    let array: ArrayRef = Arc::new(Int32Array::from(vec![7]));
    let field = Arc::new(Field::new("original", DataType::Int32, true).with_metadata(
        HashMap::from([("original-key".into(), "payload-value".into())]),
    ));
    let source = pool_with(array, field, FunctionValueType::new(DataType::Int32, true));
    let control = controller(None);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let mut l = limits();
    l.max_new_allocation_request_bytes = 0;
    assert!(matches!(
        prepare_flat_pool_write_in(&source, invoice(&source), l, &mut |_| Ok(()), &mut work),
        Err(TypeCodecError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    let control = controller(None);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let token = prepare_flat_pool_write_in(
        &source,
        invoice(&source),
        limits(),
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    let foreign = controller(None);
    let mut other = CompileCheckpoints::try_new(&foreign, CompilePhase::Validate).unwrap();
    let before = foreign.trace();
    assert!(matches!(
        token.emit_in(&mut |_| Ok(()), &mut other),
        Err(TypeCodecError::InvalidShape(
            "flat pool writer belongs to another control"
        ))
    ));
    assert_eq!(foreign.trace(), before);
}
