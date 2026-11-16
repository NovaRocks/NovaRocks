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
use arrow::datatypes::{DataType, Field, Schema};
use novarocks_connector_contract::ConnectorRowMutationEffect;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

struct Keys {
    fail: bool,
}

impl ConnectorPartitionKeyEvaluator for Keys {
    fn partition_keys(&self, inputs: &RecordBatch) -> Result<Int64Array, ConnectorError> {
        assert_eq!(inputs.num_columns(), 1);
        if self.fail {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "test transform",
            ));
        }
        Ok(inputs
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone())
    }
}

#[test]
fn evaluates_only_the_original_data_route_and_propagates_provider_refusal() {
    let writers = NonZeroU32::new(3).unwrap();
    let fixture = |effect: ConnectorRowMutationEffect| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("effect", DataType::Int8, false),
                Field::new("partition", DataType::Int64, false),
            ])),
            vec![
                Arc::new(Int8Array::from_value(effect as i8, 2)),
                Arc::new(Int64Array::from(vec![13, -42])),
            ],
        )
        .unwrap()
    };
    let mut projection = KeyProjection {
        evaluator: Arc::new(Keys { fail: false }),
        input_ordinals: Arc::from([1]),
        effects: Some((0, Arc::from([ConnectorRowMutationEffect::Insert]))),
        spread: Arc::new(WriteSpreadState::new(writers)),
    };
    let mut driver = WriteSpreadDriver::new();
    assert_eq!(
        projection
            .evaluate(&fixture(ConnectorRowMutationEffect::Insert), &mut driver)
            .unwrap()
            .values(),
        &[13, -42],
    );
    projection.evaluator = Arc::new(Keys { fail: true });
    assert_eq!(
        projection
            .evaluate(&fixture(ConnectorRowMutationEffect::Delete), &mut driver)
            .unwrap()
            .values(),
        &[0, 0],
    );
    let failure = projection
        .evaluate(&fixture(ConnectorRowMutationEffect::Insert), &mut driver)
        .unwrap_err();
    assert!(
        matches!(failure.cause(), crate::runtime::task::ExecutionFailureCause::Connector(cause)
        if cause.cause().kind() == ConnectorErrorKind::Unsupported)
    );
}
