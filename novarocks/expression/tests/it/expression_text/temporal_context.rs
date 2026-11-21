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
use novarocks_type_contract::{StatementStartUtc, TimeZoneKey};

fn statement_parameters() -> SemanticParameters {
    SemanticParameters::try_new([
        (
            ALLOW_THROW.id,
            SemanticParameterValue::AllowThrowException(true),
        ),
        (
            SemanticParameterId::new(11),
            SemanticParameterValue::StatementStartUtc(
                StatementStartUtc::try_new(1_704_067_200, 123_456_789).unwrap(),
            ),
        ),
        (
            SemanticParameterId::new(21),
            SemanticParameterValue::TimeZone(TimeZoneKey::parse("America/New_York").unwrap()),
        ),
    ])
    .unwrap()
}

fn assert_text(sql: &str, expected: &str, context: &TextContext<'_>) {
    let output = constant(sql, context);
    let strings = output
        .batch
        .values
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(strings.value(0), expected, "{sql}");
}

#[test]
fn temporal_selected_calls_consume_only_their_frozen_statement_time_facts() {
    let functions = fixture();
    let parameters = statement_parameters();
    let context = context(&functions, &parameters);
    assert_text(
        "CAST(CURRENT_TIMESTAMP(12) AS VARCHAR)",
        "2023-12-31 19:00:00.123456789000 America/New_York",
        &context,
    );
    assert_text(
        "CAST(LOCALTIMESTAMP(6) AS VARCHAR)",
        "2023-12-31 19:00:00.123457",
        &context,
    );
    assert_text(
        "CAST(TIMESTAMP '2024-03-10 02:30:00' AT LOCAL AS VARCHAR)",
        "2024-03-10 03:30:00 America/New_York",
        &context,
    );
    assert_text(
        "CAST(TIME '23:45:00 +14:00' AT TIME ZONE INTERVAL '-5:30' HOUR TO MINUTE AS VARCHAR)",
        "04:15:00-05:30",
        &context,
    );
    assert_text(
        "CAST(TIMESTAMP '1969-12-31 23:59:59.999999999999' AT TIME ZONE 'Asia/Shanghai' AS VARCHAR)",
        "1970-01-01 12:59:59.999999999999 Asia/Shanghai",
        &context,
    );

    let typed = compile("CURRENT_TIMESTAMP(12)", &[], &context);
    assert_eq!(
        typed
            .environment()
            .parameters
            .entries()
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        [SemanticParameterId::new(11), SemanticParameterId::new(21)]
    );
}

#[test]
fn temporal_cast_rounds_at_the_selected_precision_and_try_keeps_input_errors() {
    let functions = fixture();
    let parameters = statement_parameters();
    let context = context(&functions, &parameters);
    assert_text(
        "CAST(CAST(TIMESTAMP '1969-12-31 23:59:59.9999995' AS TIMESTAMP(6)) AS VARCHAR)",
        "1970-01-01 00:00:00.000000",
        &context,
    );
    assert_text(
        "CAST(CAST('2024-03-10 02:30:00 America/New_York' AS TIMESTAMP(3) WITH TIME ZONE) AS VARCHAR)",
        "2024-03-10 03:30:00.000 America/New_York",
        &context,
    );

    let result = constant("TRY_CAST('invalid' AS TIMESTAMP(12))", &context);
    assert!(result.batch.values.is_null(0));
    let source = [column("text", 5, DataType::Utf8, true)];
    let input = batch(
        &source,
        vec![Arc::new(StringArray::from(vec![
            Some("invalid"),
            Some("1969-12-31 23:59:59.9999995"),
            None,
        ]))],
    );
    let result = evaluate(
        &compile("TRY_CAST(text AS TIMESTAMP(6))", &source, &context),
        &input,
    );
    assert_compared(&result, 1, 3);
    assert!(result.batch.errors.is_empty());
    assert!(result.batch.values.is_null(0));
    assert!(result.batch.values.is_null(2));

    let result = evaluate(
        &compile(
            "TRY_CAST(CAST(text AS TIMESTAMP(12)) AS TIMESTAMP(3))",
            &source,
            &context,
        ),
        &input,
    );
    assert_eq!(result.batch.errors.len(), 1);
    assert_eq!(result.batch.errors[0].selected_ordinal(), 0);
}
