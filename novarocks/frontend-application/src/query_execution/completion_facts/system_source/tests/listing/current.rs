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

//! The statement Source borrows the original Current capture and its actual
//! metadata CPU exit. Generation selection is explicit test input; no second
//! Root, metadata runtime, result window or provider creator is installed.
use super::*;
use crate::mv::domain::staged_create::admitted_current_tests::CurrentCaptureFixture;
use novarocks_mv_application::persistence::identity::{ObjectIdentity, SchemaVersion};

fn occurrence(provider: &CurrentCaptureFixture) -> [RelationOccurrence; 1] {
    let table = provider.table();
    [RelationOccurrence {
        occurrence_id: 1,
        catalog_at_binding: table.instance_id.as_str().into(),
        namespace_at_binding: table.namespace.to_string(),
        relation_at_binding: table.table.to_string(),
        qualifier_at_binding: "source".into(),
        object_id: ObjectIdentity::try_new(vec![1]).unwrap(),
        schema_version: SchemaVersion::try_new(vec![1]).unwrap(),
        fields: vec![],
    }]
}
async fn fixture(provider: &CurrentCaptureFixture, class: ResultWindowClass) -> Fixture {
    Fixture::new_with_pin(
        provider.pin(),
        class,
        Arc::new(
            novarocks_query_application::system_tables::SystemTableRegistry::try_new([]).unwrap(),
        ),
        ResultCapacityConfig::V1,
        Duration::from_secs(5),
    )
    .await
}
#[tokio::test]
async fn cancelled_current_borrow_keeps_capture_and_original_provider_cause() {
    for class in [ResultWindowClass::Client, ResultWindowClass::Local] {
        let provider = CurrentCaptureFixture::new();
        let original = ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "original Current capture refusal",
        );
        let release = provider.hold_capture(original.clone());
        let fixture = fixture(&provider, class).await;
        let source = fixture.source();
        let occurrences = occurrence(&provider);
        let mut borrowed =
            Box::pin(source.current_sources(&occurrences, &source.scope.connector_context));
        assert!(futures::poll!(borrowed.as_mut()).is_pending());
        assert_eq!(provider.events(), ["async-capture"]);
        drop(borrowed);
        fixture.root.owner.cancel(CancellationReason::ExplicitKill {
            requester_session: 31,
        });
        for _ in 0..2 {
            let mut close = Box::pin(source.close());
            assert!(futures::poll!(close.as_mut()).is_pending());
            drop(close);
        }
        assert!(source.io.lock().unwrap().future.is_some());
        assert!(!source.io.lock().unwrap().joined);
        assert!(source.local_health_failure().is_none());
        release.send(()).unwrap();
        let Response::Current(Err(SystemTableError::Provider(error))) = source.receive().await
        else {
            panic!("original Current error response");
        };
        assert_eq!(error, original);
        assert_eq!(provider.events(), ["async-capture"]);
        drop(source);
        fixture.finish(true).await;
    }
}
#[tokio::test]
async fn current_result_waits_for_actual_cpu_tail_before_source_close_and_window_retirement() {
    for class in [ResultWindowClass::Client, ResultWindowClass::Local] {
        let provider = CurrentCaptureFixture::new();
        let (tail, entered) = provider.hold_cpu_tail();
        let mut fixture = fixture(&provider, class).await;
        let source = fixture.source();
        let occurrences = occurrence(&provider);
        let mut borrowed =
            Box::pin(source.current_sources(&occurrences, &source.scope.connector_context));
        tokio::time::timeout(Duration::from_secs(5), driven(&fixture.control, &mut fixture.driver, async {
            tokio::select! {
                result = &mut borrowed => panic!("Current handed off before actual CPU tail: {}", result.is_ok()),
                result = entered => result.unwrap(),
            }
        })).await.unwrap();
        assert_eq!(tail.calls(), 1);
        assert_eq!(tail.source_drops(), 0);
        drop(borrowed);
        fixture.root.owner.cancel(CancellationReason::ExplicitKill {
            requester_session: 37,
        });
        for _ in 0..2 {
            let mut close = Box::pin(source.close());
            assert!(futures::poll!(close.as_mut()).is_pending());
            drop(close);
        }
        assert_eq!(tail.source_drops(), 0);
        assert!(!source.io.lock().unwrap().pins.is_empty());
        assert!(!source.io.lock().unwrap().joined);
        assert!(source.local_health_failure().is_none());
        assert_eq!(
            fixture
                .control
                .snapshot()
                .result_windows
                .held_positions
                .iter()
                .sum::<usize>(),
            1
        );
        tail.release();
        tokio::time::timeout(
            Duration::from_secs(5),
            driven(&fixture.control, &mut fixture.driver, source.close()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(tail.calls(), 1);
        assert_eq!(tail.source_drops(), 1);
        assert!(source.io.lock().unwrap().pins.is_empty());
        assert!(source.io.lock().unwrap().joined);
        assert!(source.local_health_failure().is_none());
        drop(source);
        fixture.finish(true).await;
    }
}
