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

pub(crate) mod compat;
pub(crate) mod decode;
pub(crate) mod thrift_codec;
pub(crate) mod type_mapping;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::thrift::data_sinks::TPlanFragmentDestination;
    use crate::thrift::internal_service::{
        TPlanFragmentExecParams, TQueryOptions, TScanRangeParams, TSpillMode, TSpillOptions,
    };
    use crate::thrift::plan_nodes::TScanRange;
    use crate::thrift::types::{TNetworkAddress, TUniqueId};
    use novarocks::exec::spill::SpillMode;
    use novarocks::protocol::{FieldPath, ProtocolErrorKind, ProtocolFamily};

    use super::compat::request::backfill_per_node_scan_ranges;
    use super::compat::sink::select_partition_boundary_key;
    use super::decode::{decode_fragment_destination, decode_query_options};

    fn scan_range(empty: bool, volume_id: i32) -> TScanRangeParams {
        TScanRangeParams::new(
            TScanRange::default(),
            Some(volume_id),
            Some(empty),
            None::<bool>,
        )
    }

    fn exec_params(
        per_node_scan_ranges: BTreeMap<i32, Vec<TScanRangeParams>>,
        per_driver: BTreeMap<i32, BTreeMap<i32, Vec<TScanRangeParams>>>,
    ) -> TPlanFragmentExecParams {
        TPlanFragmentExecParams::new(
            TUniqueId { hi: 1, lo: 2 },
            TUniqueId { hi: 3, lo: 4 },
            per_node_scan_ranges,
            BTreeMap::new(),
            None::<Vec<TPlanFragmentDestination>>,
            None::<i32>,
            None::<i32>,
            None::<bool>,
            None::<bool>,
            None::<i32>,
            None::<bool>,
            Some(per_driver),
            None::<bool>,
            None::<i32>,
            None::<bool>,
            None::<Vec<crate::thrift::internal_service::TExecDebugOption>>,
            None::<BTreeMap<i32, i32>>,
            None::<BTreeMap<i32, crate::thrift::descriptors::TNodesInfo>>,
        )
    }

    #[test]
    fn query_option_aliases_use_documented_new_field_precedence() {
        let wire = TQueryOptions {
            mem_limit: Some(1024),
            query_mem_limit: Some(2048),
            io_tasks_per_scan_operator: Some(3),
            connector_io_tasks_per_scan_operator: Some(7),
            enable_spill: Some(true),
            spill_mode: Some(TSpillMode::AUTO),
            spill_mem_table_size: Some(4),
            spill_options: Some(TSpillOptions {
                spill_mode: Some(TSpillMode::FORCE),
                spill_mem_table_size: Some(8),
                ..Default::default()
            }),
            ..Default::default()
        };

        let decoded = decode_query_options(Some(&wire)).expect("decode query options");

        assert_eq!(decoded.exec_mem_limit(), Some(2048));
        assert_eq!(decoded.connector_io_tasks_per_scan_operator(), Some(7));
        let spill = decoded.spill().expect("spill config");
        assert_eq!(spill.spill_mode, SpillMode::Force);
        assert_eq!(spill.spill_mem_table_size, Some(8));
    }

    #[test]
    fn query_option_aliases_fall_back_only_when_current_fields_are_absent() {
        let wire = TQueryOptions {
            mem_limit: Some(1024),
            io_tasks_per_scan_operator: Some(3),
            enable_spill: Some(true),
            spill_mode: Some(TSpillMode::AUTO),
            spill_mem_table_size: Some(4),
            ..Default::default()
        };

        let decoded = decode_query_options(Some(&wire)).expect("decode query options");

        assert_eq!(decoded.exec_mem_limit(), Some(1024));
        assert_eq!(decoded.connector_io_tasks_per_scan_operator(), Some(3));
        let spill = decoded.spill().expect("spill config");
        assert_eq!(spill.spill_mode, SpillMode::Auto);
        assert_eq!(spill.spill_mem_table_size, Some(4));
    }

    #[test]
    fn current_endpoint_wins_and_missing_both_is_rejected() {
        let current = TNetworkAddress::new("current-be".to_string(), 8060);
        let legacy = TNetworkAddress::new("legacy-be".to_string(), 9060);
        let destination = TPlanFragmentDestination::new(
            TUniqueId { hi: 5, lo: 6 },
            Some(legacy.clone()),
            Some(current),
            None::<i32>,
        );

        let decoded = decode_fragment_destination(
            &destination,
            FieldPath::root("exec_plan_fragment")
                .field("params")
                .field("destinations")
                .index(0),
        )
        .expect("decode destination");
        assert_eq!(decoded.endpoint().host(), "current-be");

        let legacy_only = TPlanFragmentDestination::new(
            TUniqueId { hi: 5, lo: 6 },
            Some(legacy),
            None::<TNetworkAddress>,
            None::<i32>,
        );
        let decoded = decode_fragment_destination(
            &legacy_only,
            FieldPath::root("exec_plan_fragment")
                .field("params")
                .field("destinations")
                .index(0),
        )
        .expect("decode legacy destination");
        assert_eq!(decoded.endpoint().host(), "legacy-be");

        let missing = TPlanFragmentDestination::new(
            TUniqueId { hi: 5, lo: 6 },
            None::<TNetworkAddress>,
            None::<TNetworkAddress>,
            None::<i32>,
        );
        let error = decode_fragment_destination(
            &missing,
            FieldPath::root("exec_plan_fragment")
                .field("params")
                .field("destinations")
                .index(0),
        )
        .expect_err("missing destination address");
        let protocol = error.protocol().expect("protocol error");
        assert_eq!(protocol.family(), ProtocolFamily::StarRocks);
        assert_eq!(protocol.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(
            protocol.path().to_string(),
            "exec_plan_fragment.params.destinations[0].brpc_server"
        );
    }

    #[test]
    fn legacy_endpoint_validation_reports_the_legacy_field_path() {
        let destination = TPlanFragmentDestination::new(
            TUniqueId { hi: 5, lo: 6 },
            Some(TNetworkAddress::new("legacy-be".to_string(), 0)),
            None::<TNetworkAddress>,
            None::<i32>,
        );

        let error = decode_fragment_destination(
            &destination,
            FieldPath::root("exec_plan_fragment")
                .field("params")
                .field("destinations")
                .index(0),
        )
        .expect_err("invalid legacy destination");

        assert_eq!(
            error.protocol().expect("protocol error").path().to_string(),
            "exec_plan_fragment.params.destinations[0].deprecated_server.port"
        );
    }

    #[test]
    fn per_driver_ranges_fill_only_missing_or_placeholder_node_ranges() {
        let concrete = scan_range(false, 11);
        let replacement = scan_range(false, 22);
        let mut params = exec_params(
            BTreeMap::from([(1, vec![concrete.clone()]), (2, vec![scan_range(true, 12)])]),
            BTreeMap::from([
                (1, BTreeMap::from([(0, vec![scan_range(false, 21)])])),
                (2, BTreeMap::from([(0, vec![replacement.clone()])])),
                (3, BTreeMap::from([(0, vec![scan_range(false, 23)])])),
            ]),
        );

        backfill_per_node_scan_ranges(&mut params);

        assert_eq!(params.per_node_scan_ranges[&1], vec![concrete]);
        assert_eq!(params.per_node_scan_ranges[&2], vec![replacement]);
        assert_eq!(params.per_node_scan_ranges[&3][0].volume_id, Some(23));
    }

    #[test]
    fn partition_boundary_current_key_wins_and_legacy_is_absence_fallback() {
        let current = [10, 11];
        let legacy = 20;
        assert_eq!(
            select_partition_boundary_key(Some(current.as_slice()), Some(&legacy)),
            Some(current.as_slice())
        );
        assert_eq!(
            select_partition_boundary_key::<i32>(Some(&[]), Some(&legacy)),
            Some([].as_slice())
        );
        assert_eq!(
            select_partition_boundary_key(None, Some(&legacy)),
            Some(std::slice::from_ref(&legacy))
        );
        assert_eq!(select_partition_boundary_key::<i32>(None, None), None);
    }
}
