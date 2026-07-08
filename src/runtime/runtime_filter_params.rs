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

use std::collections::{BTreeMap, HashMap};

use crate::common::types::UniqueId;
use crate::proto::{common, novarocks};
use crate::runtime::endpoint::{RuntimeEndpoint, RuntimeFilterProberDestination};
use crate::runtime::runtime_filter_worker::{RuntimeFilterProberTarget, RuntimeFilterWorkerParams};

#[cfg(feature = "compat")]
use crate::thrift::{runtime_filter, types};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct RuntimeFilterParams {
    id_to_prober_params: BTreeMap<i32, Vec<RuntimeFilterProberDestination>>,
    runtime_filter_builder_number: BTreeMap<i32, i32>,
    runtime_filter_max_size: Option<i64>,
}

#[cfg(test)]
#[cfg(feature = "compat")]
mod compat_tests {
    use std::collections::BTreeMap;

    use crate::thrift::{runtime_filter, types};

    use super::RuntimeFilterParams;

    #[test]
    fn thrift_runtime_filter_params_convert_to_native_and_back_for_compat_boundary() {
        let thrift = runtime_filter::TRuntimeFilterParams {
            id_to_prober_params: Some(BTreeMap::from([(
                11,
                vec![runtime_filter::TRuntimeFilterProberParams {
                    fragment_instance_id: Some(types::TUniqueId::new(3, 4)),
                    fragment_instance_address: Some(types::TNetworkAddress::new(
                        "10.0.0.11".to_string(),
                        9060,
                    )),
                }],
            )])),
            runtime_filter_builder_number: Some(BTreeMap::from([(11, 2)])),
            runtime_filter_max_size: Some(4096),
            skew_join_runtime_filters: None,
        };

        let native = RuntimeFilterParams::from_thrift(&thrift).expect("from thrift");
        let projected = native.to_thrift();

        assert_eq!(projected.runtime_filter_max_size, Some(4096));
        assert_eq!(
            projected
                .runtime_filter_builder_number
                .as_ref()
                .and_then(|counts| counts.get(&11)),
            Some(&2)
        );
        let prober = &projected.id_to_prober_params.as_ref().unwrap()[&11][0];
        assert_eq!(
            prober.fragment_instance_id,
            Some(types::TUniqueId::new(3, 4))
        );
        assert_eq!(
            prober.fragment_instance_address,
            Some(types::TNetworkAddress::new("10.0.0.11".to_string(), 9060))
        );
    }

    #[test]
    fn thrift_runtime_filter_params_reject_missing_prober_fragment_instance_id() {
        let err = RuntimeFilterParams::from_thrift(&runtime_filter::TRuntimeFilterParams {
            id_to_prober_params: Some(BTreeMap::from([(
                29,
                vec![runtime_filter::TRuntimeFilterProberParams {
                    fragment_instance_id: None,
                    fragment_instance_address: Some(types::TNetworkAddress::new(
                        "10.0.0.29".to_string(),
                        9060,
                    )),
                }],
            )])),
            runtime_filter_builder_number: None,
            runtime_filter_max_size: None,
            skew_join_runtime_filters: None,
        })
        .expect_err("missing thrift fragment_instance_id");

        assert!(err.contains("id_to_prober_params[29][0]"), "{err}");
        assert!(err.contains("fragment_instance_id"), "{err}");
    }

    #[test]
    fn thrift_runtime_filter_params_reject_invalid_prober_endpoint() {
        let err = RuntimeFilterParams::from_thrift(&runtime_filter::TRuntimeFilterParams {
            id_to_prober_params: Some(BTreeMap::from([(
                31,
                vec![runtime_filter::TRuntimeFilterProberParams {
                    fragment_instance_id: Some(types::TUniqueId::new(1, 2)),
                    fragment_instance_address: Some(types::TNetworkAddress::new(
                        String::new(),
                        9060,
                    )),
                }],
            )])),
            runtime_filter_builder_number: None,
            runtime_filter_max_size: None,
            skew_join_runtime_filters: None,
        })
        .expect_err("invalid thrift endpoint");

        assert!(err.contains("id_to_prober_params[31][0]"), "{err}");
        assert!(err.contains("host must not be empty"), "{err}");
    }
}

impl RuntimeFilterParams {
    pub(crate) fn new(
        id_to_prober_params: BTreeMap<i32, Vec<RuntimeFilterProberDestination>>,
        runtime_filter_builder_number: BTreeMap<i32, i32>,
        runtime_filter_max_size: Option<i64>,
    ) -> Self {
        Self {
            id_to_prober_params,
            runtime_filter_builder_number,
            runtime_filter_max_size: runtime_filter_max_size.filter(|size| *size > 0),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.id_to_prober_params.is_empty()
            && self.runtime_filter_builder_number.is_empty()
            && self.runtime_filter_max_size.is_none()
    }

    pub(crate) fn id_to_prober_params(
        &self,
    ) -> &BTreeMap<i32, Vec<RuntimeFilterProberDestination>> {
        &self.id_to_prober_params
    }

    pub(crate) fn runtime_filter_builder_number(&self) -> &BTreeMap<i32, i32> {
        &self.runtime_filter_builder_number
    }

    pub(crate) fn runtime_filter_max_size(&self) -> Option<i64> {
        self.runtime_filter_max_size
    }

    pub(crate) fn from_native(src: &novarocks::RuntimeFilterParams) -> Result<Self, String> {
        let id_to_prober_params = src
            .id_to_prober_params
            .iter()
            .map(|(filter_id, list)| {
                let params = list
                    .params
                    .iter()
                    .map(prober_params_from_native)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((*filter_id, params))
            })
            .collect::<Result<BTreeMap<_, _>, String>>()?;
        let runtime_filter_builder_number = src
            .runtime_filter_builder_number
            .iter()
            .map(|(filter_id, count)| (*filter_id, *count))
            .collect::<BTreeMap<_, _>>();

        Ok(Self::new(
            id_to_prober_params,
            runtime_filter_builder_number,
            (src.runtime_filter_max_size > 0).then_some(src.runtime_filter_max_size),
        ))
    }

    pub(crate) fn to_native(&self) -> novarocks::RuntimeFilterParams {
        novarocks::RuntimeFilterParams {
            id_to_prober_params: self
                .id_to_prober_params
                .iter()
                .map(|(filter_id, params)| {
                    (
                        *filter_id,
                        novarocks::ProberParamsList {
                            params: params.iter().map(prober_params_to_native).collect(),
                        },
                    )
                })
                .collect::<HashMap<_, _>>(),
            runtime_filter_builder_number: self
                .runtime_filter_builder_number
                .iter()
                .map(|(filter_id, count)| (*filter_id, *count))
                .collect::<HashMap<_, _>>(),
            runtime_filter_max_size: self.runtime_filter_max_size.unwrap_or(0),
        }
    }

    #[cfg(feature = "compat")]
    pub(crate) fn from_thrift(src: &runtime_filter::TRuntimeFilterParams) -> Result<Self, String> {
        let id_to_prober_params = src
            .id_to_prober_params
            .as_ref()
            .map(|id_to_probers| {
                id_to_probers
                    .iter()
                    .map(|(filter_id, probers)| {
                        let destinations = probers
                            .iter()
                            .enumerate()
                            .map(|(idx, prober)| {
                                compat_adapters::prober_params_from_thrift(prober).map_err(|e| {
                                    format!("id_to_prober_params[{filter_id}][{idx}]: {e}")
                                })
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        Ok((*filter_id, destinations))
                    })
                    .collect::<Result<BTreeMap<_, _>, String>>()
            })
            .transpose()?
            .unwrap_or_default();
        let runtime_filter_builder_number = src
            .runtime_filter_builder_number
            .as_ref()
            .map(|counts| {
                counts
                    .iter()
                    .map(|(filter_id, count)| (*filter_id, *count))
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();

        Ok(Self::new(
            id_to_prober_params,
            runtime_filter_builder_number,
            src.runtime_filter_max_size,
        ))
    }

    #[cfg(feature = "compat")]
    pub(crate) fn to_thrift(&self) -> runtime_filter::TRuntimeFilterParams {
        let id_to_prober_params = self
            .id_to_prober_params
            .iter()
            .map(|(filter_id, probers)| {
                (
                    *filter_id,
                    probers
                        .iter()
                        .map(compat_adapters::prober_params_to_thrift)
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        runtime_filter::TRuntimeFilterParams::new(
            (!id_to_prober_params.is_empty()).then_some(id_to_prober_params),
            (!self.runtime_filter_builder_number.is_empty())
                .then_some(self.runtime_filter_builder_number.clone()),
            self.runtime_filter_max_size,
            None::<std::collections::BTreeSet<i32>>,
        )
    }

    pub(crate) fn to_worker_params(&self) -> RuntimeFilterWorkerParams {
        let id_to_prober_targets = self
            .id_to_prober_params
            .iter()
            .map(|(filter_id, probers)| {
                (
                    *filter_id,
                    probers
                        .iter()
                        .map(|prober| {
                            RuntimeFilterProberTarget::new(
                                prober.endpoint().host().to_string(),
                                prober.endpoint().port(),
                            )
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<HashMap<_, _>>();
        let runtime_filter_builder_number = self
            .runtime_filter_builder_number
            .iter()
            .map(|(filter_id, count)| (*filter_id, *count))
            .collect::<HashMap<_, _>>();
        RuntimeFilterWorkerParams::new(
            id_to_prober_targets,
            runtime_filter_builder_number,
            self.runtime_filter_max_size,
        )
    }
}

fn prober_params_from_native(
    src: &novarocks::ProberParams,
) -> Result<RuntimeFilterProberDestination, String> {
    let fragment_instance_id = src
        .fragment_instance_id
        .as_ref()
        .ok_or_else(|| "native ProberParams missing fragment_instance_id".to_string())?;
    Ok(RuntimeFilterProberDestination::new(
        UniqueId {
            hi: fragment_instance_id.hi,
            lo: fragment_instance_id.lo,
        },
        RuntimeEndpoint::parse(&src.endpoint)?,
    ))
}

fn prober_params_to_native(src: &RuntimeFilterProberDestination) -> novarocks::ProberParams {
    let fragment_instance_id = src.fragment_instance_id();
    novarocks::ProberParams {
        fragment_instance_id: Some(common::UniqueId {
            hi: fragment_instance_id.hi,
            lo: fragment_instance_id.lo,
        }),
        endpoint: src.endpoint().as_host_port(),
    }
}

#[cfg(feature = "compat")]
mod compat_adapters {
    use super::*;

    fn unique_id_from_thrift(src: &types::TUniqueId) -> UniqueId {
        UniqueId {
            hi: src.hi,
            lo: src.lo,
        }
    }

    fn unique_id_to_thrift(src: UniqueId) -> types::TUniqueId {
        types::TUniqueId::new(src.hi, src.lo)
    }

    pub(super) fn prober_params_from_thrift(
        src: &runtime_filter::TRuntimeFilterProberParams,
    ) -> Result<RuntimeFilterProberDestination, String> {
        let fragment_instance_id = src
            .fragment_instance_id
            .clone()
            .ok_or_else(|| "missing fragment_instance_id".to_string())?;
        let addr = src
            .fragment_instance_address
            .as_ref()
            .ok_or_else(|| "missing fragment_instance_address".to_string())?;
        let endpoint = RuntimeEndpoint::from_network_address(addr)?;
        Ok(RuntimeFilterProberDestination::new(
            unique_id_from_thrift(&fragment_instance_id),
            endpoint,
        ))
    }

    pub(super) fn prober_params_to_thrift(
        src: &RuntimeFilterProberDestination,
    ) -> runtime_filter::TRuntimeFilterProberParams {
        let fragment_instance_id = src.fragment_instance_id();
        runtime_filter::TRuntimeFilterProberParams::new(
            Some(unique_id_to_thrift(fragment_instance_id)),
            Some(src.endpoint().to_network_address()),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::common::types::UniqueId;
    use crate::proto::{common, novarocks};
    use crate::runtime::endpoint::{RuntimeEndpoint, RuntimeFilterProberDestination};

    use super::RuntimeFilterParams;

    fn destination(hi: i64, lo: i64, endpoint: &str) -> RuntimeFilterProberDestination {
        RuntimeFilterProberDestination::new(
            UniqueId { hi, lo },
            RuntimeEndpoint::parse(endpoint).expect("endpoint"),
        )
    }

    #[test]
    fn native_runtime_filter_params_round_trip_proto() {
        let params = RuntimeFilterParams::new(
            BTreeMap::from([(7, vec![destination(1, 2, "10.0.0.7:8060")])]),
            BTreeMap::from([(7, 3)]),
            Some(16 * 1024 * 1024),
        );

        let decoded = RuntimeFilterParams::from_native(&params.to_native()).unwrap();

        assert_eq!(decoded.runtime_filter_builder_number().get(&7), Some(&3));
        assert_eq!(decoded.runtime_filter_max_size(), Some(16 * 1024 * 1024));
        assert_eq!(
            decoded.id_to_prober_params()[&7][0]
                .endpoint()
                .as_host_port(),
            "10.0.0.7:8060"
        );
    }

    #[test]
    fn runtime_filter_worker_params_derive_from_native_destinations() {
        let params = RuntimeFilterParams::new(
            BTreeMap::from([(
                17,
                vec![
                    destination(5, 6, "10.0.0.17:8060"),
                    destination(7, 8, "10.0.0.18:8061"),
                ],
            )]),
            BTreeMap::from([(17, 4)]),
            Some(8192),
        );

        let worker = params.to_worker_params();
        let targets = worker.prober_targets(17).expect("targets");

        assert_eq!(worker.expected_builders(17), 4);
        assert_eq!(worker.runtime_filter_max_size(), Some(8192));
        assert_eq!(targets[0].hostname(), "10.0.0.17");
        assert_eq!(targets[0].port(), 8060);
        assert_eq!(targets[1].hostname(), "10.0.0.18");
        assert_eq!(targets[1].port(), 8061);
    }

    #[test]
    fn native_runtime_filter_params_reject_missing_prober_fragment_instance_id() {
        let err = RuntimeFilterParams::from_native(&novarocks::RuntimeFilterParams {
            id_to_prober_params: [(
                19,
                novarocks::ProberParamsList {
                    params: vec![novarocks::ProberParams {
                        fragment_instance_id: None,
                        endpoint: "10.0.0.19:8060".to_string(),
                    }],
                },
            )]
            .into_iter()
            .collect(),
            runtime_filter_builder_number: BTreeMap::new().into_iter().collect(),
            runtime_filter_max_size: 0,
        })
        .expect_err("missing fragment_instance_id");

        assert!(err.contains("fragment_instance_id"), "{err}");
    }

    #[test]
    fn native_runtime_filter_params_reject_invalid_prober_endpoint() {
        let err = RuntimeFilterParams::from_native(&novarocks::RuntimeFilterParams {
            id_to_prober_params: [(
                23,
                novarocks::ProberParamsList {
                    params: vec![novarocks::ProberParams {
                        fragment_instance_id: Some(common::UniqueId { hi: 1, lo: 2 }),
                        endpoint: String::new(),
                    }],
                },
            )]
            .into_iter()
            .collect(),
            runtime_filter_builder_number: BTreeMap::new().into_iter().collect(),
            runtime_filter_max_size: -1,
        })
        .expect_err("invalid endpoint");

        assert!(err.contains("host:port"), "{err}");
    }
}
