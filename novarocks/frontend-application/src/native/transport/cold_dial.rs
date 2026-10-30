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

//! Original selected cache generation and nonholding cold-dial transfer.
//!
//! This module supplies the real physical carrier for the same-ticket Joint
//! local offer. It never constructs a Joint grant or a new owner. The original
//! stable caller must accept both the offer and this handoff before polling
//! the network future below.

use super::*;
use novarocks_native_adapter::native_lane::FrontendDialReservation;
use novarocks_native_trust::NativeEndpointConnector;
use tokio::sync::OwnedMutexGuard;

use super::super::data_runtime::NativeDialReservation;

pub(in crate::native) enum NativeChannelSource {
    Ready(CachedNativeChannel),
    Cold(Box<SelectedNativeDial>),
}

pub(in crate::native) struct SelectedNativeDial {
    reservation: NativeDialReservation,
    connector: Option<NativeEndpointConnector>,
    endpoint: tonic::transport::Endpoint,
}

pub(in crate::native) enum NativeDialHandoff {
    Ready(CachedNativeChannel),
    Reserved(Box<ReservedNativeDial>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::native) enum NativeDialAttempt {
    Pending,
    Installed,
}

/// All initial local positions, retained before the first network poll.
/// Physical/connector fields precede the original lane/cache custody, so an
/// unused handoff drops original physical positions before releasing its lane.
pub(in crate::native) struct ReservedNativeDial {
    dial: Option<FrontendDialReservation>,
    connector: Option<NativeEndpointConnector>,
    endpoint: tonic::transport::Endpoint,
    _lane: OwnedMutexGuard<()>,
    reservation: Option<NativeDialReservation>,
}

impl Client {
    /// Select once using the existing rotation/election, before a local offer.
    /// Configuration is from this same original Native transport; no network,
    /// trust fallback, new pool, or stage authority is created here.
    pub(in crate::native) fn prepare_channel_source(
        &self,
        method: NativeRpcMethod,
    ) -> Result<NativeChannelSource, ChannelAcquisitionError> {
        let key = self.channel_key(method)?;
        let slot = self
            .data_runtime
            .select_slot(&key)
            .map_err(ChannelAcquisitionError::retryable_network)?;
        if let Some(acquired) = self.data_runtime.cached_channel(&slot) {
            return Ok(NativeChannelSource::Ready(acquired));
        }
        let reservation = self
            .data_runtime
            .begin_dial(slot)
            .map_err(ChannelAcquisitionError::retryable_network)?;
        let endpoint = key.endpoint.clone();
        let connector = self
            .data_runtime
            .native_transport()
            .connector_for(endpoint.clone())
            .map_err(|error| {
                ChannelAcquisitionError::fatal(format!(
                    "construct Native endpoint connector failed: {error}",
                ))
            })?;
        // Preserve the existing typed endpoint, origin and public settings.
        let origin = format!("http://{endpoint}");
        let endpoint = configure_native_endpoint(
            tonic::transport::Endpoint::from_shared(origin).map_err(|error| {
                ChannelAcquisitionError::fatal(format!(
                    "construct Native client origin failed: {error}",
                ))
            })?,
        )
        .timeout(Duration::from_secs(600));
        Ok(NativeChannelSource::Cold(Box::new(SelectedNativeDial {
            reservation,
            connector: Some(connector),
            endpoint,
        })))
    }
}

impl NativeChannelSource {
    /// Original stable slots are borrowed. No source is removed until the
    /// matching receiver can own it; every foreign try/Drop is lock-free with
    /// respect to workload State, cache, subscription Core and owner locks.
    /// `Pending` installs no durable waiter: the same finite service rechecks.
    pub(in crate::native) fn try_reserve_in(
        source: &mut Option<Self>,
        target: &mut Option<NativeDialHandoff>,
    ) -> Result<NativeDialAttempt, ChannelAcquisitionError> {
        if target.is_some() {
            return Err(ChannelAcquisitionError::fatal(
                "Native dial handoff receiver is occupied",
            ));
        }
        let current = source.as_ref().ok_or_else(|| {
            ChannelAcquisitionError::fatal("Native dial handoff source is absent")
        })?;
        match current {
            Self::Ready(acquired) => {
                if acquired.is_retired() {
                    return Err(ChannelAcquisitionError::retryable_network(
                        "selected Native channel generation retired",
                    ));
                }
                let Some(Self::Ready(acquired)) = source.take() else {
                    unreachable!("the borrowed source variant cannot change");
                };
                *target = Some(NativeDialHandoff::Ready(acquired));
                Ok(NativeDialAttempt::Installed)
            }
            Self::Cold(selected) => {
                if let Some(acquired) = selected
                    .reservation
                    .ready_channel()
                    .map_err(ChannelAcquisitionError::retryable_network)?
                {
                    *target = Some(NativeDialHandoff::Ready(acquired));
                    drop(source.take());
                    return Ok(NativeDialAttempt::Installed);
                }
                if !selected.reservation.is_leader() {
                    // This exact generation already has its original producer.
                    // A follower never silently starts a replacement dial.
                    return Ok(NativeDialAttempt::Pending);
                }
                let runtime = selected.reservation.data_runtime();
                let lane = selected.reservation.lane();
                let Some(lane_position) = runtime.try_dial_lane(lane) else {
                    return Ok(NativeDialAttempt::Pending);
                };
                let dial = match runtime
                    .transport_admission()
                    .try_reserve_frontend_dial(lane)
                {
                    Ok(dial) => dial,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        drop(lane_position);
                        return Ok(NativeDialAttempt::Pending);
                    }
                    Err(error) => {
                        drop(lane_position);
                        return Err(ChannelAcquisitionError::retryable_network(format!(
                            "Native dial reservation refused: {error}",
                        )));
                    }
                };
                // Recheck after foreign admission callbacks. Close/retirement
                // must not turn a stale elected source into another slot.
                if let Some(acquired) = selected
                    .reservation
                    .ready_channel()
                    .map_err(ChannelAcquisitionError::retryable_network)?
                {
                    *target = Some(NativeDialHandoff::Ready(acquired));
                    drop(dial);
                    drop(lane_position);
                    drop(source.take());
                    return Ok(NativeDialAttempt::Installed);
                }
                let Some(Self::Cold(selected)) = source.take() else {
                    unreachable!("the borrowed source variant cannot change");
                };
                let SelectedNativeDial {
                    reservation,
                    connector,
                    endpoint,
                } = *selected;
                *target = Some(NativeDialHandoff::Reserved(Box::new(ReservedNativeDial {
                    dial: Some(dial),
                    connector,
                    endpoint,
                    _lane: lane_position,
                    reservation: Some(reservation),
                })));
                Ok(NativeDialAttempt::Installed)
            }
        }
    }
}

impl ReservedNativeDial {
    /// Poll only after this carrier AND the original same-ticket Joint stage
    /// have entered the original stable input. Keep this actual F through
    /// cancellation/cleanup; its Drop does not prove a spawned driver exit.
    /// The target is filled before releasing the original lane guard (wake).
    pub(in crate::native) async fn connect_in(
        mut self: Box<Self>,
        target: &mut Option<CachedNativeChannel>,
    ) -> Result<(), ChannelAcquisitionError> {
        if target.is_some() {
            return Err(ChannelAcquisitionError::fatal(
                "Native channel receiver is occupied",
            ));
        }
        let reservation = self
            .reservation
            .as_ref()
            .expect("original cache election retained");
        // ready_channel also checks process close and the exact generation.
        if reservation
            .ready_channel()
            .map_err(ChannelAcquisitionError::retryable_network)?
            .is_some()
        {
            return Err(ChannelAcquisitionError::retryable_network(
                "selected cold Native channel became ready before connect",
            ));
        }
        let lane = reservation.lane();
        let admission = reservation.data_runtime().transport_admission().clone();
        let connector = self
            .dial
            .take()
            .expect("original dial retained")
            .into_connector(self.connector.take().expect("original connector retained"));
        // Endpoint consumes the nonClone service without reacquiring its first
        // physical/handshake positions. H2 setup runs with the original lane
        // guard and the caller's same stage still held.
        let created = self
            .endpoint
            .connect_with_connector(connector)
            .await
            .map_err(ChannelAcquisitionError::failed_connection)?;
        let channel = NativeLaneChannel::new(created, NativeLane::frontend(lane), Some(&admission));
        let acquired = self
            .reservation
            .take()
            .expect("original cache election retained")
            .publish(channel)
            .map_err(ChannelAcquisitionError::retryable_network)?;
        *target = Some(acquired);
        Ok(())
    }
}

#[cfg(test)]
#[path = "cold_dial_tests.rs"]
mod tests;
