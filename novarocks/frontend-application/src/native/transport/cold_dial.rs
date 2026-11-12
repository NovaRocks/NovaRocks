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

use super::super::data_runtime::NativeDialReservation;

pub(crate) enum NativeChannelSource {
    Ready(CachedNativeChannel),
    Cold(Box<SelectedNativeDial>),
}

pub(crate) struct SelectedNativeDial {
    reservation: NativeDialReservation,
    connector: Option<NativeEndpointConnector>,
    endpoint: tonic::transport::Endpoint,
    readiness: Option<NativePhysicalReadyWait>,
}

pub(crate) enum NativeDialHandoff {
    Ready(CachedNativeChannel),
    Reserved(Box<ReservedNativeDial>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeDialAttempt {
    Pending,
    Installed,
}

/// All initial local positions, retained before the first network poll.
/// Physical/connector fields precede the original lane/cache custody, so an
/// unused handoff drops original physical positions before releasing its lane.
pub(crate) struct ReservedNativeDial {
    dial: Option<FrontendDialReservation>,
    connector: Option<NativeEndpointConnector>,
    endpoint: tonic::transport::Endpoint,
    _lane: crate::native::data_runtime::NativeDialLane,
    reservation: Option<NativeDialReservation>,
}

impl Client {
    /// Select once using the existing rotation/election, before a local offer.
    /// Configuration is from this same original Native transport; no network,
    /// trust fallback, new pool, or stage authority is created here.
    pub(crate) fn prepare_channel_source(
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
            readiness: None,
        })))
    }
}

/// Two original readiness sources. Register after own partial exit and before
/// rechecking the same physical predicates, so a concurrent release cannot be
/// lost between that inspection and Pending.
/// This contains no authorization, retry policy, timer or shadow capacity.
struct NativePhysicalReadyWait {
    cache: std::pin::Pin<Box<tokio::sync::futures::OwnedNotified>>,
    outgoing: std::pin::Pin<Box<tokio::sync::futures::OwnedNotified>>,
}
impl NativePhysicalReadyWait {
    fn new(runtime: &crate::native::data_runtime::FrontendDataRuntime) -> Self {
        let mut cache = Box::pin(runtime.native_ready_notified());
        cache.as_mut().enable();
        let mut outgoing = Box::pin(runtime.transport_admission().frontend_outgoing_notified());
        outgoing.as_mut().enable();
        Self { cache, outgoing }
    }
    fn poll(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        use std::future::Future;
        if self.cache.as_mut().poll(cx).is_ready() {
            return std::task::Poll::Ready(());
        }
        self.outgoing.as_mut().poll(cx)
    }
}

impl NativeChannelSource {
    /// First-party allocations retained by the selected cold source and its
    /// handoff. Include both carrier boxes across the ownership transition;
    /// the two readiness futures are pinned in their own original boxes.
    /// Shared process services and third-party task internals are not invoiced
    /// here. This layout observation allocates no source or physical position.
    pub(crate) fn owned_source_layouts() -> [std::alloc::Layout; 4] {
        use std::alloc::Layout;
        [
            Layout::new::<SelectedNativeDial>(),
            Layout::new::<ReservedNativeDial>(),
            Layout::new::<tokio::sync::futures::OwnedNotified>(),
            Layout::new::<tokio::sync::futures::OwnedNotified>(),
        ]
    }

    /// Gate a previously blocked cold source without taking another physical
    /// position. Spurious events are absorbed here, not converted into another
    /// failed admission whose temporary drops would wake other blocked jobs.
    pub(crate) fn poll_selected_ready(
        source: &mut Option<Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), ChannelAcquisitionError>> {
        let Some(Self::Cold(selected)) = source.as_mut() else {
            return std::task::Poll::Ready(Ok(()));
        };
        loop {
            if selected.readiness.is_none() {
                selected.readiness = Some(NativePhysicalReadyWait::new(
                    selected.reservation.data_runtime(),
                ));
            }
            let ready = match selected.reservation.ready_channel() {
                Err(error) => {
                    return std::task::Poll::Ready(Err(
                        ChannelAcquisitionError::retryable_network(error),
                    ));
                }
                Ok(Some(_)) => true,
                Ok(None) => {
                    selected.reservation.is_leader()
                        && match selected
                            .reservation
                            .data_runtime()
                            .native_physical_ready(selected.reservation.lane())
                        {
                            Ok(ready) => ready,
                            Err(error) => {
                                return std::task::Poll::Ready(Err(
                                    ChannelAcquisitionError::retryable_network(error),
                                ));
                            }
                        }
                }
            };
            if ready {
                drop(selected.readiness.take());
                return std::task::Poll::Ready(Ok(()));
            }
            if selected.readiness.as_mut().unwrap().poll(cx).is_pending() {
                return std::task::Poll::Pending;
            }
            drop(selected.readiness.take());
            // Re-arm BEFORE the same-author state recheck. This loop consumes
            // real notifications but performs no physical try or WC refund.
        }
    }
    /// Same original physical/election author as try_reserve_in. Its OWN
    /// partial releases occur before arming fresh observers. The caller must
    /// poll_selected_ready before another WC offer/encoding attempt after
    /// Pending, so WC reobserve cannot turn into a native busy loop.
    pub(crate) fn poll_reserve_in(
        source: &mut Option<Self>,
        target: &mut Option<NativeDialHandoff>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), ChannelAcquisitionError>> {
        loop {
            if let Some(Self::Cold(selected)) = source.as_mut() {
                drop(selected.readiness.take());
            }
            match Self::try_reserve_in(source, target) {
                Err(error) => return std::task::Poll::Ready(Err(error)),
                Ok(NativeDialAttempt::Installed) => return std::task::Poll::Ready(Ok(())),
                Ok(NativeDialAttempt::Pending) => match Self::poll_selected_ready(source, cx) {
                    std::task::Poll::Ready(Err(error)) => {
                        return std::task::Poll::Ready(Err(error));
                    }
                    std::task::Poll::Ready(Ok(())) => continue,
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                },
            }
        }
    }
    /// Original stable slots are borrowed. No source is removed until the
    /// matching receiver can own it; every foreign try/Drop is lock-free with
    /// respect to workload State, cache, subscription Core and owner locks.
    /// `Pending` installs no durable waiter: the same finite service rechecks.
    pub(crate) fn try_reserve_in(
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
                    readiness: _,
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
    pub(crate) async fn connect_in(
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
