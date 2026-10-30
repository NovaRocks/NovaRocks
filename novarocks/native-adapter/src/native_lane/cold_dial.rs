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

//! Transfer one original Frontend dial admission into the actual connector.
//!
//! Endpoint::connect_with_connector does not require Clone. Its reconnect
//! service keeps this same closure: the first call takes the supplied original
//! holder; each later call takes fresh positions from the original author.

use super::*;

/// The original process physical+handshake holder for one Frontend lane.
/// This is not a Work/Joint grant, and its fields cannot be extracted or cloned.
pub struct FrontendDialReservation {
    original: crate::native_transport_admission::DialAdmission,
    admission: NativeTransportAdmission,
    lane: FrontendNativeLane,
}

impl fmt::Debug for FrontendDialReservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrontendDialReservation")
            .field("lane", &self.lane)
            .finish_non_exhaustive()
    }
}

impl NativeTransportAdmission {
    /// Take the original physical and handshake positions without waiting or
    /// IO. Call outside every State/cache/receiver lock: the original author
    /// publishes observer transitions and permit Drop may wake another task.
    pub fn try_reserve_frontend_dial(
        &self,
        lane: FrontendNativeLane,
    ) -> std::io::Result<FrontendDialReservation> {
        if self.role() != TransportRole::Frontend {
            return Err(std::io::ErrorKind::InvalidInput.into());
        }
        if self.frontend_outgoing_is_closed() {
            return Err(std::io::ErrorKind::ConnectionAborted.into());
        }
        let original = match self.try_dial_lane(lane) {
            Ok(original) => original,
            Err(_) if self.frontend_outgoing_is_closed() => {
                return Err(std::io::ErrorKind::ConnectionAborted.into());
            }
            Err(error) => return Err(error),
        };
        if self.frontend_outgoing_is_closed() {
            drop(original);
            return Err(std::io::ErrorKind::ConnectionAborted.into());
        }
        Ok(FrontendDialReservation {
            original,
            admission: self.clone(),
            lane,
        })
    }
}

impl FrontendDialReservation {
    /// MOVE the exact original holder into Tonic's connector. The first call
    /// does not acquire again; reconnect uses the same original author and
    /// refuses immediately if no local position exists. No hidden local wait,
    /// extra queue, shared first-holder cell, timer, or task is installed here.
    ///
    /// The original stage/process owner must retain the real connect future.
    /// The connector's URI is still only Tonic's origin; the supplied typed
    /// NativeEndpointConnector owns DNS/TLS/authenticated endpoint semantics.
    pub fn into_connector(
        self,
        connector: novarocks_native_trust::NativeEndpointConnector,
    ) -> impl Service<
        hyper::http::Uri,
        Response = hyper_util::rt::TokioIo<novarocks_transport_io::BoxedIo>,
        Error = std::io::Error,
        Future = impl Future<
            Output = std::io::Result<hyper_util::rt::TokioIo<novarocks_transport_io::BoxedIo>>,
        > + Send,
    > + Send
    + 'static {
        let Self {
            original,
            admission,
            lane,
        } = self;
        let mut first = Some(original);
        // DialAdmission is not Clone, so neither this closure nor its service
        // can duplicate the first qualification. Tonic does not need Clone.
        tower::service_fn(move |_| {
            let connector = connector.clone();
            let original = match first.take() {
                Some(original) => Ok(original),
                None => admission.try_dial_lane(lane).map_err(|error| {
                    std::io::Error::new(error.kind(), "native dial admission refused")
                }),
            };
            let admission = admission.clone();
            async move {
                let original = original?;
                // Original Semaphore::close prevents new reconnect holders.
                // A first holder reserved before close is checked again before
                // starting IO. A later close does not counterfeit its exit.
                if admission.frontend_outgoing_is_closed() {
                    return Err(std::io::ErrorKind::ConnectionAborted.into());
                }
                let io = connector.connect().await.map_err(std::io::Error::other)?;
                // Reuse the actual custody author: IO is installed before any
                // fallible/foreign Live publication, handshake returns at IO
                // establishment, physical follows OwnedIo's actual Drop.
                let io: novarocks_transport_io::BoxedIo = Box::new(original.established_io(io)?);
                Ok(hyper_util::rt::TokioIo::new(io))
            }
        })
    }
}
