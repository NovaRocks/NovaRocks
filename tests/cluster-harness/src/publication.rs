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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Publication fault services have one lifecycle owner shared by system scenarios.
//! SQL suites reuse the same proxy and service control modules independently.

use crate::isolated_iceberg_rest::IsolatedIcebergRestFixture;
use crate::publication_catalog::{FixtureControl, PublicationCatalogFixture};
use crate::publication_service::Control;
use anyhow::{Context, Result};
use std::path::Path;

pub struct PublicationFixture {
    rest: IsolatedIcebergRestFixture,
    proxy: PublicationCatalogFixture,
    service: Control,
}
impl PublicationFixture {
    pub fn start(root: &Path) -> Result<Self> {
        let (rest, uri) = IsolatedIcebergRestFixture::start_with_publication_hook(root)?;
        let service = Control::new(uri)?;
        let proxy = PublicationCatalogFixture::start(rest.endpoints().rest_uri.clone())?;
        Ok(Self {
            rest,
            proxy,
            service,
        })
    }
    pub fn rest(&self) -> &IsolatedIcebergRestFixture {
        &self.rest
    }
    pub fn uri(&self) -> &str {
        self.proxy.uri()
    }
    pub fn service(&self) -> Control {
        self.service.clone()
    }
    pub fn proxy(&self) -> Result<FixtureControl> {
        self.proxy.control()
    }
    pub fn shutdown(&mut self) -> Result<()> {
        // Cancel proxy bodies and join before destroying its downstream service.
        let proxy = self.proxy.shutdown();
        let rest = self.rest.shutdown();
        match (proxy, rest) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(primary), Err(cleanup)) => {
                anyhow::bail!("{primary:#}; REST fixture cleanup: {cleanup:#}")
            }
            (Err(error), _) | (_, Err(error)) => {
                Err(error).context("publication fixture actual exit")
            }
        }
    }
}
impl Drop for PublicationFixture {
    fn drop(&mut self) {
        if let Err(error) = self.shutdown() {
            eprintln!("publication fixture cleanup failed: {error:#}");
        }
    }
}
