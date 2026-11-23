// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Incremental digest computation with bounded observations.
use sha2::Digest;

#[derive(Clone, Copy)]
pub enum Observation {
    Step,
    OpaqueBoundary,
}
pub(super) fn digest_bytes<D: Digest, E>(
    text: &[u8],
    observe: &mut impl FnMut(Observation) -> Result<(), E>,
) -> Result<sha2::digest::Output<D>, E> {
    observe(Observation::OpaqueBoundary)?;
    let mut digest = D::new();
    observe(Observation::OpaqueBoundary)?;
    // SHA update is the same original stream author. Batching exposes the
    // existing selected kernel's boundaries; digest compression stays opaque.
    for chunk in text.chunks(256) {
        observe(Observation::OpaqueBoundary)?;
        digest.update(chunk);
        observe(Observation::OpaqueBoundary)?;
        for _ in chunk {
            observe(Observation::Step)?;
        }
    }
    observe(Observation::OpaqueBoundary)?;
    let output = digest.finalize();
    observe(Observation::OpaqueBoundary)?;
    Ok(output)
}
