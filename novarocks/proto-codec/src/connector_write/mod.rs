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

//! Structural validation and canonical encoding for the central connector
//! write carriers.
//!
//! Two carriers cross the process boundary in opposite directions: a logical
//! writer handle travels FE to BE inside a plan node, and a commit fragment
//! travels BE to FE inside the root write relation. Both carry a bounded,
//! provider-owned payload behind a common header.
//!
//! This module validates only the common carrier. Provider semantics and
//! private structure live behind the provider's own codec and constructors.

mod fragment;
mod handle;
mod runtime_codec;

pub use fragment::ValidatedCommitFragment;
pub use handle::ValidatedWriterHandle;
pub use runtime_codec::{
    ConnectorWriteCodecError, ConnectorWriteFragmentDecoder, ConnectorWriteFragmentEncoder,
    ConnectorWriteHandleDecoder, ConnectorWriteHandleEncoder,
};

// Hard bounds. Every one of these is a wire-visible budget: exceeding it is a
// typed rejection before any connector I/O or external side effect.
//
// The two encoded-size caps mirror the SPI budgets they enforce
// (`MAX_CONNECTOR_WRITER_HANDLE_BYTES`, `MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES`).
// They are restated here because the codec is the trust boundary: a carrier is
// rejected before it is parsed, not after a caller happens to check.
pub const MAX_WRITER_HANDLE_ENCODED_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_COMMIT_FRAGMENT_ENCODED_BYTES: usize = 1024 * 1024;

#[cfg(test)]
mod tests {
    use novarocks_spi::connector::write_stack::{
        MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES, MAX_CONNECTOR_WRITER_HANDLE_BYTES,
    };

    use super::{MAX_COMMIT_FRAGMENT_ENCODED_BYTES, MAX_WRITER_HANDLE_ENCODED_BYTES};

    /// The two encoded-size caps are restated here so the codec can reject a
    /// carrier before parsing it, but they are the SPI's budgets and nothing
    /// else. Restating a number is only safe while something proves the two
    /// copies still agree: widening one alone would silently open the trust
    /// boundary that the other still believes it closes.
    #[test]
    fn the_codec_size_caps_are_the_spi_budgets_they_restate() {
        assert_eq!(
            MAX_WRITER_HANDLE_ENCODED_BYTES,
            MAX_CONNECTOR_WRITER_HANDLE_BYTES
        );
        assert_eq!(
            MAX_COMMIT_FRAGMENT_ENCODED_BYTES,
            MAX_CONNECTOR_COMMIT_FRAGMENT_BYTES
        );
    }
}
