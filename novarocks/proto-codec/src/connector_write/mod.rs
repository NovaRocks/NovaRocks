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

/// The maximum wire size of a writer handle.
pub const MAX_WRITER_HANDLE_ENCODED_BYTES: usize = 16 * 1024 * 1024;

#[cfg(test)]
mod tests {
    use super::MAX_WRITER_HANDLE_ENCODED_BYTES;
    use novarocks_spi::connector::write_stack::MAX_CONNECTOR_WRITER_HANDLE_BYTES;

    #[test]
    fn the_codec_handle_size_cap_matches_its_spi_budget() {
        assert_eq!(
            MAX_WRITER_HANDLE_ENCODED_BYTES,
            MAX_CONNECTOR_WRITER_HANDLE_BYTES
        );
    }
}
