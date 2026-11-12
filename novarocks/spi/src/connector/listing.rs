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

//! Complete connector listings with source-owned pagination progress checks.
use super::{ConnectorError, ConnectorErrorKind};
use std::{collections::HashSet, num::NonZeroUsize};

/// Requested pagination granularity, never a whole-listing quota.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorListingPageSize(NonZeroUsize);
impl ConnectorListingPageSize {
    pub const DEFAULT: Self = Self(NonZeroUsize::new(256).unwrap());
    pub fn new(value: usize) -> Result<Self, ConnectorError> {
        NonZeroUsize::new(value).map(Self).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "connector listing page size must be nonzero",
            )
        })
    }
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

/// Retains a complete listing and rejects inconsistent pagination before growth.
#[derive(Debug)]
pub struct ConnectorListingCollector<T> {
    page_size: ConnectorListingPageSize,
    entries: Vec<T>,
    continuation: Option<String>,
    seen_tokens: HashSet<String>,
}
impl<T: AsRef<str>> ConnectorListingCollector<T> {
    pub fn new(page_size: ConnectorListingPageSize) -> Result<Self, ConnectorError> {
        Ok(Self {
            page_size,
            entries: Vec::new(),
            continuation: None,
            seen_tokens: HashSet::new(),
        })
    }
    pub fn page_size(&self) -> ConnectorListingPageSize {
        self.page_size
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn continuation_token(&self) -> Option<&str> {
        self.continuation.as_deref()
    }
    pub fn push(&mut self, entry: T) -> Result<(), ConnectorError> {
        self.entries.push(entry);
        Ok(())
    }
    /// Check the continuation contract without retaining names or advancing it.
    pub fn check_page(
        &self,
        entries: usize,
        next_token: Option<&str>,
    ) -> Result<(), ConnectorError> {
        if let Some(token) = next_token {
            if token.is_empty() || self.seen_tokens.contains(token) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "connector listing source returned an empty or repeated continuation token",
                ));
            }
            if entries > self.page_size.get() {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "connector listing source exceeded the requested continuing page size",
                ));
            }
        }
        Ok(())
    }
    pub fn accept_page(
        &mut self,
        page: Vec<T>,
        next_token: Option<String>,
    ) -> Result<(), ConnectorError> {
        self.check_page(page.len(), next_token.as_deref())?;
        if let Some(token) = &next_token {
            self.seen_tokens.insert(token.clone());
        }
        self.entries.extend(page);
        self.continuation = next_token;
        Ok(())
    }
    pub fn finish(self) -> Result<Vec<T>, ConnectorError> {
        if self.continuation.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "connector listing finished with an unfollowed continuation token",
            ));
        }
        Ok(self.entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn page_size_is_a_nonzero_request_without_a_profile_ceiling() {
        assert_eq!(ConnectorListingPageSize::DEFAULT.get(), 256);
        assert_eq!(
            ConnectorListingPageSize::new(0).unwrap_err().kind(),
            ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(ConnectorListingPageSize::new(65_537).unwrap().get(), 65_537);
    }
    #[test]
    fn complete_listing_exceeds_former_entry_page_name_and_token_quotas() {
        let mut collector =
            ConnectorListingCollector::new(ConnectorListingPageSize::new(64).unwrap()).unwrap();
        let mut expected = Vec::new();
        for page in 0..1025 {
            let names: Vec<_> = (0..64)
                .map(|item| format!("{page:04}-{item:02}-{}", "n".repeat(256)))
                .collect();
            expected.extend(names.iter().cloned());
            let token = Some(format!("{}-{page}", "t".repeat(4097)));
            collector.accept_page(names, token).unwrap();
        }
        let long_name = "x".repeat(65_537);
        expected.push(long_name.clone());
        collector.accept_page(vec![long_name], None).unwrap();
        assert!(expected.len() > 65_536);
        assert!(expected.iter().map(String::len).sum::<usize>() > 16 * 1024 * 1024);
        assert_eq!(collector.finish().unwrap(), expected);
    }
    #[test]
    fn unpaged_final_response_has_no_entry_quota() {
        let names: Vec<_> = (0..65_537).map(|i| i.to_string()).collect();
        let mut collector =
            ConnectorListingCollector::new(ConnectorListingPageSize::DEFAULT).unwrap();
        collector.accept_page(names.clone(), None).unwrap();
        assert_eq!(collector.finish().unwrap(), names);
    }
    #[test]
    fn page_protocol_failure_does_not_advance_or_retain() {
        let mut collector =
            ConnectorListingCollector::new(ConnectorListingPageSize::new(1).unwrap()).unwrap();
        collector
            .accept_page(vec!["a".to_owned()], Some("t1".into()))
            .unwrap();
        for token in ["", "t1"] {
            assert_eq!(
                collector
                    .accept_page(Vec::new(), Some(token.into()))
                    .unwrap_err()
                    .kind(),
                ConnectorErrorKind::CorruptData
            );
        }
        assert_eq!(
            collector
                .accept_page(vec!["b".into(), "c".into()], Some("t2".into()))
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
        collector
            .accept_page(Vec::new(), Some("t2".into()))
            .unwrap();
        assert_eq!(
            collector
                .accept_page(Vec::new(), Some("t1".into()))
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
        assert_eq!(collector.len(), 1);
        assert_eq!(collector.continuation_token(), Some("t2"));
        assert_eq!(
            collector.finish().unwrap_err().kind(),
            ConnectorErrorKind::Internal
        );
    }
}
