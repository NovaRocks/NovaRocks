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

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct FileIdentity {
    path: String,
    file_size: u64,
    modification_time: Option<i64>,
}

impl FileIdentity {
    pub fn new(path: impl Into<String>, file_size: u64, modification_time: Option<i64>) -> Self {
        Self {
            path: path.into(),
            file_size,
            modification_time,
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn modification_time(&self) -> Option<i64> {
        self.modification_time
    }

    pub fn with_modification_time_override(mut self, modification_time: Option<i64>) -> Self {
        if modification_time.is_some() {
            self.modification_time = modification_time;
        }
        self
    }

    pub fn starrocks_cache_tail(&self) -> u32 {
        if self.modification_time.unwrap_or(0) > 0 {
            ((self.modification_time.unwrap_or(0) >> 9) & 0x0000_0000_FFFF_FFFF) as u32
        } else {
            self.file_size as u32
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FileIdentity;

    #[test]
    fn override_none_keeps_existing_value() {
        let identity =
            FileIdentity::new("path", 42, Some(123)).with_modification_time_override(None);
        assert_eq!(identity.modification_time(), Some(123));
    }

    #[test]
    fn override_zero_is_preserved() {
        let identity =
            FileIdentity::new("path", 42, Some(123)).with_modification_time_override(Some(0));
        assert_eq!(identity.modification_time(), Some(0));
    }

    #[test]
    fn override_positive_value_replaces_existing_value() {
        let identity =
            FileIdentity::new("path", 42, None).with_modification_time_override(Some(456));
        assert_eq!(identity.modification_time(), Some(456));
    }

    #[test]
    fn starrocks_cache_tail_falls_back_to_file_size_without_positive_mtime() {
        let none_mtime = FileIdentity::new("path", 42, None);
        let zero_mtime = FileIdentity::new("path", 42, Some(0));
        let positive_mtime = FileIdentity::new("path", 42, Some(512 << 9));

        assert_eq!(none_mtime.starrocks_cache_tail(), 42);
        assert_eq!(zero_mtime.starrocks_cache_tail(), 42);
        assert_eq!(positive_mtime.starrocks_cache_tail(), 512);
    }
}
