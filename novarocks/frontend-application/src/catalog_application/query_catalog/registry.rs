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

use std::collections::HashSet;

#[derive(Clone, Default)]
pub struct CatalogRegistry {
    catalogs: HashSet<String>,
}

impl CatalogRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, name: &str) {
        self.catalogs.insert(name.to_ascii_lowercase());
    }

    pub fn unregister(&mut self, name: &str) {
        self.catalogs.remove(&name.to_ascii_lowercase());
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.catalogs.iter().map(String::as_str)
    }

    pub fn require_catalog(&self, name: &str) -> Result<(), String> {
        if self.catalogs.contains(&name.to_ascii_lowercase()) {
            Ok(())
        } else {
            Err(format!("unknown catalog: {name}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CatalogRegistry;

    #[test]
    fn catalog_membership_is_case_insensitive() {
        let mut registry = CatalogRegistry::new();
        registry.register("Ice");
        registry.register("ICE");
        assert_eq!(registry.names().collect::<Vec<_>>(), vec!["ice"]);
        assert!(registry.require_catalog("iCE").is_ok());

        registry.unregister("iCe");
        assert_eq!(registry.names().count(), 0);
        assert_eq!(
            registry.require_catalog("ICE"),
            Err("unknown catalog: ICE".to_string())
        );
    }
}
