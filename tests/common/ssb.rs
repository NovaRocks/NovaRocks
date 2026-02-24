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
//! SSB test data helper utilities
//!
//! Provides convenient access to SSB Parquet files for testing.

use std::path::PathBuf;

/// SSB test data helper
pub struct SSBTestData {
    base_dir: PathBuf,
}

impl SSBTestData {
    /// Create a new SSB test data helper
    pub fn new() -> Self {
        let base_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("data")
            .join("harbor_iceberg_ssb");

        // Verify the directory exists
        assert!(
            base_dir.exists(),
            "SSB data directory not found: {:?}. Please ensure tests/data/harbor_iceberg_ssb/ exists.",
            base_dir
        );

        Self { base_dir }
    }

    /// Get absolute path to customer parquet file
    pub fn customer_parquet_file(&self) -> String {
        self.base_dir
            .join("customer/data/019b43e4-4ff1-7f38-8f47-3c011e3219b8_0_0_0.parquet")
            .to_string_lossy()
            .to_string()
    }

    /// Get absolute path to dates parquet file
    pub fn dates_parquet_file(&self) -> String {
        self.base_dir
            .join("dates/data/019b43e4-7b03-7df3-8acf-7bf66df74cc6_0_0_0.parquet")
            .to_string_lossy()
            .to_string()
    }

    /// Get absolute path to lineorder parquet file
    pub fn lineorder_parquet_file(&self) -> String {
        self.base_dir
            .join("lineorder/data/019b43e3-df05-7a53-9a02-aa2a2c324c04_0_0_0.parquet")
            .to_string_lossy()
            .to_string()
    }

    /// Get absolute path to part parquet file
    pub fn part_parquet_file(&self) -> String {
        self.base_dir
            .join("part/data/019b43e4-dafa-7716-8858-a1145cd201e1_0_0_0.parquet")
            .to_string_lossy()
            .to_string()
    }

    /// Get absolute path to supplier parquet file
    pub fn supplier_parquet_file(&self) -> String {
        self.base_dir
            .join("supplier/data/019b43e4-a6ae-76de-82d9-e36739d4324e_0_0_0.parquet")
            .to_string_lossy()
            .to_string()
    }

    /// Get base directory path
    pub fn base_dir(&self) -> &PathBuf {
        &self.base_dir
    }
}

impl Default for SSBTestData {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[cfg(feature = "ssb")]
mod tests {
    use super::*;

    #[test]
    fn test_ssb_base_dir_exists() {
        let ssb = SSBTestData::new();
        assert!(ssb.base_dir().exists(), "SSB base directory should exist");
    }

    #[test]
    fn test_ssb_customer_file_exists() {
        let ssb = SSBTestData::new();
        let customer_file = ssb.customer_parquet_file();

        assert!(
            PathBuf::from(&customer_file).exists(),
            "Customer file not found: {}",
            customer_file
        );

        println!("✅ Customer file: {}", customer_file);
    }

    #[test]
    fn test_ssb_dates_file_exists() {
        let ssb = SSBTestData::new();
        let dates_file = ssb.dates_parquet_file();

        assert!(
            PathBuf::from(&dates_file).exists(),
            "Dates file not found: {}",
            dates_file
        );

        println!("✅ Dates file: {}", dates_file);
    }

    #[test]
    fn test_ssb_lineorder_file_exists() {
        let ssb = SSBTestData::new();
        let lineorder_file = ssb.lineorder_parquet_file();

        assert!(
            PathBuf::from(&lineorder_file).exists(),
            "Lineorder file not found: {}",
            lineorder_file
        );

        println!("✅ Lineorder file: {}", lineorder_file);
    }

    #[test]
    fn test_ssb_part_file_exists() {
        let ssb = SSBTestData::new();
        let part_file = ssb.part_parquet_file();

        assert!(
            PathBuf::from(&part_file).exists(),
            "Part file not found: {}",
            part_file
        );

        println!("✅ Part file: {}", part_file);
    }

    #[test]
    fn test_ssb_supplier_file_exists() {
        let ssb = SSBTestData::new();
        let supplier_file = ssb.supplier_parquet_file();

        assert!(
            PathBuf::from(&supplier_file).exists(),
            "Supplier file not found: {}",
            supplier_file
        );

        println!("✅ Supplier file: {}", supplier_file);
    }

    #[test]
    fn test_all_ssb_files_summary() {
        let ssb = SSBTestData::new();

        println!("\n═══════════════════════════════════");
        println!("   SSB Files Verification");
        println!("═══════════════════════════════════");

        let files = vec![
            ("customer", ssb.customer_parquet_file()),
            ("dates", ssb.dates_parquet_file()),
            ("lineorder", ssb.lineorder_parquet_file()),
            ("part", ssb.part_parquet_file()),
            ("supplier", ssb.supplier_parquet_file()),
        ];

        for (name, file) in files {
            let exists = PathBuf::from(&file).exists();
            let status = if exists { "✅" } else { "❌" };
            println!("{} {:12} {}", status, name, file);
            assert!(exists, "{} file should exist", name);
        }

        println!("═══════════════════════════════════\n");
    }
}
