-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @catalog=iceberg_opt
-- Create the iceberg catalog the optimizer suite uses for its base tables, so
-- ANALYZE-derived NDV (Puffin statistics) reaches the cost-based optimizer.
-- Native internal tables do not exist and are not exercised by this
-- suite. The catalog name is stable (per-case case_db reset isolates data);
-- a distinct warehouse sub-path keeps these tables separate from other iceberg
-- suites that share the warehouse root.
CREATE EXTERNAL CATALOG IF NOT EXISTS `iceberg_opt`
PROPERTIES (
    "type"="iceberg",
    "iceberg.catalog.type"="${iceberg_catalog_type}",
    "iceberg.catalog.warehouse"="${iceberg_catalog_warehouse}/optimizer",
    "aws.s3.access_key"="${oss_ak}",
    "aws.s3.secret_key"="${oss_sk}",
    "aws.s3.endpoint"="${oss_endpoint}",
    "aws.s3.enable_path_style_access"="true"
);
