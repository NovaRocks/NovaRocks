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

-- @catalog=iceberg_cat_${uuid0}
-- @db=tpcds
CREATE EXTERNAL CATALOG IF NOT EXISTS `iceberg_cat_${uuid0}`
PROPERTIES (
    "type"="iceberg",
    "iceberg.catalog.type"="${iceberg_catalog_type}",
    "iceberg.catalog.warehouse"="${benchmark_iceberg_catalog_warehouse}",
    "credential.object-store-metadata.consumer-role" = "frontend",
    "credential.object-store-metadata.mode" = "static",
    "credential.object-store-metadata.name" = "${iceberg_object_store_credential_name}",
    "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
    "credential.object-store-data.consumer-role" = "backend",
    "credential.object-store-data.mode" = "static",
    "credential.object-store-data.name" = "${iceberg_object_store_credential_name}",
    "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
    "aws.s3.endpoint"="${oss_endpoint}",
    "aws.s3.enable_path_style_access"="true"
);

USE `iceberg_cat_${uuid0}`.`tpcds`;
SELECT 1 FROM `call_center` LIMIT 1;
SELECT 1 FROM `catalog_page` LIMIT 1;
SELECT 1 FROM `catalog_returns` LIMIT 1;
SELECT 1 FROM `catalog_sales` LIMIT 1;
SELECT 1 FROM `customer` LIMIT 1;
SELECT 1 FROM `customer_address` LIMIT 1;
SELECT 1 FROM `customer_demographics` LIMIT 1;
SELECT 1 FROM `date_dim` LIMIT 1;
SELECT 1 FROM `household_demographics` LIMIT 1;
SELECT 1 FROM `income_band` LIMIT 1;
SELECT 1 FROM `inventory` LIMIT 1;
SELECT 1 FROM `item` LIMIT 1;
SELECT 1 FROM `promotion` LIMIT 1;
SELECT 1 FROM `reason` LIMIT 1;
SELECT 1 FROM `ship_mode` LIMIT 1;
SELECT 1 FROM `store` LIMIT 1;
SELECT 1 FROM `store_returns` LIMIT 1;
SELECT 1 FROM `store_sales` LIMIT 1;
SELECT 1 FROM `time_dim` LIMIT 1;
SELECT 1 FROM `warehouse` LIMIT 1;
SELECT 1 FROM `web_page` LIMIT 1;
SELECT 1 FROM `web_returns` LIMIT 1;
SELECT 1 FROM `web_sales` LIMIT 1;
SELECT 1 FROM `web_site` LIMIT 1;
