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

-- @order_sensitive=true
-- Validate Iceberg writes when write.metadata.metrics.default is set to none.
-- query 1
CREATE DATABASE iceberg_dml_cat_${suite_uuid0}.iceberg_none_db_${uuid0};
-- query 2
CREATE TABLE iceberg_dml_cat_${suite_uuid0}.iceberg_none_db_${uuid0}.iceberg_none_tbl_${uuid0} (
  k1 INT
) PROPERTIES ("write.metadata.metrics.default" = "none");
-- query 3
INSERT INTO iceberg_dml_cat_${suite_uuid0}.iceberg_none_db_${uuid0}.iceberg_none_tbl_${uuid0}
SELECT 1;
-- query 4
SELECT k1
FROM iceberg_dml_cat_${suite_uuid0}.iceberg_none_db_${uuid0}.iceberg_none_tbl_${uuid0};
-- query 5
-- query 6
DROP TABLE iceberg_dml_cat_${suite_uuid0}.iceberg_none_db_${uuid0}.iceberg_none_tbl_${uuid0} FORCE;
-- query 7
DROP DATABASE iceberg_dml_cat_${suite_uuid0}.iceberg_none_db_${uuid0};
