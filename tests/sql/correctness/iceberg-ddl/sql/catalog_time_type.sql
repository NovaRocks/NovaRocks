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
-- Validate Iceberg TIME round-trip through the Parquet write/read path.
-- query 1
CREATE DATABASE iceberg_ddl_cat_${suite_uuid0}.iceberg_time_db_${uuid0};
-- query 2
CREATE TABLE iceberg_ddl_cat_${suite_uuid0}.iceberg_time_db_${uuid0}.iceberg_time_tbl_${uuid0} (
  col_id INT,
  col_time TIME
);
-- query 3
INSERT INTO iceberg_ddl_cat_${suite_uuid0}.iceberg_time_db_${uuid0}.iceberg_time_tbl_${uuid0} VALUES
  (1, '01:02:03'),
  (2, '20:20:20'),
  (3, NULL);
-- query 4
SELECT col_id, col_time
FROM iceberg_ddl_cat_${suite_uuid0}.iceberg_time_db_${uuid0}.iceberg_time_tbl_${uuid0}
ORDER BY col_id;
-- query 5
-- query 6
DROP TABLE iceberg_ddl_cat_${suite_uuid0}.iceberg_time_db_${uuid0}.iceberg_time_tbl_${uuid0} FORCE;
-- query 7
DROP DATABASE iceberg_ddl_cat_${suite_uuid0}.iceberg_time_db_${uuid0};
