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
-- Validate SELECT * FROM <tbl>$history surfaces snapshot history rows.
-- Tests Phase A (metadata table SQL routing) — history flavour.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metahist_${uuid0} (id INT) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metahist_${uuid0} VALUES ROW(1);

-- query 4
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metahist_${uuid0} VALUES ROW(2);

-- query 5
-- 2 history rows, one per snapshot.
SELECT count(*) AS n_history
  FROM iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}."metahist_${uuid0}$history";

-- query 6
-- All history rows are ancestors of current after 2 sequential appends on main.
SELECT count(*) AS n_ancestors
  FROM iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}."metahist_${uuid0}$history"
  WHERE is_current_ancestor = TRUE;

-- query 7
-- @skip_result_check=true
DROP TABLE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metahist_${uuid0};

-- query 8
-- @skip_result_check=true
DROP SCHEMA iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0};
