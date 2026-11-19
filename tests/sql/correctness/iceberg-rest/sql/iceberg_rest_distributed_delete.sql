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
-- Validate distributed Iceberg DELETE through ICEBERG_DELETE_SINK:
-- - v2/legacy tables write position-delete files from a distributed SELECT
-- - partitioned delete files preserve partition values from sink source cols
-- - v3 row-lineage/DV DELETE commits via injected delete groups

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0}.t_del_${uuid0} (
  id INT,
  region STRING,
  amount INT
)
PARTITION BY (region);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0}.t_del_${uuid0}
VALUES (1, 'east', 10), (2, 'east', 20), (3, 'west', 30);

-- query 4
-- @skip_result_check=true
DELETE FROM iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0}.t_del_${uuid0}
WHERE region = 'east' AND amount = 10;

-- query 5
SELECT id, region, amount
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0}.t_del_${uuid0}
  ORDER BY id;

-- query 6
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0}.t_del_v3_${uuid0} (
  id INT,
  region STRING
)
TBLPROPERTIES ("format-version" = "3");

-- query 7
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0}.t_del_v3_${uuid0}
VALUES (1, 'east');

-- query 8
-- @skip_result_check=true
DELETE FROM iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0}.t_del_v3_${uuid0}
WHERE id = 1;

-- query 9
SELECT id, region
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0}.t_del_v3_${uuid0}
  ORDER BY id;

-- query 10
SELECT region, COUNT() AS cnt
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0}.t_del_${uuid0}
  GROUP BY region
  ORDER BY region;

-- query 11
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0}.t_del_v3_${uuid0};

-- query 12
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0}.t_del_${uuid0};

-- query 13
-- @skip_result_check=true
DROP DATABASE iceberg_rest_${suite_uuid0}.iceberg_rest_del_db_${uuid0};
