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
-- Validate Iceberg bucket partition evolution query correctness and pruning explain text.
-- query 1
CREATE SCHEMA iceberg_ddl_cat_${suite_uuid0}.iceberg_part_db_${uuid0};
USE iceberg_ddl_cat_${suite_uuid0}.iceberg_part_db_${uuid0};
DROP TABLE IF EXISTS test_users_bucketed;
CREATE TABLE test_users_bucketed (
  user_id BIGINT,
  country VARCHAR,
  score DOUBLE
) WITH (partitioning = ARRAY['bucket(user_id, 16)']);
INSERT INTO test_users_bucketed VALUES
  ROW(1, 'US', 1.0),
  ROW(2, 'US', 2.0),
  ROW(3, 'CN', 3.0),
  ROW(4, 'CN', 4.0),
  ROW(5, 'JP', 5.0);
ALTER TABLE test_users_bucketed SET PROPERTIES partitioning = ARRAY[];
ALTER TABLE test_users_bucketed SET PROPERTIES partitioning = ARRAY['bucket(user_id, 32)'];
INSERT INTO test_users_bucketed VALUES
  ROW(6, 'US', 6.0),
  ROW(7, 'CN', 7.0),
  ROW(16, 'US', 16.0),
  ROW(32, 'CN', 32.0);
SELECT COUNT(*) AS cnt, SUM(score) AS sum_score
FROM test_users_bucketed;

-- query 2
USE iceberg_ddl_cat_${suite_uuid0}.iceberg_part_db_${uuid0};
SELECT country, COUNT(*) AS country_cnt, SUM(score) AS score_sum
FROM test_users_bucketed
GROUP BY country
ORDER BY country;

-- query 3
USE iceberg_ddl_cat_${suite_uuid0}.iceberg_part_db_${uuid0};
SELECT COUNT(*) AS cnt, SUM(score) AS sum_score
FROM test_users_bucketed
WHERE user_id IN (1, 2, 3, 4);

-- query 4
-- @result_contains=min-max stats
-- @result_contains=predicates: user_id IN (1, 2, 3, 4, 5, 6, 7, 16, 32)
USE iceberg_ddl_cat_${suite_uuid0}.iceberg_part_db_${uuid0};
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT COUNT(*), SUM(score)
FROM test_users_bucketed
WHERE user_id IN (1, 2, 3, 4, 5, 6, 7, 16, 32);

-- query 5
-- @result_contains=min-max stats
-- @result_contains=predicates: user_id IN (1, 2, 3, 4)
USE iceberg_ddl_cat_${suite_uuid0}.iceberg_part_db_${uuid0};
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT COUNT(*), SUM(score)
FROM test_users_bucketed
WHERE user_id IN (1, 2, 3, 4);
DROP TABLE iceberg_ddl_cat_${suite_uuid0}.iceberg_part_db_${uuid0}.test_users_bucketed ;
DROP SCHEMA iceberg_ddl_cat_${suite_uuid0}.iceberg_part_db_${uuid0};
