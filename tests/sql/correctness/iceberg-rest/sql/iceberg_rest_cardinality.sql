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
-- OQ-3 end-to-end cardinality guard.
--
-- Builds a real REST-catalog Iceberg table with 1000 rows where k1 = 1..1000,
-- then asserts that a range predicate (k1 < 100) drives the scan/filter
-- row-count estimate strictly below the full-table row count via the
-- stats={rows=N} trailer that EXPLAIN VERBOSE emits on every physical node.
--
-- Observed numbers (NovaRocks debug build, REST catalog, 2026-05):
--   full table .................. stats={rows=1000}
--   WHERE k1 < 100 .............. stats={rows=99}
-- The reduction proves the selectivity chain (predicate -> LogicalProperties
-- -> stats trailer) is wired end-to-end on a real Iceberg table.
--
-- The post-filter estimate is the real range selectivity, not the 0.5
-- fallback: for k1 = 1..1000, `k1 < 100` matches the 99 values 1..99. This
-- requires the NovaRocks Iceberg writer to persist per-column lower/upper
-- bounds into the manifest. OQ-3.1 wired those bounds through the commit
-- path (DataFile -> WrittenFile -> committed DataFile), so column min/max are
-- now finite (k1[min=1 max=1000]) instead of +/-inf and the range formula
-- yields the true row count. Re-recorded from the previous 500 fallback.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_rest_${suite_uuid0}.iceberg_rest_card_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_card_db_${uuid0}.t_card_${uuid0} (
  k1 INT,
  v INT
);

-- query 3
-- @skip_result_check=true
-- Populate 1000 rows with k1 = v = 1..1000.
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_card_db_${uuid0}.t_card_${uuid0}
SELECT CAST(generate_series AS INT) AS k1, CAST(generate_series AS INT) AS v
  FROM TABLE(generate_series(1, 1000));

-- query 4
-- Sanity: the table holds exactly 1000 rows spanning [1, 1000].
SELECT COUNT(*) AS n, MIN(k1) AS lo, MAX(k1) AS hi
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_card_db_${uuid0}.t_card_${uuid0};

-- query 5
-- Full-table scan estimate is the real row count.
-- @skip_result_check=true
-- @explain_contains=stats={rows=1000}
SELECT k1 FROM iceberg_rest_${suite_uuid0}.iceberg_rest_card_db_${uuid0}.t_card_${uuid0};

-- query 6
-- Range predicate drives the estimate to the real range selectivity (99 of
-- the 1000 rows, i.e. k1 in 1..99), proving finite min/max bounds reach the
-- cost model rather than the 0.5 fallback.
-- @skip_result_check=true
-- @explain_contains=stats={rows=99}
SELECT k1 FROM iceberg_rest_${suite_uuid0}.iceberg_rest_card_db_${uuid0}.t_card_${uuid0}
  WHERE k1 < 100;

-- query 7
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_card_db_${uuid0}.t_card_${uuid0};

-- query 8
-- @skip_result_check=true
DROP SCHEMA iceberg_rest_${suite_uuid0}.iceberg_rest_card_db_${uuid0};
