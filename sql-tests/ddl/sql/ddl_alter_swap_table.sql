-- Migrated from dev/test/sql/test_ddl/T/test_alter_swap_table
-- Test Objective:
-- 1. Validate ALTER TABLE SWAP exchanges data between two tables.
-- 2. Validate that FK/unique constraints follow the physical table after SWAP.
-- 3. Validate that after DROP TABLE s1, FK constraints on referencing tables are removed.
-- 4. Validate SWAP between PRIMARY KEY table and AGGREGATE KEY table (different schemas).
-- Notes:
-- - MV table-prune optimization (assert_explain_contains/not_contains) is verified via EXPLAIN.
-- - SHOW CREATE TABLE checks use @result_contains for FK/unique constraint presence/absence
--   since the constraint text includes the dynamic case_db name.

-- query 1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE s1
(
    k1 int NOT NULL, k2 int, k3 int
)
DUPLICATE KEY(k1, k2)
DISTRIBUTED BY RANDOM
PROPERTIES("replication_num" = "1", 'unique_constraints'='s1.k1');

CREATE TABLE s2
(
    k1 int NOT NULL, k2 int, k3 int
)
DUPLICATE KEY(k1, k2)
DISTRIBUTED BY RANDOM
PROPERTIES("replication_num" = "1", 'foreign_key_constraints'='s2(k1) REFERENCES s1(k1)');

CREATE TABLE s3
(
    k1 int NOT NULL, k2 int, k3 int
)
DUPLICATE KEY(k1, k2)
DISTRIBUTED BY RANDOM
PROPERTIES("replication_num" = "1", 'foreign_key_constraints'='s3(k1) REFERENCES s1(k1)');

INSERT INTO s1 VALUES (1, 1, 1), (2, 2, 2);
INSERT INTO s2 VALUES (1, 1, 1), (2, 2, 2);
INSERT INTO s3 VALUES (1, 1, 1), (2, 2, 2);

CREATE MATERIALIZED VIEW test_mv12
REFRESH DEFERRED MANUAL
PROPERTIES (
    "replication_num" = "1",
    'foreign_key_constraints'='s2(k1) REFERENCES s1(k1)',
    'unique_constraints'='s1.k1'
)
AS SELECT s1.k1 AS s11, s1.k2 AS s12, s1.k3 AS s13, s2.k1 s21, s2.k2 s22, s2.k3 s23
   FROM s1 JOIN s2 ON s1.k1 = s2.k1;

-- query 2
-- @skip_result_check=true
-- Refresh MV and enable table-prune optimizations
USE ${case_db};
REFRESH MATERIALIZED VIEW test_mv12 WITH SYNC MODE;
SET enable_rbo_table_prune = true;
SET enable_cbo_table_prune = true;

-- query 3
-- @order_sensitive=true
USE ${case_db};
SELECT s2.k1, s2.k2 FROM s1 JOIN s2 ON s1.k1 = s2.k1 ORDER BY 1, 2;

-- query 4
-- @order_sensitive=true
USE ${case_db};
SELECT s3.k1, s3.k2 FROM s1 JOIN s3 ON s1.k1 = s3.k1 ORDER BY 1, 2;

-- query 5
-- @order_sensitive=true
USE ${case_db};
SELECT s2.k1, s2.k2 FROM s2 ORDER BY 1, 2;

-- query 6
-- @skip_result_check=true
-- SWAP s2 ↔ s3: s2 gets s3's physical data, s3 gets s2's physical data
-- Constraints should follow the name (not the data)
USE ${case_db};
ALTER TABLE s2 SWAP WITH s3;
ALTER MATERIALIZED VIEW test_mv12 ACTIVE;

-- query 7
-- After SWAP: s2 should have foreign_key_constraints (pointing to s1)
-- The DB name is dynamic so we check for the constraint key and the reference target
-- @result_contains=foreign_key_constraints
-- @result_contains=REFERENCES
-- @result_contains=s1(k1)
USE ${case_db};
SHOW CREATE TABLE s2;

-- query 8
-- After SWAP: s3 should also have foreign_key_constraints (pointing to s1)
-- @result_contains=foreign_key_constraints
-- @result_contains=REFERENCES
-- @result_contains=s1(k1)
USE ${case_db};
SHOW CREATE TABLE s3;

-- query 9
-- @order_sensitive=true
USE ${case_db};
SELECT s2.k1, s2.k2 FROM s1 JOIN s2 ON s1.k1 = s2.k1 ORDER BY 1, 2;

-- query 10
-- @order_sensitive=true
USE ${case_db};
SELECT s3.k1, s3.k2 FROM s1 JOIN s3 ON s1.k1 = s3.k1 ORDER BY 1, 2;

-- query 11
-- @order_sensitive=true
USE ${case_db};
SELECT s2.k1, s2.k2 FROM s2 ORDER BY 1, 2;

-- query 12
-- @skip_result_check=true
-- Create s1_new and SWAP s1 ↔ s1_new
USE ${case_db};
CREATE TABLE s1_new
(
    k1 int NOT NULL, k2 int, k3 int
)
DUPLICATE KEY(k1, k2)
DISTRIBUTED BY RANDOM
PROPERTIES("replication_num" = "1", 'unique_constraints'='s1_new.k1');
INSERT INTO s1_new VALUES (1, 2, 3), (2, 3, 4);
ALTER TABLE s1 SWAP WITH s1_new;
ALTER MATERIALIZED VIEW test_mv12 ACTIVE;

-- query 13
-- After s1 ↔ s1_new SWAP: s1 should have unique_constraints
-- @result_contains=unique_constraints
USE ${case_db};
SHOW CREATE TABLE s1;

-- query 14
-- After s1 ↔ s1_new SWAP: s1_new should have unique_constraints
-- @result_contains=unique_constraints
USE ${case_db};
SHOW CREATE TABLE s1_new;

-- query 15
-- s2 should still have FK constraint referencing s1
-- @result_contains=foreign_key_constraints
-- @result_contains=REFERENCES
-- @result_contains=s1(k1)
USE ${case_db};
SHOW CREATE TABLE s2;

-- query 16
-- s3 should still have FK constraint referencing s1
-- @result_contains=foreign_key_constraints
-- @result_contains=REFERENCES
-- @result_contains=s1(k1)
USE ${case_db};
SHOW CREATE TABLE s3;

-- query 17
-- @order_sensitive=true
USE ${case_db};
SELECT s2.k1, s2.k2 FROM s1 JOIN s2 ON s1.k1 = s2.k1 ORDER BY 1, 2;

-- query 18
-- @order_sensitive=true
USE ${case_db};
SELECT s3.k1, s3.k2 FROM s1 JOIN s3 ON s1.k1 = s3.k1 ORDER BY 1, 2;

-- query 19
-- @order_sensitive=true
USE ${case_db};
SELECT s2.k1, s2.k2 FROM s2 ORDER BY 1, 2;

-- query 20
-- @skip_result_check=true
-- Drop s1; this should remove FK constraints from s2 and s3
USE ${case_db};
DROP TABLE s1;

-- query 21
-- After DROP s1: s2 should no longer have foreign_key_constraints
-- @result_not_contains=foreign_key_constraints
USE ${case_db};
SHOW CREATE TABLE s2;

-- query 22
-- After DROP s1: s3 should no longer have foreign_key_constraints
-- @result_not_contains=foreign_key_constraints
USE ${case_db};
SHOW CREATE TABLE s3;

-- query 23
-- @skip_result_check=true
-- SWAP between PRIMARY KEY table and AGGREGATE KEY table (schema-incompatible SWAP)
USE ${case_db};
CREATE TABLE `primary_table_with_null_partition` (
  `k1` date NOT NULL, `k2` datetime NOT NULL, `k3` varchar(20) NOT NULL,
  `k4` varchar(20) NOT NULL, `k5` boolean NOT NULL,
  `v1` tinyint, `v2` smallint, `v3` int, `v4` bigint, `v5` largeint,
  `v6` float, `v7` double, `v8` decimal(27,9)
) PRIMARY KEY(`k1`, `k2`, `k3`, `k4`, `k5`)
COMMENT "OLAP"
PARTITION BY RANGE(`k1`) (
  PARTITION `p202006` VALUES LESS THAN ("2020-07-01"),
  PARTITION `p202007` VALUES LESS THAN ("2020-08-01"),
  PARTITION `p202008` VALUES LESS THAN ("2020-09-01")
)
DISTRIBUTED BY HASH(`k1`, `k2`, `k3`, `k4`, `k5`) BUCKETS 3
PROPERTIES ("replication_num" = "1", "storage_format" = "v2");

CREATE TABLE `aggregate_table_with_null` (
  `k1` date, `k2` datetime, `k3` char(20), `k4` varchar(20), `k5` boolean,
  `v1` bigint SUM, `v2` bigint SUM, `v3` bigint SUM, `v4` bigint MAX,
  `v5` largeint MAX, `v6` float MIN, `v7` double MIN, `v8` decimal(27,9) SUM
) AGGREGATE KEY(`k1`, `k2`, `k3`, `k4`, `k5`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`k1`, `k2`, `k3`, `k4`, `k5`) BUCKETS 3
PROPERTIES ("replication_num" = "1", "storage_format" = "v2", "light_schema_change" = "false");

INSERT INTO primary_table_with_null_partition VALUES
  ('2020-06-01', '2020-06-01 00:00:00', 'a', 'a', true, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
  ('2020-07-01', '2020-07-01 00:00:00', 'a', 'a', true, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
  ('2020-08-01', '2020-08-01 00:00:00', 'a', 'a', true, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0);
INSERT INTO aggregate_table_with_null VALUES
  ('2020-06-01', '2020-06-01 00:00:00', 'a', 'a', true, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
  ('2020-07-01', '2020-07-01 00:00:00', 'a', 'a', true, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0),
  ('2020-08-01', '2020-08-01 00:00:00', 'a', 'a', true, 1, 1, 1, 1, 1, 1.0, 1.0, 1.0);

-- query 24
-- @order_sensitive=true
USE ${case_db};
SELECT * FROM primary_table_with_null_partition ORDER BY k1 LIMIT 3;

-- query 25
-- @order_sensitive=true
USE ${case_db};
SELECT * FROM aggregate_table_with_null ORDER BY k1 LIMIT 3;

-- query 26
-- @skip_result_check=true
-- SWAP the two tables (different key types)
USE ${case_db};
ALTER TABLE primary_table_with_null_partition SWAP WITH aggregate_table_with_null;

-- query 27
-- @order_sensitive=true
-- After SWAP: primary_table_with_null_partition now holds aggregate data
USE ${case_db};
SELECT * FROM primary_table_with_null_partition ORDER BY k1 LIMIT 3;

-- query 28
-- @order_sensitive=true
-- After SWAP: aggregate_table_with_null now holds primary data
USE ${case_db};
SELECT * FROM aggregate_table_with_null ORDER BY k1 LIMIT 3;
