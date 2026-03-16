-- Migrated from dev/test/sql/test_bucket_shuffle_right_join
-- Test Objective:
-- 1. Validate that RIGHT OUTER JOIN [bucket] and RIGHT OUTER JOIN [shuffle] produce identical results.
-- 2. The right (build) table is a derived table where most join-key values are NULL
--    due to: if(murmur_hash3_32(c0)=0, c0, NULL). Most right rows are unmatched.
-- 3. A secondary LEFT JOIN [bucket/shuffle] t2 on the same (often-NULL) t1.c0 is also included.
-- 4. Results are compared via fingerprinting: count(distinct fp)=1 asserts equivalence.
-- 5. Tests 10 different left-table filter values (c0 in 0..9) to cover all bucket assignments.
--
-- Regression: COUNT(DISTINCT ...) was incorrectly returning 0 for negative fingerprint values
-- (multi_distinct_count was mapped to CountDistinctNonNegative, silently skipping negatives).

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t0(c0 BIGINT NOT NULL, c1 BIGINT NOT NULL, c2 BIGINT NOT NULL)
  ENGINE=OLAP DUPLICATE KEY(c0)
  DISTRIBUTED BY HASH(c0) BUCKETS 9
  PROPERTIES('replication_num'='1');

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1(c0 BIGINT NOT NULL, c1 BIGINT NOT NULL, c2 BIGINT NOT NULL)
  ENGINE=OLAP DUPLICATE KEY(c0)
  DISTRIBUTED BY HASH(c0) BUCKETS 9
  PROPERTIES('replication_num'='1');

-- query 3
-- @skip_result_check=true
CREATE TABLE ${case_db}.t2(c0 BIGINT NOT NULL, c1 BIGINT NOT NULL, c2 BIGINT NOT NULL)
  ENGINE=OLAP DUPLICATE KEY(c0)
  DISTRIBUTED BY HASH(c0) BUCKETS 9
  PROPERTIES('replication_num'='1');

-- query 4
-- @skip_result_check=true
CREATE TABLE ${case_db}.r(fp BIGINT NULL)
  ENGINE=OLAP DUPLICATE KEY(fp)
  DISTRIBUTED BY HASH(fp) BUCKETS 1
  PROPERTIES('replication_num'='1');

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.t0 (c0,c1,c2) VALUES
  (0,0,0),(1,1,1),(2,2,2),(3,3,3),(4,4,4),
  (5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9);

-- query 6
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 SELECT * FROM ${case_db}.t0;

-- query 7
-- @skip_result_check=true
INSERT INTO ${case_db}.t2 SELECT * FROM ${case_db}.t0;

-- query 8
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.r;
INSERT INTO ${case_db}.r (fp) SELECT (sum(murmur_hash3_32(ifnull(c0,0))+murmur_hash3_32(ifnull(c1,0))+murmur_hash3_32(ifnull(c2,0))+murmur_hash3_32(ifnull(ab,0)))) FROM (SELECT t0.c0,t0.c1,t0.c2,t1.c2 AS ab FROM (SELECT * FROM ${case_db}.t1 WHERE c0 IN (0)) t1 RIGHT OUTER JOIN[bucket] (SELECT if(murmur_hash3_32(c0)=0,c0,NULL) AS c0,c1,c2 FROM ${case_db}.t0) t0 ON t0.c0=t1.c0 LEFT JOIN[bucket] ${case_db}.t2 ON t2.c0=t1.c0) AS t;
INSERT INTO ${case_db}.r (fp) SELECT (sum(murmur_hash3_32(ifnull(c0,0))+murmur_hash3_32(ifnull(c1,0))+murmur_hash3_32(ifnull(c2,0))+murmur_hash3_32(ifnull(ab,0)))) FROM (SELECT t0.c0,t0.c1,t0.c2,t1.c2 AS ab FROM (SELECT * FROM ${case_db}.t1 WHERE c0 IN (0)) t1 RIGHT OUTER JOIN[shuffle] (SELECT if(murmur_hash3_32(c0)=0,c0,NULL) AS c0,c1,c2 FROM ${case_db}.t0) t0 ON t0.c0=t1.c0 LEFT JOIN[shuffle] ${case_db}.t2 ON t2.c0=t1.c0) AS t;

-- query 9
SELECT assert_true(count(fp)=2), assert_true(count(distinct fp)=1) FROM ${case_db}.r;

-- query 10
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.r;
INSERT INTO ${case_db}.r (fp) SELECT (sum(murmur_hash3_32(ifnull(c0,0))+murmur_hash3_32(ifnull(c1,0))+murmur_hash3_32(ifnull(c2,0))+murmur_hash3_32(ifnull(ab,0)))) FROM (SELECT t0.c0,t0.c1,t0.c2,t1.c2 AS ab FROM (SELECT * FROM ${case_db}.t1 WHERE c0 IN (1)) t1 RIGHT OUTER JOIN[bucket] (SELECT if(murmur_hash3_32(c0)=0,c0,NULL) AS c0,c1,c2 FROM ${case_db}.t0) t0 ON t0.c0=t1.c0 LEFT JOIN[bucket] ${case_db}.t2 ON t2.c0=t1.c0) AS t;
INSERT INTO ${case_db}.r (fp) SELECT (sum(murmur_hash3_32(ifnull(c0,0))+murmur_hash3_32(ifnull(c1,0))+murmur_hash3_32(ifnull(c2,0))+murmur_hash3_32(ifnull(ab,0)))) FROM (SELECT t0.c0,t0.c1,t0.c2,t1.c2 AS ab FROM (SELECT * FROM ${case_db}.t1 WHERE c0 IN (1)) t1 RIGHT OUTER JOIN[shuffle] (SELECT if(murmur_hash3_32(c0)=0,c0,NULL) AS c0,c1,c2 FROM ${case_db}.t0) t0 ON t0.c0=t1.c0 LEFT JOIN[shuffle] ${case_db}.t2 ON t2.c0=t1.c0) AS t;

-- query 11
SELECT assert_true(count(fp)=2), assert_true(count(distinct fp)=1) FROM ${case_db}.r;

-- query 12
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.r;
INSERT INTO ${case_db}.r (fp) SELECT (sum(murmur_hash3_32(ifnull(c0,0))+murmur_hash3_32(ifnull(c1,0))+murmur_hash3_32(ifnull(c2,0))+murmur_hash3_32(ifnull(ab,0)))) FROM (SELECT t0.c0,t0.c1,t0.c2,t1.c2 AS ab FROM (SELECT * FROM ${case_db}.t1 WHERE c0 IN (2)) t1 RIGHT OUTER JOIN[bucket] (SELECT if(murmur_hash3_32(c0)=0,c0,NULL) AS c0,c1,c2 FROM ${case_db}.t0) t0 ON t0.c0=t1.c0 LEFT JOIN[bucket] ${case_db}.t2 ON t2.c0=t1.c0) AS t;
INSERT INTO ${case_db}.r (fp) SELECT (sum(murmur_hash3_32(ifnull(c0,0))+murmur_hash3_32(ifnull(c1,0))+murmur_hash3_32(ifnull(c2,0))+murmur_hash3_32(ifnull(ab,0)))) FROM (SELECT t0.c0,t0.c1,t0.c2,t1.c2 AS ab FROM (SELECT * FROM ${case_db}.t1 WHERE c0 IN (2)) t1 RIGHT OUTER JOIN[shuffle] (SELECT if(murmur_hash3_32(c0)=0,c0,NULL) AS c0,c1,c2 FROM ${case_db}.t0) t0 ON t0.c0=t1.c0 LEFT JOIN[shuffle] ${case_db}.t2 ON t2.c0=t1.c0) AS t;

-- query 13
SELECT assert_true(count(fp)=2), assert_true(count(distinct fp)=1) FROM ${case_db}.r;

-- query 14
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.r;
INSERT INTO ${case_db}.r (fp) SELECT (sum(murmur_hash3_32(ifnull(c0,0))+murmur_hash3_32(ifnull(c1,0))+murmur_hash3_32(ifnull(c2,0))+murmur_hash3_32(ifnull(ab,0)))) FROM (SELECT t0.c0,t0.c1,t0.c2,t1.c2 AS ab FROM (SELECT * FROM ${case_db}.t1 WHERE c0 IN (3)) t1 RIGHT OUTER JOIN[bucket] (SELECT if(murmur_hash3_32(c0)=0,c0,NULL) AS c0,c1,c2 FROM ${case_db}.t0) t0 ON t0.c0=t1.c0 LEFT JOIN[bucket] ${case_db}.t2 ON t2.c0=t1.c0) AS t;
INSERT INTO ${case_db}.r (fp) SELECT (sum(murmur_hash3_32(ifnull(c0,0))+murmur_hash3_32(ifnull(c1,0))+murmur_hash3_32(ifnull(c2,0))+murmur_hash3_32(ifnull(ab,0)))) FROM (SELECT t0.c0,t0.c1,t0.c2,t1.c2 AS ab FROM (SELECT * FROM ${case_db}.t1 WHERE c0 IN (3)) t1 RIGHT OUTER JOIN[shuffle] (SELECT if(murmur_hash3_32(c0)=0,c0,NULL) AS c0,c1,c2 FROM ${case_db}.t0) t0 ON t0.c0=t1.c0 LEFT JOIN[shuffle] ${case_db}.t2 ON t2.c0=t1.c0) AS t;

-- query 15
SELECT assert_true(count(fp)=2), assert_true(count(distinct fp)=1) FROM ${case_db}.r;

-- query 16
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.r;
INSERT INTO ${case_db}.r (fp) SELECT (sum(murmur_hash3_32(ifnull(c0,0))+murmur_hash3_32(ifnull(c1,0))+murmur_hash3_32(ifnull(c2,0))+murmur_hash3_32(ifnull(ab,0)))) FROM (SELECT t0.c0,t0.c1,t0.c2,t1.c2 AS ab FROM (SELECT * FROM ${case_db}.t1 WHERE c0 IN (4)) t1 RIGHT OUTER JOIN[bucket] (SELECT if(murmur_hash3_32(c0)=0,c0,NULL) AS c0,c1,c2 FROM ${case_db}.t0) t0 ON t0.c0=t1.c0 LEFT JOIN[bucket] ${case_db}.t2 ON t2.c0=t1.c0) AS t;
INSERT INTO ${case_db}.r (fp) SELECT (sum(murmur_hash3_32(ifnull(c0,0))+murmur_hash3_32(ifnull(c1,0))+murmur_hash3_32(ifnull(c2,0))+murmur_hash3_32(ifnull(ab,0)))) FROM (SELECT t0.c0,t0.c1,t0.c2,t1.c2 AS ab FROM (SELECT * FROM ${case_db}.t1 WHERE c0 IN (4)) t1 RIGHT OUTER JOIN[shuffle] (SELECT if(murmur_hash3_32(c0)=0,c0,NULL) AS c0,c1,c2 FROM ${case_db}.t0) t0 ON t0.c0=t1.c0 LEFT JOIN[shuffle] ${case_db}.t2 ON t2.c0=t1.c0) AS t;

-- query 17
SELECT assert_true(count(fp)=2), assert_true(count(distinct fp)=1) FROM ${case_db}.r;

-- query 18
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.r;
INSERT INTO ${case_db}.r (fp) SELECT (sum(murmur_hash3_32(ifnull(c0,0))+murmur_hash3_32(ifnull(c1,0))+murmur_hash3_32(ifnull(c2,0))+murmur_hash3_32(ifnull(ab,0)))) FROM (SELECT t0.c0,t0.c1,t0.c2,t1.c2 AS ab FROM (SELECT * FROM ${case_db}.t1 WHERE c0 IN (5)) t1 RIGHT OUTER JOIN[bucket] (SELECT if(murmur_hash3_32(c0)=0,c0,NULL) AS c0,c1,c2 FROM ${case_db}.t0) t0 ON t0.c0=t1.c0 LEFT JOIN[bucket] ${case_db}.t2 ON t2.c0=t1.c0) AS t;
INSERT INTO ${case_db}.r (fp) SELECT (sum(murmur_hash3_32(ifnull(c0,0))+murmur_hash3_32(ifnull(c1,0))+murmur_hash3_32(ifnull(c2,0))+murmur_hash3_32(ifnull(ab,0)))) FROM (SELECT t0.c0,t0.c1,t0.c2,t1.c2 AS ab FROM (SELECT * FROM ${case_db}.t1 WHERE c0 IN (5)) t1 RIGHT OUTER JOIN[shuffle] (SELECT if(murmur_hash3_32(c0)=0,c0,NULL) AS c0,c1,c2 FROM ${case_db}.t0) t0 ON t0.c0=t1.c0 LEFT JOIN[shuffle] ${case_db}.t2 ON t2.c0=t1.c0) AS t;

-- query 19
SELECT assert_true(count(fp)=2), assert_true(count(distinct fp)=1) FROM ${case_db}.r;

-- query 20
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.r;
INSERT INTO ${case_db}.r (fp) SELECT (sum(murmur_hash3_32(ifnull(c0,0))+murmur_hash3_32(ifnull(c1,0))+murmur_hash3_32(ifnull(c2,0))+murmur_hash3_32(ifnull(ab,0)))) FROM (SELECT t0.c0,t0.c1,t0.c2,t1.c2 AS ab FROM (SELECT * FROM ${case_db}.t1 WHERE c0 IN (6)) t1 RIGHT OUTER JOIN[bucket] (SELECT if(murmur_hash3_32(c0)=0,c0,NULL) AS c0,c1,c2 FROM ${case_db}.t0) t0 ON t0.c0=t1.c0 LEFT JOIN[bucket] ${case_db}.t2 ON t2.c0=t1.c0) AS t;
INSERT INTO ${case_db}.r (fp) SELECT (sum(murmur_hash3_32(ifnull(c0,0))+murmur_hash3_32(ifnull(c1,0))+murmur_hash3_32(ifnull(c2,0))+murmur_hash3_32(ifnull(ab,0)))) FROM (SELECT t0.c0,t0.c1,t0.c2,t1.c2 AS ab FROM (SELECT * FROM ${case_db}.t1 WHERE c0 IN (6)) t1 RIGHT OUTER JOIN[shuffle] (SELECT if(murmur_hash3_32(c0)=0,c0,NULL) AS c0,c1,c2 FROM ${case_db}.t0) t0 ON t0.c0=t1.c0 LEFT JOIN[shuffle] ${case_db}.t2 ON t2.c0=t1.c0) AS t;

-- query 21
SELECT assert_true(count(fp)=2), assert_true(count(distinct fp)=1) FROM ${case_db}.r;

-- query 22
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.r;
INSERT INTO ${case_db}.r (fp) SELECT (sum(murmur_hash3_32(ifnull(c0,0))+murmur_hash3_32(ifnull(c1,0))+murmur_hash3_32(ifnull(c2,0))+murmur_hash3_32(ifnull(ab,0)))) FROM (SELECT t0.c0,t0.c1,t0.c2,t1.c2 AS ab FROM (SELECT * FROM ${case_db}.t1 WHERE c0 IN (7)) t1 RIGHT OUTER JOIN[bucket] (SELECT if(murmur_hash3_32(c0)=0,c0,NULL) AS c0,c1,c2 FROM ${case_db}.t0) t0 ON t0.c0=t1.c0 LEFT JOIN[bucket] ${case_db}.t2 ON t2.c0=t1.c0) AS t;
INSERT INTO ${case_db}.r (fp) SELECT (sum(murmur_hash3_32(ifnull(c0,0))+murmur_hash3_32(ifnull(c1,0))+murmur_hash3_32(ifnull(c2,0))+murmur_hash3_32(ifnull(ab,0)))) FROM (SELECT t0.c0,t0.c1,t0.c2,t1.c2 AS ab FROM (SELECT * FROM ${case_db}.t1 WHERE c0 IN (7)) t1 RIGHT OUTER JOIN[shuffle] (SELECT if(murmur_hash3_32(c0)=0,c0,NULL) AS c0,c1,c2 FROM ${case_db}.t0) t0 ON t0.c0=t1.c0 LEFT JOIN[shuffle] ${case_db}.t2 ON t2.c0=t1.c0) AS t;

-- query 23
SELECT assert_true(count(fp)=2), assert_true(count(distinct fp)=1) FROM ${case_db}.r;

-- query 24
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.r;
INSERT INTO ${case_db}.r (fp) SELECT (sum(murmur_hash3_32(ifnull(c0,0))+murmur_hash3_32(ifnull(c1,0))+murmur_hash3_32(ifnull(c2,0))+murmur_hash3_32(ifnull(ab,0)))) FROM (SELECT t0.c0,t0.c1,t0.c2,t1.c2 AS ab FROM (SELECT * FROM ${case_db}.t1 WHERE c0 IN (8)) t1 RIGHT OUTER JOIN[bucket] (SELECT if(murmur_hash3_32(c0)=0,c0,NULL) AS c0,c1,c2 FROM ${case_db}.t0) t0 ON t0.c0=t1.c0 LEFT JOIN[bucket] ${case_db}.t2 ON t2.c0=t1.c0) AS t;
INSERT INTO ${case_db}.r (fp) SELECT (sum(murmur_hash3_32(ifnull(c0,0))+murmur_hash3_32(ifnull(c1,0))+murmur_hash3_32(ifnull(c2,0))+murmur_hash3_32(ifnull(ab,0)))) FROM (SELECT t0.c0,t0.c1,t0.c2,t1.c2 AS ab FROM (SELECT * FROM ${case_db}.t1 WHERE c0 IN (8)) t1 RIGHT OUTER JOIN[shuffle] (SELECT if(murmur_hash3_32(c0)=0,c0,NULL) AS c0,c1,c2 FROM ${case_db}.t0) t0 ON t0.c0=t1.c0 LEFT JOIN[shuffle] ${case_db}.t2 ON t2.c0=t1.c0) AS t;

-- query 25
SELECT assert_true(count(fp)=2), assert_true(count(distinct fp)=1) FROM ${case_db}.r;

-- query 26
-- @skip_result_check=true
TRUNCATE TABLE ${case_db}.r;
INSERT INTO ${case_db}.r (fp) SELECT (sum(murmur_hash3_32(ifnull(c0,0))+murmur_hash3_32(ifnull(c1,0))+murmur_hash3_32(ifnull(c2,0))+murmur_hash3_32(ifnull(ab,0)))) FROM (SELECT t0.c0,t0.c1,t0.c2,t1.c2 AS ab FROM (SELECT * FROM ${case_db}.t1 WHERE c0 IN (9)) t1 RIGHT OUTER JOIN[bucket] (SELECT if(murmur_hash3_32(c0)=0,c0,NULL) AS c0,c1,c2 FROM ${case_db}.t0) t0 ON t0.c0=t1.c0 LEFT JOIN[bucket] ${case_db}.t2 ON t2.c0=t1.c0) AS t;
INSERT INTO ${case_db}.r (fp) SELECT (sum(murmur_hash3_32(ifnull(c0,0))+murmur_hash3_32(ifnull(c1,0))+murmur_hash3_32(ifnull(c2,0))+murmur_hash3_32(ifnull(ab,0)))) FROM (SELECT t0.c0,t0.c1,t0.c2,t1.c2 AS ab FROM (SELECT * FROM ${case_db}.t1 WHERE c0 IN (9)) t1 RIGHT OUTER JOIN[shuffle] (SELECT if(murmur_hash3_32(c0)=0,c0,NULL) AS c0,c1,c2 FROM ${case_db}.t0) t0 ON t0.c0=t1.c0 LEFT JOIN[shuffle] ${case_db}.t2 ON t2.c0=t1.c0) AS t;

-- query 27
SELECT assert_true(count(fp)=2), assert_true(count(distinct fp)=1) FROM ${case_db}.r;
