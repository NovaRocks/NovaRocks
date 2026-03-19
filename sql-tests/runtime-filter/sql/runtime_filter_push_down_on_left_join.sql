-- Test Objective:
-- 1. Validate that runtime filter is NOT incorrectly pushed through LEFT JOIN
--    onto columns produced by COALESCE on the left side.
-- 2. The outer join produces c2 via coalesce(t2.c2, 'unknown')='c2-1',
--    but t3 has c2='unknown', so the final join should return 0 rows.
-- 3. Cover both broadcast and shuffle variants of the left join.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
  c1 STRING,
  c2 STRING,
  c3 STRING
) ENGINE=OLAP
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE ${case_db}.t2 (
  c1 STRING,
  c2 STRING,
  c3 STRING
) ENGINE=OLAP
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE ${case_db}.t3 (
  c1 STRING,
  c2 STRING,
  c3 STRING
) ENGINE=OLAP
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1) BUCKETS 4
PROPERTIES ("replication_num" = "1");

INSERT INTO ${case_db}.t3 SELECT 'c1-1', 'unknown', 'c3';
INSERT INTO ${case_db}.t2 SELECT 'c1-1', 'c2-1', 'c3';
INSERT INTO ${case_db}.t1 SELECT 'c1-1', 'c2-1', 'c3';

-- query 2
-- Left join [broadcast]: coalesce output c2='c2-1' must NOT be incorrectly used
-- to push RF into t1 scan, because the final join to t3 requires c2='unknown'.
-- Expected: 0 rows (coalesce result 'c2-1' != t3.c2 'unknown').
with
  w2 as (
    select
      t1.c1,
      coalesce(t2.c2, 'unknown') as c2
    from ${case_db}.t1 left join [broadcast] ${case_db}.t2 on t1.c3 = t2.c3
  )
select
  w2.*
from
  w2
  join [bucket] ${case_db}.t3 on w2.c1 = t3.c1 and w2.c2 = t3.c2;

-- query 3
-- Shuffle variant: same semantics, same expected empty result.
with
  w2 as (
    select
      t1.c1,
      coalesce(t2.c2, 'unknown') as c2
    from ${case_db}.t1 left join [shuffle] ${case_db}.t2 on t1.c3 = t2.c3
  )
select
  w2.*
from
  w2
  join [shuffle] ${case_db}.t3 on w2.c1 = t3.c1 and w2.c2 = t3.c2;
