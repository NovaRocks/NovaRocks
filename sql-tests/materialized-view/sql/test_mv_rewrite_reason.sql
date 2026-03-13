-- Test Objective:
-- 1. Validate rewrite-reason reporting when the optimizer skips MV rewrite.
-- 2. Cover diagnosability for negative rewrite decisions.
-- Source: dev/test/sql/test_materialized_view/T/test_mv_reasoning

-- query 1
create database db_${uuid0};

-- query 2
use db_${uuid0};

-- query 3
CREATE TABLE t1(
    c1 date,
    c2 int,
    c3 int,
    c4 string
)
PARTITION BY date_trunc('DAY', c1);

-- query 4
INSERT INTO t1 VALUES ('1988-07-01', 1, 1, 'a');

-- query 5
INSERT INTO t1 VALUES ('1988-07-01', 1, 2, 'a');

-- query 6
INSERT INTO t1 VALUES ('1988-07-02', 1, 1, 'a');

-- query 7
INSERT INTO t1 VALUES ('1988-07-02', 1, 2, 'a');

-- query 8
INSERT INTO t1 VALUES ('1988-07-03', 1, 1, 'a');

-- query 9
INSERT INTO t1 VALUES ('1988-07-03', 1, 2, 'a');

-- query 10
-- Aggregation MV
CREATE MATERIALIZED VIEW mv1
PARTITION BY (c1)
REFRESH ASYNC
AS
    SELECT c1, c2, sum(c3), count(c3)
    FROM t1
    GROUP BY c1, c2;

-- query 11
REFRESH MATERIALIZED VIEW mv1 WITH SYNC MODE;

-- query 12
-- normal rewrite
-- Successful TRACE REASON output is effectively "no reason". FE may render that as either no rows
-- or a single empty row, so assert on the absence of failure diagnostics instead of exact row shape.
-- @skip_result_check=true
-- @result_not_contains=MV rewrite fail
TRACE REASON MV
    SELECT c1, c2, sum(c3), count(c3)
    FROM t1
    GROUP BY c1, c2;

-- query 13
-- @skip_result_check=true
-- @result_not_contains=MV rewrite fail
TRACE REASON MV
    SELECT c1, sum(c3), count(c3)
    FROM t1
    GROUP BY c1;

-- query 14
-- @skip_result_check=true
-- @result_not_contains=MV rewrite fail
TRACE REASON MV
    SELECT c1, sum(c3), count(c3)
    FROM t1
    WHERE c2 > 1
    GROUP BY c1;

-- query 15
-- cannot rewrite
-- @skip_result_check=true
-- @result_contains=Rewrite group by key failed: 4: c4
-- @result_contains=Rewrite rollup aggregate failed
TRACE REASON MV
    SELECT c4, sum(c3), count(c3)
    FROM t1
    WHERE c2 > 1
    GROUP BY c4;

-- query 16
-- @skip_result_check=true
-- @result_contains=Rewrite scalar operator failed: 4: c4 > a cannot be rewritten
TRACE REASON MV
    SELECT c1, sum(c3), count(c3)
    FROM t1
    WHERE c4 > 'a'
    GROUP BY c1;

-- query 17
-- Projection MV
DROP MATERIALIZED VIEW mv1;

-- query 18
CREATE MATERIALIZED VIEW mv1
REFRESH ASYNC
PARTITION BY (c1)
AS
    SELECT c1, c2, c3
    FROM t1;

-- query 19
REFRESH MATERIALIZED VIEW mv1 WITH SYNC MODE;

-- query 20
-- @skip_result_check=true
-- @result_not_contains=MV rewrite fail
TRACE REASON MV
    SELECT c1, c2, sum(c3), count(c3)
    FROM t1
    GROUP BY c1, c2;

-- query 21
-- @skip_result_check=true
-- @result_contains=Rewrite projection failed: cannot totally rewrite expr 4: c4
TRACE REASON MV
    SELECT c1, c2, c4, sum(c3), count(c3)
    FROM t1
    GROUP BY c1, c2, c4;

-- query 22
-- @skip_result_check=true
-- @result_contains=Rewrite projection failed: cannot totally rewrite expr 4: c4
TRACE REASON MV
    SELECT c1, c2, c4
    FROM t1;

-- query 23
-- @skip_result_check=true
-- @result_contains=Rewrite scalar operator failed: 4: c4 = a cannot be rewritten
TRACE REASON MV
    SELECT c1, c2
    FROM t1
    WHERE c4='a';

-- query 24
ALTER MATERIALIZED VIEW mv1 INACTIVE;

-- query 25
-- @skip_result_check=true
-- @result_contains=MV rewrite fail for mv1: is not active
TRACE REASON MV
    SELECT c1, c2, sum(c3), count(c3)
    FROM t1
    GROUP BY c1, c2;

-- query 26
-- MV Unsupported
DROP MATERIALIZED VIEW mv1;

-- query 27
CREATE MATERIALIZED VIEW mv1
REFRESH ASYNC
PARTITION BY (c1)
AS
    SELECT c1, c2, c3
    FROM t1
    ORDER BY c1;

-- query 28
-- @skip_result_check=true
-- @result_contains=MV contains non-SPJG operators(no view rewrite): LOGICAL_TOPN
TRACE REASON MV
    SELECT c1, c2, c3
    FROM t1
    ORDER BY c1;

-- query 29
drop database db_${uuid0} force;
