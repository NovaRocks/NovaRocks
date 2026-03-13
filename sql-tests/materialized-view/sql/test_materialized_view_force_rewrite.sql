-- Test Objective:
-- 1. Validate force rewrite session controls can require MV-based plans.
-- 2. Cover forced hit and non-hit behavior on the same logical query.
-- Source: dev/test/sql/test_materialized_view/T/test_materialized_view_force_rewrite

-- query 1
CREATE TABLE t1(
    t1_id INT not null,
    t1_t2_id INT not null,
    t1_t3_id INT not null,
    t1_name varchar(20) not null,
    t1_age INT not null
)
DUPLICATE KEY(t1_id)
DISTRIBUTED BY HASH(t1_id);

-- query 2
CREATE TABLE t2(
    t2_id INT,
    t2_name varchar(20) not null
)
DUPLICATE KEY(t2_id)
DISTRIBUTED BY HASH(t2_id);

-- query 3
CREATE TABLE t3(
    t3_id INT not null,
    t3_name varchar(20) not null
)
DUPLICATE KEY(t3_id)
DISTRIBUTED BY HASH(t3_id);

-- query 4
INSERT INTO t1 VALUES (1,1,1,"jack",18), (2,2,2,"nacy",18);

-- query 5
INSERT INTO t2 VALUES (1,"beijing"),(2,"tianjin");

-- query 6
INSERT INTO t3 VALUES (1,"wuhan"),(2,"shanghai");

-- query 7
CREATE MATERIALIZED VIEW mv1
DISTRIBUTED BY HASH(t1_id) BUCKETS 48
REFRESH MANUAL
PROPERTIES (
    "replication_num" = "1",
    "unique_constraints" = "t2.t2_id;t3.t3_id",
    "foreign_key_constraints" = "t1(t1_t2_id) REFERENCES t2(t2_id);t1(t1_t3_id) REFERENCES t3(t3_id);"
)
AS
    SELECT t1.t1_id, bitmap_union(to_bitmap(t1.t1_age)), hll_union(hll_hash(t1.t1_age)), percentile_union(percentile_hash(t1.t1_age)) FROM t1
    INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id
    INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id;

-- query 8
refresh materialized view mv1 with sync mode;

-- query 9
analyze full table mv1;

-- query 10
set enable_force_rule_based_mv_rewrite=false;

-- query 11
set materialized_view_rewrite_mode="disable";

-- query 12
-- hit
-- @result_not_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT t1.t1_id, bitmap_union(to_bitmap(t1.t1_age)) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id;

-- query 13
-- hit
-- @result_not_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT t1.t1_id, count(distinct t1.t1_age) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id;

-- query 14
-- hit
SELECT t1.t1_id, bitmap_union_count(to_bitmap(t1.t1_age)) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id order by t1.t1_id;

-- query 15
-- hit
SELECT t1.t1_id, count(distinct t1.t1_age) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id order by t1.t1_id;

-- query 16
-- no hit
SELECT t1.t1_id, bitmap_union_count(to_bitmap(t1.t1_age + 1)) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id order by t1.t1_id;

-- query 17
-- no hit
SELECT t1.t1_id, count(distinct t1.t1_age + 1) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id order by t1.t1_id;

-- query 18
set materialized_view_rewrite_mode="default";

-- query 19
-- hit
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT t1.t1_id, bitmap_union(to_bitmap(t1.t1_age)) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id;

-- query 20
-- hit
-- @result_not_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT t1.t1_id, count(distinct t1.t1_age) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id;

-- query 21
-- hit
SELECT t1.t1_id, bitmap_union_count(to_bitmap(t1.t1_age)) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id order by t1.t1_id;

-- query 22
-- hit
SELECT t1.t1_id, count(distinct t1.t1_age) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id order by t1.t1_id;

-- query 23
-- no hit
SELECT t1.t1_id, bitmap_union_count(to_bitmap(t1.t1_age + 1)) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id order by t1.t1_id;

-- query 24
-- no hit
SELECT t1.t1_id, count(distinct t1.t1_age + 1) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id order by t1.t1_id;

-- query 25
set materialized_view_rewrite_mode="default_or_error";

-- query 26
-- hit
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT t1.t1_id, bitmap_union(to_bitmap(t1.t1_age)) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id;

-- query 27
-- In default_or_error mode the count-distinct variant currently fails instead of forcing an MV plan.
-- @expect_error=no executable plan with materialized view
SELECT t1.t1_id, count(distinct t1.t1_age) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id order by t1.t1_id;

-- query 28
-- In default_or_error mode a non-rewritable bitmap expression must fail instead of falling back.
-- @expect_error=no executable plan with materialized view
SELECT t1.t1_id, bitmap_union_count(to_bitmap(t1.t1_age + 1)) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id order by t1.t1_id;

-- query 29
-- In default_or_error mode the count-distinct variant also errors on fallback.
-- @expect_error=no executable plan with materialized view
SELECT t1.t1_id, count(distinct t1.t1_age + 1) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id order by t1.t1_id;

-- query 30
set materialized_view_rewrite_mode="force";

-- query 31
-- hit
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT t1.t1_id, bitmap_union(to_bitmap(t1.t1_age)) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id;

-- query 32
-- hit
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT t1.t1_id, count(distinct t1.t1_age) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id;

-- query 33
-- hit
SELECT t1.t1_id, bitmap_union_count(to_bitmap(t1.t1_age)) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id order by t1.t1_id;

-- query 34
-- hit
SELECT t1.t1_id, count(distinct t1.t1_age) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id order by t1.t1_id;

-- query 35
-- no hit
SELECT t1.t1_id, bitmap_union_count(to_bitmap(t1.t1_age + 1)) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id order by t1.t1_id;

-- query 36
-- no hit
SELECT t1.t1_id, count(distinct t1.t1_age + 1) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id order by t1.t1_id;

-- query 37
set materialized_view_rewrite_mode="force_or_error";

-- query 38
-- hit
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT t1.t1_id, bitmap_union(to_bitmap(t1.t1_age)) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id;

-- query 39
-- hit
SELECT t1.t1_id, bitmap_union_count(to_bitmap(t1.t1_age)) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id order by t1.t1_id;

-- query 40
-- hit
SELECT t1.t1_id, count(distinct t1.t1_age) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id order by t1.t1_id;

-- query 41
-- In force_or_error mode a non-rewritable bitmap expression must error instead of falling back.
-- @expect_error=no executable plan with materialized view
SELECT t1.t1_id, bitmap_union_count(to_bitmap(t1.t1_age + 1)) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id order by t1.t1_id;

-- query 42
-- In force_or_error mode the count-distinct variant also errors on fallback.
-- @expect_error=no executable plan with materialized view
SELECT t1.t1_id, count(distinct t1.t1_age + 1) FROM t1 INNER JOIN t2 ON t1.t1_t2_id=t2.t2_id INNER JOIN t3 ON t1.t1_t3_id=t3.t3_id group by t1.t1_id order by t1.t1_id;

-- query 43
set materialized_view_rewrite_mode="default";
