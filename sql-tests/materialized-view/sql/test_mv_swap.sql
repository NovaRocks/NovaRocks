-- Test Objective:
-- 1. Validate table swap operations preserve MV metadata and query behavior.
-- 2. Cover swap semantics across refresh and query execution.
-- Source: dev/test/sql/test_materialized_view/T/test_mv_swap

-- query 1
admin set frontend config('enable_mv_automatic_active_check'='false');

-- query 2
create database db_${uuid0};

-- query 3
use db_${uuid0};

-- query 4
CREATE TABLE ss( event_day DATE, pv BIGINT) DUPLICATE KEY(event_day) DISTRIBUTED BY HASH(event_day) BUCKETS 8 PROPERTIES("replication_num" = "1");

-- query 5
CREATE TABLE jj( event_day DATE, pv BIGINT) DUPLICATE KEY(event_day) DISTRIBUTED BY HASH(event_day) BUCKETS 8 PROPERTIES("replication_num" = "1");

-- query 6
insert into ss values('2020-01-14', 2);

-- query 7
insert into ss values('2020-01-14', 3);

-- query 8
insert into ss values('2020-01-15', 2);

-- query 9
insert into jj values('2020-01-14', 2);

-- query 10
insert into jj values('2020-01-14', 3);

-- query 11
insert into jj values('2020-01-15', 2);

-- query 12
CREATE MATERIALIZED VIEW mv1 DISTRIBUTED BY hash(event_day) AS SELECT event_day, sum(pv) as sum_pv FROM ss GROUP BY event_day;

-- query 13
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv1 with sync mode ;

-- query 14
CREATE MATERIALIZED VIEW mv2 DISTRIBUTED BY hash(event_day) AS SELECT event_day, count(pv) as count_pv FROM ss GROUP BY event_day;

-- query 15
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv2 with sync mode ;

-- query 16
SELECT * FROM mv1 ORDER BY event_day;

-- query 17
SELECT * FROM mv2 ORDER BY event_day;

-- query 18
ALTER MATERIALIZED VIEW mv1 SWAP WITH mv2;

-- query 19
SELECT * FROM mv1 ORDER BY event_day;

-- query 20
SELECT * FROM mv2 ORDER BY event_day;

-- query 21
DESC mv1;

-- query 22
DESC mv2;

-- query 23
-- refresh again
INSERT INTO ss values('2020-01-15', 2);

-- query 24
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv1 with sync mode;

-- query 25
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv2 with sync mode;

-- query 26
SELECT * FROM mv1 ORDER BY event_day;

-- query 27
SELECT * FROM mv2 ORDER BY event_day;

-- query 28
-- Try to swap with a table
-- @expect_error=Materialized view can only SWAP WITH materialized view
ALTER MATERIALIZED VIEW mv1 SWAP WITH ss;

-- query 29
-- @expect_error=Materialized view can only SWAP WITH materialized view
ALTER TABLE ss SWAP WITH mv1;

-- query 30
-- Try to swap with self
-- @expect_error=New name conflicts with rollup index name
ALTER MATERIALIZED VIEW mv1 SWAP WITH mv1;

-- query 31
-- MV on MV
ALTER MATERIALIZED VIEW mv1 SWAP WITH mv2;

-- query 32
CREATE MATERIALIZED VIEW mv_on_mv_1 REFRESH ASYNC
AS SELECT sum(sum_pv) as sum_sum_pv FROM mv1;

-- query 33
CREATE MATERIALIZED VIEW mv_on_mv_2 REFRESH ASYNC
AS SELECT sum_sum_pv + 1 FROM mv_on_mv_1;

-- query 34
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_on_mv_1 with sync mode;

-- query 35
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_on_mv_2 with sync mode;

-- query 36
-- swap intermediate MV
ALTER MATERIALIZED VIEW mv1 SWAP WITH mv2;

-- query 37
select sleep(3);

-- query 38
SELECT IS_ACTIVE, INACTIVE_REASON FROM information_schema.materialized_views WHERE table_name = 'mv_on_mv_1' and TABLE_SCHEMA='db_${uuid0}';

-- query 39
SELECT IS_ACTIVE, INACTIVE_REASON FROM information_schema.materialized_views WHERE table_name = 'mv_on_mv_2' and TABLE_SCHEMA='db_${uuid0}';

-- query 40
-- The dependent MV remains non-activatable immediately after swapping its upstream MV chain.
-- @expect_error=Can not active materialized view [mv_on_mv_1]
ALTER MATERIALIZED VIEW mv_on_mv_1 ACTIVE;

-- query 41
-- The second-level dependent MV is blocked for the same reason.
-- @expect_error=Can not active materialized view [mv_on_mv_2]
ALTER MATERIALIZED VIEW mv_on_mv_2 ACTIVE;

-- query 42
SELECT IS_ACTIVE, INACTIVE_REASON FROM information_schema.materialized_views WHERE table_name = 'mv_on_mv_1' and TABLE_SCHEMA='db_${uuid0}';

-- query 43
SELECT IS_ACTIVE, INACTIVE_REASON FROM information_schema.materialized_views WHERE table_name = 'mv_on_mv_2' and TABLE_SCHEMA='db_${uuid0}';

-- query 44
-- swap back
ALTER MATERIALIZED VIEW mv1 SWAP WITH mv2;

-- query 45
select sleep(3);

-- query 46
ALTER MATERIALIZED VIEW mv_on_mv_1 ACTIVE;

-- query 47
ALTER MATERIALIZED VIEW mv_on_mv_2 ACTIVE;

-- query 48
SELECT IS_ACTIVE, INACTIVE_REASON FROM information_schema.materialized_views WHERE table_name = 'mv_on_mv_1' and TABLE_SCHEMA='db_${uuid0}';

-- query 49
SELECT IS_ACTIVE, INACTIVE_REASON FROM information_schema.materialized_views WHERE table_name = 'mv_on_mv_2' and TABLE_SCHEMA='db_${uuid0}';

-- query 50
-- swap base table
CREATE MATERIALIZED VIEW mv_on_table_1 REFRESH ASYNC
AS SELECT ss.event_day, sum(ss.pv) as ss_sum_pv, sum(jj.pv) as jj_sum_pv
    FROM ss JOIN jj on (ss.event_day = jj.event_day)
    GROUP BY ss.event_day;

-- query 51
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_on_table_1 with sync mode ;

-- query 52
ALTER TABLE ss SWAP WITH jj;

-- query 53
select sleep(3);

-- query 54
SELECT IS_ACTIVE, INACTIVE_REASON FROM information_schema.materialized_views WHERE table_name = 'mv_on_table_1' and TABLE_SCHEMA='db_${uuid0}';

-- query 55
ALTER MATERIALIZED VIEW mv_on_table_1 ACTIVE;

-- query 56
SELECT IS_ACTIVE, INACTIVE_REASON FROM information_schema.materialized_views WHERE table_name = 'mv_on_table_1' and TABLE_SCHEMA='db_${uuid0}';

-- query 57
-- @skip_result_check=true
drop database db_${uuid0} force;

-- query 58
admin set frontend config('enable_mv_automatic_active_check'='true');
