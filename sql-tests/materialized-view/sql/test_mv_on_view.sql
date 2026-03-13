-- Test Objective:
-- 1. Validate MVs defined on logical views react correctly to upstream view changes.
-- 2. Cover active/inactive transitions when referenced view definitions change.
-- Source: dev/test/sql/test_materialized_view/T/test_mv_on_view

-- query 1
drop database if exists db_mv_on_view force;
create database db_mv_on_view;

-- query 2
use db_mv_on_view;

-- query 3
CREATE TABLE ss( event_day DATE, pv BIGINT) DUPLICATE KEY(event_day) DISTRIBUTED BY HASH(event_day) BUCKETS 8 PROPERTIES("replication_num" = "1");

-- query 4
CREATE TABLE jj( event_day DATE, pv BIGINT) DUPLICATE KEY(event_day) DISTRIBUTED BY HASH(event_day) BUCKETS 8 PROPERTIES("replication_num" = "1");

-- query 5
insert into ss values('2020-01-14', 2);

-- query 6
insert into ss values('2020-01-14', 3);

-- query 7
insert into ss values('2020-01-15', 2);

-- query 8
insert into jj values('2020-01-16', 1);

-- query 9
CREATE VIEW view1 AS SELECT event_day, sum(pv) as sum_pv FROM ss GROUP BY event_day;

-- query 10
CREATE MATERIALIZED VIEW mv_on_view_1 REFRESH ASYNC AS select * from view1;

-- query 11
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_on_view_1 with sync mode ;

-- query 12
SELECT * FROM mv_on_view_1 ORDER BY event_day;

-- query 13
-- update the base table of view, should update the MV correspondingly
insert into ss values('2020-01-15', 3);

-- query 14
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_on_view_1 with sync mode;

-- query 15
SELECT * FROM mv_on_view_1 ORDER BY event_day;

-- query 16
-- ------------- ALTER VIEW ---------------------------
-- alter view but not changed
ALTER VIEW view1 AS SELECT event_day, sum(pv) as sum_pv FROM ss GROUP BY event_day;

-- query 17
ALTER MATERIALIZED VIEW mv_on_view_1 ACTIVE;

-- query 18
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_on_view_1 with sync mode;

-- query 19
SELECT
    IS_ACTIVE,
    IFNULL(NULLIF(INACTIVE_REASON, ''), '<empty>') AS INACTIVE_REASON
FROM information_schema.materialized_views
WHERE table_name = 'mv_on_view_1';

-- query 20
-- alter view
ALTER VIEW view1 AS SELECT event_day, sum(pv) as sum_pv FROM jj GROUP BY event_day;

-- query 21
ALTER MATERIALIZED VIEW mv_on_view_1 ACTIVE;

-- query 22
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_on_view_1 with sync mode;

-- query 23
SELECT
    IS_ACTIVE,
    IFNULL(NULLIF(INACTIVE_REASON, ''), '<empty>') AS INACTIVE_REASON
FROM information_schema.materialized_views
WHERE table_name = 'mv_on_view_1';

-- query 24
-- alter base table of view: not invalidate
ALTER TABLE jj ADD COLUMN extra int;

-- query 25
-- @result_contains=FINISHED
-- @retry_count=60
-- @retry_interval_ms=500
SHOW ALTER TABLE COLUMN ORDER BY JobId DESC LIMIT 1;

-- query 26
ALTER MATERIALIZED VIEW mv_on_view_1 ACTIVE;

-- query 27
SELECT
    IS_ACTIVE,
    IFNULL(NULLIF(INACTIVE_REASON, ''), '<empty>') AS INACTIVE_REASON
FROM information_schema.materialized_views
WHERE table_name = 'mv_on_view_1';

-- query 28
-- alter base table of view: inactive the MV
ALTER TABLE jj DROP COLUMN pv;

-- query 29
-- @result_contains=FINISHED
-- @retry_count=60
-- @retry_interval_ms=500
SHOW ALTER TABLE COLUMN ORDER BY JobId DESC LIMIT 1;

-- query 30
-- Re-activating should fail while the referenced view definition is no longer analyzable by the MV.
-- @expect_error=Can not active materialized view [mv_on_view_1]
ALTER MATERIALIZED VIEW mv_on_view_1 ACTIVE;

-- query 31
SELECT
    IS_ACTIVE,
    IFNULL(NULLIF(INACTIVE_REASON, ''), '<empty>') AS INACTIVE_REASON
FROM information_schema.materialized_views
WHERE table_name = 'mv_on_view_1';

-- query 32
-- alter base table of view: restoe the column
ALTER TABLE jj ADD COLUMN pv int;

-- query 33
-- @result_contains=FINISHED
-- @retry_count=60
-- @retry_interval_ms=500
SHOW ALTER TABLE COLUMN ORDER BY JobId DESC LIMIT 1;

-- query 34
ALTER MATERIALIZED VIEW mv_on_view_1 ACTIVE;

-- query 35
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_on_view_1 with sync mode;

-- query 36
SELECT
    IS_ACTIVE,
    IFNULL(NULLIF(INACTIVE_REASON, ''), '<empty>') AS INACTIVE_REASON
FROM information_schema.materialized_views
WHERE table_name = 'mv_on_view_1';

-- query 37
-- alter irrelavant table of mv
ALTER TABLE ss DROP COLUMN pv;

-- query 38
SELECT
    IS_ACTIVE,
    IFNULL(NULLIF(INACTIVE_REASON, ''), '<empty>') AS INACTIVE_REASON
FROM information_schema.materialized_views
WHERE table_name = 'mv_on_view_1';

-- query 39
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_on_view_1 with sync mode;

-- query 40
SELECT * FROM mv_on_view_1 ORDER BY event_day;

-- query 41
-- ------------------- DROP AND CREATE view ----------------------------
DROP VIEW view1;

-- query 42
CREATE VIEW view1 AS SELECT event_day, sum(pv) as sum_pv FROM jj GROUP BY event_day;

-- query 43
ALTER MATERIALIZED VIEW mv_on_view_1 ACTIVE;

-- query 44
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_on_view_1 with sync mode;

-- query 45
SELECT
    IS_ACTIVE,
    IFNULL(NULLIF(INACTIVE_REASON, ''), '<empty>') AS INACTIVE_REASON
FROM information_schema.materialized_views
WHERE table_name = 'mv_on_view_1';

-- query 46
SELECT * FROM mv_on_view_1 ORDER BY event_day;

-- query 47
drop database db_mv_on_view force;
