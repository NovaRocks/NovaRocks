-- Test Objective:
-- 1. Validate text-based rewrite still selects the intended MV plan.
-- 2. Cover textual equivalence rewrite matching.
-- Source: dev/test/sql/test_materialized_view/T/test_materialized_view_text_based_rewrite

-- query 1
create table user_tags (time date, user_id int, user_name varchar(20), tag_id int) partition by range (time)  (partition p1 values less than MAXVALUE) distributed by hash(time) buckets 3 properties('replication_num' = '1');

-- query 2
insert into user_tags values('2023-04-11', 1, 'a', 1), ('2023-04-12', 2, 'e', 5),('2023-04-13', 3, 'e', 6);

-- query 3
set enable_materialized_view_text_match_rewrite=true;

-- query 4
CREATE MATERIALIZED VIEW mv1 REFRESH DEFERRED MANUAL AS
select user_id, time, sum(tag_id) from user_tags group by user_id, time order by user_id, time;

-- query 5
REFRESH MATERIALIZED VIEW mv1 with sync mode;

-- query 6
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select user_id, time, sum(tag_id) from user_tags group by user_id, time order by user_id, time;

-- query 7
select user_id, time, sum(tag_id) from user_tags group by user_id, time order by user_id, time;

-- query 8
drop materialized view mv1;

-- query 9
CREATE MATERIALIZED VIEW mv1 REFRESH DEFERRED MANUAL AS
select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time order by user_id + 1, time;

-- query 10
REFRESH MATERIALIZED VIEW mv1 with sync mode;

-- query 11
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time order by user_id + 1, time;

-- query 12
select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time order by user_id + 1, time;

-- query 13
drop materialized view mv1;

-- query 14
CREATE MATERIALIZED VIEW mv1 REFRESH DEFERRED MANUAL AS
select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time;

-- query 15
REFRESH MATERIALIZED VIEW mv1 with sync mode;

-- query 16
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time;

-- query 17
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select * from (select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time) as t;

-- query 18
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select * from (select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time) as t order by time;

-- query 19
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select * from (select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time) as t where time='2023-4-13';

-- query 20
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select * from (select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time) as t where time>='2023-4-13' order by time;

-- query 21
select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time order by user_id + 1, time;

-- query 22
select * from (select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time) as t order by time;

-- query 23
select * from (select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time) as t where time='2023-4-13';

-- query 24
select * from (select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time) as t where time>='2023-4-13' order by time;

-- query 25
drop materialized view mv1;

-- query 26
CREATE MATERIALIZED VIEW mv1 REFRESH DEFERRED MANUAL AS
select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time order by user_id + 1, time limit 3;

-- query 27
REFRESH MATERIALIZED VIEW mv1 with sync mode;

-- query 28
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time order by user_id + 1, time limit 3;

-- query 29
select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time order by user_id + 1, time limit 3;

-- query 30
drop materialized view mv1;

-- query 31
CREATE MATERIALIZED VIEW mv1 REFRESH DEFERRED MANUAL AS
select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time
union all
select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time;

-- query 32
REFRESH MATERIALIZED VIEW mv1 with sync mode;

-- query 33
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time union all select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time;

-- query 34
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select * from (select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time union all select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time) as t order by time;

-- query 35
select * from (
    select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time
    union all
    select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time
) as t order by time;

-- query 36
drop materialized view mv1;

-- query 37
CREATE MATERIALIZED VIEW mv1 REFRESH DEFERRED MANUAL AS
select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time;

-- query 38
REFRESH MATERIALIZED VIEW mv1 with sync mode;

-- query 39
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time union all select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time;

-- query 40
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select * from (select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time union all select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time) as t order by time;

-- query 41
select * from (
    select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time
    union all
    select user_id + 1, time, sum(tag_id) from user_tags group by user_id + 1, time
) as t order by time;

-- query 42
drop materialized view mv1;
