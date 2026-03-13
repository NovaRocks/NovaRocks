-- Test Objective:
-- 1. Validate count-distinct rewrite through bitmap/hll-based materialized views.
-- 2. Cover distinct implementation toggles and rewrite eligibility.
-- Source: dev/test/sql/test_materialized_view/T/test_materialized_view_rewrite_count_distinct

-- query 1
create table user_tags (time date, user_id int, user_name varchar(20), tag_id int) partition by range (time)  (partition p1 values less than MAXVALUE) distributed by hash(time) buckets 3 properties('replication_num' = '1');

-- query 2
insert into user_tags values('2023-04-13', 1, 'a', 1);

-- query 3
insert into user_tags values('2023-04-13', 1, 'b', 2);

-- query 4
insert into user_tags values('2023-04-13', 1, 'c', 3);

-- query 5
insert into user_tags values('2023-04-13', 1, 'd', 4);

-- query 6
insert into user_tags values('2023-04-13', 1, 'e', 5);

-- query 7
insert into user_tags values('2023-04-13', 2, 'e', 5);

-- query 8
insert into user_tags values('2023-04-13', 3, 'e', 6);

-- query 9
set count_distinct_implementation='ndv';

-- query 10
select user_id, count(distinct tag_id), count(distinct user_name) from user_tags group by user_id order by user_id;

-- query 11
select count(distinct tag_id), count(distinct user_name) from user_tags;

-- query 12
set count_distinct_implementation='multi_count_distinct';

-- query 13
select user_id, count(distinct tag_id), count(distinct user_name) from user_tags group by user_id order by user_id;

-- query 14
select count(distinct tag_id), count(distinct user_name) from user_tags;

-- query 15
set count_distinct_implementation='default';

-- query 16
create materialized view test_mv1
distributed by hash(user_id)
as select user_id, bitmap_union(to_bitmap(tag_id)), bitmap_union(to_bitmap(user_name)) from user_tags group by user_id;

-- query 17
refresh materialized view test_mv1 with sync mode;

-- query 18
set enable_count_distinct_rewrite_by_hll_bitmap=true;

-- query 19
-- Current NovaRocks rewrites the bitmap-based distinct aggregation through the MV.
-- @result_contains=MaterializedView: true
SET enable_materialized_view_rewrite = true;
EXPLAIN select user_id, count(distinct tag_id), count(distinct user_name) from user_tags group by user_id order by user_id;

-- query 20
-- @result_contains=MaterializedView: true
SET enable_materialized_view_rewrite = true;
EXPLAIN select count(distinct tag_id), count(distinct user_name) from user_tags;

-- query 21
select user_id, count(distinct tag_id), count(distinct user_name) from user_tags group by user_id order by user_id;

-- query 22
select count(distinct tag_id), count(distinct user_name) from user_tags;

-- query 23
set enable_count_distinct_rewrite_by_hll_bitmap=false;

-- query 24
-- @result_not_contains=MaterializedView: true
SET enable_materialized_view_rewrite = true;
EXPLAIN select user_id, count(distinct tag_id), count(distinct user_name) from user_tags group by user_id order by user_id;

-- query 25
-- @result_not_contains=MaterializedView: true
SET enable_materialized_view_rewrite = true;
EXPLAIN select count(distinct tag_id), count(distinct user_name) from user_tags;

-- query 26
select user_id, count(distinct tag_id), count(distinct user_name) from user_tags group by user_id order by user_id;

-- query 27
select count(distinct tag_id), count(distinct user_name) from user_tags;

-- query 28
drop materialized view test_mv1;

-- query 29
create materialized view test_mv1
distributed by hash(user_id)
as select user_id, time, hll_union(hll_hash(tag_id)) as agg1, hll_union(hll_hash(user_name)) as agg2  from user_tags group by user_id, time;

-- query 30
refresh materialized view test_mv1 with sync mode;

-- query 31
set enable_count_distinct_rewrite_by_hll_bitmap=true;

-- query 32
-- Current NovaRocks can rewrite the grouped count-distinct query through the HLL MV.
-- @result_contains=MaterializedView: true
SET enable_materialized_view_rewrite = true;
EXPLAIN select user_id, count(distinct tag_id), count(distinct user_name) from user_tags group by user_id order by user_id;

-- query 33
-- Current NovaRocks can also rewrite the global count-distinct query through the HLL MV.
-- @result_contains=MaterializedView: true
SET enable_materialized_view_rewrite = true;
EXPLAIN select count(distinct tag_id), count(distinct user_name) from user_tags;

-- query 34
select user_id, count(distinct tag_id), count(distinct user_name) from user_tags group by user_id order by user_id;

-- query 35
select count(distinct tag_id), count(distinct user_name) from user_tags;

-- query 36
set enable_count_distinct_rewrite_by_hll_bitmap=false;

-- query 37
-- @result_not_contains=MaterializedView: true
SET enable_materialized_view_rewrite = true;
EXPLAIN select user_id, count(distinct tag_id), count(distinct user_name) from user_tags group by user_id order by user_id;

-- query 38
-- @result_not_contains=MaterializedView: true
SET enable_materialized_view_rewrite = true;
EXPLAIN select count(distinct tag_id), count(distinct user_name) from user_tags;

-- query 39
select user_id, count(distinct tag_id), count(distinct user_name) from user_tags group by user_id order by user_id;

-- query 40
select count(distinct tag_id), count(distinct user_name) from user_tags;
