-- Test Objective:
-- 1. Validate synchronous aggregate MVs built with bitmap, HLL, and percentile states return correct answers.
-- 2. Cover sync-MV population, refresh completion, and aggregate function equivalence checks.
-- Source: dev/test/sql/test_materialized_view/T/test_sync_materialized_view1

-- query 1
admin set frontend config('alter_scheduler_interval_millisecond' = '100');

-- query 2
set enable_rewrite_simple_agg_to_meta_scan = false;

-- query 3
create table user_tags (time date, user_id int, user_name varchar(20), tag_id int) partition by range (time)  (partition p1 values less than MAXVALUE) distributed by hash(time) buckets 3 properties('replication_num' = '1');

-- query 4
insert into user_tags values('2023-04-13', 1, 'a', 1), ('2023-04-13', 1, 'b', 2), ('2023-04-13', 1, 'c', 3), ('2023-04-13', 1, 'd', 4), ('2023-04-13', 1, 'e', 5), ('2023-04-13', 2, 'e', 5), ('2023-04-13', 3, 'e', 6);

-- query 5
-- normal cases
create materialized view user_tags_mv1
as select user_id, time, bitmap_union(to_bitmap(tag_id)) from user_tags group by user_id, time;

-- query 6
-- Wait briefly after the alter job becomes visible so the next sync MV build does not race with the previous one.
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 7
create materialized view user_tags_hll_mv1
as select user_id, time, hll_union(hll_hash(tag_id)) from user_tags group by user_id, time;

-- query 8
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 9
create materialized view user_tags_percential_mv1
as select user_id, time, percentile_union(percentile_hash(tag_id)) from user_tags group by user_id, time;

-- query 10
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 11
insert into user_tags values('2023-04-13', 1, 'a', 1), ('2023-04-13', 1, 'b', 2), ('2023-04-13', 1, 'c', 3), ('2023-04-13', 1, 'd', 4), ('2023-04-13', 1, 'e', 5), ('2023-04-13', 2, 'e', 5), ('2023-04-13', 3, 'e', 6);

-- query 12
-- bitmap
select count(1) from user_tags_mv1 [_SYNC_MV_];

-- query 13
select user_id, count(distinct tag_id) from user_tags group by user_id  order by 1,2;

-- query 14
select user_id, bitmap_union_count(to_bitmap(tag_id)) from user_tags group by user_id  order by 1,2;

-- query 15
select user_id, bitmap_count(bitmap_union(to_bitmap(tag_id))) x from user_tags group by user_id  order by 1,2;

-- query 16
-- hll
select count(1) from user_tags_hll_mv1 [_SYNC_MV_];

-- query 17
select user_id, approx_count_distinct(tag_id) x from user_tags group by user_id  order by 1,2;

-- query 18
select user_id, ndv(tag_id) x from user_tags group by user_id  order by 1,2;

-- query 19
select user_id, hll_union_agg(hll_hash(tag_id)) x from user_tags group by user_id  order by 1,2;

-- query 20
select user_id, hll_cardinality(hll_union(hll_hash(tag_id))) x from user_tags group by user_id  order by 1,2;

-- query 21
select count(1) from user_tags_percential_mv1 [_SYNC_MV_];

-- query 22
-- percentile
select user_id, percentile_approx(tag_id, 1) x from user_tags group by user_id  order by 1,2;

-- query 23
select user_id, percentile_approx(tag_id, 0) x from user_tags group by user_id  order by 1,2;

-- query 24
select user_id, round(percentile_approx(tag_id, 0)) x from user_tags group by user_id  order by 1,2;

-- query 25
drop materialized view user_tags_mv1;

-- query 26
drop materialized view user_tags_hll_mv1;

-- query 27
drop materialized view user_tags_percential_mv1;
