-- Test Objective:
-- 1. Validate sync MV creation and refresh on colocated tables.
-- 2. Cover colocate metadata preservation after MV build.
-- Source: dev/test/sql/test_materialized_view/T/test_sync_materialized_view_with_colocate

-- query 1
admin set frontend config('alter_scheduler_interval_millisecond' = '100');

-- query 2
create table user_tags (time date, user_id int, user_name varchar(20), tag_id int) partition by range (time)  (partition p1 values less than MAXVALUE) distributed by hash(time) buckets 3 properties('colocate_with' = 'colocate_gorup1', 'replicated_storage' = 'true');

-- query 3
insert into user_tags values('2023-04-13', 1, 'a', 1), ('2023-04-13', 1, 'b', 2), ('2023-04-13', 1, 'c', 3), ('2023-04-13', 1, 'd', 4), ('2023-04-13', 1, 'e', 5), ('2023-04-13', 2, 'e', 5), ('2023-04-13', 3, 'e', 6);

-- query 4
-- normal cases
create materialized view user_tags_mv1
PROPERTIES ("colocate_mv" = "true")
as select user_id, time, bitmap_union(to_bitmap(tag_id)) from user_tags group by user_id, time;

-- query 5
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 6
create materialized view user_tags_hll_mv1
PROPERTIES ("colocate_mv" = "true")
as select user_id, time, hll_union(hll_hash(tag_id)) from user_tags group by user_id, time;

-- query 7
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 8
create materialized view user_tags_percential_mv1
PROPERTIES ("colocate_mv" = "true")
as select user_id, time, percentile_union(percentile_hash(tag_id)) from user_tags group by user_id, time;

-- query 9
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 10
insert into user_tags values('2023-04-13', 1, 'a', 1), ('2023-04-13', 1, 'b', 2), ('2023-04-13', 1, 'c', 3), ('2023-04-13', 1, 'd', 4), ('2023-04-13', 1, 'e', 5), ('2023-04-13', 2, 'e', 5), ('2023-04-13', 3, 'e', 6);

-- query 11
insert into user_tags values('2023-04-13', 1, 'a', 1), ('2023-04-13', 1, 'b', 2), ('2023-04-13', 1, 'c', 3), ('2023-04-13', 1, 'd', 4), ('2023-04-13', 1, 'e', 5), ('2023-04-13', 2, 'e', 5), ('2023-04-13', 3, 'e', 6);

-- query 12
ADMIN SET FRONTEND CONFIG ("enable_colocate_mv_index" = "true");

-- query 13
-- bitmap
select * from user_tags_mv1 [_SYNC_MV_] order by user_id;

-- query 14
select user_id, count(distinct tag_id) from user_tags group by user_id order by user_id;

-- query 15
select user_id, bitmap_union_count(to_bitmap(tag_id)) from user_tags group by user_id order by user_id;

-- query 16
select user_id, bitmap_count(bitmap_union(to_bitmap(tag_id))) x from user_tags group by user_id order by user_id;

-- query 17
-- hll
select * from user_tags_hll_mv1 [_SYNC_MV_] order by user_id;

-- query 18
select user_id, approx_count_distinct(tag_id) x from user_tags group by user_id order by user_id;

-- query 19
select user_id, ndv(tag_id) x from user_tags group by user_id order by user_id;

-- query 20
select user_id, hll_union_agg(hll_hash(tag_id)) x from user_tags group by user_id order by user_id;

-- query 21
select user_id, hll_cardinality(hll_union(hll_hash(tag_id))) x from user_tags group by user_id order by user_id;

-- query 22
select * from user_tags_percential_mv1 [_SYNC_MV_] order by user_id;

-- query 23
-- percentile
select user_id, percentile_approx(tag_id, 1) x from user_tags group by user_id order by user_id;

-- query 24
select user_id, percentile_approx(tag_id, 0) x from user_tags group by user_id order by user_id;

-- query 25
select user_id, round(percentile_approx(tag_id, 0)) x from user_tags group by user_id order by user_id;

-- query 26
drop materialized view user_tags_mv1;

-- query 27
drop materialized view user_tags_hll_mv1;

-- query 28
drop materialized view user_tags_percential_mv1;
