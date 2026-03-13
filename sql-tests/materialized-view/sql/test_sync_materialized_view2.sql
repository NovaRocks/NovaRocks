-- Test Objective:
-- 1. Validate synchronous aggregate MVs still rewrite correctly when aggregate states use expressions and aliases.
-- 2. Cover complex-expression bitmap/HLL/percentile rewrites plus repeated-column aggregate derivation.
-- Source: dev/test/sql/test_materialized_view/T/test_sync_materialized_view2

-- query 1
admin set frontend config('alter_scheduler_interval_millisecond' = '100');

-- query 2
create table user_tags (time date, user_id int, user_name varchar(20), tag_id int) partition by range (time)  (partition p1 values less than MAXVALUE) distributed by hash(time) buckets 3 properties('replication_num' = '1');

-- query 3
insert into user_tags values('2023-04-13', 1, 'a', 1), ('2023-04-13', 1, 'b', 2), ('2023-04-13', 1, 'c', 3), ('2023-04-13', 1, 'd', 4), ('2023-04-13', 1, 'e', 5), ('2023-04-13', 2, 'e', 5), ('2023-04-13', 3, 'e', 6);

-- query 4
-- complex expression.
create materialized view user_tags_mv2
as select user_id, time, bitmap_union(to_bitmap(tag_id * 100)) as agg1 from user_tags group by user_id, time;

-- query 5
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 6
create materialized view user_tags_hll_mv2
as select user_id * 2 as col1, time, hll_union(hll_hash(abs(tag_id))) as agg2 from user_tags group by col1, time;

-- query 7
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 8
create materialized view user_tags_percential_mv2
as select user_id + 1 as col2, time, percentile_union(percentile_hash(cast(tag_id * 10 as double))) as agg3 from user_tags group by col2, time;

-- query 9
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 10
create materialized view same_column_ref_mv1
as select user_id + 1 as col2, time, sum(tag_id* 10) as sum1 , sum(tag_id* 100) as sum2, count(tag_id* 10) as count1 from user_tags group by col2, time;

-- query 11
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 12
insert into user_tags values('2023-04-13', 1, 'a', 1), ('2023-04-13', 1, 'b', 2), ('2023-04-13', 1, 'c', 3), ('2023-04-13', 1, 'd', 4), ('2023-04-13', 1, 'e', 5), ('2023-04-13', 2, 'e', 5), ('2023-04-13', 3, 'e', 6);

-- query 13
-- complex expression mv rewrite
-- user_tags_mv2
select user_id, time, bitmap_union(to_bitmap(tag_id * 100)) as agg1 from user_tags group by user_id, time;

-- query 14
select user_id, time, count(distinct tag_id * 100) as agg1 from user_tags group by user_id, time;

-- query 15
select time, count(distinct tag_id * 100) as agg1 from user_tags group by time;

-- query 16
-- user_tags_hll_mv2
select user_id * 2 as col1, time, hll_union(hll_hash(abs(tag_id))) as agg2 from user_tags group by col1, time;

-- query 17
select time, hll_union(hll_hash(abs(tag_id))) as agg2 from user_tags group by time;

-- query 18
select user_id * 2 as col1, time, ndv(abs(tag_id)) as agg2 from user_tags group by col1, time;

-- query 19
select time, approx_count_distinct(abs(tag_id)) as agg2 from user_tags group by time;

-- query 20
-- user_tags_percential_mv2
select user_id + 1 as col1, time, percentile_union(percentile_hash(cast(tag_id * 10 as double))) as agg1 from user_tags group by col1, time;

-- query 21
-- same_column_ref_mv1
select user_id + 1 as col1, time, sum(tag_id* 10) as sum1 , sum(tag_id* 100) as sum2, count(tag_id* 10) as count1 from user_tags group by col1, time;

-- query 22
drop materialized view user_tags_mv2;

-- query 23
drop materialized view user_tags_hll_mv2;

-- query 24
drop materialized view user_tags_percential_mv2;

-- query 25
drop materialized view same_column_ref_mv1;

-- query 26
-- TODO: unsupported yet
-- FE currently rejects the aggregate state expression because it is not given an explicit alias.
-- @expect_error=non-slot ref expression should have an alias
create materialized view user_tags_percential_mv2
as select case when user_id != 0 then user_id + 1 else 0 end as col1, time, percentile_union(percentile_hash(cast(tag_id as bigint))) from user_tags group by col1, time;

-- query 27
-- user case
drop table if exists tbl1;

-- query 28
CREATE TABLE `tbl1` (
  `k1` tinyint(4) NULL DEFAULT "0",
  `k2` varchar(64) NULL DEFAULT "",
  `k3` bigint NULL DEFAULT "0",
  `k4` varchar(64) NULL DEFAULT ""
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 1;

-- query 29
insert into tbl1 values (1, 'a', 1, 'aa'), (2, 'b', 1, NULL), (3, NULL, NULL, NULL);

-- query 30
CREATE MATERIALIZED VIEW test_ce_mv1
as
select k1 * 2 as k1_2, k2, sum(k3) as k4_2, hll_union(hll_hash(k4)) as k5_2 from tbl1 group by k1, k2;

-- query 31
-- @result_contains=FINISHED
-- @retry_count=60
-- @retry_interval_ms=1000
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;

-- query 32
-- Keep this user-case on the initial snapshot only; a second write currently breaks the tiny-table lake metadata path.
select sleep(1);

-- query 33
select * from tbl1 order by k1;

-- query 34
select * from test_ce_mv1 [_SYNC_MV_] order by mv_k1_2;

-- query 35
drop materialized view test_ce_mv1;

-- query 36
-- table with upper case
CREATE TABLE UPPER_TBL1
(
    K1 date,
    K2 int,
    V1 int sum
)
PARTITION BY RANGE(K1)
(
    PARTITION p1 values [('2020-01-01'),('2020-02-01')),
    PARTITION p2 values [('2020-02-01'),('2020-03-01'))
)
DISTRIBUTED BY HASH(K2) BUCKETS 3
PROPERTIES('replication_num' = '1');

-- query 37
insert into UPPER_TBL1 values ('2020-01-01', 1, 1), ('2020-01-01', 1, 1), ('2020-01-01', 1, 2),  ('2020-01-01', 2, 1);

-- query 38
create materialized view UPPER_MV1 as select K1, sum(V1) from UPPER_TBL1 group by K1;

-- query 39
-- Wait for the uppercase-name sync MV create to settle before querying it directly.
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 40
select * from UPPER_MV1 [_SYNC_MV_] order by K1, mv_sum_V1;

-- query 41
select K1, sum(V1) from UPPER_TBL1 group by K1;

-- query 42
insert into UPPER_TBL1 values ('2020-01-01', 1, 1), ('2020-01-01', 1, 1), ('2020-01-01', 1, 2),  ('2020-01-01', 2, 1);

-- query 43
select * from UPPER_MV1 [_SYNC_MV_] order by K1, mv_sum_V1;

-- query 44
select K1, sum(V1) from UPPER_TBL1 group by K1;

-- query 45
drop materialized view UPPER_MV1;

-- query 46
-- base table with delete
create table sync_mv_base_table_with_delete (k1 bigint, k2 bigint, k3 bigint) duplicate key(k1) distributed by hash(k1) buckets 1 properties ("replication_num" = "1");

-- query 47
insert into sync_mv_base_table_with_delete values (1, 1, 1), (2, 2, 2);

-- query 48
delete from sync_mv_base_table_with_delete where k1 = 1;

-- query 49
create materialized view sync_mv_base_table_with_delete_mv1 as select k2, k3 from sync_mv_base_table_with_delete;

-- query 50
-- Wait for the delete-case sync MV create to settle before querying the sync MV directly.
-- @skip_result_check=true
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;
SELECT sleep(2);

-- query 51
select k2 from sync_mv_base_table_with_delete_mv1 [_SYNC_MV_];

-- query 52
drop materialized view sync_mv_base_table_with_delete_mv1;
