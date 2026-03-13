-- Test Objective:
-- 1. Validate MV metadata functions expose expected identifiers and properties.
-- 2. Cover metadata lookup functions on active materialized views.
-- Source: dev/test/sql/test_materialized_view/T/test_mv_meta_functions

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
create materialized view user_tags_mv1  distributed by hash(user_id) as select user_id, bitmap_union(to_bitmap(tag_id)) from user_tags group by user_id;

-- query 10
-- @skip_result_check=true
select inspect_mv_refresh_info('user_tags_mv1');

-- query 11
-- @skip_result_check=true
select inspect_table_partition_info('user_tags');

-- query 12
refresh materialized view user_tags_mv1 with sync mode;

-- query 13
-- @skip_result_check=true
select inspect_mv_plan('user_tags_mv1');

-- query 14
-- @skip_result_check=true
select inspect_mv_plan('user_tags_mv1', true);

-- query 15
-- @skip_result_check=true
select inspect_mv_plan('user_tags_mv1', false);

-- query 16
insert into user_tags values('2023-04-13', 3, 'e', 6);

-- query 17
-- @skip_result_check=true
select inspect_mv_refresh_info('user_tags_mv1');

-- query 18
-- @skip_result_check=true
select inspect_table_partition_info('user_tags');
