-- Test Objective:
-- 1. Validate single-table aggregate rewrites over multiple MV rollup shapes.
-- 2. Cover grouped count/sum rewrites and empty-table behavior.
-- Source: dev/test/sql/test_materialized_view/T/test_materialized_view_rewrite

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
create materialized view agg_count_mv1
distributed by hash(user_id)
as
select user_id, count(1) as cnt
from user_tags
group by user_id;

-- query 8
refresh materialized view agg_count_mv1 with sync mode;

-- query 9
create materialized view agg_count_mv2
distributed by hash(user_id)
as
select user_id, user_name, count(1) as cnt
from user_tags
group by user_id, user_name;

-- query 10
refresh materialized view agg_count_mv2 with sync mode;

-- query 11
-- @skip_result_check=true
explain select user_id, count(1) as cnt
from user_tags
group by user_id;

-- query 12
CREATE TABLE `user_tags_2` (
  `time` date NULL COMMENT "",
  `user_id` int(11) NULL COMMENT "",
  `user_name` varchar(20) NULL COMMENT "",
  `tag_id` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`time`, `user_id`, `user_name`)
PARTITION BY RANGE(`time`)
(PARTITION p1 VALUES [("0000-01-01"), (MAXVALUE)))
DISTRIBUTED BY HASH(`time`) BUCKETS 3
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);

-- query 13
insert into user_tags_2 values('2023-04-13', 1, 'a', 1);

-- query 14
insert into user_tags_2 values('2023-04-13', 2, 'b', 2);

-- query 15
insert into user_tags_2 values('2023-04-13', 3, 'c', 3);

-- query 16
insert into user_tags_2 values('2023-04-13', 4, 'd', 4);

-- query 17
insert into user_tags_2 values('2023-04-13', 5, 'e', 5);

-- query 18
create materialized view agg_count_mv3
distributed by hash(user_id)
as
select user_id, count(1) as cnt
from user_tags_2
group by user_id;

-- query 19
refresh materialized view agg_count_mv3 with sync mode;

-- query 20
create materialized view agg_count_mv4
distributed by hash(user_id)
as
select user_id, user_name, count(1) as cnt
from user_tags_2
group by user_id, user_name;

-- query 21
refresh materialized view agg_count_mv4 with sync mode;

-- query 22
-- @skip_result_check=true
explain select user_id, count(1) as cnt
from user_tags_2
group by user_id;

-- query 23
create materialized view agg_count_mv5
distributed by hash(user_id)
as
select user_id, user_name, count(1) as cnt, sum(tag_id) as total
from user_tags_2
group by user_id, user_name;

-- query 24
refresh materialized view agg_count_mv5 with sync mode;

-- query 25
-- @skip_result_check=true
explain select user_id, user_name, count(1) as cnt
from user_tags_2
group by user_id, user_name;
