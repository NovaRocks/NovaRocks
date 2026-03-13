-- Test Objective:
-- 1. Validate optimizer rewrite chooses the intended materialized view plan.
-- 2. Cover query correctness together with rewrite plan inspection.
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
insert into user_tags values('2023-04-13', 2, 'e', 5);

-- query 8
insert into user_tags values('2023-04-13', 3, 'e', 6);

-- query 9
-- TEST BITMAP: NO ROLLUP
create materialized view user_tags_mv1  distributed by hash(user_id) as select user_id, bitmap_union(to_bitmap(tag_id)) from user_tags group by user_id;

-- query 10
refresh materialized view user_tags_mv1 with sync mode;

-- query 11
set enable_materialized_view_rewrite = off;

-- query 12
select user_id, count(distinct tag_id) from user_tags group by user_id order by user_id;

-- query 13
select user_id, bitmap_union_count(to_bitmap(tag_id)) from user_tags group by user_id order by user_id;

-- query 14
select user_id, bitmap_count(bitmap_union(to_bitmap(tag_id))) x from user_tags group by user_id order by user_id;

-- query 15
set enable_materialized_view_rewrite = on;

-- query 16
-- explain logical select user_id, count(distinct tag_id) from user_tags group by user_id order by user_id;
select user_id, count(distinct tag_id) from user_tags group by user_id order by user_id;

-- query 17
select user_id, bitmap_union_count(to_bitmap(tag_id)) from user_tags group by user_id order by user_id;

-- query 18
select user_id, bitmap_count(bitmap_union(to_bitmap(tag_id))) x from user_tags group by user_id order by user_id;

-- query 19
drop materialized view user_tags_mv1;

-- query 20
-- TEST BITMAP: ROLLUP
create materialized view user_tags_mv2  distributed by hash(user_id) as select user_id, time, bitmap_union(to_bitmap(tag_id)) from user_tags group by user_id, time;

-- query 21
refresh materialized view user_tags_mv2 with sync mode;

-- query 22
set enable_materialized_view_rewrite = on;

-- query 23
explain logical select user_id, count(distinct tag_id) from user_tags group by user_id order by user_id;

-- query 24
select user_id, count(distinct tag_id) from user_tags group by user_id order by user_id;

-- query 25
select user_id, bitmap_union_count(to_bitmap(tag_id)) from user_tags group by user_id order by user_id;

-- query 26
select user_id, bitmap_count(bitmap_union(to_bitmap(tag_id))) x from user_tags group by user_id order by user_id;

-- query 27
drop materialized view user_tags_mv2;

-- query 28
-- TEST HLL: NO ROLLUP
create materialized view user_tags_hll_mv1  distributed by hash(user_id) as select user_id, time, hll_union(hll_hash(tag_id)) a  from user_tags group by user_id, time;

-- query 29
refresh materialized view user_tags_hll_mv1 with sync mode;

-- query 30
set enable_materialized_view_rewrite = off;

-- query 31
select user_id, approx_count_distinct(tag_id) x from user_tags group by user_id order by user_id;

-- query 32
select user_id, ndv(tag_id) x from user_tags group by user_id order by user_id;

-- query 33
select user_id, hll_union_agg(hll_hash(tag_id)) x from user_tags group by user_id order by user_id;

-- query 34
select user_id, hll_cardinality(hll_union(hll_hash(tag_id))) x from user_tags group by user_id order by user_id;

-- query 35
set enable_materialized_view_rewrite = on;

-- query 36
select user_id, approx_count_distinct(tag_id) x from user_tags group by user_id order by user_id;

-- query 37
select user_id, ndv(tag_id) x from user_tags group by user_id order by user_id;

-- query 38
select user_id, hll_union_agg(hll_hash(tag_id)) x from user_tags group by user_id order by user_id;

-- query 39
select user_id, hll_cardinality(hll_union(hll_hash(tag_id))) x from user_tags group by user_id order by user_id;

-- query 40
drop materialized view user_tags_hll_mv1;

-- query 41
-- TEST HLL: ROLLUP
create materialized view user_tags_hll_mv2  distributed by hash(user_id) as select user_id, time, hll_union(hll_hash(tag_id)) from user_tags group by user_id, time;

-- query 42
refresh materialized view user_tags_hll_mv2 with sync mode;

-- query 43
set enable_materialized_view_rewrite = on;

-- query 44
select user_id, approx_count_distinct(tag_id) x from user_tags group by user_id order by user_id;

-- query 45
select user_id, ndv(tag_id) x from user_tags group by user_id order by user_id;

-- query 46
select user_id, hll_union_agg(hll_hash(tag_id)) x from user_tags group by user_id order by user_id;

-- query 47
select user_id, hll_cardinality(hll_union(hll_hash(tag_id))) x from user_tags group by user_id order by user_id;

-- query 48
drop materialized view user_tags_hll_mv2;

-- query 49
-- TEST PERCENTILE: NO ROLLUP
create materialized view user_tags_percential_mv1 distributed by hash(user_id) as select user_id, percentile_union(percentile_hash(tag_id)) from user_tags group by user_id order by user_id;

-- query 50
refresh materialized view user_tags_percential_mv1 with sync mode;

-- query 51
set enable_materialized_view_rewrite = off;

-- query 52
select user_id, percentile_approx(tag_id, 1) x from user_tags group by user_id order by user_id;

-- query 53
select user_id, percentile_approx(tag_id, 0) x from user_tags group by user_id order by user_id;

-- query 54
select user_id, round(percentile_approx(tag_id, 0)) x from user_tags group by user_id order by user_id;

-- query 55
set enable_materialized_view_rewrite = on;

-- query 56
select user_id, percentile_approx_raw(percentile_union(percentile_hash(tag_id)), 1) x from user_tags group by user_id order by user_id;

-- query 57
select user_id, percentile_approx_raw(percentile_union(percentile_hash(tag_id)), 1) x from user_tags group by user_id order by user_id;

-- query 58
select user_id, percentile_approx(tag_id, 0) x from user_tags group by user_id order by user_id;

-- query 59
select user_id, round(percentile_approx(tag_id, 0)) x from user_tags group by user_id order by user_id;

-- query 60
drop materialized view user_tags_percential_mv1;

-- query 61
-- TEST PERCENTILE: ROLLUP
create materialized view user_tags_percential_mv2 distributed by hash(user_id) as select user_id, time, percentile_union(percentile_hash(tag_id)) from user_tags group by user_id, time;

-- query 62
refresh materialized view user_tags_percential_mv2 with sync mode;

-- query 63
set enable_materialized_view_rewrite = on;

-- query 64
select user_id, percentile_approx(tag_id, 1) x from user_tags group by user_id order by user_id;

-- query 65
select user_id, percentile_approx(tag_id, 1) x from user_tags group by user_id order by user_id;

-- query 66
select user_id, percentile_approx(tag_id, 0) x from user_tags group by user_id order by user_id;

-- query 67
select user_id, round(percentile_approx(tag_id, 0)) x from user_tags group by user_id order by user_id;

-- query 68
select user_id, time, round(percentile_approx(tag_id, 0)) from user_tags group by user_id, time order by user_id;

-- query 69
drop materialized view user_tags_percential_mv2;

-- query 70
-- TEST BITMAP : UNOION
create materialized view user_tags_mv3  distributed by hash(user_id) as select user_id, tag_id from user_tags where user_id > 2;

-- query 71
refresh materialized view user_tags_mv3 with sync mode;

-- query 72
set enable_materialized_view_rewrite = off;

-- query 73
select user_id, approx_count_distinct(tag_id) x from user_tags group by user_id order by user_id;

-- query 74
select user_id, ndv(tag_id) x from user_tags group by user_id order by user_id;

-- query 75
select user_id, hll_union_agg(hll_hash(tag_id)) x from user_tags group by user_id order by user_id;

-- query 76
select user_id, hll_cardinality(hll_union(hll_hash(tag_id))) x from user_tags group by user_id order by user_id;

-- query 77
set enable_materialized_view_rewrite = on;

-- query 78
select user_id, approx_count_distinct(tag_id) x from user_tags group by user_id order by user_id;

-- query 79
select user_id, ndv(tag_id) x from user_tags group by user_id order by user_id;

-- query 80
select user_id, hll_union_agg(hll_hash(tag_id)) x from user_tags group by user_id order by user_id;

-- query 81
select user_id, hll_cardinality(hll_union(hll_hash(tag_id))) x from user_tags group by user_id order by user_id;

-- query 82
drop materialized view user_tags_mv3;

-- query 83
create materialized view agg_mv1
distributed by hash(user_id)
as
select user_id, time, sum(tag_id) as total
from user_tags
group by user_id, time;

-- query 84
refresh materialized view agg_mv1 with sync mode;

-- query 85
-- @result_contains=agg_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select user_id, time, sum(tag_id) as total from user_tags group by user_id, time having sum(tag_id) > 2 order by user_id, time;

-- query 86
select user_id, time, sum(tag_id) as total from user_tags group by user_id, time having sum(tag_id) > 2 order by user_id, time;

-- query 87
-- @result_contains=agg_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select user_id, sum(tag_id) as total from user_tags  group by user_id having sum(tag_id) > 2 order by user_id;

-- query 88
select user_id, sum(tag_id) as total from user_tags  group by user_id having sum(tag_id) > 2 order by user_id;

-- query 89
set enable_materialized_view_rewrite = off;

-- query 90
-- @result_contains=agg_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select user_id, time, sum(tag_id) as total from user_tags group by user_id, time having sum(tag_id) > 2 order by user_id, time;

-- query 91
select user_id, time, sum(tag_id) as total from user_tags group by user_id, time having sum(tag_id) > 2 order by user_id, time;

-- query 92
-- @result_contains=agg_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select user_id, sum(tag_id) as total from user_tags  group by user_id having sum(tag_id) > 2 order by user_id;

-- query 93
select user_id, sum(tag_id) as total from user_tags  group by user_id having sum(tag_id) > 2 order by user_id;
