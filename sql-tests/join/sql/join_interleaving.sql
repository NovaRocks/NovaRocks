-- @tags=join,interleaving
-- Test Objective:
-- Validate join correctness under different interleaving_group_size and chunk_size
-- settings. The interleaving parameter controls how probe rows are grouped during
-- hash join execution. Tests cover inner, left, right, full outer joins as well as
-- exists/not-exists and in/not-in subquery rewrites with inequality (<>) and
-- equality (=) other-conjuncts. Each join type is tested with interleaving_group_size
-- of 0 and -10, and chunk_size of 2 and 3 to exercise different batching paths.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.lineitem;

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.lineitem (
  `l_orderkey` int(11) NOT NULL,
  `l_partkey` int(11) NOT NULL,
  `l_suppkey` int(11)
);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.lineitem VALUES
  (1,1,1),(1,2,1),(1,3,2),(11,1,11),(11,2,1),(2,3,2),(2,3,NULL);

-- query 4
-- @skip_result_check=true
set pipeline_dop = 1;

-- ============================================================
-- chunk_size = 2, inequality join condition (<>)
-- ============================================================

-- query 5
-- @skip_result_check=true
set chunk_size = 2;

-- query 6
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 7
-- INNER JOIN, interleaving_group_size=0, chunk_size=2, <>
select count(*) as c
from ${case_db}.lineitem l1
join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey;

-- query 8
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 9
-- INNER JOIN, interleaving_group_size=-10, chunk_size=2, <>
select count(*) as c
from ${case_db}.lineitem l1
join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey;

-- query 10
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 11
-- LEFT JOIN, interleaving_group_size=0, chunk_size=2, <>
select count(*) as c
from ${case_db}.lineitem l1
left join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey;

-- query 12
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 13
-- LEFT JOIN, interleaving_group_size=-10, chunk_size=2, <>
select count(*) as c
from ${case_db}.lineitem l1
left join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey;

-- query 14
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 15
-- NOT EXISTS, interleaving_group_size=0, chunk_size=2, <>
select count(*) as c
from ${case_db}.lineitem l1
where not exists (
  select * from ${case_db}.lineitem l3
  where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey
);

-- query 16
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 17
-- NOT EXISTS, interleaving_group_size=-10, chunk_size=2, <>
select count(*) as c
from ${case_db}.lineitem l1
where not exists (
  select * from ${case_db}.lineitem l3
  where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey
);

-- query 18
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 19
-- NOT IN, interleaving_group_size=0, chunk_size=2, <>
select count(*) as c
from ${case_db}.lineitem l1
where l1.l_orderkey not in (
  select l3.l_orderkey from ${case_db}.lineitem l3 where l3.l_suppkey <> l1.l_suppkey
);

-- query 20
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 21
-- NOT IN, interleaving_group_size=-10, chunk_size=2, <>
select count(*) as c
from ${case_db}.lineitem l1
where l1.l_orderkey not in (
  select l3.l_orderkey from ${case_db}.lineitem l3 where l3.l_suppkey <> l1.l_suppkey
);

-- query 22
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 23
-- EXISTS, interleaving_group_size=0, chunk_size=2, <>
select count(*) as c
from ${case_db}.lineitem l1
where exists (
  select * from ${case_db}.lineitem l3
  where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey
);

-- query 24
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 25
-- EXISTS, interleaving_group_size=-10, chunk_size=2, <>
select count(*) as c
from ${case_db}.lineitem l1
where exists (
  select * from ${case_db}.lineitem l3
  where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey
);

-- query 26
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 27
-- IN, interleaving_group_size=0, chunk_size=2, <>
select count(*) as c
from ${case_db}.lineitem l1
where l1.l_orderkey in (
  select l3.l_orderkey from ${case_db}.lineitem l3 where l3.l_suppkey <> l1.l_suppkey
);

-- query 28
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 29
-- IN, interleaving_group_size=-10, chunk_size=2, <>
select count(*) as c
from ${case_db}.lineitem l1
where l1.l_orderkey in (
  select l3.l_orderkey from ${case_db}.lineitem l3 where l3.l_suppkey <> l1.l_suppkey
);

-- query 30
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 31
-- RIGHT JOIN, interleaving_group_size=0, chunk_size=2, <>
select count(*) as c
from ${case_db}.lineitem l1
right join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey;

-- query 32
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 33
-- RIGHT JOIN, interleaving_group_size=-10, chunk_size=2, <>
select count(*) as c
from ${case_db}.lineitem l1
right join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey;

-- query 34
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 35
-- FULL OUTER JOIN, interleaving_group_size=0, chunk_size=2, <>
select count(*) as c
from ${case_db}.lineitem l1
full outer join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey;

-- query 36
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 37
-- FULL OUTER JOIN, interleaving_group_size=-10, chunk_size=2, <>
select count(*) as c
from ${case_db}.lineitem l1
full outer join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey;

-- ============================================================
-- chunk_size = 3, inequality join condition (<>)
-- ============================================================

-- query 38
-- @skip_result_check=true
set chunk_size = 3;

-- query 39
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 40
-- INNER JOIN, interleaving_group_size=0, chunk_size=3, <>
select count(*) as c
from ${case_db}.lineitem l1
join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey;

-- query 41
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 42
-- INNER JOIN, interleaving_group_size=-10, chunk_size=3, <>
select count(*) as c
from ${case_db}.lineitem l1
join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey;

-- query 43
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 44
-- LEFT JOIN, interleaving_group_size=0, chunk_size=3, <>
select count(*) as c
from ${case_db}.lineitem l1
left join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey;

-- query 45
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 46
-- LEFT JOIN, interleaving_group_size=-10, chunk_size=3, <>
select count(*) as c
from ${case_db}.lineitem l1
left join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey;

-- query 47
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 48
-- NOT EXISTS, interleaving_group_size=0, chunk_size=3, <>
select count(*) as c
from ${case_db}.lineitem l1
where not exists (
  select * from ${case_db}.lineitem l3
  where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey
);

-- query 49
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 50
-- NOT EXISTS, interleaving_group_size=-10, chunk_size=3, <>
select count(*) as c
from ${case_db}.lineitem l1
where not exists (
  select * from ${case_db}.lineitem l3
  where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey
);

-- query 51
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 52
-- NOT IN, interleaving_group_size=0, chunk_size=3, <>
select count(*) as c
from ${case_db}.lineitem l1
where l1.l_orderkey not in (
  select l3.l_orderkey from ${case_db}.lineitem l3 where l3.l_suppkey <> l1.l_suppkey
);

-- query 53
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 54
-- NOT IN, interleaving_group_size=-10, chunk_size=3, <>
select count(*) as c
from ${case_db}.lineitem l1
where l1.l_orderkey not in (
  select l3.l_orderkey from ${case_db}.lineitem l3 where l3.l_suppkey <> l1.l_suppkey
);

-- query 55
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 56
-- EXISTS, interleaving_group_size=0, chunk_size=3, <>
select count(*) as c
from ${case_db}.lineitem l1
where exists (
  select * from ${case_db}.lineitem l3
  where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey
);

-- query 57
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 58
-- EXISTS, interleaving_group_size=-10, chunk_size=3, <>
select count(*) as c
from ${case_db}.lineitem l1
where exists (
  select * from ${case_db}.lineitem l3
  where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey
);

-- query 59
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 60
-- IN, interleaving_group_size=0, chunk_size=3, <>
select count(*) as c
from ${case_db}.lineitem l1
where l1.l_orderkey in (
  select l3.l_orderkey from ${case_db}.lineitem l3 where l3.l_suppkey <> l1.l_suppkey
);

-- query 61
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 62
-- IN, interleaving_group_size=-10, chunk_size=3, <>
select count(*) as c
from ${case_db}.lineitem l1
where l1.l_orderkey in (
  select l3.l_orderkey from ${case_db}.lineitem l3 where l3.l_suppkey <> l1.l_suppkey
);

-- query 63
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 64
-- RIGHT JOIN, interleaving_group_size=0, chunk_size=3, <>
select count(*) as c
from ${case_db}.lineitem l1
right join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey;

-- query 65
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 66
-- RIGHT JOIN, interleaving_group_size=-10, chunk_size=3, <>
select count(*) as c
from ${case_db}.lineitem l1
right join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey;

-- query 67
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 68
-- FULL OUTER JOIN, interleaving_group_size=0, chunk_size=3, <>
select count(*) as c
from ${case_db}.lineitem l1
full outer join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey;

-- query 69
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 70
-- FULL OUTER JOIN, interleaving_group_size=-10, chunk_size=3, <>
select count(*) as c
from ${case_db}.lineitem l1
full outer join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey;

-- ============================================================
-- chunk_size = 2, equality join condition (=)
-- ============================================================

-- query 71
-- @skip_result_check=true
set chunk_size = 2;

-- query 72
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 73
-- INNER JOIN, interleaving_group_size=0, chunk_size=2, =
select count(*) as c
from ${case_db}.lineitem l1
join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey;

-- query 74
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 75
-- INNER JOIN, interleaving_group_size=-10, chunk_size=2, =
select count(*) as c
from ${case_db}.lineitem l1
join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey;

-- query 76
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 77
-- LEFT JOIN, interleaving_group_size=0, chunk_size=2, =
select count(*) as c
from ${case_db}.lineitem l1
left join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey;

-- query 78
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 79
-- LEFT JOIN, interleaving_group_size=-10, chunk_size=2, =
select count(*) as c
from ${case_db}.lineitem l1
left join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey;

-- query 80
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 81
-- NOT EXISTS, interleaving_group_size=0, chunk_size=2, =
select count(*) as c
from ${case_db}.lineitem l1
where not exists (
  select * from ${case_db}.lineitem l3
  where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey
);

-- query 82
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 83
-- NOT EXISTS, interleaving_group_size=-10, chunk_size=2, =
select count(*) as c
from ${case_db}.lineitem l1
where not exists (
  select * from ${case_db}.lineitem l3
  where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey
);

-- query 84
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 85
-- NOT IN (with <>), interleaving_group_size=0, chunk_size=2
select count(*) as c
from ${case_db}.lineitem l1
where l1.l_orderkey not in (
  select l3.l_orderkey from ${case_db}.lineitem l3 where l3.l_suppkey <> l1.l_suppkey
);

-- query 86
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 87
-- NOT IN (with <>), interleaving_group_size=-10, chunk_size=2
select count(*) as c
from ${case_db}.lineitem l1
where l1.l_orderkey not in (
  select l3.l_orderkey from ${case_db}.lineitem l3 where l3.l_suppkey <> l1.l_suppkey
);

-- query 88
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 89
-- EXISTS, interleaving_group_size=0, chunk_size=2, =
select count(*) as c
from ${case_db}.lineitem l1
where exists (
  select * from ${case_db}.lineitem l3
  where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey
);

-- query 90
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 91
-- EXISTS, interleaving_group_size=-10, chunk_size=2, =
select count(*) as c
from ${case_db}.lineitem l1
where exists (
  select * from ${case_db}.lineitem l3
  where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey
);

-- query 92
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 93
-- IN (with <>), interleaving_group_size=0, chunk_size=2
select count(*) as c
from ${case_db}.lineitem l1
where l1.l_orderkey in (
  select l3.l_orderkey from ${case_db}.lineitem l3 where l3.l_suppkey <> l1.l_suppkey
);

-- query 94
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 95
-- IN (with <>), interleaving_group_size=-10, chunk_size=2
select count(*) as c
from ${case_db}.lineitem l1
where l1.l_orderkey in (
  select l3.l_orderkey from ${case_db}.lineitem l3 where l3.l_suppkey <> l1.l_suppkey
);

-- query 96
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 97
-- RIGHT JOIN, interleaving_group_size=0, chunk_size=2, =
select count(*) as c
from ${case_db}.lineitem l1
right join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey;

-- query 98
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 99
-- RIGHT JOIN, interleaving_group_size=-10, chunk_size=2, =
select count(*) as c
from ${case_db}.lineitem l1
right join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey;

-- query 100
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 101
-- FULL OUTER JOIN, interleaving_group_size=0, chunk_size=2, =
select count(*) as c
from ${case_db}.lineitem l1
full outer join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey;

-- query 102
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 103
-- FULL OUTER JOIN, interleaving_group_size=-10, chunk_size=2, =
select count(*) as c
from ${case_db}.lineitem l1
full outer join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey;

-- ============================================================
-- chunk_size = 3, equality join condition (=)
-- ============================================================

-- query 104
-- @skip_result_check=true
set chunk_size = 3;

-- query 105
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 106
-- INNER JOIN, interleaving_group_size=0, chunk_size=3, =
select count(*) as c
from ${case_db}.lineitem l1
join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey;

-- query 107
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 108
-- INNER JOIN, interleaving_group_size=-10, chunk_size=3, =
select count(*) as c
from ${case_db}.lineitem l1
join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey;

-- query 109
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 110
-- LEFT JOIN, interleaving_group_size=0, chunk_size=3, =
select count(*) as c
from ${case_db}.lineitem l1
left join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey;

-- query 111
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 112
-- LEFT JOIN, interleaving_group_size=-10, chunk_size=3, =
select count(*) as c
from ${case_db}.lineitem l1
left join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey;

-- query 113
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 114
-- NOT EXISTS, interleaving_group_size=0, chunk_size=3, =
select count(*) as c
from ${case_db}.lineitem l1
where not exists (
  select * from ${case_db}.lineitem l3
  where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey
);

-- query 115
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 116
-- NOT EXISTS, interleaving_group_size=-10, chunk_size=3, =
select count(*) as c
from ${case_db}.lineitem l1
where not exists (
  select * from ${case_db}.lineitem l3
  where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey
);

-- query 117
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 118
-- NOT IN (with <>), interleaving_group_size=0, chunk_size=3
select count(*) as c
from ${case_db}.lineitem l1
where l1.l_orderkey not in (
  select l3.l_orderkey from ${case_db}.lineitem l3 where l3.l_suppkey <> l1.l_suppkey
);

-- query 119
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 120
-- NOT IN (with <>), interleaving_group_size=-10, chunk_size=3
select count(*) as c
from ${case_db}.lineitem l1
where l1.l_orderkey not in (
  select l3.l_orderkey from ${case_db}.lineitem l3 where l3.l_suppkey <> l1.l_suppkey
);

-- query 121
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 122
-- EXISTS, interleaving_group_size=0, chunk_size=3, =
select count(*) as c
from ${case_db}.lineitem l1
where exists (
  select * from ${case_db}.lineitem l3
  where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey
);

-- query 123
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 124
-- EXISTS, interleaving_group_size=-10, chunk_size=3, =
select count(*) as c
from ${case_db}.lineitem l1
where exists (
  select * from ${case_db}.lineitem l3
  where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey
);

-- query 125
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 126
-- IN (with <>), interleaving_group_size=0, chunk_size=3
select count(*) as c
from ${case_db}.lineitem l1
where l1.l_orderkey in (
  select l3.l_orderkey from ${case_db}.lineitem l3 where l3.l_suppkey <> l1.l_suppkey
);

-- query 127
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 128
-- IN (with <>), interleaving_group_size=-10, chunk_size=3
select count(*) as c
from ${case_db}.lineitem l1
where l1.l_orderkey in (
  select l3.l_orderkey from ${case_db}.lineitem l3 where l3.l_suppkey <> l1.l_suppkey
);

-- query 129
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 130
-- RIGHT JOIN, interleaving_group_size=0, chunk_size=3, =
select count(*) as c
from ${case_db}.lineitem l1
right join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey;

-- query 131
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 132
-- RIGHT JOIN, interleaving_group_size=-10, chunk_size=3, =
select count(*) as c
from ${case_db}.lineitem l1
right join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey;

-- query 133
-- @skip_result_check=true
set interleaving_group_size = 0;

-- query 134
-- FULL OUTER JOIN, interleaving_group_size=0, chunk_size=3, =
select count(*) as c
from ${case_db}.lineitem l1
full outer join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey;

-- query 135
-- @skip_result_check=true
set interleaving_group_size = -10;

-- query 136
-- FULL OUTER JOIN, interleaving_group_size=-10, chunk_size=3, =
select count(*) as c
from ${case_db}.lineitem l1
full outer join ${case_db}.lineitem l3 on l3.l_orderkey = l1.l_orderkey and l3.l_suppkey = l1.l_suppkey;
