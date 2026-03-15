-- Migrated from dev/test/sql/test_agg/R/test_agg_compressed_key
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_agg_compressed_key
-- query 2
-- @skip_result_check=true
USE ${case_db};
create table all_t0 (
    c1 tinyint,
    c2 smallint,
    c3 int,
    c4 bigint,
    c5 largeint,
    c6 date,
    c7 datetime,
    c8 string,
    c9 string,
    c10 char(100),
    c11 float,
    c12 double,
    c13 tinyint NOT NULL,
    c14 smallint NOT NULL,
    c15 int NOT NULL,
    c16 bigint NOT NULL,
    c17 largeint NOT NULL,
    c18 date NOT NULL,
    c19 datetime NOT NULL,
    c20 string NOT NULL,
    c21 string NOT NULL,
    c22 char(100) NOT NULL,
    c23 float NOT NULL,
    c24 double NOT NULL
) DUPLICATE KEY(c1) DISTRIBUTED BY RANDOM PROPERTIES('replication_num' = '1');

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into all_t0 SELECT x%200, x%200, x%200, x%200, x%200, x, x, x%200, x, x, x, x, x % 8, x % 8, x % 16, x %200, x%200, '2020-02-02', '2020-02-02', x%200, x, x, x, x FROM TABLE(generate_series(1,  30000)) as g(x);

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into all_t0 values (null, null, null, null, null, null, null, null, null, null, null, null, -1,-2,-3,-4,-5, '2000-01-28', '2000-01-28', 'literal', 'literal', 'literal', -1, -1);

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into all_t0 values (-1, -2, -3, null, null, null, null, null, null, null, null, null, -1,-2,-3,-4,-5, '2000-01-28', '2000-01-28', 'literal', 'literal', 'literal', -1, -1);

-- query 6
-- @skip_result_check=true
USE ${case_db};
set pipeline_dop=2;

-- query 7
USE ${case_db};
select distinct c1,c2,c3,c4,c5,c6,c7,c8 from all_t0 order by 1,2,3,4,5,6,7,8 limit 100,3;

-- query 8
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=Decode
-- @skip_result_check=true
USE ${case_db};
EXPLAIN COSTS SELECT DISTINCT c8 FROM all_t0;

-- query 9
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c1 FROM all_t0;

-- query 10
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c2 FROM all_t0;

-- query 11
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c3 FROM all_t0;

-- query 12
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c4 FROM all_t0;

-- query 13
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c5 FROM all_t0;

-- query 14
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c6 FROM all_t0;

-- query 15
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c8 FROM all_t0;

-- query 16
USE ${case_db};
select distinct c9,c10,c11,c12,c13,c14,c15,c16 from all_t0 order by 1,2,3,4,5,6,7,8 limit 100,3;

-- query 17
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c11 FROM all_t0;

-- query 18
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c12 FROM all_t0;

-- query 19
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c13 FROM all_t0;

-- query 20
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c14 FROM all_t0;

-- query 21
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c15 FROM all_t0;

-- query 22
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c16 FROM all_t0;

-- query 23
USE ${case_db};
select distinct c17,c18,c19,c20,c21,c22,c23,c24 from all_t0 order by 1,2,3,4,5,6,7,8 limit 100,3;

-- query 24
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=Decode
-- @skip_result_check=true
USE ${case_db};
EXPLAIN COSTS SELECT DISTINCT c20 FROM all_t0;

-- query 25
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c17 FROM all_t0;

-- query 26
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c18 FROM all_t0;

-- query 27
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c20 FROM all_t0;

-- query 28
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c14 FROM all_t0;

-- query 29
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c23 FROM all_t0;

-- query 30
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c24 FROM all_t0;

-- query 31
USE ${case_db};
select c1, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3;

-- query 32
USE ${case_db};
select c1, c2, sum(c1) from all_t0 group by 1,2 order by 1,2,3 limit 3;

-- query 33
USE ${case_db};
select c2, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 34
USE ${case_db};
select c3, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 35
USE ${case_db};
select c4, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 36
USE ${case_db};
select c5, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 37
USE ${case_db};
select c6, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 38
USE ${case_db};
select c7, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 39
USE ${case_db};
select c8, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 40
USE ${case_db};
select c9, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 41
USE ${case_db};
select c13, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 42
USE ${case_db};
select c14, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 43
USE ${case_db};
select c14, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 44
USE ${case_db};
select c16, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 45
USE ${case_db};
select c17, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 46
USE ${case_db};
select c18, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 47
USE ${case_db};
select c19, sum(c1) from all_t0 group by 1 order by 1, 2 limit 3, 1;

-- query 48
USE ${case_db};
select c2, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 49
USE ${case_db};
select c3, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 50
USE ${case_db};
select c4, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 51
USE ${case_db};
select c5, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 52
USE ${case_db};
select c6, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 53
USE ${case_db};
select c7, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 54
USE ${case_db};
select c8, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 55
USE ${case_db};
select c9, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 56
USE ${case_db};
select c13, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 57
USE ${case_db};
select c14, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 58
USE ${case_db};
select c14, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 59
USE ${case_db};
select c16, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 60
USE ${case_db};
select c17, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 61
USE ${case_db};
select c18, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 62
USE ${case_db};
select c19, sum(c1) from all_t0 group by 1 order by 1 desc, 2 desc limit 1;

-- query 63
USE ${case_db};
select c3, c4, sum(c1) from all_t0 group by 1,2 order by 1, 2, 3 limit 30,1;

-- query 64
USE ${case_db};
select c3, c5, sum(c1) from all_t0 group by 1,2 order by 1, 2, 3 limit 30,1;

-- query 65
USE ${case_db};
select c3, c7, sum(c1) from all_t0 group by 1,2 order by 1, 2, 3 limit 30,1;

-- query 66
USE ${case_db};
select c1,c2,c3,c4,c5,c6,c8,sum(c1) from all_t0 group by 1,2,3,4,5,6,7 order by 1,2,3,4,5,6,7,8 limit 30, 1;

-- query 67
USE ${case_db};
select c1,c2,c3,c4,c5,c6,c8,c13,c14,c15,c16, sum(c1) from all_t0 group by 1,2,3,4,5,6,7,8,9,10,11 order by 1,2,3,4,5,6,7,8,9,10,11 limit 30, 1;

-- query 68
USE ${case_db};
select c1,c2,c3,c4,c5,c6,c8,c11,c12,c13,c14,c15,c16, sum(c1) from all_t0 group by 1,2,3,4,5,6,7,8,9,10,11,12,13 order by 1,2,3,4,5,6,7,8,9,10,11,12,13 limit 30,1;

-- query 69
USE ${case_db};
select c1,c2,c3,c4,c5,c6,c8, sum(c1) from all_t0 where c10 > 0 group by 1,2,3,4,5,6,7 order by 1,2,3,4,5,6,7,8 limit 1;

-- query 70
-- @skip_result_check=true
USE ${case_db};
create table all_decimal (
    c1 decimal(4,2),
    c2 decimal(10,2),
    c3 decimal(27,9),
    c4 decimal(38,5)
) DUPLICATE KEY(c1) DISTRIBUTED BY RANDOM PROPERTIES('replication_num' = '1');

-- query 71
-- @skip_result_check=true
USE ${case_db};
insert into all_decimal SELECT x%100, x%200, x%200, x%200 FROM TABLE(generate_series(1,  30000)) as g(x);

-- query 72
USE ${case_db};
select distinct c1,c2,c3,c4 from all_decimal order by 1,2,3,4 limit 100,3;

-- query 73
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c1 FROM all_decimal;

-- query 74
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c2 FROM all_decimal;

-- query 75
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c3 FROM all_decimal;

-- query 76
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c4 FROM all_decimal;

-- query 77
USE ${case_db};
select c1, sum(c1) from all_decimal group by 1 order by 1, 2 limit 1;

-- query 78
USE ${case_db};
select c2, sum(c1) from all_decimal group by 1 order by 1, 2 limit 1;

-- query 79
USE ${case_db};
select c3, sum(c1) from all_decimal group by 1 order by 1, 2 limit 1;

-- query 80
USE ${case_db};
select c4, sum(c1) from all_decimal group by 1 order by 1, 2 limit 1;

-- query 81
USE ${case_db};
select c1, c2, sum(c1) from all_decimal group by 1,2 order by 1,2,3 limit 1;

-- query 82
USE ${case_db};
select c1, c3, sum(c1) from all_decimal group by 1,2 order by 1,2,3 limit 1;

-- query 83
USE ${case_db};
select c1, c4, sum(c1) from all_decimal group by 1,2 order by 1,2,3 limit 1;

-- query 84
USE ${case_db};
select c2, c3, sum(c1) from all_decimal group by 1,2 order by 1,2,3 limit 1;

-- query 85
USE ${case_db};
select c2, c4, sum(c1) from all_decimal group by 1,2 order by 1,2,3 limit 1;

-- query 86
USE ${case_db};
select c3, c4, sum(c1) from all_decimal group by 1,2 order by 1,2,3 limit 1;

-- query 87
USE ${case_db};
select c1, c2, c3, sum(c1) from all_decimal group by 1,2,3 order by 1,2,3,4 limit 1;

-- query 88
USE ${case_db};
select c1, c2, c4, sum(c1) from all_decimal group by 1,2,3 order by 1,2,3,4 limit 1;

-- query 89
USE ${case_db};
select c2, c3, c4, sum(c1) from all_decimal group by 1,2,3 order by 1,2,3,4 limit 1;

-- query 90
USE ${case_db};
select c1, c2, c3, c4, sum(c1) from all_decimal group by 1,2,3,4 order by 1,2,3,4,5 limit 1;

-- query 91
-- @skip_result_check=true
USE ${case_db};
create table all_numbers_t0 (
    c1 tinyint,
    c2 smallint,
    c3 int,
    c4 bigint,
    c5 largeint,
    c13 tinyint NOT NULL,
    c14 smallint NOT NULL,
    c15 int NOT NULL,
    c16 bigint NOT NULL,
    c17 largeint NOT NULL
) DUPLICATE KEY(c1) DISTRIBUTED BY RANDOM PROPERTIES('replication_num' = '1');

-- query 92
-- @skip_result_check=true
USE ${case_db};
insert into all_numbers_t0 (c1, c2, c3, c4, c5, c13, c14, c15, c16, c17) values (-128, -32768, -2147483648, -9223372036854775808, -170141183460469231731687303715884105728, -128, -32768, -2147483648, -9223372036854775808, -170141183460469231731687303715884105728);

-- query 93
-- @skip_result_check=true
USE ${case_db};
insert into all_numbers_t0 (c1, c2, c3, c4, c5, c13, c14, c15, c16, c17) values (0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

-- query 94
-- @skip_result_check=true
USE ${case_db};
insert into all_numbers_t0 (c1, c2, c3, c4, c5, c13, c14, c15, c16, c17) values (null, null, null, null, null, 0, 0, 0, 0, 0);

-- query 95
-- @skip_result_check=true
USE ${case_db};
insert into all_numbers_t0 SELECT x%128, x%200, x%200, x%200, x%200, x%128, x%200, x%200, x%200, x%200 FROM TABLE(generate_series(1,  30000)) as g(x);

-- query 96
USE ${case_db};
select distinct c17,c16,c15,c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5,6,7,8,9,10 limit 30,1;

-- query 97
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c1 FROM all_numbers_t0;

-- query 98
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c2 FROM all_numbers_t0;

-- query 99
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c3 FROM all_numbers_t0;

-- query 100
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c4 FROM all_numbers_t0;

-- query 101
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c5 FROM all_numbers_t0;

-- query 102
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c13 FROM all_numbers_t0;

-- query 103
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c14 FROM all_numbers_t0;

-- query 104
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c15 FROM all_numbers_t0;

-- query 105
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c16 FROM all_numbers_t0;

-- query 106
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c17 FROM all_numbers_t0;

-- query 107
USE ${case_db};
select distinct c1 from all_numbers_t0 order by 1 limit 30,1;

-- query 108
USE ${case_db};
select distinct c2 from all_numbers_t0 order by 1 limit 30,1;

-- query 109
USE ${case_db};
select distinct c3 from all_numbers_t0 order by 1 limit 30,1;

-- query 110
USE ${case_db};
select distinct c4 from all_numbers_t0 order by 1 limit 30,1;

-- query 111
USE ${case_db};
select distinct c5 from all_numbers_t0 order by 1 limit 30,1;

-- query 112
USE ${case_db};
select distinct c13 from all_numbers_t0 order by 1 limit 30,1;

-- query 113
USE ${case_db};
select distinct c14 from all_numbers_t0 order by 1 limit 30,1;

-- query 114
USE ${case_db};
select distinct c15 from all_numbers_t0 order by 1 limit 30,1;

-- query 115
USE ${case_db};
select distinct c16 from all_numbers_t0 order by 1 limit 30,1;

-- query 116
USE ${case_db};
select distinct c17 from all_numbers_t0 order by 1 limit 30,1;

-- query 117
USE ${case_db};
select distinct c1 from all_numbers_t0 order by 1 limit 30,1;

-- query 118
USE ${case_db};
select distinct c2,c1 from all_numbers_t0 order by 1,2 limit 30,1;

-- query 119
USE ${case_db};
select distinct c3,c2,c1 from all_numbers_t0 order by 1,2,3 limit 30,1;

-- query 120
USE ${case_db};
select distinct c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4 limit 30,1;

-- query 121
USE ${case_db};
select distinct c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5 limit 30,1;

-- query 122
USE ${case_db};
select distinct c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5,6 limit 30,1;

-- query 123
USE ${case_db};
select distinct c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5,6,7 limit 30,1;

-- query 124
USE ${case_db};
select distinct c15,c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5,6,7,8 limit 30,1;

-- query 125
USE ${case_db};
select distinct c16,c15,c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5,6,7,8,9 limit 30,1;

-- query 126
USE ${case_db};
select distinct c17,c16,c15,c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5,6,7,8,9,10 limit 30,1;

-- query 127
-- @skip_result_check=true
USE ${case_db};
insert into all_numbers_t0 (c1, c2, c3, c4, c5, c13, c14, c15, c16, c17) values (127, 32767, 2147483647, 9223372036854775807, 170141183460469231731687303715884105727, 127, 32767, 2147483647, 9223372036854775807, 170141183460469231731687303715884105727);

-- query 128
USE ${case_db};
select distinct c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5 limit 30,1;

-- query 129
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c1 FROM all_numbers_t0;

-- query 130
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c2 FROM all_numbers_t0;

-- query 131
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c3 FROM all_numbers_t0;

-- query 132
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c4 FROM all_numbers_t0;

-- query 133
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c5 FROM all_numbers_t0;

-- query 134
USE ${case_db};
select distinct c17,c16,c15,c14,c13 from all_numbers_t0 order by 1,2,3,4,5 limit 30,1;

-- query 135
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c13 FROM all_numbers_t0;

-- query 136
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c14 FROM all_numbers_t0;

-- query 137
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c15 FROM all_numbers_t0;

-- query 138
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c16 FROM all_numbers_t0;

-- query 139
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c17 FROM all_numbers_t0;

-- query 140
USE ${case_db};
select distinct c1 from all_numbers_t0 order by 1 limit 30,1;

-- query 141
USE ${case_db};
select distinct c2 from all_numbers_t0 order by 1 limit 30,1;

-- query 142
USE ${case_db};
select distinct c3 from all_numbers_t0 order by 1 limit 30,1;

-- query 143
USE ${case_db};
select distinct c4 from all_numbers_t0 order by 1 limit 30,1;

-- query 144
USE ${case_db};
select distinct c5 from all_numbers_t0 order by 1 limit 30,1;

-- query 145
USE ${case_db};
select distinct c13 from all_numbers_t0 order by 1 limit 30,1;

-- query 146
USE ${case_db};
select distinct c14 from all_numbers_t0 order by 1 limit 30,1;

-- query 147
USE ${case_db};
select distinct c15 from all_numbers_t0 order by 1 limit 30,1;

-- query 148
USE ${case_db};
select distinct c16 from all_numbers_t0 order by 1 limit 30,1;

-- query 149
USE ${case_db};
select distinct c17 from all_numbers_t0 order by 1 limit 30,1;

-- query 150
USE ${case_db};
select distinct c1 from all_numbers_t0 order by 1 limit 30,1;

-- query 151
USE ${case_db};
select distinct c2,c1 from all_numbers_t0 order by 1,2 limit 30,1;

-- query 152
USE ${case_db};
select distinct c3,c2,c1 from all_numbers_t0 order by 1,2,3 limit 30,1;

-- query 153
USE ${case_db};
select distinct c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4 limit 30,1;

-- query 154
USE ${case_db};
select distinct c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5 limit 30,1;

-- query 155
USE ${case_db};
select distinct c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5,6 limit 30,1;

-- query 156
USE ${case_db};
select distinct c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5,6,7 limit 30,1;

-- query 157
USE ${case_db};
select distinct c15,c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5,6,7,8 limit 30,1;

-- query 158
USE ${case_db};
select distinct c16,c15,c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5,6,7,8,9 limit 30,1;

-- query 159
USE ${case_db};
select distinct c17,c16,c15,c14,c13,c5,c4,c3,c2,c1 from all_numbers_t0 order by 1,2,3,4,5,6,7,8,9,10 limit 30,1;

-- query 160
USE ${case_db};
select distinct c2,c1 from all_numbers_t0 where c2 = 7 order by 1,2 limit 1;

-- query 161
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE agged_table (
    k1 int,
    k2 int sum
)
AGGREGATE KEY(k1)
DISTRIBUTED BY HASH(k1)
properties (
    "replication_num" = "1"
);

-- query 162
-- @skip_result_check=true
USE ${case_db};
insert into agged_table values(1,1);

-- query 163
-- @skip_result_check=true
USE ${case_db};
insert into agged_table values(1,2);

-- query 164
-- @skip_result_check=true
USE ${case_db};
insert into agged_table values(1,3);

-- query 165
-- @skip_result_check=true
USE ${case_db};
insert into agged_table values(1,4);

-- query 166
USE ${case_db};
select distinct k2 from agged_table;

-- query 167
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE trand (
    k1 int,
    k2 int
) DUPLICATE KEY(k1)
properties (
    "replication_num" = "1"
);

-- query 168
-- @skip_result_check=true
USE ${case_db};
insert into trand values(1,1);

-- query 169
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT k1 FROM trand;

-- query 170
USE ${case_db};
select k1 from trand group by k1;

-- query 171
-- @skip_result_check=true
USE ${case_db};
insert into trand values(2,2);

-- query 172
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT k1 FROM trand;

-- query 173
USE ${case_db};
select k1 from trand group by k1;

-- query 174
-- @skip_result_check=true
USE ${case_db};
create table all_t1 (
    c1 tinyint,
    c2 tinyint,
    c3 tinyint,
    c4 tinyint,
    c5 smallint,
    c6 smallint,
    c7 smallint,
    c8 smallint,
    c9 int,
    c10 int,
    c11 int,
    c12 int,
    c13 bigint,
    c14 bigint,
    c15 bigint,
    c16 bigint,
    c17 largeint,
    c18 largeint,
    c19 largeint,
    c20 largeint,
    c21 date,
    c22 date,
    c23 date,
    c24 date
) DUPLICATE KEY(c1) DISTRIBUTED BY RANDOM PROPERTIES('replication_num' = '1');

-- query 175
-- @skip_result_check=true
USE ${case_db};
insert into all_t1 SELECT x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x FROM TABLE(generate_series(1,  300000)) as g(x);

-- query 176
USE ${case_db};
select distinct c1, c2, c3, c4, c5, c6, c7, c8 from all_t1 order by 1,2,3,4,5,6,7,8 desc limit 1;

-- query 177
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c1 FROM all_t1;

-- query 178
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c2 FROM all_t1;

-- query 179
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c3 FROM all_t1;

-- query 180
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c4 FROM all_t1;

-- query 181
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c5 FROM all_t1;

-- query 182
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c6 FROM all_t1;

-- query 183
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c7 FROM all_t1;

-- query 184
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c8 FROM all_t1;

-- query 185
USE ${case_db};
select distinct c9, c10, c11, c12, c13, c14, c15, c16 from all_t1 order by 1,2,3,4,5,6,7,8 desc limit 1;

-- query 186
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c9 FROM all_t1;

-- query 187
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c10 FROM all_t1;

-- query 188
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c11 FROM all_t1;

-- query 189
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c12 FROM all_t1;

-- query 190
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c13 FROM all_t1;

-- query 191
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c14 FROM all_t1;

-- query 192
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c15 FROM all_t1;

-- query 193
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c16 FROM all_t1;

-- query 194
USE ${case_db};
select distinct c17, c18, c19, c20, c21, c22, c23, c24 from all_t1 order by 1,2,3,4,5,6,7,8 desc limit 1;

-- query 195
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c17 FROM all_t1;

-- query 196
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c18 FROM all_t1;

-- query 197
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c19 FROM all_t1;

-- query 198
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c20 FROM all_t1;

-- query 199
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c21 FROM all_t1;

-- query 200
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c22 FROM all_t1;

-- query 201
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c23 FROM all_t1;

-- query 202
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c24 FROM all_t1;

-- query 203
-- @skip_result_check=true
USE ${case_db};
set group_concat_max_len=65535;

-- query 204
USE ${case_db};
WITH result AS (
    SELECT c1, COUNT(*) AS cnt FROM all_t0 GROUP BY c1 ORDER BY c1 LIMIT 100
) SELECT 'Test Case 1' AS test_name, MD5(GROUP_CONCAT(CAST(c1 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 205
USE ${case_db};
WITH result AS (
    SELECT c2, COUNT(*) AS cnt FROM all_t0 GROUP BY c2 ORDER BY c2 LIMIT 100
) SELECT 'Test Case 2' AS test_name, MD5(GROUP_CONCAT(CAST(c2 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 206
USE ${case_db};
WITH result AS (
    SELECT c1, COUNT(*) AS cnt FROM all_decimal GROUP BY c1 ORDER BY c1 LIMIT 100
) SELECT 'Test Case 25' AS test_name, MD5(GROUP_CONCAT(CAST(c1 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 207
USE ${case_db};
WITH result AS (
    SELECT c1, COUNT(*) AS cnt FROM all_numbers_t0 GROUP BY c1 ORDER BY c1 LIMIT 100
) SELECT 'Test Case 29' AS test_name, MD5(GROUP_CONCAT(CAST(c1 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 208
USE ${case_db};
WITH result AS (
    SELECT c1, c2, c3, COUNT(*) AS cnt FROM all_t0 GROUP BY c1, c2, c3 ORDER BY c1, c2, c3 LIMIT 100
) SELECT 'Test Case 39' AS test_name, MD5(GROUP_CONCAT(CAST(c1 AS STRING) || ':' || CAST(c2 AS STRING) || ':' || CAST(c3 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 209
USE ${case_db};
WITH result AS (
    SELECT c6, c7, c8, COUNT(*) AS cnt FROM all_t0 GROUP BY c6, c7, c8 ORDER BY c6, c7, c8 LIMIT 100
) SELECT 'Test Case 40' AS test_name, MD5(GROUP_CONCAT(CAST(c6 AS STRING) || ':' || CAST(c7 AS STRING) || ':' || c8 || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 210
USE ${case_db};
WITH result AS (
    SELECT c13, c14, c15, COUNT(*) AS cnt FROM all_t0 GROUP BY c13, c14, c15 ORDER BY c13, c14, c15 LIMIT 100
) SELECT 'Test Case 42' AS test_name, MD5(GROUP_CONCAT(CAST(c13 AS STRING) || ':' || CAST(c14 AS STRING) || ':' || CAST(c15 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 211
USE ${case_db};
WITH result AS (
    SELECT c9, COUNT(*) AS cnt FROM all_t0 GROUP BY c9 ORDER BY c9 LIMIT 100
) SELECT 'Test Case 51' AS test_name, MD5(GROUP_CONCAT(c9 || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 212
USE ${case_db};
WITH result AS (
    SELECT c9, COUNT(*) AS cnt FROM all_t1 GROUP BY c9 ORDER BY c9 LIMIT 1000
) SELECT 'Test Case 52' AS test_name, MD5(GROUP_CONCAT(CAST(c9 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 213
USE ${case_db};
WITH result AS (
    SELECT c18, COUNT(*) AS cnt FROM all_t0 GROUP BY c18 ORDER BY c18 LIMIT 100
) SELECT 'Test Case 53' AS test_name, MD5(GROUP_CONCAT(CAST(c18 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 214
USE ${case_db};
WITH result AS (
    SELECT c13, COUNT(*) AS cnt FROM all_t0 GROUP BY c13 ORDER BY c13 LIMIT 100
) SELECT 'Test Case 54' AS test_name, MD5(GROUP_CONCAT(CAST(c13 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 215
USE ${case_db};
WITH result AS (
    SELECT c1, COUNT(*) AS cnt FROM all_t0 GROUP BY c1 ORDER BY c1 NULLS FIRST LIMIT 100
) SELECT 'Test Case 55' AS test_name, MD5(GROUP_CONCAT(CAST(c1 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 216
USE ${case_db};
WITH result AS (
    SELECT c5, COUNT(*) AS cnt FROM all_t0 GROUP BY c5 ORDER BY c5 NULLS FIRST LIMIT 100
) SELECT 'Test Case 56' AS test_name, MD5(GROUP_CONCAT(CAST(c5 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 217
USE ${case_db};
WITH result AS (
    SELECT c1, c5, COUNT(*) AS cnt FROM all_numbers_t0 GROUP BY c1, c5 ORDER BY c1, c5 NULLS FIRST LIMIT 100
) SELECT 'Test Case 57' AS test_name, MD5(GROUP_CONCAT(CAST(c1 AS STRING) || ':' || CAST(c5 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 218
USE ${case_db};
WITH result AS (
    SELECT c1, c13, COUNT(*) AS cnt FROM all_t0 GROUP BY c1, c13 ORDER BY c1, c13 NULLS FIRST LIMIT 100
) SELECT 'Test Case 58' AS test_name, MD5(GROUP_CONCAT(CAST(c1 AS STRING) || ':' || CAST(c13 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 219
USE ${case_db};
WITH result AS (
    SELECT c1, c2, COUNT(*) AS cnt FROM all_t0 GROUP BY ROLLUP (c1, c2) ORDER BY 1,2,3 LIMIT 100
) SELECT 'Test Case 59' AS test_name, MD5(GROUP_CONCAT(CAST(c1 AS STRING) || ':' || CAST(c2 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 220
USE ${case_db};
WITH result AS (
    SELECT c3, c6, COUNT(*) AS cnt FROM all_t0 GROUP BY CUBE(c3, c6) ORDER BY 1,2,3 LIMIT 100
) SELECT 'Test Case 60' AS test_name, MD5(GROUP_CONCAT(CAST(c3 AS STRING) || ':' || CAST(c6 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 221
USE ${case_db};
WITH result AS (
    SELECT c13, c14, COUNT(*) AS cnt FROM all_numbers_t0 GROUP BY ROLLUP (c13, c14) ORDER BY 1,2,3 LIMIT 100
) SELECT 'Test Case 61' AS test_name, MD5(GROUP_CONCAT(CAST(c13 AS STRING) || ':' || CAST(c14 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 222
USE ${case_db};
WITH result AS (
    SELECT c1, COUNT(*) AS cnt FROM all_t0 GROUP BY c1 ORDER BY c1 LIMIT 100
) SELECT 'Test Case 62' AS test_name, MD5(GROUP_CONCAT(CAST(c1 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 223
USE ${case_db};
WITH result AS (
    SELECT c1, COUNT(*) AS cnt FROM all_t0 WHERE c1 > 200 GROUP BY c1 ORDER BY c1 LIMIT 100
) SELECT 'Test Case 89' AS test_name, MD5('empty') AS result_hash FROM result LIMIT 1;

-- query 224
USE ${case_db};
WITH result AS (
    SELECT k1, COUNT(*) AS cnt FROM trand GROUP BY k1 ORDER BY k1
) SELECT 'Test Case 90' AS test_name, MD5(GROUP_CONCAT(CAST(k1 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 225
USE ${case_db};
WITH result AS (
    SELECT c8, COUNT(*) AS cnt FROM all_t0 GROUP BY c8 ORDER BY c8 LIMIT 100
) SELECT 'Test Case 93' AS test_name, MD5(GROUP_CONCAT(c8 || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 226
USE ${case_db};
WITH result AS (
    SELECT c20, COUNT(*) AS cnt FROM all_t0 GROUP BY c20 ORDER BY c20 LIMIT 100
) SELECT 'Test Case 94' AS test_name, MD5(GROUP_CONCAT(c20 || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 227
USE ${case_db};
WITH result AS (
    SELECT c1, c5, c9, c13, c17, COUNT(*) AS cnt FROM all_t1 GROUP BY c1, c5, c9, c13, c17 ORDER BY c1, c5, c9, c13, c17 LIMIT 1000
) SELECT 'Test Case 95' AS test_name, MD5(GROUP_CONCAT(CAST(c1 AS STRING) || ':' || CAST(c5 AS STRING) || ':' || CAST(c9 AS STRING) || ':' || CAST(c13 AS STRING) || ':' || CAST(c17 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 228
USE ${case_db};
WITH result AS (
    SELECT c1, c2, c3, c4, c5, COUNT(*) AS cnt FROM all_numbers_t0 GROUP BY c1, c2, c3, c4, c5 ORDER BY c1, c2, c3, c4, c5 LIMIT 100
) SELECT 'Test Case 96' AS test_name, MD5(GROUP_CONCAT(CAST(c1 AS STRING) || ':' || CAST(c2 AS STRING) || ':' || CAST(c3 AS STRING) || ':' || CAST(c4 AS STRING) || ':' || CAST(c5 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;

-- query 229
USE ${case_db};
WITH result AS (
    SELECT c13, c14, c15, c16, c17, COUNT(*) AS cnt FROM all_numbers_t0 GROUP BY c13, c14, c15, c16, c17 ORDER BY c13, c14, c15, c16, c17 LIMIT 100
) SELECT 'Test Case 97' AS test_name, MD5(GROUP_CONCAT(CAST(c13 AS STRING) || ':' || CAST(c14 AS STRING) || ':' || CAST(c15 AS STRING) || ':' || CAST(c16 AS STRING) || ':' || CAST(c17 AS STRING) || ':' || CAST(cnt AS STRING))) AS result_hash FROM result;
