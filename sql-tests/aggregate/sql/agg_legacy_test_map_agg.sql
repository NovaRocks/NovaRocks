-- Migrated from dev/test/sql/test_agg_function/R/test_map_agg
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_map_agg
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1 (
    c1 int,
    c2 boolean,
    c3 tinyint,
    c4 int,
    c5 bigint,
    c6 largeint,
    c7 string,
    c8 double,
    c9 date,
    c10 datetime,
    c11 array<int>,
    c12 map<varchar(5), double>,
    c13 struct<a bigint, b string>
    )
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 3
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 values
    (1, true, 11, 111, 1111, 11111, "111111", 1.1, "2024-09-01", "2024-09-01 18:00:00", [1, 2, 3], map('key', 5.5), row(100, "abc")),
    (2, false, 22, 222, 2222, 22222, "222222", 2.2, "2024-09-02", "2024-09-02 11:00:00", [3, 4, 5], map('key', 511.2), row(200, "bcd")),
    (3, true, 33, 333, 3333, 33333, "333333", 3.3,  "2024-09-03", "2024-09-03 00:00:00", [4, 1, 2], map('key', 666.6), row(300, "cccecd")),
    (4, false, 11, 444, 4444, 44444, "444444", 4.4, "2024-09-04", "2024-09-04 12:00:00", [7, 7, 5], map('key', 444.4), row(400, "efdg")),
    (5, null, null, null, null, null, null, null, null, null, null, null, null);

-- query 4
-- @skip_result_check=true
USE ${case_db};
set streaming_preaggregation_mode=force_preaggregation;

-- query 5
USE ${case_db};
select map_size(map_agg(c1, c3)) from t1;

-- query 6
USE ${case_db};
select map_agg(c1, c3)[1] from t1;

-- query 7
USE ${case_db};
select map_agg(c1, c3)[2] from t1;

-- query 8
USE ${case_db};
select map_agg(c1, c3)[3] from t1;

-- query 9
USE ${case_db};
select map_agg(c1, c3)[4] from t1;

-- query 10
USE ${case_db};
select map_agg(c1, c3)[5] from t1;

-- query 11
USE ${case_db};
select map_size(map_agg(c5, c6)) from t1;

-- query 12
USE ${case_db};
select map_agg(c5, c6)[1111] from t1;

-- query 13
USE ${case_db};
select map_size(map_agg(c6, c10)) from t1;

-- query 14
USE ${case_db};
select map_agg(c6, c10)[11111] from t1;

-- query 15
USE ${case_db};
select map_agg(c6, c10)[22222] from t1;

-- query 16
USE ${case_db};
select map_size(map_agg(c8, c5)) from t1;

-- query 17
USE ${case_db};
select map_agg(c8, c5)[1.1] from t1;

-- query 18
USE ${case_db};
select map_agg(c8, c5)[4.4] from t1;

-- query 19
USE ${case_db};
select c11, map_agg(c10, c11) res from t1 group by c11 order by c11[1];

-- query 20
USE ${case_db};
select c12, map_agg(c9, c12) res from t1 group by c12 order by c12['key'];

-- query 21
USE ${case_db};
select c13, map_agg(c9, c13) res from t1 group by c13 order by c13.a;

-- query 22
-- @skip_result_check=true
USE ${case_db};
set streaming_preaggregation_mode=force_streaming;

-- query 23
USE ${case_db};
select map_size(map_agg(c1, c3)) from t1;

-- query 24
USE ${case_db};
select map_agg(c1, c3)[1] from t1;

-- query 25
USE ${case_db};
select map_agg(c1, c3)[2] from t1;

-- query 26
USE ${case_db};
select map_agg(c1, c3)[3] from t1;

-- query 27
USE ${case_db};
select map_agg(c1, c3)[4] from t1;

-- query 28
USE ${case_db};
select map_agg(c1, c3)[5] from t1;

-- query 29
USE ${case_db};
select map_size(map_agg(c5, c6)) from t1;

-- query 30
USE ${case_db};
select map_agg(c5, c6)[1111] from t1;

-- query 31
USE ${case_db};
select map_size(map_agg(c6, c10)) from t1;

-- query 32
USE ${case_db};
select map_agg(c6, c10)[11111] from t1;

-- query 33
USE ${case_db};
select map_agg(c6, c10)[22222] from t1;

-- query 34
USE ${case_db};
select map_size(map_agg(c8, c5)) from t1;

-- query 35
USE ${case_db};
select map_agg(c8, c5)[1.1] from t1;

-- query 36
USE ${case_db};
select map_agg(c8, c5)[4.4] from t1;

-- query 37
USE ${case_db};
select c11, map_agg(c10, c11) res from t1 group by c11 order by c11[1];

-- query 38
USE ${case_db};
select c12, map_agg(c9, c12) res from t1 group by c12 order by c12['key'];

-- query 39
USE ${case_db};
select c13, map_agg(c9, c13) res from t1 group by c13 order by c13.a;
