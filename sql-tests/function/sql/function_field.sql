-- Migrated from dev/test/sql/test_function/T/test_field
-- Test Objective:
-- 1. Validate FIELD() function with int, float, string, and mixed-type arguments.
-- 2. Validate FIELD() with NULL inputs.
-- 3. Validate ORDER BY field() for custom sort ordering on table data.
-- 4. Validate FIELD() across views with different numeric/date types (boolean, float, double, date, datetime).
-- 5. Validate FIELD() concurrency correctness with constant values across multiple parallel scans.

-- query 1
-- Setup: create table t1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1 (c1 int, c2 varchar(100), c3 double)
    DUPLICATE KEY(c1)
    DISTRIBUTED BY HASH(c1) BUCKETS 1
    PROPERTIES("replication_num" = "1");

-- query 2
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1(c1, c2, c3) VALUES (0, "hello", 5.5), (1, "world", 6.01), (2, "star", 5.0), (3, "rocks", 1.1111);

-- query 3
-- FIELD with matching int literals
USE ${case_db};
SELECT field(1, 1, 1);

-- query 4
USE ${case_db};
SELECT field(1.0, 1.0, 1.0);

-- query 5
USE ${case_db};
SELECT field(01.0, 001.0, 0001.0);

-- query 6
-- String vs numeric comparison
USE ${case_db};
SELECT field('01.0', 001.0, 0001.0);

-- query 7
USE ${case_db};
SELECT field('01', '1');

-- query 8
USE ${case_db};
SELECT field('01', '1', 1);

-- query 9
USE ${case_db};
SELECT field('a', 'b');

-- query 10
USE ${case_db};
SELECT field('a', 'b', 1);

-- query 11
-- FIELD with NULL inputs
USE ${case_db};
SELECT field(NULL, NULL, 1);

-- query 12
USE ${case_db};
SELECT field(NULL, NULL);

-- query 13
-- ORDER BY field() on int column
USE ${case_db};
SELECT *, c1, field(c1, 3, 2, 1, 0) FROM t1 ORDER BY field(c1, 3, 2, 1, 0);

-- query 14
USE ${case_db};
SELECT *, c1, field(c1, 2, 1, 3, 0) FROM t1 ORDER BY field(c1, 2, 1, 3, 0);

-- query 15
USE ${case_db};
SELECT *, c1, field(c1, 1, 1, 1, 1) FROM t1 ORDER BY field(c1, 1, 1, 1, 1);

-- query 16
-- ORDER BY field() on string column asc
USE ${case_db};
SELECT * FROM t1 ORDER BY field(c2, "star", "rocks", "hello", "world") asc;

-- query 17
-- ORDER BY field() on string column desc
USE ${case_db};
SELECT * FROM t1 ORDER BY field(c2, "star", "rocks", "hello", "world") desc;

-- query 18
-- ORDER BY field() on double column with expressions
USE ${case_db};
SELECT * FROM t1 ORDER BY field(c3, 1.1111, 6 - 1, 6.01, 5 + 0.5);

-- query 19
-- Setup views for cross-type field() tests
-- @skip_result_check=true
USE ${case_db};
CREATE VIEW vvv AS SELECT cast(1 as double) AS c1;

-- query 20
-- @skip_result_check=true
USE ${case_db};
CREATE VIEW vv AS SELECT cast(1 as float) AS c1;

-- query 21
-- @skip_result_check=true
USE ${case_db};
CREATE VIEW v AS SELECT cast(1 as boolean) AS c1;

-- query 22
-- FIELD across float and double views
USE ${case_db};
SELECT field(vv.c1, vvv.c1) FROM vv, vvv;

-- query 23
-- FIELD with boolean view
USE ${case_db};
SELECT field(v.c1, v.c1) FROM vv, v, vvv;

-- query 24
-- FIELD boolean vs float
USE ${case_db};
SELECT field(v.c1, vv.c1) FROM vv, v, vvv;

-- query 25
-- Setup date/datetime views
-- @skip_result_check=true
USE ${case_db};
CREATE VIEW dv AS SELECT cast('2022-02-02' as date) c1;

-- query 26
-- @skip_result_check=true
USE ${case_db};
CREATE VIEW dvv AS SELECT cast('2022-02-02' as datetime) c1;

-- query 27
-- FIELD date vs datetime
USE ${case_db};
SELECT field(dv.c1, dvv.c1) FROM dv, dvv;

-- query 28
-- FIELD date vs date
USE ${case_db};
SELECT field(dv.c1, dv.c1) FROM dv, dvv;

-- query 29
-- FIELD date vs float and datetime
USE ${case_db};
SELECT field(dv.c1, vv.c1, dvv.c1) FROM dv, dvv, vv;

-- query 30
-- Setup: create t2 for concurrency test with 16 buckets
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t2 (c1 int, c2 string)
    DUPLICATE KEY(c1)
    DISTRIBUTED BY HASH(c1) BUCKETS 16
    PROPERTIES("replication_num" = "1");

-- query 31
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t2 VALUES (1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f"), (7, "g"), (8, "h"), (9, "i"),
                      (10, "j"), (11, "k"), (12, "l"), (13, "m"), (14, "n"), (15, "o"), (16, "p");

-- query 32
-- Concurrency test: repeated identical queries to detect constant-value race conditions
USE ${case_db};
SELECT * FROM t2 ORDER BY field('a', 'bb', 'cc', 'a'), c1 asc;

-- query 33
USE ${case_db};
SELECT * FROM t2 ORDER BY field('a', 'bb', 'cc', 'a'), c1 asc;

-- query 34
USE ${case_db};
SELECT * FROM t2 ORDER BY field('a', 'bb', 'cc', 'a'), c1 asc;

-- query 35
USE ${case_db};
SELECT * FROM t2 ORDER BY field('a', 'bb', 'cc', 'a'), c1 asc;

-- query 36
USE ${case_db};
SELECT * FROM t2 ORDER BY field('a', 'bb', 'cc', 'a'), c1 asc;

-- query 37
USE ${case_db};
SELECT * FROM t2 ORDER BY field('a', 'bb', 'cc', 'a'), c1 asc;

-- query 38
USE ${case_db};
SELECT * FROM t2 ORDER BY field('a', 'bb', 'cc', 'a'), c1 asc;

-- query 39
USE ${case_db};
SELECT * FROM t2 ORDER BY field('a', 'bb', 'cc', 'a'), c1 asc;

-- query 40
USE ${case_db};
SELECT * FROM t2 ORDER BY field('a', 'bb', 'cc', 'a'), c1 asc;

-- query 41
USE ${case_db};
SELECT * FROM t2 ORDER BY field('a', 'bb', 'cc', 'a'), c1 asc;
