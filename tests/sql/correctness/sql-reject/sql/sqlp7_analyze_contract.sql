-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
-- SQLP-7 analyzer diagnostics and query capability refusals use the Trino wire.

-- query 1
-- @expect_trino_error=TABLE_NOT_FOUND,USER_ERROR
SELECT * FROM sqlp7_missing_table;

-- query 2
-- @expect_trino_error=COLUMN_NOT_FOUND,USER_ERROR
WITH t AS (SELECT 1 AS c) SELECT missing FROM t;

-- query 3
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT sqlp7_not_a_function(1);

-- query 4
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT substring('x', 1, 2, 3);

-- query 5
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SELECT 1 GROUP BY 9999999999999999999999999999999999999999;

-- query 6
-- @expect_trino_error=INVALID_ARGUMENTS,USER_ERROR
SELECT ds_hll_count_distinct(1, 1);

-- query 7
-- @expect_trino_error=GENERIC_USER_ERROR,USER_ERROR
SELECT count_if(DISTINCT true);

-- query 8
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT (x) -> x;

-- query 9
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS x) SELECT * FROM a NATURAL JOIN b;

-- query 10
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT * FROM (VALUES 1) t(x) MATCH_RECOGNIZE (ORDER BY x PATTERN (A) DEFINE A AS TRUE);

-- query 11
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT * FROM JSON_TABLE('[1]', 'lax $[*]' COLUMNS (item integer PATH 'lax $')) jt;

-- query 12
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT * FROM (VALUES (1, 2)) t(a, b) PIVOT (sum(a) FOR b IN (2));

-- query 13
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT * FROM NEAREST (FROM (VALUES 1) t(x) WHERE x > 0 MATCH x <= 1);

-- query 14
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT * FROM (VALUES 1) t(x) TABLESAMPLE SYSTEM (10);

-- query 15
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT 1 AS x UNION CORRESPONDING SELECT 2 AS x;

-- query 16
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT 1 AS x UNION CORRESPONDING BY (x) SELECT 2 AS x;

-- query 17
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
WITH FUNCTION f() RETURNS bigint RETURN 1 SELECT f();

-- query 18
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT INTERVAL '1' DAY;

-- query 19
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT CAST(NULL AS INTERVAL DAY TO SECOND);

-- query 20
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT ((1 - 1) DAY TO SECOND);

-- query 21
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT CAST(NULL AS integer ARRAY[3]);

-- query 22
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT 1 MATCH (SELECT 1);

-- query 23
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT ROW(1, 2) OVERLAPS ROW(3, 4);

-- query 24
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT 1 = ALL (SELECT 1);

-- query 25
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT TRUE IS UNKNOWN;

-- query 26
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT 1 BETWEEN SYMMETRIC 0 AND 2;

-- query 27
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT UNIQUE (SELECT 1);

-- query 28
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT (1).f();

-- query 29
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT bigint::f();

-- query 30
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT ARRAY[1][*];

-- query 31
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT measure_name OVER w;

-- query 32
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT sum(1) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW);

-- query 33
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT sum(1) OVER (ORDER BY 1 ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING PATTERN (A) DEFINE A AS TRUE);

-- query 34
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT * FROM TABLE (p14_other_function());

-- query 35
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT * FROM TABLE (p14_other_function(TABLE (SELECT 1 AS id)));

-- query 36
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT * FROM TABLE (p14_other_function(DESCRIPTOR(id bigint)));

-- query 37
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT * FROM TABLE (p14_other_function(1 COPARTITION (t, u)));

-- query 38
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT JSON_EXISTS('{"a":1}', 'lax $.a');

-- query 39
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT JSON_VALUE('{"a":1}', 'lax $.a');

-- query 40
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT JSON_SERIALIZE('[]');

-- query 41
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT JSON_QUERY('{"a":1}', 'lax $.a' WITH UNCONDITIONAL ARRAY WRAPPER);

-- query 42
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT JSON_OBJECT('a' : NULL ABSENT ON NULL);

-- query 43
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT JSON_OBJECT('a' : 1 WITH UNIQUE KEYS);

-- query 44
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT JSON_OBJECT('a' : 1 RETURNING varchar);

-- query 45
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT JSON_OBJECT('a' : 'null' FORMAT JSON);

-- query 46
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT JSON_ARRAY(1 RETURNING varchar);

-- query 47
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT TIMESTAMP '2020-01-01 00:00:00' AT TIME ZONE 'UTC';

-- query 48
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT TIMESTAMP '2020-01-01 00:00:00' AT LOCAL;

-- query 49
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT CAST(NULL AS TIMESTAMP(3) WITH TIME ZONE);

-- query 50
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SELECT 1 <=> 1;

-- query 51
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SELECT ARRAY<INTEGER>[1];

-- query 52
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SELECT JSON_OBJECT('a', 1, 'b');

-- query 53
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT TRIM(BOTH 'x' FROM 'xxokxx');

-- query 54
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT OVERLAY('abc' PLACING 'x' FROM 2);

-- query 55
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT substring(a => 'x', b => 1);

-- query 56
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT FINAL count(*);
