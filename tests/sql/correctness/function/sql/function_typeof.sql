-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- Type names retain the exact declaration of an evaluated operand.
-- Include persisted values, nested declarations, and original operand errors.

-- query 1
select typeof(cast(1 as tinyint));

-- query 2
select typeof(cast(1 as smallint));

-- query 3
select typeof(cast(1 as int));

-- query 4
select typeof(cast(1 as bigint));

-- query 5
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
-- @expect_error=[sql.analyze.type_mismatch] Unknown type: largeint
select typeof(cast(1 as largeint));

-- query 6
select typeof(cast(1 as decimal(19, 2)));

-- query 7
select typeof(cast(1 as double));

-- query 8
select typeof(cast(1 as REAL));

-- query 9
select typeof(cast(1 as boolean));

-- query 10
select typeof(cast('a' as char));

-- query 11
select typeof(cast(1 as VARCHAR));

-- query 12
select typeof(cast(1 as varchar));

-- query 13
select typeof(cast('s' as VARBINARY));

-- query 14
select typeof(cast('2023-03-07' as date));

-- query 15
SELECT typeof(CAST('2023-03-07 11:22:33' AS TIMESTAMP(6)));

-- query 16
select typeof([1, 2, 3]);

-- query 17
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
-- @expect_error=[sql.analyze.unavailable_implementation] selected overload `builtin.scalar/get_json_object/(any,utf8)->utf8;strict;legacy` has no installed implementation
select typeof(get_json_object('{"k1":1, "k2":"v2"}', '$.k1'));

-- query 18
select typeof(MAP(ARRAY[1, 2, 3], ARRAY['apple', 'orange', 'pear']));

-- query 19
select typeof(CAST(ROW(1, 2, 3, 4) AS ROW(col1 INTEGER, col2 INTEGER, col3 INTEGER, col4 INTEGER)));

-- query 20
select typeof(parse_json('{"a": 1, "b": true}'));

-- query 21
select typeof(null);

-- query 22
-- @skip_result_check=true
USE ${case_db};
create table t1 (
  c1 tinyint,
  c2 smallint,
  c3 int,
  c4 bigint,
  c5 REAL,
  c6 decimal(19, 2),
  c7 decimal(38, 9),
  c8 decimal(38, 9),
  c9 boolean,
  c10 varchar,
  c11 VARCHAR,
  c12 varchar,
  c13 VARBINARY,
  c14 date,
  c15 TIMESTAMP(6),
  c16 array<tinyint>,
  c17 VARCHAR,
  c18 map<tinyint, varchar>,
  c19 ROW(col1 tinyint, col2 tinyint, col3 tinyint, col4 tinyint),
  c20 json
) WITH (format_version = 3);
insert into t1 values ROW(
  NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL,
  NULL, NULL, NULL, NULL, NULL
);

-- query 23
USE ${case_db};
select typeof(c1)
  ,typeof(c2)
  ,typeof(c3)
  ,typeof(c4)
  ,typeof(c5)
  ,typeof(c6)
  ,typeof(c7)
  ,typeof(c8)
  ,typeof(c9)
  ,typeof(c10)
  ,typeof(c11)
  ,typeof(c12)
  ,typeof(c13)
  ,typeof(c14)
  ,typeof(c15)
  ,typeof(c16)
  ,typeof(c17)
  ,typeof(c18)
  ,typeof(c19)
  ,typeof(c20)
  from t1;

-- query 24
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT typeof(10 / x) FROM (VALUES 0, 1) AS t(x);

-- query 25
SELECT x, TRY(typeof(10 / x)) AS type_name
FROM (VALUES 0, 1, NULL) AS t(x)
ORDER BY x NULLS LAST;

-- query 26
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT typeof(CAST(1 AS CHAR));

-- query 27
SELECT typeof(CAST(NULL AS ROW(a INTEGER, "B" VARCHAR(2)))) AS row_type,
       typeof(CAST(NULL AS MAP(VARCHAR(3), ARRAY(DECIMAL(5,2))))) AS map_type,
       typeof(CAST(NULL AS TIMESTAMP(12))) AS temporal_type;

-- query 28
-- @expect_trino_error=COLUMN_NOT_FOUND,USER_ERROR
SELECT typeof(CAST(missing_input AS INTEGER));

-- query 29
SELECT typeof(JSON '{"a":1}') AS json_type;
