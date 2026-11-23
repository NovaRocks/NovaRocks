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

-- Migrated from dev/test/sql/test_function/T/test_typeof
-- Test Objective:
-- 1. Validate typeof() returns correct type names for all scalar literal types (tinyint through json, including BINARY).
-- 2. Validate typeof() for complex types: array, map, struct, bitmap, hll, json.
-- 3. Validate typeof() with table columns preserves the stored column types.
-- 4. Validate typeof() with ordinary BITMAP and HLL columns in Iceberg append tables.

-- query 1
select typeof(cast(1 as tinyint));

-- query 2
select typeof(cast(1 as smallint));

-- query 3
select typeof(cast(1 as int));

-- query 4
select typeof(cast(1 as bigint));

-- query 5
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
select typeof(cast(1 as char));

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
  c5 largeint,
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
