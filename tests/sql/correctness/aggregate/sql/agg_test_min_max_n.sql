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

-- Migrated from dev/test/sql/test_agg_function/R/test_min_max_n
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_min_max_n
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE "t_without_null" (
  "c_id" INTEGER NOT NULL,
  "c_int" INTEGER NOT NULL,
  "c_tinyint" TINYINT NOT NULL,
  "c_smallint" SMALLINT NOT NULL,
  "c_bigint" BIGINT NOT NULL,
  "c_decimal_wide" DECIMAL(38,0) NOT NULL,
  "c_float" REAL NOT NULL,
  "c_double" DOUBLE NOT NULL,
  "c_char" CHAR(10) NOT NULL,
  "c_varchar" VARCHAR(100) NOT NULL,
  "c_date" DATE NOT NULL,
  "c_datetime" TIMESTAMP(6) NOT NULL,
  "c_decimal" DECIMAL(9,3) NOT NULL
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
INSERT INTO "t_without_null" (
  "c_id", "c_tinyint", "c_smallint", "c_int", "c_bigint", "c_decimal_wide", "c_float", "c_double", "c_char", "c_varchar", "c_date", "c_datetime", "c_decimal")
VALUES
  ROW(1, 1, 11, 111, 1111, 11111, REAL '1.1', DOUBLE '11.11', CAST('char1' AS CHAR(10)), 'varchar1', DATE '2021-01-01', TIMESTAMP '2021-01-01 00:00:00', 111.11),
  ROW(2, 1, 11, 111, 1111, 11111, REAL '1.1', DOUBLE '11.11', CAST('char1' AS CHAR(10)), 'varchar1', DATE '2021-01-01', TIMESTAMP '2021-01-01 00:00:00', 111.11),
  ROW(3, 1, 11, 111, 1111, 11111, REAL '1.1', DOUBLE '11.11', CAST('char1' AS CHAR(10)), 'varchar1', DATE '2021-01-01', TIMESTAMP '2021-01-01 00:00:00', 111.11),
  ROW(4, 1, 11, 111, 1111, 11111, REAL '1.1', DOUBLE '11.11', CAST('char1' AS CHAR(10)), 'varchar1', DATE '2021-01-01', TIMESTAMP '2021-01-01 00:00:00', 111.11),
  ROW(5, 1, 11, 111, 1111, 11111, REAL '1.1', DOUBLE '11.11', CAST('char1' AS CHAR(10)), 'varchar1', DATE '2021-01-01', TIMESTAMP '2021-01-01 00:00:00', 111.11),

  ROW(11, 2, 22, 222, 2222, 22222, REAL '2.2', DOUBLE '22.22', CAST('char2' AS CHAR(10)), 'varchar2', DATE '2021-01-02', TIMESTAMP '2021-01-02 00:00:00', 222.22),
  ROW(12, 2, 22, 222, 2222, 22222, REAL '2.2', DOUBLE '22.22', CAST('char2' AS CHAR(10)), 'varchar2', DATE '2021-01-02', TIMESTAMP '2021-01-02 00:00:00', 222.22),
  ROW(13, 2, 22, 222, 2222, 22222, REAL '2.2', DOUBLE '22.22', CAST('char2' AS CHAR(10)), 'varchar2', DATE '2021-01-02', TIMESTAMP '2021-01-02 00:00:00', 222.22),

  ROW(21, 3, 33, 333, 3333, 33333, REAL '3.3', DOUBLE '33.33', CAST('char3' AS CHAR(10)), 'varchar3', DATE '2021-01-03', TIMESTAMP '2021-01-03 00:00:00', 333.33),
  ROW(22, 3, 33, 333, 3333, 33333, REAL '3.3', DOUBLE '33.33', CAST('char3' AS CHAR(10)), 'varchar3', DATE '2021-01-03', TIMESTAMP '2021-01-03 00:00:00', 333.33),

  ROW(31, 4, 44, 444, 4444, 44444, REAL '4.4', DOUBLE '44.44', CAST('char4' AS CHAR(10)), 'varchar4', DATE '2021-01-04', TIMESTAMP '2021-01-04 00:00:00', 444.44),

  ROW(91, 10, 100, 1000, 10000, 100000, REAL '1.01', 100.01, CAST('char10' AS CHAR(10)), 'varchar10', DATE '2021-01-10', TIMESTAMP '2021-01-10 00:00:00', 1000.01);

-- query 4
USE ${case_db};
SELECT  min(c_tinyint, 3) FROM t_without_null;

-- query 5
USE ${case_db};
SELECT  min(c_smallint, 3) FROM t_without_null;

-- query 6
USE ${case_db};
SELECT  min(c_int, 3) FROM t_without_null;

-- query 7
USE ${case_db};
SELECT  min(c_bigint, 3) FROM t_without_null;

-- query 8
USE ${case_db};
SELECT CONCAT('[',
              CAST(boundary_result.min_values[1] AS VARCHAR), ',',
              CAST(boundary_result.min_values[2] AS VARCHAR), ',',
              CAST(boundary_result.min_values[3] AS VARCHAR), ']') AS "min(c_decimal_wide, 3)"
FROM (
  SELECT  min(c_decimal_wide, 3) AS min_values
  FROM t_without_null
) AS boundary_result;

-- query 9
USE ${case_db};
SELECT  min(c_float, 3) FROM t_without_null;

-- query 10
USE ${case_db};
SELECT  min(c_double, 3) FROM t_without_null;

-- query 11
USE ${case_db};
SELECT min(CAST(c_char AS VARCHAR), 3) FROM t_without_null;

-- query 12
USE ${case_db};
SELECT  min(c_varchar, 3) FROM t_without_null;

-- query 13
USE ${case_db};
SELECT  min(c_date, 3) FROM t_without_null;

-- query 14
USE ${case_db};
SELECT  min(c_datetime, 3) FROM t_without_null;

-- query 15
USE ${case_db};
SELECT  min(c_decimal, 3) FROM t_without_null;

-- query 16
USE ${case_db};
SELECT  min(c_tinyint, 1) FROM t_without_null;

-- query 17
USE ${case_db};
SELECT  min(c_smallint, 1) FROM t_without_null;

-- query 18
USE ${case_db};
SELECT  min(c_int, 1) FROM t_without_null;

-- query 19
USE ${case_db};
SELECT  min(c_tinyint, 5) FROM t_without_null;

-- query 20
USE ${case_db};
SELECT  min(c_smallint, 5) FROM t_without_null;

-- query 21
USE ${case_db};
SELECT  min(c_int, 5) FROM t_without_null;

-- query 22
USE ${case_db};
SELECT  c_id % 2 AS g, min(c_int, 2) FROM t_without_null GROUP BY g ORDER BY g;

-- query 23
USE ${case_db};
SELECT  c_id % 3 AS g, min(c_varchar, 2) FROM t_without_null GROUP BY g ORDER BY g;

-- query 24
USE ${case_db};
SELECT  c_id % 3 AS g, min(c_varchar, 2) FROM t_without_null GROUP BY g ORDER BY g;

-- query 25
USE ${case_db};
SELECT  c_id % 4 AS g, min(c_date, 2) FROM t_without_null GROUP BY g ORDER BY g;

-- query 26
USE ${case_db};
SELECT min(col1, 3) FROM (VALUES ROW(1)) AS tmp(col1);

-- query 27
USE ${case_db};
SELECT min(col1, 3) FROM (VALUES ROW(1),ROW(2),ROW(3),ROW(4),ROW(5),ROW(6)) AS tmp(col1);

-- query 28
USE ${case_db};
SELECT min(1, 3);
