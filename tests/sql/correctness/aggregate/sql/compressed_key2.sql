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

-- Migrated from dev/test/sql/test_agg/R/test_agg_compressed_key2
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_agg_compressed_key2 @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t3 (
    c_2_0 LARGEINT NOT NULL,
    c_2_1 LARGEINT NOT NULL,
    c_2_2 LARGEINT NOT NULL,
    c_2_3 LARGEINT NOT NULL,
    c_2_4 LARGEINT NOT NULL,
    c_2_5 LARGEINT NOT NULL,
    c_2_6 LARGEINT NOT NULL,
    c_2_7 LARGEINT NOT NULL,
    c_2_8 LARGEINT NOT NULL,
    c_2_9 LARGEINT NOT NULL,
    c_2_10 LARGEINT NOT NULL,
    c_2_11 LARGEINT NOT NULL,
    c_2_12 LARGEINT NOT NULL,
    c_2_13 LARGEINT NOT NULL,
    c_2_14 LARGEINT NOT NULL,
    c_2_15 LARGEINT NOT NULL
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into t3 values (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into t3 values (128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128);

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into t3 values (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);

-- query 6
USE ${case_db};
SELECT
  CAST(trc2_output.c_2_0 AS VARCHAR) AS c_2_0,
  CAST(trc2_output.c_2_1 AS VARCHAR) AS c_2_1,
  CAST(trc2_output.c_2_2 AS VARCHAR) AS c_2_2,
  CAST(trc2_output.c_2_3 AS VARCHAR) AS c_2_3,
  CAST(trc2_output.c_2_4 AS VARCHAR) AS c_2_4,
  CAST(trc2_output.c_2_5 AS VARCHAR) AS c_2_5,
  CAST(trc2_output.c_2_6 AS VARCHAR) AS c_2_6,
  CAST(trc2_output.c_2_7 AS VARCHAR) AS c_2_7,
  CAST(trc2_output.c_2_8 AS VARCHAR) AS c_2_8,
  CAST(trc2_output.c_2_9 AS VARCHAR) AS c_2_9,
  CAST(trc2_output.c_2_10 AS VARCHAR) AS c_2_10,
  CAST(trc2_output.c_2_11 AS VARCHAR) AS c_2_11,
  CAST(trc2_output.c_2_12 AS VARCHAR) AS c_2_12,
  CAST(trc2_output.c_2_13 AS VARCHAR) AS c_2_13,
  CAST(trc2_output.c_2_14 AS VARCHAR) AS c_2_14,
  CAST(trc2_output.c_2_15 AS VARCHAR) AS c_2_15
FROM (
  select distinct c_2_0, c_2_1, c_2_2, c_2_3, c_2_4, c_2_5, c_2_6, c_2_7, c_2_8, c_2_9, c_2_10, c_2_11, c_2_12, c_2_13, c_2_14, c_2_15 from t3 order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
) AS trc2_output
ORDER BY trc2_output.c_2_0, trc2_output.c_2_1, trc2_output.c_2_2, trc2_output.c_2_3, trc2_output.c_2_4, trc2_output.c_2_5, trc2_output.c_2_6, trc2_output.c_2_7, trc2_output.c_2_8, trc2_output.c_2_9, trc2_output.c_2_10, trc2_output.c_2_11, trc2_output.c_2_12, trc2_output.c_2_13, trc2_output.c_2_14, trc2_output.c_2_15;

-- query 7
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t4 (
    c_2_0 LARGEINT NOT NULL
)
TBLPROPERTIES ("format-version" = "3");

-- query 8
-- @skip_result_check=true
USE ${case_db};
insert into t4 values (1024), (-2139922094);

-- query 9
-- @expect_error=Column 't4' cannot be resolved.
USE ${case_db};
select distinct c_2_0 from t4 order by t4;
