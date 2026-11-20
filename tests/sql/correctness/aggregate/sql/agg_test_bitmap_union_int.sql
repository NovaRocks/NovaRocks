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

-- Migrated from dev/test/sql/test_agg_function/R/test_bitmap_union_int
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_bitmap_union_int
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1 (
    c1 int,
    c2 int
    ) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 values
    ROW(-1, -1),
    ROW(1, 1), ROW(2, 2), ROW(3, 3), ROW(4, 4), ROW(5, 5), ROW(6, 6), ROW(7, 7), ROW(8, 8), ROW(9, 9),
    ROW(10, 10), ROW(11, 11), ROW(12, 12), ROW(13, 13), ROW(14, 14), ROW(15, 15), ROW(16, 16), ROW(17, 17), ROW(18, 18), ROW(19, 19),
    ROW(20, 20), ROW(21, 21), ROW(22, 22), ROW(23, 23), ROW(24, 24), ROW(25, 25), ROW(26, 26), ROW(27, 27), ROW(28, 28), ROW(29, 29),
    ROW(30, 30), ROW(31, 31), ROW(32, 32), ROW(33, 33), ROW(34, 34), ROW(35, 35), ROW(36, 36), ROW(37, 37), ROW(38, 38), ROW(39, -1);

-- query 4
USE ${case_db};
select bitmap_union_int(c2) from t1;
