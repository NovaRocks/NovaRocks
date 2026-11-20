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

-- Migrated from dev/test/sql/test_bitmap_functions/T/test_bitmap_to_string
-- Test Objective:
-- 1. Validate bitmap_to_string with large uint64 values near UINT64_MAX.
-- 2. Validate multi-value bitmaps covering the uint64 upper range.
--
-- The original StarRocks test exercised this via `generate_series(LARGEINT)`,
-- which is not supported here. We construct the same bitmap contents
-- directly with `bitmap_from_string('v1,v2,...')` so the
-- `bitmap_to_string` paths under test still cover values near UINT64_MAX.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_bitmap_str (
  "c1" int(11),
  "c2" bitmap
) WITH (format_version = 3);

-- query 2
-- Table is still empty (no inserts yet)
select bitmap_to_string(c2) from ${case_db}.t_bitmap_str;

-- query 3
-- @skip_result_check=true
insert into ${case_db}.t_bitmap_str values ROW(1, bitmap_from_string('18446744073709551611,18446744073709551612,18446744073709551613,18446744073709551614,18446744073709551615'));

-- query 4
-- 5 large values: 18446744073709551611 through 18446744073709551615
select bitmap_to_string(c2) from ${case_db}.t_bitmap_str;

-- query 5
-- @skip_result_check=true
-- The expected 65-value bitmap is inserted as one explicit row so the
-- append table stores exactly the value read by the query.
TRUNCATE TABLE ${case_db}.t_bitmap_str;
insert into ${case_db}.t_bitmap_str values ROW(1, bitmap_from_string('18446744073709551551,18446744073709551552,18446744073709551553,18446744073709551554,18446744073709551555,18446744073709551556,18446744073709551557,18446744073709551558,18446744073709551559,18446744073709551560,18446744073709551561,18446744073709551562,18446744073709551563,18446744073709551564,18446744073709551565,18446744073709551566,18446744073709551567,18446744073709551568,18446744073709551569,18446744073709551570,18446744073709551571,18446744073709551572,18446744073709551573,18446744073709551574,18446744073709551575,18446744073709551576,18446744073709551577,18446744073709551578,18446744073709551579,18446744073709551580,18446744073709551581,18446744073709551582,18446744073709551583,18446744073709551584,18446744073709551585,18446744073709551586,18446744073709551587,18446744073709551588,18446744073709551589,18446744073709551590,18446744073709551591,18446744073709551592,18446744073709551593,18446744073709551594,18446744073709551595,18446744073709551596,18446744073709551597,18446744073709551598,18446744073709551599,18446744073709551600,18446744073709551601,18446744073709551602,18446744073709551603,18446744073709551604,18446744073709551605,18446744073709551606,18446744073709551607,18446744073709551608,18446744073709551609,18446744073709551610,18446744073709551611,18446744073709551612,18446744073709551613,18446744073709551614,18446744073709551615'));

-- query 6
-- @skip_result_check=true
USE ${case_db};

-- query 7
-- 65 values: 18446744073709551551 through 18446744073709551615
select bitmap_to_string(c2) from ${case_db}.t_bitmap_str;
