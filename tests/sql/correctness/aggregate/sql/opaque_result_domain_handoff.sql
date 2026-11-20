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

-- Exact producer domains keep opaque values internal while their consumers remain usable.
-- query 1
SELECT to_bitmap(1) AS bitmap_state,
       hll_hash(CAST(1 AS BIGINT)) AS hll_state,
       percentile_hash(CAST(1 AS DOUBLE)) AS percentile_state;

-- query 2
SELECT bitmap_to_string(to_bitmap(1)) AS bitmap_members,
       hll_cardinality(hll_hash(CAST(1 AS BIGINT))) AS hll_members;

-- query 3
SELECT [to_bitmap(1), NULL] AS bitmap_values,
       [percentile_hash(CAST(1 AS DOUBLE)), NULL] AS percentile_values;

-- query 4
-- @expect_error=CTAS source contains an unsupported internal opaque value domain
CREATE TABLE ${case_db}.private_percentile_state AS SELECT percentile_hash(CAST(1 AS DOUBLE)) AS state;
