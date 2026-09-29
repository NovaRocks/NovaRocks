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

-- Independent admission and authoritative lowering controls; no storage dependency.
-- query 1
SELECT lower('A') AS lowercase, parse_json('{') IS NULL AS caught_parse;

-- query 2
SELECT every(TRUE) AS all_true, max_by(1,2) AS winning_value;

-- query 3
SELECT row_number() OVER () AS row_number_value;

-- query 4
SELECT -CAST(1 AS SMALLINT) AS negated, typeof(CAST(1 AS TINYINT)) AS narrow_type;

-- query 5
SELECT field(1,1,2) AS first_match;

-- query 6
-- @expect_error=no admitted selected scalar implementation
SELECT array_reverse([1]) WHERE FALSE;

-- query 7
-- @expect_error=no admitted selected scalar implementation
SELECT next_day('2020-01-01') WHERE FALSE;

-- query 8
-- @expect_error=Unknown function
SELECT intrinsic_definitely_unknown(1) WHERE FALSE;

-- query 9
-- @expect_error=no admitted selected scalar implementation
SELECT map_filter(map(1,10),[TRUE]) WHERE FALSE;

-- query 10
SELECT array_sort([]) AS sorted_empty,
       array_min([NULL]) IS NULL AS minimum_null,
       array_max([NULL]) IS NULL AS maximum_null,
       array_top_n([],3) AS top_empty,
       array_top_n([NULL],3) AS top_single_null,
       array_sortby([[2],[1]],[NULL,NULL]) AS null_key_order;
