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

-- FIELD common-type contract. Results are independently derived from typed
-- equality and first non-NULL match; aliases isolate the display migration.

-- query 1
SELECT field('01', '1') AS text_identity,
       field('01', '1', 1) AS mixed_numeric,
       field('01.0', 1.0, 1.0) AS decimal_text,
       field('a', 'b', 1) AS invalid_numeric;

-- query 2
SELECT field(NULL, NULL, 1) AS null_first,
       field(NULL, 2147483648, 'bad') AS null_first_ignored_candidates,
       field(1, NULL, 1, 1) AS first_match,
       field(4, NULL, 5) AS no_match;

-- query 3
SELECT field(cast(9007199254740993 AS BIGINT), cast(9007199254740992 AS BIGINT), cast(9007199254740993 AS BIGINT)) AS exact_bigint,
       field(cast('170141183460469231731687303715884105727' AS LARGEINT), cast('170141183460469231731687303715884105726' AS LARGEINT), cast('170141183460469231731687303715884105727' AS LARGEINT)) AS exact_largeint;

-- query 4
SELECT field(cast('9007199254740993.00' AS DECIMAL(20,2)), cast(9007199254740993 AS BIGINT)) AS decimal_integer,
       field(cast('12.3450' AS DECIMAL(20,4)), cast('12.345' AS DECIMAL(20,3)), cast('12.3450' AS DECIMAL(20,4))) AS decimal_scales,
       field(cast('170141183460469231731687303715884105727' AS LARGEINT), cast('170141183460469231731687303715884105727.00' AS DECIMAL(41,2))) AS largeint_decimal;

-- query 5
SELECT field(cast(-0.0 AS DOUBLE), cast(0.0 AS DOUBLE)) AS signed_zero,
       field(cast(1 AS FLOAT), cast(1 AS DOUBLE)) AS float_double,
       field(TRUE, cast(1 AS FLOAT)) AS boolean_numeric;

-- query 6
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE field_keys (id INT, text_key VARCHAR(20), int_key BIGINT, candidate BIGINT, decimal_key DECIMAL(20,2))
TBLPROPERTIES ("format-version" = "3");

-- query 7
-- @skip_result_check=true
USE ${case_db};
INSERT INTO field_keys VALUES
(1, '01', 1, 2, 1.00), (2, '2', 2, 2, 2.00),
(3, 'bad', 3, 3, 3.00), (4, NULL, NULL, 4, NULL),
(5, '9007199254740993', 9007199254740993, 9007199254740992, 9007199254740993.00);

-- query 8
USE ${case_db};
SELECT id, field(text_key, '1', int_key) AS mixed_numeric,
       field(int_key, candidate, int_key) AS exact_first,
       field(decimal_key, int_key) AS decimal_integer
FROM field_keys ORDER BY id;

-- query 9
USE ${case_db};
SELECT id FROM field_keys ORDER BY field(int_key, cast(2 AS BIGINT), cast(1 AS BIGINT)), id;

-- query 10
USE ${case_db};
SELECT sum(field(int_key, candidate, int_key)) AS first_match_sum,
       count(field(text_key, '1', int_key)) AS nonnull_result_rows FROM field_keys;

-- query 11
-- @expect_error=field requires a value and at least one candidate
SELECT field(1);

-- query 12
-- @expect_error=field does not support argument type
SELECT field([1], [1]);

-- query 13
-- @expect_error=decimal comparison precision overflow
SELECT field(cast(1 AS DECIMAL(76,0)), cast(1 AS DECIMAL(76,38)));
