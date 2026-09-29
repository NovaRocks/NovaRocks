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

-- Test Objective: strict unary expressions preserve NULL domains before binding.
-- @order_sensitive=true

-- query 1
SELECT seq, -k AS negative_key, ~i AS inverted_integer
FROM (VALUES
(1,CAST('1.25' AS DECIMAL(18,9)),CAST(1 AS BIGINT)),
(2,CAST('-2.5' AS DECIMAL(18,9)),CAST(-2 AS BIGINT)),
(3,CAST(NULL AS DECIMAL(18,9)),CAST(NULL AS BIGINT))
) AS source(seq,k,i) ORDER BY seq;

-- query 2
SELECT max_by(v,-k) AS max_negative_key_value,
       min_by(v,-k) AS min_negative_key_value, count(-k) AS nonnull_keys
FROM (VALUES
(11,CAST('1.25' AS DECIMAL(18,9))),
(22,CAST('2.5' AS DECIMAL(18,9))),
(99,CAST(NULL AS DECIMAL(18,9)))
) AS source(v,k);

-- query 3
SELECT max_by(11,-CAST(NULL AS DECIMAL(18,9))) AS null_key_winner;

-- query 4
SELECT -1.25 AS negative_literal, ~1 AS inverted_literal;
