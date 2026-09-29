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

-- Test Objective: ordinary AVG retains the real Decimal256 bound result type.
-- @order_sensitive=true

-- query 1
SELECT avg(x) AS wide_avg FROM (VALUES (CAST('1000000000000000000000000000000000000000000000000.00' AS DECIMAL(60,2))),(CAST('1000000000000000000000000000000000000000000000000.00' AS DECIMAL(60,2))),(CAST('3000000000000000000000000000000000000000000000000.00' AS DECIMAL(60,2))),(CAST('3000000000000000000000000000000000000000000000000.00' AS DECIMAL(60,2))),(CAST('5000000000000000000000000000000000000000000000000.00' AS DECIMAL(60,2)))) AS source(x);

-- query 2
SELECT avg(x) AS null_avg FROM (VALUES (CAST(NULL AS DECIMAL(60,2)))) AS source(x);
