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

-- @order_sensitive=true
-- @tags=aggregate,max_by,min_by,null
-- Test Objective:
-- Select the value at the extremal non-null key, retaining a NULL winning value.
-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.max_min_by_null_value
(seq INT, v INT, k DECIMAL(18,9), neg_k DECIMAL(18,9), d DECIMAL(18,9))
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.max_min_by_null_value VALUES
(1,4,4008,-4008,4),(2,9,9006,-9006,9),
(3,NULL,6,-6,NULL),(4,999,NULL,NULL,999);

-- query 3
SELECT max_by(v,k) AS max_v, min_by(v,k) AS min_v,
       max_by(v,neg_k) AS max_negkey, min_by(v,neg_k) AS min_negkey,
       max_by(d,k) AS max_decimal, min_by(d,k) AS min_decimal
FROM ${case_db}.max_min_by_null_value;

-- query 4
SELECT seq,
       max_by(v,k) OVER (ORDER BY seq ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_v,
       min_by(v,k) OVER (ORDER BY seq ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_v
FROM ${case_db}.max_min_by_null_value ORDER BY seq;

-- query 5
SELECT max_by(v,k) AS max_null_key, min_by(v,k) AS min_null_key
FROM ${case_db}.max_min_by_null_value WHERE seq=4;

-- query 6
SELECT max_by(v,k) AS max_empty, min_by(v,k) AS min_empty
FROM ${case_db}.max_min_by_null_value WHERE seq=999;
