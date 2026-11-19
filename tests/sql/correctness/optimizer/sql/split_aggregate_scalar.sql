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

-- OQ-4: scalar aggregate over join can use Local->Global aggregate.

CREATE TABLE ${case_db}.t_split_agg_l (k INT, payload INT);
CREATE TABLE ${case_db}.t_split_agg_r (k INT, payload INT);
INSERT INTO ${case_db}.t_split_agg_l VALUES
    ROW(1, 10), ROW(1, 11), ROW(2, 20), ROW(2, 21), ROW(3, 30), ROW(4, 40);
INSERT INTO ${case_db}.t_split_agg_r VALUES
    ROW(1, 100), ROW(1, 101), ROW(2, 200), ROW(5, 500);
ANALYZE ${case_db}.t_split_agg_l;
ANALYZE ${case_db}.t_split_agg_r;

-- @explain_contains=HASH AGGREGATE (LOCAL)
-- @explain_contains=HASH AGGREGATE (GLOBAL)
SELECT COUNT(r.payload) AS cnt
FROM ${case_db}.t_split_agg_l AS l
JOIN ${case_db}.t_split_agg_r AS r
  ON l.k = r.k;
