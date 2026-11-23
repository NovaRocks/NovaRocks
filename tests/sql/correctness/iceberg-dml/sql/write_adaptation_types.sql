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
-- Nested carrier differences and decimal layouts retain bytes, values and NULLs.
-- Timestamptz requires an externally authored target; the REST Java fixture covers it.
-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.write_adaptation_types (
  id INT,s ROW(raw VARBINARY,code INT,label VARCHAR),a ARRAY<VARBINARY>,m MAP<VARCHAR,VARBINARY>,
  strings ARRAY<VARCHAR>,d1 DECIMAL(1,0),d9 DECIMAL(9,2),d18 DECIMAL(18,2),d38 DECIMAL(38,2)
);
INSERT INTO ${case_db}.write_adaptation_types VALUES
  ROW(1,ROW(X'FF00',7,'東京'),[X'00FF',NULL],MAP(ARRAY['alpha'], ARRAY[X'FF00']),
      ['alpha',NULL,'東京'],9,1234567.89,1234567890123456.78,123456789012345678901234567890123456.78),
  ROW(2,ROW(NULL,NULL,NULL),[],map(),[],NULL,NULL,NULL,NULL),
  ROW(3,NULL,NULL,NULL,NULL,-9,-(1234567.89),-(1234567890123456.78),-(123456789012345678901234567890123456.78));

-- query 2
SELECT id,to_hex(s.raw) AS raw_hex,s.code AS code,s.label AS label,to_hex(a[1]) AS first_hex,
       a[2] IS NULL AS second_is_null,cardinality(a) AS array_count,
       to_hex(m['alpha']) AS map_hex,cardinality(m) AS map_count,
       strings[3] AS third_string,d1,d9,d18,d38
FROM ${case_db}.write_adaptation_types ORDER BY id;

-- query 3
-- @skip_result_check=true
DROP TABLE ${case_db}.write_adaptation_types;
