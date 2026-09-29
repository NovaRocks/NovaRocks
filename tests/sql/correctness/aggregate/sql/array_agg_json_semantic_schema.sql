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

-- JSON and STRING have the same Utf8 text and different frozen child domains.
SELECT array_agg(json_object('2:3')) AS json_values,
       array_agg(CAST(json_object('2:3') AS VARCHAR)) AS string_values;

WITH q AS (SELECT json_object('k',1) AS j)
SELECT array_agg(j) AS json_values, array_agg(CAST(j AS VARCHAR)) AS string_values FROM q;

WITH q AS (
  SELECT 1 AS n, json_object('k',1) AS j
  UNION ALL SELECT 2, json_object('k',2)
  UNION ALL SELECT 3, NULL
)
SELECT array_agg(j ORDER BY n) AS json_values,
       array_agg(CAST(j AS VARCHAR) ORDER BY n) AS string_values FROM q;

SELECT array_agg(json_object('k', 'a''b')) AS json_values,
       array_agg(CAST(json_object('k', 'a''b') AS VARCHAR)) AS string_values;
